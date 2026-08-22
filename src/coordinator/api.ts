import { createHash, randomUUID } from 'node:crypto';
import { canonicalizeInput, createCanonicalDigest } from '../canonical';
import type {
  QueuebitCompletionHandlerConfig,
  QueuebitConfig,
  QueuebitNormalizedBatchRunConfig
} from '../config';
import { getCompletionSnapshot, type CompletionSnapshot } from '../completions';
import { QueuebitError, type QueuebitErrorCode } from '../errors';
import type { JobAddOptions, QueuebitSerializedError } from '../jobs';
import {
  noopQueuebitObservabilityRecorder,
  type QueuebitObservabilityRecorder
} from '../observability';
import {
  createQueuebitKeyBuilder,
  executeQueuebitScript,
  type QueuebitRedisCommandClient
} from '../redis';
import type { FailureRecord } from '../runs';
import type {
  QueuebitCompletionEvent,
  QueuebitCompletionHandler,
  QueuebitMappedJob,
  QueuebitMapper,
  QueuebitRuntimeDefinition,
  QueuebitSource
} from '../runtime';
import { registerCoordinatorScripts } from './scripts';
import type {
  QueuebitCoordinator,
  QueuebitCoordinatorAdvanceOptions,
  QueuebitCoordinatorAdvanceResult,
  QueuebitCompletionDeliveryOptions,
  QueuebitCompletionDeliveryResult,
  QueuebitCoordinatorOptions,
  QueuebitInternalPreparedBatchJob
} from './types';

interface RunRecord {
  id: string;
  definition: string;
  executionState: string;
  input: unknown;
  boundary: unknown;
  dispatchCursor: unknown;
  boundaryMissing: boolean;
  sourceExhausted: boolean;
  inFlightBatches: number;
  nextBatchIndex: number;
  nextDispatchAt?: string;
  dispatchHoldReason?: DispatchHoldReason;
  recoveryParentRunId?: string;
  recoveryFailureDigest?: string;
  recoveryFailureCount?: number;
}

type DispatchHoldReason =
  | 'interval'
  | 'in_flight_limit'
  | 'backpressure'
  | 'no_active_worker'
  | 'redis_reconnecting';

interface DispatchHold {
  reason: DispatchHoldReason;
  nextDispatchAt?: string;
}

interface ClaimRunResult {
  coordinatorGeneration: number;
}

interface DispatchBatchResult {
  batchId: string;
  jobIds: string[];
  deduplicated: boolean;
}

interface ClaimCompletionResult {
  eventId: string;
  deliveryGeneration: number;
  attempt: number;
}

interface CompletionEventEnvelope {
  id: string;
  type: QueuebitCompletionEvent['type'];
  runId: string;
  batchId: string;
  handler: string;
  attempts: number;
  backoffJson: string;
  summaryJson: string;
  createdAt: string;
  updatedAt: string;
  nowMs: number;
}

interface CompletionParent {
  kind: 'batch' | 'run';
  key: string;
  nextCursorJson: string;
}

interface RecoveryProcessorPayload {
  name: string;
  data: unknown;
  options?: JobAddOptions;
  deduplicationKey?: string;
  idempotencyKey?: string;
}

interface RecoveryMapperPayload {
  record: unknown;
  input: unknown;
  boundary: unknown;
  cursor: unknown;
  recordIndex: number;
}

interface MapperFailureEnvelope {
  recordIdentity: string;
  errorJson: string;
  payloadJson: string;
}

const segmentPattern = /^[A-Za-z0-9][A-Za-z0-9:_-]{0,191}$/;
const dispatchHoldReasons = new Set<DispatchHoldReason>([
  'interval',
  'in_flight_limit',
  'backpressure',
  'no_active_worker',
  'redis_reconnecting'
]);

export function createQueuebitCoordinator(options: QueuebitCoordinatorOptions): QueuebitCoordinator {
  const coordinatorId = options.coordinatorId ?? randomUUID();
  assertSegment('coordinatorId', coordinatorId);
  return new QueuebitCoordinatorKernel({ ...options, coordinatorId });
}

class QueuebitCoordinatorKernel implements QueuebitCoordinator {
  readonly coordinatorId: string;

  readonly #config: QueuebitConfig;
  readonly #redis: QueuebitRedisCommandClient;
  readonly #runtime: QueuebitRuntimeDefinition;
  readonly #keys: ReturnType<typeof createQueuebitKeyBuilder>;
  readonly #scripts = registerCoordinatorScripts();
  readonly #observability: QueuebitObservabilityRecorder;
  readonly #now: () => Date;
  readonly #idGenerator: () => string;
  readonly #leaseMs: number;
  readonly #sourceTimeoutMs: number;

  constructor(options: Required<Pick<QueuebitCoordinatorOptions, 'coordinatorId'>> & QueuebitCoordinatorOptions) {
    this.#config = options.config;
    this.#redis = options.redis;
    this.#runtime = options.runtime;
    this.#keys = createQueuebitKeyBuilder(options.config);
    this.#observability = options.observability ?? noopQueuebitObservabilityRecorder;
    this.#now = options.now ?? (() => new Date());
    this.#idGenerator = options.idGenerator ?? (() => randomUUID());
    this.#leaseMs = normalizePositiveInteger('leaseMs', options.leaseMs ?? 30_000);
    this.#sourceTimeoutMs = normalizePositiveInteger(
      'sourceTimeoutMs',
      options.sourceTimeoutMs ?? 30_000
    );
    this.coordinatorId = options.coordinatorId;
  }

  async advanceRun(
    runId: string,
    options: QueuebitCoordinatorAdvanceOptions = {}
  ): Promise<QueuebitCoordinatorAdvanceResult> {
    const startedAtMs = this.#now().getTime();
    try {
      const result = await this.#advanceRunInternal(runId, options);
      this.#observeAdvance(result, startedAtMs);
      return result;
    } catch (cause) {
      this.#observability.incrementCounter('coordinator_advance_errors_total', 1, {
        coordinatorId: this.coordinatorId
      });
      throw cause;
    }
  }

  async #advanceRunInternal(
    runId: string,
    options: QueuebitCoordinatorAdvanceOptions = {}
  ): Promise<QueuebitCoordinatorAdvanceResult> {
    assertSegment('runId', runId);
    const completionDeliveryOptions = options.signal === undefined ? {} : { signal: options.signal };
    await this.deliverDueCompletions(completionDeliveryOptions);
    const initialRecord = await this.#readRun(runId);
    const definitionConfig = this.#getDefinitionConfig(initialRecord.definition);
    if (initialRecord.executionState === 'paused') {
      return {
        status: 'paused',
        runId,
        definition: initialRecord.definition,
        coordinatorId: this.coordinatorId,
        recordsSeen: 0,
        recordsDispatched: 0,
        recordsSkipped: 0,
        recordsFailed: 0,
        jobsCreated: 0,
        sourceExhausted: initialRecord.sourceExhausted,
        dispatchCursor: initialRecord.dispatchCursor
      };
    }
    if (isTerminalRun(initialRecord.executionState)) {
      return {
        status: 'already_terminal',
        runId,
        definition: initialRecord.definition,
        coordinatorId: this.coordinatorId,
        recordsSeen: 0,
        recordsDispatched: 0,
        recordsSkipped: 0,
        recordsFailed: 0,
        jobsCreated: 0,
        sourceExhausted: true,
        dispatchCursor: initialRecord.dispatchCursor
      };
    }

    const claim = await this.#claimRun(runId);
    await this.#reconcileOpenBatches(runId, claim, definitionConfig);
    await this.deliverDueCompletions(completionDeliveryOptions);
    const claimedRecord = await this.#readRun(runId);
    if (isTerminalRun(claimedRecord.executionState)) {
      return {
        status: 'source_exhausted',
        runId,
        definition: claimedRecord.definition,
        coordinatorId: this.coordinatorId,
        coordinatorGeneration: claim.coordinatorGeneration,
        recordsSeen: 0,
        recordsDispatched: 0,
        recordsSkipped: 0,
        recordsFailed: 0,
        jobsCreated: 0,
        sourceExhausted: true,
        dispatchCursor: claimedRecord.dispatchCursor
      };
    }
    if (claimedRecord.executionState === 'paused') {
      return {
        status: 'paused',
        runId,
        definition: claimedRecord.definition,
        coordinatorId: this.coordinatorId,
        coordinatorGeneration: claim.coordinatorGeneration,
        recordsSeen: 0,
        recordsDispatched: 0,
        recordsSkipped: 0,
        recordsFailed: 0,
        jobsCreated: 0,
        sourceExhausted: claimedRecord.sourceExhausted,
        dispatchCursor: claimedRecord.dispatchCursor
      };
    }
    if (claimedRecord.executionState === 'pausing' || claimedRecord.executionState === 'cancelling') {
      return {
        status: 'waiting_for_batch',
        runId,
        definition: claimedRecord.definition,
        coordinatorId: this.coordinatorId,
        coordinatorGeneration: claim.coordinatorGeneration,
        recordsSeen: 0,
        recordsDispatched: 0,
        recordsSkipped: 0,
        recordsFailed: 0,
        jobsCreated: 0,
        sourceExhausted: claimedRecord.sourceExhausted,
        dispatchCursor: claimedRecord.dispatchCursor
      };
    }
    const hold = await this.#getDispatchHold(claimedRecord, definitionConfig);
    if (hold !== null) {
      await this.#setDispatchHold(runId, hold);
      return {
        status: 'waiting_for_batch',
        runId,
        definition: claimedRecord.definition,
        coordinatorId: this.coordinatorId,
        coordinatorGeneration: claim.coordinatorGeneration,
        recordsSeen: 0,
        recordsDispatched: 0,
        recordsSkipped: 0,
        recordsFailed: 0,
        jobsCreated: 0,
        sourceExhausted: claimedRecord.sourceExhausted,
        dispatchCursor: claimedRecord.dispatchCursor
      };
    }
    await this.#clearDispatchHold(runId, claimedRecord);
    if (claimedRecord.recoveryParentRunId !== undefined) {
      const recoveryState = await this.#loadRecoveryState(claimedRecord, definitionConfig);
      const batchIndex = claimedRecord.nextBatchIndex;
      const batchId = `${runId}:batch:${batchIndex}`;
      assertSegment('batchId', batchId);
      const prepared = await this.#mapRecoveryFailures(
        definitionConfig,
        claimedRecord,
        batchId,
        recoveryState.records
      );
      const dispatch = await this.#dispatchBatch({
        run: claimedRecord,
        definitionConfig,
        claim,
        batchId,
        batchIndex,
        boundary: recoveryState.boundary,
        cursor: recoveryState.cursor,
        nextCursor: recoveryState.nextCursor,
        sourceExhausted: recoveryState.exhausted,
        boundaryTotalRecords: recoveryState.boundaryTotalRecords,
        recordsSeen: recoveryState.records.length,
        recordsDispatched: prepared.recordsDispatched,
        recordsSkipped: prepared.recordsSkipped,
        recordsFailed: prepared.mapperFailures.length,
        recordsUndispatched: 0,
        jobs: prepared.jobs,
        mapperFailures: prepared.mapperFailures
      });
      if (recoveryState.exhausted && dispatch.jobIds.length === 0) {
        await this.deliverDueCompletions(completionDeliveryOptions);
      }

      return {
        status: recoveryState.exhausted && dispatch.jobIds.length === 0 ? 'source_exhausted' : 'dispatched',
        runId,
        definition: claimedRecord.definition,
        coordinatorId: this.coordinatorId,
        coordinatorGeneration: claim.coordinatorGeneration,
        batchId: dispatch.batchId,
        batchIndex,
        recordsSeen: recoveryState.records.length,
        recordsDispatched: prepared.recordsDispatched,
        recordsSkipped: prepared.recordsSkipped,
        recordsFailed: prepared.mapperFailures.length,
        jobsCreated: dispatch.jobIds.length,
        sourceExhausted: recoveryState.exhausted,
        dispatchCursor: recoveryState.nextCursor
      };
    }
    const source = this.#getSource(definitionConfig.source);
    const mapper = this.#getMapper(definitionConfig.mapper);
    const sourceState = await this.#loadSourceState(
      source,
      claimedRecord,
      definitionConfig,
      options.signal
    );
    const batchIndex = claimedRecord.nextBatchIndex;
    const batchId = `${runId}:batch:${batchIndex}`;
    assertSegment('batchId', batchId);
    const prepared = await this.#mapRecords(
      mapper,
      definitionConfig,
      claimedRecord,
      batchId,
      sourceState.records,
      sourceState.boundary,
      sourceState.cursor
    );
    const dispatch = await this.#dispatchBatch({
      run: claimedRecord,
      definitionConfig,
      claim,
      batchId,
      batchIndex,
      boundary: sourceState.boundary,
      cursor: sourceState.cursor,
      nextCursor: sourceState.nextCursor,
      sourceExhausted: sourceState.exhausted,
      boundaryTotalRecords: sourceState.boundaryTotalRecords,
      recordsSeen: sourceState.records.length,
      recordsDispatched: prepared.recordsDispatched,
      recordsSkipped: prepared.recordsSkipped,
      recordsFailed: prepared.mapperFailures.length,
      recordsUndispatched: 0,
      jobs: prepared.jobs,
      mapperFailures: prepared.mapperFailures
    });
    if (sourceState.exhausted && dispatch.jobIds.length === 0) {
      await this.deliverDueCompletions(completionDeliveryOptions);
    }

    return {
      status: sourceState.exhausted && dispatch.jobIds.length === 0 ? 'source_exhausted' : 'dispatched',
      runId,
      definition: claimedRecord.definition,
      coordinatorId: this.coordinatorId,
      coordinatorGeneration: claim.coordinatorGeneration,
      batchId: dispatch.batchId,
      batchIndex,
      recordsSeen: sourceState.records.length,
      recordsDispatched: prepared.recordsDispatched,
      recordsSkipped: prepared.recordsSkipped,
      recordsFailed: prepared.mapperFailures.length,
      jobsCreated: dispatch.jobIds.length,
      sourceExhausted: sourceState.exhausted,
      dispatchCursor: sourceState.nextCursor
    };
  }

  async deliverDueCompletions(
    options: QueuebitCompletionDeliveryOptions = {}
  ): Promise<QueuebitCompletionDeliveryResult> {
    const signal = options.signal ?? new AbortController().signal;
    const limit = normalizeCompletionDeliveryLimit(options.limit ?? 25);
    const observedAt = this.#now();
    const due = parseZrangeWithScores(await this.#redis.sendCommand([
      'ZRANGEBYSCORE',
      this.#keys.completionsDue(),
      '-inf',
      String(observedAt.getTime()),
      'WITHSCORES',
      'LIMIT',
      '0',
      String(limit)
    ]));
    const result: QueuebitCompletionDeliveryResult = {
      claimed: 0,
      delivered: 0,
      retrying: 0,
      failed: 0,
      skipped: 0,
      eventIds: []
    };

    for (const dueEvent of due) {
      throwIfAborted(signal);
      const claim = await this.#claimCompletion(dueEvent.member);
      if (claim === null) {
        result.skipped += 1;
        continue;
      }
      result.claimed += 1;
      result.eventIds.push(claim.eventId);
      const snapshot = await getCompletionSnapshot(
        this.#redis,
        this.#keys.completion(claim.eventId)
      );
      if (snapshot === null) {
        result.skipped += 1;
        continue;
      }

      const handlerOutcome = await this.#invokeCompletionHandler(snapshot, signal);
      const status = await this.#settleCompletion(snapshot, claim, handlerOutcome);
      if (status === 'delivered') result.delivered += 1;
      else if (status === 'retrying') result.retrying += 1;
      else result.failed += 1;
    }

    this.#observeCompletionDelivery(result);
    return result;
  }

  #observeAdvance(result: QueuebitCoordinatorAdvanceResult, startedAtMs: number): void {
    const labels = {
      coordinatorId: this.coordinatorId,
      definition: result.definition,
      status: result.status
    };
    this.#observability.incrementCounter('coordinator_runs_advanced_total', 1, labels);
    this.#observability.incrementCounter('coordinator_records_seen_total', result.recordsSeen, labels);
    this.#observability.incrementCounter('coordinator_records_dispatched_total', result.recordsDispatched, labels);
    this.#observability.incrementCounter('coordinator_records_failed_total', result.recordsFailed, labels);
    this.#observability.incrementCounter('coordinator_jobs_created_total', result.jobsCreated, labels);
    this.#observability.observeDuration(
      'coordinator_advance_duration_ms',
      this.#now().getTime() - startedAtMs,
      labels
    );
  }

  #observeCompletionDelivery(result: QueuebitCompletionDeliveryResult): void {
    const labels = { coordinatorId: this.coordinatorId };
    this.#observability.incrementCounter('completion_events_claimed_total', result.claimed, labels);
    this.#observability.incrementCounter('completion_events_delivered_total', result.delivered, labels);
    this.#observability.incrementCounter('completion_events_retrying_total', result.retrying, labels);
    this.#observability.incrementCounter('completion_events_failed_total', result.failed, labels);
    this.#observability.incrementCounter('completion_events_skipped_total', result.skipped, labels);
  }

  async #readRun(runId: string): Promise<RunRecord> {
    const record = redisHashToRecord(await this.#redis.sendCommand(['HGETALL', this.#keys.run(runId)]));
    if (Object.keys(record).length === 0) {
      throw new QueuebitError({
        code: 'QB_RUN_NOT_FOUND',
        message: 'Run does not exist.',
        details: { runId }
      });
    }
    const boundaryRaw = requiredField(record, 'boundary');
    const dispatchCursorRaw = requiredField(record, 'dispatchCursor');
    const run: RunRecord = {
      id: requiredField(record, 'id'),
      definition: requiredField(record, 'definition'),
      executionState: requiredField(record, 'executionState'),
      input: JSON.parse(requiredField(record, 'input')),
      boundary: boundaryRaw.length === 0 ? null : JSON.parse(boundaryRaw),
      dispatchCursor: dispatchCursorRaw.length === 0 ? null : JSON.parse(dispatchCursorRaw),
      boundaryMissing: boundaryRaw.length === 0,
      sourceExhausted: requiredField(record, 'sourceExhausted') === '1',
      inFlightBatches: Number.parseInt(requiredField(record, 'inFlightBatches'), 10),
      nextBatchIndex: Number.parseInt(record.nextBatchIndex ?? '0', 10)
    };
    assignOptional(run, 'recoveryParentRunId', record.recoveryParentRunId);
    assignOptional(run, 'recoveryFailureDigest', record.recoveryFailureDigest);
    assignOptional(run, 'nextDispatchAt', record.nextDispatchAt);
    assignOptional(run, 'dispatchHoldReason', parseDispatchHoldReason(record.dispatchHoldReason));
    if (record.recoveryFailureCount !== undefined) {
      run.recoveryFailureCount = Number.parseInt(record.recoveryFailureCount, 10);
    }
    return run;
  }

  async #getDispatchHold(
    run: RunRecord,
    definitionConfig: QueuebitNormalizedBatchRunConfig
  ): Promise<DispatchHold | null> {
    const inFlightLimit = definitionConfig.dispatch.mode === 'sequential'
      ? 1
      : definitionConfig.dispatch.maxInFlightBatches;
    if (run.inFlightBatches >= inFlightLimit) {
      return { reason: 'in_flight_limit' };
    }
    if (run.sourceExhausted && run.inFlightBatches > 0) {
      return { reason: 'in_flight_limit' };
    }
    if (definitionConfig.dispatch.mode === 'paced' && run.nextDispatchAt !== undefined) {
      const nextDispatchAtMs = Date.parse(run.nextDispatchAt);
      if (Number.isFinite(nextDispatchAtMs) && nextDispatchAtMs > this.#now().getTime()) {
        return { reason: 'interval', nextDispatchAt: run.nextDispatchAt };
      }
    }
    if (await this.#isQueueBackpressured(definitionConfig.queue)) {
      return { reason: 'backpressure' };
    }
    return null;
  }

  async #isQueueBackpressured(queue: string): Promise<boolean> {
    const queueConfig = this.#config.queues[queue];
    const backpressure = queueConfig?.backpressure;
    if (backpressure === undefined) return false;
    const countersKey = this.#keys.queueCounters(queue);
    const counters = redisHashToRecord(await this.#redis.sendCommand(['HGETALL', countersKey]));
    const queuedJobs = Number.parseInt(counters.queuedJobs ?? '0', 10);
    const queuedBytes = Number.parseInt(counters.queuedBytes ?? '0', 10);
    const highJobs = backpressure.highWatermarkJobs;
    const highBytes = backpressure.highWatermarkBytes;
    const lowJobs = backpressure.lowWatermarkJobs ?? highJobs;
    const lowBytes = backpressure.lowWatermarkBytes ?? highBytes;
    const belowJobs = highJobs === undefined || queuedJobs <= (lowJobs ?? highJobs);
    const belowBytes = highBytes === undefined || queuedBytes <= (lowBytes ?? highBytes);
    if (counters.backpressureLatched === '1' && belowJobs && belowBytes) {
      await this.#redis.sendCommand([
        'HDEL',
        countersKey,
        'backpressureLatched',
        'backpressureReason',
        'backpressureSince',
        'backpressureLastCheckedAt'
      ]);
      return false;
    }
    const reason =
      highJobs !== undefined && queuedJobs >= highJobs
        ? 'jobs'
        : highBytes !== undefined && queuedBytes >= highBytes
          ? 'bytes'
          : '';
    if (reason.length > 0) {
      const observedAt = this.#now().toISOString();
      const existingSince = counters.backpressureSince ?? observedAt;
      await this.#redis.sendCommand([
        'HSET',
        countersKey,
        'backpressureLatched',
        '1',
        'backpressureReason',
        reason,
        'backpressureSince',
        existingSince,
        'backpressureLastCheckedAt',
        observedAt
      ]);
      return true;
    }
    return counters.backpressureLatched === '1';
  }

  async #setDispatchHold(runId: string, hold: DispatchHold) {
    const runKey = this.#keys.run(runId);
    const updatedAt = this.#now().toISOString();
    await this.#redis.sendCommand([
      'HSET',
      runKey,
      'dispatchHoldReason',
      hold.reason,
      'updatedAt',
      updatedAt
    ]);
    if (hold.nextDispatchAt === undefined) {
      await this.#redis.sendCommand(['HDEL', runKey, 'nextDispatchAt']);
    } else {
      await this.#redis.sendCommand(['HSET', runKey, 'nextDispatchAt', hold.nextDispatchAt]);
    }
  }

  async #clearDispatchHold(runId: string, run: RunRecord) {
    if (run.dispatchHoldReason === undefined && run.nextDispatchAt === undefined) return;
    await this.#redis.sendCommand([
      'HDEL',
      this.#keys.run(runId),
      'dispatchHoldReason',
      'nextDispatchAt'
    ]);
  }

  #getDefinitionConfig(definition: string): QueuebitNormalizedBatchRunConfig {
    const definitionConfig = this.#config.batchRuns[definition];
    if (definitionConfig === undefined) {
      throw new QueuebitError({
        code: 'QB_RUN_DEFINITION_NOT_FOUND',
        message: `BatchRun definition "${definition}" is not declared in Queuebit config.`,
        details: { definition }
      });
    }
    return definitionConfig;
  }

  #getSource(name: string): QueuebitSource {
    const source = this.#runtime.sources[name];
    if (source === undefined) {
      throw new QueuebitError({
        code: 'QB_CONFIG_INVALID',
        message: `Queuebit source "${name}" is not registered in runtime.`,
        details: { source: name }
      });
    }
    return source;
  }

  #getMapper(name: string): QueuebitMapper {
    const mapper = this.#runtime.mappers[name];
    if (mapper === undefined) {
      throw new QueuebitError({
        code: 'QB_CONFIG_INVALID',
        message: `Queuebit mapper "${name}" is not registered in runtime.`,
        details: { mapper: name }
      });
    }
    return mapper;
  }

  async #claimRun(runId: string): Promise<ClaimRunResult> {
    const observedAt = this.#now();
    const leaseDeadlineMs = observedAt.getTime() + this.#leaseMs;
    const reply = await executeQueuebitScript(
      this.#redis,
      this.#scripts.claimRun,
      [this.#keys.run(runId)],
      [
        this.coordinatorId,
        String(leaseDeadlineMs),
        new Date(leaseDeadlineMs).toISOString(),
        String(observedAt.getTime()),
        observedAt.toISOString()
      ]
    );
    const [tag, codeOrGeneration, message, detailsJson] = assertTaggedReply(reply);
    if (tag === 'ok') {
      return { coordinatorGeneration: Number.parseInt(String(codeOrGeneration), 10) };
    }
    throw new QueuebitError({
      code: toCoordinatorErrorCode(String(codeOrGeneration)),
      message: String(message ?? 'Coordinator could not claim Run.'),
      details: parseOptionalJson(detailsJson)
    });
  }

  async #claimCompletion(eventId: string): Promise<ClaimCompletionResult | null> {
    const observedAt = this.#now();
    const leaseDeadlineMs = observedAt.getTime() + this.#leaseMs;
    const reply = await executeQueuebitScript(
      this.#redis,
      this.#scripts.claimCompletion,
      [this.#keys.completion(eventId), this.#keys.completionsDue()],
      [
        this.coordinatorId,
        String(leaseDeadlineMs),
        new Date(leaseDeadlineMs).toISOString(),
        String(observedAt.getTime()),
        observedAt.toISOString()
      ]
    );
    const [tag, idOrCode, generationOrMessage, attemptOrDetails] = assertTaggedReply(reply);
    if (tag === 'ok') {
      const claimedEventId = String(idOrCode ?? '');
      if (claimedEventId.length === 0) return null;
      return {
        eventId: claimedEventId,
        deliveryGeneration: Number.parseInt(String(generationOrMessage), 10),
        attempt: Number.parseInt(String(attemptOrDetails), 10)
      };
    }
    throw new QueuebitError({
      code: toCoordinatorErrorCode(String(idOrCode)),
      message: String(generationOrMessage ?? 'Coordinator could not claim completion event.'),
      details: parseOptionalJson(attemptOrDetails)
    });
  }

  async #invokeCompletionHandler(
    snapshot: CompletionSnapshot,
    signal: AbortSignal
  ): Promise<{ result: 'delivered' } | { result: 'failed'; error: QueuebitSerializedError }> {
    try {
      if (snapshot.handler === undefined || snapshot.handler.length === 0) {
        throw new QueuebitError({
          code: 'QB_CONFIG_INVALID',
          message: 'Completion event has no handler but was scheduled for delivery.',
          details: { eventId: snapshot.id }
        });
      }
      const handler = this.#getCompletionHandler(snapshot.handler);
      const event = completionSnapshotToEvent(snapshot);
      await handler(event, { signal, coordinatorId: this.coordinatorId });
      return { result: 'delivered' };
    } catch (cause) {
      return { result: 'failed', error: serializeFailure(cause) };
    }
  }

  #getCompletionHandler(name: string): QueuebitCompletionHandler {
    const handler = this.#runtime.completions?.[name];
    if (handler === undefined) {
      throw new QueuebitError({
        code: 'QB_CONFIG_INVALID',
        message: `Queuebit completion handler "${name}" is not registered in runtime.`,
        details: { handler: name }
      });
    }
    return handler;
  }

  async #settleCompletion(
    snapshot: CompletionSnapshot,
    claim: ClaimCompletionResult,
    outcome: { result: 'delivered' } | { result: 'failed'; error: QueuebitSerializedError }
  ): Promise<'delivered' | 'retrying' | 'failed'> {
    const observedAt = this.#now();
    const parent = await this.#getCompletionParent(snapshot);
    const runCompletion = await this.#createRunCompletionEnvelope(snapshot.runId, observedAt);
    const nextDueMs = outcome.result === 'failed'
      ? computeCompletionNextDueMs(snapshot, observedAt)
      : observedAt.getTime();
    const reply = await executeQueuebitScript(
      this.#redis,
      this.#scripts.settleCompletion,
      [
        this.#keys.completion(snapshot.id),
        parent.key,
        this.#keys.run(snapshot.runId),
        this.#keys.runBatches(snapshot.runId),
        this.#keys.completionCounters(),
        this.#keys.completionsIndex(),
        this.#keys.completionsDetails(),
        this.#keys.completionsDue(),
        this.#keys.completion(runCompletion.id || `${snapshot.runId}:settled`),
        this.#keys.runsTerminalDetails()
      ],
      [JSON.stringify({
        coordinatorId: this.coordinatorId,
        deliveryGeneration: claim.deliveryGeneration,
        parentKind: parent.kind,
        parentNextCursorJson: parent.nextCursorJson,
        result: outcome.result,
        errorJson: outcome.result === 'failed' ? canonicalizeInput(outcome.error) : '',
        nextDueMs,
        nextDueAt: new Date(nextDueMs).toISOString(),
        runCompletion,
        nowMs: observedAt.getTime(),
        updatedAt: observedAt.toISOString()
      })]
    );
    const [tag, statusOrCode, message, details] = assertTaggedReply(reply);
    if (tag === 'ok') {
      const status = String(statusOrCode);
      if (status === 'delivered' || status === 'retrying' || status === 'failed') return status;
    }
    throw new QueuebitError({
      code: toCoordinatorErrorCode(String(statusOrCode)),
      message: String(message ?? 'Coordinator could not settle completion event.'),
      details: parseOptionalJson(details)
    });
  }

  async #getCompletionParent(snapshot: CompletionSnapshot): Promise<CompletionParent> {
    if (snapshot.batchId !== undefined) {
      const batchIndex = parseBatchIndex(snapshot.batchId);
      const batchKey = this.#keys.batch(snapshot.runId, batchIndex);
      const batch = redisHashToRecord(await this.#redis.sendCommand(['HGETALL', batchKey]));
      return {
        kind: 'batch',
        key: batchKey,
        nextCursorJson: requiredField(batch, 'nextCursor')
      };
    }
    return {
      kind: 'run',
      key: this.#keys.run(snapshot.runId),
      nextCursorJson: ''
    };
  }

  async #loadRecoveryState(
    run: RunRecord,
    definitionConfig: QueuebitNormalizedBatchRunConfig
  ): Promise<{
    boundary: unknown;
    boundaryTotalRecords: number | undefined;
    cursor: number;
    records: FailureRecord[];
    nextCursor: number;
    exhausted: boolean;
  }> {
    if (run.recoveryParentRunId === undefined) {
      throw new QueuebitError({
        code: 'QB_COORDINATOR_INVALID',
        message: 'Recovery Run is missing recoveryParentRunId.',
        details: { runId: run.id }
      });
    }
    const cursor = normalizeRecoveryCursor(run.dispatchCursor);
    const reply = await this.#redis.sendCommand([
      'ZRANGEBYSCORE',
      this.#keys.failures(run.recoveryParentRunId),
      cursor === 0 ? '-inf' : `(${cursor}`,
      '+inf',
      'WITHSCORES',
      'LIMIT',
      '0',
      String(definitionConfig.pageSize + 1)
    ]);
    const pairs = parseZrangeWithScores(reply);
    const visible = pairs.slice(0, definitionConfig.pageSize);
    const records = visible
      .map(pair => parseRecoveryFailure(pair.member, pair.score))
      .filter(failure => failure.recoveryAvailable);
    const last = visible[visible.length - 1];
    const boundary = run.boundaryMissing
      ? {
          parentRunId: run.recoveryParentRunId,
          failureDigest: run.recoveryFailureDigest ?? '',
          failureCount: run.recoveryFailureCount ?? records.length,
          stages: [...new Set(records.map(failure => failure.stage))]
        }
      : run.boundary;
    return {
      boundary,
      boundaryTotalRecords: run.recoveryFailureCount,
      cursor,
      records,
      nextCursor: last === undefined ? cursor : Number.parseInt(last.score, 10),
      exhausted: pairs.length <= definitionConfig.pageSize
    };
  }

  async #mapRecoveryFailures(
    definitionConfig: QueuebitNormalizedBatchRunConfig,
    run: RunRecord,
    batchId: string,
    failures: FailureRecord[]
  ): Promise<{
    jobs: QueuebitInternalPreparedBatchJob[];
    recordsDispatched: number;
    recordsSkipped: number;
    mapperFailures: MapperFailureEnvelope[];
  }> {
    const jobs: QueuebitInternalPreparedBatchJob[] = [];
    let recordsDispatched = 0;
    let recordsSkipped = 0;
    const mapperFailures: MapperFailureEnvelope[] = [];
    for (const failure of failures) {
      if (failure.stage === 'processor') {
        const payload = assertRecoveryProcessorPayload(failure);
        if (payload.name.length === 0) {
          recordsSkipped += 1;
          continue;
        }
        const options = createRecoveryJobOptions(run, failure, payload);
        jobs.push(this.#prepareJob(
          definitionConfig.queue,
          run.id,
          batchId,
          {
            name: payload.name,
            identity: `failure:${failure.sequence}:${failure.recordIdentity}`,
            data: payload.data,
            options
          },
          `failure:${failure.sequence}:${failure.recordIdentity}`
        ));
        recordsDispatched += 1;
        continue;
      }

      const payload = assertRecoveryMapperPayload(failure);
      const mapper = this.#getMapper(definitionConfig.mapper);
      let mapped: unknown;
      try {
        mapped = await mapper(payload.record, {
          runId: run.id,
          batchId,
          input: payload.input,
          boundary: payload.boundary,
          cursor: payload.cursor,
          recordIndex: payload.recordIndex
        });
      } catch (cause) {
        mapperFailures.push(createMapperFailureEnvelope({
          recordIdentity: failure.recordIdentity,
          record: payload.record,
          input: payload.input,
          boundary: payload.boundary,
          cursor: payload.cursor,
          recordIndex: payload.recordIndex,
          error: serializeFailure(cause)
        }));
        continue;
      }
      const mappedJobs = normalizeMappedJobs(mapped);
      if (mappedJobs.length === 0) {
        recordsSkipped += 1;
        continue;
      }
      for (const [jobIndex, mappedJob] of mappedJobs.entries()) {
        const identity = `failure:${failure.sequence}:${mappedJob.identity}:${jobIndex}`;
        jobs.push(this.#prepareJob(definitionConfig.queue, run.id, batchId, {
          ...mappedJob,
          options: createRecoveryMapperJobOptions(run, failure, mappedJob.options, jobIndex)
        }, identity));
      }
      recordsDispatched += 1;
    }
    return { jobs, recordsDispatched, recordsSkipped, mapperFailures };
  }

  async #createRunCompletionEnvelope(runId: string, observedAt: Date): Promise<CompletionEventEnvelope> {
    const run = redisHashToRecord(await this.#redis.sendCommand(['HGETALL', this.#keys.run(runId)]));
    const definition = requiredField(run, 'definition');
    const handlerConfig = this.#getDefinitionConfig(definition).completion.run;
    const cancelling = run.executionState === 'cancelling' || run.executionState === 'cancelled';
    return createCompletionEnvelope({
      id: cancelling ? `${runId}:cancelled` : `${runId}:settled`,
      type: cancelling ? 'run.cancelled' : 'run.settled',
      runId,
      observedAt,
      summaryJson: runSummaryJsonFromHash(run),
      ...(handlerConfig === undefined ? {} : { handlerConfig })
    });
  }

  async #loadSourceState(
    source: QueuebitSource,
    run: RunRecord,
    definitionConfig: QueuebitNormalizedBatchRunConfig,
    parentSignal: AbortSignal | undefined
  ) {
    const linkedController = createLinkedAbortController(parentSignal);
    const { controller } = linkedController;
    const timeout = setTimeout(() => controller.abort('source-timeout'), this.#sourceTimeoutMs);
    try {
      const frozen = run.boundaryMissing
        ? await source.freeze({ runId: run.id, input: run.input, signal: controller.signal })
        : { boundary: run.boundary, cursor: run.dispatchCursor, totalRecords: undefined };
      const cursor = frozen.cursor;
      const loaded = await source.load({
        runId: run.id,
        input: run.input,
        boundary: frozen.boundary,
        cursor,
        limit: definitionConfig.pageSize,
        signal: controller.signal
      });
      assertSourceLoadResult(loaded);
      if (
        loaded.records.length > 0
        && canonicalizeInput(loaded.nextCursor) === canonicalizeInput(cursor)
      ) {
        throw new QueuebitError({
          code: 'QB_SOURCE_CURSOR_NOT_ADVANCED',
          message: 'Source returned records without advancing nextCursor.',
          details: { runId: run.id, definition: run.definition }
        });
      }
      return {
        boundary: frozen.boundary,
        boundaryTotalRecords: frozen.totalRecords,
        cursor,
        records: loaded.records,
        nextCursor: loaded.nextCursor,
        exhausted: loaded.exhausted
      };
    } finally {
      clearTimeout(timeout);
      linkedController.dispose();
    }
  }

  async #reconcileOpenBatches(
    runId: string,
    claim: ClaimRunResult,
    definitionConfig: QueuebitNormalizedBatchRunConfig
  ): Promise<void> {
    const pairs = await this.#readRunBatches(runId);
    for (const pair of pairs) {
      const batch = redisHashToRecord(await this.#redis.sendCommand(['HGETALL', this.#keys.batch(runId, pair.index)]));
      if (batch.executionState !== 'running') continue;
      const jobIds = parseJsonArray(requiredField(batch, 'jobIds'));
      const jobKeys = jobIds.map(jobId => this.#keys.job(jobId));
      const observedAt = this.#now();
      const batchId = requiredField(batch, 'id');
      const batchCompletion = createCompletionEnvelope({
        id: `${batchId}:settled`,
        type: 'batch.settled',
        runId,
        batchId,
        observedAt,
        ...(definitionConfig.completion.batch === undefined
          ? {}
          : { handlerConfig: definitionConfig.completion.batch })
      });
      const runCompletion = await this.#createRunCompletionEnvelope(runId, observedAt);
      const reply = await executeQueuebitScript(
        this.#redis,
        this.#scripts.settleBatch,
        [
          this.#keys.run(runId),
          this.#keys.runBatches(runId),
          this.#keys.batch(runId, pair.index),
          this.#keys.completionCounters(),
          this.#keys.completionsIndex(),
          this.#keys.completionsDetails(),
          this.#keys.completionsDue(),
          this.#keys.completion(batchCompletion.id),
          this.#keys.completion(runCompletion.id),
          this.#keys.failures(runId),
          this.#keys.runsTerminalDetails(),
          ...jobKeys
        ],
        [JSON.stringify({
          coordinatorId: this.coordinatorId,
          coordinatorGeneration: claim.coordinatorGeneration,
          batchId,
          jobCount: jobKeys.length,
          nextCursorJson: requiredField(batch, 'nextCursor'),
          batchCompletion,
          runCompletion,
          updatedAt: observedAt.toISOString()
        })]
      );
      const [tag, stateOrCode, message, details] = assertTaggedReply(reply);
      if (tag === 'err') {
        throw new QueuebitError({
          code: toCoordinatorErrorCode(String(stateOrCode)),
          message: String(message ?? 'Coordinator could not settle Batch.'),
          details: parseOptionalJson(details)
        });
      }
      if (stateOrCode === 'waiting') continue;
    }
  }

  async #readRunBatches(runId: string): Promise<Array<{ batchId: string; index: number }>> {
    const reply = await this.#redis.sendCommand([
      'ZRANGEBYSCORE',
      this.#keys.runBatches(runId),
      '-inf',
      '+inf',
      'WITHSCORES',
      'LIMIT',
      '0',
      '100'
    ]);
    return parseZrangeWithScores(reply).map(pair => ({
      batchId: pair.member,
      index: Number.parseInt(pair.score, 10)
    }));
  }

  async #mapRecords(
    mapper: QueuebitMapper,
    definitionConfig: QueuebitNormalizedBatchRunConfig,
    run: RunRecord,
    batchId: string,
    records: unknown[],
    boundary: unknown,
    cursor: unknown
  ): Promise<{
    jobs: QueuebitInternalPreparedBatchJob[];
    recordsDispatched: number;
    recordsSkipped: number;
    mapperFailures: MapperFailureEnvelope[];
  }> {
    const jobs: QueuebitInternalPreparedBatchJob[] = [];
    let recordsDispatched = 0;
    let recordsSkipped = 0;
    const mapperFailures: MapperFailureEnvelope[] = [];
    const seenIdentities = new Set<string>();
    for (const [recordIndex, record] of records.entries()) {
      let mapped: unknown;
      try {
        mapped = await mapper(record, {
          runId: run.id,
          batchId,
          input: run.input,
          boundary,
          cursor,
          recordIndex
        });
      } catch (cause) {
        mapperFailures.push(createMapperFailureEnvelope({
          recordIdentity: createMapperRecordIdentity(record, cursor, recordIndex),
          record,
          input: run.input,
          boundary,
          cursor,
          recordIndex,
          error: serializeFailure(cause)
        }));
        continue;
      }
      const mappedJobs = normalizeMappedJobs(mapped);
      if (mappedJobs.length === 0) {
        recordsSkipped += 1;
        continue;
      }
      recordsDispatched += 1;
      for (const [jobIndex, mappedJob] of mappedJobs.entries()) {
        const identity = `${mappedJob.identity}:${jobIndex}`;
        if (seenIdentities.has(identity)) {
          throw new QueuebitError({
            code: 'QB_COORDINATOR_INVALID',
            message: 'Mapper produced duplicate job identities within one Batch.',
            details: { batchId, identity }
          });
        }
        seenIdentities.add(identity);
        jobs.push(this.#prepareJob(definitionConfig.queue, run.id, batchId, mappedJob, identity));
      }
    }
    return { jobs, recordsDispatched, recordsSkipped, mapperFailures };
  }

  #prepareJob(
    queue: string,
    runId: string,
    batchId: string,
    mappedJob: QueuebitMappedJob,
    identity: string
  ): QueuebitInternalPreparedBatchJob {
    assertSegment('job name', mappedJob.name);
    assertSegment('job identity', mappedJob.identity);
    const jobId = this.#idGenerator();
    assertSegment('jobId', jobId);
    const options = normalizeJobOptions(mappedJob.options);
    const digest = createCanonicalDigest(mappedJob.data);
    const dataBytes = Buffer.byteLength(digest.json);
    if (dataBytes > this.#config.limits.maxJobDataBytes) {
      throw new QueuebitError({
        code: 'QB_DISPATCH_LIMIT_EXCEEDED',
        message: 'Mapped job data exceeds maxJobDataBytes.',
        details: { jobId, actual: dataBytes, limit: this.#config.limits.maxJobDataBytes }
      });
    }
    const observedAt = this.#now();
    const createdAt = observedAt.toISOString();
    const delayMs = options.delayMs ?? 0;
    const deduplicationKey = options.deduplicationKey ?? `run:${runId}:batch:${batchId}:job:${identity}`;
    const idempotencyKey = options.idempotencyKey ?? deduplicationKey;
    return {
      jobId,
      jobKey: this.#keys.job(jobId),
      dedupeKey: this.#keys.jobKey(createJobDedupeDigest(queue, deduplicationKey)),
      envelope: {
        jobId,
        queue,
        name: mappedJob.name,
        state: delayMs > 0 ? 'delayed' : 'waiting',
        attempts: options.attempts ?? 1,
        createdAt,
        updatedAt: createdAt,
        dataJson: digest.json,
        dataDigest: digest.sha256,
        dataBytes,
        optionsJson: canonicalizeInput(options),
        delayUntilMs: observedAt.getTime() + delayMs,
        deduplicationKey,
        idempotencyKey,
        recordIdentity: identity,
        runId,
        batchId
      },
      options
    };
  }

  async #dispatchBatch(input: {
    run: RunRecord;
    definitionConfig: QueuebitNormalizedBatchRunConfig;
    claim: ClaimRunResult;
    batchId: string;
    batchIndex: number;
    boundary: unknown;
    cursor: unknown;
    nextCursor: unknown;
    sourceExhausted: boolean;
    boundaryTotalRecords: number | undefined;
    recordsSeen: number;
    recordsDispatched: number;
    recordsSkipped: number;
    recordsFailed: number;
    recordsUndispatched: number;
    jobs: QueuebitInternalPreparedBatchJob[];
    mapperFailures: MapperFailureEnvelope[];
  }): Promise<DispatchBatchResult> {
    const bulkBytes = input.jobs.reduce((sum, job) => sum + job.envelope.dataBytes, 0);
    if (bulkBytes > this.#config.limits.maxBulkBytes) {
      throw new QueuebitError({
        code: 'QB_DISPATCH_LIMIT_EXCEEDED',
        message: 'Batch dispatch exceeds maxBulkBytes.',
        details: { actual: bulkBytes, limit: this.#config.limits.maxBulkBytes }
      });
    }
    const queueConfig = this.#config.queues[input.definitionConfig.queue];
    const observedAt = this.#now();
    const observedAtIso = observedAt.toISOString();
    const runCompletion = createCompletionEnvelope({
      id: `${input.run.id}:settled`,
      type: 'run.settled',
      runId: input.run.id,
      observedAt,
      ...(input.definitionConfig.completion.run === undefined
        ? {}
        : { handlerConfig: input.definitionConfig.completion.run })
    });
    const envelope = {
      runId: input.run.id,
      definition: input.run.definition,
      coordinatorId: this.coordinatorId,
      coordinatorGeneration: input.claim.coordinatorGeneration,
      batchId: input.batchId,
      batchIndex: input.batchIndex,
      expectedDispatchCursorJson: input.run.boundaryMissing ? '' : canonicalizeInput(input.cursor),
      nextCursorJson: canonicalizeInput(input.nextCursor),
      boundaryJson: canonicalizeInput(input.boundary),
      boundaryTotalRecords: input.boundaryTotalRecords ?? '',
      sourceExhausted: input.sourceExhausted,
      recordsSeen: input.recordsSeen,
      recordsDispatched: input.recordsDispatched,
      recordsSkipped: input.recordsSkipped,
      recordsFailed: input.recordsFailed,
      recordsUndispatched: input.recordsUndispatched,
      mapperFailures: input.mapperFailures,
      maxBulkJobs: this.#config.limits.maxBulkJobs,
      maxBulkBytes: this.#config.limits.maxBulkBytes,
      highJobs: queueConfig?.backpressure?.highWatermarkJobs ?? '',
      highBytes: queueConfig?.backpressure?.highWatermarkBytes ?? '',
      lowJobs: queueConfig?.backpressure?.lowWatermarkJobs ?? '',
      lowBytes: queueConfig?.backpressure?.lowWatermarkBytes ?? '',
      bulkBytes,
      runCompletion,
      nextDispatchAt:
        input.definitionConfig.dispatch.intervalMs > 0
          ? new Date(observedAt.getTime() + input.definitionConfig.dispatch.intervalMs).toISOString()
          : '',
      createdAt: observedAtIso,
      updatedAt: observedAtIso,
      entries: input.jobs.map(job => job.envelope)
    };
    const reply = await executeQueuebitScript(
      this.#redis,
      this.#scripts.dispatchBatch,
      [
        this.#keys.run(input.run.id),
        this.#keys.runBatches(input.run.id),
        this.#keys.batch(input.run.id, input.batchIndex),
        this.#keys.queueCounters(input.definitionConfig.queue),
        this.#keys.queueWaiting(input.definitionConfig.queue),
        this.#keys.queueDue(input.definitionConfig.queue),
        this.#keys.queueJobs(input.definitionConfig.queue),
        this.#keys.queueState(input.definitionConfig.queue, 'waiting'),
        this.#keys.queueState(input.definitionConfig.queue, 'delayed'),
        this.#keys.completionCounters(),
        this.#keys.completionsIndex(),
        this.#keys.completionsDetails(),
        this.#keys.completionsDue(),
        this.#keys.completion(runCompletion.id),
        this.#keys.failures(input.run.id),
        this.#keys.runsTerminalDetails(),
        ...input.jobs.map(job => job.jobKey),
        ...input.jobs.map(job => job.dedupeKey)
      ],
      [JSON.stringify(envelope)]
    );
    const [tag, batchIdOrCode, jobIdsOrMessage, deduplicatedOrDetails] = assertTaggedReply(reply);
    if (tag === 'ok') {
      return {
        batchId: String(batchIdOrCode),
        jobIds: JSON.parse(String(jobIdsOrMessage)) as string[],
        deduplicated: String(deduplicatedOrDetails) === '1'
      };
    }
    throw new QueuebitError({
      code: toCoordinatorErrorCode(String(batchIdOrCode)),
      message: String(jobIdsOrMessage ?? 'Coordinator could not dispatch Batch.'),
      details: parseOptionalJson(deduplicatedOrDetails)
    });
  }
}

function isTerminalRun(state: string): boolean {
  return state === 'completed' || state === 'partial_failed' || state === 'failed' || state === 'cancelled';
}

function parseDispatchHoldReason(value: string | undefined): DispatchHoldReason | undefined {
  if (value === undefined) return undefined;
  if (dispatchHoldReasons.has(value as DispatchHoldReason)) return value as DispatchHoldReason;
  throw new QueuebitError({
    code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
    message: `Redis stored an unknown dispatch hold reason: ${value}.`
  });
}

function normalizeRecoveryCursor(cursor: unknown): number {
  if (cursor === null || cursor === undefined) return 0;
  if (typeof cursor === 'number' && Number.isInteger(cursor) && cursor >= 0) return cursor;
  if (typeof cursor === 'string' && /^[0-9]+$/.test(cursor)) return Number.parseInt(cursor, 10);
  throw new QueuebitError({
    code: 'QB_COORDINATOR_INVALID',
    message: 'Recovery Run dispatch cursor is invalid.',
    details: { cursor }
  });
}

function parseRecoveryFailure(member: string, score: string): FailureRecord {
  const raw = JSON.parse(member) as Record<string, unknown>;
  const sequence = String(raw.sequence ?? score);
  const stage = parseFailureStage(String(raw.stage ?? 'processor'));
  const record: FailureRecord = {
    sequence,
    runId: String(raw.runId ?? ''),
    stage,
    recordIdentity: String(raw.recordIdentity ?? raw.jobId ?? sequence),
    attempt: Number(raw.attempt ?? 0),
    error: parseSerializedFailure(raw.error),
    recoveryAvailable: raw.recoveryAvailable === true
  };
  assignOptional(record, 'batchId', optionalString(raw.batchId));
  assignOptional(record, 'jobId', optionalString(raw.jobId));
  assignOptional(record, 'envelopeExpiresAt', optionalString(raw.envelopeExpiresAt));
  if (raw.payload !== undefined) {
    record.payload = raw.payload;
  }
  return record;
}

function parseFailureStage(value: string): 'mapper' | 'processor' {
  if (value === 'mapper' || value === 'processor') return value;
  throw new QueuebitError({
    code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
    message: `Redis stored an unknown failure stage: ${value}.`,
    details: { stage: value }
  });
}

function parseSerializedFailure(value: unknown): QueuebitSerializedError {
  if (value !== null && typeof value === 'object' && typeof (value as { message?: unknown }).message === 'string') {
    const raw = value as { name?: unknown; code?: unknown; message: string; details?: unknown };
    const serialized: QueuebitSerializedError = { message: raw.message };
    if (typeof raw.name === 'string') serialized.name = raw.name;
    if (typeof raw.code === 'string') serialized.code = raw.code;
    if (raw.details !== undefined) serialized.details = raw.details;
    return serialized;
  }
  return { name: 'Error', message: String(value ?? 'Job failed without a serialized reason.') };
}

function assertRecoveryProcessorPayload(
  failure: FailureRecord
): RecoveryProcessorPayload {
  const payload = failure.payload;
  if (
    payload === undefined
    || payload === null
    || typeof payload !== 'object'
    || typeof (payload as { name?: unknown }).name !== 'string'
    || !('data' in payload)
  ) {
    throw new QueuebitError({
      code: 'QB_COORDINATOR_INVALID',
      message: 'Processor failure envelope is missing replay payload.',
      details: { runId: failure.runId, sequence: failure.sequence }
    });
  }
  return payload as RecoveryProcessorPayload;
}

function assertRecoveryMapperPayload(failure: FailureRecord): RecoveryMapperPayload {
  const payload = failure.payload;
  if (
    payload === undefined
    || payload === null
    || typeof payload !== 'object'
    || !('record' in payload)
    || !('input' in payload)
    || !('boundary' in payload)
    || !('cursor' in payload)
    || typeof (payload as { recordIndex?: unknown }).recordIndex !== 'number'
  ) {
    throw new QueuebitError({
      code: 'QB_COORDINATOR_INVALID',
      message: 'Mapper failure envelope is missing replay payload.',
      details: { runId: failure.runId, sequence: failure.sequence }
    });
  }
  return payload as RecoveryMapperPayload;
}

function createRecoveryJobOptions(
  run: RunRecord,
  failure: FailureRecord,
  payload: RecoveryProcessorPayload
): JobAddOptions {
  const source = payload.options ?? {};
  const options: JobAddOptions = {};
  if (source.attempts !== undefined) options.attempts = source.attempts;
  if (source.timeoutMs !== undefined) options.timeoutMs = source.timeoutMs;
  if (source.backoff !== undefined) options.backoff = source.backoff;
  if (source.delayMs !== undefined) options.delayMs = source.delayMs;
  options.deduplicationKey = `run:${run.id}:recovery:${failure.sequence}`;
  const idempotencyKey = payload.idempotencyKey ?? source.idempotencyKey;
  if (idempotencyKey !== undefined && idempotencyKey.length > 0) {
    options.idempotencyKey = idempotencyKey;
  }
  return options;
}

function createRecoveryMapperJobOptions(
  run: RunRecord,
  failure: FailureRecord,
  source: JobAddOptions | undefined,
  jobIndex: number
): JobAddOptions {
  const options: JobAddOptions = {};
  if (source?.attempts !== undefined) options.attempts = source.attempts;
  if (source?.timeoutMs !== undefined) options.timeoutMs = source.timeoutMs;
  if (source?.backoff !== undefined) options.backoff = source.backoff;
  if (source?.delayMs !== undefined) options.delayMs = source.delayMs;
  options.deduplicationKey = `run:${run.id}:recovery:${failure.sequence}:${jobIndex}`;
  if (source?.idempotencyKey !== undefined && source.idempotencyKey.length > 0) {
    options.idempotencyKey = source.idempotencyKey;
  }
  return options;
}

function createMapperFailureEnvelope(input: {
  recordIdentity: string;
  record: unknown;
  input: unknown;
  boundary: unknown;
  cursor: unknown;
  recordIndex: number;
  error: QueuebitSerializedError;
}): MapperFailureEnvelope {
  return {
    recordIdentity: input.recordIdentity,
    errorJson: canonicalizeInput(input.error),
    payloadJson: canonicalizeInput({
      record: input.record,
      input: input.input,
      boundary: input.boundary,
      cursor: input.cursor,
      recordIndex: input.recordIndex
    })
  };
}

function createMapperRecordIdentity(record: unknown, cursor: unknown, recordIndex: number): string {
  const digest = createHash('sha256')
    .update(canonicalizeInput({ cursor, record, recordIndex }))
    .digest('hex')
    .slice(0, 16);
  return `record:${recordIndex}:${digest}`;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function assertSourceLoadResult(result: unknown): asserts result is {
  records: unknown[];
  nextCursor: unknown;
  exhausted: boolean;
} {
  if (
    result === null
    || typeof result !== 'object'
    || !Array.isArray((result as { records?: unknown }).records)
    || typeof (result as { exhausted?: unknown }).exhausted !== 'boolean'
    || !('nextCursor' in result)
  ) {
    throw new QueuebitError({
      code: 'QB_SOURCE_INVALID',
      message: 'Source.load returned an invalid result shape.'
    });
  }
}

function normalizeMappedJobs(mapped: unknown): QueuebitMappedJob[] {
  if (mapped === null || mapped === undefined) return [];
  const jobs = Array.isArray(mapped) ? mapped : [mapped];
  for (const job of jobs) {
    if (
      job === null
      || typeof job !== 'object'
      || typeof (job as QueuebitMappedJob).name !== 'string'
      || typeof (job as QueuebitMappedJob).identity !== 'string'
      || !('data' in job)
    ) {
      throw new QueuebitError({
        code: 'QB_COORDINATOR_INVALID',
        message: 'Mapper must return jobs with name, identity, and data.'
      });
    }
  }
  return jobs as QueuebitMappedJob[];
}

function normalizeJobOptions(options: JobAddOptions = {}): JobAddOptions {
  const normalized: JobAddOptions = {};
  if (options.attempts !== undefined) normalized.attempts = assertInteger('attempts', options.attempts, 1);
  if (options.timeoutMs !== undefined) normalized.timeoutMs = assertInteger('timeoutMs', options.timeoutMs, 1);
  if (options.delayMs !== undefined) normalized.delayMs = assertInteger('delayMs', options.delayMs, 0);
  if (options.deduplicationKey !== undefined) normalized.deduplicationKey = assertBoundedString('deduplicationKey', options.deduplicationKey);
  if (options.idempotencyKey !== undefined) normalized.idempotencyKey = assertBoundedString('idempotencyKey', options.idempotencyKey);
  if (options.backoff !== undefined) {
    if (options.backoff.type !== 'fixed' && options.backoff.type !== 'exponential') {
      throw new QueuebitError({ code: 'QB_COORDINATOR_INVALID', message: `Invalid backoff type: ${options.backoff.type}.` });
    }
    const backoff: NonNullable<JobAddOptions['backoff']> = {
      type: options.backoff.type,
      delayMs: assertInteger('backoff.delayMs', options.backoff.delayMs, 1)
    };
    if (options.backoff.maxDelayMs !== undefined) {
      backoff.maxDelayMs = assertInteger('backoff.maxDelayMs', options.backoff.maxDelayMs, 1);
    }
    if (options.backoff.jitter !== undefined) {
      if (typeof options.backoff.jitter !== 'number' || options.backoff.jitter < 0 || options.backoff.jitter > 1) {
        throw new QueuebitError({
          code: 'QB_COORDINATOR_INVALID',
          message: 'backoff.jitter must be between 0 and 1.',
          details: { jitter: options.backoff.jitter }
        });
      }
      backoff.jitter = options.backoff.jitter;
    }
    normalized.backoff = backoff;
  }
  return normalized;
}

function createJobDedupeDigest(queue: string, key: string): string {
  return createHash('sha256')
    .update('queuebit-job-dedupe-v1')
    .update('\0')
    .update(queue)
    .update('\0')
    .update(key)
    .digest('hex');
}

function redisHashToRecord(reply: unknown): Record<string, string> {
  if (Array.isArray(reply)) {
    const record: Record<string, string> = {};
    for (let index = 0; index < reply.length - 1; index += 2) {
      record[String(reply[index])] = String(reply[index + 1]);
    }
    return record;
  }
  if (reply !== null && typeof reply === 'object') {
    const record: Record<string, string> = {};
    for (const [key, value] of Object.entries(reply as Record<string, unknown>)) {
      record[key] = String(value);
    }
    return record;
  }
  return {};
}

function parseJsonArray(value: string): string[] {
  const parsed = JSON.parse(value);
  if (!Array.isArray(parsed) || !parsed.every(item => typeof item === 'string')) {
    throw new QueuebitError({
      code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
      message: 'Redis stored an invalid Batch jobIds array.',
      details: { value }
    });
  }
  return parsed;
}

function parseZrangeWithScores(reply: unknown): Array<{ member: string; score: string }> {
  if (!Array.isArray(reply)) {
    throw new QueuebitError({
      code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
      message: 'Redis returned an invalid Batch index reply.',
      details: { reply }
    });
  }
  const pairs: Array<{ member: string; score: string }> = [];
  for (let index = 0; index < reply.length - 1; index += 2) {
    pairs.push({ member: String(reply[index]), score: String(reply[index + 1]) });
  }
  return pairs;
}

function requiredField(record: Record<string, string>, field: string): string {
  const value = record[field];
  if (value === undefined) {
    throw new QueuebitError({
      code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
      message: `Redis run snapshot is missing field: ${field}.`,
      details: { field }
    });
  }
  return value;
}

function assertTaggedReply(reply: unknown): unknown[] {
  if (!Array.isArray(reply) || (reply[0] !== 'ok' && reply[0] !== 'err')) {
    throw new QueuebitError({
      code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
      message: 'Redis script returned an unknown reply shape.',
      details: { reply }
    });
  }
  return reply;
}

function toCoordinatorErrorCode(code: string): QueuebitErrorCode {
  if (
    code === 'QB_RUN_NOT_FOUND'
    || code === 'QB_RUN_STATE_CONFLICT'
    || code === 'QB_JOB_DEDUPLICATION_CONFLICT'
    || code === 'QB_DISPATCH_STATE_CONFLICT'
    || code === 'QB_DISPATCH_LIMIT_EXCEEDED'
    || code === 'QB_BACKPRESSURE_REJECTED'
    || code === 'QB_BACKPRESSURE_REQUEST_TOO_LARGE'
    || code === 'QB_COMPLETION_INVALID'
    || code === 'QB_COMPLETION_NOT_FOUND'
    || code === 'QB_COMPLETION_STATE_CONFLICT'
  ) {
    return code;
  }
  return 'QB_REDIS_SCRIPT_EXECUTION_FAILED';
}

function assertSegment(label: string, value: string): string {
  if (!segmentPattern.test(value)) {
    throw new QueuebitError({
      code: 'QB_COORDINATOR_INVALID',
      message: `Invalid ${label}: "${value}".`,
      details: { label, value }
    });
  }
  return value;
}

function assertInteger(label: string, value: number, minimum: number): number {
  if (!Number.isInteger(value) || value < minimum) {
    throw new QueuebitError({
      code: 'QB_COORDINATOR_INVALID',
      message: `${label} must be an integer >= ${minimum}.`,
      details: { label, value, minimum }
    });
  }
  return value;
}

function normalizePositiveInteger(label: string, value: number): number {
  return assertInteger(label, value, 1);
}

function normalizeCompletionDeliveryLimit(limit: number): number {
  if (!Number.isInteger(limit) || limit < 1 || limit > 100) {
    throw new QueuebitError({
      code: 'QB_COMPLETION_INVALID',
      message: 'Completion delivery limit must be an integer between 1 and 100.',
      details: { limit }
    });
  }
  return limit;
}

function assertBoundedString(label: string, value: string): string {
  if (typeof value !== 'string' || value.length === 0 || value.length > 512) {
    throw new QueuebitError({
      code: 'QB_COORDINATOR_INVALID',
      message: `${label} must be a non-empty string of at most 512 characters.`,
      details: { label }
    });
  }
  return value;
}

function completionSnapshotToEvent(snapshot: CompletionSnapshot): QueuebitCompletionEvent {
  const event: QueuebitCompletionEvent = {
    id: snapshot.id,
    type: snapshot.type,
    runId: snapshot.runId,
    handler: requiredCompletionHandler(snapshot),
    attempt: snapshot.attempt,
    deliveryGeneration: snapshot.deliveryGeneration,
    summary: requiredCompletionSummary(snapshot)
  };
  if (snapshot.batchId !== undefined) event.batchId = snapshot.batchId;
  return event;
}

function requiredCompletionHandler(snapshot: CompletionSnapshot): string {
  if (snapshot.handler === undefined || snapshot.handler.length === 0) {
    throw new QueuebitError({
      code: 'QB_CONFIG_INVALID',
      message: 'Completion event has no handler.',
      details: { eventId: snapshot.id }
    });
  }
  return snapshot.handler;
}

function requiredCompletionSummary(snapshot: CompletionSnapshot): unknown {
  if (snapshot.summary === undefined) {
    throw new QueuebitError({
      code: 'QB_COMPLETION_STATE_CONFLICT',
      message: 'Completion event details have expired.',
      details: { eventId: snapshot.id, detailsExpired: snapshot.detailsExpired === true }
    });
  }
  return snapshot.summary;
}

function serializeFailure(cause: unknown): QueuebitSerializedError {
  if (typeof cause === 'string') return { name: 'Error', message: cause };
  if (cause instanceof QueuebitError) {
    const serialized: QueuebitSerializedError = {
      name: cause.name,
      code: cause.code,
      message: cause.message
    };
    if (cause.details !== undefined) serialized.details = cause.details;
    return serialized;
  }
  if (cause instanceof Error) {
    return { name: cause.name, message: cause.message };
  }
  return { name: 'Error', message: 'Completion handler failed with a non-Error value.', details: cause };
}

function computeCompletionNextDueMs(snapshot: CompletionSnapshot, observedAt: Date): number {
  const backoff = snapshot.backoff;
  if (backoff === undefined) return observedAt.getTime();
  const multiplier = backoff.type === 'exponential'
    ? 2 ** Math.max(0, snapshot.attempt - 1)
    : 1;
  const uncappedDelay = backoff.delayMs * multiplier;
  const delayMs = backoff.maxDelayMs === undefined
    ? uncappedDelay
    : Math.min(uncappedDelay, backoff.maxDelayMs);
  return observedAt.getTime() + delayMs;
}

function parseBatchIndex(batchId: string): number {
  const marker = ':batch:';
  const markerIndex = batchId.lastIndexOf(marker);
  if (markerIndex === -1) {
    throw new QueuebitError({
      code: 'QB_COMPLETION_INVALID',
      message: 'Completion batchId does not contain a batch index.',
      details: { batchId }
    });
  }
  const parsed = Number.parseInt(batchId.slice(markerIndex + marker.length), 10);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new QueuebitError({
      code: 'QB_COMPLETION_INVALID',
      message: 'Completion batchId contains an invalid batch index.',
      details: { batchId }
    });
  }
  return parsed;
}

function createCompletionEnvelope(input: {
  id: string;
  type: QueuebitCompletionEvent['type'];
  runId: string;
  batchId?: string;
  handlerConfig?: QueuebitCompletionHandlerConfig;
  observedAt: Date;
  summaryJson?: string;
}): CompletionEventEnvelope {
  return {
    id: input.id,
    type: input.type,
    runId: input.runId,
    batchId: input.batchId ?? '',
    handler: input.handlerConfig?.handler ?? '',
    attempts: input.handlerConfig?.attempts ?? 1,
    backoffJson: input.handlerConfig?.backoff === undefined
      ? ''
      : canonicalizeInput(input.handlerConfig.backoff),
    summaryJson: input.summaryJson ?? '',
    createdAt: input.observedAt.toISOString(),
    updatedAt: input.observedAt.toISOString(),
    nowMs: input.observedAt.getTime()
  };
}

function runSummaryJsonFromHash(record: Record<string, string>): string {
  return canonicalizeInput({
    recordsSeen: numberHashField(record, 'recordsSeen'),
    recordsDispatched: numberHashField(record, 'recordsDispatched'),
    recordsSkipped: numberHashField(record, 'recordsSkipped'),
    recordsFailed: numberHashField(record, 'recordsFailed'),
    recordsUndispatched: numberHashField(record, 'recordsUndispatched'),
    boundaryTotalRecords: requiredField(record, 'boundaryTotalRecords'),
    jobsCreated: numberHashField(record, 'jobsCreated'),
    jobsCompleted: numberHashField(record, 'jobsCompleted'),
    jobsFailed: numberHashField(record, 'jobsFailed'),
    jobsCancelled: numberHashField(record, 'jobsCancelled')
  });
}

function numberHashField(record: Record<string, string>, field: string): number {
  return Number.parseInt(requiredField(record, field), 10);
}

function throwIfAborted(signal: AbortSignal) {
  if (!signal.aborted) return;
  throw new QueuebitError({
    code: 'QB_COMPLETION_STATE_CONFLICT',
    message: 'Completion delivery was aborted.',
    details: { reason: String(signal.reason ?? 'aborted') }
  });
}

function parseOptionalJson(value: unknown): unknown {
  if (value === undefined) return undefined;
  try {
    return JSON.parse(String(value));
  } catch {
    return value;
  }
}

interface LinkedAbortController {
  controller: AbortController;
  dispose(): void;
}

function createLinkedAbortController(parentSignal: AbortSignal | undefined): LinkedAbortController {
  const controller = new AbortController();
  if (parentSignal === undefined) return { controller, dispose() {} };
  if (parentSignal.aborted) {
    controller.abort(parentSignal.reason);
    return { controller, dispose() {} };
  }
  const onAbort = () => controller.abort(parentSignal.reason);
  parentSignal.addEventListener('abort', onAbort, { once: true });
  return {
    controller,
    dispose() {
      parentSignal.removeEventListener('abort', onAbort);
    }
  };
}

function assignOptional<T extends object, K extends keyof T>(target: T, key: K, value: T[K] | undefined) {
  if (value !== undefined) target[key] = value;
}
