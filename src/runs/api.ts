import { createHash, randomUUID } from 'node:crypto';
import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import { canonicalizeInput, createCanonicalDigest } from '../canonical';
import type { QueuebitConfig } from '../config';
import { QueuebitError, type QueuebitErrorCode } from '../errors';
import {
  createQueuebitKeyBuilder,
  executeQueuebitScript,
  type QueuebitRedisCommandClient
} from '../redis';
import { registerRunsScripts } from './scripts';
import type {
  CompletionState,
  CursorPage,
  FailureListQuery,
  FailureRecord,
  FailureStage,
  RunCancelRequest,
  RunExecutionState,
  RunListQuery,
  RunRetryFailedRequest,
  RunSnapshot,
  RunStartRequest,
  RunStartResult,
  RunSummary,
  RunsApi
} from './types';

export interface QueuebitRunsApiOptions {
  config: QueuebitConfig;
  redis: QueuebitRedisCommandClient;
  now?: () => Date;
  idGenerator?: () => string;
}

const segmentPattern = /^[A-Za-z0-9][A-Za-z0-9:_-]{0,191}$/;
const executionStates = new Set<RunExecutionState>([
  'created',
  'running',
  'pausing',
  'paused',
  'blocked',
  'cancelling',
  'completed',
  'partial_failed',
  'failed',
  'cancelled'
]);
const completionStates = new Set<CompletionState>([
  'not_created',
  'not_required',
  'pending',
  'delivering',
  'retrying',
  'delivered',
  'failed'
]);
const failureStages = new Set<FailureStage>(['mapper', 'processor']);
const dispatchHoldReasons = new Set<NonNullable<RunSnapshot['dispatchHoldReason']>>([
  'interval',
  'in_flight_limit',
  'backpressure',
  'no_active_worker',
  'redis_reconnecting'
]);
const maxRecoveryFailures = 1000;
const inputAjv = new Ajv2020({
  strict: true,
  allErrors: true,
  coerceTypes: false,
  useDefaults: false,
  removeAdditional: false
});
addFormats(inputAjv, ['date-time', 'uuid', 'email']);

export function createQueuebitRunsApi(options: QueuebitRunsApiOptions): RunsApi {
  const keys = createQueuebitKeyBuilder(options.config);
  const scripts = registerRunsScripts();
  const now = options.now ?? (() => new Date());
  const idGenerator = options.idGenerator ?? (() => randomUUID());

  async function start<Input>(
    definition: string,
    request: RunStartRequest<Input>
  ): Promise<RunStartResult<Input>> {
    assertSegment('definition', definition);
    const definitionConfig = options.config.batchRuns[definition];
    if (definitionConfig === undefined) {
      throw new QueuebitError({
        code: 'QB_RUN_DEFINITION_NOT_FOUND',
        message: `BatchRun definition "${definition}" is not declared in Queuebit config.`,
        details: { definition }
      });
    }
    assertInputSchema(definition, definitionConfig.inputSchema, request.input);
    const idempotencyKey = assertBoundedString('idempotencyKey', request.idempotencyKey);
    const digest = createCanonicalDigest(request.input);
    if (Buffer.byteLength(digest.json) > options.config.limits.maxRunInputBytes) {
      throw new QueuebitError({
        code: 'QB_RUN_INVALID',
        message: 'Run input exceeds maxRunInputBytes.',
        details: { limit: options.config.limits.maxRunInputBytes }
      });
    }
    const runId = idGenerator();
    assertSegment('runId', runId);
    const observedAt = now();
    const runKeyDigest = createRunKeyDigest(definition, idempotencyKey);
    const reply = await executeQueuebitScript(
      options.redis,
      scripts.start,
      [
        keys.queueCounters('runs'),
        keys.runsRunnable(),
        keys.runKey(definition, runKeyDigest),
        keys.run(runId)
      ],
      [
        canonicalizeInput({
          runId,
          definition,
          definitionVersion: definitionConfig.version,
          inputJson: digest.json,
          inputDigest: digest.sha256,
          idempotencyKey,
          createdAt: observedAt.toISOString()
        })
      ]
    );
    const { runId: createdOrExistingRunId, deduplicated } = parseStartReply(reply);
    const snapshot = await getRunSnapshot<Input>(options.redis, keys.run(createdOrExistingRunId));
    if (snapshot === null) {
      throw new QueuebitError({
        code: 'QB_RUN_NOT_FOUND',
        message: 'Run could not be read back after start.',
        details: { runId: createdOrExistingRunId }
      });
    }
    return { ...snapshot, deduplicated };
  }

  async function get<Input = unknown, Boundary = unknown, Cursor = unknown>(
    runId: string
  ): Promise<RunSnapshot<Input, Boundary, Cursor> | null> {
    assertSegment('runId', runId);
    return getRunSnapshot<Input, Boundary, Cursor>(options.redis, keys.run(runId));
  }

  async function list(query: RunListQuery = {}): Promise<CursorPage<RunSummary>> {
    if (query.definition !== undefined) assertSegment('definition', query.definition);
    if (query.executionState !== undefined && !executionStates.has(query.executionState)) {
      throw new QueuebitError({
        code: 'QB_RUN_INVALID',
        message: `Invalid run execution state filter: ${query.executionState}.`,
        details: { executionState: query.executionState }
      });
    }
    const limit = normalizeListLimit(query.limit);
    const cursor = normalizeCursor(query.cursor);
    const reply = await options.redis.sendCommand([
      'ZRANGEBYSCORE',
      keys.runsRunnable(),
      cursor === undefined ? '-inf' : `(${cursor}`,
      '+inf',
      'WITHSCORES',
      'LIMIT',
      '0',
      String(limit + 1)
    ]);
    const pairs = parseZrangeWithScores(reply);
    const visible = pairs.slice(0, limit);
    const snapshots = await Promise.all(visible.map(pair => getRunSnapshot(options.redis, keys.run(pair.member))));
    const items = snapshots
      .filter((snapshot): snapshot is RunSnapshot => snapshot !== null)
      .filter(snapshot => query.definition === undefined || snapshot.definition === query.definition)
      .filter(snapshot => query.executionState === undefined || snapshot.executionState === query.executionState)
      .map(toRunSummary);
    const page: CursorPage<RunSummary> = { items };
    if (pairs.length > limit && visible.length > 0) {
      const last = visible[visible.length - 1];
      if (last) page.nextCursor = last.score;
    }
    return page;
  }

  async function listFailures<Payload = unknown>(
    runId: string,
    query: FailureListQuery = {}
  ): Promise<CursorPage<FailureRecord<Payload>>> {
    assertSegment('runId', runId);
    if (query.stage !== undefined && !failureStages.has(query.stage)) {
      throw new QueuebitError({
        code: 'QB_RUN_INVALID',
        message: `Invalid failure stage filter: ${query.stage}.`,
        details: { stage: query.stage }
      });
    }
    const run = await readRequiredRun(options.redis, keys.run(runId), runId);
    if (run.detailsExpired || run.failureDetailsExpired) {
      throw new QueuebitError({
        code: 'QB_RUN_STATE_CONFLICT',
        message: 'Run failure details have expired and cannot be listed.',
        details: { runId }
      });
    }
    const limit = normalizeListLimit(query.limit);
    let cursor = normalizeCursor(query.cursor);
    const items: Array<FailureRecord<Payload>> = [];
    let scannedMore = false;

    while (items.length < limit) {
      const pairs = await readFailurePairs(options.redis, keys.failures(runId), cursor, limit + 1);
      if (pairs.length === 0) break;
      scannedMore = pairs.length > limit;
      for (const pair of pairs) {
        cursor = pair.score;
        const failure = parseFailureRecord<Payload>(pair.member, pair.score, query.includePayload === true);
        if (query.stage !== undefined && failure.stage !== query.stage) continue;
        items.push(failure);
        if (items.length === limit) break;
      }
      if (!scannedMore || items.length === limit) break;
    }

    const page: CursorPage<FailureRecord<Payload>> = { items };
    if (items.length === limit && cursor !== undefined) page.nextCursor = cursor;
    return page;
  }

  async function pause<Input = unknown, Boundary = unknown, Cursor = unknown>(
    runId: string
  ): Promise<RunSnapshot<Input, Boundary, Cursor>> {
    assertSegment('runId', runId);
    const observedAt = now();
    const reply = await executeQueuebitScript(
      options.redis,
      scripts.pause,
      [keys.run(runId)],
      [observedAt.toISOString()]
    );
    const pausedRunId = parseControlReply(reply, 'runs.pause');
    return readRequiredRun<Input, Boundary, Cursor>(options.redis, keys.run(pausedRunId), pausedRunId);
  }

  async function resume<Input = unknown, Boundary = unknown, Cursor = unknown>(
    runId: string
  ): Promise<RunSnapshot<Input, Boundary, Cursor>> {
    assertSegment('runId', runId);
    const observedAt = now();
    const reply = await executeQueuebitScript(
      options.redis,
      scripts.resume,
      [keys.run(runId)],
      [observedAt.toISOString()]
    );
    const resumedRunId = parseControlReply(reply, 'runs.resume');
    return readRequiredRun<Input, Boundary, Cursor>(options.redis, keys.run(resumedRunId), resumedRunId);
  }

  async function cancel<Input = unknown, Boundary = unknown, Cursor = unknown>(
    runId: string,
    request: RunCancelRequest
  ): Promise<RunSnapshot<Input, Boundary, Cursor>> {
    assertSegment('runId', runId);
    const reason = assertBoundedString('reason', request.reason);
    const runRecord = await getRunRecord(options.redis, keys.run(runId));
    if (Object.keys(runRecord).length === 0) {
      throw new QueuebitError({
        code: 'QB_RUN_NOT_FOUND',
        message: 'Run does not exist.',
        details: { runId }
      });
    }
    const definition = requiredField(runRecord, 'definition');
    const definitionConfig = options.config.batchRuns[definition];
    if (definitionConfig === undefined) {
      throw new QueuebitError({
        code: 'QB_RUN_DEFINITION_NOT_FOUND',
        message: `BatchRun definition "${definition}" is not declared in Queuebit config.`,
        details: { definition }
      });
    }
    const observedAt = now();
    const jobIds = await readRunJobIds(options.redis, keys, runId);
    const handlerConfig = definitionConfig.completion.run;
    const runCompletion = {
      id: `${runId}:cancelled`,
      type: 'run.cancelled',
      runId,
      handler: handlerConfig?.handler ?? '',
      attempts: handlerConfig?.attempts ?? 1,
      backoffJson: handlerConfig?.backoff === undefined ? '' : canonicalizeInput(handlerConfig.backoff)
    };
    const reply = await executeQueuebitScript(
      options.redis,
      scripts.cancel,
      [
        keys.run(runId),
        keys.completionCounters(),
        keys.completionsIndex(),
        keys.completionsDetails(),
        keys.completionsDue(),
        keys.completion(runCompletion.id),
        keys.runsTerminalDetails(),
        keys.queueWaiting(definitionConfig.queue),
        keys.queueDue(definitionConfig.queue),
        keys.queueState(definitionConfig.queue, 'waiting'),
        keys.queueState(definitionConfig.queue, 'delayed'),
        keys.queueState(definitionConfig.queue, 'retrying'),
        keys.queueState(definitionConfig.queue, 'cancelled'),
        keys.queueCounters(definitionConfig.queue),
        ...jobIds.map(jobId => keys.job(jobId))
      ],
      [JSON.stringify({
        runId,
        reason,
        jobCount: jobIds.length,
        runCompletion,
        nowMs: observedAt.getTime(),
        updatedAt: observedAt.toISOString()
      })]
    );
    const cancelledRunId = parseControlReply(reply, 'runs.cancel');
    return readRequiredRun<Input, Boundary, Cursor>(options.redis, keys.run(cancelledRunId), cancelledRunId);
  }

  async function retryFailed<Input = unknown, Boundary = unknown, Cursor = unknown>(
    runId: string,
    request: RunRetryFailedRequest
  ): Promise<RunSnapshot<Input, Boundary, Cursor>> {
    assertSegment('runId', runId);
    const idempotencyKey = assertBoundedString('idempotencyKey', request.idempotencyKey);
    const parent = await readRequiredRun<Input, Boundary, Cursor>(options.redis, keys.run(runId), runId);
    if (parent.executionState !== 'partial_failed' && parent.executionState !== 'failed') {
      throw new QueuebitError({
        code: 'QB_RUN_STATE_CONFLICT',
        message: 'runs.retryFailed requires a terminal failed or partial_failed Run.',
        details: { runId, state: parent.executionState }
      });
    }
    if (parent.detailsExpired || parent.failureDetailsExpired) {
      throw new QueuebitError({
        code: 'QB_RUN_STATE_CONFLICT',
        message: 'runs.retryFailed cannot replay expired Run failure details.',
        details: { runId }
      });
    }
    const definitionConfig = options.config.batchRuns[parent.definition];
    if (definitionConfig === undefined) {
      throw new QueuebitError({
        code: 'QB_RUN_DEFINITION_NOT_FOUND',
        message: `BatchRun definition "${parent.definition}" is not declared in Queuebit config.`,
        details: { definition: parent.definition }
      });
    }
    const requestedVersion = request.definitionVersion ?? parent.definitionVersion;
    if (requestedVersion !== definitionConfig.version) {
      throw new QueuebitError({
        code: 'QB_RUN_STATE_CONFLICT',
        message: 'runs.retryFailed cannot replay failures with an unavailable BatchRun definition version.',
        details: {
          runId,
          requestedVersion,
          availableVersion: definitionConfig.version,
          parentVersion: parent.definitionVersion
        }
      });
    }
    const failures = await readRecoverableFailures(options.redis, keys.failures(runId));
    if (failures.length === 0) {
      throw new QueuebitError({
        code: 'QB_RUN_STATE_CONFLICT',
        message: 'Run has no recoverable failure envelopes.',
        details: { runId }
      });
    }
    const failureDigest = createCanonicalDigest(failures.map(toFailureDigestInput));
    const stages = [...new Set(failures.map(failure => failure.stage))].sort();
    const recoveryInput = {
      queuebitRecovery: {
        parentRunId: runId,
        definition: parent.definition,
        definitionVersion: requestedVersion,
        failureDigest: failureDigest.sha256,
        failureCount: failures.length,
        stages
      }
    };
    const inputDigest = createCanonicalDigest(recoveryInput);
    if (Buffer.byteLength(inputDigest.json) > options.config.limits.maxRunInputBytes) {
      throw new QueuebitError({
        code: 'QB_RUN_INVALID',
        message: 'Recovery Run input exceeds maxRunInputBytes.',
        details: { limit: options.config.limits.maxRunInputBytes }
      });
    }
    const recoveryRunId = idGenerator();
    assertSegment('runId', recoveryRunId);
    const observedAt = now();
    const runKeyDigest = createRunKeyDigest(parent.definition, idempotencyKey);
    const reply = await executeQueuebitScript(
      options.redis,
      scripts.start,
      [
        keys.queueCounters('runs'),
        keys.runsRunnable(),
        keys.runKey(parent.definition, runKeyDigest),
        keys.run(recoveryRunId)
      ],
      [
        canonicalizeInput({
          runId: recoveryRunId,
          definition: parent.definition,
          definitionVersion: requestedVersion,
          inputJson: inputDigest.json,
          inputDigest: inputDigest.sha256,
          idempotencyKey,
          createdAt: observedAt.toISOString(),
          parentRunId: runId,
          recoveryDepth: parent.recoveryDepth + 1,
          recoveryParentRunId: runId,
          recoveryFailureDigest: failureDigest.sha256,
          recoveryFailureCount: failures.length,
          recoveryStage: stages.length === 1 ? stages[0] : 'mixed'
        })
      ]
    );
    const { runId: createdOrExistingRunId } = parseStartReply(reply);
    return readRequiredRun<Input, Boundary, Cursor>(
      options.redis,
      keys.run(createdOrExistingRunId),
      createdOrExistingRunId
    );
  }

  return { start, get, list, listFailures, pause, resume, cancel, retryFailed };
}

function createRunKeyDigest(definition: string, idempotencyKey: string): string {
  return createHash('sha256')
    .update('queuebit-run-key-v1')
    .update('\0')
    .update(definition)
    .update('\0')
    .update(idempotencyKey)
    .digest('hex');
}

async function readFailurePairs(
  redis: QueuebitRedisCommandClient,
  failuresKey: string,
  cursor: string | undefined,
  limit: number
): Promise<Array<{ member: string; score: string }>> {
  const reply = await redis.sendCommand([
    'ZRANGEBYSCORE',
    failuresKey,
    cursor === undefined ? '-inf' : `(${cursor}`,
    '+inf',
    'WITHSCORES',
    'LIMIT',
    '0',
    String(limit)
  ]);
  return parseZrangeWithScores(reply);
}

async function readRecoverableFailures(
  redis: QueuebitRedisCommandClient,
  failuresKey: string
): Promise<Array<FailureRecord>> {
  const pairs = await readFailurePairs(redis, failuresKey, undefined, maxRecoveryFailures + 1);
  if (pairs.length > maxRecoveryFailures) {
    throw new QueuebitError({
      code: 'QB_RUN_STATE_CONFLICT',
      message: 'runs.retryFailed currently supports at most 1000 failure envelopes per recovery Run.',
      details: { limit: maxRecoveryFailures }
    });
  }
  return pairs
    .map(pair => parseFailureRecord(
      pair.member,
      pair.score,
      true
    ))
    .filter(failure => failure.recoveryAvailable);
}

function parseFailureRecord<Payload = unknown>(
  member: string,
  score: string,
  includePayload: boolean
): FailureRecord<Payload> {
  const raw = JSON.parse(member) as Record<string, unknown>;
  const stage = parseFailureStage(String(raw.stage ?? 'processor'));
  const record: FailureRecord<Payload> = {
    sequence: String(raw.sequence ?? score),
    runId: String(raw.runId ?? ''),
    stage,
    recordIdentity: String(raw.recordIdentity ?? raw.jobId ?? raw.sequence ?? score),
    attempt: Number(raw.attempt ?? 0),
    error: parseFailureError(raw.error),
    recoveryAvailable: raw.recoveryAvailable === true
  };
  assignOptional(record, 'batchId', optionalString(raw.batchId));
  assignOptional(record, 'jobId', optionalString(raw.jobId));
  assignOptional(record, 'envelopeExpiresAt', optionalString(raw.envelopeExpiresAt));
  if (includePayload && raw.payload !== undefined) {
    record.payload = raw.payload as Payload;
  }
  return record;
}

function parseFailureStage(value: string): FailureStage {
  if (failureStages.has(value as FailureStage)) return value as FailureStage;
  throw new QueuebitError({
    code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
    message: `Redis stored an unknown failure stage: ${value}.`,
    details: { stage: value }
  });
}

function parseFailureError(value: unknown): FailureRecord['error'] {
  if (value !== null && typeof value === 'object' && typeof (value as { message?: unknown }).message === 'string') {
    const raw = value as { name?: unknown; code?: unknown; message: string; details?: unknown };
    const error: FailureRecord['error'] = { message: raw.message };
    if (typeof raw.name === 'string') error.name = raw.name;
    if (typeof raw.code === 'string') error.code = raw.code;
    if (raw.details !== undefined) error.details = raw.details;
    return error;
  }
  return { name: 'Error', message: String(value ?? 'Job failed without a serialized reason.') };
}

function toFailureDigestInput(failure: FailureRecord): Record<string, unknown> {
  return {
    sequence: failure.sequence,
    runId: failure.runId,
    batchId: failure.batchId ?? '',
    jobId: failure.jobId ?? '',
    stage: failure.stage,
    recordIdentity: failure.recordIdentity
  };
}

function optionalString(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function parseStartReply(reply: unknown): { runId: string; deduplicated: boolean } {
  const [tag, codeOrBody, deduplicatedOrMessage, detailsJson] = assertTaggedReply(reply);
  if (tag === 'ok') {
    const runId = String(codeOrBody ?? '');
    if (runId.length === 0) {
      throw new QueuebitError({
        code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
        message: 'runs.start script did not return a run id.'
      });
    }
    return { runId, deduplicated: String(deduplicatedOrMessage) === '1' };
  }
  throw new QueuebitError({
    code: toRunErrorCode(String(codeOrBody)),
    message: String(deduplicatedOrMessage ?? 'runs.start failed.'),
    details: parseOptionalJson(detailsJson)
  });
}

function parseControlReply(reply: unknown, operation: string): string {
  const [tag, runIdOrCode, message, detailsJson] = assertTaggedReply(reply);
  if (tag === 'ok') {
    const runId = String(runIdOrCode ?? '');
    if (runId.length > 0) return runId;
    throw new QueuebitError({
      code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
      message: `${operation} script did not return a run id.`
    });
  }
  throw new QueuebitError({
    code: toRunErrorCode(String(runIdOrCode)),
    message: String(message ?? `${operation} failed.`),
    details: parseOptionalJson(detailsJson)
  });
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

function toRunErrorCode(code: string): QueuebitErrorCode {
  if (
    code === 'QB_RUN_INVALID'
    || code === 'QB_RUN_INPUT_INVALID'
    || code === 'QB_RUN_NOT_FOUND'
    || code === 'QB_RUN_DEFINITION_NOT_FOUND'
    || code === 'QB_RUN_DEDUPLICATION_CONFLICT'
    || code === 'QB_RUN_STATE_CONFLICT'
  ) {
    return code;
  }
  return 'QB_REDIS_SCRIPT_EXECUTION_FAILED';
}

function assertInputSchema(definition: string, schema: Record<string, unknown> | undefined, input: unknown) {
  if (schema === undefined) return;
  const validate = inputAjv.compile(schema);
  if (!validate(input)) {
    throw new QueuebitError({
      code: 'QB_RUN_INPUT_INVALID',
      message: `Run input for definition "${definition}" does not match inputSchema.`,
      details: { definition, errors: validate.errors ?? [] }
    });
  }
}

async function getRunSnapshot<Input = unknown, Boundary = unknown, Cursor = unknown>(
  redis: QueuebitRedisCommandClient,
  runKey: string
): Promise<RunSnapshot<Input, Boundary, Cursor> | null> {
  const record = await getRunRecord(redis, runKey);
  if (Object.keys(record).length === 0) return null;
  return hashRecordToRunSnapshot<Input, Boundary, Cursor>(record);
}

async function readRequiredRun<Input = unknown, Boundary = unknown, Cursor = unknown>(
  redis: QueuebitRedisCommandClient,
  runKey: string,
  runId: string
): Promise<RunSnapshot<Input, Boundary, Cursor>> {
  const snapshot = await getRunSnapshot<Input, Boundary, Cursor>(redis, runKey);
  if (snapshot === null) {
    throw new QueuebitError({
      code: 'QB_RUN_NOT_FOUND',
      message: 'Run could not be read back after control operation.',
      details: { runId }
    });
  }
  return snapshot;
}

async function getRunRecord(
  redis: QueuebitRedisCommandClient,
  runKey: string
): Promise<Record<string, string>> {
  const reply = await redis.sendCommand(['HGETALL', runKey]);
  return redisHashToRecord(reply);
}

async function readRunJobIds(
  redis: QueuebitRedisCommandClient,
  keys: ReturnType<typeof createQueuebitKeyBuilder>,
  runId: string
): Promise<string[]> {
  const reply = await redis.sendCommand([
    'ZRANGEBYSCORE',
    keys.runBatches(runId),
    '-inf',
    '+inf',
    'WITHSCORES',
    'LIMIT',
    '0',
    '1000'
  ]);
  const batches = parseZrangeWithScores(reply);
  const jobIds = new Set<string>();
  for (const batch of batches) {
    const batchIndex = parseBatchIndex(batch.member);
    const record = await getRunRecord(redis, keys.batch(runId, batchIndex));
    const raw = record.jobIds;
    if (raw === undefined) continue;
    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) continue;
    for (const jobId of parsed) {
      if (typeof jobId === 'string' && jobId.length > 0) jobIds.add(jobId);
    }
  }
  return [...jobIds];
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

function hashRecordToRunSnapshot<Input = unknown, Boundary = unknown, Cursor = unknown>(
  record: Record<string, string>
): RunSnapshot<Input, Boundary, Cursor> {
  const snapshot: RunSnapshot<Input, Boundary, Cursor> = {
    id: requiredField(record, 'id'),
    definition: requiredField(record, 'definition'),
    definitionVersion: numberField(record, 'definitionVersion'),
    recoveryDepth: numberField(record, 'recoveryDepth'),
    executionState: parseExecutionState(requiredField(record, 'executionState')),
    completionState: parseCompletionState(requiredField(record, 'completionState')),
    recordsSeen: numberField(record, 'recordsSeen'),
    recordsDispatched: numberField(record, 'recordsDispatched'),
    recordsSkipped: numberField(record, 'recordsSkipped'),
    recordsFailed: numberField(record, 'recordsFailed'),
    recordsUndispatched: numberField(record, 'recordsUndispatched'),
    boundaryTotalRecords: nullableNumberField(record, 'boundaryTotalRecords'),
    jobsCreated: numberField(record, 'jobsCreated'),
    jobsCompleted: numberField(record, 'jobsCompleted'),
    jobsFailed: numberField(record, 'jobsFailed'),
    jobsCancelled: numberField(record, 'jobsCancelled'),
    checkpointBatchIndex: optionalNumberField(record, 'checkpointBatchIndex', 0),
    createdAt: requiredField(record, 'createdAt'),
    updatedAt: requiredField(record, 'updatedAt'),
    sourceExhausted: requiredField(record, 'sourceExhausted') === '1',
    inFlightBatches: numberField(record, 'inFlightBatches')
  };
  assignOptional(snapshot, 'inputDigest', record.inputDigest);
  assignOptional(snapshot, 'detailsExpiredAt', record.detailsExpiredAt);
  if (record.detailsExpired === '1') snapshot.detailsExpired = true;
  if (record.failureDetailsExpired === '1') snapshot.failureDetailsExpired = true;
  assignOptional(snapshot, 'parentRunId', record.parentRunId);
  assignOptional(snapshot, 'nextDispatchAt', record.nextDispatchAt);
  assignOptional(snapshot, 'dispatchHoldReason', parseDispatchHoldReason(record.dispatchHoldReason));
  assignOptional(snapshot, 'pauseRequestedAt', record.pauseRequestedAt);
  assignOptional(snapshot, 'pausedAt', record.pausedAt);
  assignOptional(snapshot, 'resumedAt', record.resumedAt);
  assignOptional(snapshot, 'cancelReason', record.cancelReason);
  assignOptional(snapshot, 'cancelRequestedAt', record.cancelRequestedAt);
  assignOptional(snapshot, 'cancelledAt', record.cancelledAt);
  if (snapshot.detailsExpired) return snapshot;
  snapshot.input = JSON.parse(requiredField(record, 'input')) as Input;
  snapshot.boundary = nullableJsonField<Boundary>(record, 'boundary');
  snapshot.dispatchCursor = nullableJsonField<Cursor>(record, 'dispatchCursor');
  snapshot.checkpointCursor = nullableJsonField<Cursor>(record, 'checkpointCursor');
  return snapshot;
}

function toRunSummary(snapshot: RunSnapshot): RunSummary {
  const summary: RunSummary = {
    id: snapshot.id,
    definition: snapshot.definition,
    definitionVersion: snapshot.definitionVersion,
    recoveryDepth: snapshot.recoveryDepth,
    executionState: snapshot.executionState,
    completionState: snapshot.completionState,
    recordsSeen: snapshot.recordsSeen,
    recordsDispatched: snapshot.recordsDispatched,
    recordsSkipped: snapshot.recordsSkipped,
    recordsFailed: snapshot.recordsFailed,
    recordsUndispatched: snapshot.recordsUndispatched,
    boundaryTotalRecords: snapshot.boundaryTotalRecords,
    jobsCreated: snapshot.jobsCreated,
    jobsCompleted: snapshot.jobsCompleted,
    jobsFailed: snapshot.jobsFailed,
    jobsCancelled: snapshot.jobsCancelled,
    checkpointBatchIndex: snapshot.checkpointBatchIndex,
    createdAt: snapshot.createdAt,
    updatedAt: snapshot.updatedAt
  };
  assignOptional(summary, 'inputDigest', snapshot.inputDigest);
  assignOptional(summary, 'detailsExpired', snapshot.detailsExpired);
  assignOptional(summary, 'detailsExpiredAt', snapshot.detailsExpiredAt);
  assignOptional(summary, 'failureDetailsExpired', snapshot.failureDetailsExpired);
  assignOptional(summary, 'parentRunId', snapshot.parentRunId);
  return summary;
}

function parseExecutionState(value: string): RunExecutionState {
  if (executionStates.has(value as RunExecutionState)) return value as RunExecutionState;
  throw new QueuebitError({
    code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
    message: `Redis stored an unknown run execution state: ${value}.`
  });
}

function parseCompletionState(value: string): CompletionState {
  if (completionStates.has(value as CompletionState)) return value as CompletionState;
  throw new QueuebitError({
    code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
    message: `Redis stored an unknown completion state: ${value}.`
  });
}

function parseDispatchHoldReason(
  value: string | undefined
): RunSnapshot['dispatchHoldReason'] | undefined {
  if (value === undefined) return undefined;
  if (dispatchHoldReasons.has(value as NonNullable<RunSnapshot['dispatchHoldReason']>)) {
    return value as NonNullable<RunSnapshot['dispatchHoldReason']>;
  }
  throw new QueuebitError({
    code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
    message: `Redis stored an unknown dispatch hold reason: ${value}.`
  });
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

function numberField(record: Record<string, string>, field: string): number {
  return Number.parseInt(requiredField(record, field), 10);
}

function optionalNumberField(record: Record<string, string>, field: string, fallback: number): number {
  const value = record[field];
  return value === undefined ? fallback : Number.parseInt(value, 10);
}

function nullableNumberField(record: Record<string, string>, field: string): number | null {
  const value = requiredField(record, field);
  return value.length === 0 ? null : Number.parseInt(value, 10);
}

function nullableJsonField<T>(record: Record<string, string>, field: string): T | null {
  const value = requiredField(record, field);
  return value.length === 0 ? null : JSON.parse(value) as T;
}

function assertSegment(label: string, value: string): string {
  if (!segmentPattern.test(value)) {
    throw new QueuebitError({
      code: 'QB_RUN_INVALID',
      message: `Invalid ${label}: "${value}".`,
      details: { label, value }
    });
  }
  return value;
}

function assertBoundedString(label: string, value: string): string {
  if (typeof value !== 'string' || value.length === 0 || value.length > 512) {
    throw new QueuebitError({
      code: 'QB_RUN_INVALID',
      message: `${label} must be a non-empty string of at most 512 characters.`,
      details: { label }
    });
  }
  return value;
}

function normalizeListLimit(limit = 50): number {
  if (!Number.isInteger(limit) || limit < 1 || limit > 100) {
    throw new QueuebitError({
      code: 'QB_RUN_INVALID',
      message: 'runs.list limit must be an integer between 1 and 100.',
      details: { limit }
    });
  }
  return limit;
}

function normalizeCursor(cursor: string | undefined): string | undefined {
  if (cursor === undefined) return undefined;
  if (!/^[0-9]+$/.test(cursor)) {
    throw new QueuebitError({
      code: 'QB_RUN_INVALID',
      message: 'runs.list cursor must be an opaque numeric cursor.',
      details: { cursor }
    });
  }
  return cursor;
}

function parseZrangeWithScores(reply: unknown): Array<{ member: string; score: string }> {
  if (!Array.isArray(reply)) {
    throw new QueuebitError({
      code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
      message: 'Redis returned an invalid run index reply.',
      details: { reply }
    });
  }
  const pairs: Array<{ member: string; score: string }> = [];
  for (let index = 0; index < reply.length - 1; index += 2) {
    pairs.push({ member: String(reply[index]), score: String(reply[index + 1]) });
  }
  return pairs;
}

function parseBatchIndex(batchId: string): number {
  const marker = ':batch:';
  const markerIndex = batchId.lastIndexOf(marker);
  const parsed = markerIndex === -1
    ? Number.parseInt(batchId, 10)
    : Number.parseInt(batchId.slice(markerIndex + marker.length), 10);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new QueuebitError({
      code: 'QB_RUN_INVALID',
      message: 'Run batch index cursor is invalid.',
      details: { batchId }
    });
  }
  return parsed;
}

function parseOptionalJson(value: unknown): unknown {
  if (value === undefined) return undefined;
  try {
    return JSON.parse(String(value));
  } catch {
    return value;
  }
}

function assignOptional<T extends object, K extends keyof T>(target: T, key: K, value: T[K] | undefined) {
  if (value !== undefined) target[key] = value;
}
