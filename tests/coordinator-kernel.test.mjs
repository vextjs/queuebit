import assert from 'node:assert/strict';
import test from 'node:test';
import {
  QueuebitError,
  createQueuebitClient,
  createQueuebitCompletionsApi,
  createQueuebitCoordinator,
  createQueuebitRuntimeProcessor,
  createQueuebitKeyBuilder,
  createQueuebitJobsApi,
  createQueuebitRunsApi,
  defineQueuebitConfig,
  defineQueuebitRuntime
} from '../dist/index.js';

const fixedNow = new Date('2026-07-23T12:00:00.000Z');

class FakeRedisCoordinatorClient {
  commands = [];
  hashes = new Map();
  strings = new Map();
  zsets = new Map();
  lists = new Map();

  async sendCommand(command) {
    this.commands.push(command);
    const [name] = command;
    if (name === 'EVALSHA') return this.evalScript(command);
    if (name === 'HSET') return this.hset(command);
    if (name === 'HDEL') return this.hdel(command);
    if (name === 'HGETALL') return { ...(this.hashes.get(command[1]) ?? {}) };
    if (name === 'ZADD') return this.zadd(command[1], command[2], command[3]);
    if (name === 'ZREM') return this.zrem(command[1], command[2]);
    if (name === 'DEL') return this.del(command[1]);
    if (name === 'ZRANGEBYSCORE') return this.zrangeByScore(command);
    throw new Error(`Unexpected command ${name}`);
  }

  evalScript(command) {
    const keyCount = Number(command[2]);
    const keys = command.slice(3, 3 + keyCount);
    const args = command.slice(3 + keyCount);
    if (keyCount === 4 && keys[2].includes(':run-key:')) return this.evalRunsStart(keys, args);
    if (keyCount === 1 && keys[0].includes(':run:')) return this.evalCoordinatorClaim(keys, args);
    if (keyCount >= 14 && keys[1].endsWith(':batches') && keys[3].includes(':q:')) return this.evalDispatchBatch(keys, args);
    if (keyCount === 2 && keys[0].includes(':completion:') && args.length === 5) return this.evalClaimCompletion(keys, args);
    if (keyCount === 2 && keys[0].includes(':completion:') && args.length === 3) return this.evalRetryCompletion(keys, args);
    if (keyCount === 10 && keys[0].includes(':completion:')) return this.evalSettleCompletion(keys, args);
    if (keyCount >= 10 && keys[1].endsWith(':batches') && keys[2].includes(':batch:')) return this.evalSettleBatch(keys, args);
    throw new Error(`Unexpected script shape with ${keyCount} keys`);
  }

  evalRunsStart(keys, args) {
    const [countersKey, runsIndexKey, runIdentityKey, runHashKey] = keys;
    const envelope = JSON.parse(args[0]);
    const existingRaw = this.strings.get(runIdentityKey);
    if (existingRaw !== undefined) {
      const existing = JSON.parse(existingRaw);
      if (existing.inputDigest !== envelope.inputDigest) {
        return ['err', 'QB_RUN_DEDUPLICATION_CONFLICT', 'runs.start idempotencyKey conflicts with existing input.', '{}'];
      }
      return ['ok', existing.runId, '1'];
    }
    const counters = this.getHash(countersKey);
    const sequence = String(Number(counters.nextRunSequence ?? 0) + 1);
    counters.nextRunSequence = sequence;
    this.hashes.set(runHashKey, {
      id: envelope.runId,
      definition: envelope.definition,
      definitionVersion: String(envelope.definitionVersion),
      executionState: 'created',
      completionState: 'not_created',
      input: envelope.inputJson,
      inputDigest: envelope.inputDigest,
      idempotencyKey: envelope.idempotencyKey,
      recoveryDepth: String(envelope.recoveryDepth ?? 0),
      recordsSeen: '0',
      recordsDispatched: '0',
      recordsSkipped: '0',
      recordsFailed: '0',
      recordsUndispatched: '0',
      boundaryTotalRecords: '',
      jobsCreated: '0',
      jobsCompleted: '0',
      jobsFailed: '0',
      jobsCancelled: '0',
      boundary: '',
      dispatchCursor: '',
      checkpointCursor: '',
      checkpointBatchIndex: '0',
      sourceExhausted: '0',
      inFlightBatches: '0',
      sequence,
      createdAt: envelope.createdAt,
      updatedAt: envelope.createdAt
    });
    const created = this.hashes.get(runHashKey);
    if (envelope.parentRunId !== undefined && envelope.parentRunId !== '') {
      created.parentRunId = envelope.parentRunId;
    }
    if (envelope.recoveryParentRunId !== undefined && envelope.recoveryParentRunId !== '') {
      Object.assign(created, {
        recoveryParentRunId: envelope.recoveryParentRunId,
        recoveryFailureDigest: envelope.recoveryFailureDigest,
        recoveryFailureCount: String(envelope.recoveryFailureCount ?? 0),
        recoveryStage: envelope.recoveryStage ?? 'processor'
      });
    }
    this.strings.set(runIdentityKey, JSON.stringify({ runId: envelope.runId, inputDigest: envelope.inputDigest }));
    this.zadd(runsIndexKey, sequence, envelope.runId);
    return ['ok', envelope.runId, '0'];
  }

  evalCoordinatorClaim(keys, args) {
    const [runHashKey] = keys;
    const [coordinatorId, leaseDeadlineMs, leaseDeadlineAt, nowMsRaw, updatedAt] = args;
    const run = this.hashes.get(runHashKey);
    if (run === undefined) return ['err', 'QB_RUN_NOT_FOUND', 'Run does not exist.', '{}'];
    if (!['created', 'running', 'pausing', 'cancelling'].includes(run.executionState)) {
      return ['err', 'QB_RUN_STATE_CONFLICT', 'Only created, running, pausing, or cancelling runs can be advanced.', JSON.stringify({ state: run.executionState })];
    }
    const currentDeadline = Number(run.coordinatorLeaseDeadlineMs ?? 0);
    if (run.coordinatorId !== undefined && run.coordinatorId !== coordinatorId && currentDeadline > Number(nowMsRaw)) {
      return ['err', 'QB_RUN_STATE_CONFLICT', 'Another Coordinator owns this Run lease.', JSON.stringify({ owner: run.coordinatorId })];
    }
    const generation = String(Number(run.coordinatorGeneration ?? 0) + 1);
    Object.assign(run, {
      executionState: run.executionState === 'created' ? 'running' : run.executionState,
      coordinatorId,
      coordinatorGeneration: generation,
      coordinatorLeaseDeadlineMs: leaseDeadlineMs,
      coordinatorLeaseDeadlineAt: leaseDeadlineAt,
      updatedAt
    });
    return ['ok', generation];
  }

  evalDispatchBatch(keys, args) {
    const [
      runHashKey,
      runBatchesKey,
      batchHashKey,
      countersKey,
      waitingKey,
      dueKey,
      jobsIndexKey,
      waitingIndexKey,
      delayedIndexKey,
      completionCountersKey,
      completionsIndexKey,
      completionsDetailsKey,
      completionsDueKey,
      runCompletionKey,
      failuresKey,
      terminalRunsIndexKey
    ] = keys;
    const envelope = JSON.parse(args[0]);
    if (this.hashes.has(batchHashKey)) {
      return ['ok', envelope.batchId, this.hashes.get(batchHashKey).jobIds, '1'];
    }
    const run = this.hashes.get(runHashKey);
    if (run === undefined) return ['err', 'QB_RUN_NOT_FOUND', 'Run does not exist.', '{}'];
    if (run.executionState !== 'running') return ['err', 'QB_DISPATCH_STATE_CONFLICT', 'Run is not running.', '{}'];
    if (run.coordinatorId !== envelope.coordinatorId) return ['err', 'QB_DISPATCH_STATE_CONFLICT', 'Coordinator does not own this Run lease.', '{}'];
    if (run.coordinatorGeneration !== String(envelope.coordinatorGeneration)) return ['err', 'QB_DISPATCH_STATE_CONFLICT', 'Coordinator generation is stale.', '{}'];
    if ((run.dispatchCursor ?? '') !== envelope.expectedDispatchCursorJson) {
      return ['err', 'QB_DISPATCH_STATE_CONFLICT', 'Run dispatchCursor changed before Batch dispatch.', '{}'];
    }

    const counters = this.getHash(countersKey);
    const resultIds = [];
    let newBytes = 0;
    let waitingAdded = 0;
    let delayedAdded = 0;
    const jobKeyOffset = 16;
    const dedupeKeyOffset = 16 + envelope.entries.length;
    for (const [index, entry] of envelope.entries.entries()) {
      const dedupeKey = keys[dedupeKeyOffset + index];
      const existingRaw = this.strings.get(dedupeKey);
      if (existingRaw !== undefined) {
        resultIds[index] = JSON.parse(existingRaw).jobId;
        continue;
      }
      const sequence = String(Number(counters.nextSequence ?? 0) + 1);
      counters.nextSequence = sequence;
      const jobKey = keys[jobKeyOffset + index];
      this.hashes.set(jobKey, {
        id: entry.jobId,
        queue: entry.queue,
        name: entry.name,
        state: entry.state,
        attempt: '0',
        attempts: String(entry.attempts),
        createdAt: entry.createdAt,
        updatedAt: entry.updatedAt,
        data: entry.dataJson,
        dataDigest: entry.dataDigest,
        dataBytes: String(entry.dataBytes),
        options: entry.optionsJson,
        sequence,
        deduplicationKey: entry.deduplicationKey,
        idempotencyKey: entry.idempotencyKey,
        recordIdentity: entry.recordIdentity,
        runId: entry.runId,
        batchId: entry.batchId
      });
      this.zadd(jobsIndexKey, sequence, entry.jobId);
      if (entry.state === 'delayed') {
        this.zadd(dueKey, String(entry.delayUntilMs), jobKey);
        this.zadd(delayedIndexKey, sequence, entry.jobId);
        delayedAdded += 1;
      } else {
        this.rpush(waitingKey, jobKey);
        this.zadd(waitingIndexKey, sequence, entry.jobId);
        waitingAdded += 1;
      }
      this.strings.set(dedupeKey, JSON.stringify({ jobId: entry.jobId, dataDigest: entry.dataDigest }));
      resultIds[index] = entry.jobId;
      newBytes += Number(entry.dataBytes);
    }

    this.hashes.set(batchHashKey, {
      id: envelope.batchId,
      runId: envelope.runId,
      index: String(envelope.batchIndex),
      inputCursor: envelope.expectedDispatchCursorJson,
      nextCursor: envelope.nextCursorJson,
      executionState: resultIds.length > 0 ? 'running' : 'completed',
      completionState: 'not_required',
      recordsSeen: String(envelope.recordsSeen),
      recordsDispatched: String(envelope.recordsDispatched),
      recordsSkipped: String(envelope.recordsSkipped),
      recordsFailed: String(envelope.recordsFailed),
      recordsUndispatched: String(envelope.recordsUndispatched),
      jobsCreated: String(resultIds.length),
      jobsCompleted: '0',
      jobsFailed: '0',
      jobsCancelled: '0',
      sourceExhausted: envelope.sourceExhausted ? '1' : '0',
      jobIds: JSON.stringify(resultIds),
      createdAt: envelope.createdAt,
      updatedAt: envelope.updatedAt
    });
    this.zadd(runBatchesKey, String(envelope.batchIndex), envelope.batchId);
    for (const failure of envelope.mapperFailures ?? []) {
      const sequence = String(Number(run.nextFailureSequence ?? 0) + 1);
      run.nextFailureSequence = sequence;
      this.zadd(failuresKey, sequence, JSON.stringify({
        sequence,
        runId: envelope.runId,
        batchId: envelope.batchId,
        stage: 'mapper',
        recordIdentity: failure.recordIdentity,
        attempt: 0,
        error: JSON.parse(failure.errorJson),
        recoveryAvailable: true,
        payload: JSON.parse(failure.payloadJson)
      }));
    }
    if (run.boundary === '') run.boundary = envelope.boundaryJson;
    if (envelope.boundaryTotalRecords !== '') run.boundaryTotalRecords = String(envelope.boundaryTotalRecords);
    run.recordsSeen = String(Number(run.recordsSeen ?? 0) + envelope.recordsSeen);
    run.recordsDispatched = String(Number(run.recordsDispatched ?? 0) + envelope.recordsDispatched);
    run.recordsSkipped = String(Number(run.recordsSkipped ?? 0) + envelope.recordsSkipped);
    run.recordsFailed = String(Number(run.recordsFailed ?? 0) + envelope.recordsFailed);
    run.recordsUndispatched = String(Number(run.recordsUndispatched ?? 0) + envelope.recordsUndispatched);
    run.jobsCreated = String(Number(run.jobsCreated ?? 0) + resultIds.length);
    run.dispatchCursor = envelope.nextCursorJson;
    run.sourceExhausted = envelope.sourceExhausted ? '1' : '0';
    run.nextBatchIndex = String(envelope.batchIndex + 1);
    run.updatedAt = envelope.updatedAt;
    if (resultIds.length > 0) run.inFlightBatches = String(Number(run.inFlightBatches ?? 0) + 1);
    if (resultIds.length === 0) {
      this.advanceCheckpoint(run);
    }
    if (envelope.sourceExhausted && resultIds.length === 0 && this.canTerminalRun(run)) {
      const completionCounters = this.getHash(completionCountersKey);
      const completionSequence = String(Number(completionCounters.nextSequence ?? 0) + 1);
      completionCounters.nextSequence = completionSequence;
      envelope.runCompletion.summaryJson = JSON.stringify({
        recordsSeen: Number(run.recordsSeen ?? 0),
        recordsDispatched: Number(run.recordsDispatched ?? 0),
        recordsSkipped: Number(run.recordsSkipped ?? 0),
        recordsFailed: Number(run.recordsFailed ?? 0),
        recordsUndispatched: Number(run.recordsUndispatched ?? 0),
        boundaryTotalRecords: run.boundaryTotalRecords ?? '',
        jobsCreated: Number(run.jobsCreated ?? 0),
        jobsCompleted: Number(run.jobsCompleted ?? 0),
        jobsFailed: Number(run.jobsFailed ?? 0),
        jobsCancelled: Number(run.jobsCancelled ?? 0)
      });
      const completionState = envelope.runCompletion.handler === '' ? 'not_required' : 'pending';
      const runCompletion = {
        id: envelope.runCompletion.id,
        type: envelope.runCompletion.type,
        runId: envelope.runCompletion.runId,
        completionState,
        handler: envelope.runCompletion.handler,
        attempt: '0',
        attempts: String(envelope.runCompletion.attempts),
        deliveryGeneration: '0',
        summary: envelope.runCompletion.summaryJson,
        sequence: completionSequence,
        createdAt: envelope.runCompletion.createdAt,
        updatedAt: envelope.runCompletion.updatedAt
      };
      if (envelope.runCompletion.backoffJson !== '') runCompletion.backoff = envelope.runCompletion.backoffJson;
      if (completionState === 'pending') {
        runCompletion.nextDueAt = envelope.runCompletion.updatedAt;
        this.zadd(completionsDueKey, String(envelope.runCompletion.nowMs), envelope.runCompletion.id);
      }
      this.hashes.set(runCompletionKey, runCompletion);
      this.zadd(completionsIndexKey, completionSequence, envelope.runCompletion.id);
      this.zadd(completionsDetailsKey, completionSequence, envelope.runCompletion.id);
      run.executionState = Number(run.recordsFailed ?? 0) > 0 || Number(run.jobsFailed ?? 0) > 0 || Number(run.jobsCancelled ?? 0) > 0
        ? 'partial_failed'
        : 'completed';
      run.completionState = completionState;
      if (run.detailsExpired !== '1') this.zadd(terminalRunsIndexKey, run.sequence, run.id);
    }
    if (resultIds.length > 0) {
      counters.queuedJobs = String(Number(counters.queuedJobs ?? 0) + resultIds.length);
      counters.queuedBytes = String(Number(counters.queuedBytes ?? 0) + newBytes);
      counters.totalJobs = String(Number(counters.totalJobs ?? 0) + resultIds.length);
      if (waitingAdded > 0) counters.waitingJobs = String(Number(counters.waitingJobs ?? 0) + waitingAdded);
      if (delayedAdded > 0) counters.delayedJobs = String(Number(counters.delayedJobs ?? 0) + delayedAdded);
      if (envelope.highJobs !== '' && Number(counters.queuedJobs) >= Number(envelope.highJobs)) {
        counters.backpressureLatched = '1';
        counters.backpressureReason = 'jobs';
        counters.backpressureSince ??= envelope.updatedAt;
        counters.backpressureLastCheckedAt = envelope.updatedAt;
      } else if (envelope.highBytes !== '' && Number(counters.queuedBytes) >= Number(envelope.highBytes)) {
        counters.backpressureLatched = '1';
        counters.backpressureReason = 'bytes';
        counters.backpressureSince ??= envelope.updatedAt;
        counters.backpressureLastCheckedAt = envelope.updatedAt;
      }
    }
    delete run.dispatchHoldReason;
    if (envelope.nextDispatchAt !== '') run.nextDispatchAt = envelope.nextDispatchAt;
    else delete run.nextDispatchAt;
    return ['ok', envelope.batchId, JSON.stringify(resultIds), '0'];
  }

  evalSettleBatch(keys, args) {
    const [
      runHashKey,
      runBatchesKey,
      batchHashKey,
      completionCountersKey,
      completionsIndexKey,
      completionsDetailsKey,
      completionsDueKey,
      batchCompletionKey,
      runCompletionKey,
      failuresKey,
      terminalRunsIndexKey
    ] = keys;
    void runBatchesKey;
    const envelope = JSON.parse(args[0]);
    const run = this.hashes.get(runHashKey);
    const batch = this.hashes.get(batchHashKey);
    if (run === undefined) return ['err', 'QB_RUN_NOT_FOUND', 'Run does not exist.', '{}'];
    if (batch === undefined) return ['err', 'QB_RUN_NOT_FOUND', 'Batch does not exist.', '{}'];
    if (run.coordinatorId !== envelope.coordinatorId) return ['err', 'QB_DISPATCH_STATE_CONFLICT', 'Coordinator does not own this Run lease.', '{}'];
    if (run.coordinatorGeneration !== String(envelope.coordinatorGeneration)) return ['err', 'QB_DISPATCH_STATE_CONFLICT', 'Coordinator generation is stale.', '{}'];
    if (batch.executionState !== 'running') return ['ok', 'settled', batch.executionState, batch.nextCursor];

    let completed = 0;
    let failed = 0;
    let cancelled = 0;
    for (let index = 0; index < envelope.jobCount; index += 1) {
      const job = this.hashes.get(keys[11 + index]);
      if (job?.state === 'completed') completed += 1;
      else if (job?.state === 'failed') failed += 1;
      else if (job?.state === 'cancelled') cancelled += 1;
      else return ['ok', 'waiting', job?.state ?? 'missing', batch.nextCursor];
    }
    if (failed > 0) {
      for (let index = 0; index < envelope.jobCount; index += 1) {
        const job = this.hashes.get(keys[11 + index]);
        if (job?.state !== 'failed') continue;
        const sequence = String(Number(run.nextFailureSequence ?? 0) + 1);
        run.nextFailureSequence = sequence;
        this.zadd(failuresKey, sequence, JSON.stringify({
          sequence,
          runId: envelope.runId,
          batchId: envelope.batchId,
          jobId: job.id,
          stage: 'processor',
          recordIdentity: job.recordIdentity ?? job.deduplicationKey ?? job.id,
          attempt: Number(job.attempt ?? 0),
          error: JSON.parse(job.failedReason ?? '{"name":"Error","message":"Job failed without a serialized reason."}'),
          recoveryAvailable: true,
          payload: {
            name: job.name,
            data: JSON.parse(job.data),
            options: JSON.parse(job.options ?? '{}'),
            deduplicationKey: job.deduplicationKey,
            idempotencyKey: job.idempotencyKey
          }
        }));
      }
    }

    const createCompletion = (completionKey, event) => {
      if (this.hashes.has(completionKey)) return;
      const counters = this.getHash(completionCountersKey);
      const sequence = String(Number(counters.nextSequence ?? 0) + 1);
      counters.nextSequence = sequence;
      const state = event.handler === '' ? 'not_required' : 'pending';
      const completion = {
        id: event.id,
        type: event.type,
        runId: event.runId,
        completionState: state,
        handler: event.handler,
        attempt: '0',
        attempts: String(event.attempts),
        deliveryGeneration: '0',
        summary: event.summaryJson,
        sequence,
        createdAt: event.createdAt,
        updatedAt: event.updatedAt
      };
      if (event.batchId !== '') completion.batchId = event.batchId;
      if (event.backoffJson !== '') completion.backoff = event.backoffJson;
      if (state === 'pending') {
        completion.nextDueAt = event.updatedAt;
        this.zadd(completionsDueKey, String(event.nowMs), event.id);
      }
      this.hashes.set(completionKey, completion);
      this.zadd(completionsIndexKey, sequence, event.id);
      this.zadd(completionsDetailsKey, sequence, event.id);
    };

    const runSummaryJson = () => JSON.stringify({
      recordsSeen: Number(run.recordsSeen ?? 0),
      recordsDispatched: Number(run.recordsDispatched ?? 0),
      recordsSkipped: Number(run.recordsSkipped ?? 0),
      recordsFailed: Number(run.recordsFailed ?? 0),
      recordsUndispatched: Number(run.recordsUndispatched ?? 0),
      boundaryTotalRecords: run.boundaryTotalRecords ?? '',
      jobsCreated: Number(run.jobsCreated ?? 0),
      jobsCompleted: Number(run.jobsCompleted ?? 0),
      jobsFailed: Number(run.jobsFailed ?? 0),
      jobsCancelled: Number(run.jobsCancelled ?? 0)
    });

    const maybeCreateRunCompletion = () => {
      if (Number(run.inFlightBatches ?? 0) > 0) return;
      if (run.executionState === 'pausing') {
        run.executionState = 'paused';
        run.pausedAt = envelope.updatedAt;
        run.updatedAt = envelope.updatedAt;
        return;
      }
      if (run.executionState === 'cancelling') {
        run.executionState = 'cancelled';
        run.cancelledAt = envelope.updatedAt;
        if (run.detailsExpired !== '1') this.zadd(terminalRunsIndexKey, run.sequence, run.id);
        createCompletion(runCompletionKey, envelope.runCompletion);
        run.completionState = envelope.runCompletion.handler === '' ? 'not_required' : 'pending';
        run.updatedAt = envelope.updatedAt;
        return;
      }
      if (run.sourceExhausted !== '1') return;
      if (!this.canTerminalRun(run)) return;
      run.executionState = Number(run.recordsFailed ?? 0) > 0 || Number(run.jobsFailed ?? 0) > 0 || Number(run.jobsCancelled ?? 0) > 0
        ? 'partial_failed'
        : 'completed';
      if (run.detailsExpired !== '1') this.zadd(terminalRunsIndexKey, run.sequence, run.id);
      envelope.runCompletion.summaryJson = runSummaryJson();
      createCompletion(runCompletionKey, envelope.runCompletion);
      run.completionState = envelope.runCompletion.handler === '' ? 'not_required' : 'pending';
      run.updatedAt = envelope.updatedAt;
    };

    batch.executionState = failed > 0 || cancelled > 0 ? 'partial_failed' : 'completed';
    envelope.batchCompletion.summaryJson = JSON.stringify({
      recordsSeen: Number(batch.recordsSeen ?? 0),
      recordsDispatched: Number(batch.recordsDispatched ?? 0),
      recordsSkipped: Number(batch.recordsSkipped ?? 0),
      recordsFailed: Number(batch.recordsFailed ?? 0),
      recordsUndispatched: Number(batch.recordsUndispatched ?? 0),
      boundaryTotalRecords: run.boundaryTotalRecords ?? '',
      jobsCreated: Number(batch.jobsCreated ?? 0),
      jobsCompleted: completed,
      jobsFailed: failed,
      jobsCancelled: cancelled
    });
    createCompletion(batchCompletionKey, envelope.batchCompletion);
    batch.completionState = envelope.batchCompletion.handler === '' ? 'not_required' : 'pending';
    batch.completionEventId = envelope.batchCompletion.id;
    batch.jobsCompleted = String(completed);
    batch.jobsFailed = String(failed);
    batch.jobsCancelled = String(cancelled);
    batch.updatedAt = envelope.updatedAt;
    run.jobsCompleted = String(Number(run.jobsCompleted ?? 0) + completed);
    run.jobsFailed = String(Number(run.jobsFailed ?? 0) + failed);
    run.jobsCancelled = String(Number(run.jobsCancelled ?? 0) + cancelled);
    if (batch.completionState === 'not_required') {
      run.inFlightBatches = String(Number(run.inFlightBatches ?? 0) - 1);
      this.advanceCheckpoint(run);
      run.updatedAt = envelope.updatedAt;
      maybeCreateRunCompletion();
    } else {
      run.updatedAt = envelope.updatedAt;
    }
    return ['ok', 'settled', batch.executionState, batch.nextCursor];
  }

  evalClaimCompletion(keys, args) {
    const [completionKey, dueKey] = keys;
    const [coordinatorId, leaseDeadlineMs, leaseDeadlineAt, nowMsRaw, updatedAt] = args;
    const completion = this.hashes.get(completionKey);
    if (completion === undefined) return ['err', 'QB_COMPLETION_NOT_FOUND', 'Completion event does not exist.', '{}'];
    if (!['pending', 'retrying', 'delivering'].includes(completion.completionState)) return ['ok', ''];
    const currentDeadline = Number(completion.deliveryLeaseDeadlineMs ?? 0);
    if (completion.deliveryOwnerId !== undefined && completion.deliveryOwnerId !== coordinatorId && currentDeadline > Number(nowMsRaw)) {
      return ['ok', ''];
    }
    completion.deliveryGeneration = String(Number(completion.deliveryGeneration ?? 0) + 1);
    completion.attempt = String(Number(completion.attempt ?? 0) + 1);
    completion.completionState = 'delivering';
    completion.deliveryOwnerId = coordinatorId;
    completion.deliveryLeaseDeadlineMs = leaseDeadlineMs;
    completion.deliveryLeaseDeadlineAt = leaseDeadlineAt;
    completion.updatedAt = updatedAt;
    delete completion.nextDueAt;
    this.zadd(dueKey, leaseDeadlineMs, completion.id);
    return ['ok', completion.id, completion.deliveryGeneration, completion.attempt];
  }

  evalSettleCompletion(keys, args) {
    const [
      completionKey,
      parentKey,
      runHashKey,
      runBatchesKey,
      completionCountersKey,
      completionsIndexKey,
      completionsDetailsKey,
      completionsDueKey,
      runCompletionKey,
      terminalRunsIndexKey
    ] = keys;
    void runBatchesKey;
    const envelope = JSON.parse(args[0]);
    const completion = this.hashes.get(completionKey);
    const parent = this.hashes.get(parentKey);
    const run = this.hashes.get(runHashKey);
    if (completion === undefined) return ['err', 'QB_COMPLETION_NOT_FOUND', 'Completion event does not exist.', '{}'];
    if (parent === undefined) return ['err', 'QB_COMPLETION_STATE_CONFLICT', 'Completion parent does not exist.', '{}'];
    if (run === undefined) return ['err', 'QB_RUN_NOT_FOUND', 'Run does not exist.', '{}'];
    if (completion.deliveryOwnerId !== envelope.coordinatorId) {
      return ['err', 'QB_COMPLETION_STATE_CONFLICT', 'Coordinator does not own this completion lease.', '{}'];
    }
    if (completion.deliveryGeneration !== String(envelope.deliveryGeneration)) {
      return ['err', 'QB_COMPLETION_STATE_CONFLICT', 'Completion delivery generation is stale.', '{}'];
    }
    if (completion.completionState !== 'delivering') {
      return ['err', 'QB_COMPLETION_STATE_CONFLICT', 'Completion is not delivering.', '{}'];
    }

    const createRunCompletion = () => {
      if (envelope.runCompletion.id === '' || this.hashes.has(runCompletionKey)) return;
      const counters = this.getHash(completionCountersKey);
      const sequence = String(Number(counters.nextSequence ?? 0) + 1);
      counters.nextSequence = sequence;
      const state = envelope.runCompletion.handler === '' ? 'not_required' : 'pending';
      const event = {
        id: envelope.runCompletion.id,
        type: envelope.runCompletion.type,
        runId: envelope.runCompletion.runId,
        completionState: state,
        handler: envelope.runCompletion.handler,
        attempt: '0',
        attempts: String(envelope.runCompletion.attempts),
        deliveryGeneration: '0',
        summary: envelope.runCompletion.summaryJson,
        sequence,
        createdAt: envelope.updatedAt,
        updatedAt: envelope.updatedAt
      };
      if (envelope.runCompletion.backoffJson !== '') event.backoff = envelope.runCompletion.backoffJson;
      if (state === 'pending') {
        event.nextDueAt = envelope.updatedAt;
        this.zadd(completionsDueKey, String(envelope.nowMs), event.id);
      }
      this.hashes.set(runCompletionKey, event);
      this.zadd(completionsIndexKey, sequence, event.id);
      this.zadd(completionsDetailsKey, sequence, event.id);
      run.completionState = state;
      run.updatedAt = envelope.updatedAt;
    };
    const maybeAddTerminalRunDetailsIndex = () => {
      if (run.detailsExpired === '1') return;
      if (!['completed', 'partial_failed', 'failed', 'cancelled'].includes(run.executionState)) return;
      this.zadd(terminalRunsIndexKey, run.sequence, run.id);
    };

    if (envelope.result === 'delivered') {
      completion.completionState = 'delivered';
      completion.updatedAt = envelope.updatedAt;
      delete completion.deliveryOwnerId;
      delete completion.deliveryLeaseDeadlineMs;
      delete completion.deliveryLeaseDeadlineAt;
      delete completion.lastError;
      delete completion.nextDueAt;
      this.zrem(completionsDueKey, completion.id);
      parent.completionState = 'delivered';
      parent.updatedAt = envelope.updatedAt;
      maybeAddTerminalRunDetailsIndex();
      if (envelope.parentKind === 'batch') {
        run.inFlightBatches = String(Number(run.inFlightBatches ?? 0) - 1);
        this.advanceCheckpoint(run);
        run.updatedAt = envelope.updatedAt;
        if (run.executionState === 'pausing' && Number(run.inFlightBatches ?? 0) === 0) {
          run.executionState = 'paused';
          run.pausedAt = envelope.updatedAt;
        } else if (run.executionState === 'cancelling' && Number(run.inFlightBatches ?? 0) === 0) {
          run.executionState = 'cancelled';
          run.cancelledAt = envelope.updatedAt;
          maybeAddTerminalRunDetailsIndex();
          createRunCompletion();
        } else if (this.canTerminalRun(run)) {
          run.executionState = Number(run.recordsFailed ?? 0) > 0 || Number(run.jobsFailed ?? 0) > 0 || Number(run.jobsCancelled ?? 0) > 0
            ? 'partial_failed'
            : 'completed';
          maybeAddTerminalRunDetailsIndex();
          createRunCompletion();
        }
      }
      return ['ok', 'delivered'];
    }

    if (Number(completion.attempt ?? 0) < Number(completion.attempts ?? 1)) {
      completion.completionState = 'retrying';
      completion.lastError = envelope.errorJson;
      completion.nextDueAt = envelope.nextDueAt;
      completion.updatedAt = envelope.updatedAt;
      delete completion.deliveryOwnerId;
      delete completion.deliveryLeaseDeadlineMs;
      delete completion.deliveryLeaseDeadlineAt;
      this.zadd(completionsDueKey, String(envelope.nextDueMs), completion.id);
      parent.completionState = 'retrying';
      parent.updatedAt = envelope.updatedAt;
      return ['ok', 'retrying'];
    }

    completion.completionState = 'failed';
    completion.lastError = envelope.errorJson;
    completion.updatedAt = envelope.updatedAt;
    delete completion.deliveryOwnerId;
    delete completion.deliveryLeaseDeadlineMs;
    delete completion.deliveryLeaseDeadlineAt;
    delete completion.nextDueAt;
    this.zrem(completionsDueKey, completion.id);
    parent.completionState = 'failed';
    parent.updatedAt = envelope.updatedAt;
    return ['ok', 'failed'];
  }

  evalRetryCompletion(keys, args) {
    const [completionKey, dueKey] = keys;
    const [nowMs, nextDueAt, updatedAt] = args;
    const completion = this.hashes.get(completionKey);
    if (completion === undefined) return ['err', 'QB_COMPLETION_NOT_FOUND', 'Completion event does not exist.', '{}'];
    if (completion.completionState !== 'failed') {
      return ['err', 'QB_COMPLETION_STATE_CONFLICT', 'Only failed completion events can be retried.', JSON.stringify({ state: completion.completionState })];
    }
    completion.completionState = 'pending';
    completion.nextDueAt = nextDueAt;
    completion.updatedAt = updatedAt;
    delete completion.lastError;
    this.zadd(dueKey, nowMs, completion.id);
    return ['ok', completion.id];
  }

  batchKeyFor(runId, index) {
    return [...this.hashes.keys()].find(key => key.endsWith(`:run:${runId}:batch:${index}`));
  }

  batchReady(batch) {
    return ['completed', 'partial_failed', 'cancelled'].includes(batch?.executionState)
      && ['not_required', 'delivered'].includes(batch?.completionState ?? 'not_required');
  }

  advanceCheckpoint(run) {
    let index = Number(run.checkpointBatchIndex ?? 0);
    let cursor = run.checkpointCursor ?? '';
    let advanced = false;
    while (true) {
      const batchKey = this.batchKeyFor(run.id, index);
      if (batchKey === undefined) break;
      const batch = this.hashes.get(batchKey);
      if (!this.batchReady(batch)) break;
      cursor = batch.nextCursor ?? cursor;
      index += 1;
      advanced = true;
    }
    if (!advanced) return;
    run.checkpointCursor = cursor;
    run.checkpointBatchIndex = String(index);
  }

  canTerminalRun(run) {
    return Number(run.inFlightBatches ?? 0) === 0
      && run.sourceExhausted === '1'
      && Number(run.checkpointBatchIndex ?? 0) >= Number(run.nextBatchIndex ?? 0);
  }

  getHash(key) {
    const existing = this.hashes.get(key);
    if (existing) return existing;
    const created = {};
    this.hashes.set(key, created);
    return created;
  }

  hset(command) {
    const hash = this.getHash(command[1]);
    for (let index = 2; index < command.length; index += 2) {
      hash[command[index]] = String(command[index + 1]);
    }
    return 1;
  }

  hdel(command) {
    const hash = this.getHash(command[1]);
    let deleted = 0;
    for (const field of command.slice(2)) {
      if (hash[field] !== undefined) {
        delete hash[field];
        deleted += 1;
      }
    }
    return deleted;
  }

  del(key) {
    let deleted = 0;
    for (const store of [this.hashes, this.strings, this.zsets, this.lists]) {
      if (store.delete(key)) deleted += 1;
    }
    return deleted;
  }

  zadd(key, score, member) {
    const zset = this.zsets.get(key) ?? new Map();
    zset.set(member, Number(score));
    this.zsets.set(key, zset);
  }

  zrem(key, member) {
    const zset = this.zsets.get(key);
    if (zset === undefined) return;
    zset.delete(member);
  }

  rpush(key, member) {
    const list = this.lists.get(key) ?? [];
    list.push(member);
    this.lists.set(key, list);
  }

  zrangeByScore(command) {
    const [, key, min, max] = command;
    const limitIndex = command.indexOf('LIMIT');
    const limit = Number(command[limitIndex + 2]);
    const withScores = command.includes('WITHSCORES');
    const minExclusive = min.startsWith('(');
    const minRaw = minExclusive ? min.slice(1) : min;
    const minValue = minRaw === '-inf' ? Number.NEGATIVE_INFINITY : Number(minRaw);
    const maxValue = max === '+inf' ? Number.POSITIVE_INFINITY : Number(max);
    const zset = this.zsets.get(key) ?? new Map();
    const pairs = [...zset.entries()]
      .filter(([, score]) => (minExclusive ? score > minValue : score >= minValue) && score <= maxValue)
      .sort((left, right) => left[1] - right[1])
      .slice(0, limit);
    return withScores
      ? pairs.flatMap(([member, score]) => [member, String(score)])
      : pairs.map(([member]) => member);
  }
}

function createCoordinatorRuntime(overrides = {}) {
  const batchRunConfig = {
    queue: 'notification',
    source: 'paid-orders',
    mapper: 'receipt-job',
    pageSize: 2,
    inputSchema: {
      type: 'object',
      additionalProperties: false,
      required: ['tenantId'],
      properties: {
        tenantId: { type: 'string' }
      }
    }
  };
  Object.assign(batchRunConfig, overrides.batchRunConfig ?? {});
  if (overrides.completionConfig !== undefined) batchRunConfig.completion = overrides.completionConfig;
  const config = defineQueuebitConfig({
    namespace: 'test:coordinator',
    queues: {
      notification: overrides.queueConfig ?? {}
    },
    batchRuns: {
      'receipt-campaign': batchRunConfig
    }
  });
  const redis = new FakeRedisCoordinatorClient();
  let nextRun = 0;
  let nextJob = 0;
  const now = overrides.now ?? (() => fixedNow);
  const runs = createQueuebitRunsApi({
    config,
    redis,
    now,
    idGenerator: () => `run-${++nextRun}`
  });
  const jobs = createQueuebitJobsApi({
    config,
    redis,
    now,
    idGenerator: () => `direct-job-${++nextJob}`
  });
  const completions = createQueuebitCompletionsApi({ config, redis, now });
  const sourceRecords = overrides.sourceRecords ?? [
    { id: 1, orderId: 'ord-1' },
    { id: 2, orderId: 'ord-2' },
    { id: 3, orderId: 'ord-3' }
  ];
  const runtimeDefinition = {
    sources: {
      'paid-orders': {
        async freeze({ input }) {
          return { boundary: { tenantId: input.tenantId, upperId: 3 }, cursor: 0, totalRecords: 3 };
        },
        async load({ cursor, limit, signal }) {
          if (overrides.load) return overrides.load({ cursor, limit, signal });
          const page = sourceRecords.filter(record => record.id > cursor).slice(0, limit);
          return {
            records: page,
            nextCursor: page.at(-1)?.id ?? cursor,
            exhausted: page.length === 0 || page.at(-1)?.id === 3
          };
        }
      }
    },
    mappers: {
      'receipt-job': overrides.mapper ?? ((record) => ({
        name: 'send-receipt',
        identity: `order:${record.id}`,
        data: { orderId: record.orderId }
      }))
    }
  };
  if (overrides.completionHandlers !== undefined) {
    runtimeDefinition.completions = overrides.completionHandlers;
  }
  const runtime = defineQueuebitRuntime(runtimeDefinition);
  const createCoordinator = (coordinatorId, options = {}) => createQueuebitCoordinator({
    config,
    redis,
    runtime,
    coordinatorId,
    now,
    idGenerator: () => `job-${++nextJob}`,
    ...options
  });
  return { completions, config, createCoordinator, jobs, redis, runs, runtime };
}

async function waitFor(predicate, timeoutMs = 250) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() <= deadline) {
    if (await predicate()) return;
    await new Promise(resolve => setTimeout(resolve, 2));
  }
  assert.fail('Timed out waiting for condition');
}

class FakeObservabilityRecorder {
  events = [];

  incrementCounter(suffix, value = 1, labels = {}) {
    this.events.push({ type: 'counter', suffix, value, labels });
  }

  setGauge(suffix, value, labels = {}) {
    this.events.push({ type: 'gauge', suffix, value, labels });
  }

  observeDuration(suffix, durationMs, labels = {}) {
    this.events.push({ type: 'duration', suffix, value: durationMs, labels });
  }

  total(suffix, labels = {}) {
    return this.events
      .filter(event =>
        event.suffix === suffix
        && Object.entries(labels).every(([key, value]) => event.labels[key] === value)
      )
      .reduce((sum, event) => sum + event.value, 0);
  }
}

function markJobs(redis, jobIds, state) {
  for (const record of redis.hashes.values()) {
    if (jobIds.includes(record.id)) record.state = state;
  }
}

function findHashById(redis, id) {
  for (const record of redis.hashes.values()) {
    if (record.id === id) return record;
  }
  assert.fail(`Could not find Redis hash with id ${id}`);
}

test('coordinator dispatches one source page into Batch-owned waiting jobs', async () => {
  const { createCoordinator, jobs, runs } = createCoordinatorRuntime();
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:tenant-1'
  });
  const coordinator = createCoordinator('coord-a');

  const result = await coordinator.advanceRun(run.id);
  assert.equal(result.status, 'dispatched');
  assert.equal(result.batchId, 'run-1:batch:0');
  assert.equal(result.recordsSeen, 2);
  assert.equal(result.recordsDispatched, 2);
  assert.equal(result.jobsCreated, 2);
  assert.equal(result.dispatchCursor, 2);

  const updatedRun = await runs.get(run.id);
  assert.equal(updatedRun?.executionState, 'running');
  assert.deepEqual(updatedRun?.boundary, { tenantId: 'tenant-1', upperId: 3 });
  assert.equal(updatedRun?.dispatchCursor, 2);
  assert.equal(updatedRun?.checkpointCursor, null);
  assert.equal(updatedRun?.jobsCreated, 2);

  const waiting = await jobs.list({ queue: 'notification', state: 'waiting', limit: 10 });
  assert.deepEqual(waiting.items.map(job => job.id), ['job-1', 'job-2']);
  assert.deepEqual(waiting.items.map(job => job.runId), ['run-1', 'run-1']);
  assert.deepEqual(waiting.items.map(job => job.batchId), ['run-1:batch:0', 'run-1:batch:0']);
});

test('coordinator records advance and completion delivery metrics through recorder', async () => {
  const observability = new FakeObservabilityRecorder();
  const { createCoordinator, redis, runs } = createCoordinatorRuntime({
    completionConfig: { batch: { handler: 'batch-complete' } },
    completionHandlers: {
      'batch-complete': async () => ({ ok: true })
    }
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:tenant-1'
  });
  const coordinator = createCoordinator('coord-metrics', { observability });

  const result = await coordinator.advanceRun(run.id);
  assert.equal(result.status, 'dispatched');
  assert.equal(
    observability.total('coordinator_runs_advanced_total', {
      coordinatorId: 'coord-metrics',
      definition: 'receipt-campaign',
      status: 'dispatched'
    }),
    1
  );
  assert.equal(
    observability.total('coordinator_jobs_created_total', {
      coordinatorId: 'coord-metrics',
      definition: 'receipt-campaign',
      status: 'dispatched'
    }),
    2
  );

  markJobs(redis, ['job-1', 'job-2'], 'completed');
  const secondPage = await coordinator.advanceRun(run.id);
  assert.equal(secondPage.status, 'dispatched');
  assert.equal(observability.total('completion_events_claimed_total', { coordinatorId: 'coord-metrics' }), 1);
  assert.equal(observability.total('completion_events_delivered_total', { coordinatorId: 'coord-metrics' }), 1);
  assert.ok(observability.events.some(event => event.suffix === 'coordinator_advance_duration_ms'));
});

test('coordinator waits for sequential batch barrier before loading the next page', async () => {
  let loadCalls = 0;
  const { createCoordinator, runs } = createCoordinatorRuntime({
    load: ({ cursor, limit }) => {
      loadCalls += 1;
      const records = [
        { id: 1, orderId: 'ord-1' },
        { id: 2, orderId: 'ord-2' },
        { id: 3, orderId: 'ord-3' }
      ].filter(record => record.id > cursor).slice(0, limit);
      return {
        records,
        nextCursor: records.at(-1)?.id ?? cursor,
        exhausted: records.at(-1)?.id === 3
      };
    }
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:tenant-1'
  });
  const coordinator = createCoordinator('coord-a');

  await coordinator.advanceRun(run.id);
  const waiting = await coordinator.advanceRun(run.id);
  assert.equal(waiting.status, 'waiting_for_batch');
  assert.equal(loadCalls, 1);
});

test('coordinator honors paced interval and in-flight limits without loading source', async () => {
  let nowMs = fixedNow.getTime();
  let loadCalls = 0;
  const { createCoordinator, runs } = createCoordinatorRuntime({
    now: () => new Date(nowMs),
    batchRunConfig: {
      dispatch: { mode: 'paced', intervalMs: 5_000, maxInFlightBatches: 2 }
    },
    load: ({ cursor, limit }) => {
      loadCalls += 1;
      const records = [
        { id: 1, orderId: 'ord-1' },
        { id: 2, orderId: 'ord-2' },
        { id: 3, orderId: 'ord-3' }
      ].filter(record => record.id > cursor).slice(0, limit);
      return {
        records,
        nextCursor: records.at(-1)?.id ?? cursor,
        exhausted: records.length === 0 || records.at(-1)?.id === 3
      };
    }
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:paced'
  });
  const coordinator = createCoordinator('coord-paced');

  const first = await coordinator.advanceRun(run.id);
  assert.equal(first.status, 'dispatched');
  assert.equal(loadCalls, 1);

  const intervalHold = await coordinator.advanceRun(run.id);
  assert.equal(intervalHold.status, 'waiting_for_batch');
  assert.equal(loadCalls, 1);
  const heldByInterval = await runs.get(run.id);
  assert.equal(heldByInterval?.dispatchHoldReason, 'interval');
  assert.equal(heldByInterval?.nextDispatchAt, '2026-07-23T12:00:05.000Z');

  nowMs += 6_000;
  const second = await coordinator.advanceRun(run.id);
  assert.equal(second.status, 'dispatched');
  assert.equal(loadCalls, 2);

  nowMs += 6_000;
  const inFlightHold = await coordinator.advanceRun(run.id);
  assert.equal(inFlightHold.status, 'waiting_for_batch');
  assert.equal(loadCalls, 2);
  const heldByInFlight = await runs.get(run.id);
  assert.equal(heldByInFlight?.dispatchHoldReason, 'in_flight_limit');
  assert.equal(heldByInFlight?.nextDispatchAt, undefined);
});

test('coordinator holds on queue backpressure and resumes after low watermark clears', async () => {
  let loadCalls = 0;
  const { config, createCoordinator, redis, runs } = createCoordinatorRuntime({
    queueConfig: {
      backpressure: {
        highWatermarkJobs: 2,
        lowWatermarkJobs: 1
      }
    },
    load: ({ cursor, limit }) => {
      loadCalls += 1;
      const records = [{ id: 1, orderId: 'ord-1' }]
        .filter(record => record.id > cursor)
        .slice(0, limit);
      return {
        records,
        nextCursor: records.at(-1)?.id ?? cursor,
        exhausted: true
      };
    }
  });
  const keys = createQueuebitKeyBuilder(config);
  const counters = redis.getHash(keys.queueCounters('notification'));
  counters.queuedJobs = '2';
  counters.queuedBytes = '0';
  counters.backpressureLatched = '1';
  counters.backpressureReason = 'jobs';

  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:backpressure'
  });
  const coordinator = createCoordinator('coord-backpressure');

  const held = await coordinator.advanceRun(run.id);
  assert.equal(held.status, 'waiting_for_batch');
  assert.equal(loadCalls, 0);
  const heldRun = await runs.get(run.id);
  assert.equal(heldRun?.dispatchHoldReason, 'backpressure');

  counters.queuedJobs = '0';
  counters.queuedBytes = '0';
  const resumed = await coordinator.advanceRun(run.id);
  assert.equal(resumed.status, 'dispatched');
  assert.equal(loadCalls, 1);
  const resumedRun = await runs.get(run.id);
  assert.equal(resumedRun?.dispatchHoldReason, undefined);
});

test('coordinator keeps checkpoint behind out-of-order paced batches without completion handlers', async () => {
  const { createCoordinator, redis, runs } = createCoordinatorRuntime({
    batchRunConfig: {
      pageSize: 1,
      dispatch: { mode: 'paced', intervalMs: 0, maxInFlightBatches: 2 }
    },
    load: ({ cursor, limit }) => {
      const records = [
        { id: 1, orderId: 'ord-1' },
        { id: 2, orderId: 'ord-2' }
      ].filter(record => record.id > cursor).slice(0, limit);
      return {
        records,
        nextCursor: records.at(-1)?.id ?? cursor,
        exhausted: records.length === 0 || records.at(-1)?.id === 2
      };
    }
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:out-of-order'
  });
  const coordinator = createCoordinator('coord-paced');

  await coordinator.advanceRun(run.id);
  await coordinator.advanceRun(run.id);

  markJobs(redis, ['job-2'], 'completed');
  const held = await coordinator.advanceRun(run.id);
  assert.equal(held.status, 'waiting_for_batch');
  const blockedRun = await runs.get(run.id);
  assert.equal(blockedRun?.checkpointCursor, null);
  assert.equal(blockedRun?.checkpointBatchIndex, 0);
  assert.equal(blockedRun?.inFlightBatches, 1);
  assert.equal(blockedRun?.executionState, 'running');

  markJobs(redis, ['job-1'], 'completed');
  const finished = await coordinator.advanceRun(run.id);
  assert.equal(finished.status, 'source_exhausted');
  const finalRun = await runs.get(run.id);
  assert.equal(finalRun?.executionState, 'completed');
  assert.equal(finalRun?.checkpointCursor, 2);
  assert.equal(finalRun?.checkpointBatchIndex, 2);
});

test('coordinator waits for earlier batch completion delivery before advancing paced checkpoint', async () => {
  const delivered = [];
  let firstBatchFailures = 0;
  const { completions, createCoordinator, redis, runs } = createCoordinatorRuntime({
    batchRunConfig: {
      pageSize: 1,
      dispatch: { mode: 'paced', intervalMs: 0, maxInFlightBatches: 2 }
    },
    completionConfig: {
      batch: { handler: 'after-batch', attempts: 1 }
    },
    completionHandlers: {
      'after-batch': event => {
        if (event.batchId === 'run-1:batch:0' && firstBatchFailures === 0) {
          firstBatchFailures += 1;
          throw new Error('first batch callback is down');
        }
        delivered.push(event.batchId);
      }
    },
    load: ({ cursor, limit }) => {
      const records = [
        { id: 1, orderId: 'ord-1' },
        { id: 2, orderId: 'ord-2' }
      ].filter(record => record.id > cursor).slice(0, limit);
      return {
        records,
        nextCursor: records.at(-1)?.id ?? cursor,
        exhausted: records.length === 0 || records.at(-1)?.id === 2
      };
    }
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:out-of-order-completion'
  });
  const coordinator = createCoordinator('coord-paced');

  await coordinator.advanceRun(run.id);
  await coordinator.advanceRun(run.id);
  markJobs(redis, ['job-1', 'job-2'], 'completed');
  await coordinator.advanceRun(run.id);

  assert.deepEqual(delivered, ['run-1:batch:1']);
  assert.equal((await completions.get('run-1:batch:0:settled'))?.completionState, 'failed');
  const blockedRun = await runs.get(run.id);
  assert.equal(blockedRun?.checkpointCursor, null);
  assert.equal(blockedRun?.checkpointBatchIndex, 0);
  assert.equal(blockedRun?.inFlightBatches, 1);

  await completions.retry('run-1:batch:0:settled');
  const delivery = await coordinator.deliverDueCompletions();
  assert.equal(delivery.delivered, 1);

  const finalRun = await runs.get(run.id);
  assert.equal(finalRun?.executionState, 'completed');
  assert.equal(finalRun?.checkpointCursor, 2);
  assert.equal(finalRun?.checkpointBatchIndex, 2);
  assert.deepEqual(delivered, ['run-1:batch:1', 'run-1:batch:0']);
});

test('coordinator does not terminal exhausted empty page before earlier in-flight batch closes', async () => {
  const { createCoordinator, redis, runs } = createCoordinatorRuntime({
    batchRunConfig: {
      pageSize: 1,
      dispatch: { mode: 'paced', intervalMs: 0, maxInFlightBatches: 2 }
    },
    load: ({ cursor }) => {
      if (cursor === 0) {
        return { records: [{ id: 1, orderId: 'ord-1' }], nextCursor: 1, exhausted: false };
      }
      return { records: [], nextCursor: cursor, exhausted: true };
    }
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:empty-tail'
  });
  const coordinator = createCoordinator('coord-paced');

  await coordinator.advanceRun(run.id);
  const emptyTail = await coordinator.advanceRun(run.id);
  assert.equal(emptyTail.status, 'source_exhausted');
  const notTerminal = await runs.get(run.id);
  assert.equal(notTerminal?.executionState, 'running');
  assert.equal(notTerminal?.sourceExhausted, true);
  assert.equal(notTerminal?.checkpointCursor, null);
  assert.equal(notTerminal?.checkpointBatchIndex, 0);
  assert.equal(notTerminal?.inFlightBatches, 1);

  markJobs(redis, ['job-1'], 'completed');
  await coordinator.advanceRun(run.id);
  const finalRun = await runs.get(run.id);
  assert.equal(finalRun?.executionState, 'completed');
  assert.equal(finalRun?.checkpointCursor, 1);
  assert.equal(finalRun?.checkpointBatchIndex, 2);
});

test('one-to-many mapper capacity uses actual created job count', async () => {
  const { config, createCoordinator, redis, runs } = createCoordinatorRuntime({
    queueConfig: {
      backpressure: { highWatermarkJobs: 10, lowWatermarkJobs: 2 }
    },
    batchRunConfig: {
      pageSize: 1
    },
    load: ({ cursor }) => {
      if (cursor === 0) {
        return { records: [{ id: 1, orderId: 'ord-1' }], nextCursor: 1, exhausted: true };
      }
      return { records: [], nextCursor: cursor, exhausted: true };
    },
    mapper: record => [
      {
        name: 'send-receipt',
        identity: `order:${record.id}:receipt`,
        data: { orderId: record.orderId, channel: 'email' }
      },
      {
        name: 'send-receipt',
        identity: `order:${record.id}:sms`,
        data: { orderId: record.orderId, channel: 'sms' }
      }
    ]
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:one-to-many'
  });

  const dispatched = await createCoordinator('coord-a').advanceRun(run.id);
  assert.equal(dispatched.recordsSeen, 1);
  assert.equal(dispatched.recordsDispatched, 1);
  assert.equal(dispatched.jobsCreated, 2);

  const client = await createQueuebitClient({
    config,
    redis,
    preflight: false,
    now: () => fixedNow
  });
  const capacity = await client.capacity.snapshot();
  assert.equal(capacity.queues[0].counters.queuedJobs, 2);
  assert.equal(capacity.queues[0].watermarks.highWatermarkJobs, 10);
  assert.equal(capacity.queues[0].utilization.jobs, 0.2);
  await client.close();
});

test('coordinator settles completed batches, advances checkpoint, and dispatches until run completion', async () => {
  const { createCoordinator, jobs, redis, runs } = createCoordinatorRuntime();
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:tenant-1'
  });
  const coordinator = createCoordinator('coord-a');

  await coordinator.advanceRun(run.id);
  markJobs(redis, ['job-1', 'job-2'], 'completed');
  const secondPage = await coordinator.advanceRun(run.id);
  assert.equal(secondPage.status, 'dispatched');
  assert.equal(secondPage.batchId, 'run-1:batch:1');
  assert.equal(secondPage.dispatchCursor, 3);

  const afterSecondDispatch = await runs.get(run.id);
  assert.equal(afterSecondDispatch?.checkpointCursor, 2);
  assert.equal(afterSecondDispatch?.dispatchCursor, 3);
  assert.equal(afterSecondDispatch?.sourceExhausted, true);
  assert.equal(afterSecondDispatch?.jobsCreated, 3);

  const waiting = await jobs.list({ queue: 'notification', state: 'waiting', limit: 10 });
  assert.deepEqual(waiting.items.map(job => job.id), ['job-1', 'job-2', 'job-3']);

  markJobs(redis, ['job-3'], 'completed');
  const finished = await coordinator.advanceRun(run.id);
  assert.equal(finished.status, 'source_exhausted');

  const finalRun = await runs.get(run.id);
  assert.equal(finalRun?.executionState, 'completed');
  assert.equal(finalRun?.completionState, 'not_required');
  assert.equal(finalRun?.checkpointCursor, 3);
  assert.equal(finalRun?.jobsCompleted, 3);
});

test('coordinator delivers batch completion handlers before advancing the sequential checkpoint', async () => {
  const delivered = [];
  const { completions, createCoordinator, redis, runs } = createCoordinatorRuntime({
    completionConfig: {
      batch: { handler: 'after-batch' }
    },
    completionHandlers: {
      'after-batch': event => delivered.push(event)
    }
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:tenant-1'
  });
  const coordinator = createCoordinator('coord-a');

  await coordinator.advanceRun(run.id);
  markJobs(redis, ['job-1', 'job-2'], 'completed');
  const secondPage = await coordinator.advanceRun(run.id);

  assert.equal(secondPage.status, 'dispatched');
  assert.equal(secondPage.batchId, 'run-1:batch:1');
  assert.deepEqual(delivered.map(event => event.id), ['run-1:batch:0:settled']);
  assert.equal(delivered[0].type, 'batch.settled');
  assert.equal(delivered[0].attempt, 1);

  const event = await completions.get('run-1:batch:0:settled');
  assert.equal(event?.completionState, 'delivered');
  assert.equal(event?.attempt, 1);
  assert.equal(event?.attempts, 3);
  assert.equal((await runs.get(run.id))?.checkpointCursor, 2);
});

test('coordinator delivers run completion handlers when the exhausted run reaches terminal state', async () => {
  const delivered = [];
  const { completions, createCoordinator, redis, runs } = createCoordinatorRuntime({
    completionConfig: {
      run: { handler: 'after-run' }
    },
    completionHandlers: {
      'after-run': event => delivered.push(event)
    }
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:tenant-1'
  });
  const coordinator = createCoordinator('coord-a');

  await coordinator.advanceRun(run.id);
  markJobs(redis, ['job-1', 'job-2'], 'completed');
  await coordinator.advanceRun(run.id);
  markJobs(redis, ['job-3'], 'completed');
  const finished = await coordinator.advanceRun(run.id);

  assert.equal(finished.status, 'source_exhausted');
  assert.deepEqual(delivered.map(event => event.id), ['run-1:settled']);
  assert.equal(delivered[0].type, 'run.settled');
  assert.equal(delivered[0].summary.jobsCompleted, 3);

  const runEvent = await completions.get('run-1:settled');
  assert.equal(runEvent?.completionState, 'delivered');
  assert.equal((await runs.get(run.id))?.completionState, 'delivered');
});

test('coordinator persists and delivers run completion when the source is exhausted with no jobs', async () => {
  const delivered = [];
  const { completions, createCoordinator, runs } = createCoordinatorRuntime({
    load: ({ cursor }) => ({
      records: [],
      nextCursor: cursor,
      exhausted: true
    }),
    completionConfig: {
      run: { handler: 'after-run' }
    },
    completionHandlers: {
      'after-run': event => delivered.push(event)
    }
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:tenant-1'
  });
  const coordinator = createCoordinator('coord-a');

  const finished = await coordinator.advanceRun(run.id);
  assert.equal(finished.status, 'source_exhausted');
  assert.equal(finished.jobsCreated, 0);

  const finalRun = await runs.get(run.id);
  assert.equal(finalRun?.executionState, 'completed');
  assert.equal(finalRun?.completionState, 'delivered');
  assert.deepEqual(delivered.map(event => event.id), ['run-1:settled']);

  const event = await completions.get('run-1:settled');
  assert.equal(event?.completionState, 'delivered');
  assert.equal(event?.summary.jobsCreated, 0);
});

test('coordinator records failed completion handlers and completions.retry reopens delivery', async () => {
  let calls = 0;
  const { completions, createCoordinator, redis, runs } = createCoordinatorRuntime({
    completionConfig: {
      batch: { handler: 'after-batch', attempts: 1 }
    },
    completionHandlers: {
      'after-batch': () => {
        calls += 1;
        if (calls === 1) throw new Error('webhook is down');
      }
    }
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:tenant-1'
  });
  const coordinator = createCoordinator('coord-a');

  await coordinator.advanceRun(run.id);
  markJobs(redis, ['job-1', 'job-2'], 'completed');
  const blocked = await coordinator.advanceRun(run.id);
  assert.equal(blocked.status, 'waiting_for_batch');

  const failed = await completions.get('run-1:batch:0:settled');
  assert.equal(failed?.completionState, 'failed');
  assert.equal(failed?.lastError?.message, 'webhook is down');
  assert.equal((await runs.get(run.id))?.checkpointCursor, null);

  const reopened = await completions.retry('run-1:batch:0:settled');
  assert.equal(reopened.completionState, 'pending');
  const delivery = await coordinator.deliverDueCompletions();
  assert.equal(delivery.delivered, 1);

  const secondPage = await coordinator.advanceRun(run.id);
  assert.equal(secondPage.status, 'dispatched');
  assert.equal((await completions.get('run-1:batch:0:settled'))?.completionState, 'delivered');
  assert.equal((await runs.get(run.id))?.checkpointCursor, 2);
});

test('coordinator recovers stale delivering completion events with generation fencing', async () => {
  let nowMs = fixedNow.getTime();
  let calls = 0;
  let releaseFirst;
  let firstStarted;
  const firstStartedPromise = new Promise(resolve => {
    firstStarted = resolve;
  });
  const firstDeliveryHangs = new Promise(resolve => {
    releaseFirst = resolve;
  });
  const { completions, createCoordinator, redis, runs } = createCoordinatorRuntime({
    now: () => new Date(nowMs),
    completionConfig: {
      batch: { handler: 'after-batch' }
    },
    completionHandlers: {
      'after-batch': async () => {
        calls += 1;
        if (calls === 1) {
          firstStarted();
          await firstDeliveryHangs;
        }
      }
    }
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:tenant-1'
  });
  const first = createCoordinator('coord-a', { leaseMs: 10_000 });
  const second = createCoordinator('coord-b', { leaseMs: 10_000 });

  await first.advanceRun(run.id);
  markJobs(redis, ['job-1', 'job-2'], 'completed');
  const firstAdvance = first.advanceRun(run.id);
  await firstStartedPromise;

  const delivering = await completions.get('run-1:batch:0:settled');
  assert.equal(delivering?.completionState, 'delivering');
  assert.equal(delivering?.deliveryGeneration, 1);

  nowMs += 11_000;
  const recovered = await second.deliverDueCompletions();
  assert.equal(recovered.delivered, 1);
  assert.equal(calls, 2);

  releaseFirst();
  await assert.rejects(
    firstAdvance,
    error => error instanceof QueuebitError && error.code === 'QB_COMPLETION_STATE_CONFLICT'
  );

  const delivered = await completions.get('run-1:batch:0:settled');
  assert.equal(delivered?.completionState, 'delivered');
  assert.equal(delivered?.deliveryGeneration, 2);
  assert.equal((await runs.get(run.id))?.checkpointCursor, 2);
});

test('coordinator does not load source for paused runs', async () => {
  let loadCalls = 0;
  const { createCoordinator, redis, runs } = createCoordinatorRuntime({
    load: ({ cursor, limit }) => {
      loadCalls += 1;
      return {
        records: [{ id: cursor + 1, orderId: `ord-${cursor + 1}` }].slice(0, limit),
        nextCursor: cursor + 1,
        exhausted: false
      };
    }
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:paused'
  });
  findHashById(redis, run.id).executionState = 'paused';

  const result = await createCoordinator('coord-a').advanceRun(run.id);
  assert.equal(result.status, 'paused');
  assert.equal(loadCalls, 0);
});

test('coordinator converges pausing and cancelling runs without dispatching new batches', async () => {
  let loadCalls = 0;
  const { createCoordinator, redis, runs } = createCoordinatorRuntime({
    load: ({ cursor, limit }) => {
      loadCalls += 1;
      const records = [
        { id: 1, orderId: 'ord-1' },
        { id: 2, orderId: 'ord-2' }
      ].filter(record => record.id > cursor).slice(0, limit);
      return { records, nextCursor: 2, exhausted: false };
    }
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:pausing'
  });
  const coordinator = createCoordinator('coord-a');

  await coordinator.advanceRun(run.id);
  assert.equal(loadCalls, 1);
  markJobs(redis, ['job-1', 'job-2'], 'completed');
  findHashById(redis, run.id).executionState = 'pausing';

  const paused = await coordinator.advanceRun(run.id);
  assert.equal(paused.status, 'paused');
  assert.equal(loadCalls, 1);
  assert.equal((await runs.get(run.id))?.executionState, 'paused');

  const cancelledRun = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:cancelling'
  });
  await coordinator.advanceRun(cancelledRun.id);
  markJobs(redis, ['job-3', 'job-4'], 'cancelled');
  const cancellingRecord = findHashById(redis, cancelledRun.id);
  cancellingRecord.executionState = 'cancelling';
  cancellingRecord.cancelReason = 'stop batch';
  cancellingRecord.cancelRequestedAt = fixedNow.toISOString();

  const cancelling = await coordinator.advanceRun(cancelledRun.id);
  assert.equal(cancelling.status, 'source_exhausted');
  const finalRun = await runs.get(cancelledRun.id);
  assert.equal(finalRun?.executionState, 'cancelled');
  assert.equal(finalRun?.cancelledAt, fixedNow.toISOString());
  assert.equal(loadCalls, 2);
});

test('runs.retryFailed creates a recovery run that replays processor failure envelopes', async () => {
  let loadCalls = 0;
  const { createCoordinator, jobs, redis, runs } = createCoordinatorRuntime({
    load: ({ cursor, limit }) => {
      loadCalls += 1;
      const records = [
        { id: 1, orderId: 'ord-1' },
        { id: 2, orderId: 'ord-2' },
        { id: 3, orderId: 'ord-3' }
      ].filter(record => record.id > cursor).slice(0, limit);
      return {
        records,
        nextCursor: records.at(-1)?.id ?? cursor,
        exhausted: records.length === 0 || records.at(-1)?.id === 3
      };
    }
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:failure-replay'
  });
  const coordinator = createCoordinator('coord-a');

  await coordinator.advanceRun(run.id);
  const failedJob = findHashById(redis, 'job-1');
  failedJob.state = 'failed';
  failedJob.attempt = '1';
  failedJob.failedReason = JSON.stringify({ name: 'Error', message: 'provider failed' });
  markJobs(redis, ['job-2'], 'completed');
  await coordinator.advanceRun(run.id);
  markJobs(redis, ['job-3'], 'completed');
  await coordinator.advanceRun(run.id);

  const parent = await runs.get(run.id);
  assert.equal(parent?.executionState, 'partial_failed');
  const failures = await runs.listFailures(run.id, { includePayload: true });
  assert.equal(failures.items.length, 1);
  assert.equal(failures.items[0].recordIdentity, 'order:1:0');
  assert.equal(failures.items[0].payload.name, 'send-receipt');

  const recovery = await runs.retryFailed(run.id, {
    idempotencyKey: `recovery:${run.id}:1`
  });
  assert.equal(recovery.parentRunId, run.id);
  assert.equal(recovery.recoveryDepth, 1);

  const beforeRecoveryLoadCalls = loadCalls;
  const recoveryDispatch = await coordinator.advanceRun(recovery.id);
  assert.equal(recoveryDispatch.status, 'dispatched');
  assert.equal(recoveryDispatch.sourceExhausted, true);
  assert.equal(loadCalls, beforeRecoveryLoadCalls);

  const recoveryJob = await jobs.get('job-4');
  assert.equal(recoveryJob?.runId, recovery.id);
  assert.equal(recoveryJob?.batchId, `${recovery.id}:batch:0`);
  assert.deepEqual(recoveryJob?.data, { orderId: 'ord-1' });
  assert.equal(recoveryJob?.idempotencyKey, 'run:run-1:batch:run-1:batch:0:job:order:1:0');
  assert.match(recoveryJob?.deduplicationKey ?? '', /^run:run-2:recovery:1$/);
});

test('coordinator records mapper failure envelopes without aborting the source page', async () => {
  const { createCoordinator, jobs, runs } = createCoordinatorRuntime({
    mapper: (record) => {
      if (record.id === 1) throw new Error('mapper cannot read order payload');
      return {
        name: 'send-receipt',
        identity: `order:${record.id}`,
        data: { orderId: record.orderId }
      };
    }
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:mapper-continuation'
  });

  const result = await createCoordinator('coord-a').advanceRun(run.id);
  assert.equal(result.status, 'dispatched');
  assert.equal(result.recordsSeen, 2);
  assert.equal(result.recordsDispatched, 1);
  assert.equal(result.recordsFailed, 1);
  assert.equal(result.jobsCreated, 1);

  const waiting = await jobs.list({ queue: 'notification', state: 'waiting', limit: 10 });
  assert.deepEqual(waiting.items.map(job => job.id), ['job-1']);
  assert.deepEqual((await jobs.get('job-1'))?.data, { orderId: 'ord-2' });

  const parent = await runs.get(run.id);
  assert.equal(parent?.recordsFailed, 1);
  assert.equal(parent?.recordsDispatched, 1);
  assert.equal(parent?.jobsCreated, 1);

  const hiddenPayload = await runs.listFailures(run.id, { stage: 'mapper' });
  assert.equal(hiddenPayload.items.length, 1);
  assert.match(hiddenPayload.items[0].recordIdentity, /^record:0:/);
  assert.equal(hiddenPayload.items[0].error.message, 'mapper cannot read order payload');
  assert.equal(hiddenPayload.items[0].payload, undefined);

  const withPayload = await runs.listFailures(run.id, { stage: 'mapper', includePayload: true });
  assert.deepEqual(withPayload.items[0].payload.record, { id: 1, orderId: 'ord-1' });
  assert.deepEqual(withPayload.items[0].payload.input, { tenantId: 'tenant-1' });
  assert.deepEqual(withPayload.items[0].payload.boundary, { tenantId: 'tenant-1', upperId: 3 });
  assert.equal(withPayload.items[0].payload.cursor, 0);
  assert.equal(withPayload.items[0].payload.recordIndex, 0);
});

test('runs.retryFailed replays mapper failure envelopes without loading the source again', async () => {
  let loadCalls = 0;
  const mapperContexts = [];
  const { createCoordinator, jobs, redis, runs } = createCoordinatorRuntime({
    load: ({ cursor, limit }) => {
      loadCalls += 1;
      const records = [
        { id: 1, orderId: 'ord-1' },
        { id: 2, orderId: 'ord-2' },
        { id: 3, orderId: 'ord-3' }
      ].filter(record => record.id > cursor).slice(0, limit);
      return {
        records,
        nextCursor: records.at(-1)?.id ?? cursor,
        exhausted: records.length === 0 || records.at(-1)?.id === 3
      };
    },
    mapper: (record, context) => {
      mapperContexts.push({ record, context });
      if (context.runId === 'run-1' && record.id === 1) {
        throw new Error('mapper cannot read order payload');
      }
      return {
        name: 'send-receipt',
        identity: `order:${record.id}`,
        data: { orderId: record.orderId }
      };
    }
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:mapper-recovery'
  });
  const coordinator = createCoordinator('coord-a');

  await coordinator.advanceRun(run.id);
  markJobs(redis, ['job-1'], 'completed');
  await coordinator.advanceRun(run.id);
  markJobs(redis, ['job-2'], 'completed');
  await coordinator.advanceRun(run.id);

  const parent = await runs.get(run.id);
  assert.equal(parent?.executionState, 'partial_failed');
  assert.equal((await runs.listFailures(run.id, { stage: 'mapper' })).items.length, 1);

  const recovery = await runs.retryFailed(run.id, {
    idempotencyKey: `recovery:${run.id}:mapper`
  });
  const beforeRecoveryLoadCalls = loadCalls;
  const recoveryDispatch = await coordinator.advanceRun(recovery.id);
  assert.equal(recoveryDispatch.status, 'dispatched');
  assert.equal(recoveryDispatch.recordsSeen, 1);
  assert.equal(recoveryDispatch.recordsDispatched, 1);
  assert.equal(recoveryDispatch.sourceExhausted, true);
  assert.equal(loadCalls, beforeRecoveryLoadCalls);

  const recoveryMapperCall = mapperContexts.find(entry => entry.context.runId === recovery.id);
  assert.deepEqual(recoveryMapperCall.record, { id: 1, orderId: 'ord-1' });
  assert.deepEqual(recoveryMapperCall.context.input, { tenantId: 'tenant-1' });
  assert.deepEqual(recoveryMapperCall.context.boundary, { tenantId: 'tenant-1', upperId: 3 });
  assert.equal(recoveryMapperCall.context.cursor, 0);
  assert.equal(recoveryMapperCall.context.recordIndex, 0);

  const recoveryJob = await jobs.get('job-3');
  assert.equal(recoveryJob?.runId, recovery.id);
  assert.deepEqual(recoveryJob?.data, { orderId: 'ord-1' });
  assert.match(recoveryJob?.deduplicationKey ?? '', /^run:run-2:recovery:1:0$/);
});

test('coordinator writes a new mapper failure envelope when recovery mapping fails again', async () => {
  const { createCoordinator, redis, runs } = createCoordinatorRuntime({
    sourceRecords: [
      { id: 1, orderId: 'ord-1' },
      { id: 2, orderId: 'ord-2' }
    ],
    mapper: (record) => {
      if (record.id === 1) throw new Error('mapper still cannot read order payload');
      return {
        name: 'send-receipt',
        identity: `order:${record.id}`,
        data: { orderId: record.orderId }
      };
    }
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:mapper-recovery-fails'
  });
  const coordinator = createCoordinator('coord-a');

  await coordinator.advanceRun(run.id);
  markJobs(redis, ['job-1'], 'completed');
  await coordinator.advanceRun(run.id);

  const recovery = await runs.retryFailed(run.id, {
    idempotencyKey: `recovery:${run.id}:mapper-fails`
  });
  const recoveryResult = await coordinator.advanceRun(recovery.id);
  assert.equal(recoveryResult.status, 'source_exhausted');
  assert.equal(recoveryResult.recordsFailed, 1);

  const recoveryRun = await runs.get(recovery.id);
  assert.equal(recoveryRun?.executionState, 'partial_failed');
  const recoveryFailures = await runs.listFailures(recovery.id, {
    stage: 'mapper',
    includePayload: true
  });
  assert.equal(recoveryFailures.items.length, 1);
  assert.equal(recoveryFailures.items[0].error.message, 'mapper still cannot read order payload');
  assert.deepEqual(recoveryFailures.items[0].payload.record, { id: 1, orderId: 'ord-1' });
});

test('coordinator writes processor failure envelopes only after the whole batch is terminal', async () => {
  const { createCoordinator, redis, runs } = createCoordinatorRuntime();
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:failure-envelope-once'
  });
  const coordinator = createCoordinator('coord-a');

  await coordinator.advanceRun(run.id);
  const failedJob = findHashById(redis, 'job-1');
  failedJob.state = 'failed';
  failedJob.attempt = '1';
  failedJob.failedReason = JSON.stringify({ name: 'Error', message: 'provider failed' });

  const waiting = await coordinator.advanceRun(run.id);
  assert.equal(waiting.status, 'waiting_for_batch');
  assert.equal((await runs.listFailures(run.id)).items.length, 0);

  markJobs(redis, ['job-2'], 'completed');
  await coordinator.advanceRun(run.id);
  await coordinator.advanceRun(run.id);
  assert.equal((await runs.listFailures(run.id)).items.length, 1);
});

test('coordinator rejects source pages that do not advance cursor', async () => {
  const { createCoordinator, runs } = createCoordinatorRuntime({
    load: ({ cursor }) => ({
      records: [{ id: 1, orderId: 'ord-1' }],
      nextCursor: cursor,
      exhausted: false
    })
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:tenant-1'
  });
  const coordinator = createCoordinator('coord-a');

  await assert.rejects(
    () => coordinator.advanceRun(run.id),
    (error) => error instanceof QueuebitError && error.code === 'QB_SOURCE_CURSOR_NOT_ADVANCED'
  );
});

test('coordinator keeps a single active owner for one run lease', async () => {
  let releaseLoad;
  const loadStarted = new Promise(resolve => {
    releaseLoad = () => resolve({
      records: [],
      nextCursor: 0,
      exhausted: true
    });
  });
  const { createCoordinator, redis, runs } = createCoordinatorRuntime({
    load: () => loadStarted
  });
  const run = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:tenant-1'
  });
  const first = createCoordinator('coord-a', { leaseMs: 10_000 });
  const second = createCoordinator('coord-b', { leaseMs: 10_000 });

  const firstAdvance = first.advanceRun(run.id);
  await waitFor(() => [...redis.hashes.values()].some(hash => hash.coordinatorId === 'coord-a'));

  await assert.rejects(
    () => second.advanceRun(run.id),
    (error) => error instanceof QueuebitError && error.code === 'QB_RUN_STATE_CONFLICT'
  );

  releaseLoad();
  const result = await firstAdvance;
  assert.equal(result.status, 'source_exhausted');
});

test('coordinator disposes linked abort listeners while preserving source cancellation', async () => {
  const signal = createTrackedAbortSignal();
  const { createCoordinator, runs } = createCoordinatorRuntime();
  const coordinator = createCoordinator('coord-abort-cleanup');

  for (let index = 0; index < 3; index += 1) {
    const run = await runs.start('receipt-campaign', {
      input: { tenantId: `tenant-${index}` },
      idempotencyKey: `campaign:abort-cleanup:${index}`
    });
    await coordinator.advanceRun(run.id, { signal });
    assert.equal(signal.listenerCount, 0);
  }

  const cancellation = new Error('caller cancelled source loading');
  const cancellableSignal = createTrackedAbortSignal();
  let loadStarted = false;
  const cancellable = createCoordinatorRuntime({
    load: ({ signal: sourceSignal }) => new Promise((_resolve, reject) => {
      loadStarted = true;
      sourceSignal.addEventListener('abort', () => reject(cancellation), { once: true });
    })
  });
  const run = await cancellable.runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-cancel' },
    idempotencyKey: 'campaign:abort-cleanup:cancel'
  });
  const advance = cancellable.createCoordinator('coord-abort-cancel').advanceRun(run.id, {
    signal: cancellableSignal
  });
  await waitFor(() => loadStarted && cancellableSignal.listenerCount === 1);
  cancellableSignal.abort(cancellation);

  await assert.rejects(advance, error => error === cancellation);
  assert.equal(cancellableSignal.listenerCount, 0);
});

test('runtime processor dispatcher is available to application code and rejects unknown jobs', async () => {
  const runtime = defineQueuebitRuntime({
    sources: {},
    mappers: {},
    processors: {
      'send-receipt': async job => ({ delivered: job.data.orderId })
    }
  });
  const processor = createQueuebitRuntimeProcessor(runtime);
  const context = {
    queue: 'notification',
    workerId: 'worker-code-host',
    jobId: 'job-1',
    attempt: 1,
    signal: new AbortController().signal
  };

  assert.deepEqual(
    await processor({ id: 'job-1', queue: 'notification', name: 'send-receipt', data: { orderId: 'ord-1' } }, context),
    { delivered: 'ord-1' }
  );
  await assert.rejects(
    () => processor({ id: 'job-2', queue: 'notification', name: 'unknown', data: {} }, context),
    error => error instanceof QueuebitError && error.code === 'QB_CONFIG_HANDLER_NOT_REGISTERED'
  );
});

test('CoordinatorRunner lets application code advance runs and honor remote drain requests', async () => {
  const { config, redis, runtime } = createCoordinatorRuntime();
  const client = await createQueuebitClient({ config, redis, preflight: false });
  const runner = client.createCoordinatorRunner(runtime, {
    coordinatorId: 'code-host',
    pollIntervalMs: 2,
    heartbeatIntervalMs: 2,
    heartbeatTtlMs: 20
  });
  try {
    const run = await client.runs.start('receipt-campaign', {
      input: { tenantId: 'tenant-code-host' },
      idempotencyKey: 'campaign:code-host'
    });
    runner.start();

    await waitFor(async () => (await client.roles.get({
      role: 'coordinator',
      domain: config.scheduler.domain,
      identity: 'code-host'
    })) !== null);
    await waitFor(async () => (await client.runs.get(run.id))?.executionState === 'running');
    assert.equal(runner.status().status, 'running');

    await client.roles.requestDrain({
      role: 'coordinator',
      domain: config.scheduler.domain,
      identity: 'code-host',
      reason: 'deploy'
    });
    await waitFor(() => runner.status().status === 'stopped', 500);
    assert.equal(await client.roles.get({
      role: 'coordinator',
      domain: config.scheduler.domain,
      identity: 'code-host'
    }), null);
  } finally {
    await client.close({ timeoutMs: 100 });
  }
});

test('CoordinatorRunner reports advance failures instead of swallowing them', async () => {
  const failures = [];
  const { config, redis, runtime } = createCoordinatorRuntime({
    load: async () => {
      throw new Error('source temporarily unavailable');
    }
  });
  const client = await createQueuebitClient({ config, redis, preflight: false });
  const runner = client.createCoordinatorRunner(runtime, {
    coordinatorId: 'error-host',
    pollIntervalMs: 2,
    onError: event => failures.push(event)
  });
  try {
    await client.runs.start('receipt-campaign', {
      input: { tenantId: 'tenant-error-host' },
      idempotencyKey: 'campaign:error-host'
    });
    runner.start();

    await waitFor(() => failures.some(event => event.operation === 'advance'));
    const failure = runner.status().lastError;
    assert.equal(failure?.operation, 'advance');
    assert.match(failure?.error.message ?? '', /source temporarily unavailable/);
  } finally {
    await client.close({ timeoutMs: 100 });
  }
});

test('CoordinatorRunner preserves the primary failure when its error observer throws', async () => {
  const { config, redis, runtime } = createCoordinatorRuntime({
    load: async () => {
      throw new Error('source temporarily unavailable');
    }
  });
  const client = await createQueuebitClient({ config, redis, preflight: false });
  const runner = client.createCoordinatorRunner(runtime, {
    coordinatorId: 'error-observer-host',
    pollIntervalMs: 2,
    onError: () => {
      throw new Error('application logger unavailable');
    }
  });
  try {
    await client.runs.start('receipt-campaign', {
      input: { tenantId: 'tenant-error-observer-host' },
      idempotencyKey: 'campaign:error-observer-host'
    });
    runner.start();

    await waitFor(() => runner.status().lastError?.operation === 'advance');
    const failure = runner.status().lastError;
    assert.equal(failure?.operation, 'advance');
    assert.match(failure?.error.message ?? '', /source temporarily unavailable/);
  } finally {
    await client.close({ timeoutMs: 100 });
  }
});

test('CoordinatorRunner keeps draining state after a timeout and can finish later', async () => {
  let releaseLoad;
  const loadBlocked = new Promise(resolve => {
    releaseLoad = resolve;
  });
  const { config, redis, runtime } = createCoordinatorRuntime({
    load: async ({ cursor }) => {
      await loadBlocked;
      return { records: [], nextCursor: cursor, exhausted: true };
    }
  });
  const client = await createQueuebitClient({ config, redis, preflight: false });
  const runner = client.createCoordinatorRunner(runtime, {
    coordinatorId: 'drain-host',
    pollIntervalMs: 2
  });
  try {
    await client.runs.start('receipt-campaign', {
      input: { tenantId: 'tenant-drain-host' },
      idempotencyKey: 'campaign:drain-host'
    });
    runner.start();
    await waitFor(() => runner.status().activeRuns === 1);

    await assert.rejects(
      () => runner.drain({ timeoutMs: 1 }),
      error => error instanceof QueuebitError && error.code === 'QB_COORDINATOR_DRAIN_TIMEOUT'
    );
    assert.equal(runner.status().status, 'draining');

    releaseLoad();
    await waitFor(() => runner.status().activeRuns === 0);
    await runner.stop({ timeoutMs: 100 });
    assert.equal(runner.status().status, 'stopped');
  } finally {
    releaseLoad?.();
    await client.close({ timeoutMs: 100 }).catch(() => undefined);
  }
});

function createTrackedAbortSignal() {
  const listeners = new Set();
  return {
    aborted: false,
    reason: undefined,
    get listenerCount() {
      return listeners.size;
    },
    addEventListener(type, listener) {
      if (type === 'abort') listeners.add(listener);
    },
    removeEventListener(type, listener) {
      if (type === 'abort') listeners.delete(listener);
    },
    abort(reason) {
      if (this.aborted) return;
      this.aborted = true;
      this.reason = reason;
      for (const listener of [...listeners]) listener();
    }
  };
}
