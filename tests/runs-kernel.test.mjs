import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import {
  QueuebitError,
  createQueuebitClient,
  createQueuebitKeyBuilder,
  createQueuebitRunsApi,
  defineQueuebitConfig
} from '../dist/index.js';

const fixedNow = new Date('2026-07-23T11:00:00.000Z');

class FakeRedisRunsClient {
  commands = [];
  hashes = new Map();
  strings = new Map();
  zsets = new Map();
  lists = new Map();

  async sendCommand(command) {
    this.commands.push(command);
    const [name] = command;
    if (name === 'EVALSHA') return this.evalScript(command);
    if (name === 'HGETALL') return { ...(this.hashes.get(command[1]) ?? {}) };
    if (name === 'ZCARD') return this.zsets.get(command[1])?.size ?? 0;
    if (name === 'ZRANGEBYSCORE') return this.zrangeByScore(command);
    throw new Error(`Unexpected command ${name}`);
  }

  evalScript(command) {
    const keyCount = Number(command[2]);
    const keys = command.slice(3, 3 + keyCount);
    const args = command.slice(3 + keyCount);
    if (keyCount === 4 && keys[2].includes(':run-key:')) return this.evalRunsStart(keys, args);
    if (keyCount === 2 && keys[0].includes(':completion:') && args.length === 3) {
      return this.evalCompletionsRetry(keys, args);
    }
    if (keyCount === 4 && keys[0].includes(':completion:') && keys[2].includes(':completions:due')) {
      return this.evalRetentionPurgeCompletionEvent(keys, args);
    }
    if (keyCount === 3 && keys[0].includes(':run:') && keys[1].includes(':failures')) {
      return this.evalRetentionPurgeTerminalRun(keys, args);
    }
    if (keyCount >= 12 && keys[0].includes(':run:')) return this.evalRunsCancel(keys, args);
    if (keyCount === 1 && keys[0].includes(':run:')) {
      const run = this.hashes.get(keys[0]);
      if (run?.executionState === 'paused' || run?.executionState === 'pausing' || run?.executionState === 'blocked') {
        return this.evalRunsResume(keys, args);
      }
      return this.evalRunsPause(keys, args);
    }
    throw new Error(`Unexpected script shape with ${keyCount} keys`);
  }

  evalRetentionPurgeTerminalRun(keys, args) {
    const [runKey, failuresKey, terminalRunsIndexKey] = keys;
    const [runId, observedAt] = args;
    const run = this.hashes.get(runKey);
    if (run === undefined) {
      this.zrem(terminalRunsIndexKey, runId);
      return ['skip', 'snapshot_missing'];
    }
    if (run.detailsExpired === '1') {
      this.zrem(terminalRunsIndexKey, runId);
      return ['skip', 'details_expired'];
    }
    if (!['completed', 'partial_failed', 'failed', 'cancelled'].includes(run.executionState)) {
      this.zrem(terminalRunsIndexKey, runId);
      return ['skip', 'state_protected', run.executionState ?? ''];
    }
    if (!['not_required', 'delivered'].includes(run.completionState)) {
      return ['skip', 'completion_protected', run.completionState ?? ''];
    }
    if (run.inputDigest === undefined) return ['skip', 'snapshot_missing'];
    run.detailsExpired = '1';
    run.detailsExpiredAt = observedAt;
    run.failureDetailsExpired = '1';
    delete run.input;
    delete run.boundary;
    delete run.dispatchCursor;
    delete run.checkpointCursor;
    delete run.nextDispatchAt;
    delete run.dispatchHoldReason;
    this.zsets.delete(failuresKey);
    this.zrem(terminalRunsIndexKey, runId);
    return ['tombstoned', runId];
  }

  evalRetentionPurgeCompletionEvent(keys, args) {
    const [completionKey, runKey, completionsDueKey, completionsDetailsKey] = keys;
    const [eventId, observedAt] = args;
    const completion = this.hashes.get(completionKey);
    if (completion === undefined) {
      this.zrem(completionsDetailsKey, eventId);
      return ['skip', 'snapshot_missing'];
    }
    if (completion.detailsExpired === '1') {
      this.zrem(completionsDetailsKey, eventId);
      return ['skip', 'details_expired'];
    }
    if (!['delivered', 'not_required'].includes(completion.completionState)) {
      return ['skip', 'completion_protected', completion.completionState ?? ''];
    }
    const run = this.hashes.get(runKey);
    if (run === undefined) return ['skip', 'snapshot_missing'];
    if (!['completed', 'partial_failed', 'failed', 'cancelled'].includes(run.executionState)) {
      return ['skip', 'state_protected', run.executionState ?? ''];
    }
    if (!['not_required', 'delivered'].includes(run.completionState)) {
      return ['skip', 'completion_protected', run.completionState ?? ''];
    }
    if (completion.summary === undefined) return ['skip', 'snapshot_missing'];
    completion.summaryDigest ??= createHash('sha1').update(completion.summary).digest('hex');
    completion.detailsExpired = '1';
    completion.detailsExpiredAt = observedAt;
    delete completion.summary;
    delete completion.backoff;
    delete completion.lastError;
    delete completion.nextDueAt;
    delete completion.deliveryOwnerId;
    delete completion.deliveryLeaseDeadlineMs;
    delete completion.deliveryLeaseDeadlineAt;
    this.zrem(completionsDueKey, eventId);
    this.zrem(completionsDetailsKey, eventId);
    return ['tombstoned', eventId];
  }

  evalCompletionsRetry(keys, args) {
    const [completionKey, dueKey] = keys;
    const [nowMs, nextDueAt, updatedAt] = args;
    const completion = this.hashes.get(completionKey);
    if (completion === undefined) return ['err', 'QB_COMPLETION_NOT_FOUND', 'Completion event does not exist.', '{}'];
    if (completion.detailsExpired === '1') {
      return ['err', 'QB_COMPLETION_STATE_CONFLICT', 'Completion event details have expired.', JSON.stringify({ detailsExpired: true })];
    }
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

  evalRunsStart(keys, args) {
    const [countersKey, runsIndexKey, runIdentityKey, runHashKey] = keys;
    const envelope = JSON.parse(args[0]);
    const existingRaw = this.strings.get(runIdentityKey);
    if (existingRaw !== undefined) {
      const existing = JSON.parse(existingRaw);
      if (existing.inputDigest !== envelope.inputDigest) {
        return [
          'err',
          'QB_RUN_DEDUPLICATION_CONFLICT',
          'runs.start idempotencyKey conflicts with existing input.',
          JSON.stringify({ idempotencyKey: envelope.idempotencyKey })
        ];
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
    this.strings.set(runIdentityKey, JSON.stringify({
      runId: envelope.runId,
      inputDigest: envelope.inputDigest
    }));
    this.zadd(runsIndexKey, sequence, envelope.runId);
    return ['ok', envelope.runId, '0'];
  }

  evalRunsPause(keys, args) {
    const [runHashKey] = keys;
    const [updatedAt] = args;
    const run = this.hashes.get(runHashKey);
    if (run === undefined) return ['err', 'QB_RUN_NOT_FOUND', 'Run does not exist.', '{}'];
    if (run.executionState === 'paused' || run.executionState === 'pausing') {
      return ['ok', run.id, run.executionState];
    }
    if (['completed', 'partial_failed', 'failed', 'cancelled', 'cancelling'].includes(run.executionState)) {
      return ['err', 'QB_RUN_STATE_CONFLICT', 'Only non-terminal, non-cancelling runs can be paused.', JSON.stringify({ state: run.executionState })];
    }
    const nextState = Number(run.inFlightBatches ?? 0) > 0 ? 'pausing' : 'paused';
    run.executionState = nextState;
    run.pauseRequestedAt = updatedAt;
    run.updatedAt = updatedAt;
    if (nextState === 'paused') run.pausedAt = updatedAt;
    return ['ok', run.id, nextState];
  }

  evalRunsResume(keys, args) {
    const [runHashKey] = keys;
    const [updatedAt] = args;
    const run = this.hashes.get(runHashKey);
    if (run === undefined) return ['err', 'QB_RUN_NOT_FOUND', 'Run does not exist.', '{}'];
    if (run.executionState === 'running' || run.executionState === 'created') return ['ok', run.id, run.executionState];
    if (!['paused', 'pausing', 'blocked'].includes(run.executionState)) {
      return ['err', 'QB_RUN_STATE_CONFLICT', 'Only paused, pausing, or blocked runs can be resumed.', JSON.stringify({ state: run.executionState })];
    }
    run.executionState = 'running';
    run.resumedAt = updatedAt;
    run.updatedAt = updatedAt;
    return ['ok', run.id, 'running'];
  }

  evalRunsCancel(keys, args) {
    const [
      runHashKey,
      completionCountersKey,
      completionsIndexKey,
      completionsDetailsKey,
      completionsDueKey,
      runCompletionKey,
      terminalRunsIndexKey,
      waitingKey,
      dueKey,
      waitingIndexKey,
      delayedIndexKey,
      retryingIndexKey,
      cancelledIndexKey,
      countersKey,
      ...jobKeys
    ] = keys;
    const envelope = JSON.parse(args[0]);
    const run = this.hashes.get(runHashKey);
    if (run === undefined) return ['err', 'QB_RUN_NOT_FOUND', 'Run does not exist.', '{}'];
    if (run.executionState === 'cancelled') {
      if (run.detailsExpired !== '1') this.zadd(terminalRunsIndexKey, run.sequence, run.id);
      return ['ok', run.id, 'cancelled', '0'];
    }
    if (['completed', 'partial_failed', 'failed'].includes(run.executionState)) {
      return ['err', 'QB_RUN_STATE_CONFLICT', 'Terminal runs cannot be cancelled.', JSON.stringify({ state: run.executionState })];
    }
    if (run.cancelReason === undefined) {
      run.cancelReason = envelope.reason;
      run.cancelRequestedAt = envelope.updatedAt;
    }
    const counters = this.getHash(countersKey);
    let cancelledJobs = 0;
    for (const jobKey of jobKeys.slice(0, envelope.jobCount)) {
      const job = this.hashes.get(jobKey);
      if (job === undefined || job.runId !== envelope.runId) continue;
      if (!['waiting', 'delayed', 'retrying'].includes(job.state)) continue;
      if (job.state === 'waiting') {
        this.lrem(waitingKey, jobKey);
        this.zrem(waitingIndexKey, job.id);
        counters.waitingJobs = String(Number(counters.waitingJobs ?? 0) - 1);
      } else if (job.state === 'delayed') {
        this.zrem(dueKey, jobKey);
        this.zrem(delayedIndexKey, job.id);
        counters.delayedJobs = String(Number(counters.delayedJobs ?? 0) - 1);
      } else {
        this.zrem(dueKey, jobKey);
        this.zrem(retryingIndexKey, job.id);
        counters.retryingJobs = String(Number(counters.retryingJobs ?? 0) - 1);
      }
      job.state = 'cancelled';
      job.updatedAt = envelope.updatedAt;
      this.zadd(cancelledIndexKey, job.sequence, job.id);
      counters.cancelledJobs = String(Number(counters.cancelledJobs ?? 0) + 1);
      counters.queuedJobs = String(Number(counters.queuedJobs ?? 0) - 1);
      counters.queuedBytes = String(Number(counters.queuedBytes ?? 0) - Number(job.dataBytes ?? 0));
      cancelledJobs += 1;
    }
    const nextState = Number(run.inFlightBatches ?? 0) > 0 ? 'cancelling' : 'cancelled';
    run.executionState = nextState;
    run.updatedAt = envelope.updatedAt;
    if (nextState === 'cancelled') {
      run.cancelledAt = envelope.updatedAt;
      if (run.detailsExpired !== '1') this.zadd(terminalRunsIndexKey, run.sequence, run.id);
      const completionCounters = this.getHash(completionCountersKey);
      const sequence = String(Number(completionCounters.nextSequence ?? 0) + 1);
      completionCounters.nextSequence = sequence;
      const completionState = envelope.runCompletion.handler === '' ? 'not_required' : 'pending';
      this.hashes.set(runCompletionKey, {
        id: envelope.runCompletion.id,
        type: envelope.runCompletion.type,
        runId: envelope.runCompletion.runId,
        completionState,
        handler: envelope.runCompletion.handler,
        attempt: '0',
        attempts: String(envelope.runCompletion.attempts),
        deliveryGeneration: '0',
        summary: JSON.stringify({
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
        }),
        sequence,
        createdAt: envelope.updatedAt,
        updatedAt: envelope.updatedAt
      });
      this.zadd(completionsIndexKey, sequence, envelope.runCompletion.id);
      this.zadd(completionsDetailsKey, sequence, envelope.runCompletion.id);
      if (completionState === 'pending') this.zadd(completionsDueKey, String(envelope.nowMs), envelope.runCompletion.id);
      run.completionState = completionState;
    }
    return ['ok', run.id, nextState, String(cancelledJobs)];
  }

  getHash(key) {
    const existing = this.hashes.get(key);
    if (existing) return existing;
    const created = {};
    this.hashes.set(key, created);
    return created;
  }

  zadd(key, score, member) {
    const zset = this.zsets.get(key) ?? new Map();
    zset.set(member, Number(score));
    this.zsets.set(key, zset);
  }

  zrem(key, member) {
    this.zsets.get(key)?.delete(member);
  }

  rpush(key, member) {
    const list = this.lists.get(key) ?? [];
    list.push(member);
    this.lists.set(key, list);
  }

  lrem(key, member) {
    this.lists.set(key, (this.lists.get(key) ?? []).filter(item => item !== member));
  }

  zrangeByScore(command) {
    const [, key, min, , , , , countRaw] = command;
    const limit = Number(countRaw);
    const minValue = min === '-inf' ? Number.NEGATIVE_INFINITY : Number(min.slice(1));
    const zset = this.zsets.get(key) ?? new Map();
    return [...zset.entries()]
      .filter(([, score]) => score > minValue)
      .sort((left, right) => left[1] - right[1])
      .slice(0, limit)
      .flatMap(([member, score]) => [member, String(score)]);
  }
}

function createRunsRuntime(options = {}) {
  const config = defineQueuebitConfig({
    namespace: 'test:runs',
    queues: {
      notification: {}
    },
    ...(options.retention === undefined ? {} : { retention: options.retention }),
    batchRuns: {
      'receipt-campaign': {
        queue: 'notification',
        source: 'paid-orders',
        mapper: 'receipt-job',
        inputSchema: {
          type: 'object',
          additionalProperties: true,
          properties: {
            tenantId: { type: 'string' },
            paidBefore: { type: 'string' },
            page: { type: 'integer' }
          }
        }
      }
    }
  });
  const redis = new FakeRedisRunsClient();
  let nextRun = 0;
  const runs = createQueuebitRunsApi({
    config,
    redis,
    now: () => fixedNow,
    idGenerator: () => `run-${++nextRun}`
  });
  return { config, redis, runs };
}

test('runs.start creates durable run identity and deduplicates identical input', async () => {
  const { runs } = createRunsRuntime();
  const first = await runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1', paidBefore: '2026-07-15T00:00:00.000Z' },
    idempotencyKey: 'campaign:tenant-1'
  });
  assert.equal(first.id, 'run-1');
  assert.equal(first.deduplicated, false);
  assert.equal(first.executionState, 'created');
  assert.equal(first.completionState, 'not_created');
  assert.equal(first.sourceExhausted, false);

  const second = await runs.start('receipt-campaign', {
    input: { paidBefore: '2026-07-15T00:00:00.000Z', tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:tenant-1'
  });
  assert.equal(second.id, first.id);
  assert.equal(second.deduplicated, true);

  await assert.rejects(
    () => runs.start('receipt-campaign', {
      input: { tenantId: 'tenant-2' },
      idempotencyKey: 'campaign:tenant-1'
    }),
    (error) => error instanceof QueuebitError && error.code === 'QB_RUN_DEDUPLICATION_CONFLICT'
  );
});

test('runs.start rejects unknown definitions and invalid inputSchema payloads', async () => {
  const { runs } = createRunsRuntime();
  await assert.rejects(
    () => runs.start('missing-definition', { input: { tenantId: 'tenant-1' }, idempotencyKey: 'missing:1' }),
    (error) => error instanceof QueuebitError && error.code === 'QB_RUN_DEFINITION_NOT_FOUND'
  );

  await assert.rejects(
    () => runs.start('receipt-campaign', { input: { page: 'not-a-number' }, idempotencyKey: 'invalid:1' }),
    (error) => error instanceof QueuebitError && error.code === 'QB_RUN_INPUT_INVALID'
  );
});

test('runs.get and runs.list use stable run cursors', async () => {
  const { runs } = createRunsRuntime();
  await runs.start('receipt-campaign', { input: { page: 1 }, idempotencyKey: 'run:1' });
  await runs.start('receipt-campaign', { input: { page: 2 }, idempotencyKey: 'run:2' });

  const readBack = await runs.get('run-1');
  assert.equal(readBack?.definition, 'receipt-campaign');
  assert.deepEqual(readBack?.input, { page: 1 });

  const firstPage = await runs.list({ limit: 1 });
  assert.deepEqual(firstPage.items.map(run => run.id), ['run-1']);
  assert.equal(firstPage.nextCursor, '1');

  const secondPage = await runs.list({ cursor: firstPage.nextCursor });
  assert.deepEqual(secondPage.items.map(run => run.id), ['run-2']);
});

test('runs.pause and runs.resume control the same durable run', async () => {
  const { runs } = createRunsRuntime();
  const run = await runs.start('receipt-campaign', { input: { page: 1 }, idempotencyKey: 'run:pause' });

  const paused = await runs.pause(run.id);
  assert.equal(paused.executionState, 'paused');
  assert.equal(paused.pauseRequestedAt, fixedNow.toISOString());
  assert.equal(paused.pausedAt, fixedNow.toISOString());

  const resumed = await runs.resume(run.id);
  assert.equal(resumed.executionState, 'running');
  assert.equal(resumed.resumedAt, fixedNow.toISOString());

  await assert.rejects(
    () => runs.pause('missing-run'),
    (error) => error instanceof QueuebitError && error.code === 'QB_RUN_NOT_FOUND'
  );
});

test('runs.cancel preserves the first reason and creates an immediate cancellation completion when idle', async () => {
  const { config, redis, runs } = createRunsRuntime();
  const keys = createQueuebitKeyBuilder(config);
  const run = await runs.start('receipt-campaign', { input: { page: 1 }, idempotencyKey: 'run:cancel-idle' });

  const cancelled = await runs.cancel(run.id, { reason: 'campaign withdrawn' });
  assert.equal(cancelled.executionState, 'cancelled');
  assert.equal(cancelled.completionState, 'not_required');
  assert.equal(cancelled.cancelReason, 'campaign withdrawn');
  assert.equal(cancelled.cancelRequestedAt, fixedNow.toISOString());
  assert.equal(cancelled.cancelledAt, fixedNow.toISOString());

  const repeated = await runs.cancel(run.id, { reason: 'different reason' });
  assert.equal(repeated.cancelReason, 'campaign withdrawn');

  const completion = redis.hashes.get(keys.completion(`${run.id}:cancelled`));
  assert.equal(completion?.type, 'run.cancelled');
  assert.equal(completion?.completionState, 'not_required');
  assert.equal(redis.zsets.get(keys.runsTerminalDetails())?.has(run.id), true);
});

test('runs.cancel cancels BatchRun-owned waiting jobs and leaves active batches to converge', async () => {
  const { config, redis, runs } = createRunsRuntime();
  const keys = createQueuebitKeyBuilder(config);
  const run = await runs.start('receipt-campaign', { input: { page: 1 }, idempotencyKey: 'run:cancel-batch' });
  const batchId = `${run.id}:batch:0`;
  const jobKey = keys.job('job-1');
  const runHash = redis.hashes.get(keys.run(run.id));
  runHash.inFlightBatches = '1';
  runHash.jobsCreated = '1';
  redis.hashes.set(keys.batch(run.id, 0), {
    id: batchId,
    runId: run.id,
    index: '0',
    nextCursor: '1',
    executionState: 'running',
    completionState: 'not_required',
    jobIds: JSON.stringify(['job-1'])
  });
  redis.zadd(keys.runBatches(run.id), '0', batchId);
  redis.hashes.set(jobKey, {
    id: 'job-1',
    queue: 'notification',
    name: 'send-receipt',
    state: 'waiting',
    attempt: '0',
    attempts: '1',
    createdAt: fixedNow.toISOString(),
    updatedAt: fixedNow.toISOString(),
    data: JSON.stringify({ receiptId: 1 }),
    dataBytes: '15',
    sequence: '1',
    runId: run.id,
    batchId
  });
  redis.rpush(keys.queueWaiting('notification'), jobKey);
  redis.zadd(keys.queueState('notification', 'waiting'), '1', 'job-1');
  Object.assign(redis.getHash(keys.queueCounters('notification')), {
    waitingJobs: '1',
    queuedJobs: '1',
    queuedBytes: '15'
  });

  const cancelling = await runs.cancel(run.id, { reason: 'stop remaining work' });
  assert.equal(cancelling.executionState, 'cancelling');
  assert.equal(cancelling.cancelReason, 'stop remaining work');
  assert.equal(redis.hashes.get(jobKey)?.state, 'cancelled');
  assert.deepEqual(redis.lists.get(keys.queueWaiting('notification')), []);
  assert.equal(redis.getHash(keys.queueCounters('notification')).cancelledJobs, '1');
});

test('runs.listFailures pages processor failures and hides payload by default', async () => {
  const { config, redis, runs } = createRunsRuntime();
  const keys = createQueuebitKeyBuilder(config);
  const run = await runs.start('receipt-campaign', { input: { page: 1 }, idempotencyKey: 'run:failures' });
  redis.zadd(keys.failures(run.id), '1', JSON.stringify({
    sequence: '1',
    runId: run.id,
    batchId: `${run.id}:batch:0`,
    jobId: 'job-1',
    stage: 'processor',
    recordIdentity: 'order:1:0',
    attempt: 2,
    error: { name: 'Error', message: 'provider failed' },
    recoveryAvailable: true,
    payload: { name: 'send-receipt', data: { orderId: 'ord-1' }, idempotencyKey: 'receipt:ord-1' }
  }));
  redis.zadd(keys.failures(run.id), '2', JSON.stringify({
    sequence: '2',
    runId: run.id,
    stage: 'mapper',
    recordIdentity: 'order:2',
    attempt: 1,
    error: { name: 'Error', message: 'mapper failed' },
    recoveryAvailable: false,
    payload: { record: { id: 2 } }
  }));

  const first = await runs.listFailures(run.id, { limit: 1 });
  assert.equal(first.items.length, 1);
  assert.equal(first.items[0].stage, 'processor');
  assert.equal(first.items[0].payload, undefined);
  assert.equal(first.nextCursor, '1');

  const withPayload = await runs.listFailures(run.id, { stage: 'processor', includePayload: true });
  assert.equal(withPayload.items.length, 1);
  assert.deepEqual(withPayload.items[0].payload.data, { orderId: 'ord-1' });
});

test('runs.retryFailed creates an idempotent parent-linked recovery run', async () => {
  const { config, redis, runs } = createRunsRuntime();
  const keys = createQueuebitKeyBuilder(config);
  const run = await runs.start('receipt-campaign', { input: { page: 1 }, idempotencyKey: 'run:retry' });
  redis.hashes.get(keys.run(run.id)).executionState = 'partial_failed';
  redis.hashes.get(keys.run(run.id)).jobsFailed = '1';
  redis.zadd(keys.failures(run.id), '1', JSON.stringify({
    sequence: '1',
    runId: run.id,
    batchId: `${run.id}:batch:0`,
    jobId: 'job-1',
    stage: 'processor',
    recordIdentity: 'order:1:0',
    attempt: 2,
    error: { name: 'Error', message: 'provider failed' },
    recoveryAvailable: true,
    payload: { name: 'send-receipt', data: { orderId: 'ord-1' }, idempotencyKey: 'receipt:ord-1' }
  }));

  const recovery = await runs.retryFailed(run.id, { idempotencyKey: `recovery:${run.id}:1` });
  assert.equal(recovery.id, 'run-2');
  assert.equal(recovery.parentRunId, run.id);
  assert.equal(recovery.recoveryDepth, 1);
  assert.deepEqual(recovery.input.queuebitRecovery.parentRunId, run.id);
  assert.equal(recovery.input.queuebitRecovery.failureCount, 1);

  const repeated = await runs.retryFailed(run.id, { idempotencyKey: `recovery:${run.id}:1` });
  assert.equal(repeated.id, recovery.id);
});

test('runs.retryFailed accepts mapper-only recoverable failures', async () => {
  const { config, redis, runs } = createRunsRuntime();
  const keys = createQueuebitKeyBuilder(config);
  const run = await runs.start('receipt-campaign', { input: { page: 1 }, idempotencyKey: 'run:retry-mapper' });
  redis.hashes.get(keys.run(run.id)).executionState = 'partial_failed';
  redis.hashes.get(keys.run(run.id)).recordsFailed = '1';
  redis.zadd(keys.failures(run.id), '1', JSON.stringify({
    sequence: '1',
    runId: run.id,
    batchId: `${run.id}:batch:0`,
    stage: 'mapper',
    recordIdentity: 'record:0:abc',
    attempt: 0,
    error: { name: 'Error', message: 'mapper failed' },
    recoveryAvailable: true,
    payload: {
      record: { id: 1, orderId: 'ord-1' },
      input: { page: 1 },
      boundary: { upperId: 1 },
      cursor: 0,
      recordIndex: 0
    }
  }));

  const recovery = await runs.retryFailed(run.id, { idempotencyKey: `recovery:${run.id}:mapper` });
  assert.equal(recovery.parentRunId, run.id);
  assert.equal(recovery.recoveryDepth, 1);
  assert.equal(recovery.input.queuebitRecovery.failureCount, 1);
  assert.deepEqual(recovery.input.queuebitRecovery.stages, ['mapper']);
});

test('retention.purge tombstones terminal runs and expires failure envelopes', async () => {
  const { config, redis, runs } = createRunsRuntime({
    retention: {
      terminalRuns: { ageMs: 1, maxCount: 10 }
    }
  });
  const keys = createQueuebitKeyBuilder(config);
  const client = await createQueuebitClient({
    config,
    redis,
    preflight: false,
    now: () => new Date(fixedNow.getTime() + 2)
  });
  try {
    const run = await runs.start('receipt-campaign', {
      input: { tenantId: 'tenant-1' },
      idempotencyKey: 'campaign:tenant-1'
    });
    const record = redis.hashes.get(keys.run(run.id));
    Object.assign(record, {
      executionState: 'failed',
      completionState: 'delivered',
      recordsFailed: '1',
      sourceExhausted: '1',
      inFlightBatches: '0',
      updatedAt: fixedNow.toISOString()
    });
    redis.zadd(keys.runsTerminalDetails(), record.sequence, run.id);
    redis.zadd(keys.failures(run.id), '1', JSON.stringify({
      sequence: '1',
      runId: run.id,
      stage: 'processor',
      recordIdentity: 'order:1:0',
      attempt: 1,
      error: { name: 'Error', message: 'provider failed' },
      recoveryAvailable: true,
      payload: { name: 'send-receipt', data: { orderId: 'ord-1' } }
    }));

    const preview = await client.retention.purge();
    assert.deepEqual(
      preview.candidates.map(candidate => [candidate.window, candidate.runId, candidate.decision, candidate.reason]),
      [['terminalRuns', run.id, 'would_tombstone', 'expired_by_age']]
    );

    const result = await client.retention.purge({ mode: 'execute' });
    assert.equal(result.tombstoned, 1);
    assert.deepEqual(
      result.candidates.map(candidate => [candidate.window, candidate.runId, candidate.decision, candidate.reason]),
      [['terminalRuns', run.id, 'tombstoned', 'expired_by_age']]
    );

    const expired = await runs.get(run.id);
    assert.equal(expired?.detailsExpired, true);
    assert.equal(expired?.failureDetailsExpired, true);
    assert.equal(expired?.detailsExpiredAt, '2026-07-23T11:00:00.002Z');
    assert.equal(expired?.inputDigest, run.inputDigest);
    assert.equal('input' in expired, false);
    assert.equal('boundary' in expired, false);
    assert.equal(redis.zsets.has(keys.failures(run.id)), false);

    const same = await runs.start('receipt-campaign', {
      input: { tenantId: 'tenant-1' },
      idempotencyKey: 'campaign:tenant-1'
    });
    assert.equal(same.id, run.id);
    assert.equal(same.deduplicated, true);
    assert.equal(same.detailsExpired, true);
    assert.equal('input' in same, false);

    const listed = await runs.list({ executionState: 'failed' });
    assert.deepEqual(listed.items.map(item => ({
      id: item.id,
      detailsExpired: item.detailsExpired,
      failureDetailsExpired: item.failureDetailsExpired
    })), [{
      id: run.id,
      detailsExpired: true,
      failureDetailsExpired: true
    }]);

    await assert.rejects(
      () => runs.listFailures(run.id),
      error => error instanceof QueuebitError && error.code === 'QB_RUN_STATE_CONFLICT'
    );
    await assert.rejects(
      () => runs.retryFailed(run.id, { idempotencyKey: `recovery:${run.id}:expired` }),
      error => error instanceof QueuebitError && error.code === 'QB_RUN_STATE_CONFLICT'
    );
  } finally {
    await client.close();
  }
});

test('retention.purge protects terminal runs with unresolved completion events', async () => {
  const { config, redis, runs } = createRunsRuntime({
    retention: {
      completionEvents: { ageMs: 1, maxCount: 10 }
    }
  });
  const keys = createQueuebitKeyBuilder(config);
  const client = await createQueuebitClient({
    config,
    redis,
    preflight: false,
    now: () => new Date(fixedNow.getTime() + 2)
  });
  try {
    const run = await runs.start('receipt-campaign', {
      input: { tenantId: 'tenant-2' },
      idempotencyKey: 'campaign:tenant-2'
    });
    Object.assign(redis.hashes.get(keys.run(run.id)), {
      executionState: 'completed',
      completionState: 'failed',
      sourceExhausted: '1',
      inFlightBatches: '0',
      updatedAt: fixedNow.toISOString()
    });
    redis.zadd(keys.runsTerminalDetails(), redis.hashes.get(keys.run(run.id)).sequence, run.id);

    const result = await client.retention.purge({ mode: 'execute' });
    assert.equal(result.tombstoned, 0);
    assert.deepEqual(
      result.candidates.map(candidate => [candidate.window, candidate.runId, candidate.decision, candidate.reason]),
      [['terminalRuns', run.id, 'skipped', 'completion_protected']]
    );
    assert.equal((await runs.get(run.id))?.detailsExpired, undefined);
  } finally {
    await client.close();
  }
});

test('retention.purge applies terminalRuns maxCount only to terminal detail records', async () => {
  const { config, redis, runs } = createRunsRuntime({
    retention: {
      terminalRuns: { ageMs: 1_000_000, maxCount: 1 }
    }
  });
  const keys = createQueuebitKeyBuilder(config);
  const client = await createQueuebitClient({
    config,
    redis,
    preflight: false,
    now: () => new Date(fixedNow.getTime() + 2)
  });
  try {
    const running = await runs.start('receipt-campaign', {
      input: { tenantId: 'tenant-running' },
      idempotencyKey: 'campaign:tenant-running'
    });
    const oldestTerminal = await runs.start('receipt-campaign', {
      input: { tenantId: 'tenant-terminal-old' },
      idempotencyKey: 'campaign:tenant-terminal-old'
    });
    const newestTerminal = await runs.start('receipt-campaign', {
      input: { tenantId: 'tenant-terminal-new' },
      idempotencyKey: 'campaign:tenant-terminal-new'
    });

    for (const run of [oldestTerminal, newestTerminal]) {
      const record = redis.hashes.get(keys.run(run.id));
      Object.assign(record, {
        executionState: 'completed',
        completionState: 'delivered',
        sourceExhausted: '1',
        inFlightBatches: '0',
        updatedAt: fixedNow.toISOString()
      });
      redis.zadd(keys.runsTerminalDetails(), record.sequence, run.id);
    }

    const preview = await client.retention.purge();
    const previewTerminal = preview.candidates.filter(candidate => candidate.window === 'terminalRuns');
    assert.deepEqual(
      previewTerminal.map(candidate => [candidate.runId, candidate.decision, candidate.reason]),
      [
        [oldestTerminal.id, 'would_tombstone', 'exceeds_max_count'],
        [newestTerminal.id, 'skipped', 'retained_by_window']
      ]
    );
    assert.equal(previewTerminal.some(candidate => candidate.runId === running.id), false);

    const result = await client.retention.purge({ mode: 'execute' });
    const resultTerminal = result.candidates.filter(candidate => candidate.window === 'terminalRuns');
    assert.deepEqual(
      resultTerminal.map(candidate => [candidate.runId, candidate.decision, candidate.reason]),
      [
        [oldestTerminal.id, 'tombstoned', 'exceeds_max_count'],
        [newestTerminal.id, 'skipped', 'retained_by_window']
      ]
    );
    assert.equal(redis.zsets.get(keys.runsTerminalDetails())?.has(oldestTerminal.id), false);
    assert.equal(redis.zsets.get(keys.runsTerminalDetails())?.has(newestTerminal.id), true);
    assert.equal(redis.zsets.get(keys.runsTerminalDetails())?.has(running.id), false);

    const secondPreview = await client.retention.purge();
    assert.deepEqual(
      secondPreview.candidates
        .filter(candidate => candidate.window === 'terminalRuns')
        .map(candidate => [candidate.runId, candidate.decision, candidate.reason]),
      [[newestTerminal.id, 'skipped', 'retained_by_window']]
    );
  } finally {
    await client.close();
  }
});

test('retention.purge tombstones delivered completion events after parent runs are terminal', async () => {
  const { config, redis, runs } = createRunsRuntime({
    retention: {
      completionEvents: { ageMs: 1, maxCount: 10 }
    }
  });
  const keys = createQueuebitKeyBuilder(config);
  const client = await createQueuebitClient({
    config,
    redis,
    preflight: false,
    now: () => new Date(fixedNow.getTime() + 2)
  });
  try {
    const run = await runs.start('receipt-campaign', {
      input: { tenantId: 'tenant-3' },
      idempotencyKey: 'campaign:tenant-3'
    });
    Object.assign(redis.hashes.get(keys.run(run.id)), {
      executionState: 'completed',
      completionState: 'delivered',
      sourceExhausted: '1',
      inFlightBatches: '0',
      updatedAt: new Date(fixedNow.getTime() + 2).toISOString()
    });
    const eventId = `${run.id}:settled`;
    const summary = JSON.stringify({ recordsSeen: 1, jobsCompleted: 1 });
    redis.hashes.set(keys.completion(eventId), {
      id: eventId,
      type: 'run.settled',
      runId: run.id,
      completionState: 'delivered',
      handler: 'finalize-campaign',
      attempt: '1',
      attempts: '3',
      deliveryGeneration: '1',
      summary,
      backoff: JSON.stringify({ type: 'fixed', delayMs: 1000 }),
      lastError: JSON.stringify({ message: 'old transient failure' }),
      nextDueAt: fixedNow.toISOString(),
      deliveryOwnerId: 'coordinator-a',
      deliveryLeaseDeadlineMs: String(fixedNow.getTime() + 30_000),
      deliveryLeaseDeadlineAt: new Date(fixedNow.getTime() + 30_000).toISOString(),
      sequence: '1',
      createdAt: fixedNow.toISOString(),
      updatedAt: fixedNow.toISOString()
    });
    redis.zadd(keys.completionsIndex(), '1', eventId);
    redis.zadd(keys.completionsDetails(), '1', eventId);
    redis.zadd(keys.completionsDue(), String(fixedNow.getTime()), eventId);

    const preview = await client.retention.purge();
    const previewCompletion = preview.candidates.find(candidate => candidate.window === 'completions');
    assert.deepEqual(
      [previewCompletion?.eventId, previewCompletion?.decision, previewCompletion?.reason],
      [eventId, 'would_tombstone', 'expired_by_age']
    );

    const result = await client.retention.purge({ mode: 'execute' });
    const completionCandidate = result.candidates.find(candidate => candidate.window === 'completions');
    assert.deepEqual(
      [completionCandidate?.eventId, completionCandidate?.decision, completionCandidate?.reason],
      [eventId, 'tombstoned', 'expired_by_age']
    );

    const expired = await client.completions.get(eventId);
    assert.equal(expired?.detailsExpired, true);
    assert.equal(expired?.detailsExpiredAt, '2026-07-23T11:00:00.002Z');
    assert.equal(expired?.summaryDigest, createHash('sha1').update(summary).digest('hex'));
    assert.equal('summary' in expired, false);
    assert.equal('backoff' in expired, false);
    assert.equal('lastError' in expired, false);
    assert.equal('nextDueAt' in expired, false);
    assert.equal(redis.zsets.get(keys.completionsDue())?.has(eventId), false);
    assert.equal(redis.zsets.get(keys.completionsDetails())?.has(eventId), false);

    const listed = await client.completions.list({ runId: run.id });
    assert.deepEqual(listed.items.map(item => ({
      id: item.id,
      detailsExpired: item.detailsExpired,
      summaryDigest: item.summaryDigest
    })), [{
      id: eventId,
      detailsExpired: true,
      summaryDigest: createHash('sha1').update(summary).digest('hex')
    }]);

    await assert.rejects(
      () => client.completions.retry(eventId),
      error => error instanceof QueuebitError && error.code === 'QB_COMPLETION_STATE_CONFLICT'
    );
  } finally {
    await client.close();
  }
});

test('retention.purge applies completionEvents maxCount without removing list identity', async () => {
  const { config, redis, runs } = createRunsRuntime({
    retention: {
      terminalRuns: { ageMs: 999_999_999, maxCount: 999 },
      completionEvents: { ageMs: 999_999_999, maxCount: 1 }
    }
  });
  const keys = createQueuebitKeyBuilder(config);
  const client = await createQueuebitClient({
    config,
    redis,
    preflight: false,
    now: () => new Date(fixedNow.getTime() + 2)
  });
  try {
    const run = await runs.start('receipt-campaign', {
      input: { tenantId: 'tenant-max-completions' },
      idempotencyKey: 'campaign:tenant-max-completions'
    });
    Object.assign(redis.hashes.get(keys.run(run.id)), {
      executionState: 'completed',
      completionState: 'delivered',
      sourceExhausted: '1',
      inFlightBatches: '0',
      updatedAt: new Date(fixedNow.getTime() + 2).toISOString()
    });

    const events = [
      { id: `${run.id}:batch:0:settled`, type: 'batch.settled', batchId: `${run.id}:batch:0`, sequence: '1' },
      { id: `${run.id}:settled`, type: 'run.settled', sequence: '2' }
    ];
    for (const event of events) {
      const summary = JSON.stringify({ recordsSeen: Number(event.sequence), jobsCompleted: Number(event.sequence) });
      redis.hashes.set(keys.completion(event.id), {
        id: event.id,
        type: event.type,
        runId: run.id,
        ...(event.batchId === undefined ? {} : { batchId: event.batchId }),
        completionState: 'delivered',
        handler: event.type === 'run.settled' ? 'finalize-campaign' : 'after-batch',
        attempt: '1',
        attempts: '3',
        deliveryGeneration: '1',
        summary,
        sequence: event.sequence,
        createdAt: fixedNow.toISOString(),
        updatedAt: fixedNow.toISOString()
      });
      redis.zadd(keys.completionsIndex(), event.sequence, event.id);
      redis.zadd(keys.completionsDetails(), event.sequence, event.id);
    }

    const preview = await client.retention.purge();
    assert.deepEqual(
      preview.candidates
        .filter(candidate => candidate.window === 'completions')
        .map(candidate => [candidate.eventId, candidate.decision, candidate.reason]),
      [
        [events[0].id, 'would_tombstone', 'exceeds_max_count'],
        [events[1].id, 'skipped', 'retained_by_window']
      ]
    );

    const result = await client.retention.purge({ mode: 'execute' });
    const tombstoned = result.candidates.find(candidate => candidate.eventId === events[0].id);
    assert.deepEqual(
      [tombstoned?.decision, tombstoned?.reason],
      ['tombstoned', 'exceeds_max_count']
    );
    assert.equal(redis.zsets.get(keys.completionsDetails())?.has(events[0].id), false);
    assert.equal(redis.zsets.get(keys.completionsIndex())?.has(events[0].id), true);

    const listed = await client.completions.list({ runId: run.id });
    assert.deepEqual(
      listed.items.map(item => [item.id, item.detailsExpired === true]),
      [
        [events[0].id, true],
        [events[1].id, false]
      ]
    );

    const secondPreview = await client.retention.purge();
    assert.equal(
      secondPreview.candidates.some(candidate => (
        candidate.window === 'completions'
        && candidate.reason === 'exceeds_max_count'
      )),
      false
    );
  } finally {
    await client.close();
  }
});

test('retention.purge protects failed completion events and events whose parent run is not terminal', async () => {
  const { config, redis, runs } = createRunsRuntime({
    retention: {
      completionEvents: { ageMs: 1, maxCount: 10 }
    }
  });
  const keys = createQueuebitKeyBuilder(config);
  const client = await createQueuebitClient({
    config,
    redis,
    preflight: false,
    now: () => new Date(fixedNow.getTime() + 2)
  });
  try {
    const failedRun = await runs.start('receipt-campaign', {
      input: { tenantId: 'tenant-4' },
      idempotencyKey: 'campaign:tenant-4'
    });
    Object.assign(redis.hashes.get(keys.run(failedRun.id)), {
      executionState: 'completed',
      completionState: 'delivered',
      sourceExhausted: '1',
      inFlightBatches: '0',
      updatedAt: new Date(fixedNow.getTime() + 2).toISOString()
    });
    const failedEventId = `${failedRun.id}:failed-completion`;
    redis.hashes.set(keys.completion(failedEventId), {
      id: failedEventId,
      type: 'run.settled',
      runId: failedRun.id,
      completionState: 'failed',
      handler: 'finalize-campaign',
      attempt: '3',
      attempts: '3',
      deliveryGeneration: '3',
      summary: JSON.stringify({ recordsSeen: 1 }),
      lastError: JSON.stringify({ message: 'handler failed' }),
      sequence: '1',
      createdAt: fixedNow.toISOString(),
      updatedAt: fixedNow.toISOString()
    });
    redis.zadd(keys.completionsIndex(), '1', failedEventId);
    redis.zadd(keys.completionsDetails(), '1', failedEventId);

    const runningRun = await runs.start('receipt-campaign', {
      input: { tenantId: 'tenant-5' },
      idempotencyKey: 'campaign:tenant-5'
    });
    Object.assign(redis.hashes.get(keys.run(runningRun.id)), {
      executionState: 'running',
      completionState: 'not_created',
      updatedAt: new Date(fixedNow.getTime() + 2).toISOString()
    });
    const runningEventId = `${runningRun.id}:batch:0:settled`;
    redis.hashes.set(keys.completion(runningEventId), {
      id: runningEventId,
      type: 'batch.settled',
      runId: runningRun.id,
      batchId: `${runningRun.id}:batch:0`,
      completionState: 'delivered',
      handler: 'after-batch',
      attempt: '1',
      attempts: '3',
      deliveryGeneration: '1',
      summary: JSON.stringify({ recordsSeen: 1 }),
      sequence: '2',
      createdAt: fixedNow.toISOString(),
      updatedAt: fixedNow.toISOString()
    });
    redis.zadd(keys.completionsIndex(), '2', runningEventId);
    redis.zadd(keys.completionsDetails(), '2', runningEventId);

    const result = await client.retention.purge({ mode: 'execute' });
    const completionCandidates = result.candidates
      .filter(candidate => candidate.window === 'completions')
      .map(candidate => [candidate.eventId, candidate.decision, candidate.reason]);
    assert.deepEqual(completionCandidates, [
      [failedEventId, 'skipped', 'completion_protected'],
      [runningEventId, 'skipped', 'state_protected']
    ]);
    assert.equal((await client.completions.get(failedEventId))?.detailsExpired, undefined);
    assert.equal((await client.completions.get(runningEventId))?.detailsExpired, undefined);
  } finally {
    await client.close();
  }
});

test('createQueuebitClient exposes runs API when Redis is injected', async () => {
  const { config, redis } = createRunsRuntime();
  const client = await createQueuebitClient({
    config,
    redis,
    preflight: false,
    now: () => fixedNow
  });
  const run = await client.runs.start('receipt-campaign', {
    input: { tenantId: 'tenant-1' },
    idempotencyKey: 'campaign:tenant-1'
  });
  assert.equal(run.definition, 'receipt-campaign');
  await client.close();
});
