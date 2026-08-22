import assert from 'node:assert/strict';
import { createServer } from 'node:net';
import test from 'node:test';
import {
  QueuebitError,
  createQueuebitClient,
  createQueuebitJobsApi,
  createQueuebitObservabilityHttpApi,
  createQueuebitWorker,
  createQueuebitWorkerKernel,
  defineQueuebitConfig
} from '../dist/index.js';

const initialNow = new Date('2026-07-23T10:00:00.000Z');

class FakeRedisQueueClient {
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
    if (name === 'HGETALL') return { ...(this.hashes.get(command[1]) ?? {}) };
    if (name === 'ZADD') return this.zadd(command[1], command[2], command[3]);
    if (name === 'ZCARD') return this.zsets.get(command[1])?.size ?? 0;
    if (name === 'ZRANGEBYSCORE') return this.zrangeByScore(command);
    if (name === 'DEL') return this.hashes.delete(command[1]) ? 1 : 0;
    if (name === 'ZREM') return this.zrem(command[1], command[2]);
    throw new Error(`Unexpected command ${name}`);
  }

  evalScript(command) {
    const keyCount = Number(command[2]);
    const keys = command.slice(3, 3 + keyCount);
    const args = command.slice(3 + keyCount);
    if (keyCount >= 6 && keys[0].includes(':counters')) return this.evalJobsAddBulk(keys, args);
    if (keyCount === 5 && keys[0].includes(':waiting')) return this.evalWorkerClaim(keys, args);
    if (keyCount === 2) return this.evalWorkerRenew(keys, args);
    if (keyCount === 7 && keys[0].includes(':job:')) return this.evalWorkerSettle(keys, args);
    if (keyCount === 8 && keys[0].includes(':job:')) return this.evalJobsCancel(keys, args);
    if (keyCount === 4 && keys[0].includes(':job:')) return this.evalRetentionPurgeCompletedJob(keys, args);
    if (keyCount === 6 && keys[0].includes(':due')) return this.evalPromoteDue(keys, args);
    if (keyCount === 6 && keys[0].includes(':active')) return this.evalRecoverStalled(keys, args);
    throw new Error(`Unexpected script shape with ${keyCount} keys`);
  }

  evalJobsAddBulk(keys, args) {
    const countersKey = keys[0];
    const waitingKey = keys[1];
    const dueKey = keys[2];
    const jobsIndexKey = keys[3];
    const waitingIndexKey = keys[4];
    const delayedIndexKey = keys[5];
    const entryCount = Number(args[0]);
    const maxBulkJobs = Number(args[1]);
    const maxBulkBytes = Number(args[2]);
    const highJobs = args[3] === '' ? undefined : Number(args[3]);
    const highBytes = args[4] === '' ? undefined : Number(args[4]);
    const bulkBytes = Number(args[8]);
    const entries = args.slice(9).map(raw => JSON.parse(raw));

    if (entryCount > maxBulkJobs) {
      return ['err', 'QB_JOB_LIMIT_EXCEEDED', 'jobs.addBulk exceeds maxBulkJobs.', '{}'];
    }
    if (bulkBytes > maxBulkBytes) {
      return ['err', 'QB_JOB_LIMIT_EXCEEDED', 'jobs.addBulk exceeds maxBulkBytes.', '{}'];
    }

    const resultIds = [];
    const newEntries = [];
    for (const [index, entry] of entries.entries()) {
      if (entry.dedupeKeyPosition > 0) {
        const dedupeKey = keys[6 + entryCount + entry.dedupeKeyPosition - 1];
        const existingRaw = this.strings.get(dedupeKey);
        if (existingRaw !== undefined) {
          const existing = JSON.parse(existingRaw);
          if (existing.dataDigest !== entry.dataDigest) {
            return [
              'err',
              'QB_JOB_DEDUPLICATION_CONFLICT',
              'jobs.addBulk deduplicationKey conflicts with existing job data.',
              JSON.stringify({ deduplicationKey: entry.deduplicationKey })
            ];
          }
          resultIds[index] = existing.jobId;
          continue;
        }
      }
      newEntries.push({ index, entry });
    }

    const counters = this.getHash(countersKey);
    const queuedJobs = Number(counters.queuedJobs ?? 0);
    const queuedBytes = Number(counters.queuedBytes ?? 0);
    const newBytes = newEntries.reduce((sum, item) => sum + Number(item.entry.dataBytes), 0);
    if (highJobs !== undefined && queuedJobs + newEntries.length > highJobs) {
      return ['err', 'QB_BACKPRESSURE_REJECTED', 'Queue job high watermark would be exceeded.', '{}'];
    }
    if (highBytes !== undefined && queuedBytes + newBytes > highBytes) {
      return ['err', 'QB_BACKPRESSURE_REJECTED', 'Queue byte high watermark would be exceeded.', '{}'];
    }

    let waitingAdded = 0;
    let delayedAdded = 0;
    for (const { index, entry } of newEntries) {
      const sequence = String(Number(counters.nextSequence ?? 0) + 1);
      counters.nextSequence = sequence;
      const jobKey = keys[6 + index];
      const record = {
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
        sequence
      };
      if (entry.deduplicationKey !== '') record.deduplicationKey = entry.deduplicationKey;
      if (entry.idempotencyKey !== '') record.idempotencyKey = entry.idempotencyKey;
      if (entry.parentJobId !== '') record.parentJobId = entry.parentJobId;
      this.hashes.set(jobKey, record);
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
      if (entry.dedupeKeyPosition > 0) {
        const dedupeKey = keys[6 + entryCount + entry.dedupeKeyPosition - 1];
        this.strings.set(dedupeKey, JSON.stringify({ jobId: entry.jobId, dataDigest: entry.dataDigest }));
      }
      resultIds[index] = entry.jobId;
    }

    this.hincrby(countersKey, 'queuedJobs', newEntries.length);
    this.hincrby(countersKey, 'queuedBytes', newBytes);
    this.hincrby(countersKey, 'totalJobs', newEntries.length);
    if (waitingAdded > 0) this.hincrby(countersKey, 'waitingJobs', waitingAdded);
    if (delayedAdded > 0) this.hincrby(countersKey, 'delayedJobs', delayedAdded);
    return ['ok', JSON.stringify(resultIds)];
  }

  evalWorkerClaim(keys, args) {
    const [waitingKey, activeKey, waitingIndexKey, activeIndexKey, countersKey] = keys;
    const [workerId, leaseDeadlineMs, leaseDeadlineAt, updatedAt, maxSkipsRaw] = args;
    const maxSkips = Number(maxSkipsRaw);
    const list = this.lists.get(waitingKey) ?? [];

    for (let index = 0; index < maxSkips; index += 1) {
      const jobKey = list.shift();
      if (jobKey === undefined) return ['ok', ''];
      const record = this.hashes.get(jobKey);
      if (record?.state !== 'waiting') continue;
      const nextGeneration = String(Number(record.leaseGeneration ?? 0) + 1);
      record.leaseGeneration = nextGeneration;
      if (record.reclaimSameAttempt === '1') {
        delete record.reclaimSameAttempt;
      } else {
        record.attempt = String(Number(record.attempt ?? 0) + 1);
      }
      record.state = 'active';
      record.workerId = workerId;
      record.leaseDeadlineAt = leaseDeadlineAt;
      record.updatedAt = updatedAt;
      this.zrem(waitingIndexKey, record.id);
      this.zadd(activeIndexKey, record.sequence, record.id);
      this.zadd(activeKey, leaseDeadlineMs, jobKey);
      this.hincrby(countersKey, 'waitingJobs', -1);
      this.hincrby(countersKey, 'activeJobs', 1);
      return ['ok', record.id, nextGeneration];
    }
    return ['ok', ''];
  }

  evalWorkerRenew(keys, args) {
    const [jobKey, activeKey] = keys;
    const [workerId, leaseGeneration, leaseDeadlineMs, leaseDeadlineAt, updatedAt] = args;
    const record = this.hashes.get(jobKey);
    const conflict = this.assertActiveOwner(record, workerId, leaseGeneration, 'renewed');
    if (conflict) return conflict;
    record.leaseDeadlineAt = leaseDeadlineAt;
    record.updatedAt = updatedAt;
    this.zadd(activeKey, leaseDeadlineMs, jobKey);
    return ['ok', record.id];
  }

  evalWorkerSettle(keys, args) {
    const [jobKey, activeKey, activeIndexKey, terminalIndexKey, countersKey, dueKey, retryingIndexKey] = keys;
    const [workerId, leaseGeneration, terminalState, payloadJson, updatedAt, nowMsRaw] = args;
    const record = this.hashes.get(jobKey);
    const conflict = this.assertActiveOwner(record, workerId, leaseGeneration, 'settled');
    if (conflict) return conflict;
    if (terminalState === 'completed') {
      record.state = terminalState;
      record.updatedAt = updatedAt;
      if (payloadJson !== '') record.result = payloadJson;
      this.hincrby(countersKey, 'completedJobs', 1);
    } else {
      const attempt = Number(record.attempt ?? 0);
      const attempts = Number(record.attempts ?? 1);
      if (attempt < attempts) {
        const options = JSON.parse(record.options ?? '{}');
        const delayMs = this.computeBackoffDelay(options.backoff, attempt);
        record.state = 'retrying';
        record.failedReason = payloadJson;
        record.retryAtMs = String(Number(nowMsRaw) + delayMs);
        record.updatedAt = updatedAt;
        delete record.workerId;
        delete record.leaseDeadlineAt;
        this.zrem(activeKey, jobKey);
        this.zrem(activeIndexKey, record.id);
        this.zadd(retryingIndexKey, record.sequence, record.id);
        this.zadd(dueKey, record.retryAtMs, jobKey);
        this.hincrby(countersKey, 'activeJobs', -1);
        this.hincrby(countersKey, 'retryingJobs', 1);
        return ['ok', record.id];
      }
      record.state = terminalState;
      record.updatedAt = updatedAt;
      record.failedReason = payloadJson;
      this.hincrby(countersKey, 'failedJobs', 1);
    }
    this.zrem(activeKey, jobKey);
    this.zrem(activeIndexKey, record.id);
    this.zadd(terminalIndexKey, record.sequence, record.id);
    this.hincrby(countersKey, 'activeJobs', -1);
    this.hincrby(countersKey, 'queuedJobs', -1);
    this.hincrby(countersKey, 'queuedBytes', -Number(record.dataBytes ?? 0));
    return ['ok', record.id];
  }

  evalJobsCancel(keys, args) {
    const [
      jobKey,
      waitingKey,
      dueKey,
      waitingIndexKey,
      delayedIndexKey,
      retryingIndexKey,
      cancelledIndexKey,
      countersKey
    ] = keys;
    const [updatedAt] = args;
    const record = this.hashes.get(jobKey);
    if (record === undefined) return ['err', 'QB_JOB_NOT_FOUND', 'Job does not exist.', '{}'];
    if (record.state === 'cancelled') return ['ok', record.id];
    if (!['waiting', 'delayed', 'retrying'].includes(record.state)) {
      return [
        'err',
        'QB_JOB_STATE_CONFLICT',
        'Only waiting, delayed, or retrying jobs can be cancelled.',
        '{}'
      ];
    }
    if (record.state === 'waiting') {
      this.lrem(waitingKey, jobKey);
      this.zrem(waitingIndexKey, record.id);
      this.hincrby(countersKey, 'waitingJobs', -1);
    } else if (record.state === 'delayed') {
      this.zrem(dueKey, jobKey);
      this.zrem(delayedIndexKey, record.id);
      this.hincrby(countersKey, 'delayedJobs', -1);
    } else {
      this.zrem(dueKey, jobKey);
      this.zrem(retryingIndexKey, record.id);
      this.hincrby(countersKey, 'retryingJobs', -1);
    }
    record.state = 'cancelled';
    record.updatedAt = updatedAt;
    delete record.workerId;
    delete record.leaseDeadlineAt;
    delete record.retryAtMs;
    this.zadd(cancelledIndexKey, record.sequence, record.id);
    this.hincrby(countersKey, 'cancelledJobs', 1);
    this.hincrby(countersKey, 'queuedJobs', -1);
    this.hincrby(countersKey, 'queuedBytes', -Number(record.dataBytes ?? 0));
    return ['ok', record.id];
  }

  evalPromoteDue(keys, args) {
    const [dueKey, waitingKey, delayedIndexKey, retryingIndexKey, waitingIndexKey, countersKey] = keys;
    const [nowMsRaw, updatedAt, limitRaw] = args;
    const nowMs = Number(nowMsRaw);
    const limit = Number(limitRaw);
    const due = this.zrangeEntries(dueKey, Number.NEGATIVE_INFINITY, nowMs).slice(0, limit);
    const promoted = [];
    for (const [jobKey] of due) {
      const record = this.hashes.get(jobKey);
      if (record?.state !== 'delayed' && record?.state !== 'retrying') {
        this.zrem(dueKey, jobKey);
        continue;
      }
      const previousState = record.state;
      record.state = 'waiting';
      record.updatedAt = updatedAt;
      delete record.retryAtMs;
      delete record.workerId;
      delete record.leaseDeadlineAt;
      this.zrem(dueKey, jobKey);
      if (previousState === 'delayed') {
        this.zrem(delayedIndexKey, record.id);
        this.hincrby(countersKey, 'delayedJobs', -1);
      } else {
        this.zrem(retryingIndexKey, record.id);
        this.hincrby(countersKey, 'retryingJobs', -1);
      }
      this.rpush(waitingKey, jobKey);
      this.zadd(waitingIndexKey, record.sequence, record.id);
      this.hincrby(countersKey, 'waitingJobs', 1);
      promoted.push(record.id);
    }
    return ['ok', JSON.stringify(promoted)];
  }

  evalRecoverStalled(keys, args) {
    const [activeKey, waitingKey, activeIndexKey, waitingIndexKey, failedIndexKey, countersKey] = keys;
    const [nowMsRaw, updatedAt, limitRaw, maxStalledRaw, failedReasonJson] = args;
    const nowMs = Number(nowMsRaw);
    const limit = Number(limitRaw);
    const maxStalled = Number(maxStalledRaw);
    const expired = this.zrangeEntries(activeKey, Number.NEGATIVE_INFINITY, nowMs).slice(0, limit);
    const recovered = [];
    for (const [jobKey] of expired) {
      const record = this.hashes.get(jobKey);
      if (record?.state !== 'active') {
        this.zrem(activeKey, jobKey);
        continue;
      }
      record.stalledRecoveries = String(Number(record.stalledRecoveries ?? 0) + 1);
      this.zrem(activeKey, jobKey);
      this.zrem(activeIndexKey, record.id);
      this.hincrby(countersKey, 'activeJobs', -1);
      if (Number(record.stalledRecoveries) > maxStalled) {
        record.state = 'failed';
        record.failedReason = failedReasonJson;
        record.updatedAt = updatedAt;
        delete record.workerId;
        delete record.leaseDeadlineAt;
        this.zadd(failedIndexKey, record.sequence, record.id);
        this.hincrby(countersKey, 'failedJobs', 1);
        this.hincrby(countersKey, 'queuedJobs', -1);
        this.hincrby(countersKey, 'queuedBytes', -Number(record.dataBytes ?? 0));
      } else {
        record.state = 'waiting';
        record.updatedAt = updatedAt;
        record.reclaimSameAttempt = '1';
        delete record.workerId;
        delete record.leaseDeadlineAt;
        this.rpush(waitingKey, jobKey);
        this.zadd(waitingIndexKey, record.sequence, record.id);
        this.hincrby(countersKey, 'waitingJobs', 1);
      }
      recovered.push(record.id);
    }
    return ['ok', JSON.stringify(recovered)];
  }

  evalRetentionPurgeCompletedJob(keys, args) {
    const [jobKey, jobsIndexKey, completedIndexKey, countersKey] = keys;
    const [jobId, observedAt] = args;
    const record = this.hashes.get(jobKey);
    if (record === undefined) return ['skip', 'snapshot_missing'];
    if (record.state !== 'completed') return ['skip', 'state_protected', record.state ?? ''];
    if (record.detailsExpired === '1') return ['skip', 'details_expired'];
    if (record.runId !== undefined || record.batchId !== undefined) return ['skip', 'batchrun_owned'];
    if (
      record.deduplicationKey !== undefined
      || record.idempotencyKey !== undefined
      || record.parentJobId !== undefined
    ) {
      if (record.dataDigest === undefined) return ['skip', 'snapshot_missing'];
      record.detailsExpired = '1';
      record.detailsExpiredAt = observedAt;
      delete record.data;
      delete record.result;
      delete record.failedReason;
      delete record.options;
      delete record.workerId;
      delete record.leaseDeadlineAt;
      delete record.retryAtMs;
      delete record.leaseGeneration;
      delete record.stalledRecoveries;
      return ['tombstoned', jobId];
    }
    this.hashes.delete(jobKey);
    this.zrem(jobsIndexKey, jobId);
    this.zrem(completedIndexKey, jobId);
    this.hincrby(countersKey, 'completedJobs', -1);
    return ['deleted', jobId];
  }

  assertActiveOwner(record, workerId, leaseGeneration, verb) {
    if (record === undefined) return ['err', 'QB_JOB_NOT_FOUND', 'Job does not exist.', '{}'];
    if (record.state !== 'active') {
      return ['err', 'QB_JOB_STATE_CONFLICT', `Only active jobs can be ${verb}.`, '{}'];
    }
    if (record.workerId !== workerId) {
      return ['err', 'QB_JOB_STATE_CONFLICT', 'Worker does not own this job lease.', '{}'];
    }
    if (record.leaseGeneration !== leaseGeneration) {
      return ['err', 'QB_JOB_STATE_CONFLICT', 'Worker lease generation is stale.', '{}'];
    }
    return null;
  }

  getHash(key) {
    const existing = this.hashes.get(key);
    if (existing) return existing;
    const created = {};
    this.hashes.set(key, created);
    return created;
  }

  hset(command) {
    const [, key, ...pairs] = command;
    const record = this.getHash(key);
    for (let index = 0; index < pairs.length; index += 2) {
      record[pairs[index]] = pairs[index + 1];
    }
    return pairs.length / 2;
  }

  hincrby(key, field, amount) {
    const record = this.getHash(key);
    record[field] = String(Number(record[field] ?? 0) + amount);
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
    const list = this.lists.get(key) ?? [];
    this.lists.set(key, list.filter(item => item !== member));
  }

  zrangeByScore(command) {
    const [, key, min, max] = command;
    const withScores = command.includes('WITHSCORES');
    const limit = Number(command.at(-1));
    const minValue = min === '-inf' ? Number.NEGATIVE_INFINITY : Number(String(min).slice(1));
    const maxValue = max === '+inf' ? Number.POSITIVE_INFINITY : Number(max);
    const entries = this.zrangeEntries(key, minValue, maxValue).slice(0, limit);
    if (!withScores) return entries.map(([member]) => member);
    return entries.flatMap(([member, score]) => [member, String(score)]);
  }

  zrangeEntries(key, minExclusive, maxInclusive) {
    const zset = this.zsets.get(key) ?? new Map();
    return [...zset.entries()]
      .filter(([, score]) => score > minExclusive && score <= maxInclusive)
      .sort((left, right) => left[1] - right[1]);
  }

  computeBackoffDelay(backoff, attempt) {
    if (!backoff) return 0;
    let delay = Number(backoff.delayMs ?? 0);
    if (backoff.type === 'exponential') delay *= 2 ** Math.max(attempt - 1, 0);
    if (backoff.maxDelayMs !== undefined) delay = Math.min(delay, Number(backoff.maxDelayMs));
    return delay;
  }
}

function createRuntime(options = {}) {
  const config = defineQueuebitConfig({
    namespace: 'test:worker',
    workerDefaults: {
      leaseMs: options.leaseMs ?? 5_000,
      renewIntervalMs: 1_000
    },
    queues: {
      notification: {
        backpressure: {
          highWatermarkJobs: options.highWatermarkJobs ?? 1_000,
          lowWatermarkJobs: options.lowWatermarkJobs ?? 500,
          highWatermarkBytes: options.highWatermarkBytes ?? 10_000,
          lowWatermarkBytes: options.lowWatermarkBytes ?? 5_000
        }
      }
    },
    ...(options.observability === undefined ? {} : { observability: options.observability }),
    ...(options.retention === undefined ? {} : { retention: options.retention }),
    limits: {
      maxBulkJobs: 10,
      maxBulkBytes: 10_000,
      maxJobDataBytes: 1_000,
      maxJobResultBytes: options.maxJobResultBytes ?? 1_000
    }
  });
  const redis = new FakeRedisQueueClient();
  let currentNow = new Date(initialNow);
  let nextId = 0;
  const now = () => currentNow;
  const advance = (ms) => {
    currentNow = new Date(currentNow.getTime() + ms);
  };
  const jobs = createQueuebitJobsApi({
    config,
    redis,
    now,
    idGenerator: () => `job-${++nextId}`
  });
  const workerA = createQueuebitWorkerKernel({
    config,
    redis,
    queue: 'notification',
    workerId: 'worker-a',
    now
  });
  const workerB = createQueuebitWorkerKernel({
    config,
    redis,
    queue: 'notification',
    workerId: 'worker-b',
    now
  });
  return { advance, config, jobs, now, redis, workerA, workerB };
}

async function waitFor(predicate, timeoutMs = 250) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() <= deadline) {
    if (await predicate()) return;
    await new Promise(resolve => setTimeout(resolve, 2));
  }
  assert.fail('Timed out waiting for condition');
}

function findMetric(samples, name, labels = {}) {
  return samples.find(sample =>
    sample.name === name
    && Object.entries(labels).every(([key, value]) => sample.labels[key] === value)
  );
}

test('worker kernel claims, renews, and completes a waiting job with lease fencing', async () => {
  const { advance, jobs, workerA } = createRuntime();
  const created = await jobs.add('notification', 'send-receipt', { orderId: 1 });

  const claimed = await workerA.claim({ leaseMs: 10_000 });
  assert.equal(claimed?.id, created.id);
  assert.equal(claimed?.state, 'active');
  assert.equal(claimed?.attempt, 1);
  assert.equal(claimed?.workerId, 'worker-a');
  assert.equal(claimed?.leaseGeneration, 1);
  assert.equal(claimed?.leaseDeadlineAt, '2026-07-23T10:00:10.000Z');

  advance(2_000);
  const renewed = await workerA.renew(claimed.id, claimed.leaseGeneration, { leaseMs: 20_000 });
  assert.equal(renewed.leaseGeneration, 1);
  assert.equal(renewed.leaseDeadlineAt, '2026-07-23T10:00:22.000Z');

  const completed = await workerA.complete(claimed.id, renewed.leaseGeneration, { delivered: true });
  assert.equal(completed.state, 'completed');
  assert.deepEqual(completed.result, { delivered: true });

  const readBack = await jobs.get(created.id);
  assert.equal(readBack?.state, 'completed');
  assert.deepEqual(readBack?.result, { delivered: true });

  await assert.rejects(
    () => workerA.renew(created.id, renewed.leaseGeneration),
    (error) => error instanceof QueuebitError && error.code === 'QB_JOB_STATE_CONFLICT'
  );
});

test('client metrics honor observability prefix and disabled flag', async () => {
  const enabledRuntime = createRuntime({
    observability: { metrics: { prefix: 'qb_custom_' } }
  });
  const enabledClient = await createQueuebitClient({
    config: enabledRuntime.config,
    redis: enabledRuntime.redis,
    preflight: false,
    now: () => initialNow
  });
  try {
    assert.match(enabledClient.metrics.renderPrometheus(), /qb_custom_client_ready/);
    assert.doesNotMatch(enabledClient.metrics.renderPrometheus(), /queuebit_client_ready/);
  } finally {
    await enabledClient.close({ timeoutMs: 100 });
  }

  const disabledRuntime = createRuntime({
    observability: { metrics: { enabled: false } }
  });
  const disabledClient = await createQueuebitClient({
    config: disabledRuntime.config,
    redis: disabledRuntime.redis,
    preflight: false,
    now: () => initialNow
  });
  try {
    assert.deepEqual(disabledClient.metrics.collect(), []);
    assert.equal(disabledClient.metrics.renderPrometheus(), '');
  } finally {
    await disabledClient.close({ timeoutMs: 100 });
  }
});

test('client observability HTTP helper renders health and metrics responses without listening', async () => {
  const { config, redis } = createRuntime();
  const client = await createQueuebitClient({
    config,
    redis,
    preflight: false,
    now: () => initialNow
  });
  try {
    const health = client.observabilityHttp.handle({ method: 'GET', path: '/queuebit/health?probe=1' });
    assert.equal(health.status, 200);
    assert.equal(health.headers['content-type'], 'application/json; charset=utf-8');
    assert.equal(JSON.parse(health.body).status, 'ready');

    const metrics = client.observabilityHttp.handle({ method: 'GET', path: '/queuebit/metrics' });
    assert.equal(metrics.status, 200);
    assert.match(metrics.headers['content-type'], /text\/plain/);
    assert.match(metrics.body, /queuebit_client_ready/);

    const head = client.observabilityHttp.handle({ method: 'HEAD', path: '/queuebit/metrics' });
    assert.equal(head.status, 200);
    assert.equal(head.body, '');

    const missing = client.observabilityHttp.handle({ method: 'GET', path: '/not-found' });
    assert.equal(missing.status, 404);
    assert.equal(JSON.parse(missing.body).error, 'not_found');

    const method = client.observabilityHttp.handle({ method: 'POST', path: '/queuebit/metrics' });
    assert.equal(method.status, 405);
    assert.equal(method.headers.allow, 'GET, HEAD');

    const custom = createQueuebitObservabilityHttpApi(client, {
      healthPath: 'healthz',
      metricsPath: '/internal/metrics'
    });
    assert.equal(custom.handle({ method: 'GET', path: '/queuebit/metrics' }).status, 404);
    assert.equal(custom.handle({ method: 'GET', path: '/healthz' }).status, 200);
    assert.equal(custom.handle({ method: 'GET', path: '/internal/metrics' }).status, 200);
  } finally {
    await client.close({ timeoutMs: 100 });
  }
});

test('client alerts evaluate health and capacity starting points', async () => {
  const { config, jobs, redis } = createRuntime({
    highWatermarkJobs: 1,
    lowWatermarkJobs: 0
  });
  const client = await createQueuebitClient({
    config,
    redis,
    preflight: false,
    now: () => initialNow
  });
  try {
    await jobs.add('notification', 'send-push', { userId: 'u1' });
    const capacityEvaluation = await client.alerts.evaluate({
      queueUtilizationWarning: 0.5,
      queueUtilizationCritical: 1
    });
    assert.equal(capacityEvaluation.status, 'critical');
    assert.ok(capacityEvaluation.findings.some(finding => finding.id === 'queue_jobs_utilization_high:notification'));

    await client.close({ timeoutMs: 100 });
    const healthEvaluation = await client.alerts.evaluate();
    assert.equal(healthEvaluation.status, 'critical');
    assert.ok(healthEvaluation.findings.some(finding => finding.id === 'health_not_ready'));
  } finally {
    await client.close({ timeoutMs: 100 });
  }
});

test('retention.purge dry-runs expired completed jobs and reports tombstone candidates', async () => {
  const { advance, config, jobs, now, redis, workerA } = createRuntime({
    retention: {
      completedJobs: { ageMs: 1, maxCount: 10 }
    }
  });
  const client = await createQueuebitClient({
    config,
    redis,
    preflight: false,
    now
  });
  try {
    const safe = await jobs.add('notification', 'send-push', { userId: 'u1' });
    const deduped = await jobs.add(
      'notification',
      'send-push',
      { userId: 'u2' },
      { deduplicationKey: 'push:u2' }
    );

    let claimed = await workerA.claim();
    await workerA.complete(claimed.id, claimed.leaseGeneration, { ok: true });
    claimed = await workerA.claim();
    await workerA.complete(claimed.id, claimed.leaseGeneration, { ok: true });

    advance(2);
    const result = await client.retention.purge();
    assert.equal(result.mode, 'dry-run');
    assert.equal(result.deleted, 0);
    assert.equal(result.tombstoned, 0);
    assert.equal(result.skipped, 0);
    assert.equal(result.hasMore, false);
    assert.deepEqual(
      result.candidates.map(candidate => [candidate.jobId, candidate.decision, candidate.reason]),
      [
        [safe.id, 'would_delete', 'expired_by_age'],
        [deduped.id, 'would_tombstone', 'expired_by_age']
      ]
    );
    assert.equal((await jobs.get(safe.id))?.state, 'completed');
    assert.equal((await jobs.get(deduped.id))?.state, 'completed');
  } finally {
    await client.close({ timeoutMs: 100 });
  }
});

test('retention.purge execute deletes safe completed direct jobs and tombstones identity-bound jobs', async () => {
  const { advance, config, jobs, now, redis, workerA } = createRuntime({
    retention: {
      completedJobs: { ageMs: 1, maxCount: 10 }
    }
  });
  const client = await createQueuebitClient({
    config,
    redis,
    preflight: false,
    now
  });
  try {
    const safe = await jobs.add('notification', 'send-push', { userId: 'u1' });
    const deduped = await jobs.add(
      'notification',
      'send-push',
      { userId: 'u2' },
      { deduplicationKey: 'push:u2' }
    );

    let claimed = await workerA.claim();
    await workerA.complete(claimed.id, claimed.leaseGeneration, { ok: true });
    claimed = await workerA.claim();
    await workerA.complete(claimed.id, claimed.leaseGeneration, { ok: true });

    advance(2);
    const result = await client.retention.purge({ mode: 'execute' });
    assert.equal(result.deleted, 1);
    assert.equal(result.tombstoned, 1);
    assert.equal(result.skipped, 0);
    assert.deepEqual(
      result.candidates.map(candidate => [candidate.jobId, candidate.decision, candidate.reason]),
      [
        [safe.id, 'deleted', 'expired_by_age'],
        [deduped.id, 'tombstoned', 'expired_by_age']
      ]
    );
    assert.equal(await jobs.get(safe.id), null);
    const tombstone = await jobs.get(deduped.id);
    assert.equal(tombstone?.state, 'completed');
    assert.equal(tombstone?.detailsExpired, true);
    assert.equal(tombstone?.detailsExpiredAt, '2026-07-23T10:00:00.002Z');
    assert.equal(tombstone?.deduplicationKey, 'push:u2');
    assert.equal(tombstone?.dataDigest, deduped.dataDigest);
    assert.equal('data' in tombstone, false);
    assert.equal('result' in tombstone, false);
    const sameDedupe = await jobs.add(
      'notification',
      'send-push',
      { userId: 'u2' },
      { deduplicationKey: 'push:u2' }
    );
    assert.equal(sameDedupe.id, deduped.id);
    assert.equal(sameDedupe.detailsExpired, true);
    assert.equal('data' in sameDedupe, false);
    await assert.rejects(
      () => jobs.add(
        'notification',
        'send-push',
        { userId: 'changed' },
        { deduplicationKey: 'push:u2' }
      ),
      error => error instanceof QueuebitError && error.code === 'QB_JOB_DEDUPLICATION_CONFLICT'
    );
    assert.deepEqual(
      (await jobs.list({ queue: 'notification', state: 'completed', limit: 10 })).items.map(job => ({
        id: job.id,
        detailsExpired: job.detailsExpired,
        dataDigest: job.dataDigest
      })),
      [{ id: deduped.id, detailsExpired: true, dataDigest: deduped.dataDigest }]
    );
    const capacity = await client.capacity.snapshot();
    assert.equal(capacity.queues[0].counters.completedJobs, 1);
  } finally {
    await client.close({ timeoutMs: 100 });
  }
});

test('worker kernel distributes bulk jobs across workers and rejects wrong owners', async () => {
  const { jobs, workerA, workerB } = createRuntime();
  await jobs.addBulk('notification', [
    { name: 'send-push', data: { userId: 'u1' } },
    { name: 'send-push', data: { userId: 'u2' } }
  ]);

  const first = await workerA.claim();
  const second = await workerB.claim();
  assert.equal(first?.id, 'job-1');
  assert.equal(second?.id, 'job-2');
  assert.equal(await workerA.claim(), null);

  await assert.rejects(
    () => workerA.fail(second.id, second.leaseGeneration, 'not my lease'),
    (error) => error instanceof QueuebitError && error.code === 'QB_JOB_STATE_CONFLICT'
  );

  const failed = await workerB.fail(second.id, second.leaseGeneration, new Error('push provider down'));
  assert.equal(failed.state, 'failed');
  assert.equal(failed.failedReason?.name, 'Error');
  assert.equal(failed.failedReason?.message, 'push provider down');
});

test('worker kernel rejects oversized results before Redis writes and stale generations at settle time', async () => {
  const { jobs, redis, workerA } = createRuntime({ maxJobResultBytes: 20 });
  const created = await jobs.add('notification', 'send-receipt', { orderId: 1 });
  const claimed = await workerA.claim();
  assert.ok(claimed);

  const commandCount = redis.commands.length;
  await assert.rejects(
    () => workerA.complete(created.id, claimed.leaseGeneration, { message: 'this payload is too large' }),
    (error) => error instanceof QueuebitError && error.code === 'QB_JOB_LIMIT_EXCEEDED'
  );
  assert.equal(redis.commands.length, commandCount);

  const stillActive = await jobs.get(created.id);
  assert.equal(stillActive?.state, 'active');

  await assert.rejects(
    () => workerA.complete(created.id, claimed.leaseGeneration + 1, { ok: true }),
    (error) => error instanceof QueuebitError && error.code === 'QB_JOB_STATE_CONFLICT'
  );
});

test('worker runner starts, fills concurrency, completes jobs, and drains cleanly', async () => {
  const { config, jobs, redis } = createRuntime();
  await jobs.addBulk('notification', [
    { name: 'send-push', data: { userId: 'u1' } },
    { name: 'send-push', data: { userId: 'u2' } },
    { name: 'send-push', data: { userId: 'u3' } }
  ]);
  const processed = [];
  const worker = createQueuebitWorker({
    config,
    redis,
    queue: 'notification',
    workerId: 'worker-loop',
    concurrency: 2,
    pollIntervalMs: 1,
    renewIntervalMs: 25,
    leaseMs: 1_000,
    processor: async (job, context) => {
      assert.equal(context.workerId, 'worker-loop');
      assert.equal(context.queue, 'notification');
      processed.push(job.id);
      return { processed: job.id };
    }
  });

  worker.start();
  await waitFor(() => processed.length === 3);
  await worker.drain({ timeoutMs: 100 });

  const status = worker.status();
  assert.equal(status.status, 'stopped');
  assert.equal(status.activeJobs, 0);
  const completed = await jobs.list({ queue: 'notification', state: 'completed', limit: 10 });
  assert.deepEqual(completed.items.map(job => job.id), ['job-1', 'job-2', 'job-3']);
});

test('worker runner fails jobs when the processor throws', async () => {
  const { config, jobs, redis } = createRuntime();
  await jobs.add('notification', 'send-push', { userId: 'u1' });
  const worker = createQueuebitWorker({
    config,
    redis,
    queue: 'notification',
    workerId: 'worker-fail',
    pollIntervalMs: 1,
    renewIntervalMs: 25,
    leaseMs: 1_000,
    processor: () => {
      throw new Error('provider unavailable');
    }
  });

  worker.start();
  await waitFor(async () => (await jobs.get('job-1'))?.state === 'failed');
  await worker.drain({ timeoutMs: 100 });

  const failed = await jobs.get('job-1');
  assert.equal(failed?.state, 'failed');
  assert.equal(failed?.failedReason?.message, 'provider unavailable');
});

test('worker runner captures an initial heartbeat failure without an unhandled rejection', async () => {
  const { config, redis } = createRuntime();
  const initialFailure = new Error('synthetic initial heartbeat failure');
  let heartbeatCalls = 0;
  const unhandledRejections = [];
  const onUnhandledRejection = reason => unhandledRejections.push(reason);
  const worker = createQueuebitWorker({
    config,
    redis,
    queue: 'notification',
    workerId: 'worker-initial-heartbeat',
    pollIntervalMs: 100,
    heartbeatIntervalMs: 10_000,
    heartbeatTtlMs: 20_000,
    processor: async () => ({ ok: true }),
    roleRegistry: {
      async heartbeat() {
        heartbeatCalls += 1;
        if (heartbeatCalls === 1) throw initialFailure;
        return { snapshot: {}, drainRequested: false };
      },
      async unregister() {}
    }
  });

  process.on('unhandledRejection', onUnhandledRejection);
  try {
    worker.start();
    await waitFor(() => worker.status().lastError?.message === initialFailure.message);
    await worker.stop({ timeoutMs: 100 });
    await new Promise(resolve => setTimeout(resolve, 0));

    assert.equal(worker.status().status, 'stopped');
    assert.deepEqual(unhandledRejections, []);
  } finally {
    process.off('unhandledRejection', onUnhandledRejection);
    if (worker.status().status !== 'stopped') {
      await worker.stop({ timeoutMs: 100 }).catch(() => {});
    }
  }
});

test('worker runner exposes timeout through AbortSignal and settles the resulting failure', async () => {
  const { config, jobs, redis } = createRuntime();
  await jobs.add('notification', 'send-push', { userId: 'u1' });
  let sawAbort = false;
  const worker = createQueuebitWorker({
    config,
    redis,
    queue: 'notification',
    workerId: 'worker-timeout',
    pollIntervalMs: 1,
    renewIntervalMs: 25,
    leaseMs: 1_000,
    timeoutMs: 5,
    processor: (_job, context) => new Promise((_resolve, reject) => {
      context.signal.addEventListener('abort', () => {
        sawAbort = true;
        reject(new Error('processor aborted'));
      }, { once: true });
    })
  });

  worker.start();
  await waitFor(async () => (await jobs.get('job-1'))?.state === 'failed');
  await worker.drain({ timeoutMs: 100 });

  const failed = await jobs.get('job-1');
  assert.equal(sawAbort, true);
  assert.equal(failed?.failedReason?.message, 'processor aborted');
});

test('worker runner drain stops new claims, times out safely, and can finish later', async () => {
  const { config, jobs, redis } = createRuntime();
  await jobs.addBulk('notification', [
    { name: 'send-push', data: { userId: 'u1' } },
    { name: 'send-push', data: { userId: 'u2' } }
  ]);
  let release;
  const worker = createQueuebitWorker({
    config,
    redis,
    queue: 'notification',
    workerId: 'worker-drain',
    concurrency: 1,
    pollIntervalMs: 1,
    renewIntervalMs: 25,
    leaseMs: 1_000,
    processor: () => new Promise(resolve => {
      release = () => resolve({ ok: true });
    })
  });

  worker.start();
  await waitFor(() => worker.status().activeJobs === 1);
  await assert.rejects(
    () => worker.drain({ timeoutMs: 5 }),
    (error) => error instanceof QueuebitError && error.code === 'QB_WORKER_DRAIN_TIMEOUT'
  );
  assert.equal(worker.status().status, 'draining');

  release();
  await worker.drain({ timeoutMs: 100 });

  const completed = await jobs.list({ queue: 'notification', state: 'completed', limit: 10 });
  const waiting = await jobs.list({ queue: 'notification', state: 'waiting', limit: 10 });
  assert.deepEqual(completed.items.map(job => job.id), ['job-1']);
  assert.deepEqual(waiting.items.map(job => job.id), ['job-2']);
});

test('worker advancement promotes delayed jobs into the claimable waiting queue', async () => {
  const { advance, jobs, workerA } = createRuntime();
  await jobs.add('notification', 'send-push', { userId: 'u1' }, { delayMs: 100 });

  assert.equal(await workerA.claim(), null);
  assert.deepEqual(await workerA.promoteDue(), []);

  advance(100);
  assert.deepEqual(await workerA.promoteDue(), ['job-1']);
  const claimed = await workerA.claim();
  assert.equal(claimed?.id, 'job-1');
  assert.equal(claimed?.state, 'active');
});

test('worker settlement moves failed attempts into retrying and promotes them after backoff', async () => {
  const { advance, jobs, workerA } = createRuntime();
  await jobs.add('notification', 'send-push', { userId: 'u1' }, {
    attempts: 2,
    backoff: { type: 'fixed', delayMs: 50 }
  });

  const first = await workerA.claim();
  assert.ok(first);
  const retrying = await workerA.fail(first.id, first.leaseGeneration, 'temporary provider failure');
  assert.equal(retrying.state, 'retrying');

  assert.deepEqual(await workerA.promoteDue(), []);
  advance(50);
  assert.deepEqual(await workerA.promoteDue(), ['job-1']);

  const second = await workerA.claim();
  assert.equal(second?.attempt, 2);
  const failed = await workerA.fail(second.id, second.leaseGeneration, 'permanent provider failure');
  assert.equal(failed.state, 'failed');
  assert.equal(failed.failedReason?.message, 'permanent provider failure');
});

test('worker advancement recovers stalled active jobs and fails them after the recovery limit', async () => {
  const { advance, jobs, workerA, workerB } = createRuntime();
  await jobs.add('notification', 'send-push', { userId: 'u1' });

  const first = await workerA.claim({ leaseMs: 5 });
  assert.ok(first);
  assert.equal(first.attempt, 1);
  assert.equal(first.leaseGeneration, 1);
  advance(6);
  assert.deepEqual(await workerB.recoverStalled({ maxStalledRecoveries: 1 }), ['job-1']);
  assert.equal((await jobs.get('job-1'))?.state, 'waiting');
  assert.equal((await jobs.get('job-1'))?.attempt, 1);

  const second = await workerB.claim({ leaseMs: 5 });
  assert.ok(second);
  // Stalled reclaim keeps the same business attempt and only advances lease generation.
  assert.equal(second.attempt, 1);
  assert.equal(second.leaseGeneration, 2);
  advance(6);
  assert.deepEqual(await workerA.recoverStalled({ maxStalledRecoveries: 1 }), ['job-1']);

  const failed = await jobs.get('job-1');
  assert.equal(failed?.state, 'failed');
  assert.equal(failed?.failedReason?.message, 'Job exceeded max stalled recoveries.');
});

test('jobs.cancel atomically cancels waiting jobs and is idempotent', async () => {
  const { jobs, workerA } = createRuntime();
  await jobs.add('notification', 'send-push', { userId: 'u1' });

  const cancelled = await jobs.cancel('job-1');
  assert.equal(cancelled.state, 'cancelled');
  assert.equal((await jobs.cancel('job-1')).state, 'cancelled');
  assert.equal(await workerA.claim(), null);

  const page = await jobs.list({ queue: 'notification', state: 'cancelled' });
  assert.deepEqual(page.items.map(job => job.id), ['job-1']);
});

test('jobs.cancel rejects active jobs and jobs.retryFailed creates direct replacements', async () => {
  const { jobs, workerA } = createRuntime();
  await jobs.add('notification', 'send-push', { userId: 'u1' }, {
    attempts: 1,
    idempotencyKey: 'side-effect:u1'
  });
  const claimed = await workerA.claim();
  assert.ok(claimed);

  await assert.rejects(
    () => jobs.cancel('job-1'),
    (error) => error instanceof QueuebitError && error.code === 'QB_JOB_STATE_CONFLICT'
  );

  await workerA.fail(claimed.id, claimed.leaseGeneration, 'permanent failure');
  const replacement = await jobs.retryFailed('job-1', {
    deduplicationKey: 'replacement:job-1:1'
  });

  assert.equal(replacement.id, 'job-2');
  assert.equal(replacement.state, 'waiting');
  assert.equal(replacement.parentJobId, 'job-1');
  assert.equal(replacement.idempotencyKey, 'side-effect:u1');

  await assert.rejects(
    () => jobs.retryFailed('job-2', { deduplicationKey: 'replacement:job-2:1' }),
    (error) => error instanceof QueuebitError && error.code === 'QB_JOB_STATE_CONFLICT'
  );
});

test('createQueuebitClient exposes jobs, worker factory, health, metrics, and close', async () => {
  const { config, redis } = createRuntime();
  const client = await createQueuebitClient({
    config,
    redis,
    preflight: false,
    now: () => initialNow
  });
  assert.equal(client.health.snapshot().status, 'ready');
  assert.match(client.metrics.renderPrometheus(), /queuebit_client_ready/);
  assert.equal(client.retention.plan().completedJobs.tombstoneTtlMs, 604_800_000);

  let worker;
  try {
    const created = await client.jobs.add('notification', 'send-push', { userId: 'u1' });
    worker = client.createWorker('notification', async (job) => ({ ok: job.id }), {
      workerId: 'client-worker',
      pollIntervalMs: 1,
      heartbeatIntervalMs: 1,
      heartbeatTtlMs: 100,
      renewIntervalMs: 25,
      leaseMs: 1_000,
      roleRegistry: undefined,
      observability: {
        incrementCounter() {
          throw new Error('client.createWorker must use the client observability recorder.');
        },
        setGauge() {
          throw new Error('client.createWorker must use the client observability recorder.');
        },
        observeDuration() {
          throw new Error('client.createWorker must use the client observability recorder.');
        }
      }
    });
    worker.start();
    await waitFor(async () => (await client.jobs.get(created.id))?.state === 'completed');
    const roles = await client.roles.list({ role: 'worker', domain: 'notification' });
    assert.deepEqual(roles.items.map(role => role.identity), ['client-worker']);

    await client.roles.requestDrain({
      role: 'worker',
      domain: 'notification',
      identity: 'client-worker',
      reason: 'rolling deploy'
    });
    await waitFor(() => worker.status().status === 'stopped');
    const capacity = await client.capacity.snapshot();
    assert.equal(capacity.queues[0].queue, 'notification');
    assert.equal(capacity.queues[0].counters.completedJobs, 1);
    assert.equal(capacity.queues[0].watermarks.highWatermarkJobs, 1_000);
    assert.equal(capacity.queues[0].utilization.jobs, 0);

    const samples = client.metrics.collect();
    assert.equal(
      findMetric(samples, 'queuebit_jobs_submitted_total', { queue: 'notification', source: 'direct' })?.value,
      1
    );
    assert.equal(
      findMetric(samples, 'queuebit_worker_jobs_claimed_total', {
        queue: 'notification',
        workerId: 'client-worker'
      })?.value,
      1
    );
    assert.equal(
      findMetric(samples, 'queuebit_worker_jobs_completed_total', {
        queue: 'notification',
        workerId: 'client-worker'
      })?.value,
      1
    );
    assert.equal(
      findMetric(samples, 'queuebit_worker_job_duration_ms_count', {
        queue: 'notification',
        workerId: 'client-worker'
      })?.value,
      1
    );
    assert.match(client.metrics.renderPrometheus(), /# TYPE queuebit_worker_jobs_completed_total counter/);
  } finally {
    await client.close({ timeoutMs: 100 });
  }
  assert.equal(worker?.status().status, 'stopped');
  assert.equal(client.health.snapshot().status, 'draining');
});

test('createQueuebitClient accepts a direct user config', async () => {
  const { redis } = createRuntime();
  const client = await createQueuebitClient(
    {
      connection: { url: 'redis://127.0.0.1:6379/0' },
      namespace: 'test:client-direct-config',
      queues: { notification: {} }
    },
    { redis, preflight: false, now: () => initialNow }
  );
  try {
    const job = await client.jobs.add('notification', 'send-push', { userId: 'u1' });
    assert.equal(job.state, 'waiting');
  } finally {
    await client.close();
  }
});

test('createQueuebitClient closes an owned Redis connection when preflight rejects', async () => {
  const server = await createRedisProtocolServer({ redisVersion: '7.1.0' });
  try {
    const config = createOwnedConnectionConfig(server.port, 'test:client-preflight-close');
    await assert.rejects(
      () => createQueuebitClient({ config }),
      (error) => error instanceof QueuebitError && error.code === 'QB_REDIS_PREFLIGHT_FAILED'
    );
    await waitFor(() => server.commands.some(command => command[0] === 'QUIT'));
    await waitFor(() => server.closedConnections >= 1);
  } finally {
    await server.close();
  }
});

test('createQueuebitClient cleans all workers and its owned Redis connection when stop fails', async () => {
  const server = await createRedisProtocolServer();
  let client;
  try {
    const config = createOwnedConnectionConfig(server.port, 'test:client-close-cleanup');
    client = await createQueuebitClient({ config, preflight: false });
    const first = client.createWorker('notification', async () => ({ ok: true }), { workerId: 'close-first' });
    const second = client.createWorker('notification', async () => ({ ok: true }), { workerId: 'close-second' });
    const stopFailure = new Error('synthetic worker stop failure');
    let firstStops = 0;
    let secondStops = 0;
    first.stop = async () => {
      firstStops += 1;
      throw stopFailure;
    };
    second.stop = async () => {
      secondStops += 1;
    };

    await assert.rejects(
      () => client.close(),
      (error) => error instanceof AggregateError && error.errors.includes(stopFailure)
    );
    assert.equal(firstStops, 1);
    assert.equal(secondStops, 1);
    await waitFor(() => server.commands.some(command => command[0] === 'QUIT'));
  } finally {
    await client?.close().catch(() => {});
    await server.close();
  }
});

function createOwnedConnectionConfig(port, namespace) {
  return defineQueuebitConfig({
    namespace,
    connection: {
      host: '127.0.0.1',
      port,
      connectTimeoutMs: 500,
      commandTimeoutMs: 500,
      requestRetryLimit: 0,
      serverPolicy: { mode: 'strict' }
    },
    queues: { notification: {} }
  });
}

async function createRedisProtocolServer({ redisVersion = '7.2.5' } = {}) {
  const commands = [];
  const sockets = new Set();
  let closedConnections = 0;
  const server = createServer(socket => {
    sockets.add(socket);
    let pending = Buffer.alloc(0);
    socket.on('data', chunk => {
      pending = Buffer.concat([pending, chunk]);
      const parsed = parseRespCommands(pending);
      pending = parsed.remainder;
      for (const command of parsed.commands) {
        commands.push(command);
        socket.write(redisResponse(command, redisVersion));
      }
    });
    socket.on('close', () => {
      sockets.delete(socket);
      closedConnections += 1;
    });
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  return {
    commands,
    get closedConnections() {
      return closedConnections;
    },
    port: address.port,
    async close() {
      for (const socket of sockets) socket.destroy();
      await new Promise(resolve => server.close(resolve));
    }
  };
}

function parseRespCommands(source) {
  const commands = [];
  let offset = 0;
  while (offset < source.length) {
    if (source[offset] !== 42) break;
    const commandStart = offset;
    const arrayLengthLine = readRespLine(source, offset + 1);
    if (arrayLengthLine === undefined) break;
    const arrayLength = Number.parseInt(arrayLengthLine.value, 10);
    if (!Number.isInteger(arrayLength) || arrayLength < 0) throw new Error('Invalid RESP array length.');
    offset = arrayLengthLine.next;
    const command = [];
    let complete = true;
    for (let index = 0; index < arrayLength; index += 1) {
      if (source[offset] !== 36) {
        complete = false;
        break;
      }
      const lengthLine = readRespLine(source, offset + 1);
      if (lengthLine === undefined) {
        complete = false;
        break;
      }
      const length = Number.parseInt(lengthLine.value, 10);
      const start = lengthLine.next;
      const end = start + length;
      if (!Number.isInteger(length) || length < 0 || end + 2 > source.length) {
        complete = false;
        break;
      }
      command.push(source.toString('utf8', start, end));
      offset = end + 2;
    }
    if (!complete) {
      offset = commandStart;
      break;
    }
    commands.push(command);
  }
  return { commands, remainder: source.subarray(offset) };
}

function readRespLine(source, offset) {
  const end = source.indexOf('\r\n', offset, 'utf8');
  if (end === -1) return undefined;
  return { value: source.toString('utf8', offset, end), next: end + 2 };
}

function redisResponse(command, redisVersion) {
  const name = command[0];
  if (name === 'HELLO') {
    return [
      '%7\r\n',
      '$6\r\nserver\r\n$5\r\nredis\r\n',
      '$7\r\nversion\r\n$5\r\n7.2.5\r\n',
      '$5\r\nproto\r\n:3\r\n',
      '$2\r\nid\r\n:1\r\n',
      '$4\r\nmode\r\n$10\r\nstandalone\r\n',
      '$4\r\nrole\r\n$6\r\nmaster\r\n',
      '$7\r\nmodules\r\n*0\r\n'
    ].join('');
  }
  if (name === 'INFO') {
    const section = command[1];
    if (section === 'server') return redisBulk(`redis_version:${redisVersion}\r\ncluster_enabled:0\r\n`);
    if (section === 'persistence') return redisBulk('aof_enabled:1\r\n');
    if (section === 'replication') return redisBulk('role:master\r\n');
  }
  if (name === 'CONFIG' && command[1] === 'GET') {
    if (command[2] === 'maxmemory-policy') return redisMap({ 'maxmemory-policy': 'noeviction' });
    if (command[2] === 'save') return redisMap({ save: '60 1' });
  }
  if (name === 'ROLE') return '*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n';
  return '+OK\r\n';
}

function redisBulk(value) {
  return `$${Buffer.byteLength(value)}\r\n${value}\r\n`;
}

function redisMap(entries) {
  const pairs = Object.entries(entries);
  return `%${pairs.length}\r\n${pairs.map(([key, value]) => redisBulk(key) + redisBulk(value)).join('')}`;
}
