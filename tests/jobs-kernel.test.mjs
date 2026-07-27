import assert from 'node:assert/strict';
import test from 'node:test';
import {
  QueuebitError,
  QueuebitScriptRegistry,
  createQueuebitJobsApi,
  defineQueuebitConfig,
  executeQueuebitScript
} from '../dist/index.js';

const fixedNow = new Date('2026-07-23T10:00:00.000Z');

class FakeRedisJobsClient {
  commands = [];
  hashes = new Map();
  strings = new Map();
  zsets = new Map();
  lists = new Map();

  async sendCommand(command) {
    this.commands.push(command);
    const [name] = command;
    if (name === 'EVALSHA') return this.evalJobsAddBulk(command);
    if (name === 'HGETALL') return this.hashes.get(command[1]) ?? {};
    if (name === 'ZRANGEBYSCORE') return this.zrangeByScore(command);
    throw new Error(`Unexpected command ${name}`);
  }

  evalJobsAddBulk(command) {
    const keyCount = Number(command[2]);
    const keys = command.slice(3, 3 + keyCount);
    const args = command.slice(3 + keyCount);
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
    const lowJobs = args[5] === '' ? undefined : Number(args[5]);
    const lowBytes = args[6] === '' ? undefined : Number(args[6]);
    const observedAt = args[7];
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
    const setLatch = (reason) => {
      counters.backpressureLatched = '1';
      counters.backpressureReason = reason;
      counters.backpressureLastCheckedAt = observedAt;
      counters.backpressureSince ??= observedAt;
    };
    const clearLatch = () => {
      delete counters.backpressureLatched;
      delete counters.backpressureReason;
      delete counters.backpressureLastCheckedAt;
      delete counters.backpressureSince;
    };
    const refreshLatch = (jobs, bytes) => {
      if (highJobs === undefined && highBytes === undefined) {
        clearLatch();
        return false;
      }
      counters.backpressureLastCheckedAt = observedAt;
      const belowJobs = highJobs === undefined || jobs <= (lowJobs ?? highJobs);
      const belowBytes = highBytes === undefined || bytes <= (lowBytes ?? highBytes);
      if (counters.backpressureLatched === '1' && belowJobs && belowBytes) {
        clearLatch();
      }
      if (highJobs !== undefined && jobs >= highJobs) setLatch('jobs');
      else if (highBytes !== undefined && bytes >= highBytes) setLatch('bytes');
      return counters.backpressureLatched === '1';
    };
    if (newEntries.length > 0 && highJobs !== undefined && newEntries.length > highJobs) {
      return ['err', 'QB_BACKPRESSURE_REQUEST_TOO_LARGE', 'jobs.addBulk request exceeds queue high job watermark.', '{}'];
    }
    if (newEntries.length > 0 && highBytes !== undefined && newBytes > highBytes) {
      return ['err', 'QB_BACKPRESSURE_REQUEST_TOO_LARGE', 'jobs.addBulk request exceeds queue high byte watermark.', '{}'];
    }
    if (newEntries.length > 0 && refreshLatch(queuedJobs, queuedBytes)) {
      return ['err', 'QB_BACKPRESSURE_REJECTED', 'Queue backpressure is active.', '{}'];
    }
    if (highJobs !== undefined && queuedJobs + newEntries.length > highJobs) {
      setLatch('jobs');
      return ['err', 'QB_BACKPRESSURE_REJECTED', 'Queue job high watermark would be exceeded.', '{}'];
    }
    if (highBytes !== undefined && queuedBytes + newBytes > highBytes) {
      setLatch('bytes');
      return ['err', 'QB_BACKPRESSURE_REJECTED', 'Queue byte high watermark would be exceeded.', '{}'];
    }

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
      this.hashes.set(jobKey, record);
      this.zadd(jobsIndexKey, sequence, entry.jobId);
      if (entry.state === 'delayed') {
        this.zadd(dueKey, String(entry.delayUntilMs), entry.jobId);
        this.zadd(delayedIndexKey, sequence, entry.jobId);
      } else {
        this.rpush(waitingKey, entry.jobId);
        this.zadd(waitingIndexKey, sequence, entry.jobId);
      }
      if (entry.dedupeKeyPosition > 0) {
        const dedupeKey = keys[6 + entryCount + entry.dedupeKeyPosition - 1];
        this.strings.set(dedupeKey, JSON.stringify({ jobId: entry.jobId, dataDigest: entry.dataDigest }));
      }
      resultIds[index] = entry.jobId;
    }

    counters.queuedJobs = String(queuedJobs + newEntries.length);
    counters.queuedBytes = String(queuedBytes + newBytes);
    counters.totalJobs = String(Number(counters.totalJobs ?? 0) + newEntries.length);
    if (newEntries.length > 0) refreshLatch(queuedJobs + newEntries.length, queuedBytes + newBytes);
    return ['ok', JSON.stringify(resultIds)];
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

  rpush(key, member) {
    const list = this.lists.get(key) ?? [];
    list.push(member);
    this.lists.set(key, list);
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

function createJobsApi(options = {}) {
  const config = defineQueuebitConfig({
    namespace: 'test:jobs',
    queues: {
      notification: {
        backpressure: {
          highWatermarkJobs: options.highWatermarkJobs ?? 10,
          lowWatermarkJobs: 0,
          highWatermarkBytes: options.highWatermarkBytes ?? 10_000,
          lowWatermarkBytes: 0
        }
      }
    },
    limits: {
      maxBulkJobs: options.maxBulkJobs ?? 5,
      maxBulkBytes: options.maxBulkBytes ?? 10_000,
      maxJobDataBytes: options.maxJobDataBytes ?? 1_000
    }
  });
  const redis = new FakeRedisJobsClient();
  let nextId = 0;
  const api = createQueuebitJobsApi({
    config,
    redis,
    now: () => fixedNow,
    idGenerator: () => `job-${++nextId}`
  });
  return { api, redis };
}

test('jobs.addBulk creates waiting and delayed snapshots and stable list cursors', async () => {
  const { api } = createJobsApi();
  const jobs = await api.addBulk('notification', [
    {
      name: 'send-receipt',
      data: { orderId: 1 },
      options: { deduplicationKey: 'receipt:1', idempotencyKey: 'side-effect:1' }
    },
    {
      name: 'send-receipt',
      data: { orderId: 2 },
      options: { delayMs: 5_000 }
    }
  ]);

  assert.equal(jobs.length, 2);
  assert.equal(jobs[0].id, 'job-1');
  assert.equal(jobs[0].state, 'waiting');
  assert.deepEqual(jobs[0].data, { orderId: 1 });
  assert.equal(jobs[0].deduplicationKey, 'receipt:1');
  assert.equal(jobs[0].idempotencyKey, 'side-effect:1');
  assert.equal(jobs[1].id, 'job-2');
  assert.equal(jobs[1].state, 'delayed');

  const readBack = await api.get('job-1');
  assert.equal(readBack?.name, 'send-receipt');

  const firstPage = await api.list({ queue: 'notification', limit: 1 });
  assert.equal(firstPage.items.length, 1);
  assert.equal(firstPage.items[0].id, 'job-1');
  assert.equal(firstPage.nextCursor, '1');

  const secondPage = await api.list({ queue: 'notification', cursor: firstPage.nextCursor });
  assert.equal(secondPage.items.length, 1);
  assert.equal(secondPage.items[0].id, 'job-2');

  const delayedPage = await api.list({ queue: 'notification', state: 'delayed' });
  assert.deepEqual(delayedPage.items.map(job => job.id), ['job-2']);
});

test('jobs.add delegates to addBulk and rejects invalid bulk input before Redis writes', async () => {
  const { api, redis } = createJobsApi({ maxBulkJobs: 1 });
  const job = await api.add('notification', 'send-receipt', { orderId: 1 });
  assert.equal(job.id, 'job-1');

  await assert.rejects(
    () => api.addBulk('notification', [
      { name: 'send-receipt', data: { orderId: 2 } },
      { name: 'send-receipt', data: { orderId: 3 } }
    ]),
    (error) => error instanceof QueuebitError && error.code === 'QB_JOB_LIMIT_EXCEEDED'
  );
  const commandCountAfterLimit = redis.commands.length;

  await assert.rejects(
    () => api.addBulk('notification', [
      { name: 'send-receipt', data: { orderId: 4 }, options: { deduplicationKey: 'same' } },
      { name: 'send-receipt', data: { orderId: 5 }, options: { deduplicationKey: 'same' } }
    ]),
    (error) => error instanceof QueuebitError && error.code === 'QB_JOB_LIMIT_EXCEEDED'
  );
  assert.equal(redis.commands.length, commandCountAfterLimit);

  const duplicate = createJobsApi();
  const duplicateCommandCount = duplicate.redis.commands.length;
  await assert.rejects(
    () => duplicate.api.addBulk('notification', [
      { name: 'send-receipt', data: { orderId: 4 }, options: { deduplicationKey: 'same' } },
      { name: 'send-receipt', data: { orderId: 5 }, options: { deduplicationKey: 'same' } }
    ]),
    (error) => error instanceof QueuebitError && error.code === 'QB_JOB_DEDUPLICATION_CONFLICT'
  );
  assert.equal(duplicate.redis.commands.length, duplicateCommandCount);
});

test('jobs.addBulk maps high watermark and deduplication conflicts', async () => {
  const { api } = createJobsApi({ highWatermarkJobs: 1 });
  await api.add('notification', 'send-receipt', { orderId: 1 });
  await assert.rejects(
    () => api.add('notification', 'send-receipt', { orderId: 2 }),
    (error) => error instanceof QueuebitError && error.code === 'QB_BACKPRESSURE_REJECTED'
  );

  const other = createJobsApi();
  const first = await other.api.add('notification', 'send-receipt', { orderId: 1 }, {
    deduplicationKey: 'receipt:1'
  });
  const second = await other.api.add('notification', 'send-receipt', { orderId: 1 }, {
    deduplicationKey: 'receipt:1'
  });
  assert.equal(second.id, first.id);
  await assert.rejects(
    () => other.api.add('notification', 'send-receipt', { orderId: 999 }, {
      deduplicationKey: 'receipt:1'
    }),
    (error) => error instanceof QueuebitError && error.code === 'QB_JOB_DEDUPLICATION_CONFLICT'
  );
});

test('jobs API returns expired detail tombstones without fabricating payloads', async () => {
  const { api, redis } = createJobsApi();
  const first = await api.add('notification', 'send-receipt', { orderId: 1 }, {
    deduplicationKey: 'receipt:1'
  });
  const entry = [...redis.hashes.entries()].find(([, record]) => record.id === first.id);
  assert.ok(entry);
  const [, record] = entry;
  record.state = 'completed';
  record.updatedAt = '2026-07-23T10:00:01.000Z';
  record.detailsExpired = '1';
  record.detailsExpiredAt = '2026-07-23T10:00:02.000Z';
  delete record.data;
  delete record.result;
  delete record.failedReason;
  delete record.options;
  redis.zadd('qb:{test:jobs}:q:notification:state:completed', '1', first.id);

  const expired = await api.get(first.id);
  assert.ok(expired);
  assert.equal(expired.id, first.id);
  assert.equal(expired.state, 'completed');
  assert.equal(expired.detailsExpired, true);
  assert.equal(expired.detailsExpiredAt, '2026-07-23T10:00:02.000Z');
  assert.equal(expired.deduplicationKey, 'receipt:1');
  assert.equal(expired.dataDigest, first.dataDigest);
  assert.equal('data' in expired, false);
  assert.equal('result' in expired, false);

  const same = await api.add('notification', 'send-receipt', { orderId: 1 }, {
    deduplicationKey: 'receipt:1'
  });
  assert.equal(same.id, first.id);
  assert.equal(same.detailsExpired, true);
  assert.equal('data' in same, false);

  await assert.rejects(
    () => api.add('notification', 'send-receipt', { orderId: 2 }, {
      deduplicationKey: 'receipt:1'
    }),
    (error) => error instanceof QueuebitError && error.code === 'QB_JOB_DEDUPLICATION_CONFLICT'
  );

  const completed = await api.list({ queue: 'notification', state: 'completed' });
  assert.deepEqual(completed.items, [{
    id: first.id,
    queue: 'notification',
    name: 'send-receipt',
    state: 'completed',
    attempt: 0,
    attempts: 1,
    createdAt: '2026-07-23T10:00:00.000Z',
    updatedAt: '2026-07-23T10:00:01.000Z',
    dataDigest: first.dataDigest,
    detailsExpired: true,
    detailsExpiredAt: '2026-07-23T10:00:02.000Z'
  }]);
});

test('jobs.addBulk distinguishes oversized requests from recoverable backpressure latch', async () => {
  const tooLarge = createJobsApi({ highWatermarkJobs: 1, maxBulkJobs: 5 });
  await assert.rejects(
    () => tooLarge.api.addBulk('notification', [
      { name: 'send-receipt', data: { orderId: 1 } },
      { name: 'send-receipt', data: { orderId: 2 } }
    ]),
    (error) => error instanceof QueuebitError && error.code === 'QB_BACKPRESSURE_REQUEST_TOO_LARGE'
  );

  const { api, redis } = createJobsApi({ highWatermarkJobs: 2 });
  await api.addBulk('notification', [
    { name: 'send-receipt', data: { orderId: 1 } },
    { name: 'send-receipt', data: { orderId: 2 } }
  ]);
  await assert.rejects(
    () => api.add('notification', 'send-receipt', { orderId: 3 }),
    (error) => error instanceof QueuebitError && error.code === 'QB_BACKPRESSURE_REJECTED'
  );

  const counters = [...redis.hashes.entries()].find(([key]) => key.endsWith(':q:notification:counters'))?.[1];
  assert.ok(counters);
  counters.queuedJobs = '0';
  counters.queuedBytes = '0';
  const recovered = await api.add('notification', 'send-receipt', { orderId: 4 });
  assert.equal(recovered.id, 'job-4');
});

test('executeQueuebitScript reloads NOSCRIPT and validates fixed key counts', async () => {
  const registry = new QueuebitScriptRegistry();
  const script = registry.register({
    name: 'test:script',
    version: 'v1',
    numberOfKeys: 1,
    source: 'return {"ok", "[]"}'
  });
  const calls = [];
  const client = {
    async sendCommand(command) {
      calls.push(command);
      if (command[0] === 'EVALSHA' && calls.length === 1) throw new Error('NOSCRIPT No matching script.');
      if (command[0] === 'SCRIPT') return 'loaded-sha';
      return ['ok', '[]'];
    }
  };

  const reply = await executeQueuebitScript(client, script, ['key:1'], ['arg']);
  assert.deepEqual(reply, ['ok', '[]']);
  assert.deepEqual(calls.map(command => command[0]), ['EVALSHA', 'SCRIPT', 'EVALSHA']);
  await assert.rejects(
    () => executeQueuebitScript(client, script, [], []),
    (error) => error instanceof QueuebitError && error.code === 'QB_REDIS_SCRIPT_INVALID'
  );
});
