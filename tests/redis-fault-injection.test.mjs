import assert from 'node:assert/strict';
import test from 'node:test';
import { createClient } from '@redis/client';
import {
  createQueuebitJobsApi,
  createQueuebitWorkerKernel,
  defineQueuebitConfig
} from '../dist/index.js';

const redisTargetConfigured = hasRedisTargetEnv();

test('real Redis fault injection recovers a stalled active job after worker crash simulation', {
  skip: redisTargetConfigured
    ? false
    : 'Set QUEBIT_REDIS_URL or QUEBIT_REDIS_HOST to run the Redis fault-injection suite.',
  timeout: 15_000
}, async () => {
  const target = readRedisTargetFromEnv();
  const namespace = `redis_fault:${Date.now()}:${Math.random().toString(36).slice(2, 10)}`;
  const redis = createClient(target.redisClientOptions);

  await redis.connect();
  try {
    await assertRedisTargetSupported(redis);
    await cleanupNamespace(redis, namespace);

    const config = defineQueuebitConfig({
      namespace,
      connection: {
        ...target.connection,
        connectTimeoutMs: 5_000,
        commandTimeoutMs: 5_000,
        requestRetryLimit: 1,
        serverPolicy: { mode: 'strict' }
      },
      workerDefaults: {
        concurrency: 1,
        leaseMs: 100,
        renewIntervalMs: 25,
        pollIntervalMs: 5,
        drainTimeoutMs: 500,
        maxStalledRecoveries: 1
      },
      queues: {
        notification: {}
      },
      limits: {
        maxBulkJobs: 10,
        maxBulkBytes: 100_000,
        maxJobDataBytes: 10_000,
        maxJobResultBytes: 10_000
      }
    });

    const jobs = createQueuebitJobsApi({ config, redis });
    const workerA = createQueuebitWorkerKernel({
      config,
      redis,
      queue: 'notification',
      workerId: 'fault-worker-a'
    });
    const workerB = createQueuebitWorkerKernel({
      config,
      redis,
      queue: 'notification',
      workerId: 'fault-worker-b'
    });

    const created = await jobs.add('notification', 'send-receipt', {
      orderId: 'ord-fault-1',
      message: 'recover after crash'
    }, {
      idempotencyKey: `fault:${namespace}:ord-fault-1`
    });
    assert.equal(created.state, 'waiting');

    const claimedByA = await workerA.claim({ leaseMs: 100 });
    assert.ok(claimedByA);
    assert.equal(claimedByA.id, created.id);
    assert.equal(claimedByA.workerId, 'fault-worker-a');
    assert.equal(claimedByA.leaseGeneration, 1);

    await delay(160);
    const recovered = await workerB.recoverStalled({ limit: 10, maxStalledRecoveries: 1 });
    assert.deepEqual(recovered, [created.id]);

    await assert.rejects(
      () => workerA.complete(created.id, claimedByA.leaseGeneration, { deliveredBy: 'stale-worker-a' }),
      error => error?.code === 'QB_JOB_STATE_CONFLICT'
    );

    const claimedByB = await workerB.claim({ leaseMs: 500 });
    assert.ok(claimedByB);
    assert.equal(claimedByB.id, created.id);
    assert.equal(claimedByB.workerId, 'fault-worker-b');
    assert.equal(claimedByB.leaseGeneration, 2);
    assert.equal(claimedByB.attempt, 2);

    const completed = await workerB.complete(created.id, claimedByB.leaseGeneration, {
      delivered: true,
      recoveredBy: 'fault-worker-b'
    });
    assert.equal(completed.state, 'completed');
    assert.deepEqual(completed.result, {
      delivered: true,
      recoveredBy: 'fault-worker-b'
    });

    const active = await jobs.list({ queue: 'notification', state: 'active', limit: 10 });
    assert.equal(active.items.length, 0);
    const completedPage = await jobs.list({ queue: 'notification', state: 'completed', limit: 10 });
    assert.equal(completedPage.items.filter(job => job.id === created.id).length, 1);
  } finally {
    try {
      await cleanupNamespace(redis, namespace);
    } finally {
      await redis.quit();
    }
  }
});

function hasRedisTargetEnv() {
  return Boolean(process.env.QUEUEBIT_REDIS_URL || process.env.QUEUEBIT_REDIS_HOST);
}

function readRedisTargetFromEnv() {
  const urlValue = process.env.QUEUEBIT_REDIS_URL;
  if (urlValue) return parseRedisUrl(urlValue);

  const host = process.env.QUEUEBIT_REDIS_HOST;
  if (!host) throw new Error('QUEUEBIT_REDIS_HOST is required when QUEUEBIT_REDIS_URL is not set.');
  const port = readIntegerEnv('QUEUEBIT_REDIS_PORT', 6379, 1);
  const database = readIntegerEnv('QUEUEBIT_REDIS_DB', 0);
  const tls = process.env.QUEUEBIT_REDIS_TLS === '1' || process.env.QUEUEBIT_REDIS_TLS === 'true';
  const username = readOptionalEnv('QUEUEBIT_REDIS_USERNAME');
  const password = readOptionalEnv('QUEUEBIT_REDIS_PASSWORD');
  return {
    connection: {
      host,
      port,
      database,
      ...(username === undefined ? {} : { username }),
      ...(password === undefined ? {} : { password }),
      ...(tls ? { tls: {} } : {})
    },
    redisClientOptions: {
      socket: { host, port, tls },
      database,
      ...(username === undefined ? {} : { username }),
      ...(password === undefined ? {} : { password })
    }
  };
}

function parseRedisUrl(value) {
  const url = new URL(value);
  if (url.protocol !== 'redis:' && url.protocol !== 'rediss:') {
    throw new Error('QUEUEBIT_REDIS_URL must use redis:// or rediss://.');
  }
  const database = url.pathname.length > 1 ? Number.parseInt(url.pathname.slice(1), 10) : 0;
  if (!Number.isInteger(database) || database < 0) {
    throw new Error('QUEUEBIT_REDIS_URL database path must be a non-negative integer.');
  }
  const tls = url.protocol === 'rediss:';
  return {
    connection: {
      host: url.hostname,
      port: url.port ? Number.parseInt(url.port, 10) : 6379,
      database,
      ...(url.username ? { username: decodeURIComponent(url.username) } : {}),
      ...(url.password ? { password: decodeURIComponent(url.password) } : {}),
      ...(tls ? { tls: { servername: url.hostname } } : {})
    },
    redisClientOptions: { url: value }
  };
}

function readIntegerEnv(name, fallback, minimum = 0) {
  const value = process.env[name];
  if (value === undefined || value.length === 0) return fallback;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed < minimum) {
    throw new Error(`${name} must be an integer >= ${minimum}.`);
  }
  return parsed;
}

function readOptionalEnv(name) {
  const value = process.env[name];
  return value === undefined || value.length === 0 ? undefined : value;
}

async function assertRedisTargetSupported(redis) {
  const info = parseRedisInfo(await redis.info('server'));
  const version = info.redis_version ?? '0.0.0';
  assert.ok(
    compareRedisVersion(version, '7.2.0') >= 0,
    `Queuebit Redis fault injection requires Redis >=7.2, got ${version}.`
  );
}

function parseRedisInfo(source) {
  const result = {};
  for (const line of source.split(/\r?\n/)) {
    if (!line || line.startsWith('#')) continue;
    const separator = line.indexOf(':');
    if (separator === -1) continue;
    result[line.slice(0, separator)] = line.slice(separator + 1).trim();
  }
  return result;
}

function compareRedisVersion(left, right) {
  const leftParts = left.split(/[.-]/).map(part => Number.parseInt(part, 10) || 0);
  const rightParts = right.split(/[.-]/).map(part => Number.parseInt(part, 10) || 0);
  for (let index = 0; index < Math.max(leftParts.length, rightParts.length); index += 1) {
    const diff = (leftParts[index] ?? 0) - (rightParts[index] ?? 0);
    if (diff !== 0) return diff;
  }
  return 0;
}

async function cleanupNamespace(redis, namespace) {
  const pattern = `qb:{${namespace}}:*`;
  let cursor = '0';
  do {
    const reply = await redis.sendCommand(['SCAN', cursor, 'MATCH', pattern, 'COUNT', '1000']);
    cursor = String(reply[0]);
    const keys = reply[1] ?? [];
    if (keys.length > 0) await redis.sendCommand(['DEL', ...keys]);
  } while (cursor !== '0');
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
