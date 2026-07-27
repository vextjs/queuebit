import assert from 'node:assert/strict';
import test from 'node:test';
import { createClient } from '@redis/client';
import {
  createQueuebitClient,
  defineQueuebitConfig,
  defineQueuebitRuntime
} from '../dist/index.js';

const redisTargetConfigured = hasRedisTargetEnv();

test('real Redis 7.2 integration runs direct jobs, BatchRun pages, completions, and recovery', {
  skip: redisTargetConfigured
    ? false
    : 'Set QUEBIT_REDIS_URL or QUEBIT_REDIS_HOST to run the real Redis integration suite.',
  timeout: 20_000
}, async () => {
  const target = readRedisTargetFromEnv();
  const namespace = `redis_it:${Date.now()}:${Math.random().toString(36).slice(2, 10)}`;
  const cleanup = createClient(target.redisClientOptions);
  let client;

  await cleanup.connect();
  try {
    await assertRedisTargetSupported(cleanup);
    await cleanupNamespace(cleanup, namespace);

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
        leaseMs: 2_000,
        renewIntervalMs: 250,
        pollIntervalMs: 5,
        drainTimeoutMs: 2_000
      },
      queues: {
        notification: {}
      },
      batchRuns: {
        'receipt-campaign': {
          queue: 'notification',
          source: 'paid-orders',
          mapper: 'receipt-job',
          pageSize: 2,
          inputSchema: {
            type: 'object',
            additionalProperties: false,
            required: ['tenantId', 'campaignId'],
            properties: {
              tenantId: { type: 'string' },
              campaignId: { type: 'string' }
            }
          },
          completion: {
            run: { handler: 'after-run' }
          }
        }
      },
      limits: {
        maxBulkJobs: 50,
        maxBulkBytes: 100_000,
        maxJobDataBytes: 10_000,
        maxJobResultBytes: 10_000
      }
    });

    const campaigns = new Map([
      ['success', [
        { id: 1, orderId: 'ord-1', userId: 'u-1', message: 'receipt 1' },
        { id: 2, orderId: 'ord-2', userId: 'u-2', message: 'receipt 2' },
        { id: 3, orderId: 'ord-3', userId: 'u-3', message: 'receipt 3' },
        { id: 4, orderId: 'ord-4', userId: 'u-4', message: 'receipt 4' },
        { id: 5, orderId: 'ord-5', userId: 'u-5', message: 'receipt 5' }
      ]],
      ['recover', [
        {
          id: 101,
          orderId: 'ord-recover',
          userId: 'u-recover',
          message: 'retry this receipt',
          failOnce: true
        }
      ]]
    ]);
    let loadCalls = 0;
    const runCompletions = [];
    const mapperCalls = [];
    const processed = [];
    const failedOnce = new Set();

    const runtime = defineQueuebitRuntime({
      sources: {
        'paid-orders': {
          async freeze({ input }) {
            const records = getCampaignRecords(campaigns, input.campaignId);
            return {
              boundary: {
                tenantId: input.tenantId,
                campaignId: input.campaignId,
                upperId: records.at(-1)?.id ?? 0
              },
              cursor: 0,
              totalRecords: records.length
            };
          },
          async load({ input, boundary, cursor, limit }) {
            loadCalls += 1;
            const records = getCampaignRecords(campaigns, input.campaignId)
              .filter(record => record.id > cursor && record.id <= boundary.upperId)
              .slice(0, limit);
            return {
              records,
              nextCursor: records.at(-1)?.id ?? cursor,
              exhausted: records.length === 0 || (records.at(-1)?.id ?? cursor) >= boundary.upperId
            };
          }
        }
      },
      mappers: {
        'receipt-job': (record, context) => {
          mapperCalls.push({ record, runId: context.runId, campaignId: context.input.campaignId });
          return {
            name: 'send-receipt',
            identity: `order:${record.id}`,
            data: {
              kind: 'batch',
              tenantId: context.input.tenantId,
              campaignId: context.input.campaignId,
              orderId: record.orderId,
              userId: record.userId,
              message: record.message,
              failOnce: record.failOnce === true
            },
            options: {
              attempts: 1,
              idempotencyKey: `receipt:${context.input.tenantId}:${record.orderId}`
            }
          };
        }
      },
      completions: {
        'after-run': event => {
          runCompletions.push({
            id: event.id,
            runId: event.runId,
            type: event.type,
            summary: event.summary
          });
        }
      }
    });

    client = await createQueuebitClient({ config });
    const workerA = client.createWorker('notification', createProcessor(processed, failedOnce), {
      workerId: 'redis-worker-a',
      concurrency: 1,
      pollIntervalMs: 5,
      renewIntervalMs: 250,
      leaseMs: 2_000
    });
    const workerB = client.createWorker('notification', createProcessor(processed, failedOnce), {
      workerId: 'redis-worker-b',
      concurrency: 1,
      pollIntervalMs: 5,
      renewIntervalMs: 250,
      leaseMs: 2_000
    });
    workerA.start();
    workerB.start();

    const directJobs = await client.jobs.addBulk('notification', [
      { name: 'send-push', data: { kind: 'direct', userId: 'u-1', message: 'push 1' } },
      { name: 'send-push', data: { kind: 'direct', userId: 'u-2', message: 'push 2' } },
      { name: 'send-push', data: { kind: 'direct', userId: 'u-3', message: 'push 3' } },
      { name: 'send-push', data: { kind: 'direct', userId: 'u-4', message: 'push 4' } }
    ]);
    const directIds = new Set(directJobs.map(job => job.id));
    await waitFor(async () => {
      const completed = await client.jobs.list({ queue: 'notification', state: 'completed', limit: 50 });
      return completed.items.filter(job => directIds.has(job.id)).length === directIds.size;
    });
    assert.ok(
      new Set(processed.filter(item => item.data.kind === 'direct').map(item => item.workerId)).size >= 2,
      'direct bulk jobs should be processed by more than one worker'
    );

    const coordinator = client.createCoordinator(runtime, { coordinatorId: 'redis-coordinator-a' });
    const successRun = await client.runs.start('receipt-campaign', {
      input: { tenantId: 'tenant-1', campaignId: 'success' },
      idempotencyKey: `campaign:success:${namespace}`
    });
    const completedRun = await advanceUntilTerminal(client, coordinator, successRun.id);
    assert.equal(completedRun.executionState, 'completed');
    assert.equal(completedRun.recordsSeen, 5);
    assert.equal(completedRun.jobsCreated, 5);
    assert.equal(completedRun.jobsCompleted, 5);
    assert.equal(completedRun.checkpointCursor, 5);
    assert.ok(runCompletions.some(event => event.runId === successRun.id && event.summary.jobsCompleted === 5));

    const recoveryParent = await client.runs.start('receipt-campaign', {
      input: { tenantId: 'tenant-1', campaignId: 'recover' },
      idempotencyKey: `campaign:recover:${namespace}`
    });
    const partialRun = await advanceUntilTerminal(client, coordinator, recoveryParent.id);
    assert.equal(partialRun.executionState, 'partial_failed');
    assert.equal(partialRun.jobsFailed, 1);

    const failures = await client.runs.listFailures(recoveryParent.id, { includePayload: true });
    assert.equal(failures.items.length, 1);
    assert.equal(failures.items[0].stage, 'processor');
    assert.equal(failures.items[0].payload.data.orderId, 'ord-recover');

    const loadCallsBeforeRecovery = loadCalls;
    const recovery = await client.runs.retryFailed(recoveryParent.id, {
      idempotencyKey: `recovery:${recoveryParent.id}:1`
    });
    assert.equal(recovery.parentRunId, recoveryParent.id);
    const recoveredRun = await advanceUntilTerminal(client, coordinator, recovery.id);
    assert.equal(loadCalls, loadCallsBeforeRecovery, 'recovery replay must not call the source again');
    assert.equal(recoveredRun.executionState, 'completed');
    assert.equal(recoveredRun.jobsCompleted, 1);
    assert.ok(runCompletions.some(event => event.runId === recovery.id && event.summary.jobsCompleted === 1));

    assert.ok(mapperCalls.some(call => call.campaignId === 'success'));
    assert.ok(processed.some(item => item.data.orderId === 'ord-recover' && item.result.delivered === true));
  } finally {
    let closeError;
    if (client !== undefined) {
      try {
        await client.close({ timeoutMs: 2_000 });
      } catch (cause) {
        closeError = cause;
      }
    }
    try {
      await cleanupNamespace(cleanup, namespace);
    } finally {
      await cleanup.quit();
    }
    if (closeError !== undefined) throw closeError;
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
    `Queuebit real Redis integration requires Redis >=7.2, got ${version}.`
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

function getCampaignRecords(campaigns, campaignId) {
  const records = campaigns.get(campaignId);
  if (records === undefined) throw new Error(`Unknown campaign fixture: ${campaignId}`);
  return records;
}

function createProcessor(processed, failedOnce) {
  return async (job, context) => {
    await delay(15);
    if (job.data.failOnce === true && !failedOnce.has(job.data.orderId)) {
      failedOnce.add(job.data.orderId);
      throw new Error('push provider unavailable once');
    }
    const result = { delivered: true, by: context.workerId };
    processed.push({
      jobId: job.id,
      workerId: context.workerId,
      data: job.data,
      result
    });
    return result;
  };
}

async function advanceUntilTerminal(client, coordinator, runId) {
  for (let iteration = 0; iteration < 20; iteration += 1) {
    const result = await coordinator.advanceRun(runId);
    const latest = await client.runs.get(runId);
    if (latest === null) assert.fail(`Run ${runId} disappeared during integration test.`);
    if (isTerminalRun(latest.executionState)) return latest;
    if (result.jobsCreated > 0) {
      const alreadySettled = latest.jobsCompleted + latest.jobsFailed + latest.jobsCancelled;
      await waitFor(async () => (await countSettledJobs(client, runId)) >= alreadySettled + result.jobsCreated);
    } else {
      await delay(10);
    }
  }
  assert.fail(`Run ${runId} did not reach a terminal state.`);
}

function isTerminalRun(state) {
  return state === 'completed' || state === 'partial_failed' || state === 'failed' || state === 'cancelled';
}

async function countSettledJobs(client, runId) {
  const [completed, failed, cancelled] = await Promise.all([
    client.jobs.list({ queue: 'notification', state: 'completed', limit: 100 }),
    client.jobs.list({ queue: 'notification', state: 'failed', limit: 100 }),
    client.jobs.list({ queue: 'notification', state: 'cancelled', limit: 100 })
  ]);
  return [...completed.items, ...failed.items, ...cancelled.items]
    .filter(job => job.runId === runId)
    .length;
}

async function waitFor(predicate, timeoutMs = 5_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() <= deadline) {
    if (await predicate()) return;
    await delay(10);
  }
  assert.fail('Timed out waiting for Redis integration condition.');
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
