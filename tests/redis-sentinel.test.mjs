import assert from 'node:assert/strict';
import { createClient } from '@redis/client';
import test from 'node:test';
import {
  createQueuebitClient,
  createQueuebitRedisConnection,
  defineQueuebitConfig
} from '../dist/index.js';

let target;
try {
  target = getSentinelTargetFromEnv();
} catch (error) {
  target = { error };
}

test(
  'real Redis Sentinel target routes Queuebit commands through discovered primary and optionally fails over safely',
  target?.error ? { skip: String(target.error.message ?? target.error) } : { timeout: 60_000 },
  async () => {
    const namespace = `redis_sentinel:${Date.now()}:${Math.random().toString(16).slice(2)}`;
    const config = defineQueuebitConfig({
      namespace,
      connection: {
        ...target.connection,
        serverPolicy: { mode: 'warn' }
      }
    });
    let cleanupConnection;
    let cleanupRedis;
    let client;
    let testFailure;

    try {
      cleanupConnection = createQueuebitRedisConnection(config, { name: `queuebit:${namespace}:cleanup` });
      cleanupRedis = await cleanupConnection.connect();
      await cleanupNamespace(cleanupRedis, namespace);
      client = await createQueuebitClient({ config });
      const job = await client.jobs.add('notification', { kind: 'sentinel-smoke' });
      const fetched = await client.jobs.get('notification', job.id);

      assert.equal(config.connection.mode, 'sentinel');
      assert.equal(fetched?.id, job.id);
      assert.deepEqual(fetched?.data, { kind: 'sentinel-smoke' });

      if (process.env.QUEUEBIT_REDIS_SENTINEL_ALLOW_FAILOVER === '1') {
        await client.close();
        client = undefined;
        await cleanupNamespace(cleanupRedis, namespace);
        await cleanupConnection.close();
        cleanupConnection = undefined;
        cleanupRedis = undefined;

        await requestSentinelFailover(target);
        cleanupConnection = createQueuebitRedisConnection(
          config,
          { name: `queuebit:${namespace}:recovered-cleanup` }
        );
        cleanupRedis = await cleanupConnection.connect();
        await cleanupNamespace(cleanupRedis, namespace);
        client = await createQueuebitClient({ config });
        const recoveredJob = await client.jobs.add('notification', { kind: 'sentinel-failover-smoke' });
        const recoveredFetched = await client.jobs.get('notification', recoveredJob.id);
        assert.equal(recoveredFetched?.id, recoveredJob.id);
        assert.deepEqual(recoveredFetched?.data, { kind: 'sentinel-failover-smoke' });
      }
    } catch (error) {
      testFailure = error;
      throw error;
    } finally {
      const cleanupFailures = [];
      if (client !== undefined) {
        try {
          await client.close();
        } catch (error) {
          cleanupFailures.push(error);
        }
      }
      if (cleanupRedis !== undefined && cleanupConnection !== undefined) {
        try {
          await cleanupNamespace(cleanupRedis, namespace);
        } catch (error) {
          cleanupFailures.push(error);
        }
        try {
          await cleanupConnection.close();
        } catch (error) {
          cleanupFailures.push(error);
        }
      }
      if (cleanupFailures.length > 0 && testFailure === undefined) {
        throw new AggregateError(cleanupFailures, 'Redis Sentinel test cleanup failed.');
      }
    }
  }
);

function getSentinelTargetFromEnv() {
  const masterName = process.env.QUEUEBIT_REDIS_SENTINEL_MASTER;
  const sentinelList = process.env.QUEUEBIT_REDIS_SENTINELS;
  if (!masterName || !sentinelList) {
    throw new Error('QUEUEBIT_REDIS_SENTINEL_MASTER and QUEUEBIT_REDIS_SENTINELS are required.');
  }
  const sentinels = sentinelList.split(',').map(entry => {
    const [host, rawPort] = entry.trim().split(':');
    if (!host) throw new Error(`Invalid Sentinel entry: ${entry}`);
    return {
      host,
      port: rawPort ? Number.parseInt(rawPort, 10) : 26379
    };
  });
  if (sentinels.length < 2) throw new Error('QUEUEBIT_REDIS_SENTINELS requires at least two nodes.');
  for (const node of sentinels) {
    if (!Number.isInteger(node.port) || node.port < 1 || node.port > 65_535) {
      throw new Error(`Invalid Sentinel port for ${node.host}: ${node.port}`);
    }
  }
  return {
    connection: {
      sentinels,
      masterName,
      ...(process.env.QUEUEBIT_REDIS_USERNAME ? { username: process.env.QUEUEBIT_REDIS_USERNAME } : {}),
      ...(process.env.QUEUEBIT_REDIS_PASSWORD ? { password: process.env.QUEUEBIT_REDIS_PASSWORD } : {}),
      ...(process.env.QUEUEBIT_REDIS_SENTINEL_USERNAME
        ? { sentinelUsername: process.env.QUEUEBIT_REDIS_SENTINEL_USERNAME }
        : {}),
      ...(process.env.QUEUEBIT_REDIS_SENTINEL_PASSWORD
        ? { sentinelPassword: process.env.QUEUEBIT_REDIS_SENTINEL_PASSWORD }
        : {}),
      database: readIntegerEnv('QUEUEBIT_REDIS_DATABASE', 0, 0)
    }
  };
}

async function requestSentinelFailover(target) {
  const sentinel = target.connection.sentinels[0];
  const admin = createClient({
    socket: { host: sentinel.host, port: sentinel.port },
    ...(target.connection.sentinelUsername === undefined ? {} : { username: target.connection.sentinelUsername }),
    ...(target.connection.sentinelPassword === undefined ? {} : { password: target.connection.sentinelPassword })
  });
  await admin.connect();
  try {
    const before = await readSentinelMasterAddress(admin, target.connection.masterName);
    await waitFor(async () => {
      try {
        const reply = await admin.sendCommand(['SENTINEL', 'FAILOVER', target.connection.masterName]);
        return String(reply).toUpperCase() === 'OK';
      } catch {
        return false;
      }
    }, 20_000, 'Sentinel did not accept a failover request.');
    await waitFor(async () => {
      const after = await readSentinelMasterAddress(admin, target.connection.masterName);
      return after.host !== before.host || after.port !== before.port;
    }, 30_000, 'Sentinel did not publish a replacement primary.');
  } finally {
    try {
      await admin.quit();
    } catch {
      admin.disconnect();
    }
  }
}

async function readSentinelMasterAddress(admin, masterName) {
  const reply = await admin.sendCommand(['SENTINEL', 'GET-MASTER-ADDR-BY-NAME', masterName]);
  if (!Array.isArray(reply) || reply.length < 2) {
    throw new Error(`Sentinel did not return a primary address for ${masterName}.`);
  }
  return { host: String(reply[0]), port: Number.parseInt(String(reply[1]), 10) };
}

async function waitFor(predicate, timeoutMs, message) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() <= deadline) {
    if (await predicate()) return;
    await new Promise(resolve => setTimeout(resolve, 200));
  }
  assert.fail(message);
}

function readIntegerEnv(name, fallback, min) {
  const value = process.env[name];
  if (!value) return fallback;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed < min) {
    throw new Error(`${name} must be an integer >= ${min}.`);
  }
  return parsed;
}

async function cleanupNamespace(redis, namespace) {
  let cursor = '0';
  const match = `qb:{${namespace}}:*`;
  do {
    const reply = await redis.sendCommand(['SCAN', cursor, 'MATCH', match, 'COUNT', '100']);
    const nextCursor = Array.isArray(reply) ? String(reply[0]) : '0';
    const keys = Array.isArray(reply) && Array.isArray(reply[1]) ? reply[1].map(String) : [];
    if (keys.length > 0) await redis.sendCommand(['DEL', ...keys]);
    cursor = nextCursor;
  } while (cursor !== '0');
}
