import assert from 'node:assert/strict';
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
  'real Redis Sentinel target routes Queuebit commands through discovered primary',
  target?.error ? { skip: String(target.error.message ?? target.error) } : {},
  async () => {
    const namespace = `redis_sentinel:${Date.now()}:${Math.random().toString(16).slice(2)}`;
    const config = defineQueuebitConfig({
      namespace,
      connection: {
        ...target.connection,
        serverPolicy: { mode: 'warn' }
      }
    });
    const cleanupConnection = createQueuebitRedisConnection(config, { name: `queuebit:${namespace}:cleanup` });
    const cleanupRedis = await cleanupConnection.connect();

    try {
      await cleanupNamespace(cleanupRedis, namespace);
      const client = await createQueuebitClient({ config });
      try {
        const job = await client.jobs.add('notification', { kind: 'sentinel-smoke' });
        const fetched = await client.jobs.get('notification', job.id);

        assert.equal(config.connection.mode, 'sentinel');
        assert.equal(fetched?.id, job.id);
        assert.deepEqual(fetched?.data, { kind: 'sentinel-smoke' });
      } finally {
        await client.close();
      }
    } finally {
      await cleanupNamespace(cleanupRedis, namespace);
      await cleanupConnection.close();
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
