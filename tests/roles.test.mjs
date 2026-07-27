import assert from 'node:assert/strict';
import test from 'node:test';
import {
  QueuebitError,
  createQueuebitRolesApi,
  defineQueuebitConfig
} from '../dist/index.js';

class FakeRedisRolesClient {
  commands = [];
  hashes = new Map();
  zsets = new Map();

  async sendCommand(command) {
    this.commands.push(command);
    const [name] = command;
    if (name === 'HSET') return this.hset(command);
    if (name === 'HGETALL') return { ...(this.hashes.get(command[1]) ?? {}) };
    if (name === 'ZADD') return this.zadd(command[1], command[2], command[3]);
    if (name === 'ZRANGEBYSCORE') return this.zrangeByScore(command);
    if (name === 'DEL') return this.hashes.delete(command[1]) ? 1 : 0;
    if (name === 'ZREM') return this.zrem(command[1], command[2]);
    throw new Error(`Unexpected command ${name}`);
  }

  hset(command) {
    const [, key, ...pairs] = command;
    const record = this.hashes.get(key) ?? {};
    for (let index = 0; index < pairs.length; index += 2) {
      record[pairs[index]] = pairs[index + 1];
    }
    this.hashes.set(key, record);
    return pairs.length / 2;
  }

  zadd(key, score, member) {
    const zset = this.zsets.get(key) ?? new Map();
    zset.set(member, Number(score));
    this.zsets.set(key, zset);
    return 1;
  }

  zrem(key, member) {
    const existed = this.zsets.get(key)?.delete(member) === true;
    return existed ? 1 : 0;
  }

  zrangeByScore(command) {
    const [, key, min, max, , , countRaw] = command;
    const minValue = min === '-inf' ? Number.NEGATIVE_INFINITY : Number(min.slice(1));
    const maxValue = max === '+inf' ? Number.POSITIVE_INFINITY : Number(max);
    const limit = Number(countRaw);
    const zset = this.zsets.get(key) ?? new Map();
    return [...zset.entries()]
      .filter(([, score]) => score > minValue && score <= maxValue)
      .sort((left, right) => left[1] - right[1])
      .slice(0, limit)
      .map(([member]) => member);
  }
}

test('roles API tracks active heartbeats, drain requests, stale records, and unregister', async () => {
  let currentNow = new Date('2026-07-23T12:00:00.000Z');
  const config = defineQueuebitConfig({ namespace: 'roles:test' });
  const redis = new FakeRedisRolesClient();
  const roles = createQueuebitRolesApi({
    config,
    redis,
    now: () => currentNow
  });

  const first = await roles.heartbeat({
    role: 'worker',
    domain: 'notification',
    identity: 'worker-a',
    status: 'running',
    heartbeatTtlMs: 1_000,
    startedAt: currentNow.toISOString(),
    metadata: { activeJobs: 2, concurrency: 4 }
  });
  assert.equal(first.drainRequested, false);
  assert.equal(first.snapshot.stale, false);
  assert.deepEqual(first.snapshot.metadata, { activeJobs: 2, concurrency: 4 });

  const active = await roles.list({ role: 'worker', domain: 'notification' });
  assert.deepEqual(active.items.map(role => role.identity), ['worker-a']);

  const requested = await roles.requestDrain({
    role: 'worker',
    domain: 'notification',
    identity: 'worker-a',
    reason: 'deploy'
  });
  assert.equal(requested.drainRequestedAt, currentNow.toISOString());
  assert.equal(requested.drainReason, 'deploy');

  const second = await roles.heartbeat({
    role: 'worker',
    domain: 'notification',
    identity: 'worker-a',
    status: 'running',
    heartbeatTtlMs: 1_000
  });
  assert.equal(second.drainRequested, true);

  currentNow = new Date(currentNow.getTime() + 1_001);
  assert.equal((await roles.list({ role: 'worker', domain: 'notification' })).items.length, 0);
  const stale = await roles.list({ role: 'worker', domain: 'notification', includeStale: true });
  assert.equal(stale.items[0].identity, 'worker-a');
  assert.equal(stale.items[0].stale, true);

  await roles.unregister({ role: 'worker', domain: 'notification', identity: 'worker-a' });
  assert.equal((await roles.get({ role: 'worker', domain: 'notification', identity: 'worker-a' })), null);
});

test('roles API rejects drain requests for unknown identities', async () => {
  const config = defineQueuebitConfig({ namespace: 'roles:test' });
  const roles = createQueuebitRolesApi({
    config,
    redis: new FakeRedisRolesClient(),
    now: () => new Date('2026-07-23T12:00:00.000Z')
  });

  await assert.rejects(
    () => roles.requestDrain({ role: 'coordinator', identity: 'missing' }),
    error => error instanceof QueuebitError && error.code === 'QB_ROLE_NOT_FOUND'
  );
});

test('config rejects heartbeat TTL that cannot cover the heartbeat interval', () => {
  assert.throws(
    () => defineQueuebitConfig({
      workerDefaults: {
        heartbeatIntervalMs: 5_000,
        heartbeatTtlMs: 5_000
      }
    }),
    error => error instanceof QueuebitError && error.code === 'QB_CONFIG_INVALID'
  );
});
