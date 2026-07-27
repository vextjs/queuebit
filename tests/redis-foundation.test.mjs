import assert from 'node:assert/strict';
import test from 'node:test';
import {
  QueuebitError,
  QueuebitScriptRegistry,
  assertRedisPreflightReady,
  createQueuebitKeyBuilder,
  createQueuebitRedisClientOptions,
  createQueuebitRedisConnection,
  createQueuebitRedisSentinelOptions,
  createReconnectStrategy,
  defineQueuebitConfig,
  runRedisPreflight
} from '../dist/index.js';

const healthyInfo = {
  server: [
    '# Server',
    'redis_version:7.2.4',
    'cluster_enabled:0'
  ].join('\n'),
  persistence: [
    '# Persistence',
    'aof_enabled:0',
    'rdb_last_bgsave_status:ok',
    'aof_last_bgrewrite_status:ok'
  ].join('\n'),
  replication: [
    '# Replication',
    'role:master',
    'connected_slaves:1'
  ].join('\n')
};

function createFakePreflightClient(overrides = {}) {
  const info = { ...healthyInfo, ...overrides.info };
  const config = {
    'maxmemory-policy': 'noeviction',
    save: '3600 1',
    ...overrides.config
  };
  return {
    async info(section) {
      return info[section] ?? '';
    },
    async configGet(key) {
      return { [key]: config[key] };
    },
    async role() {
      return overrides.role ?? ['master', 0, []];
    }
  };
}

test('createQueuebitRedisClientOptions maps direct TLS connection without undefined credentials', () => {
  const config = defineQueuebitConfig({
    connection: {
      host: 'redis.example.internal',
      port: 6380,
      username: 'queuebit',
      password: 'secret',
      database: 3,
      tls: { servername: 'redis.example.internal', rejectUnauthorized: true },
      backgroundReconnect: { jitter: 'none' }
    }
  });
  const options = createQueuebitRedisClientOptions(config, { name: 'queuebit-test' });

  assert.equal(options.username, 'queuebit');
  assert.equal(options.password, 'secret');
  assert.equal(options.database, 3);
  assert.equal(options.name, 'queuebit-test');
  assert.equal(options.disableOfflineQueue, true);
  assert.equal(options.socket?.tls, true);
  assert.equal(options.socket?.host, 'redis.example.internal');
  assert.equal(options.socket?.port, 6380);
  assert.equal(options.socket?.servername, 'redis.example.internal');
});

test('createQueuebitRedisSentinelOptions maps docs-first Sentinel configuration', () => {
  const config = defineQueuebitConfig({
    connection: {
      sentinels: [
        { host: '10.0.1.11', port: 26379 },
        { host: '10.0.1.12', port: 26379 },
        { host: '10.0.1.13', port: 26379 }
      ],
      masterName: 'mymaster',
      username: 'queuebit',
      password: 'secret',
      sentinelUsername: 'sentinel-user',
      sentinelPassword: 'sentinel-secret',
      database: 4,
      tls: { servername: 'redis.service.internal', rejectUnauthorized: true },
      backgroundReconnect: { jitter: 'none', logThrottleMs: 15_000 }
    }
  });
  const options = createQueuebitRedisSentinelOptions(config, { name: 'queuebit-sentinel-test' });

  assert.equal(config.connection.mode, 'sentinel');
  assert.equal(options.name, 'mymaster');
  assert.deepEqual(options.sentinelRootNodes, [
    { host: '10.0.1.11', port: 26379 },
    { host: '10.0.1.12', port: 26379 },
    { host: '10.0.1.13', port: 26379 }
  ]);
  assert.equal(options.nodeClientOptions?.username, 'queuebit');
  assert.equal(options.nodeClientOptions?.password, 'secret');
  assert.equal(options.nodeClientOptions?.database, 4);
  assert.equal(options.nodeClientOptions?.disableOfflineQueue, true);
  assert.equal(options.nodeClientOptions?.commandsQueueMaxLength, config.limits.maxBulkJobs);
  assert.equal(options.nodeClientOptions?.socket?.tls, true);
  assert.equal(options.nodeClientOptions?.socket?.servername, 'redis.service.internal');
  assert.equal(options.sentinelClientOptions?.username, 'sentinel-user');
  assert.equal(options.sentinelClientOptions?.password, 'sentinel-secret');
  assert.equal(options.sentinelClientOptions?.disableOfflineQueue, true);
  assert.equal(options.masterPoolSize, 1);
  assert.equal(options.replicaPoolSize, 0);
  assert.equal(options.scanInterval, 15_000);
});

test('createQueuebitRedisConnection adapts Sentinel sendCommand and close lifecycle', async () => {
  const config = defineQueuebitConfig({
    connection: {
      sentinels: [
        { host: '10.0.1.11', port: 26379 },
        { host: '10.0.1.12', port: 26379 }
      ],
      masterName: 'mymaster'
    }
  });
  const calls = [];
  const rawSentinel = {
    isOpen: false,
    isReady: false,
    async connect() {
      this.isOpen = true;
      this.isReady = true;
    },
    async close() {
      this.isOpen = false;
      this.isReady = false;
    },
    async sendCommand(isReadonly, command) {
      calls.push({ isReadonly, command });
      return command[0] === 'INFO' ? 'redis_version:7.2.4' : 'OK';
    }
  };
  const connection = createQueuebitRedisConnection(config, {
    sentinelFactory(options) {
      assert.equal(options.name, 'mymaster');
      return rawSentinel;
    }
  });

  assert.equal(connection.isOpen, false);
  const client = await connection.connect();
  assert.equal(connection.isOpen, true);
  assert.equal(client.isReady, true);
  assert.equal(await client.sendCommand(['PING']), 'OK');
  assert.equal(await client.info('server'), 'redis_version:7.2.4');
  assert.deepEqual(calls, [
    { isReadonly: false, command: ['PING'] },
    { isReadonly: true, command: ['INFO', 'server'] }
  ]);
  await connection.close();
  assert.equal(connection.isOpen, false);
});

test('createReconnectStrategy respects retry limit and deterministic no-jitter delays', () => {
  const config = defineQueuebitConfig({
    connection: {
      requestRetryLimit: 2,
      backgroundReconnect: {
        initialDelayMs: 100,
        factor: 2,
        maxDelayMs: 500,
        jitter: 'none'
      }
    }
  });
  const reconnect = createReconnectStrategy(config.connection);

  assert.equal(reconnect(1, new Error('first')), 100);
  assert.equal(reconnect(2, new Error('second')), 200);
  assert.equal(reconnect(3, new Error('third')), false);
});

test('createQueuebitKeyBuilder validates and builds the v0.1 namespace shape', () => {
  const keys = createQueuebitKeyBuilder('prod:billing');
  const digest = 'a'.repeat(64);

  assert.equal(keys.prefix, 'qb:{prod:billing}');
  assert.equal(keys.queueWaiting('notification'), 'qb:{prod:billing}:q:notification:waiting');
  assert.equal(keys.jobKey(digest), `qb:{prod:billing}:job-key:${digest}`);
  assert.equal(keys.runsTerminalDetails(), 'qb:{prod:billing}:runs:terminal-details');
  assert.equal(keys.completionsDetails(), 'qb:{prod:billing}:completions:details');
  assert.equal(keys.batch('run_1', 2), 'qb:{prod:billing}:run:run_1:batch:2');
  assert.equal(
    keys.roleMember('worker', 'notification', 'worker-a'),
    'qb:{prod:billing}:roles:worker:notification:member:worker-a'
  );
  assert.throws(
    () => keys.jobKey('not-a-digest'),
    (error) => error instanceof QueuebitError && error.code === 'QB_REDIS_KEY_INVALID'
  );
});

test('runRedisPreflight accepts a healthy single-primary Redis policy', async () => {
  const result = await runRedisPreflight(createFakePreflightClient(), 'strict');

  assert.equal(result.ready, true);
  assert.equal(result.status, 'ready');
  assert.deepEqual(result.issues, []);
  assert.doesNotThrow(() => assertRedisPreflightReady(result));
});

test('runRedisPreflight rejects Redis Cluster and unsafe persistence in strict mode', async () => {
  const result = await runRedisPreflight(
    createFakePreflightClient({
      info: {
        server: 'redis_version:7.2.4\ncluster_enabled:1',
        persistence: 'aof_enabled:0\nrdb_last_bgsave_status:ok\naof_last_bgrewrite_status:ok'
      },
      config: { save: '', 'maxmemory-policy': 'allkeys-lru' }
    }),
    'strict'
  );

  assert.equal(result.ready, false);
  assert.equal(result.status, 'not_ready');
  assert.deepEqual(
    result.issues.map(issue => issue.code).sort(),
    [
      'redis-cluster-unsupported',
      'redis-eviction-policy-unsafe',
      'redis-persistence-disabled'
    ]
  );
  assert.throws(
    () => assertRedisPreflightReady(result),
    (error) => error instanceof QueuebitError && error.code === 'QB_REDIS_CLUSTER_UNSUPPORTED'
  );
});

test('runRedisPreflight keeps warn mode degraded but usable for policy warnings', async () => {
  const result = await runRedisPreflight(
    createFakePreflightClient({ config: { 'maxmemory-policy': 'volatile-lru' } }),
    'warn'
  );

  assert.equal(result.ready, true);
  assert.equal(result.status, 'degraded');
  assert.equal(result.issues[0].code, 'redis-eviction-policy-unsafe');
  assert.equal(result.issues[0].severity, 'warning');
});

test('runRedisPreflight still blocks Redis Cluster in warn mode', async () => {
  const result = await runRedisPreflight(
    createFakePreflightClient({
      info: { server: 'redis_version:7.2.4\ncluster_enabled:1' }
    }),
    'warn'
  );

  assert.equal(result.ready, false);
  assert.equal(result.status, 'not_ready');
  assert.equal(result.issues[0].code, 'redis-cluster-unsupported');
  assert.equal(result.issues[0].severity, 'error');
});

test('QueuebitScriptRegistry computes stable SHA1 and rejects duplicates', () => {
  const registry = new QueuebitScriptRegistry();
  const script = registry.register({
    name: 'jobs:add-bulk',
    version: 'v1',
    numberOfKeys: 3,
    source: 'return redis.call("PING")'
  });

  assert.equal(script.sha1, 'dc9acaa27306c368e1f1eb390e9b0dc9ec7314ee');
  assert.equal(registry.get('jobs:add-bulk', 'v1')?.sha1, script.sha1);
  assert.throws(
    () => registry.register({
      name: 'jobs:add-bulk',
      version: 'v1',
      numberOfKeys: 3,
      source: 'return redis.call("PING")'
    }),
    (error) => error instanceof QueuebitError && error.code === 'QB_REDIS_SCRIPT_INVALID'
  );
});
