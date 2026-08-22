import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import {
  QueuebitError,
  canonicalizeInput,
  createCanonicalDigest,
  defineQueuebitConfig
} from '../dist/index.js';

test('defineQueuebitConfig applies defaults without mutating input', () => {
  const input = {
    namespace: 'tenant:billing',
    queues: {
      notification: {
        backpressure: {
          highWatermarkJobs: 10,
          lowWatermarkJobs: 5
        }
      }
    }
  };
  const before = structuredClone(input);
  const config = defineQueuebitConfig(input);

  assert.deepEqual(input, before);
  assert.equal(config.connection.mode, 'direct');
  assert.equal(config.connection.host, '127.0.0.1');
  assert.equal(config.connection.serverPolicy.mode, 'warn');
  assert.equal(config.scheduler.mode, 'cooperative');
  assert.equal(config.namespace, 'tenant:billing');
  assert.deepEqual(config.retention.completedJobs, { ageMs: 86_400_000, maxCount: 100_000 });
  assert.deepEqual(config.retention.terminalRuns, { ageMs: 2_592_000_000, maxCount: 10_000 });
  assert.deepEqual(config.retention.completionEvents, { ageMs: 2_592_000_000, maxCount: 10_000 });
  assert.equal(config.deduplication.jobKeyTtlMs, 604_800_000);
  assert.equal(config.observability.metrics.prefix, 'queuebit_');
});

test('defineQueuebitConfig derives a stable namespace from the application package name', () => {
  const original = process.env.QUEUEBIT_NAMESPACE;
  delete process.env.QUEUEBIT_NAMESPACE;
  try {
    const config = defineQueuebitConfig({ queues: { notification: {} } });
    const digest = createHash('sha256').update('queuebit').digest('hex').slice(0, 12);

    assert.equal(config.namespace, `app:queuebit:${digest}`);
  } finally {
    if (original === undefined) delete process.env.QUEUEBIT_NAMESPACE;
    else process.env.QUEUEBIT_NAMESPACE = original;
  }
});

test('defineQueuebitConfig finds and normalizes the nearest scoped package name', () => {
  const originalDirectory = process.cwd();
  const originalNamespace = process.env.QUEUEBIT_NAMESPACE;
  const packageDirectory = mkdtempSync(join(tmpdir(), 'queuebit-namespace-'));
  const nestedDirectory = join(packageDirectory, 'worker');
  try {
    writeFileSync(join(packageDirectory, 'package.json'), '{"name":"@queuebit/demo.app"}');
    mkdirSync(nestedDirectory);
    process.chdir(packageDirectory);
    process.chdir(nestedDirectory);
    delete process.env.QUEUEBIT_NAMESPACE;

    const digest = createHash('sha256').update('@queuebit/demo.app').digest('hex').slice(0, 12);
    assert.equal(
      defineQueuebitConfig({}).namespace,
      `app:queuebit-demo-app:${digest}`
    );
  } finally {
    process.chdir(originalDirectory);
    if (originalNamespace === undefined) delete process.env.QUEUEBIT_NAMESPACE;
    else process.env.QUEUEBIT_NAMESPACE = originalNamespace;
    rmSync(packageDirectory, { recursive: true, force: true });
  }
});

test('defineQueuebitConfig prefers explicit namespace over QUEUEBIT_NAMESPACE', () => {
  const original = process.env.QUEUEBIT_NAMESPACE;
  process.env.QUEUEBIT_NAMESPACE = 'environment:queuebit';
  try {
    assert.equal(
      defineQueuebitConfig({ namespace: 'explicit:queuebit' }).namespace,
      'explicit:queuebit'
    );
    assert.equal(defineQueuebitConfig({}).namespace, 'environment:queuebit');
  } finally {
    if (original === undefined) delete process.env.QUEUEBIT_NAMESPACE;
    else process.env.QUEUEBIT_NAMESPACE = original;
  }
});

test('defineQueuebitConfig rejects an invalid QUEUEBIT_NAMESPACE', () => {
  const original = process.env.QUEUEBIT_NAMESPACE;
  process.env.QUEUEBIT_NAMESPACE = 'contains.dot';
  try {
    assert.throws(
      () => defineQueuebitConfig({}),
      (error) => error instanceof QueuebitError && error.code === 'QB_CONFIG_INVALID'
    );
  } finally {
    if (original === undefined) delete process.env.QUEUEBIT_NAMESPACE;
    else process.env.QUEUEBIT_NAMESPACE = original;
  }
});

test('defineQueuebitConfig accepts an independent completion retention window', () => {
  const config = defineQueuebitConfig({
    retention: {
      terminalRuns: { ageMs: 2_592_000_000, maxCount: 10_000 },
      completionEvents: { ageMs: 86_400_000, maxCount: 1_000 }
    }
  });

  assert.deepEqual(config.retention.terminalRuns, { ageMs: 2_592_000_000, maxCount: 10_000 });
  assert.deepEqual(config.retention.completionEvents, { ageMs: 86_400_000, maxCount: 1_000 });
});

test('defineQueuebitConfig normalizes Redis URL connection mode', () => {
  const config = defineQueuebitConfig({
    connection: {
      url: 'rediss://queuebit:secret@redis.example.internal:6380/2',
      serverPolicy: { mode: 'strict' }
    }
  });

  assert.equal(config.connection.mode, 'direct');
  assert.equal(config.connection.host, 'redis.example.internal');
  assert.equal(config.connection.port, 6380);
  assert.equal(config.connection.username, 'queuebit');
  assert.equal(config.connection.password, 'secret');
  assert.equal(config.connection.database, 2);
  assert.equal(config.connection.tls?.servername, 'redis.example.internal');
  assert.equal(config.connection.serverPolicy.mode, 'strict');
});

test('defineQueuebitConfig rejects invalid Redis URLs', () => {
  assert.throws(
    () =>
      defineQueuebitConfig({
        connection: { url: 'redis:///0' }
      }),
    (error) => error instanceof QueuebitError && error.code === 'QB_CONFIG_INVALID'
  );
  assert.throws(
    () =>
      defineQueuebitConfig({
        connection: { url: 'redis://127.0.0.1:0/0' }
      }),
    (error) => error instanceof QueuebitError && error.code === 'QB_CONFIG_INVALID'
  );
});

test('defineQueuebitConfig rejects unknown fields', () => {
  assert.throws(
    () => defineQueuebitConfig({ namespace: 'ok', redis: { host: '127.0.0.1' } }),
    (error) => error instanceof QueuebitError && error.code === 'QB_CONFIG_INVALID'
  );
});

test('defineQueuebitConfig rejects mixed Redis connection modes', () => {
  assert.throws(
    () =>
      defineQueuebitConfig({
        connection: {
          url: 'redis://127.0.0.1:6379/0',
          host: 'redis.example.internal'
        }
      }),
    (error) => error instanceof QueuebitError && error.code === 'QB_CONFIG_INVALID'
  );
  assert.throws(
    () =>
      defineQueuebitConfig({
        connection: {
          host: 'redis.example.internal',
          sentinels: [
            { host: '10.0.1.11', port: 26379 },
            { host: '10.0.1.12', port: 26379 }
          ],
          masterName: 'mymaster'
        }
      }),
    (error) => error instanceof QueuebitError && error.code === 'QB_CONFIG_INVALID'
  );
});

test('defineQueuebitConfig requires a complete Sentinel topology', () => {
  assert.throws(
    () =>
      defineQueuebitConfig({
        connection: {
          sentinels: [{ host: '10.0.1.11', port: 26379 }],
          masterName: 'mymaster'
        }
      }),
    (error) => error instanceof QueuebitError && error.code === 'QB_CONFIG_INVALID'
  );
  assert.throws(
    () =>
      defineQueuebitConfig({
        connection: {
          sentinels: [
            { host: '10.0.1.11', port: 26379 },
            { host: '10.0.1.12', port: 26379 }
          ]
        }
      }),
    (error) => error instanceof QueuebitError && error.code === 'QB_CONFIG_INVALID'
  );
});

test('defineQueuebitConfig accepts its normalized connection output', () => {
  const direct = defineQueuebitConfig({
    connection: { url: 'redis://127.0.0.1:6379/1' }
  });
  assert.deepEqual(defineQueuebitConfig(direct), direct);

  const sentinel = defineQueuebitConfig({
    connection: {
      sentinels: [
        { host: '10.0.1.11', port: 26379 },
        { host: '10.0.1.12', port: 26379 }
      ],
      masterName: 'mymaster'
    }
  });
  assert.deepEqual(defineQueuebitConfig(sentinel), sentinel);
});

test('defineQueuebitConfig rejects invalid watermark pairs', () => {
  assert.throws(
    () =>
      defineQueuebitConfig({
        queues: {
          notification: {
            backpressure: {
              highWatermarkJobs: 5,
              lowWatermarkJobs: 5
            }
          }
        }
      }),
    /lowWatermarkJobs/
  );
});

test('defineQueuebitConfig rejects unsafe retention, observability, and timing settings', () => {
  assert.throws(
    () =>
      defineQueuebitConfig({
        retention: { completedJobs: { ageMs: 10_000 } },
        deduplication: { jobKeyTtlMs: 9_999 }
      }),
    (error) => error instanceof QueuebitError && error.code === 'QB_CONFIG_INVALID'
  );
  assert.throws(
    () =>
      defineQueuebitConfig({
        retention: { completionEvents: { ageMs: 10_000 } },
        deduplication: { runKeyTtlMs: 9_999 }
      }),
    (error) => error instanceof QueuebitError && error.code === 'QB_CONFIG_INVALID'
  );
  assert.throws(
    () =>
      defineQueuebitConfig({
        observability: { metrics: { prefix: '1bad' } }
      }),
    (error) => error instanceof QueuebitError && error.code === 'QB_CONFIG_INVALID'
  );
  assert.throws(
    () =>
      defineQueuebitConfig({
        connection: {
          backgroundReconnect: { initialDelayMs: 1_000, maxDelayMs: 999 }
        }
      }),
    (error) => error instanceof QueuebitError && error.code === 'QB_CONFIG_INVALID'
  );
  assert.throws(
    () =>
      defineQueuebitConfig({
        workerDefaults: { leaseMs: 1_000, renewIntervalMs: 500 }
      }),
    (error) => error instanceof QueuebitError && error.code === 'QB_CONFIG_INVALID'
  );
});

test('canonicalizeInput sorts object keys and preserves array order', () => {
  assert.equal(
    canonicalizeInput({ z: 1, a: [{ b: true, a: false }] }),
    '{"a":[{"a":false,"b":true}],"z":1}'
  );
});

test('createCanonicalDigest is stable for equivalent objects', () => {
  const left = createCanonicalDigest({ tenant: 'acme', range: { to: 20, from: 10 } });
  const right = createCanonicalDigest({ range: { from: 10, to: 20 }, tenant: 'acme' });

  assert.equal(left.version, 'qbcj-v1');
  assert.equal(left.json, right.json);
  assert.equal(left.sha256, right.sha256);
  assert.match(left.sha256, /^[a-f0-9]{64}$/);
});

test('canonicalizeInput rejects ambiguous values', () => {
  assert.throws(
    () => canonicalizeInput({ id: undefined }),
    (error) =>
      error instanceof QueuebitError && error.code === 'QB_CANONICAL_INPUT_UNSUPPORTED'
  );
  assert.throws(
    () => canonicalizeInput({ createdAt: new Date('2026-07-23T00:00:00.000Z') }),
    (error) =>
      error instanceof QueuebitError && error.code === 'QB_CANONICAL_INPUT_UNSUPPORTED'
  );
});
