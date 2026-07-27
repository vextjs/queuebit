import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import test from 'node:test';

const cliPath = resolve('dist/cli.js');

test('queuebit CLI renders help without requiring config', () => {
  const result = runCli(['--help']);
  assert.equal(result.status, 0);
  assert.match(result.stdout, /Queuebit CLI/);
  assert.match(result.stdout, /config validate/);
  assert.match(result.stdout, /health inspect/);
  assert.match(result.stdout, /run list/);
  assert.match(result.stdout, /queue inspect/);
  assert.match(result.stdout, /job inspect/);
  assert.match(result.stdout, /completion retry/);
});

test('queuebit CLI --version prints package version without help', () => {
  const result = runCli(['--version']);
  assert.equal(result.status, 0);
  assert.match(result.stdout.trim(), /^\d+\.\d+\.\d+/);
  assert.doesNotMatch(result.stdout, /Usage:/);
});

test('queuebit CLI validates TypeScript config and runtime through built-in loader', async () => {
  const fixture = await createCliFixture('ts-valid');
  try {
    const result = runCli([
      'config',
      'validate',
      '--config',
      fixture.configTs,
      '--runtime',
      fixture.runtimeTs,
      '--json'
    ]);
    assert.equal(result.status, 0, result.stderr);
    const envelope = JSON.parse(result.stdout);
    assert.equal(envelope.ok, true);
    assert.equal(envelope.data.config.loader, 'typescript-cjs');
    assert.deepEqual(envelope.data.config.queues, ['notification']);
    assert.deepEqual(envelope.data.validation.processors, ['send-receipt']);
  } finally {
    await fixture.cleanup();
  }
});

test('queuebit CLI validates precompiled mjs config and runtime', async () => {
  const fixture = await createCliFixture('mjs-valid');
  try {
    const result = runCli([
      'config',
      'validate',
      '--config',
      fixture.configMjs,
      '--runtime',
      fixture.runtimeMjs,
      '--json'
    ]);
    assert.equal(result.status, 0, result.stderr);
    const envelope = JSON.parse(result.stdout);
    assert.equal(envelope.ok, true);
    assert.equal(envelope.data.config.loader, 'esm');
    assert.deepEqual(envelope.data.validation.sources, ['paid-orders']);
  } finally {
    await fixture.cleanup();
  }
});

test('queuebit CLI returns stable JSON error for missing runtime handlers', async () => {
  const fixture = await createCliFixture('missing-handler');
  try {
    const result = runCli([
      'config',
      'validate',
      '--config',
      fixture.configTs,
      '--runtime',
      fixture.missingRuntimeTs,
      '--json'
    ]);
    assert.equal(result.status, 2);
    const envelope = JSON.parse(result.stdout);
    assert.equal(envelope.ok, false);
    assert.equal(envelope.error.code, 'QB_CONFIG_HANDLER_NOT_REGISTERED');
    assert.equal(envelope.error.operation, 'config.validate');
  } finally {
    await fixture.cleanup();
  }
});

test('queuebit CLI rejects scheduler commands with documented exit code and error', () => {
  const result = runCli(['scheduler', 'start', '--json']);
  assert.equal(result.status, 2);
  const envelope = JSON.parse(result.stdout);
  assert.equal(envelope.ok, false);
  assert.equal(envelope.error.code, 'QB_CLI_COMMAND_UNSUPPORTED');
  assert.equal(envelope.error.operation, 'scheduler.start');
});

test('queuebit CLI human validate output is readable and not JSON-only', async () => {
  const fixture = await createCliFixture('human-valid');
  try {
    const result = runCli([
      'config',
      'validate',
      '--config',
      fixture.configTs,
      '--runtime',
      fixture.runtimeTs
    ]);
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /message=Queuebit configuration is valid\./);
    assert.match(result.stdout, /config.namespace=cli:test/);
  } finally {
    await fixture.cleanup();
  }
});

function runCli(args) {
  return spawnSync(process.execPath, [cliPath, ...args], {
    cwd: resolve('.'),
    encoding: 'utf8',
    env: { ...process.env, NO_COLOR: '1' }
  });
}

async function createCliFixture(name) {
  const dir = await mkdtemp(join(resolve('tests'), `.cli-${name}-`));
  const configTs = join(dir, 'queuebit.config.ts');
  const runtimeTs = join(dir, 'queuebit.runtime.ts');
  const missingRuntimeTs = join(dir, 'queuebit.missing-runtime.ts');
  const configMjs = join(dir, 'queuebit.config.mjs');
  const runtimeMjs = join(dir, 'queuebit.runtime.mjs');

  await writeFile(configTs, createConfigSource(), 'utf8');
  await writeFile(runtimeTs, createRuntimeSource(), 'utf8');
  await writeFile(missingRuntimeTs, createMissingRuntimeSource(), 'utf8');
  await writeFile(configMjs, createConfigSource(), 'utf8');
  await writeFile(runtimeMjs, createRuntimeSource(), 'utf8');

  return {
    configTs,
    runtimeTs,
    missingRuntimeTs,
    configMjs,
    runtimeMjs,
    cleanup: () => rm(dir, { recursive: true, force: true })
  };
}

function createConfigSource() {
  return `
import { defineQueuebitConfig } from 'queuebit';

export default defineQueuebitConfig({
  namespace: 'cli:test',
  connection: {
    host: '127.0.0.1',
    port: 6379,
    database: 0,
    serverPolicy: { mode: 'warn' }
  },
  queues: { notification: {} },
  batchRuns: {
    'receipt-campaign': {
      queue: 'notification',
      source: 'paid-orders',
      mapper: 'receipt-jobs',
      completion: { run: { handler: 'record-run' } }
    }
  }
});
`;
}

function createRuntimeSource() {
  return `
import {
  defineQueuebitCompletionHandler,
  defineQueuebitMapper,
  defineQueuebitProcessor,
  defineQueuebitRuntime,
  defineQueuebitSource
} from 'queuebit';

export default defineQueuebitRuntime({
  sources: {
    'paid-orders': defineQueuebitSource({
      async freeze() {
        return { boundary: { upperId: 0 }, cursor: 0, totalRecords: 0 };
      },
      async load() {
        return { records: [], nextCursor: 0, exhausted: true };
      }
    })
  },
  mappers: {
    'receipt-jobs': defineQueuebitMapper(() => null)
  },
  processors: {
    'send-receipt': defineQueuebitProcessor(async () => ({ ok: true }))
  },
  completions: {
    'record-run': defineQueuebitCompletionHandler(() => undefined)
  }
});
`;
}

function createMissingRuntimeSource() {
  return `
import { defineQueuebitRuntime } from 'queuebit';

export default defineQueuebitRuntime({
  sources: {},
  mappers: {}
});
`;
}
