import assert from 'node:assert/strict';
import test from 'node:test';
import { defineQueuebitConfig } from '../dist/index.js';
import { createQueuebitVextPlugin } from '../dist/vext/index.js';

class FakeVextRedisClient {
  async sendCommand(command) {
    throw new Error(`Unexpected Redis command ${command[0]}`);
  }
}

test('queuebit/vext creates one client, extends the vext app, and closes on lifecycle shutdown', async () => {
  const config = defineQueuebitConfig({ namespace: 'vext:test' });
  const app = createFakeVextApp();
  let observedClient;
  const plugin = createQueuebitVextPlugin({
    config,
    clientOptions: {
      redis: new FakeVextRedisClient(),
      preflight: false,
      now: () => new Date('2026-07-23T12:00:00.000Z')
    },
    onClient(client) {
      observedClient = client;
    }
  });

  assert.equal(plugin.name, 'queuebit');
  await plugin.setup(app);

  assert.equal(app.extensions.queuebit, observedClient);
  assert.equal(observedClient.health.snapshot().status, 'ready');

  await plugin.onClose?.(app);
  await plugin.onClose?.(app);
  assert.equal(observedClient.health.snapshot().status, 'draining');
});

test('queuebit/vext supports custom extension names, dependencies, config resolver, and logger resolver', async () => {
  const app = createFakeVextApp();
  const config = defineQueuebitConfig({ namespace: 'vext:custom' });
  const customLogger = { info() {} };
  const plugin = createQueuebitVextPlugin({
    pluginName: 'queuebit-billing',
    extensionName: 'billingQueuebit',
    dependencies: ['database'],
    config(receivedApp) {
      assert.equal(receivedApp, app);
      return config;
    },
    logger(receivedApp) {
      assert.equal(receivedApp, app);
      return customLogger;
    },
    clientOptions: {
      redis: new FakeVextRedisClient(),
      preflight: false
    }
  });

  assert.equal(plugin.name, 'queuebit-billing');
  assert.deepEqual(plugin.dependencies, ['database']);

  await plugin.setup(app);
  assert.ok(app.extensions.billingQueuebit);
  await plugin.onClose?.(app);
});

test('queuebit/vext rejects invalid options and duplicate setup', async () => {
  assert.throws(
    () => createQueuebitVextPlugin({ config: undefined }),
    error => isQueuebitPluginError(error)
  );
  assert.throws(
    () => createQueuebitVextPlugin({
      config: defineQueuebitConfig({ namespace: 'vext:invalid' }),
      extensionName: '   '
    }),
    error => isQueuebitPluginError(error)
  );

  const plugin = createQueuebitVextPlugin({
    config: defineQueuebitConfig({ namespace: 'vext:duplicate' }),
    clientOptions: {
      redis: new FakeVextRedisClient(),
      preflight: false
    }
  });
  const app = createFakeVextApp();
  await plugin.setup(app);
  await assert.rejects(
    () => plugin.setup(app),
    error => isQueuebitPluginError(error)
  );
  await plugin.onClose?.(app);
});

function isQueuebitPluginError(error) {
  return error?.name === 'QueuebitError' && error.code === 'QB_VEXT_PLUGIN_INVALID';
}

function createFakeVextApp() {
  return {
    config: {},
    services: {},
    hooks: { on() {} },
    adapter: {},
    cache: undefined,
    fetch: undefined,
    logger: {
      info() {},
      warn() {},
      error() {},
      debug() {},
      child() {
        return this;
      }
    },
    extensions: {},
    extend(name, value) {
      this.extensions[name] = value;
    },
    onClose() {},
    onReady() {},
    use() {},
    throw(error) {
      throw error;
    },
    setValidator() {},
    getValidator() {},
    setThrow() {},
    setLogger() {},
    setRateLimiter() {},
    setRequestIdGenerator() {}
  };
}
