import { definePlugin, type VextPlugin } from 'vextjs';
import { createQueuebitClient, type QueuebitClient, type QueuebitLogger } from '../client';
import type { QueuebitConfig } from '../config';
import { QueuebitError } from '../errors';
import type {
  QueuebitVextConfigResolver,
  QueuebitVextLoggerResolver,
  QueuebitVextPluginContext,
  QueuebitVextPluginOptions
} from './types';

const DEFAULT_PLUGIN_NAME = 'queuebit';
const DEFAULT_EXTENSION_NAME = 'queuebit';

export function createQueuebitVextPlugin(options: QueuebitVextPluginOptions): VextPlugin {
  assertPluginOptions(options);
  let client: QueuebitClient | undefined;

  return definePlugin({
    name: options.pluginName ?? DEFAULT_PLUGIN_NAME,
    ...(options.dependencies === undefined ? {} : { dependencies: [...options.dependencies] }),
    async setup(app) {
      if (client !== undefined) {
        throw new QueuebitError({
          code: 'QB_VEXT_PLUGIN_INVALID',
          message: 'Queuebit vext plugin setup was called more than once without closing.'
        });
      }

      const extensionName = options.extensionName ?? DEFAULT_EXTENSION_NAME;
      try {
        const config = await resolveConfig(options.config, app);
        client = await createQueuebitClient({
          ...options.clientOptions,
          config,
          logger: resolveLogger(options.logger, app) ?? adaptVextLogger(app.logger)
        });
        app.extend(extensionName, client);
        await options.onClient?.(client, app);
      } catch (error) {
        const created = client;
        client = undefined;
        if (created !== undefined) await created.close().catch(() => undefined);
        throw error;
      }
    },
    async onClose() {
      const closing = client;
      client = undefined;
      if (closing !== undefined) await closing.close();
    }
  });
}

function assertPluginOptions(options: QueuebitVextPluginOptions) {
  if (options === null || typeof options !== 'object') {
    throw new QueuebitError({
      code: 'QB_VEXT_PLUGIN_INVALID',
      message: 'Queuebit vext plugin options must be an object.'
    });
  }
  if (options.config === undefined) {
    throw new QueuebitError({
      code: 'QB_VEXT_PLUGIN_INVALID',
      message: 'Queuebit vext plugin requires a config value or resolver.'
    });
  }
  assertNonEmptyString(options.pluginName, 'pluginName');
  assertNonEmptyString(options.extensionName, 'extensionName');
}

function assertNonEmptyString(value: string | undefined, field: string) {
  if (value !== undefined && value.trim() === '') {
    throw new QueuebitError({
      code: 'QB_VEXT_PLUGIN_INVALID',
      message: `Queuebit vext plugin ${field} must not be empty.`,
      details: { field }
    });
  }
}

async function resolveConfig(
  config: QueuebitVextConfigResolver,
  app: QueuebitVextPluginContext
): Promise<QueuebitConfig> {
  return typeof config === 'function' ? config(app) : config;
}

function resolveLogger(
  logger: QueuebitVextLoggerResolver | undefined,
  app: QueuebitVextPluginContext
): QueuebitLogger | undefined {
  return typeof logger === 'function' ? logger(app) : logger;
}

function adaptVextLogger(logger: QueuebitVextPluginContext['logger']): QueuebitLogger {
  return logger as QueuebitLogger;
}
