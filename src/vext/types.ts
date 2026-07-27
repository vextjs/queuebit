import type { VextPluginContext } from 'vextjs';
import type { QueuebitConfig } from '../config';
import type { QueuebitClient, QueuebitClientOptions, QueuebitLogger } from '../client';

export type QueuebitVextPluginContext = VextPluginContext;

export interface QueuebitVextAppExtensions {
  [key: string]: unknown;
  queuebit: QueuebitClient;
}

export type QueuebitVextConfigResolver =
  | QueuebitConfig
  | ((app: QueuebitVextPluginContext) => QueuebitConfig | Promise<QueuebitConfig>);

export type QueuebitVextLoggerResolver =
  | QueuebitLogger
  | ((app: QueuebitVextPluginContext) => QueuebitLogger | undefined);

export interface QueuebitVextPluginOptions {
  config: QueuebitVextConfigResolver;
  pluginName?: string;
  extensionName?: string;
  dependencies?: readonly string[];
  logger?: QueuebitVextLoggerResolver;
  clientOptions?: Omit<QueuebitClientOptions, 'config' | 'logger'>;
  onClient?: (
    client: QueuebitClient,
    app: QueuebitVextPluginContext
  ) => void | Promise<void>;
}
