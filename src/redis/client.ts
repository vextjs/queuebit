import {
  createClient,
  createSentinel,
  type RedisClientOptions,
  type RedisClientType,
  type RedisSentinelOptions,
  type RedisSentinelType
} from '@redis/client';
import type { QueuebitConfig, QueuebitNormalizedConnectionConfig } from '../config';
import { QueuebitError } from '../errors';

export interface QueuebitRedisClient {
  readonly isOpen: boolean;
  readonly isReady: boolean;
  sendCommand(command: string[]): Promise<unknown>;
  info(section?: string): Promise<string>;
  configGet(parameter: string): Promise<unknown>;
  role(): Promise<unknown>;
}

type QueuebitRedisRawClient = RedisClientType | RedisSentinelType;

export interface QueuebitRedisConnection {
  readonly client: QueuebitRedisClient;
  readonly isOpen: boolean;
  readonly isReady: boolean;
  connect(): Promise<QueuebitRedisClient>;
  close(): Promise<void>;
}

export interface QueuebitRedisClientFactoryOptions {
  name?: string;
  clientFactory?: (options: RedisClientOptions) => QueuebitRedisRawClient;
  sentinelFactory?: (options: RedisSentinelOptions) => QueuebitRedisRawClient;
}

export function createQueuebitRedisClientOptions(
  config: QueuebitConfig,
  options: QueuebitRedisClientFactoryOptions = {}
): RedisClientOptions {
  const connection = config.connection;
  if (connection.mode !== 'direct') {
    throw new QueuebitError({
      code: 'QB_CONFIG_INVALID',
      message: 'Direct Redis client options require connection mode "direct".',
      details: { mode: connection.mode }
    });
  }
  const redisOptions: RedisClientOptions = {
    socket: createDirectSocketOptions(connection),
    database: connection.database,
    disableOfflineQueue: true,
    commandsQueueMaxLength: config.limits.maxBulkJobs
  };

  if (connection.username !== undefined) redisOptions.username = connection.username;
  if (connection.password !== undefined) redisOptions.password = connection.password;
  if (options.name !== undefined) redisOptions.name = options.name;

  return redisOptions;
}

export function createQueuebitRedisSentinelOptions(
  config: QueuebitConfig,
  options: QueuebitRedisClientFactoryOptions = {}
): RedisSentinelOptions {
  const connection = config.connection;
  if (connection.mode !== 'sentinel' || connection.sentinels === undefined || connection.masterName === undefined) {
    throw new QueuebitError({
      code: 'QB_CONFIG_INVALID',
      message: 'Sentinel Redis client options require connection.sentinels and connection.masterName.',
      details: { mode: connection.mode }
    });
  }

  const nodeClientOptions: NonNullable<RedisSentinelOptions['nodeClientOptions']> = {
    socket: createDiscoveredSocketOptions(connection),
    database: connection.database,
    disableOfflineQueue: true,
    commandsQueueMaxLength: config.limits.maxBulkJobs
  };
  if (connection.username !== undefined) nodeClientOptions.username = connection.username;
  if (connection.password !== undefined) nodeClientOptions.password = connection.password;
  if (options.name !== undefined) nodeClientOptions.name = options.name;

  const sentinelClientOptions: NonNullable<RedisSentinelOptions['sentinelClientOptions']> = {
    socket: createDiscoveredSocketOptions(connection),
    disableOfflineQueue: true,
    commandsQueueMaxLength: config.limits.maxBulkJobs
  };
  if (connection.sentinelUsername !== undefined) {
    sentinelClientOptions.username = connection.sentinelUsername;
  }
  if (connection.sentinelPassword !== undefined) {
    sentinelClientOptions.password = connection.sentinelPassword;
  }

  return {
    name: connection.masterName,
    sentinelRootNodes: connection.sentinels.map(node => ({ ...node })),
    nodeClientOptions,
    sentinelClientOptions,
    masterPoolSize: 1,
    replicaPoolSize: 0,
    scanInterval: connection.backgroundReconnect.logThrottleMs
  };
}

function createDirectSocketOptions(
  connection: QueuebitNormalizedConnectionConfig
): NonNullable<RedisClientOptions['socket']> {
  const common = {
    host: connection.host,
    port: connection.port,
    connectTimeout: connection.connectTimeoutMs,
    socketTimeout: connection.commandTimeoutMs,
    reconnectStrategy: createReconnectStrategy(connection)
  };

  if (!connection.tls) {
    return { ...common, tls: false };
  }

  const socket: NonNullable<RedisClientOptions['socket']> = {
    ...common,
    tls: true
  };
  if (connection.tls.servername !== undefined) socket.servername = connection.tls.servername;
  if (connection.tls.rejectUnauthorized !== undefined) {
    socket.rejectUnauthorized = connection.tls.rejectUnauthorized;
  }
  return socket;
}

function createDiscoveredSocketOptions(
  connection: QueuebitNormalizedConnectionConfig
): NonNullable<RedisClientOptions['socket']> {
  const common = {
    connectTimeout: connection.connectTimeoutMs,
    socketTimeout: connection.commandTimeoutMs,
    reconnectStrategy: createReconnectStrategy(connection)
  };

  if (!connection.tls) return { ...common, tls: false };

  const socket: NonNullable<RedisClientOptions['socket']> = {
    ...common,
    tls: true
  };
  if (connection.tls.servername !== undefined) socket.servername = connection.tls.servername;
  if (connection.tls.rejectUnauthorized !== undefined) {
    socket.rejectUnauthorized = connection.tls.rejectUnauthorized;
  }
  return socket;
}

export function createReconnectStrategy(connection: QueuebitNormalizedConnectionConfig) {
  return (retries: number, cause: Error): false | number => {
    if (connection.requestRetryLimit > 0 && retries > connection.requestRetryLimit) {
      return false;
    }
    const baseDelay = Math.min(
      connection.backgroundReconnect.maxDelayMs,
      connection.backgroundReconnect.initialDelayMs
        * (connection.backgroundReconnect.factor ** Math.max(0, retries - 1))
    );
    if (connection.backgroundReconnect.jitter === 'none') return baseDelay;
    const jittered = Math.floor(Math.random() * baseDelay);
    return Math.max(1, jittered);
  };
}

export function createQueuebitRedisConnection(
  config: QueuebitConfig,
  options: QueuebitRedisClientFactoryOptions = {}
): QueuebitRedisConnection {
  const rawClient = config.connection.mode === 'sentinel'
    ? (options.sentinelFactory ?? createSentinel)(createQueuebitRedisSentinelOptions(config, options))
    : (options.clientFactory ?? createClient)(createQueuebitRedisClientOptions(config, options));
  const client = config.connection.mode === 'sentinel'
    ? createSentinelCommandClient(rawClient as RedisSentinelType)
    : rawClient as QueuebitRedisClient;

  return {
    client,
    get isOpen() {
      return rawClient.isOpen;
    },
    get isReady() {
      return rawClient.isReady;
    },
    async connect() {
      try {
        await rawClient.connect();
        return client;
      } catch (cause) {
        throw new QueuebitError({
          code: 'QB_REDIS_CONNECTION_FAILED',
          message: 'Queuebit failed to connect to Redis.',
          details: { cause }
        });
      }
    },
    async close() {
      if (!rawClient.isOpen) return;
      await closeRawRedisClient(rawClient);
    }
  };
}

function createSentinelCommandClient(client: RedisSentinelType): QueuebitRedisClient {
  return {
    get isOpen() {
      return client.isOpen;
    },
    get isReady() {
      return client.isReady;
    },
    sendCommand(command: string[]) {
      return client.sendCommand(false, command);
    },
    info(section?: string) {
      return client.sendCommand(true, section === undefined ? ['INFO'] : ['INFO', section]) as Promise<string>;
    },
    configGet(parameter: string) {
      return client.sendCommand(true, ['CONFIG', 'GET', parameter]);
    },
    role() {
      return client.sendCommand(true, ['ROLE']);
    }
  };
}

async function closeRawRedisClient(client: QueuebitRedisRawClient) {
  const maybeDirect = client as RedisClientType;
  if (typeof maybeDirect.quit === 'function') {
    await maybeDirect.quit();
    return;
  }
  await (client as RedisSentinelType).close();
}
