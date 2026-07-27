import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import { QueuebitError } from './errors';

export type ServerPolicyMode = 'warn' | 'strict';
export type SchedulerMode = 'cooperative';
export type BackoffJitter = 'none' | 'full';
export type QueuebitBatchDispatchMode = 'sequential' | 'paced';
export type QueuebitConnectionMode = 'direct' | 'sentinel';

export interface QueuebitTlsConfig {
  servername?: string;
  rejectUnauthorized?: boolean;
}

export interface QueuebitBackgroundReconnectConfig {
  initialDelayMs?: number;
  maxDelayMs?: number;
  factor?: number;
  jitter?: BackoffJitter;
  logThrottleMs?: number;
}

export interface QueuebitSentinelNodeConfig {
  host: string;
  port: number;
}

export interface QueuebitConnectionConfig {
  mode?: QueuebitConnectionMode;
  url?: string;
  host?: string;
  port?: number;
  username?: string;
  password?: string;
  database?: number;
  sentinels?: QueuebitSentinelNodeConfig[];
  masterName?: string;
  sentinelUsername?: string;
  sentinelPassword?: string;
  tls?: QueuebitTlsConfig;
  connectTimeoutMs?: number;
  commandTimeoutMs?: number;
  requestRetryLimit?: number;
  backgroundReconnect?: QueuebitBackgroundReconnectConfig;
  serverPolicy?: { mode?: ServerPolicyMode };
}

export interface QueuebitWorkerDefaults {
  concurrency?: number;
  leaseMs?: number;
  renewIntervalMs?: number;
  pollIntervalMs?: number;
  drainTimeoutMs?: number;
  maxStalledRecoveries?: number;
  heartbeatIntervalMs?: number;
  heartbeatTtlMs?: number;
}

export interface QueuebitSchedulerConfig {
  mode?: SchedulerMode;
  domain?: string;
  leaseMs?: number;
  renewIntervalMs?: number;
  pollIntervalMs?: number;
  promotionBatchSize?: number;
  drainTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  heartbeatTtlMs?: number;
}

export interface QueuebitBackpressureConfig {
  highWatermarkJobs?: number;
  lowWatermarkJobs?: number;
  highWatermarkBytes?: number;
  lowWatermarkBytes?: number;
}

export interface QueuebitQueueConfig {
  backpressure?: QueuebitBackpressureConfig;
}

export interface QueuebitBatchDispatchConfig {
  mode?: QueuebitBatchDispatchMode;
  intervalMs?: number;
  maxInFlightBatches?: number;
}

export interface QueuebitCompletionHandlerConfig {
  handler: string;
  attempts?: number;
  backoff?: {
    type: 'fixed' | 'exponential';
    delayMs: number;
    maxDelayMs?: number;
  };
}

export interface QueuebitBatchRunCompletionConfig {
  batch?: QueuebitCompletionHandlerConfig;
  run?: QueuebitCompletionHandlerConfig;
}

export interface QueuebitBatchRunConfig {
  version?: number;
  queue: string;
  source: string;
  mapper: string;
  inputSchema?: Record<string, unknown>;
  pageSize?: number;
  dispatch?: QueuebitBatchDispatchConfig;
  completion?: QueuebitBatchRunCompletionConfig;
}

export interface QueuebitNormalizedBatchRunConfig {
  version: number;
  queue: string;
  source: string;
  mapper: string;
  inputSchema?: Record<string, unknown>;
  pageSize: number;
  dispatch: Required<QueuebitBatchDispatchConfig>;
  completion: QueuebitBatchRunCompletionConfig;
}

export interface QueuebitLimitsConfig {
  maxRunInputBytes?: number;
  maxJobDataBytes?: number;
  maxJobResultBytes?: number;
  maxPageBytes?: number;
  maxBulkJobs?: number;
  maxBulkBytes?: number;
}

export interface QueuebitRetentionWindowConfig {
  ageMs?: number;
  maxCount?: number;
}

export interface QueuebitRetentionConfig {
  completedJobs?: QueuebitRetentionWindowConfig;
  failedWork?: QueuebitRetentionWindowConfig;
  terminalRuns?: QueuebitRetentionWindowConfig;
  completionEvents?: QueuebitRetentionWindowConfig;
}

export interface QueuebitDeduplicationConfig {
  jobKeyTtlMs?: number;
  runKeyTtlMs?: number;
}

export type QueuebitLogLevel = 'debug' | 'info' | 'warn' | 'error';
export type QueuebitMetricsFormat = 'prometheus';

export interface QueuebitMetricsConfig {
  enabled?: boolean;
  format?: QueuebitMetricsFormat;
  prefix?: string;
}

export interface QueuebitHealthConfig {
  staleAfterMs?: number;
}

export interface QueuebitObservabilityConfig {
  logLevel?: QueuebitLogLevel;
  metrics?: QueuebitMetricsConfig;
  health?: QueuebitHealthConfig;
}

export interface QueuebitUserConfig {
  connection?: QueuebitConnectionConfig;
  namespace?: string;
  workerDefaults?: QueuebitWorkerDefaults;
  scheduler?: QueuebitSchedulerConfig;
  queues?: Record<string, QueuebitQueueConfig>;
  batchRuns?: Record<string, QueuebitBatchRunConfig>;
  retention?: QueuebitRetentionConfig;
  deduplication?: QueuebitDeduplicationConfig;
  observability?: QueuebitObservabilityConfig;
  limits?: QueuebitLimitsConfig;
}

export interface QueuebitNormalizedConnectionConfig {
  mode: QueuebitConnectionMode;
  host: string;
  port: number;
  username?: string;
  password?: string;
  database: number;
  sentinels?: QueuebitSentinelNodeConfig[];
  masterName?: string;
  sentinelUsername?: string;
  sentinelPassword?: string;
  tls?: QueuebitTlsConfig;
  connectTimeoutMs: number;
  commandTimeoutMs: number;
  requestRetryLimit: number;
  backgroundReconnect: Required<QueuebitBackgroundReconnectConfig>;
  serverPolicy: { mode: ServerPolicyMode };
}

export interface QueuebitConfig {
  connection: QueuebitNormalizedConnectionConfig;
  namespace: string;
  workerDefaults: Required<QueuebitWorkerDefaults>;
  scheduler: Required<QueuebitSchedulerConfig>;
  queues: Record<string, QueuebitQueueConfig>;
  batchRuns: Record<string, QueuebitNormalizedBatchRunConfig>;
  retention: {
    completedJobs: Required<QueuebitRetentionWindowConfig>;
    failedWork: Required<QueuebitRetentionWindowConfig>;
    terminalRuns: Required<QueuebitRetentionWindowConfig>;
    completionEvents: Required<QueuebitRetentionWindowConfig>;
  };
  deduplication: Required<QueuebitDeduplicationConfig>;
  observability: {
    logLevel: QueuebitLogLevel;
    metrics: Required<QueuebitMetricsConfig>;
    health: Required<QueuebitHealthConfig>;
  };
  limits: Required<QueuebitLimitsConfig>;
}

const builtInDefaults: QueuebitConfig = {
  connection: {
    mode: 'direct',
    host: '127.0.0.1',
    port: 6379,
    database: 0,
    connectTimeoutMs: 5_000,
    commandTimeoutMs: 5_000,
    requestRetryLimit: 1,
    backgroundReconnect: {
      initialDelayMs: 250,
      maxDelayMs: 30_000,
      factor: 2,
      jitter: 'full',
      logThrottleMs: 30_000
    },
    serverPolicy: { mode: 'warn' }
  },
  namespace: 'default',
  workerDefaults: {
    concurrency: 1,
    leaseMs: 30_000,
    renewIntervalMs: 10_000,
    pollIntervalMs: 1_000,
    drainTimeoutMs: 60_000,
    maxStalledRecoveries: 2,
    heartbeatIntervalMs: 5_000,
    heartbeatTtlMs: 15_000
  },
  scheduler: {
    mode: 'cooperative',
    domain: 'default',
    leaseMs: 30_000,
    renewIntervalMs: 10_000,
    pollIntervalMs: 1_000,
    promotionBatchSize: 500,
    drainTimeoutMs: 60_000,
    heartbeatIntervalMs: 5_000,
    heartbeatTtlMs: 15_000
  },
  queues: {},
  batchRuns: {},
  retention: {
    completedJobs: { ageMs: 86_400_000, maxCount: 100_000 },
    failedWork: { ageMs: 604_800_000, maxCount: 100_000 },
    terminalRuns: { ageMs: 2_592_000_000, maxCount: 10_000 },
    completionEvents: { ageMs: 2_592_000_000, maxCount: 10_000 }
  },
  deduplication: {
    jobKeyTtlMs: 604_800_000,
    runKeyTtlMs: 2_592_000_000
  },
  observability: {
    logLevel: 'info',
    metrics: { enabled: true, format: 'prometheus', prefix: 'queuebit_' },
    health: { staleAfterMs: 45_000 }
  },
  limits: {
    maxRunInputBytes: 65_536,
    maxJobDataBytes: 262_144,
    maxJobResultBytes: 65_536,
    maxPageBytes: 8_388_608,
    maxBulkJobs: 1_000,
    maxBulkBytes: 8_388_608
  }
};

const positiveInteger = { type: 'integer', minimum: 1 };
const nonNegativeInteger = { type: 'integer', minimum: 0 };
const prometheusMetricPrefixPattern = '^[A-Za-z_:][A-Za-z0-9_:]*$';

const retentionWindowSchema = {
  type: 'object',
  additionalProperties: false,
  properties: {
    ageMs: positiveInteger,
    maxCount: positiveInteger
  }
};

const backpressureSchema = {
  type: 'object',
  additionalProperties: false,
  properties: {
    highWatermarkJobs: positiveInteger,
    lowWatermarkJobs: nonNegativeInteger,
    highWatermarkBytes: positiveInteger,
    lowWatermarkBytes: nonNegativeInteger
  }
};

const completionHandlerSchema = {
  type: 'object',
  additionalProperties: false,
  required: ['handler'],
  properties: {
    handler: { type: 'string', pattern: '^[A-Za-z0-9][A-Za-z0-9:_-]{0,191}$' },
    attempts: positiveInteger,
    backoff: {
      type: 'object',
      additionalProperties: false,
      required: ['type', 'delayMs'],
      properties: {
        type: { enum: ['fixed', 'exponential'] },
        delayMs: nonNegativeInteger,
        maxDelayMs: positiveInteger
      }
    }
  }
};

const configSchema = {
  type: 'object',
  additionalProperties: false,
  properties: {
    connection: {
      type: 'object',
      additionalProperties: false,
      properties: {
        mode: { enum: ['direct', 'sentinel'] },
        url: { type: 'string', minLength: 1 },
        host: { type: 'string', minLength: 1 },
        port: { type: 'integer', minimum: 1, maximum: 65_535 },
        username: { type: 'string', minLength: 1 },
        password: { type: 'string' },
        database: { type: 'integer', minimum: 0 },
        sentinels: {
          type: 'array',
          minItems: 2,
          items: {
            type: 'object',
            additionalProperties: false,
            required: ['host', 'port'],
            properties: {
              host: { type: 'string', minLength: 1 },
              port: { type: 'integer', minimum: 1, maximum: 65_535 }
            }
          }
        },
        masterName: { type: 'string', minLength: 1 },
        sentinelUsername: { type: 'string', minLength: 1 },
        sentinelPassword: { type: 'string' },
        tls: {
          type: 'object',
          additionalProperties: false,
          properties: {
            servername: { type: 'string', minLength: 1 },
            rejectUnauthorized: { type: 'boolean' }
          }
        },
        connectTimeoutMs: positiveInteger,
        commandTimeoutMs: positiveInteger,
        requestRetryLimit: nonNegativeInteger,
        backgroundReconnect: {
          type: 'object',
          additionalProperties: false,
          properties: {
            initialDelayMs: positiveInteger,
            maxDelayMs: positiveInteger,
            factor: { type: 'number', minimum: 1, maximum: 10 },
            jitter: { enum: ['none', 'full'] },
            logThrottleMs: positiveInteger
          }
        },
        serverPolicy: {
          type: 'object',
          additionalProperties: false,
          properties: {
            mode: { enum: ['warn', 'strict'] }
          }
        }
      }
    },
    namespace: { type: 'string', pattern: '^[A-Za-z0-9][A-Za-z0-9:_-]{0,127}$' },
    workerDefaults: {
      type: 'object',
      additionalProperties: false,
      properties: {
        concurrency: positiveInteger,
        leaseMs: positiveInteger,
        renewIntervalMs: positiveInteger,
        pollIntervalMs: positiveInteger,
        drainTimeoutMs: positiveInteger,
        maxStalledRecoveries: nonNegativeInteger,
        heartbeatIntervalMs: positiveInteger,
        heartbeatTtlMs: positiveInteger
      }
    },
    scheduler: {
      type: 'object',
      additionalProperties: false,
      properties: {
        mode: { const: 'cooperative' },
        domain: { type: 'string', minLength: 1 },
        leaseMs: positiveInteger,
        renewIntervalMs: positiveInteger,
        pollIntervalMs: positiveInteger,
        promotionBatchSize: positiveInteger,
        drainTimeoutMs: positiveInteger,
        heartbeatIntervalMs: positiveInteger,
        heartbeatTtlMs: positiveInteger
      }
    },
    queues: {
      type: 'object',
      additionalProperties: {
        type: 'object',
        additionalProperties: false,
        properties: {
          backpressure: backpressureSchema
        }
      }
    },
    batchRuns: {
      type: 'object',
      additionalProperties: {
        type: 'object',
        additionalProperties: false,
        required: ['queue', 'source', 'mapper'],
        properties: {
          version: positiveInteger,
          queue: { type: 'string', pattern: '^[A-Za-z0-9][A-Za-z0-9:_-]{0,191}$' },
          source: { type: 'string', pattern: '^[A-Za-z0-9][A-Za-z0-9:_-]{0,191}$' },
          mapper: { type: 'string', pattern: '^[A-Za-z0-9][A-Za-z0-9:_-]{0,191}$' },
          inputSchema: { type: 'object' },
          pageSize: positiveInteger,
          dispatch: {
            type: 'object',
            additionalProperties: false,
            properties: {
              mode: { enum: ['sequential', 'paced'] },
              intervalMs: nonNegativeInteger,
              maxInFlightBatches: positiveInteger
            }
          },
          completion: {
            type: 'object',
            additionalProperties: false,
            properties: {
              batch: completionHandlerSchema,
              run: completionHandlerSchema
            }
          }
        }
      }
    },
    retention: {
      type: 'object',
      additionalProperties: false,
      properties: {
        completedJobs: retentionWindowSchema,
        failedWork: retentionWindowSchema,
        terminalRuns: retentionWindowSchema,
        completionEvents: retentionWindowSchema
      }
    },
    deduplication: {
      type: 'object',
      additionalProperties: false,
      properties: {
        jobKeyTtlMs: positiveInteger,
        runKeyTtlMs: positiveInteger
      }
    },
    observability: {
      type: 'object',
      additionalProperties: false,
      properties: {
        logLevel: { enum: ['debug', 'info', 'warn', 'error'] },
        metrics: {
          type: 'object',
          additionalProperties: false,
          properties: {
            enabled: { type: 'boolean' },
            format: { const: 'prometheus' },
            prefix: { type: 'string', pattern: prometheusMetricPrefixPattern }
          }
        },
        health: {
          type: 'object',
          additionalProperties: false,
          properties: {
            staleAfterMs: positiveInteger
          }
        }
      }
    },
    limits: {
      type: 'object',
      additionalProperties: false,
      properties: {
        maxRunInputBytes: positiveInteger,
        maxJobDataBytes: positiveInteger,
        maxJobResultBytes: positiveInteger,
        maxPageBytes: positiveInteger,
        maxBulkJobs: positiveInteger,
        maxBulkBytes: positiveInteger
      }
    }
  }
} as const;

const ajv = new Ajv2020({
  strict: true,
  allErrors: true,
  coerceTypes: false,
  useDefaults: false,
  removeAdditional: false
});
addFormats(ajv, ['date-time', 'uuid', 'email']);

const validateConfig = ajv.compile(configSchema);

function mergeConnection(input: QueuebitConnectionConfig = {}): QueuebitNormalizedConnectionConfig {
  assertConnectionMode(input);
  const parsedUrl = input.url === undefined ? undefined : parseRedisConnectionUrl(input.url);
  const sentinelRoot = input.sentinels?.[0];
  const connection: QueuebitNormalizedConnectionConfig = {
    mode: input.mode ?? (input.sentinels !== undefined ? 'sentinel' : 'direct'),
    host:
      parsedUrl?.host
      ?? input.host
      ?? sentinelRoot?.host
      ?? builtInDefaults.connection.host,
    port:
      parsedUrl?.port
      ?? input.port
      ?? sentinelRoot?.port
      ?? builtInDefaults.connection.port,
    database: parsedUrl?.database ?? input.database ?? builtInDefaults.connection.database,
    connectTimeoutMs: input.connectTimeoutMs ?? builtInDefaults.connection.connectTimeoutMs,
    commandTimeoutMs: input.commandTimeoutMs ?? builtInDefaults.connection.commandTimeoutMs,
    requestRetryLimit: input.requestRetryLimit ?? builtInDefaults.connection.requestRetryLimit,
    backgroundReconnect: {
      initialDelayMs:
        input.backgroundReconnect?.initialDelayMs
        ?? builtInDefaults.connection.backgroundReconnect.initialDelayMs,
      maxDelayMs:
        input.backgroundReconnect?.maxDelayMs ?? builtInDefaults.connection.backgroundReconnect.maxDelayMs,
      factor: input.backgroundReconnect?.factor ?? builtInDefaults.connection.backgroundReconnect.factor,
      jitter: input.backgroundReconnect?.jitter ?? builtInDefaults.connection.backgroundReconnect.jitter,
      logThrottleMs:
        input.backgroundReconnect?.logThrottleMs
        ?? builtInDefaults.connection.backgroundReconnect.logThrottleMs
    },
    serverPolicy: {
      mode: input.serverPolicy?.mode ?? builtInDefaults.connection.serverPolicy.mode
    }
  };

  const username = parsedUrl?.username ?? input.username;
  const password = parsedUrl?.password ?? input.password;
  const tls = parsedUrl?.tls ?? input.tls;

  if (username !== undefined) connection.username = username;
  if (password !== undefined) connection.password = password;
  if (tls !== undefined) connection.tls = { ...tls };
  if (input.sentinels !== undefined) connection.sentinels = input.sentinels.map(node => ({ ...node }));
  if (input.masterName !== undefined) connection.masterName = input.masterName;
  if (input.sentinelUsername !== undefined) connection.sentinelUsername = input.sentinelUsername;
  if (input.sentinelPassword !== undefined) connection.sentinelPassword = input.sentinelPassword;

  return connection;
}

function assertConnectionMode(input: QueuebitConnectionConfig) {
  const usesUrl = input.url !== undefined;
  const usesSentinelFields =
    input.sentinels !== undefined
    || input.masterName !== undefined
    || input.sentinelUsername !== undefined
    || input.sentinelPassword !== undefined;
  const usesSentinel = input.mode === 'sentinel' || usesSentinelFields;
  const usesDirectEndpoint = input.host !== undefined || input.port !== undefined;

  if (input.mode === 'direct' && usesSentinelFields) {
    throw connectionModeError('connection.mode=direct cannot be combined with Sentinel fields.');
  }
  if (usesUrl && (usesDirectEndpoint || usesSentinelFields || input.mode === 'sentinel')) {
    throw connectionModeError('connection.url cannot be combined with direct host/port or Sentinel fields.');
  }
  if (usesUrl && (
    input.username !== undefined
    || input.password !== undefined
    || input.database !== undefined
    || input.tls !== undefined
  )) {
    throw connectionModeError('connection.url must carry credentials, database, and TLS scheme by itself.');
  }
  if (usesSentinel && input.mode !== 'sentinel' && usesDirectEndpoint) {
    throw connectionModeError('connection.sentinels/masterName cannot be combined with direct host/port.');
  }
  if (!usesSentinel) return;
  if (input.url !== undefined) {
    throw connectionModeError('connection.sentinels/masterName cannot be combined with connection.url.');
  }
  if (input.sentinels === undefined || input.sentinels.length < 2) {
    throw connectionModeError('connection.sentinels requires at least two Sentinel addresses.');
  }
  if (input.masterName === undefined) {
    throw connectionModeError('connection.masterName is required when Sentinel is configured.');
  }
}

function connectionModeError(message: string): QueuebitError {
  return new QueuebitError({
    code: 'QB_CONFIG_INVALID',
    message,
    details: { field: 'connection' }
  });
}

function parseRedisConnectionUrl(value: string): {
  host: string;
  port: number;
  username?: string;
  password?: string;
  database: number;
  tls?: QueuebitTlsConfig;
} {
  let url: URL;
  try {
    url = new URL(value);
  } catch (cause) {
    throw new QueuebitError({
      code: 'QB_CONFIG_INVALID',
      message: 'connection.url must be a valid redis:// or rediss:// URL.',
      details: { cause }
    });
  }
  if (url.protocol !== 'redis:' && url.protocol !== 'rediss:') {
    throw new QueuebitError({
      code: 'QB_CONFIG_INVALID',
      message: 'connection.url must use redis:// or rediss://.',
      details: { protocol: url.protocol }
    });
  }
  if (url.hostname.length === 0) {
    throw new QueuebitError({
      code: 'QB_CONFIG_INVALID',
      message: 'connection.url must include a Redis host.',
      details: { field: 'connection.url' }
    });
  }
  const database = parseRedisDatabase(url);
  const port = parseRedisUrlPort(url);
  const tls = url.protocol === 'rediss:' ? { servername: url.hostname } : undefined;
  return {
    host: url.hostname,
    port,
    ...(url.username ? { username: decodeURIComponent(url.username) } : {}),
    ...(url.password ? { password: decodeURIComponent(url.password) } : {}),
    database,
    ...(tls === undefined ? {} : { tls })
  };
}

function parseRedisUrlPort(url: URL): number {
  const port = url.port ? Number.parseInt(url.port, 10) : 6379;
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new QueuebitError({
      code: 'QB_CONFIG_INVALID',
      message: 'connection.url port must be an integer between 1 and 65535.',
      details: { port: url.port }
    });
  }
  return port;
}

function parseRedisDatabase(url: URL): number {
  if (url.pathname.length <= 1) return 0;
  const database = Number.parseInt(url.pathname.slice(1), 10);
  if (!Number.isInteger(database) || database < 0) {
    throw new QueuebitError({
      code: 'QB_CONFIG_INVALID',
      message: 'connection.url database path must be a non-negative integer.',
      details: { path: url.pathname }
    });
  }
  return database;
}

const defaultQueueBackpressure = {
  highWatermarkJobs: 10_000,
  lowWatermarkJobs: 5_000,
  highWatermarkBytes: 268_435_456,
  lowWatermarkBytes: 134_217_728
} as const;

function mergeQueues(input: Record<string, QueuebitQueueConfig> = {}): Record<string, QueuebitQueueConfig> {
  const queues: Record<string, QueuebitQueueConfig> = {};
  for (const [queueName, queueConfig] of Object.entries(input)) {
    // Documented built-in defaults apply whenever a queue is declared without explicit watermarks.
    queues[queueName] = {
      backpressure: {
        ...defaultQueueBackpressure,
        ...(queueConfig.backpressure ?? {})
      }
    };
  }
  return queues;
}

function mergeBatchRuns(
  input: Record<string, QueuebitBatchRunConfig> = {}
): QueuebitConfig['batchRuns'] {
  const batchRuns: QueuebitConfig['batchRuns'] = {};
  for (const [definition, runConfig] of Object.entries(input)) {
    const dispatch: Required<QueuebitBatchDispatchConfig> = {
      mode: runConfig.dispatch?.mode ?? 'sequential',
      intervalMs: runConfig.dispatch?.intervalMs ?? 0,
      maxInFlightBatches: runConfig.dispatch?.maxInFlightBatches ?? 1
    };
    batchRuns[definition] = {
      version: runConfig.version ?? 1,
      queue: runConfig.queue,
      source: runConfig.source,
      mapper: runConfig.mapper,
      pageSize: runConfig.pageSize ?? 100,
      dispatch,
      completion: normalizeCompletionConfig(runConfig.completion),
      ...(runConfig.inputSchema === undefined ? {} : { inputSchema: runConfig.inputSchema })
    };
  }
  return batchRuns;
}

function normalizeCompletionConfig(
  completion: QueuebitBatchRunCompletionConfig | undefined
): QueuebitBatchRunCompletionConfig {
  const normalized: QueuebitBatchRunCompletionConfig = {};
  if (completion?.batch !== undefined) normalized.batch = normalizeCompletionHandler(completion.batch);
  if (completion?.run !== undefined) normalized.run = normalizeCompletionHandler(completion.run);
  return normalized;
}

function normalizeCompletionHandler(handler: QueuebitCompletionHandlerConfig): QueuebitCompletionHandlerConfig {
  const normalized: QueuebitCompletionHandlerConfig = {
    handler: handler.handler,
    attempts: handler.attempts ?? 3
  };
  if (handler.backoff !== undefined) {
    normalized.backoff = {
      type: handler.backoff.type,
      delayMs: handler.backoff.delayMs,
      ...(handler.backoff.maxDelayMs === undefined ? {} : { maxDelayMs: handler.backoff.maxDelayMs })
    };
  }
  return normalized;
}

function normalizeRetentionWindow(
  input: QueuebitRetentionWindowConfig | undefined,
  defaults: Required<QueuebitRetentionWindowConfig>
): Required<QueuebitRetentionWindowConfig> {
  return {
    ageMs: input?.ageMs ?? defaults.ageMs,
    maxCount: input?.maxCount ?? defaults.maxCount
  };
}

function mergeRetention(input: QueuebitRetentionConfig = {}): QueuebitConfig['retention'] {
  return {
    completedJobs: normalizeRetentionWindow(
      input.completedJobs,
      builtInDefaults.retention.completedJobs
    ),
    failedWork: normalizeRetentionWindow(
      input.failedWork,
      builtInDefaults.retention.failedWork
    ),
    terminalRuns: normalizeRetentionWindow(
      input.terminalRuns,
      builtInDefaults.retention.terminalRuns
    ),
    completionEvents: normalizeRetentionWindow(
      input.completionEvents,
      builtInDefaults.retention.completionEvents
    )
  };
}

function mergeDeduplication(
  input: QueuebitDeduplicationConfig = {}
): QueuebitConfig['deduplication'] {
  return {
    jobKeyTtlMs: input.jobKeyTtlMs ?? builtInDefaults.deduplication.jobKeyTtlMs,
    runKeyTtlMs: input.runKeyTtlMs ?? builtInDefaults.deduplication.runKeyTtlMs
  };
}

function mergeObservability(
  input: QueuebitObservabilityConfig = {}
): QueuebitConfig['observability'] {
  return {
    logLevel: input.logLevel ?? builtInDefaults.observability.logLevel,
    metrics: {
      enabled: input.metrics?.enabled ?? builtInDefaults.observability.metrics.enabled,
      format: input.metrics?.format ?? builtInDefaults.observability.metrics.format,
      prefix: input.metrics?.prefix ?? builtInDefaults.observability.metrics.prefix
    },
    health: {
      staleAfterMs: input.health?.staleAfterMs ?? builtInDefaults.observability.health.staleAfterMs
    }
  };
}

function mergeConfig(input: QueuebitUserConfig): QueuebitConfig {
  return {
    connection: mergeConnection(input.connection),
    namespace: input.namespace ?? builtInDefaults.namespace,
    workerDefaults: {
      concurrency: input.workerDefaults?.concurrency ?? builtInDefaults.workerDefaults.concurrency,
      leaseMs: input.workerDefaults?.leaseMs ?? builtInDefaults.workerDefaults.leaseMs,
      renewIntervalMs:
        input.workerDefaults?.renewIntervalMs ?? builtInDefaults.workerDefaults.renewIntervalMs,
      pollIntervalMs: input.workerDefaults?.pollIntervalMs ?? builtInDefaults.workerDefaults.pollIntervalMs,
      drainTimeoutMs:
        input.workerDefaults?.drainTimeoutMs ?? builtInDefaults.workerDefaults.drainTimeoutMs,
      maxStalledRecoveries:
        input.workerDefaults?.maxStalledRecoveries
        ?? builtInDefaults.workerDefaults.maxStalledRecoveries,
      heartbeatIntervalMs:
        input.workerDefaults?.heartbeatIntervalMs ?? builtInDefaults.workerDefaults.heartbeatIntervalMs,
      heartbeatTtlMs:
        input.workerDefaults?.heartbeatTtlMs ?? builtInDefaults.workerDefaults.heartbeatTtlMs
    },
    scheduler: {
      mode: input.scheduler?.mode ?? builtInDefaults.scheduler.mode,
      domain: input.scheduler?.domain ?? builtInDefaults.scheduler.domain,
      leaseMs: input.scheduler?.leaseMs ?? builtInDefaults.scheduler.leaseMs,
      renewIntervalMs: input.scheduler?.renewIntervalMs ?? builtInDefaults.scheduler.renewIntervalMs,
      pollIntervalMs: input.scheduler?.pollIntervalMs ?? builtInDefaults.scheduler.pollIntervalMs,
      promotionBatchSize:
        input.scheduler?.promotionBatchSize ?? builtInDefaults.scheduler.promotionBatchSize,
      drainTimeoutMs: input.scheduler?.drainTimeoutMs ?? builtInDefaults.scheduler.drainTimeoutMs,
      heartbeatIntervalMs:
        input.scheduler?.heartbeatIntervalMs ?? builtInDefaults.scheduler.heartbeatIntervalMs,
      heartbeatTtlMs:
        input.scheduler?.heartbeatTtlMs ?? builtInDefaults.scheduler.heartbeatTtlMs
    },
    queues: mergeQueues(input.queues),
    batchRuns: mergeBatchRuns(input.batchRuns),
    retention: mergeRetention(input.retention),
    deduplication: mergeDeduplication(input.deduplication),
    observability: mergeObservability(input.observability),
    limits: {
      maxRunInputBytes: input.limits?.maxRunInputBytes ?? builtInDefaults.limits.maxRunInputBytes,
      maxJobDataBytes: input.limits?.maxJobDataBytes ?? builtInDefaults.limits.maxJobDataBytes,
      maxJobResultBytes:
        input.limits?.maxJobResultBytes ?? builtInDefaults.limits.maxJobResultBytes,
      maxPageBytes: input.limits?.maxPageBytes ?? builtInDefaults.limits.maxPageBytes,
      maxBulkJobs: input.limits?.maxBulkJobs ?? builtInDefaults.limits.maxBulkJobs,
      maxBulkBytes: input.limits?.maxBulkBytes ?? builtInDefaults.limits.maxBulkBytes
    }
  };
}

function assertWatermarks(config: QueuebitConfig) {
  for (const [queue, queueConfig] of Object.entries(config.queues)) {
    const backpressure = queueConfig.backpressure;
    if (!backpressure) continue;
    const { highWatermarkJobs, lowWatermarkJobs, highWatermarkBytes, lowWatermarkBytes } =
      backpressure;
    if (
      typeof highWatermarkJobs === 'number'
      && typeof lowWatermarkJobs === 'number'
      && lowWatermarkJobs >= highWatermarkJobs
    ) {
      throw new QueuebitError({
        code: 'QB_CONFIG_INVALID',
        message: `Queue "${queue}" lowWatermarkJobs must be lower than highWatermarkJobs.`,
        details: { queue, field: 'backpressure.lowWatermarkJobs' }
      });
    }
    if (
      typeof highWatermarkBytes === 'number'
      && typeof lowWatermarkBytes === 'number'
      && lowWatermarkBytes >= highWatermarkBytes
    ) {
      throw new QueuebitError({
        code: 'QB_CONFIG_INVALID',
        message: `Queue "${queue}" lowWatermarkBytes must be lower than highWatermarkBytes.`,
        details: { queue, field: 'backpressure.lowWatermarkBytes' }
      });
    }
  }
}

function assertBatchRuns(config: QueuebitConfig) {
  const definitionPattern = /^[A-Za-z0-9][A-Za-z0-9:_-]{0,191}$/;
  for (const [definition, batchRun] of Object.entries(config.batchRuns)) {
    if (!definitionPattern.test(definition)) {
      throw new QueuebitError({
        code: 'QB_CONFIG_INVALID',
        message: `BatchRun definition "${definition}" has an invalid name.`,
        details: { definition }
      });
    }
    if (config.queues[batchRun.queue] === undefined) {
      throw new QueuebitError({
        code: 'QB_CONFIG_INVALID',
        message: `BatchRun "${definition}" references undeclared queue "${batchRun.queue}".`,
        details: { definition, queue: batchRun.queue }
      });
    }
    if (
      batchRun.dispatch.mode === 'sequential'
      && batchRun.dispatch.maxInFlightBatches !== 1
    ) {
      throw new QueuebitError({
        code: 'QB_CONFIG_INVALID',
        message: `BatchRun "${definition}" sequential dispatch requires maxInFlightBatches=1.`,
        details: { definition, field: 'dispatch.maxInFlightBatches' }
      });
    }
  }
}

function assertHeartbeatDefaults(config: QueuebitConfig) {
  if (config.workerDefaults.heartbeatTtlMs <= config.workerDefaults.heartbeatIntervalMs) {
    throw new QueuebitError({
      code: 'QB_CONFIG_INVALID',
      message: 'workerDefaults.heartbeatTtlMs must be greater than heartbeatIntervalMs.',
      details: { section: 'workerDefaults' }
    });
  }
  if (config.scheduler.heartbeatTtlMs <= config.scheduler.heartbeatIntervalMs) {
    throw new QueuebitError({
      code: 'QB_CONFIG_INVALID',
      message: 'scheduler.heartbeatTtlMs must be greater than heartbeatIntervalMs.',
      details: { section: 'scheduler' }
    });
  }
}

function assertTimingDefaults(config: QueuebitConfig) {
  if (config.connection.backgroundReconnect.maxDelayMs < config.connection.backgroundReconnect.initialDelayMs) {
    throw new QueuebitError({
      code: 'QB_CONFIG_INVALID',
      message: 'connection.backgroundReconnect.maxDelayMs must be greater than or equal to initialDelayMs.',
      details: { section: 'connection.backgroundReconnect' }
    });
  }
  if (config.workerDefaults.renewIntervalMs >= config.workerDefaults.leaseMs / 2) {
    throw new QueuebitError({
      code: 'QB_CONFIG_INVALID',
      message: 'workerDefaults.renewIntervalMs must be less than half of leaseMs.',
      details: { section: 'workerDefaults' }
    });
  }
  if (config.scheduler.renewIntervalMs >= config.scheduler.leaseMs / 2) {
    throw new QueuebitError({
      code: 'QB_CONFIG_INVALID',
      message: 'scheduler.renewIntervalMs must be less than half of leaseMs.',
      details: { section: 'scheduler' }
    });
  }
}

function assertRetentionAndDeduplication(config: QueuebitConfig) {
  if (config.deduplication.jobKeyTtlMs < config.retention.completedJobs.ageMs) {
    throw new QueuebitError({
      code: 'QB_CONFIG_INVALID',
      message: 'deduplication.jobKeyTtlMs must not be shorter than retention.completedJobs.ageMs.',
      details: { section: 'deduplication' }
    });
  }
  if (config.deduplication.runKeyTtlMs < config.retention.terminalRuns.ageMs) {
    throw new QueuebitError({
      code: 'QB_CONFIG_INVALID',
      message: 'deduplication.runKeyTtlMs must not be shorter than retention.terminalRuns.ageMs.',
      details: { section: 'deduplication' }
    });
  }
  if (config.deduplication.runKeyTtlMs < config.retention.completionEvents.ageMs) {
    throw new QueuebitError({
      code: 'QB_CONFIG_INVALID',
      message: 'deduplication.runKeyTtlMs must not be shorter than retention.completionEvents.ageMs.',
      details: { section: 'deduplication' }
    });
  }
}

/**
 * Validates and normalizes Queuebit configuration without opening Redis,
 * importing runtime handlers, or mutating the user-provided object.
 */
export function defineQueuebitConfig(input: QueuebitUserConfig): QueuebitConfig {
  if (!validateConfig(input)) {
    throw new QueuebitError({
      code: 'QB_CONFIG_INVALID',
      message: 'Queuebit configuration is invalid.',
      details: validateConfig.errors ?? []
    });
  }
  const merged = mergeConfig(input);
  assertWatermarks(merged);
  assertBatchRuns(merged);
  assertHeartbeatDefaults(merged);
  assertTimingDefaults(merged);
  assertRetentionAndDeduplication(merged);
  return merged;
}
