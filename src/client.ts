import type { QueuebitConfig } from './config';
import { createQueuebitCompletionsApi, type CompletionsApi } from './completions';
import {
  createQueuebitRedisConnection,
  assertRedisPreflightReady,
  createQueuebitKeyBuilder,
  runRedisPreflight,
  type QueuebitRedisConnection,
  type QueuebitRedisCommandClient,
  type RedisPreflightClient,
  type RedisPreflightResult
} from './redis';
import { createQueuebitJobsApi, type JobsApi } from './jobs';
import {
  createQueuebitObservabilityBackend,
  type AlertsApi,
  type CapacityApi,
  type HealthApi,
  type HealthCheck,
  type HealthSnapshot,
  type HealthStatus,
  type MetricsApi,
  type QueuebitAlertEvaluation,
  type QueuebitAlertEvaluationOptions,
  type QueuebitAlertFinding,
  type QueuebitAlertSeverity,
  type QueuebitCapacitySnapshot,
  type QueuebitMetricSample,
  type QueuebitObservabilityHttpApi,
  type QueuebitObservabilityHttpOptions,
  type QueuebitObservabilityHttpRequest,
  type QueuebitObservabilityHttpResponse,
  type QueuebitQueueCapacitySnapshot,
  type QueuebitRetentionPurgeCandidate,
  type QueuebitRetentionPurgeDecision,
  type QueuebitRetentionPurgeMode,
  type QueuebitRetentionPurgeOptions,
  type QueuebitRetentionPurgeReason,
  type QueuebitRetentionPurgeResult,
  type QueuebitRetentionPurgeWindow,
  type QueuebitRetentionPlan,
  type QueuebitRetentionWindowPlan,
  type RetentionApi
} from './observability';
import { createQueuebitRunsApi, type RunsApi } from './runs';
import { createQueuebitRolesApi, type QueuebitRolesApi } from './roles';
import {
  createQueuebitCoordinator,
  type QueuebitCoordinator,
  type QueuebitCoordinatorOptions
} from './coordinator';
import type { QueuebitRuntimeDefinition } from './runtime';
import {
  createQueuebitWorker,
  type QueuebitWorker,
  type QueuebitWorkerDrainOptions,
  type QueuebitWorkerOptions,
  type QueuebitWorkerProcessor
} from './worker';

export type {
  AlertsApi,
  CapacityApi,
  HealthApi,
  HealthCheck,
  HealthSnapshot,
  HealthStatus,
  MetricsApi,
  QueuebitAlertEvaluation,
  QueuebitAlertEvaluationOptions,
  QueuebitAlertFinding,
  QueuebitAlertSeverity,
  QueuebitCapacitySnapshot,
  QueuebitMetricSample,
  QueuebitObservabilityHttpApi,
  QueuebitObservabilityHttpOptions,
  QueuebitObservabilityHttpRequest,
  QueuebitObservabilityHttpResponse,
  QueuebitQueueCapacitySnapshot,
  QueuebitRetentionPurgeCandidate,
  QueuebitRetentionPurgeDecision,
  QueuebitRetentionPurgeMode,
  QueuebitRetentionPurgeOptions,
  QueuebitRetentionPurgeReason,
  QueuebitRetentionPurgeResult,
  QueuebitRetentionPurgeWindow,
  QueuebitRetentionPlan,
  QueuebitRetentionWindowPlan,
  RetentionApi
} from './observability';

export interface QueuebitLogger {
  trace?(...args: unknown[]): void;
  debug?(...args: unknown[]): void;
  info?(...args: unknown[]): void;
  warn?(...args: unknown[]): void;
  error?(...args: unknown[]): void;
  fatal?(...args: unknown[]): void;
  child?(bindings: Record<string, unknown>): QueuebitLogger;
}

export interface QueuebitClient {
  readonly jobs: JobsApi;
  readonly runs: RunsApi;
  readonly completions: CompletionsApi;
  readonly roles: QueuebitRolesApi;
  readonly health: HealthApi;
  readonly metrics: MetricsApi;
  readonly retention: RetentionApi;
  readonly capacity: CapacityApi;
  readonly alerts: AlertsApi;
  readonly observabilityHttp: QueuebitObservabilityHttpApi;
  createCoordinator(runtime: QueuebitRuntimeDefinition, options?: QueuebitClientCoordinatorOptions): QueuebitCoordinator;
  createWorker<Data = unknown, Result = unknown>(
    queue: string,
    processor: QueuebitWorkerProcessor<Data, Result>,
    options?: QueuebitClientWorkerOptions
  ): QueuebitWorker;
  close(options?: QueuebitWorkerDrainOptions): Promise<void>;
}

export type QueuebitClientWorkerOptions = Omit<
  QueuebitWorkerOptions,
  'config' | 'redis' | 'queue' | 'processor' | 'roleRegistry' | 'observability' | 'now'
>;

export type QueuebitClientCoordinatorOptions = Omit<
  QueuebitCoordinatorOptions,
  'config' | 'redis' | 'runtime' | 'observability' | 'now'
>;

export interface QueuebitClientOptions {
  config: QueuebitConfig;
  redis?: QueuebitRedisCommandClient;
  preflight?: boolean;
  logger?: QueuebitLogger;
  now?: () => Date;
}

export async function createQueuebitClient(options: QueuebitClientOptions): Promise<QueuebitClient> {
  const now = options.now ?? (() => new Date());
  const ownedConnection = options.redis === undefined
    ? createQueuebitRedisConnection(options.config, { name: `queuebit:${options.config.namespace}` })
    : undefined;
  const redis = options.redis ?? await connectOwned(ownedConnection);
  let preflightResult: RedisPreflightResult | undefined;

  if (options.preflight !== false) {
    preflightResult = await runRedisPreflight(
      redis as QueuebitRedisCommandClient & RedisPreflightClient,
      options.config.connection.serverPolicy.mode
    );
    assertRedisPreflightReady(preflightResult);
  }

  const keys = createQueuebitKeyBuilder(options.config);
  const workers = new Set<QueuebitWorker>();
  let closing = false;
  const observability = createQueuebitObservabilityBackend({
    config: options.config,
    redis,
    keys,
    now,
    getPreflight: () => preflightResult,
    getClosing: () => closing,
    getWorkerCount: () => workers.size
  });
  const jobs = createQueuebitJobsApi({
    config: options.config,
    redis,
    now,
    observability: observability.recorder
  });
  const runs = createQueuebitRunsApi({ config: options.config, redis, now });
  const completions = createQueuebitCompletionsApi({ config: options.config, redis, now });
  const roles = createQueuebitRolesApi({ config: options.config, redis, now });

  return {
    jobs,
    runs,
    completions,
    roles,
    health: observability.health,
    metrics: observability.metrics,
    retention: observability.retention,
    capacity: observability.capacity,
    alerts: observability.alerts,
    observabilityHttp: observability.observabilityHttp,
    createCoordinator(runtime: QueuebitRuntimeDefinition, coordinatorOptions: QueuebitClientCoordinatorOptions = {}) {
      return createQueuebitCoordinator({
        config: options.config,
        redis,
        runtime,
        ...coordinatorOptions,
        observability: observability.recorder,
        now
      });
    },
    createWorker<Data = unknown, Result = unknown>(
      queue: string,
      processor: QueuebitWorkerProcessor<Data, Result>,
      workerOptions: QueuebitClientWorkerOptions = {}
    ) {
      const worker = createQueuebitWorker<Data, Result>({
        config: options.config,
        redis,
        queue,
        processor,
        ...workerOptions,
        roleRegistry: roles,
        observability: observability.recorder,
        now
      });
      workers.add(worker);
      return worker;
    },
    async close(closeOptions: QueuebitWorkerDrainOptions = {}) {
      closing = true;
      await Promise.all([...workers].map(worker => worker.stop(closeOptions)));
      workers.clear();
      if (ownedConnection !== undefined) await ownedConnection.close();
    }
  };
}

async function connectOwned(connection: QueuebitRedisConnection | undefined): Promise<QueuebitRedisCommandClient> {
  if (connection === undefined) {
    throw new Error('Queuebit internal invariant violated: missing Redis connection.');
  }
  return connection.connect() as Promise<QueuebitRedisCommandClient>;
}
