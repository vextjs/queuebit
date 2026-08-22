import {
  defineQueuebitConfig,
  type QueuebitConfig,
  type QueuebitUserConfig
} from './config';
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
  createQueuebitCoordinatorRunner,
  type QueuebitCoordinator,
  type QueuebitCoordinatorOptions,
  type QueuebitCoordinatorRunner,
  type QueuebitCoordinatorRunnerOptions
} from './coordinator';
import type { QueuebitRuntimeDefinition } from './runtime';
import {
  createQueuebitWorker,
  type QueuebitWorker,
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
  createCoordinatorRunner(
    runtime: QueuebitRuntimeDefinition,
    options?: QueuebitClientCoordinatorRunnerOptions
  ): QueuebitCoordinatorRunner;
  createWorker<Data = unknown, Result = unknown>(
    queue: string,
    processor: QueuebitWorkerProcessor<Data, Result>,
    options?: QueuebitClientWorkerOptions
  ): QueuebitWorker;
  close(options?: QueuebitClientCloseOptions): Promise<void>;
}

export type QueuebitClientWorkerOptions = Omit<
  QueuebitWorkerOptions,
  'config' | 'redis' | 'queue' | 'processor' | 'roleRegistry' | 'observability' | 'now'
>;

export type QueuebitClientCoordinatorOptions = Omit<
  QueuebitCoordinatorOptions,
  'config' | 'redis' | 'runtime' | 'observability' | 'now'
>;

export type QueuebitClientCoordinatorRunnerOptions = Omit<
  QueuebitCoordinatorRunnerOptions,
  'config' | 'redis' | 'runtime' | 'roleRegistry' | 'observability' | 'now'
>;

export interface QueuebitClientCloseOptions {
  timeoutMs?: number;
}

export interface QueuebitClientOptions {
  config: QueuebitConfig;
  redis?: QueuebitRedisCommandClient;
  preflight?: boolean;
  logger?: QueuebitLogger;
  now?: () => Date;
}

export type QueuebitClientDirectOptions = Omit<QueuebitClientOptions, 'config'>;

export function createQueuebitClient(
  config: QueuebitUserConfig,
  options?: QueuebitClientDirectOptions
): Promise<QueuebitClient>;
export function createQueuebitClient(options: QueuebitClientOptions): Promise<QueuebitClient>;
export async function createQueuebitClient(
  configOrOptions: QueuebitUserConfig | QueuebitClientOptions,
  directOptions: QueuebitClientDirectOptions = {}
): Promise<QueuebitClient> {
  const options: QueuebitClientOptions = 'config' in configOrOptions
    ? configOrOptions
    : { ...directOptions, config: defineQueuebitConfig(configOrOptions) };
  const now = options.now ?? (() => new Date());
  const ownedConnection = options.redis === undefined
    ? createQueuebitRedisConnection(options.config, { name: `queuebit:${options.config.namespace}` })
    : undefined;
  let redis: QueuebitRedisCommandClient;
  let preflightResult: RedisPreflightResult | undefined;

  try {
    redis = options.redis ?? await connectOwned(ownedConnection);
    if (options.preflight !== false) {
      preflightResult = await runRedisPreflight(
        redis as QueuebitRedisCommandClient & RedisPreflightClient,
        options.config.connection.serverPolicy.mode
      );
      assertRedisPreflightReady(preflightResult);
    }
  } catch (cause) {
    await closeOwnedConnectionAfterInitializationFailure(ownedConnection, cause);
    throw cause;
  }

  const keys = createQueuebitKeyBuilder(options.config);
  const workers = new Set<QueuebitWorker>();
  const coordinatorRunners = new Set<QueuebitCoordinatorRunner>();
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
    createCoordinatorRunner(
      runtime: QueuebitRuntimeDefinition,
      runnerOptions: QueuebitClientCoordinatorRunnerOptions = {}
    ) {
      const runner = createQueuebitCoordinatorRunner({
        config: options.config,
        redis,
        runtime,
        ...runnerOptions,
        roleRegistry: roles,
        observability: observability.recorder,
        now
      });
      coordinatorRunners.add(runner);
      return runner;
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
    async close(closeOptions: QueuebitClientCloseOptions = {}) {
      if (closing) return;
      closing = true;
      const runnerResults = await Promise.allSettled(
        [...coordinatorRunners].map(runner => runner.stop(closeOptions))
      );
      coordinatorRunners.clear();
      const workerResults = await Promise.allSettled([...workers].map(worker => worker.stop(closeOptions)));
      workers.clear();
      const failures = [...runnerResults, ...workerResults]
        .filter((result): result is PromiseRejectedResult => result.status === 'rejected')
        .map(result => result.reason);
      if (ownedConnection !== undefined) {
        try {
          await ownedConnection.close();
        } catch (cause) {
          failures.push(cause);
        }
      }
      if (failures.length > 0) {
        throw new AggregateError(failures, 'Queuebit client close encountered one or more cleanup failures.');
      }
    }
  };
}

async function closeOwnedConnectionAfterInitializationFailure(
  connection: QueuebitRedisConnection | undefined,
  cause: unknown
): Promise<never> {
  if (connection === undefined) throw cause;
  try {
    await connection.close();
  } catch (closeCause) {
    throw new AggregateError(
      [cause, closeCause],
      'Queuebit client initialization failed and the owned Redis connection could not be closed.'
    );
  }
  throw cause;
}

async function connectOwned(connection: QueuebitRedisConnection | undefined): Promise<QueuebitRedisCommandClient> {
  if (connection === undefined) {
    throw new Error('Queuebit internal invariant violated: missing Redis connection.');
  }
  return connection.connect() as Promise<QueuebitRedisCommandClient>;
}
