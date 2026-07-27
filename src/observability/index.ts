import type { QueuebitConfig } from '../config';
import { QueuebitError } from '../errors';
import type {
  createQueuebitKeyBuilder,
  RedisPreflightResult
} from '../redis';
import { executeQueuebitScript, type QueuebitRedisCommandClient } from '../redis';
import { registerObservabilityScripts } from './scripts';

export type HealthStatus = 'ready' | 'degraded' | 'not_ready' | 'draining';
export type QueuebitMetricType = 'counter' | 'gauge' | 'histogram';
export type QueuebitMetricLabels = Record<string, string>;

export interface HealthCheck {
  status: 'pass' | 'warn' | 'fail';
  observedAt: string;
  message?: string;
  details?: Record<string, unknown>;
}

export interface HealthSnapshot {
  status: HealthStatus;
  ready: boolean;
  role: 'producer' | 'worker' | 'coordinator';
  identity?: string;
  timestamp: string;
  checks: Record<string, HealthCheck>;
}

export interface QueuebitMetricSample {
  name: string;
  type: QueuebitMetricType;
  value: number;
  labels: QueuebitMetricLabels;
  timestamp?: string;
}

export interface MetricsApi {
  collect(): readonly QueuebitMetricSample[];
  renderPrometheus(): string;
}

export interface QueuebitRetentionWindowPlan {
  ageMs: number;
  maxCount: number;
  tombstoneTtlMs?: number;
  eligibleStates: readonly string[];
  protectedStates: readonly string[];
}

export interface QueuebitRetentionPlan {
  namespace: string;
  observedAt: string;
  completedJobs: QueuebitRetentionWindowPlan;
  failedWork: QueuebitRetentionWindowPlan;
  terminalRuns: QueuebitRetentionWindowPlan;
  completionEvents: QueuebitRetentionWindowPlan;
  guards: readonly string[];
}

export type QueuebitRetentionPurgeMode = 'dry-run' | 'execute';
export type QueuebitRetentionPurgeWindow = 'completedJobs' | 'terminalRuns' | 'completions';
export type QueuebitRetentionPurgeDecision =
  | 'would_delete'
  | 'would_tombstone'
  | 'deleted'
  | 'tombstoned'
  | 'skipped';
export type QueuebitRetentionPurgeReason =
  | 'expired_by_age'
  | 'exceeds_max_count'
  | 'retained_by_window'
  | 'snapshot_missing'
  | 'state_protected'
  | 'completion_protected'
  | 'batchrun_owned'
  | 'tombstone_required'
  | 'details_expired'
  | 'updated_at_invalid'
  | 'execute_conflict';

export interface QueuebitRetentionPurgeOptions {
  mode?: QueuebitRetentionPurgeMode;
  limit?: number;
}

export interface QueuebitRetentionPurgeCandidate {
  window: QueuebitRetentionPurgeWindow;
  queue?: string;
  jobId?: string;
  runId?: string;
  eventId?: string;
  key: string;
  score: string;
  state?: string;
  completionState?: string;
  type?: string;
  updatedAt?: string;
  decision: QueuebitRetentionPurgeDecision;
  reason: QueuebitRetentionPurgeReason;
}

export interface QueuebitRetentionPurgeResult {
  namespace: string;
  mode: QueuebitRetentionPurgeMode;
  observedAt: string;
  windows: {
    completedJobs: {
      ageMs: number;
      maxCount: number;
      cutoffAt: string;
      eligibleStates: readonly string[];
      protectedStates: readonly string[];
    };
    terminalRuns: {
      ageMs: number;
      maxCount: number;
      cutoffAt: string;
      eligibleStates: readonly string[];
      protectedStates: readonly string[];
    };
    completions: {
      ageMs: number;
      maxCount: number;
      cutoffAt: string;
      eligibleStates: readonly string[];
      protectedStates: readonly string[];
    };
  };
  scanned: number;
  deleted: number;
  tombstoned: number;
  skipped: number;
  hasMore: boolean;
  candidates: readonly QueuebitRetentionPurgeCandidate[];
}

export interface RetentionApi {
  plan(): QueuebitRetentionPlan;
  purge(options?: QueuebitRetentionPurgeOptions): Promise<QueuebitRetentionPurgeResult>;
}

export interface QueuebitQueueCapacitySnapshot {
  queue: string;
  observedAt: string;
  counters: {
    queuedJobs: number;
    queuedBytes: number;
    waitingJobs: number;
    activeJobs: number;
    delayedJobs: number;
    retryingJobs: number;
    completedJobs: number;
    failedJobs: number;
    cancelledJobs: number;
  };
  watermarks: {
    highWatermarkJobs?: number;
    lowWatermarkJobs?: number;
    highWatermarkBytes?: number;
    lowWatermarkBytes?: number;
  };
  utilization: {
    jobs?: number;
    bytes?: number;
  };
  backpressure: {
    latched: boolean;
    reason?: string;
    since?: string;
    lastCheckedAt?: string;
  };
}

export interface QueuebitCapacitySnapshot {
  namespace: string;
  observedAt: string;
  queues: readonly QueuebitQueueCapacitySnapshot[];
}

export interface CapacityApi {
  snapshot(): Promise<QueuebitCapacitySnapshot>;
}

export interface HealthApi {
  snapshot(): HealthSnapshot;
}

export type QueuebitAlertSeverity = 'warning' | 'critical';

export interface QueuebitAlertFinding {
  id: string;
  severity: QueuebitAlertSeverity;
  message: string;
  observedAt: string;
  details: Record<string, unknown>;
}

export interface QueuebitAlertEvaluationOptions {
  queueUtilizationWarning?: number;
  queueUtilizationCritical?: number;
  completionFailedMinimum?: number;
  stalledRecoveryMinimum?: number;
}

export interface QueuebitAlertEvaluation {
  status: 'ok' | QueuebitAlertSeverity;
  observedAt: string;
  findings: readonly QueuebitAlertFinding[];
}

export interface AlertsApi {
  evaluate(options?: QueuebitAlertEvaluationOptions): Promise<QueuebitAlertEvaluation>;
}

export interface QueuebitObservabilityHttpRequest {
  path: string;
  method?: string;
}

export interface QueuebitObservabilityHttpResponse {
  status: number;
  headers: Record<string, string>;
  body: string;
}

export interface QueuebitObservabilityHttpOptions {
  healthPath?: string;
  metricsPath?: string;
}

export interface QueuebitObservabilityHttpApi {
  handle(request: QueuebitObservabilityHttpRequest): QueuebitObservabilityHttpResponse;
}

export interface QueuebitObservabilityRecorder {
  incrementCounter(suffix: string, value?: number, labels?: QueuebitMetricLabels): void;
  setGauge(suffix: string, value: number, labels?: QueuebitMetricLabels): void;
  observeDuration(suffix: string, durationMs: number, labels?: QueuebitMetricLabels): void;
}

export interface QueuebitObservabilityBackend {
  readonly health: HealthApi;
  readonly metrics: MetricsApi;
  readonly retention: RetentionApi;
  readonly capacity: CapacityApi;
  readonly alerts: AlertsApi;
  readonly observabilityHttp: QueuebitObservabilityHttpApi;
  readonly recorder: QueuebitObservabilityRecorder;
}

export interface QueuebitObservabilityBackendOptions {
  config: QueuebitConfig;
  redis: QueuebitRedisCommandClient;
  keys: ReturnType<typeof createQueuebitKeyBuilder>;
  now: () => Date;
  getPreflight: () => RedisPreflightResult | undefined;
  getClosing: () => boolean;
  getWorkerCount: () => number;
}

interface StoredMetricSample {
  name: string;
  type: QueuebitMetricType;
  value: number;
  labels: QueuebitMetricLabels;
}

export const noopQueuebitObservabilityRecorder: QueuebitObservabilityRecorder = {
  incrementCounter() {},
  setGauge() {},
  observeDuration() {}
};

export function createQueuebitObservabilityBackend(
  options: QueuebitObservabilityBackendOptions
): QueuebitObservabilityBackend {
  const registry = new QueuebitMetricsRegistry(options.config, options.now);
  const scripts = registerObservabilityScripts();
  const health: HealthApi = {
    snapshot() {
      return createHealthSnapshot(
        options.config,
        options.getPreflight(),
        options.getClosing(),
        options.now()
      );
    }
  };
  const metrics: MetricsApi = {
    collect() {
      return registry.collectBase(health.snapshot(), options.getWorkerCount());
    },
    renderPrometheus() {
      return renderPrometheus(metrics.collect());
    }
  };
  const retention: RetentionApi = {
    plan() {
      return createRetentionPlan(options.config, options.now());
    },
    purge(purgeOptions = {}) {
      return purgeRetention({
        config: options.config,
        redis: options.redis,
        keys: options.keys,
        now: options.now,
        script: scripts.purgeCompletedJob,
        terminalRunScript: scripts.purgeTerminalRun,
        completionEventScript: scripts.purgeCompletionEvent,
        options: purgeOptions
      });
    }
  };
  const capacity: CapacityApi = {
    async snapshot() {
      return createCapacitySnapshot(options.config, options.redis, options.keys, options.now());
    }
  };
  const alerts: AlertsApi = {
    evaluate(evaluationOptions = {}) {
      return evaluateAlerts({ health, metrics, capacity }, evaluationOptions, options.now());
    }
  };
  const observabilityHttp = createQueuebitObservabilityHttpApi({ health, metrics });
  return { health, metrics, retention, capacity, alerts, observabilityHttp, recorder: registry.recorder };
}

export function createQueuebitObservabilityHttpApi(
  target: Pick<QueuebitObservabilityBackend, 'health' | 'metrics'>,
  options: QueuebitObservabilityHttpOptions = {}
): QueuebitObservabilityHttpApi {
  const healthPath = normalizeRoutePath(options.healthPath ?? '/queuebit/health');
  const metricsPath = normalizeRoutePath(options.metricsPath ?? '/queuebit/metrics');
  return {
    handle(request) {
      const method = (request.method ?? 'GET').toUpperCase();
      const path = normalizeRequestPath(request.path);
      if (method !== 'GET' && method !== 'HEAD') {
        return jsonResponse(
          405,
          { error: 'method_not_allowed', allowedMethods: ['GET', 'HEAD'] },
          method,
          { allow: 'GET, HEAD' }
        );
      }
      if (path === healthPath) {
        return jsonResponse(200, target.health.snapshot(), method);
      }
      if (path === metricsPath) {
        return textResponse(
          200,
          target.metrics.renderPrometheus(),
          method,
          'text/plain; version=0.0.4; charset=utf-8'
        );
      }
      return jsonResponse(404, { error: 'not_found', path }, method);
    }
  };
}

async function evaluateAlerts(
  target: Pick<QueuebitObservabilityBackend, 'health' | 'metrics' | 'capacity'>,
  options: QueuebitAlertEvaluationOptions,
  observedAt: Date
): Promise<QueuebitAlertEvaluation> {
  const observedAtIso = observedAt.toISOString();
  const findings: QueuebitAlertFinding[] = [];
  const health = target.health.snapshot();
  if (!health.ready) {
    findings.push({
      id: 'health_not_ready',
      severity: 'critical',
      message: `Queuebit health is ${health.status}.`,
      observedAt: observedAtIso,
      details: { status: health.status, checks: health.checks }
    });
  } else if (health.status === 'degraded') {
    findings.push({
      id: 'health_degraded',
      severity: 'warning',
      message: 'Queuebit health is degraded.',
      observedAt: observedAtIso,
      details: { status: health.status, checks: health.checks }
    });
  }

  for (const sample of target.metrics.collect()) {
    if (sample.name.endsWith('completion_events_failed_total') && sample.value >= (options.completionFailedMinimum ?? 1)) {
      findings.push(metricFinding('completion_events_failed', 'critical', sample, observedAtIso));
    }
    if (sample.name.endsWith('worker_stalled_jobs_recovered_total') && sample.value >= (options.stalledRecoveryMinimum ?? 1)) {
      findings.push(metricFinding('worker_stalled_recoveries', 'warning', sample, observedAtIso));
    }
  }

  try {
    const capacity = await target.capacity.snapshot();
    const warningThreshold = normalizedRatioThreshold(options.queueUtilizationWarning, 0.8);
    const criticalThreshold = Math.max(
      warningThreshold,
      normalizedRatioThreshold(options.queueUtilizationCritical, 0.95)
    );
    for (const queue of capacity.queues) {
      if (queue.backpressure.latched) {
        findings.push({
          id: `queue_backpressure_latched:${queue.queue}`,
          severity: 'warning',
          message: `Queue "${queue.queue}" is backpressured.`,
          observedAt: observedAtIso,
          details: { queue: queue.queue, backpressure: queue.backpressure }
        });
      }
      collectUtilizationAlert(findings, observedAtIso, queue, 'jobs', warningThreshold, criticalThreshold);
      collectUtilizationAlert(findings, observedAtIso, queue, 'bytes', warningThreshold, criticalThreshold);
    }
  } catch (cause) {
    findings.push({
      id: 'capacity_snapshot_failed',
      severity: 'critical',
      message: 'Queuebit capacity snapshot failed during alert evaluation.',
      observedAt: observedAtIso,
      details: { error: serializeAlertError(cause) }
    });
  }

  return {
    status: summarizeAlertStatus(findings),
    observedAt: observedAtIso,
    findings: findings.sort((left, right) => left.id.localeCompare(right.id))
  };
}

function normalizeRetentionPurgeMode(mode: QueuebitRetentionPurgeOptions['mode']): QueuebitRetentionPurgeMode {
  if (mode === undefined) return 'dry-run';
  if (mode === 'dry-run' || mode === 'execute') return mode;
  throw new QueuebitError({
    code: 'QB_CONFIG_INVALID',
    message: 'retention.purge mode must be "dry-run" or "execute".',
    details: { mode }
  });
}

function normalizeRetentionPurgeLimit(limit: number | undefined): number {
  if (limit === undefined) return 100;
  if (Number.isInteger(limit) && limit >= 1 && limit <= 1_000) return limit;
  throw new QueuebitError({
    code: 'QB_CONFIG_INVALID',
    message: 'retention.purge limit must be an integer between 1 and 1000.',
    details: { limit }
  });
}

function normalizeRetentionPurgeReason(value: string): QueuebitRetentionPurgeReason {
  if (
    value === 'expired_by_age'
    || value === 'exceeds_max_count'
    || value === 'retained_by_window'
    || value === 'snapshot_missing'
    || value === 'state_protected'
    || value === 'completion_protected'
    || value === 'batchrun_owned'
    || value === 'tombstone_required'
    || value === 'details_expired'
    || value === 'updated_at_invalid'
    || value === 'execute_conflict'
  ) {
    return value;
  }
  return 'execute_conflict';
}

class QueuebitMetricsRegistry {
  readonly #config: QueuebitConfig;
  readonly #now: () => Date;
  readonly #samples = new Map<string, StoredMetricSample>();

  readonly recorder: QueuebitObservabilityRecorder = {
    incrementCounter: (suffix, value = 1, labels = {}) => {
      if (value === 0) return;
      this.#upsert('counter', suffix, labels, current => current + value);
    },
    setGauge: (suffix, value, labels = {}) => {
      this.#upsert('gauge', suffix, labels, () => value);
    },
    observeDuration: (suffix, durationMs, labels = {}) => {
      const safeDurationMs = Number.isFinite(durationMs) && durationMs >= 0 ? durationMs : 0;
      this.recorder.incrementCounter(`${suffix}_count`, 1, labels);
      this.recorder.incrementCounter(`${suffix}_sum`, safeDurationMs, labels);
    }
  };

  constructor(config: QueuebitConfig, now: () => Date) {
    this.#config = config;
    this.#now = now;
  }

  collectBase(health: HealthSnapshot, workers: number): QueuebitMetricSample[] {
    if (!this.#config.observability.metrics.enabled) return [];
    const timestamp = this.#now().toISOString();
    const samples: QueuebitMetricSample[] = [
      {
        name: this.#metricName('client_ready'),
        type: 'gauge',
        value: health.ready ? 1 : 0,
        labels: { namespace: this.#config.namespace, role: health.role },
        timestamp
      },
      {
        name: this.#metricName('workers_created'),
        type: 'gauge',
        value: workers,
        labels: { namespace: this.#config.namespace },
        timestamp
      },
      {
        name: this.#metricName('retention_completed_jobs_age_ms'),
        type: 'gauge',
        value: this.#config.retention.completedJobs.ageMs,
        labels: { namespace: this.#config.namespace },
        timestamp
      },
      {
        name: this.#metricName('retention_failed_work_age_ms'),
        type: 'gauge',
        value: this.#config.retention.failedWork.ageMs,
        labels: { namespace: this.#config.namespace },
        timestamp
      },
      {
        name: this.#metricName('retention_terminal_runs_age_ms'),
        type: 'gauge',
        value: this.#config.retention.terminalRuns.ageMs,
        labels: { namespace: this.#config.namespace },
        timestamp
      },
      {
        name: this.#metricName('retention_completion_events_age_ms'),
        type: 'gauge',
        value: this.#config.retention.completionEvents.ageMs,
        labels: { namespace: this.#config.namespace },
        timestamp
      }
    ];
    for (const [queue, queueConfig] of Object.entries(this.#config.queues).sort(([left], [right]) => left.localeCompare(right))) {
      const backpressure = queueConfig.backpressure;
      if (backpressure?.highWatermarkJobs !== undefined) {
        samples.push({
          name: this.#metricName('queue_high_watermark_jobs'),
          type: 'gauge',
          value: backpressure.highWatermarkJobs,
          labels: { namespace: this.#config.namespace, queue },
          timestamp
        });
      }
      if (backpressure?.highWatermarkBytes !== undefined) {
        samples.push({
          name: this.#metricName('queue_high_watermark_bytes'),
          type: 'gauge',
          value: backpressure.highWatermarkBytes,
          labels: { namespace: this.#config.namespace, queue },
          timestamp
        });
      }
    }
    const dynamicSamples = [...this.#samples.values()].map(sample => ({
      ...sample,
      labels: { ...sample.labels },
      timestamp
    }));
    return [...samples, ...dynamicSamples].sort(compareMetricSamples);
  }

  #upsert(
    type: QueuebitMetricType,
    suffix: string,
    labels: QueuebitMetricLabels,
    update: (current: number) => number
  ): void {
    if (!this.#config.observability.metrics.enabled) return;
    const sample = this.#sample(type, suffix, labels);
    sample.value = update(sample.value);
  }

  #sample(type: QueuebitMetricType, suffix: string, labels: QueuebitMetricLabels): StoredMetricSample {
    const normalizedLabels = normalizeLabels({ namespace: this.#config.namespace, ...labels });
    const name = this.#metricName(suffix);
    const key = metricKey(name, normalizedLabels);
    const existing = this.#samples.get(key);
    if (existing !== undefined) {
      return existing;
    }
    const created: StoredMetricSample = {
      name,
      type,
      value: 0,
      labels: normalizedLabels
    };
    this.#samples.set(key, created);
    return created;
  }

  #metricName(suffix: string): string {
    return `${this.#config.observability.metrics.prefix}${suffix}`;
  }
}

function createHealthSnapshot(
  config: QueuebitConfig,
  preflight: RedisPreflightResult | undefined,
  closing: boolean,
  observedAt: Date
): HealthSnapshot {
  const timestamp = observedAt.toISOString();
  if (closing) {
    return {
      status: 'draining',
      ready: false,
      role: 'producer',
      timestamp,
      checks: {
        lifecycle: { status: 'warn', observedAt: timestamp, message: 'Queuebit client is closing.' }
      }
    };
  }
  if (preflight === undefined) {
    return {
      status: 'ready',
      ready: true,
      role: 'producer',
      timestamp,
      checks: {
        preflight: {
          status: 'pass',
          observedAt: timestamp,
          message: 'Redis preflight was skipped for an injected command client.'
        }
      }
    };
  }
  return {
    status: preflight.status,
    ready: preflight.ready,
    role: 'producer',
    timestamp,
    checks: {
      preflight: {
        status: preflight.ready ? (preflight.status === 'degraded' ? 'warn' : 'pass') : 'fail',
        observedAt: timestamp,
        details: {
          namespace: config.namespace,
          issues: preflight.issues
        }
      }
    }
  };
}

function createRetentionPlan(config: QueuebitConfig, observedAt: Date): QueuebitRetentionPlan {
  return {
    namespace: config.namespace,
    observedAt: observedAt.toISOString(),
    completedJobs: {
      ...config.retention.completedJobs,
      tombstoneTtlMs: config.deduplication.jobKeyTtlMs,
      eligibleStates: ['completed'],
      protectedStates: ['waiting', 'delayed', 'retrying', 'active']
    },
    failedWork: {
      ...config.retention.failedWork,
      eligibleStates: ['failed mapper envelope', 'failed processor envelope'],
      protectedStates: ['retryable recovery envelope referenced by non-terminal Run']
    },
    terminalRuns: {
      ...config.retention.terminalRuns,
      tombstoneTtlMs: config.deduplication.runKeyTtlMs,
      eligibleStates: ['completed', 'partial_failed', 'failed', 'cancelled'],
      protectedStates: ['created', 'running', 'pausing', 'paused', 'blocked', 'cancelling']
    },
    completionEvents: {
      ...config.retention.completionEvents,
      tombstoneTtlMs: config.deduplication.runKeyTtlMs,
      eligibleStates: ['delivered', 'not_required'],
      protectedStates: ['pending', 'delivering', 'retrying', 'failed']
    },
    guards: [
      'never remove active/non-terminal jobs',
      'never remove non-terminal Runs',
      'never remove undelivered completion events',
      'never remove completion events whose parent Run is non-terminal',
      'keep job/run identity tombstones until deduplication TTL expires',
      'return recovery-data-expired instead of fabricating deleted payloads'
    ]
  };
}

async function purgeRetention(input: {
  config: QueuebitConfig;
  redis: QueuebitRedisCommandClient;
  keys: ReturnType<typeof createQueuebitKeyBuilder>;
  now: () => Date;
  script: ReturnType<typeof registerObservabilityScripts>['purgeCompletedJob'];
  terminalRunScript: ReturnType<typeof registerObservabilityScripts>['purgeTerminalRun'];
  completionEventScript: ReturnType<typeof registerObservabilityScripts>['purgeCompletionEvent'];
  options: QueuebitRetentionPurgeOptions;
}): Promise<QueuebitRetentionPurgeResult> {
  const mode = normalizeRetentionPurgeMode(input.options.mode);
  const limit = normalizeRetentionPurgeLimit(input.options.limit);
  const observedAt = input.now();
  const plan = createRetentionPlan(input.config, observedAt);
  const observedAtIso = observedAt.toISOString();
  const cutoffMs = observedAt.getTime() - input.config.retention.completedJobs.ageMs;
  const cutoffAt = new Date(cutoffMs).toISOString();
  const terminalCutoffMs = observedAt.getTime() - input.config.retention.terminalRuns.ageMs;
  const terminalCutoffAt = new Date(terminalCutoffMs).toISOString();
  const completionCutoffMs = observedAt.getTime() - input.config.retention.completionEvents.ageMs;
  const completionCutoffAt = new Date(completionCutoffMs).toISOString();
  const candidates: QueuebitRetentionPurgeCandidate[] = [];
  let hasMore = false;

  for (const queue of Object.keys(input.config.queues).sort()) {
    if (candidates.length >= limit) {
      hasMore = true;
      break;
    }

    const completedIndexKey = input.keys.queueState(queue, 'completed');
    const total = parseRedisInteger(await input.redis.sendCommand(['ZCARD', completedIndexKey]), 'ZCARD');
    const overCount = Math.max(total - input.config.retention.completedJobs.maxCount, 0);
    const readLimit = limit - candidates.length + 1;
    const reply = await input.redis.sendCommand([
      'ZRANGEBYSCORE',
      completedIndexKey,
      '-inf',
      '+inf',
      'WITHSCORES',
      'LIMIT',
      '0',
      String(readLimit)
    ]);
    const pairs = parseZrangeWithScores(reply);
    if (pairs.length > readLimit - 1) hasMore = true;

    for (const [index, pair] of pairs.slice(0, readLimit - 1).entries()) {
      const jobKey = input.keys.job(pair.member);
      const record = redisHashToRecord(await input.redis.sendCommand(['HGETALL', jobKey]));
      const evaluation = evaluateCompletedJobRetention(record, index < overCount, cutoffMs);
      if (!evaluation.eligible) {
        candidates.push({
          window: 'completedJobs',
          queue,
          jobId: pair.member,
          key: jobKey,
          score: pair.score,
          ...(record.state === undefined ? {} : { state: record.state }),
          ...(record.updatedAt === undefined ? {} : { updatedAt: record.updatedAt }),
          decision: 'skipped',
          reason: evaluation.reason
        });
        continue;
      }

      if (mode === 'dry-run') {
        candidates.push({
          window: 'completedJobs',
          queue,
          jobId: pair.member,
          key: jobKey,
          score: pair.score,
          ...(record.state === undefined ? {} : { state: record.state }),
          ...(record.updatedAt === undefined ? {} : { updatedAt: record.updatedAt }),
          decision: evaluation.action === 'tombstone' ? 'would_tombstone' : 'would_delete',
          reason: evaluation.reason
        });
        continue;
      }

      const executed = await executeRetentionCompletedJobPurge(input, queue, pair.member, jobKey, observedAtIso);
      candidates.push({
        window: 'completedJobs',
        queue,
        jobId: pair.member,
        key: jobKey,
        score: pair.score,
        ...(record.state === undefined ? {} : { state: record.state }),
        ...(record.updatedAt === undefined ? {} : { updatedAt: record.updatedAt }),
        decision: executed.decision,
        reason: executed.reason ?? evaluation.reason
      });
    }
  }

  if (candidates.length < limit) {
    const runsIndexKey = input.keys.runsTerminalDetails();
    const total = parseRedisInteger(await input.redis.sendCommand(['ZCARD', runsIndexKey]), 'ZCARD');
    const overCount = Math.max(total - input.config.retention.terminalRuns.maxCount, 0);
    const readLimit = limit - candidates.length + 1;
    const reply = await input.redis.sendCommand([
      'ZRANGEBYSCORE',
      runsIndexKey,
      '-inf',
      '+inf',
      'WITHSCORES',
      'LIMIT',
      '0',
      String(readLimit)
    ]);
    const pairs = parseZrangeWithScores(reply);
    if (pairs.length > readLimit - 1) hasMore = true;

    for (const [index, pair] of pairs.slice(0, readLimit - 1).entries()) {
      const runKey = input.keys.run(pair.member);
      const record = redisHashToRecord(await input.redis.sendCommand(['HGETALL', runKey]));
      const evaluation = evaluateTerminalRunRetention(record, index < overCount, terminalCutoffMs);
      if (!evaluation.eligible) {
        candidates.push({
          window: 'terminalRuns',
          runId: pair.member,
          key: runKey,
          score: pair.score,
          ...(record.executionState === undefined ? {} : { state: record.executionState }),
          ...(record.completionState === undefined ? {} : { completionState: record.completionState }),
          ...(record.updatedAt === undefined ? {} : { updatedAt: record.updatedAt }),
          decision: 'skipped',
          reason: evaluation.reason
        });
        continue;
      }

      if (mode === 'dry-run') {
        candidates.push({
          window: 'terminalRuns',
          runId: pair.member,
          key: runKey,
          score: pair.score,
          ...(record.executionState === undefined ? {} : { state: record.executionState }),
          ...(record.completionState === undefined ? {} : { completionState: record.completionState }),
          ...(record.updatedAt === undefined ? {} : { updatedAt: record.updatedAt }),
          decision: 'would_tombstone',
          reason: evaluation.reason
        });
        continue;
      }

      const executed = await executeRetentionTerminalRunPurge(input, pair.member, runKey, observedAtIso);
      candidates.push({
        window: 'terminalRuns',
        runId: pair.member,
        key: runKey,
        score: pair.score,
        ...(record.executionState === undefined ? {} : { state: record.executionState }),
        ...(record.completionState === undefined ? {} : { completionState: record.completionState }),
        ...(record.updatedAt === undefined ? {} : { updatedAt: record.updatedAt }),
        decision: executed.decision,
        reason: executed.reason ?? evaluation.reason
      });
    }
  }

  if (candidates.length < limit) {
    const completionsIndexKey = input.keys.completionsDetails();
    const total = parseRedisInteger(await input.redis.sendCommand(['ZCARD', completionsIndexKey]), 'ZCARD');
    const overCount = Math.max(total - input.config.retention.completionEvents.maxCount, 0);
    const readLimit = limit - candidates.length + 1;
    const reply = await input.redis.sendCommand([
      'ZRANGEBYSCORE',
      completionsIndexKey,
      '-inf',
      '+inf',
      'WITHSCORES',
      'LIMIT',
      '0',
      String(readLimit)
    ]);
    const pairs = parseZrangeWithScores(reply);
    if (pairs.length > readLimit - 1) hasMore = true;

    for (const [index, pair] of pairs.slice(0, readLimit - 1).entries()) {
      const completionKey = input.keys.completion(pair.member);
      const record = redisHashToRecord(await input.redis.sendCommand(['HGETALL', completionKey]));
      const runId = record.runId;
      const runRecord = runId === undefined
        ? {}
        : redisHashToRecord(await input.redis.sendCommand(['HGETALL', input.keys.run(runId)]));
      const evaluation = evaluateCompletionEventRetention(record, runRecord, index < overCount, completionCutoffMs);
      if (!evaluation.eligible) {
        candidates.push({
          window: 'completions',
          eventId: pair.member,
          key: completionKey,
          score: pair.score,
          ...(runId === undefined ? {} : { runId }),
          ...(record.type === undefined ? {} : { type: record.type }),
          ...(runRecord.executionState === undefined ? {} : { state: runRecord.executionState }),
          ...(record.completionState === undefined ? {} : { completionState: record.completionState }),
          ...(record.updatedAt === undefined ? {} : { updatedAt: record.updatedAt }),
          decision: 'skipped',
          reason: evaluation.reason
        });
        continue;
      }

      if (mode === 'dry-run') {
        candidates.push({
          window: 'completions',
          eventId: pair.member,
          key: completionKey,
          score: pair.score,
          ...(runId === undefined ? {} : { runId }),
          ...(record.type === undefined ? {} : { type: record.type }),
          ...(runRecord.executionState === undefined ? {} : { state: runRecord.executionState }),
          ...(record.completionState === undefined ? {} : { completionState: record.completionState }),
          ...(record.updatedAt === undefined ? {} : { updatedAt: record.updatedAt }),
          decision: 'would_tombstone',
          reason: evaluation.reason
        });
        continue;
      }

      const executed = await executeRetentionCompletionEventPurge(input, pair.member, completionKey, runId ?? '', observedAtIso);
      candidates.push({
        window: 'completions',
        eventId: pair.member,
        key: completionKey,
        score: pair.score,
        ...(runId === undefined ? {} : { runId }),
        ...(record.type === undefined ? {} : { type: record.type }),
        ...(runRecord.executionState === undefined ? {} : { state: runRecord.executionState }),
        ...(record.completionState === undefined ? {} : { completionState: record.completionState }),
        ...(record.updatedAt === undefined ? {} : { updatedAt: record.updatedAt }),
        decision: executed.decision,
        reason: executed.reason ?? evaluation.reason
      });
    }
  }

  const deleted = candidates.filter(candidate => candidate.decision === 'deleted').length;
  const tombstoned = candidates.filter(candidate => candidate.decision === 'tombstoned').length;
  const skipped = candidates.filter(candidate => candidate.decision === 'skipped').length;
  return {
    namespace: input.config.namespace,
    mode,
    observedAt: observedAtIso,
    windows: {
      completedJobs: {
        ageMs: input.config.retention.completedJobs.ageMs,
        maxCount: input.config.retention.completedJobs.maxCount,
        cutoffAt,
        eligibleStates: plan.completedJobs.eligibleStates,
        protectedStates: plan.completedJobs.protectedStates
      },
      terminalRuns: {
        ageMs: input.config.retention.terminalRuns.ageMs,
        maxCount: input.config.retention.terminalRuns.maxCount,
        cutoffAt: terminalCutoffAt,
        eligibleStates: plan.terminalRuns.eligibleStates,
        protectedStates: plan.terminalRuns.protectedStates
      },
      completions: {
        ageMs: input.config.retention.completionEvents.ageMs,
        maxCount: input.config.retention.completionEvents.maxCount,
        cutoffAt: completionCutoffAt,
        eligibleStates: plan.completionEvents.eligibleStates,
        protectedStates: plan.completionEvents.protectedStates
      }
    },
    scanned: candidates.length,
    deleted,
    tombstoned,
    skipped,
    hasMore,
    candidates
  };
}

function evaluateCompletedJobRetention(
  record: Record<string, string>,
  exceedsMaxCount: boolean,
  cutoffMs: number
): { eligible: boolean; reason: QueuebitRetentionPurgeReason; action?: 'delete' | 'tombstone' } {
  if (Object.keys(record).length === 0) return { eligible: false, reason: 'snapshot_missing' };
  if (record.state !== 'completed') return { eligible: false, reason: 'state_protected' };
  if (record.detailsExpired === '1') return { eligible: false, reason: 'details_expired' };
  if (record.runId !== undefined || record.batchId !== undefined) {
    return { eligible: false, reason: 'batchrun_owned' };
  }
  const action = (
    record.deduplicationKey !== undefined
    || record.idempotencyKey !== undefined
    || record.parentJobId !== undefined
  )
    ? 'tombstone'
    : 'delete';
  if (action === 'tombstone' && record.dataDigest === undefined) {
    return { eligible: false, reason: 'snapshot_missing' };
  }
  const updatedAtMs = record.updatedAt === undefined ? Number.NaN : Date.parse(record.updatedAt);
  if (!Number.isFinite(updatedAtMs)) return { eligible: false, reason: 'updated_at_invalid' };
  if (updatedAtMs <= cutoffMs) return { eligible: true, reason: 'expired_by_age', action };
  if (exceedsMaxCount) return { eligible: true, reason: 'exceeds_max_count', action };
  return { eligible: false, reason: 'retained_by_window' };
}

function evaluateTerminalRunRetention(
  record: Record<string, string>,
  exceedsMaxCount: boolean,
  cutoffMs: number
): { eligible: boolean; reason: QueuebitRetentionPurgeReason } {
  if (Object.keys(record).length === 0) return { eligible: false, reason: 'snapshot_missing' };
  if (record.detailsExpired === '1') return { eligible: false, reason: 'details_expired' };
  if (
    record.executionState !== 'completed'
    && record.executionState !== 'partial_failed'
    && record.executionState !== 'failed'
    && record.executionState !== 'cancelled'
  ) {
    return { eligible: false, reason: 'state_protected' };
  }
  if (record.completionState !== 'not_required' && record.completionState !== 'delivered') {
    return { eligible: false, reason: 'completion_protected' };
  }
  if (record.inputDigest === undefined) return { eligible: false, reason: 'snapshot_missing' };
  const updatedAtMs = record.updatedAt === undefined ? Number.NaN : Date.parse(record.updatedAt);
  if (!Number.isFinite(updatedAtMs)) return { eligible: false, reason: 'updated_at_invalid' };
  if (updatedAtMs <= cutoffMs) return { eligible: true, reason: 'expired_by_age' };
  if (exceedsMaxCount) return { eligible: true, reason: 'exceeds_max_count' };
  return { eligible: false, reason: 'retained_by_window' };
}

function evaluateCompletionEventRetention(
  record: Record<string, string>,
  runRecord: Record<string, string>,
  exceedsMaxCount: boolean,
  cutoffMs: number
): { eligible: boolean; reason: QueuebitRetentionPurgeReason } {
  if (Object.keys(record).length === 0) return { eligible: false, reason: 'snapshot_missing' };
  if (record.detailsExpired === '1') return { eligible: false, reason: 'details_expired' };
  if (record.completionState !== 'delivered' && record.completionState !== 'not_required') {
    return { eligible: false, reason: 'completion_protected' };
  }
  if (record.runId === undefined || Object.keys(runRecord).length === 0) {
    return { eligible: false, reason: 'snapshot_missing' };
  }
  if (
    runRecord.executionState !== 'completed'
    && runRecord.executionState !== 'partial_failed'
    && runRecord.executionState !== 'failed'
    && runRecord.executionState !== 'cancelled'
  ) {
    return { eligible: false, reason: 'state_protected' };
  }
  if (runRecord.completionState !== 'not_required' && runRecord.completionState !== 'delivered') {
    return { eligible: false, reason: 'completion_protected' };
  }
  if (record.summary === undefined) return { eligible: false, reason: 'snapshot_missing' };
  const updatedAtMs = record.updatedAt === undefined ? Number.NaN : Date.parse(record.updatedAt);
  if (!Number.isFinite(updatedAtMs)) return { eligible: false, reason: 'updated_at_invalid' };
  if (updatedAtMs <= cutoffMs) return { eligible: true, reason: 'expired_by_age' };
  if (exceedsMaxCount) return { eligible: true, reason: 'exceeds_max_count' };
  return { eligible: false, reason: 'retained_by_window' };
}

async function executeRetentionCompletedJobPurge(
  input: {
    redis: QueuebitRedisCommandClient;
    keys: ReturnType<typeof createQueuebitKeyBuilder>;
    script: ReturnType<typeof registerObservabilityScripts>['purgeCompletedJob'];
  },
  queue: string,
  jobId: string,
  jobKey: string,
  observedAtIso: string
): Promise<{ decision: 'deleted' | 'tombstoned' | 'skipped'; reason?: QueuebitRetentionPurgeReason }> {
  const reply = await executeQueuebitScript(
    input.redis,
    input.script,
    [
      jobKey,
      input.keys.queueJobs(queue),
      input.keys.queueState(queue, 'completed'),
      input.keys.queueCounters(queue)
    ],
    [jobId, observedAtIso]
  );
  if (!Array.isArray(reply)) return { decision: 'skipped', reason: 'execute_conflict' };
  if (reply[0] === 'deleted') return { decision: 'deleted' };
  if (reply[0] === 'tombstoned') return { decision: 'tombstoned' };
  if (reply[0] === 'skip') {
    return {
      decision: 'skipped',
      reason: normalizeRetentionPurgeReason(String(reply[1] ?? 'execute_conflict'))
    };
  }
  return { decision: 'skipped', reason: 'execute_conflict' };
}

async function executeRetentionTerminalRunPurge(
  input: {
    redis: QueuebitRedisCommandClient;
    keys: ReturnType<typeof createQueuebitKeyBuilder>;
    terminalRunScript: ReturnType<typeof registerObservabilityScripts>['purgeTerminalRun'];
  },
  runId: string,
  runKey: string,
  observedAtIso: string
): Promise<{ decision: 'tombstoned' | 'skipped'; reason?: QueuebitRetentionPurgeReason }> {
  const reply = await executeQueuebitScript(
    input.redis,
    input.terminalRunScript,
    [
      runKey,
      input.keys.failures(runId),
      input.keys.runsTerminalDetails()
    ],
    [runId, observedAtIso]
  );
  if (!Array.isArray(reply)) return { decision: 'skipped', reason: 'execute_conflict' };
  if (reply[0] === 'tombstoned') return { decision: 'tombstoned' };
  if (reply[0] === 'skip') {
    return {
      decision: 'skipped',
      reason: normalizeRetentionPurgeReason(String(reply[1] ?? 'execute_conflict'))
    };
  }
  return { decision: 'skipped', reason: 'execute_conflict' };
}

async function executeRetentionCompletionEventPurge(
  input: {
    redis: QueuebitRedisCommandClient;
    keys: ReturnType<typeof createQueuebitKeyBuilder>;
    completionEventScript: ReturnType<typeof registerObservabilityScripts>['purgeCompletionEvent'];
  },
  eventId: string,
  completionKey: string,
  runId: string,
  observedAtIso: string
): Promise<{ decision: 'tombstoned' | 'skipped'; reason?: QueuebitRetentionPurgeReason }> {
  const reply = await executeQueuebitScript(
    input.redis,
    input.completionEventScript,
    [
      completionKey,
      input.keys.run(runId),
      input.keys.completionsDue(),
      input.keys.completionsDetails()
    ],
    [eventId, observedAtIso]
  );
  if (!Array.isArray(reply)) return { decision: 'skipped', reason: 'execute_conflict' };
  if (reply[0] === 'tombstoned') return { decision: 'tombstoned' };
  if (reply[0] === 'skip') {
    return {
      decision: 'skipped',
      reason: normalizeRetentionPurgeReason(String(reply[1] ?? 'execute_conflict'))
    };
  }
  return { decision: 'skipped', reason: 'execute_conflict' };
}

async function createCapacitySnapshot(
  config: QueuebitConfig,
  redis: QueuebitRedisCommandClient,
  keys: ReturnType<typeof createQueuebitKeyBuilder>,
  observedAt: Date
): Promise<QueuebitCapacitySnapshot> {
  const timestamp = observedAt.toISOString();
  const queues = await Promise.all(
    Object.entries(config.queues)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(async ([queue, queueConfig]) => {
        const record = redisHashToRecord(await redis.sendCommand(['HGETALL', keys.queueCounters(queue)]));
        const counters = {
          queuedJobs: numberField(record, 'queuedJobs'),
          queuedBytes: numberField(record, 'queuedBytes'),
          waitingJobs: numberField(record, 'waitingJobs'),
          activeJobs: numberField(record, 'activeJobs'),
          delayedJobs: numberField(record, 'delayedJobs'),
          retryingJobs: numberField(record, 'retryingJobs'),
          completedJobs: numberField(record, 'completedJobs'),
          failedJobs: numberField(record, 'failedJobs'),
          cancelledJobs: numberField(record, 'cancelledJobs')
        };
        const watermarks = { ...(queueConfig.backpressure ?? {}) };
        return {
          queue,
          observedAt: timestamp,
          counters,
          watermarks,
          utilization: {
            ...(watermarks.highWatermarkJobs === undefined
              ? {}
              : { jobs: ratio(counters.queuedJobs, watermarks.highWatermarkJobs) }),
            ...(watermarks.highWatermarkBytes === undefined
              ? {}
              : { bytes: ratio(counters.queuedBytes, watermarks.highWatermarkBytes) })
          },
          backpressure: {
            latched: record.backpressureLatched === '1',
            ...(record.backpressureReason === undefined ? {} : { reason: record.backpressureReason }),
            ...(record.backpressureSince === undefined ? {} : { since: record.backpressureSince }),
            ...(record.backpressureLastCheckedAt === undefined
              ? {}
              : { lastCheckedAt: record.backpressureLastCheckedAt })
          }
        };
      })
  );
  return { namespace: config.namespace, observedAt: timestamp, queues };
}

function collectUtilizationAlert(
  findings: QueuebitAlertFinding[],
  observedAt: string,
  queue: QueuebitQueueCapacitySnapshot,
  dimension: 'jobs' | 'bytes',
  warningThreshold: number,
  criticalThreshold: number
): void {
  const utilization = queue.utilization[dimension];
  if (utilization === undefined || utilization < warningThreshold) return;
  const severity: QueuebitAlertSeverity = utilization >= criticalThreshold ? 'critical' : 'warning';
  findings.push({
    id: `queue_${dimension}_utilization_high:${queue.queue}`,
    severity,
    message: `Queue "${queue.queue}" ${dimension} utilization is ${utilization}.`,
    observedAt,
    details: {
      queue: queue.queue,
      dimension,
      utilization,
      counters: queue.counters,
      watermarks: queue.watermarks
    }
  });
}

function metricFinding(
  id: string,
  severity: QueuebitAlertSeverity,
  sample: QueuebitMetricSample,
  observedAt: string
): QueuebitAlertFinding {
  return {
    id: `${id}:${sample.name}${metricLabelSuffix(sample.labels)}`,
    severity,
    message: `Metric ${sample.name} reached ${sample.value}.`,
    observedAt,
    details: {
      metric: sample.name,
      value: sample.value,
      labels: sample.labels
    }
  };
}

function metricLabelSuffix(labels: QueuebitMetricLabels): string {
  const entries = Object.entries(labels).sort(([left], [right]) => left.localeCompare(right));
  if (entries.length === 0) return '';
  return `:${entries.map(([key, value]) => `${key}=${value}`).join(',')}`;
}

function summarizeAlertStatus(findings: readonly QueuebitAlertFinding[]): QueuebitAlertEvaluation['status'] {
  if (findings.some(finding => finding.severity === 'critical')) return 'critical';
  if (findings.some(finding => finding.severity === 'warning')) return 'warning';
  return 'ok';
}

function normalizeRoutePath(path: string): string {
  const withSlash = path.startsWith('/') ? path : `/${path}`;
  return withSlash.length > 1 && withSlash.endsWith('/') ? withSlash.slice(0, -1) : withSlash;
}

function normalizeRequestPath(path: string): string {
  try {
    return normalizeRoutePath(new URL(path, 'http://queuebit.local').pathname);
  } catch {
    return normalizeRoutePath(path.split(/[?#]/, 1)[0] ?? '/');
  }
}

function jsonResponse(
  status: number,
  payload: unknown,
  method: string,
  headers: Record<string, string> = {}
): QueuebitObservabilityHttpResponse {
  return textResponse(
    status,
    JSON.stringify(payload),
    method,
    'application/json; charset=utf-8',
    headers
  );
}

function textResponse(
  status: number,
  body: string,
  method: string,
  contentType: string,
  headers: Record<string, string> = {}
): QueuebitObservabilityHttpResponse {
  const responseBody = method === 'HEAD' ? '' : body;
  return {
    status,
    headers: {
      'cache-control': 'no-store',
      'content-type': contentType,
      ...headers
    },
    body: responseBody
  };
}

function serializeAlertError(cause: unknown): Record<string, unknown> {
  if (cause instanceof Error) return { name: cause.name, message: cause.message };
  return { message: String(cause) };
}

function renderPrometheus(samples: readonly QueuebitMetricSample[]): string {
  const typeLines = new Set<string>();
  const lines: string[] = [];
  for (const sample of samples) {
    const typeLine = `# TYPE ${sample.name} ${sample.type}`;
    if (!typeLines.has(typeLine)) {
      typeLines.add(typeLine);
      lines.push(typeLine);
    }
    const labels = renderLabels(sample.labels);
    lines.push(`${sample.name}${labels} ${sample.value}`);
  }
  return lines.join('\n');
}

function renderLabels(labels: QueuebitMetricLabels): string {
  const entries = Object.entries(labels).sort(([left], [right]) => left.localeCompare(right));
  if (entries.length === 0) return '';
  return `{${entries.map(([key, value]) => `${key}="${escapeLabel(value)}"`).join(',')}}`;
}

function escapeLabel(value: string): string {
  return value.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

function compareMetricSamples(left: QueuebitMetricSample, right: QueuebitMetricSample): number {
  const name = left.name.localeCompare(right.name);
  if (name !== 0) return name;
  return metricKey(left.name, left.labels).localeCompare(metricKey(right.name, right.labels));
}

function metricKey(name: string, labels: QueuebitMetricLabels): string {
  return `${name}\0${JSON.stringify(Object.entries(labels).sort(([left], [right]) => left.localeCompare(right)))}`;
}

function normalizeLabels(labels: QueuebitMetricLabels): QueuebitMetricLabels {
  return Object.fromEntries(
    Object.entries(labels)
      .filter(([, value]) => value.length > 0)
      .sort(([left], [right]) => left.localeCompare(right))
  );
}

function redisHashToRecord(value: unknown): Record<string, string> {
  if (Array.isArray(value)) {
    const record: Record<string, string> = {};
    for (let index = 0; index < value.length; index += 2) {
      record[String(value[index])] = String(value[index + 1]);
    }
    return record;
  }
  if (value !== null && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, entry]) => [key, String(entry)])
    );
  }
  return {};
}

function parseZrangeWithScores(reply: unknown): Array<{ member: string; score: string }> {
  if (!Array.isArray(reply)) {
    throw new QueuebitError({
      code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
      message: 'Redis returned an invalid retention index reply.',
      details: { reply }
    });
  }
  const pairs: Array<{ member: string; score: string }> = [];
  for (let index = 0; index < reply.length - 1; index += 2) {
    pairs.push({ member: String(reply[index]), score: String(reply[index + 1]) });
  }
  return pairs;
}

function parseRedisInteger(reply: unknown, operation: string): number {
  const value = Number(reply);
  if (Number.isInteger(value) && value >= 0) return value;
  throw new QueuebitError({
    code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
    message: `Redis returned an invalid integer for ${operation}.`,
    details: { reply }
  });
}

function numberField(record: Record<string, string>, field: string): number {
  const value = Number.parseInt(record[field] ?? '0', 10);
  return Number.isFinite(value) ? value : 0;
}

function ratio(current: number, high: number): number {
  if (high <= 0) return 0;
  return Number((current / high).toFixed(6));
}

function normalizedRatioThreshold(value: number | undefined, fallback: number): number {
  if (value === undefined) return fallback;
  return Number.isFinite(value) && value >= 0 ? value : fallback;
}
