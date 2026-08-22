import type { QueuebitConfig } from '../config';
import { QueuebitError } from '../errors';
import type { QueuebitSerializedError } from '../jobs';
import {
  noopQueuebitObservabilityRecorder,
  type QueuebitObservabilityRecorder
} from '../observability';
import type { QueuebitRoleSnapshot, QueuebitRolesApi } from '../roles';
import { createQueuebitRunsApi, type RunsApi } from '../runs';
import { createQueuebitCoordinator } from './api';
import type {
  QueuebitCoordinator,
  QueuebitCoordinatorRunner,
  QueuebitCoordinatorRunnerDrainOptions,
  QueuebitCoordinatorRunnerError,
  QueuebitCoordinatorRunnerOptions,
  QueuebitCoordinatorRunnerStatus,
  QueuebitCoordinatorRunnerStatusSnapshot
} from './types';

interface RunnerConfig {
  concurrency: number;
  completionLimit: number;
  domain: string;
  pollIntervalMs: number;
  heartbeatIntervalMs: number;
  heartbeatTtlMs: number;
  drainTimeoutMs: number;
}

type Timer = ReturnType<typeof setTimeout>;
type Interval = ReturnType<typeof setInterval>;

const terminalRunStates = new Set(['completed', 'partial_failed', 'failed', 'cancelled']);

/**
 * Runs the normal coordinator loop from application code. It intentionally has
 * no process or signal side effects: the application owns its service host and
 * chooses when to call stop().
 */
export function createQueuebitCoordinatorRunner(
  options: QueuebitCoordinatorRunnerOptions
): QueuebitCoordinatorRunner {
  return new QueuebitCoordinatorRunnerImpl(options);
}

class QueuebitCoordinatorRunnerImpl implements QueuebitCoordinatorRunner {
  readonly coordinatorId: string;

  readonly #coordinator: QueuebitCoordinator;
  readonly #runs: RunsApi;
  readonly #runnerConfig: RunnerConfig;
  readonly #roleRegistry: QueuebitRolesApi | undefined;
  readonly #observability: QueuebitObservabilityRecorder;
  readonly #now: () => Date;
  readonly #onError: ((event: QueuebitCoordinatorRunnerError) => void) | undefined;
  readonly #activeRuns = new Set<Promise<void>>();
  readonly #drainWaiters = new Set<() => void>();

  #status: QueuebitCoordinatorRunnerStatus = 'idle';
  #startedAt: string | undefined;
  #drainingSince: string | undefined;
  #stoppedAt: string | undefined;
  #lastError: QueuebitCoordinatorRunnerError | undefined;
  #roleSnapshot: QueuebitRoleSnapshot | undefined;
  #tickTimer: Timer | undefined;
  #heartbeatTimer: Interval | undefined;
  #tickPromise: Promise<void> | undefined;
  #drainPromise: Promise<void> | undefined;

  constructor(options: QueuebitCoordinatorRunnerOptions) {
    this.#now = options.now ?? (() => new Date());
    this.#runnerConfig = normalizeRunnerConfig(options);
    this.#roleRegistry = options.roleRegistry;
    this.#observability = options.observability ?? noopQueuebitObservabilityRecorder;
    this.#onError = options.onError;
    this.#coordinator = createQueuebitCoordinator({
      config: options.config,
      redis: options.redis,
      runtime: options.runtime,
      ...(options.coordinatorId === undefined ? {} : { coordinatorId: options.coordinatorId }),
      ...(options.leaseMs === undefined ? {} : { leaseMs: options.leaseMs }),
      ...(options.sourceTimeoutMs === undefined ? {} : { sourceTimeoutMs: options.sourceTimeoutMs }),
      observability: this.#observability,
      now: this.#now
    });
    this.coordinatorId = this.#coordinator.coordinatorId;
    this.#runs = createQueuebitRunsApi({ config: options.config, redis: options.redis, now: this.#now });
  }

  start(): void {
    if (this.#status === 'running') return;
    if (this.#status === 'draining') {
      throw new QueuebitError({
        code: 'QB_COORDINATOR_STATE_CONFLICT',
        message: 'Cannot start a CoordinatorRunner while it is draining.',
        details: { coordinatorId: this.coordinatorId }
      });
    }
    this.#status = 'running';
    this.#startedAt = this.#now().toISOString();
    this.#drainingSince = undefined;
    this.#stoppedAt = undefined;
    this.#lastError = undefined;
    this.#observability.setGauge('coordinator_running', 1, this.#runnerLabels());
    void this.#heartbeat('running').catch(cause => this.#recordError('heartbeat', cause));
    this.#scheduleHeartbeat();
    this.#scheduleTick(0);
  }

  status(): QueuebitCoordinatorRunnerStatusSnapshot {
    const snapshot: QueuebitCoordinatorRunnerStatusSnapshot = {
      status: this.#status,
      coordinatorId: this.coordinatorId,
      activeRuns: this.#activeRuns.size
    };
    assignOptional(snapshot, 'startedAt', this.#startedAt);
    assignOptional(snapshot, 'drainingSince', this.#drainingSince);
    assignOptional(snapshot, 'stoppedAt', this.#stoppedAt);
    assignOptional(snapshot, 'lastError', this.#lastError);
    assignOptional(snapshot, 'role', this.#roleSnapshot);
    return snapshot;
  }

  drain(options: QueuebitCoordinatorRunnerDrainOptions = {}): Promise<void> {
    if (this.#drainPromise !== undefined) return this.#drainPromise;
    const task = this.#drainInternal(options);
    this.#drainPromise = task;
    void task.finally(() => {
      if (this.#drainPromise === task) this.#drainPromise = undefined;
    }).catch(() => undefined);
    return task;
  }

  stop(options: QueuebitCoordinatorRunnerDrainOptions = {}): Promise<void> {
    return this.drain(options);
  }

  #scheduleTick(delayMs: number): void {
    if (this.#status !== 'running' || this.#tickTimer !== undefined) return;
    this.#tickTimer = setTimeout(() => {
      this.#tickTimer = undefined;
      const task = this.#tick().catch(cause => this.#recordError('advance', cause));
      this.#tickPromise = task;
      void task.finally(() => {
        if (this.#tickPromise === task) this.#tickPromise = undefined;
        this.#notifyDrainWaiters();
        if (this.#status === 'running') this.#scheduleTick(this.#runnerConfig.pollIntervalMs);
      }).catch(cause => this.#recordError('advance', cause));
    }, delayMs);
  }

  async #tick(): Promise<void> {
    if (this.#status !== 'running') return;
    try {
      await this.#coordinator.deliverDueCompletions({ limit: this.#runnerConfig.completionLimit });
    } catch (cause) {
      this.#recordError('completion_delivery', cause);
    }
    if (this.#status !== 'running') return;

    let page;
    try {
      page = await this.#runs.list({ limit: Math.max(1, Math.min(100, this.#runnerConfig.concurrency * 4)) });
    } catch (cause) {
      this.#recordError('advance', cause);
      return;
    }
    const runnable = page.items
      .filter(run => !terminalRunStates.has(run.executionState))
      .slice(0, this.#runnerConfig.concurrency);
    await Promise.all(runnable.map(run => this.#trackAdvance(run.id)));
  }

  #trackAdvance(runId: string): Promise<void> {
    const task = this.#coordinator.advanceRun(runId)
      .then(() => undefined)
      .catch(cause => this.#recordError('advance', cause, runId))
      .finally(() => {
        this.#activeRuns.delete(task);
        this.#observability.setGauge('coordinator_active_runs', this.#activeRuns.size, this.#runnerLabels());
        this.#notifyDrainWaiters();
      });
    this.#activeRuns.add(task);
    this.#observability.setGauge('coordinator_active_runs', this.#activeRuns.size, this.#runnerLabels());
    return task;
  }

  #scheduleHeartbeat(): void {
    if (this.#roleRegistry === undefined || this.#heartbeatTimer !== undefined) return;
    this.#heartbeatTimer = setInterval(() => {
      if (this.#status !== 'running') return;
      void this.#heartbeat('running').catch(cause => this.#recordError('heartbeat', cause));
    }, this.#runnerConfig.heartbeatIntervalMs);
  }

  async #heartbeat(status: 'running' | 'draining'): Promise<void> {
    if (this.#roleRegistry === undefined) return;
    const result = await this.#roleRegistry.heartbeat({
      role: 'coordinator',
      domain: this.#runnerConfig.domain,
      identity: this.coordinatorId,
      status,
      heartbeatTtlMs: this.#runnerConfig.heartbeatTtlMs,
      ...(this.#startedAt === undefined ? {} : { startedAt: this.#startedAt }),
      metadata: {
        activeRuns: this.#activeRuns.size,
        concurrency: this.#runnerConfig.concurrency
      }
    });
    this.#roleSnapshot = result.snapshot;
    this.#observability.incrementCounter('role_heartbeats_total', 1, {
      role: 'coordinator',
      domain: this.#runnerConfig.domain,
      identity: this.coordinatorId,
      status
    });
    if (result.drainRequested && this.#status === 'running') {
      this.#observability.incrementCounter('role_drain_requests_observed_total', 1, this.#runnerLabels());
      void this.drain({ timeoutMs: this.#runnerConfig.drainTimeoutMs })
        .catch(cause => this.#recordError('heartbeat', cause));
    }
  }

  async #drainInternal(options: QueuebitCoordinatorRunnerDrainOptions): Promise<void> {
    if (this.#status === 'idle' || this.#status === 'stopped') {
      this.#status = 'stopped';
      this.#stoppedAt = this.#now().toISOString();
      this.#observability.setGauge('coordinator_running', 0, this.#runnerLabels());
      await this.#unregisterRole();
      return;
    }
    if (this.#status === 'running') {
      this.#status = 'draining';
      this.#drainingSince = this.#now().toISOString();
      this.#clearTickTimer();
      this.#clearHeartbeatTimer();
      try {
        await this.#heartbeat('draining');
      } catch (cause) {
        this.#recordError('heartbeat', cause);
      }
    }
    const timeoutMs = normalizePositiveInteger(
      'timeoutMs',
      options.timeoutMs ?? this.#runnerConfig.drainTimeoutMs
    );
    try {
      await this.#waitForIdle(timeoutMs);
    } catch (cause) {
      this.#recordError('advance', cause);
      throw cause;
    }
    this.#status = 'stopped';
    this.#stoppedAt = this.#now().toISOString();
    this.#observability.setGauge('coordinator_running', 0, this.#runnerLabels());
    try {
      await this.#unregisterRole();
    } catch (cause) {
      this.#recordError('heartbeat', cause);
      throw cause;
    }
  }

  #waitForIdle(timeoutMs: number): Promise<void> {
    if (this.#tickPromise === undefined && this.#activeRuns.size === 0) return Promise.resolve();
    return new Promise((resolve, reject) => {
      let timeout: Timer | undefined;
      const done = () => {
        if (timeout !== undefined) clearTimeout(timeout);
        this.#drainWaiters.delete(done);
        resolve();
      };
      timeout = setTimeout(() => {
        this.#drainWaiters.delete(done);
        reject(new QueuebitError({
          code: 'QB_COORDINATOR_DRAIN_TIMEOUT',
          message: 'CoordinatorRunner drain timed out before active work settled.',
          details: {
            coordinatorId: this.coordinatorId,
            activeRuns: this.#activeRuns.size,
            tickActive: this.#tickPromise !== undefined
          }
        }));
      }, timeoutMs);
      this.#drainWaiters.add(done);
    });
  }

  #notifyDrainWaiters(): void {
    if (this.#tickPromise !== undefined || this.#activeRuns.size > 0) return;
    for (const waiter of [...this.#drainWaiters]) waiter();
  }

  #clearTickTimer(): void {
    if (this.#tickTimer === undefined) return;
    clearTimeout(this.#tickTimer);
    this.#tickTimer = undefined;
  }

  #clearHeartbeatTimer(): void {
    if (this.#heartbeatTimer === undefined) return;
    clearInterval(this.#heartbeatTimer);
    this.#heartbeatTimer = undefined;
  }

  async #unregisterRole(): Promise<void> {
    this.#clearHeartbeatTimer();
    if (this.#roleRegistry === undefined) return;
    await this.#roleRegistry.unregister({
      role: 'coordinator',
      domain: this.#runnerConfig.domain,
      identity: this.coordinatorId
    });
  }

  #recordError(
    operation: QueuebitCoordinatorRunnerError['operation'],
    cause: unknown,
    runId?: string
  ): void {
    const event: QueuebitCoordinatorRunnerError = {
      operation,
      occurredAt: this.#now().toISOString(),
      error: serializeFailure(cause),
      ...(runId === undefined ? {} : { runId })
    };
    this.#lastError = event;
    this.#observability.incrementCounter('coordinator_errors_total', 1, {
      ...this.#runnerLabels(),
      operation
    });
    if (this.#onError === undefined) return;
    try {
      this.#onError(event);
    } catch {
      this.#observability.incrementCounter(
        'coordinator_error_callback_failures_total',
        1,
        { ...this.#runnerLabels(), operation }
      );
    }
  }

  #runnerLabels(): Record<string, string> {
    return { coordinatorId: this.coordinatorId, domain: this.#runnerConfig.domain };
  }
}

function normalizeRunnerConfig(options: QueuebitCoordinatorRunnerOptions): RunnerConfig {
  const scheduler = options.config.scheduler;
  const config: RunnerConfig = {
    concurrency: normalizePositiveInteger('concurrency', options.concurrency ?? 1),
    completionLimit: normalizeCompletionLimit(options.completionLimit ?? 25),
    domain: normalizeSegment('domain', options.domain ?? scheduler.domain),
    pollIntervalMs: normalizePositiveInteger('pollIntervalMs', options.pollIntervalMs ?? scheduler.pollIntervalMs),
    heartbeatIntervalMs: normalizePositiveInteger(
      'heartbeatIntervalMs',
      options.heartbeatIntervalMs ?? scheduler.heartbeatIntervalMs
    ),
    heartbeatTtlMs: normalizePositiveInteger(
      'heartbeatTtlMs',
      options.heartbeatTtlMs ?? scheduler.heartbeatTtlMs
    ),
    drainTimeoutMs: normalizePositiveInteger(
      'drainTimeoutMs',
      options.drainTimeoutMs ?? scheduler.drainTimeoutMs
    )
  };
  if (config.heartbeatTtlMs <= config.heartbeatIntervalMs) {
    throw new QueuebitError({
      code: 'QB_COORDINATOR_INVALID',
      message: 'heartbeatTtlMs must be greater than heartbeatIntervalMs.',
      details: {
        heartbeatIntervalMs: config.heartbeatIntervalMs,
        heartbeatTtlMs: config.heartbeatTtlMs
      }
    });
  }
  return config;
}

function normalizePositiveInteger(label: string, value: number): number {
  if (!Number.isInteger(value) || value < 1) {
    throw new QueuebitError({
      code: 'QB_COORDINATOR_INVALID',
      message: `${label} must be an integer >= 1.`,
      details: { label, value }
    });
  }
  return value;
}

function normalizeCompletionLimit(value: number): number {
  if (!Number.isInteger(value) || value < 1 || value > 100) {
    throw new QueuebitError({
      code: 'QB_COORDINATOR_INVALID',
      message: 'completionLimit must be an integer between 1 and 100.',
      details: { completionLimit: value }
    });
  }
  return value;
}

function normalizeSegment(label: string, value: string): string {
  if (!/^[A-Za-z0-9][A-Za-z0-9:_-]{0,191}$/.test(value)) {
    throw new QueuebitError({
      code: 'QB_COORDINATOR_INVALID',
      message: `Invalid ${label}: "${value}".`,
      details: { label, value }
    });
  }
  return value;
}

function serializeFailure(cause: unknown): QueuebitSerializedError {
  if (cause instanceof QueuebitError) {
    const failure: QueuebitSerializedError = {
      name: cause.name,
      code: cause.code,
      message: cause.message
    };
    if (cause.details !== undefined) failure.details = cause.details;
    return failure;
  }
  if (cause instanceof Error) return { name: cause.name, message: cause.message };
  if (typeof cause === 'string') return { name: 'Error', message: cause };
  return { name: 'Error', message: 'CoordinatorRunner failed.', details: cause };
}

function assignOptional<T extends object, K extends keyof T>(target: T, key: K, value: T[K] | undefined): void {
  if (value !== undefined) target[key] = value;
}
