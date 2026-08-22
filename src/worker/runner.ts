import type { QueuebitConfig } from '../config';
import { QueuebitError } from '../errors';
import type { QueuebitSerializedError } from '../jobs';
import {
  noopQueuebitObservabilityRecorder,
  type QueuebitObservabilityRecorder
} from '../observability';
import type { QueuebitRoleSnapshot, QueuebitRolesApi } from '../roles';
import type { QueuebitRedisCommandClient } from '../redis';
import { createQueuebitWorkerKernel } from './api';
import type {
  ClaimedJob,
  QueuebitWorker,
  QueuebitWorkerDrainOptions,
  QueuebitWorkerProcessor,
  QueuebitWorkerProcessorContext,
  QueuebitWorkerStatus,
  QueuebitWorkerStatusSnapshot,
  WorkerKernel
} from './types';

export interface QueuebitWorkerOptions<Data = unknown, Result = unknown> {
  config: QueuebitConfig;
  redis: QueuebitRedisCommandClient;
  queue: string;
  processor: QueuebitWorkerProcessor<Data, Result>;
  workerId?: string;
  concurrency?: number;
  leaseMs?: number;
  renewIntervalMs?: number;
  pollIntervalMs?: number;
  drainTimeoutMs?: number;
  timeoutMs?: number;
  heartbeatIntervalMs?: number;
  heartbeatTtlMs?: number;
  roleRegistry?: QueuebitRolesApi;
  observability?: QueuebitObservabilityRecorder;
  now?: () => Date;
}

interface RunnerConfig {
  concurrency: number;
  leaseMs: number;
  renewIntervalMs: number;
  pollIntervalMs: number;
  drainTimeoutMs: number;
  timeoutMs?: number;
  heartbeatIntervalMs: number;
  heartbeatTtlMs: number;
}

type Timer = ReturnType<typeof setTimeout>;
type Interval = ReturnType<typeof setInterval>;

export function createQueuebitWorker<Data = unknown, Result = unknown>(
  options: QueuebitWorkerOptions<Data, Result>
): QueuebitWorker {
  return new QueuebitWorkerRunner(options);
}

class QueuebitWorkerRunner<Data = unknown, Result = unknown> implements QueuebitWorker {
  readonly queue: string;
  readonly workerId: string;

  readonly #kernel: WorkerKernel;
  readonly #processor: QueuebitWorkerProcessor<Data, Result>;
  readonly #runnerConfig: RunnerConfig;
  readonly #roleRegistry: QueuebitRolesApi | undefined;
  readonly #observability: QueuebitObservabilityRecorder;
  readonly #now: () => Date;
  readonly #inFlight = new Set<Promise<void>>();
  readonly #controllers = new Set<AbortController>();
  readonly #drainWaiters = new Set<() => void>();

  #status: QueuebitWorkerStatus = 'idle';
  #startedAt: string | undefined;
  #drainingSince: string | undefined;
  #stoppedAt: string | undefined;
  #lastError: QueuebitSerializedError | undefined;
  #roleSnapshot: QueuebitRoleSnapshot | undefined;
  #pumpTimer: Timer | undefined;
  #heartbeatTimer: Interval | undefined;
  #pumping = false;

  constructor(options: QueuebitWorkerOptions<Data, Result>) {
    this.queue = options.queue;
    this.#now = options.now ?? (() => new Date());
    this.#processor = options.processor;
    this.#runnerConfig = normalizeRunnerConfig(options);
    this.#roleRegistry = options.roleRegistry;
    this.#observability = options.observability ?? noopQueuebitObservabilityRecorder;
    this.#kernel = createQueuebitWorkerKernel({
      config: options.config,
      redis: options.redis,
      queue: options.queue,
      ...(options.workerId === undefined ? {} : { workerId: options.workerId }),
      now: this.#now
    });
    this.workerId = this.#kernel.workerId;
  }

  start(): void {
    if (this.#status === 'running') return;
    if (this.#status === 'draining') {
      throw new QueuebitError({
        code: 'QB_WORKER_STATE_CONFLICT',
        message: 'Cannot start a Worker while it is draining.',
        details: { queue: this.queue, workerId: this.workerId }
      });
    }
    this.#status = 'running';
    this.#startedAt = this.#now().toISOString();
    this.#stoppedAt = undefined;
    this.#drainingSince = undefined;
    this.#lastError = undefined;
    this.#observability.setGauge('worker_running', 1, this.#workerLabels());
    void this.#heartbeat('running').catch(cause => {
      this.#lastError = serializeFailure(cause);
    });
    this.#scheduleHeartbeat();
    this.#schedulePump(0);
  }

  status(): QueuebitWorkerStatusSnapshot {
    const snapshot: QueuebitWorkerStatusSnapshot = {
      status: this.#status,
      queue: this.queue,
      workerId: this.workerId,
      activeJobs: this.#inFlight.size
    };
    assignOptional(snapshot, 'startedAt', this.#startedAt);
    assignOptional(snapshot, 'drainingSince', this.#drainingSince);
    assignOptional(snapshot, 'stoppedAt', this.#stoppedAt);
    assignOptional(snapshot, 'lastError', this.#lastError);
    assignOptional(snapshot, 'role', this.#roleSnapshot);
    return snapshot;
  }

  async drain(options: QueuebitWorkerDrainOptions = {}): Promise<void> {
    if (this.#status === 'idle' || this.#status === 'stopped') {
      this.#status = 'stopped';
      this.#stoppedAt = this.#now().toISOString();
      this.#observability.setGauge('worker_running', 0, this.#workerLabels());
      await this.#unregisterRole();
      return;
    }
    if (this.#status === 'running') {
      this.#status = 'draining';
      this.#drainingSince = this.#now().toISOString();
      this.#clearPumpTimer();
      this.#clearHeartbeatTimer();
      await this.#heartbeat('draining');
    }
    const timeoutMs = normalizePositiveInteger(
      'timeoutMs',
      options.timeoutMs ?? this.#runnerConfig.drainTimeoutMs
    );
    await this.#waitForInFlight(timeoutMs);
    this.#status = 'stopped';
    this.#stoppedAt = this.#now().toISOString();
    this.#observability.setGauge('worker_running', 0, this.#workerLabels());
    await this.#unregisterRole();
  }

  async stop(options: QueuebitWorkerDrainOptions = {}): Promise<void> {
    await this.drain(options);
  }

  #schedulePump(delayMs: number): void {
    if (this.#status !== 'running') return;
    if (this.#pumpTimer !== undefined) return;
    this.#pumpTimer = setTimeout(() => {
      this.#pumpTimer = undefined;
      void this.#pump();
    }, delayMs);
  }

  async #pump(): Promise<void> {
    if (this.#pumping || this.#status !== 'running') return;
    this.#pumping = true;
    let claimedAny = false;
    try {
      while (this.#status === 'running' && this.#inFlight.size < this.#runnerConfig.concurrency) {
        await this.#advanceTime();
        const job = await this.#kernel.claim<Data>({
          leaseMs: this.#runnerConfig.leaseMs
        });
        if (job === null) break;
        claimedAny = true;
        this.#observability.incrementCounter('worker_jobs_claimed_total', 1, this.#workerLabels());
        this.#observability.incrementCounter('worker_job_attempts_total', job.attempt, this.#workerLabels());
        this.#trackJob(job);
      }
    } catch (cause) {
      this.#lastError = serializeFailure(cause);
    } finally {
      this.#pumping = false;
      if (this.#status === 'running') {
        this.#schedulePump(claimedAny ? 0 : this.#runnerConfig.pollIntervalMs);
      }
    }
  }

  async #advanceTime(): Promise<void> {
    try {
      const promoted = await this.#kernel.promoteDue();
      if (promoted.length > 0) {
        this.#observability.incrementCounter('worker_due_jobs_promoted_total', promoted.length, this.#workerLabels());
      }
      const recovered = await this.#kernel.recoverStalled();
      if (recovered.length > 0) {
        this.#observability.incrementCounter('worker_stalled_jobs_recovered_total', recovered.length, this.#workerLabels());
      }
    } catch (cause) {
      this.#lastError = serializeFailure(cause);
    }
  }

  #trackJob(job: ClaimedJob<Data>): void {
    const task = this.#processJob(job)
      .catch(cause => {
        this.#lastError = serializeFailure(cause);
      })
      .finally(() => {
        this.#inFlight.delete(task);
        this.#observability.setGauge('worker_active_jobs', this.#inFlight.size, this.#workerLabels());
        this.#notifyDrainWaiters();
        if (this.#status === 'running') this.#schedulePump(0);
      });
    this.#inFlight.add(task);
    this.#observability.setGauge('worker_active_jobs', this.#inFlight.size, this.#workerLabels());
  }

  async #processJob(job: ClaimedJob<Data>): Promise<void> {
    const startedAtMs = this.#now().getTime();
    const controller = new AbortController();
    this.#controllers.add(controller);
    const timeoutTimer = this.#createTimeoutTimer(controller);
    const renewTimer = this.#createRenewTimer(job, controller);
    try {
      const context = createProcessorContext(job, this.queue, this.workerId, controller.signal);
      const result = await this.#processor(job, context);
      await this.#kernel.complete<Data, Result>(job.id, job.leaseGeneration, result);
      this.#observability.incrementCounter('worker_jobs_completed_total', 1, this.#workerLabels());
    } catch (cause) {
      this.#lastError = serializeFailure(cause);
      try {
        await this.#kernel.fail<Data>(job.id, job.leaseGeneration, serializeFailure(cause));
        this.#observability.incrementCounter('worker_jobs_failed_total', 1, this.#workerLabels());
      } catch (settleCause) {
        this.#lastError = serializeFailure(settleCause);
      }
    } finally {
      this.#observability.observeDuration(
        'worker_job_duration_ms',
        this.#now().getTime() - startedAtMs,
        this.#workerLabels()
      );
      clearInterval(renewTimer);
      if (timeoutTimer !== undefined) clearTimeout(timeoutTimer);
      this.#controllers.delete(controller);
    }
  }

  #createRenewTimer(job: ClaimedJob<Data>, controller: AbortController): Interval {
    return setInterval(() => {
      if (controller.signal.aborted) return;
      void this.#kernel.renew<Data>(job.id, job.leaseGeneration, {
        leaseMs: this.#runnerConfig.leaseMs
      }).catch(cause => {
        this.#lastError = serializeFailure(cause);
        controller.abort(createAbortReason('lease-renew-failed', cause));
      });
    }, this.#runnerConfig.renewIntervalMs);
  }

  #createTimeoutTimer(controller: AbortController): Timer | undefined {
    const timeoutMs = this.#runnerConfig.timeoutMs;
    if (timeoutMs === undefined) return undefined;
    return setTimeout(() => {
      controller.abort(createAbortReason('processor-timeout'));
    }, timeoutMs);
  }

  #waitForInFlight(timeoutMs: number): Promise<void> {
    if (this.#inFlight.size === 0) return Promise.resolve();
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
          code: 'QB_WORKER_DRAIN_TIMEOUT',
          message: 'Worker drain timed out before active jobs settled.',
          details: { queue: this.queue, workerId: this.workerId, activeJobs: this.#inFlight.size }
        }));
      }, timeoutMs);
      this.#drainWaiters.add(done);
    });
  }

  #notifyDrainWaiters(): void {
    if (this.#inFlight.size > 0) return;
    for (const waiter of [...this.#drainWaiters]) waiter();
  }

  #clearPumpTimer(): void {
    if (this.#pumpTimer === undefined) return;
    clearTimeout(this.#pumpTimer);
    this.#pumpTimer = undefined;
  }

  #scheduleHeartbeat(): void {
    if (this.#roleRegistry === undefined || this.#heartbeatTimer !== undefined) return;
    this.#heartbeatTimer = setInterval(() => {
      if (this.#status !== 'running') return;
      void this.#heartbeat('running').catch(cause => {
        this.#lastError = serializeFailure(cause);
      });
    }, this.#runnerConfig.heartbeatIntervalMs);
  }

  async #heartbeat(status: 'running' | 'draining'): Promise<void> {
    if (this.#roleRegistry === undefined) return;
    const result = await this.#roleRegistry.heartbeat({
      role: 'worker',
      domain: this.queue,
      identity: this.workerId,
      status,
      heartbeatTtlMs: this.#runnerConfig.heartbeatTtlMs,
      ...(this.#startedAt === undefined ? {} : { startedAt: this.#startedAt }),
      metadata: {
        activeJobs: this.#inFlight.size,
        concurrency: this.#runnerConfig.concurrency
      }
    });
    this.#observability.incrementCounter('role_heartbeats_total', 1, {
      role: 'worker',
      domain: this.queue,
      identity: this.workerId,
      status
    });
    this.#roleSnapshot = result.snapshot;
    if (result.drainRequested && this.#status === 'running') {
      this.#observability.incrementCounter('role_drain_requests_observed_total', 1, this.#workerLabels());
      void this.drain({ timeoutMs: this.#runnerConfig.drainTimeoutMs }).catch(cause => {
        this.#lastError = serializeFailure(cause);
      });
    }
  }

  #workerLabels(): Record<string, string> {
    return { queue: this.queue, workerId: this.workerId };
  }

  async #unregisterRole(): Promise<void> {
    this.#clearHeartbeatTimer();
    if (this.#roleRegistry === undefined) return;
    await this.#roleRegistry.unregister({
      role: 'worker',
      domain: this.queue,
      identity: this.workerId
    });
  }

  #clearHeartbeatTimer(): void {
    if (this.#heartbeatTimer === undefined) return;
    clearInterval(this.#heartbeatTimer);
    this.#heartbeatTimer = undefined;
  }
}

function normalizeRunnerConfig<Data, Result>(options: QueuebitWorkerOptions<Data, Result>): RunnerConfig {
  const workerDefaults = options.config.workerDefaults;
  const config: RunnerConfig = {
    concurrency: normalizePositiveInteger('concurrency', options.concurrency ?? workerDefaults.concurrency),
    leaseMs: normalizePositiveInteger('leaseMs', options.leaseMs ?? workerDefaults.leaseMs),
    renewIntervalMs: normalizePositiveInteger(
      'renewIntervalMs',
      options.renewIntervalMs ?? workerDefaults.renewIntervalMs
    ),
    pollIntervalMs: normalizePositiveInteger(
      'pollIntervalMs',
      options.pollIntervalMs ?? workerDefaults.pollIntervalMs
    ),
    drainTimeoutMs: normalizePositiveInteger(
      'drainTimeoutMs',
      options.drainTimeoutMs ?? workerDefaults.drainTimeoutMs
    ),
    heartbeatIntervalMs: normalizePositiveInteger(
      'heartbeatIntervalMs',
      options.heartbeatIntervalMs ?? workerDefaults.heartbeatIntervalMs
    ),
    heartbeatTtlMs: normalizePositiveInteger(
      'heartbeatTtlMs',
      options.heartbeatTtlMs ?? workerDefaults.heartbeatTtlMs
    )
  };
  if (config.heartbeatTtlMs <= config.heartbeatIntervalMs) {
    throw new QueuebitError({
      code: 'QB_WORKER_INVALID',
      message: 'heartbeatTtlMs must be greater than heartbeatIntervalMs.',
      details: {
        heartbeatIntervalMs: config.heartbeatIntervalMs,
        heartbeatTtlMs: config.heartbeatTtlMs
      }
    });
  }
  if (options.timeoutMs !== undefined) {
    config.timeoutMs = normalizePositiveInteger('timeoutMs', options.timeoutMs);
  }
  return config;
}

function normalizePositiveInteger(label: string, value: number): number {
  if (!Number.isInteger(value) || value < 1) {
    throw new QueuebitError({
      code: 'QB_WORKER_INVALID',
      message: `${label} must be an integer >= 1.`,
      details: { label, value }
    });
  }
  return value;
}

function createProcessorContext(
  job: ClaimedJob,
  queue: string,
  workerId: string,
  signal: AbortSignal
): QueuebitWorkerProcessorContext {
  const context: QueuebitWorkerProcessorContext = {
    queue,
    workerId,
    jobId: job.id,
    attempt: job.attempt,
    signal
  };
  if (job.idempotencyKey !== undefined) context.idempotencyKey = job.idempotencyKey;
  return context;
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
  if (cause instanceof Error) {
    return { name: cause.name, message: cause.message };
  }
  if (typeof cause === 'string') {
    return { name: 'Error', message: cause };
  }
  return { name: 'Error', message: 'Worker processor failed.', details: cause };
}

function createAbortReason(reason: string, cause?: unknown): QueuebitSerializedError {
  const failure: QueuebitSerializedError = {
    name: 'AbortError',
    code: reason,
    message: `Worker processor aborted: ${reason}.`
  };
  if (cause !== undefined) failure.details = serializeFailure(cause);
  return failure;
}

function assignOptional<T extends object, K extends keyof T>(target: T, key: K, value: T[K] | undefined) {
  if (value !== undefined) target[key] = value;
}
