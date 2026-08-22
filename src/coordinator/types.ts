import type { QueuebitConfig } from '../config';
import type { JobAddOptions, QueuebitSerializedError } from '../jobs';
import type { QueuebitObservabilityRecorder } from '../observability';
import type { QueuebitRedisCommandClient } from '../redis';
import type { QueuebitRoleSnapshot, QueuebitRolesApi } from '../roles';
import type { QueuebitRuntimeDefinition } from '../runtime';

export interface QueuebitCoordinatorOptions {
  config: QueuebitConfig;
  redis: QueuebitRedisCommandClient;
  runtime: QueuebitRuntimeDefinition;
  coordinatorId?: string;
  leaseMs?: number;
  sourceTimeoutMs?: number;
  observability?: QueuebitObservabilityRecorder;
  now?: () => Date;
  idGenerator?: () => string;
}

export interface QueuebitCoordinatorAdvanceOptions {
  signal?: AbortSignal;
}

export interface QueuebitCompletionDeliveryOptions {
  limit?: number;
  signal?: AbortSignal;
}

export interface QueuebitCompletionDeliveryResult {
  claimed: number;
  delivered: number;
  retrying: number;
  failed: number;
  skipped: number;
  eventIds: string[];
}

export type QueuebitCoordinatorAdvanceStatus =
  | 'dispatched'
  | 'source_exhausted'
  | 'waiting_for_batch'
  | 'paused'
  | 'already_terminal';

export interface QueuebitCoordinatorAdvanceResult {
  status: QueuebitCoordinatorAdvanceStatus;
  runId: string;
  definition: string;
  coordinatorId: string;
  coordinatorGeneration?: number;
  batchId?: string;
  batchIndex?: number;
  recordsSeen: number;
  recordsDispatched: number;
  recordsSkipped: number;
  recordsFailed: number;
  jobsCreated: number;
  sourceExhausted: boolean;
  dispatchCursor: unknown;
}

export interface QueuebitCoordinator {
  readonly coordinatorId: string;
  advanceRun(runId: string, options?: QueuebitCoordinatorAdvanceOptions): Promise<QueuebitCoordinatorAdvanceResult>;
  deliverDueCompletions(options?: QueuebitCompletionDeliveryOptions): Promise<QueuebitCompletionDeliveryResult>;
}

export type QueuebitCoordinatorRunnerStatus = 'idle' | 'running' | 'draining' | 'stopped';

export interface QueuebitCoordinatorRunnerError {
  operation: 'heartbeat' | 'completion_delivery' | 'advance';
  occurredAt: string;
  error: QueuebitSerializedError;
  runId?: string;
}

export interface QueuebitCoordinatorRunnerStatusSnapshot {
  status: QueuebitCoordinatorRunnerStatus;
  coordinatorId: string;
  activeRuns: number;
  startedAt?: string;
  drainingSince?: string;
  stoppedAt?: string;
  lastError?: QueuebitCoordinatorRunnerError;
  role?: QueuebitRoleSnapshot;
}

export interface QueuebitCoordinatorRunnerDrainOptions {
  timeoutMs?: number;
}

export interface QueuebitCoordinatorRunnerOptions {
  config: QueuebitConfig;
  redis: QueuebitRedisCommandClient;
  runtime: QueuebitRuntimeDefinition;
  coordinatorId?: string;
  leaseMs?: number;
  sourceTimeoutMs?: number;
  concurrency?: number;
  pollIntervalMs?: number;
  completionLimit?: number;
  domain?: string;
  heartbeatIntervalMs?: number;
  heartbeatTtlMs?: number;
  drainTimeoutMs?: number;
  roleRegistry?: QueuebitRolesApi;
  observability?: QueuebitObservabilityRecorder;
  now?: () => Date;
  onError?: (event: QueuebitCoordinatorRunnerError) => void;
}

export interface QueuebitCoordinatorRunner {
  readonly coordinatorId: string;
  start(): void;
  status(): QueuebitCoordinatorRunnerStatusSnapshot;
  drain(options?: QueuebitCoordinatorRunnerDrainOptions): Promise<void>;
  stop(options?: QueuebitCoordinatorRunnerDrainOptions): Promise<void>;
}

export interface QueuebitInternalPreparedBatchJob {
  jobId: string;
  jobKey: string;
  dedupeKey: string;
  envelope: {
    jobId: string;
    queue: string;
    name: string;
    state: 'waiting' | 'delayed';
    attempts: number;
    createdAt: string;
    updatedAt: string;
    dataJson: string;
    dataDigest: string;
    dataBytes: number;
    optionsJson: string;
    delayUntilMs: number;
    deduplicationKey: string;
    idempotencyKey: string;
    recordIdentity: string;
    runId: string;
    batchId: string;
  };
  options: JobAddOptions;
}
