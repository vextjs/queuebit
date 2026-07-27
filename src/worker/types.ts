import type { QueuebitSerializedError, JobSnapshot } from '../jobs';
import type { QueuebitRoleSnapshot } from '../roles';

export type QueuebitWorkerStatus = 'idle' | 'running' | 'draining' | 'stopped';

export interface WorkerClaimOptions {
  leaseMs?: number;
  maxSkips?: number;
}

export interface WorkerRenewOptions {
  leaseMs?: number;
}

export interface WorkerPromoteDueOptions {
  limit?: number;
}

export interface WorkerRecoverStalledOptions {
  limit?: number;
  maxStalledRecoveries?: number;
}

export interface ClaimedJob<Data = unknown> extends JobSnapshot<Data> {
  state: 'active';
  data: Data;
  detailsExpired?: never;
  workerId: string;
  leaseGeneration: number;
  leaseDeadlineAt: string;
}

export interface QueuebitWorkerProcessorContext {
  queue: string;
  workerId: string;
  jobId: string;
  attempt: number;
  idempotencyKey?: string;
  signal: AbortSignal;
}

export type QueuebitWorkerProcessor<Data = unknown, Result = unknown> = (
  job: ClaimedJob<Data>,
  context: QueuebitWorkerProcessorContext
) => Promise<Result> | Result;

export interface QueuebitWorkerStatusSnapshot {
  status: QueuebitWorkerStatus;
  queue: string;
  workerId: string;
  activeJobs: number;
  startedAt?: string;
  drainingSince?: string;
  stoppedAt?: string;
  lastError?: QueuebitSerializedError;
  role?: QueuebitRoleSnapshot;
}

export interface QueuebitWorkerDrainOptions {
  timeoutMs?: number;
}

export interface QueuebitWorker {
  readonly queue: string;
  readonly workerId: string;
  start(): void;
  status(): QueuebitWorkerStatusSnapshot;
  drain(options?: QueuebitWorkerDrainOptions): Promise<void>;
  stop(options?: QueuebitWorkerDrainOptions): Promise<void>;
}

export interface WorkerKernel {
  readonly queue: string;
  readonly workerId: string;
  claim<Data = unknown>(options?: WorkerClaimOptions): Promise<ClaimedJob<Data> | null>;
  renew<Data = unknown>(
    jobId: string,
    leaseGeneration: number,
    options?: WorkerRenewOptions
  ): Promise<ClaimedJob<Data>>;
  complete<Data = unknown, Result = unknown>(
    jobId: string,
    leaseGeneration: number,
    result?: Result
  ): Promise<JobSnapshot<Data, Result>>;
  fail<Data = unknown>(
    jobId: string,
    leaseGeneration: number,
    error: QueuebitSerializedError | Error | string
  ): Promise<JobSnapshot<Data>>;
  promoteDue(options?: WorkerPromoteDueOptions): Promise<string[]>;
  recoverStalled(options?: WorkerRecoverStalledOptions): Promise<string[]>;
}
