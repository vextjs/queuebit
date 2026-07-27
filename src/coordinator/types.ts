import type { QueuebitConfig } from '../config';
import type { JobAddOptions } from '../jobs';
import type { QueuebitObservabilityRecorder } from '../observability';
import type { QueuebitRedisCommandClient } from '../redis';
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
