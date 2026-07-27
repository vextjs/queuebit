export type JobState =
  | 'waiting'
  | 'active'
  | 'delayed'
  | 'retrying'
  | 'completed'
  | 'failed'
  | 'cancelled';

export interface CursorPage<T> {
  items: T[];
  nextCursor?: string;
}

export interface JobAddOptions {
  attempts?: number;
  timeoutMs?: number;
  backoff?: {
    type: 'fixed' | 'exponential';
    delayMs: number;
    maxDelayMs?: number;
    jitter?: number;
  };
  delayMs?: number;
  deduplicationKey?: string;
  idempotencyKey?: string;
}

export interface BulkJobEntry<Data = unknown> {
  name: string;
  data: Data;
  options?: JobAddOptions;
}

export interface JobListQuery {
  queue: string;
  state?: JobState;
  cursor?: string;
  limit?: number;
}

export interface JobRetryFailedRequest {
  deduplicationKey: string;
}

export interface JobSummary {
  id: string;
  queue: string;
  name: string;
  state: JobState;
  attempt: number;
  attempts: number;
  runId?: string;
  batchId?: string;
  parentJobId?: string;
  createdAt: string;
  updatedAt: string;
  dataDigest?: string;
  detailsExpired?: true;
  detailsExpiredAt?: string;
}

export interface QueuebitSerializedError {
  name?: string;
  code?: string;
  message: string;
  details?: unknown;
}

export interface JobSnapshot<Data = unknown, Result = unknown> extends JobSummary {
  data?: Data;
  result?: Result;
  deduplicationKey?: string;
  idempotencyKey?: string;
  failedReason?: QueuebitSerializedError;
}

export interface JobsApi {
  add<Data>(
    queue: string,
    name: string,
    data: Data,
    options?: JobAddOptions
  ): Promise<JobSnapshot<Data>>;
  addBulk<Data>(queue: string, entries: BulkJobEntry<Data>[]): Promise<Array<JobSnapshot<Data>>>;
  get<Data = unknown, Result = unknown>(jobId: string): Promise<JobSnapshot<Data, Result> | null>;
  list(query: JobListQuery): Promise<CursorPage<JobSummary>>;
  cancel(jobId: string): Promise<JobSnapshot>;
  retryFailed(jobId: string, request: JobRetryFailedRequest): Promise<JobSnapshot>;
}
