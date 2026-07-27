export type RunExecutionState =
  | 'created'
  | 'running'
  | 'pausing'
  | 'paused'
  | 'blocked'
  | 'cancelling'
  | 'completed'
  | 'partial_failed'
  | 'failed'
  | 'cancelled';

export type CompletionState =
  | 'not_created'
  | 'not_required'
  | 'pending'
  | 'delivering'
  | 'retrying'
  | 'delivered'
  | 'failed';

export interface RunStartRequest<Input = unknown> {
  input: Input;
  idempotencyKey: string;
}

export interface RunCancelRequest {
  reason: string;
}

export interface RunRetryFailedRequest {
  idempotencyKey: string;
  definitionVersion?: number;
}

export type FailureStage = 'mapper' | 'processor';

export interface FailureListQuery {
  stage?: FailureStage;
  cursor?: string;
  limit?: number;
  includePayload?: boolean;
}

export interface FailureRecord<Payload = unknown> {
  sequence: string;
  runId: string;
  stage: FailureStage;
  recordIdentity: string;
  attempt: number;
  error: {
    name?: string;
    code?: string;
    message: string;
    details?: unknown;
  };
  recoveryAvailable: boolean;
  batchId?: string;
  jobId?: string;
  envelopeExpiresAt?: string;
  payload?: Payload;
}

export interface RunListQuery {
  definition?: string;
  executionState?: RunExecutionState;
  cursor?: string;
  limit?: number;
}

export interface CompletionSummary {
  recordsSeen: number;
  recordsDispatched: number;
  recordsSkipped: number;
  recordsFailed: number;
  recordsUndispatched: number;
  boundaryTotalRecords: number | null;
  jobsCreated: number;
  jobsCompleted: number;
  jobsFailed: number;
  jobsCancelled: number;
}

export interface RunSummary extends CompletionSummary {
  id: string;
  definition: string;
  definitionVersion: number;
  recoveryDepth: number;
  executionState: RunExecutionState;
  completionState: CompletionState;
  checkpointBatchIndex: number;
  createdAt: string;
  updatedAt: string;
  inputDigest?: string;
  detailsExpired?: true;
  detailsExpiredAt?: string;
  failureDetailsExpired?: true;
  parentRunId?: string;
}

export interface RunSnapshot<Input = unknown, Boundary = unknown, Cursor = unknown>
  extends RunSummary {
  input?: Input;
  boundary?: Boundary | null;
  dispatchCursor?: Cursor | null;
  checkpointCursor?: Cursor | null;
  sourceExhausted: boolean;
  inFlightBatches: number;
  nextDispatchAt?: string;
  pauseRequestedAt?: string;
  pausedAt?: string;
  resumedAt?: string;
  cancelReason?: string;
  cancelRequestedAt?: string;
  cancelledAt?: string;
  dispatchHoldReason?:
    | 'interval'
    | 'in_flight_limit'
    | 'backpressure'
    | 'no_active_worker'
    | 'redis_reconnecting';
}

export interface RunStartResult<Input = unknown, Boundary = unknown, Cursor = unknown>
  extends RunSnapshot<Input, Boundary, Cursor> {
  deduplicated: boolean;
}

export interface CursorPage<T> {
  items: T[];
  nextCursor?: string;
}

export interface RunsApi {
  start<Input>(definition: string, request: RunStartRequest<Input>): Promise<RunStartResult<Input>>;
  get<Input = unknown, Boundary = unknown, Cursor = unknown>(
    runId: string
  ): Promise<RunSnapshot<Input, Boundary, Cursor> | null>;
  list(query?: RunListQuery): Promise<CursorPage<RunSummary>>;
  listFailures<Payload = unknown>(
    runId: string,
    query?: FailureListQuery
  ): Promise<CursorPage<FailureRecord<Payload>>>;
  pause<Input = unknown, Boundary = unknown, Cursor = unknown>(
    runId: string
  ): Promise<RunSnapshot<Input, Boundary, Cursor>>;
  resume<Input = unknown, Boundary = unknown, Cursor = unknown>(
    runId: string
  ): Promise<RunSnapshot<Input, Boundary, Cursor>>;
  cancel<Input = unknown, Boundary = unknown, Cursor = unknown>(
    runId: string,
    request: RunCancelRequest
  ): Promise<RunSnapshot<Input, Boundary, Cursor>>;
  retryFailed<Input = unknown, Boundary = unknown, Cursor = unknown>(
    runId: string,
    request: RunRetryFailedRequest
  ): Promise<RunSnapshot<Input, Boundary, Cursor>>;
}
