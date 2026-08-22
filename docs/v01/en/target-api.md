# API quick lookup

<span class="manual-label">Reference · find methods by task</span>

Use this page when you already need a method name or return shape. For first integration, start with [Quick start](./quick-start.md): create a client, pass Redis config, register a processor, create a Worker from application code, then enqueue one normal job with `jobs.add`. `runs.start` is the database batch entrypoint and `createCoordinatorRunner` advances it from application code. CLI commands are optional operations tooling, not the normal runtime path.

## Find the API by task

| Need | Use these methods |
|---|---|
| Create a Queuebit client | `createQueuebitClient(config, options?)` or `createQueuebitClient({ config, logger? })` |
| Start a normal Worker from service code | `queuebit.createWorker(queue, createQueuebitRuntimeProcessor(runtime), options).start()` |
| Start a BatchRun Coordinator from service code | `queuebit.createCoordinatorRunner(runtime, options).start()` |
| Enqueue one background job | `queuebit.jobs.add(queue, name, data, options?)` |
| Enqueue many background jobs | `queuebit.jobs.addBulk(entries)` |
| Inspect, cancel, or retry Jobs | `queuebit.jobs.get/list/cancel/retryFailed` |
| Process many database records | `queuebit.runs.start(definition, { input, idempotencyKey })` |
| Inspect, pause, resume, or cancel Runs | `queuebit.runs.get/list/pause/resume/cancel` |
| Retry failed Run work | `queuebit.runs.listFailures/retryFailed` |
| Inspect or retry completion callbacks | `queuebit.completions.get/list/retry` |
| Wire health and metrics | `queuebit.health.snapshot()` and `queuebit.metrics.renderPrometheus()` |
| Check queue watermarks and local alerts | `queuebit.capacity.snapshot()` and `queuebit.alerts.evaluate()` |

## Create one long-lived client

```ts
import { createQueuebitClient } from 'queuebit';

const queuebit = await createQueuebitClient({
  connection: { url: process.env.QUEUEBIT_REDIS_URL },
  queues: { notification: {} }
}, { logger });
```

Without an explicit `namespace`, Queuebit derives the application namespace from `QUEUEBIT_NAMESPACE` or the nearest `package.json` name.

| Method | Semantics | Error/boundary |
|---|---|---|
| `createQueuebitClient(config, options?)` | Create one application-level long-lived client from an ordinary config object | Reject static config or Redis preflight failure |
| `createQueuebitClient({config, logger?})` | Compatible options form for one application-level long-lived client | Reject static config or Redis preflight failure |
| `queuebit.createWorker(queue, processor, options?)` | Construct a Worker owned by this client | Call `start()` from the application's Worker host |
| `queuebit.createCoordinatorRunner(runtime, options?)` | Construct a BatchRun CoordinatorRunner owned by this client | Call `start()` only in a Coordinator host; it is not needed for direct jobs |
| `queuebit.close()` | Release this client's resources | Call at script end or application `onClose` |
| `queuebit.health.snapshot()` | Return a `HealthSnapshot` | Does not pretend to be cluster health |
| `queuebit.metrics.collect()` | Return structured samples for this process | Does not pretend to be a cluster aggregate |
| `queuebit.metrics.renderPrometheus()` | Render Prometheus text for this process | Core starts no HTTP server |
| `queuebit.observabilityHttp.handle(request)` | Return a health or metrics HTTP response object | No listener; the application mounts/authenticates it |
| `queuebit.alerts.evaluate(options?)` | Return local alert findings from health, metrics, and capacity | Starting points, not a global incident engine |
| `queuebit.retention.plan()` | Inspect the history cleanup plan | Read-only; does not delete Redis data |
| `queuebit.retention.purge(options?)` | Preview or run history cleanup | Defaults to dry-run; execute only cleans safely finished history details |
| `queuebit.capacity.snapshot()` | Read declared queue counters and watermarks | Read-only; does not scan arbitrary keys |

Do not close the client after each HTTP request or each add/start. A vext plugin closes once in `onClose`.

## Start background services from application code

```ts
import {
  createQueuebitClient,
  createQueuebitRuntimeProcessor
} from 'queuebit';
import config from './queuebit.config.js';
import runtime from './queuebit.runtime.js';

const queuebit = await createQueuebitClient({ config });

const worker = queuebit.createWorker(
  'notification',
  createQueuebitRuntimeProcessor(runtime),
  { workerId: 'worker-a', concurrency: 8, drainTimeoutMs: 60_000 }
);
worker.start();

// Only create this in the BatchRun Coordinator service host.
const coordinator = queuebit.createCoordinatorRunner(runtime, {
  coordinatorId: 'coordinator-a',
  concurrency: 2,
  onError: event => console.error('Queuebit coordinator error', event)
});
coordinator.start();

// Call from your framework lifecycle or process-manager shutdown hook.
await queuebit.close({ timeoutMs: 60_000 });
```

Use separate Worker and Coordinator service hosts in production; the snippet places both calls together only to show the symmetric API. The host owns process signals. Queuebit has no import-time side effects and does not register a signal handler.

`QueuebitCoordinatorRunner` has `idle`, `running`, `draining`, and `stopped` states. `start()` begins the heartbeat and one polling loop. Each tick delivers up to `completionLimit` due completion events, lists runnable Runs, and advances up to `concurrency` Runs. `status()` exposes `activeRuns`, the role snapshot, and `lastError`; `onError` receives the same heartbeat, completion-delivery, or advance failure immediately.

| CoordinatorRunner option | Default | Meaning |
|---|---:|---|
| `coordinatorId` | generated | Stable role identity for heartbeats and remote drain |
| `concurrency` | 1 | Max runnable Runs advanced per tick |
| `leaseMs` / `sourceTimeoutMs` | 30000 / 30000 | Run-claim lease and one source load timeout |
| `pollIntervalMs` | `scheduler.pollIntervalMs` | Delay before the next tick |
| `completionLimit` | 25 | Due completion events per tick; integer 1 through 100 |
| `domain` | `scheduler.domain` | Role heartbeat scope |
| `heartbeatIntervalMs` / `heartbeatTtlMs` | scheduler values | TTL must be greater than interval |
| `drainTimeoutMs` | `scheduler.drainTimeoutMs` | Default wait for `drain()` / `stop()` |

`drain()` stops new polling, writes a draining role heartbeat, and waits for current work. A remote `coordinator drain` command is observed through that heartbeat. If the deadline expires, it throws `QB_COORDINATOR_DRAIN_TIMEOUT` and the runner remains `draining`; retry `stop()` once active work has settled. Retry that runner directly before calling `queuebit.close()`: client close is terminal cleanup, drains client-created CoordinatorRunners before Workers, and closes an owned Redis connection even when it reports a cleanup failure.

<a id="public-api-contract"></a>
## Public input and return types

The following types are the input and return shapes users see directly. `list` returns summaries without business `data/input/result`; `get` returns a complete snapshot. All timestamps are ISO 8601 UTC strings.

```ts
type JobState =
  | 'waiting' | 'active' | 'delayed' | 'retrying'
  | 'completed' | 'failed' | 'cancelled';

type RunExecutionState =
  | 'created' | 'running' | 'pausing' | 'paused'
  | 'blocked' | 'cancelling'
  | 'completed' | 'partial_failed' | 'failed' | 'cancelled';

type CompletionState =
  | 'not_created' | 'not_required' | 'pending' | 'delivering'
  | 'retrying' | 'delivered' | 'failed';

type HealthStatus = 'ready' | 'degraded' | 'not_ready' | 'draining';

type HealthCheck = {
  status: 'pass' | 'warn' | 'fail';
  message?: string;
  observedAt: string;
  details?: Record<string, unknown>;
};

type HealthSnapshot = {
  status: HealthStatus;
  ready: boolean;
  role: 'producer' | 'worker' | 'coordinator';
  identity?: string;
  timestamp: string;
  checks: Record<string, HealthCheck>;
};

type QueuebitMetricSample = {
  name: string;
  type: 'counter' | 'gauge' | 'histogram';
  value: number;
  labels: Record<string, string>;
  timestamp?: string;
};

type QueuebitRetentionPlan = {
  namespace: string;
  observedAt: string;
  completedJobs: {
    ageMs: number;
    maxCount: number;
    tombstoneTtlMs: number;
    eligibleStates: string[];
    protectedStates: string[];
  };
  failedWork: {
    ageMs: number;
    maxCount: number;
    eligibleStates: string[];
    protectedStates: string[];
  };
  terminalRuns: {
    ageMs: number;
    maxCount: number;
    tombstoneTtlMs: number;
    eligibleStates: string[];
    protectedStates: string[];
  };
  completionEvents: {
    ageMs: number;
    maxCount: number;
    tombstoneTtlMs: number;
    eligibleStates: string[];
    protectedStates: string[];
  };
  guards: string[];
};

type QueuebitRetentionPurgeMode = 'dry-run' | 'execute';
type QueuebitRetentionPurgeWindow = 'completedJobs' | 'terminalRuns' | 'completions';
type QueuebitRetentionPurgeDecision =
  | 'would_delete'
  | 'would_tombstone'
  | 'deleted'
  | 'tombstoned'
  | 'skipped';
type QueuebitRetentionPurgeReason =
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

type QueuebitRetentionPurgeOptions = {
  mode?: QueuebitRetentionPurgeMode;
  limit?: number;
};

type QueuebitRetentionPurgeResult = {
  namespace: string;
  mode: QueuebitRetentionPurgeMode;
  observedAt: string;
  scanned: number;
  deleted: number;
  tombstoned: number;
  skipped: number;
  hasMore: boolean;
  windows: {
    completedJobs: {
      ageMs: number;
      maxCount: number;
      cutoffAt: string;
      eligibleStates: string[];
      protectedStates: string[];
    };
    terminalRuns: {
      ageMs: number;
      maxCount: number;
      cutoffAt: string;
      eligibleStates: string[];
      protectedStates: string[];
    };
    completions: {
      ageMs: number;
      maxCount: number;
      cutoffAt: string;
      eligibleStates: string[];
      protectedStates: string[];
    };
  };
  candidates: Array<{
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
  }>;
};

type QueuebitCapacitySnapshot = {
  namespace: string;
  observedAt: string;
  queues: Array<{
    queue: string;
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
    utilization: { jobs?: number; bytes?: number };
    backpressure: {
      latched: boolean;
      reason?: string;
      since?: string;
      lastCheckedAt?: string;
    };
  }>;
};

type QueuebitAlertSeverity = 'warning' | 'critical';

type QueuebitAlertFinding = {
  id: string;
  severity: QueuebitAlertSeverity;
  message: string;
  observedAt: string;
  details: Record<string, unknown>;
};

type QueuebitAlertEvaluationOptions = {
  queueUtilizationWarning?: number;
  queueUtilizationCritical?: number;
  completionFailedMinimum?: number;
  stalledRecoveryMinimum?: number;
};

type QueuebitAlertEvaluation = {
  status: 'ok' | QueuebitAlertSeverity;
  observedAt: string;
  findings: QueuebitAlertFinding[];
};

type QueuebitObservabilityHttpRequest = {
  path: string;
  method?: string;
};

type QueuebitObservabilityHttpResponse = {
  status: number;
  headers: Record<string, string>;
  body: string;
};

type QueuebitObservabilityHttpOptions = {
  healthPath?: string;
  metricsPath?: string;
};

type CursorPage<T> = {
  items: T[];
  nextCursor?: string;
};

type JobSummary = {
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
};

type JobSnapshot<Data = unknown, Result = unknown> = JobSummary & {
  data?: Data;
  result?: Result;
  deduplicationKey?: string;
  idempotencyKey?: string;
  failedReason?: QueuebitSerializedError;
};

type QueuebitSerializedError = {
  name?: string;
  code?: string;
  message: string;
  details?: unknown;
};

type CompletionSummary = {
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
};

type RunSummary = CompletionSummary & {
  id: string;
  definition: string;
  definitionVersion: number;
  parentRunId?: string;
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
};

type RunSnapshot<Input = unknown, Boundary = unknown, Cursor = unknown> =
  RunSummary & {
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
      | 'interval' | 'in_flight_limit' | 'backpressure'
      | 'no_active_worker' | 'redis_reconnecting';
  };

type RunStartResult<Input = unknown, Boundary = unknown, Cursor = unknown> =
  RunSnapshot<Input, Boundary, Cursor> & {
    deduplicated: boolean;
  };

type FailureRecord<Payload = unknown> = {
  sequence: string;
  runId: string;
  batchId?: string;
  jobId?: string;
  stage: 'mapper' | 'processor';
  recordIdentity: string;
  attempt: number;
  error: QueuebitSerializedError;
  recoveryAvailable: boolean;
  envelopeExpiresAt?: string;
  payload?: Payload;
};

type CompletionBackoffSnapshot = {
  type: 'fixed' | 'exponential';
  delayMs: number;
  maxDelayMs?: number;
};

type CompletionEventSummary = {
  id: string;
  type: 'batch.settled' | 'run.settled' | 'run.cancelled';
  runId: string;
  batchId?: string;
  handler?: string;
  completionState: CompletionState;
  attempt: number;
  attempts: number;
  deliveryGeneration: number;
  summaryDigest?: string;
  detailsExpired?: true;
  detailsExpiredAt?: string;
  createdAt: string;
  updatedAt: string;
};

type CompletionSnapshot<Summary = unknown> =
  CompletionEventSummary & {
    summary?: Summary;
    backoff?: CompletionBackoffSnapshot;
    lastError?: QueuebitSerializedError;
    nextDueAt?: string;
  };

type CompletionEventSnapshot<Summary = unknown> = CompletionSnapshot<Summary>;
```

```ts
type JobBackoffOptions = {
  type: 'fixed' | 'exponential';
  delayMs: number;
  maxDelayMs?: number;
  jitter?: number;
};

type JobAddOptions = {
  attempts?: number;
  timeoutMs?: number;
  backoff?: JobBackoffOptions;
  delayMs?: number;
  deduplicationKey?: string;
  idempotencyKey?: string;
};

type BulkJobEntry<Data = unknown> = {
  name: string;
  data: Data;
  options?: JobAddOptions;
};

type JobListQuery = {
  queue: string;
  state?: JobState;
  cursor?: string;
  limit?: number;
};

type RunStartRequest<Input = unknown> = {
  input: Input;
  idempotencyKey: string;
};

type RunListQuery = {
  definition?: string;
  executionState?: RunExecutionState;
  cursor?: string;
  limit?: number;
};

type FailureListQuery = {
  stage?: 'mapper' | 'processor';
  cursor?: string;
  limit?: number;
  includePayload?: boolean;
};

type CompletionListQuery = {
  runId?: string;
  batchId?: string;
  type?: CompletionEventSummary['type'];
  completionState?: CompletionState;
  cursor?: string;
  limit?: number;
};

interface JobsApi {
  add<Data>(queue: string, name: string, data: Data, options?: JobAddOptions):
    Promise<JobSnapshot<Data>>;
  addBulk<Data>(queue: string, entries: BulkJobEntry<Data>[]):
    Promise<JobSnapshot<Data>[]>;
  get<Data = unknown, Result = unknown>(jobId: string):
    Promise<JobSnapshot<Data, Result> | null>;
  list(query: JobListQuery): Promise<CursorPage<JobSummary>>;
  cancel(jobId: string): Promise<JobSnapshot>;
  retryFailed(jobId: string, request: { deduplicationKey: string }):
    Promise<JobSnapshot>;
}

interface RunsApi {
  start<Input>(definition: string, request: RunStartRequest<Input>):
    Promise<RunStartResult<Input>>;
  get(runId: string): Promise<RunSnapshot | null>;
  list(query: RunListQuery): Promise<CursorPage<RunSummary>>;
  listFailures(runId: string, query?: FailureListQuery):
    Promise<CursorPage<FailureRecord>>;
  pause(runId: string): Promise<RunSnapshot>;
  resume(runId: string): Promise<RunSnapshot>;
  cancel(runId: string, request: { reason: string }): Promise<RunSnapshot>;
  retryFailed(
    runId: string,
    request: { idempotencyKey: string; definitionVersion?: number }
  ): Promise<RunSnapshot>;
}

interface CompletionsApi {
  list(query?: CompletionListQuery):
    Promise<CursorPage<CompletionEventSummary>>;
  get<Summary = unknown>(eventId: string):
    Promise<CompletionSnapshot<Summary> | null>;
  retry<Summary = unknown>(eventId: string):
    Promise<CompletionSnapshot<Summary>>;
}

interface HealthApi {
  snapshot(): HealthSnapshot;
}

interface MetricsApi {
  collect(): readonly QueuebitMetricSample[];
  renderPrometheus(): string;
}

interface RetentionApi {
  plan(): QueuebitRetentionPlan;
  purge(options?: QueuebitRetentionPurgeOptions):
    Promise<QueuebitRetentionPurgeResult>;
}

interface AlertsApi {
  evaluate(options?: QueuebitAlertEvaluationOptions):
    Promise<QueuebitAlertEvaluation>;
}

interface QueuebitObservabilityHttpApi {
  handle(request: QueuebitObservabilityHttpRequest):
    QueuebitObservabilityHttpResponse;
}
```

`get` returns `null` when the identity does not exist or has left its retention window. If a retained Job, Run, or Completion identity has `detailsExpired=true`, Queuebit returns the original identity, state, digest, and reference metadata without `data`, `result`, `failedReason`, Run `input`, Run `boundary`, Run cursors, or Completion `summary`; applications must not assume historical payloads are always available after retention. After Run failure details expire, `runs.listFailures` and `runs.retryFailed` return `QB_RUN_STATE_CONFLICT`; after Completion details expire, `completions.retry` returns `QB_COMPLETION_STATE_CONFLICT`. Control and recovery methods throw `QB_JOB_NOT_FOUND`, `QB_RUN_NOT_FOUND`, or `QB_COMPLETION_NOT_FOUND` for a missing identity and the corresponding `*_STATE_CONFLICT` for an incompatible state. `limit` defaults to 50 and accepts 1 through 100; an exhausted page omits `nextCursor`. `addBulk` returns snapshots in input order and never returns a partial result when any entry fails. `jitter` accepts 0 through 1: 0 disables jitter and 0.2 adds up to 20% randomization inside the computed backoff window.

When Redis is disconnected, strict server policy fails, or role ownership cannot be proved, `HealthSnapshot.status='not_ready'` and `ready=false`. `degraded` is reserved for a role that can still work safely but has a warn-policy or non-critical observability warning. `draining` never accepts new work.

`metrics.collect()` reads the same in-process registry used by `renderPrometheus()`. Current metrics cover client state, history cleanup, watermarks, direct submits, Worker claim/completion/failure/duration/attempt/stalled recovery, role heartbeat/drain observation, and Coordinator advancement plus completion delivery. They remain process-local; external monitoring must scrape every Producer, Worker, and Coordinator process and aggregate there. `alerts.evaluate()` returns local starting-point findings for health, queue utilization/backpressure, completion failures, and stalled recovery metrics. `observabilityHttp.handle()` returns response objects only; Queuebit core still starts no HTTP server.

`retention.purge()` defaults to `mode: 'dry-run'`, so it only previews history details that would be cleaned; pass `mode: 'execute'` to actually clean. It reads declared queue, terminal Run, and Completion indexes instead of scanning the Redis keyspace. Execute mode only handles safely finished direct Jobs, terminal Runs, and delivered/not_required Completion events. Records that still need deduplication during their TTL keep lightweight identity, state, digest, idempotency key, parent/recovery metadata, and summary counters while dropping larger payload, result, failure replay, and completion summary details. Non-terminal work, unresolved or failed completion events, BatchRun-owned job cleanup, and deletion candidates without enough evidence are skipped. `completionEvents.ageMs/maxCount` controls the Completion event detail window independently; after details expire, `completions.list/get` can still show the identity but cannot read the deleted summary.

```js
import {
  createQueuebitClient,
  createQueuebitObservabilityHttpApi
} from 'queuebit';

const queuebit = await createQueuebitClient({ config });

const defaultHealth = queuebit.observabilityHttp.handle({
  method: 'GET',
  path: '/queuebit/health'
});

const mounted = createQueuebitObservabilityHttpApi(queuebit, {
  healthPath: '/healthz',
  metricsPath: '/internal/metrics'
});

const metricsResponse = mounted.handle({
  method: 'GET',
  path: '/internal/metrics'
});

const retentionPreview = await queuebit.retention.purge({
  limit: 100
});

const alertEvaluation = await queuebit.alerts.evaluate({
  queueUtilizationWarning: 0.8,
  queueUtilizationCritical: 0.95
});
```

Adapt `status`, `headers`, and `body` to vext, Fastify, Express, or your platform router. The application owns authentication, tenant authorization, rate limiting, TLS, and network exposure.

## Jobs API

```ts
const job = await queuebit.jobs.add(
  'notification',
  'send-receipt',
  { schemaVersion: 1, orderId: 1024, tenantId, recipient },
  {
    deduplicationKey: `request:receipt:${tenantId}:1024`,
    idempotencyKey: `receipt:${tenantId}:1024`,
    delayMs: 5_000
  }
);
```

### `jobs.add(queue, name, data, options?)`

| Option | Default | Meaning |
|---|---:|---|
| `attempts` | 1 | Includes first execution |
| `timeoutMs` | none | Optional per-job timeout passed to the Worker processor signal path |
| `backoff` | none | fixed/exponential |
| `delayMs` | 0 | Non-negative integer |
| `deduplicationKey` | none | Bounded queue-identity deduplication |
| `idempotencyKey` | none | Business side-effect key passed to processor |

### `jobs.addBulk(queue, entries)`

Accepts one queue only. It validates all entries against `maxBulkJobs/maxBulkBytes`, then atomically creates all or none. It is not a BatchRun.

### Query and control

```ts
await queuebit.jobs.get(jobId);
await queuebit.jobs.list({ queue: 'notification', state: 'failed', limit: 100 });
await queuebit.jobs.cancel(waitingJobId);
await queuebit.jobs.retryFailed(failedJobId, {
  deduplicationKey: `replacement:${failedJobId}:1`
});
```

| Method | Allowed state | Result |
|---|---|---|
| `jobs.cancel` | waiting/delayed/retrying | Repeated call returns current snapshot; active/terminal is a state conflict |
| `jobs.retryFailed` | Retained failed direct job | New replacement with `parentJobId`, original business key, new deduplication identity |
| `jobs.retryFailed` on BatchRun-owned job | never | Direct caller to `runs.retryFailed` |

`jobs.list` uses a stable creation-sequence cursor and frozen index, never a Redis keyspace scan. Summary is returned by default.

## Runs API

### `runs.start(definition, request)`

Call this from a server-owned application service. `actor` is the authenticated identity and `request.paidBefore` is already validated business input; Queuebit does not resolve either value from a browser request or the database for you.

```ts
const run = await queuebit.runs.start('receipt-campaign', {
  input: {
    tenantId: actor.tenantId,
    paidBefore: request.paidBefore
  },
  idempotencyKey: `receipt:${actor.tenantId}:${request.paidBefore}`
});

console.log(run.id, run.deduplicated);
```

`idempotencyKey` is required. Order is inputSchema validation, `qbcj-v1` canonicalization, key/digest comparison, then atomic Run creation. The first creation returns `deduplicated=false`, `executionState=created`, and `completionState=not_created`. The same key and canonical input returns the existing Run's current snapshot with `deduplicated=true`; different input returns `QB_RUN_DEDUPLICATION_CONFLICT`.

### Query

```ts
await queuebit.runs.get(run.id);
await queuebit.runs.list({
  definition: 'receipt-campaign',
  executionState: 'partial_failed',
  limit: 100
});
await queuebit.runs.listFailures(run.id, {
  stage: 'mapper',
  limit: 100,
  includePayload: false
});
```

`runs.listFailures` uses a stable failure sequence; `limit` is 1 to 100. It includes stage, record/job identity, error code, attempt, recovery availability, and envelope expiry. Full payload is excluded by default. The application authorizes tenant access before exposing it.

### Continuous control and recovery

```ts
await queuebit.runs.pause(runId);
await queuebit.runs.resume(runId);
await queuebit.runs.cancel(runId, { reason: 'campaign withdrawn' });
await queuebit.runs.retryFailed(failedRunId, {
  idempotencyKey: `recovery:${failedRunId}:1`
});
```

| Method | Semantics |
|---|---|
| `pause/resume` | Continuous control of the same non-terminal Run; blocked recovery resumes that Run |
| `cancel` | Stop new batches, settle active work, preserve the first atomically written reason |
| `retryFailed` | Create a new recovery run for terminal mapper/processor failures without rewriting the original |

A recovery run reads saved failed work from Queuebit. It does not call original `Source.freeze/load` or query changed business rows.

## Completions API

```ts
const page = await queuebit.completions.list({
  runId,
  completionState: 'failed',
  limit: 100
});
for (const event of page.items) {
  await queuebit.completions.retry(event.id);
}
```

`list` uses stable event sequence pagination and supports runId, batchId, type, and state filters. `get` returns one durable event snapshot or `null`. `retry` atomically requires failed state and reopens delivery without changing execution state or creating business work.

<a id="runtime-registration"></a>
## Runtime registration

```ts
export default defineQueuebitRuntime({
  sources: {
    'paid-orders': defineQueuebitSource({ freeze, load })
  },
  mappers: {
    'receipt-jobs': defineQueuebitMapper(mapReceipt)
  },
  processors: {
    'send-receipt': defineQueuebitProcessor(sendReceipt)
  },
  completions: {
    'record-receipt-batch-result': defineQueuebitCompletionHandler(recordBatch),
    'record-receipt-run-result': defineQueuebitCompletionHandler(recordRun)
  }
});
```

`defineQueuebitRuntime()` input may include only the registries the current role needs. A Worker for normal background jobs can register only `processors`; `sources`, `mappers`, and `completions` are needed only for BatchRun. The function returns a normalized runtime and treats omitted registries as empty objects.

### Source

| Function | Input | Output/invariant |
|---|---|---|
| `freeze` | runId, input, signal | Serializable `boundary` and initial `cursor`; optional `totalRecords` |
| `load` | runId, input, boundary, cursor, limit, signal | `{records,nextCursor,exhausted}`; non-empty pages advance cursor |

### Mapper

Each record maps to one job, multiple jobs, or `null` / `undefined` for skipped. It is a pure transformation; one-to-many output needs stable `identity` values. Throwing from the mapper records saved failure details for recovery.

### Processor

Receives `jobId/runId/batchId/data/attempt/idempotencyKey/signal/logger`. Timeout signals `AbortSignal`; user code forwards it downstream. Lease-generation fencing protects Redis settlement.

### Completion handler

Receives a durable `batch.settled`, `run.settled`, or `run.cancelled` event with execution/completion semantics, attempt, delivery generation, and summary. Deduplicate external writes by event ID.

## Public error shape

```ts
type QueuebitError = {
  code: string;
  message: string;
  details?: unknown;
};
```

`cause` is process-local diagnostic data and is not serialized to CLI JSON, events, or durable errors. Retry behavior is documented per API method and state; do not infer it from extra fields on the error object.
