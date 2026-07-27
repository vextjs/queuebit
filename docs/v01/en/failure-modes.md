# How to read states and errors

<span class="manual-label">Reference · map symptoms to states and error codes</span>

This page is a dictionary, not the recovery flow. If production is already broken, start with [Recover from failures](./failure-runbooks.md). If you need to understand a state in a page, log, or API response, start by symptom.

## Find by symptom

| What you see | Start here |
|---|---|
| One background job is stuck, failed, or cancelled | [Job states: where one task is](#job-state) |
| One database batch run is paused, blocked, or partial_failed | [BatchRun states: where one batch run is](#batchrun-state) |
| Success/failure callback was not delivered | [Completion states: where result delivery is](#completion-state) |
| Completion event contains `batch.settled` or `run.settled` | [Completion events: which results are delivered](#completion-event-types) |
| API or CLI throws a `QB_*` error | [Error-code groups](#error-code-groups) |
| You need log correlation fields | [Events and logs](#events-and-logs) |

<a id="job-state"></a>
## Job states: where one task is

| State | Meaning | Typical next step |
|---|---|---|
| `waiting` | Available for Worker claim | active or cancel |
| `active` | Owned by a Worker lease | completed/retrying/failed, or atomically back to waiting after lease expiry |
| `delayed` | Execution time not reached | waiting when due |
| `retrying` | Business failure waiting for backoff | waiting when due, or failed when attempts exhausted |
| `completed` | Processor succeeded and current generation settled | terminal |
| `failed` | Non-retryable, attempts exhausted, or stalled limit exceeded | Direct replacement; BatchRun summary/recovery |
| `cancelled` | Non-active job was cancelled | terminal |

`stalled` is an observable recovery event and counter, not a durable or filterable `JobState`. Lease-expiry recovery checks the old generation, increments `stalledRecoveries`, and atomically moves the job from `active` to `waiting`; exceeding the limit moves it to `failed`.

<a id="batchrun-state"></a>
## BatchRun states: where one batch run is

| State | Meaning | Control/recovery |
|---|---|---|
| `created` | Run identity is durable and awaits first Coordinator advancement | inspect/cancel |
| `running` | Freeze, load, dispatch, wait, and completion loop | pause/cancel |
| `pausing` | New batches stopped while the current atomic boundary settles | inspect |
| `paused` | User paused new advancement | resume/cancel |
| `blocked` | Source, Dispatch, or Redis control retries exhausted/precondition failed | Repair and resume original Run |
| `cancelling` | No new batches; active work is settling | eventually cancelled |
| `completed` | All business work succeeded or skipped | terminal; completion can still fail separately |
| `partial_failed` | Continue policy has terminal failed work | create recovery run |
| `failed` | Fail-fast or unrecoverable business failure | recovery run while envelope retained |
| `cancelled` | Cancellation settled | create a fresh Run to process again |

`dispatchHoldReason` is automatic waiting inside running: `interval`, `in_flight_limit`, `backpressure`, `no_active_worker`, or `redis_reconnecting`. It is not blocked.

<a id="completion-state"></a>
## Completion states: where result delivery is

| State | Meaning |
|---|---|
| `not_created` | Execution is not terminal, so no completion event exists |
| `not_required` | No handler configured; barrier passes automatically |
| `pending` | Durable event awaits delivery |
| `delivering` | Owned by a delivery generation |
| `retrying` | Handler failure waiting for backoff |
| `delivered` | Handler succeeded and barrier passed |
| `failed` | Delivery attempts exhausted; repair then explicitly retry event |

<a id="completion-event-types"></a>
## Completion events: which results are delivered

| Type | Trigger | Key content |
|---|---|---|
| `batch.settled` | Batch execution terminal | batchId, execution state, summary, attempt/generation |
| `run.settled` | Run completed/partial_failed/failed | runId, parentRunId, recoveryDepth, summary |
| `run.cancelled` | Cancellation settled | reason plus seen/undispatched summary |

Summary invariants:

```text
recordsSeen = recordsDispatched + recordsSkipped + recordsFailed + recordsUndispatched
jobsCreated = jobsCompleted + jobsFailed + jobsCancelled
```

## Error shape

```ts
// Public shape: QueuebitError is an Error subclass with stable code/details.
class QueuebitError extends Error {
  readonly code: string;
  readonly details?: unknown;
  // name === 'QueuebitError'
}
```

<a id="error-code-groups"></a>
## Error-code groups

| Prefix | Domain | Representative | Recovery principle |
|---|---|---|---|
| `QB_CONFIG_*` | Static config/runtime registration | `QB_CONFIG_HANDLER_NOT_REGISTERED` | Repair config and restart |
| `QB_REDIS_*` | Connection/coordination | `QB_REDIS_CONNECTION_FAILED`, `QB_REDIS_PREFLIGHT_FAILED` | Same identity retry; background reconnect |
| `QB_SOURCE_*` | freeze/load | `QB_SOURCE_CURSOR_NOT_ADVANCED` | Repair source; resume blocked Run |
| Mapper failures | Record transformation | serialized mapper error in saved failure details | Retain details; recovery run after terminal |
| `QB_DISPATCH_*` | Atomic batch/job commit | `QB_DISPATCH_STATE_CONFLICT`, `QB_DISPATCH_LIMIT_EXCEEDED` | Cursor stays; repair cause and resume |
| `QB_JOB_*` | Processor/state/limits | `QB_JOB_STATE_CONFLICT`, `QB_JOB_LIMIT_EXCEEDED` | Follow retry policy and keep external effects deduplicated |
| Owner generation | Worker/Coordinator fencing | `QB_JOB_STATE_CONFLICT`, `QB_DISPATCH_STATE_CONFLICT` | Old owner stops; new owner continues |
| `QB_COMPLETION_*` | Completion delivery | `QB_COMPLETION_STATE_CONFLICT` | Repair handler and retry event |
| `QB_RUN_*` | Input/state/recovery | `QB_RUN_INPUT_INVALID`, `QB_RUN_STATE_CONFLICT` | Repair input or create fresh Run |
| Deduplication conflicts | Key/canonical input | `QB_JOB_DEDUPLICATION_CONFLICT`, `QB_RUN_DEDUPLICATION_CONFLICT` | Repair business identity, not random key |
| `QB_BACKPRESSURE_*` | Queue jobs/bytes watermark | `QB_BACKPRESSURE_REJECTED`, `QB_BACKPRESSURE_REQUEST_TOO_LARGE` | Wait for low or reduce request |

<a id="events-and-logs"></a>
## Events and logs

There is no public Worker event-bus or `worker.on(...)` listener API today. Observe job lifecycle through structured logs, role heartbeats, and process-local metrics (for example `worker_jobs_completed_total`, `worker_jobs_failed_total`, `worker_stalled_jobs_recovered_total`, and attempt counters). Metric or log emission failure must never rewrite Job state. Correlate with namespace, queue, runId, batchId, jobId, eventId, attempt, leaseGeneration, role identity, and errorCode; do not log full business payloads by default.
