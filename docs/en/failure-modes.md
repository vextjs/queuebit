# Failure Modes and Recovery

## Page positioning

<span class="manual-label">v0.1 final user manual</span>

This page explains queuebit v0.1 user-visible recovery semantics for Redis, worker, scheduler, ack, and lease failures.

## Recovery principles

queuebit recovery follows:

- Prefer recoverable jobs over exactly-once guarantees.
- Stop progression when uncertain instead of writing risky state.
- Recovery actions must be observable.
- Users need business idempotency or deduplication for side effects.

## Failure handling flow

First identify what failed: Redis, worker, scheduler, handler, or shutdown. Each failure point has a different response.

```mermaid
flowchart TD
  Failure["failure or unfinished job"] --> RedisCheck{"Redis reachable?"}
  RedisCheck -- "no" --> StopClaim["worker stops claiming<br/>wait for Redis recovery or operator action"]
  RedisCheck -- "yes" --> WorkerCheck{"worker heartbeat alive?"}
  WorkerCheck -- "no" --> Stalled["wait for lease expiry<br/>scheduler performs stalled recovery"]
  WorkerCheck -- "yes" --> HandlerCheck{"handler failed?"}
  HandlerCheck -- "yes" --> Retry["retry by attempts/backoff<br/>eventually failed"]
  HandlerCheck -- "no" --> SchedulerCheck{"scheduler active?"}
  SchedulerCheck -- "no" --> DelayedStop["delayed/retry promotion pauses<br/>start or fix scheduler"]
  SchedulerCheck -- "yes" --> Inspect["read inspect output<br/>locate waiting/active/delayed/retrying/failed"]
```

Node explanations:

| Node | Handling principle |
|------|--------------------|
| Redis unreachable | Do not keep claiming new jobs and expand uncertainty |
| worker missing heartbeat | Do not assume job failure; wait for lease/recovery |
| handler failed | Retry by backoff; handlers must be idempotent |
| scheduler inactive | Pause delayed/retry promotion until single-active is restored |
| inspect output | The first user-facing evidence for the next action |

## Failure matrix

| Failure | Target system behavior | User action |
|---------|------------------------|-------------|
| Redis unavailable at startup | producer/worker/scheduler fails to start or backs off | Fix Redis and validate namespace/connection |
| Redis briefly unavailable while processing | worker stops claiming; renew failure enters uncertainty path | Watch retry/stalled metrics and confirm idempotency |
| Worker process crashes | job enters stalled recovery after lease expiration | Check worker logs and redelivery risk |
| Handler throws | job enters retry or terminal failed | Inspect error summary, attempts, backoff |
| Handler timeout | treated as failure or recovered after lease expiration | Adjust timeout, lease, or task granularity |
| Ack lost | job may be delivered again | Use idempotency key for business side effects |
| Lease renewal fails | worker stops claiming; active job waits for recovery | Check Redis latency, network, and worker load |
| Scheduler double-instance race | only active scheduler advances; uncertainty stops progression | Check scheduler domain and identity |
| Delayed jobs not promoted | scheduler is not running or lost leadership | Inspect scheduler identity and delayed depth |
| Drain timeout | active jobs follow lease/recovery rules | Shorten jobs, increase shutdown timeout, or split work |

## User troubleshooting path

When a job does not complete as expected:

1. Did queue depth increase? If not, check producer enqueue.
2. Are active jobs stuck? Check worker process and lease renewal.
3. Are delayed or retry jobs piling up? Check active scheduler status.
4. Is stalled recovery increasing? Check worker crash, Redis latency, or handler timeout.
5. Are failed jobs increasing? Inspect error summaries and attempt history.

## Idempotency guidance

Under at-least-once delivery, business handlers should:

- use a business unique key or job id for idempotency;
- protect external side effects with state-machine or check-before-write logic;
- split long tasks to keep lease and drain windows reasonable;
- log processing start, success, failure, and external request ids.

queuebit may expose an idempotency key entry point, but it cannot prove exactly-once behavior for a business system.

## Before production

- Every failure mode has an automated or manual verification path.
- Operations entries describe matching metrics and troubleshooting entry points.
- Worker crash, approximate lost ack, and scheduler leadership loss are covered by the test matrix.
- Redelivery is not abnormal; it is a normal at-least-once risk, so business handlers need idempotency.
