# Worker, Coordinator, and time-advancement lifecycle

<span class="manual-label">Maintainer · internal ownership and cleanup</span>

This page is for maintainers checking the internal Worker, Coordinator, and time-advancement lifecycle. Queuebit users only need to know how to start Workers, drain them, and inspect health.

## Common lifecycle

```text
load static config -> validate role registrations -> connect dependencies
-> Redis preflight -> acquire/announce role identity -> ready
-> work loop -> draining -> close role resources -> exit
```

Startup failure still closes resources already opened. Closing an unopened factory is a safe no-op. Cleanup timeout names the resource and exits non-zero.

## Worker

| Phase | Operation | Failure principle |
|---|---|---|
| boot | Activate processor resources and validate queue/version | Do not become ready; close opened resources |
| claim | Atomically claim attempt/generation/workerId/expiry | No owner means no execution |
| process | Invoke processor with signal/logger/idempotencyKey | Timeout or lease loss signals abort but does not replace fencing |
| renew | Extend inside lease window | Failure stops new claims |
| settle | Check generation/owner and atomically update Job/Batch | Stale owner gets stable error |
| drain | Stop claims, wait for active, stop renewal | Timeout does not invent a business result |

In the public Worker kernel this owner generation is the `leaseGeneration` field. `complete(jobId, leaseGeneration, result)` and `fail(jobId, leaseGeneration, error)` are the two settle paths; both reject stale owners with `QB_JOB_STATE_CONFLICT`.

## Coordinator

| Phase | Operation | Invariant |
|---|---|---|
| acquire Run | Generate per-Run ownership generation | Old generation cannot commit |
| freeze | Freeze boundary plus initial cursors | One atomic write |
| load/map | Read page, pure map, prepare envelopes/jobs | Unpersisted page is not counted |
| dispatch | Commit Batch/jobs/summary/envelopes/dispatchCursor | Expected cursor plus generation |
| checkpoint | Cross continuous execution+completion barriers | Never skip a gap |
| completion | Claim, deliver, settle event | Independent delivery generation |
| drain | Stop load/dispatch and finish current atomic boundary | Runtime lifecycle closes |

## Time advancement

v0.1 provides cooperative mode only: candidate loops inside background Workers compete for one owner generation per domain, and Web/Producer never participates. Standalone Scheduler is deferred and has no v0.1 command or configuration-compatibility promise.

| Operation | When ownership becomes invalid |
|---|---|
| Promote delayed/retrying | Stop new promotion |
| Detect stalled work | Do not submit recovery under old generation |
| Renew owner | Uncertainty becomes `not_ready` and stops new promotion |
| Drain | Stop promotion and safely release or expire ownership |

## Connection policy

Producer and CLI fail promptly after bounded `requestRetryLimit` retries. Worker, Coordinator, and time advancement stop new work during a Redis outage and reconnect indefinitely with full-jitter exponential backoff: caps progress through 250ms, 500ms, 1s, and 2s up to 30s, while each actual wait is random from zero to its cap. The first failure logs immediately; the same role/endpoint logs at most every 30s until reconnect, drain, or close. Before and after initial readiness, disconnection maps to `health.status=not_ready` and `ready=false`, never a traffic-admitting degraded state.

## Required fault windows

- Crash after processor success but before ACK.
- Old handler returns after timeout.
- Worker renewal succeeds but response is lost.
- Coordinator crashes after source load but before atomic Batch commit.
- Crash after batch dispatch but before completion delivery.
- Old time-owner generation promotes late during handover.
- New owner takes over after drain timeout.
