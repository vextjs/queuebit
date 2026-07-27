# Recover From Failures

<span class="manual-label">Production operations · preserve state, then recover work</span>

## Remember Three Rules First

| Rule | Why |
|---|---|
| Do not edit Redis keys by hand | Queuebit state, leases, retries, and recovery data are consistent as a set |
| Retry with the same business identity/idempotency key | Random keys can duplicate email, payment, or webhook effects |
| Classify the failure before choosing the recovery action | Redis disconnect, Worker crash, business failure, and completion failure recover differently |

## Capture These Evidence Commands First

```bash
npx queuebit health inspect --config queuebit.config.ts --json
npx queuebit queue inspect notification --config queuebit.config.ts --json
npx queuebit run inspect <runId> --config queuebit.config.ts --json
npx queuebit workers inspect --queue notification --config queuebit.config.ts --json
```

Use these commands to see state first; do not edit Redis while investigating. Preserve the UTC timeline, role identity/version/config digest, correlation IDs, Redis role/persistence/policy, and recent errors before choosing a recovery action.

## Triage: Choose Recovery by Symptom

| Symptom | Class | Correct recovery |
|---|---|---|
| Producer/API cannot accept | Config, Redis, server policy, or backpressure | Repair cause and submit again with the same business identity |
| Direct job failed | Processor business work | Repair cause and create a replacement job |
| Run is `blocked` | Source, Dispatch, or Redis control plane | Repair and resume the original Run |
| Run is `partial_failed/failed` and failed-work details remain | Mapper or processor terminal failure | Create a recovery run for failed work only |
| Execution terminal but completion failed | Result callback delivery | Retry only the completion event, not the jobs |
| Original Redis state is lost | Durability incident | Restore backup or create a fresh Run from business DB |

## Redis unavailable or network partition

**Immediate action:** do not amplify Producer retries. Background Workers/Coordinators stop new claims, loads, dispatches, and promotions when lease state is uncertain. Keep them alive for persistent reconnect.

```bash
redis-cli PING
redis-cli INFO replication
redis-cli INFO persistence
redis-cli CONFIG GET maxmemory-policy
```

Restore primary connectivity and pass Redis policy checks. After automatic reconnect, reconcile any new `leaseGeneration`, `stalledRecoveries`, Run cursor changes, and duplicate side-effect outcomes. Run `run resume` only when the Run actually entered `blocked`.

Stop new work and escalate to Redis operations while the role is flapping, persistence is failing, write loss cannot be bounded, or more than one node accepts writes.

## Worker Crash or Work Stuck in Active

1. Inspect heartbeat, event loop, memory, CPU, Redis renewal latency, and downstream timeout.
2. Restore at least one ready Worker without creating a retry storm.
3. Wait for the old Worker's lease to expire and observe a higher `leaseGeneration` plus `stalledRecoveries`. They mean another Worker reclaimed the job.
4. Reconcile external effects by business `idempotencyKey`.
5. After `maxStalledRecoveries`, use a recovery run for BatchRun work or a replacement for a direct job.

## Database BatchRun Blocked

```bash
npx queuebit run inspect <runId> --config queuebit.config.ts --json
```

| Blocked reason | Check | Recovery |
|---|---|---|
| Source timeout/unavailable | DB pool, query plan, snapshot token | Repair source, continue reading from the same cursor, then resume |
| Cursor did not advance | Whether source returns the next page position correctly | Repair loader; never edit cursor manually |
| Dispatch retry exhausted | Whether Redis connection and submit have recovered | Restore Redis; repeated commit converges on the same batch/jobs |
| Request too large | Whether page, fan-out, or payload is too large | Version and shrink definition, cancel old Run, start a new Run |

Blocked is a control-plane failure, not a reason to call `run retry-failed`.

<span id="sc09-recovery-run"></span>
## Re-execute Failed BatchRun Work

```bash
npx queuebit run failures <runId> --limit 100 --config queuebit.config.ts
npx queuebit run retry-failed <runId> \
  --idempotency-key "recovery:<runId>:1" \
  --config queuebit.config.ts
```

A recovery run reads the failed-work details retained by Queuebit; it does not query the current business database. Mapper-stage failures rerun the mapper, while processor-stage failures keep the original job data and business idempotency key. The original Run summary and state remain unchanged.

| Failure | Meaning | Action |
|---|---|---|
| `QB_RUN_STATE_CONFLICT` | Retention removed saved failure details or the Run is not recoverable | Start a fresh Run from the business database |
| Definition version unavailable | Old runtime no longer exists | Restore compatible runtime or select an explicitly verified new version |
| Business data changed and new values are required | Recovery no longer matches your business semantics | Start a fresh Run; do not present it as snapshot recovery |

## Result Callback Completion Failure

```bash
npx queuebit completion inspect --run <runId> --config queuebit.config.ts
npx queuebit completion retry <eventId> --config queuebit.config.ts
```

Repair the audit database, webhook, or handler first, then retry the exact event. Queuebit prevents a late old owner from overwriting the new state, but the handler's external write still needs `event.id` idempotency.

## Backpressure or No Active Worker

- `dispatchHoldReason=backpressure/no_active_worker` is automatic waiting; it consumes no retry and needs no manual resume.
- Check downstream capacity and Worker health before scaling. Adding Workers to an overloaded downstream makes the incident worse.
- Automatic recovery occurs only when both jobs and bytes fall to or below low watermarks.
- A single oversized request requires a smaller page, bulk, fan-out, or payload; waiting cannot make it smaller.

## Incident exit criteria

- Strict server policy and role readiness pass.
- New work can be accepted and waiting age trends down.
- The Run's read position, created jobs, and completed counts reconcile.
- Stalled, completion-failed, and server-policy errors stop increasing.
- Side-effect reconciliation finds no duplicate result.
- Cause, affected identities, recovery actions, and prevention items are recorded.
