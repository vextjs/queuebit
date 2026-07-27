# Failure Runbooks

<!-- queuebit-v01-legacy-doc -->
> [!WARNING]
> **Archived and no longer maintained.** The current v0.1 final-user manual is [`docs/v01/en`](../v01/en/index.md). This page remains for historical context only; do not use its APIs, commands, configuration, or examples for new integrations or implementation.

<span class="manual-label">Symptom-driven operations runbook</span>

Use this page during an incident. Save the time range, namespace, queue, deployment version, and alert evidence, then run read-only inspection. Never edit Redis keys directly. See [Failure modes and recovery](./failure-modes.md) for the conceptual model.

## Collect three evidence sets first

```bash
npx queuebit inspect queue notification --config queuebit.config.ts
npx queuebit inspect workers --queue notification --config queuebit.config.ts
npx queuebit inspect scheduler --domain billing-notification --config queuebit.config.ts
```

Also query Producer enqueue errors, Worker job/attempt logs, and Scheduler identity/renew logs. When Redis is unreachable, preserve client errors and the Redis/Sentinel event timeline.

## Route by state

| Symptom | First check | Do not do first |
|---------|-------------|-----------------|
| Waiting keeps growing | Worker heartbeat, drain, queue/namespace, downstream capacity | Resubmit the same batch |
| Active does not change | Worker/lease, handler duration, Redis latency | Delete active keys |
| Due delayed/retrying does not move | Active Scheduler, domain, system time | Start a different-domain Scheduler |
| Failed rises sharply | Final error classes, downstream status, recent deploy | Replay unchanged jobs forever |
| Stalled rises sharply | Worker crashes, renewal, GC, Redis network | Assume no side effect occurred |

<span id="s15-redis-outage"></span>
## S15 Redis outage

**Trigger:** sustained connection errors, renew failures, Producer enqueue failures, or every role losing Redis.

**Immediate actions:**

1. Stop deployments and scaling changes; record incident start and affected namespaces.
2. Let Workers back off and stop claiming. Do not introduce an in-memory queue that reports false success.
3. Pause or reject upstream requests that depend on confirmed enqueue. Producers confirm only after `add/addBulk` succeeds.
4. Check Redis health, DNS, network, auth, TLS, and connection limits. Do not flush the keyspace.
5. Mark active-job outcomes uncertain and use business idempotency records to check side effects.

**Validate recovery:**

1. Confirm stable single-primary identity before restoring Producer traffic.
2. Confirm Worker heartbeats return without sustained renew errors.
3. Confirm exactly one active Scheduler.
4. Watch stalled recovery, retrying, failed, and oldest waiting age until they fall.
5. Sample business keys from the incident window for missing or duplicate effects.

**Escalate when:** the primary keeps changing, lease mismatch persists after reconnect, business results cannot be reconciled, or backlog cannot recover within SLO.

<span id="s16-sentinel-failover"></span>
## S16 Sentinel failover

**Trigger:** Sentinel reports a master change and clients rediscover the primary, with short connection or read-only errors.

**Expected behavior:** Producers may fail briefly; Workers stop new claims and uncertain renewals recover later; Scheduler progression stops when ownership cannot be proven. After the primary stabilizes, retry, lease, and stalled recovery converge jobs.

**Procedure:**

1. Confirm the master name and new primary through Sentinel; compare `sentinel.name` in configuration.
2. Confirm applications do not cache the old primary address or connect to a Redis Cluster endpoint.
3. Inspect Scheduler active identity. Zero active during the window is acceptable; dual active is not.
4. After connections stabilize, run all three inspect commands and record stalled/retrying increments.
5. Reconcile success-with-uncertain-ack jobs by business key.

**Stop recovery and escalate:** Sentinel views disagree, clients oscillate between primaries, split-brain is detected, dual-active Scheduler persists, or Redis data is no longer monotonically visible.

## Worker crash or stuck active job

1. Check whether the Worker identity still has heartbeat and whether OOM or the platform terminated it.
2. With no heartbeat, wait for lease expiry and active Scheduler recovery; do not resubmit.
3. With heartbeat, inspect handler p99, downstream calls, `timeoutMs`, and event-loop blocking.
4. Correlate old and new attempts after recovery and reconcile effects by business key.

If active lasts beyond `leaseMs` without stalled recovery, investigate the Scheduler before repeatedly restarting Workers.

## Delayed or retrying does not progress

1. Confirm system time and job due time.
2. Check Scheduler candidates, domain, and namespace.
3. Require one active identity. Restore candidates for zero active; stop progression and escalate for multiple active.
4. Verify due jobs move from delayed/retrying to waiting and are then processed.

## Failed rises sharply

1. Group errors by configuration, auth, throttle, business validation, data/template, and `HandlerTimeoutError`.
2. Compare recent deploys and downstream incidents to distinguish systemic from bad-record failures.
3. Fix recoverable downstream failures and let existing retry continue. For terminal failures, follow [S05 Handle terminal failures](./job-recipes.md#s05-terminal-failed).
4. There is no built-in DLQ/manual retry; resubmit only through an audited business administration path.

## Drain timeout

1. Keep other Workers healthy and stop new claims on this instance.
2. Inspect active job start time, handler stage, and `ctx.signal` propagation.
3. Wait within platform termination grace; after it expires, allow lease/recovery to take over.
4. Before the next deploy, split long work or tune timeout, lease, and drain from measured p99.

## Incident exit criteria

- Redis single-primary identity and connectivity are stable.
- Worker heartbeats and exactly one active Scheduler are restored.
- Oldest waiting age and failed/stalled increments return to business baseline.
- Side effects from the incident window are reconciled for duplicates and omissions.
- The timeline records root cause, recovery actions, affected jobs/business keys, and prevention work.
