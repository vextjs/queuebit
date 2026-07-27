# Redis model and atomic invariants

<span class="manual-label">Maintainer · internal storage contract</span>

This page explains how Queuebit keeps Redis state consistent. Normal users should not depend on these keys or models; integration, debugging, and recovery use only public API/CLI surfaces.

## User boundary

Public API and CLI are the only supported query and recovery surfaces. Redis keys are not a user API; never cancel, retry, or recover by editing them.

Production APIs do not scan arbitrary Redis keyspace for user operations. Target test harnesses may use `SCAN qb:{namespace}:*` only to clean the unique namespace created for that run; Queuebit docs, CLI, and recovery procedures never rely on `FLUSHDB`, `FLUSHALL`, global `SCRIPT FLUSH`, or manual key edits.

## Conceptual keyspace

| Category | Content | Primary index |
|---|---|---|
| Queue | waiting/active/delayed/retrying/terminal and jobs/bytes latch | queue + state + due/sequence |
| Job | data/options/attempt/lease generation/owner/result/error/parent | jobId and dedup digest |
| Run | definition/version/input digest/boundary/cursors/exhaustion/summary | runId, definition/state/created sequence |
| Batch | index/cursor range/record summary/jobs/execution/completion | runId + batch index |
| Failure envelope | Mapper record or processor job replay data | runId + failure sequence |
| Completion event | type/attempt/delivery generation/handler/summary/error | event sequence + run/batch/state |
| Role lease | Worker/Coordinator/time-owner identity, generation, expiry | role/domain |
| Tombstone | identity/digest/version/state after details expire | dedup key TTL |

Exact key names freeze during implementation, but related state must commit under one single-primary atomic boundary using hash tags or equivalent. v0.1 does not weaken the contract by sharding across Redis Cluster.

## Required atomic transitions

- Job add/addBulk/dedup/backpressure counting.
- waiting/delayed/retrying to active claim.
- active to completed/retrying/failed plus Batch summary.
- Lease expiry to stalled reclaim generation.
- Run boundary plus initial dispatch/checkpoint cursor.
- Source page to Batch/jobs/envelopes/dispatchCursor.
- Batch barrier to continuous checkpoint-prefix cascade.
- Source-exhausted marker plus Run terminal evaluation.
- Completion claim/settle/retry generation.
- Idempotent pause/resume/cancel/recovery identity.

## Canonical input

`qbcj-v1` accepts JSON-serializable values only. Object keys sort recursively by Unicode code point, arrays preserve order, strings use UTF-8, and version plus SHA-256 digest is stored. `undefined`, functions, symbols, BigInt, NaN/Infinity, and cycles fail before Redis.

## Retention and non-removable state

- Active, waiting, delayed, retrying, and non-terminal Run work is never cleaned.
- A Run or descendant Batch with completion outside `not_required/delivered` is not cleaned.
- A `completionState=failed` event is alerted and explicitly retried, never silently deleted.
- After failed-work cleanup, Run reports recovery data expired.
- When details expire before dedup TTL, a compact tombstone remains through key TTL.
- Runtime M2K `retention.purge()` is a safe local Job/Run/Completion foundation: it reads declared queue `completed` indexes, the terminal Run detail index, and the Completion detail index, defaults to dry-run, deletes direct completed Jobs with no identity references, compacts deduplication/idempotency/replacement-bound direct completed Jobs into `detailsExpired=true` tombstones, compacts age-expired or maxCount-excess terminal Runs whose completion is `not_required` or `delivered`, and compacts delivered/not-required Completion events after the parent Run is terminal using independent `completionEvents.ageMs/maxCount`. Run compaction deletes input/boundary/cursors and failure replay envelopes while preserving identity and summary counters, then removes the Run from the terminal detail index so tombstones do not consume `terminalRuns.maxCount`. Completion compaction deletes `summary`, backoff/error, due, and delivery lease details while preserving event identity plus `summaryDigest`, removes the event from the Completion detail index so tombstones do not consume `completionEvents.maxCount`, and keeps the stable Completion event index for `completions.list/get` tombstone readback. It still skips non-terminal work, pending/retrying/delivering/failed completion events, Completion events whose parent Run is not terminal, BatchRun-owned job cleanup, and target Redis cleanup evidence.

## Server policy

Strict preflight verifies noeviction, persistence status, primary/replica role, replication connection/lag, and recent persistence errors. It reruns after Sentinel failover. Unreadable policy is unknown/not_ready, never inferred healthy.

## Verification matrix

- Concurrent claim produces one owner.
- Late Worker, Coordinator, time-owner, and completion generations are rejected.
- Failed page commit cannot advance cursor alone.
- Out-of-order paced batch completion cannot jump checkpoint.
- addBulk validation/backpressure failure creates no partial jobs.
- Retention preserves active and undelivered completion state, and tombstones preserve deduplication conflict.
