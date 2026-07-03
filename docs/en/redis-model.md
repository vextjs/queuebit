# Redis Model

## Page positioning

<span class="manual-label">v0.1 final user manual</span>

This page describes the Redis keyspace, state collections, and atomic transition semantics behind the queuebit v0.1 user manual. Users usually do not read or write these keys directly, but operations, troubleshooting, and implementation alignment rely on this model.

## Design boundary

Redis is the only first-version source of shared state. queuebit does not treat in-process queues, memory locks, or local timers as formal consistency sources.

The Redis model must support:

- concurrent enqueue from multiple producers;
- concurrent claim from multiple workers;
- stalled recovery after worker crash;
- delayed jobs and retry promotion;
- scheduler single-active ownership;
- metrics and introspection queries.

## Keyspace target

Suggested key prefix:

```text
qb:{namespace}:{queue}:...
```

Segment meaning:

| Segment | Meaning |
|---------|---------|
| `qb` | queuebit keyspace marker |
| `{namespace}` | app, environment, or tenant isolation |
| `{queue}` | queue name |

If Redis Cluster hash tags are required later, the key design must describe them consistently to avoid cross-slot atomic operation failures.

## Redis deployment boundary

Redis deployment support status must be visible and must not be hidden behind a vague "Redis-compatible" claim:

| Deployment shape | v0.1 target status | Redis model requirement |
|------------------|--------------------|-------------------------|
| standalone Redis | baseline target | current keyspace and atomic transitions use this as the minimum verification target |
| managed single-primary Redis | conditionally equivalent | may enter verification when commands, TTL, atomic scripts/transactions, and connection semantics match the baseline |
| Sentinel / failover | conditional support target | must cover disconnect/reconnect, lease renewal failure, and scheduler renewal failure |
| Redis Cluster | unsupported in v0.1 | later support requires a unified hash tag and proof that every Lua/transaction transition stays in one slot |
| multiple Redis backends | explicitly out of scope | v0.1 must not mix cross-backend abstraction into the Redis adapter |

If the implementation does not support Redis Cluster yet, the configuration loader and CLI must fail before startup instead of letting users hit `CROSSSLOT` or partially completed state transitions at runtime.

## Data structures

| Target key | Type | Semantics |
|------------|------|-----------|
| `...:waiting` | list or stream | Jobs ready for workers |
| `...:delayed` | sorted set | Delayed jobs ordered by executable time |
| `...:retry` | sorted set | Retry jobs ordered by next attempt time |
| `...:active` | hash / set | Claimed jobs and worker identity |
| `...:lease:{jobId}` | string with TTL | active job lease token |
| `...:job:{jobId}` | hash | job metadata, payload reference, state, attempts, error summary |
| `...:events` | stream | state change events for debugging and future subscriptions |
| `...:scheduler:{domain}` | string with TTL | scheduler single-active token |
| `...:metrics` | hash / derived view | queue depth, active, retry, delayed, and similar observations |

Concrete Redis types may change based on atomicity and performance tradeoffs, but user-visible semantics must remain covered.

## State transitions

| Transition | Trigger | Atomicity requirement |
|------------|---------|----------------------|
| enqueue -> waiting | producer | job metadata and waiting write stay consistent |
| enqueue -> delayed | producer | job metadata and delayed score stay consistent |
| waiting -> active | worker | claim and lease token creation stay consistent |
| active -> completed | worker | lease token validation and state update stay consistent |
| active -> retry | worker | attempt increment, error summary, and retry score stay consistent |
| active -> failed | worker | terminal failure and active removal stay consistent |
| delayed -> waiting | scheduler | due check and waiting write stay consistent |
| retry -> waiting | scheduler | due check and redelivery stay consistent |
| active -> stalled -> waiting | scheduler/recovery | lease expiration check and redelivery stay consistent |

## Atomicity requirements

These operations must not be split into unprotected commands:

- worker claim job and create lease;
- ack completed and remove active state;
- fail retryable and write retry schedule;
- scheduler delayed/retry promotion;
- stalled recovery redelivery;
- scheduler leadership acquire and renew.

The implementation may use Lua, transactions, or Redis-native command combinations, but tests must cover concurrency races.

## Retention policy

v0.1 must define at least:

| Data | Target policy |
|------|---------------|
| completed jobs | cleanup by count or age by default |
| failed jobs | retained longer for diagnostics |
| events | configurable stream length |
| metrics | may be derived from live keys; permanent storage not required |
| payload | inline or referenced by implementation, with size limits documented |

## Implementation acceptance

- Redis key prefixes include namespace and queue to avoid environment collisions.
- All state transitions remain recoverable under Redis uncertainty or concurrency.
- Redis Cluster support status is explicit.
- Operations docs metrics can be obtained from keyspace or public APIs.
- Tests cover worker crash, lease expiration, scheduler double-instance competition, and approximate lost ack cases.
