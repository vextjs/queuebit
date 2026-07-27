# Redis-only and Distributed Recovery

<!-- queuebit-v01-legacy-doc -->
> [!WARNING]
> **Archived and no longer maintained.** The current v0.1 final-user manual is [`docs/v01/en`](../v01/en/index.md). This page remains for historical context only; do not use its APIs, commands, configuration, or examples for new integrations or implementation.

## Design premise

queuebit starts from multi-process, multi-instance, multi-node deployments. It does not treat local memory as the formal coordination layer.

Queue, job, lease, timer, retry, and scheduler coordination state must close over Redis.

## Redis-only

The first version only integrates Redis. It does not provide memory, database, or alternative broker backends.

This narrows the failure model: all instances coordinate through the same shared state, and the semantics can focus on Redis atomic operations, expiration, time windows, and recovery scans.

## At-least-once

queuebit does not target exactly-once delivery. It prioritizes recoverability.

When a worker crashes, an ack is lost, a lease expires, or the network jitters, a job may be delivered again. Users should understand that:

- successful processing can still be retried if ack is lost;
- the same job handler may run more than once;
- business idempotency, deduplication, or state-machine protection belongs to the application domain.

## Lease recovery

A worker needs a lease while processing a job. When that lease expires or cannot be renewed, the system should treat the job as recoverable rather than leaving it stuck as active forever.

If a worker cannot confirm that it still owns the lease, it should stop pulling new jobs and let the current job move through recovery.

## Single-active scheduler

Delayed promotion and retry rescheduling advance time-based states. If multiple schedulers advance the same domain, duplicate delivery or state drift may occur.

queuebit's constraint is simple: one active scheduler may advance a scheduler domain at a time. If active ownership cannot be confirmed, advancement must stop.

## Graceful drain

Graceful drain lets a worker stop accepting new jobs while attempting to finish work it already claimed.

Drain does not force success. If the worker disappears or its lease expires during drain, the job must still return to a recoverable state.

## Related docs

- Redis keyspace, state collections, and atomic transitions are covered by [Redis model](./redis-model.md).
- Worker claim, renew, ack/fail, and drain are covered by [Worker and Scheduler Lifecycle](./worker-lifecycle.md).
- Redis unavailable, worker crash, lost ack, and scheduler uncertainty are covered by [Failure Modes and Recovery](./failure-modes.md).
- API exposure for these semantics is covered by [API reference](./target-api.md).
