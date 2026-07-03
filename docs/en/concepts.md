# Core Concepts

## Concept overview

queuebit's model is built around shared state in Redis. Even though vext is the first integration target, the core model should not depend on a `vext app` object, HTTP worker count, or framework lifecycle.

## Queue

A Queue is the logical entry for a group of jobs. It defines the boundary for enqueueing, observing state, draining, and future reference APIs.

Each queue should have a clear namespace so multiple applications or environments do not mix state in Redis.

## Job

A Job is a unit of work submitted by a producer and handled by a worker.

The first-version delivery model is at-least-once. If a worker crashes, an ack is lost, a lease expires, or the network jitters, the same job may be delivered again. Business handlers should be designed with idempotency or deduplication in mind.

## Producer

A Producer submits jobs. It should be usable inside an application process without requiring that process to also run workers or schedulers.

In vext scenarios, the producer can sit close to the HTTP/API layer, but each HTTP worker should not automatically become a queue worker.

## Worker

A Worker claims, processes, and acknowledges jobs. It should run through an explicit standalone entry or command.

When a worker cannot confirm its lease, cannot renew it, or is draining gracefully, it must stop pulling new jobs.

## Scheduler

A Scheduler advances time-based states such as delayed promotion and retry rescheduling.

Within the same queue namespace and scheduler domain, only one active scheduler may advance state at a time. If active ownership cannot be confirmed, the scheduler should stop advancing rather than risk duplicate promotion.

## Lease

A Lease is the time-bound claim that lets a worker process a job.

It is not an exactly-once guarantee. It allows the system to detect work that can be recovered after worker death, timeout, or connection issues.

## Namespace and Scheduler Domain

Namespace isolates queue state in Redis.

Scheduler Domain defines the scope where scheduler single-active behavior applies. If recurring dispatch is introduced later, it should reuse the same single-active constraint instead of adding a conflicting advancement mechanism.

## Related docs

| Concept | Continue reading |
|---------|------------------|
| Public semantics for Queue / Producer / Worker / Scheduler | [API reference](./target-api.md) |
| Namespace, Domain, and Redis keyspace | [Redis model](./redis-model.md) |
| Worker renewal, drain, and recovery | [Worker and Scheduler Lifecycle](./worker-lifecycle.md) |
| Crash, lost ack, and redelivery | [Failure Modes and Recovery](./failure-modes.md) |
| producer / worker split in vext | [vext integration](./vext-integration.md) |
