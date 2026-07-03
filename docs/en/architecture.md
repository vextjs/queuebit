# Architecture

## Page positioning

<span class="manual-label">v0.1 final user manual</span>

This page supports the final user manual by explaining why queuebit v0.1 separates core, Redis coordination, worker runtime, scheduler runtime, and the vext adapter. Users do not need this page before starting; implementers use it to keep future development aligned with the manual.

## Design goals

The first queuebit version solves one focused problem: a Redis-backed, distributed-first, BullMQ-like job queue foundation for Node.js and vext projects.

The first version must satisfy:

- **Redis-only**: shared state, coordination, time progression, and recovery semantics are designed around Redis.
- **Distributed-first**: multiple producers, workers, deployments, and restarts are the default model.
- **At-least-once**: recovery is prioritized over exactly-once guarantees.
- **Core / adapter split**: core does not depend on vext; the vext adapter only integrates with the host.
- **Observable**: queue depth, active jobs, retry, delayed, stalled recovery, worker identity, and scheduler identity must be visible.

## Layer boundaries

| Layer | Owns | Does not own |
|-------|------|--------------|
| Queuebit core | Queue, job, producer, worker, scheduler, lease, retry, delay, drain, metrics, Redis coordination | vext lifecycle, HTTP routes, application config parsing |
| Redis coordination | keyspace, atomic transitions, locks/leases, state collections, timing windows, event stream | business idempotency, business transactions, cross-Redis disaster recovery |
| Worker runtime | claiming, leasing, handling, ack/fail, graceful drain, stalled handling | automatic consumption from every Web process, business handler idempotency |
| Scheduler runtime | delayed promotion, retry rescheduling, single-active progression, recovery scans | business cron orchestration or workflow orchestration |
| vext adapter | vext config entry, lifecycle hooks, health checks, metrics exposure, recommended process entries | rewriting core semantics or hiding worker/scheduler topology |
| Documentation contract | user path, API, Redis model, failure semantics, acceptance matrix | internal ledgers or replacing tests and implementation facts |

## Module responsibilities

| Module | Target capability | Implementation constraint |
|--------|-------------------|---------------------------|
| Queue | Logical queue creation, enqueue, status observation, drain | Must include namespace; process memory is not source of truth |
| Job | payload, attempts, state, timestamps, error summary, business idempotency key | Redelivery must be explicit; no exactly-once promise |
| Producer | Submit jobs to Redis and return traceable job ids | Usable from Web/API processes; must not implicitly start workers |
| Worker | Claim, process, renew, ack/fail, drain | Must stop claiming when lease ownership is uncertain |
| Scheduler | Promote delayed/retry/stalled work | Must be single-active inside one scheduler domain |
| Metrics | Queue and runtime observability | Names may evolve, but operational questions must stay covered |

## Deployment topology

The recommended v0.1 topology explicitly separates three process roles:

| Process type | Required | Typical location | Notes |
|--------------|:--------:|------------------|-------|
| Producer | Yes | vext HTTP/API process | Submits jobs only; does not consume by default |
| Worker | Yes | Dedicated worker process or container | Horizontally scalable, controlled by concurrency and leases |
| Scheduler | Conditional | Dedicated scheduler process or worker-side mode | Single-active per domain; may be disabled without delayed/retry |

Do not default every HTTP worker into a queue worker. Do not hide unconditional schedulers in every application instance.

## Invariants

- Redis is the only first-version coordination backend.
- Every active job needs a recovery path.
- Delayed and retry progression must be handled by the active scheduler for the scheduler domain.
- A worker that cannot renew or prove its lease must stop claiming new jobs.
- Drain stops new claims; it does not guarantee success for already active jobs.
- The vext adapter must not make core APIs usable only through a vext app object.
- When the final user manual and implementation disagree, decide whether implementation drifted or the manual needs revision before proceeding.

## Implementation acceptance

Future implementation work must answer:

| Acceptance question | Related docs |
|---------------------|--------------|
| Do APIs cover producer, worker, scheduler, and metrics lifecycles? | [API reference](./target-api.md) |
| Can CLI and config express separate worker / scheduler topology? | [CLI and Configuration](./cli-and-config.md) |
| Do Redis keyspace and transitions support recovery? | [Redis model](./redis-model.md) |
| What happens on worker crash, lost ack, or uncertain lease? | [Worker and Scheduler Lifecycle](./worker-lifecycle.md) and [Failure Modes](./failure-modes.md) |
| Does vext stay an adapter instead of swallowing core boundaries? | [vext integration](./vext-integration.md) |
| Did this implementation stay aligned with the user manual? | [Development guardrails](./development-contract.md) |
