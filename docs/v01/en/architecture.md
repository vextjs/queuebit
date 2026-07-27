# Architecture

<span class="manual-label">Maintainer · not an integration prerequisite</span>

This page is for implementers and maintainers checking internal boundaries. Users integrating Queuebit do not need it; start with [Quick start](./quick-start.md), [Run one background job](./job-recipes.md), or [Configure Redis and Workers](./configuration-recipes.md).

## Goals

- Redis-only: shared correctness state never lives only in local memory or files.
- Distributed-first: claims, cursors, batches, completion, and role ownership converge across processes.
- Core/host separation: vext bridges client, dependency injection, and lifecycle; core does not depend on the app.
- At least once: fencing protects Redis commits and business idempotency protects external side effects.

## Module boundaries

| Module | Owns | Must not own |
|---|---|---|
| config/schema | Static types, defaults, cross-validation, canonical digest | Business connections and handler functions |
| client/producer | Job/Run create, query, control, prompt failure | Processor/source execution |
| worker runtime | claim, renew, process, settle, drain | Source and Run cursor |
| coordinator runtime | source, mapper, Batch, cursor, completion | Processor execution |
| time advancement | Promote delayed/retry work and recover timers | Business handler or DB source |
| Redis adapter | Keyspace, atomic transitions, indexes, retention | Business authorization and side effects |
| vext adapter | Plugin, extension, logger, onClose, consumer type | Implicitly start background roles |

## Atomic boundaries

1. Job claim writes owner, attempt, lease generation, and expiry together.
2. Job settlement checks jobId, attempt, generation, and workerId and commits Batch counters in the same operation.
3. Source-page dispatch commits Batch identity, cursor range, record summary, replay envelopes, jobs, and dispatchCursor together.
4. Checkpoint advances only across a continuous prefix of execution plus completion barriers.
5. Completion claim/settle checks eventId, attempt, delivery generation, and ownerId.
6. Queue jobs/bytes backpressure counts share atomic boundaries with add, addBulk, and Batch dispatch.

## Role composition and lazy loading

`queuebit.runtime.ts` can be the single composition root, but importing it opens no connection. Worker activates processors only and Coordinator activates source/mapper/completion only. Cooperative time advancement reuses the Worker's Redis connection and ownership loop without activating source, completion, or extra business DB/HTTP resources. Large projects may split role modules while the canonical example keeps one definition truth source.

## Milestone closure

| Milestone | Scope | Closure evidence |
|---|---|---|
| M0 Queue kernel | Queue, Job, Producer, Worker, delay/retry, lease/fencing, direct replacement, cooperative time, vext Producer | Multi-Worker, stale-attempt rejection, crash redelivery, addBulk atomic/limits, loader/ESM/CJS/types/consumer smoke |
| M1 BatchRun closed loop | Source, Mapper, Coordinator, Batch, completion, dual cursor, blocked/recovery | Seed DB to final completion, multi-Worker, Coordinator crash, cursor, completion generation, cancellation invariants |
| M2 Production foundation (M2A–M2K in source) | paced/backpressure, TLS/ACL/Sentinel mapping, metrics/health/CLI foundation, bilingual site | Local foundation tests + environment-gated Redis harnesses. **Not** complete v0.1 until target Redis `>=7.2` execution, fault/Sentinel failover evidence, destructive purge/full tombstone, production scrape/auth/network evidence, clean example E2E, and publish gates close |

M0 and M1 are internal milestones and cannot independently be described as complete v0.1. M2K source delivery is foundation, not release-complete v0.1.
