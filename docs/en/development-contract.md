# Development Guardrails

<!-- queuebit-v01-legacy-doc -->
> [!WARNING]
> **Archived and no longer maintained.** The current v0.1 final-user manual is [`docs/v01/en`](../v01/en/index.md). This page remains for historical context only; do not use its APIs, commands, configuration, or examples for new integrations or implementation.

## Purpose

<span class="manual-label">Maintainer / Internals</span>

This page answers one question: how development should stay aligned with the final user manual.

The queuebit documentation site is not a current-status note. It is the final v0.1 user manual and engineering acceptance entry. Runtime, CLI, Redis adapter, vext adapter, tests, and operations work must be checked against these docs.

## Documentation source of truth

| Document | Role |
|----------|------|
| [Architecture](./architecture.md) | Defines core/adapter, Redis-only, distributed topology boundaries |
| [API reference](./target-api.md) | Defines public API semantics and lifecycle |
| [CLI and Configuration](./cli-and-config.md) | Defines config, commands, process entries, and vext config relationship |
| [Redis model](./redis-model.md) | Defines keyspace, state transitions, and atomicity |
| [Worker and Scheduler Lifecycle](./worker-lifecycle.md) | Defines producer/worker/scheduler runtime paths |
| [Failure Modes and Recovery](./failure-modes.md) | Defines failure modes, troubleshooting, and idempotency requirements |
| [Operations and troubleshooting](./operations.md) | Defines metrics, introspection, and troubleshooting direction |

## Pre-development checks

Before implementation work starts, check:

| Check | Pass standard |
|-------|---------------|
| Scope mapping | The change maps to specific manual pages |
| Terminology | Queue, Job, Worker, Scheduler, Lease, Domain keep the same meaning |
| Manual acceptance | Runtime behavior satisfies quick start, API, CLI, vext, and operations user paths |
| Core/adapter boundary | vext adapter does not turn core into vext-only |
| Redis-only | No other broker or database backend is introduced |
| Distributed semantics | multi-instance, lease, scheduler single-active, and recovery have a path |

## Implementation alignment matrix

| Development item | Docs to update | Must verify |
|------------------|----------------|-------------|
| Queue/Producer API | target-api, quick-start, reference | enqueue, delayed, idempotency, status query |
| Worker runtime | target-api, worker-lifecycle, failure-modes | claim, renew, ack/fail, drain, crash recovery |
| Scheduler runtime | worker-lifecycle, redis-model, operations | single-active, delayed promotion, retry, stalled |
| Redis keyspace | redis-model, operations | atomic transitions, namespace isolation, races |
| CLI/config | cli-and-config, quick-start, reference | config validation, command help, process topology |
| vext adapter | vext-integration, architecture, cli-and-config | producer/worker split, reload drain, metrics |
| Metrics | operations, failure-modes, reference | queue depth, active, retry, delayed, stalled, identity |

## Documentation change rules

- If implementation proves a manual page unreasonable, update that user manual page first with rationale, then change code.
- If implementation fills manual capability, validate it with quick start, API, CLI, vext, and operations examples.
- Public API, CLI, config field, or Redis key naming changes must update README, reference, quick start, and relevant manual pages.
- Do not finish with only the Chinese page updated; English must stay in sync.
- Do not put DevCodex reports, internal ledgers, or maintainer checklists in the public user path.

## Run the documentation site locally

From the repository root:

```bash
npm install --prefix website
npm run docs:dev
npm run docs:build
npm run docs:preview
```

You can also work inside `website/` with `npm run dev`, `npm run build`, and `npm run preview`. The root package only proxies scripts; `website/` owns documentation dependencies.

## Acceptance route

Documentation-stage validation:

- `npm --prefix website run build`
- multilingual page structure and navigation consistency check
- docs-site smoke test for new pages
- scan for current-status narrative left in the user primary path

Runtime-stage validation will add:

- unit tests for state machine, config validation, and error categories;
- integration tests for Redis keyspace, concurrent claim, lease expiration, scheduler single-active;
- scenario tests for worker crash, brief Redis outage, drain timeout, and approximate lost ack;
- package boundary checks for expected npm package contents.
