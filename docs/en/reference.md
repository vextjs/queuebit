# Reference Index

<!-- queuebit-v01-legacy-doc -->
> [!WARNING]
> **Archived and no longer maintained.** The current v0.1 final-user manual is [`docs/v01/en`](../v01/en/index.md). This page remains for historical context only; do not use its APIs, commands, configuration, or examples for new integrations or implementation.

## How to use this page

<span class="manual-label">User reference index</span>

This page helps users find API, configuration, CLI, vext adapter, and operations references. queuebit documentation is organized by task first, then by object.

## Find by task

| Task | Entry |
|------|-------|
| Run the first batch of jobs | [Quick Start](./quick-start.md) |
| Build batch, delayed, retry, concurrency, event, and metric flows | [Job recipes](./job-recipes.md) |
| Scale Workers, roll deployments, and fail over Scheduler | [Distributed workers](./distributed-workers.md) |
| Prevent duplicate external side effects | [Idempotency patterns](./idempotency-patterns.md) |
| Enqueue jobs from a vext project | [vext integration](./vext-integration.md) |
| Deploy dedicated Worker, Scheduler, and production Redis | [Production deployment](./production-deployment.md) |
| Choose Redis, Worker, and Scheduler values by environment | [Configuration recipes](./configuration-recipes.md) |
| Look up configuration fields and CLI | [CLI and configuration](./cli-and-config.md) |
| Check Node / Redis / Cluster support | [Environment and Compatibility Boundary](./compatibility.md) |
| Troubleshoot waiting / delayed / retry / stalled jobs | [Operations and troubleshooting](./operations.md) |
| Understand redelivery and recovery | [Failure Modes and Recovery](./failure-modes.md) |
| Follow production incident steps | [Failure runbooks](./failure-runbooks.md) |

## Find by object

| Object | Entry |
|--------|-------|
| `Queue.addBulk` / job options / job state | [API reference](./target-api.md) |
| `Worker.run` / `Worker.close` / drain | [API reference](./target-api.md) |
| `Scheduler.run` / `Scheduler.close` / domain | [API reference](./target-api.md) |
| Production Worker and Scheduler processes | [Production deployment](./production-deployment.md) |
| Queue / Job / Producer / Worker / Scheduler concepts | [Core concepts](./concepts.md) |

## Find by command

| Command | Entry |
|---------|-------|
| `npx queuebit worker start` | [CLI and configuration](./cli-and-config.md) |
| `npx queuebit worker drain` | [CLI and configuration](./cli-and-config.md) |
| `npx queuebit scheduler start` | [CLI and configuration](./cli-and-config.md) |
| `npx queuebit inspect queue` | [Operations and troubleshooting](./operations.md) |
| `npx queuebit inspect workers` | [Operations and troubleshooting](./operations.md) |
| `npx queuebit inspect scheduler` | [Operations and troubleshooting](./operations.md) |

## Maintainer docs (not required for integration)

Regular integration users do not need these pages first. They are for future development and maintenance work to verify that implementation stays aligned with the final user manual.

| What to verify | Entry |
|----------------|-------|
| Boundaries between core, Redis runtime, worker, scheduler, and vext adapter | [Architecture](./architecture.md) |
| Redis keyspace, state collections, and atomic transition model | [Redis model](./redis-model.md) |
| Worker / Scheduler internal phases and maintainer acceptance | [Internal lifecycle](./worker-lifecycle.md) |
| How development should satisfy the user manual | [Development guardrails](./development-contract.md) |
