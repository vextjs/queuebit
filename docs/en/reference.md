# Reference Index

## How to use this page

<span class="manual-label">v0.1 final user manual</span>

This page helps users find API, configuration, CLI, vext adapter, and operations references. queuebit documentation is organized by task first, then by object.

## Find by task

| Task | Entry |
|------|-------|
| Run the first batch of jobs | [Quick Start](./quick-start.md) |
| Enqueue jobs from a vext project | [vext integration](./vext-integration.md) |
| Configure Redis, worker, and scheduler | [CLI and configuration](./cli-and-config.md) |
| Check Node / Redis / Cluster support | [Environment and Compatibility Boundary](./compatibility.md) |
| Troubleshoot waiting / delayed / retry / stalled jobs | [Operations and troubleshooting](./operations.md) |
| Understand redelivery and recovery | [Failure Modes and Recovery](./failure-modes.md) |

## Find by object

| Object | Entry |
|--------|-------|
| `Queue.addBulk` / job options / job state | [API reference](./target-api.md) |
| `Worker.run` / lease / drain | [Worker and Scheduler Lifecycle](./worker-lifecycle.md) |
| `Scheduler.run` / scheduler domain | [Worker and Scheduler Lifecycle](./worker-lifecycle.md) |
| Redis keyspace and state transitions | [Redis model](./redis-model.md) |
| Queue / Job / Producer / Worker / Scheduler concepts | [Core concepts](./concepts.md) |

## Find by command

| Command | Entry |
|---------|-------|
| `queuebit worker start` | [CLI and configuration](./cli-and-config.md) |
| `queuebit worker drain` | [CLI and configuration](./cli-and-config.md) |
| `queuebit scheduler start` | [CLI and configuration](./cli-and-config.md) |
| `queuebit inspect queue` | [Operations and troubleshooting](./operations.md) |
| `queuebit inspect workers` | [Operations and troubleshooting](./operations.md) |
| `queuebit inspect scheduler` | [Operations and troubleshooting](./operations.md) |

## Implementation appendix

Regular integration users do not need these pages first. They are for future development and maintenance work to verify that implementation stays aligned with the final user manual.

| What to verify | Entry |
|----------------|-------|
| Boundaries between core, Redis runtime, worker, scheduler, and vext adapter | [Architecture](./architecture.md) |
| Redis keyspace, state collections, and atomic transition model | [Redis model](./redis-model.md) |
| How development should satisfy the user manual | [Development guardrails](./development-contract.md) |
