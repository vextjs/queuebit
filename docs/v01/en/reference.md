# Reference

<span class="manual-label">Reference · exact lookup</span>

Use these pages to look up methods, fields, commands, states, and errors. They do not replace [Quick start](./quick-start.md) or task-oriented guides.

## Version Status

This page records the intended stable public usage for Queuebit v0.1. Whether a method, configuration field, state, or CLI command is available depends on the npm package version you installed and its release notes.

## Find by task

| Need | Page |
|---|---|
| Client, jobs, Runs, or completion methods | [API quick lookup](./target-api.md) |
| Source, mapper, processor, or completion runtime contract | [API quick lookup](./target-api.md#runtime-registration) |
| Configuration type, default, or mutual exclusion | [Configuration field dictionary](./cli-and-config.md) |
| Start, inspect, control, or drain command | [CLI reference](./cli-reference.md) |
| Job, Run, or Completion state | [States and errors](./failure-modes.md) |
| Whether Node, Redis, or vext is supported | [Can my environment use Queuebit?](./compatibility.md) |
| Safe recovery by incident symptom | [Failure runbooks](./failure-runbooks.md) |

## Public naming overview

| Domain | Stable entry |
|---|---|
| Create client | `createQueuebitClient({ config, logger? })` |
| Static config | `defineQueuebitConfig()` |
| Runtime registration | `defineQueuebitRuntime()` plus named source/mapper/processor/completion helpers |
| Direct jobs | `queuebit.jobs.add/addBulk/get/list/cancel/retryFailed` |
| BatchRun | `queuebit.runs.start/get/list/listFailures/pause/resume/cancel/retryFailed` |
| Completion | `queuebit.completions.get/list/retry` |
| Lifecycle | `queuebit.close()` and background-role SIGTERM drain |

## Deferred capabilities

v0.1 excludes repeatable/cron jobs, DAG/Flows, Redis Cluster, non-Redis backends, priority, global rate limiter, partition/key ordering, Dashboard/Admin UI, and CDC/unbounded sources. It does not promise exactly-once delivery, strict FIFO, tenant fairness, or automatic rollback of external side effects.

## Maintainer entry

Users do not need Redis key/Lua or internal acceptance details. Implementers start at [Architecture](./architecture.md), [Redis model](./redis-model.md), [Internal worker lifecycle](./worker-lifecycle.md), and [Development guardrails](./development-contract.md).
