# Configuration field dictionary

<span class="manual-label">Configuration · types, defaults, constraints, and scope</span>

Use this page for exact fields. Do not start here for first integration; start with [Quick start](./quick-start.md). If you are deciding Redis, Worker, or batch-processing values, start with [Configure Redis and Workers](./configuration-recipes.md).

## Common fields first

| Need | Field | When to change it |
|---|---|---|
| Redis address | `connection` | Local, production, TLS, or Sentinel addresses differ |
| Environment isolation | `namespace` | Separate dev, staging, prod, or business lines |
| Worker concurrency | `workerDefaults.concurrency` | How many jobs one Worker process handles at once |
| Worker shutdown wait | `workerDefaults.drainTimeoutMs` | Deploy, restart, or scale down while active jobs finish |
| Queue watermarks | `queues.*.backpressure` | Prevent Redis from being flooded by a burst |
| Database batch processing | `batchRuns` | Only when using `runs.start` to scan database records |
| History retention | `retention` | Control how long successful, failed, and Completion history stays readable |
| Health and metrics | `observability` | Wire `/healthz`, Prometheus, or log level |
| Payload size | `limits` | Job data, results, or bulk submissions can be large |
| Deduplication identity window | `deduplication` | Return the same record for repeated submissions of one business identity |

## Naming and static validation

`namespace` has no shared static default. Queuebit resolves it in this order: the config object's `namespace`, `QUEUEBIT_NAMESPACE`, then a stable `app:<normalized-package-name>:<hash>` value derived from the nearest `package.json` name. If no package name is available, configuration fails with `QB_CONFIG_INVALID` instead of sharing a keyspace. Namespace values accept 1 to 128 characters: letters, digits, colon, underscore, and hyphen. Queue, source, mapper, and handler references accept 1 to 192 characters with the same character set. Dots are not valid name characters. Unknown fields are never silently ignored.

**Upgrade from the old static default:** an application that previously omitted `namespace` used `default`. Keep both its API and Worker on that existing keyspace by setting `namespace: 'default'` or `QUEUEBIT_NAMESPACE=default` during the upgrade. Drain or migrate old jobs before removing that override and switching to the automatic application namespace.

## Root configuration

| Field | Type | Required | Default | Purpose |
|---|---|---:|---|---|
| `connection` | `RedisConnection` | no | direct `127.0.0.1:6379/0` | Redis endpoint and server policy |
| `namespace` | `string` | no | derived application namespace | Redis keyspace isolation; explicit code value has highest priority |
| `workerDefaults` | `WorkerOptions` | no | below | Worker defaults |
| `scheduler` | `SchedulerOptions` | no | cooperative | Time advancement mode and domain |
| `queues` | `Record<string, QueueConfig>` | no | `{}` | Declared queues and backpressure |
| `batchRuns` | `Record<string, BatchRunConfig>` | no | `{}` | Named batch definitions |
| `retention` | `RetentionConfig` | no | below | History and recovery information |
| `observability` | `ObservabilityConfig` | no | below | Logger, metrics, and health |
| `limits` | `PayloadLimits` | no | below | Serialized payload and bulk limits |
| `deduplication` | `DeduplicationConfig` | no | below | Business identity retention windows |

Coordinator concurrency, source timeout, completion delivery limits, and role identity are process options passed to `client.createCoordinatorRunner(runtime, options)`, not root fields in `queuebit.config.ts`. The optional CLI role host accepts corresponding flags, but it is not the normal integration path.

## Redis connection

URL, direct host/port, and Sentinel/master-name modes are mutually exclusive.

| Field | Type | Default | Constraint/error |
|---|---|---|---|
| `url` | string | none | Mutually exclusive with host/Sentinel |
| `host` | string | none | Required for direct fields |
| `port` | number | 6379 | 1 to 65535 |
| `username/password` | string | none | Redis ACL |
| `database` | number | 0 | Non-negative integer |
| `tls` | object/true | none | Direct Redis TLS |
| `sentinels` | `{host,port}[]` | none | At least two addresses |
| `masterName` | string | none | Required for Sentinel |
| `sentinelUsername/password` | string | none | Sentinel ACL |
| `connectTimeoutMs` | number | 5000 | Positive integer |
| `commandTimeoutMs` | number | 5000 | Positive integer |
| `requestRetryLimit` | number | 1 | Producer/inspect/control; non-negative |
| `backgroundReconnect.initialDelayMs` | number | 250 | First background reconnect cap; positive |
| `backgroundReconnect.maxDelayMs` | number | 30000 | Backoff cap, not below initial |
| `backgroundReconnect.factor` | number | 2 | Exponential factor from 1 through 10 |
| `backgroundReconnect.jitter` | `full` | full | Random wait from zero through the current cap |
| `backgroundReconnect.logThrottleMs` | number | 30000 | Minimum repeated-log interval per role/endpoint |
| `serverPolicy.mode` | `warn/strict` | warn | Production should explicitly use strict |

Strict mode does not become ready when `maxmemory-policy` is not `noeviction`, persistence is disabled or failing, or critical policy cannot be read.

`requestRetryLimit` applies only to Producer, inspect, and control commands. Worker, Coordinator, and cooperative time advancement stop new claim/load/dispatch/promotion work during a Redis outage and reconnect indefinitely using `backgroundReconnect` until drain or close. The first failure is logged immediately; the same role/endpoint logs at most once per `logThrottleMs` afterward. Health is `not_ready`, not `degraded`, while disconnected.

## Direct job options

Direct job retry, timeout, delay, backoff, deduplication key, and business `idempotencyKey` are supplied per `jobs.add` / `jobs.addBulk` request. The current config file has no root `jobDefaults` or per-queue `jobDefaults` field.

## Queue and shared backpressure

When a queue is declared, Queuebit fills these built-in defaults unless you override any watermark field:

| Field | Default | Rule |
|---|---:|---|
| `backpressure.highWatermarkJobs` | 10000 | High for waiting, delayed, and retrying jobs |
| `backpressure.lowWatermarkJobs` | 5000 | Must be below high |
| `backpressure.highWatermarkBytes` | 268435456 | 256 MiB non-terminal payload high |
| `backpressure.lowWatermarkBytes` | 134217728 | Must be below high |

Either high watermark sets the shared latch. Both dimensions must fall to or below low to clear it. BatchRun and direct `jobs.add/addBulk` share this boundary. A single request over high fails with `QB_BACKPRESSURE_REQUEST_TOO_LARGE` instead of waiting forever.

## Worker

| Field | Default | Constraint |
|---|---:|---|
| `concurrency` | 1 | Active jobs per process; cluster total is the sum |
| `leaseMs` | 30000 | Job owner lease |
| `renewIntervalMs` | 10000 | Must be `< leaseMs/2` |
| `pollIntervalMs` | 1000 | Idle polling upper bound |
| `drainTimeoutMs` | 60000 | Shutdown wait for active work |
| `maxStalledRecoveries` | 2 | Lease-loss recovery limit |
| `heartbeatIntervalMs` | 5000 | Role heartbeat write interval |
| `heartbeatTtlMs` | 15000 | Role heartbeat TTL |

## CoordinatorRunner options

Pass these options to `client.createCoordinatorRunner(runtime, options)`. The client supplies config, Redis, role registry, and observability; your application supplies only the role behavior below. A `coordinatorId` is generated unless you pass one. The optional CLI role host accepts corresponding flags, and its remote drain command requires that same identity so Redis can write a cooperative drain request for the intended role.

| Field | Default | Constraint / effect |
|---|---:|---|
| `coordinatorId` | generated | Stable per service instance; used for role heartbeat and remote drain |
| `concurrency` | 1 | Positive integer; max runnable Runs advanced per polling tick |
| `leaseMs` | 30000 | Positive integer; Run advancement lease |
| `sourceTimeoutMs` | 30000 | Positive integer; one source load timeout |
| `pollIntervalMs` | `scheduler.pollIntervalMs` | Positive integer; wait before the next polling tick |
| `completionLimit` | 25 | Integer from 1 through 100; due completion events delivered per tick |
| `domain` | `scheduler.domain` | Role-heartbeat ownership scope |
| `heartbeatIntervalMs` | `scheduler.heartbeatIntervalMs` | Positive integer; role heartbeat interval |
| `heartbeatTtlMs` | `scheduler.heartbeatTtlMs` | Must be greater than heartbeat interval |
| `drainTimeoutMs` | `scheduler.drainTimeoutMs` | Default wait for `drain()` / `stop()` |
| `onError` | none | Receives heartbeat, completion-delivery, and Run-advance failures |

One CoordinatorRunner owns one polling loop at a time: it delivers due completion events, lists runnable Runs, and advances up to `concurrency` of them. Read `runner.status().lastError` and attach `onError` to your application logger; failures are not silently discarded. `drain()` stops new polling, heartbeats the role as draining, and waits for active work. If it reaches its deadline it throws `QB_COORDINATOR_DRAIN_TIMEOUT` and remains `draining`, so the host can retry `stop()` after active work settles. Retry that runner directly before calling `client.close()`: client close is terminal cleanup and releases an owned Redis connection even when it reports a cleanup failure.

## Time advancement and Scheduler

| Field | Default | Meaning |
|---|---:|---|
| `mode` | cooperative | v0.1 accepts `cooperative` only |
| `domain` | default | Single-active ownership scope |
| `leaseMs` | 30000 | Owner lease |
| `renewIntervalMs` | 10000 | `< leaseMs/2` |
| `pollIntervalMs` | 1000 | Due-work scan |
| `promotionBatchSize` | 500 | Per-cycle promotion limit |
| `drainTimeoutMs` | 60000 | Shutdown window |
| `heartbeatIntervalMs` | 5000 | Role heartbeat write interval |
| `heartbeatTtlMs` | 15000 | Role heartbeat TTL |

`mode` is a constant `cooperative` in the current schema. Passing any other value returns `QB_CONFIG_INVALID`. Only background Workers participate; Web/Producer does not. Multiple Workers compete for one generation-fenced effective owner per domain.

<a id="batchrun-definition"></a>
## BatchRun definition

| Field | Required | Default | Rule |
|---|---:|---:|---|
| `version` | yes | none | Positive integer; increase on behavior change |
| `queue` | yes | none | Must be declared |
| `source` | yes | none | Named runtime registration |
| `mapper` | yes | none | Named runtime registration |
| `inputSchema` | no | none | Validate Run input when present |
| `pageSize` | no | 100 | Positive integer and payload limits |
| `dispatch.mode` | no | sequential | `sequential/paced` |
| `dispatch.intervalMs` | no | 0 | Non-negative integer |
| `dispatch.maxInFlightBatches` | no | 1 | Sequential requires 1 |
| `completion.batch/run` | no | none | Named handler registration |

Completion handler policy supports `handler`, optional `attempts`, and optional `backoff.type/delayMs/maxDelayMs`. The default `attempts` value is `3`. Completion handler config has no `timeoutMs` or `jitter` field in the current runtime.

`inputSchema` is stored with the BatchRun definition and compiled when `runs.start` validates input. Invalid Run input returns `QB_RUN_INPUT_INVALID` with validation details. Keep schemas simple, JSON-serializable, and versioned with the BatchRun definition.

## Retention

| Field | Default | Meaning |
|---|---:|---|
| `completedJobs.ageMs/maxCount` | 24h / 100000 | Successful-job diagnostic window |
| `failedWork.ageMs/maxCount` | 7d / 100000 | Saved mapper/job failure details |
| `terminalRuns.ageMs/maxCount` | 30d / 10000 | Terminal Run summary and audit chain |
| `completionEvents.ageMs/maxCount` | 30d / 10000 | Delivered/not-required Completion event details |

Active/non-terminal Runs and undelivered completion events cannot be removed. After recovery details expire, `runs.retryFailed` returns `QB_RUN_STATE_CONFLICT` and the caller should create a fresh Run from the business database.

## Deduplication window and expired identity records

| Field | Default | Constraint |
|---|---:|---|
| `jobKeyTtlMs` | 7d | Not shorter than completedJobs age |
| `runKeyTtlMs` | 30d | Not shorter than terminalRuns and completionEvents age |

If full details are cleaned first, Queuebit keeps lightweight identity, digest, version, and state records through the TTL. Identical input returns the original identity with `detailsExpired=true`; deleted payload, result, or Completion summary is never fabricated. `completionEvents.ageMs/maxCount` independently controls the detail window for safely finished Completion events. After details expire, `completions.list/get` can still show the identity, but that lightweight record no longer consumes the cleanup window.

## Payload limits

| Field | Default bytes |
|---|---:|
| `maxRunInputBytes` | 65536 |
| `maxJobDataBytes` | 262144 |
| `maxJobResultBytes` | 65536 |
| `maxPageBytes` | 8388608 |
| `maxBulkJobs` | 1000 entries |
| `maxBulkBytes` | 8388608 |

Oversized data fails before Redis and reports actual size, limit, and reduction guidance. Keep business IDs and processor-required fields in payloads; store files and binary data in object storage.

## Observability

| Field | Default | Meaning |
|---|---:|---|
| `logLevel` | info | debug/info/warn/error |
| `metrics.enabled` | true | In-process registry |
| `metrics.format` | prometheus | Text export |
| `metrics.prefix` | `queuebit_` | Metric prefix |
| `health.staleAfterMs` | 45000 | Heartbeat stale threshold |

Queuebit core starts no HTTP server. The application mounts and protects metrics/readiness endpoints. Per-process metrics must not be presented as a whole-cluster aggregate.

## Role-specific lazy loading

- Producer does not load runtime.
- Worker activates processors and Worker resources only.
- Coordinator activates source, mapper, completion, and Coordinator resources only.
- Cooperative time advancement reuses Worker processes without activating extra business database or source resources.
- Importing runtime has no connection side effects; factories open on first use and lifecycle close releases them.
