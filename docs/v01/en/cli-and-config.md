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

`namespace` defaults to `default` and accepts 1 to 128 characters: letters, digits, colon, underscore, and hyphen. Queue, source, mapper, and handler references accept 1 to 192 characters with the same character set. Dots are not valid name characters. Unknown fields are never silently ignored.

## Root configuration

| Field | Type | Required | Default | Purpose |
|---|---|---:|---|---|
| `connection` | `RedisConnection` | no | direct `127.0.0.1:6379/0` | Redis endpoint and server policy |
| `namespace` | `string` | no | `default` | Environment/business keyspace isolation |
| `workerDefaults` | `WorkerOptions` | no | below | Worker defaults |
| `scheduler` | `SchedulerOptions` | no | cooperative | Time advancement mode and domain |
| `queues` | `Record<string, QueueConfig>` | no | `{}` | Declared queues and backpressure |
| `batchRuns` | `Record<string, BatchRunConfig>` | no | `{}` | Named batch definitions |
| `retention` | `RetentionConfig` | no | below | History and recovery information |
| `observability` | `ObservabilityConfig` | no | below | Logger, metrics, and health |
| `limits` | `PayloadLimits` | no | below | Serialized payload and bulk limits |
| `deduplication` | `DeduplicationConfig` | no | below | Business identity retention windows |

Coordinator concurrency, source timeout, completion delivery limits, and role identity are process options passed when starting the Coordinator. They are not root `queuebit.config.ts` fields in the current runtime.

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

## Coordinator role options

Coordinator options are supplied by the Coordinator factory or CLI role process, not by root config. The role uses a generated `coordinatorId` unless one is passed. The CLI drain command requires that same identity so Redis can write a cooperative drain request for the intended role.

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
