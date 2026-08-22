# Configure Redis and Workers

<span class="manual-label">Production operations · start from the small config</span>

For one first background job, follow [Quick start](./quick-start.md) with an ordinary config object and a Worker business function. Adopt the `queuebit.config.ts` and `queuebit.runtime.ts` split here when you need the CLI, multiple Worker/Coordinator service hosts, BatchRun, or centralized configuration governance. API processes submit work, Worker processes execute it, and the CLI is only a validation and debugging tool. It is not required as the runtime entrypoint. Sources, mappers, and completion handlers are needed only for database batching.

## Choose Your Situation

| What you need | Configure first | Ignore for now |
|---|---|---|
| Run one background job locally or inside a Node framework | `connection`, `queues` | Sentinel, retention, BatchRun, CI |
| Run multiple Workers in production | Add `workerDefaults`, Redis strict policy, health checks | BatchRun definition |
| Page many database records into jobs | Add `batchRuns`, plus source/mapper/completion in runtime | Redis internals |

For a normal Node, Fastify, Nest, or vext integration, create the client when the service starts, pass the business function in a separate worker script, and let your process manager or container run the Worker.

Queuebit derives a stable isolated namespace from the application package name by default. When multiple deployments of the same package intentionally share Redis, set a distinct `QUEUEBIT_NAMESPACE` for each deployment.

## Advanced Files, Startup Overrides, and Runtime

Most projects use the file defaults. Pass startup options when a Worker or Coordinator service host needs a temporary concurrency or drain-timeout override. Use optional CLI flags only when the CLI is deliberately the service host.

1. Runtime overrides passed while creating a client or role.
2. Explicit process flags when the CLI is deliberately the service host, such as `--concurrency 12`.
3. Queue backpressure or BatchRun-local configuration.
4. Root Worker, Scheduler, retention, limits, deduplication, and observability defaults.
5. Queuebit built-in defaults.

Producer/API processes need static configuration and do not always load `queuebit.runtime.ts`. `config validate --runtime`, Worker, and Coordinator verify processor registration; only BatchRun requires additional source, mapper, and completion checks.

## Minimal Local Configuration: One Redis, One Queue

```ts
// queuebit.config.ts
import { defineQueuebitConfig } from 'queuebit';

export default defineQueuebitConfig({
  connection: { url: 'redis://127.0.0.1:6379/0' },
  queues: { notification: {} }
});
```

This already supports [one background job](./job-recipes.md). Add BatchRun definition, runtime registration, and Coordinator only for database batching.

## Production Redis: Managed Redis with TLS

```ts
connection: {
  host: 'redis.example.internal',
  port: 6380,
  username: 'queuebit',
  password: 'replace-with-project-value',
  database: 0,
  tls: {
    servername: 'redis.example.internal',
    rejectUnauthorized: true
  },
  connectTimeoutMs: 5_000,
  commandTimeoutMs: 5_000,
  requestRetryLimit: 1,
  backgroundReconnect: {
    initialDelayMs: 250,
    maxDelayMs: 30_000,
    factor: 2,
    jitter: 'full',
    logThrottleMs: 30_000
  },
  serverPolicy: { mode: 'strict' }
}
```

`tls.servername` must match the certificate. Do not hide certificate errors with `rejectUnauthorized:false`. `requestRetryLimit` makes Producer, inspect, and control commands fail promptly. Background Worker/Coordinator roles keep reconnecting, stop advancement, and report `not_ready` while disconnected.

## Highly Available Redis: Sentinel

```ts
connection: {
  sentinels: [
    { host: '10.0.1.11', port: 26379 },
    { host: '10.0.1.12', port: 26379 },
    { host: '10.0.1.13', port: 26379 }
  ],
  masterName: 'mymaster',
  username: 'queuebit',
  password: 'replace-with-project-value',
  sentinelUsername: 'sentinel-user',
  sentinelPassword: 'replace-with-project-value',
  database: 0,
  connectTimeoutMs: 5_000,
  commandTimeoutMs: 5_000,
  serverPolicy: { mode: 'strict' }
}
```

Sentinel helps Queuebit rediscover the new primary. It is not Redis Cluster and does not guarantee zero loss of acknowledged writes. After failover, recheck primary/replica roles, persistence, replication, and `maxmemory-policy`.

## Only for Database Batching: BatchRun Definition

```ts
batchRuns: {
  'receipt-campaign': {
    version: 1,
    queue: 'notification',
    source: 'paid-orders',
    mapper: 'receipt-jobs',
    inputSchema: {
      type: 'object',
      required: ['tenantId', 'paidBefore'],
      additionalProperties: false,
      properties: {
        tenantId: { type: 'string', minLength: 1 },
        paidBefore: { type: 'string', format: 'date-time' }
      }
    },
    pageSize: 500,
    dispatch: {
      mode: 'sequential',
      intervalMs: 2_000,
      maxInFlightBatches: 1
    },
    completion: {
      batch: { handler: 'record-receipt-batch-result' },
      run: { handler: 'record-receipt-run-result' }
    }
  }
}
```

| Choice | Recommended start | Change when |
|---|---:|---|
| `pageSize` | 100 built-in, often 500 after sizing | A page is too slow, payload is too large, or database queries are too heavy |
| `dispatch.mode` | sequential | Downstream capacity can safely accept multiple in-flight batches |
| `intervalMs` | 0 to 2000 | Downstream quotas or per-batch completion time require pacing |

## Complete Production Configuration Example

The example below puts Redis, Workers, BatchRun, retention, observability, and payload limits in one file. Do not copy all of it for a first integration; first get one background job working, then add only the sections you need.

```ts
import { defineQueuebitConfig } from 'queuebit';

export default defineQueuebitConfig({
  connection: {
    host: 'redis.example.internal', port: 6380,
    username: 'queuebit', password: 'replace-with-project-value', database: 0,
    tls: { servername: 'redis.example.internal', rejectUnauthorized: true },
    connectTimeoutMs: 5_000, commandTimeoutMs: 5_000,
    requestRetryLimit: 1,
    backgroundReconnect: {
      initialDelayMs: 250, maxDelayMs: 30_000, factor: 2,
      jitter: 'full', logThrottleMs: 30_000
    },
    serverPolicy: { mode: 'strict' }
  },
  namespace: 'prod:billing',
  workerDefaults: {
    concurrency: 8, leaseMs: 30_000, renewIntervalMs: 10_000,
    pollIntervalMs: 1_000, drainTimeoutMs: 60_000, maxStalledRecoveries: 2
  },
  scheduler: {
    mode: 'cooperative', domain: 'billing', leaseMs: 30_000,
    renewIntervalMs: 10_000, pollIntervalMs: 1_000,
    promotionBatchSize: 500, drainTimeoutMs: 60_000
  },
  queues: {
    notification: {
      backpressure: {
        highWatermarkJobs: 10_000, lowWatermarkJobs: 5_000,
        highWatermarkBytes: 268_435_456, lowWatermarkBytes: 134_217_728
      }
    }
  },
  batchRuns: {
    'receipt-campaign': {
      version: 1, queue: 'notification', source: 'paid-orders', mapper: 'receipt-jobs',
      inputSchema: {
        type: 'object', required: ['tenantId', 'paidBefore'], additionalProperties: false,
        properties: {
          tenantId: { type: 'string', minLength: 1 },
          paidBefore: { type: 'string', format: 'date-time' }
        }
      },
      pageSize: 500,
      dispatch: { mode: 'sequential', intervalMs: 2_000, maxInFlightBatches: 1 },
      completion: {
        batch: {
          handler: 'record-receipt-batch-result', attempts: 5,
          backoff: { type: 'exponential', delayMs: 1_000, maxDelayMs: 60_000 }
        },
        run: {
          handler: 'record-receipt-run-result', attempts: 10,
          backoff: { type: 'exponential', delayMs: 1_000, maxDelayMs: 300_000 }
        }
      }
    }
  },
  retention: {
    completedJobs: { ageMs: 86_400_000, maxCount: 100_000 },
    failedWork: { ageMs: 604_800_000, maxCount: 100_000 },
    terminalRuns: { ageMs: 2_592_000_000, maxCount: 10_000 },
    completionEvents: { ageMs: 2_592_000_000, maxCount: 10_000 }
  },
  observability: {
    logLevel: 'info',
    metrics: { enabled: true, format: 'prometheus', prefix: 'queuebit_' },
    health: { staleAfterMs: 45_000 }
  },
  limits: {
    maxRunInputBytes: 65_536, maxJobDataBytes: 262_144,
    maxJobResultBytes: 65_536, maxPageBytes: 8_388_608,
    maxBulkJobs: 1_000, maxBulkBytes: 8_388_608
  },
  deduplication: { jobKeyTtlMs: 604_800_000, runKeyTtlMs: 2_592_000_000 }
});
```

## Validate before startup

```bash
npx queuebit config validate --config queuebit.config.ts --runtime queuebit.runtime.ts
npx queuebit health inspect --config queuebit.config.ts --json
```

| Error | Cause | Recovery |
|---|---|---|
| `QB_CONFIG_INVALID` | Misspelled field, obsolete field, invalid value, or invalid connection mode mix | Use the [configuration field dictionary](./cli-and-config.md) |
| `QB_CONFIG_HANDLER_NOT_REGISTERED` | Definition references a missing source, mapper, or handler | Add the matching named registration in `queuebit.runtime.ts` |
| `QB_REDIS_CLUSTER_UNSUPPORTED` | Redis Cluster topology configured or detected | Use single-primary or Sentinel |
| `QB_REDIS_PREFLIGHT_FAILED` | strict policy rejects eviction, persistence, role, or unreadable policy | Repair Redis policy and validate again |
| `QB_RUN_INPUT_INVALID` | start input violates `inputSchema` | Repair input using JSON Pointer and keyword details |
