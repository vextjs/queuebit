# API Reference

<!-- queuebit-v01-legacy-doc -->
> [!WARNING]
> **Archived and no longer maintained.** The current v0.1 final-user manual is [`docs/v01/en`](../v01/en/index.md). This page remains for historical context only; do not use its APIs, commands, configuration, or examples for new integrations or implementation.

## How to use this page

<span class="manual-label">Public API reference</span>

Use this page to look up queuebit v0.1 public objects, inputs, results, states, and errors. Complete [Quick Start](./quick-start.md) first, then use [Production deployment](./production-deployment.md) when deploying.

## Usage conventions

- Use stable namespace and queue names so Producer, Worker, and Scheduler address the same logical queue.
- Create `Queue` in Web/API processes and run `Worker` and `Scheduler` in dedicated processes.
- Design handlers for at-least-once delivery because crashes or uncertain acknowledgements can redeliver jobs.
- Call `close` during application shutdown and drain Workers during deployment.
- Normal integrations do not read or modify Redis keys.

## Core objects

| Object | What it does | When to close it |
|--------|--------------|------------------|
| `Queue` | User entry for submitting jobs, reading state, and closing resources | Reused during app lifetime, releases Redis resources on close |
| `Producer` | Submit jobs and return job handles | Short-lived or application-scoped |
| `Worker` | Process jobs, renew leases, ack/fail, drain | Long-lived, must support graceful shutdown |
| `Scheduler` | Promote delayed, retry, and stalled work | Long-lived, must prove single-active status |
| `Job` | Job metadata, payload, attempt, state, and error summary | Persisted and queryable by queuebit |

Public type outline:

```ts
type QueuebitTlsOptions = {
  servername?: string;
  ca?: string | string[];
};

type QueuebitSentinelNode = {
  host: string;
  port: number;
};

type QueuebitConnection = {
  url?: string;
  host?: string;
  port?: number;
  database?: number;
  username?: string;
  password?: string;
  tls?: boolean | QueuebitTlsOptions;
  sentinel?: {
    name: string;
    nodes: QueuebitSentinelNode[];
    username?: string;
    password?: string;
    tls?: boolean | QueuebitTlsOptions;
  };
};

type QueueOptions = {
  connection: QueuebitConnection;
  namespace: string;
};

type QueuebitJobState =
  | 'waiting'
  | 'delayed'
  | 'active'
  | 'retrying'
  | 'completed'
  | 'failed'
  | 'stalled'
  | 'draining';

type JobInput<TPayload> = {
  name: string;
  data: TPayload;
  opts?: {
    idempotencyKey?: string;
    delayMs?: number;
    attempts?: number;
    backoff?: { type: 'fixed' | 'exponential'; delayMs: number };
    timeoutMs?: number;
  };
};

type BulkAddResult<TPayload> = {
  inputIndex: number;
  job: Job<TPayload>;
  created: boolean;
};

type HandlerContext = {
  signal: AbortSignal;
  attempt: number;
};
```

## Redis connection model

`QueuebitConnection` supports local Redis, managed Redis, ACL/password, TLS, and Sentinel connection-layer failover. Queue semantics target single-primary Redis only; Redis Cluster is unsupported in v0.1.

| Form | Use case | Example |
|------|----------|---------|
| `url` | Local or managed Redis provides one connection string | `redis://127.0.0.1:6379`, `rediss://user:pass@redis.example.com:6380/0` |
| `host` / `port` / `database` | Platforms expose host, port, and DB separately | `{ host: 'redis.example.com', port: 6379, database: 0 }` |
| `username` / `password` | Redis ACL or managed Redis password | Use with `url` or `host/port` |
| `tls` | Managed Redis requires TLS | `tls: true` or `tls: { servername: 'redis.example.com' }` |
| `sentinel` | Sentinel / automatic failover | Set master `name` and sentinel `nodes` |

Sentinel only means the connection layer can rediscover the primary. During failover, workers should stop claiming, schedulers should stop promoting when leadership is uncertain, and jobs recover through lease/retry/stalled recovery.

## Minimal examples

Producer:

`pendingReceipts` should come from your business database, API, event stream, or import file. queuebit APIs receive job payloads after your app has prepared them.

```ts
import { Queue } from 'queuebit';

const queue = new Queue('notification', {
  connection: { url: 'redis://127.0.0.1:6379' },
  namespace: 'dev:billing'
});

const jobs = pendingReceipts.map((order) => ({
  name: 'send-receipt-notification',
  data: {
    orderId: order.id,
    userId: order.user.id,
    recipient: order.user.email,
    templateId: 'receipt-paid'
  },
  opts: {
    idempotencyKey: `receipt:${order.id}`,
    attempts: 3,
    backoff: { type: 'exponential', delayMs: 1000 }
  }
}));

await queue.addBulk(jobs);
```

Worker:

```ts
import { Worker } from 'queuebit';

const worker = new Worker('notification', async (job) => {
  await sendReceiptNotification(job.data);
}, {
  connection: { url: 'redis://127.0.0.1:6379' },
  namespace: 'dev:billing',
  concurrency: 4
});

await worker.run();
```

Scheduler:

```ts
import { Scheduler } from 'queuebit';

const scheduler = new Scheduler({
  connection: { url: 'redis://127.0.0.1:6379' },
  namespace: 'dev:billing',
  queues: ['notification'],
  domain: 'billing-notification'
});

await scheduler.run();
```

## Queue API

| Call | Result | Failures and notes |
|------|--------|--------------------|
| `new Queue(name, options)` | Binds Redis, namespace, and a stable queue name | Invalid connection or namespace fails creation |
| `queue.add(name, data, opts?)` | Returns one traceable job | Use for truly single-record business actions |
| `queue.addBulk(jobs)` | Returns `BulkAddResult[]` in input order; prefer for business batches | Validates the whole batch and atomically writes new jobs; one invalid or conflicting item rejects the whole batch |
| `queue.getJob(id)` | Returns `Job` metadata and current state | Returns `null` when the job does not exist |
| `queue.inspect()` | Returns waiting, active, delayed, retry, failed, and stalled overview | Read-only; does not advance state |
| `queue.close()` | Releases resources used by this Queue | Do not submit new jobs after close |

`addBulk` has one deterministic contract:

| Situation | Result |
|-----------|--------|
| Every item is new and valid | All jobs are created atomically and returned in input order with `created: true` |
| A key already exists with the same job content | That result uses the existing job and returns `created: false` |
| The same key is repeated inside one input batch | The whole call rejects before writing |
| An existing key points to different job content | The whole call rejects with an idempotency conflict |
| Any item is invalid or Redis cannot commit | The whole call rejects; callers must not assume a partial batch was accepted |

## Worker API

| Call | Result | Failures and notes |
|------|--------|--------------------|
| `new Worker(name, handler, options)` | Registers `(job, ctx: HandlerContext) => Promise<void>` and Worker settings | Queue and namespace must match Producer; pass `ctx.signal` to cooperative downstream APIs |
| `worker.run()` | Claims and processes jobs until closed | Handler errors follow attempts/backoff into retry or failed |
| `worker.close({ drain, timeoutMs })` | Stops new claims and optionally waits for active jobs | Timeout is not success; unfinished jobs use recovery |
| Worker events/logs | Expose completed, failed, retry, stalled, and connection errors | Uncertain ack or lease may cause redelivery |

When `opts.timeoutMs` expires, queuebit aborts `ctx.signal` and records `HandlerTimeoutError`. The attempt then follows the configured retry policy and eventually becomes `failed`. JavaScript cannot forcibly stop side effects that ignore the signal, so handlers must pass `ctx.signal` to supported clients and remain idempotent.

Worker event names are `completed`, `failed`, `retrying`, `stalled`, and `error`. A listener exception is reported as an `error` event/log entry and never changes the job state.

Metrics are pull-based in v0.1: call `queue.inspect()` or use the CLI JSON output. queuebit does not include a dashboard or a Prometheus HTTP server.

## Scheduler API

| Call | Result | Failures and notes |
|------|--------|--------------------|
| `new Scheduler(options)` | Binds queue names and a `domain` | Candidates in one group use the same stable domain |
| `scheduler.run()` | Active instance advances delayed, retrying, and stalled jobs | Does not advance without proven single-active ownership |
| `scheduler.close()` | Stops heartbeat and time progression | Delayed/retry work pauses until another candidate takes over |

## Job states

User-visible state flow:

```mermaid
flowchart LR
  P["producer enqueue"] --> W["waiting"]
  P --> D["delayed"]
  D --> W
  W --> A["active"]
  A --> C["completed"]
  A --> R["retrying"]
  R --> W
  A --> F["failed"]
  A --> S["stalled"]
  S --> W
```

Node explanations:

| Node | Meaning |
|------|---------|
| `waiting` | Job can be claimed by a worker |
| `delayed` | Job is not executable until a scheduler promotes it |
| `active` | Job has been claimed and has a lease |
| `retrying` | Job failed and waits for the next attempt |
| `completed` | Job was acknowledged as completed |
| `failed` | Job reached terminal failure |
| `stalled` | Active job became uncertain and waits for recovery |

## Errors and events

Callers handle these error categories:

| Type | What you may see | User action |
|------|----------------|-------------|
| Configuration error | Missing namespace, queue name, or invalid Redis config | Fail before start and fix config |
| Redis unavailable | Connection failure, command timeout, script failure | Stop claiming and wait for recovery or operator action |
| Lease uncertainty | Renew failure, token mismatch, missing TTL | Worker stops claiming; recovery handles the job |
| Handler failure | Business exception, timeout, explicit failure | Retry or terminal failure policy applies |
| Scheduler uncertainty | Cannot prove leadership | Stop delayed/retry progression |

## Related references

| Question | Entry |
|----------|-------|
| First integration path | [Quick Start](./quick-start.md) |
| Configuration fields and CLI | [CLI and configuration](./cli-and-config.md) |
| vext adapter API | [vext integration](./vext-integration.md) |
| Redis keyspace and state transitions | [Redis model](./redis-model.md) |
| Worker / scheduler lifecycle | [Worker and Scheduler Lifecycle](./worker-lifecycle.md) |
