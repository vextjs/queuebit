# Quick Start

## Goal

<span class="manual-label">v0.1 final user manual</span>

This page is the first queuebit v0.1 integration path. After following it, you read a batch of pending business records, enqueue jobs in bulk, process them from workers, advance delayed/retry/recovery from a scheduler, and inspect queue state.

If you use vext, read this page first to understand queuebit roles, then continue to [vext integration](./vext-integration.md).

## Shortest successful path

For the first run, do not study every configuration field first. Follow this path:

```text
1. Start Redis
2. Write queuebit.config.ts
3. Read pending records from your business database, API, event stream, or import file
4. Convert each record into a job payload and enqueue with Queue.addBulk
5. Start Worker in a second process
6. Start Scheduler in a third process
7. Inspect waiting / active / delayed / failed
```

```mermaid
flowchart TD
  Start["Prepare Redis"] --> Config["Write queuebit.config.ts"]
  Config --> Source["Read business source<br/>DB / API / event"]
  Source --> Payload["Build job payloads<br/>stable ID + render variables"]
  Payload --> Producer["Enqueue jobs in bulk<br/>Queue.addBulk"]
  Producer --> Worker["Start Worker<br/>run handler"]
  Producer --> Scheduler["Start Scheduler<br/>promote delayed / retry / stalled"]
  Worker --> Inspect["inspect queue / workers / scheduler"]
  Scheduler --> Inspect
  Inspect --> Done{"Are the batch jobs completed?"}
  Done -- "yes" --> Success["First success"]
  Done -- "no" --> Triage["Use the common errors table"]
```

Node explanations:

| Node | Success signal |
|------|----------------|
| Prepare Redis | `redis://127.0.0.1:6379` is reachable |
| Write config | `connection`, `namespace`, and `queues.notification.scheduler.domain` are set |
| Read business source | Your app can load pending records from a DB, API, event, or file |
| Build payloads | Every job has a stable business ID, recipient, template, variables, and idempotency key |
| Enqueue jobs in bulk | `Queue.addBulk` returns job ids for the submitted batch |
| Start Worker | `inspect workers` shows a worker identity |
| Start Scheduler | `inspect scheduler` shows an active scheduler |
| inspect | You can distinguish waiting, active, delayed, retrying, and failed |

## Prerequisites

| Item | Requirement | Notes |
|------|-------------|-------|
| Node.js | `>= 20` | Current LTS line for modern ESM and TypeScript tooling |
| Redis | `>= 7.0` standalone or managed single-primary Redis | v0.1 is Redis-only; Redis Cluster is unsupported |
| TypeScript | Recommended `>= 5.4` | JavaScript is also supported |
| Process topology | Web/API, worker, and scheduler are explicit | Local demos can use multiple terminals on one machine |

## Install

```bash
npm install queuebit
```

Start local Redis:

```bash
docker run --name queuebit-redis -p 6379:6379 redis:7
```

Create `queuebit.config.ts`:

```ts
import { defineQueuebitConfig } from 'queuebit';

export default defineQueuebitConfig({
  connection: { url: 'redis://127.0.0.1:6379' },
  namespace: 'dev:billing',
  queues: {
    notification: {
      defaultJobOptions: {
        attempts: 3,
        backoff: { type: 'exponential', delayMs: 1000 }
      },
      worker: {
        concurrency: 4,
        leaseMs: 30000,
        renewIntervalMs: 10000,
        drainTimeoutMs: 30000
      },
      scheduler: {
        domain: 'billing-notification'
      }
    }
  },
  metrics: { enabled: true }
});
```

How to choose the fields:

| Field | Where the value comes from | First-run choice |
|-------|----------------------------|------------------|
| `connection.url` | Redis connection string | Use `redis://127.0.0.1:6379` locally; use your managed Redis URL in production |
| `namespace` | Environment and application name | Use a stable keyspace prefix such as `dev:billing` or `prod:billing` |
| `queues.notification` | Business queue name | Name by business action, such as `notification`, `email`, or `invoice` |
| `attempts` | How many retries the business can tolerate | Start with `3` for email/notification jobs; do not default to infinite retries |
| `backoff` | How downstream systems recover | Exponential backoff fits transient provider failures; fixed delay is fine for local demos |
| `worker.concurrency` | Handler and downstream capacity | Start from `4` or lower, then raise after observing rate limits |
| `leaseMs` / `renewIntervalMs` | Normal job runtime | `renewIntervalMs` must be lower than `leaseMs`; split long jobs or increase lease |
| `scheduler.domain` | Active scheduler scope | Use one stable domain for one business scheduling group |

## Enqueue jobs from a business data source

queuebit does not fetch user data by itself, and examples should not invent a hardcoded user. The real flow is: your application queries pending business records, then converts each record into a job payload.

The following example batches receipt notifications for paid orders. The data source can be your database, internal API, event stream, or import file; queuebit only receives the payloads you submit.

```ts
import { Queue } from 'queuebit';
import { db } from './db';

const notificationQueue = new Queue('notification', {
  connection: { url: 'redis://127.0.0.1:6379' },
  namespace: 'dev:billing'
});

type ReceiptNotificationJob = {
  orderId: string;
  userId: string;
  channel: 'email' | 'push';
  recipient: string;
  templateId: 'receipt-paid';
  variables: {
    orderNo: string;
    amountText: string;
  };
};

const pendingReceipts = await db.orders.findMany({
  where: {
    paid: true,
    receiptNotificationQueuedAt: null
  },
  include: {
    user: {
      select: {
        id: true,
        email: true,
        pushToken: true,
        preferredChannel: true
      }
    }
  },
  take: 100
});

const jobs = pendingReceipts.flatMap((order) => {
  const wantsPush = order.user.preferredChannel === 'push' && Boolean(order.user.pushToken);
  const channel = wantsPush ? 'push' : 'email';
  const recipient = wantsPush ? order.user.pushToken : order.user.email;

  if (!recipient) {
    // Do not invent a recipient; let the business record stay in a fixable state.
    return [];
  }

  return [{
    name: 'send-receipt-notification',
    data: {
      orderId: order.id,
      userId: order.user.id,
      channel,
      recipient,
      templateId: 'receipt-paid',
      variables: {
        orderNo: order.orderNo,
        amountText: order.amountText
      }
    } satisfies ReceiptNotificationJob,
    opts: {
      idempotencyKey: `receipt:${order.id}`,
      attempts: 3,
      backoff: { type: 'exponential', delayMs: 1000 }
    }
  }];
});

if (jobs.length > 0) {
  const createdJobs = await notificationQueue.addBulk(jobs);
  const queuedOrderIds = jobs.map((job) => job.data.orderId);

  await db.orders.updateMany({
    where: { id: { in: queuedOrderIds } },
    data: { receiptNotificationQueuedAt: new Date() }
  });

  console.log(createdJobs.map((job) => job.id));
}
```

The important decisions:

| Question | Correct model |
|----------|---------------|
| Where does user data come from? | From your business DB/API/event/file; queuebit only receives submitted payloads |
| Is queuebit only for one job at a time? | No. The main queue path should support batch submission; use `addBulk` for one batch |
| Where does notification data come from? | User profile, notification preferences, device token, template system, and order data; skip or fix incomplete records instead of fabricating recipients |
| What belongs in payload? | Stable IDs, recipient, template ID, and required variables; avoid large uncontrolled objects in Redis |

Delayed jobs can be part of the same batch by setting `delayMs` on those entries:

```ts
const delayedJobs = pendingReceipts.flatMap((order) => {
  if (!order.user.email) {
    return [];
  }

  return [{
    name: 'send-receipt-reminder',
    data: {
      orderId: order.id,
      userId: order.user.id,
      channel: 'email',
      recipient: order.user.email,
      templateId: 'receipt-paid',
      variables: {
        orderNo: order.orderNo,
        amountText: order.amountText
      }
    },
    opts: {
      idempotencyKey: `receipt-reminder:${order.id}`,
      delayMs: 15 * 60 * 1000
    }
  }];
});

if (delayedJobs.length > 0) {
  await notificationQueue.addBulk(delayedJobs);
}
```

## Start a Worker

Workers should run as dedicated processes. A worker claims jobs, renews leases, runs handlers, ack/fails results, and drains on shutdown.

In production, put payload types such as `ReceiptNotificationJob` in a shared file like `src/jobs/receipt-notification.ts` so producers and workers do not drift.

```ts
import { Worker } from 'queuebit';

const worker = new Worker(
  'notification',
  async (job) => {
    if (job.name !== 'send-receipt-notification') {
      throw new Error(`Unknown job: ${job.name}`);
    }

    const data = job.data as ReceiptNotificationJob;

    const user = await db.users.findUnique({ where: { id: data.userId } });
    const order = await db.orders.findUnique({ where: { id: data.orderId } });

    if (!user || !order) {
      throw new Error(`Missing user or order for job ${job.id}`);
    }

    const message = await renderNotificationTemplate(data.templateId, {
      ...data.variables,
      userName: user.name
    });

    if (data.channel === 'email') {
      await emailProvider.send({
        to: data.recipient,
        subject: message.subject,
        html: message.html
      });
      return;
    }

    await pushProvider.send({
      token: data.recipient,
      title: message.title,
      body: message.body
    });
  },
  {
    connection: { url: 'redis://127.0.0.1:6379' },
    namespace: 'dev:billing',
    concurrency: 4,
    leaseMs: 30000,
    renewIntervalMs: 10000,
    drainTimeoutMs: 30000
  }
);

await worker.run();
```

CLI equivalent:

```bash
queuebit worker start --config queuebit.config.ts --queue notification
```

## Start a Scheduler

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

CLI equivalent:

```bash
queuebit scheduler start --config queuebit.config.ts --domain billing-notification
```

## Inspect queue state

```bash
queuebit inspect queue notification --config queuebit.config.ts
queuebit inspect workers --queue notification --config queuebit.config.ts
queuebit inspect scheduler --domain billing-notification --config queuebit.config.ts
```

| Metric | Meaning |
|--------|---------|
| `waiting` | Jobs waiting for a worker |
| `active` | Jobs currently being processed |
| `delayed` | Jobs waiting for scheduler promotion |
| `retrying` | Failed jobs waiting for another attempt |
| `failed` | Terminally failed jobs |
| `stalledRecoveries` | Recent stalled recovery count |
| `activeWorkers` | Workers currently heartbeating |
| `activeScheduler` | Current active scheduler identity |

## Graceful shutdown

```ts
async function shutdown() {
  await worker.close({ drain: true, timeoutMs: 30000 });
  await scheduler.close();
  await notificationQueue.close();
}

process.once('SIGTERM', shutdown);
process.once('SIGINT', shutdown);
```

Drain timeout does not mean job success; unfinished jobs recover through lease and stalled recovery rules.

## Production topology

```text
web/api process      -> Queue.addBulk(...)
worker process       -> Worker.run()
scheduler process    -> Scheduler.run()
redis                -> queue state, leases, delayed, retry, recovery
```

| Role | Recommended count | Notes |
|------|-------------------|-------|
| Web/API producer | Scales with your application | Does not implicitly start workers or schedulers |
| Worker | Scales with throughput and downstream capacity | Start with low `concurrency`; handlers must be idempotent |
| Scheduler | Multiple candidates are allowed, one active per domain | Does not run business handlers |
| Redis | Single-primary Redis or managed single-primary Redis | Redis Cluster fails fast in v0.1 |

## Common errors

If the first run fails, do not inspect Redis keys first. Check in this order:

1. `queuebit inspect queue notification --config queuebit.config.ts`: find where jobs are.
2. `queuebit inspect workers --queue notification --config queuebit.config.ts`: confirm workers exist.
3. `queuebit inspect scheduler --domain billing-notification --config queuebit.config.ts`: confirm who advances delayed / retry jobs.
4. Then inspect handler logs and business idempotency.

| Symptom | Common cause | Fix |
|---------|--------------|-----|
| Job stays waiting | Worker not running, namespace mismatch, or worker draining | Run `queuebit inspect workers` |
| Delayed job never runs | Scheduler not running or domain mismatch | Run `queuebit inspect scheduler` |
| Job runs more than once | At-least-once redelivery after ack loss, crash, or lease expiry | Use idempotency keys and business dedupe |
| Worker fails on startup | Redis unavailable, invalid lease config, or unsupported Redis Cluster | Fix config, then read [CLI and configuration](./cli-and-config.md) |
| Draining leaves active jobs | Handler is slow, downstream call hangs, or drain timeout is too short | Split work or adjust timeout / lease |

## Next steps

- Read [Environment and Compatibility Boundary](./compatibility.md) first to check Redis and deployment shape.
- Read [Core concepts](./concepts.md) to build the queue, job, worker, scheduler, and lease vocabulary.
- If you plan to integrate with vext, continue with [vext integration](./vext-integration.md).
- If you care about configuration and commands, read [CLI and configuration](./cli-and-config.md).
- If you are troubleshooting production behavior, read [Operations and troubleshooting](./operations.md).
