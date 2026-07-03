# queuebit

queuebit is a Redis-only distributed job queue for Node.js and vext projects.

This README is the v0.1 final user manual target: future implementation should make the examples, CLI commands, configuration, vext adapter, and operations behavior below work as written.

## Table of Contents

- [Documentation](#documentation)
- [Install](#install)
- [Quick Start](#quick-start)
- [vext Integration](#vext-integration)
- [Configuration and CLI](#configuration-and-cli)
- [Operations](#operations)
- [Scope](#scope)
- [Local Documentation Development](#local-documentation-development)
- [License](#license)

## Documentation

The full user manual is built as a standalone Rspress site:

- Chinese docs: [docs/zh](https://github.com/vextjs/queuebit/blob/main/docs/zh/index.md)
- English docs: [docs/en](https://github.com/vextjs/queuebit/blob/main/docs/en/index.md)
- Local docs commands: `npm run docs:dev`, `npm run docs:build`, `npm run docs:preview`

## Install

```bash
npm install queuebit
```

queuebit v0.1 expects Node.js `>= 20` and Redis `>= 7.0`. Redis standalone or managed single-primary Redis is the baseline; Redis Cluster is not supported in v0.1.

## Quick Start

```ts
import { Queue, Worker, Scheduler } from 'queuebit';

const connection = { url: 'redis://127.0.0.1:6379' };
const namespace = 'dev:billing';

const notificationQueue = new Queue('notification', { connection, namespace });

const paidOrders = await orders.findPaidOrdersNeedingReceipt({ limit: 100 });

const jobs = paidOrders.flatMap((order) => {
  const wantsPush = order.user.preferredChannel === 'push' && order.user.pushToken;
  const channel = wantsPush ? 'push' : 'email';
  const recipient = wantsPush ? order.user.pushToken : order.user.email;

  if (!recipient) {
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
    },
    opts: {
      idempotencyKey: `receipt:${order.id}`,
      attempts: 3,
      backoff: { type: 'exponential', delayMs: 1000 }
    }
  }];
});

await notificationQueue.addBulk(jobs);

const worker = new Worker('notification', async (job) => {
  if (job.data.channel === 'email') {
    await emailProvider.sendReceipt(job.data);
    return;
  }

  await pushProvider.sendReceipt(job.data);
}, {
  connection,
  namespace,
  concurrency: 4,
  leaseMs: 30000,
  renewIntervalMs: 10000,
  drainTimeoutMs: 30000
});

const scheduler = new Scheduler({
  connection,
  namespace,
  queues: ['notification'],
  domain: 'billing-notification'
});

await Promise.all([worker.run(), scheduler.run()]);
```

queuebit does not fetch users or notification recipients by itself. Your app reads business data from a database, API, event stream, or import file, then submits prepared job payloads to queuebit.

Worker and scheduler should run as explicit processes in production:

```bash
queuebit worker start --config queuebit.config.ts --queue notification
queuebit scheduler start --config queuebit.config.ts --domain billing-notification
```

## vext Integration

```ts
import { defineConfig } from 'vext';
import { queuebit } from 'queuebit/vext';

export default defineConfig({
  plugins: [
    queuebit({
      connection: { url: 'redis://127.0.0.1:6379' },
      namespace: 'prod:billing',
      queues: {
        notification: {
          defaultJobOptions: {
            attempts: 3,
            backoff: { type: 'exponential', delayMs: 1000 }
          }
        }
      },
      producer: { enabled: true },
      worker: { enabled: false },
      scheduler: { enabled: false }
    })
  ]
});
```

Use explicit worker and scheduler entries for vext projects. `vext start` does not automatically start queue workers, and vext routes should batch-submit prepared business payloads with `Queue.addBulk`.

## Configuration and CLI

```ts
import { defineQueuebitConfig } from 'queuebit';

export default defineQueuebitConfig({
  connection: { url: 'redis://127.0.0.1:6379' },
  namespace: 'dev:billing',
  queues: {
    notification: {
      defaultJobOptions: { attempts: 3 },
      worker: {
        concurrency: 4,
        leaseMs: 30000,
        renewIntervalMs: 10000,
        drainTimeoutMs: 30000
      },
      scheduler: { domain: 'billing-notification' }
    }
  },
  metrics: { enabled: true }
});
```

Useful commands:

```bash
queuebit worker start --config queuebit.config.ts --queue notification
queuebit worker drain --config queuebit.config.ts --queue notification --timeout 30s
queuebit scheduler start --config queuebit.config.ts --domain billing-notification
queuebit inspect queue notification --config queuebit.config.ts
queuebit inspect workers --queue notification --config queuebit.config.ts
queuebit inspect scheduler --domain billing-notification --config queuebit.config.ts
```

## Operations

queuebit is at-least-once. Handlers must be idempotent because worker crashes, ack loss, lease expiry, or Redis connectivity problems can redeliver a job.

Use inspect output to track waiting, active, delayed, retrying, failed, stalled recoveries, active workers, and active scheduler identity.

## Scope

v0.1 includes:

- Queue / Job / Producer / Worker / Scheduler primitives
- retry and delayed job semantics
- at-least-once delivery
- lease-based stalled recovery
- single-active scheduler domain
- metrics and introspection
- `queuebit/vext` adapter

v0.1 does not include recurring / repeatable jobs, flows / workflow orchestration, dashboard / admin UI, Redis Cluster support, or non-Redis queue backends.

## Local Documentation Development

Install the documentation site dependencies first:

```bash
cd website
npm install
```

Then run one of:

```bash
npm run docs:dev
npm run docs:build
npm run docs:preview
```

The root package remains focused on the npm package surface. The `website/` directory owns documentation build dependencies.

## License

Apache-2.0
