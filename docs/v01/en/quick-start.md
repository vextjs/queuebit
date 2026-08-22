# Quick Start: Run One Background Job

<span class="manual-label">Quick start · first run the minimum integration</span>

This page gets the first background job working: install Queuebit, connect Redis, create a client directly, call `jobs.add()` from Web/API code, create a Worker from application code, and confirm the job completes.

Do not learn every feature first. For a first integration, only the left side matters:

| Learn now | Ignore for now |
|---|---|
| Redis connection, queue name, `jobs.add()`, Worker service code | result callbacks, duplicate protection, automatic retry, delayed execution, database batching, multi-process deployment, framework integration |

## 1. Install Queuebit and Prepare Redis

```bash
npm install queuebit
```

Queuebit needs a reachable Redis `>=7.2`. Put its address in `QUEUEBIT_REDIS_URL`; when Redis is available at the default local address `redis://127.0.0.1:6379/0`, the example below uses that default directly.

## 2. Write One Ordinary Config Object

Tell Queuebit where Redis is and which queues exist. Start with one `notification` queue. This is not a framework-specific file: the API and Worker reuse the object directly.

```ts title="queuebit.ts"
export const queuebitConfig = {
  connection: { url: process.env.QUEUEBIT_REDIS_URL ?? 'redis://127.0.0.1:6379/0' },
  queues: {
    notification: {}
  }
};
```

The two fields have distinct jobs:

- `connection.url` is the Redis address. The environment variable lets each environment select its Redis, while the local fallback supports a first run.
- `queues.notification` declares the queue used in this example. One first job needs no other queue configuration.

Queuebit automatically derives a stable, isolated namespace from the nearest `package.json` name, so the API and Worker for this application use the same Redis keys without another setting. If deployments of the same package share one Redis, set a distinct `QUEUEBIT_NAMESPACE` for each deployment; an explicit `namespace` in code overrides it.

`createQueuebitClient(queuebitConfig)` validates and normalizes this ordinary object, so no additional config wrapper is needed here.

## 3. Start a Worker from Your Service Code

Pass the business function directly to the Worker. A first integration does not need `queuebit.runtime.ts`; failure handling, timeout, and duplicate protection can be added later.

```ts title="notification-worker.ts"
import { createQueuebitClient } from 'queuebit';
import { queuebitConfig } from './queuebit.js';

export async function startNotificationWorker(workerId: string) {
  const queuebit = await createQueuebitClient(queuebitConfig);
  const worker = queuebit.createWorker(
    'notification',
    async ({ data }) => receiptService.send(data),
    { workerId, concurrency: 4 }
  );
  worker.start();

  return {
    worker,
    stop: (options?: { timeoutMs?: number }) => queuebit.close(options)
  };
}
```

Call this code once from a separate Worker service host, never from every Web request:

- `createQueuebitClient(queuebitConfig)` creates one long-lived client; it uses the same Redis, namespace, and queue as the API.
- `createWorker('notification', ...)` claims only jobs from `notification`. This first example puts only receipt work in that queue, so the business function can process `data` directly.
- `concurrency: 4` lets this Worker process at most four jobs at once; `worker.start()` is the explicit point where it begins claiming work.
- Call `stop()` from the service host's shutdown hook. It stops new claims, waits for active handlers, unregisters the Worker role, and then closes the client connection.

Importing these modules does not start a process or attach signal handlers.

## 4. Enqueue Work from Web/API Code

Do not send the receipt slowly inside the request. Put the work in the queue and return a `jobId`. Create the client once at application startup and reuse it from routes; do not close it after every request.

```ts title="Your Web/API code"
import { createQueuebitClient } from 'queuebit';
import { queuebitConfig } from './queuebit.js';

// Create this once at application startup; all routes reuse the client.
const queuebit = await createQueuebitClient(queuebitConfig);

export async function enqueueReceipt(orderId: string, tenantId: string, recipient: string) {
  const job = await queuebit.jobs.add(
    'notification', // The queue declared above.
    'send-receipt', // A readable name for this kind of job.
    { orderId, tenantId, recipient } // Data received by the Worker.
  );

  return { jobId: job.id, state: job.state };
}
```

`jobs.add()` returns quickly: it guarantees durable enqueueing, not that the receipt has already been sent inside the HTTP request. Keep the `jobId` so a caller can inspect state later.

## 5. Confirm the Result

Read the job by `jobId` from the same application client:

```ts
const current = await queuebit.jobs.get(job.id);
// current?.state is waiting, active, completed, failed, or cancelled.
```

`current` can temporarily be `waiting` or `active`, and can be `null` after job history is cleaned up. When it becomes `completed`, the minimal API-enqueue and Worker-execution path works. Up to this point, you do not need result callbacks, duplicate protection, cancellation, internal scheduling details, or multi-process deployment.

When you need the CLI, multiple Worker/Coordinator service hosts, BatchRun, or centralized configuration governance, see [Configure Redis and Workers](./configuration-recipes.md). Then extract the shared object into `queuebit.config.ts` and BatchRun registration into `queuebit.runtime.ts`.

## Next

- Add retry, timeout, or duplicate protection to this job: [Run one background job](./job-recipes.md).
- Need to page many database records: [Process many database records](./batch-runs.md).
- Prepare production: [Configure Redis and Workers](./configuration-recipes.md).
