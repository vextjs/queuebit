# Quick Start: Run One Background Job

<span class="manual-label">Quick start · first run the minimum integration</span>

This page gets the first background job working: install Queuebit, configure Redis, define one processor, call `jobs.add()` from Web/API code, start a Worker, and confirm the job completes.

Do not learn every feature first. For a first integration, only the left side matters:

| Learn now | Ignore for now |
|---|---|
| Redis connection, queue name, processor name, `jobs.add()`, Worker start command | result callbacks, duplicate protection, automatic retry, delayed execution, database batching, multi-process deployment, framework integration |

## 1. Install Queuebit

```bash
npm install queuebit
docker run --name queuebit-redis -p 6379:6379 -d redis:7.2
```

## 2. Configure Redis and One Queue

Tell Queuebit where Redis is and which queues exist. Start with one `notification` queue.

```ts title="queuebit.config.ts"
import { defineQueuebitConfig } from 'queuebit';

export default defineQueuebitConfig({
  connection: { url: process.env.QUEUEBIT_REDIS_URL ?? 'redis://127.0.0.1:6379/0' },
  namespace: 'receipt-demo',
  queues: {
    notification: {}
  }
});
```

## 3. Define the Work to Execute

A processor is the business function a Worker runs. Start with `data` only; failure handling, timeout, and duplicate protection can be added later.

```ts title="queuebit.runtime.ts"
import { defineQueuebitRuntime, defineQueuebitProcessor } from 'queuebit';

export default defineQueuebitRuntime({
  processors: {
    'send-receipt': defineQueuebitProcessor(async ({ data }) => {
      await receiptService.send(data);
    })
  }
});
```

## 4. Enqueue Work from Web/API Code

Do not send the receipt slowly inside the request. Put the work in the queue and return a `jobId`.

```ts title="Your Web/API code"
import config from './queuebit.config.js';
import { createQueuebitClient } from 'queuebit';

const queuebit = await createQueuebitClient({ config });

const job = await queuebit.jobs.add(
  'notification',
  'send-receipt',
  { orderId, tenantId, recipient }
);

return { jobId: job.id, state: job.state };
```

## 5. Start a Worker and Confirm the Result

The Worker reads `queuebit.runtime.ts`, finds the `send-receipt` processor, and runs the job you just submitted.

```bash title="Background process"
npx queuebit worker start --config queuebit.config.ts --runtime queuebit.runtime.ts --queue notification
```

```bash
npx queuebit job inspect <jobId> --config queuebit.config.ts
```

When the job moves from waiting/active to completed, the minimum integration works. Up to this point, you do not need result callbacks, duplicate protection, cancellation, internal scheduling details, or multi-process deployment.

## Next

- Add retry, timeout, or duplicate protection to this job: [Run one background job](./job-recipes.md).
- Need to page many database records: [Process many database records](./batch-runs.md).
- Prepare production: [Configure Redis and Workers](./configuration-recipes.md).
