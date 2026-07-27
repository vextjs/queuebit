# Job Recipes

<!-- queuebit-v01-legacy-doc -->
> [!WARNING]
> **Archived and no longer maintained.** The current v0.1 final-user manual is [`docs/v01/en`](../v01/en/index.md). This page remains for historical context only; do not use its APIs, commands, configuration, or examples for new integrations or implementation.

<span class="manual-label">Common task guide</span>

This page starts with pending orders from a business database and turns them into production job flows. Complete [Quick Start](./quick-start.md) first. For multiple instances, continue with [Distributed workers](./distributed-workers.md).

## Shared business model

```ts
type ReceiptJob = {
  orderId: string;
  recipient: string;
  templateId: string;
};
```

A Producer reads business records from a database, API, event stream, or import file and creates the smallest payload the handler needs. A Worker must not depend on Producer memory, and the main path must not use a hard-coded user.

<span id="s01-batch"></span>
## S01 Submit pending orders in bulk

**Use when:** scanning pending records, importing data, backfilling work, or creating many independent jobs from one request.

```ts
const pendingOrders = await db.orders.findMany({
  where: { paid: true, receiptQueuedAt: null },
  take: 500
});

const results = await notificationQueue.addBulk(
  pendingOrders.map((order) => ({
    name: 'send-receipt',
    data: {
      orderId: order.id,
      recipient: order.customerEmail,
      templateId: 'receipt-paid'
    },
    opts: {
      idempotencyKey: `receipt:${order.id}`,
      attempts: 3,
      backoff: { type: 'exponential', delayMs: 1000 }
    }
  }))
);

await db.orders.markReceiptQueued(
  results.map(({ job }) => ({ orderId: job.data.orderId, jobId: job.id }))
);
```

Results preserve input order. Validation or commit failure rejects the whole batch, so do not set `receiptQueuedAt` first. The same key and content returns `created: false`; the same key with different content is an idempotency conflict that must be resolved in business data.

<span id="s02-first-success"></span>
## S02 Prove the first batch succeeded

1. Log batch size, business IDs, and returned job IDs in the Producer.
2. Log job ID, attempt, business ID, and downstream request ID in the Worker.
3. Run `npx queuebit inspect queue notification --config queuebit.config.mjs`.
4. Verify the business notification state, not only the `completed` job state.

Success means input and result counts match, jobs become `completed`, and each business side effect exists exactly once from the user's perspective. If jobs complete without the business result, the handler completion boundary is wrong. If the business result exists while the job retries, use [Idempotency patterns](./idempotency-patterns.md).

<span id="s03-delayed"></span>
## S03 Run at a later time

```ts
const runAt = new Date('2026-08-01T09:00:00+08:00');

await notificationQueue.add('send-renewal-reminder', {
  orderId,
  recipient,
  templateId: 'renewal-reminder'
}, {
  idempotencyKey: `renewal:${orderId}:2026-08`,
  delayMs: Math.max(0, runAt.getTime() - Date.now())
});
```

A delayed job stays in `delayed` until the active Scheduler promotes it to `waiting`. If it does not run, check system clocks, then run `npx queuebit inspect scheduler --domain billing-notification --config queuebit.config.ts`. Do not work around a missing Scheduler by submitting duplicates.

<span id="s04-retry"></span>
## S04 Retry recoverable failures

```ts
await notificationQueue.add('send-receipt', payload, {
  idempotencyKey: `receipt:${payload.orderId}`,
  attempts: 4,
  backoff: { type: 'exponential', delayMs: 1000 },
  timeoutMs: 15000
});
```

Throw for network timeouts, downstream `429`, and `5xx` responses so queuebit retries with backoff. Throw identifiable permanent business errors for missing recipients or templates so terminal `failed` jobs are easy to classify. Expiring `timeoutMs` produces `HandlerTimeoutError` and aborts `ctx.signal`, but cannot forcibly stop an external call that ignores the signal.

<span id="s05-terminal-failed"></span>
## S05 Handle terminal failures

v0.1 has no built-in DLQ, manual retry, or replay API. Use this production process:

1. Read the final error, attempt history, job ID, and business ID from inspect and logs.
2. Classify bad business data, a permanent downstream rejection, or an exhausted retry budget.
3. Fix the cause and determine whether an earlier attempt already created a side effect.
4. Submit an auditable new job through a business administration path, using a revision key such as `receipt:order-42:correction-2`.
5. Record the old job, new job, operator, reason, and outcome in the business database.

Never edit Redis state directly or replay unchanged work forever. Applications that need standardized manual recovery should build a failure-management screen on the public `Queue` API.

<span id="s06-concurrency"></span>
## S06 Tune Worker concurrency

```ts
const worker = new Worker('notification', handler, {
  connection,
  namespace: 'prod:billing',
  concurrency: 4
});
```

One instance can run up to `concurrency` jobs; cluster concurrency is approximately the sum across healthy Workers. Start below downstream pool, quota, and memory limits, then observe latency, errors, and Redis latency. v0.1 has no built-in global rate limiter. Enforce global provider quotas in a shared business client or gateway rather than assuming one Worker's setting limits the cluster.

<span id="s17-events"></span>
## S17 Observe job lifecycle (no Worker event bus)

There is **no** public `worker.on(...)` API in current Queuebit. Observe work through structured logs, role heartbeats, and process-local metrics (for example `worker_jobs_completed_total`, `worker_jobs_failed_total`, `worker_stalled_jobs_recovered_total`). Metric or log emission failure must never rewrite Job state.

> Prefer the bilingual contract under `docs/v01/` for v0.1; this tree may lag.

<span id="s18-metrics"></span>
## S18 Export and alert on metrics

```ts
const snapshot = await notificationQueue.inspect();

metrics.gauge('queuebit.waiting', snapshot.waiting);
metrics.gauge('queuebit.active', snapshot.active);
metrics.gauge('queuebit.delayed', snapshot.delayed);
metrics.gauge('queuebit.retrying', snapshot.retrying);
metrics.gauge('queuebit.failed', snapshot.failed);
metrics.gauge('queuebit.stalled', snapshot.stalled);
```

v0.1 exposes pull-based `queue.inspect()` and CLI JSON only; it has no bundled dashboard or Prometheus HTTP server. Alert on sustained waiting growth, oldest waiting age, failed and stalled increments, Worker heartbeat count, and active Scheduler identity. Derive thresholds from business SLOs instead of treating any non-empty queue as an incident.

## Next steps

- Multi-instance deployment and recovery: [Distributed workers](./distributed-workers.md)
- External side-effect deduplication: [Idempotency patterns](./idempotency-patterns.md)
- Parameter selection: [Configuration recipes](./configuration-recipes.md)
- Incident actions: [Failure runbooks](./failure-runbooks.md)
