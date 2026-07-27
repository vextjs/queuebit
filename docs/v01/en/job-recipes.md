# Run One Background Job

<span class="manual-label">Quick start · one job from enqueue to completion</span>

This page comes after [Quick start](./quick-start.md). Start with the smallest submit path: Web/API puts one payload into a Queue, and a Worker runs the processor with the same name. After that works, add retry, idempotency, delay, bulk submit, or failure recovery only when you need them.

| Layer | What you need to know |
|---|---|
| Required | Queue name, processor name, payload, and a running Worker |
| Common daily use | `attempts`, `timeoutMs`, `deduplicationKey`, `idempotencyKey` |
| Use when needed | `delayMs`, `addBulk`, `cancel`, `retryFailed` |
| Complex scenarios | Database paging, per-batch/final callbacks, and crash recovery belong in [BatchRun](./batch-runs.md) |

<span id="sc02-direct-job"></span>
## Smallest Submit: No Options Yet

```ts
const job = await queuebit.jobs.add(
  'notification',
  'send-receipt',
  {
    schemaVersion: 1,
    tenantId,
    orderId,
    recipient
  }
);

return { jobId: job.id, state: job.state };
```

This code does one thing: it puts `send-receipt` into the `notification` queue. The payload comes from an authenticated, validated request plus server-side business lookup. It is not a hard-coded user. Return HTTP 202 and `jobId`; expose business results through a query endpoint or business state table.

```bash
npx queuebit job inspect <jobId> --config queuebit.config.ts
```

## Daily Development: When to Add Options

Get the smallest submit working first, then add these options according to business risk:

| Need | Add | Effect |
|---|---|---|
| Try again after failure | `attempts` + `backoff` | Retry network blips or 5xx responses |
| Prevent endless execution | `timeoutMs` | Let Queuebit decide failure or retry after timeout |
| Avoid duplicate enqueue for one business request | `deduplicationKey` | Same key and same payload return the same job |
| Avoid duplicate email, webhook, or payment effects | `idempotencyKey` | Pass a side-effect key into the processor |

```ts
const job = await queuebit.jobs.add(
  'notification',
  'send-receipt',
  { schemaVersion: 1, tenantId, orderId, recipient },
  {
    attempts: 3,
    timeoutMs: 30_000,
    deduplicationKey: `request:receipt:${tenantId}:${orderId}`,
    idempotencyKey: `receipt:${tenantId}:${orderId}`
  }
);
```

Handle common submit failures like this:

| What happened | Queuebit returns | What to do |
|---|---|---|
| Redis cannot be reached | `QB_REDIS_CONNECTION_FAILED`, retryable | Wait briefly, then submit again with the same `deduplicationKey` |
| Same business request, different payload | `QB_JOB_DEDUPLICATION_CONFLICT`, HTTP 409 | Fix the business identity, such as `tenantId/orderId`; do not generate a random key to bypass the conflict |
| Queue is too full | `QB_BACKPRESSURE_REJECTED`, HTTP 429 | Use the returned `details` and wait until the queue is below the recovery watermark |

## Submit a Small Known Group: `addBulk`

```ts
const results = await queuebit.jobs.addBulk('notification', [
  {
    name: 'send-receipt',
    data: { schemaVersion: 1, orderId: 1001, tenantId, recipient: 'a@example.com' },
    options: {
      deduplicationKey: `request:receipt:${tenantId}:1001`,
      idempotencyKey: `receipt:${tenantId}:1001`
    }
  },
  {
    name: 'send-receipt',
    data: { schemaVersion: 1, orderId: 1002, tenantId, recipient: 'b@example.com' },
    options: {
      deduplicationKey: `request:receipt:${tenantId}:1002`,
      idempotencyKey: `receipt:${tenantId}:1002`
    }
  }
]);
```

`addBulk` is for submitting a known, small list at once, such as one already-loaded page of orders. It writes that list to one Queue: either every job is created, or none are. It does not keep scanning your database, track batch progress, remember the current position, or create recovery records. Use [batch database processing](./batch-runs.md) for that.

<span id="sc05-delay-retry"></span>
## Delay and Retry

```ts
await queuebit.jobs.add(
  'webhook',
  'deliver-webhook',
  { schemaVersion: 1, endpointId, eventId },
  {
    delayMs: 60_000,
    attempts: 5,
    backoff: {
      type: 'exponential',
      delayMs: 1_000,
      maxDelayMs: 30_000,
      jitter: 0.2
    },
    idempotencyKey: `webhook:${endpointId}:${eventId}`
  }
);
```

`delayMs` waits before the first execution. `attempts` includes the first execution, and another attempt is consumed only when the processor throws a retryable error or times out. If no Worker is online, jobs remain in Redis; processing resumes when a Worker returns.

## When a Processor Needs More Context

The smallest processor only uses `data`. Read more context when you need logging, timeout handling, or an idempotency key for an external system:

```ts
processors: {
  'deliver-webhook': defineQueuebitProcessor(async ({
    data,
    signal,
    attempt,
    idempotencyKey,
    logger
  }) => {
    logger.info({ eventId: data.eventId, attempt }, 'deliver webhook');
    await webhookClient.send(data, { signal, idempotencyKey });
  })
}
```

- Treat 429, 5xx, and transient network failures as retryable so Queuebit can try again with backoff.
- Treat invalid endpoints, incompatible schemas, and permanently revoked credentials as fast failures.
- Timeout aborts business code through `AbortSignal`; if an old execution returns late, it still cannot overwrite the newer result.

## Cancel or Retry Failed Work

```ts
await queuebit.jobs.cancel(waitingJobId);

const replacement = await queuebit.jobs.retryFailed(failedJobId, {
  deduplicationKey: `replacement:${failedJobId}:1`
});
```

`jobs.cancel` accepts only jobs that have not started yet or are waiting to retry. A failed direct job can be resubmitted with a new replacement identity and usually keeps the same business `idempotencyKey`, so email, webhook, and payment side effects still happen only once. A job owned by a BatchRun must not be replaced individually; recover it from the run with `runs.retryFailed`.

## Next

- Protect email, payment, and webhook side effects: [Prevent duplicate side effects](./idempotency-patterns.md).
- Run more than one Worker: [How multiple Workers run together](./distributed-workers.md).
- Look up exact methods and errors: [API quick reference](./target-api.md).
