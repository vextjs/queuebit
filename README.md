# queuebit

Queuebit is a Redis-backed background job queue for Node.js and vext projects.

The basic flow is small:

1. Install `queuebit`.
2. Pass Redis configuration.
3. Register a processor.
4. Call `jobs.add()` from your Web/API code.
5. Run one or more Workers.

Use BatchRun only when you need to page many database records, create jobs from each page, record batch/final results, and recover progress after failures.

> Release status: the current package is pre-v0.1. The `docs/v01` site is the confirmed user contract for the planned v0.1 runtime. Some full examples are still target contracts until release evidence closes.

## Install

```bash
npm install queuebit
```

## First Job

```ts
import { createQueuebitClient } from 'queuebit';
import config from './queuebit.config.js';

const queuebit = await createQueuebitClient({ config });

const job = await queuebit.jobs.add(
  'notification',
  'send-receipt',
  { orderId, tenantId, recipient }
);

return { jobId: job.id, state: job.state };
```

Run the Worker separately:

```bash
npx queuebit worker start --config queuebit.config.ts --runtime queuebit.runtime.ts --queue notification
```

## Documentation

The bilingual user manual lives in `docs/v01`:

- [Quick start](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/quick-start.md)
- [Run one background job](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/job-recipes.md)
- [Process many database records](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/batch-runs.md)
- [Configure Redis and Workers](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/configuration-recipes.md)
- [Production deployment](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/production-deployment.md)
- [Failure runbooks](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/failure-runbooks.md)
- [中文用户手册](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/zh/index.md)

## Choose a Path

| Goal | Start here |
|---|---|
| Run the first background task | [Quick start](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/quick-start.md) |
| Add retry, timeout, delay, or duplicate protection | [Run one background job](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/job-recipes.md) |
| Page many database records into jobs | [Process many database records](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/batch-runs.md) |
| Integrate with vext | [vext integration](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/vext-integration.md) |
| Prepare production Redis and Workers | [Configure Redis and Workers](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/configuration-recipes.md) |
| Recover an incident | [Failure runbooks](https://github.com/devcodex-labs/queuebit/blob/main/docs/v01/en/failure-runbooks.md) |

## Compatibility

Queuebit core requires Node.js `>=20`. The `queuebit/vext` adapter and canonical vext example require Node.js `>=20.19` with `vextjs@0.3.26`.

Redis requires `>=7.2`. Supported Redis topologies are standalone Redis or managed single-primary Redis, optionally discovered by Sentinel. Redis Cluster and non-Redis backends are not part of v0.1.

Queuebit provides at-least-once processing. External side effects such as email, payment, webhook, and business database writes still need stable business idempotency keys.

## What Is Included

- Direct jobs with `jobs.add`, `addBulk`, inspect, cancel, retry-failed replacement, delay, retry, timeout, and backpressure.
- Worker kernel and runner with Redis-backed claim, renew, drain, stalled recovery, and cooperative time advancement.
- Durable BatchRun identity with source/mapper/processor/completion registration, sequential/paced dispatch, progress checkpoints, run controls, failure listing, and recovery runs.
- Completion event delivery with retry.
- Redis direct URL/direct options/Sentinel connection support through `@redis/client`.
- Health, metrics, capacity, retention, alert evaluation, CLI, and `queuebit/vext` client lifecycle adapter.

## What Is Not Included in v0.1

- Cron/repeatable jobs.
- DAG or workflow orchestration.
- Priority queues.
- Global rate limiting.
- Dashboard/Admin UI.
- CDC or unbounded streams.
- Redis Cluster or non-Redis backends.

## Development Checks

```bash
npm run typecheck
npm run typecheck:examples
npm test
npm run docs:validate
npm run docs:build
```

Redis-backed suites are environment-gated:

```bash
npm run test:redis
npm run test:redis:faults
npm run test:redis:sentinel
```

Without the required Redis or Sentinel environment variables, those suites skip with exit code 0.

## Package

```bash
npm pack --dry-run
```

The published package contains `dist`, `README.md`, `LICENSE`, and `package.json`.

## License

Apache-2.0
