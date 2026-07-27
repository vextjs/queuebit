# Configuration Recipes

<!-- queuebit-v01-legacy-doc -->
> [!WARNING]
> **Archived and no longer maintained.** The current v0.1 final-user manual is [`docs/v01/en`](../v01/en/index.md). This page remains for historical context only; do not use its APIs, commands, configuration, or examples for new integrations or implementation.

<span class="manual-label">Choose configuration by environment</span>

Do not start by guessing from the full field table. Choose the closest deployment recipe, then use [CLI and configuration](./cli-and-config.md) to look up individual fields.

## Minimal local development

```ts
import { defineQueuebitConfig } from 'queuebit';

export default defineQueuebitConfig({
  connection: { url: 'redis://127.0.0.1:6379' },
  namespace: 'dev:billing',
  queues: {
    notification: {
      defaultJobOptions: { attempts: 3 },
      worker: { concurrency: 2 },
      scheduler: { domain: 'billing-notification-dev' }
    }
  }
});
```

Run a dedicated Worker and Scheduler locally so development does not hide production process boundaries. Include the environment in `namespace` to avoid consuming another project when Redis is shared.

## Managed Redis with TLS

```ts
export default defineQueuebitConfig({
  connection: {
    url: 'rediss://redis.example.com:6380/0',
    username: 'default',
    password: 'redis-password',
    tls: { servername: 'redis.example.com' }
  },
  namespace: 'prod:billing',
  queues: {
    notification: {
      defaultJobOptions: {
        attempts: 4,
        backoff: { type: 'exponential', delayMs: 1000 },
        timeoutMs: 15000
      },
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

Prefer the provider connection URL when it is complete; use separate fields when credentials are separate. For TLS failures, verify SNI, CA, and host name instead of disabling TLS in production.

## Sentinel failover

```ts
connection: {
  sentinel: {
    name: 'mymaster',
    nodes: [
      { host: '10.0.0.11', port: 26379 },
      { host: '10.0.0.12', port: 26379 },
      { host: '10.0.0.13', port: 26379 }
    ],
    username: 'queuebit',
    password: 'redis-password'
  }
}
```

Sentinel rediscovers one primary; it is not Redis Cluster. During failover, Workers stop claiming and an uncertain Scheduler stops progression. Lease and stalled recovery converge after connectivity returns.

## Choose values

| Setting | Starting point | Check before increasing | Misconfiguration symptom |
|---------|----------------|-------------------------|--------------------------|
| `attempts` | 3 to 4 for notifications | Permanent-error ratio and recovery time | Too low fails early; too high causes retry storms |
| `backoff.delayMs` | 1000ms exponential | Provider throttle window | Too short overloads; too long delays recovery |
| `timeoutMs` | Above normal p99 | Whether downstream supports `AbortSignal` | Too short retries; too long blocks drain |
| `concurrency` | 2 to 4 per Worker | Quota, connection pool, memory | Waiting backlog or rising 429/timeouts |
| `leaseMs` | Above normal job p99 | GC pauses and network jitter | Too short stalls; too long crash recovery |
| `renewIntervalMs` | About one third of lease | Redis p99 latency | Must remain strictly below `leaseMs` |
| `drainTimeoutMs` | Above normal job p99 | Platform termination grace | Too short recovers jobs; too long blocks deploy |
| `scheduler.domain` | One stable value per progression group | Namespace and candidate scope | Mismatch creates separate actives or no takeover |

## Three starting profiles

| Workload | concurrency | attempts/backoff | timeout/lease | Note |
|----------|-------------|------------------|---------------|------|
| Email/push | `4` | `4`, exponential 1s | `15s / 30s` | Observe provider global quotas |
| Short database work | `8` | `3`, fixed 500ms | `5s / 15s` | Stay within connection-pool budget |
| External generation | `2` | `3`, exponential 2s | `60s / 90s` | Split long jobs before extending leases indefinitely |

These are starting profiles, not default guarantees. Tune from actual p95/p99, error rates, and SLOs.

## Process-role configuration

| Process | Needs | Does not need |
|---------|-------|---------------|
| Web/API Producer | Redis, namespace, queue, default job options | Handler, Worker concurrency, automatic Scheduler start |
| Worker | Redis, namespace, queue, handler, concurrency, lease, drain | HTTP routes or Scheduler ownership |
| Scheduler | Redis, namespace, queue list, domain | Business handler or HTTP concurrency |

Processes may share one configuration source of truth, but each starts only its own role. Starting a vext app is not starting a Worker.

## Startup validation

- Redis resolves to one primary or one Sentinel master; detected Cluster topology must fail.
- `namespace`, queue name, and Scheduler domain are non-empty and consistent across instances.
- `renewIntervalMs < leaseMs`, and `timeoutMs` is compatible with the lease.
- Worker concurrency is positive and platform termination grace exceeds drain timeout.
- Startup logs show the effective configuration summary, without becoming a replacement source of truth.

## Verify after configuration

```bash
npx queuebit inspect queue notification --config queuebit.config.ts
npx queuebit inspect workers --queue notification --config queuebit.config.ts
npx queuebit inspect scheduler --domain billing-notification --config queuebit.config.ts
```

Confirm namespace and queue, Worker heartbeats and concurrency, then exactly one active Scheduler. See [Production deployment](./production-deployment.md) for the full rehearsal.
