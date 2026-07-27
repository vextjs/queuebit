# CLI and Configuration

<!-- queuebit-v01-legacy-doc -->
> [!WARNING]
> **Archived and no longer maintained.** The current v0.1 final-user manual is [`docs/v01/en`](../v01/en/index.md). This page remains for historical context only; do not use its APIs, commands, configuration, or examples for new integrations or implementation.

## Configuration entry

<span class="manual-label">User configuration reference</span>

queuebit reads `queuebit.config.ts` by default. For the first run, set only Redis, namespace, and queue name. Before production, add Worker, Scheduler, retry, lease, and observability settings for each process role.

One configuration file can express:

- Redis connection, namespace, and queue names.
- Web producer, worker, and scheduler process roles.
- Whether producer, worker, and scheduler roles are enabled in the current process.
- worker concurrency, lease, drain, retry, delay, and other distributed parameters.
- metrics and introspection exposure.

Choose every production process role explicitly. Enabling the vext plugin does not automatically turn every Web worker into a queue Worker or Scheduler.

## Configuration decision tree

On first integration, users should choose the process role first and then fill fields, instead of guessing from a complete option table.

| What are you doing? | Process role | Configure | What this process does not start automatically |
|---------------------|--------------|------------------|----------------------------|
| Submit jobs from an HTTP / API process | Web producer | Create `Queue` or enable vext producer | Web process consuming jobs or acquiring scheduler ownership |
| Consume background jobs from a dedicated process | worker-only | `worker.concurrency`, lease/drain settings, handler entry | worker acting as scheduler automatically |
| Advance delayed / retry / stalled work | scheduler-only | `scheduler.domain` and queue list | scheduler running business handlers |
| Demo the full path locally | single-process dev | all three roles explicitly enabled and marked dev/demo | documenting this topology as the production default |

Remember one rule first: production defaults to `web producer -> worker process -> scheduler process`, and you explicitly enable or disable every role.

## From minimal to production configuration

Minimal first-run configuration:

```ts
import { defineQueuebitConfig } from 'queuebit';

export default defineQueuebitConfig({
  connection: { url: 'redis://127.0.0.1:6379' },
  namespace: 'dev:billing',
  queues: { notification: {} }
});
```

After [Quick Start](./quick-start.md) works, use the production example below:

```ts
import { defineQueuebitConfig } from 'queuebit';

export default defineQueuebitConfig({
  connection: { url: 'redis://127.0.0.1:6379' },
  namespace: 'dev:billing',
  queues: {
    notification: {
      defaultJobOptions: {
        attempts: 3,
        backoff: { type: 'fixed', delayMs: 1000 }
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
  metrics: {
    enabled: true
  }
});
```

single-process dev may enable Producer, Worker, and Scheduler together for local verification. Use dedicated processes in production; see [Production deployment](./production-deployment.md).

When filling configuration for the first time, choose values in this order instead of guessing from the full table:

| Step | What to decide | Where the value comes from | Example |
|------|----------------|----------------------------|---------|
| 1 | Redis connection | Local Docker, managed Redis console, or platform connection settings | `redis://127.0.0.1:6379` |
| 2 | namespace | Environment, application name, and tenant boundary | `dev:billing`, `prod:billing` |
| 3 | queue name | Stable business action, not process or instance name | `notification`, `invoice` |
| 4 | default job options | Business retry tolerance and backoff behavior | Notification jobs often start with `attempts: 3` and exponential backoff |
| 5 | worker settings | Handler runtime, downstream rate limits, and machine capacity | `concurrency: 4`, `leaseMs: 30000` |
| 6 | scheduler domain | Single-active scope for one scheduler candidate group | `billing-notification` |
| 7 | metrics / inspect | How you will diagnose stuck jobs in production | Local inspect on; production metrics through your platform |

## Configuration fields

Configuration shape:

| Field | Fill when | Purpose | How to choose |
|-------|-----------|---------|---------------|
| `connection` | required for all roles | Redis connection information or existing connection reference | producer, worker, and scheduler for the same queue must point at the same logical Redis keyspace |
| `connection.url` | recommended for local or managed connection strings | Redis URL with `redis://` or TLS `rediss://` | first local run uses `redis://127.0.0.1:6379` |
| `connection.host` / `connection.port` / `connection.database` | when URL is not used | split connection fields | useful when platforms inject host, port, and DB separately |
| `connection.username` / `connection.password` | Redis ACL or managed Redis auth | authentication settings | can be used with URL or host/port; startup errors should identify auth failure |
| `connection.tls` | when managed Redis requires TLS | TLS switch or SNI/CA options | use either `rediss://` or `tls: true` clearly |
| `connection.sentinel` | Sentinel / automatic failover | master name and sentinel nodes | connection-layer failover only; Redis Cluster still fails fast |
| `namespace` | required for all roles | Redis keyspace isolation for environment, app, or tenant | include environment and app; do not use an empty string |
| `queues.<name>` | at least one | Stable business queue name | use a stable business name, not an instance ID |
| `queues.<name>.defaultJobOptions.attempts` | recommended for retry | maximum attempts | default behavior must not retry forever |
| `queues.<name>.defaultJobOptions.backoff` | recommended for retry | retry delay policy | must explain fixed, exponential, or custom strategies |
| `queues.<name>.worker.concurrency` | worker process or default | In-process worker concurrency | start small, then tune by handler idempotency and downstream capacity |
| `queues.<name>.worker.leaseMs` | worker process or default | active job lease duration | must be greater than renewal interval and cover normal handler jitter |
| `queues.<name>.worker.renewIntervalMs` | worker process or default | lease renewal interval | must be less than `leaseMs` |
| `queues.<name>.worker.drainTimeoutMs` | worker process or default | graceful drain waiting window | must be no shorter than the acceptable single-job shutdown window |
| `queues.<name>.scheduler.domain` | scheduler process | scheduler single-active scope | candidate schedulers for the same queue use the same domain |
| `metrics.enabled` | no | whether metrics/introspection are exposed | local introspection must be available; network exposure should be explicit |

## Redis connection examples

Local development:

```ts
connection: { url: 'redis://127.0.0.1:6379' }
```

Managed Redis with auth and TLS:

```ts
connection: {
  url: 'rediss://redis.example.com:6380/0',
  username: 'default',
  password: 'redis-password',
  tls: true
}
```

Sentinel / automatic failover:

```ts
connection: {
  sentinel: {
    name: 'mymaster',
    nodes: [
      { host: '10.0.0.11', port: 26379 },
      { host: '10.0.0.12', port: 26379 },
      { host: '10.0.0.13', port: 26379 }
    ],
    username: 'default',
    password: 'redis-password'
  }
}
```

Do not interpret Sentinel reconnects as guaranteed uninterrupted job execution. During failover, workers should stop uncertain claims, schedulers should stop promoting when single-active ownership is uncertain, and jobs recover through lease/retry/stalled recovery.

## Defaults and range policy

The table below shows defaults, accepted ranges, and what happens when a value is invalid:

| Field | v0.1 recommended default | Range / constraint | Failure behavior |
|-------|--------------------------|--------------------|------------------|
| `connection` | no default | must connect to the target Redis | fail before startup with missing connection information |
| `namespace` | no default | non-empty string; should include environment and app | fail before startup and explain keyspace isolation risk |
| `worker.concurrency` | `1` | positive integer | fail before startup for values below 1 or non-integers |
| `worker.leaseMs` | `30000` | must be greater than `renewIntervalMs` | fail before startup if the relationship is invalid |
| `worker.renewIntervalMs` | `10000` | must be less than `leaseMs` | fail before startup if the relationship is invalid |
| `worker.drainTimeoutMs` | `30000` | non-negative integer | fail before startup for invalid values |
| `defaultJobOptions.attempts` | `3` | positive integer; infinite retry is not the default | fail before startup for invalid values |
| `defaultJobOptions.backoff` | `fixed:1000ms` | fixed, exponential, or implementation-declared custom strategy | fail before startup for unknown strategies |
| `scheduler.domain` | no default | non-empty when `scheduler.enabled=true` | fail before startup when missing |
| `metrics.enabled` | `false` | boolean | fail before startup for non-boolean values |

## Configuration error policy

When configuration is invalid, queuebit reports it before the process starts claiming jobs whenever possible:

| Error type | What you will see |
|------------|---------------------|
| Missing `connection` / `namespace` / `queues` | fail fast and name the missing field plus current process role |
| Redis auth failure or TLS configuration error | fail fast and identify auth, TLS handshake, certificate, or hostname problems |
| Sentinel cannot discover a primary | fail fast or enter unavailable state; schedulers must not promote speculatively |
| Worker command with invalid lease relationships | fail fast and explain that `renewIntervalMs` must be less than `leaseMs` |
| Scheduler command without `scheduler.domain` | fail fast to avoid implicit scheduler domains |
| Redis Cluster configuration detected while Cluster is unsupported | fail fast and point to [Environment and Compatibility Boundary](./compatibility.md) |
| single-process dev topology in production | at least warn; in production mode, fail fast or require explicit override |
| adapter-derived topology | never silently infer; print the final queuebit config summary for inspection |

## CLI commands

CLI coverage:

| Command | Semantics |
|---------|-----------|
| `npx queuebit worker start --config queuebit.config.ts --queue notification` | Start dedicated worker runtime |
| `npx queuebit worker drain --config queuebit.config.ts --queue notification --timeout 30s` | Ask workers to stop claiming and wait for completion |
| `npx queuebit scheduler start --config queuebit.config.ts --domain billing-notification` | Start dedicated scheduler runtime |
| `npx queuebit inspect queue notification --config queuebit.config.ts` | Show queue depth, active, delayed, retry, stalled |
| `npx queuebit inspect workers --queue notification --config queuebit.config.ts` | Show worker identity, heartbeat, drain state |
| `npx queuebit inspect scheduler --domain billing-notification --config queuebit.config.ts` | Show active scheduler identity and domain status |

The same actions are available through the core API or vext adapter. The CLI is convenient for dedicated processes and operations.

## Process entries

Recommended process split:

```text
web process       -> producer only
worker process    -> worker runtime
scheduler process -> scheduler runtime
```

Acceptable development modes:

| Mode | Purpose | Limit |
|------|---------|-------|
| single-process dev | local demo or quick verification | Must be explicit; not a production recommendation |
| worker-only | background consumption | Does not act as scheduler unless enabled |
| scheduler-only | time progression | Does not run business handlers |
| web producer | HTTP/API job submission | Does not consume jobs by default |

## vext configuration relationship

The vext adapter maps vext configuration to queuebit configuration. Keep these boundaries in mind:

- vext app start does not equal queue worker start.
- vext cluster worker count does not equal queue worker concurrency.
- Choose Worker drain or explicit stop during vext reload.
- You still configure `scheduler.domain` and can inspect the active Scheduler.

## Next steps

- First runnable path: [Quick Start](./quick-start.md).
- vext configuration examples: [vext integration](./vext-integration.md).
- Operational checks: [Operations and troubleshooting](./operations.md).
