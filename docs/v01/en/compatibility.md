# Can my environment use Queuebit?

<span class="manual-label">Reference · Node, Redis, vext, and workload support</span>

For first integration, follow [Quick start](./quick-start.md). Use this page before production to check whether your Node version, Redis topology, web framework, and workload type are supported by Queuebit v0.1.

## One-minute check

| Your situation | Supported? | Notes |
|---|---|---|
| Node.js `>=20` | yes | core requirement |
| vext project on Node.js `>=20.19` | yes | `queuebit/vext` requirement |
| Redis `>=7.2` single-primary | yes | standalone, managed Redis, TLS, and Sentinel |
| Redis Cluster | no | v0.1 does not support cluster slots or multiple primaries |
| One-off background jobs | yes | Use `jobs.add` plus Worker |
| Finite database batch processing | yes | Use `runs.start` plus Coordinator |
| CDC, infinite streams, cron, DAG/Flow | no | Wait for a later version that explicitly ships them |
| Exactly-once or strict FIFO required | not a fit | Queuebit is at-least-once |
| Any Node web framework | can integrate | Create a client directly and run Workers separately |
| vext | first-class support | First official host is `vextjs@0.3.26` |

## Good fits

- Return HTTP 202 quickly and execute the business action in background Workers.
- Process a finite database dataset by pages, with per-batch and final completion records.
- Scale across multiple Workers and recover after any one process crashes.
- Protect email, payment, webhook, or database writes from duplicate side effects.

## Not a fit

- Redis Cluster, a non-Redis backend, or offline local execution followed by state merging.
- Strict FIFO, key partitions, DAG/Flows, repeatable/cron jobs, priorities, or a global rate limiter.
- Automatic compensation of external effects or an exactly-once guarantee.
- A process-local in-memory queue presented as a distributed queue.

## Install and check the environment

```bash
npm install queuebit
```

```bash
node --version
redis-cli INFO server
redis-cli INFO persistence
redis-cli CONFIG GET maxmemory-policy
```

Production Redis must use `maxmemory-policy=noeviction`, enable persistence and backups for your RPO, and acknowledge that Sentinel asynchronous replication can lose acknowledged writes during failover.

## Validate before starting

```bash
npx queuebit config validate \
  --config queuebit.config.ts \
  --runtime queuebit.runtime.ts

npx queuebit health inspect --config queuebit.config.ts --json
```

| Result | Meaning | Action |
|---|---|---|
| `ready` | The current role and Redis policy are acceptable | Start the workload |
| `degraded` | A warn policy or observability signal is incomplete | Local use may continue; do not admit production traffic |
| `not_ready` | Connection, strict policy, registration, or role ownership failed | Fix the cause before starting |

## Do not look for these old capabilities

v0.1 does not ship a standalone Scheduler, and there is no `scheduler start`, `scheduler inspect`, or `scheduler drain`. Time advancement is cooperative inside background Workers. If you require a completely separate time-advancement process, wait for a later version that explicitly ships it; do not treat older drafts as compatibility promises.

## Next

- First successful run: [Quick start](./quick-start.md).
- Production Redis and Worker values: [Configure Redis and Workers](./configuration-recipes.md).
- Redis outage, failover, or data loss: [When Redis is down](./distributed-semantics.md).
