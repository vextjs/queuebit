# Environment and Compatibility Boundary

## Read this first

<span class="manual-label">v0.1 final user manual</span>

- queuebit v0.1 integrates Redis only and does not introduce other queue backends.
- The first-version target topology uses explicit producer, worker, and scheduler roles instead of letting Web processes implicitly perform every role.
- Redis standalone or compatible managed single-primary Redis services are the baseline.
- Redis Cluster is unsupported in v0.1; cluster configuration fails fast.
- Node.js `>= 20`, Redis `>= 7.0`, and TypeScript `>= 5.4` are the recommended user-facing combination.

## Prerequisite matrix

| Item | v0.1 user boundary | Notes |
|------|----------------------|-------|
| Node.js runtime | `>= 20` | Targets JavaScript / TypeScript projects |
| Package manager | npm | Package name is `queuebit` |
| Redis server | `>= 7.0` standalone or managed single-primary Redis | Must support TTL, atomic updates, reconnect behavior, and script execution semantics |
| Redis auth / TLS | Supported as connection configuration | Does not change queue consistency semantics |
| Redis Cluster | Unsupported | Startup validation fails instead of silently downgrading |
| Redis Sentinel / failover | Conditional connection-layer support | During failover, workers may stop claiming and schedulers may stop promoting |
| vext adapter | `queuebit/vext` | Adapter translates configuration and lifecycle without hiding topology |
| TypeScript | Recommended `>= 5.4` | Public APIs and adapter config provide types |
| Operating systems | Follow Node.js and Redis client compatibility | No OS-specific capability is required |

## Redis deployment boundary

| Redis shape | First-version stance | User-facing impact to document |
|-------------|----------------------|--------------------------------|
| Standalone Redis | Baseline target | Best fit for first verification, development, and simple production deployments |
| Managed single-primary Redis | Equivalent to the baseline when semantics match | Users must confirm connection, auth, TLS, timeout, and persistence policy |
| Sentinel / automatic failover | Conditional support target | During failover, workers may stop claiming, schedulers may stop promoting, and jobs recover through lease/retry semantics |
| Redis Cluster | Unsupported in v0.1 | Startup validation fails to avoid hidden cross-slot atomicity risks |
| Multiple queue backends | Out of scope | v0.1 does not abstract database, memory, SQS, or other adapters |

## Distributed topology boundary

| Process role | First-version target | Scaling rule | Implicit behavior that is not allowed |
|--------------|:--------------------:|--------------|--------------------------------------|
| Web producer | Yes | Multiple app instances may submit jobs | Starting a Web service does not automatically consume jobs |
| Worker process | Yes | Multiple processes/instances are allowed; concurrency is controlled by `worker.concurrency` | A worker does not become scheduler by default |
| Scheduler process | Yes | Multiple candidate instances may run, but only one is active per `scheduler.domain` | A scheduler does not run business job handlers |
| Single-process dev | Local development only | Must be explicitly marked as dev/demo | Not a production topology recommendation |
| Dashboard / admin UI | Not a v0.1 target | Re-evaluate in a later phase | Does not block the first core runtime release |

## vext first-integration boundary

The `vext` adapter should make queuebit easier to adopt in vext projects, but it must not blur distributed responsibility:

- vext app start does not equal worker start.
- vext cluster worker count does not equal queue worker concurrency.
- vext reload must map to worker drain or explicit stop.
- scheduler domain must remain visible in configuration and operations docs.
- adapter docs must reference this environment matrix and must not define conflicting Redis Cluster or process-topology rules.

## Related references

| Question | Read first |
|----------|------------|
| How does Redis keyspace avoid cross-environment writes? | [Redis model](./redis-model.md) |
| How are process roles configured? | [CLI and configuration](./cli-and-config.md) |
| How do workers and schedulers stop? | [Worker and Scheduler Lifecycle](./worker-lifecycle.md) |
| How does recovery work after failures? | [Failure Modes and Recovery](./failure-modes.md) |
| How should the vext adapter follow these boundaries? | [vext integration](./vext-integration.md) |
