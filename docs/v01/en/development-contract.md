# Development contract and acceptance route

<span class="manual-label">Maintainer · docs-first implementation guardrails</span>

## Source-of-truth order

1. Confirmed requirements and CP2 decisions.
2. This `docs/v01` target user manual and canonical workflow.
3. Public types, config schema, CLI dispatcher, and runtime exports.
4. Integration, fault, consumer, and package evidence.

Implementation must not silently weaken the user contract because current code is easier. If a contract is infeasible, return to CP2 or requirement change and synchronize the manual before implementation continues.

## Public-contract truth probes

| Documentation claim | Required implementation evidence |
|---|---|
| Method/type | Package export, TypeScript type, runtime dispatcher, smoke |
| Configuration/default | Config schema, validator, default tests |
| CLI command | Parser/dispatcher, JSON/exit code, Windows/Linux loader smoke |
| vext integration | Independent `vextjs@0.3.26` consumer compile/start/close |
| BatchRun workflow | `npm run test:redis`, `npm run test:redis:faults`, and `npm run test:redis:sentinel` against Redis `>=7.2` / Sentinel targets, business DB integration, and crash/replay |
| Mermaid/user path | Fresh site build and Browser desktop/mobile/runtime checks |

Current local source evidence is explicitly scoped from Runtime M0A through M2K. Anything requiring target Redis/Sentinel execution, target destructive purge/full tombstone evidence, production-mounted scrape/auth/network evidence, or publish evidence remains a release gate rather than a completed capability.

## Local documentation site

Use the preview script for review and handoff:

```bash
npm run docs:preview
```

It builds the site first and then serves the generated pages at `http://localhost:4180/queuebit/`. The Chinese entry is `http://localhost:4180/queuebit/zh/`. `npm run docs:dev` serves the same generated-site behavior on `http://127.0.0.1:4181/queuebit/` for local review. Use `npm run docs:edit` only while editing docs; it runs the Rspress hot dev server on `http://127.0.0.1:4182/queuebit/` and must not be used as the handoff URL.

## M0 acceptance

- Multi-process claim, delay/retry, timeout, stalled recovery, cancel, and replacement for Queue/Job/Producer/Worker.
- Lease generation rejects an old attempt.
- All-entry validation and atomic `maxBulk*` plus queue jobs/bytes hard limits for add/addBulk and internal dispatch; M0 does not automate a shared high-low latch.
- Cooperative time-owner handover.
- Node 20 TypeScript loader, `.mjs` parity, ESM/CJS/types.
- Real vext Producer-plugin consumer smoke.

## M1 acceptance

- Source boundary and keyset reread contract.
- Atomic dual dispatch/checkpoint cursor and Batch cursor range.
- Multiple Workers process one Batch and durable per-batch/final completion is delivered.
- Source/Dispatch blocked plus resume; mapper/processor failure pagination plus recovery run.
- Completion generation/retry does not rewrite execution.
- Cancellation, fail-fast, and continue summaries satisfy invariants.
- Canonical example runs from seeded database to final idempotent audit.

## M2 acceptance

- Local M2A foundation: paced `intervalMs` / `maxInFlightBatches`, direct Producer `QB_BACKPRESSURE_*` errors, jobs/bytes high-low latch, and Coordinator `dispatchHoldReason` for interval, in-flight, and backpressure.
- Local M2B foundation: normalized retention/deduplication/observability config, read-only `retention.plan()`, read-only `capacity.snapshot()`, metric prefix/disabled behavior, and ready-time TTL/timing guards.
- Local M2C foundation: `checkpointBatchIndex`, contiguous checkpoint advancement for out-of-order paced batches, no premature terminal state for exhausted empty pages behind earlier in-flight batches, batch completion delivery barrier tests, and actual one-to-many mapper job-count capacity measurement.
- Local M2D foundation: formal `src/observability` backend, shared in-process metrics registry, Prometheus rendering from the same samples, direct job submit, Worker claim/terminal/duration/attempt/stalled recovery, role heartbeat/drain, and Coordinator advance/completion delivery metrics.
- Local M2E foundation: no-listen `observabilityHttp.handle()` health/metrics response helpers, exported `createQueuebitObservabilityHttpApi()` custom paths, and `alerts.evaluate()` local health/metrics/capacity findings.
- Local M2F/M2G/M2H/M2I/M2J/M2K foundation: safe-default `retention.purge()` dry-run plus execute support for safe direct completed Job deletion, identity-bound direct Job tombstone compaction, age-expired terminal Run tombstone compaction with failure envelope cleanup, delivered/not-required Completion event detail tombstones after the parent Run is terminal, precise `terminalRuns.maxCount` through a terminal Run detail index, and independent `completionEvents.ageMs/maxCount` through a Completion detail index. It uses indexed candidate reads, Redis Lua execution guards, protected skip reasons, and no Redis keyspace scan.
- Remaining release M2 evidence: target destructive purge/full Run-failure-completion tombstone execution and target Redis/Sentinel execution.
- TLS/ACL/Sentinel preflight and write-loss-boundary drill.
- Multi-Worker cooperative owner handover, stale-generation rejection, and recovery from an owner gap.
- Logger, metrics, health, retention, tombstone, capacity, alert, and runbook evidence; M2E covers the non-destructive local observability, alert, and no-listen HTTP helper foundation, M2G covers direct Job retention purge/tombstone foundation, M2H covers age-based terminal Run details plus failure envelope expiry, M2I covers delivered/not-required Completion event detail tombstones, M2J covers terminalRuns maxCount precision through terminal detail indexing, and M2K covers independent completionEvents retention through Completion detail indexing. Production-mounted scrape/auth/network and target-environment evidence remain release gates.
- Complete Chinese and English site with link, navigation, search, Mermaid, mobile, and accessibility checks.

## Rejected substitutes

- One `addBulk` in place of BatchRun.
- In-process callback in place of a durable completion event.
- Local Map or lock in place of Redis lease/fencing.
- Offset pagination as the recoverable source path.
- Backpressure measured only by record/pageSize instead of actual jobs/bytes.
- Reopening terminal state in place, editing Redis, or describing Queuebit deduplication as exactly once.
- Inventing vext plugin, route, or lifecycle APIs that do not exist.
- Exposing unpublished standalone Scheduler configuration or commands in v0.1.

## Release closure

The unreleased banner can be removed and the v0.1 manual described as runnable only after M0 and M1 close, all M2 evidence passes, the canonical example succeeds in a clean environment, package/consumer/fault/site validation passes, and the user completes final review.
