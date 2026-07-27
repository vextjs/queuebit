# 开发合同与验收路线

<span class="manual-label">Maintainer · docs-first 实现护栏</span>

## 真相源顺序

1. 已确认需求与 CP2 决策。
2. 本 `docs/v01` 目标用户手册与 canonical workflow。
3. public types/config schema/CLI dispatcher/runtime exports。
4. integration/fault/consumer/package 证据。

实现不得通过“当前代码更简单”静默削弱用户契约。如果发现契约不可行，先回到 CP2/需求更改，再同步手册。

## 公开契约真相探针

| 文档声明 | 必须有的实现证据 |
|---|---|
| 方法/类型 | package export + TypeScript type + runtime dispatcher + smoke |
| 配置字段/默认 | config schema + validator + default tests |
| CLI 命令 | parser/dispatcher + JSON/exit code + Windows/Linux loader smoke |
| vext 接入 | `vextjs@0.3.26` 独立 consumer compile/start/close |
| BatchRun 流程 | `npm run test:redis`、`npm run test:redis:faults` 与 `npm run test:redis:sentinel` 连接 Redis `>=7.2` / Sentinel target + business DB integration + crash/replay |
| Mermaid/用户路径 | fresh site build + Browser desktop/mobile/runtime checks |

当前本地源码证据明确只覆盖 Runtime M0A 到 M2K。凡是依赖目标 Redis/Sentinel 实机执行、目标 destructive purge/full tombstone 证据、production-mounted scrape/auth/network 证据或 publish evidence 的内容，仍属于 release gate，不是已完成能力。

## M0 验收

- Queue/Job/Producer/Worker 多进程 claim、delay/retry、timeout、stalled、cancel/replacement。
- lease generation 拒绝旧 attempt。
- add/addBulk 与内部派发整批校验、原子性、`maxBulk*` 和 queue jobs/bytes hard limit；M0 不实现共享 high-low latch 自动恢复。
- cooperative 时间推进 owner 切换。
- Node 20 TS loader、`.mjs` parity、ESM/CJS/types。
- vext Producer plugin 真实 consumer smoke。

## M1 验收

- source boundary/keyset 可重读契约。
- dispatch/checkpoint 双 cursor 与 Batch cursor range 原子性。
- 多 Worker 处理同 Batch，每批/最终 completion 持久交付。
- Source/Dispatch blocked + resume，mapper/processor failure pagination + recovery run。
- completion generation/retry 不改写 execution。
- 取消/fail-fast/continue 汇总守恒。
- canonical example 从 seed DB 到最终幂等审计。

## M2 验收

- M2A 本地 foundation：paced `intervalMs` / `maxInFlightBatches`、direct Producer `QB_BACKPRESSURE_*` 错误、jobs/bytes high-low latch，以及 Coordinator 对 interval、in-flight、backpressure 的 `dispatchHoldReason`。
- M2B 本地 foundation：retention/deduplication/observability config 归一化、只读 `retention.plan()`、只读 `capacity.snapshot()`、metrics prefix/disabled 行为，以及 TTL/timing ready-time guard。
- M2C 本地 foundation：`checkpointBatchIndex`、乱序 paced Batch 的连续 checkpoint 推进、早期 in-flight Batch 未关闭时 exhausted 空页不得提前终态、Batch completion delivery 屏障测试，以及 one-to-many mapper 按实际 jobs 计量容量。
- M2D 本地 foundation：正式 `src/observability` backend、共享本进程 metrics registry、同源 Prometheus rendering、direct job submit、Worker claim/terminal/duration/attempt/stalled recovery、role heartbeat/drain、Coordinator advance/completion delivery metrics。
- M2E 本地 foundation：no-listen `observabilityHttp.handle()` health/metrics response helper、导出的 `createQueuebitObservabilityHttpApi()` 自定义路径，以及 `alerts.evaluate()` 本地 health/metrics/capacity findings。
- M2F/M2G/M2H/M2I/M2J/M2K 本地 foundation：safe-default `retention.purge()` dry-run，以及针对安全 direct completed Job 的删除、identity-bound direct Job tombstone 压缩、age-expired terminal Run tombstone、failure envelope cleanup、父 Run 已终态后的 delivered/not_required Completion event detail tombstone、通过 terminal Run detail index 精确执行 `terminalRuns.maxCount`，以及通过 Completion detail index 独立执行 `completionEvents.ageMs/maxCount`；使用索引候选读取、Redis Lua 执行保护、受保护 skip reason，不扫描 Redis keyspace。
- 剩余 release M2 证据：目标 destructive purge/full Run-failure-completion tombstone 执行和目标 Redis/Sentinel 实机执行。
- TLS/ACL/Sentinel preflight 与丢写边界演练。
- cooperative 时间推进的多 Worker owner handover、旧 generation 拒绝和无 owner 恢复证据。
- logger/metrics/health/retention/tombstone/capacity/alert/runbook；M2E 已覆盖非破坏性的本地 observability、alert 和 no-listen HTTP helper foundation，M2G 覆盖 direct Job retention purge/tombstone foundation，M2H 覆盖 age-based terminal Run details 与 failure envelope 过期，M2I 覆盖 delivered/not_required Completion event detail tombstone，M2J 覆盖基于 terminal detail index 的 terminalRuns maxCount 精确执行，M2K 覆盖基于 Completion detail index 的独立 completionEvents retention。生产挂载 scrape/auth/network 与目标环境证据仍是 release gate。
- 完整中英文站点，链接、导航、搜索、Mermaid、移动端、可访问性验证。

## 禁止通过的“替代实现”

- 用单次 `addBulk` 代替 BatchRun。
- 用进程内 callback 代替持久 completion event。
- 用本地 Map/lock 代替 Redis lease/fencing。
- 用 offset 分页作可恢复 source 主路径。
- 只按 record/pageSize 而不按 jobs/bytes 实际背压。
- 终态原地重开、手工改 Redis 或把 Queuebit dedup 写成 exactly-once。
- 发明不存在的 vext plugin/route/lifecycle API。
- 在 v0.1 暴露未发布的 standalone Scheduler 配置或命令。

## 发布关闭

只有 M0/M1 已关闭、M2 所有证据通过、canonical example 在干净环境运行、package/consumer/故障注入/站点验证通过并获得用户最终复核后，才能移除 unreleased banner 并宣称 v0.1 手册可运行。
