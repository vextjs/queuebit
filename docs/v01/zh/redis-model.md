# Redis 模型与原子不变式

<span class="manual-label">Maintainer · 内部存储合同</span>

这页解释 Queuebit 如何在 Redis 里保持状态一致。普通使用者不要依赖这里的 key 或模型；接入、排查和恢复都只使用公开 API/CLI。

## 用户边界

公开 API/CLI 是唯一支持的查询和恢复入口。Redis key 不是用户 API，不允许通过手工修改 key 完成 cancel/retry/recovery。

生产 API 不扫描任意 Redis keyspace 来完成用户操作。目标测试 harness 只允许用 `SCAN qb:{namespace}:*` 清理本次测试创建的唯一 namespace；Queuebit 文档、CLI 和恢复流程都不得依赖 `FLUSHDB`、`FLUSHALL`、全局 `SCRIPT FLUSH` 或手工改 key。

## 概念 keyspace

| 类别 | 内容 | 主要索引 |
|---|---|---|
| Queue | waiting/active/delayed/retrying/terminal、jobs/bytes watermark latch | queue + state + due/sequence |
| Job | data/options/attempt/lease generation/owner/result/error/parent identity | jobId、dedup digest |
| Run | definition/version/input digest/boundary/dispatch/checkpoint/source exhausted/summary | runId、definition/state/created sequence |
| Batch | index/cursor range/record summary/jobs/execution/completion | runId + batch index |
| Failure envelope | mapper record 或 processor job 重放信息 | runId + failure sequence |
| Completion event | type/attempt/delivery generation/handler/summary/error | event sequence + run/batch/state |
| Role lease | Worker/Coordinator/time owner identity、generation、expiry | role/domain |
| Tombstone | detail 清理后的 identity/digest/version/state | dedup key TTL |

具体 key 命名在实现阶段冻结，但必须使用 hash tag 或等价的单主原子边界保证相关状态一起提交。v0.1 不支持 Redis Cluster，不因 key 分片而削弱原子契约。

## 必须原子的转换

- Job add/addBulk/dedup/backpressure 计数。
- waiting/delayed/retrying -> active claim。
- active -> completed/retrying/failed + Batch summary。
- lease expiry -> stalled reclaim generation。
- Run boundary + initial dispatch/checkpoint cursor。
- source page -> Batch/jobs/envelopes/dispatchCursor。
- Batch 屏障 -> checkpoint 连续前缀级联推进。
- source exhausted marker + Run 终态评估。
- completion claim/settle/retry generation。
- pause/resume/cancel/recovery identity 幂等。

## Canonical input

`qbcj-v1` 只接受 JSON 可序列化值；对象键递归按 Unicode code point 排序，数组保序，字符串 UTF-8，保存 version + SHA-256 digest。`undefined`、函数、symbol、BigInt、NaN/Infinity、循环引用在入 Redis 前失败。

## Retention 与不可删除状态

- active/waiting/delayed/retrying 和 execution 非终态 work 不清理。
- Run 或任一后代 Batch completion 未到 `not_required/delivered` 时不清理。
- `completionState=failed` event 不静默删除，必须告警并显式 retry。
- failedWork 清理后 Run 显示 recovery data expired。
- detail 在 dedup TTL 前清理时保留紧凑 tombstone，直到 key TTL 结束。
- Runtime M2K `retention.purge()` 是安全本地 Job/Run/Completion foundation：它读取已声明 queue 的 `completed` 索引、terminal Run detail index 和 Completion detail index，默认 dry-run，删除没有 identity 引用的 direct completed Job，把 deduplication/idempotency/replacement 绑定的 direct completed Job 压缩为 `detailsExpired=true` tombstone，也会把 completion 为 `not_required` 或 `delivered` 的 age-expired 或超过 maxCount 窗口的 terminal Run 压缩为 tombstone，并用独立 `completionEvents.ageMs/maxCount` 在父 Run 已终态后压缩 delivered/not_required Completion event。Run 压缩会删除 input/boundary/cursors 与 failure replay envelope，同时保留 identity 和 summary counters，然后从 terminal detail index 移除，避免 tombstone 继续占用 `terminalRuns.maxCount`。Completion 压缩会删除 `summary`、backoff/error、due 和 delivery lease 详情，同时保留 event identity 与 `summaryDigest`，并从 Completion detail index 移除，避免 tombstone 继续占用 `completionEvents.maxCount`；稳定 Completion event index 继续支持 `completions.list/get` 读取 tombstone identity。它仍会跳过非终态 work、pending/retrying/delivering/failed completion event、父 Run 未终态的 Completion event、BatchRun-owned job cleanup 和目标 Redis 清理证据。

## Server policy

strict preflight 核对 noeviction、persistence 状态、primary/replica role、复制连接/延迟和最近 persistence error。Sentinel failover 后重做检查。不可读 policy 是 unknown/not_ready，不能推断 healthy。

## 验证矩阵

- 并发 claim 只有一个 owner。
- 旧 Worker/Coordinator/time/completion generation 晚提交被拒绝。
- 页提交失败不只推进 cursor。
- paced Batch 乱序完成不跳 checkpoint。
- addBulk 校验/背压失败时无部分 jobs。
- retention 不删 active 或未交付 completion，tombstone 保持去重冲突。
