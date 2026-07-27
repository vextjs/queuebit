# 架构说明

<span class="manual-label">Maintainer · 不是接入前置</span>

这页给实现和维护者核对边界。只是接入 Queuebit 的使用者不需要读这里，请从 [快速开始](./quick-start.md)、[执行一个后台任务](./job-recipes.md) 或 [配置 Redis 和 Worker](./configuration-recipes.md) 开始。

## 架构目标

- Redis-only：共享正确性状态不落本地内存/文件。
- distributed-first：所有 claim、cursor、Batch、completion 和 role ownership 在多进程下收敛。
- core/host 解耦：vext 是 client/DI/lifecycle 桥接，core 不依赖 app。
- at-least-once：用 fencing 保护 Redis 提交，用业务幂等保护外部副作用。

## 模块边界

| 模块 | 职责 | 不应拥有 |
|---|---|---|
| config/schema | 静态类型、默认、交叉校验、canonical digest | 业务连接和 handler 函数 |
| client/producer | job/run 创建、查询、控制、快速失败 | processor/source 执行 |
| worker runtime | claim/renew/process/settle/drain | source 和 Run cursor |
| coordinator runtime | source/mapper/Batch/cursor/completion | processor 执行 |
| time advancement | delayed/retry 到期提升与时间恢复 | 业务 handler/DB source |
| Redis adapter | keyspace、原子转换、索引、retention | 业务授权和副作用 |
| vext adapter | plugin/extension/logger/onClose/consumer type | 隐式启动后台角色 |

## 原子边界

1. Job claim 同时写 owner/attempt/leaseGeneration/lease expiry。
2. Job settle 校验 jobId + attempt + generation + workerId，并与 Batch 计数一起提交。
3. Source 页转换后，Batch identity、cursor range、record summary、replay envelopes、jobs 和 dispatchCursor 一起提交。
4. checkpoint 只跨过连续 Batch execution + completion 屏障前缀。
5. Completion claim/settle 校验 eventId + attempt + deliveryGeneration + ownerId。
6. Queue jobs/bytes 背压计数与 add/addBulk/Batch dispatch 同一原子边界。

## 角色组合与 lazy load

`queuebit.runtime.ts` 可是单一组合入口，但 module import 不建立连接。Worker 只激活 processor，Coordinator 只激活 source/mapper/completion；cooperative 时间推进只复用 Worker 的 Redis 连接和资格循环，不激活 source、completion 或额外业务 DB/HTTP 资源。大项目可拆 role-specific module，但全站 canonical example 仍只有一套定义真相。

## 阶段关闭

| 阶段 | 实现范围 | 关闭证据 |
|---|---|---|
| M0 Queue kernel | Queue/Job/Producer/Worker、delay/retry、lease/fencing、direct replacement、cooperative time、vext Producer | 多 Worker、旧 attempt 拒绝、崩溃重投、addBulk 原子/限制、loader/ESM/CJS/types/consumer smoke |
| M1 BatchRun closed loop | Source/Mapper/Coordinator/Batch/completion、双 cursor、blocked/recovery | seed DB -> final completion，多 Worker、Coordinator crash、cursor、completion generation、取消守恒 |
| M2 Production foundation（源码 M2A～M2K） | paced/backpressure、TLS/ACL/Sentinel 映射、metrics/health/CLI foundation、双语站点 | 本地 foundation 测试 + 环境门控 Redis harness。**在**目标 Redis `>=7.2` 实跑、fault/Sentinel failover、destructive purge/full tombstone、production scrape/auth/network、干净 example E2E 与 publish gate 闭合前，**不是**完整 v0.1 |

M0/M1 是内部里程碑，不得单独宣称完整 v0.1。M2K 源码交付是 foundation，不是 release-complete v0.1。
