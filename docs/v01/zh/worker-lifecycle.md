# Worker、Coordinator 与时间推进生命周期

<span class="manual-label">Maintainer · 内部资格与清理</span>

这页给维护者核对 Worker、Coordinator 和时间推进的内部生命周期。使用 Queuebit 时只需要知道如何启动 Worker、如何 drain、如何查健康状态。

## 共通阶段

```text
load static config -> validate role registrations -> connect dependencies
-> Redis preflight -> acquire/announce role identity -> ready
-> work loop -> draining -> close role resources -> exit
```

启动失败也必须执行对应 close。resource factory 未打开时 close 安全 no-op；close timeout 记录资源名并非零退出。

## Worker

| 阶段 | 操作 | 失败原则 |
|---|---|---|
| boot | 激活 processor 资源，验证 queue/version | 不 ready，关闭已开资源 |
| claim | 原子声明 attempt/generation/workerId/expiry | 无 owner 就不执行 |
| process | 调用 processor，传 signal/logger/idempotencyKey | timeout/lease lost 触发 signal，不代替 fencing |
| renew | 在租约窗口内续期 | 失败即停止新 claim |
| settle | 校验 generation/owner 并原子更新 job/Batch | stale 返回稳定错误 |
| drain | 停止 claim，等 active，停止续租 | 超时不伪造业务结果 |

在公开 Worker kernel 中，这个 owner generation 对应 `leaseGeneration` 字段。`complete(jobId, leaseGeneration, result)` 与 `fail(jobId, leaseGeneration, error)` 是两个 settle 路径；旧 owner 会稳定返回 `QB_JOB_STATE_CONFLICT`。

## Coordinator

| 阶段 | 操作 | 不变式 |
|---|---|---|
| acquire Run | 按 Run 产生 generation | 旧 generation 不提交 |
| freeze | 冻结 boundary + initial cursors | 一次原子写入 |
| load/map | 读页、纯 mapper、准备 envelope/jobs | 未持久页不提前计数 |
| dispatch | Batch/jobs/summary/envelopes/dispatchCursor 原子提交 | expected cursor + generation |
| checkpoint | 跨 execution + completion 连续屏障 | 不跳过空洞 |
| completion | claim/deliver/settle event | 独立 delivery generation |
| drain | 停 load/dispatch，等当前原子边界 | runtime lifecycle close |

## 时间推进

v0.1 只提供 cooperative 模式：后台 Worker 中的候选循环为 domain 竞争 owner generation；Web/Producer 不参与。standalone Scheduler 延后到后续版本，不存在 v0.1 启动命令或配置兼容承诺。

| 操作 | 资格失效时 |
|---|---|
| promote delayed/retrying | 停止新 promotion |
| stalled 时间检测 | 不提交旧 generation 恢复 |
| owner renew | 不确定即 `not_ready`，停止新 promotion |
| drain | 停新 promotion，安全释放/过期资格 |

## 连接策略

Producer/CLI 使用 `requestRetryLimit` 有界重试后快速失败。Worker/Coordinator/时间推进遇 Redis 中断时停止新 work，并使用 full-jitter 指数退避持久重连：每轮上限依次为 250ms、500ms、1s、2s，最高 30s；实际等待在 0～当前上限内随机。首次失败立即记录，此后同 role/endpoint 最多每 30s 记录一次，直到重连成功、drain 或 close。断连前后都统一映射为 `health.status=not_ready`、`ready=false`，不因“曾经 ready”降格成可放行的 degraded。

## 必测故障窗口

- processor 成功后 ACK 前崩溃。
- timeout 后旧 handler 晚返回。
- Worker renew 成功/响应丢失的不确定窗口。
- Coordinator 在 source load 后、Batch 原子提交前崩溃。
- Batch 已派发但 completion 未交付时崩溃。
- 时间 owner 切换时旧 generation 晚 promotion。
- drain timeout 后新 owner 接管。
