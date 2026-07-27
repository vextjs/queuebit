# 状态和错误怎么读

<span class="manual-label">参考 · 从现象定位到状态和错误码</span>

这页不是排障流程，而是“字典”。如果线上已经出问题，先看 [故障恢复](./failure-runbooks.md)；如果只是想理解页面、日志或 API 返回里的状态，就从下面按现象查。

## 先按问题查

| 你看到什么 | 先看哪里 |
|---|---|
| 单个后台任务卡住、失败或被取消 | [Job 状态：一个任务做到哪一步](#job-state) |
| 一次数据库批处理 paused、blocked、partial_failed | [BatchRun 状态：一次批处理做到哪一步](#batchrun-state) |
| 成功/失败回调没有送达 | [Completion 状态：结果回写做到哪一步](#completion-state) |
| completion event 里出现 `batch.settled` 或 `run.settled` | [Completion event：哪些结果会回写](#completion-event-types) |
| API 或 CLI 抛出 `QB_*` 错误 | [错误码按前缀判断](#error-code-groups) |
| 想知道日志里该带哪些字段 | [事件和日志](#events-and-logs) |

<a id="job-state"></a>
## Job 状态：一个任务做到哪一步

| 状态 | 含义 | 常见下一步 |
|---|---|---|
| `waiting` | 可被 Worker claim | active 或 cancel |
| `active` | 已有 Worker lease owner | completed/retrying/failed，或 lease 过期后原子回到 waiting |
| `delayed` | 未到执行时间 | 到期后 waiting |
| `retrying` | 业务失败等待 backoff | 到期后 waiting，或 attempts 耗尽后 failed |
| `completed` | processor 成功且有效 generation settle | 终态 |
| `failed` | 非可重试、attempts 耗尽或 stalled 超限 | 直接 job 创建 replacement；BatchRun job 进入 Run 汇总/recovery |
| `cancelled` | 未 active job 被取消 | 终态 |

`stalled` 是可观测恢复事件和计数器，不是可停留、可过滤的 `JobState`。lease 过期时，原子恢复操作校验旧 generation、递增 `stalledRecoveries`，并直接把 job 从 `active` 转回 `waiting`；超过上限则转为 `failed`。

<a id="batchrun-state"></a>
## BatchRun 状态：一次批处理做到哪一步

| 状态 | 含义 | 控制/恢复 |
|---|---|---|
| `created` | Run identity 已持久化，等待 Coordinator 首次推进 | inspect/cancel |
| `running` | 正在 freeze/load/dispatch/wait/completion 闭环 | 可 pause/cancel |
| `pausing` | 已停止创建新 Batch，正在等待当前原子边界 | inspect |
| `paused` | 用户暂停新推进 | resume/cancel |
| `blocked` | Source/Dispatch/Redis 控制面重试耗尽或前置检查失败 | 修复后 resume 原 Run |
| `cancelling` | 停止新 Batch，等待 active work 收敛 | 最终 cancelled |
| `completed` | 所有业务 work 成功/skipped | 终态，completion 仍可独立失败 |
| `partial_failed` | continue 策略下存在终止失败 work | 可创建 recovery run |
| `failed` | fail-fast 或无法完成的业务失败 | 可对保留 envelope 创建 recovery run |
| `cancelled` | 取消收敛完成 | 需重新处理时创建全新 Run |

`dispatchHoldReason` 是 running 内的自动等待：`interval/in_flight_limit/backpressure/no_active_worker/redis_reconnecting`，不等于 blocked。

<a id="completion-state"></a>
## Completion 状态：结果回写做到哪一步

| 状态 | 含义 |
|---|---|
| `not_created` | execution 尚未终态，completion event 尚不存在 |
| `not_required` | 未配置 handler，屏障自动通过 |
| `pending` | 持久化 event 待投递 |
| `delivering` | 已有 delivery owner/generation |
| `retrying` | handler 失败后等待 backoff |
| `delivered` | handler 成功，屏障通过 |
| `failed` | delivery attempts 耗尽，需修复后显式 retry event |

<a id="completion-event-types"></a>
## Completion event：哪些结果会回写

| type | 触发 | 关键内容 |
|---|---|---|
| `batch.settled` | 每个 Batch execution 终态 | batchId、executionState、summary、attempt/generation |
| `run.settled` | Run completed/partial_failed/failed | runId/parentRunId/recoveryDepth/summary |
| `run.cancelled` | 取消收敛 | reason、已读/未派发汇总 |

汇总不变式：

```text
recordsSeen = recordsDispatched + recordsSkipped + recordsFailed + recordsUndispatched
jobsCreated = jobsCompleted + jobsFailed + jobsCancelled
```

## 错误长什么样

```ts
// 公共形状：QueuebitError 是带稳定 code/details 的 Error 子类。
class QueuebitError extends Error {
  readonly code: string;
  readonly details?: unknown;
  // name === 'QueuebitError'
}
```

<a id="error-code-groups"></a>
## 错误码分组

| 前缀 | 含义 | 代表错误 | 恢复原则 |
|---|---|---|---|
| `QB_CONFIG_*` | 静态配置/runtime registration | `QB_CONFIG_HANDLER_NOT_REGISTERED` | 修配置后重启，不带错运行 |
| `QB_REDIS_*` | 连接/协调 | `QB_REDIS_CONNECTION_FAILED`, `QB_REDIS_PREFLIGHT_FAILED` | 同 identity 重试，后台 fail-safe 重连 |
| `QB_SOURCE_*` | freeze/load | `QB_SOURCE_CURSOR_NOT_ADVANCED` | 修 source，blocked 后 resume |
| Mapper failure | record 转换 | 已保存失败详情里的 serialized mapper error | 保留失败详情，终态后 recovery run |
| `QB_DISPATCH_*` | Batch/job 原子提交 | `QB_DISPATCH_STATE_CONFLICT`, `QB_DISPATCH_LIMIT_EXCEEDED` | 不推进 cursor，修原因后 resume |
| `QB_JOB_*` | processor/state/limits | `QB_JOB_STATE_CONFLICT`, `QB_JOB_LIMIT_EXCEEDED` | 按 retry policy，外部副作用要能去重 |
| owner generation | Worker/Coordinator fencing | `QB_JOB_STATE_CONFLICT`, `QB_DISPATCH_STATE_CONFLICT` | 旧 owner 停止提交，等新 owner |
| `QB_COMPLETION_*` | completion delivery | `QB_COMPLETION_STATE_CONFLICT` | 修 handler 后 retry event |
| `QB_RUN_*` | input/state/recovery | `QB_RUN_INPUT_INVALID`, `QB_RUN_STATE_CONFLICT` | 按 details 修 input，或创建新 Run |
| deduplication conflict | 键与 canonical input | `QB_JOB_DEDUPLICATION_CONFLICT`, `QB_RUN_DEDUPLICATION_CONFLICT` | 修复业务 identity，不换随机 key |
| `QB_BACKPRESSURE_*` | Queue jobs/bytes 水位 | `QB_BACKPRESSURE_REJECTED`, `QB_BACKPRESSURE_REQUEST_TOO_LARGE` | 等 low 或缩小请求 |

<a id="events-and-logs"></a>
## 事件和日志

当前没有公共 Worker 事件总线或 `worker.on(...)` listener API。作业生命周期通过结构化日志、role heartbeat 与进程内 metrics 观察（例如 `worker_jobs_completed_total`、`worker_jobs_failed_total`、`worker_stalled_jobs_recovered_total` 与 attempt 计数）。metrics/日志发送失败不得改写 Job 状态。关联字段使用 namespace/queue/runId/batchId/jobId/eventId/attempt/leaseGeneration/role identity/errorCode，默认不输出完整业务 payload。
