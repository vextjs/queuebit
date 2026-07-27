# 任务场景与配方

<!-- queuebit-v01-legacy-doc -->
> [!WARNING]
> **历史文档，已停止维护。** 当前 v0.1 最终用户手册位于 [`docs/v01/zh`](../v01/zh/index.md)。本页仅保留历史上下文，API、命令、配置和示例不得用于新接入或实现。

<span class="manual-label">常见任务手册</span>

本页从业务数据库中的待处理订单出发，给出可以直接映射到应用代码的任务流程。首次接入先完成 [快速开始](./quick-start.md)；需要多实例部署时转到 [分布式 Worker](./distributed-workers.md)。

## 共用业务模型

```ts
type ReceiptJob = {
  orderId: string;
  recipient: string;
  templateId: string;
};
```

生产代码应让 Producer 从数据库、API、事件流或导入文件读取业务对象，再生成只包含 handler 所需字段的 payload。不要让 Worker 依赖 Producer 的内存，也不要把固定用户写进主流程。

<span id="s01-batch"></span>
## S01 批量提交待处理订单

**适用情况：** 定时扫描、导入任务、批量补发，或者一次请求产生多条独立 job。

```ts
const pendingOrders = await db.orders.findMany({
  where: { paid: true, receiptQueuedAt: null },
  take: 500
});

const results = await notificationQueue.addBulk(
  pendingOrders.map((order) => ({
    name: 'send-receipt',
    data: {
      orderId: order.id,
      recipient: order.customerEmail,
      templateId: 'receipt-paid'
    },
    opts: {
      idempotencyKey: `receipt:${order.id}`,
      attempts: 3,
      backoff: { type: 'exponential', delayMs: 1000 }
    }
  }))
);

await db.orders.markReceiptQueued(
  results.map(({ job }) => ({ orderId: job.data.orderId, jobId: job.id }))
);
```

成功时，结果顺序与输入顺序一致。整批校验或提交失败时整批拒绝，业务系统不要提前写 `receiptQueuedAt`。相同 key 和相同内容返回 `created: false`；相同 key 但内容不同是幂等冲突，需要先修正业务数据。

<span id="s02-first-success"></span>
## S02 判断第一批是否真正成功

1. Producer 记录批次大小、业务 ID 和返回的 job ID。
2. Worker 记录 job ID、attempt、业务 ID 和下游请求 ID。
3. 执行 `npx queuebit inspect queue notification --config queuebit.config.mjs`。
4. 在业务系统核对通知状态，而不是只看 job 已 `completed`。

成功条件是：输入记录数与提交结果数一致、job 最终为 `completed`、业务副作用存在且没有重复。若 job 完成但业务结果缺失，说明 handler 的完成边界写错；若业务结果存在但 job 重试，按 [业务幂等模式](./idempotency-patterns.md) 修复不确定 ack。

<span id="s03-delayed"></span>
## S03 延迟到指定时间再执行

```ts
const runAt = new Date('2026-08-01T09:00:00+08:00');

await notificationQueue.add('send-renewal-reminder', {
  orderId,
  recipient,
  templateId: 'renewal-reminder'
}, {
  idempotencyKey: `renewal:${orderId}:2026-08`,
  delayMs: Math.max(0, runAt.getTime() - Date.now())
});
```

延迟 job 先进入 `delayed`，到期后由 active Scheduler 推进到 `waiting`。如果到期未执行，先检查系统时间，再执行 `npx queuebit inspect scheduler --domain billing-notification --config queuebit.config.ts`；没有 active Scheduler 时不要通过重复提交绕过。

<span id="s04-retry"></span>
## S04 只重试可恢复错误

```ts
await notificationQueue.add('send-receipt', payload, {
  idempotencyKey: `receipt:${payload.orderId}`,
  attempts: 4,
  backoff: { type: 'exponential', delayMs: 1000 },
  timeoutMs: 15000
});
```

Handler 对网络超时、下游 `429` 和 `5xx` 抛错，让 queuebit 按退避重试；对缺少收件人、模板不存在等永久错误直接抛出可识别业务错误，便于在最终 `failed` 中定位。`timeoutMs` 到期会产生 `HandlerTimeoutError` 并中止 `ctx.signal`，但不能强行终止忽略 signal 的外部调用。

<span id="s05-terminal-failed"></span>
## S05 处理终止失败

v0.1 不提供内置 DLQ、手动 retry 或 replay API。终止失败的生产处理顺序是：

1. 用 inspect 和日志读取最终错误、attempt 历史、job ID 与业务 ID。
2. 判断是业务数据错误、永久下游拒绝，还是重试预算不足。
3. 修复业务数据或下游根因，并确认旧 attempt 是否产生过副作用。
4. 由业务管理入口重新提交一个可审计的新 job；使用包含修订版本的幂等键，例如 `receipt:order-42:correction-2`。
5. 在业务库记录旧 job、新 job、操作者、原因和修复结果。

不要直接改 Redis 状态，也不要原样无限重投。需要标准化人工处置时，由应用建立自己的“失败任务管理”页面并调用公开 `Queue` API。

<span id="s06-concurrency"></span>
## S06 调整 Worker 并发

```ts
const worker = new Worker('notification', handler, {
  connection,
  namespace: 'prod:billing',
  concurrency: 4
});
```

单实例理论并发等于 `concurrency`；总并发约等于各健康 Worker 并发之和。先按下游连接池、限额和单 job 内存选择较小值，再观察处理延迟、错误率和 Redis 延迟。v0.1 没有内置全局 rate limiter；若下游有全局配额，应在业务客户端或网关集中限流，不能只降低单个 Worker 的并发并假设集群总量受控。

<span id="s17-events"></span>
## S17 观察任务生命周期（无 Worker 事件总线）

当前 Queuebit **没有** 公共 `worker.on(...)` API。请通过结构化日志、role heartbeat 与进程内 metrics（例如 `worker_jobs_completed_total`、`worker_jobs_failed_total`、`worker_stalled_jobs_recovered_total`）观察作业生命周期。metrics/日志发送失败不得改写 Job 状态。

> v0.1 合同以 `docs/v01/` 双语手册为准；本目录可能滞后。

<span id="s18-metrics"></span>
## S18 读取并告警指标

```ts
const snapshot = await notificationQueue.inspect();

metrics.gauge('queuebit.waiting', snapshot.waiting);
metrics.gauge('queuebit.active', snapshot.active);
metrics.gauge('queuebit.delayed', snapshot.delayed);
metrics.gauge('queuebit.retrying', snapshot.retrying);
metrics.gauge('queuebit.failed', snapshot.failed);
metrics.gauge('queuebit.stalled', snapshot.stalled);
```

v0.1 只提供 `queue.inspect()` 和 CLI JSON 拉取，不内置 Dashboard 或 Prometheus HTTP Server。告警至少覆盖 waiting 持续增长、最老 waiting 时长、failed 增量、stalled 增量、Worker 心跳数和 active Scheduler 身份。阈值应基于业务 SLO，不要把队列非空本身视为故障。

## 下一步

- 多实例、滚动发布和故障恢复：[分布式 Worker](./distributed-workers.md)
- 外部副作用去重：[业务幂等模式](./idempotency-patterns.md)
- 参数选择：[配置场景与配方](./configuration-recipes.md)
- 线上故障操作：[故障处置手册](./failure-runbooks.md)
