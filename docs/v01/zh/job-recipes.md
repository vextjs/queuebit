# 执行一个后台任务

<span class="manual-label">快速开始 · 一个任务从提交到完成</span>

本页接在 [快速开始](./quick-start.md) 后面。先看最小提交：Web/API 把一个 payload 放进 Queue，Worker 执行同名 processor。确认这条线跑通后，再按需要加入重试、幂等、延时、批量提交或失败恢复。

| 层级 | 你需要知道 |
|---|---|
| 必须 | `queue` 名、processor 名、payload、Worker 已启动 |
| 日常常用 | `attempts`、`timeoutMs`、`deduplicationKey`、`idempotencyKey` |
| 按需使用 | `delayMs`、`addBulk`、`cancel`、`retryFailed` |
| 复杂场景 | 数据库分页、每批/最终回调、崩溃恢复请用 [BatchRun](./batch-runs.md) |

<span id="sc02-direct-job"></span>
## 最小提交：先不要加 options

```ts
const job = await queuebit.jobs.add(
  'notification',
  'send-receipt',
  {
    schemaVersion: 1,
    tenantId,
    orderId,
    recipient
  }
);

return { jobId: job.id, state: job.state };
```

这段代码只做一件事：把 `send-receipt` 这个任务放进 `notification` 队列。payload 来自已通过认证和校验的请求与服务端业务查询，不写死用户。API 通常返回 HTTP 202 和 `jobId`；业务结果由查询端点或业务状态表提供。

```bash
npx queuebit job inspect <jobId> --config queuebit.config.ts
```

## 日常开发：什么时候加 options

先跑通最小提交，再按业务风险加这些选项：

| 需求 | 加什么 | 作用 |
|---|---|---|
| 失败后自动再试 | `attempts` + `backoff` | 网络抖动或 5xx 时重试 |
| 单次执行不能无限等 | `timeoutMs` | 超时后让 Queuebit 进入失败或重试判断 |
| 同一个业务请求不要重复入队 | `deduplicationKey` | 同 key 同 payload 返回同一个 job |
| 邮件、Webhook、支付不要重复生效 | `idempotencyKey` | 传给 processor，给外部副作用去重 |

```ts
const job = await queuebit.jobs.add(
  'notification',
  'send-receipt',
  { schemaVersion: 1, tenantId, orderId, recipient },
  {
    attempts: 3,
    timeoutMs: 30_000,
    deduplicationKey: `request:receipt:${tenantId}:${orderId}`,
    idempotencyKey: `receipt:${tenantId}:${orderId}`
  }
);
```

常见提交失败这样处理：

| 现象 | Queuebit 会返回 | 你应该怎么做 |
|---|---|---|
| Redis 连不上 | `QB_REDIS_CONNECTION_FAILED`，可重试 | 短暂等待后用同一个 `deduplicationKey` 再提交 |
| 同一个业务请求换了 payload | `QB_JOB_DEDUPLICATION_CONFLICT`，409 | 检查 `tenantId/orderId` 这类业务身份，不要生成随机 key 绕过冲突 |
| 队列太满 | `QB_BACKPRESSURE_REJECTED`，429 | 按返回的 `details` 等到队列低于恢复线再提交 |

## 一次提交一小组任务：`addBulk`

```ts
const results = await queuebit.jobs.addBulk('notification', [
  {
    name: 'send-receipt',
    data: { schemaVersion: 1, orderId: 1001, tenantId, recipient: 'a@example.com' },
    options: {
      deduplicationKey: `request:receipt:${tenantId}:1001`,
      idempotencyKey: `receipt:${tenantId}:1001`
    }
  },
  {
    name: 'send-receipt',
    data: { schemaVersion: 1, orderId: 1002, tenantId, recipient: 'b@example.com' },
    options: {
      deduplicationKey: `request:receipt:${tenantId}:1002`,
      idempotencyKey: `receipt:${tenantId}:1002`
    }
  }
]);
```

`addBulk` 适合一次提交已知数量的一小组任务，比如已经查好的一页订单。它只负责把这一组 job 写入同一个 Queue：要么全部写入，要么全部失败。它不会持续扫描数据库，也不会记录批处理进度、当前位置或恢复记录；要处理很多记录并支持恢复，请用 [批量处理数据库记录](./batch-runs.md)。

<span id="sc05-delay-retry"></span>
## 延时执行和重试

```ts
await queuebit.jobs.add(
  'webhook',
  'deliver-webhook',
  { schemaVersion: 1, endpointId, eventId },
  {
    delayMs: 60_000,
    attempts: 5,
    backoff: {
      type: 'exponential',
      delayMs: 1_000,
      maxDelayMs: 30_000,
      jitter: 0.2
    },
    idempotencyKey: `webhook:${endpointId}:${eventId}`
  }
);
```

`delayMs` 表示先等一段时间再执行。`attempts` 包含第一次执行，processor 抛出可重试错误或 timeout 后才会消耗下一次 attempt。没有 Worker 在线时，job 会留在 Redis 里；Worker 恢复后继续处理。

## Processor 需要更多上下文时

最小 processor 只用 `data`。当你要记录日志、处理超时或给外部系统传幂等键时，再读取更多上下文：

```ts
processors: {
  'deliver-webhook': defineQueuebitProcessor(async ({
    data,
    signal,
    attempt,
    idempotencyKey,
    logger
  }) => {
    logger.info({ eventId: data.eventId, attempt }, 'deliver webhook');
    await webhookClient.send(data, {
      signal,
      idempotencyKey
    });
  })
}
```

- 429/5xx 和短暂网络错误：标记为可重试，让 Queuebit 按 backoff 再试。
- 无效 endpoint、schema 不兼容或鉴权被永久撤销：快速失败，不要反复重试。
- timeout 会通过 `AbortSignal` 通知业务代码停止；如果旧执行很晚才返回，也不能覆盖新的执行结果。

## 取消或重试失败任务

```ts
await queuebit.jobs.cancel(waitingJobId);

const replacement = await queuebit.jobs.retryFailed(failedJobId, {
  deduplicationKey: `replacement:${failedJobId}:1`
});
```

`jobs.cancel` 只能取消还没开始或正在等待重试的 job。直接提交的 failed job 可以用新的替换身份重新提交，通常继续沿用业务 `idempotencyKey`，这样邮件、Webhook、支付这类副作用仍然只会生效一次。BatchRun 里的 job 不要单独替换，请用 `runs.retryFailed` 从批处理层恢复。

## 下一步

- 保护邮件/Webhook/支付副作用：[防止重复副作用](./idempotency-patterns.md)。
- 运行多个 Worker：[多个 Worker 怎么一起跑](./distributed-workers.md)。
- 查精确方法和错误：[API 快查](./target-api.md)。
