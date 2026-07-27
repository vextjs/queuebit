# 业务幂等模式

<!-- queuebit-v01-legacy-doc -->
> [!WARNING]
> **历史文档，已停止维护。** 当前 v0.1 最终用户手册位于 [`docs/v01/zh`](../v01/zh/index.md)。本页仅保留历史上下文，API、命令、配置和示例不得用于新接入或实现。

<span class="manual-label">At-least-once 安全手册</span>

queuebit 保证 job 可恢复，因此 Worker 崩溃、ack 丢失、lease 到期或 Redis 故障都可能让同一业务动作再次执行。`opts.idempotencyKey` 只解决重复入队，不能自动撤销或去重邮件、支付、Webhook 和数据库写入。

<span id="s12-side-effect-idempotency"></span>
## S12 保护外部副作用

先选择真正代表业务动作的稳定键：

| 动作 | 推荐业务键 | 不推荐 |
|------|------------|--------|
| 订单回执 | `receipt:<orderId>` | 随机 UUID、attempt 编号 |
| 月度账单 | `invoice:<accountId>:<month>` | 当前时间戳 |
| Webhook | `webhook:<eventId>:<subscriberId>` | Worker identity |
| 退款 | `refund:<refundRequestId>` | job ID 作为唯一业务依据 |

同一次业务动作的所有 attempt 必须使用同一个键；真正的新业务动作才创建新键。

## 模式一：下游原生幂等键

支付、消息或邮件服务支持 idempotency key 时，直接把业务键传给它：

```ts
async function sendReceipt(job, ctx) {
  const key = `receipt:${job.data.orderId}`;

  const result = await emailProvider.send({
    to: job.data.recipient,
    templateId: job.data.templateId,
    idempotencyKey: key,
    signal: ctx.signal
  });

  await db.notifications.recordProviderResult({
    businessKey: key,
    providerRequestId: result.requestId
  });
}
```

重复请求必须由下游返回已有结果，而不是再次发送。日志记录业务键和 provider request ID，不能只记录 job ID。

## 模式二：数据库唯一约束 + 状态机

下游不支持幂等键时，在业务数据库建立唯一记录：

```ts
async function sendReceipt(job, ctx) {
  const businessKey = `receipt:${job.data.orderId}`;
  const claim = await db.notificationEffects.tryBegin({ businessKey });

  if (claim.status === 'completed') return;
  if (claim.status === 'processing' && !claim.isExpired) {
    throw new Error('Effect is already processing');
  }

  const response = await emailProvider.send({
    to: job.data.recipient,
    templateId: job.data.templateId,
    signal: ctx.signal
  });

  await db.notificationEffects.complete({
    businessKey,
    providerRequestId: response.requestId
  });
}
```

表中 `businessKey` 必须有唯一约束，状态至少包含 `processing/completed/failed`，并保存 owner、过期时间和下游请求 ID。`processing` 超时后不能盲目重发，应先查询下游是否已成功。

## 模式三：事务内写业务结果与 outbox

如果副作用是“更新数据库并发布后续事件”，在一个数据库事务中更新业务状态并写 outbox；独立发布器再发送 outbox。这样重试时可以根据唯一业务键读取已提交结果，避免数据库成功但事件丢失。

```ts
await db.transaction(async (tx) => {
  const changed = await tx.orders.markReceiptPrepared(job.data.orderId);
  if (!changed) return;

  await tx.outbox.insertUnique({
    key: `receipt-ready:${job.data.orderId}`,
    topic: 'receipt-ready',
    payload: { orderId: job.data.orderId }
  });
});
```

## 超时后的判断顺序

1. `ctx.signal` 中止后，先把当前 attempt 当作“不确定”，不要当作“肯定没执行”。
2. 用业务键查询本地状态或下游查询 API。
3. 已完成则直接返回，让 job ack；未开始才安全重试。
4. 无法查询时保留人工核对，不要自动无限重发高风险副作用。

## 反模式

| 反模式 | 风险 | 替代 |
|--------|------|------|
| 每次 attempt 生成随机 key | 下游把每次都当新请求 | 使用稳定业务键 |
| 先发邮件，再无条件插入“已发送” | 插入失败后重试会重复发送 | 下游幂等键或状态机 |
| 只依赖 queue 的 `idempotencyKey` | 只能防重复入队，防不了 ack 窗口 | handler 内保护副作用 |
| 捕获错误后直接返回 | queuebit 会把失败当成功 | 记录后重新抛出 |
| 直接修改 Redis 把 job 标 completed | 业务结果与队列状态永久漂移 | 通过公开 API 和业务补偿处理 |

## 验证清单

- 同一个 job 连续执行两次，业务结果只出现一次。
- 在下游成功后、ack 前强制终止 Worker，恢复后仍只有一个副作用。
- `HandlerTimeoutError` 后能通过业务键判断“已完成/未开始/不确定”。
- 日志能用业务键串起所有 job ID、attempt 和下游请求 ID。
- 修订后的重新提交使用新修订键，并保留旧记录的审计关系。
