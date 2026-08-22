# 快速开始：执行一个后台任务

<span class="manual-label">快速开始 · 先跑通最小接入</span>

本页只让你先跑通第一个后台任务：安装 Queuebit，连上 Redis，直接创建 client，从 Web/API 调用 `jobs.add()`，由应用代码创建 Worker，并确认 job 完成。

先不要理解所有能力。第一次接入只需要这张表左侧：

| 现在需要知道 | 先不用管 |
|---|---|
| Redis 连接、queue 名、`jobs.add()`、Worker 服务代码 | 结果回写、防重复、自动重试、延时执行、批量数据库处理、多进程部署、框架接入 |

## 1. 安装 Queuebit，准备 Redis

```bash
npm install queuebit
```

Queuebit 需要一个可连接的 Redis `>=7.2`。把它的地址放进 `QUEUEBIT_REDIS_URL`；如果 Redis 就在本机默认地址 `redis://127.0.0.1:6379/0`，下面的示例会直接使用该默认值。

## 2. 写一个普通配置对象

告诉 Queuebit Redis 在哪里，以及本项目有哪些队列。这里先只建一个 `notification` 队列。这个对象不是特殊的框架文件；后面的 API 和 Worker 直接复用它。

```ts title="queuebit.ts"
export const queuebitConfig = {
  connection: { url: process.env.QUEUEBIT_REDIS_URL ?? 'redis://127.0.0.1:6379/0' },
  queues: {
    notification: {}
  }
};
```

这两个字段分别表示：

- `connection.url`：Redis 的连接地址；环境变量让不同环境使用不同 Redis，而本地默认值便于首次运行。
- `queues.notification`：声明本示例要使用的队列。首次任务只有这一条队列，所以不需要其他配置。

Queuebit 会从最近的 `package.json` 名称自动派生稳定、隔离的 namespace，因此同一应用的 API 和 Worker 不用额外配置就会使用同一组 Redis key。若同一个包的多个部署共用一个 Redis，为每个部署设置不同的 `QUEUEBIT_NAMESPACE`；代码中的显式 `namespace` 优先级更高。

`createQueuebitClient(queuebitConfig)` 会校验并规范化这个普通对象；这里不需要额外的配置包装函数。

## 3. 用服务代码启动 Worker

先把业务函数直接传给 Worker。第一次接入不需要 `queuebit.runtime.ts`；失败处理、超时和防重复后面再加。

```ts title="notification-worker.ts"
import { createQueuebitClient } from 'queuebit';
import { queuebitConfig } from './queuebit.js';

export async function startNotificationWorker(workerId: string) {
  const queuebit = await createQueuebitClient(queuebitConfig);
  const worker = queuebit.createWorker(
    'notification',
    async ({ data }) => receiptService.send(data),
    { workerId, concurrency: 4 }
  );
  worker.start();

  return {
    worker,
    stop: (options?: { timeoutMs?: number }) => queuebit.close(options)
  };
}
```

这段代码只应由独立的 Worker 服务宿主调用一次，不应在每个 Web 请求中调用：

- `createQueuebitClient(queuebitConfig)` 创建一个长期使用的 client；它与 API 使用相同 Redis、namespace 和队列。
- `createWorker('notification', ...)` 只领取 `notification` 队列中的 job。本示例中该队列只放“发送收据”任务，因此业务函数直接处理 `data`。
- `concurrency: 4` 表示这个 Worker 最多同时处理四个 job；`worker.start()` 才会开始领取工作。
- 在已有服务宿主的关闭钩子中调用 `stop()`。它停止新领取、等待进行中的 handler 收尾、注销 Worker role，最后关闭 client 连接。

import 这些模块不会自动启动进程，也不会偷偷绑定 signal handler。

## 4. 在 Web/API 里提交任务

请求里不要直接慢慢发送收据，只把任务放进队列并返回 `jobId`。client 应在应用启动时创建一次、由路由复用；不要在每个请求后关闭它。

```ts title="你的 Web/API 代码"
import { createQueuebitClient } from 'queuebit';
import { queuebitConfig } from './queuebit.js';

// 应用启动时创建一次；后续所有路由复用这个 client。
const queuebit = await createQueuebitClient(queuebitConfig);

export async function enqueueReceipt(orderId: string, tenantId: string, recipient: string) {
  const job = await queuebit.jobs.add(
    'notification', // 必须是上面已声明的队列。
    'send-receipt', // 给这类 job 的可读名称，便于后续查看和排错。
    { orderId, tenantId, recipient } // Worker 收到的 data。
  );

  return { jobId: job.id, state: job.state };
}
```

`jobs.add()` 很快返回，它只保证 job 已被持久化并等待 Worker 领取，不会在 HTTP 请求中等待收据发送完成。调用方保存 `jobId`，以后可查询状态。

## 5. 确认结果

从同一个应用 client 按 `jobId` 读取 job：

```ts
const current = await queuebit.jobs.get(job.id);
// current?.state 是 waiting、active、completed、failed 或 cancelled。
```

`current` 可能暂时是 `waiting` 或 `active`，也可能在 job 已被清理时为 `null`。看到它变为 `completed`，就说明 API 入队和 Worker 执行这条最小路径已经跑通。到这里为止，你还不需要理解结果回写、防重复、取消、内部调度细节或多进程部署。

当你需要 CLI、多个 Worker/Coordinator 服务、BatchRun 或统一配置治理时，再看[配置 Redis 和 Worker](./configuration-recipes.md)，把共享对象抽成 `queuebit.config.ts`，把 BatchRun 注册抽成 `queuebit.runtime.ts`。

## 下一步

- 给这个任务加重试、超时或去重：[执行一个后台任务](./job-recipes.md)。
- 真的要分页处理很多数据库记录：[批量处理数据库记录](./batch-runs.md)。
- 准备上线：[配置 Redis 和 Worker](./configuration-recipes.md)。
