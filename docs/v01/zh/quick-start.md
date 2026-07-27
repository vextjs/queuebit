# 快速开始：执行一个后台任务

<span class="manual-label">快速开始 · 先跑通最小接入</span>

本页只让你先跑通第一个后台任务：安装 Queuebit，配置 Redis，定义一个 processor，从 Web/API 调用 `jobs.add()`，启动 Worker，并确认 job 完成。

先不要理解所有能力。第一次接入只需要这张表左侧：

| 现在需要知道 | 先不用管 |
|---|---|
| Redis 连接、queue 名、processor 名、`jobs.add()`、Worker 启动命令 | 结果回写、防重复、自动重试、延时执行、批量数据库处理、多进程部署、框架接入 |

## 1. 安装 Queuebit

```bash
npm install queuebit
docker run --name queuebit-redis -p 6379:6379 -d redis:7.2
```

## 2. 配置 Redis 和队列

告诉 Queuebit Redis 在哪里，以及本项目有哪些队列。这里先只建一个 `notification` 队列。

```ts title="queuebit.config.ts"
import { defineQueuebitConfig } from 'queuebit';

export default defineQueuebitConfig({
  connection: { url: process.env.QUEUEBIT_REDIS_URL ?? 'redis://127.0.0.1:6379/0' },
  namespace: 'receipt-demo',
  queues: {
    notification: {}
  }
});
```

## 3. 定义要执行的任务

processor 就是 Worker 真正执行的业务函数。先只接收 `data`，把事情做完即可；失败处理、超时和防重复后面再加。

```ts title="queuebit.runtime.ts"
import { defineQueuebitRuntime, defineQueuebitProcessor } from 'queuebit';

export default defineQueuebitRuntime({
  processors: {
    'send-receipt': defineQueuebitProcessor(async ({ data }) => {
      await receiptService.send(data);
    })
  }
});
```

## 4. 在 Web/API 里提交任务

请求里不要直接慢慢发送收据，只把任务放进队列并返回 `jobId`。

```ts title="你的 Web/API 代码"
import config from './queuebit.config.js';
import { createQueuebitClient } from 'queuebit';

const queuebit = await createQueuebitClient({ config });

const job = await queuebit.jobs.add(
  'notification',
  'send-receipt',
  { orderId, tenantId, recipient }
);

return { jobId: job.id, state: job.state };
```

## 5. 启动 Worker 并确认结果

Worker 会读取 `queuebit.runtime.ts`，找到名为 `send-receipt` 的 processor，然后执行刚才提交的 job。

```bash title="后台进程"
npx queuebit worker start --config queuebit.config.ts --runtime queuebit.runtime.ts --queue notification
```

```bash
npx queuebit job inspect <jobId> --config queuebit.config.ts
```

看到 job 从 waiting/active 进入 completed，就说明最小接入跑通了。到这里为止，你还不需要理解结果回写、防重复、取消、内部调度细节或多进程部署。

## 下一步

- 给这个任务加重试、超时或去重：[执行一个后台任务](./job-recipes.md)。
- 真的要分页处理很多数据库记录：[批量处理数据库记录](./batch-runs.md)。
- 准备上线：[配置 Redis 和 Worker](./configuration-recipes.md)。
