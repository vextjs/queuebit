# 快速开始

## 目标

<span class="manual-label">v0.1 final user manual</span>

本页是 queuebit v0.1 的第一次接入路径。按这里操作后，你会从业务数据源取出一批待处理对象，批量提交 jobs，由 worker 消费，由 scheduler 推进延迟/重试/恢复，并能查看队列状态。

如果你使用 vext，先看本页理解 queuebit 三角色，再进入 [vext 接入](./vext-integration.md)。

## 最短跑通路径

第一次不要先研究所有配置，先按这个顺序跑通：

```text
1. 启动 Redis
2. 写 queuebit.config.ts
3. 从业务数据库/API/事件中取出待处理数据
4. 把数据整理成 job payload，并批量提交 Queue.addBulk
5. 在第二个进程启动 Worker
6. 在第三个进程启动 Scheduler
7. 用 inspect 看 waiting / active / delayed / failed
```

```mermaid
flowchart TD
  Start["准备 Redis"] --> Config["写 queuebit.config.ts"]
  Config --> Source["读取业务数据源<br/>DB / API / event"]
  Source --> Payload["生成 job payload<br/>稳定 ID + 渲染参数"]
  Payload --> Producer["批量提交 jobs<br/>Queue.addBulk"]
  Producer --> Worker["启动 Worker<br/>逐个执行 handler"]
  Producer --> Scheduler["启动 Scheduler<br/>推进 delayed / retry / stalled"]
  Worker --> Inspect["inspect queue / workers / scheduler"]
  Scheduler --> Inspect
  Inspect --> Done{"这一批 jobs 是否 completed?"}
  Done -- "是" --> Success["第一次成功"]
  Done -- "否" --> Triage["按常见错误表排查"]
```

节点说明：

| 节点 | 判断标准 |
|------|----------|
| 准备 Redis | `redis://127.0.0.1:6379` 可连接 |
| 写配置 | `connection`、`namespace`、`queues.notification.scheduler.domain` 已填写 |
| 读取业务数据源 | 能从你的业务数据库、API、事件或文件中拿到待处理对象 |
| 生成 payload | 每个 job 有稳定业务 ID、接收人、通知模板和幂等键 |
| 批量提交 jobs | `Queue.addBulk` 返回这一批 job ids |
| 启动 Worker | `inspect workers` 能看到 worker identity |
| 启动 Scheduler | `inspect scheduler` 能看到 active scheduler |
| inspect | 能区分这一批 jobs 是 waiting、active、delayed、retrying 还是 failed |

## 前置条件

| 项 | 要求 | 说明 |
|----|------|------|
| Node.js | `>= 20` | 使用当前 LTS 线，便于稳定支持 ESM、AbortSignal 和现代 TypeScript 工具链 |
| Redis | `>= 7.0` standalone 或托管单主 Redis | v0.1 只接入 Redis；Redis Cluster 默认不支持 |
| TypeScript | 推荐 `>= 5.4` | JavaScript 也可用；TypeScript 项目获得完整类型提示 |
| 进程拓扑 | Web/API、worker、scheduler 显式拆分 | 本地演示可以单机多终端，生产不建议 Web 隐式承担 worker |

## 安装

```bash
npm install queuebit
```

准备一个本地 Redis：

```bash
docker run --name queuebit-redis -p 6379:6379 redis:7
```

新建最小配置文件 `queuebit.config.ts`：

```ts
import { defineQueuebitConfig } from 'queuebit';

export default defineQueuebitConfig({
  connection: { url: 'redis://127.0.0.1:6379' },
  namespace: 'dev:billing',
  queues: {
    notification: {
      defaultJobOptions: {
        attempts: 3,
        backoff: { type: 'exponential', delayMs: 1000 }
      },
      worker: {
        concurrency: 4,
        leaseMs: 30000,
        renewIntervalMs: 10000,
        drainTimeoutMs: 30000
      },
      scheduler: {
        domain: 'billing-notification'
      }
    }
  },
  metrics: { enabled: true }
});
```

配置字段怎么理解：

| 字段 | 你从哪里取值 | 第一次怎么选 |
|------|--------------|--------------|
| `connection.url` | Redis 连接地址 | 本地用 `redis://127.0.0.1:6379`，生产用你的托管 Redis 地址 |
| `namespace` | 环境 + 应用名 | 用 `dev:billing`、`prod:billing` 这类稳定前缀隔离 keyspace |
| `queues.notification` | 业务队列名 | 按业务动作命名，例如 `notification`、`email`、`invoice` |
| `attempts` | 业务允许重试次数 | 通知/邮件类通常先用 `3`，不要默认无限重试 |
| `backoff` | 下游服务恢复速度 | 下游偶发失败用指数退避；固定间隔适合本地演示 |
| `worker.concurrency` | handler 和下游承载能力 | 先从 `4` 或更小开始，观察外部服务限流后再调大 |
| `leaseMs` / `renewIntervalMs` | 单个 job 正常耗时 | `renewIntervalMs` 必须小于 `leaseMs`；长任务要调大 lease 或拆小任务 |
| `scheduler.domain` | 同一组 scheduler 的单活范围 | 同一业务域使用同一个稳定 domain，避免多个 scheduler 同时推进 |

## 从业务数据源批量提交 jobs

queuebit 不会凭空获取用户数据，也不应该把示例写成硬编码单用户。真实流程是：你的业务系统先查询待处理数据，再把每条数据整理成一个 job payload。

下面以“给已支付订单批量发送通知”为例。数据来源可以是你的数据库、内部 API、事件流或导入文件；queuebit 只负责把这些业务数据异步排队和可靠执行。

```ts
import { Queue } from 'queuebit';
import { db } from './db';

const notificationQueue = new Queue('notification', {
  connection: { url: 'redis://127.0.0.1:6379' },
  namespace: 'dev:billing'
});

type ReceiptNotificationJob = {
  orderId: string;
  userId: string;
  channel: 'email' | 'push';
  recipient: string;
  templateId: 'receipt-paid';
  variables: {
    orderNo: string;
    amountText: string;
  };
};

const pendingReceipts = await db.orders.findMany({
  where: {
    paid: true,
    receiptNotificationQueuedAt: null
  },
  include: {
    user: {
      select: {
        id: true,
        email: true,
        pushToken: true,
        preferredChannel: true
      }
    }
  },
  take: 100
});

const jobs = pendingReceipts.flatMap((order) => {
  const wantsPush = order.user.preferredChannel === 'push' && Boolean(order.user.pushToken);
  const channel = wantsPush ? 'push' : 'email';
  const recipient = wantsPush ? order.user.pushToken : order.user.email;

  if (!recipient) {
    // 没有可用接收地址时不要凭空构造 job，先让业务数据回到待补全状态。
    return [];
  }

  return [{
    name: 'send-receipt-notification',
    data: {
      orderId: order.id,
      userId: order.user.id,
      channel,
      recipient,
      templateId: 'receipt-paid',
      variables: {
        orderNo: order.orderNo,
        amountText: order.amountText
      }
    } satisfies ReceiptNotificationJob,
    opts: {
      idempotencyKey: `receipt:${order.id}`,
      attempts: 3,
      backoff: { type: 'exponential', delayMs: 1000 }
    }
  }];
});

if (jobs.length > 0) {
  const createdJobs = await notificationQueue.addBulk(jobs);
  const queuedOrderIds = jobs.map((job) => job.data.orderId);

  await db.orders.updateMany({
    where: { id: { in: queuedOrderIds } },
    data: { receiptNotificationQueuedAt: new Date() }
  });

  console.log(createdJobs.map((job) => job.id));
}
```

这里有三个关键点：

| 问题 | 正确做法 |
|------|----------|
| 用户数据从哪里来 | 从你的业务 DB/API/event/file 来；queuebit 只接收你提交的 payload |
| 是否只能提交一个 job | 不是。队列主路径应支持批量提交，`addBulk` 用来一次提交一批 jobs |
| 推送信息从哪里来 | 从用户资料、通知偏好、设备 token、模板系统或业务订单中取值；缺失时不要造假，先跳过或回写待补全状态 |
| payload 应放什么 | 放稳定 ID、接收人、模板 ID 和必要变量；不要把不可控的大对象随便塞进 Redis |

如果某些通知要延迟发送，可以在批量 jobs 中给对应项设置 `delayMs`：

```ts
const delayedJobs = pendingReceipts.flatMap((order) => {
  if (!order.user.email) {
    return [];
  }

  return [{
    name: 'send-receipt-reminder',
    data: {
      orderId: order.id,
      userId: order.user.id,
      channel: 'email',
      recipient: order.user.email,
      templateId: 'receipt-paid',
      variables: {
        orderNo: order.orderNo,
        amountText: order.amountText
      }
    },
    opts: {
      idempotencyKey: `receipt-reminder:${order.id}`,
      delayMs: 15 * 60 * 1000
    }
  }];
});

if (delayedJobs.length > 0) {
  await notificationQueue.addBulk(delayedJobs);
}
```

## 启动 Worker

Worker 应该作为独立进程运行。它负责 claim job、续租、执行 handler、ack/fail 和 drain。

生产项目建议把 `ReceiptNotificationJob` 这类 payload 类型放在 `src/jobs/receipt-notification.ts`，producer 和 worker 共同引用，避免两边字段漂移。

```ts
import { Worker } from 'queuebit';

const worker = new Worker(
  'notification',
  async (job) => {
    if (job.name !== 'send-receipt-notification') {
      throw new Error(`Unknown job: ${job.name}`);
    }

    const data = job.data as ReceiptNotificationJob;

    const user = await db.users.findUnique({ where: { id: data.userId } });
    const order = await db.orders.findUnique({ where: { id: data.orderId } });

    if (!user || !order) {
      throw new Error(`Missing user or order for job ${job.id}`);
    }

    const message = await renderNotificationTemplate(data.templateId, {
      ...data.variables,
      userName: user.name
    });

    if (data.channel === 'email') {
      await emailProvider.send({
        to: data.recipient,
        subject: message.subject,
        html: message.html
      });
      return;
    }

    await pushProvider.send({
      token: data.recipient,
      title: message.title,
      body: message.body
    });
  },
  {
    connection: { url: 'redis://127.0.0.1:6379' },
    namespace: 'dev:billing',
    concurrency: 4,
    leaseMs: 30000,
    renewIntervalMs: 10000,
    drainTimeoutMs: 30000
  }
);

await worker.run();
```

CLI 入口等价写法：

```bash
queuebit worker start --config queuebit.config.ts --queue notification
```

## 启动 Scheduler

Scheduler 推进 delayed、retry 和 stalled recovery。生产环境建议独立运行 scheduler 进程；同一 `scheduler.domain` 可以有多个候选实例，但同一时刻只能一个 active。

```ts
import { Scheduler } from 'queuebit';

const scheduler = new Scheduler({
  connection: { url: 'redis://127.0.0.1:6379' },
  namespace: 'dev:billing',
  queues: ['notification'],
  domain: 'billing-notification'
});

await scheduler.run();
```

CLI 入口等价写法：

```bash
queuebit scheduler start --config queuebit.config.ts --domain billing-notification
```

## 查看队列状态

```bash
queuebit inspect queue notification --config queuebit.config.ts
queuebit inspect workers --queue notification --config queuebit.config.ts
queuebit inspect scheduler --domain billing-notification --config queuebit.config.ts
```

最小输出需要让用户看到：

| 指标 | 用途 |
|------|------|
| `waiting` | 有多少 job 等待 worker claim |
| `active` | 有多少 job 正在处理 |
| `delayed` | 有多少 job 等待 scheduler 到期推进 |
| `retrying` | 有多少 job 失败后等待下一次 attempt |
| `failed` | 有多少 job 已终止失败 |
| `stalledRecoveries` | 近期发生了多少次 stalled recovery |
| `activeWorkers` | 哪些 worker 正在心跳 |
| `activeScheduler` | 当前 active scheduler identity |

## 优雅关闭

Worker 收到关闭信号时应停止拉新，并等待 active jobs 在窗口内完成。

```ts
async function shutdown() {
  await worker.close({ drain: true, timeoutMs: 30000 });
  await scheduler.close();
  await notificationQueue.close();
}

process.once('SIGTERM', shutdown);
process.once('SIGINT', shutdown);
```

Drain 超时不代表 job 成功；未完成 job 会按 lease / stalled recovery 规则恢复。

## 生产拓扑

```text
web/api process      -> Queue.addBulk(...)
worker process       -> Worker.run()
scheduler process    -> Scheduler.run()
redis                -> queue state, leases, delayed, retry, recovery
```

| 角色 | 推荐数量 | 注意事项 |
|------|----------|----------|
| Web/API producer | 按业务服务水平扩缩容 | 不隐式启动 worker 或 scheduler |
| Worker | 按吞吐与外部依赖能力扩缩容 | `concurrency` 从小值开始，handler 必须幂等 |
| Scheduler | 可多候选，单 domain 仅一个 active | 不处理业务 handler |
| Redis | 单主 Redis 或托管单主 Redis | Redis Cluster v0.1 默认 fail fast |

## 常见错误

第一次失败时，不要直接看 Redis key。先按这个顺序判断：

1. `queuebit inspect queue notification --config queuebit.config.ts`：确认 jobs 在哪里。
2. `queuebit inspect workers --queue notification --config queuebit.config.ts`：确认有没有 worker。
3. `queuebit inspect scheduler --domain billing-notification --config queuebit.config.ts`：确认 delayed / retry 谁在推进。
4. 再看 handler 日志和业务幂等。

| 现象 | 常见原因 | 处理 |
|------|----------|------|
| job 一直 waiting | worker 未启动、queue/namespace 不一致、worker 正在 drain | 运行 `queuebit inspect workers`，核对配置 |
| delayed 到期后没执行 | scheduler 未启动或 domain 不一致 | 运行 `queuebit inspect scheduler` |
| job 重复执行 | at-least-once 下 ack 丢失、worker crash 或 lease 过期 | 用 `idempotencyKey` 和业务幂等保护 |
| worker 启动即失败 | Redis 不可连接、lease 参数不合法、Redis Cluster 未支持 | 先修配置，参考 [CLI 与配置](./cli-and-config.md) |
| drain 后仍有 active | handler 太慢、外部依赖卡住、drainTimeout 太短 | 拆小任务或调整 timeout / lease |

## 下一步

- 接入 vext 项目：继续读 [vext 接入](./vext-integration.md)。
- 调整配置：继续读 [CLI 与配置](./cli-and-config.md)。
- 理解 Redis 与分布式恢复：继续读 [Redis-only 与分布式恢复](./distributed-semantics.md)。
- 排查线上问题：继续读 [运维与排查](./operations.md)。
