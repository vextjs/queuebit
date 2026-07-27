# API 参考

<!-- queuebit-v01-legacy-doc -->
> [!WARNING]
> **历史文档，已停止维护。** 当前 v0.1 最终用户手册位于 [`docs/v01/zh`](../v01/zh/index.md)。本页仅保留历史上下文，API、命令、配置和示例不得用于新接入或实现。

## 如何使用本页

<span class="manual-label">公共 API 参考</span>

本页用于查询 queuebit v0.1 的公开对象、输入、返回结果、状态和错误。第一次接入先完成 [快速开始](./quick-start.md)，部署时再看 [生产部署](./production-deployment.md)。

## 使用约定

- 使用稳定的 namespace 和 queue name，让 Producer、Worker、Scheduler 指向同一逻辑队列。
- Web/API 进程创建 `Queue`；独立进程运行 `Worker` 和 `Scheduler`。
- Handler 按 at-least-once 设计，因为崩溃或 ack 不确定可能带来重复投递。
- 应用关闭时调用 `close`；发布 Worker 时先 drain。
- 普通接入不需要读取或修改 Redis key。

## 核心对象

| 对象 | 用来做什么 | 何时关闭 |
|------|------------|----------|
| `Queue` | queue 的用户入口，提交 job、读取状态、关闭连接 | 应用生命周期内复用，关闭时释放 Redis 连接或引用 |
| `Producer` | 提交 job，返回 job handle | 可短生命周期，也可随应用常驻 |
| `Worker` | 处理 job，续租，ack/fail，drain | 长生命周期，必须可 graceful shutdown |
| `Scheduler` | 推进 delayed、retry、stalled recovery | 长生命周期，必须可确认 single-active |
| `Job` | job 元数据、payload、attempt、状态、错误摘要 | 由 queuebit 持久化并可查询 |

公开类型轮廓：

```ts
type QueuebitTlsOptions = {
  servername?: string;
  ca?: string | string[];
};

type QueuebitSentinelNode = {
  host: string;
  port: number;
};

type QueuebitConnection = {
  url?: string;
  host?: string;
  port?: number;
  database?: number;
  username?: string;
  password?: string;
  tls?: boolean | QueuebitTlsOptions;
  sentinel?: {
    name: string;
    nodes: QueuebitSentinelNode[];
    username?: string;
    password?: string;
    tls?: boolean | QueuebitTlsOptions;
  };
};

type QueueOptions = {
  connection: QueuebitConnection;
  namespace: string;
};

type QueuebitJobState =
  | 'waiting'
  | 'delayed'
  | 'active'
  | 'retrying'
  | 'completed'
  | 'failed'
  | 'stalled'
  | 'draining';

type JobInput<TPayload> = {
  name: string;
  data: TPayload;
  opts?: {
    idempotencyKey?: string;
    delayMs?: number;
    attempts?: number;
    backoff?: { type: 'fixed' | 'exponential'; delayMs: number };
    timeoutMs?: number;
  };
};

type BulkAddResult<TPayload> = {
  inputIndex: number;
  job: Job<TPayload>;
  created: boolean;
};

type HandlerContext = {
  signal: AbortSignal;
  attempt: number;
};
```

## Redis 连接模型

`QueuebitConnection` 支持本地 Redis、托管 Redis、ACL/password、TLS 和 Sentinel 连接层 failover。队列只面向单主 Redis；Redis Cluster v0.1 不支持。

| 写法 | 适用场景 | 示例 |
|------|----------|------|
| `url` | 本地或托管 Redis 提供完整连接串 | `redis://127.0.0.1:6379`、`rediss://user:pass@redis.example.com:6380/0` |
| `host` / `port` / `database` | 平台把 host、port、db 拆成字段 | `{ host: 'redis.example.com', port: 6379, database: 0 }` |
| `username` / `password` | Redis ACL、托管 Redis 密码 | 与 `url` 或 `host/port` 同时使用 |
| `tls` | 托管 Redis 要求 TLS | `tls: true` 或 `tls: { servername: 'redis.example.com' }` |
| `sentinel` | Sentinel / 自动故障转移 | 填 master `name` 和 sentinel `nodes` |

Sentinel 只表示连接层可以重新发现主节点。failover 期间 worker 应停止拉新，scheduler 失去单活资格时停止推进，job 通过 lease/retry/stalled recovery 恢复。

## 最小示例

Producer：

`pendingReceipts` 应来自你的业务数据库、API、事件或导入文件；queuebit API 只接收整理后的 job payload。

```ts
import { Queue } from 'queuebit';

const queue = new Queue('notification', {
  connection: { url: 'redis://127.0.0.1:6379' },
  namespace: 'dev:billing'
});

const jobs = pendingReceipts.map((order) => ({
  name: 'send-receipt-notification',
  data: {
    orderId: order.id,
    userId: order.user.id,
    recipient: order.user.email,
    templateId: 'receipt-paid'
  },
  opts: {
    idempotencyKey: `receipt:${order.id}`,
    attempts: 3,
    backoff: { type: 'exponential', delayMs: 1000 }
  }
}));

await queue.addBulk(jobs);
```

Worker：

```ts
import { Worker } from 'queuebit';

const worker = new Worker('notification', async (job) => {
  await sendReceiptNotification(job.data);
}, {
  connection: { url: 'redis://127.0.0.1:6379' },
  namespace: 'dev:billing',
  concurrency: 4
});

await worker.run();
```

Scheduler：

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

## Queue API

| 调用 | 返回/结果 | 失败与注意事项 |
|------|-----------|----------------|
| `new Queue(name, options)` | 绑定 Redis、namespace 和稳定 queue name | 连接或 namespace 无效时创建失败 |
| `queue.add(name, data, opts?)` | 返回一个可追踪 job | 适合确实只有一条业务对象的场景 |
| `queue.addBulk(jobs)` | 按输入顺序返回 `BulkAddResult[]`；业务批处理优先使用 | 先校验整批，再原子写入新 job；任一无效项或冲突都会拒绝整批 |
| `queue.getJob(id)` | 返回 `Job` 元数据和当前状态 | job 不存在时返回 `null` |
| `queue.inspect()` | 返回 waiting、active、delayed、retry、failed、stalled 概况 | 只读，不推进状态 |
| `queue.close()` | 释放当前 Queue 使用的资源 | 关闭后不再提交新 job |

`addBulk` 只有一套确定语义：

| 情况 | 结果 |
|------|------|
| 所有输入都是新的有效 job | 原子创建整批，并按输入顺序返回 `created: true` |
| 某个 key 已存在，且 job 内容相同 | 复用已有 job，该项返回 `created: false` |
| 同一个 key 在本次输入批次内重复 | 写入前拒绝整批 |
| 已有 key 对应不同 job 内容 | 以幂等冲突拒绝整批 |
| 任一输入无效或 Redis 无法提交 | 拒绝整批；调用方不得假设已有部分 job 入队 |

## Worker API

| 调用 | 返回/结果 | 失败与注意事项 |
|------|-----------|----------------|
| `new Worker(name, handler, options)` | 注册 `(job, ctx: HandlerContext) => Promise<void>` 和 Worker 参数 | queue/namespace 必须与 Producer 一致；将 `ctx.signal` 传给支持协作取消的下游 API |
| `worker.run()` | 开始领取并处理 jobs，持续运行直到关闭 | Handler 抛错时按 attempts/backoff 进入 retry 或 failed |
| `worker.close({ drain, timeoutMs })` | 停止拉新；`drain: true` 时等待 active jobs | 超时不代表 job 成功，未完成 job 交给恢复路径 |
| Worker 事件/日志 | 暴露 completed、failed、retry、stalled 和连接错误 | ack 或 lease 不确定时可能再次投递 |

`opts.timeoutMs` 到期后，queuebit 会中止 `ctx.signal` 并记录 `HandlerTimeoutError`；该 attempt 随后按重试策略进入 retry，最终可能成为 `failed`。JavaScript 无法强制终止忽略 signal 的副作用，因此 handler 必须把 `ctx.signal` 传给支持取消的客户端，并保持业务幂等。

Worker 事件名称固定为 `completed`、`failed`、`retrying`、`stalled`、`error`。监听器自身抛错只会记录为 `error` 事件/日志，不会改变 job 状态。

v0.1 指标采用拉取方式：调用 `queue.inspect()` 或读取 CLI JSON 输出。queuebit 不内置 Dashboard，也不内置 Prometheus HTTP Server。

## Scheduler API

| 调用 | 返回/结果 | 失败与注意事项 |
|------|-----------|----------------|
| `new Scheduler(options)` | 绑定 queue 列表和 `domain` | 同一候选组使用同一个稳定 domain |
| `scheduler.run()` | active 实例推进 delayed、retry 和 stalled recovery | 未获得或失去 single-active 资格时不推进 |
| `scheduler.close()` | 停止心跳和时间推进 | 关闭后 delayed/retry 暂停，直到其他候选接管 |

## Job 状态

用户可见状态流转：

```mermaid
flowchart LR
  P["producer enqueue"] --> W["waiting"]
  P --> D["delayed"]
  D --> W
  W --> A["active"]
  A --> C["completed"]
  A --> R["retrying"]
  R --> W
  A --> F["failed"]
  A --> S["stalled"]
  S --> W
```

节点说明：

| 节点 | 说明 |
|------|------|
| `waiting` | job 可被 worker 声明 |
| `delayed` | job 未到可执行时间，由 scheduler 推进 |
| `active` | job 已被 worker 声明并持有 lease |
| `retrying` | job 失败后等待下一次 attempt |
| `completed` | job 已被确认完成 |
| `failed` | job 达到终止失败条件 |
| `stalled` | active job 的 worker/lease 不确定，等待恢复 |

## 错误与事件

调用方需要处理以下错误类别：

| 类型 | 你会看到的示例 | 用户动作 |
|------|----------|----------|
| 配置错误 | 缺少 namespace、queue name、Redis 配置无效 | 启动前失败，修正配置 |
| Redis 不可用 | 连接失败、命令超时、脚本失败 | 停止声明新 job，等待恢复或人工介入 |
| Lease 不确定 | 续租失败、token 不匹配、TTL 丢失 | worker 停止拉新，job 交给恢复路径 |
| Handler 失败 | 业务异常、超时、显式失败 | 按 retry/terminal failure 处理 |
| Scheduler 不确定 | 无法确认单活资格 | 停止 delayed/retry 推进 |

## 参考关联

| 问题 | 入口 |
|------|------|
| 完整第一次接入 | [快速开始](./quick-start.md) |
| 配置字段和 CLI | [CLI 与配置](./cli-and-config.md) |
| vext adapter API | [vext 接入](./vext-integration.md) |
| Redis keyspace 与状态迁移 | [Redis 模型](./redis-model.md) |
| Worker / scheduler 生命周期 | [Worker 与 Scheduler 生命周期](./worker-lifecycle.md) |
