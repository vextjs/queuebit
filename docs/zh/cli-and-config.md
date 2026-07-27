# CLI 与配置

<!-- queuebit-v01-legacy-doc -->
> [!WARNING]
> **历史文档，已停止维护。** 当前 v0.1 最终用户手册位于 [`docs/v01/zh`](../v01/zh/index.md)。本页仅保留历史上下文，API、命令、配置和示例不得用于新接入或实现。

## 配置入口

<span class="manual-label">用户配置参考</span>

queuebit 默认读取 `queuebit.config.ts`。如果你刚开始接入，只需先设置 Redis、namespace 和 queue name；准备生产时，再按进程角色补齐 Worker、Scheduler、重试、租约和观测配置。

一个配置文件可以表达：

- Redis 连接、namespace 和 queue name。
- Web producer、worker、scheduler 的进程角色。
- producer、worker、scheduler 是否在当前进程启用。
- worker concurrency、lease、drain、retry、delay 等分布式关键参数。
- metrics / introspection 暴露方式。

生产环境中，请显式选择每个进程承担的角色。启用 vext plugin 不会自动把所有 Web worker 变成 queue Worker 或 Scheduler。

## 配置决策树

第一次接入时，使用者应该先选择当前进程角色，再选择字段，而不是从完整配置表里猜。

| 你要做什么 | 选择的进程角色 | 需要配置 | 当前进程不会自动做什么 |
|------------|----------------|--------------|--------------|
| 在 HTTP / API 进程提交 job | Web producer | 创建 `Queue` 或启用 vext producer | Web 进程自动消费 job 或抢占 scheduler |
| 独立消费后台任务 | worker-only | `worker.concurrency`、lease/drain 参数、handler 入口 | worker 自动承担 scheduler |
| 推进 delayed / retry / stalled | scheduler-only | `scheduler.domain` 和 queue 列表 | scheduler 执行业务 handler |
| 本地演示完整链路 | single-process dev | 三个角色都显式开启，并标记为 dev/demo | 把该拓扑写成生产推荐 |

只需先记一条规则：生产环境默认拆成 `web producer -> worker process -> scheduler process`，每个角色都由你显式开启或关闭。

## 从最小到生产配置

首次运行的最小配置：

```ts
import { defineQueuebitConfig } from 'queuebit';

export default defineQueuebitConfig({
  connection: { url: 'redis://127.0.0.1:6379' },
  namespace: 'dev:billing',
  queues: { notification: {} }
});
```

完成 [快速开始](./quick-start.md) 后，再使用下面的生产配置示例：

```ts
import { defineQueuebitConfig } from 'queuebit';

export default defineQueuebitConfig({
  connection: { url: 'redis://127.0.0.1:6379' },
  namespace: 'dev:billing',
  queues: {
    notification: {
      defaultJobOptions: {
        attempts: 3,
        backoff: { type: 'fixed', delayMs: 1000 }
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
  metrics: {
    enabled: true
  }
});
```

single-process dev 可以同时开启 Producer、Worker、Scheduler，适合本地验证；生产环境请使用独立进程，见 [生产部署](./production-deployment.md)。

第一次填写配置时，先按下面顺序取值，不要先猜完整参数表：

| 步骤 | 你要确定什么 | 值从哪里来 | 示例 |
|------|--------------|------------|------|
| 1 | Redis 连接 | 本地 Docker、托管 Redis 控制台或平台连接信息 | `redis://127.0.0.1:6379` |
| 2 | namespace | 环境、应用名、租户边界 | `dev:billing`、`prod:billing` |
| 3 | queue name | 稳定业务动作，不是实例名 | `notification`、`invoice` |
| 4 | default job options | 业务可接受的重试次数和退避策略 | 通知类 `attempts: 3` + exponential backoff |
| 5 | worker 参数 | handler 平均耗时、外部服务限流、机器资源 | `concurrency: 4`、`leaseMs: 30000` |
| 6 | scheduler domain | 同一批 scheduler 候选实例的单活范围 | `billing-notification` |
| 7 | metrics / inspect | 你要如何排查线上卡住的 jobs | 本地开启 inspect，生产按平台暴露 metrics |

## 配置字段

配置结构：

| 字段 | 何时填写 | 用途 | 怎么选 |
|------|----------|------|--------|
| `connection` | 所有角色必填 | Redis 连接信息或已创建连接引用 | 同一 queue 的 producer、worker、scheduler 必须指向同一逻辑 Redis keyspace |
| `connection.url` | 推荐本地或托管连接串 | Redis URL，支持 `redis://` 和需要 TLS 的 `rediss://` | 首次本地用 `redis://127.0.0.1:6379` |
| `connection.host` / `connection.port` / `connection.database` | 不使用 URL 时 | 拆分连接字段 | 适合平台把 host、port、db 分开注入的场景 |
| `connection.username` / `connection.password` | Redis ACL 或托管 Redis 认证 | 认证信息 | 可与 URL 或 host/port 同用；缺失时启动前失败并指出认证失败 |
| `connection.tls` | 托管 Redis 要求 TLS 时 | TLS 开关或 SNI/CA 选项 | `rediss://` 或 `tls: true` 二选一表达清楚即可 |
| `connection.sentinel` | 使用 Sentinel / 自动故障转移时 | master 名称和 sentinel 节点 | 只表示连接层 failover；Redis Cluster 仍 fail fast |
| `namespace` | 所有角色必填 | Redis keyspace 隔离前缀，区分环境、应用和租户 | 建议包含环境和应用，不要用空字符串 |
| `queues.<name>` | 至少一个 | queue name，业务稳定标识 | 使用稳定业务名，不要使用实例 ID |
| `queues.<name>.defaultJobOptions.attempts` | 使用 retry 时建议配置 | 最大尝试次数 | 默认不应无限重试 |
| `queues.<name>.defaultJobOptions.backoff` | 使用 retry 时建议配置 | retry 延迟策略 | 固定、指数或自定义策略必须可解释 |
| `queues.<name>.worker.concurrency` | worker 进程必需或使用默认 | 单 worker 进程内并发处理数 | 从小值开始，先按 handler 幂等性和外部依赖承载能力调整 |
| `queues.<name>.worker.leaseMs` | worker 进程必需或使用默认 | active job lease 时长 | 必须大于续租间隔，且覆盖正常 handler 执行抖动 |
| `queues.<name>.worker.renewIntervalMs` | worker 进程必需或使用默认 | lease 续租间隔 | 必须小于 `leaseMs` |
| `queues.<name>.worker.drainTimeoutMs` | worker 进程必需或使用默认 | graceful drain 等待窗口 | 必须不小于业务可接受的单 job 关闭窗口 |
| `queues.<name>.scheduler.domain` | scheduler 进程必填 | scheduler 单活范围 | 同一 queue 的候选 scheduler 使用同一个 domain |
| `metrics.enabled` | 否 | 是否暴露 metrics/introspection | 本地 introspection 必须可用；网络暴露应显式开启 |

## Redis 连接示例

本地开发：

```ts
connection: { url: 'redis://127.0.0.1:6379' }
```

托管 Redis，要求认证和 TLS：

```ts
connection: {
  url: 'rediss://redis.example.com:6380/0',
  username: 'default',
  password: 'redis-password',
  tls: true
}
```

Sentinel / 自动故障转移：

```ts
connection: {
  sentinel: {
    name: 'mymaster',
    nodes: [
      { host: '10.0.0.11', port: 26379 },
      { host: '10.0.0.12', port: 26379 },
      { host: '10.0.0.13', port: 26379 }
    ],
    username: 'default',
    password: 'redis-password'
  }
}
```

Sentinel failover 期间不要把“Redis 可重新连接”理解成“job 一定不中断”。worker 应停止不确定 claim，scheduler 失去 single-active 资格时停止推进，job 依靠 lease/retry/stalled recovery 恢复。

## 默认与范围策略

下表列出省略字段时采用的默认值、可用范围和配置错误结果：

| 字段 | v0.1 推荐默认 | 范围 / 约束 | 失败行为 |
|------|---------------|-------------|----------|
| `connection` | 无默认 | 必须能连接到目标 Redis | 启动前失败，提示缺少连接信息 |
| `namespace` | 无默认 | 非空字符串，建议包含环境和应用 | 启动前失败，提示 keyspace 隔离风险 |
| `worker.concurrency` | `1` | 正整数 | 小于 1 或非整数启动前失败 |
| `worker.leaseMs` | `30000` | 必须大于 `renewIntervalMs` | 不满足关系时启动前失败 |
| `worker.renewIntervalMs` | `10000` | 必须小于 `leaseMs` | 不满足关系时启动前失败 |
| `worker.drainTimeoutMs` | `30000` | 非负整数 | 非法值启动前失败 |
| `defaultJobOptions.attempts` | `3` | 正整数，不允许无限默认 | 非法值启动前失败 |
| `defaultJobOptions.backoff` | `fixed:1000ms` | 固定、指数或实现声明的自定义策略 | 未知策略启动前失败 |
| `scheduler.domain` | 无默认 | `scheduler.enabled=true` 时非空 | 缺失时启动前失败 |
| `metrics.enabled` | `false` | 布尔值 | 非布尔值启动前失败 |

## 配置错误策略

配置无效时，queuebit 会尽量在进程开始领取 job 之前报错：

| 错误类型 | 你会看到的处理 |
|----------|--------------|
| 缺少 `connection` / `namespace` / `queues` | fail fast，并指出缺少字段和当前进程角色 |
| Redis 认证失败或 TLS 配置错误 | fail fast，指出是认证、TLS 握手或证书/hostname 问题 |
| Sentinel 无法发现 master | fail fast 或进入不可用状态，不允许 scheduler 冒险推进 |
| worker 命令启动但 lease 参数关系错误 | fail fast，说明 `renewIntervalMs` 必须小于 `leaseMs` |
| scheduler 命令启动但缺少 `scheduler.domain` | fail fast，避免多个 scheduler 使用隐式 domain |
| Redis Cluster 未支持但检测到 cluster 配置 | fail fast，提示查看 [运行环境与兼容边界](./compatibility.md) |
| single-process dev 出现在生产环境 | 至少 warning；若环境标记为 production，应 fail fast 或要求显式 override |
| adapter 自动推导拓扑 | 禁止静默推导；必须输出最终 queuebit 配置摘要供 inspect |

## CLI 命令

CLI 覆盖 worker、scheduler 和 inspect：

| 命令 | 语义 |
|------|------|
| `npx queuebit worker start --config queuebit.config.ts --queue notification` | 启动独立 worker runtime |
| `npx queuebit worker drain --config queuebit.config.ts --queue notification --timeout 30s` | 请求 worker 停止拉新并等待完成 |
| `npx queuebit scheduler start --config queuebit.config.ts --domain billing-notification` | 启动独立 scheduler runtime |
| `npx queuebit inspect queue notification --config queuebit.config.ts` | 查看 queue depth、active、delayed、retry、stalled |
| `npx queuebit inspect workers --queue notification --config queuebit.config.ts` | 查看 worker identity、heartbeat、drain 状态 |
| `npx queuebit inspect scheduler --domain billing-notification --config queuebit.config.ts` | 查看 active scheduler identity 与 domain 状态 |

这些动作也可通过 core API 或 vext adapter 完成；CLI 适合独立进程和运维操作。

## 进程入口

推荐进程拆分：

```text
web process      -> producer only
worker process   -> worker runtime
scheduler process -> scheduler runtime
```

可接受的开发模式：

| 模式 | 用途 | 限制 |
|------|------|------|
| single-process dev | 本地演示、快速验证 | 必须显式声明，不代表生产推荐 |
| worker-only | 后台消费 | 不能隐式承担 scheduler，除非配置启用 |
| scheduler-only | 时间推进 | 不能处理业务 job |
| web producer | HTTP/API 提交 job | 不默认消费 job |

## vext 配置关系

vext adapter 会把 vext 配置映射为 queuebit 配置。接入时注意：

- vext app 启动不等于 queue worker 启动。
- vext cluster worker 数量不等于 queue worker concurrency。
- vext reload 时需要选择 Worker drain 或显式 stop。
- `scheduler.domain` 仍由你配置，并可通过 inspect 查看当前 active Scheduler。

## 下一步

- 最短接入示例见 [快速开始](./quick-start.md)。
- vext 配置示例见 [vext 接入](./vext-integration.md)。
- 运维指标和排查顺序见 [运维与排查](./operations.md)。
