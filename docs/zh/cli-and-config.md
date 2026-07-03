# CLI 与配置

## 配置入口

<span class="manual-label">v0.1 final user manual</span>

queuebit 使用 `queuebit.config.ts` 作为默认配置文件。配置必须让使用者明确表达：

- Redis 连接、namespace 和 queue name。
- Web producer、worker、scheduler 的进程角色。
- producer、worker、scheduler 是否在当前进程启用。
- worker concurrency、lease、drain、retry、delay 等分布式关键参数。
- metrics / introspection 暴露方式。

配置不得隐藏关键拓扑。例如，不能因为应用启用了 vext plugin，就默认让所有 Web worker 都成为 queue worker 和 scheduler。

## 配置决策树

第一次接入时，使用者应该先选择当前进程角色，再选择字段，而不是从完整配置表里猜。

| 你要做什么 | 选择的进程角色 | 必须显式配置 | 禁止隐式发生 |
|------------|----------------|--------------|--------------|
| 在 HTTP / API 进程提交 job | Web producer | 创建 `Queue` 或启用 vext producer | Web 进程自动消费 job 或抢占 scheduler |
| 独立消费后台任务 | worker-only | `worker.concurrency`、lease/drain 参数、handler 入口 | worker 自动承担 scheduler |
| 推进 delayed / retry / stalled | scheduler-only | `scheduler.domain` 和 queue 列表 | scheduler 执行业务 handler |
| 本地演示完整链路 | single-process dev | 三个角色都显式开启，并标记为 dev/demo | 把该拓扑写成生产推荐 |

如果用户只记一条规则：生产环境默认拆成 `web producer -> worker process -> scheduler process`，每个角色都必须显式开启或关闭。

## 最小配置

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

single-process dev 可以同时开启 producer、worker、scheduler，但必须只用于本地演示或最小验证。生产文档和 adapter 文档不得把它放在默认路径。

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

| 字段 | 必填条件 | 目标语义 | 用户选择建议 |
|------|----------|----------|--------------|
| `connection` | 所有角色必填 | Redis 连接信息或已创建连接引用 | 同一 queue 的 producer、worker、scheduler 必须指向同一逻辑 Redis keyspace |
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

## 默认与范围策略

每个字段都必须有默认、范围和失败行为。

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

配置错误必须在启动前尽早暴露，不能等 worker claim job 后才失败。

| 错误类型 | 目标错误策略 |
|----------|--------------|
| 缺少 `connection` / `namespace` / `queues` | fail fast，并指出缺少字段和当前进程角色 |
| worker 命令启动但 lease 参数关系错误 | fail fast，说明 `renewIntervalMs` 必须小于 `leaseMs` |
| scheduler 命令启动但缺少 `scheduler.domain` | fail fast，避免多个 scheduler 使用隐式 domain |
| Redis Cluster 未支持但检测到 cluster 配置 | fail fast，提示查看 [运行环境与兼容边界](./compatibility.md) |
| single-process dev 出现在生产环境 | 至少 warning；若环境标记为 production，应 fail fast 或要求显式 override |
| adapter 自动推导拓扑 | 禁止静默推导；必须输出最终 queuebit 配置摘要供 inspect |

## CLI 命令

CLI 覆盖 worker、scheduler 和 inspect：

| 命令 | 语义 |
|------|------|
| `queuebit worker start --config queuebit.config.ts --queue notification` | 启动独立 worker runtime |
| `queuebit worker drain --config queuebit.config.ts --queue notification --timeout 30s` | 请求 worker 停止拉新并等待完成 |
| `queuebit scheduler start --config queuebit.config.ts --domain billing-notification` | 启动独立 scheduler runtime |
| `queuebit inspect queue notification --config queuebit.config.ts` | 查看 queue depth、active、delayed、retry、stalled |
| `queuebit inspect workers --queue notification --config queuebit.config.ts` | 查看 worker identity、heartbeat、drain 状态 |
| `queuebit inspect scheduler --domain billing-notification --config queuebit.config.ts` | 查看 active scheduler identity 与 domain 状态 |

CLI 不能成为唯一入口。core API 和 vext adapter 也必须能表达同样的生命周期语义。

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

vext adapter 可以读取 vext 配置并生成 queuebit 配置，但必须保持以下边界：

- vext app 启动不等于 queue worker 启动。
- vext cluster worker 数量不等于 queue worker concurrency。
- vext reload 必须触发 worker drain 或显式 stop。
- adapter 不能隐藏 scheduler domain；单活策略必须可观测。

## 下一步

- 最短接入示例见 [快速开始](./quick-start.md)。
- vext 配置示例见 [vext 接入](./vext-integration.md)。
- 运维指标和排查顺序见 [运维与排查](./operations.md)。
