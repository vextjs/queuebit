# 配置场景与配方

<!-- queuebit-v01-legacy-doc -->
> [!WARNING]
> **历史文档，已停止维护。** 当前 v0.1 最终用户手册位于 [`docs/v01/zh`](../v01/zh/index.md)。本页仅保留历史上下文，API、命令、配置和示例不得用于新接入或实现。

<span class="manual-label">按环境选择配置</span>

不要从完整配置表开始猜参数。先选择最接近的部署场景，再用 [CLI 与配置](./cli-and-config.md) 查询单个字段。

## 最小本地开发

```ts
import { defineQueuebitConfig } from 'queuebit';

export default defineQueuebitConfig({
  connection: { url: 'redis://127.0.0.1:6379' },
  namespace: 'dev:billing',
  queues: {
    notification: {
      defaultJobOptions: { attempts: 3 },
      worker: { concurrency: 2 },
      scheduler: { domain: 'billing-notification-dev' }
    }
  }
});
```

本地也要启动独立 Worker 和 Scheduler，这样开发路径不会掩盖生产进程边界。`namespace` 必须包含环境，避免共享 Redis 时读到其他项目的 job。

## 托管 Redis + TLS

```ts
export default defineQueuebitConfig({
  connection: {
    url: 'rediss://redis.example.com:6380/0',
    username: 'default',
    password: 'redis-password',
    tls: { servername: 'redis.example.com' }
  },
  namespace: 'prod:billing',
  queues: {
    notification: {
      defaultJobOptions: {
        attempts: 4,
        backoff: { type: 'exponential', delayMs: 1000 },
        timeoutMs: 15000
      },
      worker: {
        concurrency: 4,
        leaseMs: 30000,
        renewIntervalMs: 10000,
        drainTimeoutMs: 30000
      },
      scheduler: { domain: 'billing-notification' }
    }
  },
  metrics: { enabled: true }
});
```

提供商给出完整 URL 时优先用 URL；拆分凭据时再使用字段。TLS 握手失败先核对 SNI、CA 和主机名，不要关闭 TLS 绕过生产问题。

## Sentinel 故障转移

```ts
connection: {
  sentinel: {
    name: 'mymaster',
    nodes: [
      { host: '10.0.0.11', port: 26379 },
      { host: '10.0.0.12', port: 26379 },
      { host: '10.0.0.13', port: 26379 }
    ],
    username: 'queuebit',
    password: 'redis-password'
  }
}
```

Sentinel 只负责重新发现单主，不等于 Redis Cluster。故障转移期间 Worker 停止拉新、Scheduler 资格不确定时停止推进，恢复后通过 lease 和 stalled recovery 收敛。

## 参数怎么选

| 参数 | 起点 | 调大之前检查 | 配错后的症状 |
|------|------|--------------|--------------|
| `attempts` | 通知类 3 到 4 | 永久错误比例、下游恢复时间 | 太小导致早退，太大造成重试风暴 |
| `backoff.delayMs` | 1000ms 指数退避 | 下游限流窗口 | 太短持续压垮下游，太长恢复慢 |
| `timeoutMs` | 高于正常 p99 | 下游是否支持 `AbortSignal` | 太短产生超时重试，太长拖住 drain |
| `concurrency` | 每 Worker 2 到 4 | 下游配额、连接池、内存 | waiting 堆积或下游 429/超时增加 |
| `leaseMs` | 高于正常 job p99 | GC 暂停、网络抖动 | 太短产生 stalled，太长崩溃恢复慢 |
| `renewIntervalMs` | 约 lease 的 1/3 | Redis p99 延迟 | 必须严格小于 `leaseMs` |
| `drainTimeoutMs` | 高于正常 job p99 | 平台终止宽限期 | 太短产生恢复，太长发布阻塞 |
| `scheduler.domain` | 一个业务推进组一个稳定值 | namespace 与候选范围 | 不一致导致多个独立 active 或无人接管 |

## 三组可直接采用的起始值

| 场景 | concurrency | attempts/backoff | timeout/lease | 说明 |
|------|-------------|------------------|---------------|------|
| 邮件/Push | `4` | `4`，指数 1s | `15s / 30s` | 观察供应商全局限额 |
| 短数据库任务 | `8` | `3`，固定 500ms | `5s / 15s` | 并发不能超过连接池预算 |
| 外部生成任务 | `2` | `3`，指数 2s | `60s / 90s` | 长任务优先拆分，不无限加 lease |

这些是起始值，不是默认承诺。用实际 p95/p99、错误率和 SLO 调整。

## 进程职责配置

| 进程 | 需要 | 不需要 |
|------|------|--------|
| Web/API Producer | Redis、namespace、queue、默认 job 选项 | handler、Worker 并发、自动启动 Scheduler |
| Worker | Redis、namespace、queue、handler、并发、lease、drain | HTTP 路由、Scheduler 资格 |
| Scheduler | Redis、namespace、queue 列表、domain | 业务 handler、HTTP 并发 |

可以共享一份配置真相源，但每个进程只启动自己的角色。vext app 启动不等于 Worker 启动。

## 启动前校验

- Redis 连接形式只能明确指向一个单主或 Sentinel master；检测到 Cluster 拓扑应失败。
- `namespace`、queue name 和 Scheduler domain 非空且跨实例一致。
- `renewIntervalMs < leaseMs`，`timeoutMs` 与 lease 不互相矛盾。
- Worker 并发是正整数，平台终止宽限期大于 drain timeout。
- 启动日志输出最终生效配置摘要，但不要依赖日志代替配置真相源。

## 配置后怎么验证

```bash
npx queuebit inspect queue notification --config queuebit.config.ts
npx queuebit inspect workers --queue notification --config queuebit.config.ts
npx queuebit inspect scheduler --domain billing-notification --config queuebit.config.ts
```

依次确认 namespace/queue 正确、Worker 心跳与并发正确、仅有一个 active Scheduler。完整生产演练见 [生产部署](./production-deployment.md)。
