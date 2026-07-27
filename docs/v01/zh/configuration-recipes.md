# 配置 Redis 和 Worker

<span class="manual-label">生产运维 · 从最小配置开始</span>

大多数项目先只需要两个文件：`queuebit.config.ts` 放 Redis、namespace、queue 和 Worker 默认值；`queuebit.runtime.ts` 先只注册 processor。API 进程负责提交任务，Worker 进程负责执行任务，CLI 只是验证和排查工具，不是接入 Queuebit 的必经入口。source、mapper 和 completion handler 只有数据库批处理才需要。

## 先判断你属于哪种情况

| 你要做什么 | 先配置什么 | 暂时不用看 |
|---|---|---|
| 本地或框架里跑一个后台任务 | `connection`、`namespace`、`queues` | Sentinel、retention、BatchRun、CI |
| 线上跑多个 Worker | 加 `workerDefaults`、Redis strict policy、health check | BatchRun definition |
| 从数据库分页创建很多任务 | 再加 `batchRuns`，并在 runtime 注册 source/mapper/completion | Redis 内部模型 |

如果你只是把 Queuebit 接到 Node/Fastify/Nest/vext 项目里，通常就是在服务启动时创建 client，在独立 worker 脚本里注册 runtime，然后让进程管理器或容器拉起 worker。

## 文件、启动参数和 runtime 的分工

多数项目只需要文件里的默认值；只有某个进程需要临时不同的并发或 drain timeout，才用 CLI 参数或启动参数覆盖。

1. CLI 显式进程参数，例如 `--concurrency 12`。
2. 创建 client/role 时的 runtime override。
3. Queue 背压或 BatchRun 局部配置。
4. 根级 Worker、Scheduler、retention、limits、deduplication 和 observability defaults。
5. Queuebit 内建默认。

Producer/API 进程只需要静态配置，不一定加载 `queuebit.runtime.ts`。`config validate --runtime`、Worker 和 Coordinator 会进一步确认 processor 是否注册；只有 BatchRun 才会额外检查 source、mapper 和 completion。

## 最小本地配置：一个 Redis，一个队列

```ts
// queuebit.config.ts
import { defineQueuebitConfig } from 'queuebit';

export default defineQueuebitConfig({
  connection: { url: 'redis://127.0.0.1:6379/0' },
  namespace: 'dev:demo',
  queues: { notification: {} }
});
```

这个配置已经能跑 [执行一个后台任务](./job-recipes.md)。要做数据库批处理时，才继续增加 BatchRun definition、runtime registration 和 Coordinator。

## 线上 Redis：托管 Redis + TLS

```ts
connection: {
  host: 'redis.example.internal',
  port: 6380,
  username: 'queuebit',
  password: 'replace-with-project-value',
  database: 0,
  tls: {
    servername: 'redis.example.internal',
    rejectUnauthorized: true
  },
  connectTimeoutMs: 5_000,
  commandTimeoutMs: 5_000,
  requestRetryLimit: 1,
  backgroundReconnect: {
    initialDelayMs: 250,
    maxDelayMs: 30_000,
    factor: 2,
    jitter: 'full',
    logThrottleMs: 30_000
  },
  serverPolicy: { mode: 'strict' }
}
```

`tls.servername` 应与证书匹配，不要用 `rejectUnauthorized:false` 掩盖证书错误。`requestRetryLimit` 只让 Producer、inspect 和控制命令快速失败；后台 Worker/Coordinator 会持续重连，断连期间停止推进并报告 `not_ready`。

## 高可用 Redis：Sentinel

```ts
connection: {
  sentinels: [
    { host: '10.0.1.11', port: 26379 },
    { host: '10.0.1.12', port: 26379 },
    { host: '10.0.1.13', port: 26379 }
  ],
  masterName: 'mymaster',
  username: 'queuebit',
  password: 'replace-with-project-value',
  sentinelUsername: 'sentinel-user',
  sentinelPassword: 'replace-with-project-value',
  database: 0,
  connectTimeoutMs: 5_000,
  commandTimeoutMs: 5_000,
  serverPolicy: { mode: 'strict' }
}
```

Sentinel 只帮 Queuebit 重新找到新的 primary。它不是 Redis Cluster，也不保证已经确认的写永远零丢失。failover 后要重新检查 primary/replica 角色、persistence、replication 和 `maxmemory-policy`。

## 只有数据库批处理才需要：BatchRun 定义

```ts
batchRuns: {
  'receipt-campaign': {
    version: 1,
    queue: 'notification',
    source: 'paid-orders',
    mapper: 'receipt-jobs',
    inputSchema: {
      type: 'object',
      required: ['tenantId', 'paidBefore'],
      additionalProperties: false,
      properties: {
        tenantId: { type: 'string', minLength: 1 },
        paidBefore: { type: 'string', format: 'date-time' }
      }
    },
    pageSize: 500,
    dispatch: {
      mode: 'sequential',
      intervalMs: 2_000,
      maxInFlightBatches: 1
    },
    completion: {
      batch: { handler: 'record-receipt-batch-result' },
      run: { handler: 'record-receipt-run-result' }
    }
  }
}
```

| 选择 | 推荐起点 | 什么时候调整 |
|---|---:|---|
| `pageSize` | 内建 100，压测后常调到 500 | 单页太慢、payload 太大或数据库查询太重时调整 |
| `dispatch.mode` | sequential | 只有下游能承受多个批次同时在途时才改 paced |
| `intervalMs` | 0 到 2000 | 根据下游限流和每批结果回写时间选择 |

## 完整生产配置示例

下面是把 Redis、Worker、BatchRun、保留策略、观测和 payload 限制都放在一起的示例。第一次接入不要照抄整段；先跑通一个后台任务，再按需要逐步打开对应配置。

```ts
import { defineQueuebitConfig } from 'queuebit';

export default defineQueuebitConfig({
  connection: {
    host: 'redis.example.internal',
    port: 6380,
    username: 'queuebit',
    password: 'replace-with-project-value',
    database: 0,
    tls: {
      servername: 'redis.example.internal',
      rejectUnauthorized: true
    },
    connectTimeoutMs: 5_000,
    commandTimeoutMs: 5_000,
    requestRetryLimit: 1,
    backgroundReconnect: {
      initialDelayMs: 250,
      maxDelayMs: 30_000,
      factor: 2,
      jitter: 'full',
      logThrottleMs: 30_000
    },
    serverPolicy: { mode: 'strict' }
  },
  namespace: 'prod:billing',
  workerDefaults: {
    concurrency: 8,
    leaseMs: 30_000,
    renewIntervalMs: 10_000,
    pollIntervalMs: 1_000,
    drainTimeoutMs: 60_000,
    maxStalledRecoveries: 2
  },
  scheduler: {
    mode: 'cooperative',
    domain: 'billing',
    leaseMs: 30_000,
    renewIntervalMs: 10_000,
    pollIntervalMs: 1_000,
    promotionBatchSize: 500,
    drainTimeoutMs: 60_000
  },
  queues: {
    notification: {
      backpressure: {
        highWatermarkJobs: 10_000,
        lowWatermarkJobs: 5_000,
        highWatermarkBytes: 268_435_456,
        lowWatermarkBytes: 134_217_728
      }
    }
  },
  batchRuns: {
    'receipt-campaign': {
      version: 1,
      queue: 'notification',
      source: 'paid-orders',
      mapper: 'receipt-jobs',
      inputSchema: {
        type: 'object',
        required: ['tenantId', 'paidBefore'],
        additionalProperties: false,
        properties: {
          tenantId: { type: 'string', minLength: 1 },
          paidBefore: { type: 'string', format: 'date-time' }
        }
      },
      pageSize: 500,
      dispatch: {
        mode: 'sequential',
        intervalMs: 2_000,
        maxInFlightBatches: 1
      },
      completion: {
        batch: {
          handler: 'record-receipt-batch-result',
          attempts: 5,
          backoff: { type: 'exponential', delayMs: 1_000, maxDelayMs: 60_000 }
        },
        run: {
          handler: 'record-receipt-run-result',
          attempts: 10,
          backoff: { type: 'exponential', delayMs: 1_000, maxDelayMs: 300_000 }
        }
      }
    }
  },
  retention: {
    completedJobs: { ageMs: 86_400_000, maxCount: 100_000 },
    failedWork: { ageMs: 604_800_000, maxCount: 100_000 },
    terminalRuns: { ageMs: 2_592_000_000, maxCount: 10_000 },
    completionEvents: { ageMs: 2_592_000_000, maxCount: 10_000 }
  },
  observability: {
    logLevel: 'info',
    metrics: { enabled: true, format: 'prometheus', prefix: 'queuebit_' },
    health: { staleAfterMs: 45_000 }
  },
  limits: {
    maxRunInputBytes: 65_536,
    maxJobDataBytes: 262_144,
    maxJobResultBytes: 65_536,
    maxPageBytes: 8_388_608,
    maxBulkJobs: 1_000,
    maxBulkBytes: 8_388_608
  },
  deduplication: {
    jobKeyTtlMs: 604_800_000,
    runKeyTtlMs: 2_592_000_000
  }
});
```

## 启动前验证

```bash
npx queuebit config validate --config queuebit.config.ts --runtime queuebit.runtime.ts
npx queuebit health inspect --config queuebit.config.ts --json
```

常见校验失败：

| 错误 | 原因 | 修复 |
|---|---|---|
| `QB_CONFIG_INVALID` | 字段拼写错、字段过期、值非法或连接模式混用 | 查 [配置字段字典](./cli-and-config.md) |
| `QB_CONFIG_HANDLER_NOT_REGISTERED` | definition 引用的 source/mapper/handler 未注册 | 在 `queuebit.runtime.ts` 里补上同名注册 |
| `QB_REDIS_CLUSTER_UNSUPPORTED` | 配置或探测到 Redis Cluster | 切换 single-primary 或 Sentinel |
| `QB_REDIS_PREFLIGHT_FAILED` | strict policy 下 eviction、persistence、role 或 policy 读取不符合要求 | 修复 Redis policy 后重试 |
| `QB_RUN_INPUT_INVALID` | start input 不符合 `inputSchema` | 根据 JSON Pointer/keyword 修复输入 |
