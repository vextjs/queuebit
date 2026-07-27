# 配置字段字典

<span class="manual-label">配置 · 类型、默认、约束与作用域</span>

本页用于精确查字段。第一次接入不要从这里开始，先看 [快速开始](./quick-start.md)；不知道该怎么选 Redis、Worker 或批处理参数时，先看 [配置 Redis 和 Worker](./configuration-recipes.md)。

## 常用字段先看这里

| 你要配置什么 | 字段 | 什么时候需要改 |
|---|---|---|
| Redis 地址 | `connection` | 本地、生产、TLS 或 Sentinel 地址不同 |
| 环境隔离 | `namespace` | 区分 dev/staging/prod 或多业务线 |
| Worker 默认并发 | `workerDefaults.concurrency` | 一个 Worker 进程同时处理几个 job |
| Worker 退出等待 | `workerDefaults.drainTimeoutMs` | 发布、重启、缩容时等待 active job 收尾 |
| 队列水位 | `queues.*.backpressure` | 不想 Redis 被瞬时大量任务打满 |
| 数据库批处理 | `batchRuns` | 只有使用 `runs.start` 批量扫数据库时需要 |
| 历史保留 | `retention` | 控制成功/失败/Completion 历史可查多久 |
| 健康与指标 | `observability` | 要接 `/healthz`、Prometheus 或日志级别 |
| payload 大小 | `limits` | 任务数据、结果、批量提交可能很大 |
| 去重身份窗口 | `deduplication` | 同一个业务 identity 多次提交时要稳定返回同一条记录 |

## 命名与静态校验

`namespace` 默认是 `default`，长度 1～128，只允许字母、数字、冒号、下划线和短横线。queue、source、mapper 和 handler 引用长度 1～192，字符集相同。点号不是合法名称字符。未知字段不会被静默忽略。

## 根配置

| 字段 | 类型 | 必填 | 默认 | 作用 |
|---|---|---:|---|---|
| `connection` | `RedisConnection` | 否 | direct `127.0.0.1:6379/0` | Redis endpoint 与 server policy |
| `namespace` | `string` | 否 | `default` | 环境/业务 keyspace 隔离 |
| `workerDefaults` | `WorkerOptions` | 否 | 见下 | Worker 默认 |
| `scheduler` | `SchedulerOptions` | 否 | cooperative | 时间推进模式/作用域 |
| `queues` | `Record<string, QueueConfig>` | 否 | `{}` | 已声明 Queue 与背压 |
| `batchRuns` | `Record<string, BatchRunConfig>` | 否 | `{}` | 具名批处理定义 |
| `retention` | `RetentionConfig` | 否 | 见下 | 历史和恢复信息保留 |
| `observability` | `ObservabilityConfig` | 否 | 见下 | logger/metrics/health |
| `limits` | `PayloadLimits` | 否 | 见下 | 序列化和批量上限 |
| `deduplication` | `DeduplicationConfig` | 否 | 见下 | 业务 identity 保留窗口 |

Coordinator 并发、source timeout、completion delivery 限制和 role identity 是启动 Coordinator 进程时传入的进程选项，不是当前 `queuebit.config.ts` 的根字段。

## Redis 连接

`url`、direct `host/port` 和 `sentinels/masterName` 三种模式互斥。

| 字段 | 类型 | 默认 | 约束/错误 |
|---|---|---|---|
| `url` | string | 无 | 与 `host/sentinels` 互斥 |
| `host` | string | 无 | direct fields 必填 |
| `port` | number | 6379 | 1～65535 |
| `username/password` | string | 无 | Redis ACL |
| `database` | number | 0 | 非负整数 |
| `tls` | object/true | 无 | direct Redis TLS |
| `sentinels` | `{host,port}[]` | 无 | Sentinel 至少 2 个地址 |
| `masterName` | string | 无 | Sentinel 必填 |
| `sentinelUsername/password` | string | 无 | Sentinel ACL |
| `connectTimeoutMs` | number | 5000 | 正整数 |
| `commandTimeoutMs` | number | 5000 | 正整数 |
| `requestRetryLimit` | number | 1 | Producer/inspect/control，非负整数 |
| `backgroundReconnect.initialDelayMs` | number | 250 | 后台角色首次重连上限，正整数 |
| `backgroundReconnect.maxDelayMs` | number | 30000 | 退避封顶，不小于 initial |
| `backgroundReconnect.factor` | number | 2 | 指数倍率，范围 1～10 |
| `backgroundReconnect.jitter` | `full` | full | 每轮在 0～当前上限内随机等待 |
| `backgroundReconnect.logThrottleMs` | number | 30000 | 同 role/endpoint 重复日志最小间隔 |
| `serverPolicy.mode` | `warn/strict` | warn | 生产必须显式 strict |

strict 模式下，`maxmemory-policy != noeviction`、persistence 未启用/错误、或关键 policy 不可读都不进入 ready。

`requestRetryLimit` 只约束 Producer、inspect 和控制命令。Worker、Coordinator 与 cooperative 时间推进在 Redis 中断后停止新 claim/load/dispatch/promotion，按 `backgroundReconnect` 无限轮次重连，直到 drain/close；后台重连没有总次数上限。首次失败立即记录，此后同 role/endpoint 最多每 `logThrottleMs` 记录一次。断连期间 health 为 `not_ready`，不是 `degraded`。

## 直接 Job 选项

直接 Job 的 retry、timeout、delay、backoff、deduplication key 和业务 `idempotencyKey` 在每次 `jobs.add` / `jobs.addBulk` 请求里传入。当前配置文件没有根级 `jobDefaults`，也没有 queue 级 `jobDefaults` 字段。

## Queue 与共享背压

声明 queue 时，若未显式覆盖水位字段，Queuebit 填入下列内置默认：

| 字段 | 默认 | 规则 |
|---|---:|---|
| `backpressure.highWatermarkJobs` | 10000 | waiting/delayed/retrying jobs high |
| `backpressure.lowWatermarkJobs` | 5000 | 必须小于 high |
| `backpressure.highWatermarkBytes` | 268435456 | 非终态 payload 256 MiB |
| `backpressure.lowWatermarkBytes` | 134217728 | 必须小于 high |

jobs 或 bytes 任一到 high 就设置共享 latch；两者都回到 low 或更低才解除。BatchRun 和直接 `jobs.add/addBulk` 共用该边界。单次请求自身就超过 high 时返回 `QB_BACKPRESSURE_REQUEST_TOO_LARGE`，不进入无意义等待。

## Worker

| 字段 | 默认 | 约束 |
|---|---:|---|
| `concurrency` | 1 | 每进程 active jobs；集群总并发是求和 |
| `leaseMs` | 30000 | job owner lease |
| `renewIntervalMs` | 10000 | 必须 `< leaseMs/2` |
| `pollIntervalMs` | 1000 | 无工作时轮询上限 |
| `drainTimeoutMs` | 60000 | shutdown 等待 active |
| `maxStalledRecoveries` | 2 | lease 丢失恢复上限 |
| `heartbeatIntervalMs` | 5000 | role heartbeat 写入间隔 |
| `heartbeatTtlMs` | 15000 | role heartbeat TTL |

## Coordinator 进程选项

Coordinator 选项由 Coordinator factory 或 CLI role process 传入，不属于根配置。未传时 role 会生成 `coordinatorId`；CLI drain 命令必须带同一个 identity，才能向 Redis 写入目标 role 的协作式 drain request。

## 时间推进 / Scheduler

| 字段 | 默认 | 说明 |
|---|---:|---|
| `mode` | cooperative | v0.1 只接受 `cooperative` |
| `domain` | default | single-active 作用域 |
| `leaseMs` | 30000 | owner lease |
| `renewIntervalMs` | 10000 | `< leaseMs/2` |
| `pollIntervalMs` | 1000 | due work 扫描 |
| `promotionBatchSize` | 500 | 单轮提升上限 |
| `drainTimeoutMs` | 60000 | shutdown 窗口 |
| `heartbeatIntervalMs` | 5000 | role heartbeat 写入间隔 |
| `heartbeatTtlMs` | 15000 | role heartbeat TTL |

当前 schema 中 `mode` 是常量 `cooperative`。传入其他值会返回 `QB_CONFIG_INVALID`。只有后台 Worker 参与候选，Web/Producer 不参与；多个 Worker 为同一 domain 竞争一个带 generation fencing 的有效 owner。

<a id="batchrun-定义"></a>
## BatchRun 定义

| 字段 | 必填 | 默认 | 规则 |
|---|---:|---:|---|
| `version` | 是 | 无 | 正整数；行为改变必须提升 |
| `queue` | 是 | 无 | 必须已声明 |
| `source` | 是 | 无 | runtime 具名注册 |
| `mapper` | 是 | 无 | runtime 具名注册 |
| `inputSchema` | 否 | 无 | 存在时校验 Run input |
| `pageSize` | 否 | 100 | 正整数，同时受 page/job/bytes limit |
| `dispatch.mode` | 否 | sequential | `sequential/paced` |
| `dispatch.intervalMs` | 否 | 0 | 非负整数 |
| `dispatch.maxInFlightBatches` | 否 | 1 | sequential 必须 1 |
| `completion.batch/run` | 否 | 无 | handler 具名注册 |

Completion handler policy 支持 `handler`、可选 `attempts`、可选 `backoff.type/delayMs/maxDelayMs`。默认 `attempts` 是 `3`。当前 runtime 的 completion handler 配置没有 `timeoutMs` 或 `jitter` 字段。

`inputSchema` 会随 BatchRun 定义保存，并在 `runs.start` 校验输入时编译。Run input 不符合 schema 时返回 `QB_RUN_INPUT_INVALID` 和校验详情。建议保持 schema 简单、可 JSON 序列化，并随 BatchRun definition version 一起演进。

## Retention

| 字段 | 默认 | 说明 |
|---|---:|---|
| `completedJobs.ageMs/maxCount` | 24h / 100000 | 成功 job 诊断窗口 |
| `failedWork.ageMs/maxCount` | 7d / 100000 | mapper/job 已保存失败详情的可恢复窗口 |
| `terminalRuns.ageMs/maxCount` | 30d / 10000 | 终态 Run 摘要/审计链 |
| `completionEvents.ageMs/maxCount` | 30d / 10000 | delivered/not_required Completion event 详情 |

active/非终态 Run、未交付 completion 不得被清理。recovery details 过期后，`runs.retryFailed` 返回 `QB_RUN_STATE_CONFLICT`，调用方应基于业务数据库创建新的 Run。

## 去重窗口和过期身份记录

| 字段 | 默认 | 约束 |
|---|---:|---|
| `jobKeyTtlMs` | 7d | 不得短于 completedJobs age |
| `runKeyTtlMs` | 30d | 不得短于 terminalRuns 与 completionEvents age |

完整详情先被清理时，TTL 内仍保留 identity、digest、version 和 state 这类轻量身份记录。相同 input 会返回原 identity 和 `detailsExpired=true`；Queuebit 不会伪造已删除的 payload、result 或 Completion summary。`completionEvents.ageMs/maxCount` 独立控制已安全结束的 Completion event 详情窗口；详情过期后，`completions.list/get` 仍能看到 identity，但这条轻量记录不再占用清理窗口。

## Payload 上限

| 字段 | 默认 bytes |
|---|---:|
| `maxRunInputBytes` | 65536 |
| `maxJobDataBytes` | 262144 |
| `maxJobResultBytes` | 65536 |
| `maxPageBytes` | 8388608 |
| `maxBulkJobs` | 1000 entries |
| `maxBulkBytes` | 8388608 |

超限在进入 Redis 前失败，输出实际大小、限制和缩减建议。payload 保留业务 ID 和 processor 必需字段，文件/二进制放对象存储。

## 可观测性

| 字段 | 默认 | 说明 |
|---|---:|---|
| `logLevel` | info | debug/info/warn/error |
| `metrics.enabled` | true | 进程内 registry |
| `metrics.format` | prometheus | 文本导出 |
| `metrics.prefix` | `queuebit_` | 指标前缀 |
| `health.staleAfterMs` | 45000 | heartbeat stale 界线 |

Queuebit core 不启动 HTTP server。应用挂载并保护 metrics/readiness endpoint；每个进程的指标不得冒充全集群汇总。

## 角色按需加载 runtime

- Producer 不加载 runtime。
- Worker 只激活 processor 和 Worker 资源。
- Coordinator 只激活 source、mapper、completion 和 Coordinator 资源。
- cooperative 时间推进复用 Worker 进程，但不激活额外业务 DB/source 资源。
- runtime import 本身无连接副作用，factory 首次使用才打开资源，lifecycle close 成对释放。
