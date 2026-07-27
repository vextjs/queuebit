# 参考

<span class="manual-label">参考 · 精确查询</span>

本组页面用于查方法、字段、命令、状态和错误，不代替 [快速开始](./quick-start.md) 与用户任务页。

## 版本状态

当前页面记录 Queuebit v0.1 计划稳定的公开使用方式。实际 npm 包是否已经包含对应方法、配置字段和 CLI 命令，以你安装的版本和发布说明为准。

## 按需求查找

| 你要做什么 | 去哪里 |
|---|---|
| 查 client/jobs/runs/completions 方法 | [API 快查](./target-api.md) |
| 查 source/mapper/processor/completion runtime 契约 | [API 快查](./target-api.md#runtime-registration) |
| 查配置类型、默认和互斥 | [配置字段字典](./cli-and-config.md) |
| 查 start/inspect/control/drain 命令 | [CLI 参考](./cli-reference.md) |
| 查 Job/Run/Completion 状态 | [状态和错误怎么读](./failure-modes.md) |
| 查 Node/Redis/vext 能不能用 | [我的环境能不能用](./compatibility.md) |
| 查事故现象的安全恢复 | [故障恢复](./failure-runbooks.md) |

## 公开命名快览

| 域 | 稳定入口 |
|---|---|
| 创建 client | `createQueuebitClient({ config, logger? })` |
| 静态配置 | `defineQueuebitConfig()` |
| 运行时注册 | `defineQueuebitRuntime()` + 具名 source/mapper/processor/completion helper |
| 直接 Job | `queuebit.jobs.add/addBulk/get/list/cancel/retryFailed` |
| BatchRun | `queuebit.runs.start/get/list/listFailures/pause/resume/cancel/retryFailed` |
| Completion | `queuebit.completions.get/list/retry` |
| 生命周期 | `queuebit.close()`；后台角色 SIGTERM drain |

## 延期能力

v0.1 不包含 repeatable/cron、DAG/Flows、Redis Cluster、非 Redis backend、priority、全局 rate limiter、partition/key ordering、Dashboard/Admin UI、CDC/无限数据源。不承诺 exactly-once、严格 FIFO、租户公平性或自动撤销外部副作用。

## 维护者入口

使用者不需要阅读 Redis key/Lua 或内部验收。实现者从 [架构说明](./architecture.md)、[Redis 模型](./redis-model.md)、[内部生命周期](./worker-lifecycle.md) 和 [开发合同](./development-contract.md) 开始。
