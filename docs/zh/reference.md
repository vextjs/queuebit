# 参考入口

<!-- queuebit-v01-legacy-doc -->
> [!WARNING]
> **历史文档，已停止维护。** 当前 v0.1 最终用户手册位于 [`docs/v01/zh`](../v01/zh/index.md)。本页仅保留历史上下文，API、命令、配置和示例不得用于新接入或实现。

## 如何使用本页

<span class="manual-label">用户参考索引</span>

本页是用户查找 API、配置、CLI、vext adapter 和运维说明的入口。queuebit 的最终用户手册按“先完成任务，再查对象”的方式组织。

## 按任务查找

| 你要做什么 | 入口 |
|----------|----------|
| 跑通第一批 jobs | [快速开始](./quick-start.md) |
| 批量、延迟、重试、并发、事件和指标 | [任务场景与配方](./job-recipes.md) |
| 扩容 Worker、滚动发布和 Scheduler 接管 | [分布式 Worker](./distributed-workers.md) |
| 防止外部副作用重复 | [业务幂等模式](./idempotency-patterns.md) |
| 在 vext 项目批量提交 jobs | [vext 接入](./vext-integration.md) |
| 部署独立 Worker、Scheduler 和生产 Redis | [生产部署](./production-deployment.md) |
| 按环境选择 Redis、Worker、Scheduler 参数 | [配置场景与配方](./configuration-recipes.md) |
| 查询配置字段与 CLI | [CLI 与配置](./cli-and-config.md) |
| 判断 Redis / Node / Cluster 支持 | [运行环境与兼容边界](./compatibility.md) |
| 排查 waiting / delayed / retry / stalled | [运维与排查](./operations.md) |
| 理解重复投递和恢复 | [故障模式与恢复](./failure-modes.md) |
| 按步骤处置线上故障 | [故障处置手册](./failure-runbooks.md) |

## 按对象查找

| 对象 | 入口 |
|------|------|
| `Queue.addBulk` / job options / job state | [API 参考](./target-api.md) |
| `Worker.run` / `Worker.close` / drain | [API 参考](./target-api.md) |
| `Scheduler.run` / `Scheduler.close` / domain | [API 参考](./target-api.md) |
| Worker、Scheduler 的生产进程启动 | [生产部署](./production-deployment.md) |
| Queue / Job / Producer / Worker / Scheduler 概念 | [核心概念](./concepts.md) |

## 按命令查找

| 命令 | 入口 |
|------|------|
| `npx queuebit worker start` | [CLI 与配置](./cli-and-config.md) |
| `npx queuebit worker drain` | [CLI 与配置](./cli-and-config.md) |
| `npx queuebit scheduler start` | [CLI 与配置](./cli-and-config.md) |
| `npx queuebit inspect queue` | [运维与排查](./operations.md) |
| `npx queuebit inspect workers` | [运维与排查](./operations.md) |
| `npx queuebit inspect scheduler` | [运维与排查](./operations.md) |

## Maintainer 文档（接入用户不必阅读）

普通接入用户不需要先读这些页面。它们用于后续开发和维护时校验实现没有偏离最终用户手册。

| 你要确认什么 | 入口 |
|--------------|------|
| core、Redis runtime、Worker、Scheduler、vext adapter 的边界 | [架构说明](./architecture.md) |
| Redis keyspace、状态集合和原子迁移模型 | [Redis 模型](./redis-model.md) |
| Worker / Scheduler 内部阶段和维护者验收 | [内部生命周期](./worker-lifecycle.md) |
| 开发时如何反向满足用户手册 | [开发护栏](./development-contract.md) |
