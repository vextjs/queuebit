# 参考入口

## 如何使用本页

<span class="manual-label">v0.1 final user manual</span>

本页是用户查找 API、配置、CLI、vext adapter 和运维说明的入口。queuebit 的最终用户手册按“先完成任务，再查对象”的方式组织。

## 按任务查找

| 你要做什么 | 入口 |
|----------|----------|
| 跑通第一批 jobs | [快速开始](./quick-start.md) |
| 在 vext 项目批量提交 jobs | [vext 接入](./vext-integration.md) |
| 配置 Redis、worker、scheduler | [CLI 与配置](./cli-and-config.md) |
| 判断 Redis / Node / Cluster 支持 | [运行环境与兼容边界](./compatibility.md) |
| 排查 waiting / delayed / retry / stalled | [运维与排查](./operations.md) |
| 理解重复投递和恢复 | [故障模式与恢复](./failure-modes.md) |

## 按对象查找

| 对象 | 入口 |
|------|------|
| `Queue.addBulk` / job options / job state | [API 参考](./target-api.md) |
| `Worker.run` / lease / drain | [Worker 与 Scheduler 生命周期](./worker-lifecycle.md) |
| `Scheduler.run` / scheduler domain | [Worker 与 Scheduler 生命周期](./worker-lifecycle.md) |
| Redis keyspace 与状态迁移 | [Redis 模型](./redis-model.md) |
| Queue / Job / Producer / Worker / Scheduler 概念 | [核心概念](./concepts.md) |

## 按命令查找

| 命令 | 入口 |
|------|------|
| `queuebit worker start` | [CLI 与配置](./cli-and-config.md) |
| `queuebit worker drain` | [CLI 与配置](./cli-and-config.md) |
| `queuebit scheduler start` | [CLI 与配置](./cli-and-config.md) |
| `queuebit inspect queue` | [运维与排查](./operations.md) |
| `queuebit inspect workers` | [运维与排查](./operations.md) |
| `queuebit inspect scheduler` | [运维与排查](./operations.md) |

## 实现附录

普通接入用户不需要先读这些页面。它们用于后续开发和维护时校验实现没有偏离最终用户手册。

| 你要确认什么 | 入口 |
|--------------|------|
| core、Redis runtime、worker、scheduler、vext adapter 的边界 | [架构说明](./architecture.md) |
| Redis keyspace、状态集合和原子迁移模型 | [Redis 模型](./redis-model.md) |
| 开发时如何反向满足用户手册 | [开发护栏](./development-contract.md) |
