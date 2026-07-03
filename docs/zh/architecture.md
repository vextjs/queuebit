# 架构说明

## 页面定位

<span class="manual-label">v0.1 final user manual</span>

本页支撑最终用户手册，说明 queuebit v0.1 为什么拆分 core、Redis coordination、worker runtime、scheduler runtime 和 vext adapter。用户不需要先读本页才能上手；实现者需要用它防止后续开发偏离手册。

## 设计目标

queuebit 首版只解决一个明确问题：为 Node.js / vext 项目提供 Redis-backed、分布式优先、BullMQ-like 的任务队列基础能力。

首版设计要同时满足：

- **Redis-only**：共享状态、协调状态、时间推进和恢复语义都围绕 Redis 设计。
- **Distributed-first**：默认存在多个 producer、多个 worker、多实例部署和实例重启。
- **At-least-once**：优先保证故障后可恢复，不承诺 exactly-once。
- **Core / Adapter 分离**：core 不依赖 vext；vext adapter 只负责宿主集成。
- **可观测**：queue depth、active jobs、retry、delayed、stalled recovery、worker identity 和 scheduler identity 必须能被观察。

## 层级边界

| 层级 | 职责 | 不负责 |
|------|------|--------|
| Queuebit core | Queue、job、producer、worker、scheduler、lease、retry、delay、drain、metrics、Redis 协调 | vext 生命周期、HTTP 路由、应用配置文件解析 |
| Redis coordination | keyspace、原子迁移、锁/lease、状态集合、时间窗口、事件流 | 业务幂等、业务事务、跨 Redis 集群容灾 |
| Worker runtime | 拉取、声明、续租、执行、ack/fail、graceful drain、stalled 处理 | 每个 Web 进程自动消费、业务处理函数幂等 |
| Scheduler runtime | delayed promotion、retry reschedule、单活推进、恢复扫描 | 业务定时任务编排、workflow orchestration |
| vext adapter | vext 配置入口、生命周期挂接、健康检查、指标暴露、推荐部署入口 | 改写 core 语义、隐藏 worker/scheduler 拓扑 |
| Documentation contract | 定义用户路径、API、Redis 模型、失败语义和验收矩阵 | 记录内部台账、替代测试或实现事实 |

## 模块职责

| 模块 | 目标能力 | 实现约束 |
|------|----------|----------|
| Queue | 创建逻辑队列、提交 job、查看状态、drain | 必须带 namespace；不得依赖进程内内存作为真实状态 |
| Job | 表示 payload、attempts、状态、时间戳、错误摘要和业务 idempotency key | 重投递语义必须显式；不得承诺 exactly-once |
| Producer | 向 Redis 提交 job，返回可追踪 job id | 可在 Web/API 进程中使用；不得隐式启动 worker |
| Worker | 声明 job、处理 job、续租、ack/fail、drain | 无法确认 lease 时必须停止拉新 |
| Scheduler | 推进 delayed/retry/stalled 恢复 | 同一 scheduler domain 内必须 single-active |
| Metrics | 暴露队列和运行时观察面 | 指标名称可演进，但必须覆盖运维核心问题 |

## 部署拓扑

推荐的 v0.1 拓扑是三类进程显式分离：

| 进程类型 | 是否必须 | 典型位置 | 说明 |
|----------|:--------:|----------|------|
| Producer | 是 | vext HTTP/API 进程 | 只提交 job，不默认消费 job |
| Worker | 是 | 独立 worker 进程或独立容器 | 可以水平扩展，受 concurrency 和 lease 控制 |
| Scheduler | 条件 | 独立 scheduler 进程或 worker 旁路模式 | 同一 domain 单活；没有 delayed/retry 时可按配置关闭 |

不推荐把所有 HTTP worker 都默认变成 queue worker，也不推荐把 scheduler 隐藏在每个应用实例里无条件启动。

## 不变量

- Redis 是首版唯一协调后端。
- active job 必须有可恢复路径。
- delayed 和 retry 推进必须由 scheduler domain 的 active scheduler 完成。
- Worker 无法续租或无法确认 lease 时，不得继续拉新 job。
- Drain 只阻止新声明，不保证已声明 job 一定成功。
- vext adapter 不得让 core API 只能通过 vext app 对象使用。
- 文档中的最终用户手册与实现不一致时，必须先判定是实现偏离还是文档需要修订。

## 实现验收

任何后续开发进入实现前，至少要能回答：

| 验收问题 | 对应文档 |
|----------|----------|
| API 是否覆盖 producer、worker、scheduler、metrics 生命周期？ | [API 参考](./target-api.md) |
| CLI 和配置是否能表达独立 worker / scheduler 拓扑？ | [CLI 与配置](./cli-and-config.md) |
| Redis keyspace 和状态迁移是否支持恢复？ | [Redis 模型](./redis-model.md) |
| Worker 崩溃、ack 丢失、lease 不确定时系统怎么做？ | [Worker 与 Scheduler 生命周期](./worker-lifecycle.md) 和 [故障模式](./failure-modes.md) |
| vext 是否只做 adapter，不吞掉 core 边界？ | [vext 接入](./vext-integration.md) |
| 本次实现是否没有偏离最终用户手册？ | [开发护栏](./development-contract.md) |
