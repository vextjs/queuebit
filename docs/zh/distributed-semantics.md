# Redis-only 与分布式恢复

## 设计前提

queuebit 默认面向多进程、多实例、多节点部署。它不会把单机内存状态当成正式协调语义。

这意味着 queue、job、lease、timer、retry 和 scheduler 协调状态都必须能通过 Redis 闭环。

## Redis-only

首版只接入 Redis，不提供 memory、database 或其他 broker 后端。

这样做的目标是收窄故障模型：所有实例看到同一份共享状态，协调语义围绕 Redis 原子操作、过期、时间窗口和恢复扫描展开。

## At-least-once

queuebit 的目标不是 exactly-once。它会把故障恢复放在优先级更高的位置。

当 worker 崩溃、ack 丢失、lease 过期或网络抖动发生时，job 可能被再次投递。业务侧需要理解：

- 成功处理后可能因为 ack 丢失而重试。
- 同一个 job 的处理函数可能被执行超过一次。
- 幂等键、业务去重或状态机保护应由业务按场景设计。

## Lease recovery

Worker 处理 job 时需要持有 lease。lease 到期或无法续租时，系统应把 job 视为可恢复，而不是永久卡在 active 状态。

如果 worker 无法确认自己仍然持有 lease，它应该停止拉取新 job，并让当前 job 进入可恢复路径。

## Single-active scheduler

Delayed promotion 和 retry reschedule 属于时间推进行为。同一 scheduler domain 中如果多个 scheduler 同时推进，可能产生重复投递或状态漂移。

queuebit 的约束是：同一时刻只能有一个 active scheduler 推进。如果单活资格不可确认，推进必须停止。

## Graceful drain

Graceful drain 用于让 worker 停止接收新 job，同时尽量完成已经声明的工作。

Drain 不等于强制成功。若 worker 在 drain 期间失联或 lease 失效，job 仍需要回到可恢复状态。

## 相关文档

- Redis keyspace、状态集合和原子迁移见 [Redis 模型](./redis-model.md)。
- Worker claim、续租、ack/fail、drain 见 [Worker 与 Scheduler 生命周期](./worker-lifecycle.md)。
- Redis 不可用、worker 崩溃、ack 丢失、scheduler 不确定见 [故障模式与恢复](./failure-modes.md)。
- 这些语义如何进入 API 见 [API 参考](./target-api.md)。
