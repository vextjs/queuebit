# 核心概念

## 概念总览

queuebit 的概念模型围绕 Redis 中的共享状态展开。即使首版先接入 `vext`，核心模型也不会依赖 `vext app` 对象、HTTP worker 数量或框架生命周期。

## Queue

Queue 是一组 job 的逻辑入口。它承载 enqueue、状态观察、drain 和未来参考 API 的语义边界。

一个 queue 应该有清晰的 namespace，避免多个应用或环境在 Redis 中混用状态。

## Job

Job 是被 producer 提交、由 worker 处理的工作单元。

首版语义是 at-least-once：当 worker 崩溃、ack 丢失、lease 失效或网络抖动发生时，同一个 job 允许被重新投递。业务处理函数需要按这个前提设计幂等性或去重策略。

## Producer

Producer 负责提交 job。它应该能在应用进程中独立使用，不要求应用进程同时承担 worker 或 scheduler 职责。

在 vext 场景中，producer 可以靠近 HTTP/API 层，但不应把每个 HTTP worker 都默认变成 queue worker。

## Worker

Worker 负责声明、处理和 ack job。它应该通过显式独立入口或独立命令运行。

当 worker 无法确认 lease 有效、无法续租或正在 graceful drain 时，必须停止拉取新 job。

## Scheduler

Scheduler 负责 delayed promotion、retry reschedule 等需要推进时间状态的任务。

在同一 queue namespace 的同一 scheduler domain 内，同一时刻只能有一个 active scheduler 推进。单活资格无法确认时，应停止推进而不是冒险重复推进。

## Lease

Lease 是 worker 持有 job 处理资格的时间性声明。

Lease 不是 exactly-once 保证。它用于让系统在 worker 死亡、超时或连接异常时识别可恢复的 job，并把它重新带回可声明状态。

## Namespace 与 Scheduler Domain

Namespace 用于隔离 Redis 中的 queue 状态。

Scheduler Domain 用于限定 scheduler 单活范围。未来如果引入 recurring dispatch，也应该复用同一 single-active 约束，而不是另起一套互相冲突的推进机制。

## 相关文档

| 概念 | 继续阅读 |
|------|----------|
| Queue / Producer / Worker / Scheduler 的公开语义 | [API 参考](./target-api.md) |
| Namespace、Domain 与 Redis keyspace | [Redis 模型](./redis-model.md) |
| Worker 续租、drain 与恢复 | [Worker 与 Scheduler 生命周期](./worker-lifecycle.md) |
| 崩溃、ack 丢失、重复投递 | [故障模式与恢复](./failure-modes.md) |
| vext 中 producer / worker 分离 | [vext 接入](./vext-integration.md) |
