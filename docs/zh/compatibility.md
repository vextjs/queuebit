# 运行环境与兼容边界

## 先看结论

<span class="manual-label">v0.1 final user manual</span>

- queuebit v0.1 只接入 Redis，不引入其他队列后端。
- 首版目标拓扑是显式 producer、worker、scheduler 角色，而不是让 Web 进程隐式承担所有角色。
- Redis standalone 或兼容的托管单主 Redis 服务是首版基线。
- Redis Cluster 在 v0.1 不支持；检测到 cluster 配置时应 fail fast。
- Node.js `>= 20`、Redis `>= 7.0`、TypeScript `>= 5.4` 是用户侧推荐组合。

## 环境前置矩阵

| 项 | v0.1 用户边界 | 说明 |
|----|----------------|------|
| Node.js runtime | `>= 20` | 面向 JavaScript / TypeScript 项目 |
| 包管理 | npm | 包名为 `queuebit` |
| Redis server | `>= 7.0` standalone 或托管单主 Redis | 必须支持 TTL、原子更新、连接恢复和脚本执行语义 |
| Redis auth / TLS | 支持作为连接配置 | 不改变队列一致性语义 |
| Redis Cluster | 不支持 | 发现 cluster 配置时 fail fast，不静默降级 |
| Redis Sentinel / failover | 条件支持连接层 failover | failover 期间 worker 停止拉新，scheduler 失去单活资格时停止推进 |
| vext adapter | `queuebit/vext` | adapter 只转换配置和生命周期，不隐藏拓扑 |
| TypeScript | 推荐 `>= 5.4` | public API 与 adapter 配置提供类型 |
| 操作系统 | 跟随 Node.js 与 Redis 客户端兼容性 | 不依赖 OS 特定能力 |

## Redis 部署边界

| Redis 形态 | 首版态度 | 必须说明的用户影响 |
|------------|----------|--------------------|
| 单机 Redis | 基线目标 | 最适合首版验证、开发和简单生产部署 |
| 托管单主 Redis | 可作为基线等价形态 | 需要确认连接、认证、TLS、超时和持久化策略 |
| Sentinel / 自动故障转移 | 条件支持目标 | failover 期间 worker 可能停止拉新，scheduler 可能停止推进，job 通过 lease/retry 恢复 |
| Redis Cluster | v0.1 不支持 | 启动前明确报错，避免跨 slot 原子迁移风险被隐藏 |
| 多 Redis 后端 | 非目标 | v0.1 不做多后端抽象，不做 database / memory / SQS 等 adapter |

## 分布式拓扑边界

| 进程角色 | 是否首版目标 | 扩展规则 | 不允许的隐式行为 |
|----------|:------------:|----------|------------------|
| Web producer | 是 | 可以多实例提交 job | 不因启动 Web 服务就自动消费 job |
| Worker process | 是 | 可以多进程、多实例；并发由 `worker.concurrency` 控制 | 不默认承担 scheduler |
| Scheduler process | 是 | 可以启动多个候选实例，但同一 `scheduler.domain` 只能一个 active | 不处理业务 job handler |
| Single-process dev | 仅本地开发 | 必须显式标记为 dev/demo | 不作为生产推荐拓扑 |
| Dashboard / admin UI | 非 v0.1 目标 | 后续阶段再评估 | 不阻塞 core runtime 首版 |

## vext 首接入边界

`vext` adapter 的目标是让 vext 项目更容易接入 queuebit，但它不能模糊分布式责任：

- vext app 启动不等于 worker 启动。
- vext cluster worker 数量不等于 queue worker concurrency。
- vext reload 必须映射到 worker drain 或显式 stop。
- scheduler domain 必须在配置和运维页面可见。
- adapter 文档必须引用本页的环境矩阵，不得另行定义冲突的 Redis Cluster 或进程拓扑口径。

## 关联参考

| 问题 | 先读 |
|------|------|
| Redis keyspace 如何避免串写 | [Redis 模型](./redis-model.md) |
| 进程角色怎么配置 | [CLI 与配置](./cli-and-config.md) |
| Worker 和 scheduler 怎么停止 | [Worker 与 Scheduler 生命周期](./worker-lifecycle.md) |
| 故障时如何恢复 | [故障模式与恢复](./failure-modes.md) |
| vext adapter 怎么遵守这些边界 | [vext 接入](./vext-integration.md) |
