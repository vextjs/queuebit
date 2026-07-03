# Redis 模型

## 页面定位

<span class="manual-label">v0.1 final user manual</span>

本页描述 queuebit v0.1 用户手册背后的 Redis keyspace、状态集合和原子迁移语义。用户通常不需要直接读写这些 key，但运维、排障和实现对齐都应以这里的语义为准。

## 设计边界

Redis 是首版唯一共享状态源。queuebit 不把进程内队列、内存锁或本地 timer 当作正式一致性来源。

Redis 模型必须支持：

- 多 producer 并发 enqueue。
- 多 worker 并发 claim。
- Worker crash 后 stalled recovery。
- Delayed job 与 retry 到期推进。
- Scheduler single-active。
- Metrics / introspection 查询。

## Keyspace 目标

目标 key 前缀建议：

```text
qb:{namespace}:{queue}:...
```

字段含义：

| 片段 | 说明 |
|------|------|
| `qb` | queuebit keyspace 标识 |
| `{namespace}` | 应用/环境/租户隔离 |
| `{queue}` | queue name |

若后续需要 Redis Cluster hash tag，必须在 key 设计中统一说明，避免原子操作跨 slot 失败。

## Redis 部署边界

Redis 部署支持状态必须公开可见，不能只写“Redis-compatible”：

| 部署形态 | v0.1 目标状态 | Redis 模型要求 |
|----------|---------------|----------------|
| standalone Redis | 基线目标 | 当前 keyspace 和原子迁移均以该形态作为最小验证目标 |
| 托管单主 Redis | 条件等价 | 只要命令、TTL、原子脚本/事务和连接语义与基线一致即可进入验证 |
| Sentinel / failover | 条件支持目标 | 必须覆盖断线重连、lease 续租失败和 scheduler 续期失败 |
| Redis Cluster | v0.1 不支持 | 后续支持前必须引入统一 hash tag，并证明所有 Lua/事务迁移不跨 slot |
| 多 Redis 后端 | 明确非目标 | 不允许把跨后端抽象混入 v0.1 Redis adapter |

如果实现暂不支持 Redis Cluster，配置 loader 和 CLI 必须在启动前给出明确错误，而不是让用户在运行中遇到 `CROSSSLOT` 或半完成状态迁移。

## 数据结构

| 目标 key | 类型 | 语义 |
|----------|------|------|
| `...:waiting` | list 或 stream | 等待 worker 声明的 job |
| `...:delayed` | sorted set | 按可执行时间排序的 delayed job |
| `...:retry` | sorted set | 按下一次 attempt 时间排序的 retry job |
| `...:active` | hash / set | 当前被 worker 声明的 job 与 worker identity |
| `...:lease:{jobId}` | string with TTL | active job lease token |
| `...:job:{jobId}` | hash | job 元数据、payload 引用、状态、attempts、错误摘要 |
| `...:events` | stream | 状态变化事件，用于调试和未来订阅 |
| `...:scheduler:{domain}` | string with TTL | scheduler 单活 token |
| `...:metrics` | hash / derived view | queue depth、active、retry、delayed 等观察数据 |

具体 Redis 类型可以根据原子性和性能权衡调整，但用户可见语义不能缺失。

## 状态迁移

| 迁移 | 触发者 | 原子性要求 |
|------|--------|------------|
| enqueue -> waiting | producer | job 元数据与 waiting 写入必须一致 |
| enqueue -> delayed | producer | job 元数据与 delayed score 必须一致 |
| waiting -> active | worker | claim 与 lease token 创建必须一致 |
| active -> completed | worker | lease token 校验与状态更新必须一致 |
| active -> retry | worker | attempt 增加、错误摘要、retry score 必须一致 |
| active -> failed | worker | terminal failure 与 active 移除必须一致 |
| delayed -> waiting | scheduler | 到期检查与 waiting 写入必须一致 |
| retry -> waiting | scheduler | 到期检查与重投递必须一致 |
| active -> stalled -> waiting | scheduler/recovery | lease 过期判断与重投递必须一致 |

## 原子性要求

以下操作不得拆成多个无保护命令：

- worker claim job 并创建 lease。
- ack completed 并移除 active 状态。
- fail retryable 并写入 retry schedule。
- scheduler promote delayed/retry。
- stalled recovery 重新投递。
- scheduler leadership 获取和续期。

实现可以使用 Lua、事务或 Redis 原生命令组合，但必须在测试中覆盖并发竞争。

## 保留策略

v0.1 至少要定义：

| 数据 | 目标策略 |
|------|----------|
| completed jobs | 默认可按数量或时间清理 |
| failed jobs | 默认保留更久，便于排查 |
| events | 可配置保留长度 |
| metrics | 可由实时 key 推导，不强制永久保存 |
| payload | 由实现决定内嵌或引用，但必须说明大小限制 |

## 实现验收

- Redis key 前缀必须包含 namespace 与 queue，避免环境串写。
- 所有状态迁移都能在 Redis 不确定或并发竞争下保持可恢复。
- Redis Cluster 支持与否必须明确，不得含糊。
- 运维文档中的指标必须能从 keyspace 或公开 API 获取。
- 测试必须覆盖 worker crash、lease 过期、scheduler 双实例竞争和 ack 丢失近似场景。
