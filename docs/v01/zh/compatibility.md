# 我的环境能不能用

<span class="manual-label">参考 · Node、Redis、vext 与工作负载支持范围</span>

第一次接入只需要跟着 [快速开始](./quick-start.md) 跑通。本页用于上线前确认：你的 Node 版本、Redis 拓扑、Web 框架和任务类型是否在 Queuebit v0.1 支持范围内。

## 一分钟判断

| 你现在的情况 | 能不能用 | 说明 |
|---|---|---|
| Node.js `>=20` | 可以 | core 要求 |
| vext 项目 Node.js `>=20.19` | 可以 | `queuebit/vext` 要求 |
| Redis `>=7.2` single-primary | 可以 | standalone、托管 Redis、TLS、Sentinel 都可以 |
| Redis Cluster | 不支持 | v0.1 不支持 cluster slot/多 primary |
| 普通后台任务 | 可以 | 用 `jobs.add` + Worker |
| 批量处理有限数据库记录 | 可以 | 用 `runs.start` + Coordinator |
| CDC、无限流、cron、DAG/Flow | 不支持 | 等后续版本明确发布 |
| 必须 exactly-once 或严格 FIFO | 不适合 | Queuebit 是 at-least-once |
| 任意 Node Web 框架 | 可以接 | 直接创建 client 并在后台启动 Worker |
| vext | 官方优先支持 | 首个官方宿主是 `vextjs@0.3.26` |

## 适合什么场景

- HTTP 请求先返回 202，耗时业务动作交给后台 Worker。
- 从数据库按页处理一批有限记录，并记录每批/最终完成。
- 多个 Worker 横向扩展，任一进程崩溃后可以被其他 Worker 接手。
- 邮件、支付、Webhook 或数据库写入已经能防止重复副作用。

## 不适合什么场景

- 必须使用 Redis Cluster、非 Redis 后端，或希望本地离线执行后再合并状态。
- 要求严格 FIFO、按 key 分区顺序、DAG/Flow、repeatable/cron、priority 或全局 rate limiter。
- 要求 Queuebit 自动撤销外部副作用，或承诺 exactly-once。
- 想把 Web 进程里的本地内存队列包装成分布式队列。

## 安装和环境检查

```bash
npm install queuebit
```

```bash
node --version
redis-cli INFO server
redis-cli INFO persistence
redis-cli CONFIG GET maxmemory-policy
```

生产 Redis 必须使用 `maxmemory-policy=noeviction`，启用符合你 RPO 的持久化/备份，并接受 Sentinel 异步复制在 failover 窗口内可能丢写。

## 启动前检查

```bash
npx queuebit config validate \
  --config queuebit.config.ts \
  --runtime queuebit.runtime.ts

npx queuebit health inspect --config queuebit.config.ts --json
```

| 结果 | 含义 | 动作 |
|---|---|---|
| `ready` | 当前角色和 Redis policy 满足要求 | 可以启动业务流程 |
| `degraded` | warn policy 或观测信息不完整 | 本地可继续，生产不要放行 |
| `not_ready` | 连接、strict policy、runtime registration 或角色资格失败 | 先修复，不强行启动 |

## 不要找这些旧能力

v0.1 不发布 standalone Scheduler，也没有 `scheduler start`、`scheduler inspect`、`scheduler drain`。时间推进由后台 Worker cooperative 完成。需要完全独立的时间推进进程时，等待后续版本明确发布，不要把旧草案当作兼容承诺。

## 下一步

- 第一次跑通：[快速开始](./quick-start.md)。
- 生产 Redis 和 Worker 参数：[配置 Redis 和 Worker](./configuration-recipes.md)。
- Redis 中断、failover 或数据丢失：[Redis 断了怎么办](./distributed-semantics.md)。
