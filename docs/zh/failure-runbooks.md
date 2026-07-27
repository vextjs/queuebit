# 故障处置手册

<!-- queuebit-v01-legacy-doc -->
> [!WARNING]
> **历史文档，已停止维护。** 当前 v0.1 最终用户手册位于 [`docs/v01/zh`](../v01/zh/index.md)。本页仅保留历史上下文，API、命令、配置和示例不得用于新接入或实现。

<span class="manual-label">按现象执行的 Runbook</span>

本页用于线上操作。先保存时间范围、namespace、queue、部署版本和告警截图，再执行只读 inspect；不要直接改 Redis key。概念解释见 [故障模式与恢复](./failure-modes.md)。

## 先收集三份证据

```bash
npx queuebit inspect queue notification --config queuebit.config.ts
npx queuebit inspect workers --queue notification --config queuebit.config.ts
npx queuebit inspect scheduler --domain billing-notification --config queuebit.config.ts
```

同时查询 Producer 入队错误、Worker job/attempt 日志、Scheduler identity/renew 日志。若 Redis 不可连接，保留客户端连接错误和 Redis/Sentinel 事件时间线。

## 按状态快速分流

| 现象 | 首要检查 | 先不要做 |
|------|----------|----------|
| waiting 持续增长 | Worker 心跳、drain、queue/namespace、下游容量 | 重复提交同一批 job |
| active 长时间不变 | Worker/lease、handler 耗时、Redis 延迟 | 删除 active key |
| delayed/retrying 到期不动 | active Scheduler、domain、系统时间 | 启动不同 domain 的 Scheduler |
| failed 突增 | 最终错误分类、下游状态、最近发布 | 原样无限 replay |
| stalled 突增 | Worker 崩溃、续租、GC、Redis 网络 | 假设全部 job 都没产生副作用 |

<span id="s15-redis-outage"></span>
## S15 Redis 不可用

**触发：** 连接错误持续、renew 失败、Producer 入队失败，或所有角色同时失去 Redis。

**立即动作：**

1. 停止发布和扩容操作，确定故障开始时间及影响 namespace。
2. 让 Worker 按客户端退避停止拉新；不要在本地内存临时排队冒充成功。
3. 暂停或拒绝依赖“已入队”的上游请求，Producer 只有收到 `add/addBulk` 成功结果才能确认提交。
4. 检查 Redis 服务状态、DNS、网络、认证、TLS 和连接数；不要清空 keyspace。
5. 对 active job 将结果标记为“不确定”，按业务幂等记录核对副作用。

**恢复后验证：**

1. 先确认 Redis 单主身份稳定，再恢复 Producer 流量。
2. 确认 Worker 心跳恢复，且没有持续 renew 错误。
3. 确认恰好一个 active Scheduler。
4. 观察 stalled recovery、retrying、failed 和最老 waiting 时长直到回落。
5. 抽查故障窗口内业务键，确认没有丢失或重复副作用。

**升级条件：** 主节点频繁切换、恢复后仍持续 lease mismatch、业务结果无法核对，或 waiting 预计无法在 SLO 内清空。

<span id="s16-sentinel-failover"></span>
## S16 Sentinel 主从切换

**触发：** Sentinel 报告 master change，客户端重新发现主节点，期间出现短暂连接/只读错误。

**预期行为：** Producer 可能短暂失败；Worker 停止新 claim，续租不确定的 job 等恢复；Scheduler 无法证明资格时停止推进。旧主和新主稳定后客户端重连，job 通过 retry/lease/stalled recovery 收敛。

**操作步骤：**

1. 从 Sentinel 确认 master 名称与新主地址，核对配置中的 `sentinel.name`。
2. 确认应用没有直接缓存旧主地址，也没有连接 Redis Cluster endpoint。
3. 查看 Scheduler active identity；切换窗口内零 active 可以接受，双 active 不可接受。
4. 等连接稳定后执行三条 inspect，并记录 stalled/retrying 增量。
5. 用业务键核对切换窗口内成功但 ack 不确定的 job。

**停止恢复并升级：** Sentinel 视图不一致、客户端在两个主节点间振荡、检测到 split-brain、出现持续双 active Scheduler，或 Redis 数据不再单调可见。

## Worker 崩溃或 active 卡住

1. 确认对应 Worker identity 是否仍有心跳，进程是否被 OOM/平台终止。
2. 心跳消失时等待 lease 到期和 active Scheduler 恢复，不重复提交。
3. 心跳存在时检查 handler p99、下游调用、`timeoutMs` 和事件循环阻塞。
4. 恢复完成后关联旧/新 attempt，并按业务幂等键核对副作用。

如果 active 长于 `leaseMs` 且无 stalled recovery，优先排查 Scheduler，而不是只重启 Worker。

## Delayed 或 retrying 不推进

1. 确认系统时间和 job 计划时间。
2. 检查 Scheduler 候选是否运行、domain/namespace 是否一致。
3. 必须看到一个 active identity；零 active 时恢复候选，多个 active 时停止推进并升级。
4. 恢复后确认到期 job 从 delayed/retrying 进入 waiting，随后被 Worker 处理。

## Failed 突增

1. 按错误类型聚合：配置、认证、限流、业务校验、模板/数据、`HandlerTimeoutError`。
2. 对比最近发布和下游事件，确认是系统性还是单条坏数据。
3. 可恢复错误修复下游后让现有 retry 继续；终止失败按 [S05 处理终止失败](./job-recipes.md#s05-terminal-failed)。
4. 不支持内置 DLQ/manual retry；由业务管理入口审计后重新提交。

## Drain 超时

1. 保持其他 Worker 健康，停止该实例新 claim。
2. 查看 active job 的开始时间、handler 阶段和 `ctx.signal` 使用情况。
3. 在平台终止宽限期内等待；到期后允许 lease/recovery 接管。
4. 下一次发布前拆分长任务，或基于实测 p99 调整 timeout、lease 和 drain。

## 事件结束标准

- Redis 单主与连接稳定。
- Worker 心跳和恰好一个 active Scheduler 恢复。
- waiting 最老时长、failed/stalled 增量回到业务基线。
- 故障窗口内业务副作用完成去重核对。
- 时间线记录根因、恢复动作、受影响 job/业务键和后续预防项。
