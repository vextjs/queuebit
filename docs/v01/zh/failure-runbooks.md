# 故障恢复

<span class="manual-label">生产运维 · 先保住状态，再恢复任务</span>

## 先记住三条规则

| 规则 | 原因 |
|---|---|
| 不手改 Redis key | Queuebit 的状态、租约、重试和恢复信息是一组一致数据 |
| 重试时使用同一个业务 identity/idempotency key | 避免因为随机 key 造成重复邮件、支付或 Webhook |
| 先判断是哪类失败，再选恢复动作 | Redis 断连、Worker 崩溃、业务失败、completion 失败的恢复方式不同 |

## 事故中先抓这几条证据

```bash
npx queuebit health inspect --config queuebit.config.ts --json
npx queuebit queue inspect notification --config queuebit.config.ts --json
npx queuebit run inspect <runId> --config queuebit.config.ts --json
npx queuebit workers inspect --queue notification --config queuebit.config.ts --json
```

这些命令先用来“看清状态”，不要边查边手改 Redis。保留 UTC 时间线、角色 identity/version/config digest、关联 ID、Redis role/persistence/policy 和最近错误。先看证据，再决定恢复动作。

## 快速分流：按现象选恢复方式

| 现象 | 类型 | 正确恢复 |
|---|---|---|
| Producer/API 不能受理 | 配置、Redis、server policy、背压 | 修复原因，用同一个业务 identity 再提交 |
| 直接 Job failed | processor 业务失败 | 修复业务原因后创建 replacement job |
| Run `blocked` | 数据库读取、派发或 Redis 控制面失败 | 修复后 resume 原 Run |
| Run `partial_failed/failed` 且还保留失败详情 | mapper/processor 终止失败 | 创建 recovery run，只重做失败 work |
| execution 终态但 completion failed | 结果回写失败 | 只 retry completion event，不重做 job |
| Redis 原状态丢失 | 耐久性事故 | 从备份恢复，或从业务 DB 创建全新 Run |

## Redis 不可用或网络分区

**立即动作**：先不要扩大 Producer 重试流量。后台 Worker/Coordinator 会在无法确认租约时停止新领取、新分页和新派发；保留进程让它们持续重连。

**诊断**：

```bash
redis-cli PING
redis-cli INFO replication
redis-cli INFO persistence
redis-cli CONFIG GET maxmemory-policy
```

**恢复**：先恢复 primary 连接，让 Redis policy 检查通过；角色自动重连后核对是否出现新的 `leaseGeneration`、`stalledRecoveries`、Run cursor 变化和副作用去重结果。只有 Run 已进入 `blocked` 时才执行 `run resume`。

**停止条件**：Redis role 摇摆、persistence error、无法确认是否丢写或多个节点同时接受写入时，停止新工作并升级给 Redis 运维。

## Worker 崩溃或任务卡在 active

1. 查 Worker heartbeat、event loop、内存、CPU、Redis renew 延迟和下游 timeout。
2. 恢复至少一个 ready Worker，但不启动大量实例制造重试风暴。
3. 等待旧 Worker 的租约过期，观察 `stalledRecoveries` 和新的 `leaseGeneration`。它们表示任务被其他 Worker 接手。
4. 用业务 idempotency key 对账外部副作用。
5. `maxStalledRecoveries` 耗尽后进入 failed，BatchRun 用 recovery run，直接 job 用 replacement。

## 数据库批处理进入 blocked

```bash
npx queuebit run inspect <runId> --config queuebit.config.ts --json
```

| blocked reason | 核对 | 恢复 |
|---|---|---|
| source timeout/unavailable | DB 连接池、查询计划、snapshot token | 修复 source，用同一个 cursor 继续读，然后 resume |
| cursor not advanced | source 是否正确返回下一页位置 | 修复 loader，不手动改 cursor |
| dispatch retry exhausted | Redis 连接和提交是否恢复 | 恢复 Redis，重复提交会收敛到同一批 Batch/jobs |
| request too large | page、fan-out 或 payload 本身是否太大 | 提升 definition version，缩小负载，取消旧 Run 并创建新 Run |

blocked 是控制面故障，不使用 `run retry-failed`。

<span id="sc09-recovery-run"></span>
## 重做数据库批处理中失败的 work

```bash
npx queuebit run failures <runId> --limit 100 --config queuebit.config.ts
npx queuebit run retry-failed <runId> \
  --idempotency-key "recovery:<runId>:1" \
  --config queuebit.config.ts
```

recovery run 从 Queuebit 保存的失败详情读取，不重新查询当前数据库。mapper 阶段失败会重新执行 mapper；processor 阶段失败会沿用原 job data 和业务 idempotency key。原 Run 摘要和状态不会被改写。

| 失败 | 含义 | 动作 |
|---|---|---|
| `QB_RUN_STATE_CONFLICT` | retention 已清理已保存失败详情，或 Run 不可恢复 | 根据业务 DB 创建全新 Run |
| definition version 不可用 | 旧 runtime 没保留 | 恢复兼容 runtime，或显式选择已验证的新版本 |
| 业务数据已变且必须使用新值 | recovery run 不再符合你的业务语义 | 创建全新 Run，不假装是原快照恢复 |

## 结果回写 completion 失败

```bash
npx queuebit completion inspect --run <runId> --config queuebit.config.ts
npx queuebit completion retry <eventId> --config queuebit.config.ts
```

先修复审计 DB、Webhook 或下游 handler，再 retry 具体 event。Queuebit 会防止旧 owner 晚返回覆盖新状态，但 handler 对外部系统的写入仍要用 `event.id` 做幂等。

## Queue 背压或没有 active Worker

- `dispatchHoldReason=backpressure/no_active_worker` 是自动等待，不消耗 retry，也不应手动 resume。
- 先检查下游容量和 Worker 健康，再扩容；不在下游已过载时只增 Worker。
- 只有 jobs 和 bytes 都回到 low 或更低才自动恢复。
- 单请求过大需要改 page/bulk/fan-out/payload，等待不会让请求自己变小。

## 事故结束条件

- strict server policy 和 role readiness 均通过。
- 新 work 能接受，Queue waiting age 持续回落。
- Run 的已读位置、已创建 job、完成数量能对上。
- 无新 stalled/completion failed/server policy 错误增长。
- 副作用去重对账无重复结果。
- 记录原因、影响的 identity、恢复动作和后续预防项。
