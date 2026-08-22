# 开发护栏

<!-- queuebit-v01-legacy-doc -->
> [!WARNING]
> **历史文档，已停止维护。** 当前 v0.1 最终用户手册位于 [`docs/v01/zh`](../v01/zh/index.md)。本页仅保留历史上下文，API、命令、配置和示例不得用于新接入或实现。

## 目的

<span class="manual-label">Maintainer / Internals</span>

本页回答一个问题：后续开发如何围绕文档推进，避免实现偏离。

queuebit 的文档站不是实现进度说明，而是 v0.1 最终用户手册和工程验收入口。开发 runtime、CLI、Redis adapter、vext adapter、测试和运维能力时，都必须回到这些文档核对。

## 文档真相源

| 文档 | 作用 |
|------|------|
| [架构说明](./architecture.md) | 定义 core/adapter、Redis-only、分布式拓扑边界 |
| [API 参考](./target-api.md) | 定义公开 API 语义和生命周期 |
| [CLI 与配置](./cli-and-config.md) | 定义配置、命令、进程入口和 vext 配置关系 |
| [Redis 模型](./redis-model.md) | 定义 keyspace、状态迁移和原子性 |
| [Worker 与 Scheduler 生命周期](./worker-lifecycle.md) | 定义 producer/worker/scheduler 运行路径 |
| [故障模式与恢复](./failure-modes.md) | 定义 failure mode、排查和幂等要求 |
| [运维与排查](./operations.md) | 定义 metrics / introspection 和排查路径 |

## 开发前检查

任何实现任务开始前，先做：

| 检查 | 通过标准 |
|------|----------|
| 范围定位 | 明确本次改动对应哪些手册页 |
| 术语一致 | Queue、Job、Worker、Scheduler、Lease、Domain 等术语不另起含义 |
| 手册验收 | 代码行为满足 quick start、API、CLI、vext、operations 中的用户路径 |
| Core/adapter 边界 | vext adapter 不把 core 变成 vext-only |
| Redis-only | 不引入其他 broker 或数据库后端 |
| 分布式语义 | 多实例、lease、scheduler single-active、recovery 有处理路线 |

## 实现对齐矩阵

| 开发项 | 必须同步的文档 | 必须验证 |
|--------|----------------|----------|
| Queue/Producer API | target-api、quick-start、reference | enqueue、delayed、idempotency、status 查询 |
| Worker runtime | target-api、worker-lifecycle、failure-modes | claim、renew、ack/fail、drain、crash recovery |
| Scheduler runtime | worker-lifecycle、redis-model、operations | single-active、delayed promotion、retry、stalled |
| Redis keyspace | redis-model、operations | 原子迁移、namespace 隔离、并发竞争 |
| CLI/config | cli-and-config、quick-start、reference | 配置校验、命令帮助、进程拓扑 |
| vext adapter | vext-integration、architecture、cli-and-config | producer/worker 分离、reload drain、metrics |
| Metrics | operations、failure-modes、reference | queue depth、active、retry、delayed、stalled、identity |

## 文档变更规则

- 如果实现发现手册不合理，先更新对应用户手册页并说明原因，再改实现。
- 如果实现只是补齐手册能力，必须用 quick start、API、CLI、vext、operations 示例做验收。
- 如果公开 API、CLI、配置字段或 Redis key 命名变化，必须同步 README、reference、quick start 和对应手册页。
- 如果英文页未同步，不得只更新中文页后结束。
- 不把 DevCodex 报告、内部台账或维护者 checklist 写入公开用户主路径。

## 本地运行文档站

从仓库根目录运行：

```bash
npm install --prefix website
npm run docs:dev
npm run docs:build
npm run docs:preview
```

`npm run docs:preview` 会构建站点并提供稳定交付地址 `http://localhost:4180/queuebit/`；中文页面位于 `http://localhost:4180/queuebit/zh/`。`npm run docs:dev` 会在 `http://127.0.0.1:4181/queuebit/` 提供同样的生成站点预览。`npm run docs:edit` 只用于编辑文档，地址固定为 `http://127.0.0.1:4182/queuebit/`。

也可以进入 `website/`，使用 `npm run dev`、`npm run build`、`npm run preview`。根包会固定本地文档端口，文档依赖由 `website/` 管理。

## 验收路线

文档阶段的验证路线：

- `npm --prefix website run build`
- 多语言页面结构和导航一致性检查
- 文档站 smoke，确认新页面可访问
- 搜索用户主路径是否残留实现进度说明主叙事

进入 runtime 阶段后，需要追加：

- 单元测试：状态机、配置校验、错误分类。
- 集成测试：Redis keyspace、并发 claim、lease 过期、scheduler 单活。
- 场景测试：worker crash、Redis 短暂不可用、drain timeout、ack 丢失近似。
- package boundary：确认 npm 包只发布预期文件。
