---
pageType: home
hero:
  name: queuebit
  text: Redis-only 分布式任务队列
  tagline: 面向 Node.js 与 vext 项目。把业务数据批量放入 Redis 队列，由独立 Worker 可靠执行，并在失败后重试与恢复。
  actions:
    - theme: brand
      text: 5 分钟跑通
      link: /zh/quick-start.html
    - theme: alt
      text: 接入 vext
      link: /zh/vext-integration.html
    - theme: alt
      text: 检查兼容性
      link: /zh/compatibility.html
features:
  - title: 跑通第一批任务
    details: 读取真实业务数据，用 addBulk 批量入队，启动 Worker 并核对业务结果。
    link: /zh/quick-start.html
  - title: 实现常见任务流程
    details: 直接使用延迟、重试、并发、事件和指标的完整配方。
    link: /zh/job-recipes.html
  - title: 运行分布式 Worker
    details: 扩容 Worker、安全发布、恢复崩溃并运行单活 Scheduler。
    link: /zh/distributed-workers.html
  - title: 接入 vext
    details: 明确拆分 Producer、Worker 与 Scheduler 进程职责。
    link: /zh/vext-integration.html
  - title: 处理线上故障
    details: 按现象执行 Redis、Worker、Scheduler 和终止失败处置步骤。
    link: /zh/failure-runbooks.html
  - title: 查询精确契约
    details: 查找 API、CLI、配置、兼容性和不支持能力的边界。
    link: /zh/reference.html
---

<!-- queuebit-v01-legacy-doc -->
> [!WARNING]
> **历史文档，已停止维护。** 当前 v0.1 最终用户手册位于 [`docs/v01/zh`](../v01/zh/index.md)。本页仅保留历史上下文，API、命令、配置和示例不得用于新接入或实现。
