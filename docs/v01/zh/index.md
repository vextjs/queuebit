---
pageType: home
hero:
  name: queuebit
  text: Redis-backed Node 任务队列
  tagline: 安装模块，传入 Redis 配置，注册一个 processor；业务代码调用 jobs.add，把耗时动作交给后台 Worker。
  actions:
    - theme: brand
      text: 先跑一个后台任务
      link: /zh/quick-start.html
    - theme: alt
      text: 看单任务用法
      link: /zh/job-recipes.html
    - theme: alt
      text: 看学习路径
      link: /zh/concepts.html
features:
  - title: 第一个任务只需要五步
    details: 安装、配置 Redis、写 processor、调用 jobs.add、启动 Worker 后 inspect job。
    link: /zh/quick-start.html
  - title: 普通任务只用 jobs.add
    details: Web/API 提交一个 payload，Worker 执行同名 processor；先不学 BatchRun。
    link: /zh/job-recipes.html
  - title: 常用能力逐步打开
    details: 需要时再加重试、超时、延时、幂等和取消，不挡第一次接入。
    link: /zh/concepts.html
  - title: 批量数据库记录是进阶场景
    details: 只有需要持续分页、每批完成、最终完成和恢复时，才使用 BatchRun。
    link: /zh/batch-runs.html
  - title: 多 Worker 是扩容手段
    details: 单 Worker 跑通后，再用多个进程共享 Redis 和 queue 提高吞吐。
    link: /zh/distributed-workers.html
  - title: vext 只是一个宿主
    details: vext Web 创建任务；Worker 仍然是独立后台进程。
    link: /zh/vext-integration.html
  - title: 出问题再看运维
    details: 生产部署、容量、告警、故障恢复放在生产运维组，不挡首次接入路径。
    link: /zh/failure-runbooks.html
---

<span class="manual-label">首页 · v0.1 用户手册</span>

> **发布状态：** 本站描述 v0.1 计划提供的使用方式。安装前请先核对当前 npm 包版本和 README 发布说明；如果示例提示能力尚未发布，以提示为准。
