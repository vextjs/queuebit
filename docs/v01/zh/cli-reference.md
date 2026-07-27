# CLI 参考

<span class="manual-label">参考 · 命令、JSON 和 exit code</span>

CLI 是辅助工具，不是接入 Queuebit 的前提。普通项目可以直接在 Node 框架里 `createQueuebitClient()`，也可以写一个普通 `.js/.mjs` 文件启动 Worker；CLI 主要用于本地验证、启动后台角色、inspect、drain 和故障恢复。

## 通用规则

- 每条命令接受 `--config queuebit.config.ts`；需要业务 handler 的角色还接受 `--runtime queuebit.runtime.ts`。
- CLI 自带 Node 20 可用的 TypeScript loader，不要求用户猜测安装 `tsx/ts-node`。
- 所有命令同时接受预编译 `.mjs` fallback，载入失败显示 loader、Node 版本和文件。
- inspect 默认表格，`--json` 提供稳定机器输出。
- 本页多行命令默认使用 Bash 反斜杠续行；PowerShell 请使用单行命令，或写一个 JS 启动文件。

## 启动角色

```bash
npx queuebit worker start --config queuebit.config.ts --runtime queuebit.runtime.ts --queue notification
npx queuebit coordinator start --config queuebit.config.ts --runtime queuebit.runtime.ts
```

v0.1 只提供 cooperative 时间推进，由后台 Worker 候选者竞争单活 owner，不启动独立 Scheduler。`scheduler start/inspect/drain` 不属于 v0.1 命令；传入时返回 exit code 2 与 `QB_CLI_COMMAND_UNSUPPORTED`，避免脚本误以为角色已启动。

在框架里接入时，也可以不用 CLI 启动 Web/API；Web 进程只负责调用 `jobs.add()` 或 `runs.start()`。Worker 可以用 CLI 启动，也可以用你自己的 Node 启动文件包装同一套配置和 runtime。

## Run 命令

```bash
npx queuebit run start receipt-campaign \
  --config queuebit.config.ts \
  --input-json '{"tenantId":"tenant-42","paidBefore":"2026-07-15T00:00:00.000Z"}' \
  --idempotency-key 'receipt-campaign:tenant-42:2026-07-15'

npx queuebit run inspect <runId> --config queuebit.config.ts
npx queuebit run list --definition receipt-campaign --state partial_failed --limit 100 --config queuebit.config.ts
npx queuebit run failures <runId> --stage mapper --limit 100 --config queuebit.config.ts
npx queuebit run pause <runId> --config queuebit.config.ts
npx queuebit run resume <runId> --config queuebit.config.ts
npx queuebit run cancel <runId> --reason 'campaign withdrawn' --config queuebit.config.ts
npx queuebit run retry-failed <runId> --idempotency-key 'recovery:<runId>:1' --config queuebit.config.ts
```

`retry-failed` 对 blocked Run 或没有已保存失败详情的 Run 拒绝。`--definition-version` 只在显式使用新 mapper/processor 修复时指定，并必须通过 input/schema/runtime 兼容验证。

## Job 命令

```bash
npx queuebit job inspect <jobId> --config queuebit.config.ts
npx queuebit job list --queue notification --state failed --limit 100 --config queuebit.config.ts
npx queuebit job cancel <jobId> --config queuebit.config.ts
npx queuebit job retry-failed <jobId> --deduplication-key 'replacement:<jobId>:1' --config queuebit.config.ts
```

## 运维查询

```bash
npx queuebit queue inspect notification --config queuebit.config.ts
npx queuebit workers inspect --queue notification --config queuebit.config.ts
npx queuebit coordinator inspect --config queuebit.config.ts
npx queuebit completion inspect --run <runId> --config queuebit.config.ts
npx queuebit completion retry <eventId> --config queuebit.config.ts
npx queuebit health inspect --config queuebit.config.ts --json
npx queuebit config validate --config queuebit.config.ts --runtime queuebit.runtime.ts
```

cooperative 时间 owner 的 domain、identity、generation、lease expiry 和最近 promotion 通过 health/queue/worker 视图暴露。

## Drain

```bash
npx queuebit worker drain --queue notification --worker-id worker-a --reason rolling-release --config queuebit.config.ts
npx queuebit coordinator drain --coordinator-id coordinator-a --reason rolling-release --config queuebit.config.ts
```

start 命令收到 SIGTERM 自动 drain。超时时停止续租并非零退出，不伪造 active work 已失败/取消。

## Exit code

| code | 含义 |
|---:|---|
| 0 | 成功；长运行角色完成优雅 drain |
| 1 | 操作失败或角色异常终止 |
| 2 | 参数、配置、runtime registration 或 loader 错误 |
| 3 | Redis/依赖暂不可用，调用方可根据 `retryable` 退避 |
| 4 | 当前 job/run/completion 状态不允许该控制操作 |

## JSON 契约

成功：

```json
{ "ok": true, "data": { "runId": "run_01..." } }
```

失败：

```json
{
  "ok": false,
  "error": {
    "code": "QB_RUN_STATE_CONFLICT",
    "message": "The run cannot be resumed from its current state.",
    "retryable": false,
    "operation": "runs.resume",
    "details": { "runId": "run_01..." }
  }
}
```

`runId` 等结构化字段在存在时放在 `error.details` 下。`--json` 机器结果写 stdout，诊断日志写 stderr，不污染 JSON。未知异常映射 `QB_INTERNAL`，不输出 stack 或任意 cause。

## PowerShell 使用

多行 Bash 只使用反斜杠续行时，PowerShell 用户应使用单行命令，或写普通 Node 启动脚本；不需要 CI 才能执行。例如：

```powershell
npx queuebit run inspect $env:RUN_ID --config queuebit.config.ts --json
```
