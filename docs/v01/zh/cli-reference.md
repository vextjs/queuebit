# CLI 参考

<span class="manual-label">参考 · 命令、JSON 和 exit code</span>

CLI 是可选的兼容和运维工具，不是 Queuebit 的默认运行时入口。正常接入时，应用代码调用 `createQueuebitClient()`、`client.createWorker()`，以及仅在 BatchRun 场景调用 `client.createCoordinatorRunner()`；进程管理器和关闭钩子由应用自己选择。CLI 适合本地验证、inspect、远程 drain、故障恢复，或明确选择 CLI 作为服务可执行文件的兼容场景。

## 默认接入：从应用代码启动角色

```ts
import {
  createQueuebitClient,
  createQueuebitRuntimeProcessor
} from 'queuebit';
import config from './queuebit.config.js';
import runtime from './queuebit.runtime.js';

const queuebit = await createQueuebitClient({ config });

const worker = queuebit.createWorker(
  'notification',
  createQueuebitRuntimeProcessor(runtime),
  { workerId: 'worker-a', concurrency: 8 }
);
worker.start();

// 只有推进 BatchRun 的独立服务宿主才创建它。
const coordinator = queuebit.createCoordinatorRunner(runtime, {
  coordinatorId: 'coordinator-a',
  concurrency: 2,
  onError: event => console.error('Queuebit coordinator error', event)
});
coordinator.start();

// 从服务宿主自己的 shutdown 生命周期调用。
await queuebit.close({ timeoutMs: 60_000 });
```

生产环境中 Worker 和 Coordinator 应运行在不同服务宿主里；上例把两个 factory 放在一起，只是为了展示公开 API。Queuebit 没有 import 时副作用，也不会注册 signal handler。将 `onError` 接到应用已有 logger，并用 `coordinator.status().lastError` 监控 CoordinatorRunner。

## 通用规则

- 每条命令接受 `--config queuebit.config.ts`；需要业务 handler 的角色还接受 `--runtime queuebit.runtime.ts`。
- CLI 自带 Node 20 可用的 TypeScript loader，不要求用户猜测安装 `tsx/ts-node`。
- 所有命令同时接受预编译 `.mjs` fallback，载入失败显示 loader、Node 版本和文件。
- inspect 默认表格，`--json` 提供稳定机器输出。
- 本页多行命令默认使用 Bash 反斜杠续行；PowerShell 请使用单行命令，或写一个 JS 启动文件。

## 可选的 CLI 角色宿主

只有在你明确希望把 Queuebit CLI 作为后台服务可执行文件时才使用这些命令。它们是上方代码的兼容替代，不是框架接入的必需项。

```bash
npx queuebit worker start --config queuebit.config.ts --runtime queuebit.runtime.ts --queue notification
npx queuebit coordinator start --config queuebit.config.ts --runtime queuebit.runtime.ts
```

v0.1 只提供 cooperative 时间推进，由后台 Worker 候选者竞争单活 owner，不启动独立 Scheduler。`scheduler start/inspect/drain` 不属于 v0.1 命令；传入时返回 exit code 2 与 `QB_CLI_COMMAND_UNSUPPORTED`，避免脚本误以为角色已启动。

框架接入时，Web/API、Worker 和 Coordinator 都不需要 CLI 启动。Web/API 代码调用 `jobs.add()` 或 `runs.start()`；应用自己的服务宿主负责构造、启动和关闭 Worker/Coordinator 对象。

## Run 命令（手工或运维用途）

正常业务代码在服务端推导 tenant 和业务输入后调用 `queuebit.runs.start(...)` 创建 BatchRun。`run start` 用于本地/手工恢复或运维测试，不是常规请求链路的接入方式。

```ts
await queuebit.runs.start('receipt-campaign', {
  input: { tenantId: actor.tenantId, paidBefore: request.paidBefore },
  idempotencyKey: `receipt:${actor.tenantId}:${request.paidBefore}`
});
```

```bash
TENANT_ID='<已授权事件记录中的 tenant>'
PAID_BEFORE='<已批准的 ISO-8601 campaign boundary>'

npx queuebit run start receipt-campaign \
  --config queuebit.config.ts \
  --input-json "{\"tenantId\":\"${TENANT_ID}\",\"paidBefore\":\"${PAID_BEFORE}\"}" \
  --idempotency-key "receipt:${TENANT_ID}:${PAID_BEFORE}"

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

可选 CLI 角色宿主收到 SIGTERM 时自动 drain。SDK Worker/CoordinatorRunner 只有在服务宿主调用 `drain()` 或 `queuebit.close()` 时才 drain。超时时角色停止续租并报告失败，不伪造 active work 已失败/取消。

## Exit code

| code | 含义 |
|---:|---|
| 0 | 成功；可选的长运行 CLI 角色完成优雅 drain |
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
