# CLI reference

<span class="manual-label">Reference · commands, JSON, and exit codes</span>

The CLI is optional compatibility and operations tooling, not Queuebit's normal runtime entrypoint. In a normal integration, application code calls `createQueuebitClient()`, `client.createWorker()`, and, for BatchRun, `client.createCoordinatorRunner()`. The application chooses its process manager and shutdown hook. Use the CLI for local validation, inspect, remote drain, recovery, or a deliberate CLI-host compatibility choice.

## Default integration: start roles from application code

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

// Create this only in the separate service host that advances BatchRuns.
const coordinator = queuebit.createCoordinatorRunner(runtime, {
  coordinatorId: 'coordinator-a',
  concurrency: 2,
  onError: event => console.error('Queuebit coordinator error', event)
});
coordinator.start();

// Call from the host application's own shutdown lifecycle.
await queuebit.close({ timeoutMs: 60_000 });
```

Run Workers and Coordinators in separate service hosts in production; the example places both factory calls together only to show their public API. Queuebit has no import-time or signal-handler side effects. Attach `onError` to your application's logger and inspect `coordinator.status().lastError` when monitoring a CoordinatorRunner.

## Common rules

- Every command accepts `--config queuebit.config.ts`; roles that execute business handlers also accept `--runtime queuebit.runtime.ts`.
- The CLI provides a Node 20-compatible TypeScript loader; users do not guess whether to install `tsx` or `ts-node`.
- Every command also accepts a precompiled `.mjs` fallback. A load failure reports loader, Node version, and file.
- Inspect commands use tables by default and stable machine output with `--json`.
- When Bash examples use backslash continuation, PowerShell users should use one line or write a JS startup file.

## Optional CLI role hosts

Use these commands only when you deliberately want the Queuebit CLI to be the executable of a background service. They are compatible alternatives to the code above, not a framework-integration requirement.

```bash
npx queuebit worker start --config queuebit.config.ts --runtime queuebit.runtime.ts --queue notification
npx queuebit coordinator start --config queuebit.config.ts --runtime queuebit.runtime.ts
```

v0.1 provides cooperative time advancement only: background Workers compete for one effective owner, and no separate Scheduler starts. `scheduler start`, `scheduler inspect`, and `scheduler drain` are not v0.1 commands; invoking them returns exit code 2 with `QB_CLI_COMMAND_UNSUPPORTED` so automation cannot mistake them for a running role.

Framework integrations do not need the CLI for Web/API, Worker, or Coordinator startup. Web/API code calls `jobs.add()` or `runs.start()`; its own service hosts construct, start, and close the Worker/Coordinator objects.

## Run commands (manual or operator use)

Normal business code starts a BatchRun with `queuebit.runs.start(...)` after it derives tenant and business input on the server. `run start` is for local/manual recovery or operator testing, not the normal request-path integration.

```ts
await queuebit.runs.start('receipt-campaign', {
  input: { tenantId: actor.tenantId, paidBefore: request.paidBefore },
  idempotencyKey: `receipt:${actor.tenantId}:${request.paidBefore}`
});
```

```bash
TENANT_ID='<tenant from the authorized incident record>'
PAID_BEFORE='<approved ISO-8601 campaign boundary>'

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

`retry-failed` rejects a blocked Run or a Run without saved failure details. Use `--definition-version` only when a new mapper/processor version intentionally repairs old work and input, schema, and runtime compatibility have been verified.

## Job commands

```bash
npx queuebit job inspect <jobId> --config queuebit.config.ts
npx queuebit job list --queue notification --state failed --limit 100 --config queuebit.config.ts
npx queuebit job cancel <jobId> --config queuebit.config.ts
npx queuebit job retry-failed <jobId> --deduplication-key 'replacement:<jobId>:1' --config queuebit.config.ts
```

## Operational inspection

```bash
npx queuebit queue inspect notification --config queuebit.config.ts
npx queuebit workers inspect --queue notification --config queuebit.config.ts
npx queuebit coordinator inspect --config queuebit.config.ts
npx queuebit completion inspect --run <runId> --config queuebit.config.ts
npx queuebit completion retry <eventId> --config queuebit.config.ts
npx queuebit health inspect --config queuebit.config.ts --json
npx queuebit config validate --config queuebit.config.ts --runtime queuebit.runtime.ts
```

Cooperative owner domain, identity, generation, lease expiry, and last promotion appear through health, queue, and Worker views.

## Drain

```bash
npx queuebit worker drain --queue notification --worker-id worker-a --reason rolling-release --config queuebit.config.ts
npx queuebit coordinator drain --coordinator-id coordinator-a --reason rolling-release --config queuebit.config.ts
```

An optional CLI role host drains automatically on SIGTERM. SDK Workers and CoordinatorRunners drain only when their host calls `drain()` or `queuebit.close()`. On timeout, the role stops lease renewal and reports failure without inventing failed or cancelled business state.

## Exit codes

| Code | Meaning |
|---:|---|
| 0 | Success, or an optional long-running CLI role completed graceful drain |
| 1 | Operation failed or role terminated abnormally |
| 2 | Argument, configuration, runtime registration, or loader error |
| 3 | Redis/dependency temporarily unavailable; caller may back off using `retryable` |
| 4 | Current job, Run, or completion state rejects the control action |

## JSON contract

Success:

```json
{ "ok": true, "data": { "runId": "run_01..." } }
```

Failure:

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

Structured fields such as `runId` or validation paths appear under `error.details` when present.

With `--json`, machine output goes to stdout and diagnostics go to stderr. Unknown exceptions map to `QB_INTERNAL` without stack or arbitrary cause.

## PowerShell

When a Bash example uses backslash continuation, PowerShell users should use one line or a plain Node startup script; CI is not required:

```powershell
npx queuebit run inspect $env:RUN_ID --config queuebit.config.ts --json
```
