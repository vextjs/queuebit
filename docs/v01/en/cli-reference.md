# CLI reference

<span class="manual-label">Reference · commands, JSON, and exit codes</span>

The CLI is a helper, not a requirement for using Queuebit. A normal app can call `createQueuebitClient()` from a Node framework, or start Workers from a plain `.js/.mjs` file. Use the CLI for local validation, background role startup, inspect, drain, and recovery.

## Common rules

- Every command accepts `--config queuebit.config.ts`; roles that execute business handlers also accept `--runtime queuebit.runtime.ts`.
- The CLI provides a Node 20-compatible TypeScript loader; users do not guess whether to install `tsx` or `ts-node`.
- Every command also accepts a precompiled `.mjs` fallback. A load failure reports loader, Node version, and file.
- Inspect commands use tables by default and stable machine output with `--json`.
- When Bash examples use backslash continuation, PowerShell users should use one line or write a JS startup file.

## Start roles

```bash
npx queuebit worker start --config queuebit.config.ts --runtime queuebit.runtime.ts --queue notification
npx queuebit coordinator start --config queuebit.config.ts --runtime queuebit.runtime.ts
```

v0.1 provides cooperative time advancement only: background Workers compete for one effective owner, and no separate Scheduler starts. `scheduler start`, `scheduler inspect`, and `scheduler drain` are not v0.1 commands; invoking them returns exit code 2 with `QB_CLI_COMMAND_UNSUPPORTED` so automation cannot mistake them for a running role.

Framework integrations do not need the CLI for Web/API startup. Web processes call `jobs.add()` or `runs.start()`. Workers may be started with the CLI or with your own Node startup file that loads the same config and runtime.

## Run commands

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

A start command drains automatically on SIGTERM. On timeout it stops lease renewal and exits non-zero without inventing failed or cancelled business state.

## Exit codes

| Code | Meaning |
|---:|---|
| 0 | Success, or a long-running role completed graceful drain |
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
