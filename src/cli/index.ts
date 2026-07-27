import { createRequire } from 'node:module';
import process from 'node:process';
import { setTimeout as delay } from 'node:timers/promises';
import { resolve } from 'node:path';
import { createQueuebitClient, type QueuebitClient } from '../client';
import {
  defineQueuebitConfig,
  type QueuebitConfig,
  type QueuebitUserConfig
} from '../config';
import { QueuebitError, type QueuebitErrorCode } from '../errors';
import type { CompletionListQuery } from '../completions';
import type { JobState } from '../jobs';
import type { RunExecutionState } from '../runs';
import {
  defineQueuebitRuntime,
  type QueuebitProcessor,
  type QueuebitRuntimeDefinition
} from '../runtime';
import { loadQueuebitModule, type LoadedQueuebitModule } from './loader';

type CliFlagValue = string | boolean;

interface ParsedArgs {
  positionals: string[];
  flags: Record<string, CliFlagValue>;
}

interface CliIo {
  stdout: Pick<NodeJS.WriteStream, 'write'>;
  stderr: Pick<NodeJS.WriteStream, 'write'>;
}

interface CliSuccessEnvelope {
  ok: true;
  data: unknown;
}

interface CliErrorEnvelope {
  ok: false;
  error: {
    code: string;
    message: string;
    retryable: boolean;
    operation: string;
    details?: unknown;
  };
}

const terminalRunStates = new Set<RunExecutionState>(['completed', 'partial_failed', 'failed', 'cancelled']);
const jobStates: JobState[] = ['waiting', 'active', 'delayed', 'retrying', 'completed', 'failed', 'cancelled'];

export async function runQueuebitCli(
  argv: string[] = process.argv.slice(2),
  io: CliIo = { stdout: process.stdout, stderr: process.stderr }
): Promise<number> {
  const parsed = parseArgs(argv);
  const json = parsed.flags.json === true;
  const operation = parsed.positionals.join('.') || 'help';
  try {
    const data = await dispatch(parsed, io);
    if (data !== undefined) writeSuccess(io, data, json);
    return 0;
  } catch (cause) {
    const envelope = serializeCliError(cause, operation);
    if (json) {
      io.stdout.write(`${JSON.stringify(envelope)}\n`);
    } else {
      io.stderr.write(`${envelope.error.code}: ${envelope.error.message}\n`);
      if (envelope.error.details !== undefined) {
        io.stderr.write(`${JSON.stringify(envelope.error.details)}\n`);
      }
    }
    return exitCodeFor(envelope.error.code);
  }
}

async function dispatch(parsed: ParsedArgs, io: CliIo): Promise<unknown> {
  const [resource, action] = parsed.positionals;
  // Version before help/undefined-resource so `queuebit --version` prints the package version.
  if (parsed.flags.version === true || resource === 'version') {
    io.stdout.write(`${readPackageVersion()}\n`);
    return undefined;
  }
  if (parsed.flags.help === true || resource === undefined) {
    io.stdout.write(renderHelp());
    return undefined;
  }

  if (resource === 'scheduler') {
    throw unsupported('scheduler', action ?? 'unknown');
  }

  if (resource === 'config' && action === 'validate') return validateConfigCommand(parsed);
  if (resource === 'health' && action === 'inspect') {
    return withClient(parsed, async client => client.health.snapshot());
  }
  if (resource === 'run') return runCommand(action, parsed);
  if (resource === 'job') return jobCommand(action, parsed);
  if (resource === 'completion') return completionCommand(action, parsed);
  if (resource === 'queue' && action === 'inspect') return queueInspectCommand(parsed);
  if (resource === 'workers' && action === 'inspect') return workersInspectCommand(parsed);
  if (resource === 'coordinator' && action === 'inspect') return coordinatorInspectCommand(parsed);
  if (resource === 'worker' && action === 'drain') return workerDrainCommand(parsed);
  if (resource === 'coordinator' && action === 'drain') return coordinatorDrainCommand(parsed);
  if (resource === 'worker' && action === 'start') return workerStartCommand(parsed, io);
  if (resource === 'coordinator' && action === 'start') return coordinatorStartCommand(parsed, io);

  throw unsupported(resource, action ?? 'unknown');
}

async function validateConfigCommand(parsed: ParsedArgs): Promise<unknown> {
  const { config, loadedConfig } = await loadConfig(parsed);
  const runtimeResult = await loadRuntimeIfPresent(parsed);
  const runtime = runtimeResult?.runtime;
  const validation = runtime === undefined
    ? { runtime: 'not_loaded' }
    : validateRuntimeRegistrations(config, runtime);
  return {
    message: 'Queuebit configuration is valid.',
    config: {
      file: loadedConfig.path,
      loader: loadedConfig.loader,
      namespace: config.namespace,
      queues: Object.keys(config.queues),
      batchRuns: Object.keys(config.batchRuns)
    },
    validation
  };
}

async function runCommand(action: string | undefined, parsed: ParsedArgs): Promise<unknown> {
  if (action === 'start') {
    const definition = requiredPositional(parsed, 2, 'definition');
    const input = parseJson(requiredStringFlag(parsed, 'input-json'), 'input-json');
    const idempotencyKey = requiredStringFlag(parsed, 'idempotency-key');
    return withClient(parsed, async client => client.runs.start(definition, { input, idempotencyKey }));
  }
  if (action === 'inspect') {
    const runId = requiredPositional(parsed, 2, 'runId');
    return withClient(parsed, async client => {
      const run = await client.runs.get(runId);
      if (run === null) throw notFound('QB_RUN_NOT_FOUND', 'Run does not exist.', { runId });
      return run;
    });
  }
  if (action === 'list') {
    const query: { definition?: string; executionState?: RunExecutionState; limit?: number; cursor?: string } = {};
    assignOptional(query, 'definition', optionalStringFlag(parsed, 'definition'));
    assignOptional(query, 'executionState', optionalStringFlag(parsed, 'state') as RunExecutionState | undefined);
    assignOptional(query, 'limit', optionalIntegerFlag(parsed, 'limit'));
    assignOptional(query, 'cursor', optionalStringFlag(parsed, 'cursor'));
    return withClient(parsed, async client => client.runs.list(query));
  }
  if (action === 'failures') {
    const runId = requiredPositional(parsed, 2, 'runId');
    const query: { stage?: 'mapper' | 'processor'; limit?: number; cursor?: string; includePayload?: boolean } = {};
    assignOptional(query, 'stage', optionalStringFlag(parsed, 'stage') as 'mapper' | 'processor' | undefined);
    assignOptional(query, 'limit', optionalIntegerFlag(parsed, 'limit'));
    assignOptional(query, 'cursor', optionalStringFlag(parsed, 'cursor'));
    if (parsed.flags['include-payload'] === true) query.includePayload = true;
    return withClient(parsed, async client => client.runs.listFailures(runId, query));
  }
  if (action === 'pause' || action === 'resume') {
    const runId = requiredPositional(parsed, 2, 'runId');
    return withClient(parsed, async client => client.runs[action](runId));
  }
  if (action === 'cancel') {
    const runId = requiredPositional(parsed, 2, 'runId');
    const reason = requiredStringFlag(parsed, 'reason');
    return withClient(parsed, async client => client.runs.cancel(runId, { reason }));
  }
  if (action === 'retry-failed') {
    const runId = requiredPositional(parsed, 2, 'runId');
    const request: { idempotencyKey: string; definitionVersion?: number } = {
      idempotencyKey: requiredStringFlag(parsed, 'idempotency-key')
    };
    assignOptional(request, 'definitionVersion', optionalIntegerFlag(parsed, 'definition-version'));
    return withClient(parsed, async client => client.runs.retryFailed(runId, request));
  }
  throw unsupported('run', action ?? 'unknown');
}

async function jobCommand(action: string | undefined, parsed: ParsedArgs): Promise<unknown> {
  if (action === 'inspect') {
    const jobId = requiredPositional(parsed, 2, 'jobId');
    return withClient(parsed, async client => {
      const job = await client.jobs.get(jobId);
      if (job === null) throw notFound('QB_JOB_NOT_FOUND', 'Job does not exist.', { jobId });
      return job;
    });
  }
  if (action === 'list') {
    const query: { queue: string; state?: JobState; limit?: number; cursor?: string } = {
      queue: requiredStringFlag(parsed, 'queue')
    };
    assignOptional(query, 'state', optionalStringFlag(parsed, 'state') as JobState | undefined);
    assignOptional(query, 'limit', optionalIntegerFlag(parsed, 'limit'));
    assignOptional(query, 'cursor', optionalStringFlag(parsed, 'cursor'));
    return withClient(parsed, async client => client.jobs.list(query));
  }
  if (action === 'cancel') {
    const jobId = requiredPositional(parsed, 2, 'jobId');
    return withClient(parsed, async client => client.jobs.cancel(jobId));
  }
  if (action === 'retry-failed') {
    const jobId = requiredPositional(parsed, 2, 'jobId');
    const deduplicationKey = requiredStringFlag(parsed, 'deduplication-key');
    return withClient(parsed, async client => client.jobs.retryFailed(jobId, { deduplicationKey }));
  }
  throw unsupported('job', action ?? 'unknown');
}

async function completionCommand(action: string | undefined, parsed: ParsedArgs): Promise<unknown> {
  if (action === 'inspect') {
    const runId = optionalStringFlag(parsed, 'run');
    const eventId = parsed.positionals[2];
    return withClient(parsed, async client => {
      if (runId !== undefined) {
        const query: CompletionListQuery = { runId };
        assignOptional(query, 'limit', optionalIntegerFlag(parsed, 'limit'));
        assignOptional(query, 'cursor', optionalStringFlag(parsed, 'cursor'));
        return client.completions.list(query);
      }
      if (eventId === undefined) {
        throw invalidArgument('completion inspect requires <eventId> or --run <runId>.');
      }
      const completion = await client.completions.get(eventId);
      if (completion === null) throw notFound('QB_COMPLETION_NOT_FOUND', 'Completion event does not exist.', { eventId });
      return completion;
    });
  }
  if (action === 'retry') {
    const eventId = requiredPositional(parsed, 2, 'eventId');
    return withClient(parsed, async client => client.completions.retry(eventId));
  }
  throw unsupported('completion', action ?? 'unknown');
}

async function queueInspectCommand(parsed: ParsedArgs): Promise<unknown> {
  const queue = requiredPositional(parsed, 2, 'queue');
  return withClient(parsed, async client => {
    const statePages = await Promise.all(jobStates.map(state => client.jobs.list({ queue, state, limit: 100 })));
    const states: Record<string, { sampled: number; capped: boolean }> = {};
    statePages.forEach((page, index) => {
      const state = jobStates[index];
      if (state !== undefined) states[state] = { sampled: page.items.length, capped: page.nextCursor !== undefined };
    });
    const waitingPage = statePages[jobStates.indexOf('waiting')];
    let oldestWaitingMs: number | undefined;
    if (waitingPage !== undefined && waitingPage.items.length > 0) {
      let oldestCreatedAt = waitingPage.items[0]!.createdAt;
      for (const item of waitingPage.items) {
        if (item.createdAt < oldestCreatedAt) oldestCreatedAt = item.createdAt;
      }
      const createdMs = Date.parse(oldestCreatedAt);
      if (Number.isFinite(createdMs)) {
        oldestWaitingMs = Math.max(0, Date.now() - createdMs);
      }
    }
    const capacity = await client.capacity.snapshot();
    const queueCapacity = capacity.queues.find(entry => entry.queue === queue);
    return {
      queue,
      states,
      ...(oldestWaitingMs === undefined ? {} : { oldestWaitingMs }),
      ...(waitingPage !== undefined && waitingPage.nextCursor !== undefined
        ? { oldestWaitingSampleCapped: true }
        : {}),
      counters: queueCapacity?.counters,
      watermarks: queueCapacity?.watermarks,
      utilization: queueCapacity?.utilization,
      backpressure: queueCapacity?.backpressure,
      note: 'Queue inspect samples public job indexes (limit 100 per state) and attaches capacity.snapshot() counters/watermarks for this queue.'
    };
  });
}

async function workersInspectCommand(parsed: ParsedArgs): Promise<unknown> {
  const queue = requiredStringFlag(parsed, 'queue');
  const includeStale = parsed.flags['include-stale'] === true;
  const limit = optionalIntegerFlag(parsed, 'limit');
  return withClient(parsed, async client => {
    const roles = await client.roles.list({
      role: 'worker',
      domain: queue,
      includeStale,
      ...(limit === undefined ? {} : { limit })
    });
    return {
      queue,
      activeWorkers: roles.items.filter(role => !role.stale).length,
      includeStale,
      workers: roles.items
    };
  });
}

async function coordinatorInspectCommand(parsed: ParsedArgs): Promise<unknown> {
  const includeStale = parsed.flags['include-stale'] === true;
  const limit = optionalIntegerFlag(parsed, 'limit');
  return withClient(parsed, async (client, config) => {
    const domain = optionalStringFlag(parsed, 'domain') ?? config.scheduler.domain;
    const roles = await client.roles.list({
      role: 'coordinator',
      domain,
      includeStale,
      ...(limit === undefined ? {} : { limit })
    });
    return {
      domain,
      activeCoordinators: roles.items.filter(role => !role.stale).length,
      includeStale,
      coordinators: roles.items
    };
  });
}

async function workerDrainCommand(parsed: ParsedArgs): Promise<unknown> {
  const queue = requiredStringFlag(parsed, 'queue');
  const workerId = requiredStringFlag(parsed, 'worker-id');
  const reason = optionalStringFlag(parsed, 'reason');
  return withClient(parsed, async client => {
    const role = await client.roles.requestDrain({
      role: 'worker',
      domain: queue,
      identity: workerId,
      ...(reason === undefined ? {} : { reason })
    });
    return {
      message: 'Worker drain requested. The Worker will stop claiming new jobs after its next heartbeat.',
      worker: role
    };
  });
}

async function coordinatorDrainCommand(parsed: ParsedArgs): Promise<unknown> {
  const coordinatorId = requiredStringFlag(parsed, 'coordinator-id');
  const reason = optionalStringFlag(parsed, 'reason');
  return withClient(parsed, async (client, config) => {
    const domain = optionalStringFlag(parsed, 'domain') ?? config.scheduler.domain;
    const role = await client.roles.requestDrain({
      role: 'coordinator',
      domain,
      identity: coordinatorId,
      ...(reason === undefined ? {} : { reason })
    });
    return {
      message: 'Coordinator drain requested. The Coordinator will exit after its next heartbeat cycle.',
      coordinator: role
    };
  });
}

async function workerStartCommand(parsed: ParsedArgs, io: CliIo): Promise<unknown> {
  const queue = requiredStringFlag(parsed, 'queue');
  const { config } = await loadConfig(parsed);
  const { runtime } = await loadRequiredRuntime(parsed);
  const processor = createRuntimeProcessor(runtime);
  const client = await createQueuebitClient({ config });
  const workerOptions: Parameters<QueuebitClient['createWorker']>[2] = {};
  assignOptional(workerOptions, 'workerId', optionalStringFlag(parsed, 'worker-id'));
  assignOptional(workerOptions, 'concurrency', optionalIntegerFlag(parsed, 'concurrency'));
  assignOptional(workerOptions, 'leaseMs', optionalDurationFlag(parsed, 'lease-ms'));
  assignOptional(workerOptions, 'renewIntervalMs', optionalDurationFlag(parsed, 'renew-interval-ms'));
  assignOptional(workerOptions, 'pollIntervalMs', optionalDurationFlag(parsed, 'poll-interval-ms'));
  assignOptional(workerOptions, 'drainTimeoutMs', optionalDurationFlag(parsed, 'drain-timeout-ms'));
  assignOptional(workerOptions, 'timeoutMs', optionalDurationFlag(parsed, 'timeout-ms'));
  assignOptional(workerOptions, 'heartbeatIntervalMs', optionalDurationFlag(parsed, 'heartbeat-interval-ms'));
  assignOptional(workerOptions, 'heartbeatTtlMs', optionalDurationFlag(parsed, 'heartbeat-ttl-ms'));
  const worker = client.createWorker(queue, processor, workerOptions);
  worker.start();
  writeRoleReady(io, parsed, { role: 'worker', queue, workerId: worker.workerId, status: worker.status() });
  await waitForSignal();
  const closeOptions: Parameters<QueuebitClient['close']>[0] = {};
  assignOptional(closeOptions, 'timeoutMs', optionalDurationFlag(parsed, 'drain-timeout-ms'));
  await client.close(closeOptions);
  return undefined;
}

async function coordinatorStartCommand(parsed: ParsedArgs, io: CliIo): Promise<unknown> {
  const { config } = await loadConfig(parsed);
  const { runtime } = await loadRequiredRuntime(parsed);
  const client = await createQueuebitClient({ config });
  const coordinatorOptions: Parameters<QueuebitClient['createCoordinator']>[1] = {};
  assignOptional(coordinatorOptions, 'coordinatorId', optionalStringFlag(parsed, 'coordinator-id'));
  assignOptional(coordinatorOptions, 'leaseMs', optionalDurationFlag(parsed, 'lease-ms'));
  assignOptional(coordinatorOptions, 'sourceTimeoutMs', optionalDurationFlag(parsed, 'source-timeout-ms'));
  const coordinator = client.createCoordinator(runtime, coordinatorOptions);
  writeRoleReady(io, parsed, { role: 'coordinator', coordinatorId: coordinator.coordinatorId });
  const abort = createSignalAbortController();
  const roleOptions = {
    domain: optionalStringFlag(parsed, 'domain') ?? config.scheduler.domain,
    heartbeatIntervalMs:
      optionalDurationFlag(parsed, 'heartbeat-interval-ms') ?? config.scheduler.heartbeatIntervalMs,
    heartbeatTtlMs: optionalDurationFlag(parsed, 'heartbeat-ttl-ms') ?? config.scheduler.heartbeatTtlMs
  };
  if (roleOptions.heartbeatTtlMs <= roleOptions.heartbeatIntervalMs) {
    throw invalidArgument('--heartbeat-ttl-ms must be greater than --heartbeat-interval-ms.');
  }
  try {
    await runCoordinatorLoop(client, runtime, coordinator.coordinatorId, abort.signal, parsed, roleOptions);
  } finally {
    await client.roles.unregister({
      role: 'coordinator',
      domain: roleOptions.domain,
      identity: coordinator.coordinatorId
    }).catch(() => undefined);
    const closeOptions: Parameters<QueuebitClient['close']>[0] = {};
    assignOptional(closeOptions, 'timeoutMs', optionalDurationFlag(parsed, 'drain-timeout-ms'));
    await client.close(closeOptions);
  }
  return undefined;
}

async function runCoordinatorLoop(
  client: QueuebitClient,
  runtime: QueuebitRuntimeDefinition,
  coordinatorId: string,
  signal: AbortSignal,
  parsed: ParsedArgs,
  roleOptions: { domain: string; heartbeatIntervalMs: number; heartbeatTtlMs: number }
): Promise<void> {
  const coordinator = client.createCoordinator(runtime, { coordinatorId });
  const concurrency = optionalIntegerFlag(parsed, 'concurrency') ?? 1;
  const pollIntervalMs = optionalDurationFlag(parsed, 'poll-interval-ms') ?? 1_000;
  let nextHeartbeatMs = 0;
  while (!signal.aborted) {
    const nowMs = Date.now();
    if (nowMs >= nextHeartbeatMs) {
      const role = await client.roles.heartbeat({
        role: 'coordinator',
        domain: roleOptions.domain,
        identity: coordinatorId,
        status: 'running',
        heartbeatTtlMs: roleOptions.heartbeatTtlMs,
        metadata: { concurrency }
      });
      if (role.drainRequested) {
        await client.roles.heartbeat({
          role: 'coordinator',
          domain: roleOptions.domain,
          identity: coordinatorId,
          status: 'draining',
          heartbeatTtlMs: roleOptions.heartbeatTtlMs,
          metadata: { concurrency }
        });
        return;
      }
      nextHeartbeatMs = nowMs + roleOptions.heartbeatIntervalMs;
    }
    await coordinator.deliverDueCompletions({ limit: 25, signal }).catch(() => undefined);
    const page = await client.runs.list({ limit: Math.max(1, Math.min(100, concurrency * 4)) });
    const runnable = page.items
      .filter(run => !terminalRunStates.has(run.executionState))
      .slice(0, concurrency);
    await Promise.all(runnable.map(run => coordinator.advanceRun(run.id, { signal }).catch(() => undefined)));
    try {
      await delay(pollIntervalMs, undefined, { signal });
    } catch {
      return;
    }
  }
}

async function withClient<T>(parsed: ParsedArgs, run: (client: QueuebitClient, config: QueuebitConfig) => Promise<T>): Promise<T> {
  const { config } = await loadConfig(parsed);
  const client = await createQueuebitClient({ config });
  try {
    return await run(client, config);
  } finally {
    await client.close().catch(() => undefined);
  }
}

async function loadConfig(parsed: ParsedArgs): Promise<{ config: QueuebitConfig; loadedConfig: LoadedQueuebitModule }> {
  const configPath = requiredStringFlag(parsed, 'config');
  const loadedConfig = await loadQueuebitModule(configPath);
  const candidate = pickExport(loadedConfig.namespace, ['default', 'config'], configPath);
  return { config: defineQueuebitConfig(candidate as QueuebitUserConfig), loadedConfig };
}

async function loadRuntimeIfPresent(
  parsed: ParsedArgs
): Promise<{ runtime: QueuebitRuntimeDefinition; loadedRuntime: LoadedQueuebitModule } | undefined> {
  const runtimePath = optionalStringFlag(parsed, 'runtime');
  if (runtimePath === undefined) return undefined;
  const loadedRuntime = await loadQueuebitModule(runtimePath);
  const candidate = pickExport(loadedRuntime.namespace, ['default', 'runtime'], runtimePath);
  return { runtime: defineQueuebitRuntime(candidate as QueuebitRuntimeDefinition), loadedRuntime };
}

async function loadRequiredRuntime(
  parsed: ParsedArgs
): Promise<{ runtime: QueuebitRuntimeDefinition; loadedRuntime: LoadedQueuebitModule }> {
  const result = await loadRuntimeIfPresent(parsed);
  if (result !== undefined) return result;
  throw invalidArgument('This command requires --runtime <queuebit.runtime.ts|.mjs|.js|.cjs>.');
}

function validateRuntimeRegistrations(config: QueuebitConfig, runtime: QueuebitRuntimeDefinition): Record<string, unknown> {
  const missing: Array<Record<string, string>> = [];
  for (const [definition, batchRun] of Object.entries(config.batchRuns)) {
    if (runtime.sources[batchRun.source] === undefined) {
      missing.push({ definition, kind: 'source', name: batchRun.source });
    }
    if (runtime.mappers[batchRun.mapper] === undefined) {
      missing.push({ definition, kind: 'mapper', name: batchRun.mapper });
    }
    for (const [scope, handler] of Object.entries(batchRun.completion)) {
      if (handler !== undefined && runtime.completions?.[handler.handler] === undefined) {
        missing.push({ definition, kind: `${scope}-completion`, name: handler.handler });
      }
    }
  }
  if (missing.length > 0) {
    throw new QueuebitError({
      code: 'QB_CONFIG_HANDLER_NOT_REGISTERED',
      message: 'Queuebit runtime is missing handlers referenced by config.',
      details: { missing }
    });
  }
  return {
    runtime: 'loaded',
    sources: Object.keys(runtime.sources),
    mappers: Object.keys(runtime.mappers),
    processors: Object.keys(runtime.processors ?? {}),
    completions: Object.keys(runtime.completions ?? {})
  };
}

function createRuntimeProcessor(runtime: QueuebitRuntimeDefinition): QueuebitProcessor {
  const processors = runtime.processors ?? {};
  if (Object.keys(processors).length === 0) {
    throw new QueuebitError({
      code: 'QB_CONFIG_HANDLER_NOT_REGISTERED',
      message: 'Worker start requires at least one runtime processor registration.',
      details: { registry: 'processors' }
    });
  }
  return async (job, context) => {
    const processor = processors[job.name];
    if (processor === undefined) {
      throw new QueuebitError({
        code: 'QB_CONFIG_HANDLER_NOT_REGISTERED',
        message: `No runtime processor registered for job "${job.name}".`,
        details: { jobId: job.id, queue: job.queue, name: job.name }
      });
    }
    return processor(job, context);
  };
}

function writeSuccess(io: CliIo, data: unknown, json: boolean): void {
  if (json) {
    const envelope: CliSuccessEnvelope = { ok: true, data };
    io.stdout.write(`${JSON.stringify(envelope)}\n`);
    return;
  }
  io.stdout.write(`${formatHuman(data)}\n`);
}

function writeRoleReady(io: CliIo, parsed: ParsedArgs, data: unknown): void {
  if (parsed.flags.json === true) {
    io.stdout.write(`${JSON.stringify({ ok: true, data })}\n`);
  } else {
    io.stdout.write(`${formatHuman({ status: 'ready', ...assertRecord(data) })}\n`);
  }
}

function formatHuman(data: unknown): string {
  if (data === null || typeof data !== 'object') return String(data);
  const record = data as Record<string, unknown>;
  if ('items' in record && Array.isArray(record.items)) {
    return record.items.length === 0
      ? 'No items.'
      : record.items.map(item => formatHuman(item)).join('\n\n');
  }
  return flattenHumanRecord(record).join('\n');
}

function formatHumanValue(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return value.map(item => formatHumanValue(item)).join(',');
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

function flattenHumanRecord(record: Record<string, unknown>, prefix = ''): string[] {
  const lines: string[] = [];
  for (const [key, value] of Object.entries(record)) {
    const path = prefix.length === 0 ? key : `${prefix}.${key}`;
    if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
      lines.push(...flattenHumanRecord(value as Record<string, unknown>, path));
    } else {
      lines.push(`${path}=${formatHumanValue(value)}`);
    }
  }
  return lines;
}

function serializeCliError(cause: unknown, operation: string): CliErrorEnvelope {
  if (cause instanceof QueuebitError) {
    return {
      ok: false,
      error: {
        code: cause.code,
        message: cause.message,
        retryable: isRetryable(cause.code),
        operation,
        ...(cause.details === undefined ? {} : { details: toJsonSafe(cause.details) })
      }
    };
  }
  const message = cause instanceof Error ? cause.message : String(cause);
  return {
    ok: false,
    error: {
      code: 'QB_INTERNAL',
      message,
      retryable: false,
      operation
    }
  };
}

function exitCodeFor(code: string): number {
  if (code.startsWith('QB_CLI_') || code.startsWith('QB_CONFIG_')) return 2;
  if (code.startsWith('QB_REDIS_')) return 3;
  if (code.endsWith('_STATE_CONFLICT')) return 4;
  return 1;
}

function isRetryable(code: string): boolean {
  return code.startsWith('QB_REDIS_') || code === 'QB_REDIS_SCRIPT_EXECUTION_FAILED';
}

function parseArgs(argv: string[]): ParsedArgs {
  const positionals: string[] = [];
  const flags: Record<string, CliFlagValue> = {};
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === undefined) continue;
    if (!token.startsWith('--')) {
      positionals.push(token);
      continue;
    }
    const assignmentIndex = token.indexOf('=');
    if (assignmentIndex !== -1) {
      flags[token.slice(2, assignmentIndex)] = token.slice(assignmentIndex + 1);
      continue;
    }
    const name = token.slice(2);
    const next = argv[index + 1];
    if (next !== undefined && !next.startsWith('--')) {
      flags[name] = next;
      index += 1;
    } else {
      flags[name] = true;
    }
  }
  return { positionals, flags };
}

function requiredPositional(parsed: ParsedArgs, index: number, label: string): string {
  const value = parsed.positionals[index];
  if (value === undefined || value.length === 0) throw invalidArgument(`Missing required ${label}.`);
  return value;
}

function requiredStringFlag(parsed: ParsedArgs, name: string): string {
  const value = optionalStringFlag(parsed, name);
  if (value === undefined) throw invalidArgument(`Missing required --${name}.`);
  return value;
}

function optionalStringFlag(parsed: ParsedArgs, name: string): string | undefined {
  const value = parsed.flags[name];
  if (value === undefined) return undefined;
  if (typeof value !== 'string' || value.length === 0) {
    throw invalidArgument(`--${name} requires a value.`);
  }
  return value;
}

function optionalIntegerFlag(parsed: ParsedArgs, name: string): number | undefined {
  const value = optionalStringFlag(parsed, name);
  if (value === undefined) return undefined;
  const parsedValue = Number.parseInt(value, 10);
  if (!Number.isInteger(parsedValue)) throw invalidArgument(`--${name} must be an integer.`);
  return parsedValue;
}

function optionalDurationFlag(parsed: ParsedArgs, name: string): number | undefined {
  const value = optionalStringFlag(parsed, name);
  if (value === undefined) return undefined;
  return parseDuration(value, name);
}

function parseDuration(value: string, name: string): number {
  const match = /^([0-9]+)(ms|s|m)?$/.exec(value);
  if (match === null) throw invalidArgument(`--${name} must be a duration like 500ms, 5s, or 1m.`);
  const amount = Number.parseInt(match[1] ?? '0', 10);
  const unit = match[2] ?? 'ms';
  if (unit === 'm') return amount * 60_000;
  if (unit === 's') return amount * 1_000;
  return amount;
}

function parseJson(value: string, label: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    throw invalidArgument(`--${label} must be valid JSON.`);
  }
}

function pickExport(namespace: Record<string, unknown>, names: string[], file: string): unknown {
  for (const name of names) {
    const value = namespace[name];
    if (value !== undefined) return value;
  }
  throw new QueuebitError({
    code: 'QB_CLI_LOADER_FAILED',
    message: `Queuebit module ${file} does not export ${names.join(' or ')}.`,
    details: { file, exports: Object.keys(namespace) }
  });
}

function unsupported(resource: string, action: string): QueuebitError {
  return new QueuebitError({
    code: 'QB_CLI_COMMAND_UNSUPPORTED',
    message: `Unsupported Queuebit CLI command: ${resource} ${action}.`,
    details: { resource, action }
  });
}

function invalidArgument(message: string): QueuebitError {
  return new QueuebitError({ code: 'QB_CLI_ARGUMENT_INVALID', message });
}

function notFound(code: QueuebitErrorCode, message: string, details: Record<string, unknown>): QueuebitError {
  return new QueuebitError({ code, message, details });
}

function toJsonSafe(value: unknown): unknown {
  if (value instanceof Error) return { name: value.name, message: value.message };
  if (Array.isArray(value)) return value.map(toJsonSafe);
  if (value !== null && typeof value === 'object') {
    const safe: Record<string, unknown> = {};
    for (const [key, child] of Object.entries(value)) safe[key] = toJsonSafe(child);
    return safe;
  }
  return value;
}

function assignOptional<T extends object, K extends keyof T>(target: T, key: K, value: T[K] | undefined) {
  if (value !== undefined) target[key] = value;
}

function assertRecord(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === 'object' ? value as Record<string, unknown> : {};
}

function createSignalAbortController(): AbortController {
  const controller = new AbortController();
  const abort = () => controller.abort();
  process.once('SIGINT', abort);
  process.once('SIGTERM', abort);
  return controller;
}

function waitForSignal(): Promise<void> {
  return new Promise(resolveSignal => {
    const done = () => resolveSignal();
    process.once('SIGINT', done);
    process.once('SIGTERM', done);
  });
}

function readPackageVersion(): string {
  const cliPath = process.argv[1] === undefined
    ? resolve(process.cwd(), 'dist', 'cli.js')
    : resolve(process.argv[1]);
  const requireFromCli = createRequire(cliPath);
  const raw = requireFromCli('../package.json') as { version?: string };
  return raw.version ?? '0.0.0';
}

function renderHelp(): string {
  return `Queuebit CLI

Usage:
  queuebit config validate --config queuebit.config.ts [--runtime queuebit.runtime.ts] [--json]
  queuebit health inspect --config queuebit.config.ts [--json]
  queuebit worker start --config queuebit.config.ts --runtime queuebit.runtime.ts --queue <queue>
  queuebit worker drain --config queuebit.config.ts --queue <queue> --worker-id <id>
  queuebit coordinator start --config queuebit.config.ts --runtime queuebit.runtime.ts
  queuebit coordinator drain --config queuebit.config.ts --coordinator-id <id>
  queuebit run start <definition> --config queuebit.config.ts --input-json <json> --idempotency-key <key>
  queuebit run inspect <runId> --config queuebit.config.ts [--json]
  queuebit run list --config queuebit.config.ts [--definition <name>] [--state <state>] [--limit <n>] [--json]
  queuebit run failures <runId> --config queuebit.config.ts [--stage mapper|processor] [--limit <n>] [--json]
  queuebit run pause <runId> --config queuebit.config.ts
  queuebit run resume <runId> --config queuebit.config.ts
  queuebit run cancel <runId> --reason <text> --config queuebit.config.ts
  queuebit run retry-failed <runId> --idempotency-key <key> --config queuebit.config.ts
  queuebit job inspect <jobId> --config queuebit.config.ts [--json]
  queuebit job list --queue <queue> --config queuebit.config.ts [--state <state>] [--limit <n>] [--json]
  queuebit job cancel <jobId> --config queuebit.config.ts
  queuebit job retry-failed <jobId> --deduplication-key <key> --config queuebit.config.ts
  queuebit queue inspect <queue> --config queuebit.config.ts [--json]
  queuebit completion inspect --run <runId> --config queuebit.config.ts [--json]
  queuebit completion inspect <eventId> --config queuebit.config.ts [--json]
  queuebit completion retry <eventId> --config queuebit.config.ts
  queuebit workers inspect --config queuebit.config.ts --queue <queue> [--include-stale] [--json]
  queuebit coordinator inspect --config queuebit.config.ts [--domain <domain>] [--include-stale] [--json]

Global:
  --json       Write one machine-readable JSON envelope to stdout.
  --help       Show this help.
  --version    Print package version.

Notes:
  scheduler start|inspect|drain is not supported in v0.1 (exit 2, QB_CLI_COMMAND_UNSUPPORTED).
`;
}
