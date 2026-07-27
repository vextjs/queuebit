# API 快查

<span class="manual-label">参考 · 按你要做的事找方法</span>

这页给已经开始接入的人查方法名和返回形状。第一次接入先看 [快速开始](./quick-start.md)：先建 client、传 Redis 配置、注册 processor，再用 `jobs.add` 发起一个普通后台任务。`runs.start` 是数据库批处理入口，按需再查。

## 先按任务找 API

| 你要做什么 | 看这些方法 |
|---|---|
| 创建一个 Queuebit client | `createQueuebitClient({ config, logger? })` |
| 发起一个后台任务 | `queuebit.jobs.add(queue, name, data, options?)` |
| 批量发起后台任务 | `queuebit.jobs.addBulk(entries)` |
| 查询、取消、重试 Job | `queuebit.jobs.get/list/cancel/retryFailed` |
| 批量处理数据库记录 | `queuebit.runs.start(definition, { input, idempotencyKey })` |
| 查询、暂停、恢复、取消 Run | `queuebit.runs.get/list/pause/resume/cancel` |
| 重试失败 Run | `queuebit.runs.listFailures/retryFailed` |
| 查看或重试结果回写 | `queuebit.completions.get/list/retry` |
| 接健康检查和指标 | `queuebit.health.snapshot()`、`queuebit.metrics.renderPrometheus()` |
| 看队列水位和本地告警 | `queuebit.capacity.snapshot()`、`queuebit.alerts.evaluate()` |

## 创建一个长期 client

```ts
import config from './queuebit.config.js';
import { createQueuebitClient } from 'queuebit';

const queuebit = await createQueuebitClient({ config, logger });
```

| 方法 | 语义 | 错误/边界 |
|---|---|---|
| `createQueuebitClient({config, logger?})` | 创建应用级长期 client | 静态配置或 Redis preflight 失败时拒绝 |
| `queuebit.close()` | 释放当前 client 资源 | 独立脚本结束或应用 onClose 时调用 |
| `queuebit.health.snapshot()` | 返回 `HealthSnapshot` | 只描述当前 client/role，不冒充全集群 |
| `queuebit.metrics.collect()` | 返回当前进程的结构化 samples | 不冒充全集群聚合 |
| `queuebit.metrics.renderPrometheus()` | 渲染当前进程的 Prometheus 文本 | core 不启动 HTTP server |
| `queuebit.observabilityHttp.handle(request)` | 返回 health 或 metrics HTTP response 对象 | 不监听端口；由应用挂载、鉴权和隔离 |
| `queuebit.alerts.evaluate(options?)` | 基于 health/metrics/capacity 返回本地告警 findings | 只是起点，不是全集群事故引擎 |
| `queuebit.retention.plan()` | 查看历史清理计划 | 只读；不删除 Redis 数据 |
| `queuebit.retention.purge(options?)` | 预览或执行历史清理 | 默认 dry-run；execute 只清理已安全结束的历史详情 |
| `queuebit.capacity.snapshot()` | 读取已声明 queue 的 counters 与 watermarks | 只读；不扫描任意 key |

不在单次 HTTP 请求或单次 `add/start` 后关闭 client。vext 由 plugin `onClose` 统一关闭。

<a id="public-api-contract"></a>
## 公开输入和返回类型

以下类型是用户会直接看到的输入和返回形状。`list` 只返回 summary，不包含业务 `data/input/result`；`get` 返回完整 snapshot。所有时间是 ISO 8601 UTC 字符串。

```ts
type JobState =
  | 'waiting' | 'active' | 'delayed' | 'retrying'
  | 'completed' | 'failed' | 'cancelled';

type RunExecutionState =
  | 'created' | 'running' | 'pausing' | 'paused'
  | 'blocked' | 'cancelling'
  | 'completed' | 'partial_failed' | 'failed' | 'cancelled';

type CompletionState =
  | 'not_created' | 'not_required' | 'pending' | 'delivering'
  | 'retrying' | 'delivered' | 'failed';

type HealthStatus = 'ready' | 'degraded' | 'not_ready' | 'draining';

type HealthCheck = {
  status: 'pass' | 'warn' | 'fail';
  message?: string;
  observedAt: string;
  details?: Record<string, unknown>;
};

type HealthSnapshot = {
  status: HealthStatus;
  ready: boolean;
  role: 'producer' | 'worker' | 'coordinator';
  identity?: string;
  timestamp: string;
  checks: Record<string, HealthCheck>;
};

type QueuebitMetricSample = {
  name: string;
  type: 'counter' | 'gauge' | 'histogram';
  value: number;
  labels: Record<string, string>;
  timestamp?: string;
};

type QueuebitRetentionPlan = {
  namespace: string;
  observedAt: string;
  completedJobs: {
    ageMs: number;
    maxCount: number;
    tombstoneTtlMs: number;
    eligibleStates: string[];
    protectedStates: string[];
  };
  failedWork: {
    ageMs: number;
    maxCount: number;
    eligibleStates: string[];
    protectedStates: string[];
  };
  terminalRuns: {
    ageMs: number;
    maxCount: number;
    tombstoneTtlMs: number;
    eligibleStates: string[];
    protectedStates: string[];
  };
  completionEvents: {
    ageMs: number;
    maxCount: number;
    tombstoneTtlMs: number;
    eligibleStates: string[];
    protectedStates: string[];
  };
  guards: string[];
};

type QueuebitRetentionPurgeMode = 'dry-run' | 'execute';
type QueuebitRetentionPurgeWindow = 'completedJobs' | 'terminalRuns' | 'completions';
type QueuebitRetentionPurgeDecision =
  | 'would_delete'
  | 'would_tombstone'
  | 'deleted'
  | 'tombstoned'
  | 'skipped';
type QueuebitRetentionPurgeReason =
  | 'expired_by_age'
  | 'exceeds_max_count'
  | 'retained_by_window'
  | 'snapshot_missing'
  | 'state_protected'
  | 'completion_protected'
  | 'batchrun_owned'
  | 'tombstone_required'
  | 'details_expired'
  | 'updated_at_invalid'
  | 'execute_conflict';

type QueuebitRetentionPurgeOptions = {
  mode?: QueuebitRetentionPurgeMode;
  limit?: number;
};

type QueuebitRetentionPurgeResult = {
  namespace: string;
  mode: QueuebitRetentionPurgeMode;
  observedAt: string;
  scanned: number;
  deleted: number;
  tombstoned: number;
  skipped: number;
  hasMore: boolean;
  windows: {
    completedJobs: {
      ageMs: number;
      maxCount: number;
      cutoffAt: string;
      eligibleStates: string[];
      protectedStates: string[];
    };
    terminalRuns: {
      ageMs: number;
      maxCount: number;
      cutoffAt: string;
      eligibleStates: string[];
      protectedStates: string[];
    };
    completions: {
      ageMs: number;
      maxCount: number;
      cutoffAt: string;
      eligibleStates: string[];
      protectedStates: string[];
    };
  };
  candidates: Array<{
    window: QueuebitRetentionPurgeWindow;
    queue?: string;
    jobId?: string;
    runId?: string;
    eventId?: string;
    key: string;
    score: string;
    state?: string;
    completionState?: string;
    type?: string;
    updatedAt?: string;
    decision: QueuebitRetentionPurgeDecision;
    reason: QueuebitRetentionPurgeReason;
  }>;
};

type QueuebitCapacitySnapshot = {
  namespace: string;
  observedAt: string;
  queues: Array<{
    queue: string;
    counters: {
      queuedJobs: number;
      queuedBytes: number;
      waitingJobs: number;
      activeJobs: number;
      delayedJobs: number;
      retryingJobs: number;
      completedJobs: number;
      failedJobs: number;
      cancelledJobs: number;
    };
    watermarks: {
      highWatermarkJobs?: number;
      lowWatermarkJobs?: number;
      highWatermarkBytes?: number;
      lowWatermarkBytes?: number;
    };
    utilization: { jobs?: number; bytes?: number };
    backpressure: {
      latched: boolean;
      reason?: string;
      since?: string;
      lastCheckedAt?: string;
    };
  }>;
};

type QueuebitAlertSeverity = 'warning' | 'critical';

type QueuebitAlertFinding = {
  id: string;
  severity: QueuebitAlertSeverity;
  message: string;
  observedAt: string;
  details: Record<string, unknown>;
};

type QueuebitAlertEvaluationOptions = {
  queueUtilizationWarning?: number;
  queueUtilizationCritical?: number;
  completionFailedMinimum?: number;
  stalledRecoveryMinimum?: number;
};

type QueuebitAlertEvaluation = {
  status: 'ok' | QueuebitAlertSeverity;
  observedAt: string;
  findings: QueuebitAlertFinding[];
};

type QueuebitObservabilityHttpRequest = {
  path: string;
  method?: string;
};

type QueuebitObservabilityHttpResponse = {
  status: number;
  headers: Record<string, string>;
  body: string;
};

type QueuebitObservabilityHttpOptions = {
  healthPath?: string;
  metricsPath?: string;
};

type CursorPage<T> = {
  items: T[];
  nextCursor?: string;
};

type JobSummary = {
  id: string;
  queue: string;
  name: string;
  state: JobState;
  attempt: number;
  attempts: number;
  runId?: string;
  batchId?: string;
  parentJobId?: string;
  createdAt: string;
  updatedAt: string;
  dataDigest?: string;
  detailsExpired?: true;
  detailsExpiredAt?: string;
};

type JobSnapshot<Data = unknown, Result = unknown> = JobSummary & {
  data?: Data;
  result?: Result;
  deduplicationKey?: string;
  idempotencyKey?: string;
  failedReason?: QueuebitSerializedError;
};

type QueuebitSerializedError = {
  name?: string;
  code?: string;
  message: string;
  details?: unknown;
};

type CompletionSummary = {
  recordsSeen: number;
  recordsDispatched: number;
  recordsSkipped: number;
  recordsFailed: number;
  recordsUndispatched: number;
  boundaryTotalRecords: number | null;
  jobsCreated: number;
  jobsCompleted: number;
  jobsFailed: number;
  jobsCancelled: number;
};

type RunSummary = CompletionSummary & {
  id: string;
  definition: string;
  definitionVersion: number;
  parentRunId?: string;
  recoveryDepth: number;
  executionState: RunExecutionState;
  completionState: CompletionState;
  checkpointBatchIndex: number;
  createdAt: string;
  updatedAt: string;
  inputDigest?: string;
  detailsExpired?: true;
  detailsExpiredAt?: string;
  failureDetailsExpired?: true;
};

type RunSnapshot<Input = unknown, Boundary = unknown, Cursor = unknown> =
  RunSummary & {
    input?: Input;
    boundary?: Boundary | null;
    dispatchCursor?: Cursor | null;
    checkpointCursor?: Cursor | null;
    sourceExhausted: boolean;
    inFlightBatches: number;
    nextDispatchAt?: string;
    pauseRequestedAt?: string;
    pausedAt?: string;
    resumedAt?: string;
    cancelReason?: string;
    cancelRequestedAt?: string;
    cancelledAt?: string;
    dispatchHoldReason?:
      | 'interval' | 'in_flight_limit' | 'backpressure'
      | 'no_active_worker' | 'redis_reconnecting';
  };

type RunStartResult<Input = unknown, Boundary = unknown, Cursor = unknown> =
  RunSnapshot<Input, Boundary, Cursor> & {
    deduplicated: boolean;
  };

type FailureRecord<Payload = unknown> = {
  sequence: string;
  runId: string;
  batchId?: string;
  jobId?: string;
  stage: 'mapper' | 'processor';
  recordIdentity: string;
  attempt: number;
  error: QueuebitSerializedError;
  recoveryAvailable: boolean;
  envelopeExpiresAt?: string;
  payload?: Payload;
};

type CompletionBackoffSnapshot = {
  type: 'fixed' | 'exponential';
  delayMs: number;
  maxDelayMs?: number;
};

type CompletionEventSummary = {
  id: string;
  type: 'batch.settled' | 'run.settled' | 'run.cancelled';
  runId: string;
  batchId?: string;
  handler?: string;
  completionState: CompletionState;
  attempt: number;
  attempts: number;
  deliveryGeneration: number;
  summaryDigest?: string;
  detailsExpired?: true;
  detailsExpiredAt?: string;
  createdAt: string;
  updatedAt: string;
};

type CompletionSnapshot<Summary = unknown> =
  CompletionEventSummary & {
    summary?: Summary;
    backoff?: CompletionBackoffSnapshot;
    lastError?: QueuebitSerializedError;
    nextDueAt?: string;
  };

type CompletionEventSnapshot<Summary = unknown> = CompletionSnapshot<Summary>;
```

```ts
type JobBackoffOptions = {
  type: 'fixed' | 'exponential';
  delayMs: number;
  maxDelayMs?: number;
  jitter?: number;
};

type JobAddOptions = {
  attempts?: number;
  timeoutMs?: number;
  backoff?: JobBackoffOptions;
  delayMs?: number;
  deduplicationKey?: string;
  idempotencyKey?: string;
};

type BulkJobEntry<Data = unknown> = {
  name: string;
  data: Data;
  options?: JobAddOptions;
};

type JobListQuery = {
  queue: string;
  state?: JobState;
  cursor?: string;
  limit?: number;
};

type RunStartRequest<Input = unknown> = {
  input: Input;
  idempotencyKey: string;
};

type RunListQuery = {
  definition?: string;
  executionState?: RunExecutionState;
  cursor?: string;
  limit?: number;
};

type FailureListQuery = {
  stage?: 'mapper' | 'processor';
  cursor?: string;
  limit?: number;
  includePayload?: boolean;
};

type CompletionListQuery = {
  runId?: string;
  batchId?: string;
  type?: CompletionEventSummary['type'];
  completionState?: CompletionState;
  cursor?: string;
  limit?: number;
};

interface JobsApi {
  add<Data>(queue: string, name: string, data: Data, options?: JobAddOptions):
    Promise<JobSnapshot<Data>>;
  addBulk<Data>(queue: string, entries: BulkJobEntry<Data>[]):
    Promise<JobSnapshot<Data>[]>;
  get<Data = unknown, Result = unknown>(jobId: string):
    Promise<JobSnapshot<Data, Result> | null>;
  list(query: JobListQuery): Promise<CursorPage<JobSummary>>;
  cancel(jobId: string): Promise<JobSnapshot>;
  retryFailed(jobId: string, request: { deduplicationKey: string }):
    Promise<JobSnapshot>;
}

interface RunsApi {
  start<Input>(definition: string, request: RunStartRequest<Input>):
    Promise<RunStartResult<Input>>;
  get(runId: string): Promise<RunSnapshot | null>;
  list(query: RunListQuery): Promise<CursorPage<RunSummary>>;
  listFailures(runId: string, query?: FailureListQuery):
    Promise<CursorPage<FailureRecord>>;
  pause(runId: string): Promise<RunSnapshot>;
  resume(runId: string): Promise<RunSnapshot>;
  cancel(runId: string, request: { reason: string }): Promise<RunSnapshot>;
  retryFailed(
    runId: string,
    request: { idempotencyKey: string; definitionVersion?: number }
  ): Promise<RunSnapshot>;
}

interface CompletionsApi {
  list(query?: CompletionListQuery):
    Promise<CursorPage<CompletionEventSummary>>;
  get<Summary = unknown>(eventId: string):
    Promise<CompletionSnapshot<Summary> | null>;
  retry<Summary = unknown>(eventId: string):
    Promise<CompletionSnapshot<Summary>>;
}

interface HealthApi {
  snapshot(): HealthSnapshot;
}

interface MetricsApi {
  collect(): readonly QueuebitMetricSample[];
  renderPrometheus(): string;
}

interface RetentionApi {
  plan(): QueuebitRetentionPlan;
  purge(options?: QueuebitRetentionPurgeOptions):
    Promise<QueuebitRetentionPurgeResult>;
}

interface AlertsApi {
  evaluate(options?: QueuebitAlertEvaluationOptions):
    Promise<QueuebitAlertEvaluation>;
}

interface QueuebitObservabilityHttpApi {
  handle(request: QueuebitObservabilityHttpRequest):
    QueuebitObservabilityHttpResponse;
}
```

`get` 在 identity 不存在或已超出保留窗口时返回 `null`。如果保留的 Job、Run 或 Completion identity 已是 `detailsExpired=true`，Queuebit 只返回原 identity、状态、digest 和引用元数据，不返回 `data`、`result`、`failedReason`、Run `input`、Run `boundary`、Run cursor 或 Completion `summary`；应用不能假设 retention 后历史 payload 永远可读。Run failure 详情过期后，`runs.listFailures` 和 `runs.retryFailed` 返回 `QB_RUN_STATE_CONFLICT`；Completion 详情过期后，`completions.retry` 返回 `QB_COMPLETION_STATE_CONFLICT`。所有控制/恢复方法对缺失 identity 抛出 `QB_JOB_NOT_FOUND` / `QB_RUN_NOT_FOUND` / `QB_COMPLETION_NOT_FOUND`；状态不兼容时抛出对应 `*_STATE_CONFLICT`。`limit` 默认 50，范围 1～100；页面耗尽时不返回 `nextCursor`。`addBulk` 按输入顺序返回 snapshots，任一 entry 失败时不返回部分结果。`jitter` 范围是 0～1；0 表示不抖动，0.2 表示在计算后的退避窗口内加入最多 20% 的随机抖动。

Redis 断开、strict server policy 失败或角色资格不可证明时，`HealthSnapshot.status='not_ready'` 且 `ready=false`。`degraded` 只表示仍可安全工作但存在 warn policy 或非关键观测告警；`draining` 始终不接受新 work。

`metrics.collect()` 读取的就是 `renderPrometheus()` 使用的同一个本进程 registry。当前指标覆盖 client、历史清理、水位、直接提交、Worker claim/completion/failure/duration/attempt/stalled recovery、role heartbeat/drain observation，以及 Coordinator 推进和 completion delivery。它仍然是进程本地视角；外部监控必须分别抓取每个 Producer、Worker、Coordinator 进程后再聚合。`alerts.evaluate()` 会根据 health、队列利用率/背压、completion failure 和 stalled recovery metrics 返回本进程的告警起点。`observabilityHttp.handle()` 只返回 response 对象；Queuebit core 仍不启动 HTTP server。

`retention.purge()` 默认 `mode: 'dry-run'`，只预览将要清理的历史详情；传 `mode: 'execute'` 才会真正清理。它从已声明 queue、终态 Run 和 Completion 索引读取候选，不扫描 Redis keyspace。执行时只处理已安全结束的直接 Job、终态 Run 和 delivered/not_required Completion event；还在去重 TTL 内的记录会留下 identity、状态、digest、idempotency key、父子关系和汇总计数等轻量信息，并删除较大的 payload、result、failure replay 和 completion summary。非终态 work、未完成/失败 completion、BatchRun 拥有的 job cleanup，以及证据不足的删除候选都会被跳过。`completionEvents.ageMs/maxCount` 独立控制 Completion event 详情窗口；详情过期后，`completions.list/get` 仍能看到 identity，但不能再读回已删除的 summary。

```js
import {
  createQueuebitClient,
  createQueuebitObservabilityHttpApi
} from 'queuebit';

const queuebit = await createQueuebitClient({ config });

const defaultHealth = queuebit.observabilityHttp.handle({
  method: 'GET',
  path: '/queuebit/health'
});

const mounted = createQueuebitObservabilityHttpApi(queuebit, {
  healthPath: '/healthz',
  metricsPath: '/internal/metrics'
});

const metricsResponse = mounted.handle({
  method: 'GET',
  path: '/internal/metrics'
});

const retentionPreview = await queuebit.retention.purge({
  limit: 100
});

const alertEvaluation = await queuebit.alerts.evaluate({
  queueUtilizationWarning: 0.8,
  queueUtilizationCritical: 0.95
});
```

把 `status`、`headers`、`body` 转接到 vext、Fastify、Express 或你的平台路由。应用自己负责鉴权、租户授权、限流、TLS 和网络暴露范围。

## Jobs API

```ts
const job = await queuebit.jobs.add(
  'notification',
  'send-receipt',
  { schemaVersion: 1, orderId: 1024, tenantId, recipient },
  {
    deduplicationKey: `request:receipt:${tenantId}:1024`,
    idempotencyKey: `receipt:${tenantId}:1024`,
    delayMs: 5_000
  }
);
```

### `jobs.add(queue, name, data, options?)`

| option | 默认 | 说明 |
|---|---:|---|
| `attempts` | 1 | 包含首次执行 |
| `timeoutMs` | 无 | 可选 per-job timeout，会传到 Worker processor signal 路径 |
| `backoff` | 无 | fixed/exponential |
| `delayMs` | 0 | 非负整数 |
| `deduplicationKey` | 无 | 队列 identity 有限窗口去重 |
| `idempotencyKey` | 无 | 传给 processor 的业务副作用幂等键 |

### `jobs.addBulk(queue, entries)`

只接受同一 Queue，在 `maxBulkJobs/maxBulkBytes` 内先整批校验，再全有或全无原子创建。它不是 BatchRun。

### 查询和控制

```ts
await queuebit.jobs.get(jobId);
await queuebit.jobs.list({ queue: 'notification', state: 'failed', limit: 100 });
await queuebit.jobs.cancel(waitingJobId);
await queuebit.jobs.retryFailed(failedJobId, {
  deduplicationKey: `replacement:${failedJobId}:1`
});
```

| 方法 | 允许状态 | 结果 |
|---|---|---|
| `jobs.cancel` | waiting/delayed/retrying | 重复调用返回当前 snapshot；active/终态返回 state conflict |
| `jobs.retryFailed` | 保留窗口内 failed 直接 job | 创建带 `parentJobId` 的 replacement，沿用业务 key，使用新 deduplication identity |
| BatchRun 所属 job 调用 `jobs.retryFailed` | 不允许 | 提示使用 `runs.retryFailed` |

`jobs.list` 使用稳定创建 sequence 游标和冻结索引，不扫描 Redis keyspace；默认只返回摘要。

## Runs API

### `runs.start(definition, request)`

```ts
const run = await queuebit.runs.start('receipt-campaign', {
  input: {
    tenantId: 'tenant-42',
    paidBefore: '2026-07-15T00:00:00.000Z'
  },
  idempotencyKey: 'receipt-campaign:tenant-42:2026-07-15'
});

console.log(run.id, run.deduplicated);
```

`idempotencyKey` 必填。执行顺序是 inputSchema 校验 -> `qbcj-v1` canonicalization -> key/digest 去重比较 -> Run 原子创建。首次创建返回 `deduplicated=false`、`executionState=created`、`completionState=not_created`；相同 key + 相同 canonical input 返回已有 Run 的当前 snapshot 与 `deduplicated=true`；不同 input 返回 `QB_RUN_DEDUPLICATION_CONFLICT`。

### 查询

```ts
await queuebit.runs.get(run.id);
await queuebit.runs.list({
  definition: 'receipt-campaign',
  executionState: 'partial_failed',
  limit: 100
});
await queuebit.runs.listFailures(run.id, {
  stage: 'mapper',
  limit: 100,
  includePayload: false
});
```

`runs.listFailures` 使用稳定 failure sequence，`limit` 1～100，返回 stage、record/job identity、error code、attempt、recovery availability 和 envelope expiry。默认不返回完整 payload；应用对外暴露时先做租户授权。

### 连续控制和恢复

```ts
await queuebit.runs.pause(runId);
await queuebit.runs.resume(runId);
await queuebit.runs.cancel(runId, { reason: 'campaign withdrawn' });
await queuebit.runs.retryFailed(failedRunId, {
  idempotencyKey: `recovery:${failedRunId}:1`
});
```

| 方法 | 语义 |
|---|---|
| `pause/resume` | 同一非终态 Run 的连续控制；blocked 修复后也 resume 原 Run |
| `cancel` | 停止创建新 Batch，收敛 active work，reason 首次原子写入后不被覆盖 |
| `retryFailed` | 仅对 mapper/processor 终止失败 work，创建新 recovery run，不改写原 Run |

recovery run 使用 Queuebit 保存的失败详情读取原失败 work，不调用原 `Source.freeze/load`，不重查已变化数据库。

## Completions API

```ts
const page = await queuebit.completions.list({
  runId,
  completionState: 'failed',
  limit: 100
});
for (const event of page.items) {
  await queuebit.completions.retry(event.id);
}
```

`list` 按稳定 event sequence 分页，可按 runId/batchId/type/state 过滤。`get` 返回单个持久化 event snapshot 或 `null`。`retry` 原子校验 event 仍为 failed，只重新打开 delivery，不改 executionState，不创建新业务 work。

<a id="runtime-registration"></a>
## Runtime registration

```ts
export default defineQueuebitRuntime({
  sources: {
    'paid-orders': defineQueuebitSource({ freeze, load })
  },
  mappers: {
    'receipt-jobs': defineQueuebitMapper(mapReceipt)
  },
  processors: {
    'send-receipt': defineQueuebitProcessor(sendReceipt)
  },
  completions: {
    'record-receipt-batch-result': defineQueuebitCompletionHandler(recordBatch),
    'record-receipt-run-result': defineQueuebitCompletionHandler(recordRun)
  }
});
```

`defineQueuebitRuntime()` 的输入可以只包含当前角色需要的 registry。普通后台 job 的 Worker 可以只注册 `processors`；只有 BatchRun 才需要 `sources`、`mappers` 和 `completions`。函数返回规范化后的 runtime，未传的 registry 会按空对象处理。

### Source

| 函数 | 输入 | 输出/不变式 |
|---|---|---|
| `freeze` | runId、input、signal | 可序列化 `boundary` 和初始 `cursor`；可选 `totalRecords` |
| `load` | runId、input、boundary、cursor、limit、signal | `{records,nextCursor,exhausted}`；非空页必须推进 cursor |

### Mapper

每条 record 返回 1 个 job、多个 jobs，或返回 `null` / `undefined` 表示 skipped。它是纯转换；一对多输出必须有稳定 `identity`。mapper 抛错会记录已保存失败详情，用于后续 recovery。

### Processor

接收 `jobId/runId/batchId/data/attempt/idempotencyKey/signal/logger`。超时只触发 AbortSignal；用户代码将 signal 传给下游，Redis 提交由 lease generation fencing 保护。

### Completion handler

接收持久化 `batch.settled/run.settled/run.cancelled` event，包含 execution/completion 语义、attempt、delivery generation 和汇总。以 event ID 去重外部写入。

## 公开错误结构

```ts
type QueuebitError = {
  code: string;
  message: string;
  details?: unknown;
};
```

`cause` 只供进程内诊断，不直接序列化到 CLI JSON/事件/持久错误。重试语义按各 API 方法和状态单独说明，不要从 error 对象上的额外字段推断。
