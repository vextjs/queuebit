import { createHash, randomUUID } from 'node:crypto';
import { canonicalizeInput, createCanonicalDigest } from '../canonical';
import type { QueuebitConfig } from '../config';
import { QueuebitError, type QueuebitErrorCode } from '../errors';
import {
  noopQueuebitObservabilityRecorder,
  type QueuebitObservabilityRecorder
} from '../observability';
import {
  createQueuebitKeyBuilder,
  executeQueuebitScript,
  type QueuebitRedisCommandClient
} from '../redis';
import { registerJobsScripts } from './scripts';
import type {
  BulkJobEntry,
  CursorPage,
  JobAddOptions,
  JobListQuery,
  JobRetryFailedRequest,
  JobSnapshot,
  JobState,
  JobSummary,
  QueuebitSerializedError,
  JobsApi
} from './types';

export interface QueuebitJobsApiOptions {
  config: QueuebitConfig;
  redis: QueuebitRedisCommandClient;
  now?: () => Date;
  idGenerator?: () => string;
  observability?: QueuebitObservabilityRecorder;
}

interface PreparedJobEntry {
  jobId: string;
  jobKey: string;
  dedupeKey?: string;
  envelope: Record<string, unknown>;
}

interface InternalBulkJobEntry<Data = unknown> extends BulkJobEntry<Data> {
  internal?: {
    parentJobId?: string;
    runId?: string;
    batchId?: string;
  };
}

const jobStateValues = new Set<JobState>([
  'waiting',
  'active',
  'delayed',
  'retrying',
  'completed',
  'failed',
  'cancelled'
]);
const segmentPattern = /^[A-Za-z0-9][A-Za-z0-9:_-]{0,191}$/;

export function createQueuebitJobsApi(options: QueuebitJobsApiOptions): JobsApi {
  const keys = createQueuebitKeyBuilder(options.config);
  const scripts = registerJobsScripts();
  const now = options.now ?? (() => new Date());
  const idGenerator = options.idGenerator ?? (() => randomUUID());
  const observability = options.observability ?? noopQueuebitObservabilityRecorder;

  async function add<Data>(
    queue: string,
    name: string,
    data: Data,
    jobOptions?: JobAddOptions
  ): Promise<JobSnapshot<Data>> {
    const entry: BulkJobEntry<Data> = jobOptions === undefined
      ? { name, data }
      : { name, data, options: jobOptions };
    const [job] = await addBulk(queue, [entry]);
    if (!job) {
      throw new QueuebitError({
        code: 'QB_JOB_INVALID',
        message: 'jobs.add did not return a created job.'
      });
    }
    return job as JobSnapshot<Data>;
  }

  async function addBulk<Data>(
    queue: string,
    entries: Array<BulkJobEntry<Data>>
  ): Promise<Array<JobSnapshot<Data>>> {
    return addBulkInternal(queue, entries);
  }

  async function addBulkInternal<Data>(
    queue: string,
    entries: Array<InternalBulkJobEntry<Data>>
  ): Promise<Array<JobSnapshot<Data>>> {
      const observedAt = now();
      const prepared = prepareBulk(options.config, queue, entries, observedAt, idGenerator);
      const dedupeKeys = prepared.flatMap(entry => entry.dedupeKey ? [entry.dedupeKey] : []);
      const scriptKeys = [
        keys.queueCounters(queue),
        keys.queueWaiting(queue),
        keys.queueDue(queue),
        keys.queueJobs(queue),
        keys.queueState(queue, 'waiting'),
        keys.queueState(queue, 'delayed'),
        ...prepared.map(entry => entry.jobKey),
        ...dedupeKeys
      ];
      const queueConfig = options.config.queues[queue];
      const highJobs = queueConfig?.backpressure?.highWatermarkJobs;
      const lowJobs = queueConfig?.backpressure?.lowWatermarkJobs;
      const highBytes = queueConfig?.backpressure?.highWatermarkBytes;
      const lowBytes = queueConfig?.backpressure?.lowWatermarkBytes;
      const reply = await executeQueuebitScript(
        options.redis,
        scripts.addBulk,
        scriptKeys,
        [
          String(prepared.length),
          String(options.config.limits.maxBulkJobs),
          String(options.config.limits.maxBulkBytes),
          highJobs === undefined ? '' : String(highJobs),
          highBytes === undefined ? '' : String(highBytes),
          lowJobs === undefined ? '' : String(lowJobs),
          lowBytes === undefined ? '' : String(lowBytes),
          observedAt.toISOString(),
          String(sumBulkBytes(prepared)),
          ...prepared.map(entry => JSON.stringify(entry.envelope))
        ]
      );
      const jobIds = parseAddBulkReply(reply);
      const snapshots = await Promise.all(jobIds.map(jobId => getJobSnapshot<Data>(options.redis, keys.job(jobId))));
      const readBack = snapshots.map((snapshot, index) => {
        if (!snapshot) {
          throw new QueuebitError({
            code: 'QB_JOB_NOT_FOUND',
            message: 'jobs.addBulk created a job id that could not be read back.',
            details: { jobId: jobIds[index] }
          });
        }
        return snapshot;
      });
      observeSubmittedJobs(queue, prepared, observability);
      return readBack;
  }

  async function get<Data = unknown, Result = unknown>(
    jobId: string
  ): Promise<JobSnapshot<Data, Result> | null> {
    assertSegment('jobId', jobId);
    return getJobSnapshot<Data, Result>(options.redis, keys.job(jobId));
  }

  async function list(query: JobListQuery): Promise<CursorPage<JobSummary>> {
      assertQueue(options.config, query.queue);
      const limit = normalizeListLimit(query.limit);
      if (query.state !== undefined && !jobStateValues.has(query.state)) {
        throw new QueuebitError({
          code: 'QB_JOB_INVALID',
          message: `Invalid job state filter: ${query.state}.`,
          details: { state: query.state }
        });
      }
      const cursor = normalizeCursor(query.cursor);
      const indexKey = query.state === undefined
        ? keys.queueJobs(query.queue)
        : keys.queueState(query.queue, query.state);
      const reply = await options.redis.sendCommand([
        'ZRANGEBYSCORE',
        indexKey,
        cursor === undefined ? '-inf' : `(${cursor}`,
        '+inf',
        'WITHSCORES',
        'LIMIT',
        '0',
        String(limit + 1)
      ]);
      const pairs = parseZrangeWithScores(reply);
      const visible = pairs.slice(0, limit);
      const snapshots = await Promise.all(visible.map(pair => getJobSnapshot(options.redis, keys.job(pair.member))));
      const items = snapshots
        .filter((snapshot): snapshot is JobSnapshot => snapshot !== null)
        .map(toJobSummary);
      const page: CursorPage<JobSummary> = { items };
      if (pairs.length > limit && visible.length > 0) {
        const last = visible[visible.length - 1];
        if (last) page.nextCursor = last.score;
      }
      return page;
  }

  async function cancel(jobId: string): Promise<JobSnapshot> {
    assertSegment('jobId', jobId);
    const record = await getJobRecord(options.redis, keys.job(jobId));
    if (Object.keys(record).length === 0) {
      throw new QueuebitError({
        code: 'QB_JOB_NOT_FOUND',
        message: 'Job does not exist.',
        details: { jobId }
      });
    }
    const queue = requiredField(record, 'queue');
    const observedAt = now();
    const reply = await executeQueuebitScript(
      options.redis,
      scripts.cancel,
      [
        keys.job(jobId),
        keys.queueWaiting(queue),
        keys.queueDue(queue),
        keys.queueState(queue, 'waiting'),
        keys.queueState(queue, 'delayed'),
        keys.queueState(queue, 'retrying'),
        keys.queueState(queue, 'cancelled'),
        keys.queueCounters(queue)
      ],
      [observedAt.toISOString()]
    );
    const cancelledJobId = parseSingleJobIdReply(reply, 'jobs.cancel');
    const snapshot = await getJobSnapshot(options.redis, keys.job(cancelledJobId));
    if (snapshot === null) {
      throw new QueuebitError({
        code: 'QB_JOB_NOT_FOUND',
        message: 'Cancelled job could not be read back.',
        details: { jobId: cancelledJobId }
      });
    }
    return snapshot;
  }

  async function retryFailed(jobId: string, request: JobRetryFailedRequest): Promise<JobSnapshot> {
    assertSegment('jobId', jobId);
    const deduplicationKey = assertBoundedString('deduplicationKey', request.deduplicationKey);
    const record = await getJobRecord(options.redis, keys.job(jobId));
    if (Object.keys(record).length === 0) {
      throw new QueuebitError({
        code: 'QB_JOB_NOT_FOUND',
        message: 'Job does not exist.',
        details: { jobId }
      });
    }
    if (record.state !== 'failed') {
      throw new QueuebitError({
        code: 'QB_JOB_STATE_CONFLICT',
        message: 'jobs.retryFailed requires a failed direct job.',
        details: { jobId, state: record.state }
      });
    }
    if (record.runId !== undefined || record.batchId !== undefined) {
      throw new QueuebitError({
        code: 'QB_JOB_STATE_CONFLICT',
        message: 'BatchRun-owned jobs must be recovered through runs.retryFailed.',
        details: { jobId, runId: record.runId, batchId: record.batchId }
      });
    }
    if (record.detailsExpired === '1') {
      throw new QueuebitError({
        code: 'QB_JOB_STATE_CONFLICT',
        message: 'jobs.retryFailed cannot replay an expired job snapshot.',
        details: { jobId }
      });
    }
    const retryOptions = retryOptionsFromRecord(record, deduplicationKey);
    const [replacement] = await addBulkInternal(requiredField(record, 'queue'), [{
      name: requiredField(record, 'name'),
      data: JSON.parse(requiredField(record, 'data')),
      options: retryOptions,
      internal: { parentJobId: jobId }
    }]);
    if (replacement === undefined) {
      throw new QueuebitError({
        code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
        message: 'jobs.retryFailed did not create a replacement job.',
        details: { jobId }
      });
    }
    return replacement;
  }

  return { add, addBulk, get, list, cancel, retryFailed };
}

function prepareBulk<Data>(
  config: QueuebitConfig,
  queue: string,
  entries: Array<InternalBulkJobEntry<Data>>,
  observedAt: Date,
  idGenerator: () => string
): PreparedJobEntry[] {
  assertQueue(config, queue);
  if (entries.length === 0) {
    throw new QueuebitError({
      code: 'QB_JOB_INVALID',
      message: 'jobs.addBulk requires at least one entry.'
    });
  }
  if (entries.length > config.limits.maxBulkJobs) {
    throw new QueuebitError({
      code: 'QB_JOB_LIMIT_EXCEEDED',
      message: 'jobs.addBulk exceeds maxBulkJobs.',
      details: { actual: entries.length, limit: config.limits.maxBulkJobs }
    });
  }

  const duplicateDedupe = new Set<string>();
  const prepared = entries.map((entry, index) =>
    prepareEntry(config, queue, entry, observedAt, idGenerator, index)
  );
  for (const entry of prepared) {
    const dedupe = entry.envelope.deduplicationKey;
    if (typeof dedupe !== 'string' || dedupe.length === 0) continue;
    if (duplicateDedupe.has(dedupe)) {
      throw new QueuebitError({
        code: 'QB_JOB_DEDUPLICATION_CONFLICT',
        message: 'jobs.addBulk cannot contain the same deduplicationKey twice.',
        details: { deduplicationKey: dedupe }
      });
    }
    duplicateDedupe.add(dedupe);
  }

  const bulkBytes = sumBulkBytes(prepared);
  if (bulkBytes > config.limits.maxBulkBytes) {
    throw new QueuebitError({
      code: 'QB_JOB_LIMIT_EXCEEDED',
      message: 'jobs.addBulk exceeds maxBulkBytes.',
      details: { actual: bulkBytes, limit: config.limits.maxBulkBytes }
    });
  }
  return prepared.map((entry, index) => ({
    ...entry,
    envelope: {
      ...entry.envelope,
      dedupeKeyPosition: entry.dedupeKey
        ? prepared.slice(0, index + 1).filter(item => item.dedupeKey).length
        : 0
    }
  }));
}

function prepareEntry<Data>(
  config: QueuebitConfig,
  queue: string,
  entry: InternalBulkJobEntry<Data>,
  observedAt: Date,
  idGenerator: () => string,
  index: number
): PreparedJobEntry {
  assertSegment('name', entry.name);
  const options = normalizeJobOptions(entry.options);
  const digest = createCanonicalDigest(entry.data);
  const dataBytes = Buffer.byteLength(digest.json);
  if (dataBytes > config.limits.maxJobDataBytes) {
    throw new QueuebitError({
      code: 'QB_JOB_LIMIT_EXCEEDED',
      message: 'Job data exceeds maxJobDataBytes.',
      details: { index, actual: dataBytes, limit: config.limits.maxJobDataBytes }
    });
  }
  const jobId = idGenerator();
  assertSegment('jobId', jobId);
  const keys = createQueuebitKeyBuilder(config);
  const createdAt = observedAt.toISOString();
  const delayMs = options.delayMs ?? 0;
  const state: JobState = delayMs > 0 ? 'delayed' : 'waiting';
  const delayUntilMs = observedAt.getTime() + delayMs;
  const optionsJson = canonicalizeInput(options);
  const dedupeKey = options.deduplicationKey === undefined
    ? undefined
    : keys.jobKey(createDedupeDigest(queue, options.deduplicationKey));

  return {
    jobId,
    jobKey: keys.job(jobId),
    ...(dedupeKey === undefined ? {} : { dedupeKey }),
    envelope: {
      jobId,
      queue,
      name: entry.name,
      state,
      attempts: options.attempts ?? 1,
      createdAt,
      updatedAt: createdAt,
      dataJson: digest.json,
      dataDigest: digest.sha256,
      dataBytes,
      optionsJson,
      delayUntilMs,
      deduplicationKey: options.deduplicationKey ?? '',
      idempotencyKey: options.idempotencyKey ?? '',
      parentJobId: entry.internal?.parentJobId ?? '',
      runId: entry.internal?.runId ?? '',
      batchId: entry.internal?.batchId ?? ''
    }
  };
}

function normalizeJobOptions(options: JobAddOptions = {}): JobAddOptions {
  const normalized: JobAddOptions = {};
  if (options.attempts !== undefined) {
    assertInteger('attempts', options.attempts, 1);
    normalized.attempts = options.attempts;
  }
  if (options.timeoutMs !== undefined) {
    assertInteger('timeoutMs', options.timeoutMs, 1);
    normalized.timeoutMs = options.timeoutMs;
  }
  if (options.delayMs !== undefined) {
    assertInteger('delayMs', options.delayMs, 0);
    normalized.delayMs = options.delayMs;
  }
  if (options.deduplicationKey !== undefined) {
    normalized.deduplicationKey = assertBoundedString('deduplicationKey', options.deduplicationKey);
  }
  if (options.idempotencyKey !== undefined) {
    normalized.idempotencyKey = assertBoundedString('idempotencyKey', options.idempotencyKey);
  }
  if (options.backoff !== undefined) {
    if (options.backoff.type !== 'fixed' && options.backoff.type !== 'exponential') {
      throw new QueuebitError({
        code: 'QB_JOB_INVALID',
        message: `Invalid backoff type: ${options.backoff.type}.`
      });
    }
    assertInteger('backoff.delayMs', options.backoff.delayMs, 1);
    const backoff: NonNullable<JobAddOptions['backoff']> = {
      type: options.backoff.type,
      delayMs: options.backoff.delayMs
    };
    if (options.backoff.maxDelayMs !== undefined) {
      assertInteger('backoff.maxDelayMs', options.backoff.maxDelayMs, 1);
      backoff.maxDelayMs = options.backoff.maxDelayMs;
    }
    if (options.backoff.jitter !== undefined) {
      if (typeof options.backoff.jitter !== 'number' || options.backoff.jitter < 0 || options.backoff.jitter > 1) {
        throw new QueuebitError({
          code: 'QB_JOB_INVALID',
          message: 'backoff.jitter must be between 0 and 1.',
          details: { jitter: options.backoff.jitter }
        });
      }
      backoff.jitter = options.backoff.jitter;
    }
    normalized.backoff = backoff;
  }
  return normalized;
}

function assertQueue(config: QueuebitConfig, queue: string) {
  assertSegment('queue', queue);
  if (config.queues[queue] === undefined) {
    throw new QueuebitError({
      code: 'QB_JOB_INVALID',
      message: `Queue "${queue}" is not declared in Queuebit config.`,
      details: { queue }
    });
  }
}

function assertSegment(label: string, value: string): string {
  if (!segmentPattern.test(value)) {
    throw new QueuebitError({
      code: 'QB_JOB_INVALID',
      message: `Invalid ${label}: "${value}".`,
      details: { label, value }
    });
  }
  return value;
}

function assertInteger(label: string, value: number, minimum: number) {
  if (!Number.isInteger(value) || value < minimum) {
    throw new QueuebitError({
      code: 'QB_JOB_INVALID',
      message: `${label} must be an integer >= ${minimum}.`,
      details: { label, value, minimum }
    });
  }
}

function assertBoundedString(label: string, value: string): string {
  if (typeof value !== 'string' || value.length === 0 || value.length > 512) {
    throw new QueuebitError({
      code: 'QB_JOB_INVALID',
      message: `${label} must be a non-empty string of at most 512 characters.`,
      details: { label }
    });
  }
  return value;
}

function sumBulkBytes(entries: PreparedJobEntry[]): number {
  return entries.reduce((sum, entry) => sum + Number(entry.envelope.dataBytes), 0);
}

function observeSubmittedJobs(
  queue: string,
  entries: PreparedJobEntry[],
  observability: QueuebitObservabilityRecorder
): void {
  const source = entries.some(entry => typeof entry.envelope.runId === 'string' && entry.envelope.runId.length > 0)
    ? 'batch'
    : 'direct';
  const labels = { queue, source };
  observability.incrementCounter('jobs_submitted_total', entries.length, labels);
  observability.incrementCounter('job_data_bytes_submitted_total', sumBulkBytes(entries), labels);
}

function createDedupeDigest(queue: string, key: string): string {
  return createHash('sha256')
    .update('queuebit-job-dedupe-v1')
    .update('\0')
    .update(queue)
    .update('\0')
    .update(key)
    .digest('hex');
}

function parseAddBulkReply(reply: unknown): string[] {
  const [tag, codeOrBody, message, detailsJson] = assertTaggedReply(reply);
  if (tag === 'ok') {
    const parsed = JSON.parse(String(codeOrBody));
    if (!Array.isArray(parsed) || !parsed.every(item => typeof item === 'string')) {
      throw new QueuebitError({
        code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
        message: 'jobs.addBulk script returned an invalid job id list.',
        details: { reply }
      });
    }
    return parsed;
  }
  throw new QueuebitError({
    code: toJobErrorCode(String(codeOrBody)),
    message: String(message ?? 'jobs.addBulk failed.'),
    details: parseOptionalJson(detailsJson)
  });
}

function parseSingleJobIdReply(reply: unknown, operation: string): string {
  const [tag, codeOrBody, message, detailsJson] = assertTaggedReply(reply);
  if (tag === 'ok') {
    const jobId = String(codeOrBody ?? '');
    if (jobId.length > 0) return jobId;
    throw new QueuebitError({
      code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
      message: `${operation} script did not return a job id.`
    });
  }
  throw new QueuebitError({
    code: toJobErrorCode(String(codeOrBody)),
    message: String(message ?? `${operation} failed.`),
    details: parseOptionalJson(detailsJson)
  });
}

function assertTaggedReply(reply: unknown): unknown[] {
  if (!Array.isArray(reply) || (reply[0] !== 'ok' && reply[0] !== 'err')) {
    throw new QueuebitError({
      code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
      message: 'Redis script returned an unknown reply shape.',
      details: { reply }
    });
  }
  return reply;
}

function toJobErrorCode(code: string): QueuebitErrorCode {
  if (
    code === 'QB_JOB_LIMIT_EXCEEDED'
    || code === 'QB_JOB_DEDUPLICATION_CONFLICT'
    || code === 'QB_JOB_INVALID'
    || code === 'QB_JOB_NOT_FOUND'
    || code === 'QB_JOB_STATE_CONFLICT'
    || code === 'QB_BACKPRESSURE_REJECTED'
    || code === 'QB_BACKPRESSURE_REQUEST_TOO_LARGE'
  ) {
    return code;
  }
  return 'QB_REDIS_SCRIPT_EXECUTION_FAILED';
}

async function getJobSnapshot<Data = unknown, Result = unknown>(
  redis: QueuebitRedisCommandClient,
  jobKey: string
): Promise<JobSnapshot<Data, Result> | null> {
  const record = await getJobRecord(redis, jobKey);
  if (Object.keys(record).length === 0) return null;
  return hashRecordToSnapshot<Data, Result>(record);
}

async function getJobRecord(
  redis: QueuebitRedisCommandClient,
  jobKey: string
): Promise<Record<string, string>> {
  const reply = await redis.sendCommand(['HGETALL', jobKey]);
  return redisHashToRecord(reply);
}

function redisHashToRecord(reply: unknown): Record<string, string> {
  if (Array.isArray(reply)) {
    const record: Record<string, string> = {};
    for (let index = 0; index < reply.length - 1; index += 2) {
      record[String(reply[index])] = String(reply[index + 1]);
    }
    return record;
  }
  if (reply !== null && typeof reply === 'object') {
    const record: Record<string, string> = {};
    for (const [key, value] of Object.entries(reply as Record<string, unknown>)) {
      record[key] = String(value);
    }
    return record;
  }
  return {};
}

function hashRecordToSnapshot<Data = unknown, Result = unknown>(
  record: Record<string, string>
): JobSnapshot<Data, Result> {
  const snapshot: JobSnapshot<Data, Result> = {
    id: requiredField(record, 'id'),
    queue: requiredField(record, 'queue'),
    name: requiredField(record, 'name'),
    state: parseJobState(requiredField(record, 'state')),
    attempt: Number.parseInt(requiredField(record, 'attempt'), 10),
    attempts: Number.parseInt(requiredField(record, 'attempts'), 10),
    createdAt: requiredField(record, 'createdAt'),
    updatedAt: requiredField(record, 'updatedAt')
  };
  assignOptional(snapshot, 'deduplicationKey', record.deduplicationKey);
  assignOptional(snapshot, 'idempotencyKey', record.idempotencyKey);
  assignOptional(snapshot, 'runId', record.runId);
  assignOptional(snapshot, 'batchId', record.batchId);
  assignOptional(snapshot, 'parentJobId', record.parentJobId);
  assignOptional(snapshot, 'dataDigest', record.dataDigest);
  assignOptional(snapshot, 'detailsExpiredAt', record.detailsExpiredAt);
  if (record.detailsExpired === '1') {
    snapshot.detailsExpired = true;
    return snapshot;
  }
  snapshot.data = JSON.parse(requiredField(record, 'data')) as Data;
  if (record.result !== undefined) snapshot.result = JSON.parse(record.result) as Result;
  if (record.failedReason !== undefined) {
    snapshot.failedReason = JSON.parse(record.failedReason) as QueuebitSerializedError;
  }
  return snapshot;
}

function parseJobState(value: string): JobState {
  if (jobStateValues.has(value as JobState)) return value as JobState;
  throw new QueuebitError({
    code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
    message: `Redis stored an unknown job state: ${value}.`,
    details: { state: value }
  });
}

function requiredField(record: Record<string, string>, field: string): string {
  const value = record[field];
  if (value === undefined) {
    throw new QueuebitError({
      code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
      message: `Redis job snapshot is missing field: ${field}.`,
      details: { field }
    });
  }
  return value;
}

function assignOptional<T extends object, K extends keyof T>(target: T, key: K, value: T[K] | undefined) {
  if (value !== undefined) target[key] = value;
}

function normalizeListLimit(limit = 50): number {
  if (!Number.isInteger(limit) || limit < 1 || limit > 100) {
    throw new QueuebitError({
      code: 'QB_JOB_INVALID',
      message: 'jobs.list limit must be an integer between 1 and 100.',
      details: { limit }
    });
  }
  return limit;
}

function normalizeCursor(cursor: string | undefined): string | undefined {
  if (cursor === undefined) return undefined;
  if (!/^[0-9]+$/.test(cursor)) {
    throw new QueuebitError({
      code: 'QB_JOB_INVALID',
      message: 'jobs.list cursor must be an opaque numeric creation cursor.',
      details: { cursor }
    });
  }
  return cursor;
}

function parseZrangeWithScores(reply: unknown): Array<{ member: string; score: string }> {
  if (!Array.isArray(reply)) {
    throw new QueuebitError({
      code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
      message: 'Redis returned an invalid list index reply.',
      details: { reply }
    });
  }
  const pairs: Array<{ member: string; score: string }> = [];
  for (let index = 0; index < reply.length - 1; index += 2) {
    pairs.push({ member: String(reply[index]), score: String(reply[index + 1]) });
  }
  return pairs;
}

function toJobSummary(snapshot: JobSnapshot): JobSummary {
  const summary: JobSummary = {
    id: snapshot.id,
    queue: snapshot.queue,
    name: snapshot.name,
    state: snapshot.state,
    attempt: snapshot.attempt,
    attempts: snapshot.attempts,
    createdAt: snapshot.createdAt,
    updatedAt: snapshot.updatedAt
  };
  assignOptional(summary, 'runId', snapshot.runId);
  assignOptional(summary, 'batchId', snapshot.batchId);
  assignOptional(summary, 'parentJobId', snapshot.parentJobId);
  assignOptional(summary, 'dataDigest', snapshot.dataDigest);
  assignOptional(summary, 'detailsExpired', snapshot.detailsExpired);
  assignOptional(summary, 'detailsExpiredAt', snapshot.detailsExpiredAt);
  return summary;
}

function retryOptionsFromRecord(record: Record<string, string>, deduplicationKey: string): JobAddOptions {
  const parsed = parseStoredOptions(record.options);
  const retryOptions: JobAddOptions = {
    attempts: Number.parseInt(requiredField(record, 'attempts'), 10),
    deduplicationKey
  };
  if (typeof parsed.timeoutMs === 'number') retryOptions.timeoutMs = parsed.timeoutMs;
  if (parsed.backoff !== undefined) retryOptions.backoff = parsed.backoff;
  if (record.idempotencyKey !== undefined) retryOptions.idempotencyKey = record.idempotencyKey;
  return retryOptions;
}

function parseStoredOptions(value: string | undefined): JobAddOptions {
  if (value === undefined || value.length === 0) return {};
  const parsed = JSON.parse(value) as JobAddOptions;
  const options: JobAddOptions = {};
  if (typeof parsed.attempts === 'number') options.attempts = parsed.attempts;
  if (typeof parsed.timeoutMs === 'number') options.timeoutMs = parsed.timeoutMs;
  if (parsed.backoff !== undefined) options.backoff = parsed.backoff;
  if (typeof parsed.deduplicationKey === 'string') options.deduplicationKey = parsed.deduplicationKey;
  if (typeof parsed.idempotencyKey === 'string') options.idempotencyKey = parsed.idempotencyKey;
  return options;
}

function parseOptionalJson(value: unknown): unknown {
  if (value === undefined) return undefined;
  try {
    return JSON.parse(String(value));
  } catch {
    return value;
  }
}
