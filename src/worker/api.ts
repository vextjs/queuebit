import { randomUUID } from 'node:crypto';
import { canonicalizeInput } from '../canonical';
import type { QueuebitConfig } from '../config';
import { QueuebitError, type QueuebitErrorCode } from '../errors';
import type { JobSnapshot, JobState, QueuebitSerializedError } from '../jobs';
import {
  createQueuebitKeyBuilder,
  executeQueuebitScript,
  type QueuebitRedisCommandClient
} from '../redis';
import { registerWorkerScripts } from './scripts';
import type {
  ClaimedJob,
  WorkerClaimOptions,
  WorkerKernel,
  WorkerPromoteDueOptions,
  WorkerRecoverStalledOptions,
  WorkerRenewOptions
} from './types';

export interface QueuebitWorkerKernelOptions {
  config: QueuebitConfig;
  redis: QueuebitRedisCommandClient;
  queue: string;
  workerId?: string;
  now?: () => Date;
}

const segmentPattern = /^[A-Za-z0-9][A-Za-z0-9:_-]{0,191}$/;
const jobStateValues = new Set<JobState>([
  'waiting',
  'active',
  'delayed',
  'retrying',
  'completed',
  'failed',
  'cancelled'
]);

export function createQueuebitWorkerKernel(options: QueuebitWorkerKernelOptions): WorkerKernel {
  assertQueue(options.config, options.queue);
  const workerId = assertSegment('workerId', options.workerId ?? randomUUID());
  const keys = createQueuebitKeyBuilder(options.config);
  const scripts = registerWorkerScripts();
  const now = options.now ?? (() => new Date());

  async function claim<Data = unknown>(claimOptions: WorkerClaimOptions = {}): Promise<ClaimedJob<Data> | null> {
    const leaseMs = normalizeLeaseMs(options.config, claimOptions.leaseMs);
    const maxSkips = normalizeMaxSkips(claimOptions.maxSkips);
    const observedAt = now();
    const reply = await executeQueuebitScript(
      options.redis,
      scripts.claim,
      [
        keys.queueWaiting(options.queue),
        keys.queueActive(options.queue),
        keys.queueState(options.queue, 'waiting'),
        keys.queueState(options.queue, 'active'),
        keys.queueCounters(options.queue)
      ],
      [
        workerId,
        String(observedAt.getTime() + leaseMs),
        new Date(observedAt.getTime() + leaseMs).toISOString(),
        observedAt.toISOString(),
        String(maxSkips)
      ]
    );
    const jobId = parseJobIdReply(reply, 'worker.claim');
    if (jobId === null) return null;
    return getClaimedJobSnapshot<Data>(options.redis, keys.job(jobId));
  }

  async function renew<Data = unknown>(
    jobId: string,
    leaseGeneration: number,
    renewOptions: WorkerRenewOptions = {}
  ): Promise<ClaimedJob<Data>> {
    assertSegment('jobId', jobId);
    assertPositiveInteger('leaseGeneration', leaseGeneration);
    const leaseMs = normalizeLeaseMs(options.config, renewOptions.leaseMs);
    const observedAt = now();
    const reply = await executeQueuebitScript(
      options.redis,
      scripts.renew,
      [keys.job(jobId), keys.queueActive(options.queue)],
      [
        workerId,
        String(leaseGeneration),
        String(observedAt.getTime() + leaseMs),
        new Date(observedAt.getTime() + leaseMs).toISOString(),
        observedAt.toISOString()
      ]
    );
    const renewedJobId = parseRequiredJobIdReply(reply, 'worker.renew');
    return getClaimedJobSnapshot<Data>(options.redis, keys.job(renewedJobId));
  }

  async function complete<Data = unknown, Result = unknown>(
    jobId: string,
    leaseGeneration: number,
    result?: Result
  ): Promise<JobSnapshot<Data, Result>> {
    const payloadJson = result === undefined
      ? ''
      : canonicalizeResultPayload(options.config, result, 'result');
    return settle<Data, Result>(jobId, leaseGeneration, 'completed', payloadJson);
  }

  async function fail<Data = unknown>(
    jobId: string,
    leaseGeneration: number,
    error: QueuebitSerializedError | Error | string
  ): Promise<JobSnapshot<Data>> {
    const payloadJson = canonicalizeResultPayload(options.config, serializeFailure(error), 'failedReason');
    return settle<Data, unknown>(jobId, leaseGeneration, 'failed', payloadJson);
  }

  async function settle<Data = unknown, Result = unknown>(
    jobId: string,
    leaseGeneration: number,
    terminalState: 'completed' | 'failed',
    payloadJson: string
  ): Promise<JobSnapshot<Data, Result>> {
    assertSegment('jobId', jobId);
    assertPositiveInteger('leaseGeneration', leaseGeneration);
    const observedAt = now();
    const reply = await executeQueuebitScript(
      options.redis,
      scripts.settle,
      [
        keys.job(jobId),
        keys.queueActive(options.queue),
        keys.queueState(options.queue, 'active'),
        keys.queueState(options.queue, terminalState),
        keys.queueCounters(options.queue),
        keys.queueDue(options.queue),
        keys.queueState(options.queue, 'retrying')
      ],
      [
        workerId,
        String(leaseGeneration),
        terminalState,
        payloadJson,
        observedAt.toISOString(),
        String(observedAt.getTime())
      ]
    );
    const settledJobId = parseRequiredJobIdReply(reply, `worker.${terminalState}`);
    const snapshot = await getJobSnapshot<Data, Result>(options.redis, keys.job(settledJobId));
    if (snapshot === null) {
      throw new QueuebitError({
        code: 'QB_JOB_NOT_FOUND',
        message: 'Settled job could not be read back.',
        details: { jobId: settledJobId }
      });
    }
    return snapshot;
  }

  async function promoteDue(promoteOptions: WorkerPromoteDueOptions = {}): Promise<string[]> {
    const observedAt = now();
    const reply = await executeQueuebitScript(
      options.redis,
      scripts.promoteDue,
      [
        keys.queueDue(options.queue),
        keys.queueWaiting(options.queue),
        keys.queueState(options.queue, 'delayed'),
        keys.queueState(options.queue, 'retrying'),
        keys.queueState(options.queue, 'waiting'),
        keys.queueCounters(options.queue)
      ],
      [
        String(observedAt.getTime()),
        observedAt.toISOString(),
        String(normalizeLimit('limit', promoteOptions.limit ?? options.config.scheduler.promotionBatchSize))
      ]
    );
    return parseJobIdListReply(reply, 'worker.promoteDue');
  }

  async function recoverStalled(recoverOptions: WorkerRecoverStalledOptions = {}): Promise<string[]> {
    const observedAt = now();
    const maxStalledRecoveries = recoverOptions.maxStalledRecoveries
      ?? options.config.workerDefaults.maxStalledRecoveries;
    const failure = canonicalizeResultPayload(options.config, {
      name: 'QueuebitError',
      code: 'QB_JOB_STATE_CONFLICT',
      message: 'Job exceeded max stalled recoveries.'
    }, 'failedReason');
    const reply = await executeQueuebitScript(
      options.redis,
      scripts.recoverStalled,
      [
        keys.queueActive(options.queue),
        keys.queueWaiting(options.queue),
        keys.queueState(options.queue, 'active'),
        keys.queueState(options.queue, 'waiting'),
        keys.queueState(options.queue, 'failed'),
        keys.queueCounters(options.queue)
      ],
      [
        String(observedAt.getTime()),
        observedAt.toISOString(),
        String(normalizeLimit('limit', recoverOptions.limit ?? options.config.scheduler.promotionBatchSize)),
        String(normalizeNonNegativeInteger('maxStalledRecoveries', maxStalledRecoveries)),
        failure
      ]
    );
    return parseJobIdListReply(reply, 'worker.recoverStalled');
  }

  return {
    queue: options.queue,
    workerId,
    claim,
    renew,
    complete,
    fail,
    promoteDue,
    recoverStalled
  };
}

function canonicalizeResultPayload(config: QueuebitConfig, payload: unknown, label: 'result' | 'failedReason'): string {
  const json = canonicalizeInput(payload);
  const bytes = Buffer.byteLength(json);
  if (bytes > config.limits.maxJobResultBytes) {
    throw new QueuebitError({
      code: 'QB_JOB_LIMIT_EXCEEDED',
      message: `Job ${label} exceeds maxJobResultBytes.`,
      details: { actual: bytes, limit: config.limits.maxJobResultBytes }
    });
  }
  return json;
}

function serializeFailure(error: QueuebitSerializedError | Error | string): QueuebitSerializedError {
  if (typeof error === 'string') {
    return { name: 'Error', message: error };
  }
  if (error instanceof QueuebitError) {
    const serialized: QueuebitSerializedError = {
      name: error.name,
      code: error.code,
      message: error.message
    };
    if (error.details !== undefined) serialized.details = error.details;
    return serialized;
  }
  if (error instanceof Error) {
    return { name: error.name, message: error.message };
  }
  return error;
}

function normalizeLeaseMs(config: QueuebitConfig, leaseMs: number | undefined): number {
  if (leaseMs === undefined) return config.workerDefaults.leaseMs;
  assertPositiveInteger('leaseMs', leaseMs);
  return leaseMs;
}

function normalizeMaxSkips(maxSkips = 32): number {
  assertPositiveInteger('maxSkips', maxSkips);
  return maxSkips;
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

function assertPositiveInteger(label: string, value: number) {
  if (!Number.isInteger(value) || value < 1) {
    throw new QueuebitError({
      code: 'QB_JOB_INVALID',
      message: `${label} must be an integer >= 1.`,
      details: { label, value }
    });
  }
}

function normalizeLimit(label: string, value: number): number {
  assertPositiveInteger(label, value);
  return value;
}

function normalizeNonNegativeInteger(label: string, value: number): number {
  if (!Number.isInteger(value) || value < 0) {
    throw new QueuebitError({
      code: 'QB_JOB_INVALID',
      message: `${label} must be an integer >= 0.`,
      details: { label, value }
    });
  }
  return value;
}

function parseJobIdReply(reply: unknown, operation: string): string | null {
  const [tag, codeOrBody, message, detailsJson] = assertTaggedReply(reply);
  if (tag === 'ok') {
    const jobId = String(codeOrBody ?? '');
    return jobId.length === 0 ? null : jobId;
  }
  throw new QueuebitError({
    code: toWorkerErrorCode(String(codeOrBody)),
    message: String(message ?? `${operation} failed.`),
    details: parseOptionalJson(detailsJson)
  });
}

function parseRequiredJobIdReply(reply: unknown, operation: string): string {
  const jobId = parseJobIdReply(reply, operation);
  if (jobId !== null) return jobId;
  throw new QueuebitError({
    code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
    message: `${operation} script did not return a job id.`
  });
}

function parseJobIdListReply(reply: unknown, operation: string): string[] {
  const [tag, codeOrBody, message, detailsJson] = assertTaggedReply(reply);
  if (tag === 'ok') {
    const parsed = JSON.parse(String(codeOrBody ?? '[]'));
    if (!Array.isArray(parsed) || !parsed.every(item => typeof item === 'string')) {
      throw new QueuebitError({
        code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
        message: `${operation} script returned an invalid job id list.`,
        details: { reply }
      });
    }
    return parsed;
  }
  throw new QueuebitError({
    code: toWorkerErrorCode(String(codeOrBody)),
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

function toWorkerErrorCode(code: string): QueuebitErrorCode {
  if (
    code === 'QB_JOB_INVALID'
    || code === 'QB_JOB_LIMIT_EXCEEDED'
    || code === 'QB_JOB_NOT_FOUND'
    || code === 'QB_JOB_STATE_CONFLICT'
  ) {
    return code;
  }
  return 'QB_REDIS_SCRIPT_EXECUTION_FAILED';
}

async function getClaimedJobSnapshot<Data = unknown>(
  redis: QueuebitRedisCommandClient,
  jobKey: string
): Promise<ClaimedJob<Data>> {
  const record = await getJobRecord(redis, jobKey);
  if (Object.keys(record).length === 0) {
    throw new QueuebitError({
      code: 'QB_JOB_NOT_FOUND',
      message: 'Claimed job could not be read back.'
    });
  }
  const snapshot = hashRecordToSnapshot<Data>(record);
  if (snapshot.state !== 'active') {
    throw new QueuebitError({
      code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
      message: 'Claimed job is not active.',
      details: { state: snapshot.state }
    });
  }
  const { data, detailsExpired: _detailsExpired, ...rest } = snapshot;
  if (data === undefined) {
    throw new QueuebitError({
      code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
      message: 'Claimed job snapshot is missing live data.'
    });
  }
  return {
    ...rest,
    state: 'active',
    data,
    workerId: requiredField(record, 'workerId'),
    leaseGeneration: Number.parseInt(requiredField(record, 'leaseGeneration'), 10),
    leaseDeadlineAt: requiredField(record, 'leaseDeadlineAt')
  };
}

async function getJobSnapshot<Data = unknown, Result = unknown>(
  redis: QueuebitRedisCommandClient,
  jobKey: string
): Promise<JobSnapshot<Data, Result> | null> {
  const record = await getJobRecord(redis, jobKey);
  if (Object.keys(record).length === 0) return null;
  return hashRecordToSnapshot<Data, Result>(record);
}

async function getJobRecord(redis: QueuebitRedisCommandClient, jobKey: string): Promise<Record<string, string>> {
  const reply = await redis.sendCommand(['HGETALL', jobKey]);
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
    updatedAt: requiredField(record, 'updatedAt'),
    data: JSON.parse(requiredField(record, 'data')) as Data
  };
  assignOptional(snapshot, 'deduplicationKey', record.deduplicationKey);
  assignOptional(snapshot, 'idempotencyKey', record.idempotencyKey);
  assignOptional(snapshot, 'runId', record.runId);
  assignOptional(snapshot, 'batchId', record.batchId);
  assignOptional(snapshot, 'parentJobId', record.parentJobId);
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

function parseOptionalJson(value: unknown): unknown {
  if (value === undefined) return undefined;
  try {
    return JSON.parse(String(value));
  } catch {
    return value;
  }
}
