import type { QueuebitConfig } from '../config';
import { QueuebitError, type QueuebitErrorCode } from '../errors';
import {
  createQueuebitKeyBuilder,
  executeQueuebitScript,
  type QueuebitRedisCommandClient
} from '../redis';
import type { CompletionState } from '../runs';
import type { QueuebitCompletionEventType } from '../runtime';
import { registerCompletionsScripts } from './scripts';
import type {
  CompletionListQuery,
  CompletionEventSummary,
  CompletionSnapshot,
  CompletionsApi
} from './types';

export interface QueuebitCompletionsApiOptions {
  config: QueuebitConfig;
  redis: QueuebitRedisCommandClient;
  now?: () => Date;
}

const segmentPattern = /^[A-Za-z0-9][A-Za-z0-9:_-]{0,191}$/;
const completionStates = new Set<CompletionState>([
  'not_created',
  'not_required',
  'pending',
  'delivering',
  'retrying',
  'delivered',
  'failed'
]);
const completionTypes = new Set<QueuebitCompletionEventType>([
  'batch.settled',
  'run.settled',
  'run.cancelled'
]);

export function createQueuebitCompletionsApi(options: QueuebitCompletionsApiOptions): CompletionsApi {
  const keys = createQueuebitKeyBuilder(options.config);
  const scripts = registerCompletionsScripts();
  const now = options.now ?? (() => new Date());

  async function list(query: CompletionListQuery = {}) {
    if (query.runId !== undefined) assertSegment('runId', query.runId);
    if (query.batchId !== undefined) assertSegment('batchId', query.batchId);
    if (query.type !== undefined && !completionTypes.has(query.type)) {
      throw new QueuebitError({
        code: 'QB_COMPLETION_INVALID',
        message: `Invalid completion event type filter: ${query.type}.`,
        details: { type: query.type }
      });
    }
    if (query.completionState !== undefined && !completionStates.has(query.completionState)) {
      throw new QueuebitError({
        code: 'QB_COMPLETION_INVALID',
        message: `Invalid completionState filter: ${query.completionState}.`,
        details: { completionState: query.completionState }
      });
    }
    const limit = normalizeListLimit(query.limit);
    const cursor = normalizeCursor(query.cursor);
    const reply = await options.redis.sendCommand([
      'ZRANGEBYSCORE',
      keys.completionsIndex(),
      cursor === undefined ? '-inf' : `(${cursor}`,
      '+inf',
      'WITHSCORES',
      'LIMIT',
      '0',
      String(limit + 1)
    ]);
    const pairs = parseZrangeWithScores(reply);
    const visible = pairs.slice(0, limit);
    const snapshots = await Promise.all(visible.map(pair => getCompletionSnapshot(options.redis, keys.completion(pair.member))));
    const items = snapshots
      .filter((snapshot): snapshot is CompletionSnapshot => snapshot !== null)
      .filter(snapshot => query.runId === undefined || snapshot.runId === query.runId)
      .filter(snapshot => query.batchId === undefined || snapshot.batchId === query.batchId)
      .filter(snapshot => query.type === undefined || snapshot.type === query.type)
      .filter(snapshot => query.completionState === undefined || snapshot.completionState === query.completionState)
      .map(toCompletionSummary);
    const page = { items };
    if (pairs.length > limit && visible.length > 0) {
      const last = visible[visible.length - 1];
      if (last) Object.assign(page, { nextCursor: last.score });
    }
    return page;
  }

  async function get<Summary = unknown>(eventId: string): Promise<CompletionSnapshot<Summary> | null> {
    assertSegment('eventId', eventId);
    return getCompletionSnapshot<Summary>(options.redis, keys.completion(eventId));
  }

  async function retry<Summary = unknown>(eventId: string): Promise<CompletionSnapshot<Summary>> {
    assertSegment('eventId', eventId);
    const observedAt = now();
    const reply = await executeQueuebitScript(
      options.redis,
      scripts.retry,
      [keys.completion(eventId), keys.completionsDue()],
      [String(observedAt.getTime()), observedAt.toISOString(), observedAt.toISOString()]
    );
    const [tag, idOrCode, message, detailsJson] = assertTaggedReply(reply);
    if (tag === 'err') {
      throw new QueuebitError({
        code: toCompletionErrorCode(String(idOrCode)),
        message: String(message ?? 'completions.retry failed.'),
        details: parseOptionalJson(detailsJson)
      });
    }
    const snapshot = await get<Summary>(String(idOrCode));
    if (snapshot === null) {
      throw new QueuebitError({
        code: 'QB_COMPLETION_NOT_FOUND',
        message: 'Retried completion event could not be read back.',
        details: { eventId: String(idOrCode) }
      });
    }
    return snapshot;
  }

  return { list, get, retry };
}

export async function getCompletionSnapshot<Summary = unknown>(
  redis: QueuebitRedisCommandClient,
  completionKey: string
): Promise<CompletionSnapshot<Summary> | null> {
  const record = redisHashToRecord(await redis.sendCommand(['HGETALL', completionKey]));
  if (Object.keys(record).length === 0) return null;
  return hashRecordToCompletionSnapshot<Summary>(record);
}

function hashRecordToCompletionSnapshot<Summary>(record: Record<string, string>): CompletionSnapshot<Summary> {
  const detailsExpired = record.detailsExpired === '1';
  const snapshot: CompletionSnapshot<Summary> = {
    id: requiredField(record, 'id'),
    type: parseCompletionType(requiredField(record, 'type')),
    runId: requiredField(record, 'runId'),
    completionState: parseCompletionState(requiredField(record, 'completionState')),
    attempt: Number.parseInt(requiredField(record, 'attempt'), 10),
    attempts: Number.parseInt(requiredField(record, 'attempts'), 10),
    deliveryGeneration: Number.parseInt(requiredField(record, 'deliveryGeneration'), 10),
    createdAt: requiredField(record, 'createdAt'),
    updatedAt: requiredField(record, 'updatedAt')
  };
  assignOptional(snapshot, 'batchId', record.batchId);
  assignOptional(snapshot, 'handler', record.handler);
  assignOptional(snapshot, 'summaryDigest', record.summaryDigest);
  if (detailsExpired) {
    snapshot.detailsExpired = true;
    assignOptional(snapshot, 'detailsExpiredAt', record.detailsExpiredAt);
    return snapshot;
  }
  snapshot.summary = JSON.parse(requiredField(record, 'summary')) as Summary;
  assignOptional(snapshot, 'nextDueAt', record.nextDueAt);
  if (record.backoff !== undefined) snapshot.backoff = JSON.parse(record.backoff);
  if (record.lastError !== undefined) snapshot.lastError = JSON.parse(record.lastError);
  return snapshot;
}

function toCompletionSummary(snapshot: CompletionSnapshot): CompletionEventSummary {
  const summary: CompletionEventSummary = {
    id: snapshot.id,
    type: snapshot.type,
    runId: snapshot.runId,
    completionState: snapshot.completionState,
    attempt: snapshot.attempt,
    attempts: snapshot.attempts,
    deliveryGeneration: snapshot.deliveryGeneration,
    createdAt: snapshot.createdAt,
    updatedAt: snapshot.updatedAt
  };
  assignOptional(summary, 'batchId', snapshot.batchId);
  assignOptional(summary, 'handler', snapshot.handler);
  assignOptional(summary, 'summaryDigest', snapshot.summaryDigest);
  assignOptional(summary, 'detailsExpired', snapshot.detailsExpired);
  assignOptional(summary, 'detailsExpiredAt', snapshot.detailsExpiredAt);
  return summary;
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

function parseCompletionState(value: string): CompletionState {
  if (completionStates.has(value as CompletionState)) return value as CompletionState;
  throw new QueuebitError({
    code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
    message: `Redis stored an unknown completionState: ${value}.`
  });
}

function parseCompletionType(value: string): QueuebitCompletionEventType {
  if (completionTypes.has(value as QueuebitCompletionEventType)) return value as QueuebitCompletionEventType;
  throw new QueuebitError({
    code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
    message: `Redis stored an unknown completion event type: ${value}.`
  });
}

function requiredField(record: Record<string, string>, field: string): string {
  const value = record[field];
  if (value === undefined) {
    throw new QueuebitError({
      code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
      message: `Redis completion snapshot is missing field: ${field}.`,
      details: { field }
    });
  }
  return value;
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

function toCompletionErrorCode(code: string): QueuebitErrorCode {
  if (
    code === 'QB_COMPLETION_INVALID'
    || code === 'QB_COMPLETION_NOT_FOUND'
    || code === 'QB_COMPLETION_STATE_CONFLICT'
  ) {
    return code;
  }
  return 'QB_REDIS_SCRIPT_EXECUTION_FAILED';
}

function assertSegment(label: string, value: string): string {
  if (!segmentPattern.test(value)) {
    throw new QueuebitError({
      code: 'QB_COMPLETION_INVALID',
      message: `Invalid ${label}: "${value}".`,
      details: { label, value }
    });
  }
  return value;
}

function normalizeListLimit(limit = 50): number {
  if (!Number.isInteger(limit) || limit < 1 || limit > 100) {
    throw new QueuebitError({
      code: 'QB_COMPLETION_INVALID',
      message: 'completions.list limit must be an integer between 1 and 100.',
      details: { limit }
    });
  }
  return limit;
}

function normalizeCursor(cursor: string | undefined): string | undefined {
  if (cursor === undefined) return undefined;
  if (!/^[0-9]+$/.test(cursor)) {
    throw new QueuebitError({
      code: 'QB_COMPLETION_INVALID',
      message: 'completions.list cursor must be an opaque numeric cursor.',
      details: { cursor }
    });
  }
  return cursor;
}

function parseZrangeWithScores(reply: unknown): Array<{ member: string; score: string }> {
  if (!Array.isArray(reply)) {
    throw new QueuebitError({
      code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
      message: 'Redis returned an invalid completion index reply.',
      details: { reply }
    });
  }
  const pairs: Array<{ member: string; score: string }> = [];
  for (let index = 0; index < reply.length - 1; index += 2) {
    pairs.push({ member: String(reply[index]), score: String(reply[index + 1]) });
  }
  return pairs;
}

function parseOptionalJson(value: unknown): unknown {
  if (value === undefined) return undefined;
  try {
    return JSON.parse(String(value));
  } catch {
    return value;
  }
}

function assignOptional<T extends object, K extends keyof T>(target: T, key: K, value: T[K] | undefined) {
  if (value !== undefined) target[key] = value;
}
