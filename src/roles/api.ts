import type { QueuebitConfig } from '../config';
import { QueuebitError } from '../errors';
import {
  createQueuebitKeyBuilder,
  type QueuebitRedisCommandClient
} from '../redis';
import type {
  QueuebitRoleDrainRequest,
  QueuebitRoleHeartbeatInput,
  QueuebitRoleHeartbeatResult,
  QueuebitRoleKind,
  QueuebitRoleListOptions,
  QueuebitRoleListResult,
  QueuebitRoleMetadata,
  QueuebitRoleSnapshot,
  QueuebitRoleStatus,
  QueuebitRoleUnregisterInput,
  QueuebitRolesApi
} from './types';

export interface QueuebitRolesApiOptions {
  config: QueuebitConfig;
  redis: QueuebitRedisCommandClient;
  now?: () => Date;
}

const defaultDomain = 'default';
const defaultHeartbeatTtlMs = 15_000;
const maxRoleListLimit = 1_000;
const segmentPattern = /^[A-Za-z0-9][A-Za-z0-9:_-]{0,191}$/;

export function createQueuebitRolesApi(options: QueuebitRolesApiOptions): QueuebitRolesApi {
  const keys = createQueuebitKeyBuilder(options.config);
  const now = options.now ?? (() => new Date());

  async function heartbeat(input: QueuebitRoleHeartbeatInput): Promise<QueuebitRoleHeartbeatResult> {
    const role = assertRole(input.role);
    const domain = assertSegment('domain', input.domain);
    const identity = assertSegment('identity', input.identity);
    const status = assertStatus(input.status);
    const observedAt = now();
    const heartbeatTtlMs = normalizePositiveInteger(
      'heartbeatTtlMs',
      input.heartbeatTtlMs ?? defaultHeartbeatTtlMs
    );
    const deadlineMs = observedAt.getTime() + heartbeatTtlMs;
    const recordKey = keys.roleMember(role, domain, identity);
    const fields: string[] = [
      'role', role,
      'domain', domain,
      'identity', identity,
      'status', status,
      'lastHeartbeatAt', observedAt.toISOString(),
      'heartbeatDeadlineAt', new Date(deadlineMs).toISOString(),
      'heartbeatDeadlineMs', String(deadlineMs),
      'heartbeatTtlMs', String(heartbeatTtlMs)
    ];
    appendOptional(fields, 'startedAt', input.startedAt);
    appendOptional(fields, 'stoppedAt', input.stoppedAt);
    if (input.metadata !== undefined) fields.push('metadata', JSON.stringify(input.metadata));

    await options.redis.sendCommand(['HSET', recordKey, ...fields]);
    await options.redis.sendCommand(['ZADD', keys.role(role, domain), String(deadlineMs), identity]);
    const snapshot = await readRequiredRole(role, domain, identity);
    return {
      snapshot,
      drainRequested: snapshot.drainRequestedAt !== undefined && snapshot.status !== 'draining'
    };
  }

  async function get(input: QueuebitRoleUnregisterInput): Promise<QueuebitRoleSnapshot | null> {
    const role = assertRole(input.role);
    const domain = assertSegment('domain', input.domain);
    const identity = assertSegment('identity', input.identity);
    return readRole(role, domain, identity);
  }

  async function list(optionsInput: QueuebitRoleListOptions): Promise<QueuebitRoleListResult> {
    const role = assertRole(optionsInput.role);
    const domain = assertSegment('domain', optionsInput.domain ?? defaultDomain);
    const includeStale = optionsInput.includeStale === true;
    const limit = normalizeListLimit(optionsInput.limit);
    const observedAt = now();
    const min = includeStale ? '-inf' : `(${Math.max(0, observedAt.getTime() - 1)}`;
    const identities = await options.redis.sendCommand([
      'ZRANGEBYSCORE',
      keys.role(role, domain),
      min,
      '+inf',
      'LIMIT',
      '0',
      String(limit)
    ]);
    const items: QueuebitRoleSnapshot[] = [];
    for (const identity of normalizeStringArray(identities)) {
      const snapshot = await readRole(role, domain, identity);
      if (snapshot === null) continue;
      if (!includeStale && snapshot.stale) continue;
      items.push(snapshot);
    }
    return { role, domain, includeStale, now: observedAt.toISOString(), items };
  }

  async function requestDrain(input: QueuebitRoleDrainRequest): Promise<QueuebitRoleSnapshot> {
    const role = assertRole(input.role);
    const domain = assertSegment('domain', input.domain ?? defaultDomain);
    const identity = assertSegment('identity', input.identity);
    await readRequiredRole(role, domain, identity);
    const observedAt = now().toISOString();
    const fields = ['drainRequestedAt', observedAt];
    appendOptional(fields, 'drainReason', input.reason);
    await options.redis.sendCommand(['HSET', keys.roleMember(role, domain, identity), ...fields]);
    return readRequiredRole(role, domain, identity);
  }

  async function unregister(input: QueuebitRoleUnregisterInput): Promise<void> {
    const role = assertRole(input.role);
    const domain = assertSegment('domain', input.domain);
    const identity = assertSegment('identity', input.identity);
    await options.redis.sendCommand(['DEL', keys.roleMember(role, domain, identity)]);
    await options.redis.sendCommand(['ZREM', keys.role(role, domain), identity]);
  }

  async function readRequiredRole(
    role: QueuebitRoleKind,
    domain: string,
    identity: string
  ): Promise<QueuebitRoleSnapshot> {
    const snapshot = await readRole(role, domain, identity);
    if (snapshot !== null) return snapshot;
    throw new QueuebitError({
      code: 'QB_ROLE_NOT_FOUND',
      message: 'Queuebit role heartbeat does not exist.',
      details: { role, domain, identity }
    });
  }

  async function readRole(
    role: QueuebitRoleKind,
    domain: string,
    identity: string
  ): Promise<QueuebitRoleSnapshot | null> {
    const record = await options.redis.sendCommand(['HGETALL', keys.roleMember(role, domain, identity)]);
    return parseRoleRecord(record, now().getTime());
  }

  return { heartbeat, get, list, requestDrain, unregister };
}

function parseRoleRecord(record: unknown, nowMs: number): QueuebitRoleSnapshot | null {
  const fields = normalizeRecord(record);
  if (Object.keys(fields).length === 0) return null;
  const role = assertRole(fields.role as QueuebitRoleKind);
  const domain = assertSegment('domain', String(fields.domain ?? ''));
  const identity = assertSegment('identity', String(fields.identity ?? ''));
  const status = assertStatus(fields.status as QueuebitRoleStatus);
  const deadlineMs = Number.parseInt(String(fields.heartbeatDeadlineMs ?? '0'), 10);
  const snapshot: QueuebitRoleSnapshot = {
    role,
    domain,
    identity,
    status,
    lastHeartbeatAt: String(fields.lastHeartbeatAt ?? ''),
    heartbeatDeadlineAt: String(fields.heartbeatDeadlineAt ?? ''),
    heartbeatTtlMs: Number.parseInt(String(fields.heartbeatTtlMs ?? defaultHeartbeatTtlMs), 10),
    stale: !Number.isFinite(deadlineMs) || deadlineMs < nowMs
  };
  appendSnapshotOptional(snapshot, 'startedAt', fields.startedAt);
  appendSnapshotOptional(snapshot, 'stoppedAt', fields.stoppedAt);
  appendSnapshotOptional(snapshot, 'drainRequestedAt', fields.drainRequestedAt);
  appendSnapshotOptional(snapshot, 'drainReason', fields.drainReason);
  const metadata = fields.metadata === undefined ? undefined : parseMetadata(fields.metadata);
  if (metadata !== undefined) snapshot.metadata = metadata;
  return snapshot;
}

function normalizeRecord(record: unknown): Record<string, string> {
  if (record === null || typeof record !== 'object') return {};
  const normalized: Record<string, string> = {};
  for (const [key, value] of Object.entries(record)) normalized[key] = String(value);
  return normalized;
}

function normalizeStringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.map(item => String(item)) : [];
}

function parseMetadata(value: unknown): QueuebitRoleMetadata | undefined {
  try {
    const parsed = JSON.parse(String(value));
    return parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed)
      ? parsed as QueuebitRoleMetadata
      : undefined;
  } catch {
    return undefined;
  }
}

function assertRole(role: QueuebitRoleKind): QueuebitRoleKind {
  if (role === 'worker' || role === 'coordinator') return role;
  throw new QueuebitError({
    code: 'QB_ROLE_INVALID',
    message: 'Queuebit role must be "worker" or "coordinator".',
    details: { role }
  });
}

function assertStatus(status: QueuebitRoleStatus): QueuebitRoleStatus {
  if (status === 'running' || status === 'draining' || status === 'stopped') return status;
  throw new QueuebitError({
    code: 'QB_ROLE_INVALID',
    message: 'Queuebit role status must be running, draining, or stopped.',
    details: { status }
  });
}

function assertSegment(label: string, value: string): string {
  if (!segmentPattern.test(value)) {
    throw new QueuebitError({
      code: 'QB_ROLE_INVALID',
      message: `Invalid Queuebit role ${label}: "${value}".`,
      details: { label, value }
    });
  }
  return value;
}

function normalizePositiveInteger(label: string, value: number): number {
  if (!Number.isInteger(value) || value < 1) {
    throw new QueuebitError({
      code: 'QB_ROLE_INVALID',
      message: `${label} must be an integer >= 1.`,
      details: { label, value }
    });
  }
  return value;
}

function normalizeListLimit(value: number | undefined): number {
  if (value === undefined) return 100;
  return Math.min(maxRoleListLimit, normalizePositiveInteger('limit', value));
}

function appendOptional(fields: string[], key: string, value: string | undefined): void {
  if (value !== undefined) fields.push(key, value);
}

function appendSnapshotOptional<K extends keyof QueuebitRoleSnapshot>(
  snapshot: QueuebitRoleSnapshot,
  key: K,
  value: unknown
): void {
  if (value !== undefined && String(value).length > 0) {
    (snapshot as unknown as Record<string, unknown>)[String(key)] = String(value);
  }
}
