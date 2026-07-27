import type { ServerPolicyMode } from '../config';
import { QueuebitError } from '../errors';

export type RedisPreflightStatus = 'ready' | 'degraded' | 'not_ready';
export type RedisPreflightIssueCode =
  | 'redis-unreadable'
  | 'redis-version-unsupported'
  | 'redis-cluster-unsupported'
  | 'redis-role-not-primary'
  | 'redis-eviction-policy-unsafe'
  | 'redis-persistence-disabled'
  | 'redis-persistence-error';

export interface RedisPreflightIssue {
  code: RedisPreflightIssueCode;
  severity: 'warning' | 'error';
  message: string;
  details?: Record<string, unknown>;
}

export interface RedisPreflightCheck {
  name: string;
  ok: boolean;
  details?: Record<string, unknown>;
}

export interface RedisPreflightResult {
  status: RedisPreflightStatus;
  ready: boolean;
  mode: ServerPolicyMode;
  checks: RedisPreflightCheck[];
  issues: RedisPreflightIssue[];
}

export interface RedisPreflightClient {
  isReady?: boolean;
  info(section?: string): Promise<string>;
  configGet(parameter: string): Promise<unknown>;
  role(): Promise<unknown>;
}

export async function runRedisPreflight(
  client: RedisPreflightClient,
  mode: ServerPolicyMode
): Promise<RedisPreflightResult> {
  const checks: RedisPreflightCheck[] = [];
  const issues: RedisPreflightIssue[] = [];

  try {
    const [serverInfo, persistenceInfo, replicationInfo, policy, save] = await Promise.all([
      client.info('server'),
      client.info('persistence'),
      client.info('replication'),
      readConfig(client, 'maxmemory-policy'),
      readConfig(client, 'save')
    ]);
    const info = {
      ...parseInfo(serverInfo),
      ...parseInfo(persistenceInfo),
      ...parseInfo(replicationInfo)
    };

    checkRedisVersion(info, checks, issues);
    checkCluster(info, checks, issues);
    checkRole(info, await client.role(), checks, issues);
    checkEviction(policy, mode, checks, issues);
    checkPersistence(info, save, mode, checks, issues);
  } catch (cause) {
    issues.push({
      code: 'redis-unreadable',
      severity: 'error',
      message: 'Redis preflight could not read required server policy.',
      details: { cause }
    });
  }

  const ready = !issues.some(issue => issue.severity === 'error');
  const status: RedisPreflightStatus = ready
    ? issues.length === 0
      ? 'ready'
      : 'degraded'
    : 'not_ready';

  return { status, ready, mode, checks, issues };
}

export function assertRedisPreflightReady(result: RedisPreflightResult): void {
  if (result.ready) return;
  const hasCluster = result.issues.some(issue => issue.code === 'redis-cluster-unsupported');
  throw new QueuebitError({
    code: hasCluster ? 'QB_REDIS_CLUSTER_UNSUPPORTED' : 'QB_REDIS_PREFLIGHT_FAILED',
    message: 'Redis server policy is not ready for Queuebit.',
    details: result
  });
}

function parseInfo(source: string): Record<string, string> {
  const result: Record<string, string> = {};
  for (const line of source.split(/\r?\n/)) {
    if (line.length === 0 || line.startsWith('#')) continue;
    const separator = line.indexOf(':');
    if (separator === -1) continue;
    result[line.slice(0, separator)] = line.slice(separator + 1).trim();
  }
  return result;
}

async function readConfig(client: RedisPreflightClient, key: string): Promise<string | undefined> {
  const reply = await client.configGet(key);
  if (Array.isArray(reply)) {
    for (let index = 0; index < reply.length - 1; index += 2) {
      if (String(reply[index]).toLowerCase() === key.toLowerCase()) return String(reply[index + 1]);
    }
    return undefined;
  }
  if (reply instanceof Map) {
    const value = reply.get(key) ?? reply.get(key.toLowerCase());
    return value === undefined ? undefined : String(value);
  }
  if (reply !== null && typeof reply === 'object') {
    const record = reply as Record<string, unknown>;
    const value = record[key] ?? record[key.toLowerCase()];
    return value === undefined ? undefined : String(value);
  }
  return undefined;
}

function checkRedisVersion(
  info: Record<string, string>,
  checks: RedisPreflightCheck[],
  issues: RedisPreflightIssue[]
) {
  const version = info.redis_version ?? '0.0.0';
  const ok = compareRedisVersion(version, '7.2.0') >= 0;
  checks.push({ name: 'redis-version', ok, details: { version, minimum: '7.2.0' } });
  if (!ok) {
    issues.push({
      code: 'redis-version-unsupported',
      severity: 'error',
      message: 'Queuebit requires Redis >=7.2.',
      details: { version }
    });
  }
}

function checkCluster(
  info: Record<string, string>,
  checks: RedisPreflightCheck[],
  issues: RedisPreflightIssue[]
) {
  const enabled = info.cluster_enabled === '1';
  checks.push({ name: 'redis-cluster', ok: !enabled, details: { clusterEnabled: enabled } });
  if (enabled) {
    issues.push({
      code: 'redis-cluster-unsupported',
      severity: 'error',
      message: 'Queuebit v0.1 does not support Redis Cluster.'
    });
  }
}

function checkRole(
  info: Record<string, string>,
  roleReply: unknown,
  checks: RedisPreflightCheck[],
  issues: RedisPreflightIssue[]
) {
  const role = Array.isArray(roleReply) ? String(roleReply[0]) : info.role ?? 'unknown';
  const ok = role === 'master' || role === 'primary';
  checks.push({ name: 'redis-role', ok, details: { role } });
  if (!ok) {
    issues.push({
      code: 'redis-role-not-primary',
      severity: 'error',
      message: 'Queuebit must connect to a primary Redis endpoint.',
      details: { role }
    });
  }
}

function checkEviction(
  policy: string | undefined,
  mode: ServerPolicyMode,
  checks: RedisPreflightCheck[],
  issues: RedisPreflightIssue[]
) {
  const ok = policy === 'noeviction';
  checks.push({ name: 'redis-eviction-policy', ok, details: { policy } });
  if (!ok) {
    issues.push({
      code: 'redis-eviction-policy-unsafe',
      severity: policySeverity(mode),
      message: 'Queuebit requires Redis maxmemory-policy=noeviction.',
      details: { policy }
    });
  }
}

function checkPersistence(
  info: Record<string, string>,
  save: string | undefined,
  mode: ServerPolicyMode,
  checks: RedisPreflightCheck[],
  issues: RedisPreflightIssue[]
) {
  const aofEnabled = info.aof_enabled === '1';
  const saveConfigured = typeof save === 'string' && save.trim().length > 0;
  const persistenceOk = aofEnabled || saveConfigured;
  checks.push({
    name: 'redis-persistence',
    ok: persistenceOk,
    details: { aofEnabled, saveConfigured }
  });
  if (!persistenceOk) {
    issues.push({
      code: 'redis-persistence-disabled',
      severity: policySeverity(mode),
      message: 'Queuebit requires AOF or RDB persistence to be enabled.'
    });
  }

  const rdbOk = info.rdb_last_bgsave_status !== 'err';
  const aofOk = info.aof_last_bgrewrite_status !== 'err';
  if (!rdbOk || !aofOk) {
    issues.push({
      code: 'redis-persistence-error',
      severity: policySeverity(mode),
      message: 'Redis reports a recent persistence error.',
      details: {
        rdbLastBgsaveStatus: info.rdb_last_bgsave_status,
        aofLastBgrewriteStatus: info.aof_last_bgrewrite_status
      }
    });
  }
}

function policySeverity(mode: ServerPolicyMode): RedisPreflightIssue['severity'] {
  return mode === 'warn' ? 'warning' : 'error';
}

function compareRedisVersion(left: string, right: string): number {
  const leftParts = left.split(/[.-]/).map(part => Number.parseInt(part, 10) || 0);
  const rightParts = right.split(/[.-]/).map(part => Number.parseInt(part, 10) || 0);
  for (let index = 0; index < Math.max(leftParts.length, rightParts.length); index += 1) {
    const diff = (leftParts[index] ?? 0) - (rightParts[index] ?? 0);
    if (diff !== 0) return diff;
  }
  return 0;
}
