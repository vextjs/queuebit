import { QueuebitError } from '../errors';
import type { QueuebitConfig } from '../config';

const keySegmentPattern = /^[A-Za-z0-9][A-Za-z0-9:_-]{0,191}$/;
const digestPattern = /^[a-f0-9]{64}$/;

export interface QueuebitKeyBuilder {
  readonly namespace: string;
  readonly prefix: string;
  meta(): string;
  queueCounters(queue: string): string;
  queueWaiting(queue: string): string;
  queueDue(queue: string): string;
  queueActive(queue: string): string;
  queueJobs(queue: string): string;
  queueState(queue: string, state: string): string;
  job(jobId: string): string;
  jobKey(digest: string): string;
  run(runId: string): string;
  runsRunnable(): string;
  runsTerminalDetails(): string;
  runKey(definition: string, digest: string): string;
  runBatches(runId: string): string;
  batch(runId: string, index: number): string;
  failures(runId: string): string;
  completionCounters(): string;
  completionsIndex(): string;
  completionsDetails(): string;
  completion(eventId: string): string;
  completionsDue(): string;
  role(role: string, domain?: string): string;
  roleMember(role: string, domain: string, identity: string): string;
  advancement(domain: string): string;
}

function assertSegment(label: string, value: string): string {
  if (!keySegmentPattern.test(value)) {
    throw new QueuebitError({
      code: 'QB_REDIS_KEY_INVALID',
      message: `Invalid Redis key ${label}: "${value}".`,
      details: { label, value }
    });
  }
  return value;
}

function assertDigest(value: string): string {
  if (!digestPattern.test(value)) {
    throw new QueuebitError({
      code: 'QB_REDIS_KEY_INVALID',
      message: 'Invalid Redis key digest. Expected a 64-character lowercase SHA-256 hex value.',
      details: { value }
    });
  }
  return value;
}

function assertBatchIndex(index: number): number {
  if (!Number.isInteger(index) || index < 0) {
    throw new QueuebitError({
      code: 'QB_REDIS_KEY_INVALID',
      message: 'Invalid Batch index. Expected a non-negative integer.',
      details: { index }
    });
  }
  return index;
}

/**
 * Builds the only supported Redis key namespace. Redis Cluster is excluded for
 * v0.1, but hash-tagging keeps all Queuebit keys visibly tied to one namespace.
 */
export function createQueuebitKeyBuilder(input: QueuebitConfig | string): QueuebitKeyBuilder {
  const namespace = assertSegment(
    'namespace',
    typeof input === 'string' ? input : input.namespace
  );
  const prefix = `qb:{${namespace}}`;
  const key = (...segments: Array<string | number>) => `${prefix}:${segments.join(':')}`;

  return {
    namespace,
    prefix,
    meta: () => key('meta'),
    queueCounters: queue => key('q', assertSegment('queue', queue), 'counters'),
    queueWaiting: queue => key('q', assertSegment('queue', queue), 'waiting'),
    queueDue: queue => key('q', assertSegment('queue', queue), 'due'),
    queueActive: queue => key('q', assertSegment('queue', queue), 'active'),
    queueJobs: queue => key('q', assertSegment('queue', queue), 'jobs'),
    queueState: (queue, state) =>
      key('q', assertSegment('queue', queue), 'state', assertSegment('state', state)),
    job: jobId => key('job', assertSegment('jobId', jobId)),
    jobKey: digest => key('job-key', assertDigest(digest)),
    run: runId => key('run', assertSegment('runId', runId)),
    runsRunnable: () => key('runs', 'runnable'),
    runsTerminalDetails: () => key('runs', 'terminal-details'),
    runKey: (definition, digest) =>
      key('run-key', assertSegment('definition', definition), assertDigest(digest)),
    runBatches: runId => key('run', assertSegment('runId', runId), 'batches'),
    batch: (runId, index) => key('run', assertSegment('runId', runId), 'batch', assertBatchIndex(index)),
    failures: runId => key('run', assertSegment('runId', runId), 'failures'),
    completionCounters: () => key('completions', 'counters'),
    completionsIndex: () => key('completions', 'index'),
    completionsDetails: () => key('completions', 'details'),
    completion: eventId => key('completion', assertSegment('eventId', eventId)),
    completionsDue: () => key('completions', 'due'),
    role: (role, domain = 'default') =>
      key('roles', assertSegment('role', role), assertSegment('domain', domain)),
    roleMember: (role, domain, identity) =>
      key(
        'roles',
        assertSegment('role', role),
        assertSegment('domain', domain),
        'member',
        assertSegment('identity', identity)
      ),
    advancement: domain => key('advancement', assertSegment('domain', domain))
  };
}
