import { QueuebitScriptRegistry } from '../redis';

export const retentionPurgeCompletedJobScriptSource = `
local jobKey = KEYS[1]
local jobsIndexKey = KEYS[2]
local completedIndexKey = KEYS[3]
local countersKey = KEYS[4]

local jobId = ARGV[1]
local observedAt = ARGV[2] or ''

if redis.call('EXISTS', jobKey) == 0 then
  return {'skip', 'snapshot_missing'}
end

local state = redis.call('HGET', jobKey, 'state')
if state ~= 'completed' then
  return {'skip', 'state_protected', state or ''}
end

if redis.call('HGET', jobKey, 'detailsExpired') == '1' then
  return {'skip', 'details_expired'}
end

if redis.call('HGET', jobKey, 'runId') ~= false or redis.call('HGET', jobKey, 'batchId') ~= false then
  return {'skip', 'batchrun_owned'}
end

local requiresTombstone = false
if redis.call('HGET', jobKey, 'deduplicationKey') ~= false
  or redis.call('HGET', jobKey, 'idempotencyKey') ~= false
  or redis.call('HGET', jobKey, 'parentJobId') ~= false then
  requiresTombstone = true
end

if requiresTombstone then
  if redis.call('HGET', jobKey, 'dataDigest') == false then
    return {'skip', 'snapshot_missing'}
  end
  redis.call('HSET', jobKey, 'detailsExpired', '1', 'detailsExpiredAt', observedAt)
  redis.call(
    'HDEL',
    jobKey,
    'data',
    'result',
    'failedReason',
    'options',
    'workerId',
    'leaseDeadlineAt',
    'retryAtMs',
    'leaseGeneration',
    'stalledRecoveries'
  )
  return {'tombstoned', jobId}
end

redis.call('DEL', jobKey)
redis.call('ZREM', jobsIndexKey, jobId)
redis.call('ZREM', completedIndexKey, jobId)
redis.call('HINCRBY', countersKey, 'completedJobs', -1)

return {'deleted', jobId}
`;

export const retentionPurgeTerminalRunScriptSource = `
local runKey = KEYS[1]
local failuresKey = KEYS[2]
local terminalRunsIndexKey = KEYS[3]

local runId = ARGV[1]
local observedAt = ARGV[2] or ''

if redis.call('EXISTS', runKey) == 0 then
  redis.call('ZREM', terminalRunsIndexKey, runId)
  return {'skip', 'snapshot_missing'}
end

if redis.call('HGET', runKey, 'detailsExpired') == '1' then
  redis.call('ZREM', terminalRunsIndexKey, runId)
  return {'skip', 'details_expired'}
end

local executionState = redis.call('HGET', runKey, 'executionState')
if executionState ~= 'completed'
  and executionState ~= 'partial_failed'
  and executionState ~= 'failed'
  and executionState ~= 'cancelled' then
  redis.call('ZREM', terminalRunsIndexKey, runId)
  return {'skip', 'state_protected', executionState or ''}
end

local completionState = redis.call('HGET', runKey, 'completionState')
if completionState ~= 'not_required' and completionState ~= 'delivered' then
  return {'skip', 'completion_protected', completionState or ''}
end

if redis.call('HGET', runKey, 'inputDigest') == false then
  return {'skip', 'snapshot_missing'}
end

redis.call(
  'HSET',
  runKey,
  'detailsExpired',
  '1',
  'detailsExpiredAt',
  observedAt,
  'failureDetailsExpired',
  '1'
)
redis.call(
  'HDEL',
  runKey,
  'input',
  'boundary',
  'dispatchCursor',
  'checkpointCursor',
  'nextDispatchAt',
  'dispatchHoldReason'
)
redis.call('DEL', failuresKey)
redis.call('ZREM', terminalRunsIndexKey, runId)

return {'tombstoned', runId}
`;

export const retentionPurgeCompletionEventScriptSource = `
local completionKey = KEYS[1]
local runKey = KEYS[2]
local completionsDueKey = KEYS[3]
local completionsDetailsKey = KEYS[4]

local eventId = ARGV[1]
local observedAt = ARGV[2] or ''

if redis.call('EXISTS', completionKey) == 0 then
  redis.call('ZREM', completionsDetailsKey, eventId)
  return {'skip', 'snapshot_missing'}
end

if redis.call('HGET', completionKey, 'detailsExpired') == '1' then
  redis.call('ZREM', completionsDetailsKey, eventId)
  return {'skip', 'details_expired'}
end

local completionState = redis.call('HGET', completionKey, 'completionState')
if completionState ~= 'delivered' and completionState ~= 'not_required' then
  return {'skip', 'completion_protected', completionState or ''}
end

if redis.call('EXISTS', runKey) == 0 then
  return {'skip', 'snapshot_missing'}
end

local executionState = redis.call('HGET', runKey, 'executionState')
if executionState ~= 'completed'
  and executionState ~= 'partial_failed'
  and executionState ~= 'failed'
  and executionState ~= 'cancelled' then
  return {'skip', 'state_protected', executionState or ''}
end

local runCompletionState = redis.call('HGET', runKey, 'completionState')
if runCompletionState ~= 'not_required' and runCompletionState ~= 'delivered' then
  return {'skip', 'completion_protected', runCompletionState or ''}
end

local summary = redis.call('HGET', completionKey, 'summary')
if summary == false then
  return {'skip', 'snapshot_missing'}
end

local summaryDigest = redis.call('HGET', completionKey, 'summaryDigest')
if summaryDigest == false then
  summaryDigest = redis.sha1hex(summary)
end

redis.call(
  'HSET',
  completionKey,
  'summaryDigest',
  summaryDigest,
  'detailsExpired',
  '1',
  'detailsExpiredAt',
  observedAt
)
redis.call(
  'HDEL',
  completionKey,
  'summary',
  'backoff',
  'lastError',
  'nextDueAt',
  'deliveryOwnerId',
  'deliveryLeaseDeadlineMs',
  'deliveryLeaseDeadlineAt'
)
redis.call('ZREM', completionsDueKey, eventId)
redis.call('ZREM', completionsDetailsKey, eventId)

return {'tombstoned', eventId}
`;

export function registerObservabilityScripts(registry = new QueuebitScriptRegistry()) {
  return {
    purgeCompletedJob: registry.register({
      name: 'retention:purge-completed-job',
      version: 'v2',
      numberOfKeys: 4,
      source: retentionPurgeCompletedJobScriptSource
    }),
    purgeTerminalRun: registry.register({
      name: 'retention:purge-terminal-run',
      version: 'v2',
      numberOfKeys: 3,
      source: retentionPurgeTerminalRunScriptSource
    }),
    purgeCompletionEvent: registry.register({
      name: 'retention:purge-completion-event',
      version: 'v2',
      numberOfKeys: 4,
      source: retentionPurgeCompletionEventScriptSource
    })
  };
}
