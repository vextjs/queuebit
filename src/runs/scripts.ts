import { QueuebitScriptRegistry } from '../redis';

export const runsStartScriptSource = `
local countersKey = KEYS[1]
local runsIndexKey = KEYS[2]
local runKey = KEYS[3]
local runHashKey = KEYS[4]

local envelope = cjson.decode(ARGV[1])

local existing = redis.call('GET', runKey)
if existing then
  local identity = cjson.decode(existing)
  if identity.inputDigest ~= envelope.inputDigest then
    return {'err', 'QB_RUN_DEDUPLICATION_CONFLICT', 'runs.start idempotencyKey conflicts with existing input.', cjson.encode({idempotencyKey=envelope.idempotencyKey})}
  end
  return {'ok', identity.runId, '1'}
end

local sequence = tostring(redis.call('HINCRBY', countersKey, 'nextRunSequence', 1))
local recoveryDepth = envelope.recoveryDepth or 0
redis.call(
  'HSET',
  runHashKey,
  'id', envelope.runId,
  'definition', envelope.definition,
  'definitionVersion', tostring(envelope.definitionVersion),
  'executionState', 'created',
  'completionState', 'not_created',
  'input', envelope.inputJson,
  'inputDigest', envelope.inputDigest,
  'idempotencyKey', envelope.idempotencyKey,
  'recoveryDepth', tostring(recoveryDepth),
  'recordsSeen', '0',
  'recordsDispatched', '0',
  'recordsSkipped', '0',
  'recordsFailed', '0',
  'recordsUndispatched', '0',
  'boundaryTotalRecords', '',
  'jobsCreated', '0',
  'jobsCompleted', '0',
  'jobsFailed', '0',
  'jobsCancelled', '0',
  'boundary', '',
  'dispatchCursor', '',
  'checkpointCursor', '',
  'checkpointBatchIndex', '0',
  'sourceExhausted', '0',
  'inFlightBatches', '0',
  'sequence', sequence,
  'createdAt', envelope.createdAt,
  'updatedAt', envelope.createdAt
)
if envelope.parentRunId and envelope.parentRunId ~= '' then
  redis.call('HSET', runHashKey, 'parentRunId', envelope.parentRunId)
end
if envelope.recoveryParentRunId and envelope.recoveryParentRunId ~= '' then
  redis.call(
    'HSET',
    runHashKey,
    'recoveryParentRunId', envelope.recoveryParentRunId,
    'recoveryFailureDigest', envelope.recoveryFailureDigest,
    'recoveryFailureCount', tostring(envelope.recoveryFailureCount or 0),
    'recoveryStage', envelope.recoveryStage or 'processor'
  )
end
redis.call('SET', runKey, cjson.encode({runId=envelope.runId, inputDigest=envelope.inputDigest}))
redis.call('ZADD', runsIndexKey, sequence, envelope.runId)
return {'ok', envelope.runId, '0'}
`;

export const runsPauseScriptSource = `
local runHashKey = KEYS[1]
local updatedAt = ARGV[1]

if redis.call('EXISTS', runHashKey) == 0 then
  return {'err', 'QB_RUN_NOT_FOUND', 'Run does not exist.', '{}'}
end

local state = redis.call('HGET', runHashKey, 'executionState')
local runId = redis.call('HGET', runHashKey, 'id')
if state == 'paused' or state == 'pausing' then
  return {'ok', runId, state}
end
if state == 'completed' or state == 'partial_failed' or state == 'failed' or state == 'cancelled' or state == 'cancelling' then
  return {'err', 'QB_RUN_STATE_CONFLICT', 'Only non-terminal, non-cancelling runs can be paused.', cjson.encode({state=state})}
end

local inFlight = tonumber(redis.call('HGET', runHashKey, 'inFlightBatches') or '0')
local nextState = inFlight > 0 and 'pausing' or 'paused'
redis.call(
  'HSET',
  runHashKey,
  'executionState', nextState,
  'pauseRequestedAt', updatedAt,
  'updatedAt', updatedAt
)
if nextState == 'paused' then
  redis.call('HSET', runHashKey, 'pausedAt', updatedAt)
end
return {'ok', runId, nextState}
`;

export const runsResumeScriptSource = `
local runHashKey = KEYS[1]
local updatedAt = ARGV[1]

if redis.call('EXISTS', runHashKey) == 0 then
  return {'err', 'QB_RUN_NOT_FOUND', 'Run does not exist.', '{}'}
end

local state = redis.call('HGET', runHashKey, 'executionState')
local runId = redis.call('HGET', runHashKey, 'id')
if state == 'running' or state == 'created' then
  return {'ok', runId, state}
end
if state ~= 'paused' and state ~= 'pausing' and state ~= 'blocked' then
  return {'err', 'QB_RUN_STATE_CONFLICT', 'Only paused, pausing, or blocked runs can be resumed.', cjson.encode({state=state})}
end

redis.call(
  'HSET',
  runHashKey,
  'executionState', 'running',
  'resumedAt', updatedAt,
  'updatedAt', updatedAt
)
return {'ok', runId, 'running'}
`;

export const runsCancelScriptSource = `
local runHashKey = KEYS[1]
local completionCountersKey = KEYS[2]
local completionsIndexKey = KEYS[3]
local completionsDetailsKey = KEYS[4]
local completionsDueKey = KEYS[5]
local runCompletionKey = KEYS[6]
local terminalRunsIndexKey = KEYS[7]
local waitingKey = KEYS[8]
local dueKey = KEYS[9]
local waitingIndexKey = KEYS[10]
local delayedIndexKey = KEYS[11]
local retryingIndexKey = KEYS[12]
local cancelledIndexKey = KEYS[13]
local countersKey = KEYS[14]

local envelope = cjson.decode(ARGV[1])
local jobKeyOffset = 14

local function addTerminalRunDetailsIndex()
  if redis.call('HGET', runHashKey, 'detailsExpired') == '1' then
    return
  end
  local sequence = redis.call('HGET', runHashKey, 'sequence')
  if sequence ~= false then
    redis.call('ZADD', terminalRunsIndexKey, sequence, envelope.runId)
  end
end

local function createRunCompletion()
  if envelope.runCompletion.id == '' then
    return
  end
  if redis.call('EXISTS', runCompletionKey) == 1 then
    return
  end
  local sequence = tostring(redis.call('HINCRBY', completionCountersKey, 'nextSequence', 1))
  local state = envelope.runCompletion.handler == '' and 'not_required' or 'pending'
  local summaryJson = cjson.encode({
    recordsSeen=tonumber(redis.call('HGET', runHashKey, 'recordsSeen') or '0'),
    recordsDispatched=tonumber(redis.call('HGET', runHashKey, 'recordsDispatched') or '0'),
    recordsSkipped=tonumber(redis.call('HGET', runHashKey, 'recordsSkipped') or '0'),
    recordsFailed=tonumber(redis.call('HGET', runHashKey, 'recordsFailed') or '0'),
    recordsUndispatched=tonumber(redis.call('HGET', runHashKey, 'recordsUndispatched') or '0'),
    boundaryTotalRecords=redis.call('HGET', runHashKey, 'boundaryTotalRecords') or '',
    jobsCreated=tonumber(redis.call('HGET', runHashKey, 'jobsCreated') or '0'),
    jobsCompleted=tonumber(redis.call('HGET', runHashKey, 'jobsCompleted') or '0'),
    jobsFailed=tonumber(redis.call('HGET', runHashKey, 'jobsFailed') or '0'),
    jobsCancelled=tonumber(redis.call('HGET', runHashKey, 'jobsCancelled') or '0')
  })
  redis.call(
    'HSET',
    runCompletionKey,
    'id', envelope.runCompletion.id,
    'type', envelope.runCompletion.type,
    'runId', envelope.runCompletion.runId,
    'completionState', state,
    'handler', envelope.runCompletion.handler,
    'attempt', '0',
    'attempts', tostring(envelope.runCompletion.attempts),
    'deliveryGeneration', '0',
    'summary', summaryJson,
    'sequence', sequence,
    'createdAt', envelope.updatedAt,
    'updatedAt', envelope.updatedAt
  )
  if envelope.runCompletion.backoffJson ~= '' then
    redis.call('HSET', runCompletionKey, 'backoff', envelope.runCompletion.backoffJson)
  end
  redis.call('ZADD', completionsIndexKey, sequence, envelope.runCompletion.id)
  redis.call('ZADD', completionsDetailsKey, sequence, envelope.runCompletion.id)
  if state == 'pending' then
    redis.call('HSET', runCompletionKey, 'nextDueAt', envelope.updatedAt)
    redis.call('ZADD', completionsDueKey, tostring(envelope.nowMs), envelope.runCompletion.id)
  end
  redis.call('HSET', runHashKey, 'completionState', state, 'updatedAt', envelope.updatedAt)
end

if redis.call('EXISTS', runHashKey) == 0 then
  return {'err', 'QB_RUN_NOT_FOUND', 'Run does not exist.', '{}'}
end

local state = redis.call('HGET', runHashKey, 'executionState')
local runId = redis.call('HGET', runHashKey, 'id')
if state == 'cancelled' then
  addTerminalRunDetailsIndex()
  return {'ok', runId, state, '0'}
end
if state == 'completed' or state == 'partial_failed' or state == 'failed' then
  return {'err', 'QB_RUN_STATE_CONFLICT', 'Terminal runs cannot be cancelled.', cjson.encode({state=state})}
end

if redis.call('HGET', runHashKey, 'cancelReason') == false then
  redis.call('HSET', runHashKey, 'cancelReason', envelope.reason, 'cancelRequestedAt', envelope.updatedAt)
end

local cancelledJobs = 0
for index = 1, envelope.jobCount do
  local jobKey = KEYS[jobKeyOffset + index]
  if redis.call('EXISTS', jobKey) == 1 and redis.call('HGET', jobKey, 'runId') == envelope.runId then
    local jobState = redis.call('HGET', jobKey, 'state')
    local jobId = redis.call('HGET', jobKey, 'id')
    local sequence = redis.call('HGET', jobKey, 'sequence')
    local dataBytes = tonumber(redis.call('HGET', jobKey, 'dataBytes') or '0')
    if jobState == 'waiting' or jobState == 'delayed' or jobState == 'retrying' then
      if jobState == 'waiting' then
        redis.call('LREM', waitingKey, 0, jobKey)
        redis.call('ZREM', waitingIndexKey, jobId)
        redis.call('HINCRBY', countersKey, 'waitingJobs', -1)
      elseif jobState == 'delayed' then
        redis.call('ZREM', dueKey, jobKey)
        redis.call('ZREM', delayedIndexKey, jobId)
        redis.call('HINCRBY', countersKey, 'delayedJobs', -1)
      else
        redis.call('ZREM', dueKey, jobKey)
        redis.call('ZREM', retryingIndexKey, jobId)
        redis.call('HINCRBY', countersKey, 'retryingJobs', -1)
      end
      redis.call('HSET', jobKey, 'state', 'cancelled', 'updatedAt', envelope.updatedAt)
      redis.call('HDEL', jobKey, 'workerId', 'leaseDeadlineAt', 'retryAtMs')
      redis.call('ZADD', cancelledIndexKey, sequence, jobId)
      redis.call('HINCRBY', countersKey, 'cancelledJobs', 1)
      redis.call('HINCRBY', countersKey, 'queuedJobs', -1)
      if dataBytes > 0 then
        redis.call('HINCRBY', countersKey, 'queuedBytes', -dataBytes)
      end
      cancelledJobs = cancelledJobs + 1
    end
  end
end

local inFlight = tonumber(redis.call('HGET', runHashKey, 'inFlightBatches') or '0')
local nextState = inFlight > 0 and 'cancelling' or 'cancelled'
redis.call('HSET', runHashKey, 'executionState', nextState, 'updatedAt', envelope.updatedAt)
if nextState == 'cancelled' then
  redis.call('HSET', runHashKey, 'cancelledAt', envelope.updatedAt)
  addTerminalRunDetailsIndex()
  createRunCompletion()
end

return {'ok', runId, nextState, tostring(cancelledJobs)}
`;

export function registerRunsScripts(registry = new QueuebitScriptRegistry()) {
  return {
    start: registry.register({
      name: 'runs:start',
      version: 'v1',
      numberOfKeys: 4,
      source: runsStartScriptSource
    }),
    pause: registry.register({
      name: 'runs:pause',
      version: 'v1',
      numberOfKeys: 1,
      source: runsPauseScriptSource
    }),
    resume: registry.register({
      name: 'runs:resume',
      version: 'v1',
      numberOfKeys: 1,
      source: runsResumeScriptSource
    }),
    cancel: registry.register({
      name: 'runs:cancel',
      version: 'v3',
      numberOfKeys: 'dynamic',
      source: runsCancelScriptSource
    })
  };
}
