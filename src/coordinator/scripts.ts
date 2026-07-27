import { QueuebitScriptRegistry } from '../redis';

export const coordinatorClaimRunScriptSource = `
local runHashKey = KEYS[1]
local coordinatorId = ARGV[1]
local leaseDeadlineMs = tonumber(ARGV[2])
local leaseDeadlineAt = ARGV[3]
local nowMs = tonumber(ARGV[4])
local updatedAt = ARGV[5]

if redis.call('EXISTS', runHashKey) == 0 then
  return {'err', 'QB_RUN_NOT_FOUND', 'Run does not exist.', '{}'}
end

local state = redis.call('HGET', runHashKey, 'executionState')
if state ~= 'created' and state ~= 'running' and state ~= 'pausing' and state ~= 'cancelling' then
  return {'err', 'QB_RUN_STATE_CONFLICT', 'Only created, running, pausing, or cancelling runs can be advanced.', cjson.encode({state=state})}
end

local currentOwner = redis.call('HGET', runHashKey, 'coordinatorId')
local currentDeadline = tonumber(redis.call('HGET', runHashKey, 'coordinatorLeaseDeadlineMs') or '0')
if currentOwner and currentOwner ~= coordinatorId and currentDeadline > nowMs then
  return {'err', 'QB_RUN_STATE_CONFLICT', 'Another Coordinator owns this Run lease.', cjson.encode({owner=currentOwner})}
end

local generation = tostring(redis.call('HINCRBY', runHashKey, 'coordinatorGeneration', 1))
local nextState = state == 'created' and 'running' or state
redis.call(
  'HSET',
  runHashKey,
  'executionState', nextState,
  'coordinatorId', coordinatorId,
  'coordinatorGeneration', generation,
  'coordinatorLeaseDeadlineMs', tostring(leaseDeadlineMs),
  'coordinatorLeaseDeadlineAt', leaseDeadlineAt,
  'updatedAt', updatedAt
)

return {'ok', generation}
`;

export const coordinatorDispatchBatchScriptSource = `
local runHashKey = KEYS[1]
local runBatchesKey = KEYS[2]
local batchHashKey = KEYS[3]
local countersKey = KEYS[4]
local waitingKey = KEYS[5]
local dueKey = KEYS[6]
local jobsIndexKey = KEYS[7]
local waitingIndexKey = KEYS[8]
local delayedIndexKey = KEYS[9]
local completionCountersKey = KEYS[10]
local completionsIndexKey = KEYS[11]
local completionsDetailsKey = KEYS[12]
local completionsDueKey = KEYS[13]
local runCompletionKey = KEYS[14]
local failuresKey = KEYS[15]
local terminalRunsIndexKey = KEYS[16]

local envelope = cjson.decode(ARGV[1])
local entryCount = #envelope.entries
local jobKeyOffset = 16
local dedupeKeyOffset = 16 + entryCount

local function createCompletion(completionKey, event)
  if redis.call('EXISTS', completionKey) == 1 then
    return
  end
  local sequence = tostring(redis.call('HINCRBY', completionCountersKey, 'nextSequence', 1))
  local state = event.handler == '' and 'not_required' or 'pending'
  redis.call(
    'HSET',
    completionKey,
    'id', event.id,
    'type', event.type,
    'runId', event.runId,
    'completionState', state,
    'handler', event.handler,
    'attempt', '0',
    'attempts', tostring(event.attempts),
    'deliveryGeneration', '0',
    'summary', event.summaryJson,
    'sequence', sequence,
    'createdAt', event.createdAt,
    'updatedAt', event.updatedAt
  )
  if event.backoffJson ~= '' then
    redis.call('HSET', completionKey, 'backoff', event.backoffJson)
  end
  redis.call('ZADD', completionsIndexKey, sequence, event.id)
  redis.call('ZADD', completionsDetailsKey, sequence, event.id)
  if state == 'pending' then
    redis.call('HSET', completionKey, 'nextDueAt', event.updatedAt)
    redis.call('ZADD', completionsDueKey, tostring(event.nowMs), event.id)
  end
end

local function runSummaryJson()
  return cjson.encode({
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
end

local function batchCheckpointReady(batchKey)
  local state = redis.call('HGET', batchKey, 'executionState')
  if state ~= 'completed' and state ~= 'partial_failed' and state ~= 'cancelled' then
    return false
  end
  local completionState = redis.call('HGET', batchKey, 'completionState') or 'not_required'
  return completionState == 'not_required' or completionState == 'delivered'
end

local function addTerminalRunDetailsIndex()
  if redis.call('HGET', runHashKey, 'detailsExpired') == '1' then
    return
  end
  local sequence = redis.call('HGET', runHashKey, 'sequence')
  if sequence ~= false then
    redis.call('ZADD', terminalRunsIndexKey, sequence, envelope.runId)
  end
end

local function advanceCheckpoint()
  local checkpointIndex = tonumber(redis.call('HGET', runHashKey, 'checkpointBatchIndex') or '0')
  local checkpointCursor = redis.call('HGET', runHashKey, 'checkpointCursor') or ''
  local advanced = false
  while true do
    local batchIds = redis.call('ZRANGEBYSCORE', runBatchesKey, tostring(checkpointIndex), tostring(checkpointIndex), 'LIMIT', '0', '1')
    if #batchIds == 0 then
      break
    end
    local checkpointBatchKey = runHashKey .. ':batch:' .. tostring(checkpointIndex)
    if not batchCheckpointReady(checkpointBatchKey) then
      break
    end
    checkpointCursor = redis.call('HGET', checkpointBatchKey, 'nextCursor') or checkpointCursor
    checkpointIndex = checkpointIndex + 1
    advanced = true
  end
  if advanced then
    redis.call(
      'HSET',
      runHashKey,
      'checkpointCursor', checkpointCursor,
      'checkpointBatchIndex', tostring(checkpointIndex),
      'updatedAt', envelope.updatedAt
    )
  end
  return checkpointIndex
end

local function maybeCreateRunCompletion()
  if tonumber(redis.call('HGET', runHashKey, 'inFlightBatches') or '0') > 0 then
    return
  end
  if redis.call('HGET', runHashKey, 'sourceExhausted') ~= '1' then
    return
  end
  local checkpointIndex = tonumber(redis.call('HGET', runHashKey, 'checkpointBatchIndex') or '0')
  local nextBatchIndex = tonumber(redis.call('HGET', runHashKey, 'nextBatchIndex') or '0')
  if checkpointIndex < nextBatchIndex then
    return
  end
  envelope.runCompletion.summaryJson = runSummaryJson()
  createCompletion(runCompletionKey, envelope.runCompletion)
  local runCompletionState = envelope.runCompletion.handler == '' and 'not_required' or 'pending'
  local terminalState = 'completed'
  if tonumber(redis.call('HGET', runHashKey, 'recordsFailed') or '0') > 0
    or tonumber(redis.call('HGET', runHashKey, 'jobsFailed') or '0') > 0
    or tonumber(redis.call('HGET', runHashKey, 'jobsCancelled') or '0') > 0 then
    terminalState = 'partial_failed'
  end
  redis.call('HSET', runHashKey, 'executionState', terminalState, 'completionState', runCompletionState, 'updatedAt', envelope.updatedAt)
  addTerminalRunDetailsIndex()
end

local function recordMapperFailure(failure)
  local sequence = tostring(redis.call('HINCRBY', runHashKey, 'nextFailureSequence', 1))
  local ok, errorValue = pcall(cjson.decode, failure.errorJson)
  if not ok then
    errorValue = {name='Error', message=failure.errorJson}
  end
  local payloadOk, payloadValue = pcall(cjson.decode, failure.payloadJson)
  if not payloadOk then
    payloadValue = failure.payloadJson
  end
  local record = {
    sequence=sequence,
    runId=envelope.runId,
    batchId=envelope.batchId,
    stage='mapper',
    recordIdentity=failure.recordIdentity,
    attempt=0,
    error=errorValue,
    recoveryAvailable=true,
    payload=payloadValue
  }
  redis.call('ZADD', failuresKey, sequence, cjson.encode(record))
end

if redis.call('EXISTS', batchHashKey) == 1 then
  return {'ok', envelope.batchId, redis.call('HGET', batchHashKey, 'jobIds') or '[]', '1'}
end

if redis.call('EXISTS', runHashKey) == 0 then
  return {'err', 'QB_RUN_NOT_FOUND', 'Run does not exist.', '{}'}
end

local state = redis.call('HGET', runHashKey, 'executionState')
if state ~= 'running' then
  return {'err', 'QB_DISPATCH_STATE_CONFLICT', 'Run is not running.', cjson.encode({state=state})}
end

if redis.call('HGET', runHashKey, 'coordinatorId') ~= envelope.coordinatorId then
  return {'err', 'QB_DISPATCH_STATE_CONFLICT', 'Coordinator does not own this Run lease.', '{}'}
end

if redis.call('HGET', runHashKey, 'coordinatorGeneration') ~= tostring(envelope.coordinatorGeneration) then
  return {'err', 'QB_DISPATCH_STATE_CONFLICT', 'Coordinator generation is stale.', '{}'}
end

local currentDispatchCursor = redis.call('HGET', runHashKey, 'dispatchCursor') or ''
if currentDispatchCursor ~= envelope.expectedDispatchCursorJson then
  return {
    'err',
    'QB_DISPATCH_STATE_CONFLICT',
    'Run dispatchCursor changed before Batch dispatch.',
    cjson.encode({expected=envelope.expectedDispatchCursorJson, actual=currentDispatchCursor})
  }
end

if entryCount > tonumber(envelope.maxBulkJobs) then
  return {'err', 'QB_DISPATCH_LIMIT_EXCEEDED', 'Batch dispatch exceeds maxBulkJobs.', cjson.encode({actual=entryCount, limit=envelope.maxBulkJobs})}
end
if tonumber(envelope.bulkBytes) > tonumber(envelope.maxBulkBytes) then
  return {'err', 'QB_DISPATCH_LIMIT_EXCEEDED', 'Batch dispatch exceeds maxBulkBytes.', cjson.encode({actual=envelope.bulkBytes, limit=envelope.maxBulkBytes})}
end

local resultIds = {}
local newJobs = 0
local newBytes = 0
for index = 1, entryCount do
  local entry = envelope.entries[index]
  local dedupeKey = KEYS[dedupeKeyOffset + index]
  local existing = redis.call('GET', dedupeKey)
  if existing then
    local existingIdentity = cjson.decode(existing)
    if existingIdentity.dataDigest ~= entry.dataDigest then
      return {'err', 'QB_JOB_DEDUPLICATION_CONFLICT', 'Batch job identity conflicts with existing data.', cjson.encode({deduplicationKey=entry.deduplicationKey})}
    end
    resultIds[index] = existingIdentity.jobId
  else
    newJobs = newJobs + 1
    newBytes = newBytes + tonumber(entry.dataBytes)
  end
end

local highJobs = envelope.highJobs == '' and nil or tonumber(envelope.highJobs)
local highBytes = envelope.highBytes == '' and nil or tonumber(envelope.highBytes)
local lowJobs = envelope.lowJobs == '' and nil or tonumber(envelope.lowJobs)
local lowBytes = envelope.lowBytes == '' and nil or tonumber(envelope.lowBytes)
local currentJobs = tonumber(redis.call('HGET', countersKey, 'queuedJobs') or '0')
local currentBytes = tonumber(redis.call('HGET', countersKey, 'queuedBytes') or '0')

local function pressureDetails(reason)
  return cjson.encode({
    reason=reason,
    currentJobs=currentJobs,
    currentBytes=currentBytes,
    incomingJobs=newJobs,
    incomingBytes=newBytes,
    highJobs=highJobs or '',
    lowJobs=lowJobs or '',
    highBytes=highBytes or '',
    lowBytes=lowBytes or ''
  })
end

local function setBackpressureLatch(reason)
  redis.call(
    'HSET',
    countersKey,
    'backpressureLatched', '1',
    'backpressureReason', reason,
    'backpressureLastCheckedAt', envelope.updatedAt
  )
  if redis.call('HGET', countersKey, 'backpressureSince') == false then
    redis.call('HSET', countersKey, 'backpressureSince', envelope.updatedAt)
  end
end

if newJobs > 0 then
  if highJobs and newJobs > highJobs then
    return {'err', 'QB_BACKPRESSURE_REQUEST_TOO_LARGE', 'Batch dispatch request exceeds queue high job watermark.', pressureDetails('jobs')}
  end
  if highBytes and newBytes > highBytes then
    return {'err', 'QB_BACKPRESSURE_REQUEST_TOO_LARGE', 'Batch dispatch request exceeds queue high byte watermark.', pressureDetails('bytes')}
  end
  if redis.call('HGET', countersKey, 'backpressureLatched') == '1' then
    return {'err', 'QB_BACKPRESSURE_REJECTED', 'Queue backpressure is active.', pressureDetails(redis.call('HGET', countersKey, 'backpressureReason') or 'unknown')}
  end
  if highJobs and currentJobs + newJobs > highJobs then
    setBackpressureLatch('jobs')
    return {'err', 'QB_BACKPRESSURE_REJECTED', 'Queue job high watermark would be exceeded.', pressureDetails('jobs')}
  end
  if highBytes and currentBytes + newBytes > highBytes then
    setBackpressureLatch('bytes')
    return {'err', 'QB_BACKPRESSURE_REJECTED', 'Queue byte high watermark would be exceeded.', pressureDetails('bytes')}
  end
end

local waitingAdded = 0
local delayedAdded = 0
for index = 1, entryCount do
  if not resultIds[index] then
    local entry = envelope.entries[index]
    local jobKey = KEYS[jobKeyOffset + index]
    local dedupeKey = KEYS[dedupeKeyOffset + index]
    local sequence = tostring(redis.call('HINCRBY', countersKey, 'nextSequence', 1))
    redis.call(
      'HSET',
      jobKey,
      'id', entry.jobId,
      'queue', entry.queue,
      'name', entry.name,
      'state', entry.state,
      'attempt', '0',
      'attempts', tostring(entry.attempts),
      'createdAt', entry.createdAt,
      'updatedAt', entry.updatedAt,
      'data', entry.dataJson,
      'dataDigest', entry.dataDigest,
      'dataBytes', tostring(entry.dataBytes),
      'options', entry.optionsJson,
      'sequence', sequence,
      'deduplicationKey', entry.deduplicationKey,
      'idempotencyKey', entry.idempotencyKey,
      'recordIdentity', entry.recordIdentity,
      'runId', entry.runId,
      'batchId', entry.batchId
    )
    redis.call('ZADD', jobsIndexKey, sequence, entry.jobId)
    if entry.state == 'delayed' then
      redis.call('ZADD', dueKey, tostring(entry.delayUntilMs), jobKey)
      redis.call('ZADD', delayedIndexKey, sequence, entry.jobId)
      delayedAdded = delayedAdded + 1
    else
      redis.call('RPUSH', waitingKey, jobKey)
      redis.call('ZADD', waitingIndexKey, sequence, entry.jobId)
      waitingAdded = waitingAdded + 1
    end
    redis.call('SET', dedupeKey, cjson.encode({jobId=entry.jobId, dataDigest=entry.dataDigest}))
    resultIds[index] = entry.jobId
  end
end

if newJobs > 0 then
  redis.call('HINCRBY', countersKey, 'queuedJobs', newJobs)
  redis.call('HINCRBY', countersKey, 'queuedBytes', newBytes)
  redis.call('HINCRBY', countersKey, 'totalJobs', newJobs)
  if waitingAdded > 0 then
    redis.call('HINCRBY', countersKey, 'waitingJobs', waitingAdded)
  end
  if delayedAdded > 0 then
    redis.call('HINCRBY', countersKey, 'delayedJobs', delayedAdded)
  end
  if highJobs and currentJobs + newJobs >= highJobs then
    setBackpressureLatch('jobs')
  elseif highBytes and currentBytes + newBytes >= highBytes then
    setBackpressureLatch('bytes')
  end
end

local batchExecutionState = newJobs > 0 and 'running' or 'completed'
redis.call(
  'HSET',
  batchHashKey,
  'id', envelope.batchId,
  'runId', envelope.runId,
  'index', tostring(envelope.batchIndex),
  'inputCursor', envelope.expectedDispatchCursorJson,
  'nextCursor', envelope.nextCursorJson,
  'executionState', batchExecutionState,
  'completionState', 'not_required',
  'recordsSeen', tostring(envelope.recordsSeen),
  'recordsDispatched', tostring(envelope.recordsDispatched),
  'recordsSkipped', tostring(envelope.recordsSkipped),
  'recordsFailed', tostring(envelope.recordsFailed),
  'recordsUndispatched', tostring(envelope.recordsUndispatched),
  'jobsCreated', tostring(newJobs),
  'jobsCompleted', '0',
  'jobsFailed', '0',
  'jobsCancelled', '0',
  'sourceExhausted', envelope.sourceExhausted and '1' or '0',
  'jobIds', cjson.encode(resultIds),
  'createdAt', envelope.createdAt,
  'updatedAt', envelope.updatedAt
)
redis.call('ZADD', runBatchesKey, tostring(envelope.batchIndex), envelope.batchId)

local mapperFailures = envelope.mapperFailures or {}
for index = 1, #mapperFailures do
  recordMapperFailure(mapperFailures[index])
end

if redis.call('HGET', runHashKey, 'boundary') == '' then
  redis.call('HSET', runHashKey, 'boundary', envelope.boundaryJson)
end
if envelope.boundaryTotalRecords ~= '' then
  redis.call('HSET', runHashKey, 'boundaryTotalRecords', tostring(envelope.boundaryTotalRecords))
end

redis.call('HINCRBY', runHashKey, 'recordsSeen', envelope.recordsSeen)
redis.call('HINCRBY', runHashKey, 'recordsDispatched', envelope.recordsDispatched)
redis.call('HINCRBY', runHashKey, 'recordsSkipped', envelope.recordsSkipped)
redis.call('HINCRBY', runHashKey, 'recordsFailed', envelope.recordsFailed)
redis.call('HINCRBY', runHashKey, 'recordsUndispatched', envelope.recordsUndispatched)
redis.call('HINCRBY', runHashKey, 'jobsCreated', newJobs)
if newJobs > 0 then
  redis.call('HINCRBY', runHashKey, 'inFlightBatches', 1)
end
redis.call(
  'HSET',
  runHashKey,
  'dispatchCursor', envelope.nextCursorJson,
  'sourceExhausted', envelope.sourceExhausted and '1' or '0',
  'nextBatchIndex', tostring(envelope.batchIndex + 1),
  'updatedAt', envelope.updatedAt
)
redis.call('HDEL', runHashKey, 'dispatchHoldReason')
if envelope.nextDispatchAt ~= '' then
  redis.call('HSET', runHashKey, 'nextDispatchAt', envelope.nextDispatchAt)
else
  redis.call('HDEL', runHashKey, 'nextDispatchAt')
end

if newJobs == 0 then
  advanceCheckpoint()
  maybeCreateRunCompletion()
end

return {'ok', envelope.batchId, cjson.encode(resultIds), '0'}
`;

export const coordinatorSettleBatchScriptSource = `
local runHashKey = KEYS[1]
local runBatchesKey = KEYS[2]
local batchHashKey = KEYS[3]
local completionCountersKey = KEYS[4]
local completionsIndexKey = KEYS[5]
local completionsDetailsKey = KEYS[6]
local completionsDueKey = KEYS[7]
local batchCompletionKey = KEYS[8]
local runCompletionKey = KEYS[9]
local failuresKey = KEYS[10]
local terminalRunsIndexKey = KEYS[11]

local envelope = cjson.decode(ARGV[1])
local jobKeyOffset = 11

local function createCompletion(completionKey, event)
  if redis.call('EXISTS', completionKey) == 1 then
    return
  end
  local sequence = tostring(redis.call('HINCRBY', completionCountersKey, 'nextSequence', 1))
  local state = event.handler == '' and 'not_required' or 'pending'
  redis.call(
    'HSET',
    completionKey,
    'id', event.id,
    'type', event.type,
    'runId', event.runId,
    'completionState', state,
    'handler', event.handler,
    'attempt', '0',
    'attempts', tostring(event.attempts),
    'deliveryGeneration', '0',
    'summary', event.summaryJson,
    'sequence', sequence,
    'createdAt', event.createdAt,
    'updatedAt', event.updatedAt
  )
  if event.batchId ~= '' then
    redis.call('HSET', completionKey, 'batchId', event.batchId)
  end
  if event.backoffJson ~= '' then
    redis.call('HSET', completionKey, 'backoff', event.backoffJson)
  end
  redis.call('ZADD', completionsIndexKey, sequence, event.id)
  redis.call('ZADD', completionsDetailsKey, sequence, event.id)
  if state == 'pending' then
    redis.call('HSET', completionKey, 'nextDueAt', event.updatedAt)
    redis.call('ZADD', completionsDueKey, tostring(event.nowMs), event.id)
  end
end

local function runSummaryJson()
  return cjson.encode({
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
end

local function batchCheckpointReady(batchKey)
  local state = redis.call('HGET', batchKey, 'executionState')
  if state ~= 'completed' and state ~= 'partial_failed' and state ~= 'cancelled' then
    return false
  end
  local completionState = redis.call('HGET', batchKey, 'completionState') or 'not_required'
  return completionState == 'not_required' or completionState == 'delivered'
end

local function addTerminalRunDetailsIndex()
  if redis.call('HGET', runHashKey, 'detailsExpired') == '1' then
    return
  end
  local sequence = redis.call('HGET', runHashKey, 'sequence')
  if sequence ~= false then
    redis.call('ZADD', terminalRunsIndexKey, sequence, envelope.runId)
  end
end

local function advanceCheckpoint()
  local checkpointIndex = tonumber(redis.call('HGET', runHashKey, 'checkpointBatchIndex') or '0')
  local checkpointCursor = redis.call('HGET', runHashKey, 'checkpointCursor') or ''
  local advanced = false
  while true do
    local batchIds = redis.call('ZRANGEBYSCORE', runBatchesKey, tostring(checkpointIndex), tostring(checkpointIndex), 'LIMIT', '0', '1')
    if #batchIds == 0 then
      break
    end
    local checkpointBatchKey = runHashKey .. ':batch:' .. tostring(checkpointIndex)
    if not batchCheckpointReady(checkpointBatchKey) then
      break
    end
    checkpointCursor = redis.call('HGET', checkpointBatchKey, 'nextCursor') or checkpointCursor
    checkpointIndex = checkpointIndex + 1
    advanced = true
  end
  if advanced then
    redis.call(
      'HSET',
      runHashKey,
      'checkpointCursor', checkpointCursor,
      'checkpointBatchIndex', tostring(checkpointIndex),
      'updatedAt', envelope.updatedAt
    )
  end
  return checkpointIndex
end

local function recordProcessorFailure(jobKey)
  local sequence = tostring(redis.call('HINCRBY', runHashKey, 'nextFailureSequence', 1))
  local failedReasonJson = redis.call('HGET', jobKey, 'failedReason') or '{"name":"Error","message":"Job failed without a serialized reason."}'
  local ok, errorValue = pcall(cjson.decode, failedReasonJson)
  if not ok then
    errorValue = {name='Error', message=failedReasonJson}
  end
  local dataJson = redis.call('HGET', jobKey, 'data') or 'null'
  local dataOk, dataValue = pcall(cjson.decode, dataJson)
  if not dataOk then
    dataValue = dataJson
  end
  local optionsJson = redis.call('HGET', jobKey, 'options') or '{}'
  local optionsOk, optionsValue = pcall(cjson.decode, optionsJson)
  if not optionsOk then
    optionsValue = {}
  end
  local jobId = redis.call('HGET', jobKey, 'id') or ''
  local identity = redis.call('HGET', jobKey, 'recordIdentity')
    or redis.call('HGET', jobKey, 'deduplicationKey')
    or jobId
  local payload = {
    name=redis.call('HGET', jobKey, 'name') or '',
    data=dataValue,
    options=optionsValue,
    deduplicationKey=redis.call('HGET', jobKey, 'deduplicationKey') or '',
    idempotencyKey=redis.call('HGET', jobKey, 'idempotencyKey') or ''
  }
  local record = {
    sequence=sequence,
    runId=envelope.runId,
    batchId=envelope.batchId,
    jobId=jobId,
    stage='processor',
    recordIdentity=identity,
    attempt=tonumber(redis.call('HGET', jobKey, 'attempt') or '0'),
    error=errorValue,
    recoveryAvailable=true,
    payload=payload
  }
  redis.call('ZADD', failuresKey, sequence, cjson.encode(record))
end

local function maybeCreateRunCompletion()
  if tonumber(redis.call('HGET', runHashKey, 'inFlightBatches') or '0') > 0 then
    return
  end
  local runState = redis.call('HGET', runHashKey, 'executionState')
  if runState == 'pausing' then
    redis.call('HSET', runHashKey, 'executionState', 'paused', 'pausedAt', envelope.updatedAt, 'updatedAt', envelope.updatedAt)
    return
  end
  if runState == 'cancelling' then
    redis.call('HSET', runHashKey, 'executionState', 'cancelled', 'cancelledAt', envelope.updatedAt)
    addTerminalRunDetailsIndex()
    envelope.runCompletion.summaryJson = runSummaryJson()
    createCompletion(runCompletionKey, envelope.runCompletion)
    local cancelCompletionState = envelope.runCompletion.handler == '' and 'not_required' or 'pending'
    redis.call('HSET', runHashKey, 'completionState', cancelCompletionState, 'updatedAt', envelope.updatedAt)
    return
  end
  if redis.call('HGET', runHashKey, 'sourceExhausted') ~= '1' then
    return
  end
  local checkpointIndex = tonumber(redis.call('HGET', runHashKey, 'checkpointBatchIndex') or '0')
  local nextBatchIndex = tonumber(redis.call('HGET', runHashKey, 'nextBatchIndex') or '0')
  if checkpointIndex < nextBatchIndex then
    return
  end
  local terminalState = 'completed'
  if tonumber(redis.call('HGET', runHashKey, 'recordsFailed') or '0') > 0
    or tonumber(redis.call('HGET', runHashKey, 'jobsFailed') or '0') > 0
    or tonumber(redis.call('HGET', runHashKey, 'jobsCancelled') or '0') > 0 then
    terminalState = 'partial_failed'
  end
  redis.call('HSET', runHashKey, 'executionState', terminalState)
  addTerminalRunDetailsIndex()
  envelope.runCompletion.summaryJson = runSummaryJson()
  createCompletion(runCompletionKey, envelope.runCompletion)
  local runCompletionState = envelope.runCompletion.handler == '' and 'not_required' or 'pending'
  redis.call('HSET', runHashKey, 'completionState', runCompletionState, 'updatedAt', envelope.updatedAt)
end

if redis.call('EXISTS', runHashKey) == 0 then
  return {'err', 'QB_RUN_NOT_FOUND', 'Run does not exist.', '{}'}
end
if redis.call('EXISTS', batchHashKey) == 0 then
  return {'err', 'QB_RUN_NOT_FOUND', 'Batch does not exist.', cjson.encode({batchId=envelope.batchId})}
end

if redis.call('HGET', runHashKey, 'coordinatorId') ~= envelope.coordinatorId then
  return {'err', 'QB_DISPATCH_STATE_CONFLICT', 'Coordinator does not own this Run lease.', '{}'}
end
if redis.call('HGET', runHashKey, 'coordinatorGeneration') ~= tostring(envelope.coordinatorGeneration) then
  return {'err', 'QB_DISPATCH_STATE_CONFLICT', 'Coordinator generation is stale.', '{}'}
end

local batchState = redis.call('HGET', batchHashKey, 'executionState')
if batchState ~= 'running' then
  return {'ok', 'settled', batchState or '', redis.call('HGET', batchHashKey, 'nextCursor') or ''}
end

local completed = 0
local failed = 0
local cancelled = 0
for index = 1, envelope.jobCount do
  local jobKey = KEYS[jobKeyOffset + index]
  local state = redis.call('HGET', jobKey, 'state')
  if state == 'completed' then
    completed = completed + 1
  elseif state == 'failed' then
    failed = failed + 1
  elseif state == 'cancelled' then
    cancelled = cancelled + 1
  else
    return {'ok', 'waiting', state or 'missing', redis.call('HGET', batchHashKey, 'nextCursor') or ''}
  end
end

if failed > 0 then
  for index = 1, envelope.jobCount do
    local jobKey = KEYS[jobKeyOffset + index]
    if redis.call('HGET', jobKey, 'state') == 'failed' then
      recordProcessorFailure(jobKey)
    end
  end
end

local executionState = (failed > 0 or cancelled > 0) and 'partial_failed' or 'completed'
local nextCursor = redis.call('HGET', batchHashKey, 'nextCursor') or envelope.nextCursorJson
local batchSummaryJson = cjson.encode({
  recordsSeen=tonumber(redis.call('HGET', batchHashKey, 'recordsSeen') or '0'),
  recordsDispatched=tonumber(redis.call('HGET', batchHashKey, 'recordsDispatched') or '0'),
  recordsSkipped=tonumber(redis.call('HGET', batchHashKey, 'recordsSkipped') or '0'),
  recordsFailed=tonumber(redis.call('HGET', batchHashKey, 'recordsFailed') or '0'),
  recordsUndispatched=tonumber(redis.call('HGET', batchHashKey, 'recordsUndispatched') or '0'),
  boundaryTotalRecords=redis.call('HGET', runHashKey, 'boundaryTotalRecords') or '',
  jobsCreated=tonumber(redis.call('HGET', batchHashKey, 'jobsCreated') or '0'),
  jobsCompleted=completed,
  jobsFailed=failed,
  jobsCancelled=cancelled
})
envelope.batchCompletion.summaryJson = batchSummaryJson
createCompletion(batchCompletionKey, envelope.batchCompletion)
local batchCompletionState = envelope.batchCompletion.handler == '' and 'not_required' or 'pending'
redis.call(
  'HSET',
  batchHashKey,
  'executionState', executionState,
  'completionState', batchCompletionState,
  'completionEventId', envelope.batchCompletion.id,
  'jobsCompleted', tostring(completed),
  'jobsFailed', tostring(failed),
  'jobsCancelled', tostring(cancelled),
  'updatedAt', envelope.updatedAt
)
redis.call('HINCRBY', runHashKey, 'jobsCompleted', completed)
redis.call('HINCRBY', runHashKey, 'jobsFailed', failed)
redis.call('HINCRBY', runHashKey, 'jobsCancelled', cancelled)

if batchCompletionState == 'not_required' then
  redis.call('HINCRBY', runHashKey, 'inFlightBatches', -1)
  advanceCheckpoint()
  maybeCreateRunCompletion()
else
  redis.call('HSET', runHashKey, 'updatedAt', envelope.updatedAt)
end

return {'ok', 'settled', executionState, nextCursor}
`;

export const coordinatorClaimCompletionScriptSource = `
local completionKey = KEYS[1]
local dueKey = KEYS[2]

local coordinatorId = ARGV[1]
local leaseDeadlineMs = tonumber(ARGV[2])
local leaseDeadlineAt = ARGV[3]
local nowMs = tonumber(ARGV[4])
local updatedAt = ARGV[5]

if redis.call('EXISTS', completionKey) == 0 then
  return {'err', 'QB_COMPLETION_NOT_FOUND', 'Completion event does not exist.', '{}'}
end

local state = redis.call('HGET', completionKey, 'completionState')
if state ~= 'pending' and state ~= 'retrying' and state ~= 'delivering' then
  return {'ok', ''}
end

local currentOwner = redis.call('HGET', completionKey, 'deliveryOwnerId')
local currentDeadline = tonumber(redis.call('HGET', completionKey, 'deliveryLeaseDeadlineMs') or '0')
if currentOwner and currentOwner ~= coordinatorId and currentDeadline > nowMs then
  return {'ok', ''}
end

local generation = tostring(redis.call('HINCRBY', completionKey, 'deliveryGeneration', 1))
local attempt = tostring(redis.call('HINCRBY', completionKey, 'attempt', 1))
redis.call(
  'HSET',
  completionKey,
  'completionState', 'delivering',
  'deliveryOwnerId', coordinatorId,
  'deliveryGeneration', generation,
  'deliveryLeaseDeadlineMs', tostring(leaseDeadlineMs),
  'deliveryLeaseDeadlineAt', leaseDeadlineAt,
  'updatedAt', updatedAt
)
redis.call('HDEL', completionKey, 'nextDueAt')
redis.call('ZADD', dueKey, tostring(leaseDeadlineMs), redis.call('HGET', completionKey, 'id'))
return {'ok', redis.call('HGET', completionKey, 'id'), generation, attempt}
`;

export const coordinatorSettleCompletionScriptSource = `
local completionKey = KEYS[1]
local parentKey = KEYS[2]
local runHashKey = KEYS[3]
local runBatchesKey = KEYS[4]
local completionCountersKey = KEYS[5]
local completionsIndexKey = KEYS[6]
local completionsDetailsKey = KEYS[7]
local completionsDueKey = KEYS[8]
local runCompletionKey = KEYS[9]
local terminalRunsIndexKey = KEYS[10]

local envelope = cjson.decode(ARGV[1])

local function batchCheckpointReady(batchKey)
  local state = redis.call('HGET', batchKey, 'executionState')
  if state ~= 'completed' and state ~= 'partial_failed' and state ~= 'cancelled' then
    return false
  end
  local completionState = redis.call('HGET', batchKey, 'completionState') or 'not_required'
  return completionState == 'not_required' or completionState == 'delivered'
end

local function maybeAddTerminalRunDetailsIndex()
  if redis.call('HGET', runHashKey, 'detailsExpired') == '1' then
    return
  end
  local state = redis.call('HGET', runHashKey, 'executionState')
  if state ~= 'completed' and state ~= 'partial_failed' and state ~= 'failed' and state ~= 'cancelled' then
    return
  end
  local sequence = redis.call('HGET', runHashKey, 'sequence')
  local runId = redis.call('HGET', runHashKey, 'id')
  if sequence ~= false and runId ~= false then
    redis.call('ZADD', terminalRunsIndexKey, sequence, runId)
  end
end

local function advanceCheckpoint()
  local checkpointIndex = tonumber(redis.call('HGET', runHashKey, 'checkpointBatchIndex') or '0')
  local checkpointCursor = redis.call('HGET', runHashKey, 'checkpointCursor') or ''
  local advanced = false
  while true do
    local batchIds = redis.call('ZRANGEBYSCORE', runBatchesKey, tostring(checkpointIndex), tostring(checkpointIndex), 'LIMIT', '0', '1')
    if #batchIds == 0 then
      break
    end
    local checkpointBatchKey = runHashKey .. ':batch:' .. tostring(checkpointIndex)
    if not batchCheckpointReady(checkpointBatchKey) then
      break
    end
    checkpointCursor = redis.call('HGET', checkpointBatchKey, 'nextCursor') or checkpointCursor
    checkpointIndex = checkpointIndex + 1
    advanced = true
  end
  if advanced then
    redis.call(
      'HSET',
      runHashKey,
      'checkpointCursor', checkpointCursor,
      'checkpointBatchIndex', tostring(checkpointIndex),
      'updatedAt', envelope.updatedAt
    )
  end
  return checkpointIndex
end

local function canCreateTerminalRunCompletion()
  if redis.call('HGET', runHashKey, 'sourceExhausted') ~= '1' then
    return false
  end
  if tonumber(redis.call('HGET', runHashKey, 'inFlightBatches') or '0') > 0 then
    return false
  end
  local checkpointIndex = tonumber(redis.call('HGET', runHashKey, 'checkpointBatchIndex') or '0')
  local nextBatchIndex = tonumber(redis.call('HGET', runHashKey, 'nextBatchIndex') or '0')
  return checkpointIndex >= nextBatchIndex
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
    'summary', envelope.runCompletion.summaryJson,
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

if redis.call('EXISTS', completionKey) == 0 then
  return {'err', 'QB_COMPLETION_NOT_FOUND', 'Completion event does not exist.', '{}'}
end
if redis.call('EXISTS', parentKey) == 0 then
  return {'err', 'QB_COMPLETION_STATE_CONFLICT', 'Completion parent does not exist.', cjson.encode({parentKind=envelope.parentKind})}
end
if redis.call('EXISTS', runHashKey) == 0 then
  return {'err', 'QB_RUN_NOT_FOUND', 'Run does not exist.', '{}'}
end

if redis.call('HGET', completionKey, 'deliveryOwnerId') ~= envelope.coordinatorId then
  return {'err', 'QB_COMPLETION_STATE_CONFLICT', 'Coordinator does not own this completion lease.', '{}'}
end
if redis.call('HGET', completionKey, 'deliveryGeneration') ~= tostring(envelope.deliveryGeneration) then
  return {'err', 'QB_COMPLETION_STATE_CONFLICT', 'Completion delivery generation is stale.', '{}'}
end
if redis.call('HGET', completionKey, 'completionState') ~= 'delivering' then
  return {'err', 'QB_COMPLETION_STATE_CONFLICT', 'Completion is not delivering.', '{}'}
end

if envelope.result == 'delivered' then
  redis.call(
    'HSET',
    completionKey,
    'completionState', 'delivered',
    'updatedAt', envelope.updatedAt
  )
  redis.call('HDEL', completionKey, 'deliveryOwnerId', 'deliveryLeaseDeadlineMs', 'deliveryLeaseDeadlineAt', 'lastError', 'nextDueAt')
  redis.call('ZREM', completionsDueKey, redis.call('HGET', completionKey, 'id'))
  redis.call('HSET', parentKey, 'completionState', 'delivered', 'updatedAt', envelope.updatedAt)
  maybeAddTerminalRunDetailsIndex()
  if envelope.parentKind == 'batch' then
    redis.call('HINCRBY', runHashKey, 'inFlightBatches', -1)
    advanceCheckpoint()
    local remainingBatches = tonumber(redis.call('HGET', runHashKey, 'inFlightBatches') or '0')
    local runState = redis.call('HGET', runHashKey, 'executionState')
    if runState == 'pausing' and remainingBatches == 0 then
      redis.call('HSET', runHashKey, 'executionState', 'paused', 'pausedAt', envelope.updatedAt, 'updatedAt', envelope.updatedAt)
    elseif runState == 'cancelling' and remainingBatches == 0 then
      redis.call('HSET', runHashKey, 'executionState', 'cancelled', 'cancelledAt', envelope.updatedAt)
      maybeAddTerminalRunDetailsIndex()
      createRunCompletion()
    elseif canCreateTerminalRunCompletion() then
      local terminalState = 'completed'
      if tonumber(redis.call('HGET', runHashKey, 'recordsFailed') or '0') > 0
        or tonumber(redis.call('HGET', runHashKey, 'jobsFailed') or '0') > 0
        or tonumber(redis.call('HGET', runHashKey, 'jobsCancelled') or '0') > 0 then
        terminalState = 'partial_failed'
      end
      redis.call('HSET', runHashKey, 'executionState', terminalState)
      maybeAddTerminalRunDetailsIndex()
      createRunCompletion()
    end
  end
  return {'ok', 'delivered'}
end

local attempt = tonumber(redis.call('HGET', completionKey, 'attempt') or '0')
local attempts = tonumber(redis.call('HGET', completionKey, 'attempts') or '1')
if attempt < attempts then
  redis.call(
    'HSET',
    completionKey,
    'completionState', 'retrying',
    'lastError', envelope.errorJson,
    'nextDueAt', envelope.nextDueAt,
    'updatedAt', envelope.updatedAt
  )
  redis.call('HDEL', completionKey, 'deliveryOwnerId', 'deliveryLeaseDeadlineMs', 'deliveryLeaseDeadlineAt')
  redis.call('ZADD', completionsDueKey, tostring(envelope.nextDueMs), redis.call('HGET', completionKey, 'id'))
  redis.call('HSET', parentKey, 'completionState', 'retrying', 'updatedAt', envelope.updatedAt)
  return {'ok', 'retrying'}
end

redis.call(
  'HSET',
  completionKey,
  'completionState', 'failed',
  'lastError', envelope.errorJson,
  'updatedAt', envelope.updatedAt
)
redis.call('HDEL', completionKey, 'deliveryOwnerId', 'deliveryLeaseDeadlineMs', 'deliveryLeaseDeadlineAt', 'nextDueAt')
redis.call('ZREM', completionsDueKey, redis.call('HGET', completionKey, 'id'))
redis.call('HSET', parentKey, 'completionState', 'failed', 'updatedAt', envelope.updatedAt)
return {'ok', 'failed'}
`;

export function registerCoordinatorScripts(registry = new QueuebitScriptRegistry()) {
  return {
    claimRun: registry.register({
      name: 'coordinator:claim-run',
      version: 'v1',
      numberOfKeys: 1,
      source: coordinatorClaimRunScriptSource
    }),
    dispatchBatch: registry.register({
      name: 'coordinator:dispatch-batch',
      version: 'v3',
      numberOfKeys: 'dynamic',
      source: coordinatorDispatchBatchScriptSource
    }),
    settleBatch: registry.register({
      name: 'coordinator:settle-batch',
      version: 'v3',
      numberOfKeys: 'dynamic',
      source: coordinatorSettleBatchScriptSource
    }),
    claimCompletion: registry.register({
      name: 'coordinator:claim-completion',
      version: 'v1',
      numberOfKeys: 2,
      source: coordinatorClaimCompletionScriptSource
    }),
    settleCompletion: registry.register({
      name: 'coordinator:settle-completion',
      version: 'v3',
      numberOfKeys: 10,
      source: coordinatorSettleCompletionScriptSource
    })
  };
}
