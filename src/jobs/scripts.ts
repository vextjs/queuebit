import { QueuebitScriptRegistry } from '../redis';

export const jobsAddBulkScriptSource = `
local countersKey = KEYS[1]
local waitingKey = KEYS[2]
local dueKey = KEYS[3]
local jobsIndexKey = KEYS[4]
local waitingIndexKey = KEYS[5]
local delayedIndexKey = KEYS[6]

local entryCount = tonumber(ARGV[1])
local maxBulkJobs = tonumber(ARGV[2])
local maxBulkBytes = tonumber(ARGV[3])
local highJobs = ARGV[4] == '' and nil or tonumber(ARGV[4])
local highBytes = ARGV[5] == '' and nil or tonumber(ARGV[5])
local lowJobs = ARGV[6] == '' and nil or tonumber(ARGV[6])
local lowBytes = ARGV[7] == '' and nil or tonumber(ARGV[7])
local observedAt = ARGV[8]
local bulkBytes = tonumber(ARGV[9])

if entryCount > maxBulkJobs then
  return {'err', 'QB_JOB_LIMIT_EXCEEDED', 'jobs.addBulk exceeds maxBulkJobs.', cjson.encode({actual=entryCount, limit=maxBulkJobs})}
end
if bulkBytes > maxBulkBytes then
  return {'err', 'QB_JOB_LIMIT_EXCEEDED', 'jobs.addBulk exceeds maxBulkBytes.', cjson.encode({actual=bulkBytes, limit=maxBulkBytes})}
end

local jobKeyOffset = 6
local dedupeKeyOffset = 6 + entryCount
local argOffset = 9
local entries = {}
local resultIds = {}
local newJobs = 0
local newBytes = 0

local function backpressureDetails(reason, currentJobs, currentBytes, incomingJobs, incomingBytes)
  return cjson.encode({
    reason=reason,
    currentJobs=currentJobs,
    currentBytes=currentBytes,
    incomingJobs=incomingJobs,
    incomingBytes=incomingBytes,
    highJobs=highJobs or '',
    lowJobs=lowJobs or '',
    highBytes=highBytes or '',
    lowBytes=lowBytes or ''
  })
end

local function clearBackpressureLatch()
  redis.call(
    'HDEL',
    countersKey,
    'backpressureLatched',
    'backpressureReason',
    'backpressureSince',
    'backpressureLastCheckedAt'
  )
end

local function setBackpressureLatch(reason)
  redis.call(
    'HSET',
    countersKey,
    'backpressureLatched', '1',
    'backpressureReason', reason,
    'backpressureLastCheckedAt', observedAt
  )
  if redis.call('HGET', countersKey, 'backpressureSince') == false then
    redis.call('HSET', countersKey, 'backpressureSince', observedAt)
  end
end

local function refreshBackpressureLatch(currentJobs, currentBytes)
  if not highJobs and not highBytes then
    clearBackpressureLatch()
    return false
  end
  redis.call('HSET', countersKey, 'backpressureLastCheckedAt', observedAt)
  local effectiveLowJobs = lowJobs or highJobs
  local effectiveLowBytes = lowBytes or highBytes
  local belowJobs = (not highJobs) or currentJobs <= effectiveLowJobs
  local belowBytes = (not highBytes) or currentBytes <= effectiveLowBytes
  if redis.call('HGET', countersKey, 'backpressureLatched') == '1' and belowJobs and belowBytes then
    clearBackpressureLatch()
  end
  local reason = ''
  if highJobs and currentJobs >= highJobs then
    reason = 'jobs'
  elseif highBytes and currentBytes >= highBytes then
    reason = 'bytes'
  end
  if reason ~= '' then
    setBackpressureLatch(reason)
  end
  return redis.call('HGET', countersKey, 'backpressureLatched') == '1'
end

for index = 1, entryCount do
  local entry = cjson.decode(ARGV[argOffset + index])
  entries[index] = entry
  if entry.dedupeKeyPosition > 0 then
    local dedupeKey = KEYS[dedupeKeyOffset + entry.dedupeKeyPosition]
    local existing = redis.call('GET', dedupeKey)
    if existing then
      local existingIdentity = cjson.decode(existing)
      if existingIdentity.dataDigest ~= entry.dataDigest then
        return {'err', 'QB_JOB_DEDUPLICATION_CONFLICT', 'jobs.addBulk deduplicationKey conflicts with existing job data.', cjson.encode({deduplicationKey=entry.deduplicationKey})}
      end
      resultIds[index] = existingIdentity.jobId
    else
      newJobs = newJobs + 1
      newBytes = newBytes + tonumber(entry.dataBytes)
    end
  else
    newJobs = newJobs + 1
    newBytes = newBytes + tonumber(entry.dataBytes)
  end
end

local currentJobs = tonumber(redis.call('HGET', countersKey, 'queuedJobs') or '0')
local currentBytes = tonumber(redis.call('HGET', countersKey, 'queuedBytes') or '0')
if newJobs > 0 then
  if highJobs and newJobs > highJobs then
    return {'err', 'QB_BACKPRESSURE_REQUEST_TOO_LARGE', 'jobs.addBulk request exceeds queue high job watermark.', backpressureDetails('jobs', currentJobs, currentBytes, newJobs, newBytes)}
  end
  if highBytes and newBytes > highBytes then
    return {'err', 'QB_BACKPRESSURE_REQUEST_TOO_LARGE', 'jobs.addBulk request exceeds queue high byte watermark.', backpressureDetails('bytes', currentJobs, currentBytes, newJobs, newBytes)}
  end
  if refreshBackpressureLatch(currentJobs, currentBytes) then
    return {'err', 'QB_BACKPRESSURE_REJECTED', 'Queue backpressure is active.', backpressureDetails(redis.call('HGET', countersKey, 'backpressureReason') or 'unknown', currentJobs, currentBytes, newJobs, newBytes)}
  end
  if highJobs and currentJobs + newJobs > highJobs then
    setBackpressureLatch('jobs')
    return {'err', 'QB_BACKPRESSURE_REJECTED', 'Queue job high watermark would be exceeded.', backpressureDetails('jobs', currentJobs, currentBytes, newJobs, newBytes)}
  end
  if highBytes and currentBytes + newBytes > highBytes then
    setBackpressureLatch('bytes')
    return {'err', 'QB_BACKPRESSURE_REJECTED', 'Queue byte high watermark would be exceeded.', backpressureDetails('bytes', currentJobs, currentBytes, newJobs, newBytes)}
  end
else
  refreshBackpressureLatch(currentJobs, currentBytes)
end
end

local waitingAdded = 0
local delayedAdded = 0
for index = 1, entryCount do
  if not resultIds[index] then
    local entry = entries[index]
    local jobKey = KEYS[jobKeyOffset + index]
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
      'sequence', sequence
    )
    if entry.deduplicationKey ~= '' then
      redis.call('HSET', jobKey, 'deduplicationKey', entry.deduplicationKey)
    end
    if entry.idempotencyKey ~= '' then
      redis.call('HSET', jobKey, 'idempotencyKey', entry.idempotencyKey)
    end
    if entry.parentJobId and entry.parentJobId ~= '' then
      redis.call('HSET', jobKey, 'parentJobId', entry.parentJobId)
    end
    if entry.runId and entry.runId ~= '' then
      redis.call('HSET', jobKey, 'runId', entry.runId)
    end
    if entry.batchId and entry.batchId ~= '' then
      redis.call('HSET', jobKey, 'batchId', entry.batchId)
    end
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
    if entry.dedupeKeyPosition > 0 then
      local dedupeKey = KEYS[dedupeKeyOffset + entry.dedupeKeyPosition]
      redis.call('SET', dedupeKey, cjson.encode({jobId=entry.jobId, dataDigest=entry.dataDigest}))
    end
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
  refreshBackpressureLatch(currentJobs + newJobs, currentBytes + newBytes)
end

return {'ok', cjson.encode(resultIds)}
`;

export const jobsCancelScriptSource = `
local jobKey = KEYS[1]
local waitingKey = KEYS[2]
local dueKey = KEYS[3]
local waitingIndexKey = KEYS[4]
local delayedIndexKey = KEYS[5]
local retryingIndexKey = KEYS[6]
local cancelledIndexKey = KEYS[7]
local countersKey = KEYS[8]

local updatedAt = ARGV[1]

if redis.call('EXISTS', jobKey) == 0 then
  return {'err', 'QB_JOB_NOT_FOUND', 'Job does not exist.', '{}'}
end

local state = redis.call('HGET', jobKey, 'state')
local jobId = redis.call('HGET', jobKey, 'id')
local sequence = redis.call('HGET', jobKey, 'sequence')
local dataBytes = tonumber(redis.call('HGET', jobKey, 'dataBytes') or '0')

if state == 'cancelled' then
  return {'ok', jobId}
end
if state ~= 'waiting' and state ~= 'delayed' and state ~= 'retrying' then
  return {'err', 'QB_JOB_STATE_CONFLICT', 'Only waiting, delayed, or retrying jobs can be cancelled.', '{}'}
end

if state == 'waiting' then
  redis.call('LREM', waitingKey, 0, jobKey)
  redis.call('ZREM', waitingIndexKey, jobId)
  redis.call('HINCRBY', countersKey, 'waitingJobs', -1)
elseif state == 'delayed' then
  redis.call('ZREM', dueKey, jobKey)
  redis.call('ZREM', delayedIndexKey, jobId)
  redis.call('HINCRBY', countersKey, 'delayedJobs', -1)
else
  redis.call('ZREM', dueKey, jobKey)
  redis.call('ZREM', retryingIndexKey, jobId)
  redis.call('HINCRBY', countersKey, 'retryingJobs', -1)
end

redis.call('HSET', jobKey, 'state', 'cancelled', 'updatedAt', updatedAt)
redis.call('HDEL', jobKey, 'workerId', 'leaseDeadlineAt', 'retryAtMs')
redis.call('ZADD', cancelledIndexKey, sequence, jobId)
redis.call('HINCRBY', countersKey, 'cancelledJobs', 1)
redis.call('HINCRBY', countersKey, 'queuedJobs', -1)
if dataBytes > 0 then
  redis.call('HINCRBY', countersKey, 'queuedBytes', -dataBytes)
end

return {'ok', jobId}
`;

export function registerJobsScripts(registry = new QueuebitScriptRegistry()) {
  return {
    addBulk: registry.register({
      name: 'jobs:add-bulk',
      version: 'v1',
      numberOfKeys: 'dynamic',
      source: jobsAddBulkScriptSource
    }),
    cancel: registry.register({
      name: 'jobs:cancel',
      version: 'v1',
      numberOfKeys: 8,
      source: jobsCancelScriptSource
    })
  };
}
