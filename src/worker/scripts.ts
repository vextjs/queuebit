import { QueuebitScriptRegistry } from '../redis';

export const workerClaimScriptSource = `
local waitingKey = KEYS[1]
local activeKey = KEYS[2]
local waitingIndexKey = KEYS[3]
local activeIndexKey = KEYS[4]
local countersKey = KEYS[5]

local workerId = ARGV[1]
local leaseDeadlineMs = tonumber(ARGV[2])
local leaseDeadlineAt = ARGV[3]
local updatedAt = ARGV[4]
local maxSkips = tonumber(ARGV[5])

for _ = 1, maxSkips do
  local jobKey = redis.call('LPOP', waitingKey)
  if not jobKey then
    return {'ok', ''}
  end

  local state = redis.call('HGET', jobKey, 'state')
  if state == 'waiting' then
    local jobId = redis.call('HGET', jobKey, 'id')
    local sequence = redis.call('HGET', jobKey, 'sequence')
    local nextGeneration = tostring(redis.call('HINCRBY', jobKey, 'leaseGeneration', 1))
    -- Stalled reclaims keep the same business attempt (docs: same attempt + new generation).
    local nextAttempt
    if redis.call('HGET', jobKey, 'reclaimSameAttempt') == '1' then
      nextAttempt = tostring(tonumber(redis.call('HGET', jobKey, 'attempt') or '0'))
      redis.call('HDEL', jobKey, 'reclaimSameAttempt')
    else
      nextAttempt = tostring(redis.call('HINCRBY', jobKey, 'attempt', 1))
    end
    redis.call(
      'HSET',
      jobKey,
      'state', 'active',
      'attempt', nextAttempt,
      'workerId', workerId,
      'leaseDeadlineAt', leaseDeadlineAt,
      'updatedAt', updatedAt
    )
    redis.call('ZREM', waitingIndexKey, jobId)
    redis.call('ZADD', activeIndexKey, sequence, jobId)
    redis.call('ZADD', activeKey, leaseDeadlineMs, jobKey)
    redis.call('HINCRBY', countersKey, 'waitingJobs', -1)
    redis.call('HINCRBY', countersKey, 'activeJobs', 1)
    return {'ok', jobId, nextGeneration}
  end
end

return {'ok', ''}
`;

export const workerRenewScriptSource = `
local jobKey = KEYS[1]
local activeKey = KEYS[2]

local workerId = ARGV[1]
local leaseGeneration = ARGV[2]
local leaseDeadlineMs = tonumber(ARGV[3])
local leaseDeadlineAt = ARGV[4]
local updatedAt = ARGV[5]

if redis.call('EXISTS', jobKey) == 0 then
  return {'err', 'QB_JOB_NOT_FOUND', 'Job does not exist.', '{}'}
end
if redis.call('HGET', jobKey, 'state') ~= 'active' then
  return {'err', 'QB_JOB_STATE_CONFLICT', 'Only active jobs can be renewed.', '{}'}
end
if redis.call('HGET', jobKey, 'workerId') ~= workerId then
  return {'err', 'QB_JOB_STATE_CONFLICT', 'Worker does not own this job lease.', '{}'}
end
if redis.call('HGET', jobKey, 'leaseGeneration') ~= leaseGeneration then
  return {'err', 'QB_JOB_STATE_CONFLICT', 'Worker lease generation is stale.', '{}'}
end

redis.call(
  'HSET',
  jobKey,
  'leaseDeadlineAt', leaseDeadlineAt,
  'updatedAt', updatedAt
)
redis.call('ZADD', activeKey, leaseDeadlineMs, jobKey)
return {'ok', redis.call('HGET', jobKey, 'id')}
`;

export const workerSettleScriptSource = `
local jobKey = KEYS[1]
local activeKey = KEYS[2]
local activeIndexKey = KEYS[3]
local terminalIndexKey = KEYS[4]
local countersKey = KEYS[5]
local dueKey = KEYS[6]
local retryingIndexKey = KEYS[7]

local workerId = ARGV[1]
local leaseGeneration = ARGV[2]
local terminalState = ARGV[3]
local payloadJson = ARGV[4]
local updatedAt = ARGV[5]
local nowMs = tonumber(ARGV[6])

if redis.call('EXISTS', jobKey) == 0 then
  return {'err', 'QB_JOB_NOT_FOUND', 'Job does not exist.', '{}'}
end
if redis.call('HGET', jobKey, 'state') ~= 'active' then
  return {'err', 'QB_JOB_STATE_CONFLICT', 'Only active jobs can be settled.', '{}'}
end
if redis.call('HGET', jobKey, 'workerId') ~= workerId then
  return {'err', 'QB_JOB_STATE_CONFLICT', 'Worker does not own this job lease.', '{}'}
end
if redis.call('HGET', jobKey, 'leaseGeneration') ~= leaseGeneration then
  return {'err', 'QB_JOB_STATE_CONFLICT', 'Worker lease generation is stale.', '{}'}
end
if terminalState ~= 'completed' and terminalState ~= 'failed' then
  return {'err', 'QB_JOB_INVALID', 'Invalid terminal state.', '{}'}
end

local jobId = redis.call('HGET', jobKey, 'id')
local sequence = redis.call('HGET', jobKey, 'sequence')
local dataBytes = tonumber(redis.call('HGET', jobKey, 'dataBytes') or '0')
if terminalState == 'completed' then
  redis.call('HSET', jobKey, 'state', terminalState, 'updatedAt', updatedAt)
  if payloadJson ~= '' then
    redis.call('HSET', jobKey, 'result', payloadJson)
  end
  redis.call('HINCRBY', countersKey, 'completedJobs', 1)
else
  local attempt = tonumber(redis.call('HGET', jobKey, 'attempt') or '0')
  local attempts = tonumber(redis.call('HGET', jobKey, 'attempts') or '1')
  if attempt < attempts then
    local optionsJson = redis.call('HGET', jobKey, 'options') or '{}'
    local options = cjson.decode(optionsJson)
    local delayMs = 0
    if options['backoff'] then
      delayMs = tonumber(options['backoff']['delayMs'] or '0') or 0
      if options['backoff']['type'] == 'exponential' then
        delayMs = delayMs * math.pow(2, math.max(attempt - 1, 0))
      end
      if options['backoff']['maxDelayMs'] and delayMs > tonumber(options['backoff']['maxDelayMs']) then
        delayMs = tonumber(options['backoff']['maxDelayMs'])
      end
    end
    local retryAtMs = nowMs + delayMs
    redis.call(
      'HSET',
      jobKey,
      'state', 'retrying',
      'failedReason', payloadJson,
      'retryAtMs', tostring(retryAtMs),
      'updatedAt', updatedAt
    )
    redis.call('HDEL', jobKey, 'workerId', 'leaseDeadlineAt')
    redis.call('ZREM', activeKey, jobKey)
    redis.call('ZREM', activeIndexKey, jobId)
    redis.call('ZADD', retryingIndexKey, sequence, jobId)
    redis.call('ZADD', dueKey, retryAtMs, jobKey)
    redis.call('HINCRBY', countersKey, 'activeJobs', -1)
    redis.call('HINCRBY', countersKey, 'retryingJobs', 1)
    return {'ok', jobId}
  else
    redis.call('HSET', jobKey, 'state', terminalState, 'failedReason', payloadJson, 'updatedAt', updatedAt)
    redis.call('HINCRBY', countersKey, 'failedJobs', 1)
  end
end
redis.call('ZREM', activeKey, jobKey)
redis.call('ZREM', activeIndexKey, jobId)
redis.call('ZADD', terminalIndexKey, sequence, jobId)
redis.call('HINCRBY', countersKey, 'activeJobs', -1)
redis.call('HINCRBY', countersKey, 'queuedJobs', -1)
if dataBytes > 0 then
  redis.call('HINCRBY', countersKey, 'queuedBytes', -dataBytes)
end
return {'ok', jobId}
`;

export const workerPromoteDueScriptSource = `
local dueKey = KEYS[1]
local waitingKey = KEYS[2]
local delayedIndexKey = KEYS[3]
local retryingIndexKey = KEYS[4]
local waitingIndexKey = KEYS[5]
local countersKey = KEYS[6]

local nowMs = tonumber(ARGV[1])
local updatedAt = ARGV[2]
local limit = tonumber(ARGV[3])

local jobKeys = redis.call('ZRANGEBYSCORE', dueKey, '-inf', tostring(nowMs), 'LIMIT', 0, limit)
local promoted = {}
for index, jobKey in ipairs(jobKeys) do
  local state = redis.call('HGET', jobKey, 'state')
  if state == 'delayed' or state == 'retrying' then
    local jobId = redis.call('HGET', jobKey, 'id')
    local sequence = redis.call('HGET', jobKey, 'sequence')
    redis.call('ZREM', dueKey, jobKey)
    redis.call('HSET', jobKey, 'state', 'waiting', 'updatedAt', updatedAt)
    redis.call('HDEL', jobKey, 'retryAtMs', 'workerId', 'leaseDeadlineAt')
    redis.call('RPUSH', waitingKey, jobKey)
    redis.call('ZADD', waitingIndexKey, sequence, jobId)
    if state == 'delayed' then
      redis.call('ZREM', delayedIndexKey, jobId)
      redis.call('HINCRBY', countersKey, 'delayedJobs', -1)
    else
      redis.call('ZREM', retryingIndexKey, jobId)
      redis.call('HINCRBY', countersKey, 'retryingJobs', -1)
    end
    redis.call('HINCRBY', countersKey, 'waitingJobs', 1)
    promoted[#promoted + 1] = jobId
  else
    redis.call('ZREM', dueKey, jobKey)
  end
end

return {'ok', cjson.encode(promoted)}
`;

export const workerRecoverStalledScriptSource = `
local activeKey = KEYS[1]
local waitingKey = KEYS[2]
local activeIndexKey = KEYS[3]
local waitingIndexKey = KEYS[4]
local failedIndexKey = KEYS[5]
local countersKey = KEYS[6]

local nowMs = tonumber(ARGV[1])
local updatedAt = ARGV[2]
local limit = tonumber(ARGV[3])
local maxStalledRecoveries = tonumber(ARGV[4])
local failedReasonJson = ARGV[5]

local jobKeys = redis.call('ZRANGEBYSCORE', activeKey, '-inf', tostring(nowMs), 'LIMIT', 0, limit)
local recovered = {}
for index, jobKey in ipairs(jobKeys) do
  if redis.call('HGET', jobKey, 'state') == 'active' then
    local jobId = redis.call('HGET', jobKey, 'id')
    local sequence = redis.call('HGET', jobKey, 'sequence')
    local dataBytes = tonumber(redis.call('HGET', jobKey, 'dataBytes') or '0')
    local recoveries = tonumber(redis.call('HINCRBY', jobKey, 'stalledRecoveries', 1))
    redis.call('ZREM', activeKey, jobKey)
    redis.call('ZREM', activeIndexKey, jobId)
    redis.call('HINCRBY', countersKey, 'activeJobs', -1)
    if recoveries > maxStalledRecoveries then
      redis.call(
        'HSET',
        jobKey,
        'state', 'failed',
        'failedReason', failedReasonJson,
        'updatedAt', updatedAt
      )
      redis.call('HDEL', jobKey, 'workerId', 'leaseDeadlineAt')
      redis.call('ZADD', failedIndexKey, sequence, jobId)
      redis.call('HINCRBY', countersKey, 'failedJobs', 1)
      redis.call('HINCRBY', countersKey, 'queuedJobs', -1)
      if dataBytes > 0 then
        redis.call('HINCRBY', countersKey, 'queuedBytes', -dataBytes)
      end
    else
      redis.call(
        'HSET',
        jobKey,
        'state', 'waiting',
        'updatedAt', updatedAt,
        'reclaimSameAttempt', '1'
      )
      redis.call('HDEL', jobKey, 'workerId', 'leaseDeadlineAt')
      redis.call('RPUSH', waitingKey, jobKey)
      redis.call('ZADD', waitingIndexKey, sequence, jobId)
      redis.call('HINCRBY', countersKey, 'waitingJobs', 1)
    end
    recovered[#recovered + 1] = jobId
  else
    redis.call('ZREM', activeKey, jobKey)
  end
end

return {'ok', cjson.encode(recovered)}
`;

export function registerWorkerScripts(registry = new QueuebitScriptRegistry()) {
  return {
    claim: registry.register({
      name: 'worker:claim',
      version: 'v2',
      numberOfKeys: 5,
      source: workerClaimScriptSource
    }),
    renew: registry.register({
      name: 'worker:renew',
      version: 'v1',
      numberOfKeys: 2,
      source: workerRenewScriptSource
    }),
    settle: registry.register({
      name: 'worker:settle',
      version: 'v1',
      numberOfKeys: 7,
      source: workerSettleScriptSource
    }),
    promoteDue: registry.register({
      name: 'worker:promote-due',
      version: 'v1',
      numberOfKeys: 6,
      source: workerPromoteDueScriptSource
    }),
    recoverStalled: registry.register({
      name: 'worker:recover-stalled',
      version: 'v2',
      numberOfKeys: 6,
      source: workerRecoverStalledScriptSource
    })
  };
}
