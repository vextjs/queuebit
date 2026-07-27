import { QueuebitScriptRegistry } from '../redis';

export const completionsRetryScriptSource = `
local completionKey = KEYS[1]
local dueKey = KEYS[2]

local nowMs = tonumber(ARGV[1])
local nextDueAt = ARGV[2]
local updatedAt = ARGV[3]

if redis.call('EXISTS', completionKey) == 0 then
  return {'err', 'QB_COMPLETION_NOT_FOUND', 'Completion event does not exist.', '{}'}
end

if redis.call('HGET', completionKey, 'detailsExpired') == '1' then
  return {'err', 'QB_COMPLETION_STATE_CONFLICT', 'Completion event details have expired.', cjson.encode({detailsExpired=true})}
end

local state = redis.call('HGET', completionKey, 'completionState')
if state ~= 'failed' then
  return {'err', 'QB_COMPLETION_STATE_CONFLICT', 'Only failed completion events can be retried.', cjson.encode({state=state})}
end

redis.call(
  'HSET',
  completionKey,
  'completionState', 'pending',
  'nextDueAt', nextDueAt,
  'updatedAt', updatedAt
)
redis.call('HDEL', completionKey, 'lastError')
redis.call('ZADD', dueKey, tostring(nowMs), redis.call('HGET', completionKey, 'id'))
return {'ok', redis.call('HGET', completionKey, 'id')}
`;

export function registerCompletionsScripts(registry = new QueuebitScriptRegistry()) {
  return {
    retry: registry.register({
      name: 'completions:retry',
      version: 'v1',
      numberOfKeys: 2,
      source: completionsRetryScriptSource
    })
  };
}
