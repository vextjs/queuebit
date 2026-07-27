import { QueuebitError } from '../errors';
import type { QueuebitRegisteredScript } from './script-registry';

export interface QueuebitRedisCommandClient {
  sendCommand(command: string[]): Promise<unknown>;
}

export async function executeQueuebitScript(
  client: QueuebitRedisCommandClient,
  script: QueuebitRegisteredScript,
  keys: string[],
  args: string[]
): Promise<unknown> {
  assertScriptKeyCount(script, keys);
  try {
    return await evalSha(client, script.sha1, keys, args);
  } catch (cause) {
    if (!isNoScriptError(cause)) {
      throw scriptExecutionError(script, cause);
    }
    try {
      const loadedSha = await client.sendCommand(['SCRIPT', 'LOAD', script.source]);
      return await evalSha(client, String(loadedSha), keys, args);
    } catch (loadCause) {
      throw scriptExecutionError(script, loadCause);
    }
  }
}

function assertScriptKeyCount(script: QueuebitRegisteredScript, keys: string[]) {
  if (script.numberOfKeys === 'dynamic') return;
  if (keys.length === script.numberOfKeys) return;
  throw new QueuebitError({
    code: 'QB_REDIS_SCRIPT_INVALID',
    message: `Redis script ${script.name}@${script.version} expected ${script.numberOfKeys} keys.`,
    details: { script: script.name, version: script.version, expected: script.numberOfKeys, actual: keys.length }
  });
}

async function evalSha(
  client: QueuebitRedisCommandClient,
  sha1: string,
  keys: string[],
  args: string[]
): Promise<unknown> {
  return client.sendCommand(['EVALSHA', sha1, String(keys.length), ...keys, ...args]);
}

function isNoScriptError(cause: unknown): boolean {
  return cause instanceof Error && /NOSCRIPT/i.test(cause.message);
}

function scriptExecutionError(script: QueuebitRegisteredScript, cause: unknown): QueuebitError {
  return new QueuebitError({
    code: 'QB_REDIS_SCRIPT_EXECUTION_FAILED',
    message: `Redis script execution failed: ${script.name}@${script.version}.`,
    details: { script: script.name, version: script.version, cause }
  });
}
