import { createHash } from 'node:crypto';
import { QueuebitError } from '../errors';

export interface QueuebitScriptDefinition {
  name: string;
  version: string;
  source: string;
  numberOfKeys: number | 'dynamic';
  readOnly?: boolean;
}

export interface QueuebitRegisteredScript extends QueuebitScriptDefinition {
  sha1: string;
}

const scriptNamePattern = /^[A-Za-z][A-Za-z0-9:_-]{0,127}$/;
const scriptVersionPattern = /^v[0-9]+(?:\.[0-9]+){0,2}$/;

export class QueuebitScriptRegistry {
  readonly #scripts = new Map<string, QueuebitRegisteredScript>();

  register(definition: QueuebitScriptDefinition): QueuebitRegisteredScript {
    validateScriptDefinition(definition);
    const id = scriptId(definition.name, definition.version);
    if (this.#scripts.has(id)) {
      throw new QueuebitError({
        code: 'QB_REDIS_SCRIPT_INVALID',
        message: `Redis script already registered: ${id}.`,
        details: { id }
      });
    }
    const script = {
      ...definition,
      sha1: createHash('sha1').update(definition.source).digest('hex')
    };
    this.#scripts.set(id, script);
    return script;
  }

  get(name: string, version: string): QueuebitRegisteredScript | undefined {
    return this.#scripts.get(scriptId(name, version));
  }

  list(): QueuebitRegisteredScript[] {
    return [...this.#scripts.values()].sort((left, right) =>
      scriptId(left.name, left.version).localeCompare(scriptId(right.name, right.version))
    );
  }
}

function validateScriptDefinition(definition: QueuebitScriptDefinition) {
  if (!scriptNamePattern.test(definition.name)) {
    throw new QueuebitError({
      code: 'QB_REDIS_SCRIPT_INVALID',
      message: `Invalid Redis script name: "${definition.name}".`,
      details: { name: definition.name }
    });
  }
  if (!scriptVersionPattern.test(definition.version)) {
    throw new QueuebitError({
      code: 'QB_REDIS_SCRIPT_INVALID',
      message: `Invalid Redis script version: "${definition.version}".`,
      details: { version: definition.version }
    });
  }
  if (
    definition.numberOfKeys !== 'dynamic'
    && (!Number.isInteger(definition.numberOfKeys) || definition.numberOfKeys < 0)
  ) {
    throw new QueuebitError({
      code: 'QB_REDIS_SCRIPT_INVALID',
      message: 'Redis script numberOfKeys must be a non-negative integer.',
      details: { numberOfKeys: definition.numberOfKeys }
    });
  }
  if (definition.source.trim().length === 0) {
    throw new QueuebitError({
      code: 'QB_REDIS_SCRIPT_INVALID',
      message: 'Redis script source must not be empty.'
    });
  }
}

function scriptId(name: string, version: string): string {
  return `${name}@${version}`;
}
