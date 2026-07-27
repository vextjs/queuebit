import { createHash } from 'node:crypto';
import { QueuebitError } from './errors';

export type CanonicalInputValue =
  | null
  | string
  | number
  | boolean
  | CanonicalInputValue[]
  | { [key: string]: CanonicalInputValue };

export interface CanonicalDigest {
  version: 'qbcj-v1';
  json: string;
  sha256: string;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object') return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function normalizeCanonicalValue(value: unknown, path: string): CanonicalInputValue {
  if (value === null) return null;

  if (typeof value === 'string' || typeof value === 'boolean') return value;

  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new QueuebitError({
        code: 'QB_CANONICAL_INPUT_UNSUPPORTED',
        message: `Canonical input contains a non-finite number at ${path}.`,
        details: { path, type: 'number' }
      });
    }
    return Object.is(value, -0) ? 0 : value;
  }

  if (Array.isArray(value)) {
    return value.map((item, index) => normalizeCanonicalValue(item, `${path}[${index}]`));
  }

  if (isPlainObject(value)) {
    const normalized: Record<string, CanonicalInputValue> = {};
    for (const key of Object.keys(value).sort()) {
      const child = value[key];
      if (typeof child === 'undefined') {
        throw new QueuebitError({
          code: 'QB_CANONICAL_INPUT_UNSUPPORTED',
          message: `Canonical input contains undefined at ${path}.${key}.`,
          details: { path: `${path}.${key}`, type: 'undefined' }
        });
      }
      normalized[key] = normalizeCanonicalValue(child, `${path}.${key}`);
    }
    return normalized;
  }

  throw new QueuebitError({
    code: 'QB_CANONICAL_INPUT_UNSUPPORTED',
    message: `Canonical input contains unsupported value at ${path}.`,
    details: { path, type: typeof value }
  });
}

/**
 * Converts user input into Queuebit canonical JSON. Object keys are sorted,
 * array order is preserved, and ambiguous values fail before hashing.
 */
export function canonicalizeInput(input: unknown): string {
  return JSON.stringify(normalizeCanonicalValue(input, '$'));
}

/**
 * Creates the v0.1 canonical digest used by future job/run dedupe keys.
 * The digest is a shared identity primitive, not an exactly-once guarantee.
 */
export function createCanonicalDigest(input: unknown): CanonicalDigest {
  const json = canonicalizeInput(input);
  const sha256 = createHash('sha256').update('qbcj-v1').update('\0').update(json).digest('hex');
  return { version: 'qbcj-v1', json, sha256 };
}
