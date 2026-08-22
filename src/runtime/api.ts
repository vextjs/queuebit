import { QueuebitError } from '../errors';
import type {
  QueuebitCompletionHandler,
  QueuebitMapper,
  QueuebitProcessor,
  QueuebitRuntimeDefinition,
  QueuebitRuntimeInput,
  QueuebitSource
} from './types';

const runtimeNamePattern = /^[A-Za-z0-9][A-Za-z0-9:_-]{0,191}$/;

export function defineQueuebitRuntime(definition: QueuebitRuntimeInput): QueuebitRuntimeDefinition {
  const sources = normalizeRegistry('source', definition.sources);
  const mappers = normalizeRegistry('mapper', definition.mappers);
  const processors = normalizeRegistry('processor', definition.processors);
  const completions = normalizeRegistry('completion handler', definition.completions);
  return {
    sources,
    mappers,
    ...(Object.keys(processors).length === 0 ? {} : { processors }),
    ...(Object.keys(completions).length === 0 ? {} : { completions })
  };
}

export function defineQueuebitSource<
  Input = unknown,
  Boundary = unknown,
  Cursor = unknown,
  Record = unknown
>(
  source: QueuebitSource<Input, Boundary, Cursor, Record>
): QueuebitSource<Input, Boundary, Cursor, Record> {
  return source;
}

export function defineQueuebitMapper<
  Record = unknown,
  Data = unknown,
  Input = unknown,
  Boundary = unknown,
  Cursor = unknown
>(
  mapper: QueuebitMapper<Record, Data, Input, Boundary, Cursor>
): QueuebitMapper<Record, Data, Input, Boundary, Cursor> {
  return mapper;
}

export function defineQueuebitCompletionHandler<Summary = unknown>(
  handler: QueuebitCompletionHandler<Summary>
): QueuebitCompletionHandler<Summary> {
  return handler;
}

export function defineQueuebitProcessor<Data = unknown, Result = unknown>(
  processor: QueuebitProcessor<Data, Result>
): QueuebitProcessor<Data, Result> {
  return processor;
}

/**
 * Creates the Worker processor that dispatches jobs to the runtime processor
 * registry. Applications can use this directly instead of depending on CLI
 * process bootstrapping.
 */
export function createQueuebitRuntimeProcessor(runtime: QueuebitRuntimeDefinition): QueuebitProcessor {
  const processors = runtime.processors ?? {};
  if (Object.keys(processors).length === 0) {
    throw new QueuebitError({
      code: 'QB_CONFIG_HANDLER_NOT_REGISTERED',
      message: 'Runtime processor dispatch requires at least one processor registration.',
      details: { registry: 'processors' }
    });
  }
  return async (job, context) => {
    const processor = processors[job.name];
    if (processor === undefined) {
      throw new QueuebitError({
        code: 'QB_CONFIG_HANDLER_NOT_REGISTERED',
        message: `No runtime processor registered for job "${job.name}".`,
        details: { jobId: job.id, queue: job.queue, name: job.name }
      });
    }
    return processor(job, context);
  };
}

function normalizeRegistry<T>(kind: string, registry: Record<string, T> | undefined): Record<string, T> {
  const normalized: Record<string, T> = {};
  for (const [name, value] of Object.entries(registry ?? {})) {
    if (!runtimeNamePattern.test(name)) {
      throw new QueuebitError({
        code: 'QB_CONFIG_INVALID',
        message: `Invalid Queuebit runtime ${kind} name: "${name}".`,
        details: { kind, name }
      });
    }
    normalized[name] = value;
  }
  return normalized;
}
