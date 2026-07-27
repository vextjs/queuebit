import type { JobAddOptions } from '../jobs';
import type { QueuebitWorkerProcessor, QueuebitWorkerProcessorContext } from '../worker';

export interface QueuebitSourceFreezeContext<Input = unknown> {
  runId: string;
  input: Input;
  signal: AbortSignal;
}

export interface QueuebitSourceLoadContext<Input = unknown, Boundary = unknown, Cursor = unknown> {
  runId: string;
  input: Input;
  boundary: Boundary;
  cursor: Cursor;
  limit: number;
  signal: AbortSignal;
}

export interface QueuebitSourceFreezeResult<Boundary = unknown, Cursor = unknown> {
  boundary: Boundary;
  cursor: Cursor;
  totalRecords?: number;
}

export interface QueuebitSourceLoadResult<Record = unknown, Cursor = unknown> {
  records: Record[];
  nextCursor: Cursor;
  exhausted: boolean;
}

export interface QueuebitSource<Input = unknown, Boundary = unknown, Cursor = unknown, Record = unknown> {
  freeze(context: QueuebitSourceFreezeContext<Input>): Promise<QueuebitSourceFreezeResult<Boundary, Cursor>>;
  load(
    context: QueuebitSourceLoadContext<Input, Boundary, Cursor>
  ): Promise<QueuebitSourceLoadResult<Record, Cursor>>;
}

export interface QueuebitMappedJob<Data = unknown> {
  name: string;
  data: Data;
  identity: string;
  options?: JobAddOptions;
}

export type QueuebitMapperResult<Data = unknown> =
  | QueuebitMappedJob<Data>
  | Array<QueuebitMappedJob<Data>>
  | null
  | undefined;

export interface QueuebitMapperContext<Input = unknown, Boundary = unknown, Cursor = unknown> {
  runId: string;
  batchId: string;
  input: Input;
  boundary: Boundary;
  cursor: Cursor;
  recordIndex: number;
}

export type QueuebitMapper<
  Record = unknown,
  Data = unknown,
  Input = unknown,
  Boundary = unknown,
  Cursor = unknown
> = (
  record: Record,
  context: QueuebitMapperContext<Input, Boundary, Cursor>
) => QueuebitMapperResult<Data> | Promise<QueuebitMapperResult<Data>>;

export type QueuebitCompletionEventType = 'batch.settled' | 'run.settled' | 'run.cancelled';

export interface QueuebitCompletionEvent<Summary = unknown> {
  id: string;
  type: QueuebitCompletionEventType;
  runId: string;
  batchId?: string;
  handler: string;
  attempt: number;
  deliveryGeneration: number;
  summary: Summary;
}

export interface QueuebitCompletionHandlerContext {
  signal: AbortSignal;
  coordinatorId: string;
}

export type QueuebitCompletionHandler<Summary = unknown> = (
  event: QueuebitCompletionEvent<Summary>,
  context: QueuebitCompletionHandlerContext
) => Promise<void> | void;

export type QueuebitProcessor<Data = unknown, Result = unknown> =
  QueuebitWorkerProcessor<Data, Result>;

export type QueuebitProcessorContext = QueuebitWorkerProcessorContext;

export type QueuebitAnySource = QueuebitSource<any, any, any, any>;
export type QueuebitAnyMapper = QueuebitMapper<any, any, any, any, any>;
export type QueuebitAnyProcessor = QueuebitProcessor<any, any>;
export type QueuebitAnyCompletionHandler = QueuebitCompletionHandler<any>;

export interface QueuebitRuntimeInput {
  sources?: Record<string, QueuebitAnySource>;
  mappers?: Record<string, QueuebitAnyMapper>;
  processors?: Record<string, QueuebitAnyProcessor>;
  completions?: Record<string, QueuebitAnyCompletionHandler>;
}

export interface QueuebitRuntimeDefinition {
  sources: Record<string, QueuebitAnySource>;
  mappers: Record<string, QueuebitAnyMapper>;
  processors?: Record<string, QueuebitAnyProcessor>;
  completions?: Record<string, QueuebitAnyCompletionHandler>;
}
