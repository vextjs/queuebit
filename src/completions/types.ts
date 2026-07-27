import type { CompletionState, CursorPage } from '../runs/types';
import type { QueuebitCompletionEventType } from '../runtime';
import type { QueuebitSerializedError } from '../jobs';

export interface CompletionBackoffSnapshot {
  type: 'fixed' | 'exponential';
  delayMs: number;
  maxDelayMs?: number;
}

export interface CompletionListQuery {
  runId?: string;
  batchId?: string;
  type?: QueuebitCompletionEventType;
  completionState?: CompletionState;
  cursor?: string;
  limit?: number;
}

export interface CompletionEventSummary {
  id: string;
  type: QueuebitCompletionEventType;
  runId: string;
  batchId?: string;
  handler?: string;
  completionState: CompletionState;
  attempt: number;
  attempts: number;
  deliveryGeneration: number;
  summaryDigest?: string;
  detailsExpired?: true;
  detailsExpiredAt?: string;
  createdAt: string;
  updatedAt: string;
}

export interface CompletionSnapshot<Summary = unknown> extends CompletionEventSummary {
  summary?: Summary;
  backoff?: CompletionBackoffSnapshot;
  lastError?: QueuebitSerializedError;
  nextDueAt?: string;
}

export interface CompletionsApi {
  list(query?: CompletionListQuery): Promise<CursorPage<CompletionEventSummary>>;
  get<Summary = unknown>(eventId: string): Promise<CompletionSnapshot<Summary> | null>;
  retry<Summary = unknown>(eventId: string): Promise<CompletionSnapshot<Summary>>;
}
