import type { QueuebitCompletionEvent } from 'queuebit';

export interface ReceiptRunInput {
  tenantId: string;
  paidBefore: string;
}

export interface PaidOrder {
  id: number;
  tenantId: string;
  paidAt: string;
  receiptEmail?: string;
}

export interface SendReceiptJob {
  schemaVersion: 1;
  orderId: number;
  tenantId: string;
  recipient: string;
}

export interface ReceiptRepository {
  freezePaidOrders(input: ReceiptRunInput): Promise<{ maxId: number; totalRecords: number }>;
  loadPaidOrders(request: {
    input: ReceiptRunInput;
    boundary: { maxId: number };
    cursor: number;
    limit: number;
  }): Promise<PaidOrder[]>;
  sendReceipt(
    job: SendReceiptJob,
    context: { idempotencyKey?: string; signal: AbortSignal }
  ): Promise<{ providerMessageId: string }>;
  recordReceiptBatchCompletion(event: QueuebitCompletionEvent): Promise<void>;
  recordReceiptRunCompletion(event: QueuebitCompletionEvent): Promise<void>;
}
