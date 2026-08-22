import {
  defineQueuebitCompletionHandler,
  defineQueuebitMapper,
  defineQueuebitProcessor,
  defineQueuebitRuntime,
  defineQueuebitSource
} from 'queuebit';
import type {
  PaidOrder,
  ReceiptRepository,
  ReceiptRunInput,
  SendReceiptJob
} from './receipt-repository.js';

export function createReceiptRuntime(repository: ReceiptRepository) {
  return defineQueuebitRuntime({
    sources: {
      'paid-orders': defineQueuebitSource<ReceiptRunInput, { maxId: number }, number, PaidOrder>({
        async freeze({ input }) {
          const boundary = await repository.freezePaidOrders(input);
          return { boundary: { maxId: boundary.maxId }, cursor: 0, totalRecords: boundary.totalRecords };
        },
        async load({ input, boundary, cursor, limit }) {
          const records = await repository.loadPaidOrders({ input, boundary, cursor, limit });
          const nextCursor = records.at(-1)?.id ?? cursor;
          return {
            records,
            nextCursor,
            exhausted: records.length === 0 || nextCursor >= boundary.maxId
          };
        }
      })
    },
    mappers: {
      'receipt-jobs': defineQueuebitMapper<PaidOrder, SendReceiptJob>((record) => {
        if (!record.receiptEmail) return null;
        return {
          name: 'send-receipt',
          identity: `order:${record.id}`,
          data: {
            schemaVersion: 1,
            orderId: record.id,
            tenantId: record.tenantId,
            recipient: record.receiptEmail
          },
          options: {
            idempotencyKey: `receipt:${record.tenantId}:${record.id}`
          }
        };
      })
    },
    processors: {
      'send-receipt': defineQueuebitProcessor<SendReceiptJob>(async (job, context) =>
        repository.sendReceipt(job.data, {
          signal: context.signal,
          ...(context.idempotencyKey === undefined
            ? {}
            : { idempotencyKey: context.idempotencyKey })
        }))
    },
    completions: {
      'record-receipt-batch-result': defineQueuebitCompletionHandler(async (event) => {
        await repository.recordReceiptBatchCompletion(event);
      }),
      'record-receipt-run-result': defineQueuebitCompletionHandler(async (event) => {
        await repository.recordReceiptRunCompletion(event);
      })
    }
  });
}
