import {
  defineQueuebitCompletionHandler,
  defineQueuebitMapper,
  defineQueuebitProcessor,
  defineQueuebitRuntime,
  defineQueuebitSource
} from 'queuebit';

type ReceiptRunInput = {
  tenantId: string;
  paidBefore: string;
};

type PaidOrder = {
  id: number;
  tenantId: string;
  paidAt: string;
  receiptEmail?: string;
};

type SendReceiptJob = {
  schemaVersion: 1;
  orderId: number;
  tenantId: string;
  recipient: string;
};

const orders: PaidOrder[] = [
  { id: 1, tenantId: 'tenant-demo', paidAt: '2026-07-01T10:00:00.000Z', receiptEmail: 'a@example.com' },
  { id: 2, tenantId: 'tenant-demo', paidAt: '2026-07-02T10:00:00.000Z' },
  { id: 3, tenantId: 'tenant-demo', paidAt: '2026-07-03T10:00:00.000Z', receiptEmail: 'c@example.com' }
];

function selectPaidOrders(input: ReceiptRunInput, afterId = 0, maxId = Number.MAX_SAFE_INTEGER, limit = 100): PaidOrder[] {
  const paidBefore = Date.parse(input.paidBefore);
  return orders
    .filter(order =>
      order.tenantId === input.tenantId
      && Date.parse(order.paidAt) < paidBefore
      && order.id > afterId
      && order.id <= maxId
    )
    .sort((left, right) => left.id - right.id)
    .slice(0, limit);
}

export default defineQueuebitRuntime({
  sources: {
    'paid-orders': defineQueuebitSource<ReceiptRunInput, { maxId: number }, number, PaidOrder>({
      async freeze({ input }) {
        const selected = selectPaidOrders(input);
        const maxId = selected.at(-1)?.id ?? 0;
        return { boundary: { maxId }, cursor: 0, totalRecords: selected.length };
      },
      async load({ input, boundary, cursor, limit }) {
        const records = selectPaidOrders(input, cursor, boundary.maxId, limit);
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
    'send-receipt': defineQueuebitProcessor<SendReceiptJob>(async (job, context) => ({
      provider: 'example-receipt-service',
      idempotencyKey: context.idempotencyKey,
      orderId: job.data.orderId
    }))
  },
  completions: {
    'record-receipt-batch-result': defineQueuebitCompletionHandler(async (event) => {
      console.log('batch completion target', event.id, event.summary);
    }),
    'record-receipt-run-result': defineQueuebitCompletionHandler(async (event) => {
      console.log('run completion target', event.id, event.summary);
    })
  }
});
