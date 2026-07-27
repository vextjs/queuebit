import { defineQueuebitConfig } from 'queuebit';

const redisPort = Number.parseInt(process.env.QUEUEBIT_EXAMPLE_REDIS_PORT ?? '6379', 10);

export default defineQueuebitConfig({
  namespace: 'queuebit:receipt-example',
  connection: {
    mode: 'direct',
    host: process.env.QUEUEBIT_EXAMPLE_REDIS_HOST ?? '127.0.0.1',
    port: Number.isInteger(redisPort) ? redisPort : 6379,
    database: Number.parseInt(process.env.QUEUEBIT_EXAMPLE_REDIS_DB ?? '0', 10),
    serverPolicy: { mode: 'warn' }
  },
  workerDefaults: {
    concurrency: 2,
    leaseMs: 30_000,
    renewIntervalMs: 10_000,
    pollIntervalMs: 1_000,
    drainTimeoutMs: 60_000
  },
  queues: {
    notification: {
      backpressure: {
        highWatermarkJobs: 1_000,
        lowWatermarkJobs: 800,
        highWatermarkBytes: 8_388_608,
        lowWatermarkBytes: 6_291_456
      }
    }
  },
  batchRuns: {
    'receipt-campaign': {
      version: 1,
      queue: 'notification',
      source: 'paid-orders',
      mapper: 'receipt-jobs',
      inputSchema: {
        type: 'object',
        required: ['tenantId', 'paidBefore'],
        additionalProperties: false,
        properties: {
          tenantId: { type: 'string', minLength: 1 },
          paidBefore: { type: 'string', format: 'date-time' }
        }
      },
      pageSize: 10,
      dispatch: {
        mode: 'sequential',
        intervalMs: 2_000,
        maxInFlightBatches: 1
      },
      completion: {
        batch: {
          handler: 'record-receipt-batch-result',
          attempts: 3,
          backoff: { type: 'fixed', delayMs: 1_000 }
        },
        run: {
          handler: 'record-receipt-run-result',
          attempts: 5,
          backoff: { type: 'exponential', delayMs: 1_000, maxDelayMs: 30_000 }
        }
      }
    }
  }
});
