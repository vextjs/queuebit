import {
  createQueuebitClient,
  createQueuebitRuntimeProcessor,
  type QueuebitClient,
  type QueuebitClientCoordinatorRunnerOptions,
  type QueuebitClientWorkerOptions,
  type QueuebitCoordinatorRunner,
  type QueuebitCoordinatorRunnerDrainOptions,
  type QueuebitWorker,
  type QueuebitWorkerDrainOptions
} from 'queuebit';
import queuebitConfig from './queuebit.config.js';
import { createReceiptRuntime } from './queuebit.runtime.js';
import type { ReceiptRepository } from './receipt-repository.js';

export interface ReceiptWorkerService {
  client: QueuebitClient;
  worker: QueuebitWorker;
  stop(options?: QueuebitWorkerDrainOptions): Promise<void>;
}

export interface ReceiptCoordinatorService {
  client: QueuebitClient;
  coordinator: QueuebitCoordinatorRunner;
  stop(options?: QueuebitCoordinatorRunnerDrainOptions): Promise<void>;
}

/**
 * Call this from the service process that owns receipt delivery. The host is
 * free to use systemd, Kubernetes, vext, or any other process manager.
 */
export async function startReceiptWorker(
  repository: ReceiptRepository,
  options: QueuebitClientWorkerOptions = {}
): Promise<ReceiptWorkerService> {
  const client = await createQueuebitClient({ config: queuebitConfig });
  const runtime = createReceiptRuntime(repository);
  const worker = client.createWorker(
    'notification',
    createQueuebitRuntimeProcessor(runtime),
    options
  );
  worker.start();
  return { client, worker, stop: closeOptions => client.close(closeOptions) };
}

/**
 * Call this only for the service process that advances receipt BatchRuns.
 */
export async function startReceiptCoordinator(
  repository: ReceiptRepository,
  options: QueuebitClientCoordinatorRunnerOptions = {}
): Promise<ReceiptCoordinatorService> {
  const client = await createQueuebitClient({ config: queuebitConfig });
  const coordinator = client.createCoordinatorRunner(createReceiptRuntime(repository), options);
  coordinator.start();
  return { client, coordinator, stop: closeOptions => client.close(closeOptions) };
}
