import type { QueuebitClient } from 'queuebit';

export interface AuthenticatedReceiptActor {
  tenantId: string;
}

export interface StartReceiptCampaignRequest {
  paidBefore: string;
}

/**
 * A route calls this after authentication. tenantId is derived from the server
 * identity rather than accepted from the public request body.
 */
export async function startReceiptCampaign(
  queuebit: QueuebitClient,
  actor: AuthenticatedReceiptActor,
  request: StartReceiptCampaignRequest
) {
  return queuebit.runs.start('receipt-campaign', {
    input: { tenantId: actor.tenantId, paidBefore: request.paidBefore },
    idempotencyKey: `receipt:${actor.tenantId}:${request.paidBefore}`
  });
}
