# Idempotency Patterns

<!-- queuebit-v01-legacy-doc -->
> [!WARNING]
> **Archived and no longer maintained.** The current v0.1 final-user manual is [`docs/v01/en`](../v01/en/index.md). This page remains for historical context only; do not use its APIs, commands, configuration, or examples for new integrations or implementation.

<span class="manual-label">At-least-once safety guide</span>

queuebit keeps jobs recoverable, so Worker crashes, lost acknowledgements, lease expiry, or Redis faults can execute one business action again. `opts.idempotencyKey` deduplicates enqueue only; it cannot undo or deduplicate email, payment, webhook, or database side effects.

<span id="s12-side-effect-idempotency"></span>
## S12 Protect an external side effect

Choose a stable key that identifies the business action:

| Action | Recommended business key | Avoid |
|--------|--------------------------|-------|
| Order receipt | `receipt:<orderId>` | Random UUID or attempt number |
| Monthly invoice | `invoice:<accountId>:<month>` | Current timestamp |
| Webhook | `webhook:<eventId>:<subscriberId>` | Worker identity |
| Refund | `refund:<refundRequestId>` | Job ID as the only business identity |

Every attempt of one business action uses the same key. Only a genuinely new business action gets a new key.

## Pattern 1: downstream idempotency key

When a payment, messaging, or email provider accepts an idempotency key, pass the business key directly:

```ts
async function sendReceipt(job, ctx) {
  const key = `receipt:${job.data.orderId}`;

  const result = await emailProvider.send({
    to: job.data.recipient,
    templateId: job.data.templateId,
    idempotencyKey: key,
    signal: ctx.signal
  });

  await db.notifications.recordProviderResult({
    businessKey: key,
    providerRequestId: result.requestId
  });
}
```

A repeated request must return the existing result instead of sending again. Log the business key and provider request ID, not only the job ID.

## Pattern 2: database unique constraint and state machine

When the provider has no idempotency key, claim the effect in the business database:

```ts
async function sendReceipt(job, ctx) {
  const businessKey = `receipt:${job.data.orderId}`;
  const claim = await db.notificationEffects.tryBegin({ businessKey });

  if (claim.status === 'completed') return;
  if (claim.status === 'processing' && !claim.isExpired) {
    throw new Error('Effect is already processing');
  }

  const response = await emailProvider.send({
    to: job.data.recipient,
    templateId: job.data.templateId,
    signal: ctx.signal
  });

  await db.notificationEffects.complete({
    businessKey,
    providerRequestId: response.requestId
  });
}
```

Enforce a unique constraint on `businessKey`. Keep `processing/completed/failed`, owner, expiry, and downstream request ID. After `processing` expires, query the downstream result before resending.

## Pattern 3: business transaction plus outbox

For “update a database and publish another event,” update business state and insert an outbox record in one database transaction. A separate publisher sends the outbox. Retry can read the committed result through a unique business key, avoiding a successful database update with a lost event.

```ts
await db.transaction(async (tx) => {
  const changed = await tx.orders.markReceiptPrepared(job.data.orderId);
  if (!changed) return;

  await tx.outbox.insertUnique({
    key: `receipt-ready:${job.data.orderId}`,
    topic: 'receipt-ready',
    payload: { orderId: job.data.orderId }
  });
});
```

## Decide after a timeout

1. Treat an aborted `ctx.signal` as uncertain, not proof that nothing happened.
2. Query local state or the downstream lookup API with the business key.
3. Return when completed; retry only when the action definitely never started.
4. Keep an operator-review state when the outcome cannot be queried instead of infinitely resending high-risk effects.

## Anti-patterns

| Anti-pattern | Risk | Replacement |
|--------------|------|-------------|
| Generate a random key per attempt | Every request looks new downstream | Stable business key |
| Send first, then unconditionally insert “sent” | Insert failure causes duplicate send on retry | Provider key or state machine |
| Rely only on queue `idempotencyKey` | It cannot protect the ack window | Guard effects inside the handler |
| Catch an error and return | queuebit records false success | Log and rethrow |
| Edit Redis to mark a job completed | Queue and business truth drift permanently | Public API plus business compensation |

## Verification checklist

- Execute the same job twice and observe one business result.
- Terminate the Worker after downstream success but before ack; recovery still produces one effect.
- After `HandlerTimeoutError`, classify completed, not started, or uncertain by business key.
- Logs correlate every job ID, attempt, and downstream request ID by business key.
- A corrected submission uses a revision key and keeps an audit relation to the old record.
