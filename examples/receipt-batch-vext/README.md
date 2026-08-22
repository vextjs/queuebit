# Receipt Batch vext Example

This directory is the canonical `docs/v01` target-contract entry for the first Queuebit BatchRun workflow. It is deliberately **code-first**: your Web/API code starts the Run, and your own service hosts construct and stop Queuebit Worker and Coordinator objects.

Read the source files in this order:

- `start-receipt-campaign.ts` accepts only `paidBefore`; `tenantId` comes from the authenticated server-side actor before it calls `runs.start`.
- `receipt-repository.ts` is the real database and side-effect boundary. Implement it with the repository or ORM already used by your application; do not put an array of orders in Queuebit runtime code.
- `queuebit.runtime.ts` maps the repository page into jobs, invokes the idempotent receipt sender, and records batch/final completion events.
- `receipt-services.ts` exposes `startReceiptWorker()` and `startReceiptCoordinator()`. Your systemd unit, container, vext bootstrap, or any other process manager decides how those functions are invoked and where it calls `stop()`.

Current status: this is not runnable evidence yet. The package scripts intentionally exit with a release-boundary message until the clean-environment example gate closes. It has no pretend database or hard-coded recipient data, so it cannot truthfully claim to send receipts without your `ReceiptRepository` implementation.

When the clean-environment example gate closes, this example must additionally provide a local Redis `>=7.2` container, an example business database, seeded paid-order/audit fixtures, safe example-owned cleanup, and verifiable end-to-end evidence. Until then, use it as the typed source-to-manual contract rather than proof that a local runtime is already provisioned.
