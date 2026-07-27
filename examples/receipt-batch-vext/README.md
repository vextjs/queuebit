# Receipt Batch vext Example

This directory is the canonical `docs/v01` target-contract entry for the first Queuebit user workflow.

Current status: this is not runnable evidence yet. The package scripts intentionally exit with a release-boundary message until the clean-environment example gate closes. The config and runtime files are kept here so the user manual can be checked against real exported Queuebit helper names and config fields.

When the release gate closes, this example must provide:

- A local Redis `>=7.2` container and an example business database.
- Seeded paid orders with stable tenant and receipt audit fixtures.
- A vext route that authenticates the user, derives `tenantId` server-side, and calls `runs.start`.
- One Coordinator and two Worker roles using `queuebit.config.ts` and `queuebit.runtime.ts`.
- Per-batch and final completion audit commands.
- Safe cleanup limited to example-owned containers, data, and volumes.

Until then, use this directory as the source-to-manual contract, not as proof that the quick-start can already run end to end.
