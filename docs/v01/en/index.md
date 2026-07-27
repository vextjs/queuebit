---
pageType: home
hero:
  name: queuebit
  text: Redis-backed Node job queue
  tagline: Install the module, pass Redis config, register one processor, and call jobs.add so a background Worker can run slow work.
  actions:
    - theme: brand
      text: Run one background job
      link: /quick-start.html
    - theme: alt
      text: See job usage
      link: /job-recipes.html
    - theme: alt
      text: Read the learning path
      link: /concepts.html
features:
  - title: The first job takes five steps
    details: Install, configure Redis, write a processor, call jobs.add, start a Worker, then inspect the job.
    link: /quick-start.html
  - title: Normal jobs use jobs.add
    details: Web/API submits one payload, and a Worker runs the matching processor. Do not start with BatchRun.
    link: /job-recipes.html
  - title: Common features open gradually
    details: Add retry, timeout, delay, idempotency, and cancel only when the task needs them.
    link: /concepts.html
  - title: Database batches are advanced
    details: Use BatchRun only when work must page a database, record batch/final completion, and recover progress.
    link: /batch-runs.html
  - title: Multiple Workers are for scale
    details: Start with one Worker, then add processes sharing Redis and the queue when throughput needs it.
    link: /distributed-workers.html
  - title: vext is just a host
    details: vext Web creates work; Workers still run as explicit background processes.
    link: /vext-integration.html
  - title: Operations stay out of the first path
    details: Production deployment, capacity, alerts, and recovery live under Production, not in the first integration path.
    link: /failure-runbooks.html
---

<span class="manual-label">Home · v0.1 user manual</span>

> **Release status:** This site describes the planned v0.1 usage model. Before installing, check the current npm package version and README release notes; if an example says a capability is not yet published, trust that message.
