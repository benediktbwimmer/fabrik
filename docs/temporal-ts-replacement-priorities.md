# Temporal TS Replacement Priorities

This document turns the official Temporal TypeScript samples repo into a ranked Fabrik replacement backlog.

Current source snapshot:
- Source repo: `https://github.com/temporalio/samples-typescript.git`
- Source HEAD: `a8a6d95e43aead3038e48852c2fd758b416e7246`
- Local census summary: `/Users/bene/code/fabrik/target/blocker-census/temporal-samples-typescript/summary.md`
- Local census JSON: `/Users/bene/code/fabrik/target/blocker-census/temporal-samples-typescript/summary.json`

## Current State

- Samples analyzed: `48`
- Qualified with caveats: `25`
- Blocked: `23`
- Status counts:
  - `compatible_ready_not_deployed`: `25`
  - `incompatible_blocked`: `14`
  - `source_environment_blocked`: `9`

## What Moved

The latest compiler, runtime, and packaging passes materially shifted the remaining replacement frontier:

- `unsupported_api` blocker occurrences dropped from `51` to `19`
- `unsupported_temporal_api` hard blocks dropped from `34` to `4`
- `unsupported_packaging_bootstrap` blocker occurrences dropped from `72` to `8`
- qualified official samples rose from `4` to `23`
- `log`, `workflowInfo`, `uuid4`, `ApplicationFailure`, `ActivityFailure`, `ParentClosePolicy`, `ActivityCancellationType`, `patched`, `deprecatePatch`, `setWorkflowOptions`, `SearchAttributes`, and `upsertSearchAttributes` are no longer explicit hard-block imports in the official sample frontier
- workflow-only workers no longer falsely block packaging, which moved `continue-as-new`, `nexus-hello`, and several other sample repos into the qualified set
- `batch-sliding-window`, `child-workflows`, `cron-workflows`, `patching-api`, `saga`, `search-attributes`, and `worker-versioning` now compile cleanly in the refreshed official census
- post-census direct migration checks also moved `state` and `nestjs-exchange-rates` onto the qualified side after the workflow-local state, cancellation-request, imported-definition, and `Date` subset passes; the next full census should reflect that
- the latest compiler/runtime config pass also moved direct workflow compilation for `fetch-esm` and `snippets`:
  - `fetch-esm/src/workflows.ts:exampleFetch` now compiles after adding richer proxy-activity retry option support
  - `snippets/src/workflows.ts:example` now compiles after adding deterministic `Math.random()` lowering
- `timer-examples` moved off the old property-assignment blocker: `timer.deadline = deadline` no longer fails first, and the remaining frontier there is now detached async handle / promise-race semantics
- imported helper factories used only inside Temporal `ReturnType<typeof ...>` annotations no longer poison workflow compilation, and deferred activity thunks now cover the real `saga` compensation pattern
- The remaining explicit unsupported-import census is now dominated by:
  - `WorkflowInterceptors`: `1`
  - `inWorkflowContext`: `1`
  - `Sinks`: `1`
  - `proxySinks`: `1`

This means broad import coverage is no longer the main hard-blocker class. The next replacement bottlenecks are:
- remaining compiler-subset gaps hidden behind generic `unsupported_api` compile failures, especially detached async handles and `Promise.race(...)` workflow semantics
- a smaller set of still-unfinished worker-bootstrap edge cases
- payload/codec and interceptor-specific parity gaps

## Priority Order

1. Unsupported Temporal workflow/runtime APIs
- Hard-block findings: `4`
- Affected samples: `3`
- Blocker-category occurrences: `19`
- The explicit import backlog is now narrow. The bigger problem inside this category is generic compile-subset failure on otherwise-recognized APIs.
- Remaining explicit import frontier:
  - interceptor shapes: `WorkflowInterceptors`
  - remaining workflow APIs: `inWorkflowContext`
  - sinks APIs: `Sinks`, `proxySinks`
- Recommendation: stop chasing generic import coverage and instead burn down one real compile-subset cluster at a time. The current highest-signal clusters are `activities-cancellation-heartbeating` (broader cancellation scope and promise composition), `timer-examples` (detached promise/race patterns plus promise-like timers), and the remaining environment/bootstrap outliers.

2. Remaining worker bootstrap edge cases
- Hard-block findings: `9`
- Affected samples: `6`
- Blocker-category occurrences: `8`
- The broad static-evaluation and packaging passes removed most of this class, but a few shapes remain.
- Remaining hotspots:
  - dynamic task queues from runtime config
  - dynamic activities registration factories
  - multiple-worker / unique-queue patterns
  - bootstrap wrapped in runtime helper calls
- Recommendation: only implement the next slice if it unblocks a real repo or a high-value official sample cluster.

3. Broader payload/data-converter parity
- Hard-block findings: `8`
- Affected samples: `3`
- Main samples: `ejson`, `encryption`, `sinks`
- Current alpha support only covers the default-compatible subset and static default-compatible `payloadConverterPath`.
- Recommendation: scope one exact next slice from these samples, not generic converter parity.

4. Source-environment and tooling blockers
- `source_environment_bootstrap`: `5`
- `unexpected_tool_failure`: `4`
- These are not the product bottleneck, but they do affect external-source census quality.
- Recommendation: keep the census resilient, but do not prioritize product work here unless a design-partner repo has the same structure.

5. Visibility/search expansion
- Hard-block findings: `2`
- Affected sample: none in the latest direct checks; `search-attributes` now qualifies outside the last full census snapshot
- Current alpha slice supports static start-time memo/search plus exact-match filtering, plus workflow-side `upsertSearchAttributes` within the compiler subset.
- Recommendation: expand only if a target repo needs richer query or update-time search semantics beyond the current alpha slice.

6. Interceptors and middleware
- Hard-block findings: `1`
- Affected sample: `query-subscriptions`
- Recommendation: leave blocked until a real repo makes this urgent or until the unsupported API backlog has materially shrunk.

7. Workflow-local state and date subsets
- Status: landed in direct official-sample checks after the latest census refresh
- Affected samples moved by this pass:
  - `state`
  - `nestjs-exchange-rates`
- Current frontier: promise composition around timers, cancellation, and detached activity handles rather than plain local state mutation.
- Recommendation: build on this by adding detached promise/race semantics instead of widening random syntax support.

## Near-Term Execution

1. Burn down the next generic compile-subset cluster, not just import lists.
- Use the official samples still failing with `One or more workflows failed to compile into the currently supported Fabrik subset.` as the next queue.
- Best first targets:
  - `timer-examples`
  - `activities-cancellation-heartbeating`
  - `query-subscriptions`
  - `sinks`
- Success criterion: reduce `unsupported_api` occurrences again without increasing trust debt.

2. Re-run the blocker census after every parity slice.
- Use `/Users/bene/code/fabrik/scripts/run-temporal-ts-blocker-census.sh`
- Treat the official samples repo as a standing external regression set.

3. Keep parity work gated by trust.
- Every new supported slice needs:
  - analyzer support
  - CLI packaging or runtime support
  - focused tests
  - replay/failover or mixed-build evidence if the feature affects trust semantics

## Current Qualified Official Samples

- `batch-sliding-window`
- `child-workflows`
- `continue-as-new`
- `cron-workflows`
- `custom-logger`
- `early-return`
- `empty`
- `env-config`
- `expense`
- `grpc-calls`
- `interceptors-opentelemetry`
- `nestjs-exchange-rates` (direct rerun after the latest date/import-definition pass)
- `nextjs-ecommerce-oneclick`
- `nexus-cancellation`
- `nexus-hello`
- `patching-api`
- `production`
- `saga`
- `schedules`
- `search-attributes`
- `signals-queries`
- `state` (direct rerun after the latest workflow-local state and cancellation-request pass)
- `timer-progress`
- `vscode-debugger`
- `worker-versioning`
