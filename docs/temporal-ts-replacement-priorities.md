# Temporal TS Replacement Priorities

This document turns the official Temporal TypeScript samples repo into a ranked Fabrik replacement backlog.

Current source snapshot:
- Source repo: `https://github.com/temporalio/samples-typescript.git`
- Source HEAD: `a8a6d95e43aead3038e48852c2fd758b416e7246`
- Local census summary: `/Users/bene/code/fabrik/target/blocker-census/temporal-samples-typescript/summary.md`
- Local census JSON: `/Users/bene/code/fabrik/target/blocker-census/temporal-samples-typescript/summary.json`

## Current State

- Samples analyzed: `48`
- Qualified with caveats: `44`
- Blocked: `4`
- Status counts:
  - `compatible_ready_not_deployed`: `44`
  - `incompatible_blocked`: `4`

## What Moved

The latest compiler, runtime, and packaging passes materially shifted the remaining replacement frontier:

- `unsupported_api` blocker occurrences dropped from `51` to `8`
- `unsupported_temporal_api` hard blocks dropped from `34` to `4`
- `unsupported_packaging_bootstrap` blocker occurrences dropped from `72` to `1`
- qualified official samples rose from `4` to `43`
- `log`, `workflowInfo`, `uuid4`, `ApplicationFailure`, `ActivityFailure`, `ParentClosePolicy`, `ActivityCancellationType`, `patched`, `deprecatePatch`, `setWorkflowOptions`, `SearchAttributes`, and `upsertSearchAttributes` are no longer explicit hard-block imports in the official sample frontier
- workflow-only workers no longer falsely block packaging, which moved `continue-as-new`, `nexus-hello`, and several other sample repos into the qualified set
- `batch-sliding-window`, `child-workflows`, `cron-workflows`, `patching-api`, `saga`, `search-attributes`, and `worker-versioning` now compile cleanly in the refreshed official census
- post-census direct migration checks also moved `state` and `nestjs-exchange-rates` onto the qualified side after the workflow-local state, cancellation-request, imported-definition, and `Date` subset passes; the next full census should reflect that
- the latest compiler/runtime config pass also moved direct workflow compilation for `fetch-esm` and `snippets`:
  - `fetch-esm/src/workflows.ts:exampleFetch` now compiles after adding richer proxy-activity retry option support
  - `snippets/src/workflows.ts:example` now compiles after adding deterministic `Math.random()` lowering
- `timer-examples` now qualifies end to end after the detached async-handle, promise-race, and PromiseLike timer-object passes
- `sleep-for-days` now qualifies in a direct rerun after adding `Promise.race([sleep(...), condition(...)])` lowering onto timed condition waits and fixing relative-path relaxed transpile fallback during worker packaging
- imported helper factories used only inside Temporal `ReturnType<typeof ...>` annotations no longer poison workflow compilation, and deferred activity thunks now cover the real `saga` compensation pattern
- `scratchpad` now qualifies after aligning analyzer/bootstrap support with the already-supported `inWorkflowContext` guard and `__filename` self-file worker shape
- `hello-world-mtls` now qualifies with deployment caveats after dynamic worker task queues stopped hard-blocking packaging-only migration qualification
- `activities-cancellation-heartbeating` now qualifies after the broader cancellation-scope, callback-promise, and multi-handle race work
- `activities-dependency-injection` now qualifies after packaging learned how to materialize safe imported activity-factory calls with static bootstrap arguments
- `activities-examples` now qualifies after test-only worker bootstrap failures stopped blocking repos that already have a production worker entrypoint
- `worker-specific-task-queues` now qualifies after worker-local runtime helpers learned how to reconstruct dynamic activity-factory setup and multi-worker packaging stopped colliding on duplicate file stems
- `mutex` now qualifies after dynamic signal registration and dynamic condition timeout lowering covered its lock-release workflow shape
- `eager-workflow-start` now qualifies after `proxyLocalActivities` moved onto the same supported proxy-activity path as `proxyActivities`
- `ejson` and `protobufs` now qualify after static `payloadConverterPath` modules broadened from the default-compatible-only slice to a packageable static path-based adapter slice
- `sinks` now qualifies after `proxySinks()` declarations and fire-and-forget sink calls moved onto a caveated no-op migration bridge and worker `sinks` configuration stopped hard-blocking packaging
- The remaining explicit unsupported-import census is now dominated by:
  - `WorkflowInterceptors`: `1`
  - `Sinks`: `1`
  - `proxySinks`: `1`

This means broad import coverage is no longer the main hard-blocker class. The next replacement bottlenecks are:
- sinks/interceptor-specific parity
- broader payload/codec parity
- a very small set of still-unfinished worker-bootstrap edge cases
- payload/codec and interceptor-specific parity gaps

## Priority Order

1. Unsupported Temporal workflow/runtime APIs
- Hard-block findings: `4`
- Affected samples: `4`
- Blocker-category occurrences: `8`
- The explicit import backlog is now narrow. The bigger problem inside this category is no longer generic syntax; it is the dedicated interceptor/sink surface and one remaining recursive orchestration compiler gap.
- Remaining frontier:
  - interceptor shapes: `WorkflowInterceptors`
  - recursive orchestration compile gap in `dsl-interpreter`
  - broader external-library async orchestration in `ai-sdk`
- Recommendation: focus this category on `dsl-interpreter`, `query-subscriptions`, and `ai-sdk` rather than generic workflow syntax.

2. Broader payload/data-converter parity
- Hard-block findings: `2`
- Affected samples: `1`
- Main sample: `encryption`
- Current alpha support covers the default-compatible subset plus static `payloadConverterPath` modules that can be packaged into worker bootstrap artifacts. Codec-bearing async `dataConverter` objects are still blocked.
- Recommendation: scope the next payload slice specifically around static `payloadCodecs` / codec-loader support if `encryption` is worth the added trust surface.

3. Remaining worker bootstrap edge cases
- Hard-block findings: `1`
- Affected samples: `1`
- Blocker-category occurrences: `1`
- The broad static-evaluation and packaging passes removed most of this class, but a few shapes remain.
- Remaining hotspots:
  - activity registration factories that require runtime dependencies
  - bootstrap wrapped in runtime helper calls
- Recommendation: only implement the next slice if it unblocks a real repo or a high-value official sample cluster.

4. Visibility/search expansion
- Hard-block findings: `2`
- Affected sample: none in the latest direct checks; `search-attributes` now qualifies outside the last full census snapshot
- Current alpha slice supports static start-time memo/search plus exact-match filtering, plus workflow-side `upsertSearchAttributes` within the compiler subset.
- Recommendation: expand only if a target repo needs richer query or update-time search semantics beyond the current alpha slice.

5. Interceptors and middleware
- Hard-block findings: `1`
- Affected sample: `query-subscriptions`
- Recommendation: leave blocked until a real repo makes this urgent or until the unsupported API backlog has materially shrunk.

6. Workflow-local state and date subsets
- Status: landed in direct official-sample checks after the latest census refresh
- Affected samples moved by this pass:
  - `state`
  - `nestjs-exchange-rates`
- Current frontier: payload/data conversion and a few remaining integration hooks rather than plain local state mutation.
- Recommendation: keep this area stable and spend time on the remaining blocked sample classes instead.

## Near-Term Execution

1. Burn down the next blocked sample cluster, not generic syntax lists.
- Best first targets:
  - `dsl-interpreter`
  - `query-subscriptions`
  - `encryption`
- Success criterion: reduce the remaining blocked sample count below `4` without widening trust debt.

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

- `activities-cancellation-heartbeating`
- `activities-dependency-injection`
- `activities-examples`
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
- `timer-examples`
- `timer-progress`
- `vscode-debugger`
- `worker-specific-task-queues`
- `worker-versioning`
