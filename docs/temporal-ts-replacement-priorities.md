# Temporal TS Replacement Priorities

This document turns the official Temporal TypeScript samples repo into a ranked Fabrik replacement backlog.

Current source snapshot:
- Source repo: `https://github.com/temporalio/samples-typescript.git`
- Source HEAD: `a8a6d95e43aead3038e48852c2fd758b416e7246`
- Local census summary: `/Users/bene/code/fabrik/target/blocker-census/temporal-samples-typescript/summary.md`
- Local census JSON: `/Users/bene/code/fabrik/target/blocker-census/temporal-samples-typescript/summary.json`

## Current State

- Samples analyzed: `48`
- Qualified with caveats: `47`
- Blocked: `1`
- Status counts:
  - `compatible_ready_not_deployed`: `47`
  - `incompatible_blocked`: `1`

## What Moved

The latest compiler, runtime, and packaging passes materially shifted the remaining replacement frontier:

- `unsupported_api` blocker occurrences dropped from `51` to `3`
- `unsupported_temporal_api` hard blocks dropped from `34` to `4`
- `unsupported_packaging_bootstrap` blocker occurrences dropped from `72` to `1`
- qualified official samples rose from `4` to `47`
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
- `encryption` now qualifies after static zero-argument `dataConverter` factory helpers moved into the supported packaging adapter slice
- `query-subscriptions` now qualifies after static workflow-module interceptor bootstraps moved onto the caveated packaging path and helper-side interceptor scaffolding (`setHandler`, `enablePatches`, `Object.defineProperty`) stopped hard-blocking compilation
- `dsl-interpreter` now qualifies after adding persisted async-helper call frames plus a sequential helper-join lowering for recursive `Promise.all(map(asyncHelper(...)))` patterns; this is explicitly a caveated adapter path, not trust-backed helper-level parallel semantics
- the first app-style external repo batch produced one strong non-sample candidate:
  - `worker-versioning-replay-demo` qualifies in `target/external-repo-qualification/app-batch/worker-versioning-replay-demo/migration-report.json`
  - `worker-versioning-replay-demo` now also has a passing replay/restart/rollback drill in `target/alpha-drills/worker-versioning-replay-demo/worker-versioning-replay-demo-drill-report.json`
  - `temporal-worker-versioning-typescript` is blocked only by top-level side effects in `src/commit-b/workflows.ts`
- an internal pressure-repo set now exists for continued engineering coverage when external repos are limited:
  - definition doc: `docs/pressure-repos.md`
  - batch summary: `target/pressure-repo-qualification/summary.json`
  - current status: `5/5` qualified with caveats
  - `temporal-versioning-upgrade-pressure` now also has a passing mixed-build/restart/rollback drill in `target/alpha-drills/temporal-versioning-upgrade-pressure/versioning-pressure-mixed-build-drill-report.json`
- The remaining explicit unsupported-import census is now dominated by:
  - `WorkflowInterceptors`: `1`
  - `Sinks`: `1`
  - `proxySinks`: `1`

This means broad import coverage is no longer the main hard-blocker class. The next replacement bottlenecks are:
- external non-Temporal async SDK execution inside workflow code
- trust-bounded caveat cleanup around the adapter-backed interceptor/sink slice
- deciding whether the remaining `dsl-interpreter` caveat is acceptable or worth upgrading into full trust-backed helper-parallel semantics
- scaling the now-proven trust drill from one non-sample repo to a larger real-repo set

## Priority Order

1. Unsupported Temporal workflow/runtime APIs
- Hard-block findings: `1`
- Affected samples: `1`
- Blocker-category occurrences: `3`
- The explicit import backlog is now narrow. The bigger problem inside this category is no longer generic syntax; it is one out-of-thesis external SDK workflow pattern plus one remaining trust caveat around recursive helper joins.
- Remaining frontier:
  - broader external-library async orchestration in `ai-sdk`
- Recommendation: treat `ai-sdk` as a separate product decision because it embeds non-Temporal AI SDK execution directly in workflow code.

2. Recursive async helper trust upgrade
- Hard-block findings: `0`
- Affected samples: `0`
- Qualified sample with caveat: `dsl-interpreter`
- Current support uses persisted async-helper call frames and a sequential helper-join lowering for recursive `Promise.all(map(asyncHelper(...)))` patterns. That is enough for migration/package qualification, but it is not yet trust-backed helper-parallel equivalence.
- Recommendation: only upgrade this further if a real repo needs trusted helper-level parallel semantics.

3. External SDK execution inside workflows
- Hard-block findings: `1`
- Affected samples: `1`
- Remaining sample: `ai-sdk`
- The blocking workflows directly `await generateText(...)`, `await mcpClient.tools()`, and other external AI SDK orchestration inside workflow code. That is not a normal Temporal replacement target unless Fabrik chooses to support arbitrary external async SDK execution inside workflows.
- Recommendation: treat this as a thesis decision, not a routine parity slice.

4. Caveated adapter cleanup
- Hard-block findings: `0`
- Affected samples: `0`
- Payload/data-converter and interceptor/sink slices now qualify in the official sample set, but some of them are still caveated adapter paths rather than trust-backed parity.
- Recommendation: keep these slices stable, add trust evidence only when a real repo requires it, and avoid reopening them while the last blocked samples remain.

## Near-Term Execution

1. Make an explicit product decision on `ai-sdk`.
- Either:
  - keep it blocked as out-of-thesis external SDK execution inside workflow code
  - or start a separate project for arbitrary external async library execution in workflows
- Success criterion: stop treating `ai-sdk` as if it were just one more compiler-sugar gap.

2. Decide whether to leave `dsl-interpreter` caveated or invest in a trust upgrade.
- Success criterion: be explicit about whether sequential helper-join semantics are acceptable for the replacement claim.

3. Re-run the blocker census after every parity slice.
- Use `/Users/bene/code/fabrik/scripts/run-temporal-ts-blocker-census.sh`
- Treat the official samples repo as a standing external regression set.

4. Keep parity work gated by trust.
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
