# Temporal TS Replacement Priorities

This document turns the official Temporal TypeScript samples repo into a ranked Fabrik replacement backlog.

Current source snapshot:
- Source repo: `https://github.com/temporalio/samples-typescript.git`
- Source HEAD: `a8a6d95e43aead3038e48852c2fd758b416e7246`
- Local census summary: `/Users/bene/code/fabrik/target/blocker-census/temporal-samples-typescript/summary.md`
- Local census JSON: `/Users/bene/code/fabrik/target/blocker-census/temporal-samples-typescript/summary.json`

## Current State

- Samples analyzed: `48`
- Qualified with caveats: `16`
- Blocked: `32`
- Status counts:
  - `compatible_ready_not_deployed`: `16`
  - `incompatible_blocked`: `23`
  - `source_environment_blocked`: `9`

## What Moved

The latest compiler and packaging passes materially shifted the remaining replacement frontier:

- `unsupported_api` blocker occurrences dropped from `51` to `28`
- `unsupported_temporal_api` hard blocks dropped from `34` to `10`
- `unsupported_packaging_bootstrap` blocker occurrences dropped from `72` to `8`
- qualified official samples rose from `4` to `16`
- `log`, `workflowInfo`, `uuid4`, `ApplicationFailure`, `ParentClosePolicy`, and `ActivityCancellationType` are no longer explicit hard-block imports in the official sample census
- workflow-only workers no longer falsely block packaging, which moved `continue-as-new`, `nexus-hello`, and several other sample repos into the qualified set
- The remaining explicit unsupported-import census is now dominated by:
  - `patched`: `2`
  - `deprecatePatch`: `1`
  - `WorkflowInterceptors`: `1`
  - `ActivityFailure`: `1`
  - `inWorkflowContext`: `1`
  - `SearchAttributes`: `1`
  - `upsertSearchAttributes`: `1`
  - `Sinks`: `1`
  - `proxySinks`: `1`
  - `setWorkflowOptions`: `1`

This means broad import coverage is no longer the main hard-blocker class. The next replacement bottlenecks are:
- remaining compiler-subset gaps hidden behind generic `unsupported_api` compile failures
- a smaller set of still-unfinished worker-bootstrap edge cases
- payload/codec and interceptor-specific parity gaps

## Priority Order

1. Unsupported Temporal workflow/runtime APIs
- Hard-block findings: `10`
- Affected samples: `7`
- Blocker-category occurrences: `28`
- The explicit import backlog is now narrow. The bigger problem inside this category is generic compile-subset failure on otherwise-recognized APIs.
- Remaining explicit import frontier:
  - patching/versioning APIs: `patched`, `deprecatePatch`, `setWorkflowOptions`
  - failure/interceptor shapes: `ActivityFailure`, `WorkflowInterceptors`
  - visibility/sinks APIs: `SearchAttributes`, `upsertSearchAttributes`, `Sinks`, `proxySinks`
- Recommendation: stop chasing generic import coverage and instead burn down one real compile-subset cluster at a time, starting with the official samples still failing after import support landed: `batch-sliding-window`, `child-workflows`, `cron-workflows`, `timer-examples`.

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
- Affected sample: `search-attributes`
- Current alpha slice supports static start-time memo/search plus exact-match filtering.
- Recommendation: expand only if a target repo needs richer query or update-time search semantics.

6. Interceptors and middleware
- Hard-block findings: `1`
- Affected sample: `query-subscriptions`
- Recommendation: leave blocked until a real repo makes this urgent or until the unsupported API backlog has materially shrunk.

## Near-Term Execution

1. Burn down the next generic compile-subset cluster, not just import lists.
- Use the official samples still failing with `One or more workflows failed to compile into the currently supported Fabrik subset.` as the next queue.
- Best first targets:
  - `batch-sliding-window`
  - `custom-logger`
  - `nexus-hello`
  - `timer-examples`
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

- `continue-as-new`
- `custom-logger`
- `early-return`
- `empty`
- `env-config`
- `expense`
- `grpc-calls`
- `interceptors-opentelemetry`
- `nextjs-ecommerce-oneclick`
- `nexus-cancellation`
- `nexus-hello`
- `production`
- `schedules`
- `signals-queries`
- `timer-progress`
- `vscode-debugger`
