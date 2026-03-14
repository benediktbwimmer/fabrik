# Temporal TS Trust Hardening

This document tracks the next trust-focused work after reaching broad official-sample migration compatibility.

Current compatibility snapshot:

- official Temporal TypeScript samples qualified: `47/48`
- intentionally blocked sample: `ai-sdk`

## Priority Order

1. `dsl-interpreter` helper-join semantics
- Current support is now trust-backed for the accepted semantics: persisted async-helper call frames plus deterministic depth-first sequential helper joins for recursive `Promise.all(map(asyncHelper(...)))`.
- Layer B now includes both compiler-shape coverage and a runtime semantic test for recursive helper joins.
- Remaining non-goal:
  - helper-level parallel equivalence is still not claimed; the accepted model is deterministic sequential traversal

2. Interceptor bridge semantics
- Current support allows static `interceptors.workflowModules` packaging and restart/replay-safe workflow execution when the workflow-visible state stays in the compiled workflow.
- This slice is now published as `supported_failover_validated` in the derived trust summary.
- Helper-side interceptor scaffolding and full Temporal interceptor runtime behavior remain caveated.
- Next trust task:
  - keep the supported bridge scoped to static workflow-module presence plus workflow-visible state/query semantics owned by the compiled workflow
  - document exactly which interceptor-visible behaviors are preserved and which are not

3. Sink bridge semantics
- Current support accepts `proxySinks()` declarations and fire-and-forget sink calls through a caveated bridge.
- This slice is now published as `supported_failover_validated` in the derived trust summary.
- The supported bridge is replay/restart validated when sink calls are workflow-invisible no-ops.
- Next trust task:
  - document the non-goals clearly
  - keep sink trust scoped to workflow-visible semantics, not side-effect delivery guarantees

4. Payload/data-converter adapter semantics
- Current support is broad enough for the official samples, and the adapter slice now has a restart/replay drill around static factory-based converter packaging, pre-normalize query state, restart preservation, and normalized activity output.
- Remaining non-goal:
  - keep full transport parity out of scope unless a real repo forces it

5. Async handlers and external workflow handles
- Current support is now trust-backed for the accepted subset: async signal/update handlers within the supported control-flow slice, plus external workflow handle signal/cancel against concrete workflow/run ids.
- This slice is now published as `supported_failover_validated` in the derived trust summary.
- Remaining non-goal:
  - do not claim broader external-handle parity outside the currently compiled subset

## Real Repo Qualification

Use the external qualification flow to turn this trust work into repo-level evidence:

```bash
scripts/run-external-repo-qualification.sh <repo-a> <repo-b> <repo-c>
```

The generated summaries live at:

- `target/external-repo-qualification/summary.json`
- `target/external-repo-qualification/summary.md`

When external repos are scarce, use the internal pressure-repo set for engineering coverage:

- definition doc: `docs/pressure-repos.md`
- runner: `scripts/run-pressure-repo-qualification.sh`
- current batch summary:
  - `target/pressure-repo-qualification/summary.json`
  - `target/pressure-repo-qualification/summary.md`
- current pressure-batch status: `5/5` qualified with caveats
- `temporal-versioning-upgrade-pressure` now has a passing internal mixed-build/restart/rollback drill in:
  - `target/alpha-drills/temporal-versioning-upgrade-pressure/versioning-pressure-mixed-build-drill-report.json`
  - current drill status: `passed`
  - old/new/rollback replay divergence counts: `0 / 0 / 0`
- `temporal-interceptor-pressure` now has a passing internal replay/restart drill for the supported interceptor bridge in:
  - `target/alpha-drills/temporal-interceptor-pressure/interceptor-pressure-drill-report.json`
  - current drill status: `passed`
  - replay divergence counts: post-restart `0`, post-complete `0`
- `temporal-sinks-pressure` now has a passing internal replay/restart drill for the supported sink no-op bridge in:
  - `target/alpha-drills/temporal-sinks-pressure/sinks-pressure-drill-report.json`
  - current drill status: `passed`
  - replay divergence counts: post-restart `0`, post-complete `0`
- `temporal-converter-trust-pressure` now has a passing internal replay/restart drill for the supported converter adapter slice in:
  - `target/alpha-drills/temporal-converter-trust-pressure/converter-pressure-drill-report.json`
  - current drill status: `passed`
  - replay divergence counts: post-restart `0`, post-complete `0`
  - pre-normalize query state now preserves copied `id/tags` fields before and after restart
- `temporal-async-external-pressure` now has a passing internal replay/restart drill for async handler state and external workflow handle control in:
  - `target/alpha-drills/temporal-async-external-pressure/async-external-pressure-drill-report.json`
  - current drill status: `passed`
  - replay divergence counts: target/controller post-restart `0 / 0`, post-complete `0 / 0`
- `temporal-monorepo-multiworker-pressure` now has a passing internal replay/restart drill for the multi-worker monorepo packaging and restore path in:
  - `target/alpha-drills/temporal-monorepo-multiworker-pressure/monorepo-multiworker-pressure-drill-report.json`
  - current drill status: `passed`
  - orders/report replay divergence counts: post-restart `0 / 0`, post-complete `0 / 0`

Current app-style batch signal:

- `worker-versioning-replay-demo` qualifies as `qualified_with_caveats` in:
  - `target/external-repo-qualification/app-batch/worker-versioning-replay-demo/migration-report.json`
- `worker-versioning-replay-demo` now has a passing non-sample replay/restart/build-routing drill in:
  - `target/alpha-drills/worker-versioning-replay-demo/worker-versioning-replay-demo-drill-report.json`
  - current drill status: `passed`
  - replay divergence counts: pre-restart `0`, post-restart `0`, post-complete `0`
- `temporal-worker-versioning-typescript` is blocked by a single compile failure:
  - top-level side effects in `src/commit-b/workflows.ts`
  - report path: `target/external-repo-qualification/app-batch/temporal-worker-versioning-typescript/migration-report.json`
- Recommendation:
  - use `worker-versioning-replay-demo` as the first completed non-sample trust proof
  - treat `temporal-worker-versioning-typescript` as a narrow follow-up parity bug, not a design-partner candidate yet
- Replay/restart/build-routing drill for the first non-sample target:
  - `scripts/run-worker-versioning-replay-demo-drill.sh`
  - output bundle: `target/alpha-drills/worker-versioning-replay-demo`

## Exit Criteria

- each caveated supported slice has explicit semantic fixtures
- replay/failover coverage exists where the slice can affect workflow-visible behavior
- product docs state the remaining caveats precisely enough that a design partner can evaluate them without code archaeology
