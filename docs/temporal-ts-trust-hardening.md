# Temporal TS Trust Hardening

This document tracks the next trust-focused work after reaching broad official-sample migration compatibility.

Current compatibility snapshot:

- official Temporal TypeScript samples qualified: `47/48`
- intentionally blocked sample: `ai-sdk`

## Priority Order

1. `dsl-interpreter` helper-join semantics
- Current migration support uses persisted async-helper call frames plus a sequential helper-join lowering for recursive `Promise.all(map(asyncHelper(...)))`.
- This is sufficient for migration/package qualification.
- It is not yet trust-backed helper-level parallel equivalence.
- Next trust task:
  - add semantic fixtures that compare helper-join outputs against the expected DSL branch semantics
  - decide whether sequential helper-join is an accepted caveat or whether helper-parallel equivalence is required

2. Interceptor bridge semantics
- Current support allows static `interceptors.workflowModules` packaging and helper-side interceptor scaffolding.
- Runtime interceptor behavior is still caveated.
- Next trust task:
  - add replay/failover fixtures for the supported bridge
  - document exactly which interceptor-visible behaviors are preserved and which are not

3. Sink bridge semantics
- Current support accepts `proxySinks()` declarations and fire-and-forget sink calls through a caveated bridge.
- Next trust task:
  - add semantic fixtures proving workflow-visible behavior is unchanged when sinks are ignored
  - document the non-goals clearly

4. Payload/data-converter adapter semantics
- Current support is broad enough for the official samples, but trust evidence is still bounded to the adapter slice.
- Next trust task:
  - add replay/failover coverage for static factory helpers and static `payloadConverterPath` modules
  - keep full transport parity out of scope unless a real repo forces it

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
