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

## Exit Criteria

- each caveated supported slice has explicit semantic fixtures
- replay/failover coverage exists where the slice can affect workflow-visible behavior
- product docs state the remaining caveats precisely enough that a design partner can evaluate them without code archaeology
