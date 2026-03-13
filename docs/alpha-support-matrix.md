# Alpha Support Matrix

This document defines the current design-partner alpha boundary for Temporal TypeScript migration.

The alpha claim is:

- a trusted Temporal TypeScript subset
- static worker bootstrap packaging
- replay and rollout evidence at the compiled artifact boundary
- explicit blocking of unsupported features instead of silent degradation

## Qualification Classes

- `qualified`: fits the current alpha subset and passes the available migration gates
- `qualified_with_caveats`: fits the subset, but more trust or deployment evidence is still needed
- `blocked`: outside the current alpha subset or failed a trust/operational gate

## Supported in Alpha

- `proxyActivities`
- static activity options and retries
- signals, queries, and updates within the compiler subset
- async handlers within the compiler subset
- conditions and timer waits with static timeout literals
- child workflows within the current compiler subset
- external workflow handles within the current compiler subset
- cancellation scopes within the supported Temporal-style subset
- continue-as-new
- version markers and workflow evolution
- worker build registration and compatibility routing
- static `Worker.create({ ... })` bootstrap packaging
- start-time `memo` objects with static-evaluable top-level primitive values
- start-time `searchAttributes` objects with static-evaluable top-level primitive, `Date`, or primitive/`Date`-array values
- workflow-side `upsertSearchAttributes(...)` within the supported compiler subset
- exact-match visibility filtering over the supported `memo` and `searchAttributes` slice
- static default-compatible `dataConverter` declarations on `Worker.create({ ... })`
- static default-compatible `payloadConverterPath` modules on `Worker.create({ ... })`
- mixed-build deploy, restart, replay, and rollback evidence for the supported payload/data-converter adapter slice
- workflow task queue preservation through replay, snapshot restore, and owner handoff for the supported alpha slice

## Blocked in Alpha

- custom payload/data converter customization beyond the default-compatible adapter subset
- worker or workflow interceptors
- dynamic worker bootstrap shapes
- arbitrary workflow inspection APIs outside the supported subset
- dynamic or runtime-shaped memo/search attribute writes outside the supported `upsertSearchAttributes(...)` subset
- unsupported visibility queries outside the exact-match alpha slice

## Search/Memo Boundary

The current alpha slice is intentionally narrow:

- start-time `memo` is supported only for static-evaluable top-level objects with primitive values
- start-time `searchAttributes` is supported only for static-evaluable top-level primitive, `Date`, or primitive/`Date`-array values
- workflow-side `upsertSearchAttributes(...)` is supported only when the patch object stays inside the compiled expression subset
- query/list filtering is supported only for exact-match key/value checks
- replay and handoff evidence for the alpha slice require the workflow task queue to remain stable across restore
- anything dynamic, computed, nested, or outside those operators remains blocked by the analyzer

This boundary is intentional. The alpha path prefers explicit blocking over partial support.
