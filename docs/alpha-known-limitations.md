# Alpha Known Limitations

This document lists the highest-signal limitations for the current design-partner alpha migration path.

## Current Hard Limits

- Search attributes and memo are only supported in the alpha slice for static start-time values, the narrow compiled `upsertSearchAttributes(...)` subset, and exact-match visibility filters.
- Static-evaluable start-time memo/search-attribute shapes are supported, but broader dynamic runtime-shaped visibility writes are still blocked.
- Payload/data converter support is limited to static default-compatible `dataConverter` declarations, static zero-argument `dataConverter` factory helpers, and static `payloadConverterPath` modules that export a named `payloadConverter`; codec servers and full transport parity are still out of scope.
- Interceptors and middleware are only supported through a narrow static `interceptors.workflowModules` bridge; runtime interceptor behavior is still caveated and not trust-backed parity.
- Arbitrary external async SDK execution inside workflow code is still out of scope for the current Temporal replacement claim.
- Dynamic `Worker.create(...)` bootstraps are still blocked.
- Unsupported Temporal workflow APIs remain blocked until they are explicitly implemented and trusted.
- The migration path targets the compiled Temporal TypeScript subset, not the full Temporal SDK surface.

## Trust Limits

- A repo can qualify with caveats when replay validation has not yet been exercised against Fabrik-side histories.
- A repo can qualify with caveats when deployment and poller health have not yet been proven in a target Fabrik environment.
- The alpha trust story is about replay, failover, and rollout at the artifact boundary, not wire-level Temporal compatibility.
- Queue-preservation evidence is currently exposed through replay and run-detail surfaces, not a separate fleet-wide summary view.
- Payload/data-converter trust is validated only for the default-compatible adapter slice; broader custom converter parity is still out of scope.

## Product Limits

- The alpha is not a general Temporal replacement claim.
- The alpha is not full visibility/search parity.
- The alpha is not hosted-platform hardening or multi-tenant GA readiness.

## Scope Rule

No new parity work should be added to the alpha path unless it directly unblocks:

- the primary alpha repo
- the shadow qualification repo
- an already-claimed supported feature that lacks trust evidence
