# Alpha Known Limitations

This document lists the highest-signal limitations for the current design-partner alpha migration path.

## Current Hard Limits

- Search attributes and memo are only supported in the alpha slice for static start-time values plus exact-match visibility filters.
- Custom payload/data converter behavior is still blocked.
- Interceptors and middleware are still blocked.
- Dynamic `Worker.create(...)` bootstraps are still blocked.
- The migration path targets the compiled Temporal TypeScript subset, not the full Temporal SDK surface.

## Trust Limits

- A repo can qualify with caveats when replay validation has not yet been exercised against Fabrik-side histories.
- A repo can qualify with caveats when deployment and poller health have not yet been proven in a target Fabrik environment.
- The alpha trust story is about replay, failover, and rollout at the artifact boundary, not wire-level Temporal compatibility.
- Queue-preservation evidence is currently exposed through replay and run-detail surfaces, not a separate fleet-wide summary view.

## Product Limits

- The alpha is not a general Temporal replacement claim.
- The alpha is not full visibility/search parity.
- The alpha is not hosted-platform hardening or multi-tenant GA readiness.

## Scope Rule

No new parity work should be added to the alpha path unless it directly unblocks:

- the primary alpha repo
- the shadow qualification repo
- an already-claimed supported feature that lacks trust evidence
