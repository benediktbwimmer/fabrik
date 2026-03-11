# ADR 0004: Pivot to Temporal Feature Parity

## Status

Accepted

## Date

2026-03-11

## Context

The initial `fabrik` design was a standalone event-driven workflow engine centered around:

- connector workers as the primary external-effect abstraction
- sandboxed Wasm modules for tenant-defined logic
- built-in step handlers instead of arbitrary user code

While this design was technically coherent, it created a product that was *less capable* than Temporal in critical ways:

- activities were limited to built-in connectors and lightweight handlers
- users could not run their own arbitrary code as activities
- there was no task queue infrastructure for worker dispatch
- there was no support for queries, updates, child workflows, or worker versioning

These gaps meant `fabrik` was not a viable replacement for teams already using Temporal, nor for teams evaluating durable execution platforms with broad capability requirements.

## Decision

We will pivot `fabrik` to target Temporal feature parity:

- activities become arbitrary user code executed by workers through task queues
- connectors are demoted to optional built-in activity implementations
- task queues become first-class infrastructure for both workflow and activity dispatch
- the API surface expands to cover signals, queries, updates, child workflows, cancellation, and termination
- worker versioning and visibility become required platform features
- Wasm sandboxing is moved to a future direction rather than a core requirement

The compiled-workflow execution model is preserved and becomes the primary performance differentiator.

## Rationale

The strongest version of `fabrik` keeps what was already unique:

- compiled workflow artifacts for low-latency workflow decisions
- shard-local hot-state ownership
- log-first durable history

While adding what Temporal users expect:

- arbitrary activity code
- worker fleets
- task queue dispatch
- rich workflow interaction APIs
- production-grade versioning and visibility

This positions `fabrik` as "Temporal's programming model with a faster runtime" rather than "a different, less capable workflow engine."

## Consequences

### Positive

- the product is directly comparable to Temporal and can target Temporal users
- the compiled-workflow advantage is preserved as a clear differentiator
- the activity model is no longer artificially constrained
- the platform can grow toward real production use cases

### Negative

- the implementation scope is significantly larger than the original connector-first design
- task queues, matching, worker versioning, and visibility are now mandatory
- some early specs and code must be revised or replaced
- Wasm sandboxing is deferred rather than built into the core

## Superseded Decisions

This ADR supersedes the activity constraints implied by the original connector-first design. It does not supersede:

- ADR 0001 (log-first architecture with compiled workflows)
- ADR 0002 (code-authored workflows compiled to artifacts)
- ADR 0003 (frozen execution invariants)

Those decisions remain valid and are strengthened by the pivot.
