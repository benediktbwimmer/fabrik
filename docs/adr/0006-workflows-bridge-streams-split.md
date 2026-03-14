# ADR 0006: Workflows, Bridge, and Streams Split

- Status: Accepted
- Date: 2026-03-14

## Context

`fabrik` already has a workflow-centric product story and a high-throughput execution lane for `ctx.bulkActivity()`.

The existing `stream-v2` framing creates conceptual confusion:

- it can look like a hidden workflow backend instead of a distinct execution subsystem
- it blurs workflow-authoritative state and stream-owned progress
- it leaks implementation language upward into docs and architecture

At the same time, we want to preserve the current workflow product thesis:

- `fabrik` remains a high-performance Temporal replacement
- `ctx.bulkActivity()` remains a workflow primitive
- backend choice stays server-controlled

## Decision

Adopt an explicit internal architecture split:

- `Fabrik Workflows` owns workflow-authoritative history, replay, and workflow-visible barriers
- the bridge owns admission, idempotency, fencing, and callback translation
- `Fabrik Streams` owns high-volume nonterminal execution, checkpoints, and projections

This split is architectural first. It does not require an immediate top-level external product split.

## Invariants

- workflow truth lives in workflow history
- stream truth lives in the stream subsystem
- the bridge is the only legal crossing point
- `ctx.bulkActivity()` stays backend-agnostic
- workflow code must not select or name `stream-v2` or any other throughput backend
- stream-side projections, lag, and partial progress are never workflow-authoritative

## API Consequences

### Current

- keep `ctx.bulkActivity()` as the bounded bulk primitive
- keep `StartBulkActivity` and `WaitForBulkActivity` in the workflow IR
- route throughput execution through the bridge into the stream subsystem

### Later

If dedicated stream semantics are exposed to workflows, add separate primitives such as:

- `ctx.startStreamJob(...)`
- checkpoint waits
- stream-job query or cancel APIs

Those should become distinct semantic surfaces rather than overloading throughput mode.

## Why This Decision

This preserves the current workflow story while making the architecture more honest and extensible.

Benefits:

- cleaner separation of workflow and stream semantics
- clearer recovery and debugging rules
- a strict place to define idempotency and fencing
- room for a future standalone `Fabrik Streams` product without rewriting workflow APIs

## Migration Direction

1. Treat `stream-v2` as the first implementation of the internal stream subsystem.
2. Put a formal bridge protocol in front of throughput execution.
3. Keep `ctx.bulkActivity()` stable and backend-agnostic.
4. Update docs and operator surfaces to distinguish workflow-authoritative outcomes from stream-side projections.
5. Add dedicated stream-job workflow primitives only after their semantics justify a new contract.

The concrete engineering sequence for this migration lives in [../streams-transition-plan.md](../streams-transition-plan.md).

## Non-Goals

- exposing `execution: "streams"` or other backend selectors in workflow code
- merging workflow and stream semantics into one execution model
- changing the current workflow product story before the stream subsystem has earned a distinct semantic surface
