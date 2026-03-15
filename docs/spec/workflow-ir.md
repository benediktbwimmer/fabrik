# Workflow IR

## Purpose

This document defines the internal workflow execution artifact model.

## Scope

- workflows are authored in code
- workflows compile to deterministic execution artifacts
- workflow executors run those artifacts
- activities remain external worker-executed operations
- the artifact is the workflow replay boundary

## Artifact Shape

A workflow artifact must contain:

- `definition_id`
- `definition_version`
- `artifact_hash`
- `compiler_version`
- workflow metadata
- workflow execution graph or bytecode
- signal definitions
- query definitions
- update definitions
- activity callsite metadata
- child workflow callsite metadata
- marker and version metadata
- source map metadata

## Canonical Node Types

Phase-1 node types must cover:

- `Start`
- `WaitSignal`
- `WaitUpdate`
- `RunQueryHandler`
- `WaitTimer`
- `ScheduleActivity`
- `AwaitActivity`
- `StartBulkActivity` — throughput mode batch admission with bridge-managed execution metadata
- `WaitForBulkActivity` — throughput mode batch barrier satisfied only by a bridge-delivered terminal outcome
- `StartChildWorkflow`
- `AwaitChildWorkflow`
- `InvokePredicate`
- `Branch`
- `Fork`
- `Join`
- `RecordMarker`
- `RecordVersionMarker`
- `ContinueAsNew`
- `Complete`
- `Fail`
- `Cancel`
- `Compensate`

The exact encoding may evolve, but the semantic surface must support common Temporal-style orchestration patterns.

## Canonical Transition Model

Each transition must declare:

- source node id
- target node id
- triggering condition
- optional guard expression reference
- optional payload mapping reference

## Runtime Frame

The execution frame must be representable explicitly and snapshot safely.

Minimum frame contents:

- current node id or program counter
- local variables
- pending timers
- pending activities
- pending child workflows
- pending joins
- pending updates
- compensation stack
- pinned artifact metadata

For throughput mode, bulk-call metadata must be representable in the artifact and frame, including:

- execution policy
- reducer identity
- bridge request identity
- handle and barrier metadata needed to await the terminal batch outcome

## Determinism Rule

The IR must encode only deterministic workflow control flow.

Non-deterministic values may enter workflow execution only through:

- prior history events
- explicit marker results
- version markers
- controlled runtime primitives that durably record their outputs

## Design Constraint

The IR must not be defined so narrowly that it prevents feature parity with Temporal workflow semantics.

## Planned Extension Boundary

Dedicated stream-job workflow semantics are intentionally outside the current phase-1 IR.

If `startStreamJob`-style primitives are added later, they should land as distinct node types such as:

- `StartStreamJob`
- `WaitForStreamCheckpoint`
- `QueryStreamJob`
- `CancelStreamJob`
- `AwaitStreamJobTerminal`

They must not be smuggled into the bulk-activity nodes in a way that weakens the current throughput-mode contract.

See [stream-jobs.md](/Users/bene/code/fabrik/docs/spec/stream-jobs.md) for the target semantic contract behind those nodes.
The stream-side compiled artifact model that those nodes talk to is defined separately in [streams-ir.md](/Users/bene/code/fabrik/docs/spec/streams-ir.md).
