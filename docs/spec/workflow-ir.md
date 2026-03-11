# Workflow IR

## Purpose

This document defines the internal execution artifact model.

## Scope

- public workflows are authored in code
- executors run compiled workflow IR / state machine artifacts
- this IR is the replay boundary

## Artifact Shape

A workflow artifact must contain:

- `definition_id`
- `definition_version`
- `artifact_hash`
- `compiler_version`
- workflow metadata
- state graph
- signal declarations
- activity / effect declarations
- optional marker declarations

## Canonical Node Types

Phase-1 node types:

- `Start`
- `WaitSignal`
- `WaitTimer`
- `InvokeEffect`
- `InvokePredicate`
- `Branch`
- `Fork`
- `Join`
- `RecordMarker`
- `ContinueAsNew`
- `Complete`
- `Fail`
- `Compensate`

## Canonical Transition Model

Each node transition must declare:

- source node id
- target node id
- triggering condition
- optional guard expression reference
- optional payload mapping reference

## Runtime Frame

The execution frame must be representable explicitly and snapshot safely.

Minimum frame contents:

- current node id
- local variables / stored values
- pending timers
- pending joins
- pending compensation stack
- pinned artifact metadata

## Determinism Rule

The IR must encode only deterministic control flow.

Non-deterministic values must enter execution only through:

- prior history events
- explicit marker results
- controlled runtime primitives such as `ctx.now()` or `ctx.uuid()` that record their values

## Open Flexibility

These are intentionally not frozen yet:

- exact binary or JSON encoding of artifacts
- how source maps are stored
- which compiler backend is used for the first SDK language
