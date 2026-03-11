# ADR 0003: Freeze Core Execution Invariants

## Status

Accepted

## Date

2026-03-11

## Decision

The following execution invariants are frozen for `fabrik`:

- durable workflow history is authoritative
- snapshots, caches, and visibility indexes are optimization only
- one workflow run has one ordered event history
- one active workflow owner advances a workflow run at a time
- replay must be deterministic at the workflow boundary
- activity effects count only when their terminal result event is durably accepted
- workflow and activity task dispatch are at-least-once and require dedupe-safe completion handling

## Implications

- any feature that violates deterministic workflow replay is invalid
- any optimization that makes snapshots or visibility authoritative is invalid
- no worker may mutate workflow state directly
- task-queue or matching optimizations must preserve workflow ordering and idempotent completion semantics

## Follow-Up

These invariants are elaborated in the spec set under `docs/spec/`.
