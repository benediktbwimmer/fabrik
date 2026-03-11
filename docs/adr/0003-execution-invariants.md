# ADR 0003: Freeze Core Execution Invariants

## Status

Accepted

## Date

2026-03-11

## Decision

The following execution invariants are frozen for `fabrik`:

- the event log is authoritative
- snapshots are optimization only
- one workflow instance has one ordered history per run
- one active executor owner advances a workflow instance at a time
- replay must be deterministic
- external effects only count when confirmed by result events

## Implications

- services may be renamed or split later, but these invariants are not optional
- any feature that violates deterministic replay is invalid
- any optimization that makes snapshots semantically authoritative is invalid
- any connector behavior that mutates workflow state directly is invalid

## Follow-Up

These invariants are elaborated in the spec set under `docs/spec/`.
