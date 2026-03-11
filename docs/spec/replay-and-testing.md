# Replay and Testing

## Purpose

This document freezes replay and validation as core platform infrastructure.

## Replay Requirements

The platform must support:

- replaying a captured history against a chosen artifact
- replaying from snapshot + event tail
- determinism divergence detection
- event-by-event replay diagnostics

Current implementation note:

- `GET /tenants/{tenant_id}/workflows/{instance_id}/replay` and `/runs/{run_id}/replay` return:
  - `replay_source` as either `run_start` or `snapshot_tail`
  - snapshot boundary metadata when a run-scoped snapshot exists
  - a compact transition trace with state/status before and after each replayed event
  - divergence diagnostics for snapshot-boundary mismatches and stored-projection mismatches
- `cargo run -p replay-tool -- <tenant_id> <instance_id> [run_id]` returns the same diagnostics in CLI form

## Required Test Classes

- deterministic executor tests
- golden history replay tests
- artifact compatibility tests
- crash / restart recovery tests
- timer recovery tests
- duplicate-delivery tests
- snapshot restore tests

## CI Rule

Replay validation for representative histories is a release-gating capability, not optional QA.
