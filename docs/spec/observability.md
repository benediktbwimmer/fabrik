# Observability

## Purpose

This document freezes the telemetry vocabulary for `fabrik`.

## Required Labels

- `tenant_id`
- `definition_id`
- `definition_version`
- `artifact_hash`
- `instance_id`
- `run_id`
- `partition`
- `connector`

## Required Metrics

- `fabrik_executor_cache_hits_total`
- `fabrik_executor_cache_misses_total`
- `fabrik_executor_snapshot_restores_total`
- `fabrik_executor_projection_restores_total`
- `fabrik_executor_restores_after_handoff_total`
- partition lag
- hot-state cache hit ratio
- replay duration
- timer drift
- effect success and failure rates
- workflow completion latency
- continue-as-new frequency
- replay divergence count

Current implementation note:

- the executor debug surface at `/debug/runtime` returns cache hit/miss counters, snapshot restore counts, projection restore counts, cache capacity, snapshot cadence, and the current hot-instance list
- `/debug/ownership` returns the current ownership record, lease expiry, owner epoch, and recent ownership transitions for the local executor process
- `/debug/hot-state/{tenant_id}/{instance_id}` returns the cached run id, definition id, event count, and latest restore source for one hot instance when present

## Trace Rules

Trace and span naming must distinguish:

- ingest
- executor replay
- executor turn advancement
- timer dispatch
- connector execution
- query projection
