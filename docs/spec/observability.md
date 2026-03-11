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

- partition lag
- hot-state cache hit ratio
- replay duration
- timer drift
- effect success and failure rates
- workflow completion latency
- continue-as-new frequency
- replay divergence count

## Trace Rules

Trace and span naming must distinguish:

- ingest
- executor replay
- executor turn advancement
- timer dispatch
- connector execution
- query projection
