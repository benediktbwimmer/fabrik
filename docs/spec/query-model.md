# Query Model

## Purpose

This document freezes the external read-model contract.

## Current State View

The current-state query for a workflow run should expose at least:

- `tenant_id`
- `definition_id`
- `definition_version`
- `artifact_hash`
- `instance_id`
- `run_id`
- current node / state
- current status
- latest accepted event id
- latest accepted event type
- last updated time

Current implementation note:

- `GET /tenants/{tenant_id}/workflows/{instance_id}` returns the current projected run state
- `GET /tenants/{tenant_id}/workflows/{instance_id}/runs` returns the persisted run lineage for that logical instance and accepts `limit` / `offset`
- `GET /tenants/{tenant_id}/workflows/{instance_id}/snapshot` returns the latest persisted run-scoped snapshot for that instance
- `GET /tenants/{tenant_id}/workflows/{instance_id}/signals` and `/runs/{run_id}/signals` return durable mailbox records with queue status and accept `limit` / `offset`

## History View

The history query should expose:

- ordered events
- causation and correlation links
- pagination cursor
- event retention expectations

Current implementation note:

- current history responses accept `limit` / `offset` and include a `page` object with `limit`, `offset`, `returned`, `total`, `has_more`, and `next_offset`
- current history and replay responses also include persisted effect attempt timelines for the requested run
- current history and replay responses include `previous_run_id`, `next_run_id`, and `continue_reason` when the run is part of a `ContinueAsNew` chain
- replay responses include `replay_source`, optional snapshot-boundary metadata, a compact transition trace, and divergence diagnostics when snapshot-backed replay or the stored projection do not match
- query-service exposes `GET /debug/retention` with the active pagination / retention policy and the last closed-run read-model prune result

## Retention Contract

- broker event history retention is controlled by the event-log substrate, not by query-service
- query-service retention applies only to PostgreSQL read-model data for closed runs
- retention sweeps can prune `workflow_runs`, `workflow_effects`, `workflow_signals`, and `workflow_state_snapshots` after their configured closed-run cutoff
- current-run projections are not pruned by the query retention sweeper

## Lag Contract

- query models are eventually consistent
- operational tooling must surface projection lag
- query consumers may not assume read-after-write consistency
