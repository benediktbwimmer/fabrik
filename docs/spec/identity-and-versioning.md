# Identity and Versioning

## Purpose

This document freezes workflow identity and execution versioning semantics.

## Canonical Identities

- `definition_id`: stable identity of a workflow program family
- `definition_version`: published version within that family
- `artifact_hash`: exact compiled execution artifact
- `instance_id`: logical workflow instance identity
- `run_id`: one concrete execution epoch
- `routing_key`: the partition-routing identity for an active run

## Rules

- a new workflow start creates a new `instance_id` and an initial `run_id`
- `ContinueAsNew` preserves `instance_id` and creates a fresh `run_id`
- every event within a run must carry the same pinned `definition_version` and `artifact_hash`
- replay resolves execution semantics from history, never from "latest"

## Migration Rule

- new runs may start on a newer artifact
- existing runs keep prior semantics unless an explicit migration rule exists
- migrations must themselves be represented durably and replay safely

## Current Implementation Note

- the runtime persists run lineage in PostgreSQL keyed by `tenant_id + instance_id + run_id`
- manual and automatic `ContinueAsNew` both link `previous_run_id -> next_run_id` durably
- query APIs can now expose run chains for one logical `instance_id`
