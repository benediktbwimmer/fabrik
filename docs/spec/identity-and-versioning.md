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

The current repository still uses simpler field names in code and APIs. This spec defines the target canonical model the runtime should converge toward.
