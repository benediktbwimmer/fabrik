# Snapshots

## Purpose

This document freezes snapshot semantics so snapshots do not become a second source of truth.

## Snapshot Rule

Snapshots are optimization only. The event log remains authoritative.

## Snapshot Contents

Minimum snapshot contents:

- execution frame
- current node id
- local workflow variables
- pending timer metadata
- pending join / fork metadata
- pinned `definition_version`
- pinned `artifact_hash`
- replay start offset
- snapshot schema version

## Compatibility Rules

- snapshots must be versioned
- snapshot decoding must validate compatibility with the artifact pinned in history
- on incompatibility, the runtime must fall back to replay rather than invent conversion behavior

## Trigger Policy

Snapshot frequency is an optimization policy and may evolve, but it must never alter workflow semantics.

## Current Implementation Note

- the current runtime persists run-scoped snapshots on a configurable event-count interval and always on terminal state changes
- snapshots carry the replay boundary through `last_event_id` and `event_count`
- executor cache misses restore from the latest snapshot and replay the run tail after that boundary
- snapshot restore source is exposed through executor debug endpoints so operators can distinguish cache hits from projection restores and snapshot-backed restores
