# Snapshots

## Purpose

This document freezes snapshot semantics so snapshots do not become a second source of truth.

## Snapshot Rule

Snapshots are optimization only. Durable history remains authoritative.

## Snapshot Contents

Minimum workflow snapshot contents:

- execution frame
- current workflow location
- local workflow variables
- pending timer metadata
- pending activity metadata
- pending child workflow metadata
- pending update metadata
- pinned `definition_version`
- pinned `artifact_hash`
- replay start offset
- snapshot schema version

## Compatibility Rules

- snapshots must be versioned
- snapshot decoding must validate compatibility with the workflow artifact pinned in history
- on incompatibility, the runtime must fall back to replay rather than invent conversion behavior

## Trigger Policy

Snapshot frequency is an optimization policy and may evolve, but it must never alter workflow semantics.

## Sticky Execution Interaction

- snapshots complement sticky execution; they do not replace it
- cache misses or ownership loss may restore from snapshot plus replay
- sticky routing should reduce restore frequency in the steady state
