# Durability and Replay Contract

This document freezes the durability and replay contract for the Temporal TS subset trust milestone.

## Purpose

For the supported subset, Fabrik must be able to explain:

- which persisted state is authoritative
- how replay reconstructs workflow-visible state
- what recovery guarantees hold after restart or failover
- what backup, restore, and DR exercises are expected to prove

## Source Of Truth Hierarchy

The authoritative order is:

1. durable workflow history plus pinned artifact
2. snapshot plus event tail, when the snapshot is explicitly tied to that history lineage
3. projections and queue views as operational evidence only

Hard rule:

- projections, caches, queue summaries, and read models are never allowed to become a second source of truth for workflow-visible state

## Replay Inputs

Replay for the supported subset consumes:

- captured workflow history
- the pinned workflow artifact or candidate artifact under validation
- snapshot state only when the snapshot lineage matches the replay target
- recorded activity outcomes at the workflow boundary

Replay does not re-run arbitrary activity code. It validates the workflow-visible consequences of recorded activity outcomes.

## Replay Guarantees

For the supported subset, Fabrik must guarantee:

- deterministic terminal outcome class under replay
- deterministic workflow-visible state under replay
- deterministic divergence reporting when candidate artifacts are incompatible
- stable replay diagnostics at the event and artifact boundary

The contract does not require:

- identical internal event IDs
- identical stack traces
- identical error text beyond semantic failure class and propagation location

## Recovery Expectations

Restart and failover validation for the supported subset must prove:

- recovery can resume from authoritative state without inventing new workflow-visible progress
- duplicate delivery is either absorbed or rejected by explicit fencing rules
- stale completions and stale owners do not mutate authoritative workflow state
- pinned artifacts remain valid replay targets during mixed-version rollout

## Backup, Restore, and DR

Lab trust for this milestone requires documented exercises for:

- backup capture of authoritative replay inputs
- restore into a fresh environment with replay validation
- disaster-recovery drill from documented inputs only

Success means the supported subset can be reconstructed and replay-validated without relying on ad hoc local state.
