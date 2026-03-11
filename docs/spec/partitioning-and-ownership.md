# Partitioning and Ownership

## Purpose

This document freezes how workflow runs are routed and who is allowed to advance them.

## Partition Key

The routing identity for an active execution is:

- `tenant_id + instance_id + run_id`

This combined value becomes the canonical partition key.

## Ownership Contract

- one executor owner advances a partition at a time
- within a partition owner, one workflow run is advanced by one in-memory task at a time
- duplicate delivery is tolerated and neutralized through dedupe plus idempotent side-effect protocols

## Hot-State Contract

While an executor owns a partition, it may cache:

- execution frame
- pinned artifact metadata
- pending timer metadata
- pending join state

It may not treat cached state as authoritative.

Current implementation note:

- the current executor keeps a bounded in-memory cache for active instances keyed by `tenant_id + instance_id`
- cache misses restore from PostgreSQL snapshots, replay the run tail from the broker, and then repopulate the hot cache
- the current implementation persists a single logical partition lease in PostgreSQL through `workflow_partition_ownership`
- executor-service claims and renews that lease with a monotonically increasing `owner_epoch`
- executor turns validate `(partition_id, owner_id, owner_epoch)` before mutating state or publishing follow-up events
- ownership loss clears the executor hot cache and forces later turns to restore from snapshot + replay after reacquisition
- timer-service reads the current ownership record, only dispatches due timers for the active epoch, and stamps `TimerFired` with the observed `owner_epoch`
- the executor exposes its local ownership record and recent ownership transitions over `/debug/ownership`
- restore source is surfaced as one of `initialized`, `projection`, `snapshot_replay`, or `cache` in executor debug output

## Rebalance Contract

On ownership loss:

- the old owner stops accepting new work for that partition
- best-effort snapshot flush may happen
- cached state is discarded after handoff
- replay on the new owner is the correctness fallback

## Mid-Flight Rule

If ownership changes while a run turn is in progress, duplicate consumption is acceptable. Duplicate effects are neutralized by event-id dedupe and connector idempotency keys.
