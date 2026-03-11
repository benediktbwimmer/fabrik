# Partitioning and Ownership

## Purpose

This document freezes how workflow runs are routed and who is allowed to advance them.

## Partition Key

The routing identity for an active execution is:

- `tenant_id + instance_id`

This combined value becomes the canonical partition key so all `ContinueAsNew` epochs for one logical instance stay on the same shard.

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
- the current implementation persists one PostgreSQL lease record per broker partition through `workflow_partition_ownership`
- executor-service can own multiple partitions through `WORKFLOW_PARTITIONS`, or discover them dynamically through `workflow_executor_membership` and `workflow_partition_assignments`
- dynamic assignment is driven by executor heartbeats plus a PostgreSQL advisory-lock rebalance pass that writes one owner assignment per broker partition
- executor-service claims and renews each owned lease with a monotonically increasing `owner_epoch`
- executor turns validate `(partition_id, owner_id, owner_epoch)` before mutating state or publishing follow-up events
- ownership loss clears the affected partition cache and forces later turns to restore from snapshot + replay after reacquisition
- timer-service stores timers with `partition_id`, scans all broker partitions, only claims due timers for partitions with an active executor owner, and stamps `TimerFired` with the observed `owner_epoch`
- the executor exposes its owned partition set and recent ownership transitions over `/debug/ownership`
- restore source is surfaced as one of `initialized`, `projection`, `snapshot_replay`, or `cache` in executor debug output

## Rebalance Contract

On ownership loss:

- the old owner stops accepting new work for that partition
- best-effort snapshot flush may happen
- cached state is discarded after handoff
- replay on the new owner is the correctness fallback

## Mid-Flight Rule

If ownership changes while a run turn is in progress, duplicate consumption is acceptable. Duplicate effects are neutralized by event-id dedupe and connector idempotency keys.
