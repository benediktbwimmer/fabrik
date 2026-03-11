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

## Rebalance Contract

On ownership loss:

- the old owner stops accepting new work for that partition
- best-effort snapshot flush may happen
- cached state is discarded after handoff
- replay on the new owner is the correctness fallback

## Mid-Flight Rule

If ownership changes while a run turn is in progress, duplicate consumption is acceptable. Duplicate effects are neutralized by event-id dedupe and connector idempotency keys.
