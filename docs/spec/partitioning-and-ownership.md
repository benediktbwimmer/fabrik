# Partitioning and Ownership

## Purpose

This document freezes how workflows and task queues are routed and owned.

## Workflow Partition Key

The routing identity for workflow execution is:

- `tenant_id + instance_id`

This value becomes the canonical workflow partition key so all runs for one logical workflow stay on the same workflow shard.

## Workflow Ownership Contract

- one workflow owner advances a workflow run at a time
- within one workflow owner, one workflow run is advanced by one in-memory task at a time
- duplicate delivery is tolerated and neutralized through durable dedupe and idempotent completion handling

## Hot-State Contract

While an executor owns a workflow partition, it may cache:

- execution frame
- pinned artifact metadata
- pending timer metadata
- pending activity metadata
- pending child workflow metadata
- sticky-queue assignment metadata

It may not treat cached state as authoritative.

## Sticky Execution Contract

- active workflows should use sticky workflow-task routing whenever possible
- sticky routing is an optimization only
- sticky loss must fall back to normal routing plus replay-safe restore
- sticky execution must not create multiple concurrent workflow owners for one run

## Activity Queue Contract

Activity task queues are separate from workflow partition ownership.

The platform must support:

- independent scaling of activity pollers
- queue backlog visibility
- poller liveness tracking
- capacity-aware routing
- build-compatibility-aware routing

No activity-queue optimization may violate the workflow-history contract.

## Rebalance Contract

On workflow ownership loss:

- the old owner stops accepting new workflow tasks for that partition
- cached state is discarded after handoff
- the new owner restores via snapshot plus replay
- in-flight duplicate workflow tasks are acceptable if completion handling is dedupe-safe

## Mid-Flight Rule

If ownership changes while a workflow task is in progress, duplicate workflow-task execution is acceptable. The system must neutralize duplicates through event and task-completion idempotency rather than by assuming impossible exactly-once dispatch.
