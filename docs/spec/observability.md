# Observability

## Purpose

This document freezes the telemetry vocabulary for `fabrik`.

## Required Labels

- `tenant_id`
- `definition_id`
- `definition_version`
- `artifact_hash`
- `instance_id`
- `run_id`
- `workflow_partition`
- `workflow_task_queue`
- `activity_task_queue`
- `activity_type`
- `worker_build_id`

## Required Metrics

- workflow task schedule-to-start latency
- workflow task execution latency
- sticky-cache hit ratio
- snapshot restore count
- replay duration
- timer drift
- activity task schedule-to-start latency
- activity task start-to-close latency
- activity heartbeat timeout count
- activity success and failure rates
- task queue backlog
- poller counts
- workflow completion latency
- continue-as-new frequency
- replay divergence count

## Required Operational Views

Operators must be able to inspect:

- owned workflow partitions
- hot workflow counts
- sticky-queue effectiveness
- task queue backlog and age
- worker poller presence by task queue
- workflow and activity throughput by tenant and type
- version-routing behavior by worker build

## Trace Rules

Trace and span naming must distinguish:

- request acceptance
- workflow replay
- workflow task advancement
- activity task matching
- activity worker execution
- timer dispatch
- visibility projection
