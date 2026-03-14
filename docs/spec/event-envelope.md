# Event Envelope

## Purpose

This document freezes the canonical workflow event envelope.

## Mandatory Envelope Fields

Every workflow event must carry:

- `schema_version`
- `event_id`
- `event_type`
- `tenant_id`
- `definition_id`
- `definition_version`
- `artifact_hash`
- `instance_id`
- `run_id`
- `partition_key`
- `recorded_at`
- optional `logical_time`
- optional `causation_id`
- optional `correlation_id`
- optional `dedupe_key`
- `producer`
- event payload

## Field Rules

- `event_id` is globally unique
- `recorded_at` is assigned when the event is durably accepted
- `causation_id` points to the event that directly caused this event
- `correlation_id` groups the broader logical operation
- `dedupe_key` is required for events that may be emitted more than once by retrying infrastructure
- `partition_key` must route all runs for one logical workflow identity to the same workflow shard

## Producer Identity

`producer` must identify the emitting runtime component, for example:

- `api-gateway`
- `unified-runtime`
- `activity-worker`
- `timer-service`
- `visibility-projector`

## Canonical Event Families

The canonical event families include:

- `WorkflowExecutionStarted`
- `WorkflowTaskScheduled`
- `WorkflowTaskStarted`
- `WorkflowTaskCompleted`
- `WorkflowTaskFailed`
- `ActivityTaskScheduled`
- `ActivityTaskStarted`
- `ActivityTaskHeartbeatRecorded`
- `ActivityTaskCompleted`
- `ActivityTaskFailed`
- `ActivityTaskTimedOut`
- `ActivityTaskCancelled`
- `SignalAccepted`
- `SignalDelivered`
- `QueryRecorded`
- `UpdateAccepted`
- `UpdateCompleted`
- `UpdateRejected`
- `TimerStarted`
- `TimerFired`
- `MarkerRecorded`
- `VersionMarkerRecorded`
- `ChildWorkflowExecutionStarted`
- `ChildWorkflowExecutionCompleted`
- `ChildWorkflowExecutionFailed`
- `ChildWorkflowExecutionCancelled`
- `ChildWorkflowExecutionTimedOut`
- `WorkflowExecutionContinuedAsNew`
- `WorkflowExecutionCompleted`
- `WorkflowExecutionFailed`
- `WorkflowExecutionCancelled`
- `WorkflowExecutionTerminated`

Not every event must be exposed to end users in raw form, but the history model must be rich enough to reconstruct workflow behavior and operator decisions.

## Compatibility Rule

Envelope fields may be added in backward-compatible ways, but the meaning of existing fields may not drift.
