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
- `recorded_at` is assigned when the event is committed
- `logical_time` is optional and reserved for higher-level workflow semantics
- `causation_id` points to the event that directly caused this event
- `correlation_id` groups the broader logical transaction
- `dedupe_key` is required for effect-result or timer-fire events that may be redelivered

## Producer Identity

`producer` must identify the emitting runtime component, for example:

- `ingest-service`
- `executor-service`
- `timer-service`
- `connector-service`
- future SDK compiler or deployment control plane

## Phase-1 Canonical Event Families

- `WorkflowTriggered`
- `WorkflowStarted`
- `WorkflowArtifactPinned`
- `MarkerRecorded`
- `SignalReceived`
- `TimerScheduled`
- `TimerFired`
- `StepScheduled`
- `StepCompleted`
- `StepFailed`
- `EffectRequested`
- `EffectCompleted`
- `EffectFailed`
- `EffectTimedOut`
- `EffectCancelled`

`EffectCancelled` may include operator-supplied reason and metadata for audit/query use.
- `WorkflowContinuedAsNew`
- `WorkflowCompleted`
- `WorkflowFailed`

## Compatibility Rule

Envelope fields may be added in backward-compatible ways, but the meaning of existing fields may not drift.
