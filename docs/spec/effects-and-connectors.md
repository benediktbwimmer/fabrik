# Activities, Task Queues, and Connectors

## Purpose

This document freezes the contract for external work.

## Principle

Activities are the primary external-work abstraction of the platform.

Connectors are optional built-in or managed activity implementations. They are not a separate correctness model.

## Canonical Activity Protocol

Target protocol:

- `ActivityTaskScheduled`
- `ActivityTaskStarted`
- zero or more `ActivityTaskHeartbeatRecorded`
- one terminal event:
  - `ActivityTaskCompleted`
  - `ActivityTaskFailed`
  - `ActivityTaskTimedOut`
  - `ActivityTaskCancelled`

## Required Fields

Every activity task must carry:

- activity type
- task queue
- activity id
- workflow and activity attempt counters
- idempotency or task token identity
- timeout configuration
- retry policy
- request payload
- worker version routing metadata when applicable

Every activity result must carry:

- activity id
- request causation id
- terminal status
- structured result or failure payload
- worker identity and build metadata when available

## Ownership Rules

- the workflow executor decides when an activity is scheduled
- activity workers decide how to execute the activity
- workflow state changes only when activity result events are durably accepted
- no activity worker may mutate workflow state directly

## Retry Rules

- workflow-visible activity retries are durable and represented in history
- activity retry policy must be explicit
- connector-local hidden retry loops must remain bounded and subordinate to the durable retry contract

## Heartbeats, Timeout, and Cancellation

- long-running activities may heartbeat progress
- heartbeat timeout semantics must be explicit and durable
- schedule-to-start and start-to-close timeout semantics must be explicit and durable
- cancellation must be representable as an event and deliverable to workers

## Task Queue Rules

- workers poll task queues
- task delivery is at-least-once
- completion handling must be idempotent
- matching must support backlog visibility, rate limiting, and worker-version-aware routing

## Connector Rule

Managed connectors should be implemented as ordinary activity types or activity-worker libraries so that users do not face a different correctness model depending on whether work is "custom" or "built in."
