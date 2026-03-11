# Effects and Connectors

## Purpose

This document freezes the protocol for external side effects.

## Principle

Connectors never mutate workflow state directly. They emit result events back into the log.

## Canonical Protocol

Target protocol:

- `EffectRequested`
- optional `EffectStarted`
- `EffectCompleted`
- `EffectFailed`
- `EffectTimedOut`
- `EffectCancelled`

Current implementation note:

- compiled workflow effects now use `EffectRequested`, `EffectCompleted`, `EffectFailed`, `EffectTimedOut`, and `EffectCancelled`
- legacy JSON workflow definitions still use `StepScheduled`, `StepCompleted`, and `StepFailed`
- connector workers must support both during the transition

## Required Fields

Every effect request must carry:

- connector type
- effect id
- idempotency key
- timeout configuration
- retry policy reference
- request payload

Every effect result must carry:

- effect id
- request causation id
- terminal status
- structured result or failure payload

## Ownership Rules

- executor decides when an effect is requested
- connector workers decide how to execute the effect
- workflow state changes only when result events are accepted by the executor

## Retry Rules

- workflow-owned retries are durable and visible in history
- connector-local retry loops must be bounded and subordinate to workflow policy
- backoff must be encoded durably, not held only in memory

## Timeout and Cancellation

- effect timeout semantics must be explicit
- cancellation must be representable as an event
- timed-out or cancelled effects still require a terminal result event
- timeout timers are workflow-owned and visible in history
- operator cancellation targets the currently active effect attempt for a run

## Dead-Letter Rule

Non-recoverable effect failures may be routed to a dead-letter path, but the workflow history must still record the terminal failure.
