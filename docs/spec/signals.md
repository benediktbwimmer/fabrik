# Signals, Queries, and Updates

## Purpose

This document freezes inbound workflow interaction semantics.

## Signal Contract

- signals are durable inbound workflow messages
- signal acceptance is represented in workflow history
- signal handling must be deterministic at the workflow boundary

Signals may be observed through explicit waits, registered handlers, or both, but the runtime rules must not depend on ad hoc callback timing.

## Query Contract

- queries do not mutate workflow state
- queries may require strong routing to the current workflow owner
- query results may come from hot state, replay, or a consistent snapshot depending on the query model
- the query consistency contract must be explicit per API

## Update Contract

- updates are durable mutating requests against a workflow
- update acceptance and update completion are distinct states
- update handlers must be replay-safe
- update results and rejections must be visible in workflow history

## Ordering

- signals and updates are ordered per workflow run by durable acceptance order
- the runtime must define how workflow tasks consume accepted messages when multiple message types are pending
- no later message may bypass an earlier accepted message in ways that violate the documented workflow semantics

## Interleaving

- message handling occurs only at deterministic workflow suspension or handler execution points
- arbitrary interleaving with user code execution is forbidden
- the platform must support handler-style semantics without giving up replay safety

## Visibility

Signals and updates become visible to visibility APIs after their acceptance events are durably recorded. Query invocations may be logged for audit or debugging, but queries are not required to appear in normal user-facing event history unless the product contract chooses to expose them.
