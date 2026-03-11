# Signals

## Purpose

This document freezes workflow signal semantics.

## Signal Contract

- signals are durable inbound workflow messages
- signal delivery is represented in workflow history
- signal handling behavior follows mailbox rules, not ad hoc callback timing

## Ordering

- signal ordering is per workflow run
- ordering is determined by mailbox enqueue order for a run
- only the oldest pending mailbox signal may be consumed next
- later matching signals may not bypass an earlier non-matching signal
- duplicate signals may be rejected by an explicit per-run dedupe key

## Buffering

Before workflow start, buffering behavior must be explicit.

Current rule:

- signals for unknown runs are rejected
- signals for known runs are persisted in a durable mailbox even if the workflow is not currently waiting
- mailbox records transition through `queued -> dispatching -> consumed`

## Interleaving

Initial target rule:

- signals are mailbox-queued
- signal handling may resume the workflow only at defined suspension points
- arbitrary interleaving with user code execution is forbidden
- the current implementation uses strict mailbox serialization at wait states rather than callback-style interleaving

## Visibility

Signals become visible to queries after projection accepts the corresponding signal event.

Current implementation note:

- ingest publishes `SignalQueued` after persisting the mailbox record
- executor emits `SignalReceived` only when the oldest pending mailbox entry matches the current wait state
- query APIs expose mailbox contents for the current run and explicit historical runs
