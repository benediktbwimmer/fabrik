# Signals

## Purpose

This document freezes workflow signal semantics.

## Signal Contract

- signals are durable inbound workflow messages
- signal delivery is represented in workflow history
- signal handling behavior follows mailbox rules, not ad hoc callback timing

## Ordering

- signal ordering is per workflow run
- ordering is determined by accepted event order in the log
- duplicate signals require explicit dedupe semantics if the SDK exposes signal ids

## Buffering

Before workflow start, buffering behavior must be explicit.

Initial rule:

- signals for unknown runs are rejected unless a future workflow-start buffering mode is explicitly implemented

## Interleaving

Initial target rule:

- signals are mailbox-queued
- signal handling may resume the workflow only at defined suspension points
- arbitrary interleaving with user code execution is forbidden

## Visibility

Signals become visible to queries after projection accepts the corresponding signal event.
