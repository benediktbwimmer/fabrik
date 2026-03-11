# Failure, Retry, and Compensation

## Purpose

This document freezes failure semantics.

## Failure Types

- step failure: one step or effect attempt failed
- workflow failure: the run entered a terminal failed state
- infrastructure failure: runtime crashed or lost ownership; not itself a workflow failure

## Retry Rules

- retries are durable and represented in history
- backoff must be encoded durably
- `max_attempts` includes the initial attempt unless explicitly stated otherwise by a future SDK version
- exhausting retries converts step failure into workflow failure unless compensation rules say otherwise

## Compensation Rules

- compensation is explicit, not implicit rollback
- compensation order is reverse causal order unless the compiled artifact declares otherwise
- compensation execution is itself durable and replayable

## Terminal States

Canonical terminal states:

- `Completed`
- `Failed`
- `Cancelled`
- `ContinuedAsNew`
