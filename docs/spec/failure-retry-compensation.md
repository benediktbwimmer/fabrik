# Failure, Retry, and Compensation

## Purpose

This document freezes failure semantics.

## Failure Types

- workflow task failure: one workflow decision turn failed and must be retried safely
- activity failure: one activity attempt failed
- activity timeout: one activity attempt exceeded a configured timeout or heartbeat limit
- child workflow failure: one child run failed or timed out
- workflow failure: the run entered a terminal failed state
- infrastructure failure: a runtime process crashed or lost ownership; not itself a workflow failure

## Retry Rules

- workflow task retries are internal runtime retries and must preserve deterministic workflow semantics
- activity retries are durable and represented in history
- retry backoff must be encoded durably
- `max_attempts` semantics must be explicit per SDK contract
- exhausting retries converts an activity or child-workflow failure into workflow-level failure unless workflow code handles it

## Cancellation and Termination

- cancellation is distinct from failure
- termination is operator-forced end of execution
- cancellation and termination semantics must cover running activities and child workflows

## Compensation Rules

- compensation is explicit, not implicit rollback
- compensation order is reverse causal order unless the workflow artifact declares otherwise
- compensation execution is itself durable and replayable

## Terminal States

Canonical terminal states:

- `Completed`
- `Failed`
- `Cancelled`
- `Terminated`
- `ContinuedAsNew`
