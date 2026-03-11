# Timers

## Purpose

This document freezes durable timer semantics.

## Timer Identity

A timer is identified by:

- `tenant_id`
- `instance_id`
- `run_id`
- `timer_id`
- `scheduled_event_id`

## Canonical Events

- `TimerScheduled`
- `TimerFired`

## Contract

- timers are durable records, not in-memory sleeps
- timer firing is at-least-once
- `TimerFired` must be deduplicated by timer identity plus scheduled event identity
- only the timer service may emit `TimerFired`

## Due-Time Semantics

- `due_at` is the durable due time
- timer execution is allowed to be late
- timer execution is never allowed to be early

## Ordering Rule

If a signal and a timer become available at nearly the same real time, workflow behavior follows durable log order, not wall-clock intuition.

## Recovery Rule

After crash or rebalance:

- due timers are rediscovered from durable storage
- previously fired timers must not be treated as new if their fire event was already accepted
- duplicates are neutralized through dedupe keys
