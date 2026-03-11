# Product Model

## Purpose

This document freezes the top-level product model for `fabrik` so the project does not drift between incompatible identities.

## Core Statement

`fabrik` is a log-first, deterministic workflow engine optimized for high aggregate event throughput across many workflow instances.

## Authoring Model

- workflows are authored in code through an SDK
- authored workflows compile to deterministic workflow IR / state machine artifacts
- executors run compiled artifacts, not arbitrary user code as the hot path

## Execution Model

- the durable event log is authoritative
- one workflow instance has one ordered history per run
- one active executor owner advances a workflow instance at a time
- snapshots are optimization only
- external side effects only count when confirmed by result events

## Optimization Target

`fabrik` is optimized for:

- many concurrently active workflow instances
- high total event throughput
- low steady-state decision latency through hot-state ownership
- replay-safe long-running execution

It is not optimized for:

- arbitrary unconstrained user code on the hot path
- single-workflow giant in-memory compute jobs
- strongly consistent synchronous read models

## Non-Goals

The following are explicit non-goals:

- cross-system exactly-once semantics
- arbitrary guest-language execution as the ground-truth runtime model
- strongly consistent query projections
- using snapshots as a second source of truth
- making the broker the programmable surface where workflow semantics live

## Consequence

All lower-level specs, services, SDKs, and deployment rules should be evaluated against this model first.
