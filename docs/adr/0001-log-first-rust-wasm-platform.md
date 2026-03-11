# ADR 0001: Choose a Log-First Workflow Engine with Compiled Workflows and Activity Workers

## Status

Accepted

## Date

2026-03-11

## Context

`fabrik` is intended to be a high-performance Temporal replacement.

The product needs:

- Temporal-compatible workflow semantics
- arbitrary user-defined activities
- low-latency workflow task execution
- very high event throughput
- safe replay and recovery
- long-running timers and waits
- large-scale fan-out / fan-in handling
- a strong Rust implementation story

The previous documentation direction over-weighted sandboxed handlers and connector-style external effects. That direction would have reduced capability compared to Temporal and is therefore not aligned with the product goal.

## Decision

We will build `fabrik` as a log-first workflow engine with:

- a durable history substrate
- Rust workflow executors using Tokio
- compiled workflow execution artifacts for the workflow hot path
- workflow and activity task queues
- arbitrary user activity workers outside the workflow executor hot path
- PostgreSQL as the initial control, visibility, and snapshot metadata store

Optional sandboxed runtimes such as Wasm may still be useful for managed extensions, but they are not the primary activity model of the product.

## Rationale

This decision preserves the parts of the prior architecture that are performance advantages:

- replayable durable history
- shard-local workflow ownership
- deterministic workflow execution
- strong control over timer and failover correctness

It also restores the capability surface required for a Temporal replacement:

- arbitrary activity code
- worker fleets
- task queue semantics
- richer workflow APIs

## Consequences

### Positive

- the product remains competitive with Temporal's programming model
- workflow execution can still be optimized aggressively
- activities keep the flexibility users expect
- scale work can focus on task dispatch, matching, and workflow decision latency

### Negative

- the platform must implement more Temporal-like semantics than the earlier connector-first design
- task queues, worker versioning, and visibility become mandatory infrastructure
- the system must support both deterministic workflow execution and arbitrary activity execution cleanly

## Non-Goals

This decision does not commit us to:

- executing workflow code directly as arbitrary guest code on the hot path
- exactly-once external side effects
- only one worker-hosting model

## Follow-Up

Next decisions to formalize:

- workflow and activity task queue semantics
- worker protocol and heartbeating
- worker versioning model
- visibility and search attribute contracts
- workflow IR expansion for child workflows, updates, and version markers
