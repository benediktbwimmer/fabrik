# Product Model

## Purpose

This document freezes the top-level product identity for `fabrik`.

## Core Statement

`fabrik` is a high-performance Temporal replacement.

The product goal is to match Temporal's durable execution feature set while outperforming it on:

- workflow decision latency
- aggregate workflow and activity throughput
- scale under extreme fan-out / fan-in
- replay-safe operation at very high event rates

## User Model

The user-facing programming model is intentionally Temporal-like:

- workflows are long-lived durable programs
- workflows are authored in SDK code
- activities are arbitrary user code that may be non-deterministic
- workflows communicate through signals, queries, updates, timers, retries, and child workflows

## Execution Model

The runtime model is intentionally not identical to Temporal's internals:

- workflow code compiles to deterministic workflow execution artifacts
- workflow turns execute on shard-local executors
- activities execute in worker processes outside the workflow executor hot path
- task queues mediate dispatch between the control plane, workflow executors, and workers
- durable history is authoritative
- snapshots and caches are optimization only

## Optimization Target

`fabrik` is optimized for:

- many concurrently active workflow executions
- very high total history event throughput
- large-scale activity fan-out and completion fan-in
- low workflow-task latency under sticky execution
- deterministic replay and fast failover
- high-cardinality bulk workloads via throughput mode with `O(chunks)` overhead

## Non-Goals

The following are explicit non-goals:

- inventing a new workflow programming model incompatible with Temporal users
- limiting activities to built-in connectors or sandboxed mini-handlers
- using snapshots or projections as a second source of truth
- sacrificing replay correctness for benchmark-only wins
- making the broker or connector layer the primary product abstraction

## Consequence

All lower-level docs, services, SDKs, and benchmarks should be evaluated against one question first:

Does this move `fabrik` closer to a faster, more scalable Temporal-compatible durable execution platform?
