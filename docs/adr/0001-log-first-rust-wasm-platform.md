# ADR 0001: Choose a Log-First Rust + Wasm Workflow Architecture

## Status

Accepted

## Date

2026-03-11

## Context

`fabrik` is intended to be a high-performance event-driven workflow automation platform optimized for very large workloads.

Two candidate directions were considered:

- a log-first sharded workflow engine on Redpanda
- a Fluvio + SmartModules-first platform

The product needs:

- durable workflow history
- high event throughput
- deterministic replay
- code-authored workflow ergonomics with explicit execution semantics
- long-lived waits and timers
- safe multi-tenant extensibility
- a strong Rust implementation story

## Decision

We will build `fabrik` as a log-first workflow platform with:

- Redpanda as the durable event backbone
- Rust executor services using Tokio
- workflow semantics implemented in our own execution layer
- authored workflows eventually compiling to explicit execution artifacts in our own runtime
- Wasmtime for sandboxed user-defined logic
- PostgreSQL as the initial snapshot and control metadata store

We will not make broker-level transformation the center of product semantics.

## Rationale

This decision keeps the most valuable behavior under direct product control:

- workflow durability
- replay
- timer correctness
- state-machine execution semantics
- tenant isolation
- execution debugging

It also gives a practical path to scale because the architecture is partition-oriented and replayable.

The design still captures the best idea from the Fluvio direction by adopting a Wasm-first extensibility model for tenant logic, but it avoids making the streaming substrate itself the main programmable surface.

## Consequences

### Positive

- clearer workflow correctness model
- stronger audit and replay story
- lower ecosystem risk for the broker layer
- easier integration leverage through the Kafka-compatible ecosystem
- flexibility to evolve the plugin model independently of the broker

### Negative

- more application logic must be built in-house
- workflow semantics, timers, and replay tooling are our responsibility
- the first implementation is less novel than a broker-programmable platform

## Non-Goals

This decision does not commit us to:

- exactly-once external side effects
- a specific UI
- a final persistent store beyond the initial PostgreSQL choice
- a permanent rejection of Fluvio-inspired inline transforms

## Follow-Up

Next decisions to formalize:

- event envelope and schema evolution policy
- workflow definition format
- workflow SDK and compiler model
- snapshot storage design
- connector capability model
- Wasm host function surface
