# fabrik

`fabrik` is a high-throughput, event-driven workflow automation platform built around a durable event log, sharded Rust executors, and sandboxed user logic.

The product goal is not to be another DAG runner. It is a workflow engine for massive event volumes, long-lived stateful automations, and safe multi-tenant extensibility.

## Product Thesis

Most workflow tools optimize for simple orchestration and nice UIs. They break down when workflows become:

- deeply event-driven instead of schedule-driven
- long-lived and stateful
- multi-tenant with custom logic
- audit-heavy and replay-dependent
- high-volume enough that the event backbone matters as much as the control plane

`fabrik` is designed to handle those constraints directly.

## Core Principles

- Event log first: all durable workflow progress is represented as immutable events.
- Code-authored workflows: developers should write workflows in SDK code, not raw internal state diagrams.
- Compiled execution artifacts: authored workflows compile to deterministic workflow IR / state machine artifacts.
- Shard-local execution: workflow instances are owned by shard leaders for ordered state transitions.
- Hot-state ownership: shard owners keep active workflow state warm in memory and fall back to snapshot + replay only on misses or reassignment.
- Replayable state: current workflow state is a projection, not the source of truth.
- At-least-once delivery: correctness comes from idempotency and deterministic replay, not distributed exactly-once claims.
- Wasm extensibility: tenant-defined logic runs in a sandboxed runtime instead of arbitrary host code.
- Separation of concerns: the broker is the event substrate; workflow semantics live in Rust services.

## Authoring Model

The target developer experience is "workflows are code" with strict deterministic runtime semantics.

That means:

- users author workflows through an SDK
- SDK code compiles to a deterministic workflow IR / state machine artifact
- executors run the compiled artifact, not arbitrary user code as the hot path
- every running instance is pinned to a specific definition version and artifact hash recorded in history

The current repository still exposes JSON workflow definitions because they are the fastest way to bootstrap the runtime. They are an implementation stepping stone, not the intended long-term authoring surface.

The canonical semantic contracts now live in:

- [Product Model](/Users/bene/code/fabrik/docs/product-model.md)
- [Semantic Specs Index](/Users/bene/code/fabrik/docs/spec/README.md)

## Initial Architecture

- Durable event backbone: Redpanda
- Core services: Rust with Tokio
- User-defined execution: Wasmtime / WASI components
- State snapshots and query models: pluggable state store, starting with PostgreSQL
- External effects: connector workers with idempotency keys and outbox discipline

See [docs/architecture.md](/Users/bene/code/fabrik/docs/architecture.md) for the detailed system design.

## Platform Scope

The first version of `fabrik` will support:

- event ingestion by API and broker input
- workflow definitions compiled into state machines
- timers, retries, waits, and external signals
- per-tenant workflow isolation
- deterministic workflow replay
- sandboxed user transforms and step handlers
- query APIs for workflow state and execution history

Not in scope for the first cut:

- a polished visual builder
- broad enterprise connector coverage
- arbitrary code execution outside the Wasm sandbox
- cross-region active-active execution

## Current Step Handlers

Built-in handlers available in the current implementation:

- `core.echo`
- `core.accept`
- `core.noop`
- `http.request`

`http.request` uses typed step config inside the workflow definition and returns a structured result payload with:

- connector name
- request method and URL
- response status code
- response headers
- parsed response body
- generated idempotency key

## Planned Workflow Runtime Semantics

The runtime design now explicitly aims for Temporal-like authoring ergonomics without making general-purpose guest code the execution substrate.

Key planned semantics:

- sticky hot-state execution on shard owners
- deterministic workflow APIs such as `ctx.sleep()`, `ctx.signal()`, `ctx.now()`, and `ctx.uuid()`
- artifact pinning by `definition_version` and `artifact_hash`
- marker events for side effects, versioning, and runtime metadata
- explicit history rollover via `ContinueAsNew`-style execution epochs
- replay validation tooling in CI
- defined signal mailbox ordering and interleaving rules

## Repository Direction

This repository started documentation-first so implementation decisions were explicit before code hardened around bad assumptions. It now includes the first Rust workspace scaffold and a local development stack.

Current top-level areas:

- `docs/`: architecture, roadmap, ADRs
- `docs/spec/`: canonical semantic contracts
- `crates/`: shared Rust libraries
- `services/`: service binaries
- `schemas/`: event contracts and workflow definition schemas

Planned additions:

- `deploy/`: production deployment assets
- `examples/`: sample workflows and Wasm components

## First Build Target

The first meaningful milestone is a single-node developer system that can:

1. ingest workflow-triggering events
2. persist them to the log
3. execute a sharded workflow state machine
4. fire timers
5. invoke a sandboxed step
6. persist resulting state and history
7. expose a read API for inspection and replay

That is enough to prove the architecture before scaling it out.

## Getting Started

Start local infrastructure:

```bash
docker compose up -d
```

If your machine already uses the default host ports, override them when starting compose:

```bash
REDPANDA_HOST_PORT=62303 POSTGRES_HOST_PORT=62304 docker compose up -d
```

Run all checks:

```bash
cargo check
cargo test
```

Run a service:

```bash
cargo run -p api-gateway
```

Replay one workflow run from the durable log:

```bash
cargo run -p replay-tool -- <tenant_id> <instance_id> [run_id]
```

Useful local endpoints:

- Redpanda broker: `localhost:29092`
- Redpanda Console: `http://localhost:8080`
- PostgreSQL: `localhost:55433`
- API gateway health: `http://localhost:3000/healthz`
- Ingest trigger API: `POST http://localhost:3001/workflows/{definition_id}/trigger`
- Ingest definition publish API: `POST http://localhost:3001/tenants/{tenant_id}/workflow-definitions`
- Ingest signal API: `POST http://localhost:3001/tenants/{tenant_id}/workflows/{instance_id}/signals/{signal_type}`
- Ingest continue-as-new API: `POST http://localhost:3001/tenants/{tenant_id}/workflows/{instance_id}/continue-as-new`
- Query definition API: `GET http://localhost:3005/tenants/{tenant_id}/workflow-definitions/{definition_id}/latest`
- Query instance API: `GET http://localhost:3005/tenants/{tenant_id}/workflows/{instance_id}`

## Documents

- [Architecture](/Users/bene/code/fabrik/docs/architecture.md)
- [Product Model](/Users/bene/code/fabrik/docs/product-model.md)
- [Roadmap](/Users/bene/code/fabrik/docs/roadmap.md)
- [Architecture Decision Record 0001](/Users/bene/code/fabrik/docs/adr/0001-log-first-rust-wasm-platform.md)
- [Architecture Decision Record 0002](/Users/bene/code/fabrik/docs/adr/0002-code-authored-compiled-workflows.md)
- [Architecture Decision Record 0003](/Users/bene/code/fabrik/docs/adr/0003-execution-invariants.md)
- [SDK + Compiler Direction](/Users/bene/code/fabrik/docs/sdk-compiler.md)
- [Semantic Specs Index](/Users/bene/code/fabrik/docs/spec/README.md)
