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
npm install
npm run test:compiler
```

Tune executor hot-state capacity with:

```bash
EXECUTOR_CACHE_CAPACITY=10000 cargo run -p executor-service
```

Tune sparse snapshot cadence with:

```bash
EXECUTOR_SNAPSHOT_INTERVAL_EVENTS=50 cargo run -p executor-service
```

Tune ownership lease behavior with:

```bash
WORKFLOW_PARTITIONS=0,1 OWNERSHIP_LEASE_TTL_SECONDS=15 OWNERSHIP_RENEW_INTERVAL_SECONDS=5 cargo run -p executor-service
```

Enable dynamic partition assignment with executor membership heartbeats:

```bash
EXECUTOR_CAPACITY=4 OWNERSHIP_MEMBER_HEARTBEAT_TTL_SECONDS=15 OWNERSHIP_ASSIGNMENT_POLL_INTERVAL_SECONDS=2 OWNERSHIP_REBALANCE_INTERVAL_SECONDS=5 cargo run -p executor-service
```

Tune query pagination and closed-run read-model retention with:

```bash
QUERY_DEFAULT_PAGE_SIZE=100 QUERY_MAX_PAGE_SIZE=500 QUERY_RUN_RETENTION_DAYS=30 QUERY_EFFECT_RETENTION_DAYS=14 QUERY_SIGNAL_RETENTION_DAYS=14 QUERY_SNAPSHOT_RETENTION_DAYS=7 QUERY_RETENTION_SWEEP_INTERVAL_SECONDS=300 cargo run -p query-service
```

Enable automatic history rollover with:

```bash
EXECUTOR_CONTINUE_AS_NEW_EVENT_THRESHOLD=500 EXECUTOR_CONTINUE_AS_NEW_EFFECT_ATTEMPT_THRESHOLD=100 EXECUTOR_CONTINUE_AS_NEW_RUN_AGE_SECONDS=3600 cargo run -p executor-service
```

Run a service:

```bash
cargo run -p api-gateway
```

Replay one workflow run from the durable log:

```bash
cargo run -p replay-tool -- <tenant_id> <instance_id> [run_id]
```

Compile a TypeScript workflow artifact:

```bash
node sdk/typescript-compiler/compiler.mjs \
  --entry examples/typescript-workflows/order-workflow.ts \
  --export orderWorkflow \
  --definition-id order-workflow \
  --version 1 \
  --out /tmp/order-workflow-artifact.json
```

Current TypeScript compiler constraints:

- only `await ctx.*` suspension points are allowed
- compiler errors include source file, line, and column
- compiled artifacts include a `source_map` for lowered state ids
- `ctx.now()` and `ctx.uuid()` are implemented as deterministic compiled expressions
- `ctx.sideEffect(expr)` records `MarkerRecorded` events for compiled expression values
- `await ctx.sideEffect("connector", input, { timeout })` emits host-backed `EffectRequested` / `EffectCompleted` / `EffectFailed` events and can schedule workflow-owned timeouts
- arbitrary guest callbacks inside `ctx.sideEffect()` are still not supported
- workflow history and replay responses include persisted effect attempt timelines alongside the raw event stream
- the executor keeps a bounded hot-state cache for active instances, persists periodic run-scoped PostgreSQL snapshots, and restores cold runs by replaying the event tail after the snapshot boundary
- the workflow topic is now partitioned and publishers hash on a stable `tenant_id:instance_id` partition key so all runs for one logical instance stay on the same shard
- executor-service can either own a configured partition set or discover assignments dynamically through PostgreSQL-backed executor membership and partition assignment tables
- the executor persists per-partition ownership leases with epochs in PostgreSQL, renews them in the background, clears partition-local hot state on ownership loss, and fences stale turns against those leases
- executors heartbeat membership, opportunistically rebalance partition assignments under a PostgreSQL advisory lock, and start/stop per-partition worker loops as assignments change
- timer-service polls all broker partitions, consults the active ownership table, claims due timers by partition, stamps `TimerFired` with the observed executor ownership epoch, and the executor drops stale timer fires after handoff
- executor-service can automatically emit `WorkflowContinuedAsNew` at durable wait boundaries when configured event-count, effect-attempt, or run-age thresholds are exceeded
- run lineage is persisted in PostgreSQL so current and historical runs for one logical `instance_id` can be inspected as a chain
- signals now flow through a durable FIFO mailbox per run: `SignalQueued` records arrival, executor consumes only the oldest pending signal at matching wait states, and query APIs expose queued/dispatching/consumed signal records
- the executor exposes debug surfaces for cache hit/miss counters, restore-source breakdown, ownership transitions, and partition-local runtime state across the owned shard set
- replay responses and the `replay-tool` are snapshot-aware: they report whether replay started from run start or snapshot tail, include compact transition traces, and surface snapshot/projection divergence diagnostics when determinism or restore boundaries drift
- broker topology is inspectable over HTTP, and startup now fails fast if the configured workflow partition count exceeds the actual topic partition count
- query responses for runs, history, effects, and signals are paginated with `limit` and `offset`, and query-service can prune closed-run PostgreSQL read-model data on a retention sweep without changing broker event history

Useful local endpoints:

- Redpanda broker: `localhost:29092`
- Redpanda Console: `http://localhost:8080`
- PostgreSQL: `localhost:55433`
- API gateway health: `http://localhost:3000/healthz`
- Ingest trigger API: `POST http://localhost:3001/workflows/{definition_id}/trigger`
- Ingest definition publish API: `POST http://localhost:3001/tenants/{tenant_id}/workflow-definitions`
- Ingest artifact publish API: `POST http://localhost:3001/tenants/{tenant_id}/workflow-artifacts`
- Ingest signal API: `POST http://localhost:3001/tenants/{tenant_id}/workflows/{instance_id}/signals/{signal_type}`
- Ingest continue-as-new API: `POST http://localhost:3001/tenants/{tenant_id}/workflows/{instance_id}/continue-as-new`
- Ingest effect cancel API: `POST http://localhost:3001/tenants/{tenant_id}/workflows/{instance_id}/effects/{effect_id}/cancel`
- Query definition API: `GET http://localhost:3005/tenants/{tenant_id}/workflow-definitions/{definition_id}/latest`
- Query artifact API: `GET http://localhost:3005/tenants/{tenant_id}/workflow-artifacts/{definition_id}/versions/{version}`
- Query instance API: `GET http://localhost:3005/tenants/{tenant_id}/workflows/{instance_id}`
- Query run lineage API: `GET http://localhost:3005/tenants/{tenant_id}/workflows/{instance_id}/runs`
- Query latest snapshot API: `GET http://localhost:3005/tenants/{tenant_id}/workflows/{instance_id}/snapshot`
- Query current-run effects API: `GET http://localhost:3005/tenants/{tenant_id}/workflows/{instance_id}/effects`
- Query current-run signals API: `GET http://localhost:3005/tenants/{tenant_id}/workflows/{instance_id}/signals`
- Query run effects API: `GET http://localhost:3005/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/effects`
- Query run signals API: `GET http://localhost:3005/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/signals`
- Query run history API: `GET http://localhost:3005/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/history`
- Query current-run history API: `GET http://localhost:3005/tenants/{tenant_id}/workflows/{instance_id}/history`
- Query run replay API: `GET http://localhost:3005/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/replay`
- Query current-run replay API: `GET http://localhost:3005/tenants/{tenant_id}/workflows/{instance_id}/replay`
- Query broker topology debug API: `GET http://localhost:3005/debug/broker`
- Query retention debug API: `GET http://localhost:3005/debug/retention`
- Executor runtime debug API: `GET http://localhost:3002/debug/runtime`
- Executor ownership debug API: `GET http://localhost:3002/debug/ownership`
- Executor broker topology debug API: `GET http://localhost:3002/debug/broker`
- Executor hot-state debug API: `GET http://localhost:3002/debug/hot-state/{tenant_id}/{instance_id}`

Pagination parameters:

- append `?limit={n}&offset={n}` to run lineage, history, effect, and signal query endpoints
- responses include a `page` object with `limit`, `offset`, `returned`, `total`, `has_more`, and `next_offset`

Example TypeScript workflows live in [examples/typescript-workflows/order-workflow.ts](/Users/bene/code/fabrik/examples/typescript-workflows/order-workflow.ts) and [examples/typescript-workflows/helpers.ts](/Users/bene/code/fabrik/examples/typescript-workflows/helpers.ts).

## Documents

- [Architecture](/Users/bene/code/fabrik/docs/architecture.md)
- [Product Model](/Users/bene/code/fabrik/docs/product-model.md)
- [Roadmap](/Users/bene/code/fabrik/docs/roadmap.md)
- [Architecture Decision Record 0001](/Users/bene/code/fabrik/docs/adr/0001-log-first-rust-wasm-platform.md)
- [Architecture Decision Record 0002](/Users/bene/code/fabrik/docs/adr/0002-code-authored-compiled-workflows.md)
- [Architecture Decision Record 0003](/Users/bene/code/fabrik/docs/adr/0003-execution-invariants.md)
- [SDK + Compiler Direction](/Users/bene/code/fabrik/docs/sdk-compiler.md)
- [Semantic Specs Index](/Users/bene/code/fabrik/docs/spec/README.md)
