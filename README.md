# fabrik

`fabrik` is a high-performance Temporal replacement. The product goal is feature parity with Temporal's workflow model while delivering lower decision latency, higher aggregate throughput, and better scalability for very high event rates and large fan-out / fan-in workloads.

`fabrik` keeps Temporal's core promise:

- durable workflow execution
- arbitrary, possibly non-deterministic activity code
- long-running stateful workflows
- signals, queries, updates, retries, timers, child workflows, and continue-as-new

`fabrik` aims to improve the execution substrate behind that model:

- compiled deterministic workflow tasks instead of replaying general-purpose guest code on the hot path
- shard-local workflow ownership and sticky execution
- log-first durable history
- task-queue dispatch designed for very high concurrent activity volume
- visibility and replay built into the architecture from the start

## Product Thesis

Most durable execution systems make users choose between:

- Temporal-like programming ergonomics
- low-latency workflow decisions
- high total event throughput
- operational clarity under replay, failover, and extreme fan-out

`fabrik` is intended to deliver all four.

The core product position is:

- Temporal-compatible workflow semantics
- arbitrary user activities running in workers
- compiled workflow execution for the workflow side of the house
- infrastructure optimized for massive concurrency and replay-safe scale

## Core Principles

- Durable history first: workflow correctness comes from immutable history, not mutable in-memory state.
- Workflow and activity separation: workflows stay deterministic; activities may run arbitrary user code.
- Compiled workflow execution: workflows are authored in SDKs and compiled into explicit execution artifacts.
- Arbitrary activity workers: activity code is not limited to built-in connectors or lightweight handlers.
- Task queues as first-class infrastructure: workflow tasks and activity tasks are dispatched through scalable queueing and matching layers.
- Sticky execution: active workflow runs stay warm on shard owners whenever possible.
- Replay as a guarantee: every running workflow must remain replayable and version-safe.
- Feature parity before novelty: Temporal-equivalent semantics matter more than unusual runtime abstractions.

## Target Architecture

- Front door APIs for start, signal, query, update, cancel, terminate, describe, and visibility search
- Durable event history for workflow executions
- Workflow task dispatch and sticky queue support
- Activity task queues with worker polling, heartbeats, cancellation, and retries
- Sharded workflow executors that run compiled workflow artifacts
- Timer and timeout infrastructure for workflows and activities
- Visibility and indexing services for search attributes, metadata, and operational queries
- Worker versioning and compatible routing for safe rollout

Connectors remain useful, but only as built-in activity implementations or managed integrations. They are not the primary external-work abstraction of the platform.

## Temporal-Parity Target

The target platform must support:

- workflows authored in normal SDK code
- arbitrary user-defined activities
- workflow and activity task queues
- retries, heartbeats, and cancellation
- signals, queries, and updates
- child workflows
- continue-as-new
- deterministic side effects and version markers
- worker versioning / build compatibility routing
- search attributes and rich visibility APIs
- replay tooling and deterministic upgrade validation

## Repository Status

This repository is still in an early implementation phase. The runtime is centered on compiled workflow execution, durable activity-task history, and external activity workers polled through `matching-service`.

The documentation under `docs/` now reflects the new target architecture:

- compiled workflows for low-latency workflow execution
- arbitrary activities in workers for full Temporal-equivalent capability
- task queues, visibility, updates, child workflows, and worker versioning as first-class features

## Getting Started

Start the local stack:

```bash
make up
```

Stop the local stack:

```bash
make down
```

Show local stack status:

```bash
make status
```

Run the first eager/count smoke flow against `stream-v2`:

```bash
make smoke-eager-count
```

If your machine already uses the default host ports, override them when starting the stack:

```bash
REDPANDA_HOST_PORT=62303 POSTGRES_HOST_PORT=62304 make up
```

Run all checks:

```bash
cargo check
cargo test
npm install
npm run test:compiler
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

## Near-Term Documentation Priorities

- lock the Temporal-parity semantic contract
- redesign the runtime around workflow and activity task queues
- add worker versioning and visibility as core platform features
- preserve the compiled-workflow advantage without weakening the activity model

## Documents

- [Architecture](/Users/bene/code/fabrik/docs/architecture.md)
- [Product Model](/Users/bene/code/fabrik/docs/product-model.md)
- [Roadmap](/Users/bene/code/fabrik/docs/roadmap.md)
- [Architecture Decision Record 0001](/Users/bene/code/fabrik/docs/adr/0001-log-first-rust-wasm-platform.md)
- [Architecture Decision Record 0002](/Users/bene/code/fabrik/docs/adr/0002-code-authored-compiled-workflows.md)
- [Architecture Decision Record 0003](/Users/bene/code/fabrik/docs/adr/0003-execution-invariants.md)
- [Architecture Decision Record 0004](/Users/bene/code/fabrik/docs/adr/0004-temporal-parity-pivot.md)
- [SDK + Compiler Direction](/Users/bene/code/fabrik/docs/sdk-compiler.md)
- [Semantic Specs Index](/Users/bene/code/fabrik/docs/spec/README.md)
