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
- a stream-backed throughput lane behind a strict bridge for very wide bulk work
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
- A bridge layer for throughput admission, idempotency, fencing, and callback translation
- A stream-backed execution lane for high-cardinality throughput workloads
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

This repository is still in an early implementation phase. The default runtime path is centered on `unified-runtime` for workflow truth, with compiled workflow execution, durable activity-task history, and external activity workers polling that unified runtime surface.

Throughput mode is being clarified around a stricter internal split:

- `Fabrik Workflows` owns workflow-authoritative history and replay
- a bridge layer owns admission, idempotency, fencing, and callback translation
- a stream-backed execution lane owns nonterminal high-volume throughput execution

The current implementation of that stream-backed lane is `stream-v2`, but backend details are being pushed down below the workflow API and product narrative.

The documentation under `docs/` now reflects the new target architecture:

- compiled workflows for low-latency workflow execution
- arbitrary activities in workers for full Temporal-equivalent capability
- task queues, visibility, updates, child workflows, and worker versioning as first-class features
- a bridge-mediated throughput lane that preserves workflow-authoritative semantics while letting stream execution evolve independently

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

Run the first eager/count smoke flow against the current stream-backed throughput lane (`stream-v2` today):

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
npm run test:conformance
npm run test:benchmarks
```

Run the Temporal comparison benchmark suite:

```bash
./scripts/run-temporal-comparison-benchmark.sh --profile smoke
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

Run the local streaming quickstart:

```bash
./scripts/run-streaming-local-quickstart.sh
```

Analyze or package an existing Temporal TypeScript repo for Fabrik migration:

```bash
cargo run -p fabrik-cli -- \
  migrate temporal ./path-to-temporal-app \
  --output-dir /tmp/fabrik-migration
```

Migration policy and generated output contract:

- [Temporal TypeScript Migration Policy](/Users/bene/code/fabrik/docs/temporal-typescript-migration.md)
- [Alpha Support Matrix](/Users/bene/code/fabrik/docs/alpha-support-matrix.md)
- [Alpha Known Limitations](/Users/bene/code/fabrik/docs/alpha-known-limitations.md)
- [Alpha Qualification Guide](/Users/bene/code/fabrik/docs/alpha-qualification-guide.md)
- [Operator Runbook Alpha](/Users/bene/code/fabrik/docs/operator-runbook-alpha.md)
- [Temporal TypeScript Equivalence Contract](/Users/bene/code/fabrik/docs/temporal-typescript-equivalence-contract.md)
- [Durability and Replay Contract](/Users/bene/code/fabrik/docs/durability-and-replay-contract.md)
- [Ownership and Fencing Contract](/Users/bene/code/fabrik/docs/ownership-and-fencing-contract.md)

## Near-Term Documentation Priorities

- lock the Temporal-parity semantic contract
- redesign the runtime around workflow and activity task queues
- add worker versioning and visibility as core platform features
- preserve the compiled-workflow advantage without weakening the activity model
- formalize the workflow-to-stream bridge contract for throughput mode

## Documents

- [Architecture](/Users/bene/code/fabrik/docs/architecture.md)
- [Product Model](/Users/bene/code/fabrik/docs/product-model.md)
- [Roadmap](/Users/bene/code/fabrik/docs/roadmap.md)
- [Streaming Local Quickstart](/Users/bene/code/fabrik/docs/streaming-local-quickstart.md)
- [Temporal Comparison Benchmarking](/Users/bene/code/fabrik/docs/benchmarking/temporal-comparison.md)
- [Architecture Decision Record 0001](/Users/bene/code/fabrik/docs/adr/0001-log-first-rust-wasm-platform.md)
- [Architecture Decision Record 0002](/Users/bene/code/fabrik/docs/adr/0002-code-authored-compiled-workflows.md)
- [Architecture Decision Record 0003](/Users/bene/code/fabrik/docs/adr/0003-execution-invariants.md)
- [Architecture Decision Record 0004](/Users/bene/code/fabrik/docs/adr/0004-temporal-parity-pivot.md)
- [Architecture Decision Record 0006](/Users/bene/code/fabrik/docs/adr/0006-workflows-bridge-streams-split.md)
- [SDK + Compiler Direction](/Users/bene/code/fabrik/docs/sdk-compiler.md)
- [Streams Bridge Protocol](/Users/bene/code/fabrik/docs/spec/streams-bridge.md)
- [Streams Transition Plan](/Users/bene/code/fabrik/docs/streams-transition-plan.md)
- [Temporal TypeScript Equivalence Contract](/Users/bene/code/fabrik/docs/temporal-typescript-equivalence-contract.md)
- [Temporal TS Replacement Priorities](/Users/bene/code/fabrik/docs/temporal-ts-replacement-priorities.md)
- [Durability and Replay Contract](/Users/bene/code/fabrik/docs/durability-and-replay-contract.md)
- [Ownership and Fencing Contract](/Users/bene/code/fabrik/docs/ownership-and-fencing-contract.md)
- [Semantic Specs Index](/Users/bene/code/fabrik/docs/spec/README.md)
