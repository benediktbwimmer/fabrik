# Architecture

## Overview

`fabrik` is a log-first durable execution platform designed to replace Temporal at high scale.

The product keeps Temporal's workflow and activity semantics, but the runtime is organized around:

- compiled workflow artifacts for deterministic workflow turns
- task queues for workflow and activity dispatch
- shard-local workflow ownership with sticky execution
- durable history as the source of truth
- worker fleets that run arbitrary user activity code
- a bridge-mediated stream-backed lane for throughput-heavy bulk execution

## High-Level Topology

```text
                   +------------------------+
                   | API / Control Plane    |
                   | start signal update    |
                   | query cancel describe  |
                   +-----------+------------+
                               |
                               v
                    +----------+-----------+
                    | Durable History Log  |
                    | + control metadata   |
                    +----+-------------+---+
                         |             |
                         |             v
                         |    +--------+--------+
                         |    | Visibility      |
                         |    | indexing/query  |
                         |    +-----------------+
                         |
                         v
                +--------+---------+
                | Workflow         |
                | Executors        |
                | compiled turns   |
                +---+-----------+---+
                    |           |   |
                    |           |   v
                    |           | +-------------+
                    |           | | Timer /     |
                    |           | | timeout svc |
                    |           | +-------------+
                    |           |
                    |           v
                    |    +------+------+
                    |    | Throughput  |
                    |    | Bridge      |
                    |    +------+------+
                    |           |
                    v           v
             +------+--------+  +----------------------+
             | Matching /    |  | Stream Execution     |
             | Task Queues   |  | Runtime + Checkpoint |
             +---+--------+--+  +----+------------+----+
                 |        |          |            |
                 |        v          |            v
                 |   +----+------------------+  +------------------+
                 |   | Activity Workers      |  | Stream Workers   |
                 |   | arbitrary user code   |  | bulk / stream    |
                 |   +-----------------------+  +------------------+
                 |
                 v
           +-----+---------------------------+
           | Sticky queue state / ownership  |
           +---------------------------------+
```

## Core Components

### 1. API and Control Plane

The front door accepts and coordinates:

- workflow start
- signal delivery
- query execution
- update submission
- cancellation and termination
- workflow and worker registration metadata
- visibility search

Responsibilities:

- authenticate and authorize tenants
- validate requests
- persist control-plane records
- append canonical workflow events
- route strong operations to the correct workflow owner when needed

### 2. Durable History

Durable workflow history is the authoritative record of execution.

Responsibilities:

- append immutable workflow events
- preserve per-run event ordering
- support replay and audit
- provide a durable source for failover recovery

The history substrate may be log-first internally, but product correctness is defined at the workflow-history boundary, not at the broker abstraction boundary.

### 3. Workflow Executors

Workflow executors own workflow partitions and advance runs.

Responsibilities:

- load pinned workflow artifacts
- keep hot workflow state warm on owning shards
- process workflow tasks with low latency
- evaluate compiled workflow state transitions
- schedule activities, timers, child workflows, and updates
- rebuild state by snapshot plus replay on cache miss or failover

Critical rule:

- executors run compiled workflow artifacts, not arbitrary workflow guest code, on the hot path

That is the main performance bet of `fabrik`.

### 4. Matching and Task Queues

Task queues are first-class runtime infrastructure.

The platform must support:

- workflow task queues
- activity task queues
- sticky workflow task queues
- rate limiting and backlog visibility
- poller discovery and capacity-aware routing
- at-least-once dispatch with idempotent completion handling

Matching is responsible for making work available to the right workers quickly without breaking workflow ordering guarantees.

### 5. Activity Workers

Activity workers run arbitrary user-defined activity code.

Responsibilities:

- poll activity task queues
- execute activity code in the user's runtime
- heartbeat long-running activities
- report completion, failure, and cancellation
- observe retries and timeout semantics

Activities are the general external-compute and external-I/O surface of the platform. Built-in connectors are only a convenience layer on top of the activity model.

### 6. Timer and Timeout Service

Timers are durable and first-class.

The timer subsystem handles:

- workflow sleeps
- activity schedule-to-start deadlines
- activity start-to-close deadlines
- heartbeat timeout checks
- workflow run timeouts
- retry backoff timers

No correctness-critical timeout may live only in worker memory.

### 7. Throughput Bridge

Throughput mode crosses from workflow semantics into high-volume execution through a dedicated bridge.

Responsibilities:

- accept bulk-work admission from workflow executors
- preserve idempotency for repeated admissions and retries
- fence stale workflow owners and stale stream callbacks
- translate stream-side terminal outcomes into workflow-authoritative events
- define the only legal crossing point between workflow truth and stream truth

The bridge is a protocol boundary, not a convenience helper. The workflow API remains backend-agnostic above it.

### 8. Stream Execution Subsystem

The stream-backed execution subsystem owns high-volume nonterminal throughput execution.

Responsibilities:

- own batch and stream execution state outside workflow history
- schedule and apply chunk or partition work at high volume
- checkpoint owner state and recover on failover
- expose strong owner-routed reads and cheaper eventual projections
- emit fenced terminal outcomes or named checkpoints back through the bridge

The current implementation of this subsystem is the `stream-v2` lane built from the `streams-runtime` and `streams-projector` services, Redpanda or Kafka, RocksDB, and object storage. The architecture should treat that as an implementation detail of the internal stream subsystem rather than as workflow-facing product surface.

### 9. Visibility Layer

Visibility is a primary product surface, not an afterthought.

Responsibilities:

- list and filter workflows
- index search attributes and memo-like metadata
- expose workflow execution timelines
- provide operational views over task queues and workers
- support debugging at fleet scale

The visibility model may be eventually consistent, but the product must also provide direct strong-query paths where workflow semantics require them.

### 10. Worker Versioning

Temporal parity requires safe code rollout.

The platform must support:

- worker build identifiers
- compatible version sets
- task queue routing by build compatibility
- workflow artifact pinning
- replay-safe upgrade controls

Workflow versioning and activity-worker versioning are separate concerns and both are required.

## Execution Model

### Workflow Side

Workflow SDK code compiles to deterministic artifacts with explicit operations such as:

- wait for signal
- wait for update
- run query handler
- sleep
- schedule activity
- await activity completion
- start child workflow
- await child completion
- record side effect marker
- branch and join
- continue as new
- complete, fail, cancel, or terminate

Workflow determinism remains mandatory.

### Activity Side

Activities:

- may be arbitrary code
- may use normal libraries and network clients
- may be non-deterministic
- are retried according to durable policy
- are isolated from workflow state mutation

Activity effects become real only when the corresponding completion event is durably accepted by the runtime.

### Fan-Out / Fan-In

Large fan-out / fan-in is a primary design center.

The architecture must therefore optimize for:

- efficient scheduling of many activity tasks from one workflow
- compact pending-activity state
- batched activity completion ingestion
- low-overhead join bookkeeping
- bounded replay cost through snapshots and continue-as-new

### Throughput Mode

For high-cardinality fan-out beyond what durable per-activity execution supports efficiently, workflows can opt into throughput mode using `ctx.bulkActivity()`. This trades per-item durable history for chunk-level durability, reducing workflow overhead from `O(items)` to `O(chunks)`.

Throughput mode may run with an eager execution policy. In eager mode, chunk work may execute ahead of workflow observation, but it does not directly mutate workflow state. The workflow still observes only deterministic batch barrier outcomes.

Architecturally, throughput mode is now split into:

- workflow-owned batch admission and barrier semantics
- bridge-owned idempotency, fencing, and callback translation
- stream-owned nonterminal execution, checkpoints, and progress

Backend selection remains server-controlled and must not leak into workflow code. The current stream-backed implementation is `stream-v2`, but that is below the workflow contract.

See [throughput-mode.md](spec/throughput-mode.md) for the full specification.
See [streams-bridge.md](spec/streams-bridge.md) for the bridge protocol boundary.
See [streaming-product-guide.md](streaming-product-guide.md) for the product-level operator story and [benchmarking/streaming-performance-envelope.md](benchmarking/streaming-performance-envelope.md) for the current benchmark-backed workload guidance.

### Sticky Execution

Sticky execution is required for competitive latency:

- active runs stay on the same workflow owner whenever possible
- workflow tasks should avoid full replay in the steady state
- sticky handoff failure must still remain replay-safe

### Failure and Recovery

Correctness under failure requires:

- deterministic replay from history
- durable timer rediscovery
- deduplicated task completion handling
- ownership fencing on shard handoff
- no direct workflow mutation by workers

## Why This Architecture

`fabrik` is not trying to replace Temporal by removing features.

It is trying to replace Temporal by preserving the workflow model users want while changing the runtime implementation details that most affect:

- throughput
- latency
- operational predictability
- replay efficiency

That means compiled workflows plus arbitrary activities, not compiled workflows instead of arbitrary activities.

### 11. Throughput Runtime (`stream-v2` Implementation)

`streams-runtime` is the current implementation of the internal stream execution subsystem for active throughput runs. `unified-runtime` remains the workflow barrier owner, but only through the bridge for admission and terminal resume.

The intended `stream-v2` split is:

- workflow-side bridge logic validates a bulk step, persists minimal throughput admission plus workflow resume metadata, and emits `StartThroughputRun`
- `streams-runtime` owns chunk planning, worker leasing, retries, reducer state, cancellation, checkpointing, and terminalization
- `streams-runtime` returns exactly one terminal run outcome through the bridge
- workflow-side bridge logic appends the authoritative workflow barrier event and resumes the workflow turn

The throughput runtime therefore:

- owns partition-local hot state with durable checkpoints
- consumes throughput run commands and worker reports
- serves dedicated bulk worker RPCs for chunk polling and reporting
- enforces fencing via `(chunk_id, attempt, lease_epoch, owner_epoch)` tuples
- materializes visibility asynchronously instead of depending on SQL rows for execution

Per-chunk projection tables are transitional compatibility machinery, not the long-term architecture. The long-term architecture keeps the bridge as the protocol boundary while allowing the underlying stream implementation to evolve independently.

## Future Directions

### Wasm-Sandboxed Activity Execution

The current architecture runs activities as arbitrary user code in worker processes. This is correct for Temporal parity and for the broadest possible activity ecosystem.

However, multi-tenant hosted deployments may benefit from an optional sandboxed activity execution mode using Wasm (Wasmtime / WASI):

- tenant-uploaded activity modules could run under resource limits without dedicated worker infrastructure
- sandboxed execution would provide stronger isolation than process-level separation alone
- Wasm modules are versionable, content-addressable artifacts that fit naturally with the existing artifact-pinning model
- language flexibility through compilation targets (Rust, Go, C, AssemblyScript, etc.)

This would not replace the general activity worker model. It would be an additional execution mode available for tenants or activity types where sandboxing is more valuable than full runtime flexibility.

Relevant design questions for a future Wasm extension:

- host function surface: what capabilities should sandboxed activities be able to access?
- resource limits: CPU time, memory, and network access controls per tenant
- artifact registry: versioned Wasm module storage and deployment
- integration with worker versioning: how sandboxed modules interact with build-ID routing
