# Architecture

## Overview

`fabrik` is a log-first workflow engine. Durable events are the source of truth. Rust services consume those events, apply workflow semantics, and emit new events that describe state transitions, timers, retries, side effects, version markers, and execution rollover.

The architecture is designed for:

- very high event throughput
- ordered processing within workflow instance boundaries
- deterministic replay
- code-authored workflow ergonomics
- safe multi-tenant customization
- operational recovery through log replay and snapshots

This document is an overview. The canonical semantic contracts live in [docs/spec/README.md](/Users/bene/code/fabrik/docs/spec/README.md) and the execution invariants ADR set.

## High-Level Topology

```text
                         +--------------------+
                         |  Control Plane API |
                         +---------+----------+
                                   |
                                   v
+----------------+       +---------+----------+       +------------------+
| External Inputs| ----> | Ingest / Validation | ---> | Redpanda Topics  |
+----------------+       +--------------------+       +--------+---------+
                                                           |
                                                           v
                                                 +---------+----------+
                                                 | Sharded Executors  |
                                                 +----+----------+----+
                                                      |          |
                                                      |          v
                                                      |   +------+------+
                                                      |   | Timer Service|
                                                      |   +------+------+
                                                      |          |
                                                      v          |
                                              +-------+------+   |
                                              | Snapshot Store|  |
                                              +-------+------+   |
                                                      |          |
                                                      v          v
                                                +-----+----------+----+
                                                | Read Models / Query |
                                                +---------------------+

                                                      |
                                                      v
                                                +-----+-----------+
                                                | Connector Workers|
                                                +------------------+
```

## Core Components

### 1. Ingest Plane

The ingest plane accepts:

- trigger events
- workflow definitions
- external callbacks and signals
- administrative commands

Responsibilities:

- authenticate and authorize tenants
- validate payloads against schemas
- assign routing keys
- write canonical events to Redpanda

The ingest plane does not execute workflow logic. It normalizes inputs and commits them to the durable log.

### 2. Durable Event Backbone

Redpanda is the event substrate.

Responsibilities:

- durable append-only event storage
- partitioning for shard-local ordering
- replication and retention
- replay for recovery and debugging

Partitioning strategy for workflow events:

- primary key: `tenant_id`
- secondary key: `instance_id`
- execution epoch key: `run_id`
- partition hash input: `tenant_id + instance_id + run_id`

This preserves ordering for a given workflow instance while allowing broad horizontal scale.

### 3. Sharded Workflow Executors

Executors are Rust services that own partitions and apply workflow semantics.

Responsibilities:

- consume events for assigned partitions
- keep hot workflow state in memory for active executions
- reconstruct workflow state from snapshot + event tail on cache miss or reassignment
- evaluate compiled workflow IR / state machine artifacts
- emit new workflow events
- schedule timers
- coordinate step execution and retries

Each executor is deterministic with respect to the event history it consumes. That property is required for replay and auditability.

Hot-state ownership is explicit:

- shard owners keep active executions warm in memory
- cache eviction is a deliberate policy surface
- replay is the recovery path, not the steady-state execution path

### 4. Timer Service

Timers are first-class workflow concepts, not ad hoc sleeps in worker memory.

Responsibilities:

- persist due times as durable records
- emit `TimerFired` events when deadlines are reached
- recover cleanly after restarts

Implementation direction:

- start with a persistent timer table indexed by due time
- later evolve to partition-aware timer wheels backed by durable state

### 5. Snapshot Store

Snapshots accelerate recovery and reduce replay cost.

Responsibilities:

- persist compact workflow state snapshots
- store versioned execution metadata
- support replay from snapshot boundaries

Initial choice:

- PostgreSQL for snapshots and control-plane metadata

Constraints:

- snapshots are an optimization only
- the event log remains authoritative

### 6. Connector Workers

Connector workers perform side effects against external systems.

Responsibilities:

- receive effect requests from workflow events
- apply idempotency keys
- handle retries and backoff
- emit success or failure events back into the log
- preserve connector response metadata in result payloads when available

Rules:

- no connector success is assumed until confirmed by an emitted result event
- no workflow state is mutated directly by a connector
- connector result events should carry enough response detail for debugging without reopening the external system

### 7. Query Plane

The query plane exposes current workflow state and execution history.

Responsibilities:

- serve workflow instance state
- serve workflow execution timelines
- power debugging and replay tooling

Read models are projections and may lag slightly behind the event log.

### 8. Wasm Runtime

Tenant-defined logic runs in Wasmtime.

Supported use cases:

- event transforms
- branching predicates
- lightweight step handlers
- data mapping and normalization

Why Wasm:

- sandboxing
- resource controls
- language flexibility via compilation targets
- versionable executable artifacts

## Execution Model

### Workflow Authoring Model

Public authoring and internal execution intentionally use different models.

Public model:

- workflows are authored in SDK code
- the SDK exposes deterministic workflow primitives instead of raw host APIs
- external I/O happens through activities or connectors, not direct host calls

Internal model:

- authored workflows compile to deterministic workflow IR / state machine artifacts
- executors run compiled artifacts, not arbitrary guest code as the hot path
- the compiled artifact is the replay and inspection boundary

This split is the core architecture choice behind `fabrik`: code-authored workflows for ergonomics, explicit execution artifacts for throughput and determinism.

### Workflow Execution Primitives

The SDK and compiler must lower workflow code into explicit operations such as:

- wait for signal
- durable timer sleep
- invoke activity / connector
- invoke predicate or transform
- branch on prior result
- emit marker
- continue as new
- complete or fail

Non-deterministic behavior must go through runtime APIs instead of raw host calls. Examples:

- `ctx.sleep()` instead of host timers
- `ctx.now()` instead of wall-clock reads
- `ctx.uuid()` instead of direct UUID generation
- `ctx.side_effect()` for controlled one-time non-deterministic evaluation
- `ctx.signal()` / message handlers for inbound workflow interaction

### Workflow Definition Model

Workflows are compiled into explicit state machines, not free-form imperative scripts.

A workflow may contain:

- event waits
- timers
- step invocations
- retry policies
- forks and joins
- compensations
- external signals

This makes workflow progress durable and inspectable.

### Artifact Pinning

Every running workflow instance must be pinned to the exact execution artifact that started it.

Persisted history and instance state should record at least:

- `definition_version`
- `artifact_hash`
- optional SDK / compiler version

New workflow code must never silently take over a running execution. Artifact rollout rules are part of the correctness model.

### Event Types

Initial canonical event families:

- `WorkflowDefined`
- `WorkflowTriggered`
- `WorkflowStarted`
- `WorkflowArtifactPinned`
- `StepScheduled`
- `StepStarted`
- `StepCompleted`
- `StepFailed`
- `TimerScheduled`
- `TimerFired`
- `SignalReceived`
- `MarkerRecorded`
- `WorkflowContinuedAsNew`
- `CompensationTriggered`
- `WorkflowCompleted`
- `WorkflowFailed`

All state transitions should be representable by events in this family or approved extensions.

### State Reconstruction

For each workflow instance:

1. load the latest snapshot if present
2. read the event tail after the snapshot offset
3. replay events deterministically into the state machine
4. apply the next command generated by the executor
5. emit resulting events

In steady state, executors should usually advance workflows from hot in-memory state. Full replay is the fallback for cold start, failover, cache eviction, or explicit validation.

### History Rollover

High-throughput and long-lived workflows must not accumulate unbounded history in one execution epoch.

`fabrik` should support a `ContinueAsNew`-style rollover mechanism that:

- closes the current execution epoch cleanly
- starts a fresh epoch with carried-forward logical state
- preserves logical workflow identity while resetting replay cost
- keeps history segments explicit for audit and debugging

### Signal Semantics

Signal handling is part of the replay contract, not just an SDK convenience.

The runtime must define:

- mailbox ordering guarantees
- whether signals are strictly serialized or may interleave at suspension points
- how signal handlers interact with the main workflow body
- how signal delivery is represented in history

## Correctness Model

`fabrik` is explicitly designed around at-least-once processing.

Correctness mechanisms:

- idempotent connector calls
- dedupe keys on externally visible effects
- deterministic executor logic
- immutable event histories
- outbox-style side effect requests
- explicit acknowledgement events
- version markers and artifact pinning
- replay validation against real histories

Non-goal:

- distributed exactly-once semantics across third-party APIs

## Replay Tooling

Replay is not only a recovery mechanism. It is also a deployment safety tool.

Required capabilities:

- replay a captured workflow history against a chosen artifact version
- detect determinism divergence before production rollout
- explain replay mismatches with event-by-event diagnostics
- run replay validation in CI for representative histories

## Multi-Tenancy

Multi-tenancy is enforced at several layers:

- tenant-scoped auth in the ingest and query planes
- tenant-aware partition routing
- per-tenant quotas for execution and Wasm resources
- tenant-isolated workflow definitions and secrets

Later enhancements may include dedicated executor pools for noisy-neighbor isolation.

## Scalability Strategy

### Horizontal Scale

The primary scaling unit is the partition.

Scale levers:

- increase Redpanda partitions
- increase executor replicas
- rebalance partition ownership
- separate connector pools by integration type
- isolate hot tenants into dedicated shards

### Backpressure

Backpressure is mandatory for overload safety.

Mechanisms:

- bounded executor work queues
- connector concurrency limits
- ingest throttling by tenant
- delayed retry scheduling instead of hot-loop retries

## Observability

Required signals from the first implementation:

- per-partition lag
- hot-state cache hit ratio
- executor replay time
- timer drift
- connector success/failure rates
- workflow completion latency
- continue-as-new frequency
- replay divergence rate in CI
- Wasm execution duration and memory usage

Tracing should preserve:

- tenant id
- workflow id
- workflow instance id
- partition id
- event id

## Security Posture

Security assumptions:

- all external inputs are untrusted
- tenant-defined Wasm modules are untrusted
- connector credentials are sensitive and isolated

Requirements:

- strict schema validation at ingress
- resource-limited Wasm execution
- secret storage outside workflow definitions
- audit trail for definition and deployment changes

## Suggested Initial Service Split

For the first implementation:

- `api-gateway`
- `ingest-service`
- `executor-service`
- `timer-service`
- `connector-service`
- `query-service`
- shared crates for events, workflow model, storage, and Wasm runtime

This is enough to prove the architecture without premature microservice sprawl.
