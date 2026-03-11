# Architecture

## Overview

`fabrik` is a log-first durable execution platform designed to replace Temporal at high scale.

The product keeps Temporal's workflow and activity semantics, but the runtime is organized around:

- compiled workflow artifacts for deterministic workflow turns
- task queues for workflow and activity dispatch
- shard-local workflow ownership with sticky execution
- durable history as the source of truth
- worker fleets that run arbitrary user activity code

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
                +---+-----------+--+
                    |           |
                    |           v
                    |    +------+------+
                    |    | Timer /     |
                    |    | timeout svc |
                    |    +-------------+
                    |
                    v
             +------+--------+
             | Matching /    |
             | Task Queues   |
             +---+--------+--+
                 |        |
                 |        v
                 |   +----+------------------+
                 |   | Activity Workers      |
                 |   | arbitrary user code   |
                 |   +-----------------------+
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

### 7. Visibility Layer

Visibility is a primary product surface, not an afterthought.

Responsibilities:

- list and filter workflows
- index search attributes and memo-like metadata
- expose workflow execution timelines
- provide operational views over task queues and workers
- support debugging at fleet scale

The visibility model may be eventually consistent, but the product must also provide direct strong-query paths where workflow semantics require them.

### 8. Worker Versioning

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
