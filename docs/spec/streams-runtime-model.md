# Streams Runtime Model

## Status

This document defines the target runtime contract for the internal stream subsystem that powers `stream-v2` today and future `Fabrik Streams` execution later.

It is intentionally below the workflow-facing contract in [stream-jobs.md](/Users/bene/code/fabrik/docs/spec/stream-jobs.md) and alongside the bridge contract in [streams-bridge.md](/Users/bene/code/fabrik/docs/spec/streams-bridge.md).
The first concrete compiler/runtime slice that implements this model is frozen in [streams-kernel-v1.md](/Users/bene/code/fabrik/docs/spec/streams-kernel-v1.md).

## Purpose

This document freezes the execution model that makes stream execution a first-class subsystem rather than a renamed throughput backend.

The runtime model must support:

- partition-owned execution
- keyed state and window state
- checkpoints plus durable progress tail recovery
- strong owner-routed reads
- eventual projected reads
- workflow interop through the bridge without collapsing stream semantics into workflow semantics

The authoritative storage and durability details for that model are frozen separately in [streams-state-model.md](/Users/bene/code/fabrik/docs/spec/streams-state-model.md).

## Scope

This is the semantic contract for the stream runtime itself.

It covers:

- ownership and failover
- authoritative local state
- durable recovery
- query surfaces
- execution-kernel rules

It does not define:

- the full workflow-facing SDK surface
- a SQL authoring model
- every future source and sink
- UI layout

## Core Rule

Authoritative stream state is partition-owned durable state with query surfaces layered on top.

That means:

- live execution state belongs to the active stream owner for a partition
- durable recovery comes from checkpoints plus durable progress after the checkpoint
- strong reads route to the active owner
- eventual reads may use projections
- projections are not the execution substrate
- workflow-visible milestones become workflow-authoritative only when the bridge accepts them

## Runtime Entities

- `stream job`:
  top-level deployed stream program with stable identity, version, sources, operators, state definitions, and outputs
- `stream partition`:
  routing and ownership unit for stream execution
- `shard-worker`:
  the active runtime owner for one set of stream partitions
- `operator`:
  one computation stage in the job graph
- `keyed state cell`:
  authoritative value scoped to `(job, operator, logical_key[, window])`
- `materialized view`:
  explicitly declared queryable stream output
- `checkpoint`:
  durable compact snapshot boundary for one partition or shard-worker
- `durable progress record`:
  append-only record that preserves accepted progress after the latest checkpoint
- `owner epoch`:
  fencing generation for partition ownership
- `lease epoch`:
  fencing generation for externally leased work within one owner epoch

## Ownership Model

### Partition Ownership

- each stream partition has exactly one active owner at a time
- ownership changes increment `owner_epoch`
- any state mutation must be fenced by the active `owner_epoch`
- stale owners may still emit messages transiently, but they cannot make forward progress once fenced out

### Shard-Worker Shape

The default runtime shape is:

- one shard-worker owns a set of partitions
- one shard-worker executes partition-local mutation in a single-owner apply loop
- one shard-worker holds the authoritative hot state for its owned partitions

This model exists to preserve locality, reduce contention, and keep ownership legible during failover and debugging.

### Lease Model

If work is leased to external activity workers or execution workers:

- each lease carries `attempt`, `lease_epoch`, `lease_token`, and `owner_epoch`
- retries advance `lease_epoch`
- owner failover invalidates leases from prior `owner_epoch` values
- stale reports are side-effect free for authoritative mutation

### Handoff and Restore

On ownership loss:

- the old owner stops accepting new mutation work for that partition
- the new owner restores from the latest durable checkpoint plus the post-checkpoint durable progress tail
- the new owner becomes the only legal target for strong reads
- projections may lag during handoff, but correctness does not depend on them

## State Model

### Authoritative Hot State

The authoritative hot state for active stream execution lives in a local embedded key-value store owned by the shard-worker.

The first implementation may use RocksDB directly, but the contract is semantic:

- hot state is local to the active owner
- hot state is keyed by binary routing identity, not ad hoc SQL rows
- hot state is optimized for keyed lookup, prefix iteration, and checkpoint export

### State Categories

The runtime distinguishes four state categories.

#### 1. Operator Execution State

Internal state required to advance the job:

- reducer accumulators
- join buffers
- dedupe caches
- watermark state
- retry state
- timer state

This state is authoritative for execution but not necessarily directly queryable by users.

#### 2. Window State

State scoped to one key and one window or timer frontier:

- tumbling window state
- hopping or sliding window state
- session state
- watermark-driven closure metadata

Window state must expire according to the job's retention and close semantics.

#### 3. Materialized Views

Explicit user-facing state derived from stream execution:

- current aggregates
- keyed summaries
- anomaly scores
- rollups
- live leaderboards

Every materialized view must declare:

- schema or value shape
- retention policy
- queryability contract
- consistency modes it supports

#### 4. Diagnostic Projections

Derived operational state for UI and debugging:

- lag
- hot keys
- state size
- failure counters
- checkpoint freshness

Diagnostic projections are useful, but they are not authoritative restore state.

### Large Value Rule

The authoritative hot store is for hot execution state, not arbitrary blob storage.

Rules:

- small and medium state values may be stored inline
- large result pages, member sets, or payload collections should be externalized to object storage or another blob substrate
- the hot store should retain handles, manifests, integrity metadata, and small summaries rather than giant payload bodies

### State-Key Rule

State keys must be prefix-structured and binary-encoded around execution identity such as:

- `partition_id`
- `job_id`
- `operator_id`
- `logical_key`
- `window_id`
- `view_id`

The runtime must not depend on ad hoc text keys or SQL row-per-event modeling for active execution state.

## Durability Model

### Recovery Source of Truth

Recovery is defined as:

- latest durable checkpoint
- plus the durable progress tail after that checkpoint

Neither projections nor transient in-memory state are sufficient for restore correctness.

### Durable Progress Rule

Accepted progress must never exist only in RAM.

A state transition becomes authoritative only after the runtime durably captures a progress record that is fenced to the active owner and lease context.

Each durable progress record must carry enough identity to make replay idempotent, including:

- `partition_id`
- `job_id` or `stream_run_id`
- `operator_id` or work-unit identity as applicable
- `attempt`
- `lease_epoch`
- `owner_epoch`
- `report_id` or equivalent idempotency identity
- result summary
- payload handle when payloads are externalized

### Write Ordering Contract

For externally reported work, the authoritative write order is:

1. validate fencing and idempotency against active ownership
2. durably append the progress record
3. apply the corresponding local state mutation
4. expose the new state to strong reads
5. acknowledge the work as durably accepted

The implementation may batch these operations, but it must preserve the same logical guarantee:

- no acknowledged progress without durable capture
- no restore path that depends on projections

### Checkpoint Contract

Checkpoints are compact durable snapshots, not per-event visibility writes.

Each checkpoint must capture enough state to resume efficiently, including:

- owner epoch lineage
- keyed operator state
- window and timer frontier state
- materialized view state needed for authoritative reads
- progress position included in the checkpoint
- metadata needed to resume checkpoint sequencing and compaction

Checkpoint cadence should be coarse and batch-oriented:

- periodic by time
- periodic by bytes or event volume
- forced on graceful ownership drain when needed

### Compaction Rule

Checkpoints may compact older durable progress, but compaction must preserve:

- replay idempotency
- owner-epoch fencing correctness
- monotonic checkpoint sequencing
- integrity of materialized state

## Query Model

### Read Concerns

Streams must expose consistency explicitly as a read concern.

Supported read concerns:

- `strong`
- `eventual`

### Strong Reads

Strong reads:

- route to the active owner for the requested partition or key
- read authoritative owner state
- may observe nonterminal execution state
- are suitable for correctness-sensitive inspection and bridge-coordinated reads

Strong reads are not automatically workflow-authoritative.

They become workflow-authoritative only if a separate bridge-defined primitive accepts their result as a durable workflow milestone.

### Eventual Reads

Eventual reads:

- may use projected read models
- may lag owner state
- are suitable for UI, browsing, and cheap scans
- must expose freshness or lag metadata where possible

For the current stream-job path, eventual reads should expose projection metadata explicitly, including:

- latest projected checkpoint sequence
- latest projected update time when known
- latest projected delete checkpoint or delete time when tombstone state exists
- owner checkpoint sequence when known
- lag between the owner checkpoint and the projected state

Eventual reads are never authoritative for restore and never replay-stable workflow input.

Projected rows must also obey monotonic ordering.

That means:

- a stale upsert must not overwrite a newer projected row
- a newer delete must prevent stale row resurrection
- explicit rebuild after projection loss must restore retained rows and delete stale projected rows deterministically

### Query Surface Types

The runtime should distinguish at least these query shapes:

- `get(key)`:
  owner-routed keyed lookup
- `scan(view, cursor)`:
  paginated browsing, usually eventual
- `summary(view)`:
  aggregate or metadata summary, strong or eventual depending on declaration
- `bridge output`:
  narrow stream result that workflows are allowed to observe through the bridge

The runtime must not pretend all materialized state is one table with one consistency model.

### Runtime Diagnostics

The runtime must also expose an operator-facing diagnostics query for live stream jobs.

At minimum, that diagnostics surface should expose:

- source cursor state and lease state
- checkpoint progress and latest accepted checkpoint boundary
- declared materialized views and their retention policy
- per-view policy for materialized outputs, including retention and late/evicted handling
- event-time window policy such as mode, size, time field, and allowed lateness
- explicit late-event policy
- explicit post-retention eviction policy

That diagnostics payload exists so operators can answer both:

- what happened?
- what policy caused it?

The diagnostics surface should support both:

- job-level runtime stats
- per-view runtime stats for one named materialized view
- job-level bridge state for lifecycle, awaited checkpoints, query acceptance, and repair debt
- job-level signal bridge state for `signal_workflow` emissions, including callback dedupe ids, target workflow ids, and queue/consumption status

Per-view runtime stats should expose view-local freshness and lag markers where available, such as:

- latest materialized update time
- checkpoint sequence lag relative to the owner
- event-time watermark lag
- closed-window lag

When a view supports eventual reads, the same per-view runtime stats surface should also expose projection-side summary and freshness markers for that view so operators can compare owner-local state to the projected read model without switching tools.

The same principle should hold for operator-facing strong browse surfaces: strong scans, key listings, and entry listings may include projection-side summary and lag metadata for the browsed view so browsing current rows does not require a second call just to inspect eventual-read health.

Per-view runtime stats should also expose retention history where available, such as:

- current stored or active key counts
- historical evicted-window counts for that view
- latest per-view eviction boundary and eviction time

The debug/operator surface should also support targeted projection rebuild for one materialized view in addition to whole-job projection rebuild.

The regular stream query surface should also expose a compact per-view projection-stats query so workflows can inspect eventual-read freshness and lag through the bridge without using debug endpoints.

For job-level projection stats, the query surface should also support basic filtering such as stale-only views, minimum checkpoint lag, and explicit view-name subsets so workflows can ask which projections need attention without client-side filtering.

For long-lived topic-backed jobs, the runtime should also support explicit lifecycle control operations:

- pause
- resume
- cancel

Those control operations must be idempotent at the bridge boundary and must not mutate authoritative stream state through any path other than the active owner.

The same owner-side lifecycle machinery should serve both:

- workflow-started stream jobs
- standalone direct jobs
- deployment-backed standalone revisions

## Execution Kernel Model

### Authoring and Execution Split

The preferred execution shape is:

- code-first authoring
- compiled stream IR
- Rust-native execution kernels

This keeps the developer surface expressive without forcing arbitrary user-language runtimes into the hot path.

### Built-In Operator Bias

The first-class operator set should favor built-in kernels such as:

- map
- filter
- route
- keyBy or partitionBy
- reduce or aggregate
- window
- join
- dedupe
- threshold or emit
- materialize
- signal workflow or emit checkpoint through the bridge

### User-Defined Logic

If custom user logic is supported, the runtime must distinguish clearly between:

- fast-path built-in kernels
- constrained extension points

The hot path must not assume unrestricted arbitrary user code execution.

### Hot-Loop Rules

The stream runtime should preserve these hot-loop properties:

- partition-local mutation happens under single-owner execution
- batching exists at admission, apply, logging, and projection boundaries
- binary payloads and handles are preferred over JSON on the hot path
- SQL and other external query stores are outside the execution-critical mutation path

## Workflow Interop Boundary

The stream runtime may coordinate with workflows only through the bridge contract.

This means:

- workflows admit stream work through bridge-defined requests
- awaited checkpoints must be durable and monotonic before they are bridge-accepted
- strong reads used by workflows remain non-authoritative until a bridge-defined primitive accepts them
- terminal stream outcomes become workflow-visible only through bridge acceptance

The stream runtime must not mutate workflow history directly.

## Non-Goals

This runtime model does not require:

- SQL as a primary stream authoring surface
- arbitrary external sinks to be exactly-once by magic
- projections to be restore-authoritative
- long-lived stream jobs to be disguised as throughput batches
- every internal state transition to be synchronously visible in SQL

## Consequence

If this contract holds, `stream-v2` can evolve from a throughput-focused implementation into the first real version of `Fabrik Streams` without weakening workflow semantics.

The important split stays clean:

- `Fabrik Workflows` owns workflow truth
- the bridge owns legal crossing points
- `Fabrik Streams` owns high-volume stateful execution
