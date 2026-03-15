# Streams State Model

## Status

This document defines the authoritative state, durability, and query contract for the internal stream subsystem that powers `stream-v2` today and future `Fabrik Streams` execution later.

It sits below the workflow-facing contract in [stream-jobs.md](/Users/bene/code/fabrik/docs/spec/stream-jobs.md), alongside the bridge contract in [streams-bridge.md](/Users/bene/code/fabrik/docs/spec/streams-bridge.md), and underneath the broader execution contract in [streams-runtime-model.md](/Users/bene/code/fabrik/docs/spec/streams-runtime-model.md).

## Purpose

The stream runtime already has a bridge contract and an execution model.

What must now be frozen separately is the state contract:

- what state is authoritative
- where hot state lives
- what makes progress durable
- how checkpoints work
- which reads are strong versus eventual
- how large values, retention, and projections are handled

This document exists to remove ambiguity around the stream source of truth.

The first concrete implementation shape for that contract is frozen in [streams-state-implementation-v1.md](/Users/bene/code/fabrik/docs/spec/streams-state-implementation-v1.md).

## Core Rule

Materialized state is not a table we happen to update.
It is partition-owned durable state with query surfaces layered on top.

That means:

- the active partition owner holds the authoritative hot state
- durable recovery comes from checkpoint plus durable progress tail
- projections are never the execution substrate
- strong reads route to the active owner
- eventual reads come from derived read models
- workflow-visible milestones become workflow-authoritative only through the bridge

## Scope

This document defines:

- authoritative state categories
- the first-class storage layout for active execution
- the durability and restore contract
- the query contract for materialized state
- retention, compaction, and large-value policy

It does not define:

- the full stream authoring SDK
- every future operator type
- every future source and sink
- UI layout
- external SQL or declarative authoring surfaces

## Runtime Shape

The target runtime shape is:

- Rust shard-workers
- one active owner per partition
- one embedded local key-value store per shard-worker
- binary-encoded state keys on the hot path
- append-only durable progress capture
- periodic compact checkpoints
- separate eventual projections for browsing and dashboards

The first implementation is expected to use RocksDB for hot state, object storage for checkpoint artifacts and large spilled values, and an append-only durable progress log for post-checkpoint recovery.

## State Categories

The runtime distinguishes four state categories.

### 1. Operator Execution State

Internal state required to advance the job graph:

- reducer accumulators
- join buffers
- dedupe caches
- timer wheels or timer indexes
- watermark state
- retry and cancellation state
- routing and branch-local progress markers

Properties:

- authoritative for execution
- owned by the active partition owner
- not automatically user-queryable
- checkpointed and replayable

### 2. Window State

State scoped to a logical key plus a window or frontier:

- tumbling window aggregates
- hopping or sliding window buckets
- session state
- watermark-driven close markers
- count-based or custom window state

Properties:

- authoritative for execution
- retained only according to declared close and TTL rules
- may feed user-visible materialized views
- must restore with enough frontier metadata to resume correctly

### 3. Materialized Views

Explicit user-facing state derived from stream execution:

- keyed aggregates
- keyed current summaries
- fraud scores
- anomaly snapshots
- rollups
- leaderboard entries

Every materialized view must declare:

- a stable `view_id` or equivalent identity
- key shape
- value shape or schema
- supported read concerns
- retention policy
- whether large payload pages may spill out of the hot store

Materialized views are authoritative only when read from the active owner.
Projected copies remain eventual.

### 4. Diagnostic Projections

Operational state derived for UI, debugging, and fleet operations:

- lag
- backlog
- hot keys
- checkpoint freshness
- state size
- failure counters
- recent ownership changes

Properties:

- never authoritative for restore
- may be rebuilt from checkpoints and progress history
- may lag owner state
- should be labeled clearly as diagnostic or eventual

## Authoritative Hot Store

### Ownership Rule

The authoritative hot store is local to the active shard-worker for the owned partitions.

Rules:

- only the active owner may mutate authoritative hot state
- stale owners may read their old local copy during shutdown or repair, but they cannot make forward progress once fenced out
- strong reads must route to the active owner, not an arbitrary replica or projection

### Store Shape

The hot store exists for execution locality, not for general browsing.

The first implementation should optimize for:

- point lookup by key
- prefix iteration by partition, operator, or view
- batched writes
- compact checkpoint export
- bounded per-owner restore time

The initial storage substrate is expected to be RocksDB.

### Column Family Guidance

The first implementation should separate state by access pattern, not by marketing surface.

Suggested families:

- `runs`
- `operators`
- `windows`
- `timers`
- `views`
- `idempotency`
- `checkpoint_meta`
- `diagnostics`

This list is guidance, not a wire contract.
The semantic contract is that state with different mutation and scan patterns must not be forced into one undifferentiated keyspace if that harms locality, compaction, or checkpointing.

### Key Layout Rule

Hot-store keys must be prefix-structured and binary-encoded around execution identity.

Typical dimensions:

- `partition_id`
- `job_id` or `stream_run_id`
- `operator_id`
- `logical_key`
- `window_id`
- `view_id`
- `timer_at` or timer bucket identity

Examples:

- `(partition_id, operator_id, logical_key)`
- `(partition_id, operator_id, logical_key, window_id)`
- `(partition_id, view_id, logical_key)`

The runtime must not depend on row-per-event SQL modeling or ad hoc text keys for authoritative active state.

## Large Value Policy

The hot store is for hot state, not blob retention.

Rules:

- small values may be stored inline
- medium values may be stored inline in compact binary form
- large result pages, member sets, payload collections, or replay artifacts should spill to object storage or another blob substrate
- the hot store retains handles, manifests, integrity hashes, sizes, and small summaries for spilled values

Consequences:

- checkpoints stay compact
- restore cost remains bounded
- scans over hot state do not degrade because of giant values

## Durability Model

### Recovery Source of Truth

Recovery is defined as:

1. latest durable checkpoint
2. plus the durable progress tail after that checkpoint

Neither projections nor transient in-memory state are authoritative for recovery.

### Durable Progress Rule

Accepted progress must never exist only in RAM.

A state transition becomes durable only when the runtime has appended a fenced progress record carrying enough identity for replay and dedupe.

Minimum identity carried by a durable progress record:

- `partition_id`
- `job_id` or `stream_run_id`
- `owner_epoch`
- work identity such as `operator_id`, `chunk_id`, or equivalent
- `attempt`
- `lease_epoch` when work was externally leased
- `report_id` or another idempotency key
- summary of the applied mutation
- payload or manifest handle if the value spilled out of line

### Progress Record Semantics

The durable progress log must be append-only from the runtime point of view.

Records may represent:

- admission or execution planned
- state delta applied
- materialized view updated
- timer scheduled or closed
- checkpoint reached
- terminalized
- cancellation accepted

The exact internal record schema may evolve, but replay must be able to reconstruct authoritative owner state after the last checkpoint without consulting projections.

### Write Ordering Contract

For externally reported work, the logical write order is:

1. validate owner fencing and idempotency
2. append the durable progress record
3. apply the local state mutation
4. expose the new state to strong reads
5. acknowledge durable acceptance

The implementation may batch and pipeline these steps, but it must preserve the same guarantee:

- no acknowledged progress without durable capture
- no strong read that claims a newer durable state than restore can reconstruct

### Checkpoint Contract

Checkpoints are compact durable snapshots, not per-event visibility writes.

Each checkpoint must capture enough state to restore efficiently, including:

- `partition_id` or shard-worker partition set
- checkpoint sequence or timestamp identity
- owner epoch lineage
- keyed operator state
- window state
- timer frontier state
- materialized view state required for authoritative reads
- durable progress offset included in the checkpoint
- retention and spill-manifest references needed for later cleanup

### Checkpoint Restore

Restore proceeds as:

1. load the latest valid checkpoint
2. restore authoritative hot state
3. replay the durable progress tail after the checkpoint offset
4. reject stale records fenced by older owner or lease epochs
5. reopen strong reads only after replay reaches the current durable tail

If no checkpoint exists yet, restore starts from an empty authoritative state plus the full durable progress history for the job or partition.

### Ownership Handoff

Ownership handoff must not depend on local files from the old owner.

The new owner restores from shared durable state:

- checkpoint artifact
- durable progress tail
- current ownership fence

Projections may lag or rebuild after handoff.
That does not affect authoritative execution.

## Query Contract

### Strong Reads

Strong reads:

- route to the active partition owner
- read authoritative hot state
- may observe recent nonterminal owner state
- must carry the owner epoch or another consistency source marker when available
- are suitable for correctness-sensitive reads, bridge operations, and operator inspection

Strong reads are the only authoritative way to read current materialized state.

### Eventual Reads

Eventual reads:

- come from projections or derived read models
- may lag owner state
- may be rebuilt asynchronously
- are suitable for UI listings, dashboards, scans, and cheap browsing

Eventual reads must expose:

- `consistency: "eventual"`
- a freshness marker such as latest projected checkpoint or projection lag
- the projection source when relevant

### Workflow Interop Rule

Workflows may inspect stream state through the bridge, but stream reads do not become workflow-authoritative just because a workflow issued them.

Only accepted bridge events such as terminal outcomes or awaited checkpoints may advance workflow history.

### Scan Rule

Not every materialized view is required to support every query shape.

The runtime should distinguish:

- owner-routed point lookup
- owner-routed bounded prefix scan
- eventual projection scan
- diagnostic scan

Views should declare which modes they support rather than pretending every view is a globally browsable table.

## Projection Model

The projection layer exists for eventual reads and operator surfaces.

Properties:

- projections may be stored in Postgres or another derived read store
- projections are fed asynchronously from durable progress or owner-emitted projection deltas
- projections may be dropped and rebuilt
- projection writes must never sit on the hot path required for authoritative mutation

The first implementation should strongly prefer:

- authoritative owner state in RocksDB
- projections in Postgres

This keeps query convenience separate from execution truth.

## Retention And Compaction

Every stateful stream job must define retention for the state it creates.

The runtime must support:

- keyed-state TTL when semantically valid
- window expiration
- checkpoint retention count or age limits
- progress-log truncation only after a newer checkpoint safely covers the same state
- spilled-object manifest cleanup after all referencing checkpoints and views expire
- projection rebuild or pruning independently of authoritative state retention

Retention must never violate restore correctness for the advertised durability window.

## Non-Goals

This document does not promise:

- magical exactly-once behavior for arbitrary external sinks
- globally consistent scans over all live owner state
- SQL as the primary execution substrate
- direct workflow authority over stream-local state
- one universal storage shape for every query pattern

## Consequence

The stream subsystem can now be reasoned about with one clean sentence:

authoritative execution state lives with the active Rust partition owner in a local embedded store, durability comes from checkpoint plus durable progress tail, strong reads route to the owner, and every SQL-like or dashboard-oriented view is explicitly layered on top as a projection.
