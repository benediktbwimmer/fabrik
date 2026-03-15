# Streams State Implementation v1

## Status

This document defines the first concrete implementation shape for the stream state model in the current Rust-backed stream subsystem.

It is the implementation companion to:

- [streams-state-model.md](/Users/bene/code/fabrik/docs/spec/streams-state-model.md)
- [streams-runtime-model.md](/Users/bene/code/fabrik/docs/spec/streams-runtime-model.md)
- [streams-kernel-v1.md](/Users/bene/code/fabrik/docs/spec/streams-kernel-v1.md)

## Purpose

The state model freezes semantics.
This document freezes the first intended implementation shape.

The goal is to make the current `stream-v2` path convergent around one concrete storage plan:

- RocksDB for authoritative hot state
- append-only changelog records for durable post-checkpoint progress
- checkpoint artifacts for compact restore
- owner-routed strong reads
- async projected eventual reads

## Current Scope

This implementation contract is intentionally narrow.

It covers:

- the current Rust runtime
- the `stream-v2` backend
- the first stream-job kernel shape in [streams-kernel-v1.md](/Users/bene/code/fabrik/docs/spec/streams-kernel-v1.md)
- bounded keyed rollup execution and materialized keyed reads

It does not yet attempt to freeze:

- general joins and windows
- arbitrary user-defined operators
- multi-view scan-heavy jobs
- standalone deployment packaging

## Storage Plan

The first implementation has three storage layers.

### 1. Authoritative Hot State

The active shard-owner stores authoritative stream state in RocksDB.

For the current kernel and stream-job path, this includes:

- run-local execution metadata
- keyed reducer state
- keyed materialized view state
- checkpoint metadata
- idempotency and fencing metadata needed for replay-safe apply

For the current stream-job slice, the existing authoritative persisted view entries are already aligned with this direction:

- materialized entries live in the `stream_jobs` column family
- the binary key prefix is `sjv1\0`
- view state restores through `LocalStreamJobViewState`

### 2. Durable Progress Tail

Accepted progress after the latest checkpoint is preserved in the durable changelog.

For the current stream-job slice, the existing durable progress records are:

- `StreamJobExecutionPlanned`
- `StreamJobViewUpdated`
- `StreamJobCheckpointReached`
- `StreamJobTerminalized`

Those records are the canonical post-checkpoint replay source for stream-job progress.

### 3. Eventual Projection Store

Browsable and scan-friendly state may be projected into Postgres or another read store.

That layer exists for:

- UI listings
- eventual query paths
- repair inspection
- diagnostics

It is not authoritative for recovery or strong reads.

## RocksDB Layout

### Runtime Shape

The implementation target is one RocksDB instance per shard-worker.

Reasons:

- smaller local working set
- simpler partition ownership
- less cross-thread contention
- bounded checkpoint size
- easier diagnosis of hot partitions and restore cost

### Column Families

The current implementation already has a dedicated `stream_jobs` family for stream-job view state.

The recommended v1 layout is:

- `stream_jobs`
- `stream_job_meta`
- `stream_job_idempotency`
- `stream_job_checkpoint_meta`
- `stream_job_diagnostics`

For the broader stream runtime, equivalent families may later generalize into the `runs`, `operators`, `windows`, `timers`, and `views` layout described in [streams-state-model.md](/Users/bene/code/fabrik/docs/spec/streams-state-model.md).

### Key Encoding

New authoritative keys must be binary and prefix-safe.

The existing stream-job materialized key encoding is the v1 reference shape:

- keyspace prefix
- `u16be(handle_id_len)` + `handle_id`
- `u16be(view_name_len)` + `view_name`
- `0x00`
- `logical_key`

Properties:

- prefix iteration for one `(handle_id, view_name)`
- no dependency on ad hoc text keys for new writes
- migration path that still reads legacy string keys

### Value Encoding

Values should remain compact and binary-first.

For the current keyed-rollup path, values should carry:

- logical key
- materialized aggregate
- latest included checkpoint sequence
- optional owner-epoch marker when useful for strong-read metadata

The runtime should avoid JSON on the hot path except where compatibility requires it.

## Durable Progress Contract

### Changelog Role

The changelog is the durable progress tail between checkpoints.

The implementation rule is:

- no accepted stream-job progress exists only in memory

For the current kernel, this means every accepted materialization or milestone must be represented by one of the `ThroughputChangelogPayload` stream-job variants before it is treated as durable.

### Required Fencing Fields

Durable progress records for externally reported work must carry enough identity for replay-safe dedupe and fencing.

The minimum shape remains:

- `run_id` or stream-job handle identity
- `chunk_id` or equivalent work identity
- `attempt`
- `lease_epoch`
- `owner_epoch`
- `report_id`

For the current stream-job records, `owner_epoch` is already carried on the stream-job changelog payloads and must remain part of replay validation.

### Write Path

The intended logical write path for owner-applied work is:

1. validate owner fencing and dedupe
2. append durable stream progress record
3. write the corresponding RocksDB mutation
4. make the new state visible to strong reads
5. publish projection work asynchronously if needed

This preserves the key guarantee:

- if a strong read can observe the state, restore can reconstruct it from checkpoint plus changelog

### Batching

The implementation should batch changelog appends and RocksDB writes aggressively, but not by weakening the fence:

- a batch must still be rejected atomically if it belongs to a stale owner epoch
- acknowledgements must still correspond only to durably captured progress

## Checkpoint Contract

### Checkpoint Contents

Checkpoint artifacts should capture:

- stream-job handle identity
- owner epoch lineage
- keyed view state
- reducer state not otherwise derivable from the current view
- latest included changelog offset or equivalent replay marker
- named checkpoint sequence metadata

For the current stream-job slice, `LocalStreamJobViewState` is the persisted checkpoint representation for materialized keyed view state.

### Checkpoint Cadence

The current bounded kernel may force a checkpoint around bounded completion.

Longer term, v1 should checkpoint based on:

- time
- bytes written
- number of accepted records
- ownership drain or graceful handoff

The cadence should be coarse enough to avoid replacing row churn with checkpoint churn.

### Restore Order

Restore proceeds in this order:

1. load the latest checkpoint artifact
2. restore binary RocksDB keyspaces from checkpoint contents
3. replay durable stream-job changelog records after the checkpoint marker
4. ignore stale or duplicate records by owner and lease fencing
5. reopen strong reads

For the current migration path, checkpoint restore should also remove legacy string-form entries when restoring the corresponding binary key.

## Query Paths

### Strong Reads

Strong reads route to the active owner and read RocksDB-backed authoritative state.

For the current stream-job keyed query path, the response should continue to expose:

- `consistency`
- `consistencySource`
- `checkpointSequence`
- `streamOwnerEpoch` when available

Strong reads are the only authoritative read path for current materialized state.

### Eventual Reads

Eventual reads route through the projection/query store.

For the current stream-job eventual query path, the response should expose:

- `consistency: "eventual"`
- `consistencySource: "stream_projection_query"`
- `checkpointSequence`

Projection freshness and lag should be visible in operator surfaces.

## Ownership And Restore

### Owner Rule

One partition has one active owner at a time.

The implementation rule is:

- only the active owner mutates authoritative RocksDB state
- strong reads must route to that owner
- failover fences old owners by `owner_epoch`

### Handoff Rule

The next owner must restore from shared durable state, not from local files copied from the old owner.

That means:

- checkpoint artifacts must be usable independently of the original host
- durable changelog replay must be sufficient to reconstruct post-checkpoint progress
- eventual projections may lag and recover later without affecting correctness

## Large Value Policy

The current keyed-rollup path mostly stores small values, but the implementation contract should already reserve the correct shape for growth.

Rules:

- small aggregates stay inline in RocksDB
- medium structured values stay inline in compact binary form
- large result pages or collected member sets spill to object storage via manifests
- RocksDB retains manifest references and integrity metadata only

This avoids poisoning the hot store with payload-heavy values as the product expands.

## Operational Metrics

The v1 implementation should be judged with storage-aware metrics, not only end-to-end throughput.

Important metrics:

- `changelog_entries_published`
- changelog entries per completed chunk or key update
- RocksDB bytes written per accepted update
- checkpoint size
- checkpoint duration
- restore duration from checkpoint plus tail
- strong-read latency
- projection lag
- owner handoff downtime

These metrics should become part of the benchmark story because they reveal whether the implementation is preserving locality or silently regressing into churn.

## Non-Goals

This implementation contract does not require:

- direct SQL authority for active execution state
- globally consistent scans over live owner state
- arbitrary large values inline in RocksDB
- runtime execution of arbitrary TypeScript in the hot loop

## Consequence

`stream-v2` now has a concrete convergence path:

RocksDB-backed authoritative owner state, stream-job changelog records as the durable post-checkpoint tail, checkpoint artifacts as compact restore boundaries, and projected query stores only for eventual reads and operator browsing.
