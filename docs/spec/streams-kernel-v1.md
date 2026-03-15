# Streams Kernel v1

## Status

This document freezes the first compiler-to-runtime contract for `Fabrik Streams`.

It defines the narrow kernel that the current Rust runtime and TypeScript stream compiler both support.

The storage realization for this kernel is frozen in [streams-state-implementation-v1.md](/Users/bene/code/fabrik/docs/spec/streams-state-implementation-v1.md).
The broader artifact and operator contract above this narrow kernel is frozen in [streams-ir.md](/Users/bene/code/fabrik/docs/spec/streams-ir.md).

## Purpose

`streams_kernel_v1` exists to keep the first stream-job slice honest:

- code-first authoring
- compiled artifacts
- Rust-native execution
- partition-owned RocksDB state
- durable checkpoint plus progress-tail recovery

This is not the final `Fabrik Streams` surface.
It is the first strict runtime contract that can be implemented, benchmarked, and evolved without pretending arbitrary stream graphs already exist.

## Current Scope

The kernel has one frozen base shape and one narrow extension already implemented on the same owner-runtime substrate.

Base shape:

- job name: `keyed-rollup`
- runtime: `keyed_rollup`
- source: `bounded_input` or `topic`
- operator graph:
  `reduce(sum) -> emit_checkpoint`
- one strong keyed materialized view
- one strong keyed query
- one workflow-awaitable named checkpoint

Narrow extension:

- runtime: `aggregate_v2`
- source: `bounded_input` or `topic`
- reducer: built-in `count|sum|min|max|avg`
- optional one tumbling event-time window
- strong keyed window queries
- owner-driven watermark advancement with idle-source timer ticks for topic-backed windows

## Storage Contract

### Authoritative State

The active partition owner stores authoritative stream-job state in RocksDB.

For kernel v1, the authoritative persisted state is:

- materialized view entry per `(handle_id, view_name, logical_key)`
- checkpoint sequence included in that view value
- normal owner-local checkpoint artifacts
- durable changelog tail via `ThroughputChangelogPayload::StreamJobViewUpdated`

### RocksDB Key Encoding

Stream-job materialized view entries use a dedicated binary keyspace in the `stream_jobs` column family.

Binary prefix:

- `sjv1\0`

Binary layout:

- keyspace prefix
- `u16be(handle_id_len)` + `handle_id`
- `u16be(view_name_len)` + `view_name`
- `0x00`
- `logical_key`

Properties:

- prefix-safe iteration for one `(handle_id, view_name)`
- no dependency on ad hoc text keys for new writes
- legacy string keys remain readable during migration

### Checkpoint Restore

Checkpoint files persist stream-job view state as `LocalStreamJobViewState`.

Restore behavior:

- delete any legacy string entry for the same `(handle_id, view_name, logical_key)`
- restore the binary key form
- treat checkpoint restore as authoritative owner-local state

### Durable Progress Tail

For kernel v1, accepted post-checkpoint progress is represented by the existing durable changelog entry:

- `StreamJobExecutionPlanned { ... }`
- `StreamJobViewUpdated { ... }`
- `StreamJobSourceProgressed { ... }`
- `StreamJobCheckpointReached { ... }`
- `StreamJobTerminalized { ... }`

For the bounded `keyed_rollup` slice, the durable progression is:

1. execution planned
2. materialized view updates per key
3. checkpoint reached
4. terminalized

For the topic-backed `keyed_rollup` slice, the durable progression is:

1. execution planned
2. source lease assigned per source partition
3. materialized view updates per key
4. source progress advanced per source partition
5. checkpoint reached for the current source frontier sequence
6. later source frontiers advance the checkpoint sequence while reusing the declared checkpoint name

Owner-local state, checkpoint callbacks, and terminal callbacks are reconstructed from those owner-applied stream entries rather than from the initial schedule request alone.

For the topic-backed windowed `aggregate_v2` slice, `StreamJobSourceProgressed` also carries:

- per-source-partition event-time watermark
- last closed window end
- pending window ends
- dropped late-event counters and last dropped-late metadata

That lets restore replay both source cursor advancement and window/timer progress from the same tail.

## Execution Kernel Contract

### Input

The kernel supports two source shapes.

Bounded input:

```json
{
  "kind": "bounded_items",
  "items": [
    {
      "eventId": "pay_001",
      "accountId": "acct_123",
      "amount": 42
    }
  ]
}
```

The runtime may also accept a raw array payload with equivalent item objects.

Topic input:

```json
{
  "kind": "topic",
  "topic": "payments",
  "startOffset": "earliest"
}
```

### Required Job Shape

The compiled job must satisfy all of the following:

- `job.name == "keyed-rollup"`
- `job.runtime == "keyed_rollup"`
- `job.source.kind == "bounded_input"` or `job.source.kind == "topic"`
- `job.key_by == "accountId"`
- exactly 2 operators
- operator 0 is `reduce` with config:
  - `reducer == "sum"`
  - `valueField` present
  - `outputField` optional
- operator 1 is `emit_checkpoint` with:
  - non-empty `name`
  - integer `sequence`
- exactly 1 view
- view is `strong`, `by_key`, and keyed by `accountId`
- exactly 1 query
- query points at the declared view and is `strong`
- checkpoint policy is `named_checkpoints`
- exactly 1 declared checkpoint
- declared checkpoint name and sequence match `emit_checkpoint`
- declared checkpoint delivery is `workflow_awaitable`

### Runtime Behavior

For each input item:

1. extract the partition key from `accountId`
2. extract the numeric reducer input from `amount`
3. accumulate a running sum per key
4. materialize one current output per key
5. stamp the output with `asOfCheckpoint`
6. emit a named checkpoint once the current bounded input or topic frontier is incorporated

For topic sources, the runtime must also:

1. persist `source_partition -> next_offset` as `StreamJobSourceProgressed`
2. persist `source_partition -> source-owner lease` as `StreamJobSourceLeaseAssigned`
3. persist the current checkpoint frontier target and sequence per source partition
4. restore source cursors and source leases from `checkpoint + streams source-progress tail`
5. advance the same named checkpoint with a new sequence when a later source frontier is reached

Materialized output shape:

```json
{
  "accountId": "acct_123",
  "totalAmount": 420,
  "asOfCheckpoint": 1
}
```

### Strong Query Contract

The owner-routed query path returns the materialized entry for one logical key and annotates it with:

- `consistency`
- `consistencySource`
- `checkpointSequence`
- `streamOwnerEpoch` when available

### Eventual Query Contract

The projected query path reads the latest materialized entry for one logical key from the read model and annotates it with:

- `consistency: "eventual"`
- `consistencySource: "stream_projection_query"`
- `checkpointSequence`

## Compiler-To-Runtime Contract

### Artifact Marker

Compiled artifacts must carry:

- `runtime_contract: "streams_kernel_v1"`

The runtime rejects artifacts whose declared contract does not match the supported kernel version.

### Compiler Responsibility

The TypeScript stream compiler must:

- emit only statically serializable job definitions
- validate the `keyed_rollup` kernel shape before writing the artifact
- include the runtime contract marker in the compiled artifact
- preserve source locations for the top-level job sections

### Runtime Responsibility

The Rust runtime must:

- validate stored artifacts before activation
- validate config-derived compiled jobs before activation
- derive a typed keyed-rollup kernel plan before execution
- reject malformed or unsupported jobs before mutating authoritative state

## Non-Goals

Kernel v1 does not attempt to support:

- arbitrary operator graphs
- joins
- arbitrary window types beyond the current tumbling `aggregate_v2` slice
- scans
- generic user-defined functions
- SQL authoring
