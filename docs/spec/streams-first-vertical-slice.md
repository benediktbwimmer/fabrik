# Streams First Vertical Slice

## Status

This document defines the first implementable end-to-end stream-job slice for `Fabrik Streams`.

It exists to connect:

- the workflow-facing contract in [stream-jobs.md](/Users/bene/code/fabrik/docs/spec/stream-jobs.md)
- the bridge contract in [streams-bridge.md](/Users/bene/code/fabrik/docs/spec/streams-bridge.md)
- the runtime contract in [streams-runtime-model.md](/Users/bene/code/fabrik/docs/spec/streams-runtime-model.md)
- the kernel contract in [streams-kernel-v1.md](/Users/bene/code/fabrik/docs/spec/streams-kernel-v1.md)

## Purpose

The first slice should prove that `Fabrik Streams` is a real stateful execution subsystem, not only a renamed throughput lane.

The slice should be narrow enough to ship without designing the whole product, but rich enough to validate:

- stream-job admission
- partition-owned state
- named durable checkpoints
- materialized state
- strong owner-routed query
- workflow and stream coordination through the bridge

## Scope

The first slice includes exactly one stream-job shape:

- one workflow-startable stream job
- one bounded input source
- one fixed operator chain
- one named materialized view
- one named checkpoint
- one strong keyed query path

This is intentionally not:

- a general standalone stream deployment API
- a source/sink marketplace
- a SQL surface
- a generic UDF system
- a joins-and-windows launch

## Product Slice

The first slice is:

`workflow starts bounded stream job -> stream job processes keyed input -> stream job materializes keyed aggregate -> stream job emits named checkpoint -> workflow may await checkpoint and issue strong keyed query`

## Workflow Surface

The workflow-facing shape is:

```ts
const job = await ctx.startStreamJob("keyed-rollup", {
  input: {
    kind: "bounded_items",
    items: payments,
  },
});

await job.untilCheckpoint("hourly-rollup-ready");

const account = await job.query("accountTotals", {
  key: "acct_123",
}, {
  consistency: "strong",
});
```

Rules:

- `ctx.startStreamJob()` is durable and replay-safe
- `job.untilCheckpoint(name)` is a durable workflow barrier
- `job.query(...)` is non-durable and non-replay-stable
- the workflow must not branch durably on query results

## One Allowed Job Shape

The first stream job type is `keyed-rollup`.

Its execution graph is fixed:

`bounded source -> keyBy(account_id) -> reduce(sum amount) -> materialize(accountTotals) -> emit checkpoint(hourly-rollup-ready)`

This slice intentionally fixes the operator graph so the engine contract can be proven before a broader authoring surface exists.

## Source Contract

The only source in the first slice is workflow-submitted bounded input.

Input shape:

- the workflow submits a bounded array payload
- the payload may be inline or externalized through an existing payload handle
- each item must contain:
  - stable item key or deterministic position
  - partitioning key
  - numeric amount for reduction

Example logical item:

```json
{
  "eventId": "pay_001",
  "accountId": "acct_123",
  "amount": 42
}
```

The runtime may chunk and partition the bounded input internally, but that is not visible in workflow semantics.

## Operator Contract

### `keyBy(accountId)`

- routes every item to the partition derived from `accountId`
- all mutations for one `accountId` are serialized by partition ownership

### `reduce(sum amount)`

- maintains one running total per `accountId`
- the reducer state is mergeable and deterministic
- duplicate or stale externally reported work must be fenced before mutation

### `materialize(accountTotals)`

- writes the current total for each `accountId` into a declared materialized view named `accountTotals`
- the materialized view is authoritative in owner state
- eventual projections may mirror it later, but projections are not required for this slice

### `emit checkpoint(hourly-rollup-ready)`

- the runtime emits this checkpoint only after the bounded input has been fully incorporated into authoritative partition state
- the checkpoint is durable, monotonic, and bridge-visible

## Materialized View Contract

The first slice exposes one materialized view:

- `accountTotals`

Key:

- `accountId`

Value:

```json
{
  "accountId": "acct_123",
  "totalAmount": 420,
  "asOfCheckpoint": 7
}
```

Rules:

- the strong read path routes to the active owner for the requested key
- the returned value should identify the latest included checkpoint sequence when available
- the owner state is authoritative for the key at read time

## Checkpoint Contract

The first slice exposes one named checkpoint:

- `hourly-rollup-ready`

The runtime may emit the checkpoint only when:

- all bounded source items admitted for the job have been applied or durably recorded for replay-safe application
- the materialized view state for those items is included in authoritative owner state
- the checkpoint record is durable

Bridge rules:

- one `(stream_job_handle_id, checkpoint_name, checkpoint_sequence)` may be accepted at most once
- duplicate delivery is idempotent
- older sequences are stale

Workflow rule:

- `await job.untilCheckpoint("hourly-rollup-ready")` resumes only from an accepted bridge wakeup

## Query Contract

The first slice supports one query:

- `job.query("accountTotals", { key }, { consistency: "strong" })`

Rules:

- the read routes to the active owner for the queried `accountId`
- the response is not replay-stable workflow input
- the response may include value metadata such as:
  - `owner_epoch`
  - `checkpoint_sequence`
  - `read_at`

The same keyed query may also be requested with `consistency: "eventual"`.
That path reads from the projected stream-job view in the query store and must annotate the result with a projection-backed `consistencySource`.

Initial non-goals:

- scans
- ad hoc filtering
- workflow-authoritative query branching

## Runtime Execution Contract

### Admission

When the workflow reaches `StartStreamJob`:

1. the workflow side emits one `submit_stream_job` bridge request
2. the bridge returns a stable `stream_job_handle_id`
3. the stream runtime creates or recovers the execution behind that handle

The first slice must be idempotent for the same workflow request identity.

### Partitioning

- partitioning is derived from `accountId`
- one partition has one active owner at a time
- authoritative keyed totals live with the active owner
- stream view updates are routed by logical key, not by job id
- bounded input is decomposed into partition-local work items and applied by the owning shard
- the coordinator does not pre-aggregate keyed totals before owner execution

### State

The authoritative state required for this slice is:

- reducer state per `accountId`
- materialized `accountTotals` entry per `accountId`
- checkpoint metadata
- owner-epoch and idempotency metadata needed for replay-safe recovery

For the first keyed-rollup slice, the owner may reuse the current materialized value as the reducer accumulator as long as replay remains checkpoint-plus-tail correct.

### Durability

The first slice uses the runtime durability contract:

- authoritative hot state in the active owner's local state store
- durable stream progress entries for:
  - execution planned
  - per-key materialization
  - checkpoint reached
  - terminalized
- periodic or completion-driven checkpoint

Because the source is bounded, the implementation may force a checkpoint at the completion of the initial rollup.

### Checkpoint Barrier

- `checkpoint reached` is recorded per active stream partition
- the bridge-visible named checkpoint is materialized only after every active partition has recorded that checkpoint
- terminal completion is withheld until that checkpoint barrier is complete

### Restore

On owner failover:

- the new owner restores from the latest checkpoint plus durable progress after that checkpoint
- shard checkpoints include stream-job view rows by logical-key ownership and checkpoint rows by `stream_partition_id`
- stale reports from prior owners are fenced by `owner_epoch`
- the strong query route follows the new owner

## Terminal Contract

The first slice does not require a rich final result.

It only requires:

- the stream job can eventually reach a terminal state
- terminal acceptance remains bridge-mediated
- terminal outcome is distinct from checkpoint wakeup

The workflow may choose to:

- stop after the checkpoint barrier
- or later await terminal completion through `job.result()`

## Initial Implementation Guidance

The intended first implementation bias is:

- Rust runtime
- RocksDB-backed authoritative local state
- built-in fixed operator kernels
- no arbitrary user code in the hot loop
- owner-routed RPC for strong keyed reads

This slice should reuse existing payload-handle, bridge-identity, and owner-fencing machinery where possible.

## Non-Goals

The first slice does not require:

- standalone stream-job deployment without workflows
- joins
- windows beyond the checkpoint boundary itself
- timers exposed to users
- generic sink connectors
- generic TypeScript DSL design
- arbitrary reducer plugins

## Success Criteria

The first slice is successful when all of the following are true:

- one workflow call to `ctx.startStreamJob()` yields one stable stream-job handle
- one bounded keyed input can be fully reduced into owner-local materialized state
- `hourly-rollup-ready` is emitted as a durable monotonic checkpoint
- `hourly-rollup-ready` becomes bridge-visible only after every active key partition reaches it
- `job.untilCheckpoint("hourly-rollup-ready")` resumes only through accepted bridge wakeup
- `job.query("accountTotals", { key }, { consistency: "strong" })` returns owner-routed keyed state
- `job.query("accountTotals", { key }, { consistency: "eventual" })` returns projected keyed state with explicit eventual metadata
- failover preserves checkpoint and query correctness through checkpoint plus durable progress recovery

## Consequence

If this slice works, the platform has proven the core semantic loop of `Fabrik Streams`:

- workflows can start stream jobs without collapsing stream semantics into workflows
- streams can own keyed state and checkpoints directly
- materialized state can be queried strongly from the owner
- the bridge can coordinate the two without corrupting either side's contract
