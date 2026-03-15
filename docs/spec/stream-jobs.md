# Stream Jobs

## Status

This document defines the current narrow contract for stream-native workflow primitives such as `ctx.startStreamJob(...)`.

The currently implemented workflow-facing slice is:

- `ctx.startStreamJob(...)`
- `job.awaitCheckpoint(name)`
- `job.untilCheckpoint(name)`
- `job.query(name, args?, options?)`
- `job.cancel(reason?)`
- `job.awaitTerminal()`
- `job.result()`

The runtime and bridge layer now also support lifecycle control for long-lived topic-backed jobs:

- `pause_stream_job`
- `resume_stream_job`
- strong `__bridge_state` inspection for lifecycle, awaited checkpoints, query acceptance, signal delivery causality, and repair debt

The current shipping bridge contract for throughput mode remains defined by [streams-bridge.md](/Users/bene/code/fabrik/docs/spec/streams-bridge.md) and [throughput-mode.md](/Users/bene/code/fabrik/docs/spec/throughput-mode.md).

## Purpose

`Fabrik Streams` should eventually expose a first-class stream-job contract to workflows without collapsing stream semantics into throughput mode.

The goal is:

- workflows remain workflow-authoritative
- streams remain stream-authoritative
- the bridge remains the only legal crossing point
- stream jobs become explicit workflow primitives rather than hidden backend behavior

## Core Distinction

`ctx.bulkActivity()` and `ctx.startStreamJob()` are not the same thing.

`ctx.bulkActivity()`:

- is bounded
- is batch/chunk shaped
- resolves through one batch barrier
- stays inside throughput-mode semantics

`ctx.startStreamJob()`:

- may be long-lived
- may expose named checkpoints
- may expose stream-native query handles
- may terminate much later than the workflow step that started it
- requires a distinct bridge contract

The stream-job contract must not be smuggled into throughput mode by overloading `bulkActivity()`.

## Workflow Surface

Current workflow-facing shape:

```ts
const job = await ctx.startStreamJob("fraud-detector", {
  input: "payments",
  config: { threshold: 0.97 }
});
```

```ts
await job.awaitCheckpoint("hourly-rollup-ready");
const stats = await job.query("currentStats", { consistency: "strong" });
await job.cancel("workflow closed");
const result = await job.awaitTerminal();
```

Current hybrid shape:

```ts
const job = await ctx.startStreamJob("keyed-rollup", {
  input: { topic: input.topic },
  config: {
    operators: [
      { kind: "reduce", name: "sum-account-totals", config: { reducer: "sum", valueField: "amount", outputField: "totalAmount" } },
      { kind: "emit_checkpoint", name: "hourly-rollup-ready", config: { sequence: 1 } },
      { kind: "signal_workflow", name: "notify-account-rollup", config: { view: "accountTotals", signalType: "account.rollup.ready", whenOutputField: "totalAmount" } },
    ],
  },
});

const signal = await ctx.waitForSignal("account.rollup.ready");
const account = await job.query("accountTotals", { key: signal.logicalKey }, {
  consistency: "strong",
});
await job.cancel({ reason: "workflow-threshold-handled" });
const result = await job.awaitTerminal();
```

Currently implemented semantic operations:

- `ctx.startStreamJob(name, input)`:
  starts or recovers a stable stream job handle
- `job.awaitCheckpoint(name)`:
  preferred durable checkpoint barrier for one named milestone
- `job.untilCheckpoint(name)`:
  compatibility alias for `job.awaitCheckpoint(name)`
- `job.query(name, args?, options?)`:
  performs a non-authoritative stream read with explicit consistency
- `job.cancel(reason?)`:
  requests stream-job cancellation idempotently
- `job.awaitTerminal()`:
  preferred durable terminal barrier for stream jobs that should block on final completion
- `job.result()`:
  compatibility alias for `job.awaitTerminal()`
- `ctx.waitForSignal(name)`:
  the current workflow-facing path for reacting to declared `signal_workflow` emissions from a stream job

## Bridge-Owned Entities

- `workflow run`:
  authoritative workflow execution
- `stream job request`:
  stable workflow-side admission identity for one `StartStreamJob`
- `stream job handle`:
  stable workflow-visible stream job id returned by the bridge
- `stream job execution`:
  stream-owned runtime instance behind that handle
- `awaited checkpoint`:
  workflow-side barrier request for one named checkpoint on one stream job
- `workflow owner epoch`:
  workflow ownership generation
- `stream owner epoch`:
  stream-job ownership generation

## Stable Identifiers

Every bridge interaction must carry:

- `tenant_id`
- `namespace_id` if namespaces differ from tenants
- `workflow_instance_id`
- `workflow_run_id`
- `stream_job_request_id`
- `stream_job_handle_id`
- `workflow_owner_epoch`
- `stream_owner_epoch` on callbacks and strong queries
- `artifact_hash` or equivalent workflow definition identity
- `protocol_version`
- `operation_kind`

Checkpoint-specific interactions must also carry:

- `checkpoint_name`
- `checkpoint_sequence`
- `await_request_id`

## Lifecycle

### Workflow-Side Lifecycle

`StartStreamJob` proceeds through:

- admitted
- handle assigned
- optionally waiting for one or more named checkpoints
- optionally waiting for terminal completion

The workflow advances only through accepted bridge events.

### Stream-Side Lifecycle

The stream job itself may proceed through richer internal states, but the bridge only relies on:

- running
- paused
- checkpoint reached
- completed
- failed
- cancelled

The stream runtime may expose more detailed operational state, but that state is not workflow-authoritative unless explicitly accepted through the bridge.

## Bridge Operations

### `submit_stream_job`

Called when a workflow reaches `StartStreamJob`.

Required behavior:

- idempotent for `(tenant_id, workflow_run_id, stream_job_request_id)`
- returns the same `stream_job_handle_id` on replay or retry
- never creates multiple stream jobs for one workflow admission
- records enough metadata to recover after workflow owner failover

### `await_stream_checkpoint`

Registers workflow intent to wait on a named checkpoint.

Required behavior:

- idempotent for `(stream_job_handle_id, checkpoint_name, await_request_id)`
- safe across workflow owner failover
- returns immediately as satisfied if the same or newer checkpoint has already been accepted
- never appends more than one workflow-authoritative wakeup for the same awaited checkpoint

### `query_stream_job`

Performs a non-authoritative stream read.

Required behavior:

- explicit consistency mode
- never mutates workflow state
- never becomes replay input unless routed through a separate durable primitive
- may be inspected through bridge debug surfaces in query-service without becoming workflow-authoritative

### `cancel_stream_job`

Requests cancellation of a stream job.

Required behavior:

- idempotent for the same job handle
- race-safe with terminal completion
- fenced by workflow and stream owner epochs

### `await_stream_terminal`

Represents the workflow barrier behind `job.awaitTerminal()`.

Required behavior:

- exactly one terminal outcome wins:
  `completed`, `failed`, or `cancelled`
- duplicate or stale terminal callbacks are side-effect free
- terminal acceptance appends at most one workflow-authoritative event

## Callback Shapes

The stream side may call back into the bridge with:

- `stream_job_checkpoint_reached`
- `stream_job_completed`
- `stream_job_failed`
- `stream_job_cancelled`

For query requests, the bridge also accepts:

- `stream_job_query_completed`
- `stream_job_query_failed`

Each callback must include:

- stable bridge identity
- stream owner epoch
- callback idempotency key
- protocol version
- operation kind

Checkpoint callbacks must also include:

- `checkpoint_name`
- `checkpoint_sequence`
- `checkpoint_at`

## Checkpoint Contract

Named checkpoints are the most important new primitive.

A checkpoint is workflow-awaitable only if it is:

## Operator Visibility

Current bridge/debug visibility for stream jobs is exposed through query-service:

- `/debug/streams-bridge/jobs/{tenant_id}/{instance_id}/{run_id}`
- `/debug/streams-bridge/jobs/{tenant_id}/{instance_id}/{run_id}/repairs`
- `/debug/streams-bridge/jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}`

Supported list filters:

- `status`
- `next_repair`
- `latest_query_status`
- `latest_query_name`
- `latest_query_consistency`

The `/repairs` route applies the same filters and sorts but returns only handles that still need bridge acceptance repair.

Supported list sorts:

- `created_at_asc`
- `created_at_desc`
- `latest_query_activity_desc`
- `latest_query_activity_asc`

The list response includes latest-query summary fields so operators can sort by recent query activity and spot jobs whose newest query is failed, stale, or not yet accepted.

The stream-job bridge payloads also expose pending bridge work explicitly:

- per-handle `pending_repairs`, `pending_repair_count`, and `next_repair`
- per-checkpoint `next_repair`
- per-query `next_repair`

Signal-facing diagnostics now also expose:

- callback dedupe key and callback event id per emitted `signal_workflow`
- target workflow instance and run identity
- workflow signal queue status such as `queued`, `dispatching`, or `consumed`
- workflow-side dispatch and consumed event ids when known

That lets operators distinguish:

- stream callback persisted but not yet workflow-accepted
- checkpoint/query/terminal acceptance debt
- a fully accepted handle with no pending bridge repair

- named
- durable
- monotonic for a given stream job handle
- version-stable within one workflow artifact version
- accepted by the bridge at most once per awaited barrier

A checkpoint is not enough if it is only:

- a local operator progress marker
- a transient watermark
- a best-effort metric sample
- an owner-local observation without durable handoff semantics

### Checkpoint Ordering

For a given `(stream_job_handle_id, checkpoint_name)`:

- each later accepted checkpoint must have a strictly newer `checkpoint_sequence`
- replay or duplicate callbacks with the same sequence are idempotent
- older sequences are rejected as stale

### Checkpoint Reuse

The same checkpoint name may be awaited multiple times by workflow code only if the API explicitly models that intent.

Default rule:

- one `await_request_id` maps to one accepted checkpoint advancement

## Query Consistency

Stream-job queries are never workflow-authoritative on their own.

### Strong

Strong queries:

- route to the active stream owner
- may observe live nonterminal state
- are useful for UI, operators, and workflow code making non-durable choices
- are not replay-stable

### Eventual

Eventual queries:

- may use projections
- may lag the owner
- are suitable for dashboards and cheap polling
- are never replay-stable

Workflow code must not branch durably on stream-job query results unless a separate durable primitive is defined for that exact purpose.

## Replay Contract

`StartStreamJob` itself is durable and replay-safe.

`job.awaitCheckpoint(name)` and `job.awaitTerminal()` are durable barriers.

Compatibility note:

- `job.untilCheckpoint(name)` remains supported as an alias for `job.awaitCheckpoint(name)`
- `job.result()` remains supported as an alias for `job.awaitTerminal()`

`job.query(...)` is not replay-safe and must not be treated as deterministic workflow input.

The workflow replay log therefore records:

- stream-job admission
- assigned stream-job handle
- accepted checkpoint wakeups
- accepted terminal outcome

The replay log does not record:

- owner-local stream state
- arbitrary query responses
- projection reads

## Closed Workflow Behavior

If the workflow run is already closed:

- stream-job callbacks must not reopen workflow execution
- accepted terminal or checkpoint callbacks must be classified as ignored or orphaned
- stream-side cleanup may continue independently

## Cancellation Races

Cancellation must be race-safe across workflow and stream ownership changes.

Rules:

- if terminal completion wins first, the workflow observes the terminal result
- if cancellation wins first, later terminal completions remain non-authoritative
- a closed workflow may still leave the stream job to be cleaned up asynchronously
- stale cancellation callbacks are fenced by owner epochs and idempotency keys

## IR Mapping

These workflow IR nodes are the correct future shape:

- `StartStreamJob`
- `WaitForStreamCheckpoint`
- `QueryStreamJob`
- `CancelStreamJob`
- `AwaitStreamJobTerminal`

They are intentionally separate from:

- `StartBulkActivity`
- `WaitForBulkActivity`

## Bridge Evolution Rule

The bridge may share protocol machinery across throughput mode and stream jobs:

- idempotent admission
- fencing
- callback dedupe
- repair
- query consistency labeling

But the stream-job contract must remain distinct at the semantic layer.

That means:

- no pretending a long-lived stream job is just a bulk batch
- no overloading throughput batch barriers to mean stream checkpoints
- no leaking stream runtime internals directly into workflow state

## Non-Goals

This contract is not:

- a streaming SQL spec
- a generic Flink replacement API surface
- permission for workflow code to branch durably on arbitrary stream reads
- a justification for weakening workflow replay guarantees

## Consequence

`Fabrik Workflows` can keep throughput mode as a workflow-native bulk primitive while `Fabrik Streams` grows into a true standalone stream processor.

When stream-native workflow primitives ship, they should implement this contract rather than extending throughput mode by accident.
