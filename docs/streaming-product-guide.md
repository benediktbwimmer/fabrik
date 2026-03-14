# Streaming Product Guide

This guide describes Fabrik's current workflow-facing streaming story and the internal architecture direction behind it.

## What The Streaming Story Is

Fabrik's streaming story now has three layers:

1. **Durable ingress**
   Topic adapters consume Kafka or Redpanda records and map them deterministically into:
   - `start_workflow`
   - `signal_workflow`

2. **Bridge-mediated throughput execution**
   Workflows opt into bulk fan-out / fan-in through `ctx.bulkActivity()`. A bridge layer admits the work, enforces idempotency and fencing, and routes it into the stream-backed execution lane selected by server policy.

3. **Operator visibility and recovery**
   Operators can watch lag, routing, reducer progress, ownership, dead letters, replay, and failover from the console.

The important current product constraint is that `Fabrik Workflows` is still presented as a workflow platform first. The architecture is being split so a future `Fabrik Streams` product can emerge cleanly, but the current workflow product story stays centered on durable orchestration with a stream-backed bulk lane.

Internally, that split is:

- `Fabrik Workflows` for workflow-authoritative history and replay
- the bridge for admission, fencing, idempotency, and callback translation
- a stream-backed execution subsystem for high-volume nonterminal work

## Core Product Surfaces

### Topic adapters

Topic adapters provide the ingress path for real-time event streams.

They support:

- Kafka / Redpanda JSON topics
- `start_workflow` and `signal_workflow`
- durable offset tracking
- ownership fencing for multi-replica ingest
- dead-letter storage
- DLQ replay
- live lag and ownership watch state

They intentionally do **not** provide:

- arbitrary transforms
- stream joins
- windows
- general stateful streaming compute inside the `Fabrik Workflows` product surface

### Throughput mode

Throughput mode is the execution surface for high-cardinality batch work:

```ts
const batch = await ctx.bulkActivity("process.enrich", items, {
  execution: "eager",
  chunkSize: 256,
  reducer: "sum",
});
```

The workflow-visible contract remains barrier-based:

- the workflow observes one deterministic batch outcome
- chunk progress is live and useful, but non-authoritative
- workers do not directly mutate workflow state
- the workflow does not choose the underlying stream implementation

### Reducers

Built-in reducers currently include:

- `count`
- `all_succeeded`
- `all_settled`
- `sum`
- `min`
- `max`
- `avg`
- `histogram`
- `sample_errors`

The current stream-backed execution lane is strongest on mergeable reducers and wide workloads. Today that lane is implemented by `stream-v2`.

### Streaming Ops page

The console now exposes a unified streaming operations surface at `/streaming`.

It pulls together:

- topic-adapter ingress health
- queue pressure and stream-lane state
- active streaming batches
- routing and reducer mix

This is the best first surface for answering:

- what is coming into the system
- where it is routing
- whether lag is building
- whether reducers are converging
- whether capacity or ownership events are affecting throughput

## Operator Workflow

The intended operator loop is:

1. Inspect the **Streaming Ops** page for tenant-wide ingress and throughput health.
2. Drill into **Topic Adapters** when lag, ownership, or DLQ issues appear.
3. Replay dead letters from the adapter page after fixing the mapping or downstream issue.
4. Drill into **Task Queues** when capacity pressure or bridge routing needs explanation.
5. Drill into **Runs** and **Run detail** when a specific workflow or throughput batch needs batch-level progress, reducer output, or routing inspection.

For a concrete setup path with example workflows and adapter configs, see [streaming-getting-started.md](streaming-getting-started.md).

## Product Positioning

The clean way to describe Fabrik's streaming story is:

> Streams in, workflows and signals triggered durably, high-throughput batch work executed through a stream-backed lane, and live operator visibility for lag, routing, reducers, and recovery.

That is a coherent product story.

The wrong framing for the current workflow product would be:

> Fabrik Workflows is already a full general-purpose stream processor.

That framing is wrong because the current workflow product center of gravity remains durable workflow orchestration. The internal split is intentionally being shaped so that a future `Fabrik Streams` product can grow out of the same platform without changing workflow semantics.

## Choosing The Right Shape

The execution backend is strongest on a smaller number of wider workflows.

From the current benchmark envelope:

- `1 x 100000` activities performed much better than `100 x 1000`
- `10 x 10000` was also materially better than `100 x 1000`

That means teams should prefer:

- grouping related work into one wide batch when the workflow semantics allow it
- mergeable reducers where possible
- batch-level observability instead of per-item durable history when throughput matters

See [streaming-performance-envelope.md](benchmarking/streaming-performance-envelope.md) for the current benchmark-backed guidance.

## Recovery Story

The streaming product story is not only about throughput.

It also now includes:

- `stream-v2` owner restart benchmarks
- adapter owner crash handoff benchmarks
- live ownership state in operator surfaces
- DLQ replay for topic adapters

The goal is straightforward:

- high throughput in the happy path
- boring, visible recovery when ownership moves

## Architecture Direction

The architecture direction is:

- keep `ctx.bulkActivity()` stable and backend-agnostic
- move throughput execution behind the bridge
- let the current `stream-v2` lane become the first implementation of the internal stream subsystem
- add dedicated stream-job semantics later rather than overloading throughput mode with every future stream concern

That keeps the workflow product story clean while making room for a stronger standalone stream product later.

## Recommended Entry Points

For engineers:

- [streaming-getting-started.md](streaming-getting-started.md)
- [spec/throughput-mode.md](spec/throughput-mode.md)
- [spec/streams-bridge.md](spec/streams-bridge.md)
- [benchmarking/streaming-performance-envelope.md](benchmarking/streaming-performance-envelope.md)
- [benchmarking/streaming-release-scorecard.md](benchmarking/streaming-release-scorecard.md)

For operators:

- [operator-runbook-alpha.md](operator-runbook-alpha.md)
- `/streaming`
- `/topic-adapters`
- `/task-queues`

For architecture context:

- [architecture.md](architecture.md)
