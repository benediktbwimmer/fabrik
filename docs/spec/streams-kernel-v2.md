# Streams Kernel v2

## Status

This document defines the intended next compiler-to-runtime contract for `Fabrik Streams`.

Unlike [streams-kernel-v1.md](/Users/bene/code/fabrik/docs/spec/streams-kernel-v1.md), this kernel is not yet implemented. It exists to freeze the next built-in expansion step so compiler, runtime, and storage work can converge on one shape.

## Purpose

`streams_kernel_v1` proves the first real stream-job slice.

The next kernel should expand the product meaningfully without collapsing into arbitrary graph execution.

The design goal is:

- more expressive than `keyed_rollup`
- still Rust-native and benchmarkable
- still explicit about fast-lane constraints
- still narrow enough to harden and inspect

## Runtime Contract Marker

Compiled artifacts for this kernel should declare:

- `runtime_contract: "streams_kernel_v2"`

The runtime must reject artifacts that claim this contract before the runtime explicitly supports it.

## Current Scope

Kernel v2 supports a constrained but broader stateful stream shape:

- one source
- explicit `key_by`
- optional stateless pre-aggregation transforms
- optional tumbling window
- one or more built-in reducers
- one or more materialized views
- one or more queries
- one or more named checkpoints

This kernel is intended to cover:

- keyed counters
- keyed rollups
- threshold detectors
- simple real-time summaries
- bounded batch rollups
- narrow continuous aggregates

It is intentionally not:

- arbitrary joins
- generic UDF execution
- session windows
- SQL authoring
- general graph compilation

## Kernel Shape

The supported graph shape is:

`source -> [map/filter/route]* -> key_by -> [window]? -> aggregate+ -> materialize+ -> [emit_checkpoint|signal_workflow|sink]*`

Rules:

- `key_by` is required
- `window` is optional
- all stateful aggregation happens after `key_by`
- at least one `materialize` operator is required
- checkpoint operators may appear after materialization
- bridge and sink operators may only appear after the state mutation they depend on

This is still a structured kernel, not an open-ended DAG runtime.

## Supported Sources

Kernel v2 should support these source kinds:

- `bounded_input`
- `topic`

Rules:

- `bounded_input` remains the easiest path for workflow-started stream jobs
- `topic` is the first continuous source shape
- both sources must compile to the same internal keyed execution semantics after admission

Later source kinds may exist in the broader IR without being part of kernel v2.

## Supported Operator Families

### Pre-Key Stateless Operators

Allowed operators:

- `map`
- `filter`
- `route`

Rules:

- these must be deterministic
- these must not mutate durable state
- these must compile to built-in runtime expressions, not arbitrary user code blobs
- pre-key operators preserve declared order before the keyed/windowed stage
- initial `map` support is limited to built-in field transforms:
  - `inputField`
  - `outputField`
  - optional `multiplyBy`
  - optional `add`
- initial runtime support currently accepts `map` and `filter` in any declared pre-key order
- initial `route` support writes a string field from ordered predicate branches:
  - `outputField`
  - `branches[] = { predicate, value }`
  - optional `defaultValue`

Examples:

- normalize a numeric field
- discard invalid events
- route severity buckets

### Partitioning Operator

Allowed operator:

- `key_by`

Rules:

- exactly one `key_by` must appear
- it defines partition ownership and state placement
- the compiled key expression must be deterministic and statically validated

### Window Operator

Allowed operator:

- `window`

For kernel v2, only one window mode is supported:

- tumbling window

Required config:

- window size
- time field or event-time policy
- allowed lateness policy if event-time is enabled
- retention/close metadata

Rules:

- jobs may omit the window operator entirely for current-value keyed state
- when present, the window operator defines the state key as `(logical_key, window_id)`
- window close must be checkpoint- and replay-safe

### Aggregate Operators

Allowed operators:

- `reduce`
- `aggregate`
- `dedupe`

Supported built-in reducer kinds for kernel v2:

- `count`
- `sum`
- `min`
- `max`
- `avg`
- `histogram`

Optional near-term reducer:

- `threshold`

Threshold reducer config:

- `valueField`
- numeric `threshold`
- optional `comparison` of `gt`, `gte`, `lt`, or `lte` (default `gte`)
- optional `outputField` for the materialized boolean result

Rules:

- reducers must be built-in and statically known
- reducer state must be compact and binary-encodable
- reducers must have explicit merge semantics
- multiple aggregate operators are allowed only when the runtime can plan them as one built-in aggregate kernel stage rather than independent opaque callbacks

### Materialization Operators

Allowed operator:

- `materialize`

Rules:

- at least one materialized view must be declared
- every materialize operator must target a declared view
- views may expose current keyed state, keyed window state, or narrow summary state
- the runtime must know which materialized writes are authoritative strong-read surfaces

### Bridge And Output Operators

Allowed operators:

- `emit_checkpoint`
- `signal_workflow`
- `sink`

Rules:

- `emit_checkpoint` may be workflow-awaitable only if declared in checkpoint policy
- `signal_workflow` must cross the bridge through explicit idempotency and fencing rules
- sinks are outside exactly-once guarantees unless separately specified

## State Model For Kernel v2

Kernel v2 introduces explicit declared state instead of inferring everything from one view.

The artifact should declare:

- keyed aggregate state
- optional keyed window state
- dedupe state where used
- materialized views
- checkpoint metadata

Recommended state shapes:

- `(partition_id, operator_id, logical_key)` for unwindowed state
- `(partition_id, operator_id, logical_key, window_id)` for windowed state
- `(partition_id, view_id, logical_key)` for current-value views
- `(partition_id, view_id, logical_key, window_id)` for windowed views when exposed

## Query Model For Kernel v2

Kernel v2 should support explicit query modes:

- `by_key`
- `prefix_scan`

Rules:

- `by_key` may be served as a strong owner-routed read
- `prefix_scan` should default to eventual unless explicitly proven safe and bounded for owner reads
- every query must declare supported consistencies

Typical query shapes:

- current aggregate by key
- latest window aggregate by key
- recent keyed summaries via eventual scan

## Checkpoint Model For Kernel v2

Kernel v2 allows one or more named checkpoints.

Checkpoint declarations must include:

- checkpoint name
- monotonic sequence source
- delivery mode
- triggering condition

Allowed triggering shapes:

- bounded completion barrier
- periodic window-complete barrier
- explicit named operator milestone

Rules:

- named checkpoints must be monotonic per `(job_handle, checkpoint_name)`
- workflow-awaitable checkpoints must be durable before bridge acceptance
- checkpoint emission must reflect authoritative state already included in restoreable owner state

## Example Logical Jobs

### 1. Keyed Rolling Sum

`source -> filter(valid) -> key_by(accountId) -> reduce(sum amount) -> materialize(accountTotals) -> emit_checkpoint(batch-ready)`

### 2. Windowed Count

`topic -> key_by(customerId) -> window(tumbling 1m) -> reduce(count) -> materialize(minuteCounts) -> emit_checkpoint(minute-closed)`

### 3. Threshold Detector

`topic -> map(scoreInput) -> key_by(accountId) -> reduce(avg risk) -> materialize(riskScores) -> signal_workflow(threshold-crossed)`

## Compiler Responsibilities

For kernel v2, the compiler must:

- emit deterministic stream artifacts only
- normalize supported operators into the kernel-v2 graph shape
- reject unsupported operator combinations
- declare state and view identities explicitly
- classify the job as built-in fast-lane or reject it from kernel v2
- preserve source-map metadata for sources, operators, views, and checkpoints

The compiler must reject:

- arbitrary user closures that cannot compile into built-in expressions
- joins or window modes outside kernel v2
- queries whose consistency model is not explicit
- sinks that imply stronger guarantees than the kernel provides

## Runtime Responsibilities

For kernel v2, the Rust runtime must:

- validate the artifact shape before activation
- derive a typed kernel-v2 execution plan
- map aggregate declarations into built-in state kernels
- keep the hot path free of arbitrary language runtimes
- enforce owner-epoch and lease-epoch fencing on state mutation
- expose strong versus eventual reads clearly

The runtime should derive a plan resembling:

- source plan
- pre-key transform plan
- partitioning plan
- optional window plan
- aggregate state plan
- view plan
- checkpoint plan
- output plan

## Fast-Lane Constraints

Kernel v2 remains a built-in fast-lane contract.

That means:

- no arbitrary JS/TS execution in the hot loop
- no JSON as the hot-path state representation
- no SQL as the execution substrate
- no unbounded scans in owner-critical paths
- no hidden downgrade to a generic runtime without explicit job classification

If a job falls outside these constraints, it should either:

- fail compilation for kernel v2
- or be classified into a future generic extension lane

It must not silently masquerade as fast-lane execution.

## Storage Expectations

Kernel v2 relies on the state and implementation contracts already frozen elsewhere:

- authoritative state in RocksDB
- durable post-checkpoint progress in changelog records
- compact checkpoint artifacts
- owner-routed strong reads
- eventual projected read models for scans and browsing

The kernel may broaden the state declarations, but it must not violate those underlying storage rules.

## Benchmark Expectations

Kernel v2 should be benchmarked explicitly as a product surface, not only as an internal engine experiment.

The first benchmark suite should track:

- throughput by reducer kind
- throughput with and without tumbling windows
- checkpoint latency under bounded and continuous load
- strong-read latency
- owner handoff recovery time
- RocksDB bytes written per accepted event
- changelog entries per accepted event

The product promise only holds if these remain transparent.

## Non-Goals

Kernel v2 does not attempt to support:

- session windows
- joins across independently partitioned sources
- arbitrary user-defined reducers
- ad hoc SQL queries over live state
- globally consistent live scans

## Consequence

Kernel v2 is the first genuinely useful built-in stream kernel beyond the toy slice:

it supports real keyed aggregates, basic filtering/routing, tumbling windows, multiple views, and workflow-visible checkpoints while still preserving the core Fabrik Streams bet of compiled artifacts plus Rust-native execution.
