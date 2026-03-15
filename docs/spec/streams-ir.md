# Streams IR

## Status

This document defines the target compiled artifact model for `Fabrik Streams`.

It sits above the current narrow kernel in [streams-kernel-v1.md](/Users/bene/code/fabrik/docs/spec/streams-kernel-v1.md) and alongside the storage/runtime contracts in:

- [streams-runtime-model.md](/Users/bene/code/fabrik/docs/spec/streams-runtime-model.md)
- [streams-state-model.md](/Users/bene/code/fabrik/docs/spec/streams-state-model.md)
- [streams-state-implementation-v1.md](/Users/bene/code/fabrik/docs/spec/streams-state-implementation-v1.md)

## Purpose

`Fabrik Streams` should be code-first for authoring, but Rust-native for execution.

That requires a compiled boundary:

- stream jobs are authored in SDK code
- the compiler emits a deterministic stream artifact
- the runtime validates that artifact
- built-in Rust kernels execute the artifact without running arbitrary TypeScript in the hot loop

This document freezes that boundary.

## Core Rule

Code-first does not mean "execute arbitrary user code in the hot path."

The execution model is:

- code-first authoring
- compiled stream IR
- Rust-native built-in operator kernels
- explicit fast-lane versus generic execution modes

The compiler may describe custom logic, but the runtime must know exactly which execution contract it is accepting.

## Scope

This document defines:

- the target stream artifact shape
- the semantic sections of stream IR
- built-in operator families
- kernel planning rules
- the execution contract between compiler and Rust runtime

It does not define:

- the final SDK syntax
- a SQL authoring model
- every future source and sink
- UI layout

## Why Streams Need Their Own IR

Workflow IR and stream IR solve different problems.

Workflow IR optimizes for:

- deterministic orchestration
- replay through durable workflow history
- short decision turns
- workflow-authoritative control flow

Stream IR optimizes for:

- partition-owned execution
- keyed state
- windows and timers
- materialized views
- checkpoint plus progress-tail recovery
- high-volume Rust-native execution

The two systems should integrate tightly, but they should not share one execution artifact.

## Artifact Shape

Every compiled stream artifact must contain:

- `definition_id`
- `definition_version`
- `artifact_hash`
- `compiler_version`
- `runtime_contract`
- `source_language`
- `entrypoint`
- `source_files`
- `source_map`
- stream job metadata
- source definitions
- operator graph
- state declarations
- view declarations
- query declarations
- checkpoint policy

The current persisted artifact shape already includes:

- `definition_id`
- `definition_version`
- `compiler_version`
- `runtime_contract`
- `source_language`
- `entrypoint`
- `source_files`
- `source_map`
- `job`
- `artifact_hash`

That current shape is the correct base.

## Top-Level Job Sections

The semantic sections of one compiled stream job are:

### 1. Job Metadata

Required fields:

- stable job name
- runtime identity
- artifact identity
- compatibility contract

Optional future fields:

- job description
- default partitioning policy
- retention defaults
- deployment hints

### 2. Sources

Source definitions describe how events enter the job.

Initial families:

- `bounded_input`
- `topic`
- `changefeed`
- `object_store`
- `http_ingress`
- `synthetic`

Every source definition must declare:

- source kind
- payload schema or shape reference
- ordering assumptions where relevant
- external cursor or offset identity where relevant

### 3. Operator Graph

The operator graph is the compiled computation plan.

Each operator must declare:

- stable operator id or name
- operator kind
- input edge identity
- output edge identity or routing
- static config
- state declarations when stateful
- source-map metadata

The graph may be expressed as a linear list for narrow kernels, but the semantic contract is a directed operator graph, not "just an array of callbacks."

### 4. State Declarations

The artifact must declare state explicitly rather than smuggling it through arbitrary user code.

State declarations may include:

- keyed state
- window state
- timers
- dedupe state
- join buffers
- materialized view state

Each declared state object should identify:

- state id
- state kind
- key shape
- value shape
- retention or TTL
- checkpoint requirements

### 5. Views And Queries

Materialized views and query surfaces are separate first-class declarations.

Each view must declare:

- view name
- key shape
- value shape
- consistency modes it supports
- query mode such as `by_key`, `prefix_scan`, or `diagnostic`

Each query must declare:

- query name
- target view or query plan
- supported consistency
- argument shape

### 6. Checkpoint Policy

Checkpoint policy is part of the artifact, not a runtime guess.

The artifact may declare:

- named checkpoints
- checkpoint delivery mode
- monotonic sequence rules
- optional periodic policy hints

## Operator Families

The runtime should support explicit operator families rather than arbitrary embedded code.

### Stateless Transform Family

Operators:

- `map`
- `filter`
- `project`
- `route`
- `branch`

Properties:

- no persistent state
- deterministic and data-local
- suitable for the fastest execution lane

### Partitioning Family

Operators:

- `key_by`
- `partition_by`
- `rebalance`

Properties:

- define ownership routing
- may change partition locality
- must remain explicit in the graph because they affect state placement

### Stateful Aggregate Family

Operators:

- `reduce`
- `aggregate`
- `dedupe`
- `materialize`

Properties:

- mutate keyed authoritative state
- checkpointed and replayable
- primary fit for the Rust fast lane

### Window And Time Family

Operators:

- `window`
- `watermark`
- `schedule_timer`
- `on_timer`
- `close_window`

Properties:

- depend on time frontier semantics
- must declare retention and close behavior explicitly
- must restore with enough timer and watermark metadata to resume correctly

### Join And Enrichment Family

Operators:

- `join`
- `lookup`
- `enrich`

Properties:

- may require buffered state
- may require read-through behavior
- should declare whether they execute in the built-in fast lane or a slower generic lane

### Bridge And Output Family

Operators:

- `emit_checkpoint`
- `signal_workflow`
- `emit_event`
- `sink`

Properties:

- mark durable milestones or outputs
- may cross subsystem boundaries
- must remain explicit because bridge semantics and sink semantics are not interchangeable

## Kernel Classes

The runtime should execute stream IR through built-in kernel classes.

### Fast-Lane Kernels

Fast-lane kernels are Rust-native built-ins for common high-volume shapes.

Examples:

- keyed rollup
- keyed sum/min/max/avg
- keyed dedupe
- keyed threshold detector
- fixed-shape tumbling window aggregate

Properties:

- no arbitrary user code in the hot loop
- compact binary state
- predictable RocksDB writes
- explicit benchmark targets

### Structured Stateful Kernels

These remain built-in Rust kernels, but with broader semantics than the initial fast lane.

Examples:

- session windows
- join buffers
- watermark-driven aggregates
- multi-view materializers

Properties:

- still artifact-driven
- still validated statically
- may carry higher restore and checkpoint cost

### Generic Extension Lane

If custom user logic is introduced later, it should be visibly separate from the fast lane.

Possible future forms:

- Wasm UDFs
- sandboxed extension calls
- restricted expression engines

Rules:

- generic extension logic must never pretend to have the same performance envelope as built-in kernels
- the artifact must declare the presence of generic execution explicitly
- operator and benchmark surfaces should expose whether a job is all built-in or mixed-mode

## Planning Model

The runtime should not interpret the artifact ad hoc on every event.

Instead:

1. validate the stored artifact
2. derive a typed kernel plan
3. bind state ids, views, queries, and checkpoint metadata
4. execute the typed plan in Rust

The current `keyed_rollup_kernel()` derivation in the Rust implementation is the first example of this planning model.

## Runtime Contract

The Rust runtime must:

- reject artifacts whose `runtime_contract` it does not support
- validate graph shape before activation
- derive typed execution kernels before mutation
- fence unsupported operators or unsupported combinations deterministically
- preserve source locations for operator-level diagnostics

The compiler must:

- emit only statically serializable stream artifacts
- assign stable operator and view identities
- declare state and checkpoint semantics explicitly
- preserve source-map locations for user-authored job sections
- choose a runtime contract that matches the operators actually emitted

## Versioning

Every stream artifact must be pinned and hashable.

Rules:

- a running stream job is pinned to one artifact hash
- runtime validation must fail deterministically on incompatible artifacts
- artifact evolution must be explicit when state or checkpoint compatibility changes

Future migration tools may classify changes such as:

- metadata-only
- query/view additive
- state-layout changing
- incompatible checkpoint restore

## Source Maps And Inspection

The stream artifact should preserve enough source metadata to make the runtime explainable.

At minimum, source maps should support:

- operator-to-source lookup
- view-to-source lookup
- checkpoint declaration lookup
- query declaration lookup

This is required for:

- graph explorer
- causality inspection
- replay/debug tooling
- error messages that point back to authored code

## Current Narrow Contract

The current shipping stream compiler/runtime slice is intentionally narrow:

- `runtime_contract: "streams_kernel_v1"`
- one fixed `keyed_rollup` runtime
- one bounded input source
- one reduce operator
- one named checkpoint operator
- one strong keyed materialized view

That narrow contract remains frozen in [streams-kernel-v1.md](/Users/bene/code/fabrik/docs/spec/streams-kernel-v1.md).

## Target Next Contract

The next broader stream IR contract should support at least:

- multiple built-in reducer kinds
- explicit `map`, `filter`, and `route`
- declared keyed state separate from the materialized view
- tumbling windows
- explicit timer declarations
- multiple views and queries
- explicit fast-lane versus mixed-mode job classification

That broader contract does not need to ship all operators at once.
It only needs to freeze the artifact vocabulary so compiler and runtime can evolve without drift.
The first concrete target for that broader built-in contract is [streams-kernel-v2.md](/Users/bene/code/fabrik/docs/spec/streams-kernel-v2.md).

## Non-Goals

This document does not require:

- SQL as the primary authoring model
- arbitrary user JavaScript or TypeScript in the hot loop
- one universal kernel that covers every future job shape
- hiding performance tradeoffs between built-in and generic execution

## Consequence

`Fabrik Streams` now has a clean authoring-to-runtime story:

users author stream jobs in code, the compiler emits a typed stream artifact, the Rust runtime validates and plans that artifact into built-in execution kernels, and the hot path stays architecture-legible instead of becoming an opaque embedded language runtime.
