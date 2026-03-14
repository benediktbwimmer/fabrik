# ADR 0005: Stream-Native Throughput Engine

## Status

Accepted

## Date

2026-03-13

## Context

Throughput mode exists to make very large fan-out / fan-in workloads materially faster than the fully durable workflow engine. That promise is not being met by the current `stream-v2` architecture.

The current `stream-v2` path still preserves too much workflow-shaped control-plane work on the hot path:

- `unified-runtime` persists full or near-full workflow instance state at trigger time
- workflow history emits `BulkActivityBatchScheduled` and the async `workflow_event_outbox` republishes it
- `throughput_bridge_progress` tracks whether the bridge published the stream command
- `streams-runtime` rebuilds batch state from the bridged event
- `throughput_projection_batches` and `throughput_projection_chunks` are updated continuously for visibility

This means throughput work does not become truly stream-native until after the workflow runtime has already paid multiple JSON, SQL, and broker costs. Benchmarks have shown only modest gains over durable mode, which indicates the system is still bottlenecked by the workflow-era control plane rather than the chunk execution path.

We already validated that local persistence bloat was one contributor, but removing that did not close the gap. The remaining problem is architectural: `stream-v2` still behaves like "workflow mode plus a bridge" instead of a dedicated throughput engine.

## Decision

We will rewrite `stream-v2` as a stream-native throughput engine with a minimal workflow admission boundary.

### 1. `unified-runtime` becomes admission and resume only

For `ctx.bulkActivity()` batches admitted to `stream-v2`, `unified-runtime` will:

- validate the compiled bulk configuration
- resolve or externalize the input payload handle
- persist a minimal durable throughput admission record
- persist enough workflow resume metadata to continue at the barrier later
- emit one `StartThroughputRun` command
- stop participating in nonterminal chunk lifecycle management

`unified-runtime` will resume ownership only when it receives one terminal run outcome:

- `ThroughputRunCompleted`
- `ThroughputRunFailed`
- `ThroughputRunCancelled`

At that point it appends the corresponding authoritative workflow event and resumes the compiled workflow turn.

### 2. `streams-runtime` becomes the sole owner of throughput execution

`streams-runtime` will own the full nonterminal lifecycle of a throughput run:

- input expansion and chunk planning
- partition-local scheduling and admission control
- worker leasing and result ingestion
- retry policy enforcement
- reducer state and aggregation trees
- cancellation fanout
- checkpointing and restore
- terminalization and final result materialization

This removes the "workflow event -> bridge -> create batch -> rebuild state" pipeline from the hot path.

### 3. Hot execution state moves out of SQL row-per-chunk storage

The source of truth for active `stream-v2` runs will be dedicated throughput-run state, not the current workflow-oriented SQL tables.

The target durable model is:

- `throughput_runs`
  - one row per admitted run
  - identity, workflow resume target, routing policy, reducer config, payload handles, coarse status
- `throughput_checkpoints`
  - owner epoch, reducer state, progress watermark/bitmap, retry schedule, lease state snapshot
- `throughput_terminals`
  - terminal counters, terminal error, final reducer output, result handle

Derived visibility rows may still exist, but they are not the execution substrate.

### 3a. Durable progress between checkpoints uses a write-ahead report log

Checkpoints alone are not sufficient to preserve accepted worker progress. `stream-v2` therefore requires a durable progress tail between checkpoints.

The chosen model is a write-ahead report log:

- before a worker result is acknowledged, `streams-runtime` durably appends a report record
- restore rebuilds from the latest checkpoint plus the durable report tail after that checkpoint
- checkpoints compact prior report-log progress but do not replace the need for a durable tail

Each durably captured report record must include enough fenced identity to make replay idempotent:

- `run_id`
- `chunk_id`
- `attempt`
- `lease_epoch`
- `owner_epoch`
- `report_id`
- terminal status
- result summary and payload handle

The hot-path rule is:

- a worker result is authoritative only after its fenced report has been durably captured
- in-memory apply before durable capture is not sufficient

This gives `stream-v2` the same safety property we expect from a log-first system: accepted progress is never represented only in RAM.

### 4. Worker interaction becomes partition-native

Workers for `stream-v2` will poll and report against partition-owned throughput lanes, not a workflow-shaped compatibility layer.

The hot worker envelope should be limited to execution fields such as:

- `run_id`
- `chunk_id`
- `attempt`
- `lease_epoch`
- `owner_epoch`
- payload handle or inline payload reference
- cancellation / retry fields

Workflow instance identity remains available for diagnostics, but it is not the primary execution key on the hot path.

### 5. Durability is preserved at the right boundary

The durable contract for `stream-v2` becomes:

- the trigger was accepted
- the throughput run was admitted with a stable identity
- checkpoints plus the durable report tail are sufficient to recover active progress after failover
- exactly one terminal run outcome is committed
- exactly one corresponding workflow barrier event is appended

We explicitly do not require every internal chunk transition to be synchronously queryable in SQL as part of the correctness path.

### 6. Terminal barrier handoff is an explicit protocol

Exactly-once workflow barrier emission is a first-class protocol, not an informal consequence.

The terminalization rule is:

- nonterminal run state may transition to terminal exactly once
- terminal commit is fenced by `run_id` and the active `owner_epoch`
- `throughput_terminals` enforces one durable terminal record per run
- the handoff from `streams-runtime` to `unified-runtime` uses a durable terminal outbox or equivalent atomic terminal command
- `unified-runtime` deduplicates terminal handoff by `run_id` plus terminal sequencing metadata

The workflow-visible barrier event is appended only from that durable terminal handoff, never from transient in-memory state.

## Execution Model

The intended `stream-v2` lifecycle is:

1. `unified-runtime` admits a `stream-v2` bulk step and writes a `throughput_runs` row plus minimal workflow resume state.
2. `unified-runtime` emits `StartThroughputRun`.
3. `streams-runtime` expands the payload into chunk groups, schedules work, and checkpoints active state.
4. workers poll partition-owned chunk leases and report terminal chunk results directly to `streams-runtime`.
5. `streams-runtime` reduces results, retries failed chunks when needed, and computes the terminal run outcome.
6. `streams-runtime` emits exactly one terminal command back to `unified-runtime`.
7. `unified-runtime` appends the authoritative workflow barrier event and resumes the workflow.

## Correctness Invariants

1. `streams-runtime` is authoritative for all nonterminal progress of a `stream-v2` run.
2. `unified-runtime` is authoritative only for workflow-visible barrier semantics.
3. A worker result is authoritative only after it is durably captured in the report log.
4. A throughput run has exactly one durable terminal outcome.
5. A workflow receives exactly one corresponding barrier event for that outcome.
6. Owner changes fence all prior leases and result applications by epoch.
7. Restore starts from the latest durable checkpoint plus durable progress tail, never from projections.

## Ownership And Fencing

Because `streams-runtime` is the sole nonterminal owner, epoch fencing is part of the core execution model.

The ownership model is:

- each throughput partition has exactly one active owner at a time
- ownership changes increment `owner_epoch`
- checkpoints and durable report records are tagged with `owner_epoch`
- a new owner restores from the latest checkpoint plus the post-checkpoint durable report tail for that epoch lineage

The lease model is:

- each chunk lease carries `attempt`, `lease_epoch`, `lease_token`, and `owner_epoch`
- retries advance `lease_epoch`
- owner failover advances `owner_epoch` and invalidates all leases from older owners
- worker reports with stale `owner_epoch`, stale `lease_epoch`, or mismatched `lease_token` are rejected for mutation and may be retained only for audit/debug

Any state mutation derived from worker execution must be fenced by the active owner epoch. Old owners may continue to emit messages transiently, but they cannot make forward progress once fenced out.

## Checkpointing

Checkpoints are intentionally coarse-grained.

The checkpointing rules are:

- checkpoints are periodic and compact, not per-report
- reducer state is checkpointed in reduced form, not as full per-item materialized payloads
- active progress between checkpoints is preserved by the durable report log
- async visibility projections are not part of restore correctness

The design goal is to avoid replacing SQL row churn with checkpoint churn.

## What Leaves the Hot Path

The following objects are no longer part of `stream-v2` execution correctness and must be removed from the hot path:

- `workflow_instances.state` updates for nonterminal throughput progress
- `workflow_event_outbox` publication of internal throughput scheduling intent
- `throughput_bridge_progress`
- `workflow_bulk_chunks` for `stream-v2`
- `throughput_projection_batches` and `throughput_projection_chunks` as execution state
- per-chunk SQL visibility writes required before work can continue

These may remain temporarily as compatibility projections or migration scaffolding, but they are not the target design.

## Consequences

### Positive

- the throughput hot path finally matches the intended execution model
- `stream-v2` can scale with partition-local state and checkpoints instead of SQL row churn
- workflow runtime latency stops being coupled to chunk scheduling and reduction
- the service boundary becomes cleaner: workflow barrier semantics stay in `unified-runtime`, throughput execution stays in `streams-runtime`

### Negative

- the rewrite is invasive across storage, RPC, and recovery
- some current debugging and SQL introspection paths will become asynchronous projections rather than authoritative state
- migration must preserve exact-once barrier emission across mixed old/new runs

## Migration Plan

### Phase 0: Freeze the protocol

Before deep implementation work, define and lock the protocol for:

- admission durability
- worker report ack semantics
- durable report-log capture
- checkpoint contents and cadence
- ownership and fencing
- terminal handoff to `unified-runtime`
- restore source of truth

This phase exists to keep storage, RPC, and failover semantics from drifting while the rewrite is underway.

### Phase 1: Introduce direct throughput admission

- add `throughput_runs`, `throughput_checkpoints`, and `throughput_terminals`
- add the durable throughput report log
- teach `unified-runtime` to write throughput admission records
- publish `StartThroughputRun` directly from admission, while keeping the legacy bridge available behind a compatibility flag

### Phase 2: Make `streams-runtime` restore from checkpoints, not bridged workflow state

- build run state directly from `throughput_runs`, checkpoints, and the durable report tail
- move chunk planning and reducer initialization fully into `streams-runtime`
- stop requiring `throughput_projection_*` rows for restore

### Phase 3: Move worker polling and reporting to run-native state

- switch the worker RPC surface to run/chunk leases owned by `streams-runtime`
- stop depending on workflow-shaped batch/chunk SQL records for execution

### Phase 4: Return only terminal outcomes to the workflow runtime

- replace the current internal bridge-back path with terminal run commands
- keep workflow history semantics unchanged at the barrier

### Phase 5: Demote legacy bridge tables to compatibility projections

- remove `throughput_bridge_progress` from execution
- stop writing `workflow_bulk_chunks` for `stream-v2`
- treat `throughput_projection_batches` and `throughput_projection_chunks` as optional visibility materializations

## Non-Goals

- changing the product-facing `ctx.bulkActivity()` API
- changing historical batch semantics
- changing workflow-visible barrier semantics
- making per-item workflow history durable for throughput mode

## Guardrails

- historical rows remain readable during the rewrite
- in-flight `stream-v2` runs must finish on the execution model they started with
- restore must be validated under owner failover before removing any legacy path
- benchmark success is not enough on its own; crash recovery and exactly-once terminal barrier emission must remain intact
