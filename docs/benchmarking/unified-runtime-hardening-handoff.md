# Unified Runtime Hardening Handoff

## Purpose
This document is a handoff for analysis of the experimental unified runtime on the `fabrik` branch. The runtime was built as a branch-only throughput experiment to test whether collapsing the durable hot path into a single shard-owner service materially changes the performance envelope.

The throughput experiment succeeded. The next phase is not more architecture discovery; it is hardening analysis: recovery semantics, correctness under failure, replay guarantees, and ownership transfer.

## Current State
The experimental runtime lives primarily in:

- [/Users/bene/code/fabrik/services/unified-runtime/src/main.rs](/Users/bene/code/fabrik/services/unified-runtime/src/main.rs)
- [/Users/bene/code/fabrik/services/activity-worker-service/src/main.rs](/Users/bene/code/fabrik/services/activity-worker-service/src/main.rs)
- [/Users/bene/code/fabrik/crates/fabrik-store/src/lib.rs](/Users/bene/code/fabrik/crates/fabrik-store/src/lib.rs)
- [/Users/bene/code/fabrik/scripts/run-isolated-benchmark.sh](/Users/bene/code/fabrik/scripts/run-isolated-benchmark.sh)
- [/Users/bene/code/fabrik/benchmarks/temporal-comparison/runner.mjs](/Users/bene/code/fabrik/benchmarks/temporal-comparison/runner.mjs)
- [/Users/bene/code/fabrik/services/benchmark-runner/src/main.rs](/Users/bene/code/fabrik/services/benchmark-runner/src/main.rs)

The unified runtime currently supports a narrow subset aimed at the benchmark-critical path:

- compiled workflow trigger
- fan-out of normal activities
- fan-in via `wait_for_all_activities`
- `all_settled` and `count`-style reducer behavior
- basic retry
- basic cancellation handling
- local snapshot/log persistence

It is intentionally not feature-complete. It is not yet a general replacement for the existing durable engine.

## Hardening Status
The unified runtime is no longer just a fast prototype. The following hardening slices are already implemented in the branch:

- late and duplicate activity results are explicitly rejected for:
  - missing instance
  - terminal instance
  - missing activity
  - stale attempt
  - missing lease
  - worker mismatch
- per-lease `lease_epoch` fencing is carried through poll and report paths
- per-owner `owner_epoch` fencing is enforced on activity result apply
- startup requires a shared Postgres ownership lease before serving work
- ownership renewal is continuous, and ownership loss fail-stops the process
- local restore requeues lost leased work and promotes due retries immediately
- shared Postgres snapshots are written for unified runtime instances
- restore can recover from shared store snapshots plus `workflow_activities`
- restore replays shared broker tail on top of shared snapshots
- startup reconciles local checkpoint state against shared recovery and prefers the fresher per-run state
- stale local ready, leased, and delayed-retry work is removed when shared recovery wins

The branch also has targeted tests for these behaviors in [/Users/bene/code/fabrik/services/unified-runtime/src/main.rs](/Users/bene/code/fabrik/services/unified-runtime/src/main.rs), including:

- stale and duplicate result filtering
- restore requeue of leased and due-retry work
- owner and lease epoch stamping
- restore preferring newer log state over snapshot state
- shared snapshot plus broker-tail handoff recovery
- stale-local vs fresher-shared reconciliation
- fresher-local vs older-shared non-downgrade

Latest isolated regression check after the recovery-reconciliation pass:

- [/Users/bene/code/fabrik/target/benchmark-reports/unified-baseline-final-hardening-check.json](/Users/bene/code/fabrik/target/benchmark-reports/unified-baseline-final-hardening-check.json)
- `execution_duration_ms=378`
- `throughput_activities_per_second=2645.50`
- `completed=10`
- `failed=0`

## What Changed the Throughput Curve
The experiment only became truly fast after the following changes:

1. Remove useless bulk worker lanes for unified mode.
2. Increase normal activity poll batch size in unified mode.
3. Debounce persistence so report handling does not rewrite full state on every batch.
4. Batch terminal activity writes.
5. Fix ready-queue ordering so tasks are only exposed to polling after schedule/requeue DB writes succeed.
6. Batch scheduled activity inserts into Postgres.

The key lesson is that the largest remaining overhead was not workflow execution logic; it was coordination churn between in-memory queues and DB visibility writes.

## Fresh Full Smoke Suite Result
Source:

- [/Users/bene/code/fabrik/target/benchmark-reports/temporal-comparison-smoke.json](/Users/bene/code/fabrik/target/benchmark-reports/temporal-comparison-smoke.json)
- [/Users/bene/code/fabrik/target/benchmark-reports/temporal-comparison-smoke.txt](/Users/bene/code/fabrik/target/benchmark-reports/temporal-comparison-smoke.txt)

Throughput, activities/s:

| Workload | Temporal | Durable | Unified |
|---|---:|---:|---:|
| fanout-baseline | 349.81 | 158.20 | 3631.21 |
| fanout-retry | 299.30 | 154.42 | 1060.04 |
| fanout-cancel | 338.48 | 158.35 | 3820.90 |
| fanout-payload-heavy | 305.49 | 140.43 | 1729.73 |
| fanout-wide | 34.29 | 158.54 | 6329.67 |

Notes:

- The `fanout-wide` Temporal result in this specific run is anomalously low relative to earlier smoke runs, so that ratio should not be treated as the main headline.
- Even ignoring that anomaly, unified is now materially ahead of both Temporal and current durable Fabrik across the suite.

## Runtime Shape After Optimization
Representative unified debug state from the latest full run:

- baseline report: [/Users/bene/code/fabrik/target/benchmark-reports/fanout-baseline-fabrik-r1-unified-experiment.json](/Users/bene/code/fabrik/target/benchmark-reports/fanout-baseline-fabrik-r1-unified-experiment.json)
- retry report: [/Users/bene/code/fabrik/target/benchmark-reports/fanout-retry-fabrik-r1-unified-experiment.json](/Users/bene/code/fabrik/target/benchmark-reports/fanout-retry-fabrik-r1-unified-experiment.json)
- cancel report: [/Users/bene/code/fabrik/target/benchmark-reports/fanout-cancel-fabrik-r1-unified-experiment.json](/Users/bene/code/fabrik/target/benchmark-reports/fanout-cancel-fabrik-r1-unified-experiment.json)
- payload-heavy report: [/Users/bene/code/fabrik/target/benchmark-reports/fanout-payload-heavy-fabrik-r1-unified-experiment.json](/Users/bene/code/fabrik/target/benchmark-reports/fanout-payload-heavy-fabrik-r1-unified-experiment.json)
- wide report: [/Users/bene/code/fabrik/target/benchmark-reports/fanout-wide-fabrik-r1-unified-experiment.json](/Users/bene/code/fabrik/target/benchmark-reports/fanout-wide-fabrik-r1-unified-experiment.json)

Important signs:

- baseline poll requests dropped to `56` for `1536` activities
- baseline report batches applied dropped to `16`
- baseline log writes dropped to `1`
- baseline snapshot writes dropped to `1`
- payload-heavy poll requests dropped to `32`
- wide poll requests were only `84` for `2304` activities

This means the original dominant churn modes are now largely controlled:

- worker poll churn
- per-batch persistence churn
- per-activity schedule upsert churn

## Architecture Summary
The unified runtime currently behaves like this:

1. Trigger event arrives.
2. Compiled workflow executes in-memory.
3. Activities are scheduled in a batch into `workflow_activities`.
4. Only after DB schedule success are tasks made ready in memory.
5. Workers poll normal activities in batches from the runtime.
6. Completion reports are applied in-memory and written to DB in batches.
7. Dirty runtime state is persisted asynchronously to local snapshot/log files.

The design center is now clear:

- in-memory state machine first
- DB for visibility and durable activity records
- compact persistence cadence
- shard-local ownership

## What Is Still Experimental
The experiment is fast, but several production properties remain under-specified or weak:

- local file persistence instead of a production-grade replicated log/snapshot design
- no live ownership handoff story
- no proven exactly-once or at-least-once external effect boundary model
- no feature parity with broader workflow semantics
- no mature replay validation strategy
- no robust crash injection test suite

## Future Hardening Milestone
The next milestone should be treated as a dedicated hardening project, not a side quest during performance work.

Goal:

- turn the unified runtime from “fast and partially hardened” into “architecturally credible under owner loss, replay, and duplicate delivery”

Exit criteria:

- no ambiguous source of truth during recovery
- fenced owner transitions across real handoff, not just fail-stop
- deterministic replay for the benchmark subset from shared state alone
- crash-injection coverage for the major transition windows
- explicit duplicate-delivery contract for trigger, start, and completion paths

Scope for the milestone:

### 1. Shared Source Of Truth
- choose one authoritative recovery model and document it rigorously
- likely direction: shard log plus snapshots as authority, with Postgres rows treated as projections
- remove any remaining ambiguity between:
  - local checkpoint files
  - shared snapshots
  - broker tail
  - `workflow_activities`
  - `workflow_instances`

### 2. Live Ownership Handoff
- replace fail-stop-only ownership loss with a real takeover story
- define the persisted fencing contract for:
  - schedule
  - start
  - completion apply
  - snapshot/log write
- make a new owner resume from a precise replay point without relying on local files from the old owner

### 3. Recovery Correctness Under Crash Windows
- cover the crash windows that still matter to the benchmark subset:
  - crash after DB schedule success but before ready-queue exposure
  - crash after completion terminal write but before join-state persistence
  - crash after retry scheduling but before retry persistence
  - crash during snapshot/log flush
- prove whether each case yields:
  - replay and reschedule
  - replay and ignore duplicate
  - or terminal no-op

### 4. Duplicate And Stale Delivery Contract
- specify the runtime contract for:
  - duplicate trigger
  - duplicate completion
  - stale completion from superseded attempt
  - late completion after workflow terminal
  - stale worker from prior owner epoch
- make the contract explicit in code and tests rather than inferred from guards

### 5. Failure Injection And Load Validation
- add restart and ownership-loss tests that run under load, not just unit-style recovery tests
- include:
  - owner crash during fan-out scheduling
  - owner crash during completion drain
  - owner replacement after expired lease
  - recovery from shared snapshot plus broker tail with missing local files
- collect both correctness and throughput deltas so hardening regressions are visible

### 6. Productization Readiness Boundary
- decide what remains benchmark-only behavior versus what is part of the intended production contract
- explicitly defer non-goals until the above invariants are closed:
  - broad workflow feature parity
  - full query/history fidelity beyond the benchmark subset

Suggested implementation order:

1. formalize shared source of truth
2. implement real live owner handoff and persisted fencing
3. close duplicate/stale delivery semantics
4. build crash-injection and load recovery coverage
5. only then expand feature surface

## Primary Hardening Questions
The analysis agent should focus on these questions, in this order.

### 1. Recovery Model
Current runtime durability is coarse. It persists snapshot/log locally and uses Postgres for visibility rows, but the formal source of truth is not yet nailed down.

Questions:

- What is the authoritative recovery source after owner crash: runtime snapshot/log, Postgres activity rows, broker events, or some combination?
- Can recovery reconstruct in-flight leased activities without double-starting or losing them?
- Is there any state transition that is visible in DB but absent from runtime persistence, or vice versa?
- What invariants must hold between:
  - runtime in-memory state
  - local snapshot/log
  - `workflow_activities`
  - `workflow_instances`
  - broker-trigger history

### 2. External Consistency Windows
The runtime now intentionally decouples in-memory progress from persistence flush cadence. That helped throughput, but correctness windows need precise analysis.

Questions:

- After DB activity scheduling succeeds but before snapshot/log flush, what happens on crash?
- After terminal activity DB write succeeds but before runtime snapshot/log flush, what happens on crash?
- Are there cases where a workflow can replay and reschedule work already reflected in DB?
- Is replay idempotent across:
  - scheduled rows
  - started rows
  - completed/failed/cancelled rows
  - closed workflow instances

### 3. Duplicate Delivery and Idempotency
The runtime currently assumes a fair amount of benign behavior in the benchmark path.

Questions:

- What happens if a worker reports the same completion twice?
- What happens if a late completion arrives after retry attempt `n+1` is already active?
- What happens if a completion arrives after workflow terminal resolution?
- What happens if trigger delivery is duplicated across shard ownership changes?
- Is dedupe keyed correctly on `(tenant, instance, run, activity_id, attempt)` everywhere it needs to be?

### 4. Ownership Handoff
The experiment does not yet have a real multi-owner model.

Questions:

- How should shard ownership be represented durably?
- How should lease expiry, steal, or handoff work when an owner dies mid-flight?
- What fencing token model should protect:
  - activity schedule
  - activity start
  - activity completion apply
  - snapshot/log write
- What exact replay point should a new owner start from?

### 5. Replay Semantics
The analysis should define whether replay is:

- full state reload from latest snapshot
- snapshot plus tail log
- or DB-assisted reconstruction

Questions:

- Which state is recomputed vs persisted?
- What fields are derived and should not be stored?
- What minimum persisted state is needed for deterministic restart?
- Can replay be validated against existing compiled workflow semantics?

### 6. Durability Scope
The branch still uses Postgres heavily enough that durability semantics may be split awkwardly between DB records and local files.

Questions:

- Should the productionized unified runtime keep Postgres in the write path for activity visibility?
- Should activity rows become a derived projection rather than part of the execution path?
- Is a proper append-only shard log required before productization?
- If yes, which existing storage component should carry it?

## Concrete Failure Modes To Analyze
These should be modeled and, later, tested explicitly.

### Trigger path

- crash after artifact execution but before scheduled rows are written
- crash after scheduled rows are written but before tasks are made ready in memory
- duplicate trigger after partial start

### Poll/start path

- worker leases a batch, owner crashes before start rows are durable
- owner marks started, crashes before persistence flush
- worker times out and owner requeues while original worker still completes

### Completion path

- terminal DB write succeeds, runtime crashes before join state flush
- retry scheduled in-memory but owner crashes before retry release is persisted
- completion arrives twice
- completion for stale attempt arrives after next attempt started

### Terminal workflow path

- workflow completes in-memory, DB upsert succeeds, snapshot flush fails
- closed run written twice after replay
- late activity result arrives after close

### Persistence path

- snapshot write succeeds, log append fails
- log append succeeds, snapshot write fails
- partial or corrupt local snapshot/log
- snapshot older than DB visibility state

## Recommended Analysis Deliverables
The next agent should ideally produce:

1. A state machine / invariant document for unified runtime durability.
2. A failure matrix covering each transition and crash point.
3. A proposed production durability model:
   - shard log
   - snapshot policy
   - owner fencing
   - replay source of truth
4. A duplicate-delivery contract for trigger, start, and completion paths.
5. A prioritized hardening backlog with severity and implementation order.

## Suggested Priority Order
1. Define authoritative durability and replay model.
2. Define ownership and fencing model.
3. Define duplicate and stale-result semantics.
4. Add crash-injection and replay tests for the benchmark subset.
5. Expand feature surface only after the core invariants are stable.

## Bottom Line
The throughput experiment has already answered the architecture question: collapsing the durable hot path into a unified shard-owner runtime works and changes the ceiling dramatically.

The main risk now is not performance. The main risk is semantic ambiguity in recovery, ownership, and duplicate handling. That is what should be analyzed next.
