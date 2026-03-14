# Roadmap

## Guiding Goal

Build `fabrik` into a high-performance Temporal replacement with:

- feature parity for core durable execution semantics
- lower workflow-task latency
- higher aggregate throughput
- stronger scale characteristics for heavy fan-out / fan-in workloads

Current Temporal TypeScript blocker ordering is tracked separately in
[`temporal-ts-replacement-priorities.md`](/Users/bene/code/fabrik/docs/temporal-ts-replacement-priorities.md),
which is generated from the official Temporal samples repo census and should guide the next parity slices.

## Phase 0: Product Realignment

Goal: align the product model, docs, and architecture around Temporal parity.

Deliverables:

- updated product model
- updated architecture docs
- semantic specs for activities, task queues, updates, visibility, and worker versioning
- explicit rejection of the older connector-first activity model

Exit criteria:

- the repo docs describe one coherent target platform
- workflow and activity semantics match the intended Temporal-compatible direction

## Phase 1: Workflow Runtime Core

Goal: make the workflow side of the runtime production-shaped.

Deliverables:

- compiled workflow execution artifacts
- workflow task dispatch
- sticky execution and shard ownership
- durable timers
- snapshots plus replay
- continue-as-new
- deterministic side-effect and version markers

Exit criteria:

- hot workflow execution avoids replay in the steady state
- replay reproduces workflow state exactly
- failover is replay-safe

## Phase 2: Activity Runtime and Task Queues

Goal: add a real Temporal-equivalent activity model.

Deliverables:

- activity task queues
- worker polling protocol
- arbitrary user activity execution
- activity retries and timeout handling
- heartbeat and cancellation delivery
- task queue backlog and poller visibility

Exit criteria:

- activities are no longer limited to built-in handlers or connectors
- long-running activities heartbeat and cancel correctly
- high-volume activity scheduling and completion are stable

## Phase 3: API Parity

Goal: expose the user-facing workflow control surface expected from a Temporal replacement.

Deliverables:

- start, signal, query, update, cancel, terminate APIs
- query consistency rules
- update acceptance and completion semantics
- child workflow lifecycle support
- idempotent request handling

Exit criteria:

- a realistic Temporal-style application model can run end to end

## Phase 4: SDK Parity

Goal: make the developer experience competitive.

Deliverables:

- workflow SDK with deterministic workflow primitives
- activity stubs and configuration APIs
- child workflow APIs
- signal, query, and update handlers
- testing harnesses and replay tests
- interceptors or middleware hooks

Exit criteria:

- developers can port representative Temporal workflow patterns without changing the mental model

## Phase 5: Versioning and Visibility

Goal: make production upgrades and operations safe.

Deliverables:

- worker build IDs and compatibility routing
- workflow artifact pinning
- search attributes and memo
- list and filter visibility APIs
- task queue and worker fleet observability

Exit criteria:

- workflow and worker rollouts are replay-safe
- operators can inspect task queues and workflows at fleet scale

## Phase 6: Scale Validation

Goal: prove the architecture under production-shaped load.

Deliverables:

- large fan-out / fan-in benchmarks
- high-event-rate history ingestion benchmarks
- batched completion ingestion
- sticky-cache eviction and restore tuning
- shard rebalance and chaos testing

Exit criteria:

- large activity fan-out does not collapse workflow latency
- task queue throughput scales horizontally
- failover preserves correctness under load

## Phase 6.5: Throughput Mode and Bridge Extraction

Goal: enable high-cardinality bulk workloads with chunk-level durability while making the `Workflows -> Bridge -> Streams` split explicit.

Deliverables:

- `ctx.bulkActivity()` workflow primitive
- `start_bulk_activity` and `wait_for_bulk_activity` IR nodes
- batch-level workflow history events
- bridge protocol for throughput admission, fencing, idempotency, and callback translation
- stream-backed execution lane with dedicated streams-runtime service
- chunk-level retry and coarse cancellation
- batch/chunk visibility query endpoints
- dedicated bulk worker gRPC protocol
- benchmark harness comparing durable and throughput modes

Exit criteria:

- throughput mode outperforms durable mode by at least 5x on fan-out benchmarks
- the stream-backed lane handles batches with millions of items
- mixed durable and bulk steps work correctly in one workflow
- workflow code does not select or name the throughput backend

## Phase 6.75: Stream Subsystem Productization

Goal: turn the internal stream-backed lane into a clean standalone subsystem without destabilizing the workflow product story.

Deliverables:

- formal `Fabrik Workflows -> Bridge -> Streams` ownership split across docs and services
- authoritative bridge protocol for callbacks, closed-run handling, cancellation races, and checkpoint semantics
- current `stream-v2` lane treated as the first implementation of the internal stream subsystem
- operator surfaces that clearly distinguish workflow-authoritative outcomes from projected stream progress
- reserved SDK and IR design for future stream-job primitives without shipping them prematurely

Exit criteria:

- throughput internals can evolve independently of the workflow runtime
- docs no longer describe `stream-v2` as a workflow-internal product surface
- stream semantics can expand later without changing the `ctx.bulkActivity()` contract

Implementation sequencing for this phase is captured in [streams-transition-plan.md](streams-transition-plan.md).

## Phase 7: Hosted Platform Hardening

Goal: make the system safe for multi-tenant production use.

Deliverables:

- tenant quotas
- worker and API auth
- namespace isolation
- rate limiting and abuse controls
- retention and archival policies

Exit criteria:

- one hot tenant cannot take down shared control planes or matching capacity
- hosted and self-managed worker modes are both well-defined

## Cross-Cutting Workstreams

- replay tooling
- benchmark harnesses
- schema evolution
- SDK ergonomics
- operator experience
- documentation
- throughput lane evolution (v2.1 groups, v2.2 manifests, v2.3 warm failover)

## Recommended Immediate Next Steps

1. Define workflow task and activity task queue semantics.
2. Define the arbitrary activity worker protocol.
3. Expand the workflow IR to cover child workflows, updates, joins, and version markers.
4. Freeze the bridge protocol before broadening stream-backed semantics.
5. Execute the first bridge slice from [streams-transition-plan.md](streams-transition-plan.md).
