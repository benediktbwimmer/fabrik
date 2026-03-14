# Streams Transition Plan

## Purpose

This document turns the `Workflows -> Bridge -> Streams` architecture into an implementation sequence.

The goal is to get from today's `stream-v2` throughput lane to an internal stream subsystem that is cleanly separated from workflow semantics, without destabilizing the current `Fabrik Workflows` product story.

## Governing Rule

Do not extract `stream-v2` first.

First build and freeze the bridge boundary.

Then make the existing `stream-v2` lane the first implementation behind that boundary.

Only after that should naming, ownership, and productization work proceed.

If we reverse that order, we risk moving today's coupling into a new package or service layout without actually improving the architecture.

## Current Starting Point

Today the system already has most of the execution pieces:

- workflow-side admission in `unified-runtime`
- a dedicated `throughput-runtime`
- asynchronous projection in `throughput-projector`
- throughput-specific worker polling and reporting
- query and visibility paths that expose batch and chunk state

The remaining problem is not "missing throughput infrastructure." It is that the safety boundary is not yet clean enough:

- workflow-side logic still knows too much about throughput execution details
- callback, query, and cancellation semantics are not yet centered on a formal bridge contract
- `stream-v2` is still described and experienced partly as a workflow-internal backend instead of a stream execution subsystem

## Sequencing

The recommended implementation order is:

1. Freeze the bridge contract while the current `stream-v2` implementation stays where it is.
2. Make `stream-v2` conform to that bridge as the first stream implementation.
3. Shift code, naming, and ownership from "throughput backend" toward "stream subsystem."
4. Clean up operator and query surfaces so they reflect the semantic split.
5. Only then treat the subsystem as the crude first version of `Fabrik Streams`.

## Phase 1: Freeze the Bridge Contract

### Goal

Define the internal protocol boundary before moving service ownership around.

### Main Deliverables

- bridge request identity for every workflow bulk step
- idempotent `submit_bulk_run`
- idempotent `cancel_bulk_run`
- one fenced terminal callback acceptance path
- explicit strong versus eventual query semantics
- explicit handling for closed workflow runs, duplicate callbacks, and cancellation races

### Primary Code Areas

- [services/unified-runtime/src/main.rs](/Users/bene/code/fabrik/services/unified-runtime/src/main.rs)
- [services/query-service/src/main.rs](/Users/bene/code/fabrik/services/query-service/src/main.rs)
- [crates/fabrik-throughput/src/lib.rs](/Users/bene/code/fabrik/crates/fabrik-throughput/src/lib.rs)
- [crates/fabrik-store/src/lib.rs](/Users/bene/code/fabrik/crates/fabrik-store/src/lib.rs)
- [docs/spec/streams-bridge.md](/Users/bene/code/fabrik/docs/spec/streams-bridge.md)

### What Must Not Happen Yet

- no major service extraction
- no broad renaming from throughput to streams
- no new workflow SDK primitive
- no attempt to make `stream-v2` look externally like a standalone product

### Cut Line

`unified-runtime` should talk to a bridge-shaped internal interface, even if the implementation still calls directly into today's throughput path behind that interface.

### Verification

- replay preserves the same bulk-step identity across owner restart
- duplicate admission requests return the same run identity
- duplicate or stale terminal callbacks cannot advance workflow history twice
- strong and eventual query paths are explicitly labeled and tested

## Phase 2: Put `stream-v2` Behind the Bridge

### Goal

Keep the current throughput engine, but force all workflow-facing interaction to happen through the bridge boundary.

### Main Deliverables

- `StartBulkActivity` and `WaitForBulkActivity` remain unchanged at the workflow level
- workflow-side code stops depending on stream-implementation details
- terminal outcomes re-enter workflow history only through bridge acceptance
- query paths are routed either through bridge-owned strong reads or projection-owned eventual reads

### Primary Code Areas

- [services/unified-runtime/src/main.rs](/Users/bene/code/fabrik/services/unified-runtime/src/main.rs)
- [services/throughput-runtime/src/main.rs](/Users/bene/code/fabrik/services/throughput-runtime/src/main.rs)
- [services/throughput-projector/src/main.rs](/Users/bene/code/fabrik/services/throughput-projector/src/main.rs)
- [services/activity-worker-service/src/main.rs](/Users/bene/code/fabrik/services/activity-worker-service/src/main.rs)
- [services/query-service/src/main.rs](/Users/bene/code/fabrik/services/query-service/src/main.rs)

### Cut Line

No workflow-facing component should need to know `stream-v2`-specific execution details beyond the bridge contract.

### Verification

- mixed durable plus throughput workflows still replay unchanged
- worker polling and completion still function under the bridge path
- exactly one workflow barrier event is appended for one stream terminal outcome
- failover tests pass with the bridge path enabled

## Phase 3: Move Ownership and Naming Toward the Stream Subsystem

### Goal

Make the codebase reflect the architecture more honestly without forcing a dangerous flag day.

### Main Deliverables

- stream-subsystem terminology in shared types and config where semantics already permit it
- compatibility aliases for existing throughput names
- clearer distinction between workflow barrier ownership and stream execution ownership
- service docs and local-stack docs updated around the stream subsystem framing

### Primary Code Areas

- [crates/fabrik-config/src/lib.rs](/Users/bene/code/fabrik/crates/fabrik-config/src/lib.rs)
- [crates/fabrik-throughput/src/lib.rs](/Users/bene/code/fabrik/crates/fabrik-throughput/src/lib.rs)
- [services/throughput-runtime/src/main.rs](/Users/bene/code/fabrik/services/throughput-runtime/src/main.rs)
- [services/throughput-projector/src/main.rs](/Users/bene/code/fabrik/services/throughput-projector/src/main.rs)
- [scripts/dev-stack.sh](/Users/bene/code/fabrik/scripts/dev-stack.sh)

### Naming Guidance

Do this gradually.

Keep stable external compatibility where needed:

- old config keys may continue to work
- old service names may remain for a while
- `stream-v2` may remain as the implementation identifier

But new shared abstractions should prefer "bridge," "stream run," and "stream execution lane" where the semantics are no longer throughput-specific.

### Cut Line

The code should read as "workflow runtime plus stream subsystem" rather than "workflow runtime plus weird throughput backend."

### Verification

- config compatibility tests still pass
- local stack scripts still boot without manual intervention
- benchmark harnesses still target the same implementation successfully

## Phase 4: Clean Up Query, Visibility, and Operator Semantics

### Goal

Make the semantic split visible to operators and API clients.

### Main Deliverables

- authoritative workflow outcomes labeled separately from projected stream progress
- strong owner-routed versus eventual projection reads clearly separated in APIs and UI
- batch detail surfaces that no longer blur stream-local progress with workflow truth
- runbooks and operator docs updated to match the bridge model

### Primary Code Areas

- [services/query-service/src/main.rs](/Users/bene/code/fabrik/services/query-service/src/main.rs)
- [apps/fabrik-console](/Users/bene/code/fabrik/apps/fabrik-console)
- [docs/operator-runbook-alpha.md](/Users/bene/code/fabrik/docs/operator-runbook-alpha.md)
- [docs/streaming-product-guide.md](/Users/bene/code/fabrik/docs/streaming-product-guide.md)

### Cut Line

Operators should be able to answer:

- what is workflow-authoritative
- what is stream-owned
- which reads are strong
- which reads are eventual

without reading source code or guessing from labels.

### Verification

- API responses expose consistency and authority clearly
- UI labels and drill-down paths match the same distinction
- recovery drills are explainable using the bridge model alone

## Phase 5: Treat the Subsystem as the First Version of `Fabrik Streams`

### Goal

Once the bridge and subsystem boundary are real, start treating the internal stream lane as the seed of a standalone stream product.

### Main Deliverables

- stream subsystem evolves independently of workflow runtime internals
- dedicated stream-job semantics can be designed without changing `ctx.bulkActivity()`
- product docs can begin to describe a future `Fabrik Streams` surface without pretending it already ships as one

### Primary Code Areas

- [docs/adr/0006-workflows-bridge-streams-split.md](/Users/bene/code/fabrik/docs/adr/0006-workflows-bridge-streams-split.md)
- [docs/roadmap.md](/Users/bene/code/fabrik/docs/roadmap.md)
- [docs/spec/workflow-ir.md](/Users/bene/code/fabrik/docs/spec/workflow-ir.md)
- [docs/sdk-compiler.md](/Users/bene/code/fabrik/docs/sdk-compiler.md)

### Cut Line

At this point it becomes honest to say:

- `Fabrik Workflows` uses a bridge-backed stream execution lane for throughput work
- that lane is independently evolvable
- future stream-job APIs can be added without weakening the current throughput contract

### Verification

- bridge semantics remain stable while stream implementation evolves
- `bulkActivity()` stays backend-agnostic
- new stream-facing design work no longer requires changes to workflow barrier semantics

## Risks

### Risk 1: Extracting too early

If we move services or rename everything before the bridge contract is real, we preserve today's coupling under a different label.

### Risk 2: Mixing semantic layers

If strong and eventual reads are not separated clearly, users will mistake stream progress for workflow truth.

### Risk 3: Breaking in-flight runs

Old and new execution models may need to coexist during migration. In-flight runs must finish on the semantics they started with.

### Risk 4: UI lagging behind runtime truth

A clean backend split with a muddy UI still feels like one confusing system.

## Success Criteria

The transition is successful when all of the following are true:

- workflow code still sees `ctx.bulkActivity()` as one backend-agnostic workflow primitive
- workflow history remains the only workflow-authoritative source of truth
- the bridge is the only legal workflow-to-stream crossing point
- `stream-v2` can evolve as the first stream implementation without changing workflow semantics
- operator surfaces clearly distinguish authoritative outcomes from projected stream progress
- the codebase is ready for later stream-job primitives without overloading throughput mode

## Immediate Next Engineering Slice

The first concrete slice should be:

1. add bridge request identity and terminal-callback dedupe fields to shared throughput types
2. centralize admission and terminal-acceptance logic behind a bridge-shaped interface in `unified-runtime`
3. make query consistency explicit in `query-service`
4. add focused tests for duplicate admission, stale callback rejection, and closed-workflow callback handling

That is the smallest slice that improves the architecture without forcing premature extraction.
