# Streams Bridge

## Purpose

This document defines the protocol boundary between workflow-authoritative execution and the stream-backed execution lane used for throughput-heavy workloads.

## Scope

The bridge is currently required for throughput mode driven by `ctx.bulkActivity()`.

Future stream-job workflow primitives may reuse the same bridge, but they are outside the current shipping workflow contract unless stated explicitly elsewhere.

## Core Rule

The bridge is the only legal crossing point between workflow truth and stream truth.

That means:

- workflows advance only through workflow-authoritative history events
- the stream-backed lane owns nonterminal execution, checkpoints, and progress
- stream-side observations affect workflows only when the bridge accepts them under the fencing and idempotency rules below

## Current Entities

- `workflow run`: the authoritative workflow execution identified by tenant, instance, and run id
- `bridge request`: the stable workflow-side admission identity for one bulk step
- `stream run`: the execution-lane object created or recovered by the bridge
- `workflow owner epoch`: the active workflow ownership generation
- `stream owner epoch`: the active stream execution ownership generation
- `terminal callback`: a stream-side message that attempts to satisfy a workflow barrier

## Stable Identifiers

Every bridge interaction must carry enough identity to make retries and failover safe.

Minimum identity set:

- `tenant_id`
- `namespace_id` when namespaces are distinct from tenants
- `workflow_instance_id`
- `workflow_run_id`
- `bridge_request_id`
- `workflow_owner_epoch`
- `stream_run_id` after admission succeeds
- `stream_owner_epoch` on callbacks
- `artifact_hash` or equivalent workflow definition identity
- `idempotency_key` for client-visible admission retries

## Admission

### `submit_bulk_run`

The workflow side calls `submit_bulk_run` when a `StartBulkActivity` node is reached.

Required properties:

- idempotent for the same `(tenant_id, workflow_run_id, bridge_request_id)`
- returns the previously assigned `stream_run_id` if the admission already succeeded
- never creates multiple stream runs for one workflow bulk step
- records enough resume metadata for replay and owner handoff

### Admission Failure Semantics

If admission fails before the bridge commits success, the workflow step remains unsatisfied and may retry safely.

If admission succeeds but the workflow owner crashes before observing the response, replay must recover the same `stream_run_id` and continue waiting on the same barrier.

## Terminal Callbacks

### Allowed Outcomes

The bridge accepts only one terminal callback shape per stream run:

- completed
- failed
- cancelled

Each accepted terminal callback may append at most one workflow-authoritative barrier event.

### Callback Dedupe

Duplicate terminal callbacks must be safe and side-effect free.

Rules:

- if the same terminal callback is replayed with the same identity, the bridge returns the prior acceptance result
- if a stale callback arrives after a newer owner epoch is active, it is rejected as stale
- if a second distinct terminal outcome arrives after one has already been accepted, it is ignored for workflow mutation and retained only for audit if desired

## Closed Workflow Behavior

If the workflow run is already closed when a terminal callback arrives:

- the callback must not reopen or mutate the workflow
- the bridge records the callback as ignored or orphaned according to the implementation's audit model
- stream-side cleanup may still proceed, but no new workflow event is appended

## Cancellation Races

Cancellation must be race-safe across workflow and stream ownership changes.

Rules:

- `cancel_bulk_run` is idempotent for the same bridge request
- after cancellation is accepted, no newly leased work may become workflow-authoritative
- late worker reports may still arrive, but they must be fenced by lease and owner epochs
- exactly one workflow-visible terminal outcome wins: completed, failed, or cancelled

If completion wins before cancellation is accepted, the workflow observes completion.

If cancellation wins first, later completion reports remain non-authoritative.

## Query Consistency

The bridge may expose query paths for stream-backed execution, but consistency must be explicit.

### Strong

Strong reads:

- route to the active stream owner
- may observe nonterminal owner state
- are not replayed into workflow state transitions
- are operationally useful but not workflow-authoritative unless a separate workflow primitive explicitly awaits a named milestone

### Eventual

Eventual reads:

- may use projections
- may lag owner state
- are suitable for UI and cheap status inspection
- are never workflow-authoritative

## Checkpoint Boundary

The current throughput-mode contract does not expose stream checkpoints directly to workflow code.

If future stream-job primitives expose awaited checkpoints, each checkpoint must be:

- named
- durable
- monotonic for a given stream run
- accepted by the bridge at most once for workflow advancement
- clearly distinguished from local operator progress markers

## Non-Goals

The bridge is not:

- a second source of workflow truth
- a way to leak stream implementation names into workflow code
- a shortcut around workflow replay and versioning rules
- a license to let stream projections drive workflow branches directly

## Consequence

Throughput mode can evolve onto richer stream infrastructure without changing the current workflow-facing contract, as long as the bridge semantics remain stable.
