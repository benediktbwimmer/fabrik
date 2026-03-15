# Visibility and Query Model

## Purpose

This document freezes the external read and visibility contract.

## Current State View

The current-state view for a workflow run should expose at least:

- `tenant_id`
- `definition_id`
- `definition_version`
- `artifact_hash`
- `instance_id`
- `run_id`
- current workflow status
- current workflow location or summary state
- latest accepted event id
- latest accepted event type
- task queue metadata
- last updated time

## Strong Query View

The strong query API should expose:

- query name
- query result
- query consistency level
- whether the result came from hot owner state, replay, or another consistency mechanism

Strong query execution is a workflow API feature, not merely a projection lookup.

For throughput mode, strong reads must distinguish owner domains explicitly:

- workflow strong reads route to the workflow owner
- live batch strong reads route to the throughput owner

## Visibility View

The visibility API should support:

- list workflows by status
- filter by workflow type
- filter by task queue
- search by search attributes
- memo-like metadata retrieval
- pagination for large result sets
- run lineage inspection across continue-as-new chains

For stream jobs, that visibility surface should not depend on workflow origin.

The same list/detail/runtime/view endpoints should work for:

- workflow-originated stream jobs
- standalone stream jobs
- deployment-backed standalone revisions

## History View

The history view should expose:

- ordered events
- causation and correlation links
- pagination cursor
- event retention expectations
- workflow task and activity task timelines
- child workflow timelines
- update acceptance and completion records

## Retention Contract

- durable history retention is controlled by the authoritative history substrate
- visibility retention applies only to derived indexes and read models
- pruning derived state must never change workflow correctness

## Lag Contract

- visibility indexes may be eventually consistent
- strong query and update paths must state their own consistency rules explicitly
- operational tooling must surface projection lag and indexing lag

## Throughput Read Concerns

Throughput-mode reads must expose consistency as a read concern rather than as an execution option.

Supported read concerns:

- `strong`
- `eventual`

Rules:

- `strong` reads route to the active owner for the requested domain
- `eventual` reads may come from projections or other derived read models
- eventual reads are non-authoritative and not replay-stable
- operator and UI surfaces should expose projection lag metadata when serving eventual reads

When an eventual stream read is served from the projection store, the response should expose enough metadata to explain how fresh it is.

At minimum that means:

- latest projected checkpoint sequence
- latest projected update time when known
- latest projected delete checkpoint or delete time when tombstone state exists
- owner checkpoint sequence when known
- lag between the owner checkpoint and the projected checkpoint

The eventual query surface should also support explicit projection summaries such as projected key count when available.

For debug or operator-facing browse surfaces, strong scans over owner state, keyed entry listings, and key-only listings may also carry the corresponding projection summary and lag metadata for the same view so one response can show both the authoritative browse result and the current eventual-read health.

## Stream Diagnostics Queries

Stateful stream runtimes should also expose a strong diagnostics query for operators.

That query should return not only live counters and cursor state, but also the effective runtime policy for the job, including:

- window mode and size
- event-time field
- allowed lateness
- per-view retention or eviction policy
- explicit late-event handling semantics

The diagnostics surface should also support narrowing to one materialized view so operators can inspect view-local policy and owner-local counts without reading the entire job payload.

When the runtime has event-time or checkpoint metadata available, that per-view diagnostics surface should also expose freshness and lag markers for the view.

When that same view supports eventual reads, the per-view diagnostics surface should also expose projection-side freshness for that view, including projected checkpoint/update markers, projected delete markers when tombstones exist, projected key count when available, and lag relative to the owner checkpoint.

When retention eviction is enabled, that per-view diagnostics surface should also expose the view's historical eviction counters and latest eviction markers so operators can distinguish current retained state from already-evicted window history.

## Projection Rebuild

Eventual stream projections must support explicit rebuild without changing stream correctness.

The rebuild contract is:

- authoritative owner state remains the source of truth
- retained owner rows may be re-projected after projection loss
- stale projected rows must be deleted during rebuild
- projection deletes must remain monotonic so an older upsert cannot resurrect a newer delete

Projection rebuild is therefore an operator and recovery primitive, not an execution primitive.

Where practical, operators should be able to rebuild one materialized view at a time rather than forcing a whole-job rebuild for a single damaged or lagging projection.

The non-debug stream query surface should also support a compact per-view projection summary query so workflows and operators can ask for projected key count, latest projected checkpoint or delete markers, and owner-relative lag without relying on debug HTTP.

That same surface should support lightweight filtering for job-level projection summary queries, such as:

- only stale views
- views at or above a checkpoint-lag threshold
- explicit subsets of view names

The strong query surface should also support a compact bridge-state query for one stream job.

That bridge-state query should expose:

- workflow and bridge identity for the handle
- current bridge lifecycle state and stored stream lifecycle state
- control availability such as pause, resume, and cancel
- awaited checkpoint rows with repair and stale-owner classification
- recent query rows with repair and stale-owner classification
- recent `signal_workflow` rows with callback dedupe identity, target workflow identity, and queued/dispatching/consumed bridge status
