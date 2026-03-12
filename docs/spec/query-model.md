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
