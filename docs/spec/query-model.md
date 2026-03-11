# Query Model

## Purpose

This document freezes the external read-model contract.

## Current State View

The current-state query for a workflow run should expose at least:

- `tenant_id`
- `definition_id`
- `definition_version`
- `artifact_hash`
- `instance_id`
- `run_id`
- current node / state
- current status
- latest accepted event id
- latest accepted event type
- last updated time

## History View

The history query should expose:

- ordered events
- causation and correlation links
- pagination cursor
- event retention expectations

## Lag Contract

- query models are eventually consistent
- operational tooling must surface projection lag
- query consumers may not assume read-after-write consistency
