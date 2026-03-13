# Ownership and Fencing Contract

This document freezes the ownership and fencing contract for the Temporal TS subset trust milestone.

## Purpose

For the supported subset, Fabrik must make owner transition and duplicate-delivery behavior explicit instead of leaving it as an emergent property of guard code.

## Owner Transition Rules

The runtime must define:

- how ownership is acquired
- how ownership loss is detected
- which persisted epoch or lease values fence prior owners
- how a replacement owner chooses its replay point

Hard rule:

- a replacement owner may only resume from authoritative persisted state, never from another owner's unverified in-memory state

## Fenced Paths

The following paths must be fenced explicitly:

- trigger acceptance
- schedule and reschedule
- task start or lease acquisition
- completion apply
- retry scheduling
- cancellation apply
- snapshot or checkpoint publication when it participates in recovery

## Duplicate And Stale Delivery Rules

The runtime contract must define behavior for:

- duplicate trigger
- duplicate completion
- stale completion from a superseded attempt
- late completion after terminal workflow state
- stale worker from a prior owner epoch
- mixed-build completion during rolling restart

Accepted outcomes are limited to:

- apply once
- ignore as duplicate
- reject as stale

No path should remain semantically undefined.

## Upgrade And Pinned Artifact Rules

The ownership contract must preserve:

- replay against the pinned artifact for in-flight runs
- deterministic rejection of incompatible candidate artifacts
- coexistence of old and new worker builds during rollout
- routing visibility for compatibility-set changes

## Validation Requirement

The trust milestone requires automated coverage for:

- owner loss and takeover
- stale owner plus new owner interaction
- pinned run plus new deployment coexistence
- duplicate and stale delivery across restart and rolling restart windows
