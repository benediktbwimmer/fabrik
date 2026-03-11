# Replay and Testing

## Purpose

This document freezes replay and validation as core platform infrastructure.

## Replay Requirements

The platform must support:

- replaying a captured workflow history against a chosen workflow artifact
- replaying from snapshot plus event tail
- determinism divergence detection
- event-by-event replay diagnostics
- upgrade compatibility validation across workflow versions

Activity code is not replayed as workflow code. Replay validates the workflow boundary and the recorded activity outcomes.

## Required Test Classes

- deterministic workflow executor tests
- golden history replay tests
- workflow artifact compatibility tests
- worker version routing tests
- crash and restart recovery tests
- timer recovery tests
- duplicate-delivery tests
- snapshot restore tests
- large fan-out / fan-in stress tests

## CI Rule

Replay validation for representative histories is a release-gating capability, not optional QA.

## SDK Testing Rule

Every supported SDK should provide:

- deterministic workflow tests
- captured-history replay tests
- activity mocking
- child workflow testing
- upgrade regression testing
