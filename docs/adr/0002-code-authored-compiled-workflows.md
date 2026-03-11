# ADR 0002: Author Workflows in Code, Execute Compiled Workflow Artifacts, Run Activities in Workers

## Status

Accepted

## Date

2026-03-11

## Context

`fabrik` needs two things at once:

- Temporal-like workflow and activity ergonomics
- lower workflow-decision overhead than systems that directly interpret general-purpose workflow code at runtime

Running arbitrary workflow guest code directly as the execution substrate would improve implementation familiarity, but it would push replay cost, versioning risk, and determinism complexity into the workflow hot path.

Eliminating arbitrary activity code, on the other hand, would destroy Temporal parity and weaken the product.

## Decision

We will separate the workflow runtime from the activity runtime:

- workflows will be authored in SDK code
- workflow SDK code will compile to deterministic workflow artifacts
- executors will run compiled workflow artifacts on the steady-state workflow path
- activities will remain arbitrary user code executed by workers through activity task queues
- each running workflow will be pinned to a workflow artifact version recorded in history

## Rationale

This gives `fabrik` the required combination of properties:

- code-first ergonomics for workflows
- arbitrary user activities
- explicit workflow execution artifacts for inspection and replay
- lower workflow-task overhead than direct workflow guest-code execution
- clean version pinning for long-running workflows

## Consequences

### Positive

- workflow latency can improve without weakening the activity model
- replay safety is enforced at the workflow artifact boundary
- activity ecosystems remain broad and practical
- the product can target Temporal feature parity without Temporal's exact runtime structure

### Negative

- the platform must build and maintain both workflow compilers and worker protocols
- source-level debugging must map cleanly onto compiled workflow artifacts
- compatibility rules must cover both workflow artifacts and worker builds

## Required Follow-Up

- define workflow task and activity task semantics
- define the workflow IR for activities, child workflows, updates, joins, and version markers
- add worker versioning and compatibility routing
- add replay tooling that validates histories against workflow artifacts in CI
