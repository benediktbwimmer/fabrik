# ADR 0002: Author Workflows in Code, Execute Compiled Artifacts

## Status

Accepted

## Date

2026-03-11

## Context

`fabrik` needs two things that are often in tension:

- workflow authoring must feel like normal application code
- execution must stay deterministic, replayable, and fast under very large workloads

Exposing raw internal state machines as the primary developer surface would make the runtime easier to reason about, but it would produce a weak authoring experience.

Running arbitrary guest workflow code directly as the execution substrate would improve ergonomics, but it would push replay cost, versioning risk, and determinism complexity into the hot path.

## Decision

We will separate authoring from execution:

- workflows will be authored in code through an SDK
- SDK code will compile to deterministic workflow IR / state machine artifacts
- executors will run the compiled artifact, not arbitrary author code as the steady-state runtime path
- each running instance will be pinned to a `definition_version` and `artifact_hash` recorded in history

## Rationale

This gives `fabrik` the right combination of properties:

- code-first ergonomics for developers
- explicit execution artifacts for inspection and replay
- lower steady-state replay overhead than direct guest-code re-execution
- clearer deployment and version-pinning rules for long-running workflows
- simpler operational debugging because runtime state is representable as explicit transitions

## Consequences

### Positive

- the public programming model can feel similar to durable workflow systems without inheriting their full replay cost profile
- replay safety can be enforced at the artifact boundary
- long-running instances can be pinned cleanly across deployments
- internal execution remains compatible with shard-local hot-state caching

### Negative

- we must build and maintain an SDK and compiler
- source-level debugging must map cleanly onto compiled artifacts
- features that seem simple in author code may require careful IR design

## Required Follow-Up

- define the first SDK surface and deterministic runtime APIs
- define the workflow IR schema and artifact hashing strategy
- add marker events for side effects, versioning, and runtime metadata
- add replay tooling that can validate histories against artifacts in CI
- define signal mailbox ordering and interleaving rules
- add `ContinueAsNew`-style history rollover
