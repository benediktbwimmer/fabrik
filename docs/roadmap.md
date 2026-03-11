# Roadmap

## Phase 0: Project Foundations

Goal: establish a repo and technical baseline that supports fast iteration without locking in bad architecture.

Deliverables:

- product model and non-goals
- Rust workspace layout
- local development stack
- lint, format, and test tooling
- event schema conventions
- ADR process
- initial workflow IR direction
- semantic spec pack under `docs/spec/`

Exit criteria:

- a new contributor can run the workspace locally
- shared crates compile
- core architectural choices are documented

## Phase 1: Minimal End-to-End Vertical Slice

Goal: prove the log-first execution model with one simple workflow type.

Deliverables:

- ingest API for trigger events
- Redpanda-backed event publication
- single executor consuming one workflow topic
- workflow instance state machine runtime
- snapshot persistence in PostgreSQL
- hot-state cache for active executions
- timer scheduling and firing
- query API for workflow state and history
- replay harness for captured histories

Demo scenario:

- receive an event
- start a workflow
- wait on a timer
- invoke a Wasm predicate or transform
- complete the workflow

Exit criteria:

- workflow state recovers correctly after restart
- replay from log produces the same final state
- timer events survive process restarts
- replay validation can run in CI against real histories

## Phase 2: Workflow SDK and Compiler

Goal: make workflows feel like code while keeping the runtime deterministic and explicit.

Deliverables:

- first workflow SDK
- deterministic runtime API surface such as `sleep`, `signal`, `now`, `uuid`, and side-effect markers
- compiler from SDK code to workflow IR / state machine artifacts
- artifact hashing and pinning model
- artifact inspection tooling

Exit criteria:

- developers can author workflows in code instead of raw schema
- compiled artifacts are deterministic and inspectable
- running instances are pinned to exact artifact versions

## Phase 3: Side Effects and Connector Discipline

Goal: make external effects safe enough for real automation.

Deliverables:

- connector worker framework
- effect request and result events
- idempotency key model
- retry and dead-letter handling
- one reference connector, likely HTTP
- structured connector result payloads

Exit criteria:

- repeated delivery does not duplicate confirmed external side effects
- connector failures are visible in workflow history
- retry policy is configurable and durable

## Phase 4: Sandboxed Extensibility

Goal: allow tenant-defined logic without surrendering safety.

Deliverables:

- Wasmtime integration
- versioned Wasm artifact registry
- host capability model for safe bindings
- tenant quotas for CPU time and memory
- example custom transform and step module

Exit criteria:

- untrusted Wasm modules execute under resource limits
- module versions are pinned in workflow history
- replay uses the correct module version semantics

## Phase 5: Runtime Semantics and History Control

Goal: harden long-running workflow correctness before scale multiplies the cost of mistakes.

Deliverables:

- mailbox and signal interleaving rules
- marker event family for side effects and versioning
- `ContinueAsNew` / history rollover support
- safe deployment rules for artifact upgrades
- replay-diff diagnostics

Exit criteria:

- long-lived workflows can roll history without losing logical identity
- signal behavior is deterministic and documented
- artifact upgrades are replay-safe

## Phase 6: Multi-Tenant Hardening

Goal: make the platform safe and fair under mixed workloads.

Deliverables:

- tenant-aware auth and quotas
- workload isolation controls
- partition hot-spot detection
- backpressure and throttling policies
- operational dashboards

Exit criteria:

- one hot tenant cannot collapse shared execution capacity
- per-tenant limits are enforced and observable
- partition lag can be attributed to tenant or workflow class

## Phase 7: Scale and Reliability

Goal: validate the architecture under production-shaped load.

Deliverables:

- executor shard rebalancing
- snapshot compaction policy
- replay acceleration
- sticky hot-state eviction strategy
- chaos and failure testing
- throughput and latency benchmarks

Exit criteria:

- shard movement does not corrupt workflow state
- recovery objectives are measured and documented
- high-volume ingestion does not break timer correctness
- replay remains the fallback path rather than the dominant steady-state cost

## Cross-Cutting Workstreams

These should progress continuously:

- schema evolution strategy
- replay tooling
- observability
- security review
- benchmark harnesses
- local developer ergonomics
- documentation

## Recommended Immediate Next Steps

1. Define the first code-authored workflow SDK surface.
2. Define workflow IR and artifact pinning metadata.
3. Build replay tooling that can validate histories against artifacts.
4. Implement `ContinueAsNew` and marker events before histories grow large in production.
