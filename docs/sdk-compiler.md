# SDK + Compiler Direction

## Goal

`fabrik` should feel like Temporal to the application developer while executing workflow logic more efficiently internally.

That requires two different runtime boundaries:

- workflows are authored in SDK code and compiled into deterministic workflow artifacts
- activities are authored as arbitrary user code and executed by workers through task queues

## Why This Split Exists

Workflow execution and activity execution optimize for different things.

Workflow execution wants:

- determinism
- replay safety
- low-latency decisions
- explicit state transitions

Activity execution wants:

- arbitrary code
- normal libraries and network access
- full language runtime support
- user-controlled process deployment

`fabrik` should not force users to choose one model for both.

## Workflow SDK Shape

The workflow SDK should feel like normal durable orchestration code:

```ts
export async function orderWorkflow(ctx, input) {
  const order = await ctx.waitForSignal("order_placed");
  const reservation = await ctx.activity("reserveInventory", {
    orderId: order.id,
  });

  const child = await ctx.startChild("chargeWorkflow", {
    reservationId: reservation.id,
  });

  await child.result();
  return ctx.complete({ ok: true });
}
```

Required workflow primitives include:

- `ctx.sleep(duration)`
- `ctx.waitForSignal(name)`
- `ctx.setSignalHandler(name, handler)`
- `ctx.query(name, handler)`
- `ctx.update(name, handler)`
- `ctx.activity(name, input, options?)`
- `ctx.bulkActivity(name, items, options?)` — throughput mode fan-out
- `ctx.startChild(name, input, options?)`
- `ctx.sideEffect(fn)`
- `ctx.version(changeId, min, max)`
- `ctx.continueAsNew(input?)`
- `ctx.complete(output?)`
- `ctx.fail(reason)`

Future stream-native primitives such as `ctx.startStreamJob(...)` are intentionally not part of the current workflow replacement claim. They should ship only after the bridge contract and stream semantics are explicit enough to justify a distinct workflow primitive.

The target semantic contract for those primitives is defined in [spec/stream-jobs.md](spec/stream-jobs.md).

## Temporal Compatibility Progress

The long-term goal is source compatibility with Temporal workflow code, starting with the TypeScript SDK surface.

Current implemented compatibility slice in the compiler:

- top-level `proxyActivities(...)` declarations imported from `@temporalio/workflow`
- awaited calls to destructured proxy activity functions
- awaited calls to proxy activity object members
- multi-argument Temporal activity calls, lowered as array payloads when more than one argument is passed
- awaited reassignment statements like `value = await activity(...)`
- static `proxyActivities({...})` options for:
  - `taskQueue`
  - `scheduleToStartTimeout`
  - `startToCloseTimeout`
  - `heartbeatTimeout`
  - `retry.maximumAttempts`
  - `retry.initialInterval`
- awaited `sleep("...")` imported from `@temporalio/workflow`
- awaited `Promise.all(items.map((item) => activity(item)))` over proxy activities
- awaited `Promise.allSettled(items.map((item) => activity(item)))` over proxy activities
- array `.find(...)` and `.map(...)` transforms over compiler-known workflow arrays such as settled fan-out results
- top-level `defineQuery(...)` / `defineUpdate(...)` plus `setHandler(...)` registrations
- compiled Temporal signal handlers via `defineSignal(...)` plus `setHandler(...)`
  - handler bodies lower into dedicated signal subgraphs in the artifact
  - async signal handlers can await the same supported workflow primitives as other compiled handler bodies
- a broader Temporal `condition(...)` subset:
  - awaited `condition(() => predicate)` lowers into a first-class `wait_for_condition` state
  - awaited `condition(() => predicate, timeout)` lowers into a timed `wait_for_condition` state and resolves to `false` on timeout for supported compiled predicates
  - the condition is re-evaluated after compiled signal handlers or update handlers run and mutate workflow state
- a narrow Temporal child-workflow subset:
  - `await startChild(workflow, { args, workflowId, taskQueue, parentClosePolicy })`
  - `await childHandle.result()`
  - `await childHandle.signal(signalDefOrName, payload?)`
  - `await childHandle.cancel(reason?)`
  - `await executeChild(workflow, { args, workflowId, taskQueue, parentClosePolicy })`
- a narrow Temporal external-workflow-handle subset:
  - `const handle = getExternalWorkflowHandle(workflowId, runId?)`
  - `await handle.signal(signalDefOrName, payload?)`
  - `await handle.cancel(reason?)`
  - current runtime support targets the current instance row for `workflowId` and only honors `runId` when it matches that current run
- a narrow Temporal cancellation-scope subset:
  - `await CancellationScope.cancellable(async () => { ... })`
  - `await CancellationScope.nonCancellable(async () => { ... })`
  - `isCancellation(error)` inside compiled workflow expressions and `catch` branches
  - currently supported scope bodies are straight-line compiled statements that lower to the existing artifact model, including multiple awaited proxy activities or child-workflow waits plus local assignment/return flow
  - supported `nonCancellable(...)` scopes now establish a real runtime cancellation shield for the compiled states inside that scope
  - cancellation requests are deferred while execution remains inside those marked states and are delivered once the workflow exits the shielded region
  - this is still a subset, not full Temporal cancellation-scope parity; unsupported dynamic scope shapes are still rejected by the compiler
- plain `return value` as workflow completion
- `return continueAsNew(...)` imported from `@temporalio/workflow`, including multi-argument payload packing

That means a narrow class of Temporal-style workflows can now compile without being rewritten to the Fabrik `ctx.*` shape first.

Current non-goals for this slice:

- general `Promise.all(...)` lowering beyond the proxy-activity map pattern
- general `Promise.allSettled(...)` lowering beyond the proxy-activity map pattern
- broad Temporal `condition(...)` parity beyond supported compiled predicates, handler-driven state changes, and the static timeout form
- broad Temporal child workflow handles, external workflow handles, and signal APIs beyond the narrow supported start/result/signal/cancel and execute forms
- full proxy activity option parity
  - dynamic or computed retry option values are still rejected; retry options must remain static literals
- wire-level or server-level Temporal compatibility

The intended development pattern is incremental:

1. support a Temporal TS source subset directly in the compiler
2. validate it with Temporal-style fixtures and benchmarks
3. expand the subset until common Temporal workflows compile unchanged

The trust milestone now has manifest-driven conformance entry points:

- Layer A support fixtures: `sdk/typescript-compiler/conformance/layer-a-support-fixtures.json`
- Layer B semantic fixtures: `sdk/typescript-compiler/conformance/layer-b-semantic-fixtures.json`
- Runner: `node sdk/typescript-compiler/conformance-runner.mjs --layer <layer_id>`
- Test entry point: `npm run test:conformance`

The runner can also write machine-readable reports under `target/conformance-reports/`.

## Activity SDK Shape

Activities are not compiled into workflow IR.

They should look like ordinary user code:

```ts
export async function reserveInventory(input) {
  const response = await inventoryClient.reserve(input.orderId);
  return { reservationId: response.id };
}
```

Activity workers should register activity types, poll queues, and report:

- started
- heartbeat
- completed
- failed
- cancelled

## Compiler Responsibilities

The compiler target is a deterministic artifact that can be inspected and hashed.

Likely artifact sections:

- workflow metadata
- workflow state graph or bytecode
- signal, query, and update handler metadata
- activity callsites and retry metadata
- child workflow callsites
- side-effect and version markers
- source map
- compiler version
- artifact hash

For throughput mode, the compiler additionally lowers `ctx.bulkActivity()` into:

- `start_bulk_activity` — records a workflow-side bulk admission that is later resolved through the bridge
- `wait_for_bulk_activity` — blocks until the bridge delivers one terminal batch outcome

Bulk activity options (`taskQueue`, `chunkSize`, `execution`, `reducer`, `retry`) must be static literals in the compiled artifact. The `items` expression is evaluated at runtime.

Backend selection is server-controlled and pinned per batch. Workflow code does not select or name the backend. The current high-throughput execution lane is the internal stream subsystem implemented by `stream-v2`.

If dedicated stream-job primitives are added later, they should lower into separate IR nodes rather than overloading `start_bulk_activity` with long-lived stream semantics.

The compiler must reject workflow code that breaks determinism, while activity code remains unconstrained.

## Runtime Contract

Workflow executors should operate on the compiled artifact with these rules:

- active executions stay warm on shard owners whenever possible
- replay from snapshot plus event tail is the fallback path
- every run is pinned to an artifact version
- workflow tasks are short-lived decision turns
- activity tasks are scheduled durably and completed asynchronously
- signals, queries, and updates follow documented handler rules

## Versioning Rules

Temporal parity requires both workflow and worker versioning.

Required rules:

- a running workflow never silently switches artifacts
- artifact pinning is written into history
- activity tasks route according to worker build compatibility
- replay tooling validates representative histories before deployment
- version markers allow safe workflow code evolution

## Testing Contract

The SDK story must include:

- replay tests against captured histories
- deterministic workflow unit tests
- activity integration tests
- worker compatibility tests
- upgrade and rollback tests

## Non-Goal

The compiler is not a mechanism for removing arbitrary activities from the platform.

Its job is to make workflow execution fast and deterministic while preserving the general-purpose activity model users expect from a Temporal replacement.
