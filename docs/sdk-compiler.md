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

- `start_bulk_activity` — creates batch and chunk manifest
- `wait_for_bulk_activity` — blocks until batch terminal event

Bulk activity options (`taskQueue`, `chunkSize`, `backend`, `retry`) must be static literals in the compiled artifact. The `items` expression is evaluated at runtime.

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
