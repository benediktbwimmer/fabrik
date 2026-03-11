# SDK + Compiler Direction

## Goal

`fabrik` should let developers think in code while the runtime thinks in explicit deterministic artifacts.

The intended model is:

- author workflows in SDK code
- compile authored workflows to workflow IR / state machine artifacts
- execute those artifacts on shard-local executors
- treat event history as the source of truth

## Why This Split Exists

Authoring and execution optimize for different things.

Authoring wants:

- normal control flow
- readable branching
- local composition
- strong type information

Execution wants:

- explicit states and transitions
- deterministic replay
- low overhead on hot paths
- precise artifact pinning

`fabrik` should not force developers to author raw state graphs, and it should not execute arbitrary guest code as the ground-truth runtime model.

## First SDK Shape

The first SDK should feel like durable orchestration code with constrained primitives:

```ts
export async function orderWorkflow(ctx, input) {
  const order = await ctx.waitForSignal("order_placed");
  await ctx.sleep("15m");

  const ok = await ctx.predicate("inventory_check", order);
  if (!ok) return ctx.fail("out_of_stock");

  await ctx.activity("reserve_inventory", order);
  return ctx.complete();
}
```

The important constraint is that `ctx` owns all non-deterministic behavior.

Initial required primitives:

- `ctx.waitForSignal(name)`
- `ctx.sleep(duration)`
- `ctx.activity(name, input)`
- `ctx.predicate(name, input)`
- `ctx.now()`
- `ctx.uuid()`
- `ctx.sideEffect(name, input)`
- `ctx.complete(output?)`
- `ctx.fail(reason)`
- `ctx.continueAsNew(input?)`

## Compiler Output

The compiler target should be a deterministic artifact that can be inspected and hashed.

Likely artifact sections:

- workflow metadata
- IR / state machine graph
- signal definitions
- activity / connector references
- marker definitions
- compiler version
- artifact hash

Possible lowered representation for the example above:

- state `WaitingForOrderPlaced`
- transition on signal `order_placed`
- state `TimerPending(15m)`
- transition on `TimerFired`
- state `InvokePredicate(inventory_check)`
- branch on predicate result
- state `ScheduleActivity(reserve_inventory)`
- terminal `Completed` or `Failed`

## Runtime Contract

Executors should operate on the compiled artifact with these rules:

- active executions stay warm on shard owners whenever possible
- replay from snapshot + event tail is the fallback path
- every execution epoch is pinned to `definition_version` and `artifact_hash`
- inbound signals follow explicit mailbox ordering rules
- side effects are represented through explicit scheduling and result events

## Versioning Rules

Workflow deployments must preserve replay safety.

Required rules:

- a running instance never silently switches to a new artifact
- artifact pinning is written into history
- incompatible code changes require a new artifact and safe rollout policy
- replay tooling must validate representative histories before deployment

## History Rollover

Long-lived or chatty workflows need explicit history rollover.

`ContinueAsNew`-style behavior should:

- end the current execution epoch
- start a fresh epoch with carried-forward state
- preserve logical workflow identity
- keep replay cost bounded

## Open Design Questions

- which language should host the first SDK
- how much source-level debugging should the compiler preserve
- whether signal handlers may interleave with the main workflow body
- which marker events are required in the first artifact model
