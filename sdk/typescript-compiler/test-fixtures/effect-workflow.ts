export async function effectWorkflow(ctx, input) {
  const approval = await ctx.waitForSignal("approved");
  const echoed = await ctx.sideEffect("core.echo", {
    approval,
    item: input.items[0],
  }, { timeout: "30s" });
  return ctx.complete(echoed);
}
