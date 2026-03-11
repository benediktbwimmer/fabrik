export async function activityWorkflow(ctx, input) {
  const approval = await ctx.waitForSignal("approved");
  const echoed = await ctx.activity("core.echo", {
    approval,
    item: input.items[0],
  });
  return ctx.complete(echoed);
}
