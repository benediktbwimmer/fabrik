export async function stableIdsWorkflow(ctx, input) {
  const signal = await ctx.waitForSignal("approved");

  if (signal.ok) {
    await ctx.activity("core.echo", signal);
  }

  return ctx.complete(input);
}
