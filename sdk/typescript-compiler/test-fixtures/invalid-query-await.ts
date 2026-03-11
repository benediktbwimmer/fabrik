export async function invalidQueryAwaitWorkflow(ctx, input) {
  ctx.query("bad", async () => {
    await ctx.activity("core.echo", input);
    return input;
  });

  return ctx.complete(input);
}
