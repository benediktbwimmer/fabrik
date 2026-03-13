export async function versionWorkflow(ctx) {
  const gate = ctx.version("feature-x", 1, 3);
  await ctx.waitForSignal("approved");
  return ctx.complete({ gate });
}
