export async function dynamicCtxActivityWorkflow(ctx, input) {
  const activityName = input.activityName;
  const echoed = await ctx.activity(activityName, input.payload);
  return ctx.complete(echoed);
}
