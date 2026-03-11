export async function markerWorkflow(ctx, input) {
  const startedAt = ctx.now();
  const token = ctx.uuid();
  const snapshot = ctx.sideEffect({ startedAt, token });

  await ctx.waitForSignal("approved");
  return ctx.complete({ startedAt, token, snapshot, input });
}
