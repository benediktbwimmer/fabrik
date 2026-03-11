export async function invalidAwaitWorkflow(ctx, input) {
  const pending = input;
  const value = await pending;
  return ctx.complete({ value, input });
}
