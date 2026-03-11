export async function invalidGlobalWorkflow(ctx, input) {
  const token = process.env.API_TOKEN;
  return ctx.complete({ token, input });
}
