export async function invalidBulkBackendWorkflow(ctx, input) {
  const bulk = await ctx.bulkActivity("benchmark.echo", input.items, {
    backend: "postgres",
    taskQueue: "bulk",
  });

  const summary = await bulk.result();
  return ctx.complete(summary);
}
