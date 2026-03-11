export async function invalidBulkWorkflow(ctx, input) {
  const chunkSize = input.chunkSize;
  const bulk = await ctx.bulkActivity("benchmark.echo", input.items, {
    chunkSize: chunkSize,
  });
  const summary = await bulk.result();
  return ctx.complete(summary);
}
