export async function bulkWorkflow(ctx, input) {
  const bulk = await ctx.bulkActivity("benchmark.echo", input.items, {
    execution: "eager",
    reducer: "collect_results",
    taskQueue: "bulk",
    chunkSize: 128,
    retry: { maxAttempts: 2, delay: "1s" },
  });

  const summary = await bulk.result();
  return ctx.complete(summary);
}
