export async function localStreamingQuickstartWorkflow(ctx, input) {
  const batch = await ctx.bulkActivity("benchmark.echo", input, {
    execution: "eager",
    taskQueue: "bulk",
    chunkSize: 64,
    reducer: "histogram",
    retry: { maxAttempts: 2, delay: "1s" },
  });

  const summary = await batch.result();
  return ctx.complete({
    batchId: summary.batchId,
    status: summary.status,
    totalItems: summary.totalItems,
    succeededItems: summary.succeededItems,
    failedItems: summary.failedItems,
    histogram: summary.reducerOutput,
  });
}
