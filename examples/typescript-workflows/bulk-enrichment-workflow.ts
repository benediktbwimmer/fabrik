export async function bulkEnrichmentWorkflow(ctx, input) {
  const items = input.items;
  const batch = await ctx.bulkActivity("process.enrich", items, {
    execution: "eager",
    chunkSize: 256,
    reducer: "histogram",
    retry: { maxAttempts: 3, delay: "1s" },
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
