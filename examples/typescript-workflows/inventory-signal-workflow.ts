export async function inventorySignalWorkflow(ctx, input) {
  let adjustments = input.adjustments;

  if (adjustments.length === 0) {
    const signal = await ctx.waitForSignal("inventory.adjusted");
    adjustments = signal.adjustments;
  }

  const batch = await ctx.bulkActivity("inventory.apply_adjustment", adjustments, {
    execution: "eager",
    chunkSize: 128,
    reducer: "sum",
    retry: { maxAttempts: 2, delay: "1s" },
  });

  const summary = await batch.result();
  return ctx.complete({
    warehouseId: input.warehouseId,
    batchId: summary.batchId,
    appliedAdjustments: summary.succeededItems,
    rejectedAdjustments: summary.failedItems,
    totalDelta: summary.reducerOutput,
  });
}
