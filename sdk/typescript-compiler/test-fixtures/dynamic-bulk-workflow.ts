export async function dynamicBulkWorkflow(ctx, input) {
  const activityName = input.activityName;
  const bulk = await ctx.bulkActivity(activityName, input.items, {
    execution: "eager",
    reducer: "count",
    taskQueue: "bulk",
    chunkSize: 64,
    capabilities: { payloadlessTransport: true },
  });

  const summary = await bulk.result();
  return ctx.complete(summary);
}
