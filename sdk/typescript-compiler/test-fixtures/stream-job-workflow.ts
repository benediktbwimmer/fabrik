export async function streamJobWorkflow(ctx, input) {
  const job = await ctx.startStreamJob("keyed-rollup", {
    input: {
      kind: "bounded_items",
      items: input.payments,
    },
    config: {
      checkpoint: "initial-rollup-ready",
    },
  });

  const checkpoint = await job.untilCheckpoint("initial-rollup-ready");
  const account = await job.query("accountTotals", { key: input.accountId }, {
    consistency: "strong",
  });

  return ctx.complete({ checkpoint, account });
}
