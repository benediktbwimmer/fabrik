import type {
  KeyedRollupJob,
  WorkflowContext,
} from "../workflow-authoring.js";

export async function streamJobWorkflow(
  ctx: WorkflowContext,
  input: {
    payments: Array<{ eventId: string; accountId: string; amount: number }>;
    accountId: string;
  },
) {
  const job = await ctx.startStreamJob<KeyedRollupJob>("keyed-rollup", {
    input: {
      kind: "bounded_items",
      items: input.payments,
    },
    config: {
      checkpoint: "hourly-rollup-ready",
    },
  });

  const checkpoint = await job.untilCheckpoint("hourly-rollup-ready");
  const account = await job.query("accountTotals", { key: input.accountId }, {
    consistency: "strong",
  });
  const result = await job.result();
  const total = account.totalAmount + checkpoint.checkpointSequence;

  return ctx.complete({ checkpoint, account, result, total });
}
