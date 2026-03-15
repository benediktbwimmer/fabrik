import type {
  KeyedRollupJob,
  WorkflowContext,
} from "../../sdk/typescript-compiler/workflow-authoring.js";

export async function streamJobRollupWorkflow(
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
      thresholdAccountId: input.accountId,
    },
  });

  const checkpoint = await job.untilCheckpoint("hourly-rollup-ready");
  const account = await job.query(
    "accountTotals",
    { key: input.accountId },
    { consistency: "strong" },
  );
  const result = await job.result();
  const projectedTotal = account.totalAmount;

  return ctx.complete({
    jobId: job.jobId,
    checkpointName: checkpoint.checkpointName,
    checkpointSequence: checkpoint.checkpointSequence,
    terminalStatus: result.status,
    terminalJobName: result.jobName,
    projectedTotal,
    account,
  });
}
