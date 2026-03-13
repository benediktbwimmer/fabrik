import { defineQuery, defineUpdate, setHandler } from "@temporalio/workflow";

const summaryQuery = defineQuery("summary");
const approveUpdate = defineUpdate("approve");

export async function temporalQueryUpdateWorkflow(ctx, input) {
  setHandler(summaryQuery, (args) => ({
    workflow: input.workflow,
    filter: args.filter,
  }));

  setHandler(approveUpdate, async (args) => {
    const child = await ctx.startChild("childWorkflow", {
      id: args.id,
      workflow: input.workflow,
    }, {
      workflowId: "child-static",
      taskQueue: "payments",
      parentClosePolicy: "REQUEST_CANCEL",
    });
    const result = await child.result();
    return ctx.complete({
      approved: true,
      result,
    });
  });

  await ctx.waitForSignal("ready");
  return ctx.complete(input);
}
