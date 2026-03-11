export async function queryUpdateChildWorkflow(ctx, input) {
  ctx.query("summary", (args) => ({
    workflow: input.workflow,
    filter: args.filter,
  }));

  ctx.update("approve", async (args) => {
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
