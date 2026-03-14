export async function startPressureWorkflow(client: { workflow: { start: Function } }) {
  return client.workflow.start("interceptorPressureWorkflow", {
    taskQueue: "interceptor-pressure",
    args: ["draft"],
    memo: {
      lane: "interceptor",
    },
    searchAttributes: {
      PressureGroup: "interceptor",
    },
  });
}
