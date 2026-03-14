export async function startSinkPressure(client: { workflow: { start: Function } }) {
  return client.workflow.start("sinkPressureWorkflow", {
    taskQueue: "sinks-pressure",
    args: ["critical"],
    searchAttributes: {
      PressureGroup: "sinks",
    },
  });
}
