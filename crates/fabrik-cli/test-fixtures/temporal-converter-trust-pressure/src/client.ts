export async function startConverterPressure(client: { workflow: { start: Function } }) {
  return client.workflow.start("converterPressureWorkflow", {
    taskQueue: "converter-pressure",
    args: [{ id: "order-7", tags: ["vip", "alpha"] }],
    memo: {
      converter: "path-factory",
    },
    searchAttributes: {
      PressureGroup: "converter",
    },
  });
}
