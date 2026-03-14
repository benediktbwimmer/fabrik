export async function startVersioningPressure(client: { workflow: { start: Function } }) {
  return client.workflow.start("versioningPressureWorkflow", {
    taskQueue: "versioning-pressure",
    args: ["alice"],
    memo: {
      lane: "upgrade",
    },
    searchAttributes: {
      PressureGroup: "versioning",
    },
  });
}
