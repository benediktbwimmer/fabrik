export async function startOrder(client: { workflow: { start: Function } }) {
  return client.workflow.start("orderWorkflow", {
    taskQueue: "orders",
    args: ["alice"],
    memo: {
      region: "eu",
      priority: 1,
    },
    searchAttributes: {
      CustomKeywordField: ["vip", "alpha"],
      Region: "eu",
    },
  });
}
