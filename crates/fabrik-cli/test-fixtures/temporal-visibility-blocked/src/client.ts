export async function startOrder(client: { workflow: { start: Function } }) {
  return client.workflow.start("orderWorkflow", {
    taskQueue: "orders",
    args: ["alice"],
    memo: {
      region: "eu",
    },
    searchAttributes: {
      CustomKeywordField: ["vip"],
    },
  });
}
