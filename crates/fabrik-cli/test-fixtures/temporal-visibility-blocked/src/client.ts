export async function startOrder(client: { workflow: { start: Function } }) {
  const region = "eu";
  const attrs = {
    CustomKeywordField: ["vip"],
  };
  return client.workflow.start("orderWorkflow", {
    taskQueue: "orders",
    args: ["alice"],
    memo: region === "eu" ? { region } : {},
    searchAttributes: attrs,
  });
}
