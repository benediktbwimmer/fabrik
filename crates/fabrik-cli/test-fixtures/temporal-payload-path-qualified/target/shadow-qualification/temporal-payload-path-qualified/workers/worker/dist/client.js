export async function startOrder(client) {
    return client.workflow.start("orderWorkflow", {
        taskQueue: "orders",
        args: ["alice"],
    });
}
