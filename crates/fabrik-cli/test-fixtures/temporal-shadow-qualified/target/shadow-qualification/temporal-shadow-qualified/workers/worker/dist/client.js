const region = "eu";
const memoRegion = region === "eu" ? region : "us";
const memo = {
    region: memoRegion,
    priority: 2,
};
const searchAttributes = {
    CustomKeywordField: ["vip", "shadow"],
    Region: region === "eu" ? "eu" : "us",
};
export async function startShadowOrder(client) {
    return client.workflow.start("orderWorkflow", {
        taskQueue: "orders",
        args: ["alice"],
        memo,
        searchAttributes,
    });
}
