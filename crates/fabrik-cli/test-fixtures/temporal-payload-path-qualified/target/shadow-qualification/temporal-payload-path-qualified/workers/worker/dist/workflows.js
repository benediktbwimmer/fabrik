import { condition, defineSignal, proxyActivities, setHandler, } from "@temporalio/workflow";
const approvedSignal = defineSignal("approved");
const activities = proxyActivities({
    startToCloseTimeout: "30s",
});
export async function orderWorkflow(name) {
    let approved = false;
    setHandler(approvedSignal, (value) => {
        approved = value;
    });
    await condition(() => approved);
    const greeted = await activities.greet(name);
    return greeted;
}
