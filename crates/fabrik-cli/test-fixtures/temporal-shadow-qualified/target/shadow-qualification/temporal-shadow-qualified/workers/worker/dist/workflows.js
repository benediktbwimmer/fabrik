import { condition, continueAsNew, defineSignal, proxyActivities, setHandler, } from "@temporalio/workflow";
const approvedSignal = defineSignal("approved");
const activities = proxyActivities({
    startToCloseTimeout: "30s",
    retry: { maximumAttempts: 3, initialInterval: "5s" },
});
export async function orderWorkflow(name, restart = false) {
    let approved = false;
    setHandler(approvedSignal, (value) => {
        approved = value;
    });
    await condition(() => approved);
    const greeted = await activities.greet(name);
    if (restart) {
        return continueAsNew(name, false);
    }
    return greeted;
}
