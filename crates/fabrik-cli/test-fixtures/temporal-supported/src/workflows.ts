import {
  condition,
  continueAsNew,
  defineSignal,
  proxyActivities,
  setHandler,
} from "@temporalio/workflow";

const approvedSignal = defineSignal<[boolean]>("approved");
const activities = proxyActivities<typeof import("./activities")>({
  startToCloseTimeout: "30s",
  retry: { maximumAttempts: 3, initialInterval: "5s" },
});

export async function orderWorkflow(name: string, restart = false): Promise<string> {
  let approved = false;
  setHandler(approvedSignal, (value: boolean) => {
    approved = value;
  });
  await condition(() => approved);
  const greeted = await activities.greet(name);
  if (restart) {
    return continueAsNew<typeof orderWorkflow>(name, false);
  }
  return greeted;
}
