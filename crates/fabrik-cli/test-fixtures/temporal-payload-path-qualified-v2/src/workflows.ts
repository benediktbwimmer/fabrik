import {
  condition,
  defineSignal,
  proxyActivities,
  setHandler,
} from "@temporalio/workflow";

const approvedSignal = defineSignal<[boolean]>("approved");
const activities = proxyActivities<typeof import("./activities")>({
  startToCloseTimeout: "30s",
});

export async function orderWorkflow(name: string): Promise<string> {
  let approved = false;
  setHandler(approvedSignal, (value: boolean) => {
    approved = value;
  });
  await condition(() => approved);
  const greeted = await activities.greet(name);
  return greeted + " from build v2";
}
