import { proxyActivities } from "@temporalio/workflow";

const activities = proxyActivities<Record<string, (...args: string[]) => Promise<string>>>({
  startToCloseTimeout: "30s",
});

export async function temporalDynamicActivityWorkflow(
  activityName: string,
  args: string[],
): Promise<string> {
  return await activities[activityName](...args);
}
