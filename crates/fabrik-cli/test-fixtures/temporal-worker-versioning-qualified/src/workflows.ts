import { patched, proxyActivities, setWorkflowOptions } from "@temporalio/workflow";

const activities = proxyActivities<typeof import("./activities")>({
  startToCloseTimeout: "30s",
});

setWorkflowOptions({ versioningBehavior: "AUTO_UPGRADE" }, autoUpgradeWorkflow);
export async function autoUpgradeWorkflow(): Promise<string> {
  if (patched("feature-x")) {
    return await activities.greet("auto-new");
  }
  return await activities.greet("auto-old");
}

setWorkflowOptions({ versioningBehavior: "PINNED" }, pinnedWorkflow);
export async function pinnedWorkflow(): Promise<string> {
  return await activities.greet("pinned");
}
