import { deprecatePatch, patched, proxyActivities, sleep } from "@temporalio/workflow";

const activities = proxyActivities<typeof import("./activities")>({
  startToCloseTimeout: "30s",
});

export async function patchedWorkflow(): Promise<string> {
  if (patched("feature-x")) {
    return await activities.greet("new");
  }
  return await activities.greet("old");
}

export async function deprecatedPatchWorkflow(): Promise<string> {
  deprecatePatch("feature-x");
  await sleep("1s");
  return await activities.greet("final");
}
