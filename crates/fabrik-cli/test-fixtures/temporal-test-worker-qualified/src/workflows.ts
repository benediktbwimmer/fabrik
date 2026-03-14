import { proxyActivities } from "@temporalio/workflow";

const { greet } = proxyActivities<typeof import("./activities")>({
  startToCloseTimeout: "1 minute",
});

export async function orderWorkflow(name: string): Promise<string> {
  return await greet(name);
}
