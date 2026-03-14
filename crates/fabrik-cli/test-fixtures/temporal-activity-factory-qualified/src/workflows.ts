import { proxyActivities } from "@temporalio/workflow";
import type { createActivities } from "./activities";

const { greet } = proxyActivities<ReturnType<typeof createActivities>>({
  startToCloseTimeout: "1 minute",
});

export async function orderWorkflow(prefix: string): Promise<string> {
  return await greet(prefix);
}
