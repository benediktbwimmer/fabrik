import { proxyActivities } from "@temporalio/workflow";

const defaults = {
  scheduleToCloseTimeout: "15s",
  startToCloseTimeout: "5s",
} as const;

const { greet } = proxyActivities<{
  greet(name: string): Promise<string>;
}>(defaults);

export async function temporalStaticProxyOptionsWorkflow(name: string): Promise<string> {
  return await greet(name);
}
