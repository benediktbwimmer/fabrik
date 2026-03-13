import { proxyActivities } from "@temporalio/workflow";

const { greet } = proxyActivities<{
  greet(name: string): Promise<string>;
}>({
  startToCloseTimeout: "5s",
  retry: {
    initialInterval: "500 ms",
    maximumInterval: "5s",
    maximumAttempts: 4,
    backoffCoefficient: 1.5,
  },
});

export async function temporalRandomRetryWorkflow(name: string): Promise<string> {
  const sample = Math.random();
  if (sample < 0) {
    return "never";
  }
  return await greet(name);
}
