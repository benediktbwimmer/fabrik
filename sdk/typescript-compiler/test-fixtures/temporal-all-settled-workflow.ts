import { proxyActivities } from "@temporalio/workflow";

const { benchmarkEcho } = proxyActivities({});

export async function temporalAllSettledWorkflow(input) {
  const results = await Promise.allSettled(input.items.map((item) => benchmarkEcho(item)));
  const firstRejected = results.find((result) => result.status === "rejected");
  if (firstRejected) {
    throw firstRejected.reason;
  }
  return results.map((result) => result.value);
}
