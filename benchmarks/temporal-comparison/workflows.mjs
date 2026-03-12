import { proxyActivities } from "@temporalio/workflow";

const { benchmarkEcho } = proxyActivities({
  startToCloseTimeout: "30s",
  retry: {
    initialInterval: "1s",
    maximumAttempts: 2,
    nonRetryableErrorTypes: ["BenchmarkCancelled"],
  },
});

export async function fanoutBenchmarkWorkflow(input) {
  const results = await Promise.allSettled(input.items.map((item) => benchmarkEcho(item)));
  const firstRejected = results.find((result) => result.status === "rejected");
  if (firstRejected) {
    throw firstRejected.reason;
  }
  return results.map((result) => result.value);
}
