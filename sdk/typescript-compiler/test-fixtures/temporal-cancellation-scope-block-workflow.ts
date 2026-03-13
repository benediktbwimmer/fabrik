import { CancellationScope, isCancellation, proxyActivities } from "@temporalio/workflow";

const { benchmarkEcho } = proxyActivities<{
  benchmarkEcho(input: string): Promise<string>;
}>({
  startToCloseTimeout: "30s",
});

export async function temporalCancellationScopeBlockWorkflow(input: string): Promise<string> {
  try {
    const result = await CancellationScope.cancellable(async () => {
      const first = await benchmarkEcho(input);
      const second = await benchmarkEcho(first);
      return second;
    });
    return result;
  } catch (error) {
    if (isCancellation(error)) {
      return "cancelled";
    }
    throw error;
  }
}
