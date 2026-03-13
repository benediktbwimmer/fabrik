import {
  CancellationScope,
  executeChild,
  isCancellation,
  proxyActivities,
} from "@temporalio/workflow";

const { benchmarkEcho } = proxyActivities({
  startToCloseTimeout: "30s",
});

export async function temporalCancellationScopeWorkflow(input) {
  try {
    return await CancellationScope.cancellable(async () => benchmarkEcho(input.value));
  } catch (error) {
    if (isCancellation(error)) {
      return "cancelled";
    }
    throw error;
  }
}

export async function temporalCancellationScopeChildWorkflow(input) {
  try {
    return await CancellationScope.cancellable(async () =>
      executeChild("childWorkflow", {
        args: [input.value],
        workflowId: "child-cancel-scope",
      }),
    );
  } catch (error) {
    if (isCancellation(error)) {
      return "child-cancelled";
    }
    throw error;
  }
}
