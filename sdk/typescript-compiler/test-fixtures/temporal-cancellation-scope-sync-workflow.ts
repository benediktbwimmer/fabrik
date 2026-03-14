import { CancellationScope, proxyActivities } from "@temporalio/workflow";

const { cleanup } = proxyActivities({
  startToCloseTimeout: "30s",
});

export async function temporalCancellationScopeSyncWorkflow(url: string): Promise<string> {
  await CancellationScope.nonCancellable(() => cleanup(url));
  return "done";
}

export async function temporalCancellationScopeSyncReturnWorkflow(url: string): Promise<unknown> {
  return CancellationScope.nonCancellable(() => cleanup(url));
}
