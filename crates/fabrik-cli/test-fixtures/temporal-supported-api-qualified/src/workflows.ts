import {
  ApplicationFailure,
  ParentClosePolicy,
  log,
  proxyActivities,
  startChild,
  uuid4,
  workflowInfo,
} from "@temporalio/workflow";

const activities = proxyActivities<typeof import("./activities")>({
  startToCloseTimeout: "30s",
});

export async function childWorkflow(name: string): Promise<string> {
  const parent = workflowInfo().parent;
  const parentId = parent ? parent.workflowId : "no-parent";
  return await activities.greet(`${parentId}:${name}`);
}

export async function supportedApiWorkflow(shouldFail = false): Promise<string> {
  const childToken = uuid4();
  log.info(`${workflowInfo().workflowId}:${childToken}`);
  if (shouldFail) {
    throw ApplicationFailure.nonRetryable(`failed:${workflowInfo().runId}`);
  }
  const child = await startChild(childWorkflow, {
    workflowId: `${workflowInfo().workflowId}/${childToken}`,
    parentClosePolicy: ParentClosePolicy.ABANDON,
    args: ["worker"],
  });
  return await child.result();
}
