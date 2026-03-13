import * as wf from "@temporalio/workflow";

export async function childWorkflow(): Promise<string> {
  const parent = wf.workflowInfo().parent;
  return parent ? parent.workflowId : "no-parent";
}

export async function temporalSupportedApiWorkflow(shouldFail = false): Promise<string> {
  const childToken = wf.uuid4();
  wf.log.info(`${wf.workflowInfo().workflowId}:${childToken}`);
  if (shouldFail) {
    throw wf.ApplicationFailure.nonRetryable(`failed:${wf.workflowInfo().runId}`);
  }
  const child = await wf.startChild(childWorkflow, {
    workflowId: `${wf.workflowInfo().workflowId}/${childToken}`,
    parentClosePolicy: wf.ParentClosePolicy.ABANDON,
  });
  return await child.result();
}
