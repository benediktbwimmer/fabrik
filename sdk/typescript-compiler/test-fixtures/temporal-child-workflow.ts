import { executeChild, startChild } from "@temporalio/workflow";

async function childWorkflow(input) {
  return input;
}

export async function temporalChildWorkflow(input) {
  const child = await startChild(childWorkflow, {
    args: [input],
    workflowId: "child-started",
    taskQueue: "payments",
    parentClosePolicy: "REQUEST_CANCEL",
  });
  const childResult = await child.result();

  const directResult = await executeChild("namedChildWorkflow", {
    args: [input],
    workflowId: "child-executed",
  });

  return {
    childResult,
    directResult,
  };
}
