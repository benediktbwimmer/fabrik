import { defineSignal, startChild } from "@temporalio/workflow";

const approve = defineSignal<[string]>("approve");

async function childWorkflow(input) {
  return input;
}

export async function temporalChildHandleControlWorkflow(input) {
  const child = await startChild(childWorkflow, {
    args: [input],
    workflowId: "child-controlled",
    taskQueue: "payments",
  });

  await child.signal(approve, input);
  await child.cancel("stop");

  return "done";
}
