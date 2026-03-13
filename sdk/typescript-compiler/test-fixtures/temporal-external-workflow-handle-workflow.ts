import { defineSignal, getExternalWorkflowHandle } from "@temporalio/workflow";

const approve = defineSignal<[string]>("approve");

export async function temporalExternalWorkflowHandleWorkflow(input) {
  const handle = getExternalWorkflowHandle("target-workflow", "target-run");

  await handle.signal(approve, input);
  await handle.cancel("stop");

  return "done";
}
