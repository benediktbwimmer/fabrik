import * as workflow from "@temporalio/workflow";

const addItem = workflow.defineSignal<[string]>("addItem");

export async function temporalVoidPushSignalWorkflow(): Promise<number> {
  let inputs = Array<string>();
  workflow.setHandler(addItem, (input) => void inputs.push(input));
  await workflow.condition(() => inputs.length > 0);
  return inputs.length;
}
