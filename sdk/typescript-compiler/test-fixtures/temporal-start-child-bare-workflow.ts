import { startChild } from "@temporalio/workflow";

export async function childWorkflow(input: string): Promise<string> {
  return input;
}

export async function temporalStartChildBareWorkflow(input: string): Promise<string> {
  await startChild(childWorkflow, {
    args: [input],
    workflowId: `child/${input}`,
  });
  return input;
}
