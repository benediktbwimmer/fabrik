import { executeChild } from "@temporalio/workflow";

export async function childWorkflow(input: number): Promise<number> {
  return input;
}

export async function temporalChildPromiseAllWorkflow(values: number[]): Promise<number[]> {
  const childPromises: Promise<number>[] = [];
  for (const value of values) {
    childPromises.push(
      executeChild(childWorkflow, {
        args: [value],
        workflowId: `child/${value}`,
      }),
    );
  }
  return await Promise.all(childPromises);
}
