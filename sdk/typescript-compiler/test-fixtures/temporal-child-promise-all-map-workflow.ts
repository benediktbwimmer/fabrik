import { executeChild } from "@temporalio/workflow";

export async function childNameWorkflow(name: string): Promise<string> {
  return `child:${name}`;
}

export async function temporalChildPromiseAllMapWorkflow(names: string[]): Promise<string[]> {
  return await Promise.all(
    names.map((name) =>
      executeChild(childNameWorkflow, {
        args: [name],
      }),
    ),
  );
}
