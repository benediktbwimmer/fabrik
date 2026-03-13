import { sleep } from "@temporalio/workflow";

export async function esmWorkflow(name: string): Promise<string> {
  await sleep("1s");
  return name;
}
