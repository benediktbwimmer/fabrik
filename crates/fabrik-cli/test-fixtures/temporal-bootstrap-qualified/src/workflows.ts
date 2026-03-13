import { sleep } from "@temporalio/workflow";

export async function greetingWorkflow(name: string): Promise<string> {
  await sleep("1s");
  return name;
}
