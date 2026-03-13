import { sleep } from "@temporalio/workflow";

export async function workflowOnly(name: string): Promise<string> {
  await sleep("1s");
  return `hello ${name}`;
}
