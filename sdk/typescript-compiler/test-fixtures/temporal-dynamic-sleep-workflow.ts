import { sleep } from "@temporalio/workflow";

export async function temporalDynamicSleepWorkflow(seconds?: number): Promise<string> {
  await sleep(`${seconds ?? 1}s`);
  await sleep((seconds ?? 1) * 1000);
  return "done";
}
