import { sleep } from "@temporalio/workflow";

export async function temporalSleepHandleWorkflow(): Promise<string> {
  const timer = sleep("5s");
  await timer;
  return "done";
}
