import { condition, sleep } from "@temporalio/workflow";

export async function temporalPromiseRaceConditionTimeoutWorkflow(): Promise<string> {
  let ready = false;
  await Promise.race([sleep("30 days"), condition(() => ready)]);
  ready = true;
  await Promise.race([condition(() => ready), sleep("5s")]);
  return "done";
}
