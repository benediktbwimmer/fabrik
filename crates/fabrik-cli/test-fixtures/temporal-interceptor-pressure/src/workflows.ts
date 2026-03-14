import {
  condition,
  defineSignal,
  proxyActivities,
  setHandler,
} from "@temporalio/workflow";
import { trackedValue } from "./interceptors";
import type { createAuditActivities } from "./activities";

const approveSignal = defineSignal<[string]>("approve");
const { publishAudit } = proxyActivities<ReturnType<typeof createAuditActivities>>({
  startToCloseTimeout: "5m",
});

export async function interceptorPressureWorkflow(initialValue: string): Promise<string> {
  const state = trackedValue(initialValue);
  let nextValue = initialValue;
  setHandler(approveSignal, (value: string) => {
    nextValue = value;
  });
  await condition(() => nextValue !== initialValue);
  state.value = nextValue;
  await publishAudit(999, nextValue);
  return state.value;
}
