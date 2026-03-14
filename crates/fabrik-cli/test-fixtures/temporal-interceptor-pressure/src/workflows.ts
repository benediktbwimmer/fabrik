import {
  condition,
  defineQuery,
  defineSignal,
  proxyActivities,
  setHandler,
} from "@temporalio/workflow";
import type { createAuditActivities } from "./activities";

const approveSignal = defineSignal<[string]>("approve");
const trackedValueQuery = defineQuery<{ value: string }>("trackedValue");
const { publishAudit } = proxyActivities<ReturnType<typeof createAuditActivities>>({
  startToCloseTimeout: "5m",
});

export async function interceptorPressureWorkflow(initialValue: string): Promise<string> {
  const state = { value: initialValue };
  let nextValue = initialValue;
  setHandler(trackedValueQuery, () => ({
    value: state.value,
  }));
  setHandler(approveSignal, (value: string) => {
    nextValue = value;
  });
  await condition(() => nextValue !== initialValue);
  state.value = nextValue;
  await publishAudit(999, nextValue);
  return state.value;
}
