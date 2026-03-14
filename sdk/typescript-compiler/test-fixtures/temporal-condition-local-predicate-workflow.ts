import { condition } from "@temporalio/workflow";

export async function temporalConditionLocalPredicateWorkflow(): Promise<void> {
  let ready = false;
  const isReady = () => ready;
  ready = true;
  await condition(isReady);
}
