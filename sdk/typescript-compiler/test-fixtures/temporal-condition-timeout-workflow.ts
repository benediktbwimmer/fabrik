import { condition, defineSignal, setHandler } from "@temporalio/workflow";

const approvedSignal = defineSignal<[boolean]>("approved");

export async function temporalConditionTimeoutWorkflow(): Promise<boolean> {
  let approved = false;

  setHandler(approvedSignal, (value: boolean) => {
    approved = value;
  });

  return await condition(() => approved, "5s");
}
