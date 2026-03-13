import { condition, defineSignal, setHandler } from "@temporalio/workflow";

const approved = defineSignal<[string]>("approved");

export async function temporalSignalConditionWorkflow() {
  let ready = false;
  let payload: string | null = null;

  setHandler(approved, (value) => {
    ready = true;
    payload = value;
  });

  await condition(() => ready);
  return payload;
}
