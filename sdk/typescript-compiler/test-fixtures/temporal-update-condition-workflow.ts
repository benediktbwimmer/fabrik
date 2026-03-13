import { condition, defineQuery, defineUpdate, setHandler } from "@temporalio/workflow";

const setValue = defineUpdate("setValue");
const currentValue = defineQuery("currentValue");

export async function temporalUpdateConditionWorkflow() {
  let value = 0;

  setHandler(currentValue, () => value);
  setHandler(setValue, async (nextValue) => {
    value = nextValue;
    return value;
  });

  await condition(() => value > 0);
  return value;
}
