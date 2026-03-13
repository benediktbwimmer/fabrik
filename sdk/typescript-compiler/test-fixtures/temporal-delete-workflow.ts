import { condition, defineSignal, setHandler } from "@temporalio/workflow";

const clear = defineSignal<[string]>("clear");

export async function temporalDeleteWorkflow(): Promise<number> {
  let records: Record<string, boolean> = { a: true, b: true };
  setHandler(clear, (key: string) => {
    if (key in records) {
      delete records[key];
    }
  });
  await condition(() => Object.keys(records).length < 2);
  return Object.keys(records).length;
}
