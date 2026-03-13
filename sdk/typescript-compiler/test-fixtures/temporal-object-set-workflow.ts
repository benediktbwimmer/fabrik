import { condition, defineSignal, setHandler } from "@temporalio/workflow";

const mark = defineSignal<[string]>("mark");

export async function temporalObjectSetWorkflow(): Promise<number> {
  let records: Record<string, boolean> = {};
  setHandler(mark, (key: string) => {
    records[key] = true;
  });
  await condition(() => Object.keys(records).length > 0);
  return Object.keys(records).length;
}
