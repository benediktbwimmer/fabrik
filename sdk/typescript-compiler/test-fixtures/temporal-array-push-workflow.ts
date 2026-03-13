import { condition, defineSignal, setHandler } from "@temporalio/workflow";

const pushSignal = defineSignal<[string]>("push");

export async function temporalArrayPushWorkflow(): Promise<string | null> {
  let items: string[] = [];
  setHandler(pushSignal, (value: string) => {
    items.push(value);
  });
  await condition(() => items.length > 0);
  return items[0] ?? null;
}
