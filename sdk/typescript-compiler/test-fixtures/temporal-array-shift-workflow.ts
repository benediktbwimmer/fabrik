import { condition, defineSignal, setHandler } from "@temporalio/workflow";

const pushSignal = defineSignal<[string]>("push");

export async function temporalArrayShiftWorkflow(): Promise<string | null> {
  let items: string[] = [];
  setHandler(pushSignal, (value: string) => {
    items.push(value);
  });
  await condition(() => items.length > 0);
  const next = items.shift();
  return next ?? null;
}
