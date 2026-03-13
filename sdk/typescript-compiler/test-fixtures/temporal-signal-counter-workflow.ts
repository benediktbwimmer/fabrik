import { condition, defineSignal, setHandler } from "@temporalio/workflow";

const tick = defineSignal("tick");

export async function temporalSignalCounterWorkflow(): Promise<number> {
  let progress = 0;
  setHandler(tick, () => {
    progress++;
  });
  await condition(() => progress > 0);
  return progress;
}
