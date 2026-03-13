import { proxyActivities } from "@temporalio/workflow";

const defaults = {
  startToCloseTimeout: "5s",
  scheduleToCloseTimeout: "15s",
} as const;

const { cleanup, greet } = proxyActivities<{
  cleanup(name: string): Promise<void>;
  greet(name: string): Promise<string>;
}>(defaults);

interface Compensation {
  message: string;
  fn: () => Promise<void>;
}

export async function temporalDeferredActivityThunkWorkflow(name: string): Promise<string> {
  const compensations: Compensation[] = [];
  compensations.unshift({
    message: "cleanup",
    fn: () => cleanup(name),
  });

  for (const comp of compensations) {
    await comp.fn();
  }

  return await greet(name);
}
