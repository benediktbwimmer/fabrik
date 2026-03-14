import {
  Sinks,
  condition,
  defineQuery,
  defineSignal,
  log,
  proxySinks,
  setHandler,
} from "@temporalio/workflow";

export interface PressureSinks extends Sinks {
  alerts: {
    emit(severity: string, message: string): void;
  };
}

const { alerts } = proxySinks<PressureSinks>();
const releaseSignal = defineSignal<[boolean]>("release");
const statusQuery = defineQuery<{ severity: string; released: boolean }>("status");

export async function sinkPressureWorkflow(severity: string): Promise<string> {
  log.info("sink pressure workflow started");
  let released = false;
  setHandler(releaseSignal, () => {
    released = true;
  });
  setHandler(statusQuery, () => ({
    severity,
    released,
  }));
  await condition(() => released);
  alerts.emit(severity, `workflow raised ${severity}`);
  return `alerted:${severity}`;
}
