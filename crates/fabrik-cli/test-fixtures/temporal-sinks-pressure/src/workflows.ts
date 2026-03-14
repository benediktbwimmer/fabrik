import { Sinks, log, proxySinks } from "@temporalio/workflow";

export interface PressureSinks extends Sinks {
  alerts: {
    emit(severity: string, message: string): void;
  };
}

const { alerts } = proxySinks<PressureSinks>();

export async function sinkPressureWorkflow(severity: string): Promise<string> {
  log.info("sink pressure workflow started");
  alerts.emit(severity, `workflow raised ${severity}`);
  return `alerted:${severity}`;
}
