import { InjectedSinks, Worker } from "@temporalio/worker";
import type { PressureSinks } from "./workflows";

export async function createWorker() {
  const sinks: InjectedSinks<PressureSinks> = {
    alerts: {
      emit: {
        fn(workflowInfo, severity, message) {
          console.log("sink alert", {
            workflowId: workflowInfo.workflowId,
            runId: workflowInfo.runId,
            severity,
            message,
          });
        },
        callDuringReplay: false,
      },
    },
  };
  return Worker.create({
    workflowsPath: require.resolve("./workflows"),
    taskQueue: "sinks-pressure",
    sinks,
    buildId: "sinks-pressure-v1",
  });
}
