import { Worker } from "@temporalio/worker";
import { createAuditActivities } from "./activities";

export async function createWorker() {
  return Worker.create({
    workflowsPath: require.resolve("./workflows"),
    activities: createAuditActivities("audit"),
    taskQueue: "interceptor-pressure",
    interceptors: {
      workflowModules: [require.resolve("./interceptors")],
    },
    buildId: "interceptor-pressure-v1",
  });
}
