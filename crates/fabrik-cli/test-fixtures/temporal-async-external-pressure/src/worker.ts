import { Worker } from "@temporalio/worker";
import { createPressureActivities } from "./activities";

export async function createWorker() {
  return Worker.create({
    workflowsPath: require.resolve("./workflows"),
    activities: createPressureActivities("echo"),
    taskQueue: "async-external-pressure",
    buildId: "async-external-pressure-v1",
  });
}
