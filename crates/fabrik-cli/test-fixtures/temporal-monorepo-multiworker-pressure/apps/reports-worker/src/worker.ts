import { Worker } from "@temporalio/worker";
import * as activities from "@pressure/reports-activities";
import { reportsTaskQueue } from "@pressure/shared";

export async function createReportsWorker() {
  return Worker.create({
    workflowsPath: require.resolve("./workflows"),
    activities,
    taskQueue: reportsTaskQueue,
    buildId: "monorepo-reports-v1",
  });
}
