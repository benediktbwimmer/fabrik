import { Worker } from "@temporalio/worker";
import * as activities from "./activities";

export async function createWorker() {
  return Worker.create({
    taskQueue: "supported-api",
    workflowsPath: "./src/workflows.ts",
    activities,
    buildId: "supported-api-build",
  });
}
