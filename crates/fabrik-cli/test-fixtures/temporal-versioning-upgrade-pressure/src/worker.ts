import { Worker } from "@temporalio/worker";
import * as activities from "./activities";

export async function createWorker() {
  return Worker.create({
    workflowsPath: "./src/workflows.ts",
    activities,
    taskQueue: "versioning-pressure",
    buildId: "versioning-pressure-v1",
  });
}
