import { Worker } from "@temporalio/worker";
import * as activities from "./activities";

export async function createWorker() {
  return Worker.create({
    taskQueue: "patching-qualified",
    workflowsPath: "./src/workflows.ts",
    activities,
    buildId: "patching-qualified-build",
  });
}
