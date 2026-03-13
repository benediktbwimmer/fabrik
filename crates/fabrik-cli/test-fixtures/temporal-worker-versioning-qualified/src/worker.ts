import { Worker } from "@temporalio/worker";
import * as activities from "./activities";

export async function createWorker() {
  return Worker.create({
    taskQueue: "worker-versioning-qualified",
    workflowsPath: "./src/workflows.ts",
    activities,
    buildId: "worker-versioning-qualified-build",
  });
}
