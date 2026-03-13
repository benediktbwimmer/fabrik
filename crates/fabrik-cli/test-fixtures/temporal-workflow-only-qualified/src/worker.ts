import { Worker } from "@temporalio/worker";

export async function createWorker() {
  return Worker.create({
    taskQueue: "workflow-only",
    workflowsPath: "./src/workflows.ts",
    buildId: "workflow-only-build",
  });
}
