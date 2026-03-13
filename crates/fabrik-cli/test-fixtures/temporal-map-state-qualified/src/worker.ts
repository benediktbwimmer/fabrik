import { Worker } from "@temporalio/worker";

export async function createWorker() {
  return Worker.create({
    taskQueue: "map-state-qualified",
    workflowsPath: "./src/workflows.ts",
  });
}
