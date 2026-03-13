import { Worker } from "@temporalio/worker";

function workerOptions() {
  return {
    taskQueue: process.env.TEMPORAL_TASK_QUEUE ?? "orders",
    workflowsPath: "./src/workflows.ts",
  };
}

export async function createWorker() {
  return Worker.create(workerOptions());
}
