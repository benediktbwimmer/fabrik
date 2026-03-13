import { Worker } from "@temporalio/worker";
import * as activities from "./activities";

export async function createWorker() {
  return Worker.create({
    taskQueue: "orders",
    workflowsPath: "./src/workflows.ts",
    activities,
    buildId: "orders-build",
  });
}
