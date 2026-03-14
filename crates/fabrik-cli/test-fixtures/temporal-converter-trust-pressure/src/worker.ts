import { Worker } from "@temporalio/worker";
import * as activities from "./activities";
import { createPressureConverter } from "./data-converter";

export async function createWorker() {
  return Worker.create({
    workflowsPath: "./src/workflows.ts",
    activities,
    taskQueue: "converter-pressure",
    dataConverter: await createPressureConverter(),
    buildId: "converter-pressure-v1",
  });
}
