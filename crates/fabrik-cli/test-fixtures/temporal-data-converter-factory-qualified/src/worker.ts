import { Worker } from "@temporalio/worker";
import { getDataConverter } from "./data-converter";

export async function createWorker() {
  return Worker.create({
    taskQueue: "codec-factory",
    workflowsPath: "./src/workflows.ts",
    dataConverter: await getDataConverter(),
  });
}
