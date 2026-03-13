import { Worker } from "@temporalio/worker";
import * as activities from "./activities";

export async function createWorker() {
  return Worker.create({
    taskQueue: "orders",
    workflowsPath: "./src/workflows.ts",
    activities,
    dataConverter: {
      payloadConverterPath: "./src/custom-payload-converter.ts",
    },
    buildId: "payload-path-orders-build",
  });
}
