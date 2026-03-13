import { Worker } from "@temporalio/worker";

import * as activities from "./activities";

export async function createWorker() {
  return Worker.create({
    workflowsPath: "./src/workflows.ts",
    activities,
    taskQueue: "orders",
    dataConverter: {
      payloadConverterPath: "./src/custom-payload-converter.ts",
    },
    buildId: "payload-path-orders-build-v2",
  });
}
