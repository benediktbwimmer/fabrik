import { DefaultPayloadConverter } from "@temporalio/common";
import { Worker } from "@temporalio/worker";
import * as activities from "./activities";

const dataConverter = {
  payloadConverter: new DefaultPayloadConverter(),
  payloadCodecs: [],
};

export async function createWorker() {
  return Worker.create({
    taskQueue: "orders",
    workflowsPath: "./src/workflows.ts",
    activities,
    dataConverter,
    buildId: "payload-orders-build",
  });
}
