import { CompositePayloadConverter } from "@temporalio/common";
import { Worker } from "@temporalio/worker";

const dataConverter = {
  payloadConverter: new CompositePayloadConverter(),
};

export async function createWorker() {
  return Worker.create({
    taskQueue: "orders",
    workflowsPath: "./src/workflows.ts",
    dataConverter,
  });
}
