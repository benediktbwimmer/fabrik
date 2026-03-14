import { Worker } from "@temporalio/worker";
import * as activities from "@pressure/orders-activities";
import { ordersTaskQueue } from "@pressure/shared";

export async function createOrdersWorker() {
  return Worker.create({
    workflowsPath: require.resolve("./workflows"),
    activities,
    taskQueue: ordersTaskQueue,
    buildId: "monorepo-orders-v1",
  });
}
