import { ordersTaskQueue, reportsTaskQueue } from "@pressure/shared";

export async function startMonorepoPressure(client: { workflow: { start: Function } }) {
  await client.workflow.start("ordersWorkflow", {
    taskQueue: ordersTaskQueue,
    args: ["order-77"],
  });
  return client.workflow.start("reportsWorkflow", {
    taskQueue: reportsTaskQueue,
    args: ["report-77"],
  });
}
