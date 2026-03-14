import { proxyActivities } from "@temporalio/workflow";

const activities = proxyActivities<typeof import("@pressure/orders-activities")>({
  startToCloseTimeout: "1m",
});

export async function ordersWorkflow(orderId: string): Promise<string> {
  const prepared = await activities.prepareOrder(orderId);
  return prepared;
}
