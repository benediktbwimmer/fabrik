import {
  condition,
  defineQuery,
  defineSignal,
  proxyActivities,
  setHandler,
} from "@temporalio/workflow";

const activities = proxyActivities<typeof import("@pressure/orders-activities")>({
  startToCloseTimeout: "1m",
});
const releaseSignal = defineSignal<[boolean]>("release");
const statusQuery = defineQuery<{ prepared: string | null; released: boolean }>("status");

export async function ordersWorkflow(orderId: string): Promise<string> {
  const prepared = await activities.prepareOrder(orderId);
  let released = false;
  setHandler(releaseSignal, () => {
    released = true;
  });
  setHandler(statusQuery, () => ({
    prepared,
    released,
  }));
  await condition(() => released);
  return prepared;
}
