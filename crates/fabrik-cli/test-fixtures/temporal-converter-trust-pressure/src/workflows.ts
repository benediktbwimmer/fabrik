import { proxyActivities } from "@temporalio/workflow";

const activities = proxyActivities<typeof import("./activities")>({
  startToCloseTimeout: "1m",
});

export async function converterPressureWorkflow(input: {
  id: string;
  tags: string[];
}): Promise<string> {
  const normalized = await activities.normalizeOrder(input);
  return `${normalized.id}:${normalized.tags.join(",")}`;
}
