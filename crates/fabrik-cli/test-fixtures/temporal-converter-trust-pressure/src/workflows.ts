import {
  condition,
  defineQuery,
  defineSignal,
  proxyActivities,
  setHandler,
} from "@temporalio/workflow";

const activities = proxyActivities<typeof import("./activities")>({
  startToCloseTimeout: "1m",
});
const normalizeSignal = defineSignal<[boolean]>("normalize");
const statusQuery = defineQuery<{ phase: string; id: string; tags: string[] }>("status");

export async function converterPressureWorkflow(input: {
  id: string;
  tags: string[];
}): Promise<string> {
  let phase = "waiting";
  let shouldNormalize = false;
  let current: { id: string; tags: string[] } | null = null;
  setHandler(normalizeSignal, () => {
    shouldNormalize = true;
  });
  setHandler(statusQuery, () => ({
    phase,
    id: current ? current.id : input.id,
    tags: current ? current.tags : input.tags,
  }));
  await condition(() => shouldNormalize);
  phase = "normalizing";
  const normalized = await activities.normalizeOrder(input);
  current = normalized;
  phase = "completed";
  return `${normalized.id}:${normalized.tags.join(",")}`;
}
