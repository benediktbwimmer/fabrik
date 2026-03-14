import {
  condition,
  defineQuery,
  defineSignal,
  proxyActivities,
  setHandler,
} from "@temporalio/workflow";

const activities = proxyActivities<typeof import("@pressure/reports-activities")>({
  startToCloseTimeout: "1m",
});
const releaseSignal = defineSignal<[boolean]>("release");
const statusQuery = defineQuery<{ built: string | null; released: boolean }>("status");

export async function reportsWorkflow(reportId: string): Promise<string> {
  const built = await activities.buildReport(reportId);
  let released = false;
  setHandler(releaseSignal, () => {
    released = true;
  });
  setHandler(statusQuery, () => ({
    built,
    released,
  }));
  await condition(() => released);
  return built;
}
