import { proxyActivities } from "@temporalio/workflow";

const activities = proxyActivities<typeof import("@pressure/reports-activities")>({
  startToCloseTimeout: "1m",
});

export async function reportsWorkflow(reportId: string): Promise<string> {
  const built = await activities.buildReport(reportId);
  return built;
}
