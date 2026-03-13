import { proxyActivities } from "@temporalio/workflow";
import { createActivities } from "./temporal-imported-helper-factory";

const { greet } = proxyActivities<ReturnType<typeof createActivities>>({
  startToCloseTimeout: "5s",
});

export async function temporalImportedHelperTypeWorkflow(name: string): Promise<string> {
  return await greet(name);
}
