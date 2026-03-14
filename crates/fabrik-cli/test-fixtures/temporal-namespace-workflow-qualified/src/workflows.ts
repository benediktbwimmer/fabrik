import * as workflow from "@temporalio/workflow";

export async function namespaceWorkflow(): Promise<string> {
  workflow.log.info("namespace workflow started");
  await workflow.sleep("1 second");
  return "done";
}
