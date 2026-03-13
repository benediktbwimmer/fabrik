import { Client } from "@temporalio/client";
import { esmWorkflow } from "./workflows";

export async function startEsmWorkflow(client: Client, name: string) {
  return client.workflow.start(esmWorkflow, {
    taskQueue: "bootstrap-esm-qualified",
    workflowId: `bootstrap-esm-${name}`,
    args: [name],
  });
}
