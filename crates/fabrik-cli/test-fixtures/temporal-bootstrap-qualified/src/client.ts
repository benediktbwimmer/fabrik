import { Client } from "@temporalio/client";
import { taskQueue } from "./shared";
import { greetingWorkflow } from "./workflows";

export async function startGreeting(client: Client, name: string) {
  return client.workflow.start(greetingWorkflow, {
    taskQueue,
    workflowId: `bootstrap-qualified-${name}`,
    args: [name],
  });
}
