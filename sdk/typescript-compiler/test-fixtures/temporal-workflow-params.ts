import { proxyActivities } from "@temporalio/workflow";

const { greet } = proxyActivities({});

export async function temporalWorkflowParamsWorkflow(name, punctuation = "!") {
  return await greet(name, punctuation);
}
