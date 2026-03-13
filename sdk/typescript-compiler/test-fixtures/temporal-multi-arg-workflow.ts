import { continueAsNew, proxyActivities } from "@temporalio/workflow";

const { joinValues } = proxyActivities({});

export async function temporalMultiArgWorkflow(input) {
  return await joinValues(input.left, input.right, 3);
}

export async function temporalContinueAsNewArgsWorkflow(input) {
  return continueAsNew(input.id, false, {
    attempt: input.attempt,
  });
}
