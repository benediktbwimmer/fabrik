import { continueAsNew, proxyActivities, sleep } from "@temporalio/workflow";

const { benchmarkEcho } = proxyActivities({
  startToCloseTimeout: "30s",
  retry: {
    maximumAttempts: 3,
    initialInterval: "5s",
    nonRetryableErrorTypes: ["ValidationError", "FatalError"],
  },
});

const activities = proxyActivities({});

export async function temporalCompatibleWorkflow(input) {
  await sleep("2s");
  const echoed = await benchmarkEcho(input);
  return {
    echoed,
    original: input,
  };
}

export async function temporalContinueAsNewWorkflow(input) {
  if (input.retry) {
    return continueAsNew({
      id: input.id,
      retry: false,
    });
  }
  return await activities.benchmarkEcho(input);
}
