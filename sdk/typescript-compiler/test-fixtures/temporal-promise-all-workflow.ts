import { proxyActivities } from "@temporalio/workflow";

const { benchmarkEcho } = proxyActivities({});

export async function temporalPromiseAllWorkflow(input) {
  const results = await Promise.all(input.items.map((item) => benchmarkEcho(item)));
  return results;
}

export async function temporalPromiseAllReturnWorkflow(input) {
  return await Promise.all(input.items.map((item) => benchmarkEcho(item)));
}
