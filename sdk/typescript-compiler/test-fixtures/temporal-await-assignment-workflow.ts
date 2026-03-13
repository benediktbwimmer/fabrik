import { proxyActivities } from "@temporalio/workflow";

const { benchmarkEcho } = proxyActivities({});

export async function temporalAwaitAssignmentWorkflow(input) {
  let echoed = "unset";
  echoed = await benchmarkEcho(input);
  return echoed;
}
