import { defineSignal, proxyActivities, setHandler } from "@temporalio/workflow";

const { benchmarkEcho } = proxyActivities({});
const approved = defineSignal<[string]>("approved");

export async function temporalAsyncSignalWorkflow() {
  let echoed = "";

  setHandler(approved, async (value) => {
    echoed = await benchmarkEcho(value);
  });

  await benchmarkEcho("ready");
  return echoed;
}
