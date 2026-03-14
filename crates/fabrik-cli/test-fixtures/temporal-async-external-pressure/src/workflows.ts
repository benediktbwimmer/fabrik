import {
  condition,
  defineQuery,
  defineSignal,
  getExternalWorkflowHandle,
  proxyActivities,
  setHandler,
} from "@temporalio/workflow";
import type { createPressureActivities } from "./activities";

const ingestSignal = defineSignal<[string]>("ingest");
const releaseSignal = defineSignal<[boolean]>("release");
const targetStatusQuery = defineQuery<{ phase: string; echoed: string; lastInput: string }>("status");
const controllerStatusQuery = defineQuery<{ phase: string; released: boolean; targetInstanceId: string }>("status");

const { benchmarkEcho } = proxyActivities<ReturnType<typeof createPressureActivities>>({
  startToCloseTimeout: "5m",
});

export async function asyncSignalTargetWorkflow(initialValue: string): Promise<string> {
  let phase = "waiting";
  let echoed = initialValue;
  let lastInput = initialValue;

  setHandler(targetStatusQuery, () => ({
    phase,
    echoed,
    lastInput,
  }));

  setHandler(ingestSignal, async (value: string) => {
    phase = "processing";
    lastInput = value;
    echoed = await benchmarkEcho(value);
    phase = "processed";
  });

  await condition(() => false);
  return echoed;
}

export async function externalControllerWorkflow(input: {
  targetInstanceId: string;
  targetRunId: string;
  payload: string;
}): Promise<string> {
  let phase = "starting";
  let released = false;

  setHandler(controllerStatusQuery, () => ({
    phase,
    released,
    targetInstanceId: input.targetInstanceId,
  }));

  setHandler(releaseSignal, () => {
    released = true;
    phase = "released";
  });

  const handle = getExternalWorkflowHandle(input.targetInstanceId, input.targetRunId);
  await handle.signal(ingestSignal, input.payload);
  phase = "signalled";

  await condition(() => released);
  phase = "cancelling";
  await handle.cancel("async-external-pressure-stop");
  phase = "completed";
  return phase;
}
