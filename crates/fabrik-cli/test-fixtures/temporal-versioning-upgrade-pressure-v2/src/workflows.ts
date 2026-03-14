import {
  condition,
  defineSignal,
  patched,
  proxyActivities,
  setHandler,
  setWorkflowOptions,
} from "@temporalio/workflow";

const proceedSignal = defineSignal<[boolean]>("proceed");
const activities = proxyActivities<typeof import("./activities")>({
  startToCloseTimeout: "1m",
});

export const versioningPressureWorkflow = setWorkflowOptions(
  { versioningBehavior: "AUTO_UPGRADE" },
  async function versioningPressureWorkflow(name: string): Promise<string> {
    let proceed = false;
    setHandler(proceedSignal, (value: boolean) => {
      proceed = value;
    });
    await condition(() => proceed);
    const label = patched("versioning-pressure-greeting-v2")
      ? `${name}:v2`
      : `${name}:v1-compat`;
    return `${await activities.renderGreeting(label)}:build-v2`;
  },
);
