import {
  condition,
  continueAsNew,
  defineSignal,
  defineUpdate,
  proxyActivities,
  setHandler,
  sleep,
  startChild,
  workflowInfo,
} from "@temporalio/workflow";

const { benchmarkEcho } = proxyActivities({
  startToCloseTimeout: "30s",
  retry: {
    initialInterval: "1s",
    maximumAttempts: 2,
    nonRetryableErrorTypes: ["BenchmarkCancelled"],
  },
});

const approveSignal = defineSignal("approve");
const setValueUpdate = defineUpdate("setValue");

export async function fanoutBenchmarkWorkflow(input) {
  return await comparisonBenchmarkWorkflow({ ...input, kind: "fanout" });
}

export async function comparisonBenchmarkWorkflow(input) {
  let signalReady = input.kind !== "signal_gate";
  let updateReady = input.kind !== "update_gate";

  setHandler(approveSignal, () => {
    signalReady = true;
  });
  setHandler(setValueUpdate, async (value) => {
    updateReady = true;
    return value;
  });

  switch (input.kind) {
    case "timer_gate":
      await sleep(`${input.timerSecs ?? 1}s`);
      break;
    case "signal_gate":
      await condition(() => signalReady);
      break;
    case "update_gate":
      await condition(() => updateReady);
      break;
    case "continue_as_new":
      if ((input.remaining ?? 0) > 0) {
        return continueAsNew({
          ...input,
          remaining: input.remaining - 1,
        });
      }
      break;
    case "child_workflow": {
      const handle = await startChild(benchmarkChildWorkflow, {
        args: [{ ok: true }],
        workflowId: input.childWorkflowId ?? `${workflowInfo().workflowId}/child`,
      });
      await handle.result();
      break;
    }
    default:
      break;
  }

  const results = await Promise.allSettled(input.items.map((item) => benchmarkEcho(item)));
  const firstRejected = results.find((result) => result.status === "rejected");
  if (firstRejected) {
    throw firstRejected.reason;
  }
  return results.map((result) => result.value);
}

export async function benchmarkChildWorkflow(input) {
  return input;
}
