import { ActivityFailure, ApplicationFailure, log, proxyActivities } from "@temporalio/workflow";
import type * as activities from "./activities";

const { createAccount, compensate } = proxyActivities<typeof activities>({
  startToCloseTimeout: "1 minute",
});

export async function activityFailureWorkflow(): Promise<void> {
  try {
    await createAccount();
  } catch (err) {
    if (err instanceof ActivityFailure && err.cause instanceof ApplicationFailure) {
      log.error(err.cause.message);
    } else {
      log.error(`unexpected: ${err}`);
    }
    await compensate();
    throw err;
  }
}
