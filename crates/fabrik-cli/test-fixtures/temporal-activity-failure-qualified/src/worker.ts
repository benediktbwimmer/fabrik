import { Worker } from "@temporalio/worker";
import * as activities from "./activities";

declare const require: {
  resolve(specifier: string): string;
};

export async function createWorker() {
  return Worker.create({
    taskQueue: "activity-failure-qualified",
    workflowsPath: require.resolve("./workflows"),
    activities,
  });
}
