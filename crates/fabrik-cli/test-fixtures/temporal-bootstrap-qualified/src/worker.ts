import { Worker, type WorkerOptions } from "@temporalio/worker";
import * as activities from "./activities";
import { taskQueue } from "./shared";

declare const require: {
  resolve(specifier: string): string;
};

const workerOptions: WorkerOptions = {
  taskQueue,
  workflowsPath: require.resolve("./workflows"),
  activities,
};

export async function createWorker() {
  return Worker.create(workerOptions);
}
