import { Worker } from "@temporalio/worker";
import * as activities from "./activities";

const taskQueue = process.env.TEMPORAL_TASK_QUEUE;

async function main(): Promise<void> {
  const worker = await Worker.create({
    workflowsPath: require.resolve("./workflows"),
    activities,
    taskQueue,
  });
  await worker.run();
}

void main();
