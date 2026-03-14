import { Worker } from "@temporalio/worker";
import * as activities from "./activities";

async function run(): Promise<void> {
  const worker = await Worker.create({
    taskQueue: "test-worker-qualified",
    workflowsPath: require.resolve("./workflows"),
    activities,
  });
  await worker.run();
}

void run();
