import { Worker } from "@temporalio/worker";

async function runTest(): Promise<void> {
  const options = {
    taskQueue: "ignored-test-worker",
    workflowsPath: require.resolve("./workflows"),
  };
  const worker = await Worker.create(options);
  await worker.run();
}

void runTest();
