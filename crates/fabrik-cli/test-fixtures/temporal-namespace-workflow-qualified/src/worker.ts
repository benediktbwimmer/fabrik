import { Worker } from "@temporalio/worker";

async function run(): Promise<void> {
  const worker = await Worker.create({
    taskQueue: "namespace-workflow-qualified",
    workflowsPath: require.resolve("./workflows"),
  });
  await worker.run();
}

void run();
