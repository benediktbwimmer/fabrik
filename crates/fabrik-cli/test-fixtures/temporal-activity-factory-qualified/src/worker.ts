import { Worker } from "@temporalio/worker";
import { createActivities } from "./activities";

const db = {
  async get(_key: string) {
    return "Temporal";
  },
};

async function run(): Promise<void> {
  const worker = await Worker.create({
    taskQueue: "activity-factory-qualified",
    workflowsPath: require.resolve("./workflows"),
    activities: createActivities(db),
  });
  await worker.run();
}

void run();
