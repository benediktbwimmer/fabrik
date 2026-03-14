import { Worker } from "@temporalio/worker";
import * as activities from "./activities";

async function main() {
  const worker = await Worker.create({
    workflowsPath: "./workflows",
    activities,
    taskQueue: "relaxed-transpile-qualified",
  });
  await worker.run();
}

void main();
