import { Worker } from "@temporalio/worker";

declare const require: {
  resolve(specifier: string): string;
};

export async function createWorker() {
  return Worker.create({
    taskQueue: "search-attributes-qualified",
    workflowsPath: require.resolve("./workflows"),
  });
}
