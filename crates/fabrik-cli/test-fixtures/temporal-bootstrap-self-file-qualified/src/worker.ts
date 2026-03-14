import { inWorkflowContext } from "@temporalio/workflow";
import { Worker, Runtime, DefaultLogger } from "@temporalio/worker";

const taskQueue = "bootstrap-self-file";

const activities = {
  async greet(name: string): Promise<string> {
    return `hello ${name}`;
  },
};

export async function orderWorkflow(name: string): Promise<string> {
  return `hello ${name}`;
}

async function main(): Promise<void> {
  const worker = await Worker.create({
    workflowsPath: __filename,
    activities,
    taskQueue,
  });
  await worker.run();
}

if (!inWorkflowContext()) {
  Runtime.install({ logger: new DefaultLogger("WARN") });
  void main();
}
