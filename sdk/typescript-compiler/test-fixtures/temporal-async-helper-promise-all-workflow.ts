import { sleep } from "@temporalio/workflow";

export async function temporalAsyncHelperPromiseAllWorkflow(items: string[]): Promise<void> {
  await Promise.all(items.map((item) => execute(item)));
}

async function execute(item: string): Promise<void> {
  await sleep("1s");
  if (item === "nested") {
    await execute("done");
  }
}
