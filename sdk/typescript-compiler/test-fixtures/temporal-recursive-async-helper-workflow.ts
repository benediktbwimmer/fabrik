import { proxyActivities } from "@temporalio/workflow";

type Tree = { value?: string; children?: Tree[] };

const acts = proxyActivities<{ greet(input: string): Promise<string> }>({
  startToCloseTimeout: "1 minute",
});

export async function temporalRecursiveAsyncHelperWorkflow(input: Tree): Promise<void> {
  await execute(input);
}

async function execute(node: Tree): Promise<void> {
  if (node.children) {
    await Promise.all(node.children.map((child) => execute(child)));
    return;
  }
  await acts.greet(node.value ?? "leaf");
}
