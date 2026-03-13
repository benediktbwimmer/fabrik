import { proxyActivities } from "@temporalio/workflow";

const { greet } = proxyActivities<{
  greet(name: string): Promise<string>;
}>({
  startToCloseTimeout: "5s",
});

async function greetTwice(name: string): Promise<string> {
  const first = await greet(name);
  return `${first}!`;
}

export async function temporalAsyncHelperWorkflow(name: string): Promise<string> {
  return await greetTwice(name);
}
