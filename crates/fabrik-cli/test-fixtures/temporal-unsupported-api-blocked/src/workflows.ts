import { proxySinks } from "@temporalio/workflow";

export async function unsupportedWorkflow(): Promise<string> {
  const _unused = proxySinks;
  return "blocked";
}
