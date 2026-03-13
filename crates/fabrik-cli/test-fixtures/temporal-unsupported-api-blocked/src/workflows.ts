import { SearchAttributes } from "@temporalio/workflow";

export async function unsupportedWorkflow(): Promise<string> {
  const _unused = SearchAttributes;
  return "blocked";
}
