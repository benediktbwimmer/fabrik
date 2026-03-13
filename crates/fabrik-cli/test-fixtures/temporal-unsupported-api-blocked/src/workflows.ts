import { patched } from "@temporalio/workflow";

export async function unsupportedWorkflow(): Promise<string> {
  patched("feature-x");
  return "blocked";
}
