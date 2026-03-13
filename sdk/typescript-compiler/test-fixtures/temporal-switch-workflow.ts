import { ApplicationFailure, continueAsNew } from "@temporalio/workflow";

export async function temporalSwitchWorkflow(language: string, remaining = 0): Promise<string> {
  switch (language) {
    case "loop":
      if (remaining > 0) {
        await continueAsNew<typeof temporalSwitchWorkflow>(language, remaining - 1);
      }
      return "done";
    case "en":
      return "hello";
    case "fr":
      return "bonjour";
    default:
      throw new ApplicationFailure(`Unsupported language: ${language}`);
  }
}
