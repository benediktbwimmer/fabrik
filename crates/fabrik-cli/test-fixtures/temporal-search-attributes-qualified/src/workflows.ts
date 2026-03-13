import { SearchAttributes, upsertSearchAttributes, workflowInfo } from "@temporalio/workflow";

export async function searchAttributesWorkflow(): Promise<SearchAttributes> {
  const customInt = (workflowInfo().searchAttributes.CustomIntField?.[0] as number) || 0;
  upsertSearchAttributes({
    CustomIntField: [customInt + 1],
    CustomBoolField: [],
    CustomDoubleField: [3.14],
  });
  return workflowInfo().searchAttributes;
}
