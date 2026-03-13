export async function temporalExpressionBuiltinsWorkflow(records: Record<string, string>): Promise<string> {
  const now = Date.now();
  const keys = Object.keys(records);
  return `${now}:${keys.join("\n")}`;
}
