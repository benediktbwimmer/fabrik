export async function temporalExpressionBuiltinsWorkflow(records: Record<string, string>): Promise<string> {
  const now = Date.now();
  const clipped = Math.min(Object.keys(records).length, 2);
  const keys = Object.keys(records).map(Number).sort((a, b) => a - b);
  return `${now}:${clipped}:${keys.join("\n")}`;
}
