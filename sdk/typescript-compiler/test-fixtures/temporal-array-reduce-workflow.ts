export async function temporalArrayReduceWorkflow(values: number[]): Promise<number> {
  return values.reduce((sum, value) => sum + value, 0);
}
