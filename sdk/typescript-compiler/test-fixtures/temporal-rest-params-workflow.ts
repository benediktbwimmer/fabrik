export async function temporalRestParamsWorkflow(prefix: string, ...names: string[]): Promise<string[]> {
  return names.map((name) => `${prefix}:${name}`);
}
