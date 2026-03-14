export async function buildReport(reportId: string): Promise<string> {
  return `report:${reportId}`;
}
