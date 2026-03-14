export function createAuditActivities(channel: string) {
  return {
    async publishAudit(version: number, value: string): Promise<string> {
      return `${channel}:${version}:${value}`;
    },
  };
}
