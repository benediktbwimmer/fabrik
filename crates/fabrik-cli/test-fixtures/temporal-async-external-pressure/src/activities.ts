export function createPressureActivities(prefix: string) {
  return {
    async benchmarkEcho(value: string): Promise<string> {
      return `${prefix}:${value}`;
    },
  };
}
