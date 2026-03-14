import { proxyActivities } from "@temporalio/workflow";

const { enrich } = proxyActivities<{
  enrich(item: { value: number }): Promise<number>;
}>({
  capabilities: { payloadlessTransport: true },
});

export async function temporalFanoutCapabilitiesWorkflow(
  items: { value: number }[],
): Promise<number[]> {
  return await Promise.all(items.map((item) => enrich(item)));
}
