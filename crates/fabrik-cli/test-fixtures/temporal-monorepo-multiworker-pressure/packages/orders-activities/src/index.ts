export async function prepareOrder(orderId: string): Promise<string> {
  return `prepared:${orderId}`;
}
