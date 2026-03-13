export async function ping(name: string): Promise<string> {
  return `pong:${name}`;
}
