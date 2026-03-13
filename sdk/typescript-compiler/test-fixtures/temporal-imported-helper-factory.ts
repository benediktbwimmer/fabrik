export function createActivities(client: { greet(name: string): Promise<string> }) {
  return {
    greet: client.greet.bind(client),
  };
}
