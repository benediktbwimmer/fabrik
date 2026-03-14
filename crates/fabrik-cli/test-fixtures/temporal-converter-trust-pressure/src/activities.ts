export async function normalizeOrder(input: { id: string; tags: string[] }) {
  return {
    ...input,
    tags: input.tags.map((tag) => tag.toUpperCase()),
  };
}
