export async function getDataConverter() {
  return {
    payloadCodecs: [await createCodec()],
  };
}

async function createCodec() {
  return {
    async encode(payloads: unknown[]) {
      return payloads;
    },
    async decode(payloads: unknown[]) {
      return payloads;
    },
  };
}
