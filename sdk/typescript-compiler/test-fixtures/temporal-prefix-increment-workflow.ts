export async function temporalPrefixIncrementWorkflow(maxAttempts = 3): Promise<number> {
  let attempt = 0;
  for (; attempt < maxAttempts; ++attempt) {
    if (attempt === maxAttempts - 1) {
      return attempt;
    }
  }
  return attempt;
}
