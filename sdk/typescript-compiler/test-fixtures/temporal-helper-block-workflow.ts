function divideIntoPartitions(total: number, n: number): number[] {
  const base = Math.floor(total / n);
  const remainder = total % n;
  const partitions = new Array<number>(n).fill(base);
  for (let i = 0; i < remainder; i++) {
    partitions[i] += 1;
  }
  return partitions;
}

export async function temporalHelperBlockWorkflow(total: number, n: number): Promise<number[]> {
  return divideIntoPartitions(total, n);
}
