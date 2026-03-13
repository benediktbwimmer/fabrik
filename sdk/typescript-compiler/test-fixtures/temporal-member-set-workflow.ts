export async function temporalMemberSetWorkflow(): Promise<number> {
  const state = { count: 1 };
  state.count = state.count + 2;
  return state.count;
}
