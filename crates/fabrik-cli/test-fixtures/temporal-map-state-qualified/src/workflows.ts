import { CancellationScope, defineQuery, defineSignal, setHandler } from "@temporalio/workflow";

const setValueSignal = defineSignal<[string, number]>("setValue");
const getValueQuery = defineQuery<number | undefined, [string]>("getValue");

export async function mapStateWorkflow(): Promise<void> {
  const state = new Map<string, number>();
  setHandler(setValueSignal, (key, value) => void state.set(key, value));
  setHandler(getValueQuery, (key) => state.get(key));
  await CancellationScope.current().cancelRequested;
}
