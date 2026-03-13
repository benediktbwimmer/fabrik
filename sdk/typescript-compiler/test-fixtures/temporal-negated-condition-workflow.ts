import { condition } from '@temporalio/workflow'

let ready = false

export async function temporalNegatedConditionWorkflow(): Promise<boolean> {
  const timedOut = !(await condition(() => ready, '1 minute'))
  return timedOut
}
