import { ApplicationFailure } from '@temporalio/workflow'

export async function temporalApplicationFailureShorthandWorkflow(): Promise<void> {
  const message = 'broken'
  throw ApplicationFailure.create({ message })
}
