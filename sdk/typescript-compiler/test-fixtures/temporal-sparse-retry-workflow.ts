import { proxyActivities } from '@temporalio/workflow'

const { greet } = proxyActivities<{ greet(name: string): Promise<string> }>({
  startToCloseTimeout: '30 seconds',
  retry: {
    maximumInterval: '5 seconds',
  },
})

export async function temporalSparseRetryWorkflow(name: string): Promise<string> {
  return await greet(name)
}
