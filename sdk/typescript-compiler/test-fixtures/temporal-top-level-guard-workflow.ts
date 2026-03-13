import { inWorkflowContext, proxyActivities } from '@temporalio/workflow'

const activities = {
  async greet(name: string): Promise<string> {
    return `hello ${name}`
  },
}

const { greet } = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 seconds',
})

export async function temporalTopLevelGuardWorkflow(name: string): Promise<string> {
  return await greet(name)
}

if (!inWorkflowContext()) {
  console.log('bootstrapping outside workflow context')
}
