import { proxyActivities } from '@temporalio/workflow'

const { greet } = proxyActivities<{ greet(name: string): Promise<string> }>({
  startToCloseTimeout: '5 seconds',
})

export async function temporalStringCaseWorkflow(name: string): Promise<string> {
  const loud = name.toUpperCase()
  const soft = name.toLowerCase()
  return await greet(`${loud}:${soft}`)
}
