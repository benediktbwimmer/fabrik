import { Worker } from '@temporalio/worker'
import * as activities from '@demo/activities'
import { taskQueue } from '@demo/common/lib/temporal-connection'

async function run() {
  const worker = await Worker.create({
    workflowsPath: require.resolve('../../../packages/workflows'),
    activities,
    taskQueue,
  })
  await worker.run()
}

void run()
