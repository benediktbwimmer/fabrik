import { proxyActivities, sleep } from "@temporalio/workflow";

const { processOrder, sendNotificationEmail } = proxyActivities<{
  processOrder(): Promise<void>;
  sendNotificationEmail(): Promise<void>;
}>({
  startToCloseTimeout: "5m",
});

export async function temporalActivityHandleRaceWorkflow(): Promise<string> {
  let processing = true;
  const processOrderPromise = processOrder().then(() => {
    processing = false;
  });

  await Promise.race([processOrderPromise, sleep("30s")]);

  if (processing) {
    await sendNotificationEmail();
    await processOrderPromise;
  }

  return "done";
}
