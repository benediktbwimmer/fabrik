function doSomething(callback: () => void) {
  setTimeout(callback, 10);
}

export async function temporalCallbackPromiseWorkflow(): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    doSomething(resolve);
    Promise.resolve().catch(reject);
  });
}
