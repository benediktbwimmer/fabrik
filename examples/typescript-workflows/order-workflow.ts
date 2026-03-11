import { shouldContinue } from "./helpers.ts";

export async function orderWorkflow(ctx, input) {
  const signal = await ctx.waitForSignal("external.approved");
  const items = signal.items;
  let index = 0;

  while (index < items.length) {
    try {
      const response = await ctx.activity("core.echo", items[index]);
      if (!shouldContinue(response)) {
        return ctx.fail("processing_stopped");
      }
    } catch (error) {
      return ctx.continueAsNew({
        items,
        recovered: true,
        error,
      });
    }

    index = index + 1;
  }

  return ctx.complete({
    processed: index,
    originalInput: input,
  });
}
