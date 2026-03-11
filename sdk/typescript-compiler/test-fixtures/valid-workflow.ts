import { shouldEscalate } from "./valid-helper.ts";

export async function validWorkflow(ctx, input) {
  const signal = await ctx.waitForSignal("started");
  let total = 0;

  for (const item of signal.items) {
    total = total + item;
  }

  if (shouldEscalate(total)) {
    return ctx.complete({ total, level: "high" });
  }

  return ctx.complete({ total, level: "normal" });
}
