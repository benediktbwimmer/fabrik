import { invalidHelper } from "./invalid-helper.ts";

export async function invalidHelperWorkflow(ctx, input) {
  const updated = invalidHelper(input.value);
  return ctx.complete({ updated });
}
