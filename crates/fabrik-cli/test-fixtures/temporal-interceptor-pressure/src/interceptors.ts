import { WorkflowInterceptors } from "@temporalio/workflow";

export const interceptors = (): WorkflowInterceptors => ({
  internals: [
    {
      activate(input, next) {
        return next(input);
      },
      concludeActivation({ commands }, next) {
        return next({ commands });
      },
    },
  ],
  inbound: [
    {
      async execute(input, next) {
        return next(input);
      },
    },
  ],
  outbound: [],
});
