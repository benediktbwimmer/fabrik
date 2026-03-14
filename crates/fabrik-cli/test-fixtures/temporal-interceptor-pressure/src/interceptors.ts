import {
  WorkflowInterceptors,
  defineQuery,
  setHandler,
} from "@temporalio/workflow";

interface VersionedValue {
  version: number;
  value: string;
}

const state = {
  version: 0,
  current: { value: "" },
  draft: { value: "" },
};

export function trackedValue(initialValue: string): { value: string } {
  state.current.value = initialValue;
  state.draft.value = initialValue;
  setHandler(defineQuery<VersionedValue>("trackedValue"), () => ({
    version: state.version,
    value: state.current.value,
  }));
  const proxy = {};
  Object.defineProperty(proxy, "value", {
    get: () => state.draft.value,
    set: (value: unknown) => {
      state.draft.value = String(value);
    },
  });
  return proxy as { value: string };
}

export const interceptors = (): WorkflowInterceptors => ({
  internals: [
    {
      activate(input, next) {
        if (input.batchIndex === 0) {
          state.draft.value = state.current.value;
        }
        return next(input);
      },
      concludeActivation({ commands }, next) {
        if (state.current.value !== state.draft.value) {
          state.version += 1;
          state.current.value = state.draft.value;
        }
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
