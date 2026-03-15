import { streamJob } from "../stream-authoring.ts";

export const keyedRollupSignalStreamJob = streamJob({
  name: "keyed-rollup",
  runtime: "keyed_rollup",
  source: {
    kind: "bounded_input",
  },
  keyBy: "accountId",
  operators: [
    {
      kind: "reduce",
      name: "sum-account-totals",
      config: {
        reducer: "sum",
        valueField: "amount",
        outputField: "totalAmount",
      },
    },
    {
      kind: "emit_checkpoint",
      name: "hourly-rollup-ready",
      config: {
        sequence: 1,
      },
    },
    {
      kind: "signal_workflow",
      name: "notify-account-rollup",
      config: {
        view: "accountTotals",
        signalType: "account.rollup.ready",
        whenOutputField: "totalAmount",
      },
    },
  ],
  checkpointPolicy: {
    kind: "named_checkpoints",
    checkpoints: [
      {
        name: "hourly-rollup-ready",
        delivery: "workflow_awaitable",
        sequence: 1,
      },
    ],
  },
  views: [
    {
      name: "accountTotals",
      consistency: "strong",
      queryMode: "by_key",
      keyField: "accountId",
      valueFields: ["accountId", "totalAmount", "asOfCheckpoint"],
      supportedConsistencies: ["strong"],
    },
  ],
  queries: [
    {
      name: "accountTotals",
      viewName: "accountTotals",
      consistency: "strong",
      argFields: ["key"],
    },
  ],
});
