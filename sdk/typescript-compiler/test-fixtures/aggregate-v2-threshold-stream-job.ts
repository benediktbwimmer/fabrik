import { streamJob } from "../stream-authoring.ts";

export const fraudThresholdStreamJob = streamJob({
  name: "fraud-threshold",
  runtime: "aggregate_v2",
  source: {
    kind: "bounded_input",
  },
  keyBy: "accountId",
  states: [
    {
      id: "risk-threshold-state",
      kind: "keyed",
      keyFields: ["accountId"],
      valueFields: ["riskExceeded"],
      retentionSeconds: 3600,
      config: {
        reducer: "threshold",
      },
    },
  ],
  operators: [
    {
      kind: "aggregate",
      operatorId: "risk-threshold",
      name: "risk-threshold",
      inputs: ["source"],
      outputs: ["risk-view"],
      stateIds: ["risk-threshold-state"],
      config: {
        reducer: "threshold",
        valueField: "risk",
        threshold: 0.97,
        comparison: "gte",
        outputField: "riskExceeded",
      },
    },
    {
      kind: "materialize",
      operatorId: "materialize-threshold",
      name: "materialize-threshold",
      inputs: ["risk-view"],
      outputs: ["riskThresholds"],
      stateIds: ["risk-threshold-state"],
      config: {
        view: "riskThresholds",
      },
    },
  ],
  views: [
    {
      name: "riskThresholds",
      viewId: "risk-thresholds",
      consistency: "strong",
      queryMode: "by_key",
      keyField: "accountId",
      valueFields: ["accountId", "riskExceeded"],
      supportedConsistencies: ["strong"],
      retentionSeconds: 3600,
    },
  ],
  queries: [
    {
      name: "riskThresholdsByKey",
      queryId: "risk-thresholds-by-key",
      viewName: "riskThresholds",
      consistency: "strong",
      argFields: ["accountId"],
    },
  ],
  classification: "fast_lane",
});
