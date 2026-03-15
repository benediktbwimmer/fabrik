import { streamJob } from "../stream-authoring.ts";

export const fraudMapStreamJob = streamJob({
  name: "fraud-map",
  runtime: "aggregate_v2",
  source: {
    kind: "bounded_input",
  },
  keyBy: "accountId",
  states: [
    {
      id: "risk-state",
      kind: "keyed",
      keyFields: ["accountId"],
      valueFields: ["avgRisk"],
      retentionSeconds: 3600,
      config: {
        reducer: "avg",
      },
    },
  ],
  operators: [
    {
      kind: "map",
      operatorId: "normalize-risk",
      name: "normalize-risk",
      inputs: ["source"],
      outputs: ["normalized"],
      config: {
        inputField: "riskPoints",
        outputField: "risk",
        multiplyBy: 0.01,
      },
    },
    {
      kind: "aggregate",
      operatorId: "avg-risk",
      name: "avg-risk",
      inputs: ["normalized"],
      outputs: ["risk-view"],
      stateIds: ["risk-state"],
      config: {
        reducer: "avg",
        valueField: "risk",
        outputField: "avgRisk",
      },
    },
    {
      kind: "materialize",
      operatorId: "materialize-risk",
      name: "materialize-risk",
      inputs: ["risk-view"],
      outputs: ["riskScores"],
      stateIds: ["risk-state"],
      config: {
        view: "riskScores",
      },
    },
  ],
  views: [
    {
      name: "riskScores",
      viewId: "risk-scores",
      consistency: "strong",
      queryMode: "by_key",
      keyField: "accountId",
      valueFields: ["accountId", "avgRisk"],
      supportedConsistencies: ["strong"],
      retentionSeconds: 3600,
    },
  ],
  queries: [
    {
      name: "riskScoresByKey",
      queryId: "risk-scores-by-key",
      viewName: "riskScores",
      consistency: "strong",
      argFields: ["accountId"],
    },
  ],
  classification: "fast_lane",
});
