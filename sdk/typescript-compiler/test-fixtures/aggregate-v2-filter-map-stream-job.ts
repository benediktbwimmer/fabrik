import { streamJob } from "../stream-authoring.ts";

export const fraudFilterMapStreamJob = streamJob({
  name: "fraud-filter-map",
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
      kind: "filter",
      operatorId: "filter-positive",
      config: {
        predicate: "amount > 0",
      },
    },
    {
      kind: "map",
      operatorId: "normalize-risk",
      config: {
        inputField: "riskPoints",
        outputField: "risk",
        multiplyBy: 0.01,
      },
    },
    {
      kind: "aggregate",
      operatorId: "avg-risk",
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
      stateIds: ["risk-state"],
      config: {
        view: "riskScores",
      },
    },
  ],
  views: [
    {
      name: "riskScores",
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
      viewName: "riskScores",
      consistency: "strong",
      argFields: ["accountId"],
    },
  ],
});
