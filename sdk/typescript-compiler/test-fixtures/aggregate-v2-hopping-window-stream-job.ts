import { streamJob } from "../stream-authoring.ts";

export const fraudHoppingWindowStreamJob = streamJob({
  name: "fraud-hopping-window",
  runtime: "aggregate_v2",
  source: {
    kind: "bounded_input",
  },
  keyBy: "accountId",
  states: [
    {
      id: "rolling-window",
      kind: "window",
      keyFields: ["accountId", "windowStart"],
      valueFields: ["avgRisk"],
      retentionSeconds: 3600,
      config: {
        mode: "hopping",
        size: "2m",
        hop: "1m",
      },
    },
    {
      id: "risk-state",
      kind: "keyed",
      keyFields: ["accountId", "windowStart"],
      valueFields: ["avgRisk"],
      retentionSeconds: 3600,
      config: {
        reducer: "avg",
      },
    },
  ],
  operators: [
    {
      kind: "window",
      operatorId: "rolling-window",
      stateIds: ["rolling-window"],
      config: {
        mode: "hopping",
        size: "2m",
        hop: "1m",
        timeField: "eventTime",
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
      valueFields: ["accountId", "avgRisk", "windowStart", "windowEnd"],
      supportedConsistencies: ["strong", "eventual"],
      retentionSeconds: 3600,
    },
  ],
  queries: [
    {
      name: "riskScoresByKey",
      viewName: "riskScores",
      consistency: "strong",
      argFields: ["accountId", "windowStart"],
    },
  ],
});
