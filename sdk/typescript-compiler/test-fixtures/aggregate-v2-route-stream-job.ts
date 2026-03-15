import { streamJob } from "../stream-authoring.ts";

export const fraudRouteStreamJob = streamJob({
  name: "fraud-route",
  runtime: "aggregate_v2",
  source: {
    kind: "bounded_input",
  },
  keyBy: "accountId",
  states: [
    {
      id: "high-risk-state",
      kind: "keyed",
      keyFields: ["accountId"],
      valueFields: ["highRiskCount"],
      retentionSeconds: 3600,
      config: {
        reducer: "count",
      },
    },
  ],
  operators: [
    {
      kind: "route",
      operatorId: "bucket-risk",
      config: {
        outputField: "riskBucket",
        branches: [
          { predicate: "risk >= 0.8", value: "high" },
          { predicate: "risk >= 0.5", value: "medium" },
        ],
        defaultValue: "low",
      },
    },
    {
      kind: "filter",
      operatorId: "keep-high",
      config: {
        predicate: "riskBucket == \"high\"",
      },
    },
    {
      kind: "aggregate",
      operatorId: "count-high",
      stateIds: ["high-risk-state"],
      config: {
        reducer: "count",
        outputField: "highRiskCount",
      },
    },
    {
      kind: "materialize",
      operatorId: "materialize-high",
      stateIds: ["high-risk-state"],
      config: {
        view: "highRiskCounts",
      },
    },
  ],
  views: [
    {
      name: "highRiskCounts",
      consistency: "strong",
      queryMode: "by_key",
      keyField: "accountId",
      valueFields: ["accountId", "highRiskCount"],
      supportedConsistencies: ["strong"],
      retentionSeconds: 3600,
    },
  ],
  queries: [
    {
      name: "highRiskCountsByKey",
      viewName: "highRiskCounts",
      consistency: "strong",
      argFields: ["accountId"],
    },
  ],
});
