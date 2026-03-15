import { streamJob } from "../stream-authoring.ts";

export const fraudDetectorStreamJob = streamJob({
  name: "fraud-detector",
  runtime: "aggregate_v2",
  source: {
    kind: "topic",
    name: "payments",
    binding: "payments",
    config: {
      topic: "payments",
    },
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
    {
      id: "minute-window",
      kind: "window",
      keyFields: ["accountId", "windowStart"],
      valueFields: ["avgRisk"],
      retentionSeconds: 3600,
      config: {
        mode: "tumbling",
        size: "1m",
      },
    },
  ],
  operators: [
    {
      kind: "filter",
      operatorId: "filter-valid",
      name: "filter-valid",
      inputs: ["source:payments"],
      outputs: ["filtered"],
      config: {
        predicate: "amount > 0",
      },
    },
    {
      kind: "window",
      operatorId: "minute-window",
      name: "minute-window",
      inputs: ["filtered"],
      outputs: ["windowed"],
      stateIds: ["minute-window"],
      config: {
        mode: "tumbling",
        size: "1m",
      },
    },
    {
      kind: "aggregate",
      operatorId: "avg-risk",
      name: "avg-risk",
      inputs: ["windowed"],
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
    {
      kind: "emit_checkpoint",
      operatorId: "minute-closed",
      name: "minute-closed",
      inputs: ["riskScores"],
      outputs: ["checkpoint"],
      config: {
        sequence: 1,
      },
    },
  ],
  checkpointPolicy: {
    kind: "named_checkpoints",
    checkpoints: [
      {
        name: "minute-closed",
        delivery: "workflow_awaitable",
        sequence: 1,
      },
    ],
  },
  views: [
    {
      name: "riskScores",
      viewId: "risk-scores",
      consistency: "strong",
      queryMode: "by_key",
      keyField: "accountId",
      valueFields: ["accountId", "avgRisk"],
      supportedConsistencies: ["strong", "eventual"],
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
    {
      name: "riskScoresScan",
      queryId: "risk-scores-scan",
      viewName: "riskScores",
      consistency: "eventual",
      argFields: ["prefix"],
    },
  ],
  classification: "fast_lane",
  metadata: {
    kernel: "aggregate_v2",
  },
});
