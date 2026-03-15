import {
  type StreamJobQueryDefinition,
  type StreamJobTypes,
  type WorkflowContext,
} from "../workflow-authoring.js";

type TopicKeyedRollupJob = StreamJobTypes<
  { topic: string },
  {
    name: "keyed-rollup";
    runtime: "keyed_rollup";
    source: {
      kind: "topic";
      name: string;
    };
    keyBy: "accountId";
    operators: [
      {
        kind: "reduce";
        name: "sum-account-totals";
        config: {
          reducer: "sum";
          valueField: "amount";
          outputField: "totalAmount";
        };
      },
      {
        kind: "emit_checkpoint";
        name: "hourly-rollup-ready";
        config: {
          sequence: 1;
        };
      },
    ];
    checkpointPolicy: {
      kind: "named_checkpoints";
      checkpoints: [
        {
          name: "hourly-rollup-ready";
          delivery: "workflow_awaitable";
          sequence: 1;
        },
      ];
    };
    viewDefinitions: [
      {
        name: "accountTotals";
        consistency: "strong";
        queryMode: "by_key";
        keyField: "accountId";
        valueFields: ["accountId", "totalAmount", "asOfCheckpoint"];
      },
    ];
    queries: [
      {
        name: "accountTotals";
        viewName: "accountTotals";
        consistency: "strong";
        argFields: ["key"];
      },
    ];
  },
  {
    "hourly-rollup-ready": {
      jobId: string;
      jobName: "keyed-rollup";
      checkpoint: "hourly-rollup-ready";
      sequence: number;
    };
  },
  {
    accountTotals: StreamJobQueryDefinition<
      { key: string },
      {
        accountId: string;
        totalAmount: number;
        asOfCheckpoint: number;
        checkpointSequence: number;
      }
    >;
  },
  {
    jobId: string;
    jobName: "keyed-rollup";
    status: "completed" | "cancelled";
  }
>;

export async function topicStreamJobWorkflow(
  ctx: WorkflowContext,
  input: {
    topic: string;
    accountId: string;
  },
) {
  const job = await ctx.startStreamJob<TopicKeyedRollupJob>("keyed-rollup", {
    input: {
      topic: input.topic,
    },
    config: {
      name: "keyed-rollup",
      runtime: "keyed_rollup",
      source: {
        kind: "topic",
        name: input.topic,
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
      viewDefinitions: [
        {
          name: "accountTotals",
          consistency: "strong",
          queryMode: "by_key",
          keyField: "accountId",
          valueFields: ["accountId", "totalAmount", "asOfCheckpoint"],
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
    },
  });

  const checkpoint = await job.untilCheckpoint("hourly-rollup-ready");
  const account = await job.query("accountTotals", { key: input.accountId }, {
    consistency: "strong",
  });
  await job.cancel({ reason: "workflow-requested-cancel" });
  const result = await job.result();

  return ctx.complete({
    checkpoint,
    account,
    jobId: job.jobId,
    cancelRequested: true,
    terminalStatus: result.status,
  });
}
