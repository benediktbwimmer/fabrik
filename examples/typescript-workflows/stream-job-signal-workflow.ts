import type {
  StreamJobQueryDefinition,
  StreamJobTypes,
  WorkflowContext,
} from "../../sdk/typescript-compiler/workflow-authoring.js";

type AccountRollupSignalJob = StreamJobTypes<
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
      {
        kind: "signal_workflow";
        name: "notify-account-rollup";
        config: {
          view: "accountTotals";
          signalType: "account.rollup.ready";
          whenOutputField: "totalAmount";
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

type AccountRollupReadySignal = {
  jobId: string;
  handleId: string;
  jobName: "keyed-rollup";
  operatorId: "notify-account-rollup";
  viewName: "accountTotals";
  logicalKey: string;
  output: {
    accountId: string;
    totalAmount: number;
    asOfCheckpoint: number;
  };
};

export async function streamJobSignalWorkflow(
  ctx: WorkflowContext,
  input: {
    topic: string;
    accountId: string;
  },
) {
  const job = await ctx.startStreamJob<AccountRollupSignalJob>("keyed-rollup", {
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

  const signal = await ctx.waitForSignal<AccountRollupReadySignal>("account.rollup.ready");
  const account = await job.query("accountTotals", { key: input.accountId }, {
    consistency: "strong",
  });
  await job.cancel({ reason: "workflow-threshold-handled" });
  const result = await job.awaitTerminal();

  return ctx.complete({
    signal,
    account,
    terminalStatus: result.status,
    jobId: job.jobId,
  });
}
