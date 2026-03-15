export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
export type StreamJobCheckpointMap = Record<string, JsonValue>;

export interface StreamJobQueryDefinition<TArgs = JsonValue, TResult = JsonValue> {
  readonly args: TArgs;
  readonly result: TResult;
}

export type StreamJobQueryMap = Record<string, StreamJobQueryDefinition<any, any>>;

export interface StreamJobTypes<
  TInput = JsonValue,
  TConfig = JsonValue,
  TCheckpoints extends StreamJobCheckpointMap = StreamJobCheckpointMap,
  TQueries extends StreamJobQueryMap = StreamJobQueryMap,
  TTerminal = JsonValue,
> {
  readonly input: TInput;
  readonly config: TConfig;
  readonly checkpoints: TCheckpoints;
  readonly queries: TQueries;
  readonly terminal: TTerminal;
}

export function defineStreamJobTypes<
  TInput = JsonValue,
  TConfig = JsonValue,
  TCheckpoints extends StreamJobCheckpointMap = StreamJobCheckpointMap,
  TQueries extends StreamJobQueryMap = StreamJobQueryMap,
  TTerminal = JsonValue,
>(): StreamJobTypes<TInput, TConfig, TCheckpoints, TQueries, TTerminal> {
  return undefined as never;
}

export interface ActivityOptions {
  readonly timeout?: string;
}

export interface BulkActivityRetryOptions {
  readonly maxAttempts?: number;
  readonly delay?: string;
}

export interface BulkActivityOptions {
  readonly chunkSize?: number;
  readonly execution?: "default" | "eager";
  readonly reducer?:
    | "all_succeeded"
    | "all_settled"
    | "count"
    | "sum"
    | "min"
    | "max"
    | "avg"
    | "histogram"
    | "sample_errors"
    | "collect_results";
  readonly retry?: BulkActivityRetryOptions;
  readonly capabilities?: Record<string, JsonValue>;
}

export interface BulkActivityHandle<TResult = JsonValue> {
  result(): Promise<TResult>;
}

export interface ChildWorkflowOptions {
  readonly parentClosePolicy?: "terminate" | "abandon" | "request_cancel";
}

export interface ChildWorkflowHandle<TResult = JsonValue> {
  result(): Promise<TResult>;
  signal<TPayload = JsonValue>(signalName: string, payload?: TPayload): Promise<void>;
  cancel(reason?: JsonValue): Promise<void>;
}

export interface StreamJobStartRequest<TInput = JsonValue, TConfig = JsonValue> {
  readonly input: TInput;
  readonly config?: TConfig;
}

export interface StreamJobQueryOptions {
  readonly consistency?: "strong" | "eventual";
}

type StreamJobQueryArgs<TDefinition> =
  TDefinition extends StreamJobQueryDefinition<infer TArgs, any> ? TArgs : JsonValue;

type StreamJobQueryResult<TDefinition> =
  TDefinition extends StreamJobQueryDefinition<any, infer TResult> ? TResult : JsonValue;

export type StreamJobCheckpointFor<
  TJob extends StreamJobTypes<any, any, any, any, any>,
  TCheckpointName extends Extract<keyof TJob["checkpoints"], string>,
> = TJob["checkpoints"][TCheckpointName];

export type StreamJobQueryArgsFor<
  TJob extends StreamJobTypes<any, any, any, any, any>,
  TQueryName extends Extract<keyof TJob["queries"], string>,
> = StreamJobQueryArgs<TJob["queries"][TQueryName]>;

export type StreamJobQueryResultFor<
  TJob extends StreamJobTypes<any, any, any, any, any>,
  TQueryName extends Extract<keyof TJob["queries"], string>,
> = StreamJobQueryResult<TJob["queries"][TQueryName]>;

export interface StreamJobHandle<
  TCheckpoints extends StreamJobCheckpointMap = StreamJobCheckpointMap,
  TQueries extends StreamJobQueryMap = StreamJobQueryMap,
  TTerminal = JsonValue,
> {
  readonly jobId: string;
  readonly handleId?: string;
  awaitCheckpoint<TCheckpointName extends Extract<keyof TCheckpoints, string>>(
    checkpointName: TCheckpointName,
  ): Promise<TCheckpoints[TCheckpointName]>;
  untilCheckpoint<TCheckpointName extends Extract<keyof TCheckpoints, string>>(
    checkpointName: TCheckpointName,
  ): Promise<TCheckpoints[TCheckpointName]>;
  query<TQueryName extends Extract<keyof TQueries, string>>(
    queryName: TQueryName,
    args: StreamJobQueryArgs<TQueries[TQueryName]>,
    options?: StreamJobQueryOptions,
  ): Promise<StreamJobQueryResult<TQueries[TQueryName]>>;
  awaitTerminal(): Promise<TTerminal>;
  result(): Promise<TTerminal>;
  cancel(reason?: JsonValue): Promise<void>;
}

export type StreamJobStartRequestFor<TJob extends StreamJobTypes<any, any, any, any>> =
  StreamJobStartRequest<TJob["input"], TJob["config"]>;

export type StreamJobHandleFor<TJob extends StreamJobTypes<any, any, any, any, any>> =
  StreamJobHandle<TJob["checkpoints"], TJob["queries"], TJob["terminal"]>;

export type StreamJobTerminalFor<TJob extends StreamJobTypes<any, any, any, any, any>> =
  TJob["terminal"];

export type KeyedRollupItem = {
  eventId: string;
  accountId: string;
  amount: number;
};

export type KeyedRollupCheckpoints = {
  "hourly-rollup-ready": {
    checkpointName: "hourly-rollup-ready";
    checkpointSequence: number;
    ready: boolean;
  };
};

export type KeyedRollupQueries = {
  accountTotals: StreamJobQueryDefinition<
    { key: string },
    {
      accountId: string;
      totalAmount: number;
      asOfCheckpoint: number;
    }
  >;
};

export interface KeyedRollupJob
  extends StreamJobTypes<
    {
      kind: "bounded_items";
      items: KeyedRollupItem[];
    },
    {
      thresholdAccountId?: string;
      checkpoint?: string;
    },
    KeyedRollupCheckpoints,
    KeyedRollupQueries,
    {
      jobId: string;
      jobName: "keyed-rollup";
      status: "completed";
    }
  > {}

export type KeyedRollupHandle = StreamJobHandleFor<KeyedRollupJob>;

export interface WorkflowContext {
  waitForSignal<TSignal = JsonValue>(signalName: string): Promise<TSignal>;
  activity<TResult = JsonValue, TInput = JsonValue>(
    activityName: string,
    input: TInput,
    options?: ActivityOptions,
  ): Promise<TResult>;
  bulkActivity<TResult = JsonValue, TItem = JsonValue>(
    activityName: string,
    items: readonly TItem[],
    options?: BulkActivityOptions,
  ): Promise<BulkActivityHandle<TResult>>;
  startChild<TResult = JsonValue, TInput = JsonValue>(
    workflowName: string,
    input: TInput,
    options?: ChildWorkflowOptions,
  ): Promise<ChildWorkflowHandle<TResult>>;
  startStreamJob<TJob extends StreamJobTypes<any, any, any, any, any> = StreamJobTypes>(
    jobName: string,
    request: StreamJobStartRequestFor<TJob>,
  ): Promise<StreamJobHandleFor<TJob>>;
  complete<TResult = JsonValue>(output?: TResult): TResult;
  fail(reason: JsonValue): never;
}
