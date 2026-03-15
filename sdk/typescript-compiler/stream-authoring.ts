export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
export type StreamRuntime = "keyed_rollup" | "aggregate_v2";
export type StreamSourceKind = "bounded_input" | "topic";
export type StreamOperatorKind =
  | "map"
  | "filter"
  | "route"
  | "key_by"
  | "window"
  | "reduce"
  | "aggregate"
  | "dedupe"
  | "materialize"
  | "emit_checkpoint"
  | "signal_workflow"
  | "sink";

export interface StreamSourceDefinition {
  readonly kind: StreamSourceKind;
  readonly name?: string;
  readonly binding?: string;
  readonly config?: JsonValue;
}

export interface StreamReduceOperatorDefinition {
  readonly kind: "reduce";
  readonly operatorId?: string;
  readonly name?: string;
  readonly inputs?: readonly string[];
  readonly outputs?: readonly string[];
  readonly stateIds?: readonly string[];
  readonly config: {
    readonly reducer: "count" | "sum" | "min" | "max" | "avg" | "histogram" | "threshold";
    readonly valueField: string;
    readonly outputField?: string;
    readonly threshold?: number;
    readonly comparison?: "gt" | "gte" | "lt" | "lte";
  };
}

export interface StreamMapOperatorDefinition {
  readonly kind: "map";
  readonly operatorId?: string;
  readonly name?: string;
  readonly inputs?: readonly string[];
  readonly outputs?: readonly string[];
  readonly stateIds?: readonly string[];
  readonly config: {
    readonly inputField: string;
    readonly outputField: string;
    readonly multiplyBy?: number;
    readonly add?: number;
  };
}

export interface StreamFilterOperatorDefinition {
  readonly kind: "filter";
  readonly operatorId?: string;
  readonly name?: string;
  readonly inputs?: readonly string[];
  readonly outputs?: readonly string[];
  readonly stateIds?: readonly string[];
  readonly config: JsonValue;
}

export interface StreamRouteOperatorDefinition {
  readonly kind: "route";
  readonly operatorId?: string;
  readonly name?: string;
  readonly inputs?: readonly string[];
  readonly outputs?: readonly string[];
  readonly stateIds?: readonly string[];
  readonly config: {
    readonly outputField: string;
    readonly branches: readonly {
      readonly predicate: string;
      readonly value: string;
    }[];
    readonly defaultValue?: string;
  };
}

export interface StreamWindowOperatorDefinition {
  readonly kind: "window";
  readonly operatorId?: string;
  readonly name?: string;
  readonly inputs?: readonly string[];
  readonly outputs?: readonly string[];
  readonly stateIds?: readonly string[];
  readonly config: {
    readonly mode: "tumbling";
    readonly size: string;
    readonly timeField?: string;
    readonly allowedLateness?: string;
    readonly retentionSeconds?: number;
  };
}

export interface StreamAggregateOperatorDefinition {
  readonly kind: "aggregate";
  readonly operatorId?: string;
  readonly name?: string;
  readonly inputs?: readonly string[];
  readonly outputs?: readonly string[];
  readonly stateIds?: readonly string[];
  readonly config: {
    readonly reducer: "count" | "sum" | "min" | "max" | "avg" | "histogram" | "threshold";
    readonly valueField?: string;
    readonly outputField?: string;
    readonly threshold?: number;
    readonly comparison?: "gt" | "gte" | "lt" | "lte";
  };
}

export interface StreamDedupeOperatorDefinition {
  readonly kind: "dedupe";
  readonly operatorId?: string;
  readonly name?: string;
  readonly inputs?: readonly string[];
  readonly outputs?: readonly string[];
  readonly stateIds?: readonly string[];
  readonly config: JsonValue;
}

export interface StreamMaterializeOperatorDefinition {
  readonly kind: "materialize";
  readonly operatorId?: string;
  readonly name?: string;
  readonly inputs?: readonly string[];
  readonly outputs?: readonly string[];
  readonly stateIds?: readonly string[];
  readonly config: {
    readonly view: string;
  };
}

export interface StreamEmitCheckpointOperatorDefinition {
  readonly kind: "emit_checkpoint";
  readonly operatorId?: string;
  readonly name: string;
  readonly inputs?: readonly string[];
  readonly outputs?: readonly string[];
  readonly stateIds?: readonly string[];
  readonly config: {
    readonly sequence: number;
  };
}

export interface StreamSignalWorkflowOperatorDefinition {
  readonly kind: "signal_workflow";
  readonly operatorId?: string;
  readonly name?: string;
  readonly inputs?: readonly string[];
  readonly outputs?: readonly string[];
  readonly stateIds?: readonly string[];
  readonly config: {
    readonly view: string;
    readonly signalType: string;
    readonly whenOutputField?: string;
  };
}

export interface StreamSinkOperatorDefinition {
  readonly kind: "sink";
  readonly operatorId?: string;
  readonly name?: string;
  readonly inputs?: readonly string[];
  readonly outputs?: readonly string[];
  readonly stateIds?: readonly string[];
  readonly config: JsonValue;
}

export type StreamOperatorDefinition =
  | StreamMapOperatorDefinition
  | StreamFilterOperatorDefinition
  | StreamRouteOperatorDefinition
  | StreamWindowOperatorDefinition
  | StreamReduceOperatorDefinition
  | StreamAggregateOperatorDefinition
  | StreamDedupeOperatorDefinition
  | StreamMaterializeOperatorDefinition
  | StreamEmitCheckpointOperatorDefinition
  | StreamSignalWorkflowOperatorDefinition
  | StreamSinkOperatorDefinition;

export interface StreamStateDefinition {
  readonly id: string;
  readonly kind: "keyed" | "window" | "dedupe";
  readonly keyFields?: readonly string[];
  readonly valueFields?: readonly string[];
  readonly retentionSeconds?: number;
  readonly config?: JsonValue;
}

export interface StreamCheckpointDefinition {
  readonly name: string;
  readonly delivery: "workflow_awaitable" | "operator_visible";
  readonly sequence?: number;
}

export interface StreamCheckpointPolicy {
  readonly kind: "named_checkpoints";
  readonly checkpoints: readonly StreamCheckpointDefinition[];
}

export interface MaterializedViewDefinition {
  readonly name: string;
  readonly consistency: "strong" | "eventual";
  readonly queryMode: "by_key" | "prefix_scan" | "scan";
  readonly viewId?: string;
  readonly keyField?: string;
  readonly valueFields?: readonly string[];
  readonly supportedConsistencies?: readonly ("strong" | "eventual")[];
  readonly retentionSeconds?: number;
}

export interface StreamQueryDefinition {
  readonly name: string;
  readonly viewName: string;
  readonly consistency: "strong" | "eventual";
  readonly queryId?: string;
  readonly argFields?: readonly string[];
}

export interface StreamJobDefinition {
  readonly name: string;
  readonly runtime: StreamRuntime;
  readonly source: StreamSourceDefinition;
  readonly keyBy?: string;
  readonly states?: readonly StreamStateDefinition[];
  readonly operators?: readonly StreamOperatorDefinition[];
  readonly checkpointPolicy?: StreamCheckpointPolicy;
  readonly views?: readonly MaterializedViewDefinition[];
  readonly queries?: readonly StreamQueryDefinition[];
  readonly classification?: "fast_lane" | "mixed_mode";
  readonly metadata?: JsonValue;
}

export function streamJob<TDefinition extends StreamJobDefinition>(
  definition: TDefinition,
): TDefinition {
  return definition;
}
