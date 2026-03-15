use std::collections::BTreeMap;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    CompiledAggregateV2Checkpoint, CompiledAggregateV2Kernel, CompiledAggregateV2MaterializedView,
    CompiledAggregateV2PreKeyOperator, CompiledAggregateV2WorkflowSignal,
    CompiledKeyedRollupKernel, CompiledStreamJob, CompiledStreamJobArtifact, CompiledStreamQuery,
    CompiledStreamView, STREAM_CONSISTENCY_STRONG, STREAM_OPERATOR_AGGREGATE,
    STREAM_OPERATOR_EMIT_CHECKPOINT, STREAM_OPERATOR_FILTER, STREAM_OPERATOR_MAP,
    STREAM_OPERATOR_MATERIALIZE, STREAM_OPERATOR_REDUCE, STREAM_OPERATOR_ROUTE,
    STREAM_OPERATOR_SIGNAL_WORKFLOW, STREAM_OPERATOR_WINDOW, STREAM_RUNTIME_AGGREGATE_V2,
    STREAM_RUNTIME_KEYED_ROLLUP, STREAMS_KERNEL_V1_CONTRACT, STREAMS_KERNEL_V2_CONTRACT,
    StartThroughputRunCommand,
};

pub const STREAMS_DATAFLOW_V1_CONTRACT: &str = "streams_dataflow_v1";
pub const DATAFLOW_PARTITIONING_HASH: &str = "hash";
pub const DATAFLOW_PARTITIONING_AGGREGATION_GROUP: &str = "aggregation_group";
pub const DATAFLOW_REGION_SOURCE: &str = "source_admission";
pub const DATAFLOW_REGION_PRE_KEY: &str = "pre_key";
pub const DATAFLOW_REGION_STATEFUL: &str = "stateful_apply";
pub const DATAFLOW_EDGE_EXCHANGE: &str = "exchange";
pub const DATAFLOW_EDGE_LOCAL: &str = "local";
pub const DATAFLOW_DURABILITY_ACCEPTED_PROGRESS_LOG: &str = "accepted_progress_log";
pub const DATAFLOW_STRONG_READ_METADATA_OWNER_EPOCH: &str = "owner_epoch";
pub const DATAFLOW_STRONG_READ_METADATA_LAST_DURABLE_POSITION: &str = "last_durable_position";
pub const DATAFLOW_STRONG_READ_METADATA_LAST_SOURCE_FRONTIER: &str = "last_source_frontier";
pub const DATAFLOW_STRONG_READ_METADATA_LAST_SEALED_CHECKPOINT: &str = "last_sealed_checkpoint";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct PartitionedDataflowPlan {
    pub plan_id: String,
    pub runtime_contract: String,
    pub stream_runtime: String,
    pub job_name: String,
    pub source: DataflowSourcePlan,
    pub partitioning: DataflowPartitioningPlan,
    #[serde(default)]
    pub regions: Vec<DataflowFusedRegion>,
    #[serde(default)]
    pub edges: Vec<DataflowEdge>,
    #[serde(default)]
    pub materialized_views: Vec<DataflowMaterializedViewPlan>,
    #[serde(default)]
    pub queries: Vec<DataflowQueryPlan>,
    #[serde(default)]
    pub checkpoints: Vec<DataflowCheckpointPlan>,
    #[serde(default)]
    pub workflow_signals: Vec<DataflowWorkflowSignalPlan>,
    pub strong_reads: DataflowStrongReadContract,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_work: Option<DataflowExternalWorkPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DataflowSourcePlan {
    pub source_id: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DataflowPartitioningPlan {
    pub mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DataflowFusedRegion {
    pub region_id: String,
    pub stage: String,
    #[serde(default)]
    pub operators: Vec<DataflowOperatorPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DataflowOperatorPlan {
    pub operator_id: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DataflowEdge {
    pub edge_id: String,
    pub kind: String,
    pub source_region_id: String,
    pub dest_region_id: String,
    pub partitioning: DataflowPartitioningPlan,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DataflowMaterializedViewPlan {
    pub view_name: String,
    pub query_mode: String,
    #[serde(default)]
    pub supported_consistencies: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DataflowQueryPlan {
    pub query_name: String,
    pub view_name: String,
    pub consistency: String,
    #[serde(default)]
    pub arg_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DataflowCheckpointPlan {
    pub checkpoint_id: String,
    pub checkpoint_name: String,
    pub sequence: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DataflowWorkflowSignalPlan {
    pub operator_id: String,
    pub view_name: String,
    pub signal_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when_output_field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DataflowStrongReadContract {
    pub consistency: String,
    pub durability_boundary: String,
    #[serde(default)]
    pub metadata_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DataflowExternalWorkPlan {
    pub activity_type: String,
    pub task_queue: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reducer: Option<String>,
    pub chunk_size: u32,
    pub max_attempts: u32,
    pub retry_delay_ms: u64,
    pub total_items: u32,
    pub aggregation_group_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DataflowOffsetRange {
    pub start: i64,
    pub end: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DataflowBatchSummary {
    pub record_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_checksum: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DataflowBatchEnvelope {
    pub plan_id: String,
    pub batch_id: String,
    pub source_id: String,
    pub source_partition_id: i32,
    pub source_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset_range: Option<DataflowOffsetRange>,
    pub edge_id: String,
    pub dest_partition_id: i32,
    pub owner_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_epoch: Option<u64>,
    pub summary: DataflowBatchSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct AcceptedProgressRecord {
    pub plan_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    pub source_id: String,
    pub source_partition_id: i32,
    pub source_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset_range: Option<DataflowOffsetRange>,
    pub edge_id: String,
    pub dest_partition_id: i32,
    pub batch_id: String,
    pub owner_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_epoch: Option<u64>,
    pub summary: DataflowBatchSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct OwnerApplyAck {
    pub plan_id: String,
    pub batch_id: String,
    pub dest_partition_id: i32,
    pub owner_epoch: u64,
    pub durable: bool,
    pub accepted_progress_position: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DataflowSourceFrontier {
    pub source_partition_id: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_applied_offset: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_offset: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub high_watermark: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DataflowWatermarkState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_time_watermark: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DataflowStateImageManifest {
    pub manifest_key: String,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct SealedCheckpointRecord {
    pub checkpoint_id: String,
    pub plan_id: String,
    pub partition_id: i32,
    pub owner_epoch: u64,
    #[serde(default)]
    pub source_frontier: Vec<DataflowSourceFrontier>,
    pub accepted_progress_position: u64,
    pub watermark_state: DataflowWatermarkState,
    pub state_image: DataflowStateImageManifest,
    pub sealed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct StrongReadCheckpointRef {
    pub checkpoint_name: String,
    pub checkpoint_sequence: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sealed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct StrongReadMetadata {
    pub consistency: String,
    pub owner_epoch: u64,
    #[serde(default)]
    pub last_durable_position: BTreeMap<i32, i64>,
    #[serde(default)]
    pub last_source_frontier: Vec<DataflowSourceFrontier>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_sealed_checkpoint: Option<StrongReadCheckpointRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct DataflowBridgeCallbackRecord {
    pub callback_id: String,
    pub plan_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    pub owner_epoch: u64,
    pub callback_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<StrongReadCheckpointRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_progress_position: Option<u64>,
}

impl CompiledStreamJob {
    pub fn dataflow_plan(&self, plan_id: impl Into<String>) -> Result<PartitionedDataflowPlan> {
        let plan_id = plan_id.into();
        match self.runtime.as_str() {
            STREAM_RUNTIME_KEYED_ROLLUP => {
                let kernel = self
                    .keyed_rollup_kernel()?
                    .context("streams_dataflow_v1 requires a valid keyed_rollup kernel")?;
                Ok(lower_keyed_rollup_plan(&plan_id, self, &kernel))
            }
            STREAM_RUNTIME_AGGREGATE_V2 => {
                let kernel = self
                    .aggregate_v2_kernel()?
                    .context("streams_dataflow_v1 requires a valid aggregate_v2 kernel")?;
                lower_aggregate_v2_plan(&plan_id, self, &kernel)
            }
            other => anyhow::bail!("streams_dataflow_v1 does not support runtime {other}"),
        }
    }

    pub fn validate_for_runtime_contract(&self, runtime_contract: &str) -> Result<()> {
        self.validate_well_formed()?;
        match runtime_contract {
            STREAMS_KERNEL_V1_CONTRACT => {
                self.keyed_rollup_kernel()?.with_context(|| {
                    format!(
                        "stream artifact runtime_contract {STREAMS_KERNEL_V1_CONTRACT} requires keyed_rollup runtime"
                    )
                })?;
            }
            STREAMS_KERNEL_V2_CONTRACT => {
                self.aggregate_v2_kernel()?.with_context(|| {
                    format!(
                        "stream artifact runtime_contract {STREAMS_KERNEL_V2_CONTRACT} requires aggregate_v2 runtime"
                    )
                })?;
            }
            STREAMS_DATAFLOW_V1_CONTRACT => {
                self.dataflow_plan(self.name.clone())?;
            }
            other => anyhow::bail!("unsupported stream runtime_contract {other}"),
        }
        Ok(())
    }
}

impl CompiledStreamJobArtifact {
    pub fn dataflow_plan(&self) -> Result<PartitionedDataflowPlan> {
        self.validate_persistable()?;
        self.job.dataflow_plan(format!(
            "{}:{}:{}",
            self.definition_id, self.definition_version, self.artifact_hash
        ))
    }
}

impl PartitionedDataflowPlan {
    pub fn summary_value(&self) -> Value {
        json!({
            "planId": self.plan_id,
            "runtimeContract": self.runtime_contract,
            "streamRuntime": self.stream_runtime,
            "jobName": self.job_name,
            "source": {
                "sourceId": self.source.source_id,
                "kind": self.source.kind,
                "name": self.source.name,
                "binding": self.source.binding,
            },
            "partitioning": {
                "mode": self.partitioning.mode,
                "keyField": self.partitioning.key_field,
            },
            "regionCount": self.regions.len(),
            "exchangeEdgeCount": self.edges.iter().filter(|edge| edge.kind == DATAFLOW_EDGE_EXCHANGE).count(),
            "regions": self.regions.iter().map(|region| {
                json!({
                    "regionId": region.region_id,
                    "stage": region.stage,
                    "operatorCount": region.operators.len(),
                    "operatorKinds": region.operators.iter().map(|operator| operator.kind.clone()).collect::<Vec<_>>(),
                })
            }).collect::<Vec<_>>(),
            "checkpoints": self.checkpoints.iter().map(|checkpoint| {
                json!({
                    "checkpointId": checkpoint.checkpoint_id,
                    "checkpointName": checkpoint.checkpoint_name,
                    "sequence": checkpoint.sequence,
                    "delivery": checkpoint.delivery,
                })
            }).collect::<Vec<_>>(),
            "materializedViewCount": self.materialized_views.len(),
            "queryCount": self.queries.len(),
            "workflowSignalCount": self.workflow_signals.len(),
            "strongReadContract": {
                "consistency": self.strong_reads.consistency,
                "durabilityBoundary": self.strong_reads.durability_boundary,
                "metadataFields": self.strong_reads.metadata_fields,
            },
            "externalWork": self.external_work.as_ref().map(|external_work| {
                json!({
                    "activityType": external_work.activity_type,
                    "taskQueue": external_work.task_queue,
                    "reducer": external_work.reducer,
                    "chunkSize": external_work.chunk_size,
                    "maxAttempts": external_work.max_attempts,
                    "retryDelayMs": external_work.retry_delay_ms,
                    "totalItems": external_work.total_items,
                    "aggregationGroupCount": external_work.aggregation_group_count,
                })
            }).unwrap_or(Value::Null),
        })
    }
}

impl StartThroughputRunCommand {
    pub fn dataflow_plan(&self) -> PartitionedDataflowPlan {
        let plan_id = self.resolved_bridge_request_id();
        PartitionedDataflowPlan {
            plan_id: plan_id.clone(),
            runtime_contract: STREAMS_DATAFLOW_V1_CONTRACT.to_owned(),
            stream_runtime: "external_work".to_owned(),
            job_name: format!("bulk-{}", self.batch_id),
            source: DataflowSourcePlan {
                source_id: format!("throughput:{}", self.batch_id),
                kind: "throughput_batch_input".to_owned(),
                name: Some(self.batch_id.clone()),
                binding: Some(self.batch_id.clone()),
                config: Some(json!({
                    "inputHandle": self.input_handle.clone(),
                    "resultHandle": self.result_handle.clone(),
                    "totalItems": self.total_items,
                    "executionPolicy": self.execution_policy.clone(),
                })),
            },
            partitioning: DataflowPartitioningPlan {
                mode: DATAFLOW_PARTITIONING_AGGREGATION_GROUP.to_owned(),
                key_field: Some("groupId".to_owned()),
            },
            regions: vec![
                DataflowFusedRegion {
                    region_id: "source_admission".to_owned(),
                    stage: DATAFLOW_REGION_SOURCE.to_owned(),
                    operators: vec![
                        DataflowOperatorPlan {
                            operator_id: "source".to_owned(),
                            kind: "source".to_owned(),
                            config: None,
                        },
                        DataflowOperatorPlan {
                            operator_id: "input_expand".to_owned(),
                            kind: "input_expand".to_owned(),
                            config: Some(json!({
                                "chunkSize": self.chunk_size,
                                "totalItems": self.total_items,
                            })),
                        },
                    ],
                },
                DataflowFusedRegion {
                    region_id: "stateful_apply".to_owned(),
                    stage: DATAFLOW_REGION_STATEFUL.to_owned(),
                    operators: vec![
                        DataflowOperatorPlan {
                            operator_id: "dispatch_external_work".to_owned(),
                            kind: "dispatch_external_work".to_owned(),
                            config: Some(json!({
                                "activityType": self.activity_type.clone(),
                                "taskQueue": self.task_queue.clone(),
                            })),
                        },
                        DataflowOperatorPlan {
                            operator_id: "ingest_terminal_results".to_owned(),
                            kind: "ingest_terminal_results".to_owned(),
                            config: None,
                        },
                        DataflowOperatorPlan {
                            operator_id: "reduce".to_owned(),
                            kind: "reduce".to_owned(),
                            config: Some(json!({
                                "reducer": self.reducer.clone(),
                            })),
                        },
                        DataflowOperatorPlan {
                            operator_id: "terminalize".to_owned(),
                            kind: "terminalize".to_owned(),
                            config: None,
                        },
                    ],
                },
            ],
            edges: vec![DataflowEdge {
                edge_id: "exchange:aggregation_group".to_owned(),
                kind: DATAFLOW_EDGE_EXCHANGE.to_owned(),
                source_region_id: "source_admission".to_owned(),
                dest_region_id: "stateful_apply".to_owned(),
                partitioning: DataflowPartitioningPlan {
                    mode: DATAFLOW_PARTITIONING_AGGREGATION_GROUP.to_owned(),
                    key_field: Some("groupId".to_owned()),
                },
            }],
            materialized_views: Vec::new(),
            queries: Vec::new(),
            checkpoints: Vec::new(),
            workflow_signals: Vec::new(),
            strong_reads: default_strong_read_contract(),
            external_work: Some(DataflowExternalWorkPlan {
                activity_type: self.activity_type.clone(),
                task_queue: self.task_queue.clone(),
                reducer: self.reducer.clone(),
                chunk_size: self.chunk_size,
                max_attempts: self.max_attempts,
                retry_delay_ms: self.retry_delay_ms,
                total_items: self.total_items,
                aggregation_group_count: self.aggregation_group_count,
            }),
        }
    }
}

fn lower_keyed_rollup_plan(
    plan_id: &str,
    job: &CompiledStreamJob,
    kernel: &CompiledKeyedRollupKernel,
) -> PartitionedDataflowPlan {
    PartitionedDataflowPlan {
        plan_id: plan_id.to_owned(),
        runtime_contract: STREAMS_DATAFLOW_V1_CONTRACT.to_owned(),
        stream_runtime: job.runtime.clone(),
        job_name: job.name.clone(),
        source: DataflowSourcePlan {
            source_id: job.source.name.clone().unwrap_or_else(|| "source".to_owned()),
            kind: job.source.kind.clone(),
            name: job.source.name.clone(),
            binding: job.source.binding.clone(),
            config: job.source.config.clone(),
        },
        partitioning: DataflowPartitioningPlan {
            mode: DATAFLOW_PARTITIONING_HASH.to_owned(),
            key_field: Some(kernel.key_field.clone()),
        },
        regions: vec![
            DataflowFusedRegion {
                region_id: "source_admission".to_owned(),
                stage: DATAFLOW_REGION_SOURCE.to_owned(),
                operators: vec![DataflowOperatorPlan {
                    operator_id: "source".to_owned(),
                    kind: "source".to_owned(),
                    config: None,
                }],
            },
            DataflowFusedRegion {
                region_id: "stateful_apply".to_owned(),
                stage: DATAFLOW_REGION_STATEFUL.to_owned(),
                operators: vec![
                    DataflowOperatorPlan {
                        operator_id: "reduce".to_owned(),
                        kind: STREAM_OPERATOR_REDUCE.to_owned(),
                        config: Some(json!({
                            "reducer": "sum",
                            "valueField": kernel.value_field,
                            "outputField": kernel.output_field,
                        })),
                    },
                    DataflowOperatorPlan {
                        operator_id: "materialize".to_owned(),
                        kind: STREAM_OPERATOR_MATERIALIZE.to_owned(),
                        config: Some(json!({
                            "view": kernel.view_name,
                        })),
                    },
                    DataflowOperatorPlan {
                        operator_id: kernel.checkpoint_name.clone(),
                        kind: STREAM_OPERATOR_EMIT_CHECKPOINT.to_owned(),
                        config: Some(json!({
                            "sequence": kernel.checkpoint_sequence,
                        })),
                    },
                ]
                .into_iter()
                .chain(kernel.workflow_signals.iter().map(signal_operator))
                .collect(),
            },
        ],
        edges: vec![DataflowEdge {
            edge_id: format!("exchange:key_by:{}", kernel.key_field),
            kind: DATAFLOW_EDGE_EXCHANGE.to_owned(),
            source_region_id: "source_admission".to_owned(),
            dest_region_id: "stateful_apply".to_owned(),
            partitioning: DataflowPartitioningPlan {
                mode: DATAFLOW_PARTITIONING_HASH.to_owned(),
                key_field: Some(kernel.key_field.clone()),
            },
        }],
        materialized_views: job.views.iter().map(materialized_view_plan).collect(),
        queries: job.queries.iter().map(query_plan).collect(),
        checkpoints: vec![DataflowCheckpointPlan {
            checkpoint_id: format!("checkpoint:{}", kernel.checkpoint_name),
            checkpoint_name: kernel.checkpoint_name.clone(),
            sequence: kernel.checkpoint_sequence,
            delivery: Some("workflow_awaitable".to_owned()),
        }],
        workflow_signals: kernel.workflow_signals.iter().map(workflow_signal_plan).collect(),
        strong_reads: default_strong_read_contract(),
        external_work: None,
    }
}

fn lower_aggregate_v2_plan(
    plan_id: &str,
    job: &CompiledStreamJob,
    kernel: &CompiledAggregateV2Kernel,
) -> Result<PartitionedDataflowPlan> {
    let mut source_ops = vec![DataflowOperatorPlan {
        operator_id: "source".to_owned(),
        kind: "source".to_owned(),
        config: None,
    }];
    let pre_key_ops =
        kernel.pre_key_operators.iter().map(pre_key_operator_plan).collect::<Result<Vec<_>>>()?;
    if !pre_key_ops.is_empty() {
        source_ops.extend(pre_key_ops);
    }

    let mut stateful_ops = Vec::new();
    if let Some(window) = &kernel.window {
        stateful_ops.push(DataflowOperatorPlan {
            operator_id: window.operator_id.clone(),
            kind: STREAM_OPERATOR_WINDOW.to_owned(),
            config: Some(serde_json::to_value(window)?),
        });
    }
    stateful_ops.extend(kernel.aggregates.iter().map(|aggregate| DataflowOperatorPlan {
        operator_id: aggregate.operator_id.clone(),
        kind: STREAM_OPERATOR_AGGREGATE.to_owned(),
        config: Some(
            serde_json::to_value(aggregate).expect("aggregate_v2 aggregate should serialize"),
        ),
    }));
    stateful_ops.extend(kernel.materialized_views.iter().map(materialized_view_operator_plan));
    stateful_ops.extend(kernel.checkpoints.iter().map(checkpoint_operator_plan));
    stateful_ops.extend(kernel.workflow_signals.iter().map(signal_operator));

    Ok(PartitionedDataflowPlan {
        plan_id: plan_id.to_owned(),
        runtime_contract: STREAMS_DATAFLOW_V1_CONTRACT.to_owned(),
        stream_runtime: job.runtime.clone(),
        job_name: job.name.clone(),
        source: DataflowSourcePlan {
            source_id: kernel.source_name.clone().unwrap_or_else(|| "source".to_owned()),
            kind: kernel.source_kind.clone(),
            name: job.source.name.clone(),
            binding: job.source.binding.clone(),
            config: job.source.config.clone(),
        },
        partitioning: DataflowPartitioningPlan {
            mode: DATAFLOW_PARTITIONING_HASH.to_owned(),
            key_field: Some(kernel.key_field.clone()),
        },
        regions: vec![
            DataflowFusedRegion {
                region_id: if kernel.pre_key_operators.is_empty() {
                    "source_admission".to_owned()
                } else {
                    "pre_key".to_owned()
                },
                stage: if kernel.pre_key_operators.is_empty() {
                    DATAFLOW_REGION_SOURCE.to_owned()
                } else {
                    DATAFLOW_REGION_PRE_KEY.to_owned()
                },
                operators: source_ops,
            },
            DataflowFusedRegion {
                region_id: "stateful_apply".to_owned(),
                stage: DATAFLOW_REGION_STATEFUL.to_owned(),
                operators: stateful_ops,
            },
        ],
        edges: vec![DataflowEdge {
            edge_id: format!("exchange:key_by:{}", kernel.key_field),
            kind: DATAFLOW_EDGE_EXCHANGE.to_owned(),
            source_region_id: if kernel.pre_key_operators.is_empty() {
                "source_admission".to_owned()
            } else {
                "pre_key".to_owned()
            },
            dest_region_id: "stateful_apply".to_owned(),
            partitioning: DataflowPartitioningPlan {
                mode: DATAFLOW_PARTITIONING_HASH.to_owned(),
                key_field: Some(kernel.key_field.clone()),
            },
        }],
        materialized_views: job.views.iter().map(materialized_view_plan).collect(),
        queries: job.queries.iter().map(query_plan).collect(),
        checkpoints: kernel.checkpoints.iter().map(checkpoint_plan).collect(),
        workflow_signals: kernel.workflow_signals.iter().map(workflow_signal_plan).collect(),
        strong_reads: default_strong_read_contract(),
        external_work: None,
    })
}

fn pre_key_operator_plan(
    operator: &CompiledAggregateV2PreKeyOperator,
) -> Result<DataflowOperatorPlan> {
    let (operator_id, kind, config) = match operator {
        CompiledAggregateV2PreKeyOperator::Map(map) => {
            (map.operator_id.clone(), STREAM_OPERATOR_MAP.to_owned(), serde_json::to_value(map)?)
        }
        CompiledAggregateV2PreKeyOperator::Filter(filter) => (
            filter.operator_id.clone(),
            STREAM_OPERATOR_FILTER.to_owned(),
            serde_json::to_value(filter)?,
        ),
        CompiledAggregateV2PreKeyOperator::Route(route) => (
            route.operator_id.clone(),
            STREAM_OPERATOR_ROUTE.to_owned(),
            serde_json::to_value(route)?,
        ),
    };
    Ok(DataflowOperatorPlan { operator_id, kind, config: Some(config) })
}

fn materialized_view_operator_plan(
    view: &CompiledAggregateV2MaterializedView,
) -> DataflowOperatorPlan {
    DataflowOperatorPlan {
        operator_id: view.operator_id.clone(),
        kind: STREAM_OPERATOR_MATERIALIZE.to_owned(),
        config: Some(
            serde_json::to_value(view).expect("aggregate_v2 materialized view should serialize"),
        ),
    }
}

fn checkpoint_operator_plan(checkpoint: &CompiledAggregateV2Checkpoint) -> DataflowOperatorPlan {
    DataflowOperatorPlan {
        operator_id: checkpoint.name.clone(),
        kind: STREAM_OPERATOR_EMIT_CHECKPOINT.to_owned(),
        config: Some(json!({
            "name": checkpoint.name,
            "sequence": checkpoint.sequence,
        })),
    }
}

fn signal_operator(signal: &CompiledAggregateV2WorkflowSignal) -> DataflowOperatorPlan {
    DataflowOperatorPlan {
        operator_id: signal.operator_id.clone(),
        kind: STREAM_OPERATOR_SIGNAL_WORKFLOW.to_owned(),
        config: Some(
            serde_json::to_value(signal).expect("aggregate_v2 workflow signal should serialize"),
        ),
    }
}

fn materialized_view_plan(view: &CompiledStreamView) -> DataflowMaterializedViewPlan {
    DataflowMaterializedViewPlan {
        view_name: view.name.clone(),
        query_mode: view.query_mode.clone(),
        supported_consistencies: if view.supported_consistencies.is_empty() {
            vec![view.consistency.clone()]
        } else {
            view.supported_consistencies.clone()
        },
    }
}

fn query_plan(query: &CompiledStreamQuery) -> DataflowQueryPlan {
    DataflowQueryPlan {
        query_name: query.name.clone(),
        view_name: query.view_name.clone(),
        consistency: query.consistency.clone(),
        arg_fields: query.arg_fields.clone(),
    }
}

fn checkpoint_plan(checkpoint: &CompiledAggregateV2Checkpoint) -> DataflowCheckpointPlan {
    DataflowCheckpointPlan {
        checkpoint_id: format!("checkpoint:{}", checkpoint.name),
        checkpoint_name: checkpoint.name.clone(),
        sequence: checkpoint.sequence,
        delivery: Some("workflow_awaitable".to_owned()),
    }
}

fn workflow_signal_plan(signal: &CompiledAggregateV2WorkflowSignal) -> DataflowWorkflowSignalPlan {
    DataflowWorkflowSignalPlan {
        operator_id: signal.operator_id.clone(),
        view_name: signal.view_name.clone(),
        signal_type: signal.signal_type.clone(),
        when_output_field: signal.when_output_field.clone(),
    }
}

fn default_strong_read_contract() -> DataflowStrongReadContract {
    DataflowStrongReadContract {
        consistency: STREAM_CONSISTENCY_STRONG.to_owned(),
        durability_boundary: DATAFLOW_DURABILITY_ACCEPTED_PROGRESS_LOG.to_owned(),
        metadata_fields: vec![
            DATAFLOW_STRONG_READ_METADATA_OWNER_EPOCH.to_owned(),
            DATAFLOW_STRONG_READ_METADATA_LAST_DURABLE_POSITION.to_owned(),
            DATAFLOW_STRONG_READ_METADATA_LAST_SOURCE_FRONTIER.to_owned(),
            DATAFLOW_STRONG_READ_METADATA_LAST_SEALED_CHECKPOINT.to_owned(),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        PayloadHandle, STREAM_CHECKPOINT_DELIVERY_WORKFLOW_AWAITABLE,
        STREAM_CHECKPOINT_POLICY_NAMED, STREAM_CONSISTENCY_EVENTUAL, STREAM_OPERATOR_FILTER,
        STREAM_OPERATOR_MATERIALIZE, STREAM_OPERATOR_REDUCE, STREAM_OPERATOR_WINDOW,
        STREAM_QUERY_MODE_BY_KEY, STREAM_QUERY_MODE_PREFIX_SCAN, STREAM_REDUCER_AVG,
        STREAM_REDUCER_SUM,
    };

    fn keyed_rollup_job() -> CompiledStreamJob {
        CompiledStreamJob {
            name: "keyed-rollup".to_owned(),
            runtime: STREAM_RUNTIME_KEYED_ROLLUP.to_owned(),
            source: crate::CompiledStreamSource {
                kind: "topic".to_owned(),
                name: Some("payments".to_owned()),
                binding: Some("payments".to_owned()),
                config: Some(json!({ "topic": "payments" })),
            },
            key_by: Some("accountId".to_owned()),
            states: Vec::new(),
            operators: vec![
                crate::CompiledStreamOperator {
                    kind: STREAM_OPERATOR_REDUCE.to_owned(),
                    operator_id: Some("sum".to_owned()),
                    name: Some("sum".to_owned()),
                    inputs: Vec::new(),
                    outputs: Vec::new(),
                    state_ids: Vec::new(),
                    config: Some(json!({
                        "reducer": STREAM_REDUCER_SUM,
                        "valueField": "amount",
                        "outputField": "totalAmount",
                    })),
                },
                crate::CompiledStreamOperator {
                    kind: STREAM_OPERATOR_EMIT_CHECKPOINT.to_owned(),
                    operator_id: Some("hourly-rollup-ready".to_owned()),
                    name: Some("hourly-rollup-ready".to_owned()),
                    inputs: Vec::new(),
                    outputs: Vec::new(),
                    state_ids: Vec::new(),
                    config: Some(json!({ "sequence": 1 })),
                },
            ],
            checkpoint_policy: Some(json!({
                "kind": STREAM_CHECKPOINT_POLICY_NAMED,
                "checkpoints": [{
                    "name": "hourly-rollup-ready",
                    "delivery": STREAM_CHECKPOINT_DELIVERY_WORKFLOW_AWAITABLE,
                    "sequence": 1
                }]
            })),
            views: vec![crate::CompiledStreamView {
                name: "accountTotals".to_owned(),
                consistency: STREAM_CONSISTENCY_STRONG.to_owned(),
                query_mode: STREAM_QUERY_MODE_BY_KEY.to_owned(),
                view_id: None,
                key_field: Some("accountId".to_owned()),
                value_fields: vec!["accountId".to_owned(), "totalAmount".to_owned()],
                supported_consistencies: Vec::new(),
                retention_seconds: None,
            }],
            queries: vec![crate::CompiledStreamQuery {
                name: "accountTotals".to_owned(),
                view_name: "accountTotals".to_owned(),
                consistency: STREAM_CONSISTENCY_STRONG.to_owned(),
                query_id: None,
                arg_fields: vec!["accountId".to_owned()],
            }],
            classification: None,
            metadata: None,
        }
    }

    fn aggregate_v2_job() -> CompiledStreamJob {
        CompiledStreamJob {
            name: "fraud-detector".to_owned(),
            runtime: STREAM_RUNTIME_AGGREGATE_V2.to_owned(),
            source: crate::CompiledStreamSource {
                kind: "topic".to_owned(),
                name: Some("payments".to_owned()),
                binding: Some("payments".to_owned()),
                config: Some(json!({ "topic": "payments" })),
            },
            key_by: Some("accountId".to_owned()),
            states: vec![
                crate::CompiledStreamState {
                    id: "risk".to_owned(),
                    kind: "keyed".to_owned(),
                    key_fields: vec!["accountId".to_owned()],
                    value_fields: vec!["avgRisk".to_owned()],
                    retention_seconds: Some(3600),
                    config: Some(json!({ "reducer": STREAM_REDUCER_AVG })),
                },
                crate::CompiledStreamState {
                    id: "minute-window".to_owned(),
                    kind: "window".to_owned(),
                    key_fields: vec!["accountId".to_owned(), "windowStart".to_owned()],
                    value_fields: vec!["avgRisk".to_owned()],
                    retention_seconds: Some(3600),
                    config: Some(json!({ "mode": "tumbling", "size": "1m" })),
                },
            ],
            operators: vec![
                crate::CompiledStreamOperator {
                    kind: STREAM_OPERATOR_FILTER.to_owned(),
                    operator_id: Some("filter-valid".to_owned()),
                    name: Some("filter-valid".to_owned()),
                    inputs: vec!["source:payments".to_owned()],
                    outputs: vec!["filtered".to_owned()],
                    state_ids: Vec::new(),
                    config: Some(json!({ "predicate": "amount > 0" })),
                },
                crate::CompiledStreamOperator {
                    kind: STREAM_OPERATOR_WINDOW.to_owned(),
                    operator_id: Some("minute-window".to_owned()),
                    name: Some("minute-window".to_owned()),
                    inputs: vec!["filtered".to_owned()],
                    outputs: vec!["windowed".to_owned()],
                    state_ids: vec!["minute-window".to_owned()],
                    config: Some(json!({ "mode": "tumbling", "size": "1m" })),
                },
                crate::CompiledStreamOperator {
                    kind: STREAM_OPERATOR_AGGREGATE.to_owned(),
                    operator_id: Some("avg-risk".to_owned()),
                    name: Some("avg-risk".to_owned()),
                    inputs: vec!["windowed".to_owned()],
                    outputs: vec!["risk-view".to_owned()],
                    state_ids: vec!["risk".to_owned()],
                    config: Some(json!({
                        "reducer": STREAM_REDUCER_AVG,
                        "valueField": "risk",
                        "outputField": "avgRisk",
                    })),
                },
                crate::CompiledStreamOperator {
                    kind: STREAM_OPERATOR_MATERIALIZE.to_owned(),
                    operator_id: Some("materialize-risk".to_owned()),
                    name: Some("materialize-risk".to_owned()),
                    inputs: vec!["risk-view".to_owned()],
                    outputs: vec!["riskScores".to_owned()],
                    state_ids: vec!["risk".to_owned()],
                    config: Some(json!({ "view": "riskScores" })),
                },
            ],
            checkpoint_policy: Some(json!({
                "kind": STREAM_CHECKPOINT_POLICY_NAMED,
                "checkpoints": [{
                    "name": "minute-closed",
                    "delivery": STREAM_CHECKPOINT_DELIVERY_WORKFLOW_AWAITABLE,
                    "sequence": 1
                }]
            })),
            views: vec![crate::CompiledStreamView {
                name: "riskScores".to_owned(),
                consistency: STREAM_CONSISTENCY_STRONG.to_owned(),
                query_mode: STREAM_QUERY_MODE_PREFIX_SCAN.to_owned(),
                view_id: Some("risk-scores".to_owned()),
                key_field: Some("accountId".to_owned()),
                value_fields: vec!["accountId".to_owned(), "avgRisk".to_owned()],
                supported_consistencies: vec![
                    STREAM_CONSISTENCY_STRONG.to_owned(),
                    STREAM_CONSISTENCY_EVENTUAL.to_owned(),
                ],
                retention_seconds: Some(3600),
            }],
            queries: vec![crate::CompiledStreamQuery {
                name: "riskScoresByKey".to_owned(),
                view_name: "riskScores".to_owned(),
                consistency: STREAM_CONSISTENCY_STRONG.to_owned(),
                query_id: None,
                arg_fields: vec!["accountId".to_owned()],
            }],
            classification: Some("fast_lane".to_owned()),
            metadata: None,
        }
    }

    #[test]
    fn keyed_rollup_lowers_to_partitioned_dataflow_plan() -> Result<()> {
        let plan = keyed_rollup_job().dataflow_plan("plan-a")?;
        assert_eq!(plan.runtime_contract, STREAMS_DATAFLOW_V1_CONTRACT);
        assert_eq!(plan.edges.len(), 1);
        assert_eq!(plan.edges[0].kind, DATAFLOW_EDGE_EXCHANGE);
        assert_eq!(plan.partitioning.key_field.as_deref(), Some("accountId"));
        assert_eq!(plan.checkpoints[0].checkpoint_name, "hourly-rollup-ready");
        Ok(())
    }

    #[test]
    fn aggregate_v2_lowers_to_partitioned_dataflow_plan() -> Result<()> {
        let plan = aggregate_v2_job().dataflow_plan("plan-b")?;
        assert_eq!(plan.runtime_contract, STREAMS_DATAFLOW_V1_CONTRACT);
        assert_eq!(plan.regions.len(), 2);
        assert_eq!(plan.regions[0].stage, DATAFLOW_REGION_PRE_KEY);
        assert!(
            plan.regions[1]
                .operators
                .iter()
                .any(|operator| operator.kind == STREAM_OPERATOR_WINDOW)
        );
        Ok(())
    }

    #[test]
    fn throughput_run_lowers_to_external_work_dataflow_plan() {
        let command = StartThroughputRunCommand {
            dedupe_key: "throughput-start:test".to_owned(),
            bridge_request_id: "throughput-bridge:tenant:instance:run:batch".to_owned(),
            bridge_protocol_version: "2026-03-13".to_owned(),
            bridge_operation_kind: "bulk_run".to_owned(),
            tenant_id: "tenant".to_owned(),
            definition_id: "def".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("hash".to_owned()),
            instance_id: "instance".to_owned(),
            run_id: "run".to_owned(),
            batch_id: "batch".to_owned(),
            activity_type: "benchmark.echo".to_owned(),
            activity_capabilities: None,
            task_queue: "bulk".to_owned(),
            state: None,
            chunk_size: 64,
            max_attempts: 3,
            retry_delay_ms: 500,
            total_items: 1024,
            aggregation_group_count: 8,
            execution_policy: Some("parallel".to_owned()),
            reducer: Some("count".to_owned()),
            throughput_backend: "stream-v2".to_owned(),
            throughput_backend_version: "2.0.0".to_owned(),
            routing_reason: "stream_v2_selected".to_owned(),
            admission_policy_version: "2026-03-13.1".to_owned(),
            input_handle: PayloadHandle::Inline { key: "input".to_owned() },
            result_handle: PayloadHandle::Inline { key: "result".to_owned() },
        };
        let plan = command.dataflow_plan();
        assert_eq!(plan.runtime_contract, STREAMS_DATAFLOW_V1_CONTRACT);
        assert!(plan.external_work.is_some());
        assert_eq!(plan.partitioning.mode, DATAFLOW_PARTITIONING_AGGREGATION_GROUP);
    }

    #[test]
    fn strong_read_metadata_round_trips() -> Result<()> {
        let metadata = StrongReadMetadata {
            consistency: STREAM_CONSISTENCY_STRONG.to_owned(),
            owner_epoch: 7,
            last_durable_position: BTreeMap::from([(0, 41), (1, 87)]),
            last_source_frontier: vec![DataflowSourceFrontier {
                source_partition_id: 0,
                last_applied_offset: Some(41),
                next_offset: Some(42),
                high_watermark: Some(44),
            }],
            last_sealed_checkpoint: Some(StrongReadCheckpointRef {
                checkpoint_name: "minute-closed".to_owned(),
                checkpoint_sequence: 9,
                sealed_at: Some(Utc::now()),
            }),
        };
        let encoded = serde_json::to_value(&metadata)?;
        let round_tripped: StrongReadMetadata = serde_json::from_value(encoded)?;
        assert_eq!(round_tripped.owner_epoch, 7);
        assert_eq!(round_tripped.last_durable_position.get(&1), Some(&87));
        Ok(())
    }
}
