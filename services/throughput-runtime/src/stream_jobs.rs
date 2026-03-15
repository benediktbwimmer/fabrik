use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use fabrik_store::{
    StreamJobBridgeHandleRecord, StreamJobCheckpointRecord, StreamJobQueryRecord,
    StreamJobViewRecord, ThroughputProjectionEvent, WorkflowStore,
};
use fabrik_throughput::{
    CompiledStreamJob, CompiledStreamOperator, CompiledStreamQuery, CompiledStreamSource,
    CompiledStreamState, CompiledStreamView, STREAM_CONSISTENCY_STRONG, STREAM_JOB_KEYED_ROLLUP,
    STREAM_QUERY_MODE_BY_KEY, STREAM_RUNTIME_KEYED_ROLLUP, StreamJobBridgeHandleStatus,
    StreamJobOriginKind, StreamJobQueryConsistency, ThroughputChangelogEntry,
    ThroughputChangelogPayload,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::local_state::{LocalStreamJobDispatchBatch, LocalStreamJobDispatchItem};
use crate::{
    AppState, LocalThroughputState, StreamChangelogRecord, StreamJobActivationResult,
    StreamJobTerminalResult, StreamProjectionRecord, owner_local_state_for_stream_key,
    streams_changelog_entry_from_throughput, throughput_partition_for_stream_key,
    throughput_partition_key,
};

pub(crate) const KEYED_ROLLUP_JOB_NAME: &str = STREAM_JOB_KEYED_ROLLUP;
pub(crate) const KEYED_ROLLUP_CHECKPOINT_NAME: &str = "initial-rollup-ready";
pub(crate) const KEYED_ROLLUP_CHECKPOINT_SEQUENCE: i64 = 1;

#[derive(Debug, Clone)]
pub(crate) struct StreamJobMaterializationOutcome {
    pub checkpoint: Option<StreamJobCheckpointRecord>,
    pub terminal: Option<StreamJobTerminalResult>,
}

#[derive(Debug, Clone)]
pub(crate) struct StreamJobActivationPlan {
    pub execution_planned: Option<StreamChangelogRecord>,
    pub partition_work: Vec<StreamJobPartitionWork>,
    pub terminalized: Option<StreamChangelogRecord>,
}

#[derive(Debug, Clone)]
pub(crate) struct StreamJobPartitionWork {
    pub batch_id: String,
    pub stream_partition_id: i32,
    pub routing_key: String,
    pub key_field: String,
    pub output_field: String,
    pub view_name: String,
    pub checkpoint_name: String,
    pub checkpoint_sequence: i64,
    pub owner_epoch: u64,
    pub occurred_at: DateTime<Utc>,
    pub items: Vec<StreamJobPartitionItem>,
    pub is_final_partition_batch: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct StreamJobPartitionItem {
    pub logical_key: String,
    pub value: f64,
}

#[derive(Debug, Deserialize)]
struct BoundedInputEnvelope {
    kind: Option<String>,
    items: Vec<Value>,
}

fn value_property<'a>(value: &'a Value, camel: &str, snake: &str) -> Option<&'a Value> {
    value.get(camel).or_else(|| value.get(snake))
}

fn parse_compiled_source(value: &Value) -> Result<CompiledStreamSource> {
    Ok(CompiledStreamSource {
        kind: value_property(value, "kind", "kind")
            .and_then(Value::as_str)
            .context("stream job source.kind must be a string")?
            .to_owned(),
        name: value_property(value, "name", "name").and_then(Value::as_str).map(str::to_owned),
        binding: value_property(value, "binding", "binding")
            .and_then(Value::as_str)
            .map(str::to_owned),
        config: value_property(value, "config", "config").cloned(),
    })
}

fn parse_compiled_operator(value: &Value) -> Result<CompiledStreamOperator> {
    Ok(CompiledStreamOperator {
        kind: value_property(value, "kind", "kind")
            .and_then(Value::as_str)
            .context("stream job operator.kind must be a string")?
            .to_owned(),
        operator_id: value_property(value, "operatorId", "operator_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        name: value_property(value, "name", "name").and_then(Value::as_str).map(str::to_owned),
        inputs: value_property(value, "inputs", "inputs")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        outputs: value_property(value, "outputs", "outputs")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        state_ids: value_property(value, "stateIds", "state_ids")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        config: value_property(value, "config", "config").cloned(),
    })
}

fn parse_compiled_state(value: &Value) -> Result<CompiledStreamState> {
    Ok(CompiledStreamState {
        id: value_property(value, "id", "id")
            .and_then(Value::as_str)
            .context("stream job state.id must be a string")?
            .to_owned(),
        kind: value_property(value, "kind", "kind")
            .and_then(Value::as_str)
            .context("stream job state.kind must be a string")?
            .to_owned(),
        key_fields: value_property(value, "keyFields", "key_fields")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        value_fields: value_property(value, "valueFields", "value_fields")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        retention_seconds: value_property(value, "retentionSeconds", "retention_seconds")
            .and_then(Value::as_u64),
        config: value_property(value, "config", "config").cloned(),
    })
}

fn parse_compiled_view(value: &Value) -> Result<CompiledStreamView> {
    Ok(CompiledStreamView {
        name: value_property(value, "name", "name")
            .and_then(Value::as_str)
            .context("stream job view.name must be a string")?
            .to_owned(),
        consistency: value_property(value, "consistency", "consistency")
            .and_then(Value::as_str)
            .unwrap_or(StreamJobQueryConsistency::Strong.as_str())
            .to_owned(),
        query_mode: value_property(value, "queryMode", "query_mode")
            .and_then(Value::as_str)
            .unwrap_or("by_key")
            .to_owned(),
        view_id: value_property(value, "viewId", "view_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        key_field: value_property(value, "keyField", "key_field")
            .and_then(Value::as_str)
            .map(str::to_owned),
        value_fields: value_property(value, "valueFields", "value_fields")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        supported_consistencies: value_property(
            value,
            "supportedConsistencies",
            "supported_consistencies",
        )
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect(),
        retention_seconds: value_property(value, "retentionSeconds", "retention_seconds")
            .and_then(Value::as_u64),
    })
}

fn parse_compiled_query(value: &Value) -> Result<CompiledStreamQuery> {
    Ok(CompiledStreamQuery {
        name: value_property(value, "name", "name")
            .and_then(Value::as_str)
            .context("stream job query.name must be a string")?
            .to_owned(),
        view_name: value_property(value, "viewName", "view_name")
            .and_then(Value::as_str)
            .context("stream job query.viewName must be a string")?
            .to_owned(),
        consistency: value_property(value, "consistency", "consistency")
            .and_then(Value::as_str)
            .unwrap_or(StreamJobQueryConsistency::Strong.as_str())
            .to_owned(),
        query_id: value_property(value, "queryId", "query_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        arg_fields: value_property(value, "argFields", "arg_fields")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
    })
}

fn parse_compiled_views(value: Option<&Value>) -> Result<Vec<CompiledStreamView>> {
    value.and_then(Value::as_array).into_iter().flatten().map(parse_compiled_view).collect()
}

fn checkpoint_name_from_policy_value(policy: Option<&Value>) -> Option<String> {
    policy
        .and_then(|value| value.get("checkpoints"))
        .and_then(Value::as_array)
        .and_then(|checkpoints| checkpoints.first())
        .and_then(|checkpoint| checkpoint.get("name"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

pub(crate) fn keyed_rollup_checkpoint_name_from_config_ref(
    config_ref: Option<&str>,
    checkpoint_policy: Option<&Value>,
) -> String {
    if let Some(name) = checkpoint_name_from_policy_value(checkpoint_policy) {
        return name;
    }

    let Some(config) = config_ref.and_then(|value| serde_json::from_str::<Value>(value).ok())
    else {
        return KEYED_ROLLUP_CHECKPOINT_NAME.to_owned();
    };

    checkpoint_name_from_policy_value(value_property(
        &config,
        "checkpointPolicy",
        "checkpoint_policy",
    ))
    .or_else(|| {
        value_property(&config, "checkpoint", "checkpoint")
            .and_then(Value::as_str)
            .map(str::to_owned)
    })
    .unwrap_or_else(|| KEYED_ROLLUP_CHECKPOINT_NAME.to_owned())
}

fn default_keyed_rollup_compiled_job(handle: &StreamJobBridgeHandleRecord) -> CompiledStreamJob {
    let checkpoint_name = keyed_rollup_checkpoint_name_from_config_ref(
        handle.config_ref.as_deref(),
        handle.checkpoint_policy.as_ref(),
    );
    CompiledStreamJob {
        name: handle.job_name.clone(),
        runtime: STREAM_RUNTIME_KEYED_ROLLUP.to_owned(),
        source: CompiledStreamSource {
            kind: fabrik_throughput::STREAM_SOURCE_BOUNDED_INPUT.to_owned(),
            name: None,
            binding: None,
            config: None,
        },
        key_by: Some("accountId".to_owned()),
        states: vec![],
        operators: vec![
            CompiledStreamOperator {
                kind: fabrik_throughput::STREAM_OPERATOR_REDUCE.to_owned(),
                operator_id: None,
                name: Some("sum-account-totals".to_owned()),
                inputs: vec![],
                outputs: vec![],
                state_ids: vec![],
                config: Some(json!({
                    "reducer": "sum",
                    "valueField": "amount",
                    "outputField": "totalAmount"
                })),
            },
            CompiledStreamOperator {
                kind: fabrik_throughput::STREAM_OPERATOR_EMIT_CHECKPOINT.to_owned(),
                operator_id: None,
                name: Some(checkpoint_name.clone()),
                inputs: vec![],
                outputs: vec![],
                state_ids: vec![],
                config: Some(json!({
                    "sequence": KEYED_ROLLUP_CHECKPOINT_SEQUENCE
                })),
            },
        ],
        checkpoint_policy: Some(json!({
            "kind": fabrik_throughput::STREAM_CHECKPOINT_POLICY_NAMED,
            "checkpoints": [
                {
                    "name": checkpoint_name,
                    "delivery": fabrik_throughput::STREAM_CHECKPOINT_DELIVERY_WORKFLOW_AWAITABLE,
                    "sequence": KEYED_ROLLUP_CHECKPOINT_SEQUENCE
                }
            ]
        })),
        views: vec![CompiledStreamView {
            name: "accountTotals".to_owned(),
            consistency: STREAM_CONSISTENCY_STRONG.to_owned(),
            query_mode: STREAM_QUERY_MODE_BY_KEY.to_owned(),
            view_id: None,
            key_field: Some("accountId".to_owned()),
            value_fields: vec![
                "accountId".to_owned(),
                "totalAmount".to_owned(),
                "asOfCheckpoint".to_owned(),
            ],
            supported_consistencies: vec![],
            retention_seconds: None,
        }],
        queries: vec![CompiledStreamQuery {
            name: "accountTotals".to_owned(),
            view_name: "accountTotals".to_owned(),
            consistency: STREAM_CONSISTENCY_STRONG.to_owned(),
            query_id: None,
            arg_fields: vec![],
        }],
        classification: None,
        metadata: None,
    }
}

fn parse_compiled_job_from_config_ref(
    handle: &StreamJobBridgeHandleRecord,
) -> Result<Option<CompiledStreamJob>> {
    let Some(config) =
        handle.config_ref.as_deref().and_then(|value| serde_json::from_str::<Value>(value).ok())
    else {
        return Ok(None);
    };

    let Some(runtime) =
        value_property(&config, "runtime", "runtime").and_then(Value::as_str).map(str::to_owned)
    else {
        return Ok(None);
    };
    let Some(source_value) = value_property(&config, "source", "source") else {
        return Ok(None);
    };

    let operators = value_property(&config, "operators", "operators")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(parse_compiled_operator)
        .collect::<Result<Vec<_>>>()?;
    let states = value_property(&config, "states", "states")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(parse_compiled_state)
        .collect::<Result<Vec<_>>>()?;
    let queries = value_property(&config, "queries", "queries")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(parse_compiled_query)
        .collect::<Result<Vec<_>>>()?;
    let views = parse_compiled_views(
        value_property(&config, "views", "views").or(handle.view_definitions.as_ref()),
    )?;

    Ok(Some(CompiledStreamJob {
        name: value_property(&config, "name", "name")
            .and_then(Value::as_str)
            .unwrap_or(&handle.job_name)
            .to_owned(),
        runtime,
        source: parse_compiled_source(source_value)?,
        key_by: value_property(&config, "keyBy", "key_by")
            .and_then(Value::as_str)
            .map(str::to_owned),
        states,
        operators,
        checkpoint_policy: value_property(&config, "checkpointPolicy", "checkpoint_policy")
            .cloned()
            .or_else(|| handle.checkpoint_policy.clone()),
        views,
        queries,
        classification: value_property(&config, "classification", "classification")
            .and_then(Value::as_str)
            .map(str::to_owned),
        metadata: value_property(&config, "metadata", "metadata").cloned(),
    }))
}

async fn resolve_stream_job_definition(
    store: Option<&WorkflowStore>,
    handle: &StreamJobBridgeHandleRecord,
) -> Result<Option<CompiledStreamJob>> {
    if let Some(store) = store {
        if matches!(handle.parsed_origin_kind(), Some(StreamJobOriginKind::Standalone)) {
            if let Some(version) = handle.definition_version
                && let Some(artifact) = store
                    .get_stream_artifact_version(&handle.tenant_id, &handle.definition_id, version)
                    .await?
            {
                artifact.validate()?;
                return Ok(Some(artifact.job));
            }
            if let Some(artifact) =
                store.get_latest_stream_artifact(&handle.tenant_id, &handle.definition_id).await?
            {
                artifact.validate()?;
                return Ok(Some(artifact.job));
            }
        }

        if let Some(artifact) = store
            .get_latest_stream_artifact_for_job_name(&handle.tenant_id, &handle.job_name)
            .await?
        {
            artifact.validate()?;
            return Ok(Some(artifact.job));
        }
    }

    if let Some(job) = parse_compiled_job_from_config_ref(handle)? {
        job.validate_supported_contract()?;
        return Ok(Some(job));
    }

    if handle.job_name == KEYED_ROLLUP_JOB_NAME {
        let job = default_keyed_rollup_compiled_job(handle);
        job.validate_supported_contract()?;
        return Ok(Some(job));
    }

    Ok(None)
}

fn parse_bounded_input_items(input_ref: &str) -> Result<Vec<Value>> {
    let value: Value =
        serde_json::from_str(input_ref).context("failed to decode stream job input_ref JSON")?;
    if let Ok(items) = serde_json::from_value::<Vec<Value>>(value.clone()) {
        return Ok(items);
    }
    let envelope: BoundedInputEnvelope = serde_json::from_value(value)
        .context("stream job input must be an item array or bounded_items envelope")?;
    if envelope.kind.as_deref().is_some_and(|kind| kind != "bounded_items") {
        anyhow::bail!("stream job input kind must be bounded_items");
    }
    Ok(envelope.items)
}

fn string_field(item: &Value, field: &str) -> Result<String> {
    item.get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .with_context(|| format!("stream job item field {field} must be a string"))
}

fn numeric_field(item: &Value, field: &str) -> Result<f64> {
    item.get(field)
        .and_then(Value::as_f64)
        .with_context(|| format!("stream job item field {field} must be a number"))
}

fn terminal_output(handle: &StreamJobBridgeHandleRecord) -> Value {
    json!({
        "jobId": handle.job_id,
        "jobName": handle.job_name,
        "status": "completed",
    })
}

fn execution_planned_changelog_record(
    handle: &StreamJobBridgeHandleRecord,
    kernel: &fabrik_throughput::CompiledKeyedRollupKernel,
    owner_epoch: u64,
    input_item_count: u64,
    materialized_key_count: u64,
    active_partitions: Vec<i32>,
    planned_at: DateTime<Utc>,
) -> StreamChangelogRecord {
    let key = throughput_partition_key(&handle.job_id, 0);
    let entry = ThroughputChangelogEntry {
        entry_id: Uuid::now_v7(),
        occurred_at: planned_at,
        partition_key: key.clone(),
        payload: ThroughputChangelogPayload::StreamJobExecutionPlanned {
            handle_id: handle.handle_id.clone(),
            job_id: handle.job_id.clone(),
            job_name: handle.job_name.clone(),
            view_name: kernel.view_name.clone(),
            checkpoint_name: kernel.checkpoint_name.clone(),
            checkpoint_sequence: kernel.checkpoint_sequence,
            input_item_count,
            materialized_key_count,
            active_partitions,
            owner_epoch,
            planned_at,
        },
    };
    StreamChangelogRecord { key, entry, partition_mirror_applied: false }
}

fn view_update_changelog_record(
    handle: &StreamJobBridgeHandleRecord,
    view_name: &str,
    routing_key: &str,
    logical_key: &str,
    output: Value,
    checkpoint_sequence: i64,
    updated_at: DateTime<Utc>,
) -> StreamChangelogRecord {
    let key = throughput_partition_key(routing_key, 0);
    let entry = ThroughputChangelogEntry {
        entry_id: Uuid::now_v7(),
        occurred_at: updated_at,
        partition_key: key.clone(),
        payload: ThroughputChangelogPayload::StreamJobViewUpdated {
            handle_id: handle.handle_id.clone(),
            job_id: handle.job_id.clone(),
            view_name: view_name.to_owned(),
            logical_key: logical_key.to_owned(),
            output,
            checkpoint_sequence,
            updated_at,
        },
    };
    StreamChangelogRecord { key, entry, partition_mirror_applied: false }
}

fn checkpoint_reached_changelog_record(
    handle: &StreamJobBridgeHandleRecord,
    routing_key: &str,
    checkpoint_name: &str,
    checkpoint_sequence: i64,
    stream_partition_id: i32,
    owner_epoch: u64,
    reached_at: DateTime<Utc>,
) -> StreamChangelogRecord {
    let key = throughput_partition_key(routing_key, 0);
    let entry = ThroughputChangelogEntry {
        entry_id: Uuid::now_v7(),
        occurred_at: reached_at,
        partition_key: key.clone(),
        payload: ThroughputChangelogPayload::StreamJobCheckpointReached {
            handle_id: handle.handle_id.clone(),
            job_id: handle.job_id.clone(),
            checkpoint_name: checkpoint_name.to_owned(),
            checkpoint_sequence,
            stream_partition_id,
            owner_epoch,
            reached_at,
        },
    };
    StreamChangelogRecord { key, entry, partition_mirror_applied: false }
}

fn terminalized_changelog_record(
    handle: &StreamJobBridgeHandleRecord,
    owner_epoch: u64,
    terminal_at: DateTime<Utc>,
) -> StreamChangelogRecord {
    let key = throughput_partition_key(&handle.job_id, 0);
    let entry = ThroughputChangelogEntry {
        entry_id: Uuid::now_v7(),
        occurred_at: terminal_at,
        partition_key: key.clone(),
        payload: ThroughputChangelogPayload::StreamJobTerminalized {
            handle_id: handle.handle_id.clone(),
            job_id: handle.job_id.clone(),
            owner_epoch,
            status: StreamJobBridgeHandleStatus::Completed.as_str().to_owned(),
            output: Some(terminal_output(handle)),
            error: None,
            terminal_at,
        },
    };
    StreamChangelogRecord { key, entry, partition_mirror_applied: false }
}

fn dispatch_batch_id(
    handle: &StreamJobBridgeHandleRecord,
    stream_partition_id: i32,
    batch_index: usize,
) -> String {
    format!("{}:{stream_partition_id}:{batch_index}", handle.handle_id)
}

pub(crate) fn local_dispatch_batch_from_work(
    work: &StreamJobPartitionWork,
) -> LocalStreamJobDispatchBatch {
    LocalStreamJobDispatchBatch {
        batch_id: work.batch_id.clone(),
        stream_partition_id: work.stream_partition_id,
        routing_key: work.routing_key.clone(),
        key_field: work.key_field.clone(),
        output_field: work.output_field.clone(),
        view_name: work.view_name.clone(),
        checkpoint_name: work.checkpoint_name.clone(),
        checkpoint_sequence: work.checkpoint_sequence,
        owner_epoch: work.owner_epoch,
        occurred_at: work.occurred_at,
        items: work
            .items
            .iter()
            .map(|item| LocalStreamJobDispatchItem {
                logical_key: item.logical_key.clone(),
                value: item.value,
            })
            .collect(),
        is_final_partition_batch: work.is_final_partition_batch,
    }
}

fn work_from_local_dispatch_batch(batch: &LocalStreamJobDispatchBatch) -> StreamJobPartitionWork {
    StreamJobPartitionWork {
        batch_id: batch.batch_id.clone(),
        stream_partition_id: batch.stream_partition_id,
        routing_key: batch.routing_key.clone(),
        key_field: batch.key_field.clone(),
        output_field: batch.output_field.clone(),
        view_name: batch.view_name.clone(),
        checkpoint_name: batch.checkpoint_name.clone(),
        checkpoint_sequence: batch.checkpoint_sequence,
        owner_epoch: batch.owner_epoch,
        occurred_at: batch.occurred_at,
        items: batch
            .items
            .iter()
            .map(|item| StreamJobPartitionItem {
                logical_key: item.logical_key.clone(),
                value: item.value,
            })
            .collect(),
        is_final_partition_batch: batch.is_final_partition_batch,
    }
}

pub(crate) async fn plan_stream_job_activation(
    local_state: &LocalThroughputState,
    store: Option<&WorkflowStore>,
    handle: &StreamJobBridgeHandleRecord,
    throughput_partitions: i32,
    owner_epoch: Option<u64>,
    occurred_at: DateTime<Utc>,
) -> Result<Option<StreamJobActivationPlan>> {
    const STREAM_JOB_OWNER_BATCH_MAX_ITEMS: usize = 64;

    let Some(job) = resolve_stream_job_definition(store, handle).await? else {
        return Ok(None);
    };
    let Some(plan) = job.keyed_rollup_kernel()? else {
        return Ok(None);
    };
    let owner_epoch = owner_epoch.unwrap_or(1);

    if let Some(runtime_state) = local_state.load_stream_job_runtime_state(&handle.handle_id)? {
        if runtime_state.dispatch_cancelled_at.is_some() {
            return Ok(Some(StreamJobActivationPlan {
                execution_planned: None,
                partition_work: Vec::new(),
                terminalized: None,
            }));
        }
        if !runtime_state.dispatch_batches.is_empty() {
            let pending = runtime_state
                .dispatch_batches
                .iter()
                .filter(|batch| {
                    !runtime_state
                        .applied_dispatch_batch_ids
                        .iter()
                        .any(|applied| applied == &batch.batch_id)
                })
                .map(work_from_local_dispatch_batch)
                .collect::<Vec<_>>();
            let terminalized = if pending.is_empty()
                && runtime_state.dispatch_completed_at.is_some()
                && runtime_state.terminal_status.is_none()
            {
                Some(terminalized_changelog_record(
                    handle,
                    runtime_state.stream_owner_epoch,
                    runtime_state.dispatch_completed_at.unwrap_or(occurred_at),
                ))
            } else {
                None
            };
            return Ok(Some(StreamJobActivationPlan {
                execution_planned: None,
                partition_work: pending,
                terminalized,
            }));
        }
    }

    let items = parse_bounded_input_items(&handle.input_ref)?;
    let mut unique_keys = BTreeMap::<String, i32>::new();
    let mut items_by_partition = BTreeMap::<i32, Vec<StreamJobPartitionItem>>::new();
    for item in &items {
        let key = string_field(item, &plan.key_field)?;
        let value = numeric_field(item, &plan.value_field)?;
        let stream_partition_id = throughput_partition_for_stream_key(&key, throughput_partitions);
        unique_keys.entry(key.clone()).or_insert(stream_partition_id);
        items_by_partition
            .entry(stream_partition_id)
            .or_default()
            .push(StreamJobPartitionItem { logical_key: key, value });
    }

    let mut active_partitions = unique_keys.values().copied().collect::<Vec<_>>();
    active_partitions.sort_unstable();
    active_partitions.dedup();
    let execution_planned = execution_planned_changelog_record(
        handle,
        &plan,
        owner_epoch,
        items.len() as u64,
        unique_keys.len() as u64,
        active_partitions.clone(),
        occurred_at,
    );
    let mut partition_work = Vec::new();
    for (stream_partition_id, partition_items) in items_by_partition {
        let routing_key = partition_items
            .first()
            .map(|item| item.logical_key.clone())
            .unwrap_or_else(|| handle.job_id.clone());
        let batch_count = (partition_items.len() + STREAM_JOB_OWNER_BATCH_MAX_ITEMS - 1)
            / STREAM_JOB_OWNER_BATCH_MAX_ITEMS;
        for (batch_index, item_batch) in
            partition_items.chunks(STREAM_JOB_OWNER_BATCH_MAX_ITEMS).enumerate()
        {
            partition_work.push(StreamJobPartitionWork {
                batch_id: dispatch_batch_id(handle, stream_partition_id, batch_index),
                stream_partition_id,
                routing_key: routing_key.clone(),
                key_field: plan.key_field.clone(),
                output_field: plan.output_field.clone(),
                view_name: plan.view_name.clone(),
                checkpoint_name: plan.checkpoint_name.clone(),
                checkpoint_sequence: plan.checkpoint_sequence,
                owner_epoch,
                occurred_at,
                items: item_batch.to_vec(),
                is_final_partition_batch: batch_index + 1 == batch_count,
            });
        }
    }
    Ok(Some(StreamJobActivationPlan {
        execution_planned: Some(execution_planned),
        partition_work,
        terminalized: Some(terminalized_changelog_record(handle, owner_epoch, occurred_at)),
    }))
}

pub(crate) fn apply_stream_job_partition_work_on_local_state(
    local_state: &LocalThroughputState,
    handle: &StreamJobBridgeHandleRecord,
    work: &StreamJobPartitionWork,
    mirror_owner_state: bool,
) -> Result<StreamJobActivationResult> {
    if local_state.load_stream_job_runtime_state(&handle.handle_id)?.is_some_and(|state| {
        state.applied_dispatch_batch_ids.iter().any(|batch_id| batch_id == &work.batch_id)
    }) {
        return Ok(StreamJobActivationResult {
            projection_records: Vec::new(),
            changelog_records: Vec::new(),
        });
    }

    let mut updated_totals = BTreeMap::<String, f64>::new();
    for item in &work.items {
        let baseline_total = if let Some(total) = updated_totals.get(&item.logical_key).copied() {
            total
        } else if let Some(view_state) = local_state.load_stream_job_view_state(
            &handle.handle_id,
            &work.view_name,
            &item.logical_key,
        )? {
            value_property(&view_state.output, &work.output_field, &work.output_field)
                .and_then(Value::as_f64)
                .unwrap_or(0.0)
        } else {
            0.0
        };
        updated_totals.insert(item.logical_key.clone(), baseline_total + item.value);
    }

    let mut projection_records = Vec::with_capacity(updated_totals.len());
    let mut changelog_records =
        Vec::with_capacity(updated_totals.len() + usize::from(work.is_final_partition_batch));
    for (logical_key, total_amount) in updated_totals {
        let mut output = Map::new();
        output.insert(work.key_field.clone(), Value::String(logical_key.clone()));
        output.insert(work.output_field.clone(), json!(total_amount));
        output.insert("asOfCheckpoint".to_owned(), json!(work.checkpoint_sequence));
        let output = Value::Object(output);
        projection_records.push(StreamProjectionRecord {
            key: throughput_partition_key(&logical_key, 0),
            event: ThroughputProjectionEvent::UpsertStreamJobView {
                view: StreamJobViewRecord {
                    tenant_id: handle.tenant_id.clone(),
                    instance_id: handle.instance_id.clone(),
                    run_id: handle.run_id.clone(),
                    job_id: handle.job_id.clone(),
                    handle_id: handle.handle_id.clone(),
                    view_name: work.view_name.clone(),
                    logical_key: logical_key.clone(),
                    output: output.clone(),
                    checkpoint_sequence: work.checkpoint_sequence,
                    updated_at: work.occurred_at,
                },
            },
        });
        changelog_records.push(view_update_changelog_record(
            handle,
            &work.view_name,
            &logical_key,
            &logical_key,
            output,
            work.checkpoint_sequence,
            work.occurred_at,
        ));
    }
    if work.is_final_partition_batch {
        changelog_records.push(checkpoint_reached_changelog_record(
            handle,
            &work.routing_key,
            &work.checkpoint_name,
            work.checkpoint_sequence,
            work.stream_partition_id,
            work.owner_epoch,
            work.occurred_at,
        ));
    }
    if mirror_owner_state {
        let _ = local_state.mark_stream_job_dispatch_batch_applied(
            &handle.handle_id,
            &work.batch_id,
            work.occurred_at,
        )?;
        let stream_entries = changelog_records
            .iter()
            .filter_map(|record| streams_changelog_entry_from_throughput(&record.entry))
            .collect::<Vec<_>>();
        local_state.mirror_streams_changelog_records(&stream_entries)?;
        for record in &mut changelog_records {
            record.partition_mirror_applied = true;
        }
    }
    Ok(StreamJobActivationResult { projection_records, changelog_records })
}

pub(crate) async fn activate_stream_job_on_local_state(
    store: Option<&WorkflowStore>,
    local_state: &LocalThroughputState,
    handle: &StreamJobBridgeHandleRecord,
    throughput_partitions: i32,
    owner_epoch: Option<u64>,
    occurred_at: DateTime<Utc>,
) -> Result<Option<StreamJobActivationResult>> {
    let Some(plan) = plan_stream_job_activation(
        local_state,
        store,
        handle,
        throughput_partitions,
        owner_epoch,
        occurred_at,
    )
    .await?
    else {
        return Ok(None);
    };

    let mut projection_records = Vec::new();
    let mut changelog_records = Vec::with_capacity(plan.partition_work.len().saturating_add(2));
    if let Some(execution_planned) = plan.execution_planned.clone() {
        let Some(streams_entry) = streams_changelog_entry_from_throughput(&execution_planned.entry)
        else {
            anyhow::bail!("failed to convert stream job execution plan to streams changelog entry");
        };
        local_state.mirror_streams_changelog_entry(&streams_entry)?;
        local_state.replace_stream_job_dispatch_manifest(
            &handle.handle_id,
            plan.partition_work.iter().map(local_dispatch_batch_from_work).collect(),
            execution_planned.entry.occurred_at,
        )?;
        changelog_records.push(execution_planned);
    }
    for partition_work in &plan.partition_work {
        if handle.cancellation_requested_at.is_some() {
            local_state.mark_stream_job_dispatch_cancelled(
                &handle.handle_id,
                handle.cancellation_requested_at.unwrap_or(occurred_at),
            )?;
            break;
        }
        let applied = apply_stream_job_partition_work_on_local_state(
            local_state,
            handle,
            partition_work,
            true,
        )?;
        projection_records.extend(applied.projection_records);
        changelog_records.extend(applied.changelog_records);
    }
    if let Some(terminalized) = plan.terminalized {
        if local_state.load_stream_job_runtime_state(&handle.handle_id)?.is_some_and(|state| {
            state.dispatch_completed_at.is_some() && state.dispatch_cancelled_at.is_none()
        }) {
            changelog_records.push(terminalized);
        }
    }
    Ok(Some(StreamJobActivationResult { projection_records, changelog_records }))
}

pub(crate) fn materialization_outcome_from_local_state(
    local_state: &LocalThroughputState,
    handle: &StreamJobBridgeHandleRecord,
) -> Result<Option<StreamJobMaterializationOutcome>> {
    let Some(runtime_state) = local_state.load_stream_job_runtime_state(&handle.handle_id)? else {
        return Ok(None);
    };
    let checkpoint_states = local_state
        .load_all_stream_job_checkpoints()?
        .into_iter()
        .filter(|state| {
            state.handle_id == handle.handle_id
                && state.checkpoint_name == runtime_state.checkpoint_name
                && runtime_state.active_partitions.contains(&state.stream_partition_id)
        })
        .collect::<Vec<_>>();
    let checkpoint = if checkpoint_states.len() == runtime_state.active_partitions.len() {
        let reached_at = checkpoint_states
            .iter()
            .map(|state| state.reached_at)
            .max()
            .unwrap_or(runtime_state.updated_at);
        let owner_epoch = checkpoint_states
            .iter()
            .map(|state| state.stream_owner_epoch)
            .max()
            .unwrap_or(runtime_state.stream_owner_epoch);
        Some(StreamJobCheckpointRecord {
            workflow_event_id: Uuid::new_v5(
                &Uuid::NAMESPACE_URL,
                format!(
                    "stream-job-checkpoint:{}:{}:{}",
                    handle.handle_id,
                    runtime_state.checkpoint_name,
                    runtime_state.checkpoint_sequence
                )
                .as_bytes(),
            ),
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            tenant_id: handle.tenant_id.clone(),
            instance_id: handle.instance_id.clone(),
            run_id: handle.run_id.clone(),
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            bridge_request_id: handle.bridge_request_id.clone(),
            await_request_id: format!(
                "stream-job-reached:{}:{}:{}",
                handle.handle_id, runtime_state.checkpoint_name, runtime_state.checkpoint_sequence
            ),
            checkpoint_name: runtime_state.checkpoint_name.clone(),
            checkpoint_sequence: Some(runtime_state.checkpoint_sequence),
            status: fabrik_throughput::StreamJobCheckpointStatus::Reached.as_str().to_owned(),
            workflow_owner_epoch: handle.workflow_owner_epoch,
            stream_owner_epoch: Some(owner_epoch),
            reached_at: Some(reached_at),
            output: Some(json!({
                "jobId": handle.job_id,
                "checkpoint": runtime_state.checkpoint_name,
                "checkpointSequence": runtime_state.checkpoint_sequence,
                "viewName": runtime_state.view_name,
                "activePartitions": runtime_state.active_partitions,
            })),
            accepted_at: None,
            cancelled_at: None,
            created_at: runtime_state.planned_at,
            updated_at: reached_at,
        })
    } else {
        None
    };
    let terminal = if checkpoint.is_none() {
        None
    } else {
        match runtime_state.terminal_status.as_deref() {
            Some(status) if status == StreamJobBridgeHandleStatus::Completed.as_str() => {
                Some(StreamJobTerminalResult {
                    status: StreamJobBridgeHandleStatus::Completed,
                    output: runtime_state.terminal_output.clone(),
                    error: runtime_state.terminal_error.clone(),
                })
            }
            Some(status) if status == StreamJobBridgeHandleStatus::Failed.as_str() => {
                Some(StreamJobTerminalResult {
                    status: StreamJobBridgeHandleStatus::Failed,
                    output: runtime_state.terminal_output.clone(),
                    error: runtime_state.terminal_error.clone(),
                })
            }
            Some(status) if status == StreamJobBridgeHandleStatus::Cancelled.as_str() => {
                Some(StreamJobTerminalResult {
                    status: StreamJobBridgeHandleStatus::Cancelled,
                    output: runtime_state.terminal_output.clone(),
                    error: runtime_state.terminal_error.clone(),
                })
            }
            Some(other) => {
                anyhow::bail!("unexpected materialized stream job terminal status {other}")
            }
            None => None,
        }
    };
    Ok(Some(StreamJobMaterializationOutcome { checkpoint, terminal }))
}

fn resolve_query_view(
    handle: &StreamJobBridgeHandleRecord,
    query_name: &str,
) -> Result<Option<CompiledStreamView>> {
    if let Some(job) = parse_compiled_job_from_config_ref(handle)? {
        job.validate_supported_contract()?;
        if let Some(query) = job.queries.iter().find(|query| query.name == query_name)
            && let Some(view) = job.views.iter().find(|view| view.name == query.view_name)
        {
            return Ok(Some(view.clone()));
        }
        if let Some(view) = job.views.iter().find(|view| view.name == query_name) {
            return Ok(Some(view.clone()));
        }
    }

    let declared_views = parse_compiled_views(handle.view_definitions.as_ref())?;
    if let Some(view) = declared_views.into_iter().find(|view| view.name == query_name) {
        return Ok(Some(view));
    }

    if handle.job_name == KEYED_ROLLUP_JOB_NAME && query_name == "accountTotals" {
        return Ok(default_keyed_rollup_compiled_job(handle).views.into_iter().next());
    }

    Ok(None)
}

fn query_logical_key(query: &StreamJobQueryRecord, view: &CompiledStreamView) -> Result<String> {
    let args = query.query_args.clone().unwrap_or(Value::Null);
    args.get("key")
        .and_then(Value::as_str)
        .or_else(|| {
            view.key_field.as_deref().and_then(|field| args.get(field)).and_then(Value::as_str)
        })
        .map(str::to_owned)
        .context("stream job query requires args with a key field")
}

pub(crate) async fn build_stream_job_query_output(
    state: &AppState,
    handle: &StreamJobBridgeHandleRecord,
    query: &StreamJobQueryRecord,
) -> Result<Option<Value>> {
    let Some(view) = resolve_query_view(handle, &query.query_name)? else {
        return Ok(None);
    };
    let logical_key = query_logical_key(query, &view)?;
    if matches!(query.parsed_consistency(), Some(StreamJobQueryConsistency::Eventual)) {
        let Some(record) = state
            .store
            .get_stream_job_view_query(
                &handle.tenant_id,
                &handle.instance_id,
                &handle.run_id,
                &handle.job_id,
                &view.name,
                &logical_key,
            )
            .await?
        else {
            anyhow::bail!("{} key {} is not materialized", query.query_name, logical_key);
        };
        let mut output = record.output;
        if let Some(object) = output.as_object_mut() {
            object.insert(
                "consistency".to_owned(),
                Value::String(StreamJobQueryConsistency::Eventual.as_str().to_owned()),
            );
            object.insert(
                "consistencySource".to_owned(),
                Value::String("stream_projection_query".to_owned()),
            );
            object.insert("checkpointSequence".to_owned(), Value::from(record.checkpoint_sequence));
        }
        return Ok(Some(output));
    }
    build_stream_job_query_output_on_local_state(
        owner_local_state_for_stream_key(state, &logical_key),
        handle,
        query,
    )
}

pub(crate) fn build_stream_job_query_output_on_local_state(
    local_state: &LocalThroughputState,
    handle: &StreamJobBridgeHandleRecord,
    query: &StreamJobQueryRecord,
) -> Result<Option<Value>> {
    let Some(view) = resolve_query_view(handle, &query.query_name)? else {
        return Ok(None);
    };
    if view.query_mode != STREAM_QUERY_MODE_BY_KEY {
        anyhow::bail!("unsupported stream job query mode {}", view.query_mode);
    }

    let logical_key = query_logical_key(query, &view)?;
    let Some(view_state) =
        local_state.load_stream_job_view_state(&handle.handle_id, &view.name, &logical_key)?
    else {
        anyhow::bail!("{} key {} is not materialized", query.query_name, logical_key);
    };

    let mut output = view_state.output;
    if let Some(object) = output.as_object_mut() {
        object.insert("consistency".to_owned(), Value::String(query.consistency.clone()));
        object.insert(
            "consistencySource".to_owned(),
            Value::String("stream_owner_local_state".to_owned()),
        );
        object.insert("checkpointSequence".to_owned(), Value::from(view_state.checkpoint_sequence));
        if let Some(owner_epoch) = handle.stream_owner_epoch {
            object.insert("streamOwnerEpoch".to_owned(), Value::from(owner_epoch));
        }
        if !matches!(
            query.parsed_consistency(),
            Some(StreamJobQueryConsistency::Strong | StreamJobQueryConsistency::Eventual)
        ) {
            object.insert(
                "consistency".to_owned(),
                Value::String(STREAM_CONSISTENCY_STRONG.to_owned()),
            );
        }
    }
    Ok(Some(output))
}

pub(crate) fn terminal_result_after_query(
    handle: &StreamJobBridgeHandleRecord,
) -> Option<StreamJobTerminalResult> {
    if handle.workflow_accepted_at.is_some() {
        return None;
    }

    match handle.parsed_status() {
        Some(StreamJobBridgeHandleStatus::Completed) => Some(StreamJobTerminalResult {
            status: StreamJobBridgeHandleStatus::Completed,
            output: handle.terminal_output.clone().or_else(|| Some(terminal_output(handle))),
            error: None,
        }),
        Some(StreamJobBridgeHandleStatus::Failed) => Some(StreamJobTerminalResult {
            status: StreamJobBridgeHandleStatus::Failed,
            output: None,
            error: handle.terminal_error.clone(),
        }),
        Some(StreamJobBridgeHandleStatus::Cancelled) => Some(StreamJobTerminalResult {
            status: StreamJobBridgeHandleStatus::Cancelled,
            output: None,
            error: handle.terminal_error.clone().or(handle.cancellation_reason.clone()),
        }),
        _ => None,
    }
}

pub(crate) fn should_defer_terminal_callback_until_query_boundary(
    handle: &StreamJobBridgeHandleRecord,
) -> bool {
    matches!(handle.parsed_origin_kind(), Some(StreamJobOriginKind::Workflow))
        && handle.workflow_accepted_at.is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fabrik_throughput::{StreamsChangelogEntry, StreamsChangelogPayload};
    use std::path::PathBuf;

    fn temp_path(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{prefix}-{}", Uuid::now_v7()))
    }

    fn keyed_rollup_handle(input_ref: &str) -> StreamJobBridgeHandleRecord {
        let now = Utc::now();
        StreamJobBridgeHandleRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            tenant_id: "tenant".to_owned(),
            instance_id: "instance".to_owned(),
            run_id: "run".to_owned(),
            stream_instance_id: "instance".to_owned(),
            stream_run_id: "run".to_owned(),
            job_id: "job".to_owned(),
            handle_id: "handle".to_owned(),
            bridge_request_id: "bridge-request".to_owned(),
            origin_kind: StreamJobOriginKind::Workflow.as_str().to_owned(),
            definition_id: "demo".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact".to_owned()),
            job_name: KEYED_ROLLUP_JOB_NAME.to_owned(),
            input_ref: input_ref.to_owned(),
            config_ref: None,
            checkpoint_policy: Some(json!({"kind":"named_checkpoints"})),
            view_definitions: Some(json!([{"name":"accountTotals"}])),
            status: StreamJobBridgeHandleStatus::Running.as_str().to_owned(),
            workflow_owner_epoch: Some(3),
            stream_owner_epoch: Some(7),
            cancellation_requested_at: None,
            cancellation_reason: None,
            terminal_event_id: None,
            terminal_at: None,
            workflow_accepted_at: None,
            terminal_output: None,
            terminal_error: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn distinct_partition_keys(partition_count: i32) -> (String, String) {
        let first_key = "acct_partition_a".to_owned();
        let first_partition = throughput_partition_for_stream_key(&first_key, partition_count);
        let second_key = (1..64)
            .map(|index| format!("acct_partition_b_{index}"))
            .find(|candidate| {
                throughput_partition_for_stream_key(candidate, partition_count) != first_partition
            })
            .expect("should find a logical key on a different partition");
        (first_key, second_key)
    }

    fn streams_entry_from_record(record: &crate::StreamChangelogRecord) -> StreamsChangelogEntry {
        let payload = match &record.entry.payload {
            ThroughputChangelogPayload::StreamJobExecutionPlanned {
                handle_id,
                job_id,
                job_name,
                view_name,
                checkpoint_name,
                checkpoint_sequence,
                input_item_count,
                materialized_key_count,
                active_partitions,
                owner_epoch,
                planned_at,
            } => StreamsChangelogPayload::StreamJobExecutionPlanned {
                handle_id: handle_id.clone(),
                job_id: job_id.clone(),
                job_name: job_name.clone(),
                view_name: view_name.clone(),
                checkpoint_name: checkpoint_name.clone(),
                checkpoint_sequence: *checkpoint_sequence,
                input_item_count: *input_item_count,
                materialized_key_count: *materialized_key_count,
                active_partitions: active_partitions.clone(),
                owner_epoch: *owner_epoch,
                planned_at: *planned_at,
            },
            ThroughputChangelogPayload::StreamJobViewUpdated {
                handle_id,
                job_id,
                view_name,
                logical_key,
                output,
                checkpoint_sequence,
                updated_at,
            } => StreamsChangelogPayload::StreamJobViewUpdated {
                handle_id: handle_id.clone(),
                job_id: job_id.clone(),
                view_name: view_name.clone(),
                logical_key: logical_key.clone(),
                output: output.clone(),
                checkpoint_sequence: *checkpoint_sequence,
                updated_at: *updated_at,
            },
            ThroughputChangelogPayload::StreamJobCheckpointReached {
                handle_id,
                job_id,
                checkpoint_name,
                checkpoint_sequence,
                stream_partition_id,
                owner_epoch,
                reached_at,
            } => StreamsChangelogPayload::StreamJobCheckpointReached {
                handle_id: handle_id.clone(),
                job_id: job_id.clone(),
                checkpoint_name: checkpoint_name.clone(),
                checkpoint_sequence: *checkpoint_sequence,
                stream_partition_id: *stream_partition_id,
                owner_epoch: *owner_epoch,
                reached_at: *reached_at,
            },
            ThroughputChangelogPayload::StreamJobTerminalized {
                handle_id,
                job_id,
                owner_epoch,
                status,
                output,
                error,
                terminal_at,
            } => StreamsChangelogPayload::StreamJobTerminalized {
                handle_id: handle_id.clone(),
                job_id: job_id.clone(),
                owner_epoch: *owner_epoch,
                status: status.clone(),
                output: output.clone(),
                error: error.clone(),
                terminal_at: *terminal_at,
            },
            other => panic!("unexpected non-stream payload in stream job test: {other:?}"),
        };
        StreamsChangelogEntry {
            entry_id: record.entry.entry_id,
            occurred_at: record.entry.occurred_at,
            partition_key: record.entry.partition_key.clone(),
            payload,
        }
    }

    #[test]
    fn keyed_rollup_checkpoint_name_prefers_declared_checkpoint_policy() {
        let config = json!({
            "checkpoint": "legacy-name",
            "checkpointPolicy": {
                "kind": "named_checkpoints",
                "checkpoints": [
                    {
                        "name": "declared-name",
                        "delivery": "workflow_awaitable",
                        "sequence": 1
                    }
                ]
            }
        });

        let checkpoint_name = keyed_rollup_checkpoint_name_from_config_ref(
            Some(&serde_json::to_string(&config).expect("config should serialize")),
            None,
        );

        assert_eq!(checkpoint_name, "declared-name");
    }

    #[tokio::test]
    async fn keyed_rollup_activation_persists_view_state_projection_and_changelog() -> Result<()> {
        let db_path = temp_path("stream-jobs-keyed-rollup-db");
        let checkpoint_dir = temp_path("stream-jobs-keyed-rollup-checkpoints");
        let local_state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let handle = keyed_rollup_handle(
            r#"{"kind":"bounded_items","items":[{"accountId":"acct_1","amount":4},{"accountId":"acct_1","amount":5.5},{"accountId":"acct_2","amount":1}]}"#,
        );

        let activation =
            activate_stream_job_on_local_state(None, &local_state, &handle, 8, Some(9), Utc::now())
                .await?
                .expect("keyed-rollup activation should exist");

        assert_eq!(activation.projection_records.len(), 2);
        let checkpoint_record_count = activation
            .changelog_records
            .iter()
            .filter(|record| {
                matches!(
                    record.entry.payload,
                    ThroughputChangelogPayload::StreamJobCheckpointReached { .. }
                )
            })
            .count();
        assert_eq!(activation.changelog_records.len(), 1 + 2 + checkpoint_record_count + 1);
        for (offset, record) in activation.changelog_records.iter().enumerate() {
            let entry = streams_entry_from_record(record);
            assert!(local_state.apply_streams_changelog_entry(0, offset as i64, &entry)?);
        }

        let materialized = materialization_outcome_from_local_state(&local_state, &handle)?
            .expect("materialized stream job outcome should exist");
        let checkpoint = materialized.checkpoint.expect("checkpoint should be produced");
        assert_eq!(checkpoint.checkpoint_name, KEYED_ROLLUP_CHECKPOINT_NAME);
        assert_eq!(checkpoint.checkpoint_sequence, Some(KEYED_ROLLUP_CHECKPOINT_SEQUENCE));
        assert_eq!(checkpoint.stream_owner_epoch, Some(9));
        let terminal = materialized.terminal.expect("terminal should be materialized");
        assert_eq!(terminal.status, StreamJobBridgeHandleStatus::Completed);
        assert_eq!(
            terminal.output.as_ref().and_then(|output| output.get("status")),
            Some(&json!("completed"))
        );
        assert!(terminal.error.is_none());

        let runtime = local_state
            .load_stream_job_runtime_state(&handle.handle_id)?
            .expect("runtime state should exist");
        assert_eq!(runtime.input_item_count, 3);
        assert_eq!(runtime.materialized_key_count, 2);
        assert_eq!(runtime.active_partitions.len(), checkpoint_record_count);
        assert_eq!(runtime.stream_owner_epoch, 9);

        let acct_1 = local_state
            .load_stream_job_view_state(&handle.handle_id, "accountTotals", "acct_1")?
            .expect("acct_1 view should exist");
        assert_eq!(acct_1.output["totalAmount"], 9.5);
        assert_eq!(acct_1.checkpoint_sequence, KEYED_ROLLUP_CHECKPOINT_SEQUENCE);

        let acct_2 = local_state
            .load_stream_job_view_state(&handle.handle_id, "accountTotals", "acct_2")?
            .expect("acct_2 view should exist");
        assert_eq!(acct_2.output["totalAmount"], 1.0);

        std::fs::remove_dir_all(db_path).ok();
        std::fs::remove_dir_all(checkpoint_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn keyed_rollup_replays_from_checkpoint_plus_streams_tail() -> Result<()> {
        let db_path = temp_path("stream-jobs-keyed-rollup-replay-db");
        let checkpoint_dir = temp_path("stream-jobs-keyed-rollup-replay-checkpoints");
        let local_state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let handle = keyed_rollup_handle(
            r#"{"kind":"bounded_items","items":[{"accountId":"acct_1","amount":4},{"accountId":"acct_1","amount":5.5},{"accountId":"acct_2","amount":1}]}"#,
        );
        let activation =
            activate_stream_job_on_local_state(None, &local_state, &handle, 8, Some(9), Utc::now())
                .await?
                .expect("keyed-rollup activation should exist");

        let stream_entries =
            activation.changelog_records.iter().map(streams_entry_from_record).collect::<Vec<_>>();
        for (offset, entry) in stream_entries.iter().take(3).enumerate() {
            assert!(local_state.apply_streams_changelog_entry(0, offset as i64, entry)?);
        }
        let checkpoint_value = local_state.snapshot_checkpoint_value()?;

        let restored_db_path = temp_path("stream-jobs-keyed-rollup-replay-restored-db");
        let restored = LocalThroughputState::open(&restored_db_path, &checkpoint_dir, 3)?;
        assert!(restored.restore_from_checkpoint_value_if_empty(checkpoint_value)?);
        for (index, entry) in stream_entries.iter().enumerate().skip(3) {
            assert!(restored.apply_streams_changelog_entry(0, index as i64, entry)?);
        }

        let acct_2 = restored
            .load_stream_job_view_state(&handle.handle_id, "accountTotals", "acct_2")?
            .expect("acct_2 view should restore from tail");
        assert_eq!(acct_2.output["totalAmount"], 1.0);

        let materialized = materialization_outcome_from_local_state(&restored, &handle)?
            .expect("materialized outcome should survive checkpoint plus tail replay");
        assert_eq!(
            materialized.checkpoint.as_ref().and_then(|checkpoint| checkpoint.checkpoint_sequence),
            Some(KEYED_ROLLUP_CHECKPOINT_SEQUENCE)
        );
        assert_eq!(
            materialized
                .terminal
                .as_ref()
                .and_then(|terminal| terminal.output.as_ref())
                .and_then(|output| output.get("status")),
            Some(&json!("completed"))
        );

        std::fs::remove_dir_all(db_path).ok();
        std::fs::remove_dir_all(restored_db_path).ok();
        std::fs::remove_dir_all(checkpoint_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn keyed_rollup_replays_mid_partition_failover_from_owner_checkpoint() -> Result<()> {
        let partition_count = 8;
        let (first_key, second_key) = distinct_partition_keys(partition_count);
        let mut items = Vec::new();
        for _ in 0..65 {
            items.push(json!({ "accountId": first_key, "amount": 1.0 }));
        }
        items.push(json!({ "accountId": second_key, "amount": 3.0 }));
        let handle = keyed_rollup_handle(
            &json!({
                "kind": "bounded_items",
                "items": items
            })
            .to_string(),
        );
        let coordinator_db_path = temp_path("stream-jobs-keyed-rollup-midflight-coordinator-db");
        let coordinator_checkpoint_dir =
            temp_path("stream-jobs-keyed-rollup-midflight-coordinator-checkpoints");
        let coordinator_state =
            LocalThroughputState::open(&coordinator_db_path, &coordinator_checkpoint_dir, 3)?;

        let occurred_at = Utc::now();
        let plan = plan_stream_job_activation(
            &coordinator_state,
            None,
            &handle,
            partition_count,
            Some(9),
            occurred_at,
        )
        .await?
        .expect("keyed-rollup activation plan should exist");
        let first_partition = throughput_partition_for_stream_key(&first_key, partition_count);
        let second_partition = throughput_partition_for_stream_key(&second_key, partition_count);
        let mut first_partition_work = plan
            .partition_work
            .iter()
            .filter(|work| work.stream_partition_id == first_partition)
            .cloned()
            .collect::<Vec<_>>();
        let second_partition_work = plan
            .partition_work
            .iter()
            .find(|work| work.stream_partition_id == second_partition)
            .cloned()
            .expect("second partition work should exist");
        assert_eq!(first_partition_work.len(), 2);
        first_partition_work.sort_by_key(|work| work.is_final_partition_batch);
        assert!(!first_partition_work[0].is_final_partition_batch);
        assert!(first_partition_work[1].is_final_partition_batch);
        let owner_db_path = temp_path("stream-jobs-keyed-rollup-midflight-owner-db");
        let owner_checkpoint_dir =
            temp_path("stream-jobs-keyed-rollup-midflight-owner-checkpoints");
        let owner_state = LocalThroughputState::open(&owner_db_path, &owner_checkpoint_dir, 3)?;

        let execution_entry = streams_entry_from_record(
            plan.execution_planned.as_ref().expect("execution plan should exist"),
        );
        assert!(coordinator_state.apply_streams_changelog_entry(0, 0, &execution_entry)?);
        owner_state.mirror_streams_changelog_entry(&execution_entry)?;
        owner_state.replace_stream_job_dispatch_manifest(
            &handle.handle_id,
            plan.partition_work.iter().map(local_dispatch_batch_from_work).collect(),
            occurred_at,
        )?;

        let first_batch = apply_stream_job_partition_work_on_local_state(
            &owner_state,
            &handle,
            &first_partition_work[0],
            true,
        )?;
        assert_eq!(first_batch.projection_records.len(), 1);
        let partial_view = owner_state
            .load_stream_job_view_state(&handle.handle_id, "accountTotals", &first_key)?
            .expect("partial owner state should materialize the first key");
        assert_eq!(partial_view.output["totalAmount"], json!(64.0));
        assert!(
            materialization_outcome_from_local_state(&coordinator_state, &handle)?
                .expect("coordinator runtime state should exist")
                .checkpoint
                .is_none()
        );

        let owner_checkpoint =
            owner_state.snapshot_partition_checkpoint_value(first_partition, partition_count)?;
        let restored_owner_db_path =
            temp_path("stream-jobs-keyed-rollup-midflight-owner-restored-db");
        let restored_owner_checkpoint_dir =
            temp_path("stream-jobs-keyed-rollup-midflight-owner-restored-checkpoints");
        let restored_owner =
            LocalThroughputState::open(&restored_owner_db_path, &restored_owner_checkpoint_dir, 3)?;
        assert!(restored_owner.restore_from_checkpoint_value_if_empty(owner_checkpoint)?);
        let restored_partial = restored_owner
            .load_stream_job_view_state(&handle.handle_id, "accountTotals", &first_key)?
            .expect("restored owner should preserve partial key state");
        assert_eq!(restored_partial.output["totalAmount"], json!(64.0));

        let first_final_batch = apply_stream_job_partition_work_on_local_state(
            &restored_owner,
            &handle,
            &first_partition_work[1],
            true,
        )?;
        let second_batch = apply_stream_job_partition_work_on_local_state(
            &owner_state,
            &handle,
            &second_partition_work,
            true,
        )?;
        let mut replay_records = Vec::new();
        replay_records.extend(first_batch.changelog_records);
        replay_records.extend(first_final_batch.changelog_records);
        replay_records.extend(second_batch.changelog_records);
        replay_records.push(plan.terminalized.expect("terminal record should exist"));
        for (offset, record) in replay_records.iter().enumerate() {
            let entry = streams_entry_from_record(record);
            assert!(coordinator_state.apply_streams_changelog_entry(
                0,
                offset as i64 + 1,
                &entry,
            )?);
        }

        let completed_view = restored_owner
            .load_stream_job_view_state(&handle.handle_id, "accountTotals", &first_key)?
            .expect("restored owner should complete the partition reduction");
        assert_eq!(completed_view.output["totalAmount"], json!(65.0));
        let materialized = materialization_outcome_from_local_state(&coordinator_state, &handle)?
            .expect("coordinator outcome should exist after replay");
        assert_eq!(
            materialized.checkpoint.as_ref().and_then(|checkpoint| checkpoint.checkpoint_sequence),
            Some(KEYED_ROLLUP_CHECKPOINT_SEQUENCE)
        );
        assert_eq!(
            materialized.terminal.as_ref().map(|terminal| terminal.status),
            Some(StreamJobBridgeHandleStatus::Completed)
        );

        std::fs::remove_dir_all(coordinator_db_path).ok();
        std::fs::remove_dir_all(coordinator_checkpoint_dir).ok();
        std::fs::remove_dir_all(owner_db_path).ok();
        std::fs::remove_dir_all(owner_checkpoint_dir).ok();
        std::fs::remove_dir_all(restored_owner_db_path).ok();
        std::fs::remove_dir_all(restored_owner_checkpoint_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn keyed_rollup_restart_reuses_persisted_dispatch_manifest() -> Result<()> {
        let partition_count = 8;
        let (first_key, second_key) = distinct_partition_keys(partition_count);
        let mut items = Vec::new();
        for _ in 0..65 {
            items.push(json!({ "accountId": first_key, "amount": 1.0 }));
        }
        items.push(json!({ "accountId": second_key, "amount": 3.0 }));
        let handle = keyed_rollup_handle(
            &json!({
                "kind": "bounded_items",
                "items": items
            })
            .to_string(),
        );
        let db_path = temp_path("stream-jobs-keyed-rollup-dispatch-reuse-db");
        let checkpoint_dir = temp_path("stream-jobs-keyed-rollup-dispatch-reuse-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let occurred_at = Utc::now();

        let first_plan = plan_stream_job_activation(
            &state,
            None,
            &handle,
            partition_count,
            Some(9),
            occurred_at,
        )
        .await?
        .expect("first activation plan should exist");
        let execution_entry = streams_entry_from_record(
            first_plan.execution_planned.as_ref().expect("execution plan should exist"),
        );
        state.mirror_streams_changelog_entry(&execution_entry)?;
        state.replace_stream_job_dispatch_manifest(
            &handle.handle_id,
            first_plan.partition_work.iter().map(local_dispatch_batch_from_work).collect(),
            occurred_at,
        )?;
        let applied_batch_id = first_plan.partition_work[0].batch_id.clone();
        assert!(state.mark_stream_job_dispatch_batch_applied(
            &handle.handle_id,
            &applied_batch_id,
            occurred_at,
        )?);

        let resumed_plan = plan_stream_job_activation(
            &state,
            None,
            &handle,
            partition_count,
            Some(9),
            occurred_at + chrono::Duration::seconds(1),
        )
        .await?
        .expect("resumed activation plan should exist");
        assert!(resumed_plan.execution_planned.is_none());
        assert!(resumed_plan.terminalized.is_none());
        assert_eq!(
            resumed_plan.partition_work.len(),
            first_plan.partition_work.len().saturating_sub(1)
        );
        assert!(resumed_plan.partition_work.iter().all(|work| work.batch_id != applied_batch_id));

        std::fs::remove_dir_all(db_path).ok();
        std::fs::remove_dir_all(checkpoint_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn keyed_rollup_restart_halts_dispatch_after_persisted_cancellation() -> Result<()> {
        let partition_count = 8;
        let (first_key, second_key) = distinct_partition_keys(partition_count);
        let handle = keyed_rollup_handle(
            &json!({
                "kind": "bounded_items",
                "items": [
                    { "accountId": first_key, "amount": 4.0 },
                    { "accountId": second_key, "amount": 5.0 }
                ]
            })
            .to_string(),
        );
        let db_path = temp_path("stream-jobs-keyed-rollup-dispatch-cancel-db");
        let checkpoint_dir = temp_path("stream-jobs-keyed-rollup-dispatch-cancel-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let occurred_at = Utc::now();

        let first_plan = plan_stream_job_activation(
            &state,
            None,
            &handle,
            partition_count,
            Some(9),
            occurred_at,
        )
        .await?
        .expect("first activation plan should exist");
        let execution_entry = streams_entry_from_record(
            first_plan.execution_planned.as_ref().expect("execution plan should exist"),
        );
        state.mirror_streams_changelog_entry(&execution_entry)?;
        state.replace_stream_job_dispatch_manifest(
            &handle.handle_id,
            first_plan.partition_work.iter().map(local_dispatch_batch_from_work).collect(),
            occurred_at,
        )?;
        state.mark_stream_job_dispatch_cancelled(
            &handle.handle_id,
            occurred_at + chrono::Duration::milliseconds(1),
        )?;

        let resumed_plan = plan_stream_job_activation(
            &state,
            None,
            &handle,
            partition_count,
            Some(9),
            occurred_at + chrono::Duration::seconds(1),
        )
        .await?
        .expect("resumed activation plan should exist");
        assert!(resumed_plan.execution_planned.is_none());
        assert!(resumed_plan.partition_work.is_empty());
        assert!(resumed_plan.terminalized.is_none());

        std::fs::remove_dir_all(db_path).ok();
        std::fs::remove_dir_all(checkpoint_dir).ok();
        Ok(())
    }

    #[test]
    fn keyed_rollup_query_output_returns_owner_view_shape() {
        let db_path = temp_path("stream-jobs-keyed-rollup-query-db");
        let checkpoint_dir = temp_path("stream-jobs-keyed-rollup-query-checkpoints");
        let local_state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)
            .expect("local state should open");
        let handle = keyed_rollup_handle(
            r#"{"kind":"bounded_items","items":[{"accountId":"acct_1","amount":4},{"accountId":"acct_1","amount":5.5}]}"#,
        );
        local_state
            .upsert_stream_job_view_value(
                &handle.handle_id,
                &handle.job_id,
                "accountTotals",
                "acct_1",
                json!({
                    "accountId": "acct_1",
                    "totalAmount": 9.5,
                    "asOfCheckpoint": KEYED_ROLLUP_CHECKPOINT_SEQUENCE
                }),
                KEYED_ROLLUP_CHECKPOINT_SEQUENCE,
                Utc::now(),
            )
            .expect("view state should persist");
        let query = StreamJobQueryRecord {
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            workflow_event_id: Uuid::now_v7(),
            tenant_id: "tenant".to_owned(),
            instance_id: "instance".to_owned(),
            run_id: "run".to_owned(),
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            bridge_request_id: handle.bridge_request_id.clone(),
            query_id: "query".to_owned(),
            query_name: "accountTotals".to_owned(),
            query_args: Some(json!({"key":"acct_1"})),
            consistency: StreamJobQueryConsistency::Strong.as_str().to_owned(),
            status: fabrik_throughput::StreamJobQueryStatus::Completed.as_str().to_owned(),
            workflow_owner_epoch: Some(3),
            stream_owner_epoch: Some(7),
            output: None,
            error: None,
            requested_at: Utc::now(),
            completed_at: None,
            accepted_at: None,
            cancelled_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let output = build_stream_job_query_output_on_local_state(&local_state, &handle, &query)
            .expect("query output should build")
            .expect("keyed-rollup output should exist");
        assert_eq!(output["accountId"], "acct_1");
        assert_eq!(output["totalAmount"], 9.5);
        assert_eq!(output["consistency"], "strong");
        assert_eq!(output["consistencySource"], "stream_owner_local_state");
        assert_eq!(output["checkpointSequence"], KEYED_ROLLUP_CHECKPOINT_SEQUENCE);
        assert_eq!(output["streamOwnerEpoch"], 7);

        std::fs::remove_dir_all(db_path).ok();
        std::fs::remove_dir_all(checkpoint_dir).ok();
    }

    #[tokio::test]
    async fn keyed_rollup_materialization_waits_for_all_active_partition_checkpoints() -> Result<()>
    {
        let db_path = temp_path("stream-jobs-keyed-rollup-barrier-db");
        let checkpoint_dir = temp_path("stream-jobs-keyed-rollup-barrier-checkpoints");
        let local_state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let (first_key, second_key) = distinct_partition_keys(8);
        let handle = keyed_rollup_handle(
            &json!({
                "kind": "bounded_items",
                "items": [
                    { "accountId": first_key, "amount": 4 },
                    { "accountId": second_key, "amount": 5 }
                ]
            })
            .to_string(),
        );

        let plan = plan_stream_job_activation(&local_state, None, &handle, 8, Some(9), Utc::now())
            .await?
            .expect("keyed-rollup activation plan should exist");
        let runtime_entry = streams_entry_from_record(
            plan.execution_planned.as_ref().expect("execution plan should exist"),
        );
        assert!(local_state.apply_streams_changelog_entry(0, 0, &runtime_entry)?);

        let mut view_offset = 1i64;
        let mut checkpoint_entries = Vec::new();
        for work in &plan.partition_work {
            let applied =
                apply_stream_job_partition_work_on_local_state(&local_state, &handle, work, false)?;
            for record in applied.changelog_records {
                match &record.entry.payload {
                    ThroughputChangelogPayload::StreamJobViewUpdated { logical_key, .. } => {
                        assert_eq!(record.key, throughput_partition_key(logical_key, 0));
                        let entry = streams_entry_from_record(&record);
                        assert!(local_state.apply_streams_changelog_entry(
                            0,
                            view_offset,
                            &entry
                        )?);
                        view_offset += 1;
                    }
                    ThroughputChangelogPayload::StreamJobCheckpointReached { .. } => {
                        checkpoint_entries.push(streams_entry_from_record(&record));
                    }
                    other => panic!("unexpected payload in keyed-rollup barrier test: {other:?}"),
                }
            }
        }

        assert!(checkpoint_entries.len() >= 2);
        assert!(local_state.apply_streams_changelog_entry(
            0,
            view_offset,
            &checkpoint_entries[0]
        )?);
        let incomplete = materialization_outcome_from_local_state(&local_state, &handle)?
            .expect("runtime state should be present after first checkpoint");
        assert!(incomplete.checkpoint.is_none());
        assert!(incomplete.terminal.is_none());

        for (index, entry) in checkpoint_entries.iter().enumerate().skip(1) {
            assert!(local_state.apply_streams_changelog_entry(
                0,
                view_offset + index as i64,
                entry,
            )?);
        }
        let terminal_entry = streams_entry_from_record(
            plan.terminalized.as_ref().expect("terminal record should exist"),
        );
        assert!(local_state.apply_streams_changelog_entry(
            0,
            view_offset + checkpoint_entries.len() as i64,
            &terminal_entry,
        )?);

        let complete = materialization_outcome_from_local_state(&local_state, &handle)?
            .expect("materialization should exist after barrier completion");
        assert_eq!(
            complete.checkpoint.as_ref().and_then(|checkpoint| checkpoint.checkpoint_sequence),
            Some(KEYED_ROLLUP_CHECKPOINT_SEQUENCE)
        );
        assert_eq!(
            complete.terminal.as_ref().map(|terminal| terminal.status),
            Some(StreamJobBridgeHandleStatus::Completed)
        );

        std::fs::remove_dir_all(db_path).ok();
        std::fs::remove_dir_all(checkpoint_dir).ok();
        Ok(())
    }

    #[test]
    fn terminal_result_after_query_only_replays_unaccepted_terminal_state() {
        let mut handle = keyed_rollup_handle(
            r#"{"kind":"bounded_items","items":[{"accountId":"acct_1","amount":4}]}"#,
        );
        assert!(terminal_result_after_query(&handle).is_none());
        assert!(should_defer_terminal_callback_until_query_boundary(&handle));

        handle.status = StreamJobBridgeHandleStatus::Completed.as_str().to_owned();
        handle.terminal_output = Some(json!({"status": "completed", "source": "stored"}));
        let stored = terminal_result_after_query(&handle).expect("stored terminal should replay");
        assert_eq!(
            stored.output.as_ref().and_then(|output| output.get("source")),
            Some(&json!("stored"))
        );

        handle.workflow_accepted_at = Some(Utc::now());
        assert!(terminal_result_after_query(&handle).is_none());
        assert!(!should_defer_terminal_callback_until_query_boundary(&handle));
    }
}
