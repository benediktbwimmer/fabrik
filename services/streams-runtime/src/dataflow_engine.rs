use anyhow::Result;
use chrono::{DateTime, Utc};
use fabrik_store::StreamJobBridgeHandleRecord;
use fabrik_throughput::{
    STREAM_REDUCER_AVG, STREAM_REDUCER_COUNT, STREAM_REDUCER_MAX, STREAM_REDUCER_MIN,
    STREAM_REDUCER_SUM, STREAM_REDUCER_THRESHOLD, StreamJobViewBatchUpdate, StreamsChangelogEntry,
    StreamsChangelogPayload, StreamsProjectionEvent, StreamsViewRecord,
    stream_job_signal_callback_dedupe_key,
};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap, VecDeque};
use uuid::Uuid;

use crate::local_state::{LocalStreamJobAcceptedProgressState, LocalStreamJobViewState};
use crate::{
    LocalThroughputState, StreamChangelogRecord, StreamJobActivationResult, StreamProjectionRecord,
    stream_jobs, throughput_partition_key,
};

const STREAM_INTERNAL_AVG_SUM_FIELD: &str = "_stream_sum";
const STREAM_INTERNAL_AVG_COUNT_FIELD: &str = "_stream_count";

pub(crate) struct LocalDataflowEngine<'a> {
    local_state: &'a LocalThroughputState,
}

fn hidden_avg_count(output: &Map<String, Value>) -> u64 {
    output
        .get(STREAM_INTERNAL_AVG_COUNT_FIELD)
        .and_then(Value::as_u64)
        .or_else(|| {
            output
                .get(STREAM_INTERNAL_AVG_COUNT_FIELD)
                .and_then(Value::as_f64)
                .filter(|value| value.is_finite() && *value >= 0.0)
                .map(|value| value as u64)
        })
        .unwrap_or(0)
}

fn numeric_output_field(output: &Map<String, Value>, field: &str) -> Option<f64> {
    output
        .get(field)
        .and_then(Value::as_f64)
        .or_else(|| output.get(field).and_then(Value::as_u64).map(|value| value as f64))
}

fn boolean_output_field(output: &Map<String, Value>, field: &str) -> Option<bool> {
    output
        .get(field)
        .and_then(Value::as_bool)
        .or_else(|| numeric_output_field(output, field).map(|value| value > 0.0))
}

fn truthy_output_field(output: &Map<String, Value>, field: &str) -> bool {
    output.get(field).is_some_and(|value| match value {
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|value| value > 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Null => false,
        Value::Array(values) => !values.is_empty(),
        Value::Object(values) => !values.is_empty(),
    })
}

pub(crate) fn signal_output_matches(output: &Value, when_output_field: Option<&str>) -> bool {
    match when_output_field {
        Some(field) => output.as_object().is_some_and(|output| truthy_output_field(output, field)),
        None => true,
    }
}

pub(crate) fn sanitize_stream_view_output(mut output: Value) -> Value {
    if let Some(object) = output.as_object_mut() {
        object.remove(STREAM_INTERNAL_AVG_SUM_FIELD);
        object.remove(STREAM_INTERNAL_AVG_COUNT_FIELD);
    }
    output
}

fn normalize_stream_average_output(value: f64) -> f64 {
    if !value.is_finite() {
        return value;
    }
    const SCALE: f64 = 1_000_000_000_000.0;
    (value * SCALE).round() / SCALE
}

#[derive(Debug, Clone)]
enum StreamReducerAccumulator {
    Count { total: u64 },
    Sum { total: f64 },
    Min { value: Option<f64> },
    Max { value: Option<f64> },
    Avg { sum: f64, count: u64 },
    Threshold { crossed: bool },
}

impl StreamReducerAccumulator {
    fn new(reducer_kind: &str) -> Result<Self> {
        match reducer_kind {
            STREAM_REDUCER_COUNT => Ok(Self::Count { total: 0 }),
            STREAM_REDUCER_SUM => Ok(Self::Sum { total: 0.0 }),
            STREAM_REDUCER_MIN => Ok(Self::Min { value: None }),
            STREAM_REDUCER_MAX => Ok(Self::Max { value: None }),
            STREAM_REDUCER_AVG => Ok(Self::Avg { sum: 0.0, count: 0 }),
            STREAM_REDUCER_THRESHOLD => Ok(Self::Threshold { crossed: false }),
            other => anyhow::bail!("stream reducer {other} is not implemented in local activation"),
        }
    }

    fn from_output(
        output: &Map<String, Value>,
        reducer_kind: &str,
        output_field: &str,
    ) -> Result<Self> {
        match reducer_kind {
            STREAM_REDUCER_COUNT => Ok(Self::Count {
                total: numeric_output_field(output, output_field)
                    .filter(|value| value.is_finite() && *value >= 0.0)
                    .map(|value| value as u64)
                    .unwrap_or(0),
            }),
            STREAM_REDUCER_SUM => {
                Ok(Self::Sum { total: numeric_output_field(output, output_field).unwrap_or(0.0) })
            }
            STREAM_REDUCER_MIN => {
                Ok(Self::Min { value: numeric_output_field(output, output_field) })
            }
            STREAM_REDUCER_MAX => {
                Ok(Self::Max { value: numeric_output_field(output, output_field) })
            }
            STREAM_REDUCER_AVG => {
                let sum = output
                    .get(STREAM_INTERNAL_AVG_SUM_FIELD)
                    .and_then(Value::as_f64)
                    .or_else(|| numeric_output_field(output, output_field))
                    .unwrap_or(0.0);
                let count =
                    hidden_avg_count(output).max(u64::from(output.contains_key(output_field)));
                Ok(Self::Avg { sum, count })
            }
            STREAM_REDUCER_THRESHOLD => Ok(Self::Threshold {
                crossed: boolean_output_field(output, output_field).unwrap_or(false),
            }),
            other => anyhow::bail!("stream reducer {other} is not implemented in local activation"),
        }
    }

    fn apply(&mut self, input_value: f64, input_count: u64) {
        match self {
            Self::Count { total } => *total = total.saturating_add(input_count),
            Self::Sum { total } => *total += input_value,
            Self::Min { value } => {
                *value = Some(value.map_or(input_value, |baseline| baseline.min(input_value)));
            }
            Self::Max { value } => {
                *value = Some(value.map_or(input_value, |baseline| baseline.max(input_value)));
            }
            Self::Avg { sum, count } => {
                *sum += input_value;
                *count = count.saturating_add(input_count);
            }
            Self::Threshold { crossed } => *crossed |= input_value > 0.0,
        }
    }

    fn write_output(
        &self,
        output: &mut Map<String, Value>,
        output_field: &str,
        include_internal_fields: bool,
    ) {
        match self {
            Self::Count { total } => {
                output.insert(output_field.to_owned(), json!(*total as f64));
            }
            Self::Sum { total } => {
                output.insert(output_field.to_owned(), json!(*total));
            }
            Self::Min { value } | Self::Max { value } => {
                if let Some(value) = value {
                    output.insert(output_field.to_owned(), json!(*value));
                }
            }
            Self::Avg { sum, count } => {
                let average = if *count == 0 { 0.0 } else { *sum / *count as f64 };
                output.insert(
                    output_field.to_owned(),
                    json!(normalize_stream_average_output(average)),
                );
                if include_internal_fields {
                    output.insert(STREAM_INTERNAL_AVG_SUM_FIELD.to_owned(), json!(*sum));
                    output.insert(STREAM_INTERNAL_AVG_COUNT_FIELD.to_owned(), json!(*count));
                }
            }
            Self::Threshold { crossed } => {
                output.insert(output_field.to_owned(), json!(*crossed));
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StreamJobViewAccumulator {
    display_key: String,
    window_start: Option<String>,
    window_end: Option<String>,
    reducer: StreamReducerAccumulator,
}

impl StreamJobViewAccumulator {
    pub(crate) fn from_output(
        output: &Map<String, Value>,
        reducer_kind: &str,
        output_field: &str,
        key_field: &str,
        logical_key: &str,
    ) -> Result<Self> {
        Ok(Self {
            display_key: output
                .get(key_field)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| logical_key.to_owned()),
            window_start: output.get("windowStart").and_then(Value::as_str).map(str::to_owned),
            window_end: output.get("windowEnd").and_then(Value::as_str).map(str::to_owned),
            reducer: StreamReducerAccumulator::from_output(output, reducer_kind, output_field)?,
        })
    }

    pub(crate) fn from_item(item: &BoundedDataflowItem, reducer_kind: &str) -> Result<Self> {
        Ok(Self {
            display_key: item.display_key.clone().unwrap_or_else(|| item.logical_key.clone()),
            window_start: item.window_start.clone(),
            window_end: item.window_end.clone(),
            reducer: StreamReducerAccumulator::new(reducer_kind)?,
        })
    }

    pub(crate) fn apply_item(&mut self, item: &BoundedDataflowItem) {
        self.reducer.apply(item.value, item.count);
        if let Some(display_key) = &item.display_key {
            self.display_key = display_key.clone();
        }
        if self.window_start.is_none() {
            self.window_start = item.window_start.clone();
        }
        if self.window_end.is_none() {
            self.window_end = item.window_end.clone();
        }
    }

    fn append_common_fields(&self, output: &mut Map<String, Value>, key_field: &str) {
        output.insert(key_field.to_owned(), Value::String(self.display_key.clone()));
        if let Some(window_start) = &self.window_start {
            output.insert("windowStart".to_owned(), Value::String(window_start.clone()));
        }
        if let Some(window_end) = &self.window_end {
            output.insert("windowEnd".to_owned(), Value::String(window_end.clone()));
        }
    }

    pub(crate) fn into_output_value(
        &self,
        key_field: &str,
        output_field: &str,
        checkpoint_sequence: i64,
        include_internal_fields: bool,
    ) -> Value {
        let mut output = Map::with_capacity(6);
        self.append_common_fields(&mut output, key_field);
        self.reducer.write_output(&mut output, output_field, include_internal_fields);
        output.insert("asOfCheckpoint".to_owned(), json!(checkpoint_sequence));
        Value::Object(output)
    }

    fn into_output_values(
        &self,
        key_field: &str,
        output_field: &str,
        checkpoint_sequence: i64,
    ) -> (Value, Value) {
        let internal = self.into_output_value(key_field, output_field, checkpoint_sequence, true);
        let public = self.into_output_value(key_field, output_field, checkpoint_sequence, false);
        (internal, public)
    }
}

pub(crate) fn load_stream_job_view_accumulator(
    local_state: &LocalThroughputState,
    handle_id: &str,
    view_name: &str,
    reducer_kind: &str,
    output_field: &str,
    key_field: &str,
    logical_key: &str,
) -> Result<Option<(Map<String, Value>, StreamJobViewAccumulator)>> {
    local_state
        .load_stream_job_view_state(handle_id, view_name, logical_key)?
        .and_then(|view_state| view_state.output.as_object().cloned())
        .map(|output| {
            let accumulator = StreamJobViewAccumulator::from_output(
                &output,
                reducer_kind,
                output_field,
                key_field,
                logical_key,
            )?;
            Ok((output, accumulator))
        })
        .transpose()
}

#[derive(Debug)]
pub(crate) struct StreamJobMaterializedBatch {
    pub(crate) projection_records: Vec<StreamProjectionRecord>,
    pub(crate) changelog_records: Vec<StreamChangelogRecord>,
    pub(crate) owner_view_updates: Vec<LocalStreamJobViewState>,
}

#[derive(Debug, Clone)]
pub(crate) struct BoundedDataflowItem {
    pub(crate) logical_key: String,
    pub(crate) value: f64,
    pub(crate) display_key: Option<String>,
    pub(crate) window_start: Option<String>,
    pub(crate) window_end: Option<String>,
    pub(crate) count: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct BoundedDataflowBatchProgram {
    pub(crate) stream_partition_id: i32,
    pub(crate) checkpoint_partition_id: Option<i32>,
    pub(crate) source_owner_partition_id: Option<i32>,
    pub(crate) source_lease_token: Option<String>,
    pub(crate) routing_key: String,
    pub(crate) key_field: String,
    pub(crate) reducer_kind: String,
    pub(crate) output_field: String,
    pub(crate) view_name: String,
    pub(crate) additional_view_names: Vec<String>,
    pub(crate) eventual_projection_view_names: Vec<String>,
    pub(crate) workflow_signals: Vec<stream_jobs::StreamJobWorkflowSignalPlan>,
    pub(crate) checkpoint_name: String,
    pub(crate) checkpoint_sequence: i64,
    pub(crate) owner_epoch: u64,
    pub(crate) occurred_at: DateTime<Utc>,
    pub(crate) items: Vec<BoundedDataflowItem>,
    pub(crate) is_final_partition_batch: bool,
    pub(crate) completes_dispatch: bool,
}

#[derive(Debug)]
pub(crate) struct PreparedStreamJobDataflowApply {
    pub(crate) activation_result: StreamJobActivationResult,
    pub(crate) owner_view_updates: Vec<LocalStreamJobViewState>,
    pub(crate) mirrored_stream_entries: Vec<StreamsChangelogEntry>,
    pub(crate) accepted_progress_state: LocalStreamJobAcceptedProgressState,
    pub(crate) stream_partition_id: i32,
    pub(crate) state_key_delta: u64,
    pub(crate) batch_id: String,
    pub(crate) compact_checkpoint_mirrors: bool,
    pub(crate) completes_dispatch: bool,
    pub(crate) occurred_at: DateTime<Utc>,
}

pub(crate) fn accepted_progress_state_for_batch(
    handle: &StreamJobBridgeHandleRecord,
    batch: &stream_jobs::StreamJobDataflowBatch,
) -> LocalStreamJobAcceptedProgressState {
    let record = fabrik_throughput::AcceptedProgressRecord {
        plan_id: batch.envelope.plan_id.clone(),
        run_id: Some(handle.run_id.clone()),
        job_id: Some(handle.job_id.clone()),
        source_id: batch.envelope.source_id.clone(),
        source_partition_id: batch.envelope.source_partition_id,
        source_epoch: batch.envelope.source_epoch,
        offset_range: batch.envelope.offset_range.clone(),
        edge_id: batch.envelope.edge_id.clone(),
        dest_partition_id: batch.envelope.dest_partition_id,
        batch_id: batch.envelope.batch_id.clone(),
        owner_epoch: batch.envelope.owner_epoch,
        lease_epoch: batch.envelope.lease_epoch,
        summary: batch.envelope.summary.clone(),
    };
    LocalStreamJobAcceptedProgressState {
        handle_id: handle.handle_id.clone(),
        stream_partition_id: batch.program.stream_partition_id,
        accepted_progress_position: 0,
        batch_id: batch.envelope.batch_id.clone(),
        ack: fabrik_throughput::OwnerApplyAck {
            plan_id: record.plan_id.clone(),
            batch_id: record.batch_id.clone(),
            dest_partition_id: record.dest_partition_id,
            owner_epoch: record.owner_epoch,
            durable: false,
            accepted_progress_position: 0,
        },
        record,
        accepted_at: batch.program.occurred_at,
    }
}

pub(crate) fn view_batch_update_changelog_record(
    handle: &StreamJobBridgeHandleRecord,
    routing_key: &str,
    updates: Vec<StreamJobViewBatchUpdate>,
    checkpoint_sequence: i64,
    updated_at: DateTime<Utc>,
) -> StreamChangelogRecord {
    let key = throughput_partition_key(routing_key, 0);
    let entry = StreamsChangelogEntry {
        entry_id: Uuid::now_v7(),
        occurred_at: updated_at,
        partition_key: key.clone(),
        payload: StreamsChangelogPayload::StreamJobViewBatchUpdated {
            handle_id: handle.handle_id.clone(),
            job_id: handle.job_id.clone(),
            updates,
            checkpoint_sequence,
            updated_at,
        },
    };
    StreamChangelogRecord::streams(key, entry)
}

pub(crate) fn view_update_changelog_record(
    handle: &StreamJobBridgeHandleRecord,
    view_name: &str,
    routing_key: &str,
    logical_key: &str,
    output: Value,
    checkpoint_sequence: i64,
    updated_at: DateTime<Utc>,
) -> StreamChangelogRecord {
    view_batch_update_changelog_record(
        handle,
        routing_key,
        vec![StreamJobViewBatchUpdate {
            view_name: view_name.to_owned(),
            logical_key: logical_key.to_owned(),
            output,
        }],
        checkpoint_sequence,
        updated_at,
    )
}

pub(crate) fn materialize_stream_job_batch_updates(
    handle: &StreamJobBridgeHandleRecord,
    program: &BoundedDataflowBatchProgram,
    updated_outputs: Vec<(String, StreamJobViewAccumulator)>,
    mirror_owner_state: bool,
) -> StreamJobMaterializedBatch {
    let view_count = 1 + program.additional_view_names.len();
    let use_batched_materialization = true;
    let mut projection_records =
        Vec::with_capacity(updated_outputs.len() * program.eventual_projection_view_names.len());
    let mut changelog_records = Vec::with_capacity(updated_outputs.len() * view_count);
    let mut changelog_updates = Vec::with_capacity(updated_outputs.len() * view_count);
    let mut owner_view_updates = if mirror_owner_state {
        Vec::with_capacity(updated_outputs.len() * view_count)
    } else {
        Vec::new()
    };
    let should_project_view = |view_name: &str| {
        program.source_lease_token.is_some()
            || !use_batched_materialization
            || program.eventual_projection_view_names.iter().any(|candidate| candidate == view_name)
    };
    for (logical_key, output) in updated_outputs {
        let (internal_output, public_output) = output.into_output_values(
            &program.key_field,
            &program.output_field,
            program.checkpoint_sequence,
        );
        let partition_key = throughput_partition_key(&logical_key, 0);

        if program.additional_view_names.is_empty() {
            if mirror_owner_state {
                owner_view_updates.push(LocalStreamJobViewState {
                    handle_id: handle.handle_id.clone(),
                    job_id: handle.job_id.clone(),
                    view_name: program.view_name.clone(),
                    logical_key: logical_key.clone(),
                    output: internal_output.clone(),
                    checkpoint_sequence: program.checkpoint_sequence,
                    updated_at: program.occurred_at,
                });
            }
            if should_project_view(&program.view_name) {
                projection_records.push(StreamProjectionRecord::streams(
                    partition_key,
                    StreamsProjectionEvent::UpsertStreamJobView {
                        view: StreamsViewRecord {
                            tenant_id: handle.tenant_id.clone(),
                            instance_id: handle.instance_id.clone(),
                            run_id: handle.run_id.clone(),
                            job_id: handle.job_id.clone(),
                            handle_id: handle.handle_id.clone(),
                            view_name: program.view_name.clone(),
                            logical_key: logical_key.clone(),
                            output: public_output.clone(),
                            checkpoint_sequence: program.checkpoint_sequence,
                            updated_at: program.occurred_at,
                        },
                    },
                ));
            }
            if use_batched_materialization {
                changelog_updates.push(StreamJobViewBatchUpdate {
                    view_name: program.view_name.clone(),
                    logical_key,
                    output: internal_output,
                });
            } else {
                changelog_records.push(view_update_changelog_record(
                    handle,
                    &program.view_name,
                    &logical_key,
                    &logical_key,
                    internal_output,
                    program.checkpoint_sequence,
                    program.occurred_at,
                ));
            }
            continue;
        }

        for view_name in
            std::iter::once(&program.view_name).chain(program.additional_view_names.iter())
        {
            if mirror_owner_state {
                owner_view_updates.push(LocalStreamJobViewState {
                    handle_id: handle.handle_id.clone(),
                    job_id: handle.job_id.clone(),
                    view_name: view_name.clone(),
                    logical_key: logical_key.clone(),
                    output: internal_output.clone(),
                    checkpoint_sequence: program.checkpoint_sequence,
                    updated_at: program.occurred_at,
                });
            }
            if should_project_view(view_name) {
                projection_records.push(StreamProjectionRecord::streams(
                    partition_key.clone(),
                    StreamsProjectionEvent::UpsertStreamJobView {
                        view: StreamsViewRecord {
                            tenant_id: handle.tenant_id.clone(),
                            instance_id: handle.instance_id.clone(),
                            run_id: handle.run_id.clone(),
                            job_id: handle.job_id.clone(),
                            handle_id: handle.handle_id.clone(),
                            view_name: view_name.clone(),
                            logical_key: logical_key.clone(),
                            output: public_output.clone(),
                            checkpoint_sequence: program.checkpoint_sequence,
                            updated_at: program.occurred_at,
                        },
                    },
                ));
            }
            if use_batched_materialization {
                changelog_updates.push(StreamJobViewBatchUpdate {
                    view_name: view_name.clone(),
                    logical_key: logical_key.clone(),
                    output: internal_output.clone(),
                });
            } else {
                changelog_records.push(view_update_changelog_record(
                    handle,
                    view_name,
                    &logical_key,
                    &logical_key,
                    internal_output.clone(),
                    program.checkpoint_sequence,
                    program.occurred_at,
                ));
            }
        }
    }
    if use_batched_materialization && !changelog_updates.is_empty() {
        changelog_records.push(view_batch_update_changelog_record(
            handle,
            &program.routing_key,
            changelog_updates,
            program.checkpoint_sequence,
            program.occurred_at,
        ));
    }
    StreamJobMaterializedBatch { projection_records, changelog_records, owner_view_updates }
}

pub(crate) fn apply_stream_job_batch_program_in_memory(
    accumulators: &mut HashMap<String, StreamJobViewAccumulator>,
    program: &BoundedDataflowBatchProgram,
) -> Result<Vec<(String, StreamJobViewAccumulator)>> {
    let mut updated_outputs = Vec::with_capacity(program.items.len());
    for item in &program.items {
        let output = if let Some(existing) = accumulators.get_mut(&item.logical_key) {
            existing
        } else {
            accumulators.insert(
                item.logical_key.clone(),
                StreamJobViewAccumulator::from_item(item, &program.reducer_kind)?,
            );
            accumulators
                .get_mut(&item.logical_key)
                .expect("stream job accumulator should exist after insert")
        };
        output.apply_item(item);
        updated_outputs.push((item.logical_key.clone(), output.clone()));
    }
    Ok(updated_outputs)
}

pub(crate) fn workflow_signal_payload(
    handle: &StreamJobBridgeHandleRecord,
    signal: &stream_jobs::StreamJobWorkflowSignalPlan,
    logical_key: &str,
    output: Value,
) -> Value {
    json!({
        "jobId": handle.job_id,
        "handleId": handle.handle_id,
        "jobName": handle.job_name,
        "operatorId": signal.operator_id,
        "viewName": signal.view_name,
        "logicalKey": logical_key,
        "output": output,
    })
}

pub(crate) fn workflow_signaled_changelog_record(
    handle: &StreamJobBridgeHandleRecord,
    signal: &stream_jobs::StreamJobWorkflowSignalPlan,
    logical_key: &str,
    payload: Value,
    owner_epoch: u64,
    signaled_at: DateTime<Utc>,
) -> StreamChangelogRecord {
    let key = throughput_partition_key(logical_key, 0);
    let signal_id = stream_job_signal_callback_dedupe_key(
        &handle.tenant_id,
        &handle.instance_id,
        &handle.run_id,
        &handle.job_id,
        &signal.operator_id,
        logical_key,
    );
    let entry = StreamsChangelogEntry {
        entry_id: Uuid::now_v7(),
        occurred_at: signaled_at,
        partition_key: key.clone(),
        payload: StreamsChangelogPayload::StreamJobWorkflowSignaled {
            handle_id: handle.handle_id.clone(),
            job_id: handle.job_id.clone(),
            operator_id: signal.operator_id.clone(),
            view_name: signal.view_name.clone(),
            logical_key: logical_key.to_owned(),
            signal_id,
            signal_type: signal.signal_type.clone(),
            payload,
            owner_epoch,
            signaled_at,
        },
    };
    StreamChangelogRecord::streams(key, entry)
}

pub(crate) fn checkpoint_reached_changelog_record(
    handle: &StreamJobBridgeHandleRecord,
    routing_key: &str,
    checkpoint_name: &str,
    checkpoint_sequence: i64,
    stream_partition_id: i32,
    owner_epoch: u64,
    reached_at: DateTime<Utc>,
) -> StreamChangelogRecord {
    let key = throughput_partition_key(routing_key, 0);
    let entry = StreamsChangelogEntry {
        entry_id: Uuid::now_v7(),
        occurred_at: reached_at,
        partition_key: key.clone(),
        payload: StreamsChangelogPayload::StreamJobCheckpointReached {
            handle_id: handle.handle_id.clone(),
            job_id: handle.job_id.clone(),
            checkpoint_name: checkpoint_name.to_owned(),
            checkpoint_sequence,
            stream_partition_id,
            owner_epoch,
            reached_at,
        },
    };
    StreamChangelogRecord::streams(key, entry)
}

impl<'a> LocalDataflowEngine<'a> {
    pub(crate) fn new(local_state: &'a LocalThroughputState) -> Self {
        Self { local_state }
    }

    pub(crate) fn apply_stream_job_batch_list(
        &self,
        handle: &StreamJobBridgeHandleRecord,
        runtime_state_override: Option<&crate::local_state::LocalStreamJobRuntimeState>,
        batch_list: &[stream_jobs::StreamJobDataflowBatch],
        mirror_owner_state: bool,
    ) -> Result<stream_jobs::StreamJobDataflowApplyResult> {
        let bounded_batch_persist = mirror_owner_state
            && !batch_list.is_empty()
            && batch_list.iter().all(|batch| batch.program.source_lease_token.is_none());
        if !bounded_batch_persist {
            let mut batch_results = Vec::with_capacity(batch_list.len());
            for batch in batch_list {
                batch_results.push(self.apply_stream_job_batch(
                    handle,
                    runtime_state_override,
                    batch,
                    mirror_owner_state,
                )?);
            }
            return Ok(stream_jobs::StreamJobDataflowApplyResult { batch_results });
        }

        let mut batch_results = Vec::with_capacity(batch_list.len());
        let mut checkpoint_expected_by_batch_id = HashMap::<String, i32>::new();
        let mut batched_owner_view_updates = Vec::new();
        let mut batched_stream_entries = Vec::new();
        let mut batched_accepted_progress = Vec::new();
        let mut batched_dispatch_batch_ids = Vec::new();
        let mut batched_partition_state_key_deltas = BTreeMap::<i32, u64>::new();
        let mut batched_completes_dispatch = false;
        let mut batched_updated_at =
            runtime_state_override.map(|state| state.updated_at).unwrap_or_else(Utc::now);

        for batch in batch_list {
            let Some(prepared) = self.prepare_stream_job_batch_apply(
                handle,
                batch,
                mirror_owner_state,
                runtime_state_override,
                None,
            )?
            else {
                continue;
            };
            let emits_sealed_checkpoint = prepared.mirrored_stream_entries.iter().any(|entry| {
                matches!(
                    entry.payload,
                    fabrik_throughput::StreamsChangelogPayload::StreamJobCheckpointReached {
                        stream_partition_id,
                        ..
                    } if stream_partition_id == prepared.stream_partition_id
                )
            });
            batch_results.push(stream_jobs::StreamJobDataflowBatchApplyResult {
                batch: batch.clone(),
                activation_result: prepared.activation_result,
                owner_apply_ack: None,
                sealed_checkpoint: None,
            });
            if emits_sealed_checkpoint {
                checkpoint_expected_by_batch_id
                    .insert(batch.envelope.batch_id.clone(), prepared.stream_partition_id);
            }
            batched_owner_view_updates.extend(prepared.owner_view_updates);
            batched_stream_entries.extend(prepared.mirrored_stream_entries);
            batched_accepted_progress.push(prepared.accepted_progress_state);
            batched_dispatch_batch_ids.push(prepared.batch_id);
            if prepared.state_key_delta > 0 {
                let entry = batched_partition_state_key_deltas
                    .entry(prepared.stream_partition_id)
                    .or_default();
                *entry = entry.saturating_add(prepared.state_key_delta);
            }
            batched_completes_dispatch |= prepared.completes_dispatch;
            batched_updated_at = batched_updated_at.max(prepared.occurred_at);
        }

        if batched_dispatch_batch_ids.is_empty() {
            return Ok(stream_jobs::StreamJobDataflowApplyResult { batch_results });
        }

        let applied = self.local_state.persist_stream_job_activation_apply(
            &handle.handle_id,
            &batched_dispatch_batch_ids,
            true,
            batched_completes_dispatch,
            batched_updated_at,
            batched_owner_view_updates,
            batched_accepted_progress,
            &batched_stream_entries,
        )?;
        if !batched_partition_state_key_deltas.is_empty() {
            let Some(mut runtime_state) =
                self.local_state.load_stream_job_runtime_state(&handle.handle_id)?
            else {
                anyhow::bail!("stream job runtime state missing for handle {}", handle.handle_id);
            };
            for (stream_partition_id, state_key_delta) in batched_partition_state_key_deltas {
                runtime_state.record_owner_partition_state_key_delta(
                    stream_partition_id,
                    state_key_delta,
                    batched_updated_at,
                );
            }
            runtime_state.updated_at = batched_updated_at;
            self.local_state.upsert_stream_job_runtime_state(&runtime_state)?;
        }

        let mut ack_by_batch_id = applied
            .accepted_progress
            .into_iter()
            .map(|state| (state.batch_id, state.ack))
            .collect::<HashMap<_, _>>();
        let mut sealed_checkpoints_by_partition = applied.sealed_checkpoints.into_iter().fold(
            HashMap::<i32, VecDeque<fabrik_throughput::SealedCheckpointRecord>>::new(),
            |mut acc, state| {
                acc.entry(state.stream_partition_id).or_default().push_back(state.record);
                acc
            },
        );
        for batch_result in &mut batch_results {
            batch_result.owner_apply_ack =
                ack_by_batch_id.remove(&batch_result.batch.envelope.batch_id);
            if let Some(stream_partition_id) =
                checkpoint_expected_by_batch_id.get(&batch_result.batch.envelope.batch_id)
            {
                batch_result.sealed_checkpoint = sealed_checkpoints_by_partition
                    .get_mut(stream_partition_id)
                    .and_then(VecDeque::pop_front);
            }
        }
        Ok(stream_jobs::StreamJobDataflowApplyResult { batch_results })
    }

    pub(crate) fn apply_stream_job_batch(
        &self,
        handle: &StreamJobBridgeHandleRecord,
        runtime_state_override: Option<&crate::local_state::LocalStreamJobRuntimeState>,
        batch: &stream_jobs::StreamJobDataflowBatch,
        mirror_owner_state: bool,
    ) -> Result<stream_jobs::StreamJobDataflowBatchApplyResult> {
        let Some(prepared) = self.prepare_stream_job_batch_apply(
            handle,
            batch,
            mirror_owner_state,
            runtime_state_override,
            None,
        )?
        else {
            return Ok(stream_jobs::StreamJobDataflowBatchApplyResult {
                batch: batch.clone(),
                activation_result: StreamJobActivationResult {
                    projection_records: Vec::new(),
                    changelog_records: Vec::new(),
                },
                owner_apply_ack: None,
                sealed_checkpoint: None,
            });
        };
        let mut activation_result = prepared.activation_result;
        let mut owner_apply_ack = None;
        let mut sealed_checkpoint = None;
        if mirror_owner_state {
            let applied = self.local_state.persist_stream_job_batch_apply(
                &handle.handle_id,
                &prepared.batch_id,
                prepared.compact_checkpoint_mirrors,
                prepared.completes_dispatch,
                prepared.occurred_at,
                prepared.owner_view_updates,
                vec![prepared.accepted_progress_state],
                &prepared.mirrored_stream_entries,
            )?;
            owner_apply_ack = applied.accepted_progress.last().map(|state| state.ack.clone());
            sealed_checkpoint = applied
                .sealed_checkpoints
                .into_iter()
                .find(|state| state.stream_partition_id == prepared.stream_partition_id)
                .map(|state| state.record);
            if prepared.state_key_delta > 0 {
                let Some(mut runtime_state) =
                    self.local_state.load_stream_job_runtime_state(&handle.handle_id)?
                else {
                    anyhow::bail!(
                        "stream job runtime state missing for handle {}",
                        handle.handle_id
                    );
                };
                runtime_state.record_owner_partition_state_key_delta(
                    prepared.stream_partition_id,
                    prepared.state_key_delta,
                    prepared.occurred_at,
                );
                runtime_state.updated_at = prepared.occurred_at;
                self.local_state.upsert_stream_job_runtime_state(&runtime_state)?;
            }
            for record in &mut activation_result.changelog_records {
                if matches!(
                    &record.entry,
                    crate::ChangelogRecordEntry::Streams(entry)
                        if !matches!(
                            entry.payload,
                            fabrik_throughput::StreamsChangelogPayload::StreamJobViewUpdated { .. }
                                | fabrik_throughput::StreamsChangelogPayload::StreamJobViewBatchUpdated { .. }
                        )
                ) {
                    record.local_mirror_applied = true;
                    record.partition_mirror_applied = true;
                }
            }
        }
        Ok(stream_jobs::StreamJobDataflowBatchApplyResult {
            batch: batch.clone(),
            activation_result,
            owner_apply_ack,
            sealed_checkpoint,
        })
    }

    pub(crate) fn prepare_stream_job_batch_apply(
        &self,
        handle: &StreamJobBridgeHandleRecord,
        batch: &stream_jobs::StreamJobDataflowBatch,
        mirror_owner_state: bool,
        runtime_state_override: Option<&crate::local_state::LocalStreamJobRuntimeState>,
        mut metrics: Option<&mut stream_jobs::StreamJobApplyMetrics>,
    ) -> Result<Option<PreparedStreamJobDataflowApply>> {
        let program = &batch.program;
        if self
            .local_state
            .has_stream_job_applied_dispatch_batch(&handle.handle_id, &batch.envelope.batch_id)?
        {
            return Ok(None);
        }
        let loaded_runtime_state = if runtime_state_override.is_none() {
            self.local_state.load_stream_job_runtime_state(&handle.handle_id)?
        } else {
            None
        };
        if let (Some(source_partition_id), Some(source_lease_token)) =
            (program.checkpoint_partition_id, program.source_lease_token.as_ref())
        {
            if let Some(metrics) = metrics.as_deref_mut() {
                metrics.runtime_state_loads += 1;
            }
            let Some(runtime_state) = runtime_state_override.or(loaded_runtime_state.as_ref())
            else {
                return Ok(None);
            };
            let Some(lease) = runtime_state
                .source_partition_leases
                .iter()
                .find(|lease| lease.source_partition_id == source_partition_id)
            else {
                return Ok(None);
            };
            if lease.lease_token != *source_lease_token
                || program.source_owner_partition_id.is_some_and(|owner_partition_id| {
                    lease.owner_partition_id != owner_partition_id
                })
            {
                return Ok(None);
            }
        }

        let accumulate_started = std::time::Instant::now();
        let mut updated_outputs =
            HashMap::<String, StreamJobViewAccumulator>::with_capacity(program.items.len());
        let mut prior_outputs =
            HashMap::<String, Option<Map<String, Value>>>::with_capacity(program.items.len());
        for item in &program.items {
            if let Some(output) = updated_outputs.get_mut(&item.logical_key) {
                output.apply_item(item);
                continue;
            }

            let load_started = std::time::Instant::now();
            let output = load_stream_job_view_accumulator(
                self.local_state,
                &handle.handle_id,
                &program.view_name,
                &program.reducer_kind,
                &program.output_field,
                &program.key_field,
                &item.logical_key,
            )?;
            if let Some(metrics) = metrics.as_deref_mut() {
                metrics.view_state_loads += 1;
                metrics.load_existing_state_elapsed += load_started.elapsed();
                if output.is_some() {
                    metrics.view_state_hits += 1;
                } else {
                    metrics.view_state_misses += 1;
                }
            }
            let (previous_output, mut output) = match output {
                Some((previous_output, accumulator)) => (Some(previous_output), accumulator),
                None => (None, StreamJobViewAccumulator::from_item(item, &program.reducer_kind)?),
            };
            output.apply_item(item);
            prior_outputs.insert(item.logical_key.clone(), previous_output);
            updated_outputs.insert(item.logical_key.clone(), output);
        }
        if let Some(metrics) = metrics.as_deref_mut() {
            metrics.accumulate_elapsed += accumulate_started.elapsed();
        }

        let materialize_started = std::time::Instant::now();
        let signal_records = updated_outputs
            .iter()
            .flat_map(|(logical_key, output)| {
                let previous_output =
                    prior_outputs.get(logical_key).and_then(|value| value.as_ref());
                let public_output = sanitize_stream_view_output(output.into_output_value(
                    &program.key_field,
                    &program.output_field,
                    program.checkpoint_sequence,
                    true,
                ));
                program.workflow_signals.iter().filter_map(move |signal| {
                    if signal.view_name != program.view_name
                        && !program
                            .additional_view_names
                            .iter()
                            .any(|view_name| view_name == &signal.view_name)
                    {
                        return None;
                    }
                    if self
                        .local_state
                        .load_stream_job_workflow_signal_state(
                            &handle.handle_id,
                            &signal.operator_id,
                            logical_key,
                        )
                        .ok()
                        .flatten()
                        .is_some()
                    {
                        return None;
                    }
                    if !signal_output_matches(&public_output, signal.when_output_field.as_deref()) {
                        return None;
                    }
                    if previous_output.is_some_and(|output| {
                        signal_output_matches(
                            &Value::Object(output.clone()),
                            signal.when_output_field.as_deref(),
                        )
                    }) {
                        return None;
                    }
                    Some(workflow_signaled_changelog_record(
                        handle,
                        signal,
                        logical_key,
                        workflow_signal_payload(handle, signal, logical_key, public_output.clone()),
                        program.owner_epoch,
                        program.occurred_at,
                    ))
                })
            })
            .collect::<Vec<_>>();
        let materialized = materialize_stream_job_batch_updates(
            handle,
            program,
            updated_outputs.into_iter().collect(),
            mirror_owner_state,
        );
        if let Some(metrics) = metrics.as_deref_mut() {
            metrics.materialize_elapsed += materialize_started.elapsed();
            metrics.projection_record_count += materialized.projection_records.len();
            metrics.changelog_record_count +=
                materialized.changelog_records.len() + signal_records.len();
            metrics.owner_view_update_count += materialized.owner_view_updates.len();
        }
        let projection_records = materialized.projection_records;
        let mut changelog_records = materialized.changelog_records;
        changelog_records.extend(signal_records);
        let owner_view_updates = materialized.owner_view_updates;
        let state_key_delta =
            prior_outputs.values().filter(|output| output.is_none()).count() as u64;
        let accepted_progress_state = accepted_progress_state_for_batch(handle, batch);
        if program.is_final_partition_batch && !program.checkpoint_name.is_empty() {
            changelog_records.push(checkpoint_reached_changelog_record(
                handle,
                &program.routing_key,
                &program.checkpoint_name,
                program.checkpoint_sequence,
                program.checkpoint_partition_id.unwrap_or(program.stream_partition_id),
                program.owner_epoch,
                program.occurred_at,
            ));
            if let Some(metrics) = metrics.as_deref_mut() {
                metrics.changelog_record_count += 1;
            }
        }
        let mirrored_stream_entries = changelog_records
            .iter()
            .filter_map(|record| match &record.entry {
                crate::ChangelogRecordEntry::Streams(entry)
                    if !matches!(
                        entry.payload,
                        fabrik_throughput::StreamsChangelogPayload::StreamJobViewUpdated { .. }
                            | fabrik_throughput::StreamsChangelogPayload::StreamJobViewBatchUpdated { .. }
                    ) =>
                {
                    Some(entry.clone())
                }
                crate::ChangelogRecordEntry::Throughput(_) => None,
                crate::ChangelogRecordEntry::Streams(_) => None,
            })
            .collect::<Vec<_>>();
        if mirror_owner_state {
            for record in &mut changelog_records {
                if matches!(
                    &record.entry,
                    crate::ChangelogRecordEntry::Streams(entry)
                        if !matches!(
                            entry.payload,
                            fabrik_throughput::StreamsChangelogPayload::StreamJobViewUpdated { .. }
                                | fabrik_throughput::StreamsChangelogPayload::StreamJobViewBatchUpdated { .. }
                        )
                ) {
                    record.local_mirror_applied = true;
                    record.partition_mirror_applied = true;
                }
            }
        }
        Ok(Some(PreparedStreamJobDataflowApply {
            activation_result: StreamJobActivationResult { projection_records, changelog_records },
            owner_view_updates,
            mirrored_stream_entries,
            accepted_progress_state,
            stream_partition_id: program.stream_partition_id,
            state_key_delta,
            batch_id: batch.envelope.batch_id.clone(),
            compact_checkpoint_mirrors: program.source_lease_token.is_none(),
            completes_dispatch: program.completes_dispatch,
            occurred_at: program.occurred_at,
        }))
    }

    pub(crate) fn execute_stream_job_plan(
        &self,
        handle: &StreamJobBridgeHandleRecord,
        plan: stream_jobs::StreamJobActivationPlan,
        occurred_at: DateTime<Utc>,
    ) -> Result<StreamJobActivationResult> {
        let mut projection_records = Vec::new();
        let mut changelog_records = self.stage_stream_job_plan(handle, &plan)?;

        if handle.cancellation_requested_at.is_some() {
            self.local_state.mark_stream_job_dispatch_cancelled(
                &handle.handle_id,
                handle.cancellation_requested_at.unwrap_or(occurred_at),
            )?;
        } else {
            let applied =
                self.apply_stream_job_batch_list(handle, None, &plan.dataflow_batches, true)?;
            let activation_result = applied.into_activation_result();
            projection_records.extend(activation_result.projection_records);
            changelog_records.extend(activation_result.changelog_records);
        }

        self.finish_stream_job_plan(
            handle,
            plan,
            occurred_at,
            &mut projection_records,
            &mut changelog_records,
        )?;
        Ok(StreamJobActivationResult { projection_records, changelog_records })
    }

    pub(crate) fn runtime_state_allows_terminal_publish(&self, handle_id: &str) -> Result<bool> {
        Ok(self.local_state.load_stream_job_runtime_state(handle_id)?.is_some_and(|state| {
            let bounded_completed =
                state.dispatch_completed_at.is_some() && state.dispatch_cancelled_at.is_none();
            let topic_cancelled = state
                .source_kind
                .as_deref()
                .is_some_and(|kind| kind == fabrik_throughput::STREAM_SOURCE_TOPIC);
            bounded_completed || topic_cancelled
        }))
    }

    pub(crate) fn stage_stream_job_plan(
        &self,
        handle: &StreamJobBridgeHandleRecord,
        plan: &stream_jobs::StreamJobActivationPlan,
    ) -> Result<Vec<StreamChangelogRecord>> {
        let mut changelog_records = Vec::with_capacity(
            plan.dataflow_batches.len().saturating_add(plan.post_apply_changelog_records.len() + 2),
        );
        if let Some(execution_planned) = plan.execution_planned.clone() {
            let crate::ChangelogRecordEntry::Streams(streams_entry) = &execution_planned.entry
            else {
                anyhow::bail!("stream job execution plan must use a streams changelog entry");
            };
            self.local_state.mirror_streams_changelog_entry(streams_entry)?;
            if let Some(runtime_state) = &plan.runtime_state_update {
                self.local_state.upsert_stream_job_runtime_state(runtime_state)?;
            }
            if !plan.dataflow_batches.is_empty() {
                self.local_state.replace_stream_job_dispatch_manifest(
                    &handle.handle_id,
                    plan.dataflow_batches
                        .iter()
                        .map(stream_jobs::local_dispatch_batch_from_dataflow_batch)
                        .collect(),
                    streams_entry.occurred_at,
                )?;
            }
            changelog_records.push(execution_planned);
        } else if let Some(runtime_state) = &plan.runtime_state_update {
            self.local_state.upsert_stream_job_runtime_state(runtime_state)?;
        }
        Ok(changelog_records)
    }

    pub(crate) fn finish_stream_job_plan(
        &self,
        handle: &StreamJobBridgeHandleRecord,
        plan: stream_jobs::StreamJobActivationPlan,
        occurred_at: DateTime<Utc>,
        projection_records: &mut Vec<crate::StreamProjectionRecord>,
        changelog_records: &mut Vec<StreamChangelogRecord>,
    ) -> Result<()> {
        if let Some(source_cursors) = plan.source_cursor_updates {
            self.local_state.replace_stream_job_source_cursors(
                &handle.handle_id,
                source_cursors,
                occurred_at,
            )?;
        }
        projection_records.extend(plan.post_apply_projection_records);
        changelog_records.extend(plan.post_apply_changelog_records);
        if let Some(terminalized) = plan.terminalized
            && self.runtime_state_allows_terminal_publish(&handle.handle_id)?
        {
            changelog_records.push(terminalized);
        }
        Ok(())
    }
}
