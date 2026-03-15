use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use fabrik_broker::{
    ConsumedJsonRecord, JsonPartitionFetcher, JsonTopicConfig, build_json_consumer_from_offsets,
    build_json_partition_fetcher, decode_json_record, load_json_topic_latest_offsets,
};
use fabrik_store::{
    StreamJobBridgeHandleRecord, StreamJobBridgeLedgerRecord, StreamJobCheckpointRecord,
    StreamJobQueryRecord, StreamJobRecord, StreamJobViewDeleteRecord,
    StreamJobViewProjectionSummaryRecord, StreamJobViewRecord, WorkflowSignalStatus, WorkflowStore,
};
use fabrik_throughput::{
    CompiledAggregateV2Kernel, CompiledAggregateV2PreKeyOperator, CompiledStreamJob,
    CompiledStreamOperator, CompiledStreamQuery, CompiledStreamSource, CompiledStreamState,
    CompiledStreamView, DataflowBatchEnvelope, DataflowBatchSummary, DataflowBridgeCallbackRecord,
    DataflowOffsetRange, DataflowSourceFrontier, OwnerApplyAck, STREAM_CONSISTENCY_STRONG,
    STREAM_JOB_KEYED_ROLLUP, STREAM_QUERY_MODE_BY_KEY, STREAM_QUERY_MODE_PREFIX_SCAN,
    STREAM_REDUCER_AVG, STREAM_REDUCER_COUNT, STREAM_REDUCER_HISTOGRAM, STREAM_REDUCER_MAX,
    STREAM_REDUCER_MIN, STREAM_REDUCER_SUM, STREAM_REDUCER_THRESHOLD, STREAM_RUNTIME_AGGREGATE_V2,
    STREAM_RUNTIME_KEYED_ROLLUP, STREAM_SOURCE_BOUNDED_INPUT, STREAM_SOURCE_TOPIC,
    STREAMS_DATAFLOW_V1_CONTRACT, SealedCheckpointRecord, StreamJobBridgeHandleStatus,
    StreamJobBridgeRepairKind, StreamJobOriginKind, StreamJobQueryConsistency,
    StreamJobViewBatchUpdate, StreamsChangelogEntry, StreamsChangelogPayload,
    StreamsProjectionEvent, StreamsViewDeleteRecord, StrongReadCheckpointRef, StrongReadMetadata,
    stream_job_callback_event_id, stream_job_signal_callback_dedupe_key,
};
use futures_util::StreamExt;
use serde::{
    Deserialize, Serialize,
    de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use tokio::time::{Duration, timeout};
use uuid::Uuid;

use crate::local_state::{
    LocalStreamJobBridgeCallbackState, LocalStreamJobDispatchBatch, LocalStreamJobDispatchItem,
    LocalStreamJobHotKeyRuntimeStatsState, LocalStreamJobOwnerPartitionRuntimeStatsState,
    LocalStreamJobPreKeyRuntimeStatsState, LocalStreamJobRouteBranchRuntimeStatsState,
    LocalStreamJobRuntimeState, LocalStreamJobSourceCursorState, LocalStreamJobSourceLeaseState,
    LocalStreamJobViewState, LocalStreamJobWorkflowSignalState,
};
use crate::{
    AppState, LocalThroughputState, StreamChangelogRecord, StreamJobActivationResult,
    StreamJobTerminalResult, StreamProjectionRecord, owner_local_state_for_stream_job,
    owner_local_state_for_stream_key, throughput_partition_for_stream_key,
    throughput_partition_key,
};

pub(crate) const KEYED_ROLLUP_JOB_NAME: &str = STREAM_JOB_KEYED_ROLLUP;
pub(crate) const KEYED_ROLLUP_CHECKPOINT_NAME: &str = "initial-rollup-ready";
pub(crate) const KEYED_ROLLUP_CHECKPOINT_SEQUENCE: i64 = 1;
pub(crate) const STREAM_JOB_BRIDGE_STATE_QUERY_NAME: &str = "__bridge_state";
pub(crate) const STREAM_JOB_PROJECTION_STATS_QUERY_NAME: &str = "__projection_stats";
pub(crate) const STREAM_JOB_RUNTIME_STATS_QUERY_NAME: &str = "__runtime_stats";
pub(crate) const STREAM_JOB_VIEW_RUNTIME_STATS_QUERY_NAME: &str = "__view_runtime_stats";
pub(crate) const STREAM_JOB_VIEW_PROJECTION_STATS_QUERY_NAME: &str = "__view_projection_stats";
const STREAM_INTERNAL_AVG_SUM_FIELD: &str = "_stream_sum";
const STREAM_INTERNAL_AVG_COUNT_FIELD: &str = "_stream_count";
const STREAM_WINDOW_LOGICAL_KEY_SEPARATOR: char = '\u{1f}';
const STREAM_JOB_HOT_KEY_RUNTIME_STATS_LIMIT: usize = 8;
const STREAM_JOB_TOPIC_POLL_MAX_RECORDS: usize = 4_096;
const STREAM_JOB_TOPIC_IDLE_WAIT_MS: u64 = 5;
const STREAM_JOB_TOPIC_FRONTIER_REFRESH_INTERVAL_MS: i64 = 250;
const STREAM_JOB_TOPIC_POLL_MAX_BATCH_BYTES: i32 = 50 * 1024 * 1024;
const STREAM_JOB_CALLBACK_KIND_CHECKPOINT: &str = "checkpoint";
const STREAM_JOB_CALLBACK_KIND_TERMINAL: &str = "terminal";

#[derive(Debug, Clone)]
pub(crate) struct StreamJobMaterializationOutcome {
    pub checkpoint: Option<StreamJobCheckpointRecord>,
    pub terminal: Option<StreamJobTerminalResult>,
    pub workflow_signals: Vec<LocalStreamJobWorkflowSignalState>,
}

pub(crate) fn should_refresh_topic_frontier(
    existing_runtime_state: Option<&LocalStreamJobRuntimeState>,
    occurred_at: DateTime<Utc>,
) -> bool {
    existing_runtime_state.is_none_or(|runtime_state| {
        let last_progress_at = runtime_state
            .source_cursors
            .iter()
            .map(|cursor| cursor.updated_at)
            .max()
            .unwrap_or(runtime_state.updated_at);
        occurred_at.signed_duration_since(last_progress_at)
            >= ChronoDuration::milliseconds(STREAM_JOB_TOPIC_FRONTIER_REFRESH_INTERVAL_MS)
    })
}

fn topic_frontier_offsets_from_runtime_state(
    runtime_state: &LocalStreamJobRuntimeState,
) -> HashMap<i32, i64> {
    runtime_state
        .source_cursors
        .iter()
        .map(|cursor| {
            let frontier = cursor
                .last_high_watermark
                .unwrap_or(cursor.initial_checkpoint_target_offset)
                .max(cursor.initial_checkpoint_target_offset)
                .max(cursor.next_offset);
            (cursor.source_partition_id, frontier)
        })
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StreamJobSignalBridgeState {
    pub operator_id: String,
    pub view_name: String,
    pub logical_key: String,
    pub signal_id: String,
    pub signal_type: String,
    pub payload: Value,
    pub stream_owner_epoch: u64,
    pub signaled_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub callback_dedupe_key: String,
    pub callback_event_id: String,
    pub target_tenant_id: String,
    pub target_instance_id: String,
    pub target_run_id: Option<String>,
    pub bridge_status: String,
    pub workflow_signal_status: Option<String>,
    pub workflow_signal_dedupe_key: Option<String>,
    pub workflow_signal_source_event_id: Option<String>,
    pub workflow_signal_dispatch_event_id: Option<String>,
    pub workflow_signal_consumed_event_id: Option<String>,
    pub workflow_signal_enqueued_at: Option<DateTime<Utc>>,
    pub workflow_signal_consumed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
struct StreamJobSignalTarget {
    tenant_id: String,
    instance_id: String,
    run_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct StreamJobActivationPlan {
    pub execution_planned: Option<StreamChangelogRecord>,
    pub runtime_state_update: Option<LocalStreamJobRuntimeState>,
    pub dataflow_batches: Vec<StreamJobDataflowBatch>,
    pub post_apply_projection_records: Vec<StreamProjectionRecord>,
    pub post_apply_changelog_records: Vec<StreamChangelogRecord>,
    pub source_cursor_updates: Option<Vec<LocalStreamJobSourceCursorState>>,
    pub terminalized: Option<StreamChangelogRecord>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct StreamJobApplyMetrics {
    pub runtime_state_loads: usize,
    pub view_state_loads: usize,
    pub view_state_hits: usize,
    pub view_state_misses: usize,
    pub projection_record_count: usize,
    pub changelog_record_count: usize,
    pub owner_view_update_count: usize,
    pub mirrored_stream_entry_count: usize,
    pub runtime_state_write_count: usize,
    pub view_state_write_count: usize,
    pub changelog_mirror_write_count: usize,
    pub load_existing_state_elapsed: std::time::Duration,
    pub accumulate_elapsed: std::time::Duration,
    pub materialize_elapsed: std::time::Duration,
    pub persist_elapsed: std::time::Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StreamJobWorkflowSignalPlan {
    pub operator_id: String,
    pub view_name: String,
    pub signal_type: String,
    pub when_output_field: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct StreamJobPartitionItem {
    pub logical_key: String,
    pub value: f64,
    pub display_key: Option<String>,
    pub window_start: Option<String>,
    pub window_end: Option<String>,
    pub count: u64,
}

#[derive(Debug, Deserialize)]
struct TopicInputEnvelope {
    kind: Option<String>,
    topic: Option<String>,
    #[serde(rename = "startOffset")]
    start_offset: Option<String>,
    #[serde(rename = "startOffsets")]
    start_offsets: Option<Vec<TopicPartitionStartOffset>>,
}

#[derive(Debug, Deserialize)]
struct TopicPartitionStartOffset {
    #[serde(rename = "partitionId")]
    partition_id: i32,
    offset: i64,
}

#[derive(Debug, Clone)]
struct TopicSourceSpec {
    topic_name: String,
    start_from_latest: bool,
    explicit_offsets: HashMap<i32, i64>,
}

#[derive(Debug, Clone)]
pub(crate) struct TopicStreamJobPollRequest {
    pub source_partition_id: i32,
    pub source_owner_partition_id: i32,
    pub lease_token: String,
    pub checkpoint_sequence: i64,
    pub start_offset: i64,
    pub checkpoint_target_offset: i64,
    pub idle_window_timer: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct TopicStreamJobPollResult {
    pub runtime_state_update: Option<LocalStreamJobRuntimeState>,
    pub runtime_delta: Option<TopicStreamJobPollRuntimeDelta>,
    pub projection_records: Vec<StreamProjectionRecord>,
    pub owner_input_batches: Vec<StreamJobDataflowBatch>,
    pub changelog_records: Vec<StreamChangelogRecord>,
}

#[derive(Debug, Clone)]
pub(crate) struct TopicStreamJobPollRuntimeDelta {
    pub checkpoint_sequence: i64,
    pub source_cursor_update: LocalStreamJobSourceCursorState,
    pub source_lease_update: LocalStreamJobSourceLeaseState,
    pub pre_key_runtime_stats: Vec<LocalStreamJobPreKeyRuntimeStatsState>,
    pub hot_key_runtime_stats: Vec<LocalStreamJobHotKeyRuntimeStatsState>,
    pub owner_partition_runtime_stats: Vec<LocalStreamJobOwnerPartitionRuntimeStatsState>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub(crate) struct TopicSourceBatchRecord {
    pub logical_key: String,
    pub display_key: String,
    pub window_start: Option<String>,
    pub window_end: Option<String>,
    pub event_time: Option<DateTime<Utc>>,
    pub window_end_at: Option<DateTime<Utc>>,
    pub value: f64,
    pub offset: i64,
    pub high_watermark: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct TopicSourceBatch {
    pub handle_id: String,
    pub job_id: String,
    pub source_partition_id: i32,
    pub source_owner_partition_id: i32,
    pub source_lease_token: String,
    pub checkpoint_sequence: i64,
    pub checkpoint_target_offset: i64,
    pub idle_window_timer: bool,
    pub offset_start: i64,
    pub offset_end: i64,
    pub records: Vec<TopicSourceBatchRecord>,
}

#[derive(Debug, Clone)]
pub(crate) struct StreamJobDataflowBatch {
    pub envelope: DataflowBatchEnvelope,
    pub program: crate::dataflow_engine::BoundedDataflowBatchProgram,
}

#[derive(Debug, Clone)]
struct PlannedBoundedDataflowBatch {
    batch_id: String,
    source_id: String,
    source_partition_id: i32,
    source_epoch: u64,
    offset_range: Option<DataflowOffsetRange>,
    program: crate::dataflow_engine::BoundedDataflowBatchProgram,
}

#[derive(Debug, Clone)]
pub(crate) struct StreamJobDataflowBatchApplyResult {
    pub batch: StreamJobDataflowBatch,
    pub activation_result: StreamJobActivationResult,
    pub owner_apply_ack: Option<OwnerApplyAck>,
    pub sealed_checkpoint: Option<SealedCheckpointRecord>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct StreamJobDataflowApplyResult {
    pub batch_results: Vec<StreamJobDataflowBatchApplyResult>,
}

impl StreamJobDataflowApplyResult {
    pub(crate) fn into_activation_result(self) -> StreamJobActivationResult {
        let mut projection_records = Vec::new();
        let mut changelog_records = Vec::new();
        for batch_result in self.batch_results {
            projection_records.extend(batch_result.activation_result.projection_records);
            changelog_records.extend(batch_result.activation_result.changelog_records);
        }
        StreamJobActivationResult { projection_records, changelog_records }
    }
}

#[derive(Debug, Clone)]
enum StreamJobPreKeyOperator {
    Map(StreamJobMapTransform),
    Filter(StreamJobFilterPredicate),
    Route(StreamJobRouteTransform),
}

#[derive(Debug, Clone)]
struct StreamJobMapTransform {
    operator_id: String,
    input_field: String,
    output_field: String,
    multiply_by: Option<f64>,
    add: Option<f64>,
}

#[derive(Debug, Clone)]
struct StreamJobRouteBranch {
    predicate: StreamJobFilterPredicate,
    value: String,
}

#[derive(Debug, Clone)]
struct StreamJobRouteTransform {
    operator_id: String,
    output_field: String,
    branches: Vec<StreamJobRouteBranch>,
    default_value: Option<String>,
}

#[derive(Debug, Clone)]
struct StreamJobFilterPredicate {
    operator_id: String,
    field: String,
    comparison: StreamFilterComparison,
    value: StreamFilterValue,
}

#[derive(Debug, Clone)]
enum StreamFilterValue {
    Number(f64),
    String(String),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone)]
pub(crate) struct TopicStreamJobRuntimeInstallPlan {
    pub execution_planned: Option<StreamChangelogRecord>,
    pub runtime_state_update: Option<LocalStreamJobRuntimeState>,
    pub post_apply_projection_records: Vec<StreamProjectionRecord>,
    pub post_apply_changelog_records: Vec<StreamChangelogRecord>,
    pub terminalized: Option<StreamChangelogRecord>,
}

pub(crate) fn topic_poll_requests_from_runtime_state(
    handle: &StreamJobBridgeHandleRecord,
    resolved_job: &CompiledStreamJob,
    runtime_state: &LocalStreamJobRuntimeState,
) -> Result<Vec<TopicStreamJobPollRequest>> {
    if resolved_job.source.kind != STREAM_SOURCE_TOPIC {
        return Ok(Vec::new());
    }
    let Some(plan) = bounded_stream_plan_for_job(resolved_job)? else {
        return Ok(Vec::new());
    };
    ensure_signal_workflows_supported(handle, &plan)?;
    if runtime_state.source_kind.as_deref().is_some_and(|kind| kind != STREAM_SOURCE_TOPIC)
        || runtime_state.terminal_status.is_some()
    {
        return Ok(Vec::new());
    }

    let lease_by_partition = runtime_state
        .source_partition_leases
        .iter()
        .map(|lease| (lease.source_partition_id, lease))
        .collect::<HashMap<_, _>>();
    let mut poll_requests = Vec::new();
    for cursor in &runtime_state.source_cursors {
        let Some(lease) = lease_by_partition.get(&cursor.source_partition_id) else {
            continue;
        };
        let latest_target = cursor.initial_checkpoint_target_offset;
        if cursor.next_offset < latest_target {
            poll_requests.push(TopicStreamJobPollRequest {
                source_partition_id: cursor.source_partition_id,
                source_owner_partition_id: lease.owner_partition_id,
                lease_token: lease.lease_token.clone(),
                checkpoint_sequence: runtime_state.checkpoint_sequence,
                start_offset: cursor.next_offset,
                checkpoint_target_offset: latest_target,
                idle_window_timer: false,
            });
        } else if should_request_idle_window_timer(cursor, &plan, latest_target) {
            poll_requests.push(TopicStreamJobPollRequest {
                source_partition_id: cursor.source_partition_id,
                source_owner_partition_id: lease.owner_partition_id,
                lease_token: lease.lease_token.clone(),
                checkpoint_sequence: runtime_state.checkpoint_sequence,
                start_offset: cursor.next_offset,
                checkpoint_target_offset: latest_target,
                idle_window_timer: true,
            });
        }
    }
    Ok(poll_requests)
}

#[derive(Debug, Clone)]
struct BoundedStreamAggregationPlan {
    key_field: String,
    reducer_kind: String,
    value_field: Option<String>,
    output_field: String,
    pre_key_operators: Vec<StreamJobPreKeyOperator>,
    threshold: Option<f64>,
    threshold_comparison: StreamThresholdComparison,
    view_name: String,
    additional_view_names: Vec<String>,
    eventual_projection_view_names: Vec<String>,
    workflow_signals: Vec<StreamJobWorkflowSignalPlan>,
    view_retention_seconds: Option<u64>,
    window_time_field: Option<String>,
    window_mode: Option<StreamWindowMode>,
    window_size: Option<ChronoDuration>,
    window_hop: Option<ChronoDuration>,
    allowed_lateness: Option<ChronoDuration>,
    checkpoint_name: String,
    checkpoint_sequence: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamWindowMode {
    Tumbling,
    Hopping,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamThresholdComparison {
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamFilterComparison {
    Gt,
    Gte,
    Lt,
    Lte,
    Eq,
    Ne,
}

impl StreamWindowMode {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "tumbling" => Ok(Self::Tumbling),
            "hopping" => Ok(Self::Hopping),
            other => anyhow::bail!(
                "stream window mode must be one of tumbling or hopping, got {}",
                other
            ),
        }
    }
}

impl StreamThresholdComparison {
    fn parse(value: Option<&str>) -> Result<Self> {
        match value.unwrap_or("gte") {
            "gt" => Ok(Self::Gt),
            "gte" => Ok(Self::Gte),
            "lt" => Ok(Self::Lt),
            "lte" => Ok(Self::Lte),
            other => anyhow::bail!(
                "stream runtime {} threshold comparison must be one of gt, gte, lt, lte, got {}",
                STREAM_RUNTIME_AGGREGATE_V2,
                other
            ),
        }
    }

    fn matches(self, value: f64, threshold: f64) -> bool {
        match self {
            Self::Gt => value > threshold,
            Self::Gte => value >= threshold,
            Self::Lt => value < threshold,
            Self::Lte => value <= threshold,
        }
    }
}

impl StreamFilterComparison {
    fn parse(value: &str) -> Result<Self> {
        match value {
            ">" => Ok(Self::Gt),
            ">=" => Ok(Self::Gte),
            "<" => Ok(Self::Lt),
            "<=" => Ok(Self::Lte),
            "==" => Ok(Self::Eq),
            "!=" => Ok(Self::Ne),
            other => anyhow::bail!("unsupported stream filter comparison {other}"),
        }
    }

    fn matches(self, actual: &Value, expected: &StreamFilterValue) -> bool {
        match self {
            Self::Gt => match (actual.as_f64(), expected) {
                (Some(actual), StreamFilterValue::Number(expected)) => actual > *expected,
                _ => false,
            },
            Self::Gte => match (actual.as_f64(), expected) {
                (Some(actual), StreamFilterValue::Number(expected)) => actual >= *expected,
                _ => false,
            },
            Self::Lt => match (actual.as_f64(), expected) {
                (Some(actual), StreamFilterValue::Number(expected)) => actual < *expected,
                _ => false,
            },
            Self::Lte => match (actual.as_f64(), expected) {
                (Some(actual), StreamFilterValue::Number(expected)) => actual <= *expected,
                _ => false,
            },
            Self::Eq => match expected {
                StreamFilterValue::Number(expected) => {
                    actual.as_f64().is_some_and(|actual| actual == *expected)
                }
                StreamFilterValue::String(expected) => {
                    actual.as_str().is_some_and(|actual| actual == expected)
                }
                StreamFilterValue::Bool(expected) => {
                    actual.as_bool().is_some_and(|actual| actual == *expected)
                }
                StreamFilterValue::Null => actual.is_null(),
            },
            Self::Ne => !Self::Eq.matches(actual, expected),
        }
    }
}

fn threshold_value(plan: &BoundedStreamAggregationPlan, raw_value: f64) -> f64 {
    match plan.threshold {
        Some(threshold) if plan.threshold_comparison.matches(raw_value, threshold) => 1.0,
        Some(_) => 0.0,
        None => raw_value,
    }
}

fn initial_pre_key_runtime_stats(
    operators: &[StreamJobPreKeyOperator],
) -> Vec<LocalStreamJobPreKeyRuntimeStatsState> {
    operators
        .iter()
        .map(|operator| match operator {
            StreamJobPreKeyOperator::Map(map) => LocalStreamJobPreKeyRuntimeStatsState {
                operator_id: map.operator_id.clone(),
                kind: "map".to_owned(),
                processed_count: 0,
                dropped_count: 0,
                failure_count: 0,
                route_default_count: 0,
                route_branch_counts: Vec::new(),
                last_failure: None,
            },
            StreamJobPreKeyOperator::Filter(filter) => LocalStreamJobPreKeyRuntimeStatsState {
                operator_id: filter.operator_id.clone(),
                kind: "filter".to_owned(),
                processed_count: 0,
                dropped_count: 0,
                failure_count: 0,
                route_default_count: 0,
                route_branch_counts: Vec::new(),
                last_failure: None,
            },
            StreamJobPreKeyOperator::Route(route) => LocalStreamJobPreKeyRuntimeStatsState {
                operator_id: route.operator_id.clone(),
                kind: "route".to_owned(),
                processed_count: 0,
                dropped_count: 0,
                failure_count: 0,
                route_default_count: 0,
                route_branch_counts: route
                    .branches
                    .iter()
                    .map(|branch| LocalStreamJobRouteBranchRuntimeStatsState {
                        value: branch.value.clone(),
                        matched_count: 0,
                    })
                    .collect(),
                last_failure: None,
            },
        })
        .collect()
}

fn hot_key_runtime_stats_from_counts(
    key_counts: HashMap<String, (String, u64, i32)>,
    occurred_at: DateTime<Utc>,
) -> Vec<LocalStreamJobHotKeyRuntimeStatsState> {
    let mut entries = key_counts
        .into_iter()
        .map(|(display_key, (logical_key, observed_count, source_partition_id))| {
            LocalStreamJobHotKeyRuntimeStatsState {
                display_key,
                logical_key,
                observed_count,
                source_partition_ids: vec![source_partition_id],
                last_seen_at: Some(occurred_at),
            }
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        right
            .observed_count
            .cmp(&left.observed_count)
            .then_with(|| left.display_key.cmp(&right.display_key))
    });
    if entries.len() > STREAM_JOB_HOT_KEY_RUNTIME_STATS_LIMIT {
        entries.truncate(STREAM_JOB_HOT_KEY_RUNTIME_STATS_LIMIT);
    }
    entries
}

fn owner_partition_runtime_stats_from_batches(
    batches: &[StreamJobDataflowBatch],
    occurred_at: DateTime<Utc>,
) -> Vec<LocalStreamJobOwnerPartitionRuntimeStatsState> {
    let mut by_partition = BTreeMap::<i32, LocalStreamJobOwnerPartitionRuntimeStatsState>::new();
    for batch in batches {
        let program = &batch.program;
        let batch_item_count = program.items.len() as u64;
        let stats = by_partition.entry(program.stream_partition_id).or_insert_with(|| {
            LocalStreamJobOwnerPartitionRuntimeStatsState {
                stream_partition_id: program.stream_partition_id,
                observed_batch_count: 0,
                observed_item_count: 0,
                last_batch_item_count: 0,
                max_batch_item_count: 0,
                state_key_count: 0,
                source_partition_ids: Vec::new(),
                last_updated_at: Some(occurred_at),
            }
        });
        stats.observed_batch_count = stats.observed_batch_count.saturating_add(1);
        stats.observed_item_count = stats.observed_item_count.saturating_add(batch_item_count);
        stats.last_batch_item_count = batch_item_count;
        stats.max_batch_item_count = stats.max_batch_item_count.max(batch_item_count);
        if let Some(source_partition_id) = program.checkpoint_partition_id
            && !stats.source_partition_ids.contains(&source_partition_id)
        {
            stats.source_partition_ids.push(source_partition_id);
            stats.source_partition_ids.sort_unstable();
        }
        stats.last_updated_at =
            Some(stats.last_updated_at.map_or(occurred_at, |current| current.max(occurred_at)));
    }
    by_partition.into_values().collect()
}

pub(crate) fn apply_topic_stream_job_poll_runtime_delta(
    runtime_state: &mut LocalStreamJobRuntimeState,
    delta: &TopicStreamJobPollRuntimeDelta,
) {
    runtime_state.checkpoint_sequence =
        runtime_state.checkpoint_sequence.max(delta.checkpoint_sequence);
    if let Some(cursor) = runtime_state
        .source_cursors
        .iter_mut()
        .find(|cursor| cursor.source_partition_id == delta.source_cursor_update.source_partition_id)
    {
        *cursor = delta.source_cursor_update.clone();
    } else {
        runtime_state.source_cursors.push(delta.source_cursor_update.clone());
        runtime_state.source_cursors.sort_by_key(|cursor| cursor.source_partition_id);
    }
    if let Some(lease) = runtime_state
        .source_partition_leases
        .iter_mut()
        .find(|lease| lease.source_partition_id == delta.source_lease_update.source_partition_id)
    {
        *lease = delta.source_lease_update.clone();
    } else {
        runtime_state.source_partition_leases.push(delta.source_lease_update.clone());
        runtime_state.source_partition_leases.sort_by_key(|lease| lease.source_partition_id);
    }
    runtime_state.merge_pre_key_runtime_stats(&delta.pre_key_runtime_stats);
    runtime_state.merge_hot_key_runtime_stats(
        &delta.hot_key_runtime_stats,
        STREAM_JOB_HOT_KEY_RUNTIME_STATS_LIMIT,
    );
    runtime_state.merge_owner_partition_runtime_stats(&delta.owner_partition_runtime_stats);
    runtime_state.updated_at = runtime_state.updated_at.max(delta.updated_at);
}

fn pre_key_runtime_stats_entry_mut<'a>(
    stats: &'a mut [LocalStreamJobPreKeyRuntimeStatsState],
    operator_id: &str,
) -> Result<&'a mut LocalStreamJobPreKeyRuntimeStatsState> {
    stats
        .iter_mut()
        .find(|entry| entry.operator_id == operator_id)
        .with_context(|| format!("missing pre-key runtime stats entry for operator {operator_id}"))
}

fn apply_stream_job_pre_key_operators(
    item: &mut Value,
    operators: &[StreamJobPreKeyOperator],
    stats: &mut [LocalStreamJobPreKeyRuntimeStatsState],
) -> Result<bool> {
    if operators.is_empty() {
        return Ok(true);
    }
    for operator in operators {
        match operator {
            StreamJobPreKeyOperator::Map(map) => {
                let stats_entry = pre_key_runtime_stats_entry_mut(stats, &map.operator_id)?;
                stats_entry.processed_count = stats_entry.processed_count.saturating_add(1);
                let object = item
                    .as_object_mut()
                    .context("stream map operators require object-shaped source items")?;
                let input = object.get(&map.input_field).cloned().with_context(|| {
                    format!("stream map input field {} must exist", map.input_field)
                });
                let input = match input {
                    Ok(input) => input,
                    Err(error) => {
                        stats_entry.failure_count = stats_entry.failure_count.saturating_add(1);
                        stats_entry.last_failure = Some(error.to_string());
                        return Err(error);
                    }
                };
                let output = if map.multiply_by.is_some() || map.add.is_some() {
                    let input = input.as_f64().with_context(|| {
                        format!("stream map input field {} must be numeric", map.input_field)
                    });
                    let input = match input {
                        Ok(input) => input,
                        Err(error) => {
                            stats_entry.failure_count = stats_entry.failure_count.saturating_add(1);
                            stats_entry.last_failure = Some(error.to_string());
                            return Err(error);
                        }
                    };
                    let scaled = (input * map.multiply_by.unwrap_or(1.0)) + map.add.unwrap_or(0.0);
                    Value::from(scaled)
                } else {
                    input
                };
                object.insert(map.output_field.clone(), output);
            }
            StreamJobPreKeyOperator::Filter(filter) => {
                let stats_entry = pre_key_runtime_stats_entry_mut(stats, &filter.operator_id)?;
                stats_entry.processed_count = stats_entry.processed_count.saturating_add(1);
                let matched = item
                    .as_object()
                    .and_then(|object| object.get(&filter.field))
                    .is_some_and(|actual| filter.comparison.matches(actual, &filter.value));
                if !matched {
                    stats_entry.dropped_count = stats_entry.dropped_count.saturating_add(1);
                    return Ok(false);
                }
            }
            StreamJobPreKeyOperator::Route(route) => {
                let stats_entry = pre_key_runtime_stats_entry_mut(stats, &route.operator_id)?;
                stats_entry.processed_count = stats_entry.processed_count.saturating_add(1);
                let object = item
                    .as_object_mut()
                    .context("stream route operators require object-shaped source items")?;
                let routed = route.branches.iter().find_map(|branch| {
                    object
                        .get(&branch.predicate.field)
                        .filter(|actual| {
                            branch.predicate.comparison.matches(actual, &branch.predicate.value)
                        })
                        .map(|_| branch.value.clone())
                });
                if let Some(value) = routed.as_ref() {
                    if let Some(branch_stats) = stats_entry
                        .route_branch_counts
                        .iter_mut()
                        .find(|branch| branch.value == *value)
                    {
                        branch_stats.matched_count = branch_stats.matched_count.saturating_add(1);
                    }
                } else if route.default_value.is_some() {
                    stats_entry.route_default_count =
                        stats_entry.route_default_count.saturating_add(1);
                }
                let value = routed
                    .or_else(|| route.default_value.clone())
                    .map(Value::String)
                    .unwrap_or(Value::Null);
                object.insert(route.output_field.clone(), value);
            }
        }
    }
    Ok(true)
}

fn parse_stream_job_filter_predicate(
    operator_id: &str,
    predicate: &str,
) -> Result<StreamJobFilterPredicate> {
    let mut parts = predicate.split_whitespace();
    let field = parts.next().context("stream filter predicate must include a field name")?;
    let comparison =
        parts.next().context("stream filter predicate must include a comparison operator")?;
    let raw_value =
        parts.next().context("stream filter predicate must include a comparison value")?;
    if parts.next().is_some() {
        anyhow::bail!("stream filter predicate must be of the form '<field> <op> <value>'");
    }
    let value = if (raw_value.starts_with('"') && raw_value.ends_with('"'))
        || (raw_value.starts_with('\'') && raw_value.ends_with('\''))
    {
        StreamFilterValue::String(raw_value[1..raw_value.len() - 1].to_owned())
    } else if raw_value == "true" {
        StreamFilterValue::Bool(true)
    } else if raw_value == "false" {
        StreamFilterValue::Bool(false)
    } else if raw_value == "null" {
        StreamFilterValue::Null
    } else {
        StreamFilterValue::Number(
            raw_value
                .parse::<f64>()
                .with_context(|| format!("stream filter literal {raw_value} must be a number"))?,
        )
    };
    Ok(StreamJobFilterPredicate {
        operator_id: operator_id.to_owned(),
        field: field.to_owned(),
        comparison: StreamFilterComparison::parse(comparison)?,
        value,
    })
}

fn ensure_signal_workflows_supported(
    handle: &StreamJobBridgeHandleRecord,
    plan: &BoundedStreamAggregationPlan,
) -> Result<()> {
    let _ = handle;
    let _ = plan;
    Ok(())
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

#[cfg(test)]
fn debug_topic_activation_trace(handle_id: &str, message: impl AsRef<str>) {
    if std::env::var_os("FABRIK_DEBUG_TOPIC_TEST").is_some() {
        eprintln!("[topic-debug][stream_jobs] handle_id={} {}", handle_id, message.as_ref());
    }
}

#[cfg(not(test))]
fn debug_topic_activation_trace(_handle_id: &str, _message: impl AsRef<str>) {}

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

fn view_supports_eventual_reads(view: &CompiledStreamView) -> bool {
    view.consistency == StreamJobQueryConsistency::Eventual.as_str()
        || view
            .supported_consistencies
            .iter()
            .any(|consistency| consistency == StreamJobQueryConsistency::Eventual.as_str())
}

fn eventual_projection_view_names(
    handle: &StreamJobBridgeHandleRecord,
    view_names: &[String],
) -> Result<Vec<String>> {
    let definitions = if let Some(job) = parse_compiled_job_from_config_ref(handle)? {
        job.views
    } else {
        parse_compiled_views(handle.view_definitions.as_ref())?
    };
    Ok(view_names
        .iter()
        .filter(|view_name| {
            definitions
                .iter()
                .find(|view| view.name == view_name.as_str())
                .is_some_and(view_supports_eventual_reads)
        })
        .cloned()
        .collect())
}

pub(crate) fn eventual_view_names_for_handle(
    handle: &StreamJobBridgeHandleRecord,
) -> Result<HashSet<String>> {
    let definitions = if let Some(job) = parse_compiled_job_from_config_ref(handle)? {
        job.views
    } else {
        parse_compiled_views(handle.view_definitions.as_ref())?
    };
    Ok(definitions.into_iter().filter(view_supports_eventual_reads).map(|view| view.name).collect())
}

pub(crate) async fn resolve_stream_job_definition(
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
                artifact.validate_persistable()?;
                return Ok(Some(artifact.job));
            }
            if let Some(artifact) =
                store.get_latest_stream_artifact(&handle.tenant_id, &handle.definition_id).await?
            {
                artifact.validate_persistable()?;
                return Ok(Some(artifact.job));
            }
        }

        if let Some(artifact) = store
            .get_latest_stream_artifact_for_job_name(&handle.tenant_id, &handle.job_name)
            .await?
        {
            artifact.validate_persistable()?;
            return Ok(Some(artifact.job));
        }
    }

    resolve_stream_job_definition_without_store(handle)
}

fn resolve_stream_job_definition_without_store(
    handle: &StreamJobBridgeHandleRecord,
) -> Result<Option<CompiledStreamJob>> {
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

fn dataflow_plan_id_for_handle(handle: &StreamJobBridgeHandleRecord) -> String {
    format!("stream-job:{}", handle.handle_id)
}

fn dataflow_plan_summary_value_for_job(
    handle: &StreamJobBridgeHandleRecord,
    job: &CompiledStreamJob,
) -> Result<Value> {
    Ok(job.dataflow_plan(dataflow_plan_id_for_handle(handle))?.summary_value())
}

fn dataflow_batch_for_program(
    handle: &StreamJobBridgeHandleRecord,
    planned_batch: PlannedBoundedDataflowBatch,
) -> StreamJobDataflowBatch {
    let record_count = planned_batch
        .program
        .items
        .iter()
        .fold(0u64, |count, item| count.saturating_add(item.count));
    let envelope = DataflowBatchEnvelope {
        plan_id: dataflow_plan_id_for_handle(handle),
        batch_id: planned_batch.batch_id,
        source_id: planned_batch.source_id,
        source_partition_id: planned_batch.source_partition_id,
        source_epoch: planned_batch.source_epoch,
        offset_range: planned_batch.offset_range,
        edge_id: format!("exchange:key_by:{}", planned_batch.program.key_field),
        dest_partition_id: planned_batch.program.stream_partition_id,
        owner_epoch: planned_batch.program.owner_epoch,
        lease_epoch: None,
        summary: DataflowBatchSummary { record_count, payload_checksum: None },
    };
    StreamJobDataflowBatch { envelope, program: planned_batch.program }
}

fn dataflow_batches_for_planned_programs(
    handle: &StreamJobBridgeHandleRecord,
    planned_batches: Vec<PlannedBoundedDataflowBatch>,
) -> Vec<StreamJobDataflowBatch> {
    planned_batches
        .into_iter()
        .map(|planned_batch| dataflow_batch_for_program(handle, planned_batch))
        .collect()
}

fn default_source_id_for_program(
    runtime_state: Option<&LocalStreamJobRuntimeState>,
    program: &crate::dataflow_engine::BoundedDataflowBatchProgram,
) -> String {
    runtime_state
        .and_then(|state| state.source_name.clone())
        .or_else(|| runtime_state.and_then(|state| state.source_kind.clone()))
        .unwrap_or_else(|| {
            if program.source_lease_token.is_some() {
                STREAM_SOURCE_TOPIC.to_owned()
            } else {
                STREAM_SOURCE_BOUNDED_INPUT.to_owned()
            }
        })
}

fn direct_dataflow_batch_for_program(
    handle: &StreamJobBridgeHandleRecord,
    batch_id: String,
    program: crate::dataflow_engine::BoundedDataflowBatchProgram,
    runtime_state: Option<&LocalStreamJobRuntimeState>,
) -> StreamJobDataflowBatch {
    dataflow_batch_for_program(
        handle,
        PlannedBoundedDataflowBatch {
            batch_id,
            source_id: default_source_id_for_program(runtime_state, &program),
            source_partition_id: program
                .checkpoint_partition_id
                .unwrap_or(program.stream_partition_id),
            source_epoch: program.checkpoint_sequence.max(0) as u64,
            offset_range: None,
            program,
        },
    )
}

fn for_each_bounded_input_item<F>(input_ref: &str, mut on_item: F) -> Result<usize>
where
    F: FnMut(Value) -> Result<()>,
{
    struct ItemSeed<'a, F> {
        on_item: &'a mut F,
        count: &'a mut usize,
    }

    impl<'de, F> DeserializeSeed<'de> for ItemSeed<'_, F>
    where
        F: FnMut(Value) -> Result<()>,
    {
        type Value = ();

        fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserializer.deserialize_any(ItemVisitor { on_item: self.on_item, count: self.count })
        }
    }

    struct ItemVisitor<'a, F> {
        on_item: &'a mut F,
        count: &'a mut usize,
    }

    impl<'de, F> Visitor<'de> for ItemVisitor<'_, F>
    where
        F: FnMut(Value) -> Result<()>,
    {
        type Value = ();

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("bounded stream input items")
        }

        fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            while let Some(item) = seq.next_element::<Value>()? {
                (self.on_item)(item).map_err(de::Error::custom)?;
                *self.count += 1;
            }
            Ok(())
        }

        fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut saw_items = false;
            while let Some(key) = map.next_key::<String>()? {
                match key.as_str() {
                    "kind" => {
                        let kind = map.next_value::<Option<String>>()?;
                        if kind.as_deref().is_some_and(|value| value != "bounded_items") {
                            return Err(de::Error::custom(
                                "stream job input kind must be bounded_items",
                            ));
                        }
                    }
                    "items" => {
                        saw_items = true;
                        map.next_value_seed(ItemSeed { on_item: self.on_item, count: self.count })?;
                    }
                    _ => {
                        map.next_value::<IgnoredAny>()?;
                    }
                }
            }
            if !saw_items {
                return Err(de::Error::custom(
                    "stream job bounded input object must contain an items array",
                ));
            }
            Ok(())
        }
    }

    let mut count = 0usize;
    let mut deserializer = serde_json::Deserializer::from_str(input_ref);
    ItemSeed { on_item: &mut on_item, count: &mut count }
        .deserialize(&mut deserializer)
        .context("failed to decode stream job input_ref JSON")?;
    Ok(count)
}

fn parse_topic_source_spec(
    handle: &StreamJobBridgeHandleRecord,
    job: &CompiledStreamJob,
) -> Result<Option<TopicSourceSpec>> {
    if job.source.kind != STREAM_SOURCE_TOPIC {
        return Ok(None);
    }

    let parsed_input = serde_json::from_str::<Value>(&handle.input_ref).ok();
    let envelope = parsed_input
        .as_ref()
        .and_then(|value| serde_json::from_value::<TopicInputEnvelope>(value.clone()).ok());

    let topic_from_job = job
        .source
        .name
        .clone()
        .or_else(|| {
            job.source
                .config
                .as_ref()
                .and_then(|config| value_property(config, "topic", "topic"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .or_else(|| envelope.as_ref().and_then(|value| value.topic.clone()))
        .context("topic stream job source must declare a topic name")?;
    if envelope.as_ref().and_then(|value| value.kind.as_deref()).is_some_and(|kind| kind != "topic")
    {
        anyhow::bail!("stream job input kind must be topic for topic-backed sources");
    }
    let start_from_latest = envelope
        .as_ref()
        .and_then(|value| value.start_offset.as_deref())
        .is_some_and(|offset| offset.eq_ignore_ascii_case("latest"));
    let explicit_offsets = envelope
        .and_then(|value| value.start_offsets)
        .unwrap_or_default()
        .into_iter()
        .map(|entry| (entry.partition_id, entry.offset))
        .collect();

    Ok(Some(TopicSourceSpec { topic_name: topic_from_job, start_from_latest, explicit_offsets }))
}

fn bounded_stream_plan_for_job(
    job: &CompiledStreamJob,
) -> Result<Option<BoundedStreamAggregationPlan>> {
    if let Some(kernel) = job.keyed_rollup_kernel()? {
        let all_view_names = vec![kernel.view_name.clone()];
        return Ok(Some(BoundedStreamAggregationPlan {
            key_field: kernel.key_field.clone(),
            reducer_kind: STREAM_REDUCER_SUM.to_owned(),
            value_field: Some(kernel.value_field.clone()),
            output_field: kernel.output_field.clone(),
            pre_key_operators: Vec::new(),
            threshold: None,
            threshold_comparison: StreamThresholdComparison::Gte,
            view_name: kernel.view_name.clone(),
            additional_view_names: Vec::new(),
            eventual_projection_view_names: all_view_names
                .iter()
                .filter(|view_name| {
                    job.views
                        .iter()
                        .find(|view| view.name == view_name.as_str())
                        .is_some_and(view_supports_eventual_reads)
                })
                .cloned()
                .collect(),
            workflow_signals: kernel
                .workflow_signals
                .iter()
                .map(|signal| StreamJobWorkflowSignalPlan {
                    operator_id: signal.operator_id.clone(),
                    view_name: signal.view_name.clone(),
                    signal_type: signal.signal_type.clone(),
                    when_output_field: signal.when_output_field.clone(),
                })
                .collect(),
            view_retention_seconds: None,
            window_time_field: None,
            window_mode: None,
            window_size: None,
            window_hop: None,
            allowed_lateness: None,
            checkpoint_name: kernel.checkpoint_name.clone(),
            checkpoint_sequence: kernel.checkpoint_sequence,
        }));
    }
    if let Some(kernel) = job.aggregate_v2_kernel()? {
        return Ok(Some(bounded_aggregate_v2_kernel_subset(job, &kernel)?));
    }
    Ok(None)
}

fn string_field_ref<'a>(item: &'a Value, field: &str) -> Result<&'a str> {
    item.get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("stream job item field {field} must be a string"))
}

fn numeric_field(item: &Value, field: &str) -> Result<f64> {
    item.get(field)
        .and_then(Value::as_f64)
        .with_context(|| format!("stream job item field {field} must be a number"))
}

fn parse_window_size(size: &str) -> Result<ChronoDuration> {
    let (value, unit) = size.split_at(size.len().saturating_sub(1));
    let amount = value
        .parse::<i64>()
        .with_context(|| format!("stream window size {size} must start with an integer"))?;
    if amount <= 0 {
        anyhow::bail!("stream window size {size} must be positive");
    }
    match unit {
        "s" => Ok(ChronoDuration::seconds(amount)),
        "m" => Ok(ChronoDuration::minutes(amount)),
        "h" => Ok(ChronoDuration::hours(amount)),
        "d" => Ok(ChronoDuration::days(amount)),
        _ => anyhow::bail!("stream window size {size} must end with s, m, h, or d"),
    }
}

fn default_window_time_field(item: &Value) -> Option<&'static str> {
    ["timestamp", "eventTime", "event_time", "occurredAt", "occurred_at"]
        .into_iter()
        .find(|field| item.get(*field).is_some())
}

fn event_time_field(item: &Value, field: Option<&str>) -> Result<DateTime<Utc>> {
    let resolved_field = field
        .map(str::to_owned)
        .or_else(|| default_window_time_field(item).map(str::to_owned))
        .context("windowed stream job items require a time field or a timestamp/eventTime field")?;
    let value = item
        .get(&resolved_field)
        .with_context(|| format!("stream job item field {resolved_field} must exist"))?;
    if let Some(text) = value.as_str() {
        return chrono::DateTime::parse_from_rfc3339(text)
            .map(|value| value.with_timezone(&Utc))
            .with_context(|| {
                format!("stream job item field {resolved_field} must be RFC3339 when string")
            });
    }
    if let Some(millis) = value.as_i64() {
        let (seconds, nanos) = if millis.abs() >= 1_000_000_000_000 {
            (millis.div_euclid(1_000), (millis.rem_euclid(1_000) as u32) * 1_000_000)
        } else {
            (millis, 0)
        };
        return Utc.timestamp_opt(seconds, nanos).single().with_context(|| {
            format!("stream job item field {resolved_field} must be a valid unix timestamp")
        });
    }
    anyhow::bail!("stream job item field {resolved_field} must be a string or integer timestamp")
}

fn tumbling_window_bounds(
    event_time: DateTime<Utc>,
    window_size: ChronoDuration,
) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    let window_seconds = window_size.num_seconds();
    if window_seconds <= 0 {
        anyhow::bail!("stream window size must be positive");
    }
    let event_seconds = event_time.timestamp();
    let start_seconds = event_seconds.div_euclid(window_seconds) * window_seconds;
    let start = Utc
        .timestamp_opt(start_seconds, 0)
        .single()
        .context("stream window start must be representable")?;
    Ok((start, start + window_size))
}

fn hopping_window_bounds(
    event_time: DateTime<Utc>,
    window_size: ChronoDuration,
    hop_size: ChronoDuration,
) -> Result<Vec<(DateTime<Utc>, DateTime<Utc>)>> {
    let window_seconds = window_size.num_seconds();
    let hop_seconds = hop_size.num_seconds();
    if window_seconds <= 0 || hop_seconds <= 0 {
        anyhow::bail!("stream hopping window size and hop must be positive");
    }
    if hop_seconds > window_seconds {
        anyhow::bail!("stream hopping window hop must be less than or equal to the window size");
    }
    if window_seconds % hop_seconds != 0 {
        anyhow::bail!("stream hopping window size must be an exact multiple of the hop");
    }
    let event_seconds = event_time.timestamp();
    let base_start_seconds = event_seconds.div_euclid(hop_seconds) * hop_seconds;
    let base_start = Utc
        .timestamp_opt(base_start_seconds, 0)
        .single()
        .context("stream hopping window start must be representable")?;
    let window_count = (window_seconds / hop_seconds) as usize;
    let mut bounds = Vec::with_capacity(window_count);
    for step in (0..window_count).rev() {
        let start = base_start - ChronoDuration::seconds((step as i64) * hop_seconds);
        let end = start + window_size;
        if event_time >= start && event_time < end {
            bounds.push((start, end));
        }
    }
    Ok(bounds)
}

pub(crate) fn windowed_logical_key(logical_key: &str, window_start: &str) -> String {
    format!("{logical_key}{STREAM_WINDOW_LOGICAL_KEY_SEPARATOR}{window_start}")
}

#[derive(Debug, Clone)]
struct StreamWindowAssignment {
    logical_key: String,
    window_start: Option<String>,
    window_end: Option<String>,
    event_time: Option<DateTime<Utc>>,
    window_end_at: Option<DateTime<Utc>>,
}

fn event_window_bounds(
    event_time: DateTime<Utc>,
    plan: &BoundedStreamAggregationPlan,
) -> Result<Vec<(DateTime<Utc>, DateTime<Utc>)>> {
    let Some(window_size) = plan.window_size else {
        return Ok(Vec::new());
    };
    match plan.window_mode.unwrap_or(StreamWindowMode::Tumbling) {
        StreamWindowMode::Tumbling => Ok(vec![tumbling_window_bounds(event_time, window_size)?]),
        StreamWindowMode::Hopping => Ok(hopping_window_bounds(
            event_time,
            window_size,
            plan.window_hop.context("stream hopping window hop must exist")?,
        )?),
    }
}

fn owner_input_batches_from_source_batch(
    handle: &StreamJobBridgeHandleRecord,
    source_batch: &TopicSourceBatch,
    plan: &BoundedStreamAggregationPlan,
    throughput_partitions: i32,
    source_owner_epoch: u64,
    occurred_at: DateTime<Utc>,
) -> Result<Vec<StreamJobDataflowBatch>> {
    let mut planned_batches = Vec::new();
    let mut by_owner_partition =
        BTreeMap::<i32, BTreeMap<String, crate::dataflow_engine::BoundedDataflowItem>>::new();
    for record in &source_batch.records {
        let owner_partition_id =
            throughput_partition_for_stream_key(&record.display_key, throughput_partitions);
        let partition_items = by_owner_partition.entry(owner_partition_id).or_default();
        if let Some(aggregate) = partition_items.get_mut(&record.logical_key) {
            merge_dataflow_item(&plan.reducer_kind, aggregate, record.value)?;
        } else {
            partition_items.insert(
                record.logical_key.clone(),
                crate::dataflow_engine::BoundedDataflowItem {
                    logical_key: record.logical_key.clone(),
                    value: record.value,
                    display_key: Some(record.display_key.clone()),
                    window_start: record.window_start.clone(),
                    window_end: record.window_end.clone(),
                    count: 1,
                },
            );
        }
    }

    let mut owner_input_batches = Vec::new();
    for (owner_partition_id, items_by_key) in by_owner_partition {
        let items = items_by_key.into_values().collect::<Vec<_>>();
        let routing_key = items
            .first()
            .map(|item| item.logical_key.clone())
            .unwrap_or_else(|| source_batch.job_id.clone());
        planned_batches.push(PlannedBoundedDataflowBatch {
            batch_id: format!(
                "{}:topic:{}:{}:{}:{}",
                source_batch.handle_id,
                source_batch.source_partition_id,
                source_batch.offset_start,
                source_batch.offset_end,
                owner_partition_id,
            ),
            source_id: format!("topic:{}", source_batch.handle_id),
            source_partition_id: source_batch.source_partition_id,
            source_epoch: source_batch.checkpoint_sequence.max(0) as u64,
            offset_range: Some(DataflowOffsetRange {
                start: source_batch.offset_start,
                end: source_batch.offset_end,
            }),
            program: crate::dataflow_engine::BoundedDataflowBatchProgram {
                stream_partition_id: owner_partition_id,
                checkpoint_partition_id: Some(source_batch.source_partition_id),
                source_owner_partition_id: Some(source_batch.source_owner_partition_id),
                source_lease_token: Some(source_batch.source_lease_token.clone()),
                routing_key,
                key_field: plan.key_field.clone(),
                reducer_kind: plan.reducer_kind.clone(),
                output_field: plan.output_field.clone(),
                view_name: plan.view_name.clone(),
                additional_view_names: plan.additional_view_names.clone(),
                eventual_projection_view_names: plan.eventual_projection_view_names.clone(),
                workflow_signals: plan.workflow_signals.clone(),
                checkpoint_name: plan.checkpoint_name.clone(),
                checkpoint_sequence: source_batch.checkpoint_sequence,
                owner_epoch: source_owner_epoch,
                occurred_at,
                items,
                is_final_partition_batch: false,
                completes_dispatch: false,
            },
        });
    }
    owner_input_batches.extend(dataflow_batches_for_planned_programs(handle, planned_batches));
    Ok(owner_input_batches)
}

fn windowed_item_assignments(
    item: &Value,
    base_key: &str,
    plan: &BoundedStreamAggregationPlan,
) -> Result<Vec<StreamWindowAssignment>> {
    let Some(_) = plan.window_size else {
        return Ok(vec![StreamWindowAssignment {
            logical_key: base_key.to_owned(),
            window_start: None,
            window_end: None,
            event_time: None,
            window_end_at: None,
        }]);
    };
    let event_time = event_time_field(item, plan.window_time_field.as_deref())?;
    Ok(event_window_bounds(event_time, plan)?
        .into_iter()
        .map(|(window_start, window_end)| StreamWindowAssignment {
            logical_key: windowed_logical_key(base_key, &window_start.to_rfc3339()),
            window_start: Some(window_start.to_rfc3339()),
            window_end: Some(window_end.to_rfc3339()),
            event_time: Some(event_time),
            window_end_at: Some(window_end),
        })
        .collect())
}

fn latest_closed_tumbling_window_end(
    watermark: DateTime<Utc>,
    window_size: ChronoDuration,
) -> Result<DateTime<Utc>> {
    let (window_start, _) = tumbling_window_bounds(watermark, window_size)?;
    Ok(window_start)
}

fn latest_closed_hopping_window_end(
    watermark: DateTime<Utc>,
    hop_size: ChronoDuration,
) -> Result<DateTime<Utc>> {
    let hop_seconds = hop_size.num_seconds();
    if hop_seconds <= 0 {
        anyhow::bail!("stream hopping window hop must be positive");
    }
    let watermark_seconds = watermark.timestamp();
    let end_seconds = watermark_seconds.div_euclid(hop_seconds) * hop_seconds;
    Utc.timestamp_opt(end_seconds, 0)
        .single()
        .context("stream hopping window end must be representable")
}

fn adjusted_window_watermark(
    watermark: DateTime<Utc>,
    allowed_lateness: Option<ChronoDuration>,
) -> DateTime<Utc> {
    allowed_lateness.map_or(watermark, |lateness| watermark - lateness)
}

fn should_request_idle_window_timer(
    cursor: &LocalStreamJobSourceCursorState,
    plan: &BoundedStreamAggregationPlan,
    checkpoint_target_offset: i64,
) -> bool {
    plan.window_size.is_some()
        && cursor.next_offset >= checkpoint_target_offset
        && cursor.checkpoint_reached_at.is_none()
        && !cursor.pending_window_ends.is_empty()
}

fn advance_topic_window_cursor(
    cursor: &mut LocalStreamJobSourceCursorState,
    plan: &BoundedStreamAggregationPlan,
    idle_timer_watermark: Option<DateTime<Utc>>,
) -> Result<(bool, bool)> {
    let Some(window_size) = plan.window_size else {
        return Ok((false, false));
    };
    let previous_watermark = cursor.last_event_time_watermark;
    if let Some(watermark) = idle_timer_watermark {
        cursor.last_event_time_watermark = Some(
            cursor.last_event_time_watermark.map_or(watermark, |current| current.max(watermark)),
        );
    }
    let watermark_advanced = cursor.last_event_time_watermark != previous_watermark;
    let Some(watermark) = cursor.last_event_time_watermark else {
        return Ok((watermark_advanced, false));
    };
    let adjusted_watermark = adjusted_window_watermark(watermark, plan.allowed_lateness);
    let closed_frontier = match plan.window_mode.unwrap_or(StreamWindowMode::Tumbling) {
        StreamWindowMode::Tumbling => {
            latest_closed_tumbling_window_end(adjusted_watermark, window_size)?
        }
        StreamWindowMode::Hopping => latest_closed_hopping_window_end(
            adjusted_watermark,
            plan.window_hop.context("stream hopping window hop must exist")?,
        )?,
    };
    let previous_closed_window_end = cursor.last_closed_window_end;
    let mut latest_closed_window_end = previous_closed_window_end;
    cursor.pending_window_ends.retain(|window_end| {
        if *window_end <= closed_frontier {
            latest_closed_window_end = Some(
                latest_closed_window_end.map_or(*window_end, |current| current.max(*window_end)),
            );
            false
        } else {
            true
        }
    });
    cursor.last_closed_window_end = latest_closed_window_end;
    Ok((watermark_advanced, latest_closed_window_end != previous_closed_window_end))
}

#[derive(Debug, Clone)]
struct WindowAdmissionResult {
    accepted_records: Vec<TopicSourceBatchRecord>,
    last_event_time_watermark: Option<DateTime<Utc>>,
    last_closed_window_end: Option<DateTime<Utc>>,
    pending_window_ends: Vec<DateTime<Utc>>,
    dropped_late_event_count: u64,
    last_dropped_late_offset: Option<i64>,
    last_dropped_late_event_at: Option<DateTime<Utc>>,
    last_dropped_late_window_end: Option<DateTime<Utc>>,
    dropped_evicted_window_event_count: u64,
    last_dropped_evicted_window_offset: Option<i64>,
    last_dropped_evicted_window_event_at: Option<DateTime<Utc>>,
    last_dropped_evicted_window_end: Option<DateTime<Utc>>,
}

fn admit_windowed_source_records(
    records: &[TopicSourceBatchRecord],
    cursor: &LocalStreamJobSourceCursorState,
    plan: &BoundedStreamAggregationPlan,
    occurred_at: DateTime<Utc>,
) -> Result<WindowAdmissionResult> {
    if plan.window_size.is_none() {
        return Ok(WindowAdmissionResult {
            accepted_records: records.to_vec(),
            last_event_time_watermark: cursor.last_event_time_watermark,
            last_closed_window_end: cursor.last_closed_window_end,
            pending_window_ends: cursor.pending_window_ends.clone(),
            dropped_late_event_count: cursor.dropped_late_event_count,
            last_dropped_late_offset: cursor.last_dropped_late_offset,
            last_dropped_late_event_at: cursor.last_dropped_late_event_at,
            last_dropped_late_window_end: cursor.last_dropped_late_window_end,
            dropped_evicted_window_event_count: cursor.dropped_evicted_window_event_count,
            last_dropped_evicted_window_offset: cursor.last_dropped_evicted_window_offset,
            last_dropped_evicted_window_event_at: cursor.last_dropped_evicted_window_event_at,
            last_dropped_evicted_window_end: cursor.last_dropped_evicted_window_end,
        });
    }

    let mut candidate = cursor.clone();
    let mut accepted_records = Vec::with_capacity(records.len());
    for record in records {
        if let Some(window_end_at) = record.window_end_at
            && candidate.last_closed_window_end.is_some_and(|closed| window_end_at <= closed)
        {
            if window_expired_from_deadline(
                window_expired_at(
                    &json!({ "windowEnd": window_end_at.to_rfc3339() }),
                    plan.view_retention_seconds,
                    plan.allowed_lateness,
                ),
                occurred_at,
            ) {
                candidate.dropped_evicted_window_event_count =
                    candidate.dropped_evicted_window_event_count.saturating_add(1);
                candidate.last_dropped_evicted_window_offset = Some(record.offset);
                candidate.last_dropped_evicted_window_event_at = record.event_time;
                candidate.last_dropped_evicted_window_end = record.window_end_at;
            } else {
                candidate.dropped_late_event_count =
                    candidate.dropped_late_event_count.saturating_add(1);
                candidate.last_dropped_late_offset = Some(record.offset);
                candidate.last_dropped_late_event_at = record.event_time;
                candidate.last_dropped_late_window_end = record.window_end_at;
            }
            continue;
        }

        accepted_records.push(record.clone());

        if let Some(event_time) = record.event_time {
            candidate.last_event_time_watermark = Some(
                candidate
                    .last_event_time_watermark
                    .map_or(event_time, |current| current.max(event_time)),
            );
        }
        if let Some(window_end_at) = record.window_end_at
            && candidate.last_closed_window_end.is_none_or(|closed| window_end_at > closed)
        {
            candidate.pending_window_ends.push(window_end_at);
            candidate.pending_window_ends.sort_unstable();
            candidate.pending_window_ends.dedup();
        }
        let _ = advance_topic_window_cursor(&mut candidate, plan, None)?;
    }

    Ok(WindowAdmissionResult {
        accepted_records,
        last_event_time_watermark: candidate.last_event_time_watermark,
        last_closed_window_end: candidate.last_closed_window_end,
        pending_window_ends: candidate.pending_window_ends,
        dropped_late_event_count: candidate.dropped_late_event_count,
        last_dropped_late_offset: candidate.last_dropped_late_offset,
        last_dropped_late_event_at: candidate.last_dropped_late_event_at,
        last_dropped_late_window_end: candidate.last_dropped_late_window_end,
        dropped_evicted_window_event_count: candidate.dropped_evicted_window_event_count,
        last_dropped_evicted_window_offset: candidate.last_dropped_evicted_window_offset,
        last_dropped_evicted_window_event_at: candidate.last_dropped_evicted_window_event_at,
        last_dropped_evicted_window_end: candidate.last_dropped_evicted_window_end,
    })
}

fn query_window_start(query: &StreamJobQueryRecord) -> Option<String> {
    query
        .query_args
        .as_ref()
        .and_then(|args| value_property(args, "windowStart", "window_start"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn query_scan_prefix(query: &StreamJobQueryRecord) -> Option<String> {
    query
        .query_args
        .as_ref()
        .and_then(|args| {
            value_property(args, "prefix", "prefix")
                .or_else(|| value_property(args, "keyPrefix", "key_prefix"))
                .or_else(|| value_property(args, "key", "key"))
        })
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn query_page_value(query: &StreamJobQueryRecord, camel: &str, snake: &str) -> Option<usize> {
    query
        .query_args
        .as_ref()
        .and_then(|args| value_property(args, camel, snake))
        .and_then(Value::as_u64)
        .map(|value| value as usize)
}

fn query_page_offset(query: &StreamJobQueryRecord) -> usize {
    query_page_value(query, "offset", "offset").unwrap_or(0)
}

fn query_page_limit(query: &StreamJobQueryRecord) -> usize {
    query_page_value(query, "limit", "limit").unwrap_or(100).clamp(1, 1000)
}

fn is_prefix_scan_query(handle: &StreamJobBridgeHandleRecord, query_name: &str) -> Result<bool> {
    if let Some(job) = parse_compiled_job_from_config_ref(handle)? {
        job.validate_supported_contract()?;
        if let Some(query) = job.queries.iter().find(|query| query.name == query_name) {
            return Ok(query
                .arg_fields
                .iter()
                .any(|field| matches!(field.as_str(), "prefix" | "keyPrefix" | "key_prefix")));
        }
    }
    Ok(false)
}

fn window_expired_at(
    output: &Value,
    retention_seconds: Option<u64>,
    allowed_lateness: Option<ChronoDuration>,
) -> Option<DateTime<Utc>> {
    let window_end = output
        .get("windowEnd")
        .and_then(Value::as_str)
        .and_then(|text| chrono::DateTime::parse_from_rfc3339(text).ok())
        .map(|value| value.with_timezone(&Utc))?;
    let retention = ChronoDuration::seconds(i64::try_from(retention_seconds?).ok()?);
    Some(window_end + allowed_lateness.unwrap_or_else(ChronoDuration::zero) + retention)
}

fn window_expired_from_deadline(expired_at: Option<DateTime<Utc>>, at: DateTime<Utc>) -> bool {
    expired_at.is_some_and(|expired_at| expired_at <= at)
}

fn aggregate_v2_allowed_lateness(
    job: Option<&CompiledStreamJob>,
) -> Result<Option<ChronoDuration>> {
    job.and_then(|job| job.aggregate_v2_kernel().ok().flatten())
        .and_then(|kernel| kernel.window.and_then(|window| window.allowed_lateness))
        .as_deref()
        .map(parse_window_size)
        .transpose()
}

fn view_window_expired_at(
    job: Option<&CompiledStreamJob>,
    view: &CompiledStreamView,
    output: &Value,
) -> Result<Option<DateTime<Utc>>> {
    Ok(window_expired_at(output, view.retention_seconds, aggregate_v2_allowed_lateness(job)?))
}

fn window_query_annotations(
    job: Option<&CompiledStreamJob>,
    view: &CompiledStreamView,
    output: &mut Map<String, Value>,
) -> Result<()> {
    if let Some(expired_at) = view_window_expired_at(job, view, &Value::Object(output.clone()))? {
        output.insert("windowExpiredAt".to_owned(), Value::String(expired_at.to_rfc3339()));
    }
    if let Some(retention_seconds) = view.retention_seconds {
        output.insert("windowRetentionSeconds".to_owned(), Value::from(retention_seconds));
    }
    Ok(())
}

#[derive(Debug)]
struct StreamJobWindowRetentionSweep {
    delete_keys: Vec<(String, String, String)>,
    projection_records: Vec<StreamProjectionRecord>,
    changelog_records: Vec<StreamChangelogRecord>,
    evicted_views: Vec<(String, Option<DateTime<Utc>>)>,
    evicted_window_count: u64,
    last_evicted_window_end: Option<DateTime<Utc>>,
    last_evicted_at: Option<DateTime<Utc>>,
}

fn sweep_expired_stream_job_windows(
    local_state: &LocalThroughputState,
    handle: &StreamJobBridgeHandleRecord,
    runtime_state: &LocalStreamJobRuntimeState,
    occurred_at: DateTime<Utc>,
) -> Result<StreamJobWindowRetentionSweep> {
    let Some(job) = parse_compiled_job_from_config_ref(handle)? else {
        return Ok(StreamJobWindowRetentionSweep {
            delete_keys: Vec::new(),
            projection_records: Vec::new(),
            changelog_records: Vec::new(),
            evicted_views: Vec::new(),
            evicted_window_count: 0,
            last_evicted_window_end: None,
            last_evicted_at: None,
        });
    };

    let candidates = prunable_window_views(local_state, handle, &job, runtime_state, occurred_at)?;
    let mut unique_windows = std::collections::BTreeSet::new();
    let mut last_evicted_window_end = None;
    let mut delete_keys = Vec::with_capacity(candidates.len());
    for (view_state, window_end, _) in &candidates {
        if let Some(window_end) = *window_end {
            last_evicted_window_end = Some(
                last_evicted_window_end
                    .map_or(window_end, |current: DateTime<Utc>| current.max(window_end)),
            );
        }
        unique_windows.insert(view_state.logical_key.clone());
        delete_keys.push((
            handle.handle_id.clone(),
            view_state.view_name.clone(),
            view_state.logical_key.clone(),
        ));
    }

    if candidates.is_empty() {
        return Ok(StreamJobWindowRetentionSweep {
            delete_keys: Vec::new(),
            projection_records: Vec::new(),
            changelog_records: Vec::new(),
            evicted_views: Vec::new(),
            evicted_window_count: 0,
            last_evicted_window_end: None,
            last_evicted_at: None,
        });
    }

    let evicted_window_count = runtime_state
        .evicted_window_count
        .saturating_add(u64::try_from(unique_windows.len()).unwrap_or(u64::MAX));
    let last_evicted_window_end =
        runtime_state.last_evicted_window_end.map_or(last_evicted_window_end, |current| {
            Some(last_evicted_window_end.map_or(current, |swept| current.max(swept)))
        });
    let last_evicted_at = Some(occurred_at);

    let mut projection_records = Vec::with_capacity(candidates.len());
    let mut changelog_records = Vec::with_capacity(candidates.len());
    let mut evicted_views = Vec::with_capacity(candidates.len());
    for (view_state, window_end, retention_seconds) in candidates {
        evicted_views.push((view_state.view_name.clone(), window_end));
        projection_records.push(delete_view_projection_record(
            handle,
            &view_state.view_name,
            &view_state.logical_key,
            view_state.checkpoint_sequence,
            occurred_at,
        ));
        changelog_records.push(view_evicted_changelog_record(
            handle,
            &view_state.view_name,
            &view_state.logical_key,
            view_state.checkpoint_sequence,
            window_end,
            retention_seconds,
            evicted_window_count,
            last_evicted_window_end,
            last_evicted_at,
            occurred_at,
        ));
    }

    Ok(StreamJobWindowRetentionSweep {
        delete_keys,
        projection_records,
        changelog_records,
        evicted_views,
        evicted_window_count,
        last_evicted_window_end,
        last_evicted_at,
    })
}

fn apply_window_retention_job_counters_to_runtime_state(
    runtime_state: &mut LocalStreamJobRuntimeState,
    sweep: &StreamJobWindowRetentionSweep,
) {
    runtime_state.evicted_window_count = sweep.evicted_window_count;
    runtime_state.last_evicted_window_end = sweep.last_evicted_window_end;
    runtime_state.last_evicted_at = sweep.last_evicted_at;
}

fn apply_window_retention_view_history_to_runtime_state(
    runtime_state: &mut LocalStreamJobRuntimeState,
    sweep: &StreamJobWindowRetentionSweep,
    occurred_at: DateTime<Utc>,
) {
    for (view_name, window_end) in &sweep.evicted_views {
        runtime_state.record_view_eviction(view_name, *window_end, Some(occurred_at));
    }
}

fn apply_window_retention_sweep_to_runtime_state(
    runtime_state: &mut LocalStreamJobRuntimeState,
    sweep: &StreamJobWindowRetentionSweep,
    occurred_at: DateTime<Utc>,
) {
    apply_window_retention_job_counters_to_runtime_state(runtime_state, sweep);
    apply_window_retention_view_history_to_runtime_state(runtime_state, sweep, occurred_at);
    runtime_state.updated_at = occurred_at;
}

fn apply_stream_job_window_retention_sweep_on_local_state(
    local_state: &LocalThroughputState,
    runtime_state: &LocalStreamJobRuntimeState,
    sweep: &StreamJobWindowRetentionSweep,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    if sweep.delete_keys.is_empty() {
        return Ok(());
    }
    local_state.delete_stream_job_view_states(&sweep.delete_keys)?;
    let mut updated_runtime_state = runtime_state.clone();
    apply_window_retention_sweep_to_runtime_state(&mut updated_runtime_state, sweep, occurred_at);
    local_state.upsert_stream_job_runtime_state(&updated_runtime_state)?;
    Ok(())
}

fn query_source_partition_id(query: &StreamJobQueryRecord) -> Result<Option<i32>> {
    let Some(args) = query.query_args.as_ref() else {
        return Ok(None);
    };
    let Some(value) = value_property(args, "sourcePartitionId", "source_partition_id") else {
        return Ok(None);
    };
    if let Some(partition_id) = value.as_i64() {
        return Ok(Some(i32::try_from(partition_id).with_context(|| {
            format!("stream job query sourcePartitionId {partition_id} is out of range")
        })?));
    }
    if let Some(partition_id) = value.as_u64() {
        return Ok(Some(i32::try_from(partition_id).with_context(|| {
            format!("stream job query sourcePartitionId {partition_id} is out of range")
        })?));
    }
    anyhow::bail!("stream job query sourcePartitionId must be an integer");
}

fn query_view_name(query: &StreamJobQueryRecord) -> Result<Option<String>> {
    let Some(args) = query.query_args.as_ref() else {
        return Ok(None);
    };
    let Some(value) = value_property(args, "viewName", "view_name") else {
        return Ok(None);
    };
    value
        .as_str()
        .map(str::to_owned)
        .with_context(|| "stream job query viewName must be a string".to_owned())
        .map(Some)
}

fn query_bool_arg(query: &StreamJobQueryRecord, field: &str) -> Result<Option<bool>> {
    let Some(args) = query.query_args.as_ref() else {
        return Ok(None);
    };
    let Some(value) = args.get(field) else {
        return Ok(None);
    };
    value.as_bool().map(Some).with_context(|| format!("stream job query {field} must be a boolean"))
}

fn query_i64_arg(query: &StreamJobQueryRecord, field: &str) -> Result<Option<i64>> {
    let Some(args) = query.query_args.as_ref() else {
        return Ok(None);
    };
    let Some(value) = args.get(field) else {
        return Ok(None);
    };
    value.as_i64().map(Some).with_context(|| format!("stream job query {field} must be an integer"))
}

fn query_string_array_arg(
    query: &StreamJobQueryRecord,
    field: &str,
) -> Result<Option<Vec<String>>> {
    let Some(args) = query.query_args.as_ref() else {
        return Ok(None);
    };
    let Some(value) = args.get(field) else {
        return Ok(None);
    };
    let array =
        value.as_array().with_context(|| format!("stream job query {field} must be an array"))?;
    Ok(Some(
        array
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .with_context(|| format!("stream job query {field} entries must be strings"))
            })
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn optional_datetime_value(value: Option<DateTime<Utc>>) -> Value {
    value.map(|value| Value::String(value.to_rfc3339())).unwrap_or(Value::Null)
}

fn optional_repair_value(value: Option<StreamJobBridgeRepairKind>) -> Value {
    value.map(|value| Value::String(value.as_str().to_owned())).unwrap_or(Value::Null)
}

fn repair_list_value(values: Vec<StreamJobBridgeRepairKind>) -> Value {
    Value::Array(values.into_iter().map(|value| Value::String(value.as_str().to_owned())).collect())
}

fn pre_key_runtime_policy(job: &CompiledStreamJob) -> Result<Value> {
    let Some(kernel) = job.aggregate_v2_kernel()? else {
        return Ok(Value::Array(Vec::new()));
    };
    Ok(Value::Array(
        kernel
            .pre_key_operators
            .into_iter()
            .map(|operator| match operator {
                CompiledAggregateV2PreKeyOperator::Map(map) => json!({
                    "kind": "map",
                    "operatorId": map.operator_id,
                    "inputField": map.input_field,
                    "outputField": map.output_field,
                    "multiplyBy": map.multiply_by,
                    "add": map.add,
                }),
                CompiledAggregateV2PreKeyOperator::Filter(filter) => json!({
                    "kind": "filter",
                    "operatorId": filter.operator_id,
                    "predicate": filter.predicate,
                }),
                CompiledAggregateV2PreKeyOperator::Route(route) => json!({
                    "kind": "route",
                    "operatorId": route.operator_id,
                    "outputField": route.output_field,
                    "branches": route.branches.into_iter().map(|branch| {
                        json!({
                            "predicate": branch.predicate,
                            "value": branch.value,
                        })
                    }).collect::<Vec<_>>(),
                    "defaultValue": route.default_value,
                }),
            })
            .collect(),
    ))
}

fn pre_key_runtime_stats_value(runtime_state: &LocalStreamJobRuntimeState) -> Value {
    let operators = runtime_state
        .pre_key_runtime_stats
        .iter()
        .map(|stats| {
            let drop_reasons = if stats.kind == "filter" && stats.dropped_count > 0 {
                vec![json!({
                    "reason": "predicate_not_matched",
                    "count": stats.dropped_count,
                })]
            } else {
                Vec::new()
            };
            let failure_reasons = if stats.failure_count > 0 {
                vec![json!({
                    "reason": "operator_error",
                    "count": stats.failure_count,
                    "lastError": stats.last_failure,
                })]
            } else {
                Vec::new()
            };
            json!({
                "operatorId": stats.operator_id,
                "kind": stats.kind,
                "processedCount": stats.processed_count,
                "droppedCount": stats.dropped_count,
                "failureCount": stats.failure_count,
                "routeDefaultCount": stats.route_default_count,
                "routeBranchCounts": stats.route_branch_counts.iter().map(|branch| {
                    json!({
                        "value": branch.value,
                        "matchedCount": branch.matched_count,
                    })
                }).collect::<Vec<_>>(),
                "dropReasons": drop_reasons,
                "failureReasons": failure_reasons,
            })
        })
        .collect::<Vec<_>>();
    let totals = runtime_state.pre_key_runtime_stats.iter().fold(
        (0u64, 0u64, 0u64),
        |(processed, dropped, failed), stats| {
            (
                processed.saturating_add(stats.processed_count),
                dropped.saturating_add(stats.dropped_count),
                failed.saturating_add(stats.failure_count),
            )
        },
    );
    json!({
        "operators": operators,
        "totals": {
            "processedCount": totals.0,
            "droppedCount": totals.1,
            "failureCount": totals.2,
        }
    })
}

fn hot_key_runtime_stats_value(runtime_state: &LocalStreamJobRuntimeState) -> Value {
    let observed_total = runtime_state
        .hot_key_runtime_stats
        .iter()
        .fold(0u64, |total, stats| total.saturating_add(stats.observed_count));
    json!({
        "topKeys": runtime_state.hot_key_runtime_stats.iter().map(|stats| {
            json!({
                "displayKey": stats.display_key,
                "logicalKey": stats.logical_key,
                "observedCount": stats.observed_count,
                "sourcePartitionIds": stats.source_partition_ids,
                "lastSeenAt": optional_datetime_value(stats.last_seen_at),
            })
        }).collect::<Vec<_>>(),
        "totals": {
            "observedCount": observed_total,
            "trackedKeyCount": runtime_state.hot_key_runtime_stats.len(),
        }
    })
}

fn stream_job_checkpoint_reached_at_by_partition(
    runtime_state: &LocalStreamJobRuntimeState,
) -> BTreeMap<i32, DateTime<Utc>> {
    let mut checkpoints = BTreeMap::new();
    for checkpoint in &runtime_state.checkpoint_partitions {
        if checkpoint.checkpoint_name != runtime_state.checkpoint_name
            || checkpoint.checkpoint_sequence != runtime_state.checkpoint_sequence
        {
            continue;
        }
        checkpoints
            .entry(checkpoint.stream_partition_id)
            .and_modify(|current: &mut DateTime<Utc>| {
                *current = (*current).max(checkpoint.reached_at)
            })
            .or_insert(checkpoint.reached_at);
    }
    checkpoints
}

fn owner_partition_runtime_stats_value(
    local_state: &LocalThroughputState,
    runtime_state: &LocalStreamJobRuntimeState,
    requested_at: DateTime<Utc>,
) -> Result<Value> {
    let debug_snapshot = local_state.debug_snapshot()?;
    let checkpoint_reached_at_by_partition =
        stream_job_checkpoint_reached_at_by_partition(runtime_state);
    let mut pending_batches_by_partition = BTreeMap::<i32, (u64, u64)>::new();
    for batch in runtime_state.dispatch_manifest_batches() {
        let entry = pending_batches_by_partition.entry(batch.stream_partition_id).or_default();
        entry.0 = entry.0.saturating_add(1);
        entry.1 = entry.1.saturating_add(u64::try_from(batch.items.len()).unwrap_or(u64::MAX));
    }
    let mut partitions = runtime_state.owner_partition_runtime_stats.clone();
    for partition_id in &runtime_state.active_partitions {
        if !partitions.iter().any(|stats| stats.stream_partition_id == *partition_id) {
            partitions.push(LocalStreamJobOwnerPartitionRuntimeStatsState {
                stream_partition_id: *partition_id,
                observed_batch_count: 0,
                observed_item_count: 0,
                last_batch_item_count: 0,
                max_batch_item_count: 0,
                state_key_count: 0,
                source_partition_ids: Vec::new(),
                last_updated_at: None,
            });
        }
    }
    partitions.sort_by_key(|stats| stats.stream_partition_id);
    let total_state_key_count =
        partitions.iter().fold(0u64, |total, stats| total.saturating_add(stats.state_key_count));
    let total_observed_batch_count = partitions
        .iter()
        .fold(0u64, |total, stats| total.saturating_add(stats.observed_batch_count));
    let total_observed_item_count = partitions
        .iter()
        .fold(0u64, |total, stats| total.saturating_add(stats.observed_item_count));
    let max_state_key_count =
        partitions.iter().map(|stats| stats.state_key_count).max().unwrap_or(0);
    let max_observed_item_count =
        partitions.iter().map(|stats| stats.observed_item_count).max().unwrap_or(0);
    let avg_state_key_count = if partitions.is_empty() {
        None
    } else {
        Some(total_state_key_count as f64 / partitions.len() as f64)
    };
    let avg_observed_item_count = if partitions.is_empty() {
        None
    } else {
        Some(total_observed_item_count as f64 / partitions.len() as f64)
    };
    let total_pending_batch_count = pending_batches_by_partition
        .values()
        .fold(0u64, |total, (count, _)| total.saturating_add(*count));
    let total_pending_item_count = pending_batches_by_partition
        .values()
        .fold(0u64, |total, (_, items)| total.saturating_add(*items));
    let shard_checkpoint_age_seconds =
        optional_duration_seconds(debug_snapshot.last_checkpoint_at.map(|at| requested_at - at));
    let mut checkpoint_bytes_by_partition = BTreeMap::<i32, u64>::new();
    if runtime_state.throughput_partition_count > 0 {
        for stats in &partitions {
            let checkpoint_value = local_state.snapshot_partition_checkpoint_value(
                stats.stream_partition_id,
                runtime_state.throughput_partition_count,
            )?;
            let checkpoint_bytes = serde_json::to_vec(&checkpoint_value)?.len() as u64;
            checkpoint_bytes_by_partition.insert(stats.stream_partition_id, checkpoint_bytes);
        }
    }
    let total_checkpoint_bytes = checkpoint_bytes_by_partition
        .values()
        .fold(0u64, |total, bytes| total.saturating_add(*bytes));
    let max_checkpoint_bytes =
        checkpoint_bytes_by_partition.values().copied().max().unwrap_or_default();
    let total_restore_tail_lag_entries = if debug_snapshot.restored_from_checkpoint {
        partitions.iter().fold(0u64, |total, stats| {
            total.saturating_add(
                debug_snapshot
                    .streams_partition_lag
                    .get(&stats.stream_partition_id)
                    .copied()
                    .unwrap_or_default() as u64,
            )
        })
    } else {
        0
    };
    let max_restore_tail_lag_entries = if debug_snapshot.restored_from_checkpoint {
        partitions
            .iter()
            .map(|stats| {
                debug_snapshot
                    .streams_partition_lag
                    .get(&stats.stream_partition_id)
                    .copied()
                    .unwrap_or_default() as u64
            })
            .max()
            .unwrap_or_default()
    } else {
        0
    };
    Ok(json!({
        "partitions": partitions.iter().map(|stats| {
            let (pending_batch_count, pending_item_count) =
                pending_batches_by_partition.get(&stats.stream_partition_id).copied().unwrap_or((0, 0));
            let last_stream_checkpoint_at =
                checkpoint_reached_at_by_partition.get(&stats.stream_partition_id).copied();
            json!({
                "streamPartitionId": stats.stream_partition_id,
                "stateKeyCount": stats.state_key_count,
                "observedBatchCount": stats.observed_batch_count,
                "observedItemCount": stats.observed_item_count,
                "pendingBatchCount": pending_batch_count,
                "pendingItemCount": pending_item_count,
                "lastBatchItemCount": stats.last_batch_item_count,
                "maxBatchItemCount": stats.max_batch_item_count,
                "avgBatchItemCount": if stats.observed_batch_count > 0 {
                    Value::from(stats.observed_item_count as f64 / stats.observed_batch_count as f64)
                } else {
                    Value::Null
                },
                "sourcePartitionIds": stats.source_partition_ids,
                "lastUpdatedAt": optional_datetime_value(stats.last_updated_at),
                "lastStreamCheckpointAt": optional_datetime_value(last_stream_checkpoint_at),
                "streamCheckpointAgeSeconds": optional_duration_seconds(
                    last_stream_checkpoint_at.map(|at| requested_at - at)
                ),
                "shardLastCheckpointAt": optional_datetime_value(debug_snapshot.last_checkpoint_at),
                "shardCheckpointAgeSeconds": shard_checkpoint_age_seconds.clone(),
                "checkpointBytes": checkpoint_bytes_by_partition
                    .get(&stats.stream_partition_id)
                    .copied()
                    .map(Value::from)
                    .unwrap_or(Value::Null),
                "restoreTailLagEntries": if debug_snapshot.restored_from_checkpoint {
                    debug_snapshot
                        .streams_partition_lag
                        .get(&stats.stream_partition_id)
                        .copied()
                        .map(Value::from)
                        .unwrap_or(Value::Null)
                } else {
                    Value::Null
                },
            })
        }).collect::<Vec<_>>(),
        "summary": {
            "partitionCount": partitions.len(),
            "totalStateKeyCount": total_state_key_count,
            "maxStateKeyCount": max_state_key_count,
            "totalObservedBatchCount": total_observed_batch_count,
            "totalObservedItemCount": total_observed_item_count,
            "totalPendingBatchCount": total_pending_batch_count,
            "totalPendingItemCount": total_pending_item_count,
            "checkpointedPartitionCount": checkpoint_reached_at_by_partition.len(),
            "lastStreamCheckpointAt": optional_datetime_value(
                checkpoint_reached_at_by_partition.values().copied().max()
            ),
            "lastShardCheckpointAt": optional_datetime_value(debug_snapshot.last_checkpoint_at),
            "shardCheckpointAgeSeconds": shard_checkpoint_age_seconds,
            "restoredFromCheckpoint": debug_snapshot.restored_from_checkpoint,
            "totalCheckpointBytes": total_checkpoint_bytes,
            "maxCheckpointBytes": max_checkpoint_bytes,
            "totalRestoreTailLagEntries": if debug_snapshot.restored_from_checkpoint {
                Value::from(total_restore_tail_lag_entries)
            } else {
                Value::Null
            },
            "maxRestoreTailLagEntries": if debug_snapshot.restored_from_checkpoint {
                Value::from(max_restore_tail_lag_entries)
            } else {
                Value::Null
            },
            "stateKeySkewRatio": avg_state_key_count
                .filter(|average| *average > 0.0)
                .map(|average| Value::from(max_state_key_count as f64 / average))
                .unwrap_or(Value::Null),
            "observedItemSkewRatio": avg_observed_item_count
                .filter(|average| *average > 0.0)
                .map(|average| Value::from(max_observed_item_count as f64 / average))
                .unwrap_or(Value::Null),
        }
    }))
}

fn cursor_offset_lag(cursor: &LocalStreamJobSourceCursorState) -> i64 {
    cursor
        .last_high_watermark
        .map(|watermark| watermark.saturating_sub(cursor.next_offset).max(0))
        .unwrap_or(0)
}

fn cursor_checkpoint_lag(cursor: &LocalStreamJobSourceCursorState) -> i64 {
    cursor.initial_checkpoint_target_offset.saturating_sub(cursor.next_offset).max(0)
}

fn build_materialized_view_runtime_policy(
    job: &CompiledStreamJob,
    view_name: &str,
) -> Result<Option<Value>> {
    let aggregate_v2 = job.aggregate_v2_kernel()?;
    let aggregate_views = job
        .operators
        .iter()
        .filter(|operator| operator.kind == fabrik_throughput::STREAM_OPERATOR_MATERIALIZE)
        .filter_map(|operator| {
            let config = operator.config.as_ref()?.as_object()?;
            let view_name = config.get("view").and_then(Value::as_str)?;
            Some((view_name, operator))
        })
        .collect::<std::collections::HashMap<_, _>>();
    let window = aggregate_v2.as_ref().and_then(|kernel| kernel.window.clone());
    let Some(view) = job.views.iter().find(|view| view.name == view_name) else {
        return Ok(None);
    };
    let aggregate_view = aggregate_views.get(view.name.as_str()).copied();
    Ok(Some(json!({
        "name": view.name,
        "operatorId": aggregate_view.and_then(|compiled| compiled.operator_id.clone()),
        "stateIds": aggregate_view.map(|compiled| compiled.state_ids.clone()).unwrap_or_default(),
        "queryMode": view.query_mode,
        "keyField": view.key_field,
        "consistency": view.consistency,
        "supportedConsistencies": view.supported_consistencies,
        "retentionSeconds": view.retention_seconds,
        "preKeyPolicy": pre_key_runtime_policy(job)?,
        "lateEventPolicy": window
            .as_ref()
            .map(|_| "drop_after_closed_window")
            .unwrap_or("not_applicable"),
        "retentionEvictionEnabled": view.retention_seconds.is_some(),
        "evictedWindowEventPolicy": if view.retention_seconds.is_some() {
            window
                .as_ref()
                .map(|_| Value::String("drop_after_retention".to_owned()))
                .unwrap_or(Value::Null)
        } else {
            Value::Null
        },
        "windowPolicy": window.as_ref().map(|window| {
            json!({
                "mode": window.mode,
                "size": window.size,
                "hop": window.hop,
                "timeField": window.time_field,
                "allowedLateness": window.allowed_lateness,
                "retentionSeconds": view.retention_seconds,
                "checkpointReadiness": "closed_windows",
            })
        }),
    })))
}

fn materialized_view_runtime_policy(job: &CompiledStreamJob) -> Result<Value> {
    Ok(Value::Array(
        job.views
            .iter()
            .filter_map(|view| build_materialized_view_runtime_policy(job, &view.name).transpose())
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn common_window_retention_seconds(job: &CompiledStreamJob) -> Option<u64> {
    let mut retention = None;
    for view in &job.views {
        let Some(next) = view.retention_seconds else {
            continue;
        };
        if let Some(current) = retention {
            if current != next {
                return None;
            }
        } else {
            retention = Some(next);
        }
    }
    retention
}

fn window_runtime_policy(job: &CompiledStreamJob) -> Result<Value> {
    let Some(kernel) = job.aggregate_v2_kernel()? else {
        return Ok(Value::Null);
    };
    let Some(window) = kernel.window else {
        return Ok(Value::Null);
    };
    let retention_seconds = common_window_retention_seconds(job);
    Ok(json!({
        "mode": window.mode,
        "size": window.size,
        "hop": window.hop,
        "timeField": window.time_field,
        "allowedLateness": window.allowed_lateness,
        "retentionSeconds": retention_seconds,
        "checkpointReadiness": "closed_windows",
        "lateEventPolicy": "drop_after_closed_window",
        "retentionEvictionEnabled": retention_seconds.is_some(),
        "evictedWindowEventPolicy": retention_seconds
            .map(|_| Value::String("drop_after_retention".to_owned()))
            .unwrap_or(Value::Null),
    }))
}

fn source_cursor_runtime_stats(cursor: &LocalStreamJobSourceCursorState) -> Value {
    let offset_lag = cursor_offset_lag(cursor);
    let checkpoint_lag = cursor_checkpoint_lag(cursor);
    json!({
        "sourcePartitionId": cursor.source_partition_id,
        "nextOffset": cursor.next_offset,
        "checkpointTargetOffset": cursor.initial_checkpoint_target_offset,
        "lastAppliedOffset": cursor.last_applied_offset,
        "lastHighWatermark": cursor.last_high_watermark,
        "offsetLag": offset_lag,
        "checkpointLag": checkpoint_lag,
        "isCaughtUp": offset_lag == 0,
        "lastEventTimeWatermark": optional_datetime_value(cursor.last_event_time_watermark),
        "lastClosedWindowEnd": optional_datetime_value(cursor.last_closed_window_end),
        "pendingWindowEnds": cursor
            .pending_window_ends
            .iter()
            .map(DateTime::<Utc>::to_rfc3339)
            .collect::<Vec<_>>(),
        "droppedLateEventCount": cursor.dropped_late_event_count,
        "lastDroppedLateOffset": cursor.last_dropped_late_offset,
        "lastDroppedLateEventAt": optional_datetime_value(cursor.last_dropped_late_event_at),
        "lastDroppedLateWindowEnd": optional_datetime_value(cursor.last_dropped_late_window_end),
        "droppedEvictedWindowEventCount": cursor.dropped_evicted_window_event_count,
        "lastDroppedEvictedWindowOffset": cursor.last_dropped_evicted_window_offset,
        "lastDroppedEvictedWindowEventAt": optional_datetime_value(cursor.last_dropped_evicted_window_event_at),
        "lastDroppedEvictedWindowEnd": optional_datetime_value(cursor.last_dropped_evicted_window_end),
        "checkpointReachedAt": optional_datetime_value(cursor.checkpoint_reached_at),
        "updatedAt": cursor.updated_at.to_rfc3339(),
    })
}

fn source_lease_runtime_stats(lease: &LocalStreamJobSourceLeaseState) -> Value {
    json!({
        "sourcePartitionId": lease.source_partition_id,
        "ownerPartitionId": lease.owner_partition_id,
        "ownerEpoch": lease.owner_epoch,
        "leaseToken": lease.lease_token,
        "updatedAt": lease.updated_at.to_rfc3339(),
    })
}

fn source_partition_runtime_stats_value(
    cursors: &[&LocalStreamJobSourceCursorState],
    leases: &[&LocalStreamJobSourceLeaseState],
) -> Value {
    let partitions = cursors
        .iter()
        .map(|cursor| {
            let lease = leases
                .iter()
                .find(|lease| lease.source_partition_id == cursor.source_partition_id);
            json!({
                "sourcePartitionId": cursor.source_partition_id,
                "ownerPartitionId": lease.map(|lease| Value::from(lease.owner_partition_id)).unwrap_or(Value::Null),
                "ownerEpoch": lease.map(|lease| Value::from(lease.owner_epoch)).unwrap_or(Value::Null),
                "leaseUpdatedAt": lease.map(|lease| Value::String(lease.updated_at.to_rfc3339())).unwrap_or(Value::Null),
                "nextOffset": cursor.next_offset,
                "lastAppliedOffset": cursor.last_applied_offset,
                "lastHighWatermark": cursor.last_high_watermark,
                "offsetLag": cursor_offset_lag(cursor),
                "checkpointLag": cursor_checkpoint_lag(cursor),
                "isCaughtUp": cursor_offset_lag(cursor) == 0,
                "droppedLateEventCount": cursor.dropped_late_event_count,
                "droppedEvictedWindowEventCount": cursor.dropped_evicted_window_event_count,
                "lastEventTimeWatermark": optional_datetime_value(cursor.last_event_time_watermark),
                "lastClosedWindowEnd": optional_datetime_value(cursor.last_closed_window_end),
            })
        })
        .collect::<Vec<_>>();
    let (total_offset_lag, max_offset_lag, total_checkpoint_lag) =
        cursors.iter().fold((0i64, 0i64, 0i64), |(total, max, checkpoint), cursor| {
            let offset_lag = cursor_offset_lag(cursor);
            (
                total.saturating_add(offset_lag),
                max.max(offset_lag),
                checkpoint.saturating_add(cursor_checkpoint_lag(cursor)),
            )
        });
    json!({
        "partitions": partitions,
        "summary": {
            "partitionCount": cursors.len(),
            "caughtUpPartitionCount": cursors.iter().filter(|cursor| cursor_offset_lag(cursor) == 0).count(),
            "totalOffsetLag": total_offset_lag,
            "maxOffsetLag": max_offset_lag,
            "totalCheckpointLag": total_checkpoint_lag,
        }
    })
}

fn max_optional_datetime<I>(values: I) -> Option<DateTime<Utc>>
where
    I: IntoIterator<Item = Option<DateTime<Utc>>>,
{
    values.into_iter().flatten().max()
}

fn optional_duration_seconds(value: Option<chrono::Duration>) -> Value {
    value.map(|duration| Value::from(duration.num_seconds())).unwrap_or(Value::Null)
}

fn strong_read_metadata_from_runtime_state(
    local_state: &LocalThroughputState,
    runtime_state: &LocalStreamJobRuntimeState,
    handle_id: &str,
) -> StrongReadMetadata {
    let last_durable_position = local_state
        .load_stream_job_accepted_progress_cursor(handle_id)
        .ok()
        .flatten()
        .map(|cursor| cursor.last_durable_positions)
        .filter(|positions| !positions.is_empty())
        .unwrap_or_else(|| {
            runtime_state
                .source_cursors
                .iter()
                .map(|cursor| {
                    (
                        cursor.source_partition_id,
                        cursor
                            .last_applied_offset
                            .unwrap_or_else(|| cursor.next_offset.saturating_sub(1)),
                    )
                })
                .collect()
        });
    let last_source_frontier = runtime_state
        .source_cursors
        .iter()
        .map(|cursor| DataflowSourceFrontier {
            source_partition_id: cursor.source_partition_id,
            last_applied_offset: cursor.last_applied_offset,
            next_offset: Some(cursor.next_offset),
            high_watermark: cursor.last_high_watermark,
        })
        .collect();
    let last_sealed_checkpoint = sealed_checkpoint_states_for_runtime(local_state, runtime_state)
        .ok()
        .and_then(|states| {
            if states.len() == runtime_state.active_partitions.len() && !states.is_empty() {
                Some(StrongReadCheckpointRef {
                    checkpoint_name: runtime_state.checkpoint_name.clone(),
                    checkpoint_sequence: runtime_state.checkpoint_sequence,
                    sealed_at: states.into_iter().map(|state| state.sealed_at).max(),
                })
            } else {
                None
            }
        })
        .or_else(|| {
            (!runtime_state.checkpoint_name.is_empty() && runtime_state.checkpoint_sequence > 0)
                .then(|| StrongReadCheckpointRef {
                    checkpoint_name: runtime_state.checkpoint_name.clone(),
                    checkpoint_sequence: runtime_state.checkpoint_sequence,
                    sealed_at: runtime_state.latest_checkpoint_at,
                })
        });
    StrongReadMetadata {
        consistency: STREAM_CONSISTENCY_STRONG.to_owned(),
        owner_epoch: runtime_state.stream_owner_epoch,
        last_durable_position,
        last_source_frontier,
        last_sealed_checkpoint,
    }
}

fn strong_read_metadata_value(
    local_state: &LocalThroughputState,
    runtime_state: &LocalStreamJobRuntimeState,
    handle_id: &str,
) -> Result<Value> {
    let metadata = strong_read_metadata_from_runtime_state(local_state, runtime_state, handle_id);
    Ok(json!({
        "consistency": metadata.consistency,
        "ownerEpoch": metadata.owner_epoch,
        "lastDurablePosition": metadata.last_durable_position,
        "lastSourceFrontier": metadata.last_source_frontier.into_iter().map(|frontier| {
            json!({
                "sourcePartitionId": frontier.source_partition_id,
                "lastAppliedOffset": frontier.last_applied_offset,
                "nextOffset": frontier.next_offset,
                "highWatermark": frontier.high_watermark,
            })
        }).collect::<Vec<_>>(),
        "lastSealedCheckpoint": metadata.last_sealed_checkpoint.map(|checkpoint| {
            json!({
                "checkpointName": checkpoint.checkpoint_name,
                "checkpointSequence": checkpoint.checkpoint_sequence,
                "sealedAt": optional_datetime_value(checkpoint.sealed_at),
            })
        }).unwrap_or(Value::Null),
    }))
}

fn accepted_progress_runtime_value(
    local_state: &LocalThroughputState,
    handle_id: &str,
) -> Result<Value> {
    let cursor = local_state.load_stream_job_accepted_progress_cursor(handle_id)?;
    let tail = local_state.load_stream_job_accepted_progress_tail(handle_id, 4)?;
    let partition_positions = cursor
        .as_ref()
        .map(|state| serde_json::to_value(&state.last_durable_positions))
        .transpose()
        .context("failed to serialize accepted progress partition positions")?
        .unwrap_or_else(|| Value::Object(Map::new()));
    Ok(json!({
        "latestPosition": cursor.as_ref().map(|state| state.latest_position),
        "partitionPositions": partition_positions,
        "recent": tail.into_iter().map(|state| {
            json!({
                "position": state.accepted_progress_position,
                "batchId": state.batch_id,
                "streamPartitionId": state.stream_partition_id,
                "sourcePartitionId": state.record.source_partition_id,
                "sourceEpoch": state.record.source_epoch,
                "ownerEpoch": state.record.owner_epoch,
                "recordCount": state.record.summary.record_count,
                "acceptedAt": state.accepted_at.to_rfc3339(),
            })
        }).collect::<Vec<_>>(),
    }))
}

fn sealed_checkpoint_states_for_runtime(
    local_state: &LocalThroughputState,
    runtime_state: &LocalStreamJobRuntimeState,
) -> Result<Vec<crate::local_state::LocalStreamJobSealedCheckpointState>> {
    if runtime_state.checkpoint_name.is_empty() {
        return Ok(Vec::new());
    }
    Ok(local_state
        .load_all_stream_job_sealed_checkpoints()?
        .into_iter()
        .filter(|state| {
            state.handle_id == runtime_state.handle_id
                && state.checkpoint_name == runtime_state.checkpoint_name
                && state.checkpoint_sequence == runtime_state.checkpoint_sequence
                && runtime_state.active_partitions.contains(&state.stream_partition_id)
        })
        .collect())
}

fn sealed_checkpoint_runtime_value(
    local_state: &LocalThroughputState,
    runtime_state: &LocalStreamJobRuntimeState,
) -> Result<Value> {
    let sealed = sealed_checkpoint_states_for_runtime(local_state, runtime_state)?;
    if sealed.is_empty() {
        let fallback = runtime_state
            .checkpoint_partitions
            .iter()
            .filter(|state| {
                state.checkpoint_name == runtime_state.checkpoint_name
                    && state.checkpoint_sequence == runtime_state.checkpoint_sequence
                    && runtime_state.active_partitions.contains(&state.stream_partition_id)
            })
            .map(|state| {
                json!({
                    "streamPartitionId": state.stream_partition_id,
                    "checkpointName": state.checkpoint_name,
                    "checkpointSequence": state.checkpoint_sequence,
                    "acceptedProgressPosition": Value::Null,
                    "sealedAt": state.reached_at.to_rfc3339(),
                    "stateImage": Value::Null,
                })
            })
            .collect::<Vec<_>>();
        return Ok(json!({
            "count": fallback.len(),
            "partitions": fallback,
        }));
    }
    Ok(json!({
        "count": sealed.len(),
        "partitions": sealed.into_iter().map(|state| {
            json!({
                "streamPartitionId": state.stream_partition_id,
                "checkpointName": state.checkpoint_name,
                "checkpointSequence": state.checkpoint_sequence,
                "acceptedProgressPosition": state.record.accepted_progress_position,
                "sealedAt": state.sealed_at.to_rfc3339(),
                "stateImage": {
                    "manifestKey": state.record.state_image.manifest_key,
                    "checksum": state.record.state_image.checksum,
                },
            })
        }).collect::<Vec<_>>(),
    }))
}

fn build_stream_job_runtime_stats_output_from_runtime_state(
    local_state: &LocalThroughputState,
    runtime_state: &LocalStreamJobRuntimeState,
    handle: &StreamJobBridgeHandleRecord,
    query: &StreamJobQueryRecord,
) -> Result<Value> {
    if matches!(query.parsed_consistency(), Some(StreamJobQueryConsistency::Eventual)) {
        anyhow::bail!("{STREAM_JOB_RUNTIME_STATS_QUERY_NAME} only supports strong consistency");
    }

    let source_partition_id = query_source_partition_id(query)?;
    let source_cursors = runtime_state
        .source_cursors
        .iter()
        .filter(|cursor| {
            source_partition_id
                .is_none_or(|partition_id| cursor.source_partition_id == partition_id)
        })
        .collect::<Vec<_>>();
    let source_leases = runtime_state
        .source_partition_leases
        .iter()
        .filter(|lease| {
            source_partition_id.is_none_or(|partition_id| lease.source_partition_id == partition_id)
        })
        .collect::<Vec<_>>();
    let source_partition_stats =
        source_partition_runtime_stats_value(&source_cursors, &source_leases);
    if let Some(source_partition_id) = source_partition_id
        && source_cursors.is_empty()
        && source_leases.is_empty()
    {
        anyhow::bail!("source partition {source_partition_id} is not active for this stream job");
    }

    let mut output = Map::new();
    output.insert("handleId".to_owned(), Value::String(runtime_state.handle_id.clone()));
    output.insert("jobId".to_owned(), Value::String(runtime_state.job_id.clone()));
    output.insert("jobName".to_owned(), Value::String(runtime_state.job_name.clone()));
    output.insert("queryName".to_owned(), Value::String(query.query_name.clone()));
    if let Some(job) = resolve_stream_job_definition_without_store(handle)? {
        output.insert("jobRuntime".to_owned(), Value::String(job.runtime.clone()));
        output.insert(
            "runtimeContract".to_owned(),
            Value::String(STREAMS_DATAFLOW_V1_CONTRACT.to_owned()),
        );
        output
            .insert("dataflowPlan".to_owned(), dataflow_plan_summary_value_for_job(handle, &job)?);
        output.insert(
            "jobClassification".to_owned(),
            job.classification.clone().map(Value::String).unwrap_or(Value::Null),
        );
        output.insert("preKeyPolicy".to_owned(), pre_key_runtime_policy(&job)?);
        output.insert("preKeyStats".to_owned(), pre_key_runtime_stats_value(runtime_state));
        output.insert("hotKeyStats".to_owned(), hot_key_runtime_stats_value(runtime_state));
        output.insert(
            "ownerPartitionStats".to_owned(),
            owner_partition_runtime_stats_value(local_state, runtime_state, query.requested_at)?,
        );
        output.insert("materializedViews".to_owned(), materialized_view_runtime_policy(&job)?);
        output.insert("windowPolicy".to_owned(), window_runtime_policy(&job)?);
    }
    output.insert(
        "sourceKind".to_owned(),
        runtime_state.source_kind.clone().map(Value::String).unwrap_or(Value::Null),
    );
    output.insert(
        "sourceName".to_owned(),
        runtime_state.source_name.clone().map(Value::String).unwrap_or(Value::Null),
    );
    output
        .insert("checkpointName".to_owned(), Value::String(runtime_state.checkpoint_name.clone()));
    output.insert("checkpointSequence".to_owned(), Value::from(runtime_state.checkpoint_sequence));
    output.insert(
        "latestCheckpointAt".to_owned(),
        optional_datetime_value(runtime_state.latest_checkpoint_at),
    );
    output.insert("evictedWindowCount".to_owned(), Value::from(runtime_state.evicted_window_count));
    output.insert(
        "lastEvictedWindowEnd".to_owned(),
        optional_datetime_value(runtime_state.last_evicted_window_end),
    );
    output
        .insert("lastEvictedAt".to_owned(), optional_datetime_value(runtime_state.last_evicted_at));
    output.insert("streamOwnerEpoch".to_owned(), Value::from(runtime_state.stream_owner_epoch));
    output.insert(
        "terminalStatus".to_owned(),
        runtime_state.terminal_status.clone().map(Value::String).unwrap_or(Value::Null),
    );
    output.insert("terminalAt".to_owned(), optional_datetime_value(runtime_state.terminal_at));
    output.insert(
        "activePartitions".to_owned(),
        Value::Array(runtime_state.active_partitions.iter().copied().map(Value::from).collect()),
    );
    if let Some(source_partition_id) = source_partition_id {
        output.insert("sourcePartitionId".to_owned(), Value::from(source_partition_id));
    }
    output.insert(
        "sourceCursors".to_owned(),
        Value::Array(source_cursors.into_iter().map(source_cursor_runtime_stats).collect()),
    );
    output.insert(
        "sourceLeases".to_owned(),
        Value::Array(source_leases.into_iter().map(source_lease_runtime_stats).collect()),
    );
    output.insert("sourcePartitionStats".to_owned(), source_partition_stats);
    output.insert(
        "sealedCheckpoint".to_owned(),
        sealed_checkpoint_runtime_value(local_state, runtime_state)?,
    );
    output.insert(
        "acceptedProgress".to_owned(),
        accepted_progress_runtime_value(local_state, &handle.handle_id)?,
    );
    output.insert("consistency".to_owned(), Value::String(STREAM_CONSISTENCY_STRONG.to_owned()));
    output.insert(
        "consistencySource".to_owned(),
        Value::String("stream_owner_local_state".to_owned()),
    );
    Ok(Value::Object(output))
}

fn build_stream_job_runtime_stats_output_on_local_state(
    local_state: &LocalThroughputState,
    handle: &StreamJobBridgeHandleRecord,
    query: &StreamJobQueryRecord,
) -> Result<Option<Value>> {
    let Some(runtime_state) = local_state.load_stream_job_runtime_state(&handle.handle_id)? else {
        return Ok(None);
    };
    Ok(Some(build_stream_job_runtime_stats_output_from_runtime_state(
        local_state,
        &runtime_state,
        handle,
        query,
    )?))
}

fn build_stream_job_view_runtime_stats_output_on_local_state(
    local_state: &LocalThroughputState,
    handle: &StreamJobBridgeHandleRecord,
    query: &StreamJobQueryRecord,
) -> Result<Option<Value>> {
    let Some(runtime_state) = local_state.load_stream_job_runtime_state(&handle.handle_id)? else {
        return Ok(None);
    };
    if matches!(query.parsed_consistency(), Some(StreamJobQueryConsistency::Eventual)) {
        anyhow::bail!(
            "{STREAM_JOB_VIEW_RUNTIME_STATS_QUERY_NAME} only supports strong consistency"
        );
    }
    let Some(view_name) = query_view_name(query)? else {
        anyhow::bail!("{STREAM_JOB_VIEW_RUNTIME_STATS_QUERY_NAME} requires viewName");
    };
    let Some(job) = resolve_stream_job_definition_without_store(handle)? else {
        return Ok(None);
    };
    let Some(view) = job.views.iter().find(|view| view.name == view_name) else {
        return Ok(None);
    };
    let Some(policy) = build_materialized_view_runtime_policy(&job, &view_name)? else {
        return Ok(None);
    };
    let stored_entries =
        local_state.load_stream_job_views_for_view(&handle.handle_id, &view_name)?;
    let latest_materialized_window_end =
        max_optional_datetime(stored_entries.iter().map(|view_state| {
            view_state
                .output
                .get("windowEnd")
                .and_then(Value::as_str)
                .and_then(|text| chrono::DateTime::parse_from_rfc3339(text).ok())
                .map(|value| value.with_timezone(&Utc))
        }));
    let active_entries = stored_entries
        .iter()
        .filter(|view_state| {
            !window_expired_from_deadline(
                view_window_expired_at(Some(&job), view, &view_state.output).ok().flatten(),
                query.requested_at,
            )
        })
        .count();
    let latest_checkpoint_sequence =
        stored_entries.iter().map(|view| view.checkpoint_sequence).max();
    let latest_updated_at = stored_entries.iter().map(|view| view.updated_at).max();
    let latest_event_time_watermark = max_optional_datetime(
        runtime_state.source_cursors.iter().map(|cursor| cursor.last_event_time_watermark),
    );
    let latest_closed_window_end = max_optional_datetime(
        runtime_state.source_cursors.iter().map(|cursor| cursor.last_closed_window_end),
    );
    let freshness = json!({
        "latestUpdateAgeSeconds": optional_duration_seconds(
            latest_updated_at.map(|updated_at| query.requested_at - updated_at)
        ),
        "checkpointSequenceLag": latest_checkpoint_sequence
            .map(|sequence| Value::from(runtime_state.checkpoint_sequence.saturating_sub(sequence)))
            .unwrap_or(Value::Null),
        "latestEventTimeWatermark": optional_datetime_value(latest_event_time_watermark),
        "latestClosedWindowEnd": optional_datetime_value(latest_closed_window_end),
        "latestMaterializedWindowEnd": optional_datetime_value(latest_materialized_window_end),
        "eventTimeLagSeconds": optional_duration_seconds(
            latest_event_time_watermark.zip(latest_materialized_window_end).map(|(watermark, materialized)| {
                watermark - materialized
            })
        ),
        "closedWindowLagSeconds": optional_duration_seconds(
            latest_closed_window_end.zip(latest_materialized_window_end).map(|(closed, materialized)| {
                closed - materialized
            })
        ),
    });
    let view_runtime_stats = runtime_state.view_runtime_stats(&view_name);

    Ok(Some(json!({
        "queryName": query.query_name,
        "handleId": runtime_state.handle_id,
        "jobId": runtime_state.job_id,
        "jobName": runtime_state.job_name,
        "runtimeContract": STREAMS_DATAFLOW_V1_CONTRACT,
        "dataflowPlan": dataflow_plan_summary_value_for_job(handle, &job)?,
        "viewName": view_name,
        "policy": policy,
        "preKeyStats": pre_key_runtime_stats_value(&runtime_state),
        "hotKeyStats": hot_key_runtime_stats_value(&runtime_state),
        "ownerPartitionStats": owner_partition_runtime_stats_value(
            local_state,
            &runtime_state,
            query.requested_at,
        )?,
        "sourcePartitionStats": source_partition_runtime_stats_value(
            &runtime_state.source_cursors.iter().collect::<Vec<_>>(),
            &runtime_state.source_partition_leases.iter().collect::<Vec<_>>(),
        ),
        "sealedCheckpoint": sealed_checkpoint_runtime_value(local_state, &runtime_state)?,
        "acceptedProgress": accepted_progress_runtime_value(local_state, &handle.handle_id)?,
        "storedKeyCount": stored_entries.len(),
        "activeKeyCount": active_entries,
        "latestCheckpointSequence": latest_checkpoint_sequence,
        "latestUpdatedAt": latest_updated_at.map(|value| value.to_rfc3339()),
        "freshness": freshness,
        "historicalEvictedWindowCount": view_runtime_stats
            .map(|stats| stats.evicted_window_count)
            .unwrap_or_default(),
        "historicalLastEvictedWindowEnd": optional_datetime_value(
            view_runtime_stats.and_then(|stats| stats.last_evicted_window_end)
        ),
        "historicalLastEvictedAt": optional_datetime_value(
            view_runtime_stats.and_then(|stats| stats.last_evicted_at)
        ),
        "jobEvictedWindowCount": runtime_state.evicted_window_count,
        "lastEvictedWindowEnd": optional_datetime_value(runtime_state.last_evicted_window_end),
        "lastEvictedAt": optional_datetime_value(runtime_state.last_evicted_at),
        "streamOwnerEpoch": runtime_state.stream_owner_epoch,
        "consistency": STREAM_CONSISTENCY_STRONG,
        "consistencySource": "stream_owner_local_state",
    })))
}

async fn build_stream_job_view_runtime_stats_output(
    state: &AppState,
    handle: &StreamJobBridgeHandleRecord,
    query: &StreamJobQueryRecord,
) -> Result<Option<Value>> {
    let Some(mut output) = build_stream_job_view_runtime_stats_output_on_local_state(
        owner_local_state_for_stream_job(state, &handle.job_id),
        handle,
        query,
    )?
    else {
        return Ok(None);
    };
    let Some(view_name) = query_view_name(query)? else {
        return Ok(Some(output));
    };
    let Some(job) = parse_compiled_job_from_config_ref(handle)? else {
        return Ok(Some(output));
    };
    let Some(view) = job.views.iter().find(|view| view.name == view_name) else {
        return Ok(Some(output));
    };
    let projection_stats = if let Some(projection_stats) =
        eventual_projection_stats_output(state, handle, view, query.requested_at).await?
    {
        projection_stats
    } else {
        json!({
            "supported": false,
            "rebuildSupported": false,
            "summary": Value::Null,
            "freshness": Value::Null,
        })
    };
    if let Some(object) = output.as_object_mut() {
        object.insert("projectionStats".to_owned(), projection_stats);
    }
    Ok(Some(output))
}

async fn build_stream_job_view_projection_stats_output(
    state: &AppState,
    handle: &StreamJobBridgeHandleRecord,
    query: &StreamJobQueryRecord,
) -> Result<Option<Value>> {
    if matches!(query.parsed_consistency(), Some(StreamJobQueryConsistency::Eventual)) {
        anyhow::bail!(
            "{STREAM_JOB_VIEW_PROJECTION_STATS_QUERY_NAME} only supports strong consistency"
        );
    }
    let Some(view_name) = query_view_name(query)? else {
        anyhow::bail!("{STREAM_JOB_VIEW_PROJECTION_STATS_QUERY_NAME} requires viewName");
    };
    let Some(projection_stats) =
        stream_job_view_projection_stats_output(state, handle, &view_name, query.requested_at)
            .await?
    else {
        return Ok(None);
    };
    Ok(Some(json!({
        "queryName": query.query_name,
        "handleId": handle.handle_id,
        "jobId": handle.job_id,
        "jobName": handle.job_name,
        "viewName": view_name,
        "consistency": STREAM_CONSISTENCY_STRONG,
        "consistencySource": "stream_projection_summary",
        "projectionStats": projection_stats,
    })))
}

async fn build_stream_job_projection_stats_output(
    state: &AppState,
    handle: &StreamJobBridgeHandleRecord,
    query: &StreamJobQueryRecord,
) -> Result<Option<Value>> {
    if matches!(query.parsed_consistency(), Some(StreamJobQueryConsistency::Eventual)) {
        anyhow::bail!("{STREAM_JOB_PROJECTION_STATS_QUERY_NAME} only supports strong consistency");
    }
    let Some(job) = parse_compiled_job_from_config_ref(handle)? else {
        return Ok(None);
    };
    let stale_only = query_bool_arg(query, "staleOnly")?.unwrap_or(false);
    let min_checkpoint_lag = query_i64_arg(query, "minCheckpointLag")?.unwrap_or(0);
    let view_names = query_string_array_arg(query, "viewNames")?;
    let mut views = Vec::new();
    for view in job.views.iter().filter(|view| view_supports_eventual_reads(view)) {
        if let Some(view_names) = view_names.as_ref()
            && !view_names.iter().any(|candidate| candidate == &view.name)
        {
            continue;
        }
        if let Some(projection_stats) =
            eventual_projection_stats_output(state, handle, view, query.requested_at).await?
        {
            let checkpoint_lag = projection_stats
                .get("freshness")
                .and_then(|value| value.get("checkpointSequenceLag"))
                .and_then(Value::as_i64)
                .unwrap_or_default();
            let is_stale = checkpoint_lag > 0;
            if stale_only && !is_stale {
                continue;
            }
            if checkpoint_lag < min_checkpoint_lag {
                continue;
            }
            views.push(json!({
                "viewName": view.name,
                "isStale": is_stale,
                "projectionStats": projection_stats,
            }));
        }
    }
    Ok(Some(json!({
        "queryName": query.query_name,
        "handleId": handle.handle_id,
        "jobId": handle.job_id,
        "jobName": handle.job_name,
        "viewCount": views.len(),
        "consistency": STREAM_CONSISTENCY_STRONG,
        "consistencySource": "stream_projection_summary",
        "filters": {
            "staleOnly": stale_only,
            "minCheckpointLag": min_checkpoint_lag,
            "viewNames": view_names,
        },
        "views": views,
    })))
}

fn workflow_signal_status_value(status: &WorkflowSignalStatus) -> &'static str {
    match status {
        WorkflowSignalStatus::Queued => "queued",
        WorkflowSignalStatus::Dispatching => "dispatching",
        WorkflowSignalStatus::Consumed => "consumed",
    }
}

async fn resolve_stream_job_signal_target(
    state: &AppState,
    handle: &StreamJobBridgeHandleRecord,
    signal: &LocalStreamJobWorkflowSignalState,
) -> Result<StreamJobSignalTarget> {
    if !matches!(handle.parsed_origin_kind(), Some(StreamJobOriginKind::Standalone)) {
        return Ok(StreamJobSignalTarget {
            tenant_id: handle.tenant_id.clone(),
            instance_id: handle.instance_id.clone(),
            run_id: Some(handle.run_id.clone()),
        });
    }

    let target_instance_id = signal
        .payload
        .get("targetWorkflowInstanceId")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or(signal.logical_key.as_str())
        .to_owned();
    let target_run_id = signal
        .payload
        .get("targetWorkflowRunId")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    if let Some(explicit_run_id) = target_run_id {
        return Ok(StreamJobSignalTarget {
            tenant_id: handle.tenant_id.clone(),
            instance_id: target_instance_id,
            run_id: Some(explicit_run_id),
        });
    }

    let Some(instance) = state.store.get_instance(&handle.tenant_id, &target_instance_id).await?
    else {
        return Ok(StreamJobSignalTarget {
            tenant_id: handle.tenant_id.clone(),
            instance_id: target_instance_id,
            run_id: None,
        });
    };

    Ok(StreamJobSignalTarget {
        tenant_id: handle.tenant_id.clone(),
        instance_id: instance.instance_id,
        run_id: Some(instance.run_id),
    })
}

async fn build_stream_job_signal_bridge_state(
    state: &AppState,
    handle: &StreamJobBridgeHandleRecord,
    signal: &LocalStreamJobWorkflowSignalState,
) -> Result<StreamJobSignalBridgeState> {
    let callback_dedupe_key = stream_job_signal_callback_dedupe_key(
        &handle.tenant_id,
        &handle.instance_id,
        &handle.run_id,
        &handle.job_id,
        &signal.operator_id,
        &signal.logical_key,
    );
    let target = resolve_stream_job_signal_target(state, handle, signal).await?;
    let workflow_signal = if let Some(target_run_id) = target.run_id.as_deref() {
        state
            .store
            .get_signal_any_status(
                &target.tenant_id,
                &target.instance_id,
                target_run_id,
                &signal.signal_id,
            )
            .await?
    } else {
        None
    };
    let bridge_status = if let Some(workflow_signal) = workflow_signal.as_ref() {
        workflow_signal_status_value(&workflow_signal.status).to_owned()
    } else if signal.published_at.is_none() {
        "pending_publication".to_owned()
    } else if target.run_id.is_none() {
        "target_missing".to_owned()
    } else {
        "published_unobserved".to_owned()
    };

    Ok(StreamJobSignalBridgeState {
        operator_id: signal.operator_id.clone(),
        view_name: signal.view_name.clone(),
        logical_key: signal.logical_key.clone(),
        signal_id: signal.signal_id.clone(),
        signal_type: signal.signal_type.clone(),
        payload: signal.payload.clone(),
        stream_owner_epoch: signal.stream_owner_epoch,
        signaled_at: signal.signaled_at,
        published_at: signal.published_at,
        updated_at: signal.updated_at,
        callback_dedupe_key: callback_dedupe_key.clone(),
        callback_event_id: stream_job_callback_event_id(&callback_dedupe_key).to_string(),
        target_tenant_id: target.tenant_id,
        target_instance_id: target.instance_id,
        target_run_id: target.run_id,
        bridge_status,
        workflow_signal_status: workflow_signal.as_ref().map(|workflow_signal| {
            workflow_signal_status_value(&workflow_signal.status).to_owned()
        }),
        workflow_signal_dedupe_key: workflow_signal
            .as_ref()
            .and_then(|workflow_signal| workflow_signal.dedupe_key.clone()),
        workflow_signal_source_event_id: workflow_signal
            .as_ref()
            .map(|workflow_signal| workflow_signal.source_event_id.to_string()),
        workflow_signal_dispatch_event_id: workflow_signal.as_ref().and_then(|workflow_signal| {
            workflow_signal.dispatch_event_id.map(|value| value.to_string())
        }),
        workflow_signal_consumed_event_id: workflow_signal.as_ref().and_then(|workflow_signal| {
            workflow_signal.consumed_event_id.map(|value| value.to_string())
        }),
        workflow_signal_enqueued_at: workflow_signal
            .as_ref()
            .map(|workflow_signal| workflow_signal.enqueued_at),
        workflow_signal_consumed_at: workflow_signal
            .as_ref()
            .and_then(|workflow_signal| workflow_signal.consumed_at),
    })
}

pub(crate) async fn load_stream_job_signal_bridge_states(
    state: &AppState,
    handle: &StreamJobBridgeHandleRecord,
    limit: usize,
    offset: usize,
    signal_type: Option<&str>,
    operator_id: Option<&str>,
    logical_key: Option<&str>,
) -> Result<(usize, Vec<StreamJobSignalBridgeState>)> {
    let mut signals = owner_local_state_for_stream_job(state, &handle.job_id)
        .load_all_stream_job_workflow_signals()?
        .into_iter()
        .filter(|signal| signal.handle_id == handle.handle_id && signal.job_id == handle.job_id)
        .filter(|signal| signal_type.is_none_or(|expected| signal.signal_type == expected))
        .filter(|signal| operator_id.is_none_or(|expected| signal.operator_id == expected))
        .filter(|signal| logical_key.is_none_or(|expected| signal.logical_key == expected))
        .collect::<Vec<_>>();
    signals.sort_by(|left, right| {
        right.signaled_at.cmp(&left.signaled_at).then_with(|| left.signal_id.cmp(&right.signal_id))
    });
    let total = signals.len();
    let offset = offset.min(total);
    let limit = limit.clamp(1, 1_000);
    let selected = signals.into_iter().skip(offset).take(limit).collect::<Vec<_>>();
    let mut bridge_states = Vec::with_capacity(selected.len());
    for signal in selected {
        bridge_states.push(build_stream_job_signal_bridge_state(state, handle, &signal).await?);
    }
    Ok((total, bridge_states))
}

fn bridge_state_controls_output(
    handle: &StreamJobBridgeHandleRecord,
    uses_topic_source: bool,
) -> Value {
    let status = handle.parsed_status();
    let terminal = status.is_some_and(StreamJobBridgeHandleStatus::is_terminal);
    let cancellation_requested =
        matches!(status, Some(StreamJobBridgeHandleStatus::CancellationRequested))
            || handle.cancellation_requested_at.is_some();
    json!({
        "canPause": uses_topic_source
            && !terminal
            && !cancellation_requested
            && !matches!(status, Some(StreamJobBridgeHandleStatus::Paused)),
        "canResume": uses_topic_source && matches!(status, Some(StreamJobBridgeHandleStatus::Paused)),
        "canCancel": !terminal && !cancellation_requested,
    })
}

fn bridge_state_checkpoint_output(
    handle: &StreamJobBridgeHandleRecord,
    checkpoint: &StreamJobCheckpointRecord,
) -> Value {
    json!({
        "awaitRequestId": checkpoint.await_request_id.clone(),
        "checkpointName": checkpoint.checkpoint_name.clone(),
        "checkpointSequence": checkpoint.checkpoint_sequence,
        "status": checkpoint.status.clone(),
        "workflowOwnerEpoch": checkpoint.workflow_owner_epoch,
        "streamOwnerEpoch": checkpoint.stream_owner_epoch,
        "reachedAt": optional_datetime_value(checkpoint.reached_at),
        "acceptedAt": optional_datetime_value(checkpoint.accepted_at),
        "cancelledAt": optional_datetime_value(checkpoint.cancelled_at),
        "output": checkpoint.output.clone(),
        "isStaleForCurrentOwnerEpoch": checkpoint.is_stale_for_handle_epoch(handle.stream_owner_epoch),
        "nextRepair": optional_repair_value(
            checkpoint.next_repair_for_handle_epoch(handle.stream_owner_epoch)
        ),
        "createdAt": checkpoint.created_at.to_rfc3339(),
        "updatedAt": checkpoint.updated_at.to_rfc3339(),
    })
}

fn bridge_state_query_summary_output(
    handle: &StreamJobBridgeHandleRecord,
    query: &StreamJobQueryRecord,
) -> Value {
    json!({
        "queryId": query.query_id.clone(),
        "queryName": query.query_name.clone(),
        "consistency": query.consistency.clone(),
        "status": query.status.clone(),
        "requestedAt": query.requested_at.to_rfc3339(),
        "completedAt": optional_datetime_value(query.completed_at),
        "acceptedAt": optional_datetime_value(query.accepted_at),
        "cancelledAt": optional_datetime_value(query.cancelled_at),
        "workflowOwnerEpoch": query.workflow_owner_epoch,
        "streamOwnerEpoch": query.stream_owner_epoch,
        "error": query.error.clone(),
        "isStaleForCurrentOwnerEpoch": query.is_stale_for_handle_epoch(handle.stream_owner_epoch),
        "nextRepair": optional_repair_value(query.next_repair_for_handle_epoch(handle.stream_owner_epoch)),
    })
}

fn bridge_state_lifecycle_output(
    handle: &StreamJobBridgeHandleRecord,
    ledger: Option<&StreamJobBridgeLedgerRecord>,
    stream_job: Option<&StreamJobRecord>,
) -> Value {
    let pending_repairs = ledger
        .map(|ledger| repair_list_value(ledger.pending_repairs()))
        .unwrap_or(Value::Array(vec![]));
    let next_repair =
        ledger.map(|ledger| optional_repair_value(ledger.next_repair())).unwrap_or(Value::Null);
    json!({
        "bridgeStatus": handle.status.clone(),
        "streamStatus": stream_job
            .map(|job| Value::String(job.status.clone()))
            .unwrap_or_else(|| Value::String("missing".to_owned())),
        "workflowOwnerEpoch": handle.workflow_owner_epoch,
        "streamOwnerEpoch": handle.stream_owner_epoch.or_else(|| stream_job.and_then(|job| job.stream_owner_epoch)),
        "cancellationRequestedAt": optional_datetime_value(handle.cancellation_requested_at),
        "cancellationReason": handle.cancellation_reason.clone(),
        "workflowAcceptedAt": optional_datetime_value(
            handle
                .workflow_accepted_at
                .or_else(|| stream_job.and_then(|job| job.workflow_accepted_at))
        ),
        "terminalAt": optional_datetime_value(
            handle.terminal_at.or_else(|| stream_job.and_then(|job| job.terminal_at))
        ),
        "terminalEventId": handle
            .terminal_event_id
            .map(|value| Value::String(value.to_string()))
            .unwrap_or(Value::Null),
        "terminalOutput": handle.terminal_output.as_ref().cloned().unwrap_or(Value::Null),
        "terminalError": handle
            .terminal_error
            .as_ref()
            .map(|value| Value::String(value.clone()))
            .unwrap_or(Value::Null),
        "startingAt": optional_datetime_value(stream_job.and_then(|job| job.starting_at)),
        "runningAt": optional_datetime_value(stream_job.and_then(|job| job.running_at)),
        "pausedAt": match handle.parsed_status() {
            Some(StreamJobBridgeHandleStatus::Paused) => Value::String(handle.updated_at.to_rfc3339()),
            _ => Value::Null,
        },
        "drainingAt": optional_datetime_value(stream_job.and_then(|job| job.draining_at)),
        "latestCheckpointName": stream_job
            .and_then(|job| job.latest_checkpoint_name.as_ref())
            .map(|value| Value::String(value.clone()))
            .unwrap_or(Value::Null),
        "latestCheckpointSequence": stream_job
            .and_then(|job| job.latest_checkpoint_sequence.map(Value::from))
            .unwrap_or(Value::Null),
        "latestCheckpointAt": optional_datetime_value(stream_job.and_then(|job| job.latest_checkpoint_at)),
        "latestCheckpointOutput": stream_job
            .and_then(|job| job.latest_checkpoint_output.as_ref().cloned())
            .unwrap_or(Value::Null),
        "pendingRepairs": pending_repairs,
        "nextRepair": next_repair,
        "createdAt": handle.created_at.to_rfc3339(),
        "updatedAt": handle.updated_at.to_rfc3339(),
    })
}

async fn build_stream_job_bridge_state_output(
    state: &AppState,
    handle: &StreamJobBridgeHandleRecord,
    query: &StreamJobQueryRecord,
) -> Result<Option<Value>> {
    if matches!(query.parsed_consistency(), Some(StreamJobQueryConsistency::Eventual)) {
        anyhow::bail!("{STREAM_JOB_BRIDGE_STATE_QUERY_NAME} only supports strong consistency");
    }
    let checkpoint_limit = query_i64_arg(query, "checkpointLimit")?.unwrap_or(25).clamp(1, 250);
    let query_limit = query_i64_arg(query, "queryLimit")?.unwrap_or(25).clamp(1, 250);
    let signal_limit = query_i64_arg(query, "signalLimit")?.unwrap_or(25).clamp(1, 250);
    let ledger = state.store.load_stream_job_bridge_ledger_for_handle(&handle.handle_id).await?;
    let stream_job = state
        .store
        .get_stream_job(&handle.tenant_id, &handle.instance_id, &handle.run_id, &handle.job_id)
        .await?;
    let checkpoints = state
        .store
        .list_stream_job_bridge_checkpoints_for_handle_page(&handle.handle_id, checkpoint_limit, 0)
        .await?;
    let queries = state
        .store
        .list_stream_job_bridge_queries_for_handle_page(&handle.handle_id, query_limit, 0)
        .await?;
    let (signal_count, signals) = load_stream_job_signal_bridge_states(
        state,
        handle,
        signal_limit as usize,
        0,
        None,
        None,
        None,
    )
    .await?;
    let uses_topic_source = resolve_stream_job_definition(Some(&state.store), handle)
        .await?
        .is_some_and(|job| job.source.kind == STREAM_SOURCE_TOPIC);

    Ok(Some(json!({
        "queryName": query.query_name.clone(),
        "handleId": handle.handle_id.clone(),
        "jobId": handle.job_id.clone(),
        "jobName": handle.job_name.clone(),
        "workflow": {
            "tenantId": handle.tenant_id.clone(),
            "instanceId": handle.instance_id.clone(),
            "runId": handle.run_id.clone(),
            "definitionId": handle.definition_id.clone(),
            "definitionVersion": handle.definition_version,
            "artifactHash": handle.artifact_hash.clone(),
            "workflowEventId": handle.workflow_event_id.to_string(),
            "originKind": handle.origin_kind.clone(),
            "bridgeRequestId": handle.bridge_request_id.clone(),
        },
        "lifecycle": bridge_state_lifecycle_output(handle, ledger.as_ref(), stream_job.as_ref()),
        "controls": bridge_state_controls_output(handle, uses_topic_source),
        "checkpointCount": ledger.as_ref().map(|ledger| ledger.checkpoint_count).unwrap_or_else(|| checkpoints.len() as u64),
        "queryCount": ledger.as_ref().map(|ledger| ledger.query_count).unwrap_or_else(|| queries.len() as u64),
        "signalCount": signal_count,
        "latestQuery": ledger.as_ref().map(|ledger| json!({
            "queryId": ledger.latest_query_id.clone(),
            "queryName": ledger.latest_query_name.clone(),
            "status": ledger.latest_query_status.clone(),
            "consistency": ledger.latest_query_consistency.clone(),
            "requestedAt": optional_datetime_value(ledger.latest_query_requested_at),
            "completedAt": optional_datetime_value(ledger.latest_query_completed_at),
            "acceptedAt": optional_datetime_value(ledger.latest_query_accepted_at),
        })).unwrap_or(Value::Null),
        "latestSignal": signals.first().map(|signal| json!({
            "signalId": signal.signal_id,
            "signalType": signal.signal_type,
            "operatorId": signal.operator_id,
            "logicalKey": signal.logical_key,
            "bridgeStatus": signal.bridge_status,
            "signaledAt": signal.signaled_at.to_rfc3339(),
            "publishedAt": optional_datetime_value(signal.published_at),
            "workflowSignalStatus": signal.workflow_signal_status,
            "workflowSignalConsumedAt": optional_datetime_value(signal.workflow_signal_consumed_at),
        })).unwrap_or(Value::Null),
        "checkpoints": checkpoints
            .iter()
            .map(|checkpoint| bridge_state_checkpoint_output(handle, checkpoint))
            .collect::<Vec<_>>(),
        "queries": queries
            .iter()
            .map(|query_record| bridge_state_query_summary_output(handle, query_record))
            .collect::<Vec<_>>(),
        "signals": signals
            .iter()
            .map(|signal| json!({
                "operatorId": signal.operator_id,
                "viewName": signal.view_name,
                "logicalKey": signal.logical_key,
                "signalId": signal.signal_id,
                "signalType": signal.signal_type,
                "payload": signal.payload,
                "streamOwnerEpoch": signal.stream_owner_epoch,
                "signaledAt": signal.signaled_at.to_rfc3339(),
                "publishedAt": optional_datetime_value(signal.published_at),
                "updatedAt": signal.updated_at.to_rfc3339(),
                "callbackDedupeKey": signal.callback_dedupe_key,
                "callbackEventId": signal.callback_event_id,
                "targetWorkflow": {
                    "tenantId": signal.target_tenant_id,
                    "instanceId": signal.target_instance_id,
                    "runId": signal.target_run_id,
                },
                "bridgeStatus": signal.bridge_status,
                "workflowSignalStatus": signal.workflow_signal_status,
                "workflowSignalDedupeKey": signal.workflow_signal_dedupe_key,
                "workflowSignalSourceEventId": signal.workflow_signal_source_event_id,
                "workflowSignalDispatchEventId": signal.workflow_signal_dispatch_event_id,
                "workflowSignalConsumedEventId": signal.workflow_signal_consumed_event_id,
                "workflowSignalEnqueuedAt": optional_datetime_value(signal.workflow_signal_enqueued_at),
                "workflowSignalConsumedAt": optional_datetime_value(signal.workflow_signal_consumed_at),
            }))
            .collect::<Vec<_>>(),
        "consistency": STREAM_CONSISTENCY_STRONG,
        "consistencySource": "stream_bridge_store",
    })))
}

fn prunable_window_views(
    local_state: &LocalThroughputState,
    handle: &StreamJobBridgeHandleRecord,
    job: &CompiledStreamJob,
    runtime_state: &LocalStreamJobRuntimeState,
    occurred_at: DateTime<Utc>,
) -> Result<Vec<(LocalStreamJobViewState, Option<DateTime<Utc>>, Option<u64>)>> {
    let Some(kernel) = job.aggregate_v2_kernel()? else {
        return Ok(Vec::new());
    };
    if kernel.window.is_none() || runtime_state.source_cursors.is_empty() {
        return Ok(Vec::new());
    }
    let allowed_lateness = kernel
        .window
        .and_then(|window| window.allowed_lateness)
        .as_deref()
        .map(parse_window_size)
        .transpose()?;
    let mut prunable = Vec::new();
    for view in &job.views {
        let Some(retention_seconds) = view.retention_seconds else {
            continue;
        };
        for view_state in
            local_state.load_stream_job_views_for_view(&handle.handle_id, &view.name)?
        {
            let window_end = view_state
                .output
                .get("windowEnd")
                .and_then(Value::as_str)
                .and_then(|text| chrono::DateTime::parse_from_rfc3339(text).ok())
                .map(|value| value.with_timezone(&Utc));
            let Some(window_end) = window_end else {
                continue;
            };
            if !runtime_state.source_cursors.iter().all(|cursor| {
                cursor.last_closed_window_end.is_some_and(|closed| closed >= window_end)
            }) {
                continue;
            }
            let retention =
                ChronoDuration::seconds(i64::try_from(retention_seconds).unwrap_or(i64::MAX));
            let expires_at =
                window_end + allowed_lateness.unwrap_or_else(ChronoDuration::zero) + retention;
            if occurred_at >= expires_at {
                prunable.push((view_state, Some(window_end), Some(retention_seconds)));
            }
        }
    }
    Ok(prunable)
}

fn merge_partition_item(
    reducer_kind: &str,
    aggregate: &mut StreamJobPartitionItem,
    input_value: f64,
) -> Result<()> {
    match reducer_kind {
        STREAM_REDUCER_COUNT => {
            aggregate.value += 1.0;
            aggregate.count = aggregate.count.saturating_add(1);
        }
        STREAM_REDUCER_SUM => {
            aggregate.value += input_value;
            aggregate.count = aggregate.count.saturating_add(1);
        }
        STREAM_REDUCER_MIN => {
            aggregate.value = aggregate.value.min(input_value);
            aggregate.count = 1;
        }
        STREAM_REDUCER_MAX => {
            aggregate.value = aggregate.value.max(input_value);
            aggregate.count = 1;
        }
        STREAM_REDUCER_AVG => {
            aggregate.value += input_value;
            aggregate.count = aggregate.count.saturating_add(1);
        }
        STREAM_REDUCER_THRESHOLD => {
            aggregate.value = aggregate.value.max(input_value);
            aggregate.count = 1;
        }
        other => anyhow::bail!("stream reducer {other} is not implemented in local activation"),
    }
    Ok(())
}

fn merge_dataflow_item(
    reducer_kind: &str,
    aggregate: &mut crate::dataflow_engine::BoundedDataflowItem,
    input_value: f64,
) -> Result<()> {
    match reducer_kind {
        STREAM_REDUCER_COUNT => {
            aggregate.value += 1.0;
            aggregate.count = aggregate.count.saturating_add(1);
        }
        STREAM_REDUCER_SUM => {
            aggregate.value += input_value;
            aggregate.count = aggregate.count.saturating_add(1);
        }
        STREAM_REDUCER_MIN => {
            aggregate.value = aggregate.value.min(input_value);
            aggregate.count = 1;
        }
        STREAM_REDUCER_MAX => {
            aggregate.value = aggregate.value.max(input_value);
            aggregate.count = 1;
        }
        STREAM_REDUCER_AVG => {
            aggregate.value += input_value;
            aggregate.count = aggregate.count.saturating_add(1);
        }
        STREAM_REDUCER_THRESHOLD => {
            aggregate.value = aggregate.value.max(input_value);
            aggregate.count = 1;
        }
        other => anyhow::bail!("stream reducer {other} is not implemented in local activation"),
    }
    Ok(())
}

pub(crate) fn signal_output_matches(output: &Value, when_output_field: Option<&str>) -> bool {
    crate::dataflow_engine::signal_output_matches(output, when_output_field)
}

pub(crate) fn sanitize_stream_view_output(output: Value) -> Value {
    crate::dataflow_engine::sanitize_stream_view_output(output)
}

pub(crate) type StreamJobViewAccumulator = crate::dataflow_engine::StreamJobViewAccumulator;

pub(crate) fn load_stream_job_view_accumulator(
    local_state: &LocalThroughputState,
    handle_id: &str,
    view_name: &str,
    reducer_kind: &str,
    output_field: &str,
    key_field: &str,
    logical_key: &str,
) -> Result<Option<(Map<String, Value>, StreamJobViewAccumulator)>> {
    crate::dataflow_engine::load_stream_job_view_accumulator(
        local_state,
        handle_id,
        view_name,
        reducer_kind,
        output_field,
        key_field,
        logical_key,
    )
}

pub(crate) type StreamJobMaterializedBatch = crate::dataflow_engine::StreamJobMaterializedBatch;

pub(crate) fn materialize_stream_job_batch_updates(
    handle: &StreamJobBridgeHandleRecord,
    program: &crate::dataflow_engine::BoundedDataflowBatchProgram,
    updated_outputs: Vec<(String, StreamJobViewAccumulator)>,
    mirror_owner_state: bool,
) -> StreamJobMaterializedBatch {
    crate::dataflow_engine::materialize_stream_job_batch_updates(
        handle,
        program,
        updated_outputs,
        mirror_owner_state,
    )
}

fn apply_stream_job_batch_program_in_memory(
    accumulators: &mut HashMap<String, StreamJobViewAccumulator>,
    program: &crate::dataflow_engine::BoundedDataflowBatchProgram,
) -> Result<Vec<(String, StreamJobViewAccumulator)>> {
    crate::dataflow_engine::apply_stream_job_batch_program_in_memory(accumulators, program)
}

fn bounded_aggregate_v2_kernel_subset(
    job: &CompiledStreamJob,
    kernel: &CompiledAggregateV2Kernel,
) -> Result<BoundedStreamAggregationPlan> {
    if kernel.source_kind != STREAM_SOURCE_BOUNDED_INPUT
        && kernel.source_kind != STREAM_SOURCE_TOPIC
    {
        anyhow::bail!(
            "stream runtime {} local activation only supports bounded_input and topic sources",
            STREAM_RUNTIME_AGGREGATE_V2
        );
    }
    if kernel.aggregates.len() != 1 {
        anyhow::bail!(
            "stream runtime {} local activation requires exactly 1 aggregate operator",
            STREAM_RUNTIME_AGGREGATE_V2
        );
    }
    if kernel.materialized_views.is_empty() {
        anyhow::bail!(
            "stream runtime {} local activation requires at least 1 materialized view",
            STREAM_RUNTIME_AGGREGATE_V2
        );
    }
    if kernel.checkpoints.len() > 1 {
        anyhow::bail!(
            "stream runtime {} local activation supports at most 1 checkpoint",
            STREAM_RUNTIME_AGGREGATE_V2
        );
    }

    let aggregate = &kernel.aggregates[0];
    match aggregate.reducer.as_str() {
        STREAM_REDUCER_COUNT
        | STREAM_REDUCER_SUM
        | STREAM_REDUCER_MIN
        | STREAM_REDUCER_MAX
        | STREAM_REDUCER_AVG
        | STREAM_REDUCER_THRESHOLD => {}
        STREAM_REDUCER_HISTOGRAM => {
            anyhow::bail!(
                "stream runtime {} local activation does not support histogram reducers yet",
                STREAM_RUNTIME_AGGREGATE_V2
            )
        }
        other => {
            anyhow::bail!(
                "stream runtime {} local activation does not support reducer {} yet",
                STREAM_RUNTIME_AGGREGATE_V2,
                other
            )
        }
    }
    if aggregate.reducer != STREAM_REDUCER_COUNT && aggregate.value_field.is_none() {
        anyhow::bail!(
            "stream runtime {} reducer {} requires valueField",
            STREAM_RUNTIME_AGGREGATE_V2,
            aggregate.reducer
        );
    }
    let threshold_comparison = StreamThresholdComparison::parse(aggregate.comparison.as_deref())?;
    let threshold = match aggregate.reducer.as_str() {
        STREAM_REDUCER_THRESHOLD => Some(
            aggregate.threshold.as_ref().and_then(serde_json::Number::as_f64).context(format!(
                "stream runtime {} threshold reducer requires a finite numeric threshold",
                STREAM_RUNTIME_AGGREGATE_V2
            ))?,
        ),
        _ => None,
    };

    let mut view_names = Vec::with_capacity(kernel.materialized_views.len());
    let mut view_retention_seconds = None;
    for materialized_view in &kernel.materialized_views {
        if materialized_view.query_mode != STREAM_QUERY_MODE_BY_KEY {
            anyhow::bail!(
                "stream runtime {} local activation requires query_mode=by_key",
                STREAM_RUNTIME_AGGREGATE_V2
            );
        }
        if !materialized_view
            .supported_consistencies
            .iter()
            .any(|consistency| consistency == STREAM_CONSISTENCY_STRONG)
        {
            anyhow::bail!(
                "stream runtime {} local activation requires strong materialized view reads",
                STREAM_RUNTIME_AGGREGATE_V2
            );
        }
        let view = job
            .views
            .iter()
            .find(|candidate| candidate.name == materialized_view.view_name)
            .with_context(|| {
                format!(
                    "stream runtime {} materialized view {} must be declared",
                    STREAM_RUNTIME_AGGREGATE_V2, materialized_view.view_name
                )
            })?;
        if view.key_field.as_deref() != Some(kernel.key_field.as_str()) {
            anyhow::bail!(
                "stream runtime {} local activation requires view.key_field to match key_by",
                STREAM_RUNTIME_AGGREGATE_V2
            );
        }
        if let Some(existing_retention) = view_retention_seconds {
            if existing_retention != view.retention_seconds {
                anyhow::bail!(
                    "stream runtime {} local activation requires all materialized views to share the same retention",
                    STREAM_RUNTIME_AGGREGATE_V2
                );
            }
        } else {
            view_retention_seconds = Some(view.retention_seconds);
        }
        view_names.push(materialized_view.view_name.clone());
    }
    let workflow_signals = kernel
        .workflow_signals
        .iter()
        .map(|signal| {
            if !view_names.iter().any(|view_name| view_name == &signal.view_name) {
                anyhow::bail!(
                    "stream runtime {} signal_workflow view {} must also be materialized",
                    STREAM_RUNTIME_AGGREGATE_V2,
                    signal.view_name
                );
            }
            Ok(StreamJobWorkflowSignalPlan {
                operator_id: signal.operator_id.clone(),
                view_name: signal.view_name.clone(),
                signal_type: signal.signal_type.clone(),
                when_output_field: signal.when_output_field.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let primary_view_name = view_names
        .first()
        .cloned()
        .context("stream runtime aggregate_v2 local activation requires a primary view")?;
    let (window_time_field, window_mode, window_size, window_hop, allowed_lateness) = if let Some(
        window,
    ) =
        &kernel.window
    {
        let window_mode = StreamWindowMode::parse(&window.mode).with_context(|| {
            format!(
                "stream runtime {} local activation requires a supported window mode",
                STREAM_RUNTIME_AGGREGATE_V2
            )
        })?;
        let window_size = parse_window_size(&window.size).with_context(|| {
            format!(
                "stream runtime {} local activation requires a valid window size",
                STREAM_RUNTIME_AGGREGATE_V2
            )
        })?;
        let window_hop = match window_mode {
                StreamWindowMode::Tumbling => None,
                StreamWindowMode::Hopping => Some(
                    window
                        .hop
                        .as_deref()
                        .context(format!(
                            "stream runtime {} hopping windows require hop",
                            STREAM_RUNTIME_AGGREGATE_V2
                        ))
                        .and_then(parse_window_size)
                        .with_context(|| {
                            format!(
                                "stream runtime {} local activation requires a valid hopping window hop",
                                STREAM_RUNTIME_AGGREGATE_V2
                            )
                        })?,
                ),
            };
        if let Some(hop) = window_hop {
            if window_size.num_seconds() % hop.num_seconds() != 0 {
                anyhow::bail!(
                    "stream runtime {} hopping window size must be an exact multiple of hop",
                    STREAM_RUNTIME_AGGREGATE_V2
                );
            }
            if hop > window_size {
                anyhow::bail!(
                    "stream runtime {} hopping window hop must be less than or equal to size",
                    STREAM_RUNTIME_AGGREGATE_V2
                );
            }
        }
        (
            window.time_field.clone(),
            Some(window_mode),
            Some(window_size),
            window_hop,
            window.allowed_lateness.as_deref().map(parse_window_size).transpose().with_context(
                || {
                    format!(
                        "stream runtime {} local activation requires a valid allowed lateness",
                        STREAM_RUNTIME_AGGREGATE_V2
                    )
                },
            )?,
        )
    } else {
        (None, None, None, None, None)
    };

    let checkpoint = kernel.checkpoints.first();
    let mut pre_key_operators = Vec::new();
    for operator in &kernel.pre_key_operators {
        match operator {
            CompiledAggregateV2PreKeyOperator::Map(map) => {
                pre_key_operators.push(StreamJobPreKeyOperator::Map(StreamJobMapTransform {
                    operator_id: map.operator_id.clone(),
                    input_field: map.input_field.clone(),
                    output_field: map.output_field.clone(),
                    multiply_by: map.multiply_by.as_ref().and_then(serde_json::Number::as_f64),
                    add: map.add.as_ref().and_then(serde_json::Number::as_f64),
                }));
            }
            CompiledAggregateV2PreKeyOperator::Filter(filter) => {
                pre_key_operators.push(StreamJobPreKeyOperator::Filter(
                    parse_stream_job_filter_predicate(&filter.operator_id, &filter.predicate)?,
                ));
            }
            CompiledAggregateV2PreKeyOperator::Route(route) => {
                let branches = route
                    .branches
                    .iter()
                    .map(|branch| {
                        Ok(StreamJobRouteBranch {
                            predicate: parse_stream_job_filter_predicate(
                                &route.operator_id,
                                &branch.predicate,
                            )?,
                            value: branch.value.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                pre_key_operators.push(StreamJobPreKeyOperator::Route(StreamJobRouteTransform {
                    operator_id: route.operator_id.clone(),
                    output_field: route.output_field.clone(),
                    branches,
                    default_value: route.default_value.clone(),
                }));
            }
        }
    }
    let eventual_projection_view_names = view_names
        .iter()
        .filter(|view_name| {
            job.views
                .iter()
                .find(|view| view.name == view_name.as_str())
                .is_some_and(view_supports_eventual_reads)
        })
        .cloned()
        .collect();
    Ok(BoundedStreamAggregationPlan {
        key_field: kernel.key_field.clone(),
        reducer_kind: aggregate.reducer.clone(),
        value_field: aggregate.value_field.clone(),
        output_field: aggregate.output_field.clone().unwrap_or_else(|| "value".to_owned()),
        pre_key_operators,
        threshold,
        threshold_comparison,
        view_name: primary_view_name,
        additional_view_names: view_names.into_iter().skip(1).collect(),
        eventual_projection_view_names,
        workflow_signals,
        view_retention_seconds: view_retention_seconds.flatten(),
        window_time_field,
        window_mode,
        window_size,
        window_hop,
        allowed_lateness,
        checkpoint_name: checkpoint.map(|value| value.name.clone()).unwrap_or_default(),
        checkpoint_sequence: checkpoint.map(|value| value.sequence).unwrap_or_default(),
    })
}

fn terminal_output(handle: &StreamJobBridgeHandleRecord) -> Value {
    json!({
        "jobId": handle.job_id,
        "jobName": handle.job_name,
        "status": "completed",
    })
}

pub(crate) fn source_owner_partition_for_source_partition(
    source_partition_id: i32,
    throughput_partitions: i32,
) -> i32 {
    if throughput_partitions <= 0 {
        0
    } else {
        source_partition_id.rem_euclid(throughput_partitions)
    }
}

pub(crate) fn source_partition_lease_token(
    handle: &StreamJobBridgeHandleRecord,
    source_partition_id: i32,
    owner_partition_id: i32,
    owner_epoch: u64,
) -> String {
    format!(
        "{}:source-lease:{source_partition_id}:{owner_partition_id}:{owner_epoch}",
        handle.handle_id
    )
}

fn execution_planned_changelog_record(
    handle: &StreamJobBridgeHandleRecord,
    view_name: &str,
    checkpoint_name: &str,
    checkpoint_sequence: i64,
    owner_epoch: u64,
    input_item_count: u64,
    materialized_key_count: u64,
    active_partitions: Vec<i32>,
    throughput_partition_count: i32,
    planned_at: DateTime<Utc>,
) -> StreamChangelogRecord {
    let key = throughput_partition_key(&handle.job_id, 0);
    let entry = StreamsChangelogEntry {
        entry_id: Uuid::now_v7(),
        occurred_at: planned_at,
        partition_key: key.clone(),
        payload: StreamsChangelogPayload::StreamJobExecutionPlanned {
            handle_id: handle.handle_id.clone(),
            job_id: handle.job_id.clone(),
            job_name: handle.job_name.clone(),
            view_name: view_name.to_owned(),
            checkpoint_name: checkpoint_name.to_owned(),
            checkpoint_sequence,
            input_item_count,
            materialized_key_count,
            active_partitions,
            throughput_partition_count,
            owner_epoch,
            planned_at,
        },
    };
    StreamChangelogRecord::streams(key, entry)
}

fn view_batch_update_changelog_record(
    handle: &StreamJobBridgeHandleRecord,
    routing_key: &str,
    updates: Vec<StreamJobViewBatchUpdate>,
    checkpoint_sequence: i64,
    updated_at: DateTime<Utc>,
) -> StreamChangelogRecord {
    crate::dataflow_engine::view_batch_update_changelog_record(
        handle,
        routing_key,
        updates,
        checkpoint_sequence,
        updated_at,
    )
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
    crate::dataflow_engine::view_update_changelog_record(
        handle,
        view_name,
        routing_key,
        logical_key,
        output,
        checkpoint_sequence,
        updated_at,
    )
}

pub(crate) fn workflow_signal_payload(
    handle: &StreamJobBridgeHandleRecord,
    signal: &StreamJobWorkflowSignalPlan,
    logical_key: &str,
    output: Value,
) -> Value {
    crate::dataflow_engine::workflow_signal_payload(handle, signal, logical_key, output)
}

pub(crate) fn workflow_signaled_changelog_record(
    handle: &StreamJobBridgeHandleRecord,
    signal: &StreamJobWorkflowSignalPlan,
    logical_key: &str,
    payload: Value,
    owner_epoch: u64,
    signaled_at: DateTime<Utc>,
) -> StreamChangelogRecord {
    crate::dataflow_engine::workflow_signaled_changelog_record(
        handle,
        signal,
        logical_key,
        payload,
        owner_epoch,
        signaled_at,
    )
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
    crate::dataflow_engine::checkpoint_reached_changelog_record(
        handle,
        routing_key,
        checkpoint_name,
        checkpoint_sequence,
        stream_partition_id,
        owner_epoch,
        reached_at,
    )
}

fn source_lease_assigned_changelog_record(
    handle: &StreamJobBridgeHandleRecord,
    source_partition_id: i32,
    owner_partition_id: i32,
    owner_epoch: u64,
    lease_token: &str,
    assigned_at: DateTime<Utc>,
) -> StreamChangelogRecord {
    let key = throughput_partition_key(&handle.job_id, 0);
    let entry = StreamsChangelogEntry {
        entry_id: Uuid::now_v7(),
        occurred_at: assigned_at,
        partition_key: key.clone(),
        payload: StreamsChangelogPayload::StreamJobSourceLeaseAssigned {
            handle_id: handle.handle_id.clone(),
            job_id: handle.job_id.clone(),
            source_partition_id,
            owner_partition_id,
            owner_epoch,
            lease_token: lease_token.to_owned(),
            assigned_at,
        },
    };
    StreamChangelogRecord::streams(key, entry)
}

fn source_progressed_changelog_record(
    handle: &StreamJobBridgeHandleRecord,
    source_partition_id: i32,
    next_offset: i64,
    checkpoint_sequence: i64,
    checkpoint_target_offset: i64,
    last_applied_offset: Option<i64>,
    last_high_watermark: Option<i64>,
    last_event_time_watermark: Option<DateTime<Utc>>,
    last_closed_window_end: Option<DateTime<Utc>>,
    pending_window_ends: Vec<DateTime<Utc>>,
    dropped_late_event_count: u64,
    last_dropped_late_offset: Option<i64>,
    last_dropped_late_event_at: Option<DateTime<Utc>>,
    last_dropped_late_window_end: Option<DateTime<Utc>>,
    dropped_evicted_window_event_count: u64,
    last_dropped_evicted_window_offset: Option<i64>,
    last_dropped_evicted_window_event_at: Option<DateTime<Utc>>,
    last_dropped_evicted_window_end: Option<DateTime<Utc>>,
    source_owner_partition_id: i32,
    lease_token: &str,
    owner_epoch: u64,
    progressed_at: DateTime<Utc>,
) -> StreamChangelogRecord {
    let key = throughput_partition_key(&handle.job_id, 0);
    let entry = StreamsChangelogEntry {
        entry_id: Uuid::now_v7(),
        occurred_at: progressed_at,
        partition_key: key.clone(),
        payload: StreamsChangelogPayload::StreamJobSourceProgressed {
            handle_id: handle.handle_id.clone(),
            job_id: handle.job_id.clone(),
            source_partition_id,
            next_offset,
            checkpoint_sequence,
            checkpoint_target_offset,
            last_applied_offset,
            last_high_watermark,
            last_event_time_watermark,
            last_closed_window_end,
            pending_window_ends,
            dropped_late_event_count,
            last_dropped_late_offset,
            last_dropped_late_event_at,
            last_dropped_late_window_end,
            dropped_evicted_window_event_count,
            last_dropped_evicted_window_offset,
            last_dropped_evicted_window_event_at,
            last_dropped_evicted_window_end,
            source_owner_partition_id,
            lease_token: lease_token.to_owned(),
            owner_epoch,
            progressed_at,
        },
    };
    StreamChangelogRecord::streams(key, entry)
}

fn view_evicted_changelog_record(
    handle: &StreamJobBridgeHandleRecord,
    view_name: &str,
    logical_key: &str,
    checkpoint_sequence: i64,
    window_end: Option<DateTime<Utc>>,
    retention_seconds: Option<u64>,
    evicted_window_count: u64,
    last_evicted_window_end: Option<DateTime<Utc>>,
    last_evicted_at: Option<DateTime<Utc>>,
    evicted_at: DateTime<Utc>,
) -> StreamChangelogRecord {
    let key = throughput_partition_key(&handle.job_id, 0);
    let entry = StreamsChangelogEntry {
        entry_id: Uuid::now_v7(),
        occurred_at: evicted_at,
        partition_key: key.clone(),
        payload: StreamsChangelogPayload::StreamJobViewEvicted {
            handle_id: handle.handle_id.clone(),
            job_id: handle.job_id.clone(),
            view_name: view_name.to_owned(),
            logical_key: logical_key.to_owned(),
            checkpoint_sequence,
            window_end,
            retention_seconds,
            evicted_window_count,
            last_evicted_window_end,
            last_evicted_at,
            evicted_at,
        },
    };
    StreamChangelogRecord::streams(key, entry)
}

fn delete_view_projection_record(
    handle: &StreamJobBridgeHandleRecord,
    view_name: &str,
    logical_key: &str,
    checkpoint_sequence: i64,
    evicted_at: DateTime<Utc>,
) -> StreamProjectionRecord {
    StreamProjectionRecord::streams(
        throughput_partition_key(logical_key, 0),
        StreamsProjectionEvent::DeleteStreamJobView {
            view: StreamsViewDeleteRecord {
                tenant_id: handle.tenant_id.clone(),
                instance_id: handle.instance_id.clone(),
                run_id: handle.run_id.clone(),
                job_id: handle.job_id.clone(),
                handle_id: handle.handle_id.clone(),
                view_name: view_name.to_owned(),
                logical_key: logical_key.to_owned(),
                checkpoint_sequence,
                evicted_at,
            },
        },
    )
}

fn terminalized_changelog_record(
    handle: &StreamJobBridgeHandleRecord,
    owner_epoch: u64,
    terminal_at: DateTime<Utc>,
) -> StreamChangelogRecord {
    terminalized_status_changelog_record(
        handle,
        owner_epoch,
        StreamJobBridgeHandleStatus::Completed,
        Some(terminal_output(handle)),
        None,
        terminal_at,
    )
}

fn terminalized_status_changelog_record(
    handle: &StreamJobBridgeHandleRecord,
    owner_epoch: u64,
    status: StreamJobBridgeHandleStatus,
    output: Option<Value>,
    error: Option<String>,
    terminal_at: DateTime<Utc>,
) -> StreamChangelogRecord {
    let key = throughput_partition_key(&handle.job_id, 0);
    let entry = StreamsChangelogEntry {
        entry_id: Uuid::now_v7(),
        occurred_at: terminal_at,
        partition_key: key.clone(),
        payload: StreamsChangelogPayload::StreamJobTerminalized {
            handle_id: handle.handle_id.clone(),
            job_id: handle.job_id.clone(),
            owner_epoch,
            status: status.as_str().to_owned(),
            output,
            error,
            terminal_at,
        },
    };
    StreamChangelogRecord::streams(key, entry)
}

fn dispatch_batch_id(
    handle: &StreamJobBridgeHandleRecord,
    stream_partition_id: i32,
    batch_index: usize,
) -> String {
    format!("{}:{stream_partition_id}:{batch_index}", handle.handle_id)
}

pub(crate) fn local_dispatch_batch_from_dataflow_batch(
    batch: &StreamJobDataflowBatch,
) -> LocalStreamJobDispatchBatch {
    LocalStreamJobDispatchBatch {
        batch_id: batch.envelope.batch_id.clone(),
        plan_id: Some(batch.envelope.plan_id.clone()),
        source_id: Some(batch.envelope.source_id.clone()),
        source_partition_id: Some(batch.envelope.source_partition_id),
        source_epoch: Some(batch.envelope.source_epoch),
        offset_range: batch.envelope.offset_range.clone(),
        edge_id: Some(batch.envelope.edge_id.clone()),
        lease_epoch: batch.envelope.lease_epoch,
        summary: Some(batch.envelope.summary.clone()),
        stream_partition_id: batch.program.stream_partition_id,
        checkpoint_partition_id: batch.program.checkpoint_partition_id,
        source_owner_partition_id: batch.program.source_owner_partition_id,
        source_lease_token: batch.program.source_lease_token.clone(),
        routing_key: batch.program.routing_key.clone(),
        key_field: batch.program.key_field.clone(),
        reducer_kind: batch.program.reducer_kind.clone(),
        output_field: batch.program.output_field.clone(),
        view_name: batch.program.view_name.clone(),
        additional_view_names: batch.program.additional_view_names.clone(),
        workflow_signals: batch.program.workflow_signals.clone(),
        eventual_projection_view_names: batch.program.eventual_projection_view_names.clone(),
        checkpoint_name: batch.program.checkpoint_name.clone(),
        checkpoint_sequence: batch.program.checkpoint_sequence,
        owner_epoch: batch.program.owner_epoch,
        occurred_at: batch.program.occurred_at,
        items: batch
            .program
            .items
            .iter()
            .map(|item| LocalStreamJobDispatchItem {
                logical_key: item.logical_key.clone(),
                value: item.value,
                display_key: item.display_key.clone(),
                window_start: item.window_start.clone(),
                window_end: item.window_end.clone(),
                count: item.count,
            })
            .collect(),
        is_final_partition_batch: batch.program.is_final_partition_batch,
        completes_dispatch: batch.program.completes_dispatch,
    }
}

fn program_from_local_dispatch_batch(
    batch: &LocalStreamJobDispatchBatch,
) -> crate::dataflow_engine::BoundedDataflowBatchProgram {
    crate::dataflow_engine::BoundedDataflowBatchProgram {
        stream_partition_id: batch.stream_partition_id,
        checkpoint_partition_id: batch.checkpoint_partition_id,
        source_owner_partition_id: batch.source_owner_partition_id,
        source_lease_token: batch.source_lease_token.clone(),
        routing_key: batch.routing_key.clone(),
        key_field: batch.key_field.clone(),
        reducer_kind: batch.reducer_kind.clone(),
        output_field: batch.output_field.clone(),
        view_name: batch.view_name.clone(),
        additional_view_names: batch.additional_view_names.clone(),
        eventual_projection_view_names: batch.eventual_projection_view_names.clone(),
        workflow_signals: batch.workflow_signals.clone(),
        checkpoint_name: batch.checkpoint_name.clone(),
        checkpoint_sequence: batch.checkpoint_sequence,
        owner_epoch: batch.owner_epoch,
        occurred_at: batch.occurred_at,
        items: batch
            .items
            .iter()
            .map(|item| crate::dataflow_engine::BoundedDataflowItem {
                logical_key: item.logical_key.clone(),
                value: item.value,
                display_key: item.display_key.clone(),
                window_start: item.window_start.clone(),
                window_end: item.window_end.clone(),
                count: item.count,
            })
            .collect(),
        is_final_partition_batch: batch.is_final_partition_batch,
        completes_dispatch: batch.completes_dispatch,
    }
}

fn dataflow_batch_from_local_dispatch_batch(
    handle: &StreamJobBridgeHandleRecord,
    batch: &LocalStreamJobDispatchBatch,
    runtime_state: Option<&LocalStreamJobRuntimeState>,
) -> StreamJobDataflowBatch {
    let program = program_from_local_dispatch_batch(batch);
    let mut restored =
        direct_dataflow_batch_for_program(handle, batch.batch_id.clone(), program, runtime_state);
    if let Some(plan_id) = &batch.plan_id {
        restored.envelope.plan_id = plan_id.clone();
    }
    if let Some(source_id) = &batch.source_id {
        restored.envelope.source_id = source_id.clone();
    }
    if let Some(source_partition_id) = batch.source_partition_id {
        restored.envelope.source_partition_id = source_partition_id;
    }
    if let Some(source_epoch) = batch.source_epoch {
        restored.envelope.source_epoch = source_epoch;
    }
    if let Some(offset_range) = &batch.offset_range {
        restored.envelope.offset_range = Some(offset_range.clone());
    }
    if let Some(edge_id) = &batch.edge_id {
        restored.envelope.edge_id = edge_id.clone();
    }
    if let Some(lease_epoch) = batch.lease_epoch {
        restored.envelope.lease_epoch = Some(lease_epoch);
    }
    if let Some(summary) = &batch.summary {
        restored.envelope.summary = summary.clone();
    }
    restored
}

async fn plan_topic_stream_job_activation(
    local_state: &LocalThroughputState,
    app_state: &AppState,
    handle: &StreamJobBridgeHandleRecord,
    plan: &BoundedStreamAggregationPlan,
    topic: &TopicSourceSpec,
    throughput_partitions: i32,
    owner_epoch: u64,
    occurred_at: DateTime<Utc>,
) -> Result<StreamJobActivationPlan> {
    let topic_config = JsonTopicConfig::new(
        app_state.json_brokers.clone(),
        &topic.topic_name,
        throughput_partitions,
    );
    let existing_runtime_state = local_state.load_stream_job_runtime_state(&handle.handle_id)?;
    if existing_runtime_state.as_ref().and_then(|state| state.terminal_status.as_deref()).is_some()
    {
        return Ok(StreamJobActivationPlan {
            execution_planned: None,
            runtime_state_update: None,
            dataflow_batches: Vec::new(),
            post_apply_projection_records: Vec::new(),
            post_apply_changelog_records: Vec::new(),
            source_cursor_updates: None,
            terminalized: None,
        });
    }

    let refresh_frontier =
        should_refresh_topic_frontier(existing_runtime_state.as_ref(), occurred_at);
    let latest_offsets = if existing_runtime_state.is_none() || refresh_frontier {
        load_json_topic_latest_offsets(
            &topic_config,
            &format!("streams-runtime-{}", handle.handle_id),
        )
        .await?
    } else {
        Vec::new()
    };
    let latest_offsets_by_partition = if latest_offsets.is_empty() {
        existing_runtime_state
            .as_ref()
            .map(topic_frontier_offsets_from_runtime_state)
            .unwrap_or_default()
    } else {
        latest_offsets
            .iter()
            .map(|offset| (offset.partition_id, offset.latest_offset))
            .collect::<HashMap<_, _>>()
    };

    let (execution_planned, mut runtime_state_update, mut source_cursors) =
        if let Some(existing_runtime_state) = existing_runtime_state.as_ref() {
            (None, None, existing_runtime_state.source_cursors.clone())
        } else {
            let mut active_partitions = Vec::with_capacity(latest_offsets.len());
            let mut source_cursors = Vec::new();
            for partition in latest_offsets.iter() {
                active_partitions.push(partition.partition_id);
                let next_offset =
                    topic.explicit_offsets.get(&partition.partition_id).copied().unwrap_or_else(
                        || {
                            if topic.start_from_latest { partition.latest_offset } else { 0 }
                        },
                    );
                source_cursors.push(LocalStreamJobSourceCursorState {
                    source_partition_id: partition.partition_id,
                    next_offset,
                    initial_checkpoint_target_offset: partition.latest_offset,
                    last_applied_offset: None,
                    last_high_watermark: Some(partition.latest_offset),
                    last_event_time_watermark: None,
                    last_closed_window_end: None,
                    pending_window_ends: Vec::new(),
                    dropped_late_event_count: 0,
                    last_dropped_late_offset: None,
                    last_dropped_late_event_at: None,
                    last_dropped_late_window_end: None,
                    dropped_evicted_window_event_count: 0,
                    last_dropped_evicted_window_offset: None,
                    last_dropped_evicted_window_event_at: None,
                    last_dropped_evicted_window_end: None,
                    checkpoint_reached_at: None,
                    updated_at: occurred_at,
                });
            }
            active_partitions.sort_unstable();
            let runtime_state = LocalStreamJobRuntimeState {
                handle_id: handle.handle_id.clone(),
                job_id: handle.job_id.clone(),
                job_name: handle.job_name.clone(),
                view_name: plan.view_name.clone(),
                checkpoint_name: plan.checkpoint_name.clone(),
                checkpoint_sequence: plan.checkpoint_sequence,
                input_item_count: 0,
                materialized_key_count: 0,
                active_partitions: active_partitions.clone(),
                throughput_partition_count: throughput_partitions,
                source_kind: Some(STREAM_SOURCE_TOPIC.to_owned()),
                source_name: Some(topic.topic_name.clone()),
                source_cursors: Vec::new(),
                source_partition_leases: Vec::new(),
                dispatch_dataflow_batches: Vec::new(),
                applied_dispatch_batch_ids: Vec::new(),
                dispatch_completed_at: None,
                dispatch_cancelled_at: None,
                stream_owner_epoch: owner_epoch,
                planned_at: occurred_at,
                latest_checkpoint_at: None,
                evicted_window_count: 0,
                last_evicted_window_end: None,
                last_evicted_at: None,
                view_runtime_stats: Vec::new(),
                pre_key_runtime_stats: initial_pre_key_runtime_stats(&plan.pre_key_operators),
                hot_key_runtime_stats: Vec::new(),
                owner_partition_runtime_stats: Vec::new(),
                checkpoint_partitions: Vec::new(),
                terminal_status: None,
                terminal_output: None,
                terminal_error: None,
                terminal_at: None,
                updated_at: occurred_at,
            };
            (
                Some(execution_planned_changelog_record(
                    handle,
                    &plan.view_name,
                    &plan.checkpoint_name,
                    plan.checkpoint_sequence,
                    owner_epoch,
                    0,
                    0,
                    active_partitions,
                    throughput_partitions,
                    occurred_at,
                )),
                Some(runtime_state),
                source_cursors,
            )
        };
    source_cursors.sort_by_key(|cursor| cursor.source_partition_id);
    let source_leases = source_cursors
        .iter()
        .map(|cursor| {
            let owner_partition_id = source_owner_partition_for_source_partition(
                cursor.source_partition_id,
                throughput_partitions,
            );
            let lease_token = source_partition_lease_token(
                handle,
                cursor.source_partition_id,
                owner_partition_id,
                owner_epoch,
            );
            (cursor.source_partition_id, (owner_partition_id, lease_token))
        })
        .collect::<HashMap<_, _>>();
    let mut source_lease_records = Vec::new();
    for cursor in &source_cursors {
        let Some((owner_partition_id, lease_token)) =
            source_leases.get(&cursor.source_partition_id)
        else {
            continue;
        };
        let lease_changed = existing_runtime_state
            .as_ref()
            .and_then(|state| {
                state
                    .source_partition_leases
                    .iter()
                    .find(|lease| lease.source_partition_id == cursor.source_partition_id)
            })
            .is_none_or(|existing| {
                existing.owner_partition_id != *owner_partition_id
                    || existing.owner_epoch != owner_epoch
                    || existing.lease_token != *lease_token
            });
        if lease_changed {
            source_lease_records.push(source_lease_assigned_changelog_record(
                handle,
                cursor.source_partition_id,
                *owner_partition_id,
                owner_epoch,
                lease_token,
                occurred_at,
            ));
        }
    }
    let mut lease_states = source_leases
        .iter()
        .map(|(source_partition_id, (owner_partition_id, lease_token))| {
            LocalStreamJobSourceLeaseState {
                source_partition_id: *source_partition_id,
                owner_partition_id: *owner_partition_id,
                owner_epoch,
                lease_token: lease_token.clone(),
                updated_at: occurred_at,
            }
        })
        .collect::<Vec<_>>();
    lease_states.sort_by_key(|lease| lease.source_partition_id);
    let mut state_for_apply = runtime_state_update
        .clone()
        .or_else(|| existing_runtime_state.clone())
        .context("topic stream job runtime state must exist before applying partition work")?;
    state_for_apply.source_partition_leases = lease_states.clone();
    state_for_apply.updated_at = occurred_at;
    runtime_state_update = Some(state_for_apply);

    if handle.cancellation_requested_at.is_some() {
        let mut post_apply_changelog_records =
            Vec::with_capacity(source_cursors.len() + source_lease_records.len());
        post_apply_changelog_records.extend(source_lease_records);
        for cursor in &source_cursors {
            let (source_owner_partition_id, source_lease_token) =
                source_leases.get(&cursor.source_partition_id).context(
                    "topic stream job source lease metadata must exist for every source cursor",
                )?;
            post_apply_changelog_records.push(source_progressed_changelog_record(
                handle,
                cursor.source_partition_id,
                cursor.next_offset,
                plan.checkpoint_sequence,
                cursor.initial_checkpoint_target_offset,
                cursor.last_applied_offset,
                cursor.last_high_watermark,
                cursor.last_event_time_watermark,
                cursor.last_closed_window_end,
                cursor.pending_window_ends.clone(),
                cursor.dropped_late_event_count,
                cursor.last_dropped_late_offset,
                cursor.last_dropped_late_event_at,
                cursor.last_dropped_late_window_end,
                cursor.dropped_evicted_window_event_count,
                cursor.last_dropped_evicted_window_offset,
                cursor.last_dropped_evicted_window_event_at,
                cursor.last_dropped_evicted_window_end,
                *source_owner_partition_id,
                source_lease_token,
                owner_epoch,
                occurred_at,
            ));
        }
        return Ok(StreamJobActivationPlan {
            execution_planned,
            runtime_state_update,
            dataflow_batches: Vec::new(),
            post_apply_projection_records: Vec::new(),
            post_apply_changelog_records,
            source_cursor_updates: None,
            terminalized: Some(terminalized_status_changelog_record(
                handle,
                owner_epoch,
                StreamJobBridgeHandleStatus::Cancelled,
                None,
                Some(
                    handle
                        .cancellation_reason
                        .clone()
                        .unwrap_or_else(|| "stream job cancelled".to_owned()),
                ),
                handle.cancellation_requested_at.unwrap_or(occurred_at),
            )),
        });
    }

    let partitions =
        source_cursors.iter().map(|cursor| cursor.source_partition_id).collect::<Vec<_>>();
    let start_offsets = source_cursors
        .iter()
        .map(|cursor| (cursor.source_partition_id, cursor.next_offset))
        .collect::<HashMap<_, _>>();

    debug_topic_activation_trace(
        &handle.handle_id,
        format!(
            "building topic consumer topic={} partitions={:?} start_offsets={:?}",
            topic_config.topic_name, partitions, start_offsets
        ),
    );
    let mut consumer = build_json_consumer_from_offsets(
        &topic_config,
        &format!("streams-runtime-source-{}", handle.handle_id),
        &start_offsets,
        &partitions,
    )
    .await?;
    debug_topic_activation_trace(
        &handle.handle_id,
        format!("topic consumer ready topic={}", topic_config.topic_name),
    );
    let mut pre_key_runtime_stats = initial_pre_key_runtime_stats(&plan.pre_key_operators);
    let mut consumed = BTreeMap::<i32, Vec<TopicSourceBatchRecord>>::new();
    while consumed.values().map(Vec::len).sum::<usize>() < STREAM_JOB_TOPIC_POLL_MAX_RECORDS {
        let next =
            timeout(Duration::from_millis(STREAM_JOB_TOPIC_IDLE_WAIT_MS), consumer.next()).await;
        let Some(record) = (match next {
            Ok(Some(record)) => Some(record?),
            Ok(None) | Err(_) => None,
        }) else {
            break;
        };
        let mut item: Value = decode_json_record(&record.record)
            .context("failed to decode topic-backed stream job source record")?;
        let pre_key_result = apply_stream_job_pre_key_operators(
            &mut item,
            &plan.pre_key_operators,
            &mut pre_key_runtime_stats,
        );
        let pre_key_applied = match pre_key_result {
            Ok(applied) => applied,
            Err(error) => {
                let mut failure_runtime_state = runtime_state_update
                    .clone()
                    .or_else(|| existing_runtime_state.clone())
                    .context("topic stream job runtime state must exist on pre-key failure")?;
                if !pre_key_runtime_stats.is_empty() {
                    failure_runtime_state.merge_pre_key_runtime_stats(&pre_key_runtime_stats);
                }
                failure_runtime_state.updated_at = occurred_at;
                local_state.upsert_stream_job_runtime_state(&failure_runtime_state)?;
                return Err(error);
            }
        };
        if !pre_key_applied {
            continue;
        }
        let logical_key = string_field_ref(&item, &plan.key_field)?.to_owned();
        let value = if plan.reducer_kind == STREAM_REDUCER_COUNT {
            1.0
        } else {
            threshold_value(
                plan,
                numeric_field(
                    &item,
                    plan.value_field.as_deref().context(format!(
                        "stream reducer {} requires valueField for topic sources",
                        plan.reducer_kind
                    ))?,
                )?,
            )
        };
        for assignment in windowed_item_assignments(&item, &logical_key, plan)? {
            consumed.entry(record.partition_id).or_default().push(TopicSourceBatchRecord {
                logical_key: assignment.logical_key,
                display_key: logical_key.clone(),
                window_start: assignment.window_start,
                window_end: assignment.window_end,
                event_time: assignment.event_time,
                window_end_at: assignment.window_end_at,
                value,
                offset: record.record.offset,
                high_watermark: record.high_watermark,
            });
        }
    }
    debug_topic_activation_trace(
        &handle.handle_id,
        format!(
            "topic poll finished consumed_records={} partitions_with_data={}",
            consumed.values().map(Vec::len).sum::<usize>(),
            consumed.len()
        ),
    );

    let mut planned_batches = Vec::new();
    for (source_partition_id, records) in &consumed {
        let cursor = source_cursors
            .iter()
            .find(|cursor| cursor.source_partition_id == *source_partition_id)
            .context("topic stream job source cursor must exist for consumed partition")?;
        let admitted = admit_windowed_source_records(records, cursor, plan, occurred_at)?;
        if admitted.accepted_records.is_empty() {
            continue;
        }
        let mut by_owner_partition =
            BTreeMap::<i32, HashMap<String, crate::dataflow_engine::BoundedDataflowItem>>::new();
        let mut routing_keys = HashMap::<i32, String>::new();
        let (source_owner_partition_id, source_lease_token) =
            source_leases.get(source_partition_id).cloned().context(
                "topic stream job source lease metadata must exist for every consumed partition",
            )?;
        for record in &admitted.accepted_records {
            let owner_partition_id =
                throughput_partition_for_stream_key(&record.display_key, throughput_partitions);
            routing_keys.entry(owner_partition_id).or_insert_with(|| record.logical_key.clone());
            let partition_items = by_owner_partition.entry(owner_partition_id).or_default();
            if let Some(aggregate) = partition_items.get_mut(&record.logical_key) {
                merge_dataflow_item(&plan.reducer_kind, aggregate, record.value)?;
            } else {
                partition_items.insert(
                    record.logical_key.clone(),
                    crate::dataflow_engine::BoundedDataflowItem {
                        logical_key: record.logical_key.clone(),
                        value: record.value,
                        display_key: Some(record.display_key.clone()),
                        window_start: record.window_start.clone(),
                        window_end: record.window_end.clone(),
                        count: 1,
                    },
                );
            }
        }
        let start_offset = records.first().map(|record| record.offset).unwrap_or(0);
        let end_offset = records.last().map(|record| record.offset).unwrap_or(start_offset);
        for (owner_partition_id, items_by_key) in by_owner_partition {
            let items = items_by_key.into_values().collect::<Vec<_>>();
            let routing_key =
                routing_keys.remove(&owner_partition_id).unwrap_or_else(|| handle.job_id.clone());
            planned_batches.push(PlannedBoundedDataflowBatch {
                batch_id: format!(
                    "{}:topic:{source_partition_id}:{start_offset}:{end_offset}:{owner_partition_id}",
                    handle.handle_id
                ),
                source_id: format!("topic:{}", handle.handle_id),
                source_partition_id: *source_partition_id,
                source_epoch: plan.checkpoint_sequence.max(0) as u64,
                offset_range: Some(DataflowOffsetRange { start: start_offset, end: end_offset }),
                program: crate::dataflow_engine::BoundedDataflowBatchProgram {
                    stream_partition_id: owner_partition_id,
                    checkpoint_partition_id: Some(*source_partition_id),
                    source_owner_partition_id: Some(source_owner_partition_id),
                    source_lease_token: Some(source_lease_token.clone()),
                    routing_key,
                    key_field: plan.key_field.clone(),
                    reducer_kind: plan.reducer_kind.clone(),
                    output_field: plan.output_field.clone(),
                    view_name: plan.view_name.clone(),
                    additional_view_names: plan.additional_view_names.clone(),
                    eventual_projection_view_names: plan.eventual_projection_view_names.clone(),
                    workflow_signals: plan.workflow_signals.clone(),
                    checkpoint_name: plan.checkpoint_name.clone(),
                    checkpoint_sequence: plan.checkpoint_sequence,
                    owner_epoch,
                    occurred_at,
                    items,
                    is_final_partition_batch: false,
                    completes_dispatch: false,
                },
            });
        }
    }

    let mut post_apply_projection_records = Vec::new();
    let mut post_apply_changelog_records = source_lease_records;
    for cursor in &mut source_cursors {
        let consumed_records = consumed.get(&cursor.source_partition_id);
        let (source_owner_partition_id, source_lease_token) = source_leases
            .get(&cursor.source_partition_id)
            .context("topic stream job source lease metadata must exist for every source cursor")?;
        let previous_target = cursor.initial_checkpoint_target_offset;
        let previous_reached_at = cursor.checkpoint_reached_at;
        let previous_closed_window_end = cursor.last_closed_window_end;
        if let Some(records) = consumed_records {
            let last_offset = records.last().map(|record| record.offset);
            let last_high_watermark = records.iter().map(|record| record.high_watermark).max();
            if let Some(last_offset) = last_offset {
                cursor.next_offset = last_offset.saturating_add(1);
                cursor.last_applied_offset = Some(last_offset);
            }
            cursor.last_high_watermark = last_high_watermark.or(cursor.last_high_watermark);
            if plan.window_size.is_some() {
                let admitted = admit_windowed_source_records(records, cursor, plan, occurred_at)?;
                cursor.last_event_time_watermark = admitted.last_event_time_watermark;
                cursor.last_closed_window_end = admitted.last_closed_window_end;
                cursor.pending_window_ends = admitted.pending_window_ends;
                cursor.dropped_late_event_count = admitted.dropped_late_event_count;
                cursor.last_dropped_late_offset = admitted.last_dropped_late_offset;
                cursor.last_dropped_late_event_at = admitted.last_dropped_late_event_at;
                cursor.last_dropped_late_window_end = admitted.last_dropped_late_window_end;
                cursor.dropped_evicted_window_event_count =
                    admitted.dropped_evicted_window_event_count;
                cursor.last_dropped_evicted_window_offset =
                    admitted.last_dropped_evicted_window_offset;
                cursor.last_dropped_evicted_window_event_at =
                    admitted.last_dropped_evicted_window_event_at;
                cursor.last_dropped_evicted_window_end = admitted.last_dropped_evicted_window_end;
            }
        }
        let frontier_target = latest_offsets_by_partition
            .get(&cursor.source_partition_id)
            .copied()
            .unwrap_or(cursor.initial_checkpoint_target_offset);
        cursor.initial_checkpoint_target_offset = frontier_target;
        let frontier_reached = cursor.next_offset >= frontier_target;
        let idle_timer_watermark = if consumed_records.is_none()
            && should_request_idle_window_timer(cursor, plan, frontier_target)
        {
            Some(occurred_at)
        } else {
            None
        };
        let closed_window_advanced = if consumed_records.is_none() {
            let (_, closed_window_advanced) =
                advance_topic_window_cursor(cursor, plan, idle_timer_watermark)?;
            closed_window_advanced
        } else {
            cursor.last_closed_window_end != previous_closed_window_end
        };
        let checkpoint_evidence = if plan.window_size.is_some() {
            closed_window_advanced
                || (previous_reached_at.is_some() && previous_target == frontier_target)
        } else {
            true
        };
        cursor.checkpoint_reached_at = if frontier_reached && checkpoint_evidence {
            Some(previous_reached_at.unwrap_or(occurred_at))
        } else {
            None
        };
        post_apply_changelog_records.push(source_progressed_changelog_record(
            handle,
            cursor.source_partition_id,
            cursor.next_offset,
            plan.checkpoint_sequence,
            frontier_target,
            cursor.last_applied_offset,
            cursor.last_high_watermark,
            cursor.last_event_time_watermark,
            cursor.last_closed_window_end,
            cursor.pending_window_ends.clone(),
            cursor.dropped_late_event_count,
            cursor.last_dropped_late_offset,
            cursor.last_dropped_late_event_at,
            cursor.last_dropped_late_window_end,
            cursor.dropped_evicted_window_event_count,
            cursor.last_dropped_evicted_window_offset,
            cursor.last_dropped_evicted_window_event_at,
            cursor.last_dropped_evicted_window_end,
            *source_owner_partition_id,
            source_lease_token,
            owner_epoch,
            occurred_at,
        ));
        if !plan.checkpoint_name.is_empty()
            && frontier_reached
            && (if plan.window_size.is_some() {
                closed_window_advanced
            } else {
                checkpoint_evidence
            })
            && cursor.checkpoint_reached_at.is_some()
            && (previous_reached_at.is_none() || previous_target != frontier_target)
        {
            cursor.checkpoint_reached_at = Some(occurred_at);
            post_apply_changelog_records.push(checkpoint_reached_changelog_record(
                handle,
                &handle.job_id,
                &plan.checkpoint_name,
                plan.checkpoint_sequence,
                cursor.source_partition_id,
                owner_epoch,
                occurred_at,
            ));
        }
        cursor.updated_at = occurred_at;
    }
    if !pre_key_runtime_stats.is_empty() {
        if let Some(updated_runtime_state) = runtime_state_update.as_mut() {
            updated_runtime_state.merge_pre_key_runtime_stats(&pre_key_runtime_stats);
            updated_runtime_state.updated_at = occurred_at;
        }
    }
    let mut runtime_state_for_eviction = runtime_state_update
        .clone()
        .or_else(|| existing_runtime_state.clone())
        .context("topic stream job runtime state must exist before window retention")?;
    runtime_state_for_eviction.source_partition_leases = lease_states;
    runtime_state_for_eviction.source_cursors = source_cursors.clone();
    let retention_sweep = sweep_expired_stream_job_windows(
        local_state,
        handle,
        &runtime_state_for_eviction,
        occurred_at,
    )?;
    if !retention_sweep.delete_keys.is_empty() {
        let mut updated_runtime_state = runtime_state_for_eviction.clone();
        apply_window_retention_job_counters_to_runtime_state(
            &mut updated_runtime_state,
            &retention_sweep,
        );
        updated_runtime_state.updated_at = occurred_at;
        runtime_state_update = Some(updated_runtime_state);
    }
    post_apply_projection_records.extend(retention_sweep.projection_records);
    post_apply_changelog_records.extend(retention_sweep.changelog_records);

    let dataflow_batches = dataflow_batches_for_planned_programs(handle, planned_batches);
    Ok(StreamJobActivationPlan {
        execution_planned,
        runtime_state_update,
        dataflow_batches,
        post_apply_projection_records,
        post_apply_changelog_records,
        source_cursor_updates: None,
        terminalized: None,
    })
}

pub(crate) async fn prepare_topic_stream_job_runtime_install(
    local_state: &LocalThroughputState,
    store: Option<&WorkflowStore>,
    app_state: &AppState,
    handle: &StreamJobBridgeHandleRecord,
    resolved_job_override: Option<&CompiledStreamJob>,
    throughput_partitions: i32,
    owner_epoch: u64,
    runtime_state_override: Option<&LocalStreamJobRuntimeState>,
    occurred_at: DateTime<Utc>,
) -> Result<Option<TopicStreamJobRuntimeInstallPlan>> {
    let Some(job) =
        resolved_job_override.cloned().or(resolve_stream_job_definition(store, handle).await?)
    else {
        return Ok(None);
    };
    if job.source.kind != STREAM_SOURCE_TOPIC {
        return Ok(None);
    }
    let Some(plan) = bounded_stream_plan_for_job(&job)? else {
        return Ok(None);
    };
    ensure_signal_workflows_supported(handle, &plan)?;
    let topic = parse_topic_source_spec(handle, &job)?
        .context("topic-backed stream job activation requires topic source metadata")?;
    let topic_config = JsonTopicConfig::new(
        app_state.json_brokers.clone(),
        &topic.topic_name,
        throughput_partitions,
    );
    let existing_runtime_state = if let Some(runtime_state_override) = runtime_state_override {
        Some(runtime_state_override.clone())
    } else {
        local_state.load_stream_job_runtime_state(&handle.handle_id)?
    };
    let refresh_frontier =
        should_refresh_topic_frontier(existing_runtime_state.as_ref(), occurred_at);
    let latest_offsets = if existing_runtime_state.is_none() || refresh_frontier {
        load_json_topic_latest_offsets(
            &topic_config,
            &format!("streams-runtime-{}", handle.handle_id),
        )
        .await?
    } else {
        Vec::new()
    };
    let latest_offsets_by_partition = if latest_offsets.is_empty() {
        existing_runtime_state
            .as_ref()
            .map(topic_frontier_offsets_from_runtime_state)
            .unwrap_or_default()
    } else {
        latest_offsets
            .iter()
            .map(|offset| (offset.partition_id, offset.latest_offset))
            .collect::<HashMap<_, _>>()
    };
    if existing_runtime_state.as_ref().and_then(|state| state.terminal_status.as_deref()).is_some()
    {
        return Ok(Some(TopicStreamJobRuntimeInstallPlan {
            execution_planned: None,
            runtime_state_update: None,
            post_apply_projection_records: Vec::new(),
            post_apply_changelog_records: Vec::new(),
            terminalized: None,
        }));
    }

    let (execution_planned, mut runtime_state, source_cursors, mut source_leases) =
        if let Some(existing_runtime_state) = existing_runtime_state.clone() {
            (
                None,
                existing_runtime_state.clone(),
                existing_runtime_state.source_cursors.clone(),
                existing_runtime_state.source_partition_leases.clone(),
            )
        } else {
            let mut active_partitions =
                latest_offsets.iter().map(|p| p.partition_id).collect::<Vec<_>>();
            active_partitions.sort_unstable();
            let mut source_cursors = Vec::with_capacity(latest_offsets.len());
            let mut source_leases = Vec::with_capacity(latest_offsets.len());
            for partition in &latest_offsets {
                let next_offset =
                    topic.explicit_offsets.get(&partition.partition_id).copied().unwrap_or_else(
                        || {
                            if topic.start_from_latest { partition.latest_offset } else { 0 }
                        },
                    );
                source_cursors.push(LocalStreamJobSourceCursorState {
                    source_partition_id: partition.partition_id,
                    next_offset,
                    initial_checkpoint_target_offset: partition.latest_offset,
                    last_applied_offset: None,
                    last_high_watermark: Some(partition.latest_offset),
                    last_event_time_watermark: None,
                    last_closed_window_end: None,
                    pending_window_ends: Vec::new(),
                    dropped_late_event_count: 0,
                    last_dropped_late_offset: None,
                    last_dropped_late_event_at: None,
                    last_dropped_late_window_end: None,
                    dropped_evicted_window_event_count: 0,
                    last_dropped_evicted_window_offset: None,
                    last_dropped_evicted_window_event_at: None,
                    last_dropped_evicted_window_end: None,
                    checkpoint_reached_at: None,
                    updated_at: occurred_at,
                });
                let owner_partition_id = source_owner_partition_for_source_partition(
                    partition.partition_id,
                    throughput_partitions,
                );
                let source_owner_epoch =
                    crate::throughput_partition_owner_epoch(app_state, owner_partition_id)
                        .unwrap_or(owner_epoch);
                source_leases.push(LocalStreamJobSourceLeaseState {
                    source_partition_id: partition.partition_id,
                    owner_partition_id,
                    owner_epoch: source_owner_epoch,
                    lease_token: source_partition_lease_token(
                        handle,
                        partition.partition_id,
                        owner_partition_id,
                        source_owner_epoch,
                    ),
                    updated_at: occurred_at,
                });
            }
            source_cursors.sort_by_key(|cursor| cursor.source_partition_id);
            source_leases.sort_by_key(|lease| lease.source_partition_id);
            let runtime_state = LocalStreamJobRuntimeState {
                handle_id: handle.handle_id.clone(),
                job_id: handle.job_id.clone(),
                job_name: handle.job_name.clone(),
                view_name: plan.view_name.clone(),
                checkpoint_name: plan.checkpoint_name.clone(),
                checkpoint_sequence: plan.checkpoint_sequence,
                input_item_count: 0,
                materialized_key_count: 0,
                active_partitions: active_partitions.clone(),
                throughput_partition_count: throughput_partitions,
                source_kind: Some(STREAM_SOURCE_TOPIC.to_owned()),
                source_name: Some(topic.topic_name.clone()),
                source_cursors: source_cursors.clone(),
                source_partition_leases: source_leases.clone(),
                dispatch_dataflow_batches: Vec::new(),
                applied_dispatch_batch_ids: Vec::new(),
                dispatch_completed_at: None,
                dispatch_cancelled_at: None,
                stream_owner_epoch: owner_epoch,
                planned_at: occurred_at,
                latest_checkpoint_at: None,
                evicted_window_count: 0,
                last_evicted_window_end: None,
                last_evicted_at: None,
                view_runtime_stats: Vec::new(),
                pre_key_runtime_stats: initial_pre_key_runtime_stats(&plan.pre_key_operators),
                hot_key_runtime_stats: Vec::new(),
                owner_partition_runtime_stats: Vec::new(),
                checkpoint_partitions: Vec::new(),
                terminal_status: None,
                terminal_output: None,
                terminal_error: None,
                terminal_at: None,
                updated_at: occurred_at,
            };
            (
                Some(execution_planned_changelog_record(
                    handle,
                    &plan.view_name,
                    &plan.checkpoint_name,
                    plan.checkpoint_sequence,
                    owner_epoch,
                    0,
                    0,
                    active_partitions,
                    throughput_partitions,
                    occurred_at,
                )),
                runtime_state,
                source_cursors,
                source_leases,
            )
        };

    let previous_checkpoint_sequence =
        runtime_state.checkpoint_sequence.max(plan.checkpoint_sequence);
    let frontier_advanced = source_cursors.iter().any(|cursor| {
        latest_offsets_by_partition
            .get(&cursor.source_partition_id)
            .copied()
            .is_some_and(|latest| latest != cursor.initial_checkpoint_target_offset)
    });
    let current_checkpoint_sequence = if frontier_advanced {
        previous_checkpoint_sequence.saturating_add(1)
    } else {
        previous_checkpoint_sequence
    };
    runtime_state.checkpoint_sequence = current_checkpoint_sequence;
    runtime_state.updated_at = occurred_at;

    let mut post_apply_changelog_records = Vec::new();
    for cursor in &source_cursors {
        let owner_partition_id = source_owner_partition_for_source_partition(
            cursor.source_partition_id,
            throughput_partitions,
        );
        let source_owner_epoch =
            crate::throughput_partition_owner_epoch(app_state, owner_partition_id)
                .unwrap_or(owner_epoch);
        let lease_token = source_partition_lease_token(
            handle,
            cursor.source_partition_id,
            owner_partition_id,
            source_owner_epoch,
        );
        let lease_changed = source_leases
            .iter()
            .find(|lease| lease.source_partition_id == cursor.source_partition_id)
            .is_none_or(|lease| {
                lease.owner_partition_id != owner_partition_id
                    || lease.owner_epoch != source_owner_epoch
                    || lease.lease_token != lease_token
            });
        if lease_changed {
            post_apply_changelog_records.push(source_lease_assigned_changelog_record(
                handle,
                cursor.source_partition_id,
                owner_partition_id,
                source_owner_epoch,
                &lease_token,
                occurred_at,
            ));
            if let Some(existing) = source_leases
                .iter_mut()
                .find(|lease| lease.source_partition_id == cursor.source_partition_id)
            {
                existing.owner_partition_id = owner_partition_id;
                existing.owner_epoch = source_owner_epoch;
                existing.lease_token = lease_token.clone();
                existing.updated_at = occurred_at;
            } else {
                source_leases.push(LocalStreamJobSourceLeaseState {
                    source_partition_id: cursor.source_partition_id,
                    owner_partition_id,
                    owner_epoch: source_owner_epoch,
                    lease_token: lease_token.clone(),
                    updated_at: occurred_at,
                });
            }
        }
    }
    source_leases.sort_by_key(|lease| lease.source_partition_id);
    runtime_state.source_partition_leases = source_leases.clone();
    runtime_state.source_cursors = source_cursors.clone();

    if handle.cancellation_requested_at.is_some() {
        return Ok(Some(TopicStreamJobRuntimeInstallPlan {
            execution_planned,
            runtime_state_update: Some(runtime_state),
            post_apply_projection_records: Vec::new(),
            post_apply_changelog_records,
            terminalized: Some(terminalized_status_changelog_record(
                handle,
                owner_epoch,
                StreamJobBridgeHandleStatus::Cancelled,
                None,
                Some(
                    handle
                        .cancellation_reason
                        .clone()
                        .unwrap_or_else(|| "stream job cancelled".to_owned()),
                ),
                handle.cancellation_requested_at.unwrap_or(occurred_at),
            )),
        }));
    }
    for cursor in &source_cursors {
        let latest_target = latest_offsets_by_partition
            .get(&cursor.source_partition_id)
            .copied()
            .unwrap_or(cursor.initial_checkpoint_target_offset);
        let lease = source_leases
            .iter()
            .find(|lease| lease.source_partition_id == cursor.source_partition_id)
            .context("topic stream job source lease should exist")?;
        if frontier_advanced {
            post_apply_changelog_records.push(source_progressed_changelog_record(
                handle,
                cursor.source_partition_id,
                cursor.next_offset,
                current_checkpoint_sequence,
                latest_target,
                cursor.last_applied_offset,
                cursor.last_high_watermark,
                cursor.last_event_time_watermark,
                cursor.last_closed_window_end,
                cursor.pending_window_ends.clone(),
                cursor.dropped_late_event_count,
                cursor.last_dropped_late_offset,
                cursor.last_dropped_late_event_at,
                cursor.last_dropped_late_window_end,
                cursor.dropped_evicted_window_event_count,
                cursor.last_dropped_evicted_window_offset,
                cursor.last_dropped_evicted_window_event_at,
                cursor.last_dropped_evicted_window_end,
                lease.owner_partition_id,
                &lease.lease_token,
                lease.owner_epoch,
                occurred_at,
            ));
            if cursor.next_offset >= latest_target
                && !plan.checkpoint_name.is_empty()
                && plan.window_size.is_none()
            {
                post_apply_changelog_records.push(checkpoint_reached_changelog_record(
                    handle,
                    &handle.job_id,
                    &plan.checkpoint_name,
                    current_checkpoint_sequence,
                    cursor.source_partition_id,
                    lease.owner_epoch,
                    occurred_at,
                ));
            }
        }
    }
    let retention_sweep =
        sweep_expired_stream_job_windows(local_state, handle, &runtime_state, occurred_at)?;
    if !retention_sweep.delete_keys.is_empty() {
        apply_window_retention_job_counters_to_runtime_state(&mut runtime_state, &retention_sweep);
        runtime_state.updated_at = occurred_at;
    }
    let post_apply_projection_records = retention_sweep.projection_records;
    post_apply_changelog_records.extend(retention_sweep.changelog_records);
    Ok(Some(TopicStreamJobRuntimeInstallPlan {
        execution_planned,
        runtime_state_update: Some(runtime_state),
        post_apply_projection_records,
        post_apply_changelog_records,
        terminalized: None,
    }))
}

pub(crate) async fn poll_topic_stream_job_source_partition_on_local_state(
    store: Option<&WorkflowStore>,
    app_state: &AppState,
    local_state: &LocalThroughputState,
    handle: &StreamJobBridgeHandleRecord,
    resolved_job_override: Option<&CompiledStreamJob>,
    runtime_state_override: Option<&LocalStreamJobRuntimeState>,
    cached_fetcher: Option<&mut Option<JsonPartitionFetcher>>,
    buffered_records: Option<&mut VecDeque<ConsumedJsonRecord>>,
    request: &TopicStreamJobPollRequest,
    throughput_partitions: i32,
    occurred_at: DateTime<Utc>,
) -> Result<TopicStreamJobPollResult> {
    let Some(job) =
        resolved_job_override.cloned().or(resolve_stream_job_definition(store, handle).await?)
    else {
        return Ok(TopicStreamJobPollResult {
            runtime_state_update: None,
            runtime_delta: None,
            projection_records: Vec::new(),
            owner_input_batches: Vec::new(),
            changelog_records: Vec::new(),
        });
    };
    let Some(plan) = bounded_stream_plan_for_job(&job)? else {
        return Ok(TopicStreamJobPollResult {
            runtime_state_update: None,
            runtime_delta: None,
            projection_records: Vec::new(),
            owner_input_batches: Vec::new(),
            changelog_records: Vec::new(),
        });
    };
    ensure_signal_workflows_supported(handle, &plan)?;
    let topic = parse_topic_source_spec(handle, &job)?
        .context("topic-backed stream job activation requires topic source metadata")?;
    let topic_config = JsonTopicConfig::new(
        app_state.json_brokers.clone(),
        &topic.topic_name,
        throughput_partitions,
    );
    let mut owned_fetcher = None;
    let fetcher_slot =
        if let Some(cached_fetcher) = cached_fetcher { cached_fetcher } else { &mut owned_fetcher };
    let mut owned_buffer = VecDeque::new();
    let buffered_records = if let Some(buffered_records) = buffered_records {
        buffered_records
    } else {
        &mut owned_buffer
    };
    if !request.idle_window_timer || request.start_offset < request.checkpoint_target_offset {
        if fetcher_slot.is_none() {
            *fetcher_slot = Some(
                build_json_partition_fetcher(
                    &topic_config,
                    &format!(
                        "streams-runtime-source-{}-{}",
                        handle.handle_id, request.source_partition_id
                    ),
                    request.source_partition_id,
                )
                .await?,
            );
        }
    }
    let mut pre_key_runtime_stats = initial_pre_key_runtime_stats(&plan.pre_key_operators);
    let mut hot_key_counts = HashMap::<String, (String, u64, i32)>::new();
    let mut existing_runtime_state = if let Some(runtime_state_override) = runtime_state_override {
        runtime_state_override.clone()
    } else {
        local_state
            .load_stream_job_runtime_state(&handle.handle_id)?
            .context("topic stream job runtime state should exist for source poll")?
    };
    let mut consumed = Vec::<TopicSourceBatchRecord>::new();
    let mut fetch_offset = request.start_offset;
    while consumed.len() < STREAM_JOB_TOPIC_POLL_MAX_RECORDS {
        if buffered_records.is_empty()
            && (!request.idle_window_timer
                || request.start_offset < request.checkpoint_target_offset)
        {
            let Some(fetcher) = fetcher_slot.as_ref() else {
                break;
            };
            let (records, _high_watermark) = fetcher
                .fetch_records(
                    fetch_offset,
                    1..STREAM_JOB_TOPIC_POLL_MAX_BATCH_BYTES,
                    i32::try_from(STREAM_JOB_TOPIC_IDLE_WAIT_MS).unwrap_or(i32::MAX),
                )
                .await?;
            buffered_records.extend(records);
        }
        let Some(record) = buffered_records.pop_front() else {
            break;
        };
        let mut item: Value = decode_json_record(&record.record)
            .context("failed to decode topic-backed stream job source record")?;
        if !apply_stream_job_pre_key_operators(
            &mut item,
            &plan.pre_key_operators,
            &mut pre_key_runtime_stats,
        )? {
            continue;
        }
        let logical_key = string_field_ref(&item, &plan.key_field)?.to_owned();
        let hot_key_stats = hot_key_counts
            .entry(logical_key.clone())
            .or_insert_with(|| (logical_key.clone(), 0, request.source_partition_id));
        hot_key_stats.1 = hot_key_stats.1.saturating_add(1);
        let value = if plan.reducer_kind == STREAM_REDUCER_COUNT {
            1.0
        } else {
            threshold_value(
                &plan,
                numeric_field(
                    &item,
                    plan.value_field.as_deref().context(format!(
                        "stream reducer {} requires valueField for topic sources",
                        plan.reducer_kind
                    ))?,
                )?,
            )
        };
        for assignment in windowed_item_assignments(&item, &logical_key, &plan)? {
            consumed.push(TopicSourceBatchRecord {
                logical_key: assignment.logical_key,
                display_key: logical_key.clone(),
                window_start: assignment.window_start,
                window_end: assignment.window_end,
                event_time: assignment.event_time,
                window_end_at: assignment.window_end_at,
                value,
                offset: record.record.offset,
                high_watermark: record.high_watermark,
            });
        }
        fetch_offset = record.record.offset.saturating_add(1);
    }
    let existing_cursor = existing_runtime_state
        .source_cursors
        .iter()
        .find(|cursor| cursor.source_partition_id == request.source_partition_id)
        .cloned()
        .context("topic stream job source cursor should exist for source poll")?;
    if consumed.is_empty() {
        if !request.idle_window_timer {
            return Ok(TopicStreamJobPollResult {
                runtime_state_update: None,
                runtime_delta: None,
                projection_records: Vec::new(),
                owner_input_batches: Vec::new(),
                changelog_records: Vec::new(),
            });
        }
        let mut updated_cursor = existing_cursor;
        let (watermark_advanced, closed_window_advanced) =
            advance_topic_window_cursor(&mut updated_cursor, &plan, Some(occurred_at))?;
        if !watermark_advanced && !closed_window_advanced {
            return Ok(TopicStreamJobPollResult {
                runtime_state_update: None,
                runtime_delta: None,
                projection_records: Vec::new(),
                owner_input_batches: Vec::new(),
                changelog_records: Vec::new(),
            });
        }
        updated_cursor.initial_checkpoint_target_offset = request.checkpoint_target_offset;
        if closed_window_advanced {
            updated_cursor.checkpoint_reached_at = Some(occurred_at);
        }
        updated_cursor.updated_at = occurred_at;
        let source_owner_epoch =
            crate::throughput_partition_owner_epoch(app_state, request.source_owner_partition_id)
                .unwrap_or(1);
        let mut changelog_records = vec![source_progressed_changelog_record(
            handle,
            request.source_partition_id,
            updated_cursor.next_offset,
            request.checkpoint_sequence,
            request.checkpoint_target_offset,
            updated_cursor.last_applied_offset,
            updated_cursor.last_high_watermark,
            updated_cursor.last_event_time_watermark,
            updated_cursor.last_closed_window_end,
            updated_cursor.pending_window_ends.clone(),
            updated_cursor.dropped_late_event_count,
            updated_cursor.last_dropped_late_offset,
            updated_cursor.last_dropped_late_event_at,
            updated_cursor.last_dropped_late_window_end,
            updated_cursor.dropped_evicted_window_event_count,
            updated_cursor.last_dropped_evicted_window_offset,
            updated_cursor.last_dropped_evicted_window_event_at,
            updated_cursor.last_dropped_evicted_window_end,
            request.source_owner_partition_id,
            &request.lease_token,
            source_owner_epoch,
            occurred_at,
        )];
        if !plan.checkpoint_name.is_empty() {
            changelog_records.push(checkpoint_reached_changelog_record(
                handle,
                &handle.job_id,
                &plan.checkpoint_name,
                request.checkpoint_sequence,
                request.source_partition_id,
                source_owner_epoch,
                occurred_at,
            ));
        }
        if let Some(cursor) = existing_runtime_state
            .source_cursors
            .iter_mut()
            .find(|cursor| cursor.source_partition_id == request.source_partition_id)
        {
            *cursor = updated_cursor.clone();
        }
        if !pre_key_runtime_stats.is_empty() {
            existing_runtime_state.merge_pre_key_runtime_stats(&pre_key_runtime_stats);
            existing_runtime_state.updated_at = occurred_at;
        }
        let source_owner_epoch =
            crate::throughput_partition_owner_epoch(app_state, request.source_owner_partition_id)
                .unwrap_or(1);
        let runtime_delta = TopicStreamJobPollRuntimeDelta {
            checkpoint_sequence: request.checkpoint_sequence,
            source_cursor_update: updated_cursor.clone(),
            source_lease_update: LocalStreamJobSourceLeaseState {
                source_partition_id: request.source_partition_id,
                owner_partition_id: request.source_owner_partition_id,
                owner_epoch: source_owner_epoch,
                lease_token: request.lease_token.clone(),
                updated_at: occurred_at,
            },
            pre_key_runtime_stats: pre_key_runtime_stats.clone(),
            hot_key_runtime_stats: Vec::new(),
            owner_partition_runtime_stats: Vec::new(),
            updated_at: occurred_at,
        };
        return Ok(TopicStreamJobPollResult {
            runtime_state_update: Some(existing_runtime_state),
            runtime_delta: Some(runtime_delta),
            projection_records: Vec::new(),
            owner_input_batches: Vec::new(),
            changelog_records,
        });
    }
    let hot_key_runtime_stats = hot_key_runtime_stats_from_counts(hot_key_counts, occurred_at);

    let admitted = admit_windowed_source_records(&consumed, &existing_cursor, &plan, occurred_at)?;
    let source_owner_epoch =
        crate::throughput_partition_owner_epoch(app_state, request.source_owner_partition_id)
            .unwrap_or(1);
    let offset_start = consumed.first().map(|record| record.offset).unwrap_or(0);
    let offset_end = consumed.last().map(|record| record.offset).unwrap_or(offset_start);
    let source_batch = TopicSourceBatch {
        handle_id: handle.handle_id.clone(),
        job_id: handle.job_id.clone(),
        source_partition_id: request.source_partition_id,
        source_owner_partition_id: request.source_owner_partition_id,
        source_lease_token: request.lease_token.clone(),
        checkpoint_sequence: request.checkpoint_sequence,
        checkpoint_target_offset: request.checkpoint_target_offset,
        idle_window_timer: request.idle_window_timer,
        offset_start,
        offset_end,
        records: admitted.accepted_records.clone(),
    };
    let owner_input_batches = owner_input_batches_from_source_batch(
        handle,
        &source_batch,
        &plan,
        throughput_partitions,
        source_owner_epoch,
        occurred_at,
    )?;
    let owner_partition_runtime_stats =
        owner_partition_runtime_stats_from_batches(&owner_input_batches, occurred_at);

    let last_offset = consumed.last().map(|record| record.offset);
    let last_high_watermark = consumed.iter().map(|record| record.high_watermark).max();
    let next_offset =
        last_offset.map(|value| value.saturating_add(1)).unwrap_or(request.start_offset);
    let mut updated_cursor = existing_cursor.clone();
    if plan.window_size.is_some() {
        updated_cursor.last_event_time_watermark = admitted.last_event_time_watermark;
        updated_cursor.last_closed_window_end = admitted.last_closed_window_end;
        updated_cursor.pending_window_ends = admitted.pending_window_ends;
        updated_cursor.dropped_late_event_count = admitted.dropped_late_event_count;
        updated_cursor.last_dropped_late_offset = admitted.last_dropped_late_offset;
        updated_cursor.last_dropped_late_event_at = admitted.last_dropped_late_event_at;
        updated_cursor.last_dropped_late_window_end = admitted.last_dropped_late_window_end;
        updated_cursor.dropped_evicted_window_event_count =
            admitted.dropped_evicted_window_event_count;
        updated_cursor.last_dropped_evicted_window_offset =
            admitted.last_dropped_evicted_window_offset;
        updated_cursor.last_dropped_evicted_window_event_at =
            admitted.last_dropped_evicted_window_event_at;
        updated_cursor.last_dropped_evicted_window_end = admitted.last_dropped_evicted_window_end;
    }
    updated_cursor.next_offset = next_offset;
    updated_cursor.initial_checkpoint_target_offset = request.checkpoint_target_offset;
    updated_cursor.last_applied_offset = last_offset;
    updated_cursor.last_high_watermark = last_high_watermark;
    updated_cursor.updated_at = occurred_at;
    let closed_window_advanced =
        updated_cursor.last_closed_window_end != existing_cursor.last_closed_window_end;
    let mut changelog_records = vec![source_progressed_changelog_record(
        handle,
        request.source_partition_id,
        next_offset,
        request.checkpoint_sequence,
        request.checkpoint_target_offset,
        last_offset,
        last_high_watermark,
        updated_cursor.last_event_time_watermark,
        updated_cursor.last_closed_window_end,
        updated_cursor.pending_window_ends.clone(),
        updated_cursor.dropped_late_event_count,
        updated_cursor.last_dropped_late_offset,
        updated_cursor.last_dropped_late_event_at,
        updated_cursor.last_dropped_late_window_end,
        updated_cursor.dropped_evicted_window_event_count,
        updated_cursor.last_dropped_evicted_window_offset,
        updated_cursor.last_dropped_evicted_window_event_at,
        updated_cursor.last_dropped_evicted_window_end,
        request.source_owner_partition_id,
        &request.lease_token,
        source_owner_epoch,
        occurred_at,
    )];
    if next_offset >= request.checkpoint_target_offset
        && !plan.checkpoint_name.is_empty()
        && (if plan.window_size.is_some() { closed_window_advanced } else { true })
    {
        changelog_records.push(checkpoint_reached_changelog_record(
            handle,
            &handle.job_id,
            &plan.checkpoint_name,
            request.checkpoint_sequence,
            request.source_partition_id,
            source_owner_epoch,
            occurred_at,
        ));
    }
    if let Some(cursor) = existing_runtime_state
        .source_cursors
        .iter_mut()
        .find(|cursor| cursor.source_partition_id == request.source_partition_id)
    {
        *cursor = updated_cursor.clone();
    }
    if !pre_key_runtime_stats.is_empty() {
        existing_runtime_state.merge_pre_key_runtime_stats(&pre_key_runtime_stats);
        existing_runtime_state.updated_at = occurred_at;
    }
    if !hot_key_runtime_stats.is_empty() {
        existing_runtime_state.merge_hot_key_runtime_stats(
            &hot_key_runtime_stats,
            STREAM_JOB_HOT_KEY_RUNTIME_STATS_LIMIT,
        );
        existing_runtime_state.updated_at = occurred_at;
    }
    if !owner_partition_runtime_stats.is_empty() {
        existing_runtime_state.merge_owner_partition_runtime_stats(&owner_partition_runtime_stats);
        existing_runtime_state.updated_at = occurred_at;
    }
    let runtime_delta = TopicStreamJobPollRuntimeDelta {
        checkpoint_sequence: request.checkpoint_sequence,
        source_cursor_update: updated_cursor,
        source_lease_update: LocalStreamJobSourceLeaseState {
            source_partition_id: request.source_partition_id,
            owner_partition_id: request.source_owner_partition_id,
            owner_epoch: source_owner_epoch,
            lease_token: request.lease_token.clone(),
            updated_at: occurred_at,
        },
        pre_key_runtime_stats,
        hot_key_runtime_stats,
        owner_partition_runtime_stats,
        updated_at: occurred_at,
    };
    Ok(TopicStreamJobPollResult {
        runtime_state_update: Some(existing_runtime_state),
        runtime_delta: Some(runtime_delta),
        projection_records: Vec::new(),
        owner_input_batches,
        changelog_records,
    })
}

pub(crate) async fn plan_stream_job_activation(
    local_state: &LocalThroughputState,
    store: Option<&WorkflowStore>,
    app_state: Option<&AppState>,
    handle: &StreamJobBridgeHandleRecord,
    throughput_partitions: i32,
    owner_epoch: Option<u64>,
    occurred_at: DateTime<Utc>,
) -> Result<Option<StreamJobActivationPlan>> {
    const STREAM_JOB_OWNER_BATCH_MAX_ITEMS: usize = 64;

    let Some(job) = resolve_stream_job_definition(store, handle).await? else {
        return Ok(None);
    };
    let _canonical_dataflow_plan = job.dataflow_plan(dataflow_plan_id_for_handle(handle))?;
    let owner_epoch = owner_epoch.unwrap_or(1);
    let bounded_plan = bounded_stream_plan_for_job(&job)?;
    let Some(plan) = bounded_plan else {
        return Ok(None);
    };
    ensure_signal_workflows_supported(handle, &plan)?;

    if job.source.kind == STREAM_SOURCE_TOPIC {
        let app_state =
            app_state.context("topic-backed stream job activation requires runtime app state")?;
        let topic = parse_topic_source_spec(handle, &job)?
            .context("topic-backed stream job activation requires topic source metadata")?;
        return Ok(Some(
            plan_topic_stream_job_activation(
                local_state,
                app_state,
                handle,
                &plan,
                &topic,
                throughput_partitions,
                owner_epoch,
                occurred_at,
            )
            .await?,
        ));
    }

    if let Some(runtime_state) = local_state.load_stream_job_runtime_state(&handle.handle_id)? {
        if runtime_state.dispatch_cancelled_at.is_some() {
            return Ok(Some(StreamJobActivationPlan {
                execution_planned: None,
                runtime_state_update: None,
                dataflow_batches: Vec::new(),
                post_apply_projection_records: Vec::new(),
                post_apply_changelog_records: Vec::new(),
                source_cursor_updates: None,
                terminalized: None,
            }));
        }
        if !runtime_state.dispatch_manifest_batches().is_empty() {
            let pending_dispatch_batches = runtime_state
                .dispatch_manifest_batches()
                .iter()
                .filter(|batch| {
                    !runtime_state
                        .applied_dispatch_batch_ids
                        .iter()
                        .any(|applied| applied == &batch.batch_id)
                })
                .collect::<Vec<_>>();
            let terminalized = if pending_dispatch_batches.is_empty()
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
            let mut dataflow_batches = pending_dispatch_batches
                .iter()
                .map(|batch| {
                    dataflow_batch_from_local_dispatch_batch(handle, batch, Some(&runtime_state))
                })
                .collect::<Vec<_>>();
            if let Some(last) = dataflow_batches.last_mut() {
                last.program.completes_dispatch = true;
            }
            return Ok(Some(StreamJobActivationPlan {
                execution_planned: None,
                runtime_state_update: None,
                dataflow_batches,
                post_apply_projection_records: Vec::new(),
                post_apply_changelog_records: Vec::new(),
                source_cursor_updates: None,
                terminalized,
            }));
        }
    }

    let mut unique_keys = std::collections::HashSet::<String>::new();
    let mut active_partitions = std::collections::BTreeSet::<i32>::new();
    let mut items_by_partition = BTreeMap::<i32, HashMap<String, StreamJobPartitionItem>>::new();
    let mut routing_keys = HashMap::<i32, String>::new();
    let mut pre_key_runtime_stats = initial_pre_key_runtime_stats(&plan.pre_key_operators);
    let mut hot_key_counts = HashMap::<String, (String, u64, i32)>::new();
    let reducer_value_field = if plan.reducer_kind == STREAM_REDUCER_COUNT {
        None
    } else {
        Some(plan.value_field.as_deref().context(format!(
            "stream runtime {} reducer {} requires valueField",
            job.runtime, plan.reducer_kind
        ))?)
    };
    let mut accepted_item_count = 0usize;
    let _total_item_count = for_each_bounded_input_item(&handle.input_ref, |item| {
        let mut item = item;
        if !apply_stream_job_pre_key_operators(
            &mut item,
            &plan.pre_key_operators,
            &mut pre_key_runtime_stats,
        )? {
            return Ok(());
        }
        let key = string_field_ref(&item, &plan.key_field)?;
        let value = if plan.reducer_kind == STREAM_REDUCER_COUNT {
            1.0
        } else {
            threshold_value(
                &plan,
                numeric_field(&item, reducer_value_field.expect("reducer value field must exist"))?,
            )
        };
        let stream_partition_id = throughput_partition_for_stream_key(key, throughput_partitions);
        let hot_key_stats = hot_key_counts
            .entry(key.to_owned())
            .or_insert_with(|| (key.to_owned(), 0, stream_partition_id));
        hot_key_stats.1 = hot_key_stats.1.saturating_add(1);
        active_partitions.insert(stream_partition_id);
        for assignment in windowed_item_assignments(&item, key, &plan)? {
            unique_keys.insert(assignment.logical_key.clone());
            routing_keys
                .entry(stream_partition_id)
                .or_insert_with(|| assignment.logical_key.clone());
            let partition_items = items_by_partition.entry(stream_partition_id).or_default();
            if let Some(aggregate) = partition_items.get_mut(&assignment.logical_key) {
                merge_partition_item(&plan.reducer_kind, aggregate, value)?;
            } else {
                partition_items.insert(
                    assignment.logical_key.clone(),
                    StreamJobPartitionItem {
                        logical_key: assignment.logical_key,
                        value,
                        display_key: Some(key.to_owned()),
                        window_start: assignment.window_start,
                        window_end: assignment.window_end,
                        count: 1,
                    },
                );
            }
        }
        accepted_item_count += 1;
        Ok(())
    })?;
    let input_item_count = accepted_item_count;
    let hot_key_runtime_stats = hot_key_runtime_stats_from_counts(hot_key_counts, occurred_at);
    let active_partitions = active_partitions.into_iter().collect::<Vec<_>>();
    let execution_planned = execution_planned_changelog_record(
        handle,
        &plan.view_name,
        &plan.checkpoint_name,
        plan.checkpoint_sequence,
        owner_epoch,
        input_item_count as u64,
        unique_keys.len() as u64,
        active_partitions.clone(),
        throughput_partitions,
        occurred_at,
    );
    let mut runtime_state = LocalStreamJobRuntimeState {
        handle_id: handle.handle_id.clone(),
        job_id: handle.job_id.clone(),
        job_name: handle.job_name.clone(),
        view_name: plan.view_name.clone(),
        checkpoint_name: plan.checkpoint_name.clone(),
        checkpoint_sequence: plan.checkpoint_sequence,
        input_item_count: input_item_count as u64,
        materialized_key_count: unique_keys.len() as u64,
        active_partitions: active_partitions.clone(),
        throughput_partition_count: throughput_partitions,
        source_kind: Some(STREAM_SOURCE_BOUNDED_INPUT.to_owned()),
        source_name: None,
        source_cursors: Vec::new(),
        source_partition_leases: Vec::new(),
        dispatch_dataflow_batches: Vec::new(),
        applied_dispatch_batch_ids: Vec::new(),
        dispatch_completed_at: None,
        dispatch_cancelled_at: None,
        stream_owner_epoch: owner_epoch,
        planned_at: occurred_at,
        latest_checkpoint_at: None,
        evicted_window_count: 0,
        last_evicted_window_end: None,
        last_evicted_at: None,
        view_runtime_stats: Vec::new(),
        pre_key_runtime_stats,
        hot_key_runtime_stats,
        owner_partition_runtime_stats: Vec::new(),
        checkpoint_partitions: Vec::new(),
        terminal_status: None,
        terminal_output: None,
        terminal_error: None,
        terminal_at: None,
        updated_at: occurred_at,
    };
    let mut planned_batches = Vec::new();
    for (stream_partition_id, partition_items_by_key) in items_by_partition {
        let routing_key =
            routing_keys.remove(&stream_partition_id).unwrap_or_else(|| handle.job_id.clone());
        let mut partition_items =
            partition_items_by_key.into_values().collect::<Vec<_>>().into_iter();
        let partition_item_count = partition_items.len();
        let batch_count = (partition_item_count + STREAM_JOB_OWNER_BATCH_MAX_ITEMS - 1)
            / STREAM_JOB_OWNER_BATCH_MAX_ITEMS;
        for batch_index in 0..batch_count {
            let remaining_batches = batch_count.saturating_sub(batch_index + 1);
            let remaining_items =
                partition_item_count.saturating_sub(batch_index * STREAM_JOB_OWNER_BATCH_MAX_ITEMS);
            let batch_len = if remaining_batches == 0 {
                remaining_items
            } else {
                STREAM_JOB_OWNER_BATCH_MAX_ITEMS.min(remaining_items)
            };
            let items = partition_items.by_ref().take(batch_len).collect::<Vec<_>>();
            planned_batches.push(PlannedBoundedDataflowBatch {
                batch_id: dispatch_batch_id(handle, stream_partition_id, batch_index),
                source_id: format!("bounded:{}", handle.handle_id),
                source_partition_id: stream_partition_id,
                source_epoch: plan.checkpoint_sequence.max(0) as u64,
                offset_range: None,
                program: crate::dataflow_engine::BoundedDataflowBatchProgram {
                    stream_partition_id,
                    checkpoint_partition_id: None,
                    source_owner_partition_id: None,
                    source_lease_token: None,
                    routing_key: routing_key.clone(),
                    key_field: plan.key_field.clone(),
                    reducer_kind: plan.reducer_kind.clone(),
                    output_field: plan.output_field.clone(),
                    view_name: plan.view_name.clone(),
                    additional_view_names: plan.additional_view_names.clone(),
                    eventual_projection_view_names: plan.eventual_projection_view_names.clone(),
                    workflow_signals: plan.workflow_signals.clone(),
                    checkpoint_name: plan.checkpoint_name.clone(),
                    checkpoint_sequence: plan.checkpoint_sequence,
                    owner_epoch,
                    occurred_at,
                    items: items
                        .into_iter()
                        .map(|item| crate::dataflow_engine::BoundedDataflowItem {
                            logical_key: item.logical_key,
                            value: item.value,
                            display_key: item.display_key,
                            window_start: item.window_start,
                            window_end: item.window_end,
                            count: item.count,
                        })
                        .collect(),
                    is_final_partition_batch: batch_index + 1 == batch_count,
                    completes_dispatch: false,
                },
            });
        }
    }
    if let Some(last) = planned_batches.last_mut() {
        last.program.completes_dispatch = true;
    }
    let dataflow_batches = dataflow_batches_for_planned_programs(handle, planned_batches);
    runtime_state.owner_partition_runtime_stats =
        owner_partition_runtime_stats_from_batches(&dataflow_batches, occurred_at);
    Ok(Some(StreamJobActivationPlan {
        execution_planned: Some(execution_planned),
        runtime_state_update: Some(runtime_state),
        dataflow_batches,
        post_apply_projection_records: Vec::new(),
        post_apply_changelog_records: Vec::new(),
        source_cursor_updates: None,
        terminalized: Some(terminalized_changelog_record(handle, owner_epoch, occurred_at)),
    }))
}

pub(crate) fn apply_stream_job_dataflow_batch_on_local_state(
    local_state: &LocalThroughputState,
    handle: &StreamJobBridgeHandleRecord,
    batch: &StreamJobDataflowBatch,
    mirror_owner_state: bool,
) -> Result<StreamJobDataflowBatchApplyResult> {
    apply_stream_job_dataflow_batch_on_local_state_with_runtime_override(
        local_state,
        handle,
        batch,
        mirror_owner_state,
        None,
    )
}

pub(crate) fn apply_stream_job_dataflow_batch_list_on_local_state(
    local_state: &LocalThroughputState,
    handle: &StreamJobBridgeHandleRecord,
    batch_list: &[StreamJobDataflowBatch],
    mirror_owner_state: bool,
) -> Result<StreamJobDataflowApplyResult> {
    apply_stream_job_dataflow_batch_list_on_local_state_with_runtime_override(
        local_state,
        handle,
        batch_list,
        mirror_owner_state,
        None,
    )
}

pub(crate) fn apply_stream_job_dataflow_batch_list_on_local_state_with_runtime_override(
    local_state: &LocalThroughputState,
    handle: &StreamJobBridgeHandleRecord,
    batch_list: &[StreamJobDataflowBatch],
    mirror_owner_state: bool,
    runtime_state_override: Option<&LocalStreamJobRuntimeState>,
) -> Result<StreamJobDataflowApplyResult> {
    crate::dataflow_engine::LocalDataflowEngine::new(local_state).apply_stream_job_batch_list(
        handle,
        runtime_state_override,
        batch_list,
        mirror_owner_state,
    )
}

pub(crate) fn apply_stream_job_dataflow_batch_on_local_state_with_runtime_override(
    local_state: &LocalThroughputState,
    handle: &StreamJobBridgeHandleRecord,
    batch: &StreamJobDataflowBatch,
    mirror_owner_state: bool,
    runtime_state_override: Option<&LocalStreamJobRuntimeState>,
) -> Result<StreamJobDataflowBatchApplyResult> {
    crate::dataflow_engine::LocalDataflowEngine::new(local_state).apply_stream_job_batch(
        handle,
        runtime_state_override,
        batch,
        mirror_owner_state,
    )
}

pub(crate) fn apply_stream_job_dataflow_batch_on_local_state_with_metrics(
    local_state: &LocalThroughputState,
    handle: &StreamJobBridgeHandleRecord,
    batch: &StreamJobDataflowBatch,
    mirror_owner_state: bool,
    runtime_state_override: Option<&LocalStreamJobRuntimeState>,
    mut metrics: Option<&mut StreamJobApplyMetrics>,
) -> Result<StreamJobActivationResult> {
    let Some(prepared) = crate::dataflow_engine::LocalDataflowEngine::new(local_state)
        .prepare_stream_job_batch_apply(
            handle,
            batch,
            mirror_owner_state,
            runtime_state_override,
            metrics.as_deref_mut(),
        )?
    else {
        return Ok(StreamJobActivationResult {
            projection_records: Vec::new(),
            changelog_records: Vec::new(),
        });
    };
    let mut activation_result = prepared.activation_result;
    if mirror_owner_state {
        let persist_started = std::time::Instant::now();
        let owner_view_update_count = prepared.owner_view_updates.len();
        let applied = local_state.persist_stream_job_batch_apply(
            &handle.handle_id,
            &prepared.batch_id,
            prepared.compact_checkpoint_mirrors,
            prepared.completes_dispatch,
            prepared.occurred_at,
            prepared.owner_view_updates,
            vec![prepared.accepted_progress_state],
            &prepared.mirrored_stream_entries,
        )?;
        if let Some(metrics) = metrics.as_deref_mut()
            && applied.runtime_state_updated
        {
            metrics.runtime_state_write_count += 1;
        }
        if prepared.state_key_delta > 0 {
            let Some(mut runtime_state) =
                local_state.load_stream_job_runtime_state(&handle.handle_id)?
            else {
                anyhow::bail!("stream job runtime state missing for handle {}", handle.handle_id);
            };
            runtime_state.record_owner_partition_state_key_delta(
                prepared.stream_partition_id,
                prepared.state_key_delta,
                prepared.occurred_at,
            );
            runtime_state.updated_at = prepared.occurred_at;
            local_state.upsert_stream_job_runtime_state(&runtime_state)?;
            if let Some(metrics) = metrics.as_deref_mut() {
                metrics.runtime_state_write_count += 1;
            }
        }
        if let Some(metrics) = metrics.as_deref_mut() {
            metrics.view_state_write_count += owner_view_update_count;
            metrics.changelog_mirror_write_count += prepared.mirrored_stream_entries.len();
            metrics.mirrored_stream_entry_count += prepared.mirrored_stream_entries.len();
            metrics.persist_elapsed += persist_started.elapsed();
        }
        for record in &mut activation_result.changelog_records {
            if matches!(
                &record.entry,
                crate::ChangelogRecordEntry::Streams(entry)
                    if !matches!(
                        entry.payload,
                        StreamsChangelogPayload::StreamJobViewUpdated { .. }
                            | StreamsChangelogPayload::StreamJobViewBatchUpdated { .. }
                    )
            ) {
                record.local_mirror_applied = true;
                record.partition_mirror_applied = true;
            }
        }
    }
    Ok(activation_result)
}

pub(crate) async fn activate_stream_job_on_local_state(
    store: Option<&WorkflowStore>,
    app_state: Option<&AppState>,
    local_state: &LocalThroughputState,
    handle: &StreamJobBridgeHandleRecord,
    throughput_partitions: i32,
    owner_epoch: Option<u64>,
    occurred_at: DateTime<Utc>,
) -> Result<Option<StreamJobActivationResult>> {
    let Some(plan) = plan_stream_job_activation(
        local_state,
        store,
        app_state,
        handle,
        throughput_partitions,
        owner_epoch,
        occurred_at,
    )
    .await?
    else {
        return Ok(None);
    };
    Ok(Some(
        crate::dataflow_engine::LocalDataflowEngine::new(local_state).execute_stream_job_plan(
            handle,
            plan,
            occurred_at,
        )?,
    ))
}

pub(crate) fn materialization_outcome_from_local_state(
    local_state: &LocalThroughputState,
    handle: &StreamJobBridgeHandleRecord,
) -> Result<Option<StreamJobMaterializationOutcome>> {
    let Some(runtime_state) = local_state.load_stream_job_runtime_state(&handle.handle_id)? else {
        return Ok(None);
    };
    let sealed_checkpoint_states =
        sealed_checkpoint_states_for_runtime(local_state, &runtime_state)?;
    let checkpoint_states = if runtime_state.checkpoint_name.is_empty() {
        Vec::new()
    } else {
        let persisted_states = local_state
            .load_all_stream_job_checkpoints()?
            .into_iter()
            .filter(|state| {
                state.handle_id == handle.handle_id
                    && state.checkpoint_name == runtime_state.checkpoint_name
                    && state.checkpoint_sequence == runtime_state.checkpoint_sequence
                    && runtime_state.active_partitions.contains(&state.stream_partition_id)
            })
            .collect::<Vec<_>>();
        if persisted_states.is_empty() {
            runtime_state
                .checkpoint_partitions
                .iter()
                .filter(|state| {
                    state.checkpoint_name == runtime_state.checkpoint_name
                        && state.checkpoint_sequence == runtime_state.checkpoint_sequence
                        && runtime_state.active_partitions.contains(&state.stream_partition_id)
                })
                .cloned()
                .collect::<Vec<_>>()
        } else {
            persisted_states
        }
    };
    let bridge_callbacks = local_state.load_stream_job_bridge_callbacks(&handle.handle_id)?;
    let checkpoint = checkpoint_materialization_record(
        handle,
        &runtime_state,
        &sealed_checkpoint_states,
        &checkpoint_states,
        &bridge_callbacks,
    );
    let terminal_ready = runtime_state.checkpoint_name.is_empty() || checkpoint.is_some();
    let terminal =
        terminal_materialization_result(&runtime_state, &bridge_callbacks, terminal_ready)?;
    let mut workflow_signals = local_state
        .load_all_stream_job_workflow_signals()?
        .into_iter()
        .filter(|state| state.handle_id == handle.handle_id && state.published_at.is_none())
        .collect::<Vec<_>>();
    workflow_signals.sort_by(|left, right| {
        left.signaled_at.cmp(&right.signaled_at).then_with(|| left.signal_id.cmp(&right.signal_id))
    });

    Ok(Some(StreamJobMaterializationOutcome { checkpoint, terminal, workflow_signals }))
}

fn stream_job_checkpoint_callback_id(
    handle_id: &str,
    checkpoint_name: &str,
    checkpoint_sequence: i64,
) -> String {
    format!("stream-job-callback:{handle_id}:checkpoint:{checkpoint_name}:{checkpoint_sequence}")
}

fn stream_job_terminal_callback_id(handle_id: &str, status: &str) -> String {
    format!("stream-job-callback:{handle_id}:terminal:{status}")
}

fn checkpoint_materialization_record(
    handle: &StreamJobBridgeHandleRecord,
    runtime_state: &LocalStreamJobRuntimeState,
    sealed_checkpoint_states: &[crate::local_state::LocalStreamJobSealedCheckpointState],
    checkpoint_states: &[crate::local_state::LocalStreamJobCheckpointState],
    bridge_callbacks: &[LocalStreamJobBridgeCallbackState],
) -> Option<StreamJobCheckpointRecord> {
    if runtime_state.checkpoint_name.is_empty() {
        return None;
    }
    let checkpoint_callback = bridge_callbacks.iter().find(|state| {
        state.record.callback_kind == STREAM_JOB_CALLBACK_KIND_CHECKPOINT
            && state.record.checkpoint.as_ref().is_some_and(|checkpoint| {
                checkpoint.checkpoint_name == runtime_state.checkpoint_name
                    && checkpoint.checkpoint_sequence == runtime_state.checkpoint_sequence
            })
    });
    if let Some(callback_state) = checkpoint_callback {
        return Some(build_stream_job_checkpoint_record(
            handle,
            runtime_state,
            callback_state.created_at,
            callback_state.record.owner_epoch,
            sealed_checkpoint_output(sealed_checkpoint_states),
        ));
    }
    if sealed_checkpoint_states.len() == runtime_state.active_partitions.len()
        && !sealed_checkpoint_states.is_empty()
    {
        let reached_at = sealed_checkpoint_states
            .iter()
            .map(|state| state.sealed_at)
            .max()
            .unwrap_or(runtime_state.updated_at);
        let owner_epoch = sealed_checkpoint_states
            .iter()
            .map(|state| state.record.owner_epoch)
            .max()
            .unwrap_or(runtime_state.stream_owner_epoch);
        return Some(build_stream_job_checkpoint_record(
            handle,
            runtime_state,
            reached_at,
            owner_epoch,
            sealed_checkpoint_output(sealed_checkpoint_states),
        ));
    }
    if checkpoint_states.len() == runtime_state.active_partitions.len() {
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
        return Some(build_stream_job_checkpoint_record(
            handle,
            runtime_state,
            reached_at,
            owner_epoch,
            None,
        ));
    }
    None
}

fn build_stream_job_checkpoint_record(
    handle: &StreamJobBridgeHandleRecord,
    runtime_state: &LocalStreamJobRuntimeState,
    reached_at: DateTime<Utc>,
    owner_epoch: u64,
    sealed_output: Option<Vec<Value>>,
) -> StreamJobCheckpointRecord {
    let mut output = Map::new();
    output.insert("jobId".to_owned(), json!(handle.job_id));
    output.insert("checkpoint".to_owned(), json!(runtime_state.checkpoint_name));
    output.insert("checkpointSequence".to_owned(), json!(runtime_state.checkpoint_sequence));
    output.insert("viewName".to_owned(), json!(runtime_state.view_name));
    output.insert("activePartitions".to_owned(), json!(runtime_state.active_partitions));
    if let Some(sealed_output) = sealed_output.filter(|states| !states.is_empty()) {
        output.insert("sealed".to_owned(), Value::Array(sealed_output));
    }
    StreamJobCheckpointRecord {
        workflow_event_id: Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!(
                "stream-job-checkpoint:{}:{}:{}",
                handle.handle_id, runtime_state.checkpoint_name, runtime_state.checkpoint_sequence
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
        output: Some(Value::Object(output)),
        accepted_at: None,
        cancelled_at: None,
        created_at: runtime_state.planned_at,
        updated_at: reached_at,
    }
}

fn sealed_checkpoint_output(
    sealed_checkpoint_states: &[crate::local_state::LocalStreamJobSealedCheckpointState],
) -> Option<Vec<Value>> {
    if sealed_checkpoint_states.is_empty() {
        return None;
    }
    Some(
        sealed_checkpoint_states
            .iter()
            .map(|state| {
                json!({
                    "streamPartitionId": state.stream_partition_id,
                    "acceptedProgressPosition": state.record.accepted_progress_position,
                    "sealedAt": state.sealed_at.to_rfc3339(),
                })
            })
            .collect(),
    )
}

fn terminal_materialization_result(
    runtime_state: &LocalStreamJobRuntimeState,
    bridge_callbacks: &[LocalStreamJobBridgeCallbackState],
    terminal_ready: bool,
) -> Result<Option<StreamJobTerminalResult>> {
    if !terminal_ready {
        return Ok(None);
    }
    if let Some(callback_state) = bridge_callbacks.iter().find(|state| {
        state.record.callback_kind == STREAM_JOB_CALLBACK_KIND_TERMINAL
            && state
                .record
                .terminal_status
                .as_deref()
                .zip(runtime_state.terminal_status.as_deref())
                .is_some_and(|(callback_status, runtime_status)| callback_status == runtime_status)
    }) {
        return terminal_result_from_status(
            callback_state
                .record
                .terminal_status
                .as_deref()
                .expect("terminal callback record should carry terminal status"),
            runtime_state,
        )
        .map(Some);
    }
    runtime_state
        .terminal_status
        .as_deref()
        .map(|status| terminal_result_from_status(status, runtime_state))
        .transpose()
}

fn terminal_result_from_status(
    status: &str,
    runtime_state: &LocalStreamJobRuntimeState,
) -> Result<StreamJobTerminalResult> {
    match status {
        status if status == StreamJobBridgeHandleStatus::Completed.as_str() => {
            Ok(StreamJobTerminalResult {
                status: StreamJobBridgeHandleStatus::Completed,
                output: runtime_state.terminal_output.clone(),
                error: runtime_state.terminal_error.clone(),
            })
        }
        status if status == StreamJobBridgeHandleStatus::Failed.as_str() => {
            Ok(StreamJobTerminalResult {
                status: StreamJobBridgeHandleStatus::Failed,
                output: runtime_state.terminal_output.clone(),
                error: runtime_state.terminal_error.clone(),
            })
        }
        status if status == StreamJobBridgeHandleStatus::Cancelled.as_str() => {
            Ok(StreamJobTerminalResult {
                status: StreamJobBridgeHandleStatus::Cancelled,
                output: runtime_state.terminal_output.clone(),
                error: runtime_state.terminal_error.clone(),
            })
        }
        other => anyhow::bail!("unexpected materialized stream job terminal status {other}"),
    }
}

fn latest_accepted_progress_position(
    local_state: &LocalThroughputState,
    handle_id: &str,
    sealed_checkpoint_states: &[crate::local_state::LocalStreamJobSealedCheckpointState],
) -> Result<Option<u64>> {
    let cursor_position = local_state
        .load_stream_job_accepted_progress_cursor(handle_id)?
        .map(|cursor| cursor.latest_position);
    let sealed_position =
        sealed_checkpoint_states.iter().map(|state| state.record.accepted_progress_position).max();
    Ok(match (cursor_position, sealed_position) {
        (Some(cursor), Some(sealed)) => Some(cursor.max(sealed)),
        (Some(cursor), None) => Some(cursor),
        (None, Some(sealed)) => Some(sealed),
        (None, None) => None,
    })
}

pub(crate) fn sync_stream_job_bridge_callback_records_on_local_state(
    local_state: &LocalThroughputState,
    handle: &StreamJobBridgeHandleRecord,
) -> Result<()> {
    let Some(runtime_state) = local_state.load_stream_job_runtime_state(&handle.handle_id)? else {
        return Ok(());
    };
    let sealed_checkpoint_states =
        sealed_checkpoint_states_for_runtime(local_state, &runtime_state)?;
    let checkpoint_ready = !runtime_state.checkpoint_name.is_empty()
        && !sealed_checkpoint_states.is_empty()
        && sealed_checkpoint_states.len() == runtime_state.active_partitions.len();
    if checkpoint_ready {
        let owner_epoch = sealed_checkpoint_states
            .iter()
            .map(|state| state.record.owner_epoch)
            .max()
            .unwrap_or(runtime_state.stream_owner_epoch);
        let accepted_progress_position = sealed_checkpoint_states
            .iter()
            .map(|state| state.record.accepted_progress_position)
            .max()
            .unwrap_or_default();
        let created_at = sealed_checkpoint_states
            .iter()
            .map(|state| state.sealed_at)
            .max()
            .unwrap_or(runtime_state.updated_at);
        let callback_id = stream_job_checkpoint_callback_id(
            &handle.handle_id,
            &runtime_state.checkpoint_name,
            runtime_state.checkpoint_sequence,
        );
        local_state.upsert_stream_job_bridge_callback(&LocalStreamJobBridgeCallbackState {
            handle_id: handle.handle_id.clone(),
            callback_id: callback_id.clone(),
            record: DataflowBridgeCallbackRecord {
                callback_id,
                plan_id: format!("stream-job:{}", handle.handle_id),
                run_id: Some(handle.run_id.clone()),
                job_id: Some(handle.job_id.clone()),
                owner_epoch,
                callback_kind: STREAM_JOB_CALLBACK_KIND_CHECKPOINT.to_owned(),
                checkpoint: Some(StrongReadCheckpointRef {
                    checkpoint_name: runtime_state.checkpoint_name.clone(),
                    checkpoint_sequence: runtime_state.checkpoint_sequence,
                    sealed_at: Some(created_at),
                }),
                terminal_status: None,
                accepted_progress_position: Some(accepted_progress_position),
            },
            created_at,
        })?;
    }

    let terminal_ready = runtime_state.checkpoint_name.is_empty() || checkpoint_ready;
    if terminal_ready && let Some(status) = runtime_state.terminal_status.as_deref() {
        let created_at = runtime_state.terminal_at.unwrap_or(runtime_state.updated_at);
        let owner_epoch = sealed_checkpoint_states
            .iter()
            .map(|state| state.record.owner_epoch)
            .max()
            .unwrap_or(runtime_state.stream_owner_epoch);
        let callback_id = stream_job_terminal_callback_id(&handle.handle_id, status);
        local_state.upsert_stream_job_bridge_callback(&LocalStreamJobBridgeCallbackState {
            handle_id: handle.handle_id.clone(),
            callback_id: callback_id.clone(),
            record: DataflowBridgeCallbackRecord {
                callback_id,
                plan_id: format!("stream-job:{}", handle.handle_id),
                run_id: Some(handle.run_id.clone()),
                job_id: Some(handle.job_id.clone()),
                owner_epoch,
                callback_kind: STREAM_JOB_CALLBACK_KIND_TERMINAL.to_owned(),
                checkpoint: None,
                terminal_status: Some(status.to_owned()),
                accepted_progress_position: latest_accepted_progress_position(
                    local_state,
                    &handle.handle_id,
                    &sealed_checkpoint_states,
                )?,
            },
            created_at,
        })?;
    }
    Ok(())
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

fn query_logical_key(
    handle: &StreamJobBridgeHandleRecord,
    query: &StreamJobQueryRecord,
    view: &CompiledStreamView,
) -> Result<String> {
    let args = query.query_args.clone().unwrap_or(Value::Null);
    let base_key = args
        .get("key")
        .and_then(Value::as_str)
        .or_else(|| {
            view.key_field.as_deref().and_then(|field| args.get(field)).and_then(Value::as_str)
        })
        .map(str::to_owned)
        .context("stream job query requires args with a key field")?;
    if let Some(job) = parse_compiled_job_from_config_ref(handle)?
        && let Some(kernel) = job.aggregate_v2_kernel()?
        && kernel.window.is_some()
    {
        let window_start = query_window_start(query)
            .context("windowed stream job query requires args.windowStart")?;
        return Ok(windowed_logical_key(&base_key, &window_start));
    }
    Ok(base_key)
}

fn resolved_query_consistency(query: &StreamJobQueryRecord) -> String {
    if matches!(
        query.parsed_consistency(),
        Some(StreamJobQueryConsistency::Strong | StreamJobQueryConsistency::Eventual)
    ) {
        query.consistency.clone()
    } else {
        STREAM_CONSISTENCY_STRONG.to_owned()
    }
}

fn stream_scan_item_output(
    job: Option<&CompiledStreamJob>,
    view: &CompiledStreamView,
    logical_key: &str,
    checkpoint_sequence: i64,
    updated_at: DateTime<Utc>,
    output: Value,
) -> Result<Value> {
    let output = sanitize_stream_view_output(output);
    let mut object = match output {
        Value::Object(object) => object,
        other => {
            let mut object = Map::new();
            object.insert("value".to_owned(), other);
            object
        }
    };
    window_query_annotations(job, view, &mut object)?;
    object.insert("logicalKey".to_owned(), Value::String(logical_key.to_owned()));
    object.insert("checkpointSequence".to_owned(), Value::from(checkpoint_sequence));
    object.insert("updatedAt".to_owned(), Value::String(updated_at.to_rfc3339()));
    Ok(Value::Object(object))
}

fn stream_scan_output(
    query: &StreamJobQueryRecord,
    prefix: &str,
    total: usize,
    offset: usize,
    limit: usize,
    consistency: &str,
    consistency_source: &str,
    items: Vec<Value>,
    runtime_stats: Option<Value>,
    projection_stats: Option<Value>,
    strong_read_metadata: Option<Value>,
) -> Value {
    let mut output = Map::new();
    output.insert("queryName".to_owned(), Value::String(query.query_name.clone()));
    output.insert("prefix".to_owned(), Value::String(prefix.to_owned()));
    output.insert("total".to_owned(), Value::from(total));
    output.insert("offset".to_owned(), Value::from(offset));
    output.insert("limit".to_owned(), Value::from(limit));
    output.insert("items".to_owned(), Value::Array(items));
    output.insert("consistency".to_owned(), Value::String(consistency.to_owned()));
    output.insert("consistencySource".to_owned(), Value::String(consistency_source.to_owned()));
    if let Some(runtime_stats) = runtime_stats {
        output.insert("runtimeStats".to_owned(), runtime_stats);
    }
    if let Some(projection_stats) = projection_stats {
        output.insert("projectionStats".to_owned(), projection_stats);
    }
    if let Some(strong_read_metadata) = strong_read_metadata {
        output.insert("strongReadMetadata".to_owned(), strong_read_metadata);
    }
    Value::Object(output)
}

fn projection_summary_output(summary: Option<&StreamJobViewProjectionSummaryRecord>) -> Value {
    match summary {
        Some(summary) => json!({
            "keyCount": summary.key_count,
            "latestProjectedCheckpointSequence": summary.latest_checkpoint_sequence,
            "latestProjectedUpdatedAt": optional_datetime_value(summary.latest_updated_at),
            "latestDeletedCheckpointSequence": summary.latest_deleted_checkpoint_sequence,
            "latestDeletedAt": optional_datetime_value(summary.latest_deleted_at),
        }),
        None => json!({
            "keyCount": 0u64,
            "latestProjectedCheckpointSequence": Value::Null,
            "latestProjectedUpdatedAt": Value::Null,
            "latestDeletedCheckpointSequence": Value::Null,
            "latestDeletedAt": Value::Null,
        }),
    }
}

fn eventual_projection_freshness_output(
    summary: Option<&StreamJobViewProjectionSummaryRecord>,
    owner_runtime_state: Option<&LocalStreamJobRuntimeState>,
    requested_at: DateTime<Utc>,
) -> Value {
    let latest_projected_checkpoint_sequence =
        summary.and_then(|summary| summary.latest_checkpoint_sequence);
    let latest_projected_updated_at = summary.and_then(|summary| summary.latest_updated_at);
    let owner_checkpoint_sequence = owner_runtime_state.map(|state| state.checkpoint_sequence);
    let owner_latest_checkpoint_at =
        owner_runtime_state.and_then(|state| state.latest_checkpoint_at);
    json!({
        "latestProjectedCheckpointSequence": latest_projected_checkpoint_sequence,
        "latestProjectedUpdatedAt": optional_datetime_value(latest_projected_updated_at),
        "latestDeletedCheckpointSequence": summary.and_then(|summary| summary.latest_deleted_checkpoint_sequence),
        "latestDeletedAt": optional_datetime_value(summary.and_then(|summary| summary.latest_deleted_at)),
        "ownerCheckpointSequence": owner_checkpoint_sequence,
        "ownerLatestCheckpointAt": optional_datetime_value(owner_latest_checkpoint_at),
        "checkpointSequenceLag": owner_checkpoint_sequence
            .zip(latest_projected_checkpoint_sequence)
            .map(|(owner, projected)| Value::from(owner.saturating_sub(projected)))
            .unwrap_or(Value::Null),
        "projectedUpdateAgeSeconds": optional_duration_seconds(
            latest_projected_updated_at.map(|updated_at| requested_at - updated_at)
        ),
        "ownerCheckpointAgeSeconds": optional_duration_seconds(
            owner_latest_checkpoint_at.map(|updated_at| requested_at - updated_at)
        ),
    })
}

async fn eventual_projection_stats_output(
    state: &AppState,
    handle: &StreamJobBridgeHandleRecord,
    view: &CompiledStreamView,
    requested_at: DateTime<Utc>,
) -> Result<Option<Value>> {
    if !view_supports_eventual_reads(view) {
        return Ok(None);
    }
    let projection_summary = load_eventual_projection_summary(state, handle, &view.name).await?;
    let owner_runtime_state = owner_local_state_for_stream_job(state, &handle.job_id)
        .load_stream_job_runtime_state(&handle.handle_id)?;
    Ok(Some(json!({
        "supported": true,
        "rebuildSupported": true,
        "summary": projection_summary_output(projection_summary.as_ref()),
        "freshness": eventual_projection_freshness_output(
            projection_summary.as_ref(),
            owner_runtime_state.as_ref(),
            requested_at,
        ),
    })))
}

pub(crate) async fn stream_job_view_projection_stats_output(
    state: &AppState,
    handle: &StreamJobBridgeHandleRecord,
    view_name: &str,
    requested_at: DateTime<Utc>,
) -> Result<Option<Value>> {
    let Some(job) = parse_compiled_job_from_config_ref(handle)? else {
        return Ok(None);
    };
    let Some(view) = job.views.iter().find(|view| view.name == view_name) else {
        return Ok(None);
    };
    Ok(Some(
        eventual_projection_stats_output(state, handle, view, requested_at).await?.unwrap_or_else(
            || {
                json!({
                    "supported": false,
                    "rebuildSupported": false,
                    "summary": Value::Null,
                    "freshness": Value::Null,
                })
            },
        ),
    ))
}

async fn load_eventual_projection_summary(
    state: &AppState,
    handle: &StreamJobBridgeHandleRecord,
    view_name: &str,
) -> Result<Option<StreamJobViewProjectionSummaryRecord>> {
    state
        .store
        .get_stream_job_view_projection_summary(
            &handle.tenant_id,
            &handle.instance_id,
            &handle.run_id,
            &handle.job_id,
            view_name,
        )
        .await
}

fn query_base_key(query: &StreamJobQueryRecord, view: &CompiledStreamView) -> Option<String> {
    let args = query.query_args.as_ref()?;
    args.get("key")
        .and_then(Value::as_str)
        .or_else(|| {
            view.key_field.as_deref().and_then(|field| args.get(field)).and_then(Value::as_str)
        })
        .map(str::to_owned)
}

fn owner_view_state_matches_query(
    view_state: &LocalStreamJobViewState,
    view: &CompiledStreamView,
    query: &StreamJobQueryRecord,
    logical_key: &str,
) -> bool {
    if view_state.logical_key == logical_key {
        return true;
    }

    let Some(base_key) = query_base_key(query, view) else {
        return false;
    };
    let Some(output) = view_state.output.as_object() else {
        return false;
    };
    let Some(key_field) = view.key_field.as_deref() else {
        return false;
    };
    let Some(output_key) = output.get(key_field).and_then(Value::as_str) else {
        return false;
    };
    if output_key != base_key {
        return false;
    }

    match query_window_start(query) {
        Some(window_start) => output
            .get("windowStart")
            .and_then(Value::as_str)
            .is_some_and(|candidate| candidate == window_start),
        None => true,
    }
}

async fn load_or_hydrate_eventual_view_record(
    state: &AppState,
    handle: &StreamJobBridgeHandleRecord,
    view: &CompiledStreamView,
    query: &StreamJobQueryRecord,
    logical_key: &str,
) -> Result<Option<StreamJobViewRecord>> {
    if let Some(record) = state
        .store
        .get_stream_job_view_query(
            &handle.tenant_id,
            &handle.instance_id,
            &handle.run_id,
            &handle.job_id,
            &view.name,
            logical_key,
        )
        .await?
    {
        return Ok(Some(record));
    }

    let owner_local_state = owner_local_state_for_stream_job(state, &handle.job_id);
    if let Some(view_state) =
        owner_local_state.load_stream_job_view_state(&handle.handle_id, &view.name, logical_key)?
    {
        let record = public_projection_view_record(handle, &view.name, &view_state);
        state.store.upsert_stream_job_view_query(&record).await?;
        return Ok(Some(record));
    }

    let owner_rows =
        owner_local_state.load_stream_job_views_for_view(&handle.handle_id, &view.name)?;
    let matching_view_state = owner_rows
        .into_iter()
        .find(|view_state| owner_view_state_matches_query(view_state, view, query, logical_key));
    let Some(view_state) = matching_view_state else {
        return Ok(None);
    };
    let record = public_projection_view_record(handle, &view.name, &view_state);
    state.store.upsert_stream_job_view_query(&record).await?;
    Ok(Some(record))
}

fn public_projection_view_record(
    handle: &StreamJobBridgeHandleRecord,
    view_name: &str,
    view_state: &LocalStreamJobViewState,
) -> StreamJobViewRecord {
    StreamJobViewRecord {
        tenant_id: handle.tenant_id.clone(),
        instance_id: handle.instance_id.clone(),
        run_id: handle.run_id.clone(),
        job_id: handle.job_id.clone(),
        handle_id: handle.handle_id.clone(),
        view_name: view_name.to_owned(),
        logical_key: view_state.logical_key.clone(),
        output: sanitize_stream_view_output(view_state.output.clone()),
        checkpoint_sequence: view_state.checkpoint_sequence,
        updated_at: view_state.updated_at,
    }
}

async fn load_all_projected_view_rows(
    state: &AppState,
    handle: &StreamJobBridgeHandleRecord,
    view_name: &str,
) -> Result<Vec<StreamJobViewRecord>> {
    let mut offset = 0i64;
    let page_size = 512i64;
    let mut rows = Vec::new();
    loop {
        let page = state
            .store
            .list_stream_job_view_query_page(
                &handle.tenant_id,
                &handle.instance_id,
                &handle.run_id,
                &handle.job_id,
                view_name,
                None,
                page_size,
                offset,
            )
            .await?;
        if page.is_empty() {
            break;
        }
        offset = offset.saturating_add(i64::try_from(page.len()).unwrap_or(page_size));
        rows.extend(page);
    }
    Ok(rows)
}

pub(crate) async fn rebuild_stream_job_eventual_projections(
    state: &AppState,
    handle: &StreamJobBridgeHandleRecord,
    requested_view_name: Option<&str>,
    requested_at: DateTime<Utc>,
) -> Result<Value> {
    let Some(job) = parse_compiled_job_from_config_ref(handle)? else {
        anyhow::bail!(
            "stream job {} has no compiled config to rebuild projections from",
            handle.job_id
        );
    };
    let owner_local_state = owner_local_state_for_stream_job(state, &handle.job_id);
    let owner_runtime_state = owner_local_state.load_stream_job_runtime_state(&handle.handle_id)?;
    let mut view_results = Vec::new();
    for view in job.views.iter().filter(|view| {
        view_supports_eventual_reads(view)
            && requested_view_name.is_none_or(|view_name| view.name == view_name)
    }) {
        let owner_rows = owner_local_state
            .load_stream_job_views_for_view(&handle.handle_id, &view.name)?
            .into_iter()
            .filter(|view_state| {
                !window_expired_from_deadline(
                    view_window_expired_at(Some(&job), view, &view_state.output).ok().flatten(),
                    requested_at,
                )
            })
            .collect::<Vec<_>>();
        let projected_rows = load_all_projected_view_rows(state, handle, &view.name).await?;
        let owner_by_key = owner_rows
            .iter()
            .map(|view_state| {
                (
                    view_state.logical_key.clone(),
                    public_projection_view_record(handle, &view.name, view_state),
                )
            })
            .collect::<HashMap<_, _>>();
        let projected_by_key = projected_rows
            .iter()
            .map(|row| (row.logical_key.clone(), row))
            .collect::<HashMap<_, _>>();
        let mut upserted = 0usize;
        let mut deleted = 0usize;

        for row in owner_by_key.values() {
            state.store.upsert_stream_job_view_query(row).await?;
            upserted = upserted.saturating_add(1);
        }
        let delete_checkpoint_sequence = owner_runtime_state
            .as_ref()
            .map(|runtime_state| runtime_state.checkpoint_sequence)
            .unwrap_or_default();
        for (logical_key, projected_row) in projected_by_key {
            if owner_by_key.contains_key(&logical_key) {
                continue;
            }
            state
                .store
                .delete_stream_job_view_query(&StreamJobViewDeleteRecord {
                    tenant_id: handle.tenant_id.clone(),
                    instance_id: handle.instance_id.clone(),
                    run_id: handle.run_id.clone(),
                    job_id: handle.job_id.clone(),
                    handle_id: handle.handle_id.clone(),
                    view_name: view.name.clone(),
                    logical_key,
                    checkpoint_sequence: delete_checkpoint_sequence
                        .max(projected_row.checkpoint_sequence),
                    evicted_at: requested_at,
                })
                .await?;
            deleted = deleted.saturating_add(1);
        }

        let projection_summary =
            load_eventual_projection_summary(state, handle, &view.name).await?;
        view_results.push(json!({
            "viewName": view.name,
            "upsertedCount": upserted,
            "deletedCount": deleted,
            "projectionSummary": projection_summary_output(projection_summary.as_ref()),
            "freshness": eventual_projection_freshness_output(
                projection_summary.as_ref(),
                owner_runtime_state.as_ref(),
                requested_at,
            ),
        }));
    }
    Ok(json!({
        "jobId": handle.job_id,
        "handleId": handle.handle_id,
        "viewName": requested_view_name,
        "rebuiltAt": requested_at.to_rfc3339(),
        "views": view_results,
    }))
}

pub(crate) async fn build_stream_job_query_output(
    state: &AppState,
    handle: &StreamJobBridgeHandleRecord,
    query: &StreamJobQueryRecord,
) -> Result<Option<Value>> {
    if query.query_name == STREAM_JOB_BRIDGE_STATE_QUERY_NAME {
        return build_stream_job_bridge_state_output(state, handle, query).await;
    }
    if query.query_name == STREAM_JOB_PROJECTION_STATS_QUERY_NAME {
        return build_stream_job_projection_stats_output(state, handle, query).await;
    }
    if query.query_name == STREAM_JOB_RUNTIME_STATS_QUERY_NAME {
        return build_stream_job_runtime_stats_output_on_local_state(
            owner_local_state_for_stream_job(state, &handle.job_id),
            handle,
            query,
        );
    }
    if query.query_name == STREAM_JOB_VIEW_RUNTIME_STATS_QUERY_NAME {
        return build_stream_job_view_runtime_stats_output(state, handle, query).await;
    }
    if query.query_name == STREAM_JOB_VIEW_PROJECTION_STATS_QUERY_NAME {
        return build_stream_job_view_projection_stats_output(state, handle, query).await;
    }
    let Some(view) = resolve_query_view(handle, &query.query_name)? else {
        return Ok(None);
    };
    let job = parse_compiled_job_from_config_ref(handle)?;
    if view.query_mode == STREAM_QUERY_MODE_PREFIX_SCAN {
        let projection_stats =
            eventual_projection_stats_output(state, handle, &view, query.requested_at).await?;
        if matches!(query.parsed_consistency(), Some(StreamJobQueryConsistency::Eventual)) {
            let prefix = query_scan_prefix(query).unwrap_or_default();
            let offset = query_page_offset(query);
            let limit = query_page_limit(query);
            let total = state
                .store
                .count_stream_job_view_query_keys(
                    &handle.tenant_id,
                    &handle.instance_id,
                    &handle.run_id,
                    &handle.job_id,
                    &view.name,
                    Some(prefix.as_str()),
                )
                .await? as usize;
            let records = state
                .store
                .list_stream_job_view_query_page(
                    &handle.tenant_id,
                    &handle.instance_id,
                    &handle.run_id,
                    &handle.job_id,
                    &view.name,
                    Some(prefix.as_str()),
                    i64::try_from(limit).context("stream scan query limit exceeds i64")?,
                    i64::try_from(offset).context("stream scan query offset exceeds i64")?,
                )
                .await?;
            let mut items = Vec::new();
            for record in records {
                if window_expired_from_deadline(
                    view_window_expired_at(job.as_ref(), &view, &record.output)?,
                    query.requested_at,
                ) {
                    continue;
                }
                items.push(stream_scan_item_output(
                    job.as_ref(),
                    &view,
                    &record.logical_key,
                    record.checkpoint_sequence,
                    record.updated_at,
                    record.output,
                )?);
            }
            return Ok(Some(stream_scan_output(
                query,
                &prefix,
                total,
                offset,
                limit,
                StreamJobQueryConsistency::Eventual.as_str(),
                "stream_projection_query",
                items,
                None,
                projection_stats,
                None,
            )));
        }
        let Some(mut output) = build_stream_job_query_output_on_local_state(
            owner_local_state_for_stream_job(state, &handle.job_id),
            handle,
            query,
        )?
        else {
            return Ok(None);
        };
        if let Some(projection_stats) = projection_stats
            && let Some(object) = output.as_object_mut()
        {
            object.insert("projectionStats".to_owned(), projection_stats);
        }
        return Ok(Some(output));
    }
    let logical_key = query_logical_key(handle, query, &view)?;
    if matches!(query.parsed_consistency(), Some(StreamJobQueryConsistency::Eventual)) {
        let owner_runtime_state = owner_local_state_for_stream_job(state, &handle.job_id)
            .load_stream_job_runtime_state(&handle.handle_id)?;
        let Some(record) =
            load_or_hydrate_eventual_view_record(state, handle, &view, query, &logical_key).await?
        else {
            anyhow::bail!("{} key {} is not materialized", query.query_name, logical_key);
        };
        let projection_summary =
            load_eventual_projection_summary(state, handle, &view.name).await?;
        if window_expired_from_deadline(
            view_window_expired_at(job.as_ref(), &view, &record.output)?,
            query.requested_at,
        ) {
            anyhow::bail!("{} key {} is not materialized", query.query_name, logical_key);
        }
        let mut output = sanitize_stream_view_output(record.output);
        if let Some(object) = output.as_object_mut() {
            window_query_annotations(job.as_ref(), &view, object)?;
            object.insert(
                "consistency".to_owned(),
                Value::String(StreamJobQueryConsistency::Eventual.as_str().to_owned()),
            );
            object.insert(
                "consistencySource".to_owned(),
                Value::String("stream_projection_query".to_owned()),
            );
            object.insert("checkpointSequence".to_owned(), Value::from(record.checkpoint_sequence));
            object.insert(
                "projectionStats".to_owned(),
                json!({
                    "summary": projection_summary_output(projection_summary.as_ref()),
                    "freshness": eventual_projection_freshness_output(
                        projection_summary.as_ref(),
                        owner_runtime_state.as_ref(),
                        query.requested_at,
                    ),
                }),
            );
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
    if query.query_name == STREAM_JOB_BRIDGE_STATE_QUERY_NAME {
        anyhow::bail!("{STREAM_JOB_BRIDGE_STATE_QUERY_NAME} requires bridge store access");
    }
    if query.query_name == STREAM_JOB_PROJECTION_STATS_QUERY_NAME {
        anyhow::bail!("{STREAM_JOB_PROJECTION_STATS_QUERY_NAME} requires projection store access");
    }
    if query.query_name == STREAM_JOB_RUNTIME_STATS_QUERY_NAME {
        return build_stream_job_runtime_stats_output_on_local_state(local_state, handle, query);
    }
    if query.query_name == STREAM_JOB_VIEW_RUNTIME_STATS_QUERY_NAME {
        return build_stream_job_view_runtime_stats_output_on_local_state(
            local_state,
            handle,
            query,
        );
    }
    if query.query_name == STREAM_JOB_VIEW_PROJECTION_STATS_QUERY_NAME {
        anyhow::bail!(
            "{STREAM_JOB_VIEW_PROJECTION_STATS_QUERY_NAME} requires projection store access"
        );
    }
    let Some(view) = resolve_query_view(handle, &query.query_name)? else {
        return Ok(None);
    };
    let job = parse_compiled_job_from_config_ref(handle)?;
    if view.query_mode == STREAM_QUERY_MODE_PREFIX_SCAN {
        let prefix = query_scan_prefix(query).unwrap_or_default();
        let offset = query_page_offset(query);
        let limit = query_page_limit(query);
        let mut matching_views = local_state
            .load_stream_job_views_for_view(&handle.handle_id, &view.name)?
            .into_iter()
            .filter(|view_state| view_state.logical_key.starts_with(&prefix))
            .collect::<Vec<_>>();
        matching_views.sort_by(|left, right| left.logical_key.cmp(&right.logical_key));
        let mut total = 0usize;
        let mut items = Vec::new();
        for view_state in matching_views {
            if window_expired_from_deadline(
                view_window_expired_at(job.as_ref(), &view, &view_state.output)?,
                query.requested_at,
            ) {
                continue;
            }
            if total >= offset && items.len() < limit {
                items.push(stream_scan_item_output(
                    job.as_ref(),
                    &view,
                    &view_state.logical_key,
                    view_state.checkpoint_sequence,
                    view_state.updated_at,
                    view_state.output,
                )?);
            }
            total = total.saturating_add(1);
        }
        let runtime_state = local_state.load_stream_job_runtime_state(&handle.handle_id)?;
        let runtime_stats = runtime_state
            .as_ref()
            .map(|runtime_state| {
                build_stream_job_runtime_stats_output_from_runtime_state(
                    local_state,
                    runtime_state,
                    handle,
                    query,
                )
            })
            .transpose()?;
        let strong_read_metadata = if resolved_query_consistency(query) == STREAM_CONSISTENCY_STRONG
        {
            runtime_state
                .as_ref()
                .map(|runtime_state| {
                    strong_read_metadata_value(local_state, runtime_state, &handle.handle_id)
                })
                .transpose()?
        } else {
            None
        };
        return Ok(Some(stream_scan_output(
            query,
            &prefix,
            total,
            offset,
            limit,
            &resolved_query_consistency(query),
            "stream_owner_local_state",
            items,
            runtime_stats,
            None,
            strong_read_metadata,
        )));
    }
    if view.query_mode != STREAM_QUERY_MODE_BY_KEY {
        anyhow::bail!("unsupported stream job query mode {}", view.query_mode);
    }

    let logical_key = query_logical_key(handle, query, &view)?;
    let Some(view_state) =
        local_state.load_stream_job_view_state(&handle.handle_id, &view.name, &logical_key)?
    else {
        anyhow::bail!("{} key {} is not materialized", query.query_name, logical_key);
    };
    if window_expired_from_deadline(
        view_window_expired_at(job.as_ref(), &view, &view_state.output)?,
        query.requested_at,
    ) {
        anyhow::bail!("{} key {} is not materialized", query.query_name, logical_key);
    }

    let mut output = sanitize_stream_view_output(view_state.output);
    let runtime_state = if resolved_query_consistency(query) == STREAM_CONSISTENCY_STRONG {
        local_state.load_stream_job_runtime_state(&handle.handle_id)?
    } else {
        None
    };
    if let Some(object) = output.as_object_mut() {
        window_query_annotations(job.as_ref(), &view, object)?;
        object.insert("consistency".to_owned(), Value::String(query.consistency.clone()));
        object.insert(
            "consistencySource".to_owned(),
            Value::String("stream_owner_local_state".to_owned()),
        );
        object.insert("checkpointSequence".to_owned(), Value::from(view_state.checkpoint_sequence));
        if let Some(owner_epoch) = handle.stream_owner_epoch {
            object.insert("streamOwnerEpoch".to_owned(), Value::from(owner_epoch));
        }
        object.insert("consistency".to_owned(), Value::String(resolved_query_consistency(query)));
        if let Some(runtime_state) = runtime_state.as_ref() {
            object.insert(
                "strongReadMetadata".to_owned(),
                strong_read_metadata_value(local_state, runtime_state, &handle.handle_id)?,
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
    use crate::local_state::{LocalChangelogPlane, LocalStreamJobCheckpointState};
    use chrono::Timelike;
    use fabrik_throughput::{
        STREAM_CHECKPOINT_DELIVERY_WORKFLOW_AWAITABLE, STREAM_CHECKPOINT_POLICY_NAMED,
        STREAM_OPERATOR_EMIT_CHECKPOINT, STREAM_OPERATOR_MATERIALIZE, STREAM_OPERATOR_REDUCE,
        STREAM_OPERATOR_SIGNAL_WORKFLOW, StreamsChangelogEntry, StreamsChangelogPayload,
    };
    use std::{hint::black_box, path::PathBuf, time::Instant};

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

    fn topic_keyed_rollup_handle(topic_name: &str) -> StreamJobBridgeHandleRecord {
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
            definition_id: "payments-rollup".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact".to_owned()),
            job_name: KEYED_ROLLUP_JOB_NAME.to_owned(),
            input_ref: json!({
                "kind": "topic",
                "topic": topic_name,
                "startOffset": "earliest"
            })
            .to_string(),
            config_ref: Some(
                json!({
                    "name": KEYED_ROLLUP_JOB_NAME,
                    "runtime": STREAM_RUNTIME_KEYED_ROLLUP,
                    "source": {
                        "kind": STREAM_SOURCE_TOPIC,
                        "name": topic_name,
                    },
                    "keyBy": "accountId",
                    "operators": [
                        {
                            "kind": STREAM_OPERATOR_REDUCE,
                            "name": "reduce-sum",
                            "config": {
                                "reducer": STREAM_REDUCER_SUM,
                                "valueField": "amount",
                                "outputField": "totalAmount"
                            }
                        },
                        {
                            "kind": STREAM_OPERATOR_MATERIALIZE,
                            "name": "materialize-account-totals",
                            "config": {
                                "view": "accountTotals"
                            }
                        },
                        {
                            "kind": STREAM_OPERATOR_EMIT_CHECKPOINT,
                            "name": KEYED_ROLLUP_CHECKPOINT_NAME,
                            "config": {
                                "sequence": KEYED_ROLLUP_CHECKPOINT_SEQUENCE
                            }
                        }
                    ],
                    "views": [
                        {
                            "name": "accountTotals",
                            "consistency": STREAM_CONSISTENCY_STRONG,
                            "queryMode": STREAM_QUERY_MODE_BY_KEY,
                            "keyField": "accountId"
                        }
                    ],
                    "queries": [
                        {
                            "name": "accountTotals",
                            "viewName": "accountTotals",
                            "consistency": STREAM_CONSISTENCY_STRONG
                        }
                    ],
                    "checkpointPolicy": {
                        "kind": STREAM_CHECKPOINT_POLICY_NAMED,
                        "checkpoints": [
                            {
                                "name": KEYED_ROLLUP_CHECKPOINT_NAME,
                                "delivery": STREAM_CHECKPOINT_DELIVERY_WORKFLOW_AWAITABLE,
                                "sequence": KEYED_ROLLUP_CHECKPOINT_SEQUENCE
                            }
                        ]
                    }
                })
                .to_string(),
            ),
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

    fn keyed_rollup_query(
        handle: &StreamJobBridgeHandleRecord,
        query_name: &str,
        key: &str,
    ) -> StreamJobQueryRecord {
        let now = Utc::now();
        StreamJobQueryRecord {
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            workflow_event_id: Uuid::now_v7(),
            tenant_id: handle.tenant_id.clone(),
            instance_id: handle.instance_id.clone(),
            run_id: handle.run_id.clone(),
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            bridge_request_id: handle.bridge_request_id.clone(),
            query_id: format!("query-{}", Uuid::now_v7()),
            query_name: query_name.to_owned(),
            query_args: Some(json!({"key": key})),
            consistency: StreamJobQueryConsistency::Strong.as_str().to_owned(),
            status: fabrik_throughput::StreamJobQueryStatus::Completed.as_str().to_owned(),
            workflow_owner_epoch: handle.workflow_owner_epoch,
            stream_owner_epoch: handle.stream_owner_epoch,
            output: None,
            error: None,
            requested_at: now,
            completed_at: None,
            accepted_at: None,
            cancelled_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn eventual_keyed_query(
        handle: &StreamJobBridgeHandleRecord,
        query_name: &str,
        key: &str,
        window_start: Option<&str>,
    ) -> StreamJobQueryRecord {
        let now = Utc::now();
        let mut query_args = serde_json::Map::new();
        query_args.insert("key".to_owned(), Value::String(key.to_owned()));
        if let Some(window_start) = window_start {
            query_args.insert("windowStart".to_owned(), Value::String(window_start.to_owned()));
        }
        StreamJobQueryRecord {
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            workflow_event_id: Uuid::now_v7(),
            tenant_id: handle.tenant_id.clone(),
            instance_id: handle.instance_id.clone(),
            run_id: handle.run_id.clone(),
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            bridge_request_id: handle.bridge_request_id.clone(),
            query_id: format!("query-{}", Uuid::now_v7()),
            query_name: query_name.to_owned(),
            query_args: Some(Value::Object(query_args)),
            consistency: StreamJobQueryConsistency::Eventual.as_str().to_owned(),
            status: fabrik_throughput::StreamJobQueryStatus::Completed.as_str().to_owned(),
            workflow_owner_epoch: handle.workflow_owner_epoch,
            stream_owner_epoch: handle.stream_owner_epoch,
            output: None,
            error: None,
            requested_at: now,
            completed_at: None,
            accepted_at: None,
            cancelled_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn windowed_scan_handle() -> StreamJobBridgeHandleRecord {
        let now = Utc::now();
        StreamJobBridgeHandleRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            tenant_id: "tenant-a".to_owned(),
            instance_id: "instance-a".to_owned(),
            run_id: "run-a".to_owned(),
            stream_instance_id: "stream-a".to_owned(),
            stream_run_id: "stream-run-a".to_owned(),
            job_id: "job-a".to_owned(),
            handle_id: "handle-a".to_owned(),
            bridge_request_id: "bridge-a".to_owned(),
            origin_kind: StreamJobOriginKind::Workflow.as_str().to_owned(),
            definition_id: "fraud-detector".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-a".to_owned()),
            job_name: "fraud-detector".to_owned(),
            input_ref: json!({
                "kind": "topic",
                "topic": "payments",
                "startOffset": "earliest"
            })
            .to_string(),
            config_ref: Some(
                json!({
                    "name": "fraud-detector",
                    "runtime": "aggregate_v2",
                    "source": {
                        "kind": "topic",
                        "name": "payments"
                    },
                    "keyBy": "accountId",
                    "states": [
                        {
                            "id": "minute-window",
                            "kind": "window",
                            "keyFields": ["accountId", "windowStart"],
                            "valueFields": ["avgRisk"],
                            "retentionSeconds": 120
                        },
                        {
                            "id": "risk-state",
                            "kind": "keyed",
                            "keyFields": ["accountId", "windowStart"],
                            "valueFields": ["avgRisk"],
                            "retentionSeconds": 120
                        }
                    ],
                    "operators": [
                        {
                            "kind": "route",
                            "operatorId": "bucket-risk",
                            "config": {
                                "outputField": "riskBucket",
                                "branches": [
                                    { "predicate": "risk >= 0.8", "value": "high" },
                                    { "predicate": "risk >= 0.5", "value": "medium" }
                                ],
                                "defaultValue": "low"
                            }
                        },
                        {
                            "kind": "filter",
                            "operatorId": "filter-positive",
                            "config": {
                                "predicate": "amount > 0"
                            }
                        },
                        {
                            "kind": "map",
                            "operatorId": "normalize-risk",
                            "config": {
                                "inputField": "riskPoints",
                                "outputField": "risk",
                                "multiplyBy": 0.01
                            }
                        },
                        {
                            "kind": "window",
                            "operatorId": "minute-window",
                            "config": {
                                "mode": "tumbling",
                                "size": "1m",
                                "timeField": "eventTime",
                                "allowedLateness": "10s"
                            },
                            "stateIds": ["minute-window"]
                        },
                        {
                            "kind": "aggregate",
                            "operatorId": "avg-risk",
                            "config": {
                                "reducer": "avg",
                                "valueField": "risk",
                                "outputField": "avgRisk"
                            },
                            "stateIds": ["risk-state"]
                        },
                        {
                            "kind": "materialize",
                            "operatorId": "materialize-risk",
                            "config": {
                                "view": "riskScores"
                            },
                            "stateIds": ["risk-state"]
                        }
                    ],
                    "views": [
                        {
                            "name": "riskScores",
                            "consistency": "strong",
                            "queryMode": "prefix_scan",
                            "keyField": "accountId",
                            "supportedConsistencies": ["strong", "eventual"],
                            "retentionSeconds": 120
                        }
                    ],
                    "queries": [
                        {
                            "name": "riskScoresScan",
                            "viewName": "riskScores",
                            "consistency": "strong",
                            "argFields": ["prefix", "limit", "offset"]
                        },
                        {
                            "name": "riskScoresByKey",
                            "viewName": "riskScores",
                            "consistency": "strong",
                            "argFields": ["accountId", "windowStart"]
                        }
                    ]
                })
                .to_string(),
            ),
            checkpoint_policy: None,
            view_definitions: None,
            status: StreamJobBridgeHandleStatus::Running.as_str().to_owned(),
            workflow_owner_epoch: Some(1),
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

    fn strong_scan_query(
        handle: &StreamJobBridgeHandleRecord,
        prefix: &str,
    ) -> StreamJobQueryRecord {
        let now = Utc::now();
        StreamJobQueryRecord {
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            workflow_event_id: Uuid::now_v7(),
            tenant_id: handle.tenant_id.clone(),
            instance_id: handle.instance_id.clone(),
            run_id: handle.run_id.clone(),
            job_id: handle.job_id.clone(),
            handle_id: handle.handle_id.clone(),
            bridge_request_id: handle.bridge_request_id.clone(),
            query_id: format!("query-{}", Uuid::now_v7()),
            query_name: "riskScoresScan".to_owned(),
            query_args: Some(json!({
                "prefix": prefix,
                "limit": 1,
                "offset": 1
            })),
            consistency: StreamJobQueryConsistency::Strong.as_str().to_owned(),
            status: fabrik_throughput::StreamJobQueryStatus::Completed.as_str().to_owned(),
            workflow_owner_epoch: handle.workflow_owner_epoch,
            stream_owner_epoch: handle.stream_owner_epoch,
            output: None,
            error: None,
            requested_at: now,
            completed_at: None,
            accepted_at: None,
            cancelled_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn eventual_scan_query(
        handle: &StreamJobBridgeHandleRecord,
        prefix: &str,
    ) -> StreamJobQueryRecord {
        let mut query = strong_scan_query(handle, prefix);
        query.consistency = StreamJobQueryConsistency::Eventual.as_str().to_owned();
        query
    }

    fn stream_job_runtime_stats_query(
        handle: &StreamJobBridgeHandleRecord,
        source_partition_id: Option<i32>,
    ) -> StreamJobQueryRecord {
        let now = Utc::now();
        StreamJobQueryRecord {
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
            workflow_event_id: Uuid::now_v7(),
            query_id: format!("query-{}", Uuid::now_v7()),
            query_name: STREAM_JOB_RUNTIME_STATS_QUERY_NAME.to_owned(),
            query_args: source_partition_id.map(|source_partition_id| {
                json!({
                    "sourcePartitionId": source_partition_id
                })
            }),
            consistency: StreamJobQueryConsistency::Strong.as_str().to_owned(),
            status: fabrik_throughput::StreamJobQueryStatus::Completed.as_str().to_owned(),
            workflow_owner_epoch: handle.workflow_owner_epoch,
            stream_owner_epoch: handle.stream_owner_epoch,
            output: None,
            error: None,
            requested_at: now,
            completed_at: None,
            accepted_at: None,
            cancelled_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn stream_job_view_runtime_stats_query(
        handle: &StreamJobBridgeHandleRecord,
        view_name: &str,
    ) -> StreamJobQueryRecord {
        let now = Utc::now();
        StreamJobQueryRecord {
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
            workflow_event_id: Uuid::now_v7(),
            query_id: format!("query-{}", Uuid::now_v7()),
            query_name: STREAM_JOB_VIEW_RUNTIME_STATS_QUERY_NAME.to_owned(),
            query_args: Some(json!({
                "viewName": view_name
            })),
            consistency: StreamJobQueryConsistency::Strong.as_str().to_owned(),
            status: fabrik_throughput::StreamJobQueryStatus::Completed.as_str().to_owned(),
            workflow_owner_epoch: handle.workflow_owner_epoch,
            stream_owner_epoch: handle.stream_owner_epoch,
            output: None,
            error: None,
            requested_at: now,
            completed_at: None,
            accepted_at: None,
            cancelled_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn stream_job_view_projection_stats_query(
        handle: &StreamJobBridgeHandleRecord,
        view_name: &str,
    ) -> StreamJobQueryRecord {
        let now = Utc::now();
        StreamJobQueryRecord {
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
            workflow_event_id: Uuid::now_v7(),
            query_id: format!("query-{}", Uuid::now_v7()),
            query_name: STREAM_JOB_VIEW_PROJECTION_STATS_QUERY_NAME.to_owned(),
            query_args: Some(json!({
                "viewName": view_name
            })),
            consistency: StreamJobQueryConsistency::Strong.as_str().to_owned(),
            status: fabrik_throughput::StreamJobQueryStatus::Completed.as_str().to_owned(),
            workflow_owner_epoch: handle.workflow_owner_epoch,
            stream_owner_epoch: handle.stream_owner_epoch,
            output: None,
            error: None,
            requested_at: now,
            completed_at: None,
            accepted_at: None,
            cancelled_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn keyed_rollup_input(
        total_items: usize,
        distinct_keys: usize,
        hot_key_ratio: f64,
    ) -> (String, String) {
        let hot_key = "acct_hot".to_owned();
        let hot_item_count = ((total_items as f64) * hot_key_ratio).round() as usize;
        let key_count = distinct_keys.max(1);
        let mut items = Vec::with_capacity(total_items);
        for _ in 0..hot_item_count.min(total_items) {
            items.push(json!({
                "accountId": hot_key,
                "amount": 1.0
            }));
        }
        for index in hot_item_count.min(total_items)..total_items {
            items.push(json!({
                "accountId": format!("acct_{:05}", index % key_count),
                "amount": 1.0
            }));
        }
        let query_key = if hot_item_count > 0 { hot_key } else { "acct_00000".to_owned() };
        (json!({ "kind": "bounded_items", "items": items }).to_string(), query_key)
    }

    fn aggregate_v2_windowed_perf_handle(input_ref: &str) -> StreamJobBridgeHandleRecord {
        let now = Utc::now();
        StreamJobBridgeHandleRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            tenant_id: "tenant-bench".to_owned(),
            instance_id: "instance-bench".to_owned(),
            run_id: "run-bench".to_owned(),
            stream_instance_id: "stream-bench".to_owned(),
            stream_run_id: "stream-run-bench".to_owned(),
            job_id: "job-bench".to_owned(),
            handle_id: "handle-bench".to_owned(),
            bridge_request_id: "bridge-bench".to_owned(),
            origin_kind: StreamJobOriginKind::Workflow.as_str().to_owned(),
            definition_id: "fraud-detector-bench".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-bench".to_owned()),
            job_name: "fraud-detector-bench".to_owned(),
            input_ref: input_ref.to_owned(),
            config_ref: Some(
                json!({
                    "name": "fraud-detector-bench",
                    "runtime": "aggregate_v2",
                    "source": {
                        "kind": "bounded_input"
                    },
                    "keyBy": "accountId",
                    "states": [
                        {
                            "id": "minute-window",
                            "kind": "window",
                            "keyFields": ["accountId", "windowStart"],
                            "valueFields": ["avgRisk"],
                            "retentionSeconds": 120
                        },
                        {
                            "id": "risk-state",
                            "kind": "keyed",
                            "keyFields": ["accountId", "windowStart"],
                            "valueFields": ["avgRisk"],
                            "retentionSeconds": 120
                        }
                    ],
                    "operators": [
                        {
                            "kind": "map",
                            "operatorId": "normalize-risk",
                            "config": {
                                "inputField": "riskPoints",
                                "outputField": "risk",
                                "multiplyBy": 0.01
                            }
                        },
                        {
                            "kind": "filter",
                            "operatorId": "filter-positive",
                            "config": {
                                "predicate": "amount > 0"
                            }
                        },
                        {
                            "kind": "route",
                            "operatorId": "bucket-risk",
                            "config": {
                                "outputField": "riskBucket",
                                "branches": [
                                    { "predicate": "risk >= 0.8", "value": "high" },
                                    { "predicate": "risk >= 0.5", "value": "medium" }
                                ],
                                "defaultValue": "low"
                            }
                        },
                        {
                            "kind": "window",
                            "operatorId": "minute-window",
                            "config": {
                                "mode": "tumbling",
                                "size": "1m",
                                "timeField": "eventTime",
                                "allowedLateness": "10s"
                            },
                            "stateIds": ["minute-window"]
                        },
                        {
                            "kind": "aggregate",
                            "operatorId": "avg-risk",
                            "config": {
                                "reducer": "avg",
                                "valueField": "risk",
                                "outputField": "avgRisk"
                            },
                            "stateIds": ["risk-state"]
                        },
                        {
                            "kind": "materialize",
                            "operatorId": "materialize-risk",
                            "config": {
                                "view": "riskScores"
                            },
                            "stateIds": ["risk-state"]
                        },
                        {
                            "kind": "emit_checkpoint",
                            "name": "initial-risk-ready",
                            "config": {
                                "sequence": 1
                            }
                        }
                    ],
                    "checkpointPolicy": {
                        "kind": "named_checkpoints",
                        "checkpoints": [
                            {
                                "name": "initial-risk-ready",
                                "delivery": "workflow_awaitable",
                                "sequence": 1
                            }
                        ]
                    },
                    "views": [
                        {
                            "name": "riskScores",
                            "consistency": "strong",
                            "queryMode": "by_key",
                            "keyField": "accountId",
                            "supportedConsistencies": ["strong", "eventual"],
                            "retentionSeconds": 120
                        }
                    ],
                    "queries": [
                        {
                            "name": "riskScoresByKey",
                            "viewName": "riskScores",
                            "consistency": "strong",
                            "argFields": ["accountId", "windowStart"]
                        }
                    ]
                })
                .to_string(),
            ),
            checkpoint_policy: None,
            view_definitions: None,
            status: StreamJobBridgeHandleStatus::Running.as_str().to_owned(),
            workflow_owner_epoch: Some(1),
            stream_owner_epoch: Some(9),
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

    fn aggregate_v2_windowed_perf_input(
        total_items: usize,
        distinct_keys: usize,
        hot_key_ratio: f64,
    ) -> (String, String, String) {
        let hot_key = "acct_hot".to_owned();
        let hot_item_count = ((total_items as f64) * hot_key_ratio).round() as usize;
        let key_count = distinct_keys.max(1);
        let base_time = Utc::now()
            .with_second(0)
            .and_then(|value| value.with_nanosecond(0))
            .unwrap_or_else(Utc::now);
        let mut items = Vec::with_capacity(total_items);
        for index in 0..total_items {
            let account_id = if index < hot_item_count.min(total_items) {
                hot_key.clone()
            } else {
                format!("acct_{:05}", index % key_count)
            };
            let event_time = base_time + ChronoDuration::seconds((index % 60) as i64);
            items.push(json!({
                "accountId": account_id,
                "amount": 10.0,
                "riskPoints": 20.0 + (index % 80) as f64,
                "eventTime": event_time.to_rfc3339()
            }));
        }
        let query_key = if hot_item_count > 0 { hot_key } else { "acct_00000".to_owned() };
        (
            json!({ "kind": "bounded_items", "items": items }).to_string(),
            query_key,
            base_time.to_rfc3339(),
        )
    }

    fn aggregate_v2_windowed_perf_query(
        handle: &StreamJobBridgeHandleRecord,
        key: &str,
        window_start: &str,
    ) -> StreamJobQueryRecord {
        StreamJobQueryRecord {
            query_args: Some(json!({
                "key": key,
                "windowStart": window_start
            })),
            ..keyed_rollup_query(handle, "riskScoresByKey", key)
        }
    }

    fn remove_paths(paths: &[&PathBuf]) {
        for path in paths {
            std::fs::remove_dir_all(path).ok();
        }
    }

    #[derive(Default)]
    struct BenchPlanSetupMetrics {
        execution_plan_mirrors: usize,
        runtime_state_writes: usize,
        dispatch_manifest_writes: usize,
        source_cursor_writes: usize,
        elapsed: std::time::Duration,
    }

    fn prepare_local_state_for_stream_job_plan(
        local_state: &LocalThroughputState,
        handle: &StreamJobBridgeHandleRecord,
        plan: &StreamJobActivationPlan,
    ) -> Result<BenchPlanSetupMetrics> {
        let started = Instant::now();
        let mut metrics = BenchPlanSetupMetrics::default();
        if let Some(execution_planned) = plan.execution_planned.as_ref() {
            local_state
                .mirror_streams_changelog_entry(&streams_entry_from_record(execution_planned))?;
            metrics.execution_plan_mirrors += 1;
            if let Some(runtime_state) = &plan.runtime_state_update {
                local_state.upsert_stream_job_runtime_state(runtime_state)?;
                metrics.runtime_state_writes += 1;
            }
            if !plan.dataflow_batches.is_empty() {
                local_state.replace_stream_job_dispatch_manifest(
                    &handle.handle_id,
                    plan.dataflow_batches
                        .iter()
                        .map(local_dispatch_batch_from_dataflow_batch)
                        .collect(),
                    execution_planned.occurred_at(),
                )?;
                metrics.dispatch_manifest_writes += 1;
            }
        } else if let Some(runtime_state) = &plan.runtime_state_update {
            local_state.upsert_stream_job_runtime_state(runtime_state)?;
            metrics.runtime_state_writes += 1;
        }
        if let Some(source_cursors) = plan.source_cursor_updates.clone() {
            local_state.replace_stream_job_source_cursors(
                &handle.handle_id,
                source_cursors,
                plan.execution_planned
                    .as_ref()
                    .map(StreamChangelogRecord::occurred_at)
                    .unwrap_or_else(Utc::now),
            )?;
            metrics.source_cursor_writes += 1;
        }
        metrics.elapsed = started.elapsed();
        Ok(metrics)
    }

    fn streams_entry_from_record(record: &crate::StreamChangelogRecord) -> StreamsChangelogEntry {
        match &record.entry {
            crate::ChangelogRecordEntry::Streams(entry) => entry.clone(),
            crate::ChangelogRecordEntry::Throughput(other) => {
                panic!("unexpected throughput payload in stream job test: {other:?}")
            }
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

        let activation = activate_stream_job_on_local_state(
            None,
            None,
            &local_state,
            &handle,
            8,
            Some(9),
            Utc::now(),
        )
        .await?
        .expect("keyed-rollup activation should exist");

        assert!(activation.projection_records.is_empty());
        let checkpoint_record_count = activation
            .changelog_records
            .iter()
            .filter(|record| {
                matches!(
                    &record.entry,
                    crate::ChangelogRecordEntry::Streams(StreamsChangelogEntry {
                        payload: StreamsChangelogPayload::StreamJobCheckpointReached { .. },
                        ..
                    })
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
    async fn keyed_rollup_activation_materializes_checkpoint_before_changelog_replay() -> Result<()>
    {
        let db_path = temp_path("stream-jobs-keyed-rollup-direct-materialization-db");
        let checkpoint_dir =
            temp_path("stream-jobs-keyed-rollup-direct-materialization-checkpoints");
        let local_state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let handle = keyed_rollup_handle(
            r#"{"kind":"bounded_items","items":[{"accountId":"acct_1","amount":4},{"accountId":"acct_1","amount":5.5},{"accountId":"acct_2","amount":1}]}"#,
        );

        let activation = activate_stream_job_on_local_state(
            None,
            None,
            &local_state,
            &handle,
            8,
            Some(9),
            Utc::now(),
        )
        .await?
        .expect("keyed-rollup activation should exist");

        let materialized = materialization_outcome_from_local_state(&local_state, &handle)?
            .expect("materialized stream job outcome should exist immediately after activation");
        assert!(
            materialized.checkpoint.is_some(),
            "checkpoint missing after activation; runtime_state={:?}",
            local_state.load_stream_job_runtime_state(&handle.handle_id)?,
        );
        assert!(
            activation.changelog_records.iter().any(|record| {
                matches!(
                    &record.entry,
                    crate::ChangelogRecordEntry::Streams(StreamsChangelogEntry {
                        payload: StreamsChangelogPayload::StreamJobCheckpointReached { .. },
                        ..
                    })
                )
            }),
            "activation should still publish checkpoint changelog records",
        );

        std::fs::remove_dir_all(db_path).ok();
        std::fs::remove_dir_all(checkpoint_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn keyed_rollup_activation_materializes_workflow_signal() -> Result<()> {
        let db_path = temp_path("stream-jobs-keyed-rollup-signal-db");
        let checkpoint_dir = temp_path("stream-jobs-keyed-rollup-signal-checkpoints");
        let local_state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let mut handle = keyed_rollup_handle(
            r#"{"kind":"bounded_items","items":[{"accountId":"acct_1","amount":4},{"accountId":"acct_2","amount":0}]}"#,
        );
        handle.definition_id = "keyed-rollup-signal".to_owned();
        handle.job_id = "job-signal".to_owned();
        handle.handle_id = "handle-signal".to_owned();
        handle.bridge_request_id = "bridge-request-signal".to_owned();
        handle.config_ref = Some(
            json!({
                "name": KEYED_ROLLUP_JOB_NAME,
                "runtime": STREAM_RUNTIME_KEYED_ROLLUP,
                "source": {
                    "kind": STREAM_SOURCE_BOUNDED_INPUT,
                },
                "keyBy": "accountId",
                "operators": [
                    {
                        "kind": STREAM_OPERATOR_REDUCE,
                        "operatorId": "sum-account-totals",
                        "config": {
                            "reducer": STREAM_REDUCER_SUM,
                            "valueField": "amount",
                            "outputField": "totalAmount"
                        }
                    },
                    {
                        "kind": STREAM_OPERATOR_EMIT_CHECKPOINT,
                        "operatorId": KEYED_ROLLUP_CHECKPOINT_NAME,
                        "name": KEYED_ROLLUP_CHECKPOINT_NAME,
                        "config": {
                            "sequence": KEYED_ROLLUP_CHECKPOINT_SEQUENCE
                        }
                    },
                    {
                        "kind": STREAM_OPERATOR_SIGNAL_WORKFLOW,
                        "operatorId": "notify-account-rollup",
                        "config": {
                            "view": "accountTotals",
                            "signalType": "account.rollup.ready",
                            "whenOutputField": "totalAmount"
                        }
                    }
                ],
                "views": [
                    {
                        "name": "accountTotals",
                        "consistency": STREAM_CONSISTENCY_STRONG,
                        "queryMode": STREAM_QUERY_MODE_BY_KEY,
                        "keyField": "accountId",
                        "valueFields": ["accountId", "totalAmount", "asOfCheckpoint"]
                    }
                ],
                "queries": [
                    {
                        "name": "accountTotals",
                        "viewName": "accountTotals",
                        "consistency": STREAM_CONSISTENCY_STRONG
                    }
                ],
                "checkpointPolicy": {
                    "kind": STREAM_CHECKPOINT_POLICY_NAMED,
                    "checkpoints": [
                        {
                            "name": KEYED_ROLLUP_CHECKPOINT_NAME,
                            "delivery": STREAM_CHECKPOINT_DELIVERY_WORKFLOW_AWAITABLE,
                            "sequence": KEYED_ROLLUP_CHECKPOINT_SEQUENCE
                        }
                    ]
                }
            })
            .to_string(),
        );

        let activation = activate_stream_job_on_local_state(
            None,
            None,
            &local_state,
            &handle,
            8,
            Some(9),
            Utc::now(),
        )
        .await?
        .expect("keyed-rollup signal activation should exist");

        for (offset, record) in activation.changelog_records.iter().enumerate() {
            let entry = streams_entry_from_record(record);
            assert!(local_state.apply_streams_changelog_entry(0, offset as i64, &entry)?);
        }

        let materialized = materialization_outcome_from_local_state(&local_state, &handle)?
            .expect("materialized stream job outcome should exist");
        assert_eq!(materialized.workflow_signals.len(), 1);
        let signal = &materialized.workflow_signals[0];
        assert_eq!(signal.operator_id, "notify-account-rollup");
        assert_eq!(signal.view_name, "accountTotals");
        assert_eq!(signal.logical_key, "acct_1");
        assert_eq!(signal.signal_type, "account.rollup.ready");
        assert_eq!(signal.payload["jobId"], "job-signal");
        assert_eq!(signal.payload["logicalKey"], "acct_1");
        assert_eq!(signal.payload["output"]["totalAmount"], json!(4.0));

        let query_output = build_stream_job_query_output_on_local_state(
            &local_state,
            &handle,
            &keyed_rollup_query(&handle, "accountTotals", "acct_1"),
        )?
        .expect("query output should exist");
        assert_eq!(query_output["accountId"], "acct_1");
        assert_eq!(query_output["totalAmount"], json!(4.0));

        let duplicate_free = local_state.load_stream_job_workflow_signal_state(
            &handle.handle_id,
            "notify-account-rollup",
            "acct_2",
        )?;
        assert!(duplicate_free.is_none());

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
        let activation = activate_stream_job_on_local_state(
            None,
            None,
            &local_state,
            &handle,
            8,
            Some(9),
            Utc::now(),
        )
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
    async fn topic_keyed_rollup_replays_source_progress_from_checkpoint_plus_streams_tail()
    -> Result<()> {
        let db_path = temp_path("stream-jobs-topic-replay-db");
        let checkpoint_dir = temp_path("stream-jobs-topic-replay-checkpoints");
        let local_state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let handle = topic_keyed_rollup_handle("payments");
        let planned_at = Utc::now();
        let initial_target = 2;
        let advanced_target = 3;
        let execution_planned = execution_planned_changelog_record(
            &handle,
            "accountTotals",
            KEYED_ROLLUP_CHECKPOINT_NAME,
            KEYED_ROLLUP_CHECKPOINT_SEQUENCE,
            9,
            0,
            1,
            vec![0],
            1,
            planned_at,
        );
        let runtime_state = LocalStreamJobRuntimeState {
            handle_id: handle.handle_id.clone(),
            job_id: handle.job_id.clone(),
            job_name: handle.job_name.clone(),
            view_name: "accountTotals".to_owned(),
            checkpoint_name: KEYED_ROLLUP_CHECKPOINT_NAME.to_owned(),
            checkpoint_sequence: KEYED_ROLLUP_CHECKPOINT_SEQUENCE,
            input_item_count: 0,
            materialized_key_count: 1,
            active_partitions: vec![0],
            throughput_partition_count: 1,
            source_kind: Some(STREAM_SOURCE_TOPIC.to_owned()),
            source_name: Some("payments".to_owned()),
            source_cursors: vec![LocalStreamJobSourceCursorState {
                source_partition_id: 0,
                next_offset: 0,
                initial_checkpoint_target_offset: initial_target,
                last_applied_offset: None,
                last_high_watermark: None,
                last_event_time_watermark: None,
                last_closed_window_end: None,
                pending_window_ends: Vec::new(),
                dropped_late_event_count: 0,
                last_dropped_late_offset: None,
                last_dropped_late_event_at: None,
                last_dropped_late_window_end: None,
                dropped_evicted_window_event_count: 0,
                last_dropped_evicted_window_offset: None,
                last_dropped_evicted_window_event_at: None,
                last_dropped_evicted_window_end: None,
                checkpoint_reached_at: None,
                updated_at: planned_at,
            }],
            source_partition_leases: Vec::new(),
            dispatch_dataflow_batches: Vec::new(),
            applied_dispatch_batch_ids: Vec::new(),
            dispatch_completed_at: None,
            dispatch_cancelled_at: None,
            stream_owner_epoch: 9,
            planned_at,
            latest_checkpoint_at: None,
            evicted_window_count: 0,
            last_evicted_window_end: None,
            last_evicted_at: None,
            view_runtime_stats: Vec::new(),
            pre_key_runtime_stats: Vec::new(),
            hot_key_runtime_stats: Vec::new(),
            owner_partition_runtime_stats: Vec::new(),
            checkpoint_partitions: Vec::new(),
            terminal_status: None,
            terminal_output: None,
            terminal_error: None,
            terminal_at: None,
            updated_at: planned_at,
        };
        local_state.upsert_stream_job_runtime_state(&runtime_state)?;
        let initial_view = view_update_changelog_record(
            &handle,
            "accountTotals",
            "acct_1",
            "acct_1",
            json!({
                "accountId": "acct_1",
                "totalAmount": 5.0,
                "asOfCheckpoint": KEYED_ROLLUP_CHECKPOINT_SEQUENCE,
            }),
            KEYED_ROLLUP_CHECKPOINT_SEQUENCE,
            planned_at + chrono::Duration::milliseconds(1),
        );
        let initial_source_progress = source_progressed_changelog_record(
            &handle,
            0,
            initial_target,
            KEYED_ROLLUP_CHECKPOINT_SEQUENCE,
            initial_target,
            Some(initial_target - 1),
            Some(initial_target),
            None,
            None,
            Vec::new(),
            0,
            None,
            None,
            None,
            0,
            None,
            None,
            None,
            0,
            "",
            9,
            planned_at + chrono::Duration::milliseconds(2),
        );
        let initial_checkpoint = checkpoint_reached_changelog_record(
            &handle,
            &handle.job_id,
            KEYED_ROLLUP_CHECKPOINT_NAME,
            KEYED_ROLLUP_CHECKPOINT_SEQUENCE,
            0,
            9,
            planned_at + chrono::Duration::milliseconds(3),
        );
        let advanced_view = view_update_changelog_record(
            &handle,
            "accountTotals",
            "acct_1",
            "acct_1",
            json!({
                "accountId": "acct_1",
                "totalAmount": 12.0,
                "asOfCheckpoint": KEYED_ROLLUP_CHECKPOINT_SEQUENCE,
            }),
            KEYED_ROLLUP_CHECKPOINT_SEQUENCE,
            planned_at + chrono::Duration::milliseconds(4),
        );
        let advanced_source_progress = source_progressed_changelog_record(
            &handle,
            0,
            advanced_target,
            KEYED_ROLLUP_CHECKPOINT_SEQUENCE,
            advanced_target,
            Some(advanced_target - 1),
            Some(advanced_target),
            None,
            None,
            Vec::new(),
            0,
            None,
            None,
            None,
            0,
            None,
            None,
            None,
            0,
            "",
            9,
            planned_at + chrono::Duration::milliseconds(5),
        );
        let advanced_checkpoint = checkpoint_reached_changelog_record(
            &handle,
            &handle.job_id,
            KEYED_ROLLUP_CHECKPOINT_NAME,
            KEYED_ROLLUP_CHECKPOINT_SEQUENCE,
            0,
            9,
            planned_at + chrono::Duration::milliseconds(6),
        );
        let entries = [
            execution_planned,
            initial_view,
            initial_source_progress,
            initial_checkpoint,
            advanced_view,
            advanced_source_progress,
            advanced_checkpoint,
        ]
        .into_iter()
        .map(|record| streams_entry_from_record(&record))
        .collect::<Vec<_>>();

        for (offset, entry) in entries.iter().take(4).enumerate() {
            assert!(local_state.apply_streams_changelog_entry(0, offset as i64, entry)?);
        }
        let checkpoint_value = local_state.snapshot_checkpoint_value()?;

        let restored_db_path = temp_path("stream-jobs-topic-replay-restored-db");
        let restored = LocalThroughputState::open(&restored_db_path, &checkpoint_dir, 3)?;
        assert!(restored.restore_from_checkpoint_value_if_empty(checkpoint_value)?);
        for (offset, entry) in entries.iter().enumerate().skip(4) {
            assert!(restored.apply_streams_changelog_entry(0, offset as i64, entry)?);
        }

        let runtime_state = restored
            .load_stream_job_runtime_state(&handle.handle_id)?
            .expect("topic runtime state should restore");
        assert_eq!(runtime_state.source_kind.as_deref(), Some(STREAM_SOURCE_TOPIC));
        assert_eq!(runtime_state.source_cursors.len(), 1);
        assert_eq!(runtime_state.source_cursors[0].source_partition_id, 0);
        assert_eq!(runtime_state.source_cursors[0].next_offset, advanced_target);
        assert_eq!(
            runtime_state.source_cursors[0].initial_checkpoint_target_offset,
            advanced_target
        );
        assert_eq!(runtime_state.source_cursors[0].last_applied_offset, Some(advanced_target - 1));
        assert_eq!(runtime_state.source_cursors[0].last_high_watermark, Some(advanced_target));
        assert!(runtime_state.source_cursors[0].checkpoint_reached_at.is_some());

        let acct_1 = restored
            .load_stream_job_view_state(&handle.handle_id, "accountTotals", "acct_1")?
            .expect("restored topic view should exist");
        assert_eq!(acct_1.output["totalAmount"], 12.0);

        let materialized = materialization_outcome_from_local_state(&restored, &handle)?
            .expect("checkpoint state should survive topic replay");
        assert_eq!(
            materialized.checkpoint.as_ref().map(|checkpoint| checkpoint.checkpoint_sequence),
            Some(Some(KEYED_ROLLUP_CHECKPOINT_SEQUENCE))
        );
        assert_eq!(
            materialized.checkpoint.as_ref().and_then(|checkpoint| checkpoint.reached_at),
            Some(planned_at + chrono::Duration::milliseconds(6))
        );

        std::fs::remove_dir_all(db_path).ok();
        std::fs::remove_dir_all(restored_db_path).ok();
        std::fs::remove_dir_all(checkpoint_dir).ok();
        Ok(())
    }

    #[test]
    fn source_partition_lease_fences_stale_topic_work() -> Result<()> {
        let db_path = temp_path("stream-jobs-topic-lease-fence-db");
        let checkpoint_dir = temp_path("stream-jobs-topic-lease-fence-checkpoints");
        let local_state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let handle = topic_keyed_rollup_handle("payments");
        let occurred_at = Utc::now();
        let runtime_state = LocalStreamJobRuntimeState {
            handle_id: handle.handle_id.clone(),
            job_id: handle.job_id.clone(),
            job_name: handle.job_name.clone(),
            view_name: "accountTotals".to_owned(),
            checkpoint_name: KEYED_ROLLUP_CHECKPOINT_NAME.to_owned(),
            checkpoint_sequence: KEYED_ROLLUP_CHECKPOINT_SEQUENCE,
            input_item_count: 0,
            materialized_key_count: 0,
            active_partitions: vec![0],
            throughput_partition_count: 1,
            source_kind: Some(STREAM_SOURCE_TOPIC.to_owned()),
            source_name: Some("payments".to_owned()),
            source_cursors: vec![LocalStreamJobSourceCursorState {
                source_partition_id: 0,
                next_offset: 1,
                initial_checkpoint_target_offset: 1,
                last_applied_offset: Some(0),
                last_high_watermark: Some(1),
                last_event_time_watermark: None,
                last_closed_window_end: None,
                pending_window_ends: Vec::new(),
                dropped_late_event_count: 0,
                last_dropped_late_offset: None,
                last_dropped_late_event_at: None,
                last_dropped_late_window_end: None,
                dropped_evicted_window_event_count: 0,
                last_dropped_evicted_window_offset: None,
                last_dropped_evicted_window_event_at: None,
                last_dropped_evicted_window_end: None,
                checkpoint_reached_at: Some(occurred_at),
                updated_at: occurred_at,
            }],
            source_partition_leases: vec![LocalStreamJobSourceLeaseState {
                source_partition_id: 0,
                owner_partition_id: 0,
                owner_epoch: 3,
                lease_token: "lease-current".to_owned(),
                updated_at: occurred_at,
            }],
            dispatch_dataflow_batches: Vec::new(),
            applied_dispatch_batch_ids: Vec::new(),
            dispatch_completed_at: None,
            dispatch_cancelled_at: None,
            stream_owner_epoch: 3,
            planned_at: occurred_at,
            latest_checkpoint_at: Some(occurred_at),
            evicted_window_count: 0,
            last_evicted_window_end: None,
            last_evicted_at: None,
            view_runtime_stats: Vec::new(),
            pre_key_runtime_stats: Vec::new(),
            hot_key_runtime_stats: Vec::new(),
            owner_partition_runtime_stats: Vec::new(),
            checkpoint_partitions: Vec::new(),
            terminal_status: None,
            terminal_output: None,
            terminal_error: None,
            terminal_at: None,
            updated_at: occurred_at,
        };
        local_state.upsert_stream_job_runtime_state(&runtime_state)?;

        let stale_batch = direct_dataflow_batch_for_program(
            &handle,
            "batch-stale".to_owned(),
            crate::dataflow_engine::BoundedDataflowBatchProgram {
                stream_partition_id: 0,
                checkpoint_partition_id: Some(0),
                source_owner_partition_id: Some(0),
                source_lease_token: Some("lease-stale".to_owned()),
                routing_key: "acct_1".to_owned(),
                key_field: "accountId".to_owned(),
                reducer_kind: STREAM_REDUCER_SUM.to_owned(),
                output_field: "totalAmount".to_owned(),
                view_name: "accountTotals".to_owned(),
                additional_view_names: Vec::new(),
                eventual_projection_view_names: Vec::new(),
                workflow_signals: Vec::new(),
                checkpoint_name: KEYED_ROLLUP_CHECKPOINT_NAME.to_owned(),
                checkpoint_sequence: KEYED_ROLLUP_CHECKPOINT_SEQUENCE + 1,
                owner_epoch: 3,
                occurred_at,
                items: vec![crate::dataflow_engine::BoundedDataflowItem {
                    logical_key: "acct_1".to_owned(),
                    value: 9.0,
                    display_key: Some("acct_1".to_owned()),
                    window_start: None,
                    window_end: None,
                    count: 1,
                }],
                is_final_partition_batch: false,
                completes_dispatch: false,
            },
            Some(&runtime_state),
        );
        let stale = apply_stream_job_dataflow_batch_on_local_state_with_metrics(
            &local_state,
            &handle,
            &stale_batch,
            true,
            Some(&runtime_state),
            None,
        )?;
        assert!(stale.projection_records.is_empty());
        assert!(stale.changelog_records.is_empty());
        assert!(
            local_state
                .load_stream_job_view_state(&handle.handle_id, "accountTotals", "acct_1")?
                .is_none()
        );

        std::fs::remove_dir_all(db_path).ok();
        std::fs::remove_dir_all(checkpoint_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn keyed_rollup_replays_mid_partition_failover_from_owner_checkpoint() -> Result<()> {
        let partition_count = 8;
        let (first_key, second_key) = distinct_partition_keys(partition_count);
        let first_partition = throughput_partition_for_stream_key(&first_key, partition_count);
        let mut first_partition_keys = vec![first_key.clone()];
        let mut candidate_index = 0usize;
        while first_partition_keys.len() < 65 {
            let candidate = format!("acct_partition_a_extra_{candidate_index}");
            candidate_index = candidate_index.saturating_add(1);
            if throughput_partition_for_stream_key(&candidate, partition_count) == first_partition {
                first_partition_keys.push(candidate);
            }
        }
        let mut items = Vec::new();
        for key in &first_partition_keys {
            items.push(json!({ "accountId": key, "amount": 1.0 }));
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
            None,
            &handle,
            partition_count,
            Some(9),
            occurred_at,
        )
        .await?
        .expect("keyed-rollup activation plan should exist");
        let second_partition = throughput_partition_for_stream_key(&second_key, partition_count);
        let mut first_partition_batches = plan
            .dataflow_batches
            .iter()
            .filter(|batch| batch.program.stream_partition_id == first_partition)
            .cloned()
            .collect::<Vec<_>>();
        let second_partition_batch = plan
            .dataflow_batches
            .iter()
            .find(|batch| batch.program.stream_partition_id == second_partition)
            .cloned()
            .expect("second partition batch should exist");
        assert_eq!(first_partition_batches.len(), 2);
        first_partition_batches.sort_by_key(|batch| batch.program.is_final_partition_batch);
        assert!(!first_partition_batches[0].program.is_final_partition_batch);
        assert!(first_partition_batches[1].program.is_final_partition_batch);
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
            plan.dataflow_batches.iter().map(local_dispatch_batch_from_dataflow_batch).collect(),
            occurred_at,
        )?;

        let first_batch = apply_stream_job_dataflow_batch_on_local_state(
            &owner_state,
            &handle,
            &first_partition_batches[0],
            true,
        )?;
        assert!(first_batch.activation_result.projection_records.is_empty());
        let partial_view = owner_state
            .load_stream_job_view_state(&handle.handle_id, "accountTotals", &first_key)?
            .expect("partial owner state should materialize the first key");
        assert_eq!(partial_view.output["totalAmount"], json!(1.0));
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
        assert_eq!(restored_partial.output["totalAmount"], json!(1.0));

        let first_final_batch = apply_stream_job_dataflow_batch_on_local_state(
            &restored_owner,
            &handle,
            &first_partition_batches[1],
            true,
        )?;
        let second_batch = apply_stream_job_dataflow_batch_on_local_state(
            &owner_state,
            &handle,
            &second_partition_batch,
            true,
        )?;
        let mut replay_records = Vec::new();
        replay_records.extend(first_batch.activation_result.changelog_records);
        replay_records.extend(first_final_batch.activation_result.changelog_records);
        replay_records.extend(second_batch.activation_result.changelog_records);
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
        assert_eq!(completed_view.output["totalAmount"], json!(1.0));
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
            first_plan
                .dataflow_batches
                .iter()
                .map(local_dispatch_batch_from_dataflow_batch)
                .collect(),
            occurred_at,
        )?;
        let applied_batch_id = first_plan.dataflow_batches[0].envelope.batch_id.clone();
        assert!(state.mark_stream_job_dispatch_batch_applied(
            &handle.handle_id,
            &applied_batch_id,
            first_plan.dataflow_batches[0].program.completes_dispatch,
            occurred_at,
        )?);

        let resumed_plan = plan_stream_job_activation(
            &state,
            None,
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
            resumed_plan.dataflow_batches.len(),
            first_plan.dataflow_batches.len().saturating_sub(1)
        );
        assert!(
            resumed_plan
                .dataflow_batches
                .iter()
                .all(|batch| batch.envelope.batch_id != applied_batch_id)
        );
        let expected_pending_batches = first_plan
            .dataflow_batches
            .iter()
            .filter(|batch| batch.envelope.batch_id != applied_batch_id)
            .map(|batch| (batch.envelope.batch_id.clone(), batch))
            .collect::<HashMap<_, _>>();
        assert_eq!(resumed_plan.dataflow_batches.len(), expected_pending_batches.len());
        for resumed in &resumed_plan.dataflow_batches {
            let expected = expected_pending_batches
                .get(&resumed.envelope.batch_id)
                .expect("resumed batch should exist in persisted manifest");
            assert_eq!(resumed.envelope.plan_id, expected.envelope.plan_id);
            assert_eq!(resumed.envelope.batch_id, expected.envelope.batch_id);
            assert_eq!(resumed.envelope.source_id, expected.envelope.source_id);
            assert_eq!(resumed.envelope.source_partition_id, expected.envelope.source_partition_id);
            assert_eq!(resumed.envelope.source_epoch, expected.envelope.source_epoch);
            assert_eq!(resumed.envelope.offset_range, expected.envelope.offset_range);
            assert_eq!(resumed.envelope.edge_id, expected.envelope.edge_id);
            assert_eq!(resumed.envelope.dest_partition_id, expected.envelope.dest_partition_id);
            assert_eq!(resumed.envelope.owner_epoch, expected.envelope.owner_epoch);
            assert_eq!(resumed.envelope.lease_epoch, expected.envelope.lease_epoch);
            assert_eq!(resumed.envelope.summary, expected.envelope.summary);
        }

        std::fs::remove_dir_all(db_path).ok();
        std::fs::remove_dir_all(checkpoint_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn keyed_rollup_apply_persists_accepted_progress_and_restores_checkpoint() -> Result<()> {
        let partition_count = 8;
        let handle = keyed_rollup_handle(
            &json!({
                "kind": "bounded_items",
                "items": [
                    { "accountId": "acct_1", "amount": 4.0 },
                    { "accountId": "acct_1", "amount": 5.0 }
                ]
            })
            .to_string(),
        );
        let db_path = temp_path("stream-jobs-keyed-rollup-accepted-progress-db");
        let checkpoint_dir = temp_path("stream-jobs-keyed-rollup-accepted-progress-checkpoints");
        let local_state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let occurred_at = Utc::now();

        let plan = plan_stream_job_activation(
            &local_state,
            None,
            None,
            &handle,
            partition_count,
            Some(9),
            occurred_at,
        )
        .await?
        .expect("activation plan should exist");
        let execution_entry = streams_entry_from_record(
            plan.execution_planned.as_ref().expect("execution plan should exist"),
        );
        local_state.mirror_streams_changelog_entry(&execution_entry)?;
        local_state.replace_stream_job_dispatch_manifest(
            &handle.handle_id,
            plan.dataflow_batches.iter().map(local_dispatch_batch_from_dataflow_batch).collect(),
            occurred_at,
        )?;

        let batch = plan.dataflow_batches.first().cloned().expect("dataflow batch should exist");
        let applied =
            apply_stream_job_dataflow_batch_on_local_state(&local_state, &handle, &batch, true)?;
        let owner_apply_ack = applied
            .owner_apply_ack
            .as_ref()
            .expect("dataflow batch apply should return owner apply ack");
        assert_eq!(owner_apply_ack.plan_id, batch.envelope.plan_id);
        assert_eq!(owner_apply_ack.batch_id, batch.envelope.batch_id);
        assert_eq!(owner_apply_ack.dest_partition_id, batch.envelope.dest_partition_id);
        assert_eq!(owner_apply_ack.owner_epoch, batch.envelope.owner_epoch);
        assert!(owner_apply_ack.durable);
        assert_eq!(owner_apply_ack.accepted_progress_position, 1);
        let sealed_checkpoint = applied
            .sealed_checkpoint
            .as_ref()
            .expect("checkpoint-reaching batch should return sealed checkpoint");
        assert_eq!(sealed_checkpoint.plan_id, batch.envelope.plan_id);
        assert_eq!(sealed_checkpoint.partition_id, batch.envelope.dest_partition_id);
        assert_eq!(sealed_checkpoint.accepted_progress_position, 1);

        let cursor = local_state
            .load_stream_job_accepted_progress_cursor(&handle.handle_id)?
            .expect("accepted progress cursor should exist");
        assert_eq!(cursor.latest_position, 1);
        assert_eq!(cursor.last_durable_positions.get(&batch.program.stream_partition_id), Some(&1));

        let tail = local_state.load_stream_job_accepted_progress_tail(&handle.handle_id, 8)?;
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].batch_id, batch.envelope.batch_id);
        assert_eq!(tail[0].record.batch_id, batch.envelope.batch_id);
        assert_eq!(tail[0].record.plan_id, batch.envelope.plan_id);
        assert_eq!(tail[0].record.source_id, batch.envelope.source_id);
        assert_eq!(tail[0].record.source_partition_id, batch.envelope.source_partition_id);
        assert_eq!(tail[0].record.source_epoch, batch.envelope.source_epoch);
        assert_eq!(tail[0].record.offset_range, batch.envelope.offset_range);
        assert_eq!(tail[0].record.edge_id, batch.envelope.edge_id);
        assert_eq!(tail[0].record.dest_partition_id, batch.envelope.dest_partition_id);
        assert_eq!(tail[0].record.owner_epoch, batch.envelope.owner_epoch);
        assert_eq!(tail[0].ack.accepted_progress_position, 1);
        assert!(tail[0].ack.durable);
        let sealed = local_state
            .load_all_stream_job_sealed_checkpoints()?
            .into_iter()
            .filter(|state| state.handle_id == handle.handle_id)
            .collect::<Vec<_>>();
        assert_eq!(sealed.len(), 1);
        assert_eq!(sealed[0].checkpoint_name, KEYED_ROLLUP_CHECKPOINT_NAME);
        assert_eq!(sealed[0].record.accepted_progress_position, 1);

        let checkpoint_value = local_state.snapshot_checkpoint_value()?;
        let restored_db_path = temp_path("stream-jobs-keyed-rollup-accepted-progress-restored-db");
        let restored_checkpoint_dir =
            temp_path("stream-jobs-keyed-rollup-accepted-progress-restored-checkpoints");
        let restored_state =
            LocalThroughputState::open(&restored_db_path, &restored_checkpoint_dir, 3)?;
        assert!(restored_state.restore_from_checkpoint_value_if_empty(checkpoint_value)?);

        let restored_cursor = restored_state
            .load_stream_job_accepted_progress_cursor(&handle.handle_id)?
            .expect("restored accepted progress cursor should exist");
        assert_eq!(restored_cursor.latest_position, 1);
        let restored_tail =
            restored_state.load_stream_job_accepted_progress_tail(&handle.handle_id, 8)?;
        assert_eq!(restored_tail.len(), 1);
        assert_eq!(restored_tail[0].record.batch_id, batch.envelope.batch_id);
        let restored_sealed = restored_state
            .load_all_stream_job_sealed_checkpoints()?
            .into_iter()
            .filter(|state| state.handle_id == handle.handle_id)
            .collect::<Vec<_>>();
        assert_eq!(restored_sealed.len(), 1);
        assert_eq!(restored_sealed[0].record.accepted_progress_position, 1);

        std::fs::remove_dir_all(db_path).ok();
        std::fs::remove_dir_all(checkpoint_dir).ok();
        std::fs::remove_dir_all(restored_db_path).ok();
        std::fs::remove_dir_all(restored_checkpoint_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn keyed_rollup_dataflow_batch_list_returns_ordered_owner_apply_acks() -> Result<()> {
        let partition_count = 8;
        let seed_key = "acct_batch_root".to_owned();
        let first_partition = throughput_partition_for_stream_key(&seed_key, partition_count);
        let mut batch_partition_keys = vec![seed_key];
        let mut candidate_index = 0usize;
        while batch_partition_keys.len() < 65 {
            let candidate = format!("acct_batch_extra_{candidate_index}");
            candidate_index = candidate_index.saturating_add(1);
            if throughput_partition_for_stream_key(&candidate, partition_count) == first_partition {
                batch_partition_keys.push(candidate);
            }
        }
        let handle = keyed_rollup_handle(
            &json!({
                "kind": "bounded_items",
                "items": batch_partition_keys
                    .iter()
                    .map(|account_id| json!({ "accountId": account_id, "amount": 1.0 }))
                    .collect::<Vec<_>>()
            })
            .to_string(),
        );
        let db_path = temp_path("stream-jobs-keyed-rollup-dataflow-batch-list-db");
        let checkpoint_dir = temp_path("stream-jobs-keyed-rollup-dataflow-batch-list-checkpoints");
        let local_state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let occurred_at = Utc::now();

        let plan = plan_stream_job_activation(
            &local_state,
            None,
            None,
            &handle,
            partition_count,
            Some(9),
            occurred_at,
        )
        .await?
        .expect("activation plan should exist");
        assert!(plan.dataflow_batches.len() >= 2);
        let execution_entry = streams_entry_from_record(
            plan.execution_planned.as_ref().expect("execution plan should exist"),
        );
        local_state.mirror_streams_changelog_entry(&execution_entry)?;
        local_state.replace_stream_job_dispatch_manifest(
            &handle.handle_id,
            plan.dataflow_batches.iter().map(local_dispatch_batch_from_dataflow_batch).collect(),
            occurred_at,
        )?;

        let batches = plan.dataflow_batches.clone();
        let applied = apply_stream_job_dataflow_batch_list_on_local_state(
            &local_state,
            &handle,
            &batches,
            true,
        )?;
        assert_eq!(applied.batch_results.len(), batches.len());

        let durable_positions = applied
            .batch_results
            .iter()
            .map(|result| {
                result
                    .owner_apply_ack
                    .as_ref()
                    .expect("every bounded batch should return an owner ack")
                    .accepted_progress_position
            })
            .collect::<Vec<_>>();
        assert_eq!(durable_positions, vec![1, 2]);
        assert_eq!(
            applied
                .batch_results
                .iter()
                .filter_map(|result| result.sealed_checkpoint.as_ref())
                .count(),
            1
        );
        let final_batch = applied
            .batch_results
            .iter()
            .find(|result| result.batch.program.is_final_partition_batch)
            .expect("final partition batch should exist");
        assert_eq!(
            final_batch
                .sealed_checkpoint
                .as_ref()
                .expect("final batch should seal a checkpoint")
                .accepted_progress_position,
            2
        );

        let tail = local_state.load_stream_job_accepted_progress_tail(&handle.handle_id, 8)?;
        assert_eq!(tail.len(), 2);
        assert_eq!(
            tail.iter().map(|state| state.ack.accepted_progress_position).collect::<Vec<_>>(),
            vec![1, 2]
        );

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
            first_plan
                .dataflow_batches
                .iter()
                .map(local_dispatch_batch_from_dataflow_batch)
                .collect(),
            occurred_at,
        )?;
        state.mark_stream_job_dispatch_cancelled(
            &handle.handle_id,
            occurred_at + chrono::Duration::milliseconds(1),
        )?;

        let resumed_plan = plan_stream_job_activation(
            &state,
            None,
            None,
            &handle,
            partition_count,
            Some(9),
            occurred_at + chrono::Duration::seconds(1),
        )
        .await?
        .expect("resumed activation plan should exist");
        assert!(resumed_plan.execution_planned.is_none());
        assert!(resumed_plan.dataflow_batches.is_empty());
        assert!(resumed_plan.terminalized.is_none());

        std::fs::remove_dir_all(db_path).ok();
        std::fs::remove_dir_all(checkpoint_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn aggregate_v2_bounded_activation_materializes_average_view() -> Result<()> {
        let local_state = LocalThroughputState::open(
            &temp_path("stream-jobs-aggregate-v2-unsupported-db"),
            &temp_path("stream-jobs-aggregate-v2-unsupported-checkpoints"),
            3,
        )
        .expect("local state should open");
        let handle = StreamJobBridgeHandleRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            tenant_id: "tenant-a".to_owned(),
            instance_id: "instance-a".to_owned(),
            run_id: "run-a".to_owned(),
            stream_instance_id: "stream-a".to_owned(),
            stream_run_id: "stream-run-a".to_owned(),
            job_id: "job-a".to_owned(),
            handle_id: "handle-a".to_owned(),
            bridge_request_id: "bridge-a".to_owned(),
            origin_kind: StreamJobOriginKind::Workflow.as_str().to_owned(),
            definition_id: "fraud-detector".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-a".to_owned()),
            job_name: "fraud-detector".to_owned(),
            input_ref: json!({
                "kind": "bounded_items",
                "items": [
                    {
                        "accountId": "acct_1",
                        "risk": 0.9,
                        "amount": 5
                    },
                    {
                        "accountId": "acct_1",
                        "risk": 0.3,
                        "amount": 2
                    }
                ]
            })
            .to_string(),
            config_ref: Some(
                json!({
                    "name": "fraud-detector",
                    "runtime": "aggregate_v2",
                    "source": {
                        "kind": "bounded_input"
                    },
                    "keyBy": "accountId",
                    "states": [
                        {
                            "id": "risk-state",
                            "kind": "keyed",
                            "keyFields": ["accountId"],
                            "valueFields": ["avgRisk"]
                        }
                    ],
                    "operators": [
                        {
                            "kind": "aggregate",
                            "operatorId": "avg-risk",
                            "config": {
                                "reducer": "avg",
                                "valueField": "risk",
                                "outputField": "avgRisk"
                            },
                            "stateIds": ["risk-state"]
                        },
                        {
                            "kind": "materialize",
                            "operatorId": "materialize-risk",
                            "config": {
                                "view": "riskScores"
                            },
                            "stateIds": ["risk-state"]
                        }
                    ],
                    "views": [
                        {
                            "name": "riskScores",
                            "consistency": "strong",
                            "queryMode": "by_key",
                            "keyField": "accountId"
                        }
                    ],
                    "queries": [
                        {
                            "name": "riskScoresByKey",
                            "viewName": "riskScores",
                            "consistency": "strong"
                        }
                    ]
                })
                .to_string(),
            ),
            checkpoint_policy: None,
            view_definitions: None,
            status: StreamJobBridgeHandleStatus::Admitted.as_str().to_owned(),
            workflow_owner_epoch: Some(1),
            stream_owner_epoch: Some(1),
            cancellation_requested_at: None,
            cancellation_reason: None,
            terminal_event_id: None,
            terminal_at: None,
            workflow_accepted_at: None,
            terminal_output: None,
            terminal_error: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let activation = activate_stream_job_on_local_state(
            None,
            None,
            &local_state,
            &handle,
            8,
            Some(1),
            Utc::now(),
        )
        .await
        .expect("aggregate_v2 activation should succeed")
        .expect("aggregate_v2 activation should produce work");

        for (offset, record) in activation.changelog_records.iter().enumerate() {
            let entry = streams_entry_from_record(record);
            assert!(local_state.apply_streams_changelog_entry(0, offset as i64, &entry)?);
        }

        let raw_view = local_state
            .load_stream_job_view_state(&handle.handle_id, "riskScores", "acct_1")?
            .expect("aggregate_v2 raw view state should exist");
        assert_eq!(raw_view.output["avgRisk"], 0.6);
        assert_eq!(raw_view.output[STREAM_INTERNAL_AVG_SUM_FIELD], 1.2);
        assert_eq!(raw_view.output[STREAM_INTERNAL_AVG_COUNT_FIELD], 2);

        let materialized = materialization_outcome_from_local_state(&local_state, &handle)?
            .expect("aggregate_v2 materialization outcome should exist");
        assert!(materialized.checkpoint.is_none());
        assert_eq!(
            materialized.terminal.as_ref().map(|terminal| terminal.status),
            Some(StreamJobBridgeHandleStatus::Completed)
        );

        let query = keyed_rollup_query(&handle, "riskScoresByKey", "acct_1");

        let output = build_stream_job_query_output_on_local_state(&local_state, &handle, &query)?
            .expect("aggregate_v2 query output should exist");
        assert_eq!(output["accountId"], "acct_1");
        assert_eq!(output["avgRisk"], 0.6);
        assert!(output.get(STREAM_INTERNAL_AVG_SUM_FIELD).is_none());
        assert!(output.get(STREAM_INTERNAL_AVG_COUNT_FIELD).is_none());

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_v2_bounded_activation_filters_source_items() -> Result<()> {
        let local_state = LocalThroughputState::open(
            &temp_path("stream-jobs-aggregate-v2-filter-db"),
            &temp_path("stream-jobs-aggregate-v2-filter-checkpoints"),
            3,
        )
        .expect("local state should open");
        let handle = StreamJobBridgeHandleRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            tenant_id: "tenant-a".to_owned(),
            instance_id: "instance-a".to_owned(),
            run_id: "run-a".to_owned(),
            stream_instance_id: "stream-a".to_owned(),
            stream_run_id: "stream-run-a".to_owned(),
            job_id: "job-filter".to_owned(),
            handle_id: "handle-filter".to_owned(),
            bridge_request_id: "bridge-filter".to_owned(),
            origin_kind: StreamJobOriginKind::Workflow.as_str().to_owned(),
            definition_id: "positive-amounts".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-filter".to_owned()),
            job_name: "positive-amounts".to_owned(),
            input_ref: json!({
                "kind": "bounded_items",
                "items": [
                    { "accountId": "acct_1", "amount": 2.0 },
                    { "accountId": "acct_1", "amount": -1.0 },
                    { "accountId": "acct_1", "amount": 3.0 },
                    { "accountId": "acct_2", "amount": 0.0 }
                ]
            })
            .to_string(),
            config_ref: Some(
                json!({
                    "name": "positive-amounts",
                    "runtime": "aggregate_v2",
                    "source": {
                        "kind": "bounded_input"
                    },
                    "keyBy": "accountId",
                    "states": [
                        {
                            "id": "positive-amount-state",
                            "kind": "keyed",
                            "keyFields": ["accountId"],
                            "valueFields": ["totalAmount"]
                        }
                    ],
                    "operators": [
                        {
                            "kind": "filter",
                            "operatorId": "filter-positive",
                            "config": {
                                "predicate": "amount > 0"
                            }
                        },
                        {
                            "kind": "aggregate",
                            "operatorId": "sum-positive",
                            "config": {
                                "reducer": "sum",
                                "valueField": "amount",
                                "outputField": "totalAmount"
                            },
                            "stateIds": ["positive-amount-state"]
                        },
                        {
                            "kind": "materialize",
                            "operatorId": "materialize-positive",
                            "config": {
                                "view": "positiveAmounts"
                            },
                            "stateIds": ["positive-amount-state"]
                        }
                    ],
                    "views": [
                        {
                            "name": "positiveAmounts",
                            "consistency": "strong",
                            "queryMode": "by_key",
                            "keyField": "accountId"
                        }
                    ],
                    "queries": [
                        {
                            "name": "positiveAmountsByKey",
                            "viewName": "positiveAmounts",
                            "consistency": "strong"
                        }
                    ]
                })
                .to_string(),
            ),
            checkpoint_policy: None,
            view_definitions: None,
            status: StreamJobBridgeHandleStatus::Admitted.as_str().to_owned(),
            workflow_owner_epoch: Some(1),
            stream_owner_epoch: Some(1),
            cancellation_requested_at: None,
            cancellation_reason: None,
            terminal_event_id: None,
            terminal_at: None,
            workflow_accepted_at: None,
            terminal_output: None,
            terminal_error: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let activation = activate_stream_job_on_local_state(
            None,
            None,
            &local_state,
            &handle,
            8,
            Some(1),
            Utc::now(),
        )
        .await
        .expect("filtered aggregate_v2 activation should succeed")
        .expect("filtered aggregate_v2 activation should produce work");

        let runtime_state = local_state
            .load_stream_job_runtime_state(&handle.handle_id)?
            .expect("filtered runtime state should exist");
        assert_eq!(runtime_state.owner_partition_runtime_stats.len(), 1);
        assert_eq!(runtime_state.owner_partition_runtime_stats[0].observed_batch_count, 1);
        assert_eq!(runtime_state.owner_partition_runtime_stats[0].observed_item_count, 1);
        assert_eq!(runtime_state.owner_partition_runtime_stats[0].state_key_count, 1);

        for (offset, record) in activation.changelog_records.iter().enumerate() {
            let entry = streams_entry_from_record(record);
            assert!(local_state.apply_streams_changelog_entry(0, offset as i64, &entry)?);
        }

        let acct_1 = build_stream_job_query_output_on_local_state(
            &local_state,
            &handle,
            &keyed_rollup_query(&handle, "positiveAmountsByKey", "acct_1"),
        )?
        .expect("filtered query output should exist for acct_1");
        assert_eq!(acct_1["accountId"], "acct_1");
        assert_eq!(acct_1["totalAmount"], json!(5.0));

        assert!(
            local_state
                .load_stream_job_view_state(&handle.handle_id, "positiveAmounts", "acct_2")?
                .is_none()
        );
        let acct_2_error = build_stream_job_query_output_on_local_state(
            &local_state,
            &handle,
            &keyed_rollup_query(&handle, "positiveAmountsByKey", "acct_2"),
        )
        .expect_err("fully filtered key should not materialize");
        assert!(acct_2_error.to_string().contains("not materialized"));

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_v2_bounded_activation_maps_source_items() -> Result<()> {
        let local_state = LocalThroughputState::open(
            &temp_path("stream-jobs-aggregate-v2-map-db"),
            &temp_path("stream-jobs-aggregate-v2-map-checkpoints"),
            3,
        )
        .expect("local state should open");
        let handle = StreamJobBridgeHandleRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            tenant_id: "tenant-a".to_owned(),
            instance_id: "instance-a".to_owned(),
            run_id: "run-a".to_owned(),
            stream_instance_id: "stream-a".to_owned(),
            stream_run_id: "stream-run-a".to_owned(),
            job_id: "job-map".to_owned(),
            handle_id: "handle-map".to_owned(),
            bridge_request_id: "bridge-map".to_owned(),
            origin_kind: StreamJobOriginKind::Workflow.as_str().to_owned(),
            definition_id: "normalized-risk".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-map".to_owned()),
            job_name: "normalized-risk".to_owned(),
            input_ref: json!({
                "kind": "bounded_items",
                "items": [
                    { "accountId": "acct_1", "riskPoints": 60.0 },
                    { "accountId": "acct_1", "riskPoints": 40.0 },
                    { "accountId": "acct_2", "riskPoints": 90.0 }
                ]
            })
            .to_string(),
            config_ref: Some(
                json!({
                    "name": "normalized-risk",
                    "runtime": "aggregate_v2",
                    "source": {
                        "kind": "bounded_input"
                    },
                    "keyBy": "accountId",
                    "states": [
                        {
                            "id": "risk-state",
                            "kind": "keyed",
                            "keyFields": ["accountId"],
                            "valueFields": ["avgRisk"]
                        }
                    ],
                    "operators": [
                        {
                            "kind": "map",
                            "operatorId": "normalize-risk",
                            "config": {
                                "inputField": "riskPoints",
                                "outputField": "risk",
                                "multiplyBy": 0.01
                            }
                        },
                        {
                            "kind": "aggregate",
                            "operatorId": "avg-risk",
                            "config": {
                                "reducer": "avg",
                                "valueField": "risk",
                                "outputField": "avgRisk"
                            },
                            "stateIds": ["risk-state"]
                        },
                        {
                            "kind": "materialize",
                            "operatorId": "materialize-risk",
                            "config": {
                                "view": "riskScores"
                            },
                            "stateIds": ["risk-state"]
                        }
                    ],
                    "views": [
                        {
                            "name": "riskScores",
                            "consistency": "strong",
                            "queryMode": "by_key",
                            "keyField": "accountId"
                        }
                    ],
                    "queries": [
                        {
                            "name": "riskScoresByKey",
                            "viewName": "riskScores",
                            "consistency": "strong"
                        }
                    ]
                })
                .to_string(),
            ),
            checkpoint_policy: None,
            view_definitions: None,
            status: StreamJobBridgeHandleStatus::Admitted.as_str().to_owned(),
            workflow_owner_epoch: Some(1),
            stream_owner_epoch: Some(1),
            cancellation_requested_at: None,
            cancellation_reason: None,
            terminal_event_id: None,
            terminal_at: None,
            workflow_accepted_at: None,
            terminal_output: None,
            terminal_error: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let activation = activate_stream_job_on_local_state(
            None,
            None,
            &local_state,
            &handle,
            8,
            Some(1),
            Utc::now(),
        )
        .await
        .expect("mapped aggregate_v2 activation should succeed")
        .expect("mapped aggregate_v2 activation should produce work");

        for (offset, record) in activation.changelog_records.iter().enumerate() {
            let entry = streams_entry_from_record(record);
            assert!(local_state.apply_streams_changelog_entry(0, offset as i64, &entry)?);
        }

        let acct_1 = build_stream_job_query_output_on_local_state(
            &local_state,
            &handle,
            &keyed_rollup_query(&handle, "riskScoresByKey", "acct_1"),
        )?
        .expect("mapped query output should exist for acct_1");
        assert_eq!(acct_1["accountId"], "acct_1");
        assert_eq!(acct_1["avgRisk"], json!(0.5));

        let acct_2 = build_stream_job_query_output_on_local_state(
            &local_state,
            &handle,
            &keyed_rollup_query(&handle, "riskScoresByKey", "acct_2"),
        )?
        .expect("mapped query output should exist for acct_2");
        assert_eq!(acct_2["avgRisk"], json!(0.9));

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_v2_bounded_activation_preserves_pre_key_operator_order() -> Result<()> {
        let local_state = LocalThroughputState::open(
            &temp_path("stream-jobs-aggregate-v2-pre-key-order-db"),
            &temp_path("stream-jobs-aggregate-v2-pre-key-order-checkpoints"),
            3,
        )
        .expect("local state should open");
        let handle = StreamJobBridgeHandleRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            tenant_id: "tenant-a".to_owned(),
            instance_id: "instance-a".to_owned(),
            run_id: "run-a".to_owned(),
            stream_instance_id: "stream-a".to_owned(),
            stream_run_id: "stream-run-a".to_owned(),
            job_id: "job-pre-key-order".to_owned(),
            handle_id: "handle-pre-key-order".to_owned(),
            bridge_request_id: "bridge-pre-key-order".to_owned(),
            origin_kind: StreamJobOriginKind::Workflow.as_str().to_owned(),
            definition_id: "ordered-pre-key".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-pre-key-order".to_owned()),
            job_name: "ordered-pre-key".to_owned(),
            input_ref: json!({
                "kind": "bounded_items",
                "items": [
                    { "accountId": "acct_1", "amount": 10.0, "riskPoints": 60.0 },
                    { "accountId": "acct_1", "amount": -5.0, "riskPoints": "bad" },
                    { "accountId": "acct_1", "amount": 20.0, "riskPoints": 40.0 }
                ]
            })
            .to_string(),
            config_ref: Some(
                json!({
                    "name": "ordered-pre-key",
                    "runtime": "aggregate_v2",
                    "source": {
                        "kind": "bounded_input"
                    },
                    "keyBy": "accountId",
                    "states": [
                        {
                            "id": "risk-state",
                            "kind": "keyed",
                            "keyFields": ["accountId"],
                            "valueFields": ["avgRisk"]
                        }
                    ],
                    "operators": [
                        {
                            "kind": "filter",
                            "operatorId": "filter-positive",
                            "config": {
                                "predicate": "amount > 0"
                            }
                        },
                        {
                            "kind": "map",
                            "operatorId": "normalize-risk",
                            "config": {
                                "inputField": "riskPoints",
                                "outputField": "risk",
                                "multiplyBy": 0.01,
                                "add": 0.1
                            }
                        },
                        {
                            "kind": "aggregate",
                            "operatorId": "avg-risk",
                            "config": {
                                "reducer": "avg",
                                "valueField": "risk",
                                "outputField": "avgRisk"
                            },
                            "stateIds": ["risk-state"]
                        },
                        {
                            "kind": "materialize",
                            "operatorId": "materialize-risk",
                            "config": {
                                "view": "riskScores"
                            },
                            "stateIds": ["risk-state"]
                        }
                    ],
                    "views": [
                        {
                            "name": "riskScores",
                            "consistency": "strong",
                            "queryMode": "by_key",
                            "keyField": "accountId"
                        }
                    ],
                    "queries": [
                        {
                            "name": "riskScoresByKey",
                            "viewName": "riskScores",
                            "consistency": "strong"
                        }
                    ]
                })
                .to_string(),
            ),
            checkpoint_policy: None,
            view_definitions: None,
            status: StreamJobBridgeHandleStatus::Admitted.as_str().to_owned(),
            workflow_owner_epoch: Some(1),
            stream_owner_epoch: Some(1),
            cancellation_requested_at: None,
            cancellation_reason: None,
            terminal_event_id: None,
            terminal_at: None,
            workflow_accepted_at: None,
            terminal_output: None,
            terminal_error: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let activation = activate_stream_job_on_local_state(
            None,
            None,
            &local_state,
            &handle,
            8,
            Some(1),
            Utc::now(),
        )
        .await
        .expect("ordered pre-key aggregate_v2 activation should succeed")
        .expect("ordered pre-key aggregate_v2 activation should produce work");

        for (offset, record) in activation.changelog_records.iter().enumerate() {
            let entry = streams_entry_from_record(record);
            assert!(local_state.apply_streams_changelog_entry(0, offset as i64, &entry)?);
        }

        let acct_1 = build_stream_job_query_output_on_local_state(
            &local_state,
            &handle,
            &keyed_rollup_query(&handle, "riskScoresByKey", "acct_1"),
        )?
        .expect("ordered pre-key query output should exist for acct_1");
        assert_eq!(acct_1["avgRisk"], json!(0.6));

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_v2_bounded_activation_routes_then_filters_source_items() -> Result<()> {
        let local_state = LocalThroughputState::open(
            &temp_path("stream-jobs-aggregate-v2-route-db"),
            &temp_path("stream-jobs-aggregate-v2-route-checkpoints"),
            3,
        )
        .expect("local state should open");
        let handle = StreamJobBridgeHandleRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            tenant_id: "tenant-a".to_owned(),
            instance_id: "instance-a".to_owned(),
            run_id: "run-a".to_owned(),
            stream_instance_id: "stream-a".to_owned(),
            stream_run_id: "stream-run-a".to_owned(),
            job_id: "job-route".to_owned(),
            handle_id: "handle-route".to_owned(),
            bridge_request_id: "bridge-route".to_owned(),
            origin_kind: StreamJobOriginKind::Workflow.as_str().to_owned(),
            definition_id: "high-risk-route".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-route".to_owned()),
            job_name: "high-risk-route".to_owned(),
            input_ref: json!({
                "kind": "bounded_items",
                "items": [
                    { "accountId": "acct_1", "risk": 0.95 },
                    { "accountId": "acct_1", "risk": 0.82 },
                    { "accountId": "acct_1", "risk": 0.55 },
                    { "accountId": "acct_2", "risk": 0.40 }
                ]
            })
            .to_string(),
            config_ref: Some(
                json!({
                    "name": "high-risk-route",
                    "runtime": "aggregate_v2",
                    "source": {
                        "kind": "bounded_input"
                    },
                    "keyBy": "accountId",
                    "states": [
                        {
                            "id": "high-risk-state",
                            "kind": "keyed",
                            "keyFields": ["accountId"],
                            "valueFields": ["highRiskCount"]
                        }
                    ],
                    "operators": [
                        {
                            "kind": "route",
                            "operatorId": "bucket-risk",
                            "config": {
                                "outputField": "riskBucket",
                                "branches": [
                                    { "predicate": "risk >= 0.8", "value": "high" },
                                    { "predicate": "risk >= 0.5", "value": "medium" }
                                ],
                                "defaultValue": "low"
                            }
                        },
                        {
                            "kind": "filter",
                            "operatorId": "keep-high",
                            "config": {
                                "predicate": "riskBucket == \"high\""
                            }
                        },
                        {
                            "kind": "aggregate",
                            "operatorId": "count-high",
                            "config": {
                                "reducer": "count",
                                "outputField": "highRiskCount"
                            },
                            "stateIds": ["high-risk-state"]
                        },
                        {
                            "kind": "materialize",
                            "operatorId": "materialize-high",
                            "config": {
                                "view": "highRiskCounts"
                            },
                            "stateIds": ["high-risk-state"]
                        }
                    ],
                    "views": [
                        {
                            "name": "highRiskCounts",
                            "consistency": "strong",
                            "queryMode": "by_key",
                            "keyField": "accountId"
                        }
                    ],
                    "queries": [
                        {
                            "name": "highRiskCountsByKey",
                            "viewName": "highRiskCounts",
                            "consistency": "strong"
                        }
                    ]
                })
                .to_string(),
            ),
            checkpoint_policy: None,
            view_definitions: None,
            status: StreamJobBridgeHandleStatus::Admitted.as_str().to_owned(),
            workflow_owner_epoch: Some(1),
            stream_owner_epoch: Some(1),
            cancellation_requested_at: None,
            cancellation_reason: None,
            terminal_event_id: None,
            terminal_at: None,
            workflow_accepted_at: None,
            terminal_output: None,
            terminal_error: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let activation = activate_stream_job_on_local_state(
            None,
            None,
            &local_state,
            &handle,
            8,
            Some(1),
            Utc::now(),
        )
        .await
        .expect("routed aggregate_v2 activation should succeed")
        .expect("routed aggregate_v2 activation should produce work");

        for (offset, record) in activation.changelog_records.iter().enumerate() {
            let entry = streams_entry_from_record(record);
            assert!(local_state.apply_streams_changelog_entry(0, offset as i64, &entry)?);
        }

        let acct_1 = build_stream_job_query_output_on_local_state(
            &local_state,
            &handle,
            &keyed_rollup_query(&handle, "highRiskCountsByKey", "acct_1"),
        )?
        .expect("routed query output should exist for acct_1");
        assert_eq!(acct_1["highRiskCount"], json!(2.0));

        assert!(
            local_state
                .load_stream_job_view_state(&handle.handle_id, "highRiskCounts", "acct_2")?
                .is_none()
        );

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_v2_bounded_activation_materializes_threshold_view() -> Result<()> {
        let local_state = LocalThroughputState::open(
            &temp_path("stream-jobs-aggregate-v2-threshold-db"),
            &temp_path("stream-jobs-aggregate-v2-threshold-checkpoints"),
            3,
        )
        .expect("local state should open");
        let handle = StreamJobBridgeHandleRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            tenant_id: "tenant-a".to_owned(),
            instance_id: "instance-a".to_owned(),
            run_id: "run-a".to_owned(),
            stream_instance_id: "stream-a".to_owned(),
            stream_run_id: "stream-run-a".to_owned(),
            job_id: "job-threshold".to_owned(),
            handle_id: "handle-threshold".to_owned(),
            bridge_request_id: "bridge-threshold".to_owned(),
            origin_kind: StreamJobOriginKind::Workflow.as_str().to_owned(),
            definition_id: "fraud-threshold".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-threshold".to_owned()),
            job_name: "fraud-threshold".to_owned(),
            input_ref: json!({
                "kind": "bounded_items",
                "items": [
                    {
                        "accountId": "acct_1",
                        "risk": 0.96
                    },
                    {
                        "accountId": "acct_1",
                        "risk": 0.99
                    },
                    {
                        "accountId": "acct_2",
                        "risk": 0.72
                    }
                ]
            })
            .to_string(),
            config_ref: Some(
                json!({
                    "name": "fraud-threshold",
                    "runtime": "aggregate_v2",
                    "source": {
                        "kind": "bounded_input"
                    },
                    "keyBy": "accountId",
                    "states": [
                        {
                            "id": "risk-threshold-state",
                            "kind": "keyed",
                            "keyFields": ["accountId"],
                            "valueFields": ["riskExceeded"]
                        }
                    ],
                    "operators": [
                        {
                            "kind": "aggregate",
                            "operatorId": "risk-threshold",
                            "config": {
                                "reducer": "threshold",
                                "valueField": "risk",
                                "threshold": 0.97,
                                "comparison": "gte",
                                "outputField": "riskExceeded"
                            },
                            "stateIds": ["risk-threshold-state"]
                        },
                        {
                            "kind": "materialize",
                            "operatorId": "materialize-threshold",
                            "config": {
                                "view": "riskThresholds"
                            },
                            "stateIds": ["risk-threshold-state"]
                        }
                    ],
                    "views": [
                        {
                            "name": "riskThresholds",
                            "consistency": "strong",
                            "queryMode": "by_key",
                            "keyField": "accountId"
                        }
                    ],
                    "queries": [
                        {
                            "name": "riskThresholdsByKey",
                            "viewName": "riskThresholds",
                            "consistency": "strong"
                        }
                    ]
                })
                .to_string(),
            ),
            checkpoint_policy: None,
            view_definitions: None,
            status: StreamJobBridgeHandleStatus::Admitted.as_str().to_owned(),
            workflow_owner_epoch: Some(1),
            stream_owner_epoch: Some(1),
            cancellation_requested_at: None,
            cancellation_reason: None,
            terminal_event_id: None,
            terminal_at: None,
            workflow_accepted_at: None,
            terminal_output: None,
            terminal_error: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let activation = activate_stream_job_on_local_state(
            None,
            None,
            &local_state,
            &handle,
            8,
            Some(1),
            Utc::now(),
        )
        .await
        .expect("threshold aggregate_v2 activation should succeed")
        .expect("threshold aggregate_v2 activation should produce work");

        for (offset, record) in activation.changelog_records.iter().enumerate() {
            let entry = streams_entry_from_record(record);
            assert!(local_state.apply_streams_changelog_entry(0, offset as i64, &entry)?);
        }

        let crossed = local_state
            .load_stream_job_view_state(&handle.handle_id, "riskThresholds", "acct_1")?
            .expect("threshold view for acct_1 should exist");
        assert_eq!(crossed.output["riskExceeded"], json!(true));

        let not_crossed = local_state
            .load_stream_job_view_state(&handle.handle_id, "riskThresholds", "acct_2")?
            .expect("threshold view for acct_2 should exist");
        assert_eq!(not_crossed.output["riskExceeded"], json!(false));

        let crossed_output = build_stream_job_query_output_on_local_state(
            &local_state,
            &handle,
            &keyed_rollup_query(&handle, "riskThresholdsByKey", "acct_1"),
        )?
        .expect("threshold query output should exist");
        assert_eq!(crossed_output["accountId"], "acct_1");
        assert_eq!(crossed_output["riskExceeded"], json!(true));

        let not_crossed_output = build_stream_job_query_output_on_local_state(
            &local_state,
            &handle,
            &keyed_rollup_query(&handle, "riskThresholdsByKey", "acct_2"),
        )?
        .expect("threshold query output should exist");
        assert_eq!(not_crossed_output["accountId"], "acct_2");
        assert_eq!(not_crossed_output["riskExceeded"], json!(false));

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_v2_bounded_activation_materializes_workflow_signal() -> Result<()> {
        let local_state = LocalThroughputState::open(
            &temp_path("stream-jobs-aggregate-v2-threshold-signal-db"),
            &temp_path("stream-jobs-aggregate-v2-threshold-signal-checkpoints"),
            3,
        )
        .expect("local state should open");
        let handle = StreamJobBridgeHandleRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            tenant_id: "tenant-a".to_owned(),
            instance_id: "instance-a".to_owned(),
            run_id: "run-a".to_owned(),
            stream_instance_id: "stream-a".to_owned(),
            stream_run_id: "stream-run-a".to_owned(),
            job_id: "job-threshold-signal".to_owned(),
            handle_id: "handle-threshold-signal".to_owned(),
            bridge_request_id: "bridge-threshold-signal".to_owned(),
            origin_kind: StreamJobOriginKind::Workflow.as_str().to_owned(),
            definition_id: "fraud-threshold-signal".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-threshold-signal".to_owned()),
            job_name: "fraud-threshold-signal".to_owned(),
            input_ref: json!({
                "kind": "bounded_items",
                "items": [
                    {
                        "accountId": "acct_1",
                        "risk": 0.96
                    },
                    {
                        "accountId": "acct_1",
                        "risk": 0.99
                    },
                    {
                        "accountId": "acct_2",
                        "risk": 0.72
                    }
                ]
            })
            .to_string(),
            config_ref: Some(
                json!({
                    "name": "fraud-threshold-signal",
                    "runtime": "aggregate_v2",
                    "source": {
                        "kind": "bounded_input"
                    },
                    "keyBy": "accountId",
                    "states": [
                        {
                            "id": "risk-threshold-state",
                            "kind": "keyed",
                            "keyFields": ["accountId"],
                            "valueFields": ["riskExceeded"]
                        }
                    ],
                    "operators": [
                        {
                            "kind": "aggregate",
                            "operatorId": "risk-threshold",
                            "config": {
                                "reducer": "threshold",
                                "valueField": "risk",
                                "threshold": 0.97,
                                "comparison": "gte",
                                "outputField": "riskExceeded"
                            },
                            "stateIds": ["risk-threshold-state"]
                        },
                        {
                            "kind": "materialize",
                            "operatorId": "materialize-threshold",
                            "config": {
                                "view": "riskThresholds"
                            },
                            "stateIds": ["risk-threshold-state"]
                        },
                        {
                            "kind": "signal_workflow",
                            "operatorId": "notify-fraud",
                            "config": {
                                "view": "riskThresholds",
                                "signalType": "fraud.threshold.crossed",
                                "whenOutputField": "riskExceeded"
                            }
                        }
                    ],
                    "views": [
                        {
                            "name": "riskThresholds",
                            "consistency": "strong",
                            "queryMode": "by_key",
                            "keyField": "accountId"
                        }
                    ],
                    "queries": [
                        {
                            "name": "riskThresholdsByKey",
                            "viewName": "riskThresholds",
                            "consistency": "strong"
                        }
                    ]
                })
                .to_string(),
            ),
            checkpoint_policy: None,
            view_definitions: None,
            status: StreamJobBridgeHandleStatus::Admitted.as_str().to_owned(),
            workflow_owner_epoch: Some(1),
            stream_owner_epoch: Some(1),
            cancellation_requested_at: None,
            cancellation_reason: None,
            terminal_event_id: None,
            terminal_at: None,
            workflow_accepted_at: None,
            terminal_output: None,
            terminal_error: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let activation = activate_stream_job_on_local_state(
            None,
            None,
            &local_state,
            &handle,
            8,
            Some(1),
            Utc::now(),
        )
        .await
        .expect("threshold aggregate_v2 signal activation should succeed")
        .expect("threshold aggregate_v2 signal activation should produce work");

        for (offset, record) in activation.changelog_records.iter().enumerate() {
            let entry = streams_entry_from_record(record);
            assert!(local_state.apply_streams_changelog_entry(0, offset as i64, &entry)?);
        }

        let materialized = materialization_outcome_from_local_state(&local_state, &handle)?
            .expect("signal materialization should exist");
        assert_eq!(materialized.workflow_signals.len(), 1);
        let signal = &materialized.workflow_signals[0];
        assert_eq!(signal.operator_id, "notify-fraud");
        assert_eq!(signal.view_name, "riskThresholds");
        assert_eq!(signal.logical_key, "acct_1");
        assert_eq!(signal.signal_type, "fraud.threshold.crossed");
        assert_eq!(signal.payload["jobId"], "job-threshold-signal");
        assert_eq!(signal.payload["logicalKey"], "acct_1");
        assert_eq!(signal.payload["output"]["riskExceeded"], json!(true));

        let duplicate_free = local_state.load_stream_job_workflow_signal_state(
            &handle.handle_id,
            "notify-fraud",
            "acct_2",
        )?;
        assert!(duplicate_free.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_v2_bounded_activation_materializes_multiple_views() -> Result<()> {
        let local_state = LocalThroughputState::open(
            &temp_path("stream-jobs-aggregate-v2-multi-view-db"),
            &temp_path("stream-jobs-aggregate-v2-multi-view-checkpoints"),
            3,
        )
        .expect("local state should open");
        let handle = StreamJobBridgeHandleRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            tenant_id: "tenant-a".to_owned(),
            instance_id: "instance-a".to_owned(),
            run_id: "run-a".to_owned(),
            stream_instance_id: "stream-a".to_owned(),
            stream_run_id: "stream-run-a".to_owned(),
            job_id: "job-a".to_owned(),
            handle_id: "handle-a".to_owned(),
            bridge_request_id: "bridge-a".to_owned(),
            origin_kind: StreamJobOriginKind::Workflow.as_str().to_owned(),
            definition_id: "fraud-detector".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-a".to_owned()),
            job_name: "fraud-detector".to_owned(),
            input_ref: json!({
                "kind": "bounded_items",
                "items": [
                    {
                        "accountId": "acct_1",
                        "risk": 0.9
                    },
                    {
                        "accountId": "acct_1",
                        "risk": 0.3
                    }
                ]
            })
            .to_string(),
            config_ref: Some(
                json!({
                    "name": "fraud-detector",
                    "runtime": "aggregate_v2",
                    "source": {
                        "kind": "bounded_input"
                    },
                    "keyBy": "accountId",
                    "states": [
                        {
                            "id": "risk-state",
                            "kind": "keyed",
                            "keyFields": ["accountId"],
                            "valueFields": ["avgRisk"]
                        }
                    ],
                    "operators": [
                        {
                            "kind": "aggregate",
                            "operatorId": "avg-risk",
                            "config": {
                                "reducer": "avg",
                                "valueField": "risk",
                                "outputField": "avgRisk"
                            },
                            "stateIds": ["risk-state"]
                        },
                        {
                            "kind": "materialize",
                            "operatorId": "materialize-risk-scores",
                            "config": {
                                "view": "riskScores"
                            },
                            "stateIds": ["risk-state"]
                        },
                        {
                            "kind": "materialize",
                            "operatorId": "materialize-risk-summary",
                            "config": {
                                "view": "riskSummary"
                            },
                            "stateIds": ["risk-state"]
                        }
                    ],
                    "views": [
                        {
                            "name": "riskScores",
                            "consistency": "strong",
                            "queryMode": "by_key",
                            "keyField": "accountId"
                        },
                        {
                            "name": "riskSummary",
                            "consistency": "strong",
                            "queryMode": "by_key",
                            "keyField": "accountId"
                        }
                    ],
                    "queries": [
                        {
                            "name": "riskScoresByKey",
                            "viewName": "riskScores",
                            "consistency": "strong"
                        },
                        {
                            "name": "riskSummaryByKey",
                            "viewName": "riskSummary",
                            "consistency": "strong"
                        }
                    ]
                })
                .to_string(),
            ),
            checkpoint_policy: None,
            view_definitions: None,
            status: StreamJobBridgeHandleStatus::Admitted.as_str().to_owned(),
            workflow_owner_epoch: Some(1),
            stream_owner_epoch: Some(1),
            cancellation_requested_at: None,
            cancellation_reason: None,
            terminal_event_id: None,
            terminal_at: None,
            workflow_accepted_at: None,
            terminal_output: None,
            terminal_error: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let activation = activate_stream_job_on_local_state(
            None,
            None,
            &local_state,
            &handle,
            8,
            Some(1),
            Utc::now(),
        )
        .await?
        .expect("aggregate_v2 activation should produce work");
        assert!(activation.projection_records.is_empty());

        for (offset, record) in activation.changelog_records.iter().enumerate() {
            let entry = streams_entry_from_record(record);
            assert!(local_state.apply_streams_changelog_entry(0, offset as i64, &entry)?);
        }

        let primary_view = local_state
            .load_stream_job_view_state(&handle.handle_id, "riskScores", "acct_1")?
            .expect("primary view state should exist");
        let secondary_view = local_state
            .load_stream_job_view_state(&handle.handle_id, "riskSummary", "acct_1")?
            .expect("secondary view state should exist");
        assert_eq!(primary_view.output["avgRisk"], 0.6);
        assert_eq!(secondary_view.output["avgRisk"], 0.6);
        assert_eq!(secondary_view.output[STREAM_INTERNAL_AVG_SUM_FIELD], 1.2);
        assert_eq!(secondary_view.output[STREAM_INTERNAL_AVG_COUNT_FIELD], 2);

        let secondary_query = keyed_rollup_query(&handle, "riskSummaryByKey", "acct_1");
        let output =
            build_stream_job_query_output_on_local_state(&local_state, &handle, &secondary_query)?
                .expect("secondary aggregate_v2 query output should exist");
        assert_eq!(output["accountId"], "acct_1");
        assert_eq!(output["avgRisk"], 0.6);
        assert!(output.get(STREAM_INTERNAL_AVG_SUM_FIELD).is_none());
        assert!(output.get(STREAM_INTERNAL_AVG_COUNT_FIELD).is_none());

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_v2_bounded_tumbling_window_query_uses_window_start() -> Result<()> {
        let local_state = LocalThroughputState::open(
            &temp_path("stream-jobs-aggregate-v2-window-db"),
            &temp_path("stream-jobs-aggregate-v2-window-checkpoints"),
            3,
        )
        .expect("local state should open");
        let handle = StreamJobBridgeHandleRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            tenant_id: "tenant-a".to_owned(),
            instance_id: "instance-a".to_owned(),
            run_id: "run-a".to_owned(),
            stream_instance_id: "stream-a".to_owned(),
            stream_run_id: "stream-run-a".to_owned(),
            job_id: "job-a".to_owned(),
            handle_id: "handle-a".to_owned(),
            bridge_request_id: "bridge-a".to_owned(),
            origin_kind: StreamJobOriginKind::Workflow.as_str().to_owned(),
            definition_id: "fraud-detector".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-a".to_owned()),
            job_name: "fraud-detector".to_owned(),
            input_ref: json!({
                "kind": "bounded_items",
                "items": [
                    {
                        "accountId": "acct_1",
                        "risk": 0.9,
                        "eventTime": "2026-03-15T10:00:05Z"
                    },
                    {
                        "accountId": "acct_1",
                        "risk": 0.3,
                        "eventTime": "2026-03-15T10:00:20Z"
                    },
                    {
                        "accountId": "acct_1",
                        "risk": 0.6,
                        "eventTime": "2026-03-15T10:01:05Z"
                    }
                ]
            })
            .to_string(),
            config_ref: Some(
                json!({
                    "name": "fraud-detector",
                    "runtime": "aggregate_v2",
                    "source": {
                        "kind": "bounded_input"
                    },
                    "keyBy": "accountId",
                    "states": [
                        {
                            "id": "minute-window",
                            "kind": "window",
                            "keyFields": ["accountId", "windowStart"],
                            "valueFields": ["avgRisk"]
                        },
                        {
                            "id": "risk-state",
                            "kind": "keyed",
                            "keyFields": ["accountId", "windowStart"],
                            "valueFields": ["avgRisk"]
                        }
                    ],
                    "operators": [
                        {
                            "kind": "window",
                            "operatorId": "minute-window",
                            "config": {
                                "mode": "tumbling",
                                "size": "1m",
                                "timeField": "eventTime"
                            },
                            "stateIds": ["minute-window"]
                        },
                        {
                            "kind": "aggregate",
                            "operatorId": "avg-risk",
                            "config": {
                                "reducer": "avg",
                                "valueField": "risk",
                                "outputField": "avgRisk"
                            },
                            "stateIds": ["risk-state"]
                        },
                        {
                            "kind": "materialize",
                            "operatorId": "materialize-risk",
                            "config": {
                                "view": "riskScores"
                            },
                            "stateIds": ["risk-state"]
                        }
                    ],
                    "views": [
                        {
                            "name": "riskScores",
                            "consistency": "strong",
                            "queryMode": "by_key",
                            "keyField": "accountId"
                        }
                    ],
                    "queries": [
                        {
                            "name": "riskScoresByKey",
                            "viewName": "riskScores",
                            "consistency": "strong",
                            "argFields": ["accountId", "windowStart"]
                        }
                    ]
                })
                .to_string(),
            ),
            checkpoint_policy: None,
            view_definitions: None,
            status: StreamJobBridgeHandleStatus::Admitted.as_str().to_owned(),
            workflow_owner_epoch: Some(1),
            stream_owner_epoch: Some(1),
            cancellation_requested_at: None,
            cancellation_reason: None,
            terminal_event_id: None,
            terminal_at: None,
            workflow_accepted_at: None,
            terminal_output: None,
            terminal_error: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let activation = activate_stream_job_on_local_state(
            None,
            None,
            &local_state,
            &handle,
            8,
            Some(1),
            Utc::now(),
        )
        .await?
        .expect("windowed aggregate_v2 activation should produce work");

        for (offset, record) in activation.changelog_records.iter().enumerate() {
            let entry = streams_entry_from_record(record);
            assert!(local_state.apply_streams_changelog_entry(0, offset as i64, &entry)?);
        }

        let first_window_start = "2026-03-15T10:00:00+00:00";
        let first_logical_key = windowed_logical_key("acct_1", first_window_start);
        let first_window_view = local_state
            .load_stream_job_view_state(&handle.handle_id, "riskScores", &first_logical_key)?
            .expect("first window view state should exist");
        assert_eq!(first_window_view.output["accountId"], "acct_1");
        assert_eq!(first_window_view.output["avgRisk"], 0.6);
        assert_eq!(first_window_view.output["windowStart"], first_window_start);
        assert_eq!(first_window_view.output["windowEnd"], "2026-03-15T10:01:00+00:00");

        let second_query = StreamJobQueryRecord {
            query_args: Some(json!({"key":"acct_1","windowStart":"2026-03-15T10:01:00+00:00"})),
            ..keyed_rollup_query(&handle, "riskScoresByKey", "acct_1")
        };
        let second_output =
            build_stream_job_query_output_on_local_state(&local_state, &handle, &second_query)?
                .expect("second window query output should exist");
        assert_eq!(second_output["avgRisk"], 0.6);
        assert_eq!(second_output["windowStart"], "2026-03-15T10:01:00+00:00");
        assert_eq!(second_output["windowEnd"], "2026-03-15T10:02:00+00:00");

        let missing_window_query = keyed_rollup_query(&handle, "riskScoresByKey", "acct_1");
        let error = build_stream_job_query_output_on_local_state(
            &local_state,
            &handle,
            &missing_window_query,
        )
        .expect_err("windowed aggregate_v2 query should require windowStart");
        assert!(error.to_string().contains("windowStart"));

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_v2_bounded_hopping_window_materializes_overlapping_windows() -> Result<()> {
        let local_state = LocalThroughputState::open(
            &temp_path("stream-jobs-aggregate-v2-hopping-window-db"),
            &temp_path("stream-jobs-aggregate-v2-hopping-window-checkpoints"),
            3,
        )
        .expect("local state should open");
        let handle = StreamJobBridgeHandleRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            tenant_id: "tenant-a".to_owned(),
            instance_id: "instance-a".to_owned(),
            run_id: "run-a".to_owned(),
            stream_instance_id: "stream-a".to_owned(),
            stream_run_id: "stream-run-a".to_owned(),
            job_id: "job-a".to_owned(),
            handle_id: "handle-a".to_owned(),
            bridge_request_id: "bridge-a".to_owned(),
            origin_kind: StreamJobOriginKind::Workflow.as_str().to_owned(),
            definition_id: "fraud-detector".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-a".to_owned()),
            job_name: "fraud-detector".to_owned(),
            input_ref: json!({
                "kind": "bounded_items",
                "items": [
                    {
                        "accountId": "acct_1",
                        "risk": 0.9,
                        "eventTime": "2026-03-15T10:01:30Z"
                    },
                    {
                        "accountId": "acct_1",
                        "risk": 0.3,
                        "eventTime": "2026-03-15T10:01:50Z"
                    }
                ]
            })
            .to_string(),
            config_ref: Some(
                json!({
                    "name": "fraud-detector",
                    "runtime": "aggregate_v2",
                    "source": {
                        "kind": "bounded_input"
                    },
                    "keyBy": "accountId",
                    "states": [
                        {
                            "id": "rolling-window",
                            "kind": "window",
                            "keyFields": ["accountId", "windowStart"],
                            "valueFields": ["avgRisk"]
                        },
                        {
                            "id": "risk-state",
                            "kind": "keyed",
                            "keyFields": ["accountId", "windowStart"],
                            "valueFields": ["avgRisk"]
                        }
                    ],
                    "operators": [
                        {
                            "kind": "window",
                            "operatorId": "rolling-window",
                            "config": {
                                "mode": "hopping",
                                "size": "2m",
                                "hop": "1m",
                                "timeField": "eventTime"
                            },
                            "stateIds": ["rolling-window"]
                        },
                        {
                            "kind": "aggregate",
                            "operatorId": "avg-risk",
                            "config": {
                                "reducer": "avg",
                                "valueField": "risk",
                                "outputField": "avgRisk"
                            },
                            "stateIds": ["risk-state"]
                        },
                        {
                            "kind": "materialize",
                            "operatorId": "materialize-risk",
                            "config": {
                                "view": "riskScores"
                            },
                            "stateIds": ["risk-state"]
                        }
                    ],
                    "views": [
                        {
                            "name": "riskScores",
                            "consistency": "strong",
                            "queryMode": "by_key",
                            "keyField": "accountId"
                        }
                    ],
                    "queries": [
                        {
                            "name": "riskScoresByKey",
                            "viewName": "riskScores",
                            "consistency": "strong",
                            "argFields": ["accountId", "windowStart"]
                        }
                    ]
                })
                .to_string(),
            ),
            checkpoint_policy: None,
            view_definitions: None,
            status: StreamJobBridgeHandleStatus::Admitted.as_str().to_owned(),
            workflow_owner_epoch: Some(1),
            stream_owner_epoch: Some(1),
            cancellation_requested_at: None,
            cancellation_reason: None,
            terminal_event_id: None,
            terminal_at: None,
            workflow_accepted_at: None,
            terminal_output: None,
            terminal_error: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let activation = activate_stream_job_on_local_state(
            None,
            None,
            &local_state,
            &handle,
            8,
            Some(1),
            Utc::now(),
        )
        .await?
        .expect("hopping aggregate_v2 activation should produce work");

        for (offset, record) in activation.changelog_records.iter().enumerate() {
            let entry = streams_entry_from_record(record);
            assert!(local_state.apply_streams_changelog_entry(0, offset as i64, &entry)?);
        }

        for window_start in ["2026-03-15T10:00:00+00:00", "2026-03-15T10:01:00+00:00"] {
            let logical_key = windowed_logical_key("acct_1", window_start);
            let view = local_state
                .load_stream_job_view_state(&handle.handle_id, "riskScores", &logical_key)?
                .expect("hopping window view state should exist");
            assert_eq!(view.output["accountId"], "acct_1");
            assert_eq!(view.output["avgRisk"], 0.6);
            assert_eq!(view.output["windowStart"], window_start);
        }

        let earlier_window_query = StreamJobQueryRecord {
            query_args: Some(json!({"key":"acct_1","windowStart":"2026-03-15T10:00:00+00:00"})),
            ..keyed_rollup_query(&handle, "riskScoresByKey", "acct_1")
        };
        let earlier_output = build_stream_job_query_output_on_local_state(
            &local_state,
            &handle,
            &earlier_window_query,
        )?
        .expect("earlier hopping window query output should exist");
        assert_eq!(earlier_output["avgRisk"], 0.6);
        assert_eq!(earlier_output["windowEnd"], "2026-03-15T10:02:00+00:00");

        let later_window_query = StreamJobQueryRecord {
            query_args: Some(json!({"key":"acct_1","windowStart":"2026-03-15T10:01:00+00:00"})),
            ..keyed_rollup_query(&handle, "riskScoresByKey", "acct_1")
        };
        let later_output = build_stream_job_query_output_on_local_state(
            &local_state,
            &handle,
            &later_window_query,
        )?
        .expect("later hopping window query output should exist");
        assert_eq!(later_output["avgRisk"], 0.6);
        assert_eq!(later_output["windowEnd"], "2026-03-15T10:03:00+00:00");

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
        let query = keyed_rollup_query(&handle, "accountTotals", "acct_1");
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

    #[test]
    fn aggregate_v2_prefix_scan_query_returns_paginated_items() -> Result<()> {
        let db_path = temp_path("stream-jobs-window-scan-query-db");
        let checkpoint_dir = temp_path("stream-jobs-window-scan-query-checkpoints");
        let local_state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let handle = windowed_scan_handle();
        let occurred_at = DateTime::parse_from_rfc3339("2026-03-15T10:05:00Z")
            .expect("timestamp should parse")
            .with_timezone(&Utc);
        local_state.upsert_stream_job_runtime_state(&LocalStreamJobRuntimeState {
            handle_id: handle.handle_id.clone(),
            job_id: handle.job_id.clone(),
            job_name: handle.job_name.clone(),
            view_name: "riskScores".to_owned(),
            checkpoint_name: "minute-checkpoint".to_owned(),
            checkpoint_sequence: 3,
            input_item_count: 0,
            materialized_key_count: 3,
            active_partitions: vec![0],
            throughput_partition_count: 1,
            source_kind: Some(STREAM_SOURCE_TOPIC.to_owned()),
            source_name: Some("payments".to_owned()),
            source_cursors: vec![LocalStreamJobSourceCursorState {
                source_partition_id: 0,
                next_offset: 10,
                initial_checkpoint_target_offset: 10,
                last_applied_offset: Some(9),
                last_high_watermark: Some(10),
                last_event_time_watermark: Some(occurred_at),
                last_closed_window_end: Some(
                    DateTime::parse_from_rfc3339("2026-03-15T10:02:00Z")
                        .expect("closed window should parse")
                        .with_timezone(&Utc),
                ),
                pending_window_ends: Vec::new(),
                dropped_late_event_count: 0,
                last_dropped_late_offset: None,
                last_dropped_late_event_at: None,
                last_dropped_late_window_end: None,
                dropped_evicted_window_event_count: 0,
                last_dropped_evicted_window_offset: None,
                last_dropped_evicted_window_event_at: None,
                last_dropped_evicted_window_end: None,
                checkpoint_reached_at: Some(occurred_at),
                updated_at: occurred_at,
            }],
            source_partition_leases: vec![LocalStreamJobSourceLeaseState {
                source_partition_id: 0,
                owner_partition_id: 0,
                owner_epoch: 7,
                lease_token: "lease-0".to_owned(),
                updated_at: occurred_at,
            }],
            dispatch_dataflow_batches: Vec::new(),
            applied_dispatch_batch_ids: Vec::new(),
            dispatch_completed_at: None,
            dispatch_cancelled_at: None,
            stream_owner_epoch: 7,
            planned_at: occurred_at,
            latest_checkpoint_at: Some(occurred_at),
            evicted_window_count: 0,
            last_evicted_window_end: None,
            last_evicted_at: None,
            view_runtime_stats: Vec::new(),
            pre_key_runtime_stats: vec![
                LocalStreamJobPreKeyRuntimeStatsState {
                    operator_id: "bucket-risk".to_owned(),
                    kind: "route".to_owned(),
                    processed_count: 3,
                    dropped_count: 0,
                    failure_count: 0,
                    route_default_count: 1,
                    route_branch_counts: vec![
                        LocalStreamJobRouteBranchRuntimeStatsState {
                            value: "high".to_owned(),
                            matched_count: 1,
                        },
                        LocalStreamJobRouteBranchRuntimeStatsState {
                            value: "medium".to_owned(),
                            matched_count: 1,
                        },
                    ],
                    last_failure: None,
                },
                LocalStreamJobPreKeyRuntimeStatsState {
                    operator_id: "filter-positive".to_owned(),
                    kind: "filter".to_owned(),
                    processed_count: 3,
                    dropped_count: 1,
                    failure_count: 0,
                    route_default_count: 0,
                    route_branch_counts: Vec::new(),
                    last_failure: None,
                },
                LocalStreamJobPreKeyRuntimeStatsState {
                    operator_id: "normalize-risk".to_owned(),
                    kind: "map".to_owned(),
                    processed_count: 2,
                    dropped_count: 0,
                    failure_count: 0,
                    route_default_count: 0,
                    route_branch_counts: Vec::new(),
                    last_failure: None,
                },
            ],
            hot_key_runtime_stats: vec![
                LocalStreamJobHotKeyRuntimeStatsState {
                    display_key: "acct_1".to_owned(),
                    logical_key: windowed_logical_key("acct_1", "2026-03-15T10:01:00+00:00"),
                    observed_count: 5,
                    source_partition_ids: vec![0],
                    last_seen_at: Some(occurred_at),
                },
                LocalStreamJobHotKeyRuntimeStatsState {
                    display_key: "acct_2".to_owned(),
                    logical_key: windowed_logical_key("acct_2", "2026-03-15T10:00:00+00:00"),
                    observed_count: 1,
                    source_partition_ids: vec![0],
                    last_seen_at: Some(occurred_at),
                },
            ],
            owner_partition_runtime_stats: Vec::new(),
            checkpoint_partitions: Vec::new(),
            terminal_status: None,
            terminal_output: None,
            terminal_error: None,
            terminal_at: None,
            updated_at: occurred_at,
        })?;

        let first_key = windowed_logical_key("acct_1", "2026-03-15T10:00:00+00:00");
        let second_key = windowed_logical_key("acct_1", "2026-03-15T10:01:00+00:00");
        let third_key = windowed_logical_key("acct_2", "2026-03-15T10:00:00+00:00");
        for (logical_key, account_id, window_start, avg_risk) in [
            (first_key.as_str(), "acct_1", "2026-03-15T10:00:00+00:00", 0.4),
            (second_key.as_str(), "acct_1", "2026-03-15T10:01:00+00:00", 0.6),
            (third_key.as_str(), "acct_2", "2026-03-15T10:00:00+00:00", 0.8),
        ] {
            local_state.upsert_stream_job_view_value(
                &handle.handle_id,
                &handle.job_id,
                "riskScores",
                logical_key,
                json!({
                    "accountId": account_id,
                    "avgRisk": avg_risk,
                    "windowStart": window_start,
                    "windowEnd": if window_start == "2026-03-15T10:00:00+00:00" {
                        "2026-03-15T10:01:00+00:00"
                    } else {
                        "2026-03-15T10:02:00+00:00"
                    },
                    "asOfCheckpoint": 3
                }),
                3,
                occurred_at,
            )?;
        }

        let mut query = strong_scan_query(&handle, "acct_1");
        let requested_at = occurred_at - chrono::Duration::seconds(150);
        query.requested_at = requested_at;
        query.created_at = requested_at;
        query.updated_at = requested_at;
        let output = build_stream_job_query_output_on_local_state(&local_state, &handle, &query)?
            .expect("scan output should exist");
        assert_eq!(output["prefix"], "acct_1");
        assert_eq!(output["total"], 2);
        assert_eq!(output["offset"], 1);
        assert_eq!(output["limit"], 1);
        assert_eq!(output["consistency"], "strong");
        assert_eq!(output["consistencySource"], "stream_owner_local_state");
        assert_eq!(output["items"].as_array().map(Vec::len), Some(1));
        assert_eq!(output["items"][0]["logicalKey"], second_key);
        assert_eq!(output["items"][0]["accountId"], "acct_1");
        assert_eq!(output["items"][0]["avgRisk"], 0.6);
        assert_eq!(output["items"][0]["windowRetentionSeconds"], 120);
        assert_eq!(output["runtimeStats"]["streamOwnerEpoch"], 7);
        assert_eq!(output["strongReadMetadata"]["ownerEpoch"], 7);
        assert_eq!(output["strongReadMetadata"]["lastDurablePosition"]["0"], 9);
        assert_eq!(
            output["strongReadMetadata"]["lastSealedCheckpoint"]["checkpointName"],
            "minute-checkpoint"
        );
        assert_eq!(output["strongReadMetadata"]["lastSourceFrontier"][0]["nextOffset"], 10);

        std::fs::remove_dir_all(db_path).ok();
        std::fs::remove_dir_all(checkpoint_dir).ok();
        Ok(())
    }

    #[test]
    fn aggregate_v2_window_retention_evicts_closed_views() -> Result<()> {
        let db_path = temp_path("stream-jobs-window-retention-db");
        let checkpoint_dir = temp_path("stream-jobs-window-retention-checkpoints");
        let local_state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let handle = windowed_scan_handle();
        let planned_at = DateTime::parse_from_rfc3339("2026-03-15T10:00:00Z")
            .expect("planned timestamp should parse")
            .with_timezone(&Utc);
        let occurred_at = DateTime::parse_from_rfc3339("2026-03-15T10:04:00Z")
            .expect("eviction timestamp should parse")
            .with_timezone(&Utc);
        let logical_key = windowed_logical_key("acct_1", "2026-03-15T10:00:00+00:00");
        local_state.upsert_stream_job_runtime_state(&LocalStreamJobRuntimeState {
            handle_id: handle.handle_id.clone(),
            job_id: handle.job_id.clone(),
            job_name: handle.job_name.clone(),
            view_name: "riskScores".to_owned(),
            checkpoint_name: "minute-checkpoint".to_owned(),
            checkpoint_sequence: 4,
            input_item_count: 0,
            materialized_key_count: 1,
            active_partitions: vec![0],
            throughput_partition_count: 1,
            source_kind: Some(STREAM_SOURCE_TOPIC.to_owned()),
            source_name: Some("payments".to_owned()),
            source_cursors: vec![LocalStreamJobSourceCursorState {
                source_partition_id: 0,
                next_offset: 12,
                initial_checkpoint_target_offset: 12,
                last_applied_offset: Some(11),
                last_high_watermark: Some(12),
                last_event_time_watermark: Some(occurred_at),
                last_closed_window_end: Some(
                    DateTime::parse_from_rfc3339("2026-03-15T10:03:00Z")
                        .expect("closed window should parse")
                        .with_timezone(&Utc),
                ),
                pending_window_ends: Vec::new(),
                dropped_late_event_count: 0,
                last_dropped_late_offset: None,
                last_dropped_late_event_at: None,
                last_dropped_late_window_end: None,
                dropped_evicted_window_event_count: 0,
                last_dropped_evicted_window_offset: None,
                last_dropped_evicted_window_event_at: None,
                last_dropped_evicted_window_end: None,
                checkpoint_reached_at: Some(occurred_at),
                updated_at: occurred_at,
            }],
            source_partition_leases: vec![LocalStreamJobSourceLeaseState {
                source_partition_id: 0,
                owner_partition_id: 0,
                owner_epoch: 7,
                lease_token: "lease-0".to_owned(),
                updated_at: occurred_at,
            }],
            dispatch_dataflow_batches: Vec::new(),
            applied_dispatch_batch_ids: Vec::new(),
            dispatch_completed_at: None,
            dispatch_cancelled_at: None,
            stream_owner_epoch: 7,
            planned_at,
            latest_checkpoint_at: Some(occurred_at),
            evicted_window_count: 0,
            last_evicted_window_end: None,
            last_evicted_at: None,
            view_runtime_stats: Vec::new(),
            pre_key_runtime_stats: vec![
                LocalStreamJobPreKeyRuntimeStatsState {
                    operator_id: "bucket-risk".to_owned(),
                    kind: "route".to_owned(),
                    processed_count: 3,
                    dropped_count: 0,
                    failure_count: 0,
                    route_default_count: 1,
                    route_branch_counts: vec![
                        LocalStreamJobRouteBranchRuntimeStatsState {
                            value: "high".to_owned(),
                            matched_count: 1,
                        },
                        LocalStreamJobRouteBranchRuntimeStatsState {
                            value: "medium".to_owned(),
                            matched_count: 1,
                        },
                    ],
                    last_failure: None,
                },
                LocalStreamJobPreKeyRuntimeStatsState {
                    operator_id: "filter-positive".to_owned(),
                    kind: "filter".to_owned(),
                    processed_count: 3,
                    dropped_count: 1,
                    failure_count: 0,
                    route_default_count: 0,
                    route_branch_counts: Vec::new(),
                    last_failure: None,
                },
                LocalStreamJobPreKeyRuntimeStatsState {
                    operator_id: "normalize-risk".to_owned(),
                    kind: "map".to_owned(),
                    processed_count: 2,
                    dropped_count: 0,
                    failure_count: 0,
                    route_default_count: 0,
                    route_branch_counts: Vec::new(),
                    last_failure: None,
                },
            ],
            hot_key_runtime_stats: vec![LocalStreamJobHotKeyRuntimeStatsState {
                display_key: "acct_1".to_owned(),
                logical_key: logical_key.clone(),
                observed_count: 3,
                source_partition_ids: vec![0],
                last_seen_at: Some(occurred_at),
            }],
            owner_partition_runtime_stats: Vec::new(),
            checkpoint_partitions: Vec::new(),
            terminal_status: None,
            terminal_output: None,
            terminal_error: None,
            terminal_at: None,
            updated_at: occurred_at,
        })?;
        local_state.upsert_stream_job_view_value(
            &handle.handle_id,
            &handle.job_id,
            "riskScores",
            &logical_key,
            json!({
                "accountId": "acct_1",
                "avgRisk": 0.5,
                "windowStart": "2026-03-15T10:00:00+00:00",
                "windowEnd": "2026-03-15T10:01:00+00:00",
                "asOfCheckpoint": 4
            }),
            4,
            planned_at,
        )?;

        let runtime_state = local_state
            .load_stream_job_runtime_state(&handle.handle_id)?
            .expect("runtime state should exist");
        let retention_sweep =
            sweep_expired_stream_job_windows(&local_state, &handle, &runtime_state, occurred_at)?;
        apply_stream_job_window_retention_sweep_on_local_state(
            &local_state,
            &runtime_state,
            &retention_sweep,
            occurred_at,
        )?;
        let projection_records = retention_sweep.projection_records;
        let changelog_records = retention_sweep.changelog_records;
        let updated_runtime_state = local_state
            .load_stream_job_runtime_state(&handle.handle_id)?
            .expect("runtime state should still exist after eviction");

        assert!(
            local_state
                .load_stream_job_view_state(&handle.handle_id, "riskScores", &logical_key)?
                .is_none()
        );
        assert_eq!(projection_records.len(), 1);
        assert_eq!(changelog_records.len(), 1);
        assert_eq!(updated_runtime_state.evicted_window_count, 1);
        assert_eq!(
            updated_runtime_state.last_evicted_window_end,
            Some(
                DateTime::parse_from_rfc3339("2026-03-15T10:01:00Z")
                    .expect("window end should parse")
                    .with_timezone(&Utc)
            )
        );
        assert_eq!(updated_runtime_state.last_evicted_at, Some(occurred_at));
        assert_eq!(
            updated_runtime_state
                .view_runtime_stats("riskScores")
                .map(|stats| stats.evicted_window_count),
            Some(1)
        );
        assert_eq!(
            updated_runtime_state
                .view_runtime_stats("riskScores")
                .and_then(|stats| stats.last_evicted_window_end),
            Some(
                DateTime::parse_from_rfc3339("2026-03-15T10:01:00Z")
                    .expect("window end should parse")
                    .with_timezone(&Utc)
            )
        );
        assert_eq!(
            updated_runtime_state
                .view_runtime_stats("riskScores")
                .and_then(|stats| stats.last_evicted_at),
            Some(occurred_at)
        );
        match &projection_records[0].event {
            crate::ProjectionRecordEvent::Streams(
                StreamsProjectionEvent::DeleteStreamJobView { view },
            ) => {
                assert_eq!(view.view_name, "riskScores");
                assert_eq!(view.logical_key, logical_key);
            }
            other => panic!("unexpected projection payload: {other:?}"),
        }
        match &changelog_records[0].entry {
            crate::ChangelogRecordEntry::Streams(StreamsChangelogEntry {
                payload:
                    StreamsChangelogPayload::StreamJobViewEvicted {
                        view_name,
                        logical_key: evicted_key,
                        evicted_window_count,
                        ..
                    },
                ..
            }) => {
                assert_eq!(view_name, "riskScores");
                assert_eq!(evicted_key, &logical_key);
                assert_eq!(*evicted_window_count, 1);
            }
            other => panic!("unexpected changelog payload: {other:?}"),
        }

        std::fs::remove_dir_all(db_path).ok();
        std::fs::remove_dir_all(checkpoint_dir).ok();
        Ok(())
    }

    #[test]
    fn windowed_admission_counts_events_for_evicted_windows() -> Result<()> {
        let cursor = LocalStreamJobSourceCursorState {
            source_partition_id: 0,
            next_offset: 12,
            initial_checkpoint_target_offset: 12,
            last_applied_offset: Some(11),
            last_high_watermark: Some(12),
            last_event_time_watermark: Some(
                DateTime::parse_from_rfc3339("2026-03-15T10:03:00Z")
                    .expect("watermark should parse")
                    .with_timezone(&Utc),
            ),
            last_closed_window_end: Some(
                DateTime::parse_from_rfc3339("2026-03-15T10:03:00Z")
                    .expect("closed window should parse")
                    .with_timezone(&Utc),
            ),
            pending_window_ends: Vec::new(),
            dropped_late_event_count: 0,
            last_dropped_late_offset: None,
            last_dropped_late_event_at: None,
            last_dropped_late_window_end: None,
            dropped_evicted_window_event_count: 0,
            last_dropped_evicted_window_offset: None,
            last_dropped_evicted_window_event_at: None,
            last_dropped_evicted_window_end: None,
            checkpoint_reached_at: None,
            updated_at: Utc::now(),
        };
        let plan = BoundedStreamAggregationPlan {
            key_field: "accountId".to_owned(),
            reducer_kind: STREAM_REDUCER_AVG.to_owned(),
            value_field: Some("risk".to_owned()),
            output_field: "avgRisk".to_owned(),
            pre_key_operators: Vec::new(),
            threshold: None,
            threshold_comparison: StreamThresholdComparison::Gte,
            view_name: "riskScores".to_owned(),
            additional_view_names: Vec::new(),
            eventual_projection_view_names: Vec::new(),
            workflow_signals: Vec::new(),
            view_retention_seconds: Some(120),
            window_time_field: Some("eventTime".to_owned()),
            window_mode: Some(StreamWindowMode::Tumbling),
            window_size: Some(ChronoDuration::minutes(1)),
            window_hop: None,
            allowed_lateness: Some(ChronoDuration::seconds(10)),
            checkpoint_name: "minute-checkpoint".to_owned(),
            checkpoint_sequence: 1,
        };
        let occurred_at = DateTime::parse_from_rfc3339("2026-03-15T10:04:00Z")
            .expect("occurred_at should parse")
            .with_timezone(&Utc);
        let record = TopicSourceBatchRecord {
            logical_key: windowed_logical_key("acct_1", "2026-03-15T10:00:00+00:00"),
            display_key: "acct_1".to_owned(),
            window_start: Some("2026-03-15T10:00:00+00:00".to_owned()),
            window_end: Some("2026-03-15T10:01:00+00:00".to_owned()),
            event_time: Some(
                DateTime::parse_from_rfc3339("2026-03-15T10:00:20Z")
                    .expect("event time should parse")
                    .with_timezone(&Utc),
            ),
            window_end_at: Some(
                DateTime::parse_from_rfc3339("2026-03-15T10:01:00Z")
                    .expect("window end should parse")
                    .with_timezone(&Utc),
            ),
            value: 0.9,
            offset: 12,
            high_watermark: 12,
        };

        let admitted = admit_windowed_source_records(&[record], &cursor, &plan, occurred_at)?;
        assert!(admitted.accepted_records.is_empty());
        assert_eq!(admitted.dropped_late_event_count, 0);
        assert_eq!(admitted.dropped_evicted_window_event_count, 1);
        assert_eq!(admitted.last_dropped_evicted_window_offset, Some(12));
        assert_eq!(
            admitted.last_dropped_evicted_window_end,
            Some(
                DateTime::parse_from_rfc3339("2026-03-15T10:01:00Z")
                    .expect("window end should parse")
                    .with_timezone(&Utc)
            )
        );
        Ok(())
    }

    #[test]
    fn stream_job_runtime_stats_query_returns_source_cursor_diagnostics() {
        let db_path = temp_path("stream-jobs-runtime-stats-query-db");
        let checkpoint_dir = temp_path("stream-jobs-runtime-stats-query-checkpoints");
        let local_state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)
            .expect("local state should open");
        let occurred_at = DateTime::parse_from_rfc3339("2026-03-15T10:02:00Z")
            .expect("timestamp should parse")
            .with_timezone(&Utc);
        let shard_checkpoint_at = occurred_at - chrono::Duration::seconds(30);
        let stream_checkpoint_at = occurred_at - chrono::Duration::seconds(5);
        let handle = windowed_scan_handle();
        local_state.record_checkpoint_write(shard_checkpoint_at);
        local_state
            .upsert_stream_job_runtime_state(&LocalStreamJobRuntimeState {
                handle_id: handle.handle_id.clone(),
                job_id: handle.job_id.clone(),
                job_name: handle.job_name.clone(),
                view_name: "riskScores".to_owned(),
                checkpoint_name: "initial-risk-ready".to_owned(),
                checkpoint_sequence: 1,
                input_item_count: 0,
                materialized_key_count: 1,
                active_partitions: vec![0],
                throughput_partition_count: 1,
                source_kind: Some(STREAM_SOURCE_TOPIC.to_owned()),
                source_name: Some("payments".to_owned()),
                source_cursors: vec![LocalStreamJobSourceCursorState {
                    source_partition_id: 0,
                    next_offset: 3,
                    initial_checkpoint_target_offset: 3,
                    last_applied_offset: Some(2),
                    last_high_watermark: Some(3),
                    last_event_time_watermark: Some(
                        DateTime::parse_from_rfc3339("2026-03-15T10:01:35Z")
                            .expect("event watermark should parse")
                            .with_timezone(&Utc),
                    ),
                    last_closed_window_end: Some(
                        DateTime::parse_from_rfc3339("2026-03-15T10:01:00Z")
                            .expect("closed window should parse")
                            .with_timezone(&Utc),
                    ),
                    pending_window_ends: Vec::new(),
                    dropped_late_event_count: 1,
                    last_dropped_late_offset: Some(2),
                    last_dropped_late_event_at: Some(
                        DateTime::parse_from_rfc3339("2026-03-15T10:00:20Z")
                            .expect("dropped event should parse")
                            .with_timezone(&Utc),
                    ),
                    last_dropped_late_window_end: Some(
                        DateTime::parse_from_rfc3339("2026-03-15T10:01:00Z")
                            .expect("dropped window should parse")
                            .with_timezone(&Utc),
                    ),
                    dropped_evicted_window_event_count: 0,
                    last_dropped_evicted_window_offset: None,
                    last_dropped_evicted_window_event_at: None,
                    last_dropped_evicted_window_end: None,
                    checkpoint_reached_at: Some(occurred_at),
                    updated_at: occurred_at,
                }],
                source_partition_leases: vec![LocalStreamJobSourceLeaseState {
                    source_partition_id: 0,
                    owner_partition_id: 0,
                    owner_epoch: 7,
                    lease_token: "lease-0".to_owned(),
                    updated_at: occurred_at,
                }],
                dispatch_dataflow_batches: Vec::new(),
                applied_dispatch_batch_ids: Vec::new(),
                dispatch_completed_at: None,
                dispatch_cancelled_at: None,
                stream_owner_epoch: 7,
                planned_at: occurred_at,
                latest_checkpoint_at: Some(stream_checkpoint_at),
                evicted_window_count: 0,
                last_evicted_window_end: None,
                last_evicted_at: None,
                view_runtime_stats: Vec::new(),
                pre_key_runtime_stats: vec![
                    LocalStreamJobPreKeyRuntimeStatsState {
                        operator_id: "bucket-risk".to_owned(),
                        kind: "route".to_owned(),
                        processed_count: 3,
                        dropped_count: 0,
                        failure_count: 0,
                        route_default_count: 1,
                        route_branch_counts: vec![
                            LocalStreamJobRouteBranchRuntimeStatsState {
                                value: "high".to_owned(),
                                matched_count: 1,
                            },
                            LocalStreamJobRouteBranchRuntimeStatsState {
                                value: "medium".to_owned(),
                                matched_count: 1,
                            },
                        ],
                        last_failure: None,
                    },
                    LocalStreamJobPreKeyRuntimeStatsState {
                        operator_id: "filter-positive".to_owned(),
                        kind: "filter".to_owned(),
                        processed_count: 3,
                        dropped_count: 1,
                        failure_count: 0,
                        route_default_count: 0,
                        route_branch_counts: Vec::new(),
                        last_failure: None,
                    },
                    LocalStreamJobPreKeyRuntimeStatsState {
                        operator_id: "normalize-risk".to_owned(),
                        kind: "map".to_owned(),
                        processed_count: 2,
                        dropped_count: 0,
                        failure_count: 0,
                        route_default_count: 0,
                        route_branch_counts: Vec::new(),
                        last_failure: None,
                    },
                ],
                hot_key_runtime_stats: vec![
                    LocalStreamJobHotKeyRuntimeStatsState {
                        display_key: "acct_1".to_owned(),
                        logical_key: windowed_logical_key("acct_1", "2026-03-15T10:01:00+00:00"),
                        observed_count: 5,
                        source_partition_ids: vec![0],
                        last_seen_at: Some(occurred_at),
                    },
                    LocalStreamJobHotKeyRuntimeStatsState {
                        display_key: "acct_2".to_owned(),
                        logical_key: windowed_logical_key("acct_2", "2026-03-15T10:00:00+00:00"),
                        observed_count: 1,
                        source_partition_ids: vec![0],
                        last_seen_at: Some(occurred_at),
                    },
                ],
                owner_partition_runtime_stats: vec![
                    LocalStreamJobOwnerPartitionRuntimeStatsState {
                        stream_partition_id: 0,
                        observed_batch_count: 2,
                        observed_item_count: 3,
                        last_batch_item_count: 2,
                        max_batch_item_count: 2,
                        state_key_count: 2,
                        source_partition_ids: vec![0],
                        last_updated_at: Some(occurred_at),
                    },
                ],
                checkpoint_partitions: vec![LocalStreamJobCheckpointState {
                    handle_id: handle.handle_id.clone(),
                    job_id: handle.job_id.clone(),
                    checkpoint_name: "initial-risk-ready".to_owned(),
                    checkpoint_sequence: 1,
                    stream_partition_id: 0,
                    stream_owner_epoch: 7,
                    reached_at: stream_checkpoint_at,
                    updated_at: stream_checkpoint_at,
                }],
                terminal_status: None,
                terminal_output: None,
                terminal_error: None,
                terminal_at: None,
                updated_at: occurred_at,
            })
            .expect("runtime state should persist");

        let mut query = stream_job_runtime_stats_query(&handle, Some(0));
        query.requested_at = occurred_at;
        let output = build_stream_job_query_output_on_local_state(&local_state, &handle, &query)
            .expect("runtime stats output should build")
            .expect("runtime stats output should exist");
        assert_eq!(output["queryName"], STREAM_JOB_RUNTIME_STATS_QUERY_NAME);
        assert_eq!(output["runtimeContract"], STREAMS_DATAFLOW_V1_CONTRACT);
        assert_eq!(output["jobRuntime"], STREAM_RUNTIME_AGGREGATE_V2);
        assert_eq!(output["dataflowPlan"]["streamRuntime"], STREAM_RUNTIME_AGGREGATE_V2);
        assert_eq!(output["dataflowPlan"]["exchangeEdgeCount"], 1);
        assert_eq!(output["sourceKind"], STREAM_SOURCE_TOPIC);
        assert_eq!(output["sourceName"], "payments");
        assert_eq!(output["streamOwnerEpoch"], 7);
        assert_eq!(output["sourcePartitionId"], 0);
        assert_eq!(output["consistency"], "strong");
        assert_eq!(output["consistencySource"], "stream_owner_local_state");
        assert_eq!(output["windowPolicy"]["mode"], "tumbling");
        assert_eq!(output["windowPolicy"]["size"], "1m");
        assert_eq!(output["windowPolicy"]["timeField"], "eventTime");
        assert_eq!(output["windowPolicy"]["allowedLateness"], "10s");
        assert_eq!(output["windowPolicy"]["retentionSeconds"], 120);
        assert_eq!(output["windowPolicy"]["checkpointReadiness"], "closed_windows");
        assert_eq!(output["windowPolicy"]["lateEventPolicy"], "drop_after_closed_window");
        assert_eq!(output["windowPolicy"]["retentionEvictionEnabled"], true);
        assert_eq!(output["windowPolicy"]["evictedWindowEventPolicy"], "drop_after_retention");
        assert_eq!(output["preKeyPolicy"][0]["kind"], "route");
        assert_eq!(output["preKeyPolicy"][0]["operatorId"], "bucket-risk");
        assert_eq!(output["preKeyPolicy"][0]["outputField"], "riskBucket");
        assert_eq!(output["preKeyPolicy"][0]["branches"][0]["value"], "high");
        assert_eq!(output["preKeyPolicy"][0]["defaultValue"], "low");
        assert_eq!(output["preKeyPolicy"][1]["kind"], "filter");
        assert_eq!(output["preKeyPolicy"][1]["predicate"], "amount > 0");
        assert_eq!(output["preKeyPolicy"][2]["kind"], "map");
        assert_eq!(output["preKeyPolicy"][2]["inputField"], "riskPoints");
        assert_eq!(output["preKeyPolicy"][2]["outputField"], "risk");
        assert_eq!(output["preKeyPolicy"][2]["multiplyBy"], 0.01);
        assert_eq!(output["preKeyStats"]["totals"]["processedCount"], 8);
        assert_eq!(output["preKeyStats"]["totals"]["droppedCount"], 1);
        assert_eq!(output["preKeyStats"]["totals"]["failureCount"], 0);
        assert_eq!(
            output["preKeyStats"]["operators"][0]["routeBranchCounts"][0]["matchedCount"],
            1
        );
        assert_eq!(output["preKeyStats"]["operators"][0]["routeDefaultCount"], 1);
        assert_eq!(
            output["preKeyStats"]["operators"][1]["dropReasons"][0]["reason"],
            "predicate_not_matched"
        );
        assert_eq!(output["preKeyStats"]["operators"][1]["dropReasons"][0]["count"], 1);
        assert_eq!(output["preKeyStats"]["operators"][2]["processedCount"], 2);
        assert_eq!(output["materializedViews"][0]["name"], "riskScores");
        assert_eq!(output["materializedViews"][0]["operatorId"], "materialize-risk");
        assert_eq!(output["materializedViews"][0]["stateIds"][0], "risk-state");
        assert_eq!(output["materializedViews"][0]["queryMode"], "prefix_scan");
        assert_eq!(output["materializedViews"][0]["retentionSeconds"], 120);
        assert_eq!(output["materializedViews"][0]["preKeyPolicy"][0]["kind"], "route");
        assert_eq!(output["materializedViews"][0]["lateEventPolicy"], "drop_after_closed_window");
        assert_eq!(output["materializedViews"][0]["retentionEvictionEnabled"], true);
        assert_eq!(
            output["materializedViews"][0]["evictedWindowEventPolicy"],
            "drop_after_retention"
        );
        assert_eq!(output["materializedViews"][0]["windowPolicy"]["mode"], "tumbling");
        assert_eq!(output["materializedViews"][0]["windowPolicy"]["allowedLateness"], "10s");
        assert_eq!(output["materializedViews"][0]["windowPolicy"]["retentionSeconds"], 120);
        assert_eq!(output["sourceCursors"][0]["droppedLateEventCount"], 1);
        assert_eq!(output["sourceCursors"][0]["lastDroppedLateOffset"], 2);
        assert_eq!(
            output["sourceCursors"][0]["lastDroppedLateEventAt"],
            "2026-03-15T10:00:20+00:00"
        );
        assert_eq!(
            output["sourceCursors"][0]["lastDroppedLateWindowEnd"],
            "2026-03-15T10:01:00+00:00"
        );
        assert_eq!(output["sourceLeases"][0]["leaseToken"], "lease-0");
        assert_eq!(output["sourcePartitionStats"]["summary"]["partitionCount"], 1);
        assert_eq!(output["sealedCheckpoint"]["count"], 1);
        assert_eq!(
            output["sealedCheckpoint"]["partitions"][0]["checkpointName"],
            "initial-risk-ready"
        );
        assert_eq!(output["sourcePartitionStats"]["summary"]["caughtUpPartitionCount"], 1);
        assert_eq!(output["sourcePartitionStats"]["summary"]["totalOffsetLag"], 0);
        assert_eq!(output["sourcePartitionStats"]["summary"]["totalCheckpointLag"], 0);
        assert_eq!(output["sourcePartitionStats"]["partitions"][0]["ownerPartitionId"], 0);
        assert_eq!(output["sourcePartitionStats"]["partitions"][0]["offsetLag"], 0);
        assert_eq!(output["sourcePartitionStats"]["partitions"][0]["checkpointLag"], 0);
        assert_eq!(output["hotKeyStats"]["totals"]["observedCount"], 6);
        assert_eq!(output["hotKeyStats"]["totals"]["trackedKeyCount"], 2);
        assert_eq!(output["hotKeyStats"]["topKeys"][0]["displayKey"], "acct_1");
        assert_eq!(output["hotKeyStats"]["topKeys"][0]["observedCount"], 5);
        assert_eq!(output["hotKeyStats"]["topKeys"][1]["displayKey"], "acct_2");
        assert_eq!(output["ownerPartitionStats"]["summary"]["partitionCount"], 1);
        assert_eq!(output["ownerPartitionStats"]["summary"]["totalStateKeyCount"], 2);
        assert_eq!(output["ownerPartitionStats"]["summary"]["totalObservedBatchCount"], 2);
        assert_eq!(output["ownerPartitionStats"]["summary"]["totalObservedItemCount"], 3);
        assert_eq!(output["ownerPartitionStats"]["summary"]["checkpointedPartitionCount"], 1);
        assert_eq!(
            output["ownerPartitionStats"]["summary"]["lastStreamCheckpointAt"],
            stream_checkpoint_at.to_rfc3339()
        );
        assert_eq!(
            output["ownerPartitionStats"]["summary"]["lastShardCheckpointAt"],
            shard_checkpoint_at.to_rfc3339()
        );
        assert_eq!(output["ownerPartitionStats"]["summary"]["shardCheckpointAgeSeconds"], 30);
        assert_eq!(output["ownerPartitionStats"]["summary"]["restoredFromCheckpoint"], false);
        assert_eq!(output["ownerPartitionStats"]["partitions"][0]["streamPartitionId"], 0);
        assert_eq!(output["ownerPartitionStats"]["partitions"][0]["stateKeyCount"], 2);
        assert_eq!(output["ownerPartitionStats"]["partitions"][0]["observedBatchCount"], 2);
        assert_eq!(output["ownerPartitionStats"]["partitions"][0]["maxBatchItemCount"], 2);
        assert_eq!(
            output["ownerPartitionStats"]["partitions"][0]["lastStreamCheckpointAt"],
            stream_checkpoint_at.to_rfc3339()
        );
        assert_eq!(output["ownerPartitionStats"]["partitions"][0]["streamCheckpointAgeSeconds"], 5);
        assert_eq!(
            output["ownerPartitionStats"]["partitions"][0]["shardLastCheckpointAt"],
            shard_checkpoint_at.to_rfc3339()
        );
        assert_eq!(output["ownerPartitionStats"]["partitions"][0]["shardCheckpointAgeSeconds"], 30);
        assert!(
            output["ownerPartitionStats"]["partitions"][0]["checkpointBytes"]
                .as_u64()
                .expect("checkpoint bytes should exist")
                > 0
        );
        assert_eq!(
            output["ownerPartitionStats"]["partitions"][0]["restoreTailLagEntries"],
            Value::Null
        );

        std::fs::remove_dir_all(db_path).ok();
        std::fs::remove_dir_all(checkpoint_dir).ok();
    }

    #[test]
    fn stream_job_runtime_stats_query_surfaces_topic_pre_key_failures() {
        let db_path = temp_path("stream-jobs-runtime-stats-failure-query-db");
        let checkpoint_dir = temp_path("stream-jobs-runtime-stats-failure-query-checkpoints");
        let local_state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)
            .expect("local state should open");
        let occurred_at = Utc::now();
        let handle = windowed_scan_handle();
        local_state
            .upsert_stream_job_runtime_state(&LocalStreamJobRuntimeState {
                handle_id: handle.handle_id.clone(),
                job_id: handle.job_id.clone(),
                job_name: handle.job_name.clone(),
                view_name: "riskScores".to_owned(),
                checkpoint_name: "initial-risk-ready".to_owned(),
                checkpoint_sequence: 1,
                input_item_count: 0,
                materialized_key_count: 0,
                active_partitions: vec![0],
                throughput_partition_count: 1,
                source_kind: Some(STREAM_SOURCE_TOPIC.to_owned()),
                source_name: Some("payments".to_owned()),
                source_cursors: vec![LocalStreamJobSourceCursorState {
                    source_partition_id: 0,
                    next_offset: 3,
                    initial_checkpoint_target_offset: 3,
                    last_applied_offset: Some(2),
                    last_high_watermark: Some(3),
                    last_event_time_watermark: None,
                    last_closed_window_end: None,
                    pending_window_ends: Vec::new(),
                    dropped_late_event_count: 0,
                    last_dropped_late_offset: None,
                    last_dropped_late_event_at: None,
                    last_dropped_late_window_end: None,
                    dropped_evicted_window_event_count: 0,
                    last_dropped_evicted_window_offset: None,
                    last_dropped_evicted_window_event_at: None,
                    last_dropped_evicted_window_end: None,
                    checkpoint_reached_at: Some(occurred_at),
                    updated_at: occurred_at,
                }],
                source_partition_leases: vec![LocalStreamJobSourceLeaseState {
                    source_partition_id: 0,
                    owner_partition_id: 0,
                    owner_epoch: 7,
                    lease_token: "lease-0".to_owned(),
                    updated_at: occurred_at,
                }],
                dispatch_dataflow_batches: Vec::new(),
                applied_dispatch_batch_ids: Vec::new(),
                dispatch_completed_at: None,
                dispatch_cancelled_at: None,
                stream_owner_epoch: 7,
                planned_at: occurred_at,
                latest_checkpoint_at: Some(occurred_at),
                evicted_window_count: 0,
                last_evicted_window_end: None,
                last_evicted_at: None,
                view_runtime_stats: Vec::new(),
                pre_key_runtime_stats: vec![LocalStreamJobPreKeyRuntimeStatsState {
                    operator_id: "normalize-risk".to_owned(),
                    kind: "map".to_owned(),
                    processed_count: 3,
                    dropped_count: 0,
                    failure_count: 1,
                    route_default_count: 0,
                    route_branch_counts: Vec::new(),
                    last_failure: Some("stream map input field riskPoints must exist".to_owned()),
                }],
                hot_key_runtime_stats: Vec::new(),
                owner_partition_runtime_stats: Vec::new(),
                checkpoint_partitions: Vec::new(),
                terminal_status: None,
                terminal_output: None,
                terminal_error: None,
                terminal_at: None,
                updated_at: occurred_at,
            })
            .expect("runtime state should persist");

        let query = stream_job_runtime_stats_query(&handle, Some(0));
        let output = build_stream_job_query_output_on_local_state(&local_state, &handle, &query)
            .expect("runtime stats output should build")
            .expect("runtime stats output should exist");
        assert_eq!(output["sourceKind"], STREAM_SOURCE_TOPIC);
        assert_eq!(output["preKeyStats"]["totals"]["failureCount"], 1);
        assert_eq!(
            output["preKeyStats"]["operators"][0]["failureReasons"][0]["reason"],
            "operator_error"
        );
        assert_eq!(
            output["preKeyStats"]["operators"][0]["failureReasons"][0]["lastError"],
            "stream map input field riskPoints must exist"
        );

        std::fs::remove_dir_all(db_path).ok();
        std::fs::remove_dir_all(checkpoint_dir).ok();
    }

    #[test]
    fn topic_owner_input_batches_include_dataflow_envelopes() -> Result<()> {
        let handle = topic_keyed_rollup_handle("payments");
        let plan = bounded_stream_plan_for_job(&default_keyed_rollup_compiled_job(&handle))?
            .expect("bounded plan should exist");
        let source_batch = TopicSourceBatch {
            handle_id: handle.handle_id.clone(),
            job_id: handle.job_id.clone(),
            source_partition_id: 2,
            source_owner_partition_id: 1,
            source_lease_token: "lease-2".to_owned(),
            checkpoint_sequence: 9,
            checkpoint_target_offset: 11,
            idle_window_timer: false,
            offset_start: 7,
            offset_end: 9,
            records: vec![
                TopicSourceBatchRecord {
                    logical_key: "acct_1".to_owned(),
                    display_key: "acct_1".to_owned(),
                    window_start: None,
                    window_end: None,
                    event_time: None,
                    window_end_at: None,
                    value: 4.0,
                    offset: 7,
                    high_watermark: 9,
                },
                TopicSourceBatchRecord {
                    logical_key: "acct_2".to_owned(),
                    display_key: "acct_2".to_owned(),
                    window_start: None,
                    window_end: None,
                    event_time: None,
                    window_end_at: None,
                    value: 5.0,
                    offset: 8,
                    high_watermark: 9,
                },
            ],
        };

        let batches = owner_input_batches_from_source_batch(
            &handle,
            &source_batch,
            &plan,
            8,
            12,
            Utc::now(),
        )?;
        assert!(!batches.is_empty());
        for batch in batches {
            assert_eq!(batch.envelope.plan_id, format!("stream-job:{}", handle.handle_id));
            assert_eq!(batch.envelope.source_partition_id, 2);
            assert_eq!(batch.envelope.source_epoch, 9);
            assert_eq!(
                batch.envelope.offset_range.as_ref().map(|range| (range.start, range.end)),
                Some((7, 9))
            );
            assert_eq!(batch.envelope.dest_partition_id, batch.program.stream_partition_id);
            assert!(batch.envelope.edge_id.starts_with("exchange:key_by:"));
        }
        Ok(())
    }

    #[test]
    fn stream_job_runtime_stats_query_surfaces_restored_tail_lag() -> Result<()> {
        let db_path = temp_path("stream-jobs-runtime-stats-restore-tail-db");
        let checkpoint_dir = temp_path("stream-jobs-runtime-stats-restore-tail-checkpoints");
        let local_state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let occurred_at = DateTime::parse_from_rfc3339("2026-03-15T10:02:00Z")
            .expect("timestamp should parse")
            .with_timezone(&Utc);
        let handle = windowed_scan_handle();
        let execution_planned = execution_planned_changelog_record(
            &handle,
            "riskScores",
            "initial-risk-ready",
            1,
            7,
            0,
            1,
            vec![0],
            1,
            occurred_at - chrono::Duration::seconds(20),
        );
        let execution_entry = streams_entry_from_record(&execution_planned);
        assert!(local_state.apply_streams_changelog_entry(0, 2, &execution_entry)?);
        local_state.upsert_stream_job_runtime_state(&LocalStreamJobRuntimeState {
            handle_id: handle.handle_id.clone(),
            job_id: handle.job_id.clone(),
            job_name: handle.job_name.clone(),
            view_name: "riskScores".to_owned(),
            checkpoint_name: "initial-risk-ready".to_owned(),
            checkpoint_sequence: 1,
            input_item_count: 0,
            materialized_key_count: 1,
            active_partitions: vec![0],
            throughput_partition_count: 1,
            source_kind: Some(STREAM_SOURCE_TOPIC.to_owned()),
            source_name: Some("payments".to_owned()),
            source_cursors: vec![LocalStreamJobSourceCursorState {
                source_partition_id: 0,
                next_offset: 3,
                initial_checkpoint_target_offset: 3,
                last_applied_offset: Some(2),
                last_high_watermark: Some(5),
                last_event_time_watermark: None,
                last_closed_window_end: None,
                pending_window_ends: Vec::new(),
                dropped_late_event_count: 0,
                last_dropped_late_offset: None,
                last_dropped_late_event_at: None,
                last_dropped_late_window_end: None,
                dropped_evicted_window_event_count: 0,
                last_dropped_evicted_window_offset: None,
                last_dropped_evicted_window_event_at: None,
                last_dropped_evicted_window_end: None,
                checkpoint_reached_at: Some(occurred_at),
                updated_at: occurred_at,
            }],
            source_partition_leases: vec![LocalStreamJobSourceLeaseState {
                source_partition_id: 0,
                owner_partition_id: 0,
                owner_epoch: 7,
                lease_token: "lease-0".to_owned(),
                updated_at: occurred_at,
            }],
            dispatch_dataflow_batches: Vec::new(),
            applied_dispatch_batch_ids: Vec::new(),
            dispatch_completed_at: None,
            dispatch_cancelled_at: None,
            stream_owner_epoch: 7,
            planned_at: occurred_at,
            latest_checkpoint_at: Some(occurred_at),
            evicted_window_count: 0,
            last_evicted_window_end: None,
            last_evicted_at: None,
            view_runtime_stats: Vec::new(),
            pre_key_runtime_stats: Vec::new(),
            hot_key_runtime_stats: Vec::new(),
            owner_partition_runtime_stats: vec![LocalStreamJobOwnerPartitionRuntimeStatsState {
                stream_partition_id: 0,
                observed_batch_count: 1,
                observed_item_count: 1,
                last_batch_item_count: 1,
                max_batch_item_count: 1,
                state_key_count: 1,
                source_partition_ids: vec![0],
                last_updated_at: Some(occurred_at),
            }],
            checkpoint_partitions: vec![LocalStreamJobCheckpointState {
                handle_id: handle.handle_id.clone(),
                job_id: handle.job_id.clone(),
                checkpoint_name: "initial-risk-ready".to_owned(),
                checkpoint_sequence: 1,
                stream_partition_id: 0,
                stream_owner_epoch: 7,
                reached_at: occurred_at,
                updated_at: occurred_at,
            }],
            terminal_status: None,
            terminal_output: None,
            terminal_error: None,
            terminal_at: None,
            updated_at: occurred_at,
        })?;
        let checkpoint_value = local_state.snapshot_checkpoint_value()?;

        let restored_db_path = temp_path("stream-jobs-runtime-stats-restore-tail-restored-db");
        let restored = LocalThroughputState::open(&restored_db_path, &checkpoint_dir, 3)?;
        assert!(restored.restore_from_checkpoint_value_if_empty(checkpoint_value)?);
        restored.record_observed_high_watermark_for_plane(LocalChangelogPlane::Streams, 0, 5);

        let query = stream_job_runtime_stats_query(&handle, Some(0));
        let output = build_stream_job_query_output_on_local_state(&restored, &handle, &query)?
            .expect("runtime stats output should exist");
        assert_eq!(output["ownerPartitionStats"]["summary"]["restoredFromCheckpoint"], true);
        assert_eq!(output["ownerPartitionStats"]["summary"]["totalRestoreTailLagEntries"], 2);
        assert_eq!(output["ownerPartitionStats"]["summary"]["maxRestoreTailLagEntries"], 2);
        assert_eq!(output["ownerPartitionStats"]["partitions"][0]["restoreTailLagEntries"], 2);

        std::fs::remove_dir_all(db_path).ok();
        std::fs::remove_dir_all(checkpoint_dir).ok();
        std::fs::remove_dir_all(restored_db_path).ok();
        Ok(())
    }

    #[test]
    fn stream_job_view_runtime_stats_query_returns_view_policy_and_counts() {
        let db_path = temp_path("stream-jobs-view-runtime-stats-query-db");
        let checkpoint_dir = temp_path("stream-jobs-view-runtime-stats-query-checkpoints");
        let local_state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)
            .expect("local state should open");
        let occurred_at = DateTime::parse_from_rfc3339("2026-03-15T10:02:30Z")
            .expect("timestamp should parse")
            .with_timezone(&Utc);
        let shard_checkpoint_at = occurred_at - chrono::Duration::seconds(20);
        let stream_checkpoint_at = occurred_at - chrono::Duration::seconds(4);
        let handle = windowed_scan_handle();
        local_state.record_checkpoint_write(shard_checkpoint_at);
        local_state
            .upsert_stream_job_runtime_state(&LocalStreamJobRuntimeState {
                handle_id: handle.handle_id.clone(),
                job_id: handle.job_id.clone(),
                job_name: handle.job_name.clone(),
                view_name: "riskScores".to_owned(),
                checkpoint_name: "initial-risk-ready".to_owned(),
                checkpoint_sequence: 4,
                input_item_count: 0,
                materialized_key_count: 2,
                active_partitions: vec![0],
                throughput_partition_count: 1,
                source_kind: Some(STREAM_SOURCE_TOPIC.to_owned()),
                source_name: Some("payments".to_owned()),
                source_cursors: vec![LocalStreamJobSourceCursorState {
                    source_partition_id: 0,
                    next_offset: 3,
                    initial_checkpoint_target_offset: 3,
                    last_applied_offset: Some(2),
                    last_high_watermark: Some(3),
                    last_event_time_watermark: Some(
                        DateTime::parse_from_rfc3339("2026-03-15T10:03:20Z")
                            .expect("event watermark should parse")
                            .with_timezone(&Utc),
                    ),
                    last_closed_window_end: Some(
                        DateTime::parse_from_rfc3339("2026-03-15T10:03:00Z")
                            .expect("closed window should parse")
                            .with_timezone(&Utc),
                    ),
                    pending_window_ends: Vec::new(),
                    dropped_late_event_count: 0,
                    last_dropped_late_offset: None,
                    last_dropped_late_event_at: None,
                    last_dropped_late_window_end: None,
                    dropped_evicted_window_event_count: 0,
                    last_dropped_evicted_window_offset: None,
                    last_dropped_evicted_window_event_at: None,
                    last_dropped_evicted_window_end: None,
                    checkpoint_reached_at: Some(occurred_at),
                    updated_at: occurred_at,
                }],
                source_partition_leases: Vec::new(),
                dispatch_dataflow_batches: Vec::new(),
                applied_dispatch_batch_ids: Vec::new(),
                dispatch_completed_at: None,
                dispatch_cancelled_at: None,
                stream_owner_epoch: 7,
                planned_at: occurred_at,
                latest_checkpoint_at: Some(stream_checkpoint_at),
                evicted_window_count: 1,
                last_evicted_window_end: Some(
                    DateTime::parse_from_rfc3339("2026-03-15T10:01:00Z")
                        .expect("window end should parse")
                        .with_timezone(&Utc),
                ),
                last_evicted_at: Some(occurred_at),
                view_runtime_stats: vec![crate::local_state::LocalStreamJobViewRuntimeStatsState {
                    view_name: "riskScores".to_owned(),
                    evicted_window_count: 1,
                    last_evicted_window_end: Some(
                        DateTime::parse_from_rfc3339("2026-03-15T10:01:00Z")
                            .expect("window end should parse")
                            .with_timezone(&Utc),
                    ),
                    last_evicted_at: Some(occurred_at),
                }],
                pre_key_runtime_stats: vec![
                    LocalStreamJobPreKeyRuntimeStatsState {
                        operator_id: "bucket-risk".to_owned(),
                        kind: "route".to_owned(),
                        processed_count: 3,
                        dropped_count: 0,
                        failure_count: 0,
                        route_default_count: 1,
                        route_branch_counts: vec![
                            LocalStreamJobRouteBranchRuntimeStatsState {
                                value: "high".to_owned(),
                                matched_count: 1,
                            },
                            LocalStreamJobRouteBranchRuntimeStatsState {
                                value: "medium".to_owned(),
                                matched_count: 1,
                            },
                        ],
                        last_failure: None,
                    },
                    LocalStreamJobPreKeyRuntimeStatsState {
                        operator_id: "filter-positive".to_owned(),
                        kind: "filter".to_owned(),
                        processed_count: 3,
                        dropped_count: 1,
                        failure_count: 0,
                        route_default_count: 0,
                        route_branch_counts: Vec::new(),
                        last_failure: None,
                    },
                    LocalStreamJobPreKeyRuntimeStatsState {
                        operator_id: "normalize-risk".to_owned(),
                        kind: "map".to_owned(),
                        processed_count: 2,
                        dropped_count: 0,
                        failure_count: 0,
                        route_default_count: 0,
                        route_branch_counts: Vec::new(),
                        last_failure: None,
                    },
                ],
                hot_key_runtime_stats: vec![
                    LocalStreamJobHotKeyRuntimeStatsState {
                        display_key: "acct_1".to_owned(),
                        logical_key: windowed_logical_key("acct_1", "2026-03-15T10:01:00+00:00"),
                        observed_count: 4,
                        source_partition_ids: vec![0],
                        last_seen_at: Some(occurred_at),
                    },
                    LocalStreamJobHotKeyRuntimeStatsState {
                        display_key: "acct_2".to_owned(),
                        logical_key: windowed_logical_key("acct_2", "2026-03-15T10:00:00+00:00"),
                        observed_count: 1,
                        source_partition_ids: vec![0],
                        last_seen_at: Some(occurred_at),
                    },
                ],
                owner_partition_runtime_stats: vec![
                    LocalStreamJobOwnerPartitionRuntimeStatsState {
                        stream_partition_id: 0,
                        observed_batch_count: 2,
                        observed_item_count: 3,
                        last_batch_item_count: 2,
                        max_batch_item_count: 2,
                        state_key_count: 2,
                        source_partition_ids: vec![0],
                        last_updated_at: Some(occurred_at),
                    },
                ],
                checkpoint_partitions: vec![LocalStreamJobCheckpointState {
                    handle_id: handle.handle_id.clone(),
                    job_id: handle.job_id.clone(),
                    checkpoint_name: "initial-risk-ready".to_owned(),
                    checkpoint_sequence: 4,
                    stream_partition_id: 0,
                    stream_owner_epoch: 7,
                    reached_at: stream_checkpoint_at,
                    updated_at: stream_checkpoint_at,
                }],
                terminal_status: None,
                terminal_output: None,
                terminal_error: None,
                terminal_at: None,
                updated_at: occurred_at,
            })
            .expect("runtime state should persist");
        local_state
            .upsert_stream_job_view_value(
                &handle.handle_id,
                &handle.job_id,
                "riskScores",
                &windowed_logical_key("acct_1", "2026-03-15T10:00:00+00:00"),
                json!({
                    "accountId": "acct_1",
                    "avgRisk": 0.9,
                    "windowStart": "2026-03-15T10:00:00+00:00",
                    "windowEnd": "2026-03-15T10:01:00+00:00",
                    "asOfCheckpoint": 4
                }),
                4,
                occurred_at,
            )
            .expect("view state should persist");
        local_state
            .upsert_stream_job_view_value(
                &handle.handle_id,
                &handle.job_id,
                "riskScores",
                &windowed_logical_key("acct_1", "2026-03-15T10:02:00+00:00"),
                json!({
                    "accountId": "acct_1",
                    "avgRisk": 0.8,
                    "windowStart": "2026-03-15T10:02:00+00:00",
                    "windowEnd": "2026-03-15T10:03:00+00:00",
                    "asOfCheckpoint": 4
                }),
                4,
                occurred_at,
            )
            .expect("second view state should persist");

        let mut query = stream_job_view_runtime_stats_query(&handle, "riskScores");
        query.requested_at = occurred_at;
        let output = build_stream_job_query_output_on_local_state(&local_state, &handle, &query)
            .expect("view runtime stats output should build")
            .expect("view runtime stats output should exist");
        assert_eq!(output["queryName"], STREAM_JOB_VIEW_RUNTIME_STATS_QUERY_NAME);
        assert_eq!(output["viewName"], "riskScores");
        assert_eq!(output["storedKeyCount"], 2);
        assert_eq!(output["activeKeyCount"], 2);
        assert_eq!(output["latestCheckpointSequence"], 4);
        assert_eq!(output["streamOwnerEpoch"], 7);
        assert_eq!(output["jobEvictedWindowCount"], 1);
        assert_eq!(output["freshness"]["checkpointSequenceLag"], 0);
        assert_eq!(output["freshness"]["latestEventTimeWatermark"], "2026-03-15T10:03:20+00:00");
        assert_eq!(output["freshness"]["latestClosedWindowEnd"], "2026-03-15T10:03:00+00:00");
        assert_eq!(output["freshness"]["latestMaterializedWindowEnd"], "2026-03-15T10:03:00+00:00");
        assert_eq!(output["freshness"]["eventTimeLagSeconds"], 20);
        assert_eq!(output["freshness"]["closedWindowLagSeconds"], 0);
        assert_eq!(output["historicalEvictedWindowCount"], 1);
        assert_eq!(output["historicalLastEvictedWindowEnd"], "2026-03-15T10:01:00+00:00");
        assert_eq!(output["historicalLastEvictedAt"], occurred_at.to_rfc3339());
        assert_eq!(output["policy"]["operatorId"], "materialize-risk");
        assert_eq!(output["policy"]["stateIds"][0], "risk-state");
        assert_eq!(output["policy"]["preKeyPolicy"][0]["kind"], "route");
        assert_eq!(output["policy"]["preKeyPolicy"][1]["kind"], "filter");
        assert_eq!(output["policy"]["preKeyPolicy"][2]["kind"], "map");
        assert_eq!(output["preKeyStats"]["operators"][0]["kind"], "route");
        assert_eq!(output["preKeyStats"]["operators"][1]["droppedCount"], 1);
        assert_eq!(output["preKeyStats"]["operators"][2]["processedCount"], 2);
        assert_eq!(output["hotKeyStats"]["totals"]["observedCount"], 5);
        assert_eq!(output["hotKeyStats"]["topKeys"][0]["displayKey"], "acct_1");
        assert_eq!(output["hotKeyStats"]["topKeys"][0]["observedCount"], 4);
        assert_eq!(output["sourcePartitionStats"]["summary"]["partitionCount"], 1);
        assert_eq!(output["sourcePartitionStats"]["summary"]["totalOffsetLag"], 0);
        assert_eq!(output["sourcePartitionStats"]["partitions"][0]["sourcePartitionId"], 0);
        assert_eq!(output["ownerPartitionStats"]["summary"]["partitionCount"], 1);
        assert_eq!(output["ownerPartitionStats"]["summary"]["totalStateKeyCount"], 2);
        assert_eq!(output["ownerPartitionStats"]["summary"]["checkpointedPartitionCount"], 1);
        assert_eq!(
            output["ownerPartitionStats"]["summary"]["lastShardCheckpointAt"],
            shard_checkpoint_at.to_rfc3339()
        );
        assert_eq!(output["ownerPartitionStats"]["partitions"][0]["stateKeyCount"], 2);
        assert_eq!(output["ownerPartitionStats"]["partitions"][0]["observedItemCount"], 3);
        assert_eq!(
            output["ownerPartitionStats"]["partitions"][0]["lastStreamCheckpointAt"],
            stream_checkpoint_at.to_rfc3339()
        );
        assert!(
            output["ownerPartitionStats"]["partitions"][0]["checkpointBytes"]
                .as_u64()
                .expect("checkpoint bytes should exist")
                > 0
        );
        assert_eq!(output["policy"]["retentionSeconds"], 120);
        assert_eq!(output["policy"]["lateEventPolicy"], "drop_after_closed_window");
        assert_eq!(output["policy"]["windowPolicy"]["allowedLateness"], "10s");
        assert_eq!(output["consistency"], "strong");
        assert_eq!(output["consistencySource"], "stream_owner_local_state");

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

        let plan =
            plan_stream_job_activation(&local_state, None, None, &handle, 8, Some(9), Utc::now())
                .await?
                .expect("keyed-rollup activation plan should exist");
        let runtime_entry = streams_entry_from_record(
            plan.execution_planned.as_ref().expect("execution plan should exist"),
        );
        assert!(local_state.apply_streams_changelog_entry(0, 0, &runtime_entry)?);

        let mut view_offset = 1i64;
        let mut checkpoint_entries = Vec::new();
        for batch in &plan.dataflow_batches {
            let applied = apply_stream_job_dataflow_batch_on_local_state(
                &local_state,
                &handle,
                batch,
                false,
            )?;
            for record in applied.activation_result.changelog_records {
                match &record.entry {
                    crate::ChangelogRecordEntry::Streams(StreamsChangelogEntry {
                        payload: StreamsChangelogPayload::StreamJobViewBatchUpdated { updates, .. },
                        ..
                    }) => {
                        assert!(!updates.is_empty());
                        for update in updates {
                            assert_eq!(
                                record.key,
                                throughput_partition_key(&update.logical_key, 0)
                            );
                        }
                        let entry = streams_entry_from_record(&record);
                        assert!(local_state.apply_streams_changelog_entry(
                            0,
                            view_offset,
                            &entry
                        )?);
                        view_offset += 1;
                    }
                    crate::ChangelogRecordEntry::Streams(StreamsChangelogEntry {
                        payload: StreamsChangelogPayload::StreamJobCheckpointReached { .. },
                        ..
                    }) => {
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
        sync_stream_job_bridge_callback_records_on_local_state(&local_state, &handle)?;
        assert!(local_state.load_stream_job_bridge_callbacks(&handle.handle_id)?.is_empty());
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
        sync_stream_job_bridge_callback_records_on_local_state(&local_state, &handle)?;

        let callback_states = local_state.load_stream_job_bridge_callbacks(&handle.handle_id)?;
        assert_eq!(callback_states.len(), 2);
        let checkpoint_callback = callback_states
            .iter()
            .find(|state| state.record.callback_kind == STREAM_JOB_CALLBACK_KIND_CHECKPOINT)
            .expect("checkpoint callback should be present after barrier completion");
        assert_eq!(
            checkpoint_callback
                .record
                .checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.checkpoint_name.as_str()),
            Some(KEYED_ROLLUP_CHECKPOINT_NAME)
        );
        assert_eq!(
            checkpoint_callback
                .record
                .checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.checkpoint_sequence),
            Some(KEYED_ROLLUP_CHECKPOINT_SEQUENCE)
        );
        assert!(checkpoint_callback.record.accepted_progress_position.is_some());
        let terminal_callback = callback_states
            .iter()
            .find(|state| state.record.callback_kind == STREAM_JOB_CALLBACK_KIND_TERMINAL)
            .expect("terminal callback should be present after terminal replay");
        assert_eq!(
            terminal_callback.record.terminal_status.as_deref(),
            Some(StreamJobBridgeHandleStatus::Completed.as_str())
        );
        assert!(
            terminal_callback.record.accepted_progress_position.is_some_and(|position| position
                >= checkpoint_callback.record.accepted_progress_position.unwrap_or_default())
        );

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

        let checkpoint_value = local_state.snapshot_checkpoint_value()?;
        let restored_db_path = temp_path("stream-jobs-keyed-rollup-barrier-restored-db");
        let restored_checkpoint_dir =
            temp_path("stream-jobs-keyed-rollup-barrier-restored-checkpoints");
        let restored = LocalThroughputState::open(&restored_db_path, &restored_checkpoint_dir, 3)?;
        assert!(restored.restore_from_checkpoint_value_if_empty(checkpoint_value)?);
        let restored_callbacks = restored.load_stream_job_bridge_callbacks(&handle.handle_id)?;
        assert_eq!(restored_callbacks.len(), 2);
        assert!(restored_callbacks.iter().any(|state| {
            state.record.callback_kind == STREAM_JOB_CALLBACK_KIND_CHECKPOINT
                && state.record.checkpoint.as_ref().is_some_and(|checkpoint| {
                    checkpoint.checkpoint_name == KEYED_ROLLUP_CHECKPOINT_NAME
                        && checkpoint.checkpoint_sequence == KEYED_ROLLUP_CHECKPOINT_SEQUENCE
                })
        }));
        assert!(restored_callbacks.iter().any(|state| {
            state.record.callback_kind == STREAM_JOB_CALLBACK_KIND_TERMINAL
                && state.record.terminal_status.as_deref()
                    == Some(StreamJobBridgeHandleStatus::Completed.as_str())
        }));
        let restored_complete = materialization_outcome_from_local_state(&restored, &handle)?
            .expect("restored materialization should still exist after callback restore");
        assert_eq!(
            restored_complete
                .checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.checkpoint_sequence),
            Some(KEYED_ROLLUP_CHECKPOINT_SEQUENCE)
        );
        assert_eq!(
            restored_complete.terminal.as_ref().map(|terminal| terminal.status),
            Some(StreamJobBridgeHandleStatus::Completed)
        );

        std::fs::remove_dir_all(db_path).ok();
        std::fs::remove_dir_all(checkpoint_dir).ok();
        std::fs::remove_dir_all(restored_db_path).ok();
        std::fs::remove_dir_all(restored_checkpoint_dir).ok();
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

    #[tokio::test]
    #[ignore = "perf harness"]
    async fn perf_keyed_rollup_owner_activation_reports_throughput_and_skew() -> Result<()> {
        let partition_count = 32;
        let total_items = 20_000usize;
        let distinct_keys = 4_000usize;
        let mut scenario_timings = Vec::new();

        for (label, hot_key_ratio) in [("uniform", 0.0), ("hot_95", 0.95)] {
            let (input_ref, _) = keyed_rollup_input(total_items, distinct_keys, hot_key_ratio);
            let handle = keyed_rollup_handle(&input_ref);
            let plan_db_path = temp_path(&format!("stream-jobs-bench-{label}-plan-db"));
            let plan_checkpoint_dir =
                temp_path(&format!("stream-jobs-bench-{label}-plan-checkpoints"));
            let plan_state = LocalThroughputState::open(&plan_db_path, &plan_checkpoint_dir, 3)?;

            let plan_started = Instant::now();
            let plan = plan_stream_job_activation(
                &plan_state,
                None,
                None,
                &handle,
                partition_count,
                Some(9),
                Utc::now(),
            )
            .await?
            .expect("activation plan should exist");
            let plan_elapsed = plan_started.elapsed();

            let compute_started = Instant::now();
            let mut compute_accumulators = HashMap::with_capacity(distinct_keys);
            let mut compute_updates = 0usize;
            for batch in &plan.dataflow_batches {
                compute_updates += apply_stream_job_batch_program_in_memory(
                    &mut compute_accumulators,
                    &batch.program,
                )?
                .len();
            }
            let compute_elapsed = compute_started.elapsed();

            let record_started = Instant::now();
            let mut record_accumulators = HashMap::with_capacity(distinct_keys);
            let mut record_projection_count = 0usize;
            let mut record_changelog_count = 0usize;
            for batch in &plan.dataflow_batches {
                let updated_outputs = apply_stream_job_batch_program_in_memory(
                    &mut record_accumulators,
                    &batch.program,
                )?;
                let materialized = materialize_stream_job_batch_updates(
                    &handle,
                    &batch.program,
                    updated_outputs,
                    false,
                );
                record_projection_count += materialized.projection_records.len();
                record_changelog_count += materialized.changelog_records.len()
                    + usize::from(
                        batch.program.is_final_partition_batch
                            && !batch.program.checkpoint_name.is_empty(),
                    );
                black_box(materialized.projection_records.len());
                black_box(materialized.changelog_records.len());
            }
            let record_elapsed = record_started.elapsed();

            let apply_db_path = temp_path(&format!("stream-jobs-bench-{label}-apply-db"));
            let apply_checkpoint_dir =
                temp_path(&format!("stream-jobs-bench-{label}-apply-checkpoints"));
            let apply_state = LocalThroughputState::open(&apply_db_path, &apply_checkpoint_dir, 3)?;
            let apply_started = Instant::now();
            let mut apply_metrics = StreamJobApplyMetrics::default();
            let mut apply_projection_count = 0usize;
            let mut apply_changelog_count = 0usize;
            for batch in &plan.dataflow_batches {
                let applied = apply_stream_job_dataflow_batch_on_local_state_with_metrics(
                    &apply_state,
                    &handle,
                    batch,
                    false,
                    None,
                    Some(&mut apply_metrics),
                )?;
                apply_projection_count += applied.projection_records.len();
                apply_changelog_count += applied.changelog_records.len();
            }
            let apply_elapsed = apply_started.elapsed();

            let persist_db_path = temp_path(&format!("stream-jobs-bench-{label}-persist-db"));
            let persist_checkpoint_dir =
                temp_path(&format!("stream-jobs-bench-{label}-persist-checkpoints"));
            let persist_state =
                LocalThroughputState::open(&persist_db_path, &persist_checkpoint_dir, 3)?;
            let setup_metrics =
                prepare_local_state_for_stream_job_plan(&persist_state, &handle, &plan)?;
            let persist_started = Instant::now();
            let mut persist_metrics = StreamJobApplyMetrics::default();
            let mut persist_projection_count = 0usize;
            let mut persist_changelog_count = 0usize;
            for batch in &plan.dataflow_batches {
                let applied = apply_stream_job_dataflow_batch_on_local_state_with_metrics(
                    &persist_state,
                    &handle,
                    batch,
                    true,
                    None,
                    Some(&mut persist_metrics),
                )?;
                persist_projection_count += applied.projection_records.len();
                persist_changelog_count += applied.changelog_records.len();
            }
            let persist_elapsed = persist_started.elapsed();

            let end_to_end_db_path = temp_path(&format!("stream-jobs-bench-{label}-e2e-db"));
            let end_to_end_checkpoint_dir =
                temp_path(&format!("stream-jobs-bench-{label}-e2e-checkpoints"));
            let end_to_end_state =
                LocalThroughputState::open(&end_to_end_db_path, &end_to_end_checkpoint_dir, 3)?;

            let started = Instant::now();
            let activation = activate_stream_job_on_local_state(
                None,
                None,
                &end_to_end_state,
                &handle,
                partition_count,
                Some(9),
                Utc::now(),
            )
            .await?
            .expect("activation should exist");
            let elapsed = started.elapsed();

            println!(
                "bench=owner_activation_plan_only scenario={label} items={total_items} distinct_keys={distinct_keys} elapsed_ms={:.2} items_per_sec={:.0} batches={} planned_partitions={}",
                plan_elapsed.as_secs_f64() * 1_000.0,
                total_items as f64 / plan_elapsed.as_secs_f64().max(f64::EPSILON),
                plan.dataflow_batches.len(),
                plan.dataflow_batches
                    .iter()
                    .map(|batch| batch.program.stream_partition_id)
                    .collect::<std::collections::BTreeSet<_>>()
                    .len(),
            );
            println!(
                "bench=owner_activation_compute scenario={label} items={total_items} distinct_keys={distinct_keys} elapsed_ms={:.2} items_per_sec={:.0} updated_keys={compute_updates}",
                compute_elapsed.as_secs_f64() * 1_000.0,
                total_items as f64 / compute_elapsed.as_secs_f64().max(f64::EPSILON),
            );
            println!(
                "bench=owner_activation_compute_plus_records scenario={label} items={total_items} distinct_keys={distinct_keys} elapsed_ms={:.2} items_per_sec={:.0} projection_records={} changelog_records={}",
                record_elapsed.as_secs_f64() * 1_000.0,
                total_items as f64 / record_elapsed.as_secs_f64().max(f64::EPSILON),
                record_projection_count,
                record_changelog_count,
            );
            println!(
                "bench=owner_activation_apply_only_with_existing_plan scenario={label} items={total_items} distinct_keys={distinct_keys} elapsed_ms={:.2} items_per_sec={:.0} projection_records={} changelog_records={} view_loads={} view_hits={} view_misses={} load_ms={:.2} accumulate_ms={:.2} materialize_ms={:.2}",
                apply_elapsed.as_secs_f64() * 1_000.0,
                total_items as f64 / apply_elapsed.as_secs_f64().max(f64::EPSILON),
                apply_projection_count,
                apply_changelog_count,
                apply_metrics.view_state_loads,
                apply_metrics.view_state_hits,
                apply_metrics.view_state_misses,
                apply_metrics.load_existing_state_elapsed.as_secs_f64() * 1_000.0,
                apply_metrics.accumulate_elapsed.as_secs_f64() * 1_000.0,
                apply_metrics.materialize_elapsed.as_secs_f64() * 1_000.0,
            );
            println!(
                "bench=owner_activation_apply_plus_persist scenario={label} items={total_items} distinct_keys={distinct_keys} setup_ms={:.2} elapsed_ms={:.2} items_per_sec={:.0} projection_records={} changelog_records={} runtime_loads={} execution_plan_mirrors={} runtime_writes={} dispatch_manifest_writes={} source_cursor_writes={} view_writes={} mirrored_stream_entries={} changelog_mirror_writes={} persist_ms={:.2}",
                setup_metrics.elapsed.as_secs_f64() * 1_000.0,
                persist_elapsed.as_secs_f64() * 1_000.0,
                total_items as f64 / persist_elapsed.as_secs_f64().max(f64::EPSILON),
                persist_projection_count,
                persist_changelog_count,
                persist_metrics.runtime_state_loads,
                setup_metrics.execution_plan_mirrors,
                persist_metrics.runtime_state_write_count + setup_metrics.runtime_state_writes,
                setup_metrics.dispatch_manifest_writes,
                setup_metrics.source_cursor_writes,
                persist_metrics.view_state_write_count,
                persist_metrics.mirrored_stream_entry_count,
                persist_metrics.changelog_mirror_write_count,
                persist_metrics.persist_elapsed.as_secs_f64() * 1_000.0,
            );
            println!(
                "bench=owner_activation_end_to_end scenario={label} items={total_items} distinct_keys={distinct_keys} elapsed_ms={:.2} items_per_sec={:.0} projection_records={} changelog_records={}",
                elapsed.as_secs_f64() * 1_000.0,
                total_items as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
                activation.projection_records.len(),
                activation.changelog_records.len(),
            );
            scenario_timings.push((label, elapsed));
            remove_paths(&[
                &plan_db_path,
                &plan_checkpoint_dir,
                &apply_db_path,
                &apply_checkpoint_dir,
                &persist_db_path,
                &persist_checkpoint_dir,
                &end_to_end_db_path,
                &end_to_end_checkpoint_dir,
            ]);
        }

        if let [("uniform", uniform_elapsed), ("hot_95", hot_elapsed)] = scenario_timings.as_slice()
        {
            println!(
                "bench=owner_activation compare=hot_vs_uniform slowdown={:.2}",
                hot_elapsed.as_secs_f64() / uniform_elapsed.as_secs_f64().max(f64::EPSILON),
            );
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore = "perf harness"]
    async fn perf_keyed_rollup_checkpoint_and_restore_report_costs() -> Result<()> {
        let total_items = 12_000usize;
        let distinct_keys = 2_000usize;
        let partition_count = 16;
        let occurred_at = Utc::now();
        let (input_ref, _) = keyed_rollup_input(total_items, distinct_keys, 0.0);
        let handle = keyed_rollup_handle(&input_ref);

        let owner_db_path = temp_path("stream-jobs-bench-checkpoint-owner-db");
        let owner_checkpoint_dir = temp_path("stream-jobs-bench-checkpoint-owner-checkpoints");
        let owner_state = LocalThroughputState::open(&owner_db_path, &owner_checkpoint_dir, 3)?;
        let activation = activate_stream_job_on_local_state(
            None,
            None,
            &owner_state,
            &handle,
            partition_count,
            Some(9),
            occurred_at,
        )
        .await?
        .expect("activation should exist");
        let stream_entries =
            activation.changelog_records.iter().map(streams_entry_from_record).collect::<Vec<_>>();

        let checkpoint_db_path = temp_path("stream-jobs-bench-checkpoint-source-db");
        let checkpoint_state =
            LocalThroughputState::open(&checkpoint_db_path, &owner_checkpoint_dir, 3)?;
        let checkpoint_split = (stream_entries.len() / 2).max(1);
        for (offset, entry) in stream_entries.iter().take(checkpoint_split).enumerate() {
            assert!(checkpoint_state.apply_streams_changelog_entry(0, offset as i64, entry)?);
        }

        let checkpoint_started = Instant::now();
        let checkpoint_value = checkpoint_state.snapshot_checkpoint_value()?;
        let checkpoint_elapsed = checkpoint_started.elapsed();
        let checkpoint_bytes = serde_json::to_vec(&checkpoint_value)?.len();

        let restored_db_path = temp_path("stream-jobs-bench-checkpoint-restored-db");
        let restored_state =
            LocalThroughputState::open(&restored_db_path, &owner_checkpoint_dir, 3)?;
        let restore_started = Instant::now();
        assert!(restored_state.restore_from_checkpoint_value_if_empty(checkpoint_value)?);
        for (offset, entry) in stream_entries.iter().enumerate().skip(checkpoint_split) {
            assert!(restored_state.apply_streams_changelog_entry(0, offset as i64, entry)?);
        }
        let restore_elapsed = restore_started.elapsed();

        println!(
            "bench=checkpoint_snapshot items={total_items} distinct_keys={distinct_keys} entries={} checkpoint_bytes={} elapsed_ms={:.2}",
            stream_entries.len(),
            checkpoint_bytes,
            checkpoint_elapsed.as_secs_f64() * 1_000.0,
        );
        println!(
            "bench=restore_plus_tail items={total_items} distinct_keys={distinct_keys} tail_entries={} elapsed_ms={:.2}",
            stream_entries.len().saturating_sub(checkpoint_split),
            restore_elapsed.as_secs_f64() * 1_000.0,
        );

        remove_paths(&[
            &owner_db_path,
            &owner_checkpoint_dir,
            &checkpoint_db_path,
            &restored_db_path,
        ]);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "perf harness"]
    async fn perf_keyed_rollup_owner_strong_reads_report_latency() -> Result<()> {
        let total_items = 8_000usize;
        let distinct_keys = 1_000usize;
        let iterations = 10_000usize;
        let (input_ref, query_key) = keyed_rollup_input(total_items, distinct_keys, 0.25);
        let handle = keyed_rollup_handle(&input_ref);
        let query = keyed_rollup_query(&handle, "accountTotals", &query_key);
        let db_path = temp_path("stream-jobs-bench-strong-read-db");
        let checkpoint_dir = temp_path("stream-jobs-bench-strong-read-checkpoints");
        let local_state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;

        activate_stream_job_on_local_state(
            None,
            None,
            &local_state,
            &handle,
            16,
            Some(9),
            Utc::now(),
        )
        .await?
        .expect("activation should exist");

        let started = Instant::now();
        for _ in 0..iterations {
            let output =
                build_stream_job_query_output_on_local_state(&local_state, &handle, &query)?
                    .expect("strong read should resolve");
            black_box(output);
        }
        let elapsed = started.elapsed();
        println!(
            "bench=strong_read iterations={iterations} total_ms={:.2} avg_us={:.2}",
            elapsed.as_secs_f64() * 1_000.0,
            (elapsed.as_secs_f64() * 1_000_000.0) / iterations as f64,
        );

        remove_paths(&[&db_path, &checkpoint_dir]);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "perf harness"]
    async fn perf_aggregate_v2_windowed_owner_activation_reports_throughput_and_skew() -> Result<()>
    {
        let partition_count = 16;
        let total_items = 12_000usize;
        let distinct_keys = 2_000usize;
        let mut scenario_timings = Vec::new();

        for (label, hot_key_ratio) in [("uniform", 0.0), ("hot_95", 0.95)] {
            let (input_ref, _, _) =
                aggregate_v2_windowed_perf_input(total_items, distinct_keys, hot_key_ratio);
            let handle = aggregate_v2_windowed_perf_handle(&input_ref);
            let db_path = temp_path(&format!("stream-jobs-bench-aggregate-v2-{label}-db"));
            let checkpoint_dir =
                temp_path(&format!("stream-jobs-bench-aggregate-v2-{label}-checkpoints"));
            let local_state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;

            let started = Instant::now();
            let activation = activate_stream_job_on_local_state(
                None,
                None,
                &local_state,
                &handle,
                partition_count,
                Some(9),
                Utc::now(),
            )
            .await?
            .expect("activation should exist");
            let elapsed = started.elapsed();
            println!(
                "bench=aggregate_v2_windowed_activation scenario={label} items={total_items} distinct_keys={distinct_keys} elapsed_ms={:.2} items_per_sec={:.0} changelog_records={} projection_records={}",
                elapsed.as_secs_f64() * 1_000.0,
                total_items as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
                activation.changelog_records.len(),
                activation.projection_records.len(),
            );
            scenario_timings.push((label, elapsed));
            remove_paths(&[&db_path, &checkpoint_dir]);
        }

        if let [("uniform", uniform_elapsed), ("hot_95", hot_elapsed)] = scenario_timings.as_slice()
        {
            println!(
                "bench=aggregate_v2_windowed_activation compare=hot_vs_uniform slowdown={:.2}",
                hot_elapsed.as_secs_f64() / uniform_elapsed.as_secs_f64().max(f64::EPSILON),
            );
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore = "perf harness"]
    async fn perf_aggregate_v2_windowed_checkpoint_and_restore_report_costs() -> Result<()> {
        let total_items = 10_000usize;
        let distinct_keys = 1_500usize;
        let partition_count = 16;
        let occurred_at = Utc::now();
        let (input_ref, _, _) = aggregate_v2_windowed_perf_input(total_items, distinct_keys, 0.25);
        let handle = aggregate_v2_windowed_perf_handle(&input_ref);

        let owner_db_path = temp_path("stream-jobs-bench-aggregate-v2-checkpoint-owner-db");
        let owner_checkpoint_dir =
            temp_path("stream-jobs-bench-aggregate-v2-checkpoint-owner-checkpoints");
        let owner_state = LocalThroughputState::open(&owner_db_path, &owner_checkpoint_dir, 3)?;
        let activation = activate_stream_job_on_local_state(
            None,
            None,
            &owner_state,
            &handle,
            partition_count,
            Some(9),
            occurred_at,
        )
        .await?
        .expect("activation should exist");
        let stream_entries =
            activation.changelog_records.iter().map(streams_entry_from_record).collect::<Vec<_>>();

        let checkpoint_db_path = temp_path("stream-jobs-bench-aggregate-v2-checkpoint-source-db");
        let checkpoint_state =
            LocalThroughputState::open(&checkpoint_db_path, &owner_checkpoint_dir, 3)?;
        let checkpoint_split = (stream_entries.len() / 2).max(1);
        for (offset, entry) in stream_entries.iter().take(checkpoint_split).enumerate() {
            assert!(checkpoint_state.apply_streams_changelog_entry(0, offset as i64, entry)?);
        }

        let checkpoint_started = Instant::now();
        let checkpoint_value = checkpoint_state.snapshot_checkpoint_value()?;
        let checkpoint_elapsed = checkpoint_started.elapsed();
        let checkpoint_bytes = serde_json::to_vec(&checkpoint_value)?.len();

        let restored_db_path = temp_path("stream-jobs-bench-aggregate-v2-checkpoint-restored-db");
        let restored_state =
            LocalThroughputState::open(&restored_db_path, &owner_checkpoint_dir, 3)?;
        let restore_started = Instant::now();
        assert!(restored_state.restore_from_checkpoint_value_if_empty(checkpoint_value)?);
        for (offset, entry) in stream_entries.iter().enumerate().skip(checkpoint_split) {
            assert!(restored_state.apply_streams_changelog_entry(0, offset as i64, entry)?);
        }
        let restore_elapsed = restore_started.elapsed();

        println!(
            "bench=aggregate_v2_windowed_checkpoint_snapshot items={total_items} distinct_keys={distinct_keys} entries={} checkpoint_bytes={} elapsed_ms={:.2}",
            stream_entries.len(),
            checkpoint_bytes,
            checkpoint_elapsed.as_secs_f64() * 1_000.0,
        );
        println!(
            "bench=aggregate_v2_windowed_restore_plus_tail items={total_items} distinct_keys={distinct_keys} tail_entries={} elapsed_ms={:.2}",
            stream_entries.len().saturating_sub(checkpoint_split),
            restore_elapsed.as_secs_f64() * 1_000.0,
        );

        remove_paths(&[
            &owner_db_path,
            &owner_checkpoint_dir,
            &checkpoint_db_path,
            &restored_db_path,
        ]);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "perf harness"]
    async fn perf_aggregate_v2_windowed_owner_strong_reads_report_latency() -> Result<()> {
        let total_items = 8_000usize;
        let distinct_keys = 1_000usize;
        let iterations = 10_000usize;
        let (input_ref, query_key, window_start) =
            aggregate_v2_windowed_perf_input(total_items, distinct_keys, 0.25);
        let handle = aggregate_v2_windowed_perf_handle(&input_ref);
        let query = aggregate_v2_windowed_perf_query(&handle, &query_key, &window_start);
        let db_path = temp_path("stream-jobs-bench-aggregate-v2-strong-read-db");
        let checkpoint_dir = temp_path("stream-jobs-bench-aggregate-v2-strong-read-checkpoints");
        let local_state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;

        activate_stream_job_on_local_state(
            None,
            None,
            &local_state,
            &handle,
            16,
            Some(9),
            Utc::now(),
        )
        .await?
        .expect("activation should exist");

        let started = Instant::now();
        for _ in 0..iterations {
            let output =
                build_stream_job_query_output_on_local_state(&local_state, &handle, &query)?
                    .expect("strong read should resolve");
            black_box(output);
        }
        let elapsed = started.elapsed();
        println!(
            "bench=aggregate_v2_windowed_strong_read iterations={iterations} total_ms={:.2} avg_us={:.2}",
            elapsed.as_secs_f64() * 1_000.0,
            (elapsed.as_secs_f64() * 1_000_000.0) / iterations as f64,
        );

        remove_paths(&[&db_path, &checkpoint_dir]);
        Ok(())
    }
}
