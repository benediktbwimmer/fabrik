mod aggregation_state;
mod checkpoint_state;
mod db_state;
mod derivation;
mod keyspace;
mod mutation_state;
mod query_state;
mod scheduling_state;
mod storage_core;
mod stream_job_state;

#[cfg(test)]
use self::aggregation_state::batch_group_index_key;
use self::derivation::{
    derive_batch_state_from_chunk_states, infer_ungrouped_batch_terminal_from_chunk_apply,
};
use self::keyspace::{
    STREAM_JOB_CHECKPOINT_BINARY_PREFIX, STREAM_JOB_RUNTIME_BINARY_PREFIX,
    STREAM_JOB_SIGNAL_BINARY_PREFIX, STREAM_JOB_VIEW_BINARY_PREFIX, batch_chunk_index_key,
    batch_chunk_index_prefix, batch_key, chunk_key, group_key, legacy_cf_for_key,
    legacy_stream_job_checkpoint_key, legacy_stream_job_runtime_key, legacy_stream_job_signal_key,
    legacy_stream_job_view_key, legacy_stream_job_view_prefix, mirrored_entry_key, offset_key,
    parse_stream_job_view_key, stream_job_checkpoint_key, stream_job_dispatch_applied_key,
    stream_job_dispatch_applied_prefix, stream_job_runtime_key, stream_job_signal_key,
    stream_job_view_key, stream_job_view_prefix, throughput_partition_id, timestamp_sort_key,
};
#[cfg(test)]
use self::scheduling_state::{lease_expiry_index_key, ready_index_key, started_index_key};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use fabrik_store::{
    WorkflowBulkBatchRecord, WorkflowBulkBatchStatus, WorkflowBulkChunkRecord,
    WorkflowBulkChunkStatus,
};
#[cfg(test)]
use fabrik_throughput::{
    ADMISSION_POLICY_VERSION, bulk_reducer_name, throughput_execution_mode,
    throughput_reducer_execution_path, throughput_reducer_version, throughput_routing_reason,
};
use fabrik_throughput::{
    ActivityExecutionCapabilities, StreamsChangelogEntry, StreamsChangelogPayload,
    ThroughputBatchIdentity, ThroughputChangelogEntry, ThroughputChangelogPayload,
    ThroughputChunkReport, ThroughputChunkReportPayload, bulk_reducer_default_summary_value,
    bulk_reducer_reduce_errors, bulk_reducer_reduce_values, bulk_reducer_requires_error_outputs,
    bulk_reducer_requires_success_outputs, bulk_reducer_settles, bulk_reducer_summary_field_name,
    decode_cbor, encode_cbor, reduction_tree_child_group_ids, reduction_tree_level_counts,
    reduction_tree_node_id, reduction_tree_node_level, reduction_tree_parent_group_id,
};
use rocksdb::{
    BlockBasedOptions, ColumnFamily, ColumnFamilyDescriptor, DB, DBCompactionStyle, Direction,
    IteratorMode, Options, SliceTransform, WriteBatch,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

const OFFSET_KEY_PREFIX: &str = "meta:offset:";
const MIRRORED_ENTRY_KEY_PREFIX: &str = "meta:mirrored-entry:";
const BATCH_KEY_PREFIX: &str = "batch:";
const CHUNK_KEY_PREFIX: &str = "chunk:";
const GROUP_KEY_PREFIX: &str = "group:";
const BATCH_CHUNK_INDEX_PREFIX: &str = "idx:batch-chunk:";
const BATCH_GROUP_INDEX_PREFIX: &str = "idx:batch-group:";
const READY_INDEX_PREFIX: &str = "idx:ready:";
const STARTED_INDEX_PREFIX: &str = "idx:started:";
const LEASE_EXPIRY_INDEX_PREFIX: &str = "idx:lease-expiry:";
const STREAM_JOB_VIEW_KEY_PREFIX: &str = "stream-job-view:";
const STREAM_JOB_RUNTIME_KEY_PREFIX: &str = "stream-job-runtime:";
const STREAM_JOB_CHECKPOINT_KEY_PREFIX: &str = "stream-job-checkpoint:";
const STREAM_JOB_SIGNAL_KEY_PREFIX: &str = "stream-job-signal:";
const STREAMS_OFFSET_KEY_PREFIX: &str = "meta:streams-offset:";
const STREAMS_MIRRORED_ENTRY_KEY_PREFIX: &str = "meta:streams-mirrored-entry:";
const LATEST_CHECKPOINT_FILE: &str = "latest.json";
const ROCKSDB_ENCODING_PREFIX: &[u8] = b"fs2\0";
const STREAM_JOB_VIEW_VALUE_ENCODING_PREFIX: &[u8] = b"fsv2\0";
const META_CF: &str = "meta";
const BATCHES_CF: &str = "batches";
const CHUNKS_CF: &str = "chunks";
const GROUPS_CF: &str = "groups";
const BATCH_INDEXES_CF: &str = "batch_indexes";
const SCHEDULING_CF: &str = "scheduling";
const STREAM_JOBS_CF: &str = "stream_jobs";

fn encode_rocksdb_value<T: Serialize>(value: &T, subject: &str) -> Result<Vec<u8>> {
    let encoded = encode_cbor(value, subject)?;
    let mut bytes = Vec::with_capacity(ROCKSDB_ENCODING_PREFIX.len() + encoded.len());
    bytes.extend_from_slice(ROCKSDB_ENCODING_PREFIX);
    bytes.extend_from_slice(&encoded);
    Ok(bytes)
}

fn decode_rocksdb_value<T: DeserializeOwned>(bytes: &[u8], subject: &str) -> Result<T> {
    if let Some(cbor_bytes) = bytes.strip_prefix(ROCKSDB_ENCODING_PREFIX) {
        return decode_cbor(cbor_bytes, subject);
    }
    serde_json::from_slice(bytes)
        .with_context(|| format!("failed to deserialize legacy JSON {subject}"))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredStreamJobViewValue {
    job_id: String,
    output: Value,
    checkpoint_sequence: i64,
    updated_at: DateTime<Utc>,
}

fn encode_stream_job_view_state_value(state: &LocalStreamJobViewState) -> Result<Vec<u8>> {
    let encoded = encode_cbor(
        &StoredStreamJobViewValue {
            job_id: state.job_id.clone(),
            output: state.output.clone(),
            checkpoint_sequence: state.checkpoint_sequence,
            updated_at: state.updated_at,
        },
        "stream job view state",
    )?;
    let mut bytes = Vec::with_capacity(STREAM_JOB_VIEW_VALUE_ENCODING_PREFIX.len() + encoded.len());
    bytes.extend_from_slice(STREAM_JOB_VIEW_VALUE_ENCODING_PREFIX);
    bytes.extend_from_slice(&encoded);
    Ok(bytes)
}

fn decode_stream_job_view_state_value(
    bytes: &[u8],
    handle_id: &str,
    view_name: &str,
    logical_key: &str,
) -> Result<LocalStreamJobViewState> {
    if let Some(cbor_bytes) = bytes.strip_prefix(STREAM_JOB_VIEW_VALUE_ENCODING_PREFIX) {
        let stored: StoredStreamJobViewValue = decode_cbor(cbor_bytes, "stream job view state")?;
        return Ok(LocalStreamJobViewState {
            handle_id: handle_id.to_owned(),
            job_id: stored.job_id,
            view_name: view_name.to_owned(),
            logical_key: logical_key.to_owned(),
            output: stored.output,
            checkpoint_sequence: stored.checkpoint_sequence,
            updated_at: stored.updated_at,
        });
    }
    decode_rocksdb_value(bytes, "stream job view state")
}

fn nth_colon_prefix(key: &[u8], colon_count: usize) -> &[u8] {
    let mut seen = 0usize;
    for (index, byte) in key.iter().enumerate() {
        if *byte == b':' {
            seen += 1;
            if seen == colon_count {
                return &key[..=index];
            }
        }
    }
    key
}

fn has_at_least_n_colons(key: &[u8], colon_count: usize) -> bool {
    key.iter().filter(|byte| **byte == b':').take(colon_count).count() >= colon_count
}

fn scheduling_prefix_transform(key: &[u8]) -> &[u8] {
    if key.starts_with(READY_INDEX_PREFIX.as_bytes())
        || key.starts_with(STARTED_INDEX_PREFIX.as_bytes())
    {
        nth_colon_prefix(key, 4)
    } else if key.starts_with(LEASE_EXPIRY_INDEX_PREFIX.as_bytes()) {
        nth_colon_prefix(key, 2)
    } else {
        key
    }
}

fn scheduling_prefix_in_domain(key: &[u8]) -> bool {
    if key.starts_with(READY_INDEX_PREFIX.as_bytes())
        || key.starts_with(STARTED_INDEX_PREFIX.as_bytes())
    {
        return has_at_least_n_colons(key, 4);
    }
    if key.starts_with(LEASE_EXPIRY_INDEX_PREFIX.as_bytes()) {
        return has_at_least_n_colons(key, 2);
    }
    false
}

fn batch_index_prefix_transform(key: &[u8]) -> &[u8] {
    nth_colon_prefix(key, 6)
}

fn batch_index_prefix_in_domain(key: &[u8]) -> bool {
    (key.starts_with(BATCH_CHUNK_INDEX_PREFIX.as_bytes())
        || key.starts_with(BATCH_GROUP_INDEX_PREFIX.as_bytes()))
        && has_at_least_n_colons(key, 6)
}

fn configured_block_based_options(block_size: usize, enable_filters: bool) -> BlockBasedOptions {
    let mut block = BlockBasedOptions::default();
    block.set_block_size(block_size);
    block.set_metadata_block_size((block_size / 2).max(1024));
    if enable_filters {
        block.set_bloom_filter(10.0, false);
        block.set_cache_index_and_filter_blocks(true);
        block.set_pin_top_level_index_and_filter(true);
        block.set_pin_l0_filter_and_index_blocks_in_cache(true);
    }
    block
}

fn meta_cf_options() -> Options {
    let mut opts = Options::default();
    opts.set_write_buffer_size(4 * 1024 * 1024);
    opts.set_target_file_size_base(8 * 1024 * 1024);
    opts.set_compaction_style(DBCompactionStyle::Level);
    opts.set_optimize_filters_for_hits(true);
    let block = configured_block_based_options(4 * 1024, true);
    opts.set_block_based_table_factory(&block);
    opts
}

fn batches_cf_options() -> Options {
    let mut opts = Options::default();
    opts.set_write_buffer_size(8 * 1024 * 1024);
    opts.set_target_file_size_base(32 * 1024 * 1024);
    opts.set_compaction_style(DBCompactionStyle::Level);
    opts.set_optimize_filters_for_hits(true);
    let block = configured_block_based_options(8 * 1024, true);
    opts.set_block_based_table_factory(&block);
    opts
}

fn chunks_cf_options() -> Options {
    let mut opts = Options::default();
    opts.set_write_buffer_size(64 * 1024 * 1024);
    opts.set_target_file_size_base(128 * 1024 * 1024);
    opts.set_compaction_style(DBCompactionStyle::Level);
    opts.set_optimize_filters_for_hits(true);
    let block = configured_block_based_options(16 * 1024, true);
    opts.set_block_based_table_factory(&block);
    opts
}

fn groups_cf_options() -> Options {
    let mut opts = Options::default();
    opts.set_write_buffer_size(8 * 1024 * 1024);
    opts.set_target_file_size_base(16 * 1024 * 1024);
    opts.set_compaction_style(DBCompactionStyle::Level);
    opts.set_optimize_filters_for_hits(true);
    let block = configured_block_based_options(4 * 1024, true);
    opts.set_block_based_table_factory(&block);
    opts
}

fn stream_jobs_cf_options() -> Options {
    let mut opts = Options::default();
    opts.set_write_buffer_size(64 * 1024 * 1024);
    opts.set_target_file_size_base(64 * 1024 * 1024);
    opts.set_compaction_style(DBCompactionStyle::Universal);
    opts.set_optimize_filters_for_hits(true);
    let block = configured_block_based_options(8 * 1024, true);
    opts.set_block_based_table_factory(&block);
    opts
}

fn batch_indexes_cf_options() -> Options {
    let mut opts = Options::default();
    opts.set_write_buffer_size(16 * 1024 * 1024);
    opts.set_target_file_size_base(32 * 1024 * 1024);
    opts.set_compaction_style(DBCompactionStyle::Universal);
    opts.set_prefix_extractor(SliceTransform::create(
        "throughput-batch-index-prefix",
        batch_index_prefix_transform,
        Some(batch_index_prefix_in_domain),
    ));
    opts.set_memtable_prefix_bloom_ratio(0.2);
    let block = configured_block_based_options(4 * 1024, true);
    opts.set_block_based_table_factory(&block);
    opts
}

fn scheduling_cf_options() -> Options {
    let mut opts = Options::default();
    opts.set_write_buffer_size(16 * 1024 * 1024);
    opts.set_target_file_size_base(32 * 1024 * 1024);
    opts.set_compaction_style(DBCompactionStyle::Universal);
    opts.set_prefix_extractor(SliceTransform::create(
        "throughput-scheduling-prefix",
        scheduling_prefix_transform,
        Some(scheduling_prefix_in_domain),
    ));
    opts.set_memtable_prefix_bloom_ratio(0.2);
    let block = configured_block_based_options(4 * 1024, true);
    opts.set_block_based_table_factory(&block);
    opts
}

#[derive(Clone)]
pub struct LocalThroughputState {
    db: Arc<DB>,
    db_path: PathBuf,
    checkpoint_dir: PathBuf,
    #[cfg_attr(not(test), allow(dead_code))]
    checkpoint_retention: usize,
    meta: Arc<Mutex<LocalStateMeta>>,
    lease_lock: Arc<Mutex<()>>,
    stream_job_view_overlay:
        Arc<Mutex<HashMap<StreamJobViewOverlayKey, StreamJobViewOverlayEntry>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalChangelogPlane {
    Throughput,
    Streams,
}

#[derive(Debug, Clone, Serialize)]
pub struct LocalThroughputDebugSnapshot {
    pub db_path: String,
    pub checkpoint_dir: String,
    pub restored_from_checkpoint: bool,
    pub last_checkpoint_at: Option<DateTime<Utc>>,
    pub checkpoint_writes: u64,
    pub checkpoint_failures: u64,
    pub changelog_entries_applied: u64,
    pub changelog_apply_failures: u64,
    pub last_changelog_apply_at: Option<DateTime<Utc>>,
    pub legacy_default_cf_entries_migrated: u64,
    pub last_legacy_default_cf_migration_at: Option<DateTime<Utc>>,
    pub batch_count: usize,
    pub chunk_count: usize,
    pub scheduled_chunk_count: u64,
    pub ready_chunk_count: u64,
    pub started_chunk_count: u64,
    pub batch_status_counts: BTreeMap<String, u64>,
    pub batch_tree_depth_counts: BTreeMap<u32, u64>,
    pub group_level_counts: BTreeMap<u32, u64>,
    pub last_applied_offsets: BTreeMap<i32, i64>,
    pub observed_high_watermarks: BTreeMap<i32, i64>,
    pub partition_lag: BTreeMap<i32, i64>,
    pub streams_last_applied_offsets: BTreeMap<i32, i64>,
    pub streams_observed_high_watermarks: BTreeMap<i32, i64>,
    pub streams_partition_lag: BTreeMap<i32, i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalThroughputShardPaths {
    pub throughput_partition_id: i32,
    pub db_path: PathBuf,
    pub checkpoint_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalThroughputCoordinatorPaths {
    pub db_path: PathBuf,
    pub checkpoint_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct LeaseSelectionDebug {
    pub indexed_candidates: u64,
    pub owned_candidates: u64,
    pub missing_batch: u64,
    pub terminal_batch: u64,
    pub batch_failed_or_cancelled: u64,
    pub batch_paused: u64,
    pub batch_cap_blocked: u64,
    pub missing_chunk: u64,
    pub chunk_not_scheduled: u64,
    pub chunk_not_due: u64,
    pub leaseable_candidates: u64,
}

#[derive(Debug, Default)]
struct LocalStateMeta {
    restored_from_checkpoint: bool,
    last_checkpoint_at: Option<DateTime<Utc>>,
    checkpoint_writes: u64,
    checkpoint_failures: u64,
    changelog_entries_applied: u64,
    changelog_apply_failures: u64,
    last_changelog_apply_at: Option<DateTime<Utc>>,
    legacy_default_cf_entries_migrated: u64,
    last_legacy_default_cf_migration_at: Option<DateTime<Utc>>,
    observed_high_watermarks: BTreeMap<i32, i64>,
    streams_observed_high_watermarks: BTreeMap<i32, i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CheckpointFile {
    created_at: DateTime<Utc>,
    offsets: BTreeMap<i32, i64>,
    #[serde(default)]
    mirrored_entry_ids: Vec<String>,
    #[serde(default)]
    streams_offsets: BTreeMap<i32, i64>,
    #[serde(default)]
    streams_mirrored_entry_ids: Vec<String>,
    batches: Vec<LocalBatchState>,
    chunks: Vec<LocalChunkState>,
    groups: Vec<LocalGroupState>,
    #[serde(default)]
    stream_job_views: Vec<LocalStreamJobViewState>,
    #[serde(default)]
    stream_job_runtime_states: Vec<LocalStreamJobRuntimeState>,
    #[serde(default)]
    stream_job_applied_dispatch_batches: Vec<LocalStreamJobAppliedDispatchBatchState>,
    #[serde(default)]
    stream_job_checkpoints: Vec<LocalStreamJobCheckpointState>,
    #[serde(default)]
    stream_job_workflow_signals: Vec<LocalStreamJobWorkflowSignalState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalBatchState {
    identity: ThroughputBatchIdentity,
    definition_id: String,
    definition_version: Option<u32>,
    artifact_hash: Option<String>,
    task_queue: String,
    activity_capabilities: Option<ActivityExecutionCapabilities>,
    execution_policy: Option<String>,
    reducer: Option<String>,
    #[serde(default = "default_reducer_class")]
    reducer_class: String,
    #[serde(default = "default_aggregation_tree_depth")]
    aggregation_tree_depth: u32,
    #[serde(default)]
    fast_lane_enabled: bool,
    aggregation_group_count: u32,
    total_items: u32,
    chunk_count: u32,
    #[serde(default)]
    terminal_chunk_count: u32,
    succeeded_items: u32,
    failed_items: u32,
    cancelled_items: u32,
    status: String,
    last_report_id: Option<String>,
    error: Option<String>,
    reducer_output: Option<Value>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    terminal_at: Option<DateTime<Utc>>,
}

fn default_reducer_class() -> String {
    "legacy".to_owned()
}

fn default_aggregation_tree_depth() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LocalChunkState {
    identity: ThroughputBatchIdentity,
    chunk_id: String,
    activity_type: String,
    task_queue: String,
    chunk_index: u32,
    group_id: u32,
    item_count: u32,
    attempt: u32,
    max_attempts: u32,
    retry_delay_ms: u64,
    lease_epoch: u64,
    owner_epoch: u64,
    status: String,
    worker_id: Option<String>,
    #[serde(default)]
    lease_token: Option<String>,
    report_id: Option<String>,
    #[serde(default)]
    result_handle: Value,
    #[serde(default)]
    output: Option<Vec<Value>>,
    error: Option<String>,
    #[serde(default)]
    input_handle: Value,
    #[serde(default)]
    items: Vec<Value>,
    #[serde(default)]
    cancellation_requested: bool,
    #[serde(default)]
    cancellation_reason: Option<String>,
    #[serde(default)]
    cancellation_metadata: Option<Value>,
    scheduled_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
    #[serde(default)]
    started_at: Option<DateTime<Utc>>,
    lease_expires_at: Option<DateTime<Utc>>,
    #[serde(default)]
    completed_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
}

fn encode_rocksdb_chunk_state(state: &LocalChunkState) -> Result<Vec<u8>> {
    encode_rocksdb_value(state, "throughput chunk state")
}

fn decode_rocksdb_chunk_state(bytes: &[u8]) -> Result<LocalChunkState> {
    decode_rocksdb_value(bytes, "throughput chunk state")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LocalGroupState {
    identity: ThroughputBatchIdentity,
    group_id: u32,
    #[serde(default)]
    level: u32,
    #[serde(default)]
    parent_group_id: Option<u32>,
    status: String,
    succeeded_items: u32,
    failed_items: u32,
    cancelled_items: u32,
    error: Option<String>,
    terminal_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LocalStreamJobViewState {
    pub(crate) handle_id: String,
    pub(crate) job_id: String,
    pub(crate) view_name: String,
    pub(crate) logical_key: String,
    pub(crate) output: Value,
    pub(crate) checkpoint_sequence: i64,
    pub(crate) updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LocalStreamJobViewRuntimeStatsState {
    pub(crate) view_name: String,
    #[serde(default)]
    pub(crate) evicted_window_count: u64,
    #[serde(default)]
    pub(crate) last_evicted_window_end: Option<DateTime<Utc>>,
    #[serde(default)]
    pub(crate) last_evicted_at: Option<DateTime<Utc>>,
}

pub(crate) type StreamJobViewOverlayKey = (String, String, String);

#[derive(Debug, Clone)]
pub(crate) enum StreamJobViewOverlayEntry {
    Present(LocalStreamJobViewState),
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LocalStreamJobRuntimeState {
    pub(crate) handle_id: String,
    pub(crate) job_id: String,
    pub(crate) job_name: String,
    pub(crate) view_name: String,
    pub(crate) checkpoint_name: String,
    pub(crate) checkpoint_sequence: i64,
    pub(crate) input_item_count: u64,
    pub(crate) materialized_key_count: u64,
    pub(crate) active_partitions: Vec<i32>,
    #[serde(default)]
    pub(crate) source_kind: Option<String>,
    #[serde(default)]
    pub(crate) source_name: Option<String>,
    #[serde(default)]
    pub(crate) source_cursors: Vec<LocalStreamJobSourceCursorState>,
    #[serde(default)]
    pub(crate) source_partition_leases: Vec<LocalStreamJobSourceLeaseState>,
    #[serde(default)]
    pub(crate) dispatch_batches: Vec<LocalStreamJobDispatchBatch>,
    #[serde(default)]
    pub(crate) applied_dispatch_batch_ids: Vec<String>,
    #[serde(default)]
    pub(crate) dispatch_completed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub(crate) dispatch_cancelled_at: Option<DateTime<Utc>>,
    pub(crate) stream_owner_epoch: u64,
    pub(crate) planned_at: DateTime<Utc>,
    pub(crate) latest_checkpoint_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub(crate) evicted_window_count: u64,
    #[serde(default)]
    pub(crate) last_evicted_window_end: Option<DateTime<Utc>>,
    #[serde(default)]
    pub(crate) last_evicted_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub(crate) view_runtime_stats: Vec<LocalStreamJobViewRuntimeStatsState>,
    #[serde(default)]
    pub(crate) checkpoint_partitions: Vec<LocalStreamJobCheckpointState>,
    pub(crate) terminal_status: Option<String>,
    pub(crate) terminal_output: Option<Value>,
    pub(crate) terminal_error: Option<String>,
    pub(crate) terminal_at: Option<DateTime<Utc>>,
    pub(crate) updated_at: DateTime<Utc>,
}

impl LocalStreamJobRuntimeState {
    pub(crate) fn view_runtime_stats(
        &self,
        view_name: &str,
    ) -> Option<&LocalStreamJobViewRuntimeStatsState> {
        self.view_runtime_stats.iter().find(|stats| stats.view_name == view_name)
    }

    pub(crate) fn record_view_eviction(
        &mut self,
        view_name: &str,
        window_end: Option<DateTime<Utc>>,
        evicted_at: Option<DateTime<Utc>>,
    ) {
        let stats = if let Some(stats) =
            self.view_runtime_stats.iter_mut().find(|stats| stats.view_name == view_name)
        {
            stats
        } else {
            self.view_runtime_stats.push(LocalStreamJobViewRuntimeStatsState {
                view_name: view_name.to_owned(),
                evicted_window_count: 0,
                last_evicted_window_end: None,
                last_evicted_at: None,
            });
            self.view_runtime_stats
                .last_mut()
                .expect("view runtime stats should exist after insertion")
        };
        stats.evicted_window_count = stats.evicted_window_count.saturating_add(1);
        if let Some(window_end) = window_end {
            stats.last_evicted_window_end = Some(
                stats.last_evicted_window_end.map_or(window_end, |current| current.max(window_end)),
            );
        }
        if let Some(evicted_at) = evicted_at {
            stats.last_evicted_at =
                Some(stats.last_evicted_at.map_or(evicted_at, |current| current.max(evicted_at)));
        }
    }

    pub(crate) fn record_checkpoint_partition(
        &mut self,
        checkpoint: LocalStreamJobCheckpointState,
    ) {
        if let Some(existing) = self.checkpoint_partitions.iter_mut().find(|state| {
            state.checkpoint_name == checkpoint.checkpoint_name
                && state.stream_partition_id == checkpoint.stream_partition_id
        }) {
            *existing = checkpoint;
            return;
        }
        self.checkpoint_partitions.push(checkpoint);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LocalStreamJobAppliedDispatchBatchState {
    pub(crate) handle_id: String,
    pub(crate) batch_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LocalStreamJobDispatchBatch {
    pub(crate) batch_id: String,
    pub(crate) stream_partition_id: i32,
    #[serde(default)]
    pub(crate) checkpoint_partition_id: Option<i32>,
    #[serde(default)]
    pub(crate) source_owner_partition_id: Option<i32>,
    #[serde(default)]
    pub(crate) source_lease_token: Option<String>,
    pub(crate) routing_key: String,
    pub(crate) key_field: String,
    #[serde(default)]
    pub(crate) reducer_kind: String,
    pub(crate) output_field: String,
    pub(crate) view_name: String,
    #[serde(default)]
    pub(crate) additional_view_names: Vec<String>,
    #[serde(default)]
    pub(crate) workflow_signals: Vec<crate::stream_jobs::StreamJobWorkflowSignalPlan>,
    #[serde(default)]
    pub(crate) eventual_projection_view_names: Vec<String>,
    pub(crate) checkpoint_name: String,
    pub(crate) checkpoint_sequence: i64,
    pub(crate) owner_epoch: u64,
    pub(crate) occurred_at: DateTime<Utc>,
    pub(crate) items: Vec<LocalStreamJobDispatchItem>,
    pub(crate) is_final_partition_batch: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LocalStreamJobDispatchItem {
    pub(crate) logical_key: String,
    pub(crate) value: f64,
    #[serde(default)]
    pub(crate) display_key: Option<String>,
    #[serde(default)]
    pub(crate) window_start: Option<String>,
    #[serde(default)]
    pub(crate) window_end: Option<String>,
    #[serde(default = "default_stream_job_item_count")]
    pub(crate) count: u64,
}

fn default_stream_job_item_count() -> u64 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LocalStreamJobSourceCursorState {
    pub(crate) source_partition_id: i32,
    pub(crate) next_offset: i64,
    pub(crate) initial_checkpoint_target_offset: i64,
    #[serde(default)]
    pub(crate) last_applied_offset: Option<i64>,
    #[serde(default)]
    pub(crate) last_high_watermark: Option<i64>,
    #[serde(default)]
    pub(crate) last_event_time_watermark: Option<DateTime<Utc>>,
    #[serde(default)]
    pub(crate) last_closed_window_end: Option<DateTime<Utc>>,
    #[serde(default)]
    pub(crate) pending_window_ends: Vec<DateTime<Utc>>,
    #[serde(default)]
    pub(crate) dropped_late_event_count: u64,
    #[serde(default)]
    pub(crate) last_dropped_late_offset: Option<i64>,
    #[serde(default)]
    pub(crate) last_dropped_late_event_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub(crate) last_dropped_late_window_end: Option<DateTime<Utc>>,
    #[serde(default)]
    pub(crate) dropped_evicted_window_event_count: u64,
    #[serde(default)]
    pub(crate) last_dropped_evicted_window_offset: Option<i64>,
    #[serde(default)]
    pub(crate) last_dropped_evicted_window_event_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub(crate) last_dropped_evicted_window_end: Option<DateTime<Utc>>,
    #[serde(default)]
    pub(crate) checkpoint_reached_at: Option<DateTime<Utc>>,
    pub(crate) updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LocalStreamJobSourceLeaseState {
    pub(crate) source_partition_id: i32,
    pub(crate) owner_partition_id: i32,
    pub(crate) owner_epoch: u64,
    pub(crate) lease_token: String,
    pub(crate) updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LocalStreamJobCheckpointState {
    pub(crate) handle_id: String,
    pub(crate) job_id: String,
    pub(crate) checkpoint_name: String,
    pub(crate) checkpoint_sequence: i64,
    pub(crate) stream_partition_id: i32,
    pub(crate) stream_owner_epoch: u64,
    pub(crate) reached_at: DateTime<Utc>,
    pub(crate) updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LocalStreamJobWorkflowSignalState {
    pub(crate) handle_id: String,
    pub(crate) job_id: String,
    pub(crate) operator_id: String,
    pub(crate) view_name: String,
    pub(crate) logical_key: String,
    pub(crate) signal_id: String,
    pub(crate) signal_type: String,
    pub(crate) payload: Value,
    pub(crate) stream_owner_epoch: u64,
    pub(crate) signaled_at: DateTime<Utc>,
    #[serde(default)]
    pub(crate) published_at: Option<DateTime<Utc>>,
    pub(crate) updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchChunkIndexEntry {
    identity: ThroughputBatchIdentity,
    chunk_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BatchGroupIndexEntry {
    identity: ThroughputBatchIdentity,
    group_id: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReadyChunkIndexEntry {
    identity: ThroughputBatchIdentity,
    chunk_id: String,
    group_id: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StartedChunkIndexEntry {
    identity: ThroughputBatchIdentity,
    chunk_id: String,
    group_id: u32,
    lease_expires_at: Option<DateTime<Utc>>,
    scheduled_at: DateTime<Utc>,
    chunk_index: u32,
}

#[derive(Debug, Clone)]
pub struct ExpiredChunkCandidate {
    pub identity: ThroughputBatchIdentity,
    pub chunk_id: String,
}

#[derive(Debug, Clone)]
pub struct LeasedChunkSnapshot {
    pub identity: ThroughputBatchIdentity,
    pub definition_id: String,
    pub definition_version: Option<u32>,
    pub artifact_hash: Option<String>,
    pub chunk_id: String,
    pub activity_type: String,
    pub activity_capabilities: Option<ActivityExecutionCapabilities>,
    pub task_queue: String,
    pub chunk_index: u32,
    pub group_id: u32,
    pub attempt: u32,
    pub item_count: u32,
    pub max_attempts: u32,
    pub retry_delay_ms: u64,
    pub items: Vec<Value>,
    pub input_handle: Value,
    pub omit_success_output: bool,
    pub cancellation_requested: bool,
    pub lease_epoch: u64,
    pub owner_epoch: u64,
    pub worker_id: Option<String>,
    pub lease_token: Option<String>,
    pub scheduled_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct LocalBatchSnapshot {
    pub identity: ThroughputBatchIdentity,
    pub definition_id: String,
    pub definition_version: Option<u32>,
    pub artifact_hash: Option<String>,
    pub task_queue: String,
    pub activity_capabilities: Option<ActivityExecutionCapabilities>,
    pub execution_policy: Option<String>,
    pub reducer: Option<String>,
    pub reducer_class: String,
    pub aggregation_tree_depth: u32,
    pub fast_lane_enabled: bool,
    pub total_items: u32,
    pub chunk_count: u32,
    pub succeeded_items: u32,
    pub failed_items: u32,
    pub cancelled_items: u32,
    pub status: String,
    pub error: Option<String>,
    pub reducer_output: Option<Value>,
    pub updated_at: DateTime<Utc>,
    pub terminal_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedBatchGroupSummary {
    pub succeeded_items: u32,
    pub failed_items: u32,
    pub cancelled_items: u32,
    pub error: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct LocalChunkSnapshot {
    pub chunk_id: String,
    pub status: String,
    pub attempt: u32,
    pub lease_epoch: u64,
    pub owner_epoch: u64,
    pub result_handle: Value,
    pub output: Option<Vec<Value>>,
    pub error: Option<String>,
    pub cancellation_requested: bool,
    pub cancellation_reason: Option<String>,
    pub cancellation_metadata: Option<Value>,
    pub worker_id: Option<String>,
    pub lease_token: Option<String>,
    pub report_id: Option<String>,
    pub available_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportValidation {
    Accept,
    RejectTerminalBatch,
    RejectMissingChunk,
    RejectChunkNotStarted,
    RejectAttemptMismatch,
    RejectLeaseEpochMismatch,
    RejectLeaseTokenMismatch,
    RejectOwnerEpochMismatch,
    RejectAlreadyApplied,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedTerminalApply {
    pub batch_definition_id: String,
    pub batch_definition_version: Option<u32>,
    pub batch_artifact_hash: Option<String>,
    pub batch_task_queue: String,
    pub batch_execution_policy: Option<String>,
    pub batch_reducer: Option<String>,
    pub batch_reducer_class: String,
    pub batch_aggregation_tree_depth: u32,
    pub batch_fast_lane_enabled: bool,
    pub batch_total_items: u32,
    pub batch_chunk_count: u32,
    pub chunk_id: String,
    pub chunk_status: String,
    pub chunk_attempt: u32,
    pub chunk_available_at: DateTime<Utc>,
    pub chunk_error: Option<String>,
    pub chunk_item_count: u32,
    pub chunk_max_attempts: u32,
    pub chunk_retry_delay_ms: u64,
    pub batch_status: String,
    pub batch_succeeded_items: u32,
    pub batch_failed_items: u32,
    pub batch_cancelled_items: u32,
    pub batch_error: Option<String>,
    pub batch_terminal: bool,
    pub batch_terminal_deferred: bool,
    pub terminal_at: Option<DateTime<Utc>>,
    pub group_terminal: Option<ProjectedGroupTerminal>,
    pub parent_group_terminals: Vec<ProjectedGroupTerminal>,
    pub grouped_batch_terminal: Option<ProjectedBatchTerminal>,
    pub grouped_batch_summary: Option<ProjectedBatchGroupSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedReportChunk {
    pub chunk_index: u32,
    pub group_id: u32,
    pub status: String,
    pub attempt: u32,
    pub available_at: DateTime<Utc>,
    pub error: Option<String>,
    pub item_count: u32,
    pub max_attempts: u32,
    pub retry_delay_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedChunkApply {
    pub batch_definition_id: String,
    pub batch_definition_version: Option<u32>,
    pub batch_artifact_hash: Option<String>,
    pub batch_task_queue: String,
    pub batch_execution_policy: Option<String>,
    pub batch_reducer: Option<String>,
    pub batch_reducer_class: String,
    pub batch_aggregation_tree_depth: u32,
    pub batch_fast_lane_enabled: bool,
    pub batch_total_items: u32,
    pub batch_chunk_count: u32,
    pub chunk_id: String,
    pub chunk_group_id: u32,
    pub chunk_status: String,
    pub chunk_attempt: u32,
    pub chunk_available_at: DateTime<Utc>,
    pub chunk_error: Option<String>,
    pub chunk_item_count: u32,
    pub chunk_max_attempts: u32,
    pub chunk_retry_delay_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedBatchRollup {
    pub batch_status: String,
    pub batch_succeeded_items: u32,
    pub batch_failed_items: u32,
    pub batch_cancelled_items: u32,
    pub batch_error: Option<String>,
    pub batch_terminal: bool,
    pub batch_terminal_deferred: bool,
    pub terminal_at: Option<DateTime<Utc>>,
    pub group_terminal: Option<ProjectedGroupTerminal>,
    pub parent_group_terminals: Vec<ProjectedGroupTerminal>,
    pub grouped_batch_terminal: Option<ProjectedBatchTerminal>,
    pub grouped_batch_summary: Option<ProjectedBatchGroupSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreparedValidatedReport {
    MissingBatch,
    Rejected(ReportValidation),
    Accepted(ValidatedReportChunk),
}

impl ProjectedTerminalApply {
    pub fn from_parts(chunk: ProjectedChunkApply, rollup: ProjectedBatchRollup) -> Self {
        Self {
            batch_definition_id: chunk.batch_definition_id,
            batch_definition_version: chunk.batch_definition_version,
            batch_artifact_hash: chunk.batch_artifact_hash,
            batch_task_queue: chunk.batch_task_queue,
            batch_execution_policy: chunk.batch_execution_policy,
            batch_reducer: chunk.batch_reducer,
            batch_reducer_class: chunk.batch_reducer_class,
            batch_aggregation_tree_depth: chunk.batch_aggregation_tree_depth,
            batch_fast_lane_enabled: chunk.batch_fast_lane_enabled,
            batch_total_items: chunk.batch_total_items,
            batch_chunk_count: chunk.batch_chunk_count,
            chunk_id: chunk.chunk_id,
            chunk_status: chunk.chunk_status,
            chunk_attempt: chunk.chunk_attempt,
            chunk_available_at: chunk.chunk_available_at,
            chunk_error: chunk.chunk_error,
            chunk_item_count: chunk.chunk_item_count,
            chunk_max_attempts: chunk.chunk_max_attempts,
            chunk_retry_delay_ms: chunk.chunk_retry_delay_ms,
            batch_status: rollup.batch_status,
            batch_succeeded_items: rollup.batch_succeeded_items,
            batch_failed_items: rollup.batch_failed_items,
            batch_cancelled_items: rollup.batch_cancelled_items,
            batch_error: rollup.batch_error,
            batch_terminal: rollup.batch_terminal,
            batch_terminal_deferred: rollup.batch_terminal_deferred,
            terminal_at: rollup.terminal_at,
            group_terminal: rollup.group_terminal,
            parent_group_terminals: rollup.parent_group_terminals,
            grouped_batch_terminal: rollup.grouped_batch_terminal,
            grouped_batch_summary: rollup.grouped_batch_summary,
        }
    }
}

fn validate_loaded_report(
    batch: Option<&LocalBatchState>,
    chunk: Option<&LocalChunkState>,
    attempt: u32,
    lease_epoch: u64,
    lease_token: &str,
    owner_epoch: u64,
    report_id: &str,
) -> ReportValidation {
    let Some(batch) = batch else {
        return ReportValidation::RejectMissingChunk;
    };
    if matches!(batch.status.as_str(), "completed" | "failed" | "cancelled") {
        return ReportValidation::RejectTerminalBatch;
    }
    let Some(chunk) = chunk else {
        return ReportValidation::RejectMissingChunk;
    };
    if chunk.report_id.as_deref() == Some(report_id) {
        return ReportValidation::RejectAlreadyApplied;
    }
    if chunk.status != WorkflowBulkChunkStatus::Started.as_str() {
        return ReportValidation::RejectChunkNotStarted;
    }
    if chunk.attempt != attempt {
        return ReportValidation::RejectAttemptMismatch;
    }
    if chunk.lease_epoch != lease_epoch {
        return ReportValidation::RejectLeaseEpochMismatch;
    }
    if chunk.lease_token.as_deref() != Some(lease_token) {
        return ReportValidation::RejectLeaseTokenMismatch;
    }
    if chunk.owner_epoch != owner_epoch {
        return ReportValidation::RejectOwnerEpochMismatch;
    }
    ReportValidation::Accept
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedBatchTerminal {
    pub status: String,
    pub succeeded_items: u32,
    pub failed_items: u32,
    pub cancelled_items: u32,
    pub error: Option<String>,
    pub terminal_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedChunkReschedule {
    pub chunk_id: String,
    pub chunk_index: u32,
    pub group_id: u32,
    pub attempt: u32,
    pub item_count: u32,
    pub max_attempts: u32,
    pub retry_delay_ms: u64,
    pub lease_epoch: u64,
    pub owner_epoch: u64,
    pub available_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedGroupTerminal {
    pub group_id: u32,
    pub level: u32,
    pub parent_group_id: Option<u32>,
    pub status: String,
    pub succeeded_items: u32,
    pub failed_items: u32,
    pub cancelled_items: u32,
    pub error: Option<String>,
    pub terminal_at: DateTime<Utc>,
}

impl LocalThroughputState {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregation::AggregationCoordinator;
    use fabrik_throughput::{
        StreamsChangelogEntry, StreamsChangelogPayload, ThroughputChangelogEntry,
        ThroughputChangelogPayload, group_id_for_chunk_index, reduction_tree_node_id,
        reduction_tree_parent_group_id,
    };
    use serde_json::json;
    use uuid::Uuid;

    const TEST_LEASE_TOKEN: &str = "lease-token-a";

    fn temp_path(prefix: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("{prefix}-{}", Uuid::now_v7()));
        if path.exists() {
            let _ = fs::remove_dir_all(&path);
        }
        path
    }

    fn identity() -> ThroughputBatchIdentity {
        ThroughputBatchIdentity {
            tenant_id: "tenant-a".to_owned(),
            instance_id: "wf-a".to_owned(),
            run_id: "run-a".to_owned(),
            batch_id: "batch-a".to_owned(),
        }
    }

    fn entry(payload: ThroughputChangelogPayload) -> ThroughputChangelogEntry {
        ThroughputChangelogEntry {
            entry_id: Uuid::now_v7(),
            occurred_at: Utc::now(),
            partition_key: "batch-a:0".to_owned(),
            payload,
        }
    }

    fn streams_entry(payload: StreamsChangelogPayload) -> StreamsChangelogEntry {
        StreamsChangelogEntry {
            entry_id: Uuid::now_v7(),
            occurred_at: Utc::now(),
            partition_key: "stream-job-a:0".to_owned(),
            payload,
        }
    }

    fn report(payload: ThroughputChunkReportPayload) -> ThroughputChunkReport {
        ThroughputChunkReport {
            report_id: "report-a".to_owned(),
            tenant_id: "tenant-a".to_owned(),
            instance_id: "wf-a".to_owned(),
            run_id: "run-a".to_owned(),
            batch_id: "batch-a".to_owned(),
            chunk_id: "chunk-a".to_owned(),
            chunk_index: 0,
            group_id: 0,
            attempt: 1,
            lease_epoch: 1,
            owner_epoch: 1,
            worker_id: "worker-a".to_owned(),
            worker_build_id: "build-a".to_owned(),
            lease_token: TEST_LEASE_TOKEN.to_owned(),
            occurred_at: Utc::now(),
            payload,
        }
    }

    fn validate_report_result(
        state: &LocalThroughputState,
        identity: &ThroughputBatchIdentity,
        chunk_id: &str,
        attempt: u32,
        lease_epoch: u64,
        lease_token: &str,
        owner_epoch: u64,
        report_id: &str,
    ) -> Result<ReportValidation> {
        let report = ThroughputChunkReport {
            report_id: report_id.to_owned(),
            tenant_id: identity.tenant_id.clone(),
            instance_id: identity.instance_id.clone(),
            run_id: identity.run_id.clone(),
            batch_id: identity.batch_id.clone(),
            chunk_id: chunk_id.to_owned(),
            chunk_index: 0,
            group_id: 0,
            attempt,
            lease_epoch,
            owner_epoch,
            worker_id: "worker-a".to_owned(),
            worker_build_id: "build-a".to_owned(),
            lease_token: lease_token.to_owned(),
            occurred_at: Utc::now(),
            payload: ThroughputChunkReportPayload::ChunkCompleted {
                output: None,
                result_handle: Value::Null,
            },
        };
        Ok(match state.prepare_report_validation(&report)? {
            PreparedValidatedReport::MissingBatch => ReportValidation::RejectMissingChunk,
            PreparedValidatedReport::Rejected(validation) => validation,
            PreparedValidatedReport::Accepted(_) => ReportValidation::Accept,
        })
    }

    fn lease_one_ready_chunk(
        state: &LocalThroughputState,
        tenant_id: &str,
        task_queue: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_ttl: chrono::Duration,
        max_active_chunks_per_batch: Option<usize>,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
    ) -> Result<Option<LeasedChunkSnapshot>> {
        let paused_batch_ids = HashSet::new();
        Ok(state
            .lease_ready_chunks(
                tenant_id,
                task_queue,
                worker_id,
                now,
                lease_ttl,
                max_active_chunks_per_batch,
                &paused_batch_ids,
                owned_partitions,
                partition_count,
                1,
            )?
            .into_iter()
            .next())
    }

    fn project_report_apply(
        scheduling: &LocalThroughputState,
        aggregation: &AggregationCoordinator,
        report: &ThroughputChunkReport,
    ) -> Result<Option<ProjectedTerminalApply>> {
        let identity = ThroughputBatchIdentity {
            tenant_id: report.tenant_id.clone(),
            instance_id: report.instance_id.clone(),
            run_id: report.run_id.clone(),
            batch_id: report.batch_id.clone(),
        };
        let validated = match scheduling.prepare_report_validation(report)? {
            PreparedValidatedReport::MissingBatch => return Ok(None),
            PreparedValidatedReport::Rejected(_) => return Ok(None),
            PreparedValidatedReport::Accepted(validated) => validated,
        };
        let Some(chunk) = scheduling.project_chunk_apply_after_validation(report, &validated)?
        else {
            return Ok(None);
        };
        let Some(rollup) =
            aggregation.project_batch_rollup_after_chunk_apply(&identity, report, &chunk)?
        else {
            return Ok(None);
        };
        Ok(Some(ProjectedTerminalApply::from_parts(chunk, rollup)))
    }

    fn store_batch(
        status: WorkflowBulkBatchStatus,
        total_items: u32,
        chunk_count: u32,
        aggregation_group_count: u32,
        scheduled_at: DateTime<Utc>,
    ) -> WorkflowBulkBatchRecord {
        WorkflowBulkBatchRecord {
            tenant_id: "tenant-a".to_owned(),
            instance_id: "wf-a".to_owned(),
            run_id: "run-a".to_owned(),
            definition_id: "demo".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-a".to_owned()),
            batch_id: "batch-a".to_owned(),
            activity_type: "benchmark.echo".to_owned(),
            activity_capabilities: None,
            task_queue: "bulk".to_owned(),
            state: Some("join".to_owned()),
            input_handle: Value::Null,
            result_handle: Value::Null,
            throughput_backend: "stream-v2".to_owned(),
            throughput_backend_version: "2.0.0".to_owned(),
            execution_mode: throughput_execution_mode("stream-v2").to_owned(),
            selected_backend: "stream-v2".to_owned(),
            routing_reason: throughput_routing_reason("stream-v2", Some("eager"), Some("count"))
                .to_owned(),
            admission_policy_version: ADMISSION_POLICY_VERSION.to_owned(),
            execution_policy: Some("eager".to_owned()),
            reducer: Some("count".to_owned()),
            reducer_kind: bulk_reducer_name(Some("count")).to_owned(),
            reducer_class: "mergeable".to_owned(),
            reducer_version: throughput_reducer_version(Some("count")).to_owned(),
            reducer_execution_path: throughput_reducer_execution_path(
                "stream-v2",
                Some("eager"),
                Some("count"),
            )
            .to_owned(),
            aggregation_tree_depth: 2,
            fast_lane_enabled: true,
            aggregation_group_count,
            status,
            total_items,
            chunk_size: 1,
            chunk_count,
            succeeded_items: 0,
            failed_items: 0,
            cancelled_items: 0,
            max_attempts: 3,
            retry_delay_ms: 1000,
            error: None,
            reducer_output: None,
            scheduled_at,
            terminal_at: None,
            updated_at: scheduled_at,
        }
    }

    fn store_chunk(
        batch: &WorkflowBulkBatchRecord,
        chunk_id: &str,
        chunk_index: u32,
        status: WorkflowBulkChunkStatus,
        item_count: u32,
        updated_at: DateTime<Utc>,
    ) -> WorkflowBulkChunkRecord {
        WorkflowBulkChunkRecord {
            tenant_id: batch.tenant_id.clone(),
            instance_id: batch.instance_id.clone(),
            run_id: batch.run_id.clone(),
            definition_id: batch.definition_id.clone(),
            definition_version: batch.definition_version,
            artifact_hash: batch.artifact_hash.clone(),
            batch_id: batch.batch_id.clone(),
            chunk_id: chunk_id.to_owned(),
            chunk_index,
            group_id: group_id_for_chunk_index(chunk_index, batch.aggregation_group_count),
            item_count,
            activity_type: batch.activity_type.clone(),
            task_queue: batch.task_queue.clone(),
            state: batch.state.clone(),
            status,
            attempt: 1,
            lease_epoch: 1,
            owner_epoch: 1,
            max_attempts: batch.max_attempts,
            retry_delay_ms: batch.retry_delay_ms,
            input_handle: Value::Null,
            result_handle: Value::Null,
            items: Vec::new(),
            output: None,
            error: None,
            cancellation_requested: false,
            cancellation_reason: None,
            cancellation_metadata: None,
            worker_id: None,
            worker_build_id: None,
            lease_token: None,
            last_report_id: None,
            scheduled_at: batch.scheduled_at,
            available_at: batch.scheduled_at,
            started_at: None,
            lease_expires_at: None,
            completed_at: None,
            updated_at,
        }
    }

    fn local_chunk_state_from_record(chunk: &WorkflowBulkChunkRecord) -> LocalChunkState {
        LocalChunkState {
            identity: ThroughputBatchIdentity {
                tenant_id: chunk.tenant_id.clone(),
                instance_id: chunk.instance_id.clone(),
                run_id: chunk.run_id.clone(),
                batch_id: chunk.batch_id.clone(),
            },
            chunk_id: chunk.chunk_id.clone(),
            activity_type: chunk.activity_type.clone(),
            task_queue: chunk.task_queue.clone(),
            chunk_index: chunk.chunk_index,
            group_id: chunk.group_id,
            item_count: chunk.item_count,
            attempt: chunk.attempt,
            max_attempts: chunk.max_attempts,
            retry_delay_ms: chunk.retry_delay_ms,
            lease_epoch: chunk.lease_epoch,
            owner_epoch: chunk.owner_epoch,
            status: chunk.status.as_str().to_owned(),
            worker_id: chunk.worker_id.clone(),
            lease_token: chunk.lease_token.map(|value| value.to_string()),
            report_id: chunk.last_report_id.clone(),
            result_handle: chunk.result_handle.clone(),
            output: chunk.output.clone(),
            error: chunk.error.clone(),
            input_handle: chunk.input_handle.clone(),
            items: chunk.items.clone(),
            cancellation_requested: chunk.cancellation_requested,
            cancellation_reason: chunk.cancellation_reason.clone(),
            cancellation_metadata: chunk.cancellation_metadata.clone(),
            scheduled_at: chunk.scheduled_at,
            available_at: chunk.available_at,
            started_at: chunk.started_at,
            lease_expires_at: chunk.lease_expires_at,
            completed_at: chunk.completed_at,
            updated_at: chunk.updated_at,
        }
    }

    #[test]
    fn local_state_writes_cbor_rocksdb_values_with_format_prefix() -> Result<()> {
        let db_path = temp_path("throughput-state-json-db");
        let checkpoint_dir = temp_path("throughput-state-json-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let scheduled_at = Utc::now();
        let batch = store_batch(WorkflowBulkBatchStatus::Scheduled, 2, 2, 1, scheduled_at);
        let scheduled =
            store_chunk(&batch, "chunk-a", 0, WorkflowBulkChunkStatus::Scheduled, 1, scheduled_at);
        let mut started = store_chunk(
            &batch,
            "chunk-b",
            1,
            WorkflowBulkChunkStatus::Started,
            1,
            scheduled_at + chrono::Duration::seconds(1),
        );
        started.worker_id = Some("worker-a".to_owned());
        started.lease_token = Some(Uuid::now_v7());
        started.started_at = Some(scheduled_at + chrono::Duration::seconds(1));
        started.lease_expires_at = Some(scheduled_at + chrono::Duration::seconds(30));

        state.upsert_batch_with_chunks(&batch, &[scheduled.clone(), started.clone()])?;
        let scheduled_state = local_chunk_state_from_record(&scheduled);
        let started_state = local_chunk_state_from_record(&started);

        let batch_bytes = state
            .db
            .get_cf(state.cf(BATCHES_CF), batch_key(&identity()))?
            .expect("batch state should be written");
        assert!(batch_bytes.starts_with(ROCKSDB_ENCODING_PREFIX));
        let decoded_batch: LocalBatchState =
            decode_rocksdb_value(&batch_bytes, "throughput batch state")?;
        assert_eq!(decoded_batch.status, WorkflowBulkBatchStatus::Running.as_str());

        let scheduled_bytes = state
            .db
            .get_cf(state.cf(CHUNKS_CF), chunk_key(&identity(), "chunk-a"))?
            .expect("scheduled chunk state should be written");
        assert!(scheduled_bytes.starts_with(ROCKSDB_ENCODING_PREFIX));
        let decoded_scheduled: LocalChunkState =
            decode_rocksdb_value(&scheduled_bytes, "throughput chunk state")?;
        assert_eq!(decoded_scheduled.status, WorkflowBulkChunkStatus::Scheduled.as_str());

        let started_bytes = state
            .db
            .get_cf(state.cf(CHUNKS_CF), chunk_key(&identity(), "chunk-b"))?
            .expect("started chunk state should be written");
        assert!(started_bytes.starts_with(ROCKSDB_ENCODING_PREFIX));
        let decoded_started: LocalChunkState =
            decode_rocksdb_value(&started_bytes, "throughput chunk state")?;
        assert_eq!(decoded_started.status, WorkflowBulkChunkStatus::Started.as_str());

        let chunk_index_bytes = state
            .db
            .get_cf(state.cf(BATCH_INDEXES_CF), batch_chunk_index_key(&identity(), "chunk-a"))?
            .expect("batch chunk index should be written");
        assert!(chunk_index_bytes.starts_with(ROCKSDB_ENCODING_PREFIX));
        let decoded_chunk_index: BatchChunkIndexEntry =
            decode_rocksdb_value(&chunk_index_bytes, "batch chunk index entry")?;
        assert_eq!(decoded_chunk_index.chunk_id, "chunk-a");

        let ready_index_bytes = state
            .db
            .get_cf(state.cf(SCHEDULING_CF), ready_index_key(&scheduled_state))?
            .expect("ready index should be written");
        assert!(ready_index_bytes.starts_with(ROCKSDB_ENCODING_PREFIX));
        let decoded_ready_index: ReadyChunkIndexEntry =
            decode_rocksdb_value(&ready_index_bytes, "ready chunk index entry")?;
        assert_eq!(decoded_ready_index.chunk_id, "chunk-a");

        let started_index_bytes = state
            .db
            .get_cf(state.cf(SCHEDULING_CF), started_index_key(&started_state))?
            .expect("started index should be written");
        assert!(started_index_bytes.starts_with(ROCKSDB_ENCODING_PREFIX));
        let decoded_started_index: StartedChunkIndexEntry =
            decode_rocksdb_value(&started_index_bytes, "started chunk index entry")?;
        assert_eq!(decoded_started_index.chunk_id, "chunk-b");

        let lease_expiry_bytes = state
            .db
            .get_cf(state.cf(SCHEDULING_CF), lease_expiry_index_key(&started_state))?
            .expect("lease expiry index should be written");
        assert!(lease_expiry_bytes.starts_with(ROCKSDB_ENCODING_PREFIX));
        let decoded_lease_expiry: StartedChunkIndexEntry =
            decode_rocksdb_value(&lease_expiry_bytes, "lease expiry index entry")?;
        assert_eq!(decoded_lease_expiry.chunk_id, "chunk-b");

        let mut write_batch = WriteBatch::default();
        state.write_group_state(
            &mut write_batch,
            &LocalGroupState {
                identity: identity(),
                group_id: 0,
                level: 0,
                parent_group_id: None,
                status: WorkflowBulkBatchStatus::Completed.as_str().to_owned(),
                succeeded_items: 2,
                failed_items: 0,
                cancelled_items: 0,
                error: None,
                terminal_at: scheduled_at + chrono::Duration::seconds(2),
            },
        )?;
        state.db.write(write_batch)?;

        let group_bytes = state
            .db
            .get_cf(state.cf(GROUPS_CF), group_key(&identity(), 0))?
            .expect("group state should be written");
        assert!(group_bytes.starts_with(ROCKSDB_ENCODING_PREFIX));
        let decoded_group: LocalGroupState =
            decode_rocksdb_value(&group_bytes, "throughput group state")?;
        assert_eq!(decoded_group.group_id, 0);

        let group_index_bytes = state
            .db
            .get_cf(state.cf(BATCH_INDEXES_CF), batch_group_index_key(&identity(), 0))?
            .expect("group index should be written");
        assert!(group_index_bytes.starts_with(ROCKSDB_ENCODING_PREFIX));
        let decoded_group_index: BatchGroupIndexEntry =
            decode_rocksdb_value(&group_index_bytes, "batch group index entry")?;
        assert_eq!(decoded_group_index.group_id, 0);

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_coordinator_paths_are_nested_under_coordinator_dirs() {
        let paths = LocalThroughputState::coordinator_paths(
            "/tmp/streams-local-state",
            "/tmp/streams-checkpoints",
        );
        assert_eq!(paths.db_path, PathBuf::from("/tmp/streams-local-state/coordinator"));
        assert_eq!(paths.checkpoint_dir, PathBuf::from("/tmp/streams-checkpoints/coordinator"));
    }

    #[test]
    fn local_state_open_coordinator_creates_coordinator_scoped_storage() -> Result<()> {
        let db_root = temp_path("throughput-state-coordinator-root-db");
        let checkpoint_root = temp_path("throughput-state-coordinator-root-checkpoints");
        let state = LocalThroughputState::open_coordinator(&db_root, &checkpoint_root, 2)?;
        let snapshot = state.debug_snapshot()?;
        assert!(snapshot.db_path.ends_with("coordinator"));
        assert!(snapshot.checkpoint_dir.ends_with("coordinator"));
        assert!(PathBuf::from(&snapshot.db_path).exists());
        assert!(PathBuf::from(&snapshot.checkpoint_dir).exists());
        let _ = fs::remove_dir_all(&db_root);
        let _ = fs::remove_dir_all(&checkpoint_root);
        Ok(())
    }

    #[test]
    fn local_state_partition_shard_paths_are_nested_under_shards_dirs() {
        let paths = LocalThroughputState::partition_shard_paths(
            "/tmp/streams-local-state",
            "/tmp/streams-checkpoints",
            7,
        );
        assert_eq!(paths.throughput_partition_id, 7);
        assert_eq!(paths.db_path, PathBuf::from("/tmp/streams-local-state/shards/partition-7"));
        assert_eq!(
            paths.checkpoint_dir,
            PathBuf::from("/tmp/streams-checkpoints/shards/partition-7")
        );
    }

    #[test]
    fn local_state_open_partition_shard_creates_partition_scoped_storage() -> Result<()> {
        let db_root = temp_path("throughput-state-shard-root-db");
        let checkpoint_root = temp_path("throughput-state-shard-root-checkpoints");
        let shard = LocalThroughputState::open_partition_shard(&db_root, &checkpoint_root, 3, 2)?;
        let snapshot = shard.debug_snapshot()?;
        assert!(snapshot.db_path.ends_with("shards/partition-3"));
        assert!(snapshot.checkpoint_dir.ends_with("shards/partition-3"));
        assert!(PathBuf::from(&snapshot.db_path).exists());
        assert!(PathBuf::from(&snapshot.checkpoint_dir).exists());
        let _ = fs::remove_dir_all(&db_root);
        let _ = fs::remove_dir_all(&checkpoint_root);
        Ok(())
    }

    #[test]
    fn local_state_partition_checkpoint_filters_to_single_partition() -> Result<()> {
        let db_path = temp_path("throughput-state-partition-checkpoint-db");
        let checkpoint_dir = temp_path("throughput-state-partition-checkpoint-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let occurred_at = Utc::now();
        let batch = store_batch(WorkflowBulkBatchStatus::Scheduled, 2, 2, 2, occurred_at);
        let chunk_g0 =
            store_chunk(&batch, "chunk-g0", 0, WorkflowBulkChunkStatus::Scheduled, 1, occurred_at);
        let chunk_g1 =
            store_chunk(&batch, "chunk-g1", 1, WorkflowBulkChunkStatus::Scheduled, 1, occurred_at);
        state.upsert_batch_with_chunks(&batch, &[chunk_g0.clone(), chunk_g1.clone()])?;
        let partition_g0 = throughput_partition_id(&batch.batch_id, chunk_g0.group_id, 2);
        let partition_g1 = throughput_partition_id(&batch.batch_id, chunk_g1.group_id, 2);

        let partition_zero_checkpoint =
            state.snapshot_partition_checkpoint_value(partition_g0, 2)?;
        let checkpoint: CheckpointFile = serde_json::from_value(partition_zero_checkpoint)
            .expect("partition checkpoint decodes");
        assert_eq!(checkpoint.mirrored_entry_ids, Vec::<String>::new());
        assert_eq!(checkpoint.batches.len(), 1);
        assert!(!checkpoint.chunks.is_empty());
        assert_eq!(
            checkpoint
                .chunks
                .iter()
                .filter(|chunk| {
                    throughput_partition_id(&chunk.identity.batch_id, chunk.group_id, 2)
                        == partition_g0
                })
                .count(),
            checkpoint.chunks.len()
        );
        assert!(checkpoint.chunks.iter().any(|chunk| chunk.chunk_id == "chunk-g0"));
        assert_eq!(checkpoint.groups.len(), 0);

        let partition_one_checkpoint =
            state.snapshot_partition_checkpoint_value(partition_g1, 2)?;
        let checkpoint: CheckpointFile =
            serde_json::from_value(partition_one_checkpoint).expect("partition checkpoint decodes");
        assert_eq!(checkpoint.batches.len(), 1);
        assert!(!checkpoint.chunks.is_empty());
        assert_eq!(
            checkpoint
                .chunks
                .iter()
                .filter(|chunk| {
                    throughput_partition_id(&chunk.identity.batch_id, chunk.group_id, 2)
                        == partition_g1
                })
                .count(),
            checkpoint.chunks.len()
        );
        assert!(checkpoint.chunks.iter().any(|chunk| chunk.chunk_id == "chunk-g1"));

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_reads_legacy_json_rocksdb_values() -> Result<()> {
        let db_path = temp_path("throughput-state-legacy-json-db");
        let checkpoint_dir = temp_path("throughput-state-legacy-json-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let occurred_at = Utc::now();
        let identity = identity();
        let batch_state = LocalBatchState {
            identity: identity.clone(),
            definition_id: "demo".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-a".to_owned()),
            task_queue: "bulk".to_owned(),
            activity_capabilities: None,
            execution_policy: Some("eager".to_owned()),
            reducer: Some("count".to_owned()),
            reducer_class: "mergeable".to_owned(),
            aggregation_tree_depth: 2,
            fast_lane_enabled: true,
            aggregation_group_count: 2,
            total_items: 2,
            chunk_count: 1,
            terminal_chunk_count: 0,
            succeeded_items: 0,
            failed_items: 0,
            cancelled_items: 0,
            status: WorkflowBulkBatchStatus::Running.as_str().to_owned(),
            last_report_id: None,
            error: None,
            reducer_output: None,
            created_at: occurred_at,
            updated_at: occurred_at,
            terminal_at: None,
        };
        let chunk_state = LocalChunkState {
            identity: identity.clone(),
            chunk_id: "chunk-a".to_owned(),
            activity_type: "benchmark.echo".to_owned(),
            task_queue: "bulk".to_owned(),
            chunk_index: 0,
            group_id: 0,
            item_count: 2,
            attempt: 1,
            max_attempts: 3,
            retry_delay_ms: 1000,
            lease_epoch: 0,
            owner_epoch: 1,
            status: WorkflowBulkChunkStatus::Scheduled.as_str().to_owned(),
            worker_id: None,
            lease_token: None,
            report_id: None,
            result_handle: Value::Null,
            output: None,
            error: None,
            input_handle: Value::Null,
            items: vec![serde_json::json!({"value": 1})],
            cancellation_requested: false,
            cancellation_reason: None,
            cancellation_metadata: None,
            scheduled_at: occurred_at,
            available_at: occurred_at,
            started_at: None,
            lease_expires_at: None,
            completed_at: None,
            updated_at: occurred_at,
        };
        let group_state = LocalGroupState {
            identity: identity.clone(),
            group_id: 1,
            level: 0,
            parent_group_id: None,
            status: WorkflowBulkBatchStatus::Completed.as_str().to_owned(),
            succeeded_items: 2,
            failed_items: 0,
            cancelled_items: 0,
            error: None,
            terminal_at: occurred_at,
        };
        state.db.put(batch_key(&identity), serde_json::to_vec(&batch_state)?)?;
        state
            .db
            .put(chunk_key(&identity, &chunk_state.chunk_id), serde_json::to_vec(&chunk_state)?)?;
        state.db.put(
            batch_chunk_index_key(&identity, &chunk_state.chunk_id),
            serde_json::to_vec(&BatchChunkIndexEntry {
                identity: identity.clone(),
                chunk_id: chunk_state.chunk_id.clone(),
            })?,
        )?;
        state.db.put(
            ready_index_key(&chunk_state),
            serde_json::to_vec(&ReadyChunkIndexEntry {
                identity: identity.clone(),
                chunk_id: chunk_state.chunk_id.clone(),
                group_id: chunk_state.group_id,
            })?,
        )?;
        state
            .db
            .put(group_key(&identity, group_state.group_id), serde_json::to_vec(&group_state)?)?;
        state.db.put(
            batch_group_index_key(&identity, group_state.group_id),
            serde_json::to_vec(&BatchGroupIndexEntry {
                identity: identity.clone(),
                group_id: group_state.group_id,
            })?,
        )?;
        drop(state);

        let migrated = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;

        let batch_snapshot =
            migrated.batch_snapshot(&identity)?.expect("legacy json batch state should load");
        assert_eq!(batch_snapshot.status, WorkflowBulkBatchStatus::Running.as_str());
        assert_eq!(batch_snapshot.task_queue, "bulk");

        let chunks = migrated.load_chunks_for_batch(&identity)?;
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].chunk_id, "chunk-a");
        assert_eq!(chunks[0].status, WorkflowBulkChunkStatus::Scheduled.as_str());

        let groups = migrated.load_groups_for_batch(&identity)?;
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].group_id, 1);

        let ready = migrated.load_ready_chunk_entries("tenant-a", "bulk", occurred_at)?;
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].chunk_id, "chunk-a");

        assert!(migrated.db.iterator(IteratorMode::Start).next().is_none());
        let debug = migrated.debug_snapshot()?;
        assert!(debug.legacy_default_cf_entries_migrated >= 6);
        assert!(debug.last_legacy_default_cf_migration_at.is_some());

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_replays_and_restores_from_checkpoint() -> Result<()> {
        let db_path = temp_path("throughput-state-db");
        let checkpoint_dir = temp_path("throughput-state-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let batch_identity = identity();

        state.apply_changelog_entry(
            0,
            1,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: batch_identity.clone(),
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 1,
                total_items: 3,
                chunk_count: 1,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            2,
            &entry(ThroughputChangelogPayload::ChunkLeased {
                identity: batch_identity.clone(),
                chunk_id: "chunk-1".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 3,
                max_attempts: 2,
                retry_delay_ms: 500,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-a".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                lease_expires_at: Utc::now(),
            }),
        )?;
        state.apply_changelog_entry(
            0,
            3,
            &entry(ThroughputChangelogPayload::ChunkApplied {
                identity: batch_identity.clone(),
                chunk_id: "chunk-1".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 3,
                max_attempts: 2,
                retry_delay_ms: 500,
                lease_epoch: 1,
                owner_epoch: 1,
                report_id: "report-1".to_owned(),
                status: "completed".to_owned(),
                available_at: Utc::now(),
                result_handle: Some(Value::Null),
                output: None,
                error: None,
                cancellation_reason: None,
                cancellation_metadata: None,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            4,
            &entry(ThroughputChangelogPayload::BatchTerminal {
                identity: batch_identity.clone(),
                status: "completed".to_owned(),
                report_id: "report-1".to_owned(),
                succeeded_items: 3,
                failed_items: 0,
                cancelled_items: 0,
                error: None,
                terminal_at: Utc::now(),
            }),
        )?;

        let snapshot = state.debug_snapshot()?;
        assert_eq!(snapshot.batch_count, 1);
        assert_eq!(snapshot.chunk_count, 1);
        assert_eq!(snapshot.last_applied_offsets.get(&0), Some(&4));

        let _checkpoint = state.write_checkpoint()?;

        let restored_db_path = temp_path("throughput-state-restored-db");
        let restored = LocalThroughputState::open(&restored_db_path, &checkpoint_dir, 3)?;
        assert!(restored.restore_from_latest_checkpoint_if_empty()?);
        let restored_snapshot = restored.debug_snapshot()?;
        assert!(restored_snapshot.restored_from_checkpoint);
        assert_eq!(restored_snapshot.batch_count, 1);
        assert_eq!(restored_snapshot.chunk_count, 1);
        assert_eq!(restored_snapshot.last_applied_offsets.get(&0), Some(&4));

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&restored_db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_rebuilds_running_batch_progress_from_store_chunks() -> Result<()> {
        let db_path = temp_path("throughput-state-store-progress-db");
        let checkpoint_dir = temp_path("throughput-state-store-progress-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let scheduled_at = Utc::now();
        let batch = store_batch(WorkflowBulkBatchStatus::Scheduled, 2, 2, 1, scheduled_at);
        let completed_at = scheduled_at + chrono::Duration::seconds(1);
        let lease_expires_at = scheduled_at + chrono::Duration::seconds(30);
        let mut completed =
            store_chunk(&batch, "chunk-a", 0, WorkflowBulkChunkStatus::Completed, 1, completed_at);
        completed.completed_at = Some(completed_at);
        completed.started_at = Some(scheduled_at);
        completed.last_report_id = Some("report-a".to_owned());
        let mut started = store_chunk(
            &batch,
            "chunk-b",
            1,
            WorkflowBulkChunkStatus::Started,
            1,
            scheduled_at + chrono::Duration::seconds(2),
        );
        started.attempt = 2;
        started.lease_epoch = 3;
        started.owner_epoch = 4;
        started.worker_id = Some("worker-a".to_owned());
        started.lease_token = Some(Uuid::now_v7());
        started.started_at = Some(scheduled_at + chrono::Duration::seconds(2));
        started.lease_expires_at = Some(lease_expires_at);

        state.upsert_batch_with_chunks(&batch, &[completed, started.clone()])?;

        let snapshot = state.batch_snapshot(&identity())?.expect("store-seeded batch should exist");
        assert_eq!(snapshot.status, "running");
        assert_eq!(snapshot.succeeded_items, 1);
        assert_eq!(snapshot.failed_items, 0);
        assert_eq!(snapshot.cancelled_items, 0);
        assert!(snapshot.terminal_at.is_none());

        let started_chunk = state
            .load_chunk_state(&identity(), "chunk-b")?
            .expect("started chunk should be restored from store");
        assert_eq!(started_chunk.status, "started");
        assert_eq!(started_chunk.attempt, 2);
        assert_eq!(started_chunk.lease_epoch, 3);
        assert_eq!(started_chunk.owner_epoch, 4);
        assert_eq!(started_chunk.worker_id.as_deref(), Some("worker-a"));
        assert_eq!(
            started_chunk.lease_token.as_deref(),
            started.lease_token.map(|value| value.to_string()).as_deref()
        );
        assert_eq!(started_chunk.lease_expires_at, Some(lease_expires_at));

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_rebuilds_terminal_batch_from_store_chunks() -> Result<()> {
        let db_path = temp_path("throughput-state-store-terminal-db");
        let checkpoint_dir = temp_path("throughput-state-store-terminal-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let scheduled_at = Utc::now();
        let batch = store_batch(WorkflowBulkBatchStatus::Running, 1, 1, 1, scheduled_at);
        let completed_at = scheduled_at + chrono::Duration::seconds(1);
        let mut completed =
            store_chunk(&batch, "chunk-a", 0, WorkflowBulkChunkStatus::Completed, 1, completed_at);
        completed.completed_at = Some(completed_at);
        completed.started_at = Some(scheduled_at);
        completed.last_report_id = Some("report-a".to_owned());

        state.upsert_batch_with_chunks(&batch, &[completed])?;

        let snapshot = state
            .batch_snapshot(&identity())?
            .expect("terminal batch should be derived from store chunks");
        assert_eq!(snapshot.status, "completed");
        assert_eq!(snapshot.succeeded_items, 1);
        assert_eq!(snapshot.failed_items, 0);
        assert_eq!(snapshot.cancelled_items, 0);
        assert_eq!(snapshot.terminal_at, Some(completed_at));

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_lists_expired_started_chunks() -> Result<()> {
        let db_path = temp_path("throughput-state-expired-db");
        let checkpoint_dir = temp_path("throughput-state-expired-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let batch_identity = identity();
        let expired_at = Utc::now() - chrono::Duration::seconds(1);

        state.apply_changelog_entry(
            0,
            1,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: batch_identity.clone(),
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 1,
                total_items: 1,
                chunk_count: 1,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            2,
            &entry(ThroughputChangelogPayload::ChunkLeased {
                identity: batch_identity.clone(),
                chunk_id: "chunk-expired".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-a".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                lease_expires_at: expired_at,
            }),
        )?;

        let expired = state.expired_started_chunks(Utc::now(), 10, None, 1)?;
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].identity.batch_id, "batch-a");
        assert_eq!(expired[0].chunk_id, "chunk-expired");

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_keeps_future_ready_chunks_indexed_until_due() -> Result<()> {
        let db_path = temp_path("throughput-state-future-ready-db");
        let checkpoint_dir = temp_path("throughput-state-future-ready-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let batch_identity = identity();
        let now = Utc::now();
        let future_at = now + chrono::Duration::seconds(5);

        state.apply_changelog_entry(
            0,
            1,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: batch_identity.clone(),
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 1,
                total_items: 1,
                chunk_count: 1,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            2,
            &entry(ThroughputChangelogPayload::ChunkLeased {
                identity: batch_identity.clone(),
                chunk_id: "chunk-future".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-a".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                lease_expires_at: now + chrono::Duration::seconds(30),
            }),
        )?;
        state.apply_changelog_entry(
            0,
            3,
            &entry(ThroughputChangelogPayload::ChunkRequeued {
                identity: batch_identity.clone(),
                chunk_id: "chunk-future".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                available_at: future_at,
            }),
        )?;

        assert!(!state.has_ready_chunk("tenant-a", "bulk", now, None, 1)?);
        assert!(state.has_ready_chunk(
            "tenant-a",
            "bulk",
            future_at + chrono::Duration::milliseconds(1),
            None,
            1,
        )?);

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_rejects_stale_report() -> Result<()> {
        let db_path = temp_path("throughput-state-report-db");
        let checkpoint_dir = temp_path("throughput-state-report-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let batch_identity = identity();

        state.apply_changelog_entry(
            0,
            1,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: batch_identity.clone(),
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 1,
                total_items: 1,
                chunk_count: 1,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            2,
            &entry(ThroughputChangelogPayload::ChunkLeased {
                identity: batch_identity.clone(),
                chunk_id: "chunk-report".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 2,
                retry_delay_ms: 250,
                lease_epoch: 2,
                owner_epoch: 1,
                worker_id: "worker-a".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                lease_expires_at: Utc::now() + chrono::Duration::seconds(30),
            }),
        )?;

        assert_eq!(
            validate_report_result(
                &state,
                &batch_identity,
                "chunk-report",
                1,
                1,
                TEST_LEASE_TOKEN,
                1,
                "report-stale",
            )?,
            ReportValidation::RejectLeaseEpochMismatch
        );
        assert_eq!(
            validate_report_result(
                &state,
                &batch_identity,
                "chunk-report",
                1,
                2,
                TEST_LEASE_TOKEN,
                1,
                "report-ok",
            )?,
            ReportValidation::Accept
        );
        assert_eq!(
            validate_report_result(
                &state,
                &batch_identity,
                "chunk-report",
                1,
                2,
                "lease-token-stale",
                1,
                "report-lease-token-mismatch",
            )?,
            ReportValidation::RejectLeaseTokenMismatch
        );

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_restored_checkpoint_rejects_duplicate_report_after_restore() -> Result<()> {
        let db_path = temp_path("throughput-state-duplicate-report-db");
        let checkpoint_dir = temp_path("throughput-state-duplicate-report-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let batch_identity = identity();

        state.apply_changelog_entry(
            0,
            1,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: batch_identity.clone(),
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 1,
                total_items: 2,
                chunk_count: 2,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            2,
            &entry(ThroughputChangelogPayload::ChunkLeased {
                identity: batch_identity.clone(),
                chunk_id: "chunk-report".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 2,
                retry_delay_ms: 250,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-a".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                lease_expires_at: Utc::now() + chrono::Duration::seconds(30),
            }),
        )?;
        state.apply_changelog_entry(
            0,
            3,
            &entry(ThroughputChangelogPayload::ChunkApplied {
                identity: batch_identity.clone(),
                chunk_id: "chunk-report".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 2,
                retry_delay_ms: 250,
                lease_epoch: 1,
                owner_epoch: 1,
                report_id: "report-1".to_owned(),
                status: "completed".to_owned(),
                available_at: Utc::now(),
                result_handle: Some(Value::Null),
                output: None,
                error: None,
                cancellation_reason: None,
                cancellation_metadata: None,
            }),
        )?;
        let _checkpoint = state.write_checkpoint()?;

        let restored_db_path = temp_path("throughput-state-duplicate-report-restored-db");
        let restored = LocalThroughputState::open(&restored_db_path, &checkpoint_dir, 3)?;
        assert!(restored.restore_from_latest_checkpoint_if_empty()?);
        assert_eq!(
            validate_report_result(
                &restored,
                &batch_identity,
                "chunk-report",
                1,
                1,
                TEST_LEASE_TOKEN,
                1,
                "report-1",
            )?,
            ReportValidation::RejectAlreadyApplied
        );

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&restored_db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_restored_checkpoint_rejects_old_owner_after_handoff() -> Result<()> {
        let db_path = temp_path("throughput-state-owner-handoff-db");
        let checkpoint_dir = temp_path("throughput-state-owner-handoff-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let batch_identity = identity();
        let lease_expires_at = Utc::now() + chrono::Duration::seconds(30);

        state.apply_changelog_entry(
            0,
            1,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: batch_identity.clone(),
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 1,
                total_items: 1,
                chunk_count: 1,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            2,
            &entry(ThroughputChangelogPayload::ChunkLeased {
                identity: batch_identity.clone(),
                chunk_id: "chunk-report".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 2,
                retry_delay_ms: 250,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-a".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                lease_expires_at,
            }),
        )?;
        let _checkpoint = state.write_checkpoint()?;

        let restored_db_path = temp_path("throughput-state-owner-handoff-restored-db");
        let restored = LocalThroughputState::open(&restored_db_path, &checkpoint_dir, 3)?;
        assert!(restored.restore_from_latest_checkpoint_if_empty()?);
        restored.apply_changelog_entry(
            0,
            3,
            &entry(ThroughputChangelogPayload::ChunkLeased {
                identity: batch_identity.clone(),
                chunk_id: "chunk-report".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 2,
                retry_delay_ms: 250,
                lease_epoch: 1,
                owner_epoch: 2,
                worker_id: "worker-b".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                lease_expires_at,
            }),
        )?;

        assert_eq!(
            validate_report_result(
                &restored,
                &batch_identity,
                "chunk-report",
                1,
                1,
                TEST_LEASE_TOKEN,
                1,
                "report-stale-owner",
            )?,
            ReportValidation::RejectOwnerEpochMismatch
        );
        assert_eq!(
            validate_report_result(
                &restored,
                &batch_identity,
                "chunk-report",
                1,
                1,
                TEST_LEASE_TOKEN,
                2,
                "report-current-owner",
            )?,
            ReportValidation::Accept
        );

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&restored_db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_leases_ready_chunk_once() -> Result<()> {
        let db_path = temp_path("throughput-state-lease-db");
        let checkpoint_dir = temp_path("throughput-state-lease-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let batch_identity = identity();
        let now = Utc::now();

        state.apply_changelog_entry(
            0,
            1,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: batch_identity.clone(),
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 1,
                total_items: 1,
                chunk_count: 1,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            2,
            &entry(ThroughputChangelogPayload::ChunkApplied {
                identity: batch_identity.clone(),
                chunk_id: "chunk-ready".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 0,
                owner_epoch: 1,
                report_id: "seed-ready".to_owned(),
                status: "scheduled".to_owned(),
                available_at: now,
                result_handle: Some(Value::Null),
                output: None,
                error: None,
                cancellation_reason: None,
                cancellation_metadata: None,
            }),
        )?;

        let first = lease_one_ready_chunk(
            &state,
            &batch_identity.tenant_id,
            "bulk",
            "worker-a",
            now,
            chrono::Duration::seconds(30),
            None,
            None,
            1,
        )?;
        assert!(first.is_some());
        let first = first.expect("first lease should succeed");
        assert_eq!(first.lease_epoch, 1);
        assert!(first.lease_token.is_some());

        let second = lease_one_ready_chunk(
            &state,
            &batch_identity.tenant_id,
            "bulk",
            "worker-b",
            now,
            chrono::Duration::seconds(30),
            None,
            None,
            1,
        )?;
        assert!(second.is_none());

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_tracks_retry_schedule_and_batch_counters() -> Result<()> {
        let db_path = temp_path("throughput-state-counters-db");
        let checkpoint_dir = temp_path("throughput-state-counters-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let batch_identity = identity();
        let retry_at = Utc::now() + chrono::Duration::seconds(5);
        let terminal_at = retry_at + chrono::Duration::seconds(10);

        state.apply_changelog_entry(
            0,
            1,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: batch_identity.clone(),
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 1,
                total_items: 3,
                chunk_count: 1,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            2,
            &entry(ThroughputChangelogPayload::ChunkLeased {
                identity: batch_identity.clone(),
                chunk_id: "chunk-retry".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 3,
                max_attempts: 2,
                retry_delay_ms: 5_000,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-a".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                lease_expires_at: Utc::now() + chrono::Duration::seconds(30),
            }),
        )?;
        state.apply_changelog_entry(
            0,
            3,
            &entry(ThroughputChangelogPayload::ChunkApplied {
                identity: batch_identity.clone(),
                chunk_id: "chunk-retry".to_owned(),
                chunk_index: 0,
                attempt: 2,
                group_id: 0,
                item_count: 3,
                max_attempts: 2,
                retry_delay_ms: 5_000,
                lease_epoch: 1,
                owner_epoch: 1,
                report_id: "report-retry".to_owned(),
                status: "scheduled".to_owned(),
                available_at: retry_at,
                result_handle: Some(Value::Null),
                output: None,
                error: Some("boom".to_owned()),
                cancellation_reason: None,
                cancellation_metadata: None,
            }),
        )?;

        let retried_chunk = state
            .load_chunk_state(&batch_identity, "chunk-retry")?
            .expect("retried chunk should exist");
        assert_eq!(retried_chunk.status, "scheduled");
        assert_eq!(retried_chunk.attempt, 2);
        assert_eq!(retried_chunk.item_count, 3);
        assert_eq!(retried_chunk.max_attempts, 2);
        assert_eq!(retried_chunk.retry_delay_ms, 5_000);
        assert_eq!(retried_chunk.available_at, retry_at);
        assert_eq!(retried_chunk.error.as_deref(), Some("boom"));

        let running_batch = state.load_batch_state(&batch_identity)?.expect("batch should exist");
        assert_eq!(running_batch.status, "running");
        assert_eq!(running_batch.total_items, 3);
        assert_eq!(running_batch.succeeded_items, 0);
        assert_eq!(running_batch.failed_items, 0);
        assert_eq!(running_batch.cancelled_items, 0);

        state.apply_changelog_entry(
            0,
            4,
            &entry(ThroughputChangelogPayload::ChunkLeased {
                identity: batch_identity.clone(),
                chunk_id: "chunk-retry".to_owned(),
                chunk_index: 0,
                attempt: 2,
                group_id: 0,
                item_count: 3,
                max_attempts: 2,
                retry_delay_ms: 5_000,
                lease_epoch: 2,
                owner_epoch: 1,
                worker_id: "worker-a".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                lease_expires_at: retry_at + chrono::Duration::seconds(30),
            }),
        )?;
        state.apply_changelog_entry(
            0,
            5,
            &entry(ThroughputChangelogPayload::ChunkApplied {
                identity: batch_identity.clone(),
                chunk_id: "chunk-retry".to_owned(),
                chunk_index: 0,
                attempt: 2,
                group_id: 0,
                item_count: 3,
                max_attempts: 2,
                retry_delay_ms: 5_000,
                lease_epoch: 2,
                owner_epoch: 1,
                report_id: "report-complete".to_owned(),
                status: "completed".to_owned(),
                available_at: retry_at,
                result_handle: Some(Value::Null),
                output: None,
                error: None,
                cancellation_reason: None,
                cancellation_metadata: None,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            6,
            &entry(ThroughputChangelogPayload::BatchTerminal {
                identity: batch_identity.clone(),
                status: "completed".to_owned(),
                report_id: "report-complete".to_owned(),
                succeeded_items: 3,
                failed_items: 0,
                cancelled_items: 0,
                error: None,
                terminal_at,
            }),
        )?;

        let completed_batch =
            state.load_batch_state(&batch_identity)?.expect("completed batch should exist");
        assert_eq!(completed_batch.status, "completed");
        assert_eq!(completed_batch.succeeded_items, 3);
        assert_eq!(completed_batch.failed_items, 0);
        assert_eq!(completed_batch.cancelled_items, 0);
        assert_eq!(completed_batch.terminal_at, Some(terminal_at));

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn lease_keeps_success_output_for_numeric_stream_reducer() -> Result<()> {
        let db_path = temp_path("throughput-state-sum-lease-db");
        let checkpoint_dir = temp_path("throughput-state-sum-lease-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let now = Utc::now();
        let mut batch = store_batch(WorkflowBulkBatchStatus::Scheduled, 1, 1, 1, now);
        batch.reducer = Some("sum".to_owned());
        batch.reducer_kind = bulk_reducer_name(Some("sum")).to_owned();
        batch.reducer_version = throughput_reducer_version(Some("sum")).to_owned();
        batch.reducer_execution_path =
            throughput_reducer_execution_path("stream-v2", Some("eager"), Some("sum")).to_owned();
        state.upsert_batch_record(&batch)?;
        state.upsert_chunk_record(&store_chunk(
            &batch,
            "chunk-sum",
            0,
            WorkflowBulkChunkStatus::Scheduled,
            1,
            now,
        ))?;

        let leased = lease_one_ready_chunk(
            &state,
            &batch.tenant_id,
            &batch.task_queue,
            "worker-a",
            now,
            chrono::Duration::seconds(30),
            None,
            None,
            1,
        )?
        .expect("sum reducer chunk should lease");
        assert!(!leased.omit_success_output);

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn reduce_batch_success_outputs_after_report_includes_pending_chunk_output() -> Result<()> {
        let db_path = temp_path("throughput-state-sum-output-db");
        let checkpoint_dir = temp_path("throughput-state-sum-output-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let now = Utc::now();
        let mut batch = store_batch(WorkflowBulkBatchStatus::Running, 2, 2, 2, now);
        batch.reducer = Some("sum".to_owned());
        batch.reducer_kind = bulk_reducer_name(Some("sum")).to_owned();
        batch.reducer_version = throughput_reducer_version(Some("sum")).to_owned();
        batch.reducer_execution_path =
            throughput_reducer_execution_path("stream-v2", Some("eager"), Some("sum")).to_owned();
        state.upsert_batch_record(&batch)?;

        let mut completed =
            store_chunk(&batch, "chunk-completed", 0, WorkflowBulkChunkStatus::Completed, 1, now);
        completed.output = Some(vec![json!(10)]);
        completed.completed_at = Some(now);
        state.upsert_chunk_record(&completed)?;

        let mut started =
            store_chunk(&batch, "chunk-pending", 1, WorkflowBulkChunkStatus::Started, 1, now);
        started.started_at = Some(now);
        started.lease_token = Some(Uuid::now_v7());
        state.upsert_chunk_record(&started)?;

        let reduced = state.reduce_batch_success_outputs_after_report(
            &ThroughputBatchIdentity {
                tenant_id: batch.tenant_id.clone(),
                instance_id: batch.instance_id.clone(),
                run_id: batch.run_id.clone(),
                batch_id: batch.batch_id.clone(),
            },
            Some(&ThroughputChunkReport {
                report_id: "report-sum".to_owned(),
                tenant_id: batch.tenant_id.clone(),
                instance_id: batch.instance_id.clone(),
                run_id: batch.run_id.clone(),
                batch_id: batch.batch_id.clone(),
                chunk_id: "chunk-pending".to_owned(),
                chunk_index: 1,
                group_id: group_id_for_chunk_index(1, batch.aggregation_group_count),
                attempt: 1,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-a".to_owned(),
                worker_build_id: "build-a".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                occurred_at: now,
                payload: ThroughputChunkReportPayload::ChunkCompleted {
                    result_handle: Value::Null,
                    output: Some(vec![json!(30)]),
                },
            }),
        )?;
        assert_eq!(reduced, Some(json!(40.0)));

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn reduce_batch_success_outputs_after_report_reduces_histogram_outputs() -> Result<()> {
        let db_path = temp_path("throughput-state-histogram-output-db");
        let checkpoint_dir = temp_path("throughput-state-histogram-output-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let now = Utc::now();
        let mut batch = store_batch(WorkflowBulkBatchStatus::Running, 3, 3, 3, now);
        batch.reducer = Some("histogram".to_owned());
        batch.reducer_kind = bulk_reducer_name(Some("histogram")).to_owned();
        batch.reducer_version = throughput_reducer_version(Some("histogram")).to_owned();
        batch.reducer_execution_path =
            throughput_reducer_execution_path("stream-v2", Some("eager"), Some("histogram"))
                .to_owned();
        state.upsert_batch_record(&batch)?;

        let mut completed =
            store_chunk(&batch, "chunk-completed", 0, WorkflowBulkChunkStatus::Completed, 2, now);
        completed.output = Some(vec![json!("alpha"), json!("beta")]);
        completed.completed_at = Some(now);
        state.upsert_chunk_record(&completed)?;

        let mut started =
            store_chunk(&batch, "chunk-pending", 1, WorkflowBulkChunkStatus::Started, 1, now);
        started.started_at = Some(now);
        started.lease_token = Some(Uuid::now_v7());
        state.upsert_chunk_record(&started)?;

        let reduced = state.reduce_batch_success_outputs_after_report(
            &ThroughputBatchIdentity {
                tenant_id: batch.tenant_id.clone(),
                instance_id: batch.instance_id.clone(),
                run_id: batch.run_id.clone(),
                batch_id: batch.batch_id.clone(),
            },
            Some(&ThroughputChunkReport {
                report_id: "report-histogram".to_owned(),
                tenant_id: batch.tenant_id.clone(),
                instance_id: batch.instance_id.clone(),
                run_id: batch.run_id.clone(),
                batch_id: batch.batch_id.clone(),
                chunk_id: "chunk-pending".to_owned(),
                chunk_index: 1,
                group_id: group_id_for_chunk_index(1, batch.aggregation_group_count),
                attempt: 1,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-a".to_owned(),
                worker_build_id: "build-a".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                occurred_at: now,
                payload: ThroughputChunkReportPayload::ChunkCompleted {
                    result_handle: Value::Null,
                    output: Some(vec![json!("alpha")]),
                },
            }),
        )?;
        assert_eq!(reduced, Some(json!({ "alpha": 2, "beta": 1 })));

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn reduce_batch_success_outputs_after_report_reduces_sampled_errors() -> Result<()> {
        let db_path = temp_path("throughput-state-sample-errors-db");
        let checkpoint_dir = temp_path("throughput-state-sample-errors-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let now = Utc::now();
        let mut batch = store_batch(WorkflowBulkBatchStatus::Running, 4, 4, 4, now);
        batch.reducer = Some("sample_errors".to_owned());
        batch.reducer_kind = bulk_reducer_name(Some("sample_errors")).to_owned();
        batch.reducer_version = throughput_reducer_version(Some("sample_errors")).to_owned();
        batch.reducer_execution_path =
            throughput_reducer_execution_path("stream-v2", Some("eager"), Some("sample_errors"))
                .to_owned();
        state.upsert_batch_record(&batch)?;

        let mut failed =
            store_chunk(&batch, "chunk-failed", 0, WorkflowBulkChunkStatus::Failed, 2, now);
        failed.error = Some("boom".to_owned());
        failed.completed_at = Some(now);
        state.upsert_chunk_record(&failed)?;

        let mut cancelled =
            store_chunk(&batch, "chunk-cancelled", 1, WorkflowBulkChunkStatus::Cancelled, 1, now);
        cancelled.error = Some("cancelled".to_owned());
        cancelled.completed_at = Some(now);
        state.upsert_chunk_record(&cancelled)?;

        let mut started =
            store_chunk(&batch, "chunk-pending", 2, WorkflowBulkChunkStatus::Started, 1, now);
        started.started_at = Some(now);
        started.lease_token = Some(Uuid::now_v7());
        state.upsert_chunk_record(&started)?;

        let reduced = state.reduce_batch_success_outputs_after_report(
            &ThroughputBatchIdentity {
                tenant_id: batch.tenant_id.clone(),
                instance_id: batch.instance_id.clone(),
                run_id: batch.run_id.clone(),
                batch_id: batch.batch_id.clone(),
            },
            Some(&ThroughputChunkReport {
                report_id: "report-sample-errors".to_owned(),
                tenant_id: batch.tenant_id.clone(),
                instance_id: batch.instance_id.clone(),
                run_id: batch.run_id.clone(),
                batch_id: batch.batch_id.clone(),
                chunk_id: "chunk-pending".to_owned(),
                chunk_index: 2,
                group_id: group_id_for_chunk_index(2, batch.aggregation_group_count),
                attempt: 1,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-a".to_owned(),
                worker_build_id: "build-a".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                occurred_at: now,
                payload: ThroughputChunkReportPayload::ChunkFailed { error: "timeout".to_owned() },
            }),
        )?;
        assert_eq!(
            reduced,
            Some(json!({
                "sample": ["boom", "cancelled", "timeout"],
                "total": 4,
                "truncated": false
            }))
        );

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_dedupes_mirrored_entries_when_consumer_catches_up() -> Result<()> {
        let db_path = temp_path("throughput-state-mirror-dedupe-db");
        let checkpoint_dir = temp_path("throughput-state-mirror-dedupe-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let batch_identity = identity();
        let created = entry(ThroughputChangelogPayload::BatchCreated {
            identity: batch_identity.clone(),
            task_queue: "bulk".to_owned(),
            execution_policy: Some("eager".to_owned()),
            reducer: Some("count".to_owned()),
            reducer_class: "mergeable".to_owned(),
            aggregation_tree_depth: 2,
            fast_lane_enabled: true,
            aggregation_group_count: 3,
            total_items: 100,
            chunk_count: 1,
        });
        let leased = entry(ThroughputChangelogPayload::ChunkLeased {
            identity: batch_identity.clone(),
            chunk_id: "chunk-a".to_owned(),
            chunk_index: 0,
            attempt: 1,
            group_id: 0,
            item_count: 100,
            max_attempts: 1,
            retry_delay_ms: 0,
            lease_epoch: 1,
            owner_epoch: 1,
            worker_id: "worker-a".to_owned(),
            lease_token: TEST_LEASE_TOKEN.to_owned(),
            lease_expires_at: Utc::now() + chrono::Duration::seconds(30),
        });
        let applied = entry(ThroughputChangelogPayload::ChunkApplied {
            identity: batch_identity.clone(),
            chunk_id: "chunk-a".to_owned(),
            chunk_index: 0,
            attempt: 1,
            group_id: 0,
            item_count: 100,
            max_attempts: 1,
            retry_delay_ms: 0,
            lease_epoch: 1,
            owner_epoch: 1,
            report_id: "report-a".to_owned(),
            status: "completed".to_owned(),
            available_at: Utc::now(),
            result_handle: Some(Value::Null),
            output: None,
            error: None,
            cancellation_reason: None,
            cancellation_metadata: None,
        });

        state.mirror_changelog_entry(&created)?;
        state.mirror_changelog_entry(&leased)?;
        state.mirror_changelog_entry(&applied)?;

        let mirrored_batch =
            state.load_batch_state(&batch_identity)?.expect("mirrored batch should exist");
        assert_eq!(mirrored_batch.succeeded_items, 100);
        let mirrored_chunk = state
            .load_chunk_state(&batch_identity, "chunk-a")?
            .expect("mirrored chunk should exist");
        assert_eq!(mirrored_chunk.status, "completed");

        assert!(state.apply_changelog_entry(0, 10, &created)?);
        assert!(state.apply_changelog_entry(0, 11, &leased)?);
        assert!(state.apply_changelog_entry(0, 12, &applied)?);

        let replayed_batch =
            state.load_batch_state(&batch_identity)?.expect("replayed batch should exist");
        assert_eq!(replayed_batch.status, "running");
        assert_eq!(replayed_batch.succeeded_items, 100);
        let replayed_chunk = state
            .load_chunk_state(&batch_identity, "chunk-a")?
            .expect("replayed chunk should exist");
        assert_eq!(replayed_chunk.status, "completed");
        assert_eq!(state.load_mirrored_entry_ids()?, Vec::<String>::new());

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_restores_pending_mirror_markers_from_checkpoint() -> Result<()> {
        let db_path = temp_path("throughput-state-mirror-checkpoint-db");
        let checkpoint_dir = temp_path("throughput-state-mirror-checkpoint-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let batch_identity = identity();
        let created = entry(ThroughputChangelogPayload::BatchCreated {
            identity: batch_identity.clone(),
            task_queue: "bulk".to_owned(),
            execution_policy: Some("eager".to_owned()),
            reducer: Some("count".to_owned()),
            reducer_class: "mergeable".to_owned(),
            aggregation_tree_depth: 2,
            fast_lane_enabled: true,
            aggregation_group_count: 1,
            total_items: 1,
            chunk_count: 0,
        });

        state.mirror_changelog_entry(&created)?;
        let _checkpoint = state.write_checkpoint()?;

        let restored_db_path = temp_path("throughput-state-mirror-checkpoint-restored-db");
        let restored = LocalThroughputState::open(&restored_db_path, &checkpoint_dir, 3)?;
        assert!(restored.restore_from_latest_checkpoint_if_empty()?);
        assert_eq!(restored.load_mirrored_entry_ids()?, vec![created.entry_id.to_string()]);
        assert!(restored.apply_changelog_entry(0, 1, &created)?);
        assert_eq!(restored.load_mirrored_entry_ids()?, Vec::<String>::new());

        let batch = restored
            .load_batch_state(&batch_identity)?
            .expect("batch should exist after mirrored restore");
        assert_eq!(batch.total_items, 1);

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&restored_db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_projects_report_apply_before_store_round_trip() -> Result<()> {
        let db_path = temp_path("throughput-state-project-db");
        let checkpoint_dir = temp_path("throughput-state-project-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let aggregation = AggregationCoordinator::new(state.clone());
        let batch_identity = identity();

        state.apply_changelog_entry(
            0,
            1,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: batch_identity.clone(),
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 1,
                total_items: 2,
                chunk_count: 1,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            2,
            &entry(ThroughputChangelogPayload::ChunkLeased {
                identity: batch_identity,
                chunk_id: "chunk-a".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 2,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-a".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                lease_expires_at: Utc::now() + chrono::Duration::seconds(30),
            }),
        )?;

        let completion_report = report(ThroughputChunkReportPayload::ChunkCompleted {
            result_handle: Value::Null,
            output: Some(vec![]),
        });
        let projected = project_report_apply(&state, &aggregation, &completion_report)?
            .expect("projection should succeed");
        assert_eq!(projected.batch_total_items, 2);
        assert_eq!(projected.chunk_status, "completed");
        assert_eq!(projected.batch_status, "completed");
        assert_eq!(projected.batch_succeeded_items, 2);
        assert!(projected.batch_terminal);

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_replays_chunk_requeue() -> Result<()> {
        let db_path = temp_path("throughput-state-requeue-db");
        let checkpoint_dir = temp_path("throughput-state-requeue-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let batch_identity = identity();
        let requeue_at = Utc::now() + chrono::Duration::seconds(15);

        state.apply_changelog_entry(
            0,
            1,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: batch_identity.clone(),
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 1,
                total_items: 1,
                chunk_count: 1,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            2,
            &entry(ThroughputChangelogPayload::ChunkLeased {
                identity: batch_identity.clone(),
                chunk_id: "chunk-requeue".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 2,
                retry_delay_ms: 250,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-a".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                lease_expires_at: Utc::now() + chrono::Duration::seconds(30),
            }),
        )?;
        state.apply_changelog_entry(
            0,
            3,
            &entry(ThroughputChangelogPayload::ChunkRequeued {
                identity: batch_identity.clone(),
                chunk_id: "chunk-requeue".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 2,
                retry_delay_ms: 250,
                lease_epoch: 1,
                owner_epoch: 1,
                available_at: requeue_at,
            }),
        )?;

        let chunk = state
            .load_chunk_state(&batch_identity, "chunk-requeue")?
            .expect("requeued chunk should exist");
        assert_eq!(chunk.status, "scheduled");
        assert_eq!(chunk.available_at, requeue_at);
        assert_eq!(chunk.lease_expires_at, None);
        assert_eq!(chunk.started_at.is_some(), true);

        let projected = state.project_requeue(&batch_identity, "chunk-requeue", Utc::now())?;
        assert!(projected.is_none());

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_reports_partition_catch_up_from_observed_high_watermark() -> Result<()> {
        let db_path = temp_path("throughput-state-catchup-db");
        let checkpoint_dir = temp_path("throughput-state-catchup-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;

        state.record_observed_high_watermark(2, 5);
        assert!(!state.partition_is_caught_up(2)?);

        let batch_identity = identity();
        state.apply_changelog_entry(
            2,
            4,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: batch_identity,
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 1,
                total_items: 1,
                chunk_count: 1,
            }),
        )?;
        assert!(state.partition_is_caught_up(2)?);

        let snapshot = state.debug_snapshot()?;
        assert_eq!(snapshot.observed_high_watermarks.get(&2), Some(&5));
        assert_eq!(snapshot.partition_lag.get(&2), Some(&0));

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_tracks_streams_changelog_offsets_separately() -> Result<()> {
        let db_path = temp_path("throughput-state-streams-changelog-db");
        let checkpoint_dir = temp_path("throughput-state-streams-changelog-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;

        let throughput_identity = identity();
        state.apply_changelog_entry(
            2,
            4,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: throughput_identity,
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 1,
                total_items: 1,
                chunk_count: 0,
            }),
        )?;

        let streams_entry = streams_entry(StreamsChangelogPayload::StreamJobViewUpdated {
            handle_id: "handle-a".to_owned(),
            job_id: "stream-job-a".to_owned(),
            view_name: "accountTotals".to_owned(),
            logical_key: "acct-1".to_owned(),
            output: json!({ "total": 42 }),
            checkpoint_sequence: 9,
            updated_at: Utc::now(),
        });
        state.mirror_streams_changelog_entry(&streams_entry)?;
        assert_eq!(
            state.load_mirrored_entry_ids_for_plane(LocalChangelogPlane::Streams)?,
            vec![streams_entry.entry_id.to_string()]
        );
        assert!(state.apply_streams_changelog_entry(7, 12, &streams_entry)?);
        assert_eq!(
            state.load_mirrored_entry_ids_for_plane(LocalChangelogPlane::Streams)?,
            Vec::<String>::new()
        );
        assert_eq!(
            state.last_applied_offset_for_plane(LocalChangelogPlane::Throughput, 2)?,
            Some(4)
        );
        assert_eq!(state.last_applied_offset_for_plane(LocalChangelogPlane::Streams, 7)?, Some(12));

        let checkpoint_path = state.write_checkpoint()?;
        assert!(checkpoint_path.exists());

        let restored_db_path = temp_path("throughput-state-streams-changelog-restored-db");
        let restored = LocalThroughputState::open(&restored_db_path, &checkpoint_dir, 3)?;
        assert!(restored.restore_from_latest_checkpoint_if_empty()?);
        assert_eq!(
            restored.last_applied_offset_for_plane(LocalChangelogPlane::Throughput, 2)?,
            Some(4)
        );
        assert_eq!(
            restored.last_applied_offset_for_plane(LocalChangelogPlane::Streams, 7)?,
            Some(12)
        );

        let snapshot = restored.debug_snapshot()?;
        assert_eq!(snapshot.last_applied_offsets.get(&2), Some(&4));
        assert_eq!(snapshot.streams_last_applied_offsets.get(&7), Some(&12));

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&restored_db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_defers_grouped_batch_terminal_until_all_groups_finish() -> Result<()> {
        let db_path = temp_path("throughput-state-groups-db");
        let checkpoint_dir = temp_path("throughput-state-groups-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let aggregation = AggregationCoordinator::new(state.clone());
        let batch_identity = identity();
        let first_done_at = Utc::now();
        let second_done_at = first_done_at + chrono::Duration::seconds(1);

        state.apply_changelog_entry(
            0,
            1,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: batch_identity.clone(),
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 2,
                total_items: 2,
                chunk_count: 2,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            2,
            &entry(ThroughputChangelogPayload::ChunkLeased {
                identity: batch_identity.clone(),
                chunk_id: "chunk-g0".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-a".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                lease_expires_at: first_done_at + chrono::Duration::seconds(30),
            }),
        )?;
        state.apply_changelog_entry(
            1,
            1,
            &entry(ThroughputChangelogPayload::ChunkLeased {
                identity: batch_identity.clone(),
                chunk_id: "chunk-g1".to_owned(),
                chunk_index: 1,
                attempt: 1,
                group_id: 1,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-b".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                lease_expires_at: second_done_at + chrono::Duration::seconds(30),
            }),
        )?;

        let first_report = ThroughputChunkReport {
            report_id: "report-g0".to_owned(),
            tenant_id: batch_identity.tenant_id.clone(),
            instance_id: batch_identity.instance_id.clone(),
            run_id: batch_identity.run_id.clone(),
            batch_id: batch_identity.batch_id.clone(),
            chunk_id: "chunk-g0".to_owned(),
            chunk_index: 0,
            group_id: 0,
            attempt: 1,
            lease_epoch: 1,
            owner_epoch: 1,
            worker_id: "worker-a".to_owned(),
            worker_build_id: "build-a".to_owned(),
            lease_token: TEST_LEASE_TOKEN.to_owned(),
            occurred_at: first_done_at,
            payload: ThroughputChunkReportPayload::ChunkFailed { error: "boom".to_owned() },
        };
        let first_projection = project_report_apply(&state, &aggregation, &first_report)?
            .expect("grouped chunk projection should succeed");
        assert!(first_projection.batch_terminal_deferred);
        assert!(!first_projection.batch_terminal);
        assert_eq!(first_projection.batch_status, "running");
        assert_eq!(
            first_projection
                .group_terminal
                .as_ref()
                .expect("first grouped report should project group terminal")
                .status,
            "failed"
        );
        assert_eq!(
            first_projection
                .grouped_batch_terminal
                .as_ref()
                .expect("first grouped report should project grouped batch terminal")
                .status,
            "failed"
        );

        state.apply_changelog_entry(
            0,
            3,
            &entry(ThroughputChangelogPayload::ChunkApplied {
                identity: batch_identity.clone(),
                chunk_id: "chunk-g0".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                report_id: "report-g0".to_owned(),
                status: "failed".to_owned(),
                available_at: first_done_at,
                result_handle: Some(Value::Null),
                output: None,
                error: Some("boom".to_owned()),
                cancellation_reason: None,
                cancellation_metadata: None,
            }),
        )?;
        let first_group_terminal = aggregation
            .project_group_terminal(&batch_identity, 0, first_done_at)?
            .expect("first group should terminalize");
        state.apply_changelog_entry(
            0,
            4,
            &entry(ThroughputChangelogPayload::GroupTerminal {
                identity: batch_identity.clone(),
                group_id: 0,
                level: first_group_terminal.level,
                parent_group_id: first_group_terminal.parent_group_id,
                status: first_group_terminal.status,
                succeeded_items: first_group_terminal.succeeded_items,
                failed_items: first_group_terminal.failed_items,
                cancelled_items: first_group_terminal.cancelled_items,
                error: first_group_terminal.error,
                terminal_at: first_group_terminal.terminal_at,
            }),
        )?;
        let early_terminal = aggregation
            .project_batch_terminal_from_groups(&batch_identity, first_done_at)?
            .expect("first permanent group failure should resolve parent barrier");
        assert_eq!(early_terminal.status, "failed");
        assert_eq!(early_terminal.failed_items, 1);

        state.apply_changelog_entry(
            1,
            3,
            &entry(ThroughputChangelogPayload::ChunkApplied {
                identity: batch_identity.clone(),
                chunk_id: "chunk-g1".to_owned(),
                chunk_index: 1,
                attempt: 1,
                group_id: 1,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                report_id: "report-g1".to_owned(),
                status: "completed".to_owned(),
                available_at: second_done_at,
                result_handle: Some(Value::Null),
                output: None,
                error: None,
                cancellation_reason: None,
                cancellation_metadata: None,
            }),
        )?;
        let second_group_terminal = aggregation
            .project_group_terminal(&batch_identity, 1, second_done_at)?
            .expect("second group should terminalize");
        state.apply_changelog_entry(
            1,
            4,
            &entry(ThroughputChangelogPayload::GroupTerminal {
                identity: batch_identity.clone(),
                group_id: 1,
                level: second_group_terminal.level,
                parent_group_id: second_group_terminal.parent_group_id,
                status: second_group_terminal.status,
                succeeded_items: second_group_terminal.succeeded_items,
                failed_items: second_group_terminal.failed_items,
                cancelled_items: second_group_terminal.cancelled_items,
                error: second_group_terminal.error,
                terminal_at: second_group_terminal.terminal_at,
            }),
        )?;

        let terminal = aggregation
            .project_batch_terminal_from_groups(&batch_identity, second_done_at)?
            .expect("group barrier should resolve once all groups finish");
        assert_eq!(terminal.status, "failed");
        assert_eq!(terminal.succeeded_items, 1);
        assert_eq!(terminal.failed_items, 1);
        assert_eq!(terminal.cancelled_items, 0);

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_projects_group_terminal_from_pending_chunk_apply() -> Result<()> {
        let db_path = temp_path("throughput-state-pending-group-terminal-db");
        let checkpoint_dir = temp_path("throughput-state-pending-group-terminal-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let batch_identity = identity();
        let occurred_at = Utc::now();

        state.apply_changelog_entry(
            0,
            1,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: batch_identity.clone(),
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 2,
                total_items: 1,
                chunk_count: 1,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            2,
            &entry(ThroughputChangelogPayload::ChunkLeased {
                identity: batch_identity.clone(),
                chunk_id: "chunk-g0".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-a".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                lease_expires_at: occurred_at + chrono::Duration::seconds(30),
            }),
        )?;

        assert!(state.project_group_terminal(&batch_identity, 0, occurred_at)?.is_none());
        let projected = project_report_apply(
            &state,
            &AggregationCoordinator::new(state.clone()),
            &ThroughputChunkReport {
                report_id: "report-g0".to_owned(),
                tenant_id: batch_identity.tenant_id.clone(),
                instance_id: batch_identity.instance_id.clone(),
                run_id: batch_identity.run_id.clone(),
                batch_id: batch_identity.batch_id.clone(),
                chunk_id: "chunk-g0".to_owned(),
                chunk_index: 0,
                group_id: 0,
                attempt: 1,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-a".to_owned(),
                worker_build_id: "build-a".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                occurred_at,
                payload: ThroughputChunkReportPayload::ChunkFailed { error: "boom".to_owned() },
            },
        )?
        .and_then(|apply| apply.group_terminal)
        .expect("pending chunk apply should terminalize the group");
        assert_eq!(projected.status, "failed");
        assert_eq!(projected.failed_items, 1);
        assert_eq!(projected.error.as_deref(), Some("boom"));

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_projects_grouped_batch_terminal_with_pending_group() -> Result<()> {
        let db_path = temp_path("throughput-state-pending-batch-terminal-db");
        let checkpoint_dir = temp_path("throughput-state-pending-batch-terminal-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let batch_identity = identity();
        let first_done_at = Utc::now();
        let second_done_at = first_done_at + chrono::Duration::seconds(1);

        state.apply_changelog_entry(
            0,
            1,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: batch_identity.clone(),
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 2,
                total_items: 2,
                chunk_count: 2,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            2,
            &entry(ThroughputChangelogPayload::ChunkLeased {
                identity: batch_identity.clone(),
                chunk_id: "chunk-g0".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-a".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                lease_expires_at: first_done_at + chrono::Duration::seconds(30),
            }),
        )?;
        state.apply_changelog_entry(
            1,
            1,
            &entry(ThroughputChangelogPayload::ChunkLeased {
                identity: batch_identity.clone(),
                chunk_id: "chunk-g1".to_owned(),
                chunk_index: 1,
                attempt: 1,
                group_id: 1,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-b".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                lease_expires_at: second_done_at + chrono::Duration::seconds(30),
            }),
        )?;
        state.apply_changelog_entry(
            0,
            3,
            &entry(ThroughputChangelogPayload::ChunkApplied {
                identity: batch_identity.clone(),
                chunk_id: "chunk-g0".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                report_id: "report-g0".to_owned(),
                status: "completed".to_owned(),
                available_at: first_done_at,
                result_handle: Some(Value::Null),
                output: None,
                error: None,
                cancellation_reason: None,
                cancellation_metadata: None,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            4,
            &entry(ThroughputChangelogPayload::GroupTerminal {
                identity: batch_identity.clone(),
                group_id: 0,
                level: 0,
                parent_group_id: Some(reduction_tree_node_id(1, 0)),
                status: "completed".to_owned(),
                succeeded_items: 1,
                failed_items: 0,
                cancelled_items: 0,
                error: None,
                terminal_at: first_done_at,
            }),
        )?;

        let pending_apply = project_report_apply(
            &state,
            &AggregationCoordinator::new(state.clone()),
            &ThroughputChunkReport {
                report_id: "report-g1".to_owned(),
                tenant_id: batch_identity.tenant_id.clone(),
                instance_id: batch_identity.instance_id.clone(),
                run_id: batch_identity.run_id.clone(),
                batch_id: batch_identity.batch_id.clone(),
                chunk_id: "chunk-g1".to_owned(),
                chunk_index: 1,
                group_id: 1,
                attempt: 1,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-b".to_owned(),
                worker_build_id: "build-b".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                occurred_at: second_done_at,
                payload: ThroughputChunkReportPayload::ChunkCompleted {
                    output: None,
                    result_handle: Value::Null,
                },
            },
        )?
        .expect("pending second group should project");
        let _pending_group =
            pending_apply.group_terminal.clone().expect("pending second group should terminalize");
        let summary = pending_apply
            .grouped_batch_summary
            .expect("pending group should contribute to grouped summary");
        assert_eq!(summary.succeeded_items, 2);
        assert_eq!(summary.failed_items, 0);
        assert_eq!(summary.cancelled_items, 0);

        let terminal = pending_apply
            .grouped_batch_terminal
            .expect("pending second group should complete the batch");
        assert_eq!(terminal.status, "completed");
        assert_eq!(terminal.succeeded_items, 2);
        assert_eq!(terminal.failed_items, 0);
        assert_eq!(terminal.cancelled_items, 0);

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_projects_grouped_batch_terminal_from_chunk_state_without_group_markers()
    -> Result<()> {
        let db_path = temp_path("throughput-state-derived-groups-db");
        let checkpoint_dir = temp_path("throughput-state-derived-groups-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let aggregation = AggregationCoordinator::new(state.clone());
        let batch_identity = identity();
        let occurred_at = Utc::now();

        state.apply_changelog_entry(
            0,
            1,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: batch_identity.clone(),
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 2,
                total_items: 2,
                chunk_count: 2,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            2,
            &entry(ThroughputChangelogPayload::ChunkApplied {
                identity: batch_identity.clone(),
                chunk_id: "chunk-g0".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                report_id: "report-g0".to_owned(),
                status: "completed".to_owned(),
                available_at: occurred_at,
                result_handle: Some(Value::Null),
                output: None,
                error: None,
                cancellation_reason: None,
                cancellation_metadata: None,
            }),
        )?;
        state.apply_changelog_entry(
            1,
            1,
            &entry(ThroughputChangelogPayload::ChunkApplied {
                identity: batch_identity.clone(),
                chunk_id: "chunk-g1".to_owned(),
                chunk_index: 1,
                attempt: 1,
                group_id: 1,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                report_id: "report-g1".to_owned(),
                status: "failed".to_owned(),
                available_at: occurred_at,
                result_handle: Some(Value::Null),
                output: None,
                error: Some("boom".to_owned()),
                cancellation_reason: None,
                cancellation_metadata: None,
            }),
        )?;

        let terminal = aggregation
            .project_batch_terminal_from_groups(&batch_identity, occurred_at)?
            .expect("terminal chunk state should derive grouped batch terminal");
        assert_eq!(terminal.status, "failed");
        assert_eq!(terminal.succeeded_items, 1);
        assert_eq!(terminal.failed_items, 1);
        assert_eq!(terminal.cancelled_items, 0);

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_prunes_terminal_batches_after_retention_cutoff() -> Result<()> {
        let db_path = temp_path("throughput-state-gc-db");
        let checkpoint_dir = temp_path("throughput-state-gc-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let batch_identity = identity();
        let terminal_at = Utc::now() - chrono::Duration::hours(2);

        state.apply_changelog_entry(
            0,
            1,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: batch_identity.clone(),
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 2,
                total_items: 2,
                chunk_count: 2,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            2,
            &entry(ThroughputChangelogPayload::ChunkApplied {
                identity: batch_identity.clone(),
                chunk_id: "chunk-g0".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                report_id: "report-g0".to_owned(),
                status: "completed".to_owned(),
                available_at: terminal_at,
                result_handle: Some(Value::Null),
                output: None,
                error: None,
                cancellation_reason: None,
                cancellation_metadata: None,
            }),
        )?;
        state.apply_changelog_entry(
            1,
            1,
            &entry(ThroughputChangelogPayload::GroupTerminal {
                identity: batch_identity.clone(),
                group_id: 0,
                level: 0,
                parent_group_id: Some(reduction_tree_node_id(1, 0)),
                status: "completed".to_owned(),
                succeeded_items: 1,
                failed_items: 0,
                cancelled_items: 0,
                error: None,
                terminal_at,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            3,
            &entry(ThroughputChangelogPayload::BatchTerminal {
                identity: batch_identity.clone(),
                status: "completed".to_owned(),
                report_id: "report-terminal".to_owned(),
                succeeded_items: 1,
                failed_items: 0,
                cancelled_items: 0,
                error: None,
                terminal_at,
            }),
        )?;

        let deleted = state.prune_terminal_state(Utc::now() - chrono::Duration::hours(1))?;
        assert_eq!(deleted, 3);
        assert!(state.load_batch_state(&batch_identity)?.is_none());
        assert!(state.load_chunk_state(&batch_identity, "chunk-g0")?.is_none());
        assert!(state.load_group_state(&batch_identity, 0)?.is_none());

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_projects_group_terminal_once() -> Result<()> {
        let db_path = temp_path("throughput-state-group-terminal-db");
        let checkpoint_dir = temp_path("throughput-state-group-terminal-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let aggregation = AggregationCoordinator::new(state.clone());
        let batch_identity = identity();
        let occurred_at = Utc::now();

        state.apply_changelog_entry(
            0,
            1,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: batch_identity.clone(),
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 2,
                total_items: 2,
                chunk_count: 2,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            2,
            &entry(ThroughputChangelogPayload::ChunkApplied {
                identity: batch_identity.clone(),
                chunk_id: "chunk-g0".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                report_id: "report-g0".to_owned(),
                status: "failed".to_owned(),
                available_at: occurred_at,
                result_handle: Some(Value::Null),
                output: None,
                error: Some("boom".to_owned()),
                cancellation_reason: None,
                cancellation_metadata: None,
            }),
        )?;

        let projected = aggregation
            .project_group_terminal(&batch_identity, 0, occurred_at)?
            .expect("group should terminalize after first permanent failure");
        assert_eq!(projected.status, "failed");
        assert_eq!(projected.failed_items, 1);

        state.apply_changelog_entry(
            0,
            3,
            &entry(ThroughputChangelogPayload::GroupTerminal {
                identity: batch_identity.clone(),
                group_id: 0,
                level: 0,
                parent_group_id: Some(reduction_tree_node_id(1, 0)),
                status: "failed".to_owned(),
                succeeded_items: 0,
                failed_items: 1,
                cancelled_items: 0,
                error: Some("boom".to_owned()),
                terminal_at: occurred_at,
            }),
        )?;
        assert!(aggregation.project_group_terminal(&batch_identity, 0, occurred_at)?.is_none());

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_infers_ungrouped_completed_batch_terminal_from_chunk_apply() -> Result<()> {
        let db_path = temp_path("throughput-state-ungrouped-complete-db");
        let checkpoint_dir = temp_path("throughput-state-ungrouped-complete-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let batch_identity = identity();
        let occurred_at = Utc::now();

        state.apply_changelog_entry(
            0,
            1,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: batch_identity.clone(),
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 1,
                total_items: 1,
                chunk_count: 1,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            2,
            &entry(ThroughputChangelogPayload::ChunkLeased {
                identity: batch_identity.clone(),
                chunk_id: "chunk-a".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-a".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                lease_expires_at: occurred_at + chrono::Duration::seconds(30),
            }),
        )?;
        state.apply_changelog_entry(
            0,
            3,
            &entry(ThroughputChangelogPayload::ChunkApplied {
                identity: batch_identity.clone(),
                chunk_id: "chunk-a".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                report_id: "report-a".to_owned(),
                status: "completed".to_owned(),
                available_at: occurred_at,
                result_handle: Some(Value::Null),
                output: None,
                error: None,
                cancellation_reason: None,
                cancellation_metadata: None,
            }),
        )?;

        let batch = state
            .load_batch_state(&batch_identity)?
            .expect("ungrouped batch should be terminalized from chunk apply");
        assert_eq!(batch.status, "completed");
        assert_eq!(batch.succeeded_items, 1);
        assert_eq!(batch.last_report_id.as_deref(), Some("report-a"));
        assert!(batch.terminal_at.is_some());
        assert!(state.project_group_terminal(&batch_identity, 0, occurred_at)?.is_none());

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_infers_ungrouped_failed_batch_terminal_from_chunk_apply() -> Result<()> {
        let db_path = temp_path("throughput-state-ungrouped-failed-db");
        let checkpoint_dir = temp_path("throughput-state-ungrouped-failed-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        let batch_identity = identity();
        let occurred_at = Utc::now();

        state.apply_changelog_entry(
            0,
            1,
            &entry(ThroughputChangelogPayload::BatchCreated {
                identity: batch_identity.clone(),
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 1,
                total_items: 2,
                chunk_count: 2,
            }),
        )?;
        state.apply_changelog_entry(
            0,
            2,
            &entry(ThroughputChangelogPayload::ChunkLeased {
                identity: batch_identity.clone(),
                chunk_id: "chunk-a".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: "worker-a".to_owned(),
                lease_token: TEST_LEASE_TOKEN.to_owned(),
                lease_expires_at: occurred_at + chrono::Duration::seconds(30),
            }),
        )?;
        state.apply_changelog_entry(
            0,
            3,
            &entry(ThroughputChangelogPayload::ChunkApplied {
                identity: batch_identity.clone(),
                chunk_id: "chunk-a".to_owned(),
                chunk_index: 0,
                attempt: 1,
                group_id: 0,
                item_count: 1,
                max_attempts: 1,
                retry_delay_ms: 0,
                lease_epoch: 1,
                owner_epoch: 1,
                report_id: "report-a".to_owned(),
                status: "failed".to_owned(),
                available_at: occurred_at,
                result_handle: Some(Value::Null),
                output: None,
                error: Some("boom".to_owned()),
                cancellation_reason: None,
                cancellation_metadata: None,
            }),
        )?;

        let batch = state
            .load_batch_state(&batch_identity)?
            .expect("ungrouped failed batch should be terminalized from chunk apply");
        assert_eq!(batch.status, "failed");
        assert_eq!(batch.failed_items, 1);
        assert_eq!(batch.error.as_deref(), Some("boom"));
        assert_eq!(batch.last_report_id.as_deref(), Some("report-a"));
        assert!(batch.terminal_at.is_some());
        assert!(state.project_group_terminal(&batch_identity, 0, occurred_at)?.is_none());

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[test]
    fn local_state_derives_parent_group_terminals_for_three_level_tree() {
        let batch = LocalBatchState {
            identity: identity(),
            definition_id: "demo".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-a".to_owned()),
            task_queue: "bulk".to_owned(),
            activity_capabilities: None,
            execution_policy: Some("eager".to_owned()),
            reducer: Some("count".to_owned()),
            reducer_class: "mergeable".to_owned(),
            aggregation_tree_depth: 3,
            fast_lane_enabled: true,
            aggregation_group_count: 16,
            total_items: 16,
            chunk_count: 16,
            terminal_chunk_count: 15,
            succeeded_items: 15,
            failed_items: 0,
            cancelled_items: 0,
            status: "running".to_owned(),
            last_report_id: None,
            error: None,
            reducer_output: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            terminal_at: None,
        };
        let occurred_at = Utc::now();
        let mut groups = (0..15)
            .map(|group_id| LocalGroupState {
                identity: batch.identity.clone(),
                group_id,
                level: 0,
                parent_group_id: reduction_tree_parent_group_id(
                    group_id,
                    batch.aggregation_group_count,
                    batch.aggregation_tree_depth,
                ),
                status: "completed".to_owned(),
                succeeded_items: 1,
                failed_items: 0,
                cancelled_items: 0,
                error: None,
                terminal_at: occurred_at,
            })
            .collect::<Vec<_>>();
        for slot in 0..3 {
            groups.push(LocalGroupState {
                identity: batch.identity.clone(),
                group_id: reduction_tree_node_id(1, slot),
                level: 1,
                parent_group_id: Some(reduction_tree_node_id(2, 0)),
                status: "completed".to_owned(),
                succeeded_items: 4,
                failed_items: 0,
                cancelled_items: 0,
                error: None,
                terminal_at: occurred_at,
            });
        }

        let pending_leaf = ProjectedGroupTerminal {
            group_id: 15,
            level: 0,
            parent_group_id: Some(reduction_tree_node_id(1, 3)),
            status: "completed".to_owned(),
            succeeded_items: 1,
            failed_items: 0,
            cancelled_items: 0,
            error: None,
            terminal_at: occurred_at,
        };

        let derived = aggregation_state::project_parent_group_terminals_from_loaded(
            &batch,
            &groups,
            Some(&pending_leaf),
            occurred_at,
        );
        assert_eq!(derived.len(), 2);
        assert_eq!(derived[0].group_id, reduction_tree_node_id(1, 3));
        assert_eq!(derived[0].level, 1);
        assert_eq!(derived[0].succeeded_items, 4);
        assert_eq!(derived[1].group_id, reduction_tree_node_id(2, 0));
        assert_eq!(derived[1].level, 2);
        assert_eq!(derived[1].succeeded_items, 16);
    }
}
