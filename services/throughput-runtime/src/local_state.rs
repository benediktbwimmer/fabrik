use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use fabrik_store::{
    WorkflowBulkBatchRecord, WorkflowBulkBatchStatus, WorkflowBulkChunkRecord,
    WorkflowBulkChunkStatus,
};
use fabrik_throughput::{
    ThroughputBatchIdentity, ThroughputChangelogEntry, ThroughputChangelogPayload,
    ThroughputChunkReport, ThroughputChunkReportPayload, reduction_tree_child_group_ids,
    reduction_tree_level_counts, reduction_tree_node_id, reduction_tree_node_level,
    reduction_tree_parent_group_id,
};
use rocksdb::{DB, IteratorMode, Options, WriteBatch};
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
const LATEST_CHECKPOINT_FILE: &str = "latest.json";

fn encode_rocksdb_value<T: Serialize>(value: &T, subject: &str) -> Result<Vec<u8>> {
    serde_json::to_vec(value).with_context(|| format!("failed to serialize {subject}"))
}

fn decode_rocksdb_value<T: DeserializeOwned>(bytes: &[u8], subject: &str) -> Result<T> {
    serde_json::from_slice(bytes).with_context(|| format!("failed to deserialize {subject}"))
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
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct LeaseSelectionDebug {
    pub indexed_candidates: u64,
    pub owned_candidates: u64,
    pub missing_batch: u64,
    pub terminal_batch: u64,
    pub batch_failed_or_cancelled: u64,
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
    observed_high_watermarks: BTreeMap<i32, i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CheckpointFile {
    created_at: DateTime<Utc>,
    offsets: BTreeMap<i32, i64>,
    #[serde(default)]
    mirrored_entry_ids: Vec<String>,
    batches: Vec<LocalBatchState>,
    chunks: Vec<LocalChunkState>,
    groups: Vec<LocalGroupState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalBatchState {
    identity: ThroughputBatchIdentity,
    definition_id: String,
    definition_version: Option<u32>,
    artifact_hash: Option<String>,
    task_queue: String,
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
struct LocalChunkState {
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
    serde_json::from_slice(bytes).context("failed to deserialize throughput chunk state")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalGroupState {
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
struct BatchChunkIndexEntry {
    identity: ThroughputBatchIdentity,
    chunk_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchGroupIndexEntry {
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
pub struct ReadyChunkCandidate {
    pub identity: ThroughputBatchIdentity,
    pub chunk_id: String,
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
    pub task_queue: String,
    pub chunk_index: u32,
    pub group_id: u32,
    pub attempt: u32,
    pub item_count: u32,
    pub max_attempts: u32,
    pub retry_delay_ms: u64,
    pub items: Vec<Value>,
    pub input_handle: Value,
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
pub enum PreparedReportApply {
    MissingBatch,
    Rejected(ReportValidation),
    Accepted(ProjectedTerminalApply),
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

impl LocalThroughputState {
    pub fn open(
        db_path: impl Into<PathBuf>,
        checkpoint_dir: impl Into<PathBuf>,
        checkpoint_retention: usize,
    ) -> Result<Self> {
        let db_path = db_path.into();
        let checkpoint_dir = checkpoint_dir.into();
        fs::create_dir_all(&db_path).with_context(|| {
            format!("failed to create throughput state dir {}", db_path.display())
        })?;
        fs::create_dir_all(&checkpoint_dir).with_context(|| {
            format!("failed to create throughput checkpoint dir {}", checkpoint_dir.display())
        })?;
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, &db_path)
            .with_context(|| format!("failed to open throughput state db {}", db_path.display()))?;
        Ok(Self {
            db: Arc::new(db),
            db_path,
            checkpoint_dir,
            checkpoint_retention: checkpoint_retention.max(1),
            meta: Arc::new(Mutex::new(LocalStateMeta::default())),
            lease_lock: Arc::new(Mutex::new(())),
        })
    }

    pub fn restore_from_latest_checkpoint_if_empty(&self) -> Result<bool> {
        if !self.is_empty()? {
            return Ok(false);
        }
        let latest_path = self.checkpoint_dir.join(LATEST_CHECKPOINT_FILE);
        if !latest_path.exists() {
            return Ok(false);
        }
        let bytes = fs::read(&latest_path).with_context(|| {
            format!("failed to read throughput checkpoint {}", latest_path.display())
        })?;
        let checkpoint: CheckpointFile = serde_json::from_slice(&bytes)
            .context("failed to deserialize throughput checkpoint")?;
        self.restore_from_checkpoint_if_empty(checkpoint)
    }

    pub fn next_start_offsets(&self, partitions: &[i32]) -> Result<HashMap<i32, i64>> {
        let mut offsets = HashMap::new();
        for partition in partitions {
            if let Some(offset) = self.last_applied_offset(*partition)? {
                offsets.insert(*partition, offset.saturating_add(1));
            }
        }
        Ok(offsets)
    }

    pub fn record_observed_high_watermark(&self, partition_id: i32, high_watermark: i64) {
        let mut meta = self.meta.lock().expect("local throughput meta lock poisoned");
        let entry = meta.observed_high_watermarks.entry(partition_id).or_insert(high_watermark);
        if high_watermark > *entry {
            *entry = high_watermark;
        }
    }

    pub fn apply_changelog_entry(
        &self,
        partition_id: i32,
        offset: i64,
        entry: &ThroughputChangelogEntry,
    ) -> Result<bool> {
        if self.last_applied_offset(partition_id)?.is_some_and(|current| current >= offset) {
            return Ok(false);
        }

        let mut write_batch = WriteBatch::default();
        if self.is_mirrored_entry_pending(entry.entry_id)? {
            write_batch.delete(mirrored_entry_key(entry.entry_id).as_bytes());
        } else {
            self.write_entry_payload(&mut write_batch, entry)?;
        }
        write_batch.put(offset_key(partition_id), offset.to_string().as_bytes());
        self.db
            .write(write_batch)
            .context("failed to apply throughput changelog entry to local state")?;
        let mut meta = self.meta.lock().expect("local throughput meta lock poisoned");
        meta.changelog_entries_applied = meta.changelog_entries_applied.saturating_add(1);
        meta.last_changelog_apply_at = Some(entry.occurred_at);
        Ok(true)
    }

    pub fn mirror_changelog_entry(&self, entry: &ThroughputChangelogEntry) -> Result<()> {
        self.mirror_changelog_records(std::slice::from_ref(entry))
    }

    pub fn mark_changelog_entry_pending(&self, entry: &ThroughputChangelogEntry) -> Result<()> {
        self.mark_changelog_entries_pending(std::slice::from_ref(entry))
    }

    pub fn mark_changelog_entries_pending(
        &self,
        entries: &[ThroughputChangelogEntry],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut write_batch = WriteBatch::default();
        for entry in entries {
            write_batch.put(mirrored_entry_key(entry.entry_id), b"1");
        }
        self.db
            .write(write_batch)
            .context("failed to mark throughput changelog entries pending")?;
        Ok(())
    }

    pub fn mirror_changelog_records(&self, entries: &[ThroughputChangelogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut write_batch = WriteBatch::default();
        for entry in entries {
            self.write_entry_payload(&mut write_batch, entry)?;
            write_batch.put(mirrored_entry_key(entry.entry_id), b"1");
        }
        self.db
            .write(write_batch)
            .context("failed to mirror throughput changelog entries into local state")?;
        Ok(())
    }

    pub fn upsert_batch_record(&self, batch: &WorkflowBulkBatchRecord) -> Result<()> {
        let state = LocalBatchState {
            identity: ThroughputBatchIdentity {
                tenant_id: batch.tenant_id.clone(),
                instance_id: batch.instance_id.clone(),
                run_id: batch.run_id.clone(),
                batch_id: batch.batch_id.clone(),
            },
            definition_id: batch.definition_id.clone(),
            definition_version: batch.definition_version,
            artifact_hash: batch.artifact_hash.clone(),
            task_queue: batch.task_queue.clone(),
            execution_policy: batch.execution_policy.clone(),
            reducer: batch.reducer.clone(),
            reducer_class: batch.reducer_class.clone(),
            aggregation_tree_depth: batch.aggregation_tree_depth.max(1),
            fast_lane_enabled: batch.fast_lane_enabled,
            aggregation_group_count: batch.aggregation_group_count.max(1),
            total_items: batch.total_items,
            chunk_count: batch.chunk_count,
            terminal_chunk_count: batch.terminal_at.map(|_| batch.chunk_count).unwrap_or_default(),
            succeeded_items: batch.succeeded_items,
            failed_items: batch.failed_items,
            cancelled_items: batch.cancelled_items,
            status: batch.status.as_str().to_owned(),
            last_report_id: None,
            error: batch.error.clone(),
            created_at: batch.scheduled_at,
            updated_at: batch.updated_at,
            terminal_at: batch.terminal_at,
        };
        self.db
            .put(batch_key(&state.identity), encode_rocksdb_value(&state, "direct batch state")?)
            .context("failed to upsert direct throughput batch state")?;
        Ok(())
    }

    pub fn upsert_batch_with_chunks(
        &self,
        batch: &WorkflowBulkBatchRecord,
        chunks: &[WorkflowBulkChunkRecord],
    ) -> Result<()> {
        let chunk_states = chunks
            .iter()
            .map(|chunk| LocalChunkState {
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
            })
            .collect::<Vec<_>>();
        let batch_state = derive_batch_state_from_chunk_states(batch, &chunk_states);
        let mut write_batch = WriteBatch::default();
        write_batch.put(
            batch_key(&batch_state.identity),
            encode_rocksdb_value(&batch_state, "batched direct throughput batch state")?,
        );
        for chunk_state in &chunk_states {
            let existing = self.load_chunk_state(&chunk_state.identity, &chunk_state.chunk_id)?;
            self.write_chunk_state(&mut write_batch, existing.as_ref(), chunk_state)?;
        }
        self.db
            .write(write_batch)
            .context("failed to batched-upsert direct throughput batch/chunk state")?;
        Ok(())
    }

    pub fn upsert_chunk_record(&self, chunk: &WorkflowBulkChunkRecord) -> Result<()> {
        let identity = ThroughputBatchIdentity {
            tenant_id: chunk.tenant_id.clone(),
            instance_id: chunk.instance_id.clone(),
            run_id: chunk.run_id.clone(),
            batch_id: chunk.batch_id.clone(),
        };
        let existing = self.load_chunk_state(&identity, &chunk.chunk_id)?;
        let state = LocalChunkState {
            identity,
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
        };
        let mut write_batch = WriteBatch::default();
        self.write_chunk_state(&mut write_batch, existing.as_ref(), &state)?;
        self.db.write(write_batch).context("failed to upsert direct throughput chunk state")?;
        Ok(())
    }

    pub fn batch_snapshot(
        &self,
        identity: &ThroughputBatchIdentity,
    ) -> Result<Option<LocalBatchSnapshot>> {
        self.load_batch_state(identity).map(|batch| {
            batch.map(|batch| LocalBatchSnapshot {
                identity: batch.identity,
                definition_id: batch.definition_id,
                definition_version: batch.definition_version,
                artifact_hash: batch.artifact_hash,
                task_queue: batch.task_queue,
                execution_policy: batch.execution_policy,
                reducer: batch.reducer,
                reducer_class: batch.reducer_class,
                aggregation_tree_depth: batch.aggregation_tree_depth,
                fast_lane_enabled: batch.fast_lane_enabled,
                total_items: batch.total_items,
                chunk_count: batch.chunk_count,
                succeeded_items: batch.succeeded_items,
                failed_items: batch.failed_items,
                cancelled_items: batch.cancelled_items,
                status: batch.status,
                error: batch.error,
                updated_at: batch.updated_at,
                terminal_at: batch.terminal_at,
            })
        })
    }

    pub fn mark_batch_running(
        &self,
        identity: &ThroughputBatchIdentity,
        occurred_at: DateTime<Utc>,
    ) -> Result<Option<LocalBatchSnapshot>> {
        let Some(mut batch) = self.load_batch_state(identity)? else {
            return Ok(None);
        };
        if batch.status != WorkflowBulkBatchStatus::Scheduled.as_str() {
            return Ok(None);
        }
        batch.status = WorkflowBulkBatchStatus::Running.as_str().to_owned();
        batch.updated_at = occurred_at;
        self.db
            .put(batch_key(identity), encode_rocksdb_value(&batch, "running batch state")?)
            .context("failed to mark throughput batch running")?;
        Ok(Some(LocalBatchSnapshot {
            identity: batch.identity,
            definition_id: batch.definition_id,
            definition_version: batch.definition_version,
            artifact_hash: batch.artifact_hash,
            task_queue: batch.task_queue,
            execution_policy: batch.execution_policy,
            reducer: batch.reducer,
            reducer_class: batch.reducer_class,
            aggregation_tree_depth: batch.aggregation_tree_depth,
            fast_lane_enabled: batch.fast_lane_enabled,
            total_items: batch.total_items,
            chunk_count: batch.chunk_count,
            succeeded_items: batch.succeeded_items,
            failed_items: batch.failed_items,
            cancelled_items: batch.cancelled_items,
            status: batch.status,
            error: batch.error,
            updated_at: batch.updated_at,
            terminal_at: batch.terminal_at,
        }))
    }

    pub fn project_batch_group_summary(
        &self,
        identity: &ThroughputBatchIdentity,
    ) -> Result<Option<ProjectedBatchGroupSummary>> {
        self.project_batch_group_summary_with_pending_group(identity, None)
    }

    pub fn project_batch_group_summary_with_pending_group(
        &self,
        identity: &ThroughputBatchIdentity,
        pending_group: Option<&ProjectedGroupTerminal>,
    ) -> Result<Option<ProjectedBatchGroupSummary>> {
        let groups = self.load_projected_groups_for_batch(identity, pending_group)?;
        Ok(summarize_projected_groups(&groups))
    }

    pub fn chunk_snapshots_for_batch(
        &self,
        identity: &ThroughputBatchIdentity,
    ) -> Result<Vec<LocalChunkSnapshot>> {
        self.load_chunks_for_batch(identity).map(|chunks| {
            chunks
                .into_iter()
                .map(|chunk| LocalChunkSnapshot {
                    chunk_id: chunk.chunk_id,
                    status: chunk.status,
                    attempt: chunk.attempt,
                    lease_epoch: chunk.lease_epoch,
                    owner_epoch: chunk.owner_epoch,
                    result_handle: chunk.result_handle,
                    output: chunk.output,
                    error: chunk.error,
                    cancellation_requested: chunk.cancellation_requested,
                    cancellation_reason: chunk.cancellation_reason,
                    cancellation_metadata: chunk.cancellation_metadata,
                    worker_id: chunk.worker_id,
                    lease_token: chunk.lease_token,
                    report_id: chunk.report_id,
                    available_at: chunk.available_at,
                    started_at: chunk.started_at,
                    lease_expires_at: chunk.lease_expires_at,
                    completed_at: chunk.completed_at,
                    updated_at: chunk.updated_at,
                })
                .collect()
        })
    }

    pub fn count_started_chunks(
        &self,
        tenant_id: Option<&str>,
        task_queue: Option<&str>,
        batch_id: Option<&str>,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
    ) -> Result<u64> {
        let mut count = 0_u64;
        for chunk in self.load_started_chunk_entries(tenant_id, task_queue)? {
            if batch_id.is_some_and(|value| chunk.identity.batch_id != value) {
                continue;
            }
            if owned_partitions
                .map(|partitions| {
                    partitions.contains(&throughput_partition_id(
                        &chunk.identity.batch_id,
                        chunk.group_id,
                        partition_count,
                    ))
                })
                .unwrap_or(false)
                || owned_partitions.is_none()
            {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    pub fn next_ready_chunk(
        &self,
        tenant_id: &str,
        task_queue: &str,
        now: DateTime<Utc>,
        max_active_chunks_per_batch: Option<usize>,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
    ) -> Result<Option<ReadyChunkCandidate>> {
        let started_by_batch =
            self.started_counts_by_batch(tenant_id, task_queue, owned_partitions, partition_count)?;
        for candidate in self.load_ready_chunk_entries(tenant_id, task_queue, now)? {
            if !owned_partitions
                .map(|partitions| {
                    partitions.contains(&throughput_partition_id(
                        &candidate.identity.batch_id,
                        candidate.group_id,
                        partition_count,
                    ))
                })
                .unwrap_or(true)
            {
                continue;
            }
            let Some(batch) = self.load_batch_state(&candidate.identity)? else {
                continue;
            };
            if matches!(batch.status.as_str(), "completed" | "failed" | "cancelled")
                || batch.failed_items > 0
                || batch.cancelled_items > 0
            {
                continue;
            }
            if max_active_chunks_per_batch.is_some_and(|limit| {
                started_by_batch.get(&batch_key(&candidate.identity)).copied().unwrap_or_default()
                    >= limit
            }) {
                continue;
            }
            return Ok(Some(ReadyChunkCandidate {
                identity: candidate.identity,
                chunk_id: candidate.chunk_id,
            }));
        }
        Ok(None)
    }

    pub fn lease_next_ready_chunk(
        &self,
        tenant_id: &str,
        task_queue: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_ttl: chrono::Duration,
        max_active_chunks_per_batch: Option<usize>,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
    ) -> Result<Option<LeasedChunkSnapshot>> {
        Ok(self
            .lease_ready_chunks(
                tenant_id,
                task_queue,
                worker_id,
                now,
                lease_ttl,
                max_active_chunks_per_batch,
                owned_partitions,
                partition_count,
                1,
            )?
            .into_iter()
            .next())
    }

    pub fn lease_ready_chunks(
        &self,
        tenant_id: &str,
        task_queue: &str,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_ttl: chrono::Duration,
        max_active_chunks_per_batch: Option<usize>,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
        max_chunks: usize,
    ) -> Result<Vec<LeasedChunkSnapshot>> {
        let _lease_guard = self.lease_lock.lock().expect("local throughput lease lock poisoned");
        let mut started_by_batch =
            self.started_counts_by_batch(tenant_id, task_queue, owned_partitions, partition_count)?;
        let mut leased_chunks = Vec::with_capacity(max_chunks.max(1));
        let mut write_batch = WriteBatch::default();
        for candidate in self.load_ready_chunk_entries(tenant_id, task_queue, now)? {
            if leased_chunks.len() >= max_chunks.max(1) {
                break;
            }
            if !owned_partitions
                .map(|partitions| {
                    partitions.contains(&throughput_partition_id(
                        &candidate.identity.batch_id,
                        candidate.group_id,
                        partition_count,
                    ))
                })
                .unwrap_or(true)
            {
                continue;
            }
            let Some(batch) = self.load_batch_state(&candidate.identity)? else {
                continue;
            };
            let batch_key = batch_key(&candidate.identity);
            if matches!(batch.status.as_str(), "completed" | "failed" | "cancelled")
                || batch.failed_items > 0
                || batch.cancelled_items > 0
            {
                continue;
            }
            if max_active_chunks_per_batch.is_some_and(|limit| {
                started_by_batch.get(&batch_key).copied().unwrap_or_default() >= limit
            }) {
                continue;
            }
            let Some(existing_chunk) =
                self.load_chunk_state(&candidate.identity, &candidate.chunk_id)?
            else {
                continue;
            };
            if existing_chunk.status != WorkflowBulkChunkStatus::Scheduled.as_str()
                || existing_chunk.available_at > now
            {
                continue;
            }
            let mut leased_chunk = existing_chunk.clone();
            leased_chunk.status = WorkflowBulkChunkStatus::Started.as_str().to_owned();
            leased_chunk.worker_id = Some(worker_id.to_owned());
            leased_chunk.lease_epoch = leased_chunk.lease_epoch.saturating_add(1);
            leased_chunk.lease_token = Some(uuid::Uuid::now_v7().to_string());
            leased_chunk.started_at = leased_chunk.started_at.or(Some(now));
            leased_chunk.lease_expires_at = Some(now + lease_ttl);
            leased_chunk.updated_at = now;

            self.write_chunk_state(&mut write_batch, Some(&existing_chunk), &leased_chunk)?;
            *started_by_batch.entry(batch_key).or_default() += 1;
            leased_chunks.push(LeasedChunkSnapshot {
                identity: leased_chunk.identity.clone(),
                definition_id: batch.definition_id.clone(),
                definition_version: batch.definition_version,
                artifact_hash: batch.artifact_hash.clone(),
                chunk_id: leased_chunk.chunk_id.clone(),
                activity_type: leased_chunk.activity_type.clone(),
                task_queue: leased_chunk.task_queue.clone(),
                chunk_index: leased_chunk.chunk_index,
                group_id: leased_chunk.group_id,
                attempt: leased_chunk.attempt,
                item_count: leased_chunk.item_count,
                max_attempts: leased_chunk.max_attempts,
                retry_delay_ms: leased_chunk.retry_delay_ms,
                items: leased_chunk.items.clone(),
                input_handle: leased_chunk.input_handle.clone(),
                cancellation_requested: leased_chunk.cancellation_requested,
                lease_epoch: leased_chunk.lease_epoch,
                owner_epoch: leased_chunk.owner_epoch,
                worker_id: leased_chunk.worker_id.clone(),
                lease_token: leased_chunk.lease_token.clone(),
                scheduled_at: leased_chunk.scheduled_at,
                started_at: leased_chunk.started_at,
                lease_expires_at: leased_chunk.lease_expires_at,
                updated_at: leased_chunk.updated_at,
            });
        }
        if !leased_chunks.is_empty() {
            self.db
                .write(write_batch)
                .context("failed to atomically lease ready throughput chunks from local state")?;
        }
        Ok(leased_chunks)
    }

    pub fn debug_lease_selection(
        &self,
        tenant_id: &str,
        task_queue: &str,
        now: DateTime<Utc>,
        max_active_chunks_per_batch: Option<usize>,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
    ) -> Result<LeaseSelectionDebug> {
        let started_by_batch =
            self.started_counts_by_batch(tenant_id, task_queue, owned_partitions, partition_count)?;
        let mut debug = LeaseSelectionDebug::default();
        for candidate in self.load_ready_chunk_entries(tenant_id, task_queue, now)? {
            debug.indexed_candidates = debug.indexed_candidates.saturating_add(1);
            if !owned_partitions
                .map(|partitions| {
                    partitions.contains(&throughput_partition_id(
                        &candidate.identity.batch_id,
                        candidate.group_id,
                        partition_count,
                    ))
                })
                .unwrap_or(true)
            {
                continue;
            }
            debug.owned_candidates = debug.owned_candidates.saturating_add(1);

            let Some(batch) = self.load_batch_state(&candidate.identity)? else {
                debug.missing_batch = debug.missing_batch.saturating_add(1);
                continue;
            };
            if matches!(batch.status.as_str(), "completed" | "failed" | "cancelled") {
                debug.terminal_batch = debug.terminal_batch.saturating_add(1);
                continue;
            }
            if batch.failed_items > 0 || batch.cancelled_items > 0 {
                debug.batch_failed_or_cancelled = debug.batch_failed_or_cancelled.saturating_add(1);
                continue;
            }
            if max_active_chunks_per_batch.is_some_and(|limit| {
                started_by_batch.get(&batch_key(&candidate.identity)).copied().unwrap_or_default()
                    >= limit
            }) {
                debug.batch_cap_blocked = debug.batch_cap_blocked.saturating_add(1);
                continue;
            }
            let Some(chunk) = self.load_chunk_state(&candidate.identity, &candidate.chunk_id)?
            else {
                debug.missing_chunk = debug.missing_chunk.saturating_add(1);
                continue;
            };
            if chunk.status != WorkflowBulkChunkStatus::Scheduled.as_str() {
                debug.chunk_not_scheduled = debug.chunk_not_scheduled.saturating_add(1);
                continue;
            }
            if chunk.available_at > now {
                debug.chunk_not_due = debug.chunk_not_due.saturating_add(1);
                continue;
            }
            debug.leaseable_candidates = debug.leaseable_candidates.saturating_add(1);
        }
        Ok(debug)
    }

    pub fn has_ready_chunk(
        &self,
        tenant_id: &str,
        task_queue: &str,
        now: DateTime<Utc>,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
    ) -> Result<bool> {
        for candidate in self.load_ready_chunk_entries(tenant_id, task_queue, now)? {
            if !owned_partitions
                .map(|partitions| {
                    partitions.contains(&throughput_partition_id(
                        &candidate.identity.batch_id,
                        candidate.group_id,
                        partition_count,
                    ))
                })
                .unwrap_or(true)
            {
                continue;
            }
            let Some(batch) = self.load_batch_state(&candidate.identity)? else {
                continue;
            };
            if !matches!(batch.status.as_str(), "completed" | "failed" | "cancelled")
                && batch.failed_items == 0
                && batch.cancelled_items == 0
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn expired_started_chunks(
        &self,
        now: DateTime<Utc>,
        limit: usize,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
    ) -> Result<Vec<ExpiredChunkCandidate>> {
        Ok(self
            .load_expired_started_chunk_entries(now)?
            .into_iter()
            .filter(|chunk| {
                owned_partitions
                    .map(|partitions| {
                        partitions.contains(&throughput_partition_id(
                            &chunk.identity.batch_id,
                            chunk.group_id,
                            partition_count,
                        ))
                    })
                    .unwrap_or(true)
            })
            .into_iter()
            .take(limit)
            .map(|chunk| ExpiredChunkCandidate {
                identity: chunk.identity,
                chunk_id: chunk.chunk_id,
            })
            .collect())
    }

    pub fn group_barrier_batches(
        &self,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
    ) -> Result<Vec<ThroughputBatchIdentity>> {
        let mut identities = self
            .load_all_batches()?
            .into_iter()
            .filter(|batch| {
                batch.aggregation_group_count > 1
                    && !matches!(batch.status.as_str(), "completed" | "failed" | "cancelled")
                    && owned_partitions
                        .map(|partitions| {
                            partitions.contains(&throughput_partition_id(
                                &batch.identity.batch_id,
                                0,
                                partition_count,
                            ))
                        })
                        .unwrap_or(true)
            })
            .map(|batch| batch.identity)
            .collect::<Vec<_>>();
        identities.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then_with(|| left.instance_id.cmp(&right.instance_id))
                .then_with(|| left.run_id.cmp(&right.run_id))
                .then_with(|| left.batch_id.cmp(&right.batch_id))
        });
        Ok(identities)
    }

    pub fn validate_report(
        &self,
        identity: &ThroughputBatchIdentity,
        chunk_id: &str,
        attempt: u32,
        lease_epoch: u64,
        lease_token: &str,
        owner_epoch: u64,
        report_id: &str,
    ) -> Result<ReportValidation> {
        let batch = self.load_batch_state(identity)?;
        let chunk = self.load_chunk_state(identity, chunk_id)?;
        Ok(validate_loaded_report(
            batch.as_ref(),
            chunk.as_ref(),
            attempt,
            lease_epoch,
            lease_token,
            owner_epoch,
            report_id,
        ))
    }

    pub fn prepare_report_apply(
        &self,
        report: &ThroughputChunkReport,
    ) -> Result<PreparedReportApply> {
        let identity = ThroughputBatchIdentity {
            tenant_id: report.tenant_id.clone(),
            instance_id: report.instance_id.clone(),
            run_id: report.run_id.clone(),
            batch_id: report.batch_id.clone(),
        };
        let Some(batch) = self.load_batch_state(&identity)? else {
            return Ok(PreparedReportApply::MissingBatch);
        };
        let chunk = self.load_chunk_state(&identity, &report.chunk_id)?;
        let validation = validate_loaded_report(
            Some(&batch),
            chunk.as_ref(),
            report.attempt,
            report.lease_epoch,
            &report.lease_token,
            report.owner_epoch,
            &report.report_id,
        );
        if validation != ReportValidation::Accept {
            return Ok(PreparedReportApply::Rejected(validation));
        }
        let chunk = chunk.expect("validated report apply missing chunk");
        Ok(PreparedReportApply::Accepted(
            self.project_report_apply_from_loaded(&identity, report, &batch, &chunk)?,
        ))
    }

    pub fn project_report_apply(
        &self,
        report: &ThroughputChunkReport,
    ) -> Result<Option<ProjectedTerminalApply>> {
        Ok(match self.prepare_report_apply(report)? {
            PreparedReportApply::Accepted(projected) => Some(projected),
            PreparedReportApply::MissingBatch | PreparedReportApply::Rejected(_) => None,
        })
    }

    fn project_report_apply_from_loaded(
        &self,
        identity: &ThroughputBatchIdentity,
        report: &ThroughputChunkReport,
        batch: &LocalBatchState,
        chunk: &LocalChunkState,
    ) -> Result<ProjectedTerminalApply> {
        let mut projected = ProjectedTerminalApply {
            batch_definition_id: batch.definition_id.clone(),
            batch_definition_version: batch.definition_version,
            batch_artifact_hash: batch.artifact_hash.clone(),
            batch_task_queue: batch.task_queue.clone(),
            batch_execution_policy: batch.execution_policy.clone(),
            batch_reducer: batch.reducer.clone(),
            batch_reducer_class: batch.reducer_class.clone(),
            batch_aggregation_tree_depth: batch.aggregation_tree_depth.max(1),
            batch_fast_lane_enabled: batch.fast_lane_enabled,
            batch_total_items: batch.total_items,
            batch_chunk_count: batch.chunk_count,
            chunk_id: report.chunk_id.clone(),
            chunk_status: chunk.status.clone(),
            chunk_attempt: chunk.attempt,
            chunk_available_at: chunk.available_at,
            chunk_error: chunk.error.clone(),
            chunk_item_count: chunk.item_count,
            chunk_max_attempts: chunk.max_attempts,
            chunk_retry_delay_ms: chunk.retry_delay_ms,
            batch_status: if batch.status == "scheduled" {
                "running".to_owned()
            } else {
                batch.status.clone()
            },
            batch_succeeded_items: batch.succeeded_items,
            batch_failed_items: batch.failed_items,
            batch_cancelled_items: batch.cancelled_items,
            batch_error: batch.error.clone(),
            batch_terminal: false,
            batch_terminal_deferred: false,
            terminal_at: None,
            group_terminal: None,
            parent_group_terminals: Vec::new(),
            grouped_batch_terminal: None,
            grouped_batch_summary: None,
        };
        let mut loaded_chunks: Option<Vec<LocalChunkState>> = None;

        match &report.payload {
            ThroughputChunkReportPayload::ChunkCompleted { .. } => {
                projected.chunk_status = WorkflowBulkChunkStatus::Completed.as_str().to_owned();
                projected.chunk_error = None;
                projected.batch_succeeded_items =
                    projected.batch_succeeded_items.saturating_add(chunk.item_count);
                if loaded_chunks.is_none() {
                    loaded_chunks = Some(self.load_chunks_for_batch(identity)?);
                }
                let chunks =
                    loaded_chunks.as_ref().expect("loaded report apply chunks missing after load");
                let all_chunks_terminal = chunks.iter().all(|candidate| {
                    if candidate.chunk_id == report.chunk_id {
                        true
                    } else {
                        matches!(candidate.status.as_str(), "completed" | "failed" | "cancelled")
                    }
                });
                if all_chunks_terminal
                    && batch.total_items > 0
                    && projected.batch_succeeded_items >= batch.total_items
                {
                    projected.batch_status = "completed".to_owned();
                    projected.batch_terminal = true;
                    projected.terminal_at = Some(report.occurred_at);
                    projected.batch_error = None;
                }
            }
            ThroughputChunkReportPayload::ChunkFailed { error } => {
                if chunk.attempt < chunk.max_attempts {
                    projected.chunk_status = WorkflowBulkChunkStatus::Scheduled.as_str().to_owned();
                    projected.chunk_attempt = chunk.attempt.saturating_add(1);
                    projected.chunk_available_at = report.occurred_at
                        + chrono::Duration::milliseconds(chunk.retry_delay_ms as i64);
                    projected.chunk_error = Some(error.clone());
                } else {
                    projected.chunk_status = WorkflowBulkChunkStatus::Failed.as_str().to_owned();
                    projected.chunk_error = Some(error.clone());
                    projected.batch_failed_items =
                        projected.batch_failed_items.saturating_add(chunk.item_count);
                    projected.batch_status = "failed".to_owned();
                    projected.batch_error = Some(error.clone());
                    projected.batch_terminal = true;
                    projected.terminal_at = Some(report.occurred_at);
                }
            }
            ThroughputChunkReportPayload::ChunkCancelled { reason, .. } => {
                projected.chunk_status = WorkflowBulkChunkStatus::Cancelled.as_str().to_owned();
                projected.chunk_error = Some(reason.clone());
                projected.batch_cancelled_items =
                    projected.batch_cancelled_items.saturating_add(chunk.item_count);
                projected.batch_status = "cancelled".to_owned();
                projected.batch_error = Some(reason.clone());
                projected.batch_terminal = true;
                projected.terminal_at = Some(report.occurred_at);
            }
        }

        if batch.aggregation_group_count > 1 && projected.batch_terminal {
            projected.batch_terminal = false;
            projected.batch_terminal_deferred = true;
            projected.terminal_at = None;
            projected.batch_status = "running".to_owned();
        }
        if batch.aggregation_group_count > 1 {
            if loaded_chunks.is_none() {
                loaded_chunks = Some(self.load_chunks_for_batch(identity)?);
            }
            let chunks = loaded_chunks
                .as_ref()
                .expect("loaded grouped report apply chunks missing after load");
            let existing_group = self.load_group_state(identity, report.group_id)?;
            projected.group_terminal = project_group_terminal_from_loaded(
                batch,
                existing_group.as_ref(),
                chunks,
                report.group_id,
                Some((
                    report.chunk_id.as_str(),
                    projected.chunk_status.as_str(),
                    projected.chunk_error.as_deref(),
                )),
                report.occurred_at,
            );
            if let Some(group_terminal) = projected.group_terminal.as_ref() {
                let groups = self.load_groups_for_batch(identity)?;
                projected.parent_group_terminals = project_parent_group_terminals_from_loaded(
                    batch,
                    &groups,
                    Some(group_terminal),
                    report.occurred_at,
                );
                let projected_groups = project_groups_for_batch_from_loaded(
                    chunks,
                    &groups,
                    Some(group_terminal),
                    &projected.parent_group_terminals,
                );
                projected.grouped_batch_terminal = project_batch_terminal_from_loaded(
                    batch,
                    &projected_groups,
                    report.occurred_at,
                );
                if projected.grouped_batch_terminal.is_none() {
                    projected.grouped_batch_summary = summarize_projected_groups(&projected_groups);
                }
            }
        }

        Ok(projected)
    }

    pub fn project_batch_terminal_from_groups(
        &self,
        identity: &ThroughputBatchIdentity,
        occurred_at: DateTime<Utc>,
    ) -> Result<Option<ProjectedBatchTerminal>> {
        self.project_batch_terminal_from_groups_with_pending_group(identity, None, occurred_at)
    }

    pub fn project_batch_terminal_from_groups_with_pending_group(
        &self,
        identity: &ThroughputBatchIdentity,
        pending_group: Option<&ProjectedGroupTerminal>,
        occurred_at: DateTime<Utc>,
    ) -> Result<Option<ProjectedBatchTerminal>> {
        let Some(batch) = self.load_batch_state(identity)? else {
            return Ok(None);
        };
        let groups = self.load_projected_groups_for_batch(identity, pending_group)?;
        Ok(project_batch_terminal_from_loaded(&batch, &groups, occurred_at))
    }

    pub fn project_requeue(
        &self,
        identity: &ThroughputBatchIdentity,
        chunk_id: &str,
        occurred_at: DateTime<Utc>,
    ) -> Result<Option<ProjectedChunkReschedule>> {
        let Some(chunk) = self.load_chunk_state(identity, chunk_id)? else {
            return Ok(None);
        };
        if chunk.status != WorkflowBulkChunkStatus::Started.as_str() {
            return Ok(None);
        }
        Ok(Some(ProjectedChunkReschedule {
            chunk_id: chunk.chunk_id,
            chunk_index: chunk.chunk_index,
            group_id: chunk.group_id,
            attempt: chunk.attempt,
            item_count: chunk.item_count,
            max_attempts: chunk.max_attempts,
            retry_delay_ms: chunk.retry_delay_ms,
            lease_epoch: chunk.lease_epoch,
            owner_epoch: chunk.owner_epoch,
            available_at: occurred_at,
            started_at: chunk.started_at,
        }))
    }

    pub fn project_group_terminal(
        &self,
        identity: &ThroughputBatchIdentity,
        group_id: u32,
        occurred_at: DateTime<Utc>,
    ) -> Result<Option<ProjectedGroupTerminal>> {
        self.project_group_terminal_after_apply(identity, group_id, None, occurred_at)
    }

    pub fn project_group_terminal_after_chunk_apply(
        &self,
        identity: &ThroughputBatchIdentity,
        group_id: u32,
        chunk_id: &str,
        chunk_status: &str,
        chunk_error: Option<&str>,
        occurred_at: DateTime<Utc>,
    ) -> Result<Option<ProjectedGroupTerminal>> {
        self.project_group_terminal_after_apply(
            identity,
            group_id,
            Some((chunk_id, chunk_status, chunk_error)),
            occurred_at,
        )
    }

    fn project_group_terminal_after_apply(
        &self,
        identity: &ThroughputBatchIdentity,
        group_id: u32,
        pending_chunk: Option<(&str, &str, Option<&str>)>,
        occurred_at: DateTime<Utc>,
    ) -> Result<Option<ProjectedGroupTerminal>> {
        let Some(batch) = self.load_batch_state(identity)? else {
            return Ok(None);
        };
        let existing_group = self.load_group_state(identity, group_id)?;
        let chunks = self.load_chunks_for_batch(identity)?.into_iter().collect::<Vec<_>>();
        Ok(project_group_terminal_from_loaded(
            &batch,
            existing_group.as_ref(),
            &chunks,
            group_id,
            pending_chunk,
            occurred_at,
        ))
    }

    fn load_projected_groups_for_batch(
        &self,
        identity: &ThroughputBatchIdentity,
        pending_group: Option<&ProjectedGroupTerminal>,
    ) -> Result<Vec<ProjectedGroupTerminal>> {
        let Some(batch) = self.load_batch_state(identity)? else {
            return Ok(Vec::new());
        };
        let chunks = self.load_chunks_for_batch(identity)?;
        let groups = self.load_groups_for_batch(identity)?;
        let parent_groups = project_parent_group_terminals_from_loaded(
            &batch,
            &groups,
            pending_group,
            pending_group.map(|group| group.terminal_at).unwrap_or_else(Utc::now),
        );
        Ok(project_groups_for_batch_from_loaded(&chunks, &groups, pending_group, &parent_groups))
    }

    pub fn record_changelog_apply_failure(&self) {
        let mut meta = self.meta.lock().expect("local throughput meta lock poisoned");
        meta.changelog_apply_failures = meta.changelog_apply_failures.saturating_add(1);
    }

    pub fn snapshot_checkpoint_value(&self) -> Result<Value> {
        serde_json::to_value(self.build_checkpoint()?)
            .context("failed to serialize throughput checkpoint value")
    }

    pub fn restore_from_checkpoint_value_if_empty(&self, value: Value) -> Result<bool> {
        let checkpoint: CheckpointFile =
            serde_json::from_value(value).context("failed to deserialize throughput checkpoint")?;
        self.restore_from_checkpoint_if_empty(checkpoint)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn write_checkpoint(&self) -> Result<PathBuf> {
        let checkpoint = self.build_checkpoint()?;
        let payload =
            serde_json::to_vec_pretty(&checkpoint).context("failed to serialize checkpoint")?;
        let checkpoint_file = self
            .checkpoint_dir
            .join(format!("checkpoint-{}.json", checkpoint.created_at.timestamp_millis()));
        let temp_file = self.checkpoint_dir.join("latest.tmp");
        fs::write(&temp_file, payload)
            .with_context(|| format!("failed to write checkpoint file {}", temp_file.display()))?;
        fs::rename(&temp_file, &checkpoint_file).with_context(|| {
            format!("failed to finalize checkpoint file {}", checkpoint_file.display())
        })?;
        fs::copy(&checkpoint_file, self.checkpoint_dir.join(LATEST_CHECKPOINT_FILE)).with_context(
            || {
                format!(
                    "failed to update latest throughput checkpoint in {}",
                    self.checkpoint_dir.display()
                )
            },
        )?;
        self.prune_old_checkpoints()?;
        self.record_checkpoint_write(checkpoint.created_at);
        Ok(checkpoint_file)
    }

    pub fn record_checkpoint_failure(&self) {
        let mut meta = self.meta.lock().expect("local throughput meta lock poisoned");
        meta.checkpoint_failures = meta.checkpoint_failures.saturating_add(1);
    }

    pub fn record_checkpoint_write(&self, created_at: DateTime<Utc>) {
        let mut meta = self.meta.lock().expect("local throughput meta lock poisoned");
        meta.last_checkpoint_at = Some(created_at);
        meta.checkpoint_writes = meta.checkpoint_writes.saturating_add(1);
    }

    pub fn prune_terminal_state(&self, older_than: DateTime<Utc>) -> Result<u64> {
        let batches = self.load_all_batches()?;
        let chunks = self.load_all_chunks()?;
        let groups = self.load_all_groups()?;
        let mut write_batch = WriteBatch::default();
        let mut deleted = 0_u64;

        for batch in batches.into_iter().filter(|batch| {
            matches!(batch.status.as_str(), "completed" | "failed" | "cancelled")
                && batch.terminal_at.is_some_and(|terminal_at| terminal_at <= older_than)
        }) {
            write_batch.delete(batch_key(&batch.identity));
            deleted = deleted.saturating_add(1);

            for chunk in chunks.iter().filter(|chunk| chunk.identity == batch.identity) {
                self.delete_chunk_state(&mut write_batch, chunk)?;
                deleted = deleted.saturating_add(1);
            }

            for group in groups.iter().filter(|group| group.identity == batch.identity) {
                self.delete_group_state(&mut write_batch, group)?;
                deleted = deleted.saturating_add(1);
            }
        }

        if deleted > 0 {
            self.db.write(write_batch).context("failed to prune terminal throughput state")?;
        }
        Ok(deleted)
    }

    pub fn partition_is_caught_up(&self, partition_id: i32) -> Result<bool> {
        let applied = self.last_applied_offset(partition_id)?;
        let high_watermark = {
            let meta = self.meta.lock().expect("local throughput meta lock poisoned");
            meta.observed_high_watermarks.get(&partition_id).copied()
        };
        let Some(high_watermark) = high_watermark else {
            return Ok(true);
        };
        if high_watermark <= 0 {
            return Ok(true);
        }
        Ok(applied.unwrap_or(-1) >= high_watermark.saturating_sub(1))
    }

    pub fn debug_snapshot(&self) -> Result<LocalThroughputDebugSnapshot> {
        let meta = self.meta.lock().expect("local throughput meta lock poisoned");
        let last_applied_offsets = self.load_offsets()?;
        let observed_high_watermarks = meta.observed_high_watermarks.clone();
        let partition_lag = observed_high_watermarks
            .iter()
            .map(|(partition_id, high_watermark)| {
                let applied = last_applied_offsets.get(partition_id).copied().unwrap_or(-1);
                let lag = if *high_watermark <= 0 {
                    0
                } else {
                    high_watermark.saturating_sub(applied.saturating_add(1))
                };
                (*partition_id, lag.max(0))
            })
            .collect::<BTreeMap<_, _>>();
        let batches = self.load_all_batches()?;
        let chunks = self.load_all_chunks()?;
        let groups = self.load_all_groups()?;
        let batch_status_counts =
            batches.iter().fold(BTreeMap::new(), |mut counts: BTreeMap<String, u64>, batch| {
                *counts.entry(batch.status.clone()).or_default() += 1;
                counts
            });
        let batch_tree_depth_counts =
            batches.iter().fold(BTreeMap::new(), |mut counts: BTreeMap<u32, u64>, batch| {
                *counts.entry(batch.aggregation_tree_depth.max(1)).or_default() += 1;
                counts
            });
        let group_level_counts =
            groups.iter().fold(BTreeMap::new(), |mut counts: BTreeMap<u32, u64>, group| {
                *counts.entry(group.level).or_default() += 1;
                counts
            });
        let scheduled_chunk_count = chunks
            .iter()
            .filter(|chunk| chunk.status == WorkflowBulkChunkStatus::Scheduled.as_str())
            .count() as u64;
        let ready_chunk_count = chunks
            .iter()
            .filter(|chunk| {
                chunk.status == WorkflowBulkChunkStatus::Scheduled.as_str()
                    && chunk.available_at <= Utc::now()
            })
            .count() as u64;
        Ok(LocalThroughputDebugSnapshot {
            db_path: self.db_path.display().to_string(),
            checkpoint_dir: self.checkpoint_dir.display().to_string(),
            restored_from_checkpoint: meta.restored_from_checkpoint,
            last_checkpoint_at: meta.last_checkpoint_at,
            checkpoint_writes: meta.checkpoint_writes,
            checkpoint_failures: meta.checkpoint_failures,
            changelog_entries_applied: meta.changelog_entries_applied,
            changelog_apply_failures: meta.changelog_apply_failures,
            last_changelog_apply_at: meta.last_changelog_apply_at,
            batch_count: batches.len(),
            chunk_count: chunks.len(),
            scheduled_chunk_count,
            ready_chunk_count,
            started_chunk_count: self.count_started_chunks(None, None, None, None, 1)?,
            batch_status_counts,
            batch_tree_depth_counts,
            group_level_counts,
            last_applied_offsets,
            observed_high_watermarks,
            partition_lag,
        })
    }

    fn is_empty(&self) -> Result<bool> {
        let mut iterator = self.db.iterator(IteratorMode::Start);
        Ok(iterator.next().is_none())
    }

    fn last_applied_offset(&self, partition_id: i32) -> Result<Option<i64>> {
        self.db
            .get(offset_key(partition_id))
            .context("failed to load throughput changelog offset")?
            .map(|value| {
                String::from_utf8(value)
                    .context("throughput changelog offset is not utf-8")?
                    .parse::<i64>()
                    .context("throughput changelog offset is not numeric")
            })
            .transpose()
    }

    fn is_mirrored_entry_pending(&self, entry_id: uuid::Uuid) -> Result<bool> {
        Ok(self
            .db
            .get(mirrored_entry_key(entry_id))
            .context("failed to load mirrored throughput entry marker")?
            .is_some())
    }

    fn load_offsets(&self) -> Result<BTreeMap<i32, i64>> {
        let mut offsets = BTreeMap::new();
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, value) = entry.context("failed to iterate throughput state offsets")?;
            let key =
                String::from_utf8(key.to_vec()).context("throughput state key is not utf-8")?;
            if let Some(partition) = key.strip_prefix(OFFSET_KEY_PREFIX) {
                let partition = partition
                    .parse::<i32>()
                    .context("throughput offset partition is not numeric")?;
                let offset = String::from_utf8(value.to_vec())
                    .context("throughput offset value is not utf-8")?
                    .parse::<i64>()
                    .context("throughput offset value is not numeric")?;
                offsets.insert(partition, offset);
            }
        }
        Ok(offsets)
    }

    fn load_mirrored_entry_ids(&self) -> Result<Vec<String>> {
        let mut entry_ids = Vec::new();
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, _value) =
                entry.context("failed to iterate throughput mirrored-entry markers")?;
            let key =
                String::from_utf8(key.to_vec()).context("throughput state key is not utf-8")?;
            if let Some(entry_id) = key.strip_prefix(MIRRORED_ENTRY_KEY_PREFIX) {
                entry_ids.push(entry_id.to_owned());
            }
        }
        entry_ids.sort();
        Ok(entry_ids)
    }

    fn load_batch_state(
        &self,
        identity: &ThroughputBatchIdentity,
    ) -> Result<Option<LocalBatchState>> {
        self.db
            .get(batch_key(identity))
            .context("failed to load throughput batch state")?
            .map(|value| decode_rocksdb_value(&value, "throughput batch state"))
            .transpose()
    }

    fn load_all_batches(&self) -> Result<Vec<LocalBatchState>> {
        let mut batches = Vec::new();
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, value) = entry.context("failed to iterate throughput state batches")?;
            let key =
                String::from_utf8(key.to_vec()).context("throughput state key is not utf-8")?;
            if key.starts_with(BATCH_KEY_PREFIX) {
                batches.push(decode_rocksdb_value(&value, "throughput batch state")?);
            }
        }
        Ok(batches)
    }

    fn load_all_chunks(&self) -> Result<Vec<LocalChunkState>> {
        let mut chunks = Vec::new();
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, value) = entry.context("failed to iterate throughput state chunks")?;
            let key =
                String::from_utf8(key.to_vec()).context("throughput state key is not utf-8")?;
            if key.starts_with(CHUNK_KEY_PREFIX) {
                chunks.push(decode_rocksdb_chunk_state(&value)?);
            }
        }
        Ok(chunks)
    }

    fn load_group_state(
        &self,
        identity: &ThroughputBatchIdentity,
        group_id: u32,
    ) -> Result<Option<LocalGroupState>> {
        self.db
            .get(group_key(identity, group_id))
            .context("failed to load throughput group state")?
            .map(|value| decode_rocksdb_value(&value, "throughput group state"))
            .transpose()
    }

    fn load_all_groups(&self) -> Result<Vec<LocalGroupState>> {
        let mut groups = Vec::new();
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, value) = entry.context("failed to iterate throughput state groups")?;
            let key =
                String::from_utf8(key.to_vec()).context("throughput state key is not utf-8")?;
            if key.starts_with(GROUP_KEY_PREFIX) {
                groups.push(decode_rocksdb_value(&value, "throughput group state")?);
            }
        }
        Ok(groups)
    }

    fn load_chunks_for_batch(
        &self,
        identity: &ThroughputBatchIdentity,
    ) -> Result<Vec<LocalChunkState>> {
        let mut chunks = Vec::new();
        for index in self.load_batch_chunk_entries(identity)? {
            if let Some(chunk) = self.load_chunk_state(identity, &index.chunk_id)? {
                chunks.push(chunk);
            }
        }
        Ok(chunks)
    }

    fn load_groups_for_batch(
        &self,
        identity: &ThroughputBatchIdentity,
    ) -> Result<Vec<LocalGroupState>> {
        let mut groups = Vec::new();
        for index in self.load_batch_group_entries(identity)? {
            if let Some(group) = self.load_group_state(identity, index.group_id)? {
                groups.push(group);
            }
        }
        Ok(groups)
    }

    fn load_batch_chunk_entries(
        &self,
        identity: &ThroughputBatchIdentity,
    ) -> Result<Vec<BatchChunkIndexEntry>> {
        self.load_index_entries_by_prefix(&batch_chunk_index_prefix(identity))
    }

    fn load_batch_group_entries(
        &self,
        identity: &ThroughputBatchIdentity,
    ) -> Result<Vec<BatchGroupIndexEntry>> {
        self.load_index_entries_by_prefix(&batch_group_index_prefix(identity))
    }

    fn load_ready_chunk_entries(
        &self,
        tenant_id: &str,
        task_queue: &str,
        now: DateTime<Utc>,
    ) -> Result<Vec<ReadyChunkIndexEntry>> {
        let prefix = ready_index_scope_prefix(tenant_id, task_queue);
        let mut entries = Vec::new();
        let mut stale_keys = Vec::new();
        for entry in
            self.db.iterator(IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward))
        {
            let (key, value) = entry.context("failed to iterate ready chunk index")?;
            let key =
                String::from_utf8(key.to_vec()).context("ready chunk index key is not utf-8")?;
            if !key.starts_with(&prefix) {
                break;
            }
            let indexed: ReadyChunkIndexEntry =
                decode_rocksdb_value(&value, "ready chunk index entry")?;
            if let Some(chunk) = self.load_chunk_state(&indexed.identity, &indexed.chunk_id)? {
                if chunk.status == WorkflowBulkChunkStatus::Scheduled.as_str() {
                    if chunk.available_at <= now {
                        entries.push(indexed);
                    } else {
                        // The ready index is ordered by `available_at`, so once we hit a future
                        // scheduled chunk the remaining entries are not leaseable yet either.
                        break;
                    }
                    continue;
                }
            }
            stale_keys.push(key);
        }
        if !stale_keys.is_empty() {
            let mut write_batch = WriteBatch::default();
            for key in stale_keys {
                write_batch.delete(key.as_bytes());
            }
            self.db.write(write_batch).context("failed to prune stale ready chunk index entry")?;
        }
        Ok(entries)
    }

    fn load_started_chunk_entries(
        &self,
        tenant_id: Option<&str>,
        task_queue: Option<&str>,
    ) -> Result<Vec<StartedChunkIndexEntry>> {
        let prefix = started_index_scope_prefix(tenant_id, task_queue);
        let mut entries = Vec::new();
        let mut stale_keys = Vec::new();
        for entry in
            self.db.iterator(IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward))
        {
            let (key, value) = entry.context("failed to iterate started chunk index")?;
            let key =
                String::from_utf8(key.to_vec()).context("started chunk index key is not utf-8")?;
            if !key.starts_with(&prefix) {
                break;
            }
            let indexed: StartedChunkIndexEntry =
                decode_rocksdb_value(&value, "started chunk index entry")?;
            if let Some(chunk) = self.load_chunk_state(&indexed.identity, &indexed.chunk_id)? {
                if chunk.status == WorkflowBulkChunkStatus::Started.as_str() {
                    entries.push(indexed);
                    continue;
                }
            }
            stale_keys.push(key);
        }
        if !stale_keys.is_empty() {
            let mut write_batch = WriteBatch::default();
            for key in stale_keys {
                write_batch.delete(key.as_bytes());
            }
            self.db
                .write(write_batch)
                .context("failed to prune stale started chunk index entry")?;
        }
        Ok(entries)
    }

    fn load_expired_started_chunk_entries(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<StartedChunkIndexEntry>> {
        let prefix = lease_expiry_index_prefix();
        let mut entries = Vec::new();
        let mut stale_keys = Vec::new();
        for entry in
            self.db.iterator(IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward))
        {
            let (key, value) = entry.context("failed to iterate lease expiry index")?;
            let key =
                String::from_utf8(key.to_vec()).context("lease expiry index key is not utf-8")?;
            if !key.starts_with(prefix) {
                break;
            }
            let indexed: StartedChunkIndexEntry =
                decode_rocksdb_value(&value, "lease expiry index entry")?;
            let Some(deadline) = indexed.lease_expires_at else {
                continue;
            };
            if deadline > now {
                break;
            }
            if let Some(chunk) = self.load_chunk_state(&indexed.identity, &indexed.chunk_id)? {
                if chunk.status == WorkflowBulkChunkStatus::Started.as_str()
                    && chunk.lease_expires_at.is_some_and(|expires_at| expires_at <= now)
                {
                    entries.push(indexed);
                    continue;
                }
            }
            stale_keys.push(key);
        }
        if !stale_keys.is_empty() {
            let mut write_batch = WriteBatch::default();
            for key in stale_keys {
                write_batch.delete(key.as_bytes());
            }
            self.db.write(write_batch).context("failed to prune stale lease expiry index entry")?;
        }
        Ok(entries)
    }

    fn started_counts_by_batch(
        &self,
        tenant_id: &str,
        task_queue: &str,
        owned_partitions: Option<&HashSet<i32>>,
        partition_count: i32,
    ) -> Result<HashMap<String, usize>> {
        let mut counts = HashMap::new();
        for chunk in self.load_started_chunk_entries(Some(tenant_id), Some(task_queue))? {
            if !owned_partitions
                .map(|partitions| {
                    partitions.contains(&throughput_partition_id(
                        &chunk.identity.batch_id,
                        chunk.group_id,
                        partition_count,
                    ))
                })
                .unwrap_or(true)
            {
                continue;
            }
            *counts.entry(batch_key(&chunk.identity)).or_insert(0) += 1;
        }
        Ok(counts)
    }

    fn load_index_entries_by_prefix<T>(&self, prefix: &str) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        let mut entries = Vec::new();
        for entry in
            self.db.iterator(IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward))
        {
            let (key, value) = entry.context("failed to iterate throughput secondary index")?;
            let key = String::from_utf8(key.to_vec())
                .context("throughput secondary index key is not utf-8")?;
            if !key.starts_with(prefix) {
                break;
            }
            entries.push(decode_rocksdb_value(&value, "throughput secondary index entry")?);
        }
        Ok(entries)
    }

    fn write_chunk_state(
        &self,
        write_batch: &mut WriteBatch,
        previous: Option<&LocalChunkState>,
        state: &LocalChunkState,
    ) -> Result<()> {
        if let Some(previous) = previous {
            self.delete_chunk_indices(write_batch, previous)?;
        }
        write_batch
            .put(chunk_key(&state.identity, &state.chunk_id), encode_rocksdb_chunk_state(state)?);
        write_batch.put(
            batch_chunk_index_key(&state.identity, &state.chunk_id),
            encode_rocksdb_value(
                &BatchChunkIndexEntry {
                    identity: state.identity.clone(),
                    chunk_id: state.chunk_id.clone(),
                },
                "batch chunk index entry",
            )?,
        );
        if state.status == WorkflowBulkChunkStatus::Scheduled.as_str() {
            write_batch.put(
                ready_index_key(state),
                encode_rocksdb_value(
                    &ReadyChunkIndexEntry {
                        identity: state.identity.clone(),
                        chunk_id: state.chunk_id.clone(),
                        group_id: state.group_id,
                    },
                    "ready chunk index entry",
                )?,
            );
        }
        if state.status == WorkflowBulkChunkStatus::Started.as_str() {
            let indexed = StartedChunkIndexEntry {
                identity: state.identity.clone(),
                chunk_id: state.chunk_id.clone(),
                group_id: state.group_id,
                lease_expires_at: state.lease_expires_at,
                scheduled_at: state.scheduled_at,
                chunk_index: state.chunk_index,
            };
            write_batch.put(
                started_index_key(state),
                encode_rocksdb_value(&indexed, "started chunk index entry")?,
            );
            if state.lease_expires_at.is_some() {
                write_batch.put(
                    lease_expiry_index_key(state),
                    encode_rocksdb_value(&indexed, "lease expiry index entry")?,
                );
            }
        }
        Ok(())
    }

    fn delete_chunk_state(
        &self,
        write_batch: &mut WriteBatch,
        state: &LocalChunkState,
    ) -> Result<()> {
        write_batch.delete(chunk_key(&state.identity, &state.chunk_id));
        self.delete_chunk_indices(write_batch, state)
    }

    fn delete_chunk_indices(
        &self,
        write_batch: &mut WriteBatch,
        state: &LocalChunkState,
    ) -> Result<()> {
        write_batch.delete(batch_chunk_index_key(&state.identity, &state.chunk_id));
        if state.status == WorkflowBulkChunkStatus::Scheduled.as_str() {
            write_batch.delete(ready_index_key(state));
        }
        if state.status == WorkflowBulkChunkStatus::Started.as_str() {
            write_batch.delete(started_index_key(state));
            if state.lease_expires_at.is_some() {
                write_batch.delete(lease_expiry_index_key(state));
            }
        }
        Ok(())
    }

    fn write_group_state(
        &self,
        write_batch: &mut WriteBatch,
        state: &LocalGroupState,
    ) -> Result<()> {
        write_batch.put(
            group_key(&state.identity, state.group_id),
            encode_rocksdb_value(state, "throughput group state")?,
        );
        write_batch.put(
            batch_group_index_key(&state.identity, state.group_id),
            encode_rocksdb_value(
                &BatchGroupIndexEntry {
                    identity: state.identity.clone(),
                    group_id: state.group_id,
                },
                "batch group index entry",
            )?,
        );
        Ok(())
    }

    fn delete_group_state(
        &self,
        write_batch: &mut WriteBatch,
        state: &LocalGroupState,
    ) -> Result<()> {
        write_batch.delete(group_key(&state.identity, state.group_id));
        write_batch.delete(batch_group_index_key(&state.identity, state.group_id));
        Ok(())
    }

    fn write_entry_payload(
        &self,
        write_batch: &mut WriteBatch,
        entry: &ThroughputChangelogEntry,
    ) -> Result<()> {
        match &entry.payload {
            ThroughputChangelogPayload::BatchCreated {
                identity,
                task_queue,
                execution_policy,
                reducer,
                reducer_class,
                aggregation_tree_depth,
                fast_lane_enabled,
                aggregation_group_count,
                total_items,
                chunk_count,
            } => {
                let mut state =
                    self.load_batch_state(identity)?.unwrap_or_else(|| LocalBatchState {
                        identity: identity.clone(),
                        definition_id: String::new(),
                        definition_version: None,
                        artifact_hash: None,
                        task_queue: String::new(),
                        execution_policy: None,
                        reducer: None,
                        reducer_class: default_reducer_class(),
                        aggregation_tree_depth: default_aggregation_tree_depth(),
                        fast_lane_enabled: false,
                        aggregation_group_count: 1,
                        total_items: 0,
                        chunk_count: 0,
                        terminal_chunk_count: 0,
                        succeeded_items: 0,
                        failed_items: 0,
                        cancelled_items: 0,
                        status: "scheduled".to_owned(),
                        last_report_id: None,
                        error: None,
                        created_at: entry.occurred_at,
                        updated_at: entry.occurred_at,
                        terminal_at: None,
                    });
                state.task_queue = task_queue.clone();
                state.execution_policy = execution_policy.clone();
                state.reducer = reducer.clone();
                state.reducer_class = reducer_class.clone();
                state.aggregation_tree_depth = (*aggregation_tree_depth).max(1);
                state.fast_lane_enabled = *fast_lane_enabled;
                state.aggregation_group_count = *aggregation_group_count;
                state.total_items = *total_items;
                state.chunk_count = *chunk_count;
                state.updated_at = entry.occurred_at;
                write_batch.put(batch_key(identity), encode_rocksdb_value(&state, "batch state")?);
            }
            ThroughputChangelogPayload::ChunkLeased {
                identity,
                chunk_id,
                chunk_index,
                attempt,
                group_id,
                item_count,
                max_attempts,
                retry_delay_ms,
                lease_epoch,
                owner_epoch,
                worker_id,
                lease_token,
                lease_expires_at,
            } => {
                let existing_chunk = self.load_chunk_state(identity, chunk_id)?;
                let mut batch_state =
                    self.load_batch_state(identity)?.unwrap_or_else(|| LocalBatchState {
                        identity: identity.clone(),
                        definition_id: String::new(),
                        definition_version: None,
                        artifact_hash: None,
                        task_queue: String::new(),
                        execution_policy: None,
                        reducer: None,
                        reducer_class: default_reducer_class(),
                        aggregation_tree_depth: default_aggregation_tree_depth(),
                        fast_lane_enabled: false,
                        aggregation_group_count: 1,
                        total_items: 0,
                        chunk_count: 0,
                        terminal_chunk_count: 0,
                        succeeded_items: 0,
                        failed_items: 0,
                        cancelled_items: 0,
                        status: "scheduled".to_owned(),
                        last_report_id: None,
                        error: None,
                        created_at: entry.occurred_at,
                        updated_at: entry.occurred_at,
                        terminal_at: None,
                    });
                if batch_state.status == "scheduled" {
                    batch_state.status = "running".to_owned();
                }
                batch_state.updated_at = entry.occurred_at;
                write_batch.put(
                    batch_key(identity),
                    encode_rocksdb_value(&batch_state, "leased batch state")?,
                );

                let state = LocalChunkState {
                    identity: identity.clone(),
                    chunk_id: chunk_id.clone(),
                    activity_type: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.activity_type.clone())
                        .unwrap_or_default(),
                    task_queue: batch_state.task_queue.clone(),
                    chunk_index: *chunk_index,
                    group_id: *group_id,
                    item_count: *item_count,
                    attempt: *attempt,
                    max_attempts: *max_attempts,
                    retry_delay_ms: *retry_delay_ms,
                    lease_epoch: *lease_epoch,
                    owner_epoch: *owner_epoch,
                    status: "started".to_owned(),
                    worker_id: Some(worker_id.clone()),
                    lease_token: Some(lease_token.clone()),
                    report_id: None,
                    result_handle: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.result_handle.clone())
                        .unwrap_or(Value::Null),
                    output: existing_chunk.as_ref().and_then(|chunk| chunk.output.clone()),
                    error: existing_chunk.as_ref().and_then(|chunk| chunk.error.clone()),
                    input_handle: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.input_handle.clone())
                        .unwrap_or(Value::Null),
                    items: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.items.clone())
                        .unwrap_or_default(),
                    cancellation_requested: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.cancellation_requested)
                        .unwrap_or(false),
                    cancellation_reason: existing_chunk
                        .as_ref()
                        .and_then(|chunk| chunk.cancellation_reason.clone()),
                    cancellation_metadata: existing_chunk
                        .as_ref()
                        .and_then(|chunk| chunk.cancellation_metadata.clone()),
                    scheduled_at: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.scheduled_at)
                        .unwrap_or(entry.occurred_at),
                    available_at: entry.occurred_at,
                    started_at: existing_chunk
                        .as_ref()
                        .and_then(|chunk| chunk.started_at)
                        .or(Some(entry.occurred_at)),
                    lease_expires_at: Some(*lease_expires_at),
                    completed_at: None,
                    updated_at: entry.occurred_at,
                };
                self.write_chunk_state(write_batch, existing_chunk.as_ref(), &state)?;
            }
            ThroughputChangelogPayload::ChunkApplied {
                identity,
                chunk_id,
                chunk_index,
                attempt,
                group_id,
                item_count,
                max_attempts,
                retry_delay_ms,
                lease_epoch,
                owner_epoch,
                report_id,
                status,
                available_at,
                result_handle,
                output,
                error,
                cancellation_reason,
                cancellation_metadata,
            } => {
                let existing_chunk = self.load_chunk_state(identity, chunk_id)?;
                let task_queue = existing_chunk
                    .as_ref()
                    .map(|chunk| chunk.task_queue.clone())
                    .or_else(|| {
                        self.load_batch_state(identity).ok().flatten().map(|batch| batch.task_queue)
                    })
                    .unwrap_or_default();
                let previous_status = existing_chunk
                    .as_ref()
                    .map(|chunk| chunk.status.as_str())
                    .unwrap_or("scheduled");
                let state = LocalChunkState {
                    identity: identity.clone(),
                    chunk_id: chunk_id.clone(),
                    activity_type: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.activity_type.clone())
                        .unwrap_or_default(),
                    task_queue,
                    chunk_index: *chunk_index,
                    group_id: *group_id,
                    item_count: *item_count,
                    attempt: *attempt,
                    max_attempts: *max_attempts,
                    retry_delay_ms: *retry_delay_ms,
                    lease_epoch: *lease_epoch,
                    owner_epoch: *owner_epoch,
                    status: status.clone(),
                    worker_id: None,
                    lease_token: None,
                    report_id: Some(report_id.clone()),
                    result_handle: result_handle.clone().unwrap_or(Value::Null),
                    output: output.clone(),
                    error: error.clone(),
                    input_handle: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.input_handle.clone())
                        .unwrap_or(Value::Null),
                    items: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.items.clone())
                        .unwrap_or_default(),
                    cancellation_requested: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.cancellation_requested)
                        .unwrap_or(false),
                    cancellation_reason: cancellation_reason.clone(),
                    cancellation_metadata: cancellation_metadata.clone(),
                    scheduled_at: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.scheduled_at)
                        .unwrap_or(entry.occurred_at),
                    available_at: *available_at,
                    started_at: existing_chunk.as_ref().and_then(|chunk| chunk.started_at),
                    lease_expires_at: None,
                    completed_at: matches!(status.as_str(), "completed" | "failed" | "cancelled")
                        .then_some(entry.occurred_at),
                    updated_at: entry.occurred_at,
                };
                self.write_chunk_state(write_batch, existing_chunk.as_ref(), &state)?;
                let mut batch_state =
                    self.load_batch_state(identity)?.unwrap_or_else(|| LocalBatchState {
                        identity: identity.clone(),
                        definition_id: String::new(),
                        definition_version: None,
                        artifact_hash: None,
                        task_queue: String::new(),
                        execution_policy: None,
                        reducer: None,
                        reducer_class: default_reducer_class(),
                        aggregation_tree_depth: default_aggregation_tree_depth(),
                        fast_lane_enabled: false,
                        aggregation_group_count: 1,
                        total_items: 0,
                        chunk_count: 0,
                        terminal_chunk_count: 0,
                        succeeded_items: 0,
                        failed_items: 0,
                        cancelled_items: 0,
                        status: "running".to_owned(),
                        last_report_id: None,
                        error: None,
                        created_at: entry.occurred_at,
                        updated_at: entry.occurred_at,
                        terminal_at: None,
                    });
                // Replay safety depends on ordered, exactly-once application by partition offset.
                // We only advance batch counters when the prior chunk state was `started`, which
                // prevents double-counting if a checkpoint already includes the post-apply chunk.
                if previous_status == WorkflowBulkChunkStatus::Started.as_str() {
                    match status.as_str() {
                        "completed" => {
                            batch_state.succeeded_items =
                                batch_state.succeeded_items.saturating_add(*item_count);
                            batch_state.terminal_chunk_count =
                                batch_state.terminal_chunk_count.saturating_add(1);
                        }
                        "failed" => {
                            batch_state.failed_items =
                                batch_state.failed_items.saturating_add(*item_count);
                            batch_state.terminal_chunk_count =
                                batch_state.terminal_chunk_count.saturating_add(1);
                            batch_state.error = error.clone();
                        }
                        "cancelled" => {
                            batch_state.cancelled_items =
                                batch_state.cancelled_items.saturating_add(*item_count);
                            batch_state.terminal_chunk_count =
                                batch_state.terminal_chunk_count.saturating_add(1);
                            batch_state.error = error.clone();
                        }
                        _ => {}
                    }
                }
                if batch_state.status == "scheduled" {
                    batch_state.status = "running".to_owned();
                }
                batch_state.updated_at = entry.occurred_at;
                if matches!(status.as_str(), "completed" | "scheduled") && error.is_none() {
                    if !matches!(batch_state.status.as_str(), "failed" | "cancelled") {
                        batch_state.error = None;
                    }
                }
                if let Some(terminal_status) =
                    infer_ungrouped_batch_terminal_from_chunk_apply(&batch_state, status)
                {
                    batch_state.status = terminal_status.to_owned();
                    batch_state.last_report_id = Some(report_id.clone());
                    batch_state.terminal_chunk_count = batch_state.chunk_count;
                    batch_state.updated_at = entry.occurred_at;
                    batch_state.terminal_at = Some(entry.occurred_at);
                    if terminal_status == WorkflowBulkBatchStatus::Completed.as_str() {
                        batch_state.error = None;
                    }
                }
                write_batch.put(
                    batch_key(identity),
                    encode_rocksdb_value(&batch_state, "touched batch state")?,
                );
            }
            ThroughputChangelogPayload::ChunkRequeued {
                identity,
                chunk_id,
                chunk_index,
                attempt,
                group_id,
                item_count,
                max_attempts,
                retry_delay_ms,
                lease_epoch,
                owner_epoch,
                available_at,
            } => {
                let existing_chunk = self.load_chunk_state(identity, chunk_id)?;
                let task_queue = existing_chunk
                    .as_ref()
                    .map(|chunk| chunk.task_queue.clone())
                    .or_else(|| {
                        self.load_batch_state(identity).ok().flatten().map(|batch| batch.task_queue)
                    })
                    .unwrap_or_default();
                let state = LocalChunkState {
                    identity: identity.clone(),
                    chunk_id: chunk_id.clone(),
                    activity_type: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.activity_type.clone())
                        .unwrap_or_default(),
                    task_queue,
                    chunk_index: *chunk_index,
                    group_id: *group_id,
                    item_count: *item_count,
                    attempt: *attempt,
                    max_attempts: *max_attempts,
                    retry_delay_ms: *retry_delay_ms,
                    lease_epoch: *lease_epoch,
                    owner_epoch: *owner_epoch,
                    status: WorkflowBulkChunkStatus::Scheduled.as_str().to_owned(),
                    worker_id: None,
                    lease_token: None,
                    report_id: existing_chunk.as_ref().and_then(|chunk| chunk.report_id.clone()),
                    result_handle: Value::Null,
                    output: None,
                    error: existing_chunk.as_ref().and_then(|chunk| chunk.error.clone()),
                    input_handle: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.input_handle.clone())
                        .unwrap_or(Value::Null),
                    items: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.items.clone())
                        .unwrap_or_default(),
                    cancellation_requested: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.cancellation_requested)
                        .unwrap_or(false),
                    cancellation_reason: existing_chunk
                        .as_ref()
                        .and_then(|chunk| chunk.cancellation_reason.clone()),
                    cancellation_metadata: existing_chunk
                        .as_ref()
                        .and_then(|chunk| chunk.cancellation_metadata.clone()),
                    scheduled_at: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.scheduled_at)
                        .unwrap_or(entry.occurred_at),
                    available_at: *available_at,
                    started_at: existing_chunk.as_ref().and_then(|chunk| chunk.started_at),
                    lease_expires_at: None,
                    completed_at: None,
                    updated_at: entry.occurred_at,
                };
                self.write_chunk_state(write_batch, existing_chunk.as_ref(), &state)?;
                let mut batch_state =
                    self.load_batch_state(identity)?.unwrap_or_else(|| LocalBatchState {
                        identity: identity.clone(),
                        definition_id: String::new(),
                        definition_version: None,
                        artifact_hash: None,
                        task_queue: String::new(),
                        execution_policy: None,
                        reducer: None,
                        reducer_class: default_reducer_class(),
                        aggregation_tree_depth: default_aggregation_tree_depth(),
                        fast_lane_enabled: false,
                        aggregation_group_count: 1,
                        total_items: 0,
                        chunk_count: 0,
                        terminal_chunk_count: 0,
                        succeeded_items: 0,
                        failed_items: 0,
                        cancelled_items: 0,
                        status: "running".to_owned(),
                        last_report_id: None,
                        error: None,
                        created_at: entry.occurred_at,
                        updated_at: entry.occurred_at,
                        terminal_at: None,
                    });
                if batch_state.status == "scheduled" {
                    batch_state.status = "running".to_owned();
                }
                batch_state.updated_at = entry.occurred_at;
                write_batch.put(
                    batch_key(identity),
                    encode_rocksdb_value(&batch_state, "requeue-touched batch state")?,
                );
            }
            ThroughputChangelogPayload::BatchTerminal {
                identity,
                status,
                report_id,
                succeeded_items,
                failed_items,
                cancelled_items,
                error,
                terminal_at,
            } => {
                let mut batch_state =
                    self.load_batch_state(identity)?.unwrap_or_else(|| LocalBatchState {
                        identity: identity.clone(),
                        definition_id: String::new(),
                        definition_version: None,
                        artifact_hash: None,
                        task_queue: String::new(),
                        execution_policy: None,
                        reducer: None,
                        reducer_class: default_reducer_class(),
                        aggregation_tree_depth: default_aggregation_tree_depth(),
                        fast_lane_enabled: false,
                        aggregation_group_count: 1,
                        total_items: succeeded_items
                            .saturating_add(*failed_items)
                            .saturating_add(*cancelled_items),
                        chunk_count: 0,
                        terminal_chunk_count: 0,
                        succeeded_items: 0,
                        failed_items: 0,
                        cancelled_items: 0,
                        status: status.clone(),
                        last_report_id: Some(report_id.clone()),
                        error: error.clone(),
                        created_at: entry.occurred_at,
                        updated_at: entry.occurred_at,
                        terminal_at: Some(*terminal_at),
                    });
                batch_state.status = status.clone();
                batch_state.last_report_id = Some(report_id.clone());
                batch_state.succeeded_items = *succeeded_items;
                batch_state.failed_items = *failed_items;
                batch_state.cancelled_items = *cancelled_items;
                batch_state.terminal_chunk_count = batch_state.chunk_count;
                batch_state.error = error.clone();
                batch_state.updated_at = entry.occurred_at;
                batch_state.terminal_at = Some(*terminal_at);
                write_batch.put(
                    batch_key(identity),
                    encode_rocksdb_value(&batch_state, "terminal batch state")?,
                );
            }
            ThroughputChangelogPayload::GroupTerminal {
                identity,
                group_id,
                level,
                parent_group_id,
                status,
                succeeded_items,
                failed_items,
                cancelled_items,
                error,
                terminal_at,
            } => {
                let state = LocalGroupState {
                    identity: identity.clone(),
                    group_id: *group_id,
                    level: *level,
                    parent_group_id: *parent_group_id,
                    status: status.clone(),
                    succeeded_items: *succeeded_items,
                    failed_items: *failed_items,
                    cancelled_items: *cancelled_items,
                    error: error.clone(),
                    terminal_at: *terminal_at,
                };
                self.write_group_state(write_batch, &state)?;
            }
        }
        Ok(())
    }

    fn load_chunk_state(
        &self,
        identity: &ThroughputBatchIdentity,
        chunk_id: &str,
    ) -> Result<Option<LocalChunkState>> {
        self.db
            .get(chunk_key(identity, chunk_id))
            .context("failed to load throughput chunk state")?
            .map(|value| decode_rocksdb_chunk_state(&value))
            .transpose()
    }

    fn build_checkpoint(&self) -> Result<CheckpointFile> {
        Ok(CheckpointFile {
            created_at: Utc::now(),
            offsets: self.load_offsets()?,
            mirrored_entry_ids: self.load_mirrored_entry_ids()?,
            batches: self.load_all_batches()?,
            chunks: self.load_all_chunks()?,
            groups: self.load_all_groups()?,
        })
    }

    fn restore_from_checkpoint_if_empty(&self, checkpoint: CheckpointFile) -> Result<bool> {
        if !self.is_empty()? {
            return Ok(false);
        }
        let mut batch = WriteBatch::default();
        for (partition, offset) in &checkpoint.offsets {
            batch.put(offset_key(*partition), offset.to_string().as_bytes());
        }
        for entry_id in &checkpoint.mirrored_entry_ids {
            batch.put(mirrored_entry_key(entry_id), b"1");
        }
        for state in &checkpoint.batches {
            batch.put(
                batch_key(&state.identity),
                encode_rocksdb_value(state, "checkpoint batch state")?,
            );
        }
        for state in &checkpoint.chunks {
            self.write_chunk_state(&mut batch, None, state)?;
        }
        for state in &checkpoint.groups {
            self.write_group_state(&mut batch, state)?;
        }
        self.db.write(batch).context("failed to restore throughput checkpoint into state db")?;
        let mut meta = self.meta.lock().expect("local throughput meta lock poisoned");
        meta.restored_from_checkpoint = true;
        meta.last_checkpoint_at = Some(checkpoint.created_at);
        Ok(true)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn prune_old_checkpoints(&self) -> Result<()> {
        let mut checkpoints = fs::read_dir(&self.checkpoint_dir)
            .with_context(|| {
                format!("failed to read checkpoint directory {}", self.checkpoint_dir.display())
            })?
            .filter_map(|entry| entry.ok().map(|value| value.path()))
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("checkpoint-") && name.ends_with(".json"))
            })
            .collect::<Vec<_>>();
        checkpoints.sort();
        if checkpoints.len() <= self.checkpoint_retention {
            return Ok(());
        }
        let stale_count = checkpoints.len() - self.checkpoint_retention;
        for stale in checkpoints.into_iter().take(stale_count) {
            fs::remove_file(&stale).with_context(|| {
                format!("failed to remove stale checkpoint {}", stale.display())
            })?;
        }
        Ok(())
    }
}

fn offset_key(partition_id: i32) -> String {
    format!("{OFFSET_KEY_PREFIX}{partition_id}")
}

fn mirrored_entry_key(entry_id: impl std::fmt::Display) -> String {
    format!("{MIRRORED_ENTRY_KEY_PREFIX}{entry_id}")
}

fn batch_key(identity: &ThroughputBatchIdentity) -> String {
    format!(
        "{BATCH_KEY_PREFIX}{}:{}:{}:{}",
        identity.tenant_id, identity.instance_id, identity.run_id, identity.batch_id
    )
}

fn chunk_key(identity: &ThroughputBatchIdentity, chunk_id: &str) -> String {
    format!(
        "{CHUNK_KEY_PREFIX}{}:{}:{}:{}:{}",
        identity.tenant_id, identity.instance_id, identity.run_id, identity.batch_id, chunk_id
    )
}

fn group_key(identity: &ThroughputBatchIdentity, group_id: u32) -> String {
    format!(
        "{GROUP_KEY_PREFIX}{}:{}:{}:{}:{}",
        identity.tenant_id, identity.instance_id, identity.run_id, identity.batch_id, group_id
    )
}

fn derive_terminal_group_from_chunks(
    chunks: &[LocalChunkState],
    group_id: u32,
) -> Option<ProjectedGroupTerminal> {
    let mut saw_group = false;
    let mut succeeded_items = 0_u32;
    let mut failed_items = 0_u32;
    let mut cancelled_items = 0_u32;
    let mut error = None;
    let mut saw_nonterminal = false;
    let mut terminal_at = DateTime::<Utc>::UNIX_EPOCH;

    for chunk in chunks.iter().filter(|chunk| chunk.group_id == group_id) {
        saw_group = true;
        terminal_at = terminal_at.max(chunk.completed_at.unwrap_or(chunk.updated_at));
        match chunk.status.as_str() {
            "completed" => succeeded_items = succeeded_items.saturating_add(chunk.item_count),
            "failed" => {
                failed_items = failed_items.saturating_add(chunk.item_count);
                if error.is_none() {
                    error = chunk.error.clone();
                }
            }
            "cancelled" => {
                cancelled_items = cancelled_items.saturating_add(chunk.item_count);
                if error.is_none() {
                    error = chunk.error.clone();
                }
            }
            _ => saw_nonterminal = true,
        }
    }

    if !saw_group || (failed_items == 0 && cancelled_items == 0 && saw_nonterminal) {
        return None;
    }

    let status = if failed_items > 0 {
        "failed"
    } else if cancelled_items > 0 {
        "cancelled"
    } else if saw_nonterminal {
        return None;
    } else {
        "completed"
    };

    Some(ProjectedGroupTerminal {
        group_id,
        level: reduction_tree_node_level(group_id),
        parent_group_id: None,
        status: status.to_owned(),
        succeeded_items,
        failed_items,
        cancelled_items,
        error,
        terminal_at,
    })
}

fn project_group_terminal_from_loaded(
    batch: &LocalBatchState,
    existing_group: Option<&LocalGroupState>,
    chunks: &[LocalChunkState],
    group_id: u32,
    pending_chunk: Option<(&str, &str, Option<&str>)>,
    occurred_at: DateTime<Utc>,
) -> Option<ProjectedGroupTerminal> {
    if batch.aggregation_group_count <= 1 || existing_group.is_some() {
        return None;
    }
    let chunks = chunks.iter().filter(|chunk| chunk.group_id == group_id).collect::<Vec<_>>();
    if chunks.is_empty() {
        return None;
    }

    let mut succeeded_items = 0u32;
    let mut failed_items = 0u32;
    let mut cancelled_items = 0u32;
    let mut error = None;
    let mut saw_nonterminal = false;
    let mut saw_pending_chunk = pending_chunk.is_none();
    for chunk in &chunks {
        let (status, chunk_error) =
            if let Some((pending_chunk_id, pending_status, pending_error)) = pending_chunk {
                if chunk.chunk_id == pending_chunk_id {
                    saw_pending_chunk = true;
                    (pending_status, pending_error)
                } else {
                    (chunk.status.as_str(), chunk.error.as_deref())
                }
            } else {
                (chunk.status.as_str(), chunk.error.as_deref())
            };
        match status {
            "completed" => succeeded_items = succeeded_items.saturating_add(chunk.item_count),
            "failed" => {
                failed_items = failed_items.saturating_add(chunk.item_count);
                if error.is_none() {
                    error = chunk_error.map(ToOwned::to_owned);
                }
            }
            "cancelled" => {
                cancelled_items = cancelled_items.saturating_add(chunk.item_count);
                if error.is_none() {
                    error = chunk_error.map(ToOwned::to_owned);
                }
            }
            _ => saw_nonterminal = true,
        }
    }
    if !saw_pending_chunk || (failed_items == 0 && cancelled_items == 0 && saw_nonterminal) {
        return None;
    }
    let status = if failed_items > 0 {
        "failed"
    } else if cancelled_items > 0 {
        "cancelled"
    } else if saw_nonterminal {
        return None;
    } else {
        "completed"
    };
    Some(ProjectedGroupTerminal {
        group_id,
        level: 0,
        parent_group_id: reduction_tree_parent_group_id(
            group_id,
            batch.aggregation_group_count,
            batch.aggregation_tree_depth,
        ),
        status: status.to_owned(),
        succeeded_items,
        failed_items,
        cancelled_items,
        error,
        terminal_at: occurred_at,
    })
}

fn project_parent_group_terminals_from_loaded(
    batch: &LocalBatchState,
    groups: &[LocalGroupState],
    pending_group: Option<&ProjectedGroupTerminal>,
    occurred_at: DateTime<Utc>,
) -> Vec<ProjectedGroupTerminal> {
    if reduction_tree_level_counts(batch.aggregation_group_count, batch.aggregation_tree_depth)
        .len()
        <= 1
    {
        return Vec::new();
    }

    let mut projected_groups = groups
        .iter()
        .map(|group| ProjectedGroupTerminal {
            group_id: group.group_id,
            level: group.level,
            parent_group_id: group.parent_group_id,
            status: group.status.clone(),
            succeeded_items: group.succeeded_items,
            failed_items: group.failed_items,
            cancelled_items: group.cancelled_items,
            error: group.error.clone(),
            terminal_at: group.terminal_at,
        })
        .collect::<Vec<_>>();
    if let Some(group) = pending_group {
        upsert_projected_group(&mut projected_groups, group.clone());
    }

    let mut candidates = pending_group
        .and_then(|group| {
            reduction_tree_parent_group_id(
                group.group_id,
                batch.aggregation_group_count,
                batch.aggregation_tree_depth,
            )
        })
        .into_iter()
        .collect::<Vec<_>>();
    let mut derived = Vec::new();

    while let Some(candidate_group_id) = candidates.pop() {
        if let Some(existing) =
            projected_groups.iter().find(|group| group.group_id == candidate_group_id).cloned()
        {
            if let Some(next_parent) = existing.parent_group_id {
                candidates.push(next_parent);
            }
            continue;
        }
        let Some(parent_group) =
            derive_parent_group_terminal(batch, &projected_groups, candidate_group_id, occurred_at)
        else {
            continue;
        };
        let next_parent = parent_group.parent_group_id;
        upsert_projected_group(&mut projected_groups, parent_group.clone());
        derived.push(parent_group);
        if let Some(next_parent) = next_parent {
            candidates.push(next_parent);
        }
    }

    derived.sort_by_key(|group| group.group_id);
    derived
}

fn derive_parent_group_terminal(
    batch: &LocalBatchState,
    groups: &[ProjectedGroupTerminal],
    parent_group_id: u32,
    occurred_at: DateTime<Utc>,
) -> Option<ProjectedGroupTerminal> {
    let child_group_ids = reduction_tree_child_group_ids(
        parent_group_id,
        batch.aggregation_group_count,
        batch.aggregation_tree_depth,
    );
    if child_group_ids.is_empty() {
        return None;
    }

    let children = child_group_ids
        .iter()
        .map(|child_group_id| {
            groups.iter().find(|group| group.group_id == *child_group_id).cloned()
        })
        .collect::<Option<Vec<_>>>()?;

    let mut succeeded_items = 0_u32;
    let mut failed_items = 0_u32;
    let mut cancelled_items = 0_u32;
    let mut error = None;
    let mut terminal_at = occurred_at;
    for child in children {
        succeeded_items = succeeded_items.saturating_add(child.succeeded_items);
        failed_items = failed_items.saturating_add(child.failed_items);
        cancelled_items = cancelled_items.saturating_add(child.cancelled_items);
        if error.is_none() {
            error = child.error.clone();
        }
        terminal_at = terminal_at.max(child.terminal_at);
    }

    let status = if failed_items > 0 {
        "failed"
    } else if cancelled_items > 0 {
        "cancelled"
    } else {
        "completed"
    };

    Some(ProjectedGroupTerminal {
        group_id: parent_group_id,
        level: reduction_tree_node_level(parent_group_id),
        parent_group_id: reduction_tree_parent_group_id(
            parent_group_id,
            batch.aggregation_group_count,
            batch.aggregation_tree_depth,
        ),
        status: status.to_owned(),
        succeeded_items,
        failed_items,
        cancelled_items,
        error,
        terminal_at,
    })
}

fn upsert_projected_group(groups: &mut Vec<ProjectedGroupTerminal>, group: ProjectedGroupTerminal) {
    if let Some(existing) = groups.iter_mut().find(|candidate| candidate.group_id == group.group_id)
    {
        *existing = group;
    } else {
        groups.push(group);
    }
}

fn project_groups_for_batch_from_loaded(
    chunks: &[LocalChunkState],
    groups: &[LocalGroupState],
    pending_group: Option<&ProjectedGroupTerminal>,
    parent_groups: &[ProjectedGroupTerminal],
) -> Vec<ProjectedGroupTerminal> {
    let mut projected_groups = groups
        .iter()
        .map(|group| ProjectedGroupTerminal {
            group_id: group.group_id,
            level: group.level,
            parent_group_id: group.parent_group_id,
            status: group.status.clone(),
            succeeded_items: group.succeeded_items,
            failed_items: group.failed_items,
            cancelled_items: group.cancelled_items,
            error: group.error.clone(),
            terminal_at: group.terminal_at,
        })
        .collect::<Vec<_>>();
    let derived_group_ids = chunks.iter().map(|chunk| chunk.group_id).collect::<HashSet<_>>();
    for group_id in derived_group_ids {
        if projected_groups.iter().any(|group| group.group_id == group_id) {
            continue;
        }
        if let Some(group) = derive_terminal_group_from_chunks(chunks, group_id) {
            projected_groups.push(group);
        }
    }
    if let Some(pending_group) = pending_group {
        if !projected_groups.iter().any(|group| group.group_id == pending_group.group_id) {
            projected_groups.push(pending_group.clone());
        }
    }
    for group in parent_groups {
        if !projected_groups.iter().any(|candidate| candidate.group_id == group.group_id) {
            projected_groups.push(group.clone());
        }
    }
    projected_groups.sort_by_key(|group| group.group_id);
    projected_groups
}

fn summarize_projected_groups(
    groups: &[ProjectedGroupTerminal],
) -> Option<ProjectedBatchGroupSummary> {
    let leaf_groups = groups.iter().filter(|group| group.level == 0).collect::<Vec<_>>();
    if leaf_groups.is_empty() {
        return None;
    }

    let mut succeeded_items = 0_u32;
    let mut failed_items = 0_u32;
    let mut cancelled_items = 0_u32;
    let mut error = None;
    let mut updated_at = DateTime::<Utc>::UNIX_EPOCH;
    for group in leaf_groups {
        succeeded_items = succeeded_items.saturating_add(group.succeeded_items);
        failed_items = failed_items.saturating_add(group.failed_items);
        cancelled_items = cancelled_items.saturating_add(group.cancelled_items);
        if error.is_none() {
            error = group.error.clone();
        }
        updated_at = updated_at.max(group.terminal_at);
    }

    Some(ProjectedBatchGroupSummary {
        succeeded_items,
        failed_items,
        cancelled_items,
        error,
        updated_at,
    })
}

fn project_batch_terminal_from_loaded(
    batch: &LocalBatchState,
    groups: &[ProjectedGroupTerminal],
    occurred_at: DateTime<Utc>,
) -> Option<ProjectedBatchTerminal> {
    if batch.aggregation_group_count <= 1
        || matches!(batch.status.as_str(), "completed" | "failed" | "cancelled")
        || groups.is_empty()
    {
        return None;
    }

    let level_counts =
        reduction_tree_level_counts(batch.aggregation_group_count, batch.aggregation_tree_depth);
    if level_counts.len() > 1 {
        let root_level = (level_counts.len() - 1) as u32;
        let root_group_id = reduction_tree_node_id(root_level, 0);
        if let Some(root) = groups.iter().find(|group| group.group_id == root_group_id) {
            return Some(ProjectedBatchTerminal {
                status: root.status.clone(),
                succeeded_items: root.succeeded_items,
                failed_items: root.failed_items,
                cancelled_items: root.cancelled_items,
                error: root.error.clone().or_else(|| batch.error.clone()),
                terminal_at: root.terminal_at.max(occurred_at),
            });
        }
    }

    let leaf_groups = groups.iter().filter(|group| group.level == 0).collect::<Vec<_>>();
    let mut groups_seen = HashSet::new();
    let mut succeeded_items = 0_u32;
    let mut failed_items = 0_u32;
    let mut cancelled_items = 0_u32;
    let mut batch_error = batch.error.clone();

    for group in leaf_groups {
        groups_seen.insert(group.group_id);
        succeeded_items = succeeded_items.saturating_add(group.succeeded_items);
        failed_items = failed_items.saturating_add(group.failed_items);
        cancelled_items = cancelled_items.saturating_add(group.cancelled_items);
        if batch_error.is_none() {
            batch_error = group.error.clone();
        }
    }

    let status = if failed_items > 0 {
        "failed".to_owned()
    } else if cancelled_items > 0 {
        "cancelled".to_owned()
    } else if u32::try_from(groups_seen.len()).unwrap_or_default() >= batch.aggregation_group_count
        && succeeded_items == batch.total_items
    {
        "completed".to_owned()
    } else {
        return None;
    };

    Some(ProjectedBatchTerminal {
        status,
        succeeded_items,
        failed_items,
        cancelled_items,
        error: batch_error,
        terminal_at: occurred_at,
    })
}

fn derive_batch_state_from_chunk_states(
    batch: &WorkflowBulkBatchRecord,
    chunks: &[LocalChunkState],
) -> LocalBatchState {
    let identity = ThroughputBatchIdentity {
        tenant_id: batch.tenant_id.clone(),
        instance_id: batch.instance_id.clone(),
        run_id: batch.run_id.clone(),
        batch_id: batch.batch_id.clone(),
    };
    let mut succeeded_items = 0_u32;
    let mut failed_items = 0_u32;
    let mut cancelled_items = 0_u32;
    let mut terminal_chunk_count = 0_u32;
    let mut chunk_error = None;
    let mut updated_at = batch.updated_at;
    let mut terminal_at = batch.terminal_at;
    let mut saw_active_chunk = false;

    for chunk in chunks {
        updated_at = updated_at.max(chunk.updated_at);
        match chunk.status.as_str() {
            "completed" => {
                succeeded_items = succeeded_items.saturating_add(chunk.item_count);
                terminal_chunk_count = terminal_chunk_count.saturating_add(1);
                terminal_at = Some(
                    terminal_at
                        .unwrap_or(chunk.completed_at.unwrap_or(chunk.updated_at))
                        .max(chunk.completed_at.unwrap_or(chunk.updated_at)),
                );
                saw_active_chunk = true;
            }
            "failed" => {
                failed_items = failed_items.saturating_add(chunk.item_count);
                terminal_chunk_count = terminal_chunk_count.saturating_add(1);
                chunk_error = chunk_error.or_else(|| chunk.error.clone());
                terminal_at = Some(
                    terminal_at
                        .unwrap_or(chunk.completed_at.unwrap_or(chunk.updated_at))
                        .max(chunk.completed_at.unwrap_or(chunk.updated_at)),
                );
                saw_active_chunk = true;
            }
            "cancelled" => {
                cancelled_items = cancelled_items.saturating_add(chunk.item_count);
                terminal_chunk_count = terminal_chunk_count.saturating_add(1);
                chunk_error = chunk_error.or_else(|| chunk.error.clone());
                terminal_at = Some(
                    terminal_at
                        .unwrap_or(chunk.completed_at.unwrap_or(chunk.updated_at))
                        .max(chunk.completed_at.unwrap_or(chunk.updated_at)),
                );
                saw_active_chunk = true;
            }
            "started" => saw_active_chunk = true,
            _ => {}
        }
    }

    let derived_group_terminals = chunks
        .iter()
        .map(|chunk| chunk.group_id)
        .collect::<HashSet<_>>()
        .into_iter()
        .filter_map(|group_id| derive_terminal_group_from_chunks(chunks, group_id))
        .collect::<Vec<_>>();
    let grouped_terminal = if batch.aggregation_group_count > 1
        && !derived_group_terminals.is_empty()
        && u32::try_from(derived_group_terminals.len()).unwrap_or_default()
            >= batch.aggregation_group_count
    {
        let mut grouped_succeeded = 0_u32;
        let mut grouped_failed = 0_u32;
        let mut grouped_cancelled = 0_u32;
        let mut grouped_error = None;
        let mut grouped_terminal_at = DateTime::<Utc>::UNIX_EPOCH;
        for group in &derived_group_terminals {
            grouped_succeeded = grouped_succeeded.saturating_add(group.succeeded_items);
            grouped_failed = grouped_failed.saturating_add(group.failed_items);
            grouped_cancelled = grouped_cancelled.saturating_add(group.cancelled_items);
            grouped_error = grouped_error.or_else(|| group.error.clone());
            grouped_terminal_at = grouped_terminal_at.max(group.terminal_at);
        }
        let status = if grouped_failed > 0 {
            Some(WorkflowBulkBatchStatus::Failed)
        } else if grouped_cancelled > 0 {
            Some(WorkflowBulkBatchStatus::Cancelled)
        } else if batch.total_items > 0 && grouped_succeeded >= batch.total_items {
            Some(WorkflowBulkBatchStatus::Completed)
        } else {
            None
        };
        status.map(|status| {
            (
                status,
                grouped_succeeded,
                grouped_failed,
                grouped_cancelled,
                grouped_error,
                grouped_terminal_at,
            )
        })
    } else {
        None
    };

    let mut status = batch.status.clone();
    let mut error = batch.error.clone().or(chunk_error.clone());
    if matches!(
        status,
        WorkflowBulkBatchStatus::Completed
            | WorkflowBulkBatchStatus::Failed
            | WorkflowBulkBatchStatus::Cancelled
    ) && batch.terminal_at.is_some()
    {
        succeeded_items = succeeded_items.max(batch.succeeded_items);
        failed_items = failed_items.max(batch.failed_items);
        cancelled_items = cancelled_items.max(batch.cancelled_items);
        terminal_chunk_count = batch.chunk_count;
        terminal_at = batch.terminal_at;
        error = batch.error.clone().or(chunk_error);
    } else if let Some((
        grouped_status,
        grouped_succeeded,
        grouped_failed,
        grouped_cancelled,
        grouped_error,
        grouped_terminal_at,
    )) = grouped_terminal
    {
        status = grouped_status;
        succeeded_items = grouped_succeeded;
        failed_items = grouped_failed;
        cancelled_items = grouped_cancelled;
        terminal_chunk_count = batch.chunk_count;
        terminal_at = Some(grouped_terminal_at);
        error = grouped_error;
    } else if batch.aggregation_group_count <= 1 {
        if failed_items > 0 {
            status = WorkflowBulkBatchStatus::Failed;
            terminal_chunk_count = batch.chunk_count;
            terminal_at = Some(terminal_at.unwrap_or(updated_at));
        } else if cancelled_items > 0 {
            status = WorkflowBulkBatchStatus::Cancelled;
            terminal_chunk_count = batch.chunk_count;
            terminal_at = Some(terminal_at.unwrap_or(updated_at));
        } else if batch.total_items > 0
            && succeeded_items >= batch.total_items
            && terminal_chunk_count >= batch.chunk_count
        {
            status = WorkflowBulkBatchStatus::Completed;
            terminal_chunk_count = batch.chunk_count;
            terminal_at = Some(terminal_at.unwrap_or(updated_at));
            error = None;
        } else if saw_active_chunk {
            status = WorkflowBulkBatchStatus::Running;
            terminal_at = None;
        } else {
            status = WorkflowBulkBatchStatus::Scheduled;
            terminal_at = None;
            error = batch.error.clone();
        }
    } else if saw_active_chunk {
        status = WorkflowBulkBatchStatus::Running;
        terminal_at = None;
    } else {
        status = WorkflowBulkBatchStatus::Scheduled;
        terminal_at = None;
        error = batch.error.clone();
    }

    LocalBatchState {
        identity,
        definition_id: batch.definition_id.clone(),
        definition_version: batch.definition_version,
        artifact_hash: batch.artifact_hash.clone(),
        task_queue: batch.task_queue.clone(),
        execution_policy: batch.execution_policy.clone(),
        reducer: batch.reducer.clone(),
        reducer_class: batch.reducer_class.clone(),
        aggregation_tree_depth: batch.aggregation_tree_depth.max(1),
        fast_lane_enabled: batch.fast_lane_enabled,
        aggregation_group_count: batch.aggregation_group_count.max(1),
        total_items: batch.total_items,
        chunk_count: batch.chunk_count,
        terminal_chunk_count,
        succeeded_items,
        failed_items,
        cancelled_items,
        status: status.as_str().to_owned(),
        last_report_id: None,
        error,
        created_at: batch.scheduled_at,
        updated_at,
        terminal_at,
    }
}

fn infer_ungrouped_batch_terminal_from_chunk_apply(
    batch: &LocalBatchState,
    chunk_status: &str,
) -> Option<&'static str> {
    if batch.aggregation_group_count > 1 {
        return None;
    }

    match chunk_status {
        "failed" => Some(WorkflowBulkBatchStatus::Failed.as_str()),
        "cancelled" => Some(WorkflowBulkBatchStatus::Cancelled.as_str()),
        "completed"
            if batch.total_items > 0
                && batch.succeeded_items >= batch.total_items
                && batch.terminal_chunk_count >= batch.chunk_count =>
        {
            Some(WorkflowBulkBatchStatus::Completed.as_str())
        }
        _ => None,
    }
}

fn batch_chunk_index_prefix(identity: &ThroughputBatchIdentity) -> String {
    format!(
        "{BATCH_CHUNK_INDEX_PREFIX}{}:{}:{}:{}:",
        identity.tenant_id, identity.instance_id, identity.run_id, identity.batch_id
    )
}

fn batch_chunk_index_key(identity: &ThroughputBatchIdentity, chunk_id: &str) -> String {
    format!("{}{}", batch_chunk_index_prefix(identity), chunk_id)
}

fn batch_group_index_prefix(identity: &ThroughputBatchIdentity) -> String {
    format!(
        "{BATCH_GROUP_INDEX_PREFIX}{}:{}:{}:{}:",
        identity.tenant_id, identity.instance_id, identity.run_id, identity.batch_id
    )
}

fn batch_group_index_key(identity: &ThroughputBatchIdentity, group_id: u32) -> String {
    format!("{}{:010}", batch_group_index_prefix(identity), group_id)
}

fn ready_index_scope_prefix(tenant_id: &str, task_queue: &str) -> String {
    format!("{READY_INDEX_PREFIX}{tenant_id}:{task_queue}:")
}

fn ready_index_key(state: &LocalChunkState) -> String {
    format!(
        "{}{}:{}:{}:{:020}:{}:{}:{}:{}:{:010}:{}",
        READY_INDEX_PREFIX,
        state.identity.tenant_id,
        state.task_queue,
        timestamp_sort_key(state.available_at),
        timestamp_sort_key(state.scheduled_at),
        state.identity.instance_id,
        state.identity.run_id,
        state.identity.batch_id,
        state.group_id,
        state.chunk_index,
        state.chunk_id
    )
}

fn started_index_scope_prefix(tenant_id: Option<&str>, task_queue: Option<&str>) -> String {
    match (tenant_id, task_queue) {
        (Some(tenant_id), Some(task_queue)) => {
            format!("{STARTED_INDEX_PREFIX}{tenant_id}:{task_queue}:")
        }
        (Some(tenant_id), None) => format!("{STARTED_INDEX_PREFIX}{tenant_id}:"),
        (None, _) => STARTED_INDEX_PREFIX.to_owned(),
    }
}

fn started_index_key(state: &LocalChunkState) -> String {
    format!(
        "{}{}:{}:{}:{}:{}:{}:{:010}:{}",
        STARTED_INDEX_PREFIX,
        state.identity.tenant_id,
        state.task_queue,
        state.identity.instance_id,
        state.identity.run_id,
        state.identity.batch_id,
        state.group_id,
        state.chunk_index,
        state.chunk_id
    )
}

fn lease_expiry_index_prefix() -> &'static str {
    LEASE_EXPIRY_INDEX_PREFIX
}

fn lease_expiry_index_key(state: &LocalChunkState) -> String {
    format!(
        "{}{}:{}:{}:{}:{}:{}:{:010}:{}",
        LEASE_EXPIRY_INDEX_PREFIX,
        timestamp_sort_key(state.lease_expires_at.unwrap_or(state.updated_at)),
        state.identity.tenant_id,
        state.task_queue,
        state.identity.instance_id,
        state.identity.run_id,
        state.identity.batch_id,
        state.group_id,
        state.chunk_id
    )
}

fn timestamp_sort_key(timestamp: DateTime<Utc>) -> String {
    format!("{:020}", timestamp.timestamp_millis())
}

fn throughput_partition_id(batch_id: &str, group_id: u32, partition_count: i32) -> i32 {
    fabrik_broker::partition_for_key(
        &fabrik_throughput::throughput_partition_key(batch_id, group_id),
        partition_count,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use fabrik_throughput::{
        ThroughputChangelogEntry, ThroughputChangelogPayload, group_id_for_chunk_index,
        reduction_tree_node_id, reduction_tree_parent_group_id,
    };
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
            task_queue: "bulk".to_owned(),
            state: Some("join".to_owned()),
            input_handle: Value::Null,
            result_handle: Value::Null,
            throughput_backend: "stream-v2".to_owned(),
            throughput_backend_version: "2.0.0".to_owned(),
            execution_policy: Some("eager".to_owned()),
            reducer: Some("count".to_owned()),
            reducer_class: "mergeable".to_owned(),
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
    fn local_state_writes_json_rocksdb_values() -> Result<()> {
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

        let batch_bytes =
            state.db.get(batch_key(&identity()))?.expect("batch state should be written");
        let decoded_batch: LocalBatchState = serde_json::from_slice(&batch_bytes)?;
        assert_eq!(decoded_batch.status, WorkflowBulkBatchStatus::Running.as_str());

        let scheduled_bytes = state
            .db
            .get(chunk_key(&identity(), "chunk-a"))?
            .expect("scheduled chunk state should be written");
        let decoded_scheduled: LocalChunkState = serde_json::from_slice(&scheduled_bytes)?;
        assert_eq!(decoded_scheduled.status, WorkflowBulkChunkStatus::Scheduled.as_str());

        let started_bytes = state
            .db
            .get(chunk_key(&identity(), "chunk-b"))?
            .expect("started chunk state should be written");
        let decoded_started: LocalChunkState = serde_json::from_slice(&started_bytes)?;
        assert_eq!(decoded_started.status, WorkflowBulkChunkStatus::Started.as_str());

        let chunk_index_bytes = state
            .db
            .get(batch_chunk_index_key(&identity(), "chunk-a"))?
            .expect("batch chunk index should be written");
        let decoded_chunk_index: BatchChunkIndexEntry = serde_json::from_slice(&chunk_index_bytes)?;
        assert_eq!(decoded_chunk_index.chunk_id, "chunk-a");

        let ready_index_bytes = state
            .db
            .get(ready_index_key(&scheduled_state))?
            .expect("ready index should be written");
        let decoded_ready_index: ReadyChunkIndexEntry = serde_json::from_slice(&ready_index_bytes)?;
        assert_eq!(decoded_ready_index.chunk_id, "chunk-a");

        let started_index_bytes = state
            .db
            .get(started_index_key(&started_state))?
            .expect("started index should be written");
        let decoded_started_index: StartedChunkIndexEntry =
            serde_json::from_slice(&started_index_bytes)?;
        assert_eq!(decoded_started_index.chunk_id, "chunk-b");

        let lease_expiry_bytes = state
            .db
            .get(lease_expiry_index_key(&started_state))?
            .expect("lease expiry index should be written");
        let decoded_lease_expiry: StartedChunkIndexEntry =
            serde_json::from_slice(&lease_expiry_bytes)?;
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

        let group_bytes =
            state.db.get(group_key(&identity(), 0))?.expect("group state should be written");
        let decoded_group: LocalGroupState = serde_json::from_slice(&group_bytes)?;
        assert_eq!(decoded_group.group_id, 0);

        let group_index_bytes = state
            .db
            .get(batch_group_index_key(&identity(), 0))?
            .expect("group index should be written");
        let decoded_group_index: BatchGroupIndexEntry = serde_json::from_slice(&group_index_bytes)?;
        assert_eq!(decoded_group_index.group_id, 0);

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

        let batch_snapshot =
            state.batch_snapshot(&identity)?.expect("legacy json batch state should load");
        assert_eq!(batch_snapshot.status, WorkflowBulkBatchStatus::Running.as_str());
        assert_eq!(batch_snapshot.task_queue, "bulk");

        let chunks = state.load_chunks_for_batch(&identity)?;
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].chunk_id, "chunk-a");
        assert_eq!(chunks[0].status, WorkflowBulkChunkStatus::Scheduled.as_str());

        let groups = state.load_groups_for_batch(&identity)?;
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].group_id, 1);

        let ready = state.load_ready_chunk_entries("tenant-a", "bulk", occurred_at)?;
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].chunk_id, "chunk-a");

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
                result_handle: None,
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
            state.validate_report(
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
            state.validate_report(
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
            state.validate_report(
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
                result_handle: None,
                output: None,
                error: None,
                cancellation_reason: None,
                cancellation_metadata: None,
            }),
        )?;

        let first = state.lease_next_ready_chunk(
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

        let second = state.lease_next_ready_chunk(
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
                result_handle: None,
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
                result_handle: None,
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
            result_handle: None,
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

        let projected = state
            .project_report_apply(&report(ThroughputChunkReportPayload::ChunkCompleted {
                result_handle: Value::Null,
                output: Some(vec![]),
            }))?
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
    fn local_state_defers_grouped_batch_terminal_until_all_groups_finish() -> Result<()> {
        let db_path = temp_path("throughput-state-groups-db");
        let checkpoint_dir = temp_path("throughput-state-groups-checkpoints");
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

        let first_projection = state
            .project_report_apply(&ThroughputChunkReport {
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
            })?
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
                result_handle: None,
                output: None,
                error: Some("boom".to_owned()),
                cancellation_reason: None,
                cancellation_metadata: None,
            }),
        )?;
        let first_group_terminal = state
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
        let early_terminal = state
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
                result_handle: None,
                output: None,
                error: None,
                cancellation_reason: None,
                cancellation_metadata: None,
            }),
        )?;
        let second_group_terminal = state
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

        let terminal = state
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
        let projected = state
            .project_group_terminal_after_chunk_apply(
                &batch_identity,
                0,
                "chunk-g0",
                "failed",
                Some("boom"),
                occurred_at,
            )?
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
                result_handle: None,
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

        let pending_group = state
            .project_group_terminal_after_chunk_apply(
                &batch_identity,
                1,
                "chunk-g1",
                "completed",
                None,
                second_done_at,
            )?
            .expect("pending second group should terminalize");
        let summary = state
            .project_batch_group_summary_with_pending_group(&batch_identity, Some(&pending_group))?
            .expect("pending group should contribute to grouped summary");
        assert_eq!(summary.succeeded_items, 2);
        assert_eq!(summary.failed_items, 0);
        assert_eq!(summary.cancelled_items, 0);

        let terminal = state
            .project_batch_terminal_from_groups_with_pending_group(
                &batch_identity,
                Some(&pending_group),
                second_done_at,
            )?
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
                result_handle: None,
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
                result_handle: None,
                output: None,
                error: Some("boom".to_owned()),
                cancellation_reason: None,
                cancellation_metadata: None,
            }),
        )?;

        let summary = state
            .project_batch_group_summary(&batch_identity)?
            .expect("terminal chunk state should derive grouped summary");
        assert_eq!(summary.succeeded_items, 1);
        assert_eq!(summary.failed_items, 1);
        assert_eq!(summary.cancelled_items, 0);

        let terminal = state
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
                result_handle: None,
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
                result_handle: None,
                output: None,
                error: Some("boom".to_owned()),
                cancellation_reason: None,
                cancellation_metadata: None,
            }),
        )?;

        let projected = state
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
        assert!(state.project_group_terminal(&batch_identity, 0, occurred_at)?.is_none());

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
                result_handle: None,
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
                result_handle: None,
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

        let derived = project_parent_group_terminals_from_loaded(
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
