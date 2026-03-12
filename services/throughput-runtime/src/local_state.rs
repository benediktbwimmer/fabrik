use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use fabrik_store::{WorkflowBulkBatchRecord, WorkflowBulkChunkRecord, WorkflowBulkChunkStatus};
use fabrik_throughput::{
    ThroughputBatchIdentity, ThroughputChangelogEntry, ThroughputChangelogPayload,
    ThroughputChunkReport, ThroughputChunkReportPayload,
};
use rocksdb::{DB, IteratorMode, Options, WriteBatch};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const OFFSET_KEY_PREFIX: &str = "meta:offset:";
const BATCH_KEY_PREFIX: &str = "batch:";
const CHUNK_KEY_PREFIX: &str = "chunk:";
const GROUP_KEY_PREFIX: &str = "group:";
const BATCH_CHUNK_INDEX_PREFIX: &str = "idx:batch-chunk:";
const BATCH_GROUP_INDEX_PREFIX: &str = "idx:batch-group:";
const READY_INDEX_PREFIX: &str = "idx:ready:";
const STARTED_INDEX_PREFIX: &str = "idx:started:";
const LEASE_EXPIRY_INDEX_PREFIX: &str = "idx:lease-expiry:";
const LATEST_CHECKPOINT_FILE: &str = "latest.json";

#[derive(Clone)]
pub struct LocalThroughputState {
    db: Arc<DB>,
    db_path: PathBuf,
    checkpoint_dir: PathBuf,
    #[cfg_attr(not(test), allow(dead_code))]
    checkpoint_retention: usize,
    meta: Arc<Mutex<LocalStateMeta>>,
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
    pub started_chunk_count: u64,
    pub last_applied_offsets: BTreeMap<i32, i64>,
    pub observed_high_watermarks: BTreeMap<i32, i64>,
    pub partition_lag: BTreeMap<i32, i64>,
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
    batches: Vec<LocalBatchState>,
    chunks: Vec<LocalChunkState>,
    groups: Vec<LocalGroupState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalBatchState {
    identity: ThroughputBatchIdentity,
    task_queue: String,
    aggregation_group_count: u32,
    total_items: u32,
    chunk_count: u32,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalChunkState {
    identity: ThroughputBatchIdentity,
    chunk_id: String,
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
    error: Option<String>,
    scheduled_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
    #[serde(default)]
    started_at: Option<DateTime<Utc>>,
    lease_expires_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LocalGroupState {
    identity: ThroughputBatchIdentity,
    group_id: u32,
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
        self.write_entry_payload(&mut write_batch, entry)?;
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
        let mut write_batch = WriteBatch::default();
        self.write_entry_payload(&mut write_batch, entry)?;
        self.db
            .write(write_batch)
            .context("failed to mirror throughput changelog entry into local state")?;
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
            task_queue: batch.task_queue.clone(),
            aggregation_group_count: batch.aggregation_group_count.max(1),
            total_items: batch.total_items,
            chunk_count: batch.chunk_count,
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
            .put(
                batch_key(&state.identity),
                serde_json::to_vec(&state).context("failed to serialize direct batch state")?,
            )
            .context("failed to upsert direct throughput batch state")?;
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
            error: chunk.error.clone(),
            scheduled_at: chunk.scheduled_at,
            available_at: chunk.available_at,
            started_at: chunk.started_at,
            lease_expires_at: chunk.lease_expires_at,
            updated_at: chunk.updated_at,
        };
        let mut write_batch = WriteBatch::default();
        self.write_chunk_state(&mut write_batch, existing.as_ref(), &state)?;
        self.db.write(write_batch).context("failed to upsert direct throughput chunk state")?;
        Ok(())
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
        let Some(batch) = self.load_batch_state(identity)? else {
            return Ok(ReportValidation::RejectMissingChunk);
        };
        if matches!(batch.status.as_str(), "completed" | "failed" | "cancelled") {
            return Ok(ReportValidation::RejectTerminalBatch);
        }
        let Some(chunk) = self.load_chunk_state(identity, chunk_id)? else {
            return Ok(ReportValidation::RejectMissingChunk);
        };
        if chunk.report_id.as_deref() == Some(report_id) {
            return Ok(ReportValidation::RejectAlreadyApplied);
        }
        if chunk.status != WorkflowBulkChunkStatus::Started.as_str() {
            return Ok(ReportValidation::RejectChunkNotStarted);
        }
        if chunk.attempt != attempt {
            return Ok(ReportValidation::RejectAttemptMismatch);
        }
        if chunk.lease_epoch != lease_epoch {
            return Ok(ReportValidation::RejectLeaseEpochMismatch);
        }
        if chunk.lease_token.as_deref() != Some(lease_token) {
            return Ok(ReportValidation::RejectLeaseTokenMismatch);
        }
        if chunk.owner_epoch != owner_epoch {
            return Ok(ReportValidation::RejectOwnerEpochMismatch);
        }
        Ok(ReportValidation::Accept)
    }

    pub fn project_report_apply(
        &self,
        report: &ThroughputChunkReport,
    ) -> Result<Option<ProjectedTerminalApply>> {
        let identity = ThroughputBatchIdentity {
            tenant_id: report.tenant_id.clone(),
            instance_id: report.instance_id.clone(),
            run_id: report.run_id.clone(),
            batch_id: report.batch_id.clone(),
        };
        if self.validate_report(
            &identity,
            &report.chunk_id,
            report.attempt,
            report.lease_epoch,
            &report.lease_token,
            report.owner_epoch,
            &report.report_id,
        )? != ReportValidation::Accept
        {
            return Ok(None);
        }

        let Some(batch) = self.load_batch_state(&identity)? else {
            return Ok(None);
        };
        let Some(chunk) = self.load_chunk_state(&identity, &report.chunk_id)? else {
            return Ok(None);
        };

        let mut projected = ProjectedTerminalApply {
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
        };

        match &report.payload {
            ThroughputChunkReportPayload::ChunkCompleted { .. } => {
                projected.chunk_status = WorkflowBulkChunkStatus::Completed.as_str().to_owned();
                projected.chunk_error = None;
                projected.batch_succeeded_items =
                    projected.batch_succeeded_items.saturating_add(chunk.item_count);
                if batch.total_items > 0 && projected.batch_succeeded_items >= batch.total_items {
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

        Ok(Some(projected))
    }

    pub fn project_batch_terminal_from_groups(
        &self,
        identity: &ThroughputBatchIdentity,
        occurred_at: DateTime<Utc>,
    ) -> Result<Option<ProjectedBatchTerminal>> {
        let Some(batch) = self.load_batch_state(identity)? else {
            return Ok(None);
        };
        if batch.aggregation_group_count <= 1
            || matches!(batch.status.as_str(), "completed" | "failed" | "cancelled")
        {
            return Ok(None);
        }

        let groups = self.load_groups_for_batch(identity)?;
        if groups.is_empty() {
            return Ok(None);
        }

        let mut groups_seen = HashSet::new();
        let mut succeeded_items = 0_u32;
        let mut failed_items = 0_u32;
        let mut cancelled_items = 0_u32;
        let mut batch_error = batch.error.clone();

        for group in &groups {
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
        } else if u32::try_from(groups_seen.len()).unwrap_or_default()
            >= batch.aggregation_group_count
            && succeeded_items == batch.total_items
        {
            "completed".to_owned()
        } else {
            return Ok(None);
        };

        Ok(Some(ProjectedBatchTerminal {
            status,
            succeeded_items,
            failed_items,
            cancelled_items,
            error: batch_error,
            terminal_at: occurred_at,
        }))
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
        if self.load_group_state(identity, group_id)?.is_some() {
            return Ok(None);
        }
        let chunks = self
            .load_chunks_for_batch(identity)?
            .into_iter()
            .filter(|chunk| chunk.group_id == group_id)
            .collect::<Vec<_>>();
        if chunks.is_empty() {
            return Ok(None);
        }
        let mut succeeded_items = 0u32;
        let mut failed_items = 0u32;
        let mut cancelled_items = 0u32;
        let mut error = None;
        let mut saw_nonterminal = false;
        for chunk in &chunks {
            match chunk.status.as_str() {
                "completed" => succeeded_items = succeeded_items.saturating_add(chunk.item_count),
                "failed" => {
                    failed_items = failed_items.saturating_add(chunk.item_count);
                    error = chunk.error.clone();
                }
                "cancelled" => {
                    cancelled_items = cancelled_items.saturating_add(chunk.item_count);
                    error = chunk.error.clone();
                }
                _ => saw_nonterminal = true,
            }
        }
        if failed_items == 0 && cancelled_items == 0 && saw_nonterminal {
            return Ok(None);
        }
        let status = if failed_items > 0 {
            "failed"
        } else if cancelled_items > 0 {
            "cancelled"
        } else if saw_nonterminal {
            return Ok(None);
        } else {
            "completed"
        };
        Ok(Some(ProjectedGroupTerminal {
            group_id,
            status: status.to_owned(),
            succeeded_items,
            failed_items,
            cancelled_items,
            error,
            terminal_at: occurred_at,
        }))
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
            batch_count: self.load_all_batches()?.len(),
            chunk_count: self.load_all_chunks()?.len(),
            started_chunk_count: self.count_started_chunks(None, None, None, None, 1)?,
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

    fn load_batch_state(
        &self,
        identity: &ThroughputBatchIdentity,
    ) -> Result<Option<LocalBatchState>> {
        self.db
            .get(batch_key(identity))
            .context("failed to load throughput batch state")?
            .map(|value| {
                serde_json::from_slice(&value)
                    .context("failed to deserialize throughput batch state")
            })
            .transpose()
    }

    fn load_all_batches(&self) -> Result<Vec<LocalBatchState>> {
        let mut batches = Vec::new();
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, value) = entry.context("failed to iterate throughput state batches")?;
            let key =
                String::from_utf8(key.to_vec()).context("throughput state key is not utf-8")?;
            if key.starts_with(BATCH_KEY_PREFIX) {
                batches.push(
                    serde_json::from_slice(&value)
                        .context("failed to deserialize throughput batch state")?,
                );
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
                chunks.push(
                    serde_json::from_slice(&value)
                        .context("failed to deserialize throughput chunk state")?,
                );
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
            .map(|value| {
                serde_json::from_slice(&value)
                    .context("failed to deserialize throughput group state")
            })
            .transpose()
    }

    fn load_all_groups(&self) -> Result<Vec<LocalGroupState>> {
        let mut groups = Vec::new();
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, value) = entry.context("failed to iterate throughput state groups")?;
            let key =
                String::from_utf8(key.to_vec()).context("throughput state key is not utf-8")?;
            if key.starts_with(GROUP_KEY_PREFIX) {
                groups.push(
                    serde_json::from_slice(&value)
                        .context("failed to deserialize throughput group state")?,
                );
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
            let indexed: ReadyChunkIndexEntry = serde_json::from_slice(&value)
                .context("failed to deserialize ready chunk index entry")?;
            if let Some(chunk) = self.load_chunk_state(&indexed.identity, &indexed.chunk_id)? {
                if chunk.status == WorkflowBulkChunkStatus::Scheduled.as_str()
                    && chunk.available_at <= now
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
            let indexed: StartedChunkIndexEntry = serde_json::from_slice(&value)
                .context("failed to deserialize started chunk index entry")?;
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
            let indexed: StartedChunkIndexEntry = serde_json::from_slice(&value)
                .context("failed to deserialize lease expiry index entry")?;
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
        T: for<'de> Deserialize<'de>,
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
            entries.push(
                serde_json::from_slice(&value)
                    .context("failed to deserialize throughput secondary index entry")?,
            );
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
        write_batch.put(
            chunk_key(&state.identity, &state.chunk_id),
            serde_json::to_vec(state).context("failed to serialize throughput chunk state")?,
        );
        write_batch.put(
            batch_chunk_index_key(&state.identity, &state.chunk_id),
            serde_json::to_vec(&BatchChunkIndexEntry {
                identity: state.identity.clone(),
                chunk_id: state.chunk_id.clone(),
            })
            .context("failed to serialize batch chunk index entry")?,
        );
        if state.status == WorkflowBulkChunkStatus::Scheduled.as_str() {
            write_batch.put(
                ready_index_key(state),
                serde_json::to_vec(&ReadyChunkIndexEntry {
                    identity: state.identity.clone(),
                    chunk_id: state.chunk_id.clone(),
                    group_id: state.group_id,
                })
                .context("failed to serialize ready chunk index entry")?,
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
                serde_json::to_vec(&indexed)
                    .context("failed to serialize started chunk index entry")?,
            );
            if state.lease_expires_at.is_some() {
                write_batch.put(
                    lease_expiry_index_key(state),
                    serde_json::to_vec(&indexed)
                        .context("failed to serialize lease expiry index entry")?,
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
            serde_json::to_vec(state).context("failed to serialize throughput group state")?,
        );
        write_batch.put(
            batch_group_index_key(&state.identity, state.group_id),
            serde_json::to_vec(&BatchGroupIndexEntry {
                identity: state.identity.clone(),
                group_id: state.group_id,
            })
            .context("failed to serialize batch group index entry")?,
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
                aggregation_group_count,
                total_items,
                chunk_count,
            } => {
                let state = LocalBatchState {
                    identity: identity.clone(),
                    task_queue: task_queue.clone(),
                    aggregation_group_count: *aggregation_group_count,
                    total_items: *total_items,
                    chunk_count: *chunk_count,
                    succeeded_items: 0,
                    failed_items: 0,
                    cancelled_items: 0,
                    status: "scheduled".to_owned(),
                    last_report_id: None,
                    error: None,
                    created_at: entry.occurred_at,
                    updated_at: entry.occurred_at,
                    terminal_at: None,
                };
                write_batch.put(
                    batch_key(identity),
                    serde_json::to_vec(&state).context("failed to serialize batch state")?,
                );
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
                        task_queue: String::new(),
                        aggregation_group_count: 1,
                        total_items: 0,
                        chunk_count: 0,
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
                    serde_json::to_vec(&batch_state)
                        .context("failed to serialize leased batch state")?,
                );

                let state = LocalChunkState {
                    identity: identity.clone(),
                    chunk_id: chunk_id.clone(),
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
                    error: existing_chunk.as_ref().and_then(|chunk| chunk.error.clone()),
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
                error,
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
                    error: error.clone(),
                    scheduled_at: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.scheduled_at)
                        .unwrap_or(entry.occurred_at),
                    available_at: *available_at,
                    started_at: existing_chunk.as_ref().and_then(|chunk| chunk.started_at),
                    lease_expires_at: None,
                    updated_at: entry.occurred_at,
                };
                self.write_chunk_state(write_batch, existing_chunk.as_ref(), &state)?;
                let mut batch_state =
                    self.load_batch_state(identity)?.unwrap_or_else(|| LocalBatchState {
                        identity: identity.clone(),
                        task_queue: String::new(),
                        aggregation_group_count: 1,
                        total_items: 0,
                        chunk_count: 0,
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
                        }
                        "failed" => {
                            batch_state.failed_items =
                                batch_state.failed_items.saturating_add(*item_count);
                            batch_state.error = error.clone();
                        }
                        "cancelled" => {
                            batch_state.cancelled_items =
                                batch_state.cancelled_items.saturating_add(*item_count);
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
                write_batch.put(
                    batch_key(identity),
                    serde_json::to_vec(&batch_state)
                        .context("failed to serialize touched batch state")?,
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
                    error: existing_chunk.as_ref().and_then(|chunk| chunk.error.clone()),
                    scheduled_at: existing_chunk
                        .as_ref()
                        .map(|chunk| chunk.scheduled_at)
                        .unwrap_or(entry.occurred_at),
                    available_at: *available_at,
                    started_at: existing_chunk.as_ref().and_then(|chunk| chunk.started_at),
                    lease_expires_at: None,
                    updated_at: entry.occurred_at,
                };
                self.write_chunk_state(write_batch, existing_chunk.as_ref(), &state)?;
                let mut batch_state =
                    self.load_batch_state(identity)?.unwrap_or_else(|| LocalBatchState {
                        identity: identity.clone(),
                        task_queue: String::new(),
                        aggregation_group_count: 1,
                        total_items: 0,
                        chunk_count: 0,
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
                    serde_json::to_vec(&batch_state)
                        .context("failed to serialize requeue-touched batch state")?,
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
                        task_queue: String::new(),
                        aggregation_group_count: 1,
                        total_items: succeeded_items
                            .saturating_add(*failed_items)
                            .saturating_add(*cancelled_items),
                        chunk_count: 0,
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
                batch_state.error = error.clone();
                batch_state.updated_at = entry.occurred_at;
                batch_state.terminal_at = Some(*terminal_at);
                write_batch.put(
                    batch_key(identity),
                    serde_json::to_vec(&batch_state)
                        .context("failed to serialize terminal batch state")?,
                );
            }
            ThroughputChangelogPayload::GroupTerminal {
                identity,
                group_id,
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
            .map(|value| {
                serde_json::from_slice(&value)
                    .context("failed to deserialize throughput chunk state")
            })
            .transpose()
    }

    fn build_checkpoint(&self) -> Result<CheckpointFile> {
        Ok(CheckpointFile {
            created_at: Utc::now(),
            offsets: self.load_offsets()?,
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
        for state in &checkpoint.batches {
            batch.put(
                batch_key(&state.identity),
                serde_json::to_vec(state).context("failed to serialize checkpoint batch state")?,
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
    use fabrik_throughput::{ThroughputChangelogEntry, ThroughputChangelogPayload};
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
                error: None,
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
                error: Some("boom".to_owned()),
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
                error: None,
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
                output: vec![],
            }))?
            .expect("projection should succeed");
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
                error: Some("boom".to_owned()),
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
                error: None,
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
                error: None,
            }),
        )?;
        state.apply_changelog_entry(
            1,
            1,
            &entry(ThroughputChangelogPayload::GroupTerminal {
                identity: batch_identity.clone(),
                group_id: 0,
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
                error: Some("boom".to_owned()),
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
}
