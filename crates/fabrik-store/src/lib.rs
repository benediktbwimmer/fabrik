use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use fabrik_events::{EventEnvelope, WorkflowEvent};
use fabrik_workflow::{CompiledWorkflowArtifact, WorkflowDefinition, WorkflowInstanceState};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{
    PgPool, QueryBuilder, Row,
    postgres::{PgPoolOptions, Postgres},
    types::Json,
};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ScheduledTimer {
    pub partition_id: i32,
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub definition_id: String,
    pub definition_version: Option<u32>,
    pub artifact_hash: Option<String>,
    pub timer_id: String,
    pub state: Option<String>,
    pub fire_at: DateTime<Utc>,
    pub scheduled_event_id: Uuid,
    pub correlation_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
pub struct ConsumedSignalRecord {
    pub signal_id: String,
    pub consumed_event_id: Uuid,
    pub consumed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartitionOwnershipRecord {
    pub partition_id: i32,
    pub owner_id: String,
    pub owner_epoch: u64,
    pub lease_expires_at: DateTime<Utc>,
    pub acquired_at: DateTime<Utc>,
    pub last_transition_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl PartitionOwnershipRecord {
    pub fn is_active_at(&self, now: DateTime<Utc>) -> bool {
        self.lease_expires_at > now
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutorMembershipRecord {
    pub executor_id: String,
    pub query_endpoint: String,
    pub advertised_capacity: usize,
    pub heartbeat_expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartitionAssignmentRecord {
    pub partition_id: i32,
    pub executor_id: String,
    pub assigned_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowStateSnapshot {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub definition_id: String,
    pub definition_version: Option<u32>,
    pub artifact_hash: Option<String>,
    pub snapshot_schema_version: u32,
    pub event_count: i64,
    pub last_event_id: Uuid,
    pub last_event_type: String,
    pub updated_at: DateTime<Utc>,
    pub state: WorkflowInstanceState,
}

const SNAPSHOT_SCHEMA_VERSION: u32 = 1;
const MAX_BULK_ITEMS_PER_BATCH: usize = 100_000;
const MAX_BULK_ITEM_BYTES: usize = 256 * 1024;
const MAX_BULK_TOTAL_INPUT_BYTES: usize = 8 * 1024 * 1024;
const MAX_BULK_CHUNK_SIZE: usize = 1024;
const MAX_BULK_CHUNK_INPUT_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowActivityRecord {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub definition_id: String,
    pub definition_version: Option<u32>,
    pub artifact_hash: Option<String>,
    pub activity_id: String,
    pub attempt: u32,
    pub activity_type: String,
    pub task_queue: String,
    pub state: Option<String>,
    pub status: WorkflowActivityStatus,
    pub input: Value,
    pub config: Option<Value>,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub cancellation_requested: bool,
    pub cancellation_reason: Option<String>,
    pub cancellation_metadata: Option<Value>,
    pub worker_id: Option<String>,
    pub worker_build_id: Option<String>,
    pub scheduled_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub schedule_to_start_timeout_ms: Option<u64>,
    pub start_to_close_timeout_ms: Option<u64>,
    pub heartbeat_timeout_ms: Option<u64>,
    pub last_event_id: Uuid,
    pub last_event_type: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActivityHeartbeatResult {
    pub updated: bool,
    pub cancellation_requested: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ActivityTerminalPayload {
    Completed { output: Value },
    Failed { error: String },
    Cancelled { reason: String, metadata: Option<Value> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActivityTerminalUpdate {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub activity_id: String,
    pub attempt: u32,
    pub worker_id: String,
    pub worker_build_id: String,
    pub event_id: Uuid,
    pub event_type: String,
    pub occurred_at: DateTime<Utc>,
    pub payload: ActivityTerminalPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActivityStartUpdate {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub activity_id: String,
    pub attempt: u32,
    pub worker_id: String,
    pub worker_build_id: String,
    pub lease_expires_at: DateTime<Utc>,
    pub event_id: Uuid,
    pub event_type: String,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActivityScheduleUpdate {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub definition_id: String,
    pub definition_version: Option<u32>,
    pub artifact_hash: Option<String>,
    pub activity_id: String,
    pub attempt: u32,
    pub activity_type: String,
    pub task_queue: String,
    pub state: Option<String>,
    pub input: Value,
    pub config: Option<Value>,
    pub schedule_to_start_timeout_ms: Option<u64>,
    pub start_to_close_timeout_ms: Option<u64>,
    pub heartbeat_timeout_ms: Option<u64>,
    pub event_id: Uuid,
    pub event_type: String,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AppliedActivityStartUpdate {
    pub record: WorkflowActivityRecord,
    pub event_id: Uuid,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AppliedActivityTerminalUpdate {
    pub record: WorkflowActivityRecord,
    pub event_id: Uuid,
    pub occurred_at: DateTime<Utc>,
    pub payload: ActivityTerminalPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowBulkBatchStatus {
    Scheduled,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl WorkflowBulkBatchStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn from_db(value: &str) -> Result<Self> {
        match value {
            "scheduled" => Ok(Self::Scheduled),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => anyhow::bail!("unknown workflow bulk batch status {other}"),
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowBulkChunkStatus {
    Scheduled,
    Started,
    Completed,
    Failed,
    Cancelled,
}

impl WorkflowBulkChunkStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Started => "started",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn from_db(value: &str) -> Result<Self> {
        match value {
            "scheduled" => Ok(Self::Scheduled),
            "started" => Ok(Self::Started),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => anyhow::bail!("unknown workflow bulk chunk status {other}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowBulkBatchRecord {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub definition_id: String,
    pub definition_version: Option<u32>,
    pub artifact_hash: Option<String>,
    pub batch_id: String,
    pub activity_type: String,
    pub task_queue: String,
    pub state: Option<String>,
    pub input_handle: Value,
    pub result_handle: Value,
    pub throughput_backend: String,
    pub throughput_backend_version: String,
    pub execution_policy: Option<String>,
    pub reducer: Option<String>,
    pub reducer_class: String,
    pub aggregation_tree_depth: u32,
    pub fast_lane_enabled: bool,
    pub aggregation_group_count: u32,
    pub status: WorkflowBulkBatchStatus,
    pub total_items: u32,
    pub chunk_size: u32,
    pub chunk_count: u32,
    pub succeeded_items: u32,
    pub failed_items: u32,
    pub cancelled_items: u32,
    pub max_attempts: u32,
    pub retry_delay_ms: u64,
    pub error: Option<String>,
    pub scheduled_at: DateTime<Utc>,
    pub terminal_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowBulkChunkRecord {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub definition_id: String,
    pub definition_version: Option<u32>,
    pub artifact_hash: Option<String>,
    pub batch_id: String,
    pub chunk_id: String,
    pub chunk_index: u32,
    pub group_id: u32,
    pub item_count: u32,
    pub activity_type: String,
    pub task_queue: String,
    pub state: Option<String>,
    pub status: WorkflowBulkChunkStatus,
    pub attempt: u32,
    pub lease_epoch: u64,
    pub owner_epoch: u64,
    pub max_attempts: u32,
    pub retry_delay_ms: u64,
    pub input_handle: Value,
    pub result_handle: Value,
    pub items: Vec<Value>,
    pub output: Option<Vec<Value>>,
    pub error: Option<String>,
    pub cancellation_requested: bool,
    pub cancellation_reason: Option<String>,
    pub cancellation_metadata: Option<Value>,
    pub worker_id: Option<String>,
    pub worker_build_id: Option<String>,
    pub lease_token: Option<Uuid>,
    pub last_report_id: Option<String>,
    pub scheduled_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BulkChunkTerminalPayload {
    Completed { output: Vec<Value> },
    Failed { error: String },
    Cancelled { reason: String, metadata: Option<Value> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BulkChunkTerminalUpdate {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub batch_id: String,
    pub chunk_id: String,
    pub chunk_index: u32,
    pub group_id: u32,
    pub attempt: u32,
    pub lease_epoch: u64,
    pub owner_epoch: u64,
    pub report_id: String,
    pub lease_token: Uuid,
    pub worker_id: String,
    pub worker_build_id: String,
    pub occurred_at: DateTime<Utc>,
    pub payload: BulkChunkTerminalPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AppliedBulkChunkTerminalUpdate {
    pub chunk: WorkflowBulkChunkRecord,
    pub batch: WorkflowBulkBatchRecord,
    pub terminal_event: Option<EventEnvelope<WorkflowEvent>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ThroughputProjectionBatchStateUpdate {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub batch_id: String,
    pub status: String,
    pub succeeded_items: u32,
    pub failed_items: u32,
    pub cancelled_items: u32,
    pub error: Option<String>,
    pub terminal_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ThroughputProjectionChunkStateUpdate {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub batch_id: String,
    pub chunk_id: String,
    pub chunk_index: u32,
    pub group_id: u32,
    pub item_count: u32,
    pub status: String,
    pub attempt: u32,
    pub lease_epoch: u64,
    pub owner_epoch: u64,
    pub input_handle: Option<Value>,
    pub result_handle: Option<Value>,
    pub output: Option<Vec<Value>>,
    pub error: Option<String>,
    pub cancellation_requested: bool,
    pub cancellation_reason: Option<String>,
    pub cancellation_metadata: Option<Value>,
    pub worker_id: Option<String>,
    pub worker_build_id: Option<String>,
    pub lease_token: Option<Uuid>,
    pub last_report_id: Option<String>,
    pub available_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ThroughputProjectionEvent {
    UpsertBatch { batch: WorkflowBulkBatchRecord },
    UpsertChunk { chunk: WorkflowBulkChunkRecord },
    UpdateBatchState { update: ThroughputProjectionBatchStateUpdate },
    UpdateChunkState { update: ThroughputProjectionChunkStateUpdate },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CancelledBulkBatch {
    pub batch: WorkflowBulkBatchRecord,
    pub cancelled_chunks: Vec<WorkflowBulkChunkRecord>,
    pub terminal_event: Option<EventEnvelope<WorkflowEvent>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowEventOutboxRecord {
    pub outbox_id: Uuid,
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub event_type: String,
    pub partition_key: String,
    pub event: EventEnvelope<WorkflowEvent>,
    pub available_at: DateTime<Utc>,
    pub leased_by: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub published_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowRunRecord {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub definition_id: String,
    pub definition_version: Option<u32>,
    pub artifact_hash: Option<String>,
    pub workflow_task_queue: String,
    pub sticky_workflow_build_id: Option<String>,
    pub sticky_workflow_poller_id: Option<String>,
    pub sticky_updated_at: Option<DateTime<Utc>>,
    pub previous_run_id: Option<String>,
    pub next_run_id: Option<String>,
    pub continue_reason: Option<String>,
    pub trigger_event_id: Uuid,
    pub continued_event_id: Option<Uuid>,
    pub triggered_by_run_id: Option<String>,
    pub started_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TaskQueueKind {
    Workflow,
    Activity,
}

impl TaskQueueKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Workflow => "workflow",
            Self::Activity => "activity",
        }
    }

    fn from_db(value: &str) -> Result<Self> {
        match value {
            "workflow" => Ok(Self::Workflow),
            "activity" => Ok(Self::Activity),
            other => anyhow::bail!("unknown task queue kind {other}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskQueueBuildRecord {
    pub tenant_id: String,
    pub queue_kind: TaskQueueKind,
    pub task_queue: String,
    pub build_id: String,
    pub artifact_hashes: Vec<String>,
    pub metadata: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompatibilitySetRecord {
    pub tenant_id: String,
    pub queue_kind: TaskQueueKind,
    pub task_queue: String,
    pub set_id: String,
    pub build_ids: Vec<String>,
    pub is_default: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskQueueThroughputPolicyRecord {
    pub tenant_id: String,
    pub queue_kind: TaskQueueKind,
    pub task_queue: String,
    pub backend: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueuePollerRecord {
    pub tenant_id: String,
    pub queue_kind: TaskQueueKind,
    pub task_queue: String,
    pub poller_id: String,
    pub build_id: String,
    pub partition_id: Option<i32>,
    pub metadata: Option<Value>,
    pub last_seen_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowTaskStatus {
    Pending,
    Leased,
    Completed,
}

impl WorkflowTaskStatus {
    fn from_db(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "leased" => Ok(Self::Leased),
            "completed" => Ok(Self::Completed),
            other => anyhow::bail!("unknown workflow task status {other}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowMailboxStatus {
    Queued,
    Claimed,
    Consumed,
}

impl WorkflowMailboxStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Claimed => "claimed",
            Self::Consumed => "consumed",
        }
    }

    fn from_db(value: &str) -> Result<Self> {
        match value {
            "queued" => Ok(Self::Queued),
            "claimed" => Ok(Self::Claimed),
            "consumed" => Ok(Self::Consumed),
            other => anyhow::bail!("unknown workflow mailbox status {other}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowMailboxKind {
    Trigger,
    Signal,
    Update,
    CancelRequest,
}

impl WorkflowMailboxKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Trigger => "trigger",
            Self::Signal => "signal",
            Self::Update => "update",
            Self::CancelRequest => "cancel_request",
        }
    }

    fn from_db(value: &str) -> Result<Self> {
        match value {
            "trigger" => Ok(Self::Trigger),
            "signal" => Ok(Self::Signal),
            "update" => Ok(Self::Update),
            "cancel_request" => Ok(Self::CancelRequest),
            other => anyhow::bail!("unknown workflow mailbox kind {other}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowMailboxRecord {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub accepted_seq: u64,
    pub kind: WorkflowMailboxKind,
    pub message_id: Option<String>,
    pub message_name: Option<String>,
    pub payload: Option<Value>,
    pub source_event_id: Uuid,
    pub source_event_type: String,
    pub source_event: EventEnvelope<WorkflowEvent>,
    pub status: WorkflowMailboxStatus,
    pub consumed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowResumeStatus {
    Queued,
    Claimed,
    Consumed,
}

impl WorkflowResumeStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Claimed => "claimed",
            Self::Consumed => "consumed",
        }
    }

    fn from_db(value: &str) -> Result<Self> {
        match value {
            "queued" => Ok(Self::Queued),
            "claimed" => Ok(Self::Claimed),
            "consumed" => Ok(Self::Consumed),
            other => anyhow::bail!("unknown workflow resume status {other}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowResumeKind {
    TimerFired,
    ActivityTerminal,
    ChildTerminal,
}

impl WorkflowResumeKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::TimerFired => "timer_fired",
            Self::ActivityTerminal => "activity_terminal",
            Self::ChildTerminal => "child_terminal",
        }
    }

    fn from_db(value: &str) -> Result<Self> {
        match value {
            "timer_fired" => Ok(Self::TimerFired),
            "activity_terminal" => Ok(Self::ActivityTerminal),
            "child_terminal" => Ok(Self::ChildTerminal),
            other => anyhow::bail!("unknown workflow resume kind {other}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowResumeRecord {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub resume_seq: u64,
    pub kind: WorkflowResumeKind,
    pub ref_id: String,
    pub status_label: Option<String>,
    pub source_event_id: Uuid,
    pub source_event_type: String,
    pub source_event: EventEnvelope<WorkflowEvent>,
    pub status: WorkflowResumeStatus,
    pub consumed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowTaskRecord {
    pub task_id: Uuid,
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub definition_id: String,
    pub definition_version: Option<u32>,
    pub artifact_hash: Option<String>,
    pub partition_id: i32,
    pub task_queue: String,
    pub preferred_build_id: Option<String>,
    pub status: WorkflowTaskStatus,
    pub source_event_id: Uuid,
    pub source_event_type: String,
    pub source_event: EventEnvelope<WorkflowEvent>,
    pub mailbox_consumed_seq: u64,
    pub resume_consumed_seq: u64,
    pub mailbox_high_watermark: u64,
    pub resume_high_watermark: u64,
    pub mailbox_backlog: u64,
    pub resume_backlog: u64,
    pub lease_poller_id: Option<String>,
    pub lease_build_id: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub attempt_count: u32,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StickyQueueEffectiveness {
    pub sticky_dispatch_count: u64,
    pub sticky_hit_count: u64,
    pub sticky_fallback_count: u64,
    pub sticky_hit_rate: f64,
    pub sticky_fallback_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskQueueInspection {
    pub tenant_id: String,
    pub queue_kind: TaskQueueKind,
    pub task_queue: String,
    pub backlog: u64,
    pub oldest_backlog_at: Option<DateTime<Utc>>,
    pub sticky_effectiveness: Option<StickyQueueEffectiveness>,
    pub resume_coalescing: Option<ResumeCoalescingMetrics>,
    pub activity_completion_metrics: Option<ActivityCompletionMetrics>,
    pub default_set_id: Option<String>,
    pub throughput_policy: Option<TaskQueueThroughputPolicyRecord>,
    pub compatible_build_ids: Vec<String>,
    pub registered_builds: Vec<TaskQueueBuildRecord>,
    pub compatibility_sets: Vec<CompatibilitySetRecord>,
    pub pollers: Vec<QueuePollerRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResumeCoalescingMetrics {
    pub workflow_task_rows: u64,
    pub queued_resume_rows: u64,
    pub total_resume_rows: u64,
    pub resume_rows_per_task_row: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActivityCompletionMetrics {
    pub completed: u64,
    pub failed: u64,
    pub cancelled: u64,
    pub timed_out: u64,
    pub avg_schedule_to_start_latency_ms: f64,
    pub max_schedule_to_start_latency_ms: u64,
    pub avg_start_to_close_latency_ms: f64,
    pub max_start_to_close_latency_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct QueryRetentionPruneResult {
    pub pruned_runs: u64,
    pub pruned_activities: u64,
    pub pruned_signals: u64,
    pub pruned_snapshots: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueryRetentionCutoffs {
    pub run_closed_before: Option<DateTime<Utc>>,
    pub activity_run_closed_before: Option<DateTime<Utc>>,
    pub signal_run_closed_before: Option<DateTime<Utc>>,
    pub snapshot_run_closed_before: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowSignalStatus {
    Queued,
    Dispatching,
    Consumed,
}

impl WorkflowSignalStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Dispatching => "dispatching",
            Self::Consumed => "consumed",
        }
    }

    fn from_db(value: &str) -> Result<Self> {
        match value {
            "queued" => Ok(Self::Queued),
            "dispatching" => Ok(Self::Dispatching),
            "consumed" => Ok(Self::Consumed),
            other => anyhow::bail!("unknown workflow signal status {other}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowSignalRecord {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub signal_id: String,
    pub signal_type: String,
    pub dedupe_key: Option<String>,
    pub payload: Value,
    pub status: WorkflowSignalStatus,
    pub source_event_id: Uuid,
    pub dispatch_event_id: Option<Uuid>,
    pub consumed_event_id: Option<Uuid>,
    pub enqueued_at: DateTime<Utc>,
    pub consumed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowUpdateStatus {
    Requested,
    Accepted,
    Completed,
    Rejected,
}

impl WorkflowUpdateStatus {
    fn from_db(value: &str) -> Result<Self> {
        match value {
            "requested" => Ok(Self::Requested),
            "accepted" => Ok(Self::Accepted),
            "completed" => Ok(Self::Completed),
            "rejected" => Ok(Self::Rejected),
            other => anyhow::bail!("unknown workflow update status {other}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowUpdateRecord {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub update_id: String,
    pub update_name: String,
    pub request_id: Option<String>,
    pub payload: Value,
    pub status: WorkflowUpdateStatus,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub source_event_id: Uuid,
    pub accepted_event_id: Option<Uuid>,
    pub completed_event_id: Option<Uuid>,
    pub requested_at: DateTime<Utc>,
    pub accepted_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkflowChildRecord {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub child_id: String,
    pub child_workflow_id: String,
    pub child_definition_id: String,
    pub child_run_id: Option<String>,
    pub parent_close_policy: String,
    pub input: Value,
    pub status: String,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub source_event_id: Uuid,
    pub started_event_id: Option<Uuid>,
    pub terminal_event_id: Option<Uuid>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RequestDedupRecord {
    pub tenant_id: String,
    pub instance_id: Option<String>,
    pub operation: String,
    pub request_id: String,
    pub request_hash: String,
    pub response: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowActivityStatus {
    Scheduled,
    Started,
    Completed,
    Failed,
    TimedOut,
    Cancelled,
}

impl WorkflowActivityStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Started => "started",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
        }
    }

    fn from_db(value: &str) -> Result<Self> {
        match value {
            "scheduled" => Ok(Self::Scheduled),
            "started" => Ok(Self::Started),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "timed_out" => Ok(Self::TimedOut),
            "cancelled" => Ok(Self::Cancelled),
            other => anyhow::bail!("unknown workflow activity status {other}"),
        }
    }
}

#[derive(Clone)]
pub struct WorkflowStore {
    pool: PgPool,
}

impl WorkflowStore {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
            .context("failed to connect to postgres")?;

        Ok(Self { pool })
    }

    pub async fn init(&self) -> Result<()> {
        const INIT_LOCK_KEY: i64 = 0x4641_4252_494b;
        let mut lock_conn = self
            .pool
            .acquire()
            .await
            .context("failed to acquire workflow store init connection")?;

        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(INIT_LOCK_KEY)
            .execute(&mut *lock_conn)
            .await
            .context("failed to acquire workflow store init lock")?;

        let result: Result<()> = async {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_instances (
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                workflow_id TEXT NOT NULL,
                status TEXT NOT NULL,
                event_count BIGINT NOT NULL,
                last_event_id UUID NOT NULL,
                last_event_type TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                state JSONB NOT NULL,
                PRIMARY KEY (tenant_id, workflow_instance_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_instances table")?;

        sqlx::query("ALTER TABLE workflow_instances ADD COLUMN IF NOT EXISTS run_id TEXT")
            .execute(&self.pool)
            .await
            .context("failed to add workflow_instances.run_id")?;

        sqlx::query(
            "ALTER TABLE workflow_instances ADD COLUMN IF NOT EXISTS definition_version INTEGER",
        )
        .execute(&self.pool)
        .await
        .context("failed to add workflow_instances.definition_version")?;

        sqlx::query("ALTER TABLE workflow_instances ADD COLUMN IF NOT EXISTS artifact_hash TEXT")
            .execute(&self.pool)
            .await
            .context("failed to add workflow_instances.artifact_hash")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS processed_workflow_events (
                event_id UUID PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                dedupe_key TEXT,
                event_type TEXT NOT NULL,
                processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize processed_workflow_events table")?;

        sqlx::query(
            "ALTER TABLE processed_workflow_events ADD COLUMN IF NOT EXISTS dedupe_key TEXT",
        )
        .execute(&self.pool)
        .await
        .context("failed to add processed_workflow_events.dedupe_key")?;

        sqlx::query(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS processed_workflow_events_dedupe_idx
            ON processed_workflow_events (tenant_id, workflow_instance_id, dedupe_key)
            WHERE dedupe_key IS NOT NULL
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize processed_workflow_events dedupe index")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_definitions (
                tenant_id TEXT NOT NULL,
                workflow_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                definition JSONB NOT NULL,
                PRIMARY KEY (tenant_id, workflow_id, version)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_definitions table")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_artifacts (
                tenant_id TEXT NOT NULL,
                workflow_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                artifact JSONB NOT NULL,
                PRIMARY KEY (tenant_id, workflow_id, version)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_artifacts table")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_timers (
                partition_id INTEGER NOT NULL DEFAULT 0,
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                workflow_id TEXT NOT NULL,
                workflow_version INTEGER,
                timer_id TEXT NOT NULL,
                state TEXT,
                fire_at TIMESTAMPTZ NOT NULL,
                scheduled_event_id UUID NOT NULL,
                correlation_id UUID,
                dispatched_at TIMESTAMPTZ,
                PRIMARY KEY (tenant_id, workflow_instance_id, timer_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_timers table")?;

        sqlx::query("ALTER TABLE workflow_timers ADD COLUMN IF NOT EXISTS partition_id INTEGER NOT NULL DEFAULT 0")
            .execute(&self.pool)
            .await
            .context("failed to add workflow_timers.partition_id")?;

        sqlx::query("ALTER TABLE workflow_timers ADD COLUMN IF NOT EXISTS run_id TEXT")
            .execute(&self.pool)
            .await
            .context("failed to add workflow_timers.run_id")?;

        sqlx::query("ALTER TABLE workflow_timers ADD COLUMN IF NOT EXISTS artifact_hash TEXT")
            .execute(&self.pool)
            .await
            .context("failed to add workflow_timers.artifact_hash")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_state_snapshots (
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                workflow_id TEXT NOT NULL,
                definition_version INTEGER,
                artifact_hash TEXT,
                snapshot_schema_version INTEGER NOT NULL,
                event_count BIGINT NOT NULL,
                last_event_id UUID NOT NULL,
                last_event_type TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                state JSONB NOT NULL,
                PRIMARY KEY (tenant_id, workflow_instance_id, run_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_state_snapshots table")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_activities (
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                workflow_id TEXT NOT NULL,
                workflow_version INTEGER,
                artifact_hash TEXT,
                activity_id TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                activity_type TEXT NOT NULL,
                task_queue TEXT NOT NULL,
                state TEXT,
                status TEXT NOT NULL,
                input JSONB NOT NULL,
                config JSONB,
                output JSONB,
                error TEXT,
                cancellation_requested BOOLEAN NOT NULL DEFAULT FALSE,
                cancellation_reason TEXT,
                cancellation_metadata JSONB,
                worker_id TEXT,
                worker_build_id TEXT,
                lease_token UUID,
                scheduled_at TIMESTAMPTZ NOT NULL,
                started_at TIMESTAMPTZ,
                last_heartbeat_at TIMESTAMPTZ,
                lease_expires_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ,
                schedule_to_start_timeout_ms BIGINT,
                start_to_close_timeout_ms BIGINT,
                heartbeat_timeout_ms BIGINT,
                last_event_id UUID NOT NULL,
                last_event_type TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tenant_id, workflow_instance_id, run_id, activity_id, attempt)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_activities table")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_bulk_batches (
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                workflow_id TEXT NOT NULL,
                workflow_version INTEGER,
                artifact_hash TEXT,
                batch_id TEXT NOT NULL,
                activity_type TEXT NOT NULL,
                task_queue TEXT NOT NULL,
                state TEXT,
                input_handle JSONB NOT NULL DEFAULT '{}'::jsonb,
                result_handle JSONB NOT NULL DEFAULT '{}'::jsonb,
                throughput_backend TEXT NOT NULL DEFAULT 'pg-v1',
                throughput_backend_version TEXT NOT NULL DEFAULT '1.0.0',
                execution_policy TEXT,
                reducer TEXT,
                reducer_class TEXT NOT NULL DEFAULT 'legacy',
                aggregation_tree_depth INTEGER NOT NULL DEFAULT 1,
                fast_lane_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                aggregation_group_count INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL,
                total_items INTEGER NOT NULL,
                chunk_size INTEGER NOT NULL,
                chunk_count INTEGER NOT NULL,
                succeeded_items INTEGER NOT NULL DEFAULT 0,
                failed_items INTEGER NOT NULL DEFAULT 0,
                cancelled_items INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL,
                retry_delay_ms BIGINT NOT NULL DEFAULT 0,
                error TEXT,
                scheduled_at TIMESTAMPTZ NOT NULL,
                terminal_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tenant_id, workflow_instance_id, run_id, batch_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_bulk_batches table")?;

        sqlx::query(
            r#"
            ALTER TABLE workflow_bulk_batches
            ADD COLUMN IF NOT EXISTS input_handle JSONB NOT NULL DEFAULT '{}'::jsonb,
            ADD COLUMN IF NOT EXISTS result_handle JSONB NOT NULL DEFAULT '{}'::jsonb,
            ADD COLUMN IF NOT EXISTS throughput_backend TEXT NOT NULL DEFAULT 'pg-v1',
            ADD COLUMN IF NOT EXISTS throughput_backend_version TEXT NOT NULL DEFAULT '1.0.0',
            ADD COLUMN IF NOT EXISTS execution_policy TEXT,
            ADD COLUMN IF NOT EXISTS reducer TEXT,
            ADD COLUMN IF NOT EXISTS reducer_class TEXT NOT NULL DEFAULT 'legacy',
            ADD COLUMN IF NOT EXISTS aggregation_tree_depth INTEGER NOT NULL DEFAULT 1,
            ADD COLUMN IF NOT EXISTS fast_lane_enabled BOOLEAN NOT NULL DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS aggregation_group_count INTEGER NOT NULL DEFAULT 1
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to ensure workflow_bulk_batches throughput metadata columns")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS workflow_bulk_batches_run_idx
            ON workflow_bulk_batches (tenant_id, workflow_instance_id, run_id, scheduled_at, batch_id)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_bulk_batches run index")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_bulk_chunks (
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                workflow_id TEXT NOT NULL,
                workflow_version INTEGER,
                artifact_hash TEXT,
                batch_id TEXT NOT NULL,
                chunk_id TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                group_id INTEGER NOT NULL DEFAULT 0,
                item_count INTEGER NOT NULL DEFAULT 0,
                activity_type TEXT NOT NULL,
                task_queue TEXT NOT NULL,
                state TEXT,
                status TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                lease_epoch BIGINT NOT NULL DEFAULT 0,
                owner_epoch BIGINT NOT NULL DEFAULT 1,
                max_attempts INTEGER NOT NULL,
                retry_delay_ms BIGINT NOT NULL DEFAULT 0,
                input_handle JSONB NOT NULL DEFAULT 'null'::jsonb,
                result_handle JSONB NOT NULL DEFAULT 'null'::jsonb,
                items JSONB NOT NULL,
                output JSONB,
                error TEXT,
                cancellation_requested BOOLEAN NOT NULL DEFAULT FALSE,
                cancellation_reason TEXT,
                cancellation_metadata JSONB,
                worker_id TEXT,
                worker_build_id TEXT,
                lease_token UUID,
                last_report_id TEXT,
                scheduled_at TIMESTAMPTZ NOT NULL,
                available_at TIMESTAMPTZ NOT NULL,
                started_at TIMESTAMPTZ,
                lease_expires_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tenant_id, workflow_instance_id, run_id, batch_id, chunk_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_bulk_chunks table")?;

        sqlx::query(
            r#"
            ALTER TABLE workflow_bulk_chunks
            ADD COLUMN IF NOT EXISTS lease_token UUID,
            ADD COLUMN IF NOT EXISTS group_id INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS item_count INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS lease_epoch BIGINT NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS owner_epoch BIGINT NOT NULL DEFAULT 1,
            ADD COLUMN IF NOT EXISTS last_report_id TEXT,
            ADD COLUMN IF NOT EXISTS input_handle JSONB NOT NULL DEFAULT 'null'::jsonb,
            ADD COLUMN IF NOT EXISTS result_handle JSONB NOT NULL DEFAULT 'null'::jsonb
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to ensure workflow_bulk_chunks throughput columns")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS workflow_bulk_chunks_queue_idx
            ON workflow_bulk_chunks (
                tenant_id,
                task_queue,
                status,
                available_at,
                scheduled_at,
                chunk_index
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_bulk_chunks queue index")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS workflow_bulk_chunks_batch_idx
            ON workflow_bulk_chunks (tenant_id, workflow_instance_id, run_id, batch_id, chunk_index)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_bulk_chunks batch index")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS throughput_bridge_progress (
                workflow_event_id UUID PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                throughput_backend TEXT NOT NULL,
                dedupe_key TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                command_published_at TIMESTAMPTZ
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize throughput_bridge_progress table")?;

        sqlx::query(
            "ALTER TABLE throughput_bridge_progress ADD COLUMN IF NOT EXISTS command_published_at TIMESTAMPTZ",
        )
        .execute(&self.pool)
        .await
        .context("failed to add throughput_bridge_progress.command_published_at")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS throughput_projection_batches (
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                workflow_id TEXT NOT NULL,
                workflow_version INTEGER,
                artifact_hash TEXT,
                batch_id TEXT NOT NULL,
                activity_type TEXT NOT NULL,
                task_queue TEXT NOT NULL,
                state TEXT,
                input_handle JSONB NOT NULL DEFAULT '{}'::jsonb,
                result_handle JSONB NOT NULL DEFAULT '{}'::jsonb,
                throughput_backend TEXT NOT NULL,
                throughput_backend_version TEXT NOT NULL,
                execution_policy TEXT,
                reducer TEXT,
                reducer_class TEXT NOT NULL DEFAULT 'legacy',
                aggregation_tree_depth INTEGER NOT NULL DEFAULT 1,
                fast_lane_enabled BOOLEAN NOT NULL DEFAULT FALSE,
                aggregation_group_count INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL,
                total_items INTEGER NOT NULL,
                chunk_size INTEGER NOT NULL,
                chunk_count INTEGER NOT NULL,
                succeeded_items INTEGER NOT NULL DEFAULT 0,
                failed_items INTEGER NOT NULL DEFAULT 0,
                cancelled_items INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL,
                retry_delay_ms BIGINT NOT NULL DEFAULT 0,
                error TEXT,
                scheduled_at TIMESTAMPTZ NOT NULL,
                terminal_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tenant_id, workflow_instance_id, run_id, batch_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize throughput_projection_batches table")?;

        sqlx::query(
            r#"
            ALTER TABLE throughput_projection_batches
            ADD COLUMN IF NOT EXISTS execution_policy TEXT,
            ADD COLUMN IF NOT EXISTS reducer TEXT,
            ADD COLUMN IF NOT EXISTS reducer_class TEXT NOT NULL DEFAULT 'legacy',
            ADD COLUMN IF NOT EXISTS aggregation_tree_depth INTEGER NOT NULL DEFAULT 1,
            ADD COLUMN IF NOT EXISTS fast_lane_enabled BOOLEAN NOT NULL DEFAULT FALSE
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to ensure throughput_projection_batches fast lane columns")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS throughput_projection_batches_run_idx
            ON throughput_projection_batches (tenant_id, workflow_instance_id, run_id, scheduled_at, batch_id)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize throughput_projection_batches run index")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS throughput_projection_chunks (
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                workflow_id TEXT NOT NULL,
                workflow_version INTEGER,
                artifact_hash TEXT,
                batch_id TEXT NOT NULL,
                chunk_id TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                group_id INTEGER NOT NULL DEFAULT 0,
                item_count INTEGER NOT NULL DEFAULT 0,
                activity_type TEXT NOT NULL,
                task_queue TEXT NOT NULL,
                state TEXT,
                status TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                lease_epoch BIGINT NOT NULL DEFAULT 0,
                owner_epoch BIGINT NOT NULL DEFAULT 1,
                max_attempts INTEGER NOT NULL,
                retry_delay_ms BIGINT NOT NULL DEFAULT 0,
                input_handle JSONB NOT NULL DEFAULT 'null'::jsonb,
                result_handle JSONB NOT NULL DEFAULT 'null'::jsonb,
                items JSONB NOT NULL,
                output JSONB,
                error TEXT,
                cancellation_requested BOOLEAN NOT NULL DEFAULT FALSE,
                cancellation_reason TEXT,
                cancellation_metadata JSONB,
                worker_id TEXT,
                worker_build_id TEXT,
                lease_token UUID,
                last_report_id TEXT,
                scheduled_at TIMESTAMPTZ NOT NULL,
                available_at TIMESTAMPTZ NOT NULL,
                started_at TIMESTAMPTZ,
                lease_expires_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tenant_id, workflow_instance_id, run_id, batch_id, chunk_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize throughput_projection_chunks table")?;

        sqlx::query(
            r#"
            ALTER TABLE throughput_projection_chunks
            ADD COLUMN IF NOT EXISTS item_count INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS input_handle JSONB NOT NULL DEFAULT 'null'::jsonb,
            ADD COLUMN IF NOT EXISTS result_handle JSONB NOT NULL DEFAULT 'null'::jsonb
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to ensure throughput_projection_chunks payload handle columns")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS throughput_projection_chunks_batch_idx
            ON throughput_projection_chunks (tenant_id, workflow_instance_id, run_id, batch_id, chunk_index)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize throughput_projection_chunks batch index")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_event_outbox (
                outbox_id UUID PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                partition_key TEXT NOT NULL,
                event JSONB NOT NULL,
                available_at TIMESTAMPTZ NOT NULL,
                leased_by TEXT,
                lease_expires_at TIMESTAMPTZ,
                published_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_event_outbox table")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS workflow_event_outbox_pending_idx
            ON workflow_event_outbox (available_at, created_at)
            WHERE published_at IS NULL
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_event_outbox pending index")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_runs (
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                workflow_id TEXT NOT NULL,
                definition_version INTEGER,
                artifact_hash TEXT,
                workflow_task_queue TEXT NOT NULL DEFAULT 'default',
                sticky_workflow_build_id TEXT,
                sticky_workflow_poller_id TEXT,
                sticky_updated_at TIMESTAMPTZ,
                previous_run_id TEXT,
                next_run_id TEXT,
                continue_reason TEXT,
                trigger_event_id UUID NOT NULL,
                continued_event_id UUID,
                triggered_by_run_id TEXT,
                started_at TIMESTAMPTZ NOT NULL,
                closed_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tenant_id, workflow_instance_id, run_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_runs table")?;

        sqlx::query(
            "ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS workflow_task_queue TEXT NOT NULL DEFAULT 'default'",
        )
        .execute(&self.pool)
        .await
        .context("failed to add workflow_runs.workflow_task_queue")?;

        sqlx::query(
            "ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS sticky_workflow_build_id TEXT",
        )
        .execute(&self.pool)
        .await
        .context("failed to add workflow_runs.sticky_workflow_build_id")?;

        sqlx::query(
            "ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS sticky_workflow_poller_id TEXT",
        )
        .execute(&self.pool)
        .await
        .context("failed to add workflow_runs.sticky_workflow_poller_id")?;

        sqlx::query("ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS sticky_updated_at TIMESTAMPTZ")
            .execute(&self.pool)
            .await
            .context("failed to add workflow_runs.sticky_updated_at")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS task_queue_builds (
                tenant_id TEXT NOT NULL,
                queue_kind TEXT NOT NULL,
                task_queue TEXT NOT NULL,
                build_id TEXT NOT NULL,
                artifact_hashes JSONB NOT NULL DEFAULT '[]'::jsonb,
                metadata JSONB,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tenant_id, queue_kind, task_queue, build_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize task_queue_builds table")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS task_queue_compatibility_sets (
                tenant_id TEXT NOT NULL,
                queue_kind TEXT NOT NULL,
                task_queue TEXT NOT NULL,
                set_id TEXT NOT NULL,
                is_default BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tenant_id, queue_kind, task_queue, set_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize task_queue_compatibility_sets table")?;

        sqlx::query(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS task_queue_compatibility_sets_default_idx
            ON task_queue_compatibility_sets (tenant_id, queue_kind, task_queue)
            WHERE is_default
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize task_queue_compatibility_sets default index")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS task_queue_compatibility_set_builds (
                tenant_id TEXT NOT NULL,
                queue_kind TEXT NOT NULL,
                task_queue TEXT NOT NULL,
                set_id TEXT NOT NULL,
                build_id TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tenant_id, queue_kind, task_queue, set_id, build_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize task_queue_compatibility_set_builds table")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS task_queue_pollers (
                tenant_id TEXT NOT NULL,
                queue_kind TEXT NOT NULL,
                task_queue TEXT NOT NULL,
                poller_id TEXT NOT NULL,
                build_id TEXT NOT NULL,
                partition_id INTEGER,
                metadata JSONB,
                last_seen_at TIMESTAMPTZ NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tenant_id, queue_kind, task_queue, poller_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize task_queue_pollers table")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS task_queue_throughput_policies (
                tenant_id TEXT NOT NULL,
                queue_kind TEXT NOT NULL,
                task_queue TEXT NOT NULL,
                backend TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tenant_id, queue_kind, task_queue)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize task_queue_throughput_policies table")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_tasks (
                task_id UUID PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                workflow_id TEXT NOT NULL,
                definition_version INTEGER,
                artifact_hash TEXT,
                partition_id INTEGER NOT NULL,
                task_queue TEXT NOT NULL,
                preferred_build_id TEXT,
                status TEXT NOT NULL,
                source_event_id UUID NOT NULL,
                source_event_type TEXT NOT NULL,
                source_event JSONB NOT NULL,
                mailbox_consumed_seq BIGINT NOT NULL DEFAULT 0,
                resume_consumed_seq BIGINT NOT NULL DEFAULT 0,
                mailbox_high_watermark BIGINT NOT NULL DEFAULT 0,
                resume_high_watermark BIGINT NOT NULL DEFAULT 0,
                mailbox_backlog BIGINT NOT NULL DEFAULT 0,
                resume_backlog BIGINT NOT NULL DEFAULT 0,
                lease_poller_id TEXT,
                lease_build_id TEXT,
                lease_expires_at TIMESTAMPTZ,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_tasks table")?;

        sqlx::query(
            "ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS mailbox_consumed_seq BIGINT NOT NULL DEFAULT 0",
        )
        .execute(&self.pool)
        .await
        .context("failed to add workflow_tasks.mailbox_consumed_seq")?;
        sqlx::query(
            "ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS resume_consumed_seq BIGINT NOT NULL DEFAULT 0",
        )
        .execute(&self.pool)
        .await
        .context("failed to add workflow_tasks.resume_consumed_seq")?;
        sqlx::query(
            "ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS mailbox_high_watermark BIGINT NOT NULL DEFAULT 0",
        )
        .execute(&self.pool)
        .await
        .context("failed to add workflow_tasks.mailbox_high_watermark")?;
        sqlx::query(
            "ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS resume_high_watermark BIGINT NOT NULL DEFAULT 0",
        )
        .execute(&self.pool)
        .await
        .context("failed to add workflow_tasks.resume_high_watermark")?;
        sqlx::query(
            "ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS mailbox_backlog BIGINT NOT NULL DEFAULT 0",
        )
        .execute(&self.pool)
        .await
        .context("failed to add workflow_tasks.mailbox_backlog")?;
        sqlx::query(
            "ALTER TABLE workflow_tasks ADD COLUMN IF NOT EXISTS resume_backlog BIGINT NOT NULL DEFAULT 0",
        )
        .execute(&self.pool)
        .await
        .context("failed to add workflow_tasks.resume_backlog")?;

        sqlx::query(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS workflow_tasks_source_event_idx
            ON workflow_tasks (source_event_id)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_tasks source_event index")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS workflow_tasks_partition_status_idx
            ON workflow_tasks (
                partition_id,
                status,
                task_queue,
                preferred_build_id,
                updated_at,
                mailbox_backlog,
                resume_backlog
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_tasks partition status index")?;

        sqlx::query(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS workflow_tasks_run_idx
            ON workflow_tasks (tenant_id, workflow_instance_id, run_id)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_tasks run index")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_mailbox (
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                accepted_seq BIGINT NOT NULL,
                kind TEXT NOT NULL,
                message_id TEXT,
                message_name TEXT,
                payload JSONB,
                source_event_id UUID NOT NULL,
                source_event_type TEXT NOT NULL,
                source_event JSONB NOT NULL,
                status TEXT NOT NULL,
                consumed_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tenant_id, workflow_instance_id, run_id, accepted_seq)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_mailbox table")?;

        sqlx::query(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS workflow_mailbox_source_event_idx
            ON workflow_mailbox (source_event_id)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_mailbox source event index")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS workflow_mailbox_run_status_idx
            ON workflow_mailbox (tenant_id, workflow_instance_id, run_id, status, accepted_seq)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_mailbox run status index")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_resumes (
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                resume_seq BIGINT NOT NULL,
                kind TEXT NOT NULL,
                ref_id TEXT NOT NULL,
                status_label TEXT,
                source_event_id UUID NOT NULL,
                source_event_type TEXT NOT NULL,
                source_event JSONB NOT NULL,
                status TEXT NOT NULL,
                consumed_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tenant_id, workflow_instance_id, run_id, resume_seq)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_resumes table")?;

        sqlx::query(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS workflow_resumes_source_event_idx
            ON workflow_resumes (source_event_id)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_resumes source event index")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS workflow_resumes_run_status_idx
            ON workflow_resumes (tenant_id, workflow_instance_id, run_id, status, resume_seq)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_resumes run status index")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_signals (
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                signal_id TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                dedupe_key TEXT,
                payload JSONB NOT NULL,
                status TEXT NOT NULL,
                source_event_id UUID NOT NULL,
                dispatch_event_id UUID,
                consumed_event_id UUID,
                enqueued_at TIMESTAMPTZ NOT NULL,
                consumed_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tenant_id, workflow_instance_id, run_id, signal_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_signals table")?;

        sqlx::query(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS workflow_signals_dedupe_idx
            ON workflow_signals (
                tenant_id,
                workflow_instance_id,
                run_id,
                signal_type,
                dedupe_key
            )
            WHERE dedupe_key IS NOT NULL
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_signals dedupe index")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_partition_ownership (
                partition_id INTEGER PRIMARY KEY,
                owner_id TEXT NOT NULL,
                owner_epoch BIGINT NOT NULL,
                lease_expires_at TIMESTAMPTZ NOT NULL,
                acquired_at TIMESTAMPTZ NOT NULL,
                last_transition_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_partition_ownership table")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_executor_membership (
                executor_id TEXT PRIMARY KEY,
                query_endpoint TEXT NOT NULL DEFAULT '',
                advertised_capacity INTEGER NOT NULL,
                heartbeat_expires_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_executor_membership table")?;

        sqlx::query(
            "ALTER TABLE workflow_executor_membership ADD COLUMN IF NOT EXISTS query_endpoint TEXT NOT NULL DEFAULT ''",
        )
        .execute(&self.pool)
        .await
        .context("failed to add workflow_executor_membership.query_endpoint")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_partition_assignments (
                partition_id INTEGER PRIMARY KEY,
                executor_id TEXT NOT NULL,
                assigned_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_partition_assignments table")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_throughput_membership (
                runtime_id TEXT PRIMARY KEY,
                query_endpoint TEXT NOT NULL DEFAULT '',
                advertised_capacity INTEGER NOT NULL,
                heartbeat_expires_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_throughput_membership table")?;

        sqlx::query(
            "ALTER TABLE workflow_throughput_membership ADD COLUMN IF NOT EXISTS query_endpoint TEXT NOT NULL DEFAULT ''",
        )
        .execute(&self.pool)
        .await
        .context("failed to add workflow_throughput_membership.query_endpoint")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_throughput_partition_assignments (
                partition_id INTEGER PRIMARY KEY,
                runtime_id TEXT NOT NULL,
                assigned_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_throughput_partition_assignments table")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS throughput_projection_offsets (
                projector_id TEXT NOT NULL,
                partition_id INTEGER NOT NULL,
                log_offset BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (projector_id, partition_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize throughput_projection_offsets table")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_updates (
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                update_id TEXT NOT NULL,
                update_name TEXT NOT NULL,
                request_id TEXT,
                payload JSONB NOT NULL,
                status TEXT NOT NULL,
                output JSONB,
                error TEXT,
                source_event_id UUID NOT NULL,
                accepted_event_id UUID,
                completed_event_id UUID,
                requested_at TIMESTAMPTZ NOT NULL,
                accepted_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tenant_id, workflow_instance_id, run_id, update_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_updates table")?;

        sqlx::query(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS workflow_updates_request_id_idx
            ON workflow_updates (tenant_id, workflow_instance_id, run_id, update_name, request_id)
            WHERE request_id IS NOT NULL
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_updates request dedupe index")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_children (
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                child_id TEXT NOT NULL,
                child_workflow_id TEXT NOT NULL,
                child_definition_id TEXT NOT NULL,
                child_run_id TEXT,
                parent_close_policy TEXT NOT NULL,
                input JSONB NOT NULL,
                status TEXT NOT NULL,
                output JSONB,
                error TEXT,
                source_event_id UUID NOT NULL,
                started_event_id UUID,
                terminal_event_id UUID,
                started_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tenant_id, workflow_instance_id, run_id, child_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_children table")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workflow_request_dedup (
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT,
                operation TEXT NOT NULL,
                request_id TEXT NOT NULL,
                request_hash TEXT NOT NULL,
                response JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tenant_id, workflow_instance_id, operation, request_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_request_dedup table")?;

        sqlx::query(
            "ALTER TABLE workflow_timers ADD COLUMN IF NOT EXISTS dispatch_owner_epoch BIGINT",
        )
        .execute(&self.pool)
        .await
        .context("failed to add workflow_timers.dispatch_owner_epoch")?;

        Ok(())
        }
        .await;

        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(INIT_LOCK_KEY)
            .execute(&mut *lock_conn)
            .await
            .context("failed to release workflow store init lock")?;

        result
    }

    pub async fn claim_partition_ownership(
        &self,
        partition_id: i32,
        owner_id: &str,
        lease_ttl: std::time::Duration,
    ) -> Result<Option<PartitionOwnershipRecord>> {
        let mut tx = self.pool.begin().await.context("failed to begin ownership transaction")?;
        let now = Utc::now();
        let lease_expires_at =
            now + chrono::Duration::from_std(lease_ttl).context("ownership lease ttl overflow")?;
        let row = sqlx::query(
            r#"
            SELECT
                partition_id,
                owner_id,
                owner_epoch,
                lease_expires_at,
                acquired_at,
                last_transition_at,
                updated_at
            FROM workflow_partition_ownership
            WHERE partition_id = $1
            FOR UPDATE
            "#,
        )
        .bind(partition_id)
        .fetch_optional(&mut *tx)
        .await
        .context("failed to load partition ownership for claim")?;

        let ownership = match row {
            Some(row) => {
                let current = Self::decode_partition_ownership_row(row)?;
                if current.owner_id == owner_id {
                    sqlx::query(
                        r#"
                        UPDATE workflow_partition_ownership
                        SET lease_expires_at = $2,
                            updated_at = $3
                        WHERE partition_id = $1
                        "#,
                    )
                    .bind(partition_id)
                    .bind(lease_expires_at)
                    .bind(now)
                    .execute(&mut *tx)
                    .await
                    .context("failed to renew existing partition ownership claim")?;
                    PartitionOwnershipRecord { lease_expires_at, updated_at: now, ..current }
                } else if current.lease_expires_at <= now {
                    let next_epoch = current.owner_epoch + 1;
                    sqlx::query(
                        r#"
                        UPDATE workflow_partition_ownership
                        SET owner_id = $2,
                            owner_epoch = $3,
                            lease_expires_at = $4,
                            acquired_at = $5,
                            last_transition_at = $5,
                            updated_at = $5
                        WHERE partition_id = $1
                        "#,
                    )
                    .bind(partition_id)
                    .bind(owner_id)
                    .bind(i64::try_from(next_epoch).context("ownership epoch exceeds i64")?)
                    .bind(lease_expires_at)
                    .bind(now)
                    .execute(&mut *tx)
                    .await
                    .context("failed to transfer expired partition ownership")?;
                    PartitionOwnershipRecord {
                        partition_id,
                        owner_id: owner_id.to_owned(),
                        owner_epoch: next_epoch,
                        lease_expires_at,
                        acquired_at: now,
                        last_transition_at: now,
                        updated_at: now,
                    }
                } else {
                    tx.commit().await.context("failed to commit busy ownership transaction")?;
                    return Ok(None);
                }
            }
            None => {
                sqlx::query(
                    r#"
                    INSERT INTO workflow_partition_ownership (
                        partition_id,
                        owner_id,
                        owner_epoch,
                        lease_expires_at,
                        acquired_at,
                        last_transition_at,
                        updated_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $5, $5)
                    "#,
                )
                .bind(partition_id)
                .bind(owner_id)
                .bind(1_i64)
                .bind(lease_expires_at)
                .bind(now)
                .execute(&mut *tx)
                .await
                .context("failed to insert partition ownership record")?;
                PartitionOwnershipRecord {
                    partition_id,
                    owner_id: owner_id.to_owned(),
                    owner_epoch: 1,
                    lease_expires_at,
                    acquired_at: now,
                    last_transition_at: now,
                    updated_at: now,
                }
            }
        };

        tx.commit().await.context("failed to commit ownership claim transaction")?;
        Ok(Some(ownership))
    }

    pub async fn claim_throughput_partition_ownership(
        &self,
        partition_id: i32,
        owner_id: &str,
        lease_ttl: std::time::Duration,
    ) -> Result<Option<PartitionOwnershipRecord>> {
        let mut tx =
            self.pool.begin().await.context("failed to begin throughput ownership transaction")?;
        let now = Utc::now();
        let lease_expires_at =
            now + chrono::Duration::from_std(lease_ttl).context("ownership lease ttl overflow")?;
        let row = sqlx::query(
            r#"
            SELECT
                partition_id,
                owner_id,
                owner_epoch,
                lease_expires_at,
                acquired_at,
                last_transition_at,
                updated_at
            FROM workflow_partition_ownership
            WHERE partition_id = $1
            FOR UPDATE
            "#,
        )
        .bind(partition_id)
        .fetch_optional(&mut *tx)
        .await
        .context("failed to load throughput partition ownership for claim")?;

        let ownership = match row {
            Some(row) => {
                let current = Self::decode_partition_ownership_row(row)?;
                if current.owner_id == owner_id {
                    sqlx::query(
                        r#"
                        UPDATE workflow_partition_ownership
                        SET lease_expires_at = $2,
                            updated_at = $3
                        WHERE partition_id = $1
                        "#,
                    )
                    .bind(partition_id)
                    .bind(lease_expires_at)
                    .bind(now)
                    .execute(&mut *tx)
                    .await
                    .context("failed to renew existing throughput partition ownership claim")?;
                    PartitionOwnershipRecord { lease_expires_at, updated_at: now, ..current }
                } else {
                    let can_takeover = if current.lease_expires_at <= now {
                        true
                    } else {
                        let owner_membership = sqlx::query_scalar::<_, DateTime<Utc>>(
                            r#"
                            SELECT heartbeat_expires_at
                            FROM workflow_throughput_membership
                            WHERE runtime_id = $1
                            "#,
                        )
                        .bind(&current.owner_id)
                        .fetch_optional(&mut *tx)
                        .await
                        .context("failed to load throughput owner membership for claim")?;
                        owner_membership
                            .is_none_or(|heartbeat_expires_at| heartbeat_expires_at <= now)
                    };
                    if !can_takeover {
                        tx.commit()
                            .await
                            .context("failed to commit busy throughput ownership transaction")?;
                        return Ok(None);
                    }
                    let next_epoch = current.owner_epoch + 1;
                    sqlx::query(
                        r#"
                        UPDATE workflow_partition_ownership
                        SET owner_id = $2,
                            owner_epoch = $3,
                            lease_expires_at = $4,
                            acquired_at = $5,
                            last_transition_at = $5,
                            updated_at = $5
                        WHERE partition_id = $1
                        "#,
                    )
                    .bind(partition_id)
                    .bind(owner_id)
                    .bind(i64::try_from(next_epoch).context("ownership epoch exceeds i64")?)
                    .bind(lease_expires_at)
                    .bind(now)
                    .execute(&mut *tx)
                    .await
                    .context("failed to transfer throughput partition ownership")?;
                    PartitionOwnershipRecord {
                        partition_id,
                        owner_id: owner_id.to_owned(),
                        owner_epoch: next_epoch,
                        lease_expires_at,
                        acquired_at: now,
                        last_transition_at: now,
                        updated_at: now,
                    }
                }
            }
            None => {
                sqlx::query(
                    r#"
                    INSERT INTO workflow_partition_ownership (
                        partition_id,
                        owner_id,
                        owner_epoch,
                        lease_expires_at,
                        acquired_at,
                        last_transition_at,
                        updated_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $5, $5)
                    "#,
                )
                .bind(partition_id)
                .bind(owner_id)
                .bind(1_i64)
                .bind(lease_expires_at)
                .bind(now)
                .execute(&mut *tx)
                .await
                .context("failed to insert throughput partition ownership record")?;
                PartitionOwnershipRecord {
                    partition_id,
                    owner_id: owner_id.to_owned(),
                    owner_epoch: 1,
                    lease_expires_at,
                    acquired_at: now,
                    last_transition_at: now,
                    updated_at: now,
                }
            }
        };

        tx.commit().await.context("failed to commit throughput ownership claim transaction")?;
        Ok(Some(ownership))
    }

    pub async fn renew_partition_ownership(
        &self,
        partition_id: i32,
        owner_id: &str,
        owner_epoch: u64,
        lease_ttl: std::time::Duration,
    ) -> Result<Option<PartitionOwnershipRecord>> {
        let now = Utc::now();
        let lease_expires_at =
            now + chrono::Duration::from_std(lease_ttl).context("ownership lease ttl overflow")?;
        let result = sqlx::query(
            r#"
            UPDATE workflow_partition_ownership
            SET lease_expires_at = $4,
                updated_at = $5
            WHERE partition_id = $1
              AND owner_id = $2
              AND owner_epoch = $3
              AND lease_expires_at > $5
            "#,
        )
        .bind(partition_id)
        .bind(owner_id)
        .bind(i64::try_from(owner_epoch).context("ownership epoch exceeds i64")?)
        .bind(lease_expires_at)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to renew partition ownership")?;

        if result.rows_affected() == 0 {
            return Ok(None);
        }

        self.get_partition_ownership(partition_id).await
    }

    pub async fn get_partition_ownership(
        &self,
        partition_id: i32,
    ) -> Result<Option<PartitionOwnershipRecord>> {
        let row = sqlx::query(
            r#"
            SELECT
                partition_id,
                owner_id,
                owner_epoch,
                lease_expires_at,
                acquired_at,
                last_transition_at,
                updated_at
            FROM workflow_partition_ownership
            WHERE partition_id = $1
            "#,
        )
        .bind(partition_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load partition ownership")?;

        row.map(Self::decode_partition_ownership_row).transpose()
    }

    pub async fn validate_partition_ownership(
        &self,
        partition_id: i32,
        owner_id: &str,
        owner_epoch: u64,
    ) -> Result<bool> {
        let now = Utc::now();
        let row = sqlx::query(
            r#"
            SELECT 1
            FROM workflow_partition_ownership
            WHERE partition_id = $1
              AND owner_id = $2
              AND owner_epoch = $3
              AND lease_expires_at > $4
            "#,
        )
        .bind(partition_id)
        .bind(owner_id)
        .bind(i64::try_from(owner_epoch).context("ownership epoch exceeds i64")?)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .context("failed to validate partition ownership")?;

        Ok(row.is_some())
    }

    pub async fn heartbeat_executor_member(
        &self,
        executor_id: &str,
        query_endpoint: &str,
        advertised_capacity: usize,
        heartbeat_ttl: std::time::Duration,
    ) -> Result<ExecutorMembershipRecord> {
        let now = Utc::now();
        let heartbeat_expires_at =
            now + chrono::Duration::from_std(heartbeat_ttl).context("heartbeat ttl overflow")?;
        sqlx::query(
            r#"
            INSERT INTO workflow_executor_membership (
                executor_id,
                query_endpoint,
                advertised_capacity,
                heartbeat_expires_at,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $5)
            ON CONFLICT (executor_id)
            DO UPDATE SET
                query_endpoint = EXCLUDED.query_endpoint,
                advertised_capacity = EXCLUDED.advertised_capacity,
                heartbeat_expires_at = EXCLUDED.heartbeat_expires_at,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(executor_id)
        .bind(query_endpoint)
        .bind(i32::try_from(advertised_capacity).context("executor capacity exceeds i32")?)
        .bind(heartbeat_expires_at)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to heartbeat executor membership")?;

        Ok(ExecutorMembershipRecord {
            executor_id: executor_id.to_owned(),
            query_endpoint: query_endpoint.to_owned(),
            advertised_capacity,
            heartbeat_expires_at,
            created_at: now,
            updated_at: now,
        })
    }

    pub async fn list_active_executor_members(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<ExecutorMembershipRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT executor_id, query_endpoint, advertised_capacity, heartbeat_expires_at, created_at, updated_at
            FROM workflow_executor_membership
            WHERE heartbeat_expires_at > $1
            ORDER BY executor_id ASC
            "#,
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await
        .context("failed to list active executor members")?;

        rows.into_iter().map(Self::decode_executor_membership_row).collect()
    }

    pub async fn heartbeat_throughput_member(
        &self,
        runtime_id: &str,
        query_endpoint: &str,
        advertised_capacity: usize,
        heartbeat_ttl: std::time::Duration,
    ) -> Result<ExecutorMembershipRecord> {
        let now = Utc::now();
        let heartbeat_expires_at =
            now + chrono::Duration::from_std(heartbeat_ttl).context("heartbeat ttl overflow")?;
        sqlx::query(
            r#"
            INSERT INTO workflow_throughput_membership (
                runtime_id,
                query_endpoint,
                advertised_capacity,
                heartbeat_expires_at,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $5)
            ON CONFLICT (runtime_id)
            DO UPDATE SET
                query_endpoint = EXCLUDED.query_endpoint,
                advertised_capacity = EXCLUDED.advertised_capacity,
                heartbeat_expires_at = EXCLUDED.heartbeat_expires_at,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(runtime_id)
        .bind(query_endpoint)
        .bind(i32::try_from(advertised_capacity).context("throughput capacity exceeds i32")?)
        .bind(heartbeat_expires_at)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to heartbeat throughput membership")?;

        Ok(ExecutorMembershipRecord {
            executor_id: runtime_id.to_owned(),
            query_endpoint: query_endpoint.to_owned(),
            advertised_capacity,
            heartbeat_expires_at,
            created_at: now,
            updated_at: now,
        })
    }

    pub async fn list_active_throughput_members(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<ExecutorMembershipRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT runtime_id AS executor_id, query_endpoint, advertised_capacity, heartbeat_expires_at, created_at, updated_at
            FROM workflow_throughput_membership
            WHERE heartbeat_expires_at > $1
            ORDER BY runtime_id ASC
            "#,
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await
        .context("failed to list active throughput members")?;

        rows.into_iter().map(Self::decode_executor_membership_row).collect()
    }

    pub async fn get_active_throughput_member_for_partition(
        &self,
        partition_id: i32,
        now: DateTime<Utc>,
    ) -> Result<Option<ExecutorMembershipRecord>> {
        let row = sqlx::query(
            r#"
            SELECT
                membership.runtime_id AS executor_id,
                membership.query_endpoint,
                membership.advertised_capacity,
                membership.heartbeat_expires_at,
                membership.created_at,
                membership.updated_at
            FROM workflow_throughput_partition_assignments assignments
            JOIN workflow_throughput_membership membership
              ON membership.runtime_id = assignments.runtime_id
            WHERE assignments.partition_id = $1
              AND membership.heartbeat_expires_at > $2
            ORDER BY membership.updated_at DESC, membership.heartbeat_expires_at DESC
            LIMIT 1
            "#,
        )
        .bind(partition_id)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load throughput member for partition")?;

        row.map(Self::decode_executor_membership_row).transpose()
    }

    pub async fn list_partition_assignments(&self) -> Result<Vec<PartitionAssignmentRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT partition_id, executor_id, assigned_at, updated_at
            FROM workflow_partition_assignments
            ORDER BY partition_id ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to list partition assignments")?;

        rows.into_iter().map(Self::decode_partition_assignment_row).collect()
    }

    pub async fn list_assignments_for_executor(
        &self,
        executor_id: &str,
    ) -> Result<Vec<PartitionAssignmentRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT partition_id, executor_id, assigned_at, updated_at
            FROM workflow_partition_assignments
            WHERE executor_id = $1
            ORDER BY partition_id ASC
            "#,
        )
        .bind(executor_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list partition assignments for executor")?;

        rows.into_iter().map(Self::decode_partition_assignment_row).collect()
    }

    pub async fn list_assignments_for_throughput_runtime(
        &self,
        runtime_id: &str,
    ) -> Result<Vec<PartitionAssignmentRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT partition_id, runtime_id AS executor_id, assigned_at, updated_at
            FROM workflow_throughput_partition_assignments
            WHERE runtime_id = $1
            ORDER BY partition_id ASC
            "#,
        )
        .bind(runtime_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list throughput partition assignments for runtime")?;

        rows.into_iter().map(Self::decode_partition_assignment_row).collect()
    }

    pub async fn reconcile_partition_assignments(
        &self,
        partition_count: i32,
        members: &[ExecutorMembershipRecord],
    ) -> Result<bool> {
        let mut tx = self.pool.begin().await.context("failed to begin assignment transaction")?;
        let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock($1)")
            .bind(4_237_001_i64)
            .fetch_one(&mut *tx)
            .await
            .context("failed to acquire partition assignment lock")?;
        if !acquired {
            tx.commit().await.context("failed to close assignment transaction")?;
            return Ok(false);
        }

        let assignments = plan_partition_assignments(partition_count, members);
        let assigned_partition_ids =
            assignments.iter().map(|(partition_id, _)| *partition_id).collect::<Vec<_>>();
        let now = Utc::now();
        for (partition_id, executor_id) in assignments {
            sqlx::query(
                r#"
                INSERT INTO workflow_partition_assignments (
                    partition_id,
                    executor_id,
                    assigned_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $3)
                ON CONFLICT (partition_id)
                DO UPDATE SET
                    executor_id = EXCLUDED.executor_id,
                    assigned_at = CASE
                        WHEN workflow_partition_assignments.executor_id = EXCLUDED.executor_id
                            THEN workflow_partition_assignments.assigned_at
                        ELSE EXCLUDED.assigned_at
                    END,
                    updated_at = EXCLUDED.updated_at
                "#,
            )
            .bind(partition_id)
            .bind(executor_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .context("failed to upsert partition assignment")?;
        }

        if assigned_partition_ids.is_empty() {
            sqlx::query("DELETE FROM workflow_partition_assignments")
                .execute(&mut *tx)
                .await
                .context("failed to delete stale partition assignments")?;
        } else {
            sqlx::query(
                r#"
                DELETE FROM workflow_partition_assignments
                WHERE NOT (partition_id = ANY($1))
                "#,
            )
            .bind(&assigned_partition_ids)
            .execute(&mut *tx)
            .await
            .context("failed to prune stale partition assignments")?;
        }

        tx.commit().await.context("failed to commit partition assignments")?;
        Ok(true)
    }

    pub async fn reconcile_throughput_partition_assignments(
        &self,
        partition_count: i32,
        members: &[ExecutorMembershipRecord],
    ) -> Result<bool> {
        let mut tx =
            self.pool.begin().await.context("failed to begin throughput assignment transaction")?;
        let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock($1)")
            .bind(4_237_101_i64)
            .fetch_one(&mut *tx)
            .await
            .context("failed to acquire throughput partition assignment lock")?;
        if !acquired {
            tx.commit().await.context("failed to close throughput assignment transaction")?;
            return Ok(false);
        }

        let assignments = plan_partition_assignments(partition_count, members);
        let assigned_partition_ids =
            assignments.iter().map(|(partition_id, _)| *partition_id).collect::<Vec<_>>();
        let now = Utc::now();
        for (partition_id, runtime_id) in assignments {
            sqlx::query(
                r#"
                INSERT INTO workflow_throughput_partition_assignments (
                    partition_id,
                    runtime_id,
                    assigned_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $3)
                ON CONFLICT (partition_id)
                DO UPDATE SET
                    runtime_id = EXCLUDED.runtime_id,
                    assigned_at = CASE
                        WHEN workflow_throughput_partition_assignments.runtime_id = EXCLUDED.runtime_id
                            THEN workflow_throughput_partition_assignments.assigned_at
                        ELSE EXCLUDED.assigned_at
                    END,
                    updated_at = EXCLUDED.updated_at
                "#,
            )
            .bind(partition_id)
            .bind(runtime_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .context("failed to upsert throughput partition assignment")?;
        }

        if assigned_partition_ids.is_empty() {
            sqlx::query("DELETE FROM workflow_throughput_partition_assignments")
                .execute(&mut *tx)
                .await
                .context("failed to delete stale throughput partition assignments")?;
        } else {
            sqlx::query(
                r#"
                DELETE FROM workflow_throughput_partition_assignments
                WHERE NOT (partition_id = ANY($1))
                "#,
            )
            .bind(&assigned_partition_ids)
            .execute(&mut *tx)
            .await
            .context("failed to prune stale throughput partition assignments")?;
        }

        tx.commit().await.context("failed to commit throughput partition assignments")?;
        Ok(true)
    }

    pub async fn release_partition_ownership(
        &self,
        partition_id: i32,
        owner_id: &str,
        owner_epoch: u64,
    ) -> Result<bool> {
        let now = Utc::now();
        let result = sqlx::query(
            r#"
            UPDATE workflow_partition_ownership
            SET lease_expires_at = $4,
                updated_at = $4
            WHERE partition_id = $1
              AND owner_id = $2
              AND owner_epoch = $3
            "#,
        )
        .bind(partition_id)
        .bind(owner_id)
        .bind(i64::try_from(owner_epoch).context("ownership epoch exceeds i64")?)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to release partition ownership")?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn put_definition(
        &self,
        tenant_id: &str,
        definition: &WorkflowDefinition,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO workflow_definitions (
                tenant_id,
                workflow_id,
                version,
                definition
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (tenant_id, workflow_id, version)
            DO UPDATE SET
                is_active = TRUE,
                definition = EXCLUDED.definition
            "#,
        )
        .bind(tenant_id)
        .bind(&definition.id)
        .bind(i32::try_from(definition.version).context("workflow version exceeds i32")?)
        .bind(Json(definition))
        .execute(&self.pool)
        .await
        .context("failed to store workflow definition")?;

        Ok(())
    }

    pub async fn get_latest_definition(
        &self,
        tenant_id: &str,
        workflow_id: &str,
    ) -> Result<Option<WorkflowDefinition>> {
        let row = sqlx::query(
            r#"
            SELECT definition
            FROM workflow_definitions
            WHERE tenant_id = $1 AND workflow_id = $2 AND is_active = TRUE
            ORDER BY version DESC
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(workflow_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load latest workflow definition")?;

        row.map(|row| {
            row.try_get::<Json<WorkflowDefinition>, _>("definition")
                .map(|value| value.0)
                .context("failed to decode workflow definition")
        })
        .transpose()
    }

    pub async fn get_definition_version(
        &self,
        tenant_id: &str,
        workflow_id: &str,
        version: u32,
    ) -> Result<Option<WorkflowDefinition>> {
        let row = sqlx::query(
            r#"
            SELECT definition
            FROM workflow_definitions
            WHERE tenant_id = $1 AND workflow_id = $2 AND version = $3
            "#,
        )
        .bind(tenant_id)
        .bind(workflow_id)
        .bind(i32::try_from(version).context("workflow version exceeds i32")?)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load workflow definition version")?;

        row.map(|row| {
            row.try_get::<Json<WorkflowDefinition>, _>("definition")
                .map(|value| value.0)
                .context("failed to decode workflow definition")
        })
        .transpose()
    }

    pub async fn put_artifact(
        &self,
        tenant_id: &str,
        artifact: &CompiledWorkflowArtifact,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO workflow_artifacts (
                tenant_id,
                workflow_id,
                version,
                artifact
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (tenant_id, workflow_id, version)
            DO UPDATE SET
                is_active = TRUE,
                artifact = EXCLUDED.artifact
            "#,
        )
        .bind(tenant_id)
        .bind(&artifact.definition_id)
        .bind(i32::try_from(artifact.definition_version).context("artifact version exceeds i32")?)
        .bind(Json(artifact))
        .execute(&self.pool)
        .await
        .context("failed to store workflow artifact")?;

        Ok(())
    }

    pub async fn get_latest_artifact(
        &self,
        tenant_id: &str,
        workflow_id: &str,
    ) -> Result<Option<CompiledWorkflowArtifact>> {
        let row = sqlx::query(
            r#"
            SELECT artifact
            FROM workflow_artifacts
            WHERE tenant_id = $1 AND workflow_id = $2 AND is_active = TRUE
            ORDER BY version DESC
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(workflow_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load latest workflow artifact")?;

        row.map(|row| {
            row.try_get::<Json<CompiledWorkflowArtifact>, _>("artifact")
                .map(|value| value.0)
                .context("failed to decode workflow artifact")
        })
        .transpose()
    }

    pub async fn get_artifact_version(
        &self,
        tenant_id: &str,
        workflow_id: &str,
        version: u32,
    ) -> Result<Option<CompiledWorkflowArtifact>> {
        let row = sqlx::query(
            r#"
            SELECT artifact
            FROM workflow_artifacts
            WHERE tenant_id = $1 AND workflow_id = $2 AND version = $3
            "#,
        )
        .bind(tenant_id)
        .bind(workflow_id)
        .bind(i32::try_from(version).context("artifact version exceeds i32")?)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load workflow artifact version")?;

        row.map(|row| {
            row.try_get::<Json<CompiledWorkflowArtifact>, _>("artifact")
                .map(|value| value.0)
                .context("failed to decode workflow artifact")
        })
        .transpose()
    }

    pub async fn upsert_instance(&self, state: &WorkflowInstanceState) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO workflow_instances (
                tenant_id,
                workflow_instance_id,
                workflow_id,
                run_id,
                definition_version,
                artifact_hash,
                status,
                event_count,
                last_event_id,
                last_event_type,
                updated_at,
                state
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (tenant_id, workflow_instance_id)
            DO UPDATE SET
                workflow_id = EXCLUDED.workflow_id,
                run_id = EXCLUDED.run_id,
                definition_version = EXCLUDED.definition_version,
                artifact_hash = EXCLUDED.artifact_hash,
                status = EXCLUDED.status,
                event_count = EXCLUDED.event_count,
                last_event_id = EXCLUDED.last_event_id,
                last_event_type = EXCLUDED.last_event_type,
                updated_at = EXCLUDED.updated_at,
                state = EXCLUDED.state
            "#,
        )
        .bind(&state.tenant_id)
        .bind(&state.instance_id)
        .bind(&state.definition_id)
        .bind(&state.run_id)
        .bind(state.definition_version.map(|version| version as i32))
        .bind(state.artifact_hash.as_deref())
        .bind(state.status.as_str())
        .bind(state.event_count)
        .bind(state.last_event_id)
        .bind(state.last_event_type.as_str())
        .bind(state.updated_at)
        .bind(Json(state))
        .execute(&self.pool)
        .await
        .context("failed to upsert workflow instance")?;

        Ok(())
    }

    pub async fn get_instance(
        &self,
        tenant_id: &str,
        workflow_instance_id: &str,
    ) -> Result<Option<WorkflowInstanceState>> {
        let row = sqlx::query(
            r#"
            SELECT state
            FROM workflow_instances
            WHERE tenant_id = $1 AND workflow_instance_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(workflow_instance_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load workflow instance")?;

        row.map(|row| {
            row.try_get::<Json<WorkflowInstanceState>, _>("state")
                .map(|value| value.0)
                .context("failed to decode workflow instance state")
        })
        .transpose()
    }

    pub async fn put_run_start(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        definition_id: &str,
        definition_version: Option<u32>,
        artifact_hash: Option<&str>,
        workflow_task_queue: &str,
        trigger_event_id: Uuid,
        started_at: DateTime<Utc>,
        previous_run_id: Option<&str>,
        triggered_by_run_id: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO workflow_runs (
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                definition_version,
                artifact_hash,
                workflow_task_queue,
                sticky_workflow_build_id,
                sticky_workflow_poller_id,
                sticky_updated_at,
                previous_run_id,
                next_run_id,
                continue_reason,
                trigger_event_id,
                continued_event_id,
                triggered_by_run_id,
                started_at,
                closed_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, NULL, NULL, NULL, $8, NULL, NULL, $9, NULL, $10, $11, NULL, $11
            )
            ON CONFLICT (tenant_id, workflow_instance_id, run_id)
            DO UPDATE SET
                workflow_id = EXCLUDED.workflow_id,
                definition_version = EXCLUDED.definition_version,
                artifact_hash = EXCLUDED.artifact_hash,
                workflow_task_queue = EXCLUDED.workflow_task_queue,
                previous_run_id = EXCLUDED.previous_run_id,
                trigger_event_id = EXCLUDED.trigger_event_id,
                triggered_by_run_id = EXCLUDED.triggered_by_run_id,
                started_at = EXCLUDED.started_at,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(definition_id)
        .bind(definition_version.map(|version| version as i32))
        .bind(artifact_hash)
        .bind(workflow_task_queue)
        .bind(previous_run_id)
        .bind(trigger_event_id)
        .bind(triggered_by_run_id)
        .bind(started_at)
        .execute(&self.pool)
        .await
        .context("failed to persist workflow run start")?;

        Ok(())
    }

    pub async fn record_run_continuation(
        &self,
        tenant_id: &str,
        instance_id: &str,
        previous_run_id: &str,
        new_run_id: &str,
        continue_reason: &str,
        continued_event_id: Uuid,
        triggered_event_id: Uuid,
        transitioned_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE workflow_runs
            SET next_run_id = $4,
                continue_reason = $5,
                continued_event_id = $6,
                closed_at = COALESCE(closed_at, $7),
                updated_at = $7
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(previous_run_id)
        .bind(new_run_id)
        .bind(continue_reason)
        .bind(continued_event_id)
        .bind(transitioned_at)
        .execute(&self.pool)
        .await
        .context("failed to persist workflow run continuation")?;

        sqlx::query(
            r#"
            UPDATE workflow_runs
            SET previous_run_id = COALESCE(previous_run_id, $4),
                triggered_by_run_id = COALESCE(triggered_by_run_id, $4),
                updated_at = $5
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(new_run_id)
        .bind(previous_run_id)
        .bind(transitioned_at)
        .execute(&self.pool)
        .await
        .context("failed to backfill new workflow run continuation link")?;

        sqlx::query(
            r#"
            UPDATE workflow_runs
            SET trigger_event_id = COALESCE(trigger_event_id, $4),
                updated_at = $5
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(new_run_id)
        .bind(triggered_event_id)
        .bind(transitioned_at)
        .execute(&self.pool)
        .await
        .context("failed to update new workflow run trigger event")?;

        Ok(())
    }

    pub async fn list_runs_for_instance(
        &self,
        tenant_id: &str,
        instance_id: &str,
    ) -> Result<Vec<WorkflowRunRecord>> {
        self.list_runs_for_instance_page(tenant_id, instance_id, i64::MAX, 0).await
    }

    pub async fn count_runs_for_instance(&self, tenant_id: &str, instance_id: &str) -> Result<u64> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM workflow_runs
            WHERE tenant_id = $1 AND workflow_instance_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .fetch_one(&self.pool)
        .await
        .context("failed to count workflow runs for instance")?;

        Ok(count as u64)
    }

    pub async fn list_runs_for_instance_page(
        &self,
        tenant_id: &str,
        instance_id: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<WorkflowRunRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                definition_version,
                artifact_hash,
                workflow_task_queue,
                sticky_workflow_build_id,
                sticky_workflow_poller_id,
                sticky_updated_at,
                previous_run_id,
                next_run_id,
                continue_reason,
                trigger_event_id,
                continued_event_id,
                triggered_by_run_id,
                started_at,
                closed_at,
                updated_at
            FROM workflow_runs
            WHERE tenant_id = $1 AND workflow_instance_id = $2
            ORDER BY started_at ASC
            LIMIT $3 OFFSET $4
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .context("failed to load workflow runs for instance")?;

        rows.into_iter().map(Self::decode_run_row).collect()
    }

    pub async fn get_run_record(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
    ) -> Result<Option<WorkflowRunRecord>> {
        let row = sqlx::query(
            r#"
            SELECT
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                definition_version,
                artifact_hash,
                workflow_task_queue,
                sticky_workflow_build_id,
                sticky_workflow_poller_id,
                sticky_updated_at,
                previous_run_id,
                next_run_id,
                continue_reason,
                trigger_event_id,
                continued_event_id,
                triggered_by_run_id,
                started_at,
                closed_at,
                updated_at
            FROM workflow_runs
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load workflow run record")?;

        row.map(Self::decode_run_row).transpose()
    }

    pub async fn close_run(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        closed_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE workflow_runs
            SET closed_at = COALESCE(closed_at, $4),
                updated_at = $4
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(closed_at)
        .execute(&self.pool)
        .await
        .context("failed to close workflow run")?;

        Ok(())
    }

    pub async fn update_run_workflow_sticky(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        workflow_task_queue: &str,
        build_id: &str,
        poller_id: &str,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE workflow_runs
            SET workflow_task_queue = $4,
                sticky_workflow_build_id = $5,
                sticky_workflow_poller_id = $6,
                sticky_updated_at = $7,
                updated_at = GREATEST(updated_at, $7)
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(workflow_task_queue)
        .bind(build_id)
        .bind(poller_id)
        .bind(updated_at)
        .execute(&self.pool)
        .await
        .context("failed to update workflow sticky routing")?;

        Ok(())
    }

    pub async fn register_task_queue_build(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
        build_id: &str,
        artifact_hashes: &[String],
        metadata: Option<&Value>,
    ) -> Result<TaskQueueBuildRecord> {
        let now = Utc::now();
        sqlx::query(
            r#"
            INSERT INTO task_queue_builds (
                tenant_id,
                queue_kind,
                task_queue,
                build_id,
                artifact_hashes,
                metadata,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
            ON CONFLICT (tenant_id, queue_kind, task_queue, build_id)
            DO UPDATE SET
                artifact_hashes = EXCLUDED.artifact_hashes,
                metadata = EXCLUDED.metadata,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(tenant_id)
        .bind(queue_kind.as_str())
        .bind(task_queue)
        .bind(build_id)
        .bind(Json(artifact_hashes.to_vec()))
        .bind(metadata.cloned().map(Json))
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to register task queue build")?;

        self.get_task_queue_build(tenant_id, queue_kind, task_queue, build_id)
            .await?
            .context("registered task queue build was not found")
    }

    pub async fn get_task_queue_build(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
        build_id: &str,
    ) -> Result<Option<TaskQueueBuildRecord>> {
        let row = sqlx::query(
            r#"
            SELECT tenant_id, queue_kind, task_queue, build_id, artifact_hashes, metadata, created_at, updated_at
            FROM task_queue_builds
            WHERE tenant_id = $1
              AND queue_kind = $2
              AND task_queue = $3
              AND build_id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(queue_kind.as_str())
        .bind(task_queue)
        .bind(build_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load task queue build")?;

        row.map(Self::decode_task_queue_build_row).transpose()
    }

    pub async fn list_task_queue_builds(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
    ) -> Result<Vec<TaskQueueBuildRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT tenant_id, queue_kind, task_queue, build_id, artifact_hashes, metadata, created_at, updated_at
            FROM task_queue_builds
            WHERE tenant_id = $1
              AND queue_kind = $2
              AND task_queue = $3
            ORDER BY build_id ASC
            "#,
        )
        .bind(tenant_id)
        .bind(queue_kind.as_str())
        .bind(task_queue)
        .fetch_all(&self.pool)
        .await
        .context("failed to list task queue builds")?;

        rows.into_iter().map(Self::decode_task_queue_build_row).collect()
    }

    pub async fn list_queues_for_build(
        &self,
        queue_kind: TaskQueueKind,
        build_id: &str,
    ) -> Result<Vec<(String, String)>> {
        let rows = sqlx::query(
            r#"
            SELECT tenant_id, task_queue
            FROM task_queue_builds
            WHERE queue_kind = $1 AND build_id = $2
            ORDER BY tenant_id ASC, task_queue ASC
            "#,
        )
        .bind(queue_kind.as_str())
        .bind(build_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list queues for build")?;

        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.try_get("tenant_id").expect("tenant_id is selected"),
                    row.try_get("task_queue").expect("task_queue is selected"),
                )
            })
            .collect())
    }

    pub async fn upsert_compatibility_set(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
        set_id: &str,
        build_ids: &[String],
        is_default: bool,
    ) -> Result<CompatibilitySetRecord> {
        let now = Utc::now();
        if is_default {
            sqlx::query(
                r#"
                UPDATE task_queue_compatibility_sets
                SET is_default = FALSE,
                    updated_at = $4
                WHERE tenant_id = $1 AND queue_kind = $2 AND task_queue = $3
                "#,
            )
            .bind(tenant_id)
            .bind(queue_kind.as_str())
            .bind(task_queue)
            .bind(now)
            .execute(&self.pool)
            .await
            .context("failed to clear existing default compatibility set")?;
        }

        sqlx::query(
            r#"
            INSERT INTO task_queue_compatibility_sets (
                tenant_id,
                queue_kind,
                task_queue,
                set_id,
                is_default,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $6)
            ON CONFLICT (tenant_id, queue_kind, task_queue, set_id)
            DO UPDATE SET
                is_default = EXCLUDED.is_default,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(tenant_id)
        .bind(queue_kind.as_str())
        .bind(task_queue)
        .bind(set_id)
        .bind(is_default)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to upsert compatibility set")?;

        sqlx::query(
            r#"
            DELETE FROM task_queue_compatibility_set_builds
            WHERE tenant_id = $1 AND queue_kind = $2 AND task_queue = $3 AND set_id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(queue_kind.as_str())
        .bind(task_queue)
        .bind(set_id)
        .execute(&self.pool)
        .await
        .context("failed to clear compatibility set members")?;

        for build_id in build_ids {
            sqlx::query(
                r#"
                INSERT INTO task_queue_compatibility_set_builds (
                    tenant_id,
                    queue_kind,
                    task_queue,
                    set_id,
                    build_id,
                    created_at
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
            )
            .bind(tenant_id)
            .bind(queue_kind.as_str())
            .bind(task_queue)
            .bind(set_id)
            .bind(build_id)
            .bind(now)
            .execute(&self.pool)
            .await
            .context("failed to upsert compatibility set member")?;
        }

        self.get_compatibility_set(tenant_id, queue_kind, task_queue, set_id)
            .await?
            .context("compatibility set was not found after upsert")
    }

    pub async fn set_default_compatibility_set(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
        set_id: &str,
    ) -> Result<()> {
        let now = Utc::now();
        let mut tx =
            self.pool.begin().await.context("failed to begin default-set promotion transaction")?;
        sqlx::query(
            r#"
            UPDATE task_queue_compatibility_sets
            SET is_default = FALSE,
                updated_at = $4
            WHERE tenant_id = $1 AND queue_kind = $2 AND task_queue = $3
            "#,
        )
        .bind(tenant_id)
        .bind(queue_kind.as_str())
        .bind(task_queue)
        .bind(now)
        .execute(&mut *tx)
        .await
        .context("failed to clear existing default compatibility set")?;
        let updated = sqlx::query(
            r#"
            UPDATE task_queue_compatibility_sets
            SET is_default = TRUE,
                updated_at = $5
            WHERE tenant_id = $1
              AND queue_kind = $2
              AND task_queue = $3
              AND set_id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(queue_kind.as_str())
        .bind(task_queue)
        .bind(set_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .context("failed to promote default compatibility set")?;
        if updated.rows_affected() != 1 {
            tx.rollback()
                .await
                .context("failed to rollback missing default-set promotion transaction")?;
            anyhow::bail!("compatibility set {set_id} was not found for task queue {task_queue}");
        }
        tx.commit().await.context("failed to commit default-set promotion transaction")?;

        Ok(())
    }

    pub async fn get_compatibility_set(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
        set_id: &str,
    ) -> Result<Option<CompatibilitySetRecord>> {
        let row = sqlx::query(
            r#"
            SELECT tenant_id, queue_kind, task_queue, set_id, is_default, created_at, updated_at
            FROM task_queue_compatibility_sets
            WHERE tenant_id = $1
              AND queue_kind = $2
              AND task_queue = $3
              AND set_id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(queue_kind.as_str())
        .bind(task_queue)
        .bind(set_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load compatibility set")?;

        match row {
            Some(row) => {
                let build_ids = self
                    .list_compatibility_set_build_ids(
                        tenant_id,
                        queue_kind.clone(),
                        task_queue,
                        set_id,
                    )
                    .await?;
                Ok(Some(Self::decode_compatibility_set_row(row, build_ids)?))
            }
            None => Ok(None),
        }
    }

    pub async fn list_compatibility_sets(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
    ) -> Result<Vec<CompatibilitySetRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT tenant_id, queue_kind, task_queue, set_id, is_default, created_at, updated_at
            FROM task_queue_compatibility_sets
            WHERE tenant_id = $1 AND queue_kind = $2 AND task_queue = $3
            ORDER BY set_id ASC
            "#,
        )
        .bind(tenant_id)
        .bind(queue_kind.as_str())
        .bind(task_queue)
        .fetch_all(&self.pool)
        .await
        .context("failed to list compatibility sets")?;

        let mut sets = Vec::with_capacity(rows.len());
        for row in rows {
            let set_id: String =
                row.try_get("set_id").context("compatibility set set_id missing")?;
            let build_ids = self
                .list_compatibility_set_build_ids(
                    tenant_id,
                    queue_kind.clone(),
                    task_queue,
                    &set_id,
                )
                .await?;
            sets.push(Self::decode_compatibility_set_row(row, build_ids)?);
        }
        Ok(sets)
    }

    pub async fn list_default_compatible_build_ids(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
    ) -> Result<Vec<String>> {
        let row = sqlx::query(
            r#"
            SELECT set_id
            FROM task_queue_compatibility_sets
            WHERE tenant_id = $1 AND queue_kind = $2 AND task_queue = $3 AND is_default = TRUE
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(queue_kind.as_str())
        .bind(task_queue)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load default compatibility set")?;

        if let Some(row) = row {
            let set_id: String = row.try_get("set_id").context("default set_id missing")?;
            return self
                .list_compatibility_set_build_ids(tenant_id, queue_kind, task_queue, &set_id)
                .await;
        }

        Ok(self
            .list_task_queue_builds(tenant_id, queue_kind, task_queue)
            .await?
            .into_iter()
            .map(|record| record.build_id)
            .collect())
    }

    pub async fn is_build_compatible_with_queue(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
        build_id: &str,
    ) -> Result<bool> {
        let registered_builds =
            self.list_task_queue_builds(tenant_id, queue_kind.clone(), task_queue).await?;
        if registered_builds.is_empty() {
            return Ok(true);
        }
        let compatible =
            self.list_default_compatible_build_ids(tenant_id, queue_kind, task_queue).await?;
        Ok(compatible.is_empty() || compatible.iter().any(|candidate| candidate == build_id))
    }

    pub async fn workflow_build_supports_artifact(
        &self,
        tenant_id: &str,
        task_queue: &str,
        build_id: &str,
        artifact_hash: Option<&str>,
    ) -> Result<bool> {
        let Some(record) = self
            .get_task_queue_build(tenant_id, TaskQueueKind::Workflow, task_queue, build_id)
            .await?
        else {
            return Ok(true);
        };
        let Some(artifact_hash) = artifact_hash else {
            return Ok(true);
        };
        Ok(record.artifact_hashes.is_empty()
            || record.artifact_hashes.iter().any(|candidate| candidate == artifact_hash))
    }

    pub async fn upsert_queue_poller(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
        poller_id: &str,
        build_id: &str,
        partition_id: Option<i32>,
        metadata: Option<&Value>,
        ttl: chrono::Duration,
    ) -> Result<QueuePollerRecord> {
        let now = Utc::now();
        let expires_at = now + ttl;
        sqlx::query(
            r#"
            INSERT INTO task_queue_pollers (
                tenant_id,
                queue_kind,
                task_queue,
                poller_id,
                build_id,
                partition_id,
                metadata,
                last_seen_at,
                expires_at,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $8, $8)
            ON CONFLICT (tenant_id, queue_kind, task_queue, poller_id)
            DO UPDATE SET
                build_id = EXCLUDED.build_id,
                partition_id = EXCLUDED.partition_id,
                metadata = EXCLUDED.metadata,
                last_seen_at = EXCLUDED.last_seen_at,
                expires_at = EXCLUDED.expires_at,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(tenant_id)
        .bind(queue_kind.as_str())
        .bind(task_queue)
        .bind(poller_id)
        .bind(build_id)
        .bind(partition_id)
        .bind(metadata.cloned().map(Json))
        .bind(now)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .context("failed to upsert task queue poller")?;

        self.get_queue_poller(tenant_id, queue_kind, task_queue, poller_id)
            .await?
            .context("task queue poller was not found after upsert")
    }

    pub async fn get_queue_poller(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
        poller_id: &str,
    ) -> Result<Option<QueuePollerRecord>> {
        let row = sqlx::query(
            r#"
            SELECT tenant_id, queue_kind, task_queue, poller_id, build_id, partition_id, metadata, last_seen_at, expires_at, created_at, updated_at
            FROM task_queue_pollers
            WHERE tenant_id = $1 AND queue_kind = $2 AND task_queue = $3 AND poller_id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(queue_kind.as_str())
        .bind(task_queue)
        .bind(poller_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load task queue poller")?;

        row.map(Self::decode_queue_poller_row).transpose()
    }

    pub async fn list_queue_pollers(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
        active_at: DateTime<Utc>,
    ) -> Result<Vec<QueuePollerRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT tenant_id, queue_kind, task_queue, poller_id, build_id, partition_id, metadata, last_seen_at, expires_at, created_at, updated_at
            FROM task_queue_pollers
            WHERE tenant_id = $1
              AND queue_kind = $2
              AND task_queue = $3
              AND expires_at > $4
            ORDER BY last_seen_at DESC, poller_id ASC
            "#,
        )
        .bind(tenant_id)
        .bind(queue_kind.as_str())
        .bind(task_queue)
        .bind(active_at)
        .fetch_all(&self.pool)
        .await
        .context("failed to list queue pollers")?;

        rows.into_iter().map(Self::decode_queue_poller_row).collect()
    }

    pub async fn upsert_task_queue_throughput_policy(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
        backend: &str,
    ) -> Result<TaskQueueThroughputPolicyRecord> {
        let now = Utc::now();
        sqlx::query(
            r#"
            INSERT INTO task_queue_throughput_policies (
                tenant_id,
                queue_kind,
                task_queue,
                backend,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $5)
            ON CONFLICT (tenant_id, queue_kind, task_queue)
            DO UPDATE SET
                backend = EXCLUDED.backend,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(tenant_id)
        .bind(queue_kind.as_str())
        .bind(task_queue)
        .bind(backend)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to upsert task queue throughput policy")?;

        Ok(TaskQueueThroughputPolicyRecord {
            tenant_id: tenant_id.to_owned(),
            queue_kind,
            task_queue: task_queue.to_owned(),
            backend: backend.to_owned(),
            created_at: now,
            updated_at: now,
        })
    }

    pub async fn get_task_queue_throughput_policy(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
    ) -> Result<Option<TaskQueueThroughputPolicyRecord>> {
        let row = sqlx::query(
            r#"
            SELECT tenant_id, queue_kind, task_queue, backend, created_at, updated_at
            FROM task_queue_throughput_policies
            WHERE tenant_id = $1 AND queue_kind = $2 AND task_queue = $3
            "#,
        )
        .bind(tenant_id)
        .bind(queue_kind.as_str())
        .bind(task_queue)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load task queue throughput policy")?;

        row.map(Self::decode_task_queue_throughput_policy_row).transpose()
    }

    pub async fn enqueue_workflow_mailbox_message(
        &self,
        partition_id: i32,
        task_queue: &str,
        preferred_build_id: Option<&str>,
        event: &EventEnvelope<WorkflowEvent>,
        kind: WorkflowMailboxKind,
        message_id: Option<&str>,
        message_name: Option<&str>,
        payload: Option<&Value>,
    ) -> Result<Option<WorkflowMailboxRecord>> {
        let now = event.occurred_at;
        let accepted_seq = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO workflow_mailbox (
                tenant_id,
                workflow_instance_id,
                run_id,
                accepted_seq,
                kind,
                message_id,
                message_name,
                payload,
                source_event_id,
                source_event_type,
                source_event,
                status,
                consumed_at,
                created_at,
                updated_at
            )
            SELECT
                $1,
                $2,
                $3,
                COALESCE(MAX(accepted_seq), 0) + 1,
                $4,
                $5,
                $6,
                $7,
                $8,
                $9,
                $10,
                $11,
                NULL,
                $12,
                $12
            FROM workflow_mailbox
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            ON CONFLICT (source_event_id) DO NOTHING
            RETURNING accepted_seq
            "#,
        )
        .bind(&event.tenant_id)
        .bind(&event.instance_id)
        .bind(&event.run_id)
        .bind(kind.as_str())
        .bind(message_id)
        .bind(message_name)
        .bind(payload.cloned().map(Json))
        .bind(event.event_id)
        .bind(&event.event_type)
        .bind(Json(event.clone()))
        .bind(WorkflowMailboxStatus::Queued.as_str())
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .context("failed to enqueue workflow mailbox message")?;
        let Some(accepted_seq) = accepted_seq else {
            return Ok(None);
        };
        let accepted_seq = accepted_seq as u64;

        self.ensure_runnable_workflow_task(
            partition_id,
            task_queue,
            preferred_build_id,
            event,
            Some(accepted_seq),
            None,
        )
        .await?;

        Ok(Some(WorkflowMailboxRecord {
            tenant_id: event.tenant_id.clone(),
            instance_id: event.instance_id.clone(),
            run_id: event.run_id.clone(),
            accepted_seq,
            kind,
            message_id: message_id.map(str::to_owned),
            message_name: message_name.map(str::to_owned),
            payload: payload.cloned(),
            source_event_id: event.event_id,
            source_event_type: event.event_type.clone(),
            source_event: event.clone(),
            status: WorkflowMailboxStatus::Queued,
            consumed_at: None,
            created_at: now,
            updated_at: now,
        }))
    }

    pub async fn enqueue_workflow_task(
        &self,
        partition_id: i32,
        task_queue: &str,
        preferred_build_id: Option<&str>,
        event: &EventEnvelope<WorkflowEvent>,
    ) -> Result<Option<WorkflowTaskRecord>> {
        match &event.payload {
            WorkflowEvent::WorkflowTriggered { input } => {
                self.enqueue_workflow_mailbox_message(
                    partition_id,
                    task_queue,
                    preferred_build_id,
                    event,
                    WorkflowMailboxKind::Trigger,
                    None,
                    None,
                    Some(input),
                )
                .await?;
            }
            WorkflowEvent::SignalQueued { signal_id, signal_type, payload } => {
                self.enqueue_workflow_mailbox_message(
                    partition_id,
                    task_queue,
                    preferred_build_id,
                    event,
                    WorkflowMailboxKind::Signal,
                    Some(signal_id),
                    Some(signal_type),
                    Some(payload),
                )
                .await?;
            }
            WorkflowEvent::WorkflowUpdateRequested { update_id, update_name, payload } => {
                self.enqueue_workflow_mailbox_message(
                    partition_id,
                    task_queue,
                    preferred_build_id,
                    event,
                    WorkflowMailboxKind::Update,
                    Some(update_id),
                    Some(update_name),
                    Some(payload),
                )
                .await?;
            }
            WorkflowEvent::WorkflowCancellationRequested { reason } => {
                self.enqueue_workflow_mailbox_message(
                    partition_id,
                    task_queue,
                    preferred_build_id,
                    event,
                    WorkflowMailboxKind::CancelRequest,
                    None,
                    Some(reason),
                    None,
                )
                .await?;
            }
            WorkflowEvent::TimerFired { timer_id } => {
                self.enqueue_workflow_resume(
                    partition_id,
                    task_queue,
                    preferred_build_id,
                    event,
                    WorkflowResumeKind::TimerFired,
                    timer_id,
                    None,
                )
                .await?;
            }
            WorkflowEvent::ActivityTaskCompleted { activity_id, .. } => {
                self.enqueue_workflow_resume(
                    partition_id,
                    task_queue,
                    preferred_build_id,
                    event,
                    WorkflowResumeKind::ActivityTerminal,
                    activity_id,
                    Some("completed"),
                )
                .await?;
            }
            WorkflowEvent::ActivityTaskFailed { activity_id, .. } => {
                self.enqueue_workflow_resume(
                    partition_id,
                    task_queue,
                    preferred_build_id,
                    event,
                    WorkflowResumeKind::ActivityTerminal,
                    activity_id,
                    Some("failed"),
                )
                .await?;
            }
            WorkflowEvent::ActivityTaskTimedOut { activity_id, .. } => {
                self.enqueue_workflow_resume(
                    partition_id,
                    task_queue,
                    preferred_build_id,
                    event,
                    WorkflowResumeKind::ActivityTerminal,
                    activity_id,
                    Some("timed_out"),
                )
                .await?;
            }
            WorkflowEvent::ActivityTaskCancelled { activity_id, .. } => {
                self.enqueue_workflow_resume(
                    partition_id,
                    task_queue,
                    preferred_build_id,
                    event,
                    WorkflowResumeKind::ActivityTerminal,
                    activity_id,
                    Some("cancelled"),
                )
                .await?;
            }
            WorkflowEvent::ChildWorkflowCompleted { child_id, .. } => {
                self.enqueue_workflow_resume(
                    partition_id,
                    task_queue,
                    preferred_build_id,
                    event,
                    WorkflowResumeKind::ChildTerminal,
                    child_id,
                    Some("completed"),
                )
                .await?;
            }
            WorkflowEvent::ChildWorkflowFailed { child_id, .. } => {
                self.enqueue_workflow_resume(
                    partition_id,
                    task_queue,
                    preferred_build_id,
                    event,
                    WorkflowResumeKind::ChildTerminal,
                    child_id,
                    Some("failed"),
                )
                .await?;
            }
            WorkflowEvent::ChildWorkflowCancelled { child_id, .. } => {
                self.enqueue_workflow_resume(
                    partition_id,
                    task_queue,
                    preferred_build_id,
                    event,
                    WorkflowResumeKind::ChildTerminal,
                    child_id,
                    Some("cancelled"),
                )
                .await?;
            }
            WorkflowEvent::ChildWorkflowTerminated { child_id, .. } => {
                self.enqueue_workflow_resume(
                    partition_id,
                    task_queue,
                    preferred_build_id,
                    event,
                    WorkflowResumeKind::ChildTerminal,
                    child_id,
                    Some("terminated"),
                )
                .await?;
            }
            _ => return Ok(None),
        }

        let task_id = sqlx::query(
            r#"
            SELECT task_id
            FROM workflow_tasks
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            LIMIT 1
            "#,
        )
        .bind(&event.tenant_id)
        .bind(&event.instance_id)
        .bind(&event.run_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load workflow task after enqueue")?
        .map(|row| row.try_get("task_id").expect("task_id selected"));

        match task_id {
            Some(task_id) => self.get_workflow_task(task_id).await,
            None => Ok(None),
        }
    }

    pub async fn enqueue_workflow_resume(
        &self,
        partition_id: i32,
        task_queue: &str,
        preferred_build_id: Option<&str>,
        event: &EventEnvelope<WorkflowEvent>,
        kind: WorkflowResumeKind,
        ref_id: &str,
        status_label: Option<&str>,
    ) -> Result<Option<WorkflowResumeRecord>> {
        let now = event.occurred_at;
        let resume_seq = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO workflow_resumes (
                tenant_id,
                workflow_instance_id,
                run_id,
                resume_seq,
                kind,
                ref_id,
                status_label,
                source_event_id,
                source_event_type,
                source_event,
                status,
                consumed_at,
                created_at,
                updated_at
            )
            SELECT
                $1,
                $2,
                $3,
                COALESCE(MAX(resume_seq), 0) + 1,
                $4,
                $5,
                $6,
                $7,
                $8,
                $9,
                $10,
                NULL,
                $11,
                $11
            FROM workflow_resumes
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            ON CONFLICT (source_event_id) DO NOTHING
            RETURNING resume_seq
            "#,
        )
        .bind(&event.tenant_id)
        .bind(&event.instance_id)
        .bind(&event.run_id)
        .bind(kind.as_str())
        .bind(ref_id)
        .bind(status_label)
        .bind(event.event_id)
        .bind(&event.event_type)
        .bind(Json(event.clone()))
        .bind(WorkflowResumeStatus::Queued.as_str())
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .context("failed to enqueue workflow resume")?;
        let Some(resume_seq) = resume_seq else {
            return Ok(None);
        };
        let resume_seq = resume_seq as u64;

        self.ensure_runnable_workflow_task(
            partition_id,
            task_queue,
            preferred_build_id,
            event,
            None,
            Some(resume_seq),
        )
        .await?;

        Ok(Some(WorkflowResumeRecord {
            tenant_id: event.tenant_id.clone(),
            instance_id: event.instance_id.clone(),
            run_id: event.run_id.clone(),
            resume_seq,
            kind,
            ref_id: ref_id.to_owned(),
            status_label: status_label.map(str::to_owned),
            source_event_id: event.event_id,
            source_event_type: event.event_type.clone(),
            source_event: event.clone(),
            status: WorkflowResumeStatus::Queued,
            consumed_at: None,
            created_at: now,
            updated_at: now,
        }))
    }

    pub async fn get_next_workflow_mailbox_item(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
    ) -> Result<Option<WorkflowMailboxRecord>> {
        let row = sqlx::query(
            r#"
            SELECT tenant_id, workflow_instance_id, run_id, accepted_seq, kind, message_id,
                   message_name, payload, source_event_id, source_event_type, source_event,
                   status, consumed_at, created_at, updated_at
            FROM workflow_mailbox
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND status = 'queued'
            ORDER BY accepted_seq ASC
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load next workflow mailbox item")?;

        row.map(Self::decode_workflow_mailbox_row).transpose()
    }

    pub async fn list_next_workflow_mailbox_items(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        limit: usize,
    ) -> Result<Vec<WorkflowMailboxRecord>> {
        let limit =
            i64::try_from(limit.max(1)).context("workflow mailbox batch limit exceeds i64")?;
        let rows = sqlx::query(
            r#"
            SELECT tenant_id, workflow_instance_id, run_id, accepted_seq, kind, message_id,
                   message_name, payload, source_event_id, source_event_type, source_event,
                   status, consumed_at, created_at, updated_at
            FROM workflow_mailbox
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND status = 'queued'
            ORDER BY accepted_seq ASC
            LIMIT $4
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("failed to load workflow mailbox batch")?;

        rows.into_iter().map(Self::decode_workflow_mailbox_row).collect()
    }

    pub async fn get_next_workflow_resume_item(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
    ) -> Result<Option<WorkflowResumeRecord>> {
        let row = sqlx::query(
            r#"
            SELECT tenant_id, workflow_instance_id, run_id, resume_seq, kind, ref_id,
                   status_label, source_event_id, source_event_type, source_event,
                   status, consumed_at, created_at, updated_at
            FROM workflow_resumes
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND status = 'queued'
            ORDER BY resume_seq ASC
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load next workflow resume item")?;

        row.map(Self::decode_workflow_resume_row).transpose()
    }

    pub async fn list_next_workflow_resume_items(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        limit: usize,
    ) -> Result<Vec<WorkflowResumeRecord>> {
        let limit =
            i64::try_from(limit.max(1)).context("workflow resume batch limit exceeds i64")?;
        let rows = sqlx::query(
            r#"
            SELECT tenant_id, workflow_instance_id, run_id, resume_seq, kind, ref_id,
                   status_label, source_event_id, source_event_type, source_event,
                   status, consumed_at, created_at, updated_at
            FROM workflow_resumes
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND status = 'queued'
            ORDER BY resume_seq ASC
            LIMIT $4
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("failed to load workflow resume batch")?;

        rows.into_iter().map(Self::decode_workflow_resume_row).collect()
    }

    pub async fn mark_workflow_mailbox_item_consumed(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        accepted_seq: u64,
        consumed_at: DateTime<Utc>,
    ) -> Result<bool> {
        self.mark_workflow_mailbox_items_consumed_through(
            tenant_id,
            instance_id,
            run_id,
            accepted_seq,
            consumed_at,
        )
        .await
    }

    pub async fn mark_workflow_mailbox_items_consumed_through(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        max_accepted_seq: u64,
        consumed_at: DateTime<Utc>,
    ) -> Result<bool> {
        let updated = sqlx::query(
            r#"
            WITH consumed AS (
                UPDATE workflow_mailbox
                SET status = 'consumed',
                    consumed_at = $5,
                    updated_at = $5
                WHERE tenant_id = $1
                  AND workflow_instance_id = $2
                  AND run_id = $3
                  AND accepted_seq <= $4
                  AND status = 'queued'
                RETURNING accepted_seq
            )
            UPDATE workflow_tasks
            SET mailbox_consumed_seq = GREATEST(mailbox_consumed_seq, $4),
                mailbox_backlog = GREATEST(
                    mailbox_backlog - COALESCE((SELECT COUNT(*) FROM consumed), 0),
                    0
                ),
                updated_at = $5
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND EXISTS (SELECT 1 FROM consumed)
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(i64::try_from(max_accepted_seq).context("workflow mailbox sequence exceeds i64")?)
        .bind(consumed_at)
        .execute(&self.pool)
        .await
        .context("failed to mark workflow mailbox item consumed")?
        .rows_affected();

        Ok(updated > 0)
    }

    pub async fn mark_workflow_resume_item_consumed(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        resume_seq: u64,
        consumed_at: DateTime<Utc>,
    ) -> Result<bool> {
        self.mark_workflow_resume_items_consumed_through(
            tenant_id,
            instance_id,
            run_id,
            resume_seq,
            consumed_at,
        )
        .await
    }

    pub async fn mark_workflow_resume_items_consumed_through(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        max_resume_seq: u64,
        consumed_at: DateTime<Utc>,
    ) -> Result<bool> {
        let updated = sqlx::query(
            r#"
            WITH consumed AS (
                UPDATE workflow_resumes
                SET status = 'consumed',
                    consumed_at = $5,
                    updated_at = $5
                WHERE tenant_id = $1
                  AND workflow_instance_id = $2
                  AND run_id = $3
                  AND resume_seq <= $4
                  AND status = 'queued'
                RETURNING resume_seq
            )
            UPDATE workflow_tasks
            SET resume_consumed_seq = GREATEST(resume_consumed_seq, $4),
                resume_backlog = GREATEST(
                    resume_backlog - COALESCE((SELECT COUNT(*) FROM consumed), 0),
                    0
                ),
                updated_at = $5
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND EXISTS (SELECT 1 FROM consumed)
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(i64::try_from(max_resume_seq).context("workflow resume sequence exceeds i64")?)
        .bind(consumed_at)
        .execute(&self.pool)
        .await
        .context("failed to mark workflow resume item consumed")?
        .rows_affected();

        Ok(updated > 0)
    }

    pub async fn get_workflow_task(&self, task_id: Uuid) -> Result<Option<WorkflowTaskRecord>> {
        let row = sqlx::query(
            r#"
            SELECT
                task_id,
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                definition_version,
                artifact_hash,
                partition_id,
                task_queue,
                preferred_build_id,
                status,
                source_event_id,
                source_event_type,
                source_event,
                mailbox_consumed_seq,
                resume_consumed_seq,
                mailbox_high_watermark,
                resume_high_watermark,
                mailbox_backlog,
                resume_backlog,
                lease_poller_id,
                lease_build_id,
                lease_expires_at,
                attempt_count,
                last_error,
                created_at,
                updated_at
            FROM workflow_tasks
            WHERE task_id = $1
            "#,
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load workflow task")?;

        row.map(Self::decode_workflow_task_row).transpose()
    }

    pub async fn count_workflow_tasks_for_run(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
    ) -> Result<u64> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM workflow_tasks
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .fetch_one(&self.pool)
        .await
        .context("failed to count workflow tasks for run")?;

        Ok(count as u64)
    }

    pub async fn lease_next_workflow_task(
        &self,
        partition_id: i32,
        poller_id: &str,
        build_id: &str,
        lease_ttl: chrono::Duration,
    ) -> Result<Option<WorkflowTaskRecord>> {
        let now = Utc::now();
        let rows = sqlx::query(
            r#"
            SELECT
                task_id,
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                definition_version,
                artifact_hash,
                partition_id,
                task_queue,
                preferred_build_id,
                status,
                source_event_id,
                source_event_type,
                source_event,
                mailbox_consumed_seq,
                resume_consumed_seq,
                mailbox_high_watermark,
                resume_high_watermark,
                mailbox_backlog,
                resume_backlog,
                lease_poller_id,
                lease_build_id,
                lease_expires_at,
                attempt_count,
                last_error,
                created_at,
                updated_at
            FROM workflow_tasks
            WHERE partition_id = $1
              AND (mailbox_backlog > 0 OR resume_backlog > 0)
              AND (status = 'pending' OR (status = 'leased' AND lease_expires_at <= $2))
            ORDER BY
                CASE WHEN preferred_build_id = $3 THEN 0 ELSE 1 END,
                created_at ASC
            LIMIT 128
            "#,
        )
        .bind(partition_id)
        .bind(now)
        .bind(build_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list leaseable workflow tasks")?;

        for row in rows {
            let record = Self::decode_workflow_task_row(row)?;
            if !self
                .is_build_compatible_with_queue(
                    &record.tenant_id,
                    TaskQueueKind::Workflow,
                    &record.task_queue,
                    build_id,
                )
                .await?
            {
                continue;
            }
            if !self
                .workflow_build_supports_artifact(
                    &record.tenant_id,
                    &record.task_queue,
                    build_id,
                    record.artifact_hash.as_deref(),
                )
                .await?
            {
                continue;
            }

            let lease_expires_at = now + lease_ttl;
            let updated = sqlx::query(
                r#"
                UPDATE workflow_tasks
                SET status = 'leased',
                    lease_poller_id = $2,
                    lease_build_id = $3,
                    lease_expires_at = $4,
                    updated_at = $5
                WHERE task_id = $1
                  AND (mailbox_backlog > 0 OR resume_backlog > 0)
                  AND (status = 'pending' OR (status = 'leased' AND lease_expires_at <= $5))
                "#,
            )
            .bind(record.task_id)
            .bind(poller_id)
            .bind(build_id)
            .bind(lease_expires_at)
            .bind(now)
            .execute(&self.pool)
            .await
            .context("failed to lease workflow task")?
            .rows_affected();

            if updated == 0 {
                continue;
            }
            return self.get_workflow_task(record.task_id).await;
        }

        Ok(None)
    }

    pub async fn complete_workflow_task(
        &self,
        task_id: Uuid,
        poller_id: &str,
        build_id: &str,
        completed_at: DateTime<Utc>,
    ) -> Result<bool> {
        let updated = sqlx::query(
            r#"
            UPDATE workflow_tasks
            SET mailbox_backlog = GREATEST(mailbox_high_watermark - mailbox_consumed_seq, 0),
                resume_backlog = GREATEST(resume_high_watermark - resume_consumed_seq, 0),
                status = CASE
                    WHEN mailbox_high_watermark > mailbox_consumed_seq
                      OR resume_high_watermark > resume_consumed_seq THEN 'pending'
                    ELSE 'completed'
                END,
                lease_poller_id = CASE
                    WHEN mailbox_high_watermark > mailbox_consumed_seq
                      OR resume_high_watermark > resume_consumed_seq THEN NULL
                    ELSE $2
                END,
                lease_build_id = CASE
                    WHEN mailbox_high_watermark > mailbox_consumed_seq
                      OR resume_high_watermark > resume_consumed_seq THEN NULL
                    ELSE $3
                END,
                lease_expires_at = NULL,
                updated_at = $4
            WHERE task_id = $1
              AND status = 'leased'
              AND lease_poller_id = $2
              AND lease_build_id = $3
            "#,
        )
        .bind(task_id)
        .bind(poller_id)
        .bind(build_id)
        .bind(completed_at)
        .execute(&self.pool)
        .await
        .context("failed to complete workflow task")?
        .rows_affected();

        Ok(updated > 0)
    }

    pub async fn fail_workflow_task(
        &self,
        task_id: Uuid,
        poller_id: &str,
        build_id: &str,
        error: &str,
        failed_at: DateTime<Utc>,
    ) -> Result<bool> {
        let updated = sqlx::query(
            r#"
            UPDATE workflow_tasks
            SET mailbox_backlog = GREATEST(mailbox_high_watermark - mailbox_consumed_seq, 0),
                resume_backlog = GREATEST(resume_high_watermark - resume_consumed_seq, 0),
                status = CASE
                    WHEN mailbox_high_watermark > mailbox_consumed_seq
                      OR resume_high_watermark > resume_consumed_seq THEN 'pending'
                    ELSE 'completed'
                END,
                lease_poller_id = NULL,
                lease_build_id = NULL,
                lease_expires_at = NULL,
                attempt_count = attempt_count + 1,
                last_error = $4,
                updated_at = $5
            WHERE task_id = $1
              AND status = 'leased'
              AND lease_poller_id = $2
              AND lease_build_id = $3
            "#,
        )
        .bind(task_id)
        .bind(poller_id)
        .bind(build_id)
        .bind(error)
        .bind(failed_at)
        .execute(&self.pool)
        .await
        .context("failed to fail workflow task")?
        .rows_affected();

        Ok(updated > 0)
    }

    pub async fn park_workflow_task(
        &self,
        task_id: Uuid,
        poller_id: &str,
        build_id: &str,
        parked_at: DateTime<Utc>,
    ) -> Result<bool> {
        let updated = sqlx::query(
            r#"
            UPDATE workflow_tasks
            SET status = 'completed',
                lease_poller_id = NULL,
                lease_build_id = NULL,
                lease_expires_at = NULL,
                updated_at = $4
            WHERE task_id = $1
              AND status = 'leased'
              AND lease_poller_id = $2
              AND lease_build_id = $3
            "#,
        )
        .bind(task_id)
        .bind(poller_id)
        .bind(build_id)
        .bind(parked_at)
        .execute(&self.pool)
        .await
        .context("failed to park workflow task")?
        .rows_affected();

        Ok(updated > 0)
    }

    async fn ensure_runnable_workflow_task(
        &self,
        partition_id: i32,
        task_queue: &str,
        preferred_build_id: Option<&str>,
        event: &EventEnvelope<WorkflowEvent>,
        mailbox_high_watermark: Option<u64>,
        resume_high_watermark: Option<u64>,
    ) -> Result<()> {
        let task_id = Uuid::now_v7();
        let occurred_at = event.occurred_at;
        let mailbox_increment = if mailbox_high_watermark.is_some() { 1_i64 } else { 0_i64 };
        let resume_increment = if resume_high_watermark.is_some() { 1_i64 } else { 0_i64 };
        let mailbox_high_watermark = mailbox_high_watermark
            .map(i64::try_from)
            .transpose()
            .context("workflow mailbox high watermark exceeds i64")?;
        let resume_high_watermark = resume_high_watermark
            .map(i64::try_from)
            .transpose()
            .context("workflow resume high watermark exceeds i64")?;
        let definition_version = i32::try_from(event.definition_version)
            .context("workflow task definition version exceeds i32")?;
        sqlx::query(
            r#"
            INSERT INTO workflow_tasks (
                task_id,
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                definition_version,
                artifact_hash,
                partition_id,
                task_queue,
                preferred_build_id,
                status,
                source_event_id,
                source_event_type,
                source_event,
                mailbox_consumed_seq,
                resume_consumed_seq,
                mailbox_high_watermark,
                resume_high_watermark,
                mailbox_backlog,
                resume_backlog,
                lease_poller_id,
                lease_build_id,
                lease_expires_at,
                attempt_count,
                last_error,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                'pending', $11, $12, $13,
                0, 0, COALESCE($14, 0), COALESCE($15, 0), $16, $17,
                NULL, NULL, NULL, 0, NULL, $18, $18
            )
            ON CONFLICT (tenant_id, workflow_instance_id, run_id)
            DO UPDATE SET
                workflow_id = EXCLUDED.workflow_id,
                definition_version = EXCLUDED.definition_version,
                artifact_hash = EXCLUDED.artifact_hash,
                partition_id = EXCLUDED.partition_id,
                task_queue = EXCLUDED.task_queue,
                preferred_build_id = EXCLUDED.preferred_build_id,
                source_event_id = EXCLUDED.source_event_id,
                source_event_type = EXCLUDED.source_event_type,
                source_event = EXCLUDED.source_event,
                mailbox_high_watermark = GREATEST(
                    workflow_tasks.mailbox_high_watermark,
                    COALESCE(EXCLUDED.mailbox_high_watermark, workflow_tasks.mailbox_high_watermark)
                ),
                resume_high_watermark = GREATEST(
                    workflow_tasks.resume_high_watermark,
                    COALESCE(EXCLUDED.resume_high_watermark, workflow_tasks.resume_high_watermark)
                ),
                mailbox_backlog = workflow_tasks.mailbox_backlog + $16,
                resume_backlog = workflow_tasks.resume_backlog + $17,
                status = CASE
                    WHEN workflow_tasks.status = 'leased' THEN workflow_tasks.status
                    ELSE 'pending'
                END,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(task_id)
        .bind(&event.tenant_id)
        .bind(&event.instance_id)
        .bind(&event.run_id)
        .bind(&event.definition_id)
        .bind(definition_version)
        .bind(&event.artifact_hash)
        .bind(partition_id)
        .bind(task_queue)
        .bind(preferred_build_id)
        .bind(event.event_id)
        .bind(&event.event_type)
        .bind(Json(event.clone()))
        .bind(mailbox_high_watermark)
        .bind(resume_high_watermark)
        .bind(mailbox_increment)
        .bind(resume_increment)
        .bind(occurred_at)
        .execute(&self.pool)
        .await
        .context("failed to ensure runnable workflow task")?;
        Ok(())
    }

    pub async fn inspect_task_queue(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
        active_at: DateTime<Utc>,
    ) -> Result<TaskQueueInspection> {
        let compatibility_sets =
            self.list_compatibility_sets(tenant_id, queue_kind.clone(), task_queue).await?;
        let default_set_id = compatibility_sets
            .iter()
            .find(|record| record.is_default)
            .map(|record| record.set_id.clone());
        let compatible_build_ids = self
            .list_default_compatible_build_ids(tenant_id, queue_kind.clone(), task_queue)
            .await?;
        let registered_builds =
            self.list_task_queue_builds(tenant_id, queue_kind.clone(), task_queue).await?;
        let pollers =
            self.list_queue_pollers(tenant_id, queue_kind.clone(), task_queue, active_at).await?;
        let (backlog, oldest_backlog_at) =
            self.task_queue_backlog(tenant_id, queue_kind.clone(), task_queue).await?;
        let sticky_effectiveness =
            self.task_queue_sticky_effectiveness(tenant_id, queue_kind.clone(), task_queue).await?;
        let resume_coalescing =
            self.task_queue_resume_coalescing(tenant_id, queue_kind.clone(), task_queue).await?;
        let activity_completion_metrics = self
            .task_queue_activity_completion_metrics(tenant_id, queue_kind.clone(), task_queue)
            .await?;
        let throughput_policy = self
            .get_task_queue_throughput_policy(tenant_id, queue_kind.clone(), task_queue)
            .await?;

        Ok(TaskQueueInspection {
            tenant_id: tenant_id.to_owned(),
            queue_kind,
            task_queue: task_queue.to_owned(),
            backlog,
            oldest_backlog_at,
            sticky_effectiveness,
            resume_coalescing,
            activity_completion_metrics,
            default_set_id,
            throughput_policy,
            compatible_build_ids,
            registered_builds,
            compatibility_sets,
            pollers,
        })
    }

    async fn list_compatibility_set_build_ids(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
        set_id: &str,
    ) -> Result<Vec<String>> {
        let rows = sqlx::query(
            r#"
            SELECT build_id
            FROM task_queue_compatibility_set_builds
            WHERE tenant_id = $1 AND queue_kind = $2 AND task_queue = $3 AND set_id = $4
            ORDER BY build_id ASC
            "#,
        )
        .bind(tenant_id)
        .bind(queue_kind.as_str())
        .bind(task_queue)
        .bind(set_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list compatibility set build ids")?;

        Ok(rows
            .into_iter()
            .map(|row| row.try_get("build_id").expect("build_id is selected"))
            .collect())
    }

    async fn task_queue_backlog(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
    ) -> Result<(u64, Option<DateTime<Utc>>)> {
        match queue_kind {
            TaskQueueKind::Workflow => {
                let row = sqlx::query(
                    r#"
                    SELECT COUNT(*) AS backlog, MIN(created_at) AS oldest_backlog_at
                    FROM workflow_tasks
                    WHERE tenant_id = $1
                      AND task_queue = $2
                      AND status = 'pending'
                      AND (mailbox_backlog > 0 OR resume_backlog > 0)
                    "#,
                )
                .bind(tenant_id)
                .bind(task_queue)
                .fetch_one(&self.pool)
                .await
                .context("failed to inspect workflow queue backlog")?;
                Ok((
                    row.try_get::<i64, _>("backlog").context("workflow backlog missing")? as u64,
                    row.try_get("oldest_backlog_at")
                        .context("workflow oldest backlog timestamp missing")?,
                ))
            }
            TaskQueueKind::Activity => {
                let row = sqlx::query(
                    r#"
                    SELECT COUNT(*) AS backlog, MIN(scheduled_at) AS oldest_backlog_at
                    FROM workflow_activities
                    WHERE tenant_id = $1 AND task_queue = $2 AND status = 'scheduled'
                    "#,
                )
                .bind(tenant_id)
                .bind(task_queue)
                .fetch_one(&self.pool)
                .await
                .context("failed to inspect activity queue backlog")?;
                Ok((
                    row.try_get::<i64, _>("backlog").context("activity backlog missing")? as u64,
                    row.try_get("oldest_backlog_at")
                        .context("activity oldest backlog timestamp missing")?,
                ))
            }
        }
    }

    async fn task_queue_sticky_effectiveness(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
    ) -> Result<Option<StickyQueueEffectiveness>> {
        if queue_kind != TaskQueueKind::Workflow {
            return Ok(None);
        }

        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (
                    WHERE preferred_build_id IS NOT NULL
                      AND preferred_build_id <> ''
                      AND lease_build_id IS NOT NULL
                      AND lease_build_id <> ''
                ) AS sticky_dispatch_count,
                COUNT(*) FILTER (
                    WHERE preferred_build_id IS NOT NULL
                      AND preferred_build_id <> ''
                      AND lease_build_id IS NOT NULL
                      AND lease_build_id <> ''
                      AND preferred_build_id = lease_build_id
                ) AS sticky_hit_count,
                COUNT(*) FILTER (
                    WHERE preferred_build_id IS NOT NULL
                      AND preferred_build_id <> ''
                      AND lease_build_id IS NOT NULL
                      AND lease_build_id <> ''
                      AND preferred_build_id <> lease_build_id
                ) AS sticky_fallback_count
            FROM workflow_tasks
            WHERE tenant_id = $1 AND task_queue = $2
            "#,
        )
        .bind(tenant_id)
        .bind(task_queue)
        .fetch_one(&self.pool)
        .await
        .context("failed to inspect workflow queue sticky effectiveness")?;

        let sticky_dispatch_count =
            row.try_get::<i64, _>("sticky_dispatch_count")
                .context("workflow sticky_dispatch_count missing")? as u64;
        let sticky_hit_count = row
            .try_get::<i64, _>("sticky_hit_count")
            .context("workflow sticky_hit_count missing")? as u64;
        let sticky_fallback_count =
            row.try_get::<i64, _>("sticky_fallback_count")
                .context("workflow sticky_fallback_count missing")? as u64;
        let sticky_hit_rate = if sticky_dispatch_count == 0 {
            0.0
        } else {
            sticky_hit_count as f64 / sticky_dispatch_count as f64
        };
        let sticky_fallback_rate = if sticky_dispatch_count == 0 {
            0.0
        } else {
            sticky_fallback_count as f64 / sticky_dispatch_count as f64
        };

        Ok(Some(StickyQueueEffectiveness {
            sticky_dispatch_count,
            sticky_hit_count,
            sticky_fallback_count,
            sticky_hit_rate,
            sticky_fallback_rate,
        }))
    }

    async fn task_queue_resume_coalescing(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
    ) -> Result<Option<ResumeCoalescingMetrics>> {
        if queue_kind != TaskQueueKind::Workflow {
            return Ok(None);
        }

        let row = sqlx::query(
            r#"
            SELECT
                (SELECT COUNT(*)
                 FROM workflow_tasks
                 WHERE tenant_id = $1 AND task_queue = $2) AS workflow_task_rows,
                (SELECT COUNT(*)
                 FROM workflow_resumes resumes
                 WHERE resumes.tenant_id = $1
                   AND resumes.status = 'queued'
                   AND EXISTS (
                     SELECT 1
                     FROM workflow_tasks tasks
                     WHERE tasks.tenant_id = resumes.tenant_id
                       AND tasks.task_queue = $2
                       AND tasks.workflow_instance_id = resumes.workflow_instance_id
                       AND tasks.run_id = resumes.run_id
                   )) AS queued_resume_rows,
                (SELECT COUNT(*)
                 FROM workflow_resumes resumes
                 WHERE resumes.tenant_id = $1
                   AND EXISTS (
                     SELECT 1
                     FROM workflow_tasks tasks
                     WHERE tasks.tenant_id = resumes.tenant_id
                       AND tasks.task_queue = $2
                       AND tasks.workflow_instance_id = resumes.workflow_instance_id
                       AND tasks.run_id = resumes.run_id
                   )) AS total_resume_rows
            "#,
        )
        .bind(tenant_id)
        .bind(task_queue)
        .fetch_one(&self.pool)
        .await
        .context("failed to inspect workflow queue resume coalescing")?;

        let workflow_task_rows = row
            .try_get::<i64, _>("workflow_task_rows")
            .context("workflow task rows missing")? as u64;
        let queued_resume_rows = row
            .try_get::<i64, _>("queued_resume_rows")
            .context("queued resume rows missing")? as u64;
        let total_resume_rows =
            row.try_get::<i64, _>("total_resume_rows").context("total resume rows missing")? as u64;

        Ok(Some(ResumeCoalescingMetrics {
            workflow_task_rows,
            queued_resume_rows,
            total_resume_rows,
            resume_rows_per_task_row: if workflow_task_rows == 0 {
                0.0
            } else {
                total_resume_rows as f64 / workflow_task_rows as f64
            },
        }))
    }

    async fn task_queue_activity_completion_metrics(
        &self,
        tenant_id: &str,
        queue_kind: TaskQueueKind,
        task_queue: &str,
    ) -> Result<Option<ActivityCompletionMetrics>> {
        if queue_kind != TaskQueueKind::Activity {
            return Ok(None);
        }

        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE status = 'completed') AS completed,
                COUNT(*) FILTER (WHERE status = 'failed') AS failed,
                COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled,
                COUNT(*) FILTER (WHERE status = 'timed_out') AS timed_out,
                AVG(EXTRACT(EPOCH FROM (started_at - scheduled_at)) * 1000)
                    FILTER (WHERE started_at IS NOT NULL) AS avg_schedule_to_start_latency_ms,
                MAX(EXTRACT(EPOCH FROM (started_at - scheduled_at)) * 1000)
                    FILTER (WHERE started_at IS NOT NULL) AS max_schedule_to_start_latency_ms,
                AVG(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000)
                    FILTER (WHERE completed_at IS NOT NULL AND started_at IS NOT NULL)
                    AS avg_start_to_close_latency_ms,
                MAX(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000)
                    FILTER (WHERE completed_at IS NOT NULL AND started_at IS NOT NULL)
                    AS max_start_to_close_latency_ms
            FROM workflow_activities
            WHERE tenant_id = $1 AND task_queue = $2
            "#,
        )
        .bind(tenant_id)
        .bind(task_queue)
        .fetch_one(&self.pool)
        .await
        .context("failed to inspect activity queue completion metrics")?;

        Ok(Some(ActivityCompletionMetrics {
            completed: row.try_get::<i64, _>("completed").context("completed count missing")?
                as u64,
            failed: row.try_get::<i64, _>("failed").context("failed count missing")? as u64,
            cancelled: row.try_get::<i64, _>("cancelled").context("cancelled count missing")?
                as u64,
            timed_out: row.try_get::<i64, _>("timed_out").context("timed_out count missing")?
                as u64,
            avg_schedule_to_start_latency_ms: row
                .try_get::<Option<f64>, _>("avg_schedule_to_start_latency_ms")
                .context("avg schedule-to-start latency missing")?
                .unwrap_or(0.0),
            max_schedule_to_start_latency_ms: row
                .try_get::<Option<f64>, _>("max_schedule_to_start_latency_ms")
                .context("max schedule-to-start latency missing")?
                .unwrap_or(0.0) as u64,
            avg_start_to_close_latency_ms: row
                .try_get::<Option<f64>, _>("avg_start_to_close_latency_ms")
                .context("avg start-to-close latency missing")?
                .unwrap_or(0.0),
            max_start_to_close_latency_ms: row
                .try_get::<Option<f64>, _>("max_start_to_close_latency_ms")
                .context("max start-to-close latency missing")?
                .unwrap_or(0.0) as u64,
        }))
    }

    pub async fn get_request_dedup(
        &self,
        tenant_id: &str,
        instance_id: Option<&str>,
        operation: &str,
        request_id: &str,
    ) -> Result<Option<RequestDedupRecord>> {
        let row = sqlx::query(
            r#"
            SELECT tenant_id, workflow_instance_id, operation, request_id, request_hash, response, created_at, updated_at
            FROM workflow_request_dedup
            WHERE tenant_id = $1
              AND workflow_instance_id IS NOT DISTINCT FROM $2
              AND operation = $3
              AND request_id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(operation)
        .bind(request_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load request dedup record")?;

        row.map(Self::decode_request_dedup_row).transpose()
    }

    pub async fn upsert_request_dedup(
        &self,
        tenant_id: &str,
        instance_id: Option<&str>,
        operation: &str,
        request_id: &str,
        request_hash: &str,
        response: &Value,
    ) -> Result<RequestDedupRecord> {
        let now = Utc::now();
        sqlx::query(
            r#"
            INSERT INTO workflow_request_dedup (
                tenant_id,
                workflow_instance_id,
                operation,
                request_id,
                request_hash,
                response,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
            ON CONFLICT (tenant_id, workflow_instance_id, operation, request_id)
            DO UPDATE SET
                request_hash = EXCLUDED.request_hash,
                response = EXCLUDED.response,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(operation)
        .bind(request_id)
        .bind(request_hash)
        .bind(Json(response))
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to upsert request dedup record")?;

        Ok(RequestDedupRecord {
            tenant_id: tenant_id.to_owned(),
            instance_id: instance_id.map(str::to_owned),
            operation: operation.to_owned(),
            request_id: request_id.to_owned(),
            request_hash: request_hash.to_owned(),
            response: response.clone(),
            created_at: now,
            updated_at: now,
        })
    }

    pub async fn count_activity_attempts_for_run(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
    ) -> Result<u64> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM workflow_activities
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .fetch_one(&self.pool)
        .await
        .context("failed to count workflow activity attempts")?;

        Ok(count as u64)
    }

    pub async fn queue_signal(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        signal_id: &str,
        signal_type: &str,
        dedupe_key: Option<&str>,
        payload: &Value,
        source_event_id: Uuid,
        enqueued_at: DateTime<Utc>,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            INSERT INTO workflow_signals (
                tenant_id,
                workflow_instance_id,
                run_id,
                signal_id,
                signal_type,
                dedupe_key,
                payload,
                status,
                source_event_id,
                dispatch_event_id,
                consumed_event_id,
                enqueued_at,
                consumed_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, NULL, $10, NULL, $10
            )
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(signal_id)
        .bind(signal_type)
        .bind(dedupe_key)
        .bind(Json(payload))
        .bind(WorkflowSignalStatus::Queued.as_str())
        .bind(source_event_id)
        .bind(enqueued_at)
        .execute(&self.pool)
        .await
        .context("failed to queue workflow signal")?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn delete_signal(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        signal_id: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            DELETE FROM workflow_signals
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3 AND signal_id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(signal_id)
        .execute(&self.pool)
        .await
        .context("failed to delete workflow signal")?;

        Ok(())
    }

    pub async fn list_signals_for_run(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
    ) -> Result<Vec<WorkflowSignalRecord>> {
        self.list_signals_for_run_page(tenant_id, instance_id, run_id, i64::MAX, 0).await
    }

    pub async fn count_signals_for_run(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
    ) -> Result<u64> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM workflow_signals
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .fetch_one(&self.pool)
        .await
        .context("failed to count workflow signals for run")?;

        Ok(count as u64)
    }

    pub async fn list_signals_for_run_page(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<WorkflowSignalRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT
                tenant_id,
                workflow_instance_id,
                run_id,
                signal_id,
                signal_type,
                dedupe_key,
                payload,
                status,
                source_event_id,
                dispatch_event_id,
                consumed_event_id,
                enqueued_at,
                consumed_at,
                updated_at
            FROM workflow_signals
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            ORDER BY enqueued_at ASC, signal_id ASC
            LIMIT $4 OFFSET $5
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .context("failed to list workflow signals for run")?;

        rows.into_iter().map(Self::decode_signal_row).collect()
    }

    pub async fn get_oldest_pending_signal(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
    ) -> Result<Option<WorkflowSignalRecord>> {
        let row = sqlx::query(
            r#"
            SELECT
                tenant_id,
                workflow_instance_id,
                run_id,
                signal_id,
                signal_type,
                dedupe_key,
                payload,
                status,
                source_event_id,
                dispatch_event_id,
                consumed_event_id,
                enqueued_at,
                consumed_at,
                updated_at
            FROM workflow_signals
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND status IN ('queued', 'dispatching')
            ORDER BY enqueued_at ASC, signal_id ASC
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load oldest pending workflow signal")?;

        row.map(Self::decode_signal_row).transpose()
    }

    pub async fn get_signal(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        signal_id: &str,
    ) -> Result<Option<WorkflowSignalRecord>> {
        let row = sqlx::query(
            r#"
            SELECT
                tenant_id,
                workflow_instance_id,
                run_id,
                signal_id,
                signal_type,
                dedupe_key,
                payload,
                status,
                source_event_id,
                dispatch_event_id,
                consumed_event_id,
                enqueued_at,
                consumed_at,
                updated_at
            FROM workflow_signals
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND signal_id = $4
              AND status != 'consumed'
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(signal_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load workflow signal")?;

        row.map(Self::decode_signal_row).transpose()
    }

    pub async fn claim_signal_for_dispatch(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        signal_id: &str,
    ) -> Result<Option<(WorkflowSignalRecord, bool)>> {
        let mut tx =
            self.pool.begin().await.context("failed to begin signal dispatch transaction")?;
        let row = sqlx::query(
            r#"
            SELECT
                tenant_id,
                workflow_instance_id,
                run_id,
                signal_id,
                signal_type,
                dedupe_key,
                payload,
                status,
                source_event_id,
                dispatch_event_id,
                consumed_event_id,
                enqueued_at,
                consumed_at,
                updated_at
            FROM workflow_signals
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND signal_id = $4
            FOR UPDATE
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(signal_id)
        .fetch_optional(&mut *tx)
        .await
        .context("failed to load workflow signal for dispatch claim")?;

        let Some(row) = row else {
            tx.commit().await.context("failed to commit empty signal dispatch transaction")?;
            return Ok(None);
        };

        let mut signal = Self::decode_signal_row(row)?;
        let mut newly_claimed = false;
        match signal.status {
            WorkflowSignalStatus::Queued => {
                let dispatch_event_id = Uuid::now_v7();
                let now = Utc::now();
                sqlx::query(
                    r#"
                    UPDATE workflow_signals
                    SET status = $5,
                        dispatch_event_id = $6,
                        updated_at = $7
                    WHERE tenant_id = $1
                      AND workflow_instance_id = $2
                      AND run_id = $3
                      AND signal_id = $4
                    "#,
                )
                .bind(tenant_id)
                .bind(instance_id)
                .bind(run_id)
                .bind(signal_id)
                .bind(WorkflowSignalStatus::Dispatching.as_str())
                .bind(dispatch_event_id)
                .bind(now)
                .execute(&mut *tx)
                .await
                .context("failed to mark workflow signal dispatching")?;
                signal.status = WorkflowSignalStatus::Dispatching;
                signal.dispatch_event_id = Some(dispatch_event_id);
                signal.updated_at = now;
                newly_claimed = true;
            }
            WorkflowSignalStatus::Dispatching => {}
            WorkflowSignalStatus::Consumed => {
                tx.commit()
                    .await
                    .context("failed to commit consumed signal dispatch transaction")?;
                return Ok(None);
            }
        }

        tx.commit().await.context("failed to commit signal dispatch transaction")?;
        Ok(Some((signal, newly_claimed)))
    }

    pub async fn mark_signal_consumed(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        signal_id: &str,
        consumed_event_id: Uuid,
        consumed_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE workflow_signals
            SET status = $5,
                consumed_event_id = $6,
                consumed_at = $7,
                updated_at = $7
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND signal_id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(signal_id)
        .bind(WorkflowSignalStatus::Consumed.as_str())
        .bind(consumed_event_id)
        .bind(consumed_at)
        .execute(&self.pool)
        .await
        .context("failed to mark workflow signal consumed")?;

        Ok(())
    }

    pub async fn mark_signals_consumed(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        consumed_signals: &[ConsumedSignalRecord],
    ) -> Result<()> {
        if consumed_signals.is_empty() {
            return Ok(());
        }

        let signal_ids =
            consumed_signals.iter().map(|signal| signal.signal_id.as_str()).collect::<Vec<_>>();
        let consumed_event_ids =
            consumed_signals.iter().map(|signal| signal.consumed_event_id).collect::<Vec<_>>();
        let consumed_ats =
            consumed_signals.iter().map(|signal| signal.consumed_at).collect::<Vec<_>>();

        sqlx::query(
            r#"
            WITH consumed(signal_id, consumed_event_id, consumed_at) AS (
                SELECT *
                FROM UNNEST($4::text[], $5::uuid[], $6::timestamptz[])
            )
            UPDATE workflow_signals signals
            SET status = $7,
                consumed_event_id = consumed.consumed_event_id,
                consumed_at = consumed.consumed_at,
                updated_at = consumed.consumed_at
            FROM consumed
            WHERE signals.tenant_id = $1
              AND signals.workflow_instance_id = $2
              AND signals.run_id = $3
              AND signals.signal_id = consumed.signal_id
              AND signals.status != $7
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(&signal_ids)
        .bind(&consumed_event_ids)
        .bind(&consumed_ats)
        .bind(WorkflowSignalStatus::Consumed.as_str())
        .execute(&self.pool)
        .await
        .context("failed to mark workflow signals consumed")?;

        Ok(())
    }

    pub async fn queue_update(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        update_id: &str,
        update_name: &str,
        request_id: Option<&str>,
        payload: &Value,
        source_event_id: Uuid,
        requested_at: DateTime<Utc>,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            INSERT INTO workflow_updates (
                tenant_id,
                workflow_instance_id,
                run_id,
                update_id,
                update_name,
                request_id,
                payload,
                status,
                output,
                error,
                source_event_id,
                accepted_event_id,
                completed_event_id,
                requested_at,
                accepted_at,
                completed_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'requested', NULL, NULL, $8, NULL, NULL, $9, NULL, NULL, $9)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(update_id)
        .bind(update_name)
        .bind(request_id)
        .bind(Json(payload))
        .bind(source_event_id)
        .bind(requested_at)
        .execute(&self.pool)
        .await
        .context("failed to queue workflow update")?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn get_update(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        update_id: &str,
    ) -> Result<Option<WorkflowUpdateRecord>> {
        let row = sqlx::query(
            r#"
            SELECT tenant_id, workflow_instance_id, run_id, update_id, update_name, request_id,
                   payload, status, output, error, source_event_id, accepted_event_id,
                   completed_event_id, requested_at, accepted_at, completed_at, updated_at
            FROM workflow_updates
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3 AND update_id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(update_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load workflow update")?;

        row.map(Self::decode_update_row).transpose()
    }

    pub async fn list_updates_for_run(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
    ) -> Result<Vec<WorkflowUpdateRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT tenant_id, workflow_instance_id, run_id, update_id, update_name, request_id,
                   payload, status, output, error, source_event_id, accepted_event_id,
                   completed_event_id, requested_at, accepted_at, completed_at, updated_at
            FROM workflow_updates
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            ORDER BY requested_at ASC, update_id ASC
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list workflow updates")?;

        rows.into_iter().map(Self::decode_update_row).collect()
    }

    pub async fn get_oldest_requested_update(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
    ) -> Result<Option<WorkflowUpdateRecord>> {
        let row = sqlx::query(
            r#"
            SELECT tenant_id, workflow_instance_id, run_id, update_id, update_name, request_id,
                   payload, status, output, error, source_event_id, accepted_event_id,
                   completed_event_id, requested_at, accepted_at, completed_at, updated_at
            FROM workflow_updates
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
              AND status = 'requested'
            ORDER BY requested_at ASC, update_id ASC
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load oldest requested workflow update")?;

        row.map(Self::decode_update_row).transpose()
    }

    pub async fn accept_update(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        update_id: &str,
        accepted_event_id: Uuid,
        accepted_at: DateTime<Utc>,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_updates
            SET status = 'accepted',
                accepted_event_id = $5,
                accepted_at = $6,
                updated_at = $6
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND update_id = $4
              AND status = 'requested'
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(update_id)
        .bind(accepted_event_id)
        .bind(accepted_at)
        .execute(&self.pool)
        .await
        .context("failed to accept workflow update")?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn complete_update(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        update_id: &str,
        output: Option<&Value>,
        error: Option<&str>,
        completed_event_id: Uuid,
        completed_at: DateTime<Utc>,
    ) -> Result<bool> {
        let status = if error.is_some() { "rejected" } else { "completed" };
        let result = sqlx::query(
            r#"
            UPDATE workflow_updates
            SET status = $5,
                output = $6,
                error = $7,
                completed_event_id = $8,
                completed_at = $9,
                updated_at = $9
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND update_id = $4
              AND status IN ('requested', 'accepted')
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(update_id)
        .bind(status)
        .bind(output.map(Json))
        .bind(error)
        .bind(completed_event_id)
        .bind(completed_at)
        .execute(&self.pool)
        .await
        .context("failed to complete workflow update")?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn upsert_child_start_requested(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        child_id: &str,
        child_workflow_id: &str,
        child_definition_id: &str,
        parent_close_policy: &str,
        input: &Value,
        source_event_id: Uuid,
        occurred_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO workflow_children (
                tenant_id, workflow_instance_id, run_id, child_id, child_workflow_id,
                child_definition_id, child_run_id, parent_close_policy, input, status, output,
                error, source_event_id, started_event_id, terminal_event_id, started_at,
                completed_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, $8, 'start_requested', NULL, NULL, $9, NULL, NULL, NULL, NULL, $10)
            ON CONFLICT (tenant_id, workflow_instance_id, run_id, child_id)
            DO UPDATE SET
                child_workflow_id = EXCLUDED.child_workflow_id,
                child_definition_id = EXCLUDED.child_definition_id,
                parent_close_policy = EXCLUDED.parent_close_policy,
                input = EXCLUDED.input,
                source_event_id = EXCLUDED.source_event_id,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(child_id)
        .bind(child_workflow_id)
        .bind(child_definition_id)
        .bind(parent_close_policy)
        .bind(Json(input))
        .bind(source_event_id)
        .bind(occurred_at)
        .execute(&self.pool)
        .await
        .context("failed to upsert child start requested")?;

        Ok(())
    }

    pub async fn mark_child_started(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        child_id: &str,
        child_run_id: &str,
        started_event_id: Uuid,
        occurred_at: DateTime<Utc>,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_children
            SET child_run_id = $5,
                status = 'started',
                started_event_id = $6,
                started_at = $7,
                updated_at = $7
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3 AND child_id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(child_id)
        .bind(child_run_id)
        .bind(started_event_id)
        .bind(occurred_at)
        .execute(&self.pool)
        .await
        .context("failed to mark child started")?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn complete_child(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        child_id: &str,
        status: &str,
        output: Option<&Value>,
        error: Option<&str>,
        terminal_event_id: Uuid,
        occurred_at: DateTime<Utc>,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_children
            SET status = $5,
                output = $6,
                error = $7,
                terminal_event_id = $8,
                completed_at = $9,
                updated_at = $9
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3 AND child_id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(child_id)
        .bind(status)
        .bind(output.map(Json))
        .bind(error)
        .bind(terminal_event_id)
        .bind(occurred_at)
        .execute(&self.pool)
        .await
        .context("failed to complete child workflow record")?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn list_children_for_run(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
    ) -> Result<Vec<WorkflowChildRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT tenant_id, workflow_instance_id, run_id, child_id, child_workflow_id,
                   child_definition_id, child_run_id, parent_close_policy, input, status, output,
                   error, source_event_id, started_event_id, terminal_event_id, started_at,
                   completed_at, updated_at
            FROM workflow_children
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            ORDER BY updated_at ASC, child_id ASC
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list child workflows")?;

        rows.into_iter().map(Self::decode_child_row).collect()
    }

    pub async fn list_open_children_for_run(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
    ) -> Result<Vec<WorkflowChildRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT tenant_id, workflow_instance_id, run_id, child_id, child_workflow_id,
                   child_definition_id, child_run_id, parent_close_policy, input, status, output,
                   error, source_event_id, started_event_id, terminal_event_id, started_at,
                   completed_at, updated_at
            FROM workflow_children
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND status IN ('start_requested', 'started')
            ORDER BY updated_at ASC, child_id ASC
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list open child workflows")?;

        rows.into_iter().map(Self::decode_child_row).collect()
    }

    pub async fn find_parent_for_child_run(
        &self,
        tenant_id: &str,
        child_run_id: &str,
    ) -> Result<Option<WorkflowChildRecord>> {
        let row = sqlx::query(
            r#"
            SELECT tenant_id, workflow_instance_id, run_id, child_id, child_workflow_id,
                   child_definition_id, child_run_id, parent_close_policy, input, status, output,
                   error, source_event_id, started_event_id, terminal_event_id, started_at,
                   completed_at, updated_at
            FROM workflow_children
            WHERE tenant_id = $1 AND child_run_id = $2
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(child_run_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to find parent workflow for child run")?;

        row.map(Self::decode_child_row).transpose()
    }

    pub async fn put_snapshot(&self, state: &WorkflowInstanceState) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO workflow_state_snapshots (
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                definition_version,
                artifact_hash,
                snapshot_schema_version,
                event_count,
                last_event_id,
                last_event_type,
                updated_at,
                state
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (tenant_id, workflow_instance_id, run_id)
            DO UPDATE SET
                workflow_id = EXCLUDED.workflow_id,
                definition_version = EXCLUDED.definition_version,
                artifact_hash = EXCLUDED.artifact_hash,
                snapshot_schema_version = EXCLUDED.snapshot_schema_version,
                event_count = EXCLUDED.event_count,
                last_event_id = EXCLUDED.last_event_id,
                last_event_type = EXCLUDED.last_event_type,
                updated_at = EXCLUDED.updated_at,
                state = EXCLUDED.state
            "#,
        )
        .bind(&state.tenant_id)
        .bind(&state.instance_id)
        .bind(&state.run_id)
        .bind(&state.definition_id)
        .bind(state.definition_version.map(|version| version as i32))
        .bind(state.artifact_hash.as_deref())
        .bind(i32::try_from(SNAPSHOT_SCHEMA_VERSION).expect("snapshot schema version fits in i32"))
        .bind(state.event_count)
        .bind(state.last_event_id)
        .bind(state.last_event_type.as_str())
        .bind(state.updated_at)
        .bind(Json(state))
        .execute(&self.pool)
        .await
        .context("failed to persist workflow snapshot")?;

        Ok(())
    }

    pub async fn get_latest_snapshot(
        &self,
        tenant_id: &str,
        workflow_instance_id: &str,
    ) -> Result<Option<WorkflowStateSnapshot>> {
        let row = sqlx::query(
            r#"
            SELECT
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                definition_version,
                artifact_hash,
                snapshot_schema_version,
                event_count,
                last_event_id,
                last_event_type,
                updated_at,
                state
            FROM workflow_state_snapshots
            WHERE tenant_id = $1 AND workflow_instance_id = $2
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(workflow_instance_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load latest workflow snapshot")?;

        row.map(Self::decode_snapshot_row).transpose()
    }

    pub async fn get_snapshot_for_run(
        &self,
        tenant_id: &str,
        workflow_instance_id: &str,
        run_id: &str,
    ) -> Result<Option<WorkflowStateSnapshot>> {
        let row = sqlx::query(
            r#"
            SELECT
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                definition_version,
                artifact_hash,
                snapshot_schema_version,
                event_count,
                last_event_id,
                last_event_type,
                updated_at,
                state
            FROM workflow_state_snapshots
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(workflow_instance_id)
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load workflow snapshot for run")?;

        row.map(Self::decode_snapshot_row).transpose()
    }

    pub async fn mark_event_processed(&self, event: &EventEnvelope<WorkflowEvent>) -> Result<bool> {
        let result = sqlx::query(
            r#"
            INSERT INTO processed_workflow_events (
                event_id,
                tenant_id,
                workflow_instance_id,
                dedupe_key,
                event_type
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(event.event_id)
        .bind(&event.tenant_id)
        .bind(&event.instance_id)
        .bind(event.dedupe_key.as_deref())
        .bind(&event.event_type)
        .execute(&self.pool)
        .await
        .context("failed to mark workflow event as processed")?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn mark_events_processed_batch(
        &self,
        events: &[&EventEnvelope<WorkflowEvent>],
    ) -> Result<Vec<bool>> {
        const BATCH_SIZE: usize = 250;

        if events.is_empty() {
            return Ok(Vec::new());
        }

        let mut inserted = vec![false; events.len()];
        for (offset, batch) in events.chunks(BATCH_SIZE).enumerate() {
            let base_index = offset * BATCH_SIZE;
            let mut builder = QueryBuilder::<Postgres>::new(
                r#"
                WITH input (
                    batch_index,
                    event_id,
                    tenant_id,
                    workflow_instance_id,
                    dedupe_key,
                    event_type
                ) AS (
                "#,
            );
            builder.push_values(batch.iter().enumerate(), |mut row, (index, event)| {
                row.push_bind(i64::try_from(base_index + index).expect("batch index exceeds i64"))
                    .push_bind(event.event_id)
                    .push_bind(&event.tenant_id)
                    .push_bind(&event.instance_id)
                    .push_bind(event.dedupe_key.as_deref())
                    .push_bind(&event.event_type);
            });
            builder.push(
                r#"
                ),
                inserted AS (
                    INSERT INTO processed_workflow_events (
                        event_id,
                        tenant_id,
                        workflow_instance_id,
                        dedupe_key,
                        event_type
                    )
                    SELECT
                        event_id,
                        tenant_id,
                        workflow_instance_id,
                        dedupe_key,
                        event_type
                    FROM input
                    ON CONFLICT DO NOTHING
                    RETURNING event_id
                )
                SELECT input.batch_index
                FROM input
                INNER JOIN inserted USING (event_id)
                "#,
            );

            let rows = builder
                .build()
                .fetch_all(&self.pool)
                .await
                .context("failed to mark workflow events as processed in batch")?;
            for row in rows {
                let index = usize::try_from(row.get::<i64, _>("batch_index"))
                    .expect("batch index is negative");
                inserted[index] = true;
            }
        }

        Ok(inserted)
    }

    pub async fn unmark_events_processed_batch(
        &self,
        events: &[&EventEnvelope<WorkflowEvent>],
    ) -> Result<()> {
        const BATCH_SIZE: usize = 250;

        if events.is_empty() {
            return Ok(());
        }

        for batch in events.chunks(BATCH_SIZE) {
            let mut builder = QueryBuilder::<Postgres>::new(
                r#"
                WITH input (event_id) AS (
                "#,
            );
            builder.push_values(batch, |mut row, event| {
                row.push_bind(event.event_id);
            });
            builder.push(
                r#"
                )
                DELETE FROM processed_workflow_events
                WHERE event_id IN (SELECT event_id FROM input)
                "#,
            );
            builder
                .build()
                .execute(&self.pool)
                .await
                .context("failed to rollback processed workflow events")?;
        }

        Ok(())
    }

    pub async fn upsert_timer(
        &self,
        partition_id: i32,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        definition_id: &str,
        definition_version: Option<u32>,
        artifact_hash: Option<&str>,
        timer_id: &str,
        state: Option<&str>,
        fire_at: DateTime<Utc>,
        scheduled_event_id: Uuid,
        correlation_id: Option<Uuid>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO workflow_timers (
                partition_id,
                tenant_id,
                workflow_instance_id,
                workflow_id,
                run_id,
                workflow_version,
                artifact_hash,
                timer_id,
                state,
                fire_at,
                scheduled_event_id,
                correlation_id,
                dispatched_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NULL)
            ON CONFLICT (tenant_id, workflow_instance_id, timer_id)
            DO UPDATE SET
                partition_id = EXCLUDED.partition_id,
                workflow_id = EXCLUDED.workflow_id,
                run_id = EXCLUDED.run_id,
                workflow_version = EXCLUDED.workflow_version,
                artifact_hash = EXCLUDED.artifact_hash,
                state = EXCLUDED.state,
                fire_at = EXCLUDED.fire_at,
                scheduled_event_id = EXCLUDED.scheduled_event_id,
                correlation_id = EXCLUDED.correlation_id,
                dispatched_at = NULL
            "#,
        )
        .bind(partition_id)
        .bind(tenant_id)
        .bind(instance_id)
        .bind(definition_id)
        .bind(run_id)
        .bind(definition_version.map(|version| version as i32))
        .bind(artifact_hash)
        .bind(timer_id)
        .bind(state)
        .bind(fire_at)
        .bind(scheduled_event_id)
        .bind(correlation_id)
        .execute(&self.pool)
        .await
        .context("failed to upsert workflow timer")?;

        Ok(())
    }

    pub async fn claim_due_timers(
        &self,
        partition_id: i32,
        now: DateTime<Utc>,
        limit: i64,
        owner_epoch: u64,
    ) -> Result<Vec<ScheduledTimer>> {
        let rows = sqlx::query(
            r#"
            SELECT
                partition_id,
                tenant_id,
                workflow_instance_id,
                workflow_id,
                run_id,
                workflow_version,
                artifact_hash,
                timer_id,
                state,
                fire_at,
                scheduled_event_id,
                correlation_id
            FROM workflow_timers
            WHERE partition_id = $1
              AND fire_at <= $2
              AND (dispatched_at IS NULL OR dispatch_owner_epoch IS DISTINCT FROM $4)
            ORDER BY fire_at ASC
            LIMIT $3
            "#,
        )
        .bind(partition_id)
        .bind(now)
        .bind(limit)
        .bind(i64::try_from(owner_epoch).context("ownership epoch exceeds i64")?)
        .fetch_all(&self.pool)
        .await
        .context("failed to load due workflow timers")?;

        let mut timers = Vec::with_capacity(rows.len());
        for row in rows {
            timers.push(ScheduledTimer {
                partition_id: row.try_get("partition_id").context("timer partition_id missing")?,
                tenant_id: row.try_get("tenant_id").context("timer tenant_id missing")?,
                instance_id: row
                    .try_get("workflow_instance_id")
                    .context("timer workflow_instance_id missing")?,
                run_id: row.try_get("run_id").context("timer run_id missing")?,
                definition_id: row.try_get("workflow_id").context("timer workflow_id missing")?,
                definition_version: row
                    .try_get::<Option<i32>, _>("workflow_version")
                    .context("timer workflow_version missing")?
                    .map(|version| version as u32),
                artifact_hash: row
                    .try_get("artifact_hash")
                    .context("timer artifact_hash missing")?,
                timer_id: row.try_get("timer_id").context("timer timer_id missing")?,
                state: row.try_get("state").context("timer state missing")?,
                fire_at: row.try_get("fire_at").context("timer fire_at missing")?,
                scheduled_event_id: row
                    .try_get("scheduled_event_id")
                    .context("timer scheduled_event_id missing")?,
                correlation_id: row
                    .try_get("correlation_id")
                    .context("timer correlation_id missing")?,
            });
        }

        Ok(timers)
    }

    pub async fn mark_timer_dispatched(
        &self,
        tenant_id: &str,
        workflow_instance_id: &str,
        timer_id: &str,
        owner_epoch: u64,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_timers
            SET dispatched_at = NOW(),
                dispatch_owner_epoch = $4
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND timer_id = $3
              AND (dispatched_at IS NULL OR dispatch_owner_epoch IS DISTINCT FROM $4)
            "#,
        )
        .bind(tenant_id)
        .bind(workflow_instance_id)
        .bind(timer_id)
        .bind(i64::try_from(owner_epoch).context("ownership epoch exceeds i64")?)
        .execute(&self.pool)
        .await
        .context("failed to mark timer dispatched")?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn reset_timer_dispatch(
        &self,
        tenant_id: &str,
        workflow_instance_id: &str,
        timer_id: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE workflow_timers
            SET dispatched_at = NULL,
                dispatch_owner_epoch = NULL
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND timer_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(workflow_instance_id)
        .bind(timer_id)
        .execute(&self.pool)
        .await
        .context("failed to reset timer dispatch")?;

        Ok(())
    }

    pub async fn delete_timer(
        &self,
        tenant_id: &str,
        workflow_instance_id: &str,
        timer_id: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            DELETE FROM workflow_timers
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND timer_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(workflow_instance_id)
        .bind(timer_id)
        .execute(&self.pool)
        .await
        .context("failed to delete workflow timer")?;

        Ok(())
    }

    pub async fn delete_timers_for_run(
        &self,
        tenant_id: &str,
        workflow_instance_id: &str,
        run_id: &str,
    ) -> Result<u64> {
        let result = sqlx::query(
            r#"
            DELETE FROM workflow_timers
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(workflow_instance_id)
        .bind(run_id)
        .execute(&self.pool)
        .await
        .context("failed to delete workflow timers for run")?;

        Ok(result.rows_affected())
    }

    pub async fn prune_query_retention(
        &self,
        cutoffs: &QueryRetentionCutoffs,
    ) -> Result<QueryRetentionPruneResult> {
        let pruned_activities = if let Some(cutoff) = cutoffs.activity_run_closed_before {
            sqlx::query(
                r#"
                DELETE FROM workflow_activities activities
                USING workflow_runs runs
                WHERE activities.tenant_id = runs.tenant_id
                  AND activities.workflow_instance_id = runs.workflow_instance_id
                  AND activities.run_id = runs.run_id
                  AND runs.closed_at IS NOT NULL
                  AND runs.closed_at < $1
                "#,
            )
            .bind(cutoff)
            .execute(&self.pool)
            .await
            .context("failed to prune workflow activities by retention policy")?
            .rows_affected()
        } else {
            0
        };

        let pruned_signals = if let Some(cutoff) = cutoffs.signal_run_closed_before {
            sqlx::query(
                r#"
                DELETE FROM workflow_signals signals
                USING workflow_runs runs
                WHERE signals.tenant_id = runs.tenant_id
                  AND signals.workflow_instance_id = runs.workflow_instance_id
                  AND signals.run_id = runs.run_id
                  AND runs.closed_at IS NOT NULL
                  AND runs.closed_at < $1
                "#,
            )
            .bind(cutoff)
            .execute(&self.pool)
            .await
            .context("failed to prune workflow signals by retention policy")?
            .rows_affected()
        } else {
            0
        };

        let pruned_snapshots = if let Some(cutoff) = cutoffs.snapshot_run_closed_before {
            sqlx::query(
                r#"
                DELETE FROM workflow_state_snapshots snapshots
                USING workflow_runs runs
                WHERE snapshots.tenant_id = runs.tenant_id
                  AND snapshots.workflow_instance_id = runs.workflow_instance_id
                  AND snapshots.run_id = runs.run_id
                  AND runs.closed_at IS NOT NULL
                  AND runs.closed_at < $1
                "#,
            )
            .bind(cutoff)
            .execute(&self.pool)
            .await
            .context("failed to prune workflow snapshots by retention policy")?
            .rows_affected()
        } else {
            0
        };

        let pruned_runs = if let Some(cutoff) = cutoffs.run_closed_before {
            sqlx::query(
                r#"
                DELETE FROM workflow_runs
                WHERE closed_at IS NOT NULL
                  AND closed_at < $1
                "#,
            )
            .bind(cutoff)
            .execute(&self.pool)
            .await
            .context("failed to prune workflow runs by retention policy")?
            .rows_affected()
        } else {
            0
        };

        Ok(QueryRetentionPruneResult {
            pruned_runs,
            pruned_activities,
            pruned_signals,
            pruned_snapshots,
        })
    }

    pub async fn upsert_activity_scheduled(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        definition_id: &str,
        definition_version: Option<u32>,
        artifact_hash: Option<&str>,
        activity_id: &str,
        attempt: u32,
        activity_type: &str,
        task_queue: &str,
        state: Option<&str>,
        input: &Value,
        config: Option<&Value>,
        schedule_to_start_timeout_ms: Option<u64>,
        start_to_close_timeout_ms: Option<u64>,
        heartbeat_timeout_ms: Option<u64>,
        event_id: Uuid,
        event_type: &str,
        occurred_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO workflow_activities (
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                workflow_version,
                artifact_hash,
                activity_id,
                attempt,
                activity_type,
                task_queue,
                state,
                status,
                input,
                config,
                output,
                error,
                cancellation_requested,
                cancellation_reason,
                cancellation_metadata,
                worker_id,
                worker_build_id,
                scheduled_at,
                started_at,
                last_heartbeat_at,
                lease_expires_at,
                completed_at,
                schedule_to_start_timeout_ms,
                start_to_close_timeout_ms,
                heartbeat_timeout_ms,
                last_event_id,
                last_event_type,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'scheduled', $12, $13,
                NULL, NULL, FALSE, NULL, NULL, NULL, NULL, $14, NULL, NULL, NULL, NULL,
                $15, $16, $17, $18, $19, $20
            )
            ON CONFLICT (tenant_id, workflow_instance_id, run_id, activity_id, attempt)
            DO UPDATE SET
                workflow_id = EXCLUDED.workflow_id,
                workflow_version = EXCLUDED.workflow_version,
                artifact_hash = EXCLUDED.artifact_hash,
                activity_type = EXCLUDED.activity_type,
                task_queue = EXCLUDED.task_queue,
                state = EXCLUDED.state,
                status = EXCLUDED.status,
                input = EXCLUDED.input,
                config = EXCLUDED.config,
                output = NULL,
                error = NULL,
                cancellation_requested = FALSE,
                cancellation_reason = NULL,
                cancellation_metadata = NULL,
                worker_id = NULL,
                worker_build_id = NULL,
                scheduled_at = EXCLUDED.scheduled_at,
                started_at = NULL,
                last_heartbeat_at = NULL,
                lease_expires_at = NULL,
                completed_at = NULL,
                schedule_to_start_timeout_ms = EXCLUDED.schedule_to_start_timeout_ms,
                start_to_close_timeout_ms = EXCLUDED.start_to_close_timeout_ms,
                heartbeat_timeout_ms = EXCLUDED.heartbeat_timeout_ms,
                last_event_id = EXCLUDED.last_event_id,
                last_event_type = EXCLUDED.last_event_type,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(definition_id)
        .bind(definition_version.map(|version| version as i32))
        .bind(artifact_hash)
        .bind(activity_id)
        .bind(i32::try_from(attempt).context("activity attempt exceeds i32")?)
        .bind(activity_type)
        .bind(task_queue)
        .bind(state)
        .bind(Json(input))
        .bind(config.map(Json))
        .bind(occurred_at)
        .bind(schedule_to_start_timeout_ms.map(|value| value as i64))
        .bind(start_to_close_timeout_ms.map(|value| value as i64))
        .bind(heartbeat_timeout_ms.map(|value| value as i64))
        .bind(event_id)
        .bind(event_type)
        .bind(occurred_at)
        .execute(&self.pool)
        .await
        .context("failed to upsert scheduled workflow activity")?;

        Ok(())
    }

    pub async fn upsert_activities_scheduled_batch(
        &self,
        updates: &[ActivityScheduleUpdate],
    ) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }

        let mut query = QueryBuilder::<Postgres>::new(
            r#"
            WITH updates (
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                workflow_version,
                artifact_hash,
                activity_id,
                attempt,
                activity_type,
                task_queue,
                state,
                input,
                config,
                scheduled_at,
                schedule_to_start_timeout_ms,
                start_to_close_timeout_ms,
                heartbeat_timeout_ms,
                last_event_id,
                last_event_type,
                updated_at
            ) AS (
            "#,
        );

        query.push_values(updates, |mut builder, update| {
            builder
                .push_bind(&update.tenant_id)
                .push_bind(&update.instance_id)
                .push_bind(&update.run_id)
                .push_bind(&update.definition_id)
                .push_bind(update.definition_version.map(|value| value as i32))
                .push_bind(update.artifact_hash.as_deref())
                .push_bind(&update.activity_id)
                .push_bind(i32::try_from(update.attempt).expect("activity attempt exceeds i32"))
                .push_bind(&update.activity_type)
                .push_bind(&update.task_queue)
                .push_bind(update.state.as_deref())
                .push_bind(Json(&update.input))
                .push_bind(update.config.as_ref().map(Json))
                .push_bind(update.occurred_at)
                .push_bind(update.schedule_to_start_timeout_ms.map(|value| value as i64))
                .push_bind(update.start_to_close_timeout_ms.map(|value| value as i64))
                .push_bind(update.heartbeat_timeout_ms.map(|value| value as i64))
                .push_bind(update.event_id)
                .push_bind(&update.event_type)
                .push_bind(update.occurred_at);
        });

        query.push(
            r#"
            )
            INSERT INTO workflow_activities (
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                workflow_version,
                artifact_hash,
                activity_id,
                attempt,
                activity_type,
                task_queue,
                state,
                status,
                input,
                config,
                output,
                error,
                cancellation_requested,
                cancellation_reason,
                cancellation_metadata,
                worker_id,
                worker_build_id,
                scheduled_at,
                started_at,
                last_heartbeat_at,
                lease_expires_at,
                completed_at,
                schedule_to_start_timeout_ms,
                start_to_close_timeout_ms,
                heartbeat_timeout_ms,
                last_event_id,
                last_event_type,
                updated_at
            )
            SELECT
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                workflow_version,
                artifact_hash,
                activity_id,
                attempt,
                activity_type,
                task_queue,
                state,
                'scheduled',
                input,
                config,
                NULL,
                NULL,
                FALSE,
                NULL,
                NULL,
                NULL,
                NULL,
                scheduled_at,
                NULL,
                NULL,
                NULL,
                NULL,
                schedule_to_start_timeout_ms,
                start_to_close_timeout_ms,
                heartbeat_timeout_ms,
                last_event_id,
                last_event_type,
                updated_at
            FROM updates
            ON CONFLICT (tenant_id, workflow_instance_id, run_id, activity_id, attempt)
            DO UPDATE SET
                workflow_id = EXCLUDED.workflow_id,
                workflow_version = EXCLUDED.workflow_version,
                artifact_hash = EXCLUDED.artifact_hash,
                activity_type = EXCLUDED.activity_type,
                task_queue = EXCLUDED.task_queue,
                state = EXCLUDED.state,
                status = EXCLUDED.status,
                input = EXCLUDED.input,
                config = EXCLUDED.config,
                output = NULL,
                error = NULL,
                cancellation_requested = FALSE,
                cancellation_reason = NULL,
                cancellation_metadata = NULL,
                worker_id = NULL,
                worker_build_id = NULL,
                scheduled_at = EXCLUDED.scheduled_at,
                started_at = NULL,
                last_heartbeat_at = NULL,
                lease_expires_at = NULL,
                completed_at = NULL,
                schedule_to_start_timeout_ms = EXCLUDED.schedule_to_start_timeout_ms,
                start_to_close_timeout_ms = EXCLUDED.start_to_close_timeout_ms,
                heartbeat_timeout_ms = EXCLUDED.heartbeat_timeout_ms,
                last_event_id = EXCLUDED.last_event_id,
                last_event_type = EXCLUDED.last_event_type,
                updated_at = EXCLUDED.updated_at
            "#,
        );

        query
            .build()
            .execute(&self.pool)
            .await
            .context("failed to batch upsert scheduled workflow activities")?;

        Ok(())
    }

    pub async fn request_activity_cancellation(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        activity_id: &str,
        attempt: u32,
        reason: &str,
        metadata: Option<&Value>,
        event_id: Uuid,
        event_type: &str,
        occurred_at: DateTime<Utc>,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_activities
            SET cancellation_requested = TRUE,
                cancellation_reason = $6,
                cancellation_metadata = $7,
                last_event_id = $8,
                last_event_type = $9,
                updated_at = $10
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND activity_id = $4
              AND attempt = $5
              AND status IN ('scheduled', 'started')
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(activity_id)
        .bind(i32::try_from(attempt).context("activity attempt exceeds i32")?)
        .bind(reason)
        .bind(metadata.map(Json))
        .bind(event_id)
        .bind(event_type)
        .bind(occurred_at)
        .execute(&self.pool)
        .await
        .context("failed to request workflow activity cancellation")?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn mark_activity_started(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        activity_id: &str,
        attempt: u32,
        worker_id: &str,
        worker_build_id: &str,
        lease_expires_at: DateTime<Utc>,
        event_id: Uuid,
        event_type: &str,
        occurred_at: DateTime<Utc>,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_activities
            SET status = 'started',
                worker_id = $6,
                worker_build_id = $7,
                started_at = COALESCE(started_at, $8),
                last_heartbeat_at = $8,
                lease_expires_at = $9,
                last_event_id = $10,
                last_event_type = $11,
                updated_at = $12
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND activity_id = $4
              AND attempt = $5
              AND status = 'scheduled'
              AND cancellation_requested = FALSE
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(activity_id)
        .bind(i32::try_from(attempt).context("activity attempt exceeds i32")?)
        .bind(worker_id)
        .bind(worker_build_id)
        .bind(occurred_at)
        .bind(lease_expires_at)
        .bind(event_id)
        .bind(event_type)
        .bind(occurred_at)
        .execute(&self.pool)
        .await
        .context("failed to mark workflow activity started")?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn mark_activities_started_batch(
        &self,
        updates: &[ActivityStartUpdate],
    ) -> Result<Vec<AppliedActivityStartUpdate>> {
        if updates.is_empty() {
            return Ok(Vec::new());
        }

        let mut query = QueryBuilder::<Postgres>::new(
            r#"
            WITH updates (
                batch_index,
                tenant_id,
                workflow_instance_id,
                run_id,
                activity_id,
                attempt,
                worker_id,
                worker_build_id,
                occurred_at,
                lease_expires_at,
                event_id,
                event_type
            ) AS (
            "#,
        );

        query.push_values(updates.iter().enumerate(), |mut builder, (batch_index, update)| {
            builder
                .push_bind(batch_index as i64)
                .push_bind(&update.tenant_id)
                .push_bind(&update.instance_id)
                .push_bind(&update.run_id)
                .push_bind(&update.activity_id)
                .push_bind(i32::try_from(update.attempt).expect("activity attempt exceeds i32"))
                .push_bind(&update.worker_id)
                .push_bind(&update.worker_build_id)
                .push_bind(update.occurred_at)
                .push_bind(update.lease_expires_at)
                .push_bind(update.event_id)
                .push_bind(&update.event_type);
        });

        query.push(
            r#"
            ),
            applied AS (
                UPDATE workflow_activities AS activity
                SET status = 'started',
                    worker_id = updates.worker_id,
                    worker_build_id = updates.worker_build_id,
                    started_at = COALESCE(activity.started_at, updates.occurred_at),
                    last_heartbeat_at = updates.occurred_at,
                    lease_expires_at = updates.lease_expires_at,
                    last_event_id = updates.event_id,
                    last_event_type = updates.event_type,
                    updated_at = updates.occurred_at
                FROM updates
                WHERE activity.tenant_id = updates.tenant_id
                  AND activity.workflow_instance_id = updates.workflow_instance_id
                  AND activity.run_id = updates.run_id
                  AND activity.activity_id = updates.activity_id
                  AND activity.attempt = updates.attempt
                  AND activity.status = 'scheduled'
                  AND activity.cancellation_requested = FALSE
                RETURNING
                    updates.batch_index,
                    activity.tenant_id,
                    activity.workflow_instance_id,
                    activity.run_id,
                    activity.workflow_id,
                    activity.workflow_version,
                    activity.artifact_hash,
                    activity.activity_id,
                    activity.attempt,
                    activity.activity_type,
                    activity.task_queue,
                    activity.state,
                    activity.status,
                    activity.input,
                    activity.config,
                    activity.output,
                    activity.error,
                    activity.cancellation_requested,
                    activity.cancellation_reason,
                    activity.cancellation_metadata,
                    activity.worker_id,
                    activity.worker_build_id,
                    activity.scheduled_at,
                    activity.started_at,
                    activity.last_heartbeat_at,
                    activity.lease_expires_at,
                    activity.completed_at,
                    activity.schedule_to_start_timeout_ms,
                    activity.start_to_close_timeout_ms,
                    activity.heartbeat_timeout_ms,
                    activity.last_event_id,
                    activity.last_event_type,
                    activity.updated_at
            )
            SELECT *
            FROM applied
            ORDER BY batch_index
            "#,
        );

        let rows = query
            .build()
            .fetch_all(&self.pool)
            .await
            .context("failed to batch mark workflow activities started")?;

        let mut applied = Vec::with_capacity(rows.len());
        for row in rows {
            let batch_index = row
                .try_get::<i64, _>("batch_index")
                .context("activity batch_index missing")? as usize;
            let update = &updates[batch_index];
            applied.push(AppliedActivityStartUpdate {
                record: Self::decode_activity_row(row)?,
                event_id: update.event_id,
                occurred_at: update.occurred_at,
            });
        }

        Ok(applied)
    }

    pub async fn requeue_activity(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        activity_id: &str,
        attempt: u32,
        occurred_at: DateTime<Utc>,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_activities
            SET status = 'scheduled',
                worker_id = NULL,
                worker_build_id = NULL,
                lease_expires_at = NULL,
                updated_at = $6
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND activity_id = $4
              AND attempt = $5
              AND status = 'started'
              AND completed_at IS NULL
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(activity_id)
        .bind(i32::try_from(attempt).context("activity attempt exceeds i32")?)
        .bind(occurred_at)
        .execute(&self.pool)
        .await
        .context("failed to requeue workflow activity")?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn record_activity_heartbeat(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        activity_id: &str,
        attempt: u32,
        worker_id: &str,
        worker_build_id: &str,
        lease_expires_at: DateTime<Utc>,
        event_id: Uuid,
        event_type: &str,
        occurred_at: DateTime<Utc>,
    ) -> Result<ActivityHeartbeatResult> {
        let row = sqlx::query(
            r#"
            UPDATE workflow_activities
            SET last_heartbeat_at = $8,
                lease_expires_at = $9,
                last_event_id = $10,
                last_event_type = $11,
                updated_at = $12
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND activity_id = $4
              AND attempt = $5
              AND worker_id = $6
              AND worker_build_id = $7
              AND status = 'started'
            RETURNING cancellation_requested
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(activity_id)
        .bind(i32::try_from(attempt).context("activity attempt exceeds i32")?)
        .bind(worker_id)
        .bind(worker_build_id)
        .bind(occurred_at)
        .bind(lease_expires_at)
        .bind(event_id)
        .bind(event_type)
        .bind(occurred_at)
        .fetch_optional(&self.pool)
        .await
        .context("failed to record workflow activity heartbeat")?;

        Ok(match row {
            Some(row) => ActivityHeartbeatResult {
                updated: true,
                cancellation_requested: row
                    .try_get("cancellation_requested")
                    .context("activity cancellation_requested missing")?,
            },
            None => ActivityHeartbeatResult { updated: false, cancellation_requested: false },
        })
    }

    pub async fn complete_activity(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        activity_id: &str,
        attempt: u32,
        output: &Value,
        worker_id: &str,
        worker_build_id: &str,
        event_id: Uuid,
        event_type: &str,
        occurred_at: DateTime<Utc>,
    ) -> Result<bool> {
        self.update_activity_terminal(
            tenant_id,
            instance_id,
            run_id,
            activity_id,
            attempt,
            WorkflowActivityStatus::Completed,
            Some(output),
            None,
            None,
            None,
            worker_id,
            worker_build_id,
            event_id,
            event_type,
            occurred_at,
        )
        .await
    }

    pub async fn apply_activity_terminal_batch(
        &self,
        updates: &[ActivityTerminalUpdate],
    ) -> Result<Vec<AppliedActivityTerminalUpdate>> {
        if updates.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = QueryBuilder::<Postgres>::new(
            r#"
            WITH updates (
                batch_index,
                tenant_id,
                workflow_instance_id,
                run_id,
                activity_id,
                attempt,
                status,
                output,
                error,
                cancellation_reason,
                cancellation_metadata,
                worker_id,
                worker_build_id,
                event_id,
                event_type,
                occurred_at
            ) AS (
            "#,
        );

        query.push_values(updates.iter().enumerate(), |mut builder, (batch_index, update)| {
            let (status, output, error, cancellation_reason, cancellation_metadata) = match &update
                .payload
            {
                ActivityTerminalPayload::Completed { output } => (
                    WorkflowActivityStatus::Completed.as_str(),
                    Some(output.clone()),
                    None,
                    None,
                    None,
                ),
                ActivityTerminalPayload::Failed { error } => {
                    (WorkflowActivityStatus::Failed.as_str(), None, Some(error.clone()), None, None)
                }
                ActivityTerminalPayload::Cancelled { reason, metadata } => (
                    WorkflowActivityStatus::Cancelled.as_str(),
                    None,
                    Some(reason.clone()),
                    Some(reason.clone()),
                    metadata.clone(),
                ),
            };
            builder
                .push_bind(batch_index as i64)
                .push_bind(&update.tenant_id)
                .push_bind(&update.instance_id)
                .push_bind(&update.run_id)
                .push_bind(&update.activity_id)
                .push_bind(update.attempt as i32)
                .push_bind(status)
                .push_bind(output.map(Json))
                .push_bind(error)
                .push_bind(cancellation_reason)
                .push_bind(cancellation_metadata.map(Json))
                .push_bind(&update.worker_id)
                .push_bind(&update.worker_build_id)
                .push_bind(update.event_id)
                .push_bind(&update.event_type)
                .push_bind(update.occurred_at);
        });

        query.push(
            r#"
            ),
            applied AS (
                UPDATE workflow_activities AS activity
                SET status = updates.status,
                    output = updates.output,
                    error = updates.error,
                    cancellation_reason = updates.cancellation_reason,
                    cancellation_metadata = updates.cancellation_metadata,
                    worker_id = updates.worker_id,
                    worker_build_id = updates.worker_build_id,
                    completed_at = updates.occurred_at,
                    lease_expires_at = NULL,
                    last_event_id = updates.event_id,
                    last_event_type = updates.event_type,
                    updated_at = updates.occurred_at
                FROM updates
                WHERE activity.tenant_id = updates.tenant_id
                  AND activity.workflow_instance_id = updates.workflow_instance_id
                  AND activity.run_id = updates.run_id
                  AND activity.activity_id = updates.activity_id
                  AND activity.attempt = updates.attempt
                  AND activity.status IN ('scheduled', 'started')
                RETURNING
                    updates.batch_index,
                    activity.tenant_id,
                    activity.workflow_instance_id,
                    activity.run_id,
                    activity.workflow_id,
                    activity.workflow_version,
                    activity.artifact_hash,
                    activity.activity_id,
                    activity.attempt,
                    activity.activity_type,
                    activity.task_queue,
                    activity.state,
                    activity.status,
                    activity.input,
                    activity.config,
                    activity.output,
                    activity.error,
                    activity.cancellation_requested,
                    activity.cancellation_reason,
                    activity.cancellation_metadata,
                    activity.worker_id,
                    activity.worker_build_id,
                    activity.scheduled_at,
                    activity.started_at,
                    activity.last_heartbeat_at,
                    activity.lease_expires_at,
                    activity.completed_at,
                    activity.schedule_to_start_timeout_ms,
                    activity.start_to_close_timeout_ms,
                    activity.heartbeat_timeout_ms,
                    activity.last_event_id,
                    activity.last_event_type,
                    activity.updated_at
            )
            SELECT *
            FROM applied
            ORDER BY batch_index
            "#,
        );

        let rows = query
            .build()
            .fetch_all(&self.pool)
            .await
            .context("failed to update workflow activity terminal status in batch")?;

        let mut applied = Vec::with_capacity(rows.len());
        for row in rows {
            let batch_index = row
                .try_get::<i64, _>("batch_index")
                .context("activity batch_index missing")? as usize;
            let update = &updates[batch_index];
            applied.push(AppliedActivityTerminalUpdate {
                record: Self::decode_activity_row(row)?,
                event_id: update.event_id,
                occurred_at: update.occurred_at,
                payload: update.payload.clone(),
            });
        }

        Ok(applied)
    }

    pub async fn upsert_bulk_batch(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        definition_id: &str,
        definition_version: Option<u32>,
        artifact_hash: Option<&str>,
        batch_id: &str,
        activity_type: &str,
        task_queue: &str,
        state: Option<&str>,
        input_handle: &Value,
        result_handle: &Value,
        items: &[Value],
        chunk_size: u32,
        max_attempts: u32,
        retry_delay_ms: u64,
        execution_policy: Option<&str>,
        reducer: Option<&str>,
        reducer_class: &str,
        aggregation_tree_depth: u32,
        fast_lane_enabled: bool,
        aggregation_group_count: u32,
        throughput_backend: &str,
        throughput_backend_version: &str,
        scheduled_at: DateTime<Utc>,
    ) -> Result<(WorkflowBulkBatchRecord, Vec<WorkflowBulkChunkRecord>)> {
        if items.len() > MAX_BULK_ITEMS_PER_BATCH {
            anyhow::bail!(
                "bulk batch item count {} exceeds limit {}",
                items.len(),
                MAX_BULK_ITEMS_PER_BATCH
            );
        }
        let chunk_size_usize = usize::try_from(chunk_size.max(1)).unwrap_or(1);
        if chunk_size_usize > MAX_BULK_CHUNK_SIZE {
            anyhow::bail!(
                "bulk chunk size {} exceeds limit {}",
                chunk_size_usize,
                MAX_BULK_CHUNK_SIZE
            );
        }
        let mut total_input_bytes = 0usize;
        for (chunk_index, chunk_items) in items.chunks(chunk_size_usize).enumerate() {
            let mut chunk_bytes = 0usize;
            for item in chunk_items {
                let item_bytes = serde_json::to_vec(item)
                    .map(|bytes| bytes.len())
                    .context("failed to serialize bulk batch item")?;
                if item_bytes > MAX_BULK_ITEM_BYTES {
                    anyhow::bail!(
                        "bulk batch item exceeds per-item byte limit {}",
                        MAX_BULK_ITEM_BYTES
                    );
                }
                total_input_bytes = total_input_bytes.saturating_add(item_bytes);
                chunk_bytes = chunk_bytes.saturating_add(item_bytes);
            }
            if chunk_bytes > MAX_BULK_CHUNK_INPUT_BYTES {
                anyhow::bail!(
                    "bulk chunk {} serialized to {} bytes, over limit {}",
                    chunk_index,
                    chunk_bytes,
                    MAX_BULK_CHUNK_INPUT_BYTES
                );
            }
        }
        if total_input_bytes > MAX_BULK_TOTAL_INPUT_BYTES {
            anyhow::bail!(
                "bulk batch input serialized to {} bytes, over limit {}",
                total_input_bytes,
                MAX_BULK_TOTAL_INPUT_BYTES
            );
        }

        let mut tx = self.pool.begin().await.context("failed to begin bulk batch transaction")?;
        let total_items =
            i32::try_from(items.len()).context("bulk batch item count exceeds i32")?;
        let chunk_size_i32 =
            i32::try_from(chunk_size).context("bulk batch chunk_size exceeds i32")?;
        let chunk_count = if items.is_empty() {
            0
        } else {
            ((items.len() + chunk_size_usize - 1) / chunk_size_usize) as i32
        };
        sqlx::query(
            r#"
            INSERT INTO workflow_bulk_batches (
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                workflow_version,
                artifact_hash,
                batch_id,
                activity_type,
                task_queue,
                state,
                input_handle,
                result_handle,
                throughput_backend,
                throughput_backend_version,
                execution_policy,
                reducer,
                reducer_class,
                aggregation_tree_depth,
                fast_lane_enabled,
                aggregation_group_count,
                status,
                total_items,
                chunk_size,
                chunk_count,
                max_attempts,
                retry_delay_ms,
                scheduled_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                $18, $19, $20, 'scheduled', $21, $22, $23, $24, $25, $26, $26
            )
            ON CONFLICT (tenant_id, workflow_instance_id, run_id, batch_id) DO NOTHING
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(definition_id)
        .bind(definition_version.map(|value| value as i32))
        .bind(artifact_hash)
        .bind(batch_id)
        .bind(activity_type)
        .bind(task_queue)
        .bind(state)
        .bind(Json(input_handle))
        .bind(Json(result_handle))
        .bind(throughput_backend)
        .bind(throughput_backend_version)
        .bind(execution_policy)
        .bind(reducer)
        .bind(reducer_class)
        .bind(
            i32::try_from(aggregation_tree_depth)
                .context("bulk batch aggregation_tree_depth exceeds i32")?,
        )
        .bind(fast_lane_enabled)
        .bind(
            i32::try_from(aggregation_group_count)
                .context("bulk batch aggregation_group_count exceeds i32")?,
        )
        .bind(total_items)
        .bind(chunk_size_i32)
        .bind(chunk_count)
        .bind(i32::try_from(max_attempts).context("bulk batch max_attempts exceeds i32")?)
        .bind(i64::try_from(retry_delay_ms).context("bulk batch retry_delay_ms exceeds i64")?)
        .bind(scheduled_at)
        .execute(tx.as_mut())
        .await
        .context("failed to upsert workflow bulk batch")?;

        for (chunk_index, chunk_items) in items.chunks(chunk_size_usize).enumerate() {
            let chunk_id = format!("{batch_id}::{chunk_index}");
            sqlx::query(
                r#"
                INSERT INTO workflow_bulk_chunks (
                    tenant_id,
                    workflow_instance_id,
                    run_id,
                    workflow_id,
                    workflow_version,
                    artifact_hash,
                    batch_id,
                    chunk_id,
                    chunk_index,
                    group_id,
                    item_count,
                    activity_type,
                    task_queue,
                    state,
                    status,
                    attempt,
                    owner_epoch,
                    max_attempts,
                    retry_delay_ms,
                    input_handle,
                    result_handle,
                    items,
                    scheduled_at,
                    available_at,
                    updated_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 'scheduled', 1, $15,
                    $16, $17, $18, $19, $20, $21, $21, $21
                )
                ON CONFLICT (tenant_id, workflow_instance_id, run_id, batch_id, chunk_id)
                DO NOTHING
                "#,
            )
            .bind(tenant_id)
            .bind(instance_id)
            .bind(run_id)
            .bind(definition_id)
            .bind(definition_version.map(|value| value as i32))
            .bind(artifact_hash)
            .bind(batch_id)
            .bind(&chunk_id)
            .bind(i32::try_from(chunk_index).context("bulk chunk index exceeds i32")?)
            .bind(0_i32)
            .bind(i32::try_from(chunk_items.len()).context("bulk chunk item_count exceeds i32")?)
            .bind(activity_type)
            .bind(task_queue)
            .bind(state)
            .bind(1_i64)
            .bind(i32::try_from(max_attempts).context("bulk chunk max_attempts exceeds i32")?)
            .bind(i64::try_from(retry_delay_ms).context("bulk chunk retry_delay_ms exceeds i64")?)
            .bind(Json(Value::Null))
            .bind(Json(Value::Null))
            .bind(Json(chunk_items.to_vec()))
            .bind(scheduled_at)
            .execute(tx.as_mut())
            .await
            .context("failed to upsert workflow bulk chunk")?;
        }

        tx.commit().await.context("failed to commit workflow bulk batch transaction")?;
        let batch = self
            .get_bulk_batch(tenant_id, instance_id, run_id, batch_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("bulk batch not found after upsert"))?;
        let chunks = self
            .list_bulk_chunks_for_batch_page(tenant_id, instance_id, run_id, batch_id, i64::MAX, 0)
            .await?;
        Ok((batch, chunks))
    }

    pub async fn lease_next_bulk_chunks(
        &self,
        tenant_id: &str,
        task_queue: &str,
        worker_id: &str,
        worker_build_id: &str,
        lease_ttl: chrono::Duration,
        limit: usize,
    ) -> Result<Vec<WorkflowBulkChunkRecord>> {
        self.lease_next_bulk_chunks_for_backend(
            tenant_id,
            task_queue,
            worker_id,
            worker_build_id,
            lease_ttl,
            limit,
            "pg-v1",
        )
        .await
    }

    pub async fn lease_next_bulk_chunks_for_backend(
        &self,
        tenant_id: &str,
        task_queue: &str,
        worker_id: &str,
        worker_build_id: &str,
        lease_ttl: chrono::Duration,
        limit: usize,
        throughput_backend: &str,
    ) -> Result<Vec<WorkflowBulkChunkRecord>> {
        self.lease_next_bulk_chunks_for_backend_with_batch_limit(
            tenant_id,
            task_queue,
            worker_id,
            worker_build_id,
            lease_ttl,
            limit,
            throughput_backend,
            None,
        )
        .await
    }

    pub async fn lease_next_bulk_chunks_for_backend_with_batch_limit(
        &self,
        tenant_id: &str,
        task_queue: &str,
        worker_id: &str,
        worker_build_id: &str,
        lease_ttl: chrono::Duration,
        limit: usize,
        throughput_backend: &str,
        max_active_chunks_per_batch: Option<usize>,
    ) -> Result<Vec<WorkflowBulkChunkRecord>> {
        let now = Utc::now();
        let lease_expires_at = now + lease_ttl;
        let rows = sqlx::query(
            r#"
            WITH next_chunks AS (
                SELECT chunk.tenant_id, chunk.workflow_instance_id, chunk.run_id, chunk.batch_id, chunk.chunk_id
                FROM workflow_bulk_chunks chunk
                JOIN workflow_bulk_batches batch
                  ON batch.tenant_id = chunk.tenant_id
                 AND batch.workflow_instance_id = chunk.workflow_instance_id
                 AND batch.run_id = chunk.run_id
                 AND batch.batch_id = chunk.batch_id
                WHERE chunk.tenant_id = $1
                  AND chunk.task_queue = $2
                  AND chunk.status = 'scheduled'
                  AND chunk.available_at <= $3
                  AND batch.throughput_backend = $8
                  AND batch.status IN ('scheduled', 'running')
                  AND (
                    $9::BIGINT IS NULL OR (
                        SELECT COUNT(*)
                        FROM workflow_bulk_chunks active_chunk
                        WHERE active_chunk.tenant_id = chunk.tenant_id
                          AND active_chunk.workflow_instance_id = chunk.workflow_instance_id
                          AND active_chunk.run_id = chunk.run_id
                          AND active_chunk.batch_id = chunk.batch_id
                          AND active_chunk.status = 'started'
                    ) < $9
                  )
                ORDER BY chunk.available_at ASC, chunk.scheduled_at ASC, chunk.chunk_index ASC
                LIMIT $4
                FOR UPDATE OF chunk SKIP LOCKED
            ),
            updated_chunks AS (
                UPDATE workflow_bulk_chunks chunk
                SET status = 'started',
                    worker_id = $5,
                    worker_build_id = $6,
                    lease_token = md5(random()::text || clock_timestamp()::text || chunk.chunk_id)::uuid,
                    lease_epoch = chunk.lease_epoch + 1,
                    started_at = COALESCE(chunk.started_at, $3),
                    lease_expires_at = $7,
                    updated_at = $3
                FROM next_chunks
                WHERE chunk.tenant_id = next_chunks.tenant_id
                  AND chunk.workflow_instance_id = next_chunks.workflow_instance_id
                  AND chunk.run_id = next_chunks.run_id
                  AND chunk.batch_id = next_chunks.batch_id
                  AND chunk.chunk_id = next_chunks.chunk_id
                RETURNING chunk.*
            )
            SELECT * FROM updated_chunks
            ORDER BY chunk_index ASC
            "#,
        )
        .bind(tenant_id)
        .bind(task_queue)
        .bind(now)
        .bind(i64::try_from(limit.max(1)).context("bulk chunk lease limit exceeds i64")?)
        .bind(worker_id)
        .bind(worker_build_id)
        .bind(lease_expires_at)
        .bind(throughput_backend)
        .bind(
            max_active_chunks_per_batch
                .map(|value| i64::try_from(value).context("max_active_chunks_per_batch exceeds i64"))
                .transpose()?,
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to lease workflow bulk chunks")?;

        if !rows.is_empty() {
            let batch_ids = rows
                .iter()
                .map(|row| row.try_get::<String, _>("batch_id"))
                .collect::<std::result::Result<Vec<_>, _>>()
                .context("bulk chunk batch_id missing")?;
            sqlx::query(
                r#"
                UPDATE workflow_bulk_batches
                SET status = CASE WHEN status = 'scheduled' THEN 'running' ELSE status END,
                    updated_at = $2
                WHERE tenant_id = $1
                  AND batch_id = ANY($3)
                "#,
            )
            .bind(tenant_id)
            .bind(now)
            .bind(&batch_ids)
            .execute(&self.pool)
            .await
            .context("failed to mark workflow bulk batches running")?;
        }

        rows.into_iter().map(Self::decode_bulk_chunk_row).collect()
    }

    pub async fn lease_bulk_chunk_by_id_for_backend(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        batch_id: &str,
        chunk_id: &str,
        worker_id: &str,
        worker_build_id: &str,
        lease_ttl: chrono::Duration,
        throughput_backend: &str,
        max_active_chunks_per_batch: Option<usize>,
    ) -> Result<Option<WorkflowBulkChunkRecord>> {
        let now = Utc::now();
        let lease_expires_at = now + lease_ttl;
        let row = sqlx::query(
            r#"
            UPDATE workflow_bulk_chunks chunk
            SET status = 'started',
                worker_id = $6,
                worker_build_id = $7,
                lease_token = md5(random()::text || clock_timestamp()::text || chunk.chunk_id)::uuid,
                lease_epoch = chunk.lease_epoch + 1,
                started_at = COALESCE(chunk.started_at, $8),
                lease_expires_at = $9,
                updated_at = $8
            FROM workflow_bulk_batches batch
            WHERE chunk.tenant_id = $1
              AND chunk.workflow_instance_id = $2
              AND chunk.run_id = $3
              AND chunk.batch_id = $4
              AND chunk.chunk_id = $5
              AND chunk.status = 'scheduled'
              AND chunk.available_at <= $8
              AND batch.tenant_id = chunk.tenant_id
              AND batch.workflow_instance_id = chunk.workflow_instance_id
              AND batch.run_id = chunk.run_id
              AND batch.batch_id = chunk.batch_id
              AND batch.throughput_backend = $10
              AND batch.status IN ('scheduled', 'running')
              AND (
                $11::BIGINT IS NULL OR (
                    SELECT COUNT(*)
                    FROM workflow_bulk_chunks active_chunk
                    WHERE active_chunk.tenant_id = chunk.tenant_id
                      AND active_chunk.workflow_instance_id = chunk.workflow_instance_id
                      AND active_chunk.run_id = chunk.run_id
                      AND active_chunk.batch_id = chunk.batch_id
                      AND active_chunk.status = 'started'
                ) < $11
              )
            RETURNING chunk.*
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(batch_id)
        .bind(chunk_id)
        .bind(worker_id)
        .bind(worker_build_id)
        .bind(now)
        .bind(lease_expires_at)
        .bind(throughput_backend)
        .bind(
            max_active_chunks_per_batch
                .map(|value| i64::try_from(value).context("max_active_chunks_per_batch exceeds i64"))
                .transpose()?,
        )
        .fetch_optional(&self.pool)
        .await
        .context("failed to lease specific workflow bulk chunk")?;

        let record = row.map(Self::decode_bulk_chunk_row).transpose()?;
        if record.is_some() {
            sqlx::query(
                r#"
                UPDATE workflow_bulk_batches
                SET status = CASE WHEN status = 'scheduled' THEN 'running' ELSE status END,
                    updated_at = $5
                WHERE tenant_id = $1
                  AND workflow_instance_id = $2
                  AND run_id = $3
                  AND batch_id = $4
                "#,
            )
            .bind(tenant_id)
            .bind(instance_id)
            .bind(run_id)
            .bind(batch_id)
            .bind(now)
            .execute(&self.pool)
            .await
            .context("failed to mark workflow bulk batch running for specific lease")?;
        }
        Ok(record)
    }

    pub async fn count_started_bulk_chunks_for_backend_scope(
        &self,
        throughput_backend: &str,
        tenant_id: Option<&str>,
        task_queue: Option<&str>,
        batch_id: Option<&str>,
    ) -> Result<u64> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM workflow_bulk_chunks chunk
            JOIN workflow_bulk_batches batch
              ON batch.tenant_id = chunk.tenant_id
             AND batch.workflow_instance_id = chunk.workflow_instance_id
             AND batch.run_id = chunk.run_id
             AND batch.batch_id = chunk.batch_id
            WHERE batch.throughput_backend = $1
              AND chunk.status = 'started'
              AND ($2::TEXT IS NULL OR chunk.tenant_id = $2)
              AND ($3::TEXT IS NULL OR chunk.task_queue = $3)
              AND ($4::TEXT IS NULL OR chunk.batch_id = $4)
            "#,
        )
        .bind(throughput_backend)
        .bind(tenant_id)
        .bind(task_queue)
        .bind(batch_id)
        .fetch_one(&self.pool)
        .await
        .context("failed to count started workflow bulk chunks")?;
        Ok(count as u64)
    }

    pub async fn has_ready_bulk_chunks_for_backend(
        &self,
        tenant_id: &str,
        task_queue: &str,
        throughput_backend: &str,
        now: DateTime<Utc>,
    ) -> Result<bool> {
        let exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM workflow_bulk_chunks chunk
                JOIN workflow_bulk_batches batch
                  ON batch.tenant_id = chunk.tenant_id
                 AND batch.workflow_instance_id = chunk.workflow_instance_id
                 AND batch.run_id = chunk.run_id
                 AND batch.batch_id = chunk.batch_id
                WHERE chunk.tenant_id = $1
                  AND chunk.task_queue = $2
                  AND chunk.status = 'scheduled'
                  AND chunk.available_at <= $3
                  AND batch.throughput_backend = $4
                  AND batch.status IN ('scheduled', 'running')
            )
            "#,
        )
        .bind(tenant_id)
        .bind(task_queue)
        .bind(now)
        .bind(throughput_backend)
        .fetch_one(&self.pool)
        .await
        .context("failed to check ready workflow bulk chunks")?;
        Ok(exists)
    }

    pub async fn list_started_bulk_chunks(
        &self,
        limit: usize,
    ) -> Result<Vec<WorkflowBulkChunkRecord>> {
        self.list_started_bulk_chunks_for_backend(limit, "pg-v1").await
    }

    pub async fn list_started_bulk_chunks_for_backend(
        &self,
        limit: usize,
        throughput_backend: &str,
    ) -> Result<Vec<WorkflowBulkChunkRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT chunk.*
            FROM workflow_bulk_chunks chunk
            JOIN workflow_bulk_batches batch
              ON batch.tenant_id = chunk.tenant_id
             AND batch.workflow_instance_id = chunk.workflow_instance_id
             AND batch.run_id = chunk.run_id
             AND batch.batch_id = chunk.batch_id
            WHERE chunk.status = 'started'
              AND batch.throughput_backend = $2
            ORDER BY lease_expires_at ASC NULLS LAST, scheduled_at ASC, chunk_index ASC
            LIMIT $1
            "#,
        )
        .bind(i64::try_from(limit).context("bulk chunk list limit exceeds i64")?)
        .bind(throughput_backend)
        .fetch_all(&self.pool)
        .await
        .context("failed to list started workflow bulk chunks")?;
        rows.into_iter().map(Self::decode_bulk_chunk_row).collect()
    }

    pub async fn requeue_bulk_chunk(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        batch_id: &str,
        chunk_id: &str,
        occurred_at: DateTime<Utc>,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_bulk_chunks
            SET status = 'scheduled',
                worker_id = NULL,
                worker_build_id = NULL,
                lease_token = NULL,
                lease_expires_at = NULL,
                available_at = $6,
                updated_at = $6
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND batch_id = $4
              AND chunk_id = $5
              AND status = 'started'
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(batch_id)
        .bind(chunk_id)
        .bind(occurred_at)
        .execute(&self.pool)
        .await
        .context("failed to requeue workflow bulk chunk")?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn cancel_bulk_batch(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        batch_id: &str,
        reason: &str,
        metadata: Option<&Value>,
        occurred_at: DateTime<Utc>,
    ) -> Result<CancelledBulkBatch> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin workflow bulk batch cancellation transaction")?;

        let batch_row = sqlx::query(
            r#"
            SELECT *
            FROM workflow_bulk_batches
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND batch_id = $4
            FOR UPDATE
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(batch_id)
        .fetch_optional(tx.as_mut())
        .await
        .context("failed to load workflow bulk batch for cancellation")?
        .ok_or_else(|| anyhow::anyhow!("workflow bulk batch {batch_id} not found"))?;
        let mut batch = Self::decode_bulk_batch_row(batch_row)?;

        if batch.status.is_terminal() {
            tx.commit().await.context("failed to commit terminal bulk batch cancellation")?;
            return Ok(CancelledBulkBatch {
                batch,
                cancelled_chunks: Vec::new(),
                terminal_event: None,
            });
        }

        let cancelled_rows = sqlx::query(
            r#"
            UPDATE workflow_bulk_chunks
            SET status = 'cancelled',
                error = $5,
                cancellation_requested = TRUE,
                cancellation_reason = $5,
                cancellation_metadata = $6,
                completed_at = $7,
                lease_token = NULL,
                lease_expires_at = NULL,
                updated_at = $7
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND batch_id = $4
              AND status IN ('scheduled', 'started')
            RETURNING *
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(batch_id)
        .bind(reason)
        .bind(metadata.map(Json))
        .bind(occurred_at)
        .fetch_all(tx.as_mut())
        .await
        .context("failed to cancel workflow bulk chunks")?;
        let cancelled_chunks = cancelled_rows
            .into_iter()
            .map(Self::decode_bulk_chunk_row)
            .collect::<Result<Vec<_>>>()?;

        let cancelled_items = cancelled_chunks.iter().try_fold(0_u32, |acc, chunk| {
            let count = u32::try_from(chunk.items.len())
                .context("workflow bulk chunk item count exceeds u32 during cancellation")?;
            Ok::<u32, anyhow::Error>(acc.saturating_add(count))
        })?;

        batch.cancelled_items = batch.cancelled_items.saturating_add(cancelled_items);
        batch.status = WorkflowBulkBatchStatus::Cancelled;
        batch.error = Some(reason.to_owned());
        batch.terminal_at = Some(occurred_at);

        sqlx::query(
            r#"
            UPDATE workflow_bulk_batches
            SET status = 'cancelled',
                cancelled_items = $5,
                error = $6,
                terminal_at = $7,
                updated_at = $7
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND batch_id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(batch_id)
        .bind(
            i32::try_from(batch.cancelled_items)
                .context("bulk batch cancelled_items exceeds i32 during cancellation")?,
        )
        .bind(reason)
        .bind(occurred_at)
        .execute(tx.as_mut())
        .await
        .context("failed to update workflow bulk batch cancellation")?;

        let terminal_event = Some(Self::bulk_terminal_event(
            &batch,
            WorkflowEvent::BulkActivityBatchCancelled {
                batch_id: batch.batch_id.clone(),
                total_items: batch.total_items,
                succeeded_items: batch.succeeded_items,
                failed_items: batch.failed_items,
                cancelled_items: batch.cancelled_items,
                chunk_count: batch.chunk_count,
                message: reason.to_owned(),
            },
        ));
        if let Some(event) = terminal_event.as_ref() {
            self.enqueue_workflow_event_outbox(tx.as_mut(), event, occurred_at).await?;
        }

        batch = self
            .get_bulk_batch_with_executor(tx.as_mut(), tenant_id, instance_id, run_id, batch_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("workflow bulk batch missing after cancellation"))?;

        tx.commit()
            .await
            .context("failed to commit workflow bulk batch cancellation transaction")?;
        Ok(CancelledBulkBatch { batch, cancelled_chunks, terminal_event })
    }

    pub async fn apply_bulk_chunk_terminal_update(
        &self,
        update: &BulkChunkTerminalUpdate,
    ) -> Result<AppliedBulkChunkTerminalUpdate> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("failed to begin workflow bulk chunk update transaction")?;

        let batch_row = sqlx::query(
            r#"
            SELECT *
            FROM workflow_bulk_batches
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND batch_id = $4
            FOR UPDATE
            "#,
        )
        .bind(&update.tenant_id)
        .bind(&update.instance_id)
        .bind(&update.run_id)
        .bind(&update.batch_id)
        .fetch_optional(tx.as_mut())
        .await
        .context("failed to load workflow bulk batch for terminal update")?
        .ok_or_else(|| anyhow::anyhow!("workflow bulk batch {} not found", update.batch_id))?;
        let mut batch = Self::decode_bulk_batch_row(batch_row)?;

        let chunk_row = sqlx::query(
            r#"
            SELECT *
            FROM workflow_bulk_chunks
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND batch_id = $4
              AND chunk_id = $5
            FOR UPDATE
            "#,
        )
        .bind(&update.tenant_id)
        .bind(&update.instance_id)
        .bind(&update.run_id)
        .bind(&update.batch_id)
        .bind(&update.chunk_id)
        .fetch_optional(tx.as_mut())
        .await
        .context("failed to load workflow bulk chunk for terminal update")?
        .ok_or_else(|| anyhow::anyhow!("workflow bulk chunk {} not found", update.chunk_id))?;
        let mut chunk = Self::decode_bulk_chunk_row(chunk_row)?;

        let mut terminal_event = None;
        if !batch.status.is_terminal()
            && chunk.status == WorkflowBulkChunkStatus::Started
            && chunk.attempt == update.attempt
            && chunk.group_id == update.group_id
            && chunk.lease_epoch == update.lease_epoch
            && chunk.owner_epoch == update.owner_epoch
            && chunk
                .last_report_id
                .as_ref()
                .map(|report_id| report_id != &update.report_id)
                .unwrap_or(true)
            && chunk.lease_token == Some(update.lease_token)
        {
            match &update.payload {
                BulkChunkTerminalPayload::Completed { output } => {
                    sqlx::query(
                        r#"
                        UPDATE workflow_bulk_chunks
                        SET status = 'completed',
                            output = $6,
                            error = NULL,
                            last_report_id = $7,
                            completed_at = $8,
                            lease_token = NULL,
                            lease_expires_at = NULL,
                            updated_at = $8
                        WHERE tenant_id = $1
                          AND workflow_instance_id = $2
                          AND run_id = $3
                          AND batch_id = $4
                          AND chunk_id = $5
                        "#,
                    )
                    .bind(&update.tenant_id)
                    .bind(&update.instance_id)
                    .bind(&update.run_id)
                    .bind(&update.batch_id)
                    .bind(&update.chunk_id)
                    .bind(Json(output))
                    .bind(&update.report_id)
                    .bind(update.occurred_at)
                    .execute(tx.as_mut())
                    .await
                    .context("failed to mark workflow bulk chunk completed")?;
                    let succeeded = u32::try_from(chunk.items.len())
                        .context("workflow bulk chunk item count exceeds u32")?;
                    batch.succeeded_items = batch.succeeded_items.saturating_add(succeeded);
                    if batch.succeeded_items >= batch.total_items {
                        batch.status = WorkflowBulkBatchStatus::Completed;
                        batch.terminal_at = Some(update.occurred_at);
                        terminal_event = Some(Self::bulk_terminal_event(
                            &batch,
                            WorkflowEvent::BulkActivityBatchCompleted {
                                batch_id: batch.batch_id.clone(),
                                total_items: batch.total_items,
                                succeeded_items: batch.succeeded_items,
                                failed_items: batch.failed_items,
                                cancelled_items: batch.cancelled_items,
                                chunk_count: batch.chunk_count,
                            },
                        ));
                    } else if batch.status == WorkflowBulkBatchStatus::Scheduled {
                        batch.status = WorkflowBulkBatchStatus::Running;
                    }
                }
                BulkChunkTerminalPayload::Failed { error } => {
                    if chunk.attempt < chunk.max_attempts {
                        let retry_at = update.occurred_at
                            + chrono::Duration::milliseconds(chunk.retry_delay_ms as i64);
                        sqlx::query(
                            r#"
                            UPDATE workflow_bulk_chunks
                            SET status = 'scheduled',
                                attempt = attempt + 1,
                                error = $6,
                                last_report_id = $7,
                                worker_id = NULL,
                                worker_build_id = NULL,
                                lease_token = NULL,
                                lease_expires_at = NULL,
                                available_at = $8,
                                updated_at = $8
                            WHERE tenant_id = $1
                              AND workflow_instance_id = $2
                              AND run_id = $3
                              AND batch_id = $4
                              AND chunk_id = $5
                            "#,
                        )
                        .bind(&update.tenant_id)
                        .bind(&update.instance_id)
                        .bind(&update.run_id)
                        .bind(&update.batch_id)
                        .bind(&update.chunk_id)
                        .bind(error)
                        .bind(&update.report_id)
                        .bind(retry_at)
                        .execute(tx.as_mut())
                        .await
                        .context("failed to reschedule workflow bulk chunk retry")?;
                    } else {
                        sqlx::query(
                            r#"
                            UPDATE workflow_bulk_chunks
                            SET status = 'failed',
                                error = $6,
                                last_report_id = $7,
                                completed_at = $8,
                                lease_token = NULL,
                                lease_expires_at = NULL,
                                updated_at = $8
                            WHERE tenant_id = $1
                              AND workflow_instance_id = $2
                              AND run_id = $3
                              AND batch_id = $4
                              AND chunk_id = $5
                            "#,
                        )
                        .bind(&update.tenant_id)
                        .bind(&update.instance_id)
                        .bind(&update.run_id)
                        .bind(&update.batch_id)
                        .bind(&update.chunk_id)
                        .bind(error)
                        .bind(&update.report_id)
                        .bind(update.occurred_at)
                        .execute(tx.as_mut())
                        .await
                        .context("failed to mark workflow bulk chunk failed")?;
                        batch.failed_items = batch.failed_items.saturating_add(
                            u32::try_from(chunk.items.len())
                                .context("workflow bulk chunk item count exceeds u32")?,
                        );
                        batch.status = WorkflowBulkBatchStatus::Failed;
                        batch.error = Some(error.clone());
                        batch.terminal_at = Some(update.occurred_at);
                        terminal_event = Some(Self::bulk_terminal_event(
                            &batch,
                            WorkflowEvent::BulkActivityBatchFailed {
                                batch_id: batch.batch_id.clone(),
                                total_items: batch.total_items,
                                succeeded_items: batch.succeeded_items,
                                failed_items: batch.failed_items,
                                cancelled_items: batch.cancelled_items,
                                chunk_count: batch.chunk_count,
                                message: error.clone(),
                            },
                        ));
                    }
                }
                BulkChunkTerminalPayload::Cancelled { reason, .. } => {
                    sqlx::query(
                        r#"
                        UPDATE workflow_bulk_chunks
                        SET status = 'cancelled',
                            error = $6,
                            last_report_id = $7,
                            completed_at = $8,
                            lease_token = NULL,
                            lease_expires_at = NULL,
                            updated_at = $8
                        WHERE tenant_id = $1
                          AND workflow_instance_id = $2
                          AND run_id = $3
                          AND batch_id = $4
                          AND chunk_id = $5
                        "#,
                    )
                    .bind(&update.tenant_id)
                    .bind(&update.instance_id)
                    .bind(&update.run_id)
                    .bind(&update.batch_id)
                    .bind(&update.chunk_id)
                    .bind(reason)
                    .bind(&update.report_id)
                    .bind(update.occurred_at)
                    .execute(tx.as_mut())
                    .await
                    .context("failed to mark workflow bulk chunk cancelled")?;
                    batch.cancelled_items = batch.cancelled_items.saturating_add(
                        u32::try_from(chunk.items.len())
                            .context("workflow bulk chunk item count exceeds u32")?,
                    );
                    batch.status = WorkflowBulkBatchStatus::Cancelled;
                    batch.error = Some(reason.clone());
                    batch.terminal_at = Some(update.occurred_at);
                    terminal_event = Some(Self::bulk_terminal_event(
                        &batch,
                        WorkflowEvent::BulkActivityBatchCancelled {
                            batch_id: batch.batch_id.clone(),
                            total_items: batch.total_items,
                            succeeded_items: batch.succeeded_items,
                            failed_items: batch.failed_items,
                            cancelled_items: batch.cancelled_items,
                            chunk_count: batch.chunk_count,
                            message: reason.clone(),
                        },
                    ));
                }
            }

            sqlx::query(
                r#"
                UPDATE workflow_bulk_batches
                SET status = $5,
                    succeeded_items = $6,
                    failed_items = $7,
                    cancelled_items = $8,
                    error = $9,
                    terminal_at = $10,
                    updated_at = $11
                WHERE tenant_id = $1
                  AND workflow_instance_id = $2
                  AND run_id = $3
                  AND batch_id = $4
                "#,
            )
            .bind(&batch.tenant_id)
            .bind(&batch.instance_id)
            .bind(&batch.run_id)
            .bind(&batch.batch_id)
            .bind(batch.status.as_str())
            .bind(
                i32::try_from(batch.succeeded_items)
                    .context("bulk batch succeeded_items exceeds i32")?,
            )
            .bind(i32::try_from(batch.failed_items).context("bulk batch failed_items exceeds i32")?)
            .bind(
                i32::try_from(batch.cancelled_items)
                    .context("bulk batch cancelled_items exceeds i32")?,
            )
            .bind(&batch.error)
            .bind(batch.terminal_at)
            .bind(update.occurred_at)
            .execute(tx.as_mut())
            .await
            .context("failed to update workflow bulk batch counters")?;

            if let Some(event) = terminal_event.as_ref() {
                self.enqueue_workflow_event_outbox(tx.as_mut(), event, update.occurred_at).await?;
            }

            batch = self
                .get_bulk_batch_with_executor(
                    tx.as_mut(),
                    &update.tenant_id,
                    &update.instance_id,
                    &update.run_id,
                    &update.batch_id,
                )
                .await?
                .ok_or_else(|| anyhow::anyhow!("workflow bulk batch missing after update"))?;
            chunk = self
                .get_bulk_chunk_with_executor(
                    tx.as_mut(),
                    &update.tenant_id,
                    &update.instance_id,
                    &update.run_id,
                    &update.batch_id,
                    &update.chunk_id,
                )
                .await?
                .ok_or_else(|| anyhow::anyhow!("workflow bulk chunk missing after update"))?;
        }

        tx.commit().await.context("failed to commit workflow bulk chunk update transaction")?;
        Ok(AppliedBulkChunkTerminalUpdate { chunk, batch, terminal_event })
    }

    pub async fn lease_workflow_event_outbox(
        &self,
        publisher_id: &str,
        lease_ttl: chrono::Duration,
        limit: usize,
        now: DateTime<Utc>,
    ) -> Result<Vec<WorkflowEventOutboxRecord>> {
        let lease_expires_at = now + lease_ttl;
        let rows = sqlx::query(
            r#"
            WITH candidates AS (
                SELECT outbox_id
                FROM workflow_event_outbox
                WHERE published_at IS NULL
                  AND available_at <= $1
                  AND (lease_expires_at IS NULL OR lease_expires_at <= $1)
                ORDER BY created_at, outbox_id
                LIMIT $4
                FOR UPDATE SKIP LOCKED
            )
            UPDATE workflow_event_outbox outbox
            SET leased_by = $2,
                lease_expires_at = $3,
                updated_at = $1
            FROM candidates
            WHERE outbox.outbox_id = candidates.outbox_id
            RETURNING outbox.*
            "#,
        )
        .bind(now)
        .bind(publisher_id)
        .bind(lease_expires_at)
        .bind(i64::try_from(limit.max(1)).context("workflow event outbox lease limit exceeds i64")?)
        .fetch_all(&self.pool)
        .await
        .context("failed to lease workflow event outbox rows")?;
        rows.into_iter().map(Self::decode_workflow_event_outbox_row).collect()
    }

    pub async fn mark_workflow_event_outbox_published(
        &self,
        outbox_id: Uuid,
        publisher_id: &str,
        published_at: DateTime<Utc>,
    ) -> Result<bool> {
        let updated = sqlx::query(
            r#"
            UPDATE workflow_event_outbox
            SET published_at = $3,
                leased_by = NULL,
                lease_expires_at = NULL,
                updated_at = $3
            WHERE outbox_id = $1
              AND leased_by = $2
              AND published_at IS NULL
            "#,
        )
        .bind(outbox_id)
        .bind(publisher_id)
        .bind(published_at)
        .execute(&self.pool)
        .await
        .context("failed to mark workflow event outbox row published")?;
        Ok(updated.rows_affected() > 0)
    }

    pub async fn release_workflow_event_outbox_lease(
        &self,
        outbox_id: Uuid,
        publisher_id: &str,
        available_at: DateTime<Utc>,
    ) -> Result<bool> {
        let updated = sqlx::query(
            r#"
            UPDATE workflow_event_outbox
            SET leased_by = NULL,
                lease_expires_at = NULL,
                available_at = $3,
                updated_at = $3
            WHERE outbox_id = $1
              AND leased_by = $2
              AND published_at IS NULL
            "#,
        )
        .bind(outbox_id)
        .bind(publisher_id)
        .bind(available_at)
        .execute(&self.pool)
        .await
        .context("failed to release workflow event outbox lease")?;
        Ok(updated.rows_affected() > 0)
    }

    pub async fn ensure_throughput_bridge_progress(
        &self,
        workflow_event_id: Uuid,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        batch_id: &str,
        throughput_backend: &str,
        dedupe_key: &str,
        created_at: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>> {
        let published_at = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
            r#"
            INSERT INTO throughput_bridge_progress (
                workflow_event_id,
                tenant_id,
                workflow_instance_id,
                run_id,
                batch_id,
                throughput_backend,
                dedupe_key,
                created_at,
                command_published_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL)
            ON CONFLICT (workflow_event_id) DO UPDATE
            SET tenant_id = EXCLUDED.tenant_id,
                workflow_instance_id = EXCLUDED.workflow_instance_id,
                run_id = EXCLUDED.run_id,
                batch_id = EXCLUDED.batch_id,
                throughput_backend = EXCLUDED.throughput_backend,
                dedupe_key = EXCLUDED.dedupe_key
            RETURNING command_published_at
            "#,
        )
        .bind(workflow_event_id)
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(batch_id)
        .bind(throughput_backend)
        .bind(dedupe_key)
        .bind(created_at)
        .fetch_one(&self.pool)
        .await
        .context("failed to ensure throughput bridge progress")?;
        Ok(published_at)
    }

    pub async fn mark_throughput_bridge_command_published(
        &self,
        workflow_event_id: Uuid,
        published_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE throughput_bridge_progress
            SET command_published_at = COALESCE(command_published_at, $2)
            WHERE workflow_event_id = $1
            "#,
        )
        .bind(workflow_event_id)
        .bind(published_at)
        .execute(&self.pool)
        .await
        .context("failed to mark throughput bridge command published")?;
        Ok(())
    }

    pub async fn upsert_throughput_projection_batch(
        &self,
        batch: &WorkflowBulkBatchRecord,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO throughput_projection_batches (
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                workflow_version,
                artifact_hash,
                batch_id,
                activity_type,
                task_queue,
                state,
                input_handle,
                result_handle,
                throughput_backend,
                throughput_backend_version,
                execution_policy,
                reducer,
                reducer_class,
                aggregation_tree_depth,
                fast_lane_enabled,
                aggregation_group_count,
                status,
                total_items,
                chunk_size,
                chunk_count,
                succeeded_items,
                failed_items,
                cancelled_items,
                max_attempts,
                retry_delay_ms,
                error,
                scheduled_at,
                terminal_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33
            )
            ON CONFLICT (tenant_id, workflow_instance_id, run_id, batch_id) DO UPDATE
            SET activity_type = EXCLUDED.activity_type,
                task_queue = EXCLUDED.task_queue,
                state = EXCLUDED.state,
                input_handle = EXCLUDED.input_handle,
                result_handle = EXCLUDED.result_handle,
                throughput_backend = EXCLUDED.throughput_backend,
                throughput_backend_version = EXCLUDED.throughput_backend_version,
                execution_policy = EXCLUDED.execution_policy,
                reducer = EXCLUDED.reducer,
                reducer_class = EXCLUDED.reducer_class,
                aggregation_tree_depth = EXCLUDED.aggregation_tree_depth,
                fast_lane_enabled = EXCLUDED.fast_lane_enabled,
                aggregation_group_count = EXCLUDED.aggregation_group_count,
                status = EXCLUDED.status,
                total_items = EXCLUDED.total_items,
                chunk_size = EXCLUDED.chunk_size,
                chunk_count = EXCLUDED.chunk_count,
                succeeded_items = EXCLUDED.succeeded_items,
                failed_items = EXCLUDED.failed_items,
                cancelled_items = EXCLUDED.cancelled_items,
                max_attempts = EXCLUDED.max_attempts,
                retry_delay_ms = EXCLUDED.retry_delay_ms,
                error = EXCLUDED.error,
                scheduled_at = EXCLUDED.scheduled_at,
                terminal_at = EXCLUDED.terminal_at,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(&batch.tenant_id)
        .bind(&batch.instance_id)
        .bind(&batch.run_id)
        .bind(&batch.definition_id)
        .bind(batch.definition_version.map(|value| value as i32))
        .bind(&batch.artifact_hash)
        .bind(&batch.batch_id)
        .bind(&batch.activity_type)
        .bind(&batch.task_queue)
        .bind(&batch.state)
        .bind(Json(&batch.input_handle))
        .bind(Json(&batch.result_handle))
        .bind(&batch.throughput_backend)
        .bind(&batch.throughput_backend_version)
        .bind(&batch.execution_policy)
        .bind(&batch.reducer)
        .bind(&batch.reducer_class)
        .bind(
            i32::try_from(batch.aggregation_tree_depth)
                .context("projection batch aggregation_tree_depth exceeds i32")?,
        )
        .bind(batch.fast_lane_enabled)
        .bind(
            i32::try_from(batch.aggregation_group_count)
                .context("projection batch aggregation_group_count exceeds i32")?,
        )
        .bind(batch.status.as_str())
        .bind(i32::try_from(batch.total_items).context("projection batch total_items exceeds i32")?)
        .bind(i32::try_from(batch.chunk_size).context("projection batch chunk_size exceeds i32")?)
        .bind(i32::try_from(batch.chunk_count).context("projection batch chunk_count exceeds i32")?)
        .bind(
            i32::try_from(batch.succeeded_items)
                .context("projection batch succeeded_items exceeds i32")?,
        )
        .bind(
            i32::try_from(batch.failed_items)
                .context("projection batch failed_items exceeds i32")?,
        )
        .bind(
            i32::try_from(batch.cancelled_items)
                .context("projection batch cancelled_items exceeds i32")?,
        )
        .bind(
            i32::try_from(batch.max_attempts)
                .context("projection batch max_attempts exceeds i32")?,
        )
        .bind(
            i64::try_from(batch.retry_delay_ms)
                .context("projection batch retry_delay_ms exceeds i64")?,
        )
        .bind(&batch.error)
        .bind(batch.scheduled_at)
        .bind(batch.terminal_at)
        .bind(batch.updated_at)
        .execute(&self.pool)
        .await
        .context("failed to upsert throughput projection batch")?;
        Ok(())
    }

    pub async fn upsert_throughput_projection_chunk(
        &self,
        chunk: &WorkflowBulkChunkRecord,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO throughput_projection_chunks (
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                workflow_version,
                artifact_hash,
                batch_id,
                chunk_id,
                chunk_index,
                group_id,
                item_count,
                activity_type,
                task_queue,
                state,
                status,
                attempt,
                lease_epoch,
                owner_epoch,
                max_attempts,
                retry_delay_ms,
                input_handle,
                result_handle,
                items,
                output,
                error,
                cancellation_requested,
                cancellation_reason,
                cancellation_metadata,
                worker_id,
                worker_build_id,
                lease_token,
                last_report_id,
                scheduled_at,
                available_at,
                started_at,
                lease_expires_at,
                completed_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32,
                $33, $34, $35, $36, $37, $38
            )
            ON CONFLICT (tenant_id, workflow_instance_id, run_id, batch_id, chunk_id) DO UPDATE
            SET chunk_index = EXCLUDED.chunk_index,
                group_id = EXCLUDED.group_id,
                item_count = EXCLUDED.item_count,
                activity_type = EXCLUDED.activity_type,
                task_queue = EXCLUDED.task_queue,
                state = EXCLUDED.state,
                status = EXCLUDED.status,
                attempt = EXCLUDED.attempt,
                lease_epoch = EXCLUDED.lease_epoch,
                owner_epoch = EXCLUDED.owner_epoch,
                max_attempts = EXCLUDED.max_attempts,
                retry_delay_ms = EXCLUDED.retry_delay_ms,
                input_handle = EXCLUDED.input_handle,
                result_handle = EXCLUDED.result_handle,
                items = EXCLUDED.items,
                output = EXCLUDED.output,
                error = EXCLUDED.error,
                cancellation_requested = EXCLUDED.cancellation_requested,
                cancellation_reason = EXCLUDED.cancellation_reason,
                cancellation_metadata = EXCLUDED.cancellation_metadata,
                worker_id = EXCLUDED.worker_id,
                worker_build_id = EXCLUDED.worker_build_id,
                lease_token = EXCLUDED.lease_token,
                last_report_id = EXCLUDED.last_report_id,
                scheduled_at = EXCLUDED.scheduled_at,
                available_at = EXCLUDED.available_at,
                started_at = EXCLUDED.started_at,
                lease_expires_at = EXCLUDED.lease_expires_at,
                completed_at = EXCLUDED.completed_at,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(&chunk.tenant_id)
        .bind(&chunk.instance_id)
        .bind(&chunk.run_id)
        .bind(&chunk.definition_id)
        .bind(chunk.definition_version.map(|value| value as i32))
        .bind(&chunk.artifact_hash)
        .bind(&chunk.batch_id)
        .bind(&chunk.chunk_id)
        .bind(i32::try_from(chunk.chunk_index).context("projection chunk chunk_index exceeds i32")?)
        .bind(i32::try_from(chunk.group_id).context("projection chunk group_id exceeds i32")?)
        .bind(i32::try_from(chunk.item_count).context("projection chunk item_count exceeds i32")?)
        .bind(&chunk.activity_type)
        .bind(&chunk.task_queue)
        .bind(&chunk.state)
        .bind(chunk.status.as_str())
        .bind(i32::try_from(chunk.attempt).context("projection chunk attempt exceeds i32")?)
        .bind(i64::try_from(chunk.lease_epoch).context("projection chunk lease_epoch exceeds i64")?)
        .bind(i64::try_from(chunk.owner_epoch).context("projection chunk owner_epoch exceeds i64")?)
        .bind(
            i32::try_from(chunk.max_attempts)
                .context("projection chunk max_attempts exceeds i32")?,
        )
        .bind(
            i64::try_from(chunk.retry_delay_ms)
                .context("projection chunk retry_delay_ms exceeds i64")?,
        )
        .bind(Json(&chunk.input_handle))
        .bind(Json(&chunk.result_handle))
        .bind(Json(&chunk.items))
        .bind(chunk.output.as_ref().map(Json))
        .bind(&chunk.error)
        .bind(chunk.cancellation_requested)
        .bind(&chunk.cancellation_reason)
        .bind(chunk.cancellation_metadata.as_ref().map(Json))
        .bind(&chunk.worker_id)
        .bind(&chunk.worker_build_id)
        .bind(chunk.lease_token)
        .bind(&chunk.last_report_id)
        .bind(chunk.scheduled_at)
        .bind(chunk.available_at)
        .bind(chunk.started_at)
        .bind(chunk.lease_expires_at)
        .bind(chunk.completed_at)
        .bind(chunk.updated_at)
        .execute(&self.pool)
        .await
        .context("failed to upsert throughput projection chunk")?;
        Ok(())
    }

    pub async fn upsert_throughput_projection_chunks(
        &self,
        chunks: &[WorkflowBulkChunkRecord],
    ) -> Result<()> {
        const CHUNK_BATCH_SIZE: usize = 250;

        for batch in chunks.chunks(CHUNK_BATCH_SIZE) {
            let mut builder = QueryBuilder::<Postgres>::new(
                r#"
                INSERT INTO throughput_projection_chunks (
                    tenant_id,
                    workflow_instance_id,
                    run_id,
                    workflow_id,
                    workflow_version,
                    artifact_hash,
                    batch_id,
                    chunk_id,
                    chunk_index,
                    group_id,
                    item_count,
                    activity_type,
                    task_queue,
                    state,
                    status,
                    attempt,
                    lease_epoch,
                    owner_epoch,
                    max_attempts,
                    retry_delay_ms,
                    input_handle,
                    result_handle,
                    items,
                    output,
                    error,
                    cancellation_requested,
                    cancellation_reason,
                    cancellation_metadata,
                    worker_id,
                    worker_build_id,
                    lease_token,
                    last_report_id,
                    scheduled_at,
                    available_at,
                    started_at,
                    lease_expires_at,
                    completed_at,
                    updated_at
                )
                "#,
            );

            builder.push_values(batch, |mut row, chunk| {
                row.push_bind(&chunk.tenant_id)
                    .push_bind(&chunk.instance_id)
                    .push_bind(&chunk.run_id)
                    .push_bind(&chunk.definition_id)
                    .push_bind(chunk.definition_version.map(|value| value as i32))
                    .push_bind(&chunk.artifact_hash)
                    .push_bind(&chunk.batch_id)
                    .push_bind(&chunk.chunk_id)
                    .push_bind(
                        i32::try_from(chunk.chunk_index)
                            .expect("projection chunk chunk_index exceeds i32"),
                    )
                    .push_bind(
                        i32::try_from(chunk.group_id)
                            .expect("projection chunk group_id exceeds i32"),
                    )
                    .push_bind(
                        i32::try_from(chunk.item_count)
                            .expect("projection chunk item_count exceeds i32"),
                    )
                    .push_bind(&chunk.activity_type)
                    .push_bind(&chunk.task_queue)
                    .push_bind(&chunk.state)
                    .push_bind(chunk.status.as_str())
                    .push_bind(
                        i32::try_from(chunk.attempt).expect("projection chunk attempt exceeds i32"),
                    )
                    .push_bind(
                        i64::try_from(chunk.lease_epoch)
                            .expect("projection chunk lease_epoch exceeds i64"),
                    )
                    .push_bind(
                        i64::try_from(chunk.owner_epoch)
                            .expect("projection chunk owner_epoch exceeds i64"),
                    )
                    .push_bind(
                        i32::try_from(chunk.max_attempts)
                            .expect("projection chunk max_attempts exceeds i32"),
                    )
                    .push_bind(
                        i64::try_from(chunk.retry_delay_ms)
                            .expect("projection chunk retry_delay_ms exceeds i64"),
                    )
                    .push_bind(Json(&chunk.input_handle))
                    .push_bind(Json(&chunk.result_handle))
                    .push_bind(Json(&chunk.items))
                    .push_bind(chunk.output.as_ref().map(Json))
                    .push_bind(&chunk.error)
                    .push_bind(chunk.cancellation_requested)
                    .push_bind(&chunk.cancellation_reason)
                    .push_bind(chunk.cancellation_metadata.as_ref().map(Json))
                    .push_bind(&chunk.worker_id)
                    .push_bind(&chunk.worker_build_id)
                    .push_bind(chunk.lease_token)
                    .push_bind(&chunk.last_report_id)
                    .push_bind(chunk.scheduled_at)
                    .push_bind(chunk.available_at)
                    .push_bind(chunk.started_at)
                    .push_bind(chunk.lease_expires_at)
                    .push_bind(chunk.completed_at)
                    .push_bind(chunk.updated_at);
            });

            builder.push(
                r#"
                ON CONFLICT (tenant_id, workflow_instance_id, run_id, batch_id, chunk_id) DO UPDATE
                SET chunk_index = EXCLUDED.chunk_index,
                    group_id = EXCLUDED.group_id,
                    item_count = EXCLUDED.item_count,
                    activity_type = EXCLUDED.activity_type,
                    task_queue = EXCLUDED.task_queue,
                    state = EXCLUDED.state,
                    status = EXCLUDED.status,
                    attempt = EXCLUDED.attempt,
                    lease_epoch = EXCLUDED.lease_epoch,
                    owner_epoch = EXCLUDED.owner_epoch,
                    max_attempts = EXCLUDED.max_attempts,
                    retry_delay_ms = EXCLUDED.retry_delay_ms,
                    input_handle = EXCLUDED.input_handle,
                    result_handle = EXCLUDED.result_handle,
                    items = EXCLUDED.items,
                    output = EXCLUDED.output,
                    error = EXCLUDED.error,
                    cancellation_requested = EXCLUDED.cancellation_requested,
                    cancellation_reason = EXCLUDED.cancellation_reason,
                    cancellation_metadata = EXCLUDED.cancellation_metadata,
                    worker_id = EXCLUDED.worker_id,
                    worker_build_id = EXCLUDED.worker_build_id,
                    lease_token = EXCLUDED.lease_token,
                    last_report_id = EXCLUDED.last_report_id,
                    scheduled_at = EXCLUDED.scheduled_at,
                    available_at = EXCLUDED.available_at,
                    started_at = EXCLUDED.started_at,
                    lease_expires_at = EXCLUDED.lease_expires_at,
                    completed_at = EXCLUDED.completed_at,
                    updated_at = EXCLUDED.updated_at
                "#,
            );

            builder
                .build()
                .execute(&self.pool)
                .await
                .context("failed to upsert throughput projection chunk batch")?;
        }

        Ok(())
    }

    pub async fn update_throughput_projection_batch_state(
        &self,
        update: &ThroughputProjectionBatchStateUpdate,
    ) -> Result<()> {
        self.update_throughput_projection_batch_states(std::slice::from_ref(update)).await
    }

    fn push_throughput_projection_batch_state_update_values<'args>(
        builder: &mut QueryBuilder<'args, Postgres>,
        updates: &'args [ThroughputProjectionBatchStateUpdate],
    ) {
        builder.push_values(updates, |mut row, update| {
            row.push_bind(&update.tenant_id)
                .push_bind(&update.instance_id)
                .push_bind(&update.run_id)
                .push_bind(&update.batch_id)
                .push_bind(&update.status)
                .push_bind(
                    i32::try_from(update.succeeded_items)
                        .expect("projection batch state succeeded_items exceeds i32"),
                )
                .push_bind(
                    i32::try_from(update.failed_items)
                        .expect("projection batch state failed_items exceeds i32"),
                )
                .push_bind(
                    i32::try_from(update.cancelled_items)
                        .expect("projection batch state cancelled_items exceeds i32"),
                )
                .push_bind(&update.error)
                .push_bind(update.terminal_at)
                .push_bind(update.updated_at);
        });
    }

    pub async fn update_throughput_projection_batch_states(
        &self,
        updates: &[ThroughputProjectionBatchStateUpdate],
    ) -> Result<()> {
        const BATCH_STATE_BATCH_SIZE: usize = 250;

        if updates.is_empty() {
            return Ok(());
        }

        for batch in updates.chunks(BATCH_STATE_BATCH_SIZE) {
            let mut builder = QueryBuilder::<Postgres>::new(
                r#"
                WITH updates (
                    tenant_id,
                    workflow_instance_id,
                    run_id,
                    batch_id,
                    status,
                    succeeded_items,
                    failed_items,
                    cancelled_items,
                    error,
                    terminal_at,
                    updated_at
                ) AS (
                "#,
            );
            Self::push_throughput_projection_batch_state_update_values(&mut builder, batch);
            builder.push(
                r#"
                )
                UPDATE throughput_projection_batches
                SET status = CASE
                        WHEN throughput_projection_batches.terminal_at IS NOT NULL
                            AND updates.terminal_at::timestamptz IS NULL
                            THEN throughput_projection_batches.status
                        ELSE updates.status::text
                    END,
                    succeeded_items = GREATEST(
                        throughput_projection_batches.succeeded_items,
                        updates.succeeded_items::integer
                    ),
                    failed_items = GREATEST(
                        throughput_projection_batches.failed_items,
                        updates.failed_items::integer
                    ),
                    cancelled_items = GREATEST(
                        throughput_projection_batches.cancelled_items,
                        updates.cancelled_items::integer
                    ),
                    error = CASE
                        WHEN throughput_projection_batches.terminal_at IS NOT NULL
                            AND updates.terminal_at::timestamptz IS NULL
                            THEN throughput_projection_batches.error
                        ELSE updates.error::text
                    END,
                    terminal_at = COALESCE(
                        throughput_projection_batches.terminal_at,
                        updates.terminal_at::timestamptz
                    ),
                    updated_at = GREATEST(
                        throughput_projection_batches.updated_at,
                        updates.updated_at::timestamptz
                    )
                FROM updates
                WHERE throughput_projection_batches.tenant_id = updates.tenant_id::text
                  AND throughput_projection_batches.workflow_instance_id =
                      updates.workflow_instance_id::text
                  AND throughput_projection_batches.run_id = updates.run_id::text
                  AND throughput_projection_batches.batch_id = updates.batch_id::text
                "#,
            );
            builder
                .build()
                .execute(&self.pool)
                .await
                .context("failed to update throughput projection batch states")?;
        }

        Ok(())
    }

    fn push_throughput_projection_chunk_state_update_values<'args>(
        builder: &mut QueryBuilder<'args, Postgres>,
        updates: &'args [ThroughputProjectionChunkStateUpdate],
    ) {
        builder.push_values(updates, |mut row, update| {
            row.push_bind(&update.tenant_id)
                .push_bind(&update.instance_id)
                .push_bind(&update.run_id)
                .push_bind(&update.batch_id)
                .push_bind(&update.chunk_id)
                .push_bind(
                    i32::try_from(update.chunk_index)
                        .expect("projection chunk chunk_index exceeds i32"),
                )
                .push_bind(
                    i32::try_from(update.group_id).expect("projection chunk group_id exceeds i32"),
                )
                .push_bind(
                    i32::try_from(update.item_count)
                        .expect("projection chunk item_count exceeds i32"),
                )
                .push_bind(&update.status)
                .push_bind(
                    i32::try_from(update.attempt)
                        .expect("projection chunk state attempt exceeds i32"),
                )
                .push_bind(
                    i64::try_from(update.lease_epoch)
                        .expect("projection chunk state lease_epoch exceeds i64"),
                )
                .push_bind(
                    i64::try_from(update.owner_epoch)
                        .expect("projection chunk state owner_epoch exceeds i64"),
                )
                .push_bind(update.input_handle.is_some())
                .push_bind(update.input_handle.as_ref().map(Json))
                .push_bind(update.result_handle.is_some())
                .push_bind(update.result_handle.as_ref().map(Json))
                .push_bind(update.output.as_ref().map(Json))
                .push_bind(&update.error)
                .push_bind(update.cancellation_requested)
                .push_bind(&update.cancellation_reason)
                .push_bind(update.cancellation_metadata.as_ref().map(Json))
                .push_bind(&update.worker_id)
                .push_bind(&update.worker_build_id)
                .push_bind(update.lease_token)
                .push_bind(&update.last_report_id)
                .push_bind(update.available_at)
                .push_bind(update.started_at)
                .push_bind(update.lease_expires_at)
                .push_bind(update.completed_at)
                .push_bind(update.updated_at);
        });
    }

    pub async fn update_throughput_projection_chunk_states(
        &self,
        updates: &[ThroughputProjectionChunkStateUpdate],
    ) -> Result<()> {
        const CHUNK_STATE_BATCH_SIZE: usize = 250;

        if updates.is_empty() {
            return Ok(());
        }

        for batch in updates.chunks(CHUNK_STATE_BATCH_SIZE) {
            let mut tx =
                self.pool.begin().await.context(
                    "failed to begin throughput projection chunk state batch transaction",
                )?;

            let mut insert_builder = QueryBuilder::<Postgres>::new(
                r#"
                WITH updates (
                    tenant_id,
                    workflow_instance_id,
                    run_id,
                    batch_id,
                    chunk_id,
                    chunk_index,
                    group_id,
                    item_count,
                    status,
                    attempt,
                    lease_epoch,
                    owner_epoch,
                    input_handle_present,
                    input_handle,
                    result_handle_present,
                    result_handle,
                    output,
                    error,
                    cancellation_requested,
                    cancellation_reason,
                    cancellation_metadata,
                    worker_id,
                    worker_build_id,
                    lease_token,
                    last_report_id,
                    available_at,
                    started_at,
                    lease_expires_at,
                    completed_at,
                    updated_at
                ) AS (
                "#,
            );
            Self::push_throughput_projection_chunk_state_update_values(&mut insert_builder, batch);
            insert_builder.push(
                r#"
                )
                INSERT INTO throughput_projection_chunks (
                    tenant_id,
                    workflow_instance_id,
                    run_id,
                    workflow_id,
                    workflow_version,
                    artifact_hash,
                    batch_id,
                    chunk_id,
                    chunk_index,
                    group_id,
                    item_count,
                    activity_type,
                    task_queue,
                    state,
                    status,
                    attempt,
                    lease_epoch,
                    owner_epoch,
                    max_attempts,
                    retry_delay_ms,
                    input_handle,
                    result_handle,
                    items,
                    output,
                    error,
                    cancellation_requested,
                    cancellation_reason,
                    cancellation_metadata,
                    worker_id,
                    worker_build_id,
                    lease_token,
                    last_report_id,
                    scheduled_at,
                    available_at,
                    started_at,
                    lease_expires_at,
                    completed_at,
                    updated_at
                )
                SELECT
                    updates.tenant_id::text,
                    updates.workflow_instance_id::text,
                    updates.run_id::text,
                    batches.workflow_id,
                    batches.workflow_version,
                    batches.artifact_hash,
                    updates.batch_id::text,
                    updates.chunk_id::text,
                    updates.chunk_index::integer,
                    updates.group_id::integer,
                    updates.item_count::integer,
                    batches.activity_type,
                    batches.task_queue,
                    batches.state,
                    updates.status::text,
                    updates.attempt::integer,
                    updates.lease_epoch::bigint,
                    updates.owner_epoch::bigint,
                    batches.max_attempts,
                    batches.retry_delay_ms,
                    CASE
                        WHEN updates.input_handle_present::boolean
                            THEN updates.input_handle::jsonb
                        ELSE 'null'::jsonb
                    END,
                    CASE
                        WHEN updates.result_handle_present::boolean
                            THEN updates.result_handle::jsonb
                        ELSE 'null'::jsonb
                    END,
                    '[]'::jsonb,
                    updates.output::jsonb,
                    updates.error::text,
                    updates.cancellation_requested::boolean,
                    updates.cancellation_reason::text,
                    updates.cancellation_metadata::jsonb,
                    updates.worker_id::text,
                    updates.worker_build_id::text,
                    updates.lease_token::uuid,
                    updates.last_report_id::text,
                    batches.scheduled_at,
                    updates.available_at::timestamptz,
                    updates.started_at::timestamptz,
                    updates.lease_expires_at::timestamptz,
                    updates.completed_at::timestamptz,
                    updates.updated_at::timestamptz
                FROM updates
                JOIN throughput_projection_batches AS batches
                  ON batches.tenant_id = updates.tenant_id::text
                 AND batches.workflow_instance_id = updates.workflow_instance_id::text
                 AND batches.run_id = updates.run_id::text
                 AND batches.batch_id = updates.batch_id::text
                ON CONFLICT (tenant_id, workflow_instance_id, run_id, batch_id, chunk_id) DO NOTHING
                "#,
            );
            insert_builder
                .build()
                .execute(&mut *tx)
                .await
                .context("failed to insert missing throughput projection chunk states")?;

            let mut update_builder = QueryBuilder::<Postgres>::new(
                r#"
                WITH updates (
                    tenant_id,
                    workflow_instance_id,
                    run_id,
                    batch_id,
                    chunk_id,
                    chunk_index,
                    group_id,
                    item_count,
                    status,
                    attempt,
                    lease_epoch,
                    owner_epoch,
                    input_handle_present,
                    input_handle,
                    result_handle_present,
                    result_handle,
                    output,
                    error,
                    cancellation_requested,
                    cancellation_reason,
                    cancellation_metadata,
                    worker_id,
                    worker_build_id,
                    lease_token,
                    last_report_id,
                    available_at,
                    started_at,
                    lease_expires_at,
                    completed_at,
                    updated_at
                ) AS (
                "#,
            );
            Self::push_throughput_projection_chunk_state_update_values(&mut update_builder, batch);
            update_builder.push(
                r#"
                )
                UPDATE throughput_projection_chunks
                SET chunk_index = updates.chunk_index::integer,
                    group_id = updates.group_id::integer,
                    item_count = updates.item_count::integer,
                    status = CASE
                        WHEN throughput_projection_chunks.completed_at IS NOT NULL
                            AND updates.completed_at::timestamptz IS NULL
                            THEN throughput_projection_chunks.status
                        ELSE updates.status::text
                    END,
                    attempt = CASE
                        WHEN throughput_projection_chunks.completed_at IS NOT NULL
                            AND updates.completed_at::timestamptz IS NULL
                            THEN throughput_projection_chunks.attempt
                        ELSE updates.attempt::integer
                    END,
                    lease_epoch = CASE
                        WHEN throughput_projection_chunks.completed_at IS NOT NULL
                            AND updates.completed_at::timestamptz IS NULL
                            THEN throughput_projection_chunks.lease_epoch
                        ELSE updates.lease_epoch::bigint
                    END,
                    owner_epoch = CASE
                        WHEN throughput_projection_chunks.completed_at IS NOT NULL
                            AND updates.completed_at::timestamptz IS NULL
                            THEN throughput_projection_chunks.owner_epoch
                        ELSE updates.owner_epoch::bigint
                    END,
                    input_handle = CASE
                        WHEN updates.input_handle_present::boolean
                            THEN updates.input_handle::jsonb
                        ELSE throughput_projection_chunks.input_handle
                    END,
                    result_handle = CASE
                        WHEN updates.result_handle_present::boolean
                            THEN updates.result_handle::jsonb
                        ELSE throughput_projection_chunks.result_handle
                    END,
                    output = CASE
                        WHEN throughput_projection_chunks.completed_at IS NOT NULL
                            AND updates.completed_at::timestamptz IS NULL
                            THEN throughput_projection_chunks.output
                        ELSE updates.output::jsonb
                    END,
                    error = CASE
                        WHEN throughput_projection_chunks.completed_at IS NOT NULL
                            AND updates.completed_at::timestamptz IS NULL
                            THEN throughput_projection_chunks.error
                        ELSE updates.error::text
                    END,
                    cancellation_requested = CASE
                        WHEN throughput_projection_chunks.completed_at IS NOT NULL
                            AND updates.completed_at::timestamptz IS NULL
                            THEN throughput_projection_chunks.cancellation_requested
                        ELSE updates.cancellation_requested::boolean
                    END,
                    cancellation_reason = CASE
                        WHEN throughput_projection_chunks.completed_at IS NOT NULL
                            AND updates.completed_at::timestamptz IS NULL
                            THEN throughput_projection_chunks.cancellation_reason
                        ELSE updates.cancellation_reason::text
                    END,
                    cancellation_metadata = CASE
                        WHEN throughput_projection_chunks.completed_at IS NOT NULL
                            AND updates.completed_at::timestamptz IS NULL
                            THEN throughput_projection_chunks.cancellation_metadata
                        ELSE updates.cancellation_metadata::jsonb
                    END,
                    worker_id = CASE
                        WHEN throughput_projection_chunks.completed_at IS NOT NULL
                            AND updates.completed_at::timestamptz IS NULL
                            THEN throughput_projection_chunks.worker_id
                        ELSE updates.worker_id::text
                    END,
                    worker_build_id = CASE
                        WHEN throughput_projection_chunks.completed_at IS NOT NULL
                            AND updates.completed_at::timestamptz IS NULL
                            THEN throughput_projection_chunks.worker_build_id
                        ELSE updates.worker_build_id::text
                    END,
                    lease_token = CASE
                        WHEN throughput_projection_chunks.completed_at IS NOT NULL
                            AND updates.completed_at::timestamptz IS NULL
                            THEN throughput_projection_chunks.lease_token
                        ELSE updates.lease_token::uuid
                    END,
                    last_report_id = CASE
                        WHEN throughput_projection_chunks.completed_at IS NOT NULL
                            AND updates.completed_at::timestamptz IS NULL
                            THEN throughput_projection_chunks.last_report_id
                        ELSE updates.last_report_id::text
                    END,
                    available_at = CASE
                        WHEN throughput_projection_chunks.completed_at IS NOT NULL
                            AND updates.completed_at::timestamptz IS NULL
                            THEN throughput_projection_chunks.available_at
                        ELSE updates.available_at::timestamptz
                    END,
                    started_at = COALESCE(
                        updates.started_at::timestamptz,
                        throughput_projection_chunks.started_at
                    ),
                    lease_expires_at = CASE
                        WHEN throughput_projection_chunks.completed_at IS NOT NULL
                            AND updates.completed_at::timestamptz IS NULL
                            THEN throughput_projection_chunks.lease_expires_at
                        ELSE updates.lease_expires_at::timestamptz
                    END,
                    completed_at = COALESCE(
                        throughput_projection_chunks.completed_at,
                        updates.completed_at::timestamptz
                    ),
                    updated_at = GREATEST(
                        throughput_projection_chunks.updated_at,
                        updates.updated_at::timestamptz
                    )
                FROM updates
                WHERE throughput_projection_chunks.tenant_id = updates.tenant_id::text
                  AND throughput_projection_chunks.workflow_instance_id =
                      updates.workflow_instance_id::text
                  AND throughput_projection_chunks.run_id = updates.run_id::text
                  AND throughput_projection_chunks.batch_id = updates.batch_id::text
                  AND throughput_projection_chunks.chunk_id = updates.chunk_id::text
                "#,
            );
            update_builder
                .build()
                .execute(&mut *tx)
                .await
                .context("failed to update throughput projection chunk states")?;

            tx.commit()
                .await
                .context("failed to commit throughput projection chunk state batch transaction")?;
        }

        Ok(())
    }

    pub async fn update_throughput_projection_chunk_state(
        &self,
        update: &ThroughputProjectionChunkStateUpdate,
    ) -> Result<()> {
        self.update_throughput_projection_chunk_states(std::slice::from_ref(update)).await
    }

    pub async fn count_bulk_batches_for_run_query_view(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
    ) -> Result<u64> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM (
                SELECT batch_id
                FROM workflow_bulk_batches
                WHERE tenant_id = $1
                  AND workflow_instance_id = $2
                  AND run_id = $3
                  AND throughput_backend = 'pg-v1'
                UNION ALL
                SELECT batch_id
                FROM throughput_projection_batches
                WHERE tenant_id = $1
                  AND workflow_instance_id = $2
                  AND run_id = $3
            ) batches
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .fetch_one(&self.pool)
        .await
        .context("failed to count query bulk batches for run")?;
        Ok(count as u64)
    }

    pub async fn list_bulk_batches_for_run_page_query_view(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<WorkflowBulkBatchRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                workflow_version,
                artifact_hash,
                batch_id,
                activity_type,
                task_queue,
                state,
                input_handle,
                result_handle,
                throughput_backend,
                throughput_backend_version,
                execution_policy,
                reducer,
                reducer_class,
                aggregation_tree_depth,
                fast_lane_enabled,
                aggregation_group_count,
                status,
                total_items,
                chunk_size,
                chunk_count,
                succeeded_items,
                failed_items,
                cancelled_items,
                max_attempts,
                retry_delay_ms,
                error,
                scheduled_at,
                terminal_at,
                updated_at
            FROM (
                SELECT
                    tenant_id,
                    workflow_instance_id,
                    run_id,
                    workflow_id,
                    workflow_version,
                    artifact_hash,
                    batch_id,
                    activity_type,
                    task_queue,
                    state,
                    input_handle,
                    result_handle,
                    throughput_backend,
                    throughput_backend_version,
                    execution_policy,
                    reducer,
                    reducer_class,
                    aggregation_tree_depth,
                    fast_lane_enabled,
                    aggregation_group_count,
                    status,
                    total_items,
                    chunk_size,
                    chunk_count,
                    succeeded_items,
                    failed_items,
                    cancelled_items,
                    max_attempts,
                    retry_delay_ms,
                    error,
                    scheduled_at,
                    terminal_at,
                    updated_at
                FROM workflow_bulk_batches
                WHERE tenant_id = $1
                  AND workflow_instance_id = $2
                  AND run_id = $3
                  AND throughput_backend = 'pg-v1'
                UNION ALL
                SELECT
                    tenant_id,
                    workflow_instance_id,
                    run_id,
                    workflow_id,
                    workflow_version,
                    artifact_hash,
                    batch_id,
                    activity_type,
                    task_queue,
                    state,
                    input_handle,
                    result_handle,
                    throughput_backend,
                    throughput_backend_version,
                    execution_policy,
                    reducer,
                    reducer_class,
                    aggregation_tree_depth,
                    fast_lane_enabled,
                    aggregation_group_count,
                    status,
                    total_items,
                    chunk_size,
                    chunk_count,
                    succeeded_items,
                    failed_items,
                    cancelled_items,
                    max_attempts,
                    retry_delay_ms,
                    error,
                    scheduled_at,
                    terminal_at,
                    updated_at
                FROM throughput_projection_batches
                WHERE tenant_id = $1
                  AND workflow_instance_id = $2
                  AND run_id = $3
            ) batches
            ORDER BY scheduled_at ASC, batch_id ASC
            LIMIT $4 OFFSET $5
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .context("failed to list query bulk batches for run")?;
        rows.into_iter().map(Self::decode_bulk_batch_row).collect()
    }

    pub async fn get_bulk_batch_query_view(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        batch_id: &str,
    ) -> Result<Option<WorkflowBulkBatchRecord>> {
        let row = sqlx::query(
            r#"
            SELECT
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                workflow_version,
                artifact_hash,
                batch_id,
                activity_type,
                task_queue,
                state,
                input_handle,
                result_handle,
                throughput_backend,
                throughput_backend_version,
                execution_policy,
                reducer,
                reducer_class,
                aggregation_tree_depth,
                fast_lane_enabled,
                aggregation_group_count,
                status,
                total_items,
                chunk_size,
                chunk_count,
                succeeded_items,
                failed_items,
                cancelled_items,
                max_attempts,
                retry_delay_ms,
                error,
                scheduled_at,
                terminal_at,
                updated_at
            FROM (
                SELECT
                    tenant_id,
                    workflow_instance_id,
                    run_id,
                    workflow_id,
                    workflow_version,
                    artifact_hash,
                    batch_id,
                    activity_type,
                    task_queue,
                    state,
                    input_handle,
                    result_handle,
                    throughput_backend,
                    throughput_backend_version,
                    execution_policy,
                    reducer,
                    reducer_class,
                    aggregation_tree_depth,
                    fast_lane_enabled,
                    aggregation_group_count,
                    status,
                    total_items,
                    chunk_size,
                    chunk_count,
                    succeeded_items,
                    failed_items,
                    cancelled_items,
                    max_attempts,
                    retry_delay_ms,
                    error,
                    scheduled_at,
                    terminal_at,
                    updated_at
                FROM workflow_bulk_batches
                WHERE tenant_id = $1
                  AND workflow_instance_id = $2
                  AND run_id = $3
                  AND batch_id = $4
                  AND throughput_backend = 'pg-v1'
                UNION ALL
                SELECT
                    tenant_id,
                    workflow_instance_id,
                    run_id,
                    workflow_id,
                    workflow_version,
                    artifact_hash,
                    batch_id,
                    activity_type,
                    task_queue,
                    state,
                    input_handle,
                    result_handle,
                    throughput_backend,
                    throughput_backend_version,
                    execution_policy,
                    reducer,
                    reducer_class,
                    aggregation_tree_depth,
                    fast_lane_enabled,
                    aggregation_group_count,
                    status,
                    total_items,
                    chunk_size,
                    chunk_count,
                    succeeded_items,
                    failed_items,
                    cancelled_items,
                    max_attempts,
                    retry_delay_ms,
                    error,
                    scheduled_at,
                    terminal_at,
                    updated_at
                FROM throughput_projection_batches
                WHERE tenant_id = $1
                  AND workflow_instance_id = $2
                  AND run_id = $3
                  AND batch_id = $4
            ) batch
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(batch_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load query bulk batch")?;
        row.map(Self::decode_bulk_batch_row).transpose()
    }

    pub async fn get_bulk_chunk_query_view(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        batch_id: &str,
        chunk_id: &str,
    ) -> Result<Option<WorkflowBulkChunkRecord>> {
        let row = sqlx::query(
            r#"
            SELECT *
            FROM throughput_projection_chunks
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND batch_id = $4
              AND chunk_id = $5
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(batch_id)
        .bind(chunk_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load throughput projection chunk")?;
        row.map(Self::decode_bulk_chunk_row).transpose()
    }

    pub async fn list_nonterminal_throughput_projection_batches_for_run(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        throughput_backend: &str,
    ) -> Result<Vec<WorkflowBulkBatchRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT *
            FROM throughput_projection_batches
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND throughput_backend = $4
              AND status IN ('scheduled', 'running')
            ORDER BY scheduled_at ASC, batch_id ASC
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(throughput_backend)
        .fetch_all(&self.pool)
        .await
        .context("failed to list nonterminal throughput projection batches for run")?;
        rows.into_iter().map(Self::decode_bulk_batch_row).collect()
    }

    pub async fn list_nonterminal_throughput_projection_batches_for_backend(
        &self,
        throughput_backend: &str,
        limit: usize,
    ) -> Result<Vec<WorkflowBulkBatchRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT *
            FROM throughput_projection_batches
            WHERE throughput_backend = $1
              AND status IN ('scheduled', 'running')
            ORDER BY scheduled_at ASC, batch_id ASC
            LIMIT $2
            "#,
        )
        .bind(throughput_backend)
        .bind(i64::try_from(limit.max(1)).context("projection bootstrap limit exceeds i64")?)
        .fetch_all(&self.pool)
        .await
        .context("failed to list nonterminal throughput projection batches for backend")?;
        rows.into_iter().map(Self::decode_bulk_batch_row).collect()
    }

    pub async fn load_throughput_projection_offsets(
        &self,
        projector_id: &str,
    ) -> Result<std::collections::HashMap<i32, i64>> {
        let rows = sqlx::query_as::<_, (i32, i64)>(
            r#"
            SELECT partition_id, log_offset
            FROM throughput_projection_offsets
            WHERE projector_id = $1
            "#,
        )
        .bind(projector_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to load throughput projection offsets")?;
        Ok(rows.into_iter().collect())
    }

    pub async fn commit_throughput_projection_offset(
        &self,
        projector_id: &str,
        partition_id: i32,
        offset: i64,
        updated_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO throughput_projection_offsets (
                projector_id,
                partition_id,
                log_offset,
                updated_at
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (projector_id, partition_id)
            DO UPDATE SET
                log_offset = EXCLUDED.log_offset,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(projector_id)
        .bind(partition_id)
        .bind(offset)
        .bind(updated_at)
        .execute(&self.pool)
        .await
        .context("failed to commit throughput projection offset")?;
        Ok(())
    }

    pub async fn count_bulk_chunks_for_batch_query_view(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        batch_id: &str,
    ) -> Result<u64> {
        let batch = self
            .get_bulk_batch_query_view(tenant_id, instance_id, run_id, batch_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("bulk batch {batch_id} not found"))?;
        let count = if batch.throughput_backend == "stream-v2" {
            sqlx::query_scalar::<_, i64>(
                r#"
                SELECT COUNT(*)
                FROM throughput_projection_chunks
                WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3 AND batch_id = $4
                "#,
            )
            .bind(tenant_id)
            .bind(instance_id)
            .bind(run_id)
            .bind(batch_id)
            .fetch_one(&self.pool)
            .await
            .context("failed to count throughput projection chunks for batch")?
        } else {
            sqlx::query_scalar::<_, i64>(
                r#"
                SELECT COUNT(*)
                FROM workflow_bulk_chunks
                WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3 AND batch_id = $4
                "#,
            )
            .bind(tenant_id)
            .bind(instance_id)
            .bind(run_id)
            .bind(batch_id)
            .fetch_one(&self.pool)
            .await
            .context("failed to count workflow bulk chunks for batch")?
        };
        Ok(count as u64)
    }

    pub async fn list_bulk_chunks_for_batch_page_query_view(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        batch_id: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<WorkflowBulkChunkRecord>> {
        let batch = self
            .get_bulk_batch_query_view(tenant_id, instance_id, run_id, batch_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("bulk batch {batch_id} not found"))?;
        let rows = if batch.throughput_backend == "stream-v2" {
            sqlx::query(
                r#"
                SELECT *
                FROM throughput_projection_chunks
                WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3 AND batch_id = $4
                ORDER BY chunk_index ASC
                LIMIT $5 OFFSET $6
                "#,
            )
            .bind(tenant_id)
            .bind(instance_id)
            .bind(run_id)
            .bind(batch_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .context("failed to list throughput projection chunks for batch")?
        } else {
            sqlx::query(
                r#"
                SELECT *
                FROM workflow_bulk_chunks
                WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3 AND batch_id = $4
                ORDER BY chunk_index ASC
                LIMIT $5 OFFSET $6
                "#,
            )
            .bind(tenant_id)
            .bind(instance_id)
            .bind(run_id)
            .bind(batch_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .context("failed to list workflow bulk chunks for batch")?
        };
        rows.into_iter().map(Self::decode_bulk_chunk_row).collect()
    }

    pub async fn count_bulk_batches_for_run(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
    ) -> Result<u64> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM workflow_bulk_batches
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .fetch_one(&self.pool)
        .await
        .context("failed to count workflow bulk batches for run")?;
        Ok(count as u64)
    }

    pub async fn list_bulk_batches_for_run_page(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<WorkflowBulkBatchRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT *
            FROM workflow_bulk_batches
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            ORDER BY scheduled_at ASC, batch_id ASC
            LIMIT $4 OFFSET $5
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .context("failed to list workflow bulk batches for run")?;
        rows.into_iter().map(Self::decode_bulk_batch_row).collect()
    }

    pub async fn get_bulk_batch(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        batch_id: &str,
    ) -> Result<Option<WorkflowBulkBatchRecord>> {
        self.get_bulk_batch_with_executor(&self.pool, tenant_id, instance_id, run_id, batch_id)
            .await
    }

    pub async fn list_nonterminal_bulk_batches_for_run(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        throughput_backend: &str,
    ) -> Result<Vec<WorkflowBulkBatchRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT *
            FROM workflow_bulk_batches
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND throughput_backend = $4
              AND status IN ('scheduled', 'running')
            ORDER BY scheduled_at ASC, batch_id ASC
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(throughput_backend)
        .fetch_all(&self.pool)
        .await
        .context("failed to list nonterminal workflow bulk batches for run")?;
        rows.into_iter().map(Self::decode_bulk_batch_row).collect()
    }

    pub async fn list_nonterminal_bulk_batches_for_backend(
        &self,
        throughput_backend: &str,
        limit: usize,
    ) -> Result<Vec<WorkflowBulkBatchRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT *
            FROM workflow_bulk_batches
            WHERE throughput_backend = $1
              AND status IN ('scheduled', 'running')
            ORDER BY scheduled_at ASC, batch_id ASC
            LIMIT $2
            "#,
        )
        .bind(throughput_backend)
        .bind(i64::try_from(limit.max(1)).context("bulk batch bootstrap limit exceeds i64")?)
        .fetch_all(&self.pool)
        .await
        .context("failed to list nonterminal workflow bulk batches for backend")?;
        rows.into_iter().map(Self::decode_bulk_batch_row).collect()
    }

    pub async fn count_bulk_chunks_for_batch(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        batch_id: &str,
    ) -> Result<u64> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM workflow_bulk_chunks
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3 AND batch_id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(batch_id)
        .fetch_one(&self.pool)
        .await
        .context("failed to count workflow bulk chunks for batch")?;
        Ok(count as u64)
    }

    pub async fn get_bulk_chunk(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        batch_id: &str,
        chunk_id: &str,
    ) -> Result<Option<WorkflowBulkChunkRecord>> {
        self.get_bulk_chunk_with_executor(
            &self.pool,
            tenant_id,
            instance_id,
            run_id,
            batch_id,
            chunk_id,
        )
        .await
    }

    pub async fn list_bulk_chunks_for_batch_page(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        batch_id: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<WorkflowBulkChunkRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT *
            FROM workflow_bulk_chunks
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3 AND batch_id = $4
            ORDER BY chunk_index ASC
            LIMIT $5 OFFSET $6
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(batch_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .context("failed to list workflow bulk chunks for batch")?;
        rows.into_iter().map(Self::decode_bulk_chunk_row).collect()
    }

    pub async fn fail_activity(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        activity_id: &str,
        attempt: u32,
        status: WorkflowActivityStatus,
        error: &str,
        metadata: Option<&Value>,
        worker_id: &str,
        worker_build_id: &str,
        event_id: Uuid,
        event_type: &str,
        occurred_at: DateTime<Utc>,
    ) -> Result<bool> {
        let cancellation_reason =
            matches!(status, WorkflowActivityStatus::Cancelled).then_some(error);
        self.update_activity_terminal(
            tenant_id,
            instance_id,
            run_id,
            activity_id,
            attempt,
            status,
            None,
            Some(error),
            cancellation_reason,
            metadata,
            worker_id,
            worker_build_id,
            event_id,
            event_type,
            occurred_at,
        )
        .await
    }

    async fn update_activity_terminal(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        activity_id: &str,
        attempt: u32,
        status: WorkflowActivityStatus,
        output: Option<&Value>,
        error: Option<&str>,
        cancellation_reason: Option<&str>,
        cancellation_metadata: Option<&Value>,
        worker_id: &str,
        worker_build_id: &str,
        event_id: Uuid,
        event_type: &str,
        occurred_at: DateTime<Utc>,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_activities
            SET status = $6,
                output = $7,
                error = $8,
                cancellation_reason = $9,
                cancellation_metadata = $10,
                worker_id = $11,
                worker_build_id = $12,
                completed_at = $13,
                lease_expires_at = NULL,
                last_event_id = $14,
                last_event_type = $15,
                updated_at = $16
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND activity_id = $4
              AND attempt = $5
              AND status IN ('scheduled', 'started')
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(activity_id)
        .bind(i32::try_from(attempt).context("activity attempt exceeds i32")?)
        .bind(status.as_str())
        .bind(output.map(Json))
        .bind(error)
        .bind(cancellation_reason)
        .bind(cancellation_metadata.map(Json))
        .bind(worker_id)
        .bind(worker_build_id)
        .bind(occurred_at)
        .bind(event_id)
        .bind(event_type)
        .bind(occurred_at)
        .execute(&self.pool)
        .await
        .context("failed to update workflow activity terminal status")?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn list_activities_for_run(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
    ) -> Result<Vec<WorkflowActivityRecord>> {
        self.list_activities_for_run_page(tenant_id, instance_id, run_id, i64::MAX, 0).await
    }

    pub async fn list_activities_for_run_page(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<WorkflowActivityRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                workflow_version,
                artifact_hash,
                activity_id,
                attempt,
                activity_type,
                task_queue,
                state,
                status,
                input,
                config,
                output,
                error,
                cancellation_requested,
                cancellation_reason,
                cancellation_metadata,
                worker_id,
                worker_build_id,
                scheduled_at,
                started_at,
                last_heartbeat_at,
                lease_expires_at,
                completed_at,
                schedule_to_start_timeout_ms,
                start_to_close_timeout_ms,
                heartbeat_timeout_ms,
                last_event_id,
                last_event_type,
                updated_at
            FROM workflow_activities
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            ORDER BY scheduled_at ASC, activity_id ASC, attempt ASC
            LIMIT $4 OFFSET $5
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .context("failed to list workflow activities")?;

        rows.into_iter().map(Self::decode_activity_row).collect()
    }

    pub async fn count_activities_for_run(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
    ) -> Result<i64> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(*) AS count
            FROM workflow_activities
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .fetch_one(&self.pool)
        .await
        .context("failed to count workflow activities")?;
        row.try_get("count").context("activity count missing")
    }

    pub async fn list_runnable_activities(
        &self,
        limit: i64,
    ) -> Result<Vec<WorkflowActivityRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                workflow_version,
                artifact_hash,
                activity_id,
                attempt,
                activity_type,
                task_queue,
                state,
                status,
                input,
                config,
                output,
                error,
                cancellation_requested,
                cancellation_reason,
                cancellation_metadata,
                worker_id,
                worker_build_id,
                scheduled_at,
                started_at,
                last_heartbeat_at,
                lease_expires_at,
                completed_at,
                schedule_to_start_timeout_ms,
                start_to_close_timeout_ms,
                heartbeat_timeout_ms,
                last_event_id,
                last_event_type,
                updated_at
            FROM workflow_activities
            WHERE status = 'scheduled'
            ORDER BY scheduled_at ASC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("failed to list runnable workflow activities")?;

        rows.into_iter().map(Self::decode_activity_row).collect()
    }

    pub async fn list_started_activities(&self, limit: i64) -> Result<Vec<WorkflowActivityRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                workflow_version,
                artifact_hash,
                activity_id,
                attempt,
                activity_type,
                task_queue,
                state,
                status,
                input,
                config,
                output,
                error,
                cancellation_requested,
                cancellation_reason,
                cancellation_metadata,
                worker_id,
                worker_build_id,
                scheduled_at,
                started_at,
                last_heartbeat_at,
                lease_expires_at,
                completed_at,
                schedule_to_start_timeout_ms,
                start_to_close_timeout_ms,
                heartbeat_timeout_ms,
                last_event_id,
                last_event_type,
                updated_at
            FROM workflow_activities
            WHERE status = 'started'
            ORDER BY started_at ASC NULLS FIRST, scheduled_at ASC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("failed to list started workflow activities")?;

        rows.into_iter().map(Self::decode_activity_row).collect()
    }

    pub async fn get_activity_attempt(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        activity_id: &str,
        attempt: u32,
    ) -> Result<Option<WorkflowActivityRecord>> {
        let row = sqlx::query(
            r#"
            SELECT
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                workflow_version,
                artifact_hash,
                activity_id,
                attempt,
                activity_type,
                task_queue,
                state,
                status,
                input,
                config,
                output,
                error,
                cancellation_requested,
                cancellation_reason,
                cancellation_metadata,
                worker_id,
                worker_build_id,
                scheduled_at,
                started_at,
                last_heartbeat_at,
                lease_expires_at,
                completed_at,
                schedule_to_start_timeout_ms,
                start_to_close_timeout_ms,
                heartbeat_timeout_ms,
                last_event_id,
                last_event_type,
                updated_at
            FROM workflow_activities
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND activity_id = $4
              AND attempt = $5
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(activity_id)
        .bind(i32::try_from(attempt).context("activity attempt exceeds i32")?)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load workflow activity attempt")?;

        row.map(Self::decode_activity_row).transpose()
    }

    pub async fn get_latest_active_activity(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        activity_id: &str,
    ) -> Result<Option<WorkflowActivityRecord>> {
        let row = sqlx::query(
            r#"
            SELECT
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                workflow_version,
                artifact_hash,
                activity_id,
                attempt,
                activity_type,
                task_queue,
                state,
                status,
                input,
                config,
                output,
                error,
                cancellation_requested,
                cancellation_reason,
                cancellation_metadata,
                worker_id,
                worker_build_id,
                scheduled_at,
                started_at,
                last_heartbeat_at,
                lease_expires_at,
                completed_at,
                schedule_to_start_timeout_ms,
                start_to_close_timeout_ms,
                heartbeat_timeout_ms,
                last_event_id,
                last_event_type,
                updated_at
            FROM workflow_activities
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND activity_id = $4
              AND status IN ('scheduled', 'started')
            ORDER BY attempt DESC
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(activity_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load active workflow activity")?;

        row.map(Self::decode_activity_row).transpose()
    }

    fn decode_snapshot_row(row: sqlx::postgres::PgRow) -> Result<WorkflowStateSnapshot> {
        Ok(WorkflowStateSnapshot {
            tenant_id: row.try_get("tenant_id").context("snapshot tenant_id missing")?,
            instance_id: row
                .try_get("workflow_instance_id")
                .context("snapshot workflow_instance_id missing")?,
            run_id: row.try_get("run_id").context("snapshot run_id missing")?,
            definition_id: row.try_get("workflow_id").context("snapshot workflow_id missing")?,
            definition_version: row
                .try_get::<Option<i32>, _>("definition_version")
                .context("snapshot definition_version missing")?
                .map(|version| version as u32),
            artifact_hash: row
                .try_get("artifact_hash")
                .context("snapshot artifact_hash missing")?,
            snapshot_schema_version: row
                .try_get::<i32, _>("snapshot_schema_version")
                .context("snapshot schema version missing")?
                as u32,
            event_count: row.try_get("event_count").context("snapshot event_count missing")?,
            last_event_id: row
                .try_get("last_event_id")
                .context("snapshot last_event_id missing")?,
            last_event_type: row
                .try_get("last_event_type")
                .context("snapshot last_event_type missing")?,
            updated_at: row.try_get("updated_at").context("snapshot updated_at missing")?,
            state: row
                .try_get::<Json<WorkflowInstanceState>, _>("state")
                .map(|value| value.0)
                .context("snapshot state missing")?,
        })
    }

    fn decode_partition_ownership_row(
        row: sqlx::postgres::PgRow,
    ) -> Result<PartitionOwnershipRecord> {
        Ok(PartitionOwnershipRecord {
            partition_id: row.try_get("partition_id").context("ownership partition_id missing")?,
            owner_id: row.try_get("owner_id").context("ownership owner_id missing")?,
            owner_epoch: row
                .try_get::<i64, _>("owner_epoch")
                .context("ownership owner_epoch missing")? as u64,
            lease_expires_at: row
                .try_get("lease_expires_at")
                .context("ownership lease_expires_at missing")?,
            acquired_at: row.try_get("acquired_at").context("ownership acquired_at missing")?,
            last_transition_at: row
                .try_get("last_transition_at")
                .context("ownership last_transition_at missing")?,
            updated_at: row.try_get("updated_at").context("ownership updated_at missing")?,
        })
    }

    fn decode_executor_membership_row(
        row: sqlx::postgres::PgRow,
    ) -> Result<ExecutorMembershipRecord> {
        Ok(ExecutorMembershipRecord {
            executor_id: row
                .try_get("executor_id")
                .context("executor membership executor_id missing")?,
            query_endpoint: row
                .try_get("query_endpoint")
                .context("executor membership query_endpoint missing")?,
            advertised_capacity: row
                .try_get::<i32, _>("advertised_capacity")
                .context("executor membership advertised_capacity missing")?
                as usize,
            heartbeat_expires_at: row
                .try_get("heartbeat_expires_at")
                .context("executor membership heartbeat_expires_at missing")?,
            created_at: row
                .try_get("created_at")
                .context("executor membership created_at missing")?,
            updated_at: row
                .try_get("updated_at")
                .context("executor membership updated_at missing")?,
        })
    }

    fn decode_partition_assignment_row(
        row: sqlx::postgres::PgRow,
    ) -> Result<PartitionAssignmentRecord> {
        Ok(PartitionAssignmentRecord {
            partition_id: row
                .try_get("partition_id")
                .context("partition assignment partition_id missing")?,
            executor_id: row
                .try_get("executor_id")
                .context("partition assignment executor_id missing")?,
            assigned_at: row
                .try_get("assigned_at")
                .context("partition assignment assigned_at missing")?,
            updated_at: row
                .try_get("updated_at")
                .context("partition assignment updated_at missing")?,
        })
    }

    fn decode_run_row(row: sqlx::postgres::PgRow) -> Result<WorkflowRunRecord> {
        Ok(WorkflowRunRecord {
            tenant_id: row.try_get("tenant_id").context("run tenant_id missing")?,
            instance_id: row
                .try_get("workflow_instance_id")
                .context("run workflow_instance_id missing")?,
            run_id: row.try_get("run_id").context("run run_id missing")?,
            definition_id: row.try_get("workflow_id").context("run workflow_id missing")?,
            definition_version: row
                .try_get::<Option<i32>, _>("definition_version")
                .context("run definition_version missing")?
                .map(|version| version as u32),
            artifact_hash: row.try_get("artifact_hash").context("run artifact_hash missing")?,
            workflow_task_queue: row
                .try_get("workflow_task_queue")
                .context("run workflow_task_queue missing")?,
            sticky_workflow_build_id: row
                .try_get("sticky_workflow_build_id")
                .context("run sticky_workflow_build_id missing")?,
            sticky_workflow_poller_id: row
                .try_get("sticky_workflow_poller_id")
                .context("run sticky_workflow_poller_id missing")?,
            sticky_updated_at: row
                .try_get("sticky_updated_at")
                .context("run sticky_updated_at missing")?,
            previous_run_id: row
                .try_get("previous_run_id")
                .context("run previous_run_id missing")?,
            next_run_id: row.try_get("next_run_id").context("run next_run_id missing")?,
            continue_reason: row
                .try_get("continue_reason")
                .context("run continue_reason missing")?,
            trigger_event_id: row
                .try_get("trigger_event_id")
                .context("run trigger_event_id missing")?,
            continued_event_id: row
                .try_get("continued_event_id")
                .context("run continued_event_id missing")?,
            triggered_by_run_id: row
                .try_get("triggered_by_run_id")
                .context("run triggered_by_run_id missing")?,
            started_at: row.try_get("started_at").context("run started_at missing")?,
            closed_at: row.try_get("closed_at").context("run closed_at missing")?,
            updated_at: row.try_get("updated_at").context("run updated_at missing")?,
        })
    }

    fn decode_task_queue_build_row(row: sqlx::postgres::PgRow) -> Result<TaskQueueBuildRecord> {
        Ok(TaskQueueBuildRecord {
            tenant_id: row.try_get("tenant_id").context("task queue build tenant_id missing")?,
            queue_kind: TaskQueueKind::from_db(
                &row.try_get::<String, _>("queue_kind")
                    .context("task queue build queue_kind missing")?,
            )?,
            task_queue: row.try_get("task_queue").context("task queue build task_queue missing")?,
            build_id: row.try_get("build_id").context("task queue build build_id missing")?,
            artifact_hashes: row
                .try_get::<Json<Vec<String>>, _>("artifact_hashes")
                .context("task queue build artifact_hashes missing")?
                .0,
            metadata: row
                .try_get::<Option<Json<Value>>, _>("metadata")
                .context("task queue build metadata missing")?
                .map(|value| value.0),
            created_at: row.try_get("created_at").context("task queue build created_at missing")?,
            updated_at: row.try_get("updated_at").context("task queue build updated_at missing")?,
        })
    }

    fn decode_compatibility_set_row(
        row: sqlx::postgres::PgRow,
        build_ids: Vec<String>,
    ) -> Result<CompatibilitySetRecord> {
        Ok(CompatibilitySetRecord {
            tenant_id: row.try_get("tenant_id").context("compatibility set tenant_id missing")?,
            queue_kind: TaskQueueKind::from_db(
                &row.try_get::<String, _>("queue_kind")
                    .context("compatibility set queue_kind missing")?,
            )?,
            task_queue: row
                .try_get("task_queue")
                .context("compatibility set task_queue missing")?,
            set_id: row.try_get("set_id").context("compatibility set set_id missing")?,
            build_ids,
            is_default: row
                .try_get("is_default")
                .context("compatibility set is_default missing")?,
            created_at: row
                .try_get("created_at")
                .context("compatibility set created_at missing")?,
            updated_at: row
                .try_get("updated_at")
                .context("compatibility set updated_at missing")?,
        })
    }

    fn decode_queue_poller_row(row: sqlx::postgres::PgRow) -> Result<QueuePollerRecord> {
        Ok(QueuePollerRecord {
            tenant_id: row.try_get("tenant_id").context("queue poller tenant_id missing")?,
            queue_kind: TaskQueueKind::from_db(
                &row.try_get::<String, _>("queue_kind")
                    .context("queue poller queue_kind missing")?,
            )?,
            task_queue: row.try_get("task_queue").context("queue poller task_queue missing")?,
            poller_id: row.try_get("poller_id").context("queue poller poller_id missing")?,
            build_id: row.try_get("build_id").context("queue poller build_id missing")?,
            partition_id: row
                .try_get("partition_id")
                .context("queue poller partition_id missing")?,
            metadata: row
                .try_get::<Option<Json<Value>>, _>("metadata")
                .context("queue poller metadata missing")?
                .map(|value| value.0),
            last_seen_at: row
                .try_get("last_seen_at")
                .context("queue poller last_seen_at missing")?,
            expires_at: row.try_get("expires_at").context("queue poller expires_at missing")?,
            created_at: row.try_get("created_at").context("queue poller created_at missing")?,
            updated_at: row.try_get("updated_at").context("queue poller updated_at missing")?,
        })
    }

    fn decode_task_queue_throughput_policy_row(
        row: sqlx::postgres::PgRow,
    ) -> Result<TaskQueueThroughputPolicyRecord> {
        Ok(TaskQueueThroughputPolicyRecord {
            tenant_id: row
                .try_get("tenant_id")
                .context("task queue throughput policy tenant_id missing")?,
            queue_kind: TaskQueueKind::from_db(
                &row.try_get::<String, _>("queue_kind")
                    .context("task queue throughput policy queue_kind missing")?,
            )?,
            task_queue: row
                .try_get("task_queue")
                .context("task queue throughput policy task_queue missing")?,
            backend: row
                .try_get("backend")
                .context("task queue throughput policy backend missing")?,
            created_at: row
                .try_get("created_at")
                .context("task queue throughput policy created_at missing")?,
            updated_at: row
                .try_get("updated_at")
                .context("task queue throughput policy updated_at missing")?,
        })
    }

    fn decode_workflow_task_row(row: sqlx::postgres::PgRow) -> Result<WorkflowTaskRecord> {
        Ok(WorkflowTaskRecord {
            task_id: row.try_get("task_id").context("workflow task task_id missing")?,
            tenant_id: row.try_get("tenant_id").context("workflow task tenant_id missing")?,
            instance_id: row
                .try_get("workflow_instance_id")
                .context("workflow task workflow_instance_id missing")?,
            run_id: row.try_get("run_id").context("workflow task run_id missing")?,
            definition_id: row
                .try_get("workflow_id")
                .context("workflow task workflow_id missing")?,
            definition_version: row
                .try_get::<Option<i32>, _>("definition_version")
                .context("workflow task definition_version missing")?
                .map(|value| value as u32),
            artifact_hash: row
                .try_get("artifact_hash")
                .context("workflow task artifact_hash missing")?,
            partition_id: row
                .try_get("partition_id")
                .context("workflow task partition_id missing")?,
            task_queue: row.try_get("task_queue").context("workflow task task_queue missing")?,
            preferred_build_id: row
                .try_get("preferred_build_id")
                .context("workflow task preferred_build_id missing")?,
            status: WorkflowTaskStatus::from_db(
                &row.try_get::<String, _>("status").context("workflow task status missing")?,
            )?,
            source_event_id: row
                .try_get("source_event_id")
                .context("workflow task source_event_id missing")?,
            source_event_type: row
                .try_get("source_event_type")
                .context("workflow task source_event_type missing")?,
            source_event: row
                .try_get::<Json<EventEnvelope<WorkflowEvent>>, _>("source_event")
                .context("workflow task source_event missing")?
                .0,
            mailbox_consumed_seq: row
                .try_get::<i64, _>("mailbox_consumed_seq")
                .context("workflow task mailbox_consumed_seq missing")?
                as u64,
            resume_consumed_seq: row
                .try_get::<i64, _>("resume_consumed_seq")
                .context("workflow task resume_consumed_seq missing")?
                as u64,
            mailbox_high_watermark: row
                .try_get::<i64, _>("mailbox_high_watermark")
                .context("workflow task mailbox_high_watermark missing")?
                as u64,
            resume_high_watermark: row
                .try_get::<i64, _>("resume_high_watermark")
                .context("workflow task resume_high_watermark missing")?
                as u64,
            mailbox_backlog: row
                .try_get::<i64, _>("mailbox_backlog")
                .context("workflow task mailbox_backlog missing")?
                as u64,
            resume_backlog: row
                .try_get::<i64, _>("resume_backlog")
                .context("workflow task resume_backlog missing")?
                as u64,
            lease_poller_id: row
                .try_get("lease_poller_id")
                .context("workflow task lease_poller_id missing")?,
            lease_build_id: row
                .try_get("lease_build_id")
                .context("workflow task lease_build_id missing")?,
            lease_expires_at: row
                .try_get("lease_expires_at")
                .context("workflow task lease_expires_at missing")?,
            attempt_count: row
                .try_get::<i32, _>("attempt_count")
                .context("workflow task attempt_count missing")? as u32,
            last_error: row.try_get("last_error").context("workflow task last_error missing")?,
            created_at: row.try_get("created_at").context("workflow task created_at missing")?,
            updated_at: row.try_get("updated_at").context("workflow task updated_at missing")?,
        })
    }

    fn decode_workflow_mailbox_row(row: sqlx::postgres::PgRow) -> Result<WorkflowMailboxRecord> {
        Ok(WorkflowMailboxRecord {
            tenant_id: row.try_get("tenant_id").context("workflow mailbox tenant_id missing")?,
            instance_id: row
                .try_get("workflow_instance_id")
                .context("workflow mailbox workflow_instance_id missing")?,
            run_id: row.try_get("run_id").context("workflow mailbox run_id missing")?,
            accepted_seq: row
                .try_get::<i64, _>("accepted_seq")
                .context("workflow mailbox accepted_seq missing")? as u64,
            kind: WorkflowMailboxKind::from_db(
                &row.try_get::<String, _>("kind").context("workflow mailbox kind missing")?,
            )?,
            message_id: row.try_get("message_id").context("workflow mailbox message_id missing")?,
            message_name: row
                .try_get("message_name")
                .context("workflow mailbox message_name missing")?,
            payload: row
                .try_get::<Option<Json<Value>>, _>("payload")
                .context("workflow mailbox payload missing")?
                .map(|value| value.0),
            source_event_id: row
                .try_get("source_event_id")
                .context("workflow mailbox source_event_id missing")?,
            source_event_type: row
                .try_get("source_event_type")
                .context("workflow mailbox source_event_type missing")?,
            source_event: row
                .try_get::<Json<EventEnvelope<WorkflowEvent>>, _>("source_event")
                .context("workflow mailbox source_event missing")?
                .0,
            status: WorkflowMailboxStatus::from_db(
                &row.try_get::<String, _>("status").context("workflow mailbox status missing")?,
            )?,
            consumed_at: row
                .try_get("consumed_at")
                .context("workflow mailbox consumed_at missing")?,
            created_at: row.try_get("created_at").context("workflow mailbox created_at missing")?,
            updated_at: row.try_get("updated_at").context("workflow mailbox updated_at missing")?,
        })
    }

    fn decode_workflow_resume_row(row: sqlx::postgres::PgRow) -> Result<WorkflowResumeRecord> {
        Ok(WorkflowResumeRecord {
            tenant_id: row.try_get("tenant_id").context("workflow resume tenant_id missing")?,
            instance_id: row
                .try_get("workflow_instance_id")
                .context("workflow resume workflow_instance_id missing")?,
            run_id: row.try_get("run_id").context("workflow resume run_id missing")?,
            resume_seq: row
                .try_get::<i64, _>("resume_seq")
                .context("workflow resume resume_seq missing")? as u64,
            kind: WorkflowResumeKind::from_db(
                &row.try_get::<String, _>("kind").context("workflow resume kind missing")?,
            )?,
            ref_id: row.try_get("ref_id").context("workflow resume ref_id missing")?,
            status_label: row
                .try_get("status_label")
                .context("workflow resume status_label missing")?,
            source_event_id: row
                .try_get("source_event_id")
                .context("workflow resume source_event_id missing")?,
            source_event_type: row
                .try_get("source_event_type")
                .context("workflow resume source_event_type missing")?,
            source_event: row
                .try_get::<Json<EventEnvelope<WorkflowEvent>>, _>("source_event")
                .context("workflow resume source_event missing")?
                .0,
            status: WorkflowResumeStatus::from_db(
                &row.try_get::<String, _>("status").context("workflow resume status missing")?,
            )?,
            consumed_at: row
                .try_get("consumed_at")
                .context("workflow resume consumed_at missing")?,
            created_at: row.try_get("created_at").context("workflow resume created_at missing")?,
            updated_at: row.try_get("updated_at").context("workflow resume updated_at missing")?,
        })
    }

    fn decode_signal_row(row: sqlx::postgres::PgRow) -> Result<WorkflowSignalRecord> {
        Ok(WorkflowSignalRecord {
            tenant_id: row.try_get("tenant_id").context("signal tenant_id missing")?,
            instance_id: row
                .try_get("workflow_instance_id")
                .context("signal workflow_instance_id missing")?,
            run_id: row.try_get("run_id").context("signal run_id missing")?,
            signal_id: row.try_get("signal_id").context("signal signal_id missing")?,
            signal_type: row.try_get("signal_type").context("signal signal_type missing")?,
            dedupe_key: row.try_get("dedupe_key").context("signal dedupe_key missing")?,
            payload: row
                .try_get::<Json<Value>, _>("payload")
                .map(|value| value.0)
                .context("signal payload missing")?,
            status: WorkflowSignalStatus::from_db(
                &row.try_get::<String, _>("status").context("signal status missing")?,
            )?,
            source_event_id: row
                .try_get("source_event_id")
                .context("signal source_event_id missing")?,
            dispatch_event_id: row
                .try_get("dispatch_event_id")
                .context("signal dispatch_event_id missing")?,
            consumed_event_id: row
                .try_get("consumed_event_id")
                .context("signal consumed_event_id missing")?,
            enqueued_at: row.try_get("enqueued_at").context("signal enqueued_at missing")?,
            consumed_at: row.try_get("consumed_at").context("signal consumed_at missing")?,
            updated_at: row.try_get("updated_at").context("signal updated_at missing")?,
        })
    }

    fn decode_update_row(row: sqlx::postgres::PgRow) -> Result<WorkflowUpdateRecord> {
        Ok(WorkflowUpdateRecord {
            tenant_id: row.try_get("tenant_id").context("update tenant_id missing")?,
            instance_id: row
                .try_get("workflow_instance_id")
                .context("update workflow_instance_id missing")?,
            run_id: row.try_get("run_id").context("update run_id missing")?,
            update_id: row.try_get("update_id").context("update update_id missing")?,
            update_name: row.try_get("update_name").context("update update_name missing")?,
            request_id: row.try_get("request_id").context("update request_id missing")?,
            payload: row
                .try_get::<Json<Value>, _>("payload")
                .map(|value| value.0)
                .context("update payload missing")?,
            status: WorkflowUpdateStatus::from_db(
                &row.try_get::<String, _>("status").context("update status missing")?,
            )?,
            output: row
                .try_get::<Option<Json<Value>>, _>("output")
                .map(|value: Option<Json<Value>>| value.map(|json| json.0))
                .context("update output missing")?,
            error: row.try_get("error").context("update error missing")?,
            source_event_id: row
                .try_get("source_event_id")
                .context("update source_event_id missing")?,
            accepted_event_id: row
                .try_get("accepted_event_id")
                .context("update accepted_event_id missing")?,
            completed_event_id: row
                .try_get("completed_event_id")
                .context("update completed_event_id missing")?,
            requested_at: row.try_get("requested_at").context("update requested_at missing")?,
            accepted_at: row.try_get("accepted_at").context("update accepted_at missing")?,
            completed_at: row.try_get("completed_at").context("update completed_at missing")?,
            updated_at: row.try_get("updated_at").context("update updated_at missing")?,
        })
    }

    fn decode_child_row(row: sqlx::postgres::PgRow) -> Result<WorkflowChildRecord> {
        Ok(WorkflowChildRecord {
            tenant_id: row.try_get("tenant_id").context("child tenant_id missing")?,
            instance_id: row
                .try_get("workflow_instance_id")
                .context("child workflow_instance_id missing")?,
            run_id: row.try_get("run_id").context("child run_id missing")?,
            child_id: row.try_get("child_id").context("child child_id missing")?,
            child_workflow_id: row
                .try_get("child_workflow_id")
                .context("child child_workflow_id missing")?,
            child_definition_id: row
                .try_get("child_definition_id")
                .context("child child_definition_id missing")?,
            child_run_id: row.try_get("child_run_id").context("child child_run_id missing")?,
            parent_close_policy: row
                .try_get("parent_close_policy")
                .context("child parent_close_policy missing")?,
            input: row
                .try_get::<Json<Value>, _>("input")
                .map(|value| value.0)
                .context("child input missing")?,
            status: row.try_get("status").context("child status missing")?,
            output: row
                .try_get::<Option<Json<Value>>, _>("output")
                .map(|value: Option<Json<Value>>| value.map(|json| json.0))
                .context("child output missing")?,
            error: row.try_get("error").context("child error missing")?,
            source_event_id: row
                .try_get("source_event_id")
                .context("child source_event_id missing")?,
            started_event_id: row
                .try_get("started_event_id")
                .context("child started_event_id missing")?,
            terminal_event_id: row
                .try_get("terminal_event_id")
                .context("child terminal_event_id missing")?,
            started_at: row.try_get("started_at").context("child started_at missing")?,
            completed_at: row.try_get("completed_at").context("child completed_at missing")?,
            updated_at: row.try_get("updated_at").context("child updated_at missing")?,
        })
    }

    fn decode_request_dedup_row(row: sqlx::postgres::PgRow) -> Result<RequestDedupRecord> {
        Ok(RequestDedupRecord {
            tenant_id: row.try_get("tenant_id").context("request tenant_id missing")?,
            instance_id: row
                .try_get("workflow_instance_id")
                .context("request workflow_instance_id missing")?,
            operation: row.try_get("operation").context("request operation missing")?,
            request_id: row.try_get("request_id").context("request request_id missing")?,
            request_hash: row.try_get("request_hash").context("request request_hash missing")?,
            response: row
                .try_get::<Json<Value>, _>("response")
                .map(|value| value.0)
                .context("request response missing")?,
            created_at: row.try_get("created_at").context("request created_at missing")?,
            updated_at: row.try_get("updated_at").context("request updated_at missing")?,
        })
    }

    async fn get_bulk_batch_with_executor<'e, E>(
        &self,
        executor: E,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        batch_id: &str,
    ) -> Result<Option<WorkflowBulkBatchRecord>>
    where
        E: sqlx::Executor<'e, Database = Postgres>,
    {
        let row = sqlx::query(
            r#"
            SELECT *
            FROM workflow_bulk_batches
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND batch_id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(batch_id)
        .fetch_optional(executor)
        .await
        .context("failed to load workflow bulk batch")?;
        row.map(Self::decode_bulk_batch_row).transpose()
    }

    async fn get_bulk_chunk_with_executor<'e, E>(
        &self,
        executor: E,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        batch_id: &str,
        chunk_id: &str,
    ) -> Result<Option<WorkflowBulkChunkRecord>>
    where
        E: sqlx::Executor<'e, Database = Postgres>,
    {
        let row = sqlx::query(
            r#"
            SELECT *
            FROM workflow_bulk_chunks
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND batch_id = $4
              AND chunk_id = $5
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(batch_id)
        .bind(chunk_id)
        .fetch_optional(executor)
        .await
        .context("failed to load workflow bulk chunk")?;
        row.map(Self::decode_bulk_chunk_row).transpose()
    }

    async fn enqueue_workflow_event_outbox<'e, E>(
        &self,
        executor: E,
        event: &EventEnvelope<WorkflowEvent>,
        available_at: DateTime<Utc>,
    ) -> Result<()>
    where
        E: sqlx::Executor<'e, Database = Postgres>,
    {
        sqlx::query(
            r#"
            INSERT INTO workflow_event_outbox (
                outbox_id,
                tenant_id,
                workflow_instance_id,
                run_id,
                event_type,
                partition_key,
                event,
                available_at,
                created_at,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $8)
            ON CONFLICT (outbox_id) DO NOTHING
            "#,
        )
        .bind(event.event_id)
        .bind(&event.tenant_id)
        .bind(&event.instance_id)
        .bind(&event.run_id)
        .bind(&event.event_type)
        .bind(&event.partition_key)
        .bind(Json(event))
        .bind(available_at)
        .execute(executor)
        .await
        .context("failed to insert workflow event outbox row")?;
        Ok(())
    }

    fn bulk_terminal_event(
        batch: &WorkflowBulkBatchRecord,
        payload: WorkflowEvent,
    ) -> EventEnvelope<WorkflowEvent> {
        let mut envelope = EventEnvelope::new(
            payload.event_type(),
            fabrik_events::WorkflowIdentity::new(
                batch.tenant_id.clone(),
                batch.definition_id.clone(),
                batch.definition_version.unwrap_or_default(),
                batch.artifact_hash.clone().unwrap_or_default(),
                batch.instance_id.clone(),
                batch.run_id.clone(),
                if batch.throughput_backend == "stream-v2" {
                    "throughput-runtime"
                } else {
                    "matching-service"
                },
            ),
            payload,
        );
        if let Some(state) = &batch.state {
            envelope.metadata.insert("state".to_owned(), state.clone());
        }
        envelope
    }

    fn decode_bulk_batch_row(row: sqlx::postgres::PgRow) -> Result<WorkflowBulkBatchRecord> {
        Ok(WorkflowBulkBatchRecord {
            tenant_id: row.try_get("tenant_id").context("bulk batch tenant_id missing")?,
            instance_id: row
                .try_get("workflow_instance_id")
                .context("bulk batch workflow_instance_id missing")?,
            run_id: row.try_get("run_id").context("bulk batch run_id missing")?,
            definition_id: row.try_get("workflow_id").context("bulk batch workflow_id missing")?,
            definition_version: row
                .try_get::<Option<i32>, _>("workflow_version")
                .context("bulk batch workflow_version missing")?
                .map(|value| value as u32),
            artifact_hash: row
                .try_get("artifact_hash")
                .context("bulk batch artifact_hash missing")?,
            batch_id: row.try_get("batch_id").context("bulk batch batch_id missing")?,
            activity_type: row
                .try_get("activity_type")
                .context("bulk batch activity_type missing")?,
            task_queue: row.try_get("task_queue").context("bulk batch task_queue missing")?,
            state: row.try_get("state").context("bulk batch state missing")?,
            input_handle: row
                .try_get::<Json<Value>, _>("input_handle")
                .map(|value| value.0)
                .context("bulk batch input_handle missing")?,
            result_handle: row
                .try_get::<Json<Value>, _>("result_handle")
                .map(|value| value.0)
                .context("bulk batch result_handle missing")?,
            throughput_backend: row
                .try_get("throughput_backend")
                .context("bulk batch throughput_backend missing")?,
            throughput_backend_version: row
                .try_get("throughput_backend_version")
                .context("bulk batch throughput_backend_version missing")?,
            execution_policy: row
                .try_get("execution_policy")
                .context("bulk batch execution_policy missing")?,
            reducer: row.try_get("reducer").context("bulk batch reducer missing")?,
            reducer_class: row
                .try_get("reducer_class")
                .context("bulk batch reducer_class missing")?,
            aggregation_tree_depth: row
                .try_get::<i32, _>("aggregation_tree_depth")
                .context("bulk batch aggregation_tree_depth missing")?
                as u32,
            fast_lane_enabled: row
                .try_get("fast_lane_enabled")
                .context("bulk batch fast_lane_enabled missing")?,
            aggregation_group_count: row
                .try_get::<i32, _>("aggregation_group_count")
                .context("bulk batch aggregation_group_count missing")?
                as u32,
            status: WorkflowBulkBatchStatus::from_db(
                &row.try_get::<String, _>("status").context("bulk batch status missing")?,
            )?,
            total_items: row
                .try_get::<i32, _>("total_items")
                .context("bulk batch total_items missing")? as u32,
            chunk_size: row
                .try_get::<i32, _>("chunk_size")
                .context("bulk batch chunk_size missing")? as u32,
            chunk_count: row
                .try_get::<i32, _>("chunk_count")
                .context("bulk batch chunk_count missing")? as u32,
            succeeded_items: row
                .try_get::<i32, _>("succeeded_items")
                .context("bulk batch succeeded_items missing")? as u32,
            failed_items: row
                .try_get::<i32, _>("failed_items")
                .context("bulk batch failed_items missing")? as u32,
            cancelled_items: row
                .try_get::<i32, _>("cancelled_items")
                .context("bulk batch cancelled_items missing")? as u32,
            max_attempts: row
                .try_get::<i32, _>("max_attempts")
                .context("bulk batch max_attempts missing")? as u32,
            retry_delay_ms: row
                .try_get::<i64, _>("retry_delay_ms")
                .context("bulk batch retry_delay_ms missing")? as u64,
            error: row.try_get("error").context("bulk batch error missing")?,
            scheduled_at: row.try_get("scheduled_at").context("bulk batch scheduled_at missing")?,
            terminal_at: row.try_get("terminal_at").context("bulk batch terminal_at missing")?,
            updated_at: row.try_get("updated_at").context("bulk batch updated_at missing")?,
        })
    }

    fn decode_bulk_chunk_row(row: sqlx::postgres::PgRow) -> Result<WorkflowBulkChunkRecord> {
        Ok(WorkflowBulkChunkRecord {
            tenant_id: row.try_get("tenant_id").context("bulk chunk tenant_id missing")?,
            instance_id: row
                .try_get("workflow_instance_id")
                .context("bulk chunk workflow_instance_id missing")?,
            run_id: row.try_get("run_id").context("bulk chunk run_id missing")?,
            definition_id: row.try_get("workflow_id").context("bulk chunk workflow_id missing")?,
            definition_version: row
                .try_get::<Option<i32>, _>("workflow_version")
                .context("bulk chunk workflow_version missing")?
                .map(|value| value as u32),
            artifact_hash: row
                .try_get("artifact_hash")
                .context("bulk chunk artifact_hash missing")?,
            batch_id: row.try_get("batch_id").context("bulk chunk batch_id missing")?,
            chunk_id: row.try_get("chunk_id").context("bulk chunk chunk_id missing")?,
            chunk_index: row
                .try_get::<i32, _>("chunk_index")
                .context("bulk chunk chunk_index missing")? as u32,
            group_id: row.try_get::<i32, _>("group_id").context("bulk chunk group_id missing")?
                as u32,
            item_count: row
                .try_get::<i32, _>("item_count")
                .context("bulk chunk item_count missing")? as u32,
            activity_type: row
                .try_get("activity_type")
                .context("bulk chunk activity_type missing")?,
            task_queue: row.try_get("task_queue").context("bulk chunk task_queue missing")?,
            state: row.try_get("state").context("bulk chunk state missing")?,
            status: WorkflowBulkChunkStatus::from_db(
                &row.try_get::<String, _>("status").context("bulk chunk status missing")?,
            )?,
            attempt: row.try_get::<i32, _>("attempt").context("bulk chunk attempt missing")? as u32,
            lease_epoch: row
                .try_get::<i64, _>("lease_epoch")
                .context("bulk chunk lease_epoch missing")? as u64,
            owner_epoch: row
                .try_get::<i64, _>("owner_epoch")
                .context("bulk chunk owner_epoch missing")? as u64,
            max_attempts: row
                .try_get::<i32, _>("max_attempts")
                .context("bulk chunk max_attempts missing")? as u32,
            retry_delay_ms: row
                .try_get::<i64, _>("retry_delay_ms")
                .context("bulk chunk retry_delay_ms missing")? as u64,
            input_handle: row
                .try_get::<Json<Value>, _>("input_handle")
                .map(|value| value.0)
                .context("bulk chunk input_handle missing")?,
            result_handle: row
                .try_get::<Json<Value>, _>("result_handle")
                .map(|value| value.0)
                .context("bulk chunk result_handle missing")?,
            items: row
                .try_get::<Json<Vec<Value>>, _>("items")
                .map(|value| value.0)
                .context("bulk chunk items missing")?,
            output: row
                .try_get::<Option<Json<Vec<Value>>>, _>("output")
                .map(|value: Option<Json<Vec<Value>>>| value.map(|json| json.0))
                .context("bulk chunk output missing")?,
            error: row.try_get("error").context("bulk chunk error missing")?,
            cancellation_requested: row
                .try_get("cancellation_requested")
                .context("bulk chunk cancellation_requested missing")?,
            cancellation_reason: row
                .try_get("cancellation_reason")
                .context("bulk chunk cancellation_reason missing")?,
            cancellation_metadata: row
                .try_get::<Option<Json<Value>>, _>("cancellation_metadata")
                .map(|value: Option<Json<Value>>| value.map(|json| json.0))
                .context("bulk chunk cancellation_metadata missing")?,
            worker_id: row.try_get("worker_id").context("bulk chunk worker_id missing")?,
            worker_build_id: row
                .try_get("worker_build_id")
                .context("bulk chunk worker_build_id missing")?,
            lease_token: row.try_get("lease_token").context("bulk chunk lease_token missing")?,
            last_report_id: row
                .try_get("last_report_id")
                .context("bulk chunk last_report_id missing")?,
            scheduled_at: row.try_get("scheduled_at").context("bulk chunk scheduled_at missing")?,
            available_at: row.try_get("available_at").context("bulk chunk available_at missing")?,
            started_at: row.try_get("started_at").context("bulk chunk started_at missing")?,
            lease_expires_at: row
                .try_get("lease_expires_at")
                .context("bulk chunk lease_expires_at missing")?,
            completed_at: row.try_get("completed_at").context("bulk chunk completed_at missing")?,
            updated_at: row.try_get("updated_at").context("bulk chunk updated_at missing")?,
        })
    }

    fn decode_workflow_event_outbox_row(
        row: sqlx::postgres::PgRow,
    ) -> Result<WorkflowEventOutboxRecord> {
        Ok(WorkflowEventOutboxRecord {
            outbox_id: row.try_get("outbox_id").context("workflow event outbox_id missing")?,
            tenant_id: row.try_get("tenant_id").context("workflow event tenant_id missing")?,
            instance_id: row
                .try_get("workflow_instance_id")
                .context("workflow event workflow_instance_id missing")?,
            run_id: row.try_get("run_id").context("workflow event run_id missing")?,
            event_type: row.try_get("event_type").context("workflow event event_type missing")?,
            partition_key: row
                .try_get("partition_key")
                .context("workflow event partition_key missing")?,
            event: row
                .try_get::<Json<EventEnvelope<WorkflowEvent>>, _>("event")
                .context("workflow event payload missing")?
                .0,
            available_at: row
                .try_get("available_at")
                .context("workflow event available_at missing")?,
            leased_by: row.try_get("leased_by").context("workflow event leased_by missing")?,
            lease_expires_at: row
                .try_get("lease_expires_at")
                .context("workflow event lease_expires_at missing")?,
            published_at: row
                .try_get("published_at")
                .context("workflow event published_at missing")?,
            created_at: row.try_get("created_at").context("workflow event created_at missing")?,
            updated_at: row.try_get("updated_at").context("workflow event updated_at missing")?,
        })
    }

    fn decode_activity_row(row: sqlx::postgres::PgRow) -> Result<WorkflowActivityRecord> {
        Ok(WorkflowActivityRecord {
            tenant_id: row.try_get("tenant_id").context("activity tenant_id missing")?,
            instance_id: row
                .try_get("workflow_instance_id")
                .context("activity workflow_instance_id missing")?,
            run_id: row.try_get("run_id").context("activity run_id missing")?,
            definition_id: row.try_get("workflow_id").context("activity workflow_id missing")?,
            definition_version: row
                .try_get::<Option<i32>, _>("workflow_version")
                .context("activity workflow_version missing")?
                .map(|version| version as u32),
            artifact_hash: row
                .try_get("artifact_hash")
                .context("activity artifact_hash missing")?,
            activity_id: row.try_get("activity_id").context("activity activity_id missing")?,
            attempt: row.try_get::<i32, _>("attempt").context("activity attempt missing")? as u32,
            activity_type: row
                .try_get("activity_type")
                .context("activity activity_type missing")?,
            task_queue: row.try_get("task_queue").context("activity task_queue missing")?,
            state: row.try_get("state").context("activity state missing")?,
            status: WorkflowActivityStatus::from_db(
                &row.try_get::<String, _>("status").context("activity status missing")?,
            )?,
            input: row
                .try_get::<Json<Value>, _>("input")
                .map(|value| value.0)
                .context("activity input missing")?,
            config: row
                .try_get::<Option<Json<Value>>, _>("config")
                .map(|value: Option<Json<Value>>| value.map(|json| json.0))
                .context("activity config missing")?,
            output: row
                .try_get::<Option<Json<Value>>, _>("output")
                .map(|value: Option<Json<Value>>| value.map(|json| json.0))
                .context("activity output missing")?,
            error: row.try_get("error").context("activity error missing")?,
            cancellation_requested: row
                .try_get("cancellation_requested")
                .context("activity cancellation_requested missing")?,
            cancellation_reason: row
                .try_get("cancellation_reason")
                .context("activity cancellation_reason missing")?,
            cancellation_metadata: row
                .try_get::<Option<Json<Value>>, _>("cancellation_metadata")
                .map(|value: Option<Json<Value>>| value.map(|json| json.0))
                .context("activity cancellation_metadata missing")?,
            worker_id: row.try_get("worker_id").context("activity worker_id missing")?,
            worker_build_id: row
                .try_get("worker_build_id")
                .context("activity worker_build_id missing")?,
            scheduled_at: row.try_get("scheduled_at").context("activity scheduled_at missing")?,
            started_at: row.try_get("started_at").context("activity started_at missing")?,
            last_heartbeat_at: row
                .try_get("last_heartbeat_at")
                .context("activity last_heartbeat_at missing")?,
            lease_expires_at: row
                .try_get("lease_expires_at")
                .context("activity lease_expires_at missing")?,
            completed_at: row.try_get("completed_at").context("activity completed_at missing")?,
            schedule_to_start_timeout_ms: row
                .try_get::<Option<i64>, _>("schedule_to_start_timeout_ms")
                .context("activity schedule_to_start_timeout_ms missing")?
                .map(|value| value as u64),
            start_to_close_timeout_ms: row
                .try_get::<Option<i64>, _>("start_to_close_timeout_ms")
                .context("activity start_to_close_timeout_ms missing")?
                .map(|value| value as u64),
            heartbeat_timeout_ms: row
                .try_get::<Option<i64>, _>("heartbeat_timeout_ms")
                .context("activity heartbeat_timeout_ms missing")?
                .map(|value| value as u64),
            last_event_id: row
                .try_get("last_event_id")
                .context("activity last_event_id missing")?,
            last_event_type: row
                .try_get("last_event_type")
                .context("activity last_event_type missing")?,
            updated_at: row.try_get("updated_at").context("activity updated_at missing")?,
        })
    }
}

fn plan_partition_assignments(
    partition_count: i32,
    members: &[ExecutorMembershipRecord],
) -> Vec<(i32, String)> {
    if members.is_empty() {
        return Vec::new();
    }

    let mut loads = vec![0usize; members.len()];
    let capacities =
        members.iter().map(|member| member.advertised_capacity.max(1)).collect::<Vec<_>>();
    let mut assignments = Vec::with_capacity(partition_count.max(0) as usize);

    for partition_id in 0..partition_count {
        let mut best_index = 0usize;
        let mut best_ratio = f64::INFINITY;
        for (index, capacity) in capacities.iter().enumerate() {
            let ratio = loads[index] as f64 / *capacity as f64;
            if ratio < best_ratio
                || ((ratio - best_ratio).abs() < f64::EPSILON
                    && members[index].executor_id < members[best_index].executor_id)
            {
                best_index = index;
                best_ratio = ratio;
            }
        }
        loads[best_index] += 1;
        assignments.push((partition_id, members[best_index].executor_id.clone()));
    }

    assignments
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use fabrik_events::WorkflowIdentity;
    use serde_json::json;
    use std::{
        process::Command,
        time::{Duration as StdDuration, Instant},
    };
    use tokio::time::sleep;

    struct TestPostgres {
        container_name: String,
        database_url: String,
    }

    impl TestPostgres {
        fn start() -> Result<Option<Self>> {
            if !docker_available() {
                eprintln!("skipping postgres-backed store tests because docker is unavailable");
                return Ok(None);
            }

            let container_name = format!("fabrik-store-test-{}", Uuid::now_v7());
            let image = std::env::var("FABRIK_TEST_POSTGRES_IMAGE")
                .unwrap_or_else(|_| "postgres:16-alpine".to_owned());
            let output = Command::new("docker")
                .args([
                    "run",
                    "--detach",
                    "--rm",
                    "--name",
                    &container_name,
                    "--env",
                    "POSTGRES_USER=fabrik",
                    "--env",
                    "POSTGRES_PASSWORD=fabrik",
                    "--env",
                    "POSTGRES_DB=fabrik_test",
                    "--publish-all",
                    &image,
                ])
                .output()
                .with_context(|| format!("failed to start docker container {container_name}"))?;
            if !output.status.success() {
                anyhow::bail!(
                    "docker failed to start postgres test container: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }

            let host_port = match wait_for_host_port(&container_name) {
                Ok(port) => port,
                Err(error) => {
                    let _ = cleanup_container(&container_name);
                    return Err(error);
                }
            };
            let database_url = format!(
                "postgres://fabrik:fabrik@127.0.0.1:{host_port}/fabrik_test?sslmode=disable"
            );
            Ok(Some(Self { container_name, database_url }))
        }

        async fn connect_store(&self) -> Result<WorkflowStore> {
            let deadline = Instant::now() + StdDuration::from_secs(30);
            loop {
                match WorkflowStore::connect(&self.database_url).await {
                    Ok(store) => {
                        store.init().await?;
                        return Ok(store);
                    }
                    Err(error) if Instant::now() < deadline => {
                        let _ = error;
                        sleep(StdDuration::from_millis(250)).await;
                    }
                    Err(error) => {
                        let logs = docker_logs(&self.container_name).unwrap_or_default();
                        return Err(error).with_context(|| {
                            format!(
                                "postgres test container {} did not become ready; logs:\n{}",
                                self.container_name, logs
                            )
                        });
                    }
                }
            }
        }
    }

    impl Drop for TestPostgres {
        fn drop(&mut self) {
            let _ = cleanup_container(&self.container_name);
        }
    }

    fn docker_available() -> bool {
        Command::new("docker")
            .args(["info", "--format", "{{.ServerVersion}}"])
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    fn wait_for_host_port(container_name: &str) -> Result<u16> {
        let deadline = Instant::now() + StdDuration::from_secs(15);
        loop {
            let output = Command::new("docker")
                .args([
                    "inspect",
                    "--format",
                    "{{(index (index .NetworkSettings.Ports \"5432/tcp\") 0).HostPort}}",
                    container_name,
                ])
                .output()
                .with_context(|| format!("failed to inspect docker container {container_name}"))?;
            if output.status.success() {
                let host_port = String::from_utf8_lossy(&output.stdout).trim().to_owned();
                if !host_port.is_empty() {
                    return host_port
                        .parse::<u16>()
                        .with_context(|| format!("invalid postgres host port {host_port}"));
                }
            }
            if Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for postgres port mapping for {container_name}");
            }
            std::thread::sleep(StdDuration::from_millis(100));
        }
    }

    fn docker_logs(container_name: &str) -> Result<String> {
        let output = Command::new("docker")
            .args(["logs", container_name])
            .output()
            .with_context(|| format!("failed to read docker logs for {container_name}"))?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        Ok(format!("{stdout}{stderr}"))
    }

    fn cleanup_container(container_name: &str) -> Result<()> {
        let output = Command::new("docker")
            .args(["rm", "--force", container_name])
            .output()
            .with_context(|| format!("failed to remove docker container {container_name}"))?;
        if output.status.success() {
            Ok(())
        } else {
            anyhow::bail!(
                "docker failed to remove container {container_name}: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )
        }
    }

    fn demo_event(artifact_hash: &str) -> EventEnvelope<WorkflowEvent> {
        EventEnvelope::new(
            WorkflowEvent::WorkflowTriggered { input: json!({"ok": true}) }.event_type(),
            WorkflowIdentity::new(
                "tenant-a".to_owned(),
                "demo".to_owned(),
                1,
                artifact_hash.to_owned(),
                "wf-1".to_owned(),
                "run-1".to_owned(),
                "test",
            ),
            WorkflowEvent::WorkflowTriggered { input: json!({"ok": true}) },
        )
    }

    fn signal_event(
        instance_id: &str,
        run_id: &str,
        sequence: usize,
    ) -> EventEnvelope<WorkflowEvent> {
        let mut event = EventEnvelope::new(
            WorkflowEvent::SignalQueued {
                signal_id: format!("sig-{sequence}"),
                signal_type: "external.approved".to_owned(),
                payload: json!({ "sequence": sequence }),
            }
            .event_type(),
            WorkflowIdentity::new(
                "tenant-a".to_owned(),
                "demo".to_owned(),
                1,
                "artifact-a".to_owned(),
                instance_id.to_owned(),
                run_id.to_owned(),
                "test",
            ),
            WorkflowEvent::SignalQueued {
                signal_id: format!("sig-{sequence}"),
                signal_type: "external.approved".to_owned(),
                payload: json!({ "sequence": sequence }),
            },
        );
        event.occurred_at += chrono::Duration::milliseconds(sequence as i64);
        event
    }

    fn activity_completed_event(
        instance_id: &str,
        run_id: &str,
        sequence: usize,
    ) -> EventEnvelope<WorkflowEvent> {
        let mut event = EventEnvelope::new(
            WorkflowEvent::ActivityTaskCompleted {
                activity_id: format!("activity-{sequence}"),
                attempt: 1,
                output: json!({ "sequence": sequence }),
                worker_id: "worker-a".to_owned(),
                worker_build_id: "build-a".to_owned(),
            }
            .event_type(),
            WorkflowIdentity::new(
                "tenant-a".to_owned(),
                "demo".to_owned(),
                1,
                "artifact-a".to_owned(),
                instance_id.to_owned(),
                run_id.to_owned(),
                "test",
            ),
            WorkflowEvent::ActivityTaskCompleted {
                activity_id: format!("activity-{sequence}"),
                attempt: 1,
                output: json!({ "sequence": sequence }),
                worker_id: "worker-a".to_owned(),
                worker_build_id: "build-a".to_owned(),
            },
        );
        event.occurred_at += chrono::Duration::milliseconds(sequence as i64);
        event
    }

    #[tokio::test]
    async fn workflow_task_lease_falls_back_after_expired_sticky_build() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;

        store
            .register_task_queue_build(
                "tenant-a",
                TaskQueueKind::Workflow,
                "payments",
                "build-a",
                &["artifact-a".to_owned()],
                None,
            )
            .await?;
        store
            .register_task_queue_build(
                "tenant-a",
                TaskQueueKind::Workflow,
                "payments",
                "build-b",
                &["artifact-a".to_owned()],
                None,
            )
            .await?;
        store
            .upsert_compatibility_set(
                "tenant-a",
                TaskQueueKind::Workflow,
                "payments",
                "stable",
                &["build-a".to_owned(), "build-b".to_owned()],
                true,
            )
            .await?;

        let event = demo_event("artifact-a");
        let task = store
            .enqueue_workflow_task(2, "payments", Some("build-a"), &event)
            .await?
            .context("workflow task should be created")?;
        let first_lease = store
            .lease_next_workflow_task(2, "poller-a", "build-a", chrono::Duration::milliseconds(1))
            .await?
            .context("preferred build should receive the first lease")?;
        assert_eq!(first_lease.task_id, task.task_id);
        assert_eq!(first_lease.lease_build_id.as_deref(), Some("build-a"));
        assert_eq!(first_lease.lease_poller_id.as_deref(), Some("poller-a"));

        sleep(StdDuration::from_millis(10)).await;

        let fallback_lease = store
            .lease_next_workflow_task(2, "poller-b", "build-b", chrono::Duration::seconds(30))
            .await?
            .context("compatible fallback build should receive the expired lease")?;
        assert_eq!(fallback_lease.task_id, task.task_id);
        assert_eq!(fallback_lease.lease_build_id.as_deref(), Some("build-b"));
        assert_eq!(fallback_lease.lease_poller_id.as_deref(), Some("poller-b"));

        Ok(())
    }

    #[tokio::test]
    async fn incompatible_build_cannot_lease_workflow_task() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;

        store
            .register_task_queue_build(
                "tenant-a",
                TaskQueueKind::Workflow,
                "payments",
                "build-a",
                &["artifact-a".to_owned()],
                None,
            )
            .await?;
        store
            .register_task_queue_build(
                "tenant-a",
                TaskQueueKind::Workflow,
                "payments",
                "build-b",
                &["artifact-a".to_owned()],
                None,
            )
            .await?;
        store
            .upsert_compatibility_set(
                "tenant-a",
                TaskQueueKind::Workflow,
                "payments",
                "stable",
                &["build-a".to_owned()],
                true,
            )
            .await?;
        store
            .enqueue_workflow_task(2, "payments", None, &demo_event("artifact-a"))
            .await?
            .context("workflow task should be created")?;

        let incompatible = store
            .lease_next_workflow_task(2, "poller-b", "build-b", chrono::Duration::seconds(30))
            .await?;
        assert!(incompatible.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn complete_workflow_task_reconciles_stale_resume_backlog() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;

        store
            .register_task_queue_build(
                "tenant-a",
                TaskQueueKind::Workflow,
                "payments",
                "build-a",
                &["artifact-a".to_owned()],
                None,
            )
            .await?;
        store
            .upsert_compatibility_set(
                "tenant-a",
                TaskQueueKind::Workflow,
                "payments",
                "stable",
                &["build-a".to_owned()],
                true,
            )
            .await?;

        let event = activity_completed_event("wf-stale", "run-stale", 0);
        let resume = store
            .enqueue_workflow_resume(
                2,
                "payments",
                Some("build-a"),
                &event,
                WorkflowResumeKind::ActivityTerminal,
                "activity-0",
                Some("completed"),
            )
            .await?
            .context("workflow resume should be created")?;
        let task = store
            .lease_next_workflow_task(2, "poller-a", "build-a", chrono::Duration::seconds(30))
            .await?
            .context("workflow task should be leased")?;

        store
            .mark_workflow_resume_item_consumed(
                "tenant-a",
                "wf-stale",
                "run-stale",
                resume.resume_seq,
                chrono::Utc::now(),
            )
            .await?;

        sqlx::query(
            r#"
            UPDATE workflow_tasks
            SET resume_backlog = 1
            WHERE task_id = $1
            "#,
        )
        .bind(task.task_id)
        .execute(&store.pool)
        .await?;

        assert!(
            store
                .complete_workflow_task(task.task_id, "poller-a", "build-a", chrono::Utc::now(),)
                .await?
        );

        let updated = store
            .get_workflow_task(task.task_id)
            .await?
            .context("workflow task should still exist")?;
        assert_eq!(updated.resume_backlog, 0);
        assert_eq!(updated.resume_consumed_seq, updated.resume_high_watermark);
        assert_eq!(updated.status, WorkflowTaskStatus::Completed);
        assert_eq!(updated.lease_poller_id.as_deref(), Some("poller-a"));
        assert_eq!(updated.lease_build_id.as_deref(), Some("build-a"));

        Ok(())
    }

    #[tokio::test]
    async fn partition_ownership_handoffs_after_lease_expiry() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;

        let initial = store
            .claim_partition_ownership(7, "executor-a", StdDuration::from_millis(200))
            .await?
            .context("initial owner should claim partition")?;
        assert_eq!(initial.owner_id, "executor-a");
        assert_eq!(initial.owner_epoch, 1);
        assert!(store.validate_partition_ownership(7, "executor-a", 1).await?);

        let busy =
            store.claim_partition_ownership(7, "executor-b", StdDuration::from_secs(1)).await?;
        assert!(busy.is_none());

        sleep(StdDuration::from_millis(250)).await;

        let handoff = store
            .claim_partition_ownership(7, "executor-b", StdDuration::from_secs(1))
            .await?
            .context("new owner should claim expired partition")?;
        assert_eq!(handoff.owner_id, "executor-b");
        assert_eq!(handoff.owner_epoch, 2);
        assert!(!store.validate_partition_ownership(7, "executor-a", 1).await?);
        assert!(store.validate_partition_ownership(7, "executor-b", 2).await?);
        assert!(
            store
                .renew_partition_ownership(7, "executor-a", 1, StdDuration::from_secs(1))
                .await?
                .is_none()
        );

        Ok(())
    }

    #[tokio::test]
    async fn throughput_partition_assignments_are_rebalanced_and_isolated() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let ttl = StdDuration::from_secs(30);

        store.heartbeat_executor_member("executor-a", "http://executor-a", 1, ttl).await?;
        store.heartbeat_throughput_member("runtime-a", "http://runtime-a", 2, ttl).await?;

        let executor_members = store.list_active_executor_members(Utc::now()).await?;
        let throughput_members = store.list_active_throughput_members(Utc::now()).await?;
        assert_eq!(executor_members.len(), 1);
        assert_eq!(executor_members[0].executor_id, "executor-a");
        assert_eq!(throughput_members.len(), 1);
        assert_eq!(throughput_members[0].executor_id, "runtime-a");

        assert!(store.reconcile_partition_assignments(3, &executor_members).await?);
        assert!(store.reconcile_throughput_partition_assignments(3, &throughput_members).await?);

        let executor_assignments = store.list_assignments_for_executor("executor-a").await?;
        let throughput_assignments =
            store.list_assignments_for_throughput_runtime("runtime-a").await?;
        assert_eq!(executor_assignments.len(), 3);
        assert_eq!(throughput_assignments.len(), 3);
        assert_eq!(
            executor_assignments
                .iter()
                .map(|assignment| assignment.partition_id)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            throughput_assignments
                .iter()
                .map(|assignment| assignment.partition_id)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert!(store.list_assignments_for_executor("runtime-a").await?.is_empty());
        assert!(store.list_assignments_for_throughput_runtime("executor-a").await?.is_empty());

        assert!(store.reconcile_throughput_partition_assignments(3, &[]).await?);
        assert!(store.list_assignments_for_throughput_runtime("runtime-a").await?.is_empty());
        assert_eq!(store.list_assignments_for_executor("executor-a").await?.len(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn throughput_partition_handoff_does_not_wait_for_lease_expiry_when_owner_membership_is_gone()
    -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;

        store
            .heartbeat_throughput_member(
                "runtime-a",
                "http://runtime-a",
                1,
                StdDuration::from_millis(150),
            )
            .await?;
        let initial = store
            .claim_throughput_partition_ownership(17, "runtime-a", StdDuration::from_secs(10))
            .await?
            .context("initial throughput owner should claim partition")?;
        assert_eq!(initial.owner_id, "runtime-a");
        assert_eq!(initial.owner_epoch, 1);

        sleep(StdDuration::from_millis(250)).await;

        store
            .heartbeat_throughput_member(
                "runtime-b",
                "http://runtime-b",
                1,
                StdDuration::from_secs(30),
            )
            .await?;
        let handoff = store
            .claim_throughput_partition_ownership(17, "runtime-b", StdDuration::from_secs(10))
            .await?
            .context("new throughput owner should claim inactive member partition")?;
        assert_eq!(handoff.owner_id, "runtime-b");
        assert_eq!(handoff.owner_epoch, 2);
        assert!(store.validate_partition_ownership(17, "runtime-b", 2).await?);
        assert!(!store.validate_partition_ownership(17, "runtime-a", 1).await?);

        Ok(())
    }

    #[tokio::test]
    async fn task_queue_throughput_policy_and_partition_member_are_queryable() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let ttl = StdDuration::from_secs(30);

        let policy = store
            .upsert_task_queue_throughput_policy(
                "tenant-a",
                TaskQueueKind::Activity,
                "bulk",
                "stream-v2",
            )
            .await?;
        assert_eq!(policy.backend, "stream-v2");

        let loaded = store
            .get_task_queue_throughput_policy("tenant-a", TaskQueueKind::Activity, "bulk")
            .await?
            .context("throughput policy should exist")?;
        assert_eq!(loaded.backend, "stream-v2");

        store.heartbeat_throughput_member("runtime-a", "http://runtime-a", 1, ttl).await?;
        let members = store.list_active_throughput_members(Utc::now()).await?;
        assert!(store.reconcile_throughput_partition_assignments(2, &members).await?);

        let member = store
            .get_active_throughput_member_for_partition(1, Utc::now())
            .await?
            .context("throughput member should be assigned")?;
        assert_eq!(member.executor_id, "runtime-a");
        assert_eq!(member.query_endpoint, "http://runtime-a");

        let inspection = store
            .inspect_task_queue("tenant-a", TaskQueueKind::Activity, "bulk", Utc::now())
            .await?;
        assert_eq!(
            inspection.throughput_policy.as_ref().map(|record| record.backend.as_str()),
            Some("stream-v2")
        );

        Ok(())
    }

    #[tokio::test]
    #[ignore = "perf harness; run with cargo test -p fabrik-store mailbox_task_coalescing_benchmark -- --ignored --nocapture"]
    async fn mailbox_task_coalescing_benchmark() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let instance_id = "wf-mailbox-bench";
        let run_id = "run-mailbox-bench";
        let accepted_messages = 1000_i64;
        let started_at = Instant::now();

        for sequence in 0..accepted_messages as usize {
            let event = signal_event(instance_id, run_id, sequence);
            store
                .enqueue_workflow_mailbox_message(
                    2,
                    "payments",
                    Some("build-a"),
                    &event,
                    WorkflowMailboxKind::Signal,
                    Some(&format!("sig-{sequence}")),
                    Some("external.approved"),
                    Some(&json!({ "sequence": sequence })),
                )
                .await?;
        }

        let elapsed = started_at.elapsed();
        let task_rows: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM workflow_tasks
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            "#,
        )
        .bind("tenant-a")
        .bind(instance_id)
        .bind(run_id)
        .fetch_one(&store.pool)
        .await?;
        let mailbox_backlog: i64 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(mailbox_backlog, 0)
            FROM workflow_tasks
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            LIMIT 1
            "#,
        )
        .bind("tenant-a")
        .bind(instance_id)
        .bind(run_id)
        .fetch_one(&store.pool)
        .await?;
        let legacy_task_rows = accepted_messages;
        let reduction_pct = 100.0 * (1.0 - task_rows as f64 / legacy_task_rows as f64);

        println!(
            "mailbox_task_coalescing_benchmark accepted_messages={accepted_messages} task_rows={task_rows} legacy_task_rows={legacy_task_rows} reduction_pct={reduction_pct:.2} mailbox_backlog={mailbox_backlog} elapsed_ms={}",
            elapsed.as_millis()
        );

        assert_eq!(task_rows, 1);
        assert_eq!(mailbox_backlog, accepted_messages);
        assert!(reduction_pct >= 99.0);

        Ok(())
    }

    #[tokio::test]
    #[ignore = "perf harness; run with cargo test -p fabrik-store resume_task_coalescing_benchmark -- --ignored --nocapture"]
    async fn resume_task_coalescing_benchmark() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let instance_id = "wf-resume-bench";
        let run_id = "run-resume-bench";
        let resume_events = 1000_i64;
        let started_at = Instant::now();

        for sequence in 0..resume_events as usize {
            let event = activity_completed_event(instance_id, run_id, sequence);
            store
                .enqueue_workflow_resume(
                    2,
                    "payments",
                    Some("build-a"),
                    &event,
                    WorkflowResumeKind::ActivityTerminal,
                    &format!("activity-{sequence}:1"),
                    Some("completed"),
                )
                .await?;
        }

        let elapsed = started_at.elapsed();
        let task_rows: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM workflow_tasks
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            "#,
        )
        .bind("tenant-a")
        .bind(instance_id)
        .bind(run_id)
        .fetch_one(&store.pool)
        .await?;
        let resume_backlog: i64 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(resume_backlog, 0)
            FROM workflow_tasks
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            LIMIT 1
            "#,
        )
        .bind("tenant-a")
        .bind(instance_id)
        .bind(run_id)
        .fetch_one(&store.pool)
        .await?;
        let legacy_task_rows = resume_events;
        let reduction_pct = 100.0 * (1.0 - task_rows as f64 / legacy_task_rows as f64);

        println!(
            "resume_task_coalescing_benchmark resume_events={resume_events} task_rows={task_rows} legacy_task_rows={legacy_task_rows} reduction_pct={reduction_pct:.2} resume_backlog={resume_backlog} elapsed_ms={}",
            elapsed.as_millis()
        );

        assert_eq!(task_rows, 1);
        assert_eq!(resume_backlog, resume_events);
        assert!(reduction_pct >= 99.0);

        Ok(())
    }

    #[tokio::test]
    async fn bulk_chunk_completion_enqueues_outbox_event() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;

        let (batch, chunks) = store
            .upsert_bulk_batch(
                "tenant-a",
                "wf-bulk-1",
                "run-bulk-1",
                "demo",
                Some(1),
                Some("artifact-1"),
                "batch-1",
                "benchmark.echo",
                "default",
                Some("join"),
                &json!({ "kind": "inline", "key": "bulk-input:batch-1" }),
                &json!({ "kind": "inline", "key": "bulk-result:batch-1" }),
                &[json!({"value": 1}), json!({"value": 2})],
                2,
                1,
                0,
                None,
                None,
                "legacy",
                1,
                false,
                1,
                "pg-v1",
                "1.0.0",
                Utc::now(),
            )
            .await?;
        assert_eq!(batch.chunk_count, 1);
        assert_eq!(chunks.len(), 1);

        let leased = store
            .lease_next_bulk_chunks(
                "tenant-a",
                "default",
                "worker-a",
                "build-a",
                chrono::Duration::seconds(30),
                1,
            )
            .await?;
        assert_eq!(leased.len(), 1);
        let leased_chunk = &leased[0];

        let applied = store
            .apply_bulk_chunk_terminal_update(&BulkChunkTerminalUpdate {
                tenant_id: leased_chunk.tenant_id.clone(),
                instance_id: leased_chunk.instance_id.clone(),
                run_id: leased_chunk.run_id.clone(),
                batch_id: leased_chunk.batch_id.clone(),
                chunk_id: leased_chunk.chunk_id.clone(),
                chunk_index: leased_chunk.chunk_index,
                group_id: leased_chunk.group_id,
                attempt: leased_chunk.attempt,
                lease_epoch: leased_chunk.lease_epoch,
                owner_epoch: leased_chunk.owner_epoch,
                report_id: Uuid::now_v7().to_string(),
                lease_token: leased_chunk
                    .lease_token
                    .context("leased chunk missing lease token")?,
                worker_id: "worker-a".to_owned(),
                worker_build_id: "build-a".to_owned(),
                occurred_at: Utc::now(),
                payload: BulkChunkTerminalPayload::Completed {
                    output: vec![json!({"value": 1}), json!({"value": 2})],
                },
            })
            .await?;
        assert!(applied.terminal_event.is_some());

        let outbox = store
            .lease_workflow_event_outbox(
                "publisher-a",
                chrono::Duration::seconds(30),
                10,
                Utc::now(),
            )
            .await?;
        assert_eq!(outbox.len(), 1);
        assert_eq!(
            outbox[0].event.event_type,
            WorkflowEvent::BulkActivityBatchCompleted {
                batch_id: "batch-1".to_owned(),
                total_items: 2,
                succeeded_items: 2,
                failed_items: 0,
                cancelled_items: 0,
                chunk_count: 1,
            }
            .event_type()
        );
        assert!(
            store
                .mark_workflow_event_outbox_published(
                    outbox[0].outbox_id,
                    "publisher-a",
                    Utc::now(),
                )
                .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn bulk_chunk_leasing_is_pinned_to_backend() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let now = Utc::now();

        store
            .upsert_bulk_batch(
                "tenant-a",
                "wf-bulk-pg",
                "run-bulk-pg",
                "demo",
                Some(1),
                Some("artifact-1"),
                "batch-pg",
                "benchmark.echo",
                "bulk",
                Some("join"),
                &json!({ "kind": "inline", "key": "bulk-input:batch-pg" }),
                &json!({ "kind": "inline", "key": "bulk-result:batch-pg" }),
                &[json!({"value": 1})],
                1,
                1,
                0,
                None,
                None,
                "legacy",
                1,
                false,
                1,
                "pg-v1",
                "1.0.0",
                now,
            )
            .await?;
        store
            .upsert_bulk_batch(
                "tenant-a",
                "wf-bulk-stream",
                "run-bulk-stream",
                "demo",
                Some(1),
                Some("artifact-1"),
                "batch-stream",
                "benchmark.echo",
                "bulk",
                Some("join"),
                &json!({ "kind": "inline", "key": "bulk-input:batch-stream" }),
                &json!({ "kind": "inline", "key": "bulk-result:batch-stream" }),
                &[json!({"value": 2})],
                1,
                1,
                0,
                None,
                None,
                "legacy",
                1,
                false,
                1,
                "stream-v2",
                "2.0.0",
                now,
            )
            .await?;

        let pg_leased = store
            .lease_next_bulk_chunks(
                "tenant-a",
                "bulk",
                "worker-pg",
                "build-a",
                chrono::Duration::seconds(30),
                10,
            )
            .await?;
        assert_eq!(pg_leased.len(), 1);
        assert_eq!(pg_leased[0].batch_id, "batch-pg");

        let stream_leased = store
            .lease_next_bulk_chunks_for_backend(
                "tenant-a",
                "bulk",
                "worker-stream",
                "build-a",
                chrono::Duration::seconds(30),
                10,
                "stream-v2",
            )
            .await?;
        assert_eq!(stream_leased.len(), 1);
        assert_eq!(stream_leased[0].batch_id, "batch-stream");

        Ok(())
    }

    #[tokio::test]
    async fn stream_query_view_reads_projection_tables() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let now = Utc::now();

        let (batch, chunks) = store
            .upsert_bulk_batch(
                "tenant-a",
                "wf-bulk-stream-query",
                "run-bulk-stream-query",
                "demo",
                Some(1),
                Some("artifact-1"),
                "batch-stream-query",
                "benchmark.echo",
                "bulk",
                Some("join"),
                &json!({ "kind": "inline", "key": "bulk-input:batch-stream-query" }),
                &json!({ "kind": "inline", "key": "bulk-result:batch-stream-query" }),
                &[json!({"value": 1}), json!({"value": 2})],
                1,
                1,
                0,
                None,
                None,
                "legacy",
                1,
                false,
                1,
                "stream-v2",
                "2.0.0",
                now,
            )
            .await?;

        assert_eq!(
            store
                .count_bulk_batches_for_run_query_view(
                    "tenant-a",
                    "wf-bulk-stream-query",
                    "run-bulk-stream-query",
                )
                .await?,
            0
        );

        store.upsert_throughput_projection_batch(&batch).await?;
        for chunk in &chunks {
            store.upsert_throughput_projection_chunk(chunk).await?;
        }

        assert_eq!(
            store
                .count_bulk_batches_for_run_query_view(
                    "tenant-a",
                    "wf-bulk-stream-query",
                    "run-bulk-stream-query",
                )
                .await?,
            1
        );
        let listed_batches = store
            .list_bulk_batches_for_run_page_query_view(
                "tenant-a",
                "wf-bulk-stream-query",
                "run-bulk-stream-query",
                10,
                0,
            )
            .await?;
        assert_eq!(listed_batches.len(), 1);
        assert_eq!(listed_batches[0].batch_id, "batch-stream-query");
        assert_eq!(
            store
                .count_bulk_chunks_for_batch_query_view(
                    "tenant-a",
                    "wf-bulk-stream-query",
                    "run-bulk-stream-query",
                    "batch-stream-query",
                )
                .await?,
            2
        );

        let query_batch = store
            .get_bulk_batch_query_view(
                "tenant-a",
                "wf-bulk-stream-query",
                "run-bulk-stream-query",
                "batch-stream-query",
            )
            .await?
            .context("projected stream batch missing from query view")?;
        assert_eq!(query_batch.throughput_backend, "stream-v2");

        let query_chunks = store
            .list_bulk_chunks_for_batch_page_query_view(
                "tenant-a",
                "wf-bulk-stream-query",
                "run-bulk-stream-query",
                "batch-stream-query",
                10,
                0,
            )
            .await?;
        assert_eq!(query_chunks.len(), 2);
        assert!(query_chunks.iter().all(|chunk| chunk.batch_id == "batch-stream-query"));

        Ok(())
    }

    #[tokio::test]
    async fn stream_projection_state_updates_patch_existing_rows() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let now = Utc::now();

        let (batch, chunks) = store
            .upsert_bulk_batch(
                "tenant-a",
                "wf-bulk-stream-patch",
                "run-bulk-stream-patch",
                "demo",
                Some(1),
                Some("artifact-1"),
                "batch-stream-patch",
                "benchmark.echo",
                "bulk",
                Some("join"),
                &json!({ "kind": "inline", "key": "bulk-input:batch-stream-patch" }),
                &json!({ "kind": "inline", "key": "bulk-result:batch-stream-patch" }),
                &[json!({"value": 1}), json!({"value": 2})],
                2,
                2,
                1000,
                None,
                None,
                "legacy",
                1,
                false,
                1,
                "stream-v2",
                "2.0.0",
                now,
            )
            .await?;
        store.upsert_throughput_projection_batch(&batch).await?;
        for chunk in &chunks {
            store.upsert_throughput_projection_chunk(chunk).await?;
        }

        let patch_time = now + chrono::Duration::seconds(5);
        store
            .update_throughput_projection_chunk_state(&ThroughputProjectionChunkStateUpdate {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "wf-bulk-stream-patch".to_owned(),
                run_id: "run-bulk-stream-patch".to_owned(),
                batch_id: "batch-stream-patch".to_owned(),
                chunk_id: chunks[0].chunk_id.clone(),
                chunk_index: chunks[0].chunk_index,
                group_id: chunks[0].group_id,
                item_count: chunks[0].item_count,
                status: "completed".to_owned(),
                attempt: 1,
                lease_epoch: 1,
                owner_epoch: 1,
                input_handle: None,
                result_handle: None,
                output: Some(vec![json!({"value": "done"})]),
                error: None,
                cancellation_requested: false,
                cancellation_reason: None,
                cancellation_metadata: None,
                worker_id: None,
                worker_build_id: None,
                lease_token: None,
                last_report_id: Some("report-1".to_owned()),
                available_at: patch_time,
                started_at: Some(now),
                lease_expires_at: None,
                completed_at: Some(patch_time),
                updated_at: patch_time,
            })
            .await?;
        store
            .update_throughput_projection_batch_state(&ThroughputProjectionBatchStateUpdate {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "wf-bulk-stream-patch".to_owned(),
                run_id: "run-bulk-stream-patch".to_owned(),
                batch_id: "batch-stream-patch".to_owned(),
                status: "running".to_owned(),
                succeeded_items: 1,
                failed_items: 0,
                cancelled_items: 0,
                error: None,
                terminal_at: None,
                updated_at: patch_time,
            })
            .await?;

        let query_batch = store
            .get_bulk_batch_query_view(
                "tenant-a",
                "wf-bulk-stream-patch",
                "run-bulk-stream-patch",
                "batch-stream-patch",
            )
            .await?
            .context("patched stream batch missing from query view")?;
        assert_eq!(query_batch.status, WorkflowBulkBatchStatus::Running);
        assert_eq!(query_batch.succeeded_items, 1);

        let query_chunks = store
            .list_bulk_chunks_for_batch_page_query_view(
                "tenant-a",
                "wf-bulk-stream-patch",
                "run-bulk-stream-patch",
                "batch-stream-patch",
                10,
                0,
            )
            .await?;
        assert_eq!(query_chunks[0].status, WorkflowBulkChunkStatus::Completed);
        assert_eq!(query_chunks[0].output.as_ref(), Some(&vec![json!({"value": "done"})]));
        assert_eq!(query_chunks[0].last_report_id.as_deref(), Some("report-1"));

        Ok(())
    }

    #[tokio::test]
    async fn stream_projection_chunk_state_batch_updates_patch_existing_rows() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let now = Utc::now();

        let (batch, chunks) = store
            .upsert_bulk_batch(
                "tenant-a",
                "wf-bulk-stream-batch-patch",
                "run-bulk-stream-batch-patch",
                "demo",
                Some(1),
                Some("artifact-1"),
                "batch-stream-batch-patch",
                "benchmark.echo",
                "bulk",
                Some("join"),
                &json!({ "kind": "inline", "key": "bulk-input:batch-stream-batch-patch" }),
                &json!({ "kind": "inline", "key": "bulk-result:batch-stream-batch-patch" }),
                &[json!({"value": 1}), json!({"value": 2})],
                1,
                2,
                1000,
                None,
                None,
                "legacy",
                1,
                false,
                1,
                "stream-v2",
                "2.0.0",
                now,
            )
            .await?;
        store.upsert_throughput_projection_batch(&batch).await?;
        store.upsert_throughput_projection_chunks(&chunks).await?;

        let patch_time = now + chrono::Duration::seconds(5);
        let lease_expires_at = patch_time + chrono::Duration::seconds(30);
        store
            .update_throughput_projection_chunk_states(&[
                ThroughputProjectionChunkStateUpdate {
                    tenant_id: "tenant-a".to_owned(),
                    instance_id: "wf-bulk-stream-batch-patch".to_owned(),
                    run_id: "run-bulk-stream-batch-patch".to_owned(),
                    batch_id: "batch-stream-batch-patch".to_owned(),
                    chunk_id: chunks[0].chunk_id.clone(),
                    chunk_index: chunks[0].chunk_index,
                    group_id: chunks[0].group_id,
                    item_count: chunks[0].item_count,
                    status: "completed".to_owned(),
                    attempt: 1,
                    lease_epoch: 1,
                    owner_epoch: 1,
                    input_handle: None,
                    result_handle: None,
                    output: Some(vec![json!({"value": "done"})]),
                    error: None,
                    cancellation_requested: false,
                    cancellation_reason: None,
                    cancellation_metadata: None,
                    worker_id: None,
                    worker_build_id: None,
                    lease_token: None,
                    last_report_id: Some("report-1".to_owned()),
                    available_at: patch_time,
                    started_at: Some(now),
                    lease_expires_at: None,
                    completed_at: Some(patch_time),
                    updated_at: patch_time,
                },
                ThroughputProjectionChunkStateUpdate {
                    tenant_id: "tenant-a".to_owned(),
                    instance_id: "wf-bulk-stream-batch-patch".to_owned(),
                    run_id: "run-bulk-stream-batch-patch".to_owned(),
                    batch_id: "batch-stream-batch-patch".to_owned(),
                    chunk_id: chunks[1].chunk_id.clone(),
                    chunk_index: chunks[1].chunk_index,
                    group_id: chunks[1].group_id,
                    item_count: chunks[1].item_count,
                    status: "started".to_owned(),
                    attempt: 2,
                    lease_epoch: 2,
                    owner_epoch: 1,
                    input_handle: Some(Value::Null),
                    result_handle: Some(Value::Null),
                    output: None,
                    error: None,
                    cancellation_requested: false,
                    cancellation_reason: None,
                    cancellation_metadata: None,
                    worker_id: Some("worker-a".to_owned()),
                    worker_build_id: Some("build-a".to_owned()),
                    lease_token: None,
                    last_report_id: Some("report-2".to_owned()),
                    available_at: patch_time,
                    started_at: Some(patch_time),
                    lease_expires_at: Some(lease_expires_at),
                    completed_at: None,
                    updated_at: patch_time,
                },
            ])
            .await?;

        let query_chunks = store
            .list_bulk_chunks_for_batch_page_query_view(
                "tenant-a",
                "wf-bulk-stream-batch-patch",
                "run-bulk-stream-batch-patch",
                "batch-stream-batch-patch",
                10,
                0,
            )
            .await?;
        assert_eq!(query_chunks.len(), 2);

        assert_eq!(query_chunks[0].status, WorkflowBulkChunkStatus::Completed);
        assert_eq!(query_chunks[0].input_handle, chunks[0].input_handle);
        assert_eq!(query_chunks[0].result_handle, chunks[0].result_handle);
        assert_eq!(query_chunks[0].output.as_ref(), Some(&vec![json!({"value": "done"})]));
        assert_eq!(query_chunks[0].last_report_id.as_deref(), Some("report-1"));

        assert_eq!(query_chunks[1].status, WorkflowBulkChunkStatus::Started);
        assert_eq!(query_chunks[1].input_handle, Value::Null);
        assert_eq!(query_chunks[1].result_handle, Value::Null);
        assert_eq!(query_chunks[1].worker_id.as_deref(), Some("worker-a"));
        assert_eq!(query_chunks[1].worker_build_id.as_deref(), Some("build-a"));
        assert_eq!(query_chunks[1].last_report_id.as_deref(), Some("report-2"));
        assert_eq!(query_chunks[1].lease_expires_at, Some(lease_expires_at));

        Ok(())
    }

    #[tokio::test]
    async fn stream_projection_terminal_batch_update_is_monotonic() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let now = Utc::now();

        let (batch, _chunks) = store
            .upsert_bulk_batch(
                "tenant-a",
                "wf-bulk-stream-terminal",
                "run-bulk-stream-terminal",
                "demo",
                Some(1),
                Some("artifact-1"),
                "batch-stream-terminal",
                "benchmark.echo",
                "bulk",
                Some("join"),
                &json!({ "kind": "inline", "key": "bulk-input:batch-stream-terminal" }),
                &json!({ "kind": "inline", "key": "bulk-result:batch-stream-terminal" }),
                &[json!({"value": 1}), json!({"value": 2})],
                2,
                2,
                1000,
                None,
                None,
                "legacy",
                1,
                false,
                3,
                "stream-v2",
                "2.0.0",
                now,
            )
            .await?;
        store.upsert_throughput_projection_batch(&batch).await?;

        let terminal_at = now + chrono::Duration::seconds(10);
        store
            .update_throughput_projection_batch_state(&ThroughputProjectionBatchStateUpdate {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "wf-bulk-stream-terminal".to_owned(),
                run_id: "run-bulk-stream-terminal".to_owned(),
                batch_id: "batch-stream-terminal".to_owned(),
                status: "completed".to_owned(),
                succeeded_items: 2,
                failed_items: 0,
                cancelled_items: 0,
                error: None,
                terminal_at: Some(terminal_at),
                updated_at: terminal_at,
            })
            .await?;

        let stale_running_at = terminal_at + chrono::Duration::seconds(1);
        store
            .update_throughput_projection_batch_state(&ThroughputProjectionBatchStateUpdate {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "wf-bulk-stream-terminal".to_owned(),
                run_id: "run-bulk-stream-terminal".to_owned(),
                batch_id: "batch-stream-terminal".to_owned(),
                status: "running".to_owned(),
                succeeded_items: 1,
                failed_items: 0,
                cancelled_items: 0,
                error: None,
                terminal_at: None,
                updated_at: stale_running_at,
            })
            .await?;

        let projected = store
            .get_bulk_batch_query_view(
                "tenant-a",
                "wf-bulk-stream-terminal",
                "run-bulk-stream-terminal",
                "batch-stream-terminal",
            )
            .await?
            .context("terminal stream batch missing from query view")?;

        assert_eq!(projected.status, WorkflowBulkBatchStatus::Completed);
        assert_eq!(projected.succeeded_items, 2);
        assert_eq!(projected.terminal_at, Some(terminal_at));

        Ok(())
    }

    #[tokio::test]
    async fn stream_projection_batch_state_batch_updates_patch_existing_rows() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let now = Utc::now();

        let (running_batch, _running_chunks) = store
            .upsert_bulk_batch(
                "tenant-a",
                "wf-bulk-stream-batch-state-a",
                "run-bulk-stream-batch-state-a",
                "demo",
                Some(1),
                Some("artifact-1"),
                "batch-stream-batch-state-a",
                "benchmark.echo",
                "bulk",
                Some("join"),
                &json!({ "kind": "inline", "key": "bulk-input:batch-stream-batch-state-a" }),
                &json!({ "kind": "inline", "key": "bulk-result:batch-stream-batch-state-a" }),
                &[json!({"value": 1}), json!({"value": 2})],
                1,
                2,
                1000,
                None,
                None,
                "legacy",
                1,
                false,
                2,
                "stream-v2",
                "2.0.0",
                now,
            )
            .await?;
        store.upsert_throughput_projection_batch(&running_batch).await?;

        let (terminal_batch, _terminal_chunks) = store
            .upsert_bulk_batch(
                "tenant-a",
                "wf-bulk-stream-batch-state-b",
                "run-bulk-stream-batch-state-b",
                "demo",
                Some(1),
                Some("artifact-1"),
                "batch-stream-batch-state-b",
                "benchmark.echo",
                "bulk",
                Some("join"),
                &json!({ "kind": "inline", "key": "bulk-input:batch-stream-batch-state-b" }),
                &json!({ "kind": "inline", "key": "bulk-result:batch-stream-batch-state-b" }),
                &[json!({"value": 1})],
                1,
                1,
                1000,
                None,
                None,
                "legacy",
                1,
                false,
                1,
                "stream-v2",
                "2.0.0",
                now,
            )
            .await?;
        store.upsert_throughput_projection_batch(&terminal_batch).await?;

        let patch_time = now + chrono::Duration::seconds(5);
        store
            .update_throughput_projection_batch_states(&[
                ThroughputProjectionBatchStateUpdate {
                    tenant_id: "tenant-a".to_owned(),
                    instance_id: "wf-bulk-stream-batch-state-a".to_owned(),
                    run_id: "run-bulk-stream-batch-state-a".to_owned(),
                    batch_id: "batch-stream-batch-state-a".to_owned(),
                    status: "running".to_owned(),
                    succeeded_items: 1,
                    failed_items: 0,
                    cancelled_items: 0,
                    error: None,
                    terminal_at: None,
                    updated_at: patch_time,
                },
                ThroughputProjectionBatchStateUpdate {
                    tenant_id: "tenant-a".to_owned(),
                    instance_id: "wf-bulk-stream-batch-state-b".to_owned(),
                    run_id: "run-bulk-stream-batch-state-b".to_owned(),
                    batch_id: "batch-stream-batch-state-b".to_owned(),
                    status: "completed".to_owned(),
                    succeeded_items: 1,
                    failed_items: 0,
                    cancelled_items: 0,
                    error: None,
                    terminal_at: Some(patch_time),
                    updated_at: patch_time,
                },
            ])
            .await?;

        let running_projected = store
            .get_bulk_batch_query_view(
                "tenant-a",
                "wf-bulk-stream-batch-state-a",
                "run-bulk-stream-batch-state-a",
                "batch-stream-batch-state-a",
            )
            .await?
            .context("running stream batch missing from query view")?;
        assert_eq!(running_projected.status, WorkflowBulkBatchStatus::Running);
        assert_eq!(running_projected.succeeded_items, 1);
        assert_eq!(running_projected.terminal_at, None);

        let terminal_projected = store
            .get_bulk_batch_query_view(
                "tenant-a",
                "wf-bulk-stream-batch-state-b",
                "run-bulk-stream-batch-state-b",
                "batch-stream-batch-state-b",
            )
            .await?
            .context("terminal stream batch missing from query view")?;
        assert_eq!(terminal_projected.status, WorkflowBulkBatchStatus::Completed);
        assert_eq!(terminal_projected.succeeded_items, 1);
        assert_eq!(terminal_projected.terminal_at, Some(patch_time));

        Ok(())
    }

    #[tokio::test]
    async fn stream_projection_terminal_chunk_update_is_monotonic() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let now = Utc::now();

        let (batch, chunks) = store
            .upsert_bulk_batch(
                "tenant-a",
                "wf-bulk-stream-terminal-chunk",
                "run-bulk-stream-terminal-chunk",
                "demo",
                Some(1),
                Some("artifact-1"),
                "batch-stream-terminal-chunk",
                "benchmark.echo",
                "bulk",
                Some("join"),
                &json!({ "kind": "inline", "key": "bulk-input:batch-stream-terminal-chunk" }),
                &json!({ "kind": "inline", "key": "bulk-result:batch-stream-terminal-chunk" }),
                &[json!({"value": 1}), json!({"value": 2})],
                2,
                2,
                1000,
                None,
                None,
                "legacy",
                1,
                false,
                1,
                "stream-v2",
                "2.0.0",
                now,
            )
            .await?;
        store.upsert_throughput_projection_batch(&batch).await?;
        for chunk in &chunks {
            store.upsert_throughput_projection_chunk(chunk).await?;
        }

        let terminal_at = now + chrono::Duration::seconds(5);
        store
            .update_throughput_projection_chunk_state(&ThroughputProjectionChunkStateUpdate {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "wf-bulk-stream-terminal-chunk".to_owned(),
                run_id: "run-bulk-stream-terminal-chunk".to_owned(),
                batch_id: "batch-stream-terminal-chunk".to_owned(),
                chunk_id: chunks[0].chunk_id.clone(),
                chunk_index: chunks[0].chunk_index,
                group_id: chunks[0].group_id,
                item_count: chunks[0].item_count,
                status: "completed".to_owned(),
                attempt: 1,
                lease_epoch: 1,
                owner_epoch: 1,
                input_handle: None,
                result_handle: None,
                output: Some(vec![json!({"value": "done"})]),
                error: None,
                cancellation_requested: false,
                cancellation_reason: None,
                cancellation_metadata: None,
                worker_id: Some("worker-a".to_owned()),
                worker_build_id: Some("build-a".to_owned()),
                lease_token: None,
                last_report_id: Some("report-1".to_owned()),
                available_at: terminal_at,
                started_at: Some(now),
                lease_expires_at: None,
                completed_at: Some(terminal_at),
                updated_at: terminal_at,
            })
            .await?;

        let stale_nonterminal_at = terminal_at + chrono::Duration::seconds(1);
        store
            .update_throughput_projection_chunk_state(&ThroughputProjectionChunkStateUpdate {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "wf-bulk-stream-terminal-chunk".to_owned(),
                run_id: "run-bulk-stream-terminal-chunk".to_owned(),
                batch_id: "batch-stream-terminal-chunk".to_owned(),
                chunk_id: chunks[0].chunk_id.clone(),
                chunk_index: chunks[0].chunk_index,
                group_id: chunks[0].group_id,
                item_count: chunks[0].item_count,
                status: "scheduled".to_owned(),
                attempt: 2,
                lease_epoch: 2,
                owner_epoch: 1,
                input_handle: None,
                result_handle: None,
                output: None,
                error: None,
                cancellation_requested: false,
                cancellation_reason: None,
                cancellation_metadata: None,
                worker_id: Some("worker-b".to_owned()),
                worker_build_id: Some("build-b".to_owned()),
                lease_token: None,
                last_report_id: None,
                available_at: stale_nonterminal_at,
                started_at: None,
                lease_expires_at: None,
                completed_at: None,
                updated_at: stale_nonterminal_at,
            })
            .await?;

        let projected = store
            .get_bulk_chunk_query_view(
                "tenant-a",
                "wf-bulk-stream-terminal-chunk",
                "run-bulk-stream-terminal-chunk",
                "batch-stream-terminal-chunk",
                &chunks[0].chunk_id,
            )
            .await?
            .context("terminal stream chunk missing from query view")?;

        assert_eq!(projected.status, WorkflowBulkChunkStatus::Completed);
        assert_eq!(projected.output.as_ref(), Some(&vec![json!({"value": "done"})]));
        assert_eq!(projected.completed_at, Some(terminal_at));
        assert_eq!(projected.attempt, 1);

        Ok(())
    }

    #[tokio::test]
    async fn throughput_bridge_progress_only_dedupes_after_publish() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let workflow_event_id = Uuid::now_v7();
        let created_at = Utc::now();

        assert_eq!(
            store
                .ensure_throughput_bridge_progress(
                    workflow_event_id,
                    "tenant-a",
                    "wf-bridge",
                    "run-bridge",
                    "batch-bridge",
                    "stream-v2",
                    "throughput-create:test",
                    created_at,
                )
                .await?,
            None
        );
        assert_eq!(
            store
                .ensure_throughput_bridge_progress(
                    workflow_event_id,
                    "tenant-a",
                    "wf-bridge",
                    "run-bridge",
                    "batch-bridge",
                    "stream-v2",
                    "throughput-create:test",
                    created_at,
                )
                .await?,
            None
        );

        store.mark_throughput_bridge_command_published(workflow_event_id, created_at).await?;

        let published_at = store
            .ensure_throughput_bridge_progress(
                workflow_event_id,
                "tenant-a",
                "wf-bridge",
                "run-bridge",
                "batch-bridge",
                "stream-v2",
                "throughput-create:test",
                created_at,
            )
            .await?;
        assert_eq!(published_at, Some(created_at));

        Ok(())
    }

    #[tokio::test]
    async fn bulk_chunk_batch_limit_skips_capped_batch() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let now = Utc::now();

        store
            .upsert_bulk_batch(
                "tenant-a",
                "wf-bulk-cap-a",
                "run-bulk-cap-a",
                "demo",
                Some(1),
                Some("artifact-1"),
                "batch-cap-a",
                "benchmark.echo",
                "bulk",
                Some("join"),
                &json!({ "kind": "inline", "key": "bulk-input:batch-cap-a" }),
                &json!({ "kind": "inline", "key": "bulk-result:batch-cap-a" }),
                &[json!({"value": 1}), json!({"value": 2})],
                1,
                1,
                0,
                None,
                None,
                "legacy",
                1,
                false,
                1,
                "stream-v2",
                "2.0.0",
                now,
            )
            .await?;
        store
            .upsert_bulk_batch(
                "tenant-a",
                "wf-bulk-cap-b",
                "run-bulk-cap-b",
                "demo",
                Some(1),
                Some("artifact-1"),
                "batch-cap-b",
                "benchmark.echo",
                "bulk",
                Some("join"),
                &json!({ "kind": "inline", "key": "bulk-input:batch-cap-b" }),
                &json!({ "kind": "inline", "key": "bulk-result:batch-cap-b" }),
                &[json!({"value": 3})],
                1,
                1,
                0,
                None,
                None,
                "legacy",
                1,
                false,
                1,
                "stream-v2",
                "2.0.0",
                now + chrono::Duration::milliseconds(1),
            )
            .await?;

        let first = store
            .lease_next_bulk_chunks_for_backend_with_batch_limit(
                "tenant-a",
                "bulk",
                "worker-1",
                "build-a",
                chrono::Duration::seconds(30),
                1,
                "stream-v2",
                Some(1),
            )
            .await?;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].batch_id, "batch-cap-a");

        let second = store
            .lease_next_bulk_chunks_for_backend_with_batch_limit(
                "tenant-a",
                "bulk",
                "worker-2",
                "build-a",
                chrono::Duration::seconds(30),
                1,
                "stream-v2",
                Some(1),
            )
            .await?;
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].batch_id, "batch-cap-b");

        Ok(())
    }

    #[tokio::test]
    async fn cancel_bulk_batch_cancels_remaining_chunks_and_enqueues_terminal() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let now = Utc::now();

        let (_batch, _chunks) = store
            .upsert_bulk_batch(
                "tenant-a",
                "wf-bulk-cancel",
                "run-bulk-cancel",
                "demo",
                Some(1),
                Some("artifact-1"),
                "batch-cancel",
                "benchmark.echo",
                "bulk",
                Some("join"),
                &json!({ "kind": "inline", "key": "bulk-input:batch-cancel" }),
                &json!({ "kind": "inline", "key": "bulk-result:batch-cancel" }),
                &[json!({"value": 1}), json!({"value": 2}), json!({"value": 3})],
                1,
                1,
                0,
                None,
                None,
                "legacy",
                1,
                false,
                1,
                "stream-v2",
                "2.0.0",
                now,
            )
            .await?;

        let leased = store
            .lease_next_bulk_chunks_for_backend(
                "tenant-a",
                "bulk",
                "worker-a",
                "build-a",
                chrono::Duration::seconds(30),
                1,
                "stream-v2",
            )
            .await?;
        assert_eq!(leased.len(), 1);

        let cancelled = store
            .cancel_bulk_batch(
                "tenant-a",
                "wf-bulk-cancel",
                "run-bulk-cancel",
                "batch-cancel",
                "workflow cancelled",
                None,
                Utc::now(),
            )
            .await?;
        assert!(cancelled.terminal_event.is_some());
        assert_eq!(cancelled.batch.status, WorkflowBulkBatchStatus::Cancelled);
        assert_eq!(cancelled.batch.cancelled_items, 3);
        assert_eq!(cancelled.cancelled_chunks.len(), 3);
        assert!(
            cancelled
                .cancelled_chunks
                .iter()
                .all(|chunk| chunk.status == WorkflowBulkChunkStatus::Cancelled)
        );

        let outbox = store
            .lease_workflow_event_outbox(
                "publisher-cancel",
                chrono::Duration::seconds(30),
                10,
                Utc::now(),
            )
            .await?;
        assert_eq!(outbox.len(), 1);
        assert_eq!(
            outbox[0].event.event_type,
            WorkflowEvent::BulkActivityBatchCancelled {
                batch_id: "batch-cancel".to_owned(),
                total_items: 3,
                succeeded_items: 0,
                failed_items: 0,
                cancelled_items: 3,
                chunk_count: 3,
                message: "workflow cancelled".to_owned(),
            }
            .event_type()
        );

        Ok(())
    }
}
