use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use fabrik_events::{EventEnvelope, WorkflowEvent};
use fabrik_workflow::{CompiledWorkflowArtifact, WorkflowDefinition, WorkflowInstanceState};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, Row, postgres::PgPoolOptions, types::Json};
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
    pub default_set_id: Option<String>,
    pub compatible_build_ids: Vec<String>,
    pub registered_builds: Vec<TaskQueueBuildRecord>,
    pub compatibility_sets: Vec<CompatibilitySetRecord>,
    pub pollers: Vec<QueuePollerRecord>,
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

        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(INIT_LOCK_KEY)
            .execute(&self.pool)
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
            ON workflow_tasks (partition_id, status, task_queue, preferred_build_id, updated_at)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_tasks partition status index")?;

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
            .execute(&self.pool)
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

        tx.commit().await.context("failed to commit partition assignments")?;
        Ok(true)
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
        tx.commit()
            .await
            .context("failed to commit default-set promotion transaction")?;

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
                    .list_compatibility_set_build_ids(tenant_id, queue_kind.clone(), task_queue, set_id)
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
            let set_id: String = row
                .try_get("set_id")
                .context("compatibility set set_id missing")?;
            let build_ids = self
                .list_compatibility_set_build_ids(tenant_id, queue_kind.clone(), task_queue, &set_id)
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
        let registered_builds = self.list_task_queue_builds(tenant_id, queue_kind.clone(), task_queue).await?;
        if registered_builds.is_empty() {
            return Ok(true);
        }
        let compatible = self
            .list_default_compatible_build_ids(tenant_id, queue_kind, task_queue)
            .await?;
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

    pub async fn enqueue_workflow_task(
        &self,
        partition_id: i32,
        task_queue: &str,
        preferred_build_id: Option<&str>,
        event: &EventEnvelope<WorkflowEvent>,
    ) -> Result<Option<WorkflowTaskRecord>> {
        let task_id = Uuid::now_v7();
        let occurred_at = event.occurred_at;
        let inserted = sqlx::query(
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
                lease_poller_id,
                lease_build_id,
                lease_expires_at,
                attempt_count,
                last_error,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'pending', $11, $12, $13, NULL, NULL, NULL, 0, NULL, $14, $14
            )
            ON CONFLICT (source_event_id) DO NOTHING
            "#,
        )
        .bind(task_id)
        .bind(&event.tenant_id)
        .bind(&event.instance_id)
        .bind(&event.run_id)
        .bind(&event.definition_id)
        .bind(i32::try_from(event.definition_version).context("workflow task definition version exceeds i32")?)
        .bind(&event.artifact_hash)
        .bind(partition_id)
        .bind(task_queue)
        .bind(preferred_build_id)
        .bind(event.event_id)
        .bind(&event.event_type)
        .bind(Json(event.clone()))
        .bind(occurred_at)
        .execute(&self.pool)
        .await
        .context("failed to enqueue workflow task")?
        .rows_affected();

        if inserted == 0 {
            return Ok(None);
        }

        self.get_workflow_task(task_id).await
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
                lease_poller_id,
                lease_build_id,
                lease_expires_at,
                attempt_count,
                last_error,
                created_at,
                updated_at
            FROM workflow_tasks
            WHERE partition_id = $1
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
            SET status = 'completed',
                lease_poller_id = $2,
                lease_build_id = $3,
                lease_expires_at = NULL,
                updated_at = $4
            WHERE task_id = $1
              AND status = 'leased'
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
            SET status = 'pending',
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
        let compatible_build_ids =
            self.list_default_compatible_build_ids(tenant_id, queue_kind.clone(), task_queue).await?;
        let registered_builds =
            self.list_task_queue_builds(tenant_id, queue_kind.clone(), task_queue).await?;
        let pollers = self
            .list_queue_pollers(tenant_id, queue_kind.clone(), task_queue, active_at)
            .await?;
        let (backlog, oldest_backlog_at) =
            self.task_queue_backlog(tenant_id, queue_kind.clone(), task_queue).await?;
        let sticky_effectiveness =
            self.task_queue_sticky_effectiveness(tenant_id, queue_kind.clone(), task_queue).await?;

        Ok(TaskQueueInspection {
            tenant_id: tenant_id.to_owned(),
            queue_kind,
            task_queue: task_queue.to_owned(),
            backlog,
            oldest_backlog_at,
            sticky_effectiveness,
            default_set_id,
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
                    WHERE tenant_id = $1 AND task_queue = $2 AND status = 'pending'
                    "#,
                )
                .bind(tenant_id)
                .bind(task_queue)
                .fetch_one(&self.pool)
                .await
                .context("failed to inspect workflow queue backlog")?;
                Ok((
                    row.try_get::<i64, _>("backlog")
                        .context("workflow backlog missing")? as u64,
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
                    row.try_get::<i64, _>("backlog")
                        .context("activity backlog missing")? as u64,
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

        let sticky_dispatch_count = row
            .try_get::<i64, _>("sticky_dispatch_count")
            .context("workflow sticky_dispatch_count missing")? as u64;
        let sticky_hit_count =
            row.try_get::<i64, _>("sticky_hit_count").context("workflow sticky_hit_count missing")?
                as u64;
        let sticky_fallback_count = row
            .try_get::<i64, _>("sticky_fallback_count")
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
            created_at: row
                .try_get("created_at")
                .context("task queue build created_at missing")?,
            updated_at: row
                .try_get("updated_at")
                .context("task queue build updated_at missing")?,
        })
    }

    fn decode_compatibility_set_row(
        row: sqlx::postgres::PgRow,
        build_ids: Vec<String>,
    ) -> Result<CompatibilitySetRecord> {
        Ok(CompatibilitySetRecord {
            tenant_id: row
                .try_get("tenant_id")
                .context("compatibility set tenant_id missing")?,
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
            expires_at: row
                .try_get("expires_at")
                .context("queue poller expires_at missing")?,
            created_at: row
                .try_get("created_at")
                .context("queue poller created_at missing")?,
            updated_at: row
                .try_get("updated_at")
                .context("queue poller updated_at missing")?,
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
            task_queue: row
                .try_get("task_queue")
                .context("workflow task task_queue missing")?,
            preferred_build_id: row
                .try_get("preferred_build_id")
                .context("workflow task preferred_build_id missing")?,
            status: WorkflowTaskStatus::from_db(
                &row.try_get::<String, _>("status")
                    .context("workflow task status missing")?,
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
                .context("workflow task attempt_count missing")?
                as u32,
            last_error: row
                .try_get("last_error")
                .context("workflow task last_error missing")?,
            created_at: row
                .try_get("created_at")
                .context("workflow task created_at missing")?,
            updated_at: row
                .try_get("updated_at")
                .context("workflow task updated_at missing")?,
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
            let image =
                std::env::var("FABRIK_TEST_POSTGRES_IMAGE").unwrap_or_else(|_| "postgres:16-alpine".to_owned());
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
            let database_url =
                format!("postgres://fabrik:fabrik@127.0.0.1:{host_port}/fabrik_test?sslmode=disable");
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

        let busy = store
            .claim_partition_ownership(7, "executor-b", StdDuration::from_secs(1))
            .await?;
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
}
