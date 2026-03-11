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
                advertised_capacity,
                heartbeat_expires_at,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $4)
            ON CONFLICT (executor_id)
            DO UPDATE SET
                advertised_capacity = EXCLUDED.advertised_capacity,
                heartbeat_expires_at = EXCLUDED.heartbeat_expires_at,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(executor_id)
        .bind(i32::try_from(advertised_capacity).context("executor capacity exceeds i32")?)
        .bind(heartbeat_expires_at)
        .bind(now)
        .execute(&self.pool)
        .await
        .context("failed to heartbeat executor membership")?;

        Ok(ExecutorMembershipRecord {
            executor_id: executor_id.to_owned(),
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
            SELECT executor_id, advertised_capacity, heartbeat_expires_at, created_at, updated_at
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
                $1, $2, $3, $4, $5, $6, $7, NULL, NULL, $8, NULL, $9, $10, NULL, $10
            )
            ON CONFLICT (tenant_id, workflow_instance_id, run_id)
            DO UPDATE SET
                workflow_id = EXCLUDED.workflow_id,
                definition_version = EXCLUDED.definition_version,
                artifact_hash = EXCLUDED.artifact_hash,
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
    ) -> Result<Option<WorkflowSignalRecord>> {
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
        Ok(Some(signal))
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
