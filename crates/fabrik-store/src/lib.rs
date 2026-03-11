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
pub struct WorkflowEffectRecord {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub definition_id: String,
    pub definition_version: Option<u32>,
    pub artifact_hash: Option<String>,
    pub effect_id: String,
    pub attempt: u32,
    pub connector: String,
    pub state: Option<String>,
    pub status: WorkflowEffectStatus,
    pub timeout_ref: Option<String>,
    pub input: Value,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub cancellation_reason: Option<String>,
    pub cancellation_metadata: Option<Value>,
    pub requested_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub last_event_id: Uuid,
    pub last_event_type: String,
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
pub enum WorkflowEffectStatus {
    Requested,
    Completed,
    Failed,
    TimedOut,
    Cancelled,
}

impl WorkflowEffectStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
        }
    }

    fn from_db(value: &str) -> Result<Self> {
        match value {
            "requested" => Ok(Self::Requested),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "timed_out" => Ok(Self::TimedOut),
            "cancelled" => Ok(Self::Cancelled),
            other => anyhow::bail!("unknown workflow effect status {other}"),
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
                event_type TEXT NOT NULL,
                processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize processed_workflow_events table")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS connector_processed_events (
                event_id UUID PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize connector_processed_events table")?;

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
            CREATE TABLE IF NOT EXISTS workflow_effects (
                tenant_id TEXT NOT NULL,
                workflow_instance_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                workflow_id TEXT NOT NULL,
                workflow_version INTEGER,
                artifact_hash TEXT,
                effect_id TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                connector TEXT NOT NULL,
                state TEXT,
                status TEXT NOT NULL,
                timeout_ref TEXT,
                input JSONB NOT NULL,
                output JSONB,
                error TEXT,
                cancellation_reason TEXT,
                cancellation_metadata JSONB,
                requested_at TIMESTAMPTZ NOT NULL,
                completed_at TIMESTAMPTZ,
                last_event_id UUID NOT NULL,
                last_event_type TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (tenant_id, workflow_instance_id, run_id, effect_id, attempt)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("failed to initialize workflow_effects table")?;

        sqlx::query(
            "ALTER TABLE workflow_effects ADD COLUMN IF NOT EXISTS cancellation_reason TEXT",
        )
        .execute(&self.pool)
        .await
        .context("failed to add workflow_effects.cancellation_reason")?;

        sqlx::query(
            "ALTER TABLE workflow_effects ADD COLUMN IF NOT EXISTS cancellation_metadata JSONB",
        )
        .execute(&self.pool)
        .await
        .context("failed to add workflow_effects.cancellation_metadata")?;

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
            "ALTER TABLE workflow_timers ADD COLUMN IF NOT EXISTS dispatch_owner_epoch BIGINT",
        )
        .execute(&self.pool)
        .await
        .context("failed to add workflow_timers.dispatch_owner_epoch")?;

        Ok(())
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
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
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

    pub async fn count_effect_attempts_for_run(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
    ) -> Result<u64> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM workflow_effects
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .fetch_one(&self.pool)
        .await
        .context("failed to count workflow effect attempts")?;

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
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
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
                event_type
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (event_id) DO NOTHING
            "#,
        )
        .bind(event.event_id)
        .bind(&event.tenant_id)
        .bind(&event.instance_id)
        .bind(&event.event_type)
        .execute(&self.pool)
        .await
        .context("failed to mark workflow event as processed")?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn mark_connector_event_processed(
        &self,
        event: &EventEnvelope<WorkflowEvent>,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            INSERT INTO connector_processed_events (
                event_id,
                tenant_id,
                workflow_instance_id,
                event_type
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (event_id) DO NOTHING
            "#,
        )
        .bind(event.event_id)
        .bind(&event.tenant_id)
        .bind(&event.instance_id)
        .bind(&event.event_type)
        .execute(&self.pool)
        .await
        .context("failed to mark connector event as processed")?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn upsert_timer(
        &self,
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
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULL)
            ON CONFLICT (tenant_id, workflow_instance_id, timer_id)
            DO UPDATE SET
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
        now: DateTime<Utc>,
        limit: i64,
        owner_epoch: u64,
    ) -> Result<Vec<ScheduledTimer>> {
        let rows = sqlx::query(
            r#"
            SELECT
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
            WHERE fire_at <= $1
              AND (dispatched_at IS NULL OR dispatch_owner_epoch IS DISTINCT FROM $3)
            ORDER BY fire_at ASC
            LIMIT $2
            "#,
        )
        .bind(now)
        .bind(limit)
        .bind(i64::try_from(owner_epoch).context("ownership epoch exceeds i64")?)
        .fetch_all(&self.pool)
        .await
        .context("failed to load due workflow timers")?;

        let mut timers = Vec::with_capacity(rows.len());
        for row in rows {
            timers.push(ScheduledTimer {
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

    pub async fn upsert_effect_requested(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        definition_id: &str,
        definition_version: Option<u32>,
        artifact_hash: Option<&str>,
        effect_id: &str,
        attempt: u32,
        connector: &str,
        state: Option<&str>,
        timeout_ref: Option<&str>,
        input: &Value,
        event_id: Uuid,
        occurred_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO workflow_effects (
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                workflow_version,
                artifact_hash,
                effect_id,
                attempt,
                connector,
                state,
                status,
                timeout_ref,
                input,
                output,
                error,
                cancellation_reason,
                cancellation_metadata,
                requested_at,
                completed_at,
                last_event_id,
                last_event_type,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                NULL, NULL, NULL, NULL, $14, NULL, $15, $16, $17
            )
            ON CONFLICT (tenant_id, workflow_instance_id, run_id, effect_id, attempt)
            DO UPDATE SET
                workflow_id = EXCLUDED.workflow_id,
                workflow_version = EXCLUDED.workflow_version,
                artifact_hash = EXCLUDED.artifact_hash,
                connector = EXCLUDED.connector,
                state = EXCLUDED.state,
                status = EXCLUDED.status,
                timeout_ref = EXCLUDED.timeout_ref,
                input = EXCLUDED.input,
                output = NULL,
                error = NULL,
                cancellation_reason = NULL,
                cancellation_metadata = NULL,
                requested_at = EXCLUDED.requested_at,
                completed_at = NULL,
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
        .bind(effect_id)
        .bind(i32::try_from(attempt).context("effect attempt exceeds i32")?)
        .bind(connector)
        .bind(state)
        .bind(WorkflowEffectStatus::Requested.as_str())
        .bind(timeout_ref)
        .bind(Json(input))
        .bind(occurred_at)
        .bind(event_id)
        .bind("EffectRequested")
        .bind(occurred_at)
        .execute(&self.pool)
        .await
        .context("failed to upsert requested workflow effect")?;

        Ok(())
    }

    pub async fn complete_effect(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        effect_id: &str,
        attempt: u32,
        output: &Value,
        event_id: Uuid,
        event_type: &str,
        occurred_at: DateTime<Utc>,
    ) -> Result<()> {
        self.update_effect_terminal(
            tenant_id,
            instance_id,
            run_id,
            effect_id,
            attempt,
            WorkflowEffectStatus::Completed,
            Some(output),
            None,
            None,
            None,
            event_id,
            event_type,
            occurred_at,
        )
        .await
    }

    pub async fn fail_effect(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        effect_id: &str,
        attempt: u32,
        status: WorkflowEffectStatus,
        error: &str,
        cancellation_metadata: Option<&Value>,
        event_id: Uuid,
        event_type: &str,
        occurred_at: DateTime<Utc>,
    ) -> Result<()> {
        let cancellation_reason =
            matches!(status, WorkflowEffectStatus::Cancelled).then_some(error);
        self.update_effect_terminal(
            tenant_id,
            instance_id,
            run_id,
            effect_id,
            attempt,
            status,
            None,
            Some(error),
            cancellation_reason,
            cancellation_metadata,
            event_id,
            event_type,
            occurred_at,
        )
        .await
    }

    async fn update_effect_terminal(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        effect_id: &str,
        attempt: u32,
        status: WorkflowEffectStatus,
        output: Option<&Value>,
        error: Option<&str>,
        cancellation_reason: Option<&str>,
        cancellation_metadata: Option<&Value>,
        event_id: Uuid,
        event_type: &str,
        occurred_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE workflow_effects
            SET status = $6,
                output = $7,
                error = $8,
                cancellation_reason = $9,
                cancellation_metadata = $10,
                completed_at = $11,
                last_event_id = $12,
                last_event_type = $13,
                updated_at = $14
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND effect_id = $4
              AND attempt = $5
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(effect_id)
        .bind(i32::try_from(attempt).context("effect attempt exceeds i32")?)
        .bind(status.as_str())
        .bind(output.map(Json))
        .bind(error)
        .bind(cancellation_reason)
        .bind(cancellation_metadata.map(Json))
        .bind(occurred_at)
        .bind(event_id)
        .bind(event_type)
        .bind(occurred_at)
        .execute(&self.pool)
        .await
        .context("failed to update workflow effect terminal status")?;

        Ok(())
    }

    pub async fn list_effects_for_run(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
    ) -> Result<Vec<WorkflowEffectRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                workflow_version,
                artifact_hash,
                effect_id,
                attempt,
                connector,
                state,
                status,
                timeout_ref,
                input,
                output,
                error,
                cancellation_reason,
                cancellation_metadata,
                requested_at,
                completed_at,
                last_event_id,
                last_event_type,
                updated_at
            FROM workflow_effects
            WHERE tenant_id = $1 AND workflow_instance_id = $2 AND run_id = $3
            ORDER BY requested_at ASC, effect_id ASC, attempt ASC
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .context("failed to list workflow effects")?;

        rows.into_iter().map(Self::decode_effect_row).collect()
    }

    pub async fn get_effect_attempt(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        effect_id: &str,
        attempt: u32,
    ) -> Result<Option<WorkflowEffectRecord>> {
        let row = sqlx::query(
            r#"
            SELECT
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                workflow_version,
                artifact_hash,
                effect_id,
                attempt,
                connector,
                state,
                status,
                timeout_ref,
                input,
                output,
                error,
                cancellation_reason,
                cancellation_metadata,
                requested_at,
                completed_at,
                last_event_id,
                last_event_type,
                updated_at
            FROM workflow_effects
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND effect_id = $4
              AND attempt = $5
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(effect_id)
        .bind(i32::try_from(attempt).context("effect attempt exceeds i32")?)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load workflow effect attempt")?;

        row.map(Self::decode_effect_row).transpose()
    }

    pub async fn get_latest_active_effect(
        &self,
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        effect_id: &str,
    ) -> Result<Option<WorkflowEffectRecord>> {
        let row = sqlx::query(
            r#"
            SELECT
                tenant_id,
                workflow_instance_id,
                run_id,
                workflow_id,
                workflow_version,
                artifact_hash,
                effect_id,
                attempt,
                connector,
                state,
                status,
                timeout_ref,
                input,
                output,
                error,
                cancellation_reason,
                cancellation_metadata,
                requested_at,
                completed_at,
                last_event_id,
                last_event_type,
                updated_at
            FROM workflow_effects
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND run_id = $3
              AND effect_id = $4
              AND status = 'requested'
            ORDER BY attempt DESC
            LIMIT 1
            "#,
        )
        .bind(tenant_id)
        .bind(instance_id)
        .bind(run_id)
        .bind(effect_id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load active workflow effect")?;

        row.map(Self::decode_effect_row).transpose()
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

    fn decode_effect_row(row: sqlx::postgres::PgRow) -> Result<WorkflowEffectRecord> {
        Ok(WorkflowEffectRecord {
            tenant_id: row.try_get("tenant_id").context("effect tenant_id missing")?,
            instance_id: row
                .try_get("workflow_instance_id")
                .context("effect workflow_instance_id missing")?,
            run_id: row.try_get("run_id").context("effect run_id missing")?,
            definition_id: row.try_get("workflow_id").context("effect workflow_id missing")?,
            definition_version: row
                .try_get::<Option<i32>, _>("workflow_version")
                .context("effect workflow_version missing")?
                .map(|version| version as u32),
            artifact_hash: row.try_get("artifact_hash").context("effect artifact_hash missing")?,
            effect_id: row.try_get("effect_id").context("effect effect_id missing")?,
            attempt: row.try_get::<i32, _>("attempt").context("effect attempt missing")? as u32,
            connector: row.try_get("connector").context("effect connector missing")?,
            state: row.try_get("state").context("effect state missing")?,
            status: WorkflowEffectStatus::from_db(
                &row.try_get::<String, _>("status").context("effect status missing")?,
            )?,
            timeout_ref: row.try_get("timeout_ref").context("effect timeout_ref missing")?,
            input: row
                .try_get::<Json<Value>, _>("input")
                .map(|value| value.0)
                .context("effect input missing")?,
            output: row
                .try_get::<Option<Json<Value>>, _>("output")
                .map(|value: Option<Json<Value>>| value.map(|json| json.0))
                .context("effect output missing")?,
            error: row.try_get("error").context("effect error missing")?,
            cancellation_reason: row
                .try_get("cancellation_reason")
                .context("effect cancellation_reason missing")?,
            cancellation_metadata: row
                .try_get::<Option<Json<Value>>, _>("cancellation_metadata")
                .map(|value: Option<Json<Value>>| value.map(|json| json.0))
                .context("effect cancellation_metadata missing")?,
            requested_at: row.try_get("requested_at").context("effect requested_at missing")?,
            completed_at: row.try_get("completed_at").context("effect completed_at missing")?,
            last_event_id: row.try_get("last_event_id").context("effect last_event_id missing")?,
            last_event_type: row
                .try_get("last_event_type")
                .context("effect last_event_type missing")?,
            updated_at: row.try_get("updated_at").context("effect updated_at missing")?,
        })
    }
}
