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
    pub requested_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub last_event_id: Uuid,
    pub last_event_type: String,
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

        Ok(())
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
            WHERE dispatched_at IS NULL AND fire_at <= $1
            ORDER BY fire_at ASC
            LIMIT $2
            "#,
        )
        .bind(now)
        .bind(limit)
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
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_timers
            SET dispatched_at = NOW()
            WHERE tenant_id = $1
              AND workflow_instance_id = $2
              AND timer_id = $3
              AND dispatched_at IS NULL
            "#,
        )
        .bind(tenant_id)
        .bind(workflow_instance_id)
        .bind(timer_id)
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
            SET dispatched_at = NULL
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
                requested_at,
                completed_at,
                last_event_id,
                last_event_type,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NULL, NULL, $14, NULL, $15, $16, $17)
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
            status,
            None,
            Some(error),
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
                completed_at = $9,
                last_event_id = $10,
                last_event_type = $11,
                updated_at = $12
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
