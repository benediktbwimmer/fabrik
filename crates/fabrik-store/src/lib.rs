use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use fabrik_events::{EventEnvelope, WorkflowEvent};
use fabrik_workflow::{CompiledWorkflowArtifact, WorkflowDefinition, WorkflowInstanceState};
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
}
