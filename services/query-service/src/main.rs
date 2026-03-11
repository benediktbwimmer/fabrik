use anyhow::Result;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use chrono::{DateTime, Utc};
use fabrik_broker::{BrokerConfig, WorkflowHistoryFilter, read_workflow_history};
use fabrik_config::{HttpServiceConfig, PostgresConfig, RedpandaConfig};
use fabrik_events::{EventEnvelope, WorkflowEvent};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::{
    WorkflowEffectRecord, WorkflowRunRecord, WorkflowSignalRecord, WorkflowStateSnapshot,
    WorkflowStore,
};
use fabrik_workflow::{
    CompiledWorkflowArtifact, ReplayDivergence, ReplayDivergenceKind, ReplayFieldMismatch,
    ReplaySource, ReplayTransitionTraceEntry, WorkflowDefinition, WorkflowInstanceState,
    artifact_hash, first_transition_divergence, projection_mismatches,
    replay_compiled_history_trace, replay_compiled_history_trace_from_snapshot,
    replay_history_trace, replay_history_trace_from_snapshot, same_projection,
};
use serde::Serialize;
use serde_json::{Value, to_value};
use std::time::Duration;
use tracing::info;
use uuid::Uuid;

const HISTORY_IDLE_TIMEOUT_MS: u64 = 1_000;
const HISTORY_MAX_SCAN_MS: u64 = 10_000;

#[derive(Clone)]
struct AppState {
    store: WorkflowStore,
    broker: BrokerConfig,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    message: String,
}

#[derive(Debug, Serialize)]
struct WorkflowHistoryResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    definition_id: String,
    definition_version: u32,
    artifact_hash: String,
    previous_run_id: Option<String>,
    next_run_id: Option<String>,
    continue_reason: Option<String>,
    event_count: usize,
    effect_attempt_count: usize,
    effect_attempts: Vec<WorkflowEffectRecord>,
    events: Vec<EventEnvelope<WorkflowEvent>>,
}

#[derive(Debug, Serialize)]
struct WorkflowReplayResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    definition_id: String,
    definition_version: u32,
    artifact_hash: String,
    previous_run_id: Option<String>,
    next_run_id: Option<String>,
    continue_reason: Option<String>,
    event_count: usize,
    last_event_type: String,
    effect_attempt_count: usize,
    effect_attempts: Vec<WorkflowEffectRecord>,
    projection_matches_store: Option<bool>,
    replay_source: ReplaySource,
    snapshot: Option<ReplaySnapshotSummary>,
    divergence_count: usize,
    divergences: Vec<ReplayDivergence>,
    transition_count: usize,
    transition_trace: Vec<ReplayTransitionTraceEntry>,
    replayed_state: WorkflowInstanceState,
}

#[derive(Debug, Serialize)]
struct ReplaySnapshotSummary {
    run_id: String,
    snapshot_schema_version: u32,
    event_count: i64,
    last_event_id: Uuid,
    last_event_type: String,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct WorkflowEffectsResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    effect_count: usize,
    effects: Vec<WorkflowEffectRecord>,
}

#[derive(Debug, Serialize)]
struct WorkflowSignalsResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    signal_count: usize,
    signals: Vec<WorkflowSignalRecord>,
}

#[derive(Debug, Serialize)]
struct WorkflowRunsResponse {
    tenant_id: String,
    instance_id: String,
    current_run_id: Option<String>,
    run_count: usize,
    runs: Vec<WorkflowRunRecord>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = HttpServiceConfig::from_env("QUERY_SERVICE", "query-service", 3005)?;
    let postgres = PostgresConfig::from_env()?;
    let redpanda = RedpandaConfig::from_env()?;
    init_tracing(&config.log_filter);
    info!(port = config.port, "starting query service");

    let store = WorkflowStore::connect(&postgres.url).await?;
    store.init().await?;
    let broker = BrokerConfig::new(redpanda.brokers, redpanda.workflow_events_topic);

    let app = default_router::<AppState>(ServiceInfo::new(
        config.name,
        "query",
        env!("CARGO_PKG_VERSION"),
    ))
    .route(
        "/tenants/{tenant_id}/workflow-definitions/{definition_id}/latest",
        get(get_latest_workflow_definition),
    )
    .route(
        "/tenants/{tenant_id}/workflow-definitions/{definition_id}/versions/{version}",
        get(get_workflow_definition_version),
    )
    .route(
        "/tenants/{tenant_id}/workflow-artifacts/{definition_id}/latest",
        get(get_latest_workflow_artifact),
    )
    .route(
        "/tenants/{tenant_id}/workflow-artifacts/{definition_id}/versions/{version}",
        get(get_workflow_artifact_version),
    )
    .route("/tenants/{tenant_id}/workflows/{instance_id}", get(get_workflow_instance))
    .route("/tenants/{tenant_id}/workflows/{instance_id}/runs", get(get_workflow_runs))
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/snapshot",
        get(get_latest_workflow_snapshot),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/effects",
        get(get_current_workflow_effects),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/signals",
        get(get_current_workflow_signals),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/effects",
        get(get_workflow_effects_for_run),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/signals",
        get(get_workflow_signals_for_run),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/history",
        get(get_current_workflow_history),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/history",
        get(get_workflow_history_for_run),
    )
    .route("/tenants/{tenant_id}/workflows/{instance_id}/replay", get(get_current_workflow_replay))
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/replay",
        get(get_workflow_replay_for_run),
    )
    .with_state(AppState { store, broker });

    serve(app, config.port).await
}

async fn get_workflow_instance(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowInstanceState>, (StatusCode, Json<ErrorResponse>)> {
    match state.store.get_instance(&tenant_id, &instance_id).await.map_err(internal_error)? {
        Some(instance) => Ok(Json(instance)),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                message: format!(
                    "workflow instance {instance_id} not found for tenant {tenant_id}"
                ),
            }),
        )),
    }
}

async fn get_current_workflow_history(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowHistoryResponse>, (StatusCode, Json<ErrorResponse>)> {
    let instance = state
        .store
        .get_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    message: format!(
                        "workflow instance {instance_id} not found for tenant {tenant_id}"
                    ),
                }),
            )
        })?;
    let response = load_workflow_history(&state, &tenant_id, &instance_id, &instance.run_id)
        .await
        .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_runs(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowRunsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let current_instance =
        state.store.get_instance(&tenant_id, &instance_id).await.map_err(internal_error)?;
    let runs = state
        .store
        .list_runs_for_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?;
    Ok(Json(WorkflowRunsResponse {
        tenant_id,
        instance_id,
        current_run_id: current_instance.map(|instance| instance.run_id),
        run_count: runs.len(),
        runs,
    }))
}

async fn get_latest_workflow_snapshot(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowStateSnapshot>, (StatusCode, Json<ErrorResponse>)> {
    match state.store.get_latest_snapshot(&tenant_id, &instance_id).await.map_err(internal_error)? {
        Some(snapshot) => Ok(Json(snapshot)),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                message: format!(
                    "workflow snapshot for instance {instance_id} not found for tenant {tenant_id}"
                ),
            }),
        )),
    }
}

async fn get_current_workflow_effects(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowEffectsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let instance = state
        .store
        .get_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    message: format!(
                        "workflow instance {instance_id} not found for tenant {tenant_id}"
                    ),
                }),
            )
        })?;
    let response = load_workflow_effects(&state, &tenant_id, &instance_id, &instance.run_id)
        .await
        .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_current_workflow_signals(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowSignalsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let instance = state
        .store
        .get_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    message: format!(
                        "workflow instance {instance_id} not found for tenant {tenant_id}"
                    ),
                }),
            )
        })?;
    let response = load_workflow_signals(&state, &tenant_id, &instance_id, &instance.run_id)
        .await
        .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_effects_for_run(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowEffectsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_effects(&state, &tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_signals_for_run(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowSignalsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_signals(&state, &tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_history_for_run(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowHistoryResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_history(&state, &tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_current_workflow_replay(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowReplayResponse>, (StatusCode, Json<ErrorResponse>)> {
    let current_instance = state
        .store
        .get_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    message: format!(
                        "workflow instance {instance_id} not found for tenant {tenant_id}"
                    ),
                }),
            )
        })?;
    let response = replay_workflow_run(&state, &current_instance, &current_instance.run_id)
        .await
        .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_replay_for_run(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowReplayResponse>, (StatusCode, Json<ErrorResponse>)> {
    let current_instance = state
        .store
        .get_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    message: format!(
                        "workflow instance {instance_id} not found for tenant {tenant_id}"
                    ),
                }),
            )
        })?;
    let response =
        replay_workflow_run(&state, &current_instance, &run_id).await.map_err(internal_error)?;
    Ok(Json(response))
}

fn internal_error(error: anyhow::Error) -> (StatusCode, Json<ErrorResponse>) {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { message: error.to_string() }))
}

async fn load_workflow_history(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
) -> Result<WorkflowHistoryResponse> {
    let history = read_history(&state.broker, tenant_id, instance_id, run_id).await?;
    let first_event = history.first().ok_or_else(|| {
        anyhow::anyhow!(
            "no workflow history found for tenant={tenant_id}, instance_id={instance_id}, run_id={run_id}"
        )
    })?;
    let effect_attempts = state.store.list_effects_for_run(tenant_id, instance_id, run_id).await?;
    let run = state.store.get_run_record(tenant_id, instance_id, run_id).await?;

    Ok(WorkflowHistoryResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        definition_id: first_event.definition_id.clone(),
        definition_version: first_event.definition_version,
        artifact_hash: first_event.artifact_hash.clone(),
        previous_run_id: run.as_ref().and_then(|run| run.previous_run_id.clone()),
        next_run_id: run.as_ref().and_then(|run| run.next_run_id.clone()),
        continue_reason: run.and_then(|run| run.continue_reason),
        event_count: history.len(),
        effect_attempt_count: effect_attempts.len(),
        effect_attempts,
        events: history,
    })
}

async fn load_workflow_effects(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
) -> Result<WorkflowEffectsResponse> {
    let effects = state.store.list_effects_for_run(tenant_id, instance_id, run_id).await?;
    Ok(WorkflowEffectsResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        effect_count: effects.len(),
        effects,
    })
}

async fn load_workflow_signals(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
) -> Result<WorkflowSignalsResponse> {
    let signals = state.store.list_signals_for_run(tenant_id, instance_id, run_id).await?;
    Ok(WorkflowSignalsResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        signal_count: signals.len(),
        signals,
    })
}

async fn replay_workflow_run(
    state: &AppState,
    current_instance: &WorkflowInstanceState,
    run_id: &str,
) -> Result<WorkflowReplayResponse> {
    let history = read_history(
        &state.broker,
        &current_instance.tenant_id,
        &current_instance.instance_id,
        run_id,
    )
    .await?;
    if history.is_empty() {
        anyhow::bail!(
            "no workflow history found for tenant={}, instance_id={}, run_id={run_id}",
            current_instance.tenant_id,
            current_instance.instance_id
        );
    }
    let first_event = history
        .first()
        .ok_or_else(|| anyhow::anyhow!("workflow history unexpectedly empty after read"))?;
    let run = state
        .store
        .get_run_record(&current_instance.tenant_id, &current_instance.instance_id, run_id)
        .await?;

    let pinned_artifact = {
        let version = first_event.definition_version;
        state
            .store
            .get_artifact_version(&current_instance.tenant_id, &first_event.definition_id, version)
            .await?
    };

    let full_trace = if let Some(artifact) = pinned_artifact.as_ref() {
        replay_compiled_history_trace(&history, artifact)?
    } else {
        replay_history_trace(&history)?
    };
    let definition_version = full_trace
        .final_state
        .definition_version
        .ok_or_else(|| anyhow::anyhow!("replayed state is missing definition_version"))?;
    let pinned_artifact_hash = full_trace
        .final_state
        .artifact_hash
        .clone()
        .ok_or_else(|| anyhow::anyhow!("replayed state is missing artifact_hash"))?;
    if let Some(artifact) = state
        .store
        .get_artifact_version(
            &current_instance.tenant_id,
            &full_trace.final_state.definition_id,
            definition_version,
        )
        .await?
    {
        if artifact.artifact_hash != pinned_artifact_hash {
            anyhow::bail!(
                "artifact hash mismatch for replayed run: history={}, artifact={}",
                pinned_artifact_hash,
                artifact.artifact_hash
            );
        }
    } else {
        let definition = state
            .store
            .get_definition_version(
                &current_instance.tenant_id,
                &full_trace.final_state.definition_id,
                definition_version,
            )
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "workflow definition {} version {} not found for tenant {}",
                    full_trace.final_state.definition_id,
                    definition_version,
                    current_instance.tenant_id
                )
            })?;
        let computed_artifact_hash = artifact_hash(&definition);
        if computed_artifact_hash != pinned_artifact_hash {
            anyhow::bail!(
                "artifact hash mismatch for replayed run: history={}, definition={}",
                pinned_artifact_hash,
                computed_artifact_hash
            );
        }
    }

    let snapshot = state
        .store
        .get_snapshot_for_run(&current_instance.tenant_id, &current_instance.instance_id, run_id)
        .await?;
    let snapshot_summary = snapshot.as_ref().map(snapshot_summary);
    let mut divergences = Vec::new();

    let active_trace = if let Some(snapshot) = snapshot {
        match history.iter().position(|event| event.event_id == snapshot.last_event_id) {
            Some(index) => {
                let tail = &history[index + 1..];
                let trace = if let Some(artifact) = pinned_artifact.as_ref() {
                    replay_compiled_history_trace_from_snapshot(
                        tail,
                        &snapshot.state,
                        artifact,
                        snapshot.event_count,
                        snapshot.last_event_id,
                        &snapshot.last_event_type,
                    )?
                } else {
                    replay_history_trace_from_snapshot(
                        tail,
                        &snapshot.state,
                        snapshot.event_count,
                        snapshot.last_event_id,
                        &snapshot.last_event_type,
                    )?
                };
                let expected_tail =
                    full_trace.transitions.iter().skip(index + 1).cloned().collect::<Vec<_>>();
                if let Some(divergence) =
                    first_transition_divergence(&expected_tail, &trace.transitions)
                {
                    divergences.push(divergence);
                } else if !same_projection(&full_trace.final_state, &trace.final_state) {
                    divergences.push(ReplayDivergence {
                        kind: ReplayDivergenceKind::SnapshotMismatch,
                        event_id: history.last().map(|event| event.event_id),
                        event_type: history.last().map(|event| event.event_type.clone()),
                        message: "snapshot-backed replay produced a different final state"
                            .to_owned(),
                        fields: projection_mismatches(&full_trace.final_state, &trace.final_state),
                    });
                }
                trace
            }
            None => {
                divergences.push(ReplayDivergence {
                    kind: ReplayDivergenceKind::SnapshotMismatch,
                    event_id: Some(snapshot.last_event_id),
                    event_type: Some(snapshot.last_event_type.clone()),
                    message: "snapshot boundary event was not found in broker history".to_owned(),
                    fields: vec![ReplayFieldMismatch {
                        field: "snapshot_last_event_id".to_owned(),
                        expected: to_value(snapshot.last_event_id)
                            .expect("snapshot event id serializes"),
                        actual: Value::Null,
                    }],
                });
                full_trace.clone()
            }
        }
    } else {
        full_trace.clone()
    };

    let last_event_type = history
        .last()
        .map(|event| event.event_type.clone())
        .ok_or_else(|| anyhow::anyhow!("workflow history unexpectedly empty after replay"))?;
    let effect_attempts = state
        .store
        .list_effects_for_run(&current_instance.tenant_id, &current_instance.instance_id, run_id)
        .await?;
    let projection_matches_store = (current_instance.run_id == active_trace.final_state.run_id)
        .then(|| same_projection(current_instance, &active_trace.final_state));
    if matches!(projection_matches_store, Some(false)) {
        divergences.push(ReplayDivergence {
            kind: ReplayDivergenceKind::ProjectionMismatch,
            event_id: Some(active_trace.final_state.last_event_id),
            event_type: Some(active_trace.final_state.last_event_type.clone()),
            message: "replayed final state does not match the stored workflow projection"
                .to_owned(),
            fields: projection_mismatches(current_instance, &active_trace.final_state),
        });
    }

    Ok(WorkflowReplayResponse {
        tenant_id: current_instance.tenant_id.clone(),
        instance_id: current_instance.instance_id.clone(),
        run_id: run_id.to_owned(),
        definition_id: active_trace.final_state.definition_id.clone(),
        definition_version,
        artifact_hash: pinned_artifact_hash,
        previous_run_id: run.as_ref().and_then(|run| run.previous_run_id.clone()),
        next_run_id: run.as_ref().and_then(|run| run.next_run_id.clone()),
        continue_reason: run.and_then(|run| run.continue_reason),
        event_count: history.len(),
        last_event_type,
        effect_attempt_count: effect_attempts.len(),
        effect_attempts,
        projection_matches_store,
        replay_source: active_trace.source.clone(),
        snapshot: snapshot_summary,
        divergence_count: divergences.len(),
        divergences,
        transition_count: active_trace.transitions.len(),
        transition_trace: active_trace.transitions,
        replayed_state: active_trace.final_state,
    })
}

fn snapshot_summary(snapshot: &WorkflowStateSnapshot) -> ReplaySnapshotSummary {
    ReplaySnapshotSummary {
        run_id: snapshot.run_id.clone(),
        snapshot_schema_version: snapshot.snapshot_schema_version,
        event_count: snapshot.event_count,
        last_event_id: snapshot.last_event_id,
        last_event_type: snapshot.last_event_type.clone(),
        updated_at: snapshot.updated_at,
    }
}

async fn read_history(
    broker: &BrokerConfig,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
) -> Result<Vec<EventEnvelope<WorkflowEvent>>> {
    read_workflow_history(
        broker,
        "query-service",
        &WorkflowHistoryFilter::new(tenant_id, instance_id, run_id),
        Duration::from_millis(HISTORY_IDLE_TIMEOUT_MS),
        Duration::from_millis(HISTORY_MAX_SCAN_MS),
    )
    .await
}

async fn get_latest_workflow_definition(
    Path((tenant_id, definition_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowDefinition>, (StatusCode, Json<ErrorResponse>)> {
    match state
        .store
        .get_latest_definition(&tenant_id, &definition_id)
        .await
        .map_err(internal_error)?
    {
        Some(definition) => Ok(Json(definition)),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                message: format!(
                    "workflow definition {definition_id} not found for tenant {tenant_id}"
                ),
            }),
        )),
    }
}

async fn get_workflow_definition_version(
    Path((tenant_id, definition_id, version)): Path<(String, String, u32)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowDefinition>, (StatusCode, Json<ErrorResponse>)> {
    match state
        .store
        .get_definition_version(&tenant_id, &definition_id, version)
        .await
        .map_err(internal_error)?
    {
        Some(definition) => Ok(Json(definition)),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                message: format!(
                    "workflow definition {definition_id} version {version} not found for tenant {tenant_id}"
                ),
            }),
        )),
    }
}

async fn get_latest_workflow_artifact(
    Path((tenant_id, definition_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<CompiledWorkflowArtifact>, (StatusCode, Json<ErrorResponse>)> {
    match state
        .store
        .get_latest_artifact(&tenant_id, &definition_id)
        .await
        .map_err(internal_error)?
    {
        Some(artifact) => Ok(Json(artifact)),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                message: format!(
                    "workflow artifact {definition_id} not found for tenant {tenant_id}"
                ),
            }),
        )),
    }
}

async fn get_workflow_artifact_version(
    Path((tenant_id, definition_id, version)): Path<(String, String, u32)>,
    State(state): State<AppState>,
) -> Result<Json<CompiledWorkflowArtifact>, (StatusCode, Json<ErrorResponse>)> {
    match state
        .store
        .get_artifact_version(&tenant_id, &definition_id, version)
        .await
        .map_err(internal_error)?
    {
        Some(artifact) => Ok(Json(artifact)),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                message: format!(
                    "workflow artifact {definition_id} version {version} not found for tenant {tenant_id}"
                ),
            }),
        )),
    }
}
