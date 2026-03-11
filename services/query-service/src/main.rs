use anyhow::{Context, Result};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use fabrik_broker::{
    BrokerConfig, WorkflowHistoryFilter, WorkflowTopicTopology, describe_workflow_topic,
    read_workflow_history,
};
use fabrik_config::{HttpServiceConfig, PostgresConfig, QueryRuntimeConfig, RedpandaConfig};
use fabrik_events::{EventEnvelope, WorkflowEvent};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::{
    QueryRetentionCutoffs, QueryRetentionPruneResult, WorkflowActivityRecord,
    WorkflowBulkBatchRecord, WorkflowBulkChunkRecord, WorkflowRunRecord, WorkflowSignalRecord,
    WorkflowStateSnapshot, WorkflowStore,
};
use fabrik_workflow::{
    CompiledWorkflowArtifact, ReplayDivergence, ReplayDivergenceKind, ReplayFieldMismatch,
    ReplaySource, ReplayTransitionTraceEntry, WorkflowDefinition, WorkflowInstanceState,
    artifact_hash, first_transition_divergence, projection_mismatches, replay_compiled_history,
    replay_compiled_history_trace, replay_compiled_history_trace_from_snapshot,
    replay_history_trace, replay_history_trace_from_snapshot, same_projection,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, to_value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tracing::info;
use uuid::Uuid;

const HISTORY_IDLE_TIMEOUT_MS: u64 = 1_000;
const HISTORY_MAX_SCAN_MS: u64 = 10_000;

#[derive(Clone)]
struct AppState {
    store: WorkflowStore,
    broker: BrokerConfig,
    query: QueryRuntimeConfig,
    retention: Arc<Mutex<RetentionDebugState>>,
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
    page: PageInfo,
    activity_attempt_count: usize,
    activity_attempts: Vec<WorkflowActivityRecord>,
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
    activity_attempt_count: usize,
    activity_attempts: Vec<WorkflowActivityRecord>,
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
struct WorkflowActivitiesResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    page: PageInfo,
    activity_count: usize,
    activities: Vec<WorkflowActivityRecord>,
}

#[derive(Debug, Serialize)]
struct WorkflowBulkBatchesResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    page: PageInfo,
    batch_count: usize,
    batches: Vec<WorkflowBulkBatchRecord>,
}

#[derive(Debug, Serialize)]
struct WorkflowBulkBatchResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    batch: WorkflowBulkBatchRecord,
}

#[derive(Debug, Serialize)]
struct WorkflowBulkChunksResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    batch_id: String,
    page: PageInfo,
    chunk_count: usize,
    chunks: Vec<WorkflowBulkChunkRecord>,
}

#[derive(Debug, Serialize)]
struct WorkflowBulkResultsResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    batch_id: String,
    page: PageInfo,
    chunk_count: usize,
    chunks: Vec<WorkflowBulkChunkRecord>,
}

#[derive(Debug, Serialize)]
struct WorkflowSignalsResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    page: PageInfo,
    signal_count: usize,
    signals: Vec<WorkflowSignalRecord>,
}

#[derive(Debug, Serialize)]
struct WorkflowRunsResponse {
    tenant_id: String,
    instance_id: String,
    current_run_id: Option<String>,
    page: PageInfo,
    run_count: usize,
    runs: Vec<WorkflowRunRecord>,
}

#[derive(Debug, Deserialize, Default)]
struct StrongQueryRequest {
    #[serde(default)]
    args: Value,
}

#[derive(Debug, Serialize)]
struct StrongQueryResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    query_name: String,
    consistency: &'static str,
    source: &'static str,
    result: Value,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct PaginationQuery {
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
struct ResolvedPage {
    limit: usize,
    offset: usize,
}

#[derive(Debug, Clone, Serialize)]
struct PageInfo {
    limit: usize,
    offset: usize,
    returned: usize,
    total: usize,
    has_more: bool,
    next_offset: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
struct RetentionPolicyResponse {
    history_retention_days: Option<u64>,
    run_retention_days: Option<u64>,
    activity_retention_days: Option<u64>,
    signal_retention_days: Option<u64>,
    snapshot_retention_days: Option<u64>,
    retention_sweep_interval_seconds: u64,
}

#[derive(Debug, Clone, Serialize)]
struct RetentionDebugResponse {
    policy: RetentionPolicyResponse,
    last_sweep_at: Option<DateTime<Utc>>,
    last_result: Option<QueryRetentionPruneResult>,
}

#[derive(Debug, Default)]
struct RetentionDebugState {
    last_sweep_at: Option<DateTime<Utc>>,
    last_result: Option<QueryRetentionPruneResult>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = HttpServiceConfig::from_env("QUERY_SERVICE", "query-service", 3005)?;
    let postgres = PostgresConfig::from_env()?;
    let redpanda = RedpandaConfig::from_env()?;
    let query = QueryRuntimeConfig::from_env()?;
    init_tracing(&config.log_filter);
    info!(
        port = config.port,
        default_page_size = query.default_page_size,
        max_page_size = query.max_page_size,
        history_retention_days = ?query.history_retention_days,
        run_retention_days = ?query.run_retention_days,
        activity_retention_days = ?query.activity_retention_days,
        signal_retention_days = ?query.signal_retention_days,
        snapshot_retention_days = ?query.snapshot_retention_days,
        retention_sweep_interval_seconds = query.retention_sweep_interval_seconds,
        "starting query service"
    );

    let store = WorkflowStore::connect(&postgres.url).await?;
    store.init().await?;
    let broker = BrokerConfig::new(
        redpanda.brokers,
        redpanda.workflow_events_topic,
        redpanda.workflow_events_partitions,
    );
    let retention = Arc::new(Mutex::new(RetentionDebugState::default()));
    tokio::spawn(run_retention_sweeper(store.clone(), query.clone(), retention.clone()));

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
    .route("/debug/broker", get(get_broker_debug))
    .route("/debug/retention", get(get_retention_debug))
    .route("/tenants/{tenant_id}/workflows/{instance_id}", get(get_workflow_instance))
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/queries/{query_name}",
        post(execute_strong_query),
    )
    .route("/tenants/{tenant_id}/workflows/{instance_id}/runs", get(get_workflow_runs))
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/snapshot",
        get(get_latest_workflow_snapshot),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/activities",
        get(get_current_workflow_activities),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/signals",
        get(get_current_workflow_signals),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/activities",
        get(get_workflow_activities_for_run),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/signals",
        get(get_workflow_signals_for_run),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/bulk-batches",
        get(get_workflow_bulk_batches_for_run),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/bulk-batches/{batch_id}",
        get(get_workflow_bulk_batch_for_run),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/bulk-batches/{batch_id}/chunks",
        get(get_workflow_bulk_chunks_for_run),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/bulk-batches/{batch_id}/results",
        get(get_workflow_bulk_results_for_run),
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
    .with_state(AppState { store, broker, query, retention });

    serve(app, config.port).await
}

async fn get_broker_debug(
    State(state): State<AppState>,
) -> Result<Json<WorkflowTopicTopology>, (StatusCode, Json<ErrorResponse>)> {
    let topology = describe_workflow_topic(&state.broker, "query-service-debug")
        .await
        .map_err(internal_error)?;
    Ok(Json(topology))
}

async fn get_retention_debug(
    State(state): State<AppState>,
) -> Result<Json<RetentionDebugResponse>, (StatusCode, Json<ErrorResponse>)> {
    let snapshot = state.retention.lock().map_err(|_| {
        internal_error(anyhow::anyhow!("query retention debug state lock poisoned"))
    })?;
    Ok(Json(RetentionDebugResponse {
        policy: retention_policy_response(&state.query),
        last_sweep_at: snapshot.last_sweep_at,
        last_result: snapshot.last_result.clone(),
    }))
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
    Query(pagination): Query<PaginationQuery>,
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
    let response =
        load_workflow_history(&state, &tenant_id, &instance_id, &instance.run_id, pagination)
            .await
            .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_runs(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowRunsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let current_instance =
        state.store.get_instance(&tenant_id, &instance_id).await.map_err(internal_error)?;
    let page = resolve_page(&state.query, &pagination);
    let total = state
        .store
        .count_runs_for_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)? as usize;
    let runs = state
        .store
        .list_runs_for_instance_page(
            &tenant_id,
            &instance_id,
            i64::try_from(page.limit).map_err(internal_error_from_display)?,
            i64::try_from(page.offset).map_err(internal_error_from_display)?,
        )
        .await
        .map_err(internal_error)?;
    Ok(Json(WorkflowRunsResponse {
        tenant_id,
        instance_id,
        current_run_id: current_instance.map(|instance| instance.run_id),
        page: build_page_info(&page, total, runs.len()),
        run_count: total,
        runs,
    }))
}

async fn execute_strong_query(
    Path((tenant_id, instance_id, query_name)): Path<(String, String, String)>,
    State(state): State<AppState>,
    Json(request): Json<StrongQueryRequest>,
) -> Result<Json<StrongQueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    let instance = state
        .store
        .get_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| not_found(format!("workflow instance {instance_id} not found")))?;
    let version = instance.definition_version.ok_or_else(|| {
        internal_error(anyhow::anyhow!(
            "workflow instance {instance_id} is missing definition_version"
        ))
    })?;
    let artifact = state
        .store
        .get_artifact_version(&tenant_id, &instance.definition_id, version)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            internal_error(anyhow::anyhow!(
                "workflow artifact {} version {} not found",
                instance.definition_id,
                version
            ))
        })?;

    let (query_state, source) =
        if !instance.status.is_terminal() && instance.artifact_execution.is_some() {
            (instance.clone(), "hot_owner")
        } else {
            let history = read_workflow_history(
                &state.broker,
                "query-service-strong-query",
                &WorkflowHistoryFilter::new(&tenant_id, &instance_id, &instance.run_id),
                Duration::from_millis(HISTORY_IDLE_TIMEOUT_MS),
                Duration::from_millis(HISTORY_MAX_SCAN_MS),
            )
            .await
            .map_err(internal_error)?;
            (replay_compiled_history(&history, &artifact).map_err(internal_error)?, "replay")
        };

    let result = artifact
        .evaluate_query(
            &query_name,
            &request.args,
            query_state.artifact_execution.clone().unwrap_or_default(),
        )
        .map_err(|error| {
            (StatusCode::BAD_REQUEST, Json(ErrorResponse { message: error.to_string() }))
        })?;

    Ok(Json(StrongQueryResponse {
        tenant_id,
        instance_id,
        run_id: query_state.run_id,
        query_name,
        consistency: "strong",
        source,
        result,
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

async fn get_current_workflow_activities(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowActivitiesResponse>, (StatusCode, Json<ErrorResponse>)> {
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
    let response =
        load_workflow_activities(&state, &tenant_id, &instance_id, &instance.run_id, pagination)
            .await
            .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_current_workflow_signals(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    Query(pagination): Query<PaginationQuery>,
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
    let response =
        load_workflow_signals(&state, &tenant_id, &instance_id, &instance.run_id, pagination)
            .await
            .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_activities_for_run(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowActivitiesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_activities(&state, &tenant_id, &instance_id, &run_id, pagination)
        .await
        .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_bulk_batches_for_run(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowBulkBatchesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response =
        load_workflow_bulk_batches(&state, &tenant_id, &instance_id, &run_id, pagination)
            .await
            .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_bulk_batch_for_run(
    Path((tenant_id, instance_id, run_id, batch_id)): Path<(String, String, String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowBulkBatchResponse>, (StatusCode, Json<ErrorResponse>)> {
    let batch = state
        .store
        .get_bulk_batch(&tenant_id, &instance_id, &run_id, &batch_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| not_found(format!("bulk batch {batch_id} not found")))?;
    Ok(Json(WorkflowBulkBatchResponse { tenant_id, instance_id, run_id, batch }))
}

async fn get_workflow_bulk_chunks_for_run(
    Path((tenant_id, instance_id, run_id, batch_id)): Path<(String, String, String, String)>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowBulkChunksResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_bulk_chunks(
        &state,
        &tenant_id,
        &instance_id,
        &run_id,
        &batch_id,
        pagination,
    )
    .await
    .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_bulk_results_for_run(
    Path((tenant_id, instance_id, run_id, batch_id)): Path<(String, String, String, String)>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowBulkResultsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_bulk_results(
        &state,
        &tenant_id,
        &instance_id,
        &run_id,
        &batch_id,
        pagination,
    )
    .await
    .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_signals_for_run(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowSignalsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_signals(&state, &tenant_id, &instance_id, &run_id, pagination)
        .await
        .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_history_for_run(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowHistoryResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_history(&state, &tenant_id, &instance_id, &run_id, pagination)
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

fn resolve_page(config: &QueryRuntimeConfig, pagination: &PaginationQuery) -> ResolvedPage {
    let limit = pagination.limit.unwrap_or(config.default_page_size).min(config.max_page_size);
    ResolvedPage { limit: limit.max(1), offset: pagination.offset.unwrap_or(0) }
}

fn build_page_info(page: &ResolvedPage, total: usize, returned: usize) -> PageInfo {
    let limit = page.limit;
    let offset = page.offset;
    let next_offset = (offset + returned < total).then_some(offset + returned);
    PageInfo { limit, offset, returned, total, has_more: next_offset.is_some(), next_offset }
}

fn retention_policy_response(config: &QueryRuntimeConfig) -> RetentionPolicyResponse {
    RetentionPolicyResponse {
        history_retention_days: config.history_retention_days,
        run_retention_days: config.run_retention_days,
        activity_retention_days: config.activity_retention_days,
        signal_retention_days: config.signal_retention_days,
        snapshot_retention_days: config.snapshot_retention_days,
        retention_sweep_interval_seconds: config.retention_sweep_interval_seconds,
    }
}

fn internal_error(error: anyhow::Error) -> (StatusCode, Json<ErrorResponse>) {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { message: error.to_string() }))
}

fn not_found(message: impl Into<String>) -> (StatusCode, Json<ErrorResponse>) {
    (StatusCode::NOT_FOUND, Json(ErrorResponse { message: message.into() }))
}

fn internal_error_from_display(error: impl std::fmt::Display) -> (StatusCode, Json<ErrorResponse>) {
    internal_error(anyhow::anyhow!("{error}"))
}

async fn load_workflow_history(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    pagination: PaginationQuery,
) -> Result<WorkflowHistoryResponse> {
    let history = read_history(&state.broker, tenant_id, instance_id, run_id).await?;
    let first_event = history.first().ok_or_else(|| {
        anyhow::anyhow!(
            "no workflow history found for tenant={tenant_id}, instance_id={instance_id}, run_id={run_id}"
        )
    })?;
    let page = resolve_page(&state.query, &pagination);
    let total = history.len();
    let events = history.iter().skip(page.offset).take(page.limit).cloned().collect::<Vec<_>>();
    let activity_attempts = state
        .store
        .list_activities_for_run_page(
            tenant_id,
            instance_id,
            run_id,
            i64::try_from(page.limit).context("history page limit exceeds i64")?,
            i64::try_from(page.offset).context("history page offset exceeds i64")?,
        )
        .await?;
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
        event_count: total,
        page: build_page_info(&page, total, events.len()),
        activity_attempt_count: state
            .store
            .count_activities_for_run(tenant_id, instance_id, run_id)
            .await? as usize,
        activity_attempts,
        events,
    })
}

async fn load_workflow_activities(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    pagination: PaginationQuery,
) -> Result<WorkflowActivitiesResponse> {
    let page = resolve_page(&state.query, &pagination);
    let total =
        state.store.count_activities_for_run(tenant_id, instance_id, run_id).await? as usize;
    let activities = state
        .store
        .list_activities_for_run_page(
            tenant_id,
            instance_id,
            run_id,
            i64::try_from(page.limit).context("activities page limit exceeds i64")?,
            i64::try_from(page.offset).context("activities page offset exceeds i64")?,
        )
        .await?;
    Ok(WorkflowActivitiesResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        page: build_page_info(&page, total, activities.len()),
        activity_count: total,
        activities,
    })
}

async fn load_workflow_bulk_batches(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    pagination: PaginationQuery,
) -> Result<WorkflowBulkBatchesResponse> {
    let page = resolve_page(&state.query, &pagination);
    let total = state.store.count_bulk_batches_for_run(tenant_id, instance_id, run_id).await?
        as usize;
    let batches = state
        .store
        .list_bulk_batches_for_run_page(
            tenant_id,
            instance_id,
            run_id,
            i64::try_from(page.limit).context("bulk batch page limit exceeds i64")?,
            i64::try_from(page.offset).context("bulk batch page offset exceeds i64")?,
        )
        .await?;
    Ok(WorkflowBulkBatchesResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        page: build_page_info(&page, total, batches.len()),
        batch_count: total,
        batches,
    })
}

async fn load_workflow_bulk_chunks(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
    pagination: PaginationQuery,
) -> Result<WorkflowBulkChunksResponse> {
    let page = resolve_page(&state.query, &pagination);
    let total = state
        .store
        .count_bulk_chunks_for_batch(tenant_id, instance_id, run_id, batch_id)
        .await? as usize;
    let chunks = state
        .store
        .list_bulk_chunks_for_batch_page(
            tenant_id,
            instance_id,
            run_id,
            batch_id,
            i64::try_from(page.limit).context("bulk chunk page limit exceeds i64")?,
            i64::try_from(page.offset).context("bulk chunk page offset exceeds i64")?,
        )
        .await?;
    Ok(WorkflowBulkChunksResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        batch_id: batch_id.to_owned(),
        page: build_page_info(&page, total, chunks.len()),
        chunk_count: total,
        chunks,
    })
}

async fn load_workflow_bulk_results(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
    pagination: PaginationQuery,
) -> Result<WorkflowBulkResultsResponse> {
    let page = resolve_page(&state.query, &pagination);
    let total = state
        .store
        .count_bulk_chunks_for_batch(tenant_id, instance_id, run_id, batch_id)
        .await? as usize;
    let chunks = state
        .store
        .list_bulk_chunks_for_batch_page(
            tenant_id,
            instance_id,
            run_id,
            batch_id,
            i64::try_from(page.limit).context("bulk result page limit exceeds i64")?,
            i64::try_from(page.offset).context("bulk result page offset exceeds i64")?,
        )
        .await?
        .into_iter()
        .filter(|chunk| chunk.output.is_some())
        .collect::<Vec<_>>();
    Ok(WorkflowBulkResultsResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        batch_id: batch_id.to_owned(),
        page: build_page_info(&page, total, chunks.len()),
        chunk_count: total,
        chunks,
    })
}

async fn load_workflow_signals(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    pagination: PaginationQuery,
) -> Result<WorkflowSignalsResponse> {
    let page = resolve_page(&state.query, &pagination);
    let total = state.store.count_signals_for_run(tenant_id, instance_id, run_id).await? as usize;
    let signals = state
        .store
        .list_signals_for_run_page(
            tenant_id,
            instance_id,
            run_id,
            i64::try_from(page.limit).context("signals page limit exceeds i64")?,
            i64::try_from(page.offset).context("signals page offset exceeds i64")?,
        )
        .await?;
    Ok(WorkflowSignalsResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        page: build_page_info(&page, total, signals.len()),
        signal_count: total,
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
    let activity_attempts = state
        .store
        .list_activities_for_run(&current_instance.tenant_id, &current_instance.instance_id, run_id)
        .await?;
    let mut projection_matches_store = (current_instance.run_id == active_trace.final_state.run_id)
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
    if projection_matches_store.is_some() && !divergences.is_empty() {
        projection_matches_store = Some(false);
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
        activity_attempt_count: activity_attempts.len(),
        activity_attempts,
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

async fn run_retention_sweeper(
    store: WorkflowStore,
    config: QueryRuntimeConfig,
    debug: Arc<Mutex<RetentionDebugState>>,
) {
    loop {
        let now = Utc::now();
        let cutoffs = QueryRetentionCutoffs {
            run_closed_before: config
                .run_retention_days
                .and_then(|days| chrono::Duration::try_days(days as i64))
                .map(|duration| now - duration),
            activity_run_closed_before: config
                .activity_retention_days
                .and_then(|days| chrono::Duration::try_days(days as i64))
                .map(|duration| now - duration),
            signal_run_closed_before: config
                .signal_retention_days
                .and_then(|days| chrono::Duration::try_days(days as i64))
                .map(|duration| now - duration),
            snapshot_run_closed_before: config
                .snapshot_retention_days
                .and_then(|days| chrono::Duration::try_days(days as i64))
                .map(|duration| now - duration),
        };

        if cutoffs != QueryRetentionCutoffs::default() {
            match store.prune_query_retention(&cutoffs).await {
                Ok(result) => {
                    if let Ok(mut state) = debug.lock() {
                        state.last_sweep_at = Some(now);
                        state.last_result = Some(result);
                    }
                }
                Err(error) => {
                    tracing::error!(error = %error, "failed to sweep query retention policy");
                }
            }
        } else if let Ok(mut state) = debug.lock() {
            state.last_sweep_at = Some(now);
            state.last_result = Some(QueryRetentionPruneResult::default());
        }

        tokio::time::sleep(Duration::from_secs(config.retention_sweep_interval_seconds)).await;
    }
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
