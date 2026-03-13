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
    partition_for_key, read_workflow_history,
};
use fabrik_config::{HttpServiceConfig, PostgresConfig, QueryRuntimeConfig, RedpandaConfig};
use fabrik_events::{EventEnvelope, WorkflowEvent};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::{
    QueryRetentionCutoffs, QueryRetentionPruneResult, TaskQueueKind, WorkflowActivityRecord,
    WorkflowActivityStatus, WorkflowArtifactSummary, WorkflowBulkBatchRecord,
    WorkflowBulkBatchStatus, WorkflowBulkChunkRecord, WorkflowChildRecord,
    WorkflowDefinitionSummary, WorkflowInstanceFilters, WorkflowRunFilters, WorkflowRunRecord,
    WorkflowRunSummaryRecord, WorkflowSignalRecord, WorkflowSignalStatus, WorkflowStateSnapshot,
    WorkflowStore, WorkflowUpdateRecord, WorkflowUpdateStatus,
};
use fabrik_throughput::{
    PayloadHandle, PayloadStore, PayloadStoreConfig, PayloadStoreKind, ThroughputBackend,
    throughput_partition_key,
};
use fabrik_workflow::{
    CompiledStateNode, CompiledWorkflowArtifact, ReplayDivergence, ReplayDivergenceKind,
    ReplayFieldMismatch, ReplaySource, ReplayTransitionTraceEntry, SourceLocation,
    WorkflowDefinition, WorkflowInstanceState, artifact_hash, first_transition_divergence,
    projection_mismatches, replay_compiled_history, replay_compiled_history_trace,
    replay_compiled_history_trace_from_snapshot, replay_history_trace,
    replay_history_trace_from_snapshot, same_projection,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, to_value};
use std::{
    collections::{BTreeMap, BTreeSet},
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
    payload_store: PayloadStore,
    client: Client,
    throughput_partitions: i32,
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
    consistency: &'static str,
    authoritative_source: &'static str,
    projection_lag_ms: Option<i64>,
    page: PageInfo,
    batch_count: usize,
    batches: Vec<WorkflowBulkBatchRecord>,
}

#[derive(Debug, Serialize)]
struct WorkflowBulkBatchResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    consistency: &'static str,
    authoritative_source: &'static str,
    projection_lag_ms: Option<i64>,
    batch: WorkflowBulkBatchRecord,
}

#[derive(Debug, Serialize)]
struct WorkflowBulkChunksResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    batch_id: String,
    consistency: &'static str,
    authoritative_source: &'static str,
    projection_lag_ms: Option<i64>,
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
    consistency: &'static str,
    authoritative_source: &'static str,
    projection_lag_ms: Option<i64>,
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
struct WorkflowUpdatesResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    page: PageInfo,
    update_count: usize,
    updates: Vec<WorkflowUpdateRecord>,
}

#[derive(Debug, Serialize)]
struct WorkflowUpdateResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    update: WorkflowUpdateRecord,
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

#[derive(Debug, Serialize)]
struct WorkflowListItem {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    definition_id: String,
    definition_version: Option<u32>,
    artifact_hash: Option<String>,
    workflow_task_queue: String,
    sticky_workflow_build_id: Option<String>,
    sticky_workflow_poller_id: Option<String>,
    routing_status: String,
    current_state: Option<String>,
    status: String,
    event_count: i64,
    last_event_id: Uuid,
    last_event_type: String,
    updated_at: DateTime<Utc>,
    consistency: &'static str,
    source: &'static str,
}

#[derive(Debug, Serialize)]
struct WorkflowListResponse {
    tenant_id: String,
    consistency: &'static str,
    authoritative_source: &'static str,
    page: PageInfo,
    workflow_count: usize,
    items: Vec<WorkflowListItem>,
}

#[derive(Debug, Serialize)]
struct RunListItem {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    definition_id: String,
    definition_version: Option<u32>,
    artifact_hash: Option<String>,
    workflow_task_queue: String,
    sticky_workflow_build_id: Option<String>,
    sticky_workflow_poller_id: Option<String>,
    routing_status: String,
    sticky_updated_at: Option<DateTime<Utc>>,
    previous_run_id: Option<String>,
    next_run_id: Option<String>,
    continue_reason: Option<String>,
    started_at: DateTime<Utc>,
    closed_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
    last_transition_at: DateTime<Utc>,
    status: String,
    current_state: Option<String>,
    last_event_type: Option<String>,
    event_count: Option<i64>,
    consistency: &'static str,
    source: &'static str,
}

#[derive(Debug, Serialize)]
struct RunListResponse {
    tenant_id: String,
    consistency: &'static str,
    authoritative_source: &'static str,
    page: PageInfo,
    run_count: usize,
    items: Vec<RunListItem>,
}

#[derive(Debug, Serialize)]
struct WorkflowDefinitionSummariesResponse {
    tenant_id: String,
    items: Vec<WorkflowDefinitionSummary>,
}

#[derive(Debug, Serialize)]
struct WorkflowArtifactSummariesResponse {
    tenant_id: String,
    items: Vec<WorkflowArtifactSummary>,
}

#[derive(Debug, Serialize)]
struct OverviewTaskQueueSummary {
    queue_kind: String,
    task_queue: String,
    backlog: u64,
    poller_count: usize,
    registered_build_count: usize,
    default_set_id: Option<String>,
    throughput_backend: Option<String>,
    oldest_backlog_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
struct OverviewSummaryResponse {
    tenant_id: String,
    consistency: &'static str,
    authoritative_source: &'static str,
    total_workflows: u64,
    total_workflow_definitions: u64,
    total_workflow_artifacts: u64,
    counts_by_status: BTreeMap<String, u64>,
    total_task_queues: usize,
    total_backlog: u64,
    total_pollers: usize,
    total_registered_builds: usize,
    replay_divergence_count: u64,
    recent_failures: Vec<WorkflowListItem>,
    hottest_task_queues: Vec<OverviewTaskQueueSummary>,
}

#[derive(Debug, Serialize)]
struct WorkflowExecutionGraphNode {
    id: String,
    kind: String,
    label: String,
    status: String,
    subtitle: Option<String>,
}

#[derive(Debug, Serialize)]
struct WorkflowExecutionGraphEdge {
    id: String,
    source: String,
    target: String,
    label: String,
}

#[derive(Debug, Serialize)]
struct WorkflowExecutionGraphResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    consistency: &'static str,
    authoritative_source: &'static str,
    nodes: Vec<WorkflowExecutionGraphNode>,
    edges: Vec<WorkflowExecutionGraphEdge>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphSourceAnchor {
    file: String,
    line: u32,
    column: u32,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphNode {
    id: String,
    graph: String,
    module_id: String,
    state_id: Option<String>,
    kind: String,
    label: String,
    subtitle: Option<String>,
    source_anchor: Option<WorkflowGraphSourceAnchor>,
    next_ids: Vec<String>,
    raw: Value,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphEdge {
    id: String,
    source: String,
    target: String,
    label: String,
    kind: String,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphModule {
    id: String,
    graph: String,
    kind: String,
    label: String,
    subtitle: Option<String>,
    node_ids: Vec<String>,
    state_ids: Vec<String>,
    focus_node_id: String,
    collapsed_by_default: bool,
    source_anchor: Option<WorkflowGraphSourceAnchor>,
    raw: Value,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphOverlayStatus {
    id: String,
    status: String,
    summary: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphBlockedBy {
    kind: String,
    label: String,
    detail: Option<String>,
    node_id: Option<String>,
    module_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphTraceStep {
    id: String,
    occurred_at: DateTime<Utc>,
    lane: String,
    label: String,
    detail: Option<String>,
    event_type: String,
    node_id: Option<String>,
    module_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphActivitySummary {
    node_id: String,
    module_id: String,
    activity_type: String,
    total: usize,
    pending: usize,
    completed: usize,
    failed: usize,
    retrying: usize,
    worker_build_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphBulkSummary {
    node_id: String,
    module_id: String,
    batch_id: String,
    activity_type: String,
    status: String,
    total_items: u32,
    succeeded_items: u32,
    failed_items: u32,
    cancelled_items: u32,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphSignalSummary {
    signal_name: String,
    count: usize,
    latest_status: String,
    latest_seen_at: DateTime<Utc>,
    node_id: Option<String>,
    module_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphUpdateSummary {
    update_name: String,
    count: usize,
    latest_status: String,
    latest_seen_at: DateTime<Utc>,
    module_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphChildSummary {
    child_id: String,
    child_definition_id: String,
    child_workflow_id: String,
    child_run_id: Option<String>,
    status: String,
    node_id: Option<String>,
    module_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphOverlay {
    mode: String,
    run_id: Option<String>,
    current_node_id: Option<String>,
    current_module_id: Option<String>,
    blocked_by: Option<WorkflowGraphBlockedBy>,
    node_statuses: Vec<WorkflowGraphOverlayStatus>,
    module_statuses: Vec<WorkflowGraphOverlayStatus>,
    trace: Vec<WorkflowGraphTraceStep>,
    activity_summaries: Vec<WorkflowGraphActivitySummary>,
    bulk_summaries: Vec<WorkflowGraphBulkSummary>,
    signal_summaries: Vec<WorkflowGraphSignalSummary>,
    update_summaries: Vec<WorkflowGraphUpdateSummary>,
    child_summaries: Vec<WorkflowGraphChildSummary>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphResponse {
    tenant_id: String,
    definition_id: String,
    definition_version: u32,
    artifact_hash: String,
    entrypoint_module: String,
    entrypoint_export: String,
    consistency: &'static str,
    authoritative_source: &'static str,
    source_files: Vec<String>,
    nodes: Vec<WorkflowGraphNode>,
    edges: Vec<WorkflowGraphEdge>,
    modules: Vec<WorkflowGraphModule>,
    module_edges: Vec<WorkflowGraphEdge>,
    overlay: WorkflowGraphOverlay,
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
    consistency: String,
    source: String,
    result: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct InternalQueryRequest {
    #[serde(default)]
    args: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct InternalQueryResponse {
    result: Value,
    consistency: String,
    source: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct PaginationQuery {
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct WorkflowListQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    status: Option<String>,
    definition_id: Option<String>,
    task_queue: Option<String>,
    routing_status: Option<String>,
    updated_after: Option<DateTime<Utc>>,
    q: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RunListQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    status: Option<String>,
    definition_id: Option<String>,
    instance_id: Option<String>,
    run_id: Option<String>,
    task_queue: Option<String>,
    routing_status: Option<String>,
    q: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct SearchableListQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    q: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct BulkReadQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    consistency: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ConsistencyQuery {
    consistency: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadConsistency {
    Eventual,
    Strong,
}

impl ReadConsistency {
    fn as_str(self) -> &'static str {
        match self {
            Self::Eventual => "eventual",
            Self::Strong => "strong",
        }
    }
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
    let payload_store = PayloadStore::from_config(build_payload_store_config(&query)).await?;
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
    .route("/tenants", get(list_tenants))
    .route("/tenants/{tenant_id}/overview", get(get_overview_summary))
    .route("/tenants/{tenant_id}/workflows", get(list_workflows))
    .route("/tenants/{tenant_id}/runs", get(list_runs))
    .route(
        "/tenants/{tenant_id}/workflow-definitions/{definition_id}/latest",
        get(get_latest_workflow_definition),
    )
    .route(
        "/tenants/{tenant_id}/workflow-definitions/{definition_id}/graph",
        get(get_workflow_definition_graph),
    )
    .route(
        "/tenants/{tenant_id}/workflow-definitions",
        get(list_workflow_definition_summaries),
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
        "/tenants/{tenant_id}/workflow-artifacts",
        get(list_workflow_artifact_summaries),
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
        "/tenants/{tenant_id}/workflows/{instance_id}/updates",
        get(get_current_workflow_updates),
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
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/updates",
        get(get_workflow_updates_for_run),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/updates/{update_id}",
        get(get_current_workflow_update),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/updates/{update_id}",
        get(get_workflow_update_for_run),
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
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/graph",
        get(get_workflow_run_graph),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/execution-graph",
        get(get_workflow_execution_graph),
    )
    .with_state(AppState {
        store,
        broker,
        query,
        payload_store,
        client: Client::new(),
        throughput_partitions: redpanda.throughput_partitions,
        retention,
    });

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

async fn list_tenants(
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let tenants = state.store.list_tenants().await.map_err(internal_error)?;
    Ok(Json(json!({ "tenants": tenants })))
}

async fn get_overview_summary(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<OverviewSummaryResponse>, (StatusCode, Json<ErrorResponse>)> {
    let total_workflow_definitions = state
        .store
        .count_workflow_definition_summaries(&tenant_id, None)
        .await
        .map_err(internal_error)?;
    let total_workflow_artifacts = state
        .store
        .count_workflow_artifact_summaries(&tenant_id, None)
        .await
        .map_err(internal_error)?;
    let total_workflows = state
        .store
        .count_instances_with_filters(&tenant_id, &WorkflowInstanceFilters::default())
        .await
        .map_err(internal_error)?;

    let mut counts_by_status = BTreeMap::new();
    for status in ["triggered", "running", "completed", "failed", "cancelled", "terminated"] {
        counts_by_status.insert(
            status.to_owned(),
            state
                .store
                .count_instances_with_filters(
                    &tenant_id,
                    &WorkflowInstanceFilters {
                        status: Some(status.to_owned()),
                        ..WorkflowInstanceFilters::default()
                    },
                )
                .await
                .map_err(internal_error)?,
        );
    }

    let recent_failure_states = state
        .store
        .list_instances_page_with_filters(
            &tenant_id,
            &WorkflowInstanceFilters {
                status: Some("failed".to_owned()),
                ..WorkflowInstanceFilters::default()
            },
            6,
            0,
        )
        .await
        .map_err(internal_error)?;
    let mut recent_failures = Vec::with_capacity(recent_failure_states.len());
    for workflow in recent_failure_states {
        recent_failures.push(
            workflow_list_item_from_state(&state.store, workflow).await.map_err(internal_error)?,
        );
    }

    let queue_refs =
        state.store.list_task_queue_refs_for_tenant(&tenant_id).await.map_err(internal_error)?;
    let mut task_queues = Vec::with_capacity(queue_refs.len());
    let mut total_backlog = 0_u64;
    let mut total_pollers = 0_usize;
    let mut total_registered_builds = 0_usize;
    for queue_ref in queue_refs {
        let inspection = state
            .store
            .inspect_task_queue(
                &tenant_id,
                queue_ref.queue_kind.clone(),
                &queue_ref.task_queue,
                Utc::now(),
            )
            .await
            .map_err(internal_error)?;
        total_backlog += inspection.backlog;
        total_pollers += inspection.pollers.len();
        total_registered_builds += inspection.registered_builds.len();
        task_queues.push(OverviewTaskQueueSummary {
            queue_kind: queue_kind_label(&inspection.queue_kind).to_owned(),
            task_queue: inspection.task_queue,
            backlog: inspection.backlog,
            poller_count: inspection.pollers.len(),
            registered_build_count: inspection.registered_builds.len(),
            default_set_id: inspection.default_set_id,
            throughput_backend: inspection.throughput_policy.map(|policy| policy.backend),
            oldest_backlog_at: inspection.oldest_backlog_at,
        });
    }
    task_queues.sort_by(|left, right| {
        right
            .backlog
            .cmp(&left.backlog)
            .then_with(|| left.queue_kind.cmp(&right.queue_kind))
            .then_with(|| left.task_queue.cmp(&right.task_queue))
    });

    Ok(Json(OverviewSummaryResponse {
        tenant_id,
        consistency: "eventual",
        authoritative_source: "projection",
        total_workflows,
        total_workflow_definitions,
        total_workflow_artifacts,
        counts_by_status,
        total_task_queues: task_queues.len(),
        total_backlog,
        total_pollers,
        total_registered_builds,
        replay_divergence_count: 0,
        recent_failures,
        hottest_task_queues: task_queues.into_iter().take(6).collect(),
    }))
}

async fn list_workflows(
    Path(tenant_id): Path<String>,
    Query(query): Query<WorkflowListQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowListResponse>, (StatusCode, Json<ErrorResponse>)> {
    let page =
        resolve_page(&state.query, &PaginationQuery { limit: query.limit, offset: query.offset });
    let filters = WorkflowInstanceFilters {
        status: query.status.and_then(non_empty),
        definition_id: query.definition_id.and_then(non_empty),
        task_queue: query.task_queue.and_then(non_empty),
        updated_after: query.updated_after,
        query: query.q.and_then(non_empty),
    };
    let total = state
        .store
        .count_instances_with_filters(&tenant_id, &filters)
        .await
        .map_err(internal_error)? as usize;
    if let Some(routing_status) = query.routing_status.and_then(non_empty) {
        let (items, filtered_total) = filtered_workflow_list_items(
            &state.store,
            &tenant_id,
            &filters,
            &page,
            &routing_status,
        )
        .await
        .map_err(internal_error)?;
        return Ok(Json(WorkflowListResponse {
            tenant_id,
            consistency: "eventual",
            authoritative_source: "projection",
            page: build_page_info(&page, filtered_total, items.len()),
            workflow_count: filtered_total,
            items,
        }));
    }
    let workflows = state
        .store
        .list_instances_page_with_filters(
            &tenant_id,
            &filters,
            i64::try_from(page.limit).map_err(internal_error_from_display)?,
            i64::try_from(page.offset).map_err(internal_error_from_display)?,
        )
        .await
        .map_err(internal_error)?;
    let mut items = Vec::with_capacity(workflows.len());
    for workflow in workflows {
        items.push(
            workflow_list_item_from_state(&state.store, workflow).await.map_err(internal_error)?,
        );
    }

    Ok(Json(WorkflowListResponse {
        tenant_id,
        consistency: "eventual",
        authoritative_source: "projection",
        page: build_page_info(&page, total, items.len()),
        workflow_count: total,
        items,
    }))
}

async fn list_runs(
    Path(tenant_id): Path<String>,
    Query(query): Query<RunListQuery>,
    State(state): State<AppState>,
) -> Result<Json<RunListResponse>, (StatusCode, Json<ErrorResponse>)> {
    let page =
        resolve_page(&state.query, &PaginationQuery { limit: query.limit, offset: query.offset });
    let filters = WorkflowRunFilters {
        status: query.status.and_then(non_empty),
        definition_id: query.definition_id.and_then(non_empty),
        instance_id: query.instance_id.and_then(non_empty),
        run_id: query.run_id.and_then(non_empty),
        task_queue: query.task_queue.and_then(non_empty),
        query: query.q.and_then(non_empty),
    };
    let total =
        state.store.count_runs_with_filters(&tenant_id, &filters).await.map_err(internal_error)?
            as usize;
    if let Some(routing_status) = query.routing_status.and_then(non_empty) {
        let (items, filtered_total) =
            filtered_run_list_items(&state.store, &tenant_id, &filters, &page, &routing_status)
                .await
                .map_err(internal_error)?;
        return Ok(Json(RunListResponse {
            tenant_id,
            consistency: "eventual",
            authoritative_source: "projection",
            page: build_page_info(&page, filtered_total, items.len()),
            run_count: filtered_total,
            items,
        }));
    }
    let runs = state
        .store
        .list_runs_page_with_filters(
            &tenant_id,
            &filters,
            i64::try_from(page.limit).map_err(internal_error_from_display)?,
            i64::try_from(page.offset).map_err(internal_error_from_display)?,
        )
        .await
        .map_err(internal_error)?;
    let mut items = Vec::with_capacity(runs.len());
    for run in runs {
        items.push(run_list_item_from_record(&state.store, run).await.map_err(internal_error)?);
    }

    Ok(Json(RunListResponse {
        tenant_id,
        consistency: "eventual",
        authoritative_source: "projection",
        page: build_page_info(&page, total, items.len()),
        run_count: total,
        items,
    }))
}

async fn list_workflow_definition_summaries(
    Path(tenant_id): Path<String>,
    Query(query): Query<SearchableListQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowDefinitionSummariesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let page =
        resolve_page(&state.query, &PaginationQuery { limit: query.limit, offset: query.offset });
    let items = state
        .store
        .list_workflow_definition_summaries(
            &tenant_id,
            query.q.as_deref().and_then(non_empty_ref),
            i64::try_from(page.limit).map_err(internal_error_from_display)?,
            i64::try_from(page.offset).map_err(internal_error_from_display)?,
        )
        .await
        .map_err(internal_error)?;
    Ok(Json(WorkflowDefinitionSummariesResponse { tenant_id, items }))
}

async fn list_workflow_artifact_summaries(
    Path(tenant_id): Path<String>,
    Query(query): Query<SearchableListQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowArtifactSummariesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let page =
        resolve_page(&state.query, &PaginationQuery { limit: query.limit, offset: query.offset });
    let items = state
        .store
        .list_workflow_artifact_summaries(
            &tenant_id,
            query.q.as_deref().and_then(non_empty_ref),
            i64::try_from(page.limit).map_err(internal_error_from_display)?,
            i64::try_from(page.offset).map_err(internal_error_from_display)?,
        )
        .await
        .map_err(internal_error)?;
    Ok(Json(WorkflowArtifactSummariesResponse { tenant_id, items }))
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

    if !instance.status.is_terminal() && instance.artifact_execution.is_some() {
        if let Some(owner_response) =
            try_owner_strong_query(&state, &tenant_id, &instance_id, &query_name, &request.args)
                .await
        {
            return Ok(Json(StrongQueryResponse {
                tenant_id,
                instance_id,
                run_id: instance.run_id,
                query_name,
                consistency: owner_response.consistency,
                source: owner_response.source,
                result: owner_response.result,
            }));
        }
    }

    let (query_state, source) =
        if !instance.status.is_terminal() && instance.artifact_execution.is_some() {
            (instance.clone(), "hot_owner_fallback")
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
        consistency: "strong".to_owned(),
        source: source.to_owned(),
        result,
    }))
}

async fn try_owner_strong_query(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    query_name: &str,
    args: &Value,
) -> Option<InternalQueryResponse> {
    for base_url in owner_query_endpoints(&state.query) {
        if let Some(payload) = try_owner_strong_query_endpoint(
            &state.client,
            &base_url,
            tenant_id,
            instance_id,
            query_name,
            args,
        )
        .await
        {
            return Some(payload);
        }
    }
    None
}

async fn try_owner_strong_query_endpoint(
    client: &Client,
    base_url: &str,
    tenant_id: &str,
    instance_id: &str,
    query_name: &str,
    args: &Value,
) -> Option<InternalQueryResponse> {
    let url = format!(
        "{}/internal/workflows/{}/{}/queries/{}",
        base_url.trim_end_matches('/'),
        tenant_id,
        instance_id,
        query_name,
    );
    let response =
        match client.post(&url).json(&InternalQueryRequest { args: args.clone() }).send().await {
            Ok(response) => response,
            Err(_) => return None,
        };
    if !response.status().is_success() {
        return None;
    }
    response.json::<InternalQueryResponse>().await.ok()
}

fn owner_query_endpoints(config: &QueryRuntimeConfig) -> Vec<String> {
    let mut endpoints = Vec::new();
    if let Some(url) = config.strong_query_unified_url.as_ref() {
        endpoints.push(url.clone());
    }
    if let Some(url) = config.strong_query_executor_url.as_ref() {
        endpoints.push(url.clone());
    }
    endpoints
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

async fn get_current_workflow_updates(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowUpdatesResponse>, (StatusCode, Json<ErrorResponse>)> {
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
        load_workflow_updates(&state, &tenant_id, &instance_id, &instance.run_id, pagination)
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
    Query(query): Query<BulkReadQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowBulkBatchesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_bulk_batches(
        &state,
        &tenant_id,
        &instance_id,
        &run_id,
        PaginationQuery { limit: query.limit, offset: query.offset },
        parse_read_consistency(query.consistency.as_deref()).map_err(invalid_request)?,
    )
    .await
    .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_bulk_batch_for_run(
    Path((tenant_id, instance_id, run_id, batch_id)): Path<(String, String, String, String)>,
    Query(query): Query<ConsistencyQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowBulkBatchResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_bulk_batch(
        &state,
        &tenant_id,
        &instance_id,
        &run_id,
        &batch_id,
        parse_read_consistency(query.consistency.as_deref()).map_err(invalid_request)?,
    )
    .await
    .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_bulk_chunks_for_run(
    Path((tenant_id, instance_id, run_id, batch_id)): Path<(String, String, String, String)>,
    Query(query): Query<BulkReadQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowBulkChunksResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_bulk_chunks(
        &state,
        &tenant_id,
        &instance_id,
        &run_id,
        &batch_id,
        PaginationQuery { limit: query.limit, offset: query.offset },
        parse_read_consistency(query.consistency.as_deref()).map_err(invalid_request)?,
    )
    .await
    .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_bulk_results_for_run(
    Path((tenant_id, instance_id, run_id, batch_id)): Path<(String, String, String, String)>,
    Query(query): Query<BulkReadQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowBulkResultsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_bulk_results(
        &state,
        &tenant_id,
        &instance_id,
        &run_id,
        &batch_id,
        PaginationQuery { limit: query.limit, offset: query.offset },
        parse_read_consistency(query.consistency.as_deref()).map_err(invalid_request)?,
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

async fn get_workflow_updates_for_run(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowUpdatesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_updates(&state, &tenant_id, &instance_id, &run_id, pagination)
        .await
        .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_current_workflow_update(
    Path((tenant_id, instance_id, update_id)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowUpdateResponse>, (StatusCode, Json<ErrorResponse>)> {
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
        load_workflow_update(&state, &tenant_id, &instance_id, &instance.run_id, &update_id)
            .await?;
    Ok(Json(response))
}

async fn get_workflow_update_for_run(
    Path((tenant_id, instance_id, run_id, update_id)): Path<(String, String, String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowUpdateResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response =
        load_workflow_update(&state, &tenant_id, &instance_id, &run_id, &update_id).await?;
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

async fn get_workflow_definition_graph(
    Path((tenant_id, definition_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowGraphResponse>, (StatusCode, Json<ErrorResponse>)> {
    let artifact = state
        .store
        .get_latest_artifact(&tenant_id, &definition_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            not_found(format!("workflow artifact {definition_id} not found for tenant {tenant_id}"))
        })?;
    Ok(Json(build_workflow_graph_response(tenant_id, &artifact, build_artifact_overlay())))
}

async fn get_workflow_run_graph(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowGraphResponse>, (StatusCode, Json<ErrorResponse>)> {
    let run = state
        .store
        .get_run_record(&tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| not_found(format!("workflow run {run_id} not found")))?;
    let artifact = load_artifact_for_run(&state, &tenant_id, &run)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
        not_found(format!(
            "workflow artifact {} version {:?} not found for tenant {}",
            run.definition_id, run.definition_version, tenant_id
        ))
    })?;
    let current_instance =
        state.store.get_instance(&tenant_id, &instance_id).await.map_err(internal_error)?;
    let history = read_history(&state.broker, &tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?;
    let activities = state
        .store
        .list_activities_for_run(&tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?;
    let signals = state
        .store
        .list_signals_for_run(&tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?;
    let updates = state
        .store
        .list_updates_for_run(&tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?;
    let children = state
        .store
        .list_children_for_run(&tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?;
    let bulk_batches = state
        .store
        .list_bulk_batches_for_run_page(&tenant_id, &instance_id, &run_id, i64::MAX, 0)
        .await
        .map_err(internal_error)?;
    let overlay = build_run_overlay(
        &artifact,
        &run_id,
        current_instance
            .as_ref()
            .filter(|instance| instance.run_id == run_id)
            .and_then(|instance| instance.current_state.clone()),
        &history,
        &activities,
        &signals,
        &updates,
        &children,
        &bulk_batches,
    );
    Ok(Json(build_workflow_graph_response(tenant_id, &artifact, overlay)))
}

async fn get_workflow_execution_graph(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowExecutionGraphResponse>, (StatusCode, Json<ErrorResponse>)> {
    let instance = state
        .store
        .get_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| not_found(format!("workflow instance {instance_id} not found")))?;
    let run_id = instance.run_id.clone();
    let run = state
        .store
        .get_run_record(&tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| not_found(format!("workflow run {run_id} not found")))?;
    let activities = state
        .store
        .list_activities_for_run(&tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?;
    let signals = state
        .store
        .list_signals_for_run(&tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?;
    let children = state
        .store
        .list_children_for_run(&tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?;
    let bulk_batches = state
        .store
        .list_bulk_batches_for_run_page(&tenant_id, &instance_id, &run_id, i64::MAX, 0)
        .await
        .map_err(internal_error)?;
    let runs = state
        .store
        .list_runs_for_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?;

    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    let workflow_node_id = format!("workflow:{instance_id}:{run_id}");
    nodes.push(WorkflowExecutionGraphNode {
        id: workflow_node_id.clone(),
        kind: "workflow".to_owned(),
        label: instance.definition_id.clone(),
        status: instance.status.as_str().to_owned(),
        subtitle: instance.current_state.clone(),
    });

    let queue_node_id = format!("queue:workflow:{}", run.workflow_task_queue);
    nodes.push(WorkflowExecutionGraphNode {
        id: queue_node_id.clone(),
        kind: "workflow_queue".to_owned(),
        label: run.workflow_task_queue.clone(),
        status: "active".to_owned(),
        subtitle: Some("workflow task queue".to_owned()),
    });
    edges.push(WorkflowExecutionGraphEdge {
        id: format!("{queue_node_id}->{workflow_node_id}"),
        source: queue_node_id.clone(),
        target: workflow_node_id.clone(),
        label: "dispatches".to_owned(),
    });

    if let Some(build_id) = instance.sticky_workflow_build_id.clone() {
        let build_node_id = format!("build:{build_id}");
        nodes.push(WorkflowExecutionGraphNode {
            id: build_node_id.clone(),
            kind: "workflow_build".to_owned(),
            label: build_id,
            status: "sticky".to_owned(),
            subtitle: Some("sticky build".to_owned()),
        });
        edges.push(WorkflowExecutionGraphEdge {
            id: format!("{build_node_id}->{workflow_node_id}"),
            source: build_node_id.clone(),
            target: workflow_node_id.clone(),
            label: "routes".to_owned(),
        });
        if let Some(poller_id) = instance.sticky_workflow_poller_id.clone() {
            let poller_node_id = format!("poller:{poller_id}");
            nodes.push(WorkflowExecutionGraphNode {
                id: poller_node_id.clone(),
                kind: "workflow_poller".to_owned(),
                label: poller_id,
                status: "active".to_owned(),
                subtitle: Some("sticky poller".to_owned()),
            });
            edges.push(WorkflowExecutionGraphEdge {
                id: format!("{poller_node_id}->{build_node_id}"),
                source: poller_node_id,
                target: build_node_id,
                label: "advertises".to_owned(),
            });
        }
    }

    for activity in activities {
        let activity_node_id = format!("activity:{}:{}", activity.activity_id, activity.attempt);
        nodes.push(activity_graph_node(&activity_node_id, &activity));
        edges.push(WorkflowExecutionGraphEdge {
            id: format!("{workflow_node_id}->{activity_node_id}"),
            source: workflow_node_id.clone(),
            target: activity_node_id.clone(),
            label: activity.activity_type.clone(),
        });
        let queue_id = format!("queue:activity:{}", activity.task_queue);
        nodes.push(WorkflowExecutionGraphNode {
            id: queue_id.clone(),
            kind: "activity_queue".to_owned(),
            label: activity.task_queue.clone(),
            status: "active".to_owned(),
            subtitle: Some("activity task queue".to_owned()),
        });
        edges.push(WorkflowExecutionGraphEdge {
            id: format!("{activity_node_id}->{queue_id}"),
            source: activity_node_id,
            target: queue_id,
            label: "queued_on".to_owned(),
        });
    }

    for signal in signals {
        let signal_node_id = format!("signal:{}", signal.signal_id);
        nodes.push(WorkflowExecutionGraphNode {
            id: signal_node_id.clone(),
            kind: "signal".to_owned(),
            label: signal.signal_type.clone(),
            status: signal_status_label(&signal),
            subtitle: Some(signal.signal_id.clone()),
        });
        edges.push(WorkflowExecutionGraphEdge {
            id: format!("{signal_node_id}->{workflow_node_id}"),
            source: signal_node_id,
            target: workflow_node_id.clone(),
            label: "delivers".to_owned(),
        });
    }

    for child in children {
        let child_node_id = format!("child:{}", child.child_id);
        nodes.push(child_graph_node(&child_node_id, &child));
        edges.push(WorkflowExecutionGraphEdge {
            id: format!("{workflow_node_id}->{child_node_id}"),
            source: workflow_node_id.clone(),
            target: child_node_id,
            label: "starts".to_owned(),
        });
    }

    for batch in bulk_batches {
        let batch_node_id = format!("bulk:{}", batch.batch_id);
        nodes.push(WorkflowExecutionGraphNode {
            id: batch_node_id.clone(),
            kind: "bulk_batch".to_owned(),
            label: batch.activity_type.clone(),
            status: batch.status.as_str().to_owned(),
            subtitle: Some(format!("{} items / {} chunks", batch.total_items, batch.chunk_count)),
        });
        edges.push(WorkflowExecutionGraphEdge {
            id: format!("{workflow_node_id}->{batch_node_id}"),
            source: workflow_node_id.clone(),
            target: batch_node_id,
            label: "bulk fan-out".to_owned(),
        });
    }

    for run_record in runs {
        let run_node_id = format!("run:{}", run_record.run_id);
        nodes.push(WorkflowExecutionGraphNode {
            id: run_node_id.clone(),
            kind: "run".to_owned(),
            label: run_record.run_id.clone(),
            status: if run_record.closed_at.is_some() { "closed" } else { "open" }.to_owned(),
            subtitle: run_record.continue_reason.clone(),
        });
        if run_record.run_id == run_id {
            edges.push(WorkflowExecutionGraphEdge {
                id: format!("{run_node_id}->{workflow_node_id}"),
                source: run_node_id.clone(),
                target: workflow_node_id.clone(),
                label: "active".to_owned(),
            });
        }
        if let Some(previous_run_id) = run_record.previous_run_id.clone() {
            edges.push(WorkflowExecutionGraphEdge {
                id: format!("run:{previous_run_id}->{run_node_id}"),
                source: format!("run:{previous_run_id}"),
                target: run_node_id.clone(),
                label: "continue-as-new".to_owned(),
            });
        }
    }

    dedupe_graph_nodes(&mut nodes);
    dedupe_graph_edges(&mut edges);

    Ok(Json(WorkflowExecutionGraphResponse {
        tenant_id,
        instance_id,
        run_id,
        consistency: "eventual",
        authoritative_source: "projection",
        nodes,
        edges,
    }))
}

async fn load_artifact_for_run(
    state: &AppState,
    tenant_id: &str,
    run: &WorkflowRunRecord,
) -> Result<Option<CompiledWorkflowArtifact>> {
    if let Some(version) = run.definition_version {
        if let Some(artifact) =
            state.store.get_artifact_version(tenant_id, &run.definition_id, version).await?
        {
            return Ok(Some(artifact));
        }
    }
    state.store.get_latest_artifact(tenant_id, &run.definition_id).await
}

fn build_workflow_graph_response(
    tenant_id: String,
    artifact: &CompiledWorkflowArtifact,
    overlay: WorkflowGraphOverlay,
) -> WorkflowGraphResponse {
    let projection = normalize_artifact_graph(artifact);
    WorkflowGraphResponse {
        tenant_id,
        definition_id: artifact.definition_id.clone(),
        definition_version: artifact.definition_version,
        artifact_hash: artifact.artifact_hash.clone(),
        entrypoint_module: artifact.entrypoint.module.clone(),
        entrypoint_export: artifact.entrypoint.export.clone(),
        consistency: "eventual",
        authoritative_source: "artifact+projection",
        source_files: artifact.source_files.clone(),
        nodes: projection.nodes,
        edges: projection.edges,
        modules: projection.modules,
        module_edges: projection.module_edges,
        overlay,
    }
}

struct WorkflowGraphProjection {
    nodes: Vec<WorkflowGraphNode>,
    edges: Vec<WorkflowGraphEdge>,
    modules: Vec<WorkflowGraphModule>,
    module_edges: Vec<WorkflowGraphEdge>,
}

fn normalize_artifact_graph(artifact: &CompiledWorkflowArtifact) -> WorkflowGraphProjection {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    let mut modules = Vec::new();

    let entry_node_id = workflow_entry_node_id();
    let entry_module_id = workflow_entry_module_id();
    nodes.push(WorkflowGraphNode {
        id: entry_node_id.clone(),
        graph: "workflow".to_owned(),
        module_id: entry_module_id.clone(),
        state_id: None,
        kind: "entry".to_owned(),
        label: "Entry".to_owned(),
        subtitle: Some(format!("{}::{}", artifact.entrypoint.module, artifact.entrypoint.export)),
        source_anchor: None,
        next_ids: vec![workflow_state_node_id(&artifact.workflow.initial_state)],
        raw: json!({
            "type": "entry",
            "module": artifact.entrypoint.module,
            "export": artifact.entrypoint.export,
            "initial_state": artifact.workflow.initial_state,
        }),
    });
    modules.push(WorkflowGraphModule {
        id: entry_module_id.clone(),
        graph: "workflow".to_owned(),
        kind: "entry".to_owned(),
        label: "Entry".to_owned(),
        subtitle: Some(format!("{}::{}", artifact.entrypoint.module, artifact.entrypoint.export)),
        node_ids: vec![entry_node_id.clone()],
        state_ids: Vec::new(),
        focus_node_id: entry_node_id.clone(),
        collapsed_by_default: false,
        source_anchor: None,
        raw: json!({
            "entrypoint": artifact.entrypoint,
            "initial_state": artifact.workflow.initial_state,
        }),
    });
    edges.push(WorkflowGraphEdge {
        id: format!(
            "{entry_node_id}->{}",
            workflow_state_node_id(&artifact.workflow.initial_state)
        ),
        source: entry_node_id,
        target: workflow_state_node_id(&artifact.workflow.initial_state),
        label: "starts".to_owned(),
        kind: "entry".to_owned(),
    });

    for (state_id, state) in &artifact.workflow.states {
        let node_id = workflow_state_node_id(state_id);
        let module_id = workflow_module_id(state_id);
        let next_ids = state_transition_targets(state)
            .into_iter()
            .map(|(_, target)| workflow_state_node_id(&target))
            .collect::<Vec<_>>();
        let raw = to_value(state).unwrap_or(Value::Null);
        let subtitle = compiled_state_subtitle(state);
        let source_anchor = resolve_source_anchor(
            &artifact.source_map,
            &[state_id.clone(), format!("workflow:{state_id}"), format!("state:{state_id}")],
        );
        nodes.push(WorkflowGraphNode {
            id: node_id.clone(),
            graph: "workflow".to_owned(),
            module_id: module_id.clone(),
            state_id: Some(state_id.clone()),
            kind: compiled_state_kind(state).to_owned(),
            label: state_id.clone(),
            subtitle: subtitle.clone(),
            source_anchor: source_anchor.clone(),
            next_ids,
            raw: raw.clone(),
        });
        modules.push(WorkflowGraphModule {
            id: module_id.clone(),
            graph: "workflow".to_owned(),
            kind: semantic_module_kind("workflow", state).to_owned(),
            label: state_id.clone(),
            subtitle,
            node_ids: vec![node_id.clone()],
            state_ids: vec![state_id.clone()],
            focus_node_id: node_id.clone(),
            collapsed_by_default: module_collapsed_by_default(state),
            source_anchor,
            raw,
        });
        for (label, target) in state_transition_targets(state) {
            edges.push(WorkflowGraphEdge {
                id: format!("{node_id}->{}", workflow_state_node_id(&target)),
                source: node_id.clone(),
                target: workflow_state_node_id(&target),
                label,
                kind: "transition".to_owned(),
            });
        }
    }

    if !artifact.signals.is_empty() || !artifact.updates.is_empty() || !artifact.queries.is_empty()
    {
        let handlers_node_id = handlers_hub_node_id();
        let handlers_module_id = handlers_hub_module_id();
        nodes.push(WorkflowGraphNode {
            id: handlers_node_id.clone(),
            graph: "system".to_owned(),
            module_id: handlers_module_id.clone(),
            state_id: None,
            kind: "handlers".to_owned(),
            label: "Handlers".to_owned(),
            subtitle: Some("signals, updates, queries".to_owned()),
            source_anchor: None,
            next_ids: Vec::new(),
            raw: json!({
                "signals": artifact.signals.keys().collect::<Vec<_>>(),
                "updates": artifact.updates.keys().collect::<Vec<_>>(),
                "queries": artifact.queries.keys().collect::<Vec<_>>(),
            }),
        });
        modules.push(WorkflowGraphModule {
            id: handlers_module_id.clone(),
            graph: "system".to_owned(),
            kind: "handlers".to_owned(),
            label: "Handlers".to_owned(),
            subtitle: Some("signals, updates, queries".to_owned()),
            node_ids: vec![handlers_node_id.clone()],
            state_ids: Vec::new(),
            focus_node_id: handlers_node_id.clone(),
            collapsed_by_default: false,
            source_anchor: None,
            raw: json!({
                "signals": artifact.signals.keys().collect::<Vec<_>>(),
                "updates": artifact.updates.keys().collect::<Vec<_>>(),
                "queries": artifact.queries.keys().collect::<Vec<_>>(),
            }),
        });
        edges.push(WorkflowGraphEdge {
            id: format!("{}->{}", workflow_entry_node_id(), handlers_node_id.clone()),
            source: workflow_entry_node_id(),
            target: handlers_node_id.clone(),
            label: "messages".to_owned(),
            kind: "system".to_owned(),
        });

        for (signal_name, handler) in &artifact.signals {
            let module_id = signal_handler_module_id(signal_name);
            let mut node_ids = Vec::new();
            let mut state_ids = Vec::new();
            for (state_id, state) in &handler.states {
                let node_id = signal_handler_node_id(signal_name, state_id);
                let source_anchor = resolve_source_anchor(
                    &artifact.source_map,
                    &[
                        format!("signal:{signal_name}:{state_id}"),
                        state_id.clone(),
                        format!("state:{state_id}"),
                    ],
                );
                let raw = to_value(state).unwrap_or(Value::Null);
                nodes.push(WorkflowGraphNode {
                    id: node_id.clone(),
                    graph: "signal_handler".to_owned(),
                    module_id: module_id.clone(),
                    state_id: Some(state_id.clone()),
                    kind: compiled_state_kind(state).to_owned(),
                    label: state_id.clone(),
                    subtitle: compiled_state_subtitle(state),
                    source_anchor,
                    next_ids: state_transition_targets(state)
                        .into_iter()
                        .map(|(_, target)| signal_handler_node_id(signal_name, &target))
                        .collect(),
                    raw,
                });
                node_ids.push(node_id.clone());
                state_ids.push(state_id.clone());
                for (label, target) in state_transition_targets(state) {
                    edges.push(WorkflowGraphEdge {
                        id: format!("{node_id}->{}", signal_handler_node_id(signal_name, &target)),
                        source: node_id.clone(),
                        target: signal_handler_node_id(signal_name, &target),
                        label,
                        kind: "handler_transition".to_owned(),
                    });
                }
            }
            let entry_node_id = signal_handler_node_id(signal_name, &handler.initial_state);
            modules.push(WorkflowGraphModule {
                id: module_id.clone(),
                graph: "signal_handler".to_owned(),
                kind: "signal_handler".to_owned(),
                label: signal_name.clone(),
                subtitle: Some("signal handler".to_owned()),
                node_ids,
                state_ids,
                focus_node_id: entry_node_id.clone(),
                collapsed_by_default: true,
                source_anchor: resolve_source_anchor(
                    &artifact.source_map,
                    &[format!("signal:{signal_name}"), signal_name.clone()],
                ),
                raw: json!({
                    "name": signal_name,
                    "arg_name": handler.arg_name,
                    "initial_state": handler.initial_state,
                }),
            });
            edges.push(WorkflowGraphEdge {
                id: format!("{handlers_node_id}->{entry_node_id}"),
                source: handlers_node_id.clone(),
                target: entry_node_id,
                label: format!("signal {signal_name}"),
                kind: "handler".to_owned(),
            });
        }

        for (update_name, handler) in &artifact.updates {
            let module_id = update_handler_module_id(update_name);
            let mut node_ids = Vec::new();
            let mut state_ids = Vec::new();
            for (state_id, state) in &handler.states {
                let node_id = update_handler_node_id(update_name, state_id);
                let source_anchor = resolve_source_anchor(
                    &artifact.source_map,
                    &[
                        format!("update:{update_name}:{state_id}"),
                        state_id.clone(),
                        format!("state:{state_id}"),
                    ],
                );
                let raw = to_value(state).unwrap_or(Value::Null);
                nodes.push(WorkflowGraphNode {
                    id: node_id.clone(),
                    graph: "update_handler".to_owned(),
                    module_id: module_id.clone(),
                    state_id: Some(state_id.clone()),
                    kind: compiled_state_kind(state).to_owned(),
                    label: state_id.clone(),
                    subtitle: compiled_state_subtitle(state),
                    source_anchor,
                    next_ids: state_transition_targets(state)
                        .into_iter()
                        .map(|(_, target)| update_handler_node_id(update_name, &target))
                        .collect(),
                    raw,
                });
                node_ids.push(node_id.clone());
                state_ids.push(state_id.clone());
                for (label, target) in state_transition_targets(state) {
                    edges.push(WorkflowGraphEdge {
                        id: format!("{node_id}->{}", update_handler_node_id(update_name, &target)),
                        source: node_id.clone(),
                        target: update_handler_node_id(update_name, &target),
                        label,
                        kind: "handler_transition".to_owned(),
                    });
                }
            }
            let entry_node_id = update_handler_node_id(update_name, &handler.initial_state);
            modules.push(WorkflowGraphModule {
                id: module_id.clone(),
                graph: "update_handler".to_owned(),
                kind: "update_handler".to_owned(),
                label: update_name.clone(),
                subtitle: Some("update handler".to_owned()),
                node_ids,
                state_ids,
                focus_node_id: entry_node_id.clone(),
                collapsed_by_default: true,
                source_anchor: resolve_source_anchor(
                    &artifact.source_map,
                    &[format!("update:{update_name}"), update_name.clone()],
                ),
                raw: json!({
                    "name": update_name,
                    "arg_name": handler.arg_name,
                    "initial_state": handler.initial_state,
                }),
            });
            edges.push(WorkflowGraphEdge {
                id: format!("{handlers_node_id}->{entry_node_id}"),
                source: handlers_node_id.clone(),
                target: entry_node_id,
                label: format!("update {update_name}"),
                kind: "handler".to_owned(),
            });
        }

        for (query_name, handler) in &artifact.queries {
            let node_id = query_node_id(query_name);
            let module_id = query_module_id(query_name);
            let source_anchor = resolve_source_anchor(
                &artifact.source_map,
                &[format!("query:{query_name}"), query_name.clone()],
            );
            let raw = to_value(handler).unwrap_or(Value::Null);
            nodes.push(WorkflowGraphNode {
                id: node_id.clone(),
                graph: "query_handler".to_owned(),
                module_id: module_id.clone(),
                state_id: None,
                kind: "query".to_owned(),
                label: query_name.clone(),
                subtitle: Some("query".to_owned()),
                source_anchor: source_anchor.clone(),
                next_ids: Vec::new(),
                raw: raw.clone(),
            });
            modules.push(WorkflowGraphModule {
                id: module_id,
                graph: "query_handler".to_owned(),
                kind: "query_handler".to_owned(),
                label: query_name.clone(),
                subtitle: Some("query".to_owned()),
                node_ids: vec![node_id.clone()],
                state_ids: Vec::new(),
                focus_node_id: node_id.clone(),
                collapsed_by_default: true,
                source_anchor,
                raw,
            });
            edges.push(WorkflowGraphEdge {
                id: format!("{handlers_node_id}->{node_id}"),
                source: handlers_node_id.clone(),
                target: node_id,
                label: format!("query {query_name}"),
                kind: "handler".to_owned(),
            });
        }
    }

    let module_lookup = nodes
        .iter()
        .map(|node| (node.id.clone(), node.module_id.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut seen_module_edges = BTreeSet::new();
    let mut module_edges = Vec::new();
    for edge in &edges {
        let Some(source_module) = module_lookup.get(&edge.source) else {
            continue;
        };
        let Some(target_module) = module_lookup.get(&edge.target) else {
            continue;
        };
        if source_module == target_module {
            continue;
        }
        let dedupe_key = format!("{source_module}->{target_module}:{}", edge.label);
        if seen_module_edges.insert(dedupe_key) {
            module_edges.push(WorkflowGraphEdge {
                id: format!("{source_module}->{target_module}:{}", edge.label),
                source: source_module.clone(),
                target: target_module.clone(),
                label: edge.label.clone(),
                kind: edge.kind.clone(),
            });
        }
    }

    WorkflowGraphProjection { nodes, edges, modules, module_edges }
}

fn build_artifact_overlay() -> WorkflowGraphOverlay {
    WorkflowGraphOverlay {
        mode: "artifact".to_owned(),
        run_id: None,
        current_node_id: None,
        current_module_id: None,
        blocked_by: None,
        node_statuses: Vec::new(),
        module_statuses: Vec::new(),
        trace: Vec::new(),
        activity_summaries: Vec::new(),
        bulk_summaries: Vec::new(),
        signal_summaries: Vec::new(),
        update_summaries: Vec::new(),
        child_summaries: Vec::new(),
    }
}

fn build_run_overlay(
    artifact: &CompiledWorkflowArtifact,
    run_id: &str,
    current_state: Option<String>,
    history: &[EventEnvelope<WorkflowEvent>],
    activities: &[WorkflowActivityRecord],
    signals: &[WorkflowSignalRecord],
    updates: &[WorkflowUpdateRecord],
    children: &[WorkflowChildRecord],
    bulk_batches: &[WorkflowBulkBatchRecord],
) -> WorkflowGraphOverlay {
    let current_node_id = current_state.as_deref().map(workflow_state_node_id);
    let current_module_id = current_state.as_deref().map(workflow_module_id);
    let state_to_module = artifact
        .workflow
        .states
        .keys()
        .map(|state_id| (state_id.clone(), workflow_module_id(state_id)))
        .collect::<BTreeMap<_, _>>();
    let trace = history
        .iter()
        .enumerate()
        .map(|(index, event)| {
            let state_id = graph_state_for_event(event);
            let node_id = state_id.as_deref().map(workflow_state_node_id);
            let module_id =
                state_id.as_deref().and_then(|state_id| state_to_module.get(state_id).cloned());
            WorkflowGraphTraceStep {
                id: format!("trace-{index}"),
                occurred_at: event.occurred_at,
                lane: timeline_lane_for_event(&event.event_type).to_owned(),
                label: event.event_type.clone(),
                detail: event_detail(event),
                event_type: event.event_type.clone(),
                node_id,
                module_id,
            }
        })
        .collect::<Vec<_>>();

    let mut node_summaries = BTreeMap::<String, (String, String)>::new();
    for state_id in history.iter().filter_map(graph_state_for_event) {
        let node_id = workflow_state_node_id(&state_id);
        node_summaries
            .entry(node_id)
            .or_insert_with(|| ("executed".to_owned(), "Visited in execution trace".to_owned()));
    }
    if let Some(current_state_id) = current_state.as_deref() {
        node_summaries.insert(
            workflow_state_node_id(current_state_id),
            ("current".to_owned(), "Current wait point".to_owned()),
        );
    }

    let activity_summaries = summarize_activities_by_state(activities);
    for summary in &activity_summaries {
        let status = if summary.failed > 0 {
            "failed"
        } else if summary.pending > 0 || summary.retrying > 0 {
            "pending"
        } else {
            "executed"
        };
        let detail = if summary.failed > 0 {
            format!("{} failed activity attempts", summary.failed)
        } else if summary.pending > 0 || summary.retrying > 0 {
            format!("{} pending, {} retrying", summary.pending, summary.retrying)
        } else {
            format!("{} activity attempts completed", summary.completed)
        };
        merge_status(&mut node_summaries, &summary.node_id, status, &detail);
    }

    let bulk_summaries = summarize_bulk_batches_by_state(bulk_batches);
    for summary in &bulk_summaries {
        let status = if summary.failed_items > 0 || summary.status == "failed" {
            "failed"
        } else if summary.status == "scheduled" || summary.status == "running" {
            "pending"
        } else {
            "executed"
        };
        let detail = format!(
            "{} items, {} failed, {} cancelled",
            summary.total_items, summary.failed_items, summary.cancelled_items
        );
        merge_status(&mut node_summaries, &summary.node_id, status, &detail);
    }

    let child_summaries = summarize_children(artifact, children);
    for summary in &child_summaries {
        if let Some(node_id) = summary.node_id.as_deref() {
            let status = if matches!(summary.status.as_str(), "failed" | "cancelled" | "terminated")
            {
                "failed"
            } else if summary.status == "running" || summary.status == "started" {
                "pending"
            } else {
                "executed"
            };
            merge_status(
                &mut node_summaries,
                node_id,
                status,
                &format!("child workflow {}", summary.status),
            );
        }
    }

    let signal_summaries = summarize_signals(artifact, signals);
    let update_summaries = summarize_updates(updates);

    let mut module_summaries = BTreeMap::<String, (String, String)>::new();
    for (node_id, (status, summary)) in &node_summaries {
        if let Some(module_id) = node_id_to_module_id(node_id) {
            merge_status(&mut module_summaries, &module_id, status, summary);
        }
    }
    for summary in &signal_summaries {
        if let Some(module_id) = summary.module_id.as_deref() {
            merge_status(
                &mut module_summaries,
                module_id,
                "pending",
                &format!("{} signals", summary.count),
            );
        }
    }
    for summary in &update_summaries {
        if let Some(module_id) = summary.module_id.as_deref() {
            let status = if summary.latest_status == "rejected" { "failed" } else { "pending" };
            merge_status(
                &mut module_summaries,
                module_id,
                status,
                &format!("{} updates", summary.count),
            );
        }
    }

    let blocked_by = current_state
        .as_deref()
        .and_then(|state_id| artifact.workflow.states.get(state_id).map(|state| (state_id, state)))
        .and_then(|(state_id, state)| {
            blocked_by_for_state(state_id, state, signals, bulk_batches, activities)
        });

    WorkflowGraphOverlay {
        mode: "run".to_owned(),
        run_id: Some(run_id.to_owned()),
        current_node_id,
        current_module_id,
        blocked_by,
        node_statuses: node_summaries
            .into_iter()
            .map(|(id, (status, summary))| WorkflowGraphOverlayStatus {
                id,
                status,
                summary: Some(summary),
            })
            .collect(),
        module_statuses: module_summaries
            .into_iter()
            .map(|(id, (status, summary))| WorkflowGraphOverlayStatus {
                id,
                status,
                summary: Some(summary),
            })
            .collect(),
        trace,
        activity_summaries,
        bulk_summaries,
        signal_summaries,
        update_summaries,
        child_summaries,
    }
}

fn resolve_page(config: &QueryRuntimeConfig, pagination: &PaginationQuery) -> ResolvedPage {
    let limit = pagination.limit.unwrap_or(config.default_page_size).min(config.max_page_size);
    ResolvedPage { limit: limit.max(1), offset: pagination.offset.unwrap_or(0) }
}

fn workflow_entry_node_id() -> String {
    "workflow:entry".to_owned()
}

fn workflow_entry_module_id() -> String {
    "module:workflow:entry".to_owned()
}

fn handlers_hub_node_id() -> String {
    "system:handlers".to_owned()
}

fn handlers_hub_module_id() -> String {
    "module:system:handlers".to_owned()
}

fn workflow_state_node_id(state_id: &str) -> String {
    format!("workflow:{state_id}")
}

fn workflow_module_id(state_id: &str) -> String {
    format!("module:workflow:{state_id}")
}

fn signal_handler_node_id(signal_name: &str, state_id: &str) -> String {
    format!("signal:{signal_name}:{state_id}")
}

fn signal_handler_module_id(signal_name: &str) -> String {
    format!("module:signal:{signal_name}")
}

fn update_handler_node_id(update_name: &str, state_id: &str) -> String {
    format!("update:{update_name}:{state_id}")
}

fn update_handler_module_id(update_name: &str) -> String {
    format!("module:update:{update_name}")
}

fn query_node_id(query_name: &str) -> String {
    format!("query:{query_name}")
}

fn query_module_id(query_name: &str) -> String {
    format!("module:query:{query_name}")
}

fn node_id_to_module_id(node_id: &str) -> Option<String> {
    if node_id == workflow_entry_node_id() {
        return Some(workflow_entry_module_id());
    }
    if node_id == handlers_hub_node_id() {
        return Some(handlers_hub_module_id());
    }
    node_id.strip_prefix("workflow:").map(workflow_module_id).or_else(|| {
        node_id.split_once(':').and_then(|(prefix, rest)| match prefix {
            "signal" => rest.split_once(':').map(|(name, _)| signal_handler_module_id(name)),
            "update" => rest.split_once(':').map(|(name, _)| update_handler_module_id(name)),
            "query" => Some(query_module_id(rest)),
            _ => None,
        })
    })
}

fn resolve_source_anchor(
    source_map: &BTreeMap<String, SourceLocation>,
    keys: &[String],
) -> Option<WorkflowGraphSourceAnchor> {
    keys.iter().find_map(|key| source_map.get(key)).map(source_anchor)
}

fn source_anchor(location: &SourceLocation) -> WorkflowGraphSourceAnchor {
    WorkflowGraphSourceAnchor {
        file: location.file.clone(),
        line: location.line,
        column: location.column,
    }
}

fn compiled_state_kind(state: &CompiledStateNode) -> &'static str {
    match state {
        CompiledStateNode::Assign { .. } => "assign",
        CompiledStateNode::Choice { .. } => "choice",
        CompiledStateNode::Step { .. } => "step",
        CompiledStateNode::FanOut { .. } => "fan_out",
        CompiledStateNode::StartBulkActivity { .. } => "start_bulk_activity",
        CompiledStateNode::WaitForBulkActivity { .. } => "wait_for_bulk_activity",
        CompiledStateNode::WaitForAllActivities { .. } => "wait_for_all_activities",
        CompiledStateNode::StartChild { .. } => "start_child",
        CompiledStateNode::SignalChild { .. } => "signal_child",
        CompiledStateNode::CancelChild { .. } => "cancel_child",
        CompiledStateNode::SignalExternal { .. } => "signal_external",
        CompiledStateNode::CancelExternal { .. } => "cancel_external",
        CompiledStateNode::WaitForChild { .. } => "wait_for_child",
        CompiledStateNode::WaitForEvent { .. } => "wait_for_event",
        CompiledStateNode::WaitForCondition { .. } => "wait_for_condition",
        CompiledStateNode::WaitForTimer { .. } => "wait_for_timer",
        CompiledStateNode::Succeed { .. } => "succeed",
        CompiledStateNode::Fail { .. } => "fail",
        CompiledStateNode::ContinueAsNew { .. } => "continue_as_new",
    }
}

fn semantic_module_kind(_graph: &str, state: &CompiledStateNode) -> &'static str {
    match state {
        CompiledStateNode::Assign { .. } | CompiledStateNode::Choice { .. } => "assign_decision",
        CompiledStateNode::Step { .. } => "activity_step",
        CompiledStateNode::FanOut { .. } | CompiledStateNode::WaitForAllActivities { .. } => {
            "fan_out"
        }
        CompiledStateNode::StartBulkActivity { .. }
        | CompiledStateNode::WaitForBulkActivity { .. } => "bulk_activity",
        CompiledStateNode::StartChild { .. }
        | CompiledStateNode::SignalChild { .. }
        | CompiledStateNode::CancelChild { .. }
        | CompiledStateNode::SignalExternal { .. }
        | CompiledStateNode::CancelExternal { .. }
        | CompiledStateNode::WaitForChild { .. } => "child_workflow",
        CompiledStateNode::WaitForEvent { .. } | CompiledStateNode::WaitForCondition { .. } => {
            "wait"
        }
        CompiledStateNode::WaitForTimer { .. } => "timer",
        CompiledStateNode::Succeed { .. }
        | CompiledStateNode::Fail { .. }
        | CompiledStateNode::ContinueAsNew { .. } => "terminal",
    }
}

fn module_collapsed_by_default(state: &CompiledStateNode) -> bool {
    matches!(
        state,
        CompiledStateNode::FanOut { .. }
            | CompiledStateNode::StartBulkActivity { .. }
            | CompiledStateNode::WaitForBulkActivity { .. }
            | CompiledStateNode::StartChild { .. }
            | CompiledStateNode::WaitForChild { .. }
    )
}

fn compiled_state_subtitle(state: &CompiledStateNode) -> Option<String> {
    match state {
        CompiledStateNode::Assign { actions, .. } => Some(format!("{} assignments", actions.len())),
        CompiledStateNode::Choice { .. } => Some("branch".to_owned()),
        CompiledStateNode::Step { handler, task_queue, .. } => Some(
            task_queue
                .as_ref()
                .map(|_| format!("step · {handler}"))
                .unwrap_or_else(|| handler.clone()),
        ),
        CompiledStateNode::FanOut { activity_type, .. } => {
            Some(format!("fan-out · {activity_type}"))
        }
        CompiledStateNode::StartBulkActivity { activity_type, .. } => {
            Some(format!("bulk · {activity_type}"))
        }
        CompiledStateNode::WaitForBulkActivity { bulk_ref_var, .. } => {
            Some(format!("await bulk {bulk_ref_var}"))
        }
        CompiledStateNode::WaitForAllActivities { fanout_ref_var, .. } => {
            Some(format!("await fan-out {fanout_ref_var}"))
        }
        CompiledStateNode::StartChild { child_definition_id, .. } => {
            Some(format!("child · {child_definition_id}"))
        }
        CompiledStateNode::SignalChild { signal_name, .. } => {
            Some(format!("signal child · {signal_name}"))
        }
        CompiledStateNode::CancelChild { .. } => Some("cancel child".to_owned()),
        CompiledStateNode::SignalExternal { signal_name, .. } => {
            Some(format!("signal external · {signal_name}"))
        }
        CompiledStateNode::CancelExternal { .. } => Some("cancel external".to_owned()),
        CompiledStateNode::WaitForChild { child_ref_var, .. } => {
            Some(format!("await child {child_ref_var}"))
        }
        CompiledStateNode::WaitForEvent { event_type, .. } => {
            Some(format!("wait signal · {event_type}"))
        }
        CompiledStateNode::WaitForCondition { .. } => Some("wait condition".to_owned()),
        CompiledStateNode::WaitForTimer { timer_ref, .. } => Some(format!("timer · {timer_ref}")),
        CompiledStateNode::Succeed { .. } => Some("success".to_owned()),
        CompiledStateNode::Fail { .. } => Some("failure".to_owned()),
        CompiledStateNode::ContinueAsNew { .. } => Some("continue as new".to_owned()),
    }
}

fn state_transition_targets(state: &CompiledStateNode) -> Vec<(String, String)> {
    match state {
        CompiledStateNode::Assign { next, .. } => vec![("next".to_owned(), next.clone())],
        CompiledStateNode::Choice { then_next, else_next, .. } => {
            vec![("then".to_owned(), then_next.clone()), ("else".to_owned(), else_next.clone())]
        }
        CompiledStateNode::Step { next, on_error, .. } => {
            let mut transitions = next
                .iter()
                .map(|target| ("success".to_owned(), target.clone()))
                .collect::<Vec<_>>();
            if let Some(on_error) = on_error {
                transitions.push(("error".to_owned(), on_error.next.clone()));
            }
            transitions
        }
        CompiledStateNode::FanOut { next, .. } => vec![("await".to_owned(), next.clone())],
        CompiledStateNode::StartBulkActivity { next, .. } => {
            vec![("await".to_owned(), next.clone())]
        }
        CompiledStateNode::WaitForBulkActivity { next, on_error, .. }
        | CompiledStateNode::WaitForAllActivities { next, on_error, .. }
        | CompiledStateNode::WaitForChild { next, on_error, .. } => {
            let mut transitions = vec![("success".to_owned(), next.clone())];
            if let Some(on_error) = on_error {
                transitions.push(("error".to_owned(), on_error.next.clone()));
            }
            transitions
        }
        CompiledStateNode::StartChild { next, .. }
        | CompiledStateNode::SignalChild { next, .. }
        | CompiledStateNode::CancelChild { next, .. }
        | CompiledStateNode::SignalExternal { next, .. }
        | CompiledStateNode::CancelExternal { next, .. }
        | CompiledStateNode::WaitForEvent { next, .. }
        | CompiledStateNode::WaitForTimer { next, .. } => vec![("next".to_owned(), next.clone())],
        CompiledStateNode::WaitForCondition { next, timeout_next, .. } => {
            let mut transitions = vec![("condition".to_owned(), next.clone())];
            if let Some(timeout_next) = timeout_next {
                transitions.push(("timeout".to_owned(), timeout_next.clone()));
            }
            transitions
        }
        CompiledStateNode::Succeed { .. }
        | CompiledStateNode::Fail { .. }
        | CompiledStateNode::ContinueAsNew { .. } => Vec::new(),
    }
}

fn timeline_lane_for_event(event_type: &str) -> &'static str {
    let lower = event_type.to_lowercase();
    if lower.contains("activity") {
        "activities"
    } else if lower.contains("signal") || lower.contains("update") || lower.contains("query") {
        "messages"
    } else if lower.contains("timer") {
        "timers"
    } else if lower.contains("child") {
        "children"
    } else if lower.contains("workflow") {
        "lifecycle"
    } else {
        "other"
    }
}

fn event_detail(event: &EventEnvelope<WorkflowEvent>) -> Option<String> {
    match &event.payload {
        WorkflowEvent::ActivityTaskScheduled { activity_type, attempt, .. } => {
            Some(format!("{activity_type} attempt {attempt}"))
        }
        WorkflowEvent::ActivityTaskFailed { error, .. } => Some(error.clone()),
        WorkflowEvent::BulkActivityBatchScheduled { activity_type, chunk_size, .. } => {
            Some(format!("{activity_type} chunks of {chunk_size}"))
        }
        WorkflowEvent::SignalQueued { signal_type, .. }
        | WorkflowEvent::SignalReceived { signal_type, .. } => Some(signal_type.clone()),
        WorkflowEvent::WorkflowUpdateRequested { update_name, .. }
        | WorkflowEvent::WorkflowUpdateAccepted { update_name, .. }
        | WorkflowEvent::WorkflowUpdateCompleted { update_name, .. }
        | WorkflowEvent::WorkflowUpdateRejected { update_name, .. } => Some(update_name.clone()),
        WorkflowEvent::ChildWorkflowStartRequested { child_definition_id, .. } => {
            Some(child_definition_id.clone())
        }
        WorkflowEvent::ChildWorkflowFailed { error, .. } => Some(error.clone()),
        WorkflowEvent::ChildWorkflowCancelled { reason, .. }
        | WorkflowEvent::ChildWorkflowTerminated { reason, .. } => Some(reason.clone()),
        WorkflowEvent::TimerScheduled { timer_id, .. } | WorkflowEvent::TimerFired { timer_id } => {
            Some(timer_id.clone())
        }
        WorkflowEvent::WorkflowFailed { reason } => Some(reason.clone()),
        _ => None,
    }
}

fn graph_state_for_event(event: &EventEnvelope<WorkflowEvent>) -> Option<String> {
    event.metadata.get("state").cloned().or_else(|| match &event.payload {
        WorkflowEvent::ActivityTaskScheduled { state, .. } => state.clone(),
        WorkflowEvent::BulkActivityBatchScheduled { state, .. } => state.clone(),
        _ => None,
    })
}

fn summarize_activities_by_state(
    activities: &[WorkflowActivityRecord],
) -> Vec<WorkflowGraphActivitySummary> {
    let mut grouped = BTreeMap::<String, WorkflowGraphActivitySummary>::new();
    for activity in activities {
        let Some(state_id) = activity.state.as_deref() else {
            continue;
        };
        let key = workflow_state_node_id(state_id);
        let entry = grouped.entry(key.clone()).or_insert_with(|| WorkflowGraphActivitySummary {
            node_id: key.clone(),
            module_id: workflow_module_id(state_id),
            activity_type: activity.activity_type.clone(),
            total: 0,
            pending: 0,
            completed: 0,
            failed: 0,
            retrying: 0,
            worker_build_ids: Vec::new(),
        });
        entry.total += 1;
        match workflow_activity_status_label(&activity.status) {
            "scheduled" | "started" => entry.pending += 1,
            "completed" => entry.completed += 1,
            "failed" | "timed_out" | "cancelled" => entry.failed += 1,
            _ => {}
        }
        if activity.attempt > 1 {
            entry.retrying += 1;
        }
        if let Some(build_id) = activity.worker_build_id.clone() {
            if !entry.worker_build_ids.iter().any(|existing| existing == &build_id) {
                entry.worker_build_ids.push(build_id);
            }
        }
    }
    grouped.into_values().collect()
}

fn summarize_bulk_batches_by_state(
    bulk_batches: &[WorkflowBulkBatchRecord],
) -> Vec<WorkflowGraphBulkSummary> {
    bulk_batches
        .iter()
        .filter_map(|batch| {
            batch.state.as_deref().map(|state_id| WorkflowGraphBulkSummary {
                node_id: workflow_state_node_id(state_id),
                module_id: workflow_module_id(state_id),
                batch_id: batch.batch_id.clone(),
                activity_type: batch.activity_type.clone(),
                status: workflow_bulk_status_label(&batch.status).to_owned(),
                total_items: batch.total_items,
                succeeded_items: batch.succeeded_items,
                failed_items: batch.failed_items,
                cancelled_items: batch.cancelled_items,
            })
        })
        .collect()
}

fn summarize_signals(
    artifact: &CompiledWorkflowArtifact,
    signals: &[WorkflowSignalRecord],
) -> Vec<WorkflowGraphSignalSummary> {
    let wait_states = artifact
        .workflow
        .states
        .iter()
        .filter_map(|(state_id, state)| match state {
            CompiledStateNode::WaitForEvent { event_type, .. } => {
                Some((event_type.clone(), state_id.clone()))
            }
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let mut grouped = BTreeMap::<String, WorkflowGraphSignalSummary>::new();
    for signal in signals {
        let entry = grouped.entry(signal.signal_type.clone()).or_insert_with(|| {
            let state_id = wait_states.get(&signal.signal_type).cloned();
            WorkflowGraphSignalSummary {
                signal_name: signal.signal_type.clone(),
                count: 0,
                latest_status: workflow_signal_status_label(&signal.status).to_owned(),
                latest_seen_at: signal.updated_at,
                node_id: state_id.as_deref().map(workflow_state_node_id),
                module_id: state_id.as_deref().map(workflow_module_id),
            }
        });
        entry.count += 1;
        if signal.updated_at >= entry.latest_seen_at {
            entry.latest_seen_at = signal.updated_at;
            entry.latest_status = workflow_signal_status_label(&signal.status).to_owned();
        }
    }
    grouped.into_values().collect()
}

fn summarize_updates(updates: &[WorkflowUpdateRecord]) -> Vec<WorkflowGraphUpdateSummary> {
    let mut grouped = BTreeMap::<String, WorkflowGraphUpdateSummary>::new();
    for update in updates {
        let entry = grouped.entry(update.update_name.clone()).or_insert_with(|| {
            WorkflowGraphUpdateSummary {
                update_name: update.update_name.clone(),
                count: 0,
                latest_status: workflow_update_status_label(&update.status).to_owned(),
                latest_seen_at: update.updated_at,
                module_id: Some(update_handler_module_id(&update.update_name)),
            }
        });
        entry.count += 1;
        if update.updated_at >= entry.latest_seen_at {
            entry.latest_seen_at = update.updated_at;
            entry.latest_status = workflow_update_status_label(&update.status).to_owned();
        }
    }
    grouped.into_values().collect()
}

fn summarize_children(
    artifact: &CompiledWorkflowArtifact,
    children: &[WorkflowChildRecord],
) -> Vec<WorkflowGraphChildSummary> {
    let child_state_lookup = artifact
        .workflow
        .states
        .iter()
        .filter_map(|(state_id, state)| match state {
            CompiledStateNode::StartChild { child_definition_id, .. } => {
                Some((child_definition_id.clone(), state_id.clone()))
            }
            _ => None,
        })
        .fold(BTreeMap::<String, Vec<String>>::new(), |mut acc, (definition_id, state_id)| {
            acc.entry(definition_id).or_default().push(state_id);
            acc
        });

    children
        .iter()
        .map(|child| {
            let state_id = child_state_lookup
                .get(&child.child_definition_id)
                .and_then(|matches| (matches.len() == 1).then(|| matches[0].clone()));
            WorkflowGraphChildSummary {
                child_id: child.child_id.clone(),
                child_definition_id: child.child_definition_id.clone(),
                child_workflow_id: child.child_workflow_id.clone(),
                child_run_id: child.child_run_id.clone(),
                status: child.status.clone(),
                node_id: state_id.as_deref().map(workflow_state_node_id),
                module_id: state_id.as_deref().map(workflow_module_id),
            }
        })
        .collect()
}

fn blocked_by_for_state(
    state_id: &str,
    state: &CompiledStateNode,
    signals: &[WorkflowSignalRecord],
    bulk_batches: &[WorkflowBulkBatchRecord],
    activities: &[WorkflowActivityRecord],
) -> Option<WorkflowGraphBlockedBy> {
    let node_id = Some(workflow_state_node_id(state_id));
    let module_id = Some(workflow_module_id(state_id));
    match state {
        CompiledStateNode::WaitForEvent { event_type, .. } => Some(WorkflowGraphBlockedBy {
            kind: "signal".to_owned(),
            label: event_type.clone(),
            detail: Some(format!(
                "{} queued/consumed signals",
                signals.iter().filter(|signal| signal.signal_type == *event_type).count()
            )),
            node_id,
            module_id,
        }),
        CompiledStateNode::WaitForTimer { timer_ref, .. } => Some(WorkflowGraphBlockedBy {
            kind: "timer".to_owned(),
            label: timer_ref.clone(),
            detail: Some("waiting for timer fire".to_owned()),
            node_id,
            module_id,
        }),
        CompiledStateNode::WaitForCondition { timeout_ref, .. } => Some(WorkflowGraphBlockedBy {
            kind: "condition".to_owned(),
            label: "condition".to_owned(),
            detail: timeout_ref.as_ref().map(|timeout_ref| format!("timeout {timeout_ref} armed")),
            node_id,
            module_id,
        }),
        CompiledStateNode::WaitForBulkActivity { bulk_ref_var, .. } => {
            let pending = bulk_batches
                .iter()
                .find(|batch| batch.state.as_deref() == Some(state_id))
                .map(|batch| format!("{} items still aggregating", batch.total_items));
            Some(WorkflowGraphBlockedBy {
                kind: "bulk".to_owned(),
                label: bulk_ref_var.clone(),
                detail: pending,
                node_id,
                module_id,
            })
        }
        CompiledStateNode::WaitForAllActivities { fanout_ref_var, .. } => {
            Some(WorkflowGraphBlockedBy {
                kind: "fan_out".to_owned(),
                label: fanout_ref_var.clone(),
                detail: Some(format!(
                    "{} active attempts",
                    activities
                        .iter()
                        .filter(|activity| {
                            activity.state.as_deref() == Some(state_id)
                                && matches!(
                                    workflow_activity_status_label(&activity.status),
                                    "scheduled" | "started"
                                )
                        })
                        .count()
                )),
                node_id,
                module_id,
            })
        }
        CompiledStateNode::Step { handler, .. } => Some(WorkflowGraphBlockedBy {
            kind: "activity".to_owned(),
            label: handler.clone(),
            detail: Some(format!(
                "{} active attempts",
                activities
                    .iter()
                    .filter(|activity| {
                        activity.state.as_deref() == Some(state_id)
                            && matches!(
                                workflow_activity_status_label(&activity.status),
                                "scheduled" | "started"
                            )
                    })
                    .count()
            )),
            node_id,
            module_id,
        }),
        CompiledStateNode::StartChild { child_definition_id, .. }
        | CompiledStateNode::WaitForChild { child_ref_var: child_definition_id, .. } => {
            Some(WorkflowGraphBlockedBy {
                kind: "child".to_owned(),
                label: child_definition_id.clone(),
                detail: Some("waiting on child workflow".to_owned()),
                node_id,
                module_id,
            })
        }
        _ => None,
    }
}

fn merge_status(
    target: &mut BTreeMap<String, (String, String)>,
    id: &str,
    next_status: &str,
    next_summary: &str,
) {
    match target.get(id) {
        Some((current_status, _)) if status_rank(current_status) >= status_rank(next_status) => {}
        _ => {
            target.insert(id.to_owned(), (next_status.to_owned(), next_summary.to_owned()));
        }
    }
}

fn status_rank(status: &str) -> u8 {
    match status {
        "current" => 4,
        "failed" => 3,
        "pending" => 2,
        "executed" => 1,
        _ => 0,
    }
}

fn workflow_activity_status_label(status: &WorkflowActivityStatus) -> &'static str {
    match status {
        WorkflowActivityStatus::Scheduled => "scheduled",
        WorkflowActivityStatus::Started => "started",
        WorkflowActivityStatus::Completed => "completed",
        WorkflowActivityStatus::Failed => "failed",
        WorkflowActivityStatus::TimedOut => "timed_out",
        WorkflowActivityStatus::Cancelled => "cancelled",
    }
}

fn workflow_bulk_status_label(status: &WorkflowBulkBatchStatus) -> &'static str {
    match status {
        WorkflowBulkBatchStatus::Scheduled => "scheduled",
        WorkflowBulkBatchStatus::Running => "running",
        WorkflowBulkBatchStatus::Completed => "completed",
        WorkflowBulkBatchStatus::Failed => "failed",
        WorkflowBulkBatchStatus::Cancelled => "cancelled",
    }
}

fn workflow_signal_status_label(status: &WorkflowSignalStatus) -> &'static str {
    match status {
        WorkflowSignalStatus::Queued => "queued",
        WorkflowSignalStatus::Dispatching => "dispatching",
        WorkflowSignalStatus::Consumed => "consumed",
    }
}

fn workflow_update_status_label(status: &WorkflowUpdateStatus) -> &'static str {
    match status {
        WorkflowUpdateStatus::Requested => "requested",
        WorkflowUpdateStatus::Accepted => "accepted",
        WorkflowUpdateStatus::Completed => "completed",
        WorkflowUpdateStatus::Rejected => "rejected",
    }
}

fn non_empty(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() { None } else { Some(trimmed.to_owned()) }
}

fn non_empty_ref(value: &str) -> Option<&str> {
    if value.trim().is_empty() { None } else { Some(value) }
}

fn build_page_info(page: &ResolvedPage, total: usize, returned: usize) -> PageInfo {
    let limit = page.limit;
    let offset = page.offset;
    let next_offset = (offset + returned < total).then_some(offset + returned);
    PageInfo { limit, offset, returned, total, has_more: next_offset.is_some(), next_offset }
}

async fn compute_routing_status(
    store: &WorkflowStore,
    tenant_id: &str,
    workflow_task_queue: &str,
    sticky_workflow_build_id: Option<&str>,
    artifact_hash: Option<&str>,
) -> Result<String> {
    let sticky_build_compatible_with_queue = if let Some(build_id) = sticky_workflow_build_id {
        Some(
            store
                .is_build_compatible_with_queue(
                    tenant_id,
                    TaskQueueKind::Workflow,
                    workflow_task_queue,
                    build_id,
                )
                .await?,
        )
    } else {
        None
    };
    let sticky_build_supports_pinned_artifact = if let Some(build_id) = sticky_workflow_build_id {
        Some(
            store
                .workflow_build_supports_artifact(
                    tenant_id,
                    workflow_task_queue,
                    build_id,
                    artifact_hash,
                )
                .await?,
        )
    } else {
        None
    };
    let status = match (sticky_build_compatible_with_queue, sticky_build_supports_pinned_artifact) {
        (Some(false), _) => "sticky_incompatible_fallback_required",
        (_, Some(false)) => "sticky_artifact_mismatch_fallback_required",
        (Some(true), Some(true)) | (Some(true), None) | (None, Some(true)) => "sticky_active",
        (None, None) => "queue_default_active",
    };
    Ok(status.to_owned())
}

async fn filtered_workflow_list_items(
    store: &WorkflowStore,
    tenant_id: &str,
    filters: &WorkflowInstanceFilters,
    page: &ResolvedPage,
    routing_status: &str,
) -> Result<(Vec<WorkflowListItem>, usize)> {
    const BATCH_SIZE: i64 = 200;
    let mut scan_offset = 0_i64;
    let mut matched = 0_usize;
    let mut items = Vec::new();

    loop {
        let batch = store
            .list_instances_page_with_filters(tenant_id, filters, BATCH_SIZE, scan_offset)
            .await?;
        if batch.is_empty() {
            break;
        }
        let batch_len = batch.len();
        for workflow in batch {
            let item = workflow_list_item_from_state(store, workflow).await?;
            if item.routing_status != routing_status {
                continue;
            }
            if matched >= page.offset && items.len() < page.limit {
                items.push(item);
            }
            matched = matched.saturating_add(1);
        }
        if batch_len < BATCH_SIZE as usize {
            break;
        }
        scan_offset += BATCH_SIZE;
    }

    Ok((items, matched))
}

async fn filtered_run_list_items(
    store: &WorkflowStore,
    tenant_id: &str,
    filters: &WorkflowRunFilters,
    page: &ResolvedPage,
    routing_status: &str,
) -> Result<(Vec<RunListItem>, usize)> {
    const BATCH_SIZE: i64 = 200;
    let mut scan_offset = 0_i64;
    let mut matched = 0_usize;
    let mut items = Vec::new();

    loop {
        let batch =
            store.list_runs_page_with_filters(tenant_id, filters, BATCH_SIZE, scan_offset).await?;
        if batch.is_empty() {
            break;
        }
        let batch_len = batch.len();
        for run in batch {
            let item = run_list_item_from_record(store, run).await?;
            if item.routing_status != routing_status {
                continue;
            }
            if matched >= page.offset && items.len() < page.limit {
                items.push(item);
            }
            matched = matched.saturating_add(1);
        }
        if batch_len < BATCH_SIZE as usize {
            break;
        }
        scan_offset += BATCH_SIZE;
    }

    Ok((items, matched))
}

async fn workflow_list_item_from_state(
    store: &WorkflowStore,
    state: WorkflowInstanceState,
) -> Result<WorkflowListItem> {
    let routing_status = compute_routing_status(
        store,
        &state.tenant_id,
        &state.workflow_task_queue,
        state.sticky_workflow_build_id.as_deref(),
        state.artifact_hash.as_deref(),
    )
    .await?;
    Ok(WorkflowListItem {
        tenant_id: state.tenant_id,
        instance_id: state.instance_id,
        run_id: state.run_id,
        definition_id: state.definition_id,
        definition_version: state.definition_version,
        artifact_hash: state.artifact_hash,
        workflow_task_queue: state.workflow_task_queue,
        sticky_workflow_build_id: state.sticky_workflow_build_id,
        sticky_workflow_poller_id: state.sticky_workflow_poller_id,
        routing_status,
        current_state: state.current_state,
        status: state.status.as_str().to_owned(),
        event_count: state.event_count,
        last_event_id: state.last_event_id,
        last_event_type: state.last_event_type,
        updated_at: state.updated_at,
        consistency: "eventual",
        source: "projection",
    })
}

async fn run_list_item_from_record(
    store: &WorkflowStore,
    record: WorkflowRunSummaryRecord,
) -> Result<RunListItem> {
    let routing_status = compute_routing_status(
        store,
        &record.tenant_id,
        &record.workflow_task_queue,
        record.sticky_workflow_build_id.as_deref(),
        record.artifact_hash.as_deref(),
    )
    .await?;
    Ok(RunListItem {
        tenant_id: record.tenant_id,
        instance_id: record.instance_id,
        run_id: record.run_id,
        definition_id: record.definition_id,
        definition_version: record.definition_version,
        artifact_hash: record.artifact_hash,
        workflow_task_queue: record.workflow_task_queue,
        sticky_workflow_build_id: record.sticky_workflow_build_id,
        sticky_workflow_poller_id: record.sticky_workflow_poller_id,
        routing_status,
        sticky_updated_at: record.sticky_updated_at,
        previous_run_id: record.previous_run_id,
        next_run_id: record.next_run_id,
        continue_reason: record.continue_reason,
        started_at: record.started_at,
        closed_at: record.closed_at,
        updated_at: record.updated_at,
        last_transition_at: record.last_transition_at,
        status: record.status,
        current_state: record.current_state,
        last_event_type: record.last_event_type,
        event_count: record.event_count,
        consistency: "eventual",
        source: "projection",
    })
}

fn activity_graph_node(
    node_id: &str,
    activity: &WorkflowActivityRecord,
) -> WorkflowExecutionGraphNode {
    WorkflowExecutionGraphNode {
        id: node_id.to_owned(),
        kind: "activity".to_owned(),
        label: activity.activity_type.clone(),
        status: match activity.status {
            fabrik_store::WorkflowActivityStatus::Scheduled => "scheduled",
            fabrik_store::WorkflowActivityStatus::Started => "started",
            fabrik_store::WorkflowActivityStatus::Completed => "completed",
            fabrik_store::WorkflowActivityStatus::Failed => "failed",
            fabrik_store::WorkflowActivityStatus::TimedOut => "timed_out",
            fabrik_store::WorkflowActivityStatus::Cancelled => "cancelled",
        }
        .to_owned(),
        subtitle: Some(format!("{} attempt {}", activity.activity_id, activity.attempt)),
    }
}

fn child_graph_node(node_id: &str, child: &WorkflowChildRecord) -> WorkflowExecutionGraphNode {
    WorkflowExecutionGraphNode {
        id: node_id.to_owned(),
        kind: "child_workflow".to_owned(),
        label: child.child_definition_id.clone(),
        status: child.status.clone(),
        subtitle: Some(child.child_workflow_id.clone()),
    }
}

fn queue_kind_label(queue_kind: &TaskQueueKind) -> &'static str {
    match queue_kind {
        TaskQueueKind::Workflow => "workflow",
        TaskQueueKind::Activity => "activity",
    }
}

fn signal_status_label(signal: &WorkflowSignalRecord) -> String {
    match signal.status {
        fabrik_store::WorkflowSignalStatus::Queued => "queued",
        fabrik_store::WorkflowSignalStatus::Dispatching => "dispatching",
        fabrik_store::WorkflowSignalStatus::Consumed => "consumed",
    }
    .to_owned()
}

fn dedupe_graph_nodes(nodes: &mut Vec<WorkflowExecutionGraphNode>) {
    let mut seen = std::collections::HashSet::new();
    nodes.retain(|node| seen.insert(node.id.clone()));
}

fn dedupe_graph_edges(edges: &mut Vec<WorkflowExecutionGraphEdge>) {
    let mut seen = std::collections::HashSet::new();
    edges.retain(|edge| seen.insert(edge.id.clone()));
}

fn projection_lag_ms_from_times(
    times: impl IntoIterator<Item = Option<DateTime<Utc>>>,
) -> Option<i64> {
    let now = Utc::now();
    times
        .into_iter()
        .flatten()
        .min()
        .map(|updated_at| now.signed_duration_since(updated_at).num_milliseconds().max(0))
}

fn authoritative_bulk_source(throughput_backend: &str) -> &'static str {
    if throughput_backend == "stream-v2" { "stream-v2-owner-state" } else { "pg-v1-postgres" }
}

fn parse_read_consistency(raw: Option<&str>) -> Result<ReadConsistency> {
    match raw.unwrap_or("eventual") {
        "eventual" => Ok(ReadConsistency::Eventual),
        "strong" => Ok(ReadConsistency::Strong),
        other => anyhow::bail!("unsupported consistency {other}"),
    }
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

fn invalid_request(error: anyhow::Error) -> (StatusCode, Json<ErrorResponse>) {
    (StatusCode::BAD_REQUEST, Json(ErrorResponse { message: error.to_string() }))
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
    consistency: ReadConsistency,
) -> Result<WorkflowBulkBatchesResponse> {
    let page = resolve_page(&state.query, &pagination);
    let (total, batches) = match consistency {
        ReadConsistency::Eventual => (
            state
                .store
                .count_bulk_batches_for_run_query_view(tenant_id, instance_id, run_id)
                .await? as usize,
            state
                .store
                .list_bulk_batches_for_run_page_query_view(
                    tenant_id,
                    instance_id,
                    run_id,
                    i64::try_from(page.limit).context("bulk batch page limit exceeds i64")?,
                    i64::try_from(page.offset).context("bulk batch page offset exceeds i64")?,
                )
                .await?,
        ),
        ReadConsistency::Strong => {
            let limit = i64::try_from(page.limit).context("bulk batch page limit exceeds i64")?;
            let offset =
                i64::try_from(page.offset).context("bulk batch page offset exceeds i64")?;
            let durable_total =
                state.store.count_bulk_batches_for_run(tenant_id, instance_id, run_id).await?
                    as usize;
            let mut batches = state
                .store
                .list_bulk_batches_for_run_page(tenant_id, instance_id, run_id, limit, offset)
                .await?;
            let (total, discovery_batches) = if batches.is_empty() {
                (
                    state
                        .store
                        .count_bulk_batches_for_run_query_view(tenant_id, instance_id, run_id)
                        .await? as usize,
                    state
                        .store
                        .list_bulk_batches_for_run_page_query_view(
                            tenant_id,
                            instance_id,
                            run_id,
                            limit,
                            offset,
                        )
                        .await?,
                )
            } else {
                (durable_total, batches.clone())
            };
            if batches.is_empty() {
                batches = discovery_batches;
            }
            for batch in &mut batches {
                if batch.throughput_backend == ThroughputBackend::StreamV2.as_str() {
                    *batch = fetch_strong_stream_batch(
                        state,
                        tenant_id,
                        instance_id,
                        run_id,
                        &batch.batch_id,
                    )
                    .await?;
                }
            }
            (total, batches)
        }
    };
    let authoritative_source = if consistency == ReadConsistency::Strong {
        if batches
            .iter()
            .any(|batch| batch.throughput_backend == ThroughputBackend::StreamV2.as_str())
            && batches
                .iter()
                .any(|batch| batch.throughput_backend != ThroughputBackend::StreamV2.as_str())
        {
            "mixed-bulk-backends"
        } else if batches
            .iter()
            .any(|batch| batch.throughput_backend == ThroughputBackend::StreamV2.as_str())
        {
            "stream-v2-owner-state"
        } else {
            "pg-v1-postgres"
        }
    } else if batches
        .iter()
        .any(|batch| batch.throughput_backend == ThroughputBackend::StreamV2.as_str())
    {
        "mixed-bulk-backends"
    } else {
        "pg-v1-postgres"
    };
    Ok(WorkflowBulkBatchesResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        consistency: consistency.as_str(),
        authoritative_source,
        projection_lag_ms: (consistency == ReadConsistency::Eventual)
            .then(|| {
                projection_lag_ms_from_times(batches.iter().map(|batch| Some(batch.updated_at)))
            })
            .flatten(),
        page: build_page_info(&page, total, batches.len()),
        batch_count: total,
        batches,
    })
}

async fn load_workflow_bulk_batch(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
    consistency: ReadConsistency,
) -> Result<WorkflowBulkBatchResponse> {
    let batch = match consistency {
        ReadConsistency::Eventual => {
            state.store.get_bulk_batch_query_view(tenant_id, instance_id, run_id, batch_id).await?
        }
        ReadConsistency::Strong => {
            let batch =
                load_bulk_batch_for_strong_read(state, tenant_id, instance_id, run_id, batch_id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("bulk batch {batch_id} not found"))?;
            if batch.throughput_backend == ThroughputBackend::StreamV2.as_str() {
                Some(
                    fetch_strong_stream_batch(state, tenant_id, instance_id, run_id, batch_id)
                        .await?,
                )
            } else {
                Some(batch)
            }
        }
    }
    .ok_or_else(|| anyhow::anyhow!("bulk batch {batch_id} not found"))?;
    Ok(WorkflowBulkBatchResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        consistency: consistency.as_str(),
        authoritative_source: authoritative_bulk_source(&batch.throughput_backend),
        projection_lag_ms: (consistency == ReadConsistency::Eventual)
            .then(|| projection_lag_ms_from_times([Some(batch.updated_at)]))
            .flatten(),
        batch,
    })
}

async fn load_workflow_bulk_chunks(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
    pagination: PaginationQuery,
    consistency: ReadConsistency,
) -> Result<WorkflowBulkChunksResponse> {
    let batch = match consistency {
        ReadConsistency::Eventual => {
            state.store.get_bulk_batch_query_view(tenant_id, instance_id, run_id, batch_id).await?
        }
        ReadConsistency::Strong => {
            let batch =
                load_bulk_batch_for_strong_read(state, tenant_id, instance_id, run_id, batch_id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("bulk batch {batch_id} not found"))?;
            if batch.throughput_backend == ThroughputBackend::StreamV2.as_str() {
                Some(
                    fetch_strong_stream_batch(state, tenant_id, instance_id, run_id, batch_id)
                        .await?,
                )
            } else {
                Some(batch)
            }
        }
    }
    .ok_or_else(|| anyhow::anyhow!("bulk batch {batch_id} not found"))?;
    let page = resolve_page(&state.query, &pagination);
    let total = match consistency {
        ReadConsistency::Eventual => {
            state
                .store
                .count_bulk_chunks_for_batch_query_view(tenant_id, instance_id, run_id, batch_id)
                .await? as usize
        }
        ReadConsistency::Strong => {
            if batch.throughput_backend == ThroughputBackend::StreamV2.as_str() {
                state
                    .store
                    .count_bulk_chunks_for_batch_query_view(
                        tenant_id,
                        instance_id,
                        run_id,
                        batch_id,
                    )
                    .await? as usize
            } else {
                state
                    .store
                    .count_bulk_chunks_for_batch(tenant_id, instance_id, run_id, batch_id)
                    .await? as usize
            }
        }
    };
    let chunks = match consistency {
        ReadConsistency::Eventual => {
            state
                .store
                .list_bulk_chunks_for_batch_page_query_view(
                    tenant_id,
                    instance_id,
                    run_id,
                    batch_id,
                    i64::try_from(page.limit).context("bulk chunk page limit exceeds i64")?,
                    i64::try_from(page.offset).context("bulk chunk page offset exceeds i64")?,
                )
                .await?
        }
        ReadConsistency::Strong => {
            if batch.throughput_backend == ThroughputBackend::StreamV2.as_str() {
                fetch_strong_stream_chunks(
                    state,
                    tenant_id,
                    instance_id,
                    run_id,
                    batch_id,
                    page.limit,
                    page.offset,
                )
                .await?
            } else {
                state
                    .store
                    .list_bulk_chunks_for_batch_page(
                        tenant_id,
                        instance_id,
                        run_id,
                        batch_id,
                        i64::try_from(page.limit).context("bulk chunk page limit exceeds i64")?,
                        i64::try_from(page.offset).context("bulk chunk page offset exceeds i64")?,
                    )
                    .await?
            }
        }
    };
    let mut resolved_chunks = Vec::new();
    for chunk in chunks {
        resolved_chunks.push(resolve_chunk_payloads(state, chunk, true, false).await?);
    }
    Ok(WorkflowBulkChunksResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        batch_id: batch_id.to_owned(),
        consistency: consistency.as_str(),
        authoritative_source: authoritative_bulk_source(&batch.throughput_backend),
        projection_lag_ms: (consistency == ReadConsistency::Eventual)
            .then(|| {
                projection_lag_ms_from_times(
                    resolved_chunks.iter().map(|chunk| Some(chunk.updated_at)),
                )
            })
            .flatten(),
        page: build_page_info(&page, total, resolved_chunks.len()),
        chunk_count: total,
        chunks: resolved_chunks,
    })
}

async fn load_workflow_bulk_results(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
    pagination: PaginationQuery,
    consistency: ReadConsistency,
) -> Result<WorkflowBulkResultsResponse> {
    let batch =
        load_workflow_bulk_batch(state, tenant_id, instance_id, run_id, batch_id, consistency)
            .await?
            .batch;
    let page = resolve_page(&state.query, &pagination);
    let total = match consistency {
        ReadConsistency::Eventual => {
            state
                .store
                .count_bulk_chunks_for_batch_query_view(tenant_id, instance_id, run_id, batch_id)
                .await? as usize
        }
        ReadConsistency::Strong => {
            if batch.throughput_backend == ThroughputBackend::StreamV2.as_str() {
                state
                    .store
                    .count_bulk_chunks_for_batch_query_view(
                        tenant_id,
                        instance_id,
                        run_id,
                        batch_id,
                    )
                    .await? as usize
            } else {
                state
                    .store
                    .count_bulk_chunks_for_batch(tenant_id, instance_id, run_id, batch_id)
                    .await? as usize
            }
        }
    };
    let chunks = match consistency {
        ReadConsistency::Eventual => {
            state
                .store
                .list_bulk_chunks_for_batch_page_query_view(
                    tenant_id,
                    instance_id,
                    run_id,
                    batch_id,
                    i64::try_from(page.limit).context("bulk result page limit exceeds i64")?,
                    i64::try_from(page.offset).context("bulk result page offset exceeds i64")?,
                )
                .await?
        }
        ReadConsistency::Strong => {
            if batch.throughput_backend == ThroughputBackend::StreamV2.as_str() {
                fetch_strong_stream_chunks(
                    state,
                    tenant_id,
                    instance_id,
                    run_id,
                    batch_id,
                    page.limit,
                    page.offset,
                )
                .await?
            } else {
                state
                    .store
                    .list_bulk_chunks_for_batch_page(
                        tenant_id,
                        instance_id,
                        run_id,
                        batch_id,
                        i64::try_from(page.limit).context("bulk result page limit exceeds i64")?,
                        i64::try_from(page.offset)
                            .context("bulk result page offset exceeds i64")?,
                    )
                    .await?
            }
        }
    };
    let mut resolved_chunks = Vec::new();
    for chunk in chunks {
        resolved_chunks.push(resolve_chunk_payloads(state, chunk, false, true).await?);
    }
    let chunks =
        resolved_chunks.into_iter().filter(|chunk| chunk.output.is_some()).collect::<Vec<_>>();
    Ok(WorkflowBulkResultsResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        batch_id: batch_id.to_owned(),
        consistency: consistency.as_str(),
        authoritative_source: authoritative_bulk_source(&batch.throughput_backend),
        projection_lag_ms: (consistency == ReadConsistency::Eventual)
            .then(|| {
                projection_lag_ms_from_times(chunks.iter().map(|chunk| Some(chunk.updated_at)))
            })
            .flatten(),
        page: build_page_info(&page, total, chunks.len()),
        chunk_count: total,
        chunks,
    })
}

async fn resolve_chunk_payloads(
    state: &AppState,
    mut chunk: WorkflowBulkChunkRecord,
    resolve_input: bool,
    resolve_output: bool,
) -> Result<WorkflowBulkChunkRecord> {
    if resolve_input && chunk.items.is_empty() && !chunk.input_handle.is_null() {
        if let Ok(handle) = serde_json::from_value::<PayloadHandle>(chunk.input_handle.clone()) {
            chunk.items = state.payload_store.read_items(&handle).await?;
        }
    }
    if resolve_output && chunk.output.is_none() && !chunk.result_handle.is_null() {
        if let Ok(handle) = serde_json::from_value::<PayloadHandle>(chunk.result_handle.clone()) {
            let value = state.payload_store.read_value(&handle).await?;
            if let Value::Array(items) = value {
                chunk.output = Some(items);
            }
        }
    }
    Ok(chunk)
}

async fn fetch_strong_stream_batch(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
) -> Result<WorkflowBulkBatchRecord> {
    let endpoint = throughput_owner_query_endpoint(state, batch_id).await?;
    let response = state
        .client
        .get(format!(
            "{endpoint}/debug/throughput/batches/{tenant_id}/{instance_id}/{run_id}/{batch_id}"
        ))
        .send()
        .await
        .context("failed to fetch strong throughput batch")?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        anyhow::bail!("bulk batch {batch_id} not found");
    }
    let response = response.error_for_status().context("strong throughput batch request failed")?;
    Ok(response
        .json::<StrongThroughputBatchResponse>()
        .await
        .context("failed to decode strong throughput batch response")?
        .batch)
}

async fn load_bulk_batch_for_strong_read(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
) -> Result<Option<WorkflowBulkBatchRecord>> {
    if let Some(batch) =
        state.store.get_bulk_batch(tenant_id, instance_id, run_id, batch_id).await?
    {
        return Ok(Some(batch));
    }
    state.store.get_bulk_batch_query_view(tenant_id, instance_id, run_id, batch_id).await
}

async fn fetch_strong_stream_chunks(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
    limit: usize,
    offset: usize,
) -> Result<Vec<WorkflowBulkChunkRecord>> {
    let endpoint = throughput_owner_query_endpoint(state, batch_id).await?;
    let response = state
        .client
        .get(format!(
            "{endpoint}/debug/throughput/batches/{tenant_id}/{instance_id}/{run_id}/{batch_id}/chunks"
        ))
        .query(&[("limit", limit), ("offset", offset)])
        .send()
        .await
        .context("failed to fetch strong throughput chunks")?;
    let response =
        response.error_for_status().context("strong throughput chunks request failed")?;
    Ok(response
        .json::<StrongThroughputChunksResponse>()
        .await
        .context("failed to decode strong throughput chunks response")?
        .chunks)
}

async fn throughput_owner_query_endpoint(state: &AppState, batch_id: &str) -> Result<String> {
    let partition_id = partition_for_key(
        &throughput_partition_key(batch_id, 0),
        state.throughput_partitions.max(1),
    );
    let member = state
        .store
        .get_active_throughput_member_for_partition(partition_id, Utc::now())
        .await?
        .ok_or_else(|| anyhow::anyhow!("no active throughput owner for batch {batch_id}"))?;
    Ok(member.query_endpoint.trim_end_matches('/').to_owned())
}

#[derive(Debug, Deserialize)]
struct StrongThroughputBatchResponse {
    batch: WorkflowBulkBatchRecord,
}

#[derive(Debug, Deserialize)]
struct StrongThroughputChunksResponse {
    chunks: Vec<WorkflowBulkChunkRecord>,
}

fn build_payload_store_config(query: &QueryRuntimeConfig) -> PayloadStoreConfig {
    PayloadStoreConfig {
        default_store: match query.throughput_payload_store.kind {
            fabrik_config::ThroughputPayloadStoreKind::LocalFilesystem => {
                PayloadStoreKind::LocalFilesystem
            }
            fabrik_config::ThroughputPayloadStoreKind::S3 => PayloadStoreKind::S3,
        },
        local_dir: query.throughput_payload_store.local_dir.clone(),
        s3_bucket: query.throughput_payload_store.s3_bucket.clone(),
        s3_region: query.throughput_payload_store.s3_region.clone(),
        s3_endpoint: query.throughput_payload_store.s3_endpoint.clone(),
        s3_access_key_id: query.throughput_payload_store.s3_access_key_id.clone(),
        s3_secret_access_key: query.throughput_payload_store.s3_secret_access_key.clone(),
        s3_force_path_style: query.throughput_payload_store.s3_force_path_style,
        s3_key_prefix: query.throughput_payload_store.s3_key_prefix.clone(),
    }
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

async fn load_workflow_updates(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    pagination: PaginationQuery,
) -> Result<WorkflowUpdatesResponse> {
    let page = resolve_page(&state.query, &pagination);
    let total = state.store.count_updates_for_run(tenant_id, instance_id, run_id).await? as usize;
    let updates = state
        .store
        .list_updates_for_run_page(
            tenant_id,
            instance_id,
            run_id,
            i64::try_from(page.limit).context("updates page limit exceeds i64")?,
            i64::try_from(page.offset).context("updates page offset exceeds i64")?,
        )
        .await?;
    Ok(WorkflowUpdatesResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        page: build_page_info(&page, total, updates.len()),
        update_count: total,
        updates,
    })
}

async fn load_workflow_update(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    update_id: &str,
) -> Result<WorkflowUpdateResponse, (StatusCode, Json<ErrorResponse>)> {
    let update = state
        .store
        .get_update(tenant_id, instance_id, run_id, update_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    message: format!(
                        "workflow update {update_id} not found for instance {instance_id} run {run_id}"
                    ),
                }),
            )
        })?;
    Ok(WorkflowUpdateResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        update,
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use fabrik_config::{ThroughputPayloadStoreConfig, ThroughputPayloadStoreKind};
    use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
    use fabrik_workflow::{
        ArtifactEntrypoint, CompiledQueryHandler, CompiledUpdateHandler, CompiledWorkflow,
        Expression, SourceLocation, WorkflowStatus,
    };
    use serde_json::json;
    use std::{
        collections::{BTreeMap, BTreeSet},
        net::SocketAddr,
        process::Command,
        sync::{Arc, Mutex},
        time::{Duration as StdDuration, Instant},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        time::sleep,
    };
    use uuid::Uuid;

    struct TestPostgres {
        container_name: String,
        database_url: String,
    }

    impl TestPostgres {
        fn start() -> Result<Option<Self>> {
            if !docker_available() {
                eprintln!("skipping query-service integration tests because docker is unavailable");
                return Ok(None);
            }

            let container_name = format!("fabrik-query-test-pg-{}", Uuid::now_v7());
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

            let host_port = match wait_for_docker_port(&container_name, "5432/tcp") {
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

    fn wait_for_docker_port(container_name: &str, container_port: &str) -> Result<u16> {
        let deadline = Instant::now() + StdDuration::from_secs(15);
        loop {
            let output = Command::new("docker")
                .args([
                    "inspect",
                    "--format",
                    &format!(
                        "{{{{(index (index .NetworkSettings.Ports \"{container_port}\") 0).HostPort}}}}"
                    ),
                    container_name,
                ])
                .output()
                .with_context(|| format!("failed to inspect docker container {container_name}"))?;
            if output.status.success() {
                let host_port = String::from_utf8_lossy(&output.stdout).trim().to_owned();
                if !host_port.is_empty() {
                    return host_port
                        .parse::<u16>()
                        .with_context(|| format!("invalid mapped port {host_port}"));
                }
            }
            if Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for port {container_port} on {container_name}");
            }
            std::thread::sleep(StdDuration::from_millis(100));
        }
    }

    fn docker_logs(container_name: &str) -> Result<String> {
        let output = Command::new("docker")
            .args(["logs", container_name])
            .output()
            .with_context(|| format!("failed to read docker logs for {container_name}"))?;
        Ok(format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ))
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

    async fn test_state(store: WorkflowStore) -> Result<AppState> {
        let temp_dir =
            std::env::temp_dir().join(format!("fabrik-query-payloads-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&temp_dir).context("failed to create test payload store dir")?;
        let query = QueryRuntimeConfig {
            default_page_size: 100,
            max_page_size: 500,
            throughput_payload_store: ThroughputPayloadStoreConfig {
                kind: ThroughputPayloadStoreKind::LocalFilesystem,
                local_dir: temp_dir.display().to_string(),
                s3_bucket: None,
                s3_region: "us-east-1".to_owned(),
                s3_endpoint: None,
                s3_access_key_id: None,
                s3_secret_access_key: None,
                s3_force_path_style: false,
                s3_key_prefix: String::new(),
            },
            strong_query_unified_url: None,
            strong_query_executor_url: None,
            history_retention_days: None,
            run_retention_days: None,
            activity_retention_days: None,
            signal_retention_days: None,
            snapshot_retention_days: None,
            retention_sweep_interval_seconds: 300,
        };
        let payload_store = PayloadStore::from_config(build_payload_store_config(&query)).await?;
        Ok(AppState {
            store,
            broker: BrokerConfig::new("127.0.0.1:9092", "workflow-events", 1),
            query,
            payload_store,
            client: Client::new(),
            throughput_partitions: 1,
            retention: Arc::new(Mutex::new(RetentionDebugState::default())),
        })
    }

    #[test]
    fn owner_query_endpoints_prefers_unified_then_executor() {
        let config = QueryRuntimeConfig {
            default_page_size: 100,
            max_page_size: 500,
            throughput_payload_store: fabrik_config::ThroughputPayloadStoreConfig {
                kind: fabrik_config::ThroughputPayloadStoreKind::LocalFilesystem,
                local_dir: "/tmp".to_owned(),
                s3_bucket: None,
                s3_region: "us-east-1".to_owned(),
                s3_endpoint: None,
                s3_access_key_id: None,
                s3_secret_access_key: None,
                s3_force_path_style: false,
                s3_key_prefix: String::new(),
            },
            strong_query_unified_url: Some("http://unified".to_owned()),
            strong_query_executor_url: Some("http://executor".to_owned()),
            history_retention_days: None,
            run_retention_days: None,
            activity_retention_days: None,
            signal_retention_days: None,
            snapshot_retention_days: None,
            retention_sweep_interval_seconds: 300,
        };

        assert_eq!(
            owner_query_endpoints(&config),
            vec!["http://unified".to_owned(), "http://executor".to_owned()]
        );
    }

    #[tokio::test]
    async fn owner_query_endpoint_round_trip_returns_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test listener");
        let addr: SocketAddr = listener.local_addr().expect("read test listener address");
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept test connection");
            let mut buf = vec![0_u8; 4096];
            let _ = socket.read(&mut buf).await.expect("read test request");
            let body = serde_json::to_string(&InternalQueryResponse {
                result: json!({"ok": true}),
                consistency: "strong".to_owned(),
                source: "hot_owner".to_owned(),
            })
            .expect("serialize test response");
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body,
            );
            socket.write_all(response.as_bytes()).await.expect("write test response");
        });

        let response = try_owner_strong_query_endpoint(
            &Client::new(),
            &format!("http://{addr}"),
            "tenant",
            "instance",
            "summary",
            &json!({"ok": true}),
        )
        .await
        .expect("owner query should succeed");

        assert_eq!(response.result, json!({"ok": true}));
        assert_eq!(response.consistency, "strong");
        assert_eq!(response.source, "hot_owner");
    }

    fn graph_test_artifact() -> CompiledWorkflowArtifact {
        let mut artifact = CompiledWorkflowArtifact::new(
            "payments",
            3,
            "tsc-0.1",
            ArtifactEntrypoint {
                module: "workflows/payments.ts".to_owned(),
                export: "workflow".to_owned(),
            },
            CompiledWorkflow {
                initial_state: "charge-card".to_owned(),
                states: BTreeMap::from([
                    (
                        "charge-card".to_owned(),
                        CompiledStateNode::Step {
                            handler: "payments.charge".to_owned(),
                            input: Expression::Literal { value: json!({"charge": true}) },
                            next: Some("wait-payment".to_owned()),
                            task_queue: None,
                            retry: None,
                            config: None,
                            schedule_to_start_timeout_ms: None,
                            start_to_close_timeout_ms: None,
                            heartbeat_timeout_ms: None,
                            output_var: None,
                            on_error: Some(fabrik_workflow::ErrorTransition {
                                next: "failed".to_owned(),
                                error_var: Some("error".to_owned()),
                            }),
                        },
                    ),
                    (
                        "wait-payment".to_owned(),
                        CompiledStateNode::WaitForEvent {
                            event_type: "payment-approved".to_owned(),
                            next: "bulk-refund".to_owned(),
                            output_var: Some("signal".to_owned()),
                        },
                    ),
                    (
                        "bulk-refund".to_owned(),
                        CompiledStateNode::StartBulkActivity {
                            activity_type: "payments.refund".to_owned(),
                            items: Expression::Literal { value: json!([1, 2, 3]) },
                            next: "await-refund".to_owned(),
                            handle_var: "refund-batch".to_owned(),
                            task_queue: None,
                            execution_policy: None,
                            reducer: Some("count".to_owned()),
                            throughput_backend: Some("stream-v2".to_owned()),
                            chunk_size: Some(32),
                            retry: None,
                        },
                    ),
                    (
                        "await-refund".to_owned(),
                        CompiledStateNode::WaitForBulkActivity {
                            bulk_ref_var: "refund-batch".to_owned(),
                            next: "spawn-child".to_owned(),
                            output_var: Some("refund-summary".to_owned()),
                            on_error: None,
                        },
                    ),
                    (
                        "spawn-child".to_owned(),
                        CompiledStateNode::StartChild {
                            child_definition_id: "settlement-child".to_owned(),
                            input: Expression::Literal { value: json!({"orderId": 7}) },
                            next: "await-child".to_owned(),
                            handle_var: Some("child".to_owned()),
                            workflow_id: None,
                            task_queue: None,
                            parent_close_policy: fabrik_workflow::ParentClosePolicy::Terminate,
                        },
                    ),
                    (
                        "await-child".to_owned(),
                        CompiledStateNode::WaitForChild {
                            child_ref_var: "child".to_owned(),
                            next: "done".to_owned(),
                            output_var: Some("child-output".to_owned()),
                            on_error: None,
                        },
                    ),
                    ("done".to_owned(), CompiledStateNode::Succeed { output: None }),
                    (
                        "failed".to_owned(),
                        CompiledStateNode::Fail {
                            reason: Some(Expression::Literal { value: json!("failed") }),
                        },
                    ),
                ]),
                non_cancellable_states: BTreeSet::new(),
            },
        );
        artifact.source_files = vec!["workflows/payments.ts".to_owned()];
        artifact.source_map = BTreeMap::from([
            (
                "charge-card".to_owned(),
                SourceLocation { file: "workflows/payments.ts".to_owned(), line: 12, column: 2 },
            ),
            (
                "wait-payment".to_owned(),
                SourceLocation { file: "workflows/payments.ts".to_owned(), line: 18, column: 2 },
            ),
            (
                "signal:payment-approved".to_owned(),
                SourceLocation { file: "workflows/payments.ts".to_owned(), line: 22, column: 4 },
            ),
        ]);
        artifact.queries = BTreeMap::from([(
            "status".to_owned(),
            CompiledQueryHandler {
                arg_name: None,
                expr: Expression::Literal { value: json!("ok") },
            },
        )]);
        artifact.updates = BTreeMap::from([(
            "reprice".to_owned(),
            CompiledUpdateHandler {
                arg_name: None,
                initial_state: "apply".to_owned(),
                states: BTreeMap::from([(
                    "apply".to_owned(),
                    CompiledStateNode::Succeed { output: None },
                )]),
            },
        )]);
        artifact.artifact_hash = artifact.hash();
        artifact
    }

    #[test]
    fn normalize_artifact_graph_builds_semantic_modules_and_source_anchors() {
        let artifact = graph_test_artifact();
        let projection = normalize_artifact_graph(&artifact);

        assert!(projection.modules.iter().any(|module| module.kind == "entry"));
        assert!(
            projection
                .modules
                .iter()
                .any(|module| module.kind == "fan_out" || module.kind == "bulk_activity")
        );
        assert!(projection.modules.iter().any(|module| module.kind == "child_workflow"));
        assert!(projection.modules.iter().any(|module| module.kind == "update_handler"));
        assert!(projection.modules.iter().any(|module| module.kind == "query_handler"));
        let wait_module = projection
            .modules
            .iter()
            .find(|module| module.id == workflow_module_id("wait-payment"))
            .expect("wait module");
        assert_eq!(
            wait_module.source_anchor.as_ref().map(|anchor| anchor.file.as_str()),
            Some("workflows/payments.ts")
        );
        assert!(
            projection
                .module_edges
                .iter()
                .any(|edge| edge.source == workflow_module_id("charge-card")
                    && edge.target == workflow_module_id("wait-payment"))
        );
    }

    #[test]
    fn build_run_overlay_marks_current_and_failed_nodes() {
        let artifact = graph_test_artifact();
        let identity = WorkflowIdentity::new(
            "tenant-a",
            "payments",
            artifact.definition_version,
            artifact.artifact_hash.clone(),
            "instance-1",
            "run-9",
            "test",
        );

        let mut started =
            EventEnvelope::new("WorkflowStarted", identity.clone(), WorkflowEvent::WorkflowStarted);
        started.metadata.insert("state".to_owned(), "charge-card".to_owned());

        let scheduled = EventEnvelope::new(
            "ActivityTaskScheduled",
            identity.clone(),
            WorkflowEvent::ActivityTaskScheduled {
                activity_id: "charge-card".to_owned(),
                activity_type: "payments.charge".to_owned(),
                task_queue: "payments".to_owned(),
                attempt: 2,
                input: json!({"orderId": 7}),
                config: None,
                state: Some("charge-card".to_owned()),
                schedule_to_start_timeout_ms: None,
                start_to_close_timeout_ms: None,
                heartbeat_timeout_ms: None,
            },
        );

        let mut waiting = EventEnvelope::new(
            "SignalQueued",
            identity,
            WorkflowEvent::SignalQueued {
                signal_id: "sig-1".to_owned(),
                signal_type: "payment-approved".to_owned(),
                payload: json!({"ok": true}),
            },
        );
        waiting.metadata.insert("state".to_owned(), "wait-payment".to_owned());

        let now = Utc::now();
        let overlay = build_run_overlay(
            &artifact,
            "run-9",
            Some("wait-payment".to_owned()),
            &[started, scheduled, waiting],
            &[WorkflowActivityRecord {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-1".to_owned(),
                run_id: "run-9".to_owned(),
                definition_id: "payments".to_owned(),
                definition_version: Some(3),
                artifact_hash: Some(artifact.artifact_hash.clone()),
                activity_id: "charge-card".to_owned(),
                attempt: 2,
                activity_type: "payments.charge".to_owned(),
                task_queue: "payments".to_owned(),
                state: Some("charge-card".to_owned()),
                status: WorkflowActivityStatus::Failed,
                input: json!({"orderId": 7}),
                config: None,
                output: None,
                error: Some("card declined".to_owned()),
                cancellation_requested: false,
                cancellation_reason: None,
                cancellation_metadata: None,
                worker_id: Some("worker-a".to_owned()),
                worker_build_id: Some("build-a".to_owned()),
                scheduled_at: now,
                started_at: Some(now),
                last_heartbeat_at: None,
                lease_expires_at: None,
                completed_at: Some(now),
                schedule_to_start_timeout_ms: None,
                start_to_close_timeout_ms: None,
                heartbeat_timeout_ms: None,
                last_event_id: Uuid::now_v7(),
                last_event_type: "ActivityTaskFailed".to_owned(),
                updated_at: now,
            }],
            &[WorkflowSignalRecord {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-1".to_owned(),
                run_id: "run-9".to_owned(),
                signal_id: "sig-1".to_owned(),
                signal_type: "payment-approved".to_owned(),
                dedupe_key: None,
                payload: json!({"ok": true}),
                status: WorkflowSignalStatus::Queued,
                source_event_id: Uuid::now_v7(),
                dispatch_event_id: None,
                consumed_event_id: None,
                enqueued_at: now,
                consumed_at: None,
                updated_at: now,
            }],
            &[],
            &[],
            &[],
        );

        assert_eq!(overlay.current_node_id.as_deref(), Some("workflow:wait-payment"));
        assert!(
            overlay
                .node_statuses
                .iter()
                .any(|status| status.id == "workflow:charge-card" && status.status == "failed")
        );
        assert!(
            overlay
                .node_statuses
                .iter()
                .any(|status| status.id == "workflow:wait-payment" && status.status == "current")
        );
        assert!(
            overlay
                .signal_summaries
                .iter()
                .any(|summary| summary.signal_name == "payment-approved"
                    && summary.node_id.as_deref() == Some("workflow:wait-payment"))
        );
        assert_eq!(
            overlay.blocked_by.as_ref().map(|blocked| blocked.kind.as_str()),
            Some("signal")
        );
    }

    #[tokio::test]
    async fn workflow_definition_graph_endpoint_returns_normalized_graph() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state = test_state(store.clone()).await?;
        let artifact = graph_test_artifact();
        store.put_artifact("tenant-a", &artifact).await?;

        let Json(response) = get_workflow_definition_graph(
            Path(("tenant-a".to_owned(), "payments".to_owned())),
            State(state),
        )
        .await
        .expect("graph response");

        assert_eq!(response.definition_id, "payments");
        assert!(response.nodes.iter().any(|node| node.id == "workflow:charge-card"));
        assert!(response.modules.iter().any(|module| module.kind == "update_handler"));
        Ok(())
    }

    #[tokio::test]
    async fn list_runs_uses_current_projection_and_snapshots_for_filters() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state = test_state(store.clone()).await?;

        let historical_updated_at = Utc::now() - chrono::Duration::minutes(10);
        store
            .put_run_start(
                "tenant-a",
                "instance-1",
                "run-1",
                "payments",
                Some(1),
                Some("artifact-a"),
                "payments",
                Uuid::now_v7(),
                historical_updated_at - chrono::Duration::minutes(2),
                None,
                None,
            )
            .await?;
        store.close_run("tenant-a", "instance-1", "run-1", historical_updated_at).await?;
        store
            .put_snapshot(&WorkflowInstanceState {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-1".to_owned(),
                run_id: "run-1".to_owned(),
                definition_id: "payments".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                workflow_task_queue: "payments".to_owned(),
                sticky_workflow_build_id: Some("build-a".to_owned()),
                sticky_workflow_poller_id: Some("poller-a".to_owned()),
                current_state: Some("settled".to_owned()),
                context: None,
                artifact_execution: None,
                status: WorkflowStatus::Completed,
                input: Some(json!({"orderId": 1})),
                output: Some(json!({"ok": true})),
                event_count: 8,
                last_event_id: Uuid::now_v7(),
                last_event_type: "WorkflowCompleted".to_owned(),
                updated_at: historical_updated_at,
            })
            .await?;
        store
            .put_run_start(
                "tenant-a",
                "instance-1",
                "run-2",
                "payments",
                Some(2),
                Some("artifact-b"),
                "payments",
                Uuid::now_v7(),
                Utc::now() - chrono::Duration::minutes(1),
                Some("run-1"),
                Some("run-1"),
            )
            .await?;
        store
            .record_run_continuation(
                "tenant-a",
                "instance-1",
                "run-1",
                "run-2",
                "continue-as-new",
                Uuid::now_v7(),
                Uuid::now_v7(),
                Utc::now() - chrono::Duration::minutes(1),
            )
            .await?;
        store
            .upsert_instance(&WorkflowInstanceState {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-1".to_owned(),
                run_id: "run-2".to_owned(),
                definition_id: "payments".to_owned(),
                definition_version: Some(2),
                artifact_hash: Some("artifact-b".to_owned()),
                workflow_task_queue: "payments".to_owned(),
                sticky_workflow_build_id: Some("build-b".to_owned()),
                sticky_workflow_poller_id: Some("poller-b".to_owned()),
                current_state: Some("charge-card".to_owned()),
                context: None,
                artifact_execution: None,
                status: WorkflowStatus::Running,
                input: Some(json!({"orderId": 2})),
                output: None,
                event_count: 12,
                last_event_id: Uuid::now_v7(),
                last_event_type: "ActivityScheduled".to_owned(),
                updated_at: Utc::now(),
            })
            .await?;

        let Json(response) = list_runs(
            Path("tenant-a".to_owned()),
            Query(RunListQuery { status: Some("completed".to_owned()), ..RunListQuery::default() }),
            State(state),
        )
        .await
        .expect("list runs response");

        assert_eq!(response.run_count, 1);
        assert_eq!(response.items[0].run_id, "run-1");
        assert_eq!(response.items[0].status, "completed");
        assert_eq!(response.items[0].current_state.as_deref(), Some("settled"));
        assert_eq!(response.items[0].last_event_type.as_deref(), Some("WorkflowCompleted"));
        Ok(())
    }

    #[tokio::test]
    async fn list_runs_includes_routing_status_when_sticky_build_is_incompatible() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state = test_state(store.clone()).await?;

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
            .put_run_start(
                "tenant-a",
                "instance-1",
                "run-1",
                "payments",
                Some(1),
                Some("artifact-a"),
                "payments",
                Uuid::now_v7(),
                Utc::now(),
                None,
                None,
            )
            .await?;
        store
            .upsert_instance(&WorkflowInstanceState {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-1".to_owned(),
                run_id: "run-1".to_owned(),
                definition_id: "payments".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                workflow_task_queue: "payments".to_owned(),
                sticky_workflow_build_id: Some("build-b".to_owned()),
                sticky_workflow_poller_id: Some("poller-b".to_owned()),
                current_state: Some("charge-card".to_owned()),
                context: None,
                artifact_execution: None,
                status: WorkflowStatus::Running,
                input: Some(json!({"orderId": 2})),
                output: None,
                event_count: 12,
                last_event_id: Uuid::now_v7(),
                last_event_type: "ActivityScheduled".to_owned(),
                updated_at: Utc::now(),
            })
            .await?;
        store
            .update_run_workflow_sticky(
                "tenant-a",
                "instance-1",
                "run-1",
                "payments",
                "build-b",
                "poller-b",
                Utc::now(),
            )
            .await?;

        let Json(response) =
            list_runs(Path("tenant-a".to_owned()), Query(RunListQuery::default()), State(state))
                .await
                .expect("list runs response");

        assert_eq!(response.run_count, 1);
        assert_eq!(response.items[0].routing_status, "sticky_incompatible_fallback_required");
        Ok(())
    }

    #[tokio::test]
    async fn list_runs_can_filter_by_routing_status() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state = test_state(store.clone()).await?;

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

        for (instance_id, run_id, sticky_build_id) in
            [("instance-a", "run-a", Some("build-a")), ("instance-b", "run-b", Some("build-b"))]
        {
            store
                .put_run_start(
                    "tenant-a",
                    instance_id,
                    run_id,
                    "payments",
                    Some(1),
                    Some("artifact-a"),
                    "payments",
                    Uuid::now_v7(),
                    Utc::now(),
                    None,
                    None,
                )
                .await?;
            store
                .upsert_instance(&WorkflowInstanceState {
                    tenant_id: "tenant-a".to_owned(),
                    instance_id: instance_id.to_owned(),
                    run_id: run_id.to_owned(),
                    definition_id: "payments".to_owned(),
                    definition_version: Some(1),
                    artifact_hash: Some("artifact-a".to_owned()),
                    workflow_task_queue: "payments".to_owned(),
                    sticky_workflow_build_id: sticky_build_id.map(str::to_owned),
                    sticky_workflow_poller_id: Some(format!("poller-{run_id}")),
                    current_state: Some("charge-card".to_owned()),
                    context: None,
                    artifact_execution: None,
                    status: WorkflowStatus::Running,
                    input: Some(json!({"instance": instance_id})),
                    output: None,
                    event_count: 4,
                    last_event_id: Uuid::now_v7(),
                    last_event_type: "ActivityScheduled".to_owned(),
                    updated_at: Utc::now(),
                })
                .await?;
            if let Some(build_id) = sticky_build_id {
                store
                    .update_run_workflow_sticky(
                        "tenant-a",
                        instance_id,
                        run_id,
                        "payments",
                        build_id,
                        &format!("poller-{run_id}"),
                        Utc::now(),
                    )
                    .await?;
            }
        }

        let Json(response) = list_runs(
            Path("tenant-a".to_owned()),
            Query(RunListQuery {
                routing_status: Some("sticky_incompatible_fallback_required".to_owned()),
                ..RunListQuery::default()
            }),
            State(state),
        )
        .await
        .expect("list runs response");

        assert_eq!(response.run_count, 1);
        assert_eq!(response.items[0].instance_id, "instance-b");
        assert_eq!(response.items[0].routing_status, "sticky_incompatible_fallback_required");
        Ok(())
    }

    #[tokio::test]
    async fn list_workflows_can_filter_by_routing_status() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state = test_state(store.clone()).await?;

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

        store
            .upsert_instance(&WorkflowInstanceState {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-a".to_owned(),
                run_id: "run-a".to_owned(),
                definition_id: "payments".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                workflow_task_queue: "payments".to_owned(),
                sticky_workflow_build_id: Some("build-a".to_owned()),
                sticky_workflow_poller_id: Some("poller-a".to_owned()),
                current_state: Some("charge-card".to_owned()),
                context: None,
                artifact_execution: None,
                status: WorkflowStatus::Running,
                input: Some(json!({"instance": "a"})),
                output: None,
                event_count: 4,
                last_event_id: Uuid::now_v7(),
                last_event_type: "ActivityScheduled".to_owned(),
                updated_at: Utc::now(),
            })
            .await?;
        store
            .upsert_instance(&WorkflowInstanceState {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-b".to_owned(),
                run_id: "run-b".to_owned(),
                definition_id: "payments".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                workflow_task_queue: "payments".to_owned(),
                sticky_workflow_build_id: Some("build-z".to_owned()),
                sticky_workflow_poller_id: Some("poller-b".to_owned()),
                current_state: Some("charge-card".to_owned()),
                context: None,
                artifact_execution: None,
                status: WorkflowStatus::Running,
                input: Some(json!({"instance": "b"})),
                output: None,
                event_count: 4,
                last_event_id: Uuid::now_v7(),
                last_event_type: "ActivityScheduled".to_owned(),
                updated_at: Utc::now(),
            })
            .await?;

        let Json(response) = list_workflows(
            Path("tenant-a".to_owned()),
            Query(WorkflowListQuery {
                routing_status: Some("sticky_incompatible_fallback_required".to_owned()),
                ..WorkflowListQuery::default()
            }),
            State(state),
        )
        .await
        .expect("list workflows response");

        assert_eq!(response.workflow_count, 1);
        assert_eq!(response.items[0].instance_id, "instance-b");
        assert_eq!(response.items[0].routing_status, "sticky_incompatible_fallback_required");
        Ok(())
    }
}
