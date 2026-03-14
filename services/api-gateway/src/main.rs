use std::{
    collections::{BTreeMap, HashMap},
    convert::Infallible,
    env,
    time::Duration,
};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{OriginalUri, Path, Query, State},
    http::StatusCode,
    response::sse::{Event, Sse},
    routing::{get, post, put},
};
use chrono::Utc;
use fabrik_broker::{JsonTopicConfig, load_json_topic_latest_offsets};
use fabrik_config::{
    ExecutorRuntimeConfig, HttpServiceConfig, PostgresConfig, ThroughputRuntimeConfig,
};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::{
    BulkBatchRoutingCount, TaskQueueKind, TopicAdapterAction, TopicAdapterDeadLetterPolicy,
    TopicAdapterKind, TopicAdapterRecord, TopicAdapterUpsert, WorkflowRunFilters, WorkflowStore,
    resolve_topic_adapter_dispatch_detailed, validate_topic_adapter_config,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{spawn, sync::mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;

#[derive(Clone)]
struct AppState {
    client: Client,
    ingest_base: String,
    query_base: String,
    matching_base: String,
    store: WorkflowStore,
    admission: AdmissionDebugConfig,
}

#[derive(Debug, Clone)]
struct AdmissionDebugConfig {
    default_backend: String,
    task_queue_backends: BTreeMap<String, String>,
    max_active_chunks_per_tenant: usize,
    max_active_chunks_per_task_queue: usize,
}

impl AdmissionDebugConfig {
    fn from_env() -> Result<Self> {
        let executor = ExecutorRuntimeConfig::from_env()
            .context("failed to load executor routing config for admission debug")?;
        let throughput = ThroughputRuntimeConfig::from_env()
            .context("failed to load throughput runtime config for admission debug")?;
        Ok(Self {
            default_backend: executor.throughput_default_backend,
            task_queue_backends: executor.throughput_task_queue_backends,
            max_active_chunks_per_tenant: throughput.max_active_chunks_per_tenant,
            max_active_chunks_per_task_queue: throughput.max_active_chunks_per_task_queue,
        })
    }
}

#[derive(Debug, Deserialize)]
struct RegisterBuildRequest {
    build_id: String,
    #[serde(default)]
    artifact_hashes: Vec<String>,
    #[serde(default)]
    metadata: Option<Value>,
    #[serde(default)]
    compatibility_set_id: Option<String>,
    #[serde(default)]
    promote_default: bool,
}

#[derive(Debug, Deserialize)]
struct CompatibilitySetRequest {
    build_ids: Vec<String>,
    #[serde(default)]
    is_default: bool,
}

#[derive(Debug, Deserialize)]
struct DefaultSetRequest {
    set_id: String,
}

#[derive(Debug, Deserialize)]
struct ThroughputPolicyRequest {
    backend: String,
}

#[derive(Debug, Deserialize)]
struct TaskQueueRuntimeControlRequest {
    #[serde(default)]
    is_paused: bool,
    #[serde(default)]
    is_draining: bool,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BulkBatchRuntimeControlRequest {
    #[serde(default)]
    is_paused: bool,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TopicAdapterRequest {
    adapter_kind: TopicAdapterKind,
    brokers: String,
    topic_name: String,
    topic_partitions: i32,
    action: TopicAdapterAction,
    #[serde(default)]
    definition_id: Option<String>,
    #[serde(default)]
    signal_type: Option<String>,
    #[serde(default)]
    workflow_task_queue: Option<String>,
    #[serde(default)]
    workflow_instance_id_json_pointer: Option<String>,
    #[serde(default)]
    payload_json_pointer: Option<String>,
    #[serde(default)]
    payload_template_json: Option<Value>,
    #[serde(default)]
    memo_json_pointer: Option<String>,
    #[serde(default)]
    memo_template_json: Option<Value>,
    #[serde(default)]
    search_attributes_json_pointer: Option<String>,
    #[serde(default)]
    search_attributes_template_json: Option<Value>,
    #[serde(default)]
    request_id_json_pointer: Option<String>,
    #[serde(default)]
    dedupe_key_json_pointer: Option<String>,
    #[serde(default = "default_topic_adapter_dead_letter_policy")]
    dead_letter_policy: TopicAdapterDeadLetterPolicy,
    #[serde(default)]
    is_paused: bool,
}

#[derive(Debug, Deserialize)]
struct TopicAdapterDeadLetterQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct TopicAdapterDeleteQuery {
    force: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct TopicAdapterPreviewRequest {
    payload: Value,
    #[serde(default)]
    partition_id: i32,
    #[serde(default)]
    log_offset: i64,
}

#[derive(Debug, Deserialize)]
struct TopicAdapterDraftPreviewRequest {
    #[serde(default)]
    adapter_id: Option<String>,
    adapter: TopicAdapterRequest,
    payload: Value,
    #[serde(default)]
    partition_id: i32,
    #[serde(default)]
    log_offset: i64,
}

#[derive(Debug, Serialize)]
struct TopicAdapterLagPartition {
    partition_id: i32,
    committed_offset: Option<i64>,
    latest_offset: i64,
    lag_records: i64,
}

#[derive(Debug, Serialize)]
struct WatchEventEnvelope {
    event_type: String,
    occurred_at: chrono::DateTime<Utc>,
    consistency: String,
    authoritative_source: String,
    projection_lag_ms: Option<i64>,
    owner_id: Option<String>,
    partition_id: Option<i32>,
    cursor: String,
    body: Value,
}

#[derive(Debug, Serialize)]
struct WorkflowRoutingResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    definition_id: String,
    definition_version: Option<u32>,
    artifact_hash: Option<String>,
    workflow_task_queue: String,
    routing_status: &'static str,
    default_compatibility_set_id: Option<String>,
    compatible_build_ids: Vec<String>,
    registered_build_ids: Vec<String>,
    sticky_workflow_build_id: Option<String>,
    sticky_workflow_poller_id: Option<String>,
    sticky_updated_at: Option<chrono::DateTime<Utc>>,
    sticky_build_compatible_with_queue: Option<bool>,
    sticky_build_supports_pinned_artifact: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
struct SearchQuery {
    #[serde(default)]
    q: String,
    #[serde(default)]
    tenant_id: String,
    limit: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = HttpServiceConfig::from_env("API_GATEWAY", "api-gateway", 3000)?;
    let postgres = PostgresConfig::from_env()?;
    let admission = AdmissionDebugConfig::from_env()?;
    init_tracing(&config.log_filter);
    info!(port = config.port, "starting api gateway");

    let store = WorkflowStore::connect(&postgres.url).await?;
    store.init().await?;
    let state = AppState {
        client: Client::new(),
        ingest_base: env::var("INGEST_SERVICE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:3001".to_owned()),
        query_base: env::var("QUERY_SERVICE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:3005".to_owned()),
        matching_base: env::var("MATCHING_DEBUG_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:3004".to_owned()),
        store,
        admission,
    };

    let app = build_app(state, config.name);
    serve(app, config.port).await
}

fn build_app(state: AppState, service_name: String) -> Router {
    default_router::<AppState>(ServiceInfo::new(
        service_name,
        "edge-api",
        env!("CARGO_PKG_VERSION"),
    ))
    .route("/tenants", get(proxy_to_query_get))
    .route("/tenants/{tenant_id}/overview", get(proxy_to_query_get))
    .route("/tenants/{tenant_id}/workflows", get(proxy_to_query_get))
    .route("/tenants/{tenant_id}/runs", get(proxy_to_query_get))
    .route("/workflows/{workflow_id}/trigger", post(proxy_to_ingest))
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/signals/{signal_type}",
        post(proxy_to_ingest),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/updates/{update_id}",
        post(proxy_to_ingest),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/terminate",
        post(proxy_to_ingest),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/continue-as-new",
        post(proxy_to_ingest),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/activities/{activity_id}/cancel",
        post(proxy_to_ingest),
    )
    .route("/tenants/{tenant_id}/workflows/{workflow_instance_id}", get(proxy_to_query_get))
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/queries/{query_name}",
        post(proxy_to_query_post),
    )
    .route("/tenants/{tenant_id}/workflows/{workflow_instance_id}/runs", get(proxy_to_query_get))
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/snapshot",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/activities",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/signals",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/updates",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/updates/{update_id}",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/runs/{run_id}/activities",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/runs/{run_id}/signals",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/runs/{run_id}/updates",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/runs/{run_id}/updates/{update_id}",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/runs/{run_id}/bulk-batches",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/runs/{run_id}/bulk-batches/{batch_id}",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/runs/{run_id}/bulk-batches/{batch_id}/chunks",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/runs/{run_id}/bulk-batches/{batch_id}/results",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/runs/{run_id}/bulk-batches/{batch_id}/watch",
        get(watch_bulk_batch),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/runs/{run_id}/watch",
        get(watch_workflow_run),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/history",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/runs/{run_id}/history",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/replay",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/runs/{run_id}/replay",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/runs/{run_id}/graph",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/execution-graph",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflow-definitions",
        get(proxy_to_query_get).post(proxy_to_ingest),
    )
    .route(
        "/tenants/{tenant_id}/workflow-definitions/{definition_id}/latest",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflow-definitions/{definition_id}/graph",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflow-definitions/{definition_id}/versions/{version}",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflow-artifacts/validate",
        post(proxy_to_ingest),
    )
    .route(
        "/tenants/{tenant_id}/workflow-artifacts",
        get(proxy_to_query_get).post(proxy_to_ingest),
    )
    .route(
        "/tenants/{tenant_id}/workflow-artifacts/{definition_id}/latest",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflow-artifacts/{definition_id}/versions/{version}",
        get(proxy_to_query_get),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/routing",
        get(get_workflow_routing),
    )
    .route("/search", get(search))
    .route("/admin/tenants", get(list_tenants))
    .route(
        "/admin/tenants/{tenant_id}/task-queues/{queue_kind}/{task_queue}/builds",
        post(register_build),
    )
    .route(
        "/admin/tenants/{tenant_id}/task-queues/{queue_kind}/{task_queue}/compatibility-sets/{set_id}",
        put(upsert_compatibility_set),
    )
    .route(
        "/admin/tenants/{tenant_id}/task-queues/{queue_kind}/{task_queue}/default-set",
        post(set_default_set),
    )
    .route(
        "/admin/tenants/{tenant_id}/task-queues/{queue_kind}/{task_queue}/throughput-policy",
        put(upsert_throughput_policy),
    )
    .route(
        "/admin/tenants/{tenant_id}/task-queues/{queue_kind}/{task_queue}/runtime-control",
        put(upsert_task_queue_runtime_control),
    )
    .route(
        "/admin/tenants/{tenant_id}/task-queues/{queue_kind}/{task_queue}",
        get(get_task_queue_inspection),
    )
    .route(
        "/admin/tenants/{tenant_id}/task-queues/{queue_kind}/{task_queue}/watch",
        get(watch_task_queue),
    )
    .route("/admin/tenants/{tenant_id}/task-queues", get(list_task_queues))
    .route("/admin/tenants/{tenant_id}/topic-adapters", get(list_topic_adapters))
    .route(
        "/admin/tenants/{tenant_id}/topic-adapters/preview-draft",
        post(preview_topic_adapter_draft),
    )
    .route(
        "/admin/tenants/{tenant_id}/topic-adapters/{adapter_id}",
        get(get_topic_adapter).put(upsert_topic_adapter).delete(delete_topic_adapter),
    )
    .route(
        "/admin/tenants/{tenant_id}/topic-adapters/{adapter_id}/pause",
        post(pause_topic_adapter),
    )
    .route(
        "/admin/tenants/{tenant_id}/topic-adapters/{adapter_id}/resume",
        post(resume_topic_adapter),
    )
    .route(
        "/admin/tenants/{tenant_id}/topic-adapters/{adapter_id}/dead-letters",
        get(list_topic_adapter_dead_letters),
    )
    .route(
        "/admin/tenants/{tenant_id}/topic-adapters/{adapter_id}/preview",
        post(preview_topic_adapter),
    )
    .route(
        "/admin/tenants/{tenant_id}/topic-adapters/{adapter_id}/watch",
        get(watch_topic_adapter),
    )
    .route(
        "/admin/tenants/{tenant_id}/workflows/{workflow_instance_id}/runs/{run_id}/bulk-batches/{batch_id}/runtime-control",
        put(upsert_bulk_batch_runtime_control),
    )
    .route("/admin/debug/matching/activity-results", get(proxy_to_matching_get))
    .with_state(state)
}

async fn proxy_to_ingest(
    State(state): State<AppState>,
    uri: OriginalUri,
    body: Bytes,
) -> Result<(StatusCode, Bytes), (StatusCode, String)> {
    proxy_request(state.client.clone(), state.ingest_base.clone(), uri, Some(body)).await
}

async fn proxy_to_query_get(
    State(state): State<AppState>,
    uri: OriginalUri,
) -> Result<(StatusCode, Bytes), (StatusCode, String)> {
    proxy_request(state.client.clone(), state.query_base.clone(), uri, None).await
}

async fn proxy_to_query_post(
    State(state): State<AppState>,
    uri: OriginalUri,
    body: Bytes,
) -> Result<(StatusCode, Bytes), (StatusCode, String)> {
    proxy_request(state.client.clone(), state.query_base.clone(), uri, Some(body)).await
}

async fn proxy_to_matching_get(
    State(state): State<AppState>,
    _uri: OriginalUri,
) -> Result<(StatusCode, Bytes), (StatusCode, String)> {
    proxy_get_path(state.client.clone(), state.matching_base.clone(), "/debug/activity-results")
        .await
}

async fn register_build(
    Path((tenant_id, queue_kind, task_queue)): Path<(String, String, String)>,
    State(state): State<AppState>,
    Json(request): Json<RegisterBuildRequest>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let queue_kind = parse_queue_kind(&queue_kind)?;
    let record = state
        .store
        .register_task_queue_build(
            &tenant_id,
            queue_kind.clone(),
            &task_queue,
            &request.build_id,
            &request.artifact_hashes,
            request.metadata.as_ref(),
        )
        .await
        .map_err(internal_error)?;
    if request.promote_default {
        let set_id = request
            .compatibility_set_id
            .clone()
            .unwrap_or_else(|| format!("default-{}", request.build_id));
        state
            .store
            .upsert_compatibility_set(
                &tenant_id,
                queue_kind,
                &task_queue,
                &set_id,
                &[request.build_id.clone()],
                true,
            )
            .await
            .map_err(internal_error)?;
    }
    Ok(Json(serde_json::to_value(record).expect("build record serializes")))
}

async fn upsert_compatibility_set(
    Path((tenant_id, queue_kind, task_queue, set_id)): Path<(String, String, String, String)>,
    State(state): State<AppState>,
    Json(request): Json<CompatibilitySetRequest>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let queue_kind = parse_queue_kind(&queue_kind)?;
    let record = state
        .store
        .upsert_compatibility_set(
            &tenant_id,
            queue_kind,
            &task_queue,
            &set_id,
            &request.build_ids,
            request.is_default,
        )
        .await
        .map_err(internal_error)?;
    Ok(Json(serde_json::to_value(record).expect("compatibility set serializes")))
}

async fn set_default_set(
    Path((tenant_id, queue_kind, task_queue)): Path<(String, String, String)>,
    State(state): State<AppState>,
    Json(request): Json<DefaultSetRequest>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let queue_kind = parse_queue_kind(&queue_kind)?;
    state
        .store
        .set_default_compatibility_set(&tenant_id, queue_kind, &task_queue, &request.set_id)
        .await
        .map_err(internal_error)?;
    Ok(Json(serde_json::json!({
        "tenant_id": tenant_id,
        "task_queue": task_queue,
        "default_set_id": request.set_id,
        "status": "updated"
    })))
}

async fn upsert_throughput_policy(
    Path((tenant_id, queue_kind, task_queue)): Path<(String, String, String)>,
    State(state): State<AppState>,
    Json(request): Json<ThroughputPolicyRequest>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let queue_kind = parse_queue_kind(&queue_kind)?;
    let record = state
        .store
        .upsert_task_queue_throughput_policy(&tenant_id, queue_kind, &task_queue, &request.backend)
        .await
        .map_err(internal_error)?;
    Ok(Json(serde_json::to_value(record).expect("throughput policy serializes")))
}

async fn upsert_task_queue_runtime_control(
    Path((tenant_id, queue_kind, task_queue)): Path<(String, String, String)>,
    State(state): State<AppState>,
    Json(request): Json<TaskQueueRuntimeControlRequest>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let queue_kind = parse_queue_kind(&queue_kind)?;
    let record = state
        .store
        .upsert_task_queue_runtime_control(
            &tenant_id,
            queue_kind,
            &task_queue,
            request.is_paused,
            request.is_draining,
            request.reason.as_deref(),
        )
        .await
        .map_err(internal_error)?;
    Ok(Json(serde_json::to_value(record).expect("task queue runtime control serializes")))
}

async fn upsert_bulk_batch_runtime_control(
    Path((tenant_id, workflow_instance_id, run_id, batch_id)): Path<(
        String,
        String,
        String,
        String,
    )>,
    State(state): State<AppState>,
    Json(request): Json<BulkBatchRuntimeControlRequest>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let record = state
        .store
        .upsert_bulk_batch_runtime_control(
            &tenant_id,
            &workflow_instance_id,
            &run_id,
            &batch_id,
            request.is_paused,
            request.reason.as_deref(),
        )
        .await
        .map_err(internal_error)?;
    Ok(Json(serde_json::to_value(record).expect("bulk batch runtime control serializes")))
}

async fn get_task_queue_inspection(
    Path((tenant_id, queue_kind, task_queue)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let queue_kind = parse_queue_kind(&queue_kind)?;
    let inspection = state
        .store
        .inspect_task_queue(&tenant_id, queue_kind, &task_queue, Utc::now())
        .await
        .map_err(internal_error)?;
    let persisted_backend =
        inspection.throughput_policy.as_ref().map(|policy| policy.backend.clone());
    let mut payload = serde_json::to_value(inspection).expect("inspection serializes");
    let admission = task_queue_admission_snapshot(
        &state,
        &tenant_id,
        &task_queue,
        persisted_backend.as_deref(),
    )
    .await
    .map_err(internal_error)?;
    payload["admission"] = admission;
    payload["watch_cursor"] = Value::String(Utc::now().to_rfc3339());
    Ok(Json(payload))
}

async fn list_task_queues(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let queue_refs =
        state.store.list_task_queue_refs_for_tenant(&tenant_id).await.map_err(internal_error)?;
    let mut items = Vec::with_capacity(queue_refs.len());
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
        let admission = task_queue_admission_snapshot(
            &state,
            &tenant_id,
            &inspection.task_queue,
            inspection.throughput_policy.as_ref().map(|policy| policy.backend.as_str()),
        )
        .await
        .map_err(internal_error)?;
        items.push(json!({
            "tenant_id": inspection.tenant_id,
            "queue_kind": queue_kind_label(&inspection.queue_kind),
            "task_queue": inspection.task_queue,
            "backlog": inspection.backlog,
            "oldest_backlog_at": inspection.oldest_backlog_at,
            "poller_count": inspection.pollers.len(),
            "registered_build_count": inspection.registered_builds.len(),
            "default_set_id": inspection.default_set_id,
            "throughput_backend": inspection.throughput_policy.as_ref().map(|policy| policy.backend.clone()),
            "effective_throughput_backend": admission.get("effective_preferred_backend").cloned().unwrap_or(Value::Null),
            "is_paused": inspection.runtime_control.as_ref().map(|control| control.is_paused).unwrap_or(false),
            "is_draining": inspection.runtime_control.as_ref().map(|control| control.is_draining).unwrap_or(false),
            "stream_v2_capacity_state": admission
                .get("stream_v2_capacity")
                .and_then(|value| value.get("state"))
                .cloned()
                .unwrap_or(Value::Null),
            "active_stream_v2_chunks": admission
                .get("stream_v2_active_chunks")
                .and_then(|value| value.get("task_queue"))
                .cloned()
                .unwrap_or(Value::Null),
            "sticky_hit_rate": inspection.sticky_effectiveness.as_ref().map(|metrics| metrics.sticky_hit_rate),
            "consistency": "eventual",
            "source": "projection"
        }));
    }
    items.sort_by(|left, right| {
        let left_backlog = left.get("backlog").and_then(Value::as_u64).unwrap_or(0);
        let right_backlog = right.get("backlog").and_then(Value::as_u64).unwrap_or(0);
        right_backlog.cmp(&left_backlog)
    });
    Ok(Json(json!({
        "tenant_id": tenant_id,
        "consistency": "eventual",
        "authoritative_source": "projection",
        "queue_count": items.len(),
        "items": items
    })))
}

async fn watch_bulk_batch(
    Path((tenant_id, workflow_instance_id, run_id, batch_id)): Path<(
        String,
        String,
        String,
        String,
    )>,
    State(state): State<AppState>,
) -> Sse<ReceiverStream<Result<Event, Infallible>>> {
    let (tx, rx) = mpsc::channel(16);
    spawn(async move {
        let mut last_batch = None::<String>;
        let mut last_lag = None::<i64>;
        let mut last_terminal = None::<String>;
        let mut last_routing = None::<String>;
        let mut last_capacity = None::<String>;
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            let strong = query_json(
                &state,
                &format!(
                    "/tenants/{tenant_id}/workflows/{workflow_instance_id}/runs/{run_id}/bulk-batches/{batch_id}?consistency=strong"
                ),
            )
            .await;
            let eventual = query_json(
                &state,
                &format!(
                    "/tenants/{tenant_id}/workflows/{workflow_instance_id}/runs/{run_id}/bulk-batches/{batch_id}?consistency=eventual"
                ),
            )
            .await;

            if let Ok(strong_body) = strong {
                if let Ok(encoded) = serde_json::to_string(&strong_body) {
                    if last_batch.as_ref() != Some(&encoded) {
                        let status = strong_body
                            .get("batch")
                            .and_then(|value| value.get("status"))
                            .and_then(Value::as_str)
                            .unwrap_or("unknown")
                            .to_owned();
                        let event_type =
                            if matches!(status.as_str(), "completed" | "failed" | "cancelled") {
                                "batch_terminal"
                            } else {
                                "batch_progress"
                            };
                        if send_watch_event(
                            &tx,
                            event_type,
                            strong_body
                                .get("consistency")
                                .and_then(Value::as_str)
                                .unwrap_or("strong"),
                            strong_body
                                .get("authoritative_source")
                                .and_then(Value::as_str)
                                .unwrap_or("stream-v2-owner-state"),
                            strong_body.get("projection_lag_ms").and_then(Value::as_i64),
                            strong_body
                                .get("watch_cursor")
                                .and_then(Value::as_str)
                                .map(str::to_owned)
                                .unwrap_or_else(|| Utc::now().to_rfc3339()),
                            strong_body.clone(),
                        )
                        .await
                        {
                            break;
                        }
                        let batch = strong_body.get("batch").cloned().unwrap_or(Value::Null);
                        let routing_body = json!({
                            "batch_id": batch_id,
                            "task_queue": batch.get("task_queue").cloned().unwrap_or(Value::Null),
                            "selected_backend": batch.get("selected_backend").cloned().unwrap_or(Value::Null),
                            "routing_reason": batch.get("routing_reason").cloned().unwrap_or(Value::Null),
                            "admission_policy_version": batch.get("admission_policy_version").cloned().unwrap_or(Value::Null),
                            "reducer_kind": batch.get("reducer_kind").cloned().unwrap_or(Value::Null),
                            "reducer_execution_path": batch.get("reducer_execution_path").cloned().unwrap_or(Value::Null),
                            "reducer_output": batch.get("reducer_output").cloned().unwrap_or(Value::Null),
                            "status": batch.get("status").cloned().unwrap_or(Value::Null),
                        });
                        if let Ok(encoded_routing) = serde_json::to_string(&routing_body) {
                            if last_routing.as_ref() != Some(&encoded_routing) {
                                if send_watch_event(
                                    &tx,
                                    "routing_changed",
                                    strong_body
                                        .get("consistency")
                                        .and_then(Value::as_str)
                                        .unwrap_or("strong"),
                                    strong_body
                                        .get("authoritative_source")
                                        .and_then(Value::as_str)
                                        .unwrap_or("stream-v2-owner-state"),
                                    strong_body.get("projection_lag_ms").and_then(Value::as_i64),
                                    strong_body
                                        .get("watch_cursor")
                                        .and_then(Value::as_str)
                                        .map(str::to_owned)
                                        .unwrap_or_else(|| Utc::now().to_rfc3339()),
                                    routing_body,
                                )
                                .await
                                {
                                    break;
                                }
                                last_routing = Some(encoded_routing);
                            }
                        }
                        last_batch = Some(encoded);
                        if event_type == "batch_terminal" {
                            last_terminal = Some(status);
                        }
                    }
                }
            }

            if let Ok(eventual_body) = eventual {
                let lag = eventual_body.get("projection_lag_ms").and_then(Value::as_i64);
                if lag != last_lag {
                    let consistency = eventual_body
                        .get("consistency")
                        .and_then(Value::as_str)
                        .unwrap_or("eventual")
                        .to_owned();
                    let authoritative_source = eventual_body
                        .get("authoritative_source")
                        .and_then(Value::as_str)
                        .unwrap_or("projection")
                        .to_owned();
                    let cursor = eventual_body
                        .get("watch_cursor")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .unwrap_or_else(|| Utc::now().to_rfc3339());
                    if send_watch_event(
                        &tx,
                        "projection_lag",
                        &consistency,
                        &authoritative_source,
                        lag,
                        cursor,
                        eventual_body,
                    )
                    .await
                    {
                        break;
                    }
                    last_lag = lag;
                }
            }

            if let Ok(strong_body) = query_json(
                &state,
                &format!(
                    "/tenants/{tenant_id}/workflows/{workflow_instance_id}/runs/{run_id}/bulk-batches/{batch_id}?consistency=strong"
                ),
            )
            .await
            {
                if let Some(task_queue) = strong_body
                    .get("batch")
                    .and_then(|value| value.get("task_queue"))
                    .and_then(Value::as_str)
                {
                    let admission = task_queue_admission_snapshot(
                        &state,
                        &tenant_id,
                        task_queue,
                        strong_body
                            .get("batch")
                            .and_then(|value| value.get("selected_backend"))
                            .and_then(Value::as_str),
                    )
                    .await;
                    if let Ok(admission) = admission {
                        let capacity_body = json!({
                            "batch_id": batch_id,
                            "task_queue": task_queue,
                            "selected_backend": strong_body
                                .get("batch")
                                .and_then(|value| value.get("selected_backend"))
                                .cloned()
                                .unwrap_or(Value::Null),
                            "routing_reason": strong_body
                                .get("batch")
                                .and_then(|value| value.get("routing_reason"))
                                .cloned()
                                .unwrap_or(Value::Null),
                            "reducer_kind": strong_body
                                .get("batch")
                                .and_then(|value| value.get("reducer_kind"))
                                .cloned()
                                .unwrap_or(Value::Null),
                            "reducer_execution_path": strong_body
                                .get("batch")
                                .and_then(|value| value.get("reducer_execution_path"))
                                .cloned()
                                .unwrap_or(Value::Null),
                            "reducer_output": strong_body
                                .get("batch")
                                .and_then(|value| value.get("reducer_output"))
                                .cloned()
                                .unwrap_or(Value::Null),
                            "admission": admission,
                        });
                        if let Ok(encoded_capacity) = serde_json::to_string(&capacity_body) {
                            if last_capacity.as_ref() != Some(&encoded_capacity) {
                                if send_watch_event(
                                    &tx,
                                    "capacity_pressure",
                                    strong_body
                                        .get("consistency")
                                        .and_then(Value::as_str)
                                        .unwrap_or("strong"),
                                    "stream-v2-capacity",
                                    strong_body.get("projection_lag_ms").and_then(Value::as_i64),
                                    strong_body
                                        .get("watch_cursor")
                                        .and_then(Value::as_str)
                                        .map(str::to_owned)
                                        .unwrap_or_else(|| Utc::now().to_rfc3339()),
                                    capacity_body,
                                )
                                .await
                                {
                                    break;
                                }
                                last_capacity = Some(encoded_capacity);
                            }
                        }
                    }
                }
            }

            if last_terminal.is_some() {
                break;
            }
            interval.tick().await;
        }
    });
    Sse::new(ReceiverStream::new(rx))
}

async fn watch_workflow_run(
    Path((tenant_id, workflow_instance_id, run_id)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> Sse<ReceiverStream<Result<Event, Infallible>>> {
    let (tx, rx) = mpsc::channel(16);
    spawn(async move {
        let mut last_body = None::<String>;
        let mut last_batches = None::<String>;
        let mut last_batch_deltas = HashMap::<String, String>::new();
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            let run = query_json(
                &state,
                &format!(
                    "/tenants/{tenant_id}/runs?instance_id={workflow_instance_id}&run_id={run_id}&limit=1"
                ),
            )
            .await;
            if let Ok(run_body) = run {
                if let Ok(encoded) = serde_json::to_string(&run_body) {
                    if last_body.as_ref() != Some(&encoded) {
                        let consistency = run_body
                            .get("consistency")
                            .and_then(Value::as_str)
                            .unwrap_or("eventual")
                            .to_owned();
                        let authoritative_source = run_body
                            .get("authoritative_source")
                            .and_then(Value::as_str)
                            .unwrap_or("projection")
                            .to_owned();
                        let cursor = run_body
                            .get("items")
                            .and_then(Value::as_array)
                            .and_then(|items| items.first())
                            .and_then(|item| item.get("updated_at"))
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                            .unwrap_or_else(|| Utc::now().to_rfc3339());
                        if send_watch_event(
                            &tx,
                            "workflow_state_changed",
                            &consistency,
                            &authoritative_source,
                            None,
                            cursor,
                            run_body,
                        )
                        .await
                        {
                            break;
                        }
                        last_body = Some(encoded);
                    }
                }
            }
            let bulk_batches = query_json(
                &state,
                &format!(
                    "/tenants/{tenant_id}/workflows/{workflow_instance_id}/runs/{run_id}/bulk-batches?consistency=eventual"
                ),
            )
            .await;
            if let Ok(batch_body) = bulk_batches {
                if let Ok(encoded) = serde_json::to_string(&batch_body) {
                    if last_batches.as_ref() != Some(&encoded) {
                        let consistency = batch_body
                            .get("consistency")
                            .and_then(Value::as_str)
                            .unwrap_or("eventual")
                            .to_owned();
                        let authoritative_source = batch_body
                            .get("authoritative_source")
                            .and_then(Value::as_str)
                            .unwrap_or("projection")
                            .to_owned();
                        let cursor = batch_body
                            .get("watch_cursor")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                            .unwrap_or_else(|| Utc::now().to_rfc3339());
                        if send_watch_event(
                            &tx,
                            "throughput_batches_changed",
                            &consistency,
                            &authoritative_source,
                            batch_body.get("projection_lag_ms").and_then(Value::as_i64),
                            cursor.clone(),
                            batch_body.clone(),
                        )
                        .await
                        {
                            break;
                        }
                        let mut watch_closed = false;
                        if let Some(batches) = batch_body.get("batches").and_then(Value::as_array) {
                            let mut current_batch_ids = Vec::with_capacity(batches.len());
                            for batch in batches {
                                let Some(batch_id) = batch
                                    .get("batch_id")
                                    .and_then(Value::as_str)
                                    .map(str::to_owned)
                                else {
                                    continue;
                                };
                                current_batch_ids.push(batch_id.clone());
                                let delta_body = json!({
                                    "batch_id": batch_id,
                                    "batch": batch.clone(),
                                });
                                if let Ok(encoded_delta) = serde_json::to_string(&delta_body) {
                                    if last_batch_deltas.get(&batch_id) != Some(&encoded_delta) {
                                        if send_watch_event(
                                            &tx,
                                            "throughput_batch_delta",
                                            &consistency,
                                            &authoritative_source,
                                            batch_body
                                                .get("projection_lag_ms")
                                                .and_then(Value::as_i64),
                                            cursor.clone(),
                                            delta_body,
                                        )
                                        .await
                                        {
                                            watch_closed = true;
                                            break;
                                        }
                                        last_batch_deltas.insert(batch_id, encoded_delta);
                                    }
                                }
                            }
                            last_batch_deltas.retain(|batch_id, _| {
                                current_batch_ids.iter().any(|current| current == batch_id)
                            });
                        }
                        if watch_closed {
                            break;
                        }
                        last_batches = Some(encoded);
                    }
                }
            }
            interval.tick().await;
        }
    });
    Sse::new(ReceiverStream::new(rx))
}

async fn watch_task_queue(
    Path((tenant_id, queue_kind, task_queue)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> Sse<ReceiverStream<Result<Event, Infallible>>> {
    let (tx, rx) = mpsc::channel(16);
    spawn(async move {
        let Ok(queue_kind) = parse_queue_kind(&queue_kind) else {
            return;
        };
        let mut last_body = None::<String>;
        let mut last_throttle = None::<String>;
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            let inspection = state
                .store
                .inspect_task_queue(&tenant_id, queue_kind.clone(), &task_queue, Utc::now())
                .await;
            if let Ok(inspection) = inspection {
                let mut body = serde_json::to_value(inspection).expect("inspection serializes");
                let cursor = Utc::now().to_rfc3339();
                body["watch_cursor"] = Value::String(cursor.clone());
                if let Ok(encoded) = serde_json::to_string(&body) {
                    if last_body.as_ref() != Some(&encoded) {
                        if send_watch_event(
                            &tx,
                            "queue_health",
                            "eventual",
                            "projection",
                            None,
                            cursor.clone(),
                            body.clone(),
                        )
                        .await
                        {
                            break;
                        }
                        last_body = Some(encoded);
                    }
                }

                let throttle_key = body
                    .get("runtime_control")
                    .and_then(|value| value.get("is_paused"))
                    .and_then(Value::as_bool)
                    .map(|paused| {
                        let draining = body
                            .get("runtime_control")
                            .and_then(|value| value.get("is_draining"))
                            .and_then(Value::as_bool)
                            .unwrap_or(false);
                        format!("paused={paused};draining={draining}")
                    });
                if throttle_key != last_throttle
                    && throttle_key.as_deref() != Some("paused=false;draining=false")
                {
                    if send_watch_event(
                        &tx,
                        "queue_throttle",
                        "eventual",
                        "projection",
                        None,
                        cursor,
                        body,
                    )
                    .await
                    {
                        break;
                    }
                    last_throttle = throttle_key;
                }
            }
            interval.tick().await;
        }
    });
    Sse::new(ReceiverStream::new(rx))
}

async fn send_watch_event(
    tx: &mpsc::Sender<Result<Event, Infallible>>,
    event_type: &str,
    consistency: &str,
    authoritative_source: &str,
    projection_lag_ms: Option<i64>,
    cursor: String,
    body: Value,
) -> bool {
    let payload = WatchEventEnvelope {
        event_type: event_type.to_owned(),
        occurred_at: Utc::now(),
        consistency: consistency.to_owned(),
        authoritative_source: authoritative_source.to_owned(),
        projection_lag_ms,
        owner_id: None,
        partition_id: None,
        cursor,
        body,
    };
    let event = match Event::default().event(event_type).json_data(payload) {
        Ok(event) => event,
        Err(_) => Event::default().event("watch_error").data("failed to encode watch event"),
    };
    tx.send(Ok(event)).await.is_err()
}

async fn topic_adapter_lag_snapshot(
    adapter: &fabrik_store::TopicAdapterRecord,
    offsets: &[fabrik_store::TopicAdapterOffsetRecord],
) -> Value {
    let latest_offsets = match tokio::time::timeout(
        Duration::from_millis(500),
        load_json_topic_latest_offsets(
            &JsonTopicConfig::new(
                adapter.brokers.clone(),
                adapter.topic_name.clone(),
                adapter.topic_partitions,
            ),
            &format!("api-gateway-topic-adapter-lag-{}", adapter.adapter_id),
        ),
    )
    .await
    {
        Ok(Ok(offsets)) => offsets,
        Ok(Err(error)) => {
            return json!({
                "available": false,
                "error": error.to_string(),
                "total_lag_records": Value::Null,
                "partitions": [],
            });
        }
        Err(_) => {
            return json!({
                "available": false,
                "error": "lag lookup timed out".to_owned(),
                "total_lag_records": Value::Null,
                "partitions": [],
            });
        }
    };
    let committed_by_partition = offsets
        .iter()
        .map(|offset| (offset.partition_id, offset.log_offset))
        .collect::<HashMap<_, _>>();
    let partitions = latest_offsets
        .into_iter()
        .map(|offset| {
            let committed = committed_by_partition.get(&offset.partition_id).copied();
            let lag_records = match committed {
                Some(committed) => (offset.latest_offset - committed).max(0),
                None if offset.latest_offset >= 0 => offset.latest_offset.saturating_add(1),
                None => 0,
            };
            TopicAdapterLagPartition {
                partition_id: offset.partition_id,
                committed_offset: committed,
                latest_offset: offset.latest_offset,
                lag_records,
            }
        })
        .collect::<Vec<_>>();
    let total_lag_records = partitions.iter().map(|partition| partition.lag_records).sum::<i64>();
    json!({
        "available": true,
        "total_lag_records": total_lag_records,
        "partitions": partitions,
    })
}

async fn query_json(state: &AppState, path: &str) -> Result<Value> {
    state
        .client
        .get(format!("{}{}", state.query_base.trim_end_matches('/'), path))
        .send()
        .await
        .with_context(|| format!("failed to fetch {path}"))?
        .error_for_status()
        .with_context(|| format!("query request failed for {path}"))?
        .json::<Value>()
        .await
        .with_context(|| format!("failed to decode query payload for {path}"))
}

async fn list_tenants(State(state): State<AppState>) -> Result<Json<Value>, (StatusCode, String)> {
    let tenants = state.store.list_tenants().await.map_err(internal_error)?;
    Ok(Json(json!({ "tenants": tenants })))
}

async fn search(
    Query(query): Query<SearchQuery>,
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let tenant_id = query.tenant_id.trim().to_owned();
    let needle = query.q.trim().to_owned();
    let limit = query.limit.unwrap_or(20).clamp(1, 100);
    if tenant_id.is_empty() || needle.is_empty() {
        return Ok(Json(json!({
            "tenant_id": tenant_id,
            "items": []
        })));
    }

    let workflows = state
        .store
        .list_instances_page_with_filters(
            &tenant_id,
            &fabrik_store::WorkflowInstanceFilters {
                query: Some(needle.clone()),
                ..fabrik_store::WorkflowInstanceFilters::default()
            },
            limit as i64,
            0,
        )
        .await
        .map_err(internal_error)?;

    let definitions = state
        .store
        .list_workflow_definition_summaries(&tenant_id, Some(&needle), limit as i64, 0)
        .await
        .map_err(internal_error)?;
    let runs = state
        .store
        .list_runs_page_with_filters(
            &tenant_id,
            &WorkflowRunFilters { query: Some(needle.clone()), ..WorkflowRunFilters::default() },
            limit as i64,
            0,
        )
        .await
        .map_err(internal_error)?;

    let task_queue_refs =
        state.store.list_task_queue_refs_for_tenant(&tenant_id).await.map_err(internal_error)?;

    let mut items = Vec::new();
    for workflow in workflows {
        items.push(json!({
            "kind": "workflow_instance",
            "id": workflow.instance_id,
            "title": workflow.definition_id,
            "subtitle": format!("{} · {}", workflow.status.as_str(), workflow.run_id),
            "href": format!("/runs/{}", workflow.instance_id),
            "tenant_id": workflow.tenant_id
        }));
    }
    for run in runs {
        items.push(json!({
            "kind": "run",
            "id": format!("{}:{}", run.instance_id, run.run_id),
            "title": run.definition_id,
            "subtitle": format!("{} · {} · {}", run.status, run.instance_id, run.run_id),
            "href": format!("/runs/{}/{}", run.instance_id, run.run_id),
            "tenant_id": run.tenant_id
        }));
    }
    for queue_ref in task_queue_refs
        .into_iter()
        .filter(|queue_ref| {
            queue_ref.task_queue.contains(&needle)
                || queue_kind_label(&queue_ref.queue_kind).contains(&needle)
        })
        .take(limit)
    {
        items.push(json!({
            "kind": "task_queue",
            "id": format!("{}:{}", queue_kind_label(&queue_ref.queue_kind), queue_ref.task_queue),
            "title": queue_ref.task_queue,
            "subtitle": format!("{} queue", queue_kind_label(&queue_ref.queue_kind)),
            "href": format!("/task-queues?queue_kind={}&task_queue={}", queue_kind_label(&queue_ref.queue_kind), queue_ref.task_queue),
            "tenant_id": queue_ref.tenant_id
        }));
    }
    for definition in definitions {
        items.push(json!({
            "kind": "definition",
            "id": definition.workflow_id,
            "title": definition.workflow_id,
            "subtitle": format!("definition v{}", definition.latest_version),
            "href": format!("/workflows/{}", definition.workflow_id),
            "tenant_id": definition.tenant_id
        }));
    }
    items.truncate(limit);

    Ok(Json(json!({
        "tenant_id": tenant_id,
        "items": items
    })))
}

async fn get_workflow_routing(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowRoutingResponse>, (StatusCode, String)> {
    let instance = state
        .store
        .get_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("workflow instance {instance_id} not found for tenant {tenant_id}"),
            )
        })?;
    let run = state
        .store
        .get_run_record(&tenant_id, &instance_id, &instance.run_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("workflow run {} not found for tenant {tenant_id}", instance.run_id),
            )
        })?;
    let inspection = state
        .store
        .inspect_task_queue(
            &tenant_id,
            TaskQueueKind::Workflow,
            &run.workflow_task_queue,
            Utc::now(),
        )
        .await
        .map_err(internal_error)?;
    let sticky_build_compatible_with_queue =
        if let Some(build_id) = run.sticky_workflow_build_id.as_deref() {
            Some(
                state
                    .store
                    .is_build_compatible_with_queue(
                        &tenant_id,
                        TaskQueueKind::Workflow,
                        &run.workflow_task_queue,
                        build_id,
                    )
                    .await
                    .map_err(internal_error)?,
            )
        } else {
            None
        };
    let sticky_build_supports_pinned_artifact =
        if let Some(build_id) = run.sticky_workflow_build_id.as_deref() {
            Some(
                state
                    .store
                    .workflow_build_supports_artifact(
                        &tenant_id,
                        &run.workflow_task_queue,
                        build_id,
                        run.artifact_hash.as_deref(),
                    )
                    .await
                    .map_err(internal_error)?,
            )
        } else {
            None
        };
    let routing_status =
        match (sticky_build_compatible_with_queue, sticky_build_supports_pinned_artifact) {
            (Some(false), _) => "sticky_incompatible_fallback_required",
            (_, Some(false)) => "sticky_artifact_mismatch_fallback_required",
            (Some(true), Some(true)) | (Some(true), None) | (None, Some(true)) => "sticky_active",
            (None, None) => "queue_default_active",
        };

    Ok(Json(WorkflowRoutingResponse {
        tenant_id,
        instance_id,
        run_id: run.run_id,
        definition_id: run.definition_id,
        definition_version: run.definition_version,
        artifact_hash: run.artifact_hash.clone(),
        workflow_task_queue: run.workflow_task_queue,
        routing_status,
        default_compatibility_set_id: inspection.default_set_id,
        compatible_build_ids: inspection.compatible_build_ids,
        registered_build_ids: inspection
            .registered_builds
            .into_iter()
            .map(|record| record.build_id)
            .collect(),
        sticky_workflow_build_id: run.sticky_workflow_build_id,
        sticky_workflow_poller_id: run.sticky_workflow_poller_id,
        sticky_updated_at: run.sticky_updated_at,
        sticky_build_compatible_with_queue,
        sticky_build_supports_pinned_artifact,
    }))
}

async fn list_topic_adapters(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let adapters =
        state.store.list_topic_adapters_for_tenant(&tenant_id).await.map_err(internal_error)?;
    Ok(Json(serde_json::to_value(adapters).expect("topic adapters serialize")))
}

async fn get_topic_adapter(
    Path((tenant_id, adapter_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let adapter = state
        .store
        .get_topic_adapter(&tenant_id, &adapter_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("topic adapter {adapter_id} not found for tenant {tenant_id}"),
            )
        })?;
    let offsets = state
        .store
        .load_topic_adapter_offsets(&tenant_id, &adapter_id)
        .await
        .map_err(internal_error)?;
    let dead_letters = state
        .store
        .list_topic_adapter_dead_letters(&tenant_id, &adapter_id, 20, 0)
        .await
        .map_err(internal_error)?;
    let ownership = state
        .store
        .get_topic_adapter_ownership(&tenant_id, &adapter_id)
        .await
        .map_err(internal_error)?;
    let lag = topic_adapter_lag_snapshot(&adapter, &offsets).await;
    Ok(Json(json!({
        "adapter": adapter,
        "offsets": offsets,
        "recent_dead_letters": dead_letters,
        "ownership": ownership,
        "lag": lag,
        "watch_cursor": Utc::now().to_rfc3339(),
    })))
}

async fn upsert_topic_adapter(
    Path((tenant_id, adapter_id)): Path<(String, String)>,
    State(state): State<AppState>,
    Json(request): Json<TopicAdapterRequest>,
) -> Result<Json<Value>, (StatusCode, String)> {
    validate_topic_adapter_request(&request)?;
    let record = state
        .store
        .upsert_topic_adapter(&TopicAdapterUpsert {
            tenant_id,
            adapter_id,
            adapter_kind: request.adapter_kind,
            brokers: request.brokers,
            topic_name: request.topic_name,
            topic_partitions: request.topic_partitions,
            action: request.action,
            definition_id: request.definition_id,
            signal_type: request.signal_type,
            workflow_task_queue: request.workflow_task_queue,
            workflow_instance_id_json_pointer: request.workflow_instance_id_json_pointer,
            payload_json_pointer: request.payload_json_pointer,
            payload_template_json: request.payload_template_json,
            memo_json_pointer: request.memo_json_pointer,
            memo_template_json: request.memo_template_json,
            search_attributes_json_pointer: request.search_attributes_json_pointer,
            search_attributes_template_json: request.search_attributes_template_json,
            request_id_json_pointer: request.request_id_json_pointer,
            dedupe_key_json_pointer: request.dedupe_key_json_pointer,
            dead_letter_policy: request.dead_letter_policy,
            is_paused: request.is_paused,
        })
        .await
        .map_err(internal_error)?;
    Ok(Json(serde_json::to_value(record).expect("topic adapter serializes")))
}

async fn pause_topic_adapter(
    Path((tenant_id, adapter_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let record = state
        .store
        .set_topic_adapter_paused(&tenant_id, &adapter_id, true)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("topic adapter {adapter_id} not found for tenant {tenant_id}"),
            )
        })?;
    Ok(Json(serde_json::to_value(record).expect("topic adapter serializes")))
}

async fn delete_topic_adapter(
    Path((tenant_id, adapter_id)): Path<(String, String)>,
    Query(query): Query<TopicAdapterDeleteQuery>,
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let adapter = state
        .store
        .get_topic_adapter(&tenant_id, &adapter_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("topic adapter {adapter_id} not found for tenant {tenant_id}"),
            )
        })?;
    let ownership = state
        .store
        .get_topic_adapter_ownership(&tenant_id, &adapter_id)
        .await
        .map_err(internal_error)?;
    let offsets = state
        .store
        .load_topic_adapter_offsets(&tenant_id, &adapter_id)
        .await
        .map_err(internal_error)?;
    let lag = topic_adapter_lag_snapshot(&adapter, &offsets).await;
    let now = Utc::now();
    let force = query.force.unwrap_or(false);
    let ownership_active = ownership
        .as_ref()
        .is_some_and(|record| record.lease_expires_at > now);
    let lag_total = lag
        .get("total_lag_records")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    let lag_blocked = lag
        .get("available")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        && lag_total > 0;
    if !force && (ownership_active || lag_blocked) {
        let mut blockers = Vec::new();
        if ownership_active {
            if let Some(record) = &ownership {
                blockers.push(format!(
                    "active ownership by {} until {}",
                    record.owner_id,
                    record.lease_expires_at.to_rfc3339()
                ));
            }
        }
        if lag_blocked {
            blockers.push(format!("broker lag still present ({lag_total})"));
        }
        return Err((
            StatusCode::CONFLICT,
            format!(
                "topic adapter {adapter_id} cannot be deleted without force: {}",
                blockers.join(", ")
            ),
        ));
    }
    let deleted = state
        .store
        .delete_topic_adapter(&tenant_id, &adapter_id)
        .await
        .map_err(internal_error)?;
    Ok(Json(json!({
        "tenant_id": tenant_id,
        "adapter_id": adapter_id,
        "deleted": deleted,
        "forced": force,
    })))
}

async fn resume_topic_adapter(
    Path((tenant_id, adapter_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let record = state
        .store
        .set_topic_adapter_paused(&tenant_id, &adapter_id, false)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("topic adapter {adapter_id} not found for tenant {tenant_id}"),
            )
        })?;
    Ok(Json(serde_json::to_value(record).expect("topic adapter serializes")))
}

async fn list_topic_adapter_dead_letters(
    Path((tenant_id, adapter_id)): Path<(String, String)>,
    Query(query): Query<TopicAdapterDeadLetterQuery>,
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let offset = query.offset.unwrap_or(0).max(0);
    let items = state
        .store
        .list_topic_adapter_dead_letters(&tenant_id, &adapter_id, limit, offset)
        .await
        .map_err(internal_error)?;
    Ok(Json(json!({
        "tenant_id": tenant_id,
        "adapter_id": adapter_id,
        "items": items,
        "limit": limit,
        "offset": offset,
    })))
}

async fn preview_topic_adapter(
    Path((tenant_id, adapter_id)): Path<(String, String)>,
    State(state): State<AppState>,
    Json(request): Json<TopicAdapterPreviewRequest>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let adapter = state
        .store
        .get_topic_adapter(&tenant_id, &adapter_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("topic adapter {adapter_id} not found for tenant {tenant_id}"),
            )
        })?;
    let response = match resolve_topic_adapter_dispatch_detailed(
        &adapter, &request.payload, request.partition_id, request.log_offset,
    ) {
        Ok(preview) => json!({
            "ok": true,
            "dispatch": preview.dispatch,
            "diagnostics": preview.diagnostics,
            "error": Value::Null,
        }),
        Err(failure) => json!({
            "ok": false,
            "dispatch": Value::Null,
            "diagnostics": failure.diagnostics,
            "error": failure.error,
        }),
    };
    Ok(Json(response))
}

async fn preview_topic_adapter_draft(
    Path(tenant_id): Path<String>,
    Json(request): Json<TopicAdapterDraftPreviewRequest>,
) -> Result<Json<Value>, (StatusCode, String)> {
    if let Err(error) = validate_topic_adapter_config(
        request.adapter.action.clone(),
        request.adapter.definition_id.as_deref(),
        request.adapter.signal_type.as_deref(),
        request.adapter.workflow_task_queue.as_deref(),
        request.adapter.workflow_instance_id_json_pointer.as_deref(),
        request.adapter.payload_json_pointer.as_deref(),
        request.adapter.payload_template_json.as_ref(),
        request.adapter.memo_json_pointer.as_deref(),
        request.adapter.memo_template_json.as_ref(),
        request.adapter.search_attributes_json_pointer.as_deref(),
        request.adapter.search_attributes_template_json.as_ref(),
        request.adapter.request_id_json_pointer.as_deref(),
        request.adapter.dedupe_key_json_pointer.as_deref(),
    ) {
        return Ok(Json(json!({
            "ok": false,
            "dispatch": Value::Null,
            "diagnostics": [],
            "error": error,
        })));
    }
    let adapter_id = request
        .adapter_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or("draft");
    let adapter = topic_adapter_request_to_record(&tenant_id, adapter_id, request.adapter);
    let response = match resolve_topic_adapter_dispatch_detailed(
        &adapter,
        &request.payload,
        request.partition_id,
        request.log_offset,
    ) {
        Ok(preview) => json!({
            "ok": true,
            "dispatch": preview.dispatch,
            "diagnostics": preview.diagnostics,
            "error": Value::Null,
        }),
        Err(failure) => json!({
            "ok": false,
            "dispatch": Value::Null,
            "diagnostics": failure.diagnostics,
            "error": failure.error,
        }),
    };
    Ok(Json(response))
}

async fn watch_topic_adapter(
    Path((tenant_id, adapter_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Sse<ReceiverStream<Result<Event, Infallible>>> {
    let (tx, rx) = mpsc::channel(16);
    spawn(async move {
        let mut last_detail = None::<String>;
        let mut last_lag = None::<String>;
        let mut last_dead_letter = None::<String>;
        let mut last_ownership = None::<fabrik_store::TopicAdapterOwnershipRecord>;
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            let adapter = state.store.get_topic_adapter(&tenant_id, &adapter_id).await;
            if let Ok(Some(adapter)) = adapter {
                let offsets = match state.store.load_topic_adapter_offsets(&tenant_id, &adapter_id).await {
                    Ok(offsets) => offsets,
                    Err(error) => {
                        let _ = send_watch_event(
                            &tx,
                            "adapter_runtime_error",
                            "eventual",
                            "projection",
                            None,
                            Utc::now().to_rfc3339(),
                            json!({ "error": error.to_string() }),
                        )
                        .await;
                        interval.tick().await;
                        continue;
                    }
                };
                let dead_letters = match state
                    .store
                    .list_topic_adapter_dead_letters(&tenant_id, &adapter_id, 1, 0)
                    .await
                {
                    Ok(dead_letters) => dead_letters,
                    Err(error) => {
                        let _ = send_watch_event(
                            &tx,
                            "adapter_runtime_error",
                            "eventual",
                            "projection",
                            None,
                            Utc::now().to_rfc3339(),
                            json!({ "error": error.to_string() }),
                        )
                        .await;
                        interval.tick().await;
                        continue;
                    }
                };
                let ownership = match state.store.get_topic_adapter_ownership(&tenant_id, &adapter_id).await {
                    Ok(ownership) => ownership,
                    Err(error) => {
                        let _ = send_watch_event(
                            &tx,
                            "adapter_runtime_error",
                            "eventual",
                            "projection",
                            None,
                            Utc::now().to_rfc3339(),
                            json!({ "error": error.to_string() }),
                        )
                        .await;
                        interval.tick().await;
                        continue;
                    }
                };
                let lag = topic_adapter_lag_snapshot(&adapter, &offsets).await;

                let cursor = Utc::now().to_rfc3339();
                let detail_body = json!({
                    "adapter": adapter,
                    "offsets": offsets,
                    "recent_dead_letters": dead_letters,
                    "ownership": ownership,
                    "lag": lag,
                    "watch_cursor": cursor,
                });
                if let Ok(encoded) = serde_json::to_string(&detail_body) {
                    if last_detail.as_ref() != Some(&encoded) {
                        if send_watch_event(
                            &tx,
                            "adapter_state_changed",
                            "eventual",
                            "projection",
                            None,
                            cursor.clone(),
                            detail_body.clone(),
                        )
                        .await
                        {
                            break;
                        }
                        last_detail = Some(encoded);
                    }
                }

                if ownership != last_ownership {
                    let previous_owner_id =
                        last_ownership.as_ref().map(|record| record.owner_id.clone());
                    let previous_owner_epoch =
                        last_ownership.as_ref().map(|record| record.owner_epoch);
                    let ownership_body = json!({
                        "adapter_id": adapter_id,
                        "ownership": ownership,
                        "previous_owner_id": previous_owner_id,
                        "previous_owner_epoch": previous_owner_epoch,
                        "change_kind": match (&last_ownership, &ownership) {
                            (None, Some(_)) => "claimed",
                            (Some(_), None) => "released",
                            (Some(previous), Some(current))
                                if previous.owner_id != current.owner_id
                                    || previous.owner_epoch != current.owner_epoch => "handoff",
                            _ => "lease_updated",
                        }
                    });
                    if send_watch_event(
                        &tx,
                        "adapter_ownership_changed",
                        "eventual",
                        "projection",
                        None,
                        cursor.clone(),
                        ownership_body,
                    )
                    .await
                    {
                        break;
                    }
                    last_ownership = ownership.clone();
                }

                let lag_body = json!({
                    "adapter_id": adapter_id,
                    "lag": lag,
                });
                if let Ok(encoded) = serde_json::to_string(&lag_body) {
                    if last_lag.as_ref() != Some(&encoded) {
                        if send_watch_event(
                            &tx,
                            "adapter_lag",
                            "eventual",
                            "broker",
                            None,
                            cursor.clone(),
                            lag_body,
                        )
                        .await
                        {
                            break;
                        }
                        last_lag = Some(encoded);
                    }
                }

                if let Some(dead_letter) = detail_body
                    .get("recent_dead_letters")
                    .and_then(Value::as_array)
                    .and_then(|items| items.first())
                {
                    if let Ok(encoded) = serde_json::to_string(dead_letter) {
                        if last_dead_letter.as_ref() != Some(&encoded) {
                            if send_watch_event(
                                &tx,
                                "adapter_dead_letter",
                                "eventual",
                                "projection",
                                None,
                                cursor,
                                dead_letter.clone(),
                            )
                            .await
                            {
                                break;
                            }
                            last_dead_letter = Some(encoded);
                        }
                    }
                }
            }
            interval.tick().await;
        }
    });
    Sse::new(ReceiverStream::new(rx))
}

async fn proxy_request(
    client: Client,
    base_url: String,
    uri: OriginalUri,
    body: Option<Bytes>,
) -> Result<(StatusCode, Bytes), (StatusCode, String)> {
    let path_and_query =
        uri.0.path_and_query().map(|value| value.as_str()).unwrap_or_else(|| uri.0.path());
    let url = format!("{base_url}{path_and_query}");
    let request = if let Some(body) = body {
        client.post(&url).header("content-type", "application/json").body(body)
    } else {
        client.get(&url)
    };
    let response = request
        .send()
        .await
        .with_context(|| format!("failed to proxy request to {url}"))
        .map_err(internal_error)?;
    let status = StatusCode::from_u16(response.status().as_u16())
        .map_err(|error| internal_error(anyhow::anyhow!(error.to_string())))?;
    let bytes =
        response.bytes().await.map_err(|error| internal_error(anyhow::Error::from(error)))?;
    Ok((status, bytes))
}

async fn proxy_get_path(
    client: Client,
    base_url: String,
    path: &str,
) -> Result<(StatusCode, Bytes), (StatusCode, String)> {
    let url = format!("{base_url}{path}");
    let response = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("failed to proxy request to {url}"))
        .map_err(internal_error)?;
    let status = StatusCode::from_u16(response.status().as_u16())
        .map_err(|error| internal_error(anyhow::anyhow!(error.to_string())))?;
    let bytes =
        response.bytes().await.map_err(|error| internal_error(anyhow::Error::from(error)))?;
    Ok((status, bytes))
}

fn parse_queue_kind(raw: &str) -> Result<TaskQueueKind, (StatusCode, String)> {
    match raw {
        "workflow" => Ok(TaskQueueKind::Workflow),
        "activity" => Ok(TaskQueueKind::Activity),
        _ => Err((StatusCode::BAD_REQUEST, format!("unknown queue kind {raw}"))),
    }
}

fn default_topic_adapter_dead_letter_policy() -> TopicAdapterDeadLetterPolicy {
    TopicAdapterDeadLetterPolicy::Store
}

fn validate_topic_adapter_request(
    request: &TopicAdapterRequest,
) -> Result<(), (StatusCode, String)> {
    if request.topic_partitions <= 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            "topic_partitions must be greater than zero".to_owned(),
        ));
    }
    validate_topic_adapter_config(
        request.action.clone(),
        request.definition_id.as_deref(),
        request.signal_type.as_deref(),
        request.workflow_task_queue.as_deref(),
        request.workflow_instance_id_json_pointer.as_deref(),
        request.payload_json_pointer.as_deref(),
        request.payload_template_json.as_ref(),
        request.memo_json_pointer.as_deref(),
        request.memo_template_json.as_ref(),
        request.search_attributes_json_pointer.as_deref(),
        request.search_attributes_template_json.as_ref(),
        request.request_id_json_pointer.as_deref(),
        request.dedupe_key_json_pointer.as_deref(),
    )
    .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    Ok(())
}

fn topic_adapter_request_to_record(
    tenant_id: &str,
    adapter_id: &str,
    request: TopicAdapterRequest,
) -> TopicAdapterRecord {
    let now = Utc::now();
    TopicAdapterRecord {
        tenant_id: tenant_id.to_owned(),
        adapter_id: adapter_id.to_owned(),
        adapter_kind: request.adapter_kind,
        brokers: request.brokers,
        topic_name: request.topic_name,
        topic_partitions: request.topic_partitions,
        action: request.action,
        definition_id: request.definition_id,
        signal_type: request.signal_type,
        workflow_task_queue: request.workflow_task_queue,
        workflow_instance_id_json_pointer: request.workflow_instance_id_json_pointer,
        payload_json_pointer: request.payload_json_pointer,
        payload_template_json: request.payload_template_json,
        memo_json_pointer: request.memo_json_pointer,
        memo_template_json: request.memo_template_json,
        search_attributes_json_pointer: request.search_attributes_json_pointer,
        search_attributes_template_json: request.search_attributes_template_json,
        request_id_json_pointer: request.request_id_json_pointer,
        dedupe_key_json_pointer: request.dedupe_key_json_pointer,
        dead_letter_policy: request.dead_letter_policy,
        is_paused: request.is_paused,
        processed_count: 0,
        failed_count: 0,
        ownership_handoff_count: 0,
        last_processed_at: None,
        last_handoff_at: None,
        last_takeover_latency_ms: None,
        last_error: None,
        last_error_at: None,
        created_at: now,
        updated_at: now,
    }
}

fn queue_kind_label(queue_kind: &TaskQueueKind) -> &'static str {
    match queue_kind {
        TaskQueueKind::Workflow => "workflow",
        TaskQueueKind::Activity => "activity",
    }
}

async fn task_queue_admission_snapshot(
    state: &AppState,
    tenant_id: &str,
    task_queue: &str,
    persisted_backend: Option<&str>,
) -> Result<Value> {
    let configured_backend = state.admission.task_queue_backends.get(task_queue).cloned();
    let effective_backend = persisted_backend
        .map(str::to_owned)
        .or(configured_backend.clone())
        .unwrap_or_else(|| state.admission.default_backend.clone());
    let tenant_active = state
        .store
        .count_started_bulk_chunks_for_backend_scope("stream-v2", Some(tenant_id), None, None)
        .await?;
    let task_queue_active = state
        .store
        .count_started_bulk_chunks_for_backend_scope(
            "stream-v2",
            Some(tenant_id),
            Some(task_queue),
            None,
        )
        .await?;
    let routing_counts = state
        .store
        .list_nonterminal_bulk_batch_routing_counts_for_task_queue(tenant_id, task_queue)
        .await?;
    let capacity_state = if effective_backend != "stream-v2" {
        "not_applicable"
    } else if state.admission.max_active_chunks_per_tenant > 0
        && tenant_active >= state.admission.max_active_chunks_per_tenant as u64
    {
        "tenant_saturated"
    } else if state.admission.max_active_chunks_per_task_queue > 0
        && task_queue_active >= state.admission.max_active_chunks_per_task_queue as u64
    {
        "task_queue_saturated"
    } else {
        "available"
    };
    Ok(json!({
        "configured_default_backend": state.admission.default_backend,
        "configured_task_queue_backend": configured_backend,
        "persisted_task_queue_backend": persisted_backend,
        "effective_preferred_backend": effective_backend,
        "stream_v2_active_chunks": {
            "tenant": tenant_active,
            "task_queue": task_queue_active,
        },
        "stream_v2_capacity": {
            "tenant_limit": state.admission.max_active_chunks_per_tenant,
            "task_queue_limit": state.admission.max_active_chunks_per_task_queue,
            "tenant_utilization": ratio_u64(tenant_active, state.admission.max_active_chunks_per_tenant),
            "task_queue_utilization": ratio_u64(task_queue_active, state.admission.max_active_chunks_per_task_queue),
            "state": capacity_state,
        },
        "nonterminal_backend_counts": counts_by_backend(&routing_counts),
        "nonterminal_routing_reason_counts": counts_by_reason(&routing_counts),
        "nonterminal_admission_policy_versions": counts_by_policy_version(&routing_counts),
    }))
}

fn ratio_u64(value: u64, limit: usize) -> Option<f64> {
    (limit > 0).then_some(value as f64 / limit as f64)
}

fn counts_by_backend(records: &[BulkBatchRoutingCount]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for record in records {
        *counts.entry(record.selected_backend.clone()).or_default() += record.batch_count;
    }
    counts
}

fn counts_by_reason(records: &[BulkBatchRoutingCount]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for record in records {
        *counts.entry(record.routing_reason.clone()).or_default() += record.batch_count;
    }
    counts
}

fn counts_by_policy_version(records: &[BulkBatchRoutingCount]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for record in records {
        *counts.entry(record.admission_policy_version.clone()).or_default() += record.batch_count;
    }
    counts
}

fn internal_error(error: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
        response::IntoResponse,
        routing::{get, post},
    };
    use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
    use fabrik_workflow::{StateNode, WorkflowDefinition, WorkflowInstanceState, WorkflowStatus};
    use serde_json::json;
    use std::{
        collections::BTreeMap,
        process::Command,
        time::{Duration as StdDuration, Instant},
    };
    use tokio::time::sleep;
    use tower::ServiceExt;

    struct TestPostgres {
        container_name: String,
        database_url: String,
    }

    fn test_admission_config() -> AdmissionDebugConfig {
        AdmissionDebugConfig {
            default_backend: "pg-v1".to_owned(),
            task_queue_backends: BTreeMap::new(),
            max_active_chunks_per_tenant: 4_096,
            max_active_chunks_per_task_queue: 2_048,
        }
    }

    impl TestPostgres {
        fn start() -> Result<Option<Self>> {
            if !docker_available() {
                eprintln!("skipping api-gateway integration tests because docker is unavailable");
                return Ok(None);
            }

            let container_name = format!("fabrik-gateway-test-pg-{}", uuid::Uuid::now_v7());
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

    struct TestHttpServer {
        base_url: String,
        shutdown: Option<tokio::sync::oneshot::Sender<()>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl TestHttpServer {
        async fn start(app: Router) -> Result<Self> {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .context("failed to bind test http listener")?;
            let addr = listener.local_addr().context("failed to read listener address")?;
            let base_url = format!("http://{}", addr);
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                let _ = axum::serve(listener, app)
                    .with_graceful_shutdown(async {
                        let _ = shutdown_rx.await;
                    })
                    .await;
            });
            Ok(Self { base_url, shutdown: Some(shutdown_tx), task })
        }

        async fn stop(mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
            let _ = self.task.await;
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

    #[tokio::test]
    async fn gateway_proxies_workflow_front_door_and_matching_debug() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;

        let ingest = TestHttpServer::start(
            Router::new()
                .route(
                    "/workflows/demo/trigger",
                    post(|| async {
                        (StatusCode::ACCEPTED, Json(json!({"service": "ingest"}))).into_response()
                    }),
                )
                .route(
                    "/tenants/tenant-a/workflow-artifacts/validate",
                    post(|| async {
                        (
                            StatusCode::OK,
                            Json(json!({
                                "service": "ingest",
                                "compatible": false,
                                "status": "incompatible"
                            })),
                        )
                            .into_response()
                    }),
                ),
        )
        .await?;
        let query = TestHttpServer::start(Router::new().route(
            "/tenants/tenant-a/workflows/instance-1",
            get(|| async { Json(json!({"service": "query"})).into_response() }),
        ))
        .await?;
        let matching = TestHttpServer::start(Router::new().route(
            "/debug/activity-results",
            get(|| async {
                Json(json!({"applied_batches": 2, "applied_results": 8})).into_response()
            }),
        ))
        .await?;

        let app = build_app(
            AppState {
                client: Client::new(),
                ingest_base: ingest.base_url.clone(),
                query_base: query.base_url.clone(),
                matching_base: matching.base_url.clone(),
                store,
                admission: test_admission_config(),
            },
            "api-gateway-test".to_owned(),
        );

        let trigger = app
            .clone()
            .oneshot(
                Request::post("/workflows/demo/trigger")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"tenant_id":"tenant-a"}"#))
                    .expect("trigger request"),
            )
            .await?;
        assert_eq!(trigger.status(), StatusCode::ACCEPTED);
        let trigger_body = to_bytes(trigger.into_body(), usize::MAX).await?;
        assert_eq!(serde_json::from_slice::<Value>(&trigger_body)?["service"], "ingest");

        let query_response = app
            .clone()
            .oneshot(
                Request::get("/tenants/tenant-a/workflows/instance-1")
                    .body(Body::empty())
                    .expect("query request"),
            )
            .await?;
        assert_eq!(query_response.status(), StatusCode::OK);
        let query_body = to_bytes(query_response.into_body(), usize::MAX).await?;
        assert_eq!(serde_json::from_slice::<Value>(&query_body)?["service"], "query");

        let validate_response = app
            .clone()
            .oneshot(
                Request::post("/tenants/tenant-a/workflow-artifacts/validate?validation_run_limit=3")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"definition_id":"demo","definition_version":2,"compiler_version":"test","source_language":"ts","entrypoint":{"module":"workflow.ts","export":"workflow"},"workflow":{"initial_state":"done","states":{"done":{"type":"succeed"}}},"artifact_hash":"artifact-2"}"#))
                    .expect("validate request"),
            )
            .await?;
        assert_eq!(validate_response.status(), StatusCode::OK);
        let validate_body = to_bytes(validate_response.into_body(), usize::MAX).await?;
        let validate_json = serde_json::from_slice::<Value>(&validate_body)?;
        assert_eq!(validate_json["service"], "ingest");
        assert_eq!(validate_json["status"], "incompatible");

        let matching_debug_response = app
            .oneshot(
                Request::get("/admin/debug/matching/activity-results")
                    .body(Body::empty())
                    .expect("matching debug request"),
            )
            .await?;
        assert_eq!(matching_debug_response.status(), StatusCode::OK);
        let matching_debug_body = to_bytes(matching_debug_response.into_body(), usize::MAX).await?;
        assert_eq!(serde_json::from_slice::<Value>(&matching_debug_body)?["applied_results"], 8);

        ingest.stop().await;
        query.stop().await;
        matching.stop().await;
        Ok(())
    }

    #[tokio::test]
    async fn gateway_admin_endpoints_manage_and_inspect_task_queues() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let app = build_app(
            AppState {
                client: Client::new(),
                ingest_base: "http://127.0.0.1:1".to_owned(),
                query_base: "http://127.0.0.1:1".to_owned(),
                matching_base: "http://127.0.0.1:1".to_owned(),
                store: store.clone(),
                admission: test_admission_config(),
            },
            "api-gateway-test".to_owned(),
        );

        let register_response = app
            .clone()
            .oneshot(
                Request::post("/admin/tenants/tenant-a/task-queues/workflow/payments/builds")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"build_id":"build-a","artifact_hashes":["artifact-a"],"promote_default":true}"#,
                    ))
                    .expect("register request"),
            )
            .await?;
        assert_eq!(register_response.status(), StatusCode::OK);

        let upsert_response = app
            .clone()
            .oneshot(
                Request::put(
                    "/admin/tenants/tenant-a/task-queues/workflow/payments/compatibility-sets/canary",
                )
                .header("content-type", "application/json")
                .body(Body::from(r#"{"build_ids":["build-a"],"is_default":false}"#))
                .expect("compatibility request"),
            )
            .await?;
        assert_eq!(upsert_response.status(), StatusCode::OK);

        let promote_response = app
            .clone()
            .oneshot(
                Request::post("/admin/tenants/tenant-a/task-queues/workflow/payments/default-set")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"set_id":"canary"}"#))
                    .expect("promote request"),
            )
            .await?;
        assert_eq!(promote_response.status(), StatusCode::OK);

        let throughput_policy_response = app
            .clone()
            .oneshot(
                Request::put(
                    "/admin/tenants/tenant-a/task-queues/workflow/payments/throughput-policy",
                )
                .header("content-type", "application/json")
                .body(Body::from(r#"{"backend":"stream-v2"}"#))
                .expect("throughput policy request"),
            )
            .await?;
        assert_eq!(throughput_policy_response.status(), StatusCode::OK);

        store
            .upsert_queue_poller(
                "tenant-a",
                TaskQueueKind::Workflow,
                "payments",
                "poller-a",
                "build-a",
                Some(3),
                None,
                chrono::Duration::seconds(30),
            )
            .await?;
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
        let mut event = EventEnvelope::new(
            WorkflowEvent::WorkflowTriggered { input: json!({"ok": true}) }.event_type(),
            WorkflowIdentity::new(
                "tenant-a",
                "demo",
                1,
                "artifact-a",
                "instance-1",
                "run-1",
                "test",
            ),
            WorkflowEvent::WorkflowTriggered { input: json!({"ok": true}) },
        );
        event.metadata.insert("workflow_task_queue".to_owned(), "payments".to_owned());
        let task = store
            .enqueue_workflow_task(3, "payments", Some("build-a"), &event)
            .await?
            .context("workflow task should be enqueued")?;
        let leased = store
            .lease_next_workflow_task(3, "poller-a", "build-a", chrono::Duration::seconds(30))
            .await?
            .context("workflow task should be leased")?;
        assert_eq!(leased.task_id, task.task_id);

        let inspection_response = app
            .oneshot(
                Request::get("/admin/tenants/tenant-a/task-queues/workflow/payments")
                    .body(Body::empty())
                    .expect("inspection request"),
            )
            .await?;
        assert_eq!(inspection_response.status(), StatusCode::OK);
        let inspection_body = to_bytes(inspection_response.into_body(), usize::MAX).await?;
        let inspection: Value = serde_json::from_slice(&inspection_body)?;
        assert_eq!(inspection["task_queue"], "payments");
        assert_eq!(inspection["default_set_id"], "canary");
        assert_eq!(inspection["registered_builds"][0]["build_id"], "build-a");
        assert_eq!(inspection["pollers"][0]["poller_id"], "poller-a");
        assert_eq!(inspection["sticky_effectiveness"]["sticky_dispatch_count"], 1);
        assert_eq!(inspection["sticky_effectiveness"]["sticky_hit_count"], 1);
        assert_eq!(inspection["sticky_effectiveness"]["sticky_fallback_count"], 0);
        assert_eq!(inspection["throughput_policy"]["backend"], "stream-v2");
        assert_eq!(inspection["admission"]["effective_preferred_backend"], "stream-v2");
        assert_eq!(inspection["admission"]["stream_v2_capacity"]["state"], "available");
        assert_eq!(inspection["admission"]["configured_default_backend"], "pg-v1");

        Ok(())
    }

    #[tokio::test]
    async fn gateway_admin_endpoints_manage_topic_adapters() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let app = build_app(
            AppState {
                client: Client::new(),
                ingest_base: "http://127.0.0.1:1".to_owned(),
                query_base: "http://127.0.0.1:1".to_owned(),
                matching_base: "http://127.0.0.1:1".to_owned(),
                store: store.clone(),
                admission: test_admission_config(),
            },
            "api-gateway-test".to_owned(),
        );

        let upsert_response = app
            .clone()
            .oneshot(
                Request::put("/admin/tenants/tenant-a/topic-adapters/orders")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"adapter_kind":"redpanda","brokers":"127.0.0.1:9092","topic_name":"orders.events","topic_partitions":3,"action":"start_workflow","definition_id":"order-workflow","workflow_task_queue":"orders","payload_template_json":{"order":{"$from":"/payload"},"request":{"$from":"/request_id"}},"request_id_json_pointer":"/request_id"}"#,
                    ))
                    .expect("topic adapter upsert request"),
            )
            .await?;
        assert_eq!(upsert_response.status(), StatusCode::OK);

        let draft_preview_response = app
            .clone()
            .oneshot(
                Request::post("/admin/tenants/tenant-a/topic-adapters/preview-draft")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"adapter_id":"draft-orders","adapter":{"adapter_kind":"redpanda","brokers":"127.0.0.1:9092","topic_name":"orders.preview","topic_partitions":1,"action":"start_workflow","definition_id":"order-workflow","workflow_task_queue":"orders","payload_template_json":{"order":{"$from":"/payload"},"request":{"$from":"/request_id"}},"request_id_json_pointer":"/request_id"},"payload":{"payload":{"amount":99},"request_id":"draft-99"},"partition_id":0,"log_offset":4}"#,
                    ))
                    .expect("topic adapter draft preview request"),
            )
            .await?;
        assert_eq!(draft_preview_response.status(), StatusCode::OK);
        let draft_preview_body = to_bytes(draft_preview_response.into_body(), usize::MAX).await?;
        let draft_preview_json: Value = serde_json::from_slice(&draft_preview_body)?;
        assert_eq!(draft_preview_json["ok"], true);
        assert_eq!(draft_preview_json["dispatch"]["action"], "start_workflow");
        assert_eq!(draft_preview_json["dispatch"]["input"]["order"]["amount"], 99);

        store.record_topic_adapter_success("tenant-a", "orders", 0, 11, Utc::now()).await?;
        store
            .record_topic_adapter_dead_letter(
                "tenant-a",
                "orders",
                1,
                7,
                Some("order-7"),
                &json!({"bad": true}),
                "400: missing required field input",
                Utc::now(),
            )
            .await?;

        let list_response = app
            .clone()
            .oneshot(
                Request::get("/admin/tenants/tenant-a/topic-adapters")
                    .body(Body::empty())
                    .expect("topic adapter list request"),
            )
            .await?;
        assert_eq!(list_response.status(), StatusCode::OK);
        let list_body = to_bytes(list_response.into_body(), usize::MAX).await?;
        let list_json: Value = serde_json::from_slice(&list_body)?;
        assert_eq!(list_json[0]["adapter_id"], "orders");
        assert_eq!(list_json[0]["processed_count"], 1);
        assert_eq!(list_json[0]["failed_count"], 1);

        let get_response = app
            .clone()
            .oneshot(
                Request::get("/admin/tenants/tenant-a/topic-adapters/orders")
                    .body(Body::empty())
                    .expect("topic adapter get request"),
            )
            .await?;
        assert_eq!(get_response.status(), StatusCode::OK);
        let get_body = to_bytes(get_response.into_body(), usize::MAX).await?;
        let get_json: Value = serde_json::from_slice(&get_body)?;
        assert_eq!(get_json["adapter"]["topic_name"], "orders.events");
        assert_eq!(get_json["adapter"]["payload_template_json"]["order"]["$from"], "/payload");
        assert_eq!(get_json["offsets"][0]["partition_id"], 0);
        assert_eq!(get_json["recent_dead_letters"][0]["partition_id"], 1);

        let preview_response = app
            .clone()
            .oneshot(
                Request::post("/admin/tenants/tenant-a/topic-adapters/orders/preview")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"payload":{"payload":{"amount":125},"request_id":"req-42"},"partition_id":0,"log_offset":7}"#,
                    ))
                    .expect("topic adapter preview request"),
            )
            .await?;
        assert_eq!(preview_response.status(), StatusCode::OK);
        let preview_body = to_bytes(preview_response.into_body(), usize::MAX).await?;
        let preview_json: Value = serde_json::from_slice(&preview_body)?;
        assert_eq!(preview_json["ok"], true);
        assert_eq!(preview_json["dispatch"]["action"], "start_workflow");
        assert_eq!(preview_json["dispatch"]["input"]["order"]["amount"], 125);
        assert_eq!(preview_json["dispatch"]["request_id"], "req-42");
        assert!(preview_json["diagnostics"].as_array().is_some_and(|diagnostics| diagnostics
            .iter()
            .any(|entry| entry["field"] == "input" && entry["mode"] == "template")));

        let failing_preview_response = app
            .clone()
            .oneshot(
                Request::post("/admin/tenants/tenant-a/topic-adapters/orders/preview")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"payload":{"request_id":"req-43"},"partition_id":0,"log_offset":8}"#,
                    ))
                    .expect("topic adapter failing preview request"),
            )
            .await?;
        assert_eq!(failing_preview_response.status(), StatusCode::OK);
        let failing_preview_body =
            to_bytes(failing_preview_response.into_body(), usize::MAX).await?;
        let failing_preview_json: Value = serde_json::from_slice(&failing_preview_body)?;
        assert_eq!(failing_preview_json["ok"], false);
        assert_eq!(
            failing_preview_json["error"]["field"],
            "payload_template_json.order"
        );
        assert!(failing_preview_json["diagnostics"]
            .as_array()
            .is_some_and(|diagnostics| diagnostics
                .iter()
                .any(|entry| entry["field"] == "input" && entry["mode"] == "template")));

        let invalid_upsert_response = app
            .clone()
            .oneshot(
                Request::put("/admin/tenants/tenant-a/topic-adapters/orders-invalid")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"adapter_kind":"redpanda","brokers":"127.0.0.1:9092","topic_name":"orders.invalid","topic_partitions":1,"action":"signal_workflow","definition_id":"order-workflow","signal_type":"approved","workflow_instance_id_json_pointer":"/instance_id","payload_template_json":{"payload":{"$from":"payload"}}}"#,
                    ))
                    .expect("invalid topic adapter upsert request"),
            )
            .await?;
        assert_eq!(invalid_upsert_response.status(), StatusCode::BAD_REQUEST);
        let invalid_upsert_body =
            to_bytes(invalid_upsert_response.into_body(), usize::MAX).await?;
        let invalid_upsert_text = String::from_utf8(invalid_upsert_body.to_vec())?;
        assert!(invalid_upsert_text.contains("mapping"));

        let pause_response = app
            .clone()
            .oneshot(
                Request::post("/admin/tenants/tenant-a/topic-adapters/orders/pause")
                    .body(Body::empty())
                    .expect("topic adapter pause request"),
            )
            .await?;
        assert_eq!(pause_response.status(), StatusCode::OK);

        let dead_letters_response = app
            .clone()
            .oneshot(
                Request::get("/admin/tenants/tenant-a/topic-adapters/orders/dead-letters")
                    .body(Body::empty())
                    .expect("topic adapter dead letters request"),
            )
            .await?;
        assert_eq!(dead_letters_response.status(), StatusCode::OK);
        let dead_letters_body = to_bytes(dead_letters_response.into_body(), usize::MAX).await?;
        let dead_letters_json: Value = serde_json::from_slice(&dead_letters_body)?;
        assert_eq!(dead_letters_json["items"][0]["record_key"], "order-7");

        store
            .claim_topic_adapter_ownership("tenant-a", "orders", "ingest-a", StdDuration::from_secs(30))
            .await?;

        let blocked_delete_response = app
            .clone()
            .oneshot(
                Request::delete("/admin/tenants/tenant-a/topic-adapters/orders")
                    .body(Body::empty())
                    .expect("topic adapter guarded delete request"),
            )
            .await?;
        assert_eq!(blocked_delete_response.status(), StatusCode::CONFLICT);
        let blocked_delete_body = to_bytes(blocked_delete_response.into_body(), usize::MAX).await?;
        let blocked_delete_text = String::from_utf8(blocked_delete_body.to_vec())?;
        assert!(blocked_delete_text.contains("cannot be deleted without force"));

        let force_delete_response = app
            .clone()
            .oneshot(
                Request::delete("/admin/tenants/tenant-a/topic-adapters/orders?force=true")
                    .body(Body::empty())
                    .expect("topic adapter force delete request"),
            )
            .await?;
        assert_eq!(force_delete_response.status(), StatusCode::OK);
        let force_delete_body = to_bytes(force_delete_response.into_body(), usize::MAX).await?;
        let force_delete_json: Value = serde_json::from_slice(&force_delete_body)?;
        assert_eq!(force_delete_json["deleted"], true);
        assert_eq!(force_delete_json["forced"], true);
        assert!(store.get_topic_adapter("tenant-a", "orders").await?.is_none());
        assert!(store.get_topic_adapter_ownership("tenant-a", "orders").await?.is_none());
        assert!(store.load_topic_adapter_offsets("tenant-a", "orders").await?.is_empty());
        assert!(store
            .list_topic_adapter_dead_letters("tenant-a", "orders", 10, 0)
            .await?
            .is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn workflow_routing_reports_pinned_artifact_and_sticky_compatibility() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let app = build_app(
            AppState {
                client: Client::new(),
                ingest_base: "http://127.0.0.1:1".to_owned(),
                query_base: "http://127.0.0.1:1".to_owned(),
                matching_base: "http://127.0.0.1:1".to_owned(),
                store: store.clone(),
                admission: test_admission_config(),
            },
            "api-gateway-test".to_owned(),
        );

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
                &["artifact-b".to_owned()],
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
                None,
                None,
                uuid::Uuid::now_v7(),
                chrono::Utc::now(),
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
                memo: None,
                search_attributes: None,
                sticky_workflow_build_id: Some("build-a".to_owned()),
                sticky_workflow_poller_id: Some("poller-a".to_owned()),
                current_state: Some("charge-card".to_owned()),
                context: None,
                artifact_execution: None,
                status: WorkflowStatus::Running,
                input: Some(json!({"orderId": 42})),
                persisted_input_handle: None,
                output: None,
                event_count: 4,
                last_event_id: uuid::Uuid::now_v7(),
                last_event_type: "ActivityScheduled".to_owned(),
                updated_at: chrono::Utc::now(),
            })
            .await?;
        store
            .update_run_workflow_sticky(
                "tenant-a",
                "instance-1",
                "run-1",
                "payments",
                "build-a",
                "poller-a",
                chrono::Utc::now(),
            )
            .await?;

        let response = app
            .oneshot(
                Request::get("/tenants/tenant-a/workflows/instance-1/routing")
                    .body(Body::empty())
                    .expect("routing request"),
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await?;
        let payload: Value = serde_json::from_slice(&body)?;
        assert_eq!(payload["definition_id"], "payments");
        assert_eq!(payload["definition_version"], 1);
        assert_eq!(payload["artifact_hash"], "artifact-a");
        assert_eq!(payload["workflow_task_queue"], "payments");
        assert_eq!(payload["routing_status"], "sticky_active");
        assert_eq!(payload["default_compatibility_set_id"], "stable");
        assert_eq!(payload["compatible_build_ids"][0], "build-a");
        let registered_build_ids = payload["registered_build_ids"]
            .as_array()
            .context("registered_build_ids should be an array")?;
        assert!(registered_build_ids.iter().any(|value| value == "build-a"));
        assert!(registered_build_ids.iter().any(|value| value == "build-b"));
        assert_eq!(payload["sticky_workflow_build_id"], "build-a");
        assert_eq!(payload["sticky_workflow_poller_id"], "poller-a");
        assert_eq!(payload["sticky_build_compatible_with_queue"], true);
        assert_eq!(payload["sticky_build_supports_pinned_artifact"], true);

        Ok(())
    }

    #[tokio::test]
    async fn search_returns_milestone_run_and_definition_targets() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        store
            .put_definition(
                "tenant-a",
                &WorkflowDefinition {
                    id: "payments".to_owned(),
                    version: 1,
                    initial_state: "done".to_owned(),
                    states: BTreeMap::from([("done".to_owned(), StateNode::Succeed)]),
                },
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
                None,
                None,
                uuid::Uuid::now_v7(),
                chrono::Utc::now(),
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
                memo: None,
                search_attributes: None,
                sticky_workflow_build_id: Some("build-a".to_owned()),
                sticky_workflow_poller_id: Some("poller-a".to_owned()),
                current_state: Some("charge-card".to_owned()),
                context: None,
                artifact_execution: None,
                status: WorkflowStatus::Running,
                input: Some(json!({"orderId": 42})),
                persisted_input_handle: None,
                output: None,
                event_count: 4,
                last_event_id: uuid::Uuid::now_v7(),
                last_event_type: "ActivityScheduled".to_owned(),
                updated_at: chrono::Utc::now(),
            })
            .await?;

        let Json(payload) = search(
            Query(SearchQuery {
                q: "pay".to_owned(),
                tenant_id: "tenant-a".to_owned(),
                limit: Some(10),
            }),
            State(AppState {
                client: Client::new(),
                ingest_base: "http://127.0.0.1:1".to_owned(),
                query_base: "http://127.0.0.1:1".to_owned(),
                matching_base: "http://127.0.0.1:1".to_owned(),
                store,
                admission: test_admission_config(),
            }),
        )
        .await
        .map_err(|(_, message)| anyhow::anyhow!(message))?;

        let items = payload["items"].as_array().expect("search items array");
        assert!(
            items
                .iter()
                .any(|item| item["kind"] == "run" && item["href"] == "/runs/instance-1/run-1")
        );
        assert!(
            items
                .iter()
                .any(|item| item["kind"] == "definition" && item["href"] == "/workflows/payments")
        );
        Ok(())
    }
}
