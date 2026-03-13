use std::env;

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{OriginalUri, Path, Query, State},
    http::StatusCode,
    routing::{get, post, put},
};
use chrono::Utc;
use fabrik_config::{HttpServiceConfig, PostgresConfig};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::{TaskQueueKind, WorkflowRunFilters, WorkflowStore};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::info;

#[derive(Clone)]
struct AppState {
    client: Client,
    ingest_base: String,
    query_base: String,
    executor_base: String,
    matching_base: String,
    store: WorkflowStore,
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
        executor_base: env::var("EXECUTOR_SERVICE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:3002".to_owned()),
        matching_base: env::var("MATCHING_DEBUG_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:3004".to_owned()),
        store,
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
        get(proxy_to_query_get),
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
        "/tenants/{tenant_id}/workflow-artifacts",
        get(proxy_to_query_get),
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
        "/admin/tenants/{tenant_id}/task-queues/{queue_kind}/{task_queue}",
        get(get_task_queue_inspection),
    )
    .route("/admin/tenants/{tenant_id}/task-queues", get(list_task_queues))
    .route("/admin/debug/executor/hybrid-routing", get(proxy_to_executor_get))
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

async fn proxy_to_executor_get(
    State(state): State<AppState>,
    _uri: OriginalUri,
) -> Result<(StatusCode, Bytes), (StatusCode, String)> {
    proxy_get_path(state.client.clone(), state.executor_base.clone(), "/debug/hybrid-routing").await
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
    Ok(Json(serde_json::to_value(inspection).expect("inspection serializes")))
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

fn queue_kind_label(queue_kind: &TaskQueueKind) -> &'static str {
    match queue_kind {
        TaskQueueKind::Workflow => "workflow",
        TaskQueueKind::Activity => "activity",
    }
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
    async fn gateway_proxies_workflow_and_executor_front_doors() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;

        let ingest = TestHttpServer::start(Router::new().route(
            "/workflows/demo/trigger",
            post(|| async {
                (StatusCode::ACCEPTED, Json(json!({"service": "ingest"}))).into_response()
            }),
        ))
        .await?;
        let query = TestHttpServer::start(Router::new().route(
            "/tenants/tenant-a/workflows/instance-1",
            get(|| async { Json(json!({"service": "query"})).into_response() }),
        ))
        .await?;
        let executor = TestHttpServer::start(Router::new().route(
            "/debug/hybrid-routing",
            get(|| async { Json(json!({"matching_routed_turns": 4})).into_response() }),
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
                executor_base: executor.base_url.clone(),
                matching_base: matching.base_url.clone(),
                store,
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

        let debug_response = app
            .clone()
            .oneshot(
                Request::get("/admin/debug/executor/hybrid-routing")
                    .body(Body::empty())
                    .expect("debug request"),
            )
            .await?;
        assert_eq!(debug_response.status(), StatusCode::OK);
        let debug_body = to_bytes(debug_response.into_body(), usize::MAX).await?;
        assert_eq!(serde_json::from_slice::<Value>(&debug_body)?["matching_routed_turns"], 4);

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
        executor.stop().await;
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
                executor_base: "http://127.0.0.1:1".to_owned(),
                matching_base: "http://127.0.0.1:1".to_owned(),
                store: store.clone(),
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
                executor_base: "http://127.0.0.1:1".to_owned(),
                matching_base: "http://127.0.0.1:1".to_owned(),
                store: store.clone(),
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
                sticky_workflow_build_id: Some("build-a".to_owned()),
                sticky_workflow_poller_id: Some("poller-a".to_owned()),
                current_state: Some("charge-card".to_owned()),
                context: None,
                artifact_execution: None,
                status: WorkflowStatus::Running,
                input: Some(json!({"orderId": 42})),
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
                sticky_workflow_build_id: Some("build-a".to_owned()),
                sticky_workflow_poller_id: Some("poller-a".to_owned()),
                current_state: Some("charge-card".to_owned()),
                context: None,
                artifact_execution: None,
                status: WorkflowStatus::Running,
                input: Some(json!({"orderId": 42})),
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
                executor_base: "http://127.0.0.1:1".to_owned(),
                matching_base: "http://127.0.0.1:1".to_owned(),
                store,
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
