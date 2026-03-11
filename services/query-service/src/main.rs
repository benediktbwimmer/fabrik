use anyhow::Result;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use fabrik_config::{HttpServiceConfig, PostgresConfig};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::WorkflowStore;
use fabrik_workflow::{CompiledWorkflowArtifact, WorkflowDefinition, WorkflowInstanceState};
use serde::Serialize;
use tracing::info;

#[derive(Clone)]
struct AppState {
    store: WorkflowStore,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    message: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = HttpServiceConfig::from_env("QUERY_SERVICE", "query-service", 3005)?;
    let postgres = PostgresConfig::from_env()?;
    init_tracing(&config.log_filter);
    info!(port = config.port, "starting query service");

    let store = WorkflowStore::connect(&postgres.url).await?;
    store.init().await?;

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
    .with_state(AppState { store });

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

fn internal_error(error: anyhow::Error) -> (StatusCode, Json<ErrorResponse>) {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { message: error.to_string() }))
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
