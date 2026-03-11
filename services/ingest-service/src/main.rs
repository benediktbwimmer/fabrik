use anyhow::Result;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    routing::post,
};
use fabrik_broker::{BrokerConfig, WorkflowPublisher};
use fabrik_config::{HttpServiceConfig, PostgresConfig, RedpandaConfig};
use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::WorkflowStore;
use fabrik_workflow::{
    CompiledWorkflowArtifact, WorkflowDefinition, WorkflowStatus, artifact_hash,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::info;
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    publisher: WorkflowPublisher,
    store: WorkflowStore,
}

#[derive(Debug, Deserialize)]
struct TriggerWorkflowRequest {
    tenant_id: String,
    #[serde(default, alias = "workflow_instance_id")]
    instance_id: Option<String>,
    input: Value,
}

#[derive(Debug, Serialize)]
struct TriggerWorkflowResponse {
    definition_id: String,
    definition_version: u32,
    artifact_hash: String,
    instance_id: String,
    run_id: String,
    event_id: Uuid,
    status: &'static str,
}

#[derive(Debug, Deserialize)]
struct SignalWorkflowRequest {
    payload: Value,
}

#[derive(Debug, Deserialize)]
struct ContinueAsNewRequest {
    #[serde(default)]
    input: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct CancelEffectRequest {
    #[serde(default = "default_cancel_reason")]
    reason: String,
    #[serde(default)]
    metadata: Option<Value>,
}

#[derive(Debug, Serialize)]
struct SignalWorkflowResponse {
    instance_id: String,
    run_id: String,
    signal_type: String,
    event_id: Uuid,
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct ContinueAsNewResponse {
    definition_id: String,
    definition_version: u32,
    artifact_hash: String,
    instance_id: String,
    previous_run_id: String,
    run_id: String,
    continued_event_id: Uuid,
    triggered_event_id: Uuid,
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct CancelEffectResponse {
    instance_id: String,
    run_id: String,
    effect_id: String,
    attempt: u32,
    event_id: Uuid,
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct PublishWorkflowDefinitionResponse {
    tenant_id: String,
    definition_id: String,
    version: u32,
    artifact_hash: String,
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct PublishWorkflowArtifactResponse {
    tenant_id: String,
    definition_id: String,
    version: u32,
    artifact_hash: String,
    status: &'static str,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = HttpServiceConfig::from_env("INGEST_SERVICE", "ingest-service", 3001)?;
    let redpanda = RedpandaConfig::from_env()?;
    let postgres = PostgresConfig::from_env()?;
    init_tracing(&config.log_filter);
    info!(port = config.port, "starting ingest service");

    let broker = BrokerConfig::new(redpanda.brokers, redpanda.workflow_events_topic);
    let store = WorkflowStore::connect(&postgres.url).await?;
    store.init().await?;
    let app = default_router::<AppState>(ServiceInfo::new(
        config.name,
        "ingest",
        env!("CARGO_PKG_VERSION"),
    ))
    .route("/tenants/{tenant_id}/workflow-definitions", post(publish_workflow_definition))
    .route("/tenants/{tenant_id}/workflow-artifacts", post(publish_workflow_artifact))
    .route("/workflows/{workflow_id}/trigger", post(trigger_workflow))
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/signals/{signal_type}",
        post(signal_workflow),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/continue-as-new",
        post(continue_as_new),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/effects/{effect_id}/cancel",
        post(cancel_effect),
    )
    .with_state(AppState {
        publisher: WorkflowPublisher::new(&broker, "ingest-service").await?,
        store,
    });

    serve(app, config.port).await
}

async fn publish_workflow_artifact(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    Json(artifact): Json<CompiledWorkflowArtifact>,
) -> Result<(StatusCode, Json<PublishWorkflowArtifactResponse>), (StatusCode, String)> {
    artifact.validate().map_err(validation_error)?;

    state.store.put_artifact(&tenant_id, &artifact).await.map_err(internal_error)?;

    Ok((
        StatusCode::CREATED,
        Json(PublishWorkflowArtifactResponse {
            tenant_id,
            definition_id: artifact.definition_id.clone(),
            version: artifact.definition_version,
            artifact_hash: artifact.artifact_hash.clone(),
            status: "stored",
        }),
    ))
}

async fn publish_workflow_definition(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
    Json(definition): Json<WorkflowDefinition>,
) -> Result<(StatusCode, Json<PublishWorkflowDefinitionResponse>), (StatusCode, String)> {
    definition.validate().map_err(validation_error)?;

    state.store.put_definition(&tenant_id, &definition).await.map_err(internal_error)?;

    Ok((
        StatusCode::CREATED,
        Json(PublishWorkflowDefinitionResponse {
            tenant_id,
            definition_id: definition.id.clone(),
            version: definition.version,
            artifact_hash: artifact_hash(&definition),
            status: "stored",
        }),
    ))
}

async fn trigger_workflow(
    Path(definition_id): Path<String>,
    State(state): State<AppState>,
    Json(request): Json<TriggerWorkflowRequest>,
) -> Result<(StatusCode, Json<TriggerWorkflowResponse>), (StatusCode, String)> {
    let (resolved_definition_id, definition_version, artifact_hash) = if let Some(artifact) = state
        .store
        .get_latest_artifact(&request.tenant_id, &definition_id)
        .await
        .map_err(internal_error)?
    {
        (artifact.definition_id, artifact.definition_version, artifact.artifact_hash)
    } else {
        let definition = state
            .store
            .get_latest_definition(&request.tenant_id, &definition_id)
            .await
            .map_err(internal_error)?
            .ok_or_else(|| {
                (
                    StatusCode::NOT_FOUND,
                    format!(
                        "workflow definition {definition_id} not found for tenant {}",
                        request.tenant_id
                    ),
                )
            })?;
        (definition.id.clone(), definition.version, artifact_hash(&definition))
    };

    let instance_id = request.instance_id.unwrap_or_else(|| format!("wf-{}", Uuid::now_v7()));
    let run_id = format!("run-{}", Uuid::now_v7());
    let payload = WorkflowEvent::WorkflowTriggered { input: request.input };
    let envelope = EventEnvelope::new(
        payload.event_type(),
        WorkflowIdentity::new(
            request.tenant_id,
            resolved_definition_id.clone(),
            definition_version,
            artifact_hash.clone(),
            instance_id.clone(),
            run_id.clone(),
            "ingest-service",
        ),
        payload,
    );

    state.publisher.publish(&envelope, &envelope.partition_key).await.map_err(internal_error)?;

    Ok((
        StatusCode::ACCEPTED,
        Json(TriggerWorkflowResponse {
            definition_id: resolved_definition_id,
            definition_version,
            artifact_hash,
            instance_id,
            run_id,
            event_id: envelope.event_id,
            status: "accepted",
        }),
    ))
}

async fn signal_workflow(
    Path((tenant_id, instance_id, signal_type)): Path<(String, String, String)>,
    State(state): State<AppState>,
    Json(request): Json<SignalWorkflowRequest>,
) -> Result<(StatusCode, Json<SignalWorkflowResponse>), (StatusCode, String)> {
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

    if matches!(instance.status, WorkflowStatus::Completed | WorkflowStatus::Failed) {
        return Err((
            StatusCode::CONFLICT,
            format!("workflow instance {instance_id} is already {}", instance.status.as_str()),
        ));
    }

    let definition_version = instance.definition_version.ok_or_else(|| {
        (
            StatusCode::CONFLICT,
            format!(
                "workflow instance {instance_id} has no pinned definition version; try again after it starts"
            ),
        )
    })?;
    let artifact_hash = instance.artifact_hash.clone().ok_or_else(|| {
        (
            StatusCode::CONFLICT,
            format!(
                "workflow instance {instance_id} has no pinned artifact hash; try again after it starts"
            ),
        )
    })?;

    let payload = WorkflowEvent::SignalReceived {
        signal_type: signal_type.clone(),
        payload: request.payload,
    };
    let envelope = EventEnvelope::new(
        payload.event_type(),
        WorkflowIdentity::new(
            tenant_id,
            instance.definition_id.clone(),
            definition_version,
            artifact_hash,
            instance.instance_id.clone(),
            instance.run_id.clone(),
            "ingest-service",
        ),
        payload,
    );

    state.publisher.publish(&envelope, &envelope.partition_key).await.map_err(internal_error)?;

    Ok((
        StatusCode::ACCEPTED,
        Json(SignalWorkflowResponse {
            instance_id,
            run_id: instance.run_id,
            signal_type,
            event_id: envelope.event_id,
            status: "accepted",
        }),
    ))
}

async fn continue_as_new(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    State(state): State<AppState>,
    Json(request): Json<ContinueAsNewRequest>,
) -> Result<(StatusCode, Json<ContinueAsNewResponse>), (StatusCode, String)> {
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

    let definition_version = instance.definition_version.ok_or_else(|| {
        (
            StatusCode::CONFLICT,
            format!("workflow instance {instance_id} has no pinned definition version"),
        )
    })?;
    let artifact_hash = instance.artifact_hash.clone().ok_or_else(|| {
        (
            StatusCode::CONFLICT,
            format!("workflow instance {instance_id} has no pinned artifact hash"),
        )
    })?;

    let definition_exists = state
        .store
        .get_artifact_version(&tenant_id, &instance.definition_id, definition_version)
        .await
        .map_err(internal_error)?
        .is_some()
        || state
            .store
            .get_definition_version(&tenant_id, &instance.definition_id, definition_version)
            .await
            .map_err(internal_error)?
            .is_some();
    if !definition_exists {
        return Err((
            StatusCode::CONFLICT,
            format!(
                "workflow definition or artifact {} version {} is no longer available for tenant {tenant_id}",
                instance.definition_id, definition_version
            ),
        ));
    }

    let carried_input = request
        .input
        .or_else(|| instance.context.clone())
        .or_else(|| instance.output.clone())
        .or_else(|| instance.input.clone())
        .unwrap_or(Value::Null);
    let new_run_id = format!("run-{}", Uuid::now_v7());

    let continue_payload = WorkflowEvent::WorkflowContinuedAsNew {
        new_run_id: new_run_id.clone(),
        input: carried_input.clone(),
    };
    let mut continued = EventEnvelope::new(
        continue_payload.event_type(),
        WorkflowIdentity::new(
            tenant_id.clone(),
            instance.definition_id.clone(),
            definition_version,
            artifact_hash.clone(),
            instance.instance_id.clone(),
            instance.run_id.clone(),
            "ingest-service",
        ),
        continue_payload,
    );
    continued.correlation_id = Some(continued.event_id);

    state.publisher.publish(&continued, &continued.partition_key).await.map_err(internal_error)?;

    let trigger_payload = WorkflowEvent::WorkflowTriggered { input: carried_input };
    let mut trigger = EventEnvelope::new(
        trigger_payload.event_type(),
        WorkflowIdentity::new(
            tenant_id,
            instance.definition_id.clone(),
            definition_version,
            artifact_hash.clone(),
            instance.instance_id.clone(),
            new_run_id.clone(),
            "ingest-service",
        ),
        trigger_payload,
    );
    trigger.causation_id = Some(continued.event_id);
    trigger.correlation_id = continued.correlation_id;

    state.publisher.publish(&trigger, &trigger.partition_key).await.map_err(internal_error)?;

    Ok((
        StatusCode::ACCEPTED,
        Json(ContinueAsNewResponse {
            definition_id: instance.definition_id,
            definition_version,
            artifact_hash,
            instance_id,
            previous_run_id: instance.run_id,
            run_id: new_run_id,
            continued_event_id: continued.event_id,
            triggered_event_id: trigger.event_id,
            status: "accepted",
        }),
    ))
}

async fn cancel_effect(
    Path((tenant_id, instance_id, effect_id)): Path<(String, String, String)>,
    State(state): State<AppState>,
    Json(request): Json<CancelEffectRequest>,
) -> Result<(StatusCode, Json<CancelEffectResponse>), (StatusCode, String)> {
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

    if matches!(instance.status, WorkflowStatus::Completed | WorkflowStatus::Failed) {
        return Err((
            StatusCode::CONFLICT,
            format!("workflow instance {instance_id} is already {}", instance.status.as_str()),
        ));
    }

    let definition_version = instance.definition_version.ok_or_else(|| {
        (
            StatusCode::CONFLICT,
            format!("workflow instance {instance_id} has no pinned definition version"),
        )
    })?;
    let artifact_hash = instance.artifact_hash.clone().ok_or_else(|| {
        (
            StatusCode::CONFLICT,
            format!("workflow instance {instance_id} has no pinned artifact hash"),
        )
    })?;
    let active_effect = state
        .store
        .get_latest_active_effect(&tenant_id, &instance_id, &instance.run_id, &effect_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::CONFLICT,
                format!("effect {effect_id} is not currently active on workflow {instance_id}"),
            )
        })?;

    let payload = WorkflowEvent::EffectCancelled {
        effect_id: effect_id.clone(),
        attempt: active_effect.attempt,
        reason: request.reason,
        metadata: request.metadata,
    };
    let envelope = EventEnvelope::new(
        payload.event_type(),
        WorkflowIdentity::new(
            tenant_id,
            instance.definition_id.clone(),
            definition_version,
            artifact_hash,
            instance.instance_id.clone(),
            instance.run_id.clone(),
            "ingest-service",
        ),
        payload,
    );
    state.publisher.publish(&envelope, &envelope.partition_key).await.map_err(internal_error)?;

    Ok((
        StatusCode::ACCEPTED,
        Json(CancelEffectResponse {
            instance_id,
            run_id: instance.run_id,
            effect_id,
            attempt: active_effect.attempt,
            event_id: envelope.event_id,
            status: "accepted",
        }),
    ))
}

fn internal_error(error: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

fn validation_error(error: impl std::fmt::Display) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, error.to_string())
}

fn default_cancel_reason() -> String {
    "cancelled by operator".to_owned()
}
