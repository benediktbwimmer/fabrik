use anyhow::Result;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::post,
};
use fabrik_broker::{
    BrokerConfig, WorkflowHistoryFilter, WorkflowPublisher, read_workflow_history,
};
use fabrik_config::{HttpServiceConfig, PostgresConfig, RedpandaConfig};
use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::{WorkflowRunFilters, WorkflowStore};
use fabrik_workflow::{
    CompiledWorkflowArtifact, ReplayDivergence, WorkflowDefinition, WorkflowStatus, artifact_hash,
    validate_compiled_artifact_history_compatibility,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::time::{Duration, Instant};
use tracing::info;
use uuid::Uuid;

const DEFAULT_ARTIFACT_VALIDATION_RUN_LIMIT: usize = 10;
const HISTORY_IDLE_TIMEOUT_MS: u64 = 1_000;
const HISTORY_MAX_SCAN_MS: u64 = 10_000;

#[derive(Clone)]
struct AppState {
    broker: BrokerConfig,
    publisher: WorkflowPublisher,
    store: WorkflowStore,
}

#[derive(Debug, Deserialize, Serialize)]
struct TriggerWorkflowRequest {
    tenant_id: String,
    #[serde(default, alias = "workflow_instance_id")]
    instance_id: Option<String>,
    #[serde(default)]
    workflow_task_queue: Option<String>,
    input: Value,
    #[serde(default)]
    request_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TriggerWorkflowResponse {
    definition_id: String,
    definition_version: u32,
    artifact_hash: String,
    instance_id: String,
    run_id: String,
    event_id: Uuid,
    status: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct SignalWorkflowRequest {
    payload: Value,
    #[serde(default)]
    dedupe_key: Option<String>,
    #[serde(default)]
    request_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct UpdateWorkflowRequest {
    #[serde(default)]
    payload: Value,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    wait_for: Option<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct TerminateWorkflowRequest {
    #[serde(default = "default_terminate_reason")]
    reason: String,
    #[serde(default)]
    request_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ContinueAsNewRequest {
    #[serde(default)]
    input: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct CancelActivityRequest {
    #[serde(default = "default_cancel_reason")]
    reason: String,
    #[serde(default)]
    metadata: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SignalWorkflowResponse {
    instance_id: String,
    run_id: String,
    signal_id: String,
    signal_type: String,
    event_id: Uuid,
    status: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct UpdateWorkflowResponse {
    instance_id: String,
    run_id: String,
    update_id: String,
    status: String,
    accepted_event_id: Option<Uuid>,
    result: Option<Value>,
    error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TerminateWorkflowResponse {
    instance_id: String,
    run_id: String,
    event_id: Uuid,
    status: String,
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
struct CancelActivityResponse {
    instance_id: String,
    run_id: String,
    activity_id: String,
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
    validation: ArtifactValidationSummary,
}

#[derive(Debug, Serialize)]
struct ValidateWorkflowArtifactResponse {
    tenant_id: String,
    definition_id: String,
    version: u32,
    artifact_hash: String,
    compatible: bool,
    status: &'static str,
    validation: ArtifactValidationSummary,
}

#[derive(Debug, Deserialize, Default)]
struct PublishWorkflowArtifactQuery {
    #[serde(default)]
    validate_existing_runs: Option<bool>,
    validation_run_limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct ArtifactValidationSummary {
    enabled: bool,
    validated_run_count: usize,
    skipped_run_count: usize,
    failed_run_count: usize,
    failures: Vec<ArtifactValidationFailure>,
}

#[derive(Debug, Serialize)]
struct ArtifactValidationFailure {
    instance_id: String,
    run_id: String,
    message: String,
    divergence: Option<ReplayDivergence>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = HttpServiceConfig::from_env("INGEST_SERVICE", "ingest-service", 3001)?;
    let redpanda = RedpandaConfig::from_env()?;
    let postgres = PostgresConfig::from_env()?;
    init_tracing(&config.log_filter);
    info!(port = config.port, "starting ingest service");

    let broker = BrokerConfig::new(
        redpanda.brokers,
        redpanda.workflow_events_topic,
        redpanda.workflow_events_partitions,
    );
    let store = WorkflowStore::connect(&postgres.url).await?;
    store.init().await?;
    let app = default_router::<AppState>(ServiceInfo::new(
        config.name,
        "ingest",
        env!("CARGO_PKG_VERSION"),
    ))
    .route("/tenants/{tenant_id}/workflow-definitions", post(publish_workflow_definition))
    .route("/tenants/{tenant_id}/workflow-artifacts", post(publish_workflow_artifact))
    .route("/tenants/{tenant_id}/workflow-artifacts/validate", post(validate_workflow_artifact))
    .route("/workflows/{workflow_id}/trigger", post(trigger_workflow))
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/signals/{signal_type}",
        post(signal_workflow),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/updates/{update_name}",
        post(update_workflow),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/terminate",
        post(terminate_workflow),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/continue-as-new",
        post(continue_as_new),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{workflow_instance_id}/activities/{activity_id}/cancel",
        post(cancel_activity),
    )
    .with_state(AppState {
        broker: broker.clone(),
        publisher: WorkflowPublisher::new(&broker, "ingest-service").await?,
        store,
    });

    serve(app, config.port).await
}

async fn publish_workflow_artifact(
    Path(tenant_id): Path<String>,
    Query(query): Query<PublishWorkflowArtifactQuery>,
    State(state): State<AppState>,
    Json(artifact): Json<CompiledWorkflowArtifact>,
) -> Result<(StatusCode, Json<PublishWorkflowArtifactResponse>), (StatusCode, String)> {
    artifact.validate().map_err(validation_error)?;

    let validation = if query.validate_existing_runs.unwrap_or(true) {
        validate_artifact_against_recent_runs(
            &state,
            &tenant_id,
            &artifact,
            query.validation_run_limit.unwrap_or(DEFAULT_ARTIFACT_VALIDATION_RUN_LIMIT),
        )
        .await?
    } else {
        ArtifactValidationSummary {
            enabled: false,
            validated_run_count: 0,
            skipped_run_count: 0,
            failed_run_count: 0,
            failures: Vec::new(),
        }
    };
    if validation.failed_run_count > 0 {
        let failure = &validation.failures[0];
        let sample = validation
            .failures
            .iter()
            .take(3)
            .map(|failure| format!("{}/{}", failure.instance_id, failure.run_id))
            .collect::<Vec<_>>()
            .join(", ");
        return Err((
            StatusCode::CONFLICT,
            format!(
                "artifact rollout validation failed for {} run(s); sample: {}; first issue: {}",
                validation.failed_run_count, sample, failure.message
            ),
        ));
    }

    state.store.put_artifact(&tenant_id, &artifact).await.map_err(internal_error)?;

    Ok((
        StatusCode::CREATED,
        Json(PublishWorkflowArtifactResponse {
            tenant_id,
            definition_id: artifact.definition_id.clone(),
            version: artifact.definition_version,
            artifact_hash: artifact.artifact_hash.clone(),
            status: "stored",
            validation,
        }),
    ))
}

async fn validate_workflow_artifact(
    Path(tenant_id): Path<String>,
    Query(query): Query<PublishWorkflowArtifactQuery>,
    State(state): State<AppState>,
    Json(artifact): Json<CompiledWorkflowArtifact>,
) -> Result<Json<ValidateWorkflowArtifactResponse>, (StatusCode, String)> {
    artifact.validate().map_err(validation_error)?;
    let validation = if query.validate_existing_runs.unwrap_or(true) {
        validate_artifact_against_recent_runs(
            &state,
            &tenant_id,
            &artifact,
            query.validation_run_limit.unwrap_or(DEFAULT_ARTIFACT_VALIDATION_RUN_LIMIT),
        )
        .await?
    } else {
        ArtifactValidationSummary {
            enabled: false,
            validated_run_count: 0,
            skipped_run_count: 0,
            failed_run_count: 0,
            failures: Vec::new(),
        }
    };

    Ok(Json(ValidateWorkflowArtifactResponse {
        tenant_id,
        definition_id: artifact.definition_id.clone(),
        version: artifact.definition_version,
        artifact_hash: artifact.artifact_hash.clone(),
        compatible: validation.failed_run_count == 0,
        status: if validation.failed_run_count == 0 { "validated" } else { "incompatible" },
        validation,
    }))
}

async fn validate_artifact_against_recent_runs(
    state: &AppState,
    tenant_id: &str,
    artifact: &CompiledWorkflowArtifact,
    run_limit: usize,
) -> Result<ArtifactValidationSummary, (StatusCode, String)> {
    let runs = state
        .store
        .list_runs_page_with_filters(
            tenant_id,
            &WorkflowRunFilters {
                definition_id: Some(artifact.definition_id.clone()),
                ..WorkflowRunFilters::default()
            },
            i64::try_from(run_limit).map_err(|error| validation_error(error))?,
            0,
        )
        .await
        .map_err(internal_error)?;
    let mut summary = ArtifactValidationSummary {
        enabled: true,
        validated_run_count: 0,
        skipped_run_count: 0,
        failed_run_count: 0,
        failures: Vec::new(),
    };

    for run in runs {
        let history = read_workflow_history(
            &state.broker,
            "ingest-artifact-validator",
            &WorkflowHistoryFilter::new(tenant_id, &run.instance_id, &run.run_id),
            Duration::from_millis(HISTORY_IDLE_TIMEOUT_MS),
            Duration::from_millis(HISTORY_MAX_SCAN_MS),
        )
        .await
        .map_err(internal_error)?;
        if history.is_empty() {
            summary.skipped_run_count = summary.skipped_run_count.saturating_add(1);
            continue;
        }

        let Some(version) = run.definition_version else {
            summary.skipped_run_count = summary.skipped_run_count.saturating_add(1);
            continue;
        };
        let Some(pinned_artifact) = state
            .store
            .get_artifact_version(tenant_id, &artifact.definition_id, version)
            .await
            .map_err(internal_error)?
        else {
            summary.skipped_run_count = summary.skipped_run_count.saturating_add(1);
            continue;
        };

        let divergences =
            validate_compiled_artifact_history_compatibility(&history, &pinned_artifact, artifact)
                .map_err(|error| {
                    (
                        StatusCode::CONFLICT,
                        format!(
                            "artifact replay validation failed for {}/{}: {error}",
                            run.instance_id, run.run_id
                        ),
                    )
                })?;
        if let Some(divergence) = divergences.into_iter().next() {
            summary.failed_run_count = summary.failed_run_count.saturating_add(1);
            summary.failures.push(ArtifactValidationFailure {
                instance_id: run.instance_id.clone(),
                run_id: run.run_id.clone(),
                message: divergence.message.clone(),
                divergence: Some(divergence),
            });
            continue;
        }

        summary.validated_run_count = summary.validated_run_count.saturating_add(1);
    }

    Ok(summary)
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
    if let Some(request_id) = request.request_id.as_deref() {
        if let Some(response) = load_dedup_response::<TriggerWorkflowResponse, _>(
            &state.store,
            &request.tenant_id,
            request.instance_id.as_deref(),
            &format!("trigger:{definition_id}"),
            request_id,
            &request,
        )
        .await?
        {
            return Ok((StatusCode::ACCEPTED, Json(response)));
        }
    }
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

    let instance_id =
        request.instance_id.clone().unwrap_or_else(|| format!("wf-{}", Uuid::now_v7()));
    let run_id = format!("run-{}", Uuid::now_v7());
    let workflow_task_queue =
        request.workflow_task_queue.clone().unwrap_or_else(|| "default".to_owned());
    let payload = WorkflowEvent::WorkflowTriggered { input: request.input.clone() };
    let mut envelope = EventEnvelope::new(
        payload.event_type(),
        WorkflowIdentity::new(
            request.tenant_id.clone(),
            resolved_definition_id.clone(),
            definition_version,
            artifact_hash.clone(),
            instance_id.clone(),
            run_id.clone(),
            "ingest-service",
        ),
        payload,
    );
    envelope.metadata.insert("workflow_task_queue".to_owned(), workflow_task_queue.clone());

    state.publisher.publish(&envelope, &envelope.partition_key).await.map_err(internal_error)?;
    state
        .store
        .put_run_start(
            &envelope.tenant_id,
            &envelope.instance_id,
            &envelope.run_id,
            &envelope.definition_id,
            Some(envelope.definition_version),
            Some(&envelope.artifact_hash),
            &workflow_task_queue,
            envelope.event_id,
            envelope.occurred_at,
            None,
            None,
        )
        .await
        .map_err(internal_error)?;

    let response = TriggerWorkflowResponse {
        definition_id: resolved_definition_id,
        definition_version,
        artifact_hash,
        instance_id,
        run_id,
        event_id: envelope.event_id,
        status: "accepted".to_owned(),
    };
    if let Some(request_id) = request.request_id.as_deref() {
        store_dedup_response(
            &state.store,
            &envelope.tenant_id,
            Some(&envelope.instance_id),
            &format!("trigger:{definition_id}"),
            request_id,
            &request,
            &response,
        )
        .await?;
    }
    Ok((StatusCode::ACCEPTED, Json(response)))
}

async fn signal_workflow(
    Path((tenant_id, instance_id, signal_type)): Path<(String, String, String)>,
    State(state): State<AppState>,
    Json(request): Json<SignalWorkflowRequest>,
) -> Result<(StatusCode, Json<SignalWorkflowResponse>), (StatusCode, String)> {
    if let Some(request_id) = request.request_id.as_deref() {
        if let Some(response) = load_dedup_response::<SignalWorkflowResponse, _>(
            &state.store,
            &tenant_id,
            Some(&instance_id),
            &format!("signal:{signal_type}"),
            request_id,
            &request,
        )
        .await?
        {
            return Ok((StatusCode::ACCEPTED, Json(response)));
        }
    }
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

    if matches!(
        instance.status,
        WorkflowStatus::Completed
            | WorkflowStatus::Failed
            | WorkflowStatus::Cancelled
            | WorkflowStatus::Terminated
    ) {
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

    let signal_id = format!("sig-{}", Uuid::now_v7());
    let payload = WorkflowEvent::SignalQueued {
        signal_id: signal_id.clone(),
        signal_type: signal_type.clone(),
        payload: request.payload.clone(),
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
    let queued = state
        .store
        .queue_signal(
            &envelope.tenant_id,
            &envelope.instance_id,
            &envelope.run_id,
            &signal_id,
            &signal_type,
            request.dedupe_key.as_deref(),
            &request.payload,
            envelope.event_id,
            envelope.occurred_at,
        )
        .await
        .map_err(internal_error)?;
    if !queued {
        let response = SignalWorkflowResponse {
            instance_id,
            run_id: instance.run_id,
            signal_id,
            signal_type,
            event_id: envelope.event_id,
            status: "duplicate".to_owned(),
        };
        return Ok((StatusCode::ACCEPTED, Json(response)));
    }

    if let Err(error) = state.publisher.publish(&envelope, &envelope.partition_key).await {
        let _ = state
            .store
            .delete_signal(&envelope.tenant_id, &envelope.instance_id, &envelope.run_id, &signal_id)
            .await;
        return Err(internal_error(error));
    }
    let response = SignalWorkflowResponse {
        instance_id,
        run_id: instance.run_id,
        signal_id,
        signal_type,
        event_id: envelope.event_id,
        status: "accepted".to_owned(),
    };
    if let Some(request_id) = request.request_id.as_deref() {
        store_dedup_response(
            &state.store,
            &envelope.tenant_id,
            Some(&envelope.instance_id),
            &format!("signal:{}", response.signal_type),
            request_id,
            &request,
            &response,
        )
        .await?;
    }
    Ok((StatusCode::ACCEPTED, Json(response)))
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
    let workflow_task_queue = state
        .store
        .get_run_record(&tenant_id, &instance_id, &instance.run_id)
        .await
        .map_err(internal_error)?
        .map(|run| run.workflow_task_queue)
        .unwrap_or_else(|| instance.workflow_task_queue.clone());
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
    trigger.metadata.insert("workflow_task_queue".to_owned(), workflow_task_queue.clone());

    state.publisher.publish(&trigger, &trigger.partition_key).await.map_err(internal_error)?;
    state
        .store
        .put_run_start(
            &trigger.tenant_id,
            &trigger.instance_id,
            &trigger.run_id,
            &trigger.definition_id,
            Some(trigger.definition_version),
            Some(&trigger.artifact_hash),
            &workflow_task_queue,
            trigger.event_id,
            trigger.occurred_at,
            Some(&instance.run_id),
            Some(&instance.run_id),
        )
        .await
        .map_err(internal_error)?;
    state
        .store
        .record_run_continuation(
            &continued.tenant_id,
            &continued.instance_id,
            &instance.run_id,
            &new_run_id,
            "manual_continue_as_new",
            continued.event_id,
            trigger.event_id,
            trigger.occurred_at,
        )
        .await
        .map_err(internal_error)?;

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

async fn update_workflow(
    Path((tenant_id, instance_id, update_name)): Path<(String, String, String)>,
    State(state): State<AppState>,
    Json(request): Json<UpdateWorkflowRequest>,
) -> Result<(StatusCode, Json<UpdateWorkflowResponse>), (StatusCode, String)> {
    if let Some(request_id) = request.request_id.as_deref() {
        if let Some(response) = load_dedup_response::<UpdateWorkflowResponse, _>(
            &state.store,
            &tenant_id,
            Some(&instance_id),
            &format!("update:{update_name}"),
            request_id,
            &request,
        )
        .await?
        {
            return Ok((StatusCode::ACCEPTED, Json(response)));
        }
    }

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
    if matches!(
        instance.status,
        WorkflowStatus::Completed | WorkflowStatus::Failed | WorkflowStatus::Terminated
    ) {
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
    let update_id = format!("upd-{}", Uuid::now_v7());
    let payload = WorkflowEvent::WorkflowUpdateRequested {
        update_id: update_id.clone(),
        update_name: update_name.clone(),
        payload: request.payload.clone(),
    };
    let envelope = EventEnvelope::new(
        payload.event_type(),
        WorkflowIdentity::new(
            tenant_id.clone(),
            instance.definition_id.clone(),
            definition_version,
            artifact_hash,
            instance.instance_id.clone(),
            instance.run_id.clone(),
            "ingest-service",
        ),
        payload,
    );
    state
        .store
        .queue_update(
            &tenant_id,
            &instance_id,
            &instance.run_id,
            &update_id,
            &update_name,
            request.request_id.as_deref(),
            &request.payload,
            envelope.event_id,
            envelope.occurred_at,
        )
        .await
        .map_err(internal_error)?;
    state.publisher.publish(&envelope, &envelope.partition_key).await.map_err(internal_error)?;

    let mut response = UpdateWorkflowResponse {
        instance_id: instance_id.clone(),
        run_id: instance.run_id.clone(),
        update_id: update_id.clone(),
        status: "requested".to_owned(),
        accepted_event_id: None,
        result: None,
        error: None,
    };

    if let Some(wait_for) = request.wait_for.as_deref() {
        let deadline = Instant::now() + Duration::from_millis(request.timeout_ms.unwrap_or(5_000));
        loop {
            if let Some(update) = state
                .store
                .get_update(&tenant_id, &instance_id, &instance.run_id, &update_id)
                .await
                .map_err(internal_error)?
            {
                response.status = update_status_label(&update.status).to_owned();
                response.accepted_event_id = update.accepted_event_id;
                response.result = update.output.clone();
                response.error = update.error.clone();
                if wait_for.eq_ignore_ascii_case("accepted")
                    && !matches!(update.status, fabrik_store::WorkflowUpdateStatus::Requested)
                {
                    break;
                }
                if wait_for.eq_ignore_ascii_case("completed")
                    && matches!(
                        update.status,
                        fabrik_store::WorkflowUpdateStatus::Completed
                            | fabrik_store::WorkflowUpdateStatus::Rejected
                    )
                {
                    break;
                }
            }
            if Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    if let Some(request_id) = request.request_id.as_deref() {
        store_dedup_response(
            &state.store,
            &tenant_id,
            Some(&instance_id),
            &format!("update:{update_name}"),
            request_id,
            &request,
            &response,
        )
        .await?;
    }

    Ok((StatusCode::ACCEPTED, Json(response)))
}

async fn terminate_workflow(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    State(state): State<AppState>,
    Json(request): Json<TerminateWorkflowRequest>,
) -> Result<(StatusCode, Json<TerminateWorkflowResponse>), (StatusCode, String)> {
    if let Some(request_id) = request.request_id.as_deref() {
        if let Some(response) = load_dedup_response::<TerminateWorkflowResponse, _>(
            &state.store,
            &tenant_id,
            Some(&instance_id),
            "terminate",
            request_id,
            &request,
        )
        .await?
        {
            return Ok((StatusCode::ACCEPTED, Json(response)));
        }
    }
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
    let payload = WorkflowEvent::WorkflowTerminated { reason: request.reason.clone() };
    let envelope = EventEnvelope::new(
        payload.event_type(),
        WorkflowIdentity::new(
            tenant_id.clone(),
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
    let response = TerminateWorkflowResponse {
        instance_id,
        run_id: instance.run_id,
        event_id: envelope.event_id,
        status: "accepted".to_owned(),
    };
    if let Some(request_id) = request.request_id.as_deref() {
        store_dedup_response(
            &state.store,
            &tenant_id,
            Some(&response.instance_id),
            "terminate",
            request_id,
            &request,
            &response,
        )
        .await?;
    }
    Ok((StatusCode::ACCEPTED, Json(response)))
}

async fn cancel_activity(
    Path((tenant_id, instance_id, activity_id)): Path<(String, String, String)>,
    State(state): State<AppState>,
    Json(request): Json<CancelActivityRequest>,
) -> Result<(StatusCode, Json<CancelActivityResponse>), (StatusCode, String)> {
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

    if matches!(
        instance.status,
        WorkflowStatus::Completed
            | WorkflowStatus::Failed
            | WorkflowStatus::Cancelled
            | WorkflowStatus::Terminated
    ) {
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
    let active_activity = state
        .store
        .get_latest_active_activity(&tenant_id, &instance_id, &instance.run_id, &activity_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::CONFLICT,
                format!("activity {activity_id} is not currently active on workflow {instance_id}"),
            )
        })?;

    let payload = WorkflowEvent::ActivityTaskCancellationRequested {
        activity_id: activity_id.clone(),
        attempt: active_activity.attempt,
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
        Json(CancelActivityResponse {
            instance_id,
            run_id: instance.run_id,
            activity_id,
            attempt: active_activity.attempt,
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

async fn load_dedup_response<T: DeserializeOwned, R: Serialize>(
    store: &WorkflowStore,
    tenant_id: &str,
    instance_id: Option<&str>,
    operation: &str,
    request_id: &str,
    request: &R,
) -> Result<Option<T>, (StatusCode, String)> {
    let request_hash =
        serde_json::to_string(request).map_err(|error| internal_error(error.into()))?;
    let Some(existing) = store
        .get_request_dedup(tenant_id, instance_id, operation, request_id)
        .await
        .map_err(internal_error)?
    else {
        return Ok(None);
    };
    if existing.request_hash != request_hash {
        return Err((
            StatusCode::CONFLICT,
            format!("request_id {request_id} was already used with a different payload"),
        ));
    }
    serde_json::from_value(existing.response).map(Some).map_err(|error| {
        internal_error(anyhow::anyhow!("failed to decode dedup response: {error}"))
    })
}

async fn store_dedup_response<R: Serialize, T: Serialize>(
    store: &WorkflowStore,
    tenant_id: &str,
    instance_id: Option<&str>,
    operation: &str,
    request_id: &str,
    request: &R,
    response: &T,
) -> Result<(), (StatusCode, String)> {
    let request_hash =
        serde_json::to_string(request).map_err(|error| internal_error(error.into()))?;
    let response_value =
        serde_json::to_value(response).map_err(|error| internal_error(error.into()))?;
    store
        .upsert_request_dedup(
            tenant_id,
            instance_id,
            operation,
            request_id,
            &request_hash,
            &response_value,
        )
        .await
        .map_err(internal_error)?;
    Ok(())
}

fn default_cancel_reason() -> String {
    "cancelled by operator".to_owned()
}

fn default_terminate_reason() -> String {
    "terminated by operator".to_owned()
}

fn update_status_label(status: &fabrik_store::WorkflowUpdateStatus) -> &'static str {
    match status {
        fabrik_store::WorkflowUpdateStatus::Requested => "requested",
        fabrik_store::WorkflowUpdateStatus::Accepted => "accepted",
        fabrik_store::WorkflowUpdateStatus::Completed => "completed",
        fabrik_store::WorkflowUpdateStatus::Rejected => "rejected",
    }
}
