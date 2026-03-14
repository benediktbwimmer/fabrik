use std::{collections::HashMap, env};

use anyhow::{Context, Result};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::post,
};
use chrono::{DateTime, Utc};
use fabrik_broker::{
    BrokerConfig, JsonTopicConfig, WorkflowHistoryFilter, WorkflowPublisher,
    build_json_consumer_from_offsets, decode_json_record, read_workflow_history,
};
use fabrik_config::{HttpServiceConfig, PostgresConfig, RedpandaConfig};
use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::{
    TopicAdapterRecord, TopicAdapterResolvedDispatch, WorkflowRunFilters, WorkflowStore,
    resolve_topic_adapter_dispatch,
};
use fabrik_workflow::{
    CompiledWorkflowArtifact, ReplayDivergence, WorkflowDefinition, WorkflowStatus, artifact_hash,
    validate_compiled_artifact_history_compatibility,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::{
    task::JoinHandle,
    time::{Duration, Instant},
};
use tracing::{error, info, warn};
use uuid::Uuid;

const DEFAULT_ARTIFACT_VALIDATION_RUN_LIMIT: usize = 10;
const HISTORY_IDLE_TIMEOUT_MS: u64 = 1_000;
const HISTORY_MAX_SCAN_MS: u64 = 10_000;
const TOPIC_ADAPTER_RECONCILE_INTERVAL_MS: u64 = 5_000;
const TOPIC_ADAPTER_RESTART_DELAY_MS: u64 = 1_000;
const TOPIC_ADAPTER_OWNERSHIP_LEASE_TTL_MS: u64 = 5_000;
const TOPIC_ADAPTER_OWNERSHIP_RENEW_INTERVAL_MS: u64 = 1_000;
const TOPIC_ADAPTER_OWNERSHIP_RETRY_DELAY_MS: u64 = 1_000;

#[derive(Clone)]
struct AppState {
    broker: BrokerConfig,
    publisher: WorkflowPublisher,
    store: WorkflowStore,
    runtime_id: String,
}

#[derive(Clone)]
struct TopicAdapterTaskSpec {
    tenant_id: String,
    adapter_id: String,
    updated_at: DateTime<Utc>,
    is_paused: bool,
}

#[derive(Debug)]
struct TopicAdapterOwnershipLost {
    tenant_id: String,
    adapter_id: String,
    owner_epoch: u64,
}

impl TopicAdapterOwnershipLost {
    fn new(tenant_id: &str, adapter_id: &str, owner_epoch: u64) -> Self {
        Self {
            tenant_id: tenant_id.to_owned(),
            adapter_id: adapter_id.to_owned(),
            owner_epoch,
        }
    }
}

impl std::fmt::Display for TopicAdapterOwnershipLost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "topic adapter ownership lost for {}/{} at owner epoch {}",
            self.tenant_id, self.adapter_id, self.owner_epoch
        )
    }
}

impl std::error::Error for TopicAdapterOwnershipLost {}

#[derive(Debug, Deserialize, Serialize)]
struct TriggerWorkflowRequest {
    tenant_id: String,
    #[serde(default, alias = "workflow_instance_id")]
    instance_id: Option<String>,
    #[serde(default)]
    workflow_task_queue: Option<String>,
    input: Value,
    #[serde(default)]
    memo: Option<Value>,
    #[serde(default, alias = "searchAttributes")]
    search_attributes: Option<Value>,
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
    let state = AppState {
        broker: broker.clone(),
        publisher: WorkflowPublisher::new(&broker, "ingest-service").await?,
        store,
        runtime_id: env::var("INGEST_RUNTIME_ID")
            .ok()
            .filter(|value: &String| !value.is_empty())
            .unwrap_or_else(|| format!("ingest-service-{}", Uuid::now_v7())),
    };
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
    .with_state(state.clone());

    tokio::spawn(run_topic_adapter_manager(state));
    serve(app, config.port).await
}

async fn run_topic_adapter_manager(state: AppState) {
    let mut tasks: HashMap<String, (TopicAdapterTaskSpec, JoinHandle<()>)> = HashMap::new();
    loop {
        match state.store.list_topic_adapters().await {
            Ok(adapters) => {
                let mut desired = HashMap::new();
                for adapter in adapters {
                    let key = topic_adapter_key(&adapter.tenant_id, &adapter.adapter_id);
                    desired.insert(
                        key,
                        TopicAdapterTaskSpec {
                            tenant_id: adapter.tenant_id.clone(),
                            adapter_id: adapter.adapter_id.clone(),
                            updated_at: adapter.updated_at,
                            is_paused: adapter.is_paused,
                        },
                    );

                    if adapter.is_paused {
                        if let Some((_, handle)) = tasks
                            .remove(&topic_adapter_key(&adapter.tenant_id, &adapter.adapter_id))
                        {
                            handle.abort();
                        }
                        continue;
                    }

                    let key = topic_adapter_key(&adapter.tenant_id, &adapter.adapter_id);
                    let should_restart = tasks
                        .get(&key)
                        .map(|(spec, handle)| {
                            spec.updated_at != adapter.updated_at
                                || spec.is_paused
                                || handle.is_finished()
                        })
                        .unwrap_or(true);
                    if !should_restart {
                        continue;
                    }
                    if let Some((_, handle)) = tasks.remove(&key) {
                        handle.abort();
                    }
                    info!(
                        tenant_id = %adapter.tenant_id,
                        adapter_id = %adapter.adapter_id,
                        topic = %adapter.topic_name,
                        action = %adapter.action.as_str(),
                        "starting topic adapter worker"
                    );
                    let spec = TopicAdapterTaskSpec {
                        tenant_id: adapter.tenant_id.clone(),
                        adapter_id: adapter.adapter_id.clone(),
                        updated_at: adapter.updated_at,
                        is_paused: false,
                    };
                    let handle = tokio::spawn(run_topic_adapter_worker(state.clone(), adapter));
                    tasks.insert(key, (spec, handle));
                }

                let stale = tasks
                    .keys()
                    .filter(|key| !desired.contains_key(*key))
                    .cloned()
                    .collect::<Vec<_>>();
                for key in stale {
                    if let Some((spec, handle)) = tasks.remove(&key) {
                        info!(
                            tenant_id = %spec.tenant_id,
                            adapter_id = %spec.adapter_id,
                            "stopping topic adapter worker"
                        );
                        handle.abort();
                    }
                }
            }
            Err(error) => error!(error = %error, "failed to reconcile topic adapters"),
        }

        tokio::time::sleep(Duration::from_millis(TOPIC_ADAPTER_RECONCILE_INTERVAL_MS)).await;
    }
}

async fn run_topic_adapter_worker(state: AppState, adapter: TopicAdapterRecord) {
    let lease_ttl = Duration::from_millis(TOPIC_ADAPTER_OWNERSHIP_LEASE_TTL_MS);
    loop {
        let ownership = match state
            .store
            .claim_topic_adapter_ownership(
                &adapter.tenant_id,
                &adapter.adapter_id,
                &state.runtime_id,
                lease_ttl,
            )
            .await
        {
            Ok(Some(ownership)) => ownership,
            Ok(None) => {
                tokio::time::sleep(Duration::from_millis(TOPIC_ADAPTER_OWNERSHIP_RETRY_DELAY_MS))
                    .await;
                continue;
            }
            Err(error) => {
                warn!(
                    tenant_id = %adapter.tenant_id,
                    adapter_id = %adapter.adapter_id,
                    error = %error,
                    "topic adapter worker failed to claim ownership"
                );
                tokio::time::sleep(Duration::from_millis(TOPIC_ADAPTER_RESTART_DELAY_MS)).await;
                continue;
            }
        };

        info!(
            tenant_id = %adapter.tenant_id,
            adapter_id = %adapter.adapter_id,
            owner_id = %ownership.owner_id,
            owner_epoch = ownership.owner_epoch,
            lease_expires_at = %ownership.lease_expires_at,
            "topic adapter worker claimed ownership"
        );

        if let Err(error) = run_topic_adapter_consumer_pass(&state, &adapter, ownership.owner_epoch).await {
            if error
                .downcast_ref::<TopicAdapterOwnershipLost>()
                .is_some()
            {
                info!(
                    tenant_id = %adapter.tenant_id,
                    adapter_id = %adapter.adapter_id,
                    owner_id = %state.runtime_id,
                    owner_epoch = ownership.owner_epoch,
                    "topic adapter worker lost ownership"
                );
                tokio::time::sleep(Duration::from_millis(TOPIC_ADAPTER_OWNERSHIP_RETRY_DELAY_MS))
                    .await;
                continue;
            }
            warn!(
                tenant_id = %adapter.tenant_id,
                adapter_id = %adapter.adapter_id,
                error = %error,
                "topic adapter worker restarting after error"
            );
            if let Err(store_error) = state
                .store
                .mark_topic_adapter_runtime_error(
                    &adapter.tenant_id,
                    &adapter.adapter_id,
                    &error.to_string(),
                    Utc::now(),
                )
                .await
            {
                error!(
                    tenant_id = %adapter.tenant_id,
                    adapter_id = %adapter.adapter_id,
                    error = %store_error,
                    "failed to persist topic adapter runtime error"
                );
            }
            tokio::time::sleep(Duration::from_millis(TOPIC_ADAPTER_RESTART_DELAY_MS)).await;
        }
    }
}

async fn run_topic_adapter_consumer_pass(
    state: &AppState,
    adapter: &TopicAdapterRecord,
    owner_epoch: u64,
) -> Result<()> {
    let config = JsonTopicConfig::new(
        adapter.brokers.clone(),
        adapter.topic_name.clone(),
        adapter.topic_partitions,
    );
    let partitions = config.all_partition_ids();
    let offsets = state
        .store
        .load_topic_adapter_offsets(&adapter.tenant_id, &adapter.adapter_id)
        .await
        .context("failed to load topic adapter offsets")?;
    let start_offsets = offsets
        .into_iter()
        .map(|offset| (offset.partition_id, offset.log_offset.saturating_add(1)))
        .collect::<HashMap<_, _>>();
    let mut consumer = build_json_consumer_from_offsets(
        &config,
        &format!(
            "topic-adapter-{}-{}-{}",
            adapter.tenant_id, adapter.adapter_id, state.runtime_id
        ),
        &start_offsets,
        &partitions,
    )
    .await
    .context("failed to build topic adapter consumer")?;
    let mut renew_interval =
        tokio::time::interval(Duration::from_millis(TOPIC_ADAPTER_OWNERSHIP_RENEW_INTERVAL_MS));
    renew_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        let record_result = tokio::select! {
            _ = renew_interval.tick() => {
                let renewed = state
                    .store
                    .renew_topic_adapter_ownership(
                        &adapter.tenant_id,
                        &adapter.adapter_id,
                        &state.runtime_id,
                        owner_epoch,
                        Duration::from_millis(TOPIC_ADAPTER_OWNERSHIP_LEASE_TTL_MS),
                    )
                    .await
                    .context("failed to renew topic adapter ownership")?;
                if renewed.is_none() {
                    return Err(TopicAdapterOwnershipLost::new(
                        &adapter.tenant_id,
                        &adapter.adapter_id,
                        owner_epoch,
                    )
                    .into());
                }
                continue;
            }
            record_result = consumer.next() => record_result,
        };

        let Some(record_result) = record_result else {
            return Ok(());
        };
        let consumed = record_result.context("failed to read topic adapter record")?;
        let key = consumed
            .record
            .record
            .key
            .as_deref()
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned());
        let payload = match decode_json_record::<Value>(&consumed.record) {
            Ok(payload) => payload,
            Err(error) => {
                let committed = state
                    .store
                    .record_topic_adapter_dead_letter_if_owned(
                        &adapter.tenant_id,
                        &adapter.adapter_id,
                        consumed.partition_id,
                        consumed.record.offset,
                        key.as_deref(),
                        &Value::Null,
                        &format!("invalid_json: {error}"),
                        Utc::now(),
                        &state.runtime_id,
                        owner_epoch,
                    )
                    .await
                    .context("failed to store invalid json topic adapter record")?;
                if !committed {
                    if !state
                        .store
                        .validate_topic_adapter_ownership(
                            &adapter.tenant_id,
                            &adapter.adapter_id,
                            &state.runtime_id,
                            owner_epoch,
                        )
                        .await
                        .context("failed to validate topic adapter ownership after invalid json commit rejection")?
                    {
                        return Err(TopicAdapterOwnershipLost::new(
                            &adapter.tenant_id,
                            &adapter.adapter_id,
                            owner_epoch,
                        )
                        .into());
                    }
                }
                continue;
            }
        };

        match dispatch_topic_adapter_record(
            state,
            adapter,
            &payload,
            consumed.partition_id,
            consumed.record.offset,
        )
        .await
        {
            Ok(()) => {
                let committed = state
                    .store
                    .record_topic_adapter_success_if_owned(
                        &adapter.tenant_id,
                        &adapter.adapter_id,
                        consumed.partition_id,
                        consumed.record.offset,
                        Utc::now(),
                        &state.runtime_id,
                        owner_epoch,
                    )
                    .await
                    .context("failed to commit topic adapter success")?;
                if !committed {
                    if !state
                        .store
                        .validate_topic_adapter_ownership(
                            &adapter.tenant_id,
                            &adapter.adapter_id,
                            &state.runtime_id,
                            owner_epoch,
                        )
                        .await
                        .context("failed to validate topic adapter ownership after success commit rejection")?
                    {
                        return Err(TopicAdapterOwnershipLost::new(
                            &adapter.tenant_id,
                            &adapter.adapter_id,
                            owner_epoch,
                        )
                        .into());
                    }
                }
            }
            Err((status, message)) if status.is_client_error() => {
                let committed = state
                    .store
                    .record_topic_adapter_dead_letter_if_owned(
                        &adapter.tenant_id,
                        &adapter.adapter_id,
                        consumed.partition_id,
                        consumed.record.offset,
                        key.as_deref(),
                        &payload,
                        &format!("{}: {}", status.as_u16(), message),
                        Utc::now(),
                        &state.runtime_id,
                        owner_epoch,
                    )
                    .await
                    .context("failed to store topic adapter dead letter")?;
                if !committed {
                    if !state
                        .store
                        .validate_topic_adapter_ownership(
                            &adapter.tenant_id,
                            &adapter.adapter_id,
                            &state.runtime_id,
                            owner_epoch,
                        )
                        .await
                        .context("failed to validate topic adapter ownership after dead letter commit rejection")?
                    {
                        return Err(TopicAdapterOwnershipLost::new(
                            &adapter.tenant_id,
                            &adapter.adapter_id,
                            owner_epoch,
                        )
                        .into());
                    }
                }
            }
            Err((status, message)) => {
                anyhow::bail!(
                    "topic adapter dispatch failed with status {} for {}:{}:{}: {}",
                    status.as_u16(),
                    adapter.tenant_id,
                    adapter.adapter_id,
                    consumed.record.offset,
                    message
                );
            }
        }
    }

}

async fn dispatch_topic_adapter_record(
    state: &AppState,
    adapter: &TopicAdapterRecord,
    payload: &Value,
    partition_id: i32,
    log_offset: i64,
) -> Result<(), (StatusCode, String)> {
    let preview = resolve_topic_adapter_dispatch(adapter, payload, partition_id, log_offset)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    match preview.dispatch {
        TopicAdapterResolvedDispatch::StartWorkflow(request) => {
            handle_trigger_workflow(
                state,
                &request.definition_id,
                TriggerWorkflowRequest {
                    tenant_id: adapter.tenant_id.clone(),
                    instance_id: request.instance_id,
                    workflow_task_queue: request.workflow_task_queue,
                    input: request.input,
                    memo: request.memo,
                    search_attributes: request.search_attributes,
                    request_id: Some(request.request_id),
                },
            )
            .await
            .map(|_| ())
        }
        TopicAdapterResolvedDispatch::SignalWorkflow(request) => {
            handle_signal_workflow(
                state,
                &adapter.tenant_id,
                &request.instance_id,
                &request.signal_type,
                SignalWorkflowRequest {
                    payload: request.payload,
                    dedupe_key: request.dedupe_key,
                    request_id: Some(request.request_id),
                },
            )
            .await
            .map(|_| ())
        }
    }
}

fn topic_adapter_key(tenant_id: &str, adapter_id: &str) -> String {
    format!("{tenant_id}:{adapter_id}")
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
    let response = handle_trigger_workflow(&state, &definition_id, request).await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

async fn signal_workflow(
    Path((tenant_id, instance_id, signal_type)): Path<(String, String, String)>,
    State(state): State<AppState>,
    Json(request): Json<SignalWorkflowRequest>,
) -> Result<(StatusCode, Json<SignalWorkflowResponse>), (StatusCode, String)> {
    let response =
        handle_signal_workflow(&state, &tenant_id, &instance_id, &signal_type, request).await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

async fn handle_trigger_workflow(
    state: &AppState,
    definition_id: &str,
    request: TriggerWorkflowRequest,
) -> Result<TriggerWorkflowResponse, (StatusCode, String)> {
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
            return Ok(response);
        }
    }
    let task_queue_hint = request.workflow_task_queue.as_deref();
    let queue_default_artifact = if let Some(task_queue) = task_queue_hint {
        state
            .store
            .get_default_workflow_artifact_for_queue(&request.tenant_id, task_queue, &definition_id)
            .await
            .map_err(internal_error)?
    } else {
        None
    };
    let (resolved_definition_id, definition_version, artifact_hash) =
        if let Some(artifact) = queue_default_artifact {
            (artifact.definition_id, artifact.definition_version, artifact.artifact_hash)
        } else if let Some(artifact) = state
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
    let workflow_task_queue = match request.workflow_task_queue.clone() {
        Some(task_queue) => task_queue,
        None => state
            .store
            .infer_workflow_task_queue_for_artifact(&request.tenant_id, &artifact_hash)
            .await
            .map_err(internal_error)?
            .unwrap_or_else(|| "default".to_owned()),
    };
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
    if let Some(memo) = request.memo.as_ref() {
        envelope.metadata.insert("memo_json".to_owned(), memo.to_string());
    }
    if let Some(search_attributes) = request.search_attributes.as_ref() {
        envelope
            .metadata
            .insert("search_attributes_json".to_owned(), search_attributes.to_string());
    }

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
            request.memo.as_ref(),
            request.search_attributes.as_ref(),
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
    Ok(response)
}

async fn handle_signal_workflow(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    signal_type: &str,
    request: SignalWorkflowRequest,
) -> Result<SignalWorkflowResponse, (StatusCode, String)> {
    if let Some(request_id) = request.request_id.as_deref() {
        if let Some(response) = load_dedup_response::<SignalWorkflowResponse, _>(
            &state.store,
            tenant_id,
            Some(instance_id),
            &format!("signal:{signal_type}"),
            request_id,
            &request,
        )
        .await?
        {
            return Ok(response);
        }
    }
    let instance = state
        .store
        .get_instance(tenant_id, instance_id)
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
        signal_type: signal_type.to_owned(),
        payload: request.payload.clone(),
    };
    let envelope = EventEnvelope::new(
        payload.event_type(),
        WorkflowIdentity::new(
            tenant_id.to_owned(),
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
            instance_id: instance_id.to_owned(),
            run_id: instance.run_id,
            signal_id,
            signal_type: signal_type.to_owned(),
            event_id: envelope.event_id,
            status: "duplicate".to_owned(),
        };
        return Ok(response);
    }

    if let Err(error) = state.publisher.publish(&envelope, &envelope.partition_key).await {
        let _ = state
            .store
            .delete_signal(&envelope.tenant_id, &envelope.instance_id, &envelope.run_id, &signal_id)
            .await;
        return Err(internal_error(error));
    }
    let response = SignalWorkflowResponse {
        instance_id: instance_id.to_owned(),
        run_id: instance.run_id,
        signal_id,
        signal_type: signal_type.to_owned(),
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
    Ok(response)
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
    if let Some(memo) = instance.memo.as_ref() {
        trigger.metadata.insert("memo_json".to_owned(), memo.to_string());
    }
    if let Some(search_attributes) = instance.search_attributes.as_ref() {
        trigger.metadata.insert("search_attributes_json".to_owned(), search_attributes.to_string());
    }

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
            instance.memo.as_ref(),
            instance.search_attributes.as_ref(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use fabrik_broker::{JsonTopicPublisher, WorkflowHistoryFilter, read_workflow_history};
    use fabrik_workflow::{StateNode, WorkflowInstanceState, WorkflowStatus};
    use serde_json::json;
    use std::{
        collections::BTreeMap,
        process::Command,
        time::{Duration as StdDuration, Instant},
    };
    use tokio::time::sleep;

    struct TestPostgres {
        container_name: String,
        database_url: String,
    }

    impl TestPostgres {
        fn start() -> Result<Option<Self>> {
            if !docker_available() {
                eprintln!(
                    "skipping ingest-service integration tests because docker is unavailable"
                );
                return Ok(None);
            }

            let container_name = format!("fabrik-ingest-test-pg-{}", Uuid::now_v7());
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

            let host_port = wait_for_host_port(&container_name, "5432/tcp")?;
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

    struct TestRedpanda {
        container_name: String,
        broker: BrokerConfig,
    }

    impl TestRedpanda {
        fn start() -> Result<Option<Self>> {
            if !docker_available() {
                eprintln!(
                    "skipping ingest-service integration tests because docker is unavailable"
                );
                return Ok(None);
            }

            let kafka_port = choose_free_port().context("failed to allocate kafka host port")?;
            let container_name = format!("fabrik-ingest-test-rp-{}", Uuid::now_v7());
            let image = std::env::var("FABRIK_TEST_REDPANDA_IMAGE")
                .unwrap_or_else(|_| "docker.redpanda.com/redpandadata/redpanda:v25.1.2".to_owned());
            let workflow_topic = format!("workflow-events-test-{}", Uuid::now_v7());
            let output = Command::new("docker")
                .args([
                    "run",
                    "--detach",
                    "--rm",
                    "--name",
                    &container_name,
                    "--publish",
                    &format!("{kafka_port}:{kafka_port}"),
                    &image,
                    "redpanda",
                    "start",
                    "--overprovisioned",
                    "--smp",
                    "1",
                    "--memory",
                    "1G",
                    "--reserve-memory",
                    "0M",
                    "--node-id",
                    "0",
                    "--check=false",
                    "--kafka-addr",
                    &format!("PLAINTEXT://0.0.0.0:9092,OUTSIDE://0.0.0.0:{kafka_port}"),
                    "--advertise-kafka-addr",
                    &format!("PLAINTEXT://127.0.0.1:9092,OUTSIDE://127.0.0.1:{kafka_port}"),
                    "--rpc-addr",
                    "0.0.0.0:33145",
                    "--advertise-rpc-addr",
                    "127.0.0.1:33145",
                ])
                .output()
                .with_context(|| format!("failed to start docker container {container_name}"))?;
            if !output.status.success() {
                anyhow::bail!(
                    "docker failed to start redpanda test container: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }

            Ok(Some(Self {
                container_name,
                broker: BrokerConfig::new(format!("127.0.0.1:{kafka_port}"), workflow_topic, 1),
            }))
        }

        async fn connect_workflow_publisher(&self) -> Result<WorkflowPublisher> {
            let deadline = Instant::now() + StdDuration::from_secs(45);
            loop {
                match WorkflowPublisher::new(&self.broker, "ingest-service-test").await {
                    Ok(publisher) => return Ok(publisher),
                    Err(error) if Instant::now() < deadline => {
                        let _ = error;
                        sleep(StdDuration::from_millis(500)).await;
                    }
                    Err(error) => {
                        let logs = docker_logs(&self.container_name).unwrap_or_default();
                        return Err(error).with_context(|| {
                            format!(
                                "redpanda test container {} did not become ready; logs:\n{}",
                                self.container_name, logs
                            )
                        });
                    }
                }
            }
        }

        async fn connect_json_publisher(
            &self,
            topic_name: &str,
            partitions: i32,
        ) -> Result<JsonTopicPublisher<Value>> {
            let config = JsonTopicConfig::new(
                self.broker.brokers.clone(),
                topic_name.to_owned(),
                partitions,
            );
            let deadline = Instant::now() + StdDuration::from_secs(45);
            loop {
                match JsonTopicPublisher::<Value>::new(&config, "ingest-service-test").await {
                    Ok(publisher) => return Ok(publisher),
                    Err(error) if Instant::now() < deadline => {
                        let _ = error;
                        sleep(StdDuration::from_millis(500)).await;
                    }
                    Err(error) => {
                        let logs = docker_logs(&self.container_name).unwrap_or_default();
                        return Err(error).with_context(|| {
                            format!(
                                "redpanda test container {} did not become ready for json topic {}; logs:\n{}",
                                self.container_name, topic_name, logs
                            )
                        });
                    }
                }
            }
        }
    }

    impl Drop for TestRedpanda {
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

    fn choose_free_port() -> Result<u16> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .context("failed to bind ephemeral tcp listener")?;
        let port =
            listener.local_addr().context("failed to inspect ephemeral tcp listener")?.port();
        drop(listener);
        Ok(port)
    }

    fn wait_for_host_port(container_name: &str, container_port: &str) -> Result<u16> {
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

    async fn test_state(store: WorkflowStore, redpanda: &TestRedpanda) -> Result<AppState> {
        test_state_with_runtime_id(store, redpanda, &format!("ingest-test-{}", Uuid::now_v7()))
            .await
    }

    async fn test_state_with_runtime_id(
        store: WorkflowStore,
        redpanda: &TestRedpanda,
        runtime_id: &str,
    ) -> Result<AppState> {
        Ok(AppState {
            broker: redpanda.broker.clone(),
            publisher: redpanda.connect_workflow_publisher().await?,
            store,
            runtime_id: runtime_id.to_owned(),
        })
    }

    async fn claim_adapter_for_state(
        store: &WorkflowStore,
        state: &AppState,
        tenant_id: &str,
        adapter_id: &str,
    ) -> Result<u64> {
        let ownership = store
            .claim_topic_adapter_ownership(
                tenant_id,
                adapter_id,
                &state.runtime_id,
                StdDuration::from_millis(TOPIC_ADAPTER_OWNERSHIP_LEASE_TTL_MS),
            )
            .await?
            .context("expected topic adapter ownership claim")?;
        Ok(ownership.owner_epoch)
    }

    fn simple_definition(definition_id: &str) -> WorkflowDefinition {
        WorkflowDefinition {
            id: definition_id.to_owned(),
            version: 1,
            initial_state: "done".to_owned(),
            states: BTreeMap::from([("done".to_owned(), StateNode::Succeed)]),
        }
    }

    async fn wait_for_adapter_counts(
        store: &WorkflowStore,
        tenant_id: &str,
        adapter_id: &str,
        processed: u64,
        failed: u64,
    ) -> Result<fabrik_store::TopicAdapterRecord> {
        let deadline = Instant::now() + StdDuration::from_secs(15);
        loop {
            if let Some(adapter) = store.get_topic_adapter(tenant_id, adapter_id).await? {
                if adapter.processed_count == processed && adapter.failed_count == failed {
                    return Ok(adapter);
                }
            }
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "timed out waiting for topic adapter {tenant_id}/{adapter_id} to reach processed={processed} failed={failed}"
                );
            }
            sleep(StdDuration::from_millis(100)).await;
        }
    }

    async fn wait_for_adapter_owner(
        store: &WorkflowStore,
        tenant_id: &str,
        adapter_id: &str,
        owner_id: &str,
    ) -> Result<fabrik_store::TopicAdapterOwnershipRecord> {
        let deadline = Instant::now() + StdDuration::from_secs(20);
        loop {
            if let Some(ownership) = store.get_topic_adapter_ownership(tenant_id, adapter_id).await? {
                if ownership.owner_id == owner_id {
                    return Ok(ownership);
                }
            }
            if Instant::now() >= deadline {
                anyhow::bail!(
                    "timed out waiting for topic adapter {tenant_id}/{adapter_id} owner {owner_id}"
                );
            }
            sleep(StdDuration::from_millis(100)).await;
        }
    }

    #[tokio::test]
    async fn topic_adapter_starts_workflow_and_commits_offset_once() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state = test_state(store.clone(), &redpanda).await?;

        let definition = simple_definition("order-workflow");
        store.put_definition("tenant-a", &definition).await?;

        let adapter = store
            .upsert_topic_adapter(&fabrik_store::TopicAdapterUpsert {
                tenant_id: "tenant-a".to_owned(),
                adapter_id: "orders".to_owned(),
                adapter_kind: fabrik_store::TopicAdapterKind::Redpanda,
                brokers: redpanda.broker.brokers.clone(),
                topic_name: "orders.events".to_owned(),
                topic_partitions: 1,
                action: fabrik_store::TopicAdapterAction::StartWorkflow,
                definition_id: Some("order-workflow".to_owned()),
                signal_type: None,
                workflow_task_queue: Some("orders".to_owned()),
                workflow_instance_id_json_pointer: Some("/instance_id".to_owned()),
                payload_json_pointer: Some("/payload".to_owned()),
                payload_template_json: None,
                memo_json_pointer: Some("/memo".to_owned()),
                memo_template_json: None,
                search_attributes_json_pointer: Some("/search".to_owned()),
                search_attributes_template_json: None,
                request_id_json_pointer: Some("/request_id".to_owned()),
                dedupe_key_json_pointer: None,
                dead_letter_policy: fabrik_store::TopicAdapterDeadLetterPolicy::Store,
                is_paused: false,
            })
            .await?;

        let worker_state = state.clone();
        let worker_adapter = adapter.clone();
        let owner_epoch = claim_adapter_for_state(&store, &worker_state, "tenant-a", "orders").await?;
        let worker = tokio::spawn(async move {
            let _ = run_topic_adapter_consumer_pass(&worker_state, &worker_adapter, owner_epoch).await;
        });

        let publisher = redpanda.connect_json_publisher("orders.events", 1).await?;
        publisher
            .publish(
                &json!({
                    "instance_id": "order-1",
                    "request_id": "adapter-start-1",
                    "payload": { "order_id": "o-1", "amount": 42 },
                    "memo": { "source": "topic-adapter" },
                    "search": { "order_id": "o-1" }
                }),
                "order-1",
            )
            .await?;

        let adapter_state = wait_for_adapter_counts(&store, "tenant-a", "orders", 1, 0).await?;
        assert_eq!(adapter_state.last_error, None);

        let runs = store
            .list_runs_page_with_filters(
                "tenant-a",
                &WorkflowRunFilters {
                    instance_id: Some("order-1".to_owned()),
                    ..WorkflowRunFilters::default()
                },
                10,
                0,
            )
            .await?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].workflow_task_queue, "orders");

        let history = read_workflow_history(
            &redpanda.broker,
            "ingest-service-test-history",
            &WorkflowHistoryFilter::new("tenant-a", "order-1", &runs[0].run_id),
            Duration::from_millis(250),
            Duration::from_millis(3_000),
        )
        .await?;
        assert!(history.iter().any(|event| event.event_type == "WorkflowTriggered"));

        worker.abort();
        let worker_state = state.clone();
        let worker_adapter = adapter.clone();
        let owner_epoch = claim_adapter_for_state(&store, &worker_state, "tenant-a", "orders").await?;
        let replay_worker = tokio::spawn(async move {
            let _ = run_topic_adapter_consumer_pass(&worker_state, &worker_adapter, owner_epoch).await;
        });
        sleep(StdDuration::from_millis(750)).await;
        replay_worker.abort();

        let adapter_state = store
            .get_topic_adapter("tenant-a", "orders")
            .await?
            .context("adapter missing after replay worker")?;
        assert_eq!(adapter_state.processed_count, 1);
        assert_eq!(store.load_topic_adapter_offsets("tenant-a", "orders").await?[0].log_offset, 0);

        Ok(())
    }

    #[tokio::test]
    async fn topic_adapter_queues_signal_and_publishes_signal_event() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state = test_state(store.clone(), &redpanda).await?;

        let definition = simple_definition("approval-workflow");
        let artifact_hash = artifact_hash(&definition);
        let now = Utc::now();
        store
            .put_run_start(
                "tenant-a",
                "approval-1",
                "run-1",
                "approval-workflow",
                Some(1),
                Some(&artifact_hash),
                "approvals",
                None,
                None,
                Uuid::now_v7(),
                now,
                None,
                None,
            )
            .await?;
        store
            .upsert_instance(&WorkflowInstanceState {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "approval-1".to_owned(),
                run_id: "run-1".to_owned(),
                definition_id: "approval-workflow".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some(artifact_hash.clone()),
                workflow_task_queue: "approvals".to_owned(),
                sticky_workflow_build_id: None,
                sticky_workflow_poller_id: None,
                current_state: Some("awaiting_approval".to_owned()),
                context: Some(json!({"status": "waiting"})),
                artifact_execution: None,
                status: WorkflowStatus::Running,
                input: Some(json!({"order_id": "o-2"})),
                persisted_input_handle: None,
                memo: None,
                search_attributes: None,
                output: None,
                event_count: 1,
                last_event_id: Uuid::now_v7(),
                last_event_type: "WorkflowStarted".to_owned(),
                updated_at: now,
            })
            .await?;

        let adapter = store
            .upsert_topic_adapter(&fabrik_store::TopicAdapterUpsert {
                tenant_id: "tenant-a".to_owned(),
                adapter_id: "approvals".to_owned(),
                adapter_kind: fabrik_store::TopicAdapterKind::Redpanda,
                brokers: redpanda.broker.brokers.clone(),
                topic_name: "approvals.events".to_owned(),
                topic_partitions: 1,
                action: fabrik_store::TopicAdapterAction::SignalWorkflow,
                definition_id: None,
                signal_type: Some("external.approved".to_owned()),
                workflow_task_queue: None,
                workflow_instance_id_json_pointer: Some("/instance_id".to_owned()),
                payload_json_pointer: Some("/payload".to_owned()),
                payload_template_json: None,
                memo_json_pointer: None,
                memo_template_json: None,
                search_attributes_json_pointer: None,
                search_attributes_template_json: None,
                request_id_json_pointer: Some("/request_id".to_owned()),
                dedupe_key_json_pointer: Some("/dedupe_key".to_owned()),
                dead_letter_policy: fabrik_store::TopicAdapterDeadLetterPolicy::Store,
                is_paused: false,
            })
            .await?;

        let worker_state = state.clone();
        let worker_adapter = adapter.clone();
        let owner_epoch =
            claim_adapter_for_state(&store, &worker_state, "tenant-a", "approvals").await?;
        let worker = tokio::spawn(async move {
            let _ = run_topic_adapter_consumer_pass(&worker_state, &worker_adapter, owner_epoch).await;
        });

        let publisher = redpanda.connect_json_publisher("approvals.events", 1).await?;
        publisher
            .publish(
                &json!({
                    "instance_id": "approval-1",
                    "request_id": "adapter-signal-1",
                    "dedupe_key": "approval-1-approved",
                    "payload": { "approved_by": "ops" }
                }),
                "approval-1",
            )
            .await?;

        wait_for_adapter_counts(&store, "tenant-a", "approvals", 1, 0).await?;
        let signals = store.list_signals_for_run("tenant-a", "approval-1", "run-1").await?;
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].signal_type, "external.approved");

        let history = read_workflow_history(
            &redpanda.broker,
            "ingest-service-test-history-signal",
            &WorkflowHistoryFilter::new("tenant-a", "approval-1", "run-1"),
            Duration::from_millis(250),
            Duration::from_millis(3_000),
        )
        .await?;
        assert!(history.iter().any(|event| event.event_type == "SignalQueued"));

        worker.abort();
        Ok(())
    }

    #[tokio::test]
    async fn topic_adapter_dead_letters_invalid_record_and_advances_offset() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state = test_state(store.clone(), &redpanda).await?;

        let definition = simple_definition("order-workflow");
        store.put_definition("tenant-a", &definition).await?;

        let adapter = store
            .upsert_topic_adapter(&fabrik_store::TopicAdapterUpsert {
                tenant_id: "tenant-a".to_owned(),
                adapter_id: "orders-invalid".to_owned(),
                adapter_kind: fabrik_store::TopicAdapterKind::Redpanda,
                brokers: redpanda.broker.brokers.clone(),
                topic_name: "orders.invalid".to_owned(),
                topic_partitions: 1,
                action: fabrik_store::TopicAdapterAction::StartWorkflow,
                definition_id: Some("order-workflow".to_owned()),
                signal_type: None,
                workflow_task_queue: Some("orders".to_owned()),
                workflow_instance_id_json_pointer: Some("/instance_id".to_owned()),
                payload_json_pointer: Some("/payload".to_owned()),
                payload_template_json: None,
                memo_json_pointer: None,
                memo_template_json: None,
                search_attributes_json_pointer: None,
                search_attributes_template_json: None,
                request_id_json_pointer: Some("/request_id".to_owned()),
                dedupe_key_json_pointer: None,
                dead_letter_policy: fabrik_store::TopicAdapterDeadLetterPolicy::Store,
                is_paused: false,
            })
            .await?;

        let worker_state = state.clone();
        let worker_adapter = adapter.clone();
        let owner_epoch =
            claim_adapter_for_state(&store, &worker_state, "tenant-a", "orders-invalid").await?;
        let worker = tokio::spawn(async move {
            let _ = run_topic_adapter_consumer_pass(&worker_state, &worker_adapter, owner_epoch).await;
        });

        let publisher = redpanda.connect_json_publisher("orders.invalid", 1).await?;
        publisher
            .publish(
                &json!({
                    "instance_id": "order-bad-1",
                    "request_id": "adapter-invalid-1"
                }),
                "order-bad-1",
            )
            .await?;

        let adapter_state =
            wait_for_adapter_counts(&store, "tenant-a", "orders-invalid", 0, 1).await?;
        assert_eq!(
            adapter_state.last_error.as_deref(),
            Some("400: missing required field input at json pointer /payload")
        );
        let dead_letters =
            store.list_topic_adapter_dead_letters("tenant-a", "orders-invalid", 10, 0).await?;
        assert_eq!(dead_letters.len(), 1);
        assert_eq!(dead_letters[0].log_offset, 0);
        assert_eq!(
            store.load_topic_adapter_offsets("tenant-a", "orders-invalid").await?[0].log_offset,
            0
        );

        worker.abort();
        Ok(())
    }

    #[tokio::test]
    async fn topic_adapter_multi_replica_workers_are_fenced_by_adapter_ownership() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_a = test_state_with_runtime_id(store.clone(), &redpanda, "ingest-a").await?;
        let state_b = test_state_with_runtime_id(store.clone(), &redpanda, "ingest-b").await?;

        let definition = simple_definition("order-workflow");
        store.put_definition("tenant-a", &definition).await?;

        let adapter = store
            .upsert_topic_adapter(&fabrik_store::TopicAdapterUpsert {
                tenant_id: "tenant-a".to_owned(),
                adapter_id: "orders-fenced".to_owned(),
                adapter_kind: fabrik_store::TopicAdapterKind::Redpanda,
                brokers: redpanda.broker.brokers.clone(),
                topic_name: "orders.fenced".to_owned(),
                topic_partitions: 1,
                action: fabrik_store::TopicAdapterAction::StartWorkflow,
                definition_id: Some("order-workflow".to_owned()),
                signal_type: None,
                workflow_task_queue: Some("orders".to_owned()),
                workflow_instance_id_json_pointer: Some("/instance_id".to_owned()),
                payload_json_pointer: Some("/payload".to_owned()),
                payload_template_json: None,
                memo_json_pointer: None,
                memo_template_json: None,
                search_attributes_json_pointer: None,
                search_attributes_template_json: None,
                request_id_json_pointer: Some("/request_id".to_owned()),
                dedupe_key_json_pointer: None,
                dead_letter_policy: fabrik_store::TopicAdapterDeadLetterPolicy::Store,
                is_paused: false,
            })
            .await?;

        let worker_a = tokio::spawn(run_topic_adapter_worker(state_a.clone(), adapter.clone()));
        let worker_b = tokio::spawn(run_topic_adapter_worker(state_b.clone(), adapter.clone()));

        let publisher = redpanda.connect_json_publisher("orders.fenced", 1).await?;
        publisher
            .publish(
                &json!({
                    "instance_id": "order-fenced-1",
                    "request_id": "adapter-fenced-1",
                    "payload": { "order_id": "o-fenced-1" }
                }),
                "order-fenced-1",
            )
            .await?;

        let adapter_state = wait_for_adapter_counts(&store, "tenant-a", "orders-fenced", 1, 0).await?;
        assert_eq!(adapter_state.processed_count, 1);

        sleep(StdDuration::from_millis(500)).await;
        let ownership = store
            .get_topic_adapter_ownership("tenant-a", "orders-fenced")
            .await?
            .context("adapter ownership missing after worker start")?;
        assert!(matches!(ownership.owner_id.as_str(), "ingest-a" | "ingest-b"));
        assert!(store
            .validate_topic_adapter_ownership(
                "tenant-a",
                "orders-fenced",
                &ownership.owner_id,
                ownership.owner_epoch,
            )
            .await?);

        let runs = store
            .list_runs_page_with_filters(
                "tenant-a",
                &WorkflowRunFilters {
                    instance_id: Some("order-fenced-1".to_owned()),
                    ..WorkflowRunFilters::default()
                },
                10,
                0,
            )
            .await?;
        assert_eq!(runs.len(), 1);
        let offsets = store.load_topic_adapter_offsets("tenant-a", "orders-fenced").await?;
        assert_eq!(offsets.len(), 1);
        assert_eq!(offsets[0].log_offset, 0);

        worker_a.abort();
        worker_b.abort();
        Ok(())
    }

    #[tokio::test]
    async fn topic_adapter_failover_handoffs_after_owner_crash_without_duplicate_processing()
    -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_a = test_state_with_runtime_id(store.clone(), &redpanda, "ingest-a").await?;
        let state_b = test_state_with_runtime_id(store.clone(), &redpanda, "ingest-b").await?;

        let definition = simple_definition("order-workflow");
        store.put_definition("tenant-a", &definition).await?;

        let adapter = store
            .upsert_topic_adapter(&fabrik_store::TopicAdapterUpsert {
                tenant_id: "tenant-a".to_owned(),
                adapter_id: "orders-failover".to_owned(),
                adapter_kind: fabrik_store::TopicAdapterKind::Redpanda,
                brokers: redpanda.broker.brokers.clone(),
                topic_name: "orders.failover".to_owned(),
                topic_partitions: 1,
                action: fabrik_store::TopicAdapterAction::StartWorkflow,
                definition_id: Some("order-workflow".to_owned()),
                signal_type: None,
                workflow_task_queue: Some("orders".to_owned()),
                workflow_instance_id_json_pointer: Some("/instance_id".to_owned()),
                payload_json_pointer: Some("/payload".to_owned()),
                payload_template_json: None,
                memo_json_pointer: None,
                memo_template_json: None,
                search_attributes_json_pointer: None,
                search_attributes_template_json: None,
                request_id_json_pointer: Some("/request_id".to_owned()),
                dedupe_key_json_pointer: None,
                dead_letter_policy: fabrik_store::TopicAdapterDeadLetterPolicy::Store,
                is_paused: false,
            })
            .await?;

        let worker_a = tokio::spawn(run_topic_adapter_worker(state_a.clone(), adapter.clone()));
        let worker_b = tokio::spawn(run_topic_adapter_worker(state_b.clone(), adapter.clone()));

        let publisher = redpanda.connect_json_publisher("orders.failover", 1).await?;
        publisher
            .publish(
                &json!({
                    "instance_id": "order-failover-1",
                    "request_id": "adapter-failover-1",
                    "payload": { "order_id": "o-failover-1" }
                }),
                "order-failover-1",
            )
            .await?;

        wait_for_adapter_counts(&store, "tenant-a", "orders-failover", 1, 0).await?;
        let initial_ownership = store
            .get_topic_adapter_ownership("tenant-a", "orders-failover")
            .await?
            .context("adapter ownership missing after first record")?;
        let crashed_owner = initial_ownership.owner_id.clone();
        let standby_owner = if crashed_owner == "ingest-a" { "ingest-b" } else { "ingest-a" };
        let expected_epoch = initial_ownership.owner_epoch + 1;

        if crashed_owner == "ingest-a" {
            worker_a.abort();
        } else {
            worker_b.abort();
        }

        let takeover_start = Instant::now();
        let takeover = wait_for_adapter_owner(&store, "tenant-a", "orders-failover", standby_owner).await?;
        let takeover_duration = takeover_start.elapsed();
        assert_eq!(takeover.owner_epoch, expected_epoch);
        assert!(
            takeover_duration < StdDuration::from_secs(15),
            "takeover took too long: {:?}",
            takeover_duration
        );

        publisher
            .publish(
                &json!({
                    "instance_id": "order-failover-2",
                    "request_id": "adapter-failover-2",
                    "payload": { "order_id": "o-failover-2" }
                }),
                "order-failover-2",
            )
            .await?;

        let adapter_state = wait_for_adapter_counts(&store, "tenant-a", "orders-failover", 2, 0).await?;
        assert_eq!(adapter_state.failed_count, 0);

        let runs = store
            .list_runs_page_with_filters(
                "tenant-a",
                &WorkflowRunFilters {
                    definition_id: Some("order-workflow".to_owned()),
                    ..WorkflowRunFilters::default()
                },
                10,
                0,
            )
            .await?;
        let failover_runs = runs
            .into_iter()
            .filter(|run| run.instance_id == "order-failover-1" || run.instance_id == "order-failover-2")
            .collect::<Vec<_>>();
        assert_eq!(failover_runs.len(), 2);

        let offsets = store.load_topic_adapter_offsets("tenant-a", "orders-failover").await?;
        assert_eq!(offsets.len(), 1);
        assert_eq!(offsets[0].log_offset, 1);

        if standby_owner == "ingest-a" {
            worker_a.abort();
        } else {
            worker_b.abort();
        }

        Ok(())
    }
}
