mod local_state;

use std::{
    fs,
    io::Write,
    process::{Command, Stdio},
    collections::{BTreeSet, HashMap, HashSet},
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use anyhow::{Context, Result};
use axum::{
    Json,
    extract::{Path, Query, State as AxumState},
    http::StatusCode,
    routing::get,
};
use chrono::Utc;
use fabrik_broker::{
    BrokerConfig, JsonTopicConfig, JsonTopicPublisher, WorkflowPublisher, build_json_consumer,
    build_json_consumer_from_offsets, build_workflow_consumer, decode_consumed_workflow_event,
    decode_json_record, partition_for_key,
};
use fabrik_config::{
    GrpcServiceConfig, HttpServiceConfig, PostgresConfig, RedpandaConfig,
    ThroughputOwnershipConfig, ThroughputRuntimeConfig,
};
use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::{
    PartitionOwnershipRecord, TaskQueueKind, ThroughputProjectionBatchStateUpdate,
    ThroughputProjectionChunkStateUpdate, ThroughputProjectionEvent, ThroughputResultSegmentRecord,
    ThroughputTerminalRecord, WorkflowBulkBatchRecord, WorkflowBulkBatchStatus,
    WorkflowBulkChunkRecord, WorkflowBulkChunkStatus, WorkflowStore,
};
use fabrik_throughput::{
    ADMISSION_POLICY_VERSION, ActivityCapabilityRegistry, ActivityExecutionCapabilities,
    BENCHMARK_ECHO_ACTIVITY, CORE_ACCEPT_ACTIVITY, CORE_ECHO_ACTIVITY, CORE_NOOP_ACTIVITY,
    CollectResultsBatchManifest, CollectResultsChunkSegmentRef, PayloadHandle, PayloadStore,
    PayloadStoreConfig, PayloadStoreKind, StartThroughputRunCommand, ThroughputBackend,
    ThroughputBatchIdentity, ThroughputChangelogEntry, ThroughputChangelogPayload,
    ThroughputChunkReport, ThroughputChunkReportPayload, ThroughputCommand,
    ThroughputCommandEnvelope, WorkerActivityManifest, benchmark_echo_item_requires_output, bulk_reducer_class,
    bulk_reducer_name, bulk_reducer_settles, bulk_reducer_summary_field_name,
    can_complete_payloadless_bulk_chunk, can_use_payloadless_bulk_transport, decode_cbor,
    effective_aggregation_group_count, encode_cbor, execute_benchmark_echo,
    group_id_for_chunk_index, parse_benchmark_compact_input_meta_from_handle,
    load_activity_capability_registry_from_env,
    parse_benchmark_compact_total_items_from_handle, planned_reduction_tree_depth,
    resolve_activity_capabilities,
    stream_v2_fast_lane_enabled, synthesize_benchmark_echo_items, throughput_execution_mode,
    throughput_partition_key, throughput_reducer_execution_path, throughput_reducer_version,
    throughput_routing_reason,
};
use fabrik_worker_protocol::activity_worker::{
    Ack, BulkActivityTask, BulkActivityTaskResult, PollBulkActivityTaskRequest,
    PollBulkActivityTaskResponse, ReportBulkActivityTaskResultsRequest,
    activity_worker_api_server::{ActivityWorkerApi, ActivityWorkerApiServer},
};
use fabrik_workflow::execute_handler;
use futures_util::StreamExt;
use local_state::{
    LeasedChunkSnapshot, LocalThroughputDebugSnapshot, LocalThroughputState, PreparedReportApply,
    ProjectedBatchGroupSummary, ProjectedBatchTerminal, ProjectedGroupTerminal,
    ProjectedTerminalApply, ReportValidation,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tonic::{Request, Response, Status, transport::Server};
use tracing::{error, info, warn};
use uuid::Uuid;

const BULK_EVENT_OUTBOX_BATCH_SIZE: usize = 128;
const BULK_EVENT_OUTBOX_LEASE_SECONDS: i64 = 30;
const BULK_EVENT_OUTBOX_RETRY_MS: i64 = 250;
const BRIDGE_EVENT_CONCURRENCY: usize = 64;
const LOCAL_BENCHMARK_FASTLANE_WORKER_ID: &str = "throughput-runtime-local-fastlane";
const LOCAL_BENCHMARK_FASTLANE_WORKER_BUILD_ID: &str = "throughput-runtime-local-fastlane";
const LOCAL_BENCHMARK_FASTLANE_BATCH_SIZE: usize = 256;

#[derive(Debug, Clone, Deserialize)]
struct WorkerPackageFastlaneManifest {
    task_queue: Option<String>,
    bootstrap_path: Option<String>,
    activity_manifest_path: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct LocalActivityExecutorRegistry {
    by_task_queue: HashMap<String, HashMap<String, String>>,
}

impl LocalActivityExecutorRegistry {
    fn lookup(&self, task_queue: &str, activity_type: &str) -> Option<&str> {
        self.by_task_queue
            .get(task_queue)
            .and_then(|activities| activities.get(activity_type))
            .or_else(|| self.by_task_queue.get("").and_then(|activities| activities.get(activity_type)))
            .map(String::as_str)
    }

    fn insert(&mut self, task_queue: Option<&str>, activity_type: &str, bootstrap_path: &str) {
        self.by_task_queue
            .entry(task_queue.unwrap_or_default().to_owned())
            .or_default()
            .insert(activity_type.to_owned(), bootstrap_path.to_owned());
    }
}

fn load_local_activity_executor_registry_from_env(
    env_var: &str,
) -> Result<LocalActivityExecutorRegistry> {
    let workers_dir = std::env::var(env_var).unwrap_or_default();
    if workers_dir.trim().is_empty() {
        return Ok(LocalActivityExecutorRegistry::default());
    }
    let workers_dir = std::path::PathBuf::from(workers_dir);
    if !workers_dir.exists() {
        return Ok(LocalActivityExecutorRegistry::default());
    }
    let mut registry = LocalActivityExecutorRegistry::default();
    for entry in fs::read_dir(&workers_dir)
        .with_context(|| format!("failed to read worker packages dir {}", workers_dir.display()))?
    {
        let entry = entry?;
        let package_path = entry.path().join("worker-package.json");
        if !package_path.exists() {
            continue;
        }
        let package: WorkerPackageFastlaneManifest = serde_json::from_slice(
            &fs::read(&package_path)
                .with_context(|| format!("failed to read {}", package_path.display()))?,
        )
        .with_context(|| format!("failed to parse {}", package_path.display()))?;
        let Some(bootstrap_path) = package.bootstrap_path.as_deref() else {
            continue;
        };
        let Some(activity_manifest_path) = package.activity_manifest_path.as_deref() else {
            continue;
        };
        let activity_manifest_path = std::path::PathBuf::from(activity_manifest_path);
        let manifest: WorkerActivityManifest = serde_json::from_slice(
            &fs::read(&activity_manifest_path)
                .with_context(|| format!("failed to read {}", activity_manifest_path.display()))?,
        )
        .with_context(|| format!("failed to parse {}", activity_manifest_path.display()))?;
        let task_queue = manifest.task_queue.as_deref().or(package.task_queue.as_deref());
        for activity in manifest.activities {
            registry.insert(task_queue, &activity.activity_type, bootstrap_path);
        }
    }
    Ok(registry)
}

#[derive(Clone)]
struct AppState {
    store: WorkflowStore,
    local_state: LocalThroughputState,
    workflow_publisher: WorkflowPublisher,
    changelog_publisher: JsonTopicPublisher<ThroughputChangelogEntry>,
    projection_publisher: JsonTopicPublisher<ThroughputProjectionEvent>,
    payload_store: PayloadStore,
    bulk_notify: Arc<Notify>,
    outbox_notify: Arc<Notify>,
    runtime: ThroughputRuntimeConfig,
    activity_capability_registry: Arc<ActivityCapabilityRegistry>,
    activity_executor_registry: Arc<LocalActivityExecutorRegistry>,
    throughput_partitions: i32,
    outbox_publisher_id: String,
    debug: Arc<StdMutex<ThroughputDebugState>>,
    ownership: Arc<StdMutex<ThroughputOwnershipState>>,
}

#[derive(Clone)]
struct WorkerApi {
    state: AppState,
}

#[derive(Debug, Clone, Serialize, Default)]
struct ThroughputDebugState {
    poll_requests: u64,
    poll_responses: u64,
    leased_tasks: u64,
    empty_polls: u64,
    lease_misses_with_ready_chunks: u64,
    last_lease_selection_debug: Option<serde_json::Value>,
    bridged_batches: u64,
    duplicate_bridge_events: u64,
    bridge_failures: u64,
    report_rpcs_received: u64,
    reports_received: u64,
    report_batches_applied: u64,
    report_batch_items_total: u64,
    reports_rejected: u64,
    local_fastlane_chunks_completed: u64,
    local_fastlane_batches_applied: u64,
    projection_events_published: u64,
    projection_events_skipped: u64,
    projection_events_applied_directly: u64,
    changelog_entries_published: u64,
    terminal_events_published: u64,
    leaf_group_terminals_published: u64,
    parent_group_terminals_published: u64,
    root_group_terminals_published: u64,
    tenant_throttle_events: u64,
    task_queue_throttle_events: u64,
    batch_throttle_events: u64,
    apply_divergences: u64,
    ownership_catchup_waits: u64,
    changelog_consumer_enabled: bool,
    last_bridge_at: Option<chrono::DateTime<chrono::Utc>>,
    last_report_at: Option<chrono::DateTime<chrono::Utc>>,
    last_terminal_event_at: Option<chrono::DateTime<chrono::Utc>>,
    last_lease_miss_with_ready_chunks_at: Option<chrono::DateTime<chrono::Utc>>,
    last_report_rejection_reason: Option<String>,
    last_throttle_at: Option<chrono::DateTime<chrono::Utc>>,
    last_throttle_reason: Option<String>,
    last_apply_divergence: Option<String>,
    last_catchup_wait_partition: Option<i32>,
}

#[derive(Debug, Clone, Serialize)]
struct ThroughputDebugResponse {
    runtime: ThroughputDebugState,
    local_state: LocalThroughputDebugSnapshot,
    ownership: ThroughputOwnershipDebugSnapshot,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ChunkPageQuery {
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
struct StrongBatchResponse {
    batch: WorkflowBulkBatchRecord,
}

#[derive(Debug, Clone, Serialize)]
struct StrongChunksResponse {
    chunks: Vec<WorkflowBulkChunkRecord>,
}

#[derive(Debug, Clone)]
struct ChunkTerminalArtifacts {
    result_handle: Option<Value>,
    cancellation_reason: Option<String>,
    cancellation_metadata: Option<Value>,
}

#[derive(Debug, Clone)]
struct StreamProjectionRecord {
    key: String,
    event: ThroughputProjectionEvent,
}

#[derive(Debug, Clone)]
struct StreamChangelogRecord {
    key: String,
    entry: ThroughputChangelogEntry,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CheckpointLatestPointer {
    latest_key: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
struct LeaseState {
    partition_id: i32,
    owner_id: String,
    owner_epoch: u64,
    lease_expires_at: chrono::DateTime<chrono::Utc>,
    active: bool,
}

impl LeaseState {
    fn inactive(partition_id: i32, owner_id: &str) -> Self {
        Self {
            partition_id,
            owner_id: owner_id.to_owned(),
            owner_epoch: 0,
            lease_expires_at: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
            active: false,
        }
    }

    fn from_record(record: &PartitionOwnershipRecord) -> Self {
        Self {
            partition_id: record.partition_id,
            owner_id: record.owner_id.clone(),
            owner_epoch: record.owner_epoch,
            lease_expires_at: record.lease_expires_at,
            active: true,
        }
    }
}

#[derive(Debug)]
struct ThroughputOwnershipState {
    owner_id: String,
    partition_id_offset: i32,
    leases: HashMap<i32, LeaseState>,
}

#[derive(Debug, Clone, Serialize)]
struct ThroughputOwnedPartitionDebug {
    throughput_partition_id: i32,
    ownership_partition_id: i32,
    active: bool,
    owner_epoch: u64,
    lease_expires_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct ThroughputOwnershipDebugSnapshot {
    owner_id: String,
    partition_id_offset: i32,
    partitions: Vec<ThroughputOwnedPartitionDebug>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = GrpcServiceConfig::from_env("THROUGHPUT_RUNTIME", "throughput-runtime", 50053)?;
    let debug_config =
        HttpServiceConfig::from_env("THROUGHPUT_DEBUG", "throughput-runtime-debug", 3006)?;
    let postgres = PostgresConfig::from_env()?;
    let redpanda = RedpandaConfig::from_env()?;
    let runtime = ThroughputRuntimeConfig::from_env()?;
    let ownership_config = ThroughputOwnershipConfig::from_env()?;
    if !runtime.owner_first_apply && !runtime.publish_projection_events {
        anyhow::bail!(
            "THROUGHPUT_PUBLISH_PROJECTION_EVENTS=false requires THROUGHPUT_OWNER_FIRST_APPLY=true"
        );
    }
    init_tracing(&config.log_filter);

    let store = WorkflowStore::connect(&postgres.url).await?;
    store.init().await?;
    let local_state = LocalThroughputState::open(
        &runtime.local_state_dir,
        &runtime.checkpoint_dir,
        runtime.checkpoint_retention,
    )?;
    let workflow_broker = BrokerConfig::new(
        redpanda.brokers.clone(),
        redpanda.workflow_events_topic,
        redpanda.workflow_events_partitions,
    );
    let commands_config = JsonTopicConfig::new(
        redpanda.brokers.clone(),
        redpanda.throughput_commands_topic.clone(),
        redpanda.throughput_partitions,
    );
    let changelog_config = JsonTopicConfig::new(
        redpanda.brokers.clone(),
        redpanda.throughput_changelog_topic.clone(),
        redpanda.throughput_partitions,
    );
    let workflow_publisher = WorkflowPublisher::new(&workflow_broker, "throughput-runtime").await?;
    let changelog_publisher = JsonTopicPublisher::new(
        &JsonTopicConfig::new(
            redpanda.brokers.clone(),
            redpanda.throughput_changelog_topic,
            redpanda.throughput_partitions,
        ),
        "throughput-runtime-changelog",
    )
    .await?;
    let projection_publisher = JsonTopicPublisher::new(
        &JsonTopicConfig::new(
            redpanda.brokers.clone(),
            redpanda.throughput_projections_topic,
            redpanda.throughput_partitions,
        ),
        "throughput-runtime-projections",
    )
    .await?;

    let bulk_notify = Arc::new(Notify::new());
    let outbox_notify = Arc::new(Notify::new());
    let debug = Arc::new(StdMutex::new(ThroughputDebugState::default()));
    let payload_store = PayloadStore::from_config(build_payload_store_config(&runtime)).await?;
    let activity_capability_registry =
        Arc::new(load_activity_capability_registry_from_env("FABRIK_WORKER_PACKAGES_DIR")?);
    let activity_executor_registry =
        Arc::new(load_local_activity_executor_registry_from_env("FABRIK_WORKER_PACKAGES_DIR")?);
    let owner_id = format!("throughput-runtime:{}", Uuid::now_v7());
    let managed_partitions = ownership_config
        .static_partition_ids
        .clone()
        .unwrap_or_else(|| (0..redpanda.throughput_partitions).collect());
    let ownership = Arc::new(StdMutex::new(ThroughputOwnershipState {
        owner_id: owner_id.clone(),
        partition_id_offset: ownership_config.partition_id_offset,
        leases: managed_partitions
            .iter()
            .copied()
            .map(|partition_id| (partition_id, LeaseState::inactive(partition_id, &owner_id)))
            .collect(),
    }));
    let state = AppState {
        store,
        local_state,
        workflow_publisher,
        changelog_publisher,
        projection_publisher,
        payload_store,
        bulk_notify: bulk_notify.clone(),
        outbox_notify: outbox_notify.clone(),
        runtime,
        activity_capability_registry,
        activity_executor_registry,
        throughput_partitions: redpanda.throughput_partitions,
        outbox_publisher_id: format!("throughput-runtime:{}", Uuid::now_v7()),
        debug: debug.clone(),
        ownership: ownership.clone(),
    };
    {
        let mut runtime_debug = debug.lock().expect("throughput debug lock poisoned");
        runtime_debug.changelog_consumer_enabled = !state.runtime.owner_first_apply;
    }
    let restored_from_checkpoint = restore_state_from_checkpoint(&state).await?;
    if restored_from_checkpoint {
        info!("throughput-runtime restored local state from latest checkpoint");
    } else {
        seed_local_state_from_store(&state).await?;
    }
    if state.runtime.owner_first_apply {
        info!("throughput-runtime skipping changelog restore in owner-first mode");
    } else {
        restore_local_state_from_changelog(&state, &changelog_config, true).await?;
    }
    let ownership_replay_timeout =
        Duration::from_secs(u64::from(ownership_config.lease_ttl_seconds.max(1)).saturating_mul(2));
    let debug_base_url = format!("http://127.0.0.1:{}", debug_config.port);
    if state.runtime.owner_first_apply {
        spawn_ownership_loop(
            state.clone(),
            ownership.clone(),
            managed_partitions.clone(),
            debug_base_url.clone(),
            debug.clone(),
            ownership_config.clone(),
        );
        wait_for_owned_partitions(&state, &managed_partitions, ownership_replay_timeout).await?;
        replay_report_log_tail(&state).await?;
    }

    let command_consumer = build_json_consumer(
        &commands_config,
        "throughput-runtime-commands",
        &commands_config.all_partition_ids(),
    )
    .await?;
    let consumer = build_workflow_consumer(
        &workflow_broker,
        "throughput-runtime",
        &workflow_broker.all_partition_ids(),
    )
    .await?;
    tokio::spawn(run_command_loop(state.clone(), command_consumer));
    tokio::spawn(run_bridge_loop(state.clone(), consumer));
    if !state.runtime.owner_first_apply {
        tokio::spawn(run_changelog_consumer_loop(state.clone(), changelog_config.clone()));
    }
    tokio::spawn(run_checkpoint_loop(state.clone()));
    tokio::spawn(run_terminal_state_gc_loop(state.clone()));
    tokio::spawn(run_group_barrier_reconciler(state.clone()));
    if !state.runtime.owner_first_apply {
        spawn_ownership_loop(
            state.clone(),
            ownership.clone(),
            managed_partitions.clone(),
            debug_base_url,
            debug.clone(),
            ownership_config,
        );
    }
    tokio::spawn(run_outbox_publisher(state.clone()));
    tokio::spawn(run_bulk_requeue_sweep(state.clone()));
    tokio::spawn(run_local_benchmark_fastlane_loop(state.clone()));
    let debug_state = debug.clone();
    let debug_state_app = state.clone();
    let debug_app = default_router::<AppState>(ServiceInfo::new(
        debug_config.name,
        "throughput-debug",
        env!("CARGO_PKG_VERSION"),
    ))
    .route(
        "/debug/throughput",
        get(move || {
            let debug_state = debug_state.clone();
            let state = debug_state_app.clone();
            async move {
                let runtime = debug_state.lock().expect("throughput debug lock poisoned").clone();
                let local_state =
                    state.local_state.debug_snapshot().expect("throughput local state snapshot");
                let ownership = throughput_ownership_debug_snapshot(&state);
                Json(ThroughputDebugResponse { runtime, local_state, ownership })
            }
        }),
    )
    .route(
        "/debug/throughput/batches/{tenant_id}/{instance_id}/{run_id}/{batch_id}",
        get(get_strong_batch_snapshot),
    )
    .route(
        "/debug/throughput/batches/{tenant_id}/{instance_id}/{run_id}/{batch_id}/chunks",
        get(get_strong_chunk_snapshots),
    )
    .with_state(state.clone());
    tokio::spawn(async move {
        if let Err(error) = serve(debug_app, debug_config.port).await {
            error!(error = %error, "throughput-runtime debug server exited");
        }
    });

    let addr = format!("0.0.0.0:{}", config.port).parse()?;
    info!(%addr, "throughput-runtime listening");
    Server::builder()
        .add_service(ActivityWorkerApiServer::new(WorkerApi { state }))
        .serve(addr)
        .await?;
    Ok(())
}

async fn get_strong_batch_snapshot(
    Path((tenant_id, instance_id, run_id, batch_id)): Path<(String, String, String, String)>,
    AxumState(state): AxumState<AppState>,
) -> Result<Json<StrongBatchResponse>, (StatusCode, String)> {
    let batch = load_owner_batch_record(&state, &tenant_id, &instance_id, &run_id, &batch_id)
        .await
        .map_err(internal_http_error)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("bulk batch {batch_id} not found")))?;
    Ok(Json(StrongBatchResponse { batch }))
}

async fn get_strong_chunk_snapshots(
    Path((tenant_id, instance_id, run_id, batch_id)): Path<(String, String, String, String)>,
    Query(pagination): Query<ChunkPageQuery>,
    AxumState(state): AxumState<AppState>,
) -> Result<Json<StrongChunksResponse>, (StatusCode, String)> {
    let limit = pagination.limit.unwrap_or(100).max(1);
    let offset = pagination.offset.unwrap_or(0);
    let chunks = load_owner_chunk_records(
        &state,
        &tenant_id,
        &instance_id,
        &run_id,
        &batch_id,
        limit,
        offset,
    )
    .await
    .map_err(internal_http_error)?;
    Ok(Json(StrongChunksResponse { chunks }))
}

async fn run_bridge_loop(state: AppState, mut consumer: fabrik_broker::WorkflowConsumerStream) {
    let mut in_flight = JoinSet::new();
    while let Some(message) = consumer.next().await {
        let record = match message {
            Ok(record) => record,
            Err(error) => {
                error!(error = %error, "throughput-runtime consumer error");
                continue;
            }
        };
        let event = match decode_consumed_workflow_event(&record) {
            Ok(event) => event,
            Err(error) => {
                warn!(error = %error, "throughput-runtime skipping invalid workflow event");
                continue;
            }
        };
        while in_flight.len() >= BRIDGE_EVENT_CONCURRENCY {
            if let Some(result) = in_flight.join_next().await {
                if let Err(error) = result {
                    error!(error = %error, "throughput-runtime bridge worker task failed");
                }
            }
        }
        let state = state.clone();
        let debug_state = state.clone();
        in_flight.spawn(async move {
            if let Err(error) = handle_bridge_event(state, event).await {
                let mut debug = debug_state.debug.lock().expect("throughput debug lock poisoned");
                debug.bridge_failures = debug.bridge_failures.saturating_add(1);
                drop(debug);
                error!(error = %error, "throughput-runtime failed to handle workflow bridge event");
            }
        });
    }

    while let Some(result) = in_flight.join_next().await {
        if let Err(error) = result {
            error!(error = %error, "throughput-runtime bridge worker task failed");
        }
    }
}

async fn handle_bridge_event(state: AppState, event: EventEnvelope<WorkflowEvent>) -> Result<()> {
    if let WorkflowEvent::WorkflowCancellationRequested { reason } = &event.payload {
        cancel_stream_batches_for_run(&state, &event, reason).await?;
        return Ok(());
    }

    let WorkflowEvent::BulkActivityBatchScheduled {
        batch_id,
        task_queue: _,
        activity_type: _,
        items: _,
        input_handle: _,
        result_handle: _,
        chunk_size: _,
        max_attempts: _,
        retry_delay_ms: _,
        aggregation_group_count: _,
        execution_policy: _,
        reducer: _,
        throughput_backend,
        throughput_backend_version: _,
        state: _,
        ..
    } = &event.payload
    else {
        return Ok(());
    };
    if throughput_backend != ThroughputBackend::StreamV2.as_str() {
        return Ok(());
    }
    if state
        .store
        .get_throughput_run(&event.tenant_id, &event.instance_id, &event.run_id, batch_id)
        .await?
        .is_some_and(|run| run.command_published_at.is_some())
    {
        let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
        debug.duplicate_bridge_events = debug.duplicate_bridge_events.saturating_add(1);
        return Ok(());
    }

    let dedupe_key = format!("throughput-create:{}", event.event_id);
    let published_at = state
        .store
        .ensure_throughput_bridge_progress(
            event.event_id,
            &event.tenant_id,
            &event.instance_id,
            &event.run_id,
            batch_id,
            throughput_backend,
            &dedupe_key,
            event.occurred_at,
        )
        .await
        .with_context(|| format!("failed to record throughput bridge progress for {batch_id}"))?;
    if published_at.is_some() {
        let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
        debug.duplicate_bridge_events = debug.duplicate_bridge_events.saturating_add(1);
        return Ok(());
    }

    activate_stream_batch(&state, &event).await?;

    state
        .store
        .mark_throughput_bridge_command_published(event.event_id, Utc::now())
        .await
        .with_context(|| {
            format!("failed to mark throughput create command published for {batch_id}")
        })?;
    let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
    debug.bridged_batches = debug.bridged_batches.saturating_add(1);
    debug.last_bridge_at = Some(Utc::now());
    drop(debug);
    state.bulk_notify.notify_waiters();
    Ok(())
}

async fn run_command_loop(state: AppState, mut consumer: fabrik_broker::JsonConsumerStream) {
    while let Some(message) = consumer.next().await {
        let record = match message {
            Ok(record) => record,
            Err(error) => {
                error!(error = %error, "throughput-runtime command consumer error");
                continue;
            }
        };
        let command = match decode_json_record::<ThroughputCommandEnvelope>(&record.record) {
            Ok(command) => command,
            Err(error) => {
                warn!(error = %error, "throughput-runtime skipping invalid command");
                continue;
            }
        };
        if let Err(error) = handle_throughput_command(state.clone(), command).await {
            error!(error = %error, "throughput-runtime failed to handle throughput command");
        }
    }
}

async fn handle_throughput_command(
    state: AppState,
    command: ThroughputCommandEnvelope,
) -> Result<()> {
    let (tenant_id, instance_id, run_id, batch_id, throughput_backend) = match &command.payload {
        ThroughputCommand::StartThroughputRun(command) => (
            command.tenant_id.as_str(),
            command.instance_id.as_str(),
            command.run_id.as_str(),
            command.batch_id.as_str(),
            command.throughput_backend.as_str(),
        ),
        ThroughputCommand::CreateBatch(command) => (
            command.tenant_id.as_str(),
            command.instance_id.as_str(),
            command.run_id.as_str(),
            command.batch_id.as_str(),
            command.throughput_backend.as_str(),
        ),
        ThroughputCommand::TinyWorkflowStart(_) | ThroughputCommand::TinyWorkflowStartBatch(_) => {
            return Ok(());
        }
        _ => return Ok(()),
    };
    if throughput_backend != ThroughputBackend::StreamV2.as_str() {
        return Ok(());
    }
    if state.store.get_throughput_run(tenant_id, instance_id, run_id, batch_id).await?.is_some_and(
        |run| matches!(run.status.as_str(), "running" | "completed" | "failed" | "cancelled"),
    ) {
        return Ok(());
    }
    match &command.payload {
        ThroughputCommand::StartThroughputRun(start) => {
            activate_native_stream_run(&state, &command, start).await
        }
        ThroughputCommand::CreateBatch(_) => {
            let event = workflow_event_from_throughput_command(&command)?;
            activate_stream_batch(&state, &event).await
        }
        ThroughputCommand::TinyWorkflowStart(_) | ThroughputCommand::TinyWorkflowStartBatch(_) => {
            Ok(())
        }
        _ => Ok(()),
    }
}

async fn restore_local_state_from_changelog(
    state: &AppState,
    config: &JsonTopicConfig,
    stop_on_idle: bool,
) -> Result<()> {
    let partitions = config.all_partition_ids();
    let offsets = state.local_state.next_start_offsets(&partitions)?;
    let mut consumer = build_json_consumer_from_offsets(
        config,
        "throughput-runtime-changelog-restore",
        &offsets,
        &partitions,
    )
    .await?;
    loop {
        let next = if stop_on_idle {
            match tokio::time::timeout(
                Duration::from_millis(state.runtime.restore_idle_timeout_ms.max(1)),
                consumer.next(),
            )
            .await
            {
                Ok(item) => item,
                Err(_) => break,
            }
        } else {
            consumer.next().await
        };
        let Some(message) = next else {
            if stop_on_idle {
                break;
            }
            return Ok(());
        };
        let record = message.context("failed to read throughput changelog message")?;
        state
            .local_state
            .record_observed_high_watermark(record.partition_id, record.high_watermark);
        let entry: ThroughputChangelogEntry = decode_json_record(&record.record)
            .context("failed to decode throughput changelog entry")?;
        if let Err(error) = state.local_state.apply_changelog_entry(
            record.partition_id,
            record.record.offset,
            &entry,
        ) {
            state.local_state.record_changelog_apply_failure();
            return Err(error);
        }
    }
    Ok(())
}

async fn seed_local_state_from_store_view(
    store: &WorkflowStore,
    local_state: &LocalThroughputState,
) -> Result<()> {
    let batches = store
        .list_nonterminal_throughput_projection_batches_for_backend(
            ThroughputBackend::StreamV2.as_str(),
            10_000,
        )
        .await?;
    for batch in batches {
        let existing_chunks = store
            .list_bulk_chunks_for_batch_page_query_view(
                &batch.tenant_id,
                &batch.instance_id,
                &batch.run_id,
                &batch.batch_id,
                i64::MAX,
                0,
            )
            .await?;
        let chunks = reconstruct_missing_stream_chunks(&batch, existing_chunks)?;
        local_state.upsert_batch_with_chunks(&batch, &chunks)?;
    }
    Ok(())
}

async fn seed_local_state_from_store(state: &AppState) -> Result<()> {
    seed_local_state_from_runs(state).await?;
    seed_local_state_from_store_view(&state.store, &state.local_state).await
}

async fn seed_local_state_from_runs(state: &AppState) -> Result<()> {
    let runs = state
        .store
        .list_nonterminal_throughput_runs_for_backend(ThroughputBackend::StreamV2.as_str(), 10_000)
        .await?;
    for run in runs {
        match &run.command.payload {
            ThroughputCommand::StartThroughputRun(start) => {
                activate_native_stream_run(state, &run.command, start).await?;
            }
            ThroughputCommand::CreateBatch(_) => {
                let event = workflow_event_from_throughput_command(&run.command)?;
                activate_stream_batch(state, &event).await?;
            }
            _ => {}
        }
    }
    Ok(())
}

async fn run_changelog_consumer_loop(state: AppState, config: JsonTopicConfig) {
    loop {
        if let Err(error) = restore_local_state_from_changelog(&state, &config, false).await {
            state.local_state.record_changelog_apply_failure();
            error!(error = %error, "failed to apply throughput changelog to local state");
            tokio::time::sleep(Duration::from_millis(500)).await;
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn run_checkpoint_loop(state: AppState) {
    let interval = Duration::from_secs(state.runtime.checkpoint_interval_seconds.max(1));
    loop {
        tokio::time::sleep(interval).await;
        if let Err(error) = write_remote_checkpoint(&state).await {
            state.local_state.record_checkpoint_failure();
            error!(error = %error, "failed to write throughput local-state checkpoint");
        }
    }
}

async fn run_terminal_state_gc_loop(state: AppState) {
    let interval = Duration::from_secs(state.runtime.terminal_gc_interval_seconds.max(1));
    let retention = chrono::Duration::seconds(
        i64::try_from(state.runtime.terminal_state_retention_seconds.max(1)).unwrap_or(3600),
    );
    loop {
        tokio::time::sleep(interval).await;
        let cutoff = Utc::now() - retention;
        if let Err(error) = state.local_state.prune_terminal_state(cutoff) {
            error!(error = %error, "failed to prune terminal throughput local state");
        }
    }
}

async fn run_throughput_ownership_loop(
    store: WorkflowStore,
    ownership: Arc<StdMutex<ThroughputOwnershipState>>,
    local_state: LocalThroughputState,
    managed_partitions: Vec<i32>,
    query_endpoint: String,
    debug: Arc<StdMutex<ThroughputDebugState>>,
    config: ThroughputOwnershipConfig,
    store_recovery_enabled: bool,
) {
    let use_assignments = config.static_partition_ids.is_none();
    let renew_interval = Duration::from_secs(config.renew_interval_seconds.max(1));
    let heartbeat_ttl = Duration::from_secs(config.member_heartbeat_ttl_seconds.max(1));
    let poll_interval = Duration::from_secs(config.assignment_poll_interval_seconds.max(1));
    let rebalance_interval = Duration::from_secs(config.rebalance_interval_seconds.max(1));
    let mut last_rebalance_at = std::time::Instant::now() - rebalance_interval;
    loop {
        let mut store_seeded_this_loop = false;
        let owner_id =
            { ownership.lock().expect("throughput ownership lock poisoned").owner_id.clone() };
        let desired_partitions = if use_assignments {
            if let Err(error) = store
                .heartbeat_throughput_member(
                    &owner_id,
                    &query_endpoint,
                    config.runtime_capacity,
                    heartbeat_ttl,
                )
                .await
            {
                warn!(error = %error, "failed to heartbeat throughput membership");
            }
            if last_rebalance_at.elapsed() >= rebalance_interval {
                match store.list_active_throughput_members(Utc::now()).await {
                    Ok(members) => {
                        if let Err(error) = store
                            .reconcile_throughput_partition_assignments(
                                i32::try_from(managed_partitions.len()).unwrap_or_default(),
                                &members,
                            )
                            .await
                        {
                            warn!(error = %error, "failed to reconcile throughput partition assignments");
                        }
                    }
                    Err(error) => warn!(error = %error, "failed to load active throughput members"),
                }
                last_rebalance_at = std::time::Instant::now();
            }
            match store.list_assignments_for_throughput_runtime(&owner_id).await {
                Ok(assignments) => assignments
                    .into_iter()
                    .map(|assignment| assignment.partition_id)
                    .collect::<HashSet<_>>(),
                Err(error) => {
                    warn!(error = %error, "failed to list throughput partition assignments");
                    HashSet::new()
                }
            }
        } else {
            managed_partitions.iter().copied().collect::<HashSet<_>>()
        };
        run_throughput_ownership_pass(
            &store,
            &ownership,
            &local_state,
            &managed_partitions,
            &desired_partitions,
            &debug,
            &config,
            store_recovery_enabled,
            &mut store_seeded_this_loop,
        )
        .await;
        tokio::time::sleep(if use_assignments { poll_interval } else { renew_interval }).await;
    }
}

async fn run_throughput_ownership_pass(
    store: &WorkflowStore,
    ownership: &Arc<StdMutex<ThroughputOwnershipState>>,
    local_state: &LocalThroughputState,
    managed_partitions: &[i32],
    desired_partitions: &HashSet<i32>,
    debug: &Arc<StdMutex<ThroughputDebugState>>,
    config: &ThroughputOwnershipConfig,
    store_recovery_enabled: bool,
    store_seeded_this_loop: &mut bool,
) {
    let use_assignments = config.static_partition_ids.is_none();
    let lease_ttl = Duration::from_secs(config.lease_ttl_seconds.max(1));
    for throughput_partition_id in managed_partitions {
        let snapshot =
            {
                let state = ownership.lock().expect("throughput ownership lock poisoned");
                state.leases.get(throughput_partition_id).cloned().unwrap_or_else(|| {
                    LeaseState::inactive(*throughput_partition_id, &state.owner_id)
                })
            };
        let should_own = desired_partitions.contains(throughput_partition_id);
        let ownership_partition_id =
            throughput_ownership_partition_id(*throughput_partition_id, config.partition_id_offset);
        if snapshot.active && !should_own {
            if let Err(error) = store
                .release_partition_ownership(
                    ownership_partition_id,
                    &snapshot.owner_id,
                    snapshot.owner_epoch,
                )
                .await
            {
                warn!(
                    error = %error,
                    throughput_partition_id = *throughput_partition_id,
                    "failed to release throughput partition ownership"
                );
            }
            let mut state = ownership.lock().expect("throughput ownership lock poisoned");
            state.leases.insert(
                *throughput_partition_id,
                LeaseState::inactive(*throughput_partition_id, &snapshot.owner_id),
            );
            continue;
        }
        if !should_own {
            continue;
        }
        if !snapshot.active {
            if store_recovery_enabled && !*store_seeded_this_loop {
                match seed_local_state_from_store_view(store, local_state).await {
                    Ok(()) => *store_seeded_this_loop = true,
                    Err(error) => warn!(
                        error = %error,
                        throughput_partition_id = *throughput_partition_id,
                        "failed to seed throughput local state from projection store"
                    ),
                }
            }
            if !store_recovery_enabled {
                match local_state.partition_is_caught_up(*throughput_partition_id) {
                    Ok(true) => {}
                    Ok(false) => {
                        let mut runtime_debug =
                            debug.lock().expect("throughput debug lock poisoned");
                        runtime_debug.ownership_catchup_waits =
                            runtime_debug.ownership_catchup_waits.saturating_add(1);
                        runtime_debug.last_catchup_wait_partition = Some(*throughput_partition_id);
                        continue;
                    }
                    Err(error) => {
                        warn!(
                            error = %error,
                            throughput_partition_id = *throughput_partition_id,
                            "failed to evaluate throughput partition catch-up state"
                        );
                        continue;
                    }
                }
            }
        }
        if snapshot.active {
            match store
                .renew_partition_ownership(
                    ownership_partition_id,
                    &snapshot.owner_id,
                    snapshot.owner_epoch,
                    lease_ttl,
                )
                .await
            {
                Ok(Some(record)) => {
                    let mut state = ownership.lock().expect("throughput ownership lock poisoned");
                    state.leases.insert(*throughput_partition_id, LeaseState::from_record(&record));
                }
                Ok(None) => {
                    let mut state = ownership.lock().expect("throughput ownership lock poisoned");
                    state.leases.insert(
                        *throughput_partition_id,
                        LeaseState::inactive(*throughput_partition_id, &snapshot.owner_id),
                    );
                }
                Err(error) => warn!(
                    error = %error,
                    throughput_partition_id = *throughput_partition_id,
                    "failed to renew throughput partition ownership"
                ),
            }
        } else {
            let claim = if use_assignments {
                store
                    .claim_throughput_partition_ownership(
                        ownership_partition_id,
                        &snapshot.owner_id,
                        lease_ttl,
                    )
                    .await
            } else {
                store
                    .claim_partition_ownership(
                        ownership_partition_id,
                        &snapshot.owner_id,
                        lease_ttl,
                    )
                    .await
            };
            match claim {
                Ok(Some(record)) => {
                    let mut state = ownership.lock().expect("throughput ownership lock poisoned");
                    state.leases.insert(*throughput_partition_id, LeaseState::from_record(&record));
                }
                Ok(None) => {}
                Err(error) => warn!(
                    error = %error,
                    throughput_partition_id = *throughput_partition_id,
                    "failed to claim throughput partition ownership"
                ),
            }
        }
    }
}

fn throughput_ownership_partition_id(throughput_partition_id: i32, offset: i32) -> i32 {
    offset.saturating_add(throughput_partition_id)
}

async fn restore_state_from_checkpoint(state: &AppState) -> Result<bool> {
    if let Some(value) = load_latest_checkpoint_value(state).await? {
        return state.local_state.restore_from_checkpoint_value_if_empty(value);
    }
    state.local_state.restore_from_latest_checkpoint_if_empty()
}

async fn load_latest_checkpoint_value(state: &AppState) -> Result<Option<serde_json::Value>> {
    let latest_key = checkpoint_latest_pointer_key(&state.runtime.checkpoint_key_prefix);
    let latest_handle = PayloadHandle::Manifest {
        key: latest_key.clone(),
        store: state.payload_store.default_store_kind().as_str().to_owned(),
    };
    let latest_pointer = match state.payload_store.read_value(&latest_handle).await {
        Ok(value) => Some(
            serde_json::from_value::<CheckpointLatestPointer>(value)
                .context("failed to deserialize throughput checkpoint latest pointer")?,
        ),
        Err(_) => None,
    };
    let Some(latest_pointer) = latest_pointer else {
        return Ok(None);
    };
    let checkpoint_handle = PayloadHandle::Manifest {
        key: latest_pointer.latest_key,
        store: state.payload_store.default_store_kind().as_str().to_owned(),
    };
    let checkpoint = state.payload_store.read_value(&checkpoint_handle).await?;
    Ok(Some(checkpoint))
}

async fn write_remote_checkpoint(state: &AppState) -> Result<()> {
    let checkpoint = state.local_state.snapshot_checkpoint_value()?;
    let created_at = checkpoint_created_at(&checkpoint)?;
    let checkpoint_key =
        checkpoint_object_key(&state.runtime.checkpoint_key_prefix, created_at.timestamp_millis());
    state.payload_store.write_value(&checkpoint_key, &checkpoint).await?;
    let latest_pointer = serde_json::to_value(CheckpointLatestPointer {
        latest_key: checkpoint_key.clone(),
        created_at,
    })
    .context("failed to serialize throughput checkpoint latest pointer")?;
    state
        .payload_store
        .write_value(
            &checkpoint_latest_pointer_key(&state.runtime.checkpoint_key_prefix),
            &latest_pointer,
        )
        .await?;
    prune_remote_checkpoints(state).await?;
    if state.runtime.owner_first_apply && !state.local_state.has_collect_results_batches()? {
        let _ = state.store.delete_throughput_report_log_through(created_at).await?;
    }
    state.local_state.record_checkpoint_write(created_at);
    Ok(())
}

async fn replay_report_log_tail(state: &AppState) -> Result<()> {
    let mut captured_after = state.local_state.debug_snapshot()?.last_checkpoint_at;
    let page_size = state.runtime.report_apply_batch_size.max(1).saturating_mul(32);
    loop {
        let entries =
            state.store.list_throughput_report_log_since(captured_after, page_size).await?;
        if entries.is_empty() {
            break;
        }
        for report_batch in entries.chunks(state.runtime.report_apply_batch_size.max(1)) {
            let reports = report_batch.iter().map(|entry| entry.report.clone()).collect::<Vec<_>>();
            apply_throughput_report_batch(state, &reports, false).await?;
        }
        captured_after = entries.last().map(|entry| entry.captured_at);
        if entries.len() < page_size {
            break;
        }
    }
    Ok(())
}

fn spawn_ownership_loop(
    state: AppState,
    ownership: Arc<StdMutex<ThroughputOwnershipState>>,
    managed_partitions: Vec<i32>,
    debug_base_url: String,
    debug: Arc<StdMutex<ThroughputDebugState>>,
    ownership_config: ThroughputOwnershipConfig,
) {
    tokio::spawn(run_throughput_ownership_loop(
        state.store.clone(),
        ownership,
        state.local_state.clone(),
        managed_partitions,
        debug_base_url,
        debug,
        ownership_config,
        state.runtime.owner_first_apply,
    ));
}

async fn wait_for_owned_partitions(
    state: &AppState,
    managed_partitions: &[i32],
    timeout: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let owned = owned_throughput_partitions(state);
        if managed_partitions.iter().all(|partition_id| owned.contains(partition_id)) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting to reclaim throughput ownership for {} partitions",
                managed_partitions.len()
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn prune_remote_checkpoints(state: &AppState) -> Result<()> {
    let prefix =
        format!("{}/checkpoint-", state.runtime.checkpoint_key_prefix.trim_end_matches('/'));
    let keys = state.payload_store.list_keys(&prefix).await?;
    if keys.len() <= state.runtime.checkpoint_retention {
        return Ok(());
    }
    let stale_count = keys.len() - state.runtime.checkpoint_retention;
    for key in keys.into_iter().take(stale_count) {
        state.payload_store.delete_key(&key).await?;
    }
    Ok(())
}

fn checkpoint_latest_pointer_key(prefix: &str) -> String {
    format!("{}/latest", prefix.trim_end_matches('/'))
}

fn checkpoint_object_key(prefix: &str, millis: i64) -> String {
    format!("{}/checkpoint-{}", prefix.trim_end_matches('/'), millis)
}

fn checkpoint_created_at(value: &serde_json::Value) -> Result<chrono::DateTime<chrono::Utc>> {
    serde_json::from_value(
        value
            .get("created_at")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("checkpoint value is missing created_at"))?,
    )
    .context("failed to parse throughput checkpoint created_at")
}

fn owned_throughput_partitions(state: &AppState) -> HashSet<i32> {
    let now = Utc::now();
    let ownership = state.ownership.lock().expect("throughput ownership lock poisoned");
    ownership
        .leases
        .iter()
        .filter_map(|(partition_id, lease)| {
            (lease.active && lease.lease_expires_at > now).then_some(*partition_id)
        })
        .collect()
}

fn owns_throughput_partition(state: &AppState, throughput_partition_id: i32) -> bool {
    owned_throughput_partitions(state).contains(&throughput_partition_id)
}

fn throughput_partition_for_batch(batch_id: &str, group_id: u32, partition_count: i32) -> i32 {
    partition_for_key(&throughput_partition_key(batch_id, group_id), partition_count)
}

fn local_terminal_projection_batch_update(
    local_state: &LocalThroughputState,
    batch: &local_state::LocalBatchSnapshot,
) -> Result<Option<ThroughputProjectionBatchStateUpdate>> {
    if !matches!(batch.status.as_str(), "completed" | "failed" | "cancelled") {
        return Ok(None);
    }
    let terminal_at = batch.terminal_at.unwrap_or(batch.updated_at);
    let reducer_output = if batch.status == WorkflowBulkBatchStatus::Completed.as_str() {
        local_state.reduce_batch_success_outputs_after_report(&batch.identity, None)?
    } else {
        None
    };
    Ok(Some(ThroughputProjectionBatchStateUpdate {
        tenant_id: batch.identity.tenant_id.clone(),
        instance_id: batch.identity.instance_id.clone(),
        run_id: batch.identity.run_id.clone(),
        batch_id: batch.identity.batch_id.clone(),
        status: batch.status.clone(),
        succeeded_items: batch.succeeded_items,
        failed_items: batch.failed_items,
        cancelled_items: batch.cancelled_items,
        error: batch.error.clone(),
        reducer_output: reducer_output.or_else(|| batch.reducer_output.clone()),
        terminal_at: Some(terminal_at),
        updated_at: batch.updated_at.max(terminal_at),
    }))
}

async fn repair_stale_terminal_projection_batches_in_store(
    store: &WorkflowStore,
    local_state: &LocalThroughputState,
    owned_partitions: &HashSet<i32>,
    throughput_partitions: i32,
) -> Result<usize> {
    let batches = store
        .list_nonterminal_throughput_projection_batches_for_backend(
            ThroughputBackend::StreamV2.as_str(),
            10_000,
        )
        .await?;
    let mut updates = Vec::new();
    for batch in batches {
        let batch_partition =
            throughput_partition_for_batch(&batch.batch_id, 0, throughput_partitions);
        if !owned_partitions.contains(&batch_partition) {
            continue;
        }
        let identity = ThroughputBatchIdentity {
            tenant_id: batch.tenant_id.clone(),
            instance_id: batch.instance_id.clone(),
            run_id: batch.run_id.clone(),
            batch_id: batch.batch_id.clone(),
        };
        let Some(local_batch) = local_state.batch_snapshot(&identity)? else {
            continue;
        };
        let Some(update) = local_terminal_projection_batch_update(local_state, &local_batch)?
        else {
            continue;
        };
        updates.push(update);
    }
    if updates.is_empty() {
        return Ok(0);
    }
    store.update_throughput_projection_batch_states(&updates).await?;
    Ok(updates.len())
}

async fn repair_stale_terminal_projection_batches(
    state: &AppState,
    owned_partitions: &HashSet<i32>,
) -> Result<usize> {
    let repaired = repair_stale_terminal_projection_batches_in_store(
        &state.store,
        &state.local_state,
        owned_partitions,
        state.throughput_partitions,
    )
    .await?;
    if repaired > 0 {
        record_projection_events_applied_directly(state, repaired);
    }
    Ok(repaired)
}

async fn repair_missing_terminal_handoffs(
    state: &AppState,
    owned_partitions: &HashSet<i32>,
) -> Result<usize> {
    let runs = state
        .store
        .list_nonterminal_throughput_runs_for_backend(ThroughputBackend::StreamV2.as_str(), 10_000)
        .await?;
    let mut repaired = 0usize;
    for run in runs {
        let batch_partition =
            throughput_partition_for_batch(&run.batch_id, 0, state.throughput_partitions);
        if !owned_partitions.contains(&batch_partition) {
            continue;
        }
        let Some(batch) = state
            .store
            .get_bulk_batch_query_view(&run.tenant_id, &run.instance_id, &run.run_id, &run.batch_id)
            .await?
        else {
            continue;
        };
        if !matches!(
            batch.status,
            WorkflowBulkBatchStatus::Completed
                | WorkflowBulkBatchStatus::Failed
                | WorkflowBulkBatchStatus::Cancelled
        ) {
            continue;
        }
        if state
            .store
            .get_throughput_terminal(&run.tenant_id, &run.instance_id, &run.run_id, &run.batch_id)
            .await?
            .is_some()
        {
            continue;
        }
        sync_projection_batch_result_manifest(
            state,
            &run.tenant_id,
            &run.instance_id,
            &run.run_id,
            &run.batch_id,
        )
        .await?;
        publish_stream_batch_terminal_event(state, &batch, None).await?;
        repaired = repaired.saturating_add(1);
    }
    Ok(repaired)
}

fn throughput_ownership_debug_snapshot(state: &AppState) -> ThroughputOwnershipDebugSnapshot {
    let ownership = state.ownership.lock().expect("throughput ownership lock poisoned");
    let mut partitions = ownership
        .leases
        .values()
        .cloned()
        .map(|lease| ThroughputOwnedPartitionDebug {
            throughput_partition_id: lease.partition_id,
            ownership_partition_id: throughput_ownership_partition_id(
                lease.partition_id,
                ownership.partition_id_offset,
            ),
            active: lease.active && lease.lease_expires_at > Utc::now(),
            owner_epoch: lease.owner_epoch,
            lease_expires_at: lease.lease_expires_at,
        })
        .collect::<Vec<_>>();
    partitions.sort_by_key(|partition| partition.throughput_partition_id);
    ThroughputOwnershipDebugSnapshot {
        owner_id: ownership.owner_id.clone(),
        partition_id_offset: ownership.partition_id_offset,
        partitions,
    }
}

fn build_payload_store_config(runtime: &ThroughputRuntimeConfig) -> PayloadStoreConfig {
    PayloadStoreConfig {
        default_store: match runtime.payload_store.kind {
            fabrik_config::ThroughputPayloadStoreKind::LocalFilesystem => {
                PayloadStoreKind::LocalFilesystem
            }
            fabrik_config::ThroughputPayloadStoreKind::S3 => PayloadStoreKind::S3,
        },
        local_dir: runtime.payload_store.local_dir.clone(),
        s3_bucket: runtime.payload_store.s3_bucket.clone(),
        s3_region: runtime.payload_store.s3_region.clone(),
        s3_endpoint: runtime.payload_store.s3_endpoint.clone(),
        s3_access_key_id: runtime.payload_store.s3_access_key_id.clone(),
        s3_secret_access_key: runtime.payload_store.s3_secret_access_key.clone(),
        s3_force_path_style: runtime.payload_store.s3_force_path_style,
        s3_key_prefix: runtime.payload_store.s3_key_prefix.clone(),
    }
}

async fn load_owner_batch_record(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
) -> Result<Option<WorkflowBulkBatchRecord>> {
    let Some(mut batch) =
        state.store.get_bulk_batch(tenant_id, instance_id, run_id, batch_id).await?.or(state
            .store
            .get_bulk_batch_query_view(tenant_id, instance_id, run_id, batch_id)
            .await?)
    else {
        return Ok(None);
    };
    if batch.throughput_backend != ThroughputBackend::StreamV2.as_str() {
        return Ok(Some(batch));
    }
    let identity = ThroughputBatchIdentity {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        batch_id: batch_id.to_owned(),
    };
    if let Some(snapshot) = state.local_state.batch_snapshot(&identity)? {
        batch.status = parse_bulk_batch_status(&snapshot.status)?;
        batch.succeeded_items = snapshot.succeeded_items;
        batch.failed_items = snapshot.failed_items;
        batch.cancelled_items = snapshot.cancelled_items;
        batch.error = snapshot.error;
        batch.updated_at = snapshot.updated_at;
        batch.terminal_at = snapshot.terminal_at;
    }
    batch.reducer_output =
        state.local_state.reduce_batch_success_outputs_after_report(&identity, None)?;
    Ok(Some(batch))
}

async fn load_owner_chunk_records(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
    limit: usize,
    offset: usize,
) -> Result<Vec<WorkflowBulkChunkRecord>> {
    let Some(batch) =
        load_owner_batch_record(state, tenant_id, instance_id, run_id, batch_id).await?
    else {
        return Ok(Vec::new());
    };
    let mut chunks = state
        .store
        .list_bulk_chunks_for_batch_page_query_view(
            tenant_id,
            instance_id,
            run_id,
            batch_id,
            i64::try_from(limit).context("owner chunk page limit exceeds i64")?,
            i64::try_from(offset).context("owner chunk page offset exceeds i64")?,
        )
        .await?;
    if batch.throughput_backend != ThroughputBackend::StreamV2.as_str() {
        return Ok(chunks);
    }
    let identity = ThroughputBatchIdentity {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        batch_id: batch_id.to_owned(),
    };
    let chunk_snapshots = state.local_state.chunk_snapshots_for_batch(&identity)?;
    let chunk_snapshots = chunk_snapshots
        .into_iter()
        .map(|snapshot| (snapshot.chunk_id.clone(), snapshot))
        .collect::<HashMap<_, _>>();
    for chunk in &mut chunks {
        if let Some(snapshot) = chunk_snapshots.get(&chunk.chunk_id) {
            chunk.status = parse_bulk_chunk_status(&snapshot.status)?;
            chunk.attempt = snapshot.attempt;
            chunk.lease_epoch = snapshot.lease_epoch;
            chunk.owner_epoch = snapshot.owner_epoch;
            chunk.result_handle = snapshot.result_handle.clone();
            chunk.output = snapshot.output.clone();
            chunk.error = snapshot.error.clone();
            chunk.cancellation_requested = snapshot.cancellation_requested;
            chunk.cancellation_reason = snapshot.cancellation_reason.clone();
            chunk.cancellation_metadata = snapshot.cancellation_metadata.clone();
            chunk.worker_id = snapshot.worker_id.clone();
            chunk.lease_token =
                snapshot.lease_token.as_deref().and_then(|value| uuid::Uuid::parse_str(value).ok());
            chunk.last_report_id = snapshot.report_id.clone();
            chunk.available_at = snapshot.available_at;
            chunk.started_at = snapshot.started_at;
            chunk.lease_expires_at = snapshot.lease_expires_at;
            chunk.completed_at = snapshot.completed_at;
            chunk.updated_at = snapshot.updated_at;
        }
    }
    Ok(chunks)
}

fn internal_http_error(error: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

fn parse_bulk_batch_status(status: &str) -> Result<WorkflowBulkBatchStatus> {
    match status {
        "scheduled" => Ok(WorkflowBulkBatchStatus::Scheduled),
        "running" => Ok(WorkflowBulkBatchStatus::Running),
        "completed" => Ok(WorkflowBulkBatchStatus::Completed),
        "failed" => Ok(WorkflowBulkBatchStatus::Failed),
        "cancelled" => Ok(WorkflowBulkBatchStatus::Cancelled),
        other => anyhow::bail!("unknown workflow bulk batch status {other}"),
    }
}

fn parse_bulk_chunk_status(status: &str) -> Result<WorkflowBulkChunkStatus> {
    match status {
        "scheduled" => Ok(WorkflowBulkChunkStatus::Scheduled),
        "started" => Ok(WorkflowBulkChunkStatus::Started),
        "completed" => Ok(WorkflowBulkChunkStatus::Completed),
        "failed" => Ok(WorkflowBulkChunkStatus::Failed),
        "cancelled" => Ok(WorkflowBulkChunkStatus::Cancelled),
        other => anyhow::bail!("unknown workflow bulk chunk status {other}"),
    }
}

async fn run_outbox_publisher(state: AppState) {
    loop {
        let now = Utc::now();
        let leased = match state
            .store
            .lease_workflow_event_outbox(
                &state.outbox_publisher_id,
                chrono::Duration::seconds(BULK_EVENT_OUTBOX_LEASE_SECONDS),
                BULK_EVENT_OUTBOX_BATCH_SIZE,
                now,
            )
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                error!(error = %error, "throughput-runtime failed to lease workflow outbox");
                tokio::time::sleep(Duration::from_millis(BULK_EVENT_OUTBOX_RETRY_MS as u64)).await;
                continue;
            }
        };
        if leased.is_empty() {
            state.outbox_notify.notified().await;
            continue;
        }
        for row in leased {
            if let Err(error) =
                state.workflow_publisher.publish(&row.event, &row.partition_key).await
            {
                error!(error = %error, outbox_id = %row.outbox_id, "failed to publish workflow terminal event");
                let _ = state
                    .store
                    .release_workflow_event_outbox_lease(
                        row.outbox_id,
                        &state.outbox_publisher_id,
                        Utc::now() + chrono::Duration::milliseconds(BULK_EVENT_OUTBOX_RETRY_MS),
                    )
                    .await;
                continue;
            }
            if let Err(error) = state
                .store
                .mark_workflow_event_outbox_published(
                    row.outbox_id,
                    &state.outbox_publisher_id,
                    Utc::now(),
                )
                .await
            {
                error!(error = %error, outbox_id = %row.outbox_id, "failed to mark workflow outbox published");
            } else {
                let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
                debug.terminal_events_published = debug.terminal_events_published.saturating_add(1);
                debug.last_terminal_event_at = Some(Utc::now());
            }
        }
    }
}

async fn run_bulk_requeue_sweep(state: AppState) {
    let interval = Duration::from_millis(state.runtime.sweep_interval_ms.max(1));
    loop {
        tokio::time::sleep(interval).await;
        let owned = owned_throughput_partitions(&state);
        if owned.is_empty() {
            continue;
        }
        let expired = match state.local_state.expired_started_chunks(
            Utc::now(),
            10_000,
            Some(&owned),
            state.throughput_partitions,
        ) {
            Ok(chunks) => chunks,
            Err(error) => {
                error!(error = %error, "throughput-runtime failed to list expired chunks from local state");
                continue;
            }
        };
        for chunk in expired {
            let occurred_at = Utc::now();
            let projected = match state.local_state.project_requeue(
                &chunk.identity,
                &chunk.chunk_id,
                occurred_at,
            ) {
                Ok(Some(projected)) => projected,
                Ok(None) => continue,
                Err(error) => {
                    error!(error = %error, chunk_id = %chunk.chunk_id, "failed to project expired chunk requeue from local state");
                    continue;
                }
            };
            publish_changelog(
                &state,
                ThroughputChangelogPayload::ChunkRequeued {
                    identity: chunk.identity.clone(),
                    chunk_id: projected.chunk_id.clone(),
                    chunk_index: projected.chunk_index,
                    attempt: projected.attempt,
                    group_id: projected.group_id,
                    item_count: projected.item_count,
                    max_attempts: projected.max_attempts,
                    retry_delay_ms: projected.retry_delay_ms,
                    lease_epoch: projected.lease_epoch,
                    owner_epoch: projected.owner_epoch,
                    available_at: projected.available_at,
                },
                throughput_partition_key(&chunk.identity.batch_id, projected.group_id),
            )
            .await;
            state.bulk_notify.notify_waiters();
        }
    }
}

async fn run_local_benchmark_fastlane_loop(state: AppState) {
    let idle_wait = Duration::from_millis(50);
    let batch_limit = (state.runtime.max_active_chunks_per_batch > 0)
        .then_some(state.runtime.max_active_chunks_per_batch);
    let lease_ttl = chrono::Duration::seconds(state.runtime.lease_ttl_seconds as i64);
    let max_chunks = state
        .runtime
        .report_apply_batch_size
        .max(state.runtime.poll_max_tasks)
        .max(LOCAL_BENCHMARK_FASTLANE_BATCH_SIZE);
    loop {
        match run_local_benchmark_fastlane_once(&state, batch_limit, lease_ttl, max_chunks).await {
            Ok(0) => {
                let _ = tokio::time::timeout(idle_wait, state.bulk_notify.notified()).await;
            }
            Ok(_) => tokio::task::yield_now().await,
            Err(error) => {
                error!(error = %error, "throughput-runtime local benchmark fastlane failed");
                let _ = tokio::time::timeout(idle_wait, state.bulk_notify.notified()).await;
            }
        }
    }
}

async fn run_local_benchmark_fastlane_once(
    state: &AppState,
    batch_limit: Option<usize>,
    lease_ttl: chrono::Duration,
    max_chunks: usize,
) -> Result<usize> {
    let owned = owned_throughput_partitions(state);
    if owned.is_empty() {
        return Ok(0);
    }
    let leased = state.local_state.lease_ready_chunks_matching(
        LOCAL_BENCHMARK_FASTLANE_WORKER_ID,
        Utc::now(),
        lease_ttl,
        batch_limit,
        &HashSet::new(),
        Some(&owned),
        state.throughput_partitions,
        max_chunks,
        |chunk| is_local_fastlane_chunk(state, chunk),
    )?;
    if leased.is_empty() {
        return Ok(0);
    }
    let mut reports = Vec::with_capacity(leased.len());
    for chunk in leased {
        reports.push(execute_local_fastlane_chunk(state, chunk).await?);
    }
    {
        let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
        debug.local_fastlane_batches_applied =
            debug.local_fastlane_batches_applied.saturating_add(1);
        debug.local_fastlane_chunks_completed =
            debug.local_fastlane_chunks_completed.saturating_add(reports.len() as u64);
        debug.report_batches_applied = debug.report_batches_applied.saturating_add(1);
        debug.report_batch_items_total =
            debug.report_batch_items_total.saturating_add(reports.len() as u64);
        debug.reports_received = debug.reports_received.saturating_add(reports.len() as u64);
        debug.last_report_at = Some(Utc::now());
    }
    apply_throughput_report_batch(state, &reports, true).await?;
    Ok(reports.len())
}

fn is_local_fastlane_chunk(state: &AppState, chunk: &LeasedChunkSnapshot) -> bool {
    can_complete_payloadless_bulk_chunk(
        &chunk.activity_type,
        chunk.activity_capabilities.as_ref(),
        chunk.omit_success_output,
        chunk.item_count,
        !chunk.items.is_empty(),
        !chunk.input_handle.is_null(),
        chunk.cancellation_requested,
    ) || can_execute_activity_locally(state, &chunk.task_queue, &chunk.activity_type)
}

fn can_execute_activity_locally(state: &AppState, task_queue: &str, activity_type: &str) -> bool {
    matches!(
        activity_type,
        BENCHMARK_ECHO_ACTIVITY | CORE_ECHO_ACTIVITY | CORE_NOOP_ACTIVITY | CORE_ACCEPT_ACTIVITY
    ) || state.activity_executor_registry.lookup(task_queue, activity_type).is_some()
}

async fn execute_local_fastlane_chunk(
    state: &AppState,
    chunk: LeasedChunkSnapshot,
) -> Result<ThroughputChunkReport> {
    if can_complete_payloadless_bulk_chunk(
        &chunk.activity_type,
        chunk.activity_capabilities.as_ref(),
        chunk.omit_success_output,
        chunk.item_count,
        !chunk.items.is_empty(),
        !chunk.input_handle.is_null(),
        chunk.cancellation_requested,
    ) {
        return Ok(local_benchmark_completed_report(chunk, Value::Null, None));
    }
    let items = resolve_local_fastlane_chunk_items(state, &chunk).await?;
    let omit_success_output = if chunk.activity_type == BENCHMARK_ECHO_ACTIVITY {
        chunk.omit_success_output && !items.iter().any(benchmark_echo_item_requires_output)
    } else {
        chunk.omit_success_output
    };
    let mut outputs = (!omit_success_output).then(|| Vec::with_capacity(items.len()));
    for item in items {
        match execute_local_fastlane_activity(
            state,
            &chunk.task_queue,
            &chunk.activity_type,
            chunk.attempt,
            &item,
        )
        .await
        {
            Ok(output) => {
                if let Some(outputs) = outputs.as_mut() {
                    outputs.push(output);
                }
            }
            Err(error) if error.to_string() == "activity cancelled" => {
                return Ok(local_benchmark_cancelled_report(chunk, error.to_string()));
            }
            Err(error) => {
                return Ok(local_benchmark_failed_report(chunk, error.to_string()));
            }
        }
    }
    Ok(local_benchmark_completed_report(chunk, Value::Null, outputs))
}

async fn resolve_local_fastlane_chunk_items(
    state: &AppState,
    chunk: &LeasedChunkSnapshot,
) -> Result<Vec<Value>> {
    if !chunk.items.is_empty() {
        return Ok(chunk.items.clone());
    }
    if chunk.input_handle.is_null() {
        return Ok(Vec::new());
    }
    let handle = serde_json::from_value::<PayloadHandle>(chunk.input_handle.clone())
        .context("failed to decode local benchmark chunk input handle")?;
    if chunk.activity_type == BENCHMARK_ECHO_ACTIVITY {
        if let Some(meta) = parse_benchmark_compact_input_meta_from_handle(&handle) {
            if meta.payload_size.is_some() {
                return Ok(synthesize_benchmark_echo_items(
                    meta,
                    chunk.chunk_index,
                    chunk.item_count,
                ));
            }
        }
    }
    state.payload_store.read_items(&handle).await.with_context(|| {
        format!("failed to read local benchmark chunk input for {}", chunk.chunk_id)
    })
}

async fn execute_local_fastlane_activity(
    state: &AppState,
    task_queue: &str,
    activity_type: &str,
    attempt: u32,
    input: &Value,
) -> Result<Value> {
    if activity_type == BENCHMARK_ECHO_ACTIVITY {
        return execute_benchmark_echo(attempt, input);
    }
    match activity_type {
        CORE_ECHO_ACTIVITY | CORE_NOOP_ACTIVITY | CORE_ACCEPT_ACTIVITY => {
            return execute_handler(activity_type, input).map_err(anyhow::Error::from);
        }
        _ => {}
    }
    let Some(bootstrap_path) = state.activity_executor_registry.lookup(task_queue, activity_type) else {
        anyhow::bail!("activity {activity_type} has no local fastlane executor for task queue {task_queue}");
    };
    execute_packaged_node_activity(bootstrap_path, activity_type, input, None)
}

fn execute_packaged_node_activity(
    bootstrap_path: &str,
    activity_type: &str,
    input: &Value,
    config: Option<&Value>,
) -> Result<Value> {
    let envelope = serde_json::to_vec(&serde_json::json!({
        "activity_type": activity_type,
        "input": input,
        "config": config.cloned().unwrap_or(Value::Null),
    }))?;
    let mut child = Command::new("node")
        .arg(bootstrap_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| {
            format!("failed to start packaged activity bootstrap {}", bootstrap_path)
        })?;
    {
        let stdin = child.stdin.as_mut().context("packaged activity bootstrap stdin unavailable")?;
        stdin.write_all(&envelope)?;
    }
    let output = child.wait_with_output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        anyhow::bail!(
            "packaged activity bootstrap failed for {activity_type}: {}",
            if stderr.is_empty() { "unknown error" } else { &stderr }
        );
    }
    let response: Value = serde_json::from_slice(&output.stdout).with_context(|| {
        format!("packaged activity bootstrap produced invalid JSON for {activity_type}")
    })?;
    if response.get("ok").and_then(Value::as_bool).unwrap_or(false) {
        return Ok(response.get("output").cloned().unwrap_or(Value::Null));
    }
    anyhow::bail!(
        "{}",
        response
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("packaged activity failed without an explicit error")
    )
}

fn local_benchmark_completed_report(
    chunk: LeasedChunkSnapshot,
    result_handle: Value,
    output: Option<Vec<Value>>,
) -> ThroughputChunkReport {
    ThroughputChunkReport {
        report_id: Uuid::now_v7().to_string(),
        tenant_id: chunk.identity.tenant_id,
        instance_id: chunk.identity.instance_id,
        run_id: chunk.identity.run_id,
        batch_id: chunk.identity.batch_id,
        chunk_id: chunk.chunk_id,
        chunk_index: chunk.chunk_index,
        group_id: chunk.group_id,
        attempt: chunk.attempt,
        lease_epoch: chunk.lease_epoch,
        owner_epoch: chunk.owner_epoch,
        worker_id: LOCAL_BENCHMARK_FASTLANE_WORKER_ID.to_owned(),
        worker_build_id: LOCAL_BENCHMARK_FASTLANE_WORKER_BUILD_ID.to_owned(),
        lease_token: chunk.lease_token.unwrap_or_default(),
        occurred_at: Utc::now(),
        payload: ThroughputChunkReportPayload::ChunkCompleted { result_handle, output },
    }
}

fn local_benchmark_failed_report(
    chunk: LeasedChunkSnapshot,
    error: String,
) -> ThroughputChunkReport {
    ThroughputChunkReport {
        report_id: Uuid::now_v7().to_string(),
        tenant_id: chunk.identity.tenant_id,
        instance_id: chunk.identity.instance_id,
        run_id: chunk.identity.run_id,
        batch_id: chunk.identity.batch_id,
        chunk_id: chunk.chunk_id,
        chunk_index: chunk.chunk_index,
        group_id: chunk.group_id,
        attempt: chunk.attempt,
        lease_epoch: chunk.lease_epoch,
        owner_epoch: chunk.owner_epoch,
        worker_id: LOCAL_BENCHMARK_FASTLANE_WORKER_ID.to_owned(),
        worker_build_id: LOCAL_BENCHMARK_FASTLANE_WORKER_BUILD_ID.to_owned(),
        lease_token: chunk.lease_token.unwrap_or_default(),
        occurred_at: Utc::now(),
        payload: ThroughputChunkReportPayload::ChunkFailed { error },
    }
}

fn local_benchmark_cancelled_report(
    chunk: LeasedChunkSnapshot,
    reason: String,
) -> ThroughputChunkReport {
    ThroughputChunkReport {
        report_id: Uuid::now_v7().to_string(),
        tenant_id: chunk.identity.tenant_id,
        instance_id: chunk.identity.instance_id,
        run_id: chunk.identity.run_id,
        batch_id: chunk.identity.batch_id,
        chunk_id: chunk.chunk_id,
        chunk_index: chunk.chunk_index,
        group_id: chunk.group_id,
        attempt: chunk.attempt,
        lease_epoch: chunk.lease_epoch,
        owner_epoch: chunk.owner_epoch,
        worker_id: LOCAL_BENCHMARK_FASTLANE_WORKER_ID.to_owned(),
        worker_build_id: LOCAL_BENCHMARK_FASTLANE_WORKER_BUILD_ID.to_owned(),
        lease_token: chunk.lease_token.unwrap_or_default(),
        occurred_at: Utc::now(),
        payload: ThroughputChunkReportPayload::ChunkCancelled { reason, metadata: None },
    }
}

async fn run_group_barrier_reconciler(state: AppState) {
    let interval = Duration::from_millis(state.runtime.sweep_interval_ms.max(1));
    loop {
        let owned = owned_throughput_partitions(&state);
        if !owned.is_empty() {
            let candidates = match state
                .local_state
                .group_barrier_batches(Some(&owned), state.throughput_partitions)
            {
                Ok(candidates) => candidates,
                Err(error) => {
                    error!(error = %error, "failed to list grouped throughput batches from local state");
                    Vec::new()
                }
            };
            for identity in candidates {
                let Some(projected_terminal) = (match state
                    .local_state
                    .project_batch_terminal_from_groups(&identity, Utc::now())
                {
                    Ok(projected) => projected,
                    Err(error) => {
                        error!(error = %error, batch_id = %identity.batch_id, "failed to project grouped batch terminal from local state");
                        continue;
                    }
                }) else {
                    continue;
                };
                if let Err(error) =
                    finalize_grouped_stream_batch(&state, &identity, &projected_terminal).await
                {
                    error!(error = %error, batch_id = %identity.batch_id, "failed to finalize grouped throughput batch");
                }
            }
            if let Err(error) = repair_stale_terminal_projection_batches(&state, &owned).await {
                error!(error = %error, "failed to repair stale terminal throughput projection batches");
            }
            if let Err(error) = repair_missing_terminal_handoffs(&state, &owned).await {
                error!(error = %error, "failed to repair missing throughput terminal handoffs");
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = state.bulk_notify.notified() => {}
        }
    }
}

#[tonic::async_trait]
impl ActivityWorkerApi for WorkerApi {
    async fn poll_activity_task(
        &self,
        _request: Request<fabrik_worker_protocol::activity_worker::PollActivityTaskRequest>,
    ) -> Result<Response<fabrik_worker_protocol::activity_worker::PollActivityTaskResponse>, Status>
    {
        Err(Status::unimplemented("throughput-runtime only serves bulk tasks"))
    }

    async fn poll_activity_tasks(
        &self,
        _request: Request<fabrik_worker_protocol::activity_worker::PollActivityTasksRequest>,
    ) -> Result<Response<fabrik_worker_protocol::activity_worker::PollActivityTasksResponse>, Status>
    {
        Err(Status::unimplemented("throughput-runtime only serves bulk tasks"))
    }

    async fn poll_bulk_activity_task(
        &self,
        request: Request<PollBulkActivityTaskRequest>,
    ) -> Result<Response<PollBulkActivityTaskResponse>, Status> {
        let request = request.into_inner();
        let max_tasks = usize::try_from(request.max_tasks.max(1))
            .unwrap_or(1)
            .min(self.state.runtime.poll_max_tasks.max(1));
        {
            let mut debug = self.state.debug.lock().expect("throughput debug lock poisoned");
            debug.poll_requests = debug.poll_requests.saturating_add(1);
            debug.poll_responses = debug.poll_responses.saturating_add(1);
        }
        let batch_limit = (self.state.runtime.max_active_chunks_per_batch > 0)
            .then_some(self.state.runtime.max_active_chunks_per_batch);
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(request.poll_timeout_ms.max(1));
        loop {
            let owned = owned_throughput_partitions(&self.state);
            if owned.is_empty() {
                if tokio::time::Instant::now() >= deadline {
                    return Ok(Response::new(PollBulkActivityTaskResponse { tasks: Vec::new() }));
                }
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if tokio::time::timeout(remaining, self.state.bulk_notify.notified()).await.is_err()
                {
                    return Ok(Response::new(PollBulkActivityTaskResponse { tasks: Vec::new() }));
                }
                continue;
            }
            let queue_control = self
                .state
                .store
                .get_task_queue_runtime_control(
                    &request.tenant_id,
                    TaskQueueKind::Activity,
                    &request.task_queue,
                )
                .await
                .map_err(internal_status)?;
            if let Some(control) = queue_control.as_ref() {
                if control.is_paused {
                    record_throttle(&self.state, "queue_paused");
                    if tokio::time::Instant::now() >= deadline {
                        return Ok(Response::new(PollBulkActivityTaskResponse {
                            tasks: Vec::new(),
                        }));
                    }
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if tokio::time::timeout(remaining, self.state.bulk_notify.notified())
                        .await
                        .is_err()
                    {
                        return Ok(Response::new(PollBulkActivityTaskResponse {
                            tasks: Vec::new(),
                        }));
                    }
                    continue;
                }
                if control.is_draining {
                    record_throttle(&self.state, "queue_draining");
                    if tokio::time::Instant::now() >= deadline {
                        return Ok(Response::new(PollBulkActivityTaskResponse {
                            tasks: Vec::new(),
                        }));
                    }
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if tokio::time::timeout(remaining, self.state.bulk_notify.notified())
                        .await
                        .is_err()
                    {
                        return Ok(Response::new(PollBulkActivityTaskResponse {
                            tasks: Vec::new(),
                        }));
                    }
                    continue;
                }
            }
            let paused_batch_ids = self
                .state
                .store
                .list_paused_bulk_batch_ids_for_task_queue(&request.tenant_id, &request.task_queue)
                .await
                .map_err(internal_status)?
                .into_iter()
                .collect::<HashSet<_>>();
            let tenant_active = self
                .state
                .local_state
                .count_started_chunks(
                    Some(&request.tenant_id),
                    None,
                    None,
                    Some(&owned),
                    self.state.throughput_partitions,
                )
                .map_err(internal_status)?;
            if self.state.runtime.max_active_chunks_per_tenant > 0
                && tenant_active
                    >= u64::try_from(self.state.runtime.max_active_chunks_per_tenant)
                        .map_err(|_| Status::internal("tenant chunk cap exceeds u64"))?
            {
                record_throttle(&self.state, "tenant_cap");
                if tokio::time::Instant::now() >= deadline {
                    return Ok(Response::new(PollBulkActivityTaskResponse { tasks: Vec::new() }));
                }
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if tokio::time::timeout(remaining, self.state.bulk_notify.notified()).await.is_err()
                {
                    return Ok(Response::new(PollBulkActivityTaskResponse { tasks: Vec::new() }));
                }
                continue;
            }
            let queue_active = self
                .state
                .local_state
                .count_started_chunks(
                    Some(&request.tenant_id),
                    Some(&request.task_queue),
                    None,
                    Some(&owned),
                    self.state.throughput_partitions,
                )
                .map_err(internal_status)?;
            if self.state.runtime.max_active_chunks_per_task_queue > 0
                && queue_active
                    >= u64::try_from(self.state.runtime.max_active_chunks_per_task_queue)
                        .map_err(|_| Status::internal("task queue chunk cap exceeds u64"))?
            {
                record_throttle(&self.state, "task_queue_cap");
                if tokio::time::Instant::now() >= deadline {
                    return Ok(Response::new(PollBulkActivityTaskResponse { tasks: Vec::new() }));
                }
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if tokio::time::timeout(remaining, self.state.bulk_notify.notified()).await.is_err()
                {
                    return Ok(Response::new(PollBulkActivityTaskResponse { tasks: Vec::new() }));
                }
                continue;
            }
            let now = Utc::now();
            let leased_local = self
                .state
                .local_state
                .lease_ready_chunks(
                    &request.tenant_id,
                    &request.task_queue,
                    &request.worker_id,
                    now,
                    chrono::Duration::seconds(self.state.runtime.lease_ttl_seconds as i64),
                    batch_limit,
                    &paused_batch_ids,
                    Some(&owned),
                    self.state.throughput_partitions,
                    max_tasks,
                )
                .map_err(internal_status)?;
            if !leased_local.is_empty() {
                let mut leased_tasks = Vec::with_capacity(leased_local.len());
                let mut projection_records = Vec::with_capacity(max_tasks * 2);
                let mut changelog_records = Vec::with_capacity(max_tasks);
                let mut skipped_projection_events = 0_u64;
                for leased_local in leased_local {
                    let leased = leased_snapshot_to_projection_chunk(
                        &leased_local,
                        &request.worker_build_id,
                    )
                    .map_err(internal_status)?;
                    let identity = batch_identity(&leased);
                    if let Some(batch) = self
                        .state
                        .local_state
                        .mark_batch_running(&identity, leased.updated_at)
                        .map_err(internal_status)?
                    {
                        if self.state.runtime.publish_transient_projection_updates {
                            projection_records.push(running_batch_projection_record(&batch));
                        } else {
                            skipped_projection_events = skipped_projection_events.saturating_add(1);
                        }
                    }
                    if self.state.runtime.publish_transient_projection_updates {
                        projection_records.push(leased_chunk_projection_record(&leased));
                    } else {
                        skipped_projection_events = skipped_projection_events.saturating_add(1);
                    }
                    if !self.state.runtime.owner_first_apply {
                        changelog_records.push(changelog_record(
                            throughput_partition_key(&leased.batch_id, leased.group_id),
                            ThroughputChangelogPayload::ChunkLeased {
                                identity,
                                chunk_id: leased.chunk_id.clone(),
                                chunk_index: leased.chunk_index,
                                attempt: leased.attempt,
                                group_id: leased.group_id,
                                item_count: leased.item_count,
                                max_attempts: leased.max_attempts,
                                retry_delay_ms: leased.retry_delay_ms,
                                lease_epoch: leased.lease_epoch,
                                owner_epoch: leased.owner_epoch,
                                worker_id: request.worker_id.clone(),
                                lease_token: leased
                                    .lease_token
                                    .map(|value| value.to_string())
                                    .unwrap_or_default(),
                                lease_expires_at: leased.lease_expires_at.unwrap_or_else(Utc::now),
                            },
                        ));
                    }
                    leased_tasks.push(bulk_chunk_to_proto(
                        &leased_local,
                        request.supports_cbor,
                        self.state.runtime.inline_chunk_input_threshold_bytes,
                    ));
                }
                let lease_projection_result =
                    publish_projection_records(&self.state, projection_records).await;
                if let Err(error) = lease_projection_result {
                    error!(error = %error, "failed to sync leased stream-v2 projection state");
                }
                if !self.state.runtime.owner_first_apply {
                    publish_changelog_records(&self.state, changelog_records).await;
                }
                {
                    let mut debug =
                        self.state.debug.lock().expect("throughput debug lock poisoned");
                    debug.projection_events_skipped =
                        debug.projection_events_skipped.saturating_add(skipped_projection_events);
                    debug.leased_tasks =
                        debug.leased_tasks.saturating_add(leased_tasks.len() as u64);
                }
                return Ok(Response::new(PollBulkActivityTaskResponse { tasks: leased_tasks }));
            }
            {
                let mut debug = self.state.debug.lock().expect("throughput debug lock poisoned");
                debug.empty_polls = debug.empty_polls.saturating_add(1);
            }
            if tokio::time::Instant::now() >= deadline {
                let lease_selection_debug = self
                    .state
                    .local_state
                    .debug_lease_selection(
                        &request.tenant_id,
                        &request.task_queue,
                        Utc::now(),
                        batch_limit,
                        &paused_batch_ids,
                        Some(&owned),
                        self.state.throughput_partitions,
                    )
                    .map_err(internal_status)?;
                if batch_limit.is_some() && lease_selection_debug.batch_cap_blocked > 0 {
                    record_throttle(&self.state, "batch_cap");
                    let mut debug =
                        self.state.debug.lock().expect("throughput debug lock poisoned");
                    debug.lease_misses_with_ready_chunks =
                        debug.lease_misses_with_ready_chunks.saturating_add(1);
                    debug.last_lease_miss_with_ready_chunks_at = Some(Utc::now());
                    debug.last_lease_selection_debug =
                        serde_json::to_value(lease_selection_debug).ok();
                } else if lease_selection_debug.batch_paused > 0 {
                    record_throttle(&self.state, "batch_paused");
                    let mut debug =
                        self.state.debug.lock().expect("throughput debug lock poisoned");
                    debug.last_lease_selection_debug =
                        serde_json::to_value(lease_selection_debug).ok();
                } else {
                    let mut debug =
                        self.state.debug.lock().expect("throughput debug lock poisoned");
                    debug.last_lease_selection_debug =
                        serde_json::to_value(lease_selection_debug).ok();
                }
                return Ok(Response::new(PollBulkActivityTaskResponse { tasks: Vec::new() }));
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if tokio::time::timeout(remaining, self.state.bulk_notify.notified()).await.is_err() {
                return Ok(Response::new(PollBulkActivityTaskResponse { tasks: Vec::new() }));
            }
        }
    }

    async fn complete_activity_task(
        &self,
        _request: Request<fabrik_worker_protocol::activity_worker::CompleteActivityTaskRequest>,
    ) -> Result<Response<Ack>, Status> {
        Err(Status::unimplemented("throughput-runtime only serves bulk tasks"))
    }

    async fn fail_activity_task(
        &self,
        _request: Request<fabrik_worker_protocol::activity_worker::FailActivityTaskRequest>,
    ) -> Result<Response<Ack>, Status> {
        Err(Status::unimplemented("throughput-runtime only serves bulk tasks"))
    }

    async fn record_activity_heartbeat(
        &self,
        _request: Request<fabrik_worker_protocol::activity_worker::RecordActivityHeartbeatRequest>,
    ) -> Result<
        Response<fabrik_worker_protocol::activity_worker::RecordActivityHeartbeatResponse>,
        Status,
    > {
        Err(Status::unimplemented("throughput-runtime only serves bulk tasks"))
    }

    async fn report_activity_task_cancelled(
        &self,
        _request: Request<
            fabrik_worker_protocol::activity_worker::ReportActivityTaskCancelledRequest,
        >,
    ) -> Result<Response<Ack>, Status> {
        Err(Status::unimplemented("throughput-runtime only serves bulk tasks"))
    }

    async fn report_activity_task_results(
        &self,
        _request: Request<
            fabrik_worker_protocol::activity_worker::ReportActivityTaskResultsRequest,
        >,
    ) -> Result<Response<Ack>, Status> {
        Err(Status::unimplemented("throughput-runtime only serves bulk tasks"))
    }

    async fn report_bulk_activity_task_results(
        &self,
        request: Request<ReportBulkActivityTaskResultsRequest>,
    ) -> Result<Response<Ack>, Status> {
        let mut reports = Vec::new();
        for result in request.into_inner().results {
            reports.push(throughput_report_from_result(result)?);
        }
        {
            let mut debug = self.state.debug.lock().expect("throughput debug lock poisoned");
            debug.report_rpcs_received = debug.report_rpcs_received.saturating_add(1);
            debug.reports_received = debug.reports_received.saturating_add(reports.len() as u64);
            debug.last_report_at = Some(Utc::now());
        }

        for report_batch in reports.chunks(self.state.runtime.report_apply_batch_size.max(1)) {
            {
                let mut debug = self.state.debug.lock().expect("throughput debug lock poisoned");
                debug.report_batches_applied = debug.report_batches_applied.saturating_add(1);
                debug.report_batch_items_total =
                    debug.report_batch_items_total.saturating_add(report_batch.len() as u64);
            }
            apply_throughput_report_batch(&self.state, report_batch, true)
                .await
                .map_err(internal_status)?;
        }
        Ok(Response::new(Ack {}))
    }
}

async fn apply_throughput_report_batch(
    state: &AppState,
    report_batch: &[ThroughputChunkReport],
    capture_report_log: bool,
) -> Result<()> {
    struct AcceptedReportApply {
        report: ThroughputChunkReport,
        identity: ThroughputBatchIdentity,
        projected: ProjectedTerminalApply,
    }

    let mut accepted_reports = Vec::with_capacity(report_batch.len());
    let mut terminal_handoff_candidates = BTreeSet::new();
    let mut result_segments = Vec::new();
    let mut skipped_projection_events = 0_u64;
    for report in report_batch {
        let throughput_partition_id = throughput_partition_for_batch(
            &report.batch_id,
            report.group_id,
            state.throughput_partitions,
        );
        if !owns_throughput_partition(state, throughput_partition_id) {
            anyhow::bail!(
                "throughput partition {} is not owned by this runtime",
                throughput_partition_id
            );
        }
        let identity = ThroughputBatchIdentity {
            tenant_id: report.tenant_id.clone(),
            instance_id: report.instance_id.clone(),
            run_id: report.run_id.clone(),
            batch_id: report.batch_id.clone(),
        };
        let projected = match state.local_state.prepare_report_apply(report)? {
            PreparedReportApply::MissingBatch => {
                anyhow::bail!("bulk batch {} is not owned by this runtime", report.batch_id);
            }
            PreparedReportApply::Rejected(validation) => {
                let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
                debug.reports_rejected = debug.reports_rejected.saturating_add(1);
                debug.last_report_rejection_reason =
                    Some(report_validation_label(validation).to_owned());
                continue;
            }
            PreparedReportApply::Accepted(projected) => projected,
        };
        let (report, segment) = materialize_collect_results_segment(
            state,
            report,
            projected.batch_reducer.as_deref(),
            projected.chunk_item_count,
        )
        .await?;
        if let Some(segment) = segment {
            result_segments.push(segment);
        }
        accepted_reports.push(AcceptedReportApply { report, identity, projected });
    }
    if !result_segments.is_empty() {
        state.store.upsert_throughput_result_segments(&result_segments).await?;
    }
    if capture_report_log && state.runtime.owner_first_apply {
        let captured =
            accepted_reports.iter().map(|accepted| accepted.report.clone()).collect::<Vec<_>>();
        state.store.append_throughput_report_log_entries(&captured, Utc::now()).await?;
    }
    let mut projection_records = Vec::with_capacity(report_batch.len() * 2);
    for accepted in accepted_reports {
        let report = &accepted.report;
        let identity = &accepted.identity;
        let projected = &accepted.projected;
        let terminal_artifacts =
            materialize_chunk_terminal_artifacts(state, report, projected.batch_reducer.as_deref())
                .await?;
        let batch_reducer_output = reduce_stream_batch_output(
            state,
            identity,
            projected.batch_reducer.as_deref(),
            Some(report),
        )?;
        let chunk_output = completed_chunk_output(report);
        projection_records.extend(projected_apply_projection_records(
            report,
            &projected,
            &terminal_artifacts,
            chunk_output.clone(),
            batch_reducer_output.clone(),
        ));
        let mut changelog_records = vec![changelog_record(
            throughput_partition_key(&report.batch_id, report.group_id),
            ThroughputChangelogPayload::ChunkApplied {
                identity: identity.clone(),
                chunk_id: projected.chunk_id.clone(),
                chunk_index: report.chunk_index,
                attempt: projected.chunk_attempt,
                group_id: report.group_id,
                item_count: projected.chunk_item_count,
                max_attempts: projected.chunk_max_attempts,
                retry_delay_ms: projected.chunk_retry_delay_ms,
                lease_epoch: report.lease_epoch,
                owner_epoch: report.owner_epoch,
                report_id: report.report_id.clone(),
                status: projected.chunk_status.clone(),
                available_at: projected.chunk_available_at,
                result_handle: terminal_artifacts.result_handle.clone(),
                output: chunk_output,
                error: projected.chunk_error.clone(),
                cancellation_reason: terminal_artifacts.cancellation_reason.clone(),
                cancellation_metadata: terminal_artifacts.cancellation_metadata.clone(),
            },
        )];
        if let Some(group_terminal) = projected.group_terminal.as_ref() {
            record_group_terminal_published(state, group_terminal);
            changelog_records.push(group_terminal_changelog_record(&identity, group_terminal));
            for parent_group in &projected.parent_group_terminals {
                record_group_terminal_published(state, parent_group);
                changelog_records.push(group_terminal_changelog_record(&identity, parent_group));
            }
            if let Some(batch_terminal) = projected.grouped_batch_terminal.as_ref() {
                projection_records.push(grouped_batch_terminal_projection_record(
                    &identity,
                    batch_terminal,
                    batch_reducer_output.clone(),
                ));
                changelog_records
                    .push(grouped_batch_terminal_changelog_record(identity, batch_terminal));
            } else if let Some(summary) = projected.grouped_batch_summary.as_ref() {
                if state.runtime.publish_transient_projection_updates {
                    projection_records.push(grouped_batch_summary_projection_record(
                        identity,
                        summary,
                        batch_reducer_output.clone(),
                    ));
                } else {
                    skipped_projection_events = skipped_projection_events.saturating_add(1);
                }
            }
        }
        if projected.batch_terminal && !state.runtime.owner_first_apply {
            changelog_records.push(changelog_record(
                throughput_partition_key(&report.batch_id, report.group_id),
                ThroughputChangelogPayload::BatchTerminal {
                    identity: identity.clone(),
                    status: projected.batch_status.clone(),
                    report_id: report.report_id.clone(),
                    succeeded_items: projected.batch_succeeded_items,
                    failed_items: projected.batch_failed_items,
                    cancelled_items: projected.batch_cancelled_items,
                    error: projected.batch_error.clone(),
                    terminal_at: projected.terminal_at.unwrap_or(report.occurred_at),
                },
            ));
        }
        if state.runtime.owner_first_apply {
            let staged_entries =
                changelog_records.iter().map(|record| record.entry.clone()).collect::<Vec<_>>();
            state.local_state.mirror_changelog_records(&staged_entries)?;
        }
        if !state.runtime.owner_first_apply {
            publish_changelog_records(state, changelog_records).await;
        }
        if let Some(batch_terminal) = projected.grouped_batch_terminal.as_ref() {
            publish_grouped_batch_terminal_event(state, report, &projected, batch_terminal).await?;
            let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
            debug.terminal_events_published = debug.terminal_events_published.saturating_add(1);
            debug.last_terminal_event_at = Some(Utc::now());
        }
        if projected.batch_terminal {
            publish_stream_terminal_event(state, report, &projected).await?;
            let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
            debug.terminal_events_published = debug.terminal_events_published.saturating_add(1);
            debug.last_terminal_event_at = Some(Utc::now());
        }
        if projected.chunk_status == WorkflowBulkChunkStatus::Scheduled.as_str()
            || projected.batch_terminal_deferred
        {
            state.bulk_notify.notify_waiters();
        }
        terminal_handoff_candidates.insert((
            identity.tenant_id.clone(),
            identity.instance_id.clone(),
            identity.run_id.clone(),
            identity.batch_id.clone(),
        ));
    }
    publish_projection_records(state, projection_records).await?;
    for (tenant_id, instance_id, run_id, batch_id) in terminal_handoff_candidates {
        if publish_projection_batch_terminal_handoff(
            state,
            &tenant_id,
            &instance_id,
            &run_id,
            &batch_id,
        )
        .await?
        {
            let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
            debug.terminal_events_published = debug.terminal_events_published.saturating_add(1);
            debug.last_terminal_event_at = Some(Utc::now());
        }
    }
    if skipped_projection_events > 0 {
        let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
        debug.projection_events_skipped =
            debug.projection_events_skipped.saturating_add(skipped_projection_events);
    }
    Ok(())
}

async fn ensure_bulk_batch_backend(
    store: &WorkflowStore,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
    expected_backend: &str,
) -> Result<(), Status> {
    let batch = store
        .get_bulk_batch_query_view(tenant_id, instance_id, run_id, batch_id)
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::not_found(format!("bulk batch {batch_id} not found")))?;
    if batch.throughput_backend != expected_backend {
        return Err(Status::failed_precondition(format!(
            "bulk batch {batch_id} is owned by backend {}",
            batch.throughput_backend
        )));
    }
    Ok(())
}

async fn build_stream_projection_records_from_parts(
    app_state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    definition_id: &str,
    definition_version: Option<u32>,
    artifact_hash: Option<&str>,
    batch_id: &str,
    activity_type: &str,
    activity_capabilities: Option<&ActivityExecutionCapabilities>,
    task_queue: &str,
    workflow_state: Option<&str>,
    items: Vec<Value>,
    total_items_override: Option<usize>,
    batch_input_handle: PayloadHandle,
    default_result_handle: Value,
    chunk_size: u32,
    max_attempts: u32,
    retry_delay_ms: u64,
    aggregation_group_count: u32,
    execution_policy: Option<&str>,
    reducer: Option<&str>,
    throughput_backend: &str,
    throughput_backend_version: &str,
    routing_reason: &str,
    admission_policy_version: &str,
    occurred_at: chrono::DateTime<chrono::Utc>,
    materialize_inline_chunk_items: bool,
) -> Result<(fabrik_store::WorkflowBulkBatchRecord, Vec<WorkflowBulkChunkRecord>)> {
    let chunk_size_usize =
        usize::try_from(chunk_size.max(1)).context("bulk chunk size exceeds usize")?;
    let total_items = total_items_override.unwrap_or(items.len());
    let resolved_activity_capabilities = resolve_activity_capabilities(
        Some(app_state.activity_capability_registry.as_ref()),
        task_queue,
        activity_type,
        activity_capabilities,
    );
    let chunk_count = if total_items == 0 {
        0
    } else {
        ((total_items + chunk_size_usize - 1) / chunk_size_usize) as u32
    };
    let fast_lane_enabled =
        stream_v2_fast_lane_enabled(throughput_backend, execution_policy, reducer);
    let reducer_class = bulk_reducer_class(reducer);
    let effective_group_count = planned_aggregation_group_count(
        aggregation_group_count,
        chunk_count,
        fast_lane_enabled,
        &app_state.runtime,
    );
    let aggregation_tree_depth =
        planned_reduction_tree_depth(chunk_count, effective_group_count, reducer);
    let batch_result_handle = maybe_initialize_batch_result_manifest(
        app_state,
        batch_id,
        &default_result_handle,
        reducer,
    )
    .await?;
    let payloadless_chunk_transport = can_use_payloadless_bulk_transport(
        activity_type,
        resolved_activity_capabilities.as_ref(),
        reducer,
        max_attempts,
        &items,
    );
    let uses_compact_chunk_input_handle = !materialize_inline_chunk_items
        && !payloadless_chunk_transport
        && parse_benchmark_compact_total_items_from_handle(&batch_input_handle).is_some();
    let compact_chunk_input_handle = if uses_compact_chunk_input_handle {
        Some(
            serde_json::to_value(&batch_input_handle)
                .context("failed to serialize compact stream-v2 chunk input handle")?,
        )
    } else {
        None
    };
    let batch = fabrik_store::WorkflowBulkBatchRecord {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        definition_id: definition_id.to_owned(),
        definition_version,
        artifact_hash: artifact_hash.map(str::to_owned),
        batch_id: batch_id.to_owned(),
        activity_type: activity_type.to_owned(),
        activity_capabilities: resolved_activity_capabilities.clone(),
        task_queue: task_queue.to_owned(),
        state: workflow_state.map(str::to_owned),
        input_handle: serde_json::to_value(&batch_input_handle)
            .context("failed to serialize batch input handle")?,
        result_handle: batch_result_handle,
        throughput_backend: throughput_backend.to_owned(),
        throughput_backend_version: throughput_backend_version.to_owned(),
        execution_mode: throughput_execution_mode(throughput_backend).to_owned(),
        selected_backend: throughput_backend.to_owned(),
        routing_reason: routing_reason.to_owned(),
        admission_policy_version: admission_policy_version.to_owned(),
        execution_policy: execution_policy.map(str::to_owned),
        reducer: reducer.map(str::to_owned),
        reducer_kind: bulk_reducer_name(reducer).to_owned(),
        reducer_class: reducer_class.as_str().to_owned(),
        reducer_version: throughput_reducer_version(reducer).to_owned(),
        reducer_execution_path: throughput_reducer_execution_path(
            throughput_backend,
            execution_policy,
            reducer,
        )
        .to_owned(),
        aggregation_tree_depth,
        fast_lane_enabled,
        aggregation_group_count: effective_group_count,
        status: WorkflowBulkBatchStatus::Scheduled,
        total_items: total_items as u32,
        chunk_size,
        chunk_count,
        succeeded_items: 0,
        failed_items: 0,
        cancelled_items: 0,
        max_attempts,
        retry_delay_ms,
        error: None,
        reducer_output: None,
        scheduled_at: occurred_at,
        terminal_at: None,
        updated_at: occurred_at,
    };
    let payload_key = (!materialize_inline_chunk_items
        && !payloadless_chunk_transport
        && !uses_compact_chunk_input_handle)
        .then(|| payload_handle_key(&batch_input_handle).map(str::to_owned))
        .transpose()?;
    let payload_store = (!materialize_inline_chunk_items
        && !payloadless_chunk_transport
        && !uses_compact_chunk_input_handle)
        .then(|| payload_handle_store(&batch_input_handle).map(str::to_owned))
        .transpose()?;
    let mut chunks = Vec::with_capacity(usize::try_from(chunk_count).unwrap_or_default());
    for chunk_index in 0..chunk_count {
        let chunk_index_usize = usize::try_from(chunk_index).unwrap_or_default();
        let chunk_id = format!("{batch_id}::{chunk_index}");
        let group_id = group_id_for_chunk_index(chunk_index, effective_group_count);
        let chunk_start = chunk_index_usize.saturating_mul(chunk_size_usize);
        let item_count = total_items.saturating_sub(chunk_start).min(chunk_size_usize);
        let chunk_items = if payloadless_chunk_transport || !materialize_inline_chunk_items {
            &[][..]
        } else {
            &items[chunk_start..chunk_start + item_count]
        };
        let input_handle = if materialize_inline_chunk_items || payloadless_chunk_transport {
            Value::Null
        } else if let Some(handle) = compact_chunk_input_handle.as_ref() {
            handle.clone()
        } else {
            serde_json::to_value(PayloadHandle::ManifestSlice {
                key: payload_key.clone().expect("stream batch payload key available"),
                store: payload_store.clone().expect("stream batch payload store available"),
                start: chunk_start,
                len: item_count,
            })
            .context("failed to serialize chunk input handle")?
        };
        chunks.push(WorkflowBulkChunkRecord {
            tenant_id: tenant_id.to_owned(),
            instance_id: instance_id.to_owned(),
            run_id: run_id.to_owned(),
            definition_id: definition_id.to_owned(),
            definition_version,
            artifact_hash: artifact_hash.map(str::to_owned),
            batch_id: batch_id.to_owned(),
            chunk_id,
            chunk_index,
            group_id,
            item_count: item_count as u32,
            activity_type: activity_type.to_owned(),
            task_queue: task_queue.to_owned(),
            state: workflow_state.map(str::to_owned),
            status: WorkflowBulkChunkStatus::Scheduled,
            attempt: 1,
            lease_epoch: 0,
            owner_epoch: 1,
            max_attempts,
            retry_delay_ms,
            input_handle,
            result_handle: Value::Null,
            items: if materialize_inline_chunk_items && !payloadless_chunk_transport {
                chunk_items.to_vec()
            } else {
                Vec::new()
            },
            output: None,
            error: None,
            cancellation_requested: false,
            cancellation_reason: None,
            cancellation_metadata: None,
            worker_id: None,
            worker_build_id: None,
            lease_token: None,
            last_report_id: None,
            scheduled_at: occurred_at,
            available_at: occurred_at,
            started_at: None,
            lease_expires_at: None,
            completed_at: None,
            updated_at: occurred_at,
        });
    }
    Ok((batch, chunks))
}

async fn build_stream_projection_records(
    app_state: &AppState,
    event: &EventEnvelope<WorkflowEvent>,
) -> Result<(fabrik_store::WorkflowBulkBatchRecord, Vec<WorkflowBulkChunkRecord>)> {
    let WorkflowEvent::BulkActivityBatchScheduled {
        batch_id,
        activity_type,
        activity_capabilities,
        task_queue,
        items,
        input_handle,
        result_handle,
        chunk_size,
        max_attempts,
        retry_delay_ms,
        aggregation_group_count,
        execution_policy,
        reducer,
        throughput_backend,
        throughput_backend_version,
        state,
        ..
    } = &event.payload
    else {
        anyhow::bail!("expected BulkActivityBatchScheduled event");
    };

    let parsed_input_handle = serde_json::from_value::<PayloadHandle>(input_handle.clone())
        .context("failed to decode stream batch input handle")?;
    let compact_total_items = if items.is_empty() {
        parse_benchmark_compact_total_items_from_handle(&parsed_input_handle)
            .map(|count| count as usize)
    } else {
        None
    };
    let (items, batch_input_handle) = if compact_total_items.is_some() {
        (Vec::new(), parsed_input_handle)
    } else {
        resolve_stream_batch_input(&app_state.payload_store, batch_id, items, input_handle).await?
    };
    let routing_reason = event.metadata.get("routing_reason").cloned().unwrap_or_else(|| {
        throughput_routing_reason(
            throughput_backend,
            execution_policy.as_deref(),
            reducer.as_deref(),
        )
        .to_owned()
    });
    let admission_policy_version = event
        .metadata
        .get("admission_policy_version")
        .cloned()
        .unwrap_or_else(|| ADMISSION_POLICY_VERSION.to_owned());

    build_stream_projection_records_from_parts(
        app_state,
        &event.tenant_id,
        &event.instance_id,
        &event.run_id,
        &event.definition_id,
        Some(event.definition_version),
        Some(event.artifact_hash.as_str()),
        batch_id,
        activity_type,
        activity_capabilities.as_ref(),
        task_queue,
        state.as_deref(),
        items,
        compact_total_items,
        batch_input_handle,
        result_handle.clone(),
        *chunk_size,
        *max_attempts,
        *retry_delay_ms,
        *aggregation_group_count,
        execution_policy.as_deref(),
        reducer.as_deref(),
        throughput_backend,
        throughput_backend_version,
        &routing_reason,
        &admission_policy_version,
        event.occurred_at,
        false,
    )
    .await
}

async fn load_native_stream_run_items(
    state: &AppState,
    command: &StartThroughputRunCommand,
) -> Result<Vec<Value>> {
    match &command.input_handle {
        handle if parse_benchmark_compact_total_items_from_handle(handle).is_some() => {
            Ok(Vec::new())
        }
        PayloadHandle::Manifest { .. } | PayloadHandle::ManifestSlice { .. } => {
            state.payload_store.read_items(&command.input_handle).await.with_context(|| {
                format!(
                    "failed to read native stream-v2 payload handle for batch {}",
                    command.batch_id
                )
            })
        }
        PayloadHandle::Inline { key } if key.starts_with("throughput-run-input:") => {
            let input = state
                .store
                .get_throughput_run_input(
                    &command.tenant_id,
                    &command.instance_id,
                    &command.run_id,
                    &command.batch_id,
                )
                .await?
                .with_context(|| {
                    format!(
                        "missing native stream-v2 throughput_run_inputs row for batch {}",
                        command.batch_id
                    )
                })?;
            Ok(input.items)
        }
        PayloadHandle::Inline { key } => {
            anyhow::bail!("unsupported native stream-v2 inline input handle key {key}")
        }
    }
}

async fn build_stream_projection_records_from_start_command(
    app_state: &AppState,
    command: &StartThroughputRunCommand,
    occurred_at: chrono::DateTime<chrono::Utc>,
) -> Result<(fabrik_store::WorkflowBulkBatchRecord, Vec<WorkflowBulkChunkRecord>)> {
    let compact_count_only_input =
        parse_benchmark_compact_total_items_from_handle(&command.input_handle).is_some();
    let items = load_native_stream_run_items(app_state, command).await?;
    build_stream_projection_records_from_parts(
        app_state,
        &command.tenant_id,
        &command.instance_id,
        &command.run_id,
        &command.definition_id,
        command.definition_version,
        command.artifact_hash.as_deref(),
        &command.batch_id,
        &command.activity_type,
        command.activity_capabilities.as_ref(),
        &command.task_queue,
        command.state.as_deref(),
        items,
        compact_count_only_input.then_some(command.total_items as usize),
        command.input_handle.clone(),
        serde_json::to_value(&command.result_handle)
            .context("failed to serialize native stream-v2 result handle")?,
        command.chunk_size,
        command.max_attempts,
        command.retry_delay_ms,
        command.aggregation_group_count,
        command.execution_policy.as_deref(),
        command.reducer.as_deref(),
        &command.throughput_backend,
        &command.throughput_backend_version,
        &command.routing_reason,
        &command.admission_policy_version,
        occurred_at,
        !compact_count_only_input,
    )
    .await
}

fn workflow_event_from_throughput_command(
    envelope: &ThroughputCommandEnvelope,
) -> Result<EventEnvelope<WorkflowEvent>> {
    let command = match &envelope.payload {
        ThroughputCommand::CreateBatch(command) => command,
        ThroughputCommand::StartThroughputRun(_) => {
            anyhow::bail!(
                "native start-throughput-run command cannot be translated to workflow event"
            )
        }
        other => anyhow::bail!("unsupported throughput command for activation: {other:?}"),
    };
    let payload = WorkflowEvent::BulkActivityBatchScheduled {
        batch_id: command.batch_id.clone(),
        activity_type: command.activity_type.clone(),
        activity_capabilities: command.activity_capabilities.clone(),
        task_queue: command.task_queue.clone(),
        items: command.items.clone(),
        input_handle: serde_json::to_value(&command.input_handle)
            .context("failed to serialize throughput command input handle")?,
        result_handle: serde_json::to_value(&command.result_handle)
            .context("failed to serialize throughput command result handle")?,
        chunk_size: command.chunk_size,
        max_attempts: command.max_attempts,
        retry_delay_ms: command.retry_delay_ms,
        aggregation_group_count: command.aggregation_group_count,
        execution_policy: command.execution_policy.clone(),
        reducer: command.reducer.clone(),
        throughput_backend: command.throughput_backend.clone(),
        throughput_backend_version: command.throughput_backend_version.clone(),
        state: command.state.clone(),
    };
    let mut workflow_event = EventEnvelope::new(
        payload.event_type(),
        WorkflowIdentity::new(
            command.tenant_id.clone(),
            command.definition_id.clone(),
            command.definition_version,
            command.artifact_hash.clone(),
            command.instance_id.clone(),
            command.run_id.clone(),
            "unified-runtime",
        ),
        payload,
    );
    workflow_event.occurred_at = envelope.occurred_at;
    workflow_event.event_id = Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("throughput-command-batch:{}:{}", envelope.command_id, command.batch_id).as_bytes(),
    );
    workflow_event.dedupe_key = Some(command.dedupe_key.clone());
    workflow_event
        .metadata
        .insert("throughput_backend".to_owned(), command.throughput_backend.clone());
    workflow_event.metadata.insert("routing_reason".to_owned(), command.routing_reason.clone());
    workflow_event
        .metadata
        .insert("admission_policy_version".to_owned(), command.admission_policy_version.clone());
    Ok(workflow_event)
}

async fn activate_stream_batch(
    state: &AppState,
    event: &EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    let WorkflowEvent::BulkActivityBatchScheduled { batch_id, task_queue, .. } = &event.payload
    else {
        anyhow::bail!("activate_stream_batch expects BulkActivityBatchScheduled");
    };
    let (batch, chunks) =
        build_stream_projection_records(state, event).await.with_context(|| {
            format!("failed to build stream-v2 batch projection records for {batch_id}")
        })?;
    if let Err(error) = sync_stream_batch_projection(state, &batch).await {
        error!(error = %error, batch_id = %batch_id, "failed to project stream-v2 batch");
    }
    if let Err(error) = state.local_state.upsert_batch_with_chunks(&batch, &chunks) {
        error!(error = %error, batch_id = %batch_id, "failed to seed local stream-v2 batch state");
    }
    if !state.runtime.owner_first_apply {
        publish_changelog_records(
            state,
            vec![changelog_record(
                throughput_partition_key(batch_id, 0),
                ThroughputChangelogPayload::BatchCreated {
                    identity: ThroughputBatchIdentity {
                        tenant_id: event.tenant_id.clone(),
                        instance_id: event.instance_id.clone(),
                        run_id: event.run_id.clone(),
                        batch_id: batch_id.clone(),
                    },
                    task_queue: task_queue.clone(),
                    execution_policy: batch.execution_policy.clone(),
                    reducer: batch.reducer.clone(),
                    reducer_class: batch.reducer_class.clone(),
                    aggregation_tree_depth: batch.aggregation_tree_depth,
                    fast_lane_enabled: batch.fast_lane_enabled,
                    aggregation_group_count: batch.aggregation_group_count,
                    total_items: batch.total_items,
                    chunk_count: batch.chunk_count,
                },
            )],
        )
        .await;
    }
    let _ = state
        .store
        .mark_throughput_run_started(
            &event.tenant_id,
            &event.instance_id,
            &event.run_id,
            batch_id,
            Utc::now(),
        )
        .await;
    state.bulk_notify.notify_waiters();
    Ok(())
}

async fn activate_native_stream_run(
    state: &AppState,
    envelope: &ThroughputCommandEnvelope,
    command: &StartThroughputRunCommand,
) -> Result<()> {
    let identity = ThroughputBatchIdentity {
        tenant_id: command.tenant_id.clone(),
        instance_id: command.instance_id.clone(),
        run_id: command.run_id.clone(),
        batch_id: command.batch_id.clone(),
    };
    if state.local_state.batch_snapshot(&identity)?.is_some() {
        let _ = state
            .store
            .mark_throughput_run_started(
                &command.tenant_id,
                &command.instance_id,
                &command.run_id,
                &command.batch_id,
                Utc::now(),
            )
            .await;
        return Ok(());
    }

    let (batch, chunks) =
        build_stream_projection_records_from_start_command(state, command, envelope.occurred_at)
            .await
            .with_context(|| {
                format!("failed to build native stream-v2 records for {}", command.batch_id)
            })?;
    if let Err(error) = state.local_state.upsert_batch_with_chunks(&batch, &chunks) {
        error!(error = %error, batch_id = %command.batch_id, "failed to seed native stream-v2 batch state");
    }
    let projection_state = state.clone();
    let projection_batch = batch.clone();
    tokio::spawn(async move {
        if let Err(error) = sync_stream_batch_projection(&projection_state, &projection_batch).await
        {
            error!(
                error = %error,
                batch_id = %projection_batch.batch_id,
                "failed to sync native stream-v2 projection batch"
            );
        }
    });
    if !state.runtime.owner_first_apply {
        publish_changelog_records(
            state,
            vec![changelog_record(
                throughput_partition_key(&command.batch_id, 0),
                ThroughputChangelogPayload::BatchCreated {
                    identity,
                    task_queue: command.task_queue.clone(),
                    execution_policy: batch.execution_policy.clone(),
                    reducer: batch.reducer.clone(),
                    reducer_class: batch.reducer_class.clone(),
                    aggregation_tree_depth: batch.aggregation_tree_depth,
                    fast_lane_enabled: batch.fast_lane_enabled,
                    aggregation_group_count: batch.aggregation_group_count,
                    total_items: batch.total_items,
                    chunk_count: batch.chunk_count,
                },
            )],
        )
        .await;
    }
    let _ = state
        .store
        .mark_throughput_run_started(
            &command.tenant_id,
            &command.instance_id,
            &command.run_id,
            &command.batch_id,
            Utc::now(),
        )
        .await;
    let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
    debug.last_bridge_at = Some(Utc::now());
    drop(debug);
    state.bulk_notify.notify_waiters();
    Ok(())
}

fn leased_snapshot_to_projection_chunk(
    leased_local: &LeasedChunkSnapshot,
    worker_build_id: &str,
) -> Result<WorkflowBulkChunkRecord> {
    Ok(WorkflowBulkChunkRecord {
        tenant_id: leased_local.identity.tenant_id.clone(),
        instance_id: leased_local.identity.instance_id.clone(),
        run_id: leased_local.identity.run_id.clone(),
        definition_id: leased_local.definition_id.clone(),
        definition_version: leased_local.definition_version,
        artifact_hash: leased_local.artifact_hash.clone(),
        batch_id: leased_local.identity.batch_id.clone(),
        chunk_id: leased_local.chunk_id.clone(),
        chunk_index: leased_local.chunk_index,
        group_id: leased_local.group_id,
        item_count: leased_local.item_count,
        activity_type: leased_local.activity_type.clone(),
        task_queue: leased_local.task_queue.clone(),
        state: None,
        status: WorkflowBulkChunkStatus::Started,
        attempt: leased_local.attempt,
        lease_epoch: leased_local.lease_epoch,
        owner_epoch: leased_local.owner_epoch,
        max_attempts: leased_local.max_attempts,
        retry_delay_ms: leased_local.retry_delay_ms,
        input_handle: leased_local.input_handle.clone(),
        result_handle: Value::Null,
        items: leased_local.items.clone(),
        output: None,
        error: None,
        cancellation_requested: leased_local.cancellation_requested,
        cancellation_reason: None,
        cancellation_metadata: None,
        worker_id: leased_local.worker_id.clone(),
        worker_build_id: Some(worker_build_id.to_owned()),
        lease_token: leased_local
            .lease_token
            .as_deref()
            .map(Uuid::parse_str)
            .transpose()
            .context("failed to parse leased local throughput lease token")?,
        last_report_id: None,
        scheduled_at: leased_local.scheduled_at,
        available_at: leased_local.updated_at,
        started_at: leased_local.started_at,
        lease_expires_at: leased_local.lease_expires_at,
        completed_at: None,
        updated_at: leased_local.updated_at,
    })
}

fn planned_aggregation_group_count(
    requested_group_count: u32,
    chunk_count: u32,
    fast_lane_enabled: bool,
    config: &ThroughputRuntimeConfig,
) -> u32 {
    if chunk_count == 0 {
        return 1;
    }
    if requested_group_count > 1 {
        return effective_aggregation_group_count(requested_group_count, chunk_count);
    }
    let threshold = config.grouping_chunk_threshold.max(1);
    let target_chunks_per_group = if fast_lane_enabled {
        (config.target_chunks_per_group.max(2) / 2).max(1)
    } else {
        config.target_chunks_per_group.max(1)
    };
    if usize::try_from(chunk_count).unwrap_or_default() < threshold {
        return 1;
    }
    let target_groups = usize::try_from(chunk_count)
        .unwrap_or_default()
        .div_ceil(target_chunks_per_group)
        .max(1)
        .min(config.max_aggregation_groups.max(1));
    effective_aggregation_group_count(target_groups as u32, chunk_count)
}

fn reconstruct_missing_stream_chunks(
    batch: &WorkflowBulkBatchRecord,
    existing_chunks: Vec<WorkflowBulkChunkRecord>,
) -> Result<Vec<WorkflowBulkChunkRecord>> {
    let Some(chunk_size) = usize::try_from(batch.chunk_size).ok().filter(|value| *value > 0) else {
        return Ok(existing_chunks);
    };
    let input_handle = serde_json::from_value::<PayloadHandle>(batch.input_handle.clone())
        .context("failed to deserialize stream-v2 batch input handle")?;
    let (key, store) = match input_handle {
        PayloadHandle::Manifest { key, store }
        | PayloadHandle::ManifestSlice { key, store, .. } => (key, store),
        PayloadHandle::Inline { .. } => return Ok(existing_chunks),
    };
    let mut chunks_by_index = existing_chunks
        .into_iter()
        .map(|chunk| (chunk.chunk_index, chunk))
        .collect::<HashMap<_, _>>();
    let mut chunks = Vec::with_capacity(usize::try_from(batch.chunk_count).unwrap_or_default());
    for chunk_index in 0..batch.chunk_count {
        if let Some(chunk) = chunks_by_index.remove(&chunk_index) {
            chunks.push(chunk);
            continue;
        }
        let chunk_start =
            usize::try_from(chunk_index).unwrap_or_default().saturating_mul(chunk_size);
        let remaining_items =
            usize::try_from(batch.total_items).unwrap_or_default().saturating_sub(chunk_start);
        let item_count = remaining_items.min(chunk_size);
        chunks.push(WorkflowBulkChunkRecord {
            tenant_id: batch.tenant_id.clone(),
            instance_id: batch.instance_id.clone(),
            run_id: batch.run_id.clone(),
            definition_id: batch.definition_id.clone(),
            definition_version: batch.definition_version,
            artifact_hash: batch.artifact_hash.clone(),
            batch_id: batch.batch_id.clone(),
            chunk_id: format!("{}::{chunk_index}", batch.batch_id),
            chunk_index,
            group_id: group_id_for_chunk_index(chunk_index, batch.aggregation_group_count),
            item_count: item_count as u32,
            activity_type: batch.activity_type.clone(),
            task_queue: batch.task_queue.clone(),
            state: batch.state.clone(),
            status: WorkflowBulkChunkStatus::Scheduled,
            attempt: 1,
            lease_epoch: 0,
            owner_epoch: 1,
            max_attempts: batch.max_attempts,
            retry_delay_ms: batch.retry_delay_ms,
            input_handle: serde_json::to_value(PayloadHandle::ManifestSlice {
                key: key.clone(),
                store: store.clone(),
                start: chunk_start,
                len: item_count,
            })
            .context("failed to serialize reconstructed stream-v2 chunk input handle")?,
            result_handle: Value::Null,
            items: Vec::new(),
            output: None,
            error: None,
            cancellation_requested: false,
            cancellation_reason: None,
            cancellation_metadata: None,
            worker_id: None,
            worker_build_id: None,
            lease_token: None,
            last_report_id: None,
            scheduled_at: batch.scheduled_at,
            available_at: batch.scheduled_at,
            started_at: None,
            lease_expires_at: None,
            completed_at: None,
            updated_at: batch.updated_at,
        });
    }
    Ok(chunks)
}

async fn resolve_stream_batch_input(
    payload_store: &PayloadStore,
    batch_id: &str,
    items: &[Value],
    input_handle: &Value,
) -> Result<(Vec<Value>, PayloadHandle)> {
    if !items.is_empty() {
        let handle = externalize_batch_input(payload_store, batch_id, items).await?;
        return Ok((items.to_vec(), handle));
    }
    let handle = serde_json::from_value::<PayloadHandle>(input_handle.clone())
        .context("failed to decode stream batch input handle")?;
    let items = payload_store.read_items(&handle).await?;
    Ok((items, handle))
}

async fn externalize_batch_input(
    payload_store: &PayloadStore,
    batch_id: &str,
    items: &[Value],
) -> Result<PayloadHandle> {
    payload_store
        .write_value(&format!("batches/{batch_id}/input"), &Value::Array(items.to_vec()))
        .await
}

async fn maybe_initialize_batch_result_manifest(
    state: &AppState,
    batch_id: &str,
    default_handle: &Value,
    reducer: Option<&str>,
) -> Result<Value> {
    if !bulk_reducer_materializes_results(reducer) {
        return Ok(Value::Null);
    }
    let handle = PayloadHandle::Manifest {
        key: format!("batches/{batch_id}/results/manifest"),
        store: state.payload_store.default_store_kind().as_str().to_owned(),
    };
    let serialized =
        serde_json::to_value(handle).context("failed to serialize batch result handle")?;
    if default_handle.is_null() { Ok(serialized) } else { Ok(serialized) }
}

async fn maybe_externalize_chunk_output(
    state: &AppState,
    batch_id: &str,
    chunk_id: &str,
    output: &[Value],
) -> Result<Value> {
    let payload = Value::Array(output.to_vec());
    let handle = state
        .payload_store
        .write_value(&format!("batches/{batch_id}/chunks/{chunk_id}/output"), &payload)
        .await?;
    serde_json::to_value(handle).context("failed to serialize chunk output handle")
}

async fn materialize_collect_results_segment(
    state: &AppState,
    report: &ThroughputChunkReport,
    reducer: Option<&str>,
    item_count: u32,
) -> Result<(ThroughputChunkReport, Option<ThroughputResultSegmentRecord>)> {
    if reducer != Some("collect_results") {
        return Ok((report.clone(), None));
    }
    let ThroughputChunkReportPayload::ChunkCompleted { result_handle, output } = &report.payload
    else {
        return Ok((report.clone(), None));
    };
    let resolved_handle = if result_handle.is_null() {
        let Some(output) = output.as_ref() else {
            anyhow::bail!(
                "collect_results completion for chunk {} missing result_handle and output",
                report.chunk_id
            );
        };
        maybe_externalize_chunk_output(state, &report.batch_id, &report.chunk_id, output).await?
    } else {
        result_handle.clone()
    };
    let encoded_bytes = output
        .as_ref()
        .and_then(|items| serde_json::to_vec(items).ok())
        .map(|bytes| bytes.len() as u64);
    let transformed = ThroughputChunkReport {
        payload: ThroughputChunkReportPayload::ChunkCompleted {
            result_handle: resolved_handle.clone(),
            output: None,
        },
        ..report.clone()
    };
    Ok((
        transformed,
        Some(ThroughputResultSegmentRecord {
            tenant_id: report.tenant_id.clone(),
            instance_id: report.instance_id.clone(),
            run_id: report.run_id.clone(),
            batch_id: report.batch_id.clone(),
            chunk_id: report.chunk_id.clone(),
            chunk_index: report.chunk_index,
            item_count,
            result_handle: resolved_handle,
            encoded_bytes,
            checksum: None,
            created_at: report.occurred_at,
            updated_at: report.occurred_at,
        }),
    ))
}

async fn materialize_chunk_terminal_artifacts(
    state: &AppState,
    report: &ThroughputChunkReport,
    reducer: Option<&str>,
) -> Result<ChunkTerminalArtifacts> {
    match &report.payload {
        ThroughputChunkReportPayload::ChunkCompleted { result_handle, output } => {
            if !bulk_reducer_materializes_results(reducer) {
                return Ok(ChunkTerminalArtifacts {
                    result_handle: None,
                    cancellation_reason: None,
                    cancellation_metadata: None,
                });
            }
            let resolved_result_handle = if result_handle.is_null() {
                let Some(output) = output else {
                    anyhow::bail!(
                        "stream-v2 completion for chunk {} missing result_handle and compatibility output",
                        report.chunk_id
                    );
                };
                maybe_externalize_chunk_output(state, &report.batch_id, &report.chunk_id, output)
                    .await?
            } else {
                result_handle.clone()
            };
            Ok(ChunkTerminalArtifacts {
                result_handle: Some(resolved_result_handle),
                cancellation_reason: None,
                cancellation_metadata: None,
            })
        }
        ThroughputChunkReportPayload::ChunkFailed { .. } => Ok(ChunkTerminalArtifacts {
            result_handle: None,
            cancellation_reason: None,
            cancellation_metadata: None,
        }),
        ThroughputChunkReportPayload::ChunkCancelled { reason, metadata } => {
            Ok(ChunkTerminalArtifacts {
                result_handle: None,
                cancellation_reason: Some(reason.clone()),
                cancellation_metadata: metadata.clone(),
            })
        }
    }
}

fn reduce_stream_batch_output(
    state: &AppState,
    identity: &ThroughputBatchIdentity,
    reducer: Option<&str>,
    pending_report: Option<&ThroughputChunkReport>,
) -> Result<Option<Value>> {
    if collect_results_uses_batch_result_handle_only(reducer) {
        return Ok(None);
    }
    if bulk_reducer_summary_field_name(reducer).is_none() {
        return Ok(None);
    }
    state.local_state.reduce_batch_success_outputs_after_report(identity, pending_report)
}

async fn publish_stream_terminal_event(
    state: &AppState,
    report: &ThroughputChunkReport,
    projected: &ProjectedTerminalApply,
) -> Result<()> {
    let identity_key = ThroughputBatchIdentity {
        tenant_id: report.tenant_id.clone(),
        instance_id: report.instance_id.clone(),
        run_id: report.run_id.clone(),
        batch_id: report.batch_id.clone(),
    };
    let reducer_output = if projected.batch_status == "completed"
        || bulk_reducer_settles(projected.batch_reducer.as_deref())
    {
        reduce_stream_batch_output(
            state,
            &identity_key,
            projected.batch_reducer.as_deref(),
            Some(report),
        )?
    } else {
        None
    };
    let identity = WorkflowIdentity::new(
        report.tenant_id.clone(),
        projected.batch_definition_id.clone(),
        projected.batch_definition_version.unwrap_or_default(),
        projected.batch_artifact_hash.clone().unwrap_or_default(),
        report.instance_id.clone(),
        report.run_id.clone(),
        "throughput-runtime",
    );
    let payload = match projected.batch_status.as_str() {
        "completed" => WorkflowEvent::BulkActivityBatchCompleted {
            batch_id: report.batch_id.clone(),
            total_items: projected.batch_total_items,
            succeeded_items: projected.batch_succeeded_items,
            failed_items: projected.batch_failed_items,
            cancelled_items: projected.batch_cancelled_items,
            chunk_count: projected.batch_chunk_count,
            reducer_output,
        },
        "failed" => WorkflowEvent::BulkActivityBatchFailed {
            batch_id: report.batch_id.clone(),
            total_items: projected.batch_total_items,
            succeeded_items: projected.batch_succeeded_items,
            failed_items: projected.batch_failed_items,
            cancelled_items: projected.batch_cancelled_items,
            chunk_count: projected.batch_chunk_count,
            message: projected
                .batch_error
                .clone()
                .unwrap_or_else(|| "bulk batch failed".to_owned()),
            reducer_output,
        },
        _ => WorkflowEvent::BulkActivityBatchCancelled {
            batch_id: report.batch_id.clone(),
            total_items: projected.batch_total_items,
            succeeded_items: projected.batch_succeeded_items,
            failed_items: projected.batch_failed_items,
            cancelled_items: projected.batch_cancelled_items,
            chunk_count: projected.batch_chunk_count,
            message: projected
                .batch_error
                .clone()
                .unwrap_or_else(|| "bulk batch cancelled".to_owned()),
            reducer_output,
        },
    };
    let mut envelope = EventEnvelope::new(payload.event_type(), identity, payload);
    let terminal_at = projected.terminal_at.unwrap_or(report.occurred_at);
    let dedupe_key = stream_batch_terminal_dedupe_key(
        &report.tenant_id,
        &report.instance_id,
        &report.run_id,
        &report.batch_id,
        projected.batch_status.as_str(),
    );
    envelope.event_id = stream_batch_terminal_event_id(&dedupe_key);
    envelope.occurred_at = terminal_at;
    envelope.dedupe_key = Some(dedupe_key);
    envelope
        .metadata
        .insert("throughput_backend".to_owned(), ThroughputBackend::StreamV2.as_str().to_owned());
    let terminal = ThroughputTerminalRecord {
        tenant_id: report.tenant_id.clone(),
        instance_id: report.instance_id.clone(),
        run_id: report.run_id.clone(),
        batch_id: report.batch_id.clone(),
        status: projected.batch_status.as_str().to_owned(),
        terminal_at,
        terminal_event_id: envelope.event_id,
        terminal_event: envelope,
        created_at: terminal_at,
        updated_at: terminal_at,
    };
    state.store.commit_throughput_terminal_handoff(&terminal).await?;
    Ok(())
}

async fn publish_stream_batch_terminal_event(
    state: &AppState,
    batch: &fabrik_store::WorkflowBulkBatchRecord,
    pending_report: Option<&ThroughputChunkReport>,
) -> Result<()> {
    let identity_key = ThroughputBatchIdentity {
        tenant_id: batch.tenant_id.clone(),
        instance_id: batch.instance_id.clone(),
        run_id: batch.run_id.clone(),
        batch_id: batch.batch_id.clone(),
    };
    let reducer_output = if batch.status == WorkflowBulkBatchStatus::Completed
        || bulk_reducer_settles(batch.reducer.as_deref())
    {
        reduce_stream_batch_output(state, &identity_key, batch.reducer.as_deref(), pending_report)?
    } else {
        None
    };
    let identity = WorkflowIdentity::new(
        batch.tenant_id.clone(),
        batch.definition_id.clone(),
        batch.definition_version.unwrap_or_default(),
        batch.artifact_hash.clone().unwrap_or_default(),
        batch.instance_id.clone(),
        batch.run_id.clone(),
        "throughput-runtime",
    );
    let payload = match batch.status {
        WorkflowBulkBatchStatus::Completed => WorkflowEvent::BulkActivityBatchCompleted {
            batch_id: batch.batch_id.clone(),
            total_items: batch.total_items,
            succeeded_items: batch.succeeded_items,
            failed_items: batch.failed_items,
            cancelled_items: batch.cancelled_items,
            chunk_count: batch.chunk_count,
            reducer_output,
        },
        WorkflowBulkBatchStatus::Failed => WorkflowEvent::BulkActivityBatchFailed {
            batch_id: batch.batch_id.clone(),
            total_items: batch.total_items,
            succeeded_items: batch.succeeded_items,
            failed_items: batch.failed_items,
            cancelled_items: batch.cancelled_items,
            chunk_count: batch.chunk_count,
            message: batch.error.clone().unwrap_or_else(|| "bulk batch failed".to_owned()),
            reducer_output,
        },
        WorkflowBulkBatchStatus::Cancelled => WorkflowEvent::BulkActivityBatchCancelled {
            batch_id: batch.batch_id.clone(),
            total_items: batch.total_items,
            succeeded_items: batch.succeeded_items,
            failed_items: batch.failed_items,
            cancelled_items: batch.cancelled_items,
            chunk_count: batch.chunk_count,
            message: batch.error.clone().unwrap_or_else(|| "bulk batch cancelled".to_owned()),
            reducer_output,
        },
        _ => anyhow::bail!("batch {} is not terminal", batch.batch_id),
    };
    let mut envelope = EventEnvelope::new(payload.event_type(), identity, payload);
    let terminal_at = batch.terminal_at.unwrap_or(batch.updated_at);
    let dedupe_key = stream_batch_terminal_dedupe_key(
        &batch.tenant_id,
        &batch.instance_id,
        &batch.run_id,
        &batch.batch_id,
        batch.status.as_str(),
    );
    envelope.event_id = stream_batch_terminal_event_id(&dedupe_key);
    envelope.occurred_at = terminal_at;
    envelope.dedupe_key = Some(dedupe_key);
    envelope
        .metadata
        .insert("throughput_backend".to_owned(), ThroughputBackend::StreamV2.as_str().to_owned());
    let terminal = ThroughputTerminalRecord {
        tenant_id: batch.tenant_id.clone(),
        instance_id: batch.instance_id.clone(),
        run_id: batch.run_id.clone(),
        batch_id: batch.batch_id.clone(),
        status: batch.status.as_str().to_owned(),
        terminal_at,
        terminal_event_id: envelope.event_id,
        terminal_event: envelope,
        created_at: terminal_at,
        updated_at: terminal_at,
    };
    state.store.commit_throughput_terminal_handoff(&terminal).await?;
    Ok(())
}

fn stream_batch_terminal_dedupe_key(
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
    status: &str,
) -> String {
    format!("throughput-terminal:{tenant_id}:{instance_id}:{run_id}:{batch_id}:{status}")
}

fn stream_batch_terminal_event_id(dedupe_key: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, format!("throughput-terminal-event:{dedupe_key}").as_bytes())
}

async fn finalize_grouped_stream_batch(
    state: &AppState,
    identity: &ThroughputBatchIdentity,
    projected: &ProjectedBatchTerminal,
) -> Result<()> {
    let Some(existing_batch) = state.local_state.batch_snapshot(identity)? else {
        anyhow::bail!(
            "grouped throughput batch {} missing from local owner state",
            identity.batch_id
        );
    };
    if matches!(existing_batch.status.as_str(), "completed" | "failed" | "cancelled") {
        return Ok(());
    }

    publish_projection_event(
        state,
        grouped_batch_terminal_projection_record(
            identity,
            projected,
            reduce_stream_batch_output(state, identity, existing_batch.reducer.as_deref(), None)?,
        )
        .event,
        throughput_partition_key(&identity.batch_id, 0),
    )
    .await?;

    publish_changelog_records(
        state,
        vec![grouped_batch_terminal_changelog_record(identity, projected)],
    )
    .await;
    publish_stream_batch_terminal_event(
        state,
        &fabrik_store::WorkflowBulkBatchRecord {
            tenant_id: existing_batch.identity.tenant_id.clone(),
            instance_id: existing_batch.identity.instance_id.clone(),
            run_id: existing_batch.identity.run_id.clone(),
            definition_id: existing_batch.definition_id.clone(),
            definition_version: existing_batch.definition_version,
            artifact_hash: existing_batch.artifact_hash.clone(),
            batch_id: existing_batch.identity.batch_id.clone(),
            activity_type: String::new(),
            activity_capabilities: existing_batch.activity_capabilities.clone(),
            task_queue: existing_batch.task_queue.clone(),
            state: None,
            input_handle: Value::Null,
            result_handle: Value::Null,
            throughput_backend: ThroughputBackend::StreamV2.as_str().to_owned(),
            throughput_backend_version: ThroughputBackend::StreamV2.default_version().to_owned(),
            execution_mode: throughput_execution_mode(ThroughputBackend::StreamV2.as_str())
                .to_owned(),
            selected_backend: ThroughputBackend::StreamV2.as_str().to_owned(),
            routing_reason: throughput_routing_reason(
                ThroughputBackend::StreamV2.as_str(),
                existing_batch.execution_policy.as_deref(),
                existing_batch.reducer.as_deref(),
            )
            .to_owned(),
            admission_policy_version: ADMISSION_POLICY_VERSION.to_owned(),
            execution_policy: existing_batch.execution_policy.clone(),
            reducer: existing_batch.reducer.clone(),
            reducer_kind: bulk_reducer_name(existing_batch.reducer.as_deref()).to_owned(),
            reducer_class: existing_batch.reducer_class.clone(),
            reducer_version: throughput_reducer_version(existing_batch.reducer.as_deref())
                .to_owned(),
            reducer_execution_path: throughput_reducer_execution_path(
                ThroughputBackend::StreamV2.as_str(),
                existing_batch.execution_policy.as_deref(),
                existing_batch.reducer.as_deref(),
            )
            .to_owned(),
            aggregation_tree_depth: existing_batch.aggregation_tree_depth,
            fast_lane_enabled: existing_batch.fast_lane_enabled,
            aggregation_group_count: 1,
            status: match projected.status.as_str() {
                "completed" => WorkflowBulkBatchStatus::Completed,
                "failed" => WorkflowBulkBatchStatus::Failed,
                "cancelled" => WorkflowBulkBatchStatus::Cancelled,
                other => anyhow::bail!("unexpected grouped batch terminal status {other}"),
            },
            total_items: existing_batch.total_items,
            chunk_size: 0,
            chunk_count: existing_batch.chunk_count,
            succeeded_items: projected.succeeded_items,
            failed_items: projected.failed_items,
            cancelled_items: projected.cancelled_items,
            max_attempts: 0,
            retry_delay_ms: 0,
            error: projected.error.clone(),
            reducer_output: None,
            scheduled_at: existing_batch.updated_at,
            terminal_at: Some(projected.terminal_at),
            updated_at: projected.terminal_at,
        },
        None,
    )
    .await?;
    let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
    debug.terminal_events_published = debug.terminal_events_published.saturating_add(1);
    debug.last_terminal_event_at = Some(Utc::now());
    Ok(())
}

async fn publish_grouped_batch_terminal_event(
    state: &AppState,
    report: &ThroughputChunkReport,
    projected_apply: &ProjectedTerminalApply,
    projected: &ProjectedBatchTerminal,
) -> Result<()> {
    publish_stream_batch_terminal_event(
        state,
        &fabrik_store::WorkflowBulkBatchRecord {
            tenant_id: report.tenant_id.clone(),
            instance_id: report.instance_id.clone(),
            run_id: report.run_id.clone(),
            definition_id: projected_apply.batch_definition_id.clone(),
            definition_version: projected_apply.batch_definition_version,
            artifact_hash: projected_apply.batch_artifact_hash.clone(),
            batch_id: report.batch_id.clone(),
            activity_type: String::new(),
            activity_capabilities: None,
            task_queue: projected_apply.batch_task_queue.clone(),
            state: None,
            input_handle: Value::Null,
            result_handle: Value::Null,
            throughput_backend: ThroughputBackend::StreamV2.as_str().to_owned(),
            throughput_backend_version: ThroughputBackend::StreamV2.default_version().to_owned(),
            execution_mode: throughput_execution_mode(ThroughputBackend::StreamV2.as_str())
                .to_owned(),
            selected_backend: ThroughputBackend::StreamV2.as_str().to_owned(),
            routing_reason: throughput_routing_reason(
                ThroughputBackend::StreamV2.as_str(),
                projected_apply.batch_execution_policy.as_deref(),
                projected_apply.batch_reducer.as_deref(),
            )
            .to_owned(),
            admission_policy_version: ADMISSION_POLICY_VERSION.to_owned(),
            execution_policy: projected_apply.batch_execution_policy.clone(),
            reducer: projected_apply.batch_reducer.clone(),
            reducer_kind: bulk_reducer_name(projected_apply.batch_reducer.as_deref()).to_owned(),
            reducer_class: projected_apply.batch_reducer_class.clone(),
            reducer_version: throughput_reducer_version(projected_apply.batch_reducer.as_deref())
                .to_owned(),
            reducer_execution_path: throughput_reducer_execution_path(
                ThroughputBackend::StreamV2.as_str(),
                projected_apply.batch_execution_policy.as_deref(),
                projected_apply.batch_reducer.as_deref(),
            )
            .to_owned(),
            aggregation_tree_depth: projected_apply.batch_aggregation_tree_depth,
            fast_lane_enabled: projected_apply.batch_fast_lane_enabled,
            aggregation_group_count: 1,
            status: match projected.status.as_str() {
                "completed" => WorkflowBulkBatchStatus::Completed,
                "failed" => WorkflowBulkBatchStatus::Failed,
                "cancelled" => WorkflowBulkBatchStatus::Cancelled,
                other => anyhow::bail!("unexpected grouped batch terminal status {other}"),
            },
            total_items: projected_apply.batch_total_items,
            chunk_size: 0,
            chunk_count: projected_apply.batch_chunk_count,
            succeeded_items: projected.succeeded_items,
            failed_items: projected.failed_items,
            cancelled_items: projected.cancelled_items,
            max_attempts: 0,
            retry_delay_ms: 0,
            error: projected.error.clone(),
            reducer_output: None,
            scheduled_at: projected_apply.terminal_at.unwrap_or(projected.terminal_at),
            terminal_at: Some(projected.terminal_at),
            updated_at: projected.terminal_at,
        },
        Some(report),
    )
    .await
}

async fn publish_changelog(state: &AppState, payload: ThroughputChangelogPayload, key: String) {
    let entry = ThroughputChangelogEntry {
        entry_id: Uuid::now_v7(),
        occurred_at: Utc::now(),
        partition_key: key.clone(),
        payload,
    };
    if let Err(error) = state.changelog_publisher.publish(&entry, &key).await {
        error!(error = %error, "failed to publish throughput changelog entry");
        return;
    }
    if let Err(error) = state.local_state.mirror_changelog_entry(&entry) {
        state.local_state.record_changelog_apply_failure();
        error!(error = %error, "failed to mirror throughput changelog entry into local state");
        return;
    }
    let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
    debug.changelog_entries_published = debug.changelog_entries_published.saturating_add(1);
}

async fn publish_projection_event(
    state: &AppState,
    payload: ThroughputProjectionEvent,
    key: String,
) -> Result<()> {
    if state.runtime.owner_first_apply {
        apply_projection_event_directly(state, &payload).await?;
    }
    if !state.runtime.publish_projection_events {
        return Ok(());
    }
    state
        .projection_publisher
        .publish(&payload, &key)
        .await
        .context("failed to publish throughput projection event")?;
    let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
    debug.projection_events_published = debug.projection_events_published.saturating_add(1);
    Ok(())
}

fn changelog_entry(key: String, payload: ThroughputChangelogPayload) -> ThroughputChangelogEntry {
    ThroughputChangelogEntry {
        entry_id: Uuid::now_v7(),
        occurred_at: Utc::now(),
        partition_key: key,
        payload,
    }
}

fn changelog_record(key: String, payload: ThroughputChangelogPayload) -> StreamChangelogRecord {
    let entry = changelog_entry(key.clone(), payload);
    StreamChangelogRecord { key, entry }
}

fn group_terminal_changelog_entry(
    identity: &ThroughputBatchIdentity,
    projected: &ProjectedGroupTerminal,
) -> ThroughputChangelogEntry {
    changelog_entry(
        throughput_partition_key(&identity.batch_id, projected.group_id),
        ThroughputChangelogPayload::GroupTerminal {
            identity: identity.clone(),
            group_id: projected.group_id,
            level: projected.level,
            parent_group_id: projected.parent_group_id,
            status: projected.status.clone(),
            succeeded_items: projected.succeeded_items,
            failed_items: projected.failed_items,
            cancelled_items: projected.cancelled_items,
            error: projected.error.clone(),
            terminal_at: projected.terminal_at,
        },
    )
}

fn group_terminal_changelog_record(
    identity: &ThroughputBatchIdentity,
    projected: &ProjectedGroupTerminal,
) -> StreamChangelogRecord {
    let entry = group_terminal_changelog_entry(identity, projected);
    StreamChangelogRecord { key: entry.partition_key.clone(), entry }
}

fn record_group_terminal_published(state: &AppState, projected: &ProjectedGroupTerminal) {
    let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
    if projected.level == 0 {
        debug.leaf_group_terminals_published =
            debug.leaf_group_terminals_published.saturating_add(1);
    } else if projected.parent_group_id.is_some() {
        debug.parent_group_terminals_published =
            debug.parent_group_terminals_published.saturating_add(1);
    } else {
        debug.root_group_terminals_published =
            debug.root_group_terminals_published.saturating_add(1);
    }
}

fn grouped_batch_terminal_projection_event(
    identity: &ThroughputBatchIdentity,
    projected: &ProjectedBatchTerminal,
    reducer_output: Option<Value>,
) -> ThroughputProjectionEvent {
    ThroughputProjectionEvent::UpdateBatchState {
        update: ThroughputProjectionBatchStateUpdate {
            tenant_id: identity.tenant_id.clone(),
            instance_id: identity.instance_id.clone(),
            run_id: identity.run_id.clone(),
            batch_id: identity.batch_id.clone(),
            status: projected.status.clone(),
            succeeded_items: projected.succeeded_items,
            failed_items: projected.failed_items,
            cancelled_items: projected.cancelled_items,
            error: projected.error.clone(),
            reducer_output,
            terminal_at: Some(projected.terminal_at),
            updated_at: projected.terminal_at,
        },
    }
}

fn grouped_batch_terminal_projection_record(
    identity: &ThroughputBatchIdentity,
    projected: &ProjectedBatchTerminal,
    reducer_output: Option<Value>,
) -> StreamProjectionRecord {
    StreamProjectionRecord {
        key: throughput_partition_key(&identity.batch_id, 0),
        event: grouped_batch_terminal_projection_event(identity, projected, reducer_output),
    }
}

fn grouped_batch_terminal_changelog_entry(
    identity: &ThroughputBatchIdentity,
    projected: &ProjectedBatchTerminal,
) -> ThroughputChangelogEntry {
    changelog_entry(
        throughput_partition_key(&identity.batch_id, 0),
        ThroughputChangelogPayload::BatchTerminal {
            identity: identity.clone(),
            status: projected.status.clone(),
            report_id: format!("group-barrier:{}", identity.batch_id),
            succeeded_items: projected.succeeded_items,
            failed_items: projected.failed_items,
            cancelled_items: projected.cancelled_items,
            error: projected.error.clone(),
            terminal_at: projected.terminal_at,
        },
    )
}

fn grouped_batch_terminal_changelog_record(
    identity: &ThroughputBatchIdentity,
    projected: &ProjectedBatchTerminal,
) -> StreamChangelogRecord {
    let entry = grouped_batch_terminal_changelog_entry(identity, projected);
    StreamChangelogRecord { key: entry.partition_key.clone(), entry }
}

async fn publish_changelog_records(state: &AppState, records: Vec<StreamChangelogRecord>) {
    if records.is_empty() {
        return;
    }

    for batch in records.chunks(state.runtime.changelog_publish_batch_size.max(1)) {
        let payloads = batch
            .iter()
            .map(|record| (record.key.clone(), record.entry.clone()))
            .collect::<Vec<_>>();
        if let Err(error) = state.changelog_publisher.publish_all(payloads).await {
            error!(error = %error, "failed to publish throughput changelog entries");
            return;
        }
        for record in batch {
            if let Err(error) = state.local_state.mirror_changelog_entry(&record.entry) {
                state.local_state.record_changelog_apply_failure();
                error!(error = %error, "failed to mirror throughput changelog entry into local state");
                return;
            }
        }
        let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
        debug.changelog_entries_published =
            debug.changelog_entries_published.saturating_add(batch.len() as u64);
    }
}

async fn publish_projection_records(
    state: &AppState,
    records: Vec<StreamProjectionRecord>,
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }

    if state.runtime.owner_first_apply {
        apply_projection_records_directly(state, &records).await?;
    }
    if !state.runtime.publish_projection_events {
        return Ok(());
    }

    for batch in records.chunks(state.runtime.projection_publish_batch_size.max(1)) {
        let payloads = batch
            .iter()
            .map(|record| (record.key.clone(), record.event.clone()))
            .collect::<Vec<_>>();
        state
            .projection_publisher
            .publish_all(payloads)
            .await
            .context("failed to publish throughput projection events")?;
        let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
        debug.projection_events_published =
            debug.projection_events_published.saturating_add(batch.len() as u64);
    }
    Ok(())
}

async fn apply_projection_event_directly(
    state: &AppState,
    event: &ThroughputProjectionEvent,
) -> Result<()> {
    match event {
        ThroughputProjectionEvent::UpsertBatch { batch } => {
            state.store.upsert_throughput_projection_batch(batch).await?
        }
        ThroughputProjectionEvent::UpsertChunk { chunk } => {
            state.store.upsert_throughput_projection_chunk(chunk).await?
        }
        ThroughputProjectionEvent::UpdateBatchState { update } => {
            state.store.update_throughput_projection_batch_state(update).await?;
            if update.terminal_at.is_some() {
                sync_projection_batch_result_manifest(
                    state,
                    &update.tenant_id,
                    &update.instance_id,
                    &update.run_id,
                    &update.batch_id,
                )
                .await?;
                let _ = publish_projection_batch_terminal_handoff(
                    state,
                    &update.tenant_id,
                    &update.instance_id,
                    &update.run_id,
                    &update.batch_id,
                )
                .await?;
            }
        }
        ThroughputProjectionEvent::UpdateChunkState { update } => {
            state.store.update_throughput_projection_chunk_state(update).await?
        }
    }

    record_projection_events_applied_directly(state, 1);
    Ok(())
}

async fn apply_projection_events_directly(
    state: &AppState,
    events: &[ThroughputProjectionEvent],
) -> Result<()> {
    let mut batch_upserts = Vec::new();
    let mut chunk_upserts = Vec::new();
    let mut chunk_state_updates = Vec::new();
    let mut nonterminal_batch_updates = Vec::new();
    let mut terminal_batch_updates = Vec::new();

    for event in events {
        match event {
            ThroughputProjectionEvent::UpsertBatch { batch } => batch_upserts.push(batch.clone()),
            ThroughputProjectionEvent::UpsertChunk { chunk } => chunk_upserts.push(chunk.clone()),
            ThroughputProjectionEvent::UpdateChunkState { update } => {
                chunk_state_updates.push(update.clone());
            }
            ThroughputProjectionEvent::UpdateBatchState { update }
                if update.terminal_at.is_some() =>
            {
                terminal_batch_updates.push(update.clone());
            }
            ThroughputProjectionEvent::UpdateBatchState { update } => {
                nonterminal_batch_updates.push(update.clone())
            }
        }
    }

    for batch in &batch_upserts {
        state.store.upsert_throughput_projection_batch(batch).await?;
    }
    if !chunk_upserts.is_empty() {
        state.store.upsert_throughput_projection_chunks(&chunk_upserts).await?;
    }
    if !chunk_state_updates.is_empty() {
        state.store.update_throughput_projection_chunk_states(&chunk_state_updates).await?;
    }
    if !nonterminal_batch_updates.is_empty() {
        state.store.update_throughput_projection_batch_states(&nonterminal_batch_updates).await?;
    }
    if !terminal_batch_updates.is_empty() {
        state.store.update_throughput_projection_batch_states(&terminal_batch_updates).await?;
    }
    for update in terminal_batch_updates {
        sync_projection_batch_result_manifest(
            state,
            &update.tenant_id,
            &update.instance_id,
            &update.run_id,
            &update.batch_id,
        )
        .await?;
        let _ = publish_projection_batch_terminal_handoff(
            state,
            &update.tenant_id,
            &update.instance_id,
            &update.run_id,
            &update.batch_id,
        )
        .await?;
    }
    if !events.is_empty() {
        record_projection_events_applied_directly(state, events.len());
    }
    Ok(())
}

fn record_projection_events_applied_directly(state: &AppState, count: usize) {
    let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
    debug.projection_events_applied_directly =
        debug.projection_events_applied_directly.saturating_add(count as u64);
}

async fn apply_projection_records_directly(
    state: &AppState,
    records: &[StreamProjectionRecord],
) -> Result<()> {
    let events = records.iter().map(|record| record.event.clone()).collect::<Vec<_>>();
    apply_projection_events_directly(state, &events).await
}

async fn sync_projection_batch_result_manifest(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
) -> Result<()> {
    let Some(batch) =
        state.store.get_bulk_batch_query_view(tenant_id, instance_id, run_id, batch_id).await?
    else {
        return Ok(());
    };
    if !bulk_reducer_materializes_results(batch.reducer.as_deref()) {
        return Ok(());
    }
    if collect_results_uses_batch_result_handle_only(batch.reducer.as_deref()) {
        let handle = match serde_json::from_value::<PayloadHandle>(batch.result_handle.clone()) {
            Ok(handle) => handle,
            Err(_) => return Ok(()),
        };
        let key = match handle {
            PayloadHandle::Manifest { key, .. } | PayloadHandle::ManifestSlice { key, .. } => key,
            PayloadHandle::Inline { .. } => return Ok(()),
        };
        let identity = ThroughputBatchIdentity {
            tenant_id: tenant_id.to_owned(),
            instance_id: instance_id.to_owned(),
            run_id: run_id.to_owned(),
            batch_id: batch_id.to_owned(),
        };
        let segments = state
            .store
            .list_throughput_result_segments_for_batch(tenant_id, instance_id, run_id, batch_id)
            .await?;
        if !segments.is_empty() {
            let manifest = CollectResultsBatchManifest {
                kind: "collect_results_segments".to_owned(),
                chunks: segments
                    .into_iter()
                    .map(|segment| {
                        Ok(CollectResultsChunkSegmentRef {
                            chunk_id: segment.chunk_id,
                            chunk_index: segment.chunk_index,
                            item_count: segment.item_count,
                            result_handle: serde_json::from_value(segment.result_handle).context(
                                "failed to decode collect_results segment result handle",
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            };
            state
                .payload_store
                .write_value(
                    &key,
                    &serde_json::to_value(manifest)
                        .context("failed to serialize collect_results batch manifest")?,
                )
                .await?;
            return Ok(());
        }
        let chunk_snapshots = state.local_state.chunk_snapshots_for_batch(&identity)?;
        let report_log_outputs = load_collect_results_outputs_from_report_log(
            state,
            tenant_id,
            instance_id,
            run_id,
            batch_id,
        )
        .await?;
        let mut chunk_outputs = report_log_outputs;
        for chunk in chunk_snapshots {
            if let Some(output) = chunk.output {
                chunk_outputs.insert(chunk.chunk_id, output);
            }
        }
        let ordered_chunks = state
            .store
            .list_bulk_chunks_for_batch_page_query_view(
                tenant_id,
                instance_id,
                run_id,
                batch_id,
                i64::MAX,
                0,
            )
            .await?;
        let mut values = Vec::new();
        for chunk in ordered_chunks {
            if let Some(mut output) = chunk_outputs.get(&chunk.chunk_id).cloned() {
                values.append(&mut output);
            }
        }
        state.payload_store.write_value(&key, &Value::Array(values)).await?;
        return Ok(());
    }
    let chunks = state
        .store
        .list_bulk_chunks_for_batch_page_query_view(
            tenant_id,
            instance_id,
            run_id,
            batch_id,
            i64::MAX,
            0,
        )
        .await?;
    let manifest = serde_json::json!({
        "kind": "chunk-result-manifest",
        "batchId": batch_id,
        "chunks": chunks
            .into_iter()
            .filter(|chunk| chunk.output.is_some() || !chunk.result_handle.is_null())
            .map(|chunk| serde_json::json!({
                "chunkId": chunk.chunk_id,
                "chunkIndex": chunk.chunk_index,
                "resultHandle": chunk.result_handle,
            }))
            .collect::<Vec<_>>(),
    });
    if let Ok(handle) = serde_json::from_value::<PayloadHandle>(batch.result_handle.clone()) {
        let key = match handle {
            PayloadHandle::Manifest { key, .. } | PayloadHandle::ManifestSlice { key, .. } => key,
            PayloadHandle::Inline { .. } => return Ok(()),
        };
        state.payload_store.write_value(&key, &manifest).await?;
    }
    Ok(())
}

async fn publish_projection_batch_terminal_handoff(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
) -> Result<bool> {
    if state
        .store
        .get_throughput_terminal(tenant_id, instance_id, run_id, batch_id)
        .await?
        .is_some()
    {
        return Ok(false);
    }
    let Some(batch) =
        state.store.get_bulk_batch_query_view(tenant_id, instance_id, run_id, batch_id).await?
    else {
        return Ok(false);
    };
    if !matches!(
        batch.status,
        WorkflowBulkBatchStatus::Completed
            | WorkflowBulkBatchStatus::Failed
            | WorkflowBulkBatchStatus::Cancelled
    ) {
        return Ok(false);
    }
    publish_stream_batch_terminal_event(state, &batch, None).await?;
    Ok(true)
}

async fn maybe_publish_group_terminal(
    state: &AppState,
    identity: &ThroughputBatchIdentity,
    group_id: u32,
    projected: ProjectedGroupTerminal,
) {
    publish_changelog_records(
        state,
        vec![group_terminal_changelog_record(
            identity,
            &ProjectedGroupTerminal {
                group_id,
                level: projected.level,
                parent_group_id: projected.parent_group_id,
                status: projected.status,
                succeeded_items: projected.succeeded_items,
                failed_items: projected.failed_items,
                cancelled_items: projected.cancelled_items,
                error: projected.error,
                terminal_at: projected.terminal_at,
            },
        )],
    )
    .await;
}

fn throughput_report_from_result(
    result: BulkActivityTaskResult,
) -> Result<ThroughputChunkReport, Status> {
    let payload = match result.result {
        Some(
            fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Completed(
                completed,
            ),
        ) => {
            let result_handle = if !completed.result_handle_cbor.is_empty() {
                decode_cbor(&completed.result_handle_cbor, "bulk result handle")
                    .map_err(|error| Status::invalid_argument(error.to_string()))?
            } else if completed.result_handle_json.trim().is_empty() {
                Value::Null
            } else {
                serde_json::from_str(&completed.result_handle_json)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?
            };
            let output = if !completed.output_cbor.is_empty() {
                Some(
                    match decode_cbor::<Value>(&completed.output_cbor, "bulk result output")
                        .map_err(|error| Status::invalid_argument(error.to_string()))?
                    {
                        Value::Array(items) => items,
                        other => {
                            return Err(Status::invalid_argument(format!(
                                "bulk result output CBOR must decode to an array, got {other}"
                            )));
                        }
                    },
                )
            } else if completed.output_json.trim().is_empty() {
                None
            } else {
                Some(
                    serde_json::from_str(&completed.output_json)
                        .map_err(|error| Status::invalid_argument(error.to_string()))?,
                )
            };
            ThroughputChunkReportPayload::ChunkCompleted { result_handle, output }
        }
        Some(
            fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Failed(
                failed,
            ),
        ) => ThroughputChunkReportPayload::ChunkFailed { error: failed.error },
        Some(
            fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Cancelled(
                cancelled,
            ),
        ) => {
            let metadata = if !cancelled.metadata_cbor.is_empty() {
                Some(
                    decode_cbor::<Value>(&cancelled.metadata_cbor, "bulk cancellation metadata")
                        .map_err(|error| Status::invalid_argument(error.to_string()))?,
                )
            } else if cancelled.metadata_json.trim().is_empty() {
                None
            } else {
                Some(
                    serde_json::from_str::<Value>(&cancelled.metadata_json)
                        .map_err(|error| Status::invalid_argument(error.to_string()))?,
                )
            };
            ThroughputChunkReportPayload::ChunkCancelled { reason: cancelled.reason, metadata }
        }
        None => return Err(Status::invalid_argument("bulk result payload is required")),
    };
    Ok(ThroughputChunkReport {
        report_id: result.report_id,
        tenant_id: result.tenant_id,
        instance_id: result.instance_id,
        run_id: result.run_id,
        batch_id: result.batch_id,
        chunk_id: result.chunk_id,
        chunk_index: result.chunk_index,
        group_id: result.group_id,
        attempt: result.attempt,
        lease_epoch: result.lease_epoch,
        owner_epoch: result.owner_epoch,
        worker_id: result.worker_id,
        worker_build_id: result.worker_build_id,
        lease_token: result.lease_token,
        occurred_at: Utc::now(),
        payload,
    })
}

fn bulk_chunk_to_proto(
    record: &LeasedChunkSnapshot,
    supports_cbor: bool,
    inline_chunk_input_threshold_bytes: usize,
) -> BulkActivityTask {
    let payloadless_chunk_transport = can_complete_payloadless_bulk_chunk(
        &record.activity_type,
        record.activity_capabilities.as_ref(),
        record.omit_success_output,
        record.item_count,
        !record.items.is_empty(),
        !record.input_handle.is_null(),
        record.cancellation_requested,
    );
    let must_inline_items = record.input_handle.is_null() && !payloadless_chunk_transport;
    let inline_items_value = (!payloadless_chunk_transport && !record.items.is_empty())
        .then(|| Value::Array(record.items.clone()))
        .and_then(|value| {
            let json = serde_json::to_string(&value).expect("bulk chunk items serialize");
            if must_inline_items || json.len() <= inline_chunk_input_threshold_bytes.max(1) {
                Some((value, json))
            } else {
                None
            }
        });
    BulkActivityTask {
        tenant_id: record.identity.tenant_id.clone(),
        definition_id: record.definition_id.clone(),
        definition_version: record.definition_version.unwrap_or_default(),
        artifact_hash: record.artifact_hash.clone().unwrap_or_default(),
        instance_id: record.identity.instance_id.clone(),
        run_id: record.identity.run_id.clone(),
        batch_id: record.identity.batch_id.clone(),
        chunk_id: record.chunk_id.clone(),
        chunk_index: record.chunk_index,
        group_id: record.group_id,
        activity_type: record.activity_type.clone(),
        activity_capabilities_json: if supports_cbor {
            String::new()
        } else {
            serde_json::to_string(&record.activity_capabilities).expect("bulk activity capabilities serialize")
        },
        task_queue: record.task_queue.clone(),
        attempt: record.attempt,
        items_json: if supports_cbor {
            String::new()
        } else {
            inline_items_value.as_ref().map(|(_, json)| json.clone()).unwrap_or_default()
        },
        input_handle_json: if supports_cbor
            || inline_items_value.is_some()
            || payloadless_chunk_transport
        {
            String::new()
        } else {
            serde_json::to_string(&record.input_handle).expect("bulk chunk input handle serializes")
        },
        items_cbor: if supports_cbor {
            inline_items_value
                .as_ref()
                .map(|(value, _)| {
                    encode_cbor(value, "bulk chunk items").expect("bulk chunk items encode")
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        },
        input_handle_cbor: if supports_cbor
            && inline_items_value.is_none()
            && !payloadless_chunk_transport
        {
            encode_cbor(&record.input_handle, "bulk chunk input handle")
                .expect("bulk chunk input handle encodes as CBOR")
        } else {
            Vec::new()
        },
        activity_capabilities_cbor: if supports_cbor {
            encode_cbor(
                &record.activity_capabilities,
                "bulk activity capabilities",
            )
            .expect("bulk activity capabilities encode")
        } else {
            Vec::new()
        },
        prefer_cbor: supports_cbor,
        scheduled_at_unix_ms: record.scheduled_at.timestamp_millis(),
        lease_expires_at_unix_ms: record
            .lease_expires_at
            .map(|value| value.timestamp_millis())
            .unwrap_or_default(),
        cancellation_requested: record.cancellation_requested,
        lease_token: record.lease_token.clone().unwrap_or_default(),
        lease_epoch: record.lease_epoch,
        owner_epoch: record.owner_epoch,
        omit_success_output: record.omit_success_output,
        item_count: record.item_count,
    }
}

fn batch_identity(record: &WorkflowBulkChunkRecord) -> ThroughputBatchIdentity {
    ThroughputBatchIdentity {
        tenant_id: record.tenant_id.clone(),
        instance_id: record.instance_id.clone(),
        run_id: record.run_id.clone(),
        batch_id: record.batch_id.clone(),
    }
}

fn bulk_reducer_materializes_results(reducer: Option<&str>) -> bool {
    matches!(reducer.unwrap_or("collect_results"), "collect_results" | "collect_settled_results")
}

fn collect_results_uses_batch_result_handle_only(reducer: Option<&str>) -> bool {
    reducer == Some("collect_results")
}

fn should_project_chunk_materialized_output(reducer: Option<&str>) -> bool {
    !collect_results_uses_batch_result_handle_only(reducer)
}

fn projected_apply_projection_records(
    report: &ThroughputChunkReport,
    projected: &ProjectedTerminalApply,
    artifacts: &ChunkTerminalArtifacts,
    chunk_output: Option<Vec<Value>>,
    reducer_output: Option<Value>,
) -> Vec<StreamProjectionRecord> {
    let mut records = Vec::with_capacity(2);
    for event in projected_apply_projection_events(
        report,
        projected,
        artifacts,
        chunk_output,
        reducer_output,
    ) {
        records.push(StreamProjectionRecord {
            key: throughput_partition_key(&report.batch_id, report.group_id),
            event,
        });
    }
    records
}

fn append_projected_apply_projection_events(
    events: &mut Vec<ThroughputProjectionEvent>,
    report: &ThroughputChunkReport,
    projected: &ProjectedTerminalApply,
    artifacts: &ChunkTerminalArtifacts,
    chunk_output: Option<Vec<Value>>,
    reducer_output: Option<Value>,
) {
    if projected.batch_terminal {
        events.push(ThroughputProjectionEvent::UpdateBatchState {
            update: ThroughputProjectionBatchStateUpdate {
                tenant_id: report.tenant_id.clone(),
                instance_id: report.instance_id.clone(),
                run_id: report.run_id.clone(),
                batch_id: report.batch_id.clone(),
                status: projected.batch_status.clone(),
                succeeded_items: projected.batch_succeeded_items,
                failed_items: projected.batch_failed_items,
                cancelled_items: projected.batch_cancelled_items,
                error: projected.batch_error.clone(),
                reducer_output,
                terminal_at: projected.terminal_at,
                updated_at: report.occurred_at,
            },
        });
    }

    events.push(ThroughputProjectionEvent::UpdateChunkState {
        update: projected_apply_chunk_state_update(
            report,
            projected,
            artifacts,
            chunk_output,
            projected.batch_reducer.as_deref(),
        ),
    });
}

fn projected_apply_projection_events(
    report: &ThroughputChunkReport,
    projected: &ProjectedTerminalApply,
    artifacts: &ChunkTerminalArtifacts,
    chunk_output: Option<Vec<Value>>,
    reducer_output: Option<Value>,
) -> Vec<ThroughputProjectionEvent> {
    let mut events = Vec::with_capacity(2);
    append_projected_apply_projection_events(
        &mut events,
        report,
        projected,
        artifacts,
        chunk_output,
        reducer_output,
    );
    events
}

fn projected_apply_chunk_state_update(
    report: &ThroughputChunkReport,
    projected: &ProjectedTerminalApply,
    artifacts: &ChunkTerminalArtifacts,
    chunk_output: Option<Vec<Value>>,
    reducer: Option<&str>,
) -> ThroughputProjectionChunkStateUpdate {
    ThroughputProjectionChunkStateUpdate {
        tenant_id: report.tenant_id.clone(),
        instance_id: report.instance_id.clone(),
        run_id: report.run_id.clone(),
        batch_id: report.batch_id.clone(),
        chunk_id: report.chunk_id.clone(),
        chunk_index: report.chunk_index,
        group_id: report.group_id,
        item_count: projected.chunk_item_count,
        status: projected.chunk_status.clone(),
        attempt: projected.chunk_attempt,
        lease_epoch: report.lease_epoch,
        owner_epoch: report.owner_epoch,
        input_handle: None,
        result_handle: should_project_chunk_materialized_output(reducer)
            .then(|| artifacts.result_handle.clone())
            .flatten(),
        output: should_project_chunk_materialized_output(reducer).then_some(chunk_output).flatten(),
        error: projected.chunk_error.clone(),
        cancellation_requested: false,
        cancellation_reason: artifacts.cancellation_reason.clone(),
        cancellation_metadata: artifacts.cancellation_metadata.clone(),
        worker_id: None,
        worker_build_id: None,
        lease_token: None,
        last_report_id: Some(report.report_id.clone()),
        available_at: projected.chunk_available_at,
        started_at: None,
        lease_expires_at: None,
        completed_at: projected.batch_terminal.then_some(report.occurred_at).or_else(
            || match report.payload {
                ThroughputChunkReportPayload::ChunkCompleted { .. }
                | ThroughputChunkReportPayload::ChunkCancelled { .. } => Some(report.occurred_at),
                ThroughputChunkReportPayload::ChunkFailed { .. }
                    if projected.chunk_status == WorkflowBulkChunkStatus::Failed.as_str() =>
                {
                    Some(report.occurred_at)
                }
                _ => None,
            },
        ),
        updated_at: report.occurred_at,
    }
}

fn completed_chunk_output(report: &ThroughputChunkReport) -> Option<Vec<Value>> {
    match &report.payload {
        ThroughputChunkReportPayload::ChunkCompleted { output, .. } => output.clone(),
        _ => None,
    }
}

async fn load_collect_results_outputs_from_report_log(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
) -> Result<HashMap<String, Vec<Value>>> {
    let entries = state
        .store
        .list_throughput_report_log_for_batch(tenant_id, instance_id, run_id, batch_id)
        .await?;
    let mut outputs = HashMap::new();
    for entry in entries {
        if let Some(output) = completed_chunk_output(&entry.report) {
            outputs.insert(entry.report.chunk_id, output);
        } else if let Some(handle) = completed_chunk_result_handle(&entry.report) {
            let handle = serde_json::from_value::<PayloadHandle>(handle)
                .context("failed to decode collect_results chunk result handle from report log")?;
            let value = state
                .payload_store
                .read_value(&handle)
                .await
                .context("failed to read collect_results chunk result payload")?;
            let items = match value {
                Value::Array(items) => items,
                _ => Vec::new(),
            };
            outputs.insert(entry.report.chunk_id, items);
        }
    }
    Ok(outputs)
}

fn completed_chunk_result_handle(report: &ThroughputChunkReport) -> Option<Value> {
    match &report.payload {
        ThroughputChunkReportPayload::ChunkCompleted { result_handle, .. }
            if !result_handle.is_null() =>
        {
            Some(result_handle.clone())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use std::{
        collections::HashMap,
        fs,
        path::PathBuf,
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
                    "skipping throughput-runtime postgres-backed tests because docker is unavailable"
                );
                return Ok(None);
            }

            let container_name = format!("throughput-runtime-test-{}", Uuid::now_v7());
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

            let host_port = match wait_for_host_port(&container_name) {
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

    fn wait_for_host_port(container_name: &str) -> Result<u16> {
        let deadline = Instant::now() + StdDuration::from_secs(15);
        loop {
            let output = Command::new("docker")
                .args([
                    "inspect",
                    "--format",
                    "{{(index (index .NetworkSettings.Ports \"5432/tcp\") 0).HostPort}}",
                    container_name,
                ])
                .output()
                .with_context(|| format!("failed to inspect docker container {container_name}"))?;
            if output.status.success() {
                let host_port = String::from_utf8_lossy(&output.stdout).trim().to_owned();
                if !host_port.is_empty() {
                    return host_port
                        .parse::<u16>()
                        .with_context(|| format!("invalid postgres host port {host_port}"));
                }
            }
            if Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for postgres port mapping for {container_name}");
            }
            std::thread::sleep(StdDuration::from_millis(100));
        }
    }

    fn docker_logs(container_name: &str) -> Result<String> {
        let output = Command::new("docker")
            .args(["logs", container_name])
            .output()
            .with_context(|| format!("failed to read docker logs for {container_name}"))?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        Ok(format!("{stdout}{stderr}"))
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

    fn temp_path(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{prefix}-{}", Uuid::now_v7()))
    }

    fn test_batch_identity() -> ThroughputBatchIdentity {
        ThroughputBatchIdentity {
            tenant_id: "tenant-a".to_owned(),
            instance_id: "wf-a".to_owned(),
            run_id: "run-a".to_owned(),
            batch_id: "batch-a".to_owned(),
        }
    }

    fn test_batch_created_entry(identity: &ThroughputBatchIdentity) -> ThroughputChangelogEntry {
        changelog_entry(
            throughput_partition_key(&identity.batch_id, 0),
            ThroughputChangelogPayload::BatchCreated {
                identity: identity.clone(),
                task_queue: "bulk".to_owned(),
                execution_policy: Some("eager".to_owned()),
                reducer: Some("count".to_owned()),
                reducer_class: "mergeable".to_owned(),
                aggregation_tree_depth: 2,
                fast_lane_enabled: true,
                aggregation_group_count: 1,
                total_items: 1,
                chunk_count: 0,
            },
        )
    }

    fn test_ownership_config(
        static_partition_ids: Option<Vec<i32>>,
        lease_ttl_seconds: u64,
        partition_id_offset: i32,
    ) -> ThroughputOwnershipConfig {
        ThroughputOwnershipConfig {
            static_partition_ids,
            runtime_capacity: 1,
            member_heartbeat_ttl_seconds: 15,
            assignment_poll_interval_seconds: 1,
            rebalance_interval_seconds: 1,
            lease_ttl_seconds,
            renew_interval_seconds: 1,
            partition_id_offset,
        }
    }

    #[test]
    fn retry_projection_records_include_scheduled_chunk_state() {
        let occurred_at = Utc::now();
        let report = ThroughputChunkReport {
            report_id: "report-a".to_owned(),
            tenant_id: "tenant-a".to_owned(),
            instance_id: "wf-a".to_owned(),
            run_id: "run-a".to_owned(),
            batch_id: "batch-a".to_owned(),
            chunk_id: "chunk-a".to_owned(),
            chunk_index: 0,
            group_id: 0,
            attempt: 1,
            lease_epoch: 1,
            owner_epoch: 1,
            worker_id: "worker-a".to_owned(),
            worker_build_id: "build-a".to_owned(),
            lease_token: Uuid::now_v7().to_string(),
            occurred_at,
            payload: ThroughputChunkReportPayload::ChunkFailed { error: "boom".to_owned() },
        };
        let projected = local_state::ProjectedTerminalApply {
            batch_definition_id: "demo".to_owned(),
            batch_definition_version: Some(1),
            batch_artifact_hash: Some("artifact-a".to_owned()),
            batch_task_queue: "bulk".to_owned(),
            batch_execution_policy: Some("eager".to_owned()),
            batch_reducer: Some("count".to_owned()),
            batch_reducer_class: "mergeable".to_owned(),
            batch_aggregation_tree_depth: 2,
            batch_fast_lane_enabled: true,
            batch_total_items: 3,
            batch_chunk_count: 1,
            chunk_id: report.chunk_id.clone(),
            chunk_status: WorkflowBulkChunkStatus::Scheduled.as_str().to_owned(),
            chunk_attempt: 2,
            chunk_available_at: occurred_at + chrono::Duration::milliseconds(500),
            chunk_error: Some("boom".to_owned()),
            chunk_item_count: 3,
            chunk_max_attempts: 3,
            chunk_retry_delay_ms: 500,
            batch_status: WorkflowBulkBatchStatus::Running.as_str().to_owned(),
            batch_succeeded_items: 0,
            batch_failed_items: 0,
            batch_cancelled_items: 0,
            batch_error: None,
            batch_terminal: false,
            batch_terminal_deferred: false,
            terminal_at: None,
            group_terminal: None,
            parent_group_terminals: Vec::new(),
            grouped_batch_terminal: None,
            grouped_batch_summary: None,
        };
        let artifacts = ChunkTerminalArtifacts {
            result_handle: None,
            cancellation_reason: None,
            cancellation_metadata: None,
        };

        let records =
            projected_apply_projection_records(&report, &projected, &artifacts, None, None);

        assert_eq!(records.len(), 1);
        let ThroughputProjectionEvent::UpdateChunkState { update } = &records[0].event else {
            panic!("expected chunk projection update");
        };
        assert_eq!(update.status, WorkflowBulkChunkStatus::Scheduled.as_str());
        assert_eq!(update.attempt, 2);
        assert_eq!(update.last_report_id.as_deref(), Some("report-a"));
        assert_eq!(update.completed_at, None);
    }

    #[test]
    fn throughput_report_from_result_accepts_cbor_payloads() {
        let result_handle = serde_json::json!({"kind": "inline", "key": "bulk-result:batch-a"});
        let result = BulkActivityTaskResult {
            tenant_id: "tenant-a".to_owned(),
            instance_id: "wf-a".to_owned(),
            run_id: "run-a".to_owned(),
            batch_id: "batch-a".to_owned(),
            chunk_id: "chunk-a".to_owned(),
            chunk_index: 0,
            group_id: 0,
            attempt: 1,
            worker_id: "worker-a".to_owned(),
            worker_build_id: "build-a".to_owned(),
            lease_token: Uuid::now_v7().to_string(),
            lease_epoch: 1,
            owner_epoch: 1,
            report_id: "report-a".to_owned(),
            result: Some(
                fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Completed(
                    fabrik_worker_protocol::activity_worker::BulkActivityTaskCompletedResult {
                        output_json: String::new(),
                        result_handle_json: String::new(),
                        output_cbor: encode_cbor(
                            &Value::Array(vec![serde_json::json!({"ok": true})]),
                            "bulk result output",
                        )
                        .expect("CBOR output encodes"),
                        result_handle_cbor: encode_cbor(&result_handle, "bulk result handle")
                            .expect("CBOR handle encodes"),
                    },
                ),
            ),
        };

        let report = throughput_report_from_result(result).expect("CBOR result should decode");
        let ThroughputChunkReportPayload::ChunkCompleted { result_handle: actual_handle, output } =
            report.payload
        else {
            panic!("expected completed payload");
        };

        assert_eq!(actual_handle, result_handle);
        assert_eq!(output, Some(vec![serde_json::json!({"ok": true})]));
    }

    #[test]
    fn bulk_chunk_to_proto_omits_payload_handles_for_payloadless_benchmark_echo() {
        let now = Utc::now();
        let task = bulk_chunk_to_proto(
            &LeasedChunkSnapshot {
                identity: ThroughputBatchIdentity {
                    tenant_id: "tenant-a".to_owned(),
                    instance_id: "wf-a".to_owned(),
                    run_id: "run-a".to_owned(),
                    batch_id: "batch-a".to_owned(),
                },
                definition_id: "demo".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                chunk_id: "chunk-a".to_owned(),
                activity_type: "benchmark.echo".to_owned(),
                activity_capabilities: None,
                task_queue: "bulk".to_owned(),
                chunk_index: 0,
                group_id: 0,
                attempt: 1,
                item_count: 256,
                max_attempts: 1,
                retry_delay_ms: 0,
                items: Vec::new(),
                input_handle: Value::Null,
                omit_success_output: true,
                cancellation_requested: false,
                lease_epoch: 1,
                owner_epoch: 1,
                worker_id: Some("worker-a".to_owned()),
                lease_token: Some("lease-a".to_owned()),
                scheduled_at: now,
                started_at: Some(now),
                lease_expires_at: Some(now + chrono::Duration::seconds(5)),
                updated_at: now,
            },
            false,
            1024,
        );

        assert!(task.items_json.is_empty());
        assert!(task.items_cbor.is_empty());
        assert!(task.input_handle_json.is_empty());
        assert!(task.input_handle_cbor.is_empty());
        assert_eq!(task.item_count, 256);
        assert!(task.omit_success_output);
    }

    #[test]
    fn collect_results_projection_omits_chunk_materialized_output() {
        assert!(!should_project_chunk_materialized_output(Some("collect_results")));
        assert!(should_project_chunk_materialized_output(Some("collect_settled_results")));
        assert!(should_project_chunk_materialized_output(Some("sum")));
    }

    #[tokio::test]
    async fn resolve_stream_batch_input_reads_manifest_handles() -> Result<()> {
        let payload_root = temp_path("throughput-runtime-payload-store");
        let payload_store = PayloadStore::from_config(PayloadStoreConfig {
            default_store: PayloadStoreKind::LocalFilesystem,
            local_dir: payload_root.display().to_string(),
            s3_bucket: None,
            s3_region: "us-east-1".to_owned(),
            s3_endpoint: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_force_path_style: false,
            s3_key_prefix: "throughput".to_owned(),
        })
        .await?;
        let items = vec![serde_json::json!({"value": "x"}), serde_json::json!({"value": "y"})];
        let handle = payload_store
            .write_value("batches/batch-a/input", &Value::Array(items.clone()))
            .await?;

        let (resolved_items, resolved_handle) = resolve_stream_batch_input(
            &payload_store,
            "batch-a",
            &[],
            &serde_json::to_value(&handle)?,
        )
        .await?;

        assert_eq!(resolved_items, items);
        assert_eq!(resolved_handle, handle);

        fs::remove_dir_all(payload_root).ok();
        Ok(())
    }

    #[test]
    fn throughput_report_from_result_accepts_elided_completed_payloads() {
        let result = BulkActivityTaskResult {
            tenant_id: "tenant-a".to_owned(),
            instance_id: "wf-a".to_owned(),
            run_id: "run-a".to_owned(),
            batch_id: "batch-a".to_owned(),
            chunk_id: "chunk-a".to_owned(),
            chunk_index: 0,
            group_id: 0,
            attempt: 1,
            worker_id: "worker-a".to_owned(),
            worker_build_id: "build-a".to_owned(),
            lease_token: Uuid::now_v7().to_string(),
            lease_epoch: 1,
            owner_epoch: 1,
            report_id: "report-a".to_owned(),
            result: Some(
                fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Completed(
                    fabrik_worker_protocol::activity_worker::BulkActivityTaskCompletedResult {
                        output_json: String::new(),
                        result_handle_json: String::new(),
                        output_cbor: Vec::new(),
                        result_handle_cbor: Vec::new(),
                    },
                ),
            ),
        };

        let report = throughput_report_from_result(result).expect("elided result should decode");
        let ThroughputChunkReportPayload::ChunkCompleted { result_handle, output } = report.payload
        else {
            panic!("expected completed payload");
        };

        assert_eq!(result_handle, Value::Null);
        assert_eq!(output, None);
    }

    #[test]
    fn stream_batch_terminal_identity_is_deterministic_for_report_terminals() {
        let dedupe_key =
            stream_batch_terminal_dedupe_key("tenant-a", "wf-a", "run-a", "batch-a", "completed");
        assert_eq!(dedupe_key, "throughput-terminal:tenant-a:wf-a:run-a:batch-a:completed");
        assert_eq!(
            stream_batch_terminal_event_id(&dedupe_key),
            stream_batch_terminal_event_id(&dedupe_key)
        );
    }

    #[test]
    fn stream_batch_terminal_identity_changes_when_projected_terminal_changes() {
        let completed =
            stream_batch_terminal_dedupe_key("tenant-a", "wf-a", "run-a", "batch-a", "completed");
        let failed =
            stream_batch_terminal_dedupe_key("tenant-a", "wf-a", "run-a", "batch-a", "failed");

        assert_ne!(completed, failed);
        assert_ne!(
            stream_batch_terminal_event_id(&completed),
            stream_batch_terminal_event_id(&failed)
        );
    }

    #[tokio::test]
    async fn ownership_pass_claims_partition_after_checkpoint_restore_when_caught_up() -> Result<()>
    {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let identity = test_batch_identity();

        let db_path = temp_path("throughput-ownership-restore-db");
        let checkpoint_dir = temp_path("throughput-ownership-restore-checkpoints");
        let state = LocalThroughputState::open(&db_path, &checkpoint_dir, 3)?;
        state.apply_changelog_entry(0, 1, &test_batch_created_entry(&identity))?;
        let _checkpoint = state.write_checkpoint()?;

        let restored_db_path = temp_path("throughput-ownership-restore-restored-db");
        let restored = LocalThroughputState::open(&restored_db_path, &checkpoint_dir, 3)?;
        assert!(restored.restore_from_latest_checkpoint_if_empty()?);
        restored.record_observed_high_watermark(0, 2);

        let ownership = Arc::new(StdMutex::new(ThroughputOwnershipState {
            owner_id: "runtime-a".to_owned(),
            partition_id_offset: 100,
            leases: HashMap::new(),
        }));
        let debug = Arc::new(StdMutex::new(ThroughputDebugState::default()));
        let mut store_seeded_this_loop = false;

        run_throughput_ownership_pass(
            &store,
            &ownership,
            &restored,
            &[0],
            &HashSet::from([0]),
            &debug,
            &test_ownership_config(Some(vec![0]), 1, 100),
            false,
            &mut store_seeded_this_loop,
        )
        .await;

        let lease = ownership
            .lock()
            .expect("throughput ownership lock poisoned")
            .leases
            .get(&0)
            .cloned()
            .expect("restored runtime should claim caught-up partition");
        assert!(lease.active);
        assert_eq!(lease.owner_epoch, 1);
        assert!(store.validate_partition_ownership(100, "runtime-a", 1).await?);

        let _ = fs::remove_dir_all(&db_path);
        let _ = fs::remove_dir_all(&restored_db_path);
        let _ = fs::remove_dir_all(&checkpoint_dir);
        Ok(())
    }

    #[tokio::test]
    async fn ownership_pass_handoffs_partition_after_expired_lease() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let config = test_ownership_config(Some(vec![0]), 1, 200);

        let runtime_a_state = LocalThroughputState::open(
            temp_path("throughput-ownership-a-db"),
            temp_path("throughput-ownership-a-checkpoints"),
            3,
        )?;
        let ownership_a = Arc::new(StdMutex::new(ThroughputOwnershipState {
            owner_id: "runtime-a".to_owned(),
            partition_id_offset: config.partition_id_offset,
            leases: HashMap::new(),
        }));
        let debug_a = Arc::new(StdMutex::new(ThroughputDebugState::default()));
        let mut seeded_a = false;
        run_throughput_ownership_pass(
            &store,
            &ownership_a,
            &runtime_a_state,
            &[0],
            &HashSet::from([0]),
            &debug_a,
            &config,
            false,
            &mut seeded_a,
        )
        .await;
        assert!(store.validate_partition_ownership(200, "runtime-a", 1).await?);

        sleep(StdDuration::from_millis(1100)).await;

        let runtime_b_state = LocalThroughputState::open(
            temp_path("throughput-ownership-b-db"),
            temp_path("throughput-ownership-b-checkpoints"),
            3,
        )?;
        let ownership_b = Arc::new(StdMutex::new(ThroughputOwnershipState {
            owner_id: "runtime-b".to_owned(),
            partition_id_offset: config.partition_id_offset,
            leases: HashMap::new(),
        }));
        let debug_b = Arc::new(StdMutex::new(ThroughputDebugState::default()));
        let mut seeded_b = false;
        run_throughput_ownership_pass(
            &store,
            &ownership_b,
            &runtime_b_state,
            &[0],
            &HashSet::from([0]),
            &debug_b,
            &config,
            false,
            &mut seeded_b,
        )
        .await;

        let handoff = store
            .get_partition_ownership(200)
            .await?
            .context("handoff should persist partition ownership")?;
        assert_eq!(handoff.owner_id, "runtime-b");
        assert_eq!(handoff.owner_epoch, 2);
        assert!(store.validate_partition_ownership(200, "runtime-b", 2).await?);
        assert!(!store.validate_partition_ownership(200, "runtime-a", 1).await?);
        Ok(())
    }

    #[tokio::test]
    async fn repair_stale_terminal_projection_batches_repairs_grouped_batch_after_restart()
    -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let local_state = LocalThroughputState::open(
            temp_path("throughput-repair-db"),
            temp_path("throughput-repair-checkpoints"),
            3,
        )?;
        let scheduled_at = Utc::now();
        let terminal_at = scheduled_at + chrono::Duration::seconds(5);
        let batch_id = "batch-stale-grouped-terminal";
        let (batch, chunks) = store
            .upsert_bulk_batch(
                "tenant-a",
                "wf-stale-grouped-terminal",
                "run-stale-grouped-terminal",
                "demo",
                Some(1),
                Some("artifact-1"),
                batch_id,
                "benchmark.echo",
                None,
                "bulk",
                Some("join"),
                &serde_json::json!({"kind": "inline", "key": "input"}),
                &serde_json::json!({"kind": "inline", "key": "result"}),
                &[serde_json::json!(1.0), serde_json::json!(2.0)],
                1,
                1,
                1000,
                Some("eager"),
                Some("sum"),
                "mergeable",
                2,
                true,
                2,
                ThroughputBackend::StreamV2.as_str(),
                ThroughputBackend::StreamV2.default_version(),
                scheduled_at,
            )
            .await?;
        store.upsert_throughput_projection_batch(&batch).await?;

        let completed_chunks = chunks
            .into_iter()
            .enumerate()
            .map(|(index, mut chunk)| {
                chunk.group_id = index as u32;
                chunk.status = WorkflowBulkChunkStatus::Completed;
                chunk.output = Some(vec![serde_json::json!(index as f64 + 1.0)]);
                chunk.completed_at = Some(terminal_at);
                chunk.updated_at = terminal_at;
                chunk
            })
            .collect::<Vec<_>>();
        store.upsert_throughput_projection_chunks(&completed_chunks).await?;
        local_state.upsert_batch_with_chunks(&batch, &completed_chunks)?;

        let identity = ThroughputBatchIdentity {
            tenant_id: batch.tenant_id.clone(),
            instance_id: batch.instance_id.clone(),
            run_id: batch.run_id.clone(),
            batch_id: batch.batch_id.clone(),
        };
        let local_batch = local_state
            .batch_snapshot(&identity)?
            .context("local grouped batch snapshot missing")?;
        assert_eq!(local_batch.status, WorkflowBulkBatchStatus::Completed.as_str());

        let before = store
            .get_bulk_batch_query_view(
                &batch.tenant_id,
                &batch.instance_id,
                &batch.run_id,
                batch_id,
            )
            .await?
            .context("projection batch missing before repair")?;
        assert_eq!(before.status, WorkflowBulkBatchStatus::Scheduled);
        assert_eq!(before.terminal_at, None);

        let owned = HashSet::from([throughput_partition_for_batch(batch_id, 0, 8)]);
        let repaired =
            repair_stale_terminal_projection_batches_in_store(&store, &local_state, &owned, 8)
                .await?;
        assert_eq!(repaired, 1);

        let repaired_batch = store
            .get_bulk_batch_query_view(
                &batch.tenant_id,
                &batch.instance_id,
                &batch.run_id,
                batch_id,
            )
            .await?
            .context("projection batch missing after repair")?;
        assert_eq!(repaired_batch.status, WorkflowBulkBatchStatus::Completed);
        assert_eq!(repaired_batch.succeeded_items, 2);
        assert_eq!(repaired_batch.failed_items, 0);
        assert_eq!(repaired_batch.cancelled_items, 0);
        assert_eq!(repaired_batch.reducer_output, Some(serde_json::json!(3.0)));
        assert_eq!(repaired_batch.terminal_at, Some(terminal_at));

        let still_nonterminal = store
            .list_nonterminal_throughput_projection_batches_for_run(
                &batch.tenant_id,
                &batch.instance_id,
                &batch.run_id,
                ThroughputBackend::StreamV2.as_str(),
            )
            .await?;
        assert!(still_nonterminal.is_empty());

        Ok(())
    }
}

fn leased_chunk_projection_record(chunk: &WorkflowBulkChunkRecord) -> StreamProjectionRecord {
    let chunk_update = ThroughputProjectionChunkStateUpdate {
        tenant_id: chunk.tenant_id.clone(),
        instance_id: chunk.instance_id.clone(),
        run_id: chunk.run_id.clone(),
        batch_id: chunk.batch_id.clone(),
        chunk_id: chunk.chunk_id.clone(),
        chunk_index: chunk.chunk_index,
        group_id: chunk.group_id,
        item_count: chunk.item_count,
        status: WorkflowBulkChunkStatus::Started.as_str().to_owned(),
        attempt: chunk.attempt,
        lease_epoch: chunk.lease_epoch,
        owner_epoch: chunk.owner_epoch,
        input_handle: None,
        result_handle: None,
        output: None,
        error: chunk.error.clone(),
        cancellation_requested: chunk.cancellation_requested,
        cancellation_reason: chunk.cancellation_reason.clone(),
        cancellation_metadata: chunk.cancellation_metadata.clone(),
        worker_id: chunk.worker_id.clone(),
        worker_build_id: chunk.worker_build_id.clone(),
        lease_token: chunk.lease_token,
        last_report_id: chunk.last_report_id.clone(),
        available_at: chunk.available_at,
        started_at: chunk.started_at,
        lease_expires_at: chunk.lease_expires_at,
        completed_at: None,
        updated_at: chunk.updated_at,
    };
    StreamProjectionRecord {
        key: throughput_partition_key(&chunk.batch_id, chunk.group_id),
        event: ThroughputProjectionEvent::UpdateChunkState { update: chunk_update },
    }
}

fn running_batch_projection_record(
    batch: &local_state::LocalBatchSnapshot,
) -> StreamProjectionRecord {
    StreamProjectionRecord {
        key: throughput_partition_key(&batch.identity.batch_id, 0),
        event: ThroughputProjectionEvent::UpdateBatchState {
            update: ThroughputProjectionBatchStateUpdate {
                tenant_id: batch.identity.tenant_id.clone(),
                instance_id: batch.identity.instance_id.clone(),
                run_id: batch.identity.run_id.clone(),
                batch_id: batch.identity.batch_id.clone(),
                status: WorkflowBulkBatchStatus::Running.as_str().to_owned(),
                succeeded_items: batch.succeeded_items,
                failed_items: batch.failed_items,
                cancelled_items: batch.cancelled_items,
                error: batch.error.clone(),
                reducer_output: batch.reducer_output.clone(),
                terminal_at: batch.terminal_at,
                updated_at: batch.updated_at,
            },
        },
    }
}

fn grouped_batch_summary_projection_event(
    identity: &ThroughputBatchIdentity,
    summary: &ProjectedBatchGroupSummary,
    reducer_output: Option<Value>,
) -> ThroughputProjectionEvent {
    ThroughputProjectionEvent::UpdateBatchState {
        update: ThroughputProjectionBatchStateUpdate {
            tenant_id: identity.tenant_id.clone(),
            instance_id: identity.instance_id.clone(),
            run_id: identity.run_id.clone(),
            batch_id: identity.batch_id.clone(),
            status: WorkflowBulkBatchStatus::Running.as_str().to_owned(),
            succeeded_items: summary.succeeded_items,
            failed_items: summary.failed_items,
            cancelled_items: summary.cancelled_items,
            error: summary.error.clone(),
            reducer_output,
            terminal_at: None,
            updated_at: summary.updated_at,
        },
    }
}

fn grouped_batch_summary_projection_record(
    identity: &ThroughputBatchIdentity,
    summary: &ProjectedBatchGroupSummary,
    reducer_output: Option<Value>,
) -> StreamProjectionRecord {
    StreamProjectionRecord {
        key: throughput_partition_key(&identity.batch_id, 0),
        event: grouped_batch_summary_projection_event(identity, summary, reducer_output),
    }
}

fn payload_handle_key(handle: &PayloadHandle) -> Result<&str> {
    match handle {
        PayloadHandle::Manifest { key, .. } | PayloadHandle::ManifestSlice { key, .. } => Ok(key),
        PayloadHandle::Inline { .. } => {
            anyhow::bail!("stream-v2 input handle must be object-store-backed")
        }
    }
}

fn payload_handle_store(handle: &PayloadHandle) -> Result<&str> {
    match handle {
        PayloadHandle::Manifest { store, .. } | PayloadHandle::ManifestSlice { store, .. } => {
            Ok(store)
        }
        PayloadHandle::Inline { .. } => {
            anyhow::bail!("stream-v2 input handle must be object-store-backed")
        }
    }
}

async fn sync_stream_batch_projection(
    state: &AppState,
    batch: &fabrik_store::WorkflowBulkBatchRecord,
) -> Result<()> {
    state
        .store
        .upsert_throughput_projection_batch(batch)
        .await
        .with_context(|| format!("failed to sync throughput projection batch {}", batch.batch_id))
}

async fn cancel_stream_batches_for_run(
    state: &AppState,
    event: &fabrik_events::EventEnvelope<WorkflowEvent>,
    reason: &str,
) -> Result<()> {
    let batches = state
        .store
        .list_nonterminal_throughput_projection_batches_for_run(
            &event.tenant_id,
            &event.instance_id,
            &event.run_id,
            ThroughputBackend::StreamV2.as_str(),
        )
        .await?;
    for batch in batches {
        let chunks = state
            .store
            .list_bulk_chunks_for_batch_page_query_view(
                &batch.tenant_id,
                &batch.instance_id,
                &batch.run_id,
                &batch.batch_id,
                i64::MAX,
                0,
            )
            .await?;
        let occurred_at = Utc::now();
        let mut cancelled_batch = batch.clone();
        for chunk in chunks.into_iter().filter(|chunk| {
            matches!(
                chunk.status,
                WorkflowBulkChunkStatus::Scheduled | WorkflowBulkChunkStatus::Started
            )
        }) {
            let item_count = chunk.item_count;
            state
                .projection_publisher
                .publish(
                    &ThroughputProjectionEvent::UpdateChunkState {
                        update: ThroughputProjectionChunkStateUpdate {
                            tenant_id: chunk.tenant_id.clone(),
                            instance_id: chunk.instance_id.clone(),
                            run_id: chunk.run_id.clone(),
                            batch_id: chunk.batch_id.clone(),
                            chunk_id: chunk.chunk_id.clone(),
                            chunk_index: chunk.chunk_index,
                            group_id: chunk.group_id,
                            item_count: chunk.item_count,
                            status: WorkflowBulkChunkStatus::Cancelled.as_str().to_owned(),
                            attempt: chunk.attempt,
                            lease_epoch: chunk.lease_epoch,
                            owner_epoch: chunk.owner_epoch,
                            input_handle: None,
                            result_handle: None,
                            output: None,
                            error: Some(reason.to_owned()),
                            cancellation_requested: false,
                            cancellation_reason: Some(reason.to_owned()),
                            cancellation_metadata: None,
                            worker_id: None,
                            worker_build_id: None,
                            lease_token: None,
                            last_report_id: Some(format!(
                                "batch-cancel:{}:{}",
                                event.event_id, chunk.chunk_id
                            )),
                            available_at: chunk.available_at,
                            started_at: chunk.started_at,
                            lease_expires_at: None,
                            completed_at: Some(occurred_at),
                            updated_at: occurred_at,
                        },
                    },
                    &throughput_partition_key(&chunk.batch_id, chunk.group_id),
                )
                .await?;
            cancelled_batch.cancelled_items =
                cancelled_batch.cancelled_items.saturating_add(item_count);
            publish_changelog(
                state,
                ThroughputChangelogPayload::ChunkApplied {
                    identity: batch_identity(&chunk),
                    chunk_id: chunk.chunk_id.clone(),
                    chunk_index: chunk.chunk_index,
                    attempt: chunk.attempt,
                    group_id: chunk.group_id,
                    item_count: chunk.item_count,
                    max_attempts: chunk.max_attempts,
                    retry_delay_ms: chunk.retry_delay_ms,
                    lease_epoch: chunk.lease_epoch,
                    owner_epoch: chunk.owner_epoch,
                    report_id: format!("batch-cancel:{}:{}", event.event_id, chunk.chunk_id),
                    status: WorkflowBulkChunkStatus::Cancelled.as_str().to_owned(),
                    available_at: chunk.available_at,
                    result_handle: None,
                    output: None,
                    error: Some(reason.to_owned()),
                    cancellation_reason: Some(reason.to_owned()),
                    cancellation_metadata: None,
                },
                throughput_partition_key(&chunk.batch_id, chunk.group_id),
            )
            .await;
            if let Ok(Some(group_terminal)) = state.local_state.project_group_terminal(
                &batch_identity(&chunk),
                chunk.group_id,
                occurred_at,
            ) {
                maybe_publish_group_terminal(
                    state,
                    &batch_identity(&chunk),
                    chunk.group_id,
                    group_terminal,
                )
                .await;
            }
        }
        cancelled_batch.status = WorkflowBulkBatchStatus::Cancelled;
        cancelled_batch.error = Some(reason.to_owned());
        cancelled_batch.terminal_at = Some(occurred_at);
        cancelled_batch.updated_at = occurred_at;
        publish_projection_event(
            state,
            ThroughputProjectionEvent::UpdateBatchState {
                update: ThroughputProjectionBatchStateUpdate {
                    tenant_id: cancelled_batch.tenant_id.clone(),
                    instance_id: cancelled_batch.instance_id.clone(),
                    run_id: cancelled_batch.run_id.clone(),
                    batch_id: cancelled_batch.batch_id.clone(),
                    status: cancelled_batch.status.as_str().to_owned(),
                    succeeded_items: cancelled_batch.succeeded_items,
                    failed_items: cancelled_batch.failed_items,
                    cancelled_items: cancelled_batch.cancelled_items,
                    error: cancelled_batch.error.clone(),
                    reducer_output: cancelled_batch.reducer_output.clone(),
                    terminal_at: cancelled_batch.terminal_at,
                    updated_at: cancelled_batch.updated_at,
                },
            },
            throughput_partition_key(&cancelled_batch.batch_id, 0),
        )
        .await?;
        publish_changelog(
            state,
            ThroughputChangelogPayload::BatchTerminal {
                identity: ThroughputBatchIdentity {
                    tenant_id: cancelled_batch.tenant_id.clone(),
                    instance_id: cancelled_batch.instance_id.clone(),
                    run_id: cancelled_batch.run_id.clone(),
                    batch_id: cancelled_batch.batch_id.clone(),
                },
                status: cancelled_batch.status.as_str().to_owned(),
                report_id: format!("batch-cancel:{}", event.event_id),
                succeeded_items: cancelled_batch.succeeded_items,
                failed_items: cancelled_batch.failed_items,
                cancelled_items: cancelled_batch.cancelled_items,
                error: cancelled_batch.error.clone(),
                terminal_at: cancelled_batch.terminal_at.unwrap_or(occurred_at),
            },
            throughput_partition_key(&cancelled_batch.batch_id, 0),
        )
        .await;
        publish_stream_batch_terminal_event(state, &cancelled_batch, None).await?;
        state.bulk_notify.notify_waiters();
    }
    Ok(())
}

fn record_throttle(state: &AppState, reason: &str) {
    let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
    match reason {
        "tenant_cap" => {
            debug.tenant_throttle_events = debug.tenant_throttle_events.saturating_add(1)
        }
        "task_queue_cap" | "queue_paused" | "queue_draining" => {
            debug.task_queue_throttle_events = debug.task_queue_throttle_events.saturating_add(1)
        }
        "batch_cap" | "batch_paused" => {
            debug.batch_throttle_events = debug.batch_throttle_events.saturating_add(1)
        }
        _ => {}
    }
    debug.last_throttle_at = Some(Utc::now());
    debug.last_throttle_reason = Some(reason.to_owned());
}

fn report_validation_label(validation: ReportValidation) -> &'static str {
    match validation {
        ReportValidation::Accept => "accept",
        ReportValidation::RejectTerminalBatch => "terminal_batch",
        ReportValidation::RejectMissingChunk => "missing_chunk",
        ReportValidation::RejectChunkNotStarted => "chunk_not_started",
        ReportValidation::RejectAttemptMismatch => "attempt_mismatch",
        ReportValidation::RejectLeaseEpochMismatch => "lease_epoch_mismatch",
        ReportValidation::RejectLeaseTokenMismatch => "lease_token_mismatch",
        ReportValidation::RejectOwnerEpochMismatch => "owner_epoch_mismatch",
        ReportValidation::RejectAlreadyApplied => "already_applied",
    }
}

fn internal_status(error: anyhow::Error) -> Status {
    Status::internal(error.to_string())
}
