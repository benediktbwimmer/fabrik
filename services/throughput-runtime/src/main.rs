mod local_state;

use std::{
    collections::{HashMap, HashSet},
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
    BrokerConfig, JsonTopicConfig, JsonTopicPublisher, WorkflowPublisher,
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
    PartitionOwnershipRecord, ThroughputProjectionBatchStateUpdate,
    ThroughputProjectionChunkStateUpdate, ThroughputProjectionEvent, WorkflowBulkBatchRecord,
    WorkflowBulkBatchStatus, WorkflowBulkChunkRecord, WorkflowBulkChunkStatus, WorkflowStore,
};
use fabrik_throughput::{
    PayloadHandle, PayloadStore, PayloadStoreConfig, PayloadStoreKind, ThroughputBackend,
    ThroughputBatchIdentity, ThroughputChangelogEntry, ThroughputChangelogPayload,
    ThroughputChunkReport, ThroughputChunkReportPayload, bulk_reducer_class, decode_cbor,
    effective_aggregation_group_count, encode_cbor, group_id_for_chunk_index,
    planned_reduction_tree_depth, stream_v2_fast_lane_enabled, throughput_partition_key,
};
use fabrik_worker_protocol::activity_worker::{
    Ack, BulkActivityTask, BulkActivityTaskResult, PollBulkActivityTaskRequest,
    PollBulkActivityTaskResponse, ReportBulkActivityTaskResultsRequest,
    activity_worker_api_server::{ActivityWorkerApi, ActivityWorkerApiServer},
};
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

    let consumer = build_workflow_consumer(
        &workflow_broker,
        "throughput-runtime",
        &workflow_broker.all_partition_ids(),
    )
    .await?;
    tokio::spawn(run_bridge_loop(state.clone(), consumer));
    if !state.runtime.owner_first_apply {
        tokio::spawn(run_changelog_consumer_loop(state.clone(), changelog_config.clone()));
    }
    tokio::spawn(run_checkpoint_loop(state.clone()));
    tokio::spawn(run_terminal_state_gc_loop(state.clone()));
    tokio::spawn(run_group_barrier_reconciler(state.clone()));
    tokio::spawn(run_throughput_ownership_loop(
        state.store.clone(),
        ownership.clone(),
        state.local_state.clone(),
        managed_partitions.clone(),
        format!("http://127.0.0.1:{}", debug_config.port),
        debug.clone(),
        ownership_config,
        state.runtime.owner_first_apply,
    ));
    tokio::spawn(run_outbox_publisher(state.clone()));
    tokio::spawn(run_bulk_requeue_sweep(state.clone()));
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
        task_queue,
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
    } = &event.payload
    else {
        return Ok(());
    };
    if throughput_backend != ThroughputBackend::StreamV2.as_str() {
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

    let (batch, chunks) =
        build_stream_projection_records(&state, &event).await.with_context(|| {
            format!("failed to build stream-v2 batch projection records for {batch_id}")
        })?;
    if let Err(error) = sync_stream_batch_projection(&state, &batch).await {
        error!(error = %error, batch_id = %batch_id, "failed to project stream-v2 batch");
    }
    if let Err(error) = state.local_state.upsert_batch_with_chunks(&batch, &chunks) {
        error!(error = %error, batch_id = %batch_id, "failed to seed local stream-v2 batch state");
    }
    if !state.runtime.owner_first_apply {
        publish_changelog_records(
            &state,
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
    seed_local_state_from_store_view(&state.store, &state.local_state).await
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
    let lease_ttl = Duration::from_secs(config.lease_ttl_seconds.max(1));
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
        for throughput_partition_id in &managed_partitions {
            let snapshot = {
                let state = ownership.lock().expect("throughput ownership lock poisoned");
                state.leases.get(throughput_partition_id).cloned().unwrap_or_else(|| {
                    LeaseState::inactive(*throughput_partition_id, &state.owner_id)
                })
            };
            let should_own = desired_partitions.contains(throughput_partition_id);
            let ownership_partition_id = throughput_ownership_partition_id(
                *throughput_partition_id,
                config.partition_id_offset,
            );
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
                if store_recovery_enabled && !store_seeded_this_loop {
                    match seed_local_state_from_store_view(&store, &local_state).await {
                        Ok(()) => store_seeded_this_loop = true,
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
                            runtime_debug.last_catchup_wait_partition =
                                Some(*throughput_partition_id);
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
                        let mut state =
                            ownership.lock().expect("throughput ownership lock poisoned");
                        state
                            .leases
                            .insert(*throughput_partition_id, LeaseState::from_record(&record));
                    }
                    Ok(None) => {
                        let mut state =
                            ownership.lock().expect("throughput ownership lock poisoned");
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
                        let mut state =
                            ownership.lock().expect("throughput ownership lock poisoned");
                        state
                            .leases
                            .insert(*throughput_partition_id, LeaseState::from_record(&record));
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
        tokio::time::sleep(if use_assignments { poll_interval } else { renew_interval }).await;
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
    state.local_state.record_checkpoint_write(created_at);
    Ok(())
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
                    leased_tasks.push(bulk_chunk_to_proto(&leased, request.supports_cbor));
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
            let mut projection_records = Vec::with_capacity(report_batch.len() * 2);
            let mut skipped_projection_events = 0_u64;
            {
                let mut debug = self.state.debug.lock().expect("throughput debug lock poisoned");
                debug.report_batches_applied = debug.report_batches_applied.saturating_add(1);
                debug.report_batch_items_total =
                    debug.report_batch_items_total.saturating_add(report_batch.len() as u64);
            }

            for report in report_batch {
                let throughput_partition_id = throughput_partition_for_batch(
                    &report.batch_id,
                    report.group_id,
                    self.state.throughput_partitions,
                );
                if !owns_throughput_partition(&self.state, throughput_partition_id) {
                    return Err(Status::unavailable(format!(
                        "throughput partition {} is not owned by this runtime",
                        throughput_partition_id
                    )));
                }
                let identity = ThroughputBatchIdentity {
                    tenant_id: report.tenant_id.clone(),
                    instance_id: report.instance_id.clone(),
                    run_id: report.run_id.clone(),
                    batch_id: report.batch_id.clone(),
                };
                let projected = match self
                    .state
                    .local_state
                    .prepare_report_apply(&report)
                    .map_err(internal_status)?
                {
                    PreparedReportApply::MissingBatch => {
                        return Err(Status::failed_precondition(format!(
                            "bulk batch {} is not owned by this runtime",
                            report.batch_id
                        )));
                    }
                    PreparedReportApply::Rejected(validation) => {
                        let mut debug =
                            self.state.debug.lock().expect("throughput debug lock poisoned");
                        debug.reports_rejected = debug.reports_rejected.saturating_add(1);
                        debug.last_report_rejection_reason =
                            Some(report_validation_label(validation).to_owned());
                        continue;
                    }
                    PreparedReportApply::Accepted(projected) => projected,
                };
                let terminal_artifacts = materialize_chunk_terminal_artifacts(&self.state, &report)
                    .await
                    .map_err(internal_status)?;
                projection_records.extend(projected_apply_projection_records(
                    &report,
                    &projected,
                    &terminal_artifacts,
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
                        output: None,
                        error: projected.chunk_error.clone(),
                        cancellation_reason: terminal_artifacts.cancellation_reason.clone(),
                        cancellation_metadata: terminal_artifacts.cancellation_metadata.clone(),
                    },
                )];
                if let Some(group_terminal) = projected.group_terminal.as_ref() {
                    record_group_terminal_published(&self.state, group_terminal);
                    changelog_records
                        .push(group_terminal_changelog_record(&identity, group_terminal));
                    for parent_group in &projected.parent_group_terminals {
                        record_group_terminal_published(&self.state, parent_group);
                        changelog_records
                            .push(group_terminal_changelog_record(&identity, parent_group));
                    }
                    if let Some(batch_terminal) = projected.grouped_batch_terminal.as_ref() {
                        projection_records.push(grouped_batch_terminal_projection_record(
                            &identity,
                            batch_terminal,
                        ));
                        changelog_records.push(grouped_batch_terminal_changelog_record(
                            &identity,
                            batch_terminal,
                        ));
                    } else if let Some(summary) = projected.grouped_batch_summary.as_ref() {
                        if self.state.runtime.publish_transient_projection_updates {
                            projection_records
                                .push(grouped_batch_summary_projection_record(&identity, summary));
                        } else {
                            skipped_projection_events = skipped_projection_events.saturating_add(1);
                        }
                    }
                }
                if projected.batch_terminal && !self.state.runtime.owner_first_apply {
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
                if self.state.runtime.owner_first_apply {
                    let staged_entries = changelog_records
                        .iter()
                        .map(|record| record.entry.clone())
                        .collect::<Vec<_>>();
                    self.state
                        .local_state
                        .mirror_changelog_records(&staged_entries)
                        .map_err(internal_status)?;
                }
                if !self.state.runtime.owner_first_apply {
                    publish_changelog_records(&self.state, changelog_records).await;
                }
                if let Some(batch_terminal) = projected.grouped_batch_terminal.as_ref() {
                    publish_grouped_batch_terminal_event(
                        &self.state,
                        &report,
                        &projected,
                        batch_terminal,
                    )
                    .await
                    .map_err(internal_status)?;
                    let mut debug =
                        self.state.debug.lock().expect("throughput debug lock poisoned");
                    debug.terminal_events_published =
                        debug.terminal_events_published.saturating_add(1);
                    debug.last_terminal_event_at = Some(Utc::now());
                }
                if projected.batch_terminal {
                    publish_stream_terminal_event(&self.state, &report, &projected)
                        .await
                        .map_err(internal_status)?;
                    let mut debug =
                        self.state.debug.lock().expect("throughput debug lock poisoned");
                    debug.terminal_events_published =
                        debug.terminal_events_published.saturating_add(1);
                    debug.last_terminal_event_at = Some(Utc::now());
                }
                if projected.chunk_status == WorkflowBulkChunkStatus::Scheduled.as_str()
                    || projected.batch_terminal_deferred
                {
                    self.state.bulk_notify.notify_waiters();
                }
            }
            publish_projection_records(&self.state, projection_records)
                .await
                .map_err(internal_status)?;
            if skipped_projection_events > 0 {
                let mut debug = self.state.debug.lock().expect("throughput debug lock poisoned");
                debug.projection_events_skipped =
                    debug.projection_events_skipped.saturating_add(skipped_projection_events);
            }
        }
        Ok(Response::new(Ack {}))
    }
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

async fn build_stream_projection_records(
    app_state: &AppState,
    event: &EventEnvelope<WorkflowEvent>,
) -> Result<(fabrik_store::WorkflowBulkBatchRecord, Vec<WorkflowBulkChunkRecord>)> {
    let WorkflowEvent::BulkActivityBatchScheduled {
        batch_id,
        activity_type,
        task_queue,
        items,
        input_handle: _,
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
    } = &event.payload
    else {
        anyhow::bail!("expected BulkActivityBatchScheduled event");
    };

    let chunk_size_usize =
        usize::try_from((*chunk_size).max(1)).context("bulk chunk size exceeds usize")?;
    let chunk_count = if items.is_empty() {
        0
    } else {
        ((items.len() + chunk_size_usize - 1) / chunk_size_usize) as u32
    };
    let fast_lane_enabled = stream_v2_fast_lane_enabled(
        throughput_backend,
        execution_policy.as_deref(),
        reducer.as_deref(),
    );
    let reducer_class = bulk_reducer_class(reducer.as_deref());
    let effective_group_count = planned_aggregation_group_count(
        *aggregation_group_count,
        chunk_count,
        fast_lane_enabled,
        &app_state.runtime,
    );
    let aggregation_tree_depth =
        planned_reduction_tree_depth(chunk_count, effective_group_count, reducer.as_deref());
    let batch_input_handle = externalize_batch_input(app_state, batch_id, items).await?;
    let batch_result_handle =
        maybe_initialize_batch_result_manifest(app_state, batch_id, result_handle).await?;
    let batch = fabrik_store::WorkflowBulkBatchRecord {
        tenant_id: event.tenant_id.clone(),
        instance_id: event.instance_id.clone(),
        run_id: event.run_id.clone(),
        definition_id: event.definition_id.clone(),
        definition_version: Some(event.definition_version),
        artifact_hash: Some(event.artifact_hash.clone()),
        batch_id: batch_id.clone(),
        activity_type: activity_type.clone(),
        task_queue: task_queue.clone(),
        state: state.clone(),
        input_handle: serde_json::to_value(&batch_input_handle)
            .context("failed to serialize batch input handle")?,
        result_handle: batch_result_handle,
        throughput_backend: throughput_backend.clone(),
        throughput_backend_version: throughput_backend_version.clone(),
        execution_policy: execution_policy.clone(),
        reducer: reducer.clone(),
        reducer_class: reducer_class.as_str().to_owned(),
        aggregation_tree_depth,
        fast_lane_enabled,
        aggregation_group_count: effective_group_count,
        status: WorkflowBulkBatchStatus::Scheduled,
        total_items: items.len() as u32,
        chunk_size: *chunk_size,
        chunk_count,
        succeeded_items: 0,
        failed_items: 0,
        cancelled_items: 0,
        max_attempts: *max_attempts,
        retry_delay_ms: *retry_delay_ms,
        error: None,
        scheduled_at: event.occurred_at,
        terminal_at: None,
        updated_at: event.occurred_at,
    };
    let tenant_id = event.tenant_id.clone();
    let instance_id = event.instance_id.clone();
    let run_id = event.run_id.clone();
    let definition_id = event.definition_id.clone();
    let definition_version = Some(event.definition_version);
    let artifact_hash = Some(event.artifact_hash.clone());
    let activity_type = activity_type.clone();
    let task_queue = task_queue.clone();
    let workflow_state = state.clone();
    let occurred_at = event.occurred_at;
    let payload_key = payload_handle_key(&batch_input_handle)?.to_owned();
    let payload_store = payload_handle_store(&batch_input_handle)?.to_owned();
    let mut chunks = Vec::with_capacity(usize::try_from(chunk_count).unwrap_or_default());
    for (chunk_index, chunk_items) in items.chunks(chunk_size_usize).enumerate() {
        let chunk_index = chunk_index as u32;
        let chunk_id = format!("{batch_id}::{chunk_index}");
        let group_id = group_id_for_chunk_index(chunk_index, effective_group_count);
        let chunk_start =
            usize::try_from(chunk_index).unwrap_or_default().saturating_mul(chunk_size_usize);
        let input_handle = serde_json::to_value(PayloadHandle::ManifestSlice {
            key: payload_key.clone(),
            store: payload_store.clone(),
            start: chunk_start,
            len: chunk_items.len(),
        })
        .context("failed to serialize chunk input handle")?;
        chunks.push(WorkflowBulkChunkRecord {
            tenant_id: tenant_id.clone(),
            instance_id: instance_id.clone(),
            run_id: run_id.clone(),
            definition_id: definition_id.clone(),
            definition_version,
            artifact_hash: artifact_hash.clone(),
            batch_id: batch_id.clone(),
            chunk_id,
            chunk_index,
            group_id,
            item_count: chunk_items.len() as u32,
            activity_type: activity_type.clone(),
            task_queue: task_queue.clone(),
            state: workflow_state.clone(),
            status: WorkflowBulkChunkStatus::Scheduled,
            attempt: 1,
            lease_epoch: 0,
            owner_epoch: 1,
            max_attempts: *max_attempts,
            retry_delay_ms: *retry_delay_ms,
            input_handle,
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
        PayloadHandle::Inline { .. } => {
            anyhow::bail!("stream-v2 batch input handle must be object-store-backed")
        }
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

async fn externalize_batch_input(
    state: &AppState,
    batch_id: &str,
    items: &[Value],
) -> Result<PayloadHandle> {
    state
        .payload_store
        .write_value(&format!("batches/{batch_id}/input"), &Value::Array(items.to_vec()))
        .await
}

async fn maybe_initialize_batch_result_manifest(
    state: &AppState,
    batch_id: &str,
    default_handle: &Value,
) -> Result<Value> {
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

async fn materialize_chunk_terminal_artifacts(
    state: &AppState,
    report: &ThroughputChunkReport,
) -> Result<ChunkTerminalArtifacts> {
    match &report.payload {
        ThroughputChunkReportPayload::ChunkCompleted { result_handle, output } => {
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

async fn publish_stream_terminal_event(
    state: &AppState,
    report: &ThroughputChunkReport,
    projected: &ProjectedTerminalApply,
) -> Result<()> {
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
        },
    };
    let mut envelope = EventEnvelope::new(payload.event_type(), identity, payload);
    envelope
        .metadata
        .insert("throughput_backend".to_owned(), ThroughputBackend::StreamV2.as_str().to_owned());
    state.workflow_publisher.publish(&envelope, &envelope.partition_key).await
}

async fn publish_stream_batch_terminal_event(
    state: &AppState,
    batch: &fabrik_store::WorkflowBulkBatchRecord,
) -> Result<()> {
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
        },
        WorkflowBulkBatchStatus::Failed => WorkflowEvent::BulkActivityBatchFailed {
            batch_id: batch.batch_id.clone(),
            total_items: batch.total_items,
            succeeded_items: batch.succeeded_items,
            failed_items: batch.failed_items,
            cancelled_items: batch.cancelled_items,
            chunk_count: batch.chunk_count,
            message: batch.error.clone().unwrap_or_else(|| "bulk batch failed".to_owned()),
        },
        WorkflowBulkBatchStatus::Cancelled => WorkflowEvent::BulkActivityBatchCancelled {
            batch_id: batch.batch_id.clone(),
            total_items: batch.total_items,
            succeeded_items: batch.succeeded_items,
            failed_items: batch.failed_items,
            cancelled_items: batch.cancelled_items,
            chunk_count: batch.chunk_count,
            message: batch.error.clone().unwrap_or_else(|| "bulk batch cancelled".to_owned()),
        },
        _ => anyhow::bail!("batch {} is not terminal", batch.batch_id),
    };
    let mut envelope = EventEnvelope::new(payload.event_type(), identity, payload);
    envelope
        .metadata
        .insert("throughput_backend".to_owned(), ThroughputBackend::StreamV2.as_str().to_owned());
    state.workflow_publisher.publish(&envelope, &envelope.partition_key).await
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
        grouped_batch_terminal_projection_record(identity, projected).event,
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
            task_queue: existing_batch.task_queue.clone(),
            state: None,
            input_handle: Value::Null,
            result_handle: Value::Null,
            throughput_backend: ThroughputBackend::StreamV2.as_str().to_owned(),
            throughput_backend_version: ThroughputBackend::StreamV2.default_version().to_owned(),
            execution_policy: existing_batch.execution_policy.clone(),
            reducer: existing_batch.reducer.clone(),
            reducer_class: existing_batch.reducer_class.clone(),
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
            scheduled_at: existing_batch.updated_at,
            terminal_at: Some(projected.terminal_at),
            updated_at: projected.terminal_at,
        },
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
            task_queue: projected_apply.batch_task_queue.clone(),
            state: None,
            input_handle: Value::Null,
            result_handle: Value::Null,
            throughput_backend: ThroughputBackend::StreamV2.as_str().to_owned(),
            throughput_backend_version: ThroughputBackend::StreamV2.default_version().to_owned(),
            execution_policy: projected_apply.batch_execution_policy.clone(),
            reducer: projected_apply.batch_reducer.clone(),
            reducer_class: projected_apply.batch_reducer_class.clone(),
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
            scheduled_at: projected_apply.terminal_at.unwrap_or(projected.terminal_at),
            terminal_at: Some(projected.terminal_at),
            updated_at: projected.terminal_at,
        },
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
            terminal_at: Some(projected.terminal_at),
            updated_at: projected.terminal_at,
        },
    }
}

fn grouped_batch_terminal_projection_record(
    identity: &ThroughputBatchIdentity,
    projected: &ProjectedBatchTerminal,
) -> StreamProjectionRecord {
    StreamProjectionRecord {
        key: throughput_partition_key(&identity.batch_id, 0),
        event: grouped_batch_terminal_projection_event(identity, projected),
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

fn bulk_chunk_to_proto(record: &WorkflowBulkChunkRecord, supports_cbor: bool) -> BulkActivityTask {
    BulkActivityTask {
        tenant_id: record.tenant_id.clone(),
        definition_id: record.definition_id.clone(),
        definition_version: record.definition_version.unwrap_or_default(),
        artifact_hash: record.artifact_hash.clone().unwrap_or_default(),
        instance_id: record.instance_id.clone(),
        run_id: record.run_id.clone(),
        batch_id: record.batch_id.clone(),
        chunk_id: record.chunk_id.clone(),
        chunk_index: record.chunk_index,
        group_id: record.group_id,
        activity_type: record.activity_type.clone(),
        task_queue: record.task_queue.clone(),
        attempt: record.attempt,
        items_json: String::new(),
        input_handle_json: if supports_cbor {
            String::new()
        } else {
            serde_json::to_string(&record.input_handle).expect("bulk chunk input handle serializes")
        },
        items_cbor: Vec::new(),
        input_handle_cbor: if supports_cbor {
            encode_cbor(&record.input_handle, "bulk chunk input handle")
                .expect("bulk chunk input handle encodes as CBOR")
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
        lease_token: record.lease_token.map(|value| value.to_string()).unwrap_or_default(),
        lease_epoch: record.lease_epoch,
        owner_epoch: record.owner_epoch,
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

fn projected_apply_projection_records(
    report: &ThroughputChunkReport,
    projected: &ProjectedTerminalApply,
    artifacts: &ChunkTerminalArtifacts,
) -> Vec<StreamProjectionRecord> {
    let mut records = Vec::with_capacity(2);
    for event in projected_apply_projection_events(report, projected, artifacts) {
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
                terminal_at: projected.terminal_at,
                updated_at: report.occurred_at,
            },
        });
    }

    events.push(ThroughputProjectionEvent::UpdateChunkState {
        update: projected_apply_chunk_state_update(report, projected, artifacts),
    });
}

fn projected_apply_projection_events(
    report: &ThroughputChunkReport,
    projected: &ProjectedTerminalApply,
    artifacts: &ChunkTerminalArtifacts,
) -> Vec<ThroughputProjectionEvent> {
    let mut events = Vec::with_capacity(2);
    append_projected_apply_projection_events(&mut events, report, projected, artifacts);
    events
}

fn projected_apply_chunk_state_update(
    report: &ThroughputChunkReport,
    projected: &ProjectedTerminalApply,
    artifacts: &ChunkTerminalArtifacts,
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
        result_handle: artifacts.result_handle.clone(),
        output: None,
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

#[cfg(test)]
mod tests {
    use super::*;

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

        let records = projected_apply_projection_records(&report, &projected, &artifacts);

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
                terminal_at: batch.terminal_at,
                updated_at: batch.updated_at,
            },
        },
    }
}

fn grouped_batch_summary_projection_event(
    identity: &ThroughputBatchIdentity,
    summary: &ProjectedBatchGroupSummary,
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
            terminal_at: None,
            updated_at: summary.updated_at,
        },
    }
}

fn grouped_batch_summary_projection_record(
    identity: &ThroughputBatchIdentity,
    summary: &ProjectedBatchGroupSummary,
) -> StreamProjectionRecord {
    StreamProjectionRecord {
        key: throughput_partition_key(&identity.batch_id, 0),
        event: grouped_batch_summary_projection_event(identity, summary),
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
        publish_stream_batch_terminal_event(state, &cancelled_batch).await?;
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
        "task_queue_cap" => {
            debug.task_queue_throttle_events = debug.task_queue_throttle_events.saturating_add(1)
        }
        "batch_cap" => debug.batch_throttle_events = debug.batch_throttle_events.saturating_add(1),
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
