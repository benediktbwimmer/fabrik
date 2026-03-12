mod local_state;

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use anyhow::{Context, Result};
use axum::{Json, routing::get};
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
use fabrik_store::PartitionOwnershipRecord;
use fabrik_store::{
    ThroughputProjectionBatchStateUpdate, ThroughputProjectionChunkStateUpdate,
    ThroughputProjectionEvent, WorkflowBulkBatchStatus, WorkflowBulkChunkRecord,
    WorkflowBulkChunkStatus, WorkflowStore,
};
use fabrik_throughput::{
    CreateBatchCommand, PayloadHandle, PayloadStore, PayloadStoreConfig, PayloadStoreKind,
    ThroughputBackend, ThroughputBatchIdentity, ThroughputChangelogEntry,
    ThroughputChangelogPayload, ThroughputChunkReport, ThroughputChunkReportPayload,
    ThroughputCommand, ThroughputCommandEnvelope, ThroughputReportEnvelope,
    effective_aggregation_group_count, group_id_for_chunk_index, throughput_partition_key,
};
use fabrik_worker_protocol::activity_worker::{
    Ack, BulkActivityTask, BulkActivityTaskResult, PollBulkActivityTaskRequest,
    PollBulkActivityTaskResponse, ReportBulkActivityTaskResultsRequest,
    activity_worker_api_server::{ActivityWorkerApi, ActivityWorkerApiServer},
};
use futures_util::StreamExt;
use local_state::{
    LocalThroughputDebugSnapshot, LocalThroughputState, ProjectedBatchTerminal,
    ProjectedChunkReschedule, ProjectedGroupTerminal, ProjectedTerminalApply, ReportValidation,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Notify;
use tonic::{Request, Response, Status, transport::Server};
use tracing::{error, info, warn};
use uuid::Uuid;

const BULK_EVENT_OUTBOX_BATCH_SIZE: usize = 128;
const BULK_EVENT_OUTBOX_LEASE_SECONDS: i64 = 30;
const BULK_EVENT_OUTBOX_RETRY_MS: i64 = 250;
const MAX_BULK_ITEM_BYTES: usize = 256 * 1024;
const MAX_BULK_TOTAL_INPUT_BYTES: usize = 8 * 1024 * 1024;
const MAX_BULK_CHUNK_INPUT_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
struct AppState {
    store: WorkflowStore,
    local_state: LocalThroughputState,
    workflow_publisher: WorkflowPublisher,
    command_publisher: JsonTopicPublisher<ThroughputCommandEnvelope>,
    report_publisher: JsonTopicPublisher<ThroughputReportEnvelope>,
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
    bridged_batches: u64,
    duplicate_bridge_events: u64,
    bridge_failures: u64,
    reports_received: u64,
    reports_rejected: u64,
    terminal_events_published: u64,
    tenant_throttle_events: u64,
    task_queue_throttle_events: u64,
    batch_throttle_events: u64,
    apply_divergences: u64,
    ownership_catchup_waits: u64,
    last_bridge_at: Option<chrono::DateTime<chrono::Utc>>,
    last_report_at: Option<chrono::DateTime<chrono::Utc>>,
    last_terminal_event_at: Option<chrono::DateTime<chrono::Utc>>,
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
    let command_publisher = JsonTopicPublisher::new(
        &JsonTopicConfig::new(
            redpanda.brokers.clone(),
            redpanda.throughput_commands_topic,
            redpanda.throughput_partitions,
        ),
        "throughput-runtime-commands",
    )
    .await?;
    let report_publisher = JsonTopicPublisher::new(
        &JsonTopicConfig::new(
            redpanda.brokers.clone(),
            redpanda.throughput_reports_topic,
            redpanda.throughput_partitions,
        ),
        "throughput-runtime-reports",
    )
    .await?;
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
        command_publisher,
        report_publisher,
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
    let restored_from_checkpoint = restore_state_from_checkpoint(&state).await?;
    if restored_from_checkpoint {
        info!("throughput-runtime restored local state from latest checkpoint");
    } else {
        seed_local_state_from_store(&state).await?;
    }
    restore_local_state_from_changelog(&state, &changelog_config, true).await?;

    let consumer = build_workflow_consumer(
        &workflow_broker,
        "throughput-runtime",
        &workflow_broker.all_partition_ids(),
    )
    .await?;
    tokio::spawn(run_bridge_loop(state.clone(), consumer));
    tokio::spawn(run_changelog_consumer_loop(state.clone(), changelog_config.clone()));
    tokio::spawn(run_checkpoint_loop(state.clone()));
    tokio::spawn(run_terminal_state_gc_loop(state.clone()));
    tokio::spawn(run_group_barrier_reconciler(state.clone()));
    tokio::spawn(run_throughput_ownership_loop(
        state.store.clone(),
        ownership.clone(),
        state.local_state.clone(),
        managed_partitions.clone(),
        format!("http://127.0.0.1:{}/debug/throughput", debug_config.port),
        debug.clone(),
        ownership_config,
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

async fn run_bridge_loop(state: AppState, mut consumer: fabrik_broker::WorkflowConsumerStream) {
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
        if let WorkflowEvent::WorkflowCancellationRequested { reason } = &event.payload {
            if let Err(error) = cancel_stream_batches_for_run(&state, &event, reason).await {
                let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
                debug.bridge_failures = debug.bridge_failures.saturating_add(1);
                error!(error = %error, instance_id = %event.instance_id, run_id = %event.run_id, "failed to cancel stream-v2 batches for workflow cancellation");
            }
            continue;
        }
        let WorkflowEvent::BulkActivityBatchScheduled {
            batch_id,
            activity_type,
            task_queue,
            items,
            input_handle,
            result_handle,
            chunk_size,
            max_attempts,
            retry_delay_ms,
            aggregation_group_count: _,
            throughput_backend,
            throughput_backend_version,
            state: workflow_state,
        } = &event.payload
        else {
            continue;
        };
        if throughput_backend != ThroughputBackend::StreamV2.as_str() {
            continue;
        }
        let dedupe_key = format!("throughput-create:{}", event.event_id);
        let published_at = match state
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
        {
            Ok(published_at) => published_at,
            Err(error) => {
                let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
                debug.bridge_failures = debug.bridge_failures.saturating_add(1);
                error!(error = %error, batch_id = %batch_id, "failed to record throughput bridge progress");
                continue;
            }
        };
        if published_at.is_some() {
            let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
            debug.duplicate_bridge_events = debug.duplicate_bridge_events.saturating_add(1);
            continue;
        }

        let (batch, chunks) = match build_stream_projection_records(&state, &event).await {
            Ok(result) => result,
            Err(error) => {
                let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
                debug.bridge_failures = debug.bridge_failures.saturating_add(1);
                error!(error = %error, batch_id = %batch_id, "failed to build stream-v2 batch projection records");
                continue;
            }
        };
        if let Err(error) = sync_stream_batch_projection(&state, &batch).await {
            error!(error = %error, batch_id = %batch_id, "failed to project stream-v2 batch");
        }
        if let Err(error) = sync_stream_chunk_projections(&state, &chunks).await {
            error!(error = %error, batch_id = %batch_id, "failed to project stream-v2 chunks");
        }
        if let Err(error) = state.local_state.upsert_batch_record(&batch) {
            error!(error = %error, batch_id = %batch_id, "failed to seed local stream-v2 batch state");
        }
        for chunk in &chunks {
            if let Err(error) = state.local_state.upsert_chunk_record(chunk) {
                error!(error = %error, batch_id = %chunk.batch_id, chunk_id = %chunk.chunk_id, "failed to seed local stream-v2 chunk state");
            }
        }
        publish_changelog(
            &state,
            ThroughputChangelogPayload::BatchCreated {
                identity: ThroughputBatchIdentity {
                    tenant_id: event.tenant_id.clone(),
                    instance_id: event.instance_id.clone(),
                    run_id: event.run_id.clone(),
                    batch_id: batch_id.clone(),
                },
                task_queue: task_queue.clone(),
                aggregation_group_count: batch.aggregation_group_count,
                total_items: batch.total_items,
                chunk_count: batch.chunk_count,
            },
            throughput_partition_key(batch_id, 0),
        )
        .await;

        let effective_group_count = batch.aggregation_group_count;

        let command = ThroughputCommandEnvelope {
            command_id: Uuid::now_v7(),
            occurred_at: event.occurred_at,
            dedupe_key: dedupe_key.clone(),
            partition_key: throughput_partition_key(batch_id, 0),
            payload: ThroughputCommand::CreateBatch(CreateBatchCommand {
                dedupe_key,
                tenant_id: event.tenant_id.clone(),
                definition_id: event.definition_id.clone(),
                definition_version: event.definition_version,
                artifact_hash: event.artifact_hash.clone(),
                instance_id: event.instance_id.clone(),
                run_id: event.run_id.clone(),
                batch_id: batch_id.clone(),
                activity_type: activity_type.clone(),
                task_queue: task_queue.clone(),
                state: workflow_state.clone(),
                chunk_size: *chunk_size,
                max_attempts: *max_attempts,
                retry_delay_ms: *retry_delay_ms,
                total_items: items.len() as u32,
                aggregation_group_count: effective_group_count,
                throughput_backend: throughput_backend.clone(),
                throughput_backend_version: throughput_backend_version.clone(),
                input_handle: serde_json::from_value(input_handle.clone()).unwrap_or_else(|_| {
                    fabrik_throughput::PayloadHandle::inline_batch_input(batch_id)
                }),
                result_handle: serde_json::from_value(result_handle.clone()).unwrap_or_else(|_| {
                    fabrik_throughput::PayloadHandle::inline_batch_result(batch_id)
                }),
                items: items.clone(),
            }),
        };
        if let Err(error) = state.command_publisher.publish(&command, &command.partition_key).await
        {
            let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
            debug.bridge_failures = debug.bridge_failures.saturating_add(1);
            error!(error = %error, batch_id = %batch_id, "failed to publish throughput create command");
        } else {
            if let Err(error) = state
                .store
                .mark_throughput_bridge_command_published(event.event_id, Utc::now())
                .await
            {
                let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
                debug.bridge_failures = debug.bridge_failures.saturating_add(1);
                error!(error = %error, batch_id = %batch_id, "failed to mark throughput create command published");
            }
            let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
            debug.bridged_batches = debug.bridged_batches.saturating_add(1);
            debug.last_bridge_at = Some(Utc::now());
        }
        state.bulk_notify.notify_waiters();
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

async fn seed_local_state_from_store(state: &AppState) -> Result<()> {
    let batches = state
        .store
        .list_nonterminal_throughput_projection_batches_for_backend(
            ThroughputBackend::StreamV2.as_str(),
            10_000,
        )
        .await?;
    for batch in batches {
        state.local_state.upsert_batch_record(&batch)?;
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
        for chunk in &chunks {
            state.local_state.upsert_chunk_record(chunk)?;
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
) {
    let use_assignments = config.static_partition_ids.is_none();
    let lease_ttl = Duration::from_secs(config.lease_ttl_seconds.max(1));
    let renew_interval = Duration::from_secs(config.renew_interval_seconds.max(1));
    let heartbeat_ttl = Duration::from_secs(config.member_heartbeat_ttl_seconds.max(1));
    let poll_interval = Duration::from_secs(config.assignment_poll_interval_seconds.max(1));
    let rebalance_interval = Duration::from_secs(config.rebalance_interval_seconds.max(1));
    let mut last_rebalance_at = std::time::Instant::now() - rebalance_interval;
    loop {
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
            if let Err(error) =
                sync_stream_projected_requeue(&state, &chunk.identity, &projected).await
            {
                error!(error = %error, batch_id = %chunk.identity.batch_id, chunk_id = %chunk.chunk_id, "failed to project requeued stream-v2 chunk");
                continue;
            }
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

    async fn poll_bulk_activity_task(
        &self,
        request: Request<PollBulkActivityTaskRequest>,
    ) -> Result<Response<PollBulkActivityTaskResponse>, Status> {
        let request = request.into_inner();
        let batch_limit = (self.state.runtime.max_active_chunks_per_batch > 0)
            .then_some(self.state.runtime.max_active_chunks_per_batch);
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(request.poll_timeout_ms.max(1));
        loop {
            let owned = owned_throughput_partitions(&self.state);
            if owned.is_empty() {
                if tokio::time::Instant::now() >= deadline {
                    return Ok(Response::new(PollBulkActivityTaskResponse { task: None }));
                }
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if tokio::time::timeout(remaining, self.state.bulk_notify.notified()).await.is_err()
                {
                    return Ok(Response::new(PollBulkActivityTaskResponse { task: None }));
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
                    return Ok(Response::new(PollBulkActivityTaskResponse { task: None }));
                }
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if tokio::time::timeout(remaining, self.state.bulk_notify.notified()).await.is_err()
                {
                    return Ok(Response::new(PollBulkActivityTaskResponse { task: None }));
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
                    return Ok(Response::new(PollBulkActivityTaskResponse { task: None }));
                }
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if tokio::time::timeout(remaining, self.state.bulk_notify.notified()).await.is_err()
                {
                    return Ok(Response::new(PollBulkActivityTaskResponse { task: None }));
                }
                continue;
            }
            let candidate = self
                .state
                .local_state
                .next_ready_chunk(
                    &request.tenant_id,
                    &request.task_queue,
                    Utc::now(),
                    batch_limit,
                    Some(&owned),
                    self.state.throughput_partitions,
                )
                .map_err(internal_status)?;
            if let Some(candidate) = candidate {
                let projected = self
                    .state
                    .store
                    .get_bulk_chunk_query_view(
                        &candidate.identity.tenant_id,
                        &candidate.identity.instance_id,
                        &candidate.identity.run_id,
                        &candidate.identity.batch_id,
                        &candidate.chunk_id,
                    )
                    .await
                    .map_err(internal_status)?;
                if let Some(record) = projected {
                    let leased = lease_stream_projection_chunk(
                        &record,
                        &request.worker_id,
                        &request.worker_build_id,
                        chrono::Duration::seconds(self.state.runtime.lease_ttl_seconds as i64),
                    )
                    .map_err(internal_status)?;
                    if let Err(error) = sync_stream_projected_lease(&self.state, &leased).await {
                        error!(error = %error, batch_id = %leased.batch_id, chunk_id = %leased.chunk_id, "failed to project leased stream-v2 chunk");
                    }
                    publish_changelog(
                        &self.state,
                        ThroughputChangelogPayload::ChunkLeased {
                            identity: batch_identity(&leased),
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
                        throughput_partition_key(&leased.batch_id, leased.group_id),
                    )
                    .await;
                    let mut leased = leased;
                    leased.items =
                        resolve_chunk_items(&self.state, &leased).await.map_err(internal_status)?;
                    return Ok(Response::new(PollBulkActivityTaskResponse {
                        task: Some(bulk_chunk_to_proto(&leased)),
                    }));
                }
                if let Ok(Some(chunk)) = self
                    .state
                    .store
                    .get_bulk_chunk_query_view(
                        &candidate.identity.tenant_id,
                        &candidate.identity.instance_id,
                        &candidate.identity.run_id,
                        &candidate.identity.batch_id,
                        &candidate.chunk_id,
                    )
                    .await
                {
                    let _ = self.state.local_state.upsert_chunk_record(&chunk);
                }
            }
            if batch_limit.is_some()
                && self
                    .state
                    .local_state
                    .has_ready_chunk(
                        &request.tenant_id,
                        &request.task_queue,
                        Utc::now(),
                        Some(&owned),
                        self.state.throughput_partitions,
                    )
                    .map_err(internal_status)?
            {
                record_throttle(&self.state, "batch_cap");
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(Response::new(PollBulkActivityTaskResponse { task: None }));
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if tokio::time::timeout(remaining, self.state.bulk_notify.notified()).await.is_err() {
                return Ok(Response::new(PollBulkActivityTaskResponse { task: None }));
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
        for result in request.into_inner().results {
            {
                let mut debug = self.state.debug.lock().expect("throughput debug lock poisoned");
                debug.reports_received = debug.reports_received.saturating_add(1);
                debug.last_report_at = Some(Utc::now());
            }
            ensure_bulk_batch_backend(
                &self.state.store,
                &result.tenant_id,
                &result.instance_id,
                &result.run_id,
                &result.batch_id,
                ThroughputBackend::StreamV2.as_str(),
            )
            .await?;
            let report = throughput_report_from_result(result)?;
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
            self.state
                .report_publisher
                .publish(
                    &ThroughputReportEnvelope {
                        report_id: report.report_id.clone(),
                        occurred_at: report.occurred_at,
                        partition_key: throughput_partition_key(&report.batch_id, report.group_id),
                        payload: report.clone(),
                    },
                    &throughput_partition_key(&report.batch_id, report.group_id),
                )
                .await
                .map_err(internal_status)?;
            let identity = ThroughputBatchIdentity {
                tenant_id: report.tenant_id.clone(),
                instance_id: report.instance_id.clone(),
                run_id: report.run_id.clone(),
                batch_id: report.batch_id.clone(),
            };
            let validation = self
                .state
                .local_state
                .validate_report(
                    &identity,
                    &report.chunk_id,
                    report.attempt,
                    report.lease_epoch,
                    &report.lease_token,
                    report.owner_epoch,
                    &report.report_id,
                )
                .map_err(internal_status)?;
            if validation != ReportValidation::Accept {
                let mut debug = self.state.debug.lock().expect("throughput debug lock poisoned");
                debug.reports_rejected = debug.reports_rejected.saturating_add(1);
                debug.last_report_rejection_reason =
                    Some(report_validation_label(validation).to_owned());
                continue;
            }
            let projected = self
                .state
                .local_state
                .project_report_apply(&report)
                .map_err(internal_status)?
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "report {} could not be projected from local owner state",
                        report.report_id
                    ))
                })?;
            sync_stream_projected_apply(&self.state, &report, &projected)
                .await
                .map_err(internal_status)?;
            publish_changelog(
                &self.state,
                ThroughputChangelogPayload::ChunkApplied {
                    identity: ThroughputBatchIdentity {
                        tenant_id: report.tenant_id.clone(),
                        instance_id: report.instance_id.clone(),
                        run_id: report.run_id.clone(),
                        batch_id: report.batch_id.clone(),
                    },
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
                    error: projected.chunk_error.clone(),
                },
                throughput_partition_key(&report.batch_id, report.group_id),
            )
            .await;
            if let Ok(Some(group_terminal)) = self.state.local_state.project_group_terminal(
                &identity,
                report.group_id,
                report.occurred_at,
            ) {
                maybe_publish_group_terminal(
                    &self.state,
                    &identity,
                    report.group_id,
                    group_terminal,
                )
                .await;
            }
            if projected.batch_terminal {
                publish_changelog(
                    &self.state,
                    ThroughputChangelogPayload::BatchTerminal {
                        identity: ThroughputBatchIdentity {
                            tenant_id: report.tenant_id.clone(),
                            instance_id: report.instance_id.clone(),
                            run_id: report.run_id.clone(),
                            batch_id: report.batch_id.clone(),
                        },
                        status: projected.batch_status.clone(),
                        report_id: report.report_id.clone(),
                        succeeded_items: projected.batch_succeeded_items,
                        failed_items: projected.batch_failed_items,
                        cancelled_items: projected.batch_cancelled_items,
                        error: projected.batch_error.clone(),
                        terminal_at: projected.terminal_at.unwrap_or(report.occurred_at),
                    },
                    throughput_partition_key(&report.batch_id, report.group_id),
                )
                .await;
                publish_stream_terminal_event(&self.state, &report, &projected)
                    .await
                    .map_err(internal_status)?;
                let mut debug = self.state.debug.lock().expect("throughput debug lock poisoned");
                debug.terminal_events_published = debug.terminal_events_published.saturating_add(1);
                debug.last_terminal_event_at = Some(Utc::now());
            }
            if projected.chunk_status == WorkflowBulkChunkStatus::Scheduled.as_str()
                || projected.batch_terminal_deferred
            {
                self.state.bulk_notify.notify_waiters();
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
        input_handle,
        result_handle,
        chunk_size,
        max_attempts,
        retry_delay_ms,
        aggregation_group_count,
        throughput_backend,
        throughput_backend_version,
        state,
    } = &event.payload
    else {
        anyhow::bail!("expected BulkActivityBatchScheduled event");
    };

    let chunk_size_usize =
        usize::try_from((*chunk_size).max(1)).context("bulk chunk size exceeds usize")?;
    let mut total_input_bytes = 0usize;
    for (chunk_index, chunk_items) in items.chunks(chunk_size_usize).enumerate() {
        let mut chunk_bytes = 0usize;
        for item in chunk_items {
            let item_bytes = serde_json::to_vec(item)
                .map(|bytes| bytes.len())
                .context("failed to serialize bulk batch item")?;
            if item_bytes > MAX_BULK_ITEM_BYTES {
                anyhow::bail!(
                    "bulk batch item exceeds per-item byte limit {}",
                    MAX_BULK_ITEM_BYTES
                );
            }
            total_input_bytes = total_input_bytes.saturating_add(item_bytes);
            chunk_bytes = chunk_bytes.saturating_add(item_bytes);
        }
        if chunk_bytes > MAX_BULK_CHUNK_INPUT_BYTES {
            anyhow::bail!(
                "bulk chunk {} serialized to {} bytes, over limit {}",
                chunk_index,
                chunk_bytes,
                MAX_BULK_CHUNK_INPUT_BYTES
            );
        }
    }
    if total_input_bytes > MAX_BULK_TOTAL_INPUT_BYTES {
        anyhow::bail!(
            "bulk batch input serialized to {} bytes, over limit {}",
            total_input_bytes,
            MAX_BULK_TOTAL_INPUT_BYTES
        );
    }

    let chunk_count = if items.is_empty() {
        0
    } else {
        ((items.len() + chunk_size_usize - 1) / chunk_size_usize) as u32
    };
    let effective_group_count =
        planned_aggregation_group_count(*aggregation_group_count, chunk_count, &app_state.runtime);
    let batch_input_handle =
        maybe_externalize_batch_input(app_state, batch_id, items, total_input_bytes, input_handle)
            .await?;
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
        input_handle: batch_input_handle,
        result_handle: batch_result_handle,
        throughput_backend: throughput_backend.clone(),
        throughput_backend_version: throughput_backend_version.clone(),
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
    let mut chunks = Vec::new();
    for (chunk_index, chunk_items) in items.chunks(chunk_size_usize).enumerate() {
        let chunk_index = chunk_index as u32;
        let chunk_id = format!("{batch_id}::{chunk_index}");
        let group_id = group_id_for_chunk_index(chunk_index, effective_group_count);
        let (items_inline, input_handle) =
            maybe_externalize_chunk_input(app_state, batch_id, &chunk_id, chunk_items).await?;
        chunks.push(WorkflowBulkChunkRecord {
            tenant_id: event.tenant_id.clone(),
            instance_id: event.instance_id.clone(),
            run_id: event.run_id.clone(),
            definition_id: event.definition_id.clone(),
            definition_version: Some(event.definition_version),
            artifact_hash: Some(event.artifact_hash.clone()),
            batch_id: batch_id.clone(),
            chunk_id,
            chunk_index,
            group_id,
            item_count: chunk_items.len() as u32,
            activity_type: activity_type.clone(),
            task_queue: task_queue.clone(),
            state: state.clone(),
            status: WorkflowBulkChunkStatus::Scheduled,
            attempt: 1,
            lease_epoch: 0,
            owner_epoch: 1,
            max_attempts: *max_attempts,
            retry_delay_ms: *retry_delay_ms,
            input_handle,
            result_handle: Value::Null,
            items: items_inline,
            output: None,
            error: None,
            cancellation_requested: false,
            cancellation_reason: None,
            cancellation_metadata: None,
            worker_id: None,
            worker_build_id: None,
            lease_token: None,
            last_report_id: None,
            scheduled_at: event.occurred_at,
            available_at: event.occurred_at,
            started_at: None,
            lease_expires_at: None,
            completed_at: None,
            updated_at: event.occurred_at,
        });
    }
    Ok((batch, chunks))
}

fn lease_stream_projection_chunk(
    chunk: &WorkflowBulkChunkRecord,
    worker_id: &str,
    worker_build_id: &str,
    lease_ttl: chrono::Duration,
) -> Result<WorkflowBulkChunkRecord> {
    let now = Utc::now();
    let mut leased = chunk.clone();
    leased.status = WorkflowBulkChunkStatus::Started;
    leased.worker_id = Some(worker_id.to_owned());
    leased.worker_build_id = Some(worker_build_id.to_owned());
    leased.lease_token = Some(Uuid::now_v7());
    leased.lease_epoch = leased.lease_epoch.saturating_add(1);
    leased.started_at = leased.started_at.or(Some(now));
    leased.lease_expires_at = Some(now + lease_ttl);
    leased.updated_at = now;
    Ok(leased)
}

fn planned_aggregation_group_count(
    requested_group_count: u32,
    chunk_count: u32,
    config: &ThroughputRuntimeConfig,
) -> u32 {
    if chunk_count == 0 {
        return 1;
    }
    if requested_group_count > 1 {
        return effective_aggregation_group_count(requested_group_count, chunk_count);
    }
    let threshold = config.grouping_chunk_threshold.max(1);
    let target_chunks_per_group = config.target_chunks_per_group.max(1);
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

async fn maybe_externalize_batch_input(
    state: &AppState,
    batch_id: &str,
    items: &[Value],
    total_input_bytes: usize,
    default_handle: &Value,
) -> Result<Value> {
    if total_input_bytes <= state.runtime.inline_chunk_input_threshold_bytes {
        return Ok(default_handle.clone());
    }
    let handle = state
        .payload_store
        .write_value(&format!("batches/{batch_id}/input"), &Value::Array(items.to_vec()))
        .await?;
    serde_json::to_value(handle).context("failed to serialize batch input handle")
}

async fn maybe_initialize_batch_result_manifest(
    state: &AppState,
    batch_id: &str,
    default_handle: &Value,
) -> Result<Value> {
    let manifest = serde_json::json!({
        "kind": "chunk-result-manifest",
        "batchId": batch_id,
        "chunks": []
    });
    let handle = state
        .payload_store
        .write_value(&format!("batches/{batch_id}/results/manifest"), &manifest)
        .await?;
    let serialized =
        serde_json::to_value(handle).context("failed to serialize batch result handle")?;
    if default_handle.is_null() { Ok(serialized) } else { Ok(serialized) }
}

async fn maybe_externalize_chunk_input(
    state: &AppState,
    batch_id: &str,
    chunk_id: &str,
    items: &[Value],
) -> Result<(Vec<Value>, Value)> {
    let payload = Value::Array(items.to_vec());
    let item_bytes = serde_json::to_vec(&payload)
        .map(|bytes| bytes.len())
        .context("failed to serialize chunk input payload")?;
    if item_bytes <= state.runtime.inline_chunk_input_threshold_bytes {
        return Ok((items.to_vec(), Value::Null));
    }
    let handle = state
        .payload_store
        .write_value(&format!("batches/{batch_id}/chunks/{chunk_id}/input"), &payload)
        .await?;
    Ok((
        Vec::new(),
        serde_json::to_value(handle).context("failed to serialize chunk input handle")?,
    ))
}

async fn maybe_externalize_chunk_output(
    state: &AppState,
    batch_id: &str,
    chunk_id: &str,
    output: &[Value],
) -> Result<(Option<Vec<Value>>, Value)> {
    let payload = Value::Array(output.to_vec());
    let output_bytes = serde_json::to_vec(&payload)
        .map(|bytes| bytes.len())
        .context("failed to serialize chunk output payload")?;
    if output_bytes <= state.runtime.inline_chunk_output_threshold_bytes {
        return Ok((Some(output.to_vec()), Value::Null));
    }
    let handle = state
        .payload_store
        .write_value(&format!("batches/{batch_id}/chunks/{chunk_id}/output"), &payload)
        .await?;
    Ok((None, serde_json::to_value(handle).context("failed to serialize chunk output handle")?))
}

async fn resolve_chunk_items(
    state: &AppState,
    chunk: &WorkflowBulkChunkRecord,
) -> Result<Vec<Value>> {
    if !chunk.items.is_empty() {
        return Ok(chunk.items.clone());
    }
    if let Ok(handle) =
        serde_json::from_value::<fabrik_throughput::PayloadHandle>(chunk.input_handle.clone())
    {
        return state.payload_store.read_items(&handle).await;
    }
    Ok(Vec::new())
}

async fn publish_stream_terminal_event(
    state: &AppState,
    report: &ThroughputChunkReport,
    projected: &ProjectedTerminalApply,
) -> Result<()> {
    let Some(batch) = state
        .store
        .get_bulk_batch_query_view(
            &report.tenant_id,
            &report.instance_id,
            &report.run_id,
            &report.batch_id,
        )
        .await?
    else {
        anyhow::bail!("stream-v2 batch {} not found in query projection", report.batch_id);
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
    let payload = match projected.batch_status.as_str() {
        "completed" => WorkflowEvent::BulkActivityBatchCompleted {
            batch_id: report.batch_id.clone(),
            total_items: batch.total_items,
            succeeded_items: projected.batch_succeeded_items,
            failed_items: projected.batch_failed_items,
            cancelled_items: projected.batch_cancelled_items,
            chunk_count: batch.chunk_count,
        },
        "failed" => WorkflowEvent::BulkActivityBatchFailed {
            batch_id: report.batch_id.clone(),
            total_items: batch.total_items,
            succeeded_items: projected.batch_succeeded_items,
            failed_items: projected.batch_failed_items,
            cancelled_items: projected.batch_cancelled_items,
            chunk_count: batch.chunk_count,
            message: projected
                .batch_error
                .clone()
                .unwrap_or_else(|| "bulk batch failed".to_owned()),
        },
        _ => WorkflowEvent::BulkActivityBatchCancelled {
            batch_id: report.batch_id.clone(),
            total_items: batch.total_items,
            succeeded_items: projected.batch_succeeded_items,
            failed_items: projected.batch_failed_items,
            cancelled_items: projected.batch_cancelled_items,
            chunk_count: batch.chunk_count,
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
    let Some(existing_batch) = state
        .store
        .get_bulk_batch_query_view(
            &identity.tenant_id,
            &identity.instance_id,
            &identity.run_id,
            &identity.batch_id,
        )
        .await?
    else {
        anyhow::bail!("grouped throughput batch {} missing from query view", identity.batch_id);
    };
    if matches!(
        existing_batch.status,
        WorkflowBulkBatchStatus::Completed
            | WorkflowBulkBatchStatus::Failed
            | WorkflowBulkBatchStatus::Cancelled
    ) {
        return Ok(());
    }

    let update = ThroughputProjectionBatchStateUpdate {
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
    };
    publish_projection_event(
        state,
        ThroughputProjectionEvent::UpdateBatchState { update: update.clone() },
        throughput_partition_key(&identity.batch_id, 0),
    )
    .await?;

    let mut terminal_batch = existing_batch.clone();
    terminal_batch.status = match projected.status.as_str() {
        "completed" => WorkflowBulkBatchStatus::Completed,
        "failed" => WorkflowBulkBatchStatus::Failed,
        "cancelled" => WorkflowBulkBatchStatus::Cancelled,
        other => anyhow::bail!("unexpected grouped batch terminal status {other}"),
    };
    terminal_batch.succeeded_items = projected.succeeded_items;
    terminal_batch.failed_items = projected.failed_items;
    terminal_batch.cancelled_items = projected.cancelled_items;
    terminal_batch.error = projected.error.clone();
    terminal_batch.terminal_at = Some(projected.terminal_at);
    terminal_batch.updated_at = projected.terminal_at;

    publish_changelog(
        state,
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
        throughput_partition_key(&identity.batch_id, 0),
    )
    .await;
    publish_stream_batch_terminal_event(state, &terminal_batch).await?;
    let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
    debug.terminal_events_published = debug.terminal_events_published.saturating_add(1);
    debug.last_terminal_event_at = Some(Utc::now());
    Ok(())
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
    }
}

async fn publish_projection_event(
    state: &AppState,
    payload: ThroughputProjectionEvent,
    key: String,
) -> Result<()> {
    state
        .projection_publisher
        .publish(&payload, &key)
        .await
        .context("failed to publish throughput projection event")
}

async fn maybe_publish_group_terminal(
    state: &AppState,
    identity: &ThroughputBatchIdentity,
    group_id: u32,
    projected: ProjectedGroupTerminal,
) {
    publish_changelog(
        state,
        ThroughputChangelogPayload::GroupTerminal {
            identity: identity.clone(),
            group_id,
            status: projected.status,
            succeeded_items: projected.succeeded_items,
            failed_items: projected.failed_items,
            cancelled_items: projected.cancelled_items,
            error: projected.error,
            terminal_at: projected.terminal_at,
        },
        throughput_partition_key(&identity.batch_id, group_id),
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
        ) => ThroughputChunkReportPayload::ChunkCompleted {
            output: serde_json::from_str(&completed.output_json)
                .map_err(|error| Status::invalid_argument(error.to_string()))?,
        },
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
            let metadata = if cancelled.metadata_json.trim().is_empty() {
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

fn bulk_chunk_to_proto(record: &WorkflowBulkChunkRecord) -> BulkActivityTask {
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
        items_json: serde_json::to_string(&record.items).expect("bulk chunk items serialize"),
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

async fn sync_stream_projected_apply(
    state: &AppState,
    report: &ThroughputChunkReport,
    projected: &ProjectedTerminalApply,
) -> Result<()> {
    let batch_update = ThroughputProjectionBatchStateUpdate {
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
    };
    publish_projection_event(
        state,
        ThroughputProjectionEvent::UpdateBatchState { update: batch_update },
        throughput_partition_key(&report.batch_id, report.group_id),
    )
    .await
    .with_context(|| {
        format!("failed to enqueue projected throughput batch state for {}", report.batch_id)
    })?;

    let (output, result_handle, cancellation_reason, cancellation_metadata) = match &report.payload
    {
        ThroughputChunkReportPayload::ChunkCompleted { output } => {
            let (output_inline, result_handle) =
                maybe_externalize_chunk_output(state, &report.batch_id, &report.chunk_id, output)
                    .await?;
            (output_inline, Some(result_handle), None, None)
        }
        ThroughputChunkReportPayload::ChunkFailed { .. } => (None, None, None, None),
        ThroughputChunkReportPayload::ChunkCancelled { reason, metadata } => {
            (None, None, Some(reason.clone()), metadata.clone())
        }
    };
    let chunk_update = ThroughputProjectionChunkStateUpdate {
        tenant_id: report.tenant_id.clone(),
        instance_id: report.instance_id.clone(),
        run_id: report.run_id.clone(),
        batch_id: report.batch_id.clone(),
        chunk_id: report.chunk_id.clone(),
        status: projected.chunk_status.clone(),
        attempt: projected.chunk_attempt,
        lease_epoch: report.lease_epoch,
        owner_epoch: report.owner_epoch,
        input_handle: None,
        result_handle,
        output,
        error: projected.chunk_error.clone(),
        cancellation_requested: false,
        cancellation_reason,
        cancellation_metadata,
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
    };
    publish_projection_event(
        state,
        ThroughputProjectionEvent::UpdateChunkState { update: chunk_update },
        throughput_partition_key(&report.batch_id, report.group_id),
    )
    .await?;
    Ok(())
}

async fn sync_stream_projected_lease(
    state: &AppState,
    chunk: &WorkflowBulkChunkRecord,
) -> Result<()> {
    mark_stream_batch_running_projection(state, chunk).await?;
    let chunk_update = ThroughputProjectionChunkStateUpdate {
        tenant_id: chunk.tenant_id.clone(),
        instance_id: chunk.instance_id.clone(),
        run_id: chunk.run_id.clone(),
        batch_id: chunk.batch_id.clone(),
        chunk_id: chunk.chunk_id.clone(),
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
    publish_projection_event(
        state,
        ThroughputProjectionEvent::UpdateChunkState { update: chunk_update },
        throughput_partition_key(&chunk.batch_id, chunk.group_id),
    )
    .await
    .with_context(|| {
        format!(
            "failed to enqueue leased throughput chunk state for {} in {}",
            chunk.chunk_id, chunk.batch_id
        )
    })?;
    Ok(())
}

async fn sync_stream_projected_requeue(
    state: &AppState,
    identity: &ThroughputBatchIdentity,
    projected: &ProjectedChunkReschedule,
) -> Result<()> {
    let chunk_update = ThroughputProjectionChunkStateUpdate {
        tenant_id: identity.tenant_id.clone(),
        instance_id: identity.instance_id.clone(),
        run_id: identity.run_id.clone(),
        batch_id: identity.batch_id.clone(),
        chunk_id: projected.chunk_id.clone(),
        status: WorkflowBulkChunkStatus::Scheduled.as_str().to_owned(),
        attempt: projected.attempt,
        lease_epoch: projected.lease_epoch,
        owner_epoch: projected.owner_epoch,
        input_handle: None,
        result_handle: None,
        output: None,
        error: None,
        cancellation_requested: false,
        cancellation_reason: None,
        cancellation_metadata: None,
        worker_id: None,
        worker_build_id: None,
        lease_token: None,
        last_report_id: None,
        available_at: projected.available_at,
        started_at: projected.started_at,
        lease_expires_at: None,
        completed_at: None,
        updated_at: projected.available_at,
    };
    publish_projection_event(
        state,
        ThroughputProjectionEvent::UpdateChunkState { update: chunk_update },
        throughput_partition_key(&identity.batch_id, projected.group_id),
    )
    .await
    .with_context(|| {
        format!(
            "failed to enqueue requeued throughput chunk state for {} in {}",
            projected.chunk_id, identity.batch_id
        )
    })?;
    Ok(())
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

async fn sync_stream_chunk_projection(
    state: &AppState,
    chunk: &WorkflowBulkChunkRecord,
) -> Result<()> {
    state.store.upsert_throughput_projection_chunk(chunk).await.with_context(|| {
        format!(
            "failed to sync throughput projection chunk {} for batch {}",
            chunk.chunk_id, chunk.batch_id
        )
    })
}

async fn sync_stream_chunk_projections(
    state: &AppState,
    chunks: &[WorkflowBulkChunkRecord],
) -> Result<()> {
    for chunk in chunks {
        sync_stream_chunk_projection(state, chunk).await?;
    }
    Ok(())
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
        let dedupe_key = format!("throughput-cancel:{}:{}", event.event_id, batch.batch_id);
        let partition_key = throughput_partition_key(&batch.batch_id, 0);
        state
            .command_publisher
            .publish(
                &ThroughputCommandEnvelope {
                    command_id: Uuid::now_v7(),
                    occurred_at: event.occurred_at,
                    dedupe_key: dedupe_key.clone(),
                    partition_key: partition_key.clone(),
                    payload: ThroughputCommand::CancelBatch {
                        identity: ThroughputBatchIdentity {
                            tenant_id: batch.tenant_id.clone(),
                            instance_id: batch.instance_id.clone(),
                            run_id: batch.run_id.clone(),
                            batch_id: batch.batch_id.clone(),
                        },
                        reason: reason.to_owned(),
                    },
                },
                &partition_key,
            )
            .await
            .with_context(|| {
                format!("failed to publish throughput cancel command for {}", batch.batch_id)
            })?;

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
                    error: Some(reason.to_owned()),
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

async fn mark_stream_batch_running_projection(
    state: &AppState,
    chunk: &WorkflowBulkChunkRecord,
) -> Result<()> {
    let Some(batch) = state
        .store
        .get_bulk_batch_query_view(
            &chunk.tenant_id,
            &chunk.instance_id,
            &chunk.run_id,
            &chunk.batch_id,
        )
        .await?
    else {
        anyhow::bail!("stream-v2 batch {} not found", chunk.batch_id);
    };
    if batch.status == WorkflowBulkBatchStatus::Scheduled {
        publish_projection_event(
            state,
            ThroughputProjectionEvent::UpdateBatchState {
                update: ThroughputProjectionBatchStateUpdate {
                    tenant_id: batch.tenant_id.clone(),
                    instance_id: batch.instance_id.clone(),
                    run_id: batch.run_id.clone(),
                    batch_id: batch.batch_id.clone(),
                    status: WorkflowBulkBatchStatus::Running.as_str().to_owned(),
                    succeeded_items: batch.succeeded_items,
                    failed_items: batch.failed_items,
                    cancelled_items: batch.cancelled_items,
                    error: batch.error.clone(),
                    terminal_at: batch.terminal_at,
                    updated_at: Utc::now(),
                },
            },
            throughput_partition_key(&batch.batch_id, 0),
        )
        .await?;
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
