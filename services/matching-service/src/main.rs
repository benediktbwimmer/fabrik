use std::{
    collections::{HashMap, VecDeque},
    net::{Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use anyhow::Result;
use axum::{Json, extract::State as AxumState, routing::get};
use chrono::{DateTime, Utc};
use fabrik_broker::{
    BrokerConfig, WorkflowPublisher, build_workflow_consumer, decode_consumed_workflow_event,
};
use fabrik_config::{
    GrpcServiceConfig, HttpServiceConfig, MatchingRuntimeConfig, PostgresConfig, RedpandaConfig,
};
use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
use fabrik_service::{ServiceInfo, default_router, serve};
use fabrik_store::{
    ActivityStartUpdate, ActivityTerminalPayload, ActivityTerminalUpdate,
    AppliedActivityTerminalUpdate, BulkChunkTerminalPayload, BulkChunkTerminalUpdate,
    TaskQueueKind, WorkflowActivityRecord, WorkflowActivityStatus, WorkflowBulkChunkRecord,
    WorkflowMailboxKind, WorkflowResumeKind, WorkflowStore, WorkflowTaskRecord,
};
use fabrik_throughput::{
    ThroughputBackend, bulk_reducer_class, decode_cbor, encode_cbor, planned_reduction_tree_depth,
    stream_v2_fast_lane_enabled,
};
use fabrik_worker_protocol::activity_worker::{
    Ack, ActivityTask, ActivityTaskResult, BulkActivityTask, BulkActivityTaskResult,
    CompleteActivityTaskRequest, CompleteWorkflowTaskRequest, FailActivityTaskRequest,
    FailWorkflowTaskRequest, PollActivityTaskRequest, PollActivityTaskResponse,
    PollActivityTasksRequest, PollActivityTasksResponse, PollBulkActivityTaskRequest,
    PollBulkActivityTaskResponse, PollWorkflowTaskRequest, PollWorkflowTaskResponse,
    RecordActivityHeartbeatRequest, RecordActivityHeartbeatResponse,
    ReportActivityTaskCancelledRequest, ReportActivityTaskResultsRequest,
    ReportBulkActivityTaskResultsRequest, WorkflowTask, activity_task_result,
    activity_worker_api_server::{ActivityWorkerApi, ActivityWorkerApiServer},
    workflow_worker_api_server::{WorkflowWorkerApi, WorkflowWorkerApiServer},
};
use futures_util::StreamExt;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{
    Mutex, Notify,
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    oneshot,
};
use tonic::{Request, Response, Status, transport::Server};
use tracing::{error, info, warn};
use uuid::Uuid;

const ACTIVITY_POLL_PREFETCH_SIZE: usize = 8;
const BULK_EVENT_OUTBOX_BATCH_SIZE: usize = 128;
const BULK_EVENT_OUTBOX_LEASE_SECONDS: i64 = 30;
const BULK_EVENT_OUTBOX_RETRY_MS: i64 = 250;
#[derive(Clone)]
struct AppState {
    store: WorkflowStore,
    publisher: WorkflowPublisher,
    queues: Arc<QueueIndex>,
    activity_prefetch: Arc<ActivityPrefetchIndex>,
    bulk_prefetch: Arc<BulkPrefetchIndex>,
    workflow_poller_cache: Arc<WorkflowPollerCache>,
    workflow_notify: Arc<Notify>,
    bulk_notify: Arc<Notify>,
    outbox_notify: Arc<Notify>,
    runtime: MatchingRuntimeConfig,
    debug: Arc<StdMutex<MatchingDebugState>>,
    result_submitter: UnboundedSender<PendingActivityResultRequest>,
    outbox_publisher_id: String,
}

#[derive(Debug)]
struct PendingActivityResultRequest {
    updates: Vec<PreparedActivityTerminalUpdate>,
    response_tx: oneshot::Sender<std::result::Result<(), Status>>,
}

#[derive(Debug, Clone, Serialize)]
struct MatchingDebugState {
    started_at: DateTime<Utc>,
    last_apply_finished_at: Option<DateTime<Utc>>,
    activity_result_apply_batch_max_items: usize,
    activity_result_apply_batch_max_bytes: usize,
    activity_result_apply_flush_interval_ms: u64,
    result_apply_per_run_coalescing_cap: usize,
    received_requests: u64,
    received_results: u64,
    received_result_bytes: u64,
    applied_batches: u64,
    applied_results: u64,
    skipped_results: u64,
    published_terminal_events: u64,
    completed_results: u64,
    failed_results: u64,
    cancelled_results: u64,
    avg_batch_size: f64,
    max_batch_size: u64,
    avg_batch_bytes: f64,
    max_batch_bytes: u64,
    avg_apply_latency_ms: f64,
    max_apply_latency_ms: u64,
    applied_results_per_second: f64,
    published_terminal_events_per_second: f64,
}

#[derive(Debug, Clone)]
struct PreparedActivityTerminalUpdate {
    update: ActivityTerminalUpdate,
    run_key: ActivityRunKey,
    estimated_bytes: usize,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct ActivityRunKey {
    tenant_id: String,
    instance_id: String,
    run_id: String,
}

#[derive(Debug, Clone)]
struct QueuedActivityResult {
    request_index: usize,
    prepared: PreparedActivityTerminalUpdate,
}

#[derive(Debug)]
struct PendingRequestProgress {
    response_tx: Option<oneshot::Sender<std::result::Result<(), Status>>>,
    remaining_updates: usize,
}

struct QueueIndex {
    queues: Mutex<HashMap<String, VecDeque<WorkflowActivityRecord>>>,
    notify: Notify,
}

impl QueueIndex {
    fn new() -> Self {
        Self { queues: Mutex::new(HashMap::new()), notify: Notify::new() }
    }

    async fn enqueue(&self, queue_key: &str, record: WorkflowActivityRecord) {
        let mut queues = self.queues.lock().await;
        queues.entry(queue_key.to_owned()).or_default().push_back(record);
        drop(queues);
        self.notify.notify_waiters();
    }

    async fn pop(&self, queue_key: &str) -> Option<WorkflowActivityRecord> {
        let mut queues = self.queues.lock().await;
        queues.get_mut(queue_key).and_then(VecDeque::pop_front)
    }
}

struct ActivityPrefetchIndex {
    buffers: Mutex<HashMap<String, VecDeque<WorkflowActivityRecord>>>,
}

impl ActivityPrefetchIndex {
    fn new() -> Self {
        Self { buffers: Mutex::new(HashMap::new()) }
    }

    async fn pop(&self, worker_key: &str) -> Option<WorkflowActivityRecord> {
        let mut buffers = self.buffers.lock().await;
        let Some(buffer) = buffers.get_mut(worker_key) else {
            return None;
        };
        let item = buffer.pop_front();
        if buffer.is_empty() {
            buffers.remove(worker_key);
        }
        item
    }
}

struct BulkPrefetchIndex {
    buffers: Mutex<HashMap<String, VecDeque<WorkflowBulkChunkRecord>>>,
}

impl BulkPrefetchIndex {
    fn new() -> Self {
        Self { buffers: Mutex::new(HashMap::new()) }
    }

    async fn pop(&self, worker_key: &str) -> Option<WorkflowBulkChunkRecord> {
        let mut buffers = self.buffers.lock().await;
        let Some(buffer) = buffers.get_mut(worker_key) else {
            return None;
        };
        let item = buffer.pop_front();
        if buffer.is_empty() {
            buffers.remove(worker_key);
        }
        item
    }

    async fn push_remaining(
        &self,
        worker_key: &str,
        records: impl IntoIterator<Item = WorkflowBulkChunkRecord>,
    ) {
        let mut buffers = self.buffers.lock().await;
        let buffer = buffers.entry(worker_key.to_owned()).or_default();
        buffer.extend(records);
    }
}

struct WorkflowPollerCache {
    build_queues: Mutex<HashMap<String, CachedBuildQueues>>,
    worker_heartbeats: Mutex<HashMap<String, DateTime<Utc>>>,
}

#[derive(Clone)]
struct CachedBuildQueues {
    queues: Vec<(String, String)>,
    expires_at: DateTime<Utc>,
}

impl WorkflowPollerCache {
    fn new() -> Self {
        Self {
            build_queues: Mutex::new(HashMap::new()),
            worker_heartbeats: Mutex::new(HashMap::new()),
        }
    }

    async fn heartbeat_due(
        &self,
        worker_id: &str,
        worker_build_id: &str,
        partition_id: i32,
        now: DateTime<Utc>,
    ) -> bool {
        let key = workflow_poller_cache_key(worker_id, worker_build_id, partition_id);
        let heartbeats = self.worker_heartbeats.lock().await;
        heartbeats.get(&key).map(|deadline| *deadline <= now).unwrap_or(true)
    }

    async fn mark_heartbeat(
        &self,
        worker_id: &str,
        worker_build_id: &str,
        partition_id: i32,
        next_due_at: DateTime<Utc>,
    ) {
        let key = workflow_poller_cache_key(worker_id, worker_build_id, partition_id);
        self.worker_heartbeats.lock().await.insert(key, next_due_at);
    }

    async fn cached_queues(
        &self,
        worker_build_id: &str,
        now: DateTime<Utc>,
    ) -> Option<Vec<(String, String)>> {
        let build_queues = self.build_queues.lock().await;
        build_queues
            .get(worker_build_id)
            .and_then(|entry| (entry.expires_at > now).then(|| entry.queues.clone()))
    }

    async fn store_queues(
        &self,
        worker_build_id: &str,
        queues: Vec<(String, String)>,
        expires_at: DateTime<Utc>,
    ) {
        self.build_queues
            .lock()
            .await
            .insert(worker_build_id.to_owned(), CachedBuildQueues { queues, expires_at });
    }
}

#[derive(Clone)]
struct ActivityApi {
    state: AppState,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = GrpcServiceConfig::from_env("MATCHING_SERVICE", "matching-service", 50051)?;
    let debug_config =
        HttpServiceConfig::from_env("MATCHING_DEBUG", "matching-service-debug", 3004)?;
    let redpanda = RedpandaConfig::from_env()?;
    let postgres = PostgresConfig::from_env()?;
    let runtime = MatchingRuntimeConfig::from_env()?;
    fabrik_service::init_tracing(&config.log_filter);

    let broker = BrokerConfig::new(
        redpanda.brokers,
        redpanda.workflow_events_topic,
        redpanda.workflow_events_partitions,
    );
    let store = WorkflowStore::connect(&postgres.url).await?;
    store.init().await?;
    let publisher = WorkflowPublisher::new(&broker, "matching-service").await?;
    let queues = Arc::new(QueueIndex::new());
    let activity_prefetch = Arc::new(ActivityPrefetchIndex::new());
    let bulk_prefetch = Arc::new(BulkPrefetchIndex::new());
    let workflow_poller_cache = Arc::new(WorkflowPollerCache::new());
    let workflow_notify = Arc::new(Notify::new());
    let bulk_notify = Arc::new(Notify::new());
    let outbox_notify = Arc::new(Notify::new());
    rebuild_pending_queues(&store, &queues, runtime.max_rebuild_tasks).await?;
    let debug = Arc::new(StdMutex::new(MatchingDebugState::new(&runtime)));
    let (result_submitter, result_receiver) = unbounded_channel();

    let state = AppState {
        store: store.clone(),
        publisher: publisher.clone(),
        queues: queues.clone(),
        activity_prefetch,
        bulk_prefetch,
        workflow_poller_cache,
        workflow_notify: workflow_notify.clone(),
        bulk_notify: bulk_notify.clone(),
        outbox_notify: outbox_notify.clone(),
        runtime: runtime.clone(),
        debug: debug.clone(),
        result_submitter,
        outbox_publisher_id: format!("matching-service:{}", Uuid::now_v7()),
    };
    let consumer =
        build_workflow_consumer(&broker, "matching-service", &broker.all_partition_ids()).await?;
    tokio::spawn(run_event_loop(state.clone(), consumer));
    tokio::spawn(run_timeout_loop(state.clone()));
    tokio::spawn(run_activity_result_batcher(state.clone(), result_receiver));
    tokio::spawn(run_workflow_event_outbox_publisher(state.clone()));

    let debug_app = default_router::<AppState>(ServiceInfo::new(
        debug_config.name,
        "matching-debug",
        env!("CARGO_PKG_VERSION"),
    ))
    .route("/debug/activity-results", get(get_activity_results_debug))
    .with_state(state.clone());
    tokio::spawn(async move {
        if let Err(err) = serve(debug_app, debug_config.port).await {
            error!(error = %err, "matching debug server exited");
        }
    });

    let addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, config.port));
    info!(%addr, "matching-service listening");
    Server::builder()
        .add_service(ActivityWorkerApiServer::new(ActivityApi { state: state.clone() }))
        .add_service(WorkflowWorkerApiServer::new(ActivityApi { state: state.clone() }))
        .serve(addr)
        .await?;
    Ok(())
}

async fn get_activity_results_debug(
    AxumState(state): AxumState<AppState>,
) -> Json<MatchingDebugState> {
    Json(state.debug.lock().expect("matching debug lock poisoned").clone())
}

async fn rebuild_pending_queues(
    store: &WorkflowStore,
    queues: &Arc<QueueIndex>,
    limit: i64,
) -> Result<()> {
    for record in store.list_runnable_activities(limit).await? {
        queues.enqueue(&queue_key(&record.tenant_id, &record.task_queue), record).await;
    }
    Ok(())
}

async fn run_event_loop(state: AppState, consumer: fabrik_broker::WorkflowConsumerStream) {
    let mut stream = consumer;
    while let Some(message) = stream.next().await {
        match message {
            Ok(record) => match decode_consumed_workflow_event(&record) {
                Ok(event) => {
                    if let Err(error) = process_event(&state, record.partition_id, event).await {
                        error!(error = %error, "matching-service failed to process event");
                    }
                }
                Err(error) => warn!(error = %error, "matching-service skipping invalid event"),
            },
            Err(error) => error!(error = %error, "matching-service consumer error"),
        }
    }
}

async fn process_event(
    state: &AppState,
    partition_id: i32,
    event: EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    match &event.payload {
        WorkflowEvent::ActivityTaskScheduled {
            activity_id,
            activity_type,
            task_queue,
            attempt,
            input,
            config,
            state: workflow_state,
            schedule_to_start_timeout_ms,
            start_to_close_timeout_ms,
            heartbeat_timeout_ms,
        } => {
            state
                .store
                .upsert_activity_scheduled(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    &event.definition_id,
                    Some(event.definition_version),
                    Some(&event.artifact_hash),
                    activity_id,
                    *attempt,
                    activity_type,
                    task_queue,
                    workflow_state.as_deref(),
                    input,
                    config.as_ref(),
                    *schedule_to_start_timeout_ms,
                    *start_to_close_timeout_ms,
                    *heartbeat_timeout_ms,
                    event.event_id,
                    &event.event_type,
                    event.occurred_at,
                )
                .await?;
            state
                .queues
                .enqueue(
                    &queue_key(&event.tenant_id, task_queue),
                    scheduled_activity_record(&event),
                )
                .await;
        }
        WorkflowEvent::BulkActivityBatchScheduled {
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
            execution_policy,
            reducer,
            throughput_backend,
            throughput_backend_version,
            state: workflow_state,
        } => {
            if throughput_backend != ThroughputBackend::PgV1.as_str() {
                return Ok(());
            }
            let reducer_class = bulk_reducer_class(reducer.as_deref());
            let fast_lane_enabled = stream_v2_fast_lane_enabled(
                throughput_backend,
                execution_policy.as_deref(),
                reducer.as_deref(),
            );
            let aggregation_tree_depth = planned_reduction_tree_depth(
                if *chunk_size == 0 {
                    0
                } else {
                    items.len().div_ceil(*chunk_size as usize) as u32
                },
                *aggregation_group_count,
                reducer.as_deref(),
            );
            state
                .store
                .upsert_bulk_batch(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    &event.definition_id,
                    Some(event.definition_version),
                    Some(&event.artifact_hash),
                    batch_id,
                    activity_type,
                    task_queue,
                    workflow_state.as_deref(),
                    input_handle,
                    result_handle,
                    items,
                    *chunk_size,
                    *max_attempts,
                    *retry_delay_ms,
                    execution_policy.as_deref(),
                    reducer.as_deref(),
                    reducer_class.as_str(),
                    aggregation_tree_depth,
                    fast_lane_enabled,
                    *aggregation_group_count,
                    throughput_backend,
                    throughput_backend_version,
                    event.occurred_at,
                )
                .await?;
            state.bulk_notify.notify_waiters();
        }
        WorkflowEvent::ActivityTaskCancellationRequested {
            activity_id,
            attempt,
            reason,
            metadata,
        } => {
            if state
                .store
                .request_activity_cancellation(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    activity_id,
                    *attempt,
                    reason,
                    metadata.as_ref(),
                    event.event_id,
                    &event.event_type,
                    event.occurred_at,
                )
                .await?
            {
                if let Some(record) = state
                    .store
                    .get_activity_attempt(
                        &event.tenant_id,
                        &event.instance_id,
                        &event.run_id,
                        activity_id,
                        *attempt,
                    )
                    .await?
                {
                    if record.status == WorkflowActivityStatus::Scheduled {
                        publish_cancelled_event(
                            state,
                            &record,
                            "matching-service",
                            "system",
                            reason,
                            metadata.as_ref(),
                        )
                        .await?;
                    }
                }
            }
        }
        WorkflowEvent::WorkflowCancellationRequested { reason } => {
            cancel_bulk_batches_for_backend(
                state,
                &event.tenant_id,
                &event.instance_id,
                &event.run_id,
                ThroughputBackend::PgV1.as_str(),
                reason,
                None,
            )
            .await?;
        }
        _ => {}
    }

    if let Some(kind) = mailbox_kind(&event.payload) {
        if should_use_local_stream_v2_trigger_fast_path(state, &event).await? {
            return Ok(());
        }
        let task_queue = resolve_workflow_task_queue(state, &event).await?;
        let preferred_build_id = state
            .store
            .get_run_record(&event.tenant_id, &event.instance_id, &event.run_id)
            .await?
            .and_then(|run| run.sticky_workflow_build_id);
        let inserted = state
            .store
            .enqueue_workflow_mailbox_message(
                partition_id,
                &task_queue,
                preferred_build_id.as_deref(),
                &event,
                kind,
                mailbox_message_id(&event.payload),
                mailbox_message_name(&event.payload),
                mailbox_payload(&event.payload),
            )
            .await?;
        if inserted.is_some() {
            state.workflow_notify.notify_waiters();
        }
    }

    if let Some((kind, ref_id, status_label)) = resume_details(&event.payload) {
        if should_use_local_stream_v2_bulk_terminal_fast_path(&event) {
            return Ok(());
        }
        let task_queue = resolve_workflow_task_queue(state, &event).await?;
        let preferred_build_id = state
            .store
            .get_run_record(&event.tenant_id, &event.instance_id, &event.run_id)
            .await?
            .and_then(|run| run.sticky_workflow_build_id);
        let inserted = state
            .store
            .enqueue_workflow_resume(
                partition_id,
                &task_queue,
                preferred_build_id.as_deref(),
                &event,
                kind,
                &ref_id,
                status_label,
            )
            .await?;
        if inserted.is_some() {
            state.workflow_notify.notify_waiters();
        }
    }

    Ok(())
}

async fn run_timeout_loop(state: AppState) {
    loop {
        if let Err(error) = sweep_activities(&state).await {
            error!(error = %error, "matching-service activity sweep failed");
        }
        tokio::time::sleep(Duration::from_millis(state.runtime.sweep_interval_ms)).await;
    }
}

async fn sweep_activities(state: &AppState) -> Result<()> {
    let now = Utc::now();
    for record in state.store.list_runnable_activities(state.runtime.max_rebuild_tasks).await? {
        if let Some(timeout_ms) = record.schedule_to_start_timeout_ms {
            let deadline = record.scheduled_at + chrono::Duration::milliseconds(timeout_ms as i64);
            if deadline <= now {
                publish_timed_out_event(state, &record).await?;
            }
        }
    }
    for record in state.store.list_started_activities(state.runtime.max_rebuild_tasks).await? {
        if let Some(started_at) = record.started_at {
            if let Some(timeout_ms) = record.start_to_close_timeout_ms {
                let deadline = started_at + chrono::Duration::milliseconds(timeout_ms as i64);
                if deadline <= now {
                    publish_timed_out_event(state, &record).await?;
                    continue;
                }
            }
        }
        let heartbeat_base = record.last_heartbeat_at.or(record.started_at);
        if let (Some(base), Some(timeout_ms)) = (heartbeat_base, record.heartbeat_timeout_ms) {
            let deadline = base + chrono::Duration::milliseconds(timeout_ms as i64);
            if deadline <= now {
                publish_timed_out_event(state, &record).await?;
                continue;
            }
        }
        if record.lease_expires_at.is_some_and(|expires_at| expires_at <= now) {
            if state
                .store
                .requeue_activity(
                    &record.tenant_id,
                    &record.instance_id,
                    &record.run_id,
                    &record.activity_id,
                    record.attempt,
                    now,
                )
                .await?
            {
                state
                    .queues
                    .enqueue(&queue_key(&record.tenant_id, &record.task_queue), record)
                    .await;
            }
        }
    }

    for record in state
        .store
        .list_started_bulk_chunks_for_backend(
            state.runtime.max_rebuild_tasks as usize,
            ThroughputBackend::PgV1.as_str(),
        )
        .await?
    {
        if record.lease_expires_at.is_some_and(|expires_at| expires_at <= now) {
            if state
                .store
                .requeue_bulk_chunk(
                    &record.tenant_id,
                    &record.instance_id,
                    &record.run_id,
                    &record.batch_id,
                    &record.chunk_id,
                    now,
                )
                .await?
            {
                state.bulk_notify.notify_waiters();
            }
        }
    }

    Ok(())
}

#[tonic::async_trait]
impl ActivityWorkerApi for ActivityApi {
    async fn poll_activity_task(
        &self,
        request: Request<PollActivityTaskRequest>,
    ) -> Result<Response<PollActivityTaskResponse>, Status> {
        let request = request.into_inner();
        let tasks = poll_activity_tasks_for_worker(
            &self.state,
            &request.tenant_id,
            &request.task_queue,
            &request.worker_id,
            &request.worker_build_id,
            request.poll_timeout_ms,
            1,
        )
        .await?;
        Ok(Response::new(PollActivityTaskResponse { task: tasks.first().map(record_to_proto) }))
    }

    async fn poll_activity_tasks(
        &self,
        request: Request<PollActivityTasksRequest>,
    ) -> Result<Response<PollActivityTasksResponse>, Status> {
        let request = request.into_inner();
        let max_tasks =
            usize::try_from(request.max_tasks.max(1)).unwrap_or(1).min(ACTIVITY_POLL_PREFETCH_SIZE);
        let tasks = poll_activity_tasks_for_worker(
            &self.state,
            &request.tenant_id,
            &request.task_queue,
            &request.worker_id,
            &request.worker_build_id,
            request.poll_timeout_ms,
            max_tasks,
        )
        .await?;
        Ok(Response::new(PollActivityTasksResponse {
            tasks: tasks.iter().map(record_to_proto).collect(),
        }))
    }

    async fn poll_bulk_activity_task(
        &self,
        request: Request<PollBulkActivityTaskRequest>,
    ) -> Result<Response<PollBulkActivityTaskResponse>, Status> {
        let request = request.into_inner();
        if request.tenant_id.trim().is_empty() {
            return Err(Status::invalid_argument("tenant_id is required"));
        }
        if request.task_queue.trim().is_empty() {
            return Err(Status::invalid_argument("task_queue is required"));
        }
        if request.worker_id.trim().is_empty() {
            return Err(Status::invalid_argument("worker_id is required"));
        }
        if request.worker_build_id.trim().is_empty() {
            return Err(Status::invalid_argument("worker_build_id is required"));
        }
        let timeout_ms =
            if request.poll_timeout_ms == 0 { 30_000 } else { request.poll_timeout_ms };
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
        if !self
            .state
            .store
            .is_build_compatible_with_queue(
                &request.tenant_id,
                TaskQueueKind::Activity,
                &request.task_queue,
                &request.worker_build_id,
            )
            .await
            .map_err(internal_status)?
        {
            return Err(Status::permission_denied(
                "worker build is not compatible with task queue",
            ));
        }
        self.state
            .store
            .upsert_queue_poller(
                &request.tenant_id,
                TaskQueueKind::Activity,
                &request.task_queue,
                &request.worker_id,
                &request.worker_build_id,
                None,
                None,
                chrono::Duration::seconds(self.state.runtime.lease_ttl_seconds as i64),
            )
            .await
            .map_err(internal_status)?;

        let worker_key = activity_worker_key(
            &request.tenant_id,
            &request.task_queue,
            &request.worker_id,
            &request.worker_build_id,
        );

        if let Some(record) = self.state.bulk_prefetch.pop(&worker_key).await {
            return Ok(Response::new(PollBulkActivityTaskResponse {
                tasks: vec![bulk_chunk_to_proto(&record, request.supports_cbor)],
            }));
        }

        loop {
            let leased = self
                .state
                .store
                .lease_next_bulk_chunks(
                    &request.tenant_id,
                    &request.task_queue,
                    &request.worker_id,
                    &request.worker_build_id,
                    chrono::Duration::seconds(self.state.runtime.lease_ttl_seconds as i64),
                    ACTIVITY_POLL_PREFETCH_SIZE,
                )
                .await
                .map_err(internal_status)?;
            if !leased.is_empty() {
                let (first, remaining) =
                    leased.split_first().expect("leased bulk batch is non-empty");
                if !remaining.is_empty() {
                    self.state
                        .bulk_prefetch
                        .push_remaining(&worker_key, remaining.iter().cloned())
                        .await;
                }
                let mut tasks = Vec::with_capacity(leased.len());
                tasks.push(bulk_chunk_to_proto(first, request.supports_cbor));
                tasks.extend(
                    remaining
                        .iter()
                        .map(|record| bulk_chunk_to_proto(record, request.supports_cbor)),
                );
                return Ok(Response::new(PollBulkActivityTaskResponse { tasks }));
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(Response::new(PollBulkActivityTaskResponse { tasks: Vec::new() }));
            }
            let remaining = deadline.saturating_duration_since(now);
            if tokio::time::timeout(remaining, self.state.bulk_notify.notified()).await.is_err() {
                return Ok(Response::new(PollBulkActivityTaskResponse { tasks: Vec::new() }));
            }
        }
    }

    async fn complete_activity_task(
        &self,
        request: Request<CompleteActivityTaskRequest>,
    ) -> Result<Response<Ack>, Status> {
        let request = request.into_inner();
        report_activity_results(
            &self.state,
            vec![ActivityTaskResult {
                tenant_id: request.tenant_id,
                instance_id: request.instance_id,
                run_id: request.run_id,
                activity_id: request.activity_id,
                attempt: request.attempt,
                worker_id: request.worker_id,
                worker_build_id: request.worker_build_id,
                lease_epoch: request.lease_epoch,
                owner_epoch: request.owner_epoch,
                result: Some(activity_task_result::Result::Completed(
                    fabrik_worker_protocol::activity_worker::ActivityTaskCompletedResult {
                        output_json: request.output_json,
                        output_cbor: Vec::new(),
                    },
                )),
            }],
        )
        .await?;
        Ok(Response::new(Ack {}))
    }

    async fn fail_activity_task(
        &self,
        request: Request<FailActivityTaskRequest>,
    ) -> Result<Response<Ack>, Status> {
        let request = request.into_inner();
        report_activity_results(
            &self.state,
            vec![ActivityTaskResult {
                tenant_id: request.tenant_id,
                instance_id: request.instance_id,
                run_id: request.run_id,
                activity_id: request.activity_id,
                attempt: request.attempt,
                worker_id: request.worker_id,
                worker_build_id: request.worker_build_id,
                lease_epoch: request.lease_epoch,
                owner_epoch: request.owner_epoch,
                result: Some(activity_task_result::Result::Failed(
                    fabrik_worker_protocol::activity_worker::ActivityTaskFailedResult {
                        error: request.error,
                    },
                )),
            }],
        )
        .await?;
        Ok(Response::new(Ack {}))
    }

    async fn record_activity_heartbeat(
        &self,
        request: Request<RecordActivityHeartbeatRequest>,
    ) -> Result<Response<RecordActivityHeartbeatResponse>, Status> {
        let request = request.into_inner();
        let details = if request.details_json.trim().is_empty() {
            None
        } else {
            Some(
                serde_json::from_str::<Value>(&request.details_json)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?,
            )
        };
        let event_id = Uuid::now_v7();
        let occurred_at = Utc::now();
        let lease_expires_at =
            occurred_at + chrono::Duration::seconds(self.state.runtime.lease_ttl_seconds as i64);
        let previous_record = self
            .state
            .store
            .get_activity_attempt(
                &request.tenant_id,
                &request.instance_id,
                &request.run_id,
                &request.activity_id,
                request.attempt,
            )
            .await
            .map_err(internal_status)?;
        let result = self
            .state
            .store
            .record_activity_heartbeat(
                &request.tenant_id,
                &request.instance_id,
                &request.run_id,
                &request.activity_id,
                request.attempt,
                &request.worker_id,
                &request.worker_build_id,
                lease_expires_at,
                event_id,
                "ActivityTaskHeartbeatRecorded",
                occurred_at,
            )
            .await
            .map_err(internal_status)?;
        let publish_heartbeat = result.updated
            && previous_record
                .as_ref()
                .and_then(|record| record.last_heartbeat_at.or(record.started_at))
                .map(|last_progress| {
                    occurred_at.signed_duration_since(last_progress).num_milliseconds()
                        >= self.state.runtime.heartbeat_publish_interval_ms as i64
                })
                .unwrap_or(true);
        if publish_heartbeat {
            let record = self
                .state
                .store
                .get_activity_attempt(
                    &request.tenant_id,
                    &request.instance_id,
                    &request.run_id,
                    &request.activity_id,
                    request.attempt,
                )
                .await
                .map_err(internal_status)?
                .ok_or_else(|| Status::not_found("activity attempt not found after heartbeat"))?;
            publish_activity_event(
                &self.state.publisher,
                &record,
                WorkflowEvent::ActivityTaskHeartbeatRecorded {
                    activity_id: request.activity_id,
                    attempt: request.attempt,
                    worker_id: request.worker_id,
                    worker_build_id: request.worker_build_id,
                    details,
                },
                event_id,
                occurred_at,
            )
            .await
            .map_err(internal_status)?;
        }
        Ok(Response::new(RecordActivityHeartbeatResponse {
            cancellation_requested: result.cancellation_requested,
        }))
    }

    async fn report_activity_task_cancelled(
        &self,
        request: Request<ReportActivityTaskCancelledRequest>,
    ) -> Result<Response<Ack>, Status> {
        let request = request.into_inner();
        report_activity_results(
            &self.state,
            vec![ActivityTaskResult {
                tenant_id: request.tenant_id,
                instance_id: request.instance_id,
                run_id: request.run_id,
                activity_id: request.activity_id,
                attempt: request.attempt,
                worker_id: request.worker_id,
                worker_build_id: request.worker_build_id,
                lease_epoch: request.lease_epoch,
                owner_epoch: request.owner_epoch,
                result: Some(activity_task_result::Result::Cancelled(
                    fabrik_worker_protocol::activity_worker::ActivityTaskCancelledResult {
                        reason: request.reason,
                        metadata_json: request.metadata_json,
                        metadata_cbor: Vec::new(),
                    },
                )),
            }],
        )
        .await?;
        Ok(Response::new(Ack {}))
    }

    async fn report_activity_task_results(
        &self,
        request: Request<ReportActivityTaskResultsRequest>,
    ) -> Result<Response<Ack>, Status> {
        report_activity_results(&self.state, request.into_inner().results).await?;
        Ok(Response::new(Ack {}))
    }

    async fn report_bulk_activity_task_results(
        &self,
        request: Request<ReportBulkActivityTaskResultsRequest>,
    ) -> Result<Response<Ack>, Status> {
        report_bulk_activity_results(&self.state, request.into_inner().results).await?;
        Ok(Response::new(Ack {}))
    }
}

async fn report_activity_results(
    state: &AppState,
    results: Vec<ActivityTaskResult>,
) -> Result<(), Status> {
    if results.is_empty() {
        return Ok(());
    }

    let updates =
        results.into_iter().map(prepare_activity_result).collect::<Result<Vec<_>, _>>()?;
    {
        let mut debug = state.debug.lock().expect("matching debug lock poisoned");
        let received_bytes: usize = updates.iter().map(|update| update.estimated_bytes).sum();
        debug.record_received(1, updates.len(), received_bytes);
    }

    let (response_tx, response_rx) = oneshot::channel();
    state
        .result_submitter
        .send(PendingActivityResultRequest { updates, response_tx })
        .map_err(|_| Status::internal("activity result batcher is unavailable"))?;

    response_rx
        .await
        .map_err(|_| Status::internal("activity result batcher dropped the response"))?
}

async fn report_bulk_activity_results(
    state: &AppState,
    results: Vec<BulkActivityTaskResult>,
) -> Result<(), Status> {
    for result in results {
        ensure_bulk_batch_backend(
            &state.store,
            &result.tenant_id,
            &result.instance_id,
            &result.run_id,
            &result.batch_id,
            ThroughputBackend::PgV1.as_str(),
        )
        .await?;
        let update = prepare_bulk_result(result)?;
        let applied =
            state.store.apply_bulk_chunk_terminal_update(&update).await.map_err(internal_status)?;
        if let Some(event) = applied.terminal_event {
            let _ = event;
            state.outbox_notify.notify_waiters();
        } else if applied.chunk.status == fabrik_store::WorkflowBulkChunkStatus::Scheduled {
            let now = Utc::now();
            let delay = if applied.chunk.available_at > now {
                (applied.chunk.available_at - now)
                    .to_std()
                    .unwrap_or_else(|_| Duration::from_millis(0))
            } else {
                Duration::from_millis(0)
            };
            let notify = state.bulk_notify.clone();
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                notify.notify_waiters();
            });
        }
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
        .get_bulk_batch(tenant_id, instance_id, run_id, batch_id)
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

async fn run_workflow_event_outbox_publisher(state: AppState) {
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
            Ok(leased) => leased,
            Err(error) => {
                error!(error = %error, "matching-service failed to lease workflow event outbox");
                tokio::time::sleep(Duration::from_millis(BULK_EVENT_OUTBOX_RETRY_MS as u64)).await;
                continue;
            }
        };

        if leased.is_empty() {
            if tokio::time::timeout(
                Duration::from_millis(BULK_EVENT_OUTBOX_RETRY_MS as u64),
                state.outbox_notify.notified(),
            )
            .await
            .is_err()
            {
                continue;
            }
            continue;
        }

        for record in leased {
            if let Err(error) = state.publisher.publish(&record.event, &record.partition_key).await
            {
                error!(
                    error = %error,
                    outbox_id = %record.outbox_id,
                    event_type = %record.event_type,
                    "matching-service failed to publish workflow event outbox row"
                );
                let retry_at =
                    Utc::now() + chrono::Duration::milliseconds(BULK_EVENT_OUTBOX_RETRY_MS);
                if let Err(release_error) = state
                    .store
                    .release_workflow_event_outbox_lease(
                        record.outbox_id,
                        &state.outbox_publisher_id,
                        retry_at,
                    )
                    .await
                {
                    error!(
                        error = %release_error,
                        outbox_id = %record.outbox_id,
                        "matching-service failed to release workflow event outbox lease"
                    );
                }
                continue;
            }

            if let Err(error) = state
                .store
                .mark_workflow_event_outbox_published(
                    record.outbox_id,
                    &state.outbox_publisher_id,
                    Utc::now(),
                )
                .await
            {
                error!(
                    error = %error,
                    outbox_id = %record.outbox_id,
                    "matching-service failed to mark workflow event outbox row published"
                );
            }
        }
    }
}

async fn run_activity_result_batcher(
    state: AppState,
    mut receiver: UnboundedReceiver<PendingActivityResultRequest>,
) {
    while let Some(first_request) = receiver.recv().await {
        let mut requests = vec![first_request];
        let flush_interval =
            Duration::from_millis(state.runtime.activity_result_apply_flush_interval_ms.max(1));
        let deadline = tokio::time::Instant::now() + flush_interval;

        loop {
            if batch_request_totals(&requests).0
                >= state.runtime.activity_result_apply_batch_max_items
            {
                break;
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                break;
            }

            match tokio::time::timeout(deadline.saturating_duration_since(now), receiver.recv())
                .await
            {
                Ok(Some(request)) => requests.push(request),
                Ok(None) | Err(_) => break,
            }
        }

        if let Err(error) = apply_batched_activity_results(&state, requests).await {
            error!(error = %error, "matching-service activity result batch failed");
        }
    }
}

fn batch_request_totals(requests: &[PendingActivityResultRequest]) -> (usize, usize) {
    requests.iter().fold((0, 0), |(items, bytes), request| {
        let request_bytes: usize =
            request.updates.iter().map(|update| update.estimated_bytes).sum();
        (items + request.updates.len(), bytes + request_bytes)
    })
}

async fn apply_batched_activity_results(
    state: &AppState,
    requests: Vec<PendingActivityResultRequest>,
) -> Result<()> {
    if requests.is_empty() {
        return Ok(());
    }

    let mut request_progress = Vec::with_capacity(requests.len());
    let mut queued_results = Vec::new();
    for (request_index, request) in requests.into_iter().enumerate() {
        let remaining_updates = request.updates.len();
        request_progress.push(PendingRequestProgress {
            response_tx: Some(request.response_tx),
            remaining_updates,
        });
        queued_results.extend(
            request
                .updates
                .into_iter()
                .map(|prepared| QueuedActivityResult { request_index, prepared }),
        );
    }

    if queued_results.is_empty() {
        for progress in &mut request_progress {
            complete_request(progress, Ok(()));
        }
        return Ok(());
    }

    let batches = split_activity_result_batches(
        queued_results,
        state.runtime.activity_result_apply_batch_max_items,
        state.runtime.activity_result_apply_batch_max_bytes,
        state.runtime.result_apply_per_run_coalescing_cap,
    );

    for batch in batches {
        let batch_size = batch.len();
        let batch_bytes: usize = batch.iter().map(|item| item.prepared.estimated_bytes).sum();
        let apply_started_at = std::time::Instant::now();
        let updates = batch.iter().map(|item| item.prepared.update.clone()).collect::<Vec<_>>();
        let applied = match state.store.apply_activity_terminal_batch(&updates).await {
            Ok(applied) => applied,
            Err(error) => {
                let status = internal_status(error);
                fail_open_requests(&mut request_progress, status);
                return Ok(());
            }
        };

        let applied_len = applied.len();
        let published_events = match publish_applied_activity_events(state, &applied).await {
            Ok(count) => count,
            Err(error) => {
                fail_open_requests(&mut request_progress, internal_status(error));
                return Ok(());
            }
        };

        let apply_latency_ms = apply_started_at.elapsed().as_secs_f64() * 1000.0;
        {
            let mut debug = state.debug.lock().expect("matching debug lock poisoned");
            debug.record_batch(
                batch_size,
                batch_bytes,
                applied_len,
                batch_size.saturating_sub(applied_len),
                published_events,
                &applied,
                apply_latency_ms,
            );
        }

        for item in batch {
            let progress = &mut request_progress[item.request_index];
            progress.remaining_updates = progress.remaining_updates.saturating_sub(1);
            if progress.remaining_updates == 0 {
                complete_request(progress, Ok(()));
            }
        }
    }

    Ok(())
}

async fn publish_applied_activity_events(
    state: &AppState,
    applied: &[AppliedActivityTerminalUpdate],
) -> Result<u64> {
    if applied.is_empty() {
        return Ok(0);
    }

    let envelopes = build_applied_activity_event_envelopes(applied);
    state.publisher.publish_all(&envelopes).await?;
    Ok(envelopes.len() as u64)
}

fn split_activity_result_batches(
    items: Vec<QueuedActivityResult>,
    max_items: usize,
    max_bytes: usize,
    per_run_cap: usize,
) -> Vec<Vec<QueuedActivityResult>> {
    let max_items = max_items.max(1);
    let max_bytes = max_bytes.max(1);
    let per_run_cap = per_run_cap.max(1);
    let mut batches = Vec::new();
    let mut current_batch = Vec::new();
    let mut current_bytes = 0usize;
    let mut per_run_counts: HashMap<ActivityRunKey, usize> = HashMap::new();

    for item in items {
        let next_bytes = current_bytes + item.prepared.estimated_bytes.max(1);
        let run_count = per_run_counts.get(&item.prepared.run_key).copied().unwrap_or_default();
        let would_overflow = !current_batch.is_empty()
            && (current_batch.len() >= max_items
                || next_bytes > max_bytes
                || run_count >= per_run_cap);
        if would_overflow {
            batches.push(current_batch);
            current_batch = Vec::new();
            current_bytes = 0;
            per_run_counts.clear();
        }

        current_bytes += item.prepared.estimated_bytes.max(1);
        *per_run_counts.entry(item.prepared.run_key.clone()).or_default() += 1;
        current_batch.push(item);
    }

    if !current_batch.is_empty() {
        batches.push(current_batch);
    }

    batches
}

fn prepare_activity_result(
    result: ActivityTaskResult,
) -> Result<PreparedActivityTerminalUpdate, Status> {
    let Some(outcome) = result.result else {
        return Err(Status::invalid_argument("activity result payload is required"));
    };

    let payload = match outcome {
        activity_task_result::Result::Completed(completed) => {
            let output = serde_json::from_str(&completed.output_json)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            ActivityTerminalPayload::Completed { output }
        }
        activity_task_result::Result::Failed(failed) => {
            ActivityTerminalPayload::Failed { error: failed.error }
        }
        activity_task_result::Result::Cancelled(cancelled) => {
            let metadata = if cancelled.metadata_json.trim().is_empty() {
                None
            } else {
                Some(
                    serde_json::from_str::<Value>(&cancelled.metadata_json)
                        .map_err(|error| Status::invalid_argument(error.to_string()))?,
                )
            };
            ActivityTerminalPayload::Cancelled { reason: cancelled.reason, metadata }
        }
    };

    let occurred_at = Utc::now();
    let update = ActivityTerminalUpdate {
        tenant_id: result.tenant_id,
        instance_id: result.instance_id,
        run_id: result.run_id,
        activity_id: result.activity_id,
        attempt: result.attempt,
        worker_id: result.worker_id,
        worker_build_id: result.worker_build_id,
        event_id: Uuid::now_v7(),
        event_type: match &payload {
            ActivityTerminalPayload::Completed { .. } => "ActivityTaskCompleted".to_owned(),
            ActivityTerminalPayload::Failed { .. } => "ActivityTaskFailed".to_owned(),
            ActivityTerminalPayload::Cancelled { .. } => "ActivityTaskCancelled".to_owned(),
        },
        occurred_at,
        payload,
    };
    let estimated_bytes = estimate_result_size(&update);
    Ok(PreparedActivityTerminalUpdate {
        run_key: ActivityRunKey {
            tenant_id: update.tenant_id.clone(),
            instance_id: update.instance_id.clone(),
            run_id: update.run_id.clone(),
        },
        estimated_bytes,
        update,
    })
}

fn prepare_bulk_result(result: BulkActivityTaskResult) -> Result<BulkChunkTerminalUpdate, Status> {
    let Some(outcome) = result.result else {
        return Err(Status::invalid_argument("bulk activity result payload is required"));
    };
    let lease_token = Uuid::parse_str(&result.lease_token)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let payload = match outcome {
        fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Completed(
            completed,
        ) => BulkChunkTerminalPayload::Completed {
            output: if !completed.output_cbor.is_empty() {
                match decode_cbor::<Value>(&completed.output_cbor, "bulk result output")
                    .map_err(|error| Status::invalid_argument(error.to_string()))?
                {
                    Value::Array(items) => items,
                    other => {
                        return Err(Status::invalid_argument(format!(
                            "bulk result output CBOR must decode to an array, got {other}"
                        )));
                    }
                }
            } else {
                serde_json::from_str(&completed.output_json)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?
            },
        },
        fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Failed(
            failed,
        ) => BulkChunkTerminalPayload::Failed { error: failed.error },
        fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Cancelled(
            cancelled,
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
            BulkChunkTerminalPayload::Cancelled { reason: cancelled.reason, metadata }
        }
    };
    Ok(BulkChunkTerminalUpdate {
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
        report_id: result.report_id,
        lease_token,
        worker_id: result.worker_id,
        worker_build_id: result.worker_build_id,
        occurred_at: Utc::now(),
        payload,
    })
}

fn estimate_result_size(update: &ActivityTerminalUpdate) -> usize {
    let payload_bytes = match &update.payload {
        ActivityTerminalPayload::Completed { output } => {
            serde_json::to_vec(output).map(|bytes| bytes.len()).unwrap_or_default()
        }
        ActivityTerminalPayload::Failed { error } => error.len(),
        ActivityTerminalPayload::Cancelled { reason, metadata } => {
            reason.len()
                + metadata
                    .as_ref()
                    .and_then(|value| serde_json::to_vec(value).ok())
                    .map(|bytes| bytes.len())
                    .unwrap_or_default()
        }
    };
    update.tenant_id.len()
        + update.instance_id.len()
        + update.run_id.len()
        + update.activity_id.len()
        + update.worker_id.len()
        + update.worker_build_id.len()
        + update.event_type.len()
        + payload_bytes
        + std::mem::size_of::<u32>()
        + 64
}

fn build_applied_activity_event_envelopes(
    applied: &[AppliedActivityTerminalUpdate],
) -> Vec<EventEnvelope<WorkflowEvent>> {
    if applied.is_empty() {
        return Vec::new();
    }

    applied
        .iter()
        .map(|update| {
            let worker_id = update.record.worker_id.clone().unwrap_or_default();
            let worker_build_id = update.record.worker_build_id.clone().unwrap_or_default();
            let payload = match &update.payload {
                ActivityTerminalPayload::Completed { output } => {
                    WorkflowEvent::ActivityTaskCompleted {
                        activity_id: update.record.activity_id.clone(),
                        attempt: update.record.attempt,
                        output: output.clone(),
                        worker_id,
                        worker_build_id,
                    }
                }
                ActivityTerminalPayload::Failed { error } => WorkflowEvent::ActivityTaskFailed {
                    activity_id: update.record.activity_id.clone(),
                    attempt: update.record.attempt,
                    error: error.clone(),
                    worker_id,
                    worker_build_id,
                },
                ActivityTerminalPayload::Cancelled { reason, metadata } => {
                    WorkflowEvent::ActivityTaskCancelled {
                        activity_id: update.record.activity_id.clone(),
                        attempt: update.record.attempt,
                        reason: reason.clone(),
                        worker_id,
                        worker_build_id,
                        metadata: metadata.clone(),
                    }
                }
            };
            build_activity_event_envelope(
                &update.record,
                payload,
                update.event_id,
                update.occurred_at,
            )
        })
        .collect::<Vec<_>>()
}

fn complete_request(
    progress: &mut PendingRequestProgress,
    result: std::result::Result<(), Status>,
) {
    if let Some(response_tx) = progress.response_tx.take() {
        let _ = response_tx.send(result);
    }
}

fn fail_open_requests(progress: &mut [PendingRequestProgress], error: Status) {
    for request in progress.iter_mut().filter(|request| request.remaining_updates > 0) {
        complete_request(request, Err(error.clone()));
    }
}

#[tonic::async_trait]
impl WorkflowWorkerApi for ActivityApi {
    async fn poll_workflow_task(
        &self,
        request: Request<PollWorkflowTaskRequest>,
    ) -> Result<Response<PollWorkflowTaskResponse>, Status> {
        let request = request.into_inner();
        if request.worker_id.trim().is_empty() {
            return Err(Status::invalid_argument("worker_id is required"));
        }
        if request.worker_build_id.trim().is_empty() {
            return Err(Status::invalid_argument("worker_build_id is required"));
        }

        let timeout_ms =
            if request.poll_timeout_ms == 0 { 30_000 } else { request.poll_timeout_ms };
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
        maybe_heartbeat_workflow_poller_queues(
            &self.state,
            &request.worker_id,
            &request.worker_build_id,
            request.partition_id,
        )
        .await;

        loop {
            if let Some(record) = self
                .state
                .store
                .lease_next_workflow_task(
                    request.partition_id,
                    &request.worker_id,
                    &request.worker_build_id,
                    chrono::Duration::seconds(self.state.runtime.lease_ttl_seconds as i64),
                )
                .await
                .map_err(internal_status)?
            {
                self.state
                    .store
                    .upsert_queue_poller(
                        &record.tenant_id,
                        TaskQueueKind::Workflow,
                        &record.task_queue,
                        &request.worker_id,
                        &request.worker_build_id,
                        Some(request.partition_id),
                        None,
                        chrono::Duration::seconds(self.state.runtime.lease_ttl_seconds as i64),
                    )
                    .await
                    .map_err(internal_status)?;
                return Ok(Response::new(PollWorkflowTaskResponse {
                    task: Some(workflow_task_to_proto(&record)),
                }));
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(Response::new(PollWorkflowTaskResponse { task: None }));
            }
            let remaining = deadline.saturating_duration_since(now);
            if tokio::time::timeout(remaining, self.state.workflow_notify.notified()).await.is_err()
            {
                return Ok(Response::new(PollWorkflowTaskResponse { task: None }));
            }
        }
    }

    async fn complete_workflow_task(
        &self,
        request: Request<CompleteWorkflowTaskRequest>,
    ) -> Result<Response<Ack>, Status> {
        let request = request.into_inner();
        let task_id = Uuid::parse_str(&request.task_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.state
            .store
            .complete_workflow_task(
                task_id,
                &request.worker_id,
                &request.worker_build_id,
                Utc::now(),
            )
            .await
            .map_err(internal_status)?;
        self.state.workflow_notify.notify_waiters();
        Ok(Response::new(Ack {}))
    }

    async fn fail_workflow_task(
        &self,
        request: Request<FailWorkflowTaskRequest>,
    ) -> Result<Response<Ack>, Status> {
        let request = request.into_inner();
        let task_id = Uuid::parse_str(&request.task_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let failed = self
            .state
            .store
            .fail_workflow_task(
                task_id,
                &request.worker_id,
                &request.worker_build_id,
                &request.error,
                Utc::now(),
            )
            .await
            .map_err(internal_status)?;
        if failed {
            self.state.workflow_notify.notify_waiters();
        }
        Ok(Response::new(Ack {}))
    }
}

async fn lease_next_activities(
    state: &AppState,
    tenant_id: &str,
    task_queue: &str,
    worker_id: &str,
    worker_build_id: &str,
    max_tasks: usize,
) -> Result<Vec<WorkflowActivityRecord>> {
    let queue_key = queue_key(tenant_id, task_queue);
    let mut start_updates = Vec::new();
    let mut started_envelopes = Vec::new();
    let max_tasks = max_tasks.max(1).min(ACTIVITY_POLL_PREFETCH_SIZE);

    while start_updates.len() < max_tasks {
        let Some(record) = state.queues.pop(&queue_key).await else {
            break;
        };
        if record.status != WorkflowActivityStatus::Scheduled
            || record.tenant_id != tenant_id
            || record.task_queue != task_queue
        {
            continue;
        }
        if record.cancellation_requested {
            publish_cancelled_event(
                state,
                &record,
                "matching-service",
                "system",
                "cancelled before dispatch",
                None,
            )
            .await?;
            continue;
        }

        let occurred_at = Utc::now();
        start_updates.push(ActivityStartUpdate {
            tenant_id: record.tenant_id,
            instance_id: record.instance_id,
            run_id: record.run_id,
            activity_id: record.activity_id,
            attempt: record.attempt,
            worker_id: worker_id.to_owned(),
            worker_build_id: worker_build_id.to_owned(),
            lease_expires_at: occurred_at
                + chrono::Duration::seconds(state.runtime.lease_ttl_seconds as i64),
            event_id: Uuid::now_v7(),
            event_type: "ActivityTaskStarted".to_owned(),
            occurred_at,
        });
    }

    let applied = state.store.mark_activities_started_batch(&start_updates).await?;
    let mut leased_records = Vec::with_capacity(applied.len());
    for started in applied {
        started_envelopes.push(build_activity_event_envelope(
            &started.record,
            WorkflowEvent::ActivityTaskStarted {
                activity_id: started.record.activity_id.clone(),
                attempt: started.record.attempt,
                worker_id: worker_id.to_owned(),
                worker_build_id: worker_build_id.to_owned(),
            },
            started.event_id,
            started.occurred_at,
        ));
        leased_records.push(started.record);
    }

    if !started_envelopes.is_empty() {
        state.publisher.publish_all(&started_envelopes).await?;
    }

    Ok(leased_records)
}

async fn poll_activity_tasks_for_worker(
    state: &AppState,
    tenant_id: &str,
    task_queue: &str,
    worker_id: &str,
    worker_build_id: &str,
    poll_timeout_ms: u64,
    max_tasks: usize,
) -> Result<Vec<WorkflowActivityRecord>, Status> {
    if tenant_id.trim().is_empty() {
        return Err(Status::invalid_argument("tenant_id is required"));
    }
    if task_queue.trim().is_empty() {
        return Err(Status::invalid_argument("task_queue is required"));
    }
    if worker_id.trim().is_empty() {
        return Err(Status::invalid_argument("worker_id is required"));
    }
    if worker_build_id.trim().is_empty() {
        return Err(Status::invalid_argument("worker_build_id is required"));
    }

    let timeout_ms = if poll_timeout_ms == 0 { 30_000 } else { poll_timeout_ms };
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    let max_tasks = max_tasks.max(1).min(ACTIVITY_POLL_PREFETCH_SIZE);

    if !state
        .store
        .is_build_compatible_with_queue(
            tenant_id,
            TaskQueueKind::Activity,
            task_queue,
            worker_build_id,
        )
        .await
        .map_err(internal_status)?
    {
        return Err(Status::permission_denied("worker build is not compatible with task queue"));
    }
    state
        .store
        .upsert_queue_poller(
            tenant_id,
            TaskQueueKind::Activity,
            task_queue,
            worker_id,
            worker_build_id,
            None,
            None,
            chrono::Duration::seconds(state.runtime.lease_ttl_seconds as i64),
        )
        .await
        .map_err(internal_status)?;

    let worker_key = activity_worker_key(tenant_id, task_queue, worker_id, worker_build_id);

    loop {
        let mut tasks = Vec::new();
        while tasks.len() < max_tasks {
            let Some(record) = state.activity_prefetch.pop(&worker_key).await else {
                break;
            };
            tasks.push(record);
        }
        if !tasks.is_empty() {
            return Ok(tasks);
        }

        let leased = lease_next_activities(
            state,
            tenant_id,
            task_queue,
            worker_id,
            worker_build_id,
            max_tasks,
        )
        .await
        .map_err(internal_status)?;
        if !leased.is_empty() {
            return Ok(leased);
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Ok(Vec::new());
        }
        let remaining = deadline.saturating_duration_since(now);
        if tokio::time::timeout(remaining, state.queues.notify.notified()).await.is_err() {
            return Ok(Vec::new());
        }
    }
}

async fn publish_timed_out_event(state: &AppState, record: &WorkflowActivityRecord) -> Result<()> {
    let worker_id = record.worker_id.clone().unwrap_or_else(|| "matching-service".to_owned());
    let worker_build_id = record.worker_build_id.clone().unwrap_or_else(|| "system".to_owned());
    let event_id = Uuid::now_v7();
    let occurred_at = Utc::now();
    let updated = state
        .store
        .fail_activity(
            &record.tenant_id,
            &record.instance_id,
            &record.run_id,
            &record.activity_id,
            record.attempt,
            WorkflowActivityStatus::TimedOut,
            "activity timed out",
            None,
            &worker_id,
            &worker_build_id,
            event_id,
            "ActivityTaskTimedOut",
            occurred_at,
        )
        .await?;
    if updated {
        publish_activity_event(
            &state.publisher,
            record,
            WorkflowEvent::ActivityTaskTimedOut {
                activity_id: record.activity_id.clone(),
                attempt: record.attempt,
                worker_id,
                worker_build_id,
            },
            event_id,
            occurred_at,
        )
        .await?;
    }
    Ok(())
}

async fn publish_cancelled_event(
    state: &AppState,
    record: &WorkflowActivityRecord,
    worker_id: &str,
    worker_build_id: &str,
    reason: &str,
    metadata: Option<&Value>,
) -> Result<()> {
    let event_id = Uuid::now_v7();
    let occurred_at = Utc::now();
    let updated = state
        .store
        .fail_activity(
            &record.tenant_id,
            &record.instance_id,
            &record.run_id,
            &record.activity_id,
            record.attempt,
            WorkflowActivityStatus::Cancelled,
            reason,
            metadata,
            worker_id,
            worker_build_id,
            event_id,
            "ActivityTaskCancelled",
            occurred_at,
        )
        .await?;
    if updated {
        publish_activity_event(
            &state.publisher,
            record,
            WorkflowEvent::ActivityTaskCancelled {
                activity_id: record.activity_id.clone(),
                attempt: record.attempt,
                reason: reason.to_owned(),
                worker_id: worker_id.to_owned(),
                worker_build_id: worker_build_id.to_owned(),
                metadata: metadata.cloned(),
            },
            event_id,
            occurred_at,
        )
        .await?;
    }
    Ok(())
}

async fn publish_activity_event(
    publisher: &WorkflowPublisher,
    record: &WorkflowActivityRecord,
    payload: WorkflowEvent,
    event_id: Uuid,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    let envelope = build_activity_event_envelope(record, payload, event_id, occurred_at);
    publisher.publish(&envelope, &envelope.partition_key).await?;
    Ok(())
}

async fn cancel_bulk_batches_for_backend(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    throughput_backend: &str,
    reason: &str,
    metadata: Option<&Value>,
) -> Result<()> {
    let batches = state
        .store
        .list_nonterminal_bulk_batches_for_run(tenant_id, instance_id, run_id, throughput_backend)
        .await?;
    for batch in batches {
        let cancelled = state
            .store
            .cancel_bulk_batch(
                tenant_id,
                instance_id,
                run_id,
                &batch.batch_id,
                reason,
                metadata,
                Utc::now(),
            )
            .await?;
        if cancelled.terminal_event.is_some() {
            state.outbox_notify.notify_waiters();
        }
        state.bulk_notify.notify_waiters();
    }
    Ok(())
}

fn build_activity_event_envelope(
    record: &WorkflowActivityRecord,
    payload: WorkflowEvent,
    event_id: Uuid,
    occurred_at: DateTime<Utc>,
) -> EventEnvelope<WorkflowEvent> {
    let mut envelope = EventEnvelope::new(
        payload.event_type(),
        WorkflowIdentity::new(
            record.tenant_id.clone(),
            record.definition_id.clone(),
            record.definition_version.unwrap_or_default(),
            record.artifact_hash.clone().unwrap_or_default(),
            record.instance_id.clone(),
            record.run_id.clone(),
            "matching-service",
        ),
        payload,
    );
    envelope.event_id = event_id;
    envelope.occurred_at = occurred_at;
    envelope.metadata.insert(
        "state".to_owned(),
        record.state.clone().unwrap_or_else(|| record.activity_id.clone()),
    );
    envelope.metadata.insert("attempt".to_owned(), record.attempt.to_string());
    envelope
}

fn record_to_proto(record: &WorkflowActivityRecord) -> ActivityTask {
    ActivityTask {
        tenant_id: record.tenant_id.clone(),
        definition_id: record.definition_id.clone(),
        definition_version: record.definition_version.unwrap_or_default(),
        artifact_hash: record.artifact_hash.clone().unwrap_or_default(),
        instance_id: record.instance_id.clone(),
        run_id: record.run_id.clone(),
        activity_id: record.activity_id.clone(),
        activity_type: record.activity_type.clone(),
        task_queue: record.task_queue.clone(),
        attempt: record.attempt,
        input_json: serde_json::to_string(&record.input).expect("activity input serializes"),
        input_cbor: Vec::new(),
        config_json: record
            .config
            .as_ref()
            .map(|config| serde_json::to_string(config).expect("activity config serializes"))
            .unwrap_or_default(),
        config_cbor: Vec::new(),
        prefer_cbor: false,
        state: record.state.clone().unwrap_or_default(),
        scheduled_at_unix_ms: record.scheduled_at.timestamp_millis(),
        lease_expires_at_unix_ms: record
            .lease_expires_at
            .map(|value| value.timestamp_millis())
            .unwrap_or_default(),
        start_to_close_timeout_ms: record.start_to_close_timeout_ms.unwrap_or_default(),
        heartbeat_timeout_ms: record.heartbeat_timeout_ms.unwrap_or_default(),
        cancellation_requested: record.cancellation_requested,
        schedule_to_start_timeout_ms: record.schedule_to_start_timeout_ms.unwrap_or_default(),
        lease_epoch: 0,
        owner_epoch: 0,
        omit_success_output: false,
    }
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
        items_json: if supports_cbor {
            String::new()
        } else {
            serde_json::to_string(&record.items).expect("bulk chunk items serialize")
        },
        input_handle_json: if supports_cbor {
            String::new()
        } else {
            serde_json::to_string(&record.input_handle).expect("bulk chunk input handle serializes")
        },
        items_cbor: if supports_cbor {
            encode_cbor(&record.items, "bulk chunk items").expect("bulk chunk items encode as CBOR")
        } else {
            Vec::new()
        },
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

fn workflow_task_to_proto(record: &WorkflowTaskRecord) -> WorkflowTask {
    WorkflowTask {
        task_id: record.task_id.to_string(),
        tenant_id: record.tenant_id.clone(),
        definition_id: record.definition_id.clone(),
        definition_version: record.definition_version.unwrap_or_default(),
        artifact_hash: record.artifact_hash.clone().unwrap_or_default(),
        instance_id: record.instance_id.clone(),
        run_id: record.run_id.clone(),
        partition_id: record.partition_id,
        task_queue: record.task_queue.clone(),
        preferred_build_id: record.preferred_build_id.clone().unwrap_or_default(),
        mailbox_consumed_seq: record.mailbox_consumed_seq,
        resume_consumed_seq: record.resume_consumed_seq,
        mailbox_backlog: record.mailbox_backlog,
        resume_backlog: record.resume_backlog,
        attempt_count: record.attempt_count,
        created_at_unix_ms: record.created_at.timestamp_millis(),
    }
}

fn mailbox_kind(payload: &WorkflowEvent) -> Option<WorkflowMailboxKind> {
    match payload {
        WorkflowEvent::WorkflowTriggered { .. } => Some(WorkflowMailboxKind::Trigger),
        WorkflowEvent::SignalQueued { .. } => Some(WorkflowMailboxKind::Signal),
        WorkflowEvent::WorkflowUpdateRequested { .. } => Some(WorkflowMailboxKind::Update),
        WorkflowEvent::WorkflowCancellationRequested { .. } => {
            Some(WorkflowMailboxKind::CancelRequest)
        }
        _ => None,
    }
}

fn mailbox_message_id(payload: &WorkflowEvent) -> Option<&str> {
    match payload {
        WorkflowEvent::SignalQueued { signal_id, .. } => Some(signal_id),
        WorkflowEvent::WorkflowUpdateRequested { update_id, .. } => Some(update_id),
        _ => None,
    }
}

fn mailbox_message_name(payload: &WorkflowEvent) -> Option<&str> {
    match payload {
        WorkflowEvent::SignalQueued { signal_type, .. } => Some(signal_type),
        WorkflowEvent::WorkflowUpdateRequested { update_name, .. } => Some(update_name),
        WorkflowEvent::WorkflowCancellationRequested { reason } => Some(reason),
        _ => None,
    }
}

fn mailbox_payload(payload: &WorkflowEvent) -> Option<&Value> {
    match payload {
        WorkflowEvent::WorkflowTriggered { input } => Some(input),
        WorkflowEvent::SignalQueued { payload, .. } => Some(payload),
        WorkflowEvent::WorkflowUpdateRequested { payload, .. } => Some(payload),
        _ => None,
    }
}

fn should_use_local_stream_v2_bulk_terminal_fast_path(
    event: &EventEnvelope<WorkflowEvent>,
) -> bool {
    matches!(
        event.payload,
        WorkflowEvent::BulkActivityBatchCompleted { .. }
            | WorkflowEvent::BulkActivityBatchFailed { .. }
            | WorkflowEvent::BulkActivityBatchCancelled { .. }
    ) && event.metadata.get("throughput_backend").is_some_and(|backend| backend == "stream-v2")
}

async fn should_use_local_stream_v2_trigger_fast_path(
    state: &AppState,
    event: &EventEnvelope<WorkflowEvent>,
) -> Result<bool> {
    if !matches!(event.payload, WorkflowEvent::WorkflowTriggered { .. }) {
        return Ok(false);
    }
    let Some(task_queue) = event.metadata.get("workflow_task_queue") else {
        return Ok(false);
    };
    Ok(state
        .store
        .get_task_queue_throughput_policy(&event.tenant_id, TaskQueueKind::Activity, task_queue)
        .await?
        .is_some_and(|policy| policy.backend == "stream-v2"))
}

fn resume_details(payload: &WorkflowEvent) -> Option<(WorkflowResumeKind, String, Option<&str>)> {
    match payload {
        WorkflowEvent::TimerFired { timer_id } => {
            Some((WorkflowResumeKind::TimerFired, timer_id.clone(), None))
        }
        WorkflowEvent::ActivityTaskCompleted { activity_id, .. } => {
            Some((WorkflowResumeKind::ActivityTerminal, activity_id.clone(), Some("completed")))
        }
        WorkflowEvent::ActivityTaskFailed { activity_id, .. } => {
            Some((WorkflowResumeKind::ActivityTerminal, activity_id.clone(), Some("failed")))
        }
        WorkflowEvent::ActivityTaskTimedOut { activity_id, .. } => {
            Some((WorkflowResumeKind::ActivityTerminal, activity_id.clone(), Some("timed_out")))
        }
        WorkflowEvent::ActivityTaskCancelled { activity_id, .. } => {
            Some((WorkflowResumeKind::ActivityTerminal, activity_id.clone(), Some("cancelled")))
        }
        WorkflowEvent::BulkActivityBatchCompleted { batch_id, .. } => {
            Some((WorkflowResumeKind::ActivityTerminal, batch_id.clone(), Some("completed")))
        }
        WorkflowEvent::BulkActivityBatchFailed { batch_id, .. } => {
            Some((WorkflowResumeKind::ActivityTerminal, batch_id.clone(), Some("failed")))
        }
        WorkflowEvent::BulkActivityBatchCancelled { batch_id, .. } => {
            Some((WorkflowResumeKind::ActivityTerminal, batch_id.clone(), Some("cancelled")))
        }
        WorkflowEvent::ChildWorkflowCompleted { child_id, .. } => {
            Some((WorkflowResumeKind::ChildTerminal, child_id.clone(), Some("completed")))
        }
        WorkflowEvent::ChildWorkflowFailed { child_id, .. } => {
            Some((WorkflowResumeKind::ChildTerminal, child_id.clone(), Some("failed")))
        }
        WorkflowEvent::ChildWorkflowCancelled { child_id, .. } => {
            Some((WorkflowResumeKind::ChildTerminal, child_id.clone(), Some("cancelled")))
        }
        WorkflowEvent::ChildWorkflowTerminated { child_id, .. } => {
            Some((WorkflowResumeKind::ChildTerminal, child_id.clone(), Some("terminated")))
        }
        _ => None,
    }
}

fn scheduled_activity_record(event: &EventEnvelope<WorkflowEvent>) -> WorkflowActivityRecord {
    let WorkflowEvent::ActivityTaskScheduled {
        activity_id,
        activity_type,
        task_queue,
        attempt,
        input,
        config,
        state,
        schedule_to_start_timeout_ms,
        start_to_close_timeout_ms,
        heartbeat_timeout_ms,
    } = &event.payload
    else {
        unreachable!("scheduled_activity_record only accepts ActivityTaskScheduled");
    };

    WorkflowActivityRecord {
        tenant_id: event.tenant_id.clone(),
        instance_id: event.instance_id.clone(),
        run_id: event.run_id.clone(),
        definition_id: event.definition_id.clone(),
        definition_version: Some(event.definition_version),
        artifact_hash: Some(event.artifact_hash.clone()),
        activity_id: activity_id.clone(),
        attempt: *attempt,
        activity_type: activity_type.clone(),
        task_queue: task_queue.clone(),
        state: state.clone(),
        status: WorkflowActivityStatus::Scheduled,
        input: input.clone(),
        config: config.clone(),
        output: None,
        error: None,
        cancellation_requested: false,
        cancellation_reason: None,
        cancellation_metadata: None,
        worker_id: None,
        worker_build_id: None,
        scheduled_at: event.occurred_at,
        started_at: None,
        last_heartbeat_at: None,
        lease_expires_at: None,
        completed_at: None,
        schedule_to_start_timeout_ms: *schedule_to_start_timeout_ms,
        start_to_close_timeout_ms: *start_to_close_timeout_ms,
        heartbeat_timeout_ms: *heartbeat_timeout_ms,
        last_event_id: event.event_id,
        last_event_type: event.event_type.clone(),
        updated_at: event.occurred_at,
    }
}

fn queue_key(tenant_id: &str, task_queue: &str) -> String {
    format!("{tenant_id}:{task_queue}")
}

fn activity_worker_key(
    tenant_id: &str,
    task_queue: &str,
    worker_id: &str,
    worker_build_id: &str,
) -> String {
    format!("{tenant_id}:{task_queue}:{worker_id}:{worker_build_id}")
}

fn workflow_poller_cache_key(worker_id: &str, worker_build_id: &str, partition_id: i32) -> String {
    format!("{worker_id}:{worker_build_id}:{partition_id}")
}

async fn maybe_heartbeat_workflow_poller_queues(
    state: &AppState,
    worker_id: &str,
    worker_build_id: &str,
    partition_id: i32,
) {
    let now = Utc::now();
    if !state
        .workflow_poller_cache
        .heartbeat_due(worker_id, worker_build_id, partition_id, now)
        .await
    {
        return;
    }

    let ttl = chrono::Duration::seconds(state.runtime.lease_ttl_seconds as i64);
    let heartbeat_interval =
        chrono::Duration::seconds((state.runtime.lease_ttl_seconds.max(3) / 3) as i64);
    let queues = if let Some(queues) =
        state.workflow_poller_cache.cached_queues(worker_build_id, now).await
    {
        queues
    } else {
        match state.store.list_queues_for_build(TaskQueueKind::Workflow, worker_build_id).await {
            Ok(queues) => {
                state
                    .workflow_poller_cache
                    .store_queues(worker_build_id, queues.clone(), now + ttl)
                    .await;
                queues
            }
            Err(error) => {
                warn!(
                    error = %error,
                    worker_id = %worker_id,
                    worker_build_id = %worker_build_id,
                    partition_id,
                    "failed to refresh workflow poller queues for build"
                );
                state
                    .workflow_poller_cache
                    .mark_heartbeat(
                        worker_id,
                        worker_build_id,
                        partition_id,
                        now + chrono::Duration::seconds(1),
                    )
                    .await;
                return;
            }
        }
    };

    for (tenant_id, task_queue) in queues {
        if let Err(error) = state
            .store
            .upsert_queue_poller(
                &tenant_id,
                TaskQueueKind::Workflow,
                &task_queue,
                worker_id,
                worker_build_id,
                Some(partition_id),
                None,
                ttl,
            )
            .await
        {
            warn!(
                error = %error,
                tenant_id = %tenant_id,
                task_queue = %task_queue,
                worker_id = %worker_id,
                worker_build_id = %worker_build_id,
                partition_id,
                "failed to heartbeat workflow poller queue"
            );
            state
                .workflow_poller_cache
                .mark_heartbeat(
                    worker_id,
                    worker_build_id,
                    partition_id,
                    now + chrono::Duration::seconds(1),
                )
                .await;
            return;
        }
    }

    state
        .workflow_poller_cache
        .mark_heartbeat(worker_id, worker_build_id, partition_id, now + heartbeat_interval)
        .await;
}

async fn resolve_workflow_task_queue(
    state: &AppState,
    event: &EventEnvelope<WorkflowEvent>,
) -> Result<String> {
    if let Some(task_queue) = event.metadata.get("workflow_task_queue") {
        return Ok(task_queue.clone());
    }
    Ok(state
        .store
        .get_run_record(&event.tenant_id, &event.instance_id, &event.run_id)
        .await?
        .map(|run| run.workflow_task_queue)
        .unwrap_or_else(|| "default".to_owned()))
}

fn internal_status(error: anyhow::Error) -> Status {
    Status::internal(error.to_string())
}

impl MatchingDebugState {
    fn new(runtime: &MatchingRuntimeConfig) -> Self {
        Self {
            started_at: Utc::now(),
            last_apply_finished_at: None,
            activity_result_apply_batch_max_items: runtime.activity_result_apply_batch_max_items,
            activity_result_apply_batch_max_bytes: runtime.activity_result_apply_batch_max_bytes,
            activity_result_apply_flush_interval_ms: runtime
                .activity_result_apply_flush_interval_ms,
            result_apply_per_run_coalescing_cap: runtime.result_apply_per_run_coalescing_cap,
            received_requests: 0,
            received_results: 0,
            received_result_bytes: 0,
            applied_batches: 0,
            applied_results: 0,
            skipped_results: 0,
            published_terminal_events: 0,
            completed_results: 0,
            failed_results: 0,
            cancelled_results: 0,
            avg_batch_size: 0.0,
            max_batch_size: 0,
            avg_batch_bytes: 0.0,
            max_batch_bytes: 0,
            avg_apply_latency_ms: 0.0,
            max_apply_latency_ms: 0,
            applied_results_per_second: 0.0,
            published_terminal_events_per_second: 0.0,
        }
    }

    fn record_received(&mut self, request_count: u64, result_count: usize, bytes: usize) {
        self.received_requests += request_count;
        self.received_results += result_count as u64;
        self.received_result_bytes += bytes as u64;
    }

    fn record_batch(
        &mut self,
        batch_size: usize,
        batch_bytes: usize,
        applied_results: usize,
        skipped_results: usize,
        published_events: u64,
        applied: &[AppliedActivityTerminalUpdate],
        apply_latency_ms: f64,
    ) {
        self.applied_batches += 1;
        self.applied_results += applied_results as u64;
        self.skipped_results += skipped_results as u64;
        self.published_terminal_events += published_events;
        self.avg_batch_size =
            rolling_average(self.avg_batch_size, self.applied_batches, batch_size as f64);
        self.max_batch_size = self.max_batch_size.max(batch_size as u64);
        self.avg_batch_bytes =
            rolling_average(self.avg_batch_bytes, self.applied_batches, batch_bytes as f64);
        self.max_batch_bytes = self.max_batch_bytes.max(batch_bytes as u64);
        self.avg_apply_latency_ms =
            rolling_average(self.avg_apply_latency_ms, self.applied_batches, apply_latency_ms);
        self.max_apply_latency_ms = self.max_apply_latency_ms.max(apply_latency_ms as u64);
        self.last_apply_finished_at = Some(Utc::now());
        for update in applied {
            match update.payload {
                ActivityTerminalPayload::Completed { .. } => self.completed_results += 1,
                ActivityTerminalPayload::Failed { .. } => self.failed_results += 1,
                ActivityTerminalPayload::Cancelled { .. } => self.cancelled_results += 1,
            }
        }
        let elapsed_seconds =
            (Utc::now() - self.started_at).num_milliseconds().max(1) as f64 / 1000.0;
        self.applied_results_per_second = self.applied_results as f64 / elapsed_seconds;
        self.published_terminal_events_per_second =
            self.published_terminal_events as f64 / elapsed_seconds;
    }
}

fn rolling_average(current: f64, sample_count: u64, sample: f64) -> f64 {
    if sample_count <= 1 {
        sample
    } else {
        ((current * (sample_count - 1) as f64) + sample) / sample_count as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use fabrik_broker::build_workflow_consumer;
    use fabrik_events::{WorkflowTurnRouting, workflow_turn_routing};
    use fabrik_worker_protocol::activity_worker::activity_worker_api_server::ActivityWorkerApi;
    use serde_json::json;
    use std::{
        net::TcpListener,
        process::Command,
        time::{Duration as StdDuration, Instant},
    };
    use tokio::time::sleep;
    use tonic::Request;

    fn demo_event(payload: WorkflowEvent) -> EventEnvelope<WorkflowEvent> {
        EventEnvelope::new(
            payload.event_type(),
            WorkflowIdentity::new(
                "tenant-a".to_owned(),
                "demo".to_owned(),
                1,
                "artifact".to_owned(),
                "wf-1".to_owned(),
                "run-1".to_owned(),
                "test",
            ),
            payload,
        )
    }

    struct TestPostgres {
        container_name: String,
        database_url: String,
    }

    impl TestPostgres {
        fn start() -> Result<Option<Self>> {
            if !docker_available() {
                eprintln!(
                    "skipping matching-service integration tests because docker is unavailable"
                );
                return Ok(None);
            }

            let container_name = format!("fabrik-matching-test-pg-{}", Uuid::now_v7());
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

    struct TestRedpanda {
        container_name: String,
        broker: BrokerConfig,
    }

    impl TestRedpanda {
        fn start() -> Result<Option<Self>> {
            if !docker_available() {
                eprintln!(
                    "skipping matching-service integration tests because docker is unavailable"
                );
                return Ok(None);
            }

            let kafka_port = choose_free_port().context("failed to allocate kafka host port")?;
            let container_name = format!("fabrik-matching-test-rp-{}", Uuid::now_v7());
            let image = std::env::var("FABRIK_TEST_REDPANDA_IMAGE")
                .unwrap_or_else(|_| "docker.redpanda.com/redpandadata/redpanda:v25.1.2".to_owned());
            let topic = format!("workflow-events-test-{}", Uuid::now_v7());
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
                broker: BrokerConfig::new(format!("127.0.0.1:{kafka_port}"), topic, 1),
            }))
        }

        async fn connect_publisher(&self) -> Result<WorkflowPublisher> {
            let deadline = Instant::now() + StdDuration::from_secs(45);
            loop {
                match WorkflowPublisher::new(&self.broker, "matching-service-test").await {
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

    fn choose_free_port() -> Result<u16> {
        let listener = TcpListener::bind("127.0.0.1:0").context("failed to bind ephemeral port")?;
        let port = listener.local_addr().context("failed to read socket address")?.port();
        drop(listener);
        Ok(port)
    }

    async fn wait_for_workflow_task_count(
        store: &WorkflowStore,
        tenant_id: &str,
        task_queue: &str,
        expected_backlog: u64,
        timeout: StdDuration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let inspection = store
                .inspect_task_queue(tenant_id, TaskQueueKind::Workflow, task_queue, Utc::now())
                .await?;
            if inspection.backlog == expected_backlog {
                return Ok(());
            }
            if Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for workflow backlog {expected_backlog}");
            }
            sleep(StdDuration::from_millis(50)).await;
        }
    }

    fn test_runtime() -> MatchingRuntimeConfig {
        MatchingRuntimeConfig {
            lease_ttl_seconds: 30,
            heartbeat_publish_interval_ms: 30_000,
            sweep_interval_ms: 500,
            max_rebuild_tasks: 10_000,
            activity_result_apply_batch_max_items: 256,
            activity_result_apply_batch_max_bytes: 1_048_576,
            activity_result_apply_flush_interval_ms: 5,
            result_apply_per_run_coalescing_cap: 256,
        }
    }

    fn app_state(store: WorkflowStore, publisher: WorkflowPublisher) -> AppState {
        let runtime = test_runtime();
        let (result_submitter, _result_receiver) = unbounded_channel();
        AppState {
            store,
            publisher,
            queues: Arc::new(QueueIndex::new()),
            activity_prefetch: Arc::new(ActivityPrefetchIndex::new()),
            bulk_prefetch: Arc::new(BulkPrefetchIndex::new()),
            workflow_poller_cache: Arc::new(WorkflowPollerCache::new()),
            workflow_notify: Arc::new(Notify::new()),
            bulk_notify: Arc::new(Notify::new()),
            outbox_notify: Arc::new(Notify::new()),
            debug: Arc::new(StdMutex::new(MatchingDebugState::new(&runtime))),
            result_submitter,
            runtime,
            outbox_publisher_id: "test-publisher".to_owned(),
        }
    }

    fn prepared_update(
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        activity_id: &str,
    ) -> PreparedActivityTerminalUpdate {
        let update = ActivityTerminalUpdate {
            tenant_id: tenant_id.to_owned(),
            instance_id: instance_id.to_owned(),
            run_id: run_id.to_owned(),
            activity_id: activity_id.to_owned(),
            attempt: 1,
            worker_id: "worker-a".to_owned(),
            worker_build_id: "build-a".to_owned(),
            event_id: Uuid::now_v7(),
            event_type: "ActivityTaskCompleted".to_owned(),
            occurred_at: Utc::now(),
            payload: ActivityTerminalPayload::Completed { output: json!({"ok": true}) },
        };
        PreparedActivityTerminalUpdate {
            estimated_bytes: estimate_result_size(&update),
            run_key: ActivityRunKey {
                tenant_id: tenant_id.to_owned(),
                instance_id: instance_id.to_owned(),
                run_id: run_id.to_owned(),
            },
            update,
        }
    }

    #[test]
    fn workflow_task_classification_matches_turn_boundaries() {
        assert!(matches!(
            workflow_turn_routing(&WorkflowEvent::WorkflowTriggered { input: json!({"ok": true}) }),
            WorkflowTurnRouting::MatchingPoller
        ));
        assert!(matches!(
            workflow_turn_routing(&WorkflowEvent::SignalQueued {
                signal_id: "sig-1".to_owned(),
                signal_type: "approved".to_owned(),
                payload: json!(true),
            }),
            WorkflowTurnRouting::MatchingPoller
        ));
        assert!(matches!(
            workflow_turn_routing(&WorkflowEvent::ActivityTaskCompleted {
                activity_id: "a1".to_owned(),
                attempt: 1,
                output: json!({"done": true}),
                worker_id: "worker-1".to_owned(),
                worker_build_id: "build-1".to_owned(),
            }),
            WorkflowTurnRouting::MatchingPoller
        ));
        assert!(matches!(
            workflow_turn_routing(&WorkflowEvent::WorkflowStarted),
            WorkflowTurnRouting::LocalExecutor
        ));
        assert!(matches!(
            workflow_turn_routing(&WorkflowEvent::ActivityTaskScheduled {
                activity_id: "a1".to_owned(),
                activity_type: "demo".to_owned(),
                task_queue: "default".to_owned(),
                attempt: 1,
                input: Value::Null,
                config: None,
                state: None,
                schedule_to_start_timeout_ms: None,
                start_to_close_timeout_ms: None,
                heartbeat_timeout_ms: None,
            }),
            WorkflowTurnRouting::LocalExecutor
        ));
    }

    #[test]
    fn workflow_task_proto_preserves_core_routing_fields() {
        let event = demo_event(WorkflowEvent::WorkflowTriggered { input: json!({"ok": true}) });
        let record = WorkflowTaskRecord {
            task_id: Uuid::now_v7(),
            tenant_id: event.tenant_id.clone(),
            instance_id: event.instance_id.clone(),
            run_id: event.run_id.clone(),
            definition_id: event.definition_id.clone(),
            definition_version: Some(event.definition_version),
            artifact_hash: Some(event.artifact_hash.clone()),
            partition_id: 2,
            task_queue: "payments".to_owned(),
            preferred_build_id: Some("build-a".to_owned()),
            status: fabrik_store::WorkflowTaskStatus::Pending,
            source_event_id: event.event_id,
            source_event_type: event.event_type.clone(),
            source_event: event.clone(),
            mailbox_consumed_seq: 0,
            resume_consumed_seq: 0,
            mailbox_high_watermark: 1,
            resume_high_watermark: 0,
            mailbox_backlog: 1,
            resume_backlog: 0,
            lease_poller_id: None,
            lease_build_id: None,
            lease_expires_at: None,
            attempt_count: 0,
            last_error: None,
            created_at: event.occurred_at,
            updated_at: event.occurred_at,
        };

        let proto = workflow_task_to_proto(&record);
        assert_eq!(proto.partition_id, 2);
        assert_eq!(proto.task_queue, "payments");
        assert_eq!(proto.preferred_build_id, "build-a");
        assert_eq!(proto.mailbox_backlog, 1);
        assert_eq!(proto.resume_backlog, 0);
    }

    #[test]
    fn split_activity_result_batches_respects_per_run_cap() {
        let items = vec![
            QueuedActivityResult {
                request_index: 0,
                prepared: prepared_update("tenant-a", "wf-1", "run-1", "a1"),
            },
            QueuedActivityResult {
                request_index: 0,
                prepared: prepared_update("tenant-a", "wf-1", "run-1", "a2"),
            },
            QueuedActivityResult {
                request_index: 1,
                prepared: prepared_update("tenant-a", "wf-2", "run-2", "b1"),
            },
            QueuedActivityResult {
                request_index: 1,
                prepared: prepared_update("tenant-a", "wf-1", "run-1", "a3"),
            },
        ];

        let batches = split_activity_result_batches(items, 10, usize::MAX, 2);
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 3);
        assert_eq!(batches[1].len(), 1);
        assert_eq!(batches[1][0].prepared.update.activity_id, "a3");
    }

    #[test]
    fn prepare_bulk_result_accepts_cbor_payloads() {
        let output = vec![json!({"ok": true})];
        let lease_token = Uuid::now_v7();
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
            lease_token: lease_token.to_string(),
            lease_epoch: 1,
            owner_epoch: 1,
            report_id: "report-a".to_owned(),
            result: Some(
                fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Completed(
                    fabrik_worker_protocol::activity_worker::BulkActivityTaskCompletedResult {
                        output_json: String::new(),
                        result_handle_json: String::new(),
                        output_cbor: encode_cbor(&Value::Array(output.clone()), "bulk result output")
                            .expect("CBOR output encodes"),
                        result_handle_cbor: Vec::new(),
                    },
                ),
            ),
        };

        let prepared = prepare_bulk_result(result).expect("CBOR result should decode");

        assert_eq!(prepared.lease_token, lease_token);
        assert_eq!(prepared.payload, BulkChunkTerminalPayload::Completed { output });
    }

    #[tokio::test]
    async fn activity_poll_rejects_incompatible_builds() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let publisher = redpanda.connect_publisher().await?;
        let state = app_state(store.clone(), publisher);
        let api = ActivityApi { state: state.clone() };

        store
            .register_task_queue_build(
                "tenant-a",
                TaskQueueKind::Activity,
                "payments",
                "build-a",
                &[],
                None,
            )
            .await?;
        store
            .register_task_queue_build(
                "tenant-a",
                TaskQueueKind::Activity,
                "payments",
                "build-b",
                &[],
                None,
            )
            .await?;
        store
            .upsert_compatibility_set(
                "tenant-a",
                TaskQueueKind::Activity,
                "payments",
                "stable",
                &["build-a".to_owned()],
                true,
            )
            .await?;

        let status = api
            .poll_activity_task(Request::new(PollActivityTaskRequest {
                tenant_id: "tenant-a".to_owned(),
                task_queue: "payments".to_owned(),
                worker_id: "worker-b".to_owned(),
                worker_build_id: "build-b".to_owned(),
                poll_timeout_ms: 1,
                supports_cbor: false,
            }))
            .await
            .expect_err("incompatible activity worker should be rejected");
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        Ok(())
    }

    #[tokio::test]
    async fn activity_default_set_promotion_changes_routing_for_new_tasks_only() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let publisher = redpanda.connect_publisher().await?;
        let state = app_state(store.clone(), publisher);
        let api = ActivityApi { state: state.clone() };

        store
            .register_task_queue_build(
                "tenant-a",
                TaskQueueKind::Activity,
                "payments",
                "build-a",
                &[],
                None,
            )
            .await?;
        store
            .register_task_queue_build(
                "tenant-a",
                TaskQueueKind::Activity,
                "payments",
                "build-b",
                &[],
                None,
            )
            .await?;
        store
            .upsert_compatibility_set(
                "tenant-a",
                TaskQueueKind::Activity,
                "payments",
                "stable",
                &["build-a".to_owned()],
                true,
            )
            .await?;
        store
            .upsert_compatibility_set(
                "tenant-a",
                TaskQueueKind::Activity,
                "payments",
                "canary",
                &["build-b".to_owned()],
                false,
            )
            .await?;

        process_event(
            &state,
            0,
            demo_event(WorkflowEvent::ActivityTaskScheduled {
                activity_id: "a1".to_owned(),
                activity_type: "charge".to_owned(),
                task_queue: "payments".to_owned(),
                attempt: 1,
                input: json!({"amount": 10}),
                config: None,
                state: Some("pay".to_owned()),
                schedule_to_start_timeout_ms: None,
                start_to_close_timeout_ms: None,
                heartbeat_timeout_ms: None,
            }),
        )
        .await?;

        let first = api
            .poll_activity_task(Request::new(PollActivityTaskRequest {
                tenant_id: "tenant-a".to_owned(),
                task_queue: "payments".to_owned(),
                worker_id: "worker-a".to_owned(),
                worker_build_id: "build-a".to_owned(),
                poll_timeout_ms: 1,
                supports_cbor: false,
            }))
            .await?
            .into_inner()
            .task
            .context("build-a should receive first task")?;
        assert_eq!(first.activity_id, "a1");

        store
            .set_default_compatibility_set(
                "tenant-a",
                TaskQueueKind::Activity,
                "payments",
                "canary",
            )
            .await?;

        process_event(
            &state,
            0,
            demo_event(WorkflowEvent::ActivityTaskScheduled {
                activity_id: "a2".to_owned(),
                activity_type: "charge".to_owned(),
                task_queue: "payments".to_owned(),
                attempt: 1,
                input: json!({"amount": 20}),
                config: None,
                state: Some("pay".to_owned()),
                schedule_to_start_timeout_ms: None,
                start_to_close_timeout_ms: None,
                heartbeat_timeout_ms: None,
            }),
        )
        .await?;

        let build_a_denied = api
            .poll_activity_task(Request::new(PollActivityTaskRequest {
                tenant_id: "tenant-a".to_owned(),
                task_queue: "payments".to_owned(),
                worker_id: "worker-a".to_owned(),
                worker_build_id: "build-a".to_owned(),
                poll_timeout_ms: 1,
                supports_cbor: false,
            }))
            .await
            .expect_err("promoted default set should reject old build");
        assert_eq!(build_a_denied.code(), tonic::Code::PermissionDenied);

        let second = api
            .poll_activity_task(Request::new(PollActivityTaskRequest {
                tenant_id: "tenant-a".to_owned(),
                task_queue: "payments".to_owned(),
                worker_id: "worker-b".to_owned(),
                worker_build_id: "build-b".to_owned(),
                poll_timeout_ms: 1,
                supports_cbor: false,
            }))
            .await?
            .into_inner()
            .task
            .context("build-b should receive promoted task")?;
        assert_eq!(second.activity_id, "a2");

        let first_record = store
            .get_activity_attempt("tenant-a", "wf-1", "run-1", "a1", 1)
            .await?
            .context("first activity attempt should exist")?;
        assert_eq!(first_record.status, WorkflowActivityStatus::Started);
        assert_eq!(first_record.worker_build_id.as_deref(), Some("build-a"));
        Ok(())
    }

    #[tokio::test]
    async fn activity_batch_poll_returns_multiple_tasks() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let publisher = redpanda.connect_publisher().await?;
        let state = app_state(store.clone(), publisher);
        let api = ActivityApi { state: state.clone() };

        store
            .register_task_queue_build(
                "tenant-a",
                TaskQueueKind::Activity,
                "payments",
                "build-a",
                &[],
                None,
            )
            .await?;
        store
            .upsert_compatibility_set(
                "tenant-a",
                TaskQueueKind::Activity,
                "payments",
                "stable",
                &["build-a".to_owned()],
                true,
            )
            .await?;

        for (activity_id, amount) in [("a1", 10), ("a2", 20), ("a3", 30)] {
            process_event(
                &state,
                0,
                demo_event(WorkflowEvent::ActivityTaskScheduled {
                    activity_id: activity_id.to_owned(),
                    activity_type: "charge".to_owned(),
                    task_queue: "payments".to_owned(),
                    attempt: 1,
                    input: json!({"amount": amount}),
                    config: None,
                    state: Some("pay".to_owned()),
                    schedule_to_start_timeout_ms: None,
                    start_to_close_timeout_ms: None,
                    heartbeat_timeout_ms: None,
                }),
            )
            .await?;
        }

        let response = api
            .poll_activity_tasks(Request::new(PollActivityTasksRequest {
                tenant_id: "tenant-a".to_owned(),
                task_queue: "payments".to_owned(),
                worker_id: "worker-a".to_owned(),
                worker_build_id: "build-a".to_owned(),
                poll_timeout_ms: 1,
                max_tasks: 3,
                supports_cbor: false,
            }))
            .await?
            .into_inner();

        assert_eq!(response.tasks.len(), 3);
        assert_eq!(response.tasks[0].activity_id, "a1");
        assert_eq!(response.tasks[1].activity_id, "a2");
        assert_eq!(response.tasks[2].activity_id, "a3");
        Ok(())
    }

    #[tokio::test]
    async fn broker_event_loop_materializes_workflow_tasks() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let publisher = redpanda.connect_publisher().await?;
        let state = app_state(store.clone(), publisher.clone());
        let consumer = build_workflow_consumer(
            &redpanda.broker,
            "matching-service-test",
            &redpanda.broker.all_partition_ids(),
        )
        .await?;
        let event_loop = tokio::spawn(run_event_loop(state.clone(), consumer));

        let identity = WorkflowIdentity::new(
            "tenant-a".to_owned(),
            "demo".to_owned(),
            1,
            "artifact-a".to_owned(),
            "wf-broker".to_owned(),
            "run-broker".to_owned(),
            "test",
        );
        let mut trigger = EventEnvelope::new(
            WorkflowEvent::WorkflowTriggered { input: json!({"ok": true}) }.event_type(),
            identity,
            WorkflowEvent::WorkflowTriggered { input: json!({"ok": true}) },
        );
        trigger.metadata.insert("workflow_task_queue".to_owned(), "payments".to_owned());

        publisher.publish(&trigger, &trigger.partition_key).await?;

        wait_for_workflow_task_count(&store, "tenant-a", "payments", 1, StdDuration::from_secs(10))
            .await?;

        let inspection = store
            .inspect_task_queue("tenant-a", TaskQueueKind::Workflow, "payments", Utc::now())
            .await?;
        assert_eq!(inspection.backlog, 1);

        event_loop.abort();
        let _ = event_loop.await;
        Ok(())
    }

    #[tokio::test]
    async fn bulk_backend_guard_rejects_stream_batches() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        store
            .upsert_bulk_batch(
                "tenant-a",
                "wf-stream-bulk",
                "run-stream-bulk",
                "demo",
                Some(1),
                Some("artifact-1"),
                "batch-stream",
                "benchmark.echo",
                "bulk",
                Some("join"),
                &json!({ "kind": "inline", "key": "bulk-input:batch-stream" }),
                &json!({ "kind": "inline", "key": "bulk-result:batch-stream" }),
                &[json!({"value": 1})],
                1,
                1,
                0,
                None,
                None,
                "legacy",
                1,
                false,
                1,
                "stream-v2",
                "2.0.0",
                Utc::now(),
            )
            .await?;

        let error = ensure_bulk_batch_backend(
            &store,
            "tenant-a",
            "wf-stream-bulk",
            "run-stream-bulk",
            "batch-stream",
            ThroughputBackend::PgV1.as_str(),
        )
        .await
        .expect_err("stream-v2 batch should be rejected by pg-v1 matcher");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);

        Ok(())
    }
}
