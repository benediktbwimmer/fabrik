use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    env, fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use anyhow::{Context, Result};
use axum::{Json, routing::get};
use chrono::{DateTime, Utc};
use fabrik_broker::{
    BrokerConfig, build_workflow_consumer, decode_consumed_workflow_event,
};
use fabrik_config::{GrpcServiceConfig, HttpServiceConfig, PostgresConfig, RedpandaConfig};
use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::{
    ActivityStartUpdate, ActivityTerminalPayload, ActivityTerminalUpdate, WorkflowStore,
};
use fabrik_worker_protocol::activity_worker::{
    Ack, ActivityTask, ActivityTaskCancelledResult, ActivityTaskCompletedResult,
    ActivityTaskFailedResult, ActivityTaskResult, PollActivityTaskRequest, PollActivityTaskResponse,
    PollActivityTasksRequest, PollActivityTasksResponse, PollBulkActivityTaskRequest,
    PollBulkActivityTaskResponse, RecordActivityHeartbeatRequest,
    RecordActivityHeartbeatResponse, ReportActivityTaskCancelledRequest,
    ReportActivityTaskResultsRequest, ReportBulkActivityTaskResultsRequest,
    activity_task_result,
    activity_worker_api_server::{ActivityWorkerApi, ActivityWorkerApiServer},
};
use fabrik_workflow::{
    ArtifactExecutionState, CompiledExecutionPlan, CompiledWorkflowArtifact, ExecutionTurnContext,
    RetryPolicy, WorkflowInstanceState, WorkflowStatus, parse_timer_ref,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, Notify};
use tonic::{Request, Response, Status, transport::Server};
use tracing::{error, info, warn};
use uuid::Uuid;

const DEFAULT_LEASE_TTL_SECS: i64 = 30;
const DEFAULT_REQUEUE_SWEEP_MS: u64 = 1_000;
const DEFAULT_PERSIST_SNAPSHOT_EVERY: u64 = 16;
const DEFAULT_PERSIST_FLUSH_INTERVAL_MS: u64 = 200;

#[derive(Clone)]
struct AppState {
    store: WorkflowStore,
    inner: Arc<StdMutex<RuntimeInner>>,
    notify: Arc<Notify>,
    persist_notify: Arc<Notify>,
    persist_lock: Arc<Mutex<()>>,
    persistence: PersistenceConfig,
    debug: Arc<StdMutex<UnifiedDebugState>>,
}

#[derive(Debug, Clone)]
struct PersistenceConfig {
    state_dir: PathBuf,
    snapshot_every: u64,
    flush_interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
struct UnifiedDebugState {
    restored_from_snapshot: bool,
    restore_records_applied: u64,
    workflow_triggers_applied: u64,
    duplicate_triggers_ignored: u64,
    poll_requests: u64,
    poll_responses: u64,
    leased_tasks: u64,
    empty_polls: u64,
    report_rpcs_received: u64,
    reports_received: u64,
    report_batches_applied: u64,
    retries_scheduled: u64,
    retries_released: u64,
    lease_requeues: u64,
    instance_terminals: u64,
    log_writes: u64,
    snapshot_writes: u64,
}

#[derive(Debug, Clone, Default)]
struct RuntimeInner {
    next_seq: u64,
    instances: HashMap<RunKey, RuntimeWorkflowState>,
    ready: HashMap<QueueKey, VecDeque<QueuedActivity>>,
    leased: HashMap<AttemptKey, LeasedActivity>,
    delayed_retries: Vec<DelayedRetryTask>,
    dirty_reason: Option<String>,
    force_snapshot: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct RunKey {
    tenant_id: String,
    instance_id: String,
    run_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct QueueKey {
    tenant_id: String,
    task_queue: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AttemptKey {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    activity_id: String,
    attempt: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RuntimeWorkflowState {
    artifact: CompiledWorkflowArtifact,
    instance: WorkflowInstanceState,
    active_activities: BTreeMap<String, ActiveActivityMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActiveActivityMeta {
    attempt: u32,
    task_queue: String,
    activity_type: String,
    wait_state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueuedActivity {
    tenant_id: String,
    definition_id: String,
    definition_version: u32,
    artifact_hash: String,
    instance_id: String,
    run_id: String,
    activity_id: String,
    activity_type: String,
    task_queue: String,
    attempt: u32,
    input: Value,
    config: Option<Value>,
    state: String,
    scheduled_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeasedActivity {
    task: QueuedActivity,
    worker_id: String,
    worker_build_id: String,
    lease_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DelayedRetryTask {
    task: QueuedActivity,
    due_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedRuntimeState {
    seq: u64,
    instances: Vec<RuntimeWorkflowState>,
    ready: Vec<QueuedActivity>,
    leased: Vec<LeasedActivity>,
    delayed_retries: Vec<DelayedRetryTask>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedLogRecord {
    seq: u64,
    reason: String,
    state: PersistedRuntimeState,
}

#[derive(Debug, Clone)]
enum PreparedDbAction {
    Schedule(QueuedActivity),
    Terminal(ActivityTerminalUpdate),
    UpsertInstance(WorkflowInstanceState),
    CloseRun(RunKey, DateTime<Utc>),
}

#[derive(Debug, Clone, Serialize)]
struct UnifiedDebugResponse {
    runtime: UnifiedDebugState,
    instances: usize,
    ready_tasks: usize,
    leased_tasks: usize,
    delayed_retries: usize,
}

#[derive(Clone)]
struct WorkerApi {
    state: AppState,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = GrpcServiceConfig::from_env("UNIFIED_RUNTIME", "unified-runtime", 50054)?;
    let debug_config = HttpServiceConfig::from_env("UNIFIED_DEBUG", "unified-runtime-debug", 3008)?;
    let postgres = PostgresConfig::from_env()?;
    let redpanda = RedpandaConfig::from_env()?;
    init_tracing(&config.log_filter);

    let store = WorkflowStore::connect(&postgres.url).await?;
    store.init().await?;
    let state_dir = env::var("UNIFIED_RUNTIME_STATE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/unified-runtime-state"));
    let persistence = PersistenceConfig {
        state_dir,
        snapshot_every: env::var("UNIFIED_RUNTIME_SNAPSHOT_EVERY")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PERSIST_SNAPSHOT_EVERY),
        flush_interval_ms: env::var("UNIFIED_RUNTIME_PERSIST_FLUSH_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_PERSIST_FLUSH_INTERVAL_MS),
    };
    fs::create_dir_all(&persistence.state_dir)
        .with_context(|| format!("failed to create {}", persistence.state_dir.display()))?;

    let debug = Arc::new(StdMutex::new(UnifiedDebugState::default()));
    let inner = match restore_runtime_state(&persistence, &debug)? {
        Some(inner) => inner,
        None => RuntimeInner::default(),
    };
    let state = AppState {
        store,
        inner: Arc::new(StdMutex::new(inner)),
        notify: Arc::new(Notify::new()),
        persist_notify: Arc::new(Notify::new()),
        persist_lock: Arc::new(Mutex::new(())),
        persistence,
        debug: debug.clone(),
    };

    let broker = BrokerConfig::new(
        redpanda.brokers,
        redpanda.workflow_events_topic,
        redpanda.workflow_events_partitions,
    );
    let consumer =
        build_workflow_consumer(&broker, "unified-runtime", &broker.all_partition_ids()).await?;
    tokio::spawn(run_trigger_consumer(state.clone(), consumer));
    tokio::spawn(run_retry_release_loop(state.clone()));
    tokio::spawn(run_lease_requeue_loop(state.clone()));
    tokio::spawn(run_persist_loop(state.clone()));

    let debug_state = state.clone();
    let debug_app = default_router::<AppState>(ServiceInfo::new(
        debug_config.name,
        "unified-runtime-debug",
        env!("CARGO_PKG_VERSION"),
    ))
    .route(
        "/debug/unified",
        get(move || {
            let debug_state = debug_state.clone();
            async move {
                let runtime = debug_state
                    .debug
                    .lock()
                    .expect("unified debug lock poisoned")
                    .clone();
                let inner = debug_state
                    .inner
                    .lock()
                    .expect("unified runtime lock poisoned");
                Json(UnifiedDebugResponse {
                    runtime,
                    instances: inner.instances.len(),
                    ready_tasks: inner.ready.values().map(VecDeque::len).sum(),
                    leased_tasks: inner.leased.len(),
                    delayed_retries: inner.delayed_retries.len(),
                })
            }
        }),
    )
    .with_state(state.clone());
    tokio::spawn(async move {
        if let Err(error) = serve(debug_app, debug_config.port).await {
            error!(error = %error, "unified-runtime debug server exited");
        }
    });

    let addr = format!("0.0.0.0:{}", config.port).parse()?;
    info!(%addr, "unified-runtime listening");
    Server::builder()
        .add_service(ActivityWorkerApiServer::new(WorkerApi { state }))
        .serve(addr)
        .await?;
    Ok(())
}

async fn run_trigger_consumer(state: AppState, mut consumer: fabrik_broker::WorkflowConsumerStream) {
    while let Some(message) = consumer.next().await {
        let record = match message {
            Ok(record) => record,
            Err(error) => {
                error!(error = %error, "unified-runtime consumer error");
                continue;
            }
        };
        let event = match decode_consumed_workflow_event(&record) {
            Ok(event) => event,
            Err(error) => {
                warn!(error = %error, "unified-runtime skipping invalid event");
                continue;
            }
        };
        if let WorkflowEvent::WorkflowTriggered { .. } = event.payload {
            if let Err(error) = handle_trigger_event(&state, event).await {
                error!(error = %error, "unified-runtime failed to handle trigger");
            }
        }
    }
}

async fn handle_trigger_event(state: &AppState, event: EventEnvelope<WorkflowEvent>) -> Result<()> {
    let run_key = RunKey {
        tenant_id: event.tenant_id.clone(),
        instance_id: event.instance_id.clone(),
        run_id: event.run_id.clone(),
    };
    {
        let mut debug = state.debug.lock().expect("unified debug lock poisoned");
        if state
            .inner
            .lock()
            .expect("unified runtime lock poisoned")
            .instances
            .contains_key(&run_key)
        {
            debug.duplicate_triggers_ignored = debug.duplicate_triggers_ignored.saturating_add(1);
            return Ok(());
        }
    }

    let artifact = state
        .store
        .get_artifact_version(&event.tenant_id, &event.definition_id, event.definition_version)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "missing artifact {} v{} for tenant {}",
                event.definition_id,
                event.definition_version,
                event.tenant_id
            )
        })?;
    let WorkflowEvent::WorkflowTriggered { input } = &event.payload else {
        return Ok(());
    };
    let plan = artifact.execute_trigger_with_turn(
        input,
        ExecutionTurnContext { event_id: event.event_id, occurred_at: event.occurred_at },
    )?;
    let workflow_task_queue = event
        .metadata
        .get("workflow_task_queue")
        .cloned()
        .unwrap_or_else(|| "default".to_owned());

    let mut instance = WorkflowInstanceState {
        tenant_id: event.tenant_id.clone(),
        instance_id: event.instance_id.clone(),
        run_id: event.run_id.clone(),
        definition_id: event.definition_id.clone(),
        definition_version: Some(event.definition_version),
        artifact_hash: Some(event.artifact_hash.clone()),
        workflow_task_queue,
        sticky_workflow_build_id: None,
        sticky_workflow_poller_id: None,
        current_state: Some(plan.final_state.clone()),
        context: plan.context.clone(),
        artifact_execution: Some(plan.execution_state.clone()),
        status: WorkflowStatus::Running,
        input: Some(input.clone()),
        output: plan.output.clone(),
        event_count: 1,
        last_event_id: event.event_id,
        last_event_type: event.event_type.clone(),
        updated_at: event.occurred_at,
    };
    apply_compiled_plan(&mut instance, &plan);
    let queued = schedule_activities_from_plan(&instance, &plan, event.occurred_at)?;
    {
        let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
        let runtime = RuntimeWorkflowState {
            artifact,
            instance: instance.clone(),
            active_activities: queued
                .iter()
                .map(|task| {
                    (
                        task.activity_id.clone(),
                        ActiveActivityMeta {
                            attempt: task.attempt,
                            task_queue: task.task_queue.clone(),
                            activity_type: task.activity_type.clone(),
                            wait_state: task.state.clone(),
                        },
                    )
                })
                .collect(),
        };
        inner.instances.insert(run_key.clone(), runtime);
        for task in &queued {
            inner
                .ready
                .entry(QueueKey {
                    tenant_id: task.tenant_id.clone(),
                    task_queue: task.task_queue.clone(),
                })
                .or_default()
                .push_back(task.clone());
        }
        mark_runtime_dirty(&mut inner, "trigger", true);
    }
    let schedule_actions = queued.into_iter().map(PreparedDbAction::Schedule).collect::<Vec<_>>();
    apply_db_actions(state, vec![PreparedDbAction::UpsertInstance(instance)], schedule_actions)
        .await?;
    state.notify.notify_waiters();
    state.persist_notify.notify_one();
    let mut debug = state.debug.lock().expect("unified debug lock poisoned");
    debug.workflow_triggers_applied = debug.workflow_triggers_applied.saturating_add(1);
    Ok(())
}

async fn run_retry_release_loop(state: AppState) {
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let now = Utc::now();
        let released = {
            let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
            let mut released = Vec::new();
            let mut remaining = Vec::with_capacity(inner.delayed_retries.len());
            for retry in inner.delayed_retries.drain(..) {
                if retry.due_at <= now {
                    released.push(retry.task);
                } else {
                    remaining.push(retry);
                }
            }
            if released.is_empty() {
                inner.delayed_retries = remaining;
                Vec::new()
            } else {
                inner.delayed_retries = remaining;
                for task in &released {
                    inner
                        .ready
                        .entry(QueueKey {
                            tenant_id: task.tenant_id.clone(),
                            task_queue: task.task_queue.clone(),
                        })
                        .or_default()
                        .push_back(task.clone());
                }
                mark_runtime_dirty(&mut inner, "retry_release", false);
                released
            }
        };
        if released.is_empty() {
            continue;
        }
        let actions = released.into_iter().map(PreparedDbAction::Schedule).collect::<Vec<_>>();
        let released_count = actions.len() as u64;
        if let Err(error) = apply_db_actions(&state, Vec::new(), actions).await {
            error!(error = %error, "unified-runtime failed to release retries");
            continue;
        }
        state.notify.notify_waiters();
        state.persist_notify.notify_one();
        let mut debug = state.debug.lock().expect("unified debug lock poisoned");
        debug.retries_released = debug.retries_released.saturating_add(released_count);
    }
}

async fn run_lease_requeue_loop(state: AppState) {
    loop {
        tokio::time::sleep(Duration::from_millis(DEFAULT_REQUEUE_SWEEP_MS)).await;
        let now = Utc::now();
        let requeued = {
            let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
            let expired = inner
                .leased
                .iter()
                .filter_map(|(key, leased)| {
                    (leased.lease_expires_at <= now).then_some((key.clone(), leased.task.clone()))
                })
                .collect::<Vec<_>>();
            if expired.is_empty() {
                Vec::new()
            } else {
                for (key, task) in &expired {
                    inner.leased.remove(key);
                    inner
                        .ready
                        .entry(QueueKey {
                            tenant_id: task.tenant_id.clone(),
                            task_queue: task.task_queue.clone(),
                        })
                        .or_default()
                        .push_back(task.clone());
                }
                mark_runtime_dirty(&mut inner, "lease_requeue", false);
                expired
            }
        };
        if requeued.is_empty() {
            continue;
        }
        for (key, _) in &requeued {
            if let Err(error) = state
                .store
                .requeue_activity(
                    &key.tenant_id,
                    &key.instance_id,
                    &key.run_id,
                    &key.activity_id,
                    key.attempt,
                    now,
                )
                .await
            {
                error!(error = %error, activity_id = %key.activity_id, "failed to requeue leased activity");
            }
        }
        state.notify.notify_waiters();
        state.persist_notify.notify_one();
        let mut debug = state.debug.lock().expect("unified debug lock poisoned");
        debug.lease_requeues = debug.lease_requeues.saturating_add(requeued.len() as u64);
    }
}

async fn run_persist_loop(state: AppState) {
    let interval = Duration::from_millis(state.persistence.flush_interval_ms.max(1));
    loop {
        state.persist_notify.notified().await;
        tokio::time::sleep(interval).await;
        let Some((reason, force_snapshot, persisted)) = capture_dirty_persisted_state(&state) else {
            continue;
        };
        if let Err(error) =
            persist_runtime_state(&state, &reason, persisted, force_snapshot).await
        {
            error!(error = %error, reason = %reason, "unified-runtime failed to persist state");
        }
    }
}

fn mark_runtime_dirty(inner: &mut RuntimeInner, reason: &str, force_snapshot: bool) {
    inner.dirty_reason = Some(reason.to_owned());
    inner.force_snapshot |= force_snapshot;
}

fn capture_dirty_persisted_state(
    state: &AppState,
) -> Option<(String, bool, PersistedRuntimeState)> {
    let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
    let reason = inner.dirty_reason.take()?;
    let force_snapshot = std::mem::take(&mut inner.force_snapshot);
    let persisted = capture_persisted_state(&mut inner);
    Some((reason, force_snapshot, persisted))
}

#[tonic::async_trait]
impl ActivityWorkerApi for WorkerApi {
    async fn poll_activity_task(
        &self,
        request: Request<PollActivityTaskRequest>,
    ) -> Result<Response<PollActivityTaskResponse>, Status> {
        let mut response = self
            .poll_activity_tasks(Request::new(PollActivityTasksRequest {
                tenant_id: request.get_ref().tenant_id.clone(),
                task_queue: request.get_ref().task_queue.clone(),
                worker_id: request.get_ref().worker_id.clone(),
                worker_build_id: request.get_ref().worker_build_id.clone(),
                poll_timeout_ms: request.get_ref().poll_timeout_ms,
                max_tasks: 1,
            }))
            .await?
            .into_inner();
        Ok(Response::new(PollActivityTaskResponse { task: response.tasks.pop() }))
    }

    async fn poll_activity_tasks(
        &self,
        request: Request<PollActivityTasksRequest>,
    ) -> Result<Response<PollActivityTasksResponse>, Status> {
        let request = request.into_inner();
        {
            let mut debug = self.state.debug.lock().expect("unified debug lock poisoned");
            debug.poll_requests = debug.poll_requests.saturating_add(1);
        }
        let max_tasks = request.max_tasks.max(1) as usize;
        let queue_key = QueueKey {
            tenant_id: request.tenant_id.clone(),
            task_queue: request.task_queue.clone(),
        };
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(request.poll_timeout_ms.max(1).min(30_000));

        loop {
            let leased = {
                let mut inner = self.state.inner.lock().expect("unified runtime lock poisoned");
                lease_ready_tasks(
                    &mut inner,
                    &queue_key,
                    &request.worker_id,
                    &request.worker_build_id,
                    max_tasks,
                )
            };
            if !leased.is_empty() {
                let applied = self
                    .state
                    .store
                    .mark_activities_started_batch(
                        &leased
                            .iter()
                            .map(|leased| ActivityStartUpdate {
                                tenant_id: leased.task.tenant_id.clone(),
                                instance_id: leased.task.instance_id.clone(),
                                run_id: leased.task.run_id.clone(),
                                activity_id: leased.task.activity_id.clone(),
                                attempt: leased.task.attempt,
                                worker_id: leased.worker_id.clone(),
                                worker_build_id: leased.worker_build_id.clone(),
                                occurred_at: leased.task.scheduled_at.max(Utc::now()),
                                lease_expires_at: leased.lease_expires_at,
                                event_id: Uuid::now_v7(),
                                event_type: "UnifiedActivityStarted".to_owned(),
                            })
                            .collect::<Vec<_>>(),
                    )
                    .await
                    .map_err(internal_status)?;
                let applied_keys = applied
                    .into_iter()
                    .map(|update| AttemptKey {
                        tenant_id: update.record.tenant_id,
                        instance_id: update.record.instance_id,
                        run_id: update.record.run_id,
                        activity_id: update.record.activity_id,
                        attempt: update.record.attempt,
                    })
                    .collect::<HashSet<_>>();
                let tasks = {
                    let mut inner = self.state.inner.lock().expect("unified runtime lock poisoned");
                    let mut tasks = Vec::new();
                    for leased_task in leased {
                        let key = attempt_key_from_task(&leased_task.task);
                        if !applied_keys.contains(&key) {
                            inner.leased.remove(&key);
                            inner
                                .ready
                                .entry(QueueKey {
                                    tenant_id: leased_task.task.tenant_id.clone(),
                                    task_queue: leased_task.task.task_queue.clone(),
                                })
                                .or_default()
                                .push_front(leased_task.task.clone());
                            continue;
                        }
                        tasks.push(activity_proto(&leased_task));
                    }
                    tasks
                };
                let mut debug = self.state.debug.lock().expect("unified debug lock poisoned");
                debug.poll_responses = debug.poll_responses.saturating_add(1);
                debug.leased_tasks = debug.leased_tasks.saturating_add(tasks.len() as u64);
                return Ok(Response::new(PollActivityTasksResponse { tasks }));
            }
            if tokio::time::Instant::now() >= deadline {
                let mut debug = self.state.debug.lock().expect("unified debug lock poisoned");
                debug.empty_polls = debug.empty_polls.saturating_add(1);
                return Ok(Response::new(PollActivityTasksResponse { tasks: Vec::new() }));
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let notified = self.state.notify.notified();
            let _ = tokio::time::timeout(remaining, notified).await;
        }
    }

    async fn poll_bulk_activity_task(
        &self,
        request: Request<PollBulkActivityTaskRequest>,
    ) -> Result<Response<PollBulkActivityTaskResponse>, Status> {
        let timeout = Duration::from_millis(request.get_ref().poll_timeout_ms.max(1).min(30_000));
        let _ = tokio::time::timeout(timeout, self.state.notify.notified()).await;
        Ok(Response::new(PollBulkActivityTaskResponse { tasks: Vec::new() }))
    }

    async fn complete_activity_task(
        &self,
        request: Request<fabrik_worker_protocol::activity_worker::CompleteActivityTaskRequest>,
    ) -> Result<Response<Ack>, Status> {
        let request = request.into_inner();
        self.report_activity_task_results(Request::new(ReportActivityTaskResultsRequest {
            results: vec![ActivityTaskResult {
                tenant_id: request.tenant_id,
                instance_id: request.instance_id,
                run_id: request.run_id,
                activity_id: request.activity_id,
                attempt: request.attempt,
                worker_id: request.worker_id,
                worker_build_id: request.worker_build_id,
                result: Some(activity_task_result::Result::Completed(
                    ActivityTaskCompletedResult { output_json: request.output_json },
                )),
            }],
        }))
        .await?;
        Ok(Response::new(Ack {}))
    }

    async fn fail_activity_task(
        &self,
        request: Request<fabrik_worker_protocol::activity_worker::FailActivityTaskRequest>,
    ) -> Result<Response<Ack>, Status> {
        let request = request.into_inner();
        self.report_activity_task_results(Request::new(ReportActivityTaskResultsRequest {
            results: vec![ActivityTaskResult {
                tenant_id: request.tenant_id,
                instance_id: request.instance_id,
                run_id: request.run_id,
                activity_id: request.activity_id,
                attempt: request.attempt,
                worker_id: request.worker_id,
                worker_build_id: request.worker_build_id,
                result: Some(activity_task_result::Result::Failed(ActivityTaskFailedResult {
                    error: request.error,
                })),
            }],
        }))
        .await?;
        Ok(Response::new(Ack {}))
    }

    async fn record_activity_heartbeat(
        &self,
        _request: Request<RecordActivityHeartbeatRequest>,
    ) -> Result<Response<RecordActivityHeartbeatResponse>, Status> {
        Ok(Response::new(RecordActivityHeartbeatResponse { cancellation_requested: false }))
    }

    async fn report_activity_task_cancelled(
        &self,
        request: Request<ReportActivityTaskCancelledRequest>,
    ) -> Result<Response<Ack>, Status> {
        let request = request.into_inner();
        self.report_activity_task_results(Request::new(ReportActivityTaskResultsRequest {
            results: vec![ActivityTaskResult {
                tenant_id: request.tenant_id,
                instance_id: request.instance_id,
                run_id: request.run_id,
                activity_id: request.activity_id,
                attempt: request.attempt,
                worker_id: request.worker_id,
                worker_build_id: request.worker_build_id,
                result: Some(activity_task_result::Result::Cancelled(
                    ActivityTaskCancelledResult {
                        reason: request.reason,
                        metadata_json: request.metadata_json,
                    },
                )),
            }],
        }))
        .await?;
        Ok(Response::new(Ack {}))
    }

    async fn report_activity_task_results(
        &self,
        request: Request<ReportActivityTaskResultsRequest>,
    ) -> Result<Response<Ack>, Status> {
        let request = request.into_inner();
        {
            let mut debug = self.state.debug.lock().expect("unified debug lock poisoned");
            debug.report_rpcs_received = debug.report_rpcs_received.saturating_add(1);
            debug.reports_received = debug.reports_received.saturating_add(request.results.len() as u64);
        }
        let now = Utc::now();
        let actions =
            prepare_result_application(&self.state, request.results, now).map_err(internal_status)?;
        apply_db_actions(&self.state, actions.general, actions.schedules)
            .await
            .map_err(internal_status)?;
        if actions.notifies {
            self.state.notify.notify_waiters();
        }
        self.state.persist_notify.notify_one();
        let mut debug = self.state.debug.lock().expect("unified debug lock poisoned");
        debug.report_batches_applied = debug.report_batches_applied.saturating_add(1);
        debug.retries_scheduled = debug.retries_scheduled.saturating_add(actions.retries_scheduled);
        debug.instance_terminals = debug.instance_terminals.saturating_add(actions.instance_terminals);
        Ok(Response::new(Ack {}))
    }

    async fn report_bulk_activity_task_results(
        &self,
        _request: Request<ReportBulkActivityTaskResultsRequest>,
    ) -> Result<Response<Ack>, Status> {
        Ok(Response::new(Ack {}))
    }
}

struct PreparedActions {
    general: Vec<PreparedDbAction>,
    schedules: Vec<PreparedDbAction>,
    notifies: bool,
    retries_scheduled: u64,
    instance_terminals: u64,
}

fn prepare_result_application(
    state: &AppState,
    results: Vec<ActivityTaskResult>,
    now: DateTime<Utc>,
) -> Result<PreparedActions> {
    let mut general = Vec::new();
    let mut schedules = Vec::new();
    let mut retries_scheduled = 0_u64;
    let mut instance_terminals = 0_u64;
    let mut notifies = false;
    let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
    let mut grouped = HashMap::<RunKey, Vec<EventEnvelope<WorkflowEvent>>>::new();

    for result in results {
        let run_key = RunKey {
            tenant_id: result.tenant_id.clone(),
            instance_id: result.instance_id.clone(),
            run_id: result.run_id.clone(),
        };
        let attempt_key = AttemptKey {
            tenant_id: result.tenant_id.clone(),
            instance_id: result.instance_id.clone(),
            run_id: result.run_id.clone(),
            activity_id: result.activity_id.clone(),
            attempt: result.attempt,
        };
        inner.leased.remove(&attempt_key);
        let Some(runtime) = inner.instances.get_mut(&run_key) else {
            continue;
        };
        let Some(active) = runtime.active_activities.get(&result.activity_id).cloned() else {
            continue;
        };
        if active.attempt != result.attempt {
            continue;
        }

        let mut delayed_retry = None;
        let identity = (
            runtime.instance.definition_id.clone(),
            runtime
                .instance
                .definition_version
                .unwrap_or(runtime.artifact.definition_version),
            runtime.instance.artifact_hash.clone().unwrap_or_default(),
        );
        let payload = match result.result.as_ref() {
            Some(activity_task_result::Result::Completed(completed)) => {
                let output = parse_json_or_string(&completed.output_json);
                general.push(PreparedDbAction::Terminal(ActivityTerminalUpdate {
                    tenant_id: result.tenant_id.clone(),
                    instance_id: result.instance_id.clone(),
                    run_id: result.run_id.clone(),
                    activity_id: result.activity_id.clone(),
                    attempt: result.attempt,
                    worker_id: result.worker_id.clone(),
                    worker_build_id: result.worker_build_id.clone(),
                    payload: ActivityTerminalPayload::Completed { output: output.clone() },
                    event_id: Uuid::now_v7(),
                    event_type: "UnifiedActivityCompleted".to_owned(),
                    occurred_at: now,
                }));
                runtime.active_activities.remove(&result.activity_id);
                WorkflowEvent::ActivityTaskCompleted {
                    activity_id: result.activity_id.clone(),
                    attempt: result.attempt,
                    output,
                    worker_id: result.worker_id.clone(),
                    worker_build_id: result.worker_build_id.clone(),
                }
            }
            Some(activity_task_result::Result::Failed(failed)) => {
                general.push(PreparedDbAction::Terminal(ActivityTerminalUpdate {
                    tenant_id: result.tenant_id.clone(),
                    instance_id: result.instance_id.clone(),
                    run_id: result.run_id.clone(),
                    activity_id: result.activity_id.clone(),
                    attempt: result.attempt,
                    worker_id: result.worker_id.clone(),
                    worker_build_id: result.worker_build_id.clone(),
                    payload: ActivityTerminalPayload::Failed { error: failed.error.clone() },
                    event_id: Uuid::now_v7(),
                    event_type: "UnifiedActivityFailed".to_owned(),
                    occurred_at: now,
                }));
                if let Some(retry) =
                    runtime.artifact.step_retry(&active.wait_state, runtime_execution_state(runtime))?
                {
                    if result.attempt < retry.max_attempts {
                        if let Some(retry_task) =
                            build_retry_task(runtime, &result.activity_id, result.attempt + 1, retry, now)?
                        {
                            runtime.active_activities.insert(
                                result.activity_id.clone(),
                                ActiveActivityMeta {
                                    attempt: retry_task.task.attempt,
                                    task_queue: retry_task.task.task_queue.clone(),
                                    activity_type: retry_task.task.activity_type.clone(),
                                    wait_state: retry_task.task.state.clone(),
                                },
                            );
                            delayed_retry = Some(retry_task);
                            retries_scheduled = retries_scheduled.saturating_add(1);
                        } else {
                            runtime.active_activities.remove(&result.activity_id);
                        }
                    } else {
                        runtime.active_activities.remove(&result.activity_id);
                    }
                } else {
                    runtime.active_activities.remove(&result.activity_id);
                }
                let payload = WorkflowEvent::ActivityTaskFailed {
                    activity_id: result.activity_id.clone(),
                    attempt: result.attempt,
                    error: failed.error.clone(),
                    worker_id: result.worker_id.clone(),
                    worker_build_id: result.worker_build_id.clone(),
                };
                payload
            }
            Some(activity_task_result::Result::Cancelled(cancelled)) => {
                general.push(PreparedDbAction::Terminal(ActivityTerminalUpdate {
                    tenant_id: result.tenant_id.clone(),
                    instance_id: result.instance_id.clone(),
                    run_id: result.run_id.clone(),
                    activity_id: result.activity_id.clone(),
                    attempt: result.attempt,
                    worker_id: result.worker_id.clone(),
                    worker_build_id: result.worker_build_id.clone(),
                    payload: ActivityTerminalPayload::Cancelled {
                        reason: cancelled.reason.clone(),
                        metadata: parse_optional_json(&cancelled.metadata_json),
                    },
                    event_id: Uuid::now_v7(),
                    event_type: "UnifiedActivityCancelled".to_owned(),
                    occurred_at: now,
                }));
                runtime.active_activities.remove(&result.activity_id);
                WorkflowEvent::ActivityTaskCancelled {
                    activity_id: result.activity_id.clone(),
                    attempt: result.attempt,
                    reason: cancelled.reason.clone(),
                    worker_id: result.worker_id.clone(),
                    worker_build_id: result.worker_build_id.clone(),
                    metadata: parse_optional_json(&cancelled.metadata_json),
                }
            }
            None => continue,
        };
        if let Some(retry_task) = delayed_retry {
            inner.delayed_retries.push(retry_task);
            continue;
        }
        grouped
            .entry(run_key)
            .or_default()
            .push(synthetic_runtime_event(payload, &result, &identity.0, identity.1, &identity.2, now));
    }

    for (run_key, events) in grouped {
        let Some(runtime) = inner.instances.get_mut(&run_key) else {
            continue;
        };
        let Some(wait_state) = runtime.instance.current_state.clone() else {
            continue;
        };
        let execution_state = runtime.instance.artifact_execution.clone().unwrap_or_default();
        if let Some((plan, _)) = runtime
            .artifact
            .try_execute_after_step_terminal_batch_with_turn(&wait_state, &events, execution_state)?
        {
            apply_compiled_plan(&mut runtime.instance, &plan);
            runtime.instance.updated_at = now;
            let scheduled_tasks = schedule_activities_from_plan(&runtime.instance, &plan, now)?;
            let mut terminal_instance = None;
            for task in &scheduled_tasks {
                schedules.push(PreparedDbAction::Schedule(task.clone()));
                runtime.active_activities.insert(
                    task.activity_id.clone(),
                    ActiveActivityMeta {
                        attempt: task.attempt,
                        task_queue: task.task_queue.clone(),
                        activity_type: task.activity_type.clone(),
                        wait_state: task.state.clone(),
                    },
                );
            }
            if runtime.instance.status.is_terminal() {
                terminal_instance = Some(runtime.instance.clone());
            }
            let _ = runtime;
            for task in scheduled_tasks {
                inner
                    .ready
                    .entry(QueueKey {
                        tenant_id: task.tenant_id.clone(),
                        task_queue: task.task_queue.clone(),
                    })
                    .or_default()
                    .push_back(task);
                notifies = true;
            }
            if let Some(instance) = terminal_instance {
                general.push(PreparedDbAction::UpsertInstance(instance));
                general.push(PreparedDbAction::CloseRun(run_key.clone(), now));
                instance_terminals = instance_terminals.saturating_add(1);
            }
        }
    }

    mark_runtime_dirty(&mut inner, "report_batch", false);
    Ok(PreparedActions {
        general,
        schedules,
        notifies,
        retries_scheduled,
        instance_terminals,
    })
}

async fn apply_db_actions(
    state: &AppState,
    general: Vec<PreparedDbAction>,
    schedules: Vec<PreparedDbAction>,
) -> Result<()> {
    let mut terminal_updates = Vec::new();
    let mut other_general = Vec::new();
    for action in general {
        match action {
            PreparedDbAction::Terminal(update) => terminal_updates.push(update),
            other => other_general.push(other),
        }
    }
    if !terminal_updates.is_empty() {
        state.store.apply_activity_terminal_batch(&terminal_updates).await?;
    }
    for action in other_general.into_iter().chain(schedules) {
        match action {
            PreparedDbAction::Terminal(_) => unreachable!("terminal updates are batched"),
            PreparedDbAction::Schedule(task) => {
                state
                    .store
                    .upsert_activity_scheduled(
                        &task.tenant_id,
                        &task.instance_id,
                        &task.run_id,
                        &task.definition_id,
                        Some(task.definition_version),
                        Some(&task.artifact_hash),
                        &task.activity_id,
                        task.attempt,
                        &task.activity_type,
                        &task.task_queue,
                        Some(&task.state),
                        &task.input,
                        task.config.as_ref(),
                        None,
                        None,
                        None,
                        Uuid::now_v7(),
                        "UnifiedActivityScheduled",
                        task.scheduled_at,
                    )
                    .await?;
            }
            PreparedDbAction::UpsertInstance(instance) => {
                state.store.upsert_instance(&instance).await?;
            }
            PreparedDbAction::CloseRun(run_key, closed_at) => {
                state
                    .store
                    .close_run(&run_key.tenant_id, &run_key.instance_id, &run_key.run_id, closed_at)
                    .await?;
            }
        }
    }
    Ok(())
}

fn lease_ready_tasks(
    inner: &mut RuntimeInner,
    queue_key: &QueueKey,
    worker_id: &str,
    worker_build_id: &str,
    max_tasks: usize,
) -> Vec<LeasedActivity> {
    let Some(queue) = inner.ready.get_mut(queue_key) else {
        return Vec::new();
    };
    let mut leased = Vec::new();
    let lease_expires_at = Utc::now() + chrono::Duration::seconds(DEFAULT_LEASE_TTL_SECS);
    for _ in 0..max_tasks {
        let Some(task) = queue.pop_front() else {
            break;
        };
        let leased_task = LeasedActivity {
            task: task.clone(),
            worker_id: worker_id.to_owned(),
            worker_build_id: worker_build_id.to_owned(),
            lease_expires_at,
        };
        inner
            .leased
            .insert(attempt_key_from_task(&task), leased_task.clone());
        leased.push(leased_task);
    }
    leased
}

fn activity_proto(leased: &LeasedActivity) -> ActivityTask {
    ActivityTask {
        tenant_id: leased.task.tenant_id.clone(),
        definition_id: leased.task.definition_id.clone(),
        definition_version: leased.task.definition_version,
        artifact_hash: leased.task.artifact_hash.clone(),
        instance_id: leased.task.instance_id.clone(),
        run_id: leased.task.run_id.clone(),
        activity_id: leased.task.activity_id.clone(),
        activity_type: leased.task.activity_type.clone(),
        task_queue: leased.task.task_queue.clone(),
        attempt: leased.task.attempt,
        input_json: serde_json::to_string(&leased.task.input).unwrap_or_else(|_| "null".to_owned()),
        config_json: leased
            .task
            .config
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .ok()
            .flatten()
            .unwrap_or_default(),
        state: leased.task.state.clone(),
        scheduled_at_unix_ms: leased.task.scheduled_at.timestamp_millis(),
        lease_expires_at_unix_ms: leased.lease_expires_at.timestamp_millis(),
        start_to_close_timeout_ms: 0,
        heartbeat_timeout_ms: 0,
        cancellation_requested: false,
        schedule_to_start_timeout_ms: 0,
    }
}

fn schedule_activities_from_plan(
    instance: &WorkflowInstanceState,
    plan: &CompiledExecutionPlan,
    scheduled_at: DateTime<Utc>,
) -> Result<Vec<QueuedActivity>> {
    let mut tasks = Vec::new();
    for emission in &plan.emissions {
        if let WorkflowEvent::ActivityTaskScheduled {
            activity_id,
            activity_type,
            task_queue,
            attempt,
            input,
            config,
            state,
            ..
        } = &emission.event
        {
            tasks.push(QueuedActivity {
                tenant_id: instance.tenant_id.clone(),
                definition_id: instance.definition_id.clone(),
                definition_version: instance.definition_version.unwrap_or(plan.workflow_version),
                artifact_hash: instance.artifact_hash.clone().unwrap_or_default(),
                instance_id: instance.instance_id.clone(),
                run_id: instance.run_id.clone(),
                activity_id: activity_id.clone(),
                activity_type: activity_type.clone(),
                task_queue: task_queue.clone(),
                attempt: *attempt,
                input: input.clone(),
                config: config.clone(),
                state: state.clone().or_else(|| emission.state.clone()).unwrap_or_else(|| {
                    instance.current_state.clone().unwrap_or_default()
                }),
                scheduled_at,
            });
        }
    }
    Ok(tasks)
}

fn build_retry_task(
    runtime: &RuntimeWorkflowState,
    activity_id: &str,
    next_attempt: u32,
    retry: &RetryPolicy,
    now: DateTime<Utc>,
) -> Result<Option<DelayedRetryTask>> {
    let Some(active) = runtime.active_activities.get(activity_id) else {
        return Ok(None);
    };
    let execution_state = runtime_execution_state(runtime).clone();
    let (activity_type, config, input) = runtime.artifact.step_details(activity_id, &execution_state)?;
    let due_at = now + parse_timer_ref(&retry.delay)?;
    Ok(Some(DelayedRetryTask {
        task: QueuedActivity {
            tenant_id: runtime.instance.tenant_id.clone(),
            definition_id: runtime.instance.definition_id.clone(),
            definition_version: runtime
                .instance
                .definition_version
                .unwrap_or(runtime.artifact.definition_version),
            artifact_hash: runtime.instance.artifact_hash.clone().unwrap_or_default(),
            instance_id: runtime.instance.instance_id.clone(),
            run_id: runtime.instance.run_id.clone(),
            activity_id: activity_id.to_owned(),
            activity_type,
            task_queue: active.task_queue.clone(),
            attempt: next_attempt,
            input,
            config: config
                .map(|value| serde_json::to_value(value).expect("step config serializes")),
            state: active.wait_state.clone(),
            scheduled_at: due_at,
        },
        due_at,
    }))
}

fn synthetic_runtime_event(
    payload: WorkflowEvent,
    result: &ActivityTaskResult,
    definition_id: &str,
    definition_version: u32,
    artifact_hash: &str,
    occurred_at: DateTime<Utc>,
) -> EventEnvelope<WorkflowEvent> {
    EventEnvelope::new(
        payload.event_type(),
        WorkflowIdentity::new(
            result.tenant_id.clone(),
            definition_id.to_owned(),
            definition_version,
            artifact_hash.to_owned(),
            result.instance_id.clone(),
            result.run_id.clone(),
            "unified-runtime",
        ),
        payload,
    )
    .with_occurred_at(occurred_at)
}

trait EventEnvelopeExt {
    fn with_occurred_at(self, occurred_at: DateTime<Utc>) -> Self;
}

impl EventEnvelopeExt for EventEnvelope<WorkflowEvent> {
    fn with_occurred_at(mut self, occurred_at: DateTime<Utc>) -> Self {
        self.occurred_at = occurred_at;
        self
    }
}

fn runtime_execution_state(runtime: &RuntimeWorkflowState) -> &ArtifactExecutionState {
    runtime
        .instance
        .artifact_execution
        .as_ref()
        .expect("unified runtime requires artifact execution state")
}

fn attempt_key_from_task(task: &QueuedActivity) -> AttemptKey {
    AttemptKey {
        tenant_id: task.tenant_id.clone(),
        instance_id: task.instance_id.clone(),
        run_id: task.run_id.clone(),
        activity_id: task.activity_id.clone(),
        attempt: task.attempt,
    }
}

fn capture_persisted_state(inner: &mut RuntimeInner) -> PersistedRuntimeState {
    inner.next_seq = inner.next_seq.saturating_add(1);
    PersistedRuntimeState {
        seq: inner.next_seq,
        instances: inner.instances.values().cloned().collect(),
        ready: inner
            .ready
            .values()
            .flat_map(|queue| queue.iter().cloned())
            .collect(),
        leased: inner.leased.values().cloned().collect(),
        delayed_retries: inner.delayed_retries.clone(),
    }
}

async fn persist_runtime_state(
    state: &AppState,
    reason: &str,
    persisted: PersistedRuntimeState,
    force_snapshot: bool,
) -> Result<()> {
    let _guard = state.persist_lock.lock().await;
    let log_record = PersistedLogRecord {
        seq: persisted.seq,
        reason: reason.to_owned(),
        state: persisted.clone(),
    };
    let log_path = runtime_log_path(&state.persistence);
    append_json_line(&log_path, &log_record)?;
    {
        let mut debug = state.debug.lock().expect("unified debug lock poisoned");
        debug.log_writes = debug.log_writes.saturating_add(1);
    }
    if persisted.seq % state.persistence.snapshot_every == 0 || force_snapshot {
        write_json_atomically(&runtime_snapshot_path(&state.persistence), &persisted)?;
        let mut debug = state.debug.lock().expect("unified debug lock poisoned");
        debug.snapshot_writes = debug.snapshot_writes.saturating_add(1);
    }
    Ok(())
}

fn restore_runtime_state(
    persistence: &PersistenceConfig,
    debug: &Arc<StdMutex<UnifiedDebugState>>,
) -> Result<Option<RuntimeInner>> {
    let snapshot_path = runtime_snapshot_path(persistence);
    let log_path = runtime_log_path(persistence);
    let snapshot = if snapshot_path.exists() {
        Some(
            serde_json::from_slice::<PersistedRuntimeState>(
                &fs::read(&snapshot_path)
                    .with_context(|| format!("failed to read {}", snapshot_path.display()))?,
            )
            .with_context(|| format!("failed to decode {}", snapshot_path.display()))?,
        )
    } else {
        None
    };
    let mut restored = snapshot;
    if log_path.exists() {
        let log = fs::read_to_string(&log_path)
            .with_context(|| format!("failed to read {}", log_path.display()))?;
        for line in log.lines().filter(|line| !line.trim().is_empty()) {
            let record = serde_json::from_str::<PersistedLogRecord>(line)
                .with_context(|| format!("failed to decode {}", log_path.display()))?;
            if restored.as_ref().map(|state| state.seq).unwrap_or_default() < record.seq {
                restored = Some(record.state);
            }
        }
    }
    let Some(restored) = restored else {
        return Ok(None);
    };
    let mut inner = RuntimeInner { next_seq: restored.seq, ..RuntimeInner::default() };
    for runtime in restored.instances {
        inner.instances.insert(
            RunKey {
                tenant_id: runtime.instance.tenant_id.clone(),
                instance_id: runtime.instance.instance_id.clone(),
                run_id: runtime.instance.run_id.clone(),
            },
            runtime,
        );
    }
    for task in restored.ready {
        inner
            .ready
            .entry(QueueKey {
                tenant_id: task.tenant_id.clone(),
                task_queue: task.task_queue.clone(),
            })
            .or_default()
            .push_back(task);
    }
    for leased in restored.leased {
        inner.leased.insert(attempt_key_from_task(&leased.task), leased);
    }
    inner.delayed_retries = restored.delayed_retries;
    let mut debug = debug.lock().expect("unified debug lock poisoned");
    debug.restored_from_snapshot = true;
    debug.restore_records_applied = inner.instances.len() as u64;
    Ok(Some(inner))
}

fn runtime_snapshot_path(persistence: &PersistenceConfig) -> PathBuf {
    persistence.state_dir.join("snapshot.json")
}

fn runtime_log_path(persistence: &PersistenceConfig) -> PathBuf {
    persistence.state_dir.join("log.jsonl")
}

fn append_json_line(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut encoded = serde_json::to_vec(value).context("failed to serialize json line")?;
    encoded.push(b'\n');
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    use std::io::Write as _;
    file.write_all(&encoded)
        .with_context(|| format!("failed to append {}", path.display()))?;
    Ok(())
}

fn write_json_atomically(path: &Path, value: &impl Serialize) -> Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(
        &tmp,
        serde_json::to_vec(value).context("failed to serialize snapshot")?,
    )
    .with_context(|| format!("failed to write {}", tmp.display()))?;
    fs::rename(&tmp, path)
        .with_context(|| format!("failed to replace {}", path.display()))?;
    Ok(())
}

fn parse_json_or_string(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_owned()))
}

fn parse_optional_json(raw: &str) -> Option<Value> {
    if raw.trim().is_empty() {
        None
    } else {
        serde_json::from_str(raw).ok().or_else(|| Some(Value::String(raw.to_owned())))
    }
}

fn internal_status(error: anyhow::Error) -> Status {
    Status::internal(error.to_string())
}

fn apply_compiled_plan(state: &mut WorkflowInstanceState, plan: &CompiledExecutionPlan) {
    state.current_state = Some(plan.final_state.clone());
    state.context = plan.context.clone();
    state.output = plan.output.clone();
    state.artifact_execution = Some(plan.execution_state.clone());
    if !state.status.is_terminal() {
        state.status = WorkflowStatus::Running;
    }
    for emission in &plan.emissions {
        match emission.event {
            WorkflowEvent::WorkflowCompleted { .. } => state.status = WorkflowStatus::Completed,
            WorkflowEvent::WorkflowFailed { .. } => state.status = WorkflowStatus::Failed,
            WorkflowEvent::WorkflowCancelled { .. } => state.status = WorkflowStatus::Cancelled,
            WorkflowEvent::WorkflowTerminated { .. } => state.status = WorkflowStatus::Terminated,
            _ => {}
        }
    }
}
