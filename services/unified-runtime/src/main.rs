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
    BrokerConfig, WorkflowHistoryFilter, build_workflow_consumer, decode_consumed_workflow_event,
    read_workflow_history,
};
use fabrik_config::{GrpcServiceConfig, HttpServiceConfig, PostgresConfig, RedpandaConfig};
use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::{
    ActivityScheduleUpdate, ActivityStartUpdate, ActivityTerminalPayload, ActivityTerminalUpdate,
    WorkflowActivityStatus, WorkflowStore,
};
use fabrik_worker_protocol::activity_worker::{
    Ack, ActivityTask, ActivityTaskCancelledResult, ActivityTaskCompletedResult,
    ActivityTaskFailedResult, ActivityTaskResult, PollActivityTaskRequest,
    PollActivityTaskResponse, PollActivityTasksRequest, PollActivityTasksResponse,
    PollBulkActivityTaskRequest, PollBulkActivityTaskResponse, RecordActivityHeartbeatRequest,
    RecordActivityHeartbeatResponse, ReportActivityTaskCancelledRequest,
    ReportActivityTaskResultsRequest, ReportBulkActivityTaskResultsRequest, activity_task_result,
    activity_worker_api_server::{ActivityWorkerApi, ActivityWorkerApiServer},
};
use fabrik_workflow::{
    ArtifactExecutionState, CompiledExecutionPlan, CompiledWorkflowArtifact, ExecutionTurnContext,
    RetryPolicy, WorkflowInstanceState, WorkflowStatus, parse_timer_ref,
    replay_compiled_history_trace, replay_compiled_history_trace_from_snapshot,
    replay_history_trace_from_snapshot,
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
const DEFAULT_OWNERSHIP_PARTITION_ID: i32 = 1_000_000;
const DEFAULT_OWNERSHIP_LEASE_TTL_SECS: u64 = 30;
const DEFAULT_OWNERSHIP_RENEW_INTERVAL_MS: u64 = 5_000;

#[derive(Clone)]
struct AppState {
    store: WorkflowStore,
    inner: Arc<StdMutex<RuntimeInner>>,
    ownership: Arc<StdMutex<UnifiedOwnershipState>>,
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

#[derive(Debug, Clone)]
struct OwnershipConfig {
    partition_id: i32,
    owner_id: String,
    lease_ttl: Duration,
    renew_interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
struct UnifiedDebugState {
    restored_from_snapshot: bool,
    restore_records_applied: u64,
    workflow_triggers_applied: u64,
    duplicate_triggers_ignored: u64,
    ignored_missing_instance_reports: u64,
    ignored_terminal_instance_reports: u64,
    ignored_missing_activity_reports: u64,
    ignored_stale_attempt_reports: u64,
    ignored_missing_lease_reports: u64,
    ignored_lease_epoch_reports: u64,
    ignored_owner_epoch_reports: u64,
    ignored_worker_mismatch_reports: u64,
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
    owner_epoch: u64,
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
    #[serde(default)]
    lease_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeasedActivity {
    task: QueuedActivity,
    worker_id: String,
    worker_build_id: String,
    lease_expires_at: DateTime<Utc>,
    #[serde(default)]
    owner_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DelayedRetryTask {
    task: QueuedActivity,
    due_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedRuntimeState {
    seq: u64,
    #[serde(default)]
    owner_epoch: u64,
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
    ownership: UnifiedOwnershipState,
    instances: usize,
    ready_tasks: usize,
    leased_tasks: usize,
    delayed_retries: usize,
}

#[derive(Debug, Clone, Serialize)]
struct UnifiedOwnershipState {
    partition_id: i32,
    owner_id: String,
    owner_epoch: u64,
    lease_expires_at: DateTime<Utc>,
    active: bool,
}

impl UnifiedOwnershipState {
    fn inactive(partition_id: i32, owner_id: &str) -> Self {
        Self {
            partition_id,
            owner_id: owner_id.to_owned(),
            owner_epoch: 0,
            lease_expires_at: DateTime::<Utc>::UNIX_EPOCH,
            active: false,
        }
    }
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
    let broker = BrokerConfig::new(
        redpanda.brokers.clone(),
        redpanda.workflow_events_topic.clone(),
        redpanda.workflow_events_partitions,
    );

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
    let ownership = OwnershipConfig {
        partition_id: env::var("UNIFIED_RUNTIME_OWNERSHIP_PARTITION_ID")
            .ok()
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(DEFAULT_OWNERSHIP_PARTITION_ID),
        owner_id: env::var("UNIFIED_RUNTIME_OWNER_ID")
            .unwrap_or_else(|_| format!("unified-runtime:{}", Uuid::now_v7())),
        lease_ttl: Duration::from_secs(
            env::var("UNIFIED_RUNTIME_OWNERSHIP_LEASE_TTL_SECS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(DEFAULT_OWNERSHIP_LEASE_TTL_SECS),
        ),
        renew_interval_ms: env::var("UNIFIED_RUNTIME_OWNERSHIP_RENEW_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_OWNERSHIP_RENEW_INTERVAL_MS),
    };
    fs::create_dir_all(&persistence.state_dir)
        .with_context(|| format!("failed to create {}", persistence.state_dir.display()))?;

    let debug = Arc::new(StdMutex::new(UnifiedDebugState::default()));
    let local_restored = restore_runtime_state(&persistence, &debug)?;
    let shared_restored = restore_runtime_state_from_store(&store, &broker, &debug).await?;
    let inner = reconcile_restored_runtime(local_restored, shared_restored, &debug);
    let state = AppState {
        store,
        inner: Arc::new(StdMutex::new(inner)),
        ownership: Arc::new(StdMutex::new(UnifiedOwnershipState::inactive(
            ownership.partition_id,
            &ownership.owner_id,
        ))),
        notify: Arc::new(Notify::new()),
        persist_notify: Arc::new(Notify::new()),
        persist_lock: Arc::new(Mutex::new(())),
        persistence,
        debug: debug.clone(),
    };
    acquire_initial_ownership(&state, &ownership).await?;
    reconcile_restored_ready_queue(&state).await?;

    let consumer =
        build_workflow_consumer(&broker, "unified-runtime", &broker.all_partition_ids()).await?;
    tokio::spawn(run_trigger_consumer(state.clone(), consumer));
    tokio::spawn(run_ownership_loop(state.clone(), ownership.clone()));
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
                let runtime =
                    debug_state.debug.lock().expect("unified debug lock poisoned").clone();
                let ownership =
                    debug_state.ownership.lock().expect("unified ownership lock poisoned").clone();
                let inner = debug_state.inner.lock().expect("unified runtime lock poisoned");
                Json(UnifiedDebugResponse {
                    runtime,
                    ownership,
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

async fn reconcile_restored_ready_queue(state: &AppState) -> Result<()> {
    let should_reconcile =
        { state.debug.lock().expect("unified debug lock poisoned").restored_from_snapshot };
    if !should_reconcile {
        return Ok(());
    }
    let schedule_updates = {
        let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
        let updates = inner
            .ready
            .values()
            .flat_map(|queue| queue.iter())
            .map(activity_schedule_update)
            .collect::<Vec<_>>();
        if !updates.is_empty() {
            mark_runtime_dirty(&mut inner, "restore_reconcile", true);
        }
        updates
    };
    if !schedule_updates.is_empty() {
        state.store.upsert_activities_scheduled_batch(&schedule_updates).await?;
        state.persist_notify.notify_one();
    }
    Ok(())
}

async fn restore_runtime_state_from_store(
    store: &WorkflowStore,
    broker: &BrokerConfig,
    debug: &Arc<StdMutex<UnifiedDebugState>>,
) -> Result<RuntimeInner> {
    let snapshots = store.list_nonterminal_snapshots().await?;
    if snapshots.is_empty() {
        return Ok(RuntimeInner::default());
    }

    let mut inner = RuntimeInner::default();
    for snapshot in snapshots {
        let definition_version =
            snapshot.state.definition_version.or(snapshot.definition_version).ok_or_else(|| {
                anyhow::anyhow!(
                    "snapshot missing definition version for {}/{}",
                    snapshot.tenant_id,
                    snapshot.instance_id
                )
            })?;
        let artifact = store
            .get_artifact_version(&snapshot.tenant_id, &snapshot.definition_id, definition_version)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "missing artifact {} v{} for snapshot {}/{}",
                    snapshot.definition_id,
                    definition_version,
                    snapshot.tenant_id,
                    snapshot.instance_id
                )
            })?;
        let instance =
            restore_instance_from_store_snapshot_tail(store, broker, &snapshot, &artifact).await?;
        let activities = store
            .list_activities_for_run(&snapshot.tenant_id, &snapshot.instance_id, &snapshot.run_id)
            .await?;
        let mut active_activities = BTreeMap::new();
        for activity in activities {
            if !matches!(
                activity.status,
                WorkflowActivityStatus::Scheduled | WorkflowActivityStatus::Started
            ) {
                continue;
            }
            let task = queued_activity_from_record(&activity);
            let should_replace = active_activities
                .get(&activity.activity_id)
                .is_none_or(|active: &ActiveActivityMeta| active.attempt <= activity.attempt);
            if should_replace {
                active_activities.insert(
                    activity.activity_id.clone(),
                    ActiveActivityMeta {
                        attempt: activity.attempt,
                        task_queue: activity.task_queue.clone(),
                        activity_type: activity.activity_type.clone(),
                        wait_state: activity.state.clone().unwrap_or_default(),
                    },
                );
            }
            inner
                .ready
                .entry(QueueKey {
                    tenant_id: task.tenant_id.clone(),
                    task_queue: task.task_queue.clone(),
                })
                .or_default()
                .push_back(task);
        }

        inner.instances.insert(
            RunKey {
                tenant_id: snapshot.tenant_id.clone(),
                instance_id: snapshot.instance_id.clone(),
                run_id: snapshot.run_id.clone(),
            },
            RuntimeWorkflowState { artifact, instance, active_activities },
        );
    }

    let mut debug = debug.lock().expect("unified debug lock poisoned");
    debug.restored_from_snapshot = true;
    debug.restore_records_applied = inner.instances.len() as u64;
    Ok(inner)
}

fn reconcile_restored_runtime(
    local: Option<RuntimeInner>,
    shared: RuntimeInner,
    debug: &Arc<StdMutex<UnifiedDebugState>>,
) -> RuntimeInner {
    match local {
        None => {
            if !shared.instances.is_empty()
                || !shared.ready.is_empty()
                || !shared.leased.is_empty()
                || !shared.delayed_retries.is_empty()
            {
                let mut debug = debug.lock().expect("unified debug lock poisoned");
                debug.restored_from_snapshot = true;
                debug.restore_records_applied = shared.instances.len() as u64;
            }
            shared
        }
        Some(mut local) => {
            local.next_seq = local.next_seq.max(shared.next_seq);
            for (run_key, runtime) in &shared.instances {
                let should_replace = local
                    .instances
                    .get(run_key)
                    .is_none_or(|current| restored_runtime_is_staler(current, &runtime));
                if !should_replace {
                    continue;
                }
                remove_run_work(&mut local, run_key);
                local.instances.insert(run_key.clone(), runtime.clone());
                add_run_work_from_source(&mut local, &shared, run_key);
            }
            let mut debug = debug.lock().expect("unified debug lock poisoned");
            debug.restored_from_snapshot = !local.instances.is_empty()
                || !local.ready.is_empty()
                || !local.leased.is_empty()
                || !local.delayed_retries.is_empty();
            debug.restore_records_applied = local.instances.len() as u64;
            local
        }
    }
}

fn restored_runtime_is_staler(
    current: &RuntimeWorkflowState,
    candidate: &RuntimeWorkflowState,
) -> bool {
    if candidate.instance.event_count != current.instance.event_count {
        return candidate.instance.event_count > current.instance.event_count;
    }
    if candidate.instance.updated_at != current.instance.updated_at {
        return candidate.instance.updated_at > current.instance.updated_at;
    }
    if candidate.instance.status.is_terminal() != current.instance.status.is_terminal() {
        return candidate.instance.status.is_terminal();
    }
    candidate.active_activities.len() >= current.active_activities.len()
        && candidate.instance.last_event_id != current.instance.last_event_id
}

fn remove_run_work(inner: &mut RuntimeInner, run_key: &RunKey) {
    inner.ready.retain(|_, queue| {
        queue.retain(|task| {
            !(task.tenant_id == run_key.tenant_id
                && task.instance_id == run_key.instance_id
                && task.run_id == run_key.run_id)
        });
        !queue.is_empty()
    });
    inner.leased.retain(|_, leased| {
        !(leased.task.tenant_id == run_key.tenant_id
            && leased.task.instance_id == run_key.instance_id
            && leased.task.run_id == run_key.run_id)
    });
    inner.delayed_retries.retain(|retry| {
        !(retry.task.tenant_id == run_key.tenant_id
            && retry.task.instance_id == run_key.instance_id
            && retry.task.run_id == run_key.run_id)
    });
}

fn add_run_work_from_source(inner: &mut RuntimeInner, source: &RuntimeInner, run_key: &RunKey) {
    for queue in source.ready.values() {
        for task in queue.iter().filter(|task| {
            task.tenant_id == run_key.tenant_id
                && task.instance_id == run_key.instance_id
                && task.run_id == run_key.run_id
        }) {
            inner
                .ready
                .entry(QueueKey {
                    tenant_id: task.tenant_id.clone(),
                    task_queue: task.task_queue.clone(),
                })
                .or_default()
                .push_back(task.clone());
        }
    }
    for leased in source.leased.values().filter(|leased| {
        leased.task.tenant_id == run_key.tenant_id
            && leased.task.instance_id == run_key.instance_id
            && leased.task.run_id == run_key.run_id
    }) {
        inner.leased.insert(attempt_key_from_task(&leased.task), leased.clone());
    }
    for retry in source.delayed_retries.iter().filter(|retry| {
        retry.task.tenant_id == run_key.tenant_id
            && retry.task.instance_id == run_key.instance_id
            && retry.task.run_id == run_key.run_id
    }) {
        inner.delayed_retries.push(retry.clone());
    }
}

async fn restore_instance_from_store_snapshot_tail(
    _store: &WorkflowStore,
    broker: &BrokerConfig,
    snapshot: &fabrik_store::WorkflowStateSnapshot,
    artifact: &CompiledWorkflowArtifact,
) -> Result<WorkflowInstanceState> {
    let history = read_workflow_history(
        broker,
        "unified-runtime-restore",
        &WorkflowHistoryFilter::new(&snapshot.tenant_id, &snapshot.instance_id, &snapshot.run_id),
        Duration::from_millis(500),
        Duration::from_secs(5),
    )
    .await?;
    let tail_start = history
        .iter()
        .position(|event| event.event_id == snapshot.last_event_id)
        .map(|index| index + 1)
        .unwrap_or(history.len());
    let tail = &history[tail_start..];

    if snapshot.state.artifact_execution.is_some() {
        return replay_unified_compiled_snapshot_tail(&snapshot.state, artifact, tail);
    }

    if matches!(snapshot.state.status, WorkflowStatus::Completed | WorkflowStatus::Failed) {
        return Ok(replay_compiled_history_trace_from_snapshot(
            tail,
            &snapshot.state,
            artifact,
            snapshot.event_count,
            snapshot.last_event_id,
            &snapshot.last_event_type,
        )?
        .final_state);
    }

    if tail.is_empty() {
        return Ok(snapshot.state.clone());
    }

    let replayed = replay_compiled_history_trace(&history, artifact)?;
    if replayed.final_state.run_id == snapshot.run_id {
        Ok(replayed.final_state)
    } else {
        Ok(replay_history_trace_from_snapshot(
            tail,
            &snapshot.state,
            snapshot.event_count,
            snapshot.last_event_id,
            &snapshot.last_event_type,
        )?
        .final_state)
    }
}

fn replay_unified_compiled_snapshot_tail(
    snapshot_state: &WorkflowInstanceState,
    artifact: &CompiledWorkflowArtifact,
    history_tail: &[EventEnvelope<WorkflowEvent>],
) -> Result<WorkflowInstanceState> {
    let mut replayed = snapshot_state.clone();
    let mut index = 0usize;
    while index < history_tail.len() {
        if replayed.status.is_terminal() {
            replayed.apply_event(&history_tail[index]);
            index += 1;
            continue;
        }

        let current_state = replayed.current_state.clone().unwrap_or_default();
        let execution_state = replayed.artifact_execution.clone().unwrap_or_default();
        if let Some((plan, applied)) = artifact.try_execute_after_step_terminal_batch_with_turn(
            &current_state,
            &history_tail[index..],
            execution_state,
        )? {
            for event in &history_tail[index..index + applied] {
                replayed.apply_event(event);
            }
            apply_compiled_plan(&mut replayed, &plan);
            index += applied;
            continue;
        }

        let event = &history_tail[index];
        replayed.apply_event(event);
        let execution_state = replayed.artifact_execution.clone().unwrap_or_default();
        let turn_context =
            ExecutionTurnContext { event_id: event.event_id, occurred_at: event.occurred_at };
        match &event.payload {
            WorkflowEvent::ActivityTaskCompleted { activity_id, output, .. } => {
                let plan = artifact.execute_after_step_completion_with_turn(
                    replayed.current_state.as_deref().unwrap_or_default(),
                    activity_id,
                    output,
                    execution_state,
                    turn_context,
                )?;
                apply_compiled_plan(&mut replayed, &plan);
            }
            WorkflowEvent::ActivityTaskFailed { activity_id, error, .. } => {
                let plan = artifact.execute_after_step_failure_with_turn(
                    replayed.current_state.as_deref().unwrap_or_default(),
                    activity_id,
                    error,
                    execution_state,
                    turn_context,
                )?;
                apply_compiled_plan(&mut replayed, &plan);
            }
            _ => {}
        }
        index += 1;
    }
    Ok(replayed)
}

fn ownership_is_active(ownership: &UnifiedOwnershipState, now: DateTime<Utc>) -> bool {
    ownership.active && ownership.lease_expires_at > now && ownership.owner_epoch > 0
}

fn current_owner_epoch(state: &AppState) -> Option<u64> {
    let ownership = state.ownership.lock().expect("unified ownership lock poisoned");
    ownership_is_active(&ownership, Utc::now()).then_some(ownership.owner_epoch)
}

fn apply_ownership_record(
    state: &AppState,
    partition_id: i32,
    record_owner_id: &str,
    owner_epoch: u64,
    lease_expires_at: DateTime<Utc>,
) {
    {
        let mut ownership = state.ownership.lock().expect("unified ownership lock poisoned");
        ownership.partition_id = partition_id;
        ownership.owner_id = record_owner_id.to_owned();
        ownership.owner_epoch = owner_epoch;
        ownership.lease_expires_at = lease_expires_at;
        ownership.active = true;
    }
    let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
    inner.owner_epoch = owner_epoch;
    mark_runtime_dirty(&mut inner, "ownership_epoch", true);
}

async fn acquire_initial_ownership(state: &AppState, config: &OwnershipConfig) -> Result<()> {
    loop {
        if let Some(record) = state
            .store
            .claim_partition_ownership(config.partition_id, &config.owner_id, config.lease_ttl)
            .await?
        {
            apply_ownership_record(
                state,
                record.partition_id,
                &record.owner_id,
                record.owner_epoch,
                record.lease_expires_at,
            );
            state.persist_notify.notify_one();
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(config.renew_interval_ms.max(1))).await;
    }
}

async fn run_ownership_loop(state: AppState, config: OwnershipConfig) {
    let interval = Duration::from_millis(config.renew_interval_ms.max(1));
    loop {
        let snapshot = { state.ownership.lock().expect("unified ownership lock poisoned").clone() };
        if ownership_is_active(&snapshot, Utc::now()) {
            match state
                .store
                .renew_partition_ownership(
                    snapshot.partition_id,
                    &snapshot.owner_id,
                    snapshot.owner_epoch,
                    config.lease_ttl,
                )
                .await
            {
                Ok(Some(record)) => {
                    apply_ownership_record(
                        &state,
                        record.partition_id,
                        &record.owner_id,
                        record.owner_epoch,
                        record.lease_expires_at,
                    );
                    state.persist_notify.notify_one();
                }
                Ok(None) => {
                    error!(
                        partition_id = snapshot.partition_id,
                        owner_id = %snapshot.owner_id,
                        owner_epoch = snapshot.owner_epoch,
                        "unified-runtime lost partition ownership"
                    );
                    std::process::exit(1);
                }
                Err(error) => warn!(error = %error, "unified-runtime failed to renew ownership"),
            }
        } else {
            match state
                .store
                .claim_partition_ownership(config.partition_id, &config.owner_id, config.lease_ttl)
                .await
            {
                Ok(Some(record)) => {
                    apply_ownership_record(
                        &state,
                        record.partition_id,
                        &record.owner_id,
                        record.owner_epoch,
                        record.lease_expires_at,
                    );
                    state.persist_notify.notify_one();
                }
                Ok(None) => {}
                Err(error) => warn!(error = %error, "unified-runtime failed to claim ownership"),
            }
        }
        tokio::time::sleep(interval).await;
    }
}

async fn run_trigger_consumer(
    state: AppState,
    mut consumer: fabrik_broker::WorkflowConsumerStream,
) {
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
    let workflow_task_queue =
        event.metadata.get("workflow_task_queue").cloned().unwrap_or_else(|| "default".to_owned());

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
    let schedule_actions =
        queued.iter().cloned().map(PreparedDbAction::Schedule).collect::<Vec<_>>();
    apply_db_actions(
        state,
        vec![PreparedDbAction::UpsertInstance(instance.clone())],
        schedule_actions,
    )
    .await?;
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
                released
            }
        };
        if released.is_empty() {
            continue;
        }
        let actions = released.iter().cloned().map(PreparedDbAction::Schedule).collect::<Vec<_>>();
        let released_count = actions.len() as u64;
        if let Err(error) = apply_db_actions(&state, Vec::new(), actions).await {
            error!(error = %error, "unified-runtime failed to release retries");
            continue;
        }
        {
            let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
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
                for (key, _) in &expired {
                    inner.leased.remove(key);
                }
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
        {
            let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
            for (_, task) in &requeued {
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
        let Some((reason, force_snapshot, persisted)) = capture_dirty_persisted_state(&state)
        else {
            continue;
        };
        if let Err(error) = persist_runtime_state(&state, &reason, persisted, force_snapshot).await
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
        if current_owner_epoch(&self.state).is_none() {
            return Err(Status::unavailable("unified-runtime ownership inactive"));
        }
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
                lease_epoch: request.lease_epoch,
                owner_epoch: request.owner_epoch,
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
                lease_epoch: request.lease_epoch,
                owner_epoch: request.owner_epoch,
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
                lease_epoch: request.lease_epoch,
                owner_epoch: request.owner_epoch,
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
        if current_owner_epoch(&self.state).is_none() {
            return Err(Status::unavailable("unified-runtime ownership inactive"));
        }
        let request = request.into_inner();
        {
            let mut debug = self.state.debug.lock().expect("unified debug lock poisoned");
            debug.report_rpcs_received = debug.report_rpcs_received.saturating_add(1);
            debug.reports_received =
                debug.reports_received.saturating_add(request.results.len() as u64);
        }
        let now = Utc::now();
        let actions = prepare_result_application(&self.state, request.results, now)
            .map_err(internal_status)?;
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
        debug.instance_terminals =
            debug.instance_terminals.saturating_add(actions.instance_terminals);
        debug.ignored_missing_instance_reports = debug
            .ignored_missing_instance_reports
            .saturating_add(actions.ignored_missing_instances);
        debug.ignored_terminal_instance_reports = debug
            .ignored_terminal_instance_reports
            .saturating_add(actions.ignored_terminal_instances);
        debug.ignored_missing_activity_reports = debug
            .ignored_missing_activity_reports
            .saturating_add(actions.ignored_missing_activities);
        debug.ignored_stale_attempt_reports =
            debug.ignored_stale_attempt_reports.saturating_add(actions.ignored_stale_attempts);
        debug.ignored_missing_lease_reports =
            debug.ignored_missing_lease_reports.saturating_add(actions.ignored_missing_leases);
        debug.ignored_lease_epoch_reports = debug
            .ignored_lease_epoch_reports
            .saturating_add(actions.ignored_lease_epoch_mismatches);
        debug.ignored_owner_epoch_reports = debug
            .ignored_owner_epoch_reports
            .saturating_add(actions.ignored_owner_epoch_mismatches);
        debug.ignored_worker_mismatch_reports =
            debug.ignored_worker_mismatch_reports.saturating_add(actions.ignored_worker_mismatches);
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
    ignored_missing_instances: u64,
    ignored_terminal_instances: u64,
    ignored_missing_activities: u64,
    ignored_stale_attempts: u64,
    ignored_missing_leases: u64,
    ignored_lease_epoch_mismatches: u64,
    ignored_owner_epoch_mismatches: u64,
    ignored_worker_mismatches: u64,
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
    let mut ignored_missing_instances = 0_u64;
    let mut ignored_terminal_instances = 0_u64;
    let mut ignored_missing_activities = 0_u64;
    let mut ignored_stale_attempts = 0_u64;
    let mut ignored_missing_leases = 0_u64;
    let mut ignored_lease_epoch_mismatches = 0_u64;
    let mut ignored_owner_epoch_mismatches = 0_u64;
    let mut ignored_worker_mismatches = 0_u64;
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
        let Some(runtime_view) = inner.instances.get(&run_key) else {
            ignored_missing_instances = ignored_missing_instances.saturating_add(1);
            continue;
        };
        if runtime_view.instance.status.is_terminal() {
            ignored_terminal_instances = ignored_terminal_instances.saturating_add(1);
            continue;
        }
        let Some(active) = runtime_view.active_activities.get(&result.activity_id).cloned() else {
            ignored_missing_activities = ignored_missing_activities.saturating_add(1);
            continue;
        };
        if !result_matches_active_attempt(
            &runtime_view.active_activities,
            &runtime_view.instance.status,
            &result,
        ) {
            ignored_stale_attempts = ignored_stale_attempts.saturating_add(1);
            continue;
        }
        let Some(leased) = inner.leased.get(&attempt_key) else {
            ignored_missing_leases = ignored_missing_leases.saturating_add(1);
            continue;
        };
        match result_matches_current_lease(leased, &result) {
            LeaseResultMatch::Matched => {}
            LeaseResultMatch::LeaseEpochMismatch => {
                ignored_lease_epoch_mismatches = ignored_lease_epoch_mismatches.saturating_add(1);
                continue;
            }
            LeaseResultMatch::OwnerEpochMismatch => {
                ignored_owner_epoch_mismatches = ignored_owner_epoch_mismatches.saturating_add(1);
                continue;
            }
            LeaseResultMatch::WorkerMismatch => {
                ignored_worker_mismatches = ignored_worker_mismatches.saturating_add(1);
                continue;
            }
        }
        inner.leased.remove(&attempt_key);
        let runtime =
            inner.instances.get_mut(&run_key).expect("runtime exists after immutable validation");

        let mut delayed_retry = None;
        let identity = (
            runtime.instance.definition_id.clone(),
            runtime.instance.definition_version.unwrap_or(runtime.artifact.definition_version),
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
                if let Some(retry) = runtime
                    .artifact
                    .step_retry(&active.wait_state, runtime_execution_state(runtime))?
                {
                    if result.attempt < retry.max_attempts {
                        if let Some(retry_task) = build_retry_task(
                            runtime,
                            &result.activity_id,
                            result.attempt + 1,
                            retry,
                            now,
                        )? {
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
        grouped.entry(run_key).or_default().push(synthetic_runtime_event(
            payload,
            &result,
            &identity.0,
            identity.1,
            &identity.2,
            now,
        ));
    }

    for (run_key, events) in grouped {
        let Some(runtime) = inner.instances.get_mut(&run_key) else {
            continue;
        };
        let Some(wait_state) = runtime.instance.current_state.clone() else {
            continue;
        };
        let execution_state = runtime.instance.artifact_execution.clone().unwrap_or_default();
        if let Some((plan, _)) = runtime.artifact.try_execute_after_step_terminal_batch_with_turn(
            &wait_state,
            &events,
            execution_state,
        )? {
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
        ignored_missing_instances,
        ignored_terminal_instances,
        ignored_missing_activities,
        ignored_stale_attempts,
        ignored_missing_leases,
        ignored_lease_epoch_mismatches,
        ignored_owner_epoch_mismatches,
        ignored_worker_mismatches,
    })
}

async fn apply_db_actions(
    state: &AppState,
    general: Vec<PreparedDbAction>,
    schedules: Vec<PreparedDbAction>,
) -> Result<()> {
    let mut terminal_updates = Vec::new();
    let mut other_general = Vec::new();
    let mut schedule_updates = Vec::new();
    for action in general {
        match action {
            PreparedDbAction::Terminal(update) => terminal_updates.push(update),
            other => other_general.push(other),
        }
    }
    for action in schedules {
        match action {
            PreparedDbAction::Schedule(task) => {
                schedule_updates.push(activity_schedule_update(&task))
            }
            other => other_general.push(other),
        }
    }
    if !terminal_updates.is_empty() {
        state.store.apply_activity_terminal_batch(&terminal_updates).await?;
    }
    if !schedule_updates.is_empty() {
        state.store.upsert_activities_scheduled_batch(&schedule_updates).await?;
    }
    for action in other_general {
        match action {
            PreparedDbAction::Terminal(_) => unreachable!("terminal updates are batched"),
            PreparedDbAction::Schedule(_) => unreachable!("schedule updates are batched"),
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

fn activity_schedule_update(task: &QueuedActivity) -> ActivityScheduleUpdate {
    ActivityScheduleUpdate {
        tenant_id: task.tenant_id.clone(),
        instance_id: task.instance_id.clone(),
        run_id: task.run_id.clone(),
        definition_id: task.definition_id.clone(),
        definition_version: Some(task.definition_version),
        artifact_hash: Some(task.artifact_hash.clone()),
        activity_id: task.activity_id.clone(),
        attempt: task.attempt,
        activity_type: task.activity_type.clone(),
        task_queue: task.task_queue.clone(),
        state: Some(task.state.clone()),
        input: task.input.clone(),
        config: task.config.clone(),
        schedule_to_start_timeout_ms: None,
        start_to_close_timeout_ms: None,
        heartbeat_timeout_ms: None,
        event_id: Uuid::now_v7(),
        event_type: "UnifiedActivityScheduled".to_owned(),
        occurred_at: task.scheduled_at,
    }
}

fn queued_activity_from_record(record: &fabrik_store::WorkflowActivityRecord) -> QueuedActivity {
    QueuedActivity {
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
        input: record.input.clone(),
        config: record.config.clone(),
        state: record.state.clone().unwrap_or_default(),
        scheduled_at: record.scheduled_at,
        lease_epoch: 0,
    }
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
        let Some(mut task) = queue.pop_front() else {
            break;
        };
        task.lease_epoch = task.lease_epoch.saturating_add(1);
        let leased_task = LeasedActivity {
            task: task.clone(),
            worker_id: worker_id.to_owned(),
            worker_build_id: worker_build_id.to_owned(),
            lease_expires_at,
            owner_epoch: inner.owner_epoch,
        };
        inner.leased.insert(attempt_key_from_task(&task), leased_task.clone());
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
        lease_epoch: leased.task.lease_epoch,
        owner_epoch: leased.owner_epoch,
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
                state: state
                    .clone()
                    .or_else(|| emission.state.clone())
                    .unwrap_or_else(|| instance.current_state.clone().unwrap_or_default()),
                scheduled_at,
                lease_epoch: 0,
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
    let (activity_type, config, input) =
        runtime.artifact.step_details(activity_id, &execution_state)?;
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
            lease_epoch: 0,
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

fn result_matches_active_attempt(
    active_activities: &BTreeMap<String, ActiveActivityMeta>,
    status: &WorkflowStatus,
    result: &ActivityTaskResult,
) -> bool {
    if status.is_terminal() {
        return false;
    }
    active_activities
        .get(&result.activity_id)
        .is_some_and(|active| active.attempt == result.attempt)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseResultMatch {
    Matched,
    LeaseEpochMismatch,
    OwnerEpochMismatch,
    WorkerMismatch,
}

fn result_matches_current_lease(
    leased: &LeasedActivity,
    result: &ActivityTaskResult,
) -> LeaseResultMatch {
    if leased.owner_epoch != result.owner_epoch {
        return LeaseResultMatch::OwnerEpochMismatch;
    }
    if leased.task.lease_epoch != result.lease_epoch {
        return LeaseResultMatch::LeaseEpochMismatch;
    }
    if leased.worker_id != result.worker_id || leased.worker_build_id != result.worker_build_id {
        return LeaseResultMatch::WorkerMismatch;
    }
    LeaseResultMatch::Matched
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
        owner_epoch: inner.owner_epoch,
        instances: inner.instances.values().cloned().collect(),
        ready: inner.ready.values().flat_map(|queue| queue.iter().cloned()).collect(),
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
        for runtime in &persisted.instances {
            state.store.put_snapshot(&runtime.instance).await?;
        }
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
    Ok(Some(runtime_inner_from_persisted(restored, debug)))
}

fn runtime_inner_from_persisted(
    restored: PersistedRuntimeState,
    debug: &Arc<StdMutex<UnifiedDebugState>>,
) -> RuntimeInner {
    let mut inner = RuntimeInner {
        next_seq: restored.seq,
        owner_epoch: restored.owner_epoch,
        ..RuntimeInner::default()
    };
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

    let mut seen_attempts = HashSet::new();
    for task in restored.ready {
        seen_attempts.insert(attempt_key_from_task(&task));
        inner
            .ready
            .entry(QueueKey {
                tenant_id: task.tenant_id.clone(),
                task_queue: task.task_queue.clone(),
            })
            .or_default()
            .push_back(task);
    }

    // Owner crash invalidates in-flight leases. Requeue them immediately on restore.
    for leased in restored.leased {
        let key = attempt_key_from_task(&leased.task);
        if seen_attempts.insert(key) {
            inner
                .ready
                .entry(QueueKey {
                    tenant_id: leased.task.tenant_id.clone(),
                    task_queue: leased.task.task_queue.clone(),
                })
                .or_default()
                .push_back(leased.task);
        }
    }

    let now = Utc::now();
    for retry in restored.delayed_retries {
        let key = attempt_key_from_task(&retry.task);
        if retry.due_at <= now {
            if seen_attempts.insert(key) {
                inner
                    .ready
                    .entry(QueueKey {
                        tenant_id: retry.task.tenant_id.clone(),
                        task_queue: retry.task.task_queue.clone(),
                    })
                    .or_default()
                    .push_back(retry.task);
            }
        } else {
            inner.delayed_retries.push(retry);
        }
    }

    let mut debug = debug.lock().expect("unified debug lock poisoned");
    debug.restored_from_snapshot = true;
    debug.restore_records_applied = inner.instances.len() as u64;
    inner
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
    file.write_all(&encoded).with_context(|| format!("failed to append {}", path.display()))?;
    Ok(())
}

fn write_json_atomically(path: &Path, value: &impl Serialize) -> Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, serde_json::to_vec(value).context("failed to serialize snapshot")?)
        .with_context(|| format!("failed to write {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| format!("failed to replace {}", path.display()))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use chrono::Duration as ChronoDuration;
    use fabrik_broker::WorkflowPublisher;
    use fabrik_events::WorkflowIdentity;
    use fabrik_workflow::{ArtifactEntrypoint, CompiledStateNode, CompiledWorkflow, Expression};
    use serde_json::json;
    use std::{
        collections::BTreeMap,
        net::TcpListener,
        process::Command,
        time::{Duration as StdDuration, Instant},
    };
    use tokio::time::sleep;

    fn queued_activity(
        activity_id: &str,
        attempt: u32,
        scheduled_at: DateTime<Utc>,
    ) -> QueuedActivity {
        QueuedActivity {
            tenant_id: "tenant".to_owned(),
            definition_id: "definition".to_owned(),
            definition_version: 1,
            artifact_hash: "artifact".to_owned(),
            instance_id: "instance".to_owned(),
            run_id: "run".to_owned(),
            activity_id: activity_id.to_owned(),
            activity_type: "benchmark.echo".to_owned(),
            task_queue: "default".to_owned(),
            attempt,
            input: Value::Null,
            config: None,
            state: "join".to_owned(),
            scheduled_at,
            lease_epoch: 0,
        }
    }

    fn queued_activity_for_run(
        run_id: &str,
        activity_id: &str,
        attempt: u32,
        scheduled_at: DateTime<Utc>,
    ) -> QueuedActivity {
        QueuedActivity {
            instance_id: format!("instance-{run_id}"),
            run_id: run_id.to_owned(),
            ..queued_activity(activity_id, attempt, scheduled_at)
        }
    }

    fn persistence_config_for(path: PathBuf) -> PersistenceConfig {
        PersistenceConfig { state_dir: path, snapshot_every: 16, flush_interval_ms: 200 }
    }

    fn workflow_instance_for_run(
        run_id: &str,
        status: WorkflowStatus,
        event_count: i64,
        updated_at: DateTime<Utc>,
    ) -> WorkflowInstanceState {
        WorkflowInstanceState {
            tenant_id: "tenant".to_owned(),
            instance_id: format!("instance-{run_id}"),
            run_id: run_id.to_owned(),
            definition_id: "definition".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact".to_owned()),
            workflow_task_queue: "default".to_owned(),
            sticky_workflow_build_id: None,
            sticky_workflow_poller_id: None,
            current_state: Some("join".to_owned()),
            context: None,
            artifact_execution: None,
            status,
            input: None,
            output: None,
            event_count,
            last_event_id: Uuid::now_v7(),
            last_event_type: "ActivityTaskCompleted".to_owned(),
            updated_at,
        }
    }

    fn runtime_workflow_for_run(
        run_id: &str,
        status: WorkflowStatus,
        event_count: i64,
        updated_at: DateTime<Utc>,
    ) -> RuntimeWorkflowState {
        RuntimeWorkflowState {
            artifact: test_artifact("default"),
            instance: workflow_instance_for_run(run_id, status, event_count, updated_at),
            active_activities: BTreeMap::from([(
                "activity-1".to_owned(),
                ActiveActivityMeta {
                    attempt: 1,
                    task_queue: "default".to_owned(),
                    activity_type: "benchmark.echo".to_owned(),
                    wait_state: "join".to_owned(),
                },
            )]),
        }
    }

    fn run_key_for(run_id: &str) -> RunKey {
        RunKey {
            tenant_id: "tenant".to_owned(),
            instance_id: format!("instance-{run_id}"),
            run_id: run_id.to_owned(),
        }
    }

    fn test_artifact(task_queue: &str) -> CompiledWorkflowArtifact {
        let mut states = BTreeMap::new();
        states.insert(
            "dispatch".to_owned(),
            CompiledStateNode::FanOut {
                activity_type: "benchmark.echo".to_owned(),
                items: Expression::Member {
                    object: Box::new(Expression::Identifier { name: "input".to_owned() }),
                    property: "items".to_owned(),
                },
                next: "join".to_owned(),
                handle_var: "fanout".to_owned(),
                task_queue: Some(Expression::Literal {
                    value: Value::String(task_queue.to_owned()),
                }),
                reducer: Some("all_settled".to_owned()),
                retry: None,
                config: None,
            },
        );
        states.insert(
            "join".to_owned(),
            CompiledStateNode::WaitForAllActivities {
                fanout_ref_var: "fanout".to_owned(),
                next: "done".to_owned(),
                output_var: Some("results".to_owned()),
                on_error: None,
            },
        );
        states.insert(
            "done".to_owned(),
            CompiledStateNode::Succeed {
                output: Some(Expression::Identifier { name: "results".to_owned() }),
            },
        );

        CompiledWorkflowArtifact::new(
            "unified-handoff-demo".to_owned(),
            1,
            "unified-runtime-test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            CompiledWorkflow { initial_state: "dispatch".to_owned(), states },
        )
    }

    fn test_event(
        identity: &WorkflowIdentity,
        payload: WorkflowEvent,
        occurred_at: DateTime<Utc>,
    ) -> EventEnvelope<WorkflowEvent> {
        let mut event = EventEnvelope::new(payload.event_type(), identity.clone(), payload);
        event.occurred_at = occurred_at;
        event.metadata.insert("workflow_task_queue".to_owned(), "default".to_owned());
        event
    }

    struct TestPostgres {
        container_name: String,
        database_url: String,
    }

    impl TestPostgres {
        fn start() -> Result<Option<Self>> {
            if !docker_available() {
                eprintln!("skipping unified integration test because docker is unavailable");
                return Ok(None);
            }

            let container_name = format!("fabrik-unified-test-pg-{}", Uuid::now_v7());
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
                eprintln!("skipping unified integration test because docker is unavailable");
                return Ok(None);
            }

            let kafka_port = choose_free_port().context("failed to allocate kafka host port")?;
            let container_name = format!("fabrik-unified-test-rp-{}", Uuid::now_v7());
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
                match WorkflowPublisher::new(&self.broker, "unified-runtime-test").await {
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

    fn choose_free_port() -> Result<u16> {
        let listener = TcpListener::bind("127.0.0.1:0").context("failed to bind ephemeral port")?;
        let port = listener.local_addr().context("failed to read ephemeral socket address")?.port();
        drop(listener);
        Ok(port)
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

    #[test]
    fn result_matching_rejects_stale_duplicate_and_terminal_reports() {
        let mut active = BTreeMap::new();
        active.insert(
            "activity-1".to_owned(),
            ActiveActivityMeta {
                attempt: 2,
                task_queue: "default".to_owned(),
                activity_type: "benchmark.echo".to_owned(),
                wait_state: "join".to_owned(),
            },
        );

        let matching = ActivityTaskResult {
            tenant_id: "tenant".to_owned(),
            instance_id: "instance".to_owned(),
            run_id: "run".to_owned(),
            activity_id: "activity-1".to_owned(),
            attempt: 2,
            worker_id: "worker".to_owned(),
            worker_build_id: "build".to_owned(),
            lease_epoch: 3,
            owner_epoch: 7,
            result: None,
        };
        let stale = ActivityTaskResult { attempt: 1, ..matching.clone() };
        let duplicate_unknown =
            ActivityTaskResult { activity_id: "missing".to_owned(), ..matching.clone() };

        assert!(result_matches_active_attempt(&active, &WorkflowStatus::Running, &matching));
        assert!(!result_matches_active_attempt(&active, &WorkflowStatus::Running, &stale));
        assert!(!result_matches_active_attempt(&active, &WorkflowStatus::Completed, &matching));
        assert!(!result_matches_active_attempt(
            &active,
            &WorkflowStatus::Running,
            &duplicate_unknown
        ));

        let leased = LeasedActivity {
            task: QueuedActivity { lease_epoch: 3, ..queued_activity("activity-1", 2, Utc::now()) },
            worker_id: "worker".to_owned(),
            worker_build_id: "build".to_owned(),
            lease_expires_at: Utc::now() + ChronoDuration::seconds(30),
            owner_epoch: 7,
        };
        assert_eq!(result_matches_current_lease(&leased, &matching), LeaseResultMatch::Matched);
        assert_eq!(
            result_matches_current_lease(
                &leased,
                &ActivityTaskResult { lease_epoch: 2, ..matching.clone() }
            ),
            LeaseResultMatch::LeaseEpochMismatch
        );
        assert_eq!(
            result_matches_current_lease(
                &leased,
                &ActivityTaskResult { owner_epoch: 6, ..matching.clone() }
            ),
            LeaseResultMatch::OwnerEpochMismatch
        );
        assert_eq!(
            result_matches_current_lease(
                &leased,
                &ActivityTaskResult { worker_id: "other-worker".to_owned(), ..matching.clone() }
            ),
            LeaseResultMatch::WorkerMismatch
        );
    }

    #[test]
    fn restore_requeues_leased_and_due_retry_tasks() {
        let now = Utc::now();
        let ready_task = queued_activity("ready", 1, now);
        let leased_task = queued_activity("leased", 1, now);
        let due_retry_task = queued_activity("retry-due", 2, now);
        let future_retry_task =
            queued_activity("retry-future", 2, now + ChronoDuration::seconds(30));
        let debug = Arc::new(StdMutex::new(UnifiedDebugState::default()));

        let restored = PersistedRuntimeState {
            seq: 7,
            owner_epoch: 9,
            instances: Vec::new(),
            ready: vec![ready_task.clone()],
            leased: vec![LeasedActivity {
                task: leased_task.clone(),
                worker_id: "worker".to_owned(),
                worker_build_id: "build".to_owned(),
                lease_expires_at: now + ChronoDuration::seconds(15),
                owner_epoch: 9,
            }],
            delayed_retries: vec![
                DelayedRetryTask {
                    task: due_retry_task.clone(),
                    due_at: now - ChronoDuration::seconds(1),
                },
                DelayedRetryTask {
                    task: future_retry_task.clone(),
                    due_at: now + ChronoDuration::seconds(30),
                },
            ],
        };

        let inner = runtime_inner_from_persisted(restored, &debug);
        let ready_tasks = inner.ready.values().flat_map(|queue| queue.iter()).collect::<Vec<_>>();

        assert_eq!(inner.owner_epoch, 9);
        assert_eq!(inner.leased.len(), 0);
        assert_eq!(inner.delayed_retries.len(), 1);
        assert_eq!(ready_tasks.len(), 3);
        assert!(ready_tasks.iter().any(|task| task.activity_id == ready_task.activity_id));
        assert!(ready_tasks.iter().any(|task| task.activity_id == leased_task.activity_id));
        assert!(ready_tasks.iter().any(|task| task.activity_id == due_retry_task.activity_id));
        assert_eq!(inner.delayed_retries[0].task.activity_id, future_retry_task.activity_id);
    }

    #[test]
    fn restore_prefers_newer_log_state_over_snapshot() {
        let temp_dir =
            std::env::temp_dir().join(format!("unified-runtime-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let persistence = persistence_config_for(temp_dir.clone());
        let debug = Arc::new(StdMutex::new(UnifiedDebugState::default()));

        let snapshot_state = PersistedRuntimeState {
            seq: 1,
            owner_epoch: 3,
            instances: Vec::new(),
            ready: vec![queued_activity("snapshot-task", 1, Utc::now())],
            leased: Vec::new(),
            delayed_retries: Vec::new(),
        };
        write_json_atomically(&runtime_snapshot_path(&persistence), &snapshot_state)
            .expect("write snapshot");

        let log_state = PersistedRuntimeState {
            seq: 2,
            owner_epoch: 5,
            instances: Vec::new(),
            ready: vec![queued_activity("log-task", 1, Utc::now())],
            leased: Vec::new(),
            delayed_retries: Vec::new(),
        };
        append_json_line(
            &runtime_log_path(&persistence),
            &PersistedLogRecord { seq: 2, reason: "report_batch".to_owned(), state: log_state },
        )
        .expect("append log");

        let restored = restore_runtime_state(&persistence, &debug)
            .expect("restore state")
            .expect("restored runtime");
        let ready_tasks =
            restored.ready.values().flat_map(|queue| queue.iter()).collect::<Vec<_>>();

        assert_eq!(restored.next_seq, 2);
        assert_eq!(restored.owner_epoch, 5);
        assert_eq!(ready_tasks.len(), 1);
        assert_eq!(ready_tasks[0].activity_id, "log-task");

        fs::remove_dir_all(temp_dir).expect("cleanup temp dir");
    }

    #[test]
    fn restore_ready_queue_can_be_reconciled_back_to_schedule_updates() {
        let now = Utc::now();
        let debug = Arc::new(StdMutex::new(UnifiedDebugState::default()));
        let restored = PersistedRuntimeState {
            seq: 3,
            owner_epoch: 4,
            instances: Vec::new(),
            ready: vec![queued_activity("ready", 1, now)],
            leased: vec![LeasedActivity {
                task: queued_activity("leased", 1, now),
                worker_id: "worker".to_owned(),
                worker_build_id: "build".to_owned(),
                lease_expires_at: now + ChronoDuration::seconds(10),
                owner_epoch: 4,
            }],
            delayed_retries: vec![DelayedRetryTask {
                task: queued_activity("retry-due", 2, now),
                due_at: now - ChronoDuration::seconds(1),
            }],
        };

        let inner = runtime_inner_from_persisted(restored, &debug);
        let mut updates = inner
            .ready
            .values()
            .flat_map(|queue| queue.iter())
            .map(activity_schedule_update)
            .collect::<Vec<_>>();
        updates.sort_by(|left, right| left.activity_id.cmp(&right.activity_id));

        assert_eq!(updates.len(), 3);
        assert_eq!(updates[0].activity_id, "leased");
        assert_eq!(updates[1].activity_id, "ready");
        assert_eq!(updates[2].activity_id, "retry-due");
    }

    #[test]
    fn restore_reconciliation_prefers_fresher_shared_run_and_removes_stale_local_work() {
        let now = Utc::now();
        let debug = Arc::new(StdMutex::new(UnifiedDebugState::default()));
        let run_key = run_key_for("shared");
        let local_task = queued_activity_for_run("shared", "local-ready", 1, now);
        let local_retry_task = queued_activity_for_run("shared", "local-retry", 2, now);
        let shared_task = queued_activity_for_run("shared", "shared-ready", 3, now);
        let mut local = RuntimeInner { next_seq: 4, owner_epoch: 1, ..RuntimeInner::default() };
        local.instances.insert(
            run_key.clone(),
            runtime_workflow_for_run(
                "shared",
                WorkflowStatus::Running,
                3,
                now - ChronoDuration::seconds(10),
            ),
        );
        local
            .ready
            .entry(QueueKey { tenant_id: "tenant".to_owned(), task_queue: "default".to_owned() })
            .or_default()
            .push_back(local_task.clone());
        local.leased.insert(
            attempt_key_from_task(&local_task),
            LeasedActivity {
                task: local_task.clone(),
                worker_id: "worker".to_owned(),
                worker_build_id: "build".to_owned(),
                lease_expires_at: now + ChronoDuration::seconds(10),
                owner_epoch: 1,
            },
        );
        local.delayed_retries.push(DelayedRetryTask {
            task: local_retry_task.clone(),
            due_at: now + ChronoDuration::seconds(30),
        });

        let mut shared = RuntimeInner { next_seq: 7, ..RuntimeInner::default() };
        let mut shared_runtime = runtime_workflow_for_run(
            "shared",
            WorkflowStatus::Completed,
            9,
            now + ChronoDuration::seconds(5),
        );
        shared_runtime.active_activities.clear();
        shared.instances.insert(run_key.clone(), shared_runtime);
        shared
            .ready
            .entry(QueueKey { tenant_id: "tenant".to_owned(), task_queue: "default".to_owned() })
            .or_default()
            .push_back(shared_task.clone());

        let merged = reconcile_restored_runtime(Some(local), shared, &debug);
        let merged_runtime = merged.instances.get(&run_key).expect("merged runtime");
        let ready_tasks = merged
            .ready
            .values()
            .flat_map(|queue| queue.iter())
            .filter(|task| task.run_id == "shared")
            .collect::<Vec<_>>();

        assert_eq!(merged.next_seq, 7);
        assert_eq!(merged_runtime.instance.event_count, 9);
        assert_eq!(merged_runtime.instance.status, WorkflowStatus::Completed);
        assert_eq!(ready_tasks.len(), 1);
        assert_eq!(ready_tasks[0].activity_id, shared_task.activity_id);
        assert!(merged.leased.is_empty());
        assert!(merged.delayed_retries.is_empty());
    }

    #[test]
    fn restore_reconciliation_keeps_fresher_local_run_over_older_shared_state() {
        let now = Utc::now();
        let debug = Arc::new(StdMutex::new(UnifiedDebugState::default()));
        let run_key = run_key_for("local");
        let local_task = queued_activity_for_run("local", "local-ready", 1, now);
        let shared_task = queued_activity_for_run("local", "shared-ready", 1, now);
        let mut local = RuntimeInner { next_seq: 9, owner_epoch: 2, ..RuntimeInner::default() };
        let mut local_runtime = runtime_workflow_for_run(
            "local",
            WorkflowStatus::Completed,
            12,
            now + ChronoDuration::seconds(5),
        );
        local_runtime.active_activities.clear();
        local.instances.insert(run_key.clone(), local_runtime);
        local
            .ready
            .entry(QueueKey { tenant_id: "tenant".to_owned(), task_queue: "default".to_owned() })
            .or_default()
            .push_back(local_task.clone());

        let mut shared = RuntimeInner { next_seq: 4, ..RuntimeInner::default() };
        shared.instances.insert(
            run_key.clone(),
            runtime_workflow_for_run(
                "local",
                WorkflowStatus::Running,
                7,
                now - ChronoDuration::seconds(20),
            ),
        );
        shared
            .ready
            .entry(QueueKey { tenant_id: "tenant".to_owned(), task_queue: "default".to_owned() })
            .or_default()
            .push_back(shared_task);

        let merged = reconcile_restored_runtime(Some(local), shared, &debug);
        let merged_runtime = merged.instances.get(&run_key).expect("merged runtime");
        let ready_tasks = merged
            .ready
            .values()
            .flat_map(|queue| queue.iter())
            .filter(|task| task.run_id == "local")
            .collect::<Vec<_>>();

        assert_eq!(merged.next_seq, 9);
        assert_eq!(merged_runtime.instance.event_count, 12);
        assert_eq!(merged_runtime.instance.status, WorkflowStatus::Completed);
        assert_eq!(ready_tasks.len(), 1);
        assert_eq!(ready_tasks[0].activity_id, local_task.activity_id);
    }

    #[test]
    fn leasing_increments_task_epoch_and_stamps_owner_epoch() {
        let now = Utc::now();
        let queue_key =
            QueueKey { tenant_id: "tenant".to_owned(), task_queue: "default".to_owned() };
        let mut inner = RuntimeInner { owner_epoch: 11, ..RuntimeInner::default() };
        inner
            .ready
            .entry(queue_key.clone())
            .or_default()
            .push_back(QueuedActivity { lease_epoch: 4, ..queued_activity("activity-1", 1, now) });

        let leased = lease_ready_tasks(&mut inner, &queue_key, "worker", "build", 1);

        assert_eq!(leased.len(), 1);
        assert_eq!(leased[0].task.lease_epoch, 5);
        assert_eq!(leased[0].owner_epoch, 11);
    }

    #[tokio::test]
    async fn store_snapshot_restore_replays_broker_tail_after_handoff() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let publisher = redpanda.connect_publisher().await?;
        let broker = redpanda.broker.clone();

        let artifact = test_artifact("default");
        store.put_artifact("tenant-a", &artifact).await?;

        let identity = WorkflowIdentity::new(
            "tenant-a",
            &artifact.definition_id,
            artifact.definition_version,
            artifact.artifact_hash.clone(),
            "instance-handoff",
            "run-handoff",
            "unified-runtime-test",
        );
        let partition_id = publisher.partition_for_key(&identity.partition_key);
        let trigger_time = Utc::now();
        let trigger = test_event(
            &identity,
            WorkflowEvent::WorkflowTriggered { input: json!({"items": [json!({"ok": true})]}) },
            trigger_time,
        );
        publisher.publish(&trigger, &trigger.partition_key).await?;

        let plan = artifact.execute_trigger_with_turn(
            match &trigger.payload {
                WorkflowEvent::WorkflowTriggered { input } => input,
                _ => unreachable!("trigger payload"),
            },
            ExecutionTurnContext { event_id: trigger.event_id, occurred_at: trigger.occurred_at },
        )?;
        let mut instance = WorkflowInstanceState::try_from(&trigger)?;
        apply_compiled_plan(&mut instance, &plan);

        let emitted_at = trigger_time + ChronoDuration::milliseconds(1);
        let emitted_events = plan
            .emissions
            .iter()
            .map(|emission| test_event(&identity, emission.event.clone(), emitted_at))
            .collect::<Vec<_>>();
        publisher.publish_all(&emitted_events).await?;

        instance.event_count = 1 + i64::try_from(emitted_events.len()).unwrap_or_default();
        if let Some(last) = emitted_events.last() {
            instance.last_event_id = last.event_id;
            instance.last_event_type = last.event_type.clone();
            instance.updated_at = last.occurred_at;
        }
        store.upsert_instance(&instance).await?;
        store.put_snapshot(&instance).await?;

        let scheduled = schedule_activities_from_plan(&instance, &plan, emitted_at)?;
        let scheduled_updates = scheduled.iter().map(activity_schedule_update).collect::<Vec<_>>();
        store.upsert_activities_scheduled_batch(&scheduled_updates).await?;

        let completion_time = emitted_at + ChronoDuration::milliseconds(1);
        let completion = test_event(
            &identity,
            WorkflowEvent::ActivityTaskCompleted {
                activity_id: scheduled[0].activity_id.clone(),
                attempt: scheduled[0].attempt,
                output: json!({"ok": true}),
                worker_id: "worker-a".to_owned(),
                worker_build_id: "build-a".to_owned(),
            },
            completion_time,
        );
        publisher.publish(&completion, &completion.partition_key).await?;
        store
            .apply_activity_terminal_batch(&[ActivityTerminalUpdate {
                tenant_id: identity.tenant_id.clone(),
                instance_id: identity.instance_id.clone(),
                run_id: identity.run_id.clone(),
                activity_id: scheduled[0].activity_id.clone(),
                attempt: scheduled[0].attempt,
                worker_id: "worker-a".to_owned(),
                worker_build_id: "build-a".to_owned(),
                payload: ActivityTerminalPayload::Completed { output: json!({"ok": true}) },
                event_id: completion.event_id,
                event_type: completion.event_type.clone(),
                occurred_at: completion.occurred_at,
            }])
            .await?;

        let history_deadline = Instant::now() + StdDuration::from_secs(10);
        loop {
            let history = read_workflow_history(
                &broker,
                "unified-runtime-handoff-test",
                &WorkflowHistoryFilter::new(
                    &identity.tenant_id,
                    &identity.instance_id,
                    &identity.run_id,
                ),
                StdDuration::from_millis(500),
                StdDuration::from_secs(2),
            )
            .await?;
            if history.iter().any(|event| event.event_id == completion.event_id) {
                break;
            }
            if Instant::now() >= history_deadline {
                anyhow::bail!("completion event did not appear in broker history before restore");
            }
            sleep(StdDuration::from_millis(100)).await;
        }

        let owner_a = store
            .claim_partition_ownership(
                partition_id,
                "unified-owner-a",
                StdDuration::from_millis(200),
            )
            .await?
            .context("owner a should claim partition")?;
        assert_eq!(owner_a.owner_epoch, 1);

        sleep(StdDuration::from_millis(250)).await;

        let owner_b = store
            .claim_partition_ownership(partition_id, "unified-owner-b", StdDuration::from_secs(5))
            .await?
            .context("owner b should claim expired partition")?;
        assert_eq!(owner_b.owner_epoch, 2);

        let debug = Arc::new(StdMutex::new(UnifiedDebugState::default()));
        let restored = restore_runtime_state_from_store(&store, &broker, &debug).await?;
        let runtime = restored
            .instances
            .get(&RunKey {
                tenant_id: identity.tenant_id.clone(),
                instance_id: identity.instance_id.clone(),
                run_id: identity.run_id.clone(),
            })
            .context("restored runtime state should contain the handed off workflow")?;

        assert_eq!(runtime.instance.status, WorkflowStatus::Completed);
        assert_eq!(runtime.instance.last_event_id, completion.event_id);
        assert_eq!(runtime.instance.last_event_type, "ActivityTaskCompleted");
        assert!(runtime.active_activities.is_empty());
        assert!(restored.ready.values().all(VecDeque::is_empty));

        let debug_snapshot = debug.lock().expect("unified debug lock poisoned").clone();
        assert!(debug_snapshot.restored_from_snapshot);
        assert_eq!(debug_snapshot.restore_records_applied, 1);

        Ok(())
    }
}
