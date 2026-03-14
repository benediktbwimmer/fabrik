use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    env, fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex, OnceLock},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use axum::{
    Json,
    extract::Path as AxumPath,
    extract::State as AxumState,
    http::StatusCode,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use fabrik_broker::{
    BrokerConfig, JsonTopicConfig, JsonTopicPublisher, WorkflowHistoryFilter, WorkflowPublisher,
    build_workflow_consumer, decode_consumed_workflow_event, partition_for_instance,
    read_workflow_history,
};
use fabrik_config::{
    ExecutorRuntimeConfig, GrpcServiceConfig, HttpServiceConfig, PostgresConfig, RedpandaConfig,
    ThroughputPayloadStoreKind, ThroughputRuntimeConfig,
};
use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::{
    ActivityScheduleUpdate, ActivityStartUpdate, ActivityTerminalPayload, ActivityTerminalUpdate,
    BulkChunkTerminalPayload, BulkChunkTerminalUpdate, ConsumedSignalRecord, TaskQueueKind,
    ThroughputRunInputRecord, ThroughputRunRecord, WorkflowActivityStatus, WorkflowBulkChunkRecord,
    WorkflowMailboxKind, WorkflowMailboxRecord, WorkflowStore,
};
use fabrik_throughput::{
    ADMISSION_POLICY_VERSION, PayloadHandle, PayloadStore, PayloadStoreConfig, PayloadStoreKind,
    StartThroughputRunCommand, ThroughputBackend, ThroughputCommand, ThroughputCommandEnvelope,
    bulk_reducer_class, bulk_reducer_is_mergeable, bulk_reducer_materializes_results,
    can_use_payloadless_benchmark_transport, decode_cbor, encode_cbor,
    parse_benchmark_compact_total_items_from_handle, planned_reduction_tree_depth,
    stream_v2_fast_lane_enabled, throughput_partition_key,
};
use fabrik_worker_protocol::activity_worker::{
    Ack, ActivityTask, ActivityTaskCancelledResult, ActivityTaskCompletedResult,
    ActivityTaskFailedResult, ActivityTaskResult, BulkActivityTask, BulkActivityTaskResult,
    PollActivityTaskRequest, PollActivityTaskResponse, PollActivityTasksRequest,
    PollActivityTasksResponse, PollBulkActivityTaskRequest, PollBulkActivityTaskResponse,
    RecordActivityHeartbeatRequest, RecordActivityHeartbeatResponse,
    ReportActivityTaskCancelledRequest, ReportActivityTaskResultsRequest,
    ReportBulkActivityTaskResultsRequest, activity_task_result,
    activity_worker_api_server::{ActivityWorkerApi, ActivityWorkerApiServer},
};
use fabrik_workflow::{
    ArtifactExecutionState, CompiledExecutionPlan, CompiledWorkflowArtifact, ExecutionTurnContext,
    RetryPolicy, WorkflowInstanceState, WorkflowStatus, execution_state_for_event, parse_timer_ref,
    replay_compiled_history_trace, replay_compiled_history_trace_from_snapshot,
    replay_history_trace_from_snapshot, retry_policy_allows_failure_retry,
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
const BULK_EVENT_OUTBOX_BATCH_SIZE: usize = 128;
const BULK_EVENT_OUTBOX_LEASE_SECONDS: i64 = 30;
const BULK_EVENT_OUTBOX_RETRY_MS: u64 = 250;
const COMPILED_ARTIFACT_CACHE_CAPACITY: usize = 64;
const TASK_QUEUE_POLICY_CACHE_TTL_MS: u64 = 1_000;
#[derive(Clone)]
struct AppState {
    store: WorkflowStore,
    publisher: Option<WorkflowPublisher>,
    throughput_command_publisher: Option<JsonTopicPublisher<ThroughputCommandEnvelope>>,
    payload_store: PayloadStore,
    throughput_runtime: ThroughputRuntimeConfig,
    inner: Arc<StdMutex<RuntimeInner>>,
    ownership: Arc<StdMutex<UnifiedOwnershipState>>,
    workflow_partition_count: i32,
    notify: Arc<Notify>,
    bulk_notify: Arc<Notify>,
    retry_notify: Arc<Notify>,
    outbox_notify: Arc<Notify>,
    persist_notify: Arc<Notify>,
    persist_lock: Arc<Mutex<()>>,
    persistence: PersistenceConfig,
    admission: BulkAdmissionConfig,
    task_queue_policy_cache: Arc<StdMutex<HashMap<TaskQueuePolicyCacheKey, CachedTaskQueuePolicy>>>,
    debug: Arc<StdMutex<UnifiedDebugState>>,
    outbox_publisher_id: String,
}

#[derive(Debug, Clone)]
struct BulkAdmissionConfig {
    default_backend: String,
    task_queue_backends: BTreeMap<String, String>,
    stream_v2_min_items: usize,
    stream_v2_min_chunks: u32,
    max_active_chunks_per_tenant: usize,
    max_active_chunks_per_task_queue: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BulkAdmissionDecision {
    selected_backend: String,
    selected_backend_version: String,
    routing_reason: String,
    admission_policy_version: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct TaskQueuePolicyCacheKey {
    tenant_id: String,
    task_queue: String,
}

#[derive(Debug, Clone)]
struct CachedTaskQueuePolicy {
    backend: Option<String>,
    fetched_at: Instant,
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

impl BulkAdmissionConfig {
    fn from_env() -> Result<Self> {
        let executor = ExecutorRuntimeConfig::from_env()
            .context("failed to load executor throughput backend routing config")?;
        Ok(Self {
            default_backend: executor.throughput_default_backend,
            task_queue_backends: executor.throughput_task_queue_backends,
            stream_v2_min_items: read_env_usize("UNIFIED_RUNTIME_STREAM_V2_MIN_ITEMS", 512)?,
            stream_v2_min_chunks: read_env_u32("UNIFIED_RUNTIME_STREAM_V2_MIN_CHUNKS", 8)?,
            max_active_chunks_per_tenant: read_env_usize(
                "THROUGHPUT_MAX_ACTIVE_CHUNKS_PER_TENANT",
                4_096,
            )?,
            max_active_chunks_per_task_queue: read_env_usize(
                "THROUGHPUT_MAX_ACTIVE_CHUNKS_PER_TASK_QUEUE",
                2_048,
            )?,
        })
    }
}

#[derive(Default)]
struct SharedCompiledArtifactCache {
    access_epoch: u64,
    entries: HashMap<CompiledArtifactCacheKey, CachedCompiledArtifactEntry>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct CompiledArtifactCacheKey {
    tenant_id: String,
    definition_id: String,
    version: u32,
}

#[derive(Clone)]
struct CachedCompiledArtifactEntry {
    access_epoch: u64,
    artifact: Option<CompiledWorkflowArtifact>,
}

static SHARED_COMPILED_ARTIFACT_CACHE: OnceLock<StdMutex<SharedCompiledArtifactCache>> =
    OnceLock::new();

#[derive(Debug, Clone, Serialize, Default)]
struct UnifiedDebugState {
    restored_from_snapshot: bool,
    restore_records_applied: u64,
    workflow_triggers_applied: u64,
    trigger_total_micros: u64,
    trigger_artifact_load_micros: u64,
    trigger_plan_micros: u64,
    trigger_state_apply_micros: u64,
    trigger_db_apply_micros: u64,
    trigger_post_plan_micros: u64,
    trigger_bulk_admission_micros: u64,
    trigger_bulk_pg_materialize_micros: u64,
    trigger_bulk_publish_micros: u64,
    trigger_bulk_events_published: u64,
    trigger_cancel_check_micros: u64,
    trigger_mailbox_drain_micros: u64,
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

fn elapsed_micros(started_at: Instant) -> u64 {
    started_at.elapsed().as_micros().try_into().unwrap_or(u64::MAX)
}

fn shared_compiled_artifact_cache() -> &'static StdMutex<SharedCompiledArtifactCache> {
    SHARED_COMPILED_ARTIFACT_CACHE
        .get_or_init(|| StdMutex::new(SharedCompiledArtifactCache::default()))
}

fn get_cached_compiled_artifact(
    key: &CompiledArtifactCacheKey,
) -> Option<Option<CompiledWorkflowArtifact>> {
    let mut cache =
        shared_compiled_artifact_cache().lock().expect("compiled artifact cache lock poisoned");
    let access_epoch = cache.access_epoch.saturating_add(1);
    cache.access_epoch = access_epoch;
    let entry = cache.entries.get_mut(key)?;
    entry.access_epoch = access_epoch;
    Some(entry.artifact.clone())
}

fn cache_compiled_artifact(
    key: &CompiledArtifactCacheKey,
    artifact: Option<&CompiledWorkflowArtifact>,
) {
    let mut cache =
        shared_compiled_artifact_cache().lock().expect("compiled artifact cache lock poisoned");
    let access_epoch = cache.access_epoch.saturating_add(1);
    cache.access_epoch = access_epoch;
    cache.entries.insert(
        key.clone(),
        CachedCompiledArtifactEntry { access_epoch, artifact: artifact.cloned() },
    );
    evict_cached_compiled_artifacts(&mut cache);
}

fn evict_cached_compiled_artifacts(cache: &mut SharedCompiledArtifactCache) {
    if cache.entries.len() <= COMPILED_ARTIFACT_CACHE_CAPACITY {
        return;
    }
    let mut lru = cache
        .entries
        .iter()
        .map(|(key, entry)| (entry.access_epoch, key.clone()))
        .collect::<Vec<_>>();
    lru.sort_unstable_by_key(|(access_epoch, _)| *access_epoch);
    let remove_count = cache.entries.len().saturating_sub(COMPILED_ARTIFACT_CACHE_CAPACITY);
    for (_, key) in lru.into_iter().take(remove_count) {
        cache.entries.remove(&key);
    }
}

fn read_env_usize(key: &str, default: usize) -> Result<usize> {
    match env::var(key) {
        Ok(raw) => {
            raw.parse::<usize>().with_context(|| format!("{key} must be a valid usize, got {raw}"))
        }
        Err(env::VarError::NotPresent) => Ok(default),
        Err(source) => Err(anyhow::anyhow!("failed to read {key}: {source}")),
    }
}

fn read_env_u32(key: &str, default: u32) -> Result<u32> {
    match env::var(key) {
        Ok(raw) => {
            raw.parse::<u32>().with_context(|| format!("{key} must be a valid u32, got {raw}"))
        }
        Err(env::VarError::NotPresent) => Ok(default),
        Err(source) => Err(anyhow::anyhow!("failed to read {key}: {source}")),
    }
}

async fn load_compiled_artifact_version(
    state: &AppState,
    tenant_id: &str,
    definition_id: &str,
    version: u32,
) -> Result<Option<CompiledWorkflowArtifact>> {
    let key = CompiledArtifactCacheKey {
        tenant_id: tenant_id.to_owned(),
        definition_id: definition_id.to_owned(),
        version,
    };
    if let Some(artifact) = get_cached_compiled_artifact(&key) {
        return Ok(artifact);
    }
    let artifact = state.store.get_artifact_version(tenant_id, definition_id, version).await?;
    cache_compiled_artifact(&key, artifact.as_ref());
    Ok(artifact)
}

#[derive(Debug, Clone, Default)]
struct RuntimeInner {
    next_seq: u64,
    owner_epoch: u64,
    instances: HashMap<RunKey, RuntimeWorkflowState>,
    ready: HashMap<QueueKey, VecDeque<QueuedActivity>>,
    leased: HashMap<AttemptKey, LeasedActivity>,
    delayed_retries: BTreeMap<DateTime<Utc>, Vec<DelayedRetryTask>>,
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
    #[serde(default)]
    omit_success_output: bool,
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
    schedule_to_start_timeout_ms: Option<u64>,
    schedule_to_close_timeout_ms: Option<u64>,
    start_to_close_timeout_ms: Option<u64>,
    heartbeat_timeout_ms: Option<u64>,
    scheduled_at: DateTime<Utc>,
    #[serde(default)]
    cancellation_requested: bool,
    #[serde(default)]
    omit_success_output: bool,
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
    UpsertPersistedInstance(WorkflowInstanceState),
    CloseRun(RunKey, DateTime<Utc>),
    PutRunStart {
        tenant_id: String,
        instance_id: String,
        run_id: String,
        definition_id: String,
        definition_version: Option<u32>,
        artifact_hash: Option<String>,
        workflow_task_queue: String,
        memo: Option<Value>,
        search_attributes: Option<Value>,
        trigger_event_id: Uuid,
        started_at: DateTime<Utc>,
        previous_run_id: Option<String>,
        triggered_by_run_id: Option<String>,
    },
    UpsertChildStartRequested {
        tenant_id: String,
        instance_id: String,
        run_id: String,
        child_id: String,
        child_workflow_id: String,
        child_definition_id: String,
        parent_close_policy: String,
        input: Value,
        source_event_id: Uuid,
        occurred_at: DateTime<Utc>,
    },
    MarkChildStarted {
        tenant_id: String,
        instance_id: String,
        run_id: String,
        child_id: String,
        child_run_id: String,
        started_event_id: Uuid,
        occurred_at: DateTime<Utc>,
    },
    CompleteChild {
        tenant_id: String,
        instance_id: String,
        run_id: String,
        child_id: String,
        status: String,
        output: Option<Value>,
        error: Option<String>,
        terminal_event_id: Uuid,
        occurred_at: DateTime<Utc>,
    },
    CompleteUpdate {
        tenant_id: String,
        instance_id: String,
        run_id: String,
        update_id: String,
        output: Option<Value>,
        error: Option<String>,
        completed_event_id: Uuid,
        completed_at: DateTime<Utc>,
    },
    UpsertTimer {
        partition_id: i32,
        tenant_id: String,
        instance_id: String,
        run_id: String,
        definition_id: String,
        definition_version: Option<u32>,
        artifact_hash: Option<String>,
        timer_id: String,
        state: Option<String>,
        fire_at: DateTime<Utc>,
        scheduled_event_id: Uuid,
        correlation_id: Option<Uuid>,
    },
    DeleteTimer {
        tenant_id: String,
        instance_id: String,
        timer_id: String,
    },
    DeleteTimersForRun {
        tenant_id: String,
        instance_id: String,
        run_id: String,
    },
    RecordRunContinuation {
        tenant_id: String,
        instance_id: String,
        previous_run_id: String,
        new_run_id: String,
        continue_reason: String,
        continued_event_id: Uuid,
        triggered_event_id: Uuid,
        transitioned_at: DateTime<Utc>,
    },
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

#[derive(Debug, Deserialize)]
struct InternalQueryRequest {
    #[serde(default)]
    args: Value,
}

#[derive(Debug, Serialize)]
struct InternalQueryResponse {
    result: Value,
    consistency: &'static str,
    source: &'static str,
}

#[derive(Debug, Serialize)]
struct UnifiedErrorResponse {
    message: String,
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
    let throughput_runtime = ThroughputRuntimeConfig::from_env()?;
    let admission = BulkAdmissionConfig::from_env()?;
    init_tracing(&config.log_filter);
    let broker = BrokerConfig::new(
        redpanda.brokers.clone(),
        redpanda.workflow_events_topic.clone(),
        redpanda.workflow_events_partitions,
    );
    let throughput_commands = JsonTopicConfig::new(
        redpanda.brokers.clone(),
        redpanda.throughput_commands_topic.clone(),
        redpanda.throughput_partitions,
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
    let payload_store =
        PayloadStore::from_config(build_payload_store_config(&throughput_runtime)).await?;
    let shared_restored =
        restore_runtime_state_from_store(&store, &broker, &payload_store, &debug).await?;
    let inner = reconcile_restored_runtime(local_restored, shared_restored, &debug);
    let state = AppState {
        store,
        publisher: Some(WorkflowPublisher::new(&broker, "unified-runtime").await?),
        throughput_command_publisher: Some(
            JsonTopicPublisher::new(&throughput_commands, "unified-runtime-throughput-commands")
                .await?,
        ),
        payload_store,
        throughput_runtime,
        inner: Arc::new(StdMutex::new(inner)),
        ownership: Arc::new(StdMutex::new(UnifiedOwnershipState::inactive(
            ownership.partition_id,
            &ownership.owner_id,
        ))),
        workflow_partition_count: redpanda.workflow_events_partitions,
        notify: Arc::new(Notify::new()),
        bulk_notify: Arc::new(Notify::new()),
        retry_notify: Arc::new(Notify::new()),
        outbox_notify: Arc::new(Notify::new()),
        persist_notify: Arc::new(Notify::new()),
        persist_lock: Arc::new(Mutex::new(())),
        persistence,
        admission,
        task_queue_policy_cache: Arc::new(StdMutex::new(HashMap::new())),
        debug: debug.clone(),
        outbox_publisher_id: format!("unified-runtime:{}", Uuid::now_v7()),
    };
    acquire_initial_ownership(&state, &ownership).await?;
    reconcile_restored_ready_queue(&state).await?;

    let consumer =
        build_workflow_consumer(&broker, "unified-runtime", &broker.all_partition_ids()).await?;
    tokio::spawn(run_trigger_consumer(state.clone(), consumer));
    tokio::spawn(run_ownership_loop(state.clone(), ownership.clone()));
    tokio::spawn(run_retry_release_loop(state.clone()));
    tokio::spawn(run_lease_requeue_loop(state.clone()));
    tokio::spawn(run_workflow_event_outbox_publisher(state.clone()));
    tokio::spawn(run_persist_loop(state.clone()));
    if native_stream_v2_engine_enabled(&state) {
        tokio::spawn(run_throughput_run_repair_loop(state.clone()));
    }

    let debug_state = state.clone();
    let debug_app = default_router::<AppState>(ServiceInfo::new(
        debug_config.name,
        "unified-runtime-debug",
        env!("CARGO_PKG_VERSION"),
    ))
    .route(
        "/internal/workflows/{tenant_id}/{instance_id}/queries/{query_name}",
        post(execute_internal_query),
    )
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
                    delayed_retries: delayed_retry_count(&inner.delayed_retries),
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

fn build_payload_store_config(runtime: &ThroughputRuntimeConfig) -> PayloadStoreConfig {
    PayloadStoreConfig {
        default_store: match runtime.payload_store.kind {
            ThroughputPayloadStoreKind::LocalFilesystem => PayloadStoreKind::LocalFilesystem,
            ThroughputPayloadStoreKind::S3 => PayloadStoreKind::S3,
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

fn native_stream_v2_engine_enabled(state: &AppState) -> bool {
    state.throughput_runtime.native_stream_v2_engine_enabled
}

fn native_stream_v2_run_input_handle(
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
) -> PayloadHandle {
    PayloadHandle::Inline {
        key: format!("throughput-run-input:{tenant_id}:{instance_id}:{run_id}:{batch_id}"),
    }
}

fn prepare_stream_v2_trigger_persisted_instance(
    artifact: &CompiledWorkflowArtifact,
    instance: &WorkflowInstanceState,
    plan: &CompiledExecutionPlan,
) -> Result<Option<WorkflowInstanceState>> {
    if !trigger_uses_stream_v2_bulk_wait(plan, artifact, instance) {
        return Ok(None);
    }
    let mut persisted = instance.clone();
    if !persisted.compact_trigger_bulk_wait_for_replayable_restore() {
        return Ok(None);
    }
    Ok(Some(persisted))
}

fn trigger_uses_stream_v2_bulk_wait(
    plan: &CompiledExecutionPlan,
    artifact: &CompiledWorkflowArtifact,
    instance: &WorkflowInstanceState,
) -> bool {
    let Some(wait_state) = instance.current_state.as_deref() else {
        return false;
    };
    let Some(execution_state) = instance.artifact_execution.as_ref() else {
        return false;
    };
    artifact.waits_on_bulk_activity(wait_state, execution_state)
        && plan.emissions.iter().any(|emission| {
            matches!(
                &emission.event,
                WorkflowEvent::BulkActivityBatchScheduled { throughput_backend, .. }
                    if throughput_backend == ThroughputBackend::StreamV2.as_str()
            )
        })
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
    payload_store: &PayloadStore,
    debug: &Arc<StdMutex<UnifiedDebugState>>,
) -> Result<RuntimeInner> {
    let mut inner = RuntimeInner::default();
    let mut restored_runs = HashSet::new();
    let snapshots = store.list_nonterminal_snapshots().await?;
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
        let (run_key, runtime, ready) =
            restore_runtime_from_instance_row(store, payload_store, instance, Some(artifact))
                .await?;
        for task in ready {
            inner
                .ready
                .entry(QueueKey {
                    tenant_id: task.tenant_id.clone(),
                    task_queue: task.task_queue.clone(),
                })
                .or_default()
                .push_back(task);
        }
        restored_runs.insert(run_key.clone());
        inner.instances.insert(run_key, runtime);
    }

    for instance in store.list_nonterminal_instances().await? {
        let run_key = RunKey {
            tenant_id: instance.tenant_id.clone(),
            instance_id: instance.instance_id.clone(),
            run_id: instance.run_id.clone(),
        };
        if restored_runs.contains(&run_key) {
            continue;
        }
        let (run_key, runtime, ready) =
            restore_runtime_from_instance_row(store, payload_store, instance, None).await?;
        for task in ready {
            inner
                .ready
                .entry(QueueKey {
                    tenant_id: task.tenant_id.clone(),
                    task_queue: task.task_queue.clone(),
                })
                .or_default()
                .push_back(task);
        }
        restored_runs.insert(run_key.clone());
        inner.instances.insert(run_key, runtime);
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
                || delayed_retry_count(&shared.delayed_retries) > 0
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
                || delayed_retry_count(&local.delayed_retries) > 0;
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
    inner.delayed_retries.retain(|_, retries| {
        retries.retain(|retry| {
            !(retry.task.tenant_id == run_key.tenant_id
                && retry.task.instance_id == run_key.instance_id
                && retry.task.run_id == run_key.run_id)
        });
        !retries.is_empty()
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
    for retry in source.delayed_retries.values().flatten().filter(|retry| {
        retry.task.tenant_id == run_key.tenant_id
            && retry.task.instance_id == run_key.instance_id
            && retry.task.run_id == run_key.run_id
    }) {
        push_delayed_retry(inner, retry.clone());
    }
}

fn delayed_retry_count(delayed_retries: &BTreeMap<DateTime<Utc>, Vec<DelayedRetryTask>>) -> usize {
    delayed_retries.values().map(Vec::len).sum()
}

fn push_delayed_retry(inner: &mut RuntimeInner, retry: DelayedRetryTask) -> bool {
    let due_at = retry.due_at;
    let becomes_earliest =
        inner.delayed_retries.keys().next().map(|current| due_at < *current).unwrap_or(true);
    inner.delayed_retries.entry(due_at).or_default().push(retry);
    becomes_earliest
}

fn flatten_delayed_retries(
    delayed_retries: &BTreeMap<DateTime<Utc>, Vec<DelayedRetryTask>>,
) -> Vec<DelayedRetryTask> {
    delayed_retries.values().flat_map(|retries| retries.iter().cloned()).collect()
}

fn take_due_delayed_retries(inner: &mut RuntimeInner, now: DateTime<Utc>) -> Vec<QueuedActivity> {
    let due_keys =
        inner.delayed_retries.range(..=now).map(|(due_at, _)| due_at.clone()).collect::<Vec<_>>();
    let mut released = Vec::new();
    for due_at in due_keys {
        if let Some(retries) = inner.delayed_retries.remove(&due_at) {
            released.extend(retries.into_iter().map(|retry| retry.task));
        }
    }
    released
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
        match replay_unified_compiled_snapshot_tail(&snapshot.state, artifact, tail) {
            Ok(replayed) => return Ok(replayed),
            Err(error) => {
                warn!(
                    tenant_id = %snapshot.tenant_id,
                    instance_id = %snapshot.instance_id,
                    run_id = %snapshot.run_id,
                    snapshot_last_event_type = %snapshot.last_event_type,
                    "snapshot-tail replay failed; falling back to full broker history: {error}"
                );
            }
        }
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
        let execution_state = execution_state_for_event(&replayed, history_tail.get(index));
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
        let execution_state = execution_state_for_event(&replayed, Some(event));
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

fn workflow_partition_id(state: &AppState, tenant_id: &str, instance_id: &str) -> i32 {
    partition_for_instance(tenant_id, instance_id, state.workflow_partition_count)
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
            sync_workflow_partition_leases(state, config).await?;
            state.persist_notify.notify_one();
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(config.renew_interval_ms.max(1))).await;
    }
}

async fn sync_workflow_partition_leases(state: &AppState, config: &OwnershipConfig) -> Result<()> {
    for partition_id in 0..state.workflow_partition_count {
        if partition_id == config.partition_id {
            continue;
        }
        match state
            .store
            .claim_partition_ownership(partition_id, &config.owner_id, config.lease_ttl)
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => warn!(
                partition_id,
                owner_id = %config.owner_id,
                "unified-runtime could not claim workflow partition lease"
            ),
            Err(error) => warn!(
                error = %error,
                partition_id,
                owner_id = %config.owner_id,
                "unified-runtime failed to sync workflow partition lease"
            ),
        }
    }
    Ok(())
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
        if let Err(error) = sync_workflow_partition_leases(&state, &config).await {
            warn!(error = %error, "unified-runtime failed to sync workflow partition leases");
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
        match event.payload {
            WorkflowEvent::WorkflowTriggered { .. } => {
                if let Err(error) = handle_trigger_event(&state, event).await {
                    error!(error = %error, "unified-runtime failed to handle trigger");
                }
            }
            WorkflowEvent::BulkActivityBatchCompleted { .. }
            | WorkflowEvent::BulkActivityBatchFailed { .. }
            | WorkflowEvent::BulkActivityBatchCancelled { .. } => {
                if let Err(error) = handle_bulk_batch_terminal_event(&state, event).await {
                    error!(error = %error, "unified-runtime failed to handle bulk terminal");
                }
            }
            WorkflowEvent::ActivityTaskCancellationRequested { .. } => {
                if let Err(error) =
                    handle_activity_cancellation_requested_event(&state, event).await
                {
                    error!(error = %error, "unified-runtime failed to handle activity cancellation");
                }
            }
            WorkflowEvent::WorkflowUpdateRequested { .. }
            | WorkflowEvent::WorkflowCancellationRequested { .. }
            | WorkflowEvent::SignalQueued { .. } => {
                if let Err(error) = handle_mailbox_queue_event(&state, event).await {
                    error!(error = %error, "unified-runtime failed to handle mailbox queue");
                }
            }
            WorkflowEvent::ChildWorkflowSignalRequested { .. } => {
                if let Err(error) = handle_child_signal_requested_event(&state, event).await {
                    error!(error = %error, "unified-runtime failed to handle child signal request");
                }
            }
            WorkflowEvent::ChildWorkflowCancellationRequested { .. } => {
                if let Err(error) = handle_child_cancellation_requested_event(&state, event).await {
                    error!(
                        error = %error,
                        "unified-runtime failed to handle child cancellation request"
                    );
                }
            }
            WorkflowEvent::ExternalWorkflowSignalRequested { .. } => {
                if let Err(error) = handle_external_signal_requested_event(&state, event).await {
                    error!(error = %error, "unified-runtime failed to handle external signal request");
                }
            }
            WorkflowEvent::ExternalWorkflowCancellationRequested { .. } => {
                if let Err(error) =
                    handle_external_cancellation_requested_event(&state, event).await
                {
                    error!(
                        error = %error,
                        "unified-runtime failed to handle external cancellation request"
                    );
                }
            }
            WorkflowEvent::TimerFired { .. } => {
                if let Err(error) = handle_timer_fired_event(&state, event).await {
                    error!(error = %error, "unified-runtime failed to handle timer fired");
                }
            }
            _ => {}
        }
    }
}

async fn handle_trigger_event(state: &AppState, event: EventEnvelope<WorkflowEvent>) -> Result<()> {
    let total_started_at = Instant::now();
    let WorkflowEvent::WorkflowTriggered { input } = &event.payload else {
        return Ok(());
    };
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

    let artifact_started_at = Instant::now();
    let artifact = load_compiled_artifact_version(
        state,
        &event.tenant_id,
        &event.definition_id,
        event.definition_version,
    )
    .await?
    .ok_or_else(|| {
        anyhow::anyhow!(
            "missing artifact {} v{} for tenant {}",
            event.definition_id,
            event.definition_version,
            event.tenant_id
        )
    })?;
    let artifact_load_micros = elapsed_micros(artifact_started_at);
    let plan_started_at = Instant::now();
    let projected = WorkflowInstanceState::try_from(&event)
        .context("trigger event did not project workflow state")?;
    let plan = artifact.execute_trigger_with_state_and_turn(
        input,
        execution_state_for_event(&projected, Some(&event)),
        ExecutionTurnContext { event_id: event.event_id, occurred_at: event.occurred_at },
    )?;
    let plan_micros = elapsed_micros(plan_started_at);
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
        persisted_input_handle: None,
        memo: None,
        search_attributes: None,
        output: plan.output.clone(),
        event_count: 1,
        last_event_id: event.event_id,
        last_event_type: event.event_type.clone(),
        updated_at: event.occurred_at,
    };
    apply_compiled_plan(&mut instance, &plan);
    let queued = schedule_activities_from_plan(&artifact, &instance, &plan, event.occurred_at)?;
    let persisted_instance =
        prepare_stream_v2_trigger_persisted_instance(&artifact, &instance, &plan)?;
    let state_apply_started_at = Instant::now();
    {
        let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
        let runtime = RuntimeWorkflowState {
            artifact: artifact.clone(),
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
                            omit_success_output: task.omit_success_output,
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
    let state_apply_micros = elapsed_micros(state_apply_started_at);
    let schedule_actions =
        queued.iter().cloned().map(PreparedDbAction::Schedule).collect::<Vec<_>>();
    let db_apply_started_at = Instant::now();
    apply_db_actions(
        state,
        vec![
            persisted_instance
                .map(PreparedDbAction::UpsertPersistedInstance)
                .unwrap_or_else(|| PreparedDbAction::UpsertInstance(instance.clone())),
        ],
        schedule_actions,
    )
    .await?;
    let db_apply_micros = elapsed_micros(db_apply_started_at);
    let post_plan_started_at = Instant::now();
    apply_post_plan_effects_with_options(
        state,
        PostPlanEffect {
            run_key: run_key.clone(),
            artifact: artifact.clone(),
            instance: instance.clone(),
            plan: plan.clone(),
            source_event_id: event.event_id,
            occurred_at: event.occurred_at,
        },
        false,
    )
    .await?;
    let post_plan_micros = elapsed_micros(post_plan_started_at);
    let cancel_check_started_at = Instant::now();
    maybe_enact_pending_workflow_cancellation_unified(
        state,
        &run_key,
        event.event_id,
        event.occurred_at,
    )
    .await?;
    let cancel_check_micros = elapsed_micros(cancel_check_started_at);
    state.notify.notify_waiters();
    state.persist_notify.notify_one();
    let mailbox_drain_started_at = Instant::now();
    drain_mailbox_for_run(state, &run_key.tenant_id, &run_key.instance_id, &run_key.run_id).await?;
    let mailbox_drain_micros = elapsed_micros(mailbox_drain_started_at);
    let mut debug = state.debug.lock().expect("unified debug lock poisoned");
    debug.workflow_triggers_applied = debug.workflow_triggers_applied.saturating_add(1);
    debug.trigger_total_micros =
        debug.trigger_total_micros.saturating_add(elapsed_micros(total_started_at));
    debug.trigger_artifact_load_micros =
        debug.trigger_artifact_load_micros.saturating_add(artifact_load_micros);
    debug.trigger_plan_micros = debug.trigger_plan_micros.saturating_add(plan_micros);
    debug.trigger_state_apply_micros =
        debug.trigger_state_apply_micros.saturating_add(state_apply_micros);
    debug.trigger_db_apply_micros = debug.trigger_db_apply_micros.saturating_add(db_apply_micros);
    debug.trigger_post_plan_micros =
        debug.trigger_post_plan_micros.saturating_add(post_plan_micros);
    debug.trigger_cancel_check_micros =
        debug.trigger_cancel_check_micros.saturating_add(cancel_check_micros);
    debug.trigger_mailbox_drain_micros =
        debug.trigger_mailbox_drain_micros.saturating_add(mailbox_drain_micros);
    Ok(())
}

async fn handle_bulk_batch_terminal_event(
    state: &AppState,
    event: EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    let run_key = RunKey {
        tenant_id: event.tenant_id.clone(),
        instance_id: event.instance_id.clone(),
        run_id: event.run_id.clone(),
    };

    let mut general = Vec::new();
    let mut schedules = Vec::new();
    let (final_instance, post_plan) = {
        let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
        let Some(runtime) = inner.instances.get_mut(&run_key) else {
            return Ok(());
        };
        if runtime.instance.status.is_terminal() {
            return Ok(());
        }

        let current_state = runtime
            .instance
            .current_state
            .clone()
            .unwrap_or_else(|| runtime.artifact.workflow.initial_state.clone());
        let execution_state = execution_state_for_event(&runtime.instance, Some(&event));
        let turn_context =
            ExecutionTurnContext { event_id: event.event_id, occurred_at: event.occurred_at };
        let plan = match &event.payload {
            WorkflowEvent::BulkActivityBatchCompleted {
                batch_id,
                total_items,
                succeeded_items,
                failed_items,
                cancelled_items,
                chunk_count,
                reducer_output,
            } => runtime.artifact.execute_after_bulk_completion_with_turn(
                &current_state,
                batch_id,
                &bulk_terminal_payload(
                    batch_id,
                    "completed",
                    None,
                    *total_items,
                    *succeeded_items,
                    *failed_items,
                    *cancelled_items,
                    *chunk_count,
                    reducer_output.as_ref(),
                ),
                execution_state,
                turn_context,
            )?,
            WorkflowEvent::BulkActivityBatchFailed {
                batch_id,
                total_items,
                succeeded_items,
                failed_items,
                cancelled_items,
                chunk_count,
                message,
                reducer_output,
            } => runtime.artifact.execute_after_bulk_failure_with_turn(
                &current_state,
                batch_id,
                &bulk_terminal_payload(
                    batch_id,
                    "failed",
                    Some(message.as_str()),
                    *total_items,
                    *succeeded_items,
                    *failed_items,
                    *cancelled_items,
                    *chunk_count,
                    reducer_output.as_ref(),
                ),
                execution_state,
                turn_context,
            )?,
            WorkflowEvent::BulkActivityBatchCancelled {
                batch_id,
                total_items,
                succeeded_items,
                failed_items,
                cancelled_items,
                chunk_count,
                message,
                reducer_output,
            } => runtime.artifact.execute_after_bulk_failure_with_turn(
                &current_state,
                batch_id,
                &bulk_terminal_payload(
                    batch_id,
                    "cancelled",
                    Some(message.as_str()),
                    *total_items,
                    *succeeded_items,
                    *failed_items,
                    *cancelled_items,
                    *chunk_count,
                    reducer_output.as_ref(),
                ),
                execution_state,
                turn_context,
            )?,
            _ => return Ok(()),
        };

        runtime.instance.apply_event(&event);
        apply_compiled_plan(&mut runtime.instance, &plan);
        let scheduled_tasks = schedule_activities_from_plan(
            &runtime.artifact,
            &runtime.instance,
            &plan,
            event.occurred_at,
        )?;
        for task in &scheduled_tasks {
            schedules.push(PreparedDbAction::Schedule(task.clone()));
            runtime.active_activities.insert(
                task.activity_id.clone(),
                ActiveActivityMeta {
                    attempt: task.attempt,
                    task_queue: task.task_queue.clone(),
                    activity_type: task.activity_type.clone(),
                    wait_state: task.state.clone(),
                    omit_success_output: task.omit_success_output,
                },
            );
        }
        let ready_tasks = scheduled_tasks;
        let final_instance = runtime.instance.clone();
        general.push(PreparedDbAction::UpsertInstance(runtime.instance.clone()));
        let post_plan = PostPlanEffect {
            run_key: run_key.clone(),
            artifact: runtime.artifact.clone(),
            instance: runtime.instance.clone(),
            plan,
            source_event_id: event.event_id,
            occurred_at: event.occurred_at,
        };
        mark_runtime_dirty(&mut inner, "bulk_terminal", true);
        for task in &ready_tasks {
            inner
                .ready
                .entry(QueueKey {
                    tenant_id: task.tenant_id.clone(),
                    task_queue: task.task_queue.clone(),
                })
                .or_default()
                .push_back(task.clone());
        }
        (final_instance, post_plan)
    };

    apply_db_actions(state, general, schedules).await?;
    apply_post_plan_effects(state, post_plan).await?;
    if final_instance.status.is_terminal() {
        maybe_enact_pending_workflow_cancellation_unified(
            state,
            &run_key,
            event.event_id,
            event.occurred_at,
        )
        .await?;
    }
    state.notify.notify_waiters();
    state.persist_notify.notify_one();
    Ok(())
}

async fn handle_timer_fired_event(
    state: &AppState,
    event: EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    let WorkflowEvent::TimerFired { timer_id } = &event.payload else {
        return Ok(());
    };
    let run_key = RunKey {
        tenant_id: event.tenant_id.clone(),
        instance_id: event.instance_id.clone(),
        run_id: event.run_id.clone(),
    };

    let (artifact, instance) = {
        let inner = state.inner.lock().expect("unified runtime lock poisoned");
        let Some(runtime) = inner.instances.get(&run_key) else {
            return Ok(());
        };
        (runtime.artifact.clone(), runtime.instance.clone())
    };
    if instance.status.is_terminal() {
        return Ok(());
    }

    let current_state =
        instance.current_state.clone().unwrap_or_else(|| artifact.workflow.initial_state.clone());
    let execution_state = execution_state_for_event(&instance, Some(&event));
    let plan = artifact.execute_after_timer_with_turn(
        &current_state,
        timer_id,
        execution_state,
        ExecutionTurnContext { event_id: event.event_id, occurred_at: event.occurred_at },
    )?;

    let mut general = vec![PreparedDbAction::DeleteTimer {
        tenant_id: event.tenant_id.clone(),
        instance_id: event.instance_id.clone(),
        timer_id: timer_id.clone(),
    }];
    let mut schedules = Vec::new();
    let mut notifies = false;
    let final_instance;
    {
        let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
        let Some(runtime) = inner.instances.get_mut(&run_key) else {
            return Ok(());
        };
        runtime.instance.apply_event(&event);
        apply_compiled_plan(&mut runtime.instance, &plan);
        let scheduled_tasks = schedule_activities_from_plan(
            &runtime.artifact,
            &runtime.instance,
            &plan,
            event.occurred_at,
        )?;
        for task in &scheduled_tasks {
            schedules.push(PreparedDbAction::Schedule(task.clone()));
            runtime.active_activities.insert(
                task.activity_id.clone(),
                ActiveActivityMeta {
                    attempt: task.attempt,
                    task_queue: task.task_queue.clone(),
                    activity_type: task.activity_type.clone(),
                    wait_state: task.state.clone(),
                    omit_success_output: task.omit_success_output,
                },
            );
        }
        let terminal_instance =
            runtime.instance.status.is_terminal().then_some(runtime.instance.clone());
        final_instance = Some(runtime.instance.clone());
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
            general.push(PreparedDbAction::CloseRun(run_key.clone(), event.occurred_at));
        }
        mark_runtime_dirty(&mut inner, "timer_fired", false);
    }

    apply_db_actions(state, general, schedules).await?;
    if let Some(post_plan) = final_instance.map(|instance| PostPlanEffect {
        run_key,
        artifact,
        instance,
        plan,
        source_event_id: event.event_id,
        occurred_at: event.occurred_at,
    }) {
        apply_post_plan_effects(state, post_plan).await?;
    }
    maybe_enact_pending_workflow_cancellation_unified(
        state,
        &RunKey {
            tenant_id: event.tenant_id.clone(),
            instance_id: event.instance_id.clone(),
            run_id: event.run_id.clone(),
        },
        event.event_id,
        event.occurred_at,
    )
    .await?;
    if notifies {
        state.notify.notify_waiters();
    }
    state.persist_notify.notify_one();
    Ok(())
}

async fn handle_activity_cancellation_requested_event(
    state: &AppState,
    event: EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    let WorkflowEvent::ActivityTaskCancellationRequested { activity_id, attempt, reason, metadata } =
        &event.payload
    else {
        return Ok(());
    };

    if !state
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
        return Ok(());
    }

    let run_key = RunKey {
        tenant_id: event.tenant_id.clone(),
        instance_id: event.instance_id.clone(),
        run_id: event.run_id.clone(),
    };
    let attempt_key = AttemptKey {
        tenant_id: event.tenant_id.clone(),
        instance_id: event.instance_id.clone(),
        run_id: event.run_id.clone(),
        activity_id: activity_id.clone(),
        attempt: *attempt,
    };

    let mut general = Vec::new();
    let mut schedules = Vec::new();
    let mut post_plan = None;
    let mut notifies = false;

    {
        let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
        if let Some(leased) = inner.leased.get_mut(&attempt_key) {
            leased.task.cancellation_requested = true;
            mark_runtime_dirty(&mut inner, "activity_cancel_requested", false);
        } else {
            let active = inner
                .instances
                .get(&run_key)
                .and_then(|runtime| runtime.active_activities.get(activity_id))
                .filter(|active| active.attempt == *attempt)
                .cloned();
            let Some(active) = active else {
                return Ok(());
            };
            let queue_key = QueueKey {
                tenant_id: event.tenant_id.clone(),
                task_queue: active.task_queue.clone(),
            };
            let removed = inner.ready.get_mut(&queue_key).and_then(|queue| {
                queue
                    .iter()
                    .position(|task| task.activity_id == *activity_id && task.attempt == *attempt)
                    .and_then(|index| queue.remove(index))
            });
            let Some(queued) = removed else {
                return Ok(());
            };

            let runtime = inner.instances.get_mut(&run_key).expect("runtime exists after lookup");
            runtime.active_activities.remove(activity_id);
            general.push(PreparedDbAction::Terminal(ActivityTerminalUpdate {
                tenant_id: event.tenant_id.clone(),
                instance_id: event.instance_id.clone(),
                run_id: event.run_id.clone(),
                activity_id: activity_id.clone(),
                attempt: *attempt,
                worker_id: "unified-runtime".to_owned(),
                worker_build_id: "system".to_owned(),
                payload: ActivityTerminalPayload::Cancelled {
                    reason: reason.clone(),
                    metadata: metadata.clone(),
                },
                event_id: Uuid::now_v7(),
                event_type: "UnifiedActivityCancelled".to_owned(),
                occurred_at: event.occurred_at,
            }));

            let wait_state = runtime.instance.current_state.clone();
            if let Some(wait_state) = wait_state {
                let cancelled_event = EventEnvelope::new(
                    WorkflowEvent::ActivityTaskCancelled {
                        activity_id: activity_id.clone(),
                        attempt: *attempt,
                        reason: reason.clone(),
                        worker_id: "unified-runtime".to_owned(),
                        worker_build_id: "system".to_owned(),
                        metadata: metadata.clone(),
                    }
                    .event_type(),
                    WorkflowIdentity::new(
                        event.tenant_id.clone(),
                        runtime.instance.definition_id.clone(),
                        runtime
                            .instance
                            .definition_version
                            .unwrap_or(runtime.artifact.definition_version),
                        runtime.instance.artifact_hash.clone().unwrap_or_default(),
                        event.instance_id.clone(),
                        event.run_id.clone(),
                        "unified-runtime",
                    ),
                    WorkflowEvent::ActivityTaskCancelled {
                        activity_id: activity_id.clone(),
                        attempt: *attempt,
                        reason: reason.clone(),
                        worker_id: "unified-runtime".to_owned(),
                        worker_build_id: "system".to_owned(),
                        metadata: metadata.clone(),
                    },
                )
                .with_occurred_at(event.occurred_at);
                let execution_state =
                    execution_state_for_event(&runtime.instance, Some(&cancelled_event));
                if let Some((plan, _)) =
                    runtime.artifact.try_execute_after_step_terminal_batch_with_turn(
                        &wait_state,
                        std::slice::from_ref(&cancelled_event),
                        execution_state,
                    )?
                {
                    apply_compiled_plan(&mut runtime.instance, &plan);
                    runtime.instance.updated_at = event.occurred_at;
                    post_plan = Some(PostPlanEffect {
                        run_key: run_key.clone(),
                        artifact: runtime.artifact.clone(),
                        instance: runtime.instance.clone(),
                        plan: plan.clone(),
                        source_event_id: event.event_id,
                        occurred_at: event.occurred_at,
                    });
                    let scheduled_tasks = schedule_activities_from_plan(
                        &runtime.artifact,
                        &runtime.instance,
                        &plan,
                        event.occurred_at,
                    )?;
                    let terminal_instance =
                        runtime.instance.status.is_terminal().then_some(runtime.instance.clone());
                    for task in &scheduled_tasks {
                        schedules.push(PreparedDbAction::Schedule(task.clone()));
                        runtime.active_activities.insert(
                            task.activity_id.clone(),
                            ActiveActivityMeta {
                                attempt: task.attempt,
                                task_queue: task.task_queue.clone(),
                                activity_type: task.activity_type.clone(),
                                wait_state: task.state.clone(),
                                omit_success_output: task.omit_success_output,
                            },
                        );
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
                        general
                            .push(PreparedDbAction::CloseRun(run_key.clone(), event.occurred_at));
                    }
                }
            }
            let _ = queued;
            mark_runtime_dirty(&mut inner, "activity_cancel_requested", false);
        }
    }

    if !general.is_empty() || !schedules.is_empty() {
        apply_db_actions(state, general, schedules).await?;
    }
    if let Some(post_plan) = post_plan {
        apply_post_plan_effects(state, post_plan).await?;
    }
    if notifies {
        state.notify.notify_waiters();
    }
    state.persist_notify.notify_one();
    Ok(())
}

fn unified_child_signal_event_id(source_event_id: Uuid, child_id: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("unified-child-signal:{source_event_id}:{child_id}").as_bytes(),
    )
}

fn unified_child_cancel_event_id(source_event_id: Uuid, child_id: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("unified-child-cancel:{source_event_id}:{child_id}").as_bytes(),
    )
}

async fn handle_child_signal_requested_event(
    state: &AppState,
    event: EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    let WorkflowEvent::ChildWorkflowSignalRequested { child_id, signal_name, payload } =
        &event.payload
    else {
        return Ok(());
    };
    let child = state
        .store
        .list_open_children_for_run(&event.tenant_id, &event.instance_id, &event.run_id)
        .await?
        .into_iter()
        .find(|child| &child.child_id == child_id);
    let Some(child) = child else {
        warn!(
            workflow_instance_id = %event.instance_id,
            child_id = %child_id,
            "child signal requested for unknown open child"
        );
        return Ok(());
    };
    let Some(child_run_id) = child.child_run_id.clone() else {
        warn!(
            workflow_instance_id = %event.instance_id,
            child_id = %child_id,
            "child signal requested before child run started"
        );
        return Ok(());
    };

    let hot_child_instance = {
        let inner = state.inner.lock().expect("unified runtime lock poisoned");
        inner
            .instances
            .get(&RunKey {
                tenant_id: child.tenant_id.clone(),
                instance_id: child.child_workflow_id.clone(),
                run_id: child_run_id.clone(),
            })
            .map(|runtime| runtime.instance.clone())
    };
    let stored_child_instance = if hot_child_instance.is_none() {
        state.store.get_instance(&child.tenant_id, &child.child_workflow_id).await?
    } else {
        None
    };
    let Some(child_instance) = hot_child_instance.or(stored_child_instance) else {
        warn!(
            workflow_instance_id = %event.instance_id,
            child_id = %child_id,
            child_run_id = %child_run_id,
            "child signal requested for missing child instance"
        );
        return Ok(());
    };
    let partition_id = workflow_partition_id(state, &child.tenant_id, &child.child_workflow_id);
    let child_task_queue = child_instance.workflow_task_queue.clone();
    let signal_id = format!("child-sig-{}", event.event_id);
    let signal_payload = WorkflowEvent::SignalQueued {
        signal_id: signal_id.clone(),
        signal_type: signal_name.clone(),
        payload: payload.clone(),
    };
    let mut signal_event = EventEnvelope::new(
        signal_payload.event_type(),
        WorkflowIdentity::new(
            child.tenant_id.clone(),
            child_instance.definition_id.clone(),
            child_instance.definition_version.unwrap_or(event.definition_version),
            child_instance.artifact_hash.clone().unwrap_or_else(|| event.artifact_hash.clone()),
            child.child_workflow_id.clone(),
            child_run_id.clone(),
            "unified-runtime",
        ),
        signal_payload,
    )
    .with_occurred_at(event.occurred_at);
    signal_event.event_id = unified_child_signal_event_id(event.event_id, child_id);
    signal_event.causation_id = Some(event.event_id);
    signal_event.correlation_id = event.correlation_id.or(Some(event.event_id));
    signal_event.dedupe_key = Some(format!("child-signal:{child_id}:{}", event.event_id));

    if !state
        .store
        .queue_signal(
            &signal_event.tenant_id,
            &signal_event.instance_id,
            &signal_event.run_id,
            &signal_id,
            signal_name,
            signal_event.dedupe_key.as_deref(),
            payload,
            signal_event.event_id,
            signal_event.occurred_at,
        )
        .await?
    {
        return Ok(());
    }

    state
        .store
        .enqueue_workflow_mailbox_message(
            partition_id,
            &child_task_queue,
            None,
            &signal_event,
            WorkflowMailboxKind::Signal,
            Some(&signal_id),
            Some(signal_name),
            Some(payload),
        )
        .await?;

    let child_run_key = RunKey {
        tenant_id: child.tenant_id.clone(),
        instance_id: child.child_workflow_id.clone(),
        run_id: child_run_id,
    };
    let is_hot = {
        let inner = state.inner.lock().expect("unified runtime lock poisoned");
        inner.instances.contains_key(&child_run_key)
    };
    if is_hot {
        drain_mailbox_for_run(
            state,
            &child_run_key.tenant_id,
            &child_run_key.instance_id,
            &child_run_key.run_id,
        )
        .await?;
    }
    Ok(())
}

async fn handle_child_cancellation_requested_event(
    state: &AppState,
    event: EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    let WorkflowEvent::ChildWorkflowCancellationRequested { child_id, reason } = &event.payload
    else {
        return Ok(());
    };
    let child = state
        .store
        .list_open_children_for_run(&event.tenant_id, &event.instance_id, &event.run_id)
        .await?
        .into_iter()
        .find(|child| &child.child_id == child_id);
    let Some(child) = child else {
        warn!(
            workflow_instance_id = %event.instance_id,
            child_id = %child_id,
            "child cancellation requested for unknown open child"
        );
        return Ok(());
    };
    let Some(child_run_id) = child.child_run_id.clone() else {
        state
            .store
            .complete_child(
                &child.tenant_id,
                &child.instance_id,
                &child.run_id,
                &child.child_id,
                "cancelled",
                None,
                Some("cancel requested before child start"),
                event.event_id,
                event.occurred_at,
            )
            .await?;
        return Ok(());
    };

    let hot_child_instance = {
        let inner = state.inner.lock().expect("unified runtime lock poisoned");
        inner
            .instances
            .get(&RunKey {
                tenant_id: child.tenant_id.clone(),
                instance_id: child.child_workflow_id.clone(),
                run_id: child_run_id.clone(),
            })
            .map(|runtime| runtime.instance.clone())
    };
    let stored_child_instance = if hot_child_instance.is_none() {
        state.store.get_instance(&child.tenant_id, &child.child_workflow_id).await?
    } else {
        None
    };
    let child_task_queue = hot_child_instance
        .as_ref()
        .map(|instance| instance.workflow_task_queue.clone())
        .or_else(|| {
            stored_child_instance.as_ref().map(|instance| instance.workflow_task_queue.clone())
        })
        .unwrap_or_else(|| "default".to_owned());
    let child_definition_version = hot_child_instance
        .as_ref()
        .and_then(|instance| instance.definition_version)
        .or_else(|| stored_child_instance.as_ref().and_then(|instance| instance.definition_version))
        .unwrap_or(event.definition_version);
    let child_artifact_hash = hot_child_instance
        .as_ref()
        .and_then(|instance| instance.artifact_hash.clone())
        .or_else(|| {
            stored_child_instance.as_ref().and_then(|instance| instance.artifact_hash.clone())
        })
        .unwrap_or_else(|| event.artifact_hash.clone());
    let partition_id = workflow_partition_id(state, &child.tenant_id, &child.child_workflow_id);
    let mut cancel_event = EventEnvelope::new(
        WorkflowEvent::WorkflowCancellationRequested { reason: reason.clone() }.event_type(),
        WorkflowIdentity::new(
            child.tenant_id.clone(),
            child.child_definition_id.clone(),
            child_definition_version,
            child_artifact_hash,
            child.child_workflow_id.clone(),
            child_run_id.clone(),
            "unified-runtime",
        ),
        WorkflowEvent::WorkflowCancellationRequested { reason: reason.clone() },
    )
    .with_occurred_at(event.occurred_at);
    cancel_event.event_id = unified_child_cancel_event_id(event.event_id, child_id);
    cancel_event.causation_id = Some(event.event_id);
    cancel_event.correlation_id = event.correlation_id.or(Some(event.event_id));
    cancel_event.dedupe_key = Some(format!("child-cancel:{child_id}:{}", event.event_id));
    state
        .store
        .enqueue_workflow_mailbox_message(
            partition_id,
            &child_task_queue,
            None,
            &cancel_event,
            WorkflowMailboxKind::CancelRequest,
            None,
            None,
            None,
        )
        .await?;

    let child_run_key = RunKey {
        tenant_id: child.tenant_id.clone(),
        instance_id: child.child_workflow_id.clone(),
        run_id: child_run_id,
    };
    let is_hot = {
        let inner = state.inner.lock().expect("unified runtime lock poisoned");
        inner.instances.contains_key(&child_run_key)
    };
    if is_hot {
        drain_mailbox_for_run(
            state,
            &child_run_key.tenant_id,
            &child_run_key.instance_id,
            &child_run_key.run_id,
        )
        .await?;
    }
    Ok(())
}

async fn handle_external_signal_requested_event(
    state: &AppState,
    event: EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    let WorkflowEvent::ExternalWorkflowSignalRequested {
        target_instance_id,
        target_run_id,
        signal_name,
        payload,
    } = &event.payload
    else {
        return Ok(());
    };
    let Some(target_instance) =
        state.store.get_instance(&event.tenant_id, target_instance_id).await?
    else {
        warn!(
            workflow_instance_id = %event.instance_id,
            target_instance_id = %target_instance_id,
            "external signal requested for missing target instance"
        );
        return Ok(());
    };
    if target_run_id.as_ref().is_some_and(|run_id| run_id != &target_instance.run_id) {
        warn!(
            workflow_instance_id = %event.instance_id,
            target_instance_id = %target_instance_id,
            requested_run_id = ?target_run_id,
            actual_run_id = %target_instance.run_id,
            "external signal requested for non-current target run"
        );
        return Ok(());
    }
    let partition_id = workflow_partition_id(state, &event.tenant_id, &target_instance.instance_id);
    let signal_id = format!("ext-sig-{}", event.event_id);
    let signal_payload = WorkflowEvent::SignalQueued {
        signal_id: signal_id.clone(),
        signal_type: signal_name.clone(),
        payload: payload.clone(),
    };
    let mut signal_event = EventEnvelope::new(
        signal_payload.event_type(),
        WorkflowIdentity::new(
            event.tenant_id.clone(),
            target_instance.definition_id.clone(),
            target_instance.definition_version.unwrap_or(event.definition_version),
            target_instance.artifact_hash.clone().unwrap_or_else(|| event.artifact_hash.clone()),
            target_instance.instance_id.clone(),
            target_instance.run_id.clone(),
            "unified-runtime",
        ),
        signal_payload,
    )
    .with_occurred_at(event.occurred_at);
    signal_event.causation_id = Some(event.event_id);
    signal_event.correlation_id = event.correlation_id.or(Some(event.event_id));
    signal_event.dedupe_key =
        Some(format!("external-signal:{target_instance_id}:{}", event.event_id));
    if !state
        .store
        .queue_signal(
            &signal_event.tenant_id,
            &signal_event.instance_id,
            &signal_event.run_id,
            &signal_id,
            signal_name,
            signal_event.dedupe_key.as_deref(),
            payload,
            signal_event.event_id,
            signal_event.occurred_at,
        )
        .await?
    {
        return Ok(());
    }
    state
        .store
        .enqueue_workflow_mailbox_message(
            partition_id,
            &target_instance.workflow_task_queue,
            None,
            &signal_event,
            WorkflowMailboxKind::Signal,
            Some(&signal_id),
            Some(signal_name),
            Some(payload),
        )
        .await?;
    let target_run_key = RunKey {
        tenant_id: event.tenant_id.clone(),
        instance_id: target_instance.instance_id.clone(),
        run_id: target_instance.run_id.clone(),
    };
    let is_hot = {
        let inner = state.inner.lock().expect("unified runtime lock poisoned");
        inner.instances.contains_key(&target_run_key)
    };
    if is_hot {
        drain_mailbox_for_run(
            state,
            &target_run_key.tenant_id,
            &target_run_key.instance_id,
            &target_run_key.run_id,
        )
        .await?;
    }
    Ok(())
}

async fn handle_external_cancellation_requested_event(
    state: &AppState,
    event: EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    let WorkflowEvent::ExternalWorkflowCancellationRequested {
        target_instance_id,
        target_run_id,
        reason,
    } = &event.payload
    else {
        return Ok(());
    };
    let Some(target_instance) =
        state.store.get_instance(&event.tenant_id, target_instance_id).await?
    else {
        warn!(
            workflow_instance_id = %event.instance_id,
            target_instance_id = %target_instance_id,
            "external cancellation requested for missing target instance"
        );
        return Ok(());
    };
    if target_run_id.as_ref().is_some_and(|run_id| run_id != &target_instance.run_id) {
        warn!(
            workflow_instance_id = %event.instance_id,
            target_instance_id = %target_instance_id,
            requested_run_id = ?target_run_id,
            actual_run_id = %target_instance.run_id,
            "external cancellation requested for non-current target run"
        );
        return Ok(());
    }
    let partition_id = workflow_partition_id(state, &event.tenant_id, &target_instance.instance_id);
    let mut cancel_event = EventEnvelope::new(
        WorkflowEvent::WorkflowCancellationRequested { reason: reason.clone() }.event_type(),
        WorkflowIdentity::new(
            event.tenant_id.clone(),
            target_instance.definition_id.clone(),
            target_instance.definition_version.unwrap_or(event.definition_version),
            target_instance.artifact_hash.clone().unwrap_or_else(|| event.artifact_hash.clone()),
            target_instance.instance_id.clone(),
            target_instance.run_id.clone(),
            "unified-runtime",
        ),
        WorkflowEvent::WorkflowCancellationRequested { reason: reason.clone() },
    )
    .with_occurred_at(event.occurred_at);
    cancel_event.causation_id = Some(event.event_id);
    cancel_event.correlation_id = event.correlation_id.or(Some(event.event_id));
    cancel_event.dedupe_key =
        Some(format!("external-cancel:{target_instance_id}:{}", event.event_id));
    state
        .store
        .enqueue_workflow_mailbox_message(
            partition_id,
            &target_instance.workflow_task_queue,
            None,
            &cancel_event,
            WorkflowMailboxKind::CancelRequest,
            None,
            None,
            None,
        )
        .await?;
    let target_run_key = RunKey {
        tenant_id: event.tenant_id.clone(),
        instance_id: target_instance.instance_id.clone(),
        run_id: target_instance.run_id.clone(),
    };
    let is_hot = {
        let inner = state.inner.lock().expect("unified runtime lock poisoned");
        inner.instances.contains_key(&target_run_key)
    };
    if is_hot {
        drain_mailbox_for_run(
            state,
            &target_run_key.tenant_id,
            &target_run_key.instance_id,
            &target_run_key.run_id,
        )
        .await?;
    }
    Ok(())
}

async fn handle_mailbox_queue_event(
    state: &AppState,
    event: EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    let run_key = RunKey {
        tenant_id: event.tenant_id.clone(),
        instance_id: event.instance_id.clone(),
        run_id: event.run_id.clone(),
    };
    let (mailbox_kind, message_id, message_name, payload) = match &event.payload {
        WorkflowEvent::SignalQueued { signal_id, signal_type, payload } => (
            WorkflowMailboxKind::Signal,
            Some(signal_id.as_str()),
            Some(signal_type.as_str()),
            Some(payload),
        ),
        WorkflowEvent::WorkflowUpdateRequested { update_id, update_name, payload } => (
            WorkflowMailboxKind::Update,
            Some(update_id.as_str()),
            Some(update_name.as_str()),
            Some(payload),
        ),
        WorkflowEvent::WorkflowCancellationRequested { .. } => {
            (WorkflowMailboxKind::CancelRequest, None, None, None)
        }
        _ => return Ok(()),
    };
    let hot_task_queue = {
        let inner = state.inner.lock().expect("unified runtime lock poisoned");
        inner.instances.get(&run_key).map(|runtime| runtime.instance.workflow_task_queue.clone())
    };
    let task_queue = if let Some(task_queue) = hot_task_queue {
        Some(task_queue)
    } else {
        state
            .store
            .get_instance(&event.tenant_id, &event.instance_id)
            .await?
            .map(|instance| instance.workflow_task_queue)
    };
    let Some(task_queue) = task_queue else {
        return Ok(());
    };
    state
        .store
        .enqueue_workflow_mailbox_message(
            workflow_partition_id(state, &event.tenant_id, &event.instance_id),
            &task_queue,
            None,
            &event,
            mailbox_kind,
            message_id,
            message_name,
            payload,
        )
        .await?;
    let should_drain = {
        let inner = state.inner.lock().expect("unified runtime lock poisoned");
        inner.instances.contains_key(&run_key)
    };
    if !should_drain {
        return Ok(());
    }
    drain_mailbox_for_run(state, &run_key.tenant_id, &run_key.instance_id, &run_key.run_id).await
}

enum MailboxDrainOutcome {
    Processed {
        signal: Option<ConsumedSignalRecord>,
        accepted_seq: u64,
        emitted_events: Vec<EventEnvelope<WorkflowEvent>>,
        general: Vec<PreparedDbAction>,
        schedules: Vec<PreparedDbAction>,
        notifies: bool,
        post_plan: Option<PostPlanEffect>,
        terminal_child: Option<(RunKey, WorkflowInstanceState)>,
    },
    ConsumedNoop {
        accepted_seq: u64,
    },
    Blocked,
}

#[derive(Debug, Clone)]
struct PostPlanEffect {
    run_key: RunKey,
    artifact: CompiledWorkflowArtifact,
    instance: WorkflowInstanceState,
    plan: CompiledExecutionPlan,
    source_event_id: Uuid,
    occurred_at: DateTime<Utc>,
}

async fn drain_mailbox_for_run(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
) -> Result<()> {
    let mut consumed_signals = Vec::new();
    let mut max_consumed_mailbox_seq = None;
    loop {
        let items = state
            .store
            .list_next_workflow_mailbox_items(tenant_id, instance_id, run_id, 32)
            .await?;
        if items.is_empty() {
            break;
        }
        let mut blocked = false;
        for item in items {
            match dispatch_mailbox_item_unified(state, &item).await? {
                MailboxDrainOutcome::Processed {
                    signal,
                    accepted_seq,
                    emitted_events,
                    general,
                    schedules,
                    notifies,
                    post_plan,
                    terminal_child,
                } => {
                    if let Some(signal) = signal {
                        consumed_signals.push(signal);
                    }
                    publish_history_events(state, &emitted_events).await?;
                    apply_db_actions(state, general, schedules).await?;
                    if let Some(post_plan) = post_plan {
                        let post_plan_run_key = post_plan.run_key.clone();
                        let post_plan_event_id = post_plan.source_event_id;
                        let post_plan_occurred_at = post_plan.occurred_at;
                        apply_post_plan_effects(state, post_plan).await?;
                        maybe_enact_pending_workflow_cancellation_unified(
                            state,
                            &post_plan_run_key,
                            post_plan_event_id,
                            post_plan_occurred_at,
                        )
                        .await?;
                    }
                    if let Some(terminal_child) = terminal_child {
                        let (general, schedules, child_notifies) =
                            reflect_terminal_children_to_parents(state, vec![terminal_child])
                                .await?;
                        if !general.is_empty() || !schedules.is_empty() {
                            apply_db_actions(state, general, schedules).await?;
                        }
                        if child_notifies {
                            state.notify.notify_waiters();
                        }
                    }
                    let run_key = RunKey {
                        tenant_id: tenant_id.to_owned(),
                        instance_id: instance_id.to_owned(),
                        run_id: run_id.to_owned(),
                    };
                    maybe_enact_pending_workflow_cancellation_unified(
                        state,
                        &run_key,
                        item.source_event.event_id,
                        item.source_event.occurred_at,
                    )
                    .await?;
                    if notifies {
                        state.notify.notify_waiters();
                    }
                    state.persist_notify.notify_one();
                    max_consumed_mailbox_seq = Some(accepted_seq);
                }
                MailboxDrainOutcome::ConsumedNoop { accepted_seq } => {
                    max_consumed_mailbox_seq = Some(accepted_seq);
                }
                MailboxDrainOutcome::Blocked => {
                    blocked = true;
                    break;
                }
            }
        }
        if !consumed_signals.is_empty() {
            state
                .store
                .mark_signals_consumed(tenant_id, instance_id, run_id, &consumed_signals)
                .await?;
            consumed_signals.clear();
        }
        if let Some(max_mailbox_seq) = max_consumed_mailbox_seq.take() {
            state
                .store
                .mark_workflow_mailbox_items_consumed_through(
                    tenant_id,
                    instance_id,
                    run_id,
                    max_mailbox_seq,
                    Utc::now(),
                )
                .await?;
        }
        if blocked {
            break;
        }
    }
    Ok(())
}

async fn dispatch_mailbox_item_unified(
    state: &AppState,
    item: &WorkflowMailboxRecord,
) -> Result<MailboxDrainOutcome> {
    match item.kind {
        WorkflowMailboxKind::Trigger => {
            Ok(MailboxDrainOutcome::ConsumedNoop { accepted_seq: item.accepted_seq })
        }
        WorkflowMailboxKind::Signal => dispatch_signal_mailbox_item_unified(state, item).await,
        WorkflowMailboxKind::Update => dispatch_update_mailbox_item_unified(state, item).await,
        WorkflowMailboxKind::CancelRequest => {
            dispatch_cancel_mailbox_item_unified(state, item).await
        }
    }
}

fn unified_signal_dispatch_event_id(source_event_id: Uuid) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("unified-signal-dispatch:{source_event_id}").as_bytes(),
    )
}

async fn dispatch_signal_mailbox_item_unified(
    state: &AppState,
    item: &WorkflowMailboxRecord,
) -> Result<MailboxDrainOutcome> {
    let Some(message_id) = item.message_id.as_deref() else {
        return Ok(MailboxDrainOutcome::ConsumedNoop { accepted_seq: item.accepted_seq });
    };
    let signal_name = item.message_name.clone().unwrap_or_default();
    let payload = item.payload.clone().unwrap_or(Value::Null);
    let run_key = RunKey {
        tenant_id: item.tenant_id.clone(),
        instance_id: item.instance_id.clone(),
        run_id: item.run_id.clone(),
    };

    let (artifact, instance) = {
        let inner = state.inner.lock().expect("unified runtime lock poisoned");
        let Some(runtime) = inner.instances.get(&run_key) else {
            return Ok(MailboxDrainOutcome::ConsumedNoop { accepted_seq: item.accepted_seq });
        };
        (runtime.artifact.clone(), runtime.instance.clone())
    };
    if instance.status.is_terminal() {
        return Ok(MailboxDrainOutcome::ConsumedNoop { accepted_seq: item.accepted_seq });
    }
    let current_state =
        instance.current_state.clone().unwrap_or_else(|| artifact.workflow.initial_state.clone());
    let now = Utc::now();
    let signal_event = EventEnvelope::new(
        WorkflowEvent::SignalReceived {
            signal_id: message_id.to_owned(),
            signal_type: signal_name.clone(),
            payload: payload.clone(),
        }
        .event_type(),
        WorkflowIdentity::new(
            item.tenant_id.clone(),
            item.source_event.definition_id.clone(),
            item.source_event.definition_version,
            item.source_event.artifact_hash.clone(),
            item.instance_id.clone(),
            item.run_id.clone(),
            "unified-runtime",
        ),
        WorkflowEvent::SignalReceived {
            signal_id: message_id.to_owned(),
            signal_type: signal_name.clone(),
            payload: payload.clone(),
        },
    )
    .with_occurred_at(now);
    let mut signal_event = signal_event;
    signal_event.event_id = unified_signal_dispatch_event_id(item.source_event.event_id);
    signal_event.causation_id = Some(item.source_event.event_id);
    signal_event.correlation_id =
        item.source_event.correlation_id.or(Some(item.source_event.event_id));
    signal_event.dedupe_key = Some(format!("signal:{message_id}"));
    let execution_state = execution_state_for_event(&instance, Some(&signal_event));
    if execution_state.active_update.is_some() || execution_state.active_signal.is_some() {
        return Ok(MailboxDrainOutcome::Blocked);
    }

    let plan = if artifact
        .expected_signal_type(&current_state)?
        .is_some_and(|expected| expected == signal_name)
    {
        artifact.execute_after_signal_with_turn(
            &current_state,
            &signal_name,
            &payload,
            execution_state,
            ExecutionTurnContext {
                event_id: signal_event.event_id,
                occurred_at: signal_event.occurred_at,
            },
        )?
    } else if artifact.has_signal_handler(&signal_name) {
        artifact.execute_signal_handler_with_turn(
            &current_state,
            message_id,
            &signal_name,
            &payload,
            execution_state,
            ExecutionTurnContext {
                event_id: signal_event.event_id,
                occurred_at: signal_event.occurred_at,
            },
        )?
    } else {
        return Ok(MailboxDrainOutcome::Blocked);
    };

    let mut general = Vec::new();
    let mut schedules = Vec::new();
    let mut notifies = false;
    let final_instance;
    {
        let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
        let Some(runtime) = inner.instances.get_mut(&run_key) else {
            return Ok(MailboxDrainOutcome::ConsumedNoop { accepted_seq: item.accepted_seq });
        };
        runtime.instance.apply_event(&signal_event);
        apply_compiled_plan(&mut runtime.instance, &plan);
        let scheduled_tasks = schedule_activities_from_plan(
            &runtime.artifact,
            &runtime.instance,
            &plan,
            signal_event.occurred_at,
        )?;
        for task in &scheduled_tasks {
            schedules.push(PreparedDbAction::Schedule(task.clone()));
            runtime.active_activities.insert(
                task.activity_id.clone(),
                ActiveActivityMeta {
                    attempt: task.attempt,
                    task_queue: task.task_queue.clone(),
                    activity_type: task.activity_type.clone(),
                    wait_state: task.state.clone(),
                    omit_success_output: task.omit_success_output,
                },
            );
        }
        let terminal_instance =
            runtime.instance.status.is_terminal().then_some(runtime.instance.clone());
        final_instance = Some(runtime.instance.clone());
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
            general.push(PreparedDbAction::CloseRun(run_key.clone(), signal_event.occurred_at));
        }
        mark_runtime_dirty(&mut inner, "mailbox_signal", false);
    }

    Ok(MailboxDrainOutcome::Processed {
        signal: Some(ConsumedSignalRecord {
            signal_id: message_id.to_owned(),
            consumed_event_id: signal_event.event_id,
            consumed_at: signal_event.occurred_at,
        }),
        accepted_seq: item.accepted_seq,
        emitted_events: vec![signal_event.clone()],
        general,
        schedules,
        notifies,
        post_plan: final_instance.map(|instance| PostPlanEffect {
            run_key,
            artifact: artifact.clone(),
            instance,
            plan,
            source_event_id: signal_event.event_id,
            occurred_at: signal_event.occurred_at,
        }),
        terminal_child: None,
    })
}

fn unified_cancelled_event_id(source_event_id: Uuid) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, format!("unified-cancelled:{source_event_id}").as_bytes())
}

async fn dispatch_cancel_mailbox_item_unified(
    state: &AppState,
    item: &WorkflowMailboxRecord,
) -> Result<MailboxDrainOutcome> {
    let run_key = RunKey {
        tenant_id: item.tenant_id.clone(),
        instance_id: item.instance_id.clone(),
        run_id: item.run_id.clone(),
    };
    let reason = match &item.source_event.payload {
        WorkflowEvent::WorkflowCancellationRequested { reason } => reason.clone(),
        _ => {
            item.payload.as_ref().and_then(Value::as_str).unwrap_or("workflow cancelled").to_owned()
        }
    };
    let deferred = {
        let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
        let Some(runtime) = inner.instances.get_mut(&run_key) else {
            return Ok(MailboxDrainOutcome::ConsumedNoop { accepted_seq: item.accepted_seq });
        };
        if runtime.instance.status.is_terminal() {
            return Ok(MailboxDrainOutcome::ConsumedNoop { accepted_seq: item.accepted_seq });
        }
        let current_state = runtime
            .instance
            .current_state
            .clone()
            .unwrap_or_else(|| runtime.artifact.workflow.initial_state.clone());
        runtime.instance.apply_event(&item.source_event);
        if runtime.artifact.is_non_cancellable_state(&current_state) {
            if let Some(execution) = runtime.instance.artifact_execution.as_mut() {
                execution.pending_workflow_cancellation = Some(reason.clone());
            }
            let instance = runtime.instance.clone();
            mark_runtime_dirty(&mut inner, "mailbox_cancel_deferred", false);
            Some(instance)
        } else {
            None
        }
    };
    if let Some(instance) = deferred {
        state.store.upsert_instance(&instance).await?;
        return Ok(MailboxDrainOutcome::Processed {
            signal: None,
            accepted_seq: item.accepted_seq,
            emitted_events: Vec::new(),
            general: Vec::new(),
            schedules: Vec::new(),
            notifies: false,
            post_plan: None,
            terminal_child: None,
        });
    }

    propagate_cancellation_to_open_children_unified(state, &item.source_event, &reason).await?;

    let unwindable_activities = {
        let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
        let Some(runtime) = inner.instances.get_mut(&run_key) else {
            return Ok(MailboxDrainOutcome::ConsumedNoop { accepted_seq: item.accepted_seq });
        };
        if let Some(execution) = runtime.instance.artifact_execution.as_mut() {
            execution.pending_workflow_cancellation = None;
        }
        let unwindable = runtime.instance.artifact_execution.is_some()
            && !runtime.active_activities.is_empty()
            && runtime.active_activities.values().all(|active| active.attempt > 0);
        let activities = if unwindable {
            runtime
                .active_activities
                .iter()
                .map(|(activity_id, active)| {
                    (activity_id.clone(), active.attempt, reason.clone(), None::<Value>)
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let instance = runtime.instance.clone();
        mark_runtime_dirty(
            &mut inner,
            if unwindable { "mailbox_cancel_requested" } else { "mailbox_cancel" },
            !unwindable,
        );
        (instance, activities)
    };

    let (instance_after_request, cancellations) = unwindable_activities;
    if !cancellations.is_empty() {
        state.store.upsert_instance(&instance_after_request).await?;
        for (activity_id, attempt, reason, metadata) in cancellations {
            let payload = WorkflowEvent::ActivityTaskCancellationRequested {
                activity_id,
                attempt,
                reason,
                metadata,
            };
            let mut cancel_event = EventEnvelope::new(
                payload.event_type(),
                WorkflowIdentity::new(
                    item.tenant_id.clone(),
                    item.source_event.definition_id.clone(),
                    item.source_event.definition_version,
                    item.source_event.artifact_hash.clone(),
                    item.instance_id.clone(),
                    item.run_id.clone(),
                    "unified-runtime",
                ),
                payload,
            );
            cancel_event.occurred_at = item.source_event.occurred_at;
            cancel_event.causation_id = Some(item.source_event.event_id);
            cancel_event.correlation_id =
                item.source_event.correlation_id.or(Some(item.source_event.event_id));
            handle_activity_cancellation_requested_event(state, cancel_event).await?;
        }
        return Ok(MailboxDrainOutcome::Processed {
            signal: None,
            accepted_seq: item.accepted_seq,
            emitted_events: Vec::new(),
            general: Vec::new(),
            schedules: Vec::new(),
            notifies: false,
            post_plan: None,
            terminal_child: None,
        });
    }

    let now = Utc::now();
    let mut cancelled_event = EventEnvelope::new(
        WorkflowEvent::WorkflowCancelled { reason: reason.clone() }.event_type(),
        WorkflowIdentity::new(
            item.tenant_id.clone(),
            item.source_event.definition_id.clone(),
            item.source_event.definition_version,
            item.source_event.artifact_hash.clone(),
            item.instance_id.clone(),
            item.run_id.clone(),
            "unified-runtime",
        ),
        WorkflowEvent::WorkflowCancelled { reason },
    )
    .with_occurred_at(now);
    cancelled_event.event_id = unified_cancelled_event_id(item.source_event.event_id);
    cancelled_event.causation_id = Some(item.source_event.event_id);
    cancelled_event.correlation_id =
        item.source_event.correlation_id.or(Some(item.source_event.event_id));
    cancelled_event.dedupe_key = Some(format!("workflow-cancelled:{}", item.source_event.event_id));

    let terminal_instance = {
        let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
        let runtime = inner.instances.get_mut(&run_key).expect("runtime still exists");
        runtime.instance.apply_event(&cancelled_event);
        if let Some(execution) = runtime.instance.artifact_execution.as_mut() {
            execution.active_update = None;
            execution.active_signal = None;
        }
        runtime.active_activities.clear();
        let terminal_instance = runtime.instance.clone();
        remove_run_work(&mut inner, &run_key);
        terminal_instance
    };

    Ok(MailboxDrainOutcome::Processed {
        signal: None,
        accepted_seq: item.accepted_seq,
        emitted_events: vec![cancelled_event.clone()],
        general: vec![
            PreparedDbAction::UpsertInstance(terminal_instance.clone()),
            PreparedDbAction::CloseRun(run_key, cancelled_event.occurred_at),
        ],
        schedules: Vec::new(),
        notifies: true,
        post_plan: None,
        terminal_child: Some((
            RunKey {
                tenant_id: item.tenant_id.clone(),
                instance_id: item.instance_id.clone(),
                run_id: item.run_id.clone(),
            },
            terminal_instance,
        )),
    })
}

async fn maybe_enact_pending_workflow_cancellation_unified(
    state: &AppState,
    run_key: &RunKey,
    source_event_id: Uuid,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    let (source_event, instance_after_request, cancellations) = {
        let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
        let Some(runtime) = inner.instances.get_mut(run_key) else {
            return Ok(());
        };
        if runtime.instance.status.is_terminal() {
            return Ok(());
        }
        let Some(execution) = runtime.instance.artifact_execution.as_mut() else {
            return Ok(());
        };
        let current_state = runtime
            .instance
            .current_state
            .clone()
            .unwrap_or_else(|| runtime.artifact.workflow.initial_state.clone());
        let Some(reason) = runtime
            .artifact
            .pending_workflow_cancellation_ready(&current_state, execution)
            .map(str::to_owned)
        else {
            return Ok(());
        };
        execution.pending_workflow_cancellation = None;
        let identity = WorkflowIdentity::new(
            runtime.instance.tenant_id.clone(),
            runtime.instance.definition_id.clone(),
            runtime.instance.definition_version.unwrap_or(runtime.artifact.definition_version),
            runtime.instance.artifact_hash.clone().unwrap_or_default(),
            runtime.instance.instance_id.clone(),
            runtime.instance.run_id.clone(),
            "unified-runtime",
        );
        let mut source_event = EventEnvelope::new(
            WorkflowEvent::WorkflowCancellationRequested { reason: reason.clone() }.event_type(),
            identity,
            WorkflowEvent::WorkflowCancellationRequested { reason: reason.clone() },
        )
        .with_occurred_at(occurred_at);
        source_event.event_id = source_event_id;
        source_event.correlation_id = Some(source_event_id);
        let cancellations = runtime
            .active_activities
            .iter()
            .map(|(activity_id, active)| {
                (activity_id.clone(), active.attempt, reason.clone(), None::<Value>)
            })
            .collect::<Vec<_>>();
        let instance = runtime.instance.clone();
        mark_runtime_dirty(
            &mut inner,
            if cancellations.is_empty() {
                "pending_cancel_finalize"
            } else {
                "pending_cancel_unwind"
            },
            cancellations.is_empty(),
        );
        (source_event, instance, cancellations)
    };

    let reason = match &source_event.payload {
        WorkflowEvent::WorkflowCancellationRequested { reason } => reason.clone(),
        _ => return Ok(()),
    };
    propagate_cancellation_to_open_children_unified(state, &source_event, &reason).await?;

    if !cancellations.is_empty() {
        state.store.upsert_instance(&instance_after_request).await?;
        for (activity_id, attempt, reason, metadata) in cancellations {
            let payload = WorkflowEvent::ActivityTaskCancellationRequested {
                activity_id,
                attempt,
                reason,
                metadata,
            };
            let mut cancel_event = EventEnvelope::new(
                payload.event_type(),
                WorkflowIdentity::new(
                    run_key.tenant_id.clone(),
                    source_event.definition_id.clone(),
                    source_event.definition_version,
                    source_event.artifact_hash.clone(),
                    run_key.instance_id.clone(),
                    run_key.run_id.clone(),
                    "unified-runtime",
                ),
                payload,
            );
            cancel_event.occurred_at = source_event.occurred_at;
            cancel_event.causation_id = Some(source_event.event_id);
            cancel_event.correlation_id = Some(source_event.event_id);
            handle_activity_cancellation_requested_event(state, cancel_event).await?;
        }
        return Ok(());
    }

    let mut cancelled_event = EventEnvelope::new(
        WorkflowEvent::WorkflowCancelled { reason: reason.clone() }.event_type(),
        WorkflowIdentity::new(
            run_key.tenant_id.clone(),
            source_event.definition_id.clone(),
            source_event.definition_version,
            source_event.artifact_hash.clone(),
            run_key.instance_id.clone(),
            run_key.run_id.clone(),
            "unified-runtime",
        ),
        WorkflowEvent::WorkflowCancelled { reason },
    )
    .with_occurred_at(occurred_at);
    cancelled_event.event_id = unified_cancelled_event_id(source_event.event_id);
    cancelled_event.causation_id = Some(source_event.event_id);
    cancelled_event.correlation_id = Some(source_event.event_id);
    cancelled_event.dedupe_key = Some(format!("workflow-cancelled:{}", source_event.event_id));

    let terminal_instance = {
        let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
        let Some(runtime) = inner.instances.get_mut(run_key) else {
            return Ok(());
        };
        runtime.instance.apply_event(&cancelled_event);
        if let Some(execution) = runtime.instance.artifact_execution.as_mut() {
            execution.active_update = None;
            execution.active_signal = None;
            execution.pending_workflow_cancellation = None;
        }
        runtime.active_activities.clear();
        let terminal_instance = runtime.instance.clone();
        remove_run_work(&mut inner, run_key);
        terminal_instance
    };
    apply_db_actions(
        state,
        vec![
            PreparedDbAction::UpsertInstance(terminal_instance),
            PreparedDbAction::CloseRun(run_key.clone(), cancelled_event.occurred_at),
        ],
        Vec::new(),
    )
    .await?;
    state.notify.notify_waiters();
    state.persist_notify.notify_one();
    Ok(())
}

async fn propagate_cancellation_to_open_children_unified(
    state: &AppState,
    source_event: &EventEnvelope<WorkflowEvent>,
    reason: &str,
) -> Result<()> {
    let children = state
        .store
        .list_open_children_for_run(
            &source_event.tenant_id,
            &source_event.instance_id,
            &source_event.run_id,
        )
        .await?;
    if children.is_empty() {
        return Ok(());
    }
    for child in children {
        let partition_id = workflow_partition_id(state, &child.tenant_id, &child.child_workflow_id);
        let Some(child_run_id) = child.child_run_id.clone() else {
            state
                .store
                .complete_child(
                    &child.tenant_id,
                    &child.instance_id,
                    &child.run_id,
                    &child.child_id,
                    "cancelled",
                    None,
                    Some("cancel requested before child start"),
                    source_event.event_id,
                    source_event.occurred_at,
                )
                .await?;
            continue;
        };

        let hot_child_instance = {
            let inner = state.inner.lock().expect("unified runtime lock poisoned");
            inner
                .instances
                .get(&RunKey {
                    tenant_id: child.tenant_id.clone(),
                    instance_id: child.child_workflow_id.clone(),
                    run_id: child_run_id.clone(),
                })
                .map(|runtime| runtime.instance.clone())
        };
        let stored_child_instance = if hot_child_instance.is_none() {
            state.store.get_instance(&child.tenant_id, &child.child_workflow_id).await?
        } else {
            None
        };
        let child_task_queue = hot_child_instance
            .as_ref()
            .map(|instance| instance.workflow_task_queue.clone())
            .or_else(|| {
                stored_child_instance.as_ref().map(|instance| instance.workflow_task_queue.clone())
            })
            .unwrap_or_else(|| "default".to_owned());
        let child_definition_version = hot_child_instance
            .as_ref()
            .and_then(|instance| instance.definition_version)
            .or_else(|| {
                stored_child_instance.as_ref().and_then(|instance| instance.definition_version)
            })
            .unwrap_or(source_event.definition_version);
        let child_artifact_hash = hot_child_instance
            .as_ref()
            .and_then(|instance| instance.artifact_hash.clone())
            .or_else(|| {
                stored_child_instance.as_ref().and_then(|instance| instance.artifact_hash.clone())
            })
            .unwrap_or_else(|| source_event.artifact_hash.clone());

        let mut cancel_event = EventEnvelope::new(
            WorkflowEvent::WorkflowCancellationRequested {
                reason: format!("{reason} (propagated from parent run {})", source_event.run_id),
            }
            .event_type(),
            WorkflowIdentity::new(
                child.tenant_id.clone(),
                child.child_definition_id.clone(),
                child_definition_version,
                child_artifact_hash,
                child.child_workflow_id.clone(),
                child_run_id.clone(),
                "unified-runtime",
            ),
            WorkflowEvent::WorkflowCancellationRequested {
                reason: format!("{reason} (propagated from parent run {})", source_event.run_id),
            },
        )
        .with_occurred_at(source_event.occurred_at);
        cancel_event.causation_id = Some(source_event.event_id);
        cancel_event.correlation_id = source_event.correlation_id.or(Some(source_event.event_id));
        cancel_event.dedupe_key =
            Some(format!("child-cancel:{}:{}", child.child_id, source_event.event_id));
        state
            .store
            .enqueue_workflow_mailbox_message(
                partition_id,
                &child_task_queue,
                None,
                &cancel_event,
                WorkflowMailboxKind::CancelRequest,
                None,
                None,
                None,
            )
            .await?;
        let child_run_key = RunKey {
            tenant_id: child.tenant_id.clone(),
            instance_id: child.child_workflow_id.clone(),
            run_id: child_run_id,
        };
        let is_hot = {
            let inner = state.inner.lock().expect("unified runtime lock poisoned");
            inner.instances.contains_key(&child_run_key)
        };
        if is_hot {
            Box::pin(drain_mailbox_for_run(
                state,
                &child_run_key.tenant_id,
                &child_run_key.instance_id,
                &child_run_key.run_id,
            ))
            .await?;
        }
    }
    Ok(())
}

async fn dispatch_update_mailbox_item_unified(
    state: &AppState,
    item: &WorkflowMailboxRecord,
) -> Result<MailboxDrainOutcome> {
    let Some(message_id) = item.message_id.as_deref() else {
        return Ok(MailboxDrainOutcome::ConsumedNoop { accepted_seq: item.accepted_seq });
    };
    let run_key = RunKey {
        tenant_id: item.tenant_id.clone(),
        instance_id: item.instance_id.clone(),
        run_id: item.run_id.clone(),
    };

    let (artifact, instance) = {
        let inner = state.inner.lock().expect("unified runtime lock poisoned");
        let Some(runtime) = inner.instances.get(&run_key) else {
            return Ok(MailboxDrainOutcome::ConsumedNoop { accepted_seq: item.accepted_seq });
        };
        (runtime.artifact.clone(), runtime.instance.clone())
    };
    let Some(update) = state
        .store
        .get_update(&item.tenant_id, &item.instance_id, &item.run_id, message_id)
        .await?
    else {
        return Ok(MailboxDrainOutcome::ConsumedNoop { accepted_seq: item.accepted_seq });
    };

    if instance.status.is_terminal() {
        let error = format!("workflow run {} is already {}", item.run_id, instance.status.as_str());
        let completed_event_id = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!("unified-update-terminal-reject:{}", update.update_id).as_bytes(),
        );
        state
            .store
            .complete_update(
                &update.tenant_id,
                &update.instance_id,
                &update.run_id,
                &update.update_id,
                None,
                Some(&error),
                completed_event_id,
                Utc::now(),
            )
            .await?;
        return Ok(MailboxDrainOutcome::ConsumedNoop { accepted_seq: item.accepted_seq });
    }

    let execution_state = execution_state_for_event(&instance, None);
    if execution_state.active_update.is_some() || execution_state.active_signal.is_some() {
        return Ok(MailboxDrainOutcome::Blocked);
    }
    if !artifact.has_update(&update.update_name) {
        let error = format!("unknown update handler {}", update.update_name);
        let completed_event_id = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!("unified-update-unknown-reject:{}", update.update_id).as_bytes(),
        );
        state
            .store
            .complete_update(
                &update.tenant_id,
                &update.instance_id,
                &update.run_id,
                &update.update_id,
                None,
                Some(&error),
                completed_event_id,
                Utc::now(),
            )
            .await?;
        return Ok(MailboxDrainOutcome::ConsumedNoop { accepted_seq: item.accepted_seq });
    }

    let accepted_event_id = Uuid::now_v7();
    if !state
        .store
        .accept_update(
            &update.tenant_id,
            &update.instance_id,
            &update.run_id,
            &update.update_id,
            accepted_event_id,
            Utc::now(),
        )
        .await?
    {
        return Ok(MailboxDrainOutcome::ConsumedNoop { accepted_seq: item.accepted_seq });
    }

    let current_state =
        instance.current_state.clone().unwrap_or_else(|| artifact.workflow.initial_state.clone());
    let mut accepted_event = EventEnvelope::new(
        WorkflowEvent::WorkflowUpdateAccepted {
            update_id: update.update_id.clone(),
            update_name: update.update_name.clone(),
            payload: update.payload.clone(),
        }
        .event_type(),
        WorkflowIdentity::new(
            item.tenant_id.clone(),
            item.source_event.definition_id.clone(),
            item.source_event.definition_version,
            item.source_event.artifact_hash.clone(),
            item.instance_id.clone(),
            item.run_id.clone(),
            "unified-runtime",
        ),
        WorkflowEvent::WorkflowUpdateAccepted {
            update_id: update.update_id.clone(),
            update_name: update.update_name.clone(),
            payload: update.payload.clone(),
        },
    )
    .with_occurred_at(Utc::now());
    accepted_event.event_id = accepted_event_id;
    accepted_event.causation_id = Some(update.source_event_id);
    accepted_event.correlation_id =
        item.source_event.correlation_id.or(Some(update.source_event_id));
    accepted_event.dedupe_key = Some(format!("update-accepted:{}", update.update_id));

    let plan = artifact.execute_update_with_turn(
        &current_state,
        &update.update_id,
        &update.update_name,
        &update.payload,
        execution_state,
        ExecutionTurnContext {
            event_id: accepted_event.event_id,
            occurred_at: accepted_event.occurred_at,
        },
    )?;

    let mut general = Vec::new();
    let mut schedules = Vec::new();
    let mut notifies = false;
    let final_instance;
    {
        let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
        let Some(runtime) = inner.instances.get_mut(&run_key) else {
            return Ok(MailboxDrainOutcome::ConsumedNoop { accepted_seq: item.accepted_seq });
        };
        runtime.instance.apply_event(&accepted_event);
        apply_compiled_plan(&mut runtime.instance, &plan);
        let scheduled_tasks = schedule_activities_from_plan(
            &runtime.artifact,
            &runtime.instance,
            &plan,
            accepted_event.occurred_at,
        )?;
        for task in &scheduled_tasks {
            schedules.push(PreparedDbAction::Schedule(task.clone()));
            runtime.active_activities.insert(
                task.activity_id.clone(),
                ActiveActivityMeta {
                    attempt: task.attempt,
                    task_queue: task.task_queue.clone(),
                    activity_type: task.activity_type.clone(),
                    wait_state: task.state.clone(),
                    omit_success_output: task.omit_success_output,
                },
            );
        }
        general.extend(prepared_update_actions_from_plan(
            &run_key,
            &plan,
            accepted_event.event_id,
            accepted_event.occurred_at,
        ));
        let terminal_instance =
            runtime.instance.status.is_terminal().then_some(runtime.instance.clone());
        final_instance = Some(runtime.instance.clone());
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
            general.push(PreparedDbAction::CloseRun(run_key.clone(), accepted_event.occurred_at));
        }
        mark_runtime_dirty(&mut inner, "mailbox_update", false);
    }

    Ok(MailboxDrainOutcome::Processed {
        signal: None,
        accepted_seq: item.accepted_seq,
        emitted_events: vec![accepted_event.clone()],
        general,
        schedules,
        notifies,
        post_plan: final_instance.map(|instance| PostPlanEffect {
            run_key,
            artifact: artifact.clone(),
            instance,
            plan,
            source_event_id: accepted_event.event_id,
            occurred_at: accepted_event.occurred_at,
        }),
        terminal_child: None,
    })
}

async fn materialize_child_workflows_from_plan(
    state: &AppState,
    parent_instance: &WorkflowInstanceState,
    plan: &CompiledExecutionPlan,
    occurred_at: DateTime<Utc>,
    source_event_id: Uuid,
) -> Result<(
    Vec<PreparedDbAction>,
    Vec<PreparedDbAction>,
    bool,
    Vec<(RunKey, WorkflowInstanceState)>,
)> {
    let mut general = Vec::new();
    let mut schedules = Vec::new();
    let mut notifies = false;
    let mut terminal_children = Vec::new();

    for emission in &plan.emissions {
        let WorkflowEvent::ChildWorkflowStartRequested {
            child_id,
            child_workflow_id,
            child_definition_id,
            input,
            task_queue,
            parent_close_policy,
        } = &emission.event
        else {
            continue;
        };

        let artifact = state
            .store
            .get_latest_artifact(&parent_instance.tenant_id, child_definition_id)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("child workflow definition {child_definition_id} not found")
            })?;
        let child_run_id = format!("run-{}", Uuid::now_v7());
        let workflow_task_queue =
            task_queue.clone().unwrap_or_else(|| parent_instance.workflow_task_queue.clone());
        let mut child_trigger = EventEnvelope::new(
            WorkflowEvent::WorkflowTriggered { input: input.clone() }.event_type(),
            WorkflowIdentity::new(
                parent_instance.tenant_id.clone(),
                child_definition_id.clone(),
                artifact.definition_version,
                artifact.artifact_hash.clone(),
                child_workflow_id.clone(),
                child_run_id.clone(),
                "unified-runtime",
            ),
            WorkflowEvent::WorkflowTriggered { input: input.clone() },
        )
        .with_occurred_at(occurred_at);
        child_trigger.event_id = unified_child_trigger_event_id(source_event_id, child_id);
        child_trigger.causation_id = Some(source_event_id);
        child_trigger.correlation_id = Some(source_event_id);
        child_trigger
            .metadata
            .insert("workflow_task_queue".to_owned(), workflow_task_queue.clone());
        child_trigger
            .metadata
            .insert("parent_instance_id".to_owned(), parent_instance.instance_id.clone());
        child_trigger.metadata.insert("parent_run_id".to_owned(), parent_instance.run_id.clone());
        child_trigger.metadata.insert("parent_child_id".to_owned(), child_id.clone());

        let child_plan = artifact.execute_trigger_with_turn(
            input,
            ExecutionTurnContext {
                event_id: child_trigger.event_id,
                occurred_at: child_trigger.occurred_at,
            },
        )?;
        let mut child_instance = WorkflowInstanceState::try_from(&child_trigger)?;
        apply_compiled_plan(&mut child_instance, &child_plan);
        let child_tasks = schedule_activities_from_plan(
            &artifact,
            &child_instance,
            &child_plan,
            child_trigger.occurred_at,
        )?;
        let child_run_key = RunKey {
            tenant_id: child_instance.tenant_id.clone(),
            instance_id: child_instance.instance_id.clone(),
            run_id: child_instance.run_id.clone(),
        };

        general.push(PreparedDbAction::PutRunStart {
            tenant_id: child_instance.tenant_id.clone(),
            instance_id: child_instance.instance_id.clone(),
            run_id: child_instance.run_id.clone(),
            definition_id: child_instance.definition_id.clone(),
            definition_version: child_instance.definition_version,
            artifact_hash: child_instance.artifact_hash.clone(),
            workflow_task_queue: workflow_task_queue.clone(),
            memo: child_instance.memo.clone(),
            search_attributes: child_instance.search_attributes.clone(),
            trigger_event_id: child_trigger.event_id,
            started_at: child_trigger.occurred_at,
            previous_run_id: None,
            triggered_by_run_id: Some(parent_instance.run_id.clone()),
        });
        general.push(PreparedDbAction::UpsertChildStartRequested {
            tenant_id: parent_instance.tenant_id.clone(),
            instance_id: parent_instance.instance_id.clone(),
            run_id: parent_instance.run_id.clone(),
            child_id: child_id.clone(),
            child_workflow_id: child_workflow_id.clone(),
            child_definition_id: child_definition_id.clone(),
            parent_close_policy: parent_close_policy.clone(),
            input: input.clone(),
            source_event_id,
            occurred_at,
        });
        general.push(PreparedDbAction::MarkChildStarted {
            tenant_id: parent_instance.tenant_id.clone(),
            instance_id: parent_instance.instance_id.clone(),
            run_id: parent_instance.run_id.clone(),
            child_id: child_id.clone(),
            child_run_id: child_run_id.clone(),
            started_event_id: child_trigger.event_id,
            occurred_at,
        });
        general.push(PreparedDbAction::UpsertInstance(child_instance.clone()));
        if child_instance.status.is_terminal() {
            general
                .push(PreparedDbAction::CloseRun(child_run_key.clone(), child_instance.updated_at));
        }

        {
            let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
            inner.instances.insert(
                child_run_key.clone(),
                RuntimeWorkflowState {
                    artifact: artifact.clone(),
                    instance: child_instance.clone(),
                    active_activities: child_tasks
                        .iter()
                        .map(|task| {
                            (
                                task.activity_id.clone(),
                                ActiveActivityMeta {
                                    attempt: task.attempt,
                                    task_queue: task.task_queue.clone(),
                                    activity_type: task.activity_type.clone(),
                                    wait_state: task.state.clone(),
                                    omit_success_output: task.omit_success_output,
                                },
                            )
                        })
                        .collect(),
                },
            );
            for task in &child_tasks {
                inner
                    .ready
                    .entry(QueueKey {
                        tenant_id: task.tenant_id.clone(),
                        task_queue: task.task_queue.clone(),
                    })
                    .or_default()
                    .push_back(task.clone());
                schedules.push(PreparedDbAction::Schedule(task.clone()));
                notifies = true;
            }
            mark_runtime_dirty(&mut inner, "child_start", true);
        }

        if child_instance.status.is_terminal() {
            terminal_children.push((child_run_key, child_instance));
        }
    }

    Ok((general, schedules, notifies, terminal_children))
}

async fn reflect_terminal_children_to_parents(
    state: &AppState,
    initial_terminals: Vec<(RunKey, WorkflowInstanceState)>,
) -> Result<(Vec<PreparedDbAction>, Vec<PreparedDbAction>, bool)> {
    let mut pending = initial_terminals;
    let mut general = Vec::new();
    let mut schedules = Vec::new();
    let mut notifies = false;

    while let Some((child_run_key, child_instance)) = pending.pop() {
        let Some(parent_child) = state
            .store
            .find_parent_for_child_run(&child_run_key.tenant_id, &child_run_key.run_id)
            .await?
        else {
            continue;
        };

        let (status, output, error, payload_kind) = match child_instance.status {
            WorkflowStatus::Completed => {
                ("completed".to_owned(), child_instance.output.clone(), None, "completed")
            }
            WorkflowStatus::Failed => (
                "failed".to_owned(),
                None,
                child_instance
                    .output
                    .as_ref()
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| {
                        child_instance.context.as_ref().and_then(Value::as_str).map(str::to_owned)
                    })
                    .or_else(|| Some("child workflow failed".to_owned())),
                "failed",
            ),
            WorkflowStatus::Cancelled => (
                "cancelled".to_owned(),
                None,
                child_instance
                    .output
                    .as_ref()
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| {
                        child_instance.context.as_ref().and_then(Value::as_str).map(str::to_owned)
                    })
                    .or_else(|| Some("child workflow cancelled".to_owned())),
                "cancelled",
            ),
            WorkflowStatus::Terminated => (
                "terminated".to_owned(),
                None,
                child_instance
                    .output
                    .as_ref()
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| {
                        child_instance.context.as_ref().and_then(Value::as_str).map(str::to_owned)
                    })
                    .or_else(|| Some("child workflow terminated".to_owned())),
                "terminated",
            ),
            _ => continue,
        };

        general.push(PreparedDbAction::CompleteChild {
            tenant_id: parent_child.tenant_id.clone(),
            instance_id: parent_child.instance_id.clone(),
            run_id: parent_child.run_id.clone(),
            child_id: parent_child.child_id.clone(),
            status,
            output: output.clone(),
            error: error.clone(),
            terminal_event_id: child_instance.last_event_id,
            occurred_at: child_instance.updated_at,
        });

        let parent_run_key = RunKey {
            tenant_id: parent_child.tenant_id.clone(),
            instance_id: parent_child.instance_id.clone(),
            run_id: parent_child.run_id.clone(),
        };

        let maybe_parent = {
            let inner = state.inner.lock().expect("unified runtime lock poisoned");
            inner
                .instances
                .get(&parent_run_key)
                .map(|runtime| (runtime.artifact.clone(), runtime.instance.clone()))
        };
        let Some((artifact, parent_instance)) = maybe_parent else {
            continue;
        };
        if parent_instance.status.is_terminal() {
            continue;
        }
        let wait_state = parent_instance
            .current_state
            .clone()
            .unwrap_or_else(|| artifact.workflow.initial_state.clone());
        let reflection_event_id = unified_child_reflection_event_id(
            child_instance.last_event_id,
            &parent_run_key.run_id,
            &parent_child.child_id,
            payload_kind,
        );
        let reflection_payload = match payload_kind {
            "completed" => WorkflowEvent::ChildWorkflowCompleted {
                child_id: parent_child.child_id.clone(),
                child_run_id: child_run_key.run_id.clone(),
                output: output.clone().unwrap_or(Value::Null),
            },
            "failed" => WorkflowEvent::ChildWorkflowFailed {
                child_id: parent_child.child_id.clone(),
                child_run_id: child_run_key.run_id.clone(),
                error: error.clone().unwrap_or_else(|| "child workflow failed".to_owned()),
            },
            "cancelled" => WorkflowEvent::ChildWorkflowCancelled {
                child_id: parent_child.child_id.clone(),
                child_run_id: child_run_key.run_id.clone(),
                reason: error.clone().unwrap_or_else(|| "child workflow cancelled".to_owned()),
            },
            _ => WorkflowEvent::ChildWorkflowTerminated {
                child_id: parent_child.child_id.clone(),
                child_run_id: child_run_key.run_id.clone(),
                reason: error.clone().unwrap_or_else(|| "child workflow terminated".to_owned()),
            },
        };
        let reflection_event = EventEnvelope::new(
            reflection_payload.event_type(),
            WorkflowIdentity::new(
                parent_run_key.tenant_id.clone(),
                parent_instance.definition_id.clone(),
                parent_instance.definition_version.unwrap_or(artifact.definition_version),
                parent_instance
                    .artifact_hash
                    .clone()
                    .unwrap_or_else(|| artifact.artifact_hash.clone()),
                parent_run_key.instance_id.clone(),
                parent_run_key.run_id.clone(),
                "unified-runtime",
            ),
            reflection_payload,
        )
        .with_occurred_at(child_instance.updated_at);
        let mut reflection_event = reflection_event;
        reflection_event.event_id = reflection_event_id;
        reflection_event.causation_id = Some(child_instance.last_event_id);
        reflection_event.correlation_id = Some(child_instance.last_event_id);

        let plan = if payload_kind == "completed" {
            artifact.execute_after_child_completion_with_turn(
                &wait_state,
                &parent_child.child_id,
                output.as_ref().unwrap_or(&Value::Null),
                execution_state_for_event(&parent_instance, Some(&reflection_event)),
                ExecutionTurnContext {
                    event_id: reflection_event.event_id,
                    occurred_at: reflection_event.occurred_at,
                },
            )?
        } else {
            artifact.execute_after_child_failure_with_turn(
                &wait_state,
                &parent_child.child_id,
                error.as_deref().unwrap_or("child workflow failed"),
                execution_state_for_event(&parent_instance, Some(&reflection_event)),
                ExecutionTurnContext {
                    event_id: reflection_event.event_id,
                    occurred_at: reflection_event.occurred_at,
                },
            )?
        };

        let reflected_parent_snapshot = {
            let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
            let Some(parent_runtime) = inner.instances.get_mut(&parent_run_key) else {
                continue;
            };
            parent_runtime.instance.apply_event(&reflection_event);
            apply_compiled_plan(&mut parent_runtime.instance, &plan);
            let scheduled_tasks = schedule_activities_from_plan(
                &parent_runtime.artifact,
                &parent_runtime.instance,
                &plan,
                reflection_event.occurred_at,
            )?;
            for task in &scheduled_tasks {
                schedules.push(PreparedDbAction::Schedule(task.clone()));
                parent_runtime.active_activities.insert(
                    task.activity_id.clone(),
                    ActiveActivityMeta {
                        attempt: task.attempt,
                        task_queue: task.task_queue.clone(),
                        activity_type: task.activity_type.clone(),
                        wait_state: task.state.clone(),
                        omit_success_output: task.omit_success_output,
                    },
                );
            }
            let parent_snapshot = parent_runtime.instance.clone();
            let parent_terminal = parent_snapshot.status.is_terminal();
            let _ = parent_runtime;
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
            general.push(PreparedDbAction::UpsertInstance(parent_snapshot.clone()));
            general.extend(prepared_update_actions_from_plan(
                &parent_run_key,
                &plan,
                reflection_event.event_id,
                reflection_event.occurred_at,
            ));
            if parent_terminal {
                general.push(PreparedDbAction::CloseRun(
                    parent_run_key.clone(),
                    reflection_event.occurred_at,
                ));
                pending.push((parent_run_key.clone(), parent_snapshot.clone()));
            }
            mark_runtime_dirty(&mut inner, "child_reflection", false);
            parent_snapshot
        };

        let (child_general, child_schedules, child_notifies, terminal_children) =
            materialize_child_workflows_from_plan(
                state,
                &reflected_parent_snapshot,
                &plan,
                reflection_event.occurred_at,
                reflection_event.event_id,
            )
            .await?;
        general.extend(child_general);
        schedules.extend(child_schedules);
        notifies |= child_notifies;
        pending.extend(terminal_children);
    }

    Ok((general, schedules, notifies))
}

fn unified_timer_scheduled_event_id(source_event_id: Uuid, timer_id: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("unified-timer-scheduled:{source_event_id}:{timer_id}").as_bytes(),
    )
}

fn prepared_timer_actions_from_plan(
    state: &AppState,
    effect: &PostPlanEffect,
) -> Vec<PreparedDbAction> {
    let partition_id =
        workflow_partition_id(state, &effect.instance.tenant_id, &effect.instance.instance_id);
    let mut actions = Vec::new();
    for emission in &effect.plan.emissions {
        let WorkflowEvent::TimerScheduled { timer_id, fire_at } = &emission.event else {
            continue;
        };
        actions.push(PreparedDbAction::UpsertTimer {
            partition_id,
            tenant_id: effect.instance.tenant_id.clone(),
            instance_id: effect.instance.instance_id.clone(),
            run_id: effect.instance.run_id.clone(),
            definition_id: effect.instance.definition_id.clone(),
            definition_version: effect.instance.definition_version,
            artifact_hash: effect.instance.artifact_hash.clone(),
            timer_id: timer_id.clone(),
            state: emission.state.clone().or_else(|| effect.instance.current_state.clone()),
            fire_at: *fire_at,
            scheduled_event_id: unified_timer_scheduled_event_id(effect.source_event_id, timer_id),
            correlation_id: Some(effect.source_event_id),
        });
    }
    actions
}

async fn materialize_continue_as_new_from_plan(
    state: &AppState,
    effect: &PostPlanEffect,
) -> Result<bool> {
    let mut handled = false;
    for emission in &effect.plan.emissions {
        let WorkflowEvent::WorkflowContinuedAsNew { new_run_id, input } = &emission.event else {
            continue;
        };
        handled = true;
        let continued_event_id = unified_continue_event_id(effect.source_event_id, new_run_id);
        let triggered_event_id =
            unified_continue_trigger_event_id(effect.source_event_id, new_run_id);
        let continued_at = effect.occurred_at;

        let mut trigger_event = EventEnvelope::new(
            WorkflowEvent::WorkflowTriggered { input: input.clone() }.event_type(),
            WorkflowIdentity::new(
                effect.instance.tenant_id.clone(),
                effect.instance.definition_id.clone(),
                effect.instance.definition_version.unwrap_or(effect.artifact.definition_version),
                effect
                    .instance
                    .artifact_hash
                    .clone()
                    .unwrap_or_else(|| effect.artifact.artifact_hash.clone()),
                effect.instance.instance_id.clone(),
                new_run_id.clone(),
                "unified-runtime",
            ),
            WorkflowEvent::WorkflowTriggered { input: input.clone() },
        )
        .with_occurred_at(continued_at);
        trigger_event.event_id = triggered_event_id;
        trigger_event.causation_id = Some(continued_event_id);
        trigger_event.correlation_id = Some(effect.source_event_id);
        trigger_event
            .metadata
            .insert("workflow_task_queue".to_owned(), effect.instance.workflow_task_queue.clone());
        trigger_event.metadata.insert("continue_reason".to_owned(), "continued_as_new".to_owned());

        let plan = effect.artifact.execute_trigger_with_turn(
            input,
            ExecutionTurnContext {
                event_id: trigger_event.event_id,
                occurred_at: trigger_event.occurred_at,
            },
        )?;
        let mut new_instance = WorkflowInstanceState::try_from(&trigger_event)?;
        apply_compiled_plan(&mut new_instance, &plan);
        let scheduled_tasks = schedule_activities_from_plan(
            &effect.artifact,
            &new_instance,
            &plan,
            trigger_event.occurred_at,
        )?;
        let new_run_key = RunKey {
            tenant_id: new_instance.tenant_id.clone(),
            instance_id: new_instance.instance_id.clone(),
            run_id: new_instance.run_id.clone(),
        };

        {
            let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
            remove_run_work(&mut inner, &effect.run_key);
            inner.instances.remove(&effect.run_key);
            inner.instances.insert(
                new_run_key.clone(),
                RuntimeWorkflowState {
                    artifact: effect.artifact.clone(),
                    instance: new_instance.clone(),
                    active_activities: scheduled_tasks
                        .iter()
                        .map(|task| {
                            (
                                task.activity_id.clone(),
                                ActiveActivityMeta {
                                    attempt: task.attempt,
                                    task_queue: task.task_queue.clone(),
                                    activity_type: task.activity_type.clone(),
                                    wait_state: task.state.clone(),
                                    omit_success_output: task.omit_success_output,
                                },
                            )
                        })
                        .collect(),
                },
            );
            for task in &scheduled_tasks {
                inner
                    .ready
                    .entry(QueueKey {
                        tenant_id: task.tenant_id.clone(),
                        task_queue: task.task_queue.clone(),
                    })
                    .or_default()
                    .push_back(task.clone());
            }
            mark_runtime_dirty(&mut inner, "continue_as_new", true);
        }

        let mut general = vec![
            PreparedDbAction::DeleteTimersForRun {
                tenant_id: effect.run_key.tenant_id.clone(),
                instance_id: effect.run_key.instance_id.clone(),
                run_id: effect.run_key.run_id.clone(),
            },
            PreparedDbAction::PutRunStart {
                tenant_id: new_instance.tenant_id.clone(),
                instance_id: new_instance.instance_id.clone(),
                run_id: new_instance.run_id.clone(),
                definition_id: new_instance.definition_id.clone(),
                definition_version: new_instance.definition_version,
                artifact_hash: new_instance.artifact_hash.clone(),
                workflow_task_queue: new_instance.workflow_task_queue.clone(),
                memo: new_instance.memo.clone(),
                search_attributes: new_instance.search_attributes.clone(),
                trigger_event_id: triggered_event_id,
                started_at: trigger_event.occurred_at,
                previous_run_id: Some(effect.run_key.run_id.clone()),
                triggered_by_run_id: Some(effect.run_key.run_id.clone()),
            },
            PreparedDbAction::RecordRunContinuation {
                tenant_id: effect.run_key.tenant_id.clone(),
                instance_id: effect.run_key.instance_id.clone(),
                previous_run_id: effect.run_key.run_id.clone(),
                new_run_id: new_run_id.clone(),
                continue_reason: "continued_as_new".to_owned(),
                continued_event_id,
                triggered_event_id,
                transitioned_at: continued_at,
            },
            PreparedDbAction::UpsertInstance(new_instance.clone()),
        ];
        let mut schedules =
            scheduled_tasks.iter().cloned().map(PreparedDbAction::Schedule).collect::<Vec<_>>();
        if new_instance.status.is_terminal() {
            general.push(PreparedDbAction::CloseRun(new_run_key.clone(), new_instance.updated_at));
        }
        apply_db_actions(state, general, std::mem::take(&mut schedules)).await?;
        state.persist_notify.notify_one();
        if !scheduled_tasks.is_empty() {
            state.notify.notify_waiters();
        }
        Box::pin(apply_post_plan_effects(
            state,
            PostPlanEffect {
                run_key: new_run_key,
                artifact: effect.artifact.clone(),
                instance: new_instance,
                plan,
                source_event_id: trigger_event.event_id,
                occurred_at: trigger_event.occurred_at,
            },
        ))
        .await?;
    }
    Ok(handled)
}

async fn apply_post_plan_effects(state: &AppState, effect: PostPlanEffect) -> Result<()> {
    apply_post_plan_effects_with_options(state, effect, true).await
}

async fn apply_post_plan_effects_with_options(
    state: &AppState,
    effect: PostPlanEffect,
    persist_instance: bool,
) -> Result<()> {
    let (mut general, schedules, mut notifies, terminal_children) =
        materialize_child_workflows_from_plan(
            state,
            &effect.instance,
            &effect.plan,
            effect.occurred_at,
            effect.source_event_id,
        )
        .await?;
    let published_bulk_batches = materialize_bulk_batches_from_plan(
        state,
        &effect.instance,
        &effect.plan,
        effect.source_event_id,
        effect.occurred_at,
    )
    .await?;
    if persist_instance {
        general.push(PreparedDbAction::UpsertInstance(effect.instance.clone()));
    }
    general.extend(prepared_timer_actions_from_plan(state, &effect));
    if !general.is_empty() || !schedules.is_empty() {
        apply_db_actions(state, general, schedules).await?;
        state.persist_notify.notify_one();
    }
    if published_bulk_batches > 0 {
        state.bulk_notify.notify_waiters();
    }

    for emission in &effect.plan.emissions {
        match &emission.event {
            WorkflowEvent::ChildWorkflowSignalRequested { child_id, signal_name, payload } => {
                let child_event = EventEnvelope::new(
                    WorkflowEvent::ChildWorkflowSignalRequested {
                        child_id: child_id.clone(),
                        signal_name: signal_name.clone(),
                        payload: payload.clone(),
                    }
                    .event_type(),
                    WorkflowIdentity::new(
                        effect.instance.tenant_id.clone(),
                        effect.instance.definition_id.clone(),
                        effect.instance.definition_version.unwrap_or(effect.plan.workflow_version),
                        effect.instance.artifact_hash.clone().unwrap_or_default(),
                        effect.instance.instance_id.clone(),
                        effect.instance.run_id.clone(),
                        "unified-runtime",
                    ),
                    WorkflowEvent::ChildWorkflowSignalRequested {
                        child_id: child_id.clone(),
                        signal_name: signal_name.clone(),
                        payload: payload.clone(),
                    },
                )
                .with_occurred_at(effect.occurred_at);
                let mut child_event = child_event;
                child_event.event_id =
                    unified_child_signal_event_id(effect.source_event_id, child_id);
                child_event.causation_id = Some(effect.source_event_id);
                child_event.correlation_id = Some(effect.source_event_id);
                Box::pin(handle_child_signal_requested_event(state, child_event)).await?;
            }
            WorkflowEvent::ChildWorkflowCancellationRequested { child_id, reason } => {
                let child_event = EventEnvelope::new(
                    WorkflowEvent::ChildWorkflowCancellationRequested {
                        child_id: child_id.clone(),
                        reason: reason.clone(),
                    }
                    .event_type(),
                    WorkflowIdentity::new(
                        effect.instance.tenant_id.clone(),
                        effect.instance.definition_id.clone(),
                        effect.instance.definition_version.unwrap_or(effect.plan.workflow_version),
                        effect.instance.artifact_hash.clone().unwrap_or_default(),
                        effect.instance.instance_id.clone(),
                        effect.instance.run_id.clone(),
                        "unified-runtime",
                    ),
                    WorkflowEvent::ChildWorkflowCancellationRequested {
                        child_id: child_id.clone(),
                        reason: reason.clone(),
                    },
                )
                .with_occurred_at(effect.occurred_at);
                let mut child_event = child_event;
                child_event.event_id =
                    unified_child_cancel_event_id(effect.source_event_id, child_id);
                child_event.causation_id = Some(effect.source_event_id);
                child_event.correlation_id = Some(effect.source_event_id);
                Box::pin(handle_child_cancellation_requested_event(state, child_event)).await?;
            }
            WorkflowEvent::ExternalWorkflowSignalRequested {
                target_instance_id,
                target_run_id,
                signal_name,
                payload,
            } => {
                let external_event = EventEnvelope::new(
                    WorkflowEvent::ExternalWorkflowSignalRequested {
                        target_instance_id: target_instance_id.clone(),
                        target_run_id: target_run_id.clone(),
                        signal_name: signal_name.clone(),
                        payload: payload.clone(),
                    }
                    .event_type(),
                    WorkflowIdentity::new(
                        effect.instance.tenant_id.clone(),
                        effect.instance.definition_id.clone(),
                        effect.instance.definition_version.unwrap_or(effect.plan.workflow_version),
                        effect.instance.artifact_hash.clone().unwrap_or_default(),
                        effect.instance.instance_id.clone(),
                        effect.instance.run_id.clone(),
                        "unified-runtime",
                    ),
                    WorkflowEvent::ExternalWorkflowSignalRequested {
                        target_instance_id: target_instance_id.clone(),
                        target_run_id: target_run_id.clone(),
                        signal_name: signal_name.clone(),
                        payload: payload.clone(),
                    },
                )
                .with_occurred_at(effect.occurred_at);
                let mut external_event = external_event;
                external_event.causation_id = Some(effect.source_event_id);
                external_event.correlation_id = Some(effect.source_event_id);
                Box::pin(handle_external_signal_requested_event(state, external_event)).await?;
            }
            WorkflowEvent::ExternalWorkflowCancellationRequested {
                target_instance_id,
                target_run_id,
                reason,
            } => {
                let external_event = EventEnvelope::new(
                    WorkflowEvent::ExternalWorkflowCancellationRequested {
                        target_instance_id: target_instance_id.clone(),
                        target_run_id: target_run_id.clone(),
                        reason: reason.clone(),
                    }
                    .event_type(),
                    WorkflowIdentity::new(
                        effect.instance.tenant_id.clone(),
                        effect.instance.definition_id.clone(),
                        effect.instance.definition_version.unwrap_or(effect.plan.workflow_version),
                        effect.instance.artifact_hash.clone().unwrap_or_default(),
                        effect.instance.instance_id.clone(),
                        effect.instance.run_id.clone(),
                        "unified-runtime",
                    ),
                    WorkflowEvent::ExternalWorkflowCancellationRequested {
                        target_instance_id: target_instance_id.clone(),
                        target_run_id: target_run_id.clone(),
                        reason: reason.clone(),
                    },
                )
                .with_occurred_at(effect.occurred_at);
                let mut external_event = external_event;
                external_event.causation_id = Some(effect.source_event_id);
                external_event.correlation_id = Some(effect.source_event_id);
                Box::pin(handle_external_cancellation_requested_event(state, external_event))
                    .await?;
            }
            _ => {}
        }
    }

    let continued = materialize_continue_as_new_from_plan(state, &effect).await?;
    let mut pending_terminals = terminal_children;
    if !continued && effect.instance.status.is_terminal() {
        pending_terminals.push((effect.run_key, effect.instance));
    }
    if !pending_terminals.is_empty() {
        let (general, schedules, child_notifies) =
            reflect_terminal_children_to_parents(state, pending_terminals).await?;
        if !general.is_empty() || !schedules.is_empty() {
            apply_db_actions(state, general, schedules).await?;
            state.persist_notify.notify_one();
        }
        notifies |= child_notifies;
    }

    if notifies {
        state.notify.notify_waiters();
    }
    Ok(())
}

async fn materialize_bulk_batches_from_plan(
    state: &AppState,
    instance: &WorkflowInstanceState,
    plan: &CompiledExecutionPlan,
    source_event_id: Uuid,
    occurred_at: DateTime<Utc>,
) -> Result<u64> {
    let mut published = 0_u64;
    for emission in &plan.emissions {
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
            execution_policy,
            reducer,
            throughput_backend,
            throughput_backend_version: _,
            state: workflow_state,
        } = &emission.event
        else {
            continue;
        };
        let scheduled_input_handle = serde_json::from_value::<PayloadHandle>(input_handle.clone())
            .context("failed to decode scheduled bulk input handle")?;
        let total_items = if items.is_empty() {
            parse_benchmark_compact_total_items_from_handle(&scheduled_input_handle)
                .map(|count| count as usize)
                .unwrap_or_default()
        } else {
            items.len()
        };

        let admission_started_at = Instant::now();
        let admission = admit_bulk_batch(
            state,
            &instance.tenant_id,
            task_queue,
            is_supported_throughput_backend(throughput_backend)
                .then_some(throughput_backend.as_str()),
            execution_policy.as_deref(),
            reducer.as_deref(),
            total_items,
            *chunk_size,
        )
        .await?;
        {
            let mut debug = state.debug.lock().expect("unified debug lock poisoned");
            debug.trigger_bulk_admission_micros = debug
                .trigger_bulk_admission_micros
                .saturating_add(elapsed_micros(admission_started_at));
        }
        let selected_backend = admission.selected_backend.as_str();
        let selected_backend_version = admission.selected_backend_version.as_str();

        if selected_backend == ThroughputBackend::PgV1.as_str() {
            let pg_materialize_started_at = Instant::now();
            let reducer_class = bulk_reducer_class(reducer.as_deref());
            let fast_lane_enabled = stream_v2_fast_lane_enabled(
                selected_backend,
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
            let _ = state
                .store
                .persist_bulk_batch_with_metadata(
                    &instance.tenant_id,
                    &instance.instance_id,
                    &instance.run_id,
                    &instance.definition_id,
                    instance.definition_version,
                    instance.artifact_hash.as_deref(),
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
                    selected_backend,
                    selected_backend_version,
                    &admission.routing_reason,
                    &admission.admission_policy_version,
                    occurred_at,
                )
                .await?;
            let mut debug = state.debug.lock().expect("unified debug lock poisoned");
            debug.trigger_bulk_pg_materialize_micros = debug
                .trigger_bulk_pg_materialize_micros
                .saturating_add(elapsed_micros(pg_materialize_started_at));
        }

        let publish_started_at = Instant::now();
        let native_stream_v2 = selected_backend == ThroughputBackend::StreamV2.as_str()
            && native_stream_v2_engine_enabled(state);
        let scheduled_event = WorkflowEvent::BulkActivityBatchScheduled {
            batch_id: batch_id.clone(),
            activity_type: activity_type.clone(),
            task_queue: task_queue.clone(),
            items: items.clone(),
            input_handle: input_handle.clone(),
            result_handle: result_handle.clone(),
            chunk_size: *chunk_size,
            max_attempts: *max_attempts,
            retry_delay_ms: *retry_delay_ms,
            aggregation_group_count: *aggregation_group_count,
            execution_policy: execution_policy.clone(),
            reducer: reducer.clone(),
            throughput_backend: selected_backend.to_owned(),
            throughput_backend_version: selected_backend_version.to_owned(),
            state: workflow_state.clone(),
        };
        if native_stream_v2 {
            let native_input_handle = if can_use_payloadless_benchmark_transport(
                activity_type,
                reducer.as_deref(),
                *max_attempts,
                items,
            ) {
                scheduled_input_handle.clone()
            } else {
                state
                    .store
                    .upsert_throughput_run_input(&ThroughputRunInputRecord {
                        tenant_id: instance.tenant_id.clone(),
                        instance_id: instance.instance_id.clone(),
                        run_id: instance.run_id.clone(),
                        batch_id: batch_id.clone(),
                        items: items.clone(),
                        created_at: occurred_at,
                        updated_at: occurred_at,
                    })
                    .await?;
                native_stream_v2_run_input_handle(
                    &instance.tenant_id,
                    &instance.instance_id,
                    &instance.run_id,
                    batch_id,
                )
            };
            let command = start_throughput_run_command(
                state,
                instance,
                batch_id,
                activity_type,
                task_queue,
                total_items,
                result_handle,
                workflow_state.clone(),
                *chunk_size,
                *max_attempts,
                *retry_delay_ms,
                *aggregation_group_count,
                execution_policy.clone(),
                reducer.clone(),
                selected_backend,
                selected_backend_version,
                &admission,
                native_input_handle,
                source_event_id,
                occurred_at,
            )
            .await?;
            state
                .store
                .upsert_throughput_run(&ThroughputRunRecord {
                    tenant_id: instance.tenant_id.clone(),
                    instance_id: instance.instance_id.clone(),
                    run_id: instance.run_id.clone(),
                    definition_id: instance.definition_id.clone(),
                    definition_version: instance.definition_version,
                    artifact_hash: instance.artifact_hash.clone(),
                    batch_id: batch_id.clone(),
                    throughput_backend: selected_backend.to_owned(),
                    status: "scheduled".to_owned(),
                    command_dedupe_key: command.dedupe_key.clone(),
                    command: command.clone(),
                    command_published_at: None,
                    started_at: None,
                    terminal_at: None,
                    created_at: occurred_at,
                    updated_at: occurred_at,
                })
                .await?;
            let command_publisher = state
                .throughput_command_publisher
                .as_ref()
                .context("stream-v2 bulk admission requires throughput command publisher")?;
            command_publisher.publish(&command, &command.partition_key).await?;
            let _ = state
                .store
                .mark_throughput_run_command_published(
                    &instance.tenant_id,
                    &instance.instance_id,
                    &instance.run_id,
                    batch_id,
                    Utc::now(),
                )
                .await?;
        } else if let Some(publisher) = state.publisher.as_ref() {
            let envelope = emitted_bulk_batch_event(
                instance,
                &scheduled_event,
                selected_backend,
                &admission.routing_reason,
                &admission.admission_policy_version,
                source_event_id,
                occurred_at,
            );
            publisher.publish(&envelope, &envelope.partition_key).await?;
        }
        if native_stream_v2 || state.publisher.is_some() {
            published = published.saturating_add(1);
            let mut debug = state.debug.lock().expect("unified debug lock poisoned");
            debug.trigger_bulk_publish_micros = debug
                .trigger_bulk_publish_micros
                .saturating_add(elapsed_micros(publish_started_at));
            debug.trigger_bulk_events_published =
                debug.trigger_bulk_events_published.saturating_add(1);
        }
    }
    Ok(published)
}

async fn admit_bulk_batch(
    state: &AppState,
    tenant_id: &str,
    task_queue: &str,
    requested_backend: Option<&str>,
    execution_policy: Option<&str>,
    reducer: Option<&str>,
    item_count: usize,
    chunk_size: u32,
) -> Result<BulkAdmissionDecision> {
    let chunk_size = chunk_size.max(1) as usize;
    let chunk_count = if item_count == 0 { 0 } else { item_count.div_ceil(chunk_size) as u32 };
    let configured_task_queue_backend =
        state.admission.task_queue_backends.get(task_queue).map(String::as_str);
    let task_queue_backend = if configured_task_queue_backend.is_some() {
        None
    } else {
        cached_task_queue_throughput_backend(
            state,
            tenant_id,
            task_queue,
            requested_backend.is_some(),
        )
        .await?
    };
    let has_task_queue_override =
        task_queue_backend.is_some() || configured_task_queue_backend.is_some();
    let mut decision = choose_bulk_admission_backend(
        &state.admission,
        requested_backend,
        task_queue_backend.as_deref().or(configured_task_queue_backend),
        execution_policy,
        reducer,
        item_count,
        chunk_count,
    );

    if decision.selected_backend == ThroughputBackend::StreamV2.as_str()
        && !has_task_queue_override
        && !matches!(requested_backend, Some(backend) if backend == ThroughputBackend::StreamV2.as_str())
    {
        let tenant_started = state
            .store
            .count_started_bulk_chunks_for_backend_scope(
                ThroughputBackend::StreamV2.as_str(),
                Some(tenant_id),
                None,
                None,
            )
            .await?;
        if state.admission.max_active_chunks_per_tenant > 0
            && tenant_started >= state.admission.max_active_chunks_per_tenant as u64
        {
            decision = bulk_admission_decision(
                ThroughputBackend::PgV1.as_str(),
                "tenant_capacity_fallback",
            );
        } else {
            let task_queue_started = state
                .store
                .count_started_bulk_chunks_for_backend_scope(
                    ThroughputBackend::StreamV2.as_str(),
                    Some(tenant_id),
                    Some(task_queue),
                    None,
                )
                .await?;
            if state.admission.max_active_chunks_per_task_queue > 0
                && task_queue_started >= state.admission.max_active_chunks_per_task_queue as u64
            {
                decision = bulk_admission_decision(
                    ThroughputBackend::PgV1.as_str(),
                    "task_queue_capacity_fallback",
                );
            }
        }
    }

    Ok(decision)
}

async fn cached_task_queue_throughput_backend(
    state: &AppState,
    tenant_id: &str,
    task_queue: &str,
    bypass_cache: bool,
) -> Result<Option<String>> {
    let key = TaskQueuePolicyCacheKey {
        tenant_id: tenant_id.to_owned(),
        task_queue: task_queue.to_owned(),
    };
    if !bypass_cache {
        let cache = state.task_queue_policy_cache.lock().expect("task queue policy cache poisoned");
        if let Some(entry) = cache.get(&key) {
            if entry.fetched_at.elapsed() < Duration::from_millis(TASK_QUEUE_POLICY_CACHE_TTL_MS) {
                return Ok(entry.backend.clone());
            }
        }
    }

    let backend = state
        .store
        .get_task_queue_throughput_policy(tenant_id, TaskQueueKind::Activity, task_queue)
        .await?
        .map(|record| record.backend);
    let mut cache = state.task_queue_policy_cache.lock().expect("task queue policy cache poisoned");
    cache.insert(
        key,
        CachedTaskQueuePolicy { backend: backend.clone(), fetched_at: Instant::now() },
    );
    Ok(backend)
}

fn choose_bulk_admission_backend(
    config: &BulkAdmissionConfig,
    requested_backend: Option<&str>,
    task_queue_backend: Option<&str>,
    _execution_policy: Option<&str>,
    reducer: Option<&str>,
    item_count: usize,
    chunk_count: u32,
) -> BulkAdmissionDecision {
    if let Some(backend) =
        task_queue_backend.filter(|backend| is_supported_throughput_backend(backend))
    {
        return bulk_admission_decision(backend, "task_queue_policy_override");
    }
    if let Some(backend) =
        requested_backend.filter(|backend| is_supported_throughput_backend(backend))
    {
        return bulk_admission_decision(backend, "workflow_backend_hint");
    }
    if bulk_reducer_materializes_results(reducer) {
        return bulk_admission_decision(ThroughputBackend::PgV1.as_str(), "materialized_results");
    }
    if bulk_reducer_is_mergeable(reducer)
        && (item_count >= config.stream_v2_min_items || chunk_count >= config.stream_v2_min_chunks)
    {
        return bulk_admission_decision(ThroughputBackend::StreamV2.as_str(), "mergeable_scale");
    }
    bulk_admission_decision(&config.default_backend, "default_backend")
}

fn bulk_admission_decision(backend: &str, routing_reason: &str) -> BulkAdmissionDecision {
    let selected_backend = if backend == ThroughputBackend::StreamV2.as_str() {
        ThroughputBackend::StreamV2.as_str().to_owned()
    } else {
        ThroughputBackend::PgV1.as_str().to_owned()
    };
    let selected_backend_version = if selected_backend == ThroughputBackend::StreamV2.as_str() {
        ThroughputBackend::StreamV2.default_version().to_owned()
    } else {
        ThroughputBackend::PgV1.default_version().to_owned()
    };
    BulkAdmissionDecision {
        selected_backend,
        selected_backend_version,
        routing_reason: routing_reason.to_owned(),
        admission_policy_version: ADMISSION_POLICY_VERSION.to_owned(),
    }
}

fn is_supported_throughput_backend(backend: &str) -> bool {
    matches!(
        backend,
        value if value == ThroughputBackend::PgV1.as_str()
            || value == ThroughputBackend::StreamV2.as_str()
    )
}

async fn run_workflow_event_outbox_publisher(state: AppState) {
    if state.publisher.is_none() {
        return;
    }

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
                error!(error = %error, "unified-runtime failed to lease workflow event outbox");
                tokio::time::sleep(Duration::from_millis(BULK_EVENT_OUTBOX_RETRY_MS)).await;
                continue;
            }
        };

        if leased.is_empty() {
            if tokio::time::timeout(
                Duration::from_millis(BULK_EVENT_OUTBOX_RETRY_MS),
                state.outbox_notify.notified(),
            )
            .await
            .is_err()
            {
                continue;
            }
            continue;
        }

        let publisher = state.publisher.as_ref().expect("publisher checked above");
        for record in leased {
            if let Err(error) = publisher.publish(&record.event, &record.partition_key).await {
                error!(
                    error = %error,
                    outbox_id = %record.outbox_id,
                    event_type = %record.event_type,
                    "unified-runtime failed to publish workflow event outbox row"
                );
                let retry_at =
                    Utc::now() + chrono::Duration::milliseconds(BULK_EVENT_OUTBOX_RETRY_MS as i64);
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
                        "unified-runtime failed to release workflow event outbox lease"
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
                    "unified-runtime failed to mark workflow event outbox row published"
                );
            }
        }
    }
}

async fn run_retry_release_loop(state: AppState) {
    loop {
        let retry_notified = state.retry_notify.notified();
        let next_due_at = {
            let inner = state.inner.lock().expect("unified runtime lock poisoned");
            inner.delayed_retries.keys().next().cloned()
        };
        match next_due_at {
            Some(next_due_at) => {
                let wait = next_due_at
                    .signed_duration_since(Utc::now())
                    .to_std()
                    .unwrap_or_else(|_| Duration::from_millis(0));
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {}
                    _ = retry_notified => continue,
                }
            }
            None => {
                retry_notified.await;
                continue;
            }
        }
        let now = Utc::now();
        let released = {
            let mut inner = state.inner.lock().expect("unified runtime lock poisoned");
            take_due_delayed_retries(&mut inner, now)
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

async fn repair_unpublished_stream_v2_runs(state: &AppState) -> Result<usize> {
    let Some(command_publisher) = state.throughput_command_publisher.as_ref() else {
        return Ok(0);
    };
    let runs = state
        .store
        .list_unpublished_scheduled_throughput_runs_for_backend(
            ThroughputBackend::StreamV2.as_str(),
            10_000,
        )
        .await?;
    let mut repaired = 0_usize;
    for run in runs {
        let ThroughputCommand::StartThroughputRun(_) = &run.command.payload else {
            continue;
        };
        command_publisher.publish(&run.command, &run.command.partition_key).await.with_context(
            || {
                format!(
                    "failed to republish native throughput run command for batch {}",
                    run.batch_id
                )
            },
        )?;
        let published_at = Utc::now();
        let _ = state
            .store
            .mark_throughput_run_command_published(
                &run.tenant_id,
                &run.instance_id,
                &run.run_id,
                &run.batch_id,
                published_at,
            )
            .await?;
        repaired = repaired.saturating_add(1);
    }
    Ok(repaired)
}

async fn run_throughput_run_repair_loop(state: AppState) {
    loop {
        match repair_unpublished_stream_v2_runs(&state).await {
            Ok(repaired) if repaired > 0 => {
                info!(repaired, "repaired unpublished native stream-v2 admissions");
            }
            Ok(_) => {}
            Err(error) => {
                error!(error = %error, "failed to repair unpublished native stream-v2 admissions");
            }
        }
        tokio::time::sleep(Duration::from_millis(1_000)).await;
    }
}

async fn execute_internal_query(
    AxumPath((tenant_id, instance_id, query_name)): AxumPath<(String, String, String)>,
    AxumState(state): AxumState<AppState>,
    Json(request): Json<InternalQueryRequest>,
) -> Result<Json<InternalQueryResponse>, (StatusCode, Json<UnifiedErrorResponse>)> {
    let hot_instance = {
        let inner = state.inner.lock().expect("unified runtime lock poisoned");
        inner
            .instances
            .values()
            .find(|runtime| {
                runtime.instance.tenant_id == tenant_id
                    && runtime.instance.instance_id == instance_id
            })
            .map(|runtime| (runtime.instance.clone(), runtime.artifact.clone()))
    };

    let (instance, artifact, source) = if let Some((instance, artifact)) = hot_instance {
        (instance, artifact, "hot_owner")
    } else {
        let instance =
            state.store.get_instance(&tenant_id, &instance_id).await.map_err(|error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(UnifiedErrorResponse {
                        message: format!("failed to load workflow instance: {error}"),
                    }),
                )
            })?;
        let Some(instance) = instance else {
            return Err((
                StatusCode::NOT_FOUND,
                Json(UnifiedErrorResponse {
                    message: format!("workflow instance {instance_id} not found"),
                }),
            ));
        };
        let Some(version) = instance.definition_version else {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(UnifiedErrorResponse {
                    message: "workflow instance is missing a pinned artifact version".to_owned(),
                }),
            ));
        };
        let artifact =
            load_compiled_artifact_version(&state, &tenant_id, &instance.definition_id, version)
                .await
                .map_err(|error| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(UnifiedErrorResponse {
                            message: format!("failed to load workflow artifact: {error}"),
                        }),
                    )
                })?
                .ok_or_else(|| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(UnifiedErrorResponse {
                            message: "workflow artifact not found".to_owned(),
                        }),
                    )
                })?;
        (instance, artifact, "replay")
    };

    let result = artifact
        .evaluate_query(&query_name, &request.args, execution_state_for_event(&instance, None))
        .map_err(|error| {
            (StatusCode::BAD_REQUEST, Json(UnifiedErrorResponse { message: error.to_string() }))
        })?;

    Ok(Json(InternalQueryResponse { result, consistency: "strong", source }))
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
    prune_terminal_instances(&mut inner);
    let persisted = capture_persisted_state(&mut inner);
    Some((reason, force_snapshot, persisted))
}

fn prune_terminal_instances(inner: &mut RuntimeInner) {
    let terminal_runs = inner
        .instances
        .iter()
        .filter_map(|(run_key, runtime)| {
            runtime.instance.status.is_terminal().then_some(run_key.clone())
        })
        .collect::<Vec<_>>();
    for run_key in terminal_runs {
        remove_run_work(inner, &run_key);
        inner.instances.remove(&run_key);
    }
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
                supports_cbor: request.get_ref().supports_cbor,
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
                chrono::Duration::seconds(DEFAULT_LEASE_TTL_SECS),
            )
            .await
            .map_err(internal_status)?;
        let queue_compatible = self
            .state
            .store
            .is_build_compatible_with_queue(
                &request.tenant_id,
                TaskQueueKind::Activity,
                &request.task_queue,
                &request.worker_build_id,
            )
            .await
            .map_err(internal_status)?;
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(request.poll_timeout_ms.max(1).min(30_000));

        loop {
            let leased = if queue_compatible {
                let mut inner = self.state.inner.lock().expect("unified runtime lock poisoned");
                lease_ready_tasks(
                    &mut inner,
                    &queue_key,
                    &request.worker_id,
                    &request.worker_build_id,
                    max_tasks,
                )
            } else {
                Vec::new()
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
                let accepted = {
                    let mut inner = self.state.inner.lock().expect("unified runtime lock poisoned");
                    let mut accepted = Vec::new();
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
                        accepted.push(leased_task);
                    }
                    accepted
                };
                let tasks = accepted
                    .iter()
                    .map(|leased_task| activity_proto(leased_task, request.supports_cbor))
                    .collect::<Vec<_>>();
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
        let max_tasks = usize::try_from(request.max_tasks.max(1)).unwrap_or(1).min(32);
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(request.poll_timeout_ms.max(1).min(30_000));

        loop {
            let leased = self
                .state
                .store
                .lease_next_bulk_chunks(
                    &request.tenant_id,
                    &request.task_queue,
                    &request.worker_id,
                    &request.worker_build_id,
                    chrono::Duration::seconds(DEFAULT_LEASE_TTL_SECS),
                    max_tasks,
                )
                .await
                .map_err(internal_status)?;
            if !leased.is_empty() {
                let tasks = leased
                    .iter()
                    .map(|record| bulk_chunk_to_proto(record, request.supports_cbor))
                    .collect::<Vec<_>>();
                return Ok(Response::new(PollBulkActivityTaskResponse { tasks }));
            }

            if tokio::time::Instant::now() >= deadline {
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
                    ActivityTaskCompletedResult {
                        output_json: request.output_json,
                        output_cbor: Vec::new(),
                    },
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
        request: Request<RecordActivityHeartbeatRequest>,
    ) -> Result<Response<RecordActivityHeartbeatResponse>, Status> {
        if current_owner_epoch(&self.state).is_none() {
            return Err(Status::unavailable("unified-runtime ownership inactive"));
        }
        let request = request.into_inner();
        let attempt_key = AttemptKey {
            tenant_id: request.tenant_id.clone(),
            instance_id: request.instance_id.clone(),
            run_id: request.run_id.clone(),
            activity_id: request.activity_id.clone(),
            attempt: request.attempt,
        };
        let now = Utc::now();
        let lease_expires_at = now + chrono::Duration::seconds(DEFAULT_LEASE_TTL_SECS);
        let in_memory_cancellation_requested = {
            let mut inner = self.state.inner.lock().expect("unified runtime lock poisoned");
            let Some(leased) = inner.leased.get_mut(&attempt_key) else {
                return Ok(Response::new(RecordActivityHeartbeatResponse {
                    cancellation_requested: false,
                }));
            };
            if leased.owner_epoch != request.owner_epoch
                || leased.task.lease_epoch != request.lease_epoch
                || leased.worker_id != request.worker_id
                || leased.worker_build_id != request.worker_build_id
            {
                return Ok(Response::new(RecordActivityHeartbeatResponse {
                    cancellation_requested: false,
                }));
            }
            leased.lease_expires_at = lease_expires_at;
            leased.task.cancellation_requested
        };
        let heartbeat = self
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
                Uuid::now_v7(),
                "UnifiedActivityHeartbeat",
                now,
            )
            .await
            .map_err(internal_status)?;
        self.state.persist_notify.notify_one();
        Ok(Response::new(RecordActivityHeartbeatResponse {
            cancellation_requested: in_memory_cancellation_requested
                || heartbeat.cancellation_requested,
        }))
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
                        metadata_cbor: Vec::new(),
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
        publish_history_events(&self.state, &actions.emitted_events)
            .await
            .map_err(internal_status)?;
        apply_db_actions(&self.state, actions.general, actions.schedules)
            .await
            .map_err(internal_status)?;
        for post_plan in actions.post_plans {
            let run_key = post_plan.run_key.clone();
            let event_id = post_plan.source_event_id;
            let occurred_at = post_plan.occurred_at;
            apply_post_plan_effects(&self.state, post_plan).await.map_err(internal_status)?;
            maybe_enact_pending_workflow_cancellation_unified(
                &self.state,
                &run_key,
                event_id,
                occurred_at,
            )
            .await
            .map_err(internal_status)?;
        }
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
        request: Request<ReportBulkActivityTaskResultsRequest>,
    ) -> Result<Response<Ack>, Status> {
        if current_owner_epoch(&self.state).is_none() {
            return Err(Status::unavailable("unified-runtime ownership inactive"));
        }
        for result in request.into_inner().results {
            ensure_bulk_batch_backend(
                &self.state.store,
                &result.tenant_id,
                &result.instance_id,
                &result.run_id,
                &result.batch_id,
                ThroughputBackend::PgV1.as_str(),
            )
            .await?;
            let update = prepare_bulk_result(result)?;
            let applied = self
                .state
                .store
                .apply_bulk_chunk_terminal_update(&update)
                .await
                .map_err(internal_status)?;
            if applied.terminal_event.is_some() {
                self.state.outbox_notify.notify_waiters();
            } else if applied.chunk.status == fabrik_store::WorkflowBulkChunkStatus::Scheduled {
                let now = Utc::now();
                let delay = if applied.chunk.available_at > now {
                    (applied.chunk.available_at - now)
                        .to_std()
                        .unwrap_or_else(|_| Duration::from_millis(0))
                } else {
                    Duration::from_millis(0)
                };
                let notify = self.state.bulk_notify.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    notify.notify_waiters();
                });
            }
        }
        Ok(Response::new(Ack {}))
    }
}

struct PreparedActions {
    emitted_events: Vec<EventEnvelope<WorkflowEvent>>,
    general: Vec<PreparedDbAction>,
    schedules: Vec<PreparedDbAction>,
    notifies: bool,
    post_plans: Vec<PostPlanEffect>,
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

struct AppliedActivityResultPlan {
    post_plan: PostPlanEffect,
    scheduled_tasks: Vec<QueuedActivity>,
    terminal_instance: Option<WorkflowInstanceState>,
}

fn apply_activity_result_plan(
    run_key: &RunKey,
    runtime: &mut RuntimeWorkflowState,
    plan: CompiledExecutionPlan,
) -> Result<AppliedActivityResultPlan> {
    apply_compiled_plan(&mut runtime.instance, &plan);
    let occurred_at = runtime.instance.updated_at;
    let scheduled_tasks =
        schedule_activities_from_plan(&runtime.artifact, &runtime.instance, &plan, occurred_at)?;
    let terminal_instance =
        runtime.instance.status.is_terminal().then_some(runtime.instance.clone());
    Ok(AppliedActivityResultPlan {
        post_plan: PostPlanEffect {
            run_key: run_key.clone(),
            artifact: runtime.artifact.clone(),
            instance: runtime.instance.clone(),
            plan,
            source_event_id: runtime.instance.last_event_id,
            occurred_at,
        },
        scheduled_tasks,
        terminal_instance,
    })
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
    let mut post_plans = Vec::new();
    let mut emitted_events = Vec::new();
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
                let output = decode_normal_activity_output(completed, active.omit_success_output)?;
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
                    if unified_failure_allows_retry(result.attempt, &retry, &failed.error) {
                        if let Some(retry_task) = build_retry_task(
                            runtime,
                            &result.activity_id,
                            result.attempt + 1,
                            &retry,
                            now,
                        )? {
                            runtime.active_activities.insert(
                                result.activity_id.clone(),
                                ActiveActivityMeta {
                                    attempt: retry_task.task.attempt,
                                    task_queue: retry_task.task.task_queue.clone(),
                                    activity_type: retry_task.task.activity_type.clone(),
                                    wait_state: retry_task.task.state.clone(),
                                    omit_success_output: retry_task.task.omit_success_output,
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
                let metadata = decode_cancelled_metadata(cancelled)?;
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
                        metadata: metadata.clone(),
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
                    metadata,
                }
            }
            None => continue,
        };
        if let Some(retry_task) = delayed_retry {
            let _ = runtime;
            let notify_retry_loop = push_delayed_retry(&mut inner, retry_task);
            if notify_retry_loop {
                state.retry_notify.notify_one();
            }
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
        emitted_events.extend(events.iter().cloned());
        let mut index = 0usize;
        while index < events.len() {
            let mut applied_post_plan = None;
            let mut scheduled_tasks = Vec::new();
            let mut terminal_instance = None;
            let workflow_terminal = {
                let Some(runtime) = inner.instances.get_mut(&run_key) else {
                    break;
                };
                let Some(wait_state) = runtime.instance.current_state.clone() else {
                    break;
                };
                let execution_state =
                    execution_state_for_event(&runtime.instance, events.get(index));
                if let Some((plan, applied)) =
                    runtime.artifact.try_execute_after_step_terminal_batch_with_turn(
                        &wait_state,
                        &events[index..],
                        execution_state,
                    )?
                {
                    for event in &events[index..index + applied] {
                        runtime.instance.apply_event(event);
                    }
                    let applied_plan = apply_activity_result_plan(&run_key, runtime, plan)?;
                    for task in &applied_plan.scheduled_tasks {
                        schedules.push(PreparedDbAction::Schedule(task.clone()));
                        runtime.active_activities.insert(
                            task.activity_id.clone(),
                            ActiveActivityMeta {
                                attempt: task.attempt,
                                task_queue: task.task_queue.clone(),
                                activity_type: task.activity_type.clone(),
                                wait_state: task.state.clone(),
                                omit_success_output: task.omit_success_output,
                            },
                        );
                    }
                    scheduled_tasks = applied_plan.scheduled_tasks;
                    terminal_instance = applied_plan.terminal_instance;
                    applied_post_plan = Some(applied_plan.post_plan);
                    index += applied;
                } else {
                    let event = &events[index];
                    runtime.instance.apply_event(event);
                    let execution_state = execution_state_for_event(&runtime.instance, Some(event));
                    let turn_context = ExecutionTurnContext {
                        event_id: event.event_id,
                        occurred_at: event.occurred_at,
                    };
                    let current_state =
                        runtime.instance.current_state.as_deref().unwrap_or_default();
                    let plan = match &event.payload {
                        WorkflowEvent::ActivityTaskCompleted { activity_id, output, .. } => {
                            Some(runtime.artifact.execute_after_step_completion_with_turn(
                                current_state,
                                activity_id,
                                output,
                                execution_state,
                                turn_context,
                            )?)
                        }
                        WorkflowEvent::ActivityTaskFailed { activity_id, error, .. } => {
                            Some(runtime.artifact.execute_after_step_failure_with_turn(
                                current_state,
                                activity_id,
                                error,
                                execution_state,
                                turn_context,
                            )?)
                        }
                        WorkflowEvent::ActivityTaskCancelled { activity_id, reason, .. } => {
                            Some(runtime.artifact.execute_after_step_cancellation_with_turn(
                                current_state,
                                activity_id,
                                reason,
                                execution_state,
                                turn_context,
                            )?)
                        }
                        _ => None,
                    };
                    if let Some(plan) = plan {
                        let applied_plan = apply_activity_result_plan(&run_key, runtime, plan)?;
                        for task in &applied_plan.scheduled_tasks {
                            schedules.push(PreparedDbAction::Schedule(task.clone()));
                            runtime.active_activities.insert(
                                task.activity_id.clone(),
                                ActiveActivityMeta {
                                    attempt: task.attempt,
                                    task_queue: task.task_queue.clone(),
                                    activity_type: task.activity_type.clone(),
                                    wait_state: task.state.clone(),
                                    omit_success_output: task.omit_success_output,
                                },
                            );
                        }
                        scheduled_tasks = applied_plan.scheduled_tasks;
                        terminal_instance = applied_plan.terminal_instance;
                        applied_post_plan = Some(applied_plan.post_plan);
                    }
                    index += 1;
                }
                runtime.instance.status.is_terminal()
            };

            if let Some(post_plan) = applied_post_plan {
                post_plans.push(post_plan);
            }
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
                general.push(PreparedDbAction::UpsertInstance(instance.clone()));
                general.push(PreparedDbAction::CloseRun(run_key.clone(), instance.updated_at));
                instance_terminals = instance_terminals.saturating_add(1);
            }
            if workflow_terminal {
                break;
            }
        }
    }

    mark_runtime_dirty(&mut inner, "report_batch", false);
    Ok(PreparedActions {
        emitted_events,
        general,
        schedules,
        notifies,
        post_plans,
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
            PreparedDbAction::UpsertPersistedInstance(instance) => {
                other_general.push(PreparedDbAction::UpsertPersistedInstance(instance))
            }
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
            PreparedDbAction::UpsertPersistedInstance(instance) => {
                state.store.upsert_persisted_instance(&instance).await?;
            }
            PreparedDbAction::CloseRun(run_key, closed_at) => {
                state
                    .store
                    .close_run(&run_key.tenant_id, &run_key.instance_id, &run_key.run_id, closed_at)
                    .await?;
            }
            PreparedDbAction::PutRunStart {
                tenant_id,
                instance_id,
                run_id,
                definition_id,
                definition_version,
                artifact_hash,
                workflow_task_queue,
                memo,
                search_attributes,
                trigger_event_id,
                started_at,
                previous_run_id,
                triggered_by_run_id,
            } => {
                state
                    .store
                    .put_run_start(
                        &tenant_id,
                        &instance_id,
                        &run_id,
                        &definition_id,
                        definition_version,
                        artifact_hash.as_deref(),
                        &workflow_task_queue,
                        memo.as_ref(),
                        search_attributes.as_ref(),
                        trigger_event_id,
                        started_at,
                        previous_run_id.as_deref(),
                        triggered_by_run_id.as_deref(),
                    )
                    .await?;
            }
            PreparedDbAction::UpsertChildStartRequested {
                tenant_id,
                instance_id,
                run_id,
                child_id,
                child_workflow_id,
                child_definition_id,
                parent_close_policy,
                input,
                source_event_id,
                occurred_at,
            } => {
                state
                    .store
                    .upsert_child_start_requested(
                        &tenant_id,
                        &instance_id,
                        &run_id,
                        &child_id,
                        &child_workflow_id,
                        &child_definition_id,
                        &parent_close_policy,
                        &input,
                        source_event_id,
                        occurred_at,
                    )
                    .await?;
            }
            PreparedDbAction::MarkChildStarted {
                tenant_id,
                instance_id,
                run_id,
                child_id,
                child_run_id,
                started_event_id,
                occurred_at,
            } => {
                state
                    .store
                    .mark_child_started(
                        &tenant_id,
                        &instance_id,
                        &run_id,
                        &child_id,
                        &child_run_id,
                        started_event_id,
                        occurred_at,
                    )
                    .await?;
            }
            PreparedDbAction::CompleteChild {
                tenant_id,
                instance_id,
                run_id,
                child_id,
                status,
                output,
                error,
                terminal_event_id,
                occurred_at,
            } => {
                state
                    .store
                    .complete_child(
                        &tenant_id,
                        &instance_id,
                        &run_id,
                        &child_id,
                        &status,
                        output.as_ref(),
                        error.as_deref(),
                        terminal_event_id,
                        occurred_at,
                    )
                    .await?;
            }
            PreparedDbAction::CompleteUpdate {
                tenant_id,
                instance_id,
                run_id,
                update_id,
                output,
                error,
                completed_event_id,
                completed_at,
            } => {
                state
                    .store
                    .complete_update(
                        &tenant_id,
                        &instance_id,
                        &run_id,
                        &update_id,
                        output.as_ref(),
                        error.as_deref(),
                        completed_event_id,
                        completed_at,
                    )
                    .await?;
            }
            PreparedDbAction::UpsertTimer {
                partition_id,
                tenant_id,
                instance_id,
                run_id,
                definition_id,
                definition_version,
                artifact_hash,
                timer_id,
                state: timer_state,
                fire_at,
                scheduled_event_id,
                correlation_id,
            } => {
                state
                    .store
                    .upsert_timer(
                        partition_id,
                        &tenant_id,
                        &instance_id,
                        &run_id,
                        &definition_id,
                        definition_version,
                        artifact_hash.as_deref(),
                        &timer_id,
                        timer_state.as_deref(),
                        fire_at,
                        scheduled_event_id,
                        correlation_id,
                    )
                    .await?;
            }
            PreparedDbAction::DeleteTimer { tenant_id, instance_id, timer_id } => {
                state.store.delete_timer(&tenant_id, &instance_id, &timer_id).await?;
            }
            PreparedDbAction::DeleteTimersForRun { tenant_id, instance_id, run_id } => {
                state.store.delete_timers_for_run(&tenant_id, &instance_id, &run_id).await?;
            }
            PreparedDbAction::RecordRunContinuation {
                tenant_id,
                instance_id,
                previous_run_id,
                new_run_id,
                continue_reason,
                continued_event_id,
                triggered_event_id,
                transitioned_at,
            } => {
                state
                    .store
                    .record_run_continuation(
                        &tenant_id,
                        &instance_id,
                        &previous_run_id,
                        &new_run_id,
                        &continue_reason,
                        continued_event_id,
                        triggered_event_id,
                        transitioned_at,
                    )
                    .await?;
            }
        }
    }
    Ok(())
}

async fn publish_history_events(
    state: &AppState,
    events: &[EventEnvelope<WorkflowEvent>],
) -> Result<()> {
    let Some(publisher) = state.publisher.as_ref() else {
        return Ok(());
    };
    publisher.publish_all(events).await
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
        omit_success_output: false,
        item_count: record.item_count,
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
        schedule_to_start_timeout_ms: task.schedule_to_start_timeout_ms,
        schedule_to_close_timeout_ms: task.schedule_to_close_timeout_ms,
        start_to_close_timeout_ms: task.start_to_close_timeout_ms,
        heartbeat_timeout_ms: task.heartbeat_timeout_ms,
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
        schedule_to_close_timeout_ms: record.schedule_to_close_timeout_ms,
        schedule_to_start_timeout_ms: record.schedule_to_start_timeout_ms,
        start_to_close_timeout_ms: record.start_to_close_timeout_ms,
        heartbeat_timeout_ms: record.heartbeat_timeout_ms,
        scheduled_at: record.scheduled_at,
        cancellation_requested: record.cancellation_requested,
        omit_success_output: false,
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

fn activity_proto(leased: &LeasedActivity, supports_cbor: bool) -> ActivityTask {
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
        input_json: if supports_cbor {
            String::new()
        } else {
            serde_json::to_string(&leased.task.input).unwrap_or_else(|_| "null".to_owned())
        },
        input_cbor: if supports_cbor {
            encode_cbor(&leased.task.input, "activity task input").unwrap_or_default()
        } else {
            Vec::new()
        },
        config_json: if supports_cbor {
            String::new()
        } else {
            leased
                .task
                .config
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .ok()
                .flatten()
                .unwrap_or_default()
        },
        config_cbor: if supports_cbor {
            leased
                .task
                .config
                .as_ref()
                .and_then(|config| encode_cbor(config, "activity task config").ok())
                .unwrap_or_default()
        } else {
            Vec::new()
        },
        prefer_cbor: supports_cbor,
        state: leased.task.state.clone(),
        scheduled_at_unix_ms: leased.task.scheduled_at.timestamp_millis(),
        lease_expires_at_unix_ms: leased.lease_expires_at.timestamp_millis(),
        start_to_close_timeout_ms: leased.task.start_to_close_timeout_ms.unwrap_or_default(),
        heartbeat_timeout_ms: leased.task.heartbeat_timeout_ms.unwrap_or_default(),
        cancellation_requested: leased.task.cancellation_requested,
        schedule_to_start_timeout_ms: leased.task.schedule_to_start_timeout_ms.unwrap_or_default(),
        lease_epoch: leased.task.lease_epoch,
        owner_epoch: leased.owner_epoch,
        omit_success_output: leased.task.omit_success_output,
        schedule_to_close_timeout_ms: leased.task.schedule_to_close_timeout_ms.unwrap_or_default(),
    }
}

fn parse_fanout_activity_id(activity_id: &str) -> Option<(&str, usize)> {
    let (origin_state, index) = activity_id.rsplit_once("::")?;
    Some((origin_state, index.parse().ok()?))
}

fn should_omit_success_output(
    artifact: &CompiledWorkflowArtifact,
    wait_state: &str,
    activity_id: &str,
) -> bool {
    if !matches!(
        artifact.workflow.states.get(wait_state),
        Some(fabrik_workflow::CompiledStateNode::WaitForAllActivities { .. })
    ) {
        return false;
    }
    let Some((origin_state, _)) = parse_fanout_activity_id(activity_id) else {
        return false;
    };
    matches!(
        artifact.workflow.states.get(origin_state),
        Some(fabrik_workflow::CompiledStateNode::FanOut { reducer, .. })
            if matches!(reducer.as_deref().unwrap_or("collect_results"), "all_settled" | "count")
    )
}

fn schedule_activities_from_plan(
    artifact: &CompiledWorkflowArtifact,
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
            schedule_to_start_timeout_ms,
            schedule_to_close_timeout_ms,
            start_to_close_timeout_ms,
            heartbeat_timeout_ms,
            ..
        } = &emission.event
        {
            let wait_state = state
                .clone()
                .or_else(|| emission.state.clone())
                .unwrap_or_else(|| instance.current_state.clone().unwrap_or_default());
            let task_queue =
                resolve_activity_task_queue(&instance.workflow_task_queue, task_queue.as_str());
            tasks.push(QueuedActivity {
                tenant_id: instance.tenant_id.clone(),
                definition_id: instance.definition_id.clone(),
                definition_version: instance.definition_version.unwrap_or(plan.workflow_version),
                artifact_hash: instance.artifact_hash.clone().unwrap_or_default(),
                instance_id: instance.instance_id.clone(),
                run_id: instance.run_id.clone(),
                activity_id: activity_id.clone(),
                activity_type: activity_type.clone(),
                task_queue,
                attempt: *attempt,
                input: input.clone(),
                config: config.clone(),
                state: wait_state.clone(),
                schedule_to_start_timeout_ms: *schedule_to_start_timeout_ms,
                schedule_to_close_timeout_ms: *schedule_to_close_timeout_ms,
                start_to_close_timeout_ms: *start_to_close_timeout_ms,
                heartbeat_timeout_ms: *heartbeat_timeout_ms,
                scheduled_at,
                cancellation_requested: false,
                omit_success_output: should_omit_success_output(artifact, &wait_state, activity_id),
                lease_epoch: 0,
            });
        }
    }
    Ok(tasks)
}

fn resolve_activity_task_queue(workflow_task_queue: &str, scheduled_task_queue: &str) -> String {
    if scheduled_task_queue.is_empty() || scheduled_task_queue == "default" {
        workflow_task_queue.to_owned()
    } else {
        scheduled_task_queue.to_owned()
    }
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
    let (
        schedule_to_start_timeout_ms,
        schedule_to_close_timeout_ms,
        start_to_close_timeout_ms,
        heartbeat_timeout_ms,
    ) = runtime.artifact.step_timeouts(activity_id, &execution_state)?;
    let due_at = now + retry_delay_for_attempt(retry, next_attempt)?;
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
            schedule_to_start_timeout_ms,
            schedule_to_close_timeout_ms,
            start_to_close_timeout_ms,
            heartbeat_timeout_ms,
            scheduled_at: due_at,
            cancellation_requested: false,
            omit_success_output: active.omit_success_output,
            lease_epoch: 0,
        },
        due_at,
    }))
}

fn retry_delay_for_attempt(retry: &RetryPolicy, next_attempt: u32) -> Result<chrono::TimeDelta> {
    let base_delay = parse_timer_ref(&retry.delay)?;
    let Some(backoff_millis) = retry.backoff_coefficient_millis else {
        return Ok(cap_retry_delay(base_delay, retry.maximum_interval.as_deref())?);
    };
    let exponent = next_attempt.saturating_sub(2);
    if exponent == 0 {
        return Ok(cap_retry_delay(base_delay, retry.maximum_interval.as_deref())?);
    }
    let coefficient = backoff_millis as f64 / 1000.0;
    let scaled_ms = (base_delay.num_milliseconds() as f64) * coefficient.powi(exponent as i32);
    let scaled_ms = scaled_ms.round().clamp(0.0, i64::MAX as f64) as i64;
    cap_retry_delay(chrono::TimeDelta::milliseconds(scaled_ms), retry.maximum_interval.as_deref())
}

fn cap_retry_delay(
    delay: chrono::TimeDelta,
    maximum_interval: Option<&str>,
) -> Result<chrono::TimeDelta> {
    let Some(maximum_interval) = maximum_interval else {
        return Ok(delay);
    };
    let maximum = parse_timer_ref(maximum_interval)?;
    Ok(if delay > maximum { maximum } else { delay })
}

fn synthetic_runtime_event(
    payload: WorkflowEvent,
    result: &ActivityTaskResult,
    definition_id: &str,
    definition_version: u32,
    artifact_hash: &str,
    occurred_at: DateTime<Utc>,
) -> EventEnvelope<WorkflowEvent> {
    let event_type = payload.event_type();
    let mut envelope = EventEnvelope::new(
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
    .with_occurred_at(occurred_at);
    envelope.dedupe_key = Some(format!(
        "runtime:{}:{}:{}:{}",
        result.run_id, result.activity_id, result.attempt, event_type
    ));
    envelope
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
        instances: inner
            .instances
            .iter()
            .filter_map(|(run_key, runtime)| {
                if runtime_can_skip_local_persistence(inner, run_key, runtime) {
                    return None;
                }
                let mut runtime = runtime.clone();
                runtime.instance.compact_for_persistence();
                Some(runtime)
            })
            .collect(),
        ready: inner.ready.values().flat_map(|queue| queue.iter().cloned()).collect(),
        leased: inner.leased.values().cloned().collect(),
        delayed_retries: flatten_delayed_retries(&inner.delayed_retries),
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
    for mut runtime in restored.instances {
        runtime.instance.expand_after_persistence_with_artifact(Some(&runtime.artifact));
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
            push_delayed_retry(&mut inner, retry);
        }
    }

    let mut debug = debug.lock().expect("unified debug lock poisoned");
    debug.restored_from_snapshot = true;
    debug.restore_records_applied = inner.instances.len() as u64;
    inner
}

fn runtime_can_skip_local_persistence(
    inner: &RuntimeInner,
    run_key: &RunKey,
    runtime: &RuntimeWorkflowState,
) -> bool {
    if !runtime.active_activities.is_empty() || run_has_local_work(inner, run_key) {
        return false;
    }
    let Some(wait_state) = runtime.instance.current_state.as_deref() else {
        return false;
    };
    let Some(execution_state) = runtime.instance.artifact_execution.as_ref() else {
        return false;
    };
    runtime.artifact.waits_on_bulk_activity(wait_state, execution_state)
}

fn run_has_local_work(inner: &RuntimeInner, run_key: &RunKey) -> bool {
    inner.ready.values().any(|queue| {
        queue.iter().any(|task| {
            task.tenant_id == run_key.tenant_id
                && task.instance_id == run_key.instance_id
                && task.run_id == run_key.run_id
        })
    }) || inner.leased.values().any(|leased| {
        leased.task.tenant_id == run_key.tenant_id
            && leased.task.instance_id == run_key.instance_id
            && leased.task.run_id == run_key.run_id
    }) || inner.delayed_retries.values().any(|retries| {
        retries.iter().any(|retry| {
            retry.task.tenant_id == run_key.tenant_id
                && retry.task.instance_id == run_key.instance_id
                && retry.task.run_id == run_key.run_id
        })
    })
}

async fn restore_runtime_from_instance_row(
    store: &WorkflowStore,
    payload_store: &PayloadStore,
    mut instance: WorkflowInstanceState,
    artifact: Option<CompiledWorkflowArtifact>,
) -> Result<(RunKey, RuntimeWorkflowState, Vec<QueuedActivity>)> {
    let definition_version = instance.definition_version.ok_or_else(|| {
        anyhow::anyhow!(
            "instance missing definition version for {}/{}",
            instance.tenant_id,
            instance.instance_id
        )
    })?;
    let artifact = match artifact {
        Some(artifact) => artifact,
        None => store
            .get_artifact_version(&instance.tenant_id, &instance.definition_id, definition_version)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "missing artifact {} v{} for instance {}/{}",
                    instance.definition_id,
                    definition_version,
                    instance.tenant_id,
                    instance.instance_id
                )
            })?,
    };
    instance.expand_after_persistence_with_artifact(Some(&artifact));
    rehydrate_stream_v2_trigger_state_for_restore(store, payload_store, &mut instance, &artifact)
        .await?;
    let activities = store
        .list_activities_for_run(&instance.tenant_id, &instance.instance_id, &instance.run_id)
        .await?;
    let mut active_activities = BTreeMap::new();
    let mut ready = Vec::new();
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
                    omit_success_output: false,
                },
            );
        }
        ready.push(task);
    }

    let run_key = RunKey {
        tenant_id: instance.tenant_id.clone(),
        instance_id: instance.instance_id.clone(),
        run_id: instance.run_id.clone(),
    };
    Ok((run_key, RuntimeWorkflowState { artifact, instance, active_activities }, ready))
}

async fn rehydrate_stream_v2_trigger_state_for_restore(
    store: &WorkflowStore,
    payload_store: &PayloadStore,
    instance: &mut WorkflowInstanceState,
    artifact: &CompiledWorkflowArtifact,
) -> Result<()> {
    if let Some(handle_value) = instance.persisted_input_handle.clone() {
        let handle = serde_json::from_value(handle_value)
            .context("failed to decode persisted workflow input handle")?;
        let input = payload_store.read_value(&handle).await.context(format!(
            "failed to read persisted workflow input for {}/{}",
            instance.tenant_id, instance.instance_id
        ))?;
        instance.restore_trigger_bulk_wait_from_input(artifact, &input)?;
        return Ok(());
    }
    if instance.last_event_type != "WorkflowTriggered"
        || instance.input.is_some()
        || instance.artifact_execution.is_some()
    {
        return Ok(());
    }
    let event =
        store.get_workflow_event_outbox(instance.last_event_id).await?.ok_or_else(|| {
            anyhow::anyhow!(
                "missing workflow trigger outbox row for restore {}/{}/{} {}",
                instance.tenant_id,
                instance.instance_id,
                instance.run_id,
                instance.last_event_id
            )
        })?;
    let WorkflowEvent::WorkflowTriggered { input } = event.event.payload else {
        anyhow::bail!(
            "workflow trigger restore expected WorkflowTriggered payload for {}/{}/{}",
            instance.tenant_id,
            instance.instance_id,
            instance.run_id
        );
    };
    instance.restore_trigger_bulk_wait_from_input(artifact, &input)?;
    Ok(())
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

fn decode_normal_activity_output(
    completed: &ActivityTaskCompletedResult,
    omit_success_output: bool,
) -> Result<Value> {
    if !completed.output_cbor.is_empty() {
        return decode_cbor::<Value>(&completed.output_cbor, "activity result output");
    }
    if !completed.output_json.trim().is_empty() {
        return Ok(parse_json_or_string(&completed.output_json));
    }
    if omit_success_output {
        return Ok(Value::Null);
    }
    Ok(Value::Null)
}

fn decode_cancelled_metadata(cancelled: &ActivityTaskCancelledResult) -> Result<Option<Value>> {
    if !cancelled.metadata_cbor.is_empty() {
        let value =
            decode_cbor::<Value>(&cancelled.metadata_cbor, "activity cancellation metadata")?;
        return Ok((!value.is_null()).then_some(value));
    }
    Ok(parse_optional_json(&cancelled.metadata_json))
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

fn bulk_terminal_payload(
    batch_id: &str,
    status: &str,
    message: Option<&str>,
    total_items: u32,
    succeeded_items: u32,
    failed_items: u32,
    cancelled_items: u32,
    chunk_count: u32,
    reducer_output: Option<&Value>,
) -> Value {
    let mut payload = serde_json::json!({
        "batchId": batch_id,
        "status": status,
        "totalItems": total_items,
        "succeededItems": succeeded_items,
        "failedItems": failed_items,
        "cancelledItems": cancelled_items,
        "chunkCount": chunk_count,
    });
    if status == "completed" {
        payload["resultHandle"] = serde_json::json!({ "batchId": batch_id });
    }
    if let Some(reducer_output) = reducer_output {
        payload["reducerOutput"] = reducer_output.clone();
    }
    if let Some(message) = message {
        payload["message"] = Value::String(message.to_owned());
    }
    payload
}

fn emitted_bulk_batch_event(
    instance: &WorkflowInstanceState,
    payload: &WorkflowEvent,
    throughput_backend: &str,
    routing_reason: &str,
    admission_policy_version: &str,
    source_event_id: Uuid,
    occurred_at: DateTime<Utc>,
) -> EventEnvelope<WorkflowEvent> {
    let WorkflowEvent::BulkActivityBatchScheduled { batch_id, .. } = payload else {
        unreachable!("bulk batch emitter only accepts bulk schedule events");
    };
    let mut envelope = EventEnvelope::new(
        payload.event_type(),
        WorkflowIdentity::new(
            instance.tenant_id.clone(),
            instance.definition_id.clone(),
            instance.definition_version.unwrap_or_default(),
            instance.artifact_hash.clone().unwrap_or_default(),
            instance.instance_id.clone(),
            instance.run_id.clone(),
            "unified-runtime",
        ),
        payload.clone(),
    )
    .with_occurred_at(occurred_at);
    envelope.event_id = unified_bulk_batch_event_id(source_event_id, batch_id);
    envelope.causation_id = Some(source_event_id);
    envelope.correlation_id = Some(source_event_id);
    envelope.metadata.insert("throughput_backend".to_owned(), throughput_backend.to_owned());
    envelope.metadata.insert("routing_reason".to_owned(), routing_reason.to_owned());
    envelope
        .metadata
        .insert("admission_policy_version".to_owned(), admission_policy_version.to_owned());
    envelope
}

async fn start_throughput_run_command(
    _state: &AppState,
    instance: &WorkflowInstanceState,
    batch_id: &str,
    activity_type: &str,
    task_queue: &str,
    total_items: usize,
    result_handle: &Value,
    workflow_state: Option<String>,
    chunk_size: u32,
    max_attempts: u32,
    retry_delay_ms: u64,
    aggregation_group_count: u32,
    execution_policy: Option<String>,
    reducer: Option<String>,
    throughput_backend: &str,
    throughput_backend_version: &str,
    admission: &BulkAdmissionDecision,
    input_handle: PayloadHandle,
    source_event_id: Uuid,
    occurred_at: DateTime<Utc>,
) -> Result<ThroughputCommandEnvelope> {
    let result_handle: PayloadHandle = serde_json::from_value(result_handle.clone())
        .context("failed to deserialize bulk result handle for throughput command")?;
    let dedupe_key = format!("throughput-start:{source_event_id}:{batch_id}");
    Ok(ThroughputCommandEnvelope {
        command_id: Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!("unified-throughput-start:{source_event_id}:{batch_id}").as_bytes(),
        ),
        occurred_at,
        dedupe_key: dedupe_key.clone(),
        partition_key: throughput_partition_key(batch_id, 0),
        payload: ThroughputCommand::StartThroughputRun(StartThroughputRunCommand {
            dedupe_key,
            tenant_id: instance.tenant_id.clone(),
            definition_id: instance.definition_id.clone(),
            definition_version: instance.definition_version,
            artifact_hash: instance.artifact_hash.clone(),
            instance_id: instance.instance_id.clone(),
            run_id: instance.run_id.clone(),
            batch_id: batch_id.to_owned(),
            activity_type: activity_type.to_owned(),
            task_queue: task_queue.to_owned(),
            state: workflow_state,
            chunk_size,
            max_attempts,
            retry_delay_ms,
            total_items: total_items as u32,
            aggregation_group_count,
            execution_policy,
            reducer,
            throughput_backend: throughput_backend.to_owned(),
            throughput_backend_version: throughput_backend_version.to_owned(),
            routing_reason: admission.routing_reason.clone(),
            admission_policy_version: admission.admission_policy_version.clone(),
            input_handle,
            result_handle,
        }),
    })
}

fn unified_failure_allows_retry(attempt: u32, retry: &RetryPolicy, error: &str) -> bool {
    attempt < retry.max_attempts && retry_policy_allows_failure_retry(retry, error)
}

fn unified_bulk_batch_event_id(source_event_id: Uuid, batch_id: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("unified-bulk-batch:{source_event_id}:{batch_id}").as_bytes(),
    )
}

fn unified_child_trigger_event_id(parent_event_id: Uuid, child_id: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("unified-child-trigger:{parent_event_id}:{child_id}").as_bytes(),
    )
}

fn unified_continue_trigger_event_id(source_event_id: Uuid, new_run_id: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("unified-continue-trigger:{source_event_id}:{new_run_id}").as_bytes(),
    )
}

fn unified_continue_event_id(source_event_id: Uuid, new_run_id: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("unified-continue:{source_event_id}:{new_run_id}").as_bytes(),
    )
}

fn unified_child_reflection_event_id(
    child_event_id: Uuid,
    parent_run_id: &str,
    child_id: &str,
    kind: &str,
) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("unified-child-reflection:{child_event_id}:{parent_run_id}:{child_id}:{kind}")
            .as_bytes(),
    )
}

fn prepared_update_actions_from_plan(
    run_key: &RunKey,
    plan: &CompiledExecutionPlan,
    completed_event_id: Uuid,
    completed_at: DateTime<Utc>,
) -> Vec<PreparedDbAction> {
    let mut actions = Vec::new();
    for emission in &plan.emissions {
        match &emission.event {
            WorkflowEvent::WorkflowUpdateCompleted { update_id, output, .. } => {
                actions.push(PreparedDbAction::CompleteUpdate {
                    tenant_id: run_key.tenant_id.clone(),
                    instance_id: run_key.instance_id.clone(),
                    run_id: run_key.run_id.clone(),
                    update_id: update_id.clone(),
                    output: Some(output.clone()),
                    error: None,
                    completed_event_id,
                    completed_at,
                });
            }
            WorkflowEvent::WorkflowUpdateRejected { update_id, error, .. } => {
                actions.push(PreparedDbAction::CompleteUpdate {
                    tenant_id: run_key.tenant_id.clone(),
                    instance_id: run_key.instance_id.clone(),
                    run_id: run_key.run_id.clone(),
                    update_id: update_id.clone(),
                    output: None,
                    error: Some(error.clone()),
                    completed_event_id,
                    completed_at,
                });
            }
            _ => {}
        }
    }
    actions
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
    use fabrik_store::{WorkflowMailboxStatus, WorkflowUpdateStatus};
    use fabrik_workflow::{
        ArtifactEntrypoint, CompiledStateNode, CompiledUpdateHandler, CompiledWorkflow,
        ErrorTransition, Expression, ParentClosePolicy,
    };
    use serde_json::json;
    use std::{
        collections::{BTreeMap, BTreeSet},
        net::TcpListener,
        process::Command,
        time::{Duration as StdDuration, Instant},
    };
    use tokio::time::sleep;
    use tonic::Request;

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
            schedule_to_start_timeout_ms: None,
            schedule_to_close_timeout_ms: None,
            start_to_close_timeout_ms: None,
            heartbeat_timeout_ms: None,
            scheduled_at,
            cancellation_requested: false,
            omit_success_output: false,
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

    fn continue_as_new_artifact() -> CompiledWorkflowArtifact {
        CompiledWorkflowArtifact::new(
            "continue-as-new-demo".to_owned(),
            1,
            "unified-runtime-test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            CompiledWorkflow {
                initial_state: "decide".to_owned(),
                states: BTreeMap::from([
                    (
                        "decide".to_owned(),
                        CompiledStateNode::Choice {
                            condition: Expression::Binary {
                                op: fabrik_workflow::BinaryOp::GreaterThan,
                                left: Box::new(Expression::Member {
                                    object: Box::new(Expression::Identifier {
                                        name: "input".to_owned(),
                                    }),
                                    property: "remaining".to_owned(),
                                }),
                                right: Box::new(Expression::Literal { value: json!(0) }),
                            },
                            then_next: "roll".to_owned(),
                            else_next: "done".to_owned(),
                        },
                    ),
                    (
                        "roll".to_owned(),
                        CompiledStateNode::ContinueAsNew {
                            input: Some(Expression::Object {
                                fields: BTreeMap::from([(
                                    "remaining".to_owned(),
                                    Expression::Binary {
                                        op: fabrik_workflow::BinaryOp::Subtract,
                                        left: Box::new(Expression::Member {
                                            object: Box::new(Expression::Identifier {
                                                name: "input".to_owned(),
                                            }),
                                            property: "remaining".to_owned(),
                                        }),
                                        right: Box::new(Expression::Literal { value: json!(1) }),
                                    },
                                )]),
                            }),
                        },
                    ),
                    (
                        "done".to_owned(),
                        CompiledStateNode::Succeed {
                            output: Some(Expression::Literal { value: json!({ "done": true }) }),
                        },
                    ),
                ]),
                params: Vec::new(),
                non_cancellable_states: std::collections::BTreeSet::new(),
            },
        )
    }

    fn timer_artifact() -> CompiledWorkflowArtifact {
        CompiledWorkflowArtifact::new(
            "timer-demo".to_owned(),
            1,
            "unified-runtime-test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            CompiledWorkflow {
                initial_state: "wait".to_owned(),
                states: BTreeMap::from([
                    (
                        "wait".to_owned(),
                        CompiledStateNode::WaitForTimer {
                            timer_ref: "1s".to_owned(),
                            timer_expr: None,
                            next: "done".to_owned(),
                        },
                    ),
                    (
                        "done".to_owned(),
                        CompiledStateNode::Succeed {
                            output: Some(Expression::Literal { value: json!({ "done": true }) }),
                        },
                    ),
                ]),
                params: Vec::new(),
                non_cancellable_states: std::collections::BTreeSet::new(),
            },
        )
    }

    fn fanout_artifact_with_reducer(reducer: Option<&str>) -> CompiledWorkflowArtifact {
        CompiledWorkflowArtifact::new(
            "fanout-demo".to_owned(),
            1,
            "unified-runtime-test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            CompiledWorkflow {
                initial_state: "dispatch".to_owned(),
                states: BTreeMap::from([
                    (
                        "dispatch".to_owned(),
                        CompiledStateNode::FanOut {
                            activity_type: "benchmark.echo".to_owned(),
                            items: Expression::Member {
                                object: Box::new(Expression::Identifier {
                                    name: "input".to_owned(),
                                }),
                                property: "items".to_owned(),
                            },
                            next: "join".to_owned(),
                            handle_var: "fanout".to_owned(),
                            task_queue: None,
                            reducer: reducer.map(str::to_owned),
                            retry: None,
                            config: None,
                            schedule_to_start_timeout_ms: None,
                            schedule_to_close_timeout_ms: None,
                            start_to_close_timeout_ms: None,
                            heartbeat_timeout_ms: None,
                        },
                    ),
                    (
                        "join".to_owned(),
                        CompiledStateNode::WaitForAllActivities {
                            fanout_ref_var: "fanout".to_owned(),
                            next: "done".to_owned(),
                            output_var: Some("result".to_owned()),
                            on_error: None,
                        },
                    ),
                    (
                        "done".to_owned(),
                        CompiledStateNode::Succeed {
                            output: Some(Expression::Identifier { name: "result".to_owned() }),
                        },
                    ),
                ]),
                params: Vec::new(),
                non_cancellable_states: std::collections::BTreeSet::new(),
            },
        )
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
            persisted_input_handle: None,
            memo: None,
            search_attributes: None,
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
                    omit_success_output: false,
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

    #[test]
    fn admission_prefers_stream_v2_for_large_mergeable_batches() {
        let decision = choose_bulk_admission_backend(
            &BulkAdmissionConfig {
                default_backend: ThroughputBackend::PgV1.as_str().to_owned(),
                task_queue_backends: BTreeMap::new(),
                stream_v2_min_items: 64,
                stream_v2_min_chunks: 8,
                max_active_chunks_per_tenant: 4_096,
                max_active_chunks_per_task_queue: 2_048,
            },
            None,
            None,
            None,
            Some("all_settled"),
            512,
            32,
        );
        assert_eq!(decision.selected_backend, ThroughputBackend::StreamV2.as_str());
        assert_eq!(decision.routing_reason, "mergeable_scale");
        assert_eq!(decision.admission_policy_version, ADMISSION_POLICY_VERSION);
    }

    #[test]
    fn admission_respects_task_queue_backend_override() {
        let decision = choose_bulk_admission_backend(
            &BulkAdmissionConfig {
                default_backend: ThroughputBackend::PgV1.as_str().to_owned(),
                task_queue_backends: BTreeMap::new(),
                stream_v2_min_items: 512,
                stream_v2_min_chunks: 8,
                max_active_chunks_per_tenant: 4_096,
                max_active_chunks_per_task_queue: 2_048,
            },
            None,
            Some(ThroughputBackend::StreamV2.as_str()),
            None,
            Some("collect_results"),
            8,
            1,
        );
        assert_eq!(decision.selected_backend, ThroughputBackend::StreamV2.as_str());
        assert_eq!(decision.routing_reason, "task_queue_policy_override");
    }

    #[test]
    fn admission_routes_numeric_mergeable_reducer_to_stream_v2() {
        let decision = choose_bulk_admission_backend(
            &BulkAdmissionConfig {
                default_backend: ThroughputBackend::PgV1.as_str().to_owned(),
                task_queue_backends: BTreeMap::new(),
                stream_v2_min_items: 64,
                stream_v2_min_chunks: 8,
                max_active_chunks_per_tenant: 4_096,
                max_active_chunks_per_task_queue: 2_048,
            },
            None,
            Some(ThroughputBackend::StreamV2.as_str()),
            None,
            Some("sum"),
            512,
            32,
        );
        assert_eq!(decision.selected_backend, ThroughputBackend::StreamV2.as_str());
        assert_eq!(decision.routing_reason, "task_queue_policy_override");
    }

    #[tokio::test]
    async fn admission_bypasses_stale_task_queue_cache_for_explicit_backend_requests() -> Result<()>
    {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir =
            std::env::temp_dir().join(format!("unified-cache-bypass-{}", Uuid::now_v7()));
        let state = test_app_state(store.clone(), RuntimeInner::default(), state_dir).await;
        let tenant_id = "benchmark";
        let task_queue = "default";

        store
            .upsert_task_queue_throughput_policy(
                tenant_id,
                TaskQueueKind::Activity,
                task_queue,
                ThroughputBackend::StreamV2.as_str(),
            )
            .await?;
        let cached =
            cached_task_queue_throughput_backend(&state, tenant_id, task_queue, false).await?;
        assert_eq!(cached.as_deref(), Some(ThroughputBackend::StreamV2.as_str()));

        store
            .upsert_task_queue_throughput_policy(
                tenant_id,
                TaskQueueKind::Activity,
                task_queue,
                ThroughputBackend::PgV1.as_str(),
            )
            .await?;

        let decision = admit_bulk_batch(
            &state,
            tenant_id,
            task_queue,
            Some(ThroughputBackend::PgV1.as_str()),
            None,
            Some("max"),
            1024,
            64,
        )
        .await?;

        assert_eq!(decision.selected_backend, ThroughputBackend::PgV1.as_str());
        assert_eq!(decision.routing_reason, "task_queue_policy_override");
        Ok(())
    }

    #[test]
    fn prune_terminal_instances_removes_completed_runs_and_work() {
        let now = Utc::now();
        let terminal_run = run_key_for("terminal");
        let active_run = run_key_for("active");
        let mut terminal_runtime =
            runtime_workflow_for_run("terminal", WorkflowStatus::Completed, 2, now);
        terminal_runtime.active_activities.clear();
        let active_runtime = runtime_workflow_for_run("active", WorkflowStatus::Running, 1, now);
        let terminal_task = queued_activity_for_run("terminal", "activity-terminal", 1, now);
        let active_task = queued_activity_for_run("active", "activity-active", 1, now);
        let terminal_attempt = attempt_key_from_task(&terminal_task);
        let active_attempt = attempt_key_from_task(&active_task);
        let mut inner = RuntimeInner::default();
        inner.instances.insert(terminal_run.clone(), terminal_runtime);
        inner.instances.insert(active_run.clone(), active_runtime);
        inner
            .ready
            .entry(QueueKey { tenant_id: "tenant".to_owned(), task_queue: "default".to_owned() })
            .or_default()
            .push_back(terminal_task.clone());
        inner
            .ready
            .entry(QueueKey { tenant_id: "tenant".to_owned(), task_queue: "default".to_owned() })
            .or_default()
            .push_back(active_task.clone());
        inner.leased.insert(
            terminal_attempt,
            LeasedActivity {
                task: terminal_task.clone(),
                worker_id: "worker-terminal".to_owned(),
                worker_build_id: "build".to_owned(),
                lease_expires_at: now + ChronoDuration::seconds(30),
                owner_epoch: 1,
            },
        );
        inner.leased.insert(
            active_attempt,
            LeasedActivity {
                task: active_task.clone(),
                worker_id: "worker-active".to_owned(),
                worker_build_id: "build".to_owned(),
                lease_expires_at: now + ChronoDuration::seconds(30),
                owner_epoch: 1,
            },
        );
        push_delayed_retry(
            &mut inner,
            DelayedRetryTask { task: terminal_task, due_at: now + ChronoDuration::seconds(10) },
        );
        push_delayed_retry(
            &mut inner,
            DelayedRetryTask {
                task: active_task.clone(),
                due_at: now + ChronoDuration::seconds(10),
            },
        );

        prune_terminal_instances(&mut inner);

        assert!(!inner.instances.contains_key(&terminal_run));
        assert!(inner.instances.contains_key(&active_run));
        assert!(inner.ready.values().flatten().all(|task| task.run_id != "terminal"));
        assert!(inner.ready.values().flatten().any(|task| task.run_id == "active"));
        assert!(inner.leased.values().all(|leased| leased.task.run_id != "terminal"));
        assert!(inner.leased.values().any(|leased| leased.task.run_id == "active"));
        assert!(
            inner.delayed_retries.values().flatten().all(|retry| retry.task.run_id != "terminal")
        );
        assert!(
            inner.delayed_retries.values().flatten().any(|retry| retry.task.run_id == "active")
        );
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
                schedule_to_start_timeout_ms: None,
                schedule_to_close_timeout_ms: None,
                start_to_close_timeout_ms: None,
                heartbeat_timeout_ms: None,
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
            CompiledWorkflow {
                initial_state: "dispatch".to_owned(),
                states,
                params: Vec::new(),
                non_cancellable_states: std::collections::BTreeSet::new(),
            },
        )
    }

    fn throughput_artifact(task_queue: &str) -> CompiledWorkflowArtifact {
        throughput_artifact_with_options(
            task_queue,
            Some(ThroughputBackend::PgV1.as_str()),
            Some("all_settled"),
            16,
        )
    }

    fn throughput_artifact_with_options(
        task_queue: &str,
        throughput_backend: Option<&str>,
        reducer: Option<&str>,
        chunk_size: u32,
    ) -> CompiledWorkflowArtifact {
        let mut states = BTreeMap::new();
        states.insert(
            "dispatch".to_owned(),
            CompiledStateNode::StartBulkActivity {
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
                execution_policy: None,
                reducer: reducer.map(str::to_owned),
                throughput_backend: throughput_backend.map(str::to_owned),
                chunk_size: Some(chunk_size),
                retry: None,
            },
        );
        states.insert(
            "join".to_owned(),
            CompiledStateNode::WaitForBulkActivity {
                bulk_ref_var: "fanout".to_owned(),
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
            "throughput-handoff-demo".to_owned(),
            1,
            "unified-runtime-test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            CompiledWorkflow {
                initial_state: "dispatch".to_owned(),
                states,
                params: Vec::new(),
                non_cancellable_states: std::collections::BTreeSet::new(),
            },
        )
    }

    fn child_parent_artifact() -> CompiledWorkflowArtifact {
        let workflow = CompiledWorkflow {
            initial_state: "start_child".to_owned(),
            states: BTreeMap::from([
                (
                    "start_child".to_owned(),
                    CompiledStateNode::StartChild {
                        child_definition_id: "childDefinition".to_owned(),
                        input: Expression::Literal { value: json!({"value": "hello-child"}) },
                        next: "await_child".to_owned(),
                        handle_var: Some("child".to_owned()),
                        workflow_id: Some(Expression::Literal {
                            value: Value::String("child-instance".to_owned()),
                        }),
                        task_queue: None,
                        parent_close_policy: ParentClosePolicy::Terminate,
                    },
                ),
                (
                    "await_child".to_owned(),
                    CompiledStateNode::WaitForChild {
                        child_ref_var: "child".to_owned(),
                        next: "done".to_owned(),
                        output_var: Some("child_result".to_owned()),
                        on_error: None,
                    },
                ),
                (
                    "done".to_owned(),
                    CompiledStateNode::Succeed {
                        output: Some(Expression::Identifier { name: "child_result".to_owned() }),
                    },
                ),
            ]),
            params: Vec::new(),
            non_cancellable_states: std::collections::BTreeSet::new(),
        };
        let mut artifact = CompiledWorkflowArtifact::new(
            "unified-parent-child".to_owned(),
            1,
            "unified-runtime-test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            workflow,
        );
        artifact.artifact_hash = artifact.hash();
        artifact
    }

    fn child_leaf_artifact() -> CompiledWorkflowArtifact {
        let workflow = CompiledWorkflow {
            initial_state: "done".to_owned(),
            states: BTreeMap::from([(
                "done".to_owned(),
                CompiledStateNode::Succeed {
                    output: Some(Expression::Member {
                        object: Box::new(Expression::Identifier { name: "input".to_owned() }),
                        property: "value".to_owned(),
                    }),
                },
            )]),
            params: Vec::new(),
            non_cancellable_states: std::collections::BTreeSet::new(),
        };
        let mut artifact = CompiledWorkflowArtifact::new(
            "childDefinition".to_owned(),
            1,
            "unified-runtime-test",
            ArtifactEntrypoint { module: "child.ts".to_owned(), export: "workflow".to_owned() },
            workflow,
        );
        artifact.artifact_hash = artifact.hash();
        artifact
    }

    fn child_wait_for_signal_artifact() -> CompiledWorkflowArtifact {
        let workflow = CompiledWorkflow {
            initial_state: "wait_approve".to_owned(),
            states: BTreeMap::from([
                (
                    "wait_approve".to_owned(),
                    CompiledStateNode::WaitForEvent {
                        event_type: "approve".to_owned(),
                        next: "done".to_owned(),
                        output_var: Some("approved".to_owned()),
                    },
                ),
                (
                    "done".to_owned(),
                    CompiledStateNode::Succeed {
                        output: Some(Expression::Identifier { name: "approved".to_owned() }),
                    },
                ),
            ]),
            params: Vec::new(),
            non_cancellable_states: std::collections::BTreeSet::new(),
        };
        let mut artifact = CompiledWorkflowArtifact::new(
            "childSignalDefinition".to_owned(),
            1,
            "unified-runtime-test",
            ArtifactEntrypoint {
                module: "child-signal.ts".to_owned(),
                export: "workflow".to_owned(),
            },
            workflow,
        );
        artifact.artifact_hash = artifact.hash();
        artifact
    }

    fn signal_then_step_artifact() -> CompiledWorkflowArtifact {
        let workflow = CompiledWorkflow {
            initial_state: "wait_approve".to_owned(),
            states: BTreeMap::from([
                (
                    "wait_approve".to_owned(),
                    CompiledStateNode::WaitForEvent {
                        event_type: "approve".to_owned(),
                        next: "step_greet".to_owned(),
                        output_var: Some("approved".to_owned()),
                    },
                ),
                (
                    "step_greet".to_owned(),
                    CompiledStateNode::Step {
                        handler: "greet".to_owned(),
                        input: Expression::Identifier { name: "approved".to_owned() },
                        next: Some("done".to_owned()),
                        task_queue: Some(Expression::Literal {
                            value: Value::String("orders".to_owned()),
                        }),
                        retry: None,
                        output_var: Some("greeted".to_owned()),
                        on_error: None,
                        config: None,
                        schedule_to_start_timeout_ms: None,
                        schedule_to_close_timeout_ms: None,
                        start_to_close_timeout_ms: Some(30_000),
                        heartbeat_timeout_ms: None,
                    },
                ),
                (
                    "done".to_owned(),
                    CompiledStateNode::Succeed {
                        output: Some(Expression::Identifier { name: "greeted".to_owned() }),
                    },
                ),
            ]),
            params: Vec::new(),
            non_cancellable_states: BTreeSet::new(),
        };
        let mut artifact = CompiledWorkflowArtifact::new(
            "unified-signal-step".to_owned(),
            1,
            "unified-runtime-test",
            ArtifactEntrypoint {
                module: "signal-step.ts".to_owned(),
                export: "workflow".to_owned(),
            },
            workflow,
        );
        artifact.artifact_hash = artifact.hash();
        artifact
    }

    fn child_signal_parent_artifact() -> CompiledWorkflowArtifact {
        let workflow = CompiledWorkflow {
            initial_state: "start_child".to_owned(),
            states: BTreeMap::from([
                (
                    "start_child".to_owned(),
                    CompiledStateNode::StartChild {
                        child_definition_id: "childSignalDefinition".to_owned(),
                        input: Expression::Literal { value: json!({"seed": true}) },
                        next: "signal_child".to_owned(),
                        handle_var: Some("child".to_owned()),
                        workflow_id: Some(Expression::Literal {
                            value: Value::String("child-instance-signal".to_owned()),
                        }),
                        task_queue: None,
                        parent_close_policy: ParentClosePolicy::RequestCancel,
                    },
                ),
                (
                    "signal_child".to_owned(),
                    CompiledStateNode::SignalChild {
                        child_ref_var: "child".to_owned(),
                        signal_name: "approve".to_owned(),
                        payload: Expression::Literal {
                            value: Value::String("approved".to_owned()),
                        },
                        next: "await_child".to_owned(),
                    },
                ),
                (
                    "await_child".to_owned(),
                    CompiledStateNode::WaitForChild {
                        child_ref_var: "child".to_owned(),
                        next: "done".to_owned(),
                        output_var: Some("child_result".to_owned()),
                        on_error: None,
                    },
                ),
                (
                    "done".to_owned(),
                    CompiledStateNode::Succeed {
                        output: Some(Expression::Identifier { name: "child_result".to_owned() }),
                    },
                ),
            ]),
            params: Vec::new(),
            non_cancellable_states: std::collections::BTreeSet::new(),
        };
        let mut artifact = CompiledWorkflowArtifact::new(
            "unified-parent-child-signal".to_owned(),
            1,
            "unified-runtime-test",
            ArtifactEntrypoint {
                module: "parent-child-signal.ts".to_owned(),
                export: "workflow".to_owned(),
            },
            workflow,
        );
        artifact.artifact_hash = artifact.hash();
        artifact
    }

    fn child_cancel_parent_artifact() -> CompiledWorkflowArtifact {
        let workflow = CompiledWorkflow {
            initial_state: "start_child".to_owned(),
            states: BTreeMap::from([
                (
                    "start_child".to_owned(),
                    CompiledStateNode::StartChild {
                        child_definition_id: "childSignalDefinition".to_owned(),
                        input: Expression::Literal { value: json!({"seed": true}) },
                        next: "cancel_child".to_owned(),
                        handle_var: Some("child".to_owned()),
                        workflow_id: Some(Expression::Literal {
                            value: Value::String("child-instance-cancel".to_owned()),
                        }),
                        task_queue: None,
                        parent_close_policy: ParentClosePolicy::RequestCancel,
                    },
                ),
                (
                    "cancel_child".to_owned(),
                    CompiledStateNode::CancelChild {
                        child_ref_var: "child".to_owned(),
                        reason: Some(Expression::Literal {
                            value: Value::String("stop".to_owned()),
                        }),
                        next: "await_child".to_owned(),
                    },
                ),
                (
                    "await_child".to_owned(),
                    CompiledStateNode::WaitForChild {
                        child_ref_var: "child".to_owned(),
                        next: "done".to_owned(),
                        output_var: None,
                        on_error: Some(ErrorTransition {
                            next: "done".to_owned(),
                            error_var: Some("child_error".to_owned()),
                        }),
                    },
                ),
                (
                    "done".to_owned(),
                    CompiledStateNode::Succeed {
                        output: Some(Expression::Identifier { name: "child_error".to_owned() }),
                    },
                ),
            ]),
            params: Vec::new(),
            non_cancellable_states: std::collections::BTreeSet::new(),
        };
        let mut artifact = CompiledWorkflowArtifact::new(
            "unified-parent-child-cancel".to_owned(),
            1,
            "unified-runtime-test",
            ArtifactEntrypoint {
                module: "parent-child-cancel.ts".to_owned(),
                export: "workflow".to_owned(),
            },
            workflow,
        );
        artifact.artifact_hash = artifact.hash();
        artifact
    }

    fn update_child_artifact() -> CompiledWorkflowArtifact {
        let workflow = CompiledWorkflow {
            initial_state: "wait_signal".to_owned(),
            states: BTreeMap::from([(
                "wait_signal".to_owned(),
                CompiledStateNode::WaitForEvent {
                    event_type: "ready".to_owned(),
                    next: "done".to_owned(),
                    output_var: None,
                },
            )]),
            params: Vec::new(),
            non_cancellable_states: std::collections::BTreeSet::new(),
        };
        let mut artifact = CompiledWorkflowArtifact::new(
            "unified-update-child".to_owned(),
            1,
            "unified-runtime-test",
            ArtifactEntrypoint {
                module: "update-child.ts".to_owned(),
                export: "workflow".to_owned(),
            },
            workflow,
        );
        artifact.updates.insert(
            "approve".to_owned(),
            CompiledUpdateHandler {
                arg_name: Some("args".to_owned()),
                initial_state: "start_child".to_owned(),
                states: BTreeMap::from([
                    (
                        "start_child".to_owned(),
                        CompiledStateNode::StartChild {
                            child_definition_id: "childDefinition".to_owned(),
                            input: Expression::Identifier { name: "args".to_owned() },
                            next: "await_child".to_owned(),
                            handle_var: Some("child".to_owned()),
                            workflow_id: Some(Expression::Literal {
                                value: Value::String("child-instance-update".to_owned()),
                            }),
                            task_queue: None,
                            parent_close_policy: ParentClosePolicy::RequestCancel,
                        },
                    ),
                    (
                        "await_child".to_owned(),
                        CompiledStateNode::WaitForChild {
                            child_ref_var: "child".to_owned(),
                            next: "finish".to_owned(),
                            output_var: Some("child_result".to_owned()),
                            on_error: None,
                        },
                    ),
                    (
                        "finish".to_owned(),
                        CompiledStateNode::Succeed {
                            output: Some(Expression::Identifier {
                                name: "child_result".to_owned(),
                            }),
                        },
                    ),
                ]),
            },
        );
        artifact.artifact_hash = artifact.hash();
        artifact
    }

    fn update_test_artifact() -> CompiledWorkflowArtifact {
        let workflow = CompiledWorkflow {
            initial_state: "wait".to_owned(),
            states: BTreeMap::from([
                (
                    "wait".to_owned(),
                    CompiledStateNode::WaitForEvent {
                        event_type: "resume".to_owned(),
                        next: "done".to_owned(),
                        output_var: Some("signal".to_owned()),
                    },
                ),
                ("done".to_owned(), CompiledStateNode::Succeed { output: None }),
            ]),
            params: Vec::new(),
            non_cancellable_states: std::collections::BTreeSet::new(),
        };
        let mut artifact = CompiledWorkflowArtifact::new(
            "unified-update-demo".to_owned(),
            1,
            "unified-runtime-test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            workflow,
        );
        artifact.updates.insert(
            "setValue".to_owned(),
            CompiledUpdateHandler {
                arg_name: Some("payload".to_owned()),
                initial_state: "finish".to_owned(),
                states: BTreeMap::from([(
                    "finish".to_owned(),
                    CompiledStateNode::Succeed {
                        output: Some(Expression::Identifier { name: "payload".to_owned() }),
                    },
                )]),
            },
        );
        artifact.artifact_hash = artifact.hash();
        artifact
    }

    async fn test_app_state(
        store: WorkflowStore,
        inner: RuntimeInner,
        state_dir: PathBuf,
    ) -> AppState {
        fs::create_dir_all(&state_dir).expect("create app state dir");
        let throughput_runtime =
            ThroughputRuntimeConfig::from_env().expect("load test throughput runtime config");
        let payload_store = test_payload_store(&state_dir).await;
        AppState {
            store,
            publisher: None,
            throughput_command_publisher: None,
            payload_store,
            throughput_runtime,
            inner: Arc::new(StdMutex::new(inner)),
            ownership: Arc::new(StdMutex::new(UnifiedOwnershipState::inactive(1, "test-owner"))),
            workflow_partition_count: 8,
            notify: Arc::new(Notify::new()),
            bulk_notify: Arc::new(Notify::new()),
            retry_notify: Arc::new(Notify::new()),
            outbox_notify: Arc::new(Notify::new()),
            persist_notify: Arc::new(Notify::new()),
            persist_lock: Arc::new(Mutex::new(())),
            persistence: persistence_config_for(state_dir),
            admission: BulkAdmissionConfig {
                default_backend: ThroughputBackend::PgV1.as_str().to_owned(),
                task_queue_backends: BTreeMap::new(),
                stream_v2_min_items: 512,
                stream_v2_min_chunks: 8,
                max_active_chunks_per_tenant: 4_096,
                max_active_chunks_per_task_queue: 2_048,
            },
            task_queue_policy_cache: Arc::new(StdMutex::new(HashMap::new())),
            debug: Arc::new(StdMutex::new(UnifiedDebugState::default())),
            outbox_publisher_id: "unified-runtime-test-outbox".to_owned(),
        }
    }

    async fn test_app_state_with_publisher(
        store: WorkflowStore,
        publisher: WorkflowPublisher,
        inner: RuntimeInner,
        state_dir: PathBuf,
    ) -> AppState {
        let mut state = test_app_state(store, inner, state_dir).await;
        state.publisher = Some(publisher);
        state
    }

    async fn test_payload_store(root: &Path) -> PayloadStore {
        PayloadStore::from_config(PayloadStoreConfig {
            default_store: PayloadStoreKind::LocalFilesystem,
            local_dir: root.join("payloads").display().to_string(),
            s3_bucket: None,
            s3_region: "us-east-1".to_owned(),
            s3_endpoint: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_force_path_style: false,
            s3_key_prefix: "throughput".to_owned(),
        })
        .await
        .expect("test payload store")
    }

    fn mailbox_record(
        kind: WorkflowMailboxKind,
        source_event: EventEnvelope<WorkflowEvent>,
        message_id: Option<&str>,
        message_name: Option<&str>,
        payload: Option<Value>,
    ) -> WorkflowMailboxRecord {
        let now = source_event.occurred_at;
        WorkflowMailboxRecord {
            tenant_id: source_event.tenant_id.clone(),
            instance_id: source_event.instance_id.clone(),
            run_id: source_event.run_id.clone(),
            accepted_seq: 1,
            kind,
            message_id: message_id.map(str::to_owned),
            message_name: message_name.map(str::to_owned),
            payload,
            source_event_id: source_event.event_id,
            source_event_type: source_event.event_type.clone(),
            source_event,
            status: WorkflowMailboxStatus::Queued,
            consumed_at: None,
            created_at: now,
            updated_at: now,
        }
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

    #[test]
    fn activity_proto_includes_timeouts() {
        let scheduled_at = Utc::now();
        let leased = LeasedActivity {
            task: QueuedActivity {
                schedule_to_start_timeout_ms: Some(1_000),
                start_to_close_timeout_ms: Some(30_000),
                heartbeat_timeout_ms: Some(5_000),
                ..queued_activity("activity-1", 1, scheduled_at)
            },
            worker_id: "worker-a".to_owned(),
            worker_build_id: "build-a".to_owned(),
            lease_expires_at: scheduled_at + ChronoDuration::seconds(30),
            owner_epoch: 7,
        };

        let proto = activity_proto(&leased, true);

        assert_eq!(proto.schedule_to_start_timeout_ms, 1_000);
        assert_eq!(proto.start_to_close_timeout_ms, 30_000);
        assert_eq!(proto.heartbeat_timeout_ms, 5_000);
    }

    #[test]
    fn activity_proto_populates_cbor_when_worker_supports_it() {
        let scheduled_at = Utc::now();
        let leased = LeasedActivity {
            task: QueuedActivity {
                input: json!({"ok": true}),
                config: Some(json!({"url": "https://example.com"})),
                ..queued_activity("dispatch::0", 1, scheduled_at)
            },
            worker_id: "worker-a".to_owned(),
            worker_build_id: "build-a".to_owned(),
            lease_expires_at: scheduled_at + ChronoDuration::seconds(30),
            owner_epoch: 7,
        };

        let proto = activity_proto(&leased, true);

        assert!(proto.prefer_cbor);
        assert!(proto.input_json.is_empty());
        assert!(proto.config_json.is_empty());
        assert!(!proto.input_cbor.is_empty());
        assert!(!proto.config_cbor.is_empty());
    }

    #[test]
    fn omit_success_output_only_applies_to_counter_state_fanout_reducers() {
        let all_settled = fanout_artifact_with_reducer(Some("all_settled"));
        let count = fanout_artifact_with_reducer(Some("count"));
        let collect_results = fanout_artifact_with_reducer(Some("collect_results"));
        let collect_settled = fanout_artifact_with_reducer(Some("collect_settled_results"));

        assert!(should_omit_success_output(&all_settled, "join", "dispatch::0"));
        assert!(should_omit_success_output(&count, "join", "dispatch::0"));
        assert!(!should_omit_success_output(&collect_results, "join", "dispatch::0"));
        assert!(!should_omit_success_output(&collect_settled, "join", "dispatch::0"));
        assert!(!should_omit_success_output(&all_settled, "dispatch", "dispatch::0"));
        assert!(!should_omit_success_output(&all_settled, "join", "single-step"));
    }

    #[test]
    fn deadline_ordered_retry_queue_releases_only_due_tasks() {
        let now = Utc::now();
        let mut inner = RuntimeInner::default();
        let earliest = DelayedRetryTask {
            task: queued_activity("activity-early", 1, now),
            due_at: now + ChronoDuration::milliseconds(25),
        };
        let later = DelayedRetryTask {
            task: queued_activity("activity-later", 1, now),
            due_at: now + ChronoDuration::milliseconds(50),
        };

        assert!(push_delayed_retry(&mut inner, later.clone()));
        assert!(push_delayed_retry(&mut inner, earliest.clone()));

        let released = take_due_delayed_retries(&mut inner, now + ChronoDuration::milliseconds(30));
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].activity_id, earliest.task.activity_id);
        assert_eq!(delayed_retry_count(&inner.delayed_retries), 1);
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

            let image = std::env::var("FABRIK_TEST_REDPANDA_IMAGE")
                .unwrap_or_else(|_| "docker.redpanda.com/redpandadata/redpanda:v25.1.2".to_owned());
            let topic = format!("workflow-events-test-{}", Uuid::now_v7());
            let mut last_error = None;
            let mut container_name = String::new();
            let mut kafka_port = 0_u16;
            let mut started = false;
            for _ in 0..5 {
                kafka_port = choose_free_port().context("failed to allocate kafka host port")?;
                container_name = format!("fabrik-unified-test-rp-{}", Uuid::now_v7());
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
                    .with_context(|| {
                        format!("failed to start docker container {container_name}")
                    })?;
                if output.status.success() {
                    started = true;
                    break;
                }
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
                if stderr.contains("address already in use") {
                    last_error = Some(stderr);
                    continue;
                }
                anyhow::bail!("docker failed to start redpanda test container: {stderr}");
            }
            if !started {
                anyhow::bail!(
                    "docker failed to start redpanda test container after retries: {}",
                    last_error.unwrap_or_else(|| "unknown error".to_owned())
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
                omit_success_output: false,
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
    fn unified_failure_retry_filter_respects_non_retryable_types() {
        let retry = RetryPolicy {
            max_attempts: 3,
            delay: "5s".to_owned(),
            maximum_interval: None,
            backoff_coefficient_millis: None,
            non_retryable_error_types: vec!["ValidationError".to_owned()],
        };

        assert!(unified_failure_allows_retry(1, &retry, "TransientError: retry me"));
        assert!(!unified_failure_allows_retry(
            1,
            &retry,
            r#"{"type":"ValidationError","message":"bad input"}"#
        ));
        assert!(!unified_failure_allows_retry(3, &retry, "TransientError: retry me"));
    }

    #[test]
    fn retry_delay_scales_with_backoff_and_caps_at_maximum_interval() {
        let retry = RetryPolicy {
            max_attempts: 5,
            delay: "500ms".to_owned(),
            maximum_interval: Some("1s".to_owned()),
            backoff_coefficient_millis: Some(2000),
            non_retryable_error_types: Vec::new(),
        };

        assert_eq!(retry_delay_for_attempt(&retry, 2).unwrap().num_milliseconds(), 500);
        assert_eq!(retry_delay_for_attempt(&retry, 3).unwrap().num_milliseconds(), 1000);
        assert_eq!(retry_delay_for_attempt(&retry, 4).unwrap().num_milliseconds(), 1000);
    }

    #[tokio::test]
    async fn update_mailbox_item_executes_and_completes_update_record() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let artifact = update_test_artifact();
        let now = Utc::now();
        let run_key = run_key_for("update");
        let mut instance = workflow_instance_for_run("update", WorkflowStatus::Running, 1, now);
        instance.current_state = Some("wait".to_owned());
        instance.definition_id = artifact.definition_id.clone();
        instance.definition_version = Some(artifact.definition_version);
        instance.artifact_hash = Some(artifact.artifact_hash.clone());
        instance.artifact_execution = Some(ArtifactExecutionState::default());
        store.upsert_instance(&instance).await?;
        store
            .queue_update(
                "tenant",
                "instance-update",
                "update",
                "update-1",
                "setValue",
                None,
                &json!({"ok": true}),
                Uuid::now_v7(),
                now,
            )
            .await?;

        let mut inner = RuntimeInner::default();
        inner.instances.insert(
            run_key.clone(),
            RuntimeWorkflowState { artifact, instance, active_activities: BTreeMap::new() },
        );
        let state_dir =
            std::env::temp_dir().join(format!("unified-runtime-update-test-{}", Uuid::now_v7()));
        let app_state = test_app_state(store.clone(), inner, state_dir.clone()).await;
        let requested = test_event(
            &WorkflowIdentity::new(
                "tenant",
                "definition",
                1,
                "artifact",
                "instance-update",
                "update",
                "unified-runtime-test",
            ),
            WorkflowEvent::WorkflowUpdateRequested {
                update_id: "update-1".to_owned(),
                update_name: "setValue".to_owned(),
                payload: json!({"ok": true}),
            },
            now,
        );
        let item = mailbox_record(
            WorkflowMailboxKind::Update,
            requested,
            Some("update-1"),
            Some("setValue"),
            Some(json!({"ok": true})),
        );

        let outcome = dispatch_update_mailbox_item_unified(&app_state, &item).await?;
        let MailboxDrainOutcome::Processed { general, schedules, .. } = outcome else {
            anyhow::bail!("expected processed update outcome");
        };
        apply_db_actions(&app_state, general, schedules).await?;

        let update = store
            .get_update("tenant", "instance-update", "update", "update-1")
            .await?
            .context("update record should exist")?;
        assert_eq!(update.status, WorkflowUpdateStatus::Completed);
        assert_eq!(update.output, Some(json!({"ok": true})));

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn cancel_mailbox_item_clears_run_work_and_marks_instance_terminal() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let artifact = test_artifact("default");
        let now = Utc::now();
        let run_key = run_key_for("cancel");
        let mut instance = workflow_instance_for_run("cancel", WorkflowStatus::Running, 1, now);
        instance.definition_id = artifact.definition_id.clone();
        instance.definition_version = Some(artifact.definition_version);
        instance.artifact_hash = Some(artifact.artifact_hash.clone());
        instance.artifact_execution = None;
        store.upsert_instance(&instance).await?;

        let queued = queued_activity_for_run("cancel", "activity-cancel", 1, now);
        let mut inner = RuntimeInner::default();
        inner.instances.insert(
            run_key.clone(),
            RuntimeWorkflowState {
                artifact,
                instance,
                active_activities: BTreeMap::from([(
                    queued.activity_id.clone(),
                    ActiveActivityMeta {
                        attempt: queued.attempt,
                        task_queue: queued.task_queue.clone(),
                        activity_type: queued.activity_type.clone(),
                        wait_state: queued.state.clone(),
                        omit_success_output: queued.omit_success_output,
                    },
                )]),
            },
        );
        inner
            .ready
            .entry(QueueKey { tenant_id: "tenant".to_owned(), task_queue: "default".to_owned() })
            .or_default()
            .push_back(queued.clone());
        push_delayed_retry(
            &mut inner,
            DelayedRetryTask { task: queued.clone(), due_at: now + ChronoDuration::seconds(30) },
        );
        let state_dir =
            std::env::temp_dir().join(format!("unified-runtime-cancel-test-{}", Uuid::now_v7()));
        let app_state = test_app_state(store.clone(), inner, state_dir.clone()).await;
        let cancel_requested = test_event(
            &WorkflowIdentity::new(
                "tenant",
                "definition",
                1,
                "artifact",
                "instance-cancel",
                "cancel",
                "unified-runtime-test",
            ),
            WorkflowEvent::WorkflowCancellationRequested { reason: "stop".to_owned() },
            now,
        );
        let item =
            mailbox_record(WorkflowMailboxKind::CancelRequest, cancel_requested, None, None, None);

        let outcome = dispatch_cancel_mailbox_item_unified(&app_state, &item).await?;
        let MailboxDrainOutcome::Processed { general, schedules, .. } = outcome else {
            anyhow::bail!("expected processed cancel outcome");
        };
        apply_db_actions(&app_state, general, schedules).await?;

        let inner = app_state.inner.lock().expect("unified runtime lock poisoned");
        let runtime = inner.instances.get(&run_key).context("runtime should exist")?;
        assert_eq!(runtime.instance.status, WorkflowStatus::Cancelled);
        assert!(runtime.active_activities.is_empty());
        assert!(inner.ready.values().all(VecDeque::is_empty));
        assert_eq!(delayed_retry_count(&inner.delayed_retries), 0);
        drop(inner);

        let stored = store
            .get_instance("tenant", "instance-cancel")
            .await?
            .context("stored instance should exist")?;
        assert_eq!(stored.status, WorkflowStatus::Cancelled);

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn cancel_mailbox_item_unwinds_active_activity_instead_of_force_cancelling_workflow()
    -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let artifact = test_artifact("default");
        store.put_artifact("tenant", &artifact).await?;

        let state_dir =
            std::env::temp_dir().join(format!("unified-runtime-cancel-unwind-{}", Uuid::now_v7()));
        let app_state =
            test_app_state(store.clone(), RuntimeInner::default(), state_dir.clone()).await;
        let identity = WorkflowIdentity::new(
            "tenant",
            &artifact.definition_id,
            artifact.definition_version,
            artifact.artifact_hash.clone(),
            "instance-cancel-unwind",
            "run-cancel-unwind",
            "unified-runtime-test",
        );
        let trigger = test_event(
            &identity,
            WorkflowEvent::WorkflowTriggered { input: json!({"items": [json!({"value": "x"})]}) },
            Utc::now(),
        );
        handle_trigger_event(&app_state, trigger).await?;

        let cancel_requested = test_event(
            &identity,
            WorkflowEvent::WorkflowCancellationRequested { reason: "stop".to_owned() },
            Utc::now(),
        );
        let item =
            mailbox_record(WorkflowMailboxKind::CancelRequest, cancel_requested, None, None, None);

        let outcome = dispatch_cancel_mailbox_item_unified(&app_state, &item).await?;
        let MailboxDrainOutcome::Processed { general, schedules, .. } = outcome else {
            anyhow::bail!("expected processed cancel unwind outcome");
        };
        apply_db_actions(&app_state, general, schedules).await?;

        let stored = store
            .get_instance("tenant", "instance-cancel-unwind")
            .await?
            .context("stored instance should exist")?;
        assert_ne!(stored.status, WorkflowStatus::Cancelled);
        assert_eq!(stored.status, WorkflowStatus::Completed);

        let activities = store
            .list_activities_for_run("tenant", "instance-cancel-unwind", "run-cancel-unwind")
            .await?;
        assert_eq!(activities.len(), 1);
        assert_eq!(activities[0].status, fabrik_store::WorkflowActivityStatus::Cancelled);

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn activity_cancellation_request_immediately_cancels_scheduled_task() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let now = Utc::now();
        let run_key = run_key_for("activity-cancel");
        let queued = queued_activity_for_run(&run_key.run_id, "activity-1", 1, now);
        let mut instance =
            workflow_instance_for_run(&run_key.run_id, WorkflowStatus::Running, 1, now);
        instance.current_state = None;
        store.upsert_instance(&instance).await?;
        store.upsert_activities_scheduled_batch(&[activity_schedule_update(&queued)]).await?;

        let mut inner = RuntimeInner::default();
        inner.instances.insert(
            run_key.clone(),
            RuntimeWorkflowState {
                artifact: test_artifact("default"),
                instance,
                active_activities: BTreeMap::from([(
                    "activity-1".to_owned(),
                    ActiveActivityMeta {
                        attempt: 1,
                        task_queue: "default".to_owned(),
                        activity_type: "benchmark.echo".to_owned(),
                        wait_state: "join".to_owned(),
                        omit_success_output: false,
                    },
                )]),
            },
        );
        inner
            .ready
            .entry(QueueKey { tenant_id: "tenant".to_owned(), task_queue: "default".to_owned() })
            .or_default()
            .push_back(queued);

        let state_dir = std::env::temp_dir()
            .join(format!("unified-runtime-activity-cancel-{}", Uuid::now_v7()));
        let app_state = test_app_state(store.clone(), inner, state_dir.clone()).await;
        let identity = WorkflowIdentity::new(
            "tenant",
            "definition",
            1,
            "artifact",
            &run_key.instance_id,
            &run_key.run_id,
            "unified-runtime-test",
        );
        let cancel_event = test_event(
            &identity,
            WorkflowEvent::ActivityTaskCancellationRequested {
                activity_id: "activity-1".to_owned(),
                attempt: 1,
                reason: "stop".to_owned(),
                metadata: Some(json!({"source": "test"})),
            },
            now,
        );

        handle_activity_cancellation_requested_event(&app_state, cancel_event).await?;

        let inner = app_state.inner.lock().expect("unified runtime lock poisoned");
        let runtime = inner.instances.get(&run_key).context("runtime should exist")?;
        assert!(runtime.active_activities.is_empty());
        assert!(inner.ready.values().all(VecDeque::is_empty));
        drop(inner);

        let activity = store
            .get_activity_attempt("tenant", &run_key.instance_id, &run_key.run_id, "activity-1", 1)
            .await?
            .context("cancelled activity should exist")?;
        assert_eq!(activity.status, fabrik_store::WorkflowActivityStatus::Cancelled);
        assert_eq!(activity.cancellation_reason.as_deref(), Some("stop"));

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn heartbeat_reports_cancellation_for_leased_task() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let now = Utc::now();
        let queued = queued_activity("activity-1", 1, now);
        store.upsert_activities_scheduled_batch(&[activity_schedule_update(&queued)]).await?;
        store
            .mark_activity_started(
                "tenant",
                "instance",
                "run",
                "activity-1",
                1,
                "worker",
                "build",
                now + ChronoDuration::seconds(30),
                Uuid::now_v7(),
                "test-start",
                now,
            )
            .await?;
        store
            .request_activity_cancellation(
                "tenant",
                "instance",
                "run",
                "activity-1",
                1,
                "stop",
                None,
                Uuid::now_v7(),
                "test-cancel-request",
                now,
            )
            .await?;

        let mut inner = RuntimeInner { owner_epoch: 7, ..RuntimeInner::default() };
        let mut leased_task = queued_activity("activity-1", 1, now);
        leased_task.lease_epoch = 3;
        leased_task.cancellation_requested = true;
        inner.leased.insert(
            attempt_key_from_task(&leased_task),
            LeasedActivity {
                task: leased_task,
                worker_id: "worker".to_owned(),
                worker_build_id: "build".to_owned(),
                lease_expires_at: now + ChronoDuration::seconds(10),
                owner_epoch: 7,
            },
        );

        let state_dir = std::env::temp_dir()
            .join(format!("unified-runtime-heartbeat-cancel-{}", Uuid::now_v7()));
        let app_state = test_app_state(store.clone(), inner, state_dir.clone()).await;
        apply_ownership_record(&app_state, 1, "test-owner", 7, now + ChronoDuration::seconds(30));
        let worker = WorkerApi { state: app_state.clone() };
        let response = worker
            .record_activity_heartbeat(Request::new(RecordActivityHeartbeatRequest {
                tenant_id: "tenant".to_owned(),
                instance_id: "instance".to_owned(),
                run_id: "run".to_owned(),
                activity_id: "activity-1".to_owned(),
                attempt: 1,
                worker_id: "worker".to_owned(),
                worker_build_id: "build".to_owned(),
                details_json: String::new(),
                lease_epoch: 3,
                owner_epoch: 7,
            }))
            .await?
            .into_inner();

        assert!(response.cancellation_requested);

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn trigger_start_child_completes_child_and_reflects_to_parent() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let parent_artifact = child_parent_artifact();
        let child_artifact = child_leaf_artifact();
        store.put_artifact("tenant", &parent_artifact).await?;
        store.put_artifact("tenant", &child_artifact).await?;

        let state_dir =
            std::env::temp_dir().join(format!("unified-runtime-child-test-{}", Uuid::now_v7()));
        let app_state =
            test_app_state(store.clone(), RuntimeInner::default(), state_dir.clone()).await;
        let identity = WorkflowIdentity::new(
            "tenant",
            &parent_artifact.definition_id,
            parent_artifact.definition_version,
            parent_artifact.artifact_hash.clone(),
            "parent-instance",
            "parent-run",
            "unified-runtime-test",
        );
        let trigger = test_event(
            &identity,
            WorkflowEvent::WorkflowTriggered { input: json!({"seed": true}) },
            Utc::now(),
        );

        handle_trigger_event(&app_state, trigger).await?;

        let parent = store
            .get_instance("tenant", "parent-instance")
            .await?
            .context("parent instance should exist")?;
        assert_eq!(parent.status, WorkflowStatus::Completed);
        assert_eq!(parent.output, Some(Value::String("hello-child".to_owned())));

        let child = store
            .get_instance("tenant", "child-instance")
            .await?
            .context("child instance should exist")?;
        assert_eq!(child.status, WorkflowStatus::Completed);
        assert_eq!(child.output, Some(Value::String("hello-child".to_owned())));

        let children =
            store.list_children_for_run("tenant", "parent-instance", "parent-run").await?;
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].child_workflow_id, "child-instance");
        assert_eq!(children[0].status, "completed");
        assert_eq!(children[0].output, Some(Value::String("hello-child".to_owned())));
        assert!(children[0].error.is_none());

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn trigger_materializes_throughput_bulk_batch() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let artifact = throughput_artifact("default");
        store.put_artifact("tenant", &artifact).await?;

        let state_dir = std::env::temp_dir()
            .join(format!("unified-runtime-throughput-batch-{}", Uuid::now_v7()));
        let app_state =
            test_app_state(store.clone(), RuntimeInner::default(), state_dir.clone()).await;
        let identity = WorkflowIdentity::new(
            "tenant",
            &artifact.definition_id,
            artifact.definition_version,
            artifact.artifact_hash.clone(),
            "instance-throughput-batch",
            "run-throughput-batch",
            "unified-runtime-test",
        );
        let trigger = test_event(
            &identity,
            WorkflowEvent::WorkflowTriggered { input: json!({"items": [json!({"value": "x"})]}) },
            Utc::now(),
        );

        handle_trigger_event(&app_state, trigger).await?;

        let stored = store
            .get_instance("tenant", "instance-throughput-batch")
            .await?
            .context("stored throughput instance should exist")?;
        assert_eq!(stored.status, WorkflowStatus::Running);
        assert_eq!(stored.current_state.as_deref(), Some("join"));

        let batches = store
            .list_bulk_batches_for_run_page(
                "tenant",
                "instance-throughput-batch",
                "run-throughput-batch",
                10,
                0,
            )
            .await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].status.as_str(), "scheduled");
        assert_eq!(batches[0].throughput_backend, ThroughputBackend::PgV1.as_str());

        let inner = app_state.inner.lock().expect("unified runtime lock poisoned");
        assert!(inner.instances.contains_key(&RunKey {
            tenant_id: "tenant".to_owned(),
            instance_id: "instance-throughput-batch".to_owned(),
            run_id: "run-throughput-batch".to_owned(),
        }));

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn native_start_throughput_run_command_stores_db_backed_input_reference() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = std::env::temp_dir()
            .join(format!("unified-runtime-native-start-command-{}", Uuid::now_v7()));
        let mut app_state = test_app_state(store, RuntimeInner::default(), state_dir.clone()).await;
        app_state.throughput_runtime.native_stream_v2_engine_enabled = true;

        let instance = WorkflowInstanceState {
            tenant_id: "tenant".to_owned(),
            instance_id: "instance-native-start".to_owned(),
            run_id: "run-native-start".to_owned(),
            definition_id: "demo".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-a".to_owned()),
            workflow_task_queue: "default".to_owned(),
            sticky_workflow_build_id: None,
            sticky_workflow_poller_id: None,
            current_state: Some("join".to_owned()),
            context: None,
            artifact_execution: None,
            status: WorkflowStatus::Running,
            input: None,
            persisted_input_handle: None,
            memo: None,
            search_attributes: None,
            output: None,
            event_count: 1,
            last_event_id: Uuid::now_v7(),
            last_event_type: "WorkflowTriggered".to_owned(),
            updated_at: Utc::now(),
        };
        let command = start_throughput_run_command(
            &app_state,
            &instance,
            "batch-a",
            "benchmark.echo",
            "bulk",
            2,
            &serde_json::to_value(PayloadHandle::inline_batch_result("batch-a"))?,
            Some("join".to_owned()),
            16,
            3,
            1000,
            2,
            Some("parallel".to_owned()),
            Some("count".to_owned()),
            ThroughputBackend::StreamV2.as_str(),
            ThroughputBackend::StreamV2.default_version(),
            &BulkAdmissionDecision {
                selected_backend: ThroughputBackend::StreamV2.as_str().to_owned(),
                selected_backend_version: ThroughputBackend::StreamV2.default_version().to_owned(),
                routing_reason: "stream_v2_selected".to_owned(),
                admission_policy_version: ADMISSION_POLICY_VERSION.to_owned(),
            },
            native_stream_v2_run_input_handle(
                "tenant",
                "instance-native-start",
                "run-native-start",
                "batch-a",
            ),
            Uuid::now_v7(),
            Utc::now(),
        )
        .await?;

        let ThroughputCommand::StartThroughputRun(start) = &command.payload else {
            panic!("expected native start throughput command");
        };
        let PayloadHandle::Inline { key } = &start.input_handle else {
            panic!("expected db-backed native input handle reference");
        };
        assert!(key.starts_with("throughput-run-input:"));

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn trigger_persists_bulk_batch_admission_reason() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let artifact =
            throughput_artifact_with_options("default", None, Some("collect_results"), 16);
        store.put_artifact("tenant", &artifact).await?;

        let state_dir = std::env::temp_dir()
            .join(format!("unified-runtime-throughput-routing-{}", Uuid::now_v7()));
        let app_state =
            test_app_state(store.clone(), RuntimeInner::default(), state_dir.clone()).await;
        let identity = WorkflowIdentity::new(
            "tenant",
            &artifact.definition_id,
            artifact.definition_version,
            artifact.artifact_hash.clone(),
            "instance-throughput-routing",
            "run-throughput-routing",
            "unified-runtime-test",
        );
        let trigger = test_event(
            &identity,
            WorkflowEvent::WorkflowTriggered {
                input: json!({"items": [json!({"value": "x"}), json!({"value": "y"})]}),
            },
            Utc::now(),
        );

        handle_trigger_event(&app_state, trigger).await?;

        let batches = store
            .list_bulk_batches_for_run_page(
                "tenant",
                "instance-throughput-routing",
                "run-throughput-routing",
                10,
                0,
            )
            .await?;
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.throughput_backend, ThroughputBackend::PgV1.as_str());
        assert_eq!(batch.routing_reason, "materialized_results");
        assert_eq!(batch.admission_policy_version, ADMISSION_POLICY_VERSION);

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn bulk_terminal_event_completes_throughput_run() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let artifact = throughput_artifact("default");
        store.put_artifact("tenant", &artifact).await?;

        let state_dir = std::env::temp_dir()
            .join(format!("unified-runtime-throughput-complete-{}", Uuid::now_v7()));
        let app_state =
            test_app_state(store.clone(), RuntimeInner::default(), state_dir.clone()).await;
        let identity = WorkflowIdentity::new(
            "tenant",
            &artifact.definition_id,
            artifact.definition_version,
            artifact.artifact_hash.clone(),
            "instance-throughput-complete",
            "run-throughput-complete",
            "unified-runtime-test",
        );
        let trigger = test_event(
            &identity,
            WorkflowEvent::WorkflowTriggered { input: json!({"items": [json!({"value": "x"})]}) },
            Utc::now(),
        );
        handle_trigger_event(&app_state, trigger).await?;

        let batches = store
            .list_bulk_batches_for_run_page(
                "tenant",
                "instance-throughput-complete",
                "run-throughput-complete",
                10,
                0,
            )
            .await?;
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        let completed = test_event(
            &identity,
            WorkflowEvent::BulkActivityBatchCompleted {
                batch_id: batch.batch_id.clone(),
                total_items: batch.total_items,
                succeeded_items: batch.total_items,
                failed_items: 0,
                cancelled_items: 0,
                chunk_count: batch.chunk_count,
                reducer_output: None,
            },
            Utc::now(),
        );
        handle_bulk_batch_terminal_event(&app_state, completed).await?;

        let stored = store
            .get_instance("tenant", "instance-throughput-complete")
            .await?
            .context("stored throughput instance should exist")?;
        assert_eq!(stored.status, WorkflowStatus::Completed);
        let output = stored.output.context("throughput output should exist")?;
        assert_eq!(output.get("batchId"), Some(&Value::String(batch.batch_id.clone())));
        assert_eq!(output.get("terminalStatus"), Some(&Value::String("completed".to_owned())));
        assert_eq!(output.get("succeededItems"), Some(&json!(batch.total_items)));

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn update_mailbox_item_can_start_child_and_complete_update() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let artifact = update_child_artifact();
        let child_artifact = child_leaf_artifact();
        store.put_artifact("tenant", &artifact).await?;
        store.put_artifact("tenant", &child_artifact).await?;

        let now = Utc::now();
        let run_key = RunKey {
            tenant_id: "tenant".to_owned(),
            instance_id: "instance-update-child".to_owned(),
            run_id: "update-child-run".to_owned(),
        };
        let instance = WorkflowInstanceState {
            tenant_id: run_key.tenant_id.clone(),
            instance_id: run_key.instance_id.clone(),
            run_id: run_key.run_id.clone(),
            definition_id: artifact.definition_id.clone(),
            definition_version: Some(artifact.definition_version),
            artifact_hash: Some(artifact.artifact_hash.clone()),
            workflow_task_queue: "default".to_owned(),
            sticky_workflow_build_id: None,
            sticky_workflow_poller_id: None,
            current_state: Some("wait_signal".to_owned()),
            context: None,
            artifact_execution: Some(ArtifactExecutionState::default()),
            status: WorkflowStatus::Running,
            input: Some(json!({"seed": true})),
            persisted_input_handle: None,
            memo: None,
            search_attributes: None,
            output: None,
            event_count: 1,
            last_event_id: Uuid::now_v7(),
            last_event_type: "WorkflowTriggered".to_owned(),
            updated_at: now,
        };
        store.upsert_instance(&instance).await?;
        store
            .queue_update(
                &run_key.tenant_id,
                &run_key.instance_id,
                &run_key.run_id,
                "update-1",
                "approve",
                None,
                &json!({"value": "hello-child"}),
                Uuid::now_v7(),
                now,
            )
            .await?;

        let mut inner = RuntimeInner::default();
        inner.instances.insert(
            run_key.clone(),
            RuntimeWorkflowState {
                artifact: artifact.clone(),
                instance: instance.clone(),
                active_activities: BTreeMap::new(),
            },
        );
        let state_dir = std::env::temp_dir()
            .join(format!("unified-runtime-update-child-test-{}", Uuid::now_v7()));
        let app_state = test_app_state(store.clone(), inner, state_dir.clone()).await;
        let requested = test_event(
            &WorkflowIdentity::new(
                &run_key.tenant_id,
                &artifact.definition_id,
                artifact.definition_version,
                artifact.artifact_hash.clone(),
                &run_key.instance_id,
                &run_key.run_id,
                "unified-runtime-test",
            ),
            WorkflowEvent::WorkflowUpdateRequested {
                update_id: "update-1".to_owned(),
                update_name: "approve".to_owned(),
                payload: json!({"value": "hello-child"}),
            },
            now,
        );
        let item = mailbox_record(
            WorkflowMailboxKind::Update,
            requested,
            Some("update-1"),
            Some("approve"),
            Some(json!({"value": "hello-child"})),
        );

        let outcome = dispatch_update_mailbox_item_unified(&app_state, &item).await?;
        let MailboxDrainOutcome::Processed { general, schedules, post_plan, .. } = outcome else {
            anyhow::bail!("expected processed update outcome");
        };
        apply_db_actions(&app_state, general, schedules).await?;
        if let Some(post_plan) = post_plan {
            apply_post_plan_effects(&app_state, post_plan).await?;
        }

        let update = store
            .get_update(&run_key.tenant_id, &run_key.instance_id, &run_key.run_id, "update-1")
            .await?
            .context("update record should exist")?;
        assert_eq!(update.status, WorkflowUpdateStatus::Completed);
        assert_eq!(update.output, Some(Value::String("hello-child".to_owned())));

        let child = store
            .get_instance(&run_key.tenant_id, "child-instance-update")
            .await?
            .context("child instance should exist")?;
        assert_eq!(child.status, WorkflowStatus::Completed);
        assert_eq!(child.output, Some(Value::String("hello-child".to_owned())));

        let parent = store
            .get_instance(&run_key.tenant_id, &run_key.instance_id)
            .await?
            .context("parent instance should exist")?;
        assert_eq!(parent.status, WorkflowStatus::Running);

        let children = store
            .list_children_for_run(&run_key.tenant_id, &run_key.instance_id, &run_key.run_id)
            .await?;
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].status, "completed");
        assert_eq!(children[0].output, Some(Value::String("hello-child".to_owned())));

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn trigger_signal_child_completes_child_and_reflects_to_parent() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let parent_artifact = child_signal_parent_artifact();
        let child_artifact = child_wait_for_signal_artifact();
        store.put_artifact("tenant", &parent_artifact).await?;
        store.put_artifact("tenant", &child_artifact).await?;

        let state_dir = std::env::temp_dir()
            .join(format!("unified-runtime-child-signal-test-{}", Uuid::now_v7()));
        let app_state =
            test_app_state(store.clone(), RuntimeInner::default(), state_dir.clone()).await;
        let identity = WorkflowIdentity::new(
            "tenant",
            &parent_artifact.definition_id,
            parent_artifact.definition_version,
            parent_artifact.artifact_hash.clone(),
            "parent-instance-signal",
            "parent-run-signal",
            "unified-runtime-test",
        );
        let trigger = test_event(
            &identity,
            WorkflowEvent::WorkflowTriggered { input: json!({"seed": true}) },
            Utc::now(),
        );

        handle_trigger_event(&app_state, trigger).await?;

        let parent = store
            .get_instance("tenant", "parent-instance-signal")
            .await?
            .context("parent instance should exist")?;
        assert_eq!(parent.status, WorkflowStatus::Completed);
        assert_eq!(parent.output, Some(Value::String("approved".to_owned())));

        let child = store
            .get_instance("tenant", "child-instance-signal")
            .await?
            .context("child instance should exist")?;
        assert_eq!(child.status, WorkflowStatus::Completed);
        assert_eq!(child.output, Some(Value::String("approved".to_owned())));

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn trigger_cancel_child_reflects_cancellation_to_parent_error_transition() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let parent_artifact = child_cancel_parent_artifact();
        let child_artifact = child_wait_for_signal_artifact();
        store.put_artifact("tenant", &parent_artifact).await?;
        store.put_artifact("tenant", &child_artifact).await?;

        let state_dir = std::env::temp_dir()
            .join(format!("unified-runtime-child-cancel-test-{}", Uuid::now_v7()));
        let app_state =
            test_app_state(store.clone(), RuntimeInner::default(), state_dir.clone()).await;
        let identity = WorkflowIdentity::new(
            "tenant",
            &parent_artifact.definition_id,
            parent_artifact.definition_version,
            parent_artifact.artifact_hash.clone(),
            "parent-instance-cancel",
            "parent-run-cancel",
            "unified-runtime-test",
        );
        let trigger = test_event(
            &identity,
            WorkflowEvent::WorkflowTriggered { input: json!({"seed": true}) },
            Utc::now(),
        );

        handle_trigger_event(&app_state, trigger).await?;

        let parent = store
            .get_instance("tenant", "parent-instance-cancel")
            .await?
            .context("parent instance should exist")?;
        assert_eq!(parent.status, WorkflowStatus::Completed);
        assert_eq!(parent.output, Some(Value::String("stop".to_owned())));

        let child = store
            .get_instance("tenant", "child-instance-cancel")
            .await?
            .context("child instance should exist")?;
        assert_eq!(child.status, WorkflowStatus::Cancelled);

        fs::remove_dir_all(state_dir).ok();
        Ok(())
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
        assert_eq!(delayed_retry_count(&inner.delayed_retries), 1);
        assert_eq!(ready_tasks.len(), 3);
        assert!(ready_tasks.iter().any(|task| task.activity_id == ready_task.activity_id));
        assert!(ready_tasks.iter().any(|task| task.activity_id == leased_task.activity_id));
        assert!(ready_tasks.iter().any(|task| task.activity_id == due_retry_task.activity_id));
        let future_retry = inner
            .delayed_retries
            .values()
            .next()
            .and_then(|retries| retries.first())
            .expect("future retry task");
        assert_eq!(future_retry.task.activity_id, future_retry_task.activity_id);
    }

    #[test]
    fn capture_persisted_state_skips_idle_bulk_wait_runs() {
        let artifact = throughput_artifact_with_options(
            "default",
            Some(ThroughputBackend::StreamV2.as_str()),
            Some("all_settled"),
            16,
        );
        let identity = WorkflowIdentity::new(
            "tenant".to_owned(),
            artifact.definition_id.clone(),
            artifact.definition_version,
            artifact.artifact_hash.clone(),
            "instance-bulk-persist".to_owned(),
            "run-bulk-persist".to_owned(),
            "unified-runtime-test",
        );
        let occurred_at = Utc::now();
        let input = json!({"items": [json!({"value": "x"}), json!({"value": "y"})]});
        let trigger = test_event(
            &identity,
            WorkflowEvent::WorkflowTriggered { input: input.clone() },
            occurred_at,
        );
        let plan = artifact
            .execute_trigger_with_turn(
                &input,
                ExecutionTurnContext { event_id: trigger.event_id, occurred_at },
            )
            .expect("bulk trigger plan");
        let mut instance =
            WorkflowInstanceState::try_from(&trigger).expect("workflow instance from trigger");
        apply_compiled_plan(&mut instance, &plan);
        let run_key = RunKey {
            tenant_id: identity.tenant_id.clone(),
            instance_id: identity.instance_id.clone(),
            run_id: identity.run_id.clone(),
        };
        let mut inner = RuntimeInner::default();
        inner.instances.insert(
            run_key,
            RuntimeWorkflowState { artifact, instance, active_activities: BTreeMap::new() },
        );

        let persisted = capture_persisted_state(&mut inner);

        assert!(persisted.instances.is_empty());
        assert!(persisted.ready.is_empty());
        assert!(persisted.leased.is_empty());
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
        push_delayed_retry(
            &mut local,
            DelayedRetryTask {
                task: local_retry_task.clone(),
                due_at: now + ChronoDuration::seconds(30),
            },
        );

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
        assert_eq!(delayed_retry_count(&merged.delayed_retries), 0);
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

    #[tokio::test]
    async fn trigger_continue_as_new_rolls_run_lineage() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let artifact = continue_as_new_artifact();
        store.put_artifact("tenant", &artifact).await?;
        let state_dir =
            std::env::temp_dir().join(format!("unified-runtime-continue-{}", Uuid::now_v7()));
        let app_state = test_app_state(store.clone(), RuntimeInner::default(), state_dir).await;

        let identity = WorkflowIdentity::new(
            "tenant",
            &artifact.definition_id,
            artifact.definition_version,
            artifact.artifact_hash.clone(),
            "instance-continue",
            "run-continue-1",
            "unified-runtime-test",
        );
        let trigger = test_event(
            &identity,
            WorkflowEvent::WorkflowTriggered { input: json!({ "remaining": 1 }) },
            Utc::now(),
        );

        handle_trigger_event(&app_state, trigger).await?;

        let current =
            store.get_instance("tenant", "instance-continue").await?.context("current instance")?;
        assert_ne!(current.run_id, "run-continue-1");
        assert_eq!(current.status, WorkflowStatus::Completed);

        let previous_run = store
            .get_run_record("tenant", "instance-continue", "run-continue-1")
            .await?
            .context("previous run record")?;
        assert_eq!(previous_run.next_run_id.as_deref(), Some(current.run_id.as_str()));
        assert_eq!(previous_run.continue_reason.as_deref(), Some("continued_as_new"));

        let new_run = store
            .get_run_record("tenant", "instance-continue", &current.run_id)
            .await?
            .context("new run record")?;
        assert_eq!(new_run.previous_run_id.as_deref(), Some("run-continue-1"));
        Ok(())
    }

    #[tokio::test]
    async fn trigger_wait_timer_persists_and_firing_timer_completes_run() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let artifact = timer_artifact();
        store.put_artifact("tenant", &artifact).await?;
        let state_dir =
            std::env::temp_dir().join(format!("unified-runtime-timer-{}", Uuid::now_v7()));
        let app_state = test_app_state(store.clone(), RuntimeInner::default(), state_dir).await;
        {
            let mut ownership =
                app_state.ownership.lock().expect("unified ownership lock poisoned");
            ownership.partition_id = DEFAULT_OWNERSHIP_PARTITION_ID;
        }

        let identity = WorkflowIdentity::new(
            "tenant",
            &artifact.definition_id,
            artifact.definition_version,
            artifact.artifact_hash.clone(),
            "instance-timer",
            "run-timer-1",
            "unified-runtime-test",
        );
        let trigger_time = Utc::now();
        let trigger = test_event(
            &identity,
            WorkflowEvent::WorkflowTriggered { input: json!({}) },
            trigger_time,
        );
        handle_trigger_event(&app_state, trigger).await?;

        let workflow_partition = workflow_partition_id(&app_state, "tenant", "instance-timer");
        let due = store
            .claim_due_timers(workflow_partition, trigger_time + ChronoDuration::seconds(2), 10, 1)
            .await?;
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].timer_id, "wait");
        let misrouted = store
            .claim_due_timers(
                DEFAULT_OWNERSHIP_PARTITION_ID,
                trigger_time + ChronoDuration::seconds(2),
                10,
                1,
            )
            .await?;
        assert!(misrouted.is_empty());

        let timer_event = test_event(
            &identity,
            WorkflowEvent::TimerFired { timer_id: "wait".to_owned() },
            trigger_time + ChronoDuration::seconds(2),
        );
        handle_timer_fired_event(&app_state, timer_event).await?;

        let current =
            store.get_instance("tenant", "instance-timer").await?.context("timer instance")?;
        assert_eq!(current.status, WorkflowStatus::Completed);
        let remaining = store
            .claim_due_timers(workflow_partition, trigger_time + ChronoDuration::seconds(5), 10, 2)
            .await?;
        assert!(remaining.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn brokered_signal_event_enqueues_mailbox_and_completes_hot_run() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let artifact = child_wait_for_signal_artifact();
        store.put_artifact("tenant", &artifact).await?;
        let state_dir = std::env::temp_dir()
            .join(format!("unified-runtime-brokered-signal-{}", Uuid::now_v7()));
        let app_state = test_app_state(store.clone(), RuntimeInner::default(), state_dir).await;

        let identity = WorkflowIdentity::new(
            "tenant",
            &artifact.definition_id,
            artifact.definition_version,
            artifact.artifact_hash.clone(),
            "instance-signal",
            "run-signal-1",
            "unified-runtime-test",
        );
        let trigger = test_event(
            &identity,
            WorkflowEvent::WorkflowTriggered { input: Value::Null },
            Utc::now(),
        );
        handle_trigger_event(&app_state, trigger).await?;

        let signal_event = test_event(
            &identity,
            WorkflowEvent::SignalQueued {
                signal_id: "sig-1".to_owned(),
                signal_type: "approve".to_owned(),
                payload: Value::String("approved".to_owned()),
            },
            Utc::now(),
        );
        store
            .queue_signal(
                "tenant",
                "instance-signal",
                "run-signal-1",
                "sig-1",
                "approve",
                None,
                &Value::String("approved".to_owned()),
                signal_event.event_id,
                signal_event.occurred_at,
            )
            .await?;

        handle_mailbox_queue_event(&app_state, signal_event).await?;

        let current =
            store.get_instance("tenant", "instance-signal").await?.context("signal instance")?;
        assert_eq!(current.status, WorkflowStatus::Completed);
        assert_eq!(current.output, Some(Value::String("approved".to_owned())));
        Ok(())
    }

    #[tokio::test]
    async fn poll_activity_tasks_records_queue_poller_for_visibility() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir =
            std::env::temp_dir().join(format!("unified-runtime-poller-{}", Uuid::now_v7()));
        let app_state =
            test_app_state(store.clone(), RuntimeInner::default(), state_dir.clone()).await;
        apply_ownership_record(
            &app_state,
            1,
            "test-owner",
            3,
            Utc::now() + ChronoDuration::seconds(30),
        );
        let worker = WorkerApi { state: app_state.clone() };

        let response = worker
            .poll_activity_tasks(Request::new(PollActivityTasksRequest {
                tenant_id: "tenant".to_owned(),
                task_queue: "payments".to_owned(),
                worker_id: "worker-a".to_owned(),
                worker_build_id: "build-a".to_owned(),
                poll_timeout_ms: 10,
                max_tasks: 1,
                supports_cbor: false,
            }))
            .await?
            .into_inner();
        assert!(response.tasks.is_empty());

        let inspection = store
            .inspect_task_queue("tenant", TaskQueueKind::Activity, "payments", Utc::now())
            .await?;
        assert_eq!(inspection.pollers.len(), 1);
        assert_eq!(inspection.pollers[0].poller_id, "worker-a");
        assert_eq!(inspection.pollers[0].build_id, "build-a");

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn activity_completion_after_signal_advances_single_step_workflow() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let artifact = signal_then_step_artifact();
        store.put_artifact("tenant", &artifact).await?;
        let state_dir =
            std::env::temp_dir().join(format!("unified-runtime-signal-step-{}", Uuid::now_v7()));
        let app_state =
            test_app_state(store.clone(), RuntimeInner::default(), state_dir.clone()).await;
        apply_ownership_record(
            &app_state,
            1,
            "test-owner",
            3,
            Utc::now() + ChronoDuration::seconds(30),
        );
        let worker = WorkerApi { state: app_state.clone() };

        let identity = WorkflowIdentity::new(
            "tenant",
            &artifact.definition_id,
            artifact.definition_version,
            artifact.artifact_hash.clone(),
            "instance-signal-step",
            "run-signal-step-1",
            "unified-runtime-test",
        );
        let trigger = test_event(
            &identity,
            WorkflowEvent::WorkflowTriggered { input: Value::Null },
            Utc::now(),
        );
        handle_trigger_event(&app_state, trigger).await?;

        let signal_event = test_event(
            &identity,
            WorkflowEvent::SignalQueued {
                signal_id: "sig-approve".to_owned(),
                signal_type: "approve".to_owned(),
                payload: Value::String("fiona".to_owned()),
            },
            Utc::now(),
        );
        store
            .queue_signal(
                "tenant",
                "instance-signal-step",
                "run-signal-step-1",
                "sig-approve",
                "approve",
                None,
                &Value::String("fiona".to_owned()),
                signal_event.event_id,
                signal_event.occurred_at,
            )
            .await?;
        handle_mailbox_queue_event(&app_state, signal_event).await?;

        let task = worker
            .poll_activity_tasks(Request::new(PollActivityTasksRequest {
                tenant_id: "tenant".to_owned(),
                task_queue: "orders".to_owned(),
                worker_id: "worker-a".to_owned(),
                worker_build_id: "build-a".to_owned(),
                poll_timeout_ms: 10,
                max_tasks: 1,
                supports_cbor: false,
            }))
            .await?
            .into_inner()
            .tasks
            .into_iter()
            .next()
            .context("activity task should be available after signal")?;

        worker
            .complete_activity_task(Request::new(
                fabrik_worker_protocol::activity_worker::CompleteActivityTaskRequest {
                    tenant_id: task.tenant_id.clone(),
                    instance_id: task.instance_id.clone(),
                    run_id: task.run_id.clone(),
                    activity_id: task.activity_id.clone(),
                    attempt: task.attempt,
                    worker_id: "worker-a".to_owned(),
                    worker_build_id: "build-a".to_owned(),
                    lease_epoch: task.lease_epoch,
                    owner_epoch: task.owner_epoch,
                    output_json: "\"hello fiona\"".to_owned(),
                },
            ))
            .await?;

        let current = store
            .get_instance("tenant", "instance-signal-step")
            .await?
            .context("completed instance")?;
        assert_eq!(current.status, WorkflowStatus::Completed);
        assert_eq!(current.output, Some(Value::String("hello fiona".to_owned())));
        assert_eq!(current.last_event_type, "ActivityTaskCompleted");

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn out_of_order_post_plan_persistence_does_not_regress_fanout_completion() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let artifact = fanout_artifact_with_reducer(Some("collect_results"));
        store.put_artifact("tenant", &artifact).await?;

        let state_dir =
            std::env::temp_dir().join(format!("unified-runtime-fanout-race-{}", Uuid::now_v7()));
        let app_state =
            test_app_state(store.clone(), RuntimeInner::default(), state_dir.clone()).await;
        apply_ownership_record(
            &app_state,
            1,
            "test-owner",
            5,
            Utc::now() + ChronoDuration::seconds(30),
        );

        let payload = "x".repeat(1024);
        let items =
            (0..4).map(|index| json!({ "index": index, "payload": payload })).collect::<Vec<_>>();
        let identity = WorkflowIdentity::new(
            "tenant",
            &artifact.definition_id,
            artifact.definition_version,
            artifact.artifact_hash.clone(),
            "instance-fanout-race",
            "run-fanout-race-1",
            "unified-runtime-test",
        );
        let trigger = test_event(
            &identity,
            WorkflowEvent::WorkflowTriggered { input: json!({ "items": items }) },
            Utc::now(),
        );
        handle_trigger_event(&app_state, trigger).await?;

        let leased = {
            let mut inner = app_state.inner.lock().expect("unified runtime lock poisoned");
            lease_ready_tasks(
                &mut inner,
                &QueueKey { tenant_id: "tenant".to_owned(), task_queue: "default".to_owned() },
                "worker-a",
                "build-a",
                4,
            )
        };
        assert_eq!(leased.len(), 4);

        let completed_result =
            |task: &LeasedActivity, index: usize| -> Result<ActivityTaskResult> {
                Ok(ActivityTaskResult {
                    tenant_id: task.task.tenant_id.clone(),
                    instance_id: task.task.instance_id.clone(),
                    run_id: task.task.run_id.clone(),
                    activity_id: task.task.activity_id.clone(),
                    attempt: task.task.attempt,
                    worker_id: task.worker_id.clone(),
                    worker_build_id: task.worker_build_id.clone(),
                    lease_epoch: task.task.lease_epoch,
                    owner_epoch: task.owner_epoch,
                    result: Some(activity_task_result::Result::Completed(
                        ActivityTaskCompletedResult {
                            output_json: serde_json::to_string(&json!({
                                "index": index,
                                "payload": format!("done-{index}"),
                            }))
                            .context("serialize activity result output")?,
                            output_cbor: Vec::new(),
                        },
                    )),
                })
            };

        let first_batch_at = Utc::now();
        let first_actions = prepare_result_application(
            &app_state,
            vec![completed_result(&leased[0], 0)?, completed_result(&leased[1], 1)?],
            first_batch_at,
        )?;
        assert!(!first_actions.post_plans.is_empty());
        let first_post_plan = first_actions
            .post_plans
            .iter()
            .min_by_key(|effect| effect.instance.event_count)
            .cloned()
            .context("first batch should produce a post-plan snapshot")?;
        apply_db_actions(&app_state, first_actions.general, first_actions.schedules).await?;

        let second_batch_at = first_batch_at + ChronoDuration::milliseconds(1);
        let second_actions = prepare_result_application(
            &app_state,
            vec![completed_result(&leased[2], 2)?, completed_result(&leased[3], 3)?],
            second_batch_at,
        )?;
        let second_post_plan = second_actions
            .post_plans
            .iter()
            .find(|effect| effect.instance.status.is_terminal())
            .cloned()
            .context("second batch should produce a final post-plan snapshot")?;
        assert!(second_post_plan.instance.status.is_terminal());
        assert!(second_post_plan.instance.event_count > first_post_plan.instance.event_count);

        apply_db_actions(&app_state, second_actions.general, second_actions.schedules).await?;
        apply_post_plan_effects_with_options(&app_state, second_post_plan.clone(), true).await?;

        let stored_after_final = store
            .get_instance("tenant", "instance-fanout-race")
            .await?
            .context("final workflow instance should be stored")?;
        assert_eq!(stored_after_final.status, WorkflowStatus::Completed);
        assert_eq!(stored_after_final.event_count, second_post_plan.instance.event_count);
        assert_eq!(stored_after_final.last_event_id, second_post_plan.instance.last_event_id);
        assert_eq!(stored_after_final.output, second_post_plan.instance.output);

        apply_post_plan_effects_with_options(&app_state, first_post_plan, true).await?;

        let stored_after_stale = store
            .get_instance("tenant", "instance-fanout-race")
            .await?
            .context("stale post-plan should not remove the stored workflow instance")?;
        assert_eq!(stored_after_stale.status, WorkflowStatus::Completed);
        assert_eq!(stored_after_stale.event_count, second_post_plan.instance.event_count);
        assert_eq!(stored_after_stale.last_event_id, second_post_plan.instance.last_event_id);
        assert_eq!(stored_after_stale.output, second_post_plan.instance.output);

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn hot_signal_and_activity_completion_are_published_to_broker_history() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let publisher = redpanda.connect_publisher().await?;
        let artifact = signal_then_step_artifact();
        store.put_artifact("tenant", &artifact).await?;
        let state_dir =
            std::env::temp_dir().join(format!("unified-runtime-signal-history-{}", Uuid::now_v7()));
        let app_state = test_app_state_with_publisher(
            store.clone(),
            publisher.clone(),
            RuntimeInner::default(),
            state_dir.clone(),
        )
        .await;
        apply_ownership_record(
            &app_state,
            1,
            "test-owner",
            3,
            Utc::now() + ChronoDuration::seconds(30),
        );
        let worker = WorkerApi { state: app_state.clone() };

        let identity = WorkflowIdentity::new(
            "tenant",
            &artifact.definition_id,
            artifact.definition_version,
            artifact.artifact_hash.clone(),
            "instance-signal-history",
            "run-signal-history-1",
            "unified-runtime-test",
        );
        let trigger = test_event(
            &identity,
            WorkflowEvent::WorkflowTriggered { input: Value::Null },
            Utc::now(),
        );
        publisher.publish(&trigger, &trigger.partition_key).await?;
        handle_trigger_event(&app_state, trigger).await?;

        let signal_event = test_event(
            &identity,
            WorkflowEvent::SignalQueued {
                signal_id: "sig-approve".to_owned(),
                signal_type: "approve".to_owned(),
                payload: Value::String("fiona".to_owned()),
            },
            Utc::now(),
        );
        publisher.publish(&signal_event, &signal_event.partition_key).await?;
        store
            .queue_signal(
                "tenant",
                "instance-signal-history",
                "run-signal-history-1",
                "sig-approve",
                "approve",
                None,
                &Value::String("fiona".to_owned()),
                signal_event.event_id,
                signal_event.occurred_at,
            )
            .await?;
        handle_mailbox_queue_event(&app_state, signal_event).await?;

        let task = worker
            .poll_activity_tasks(Request::new(PollActivityTasksRequest {
                tenant_id: "tenant".to_owned(),
                task_queue: "orders".to_owned(),
                worker_id: "worker-a".to_owned(),
                worker_build_id: "build-a".to_owned(),
                poll_timeout_ms: 10,
                max_tasks: 1,
                supports_cbor: false,
            }))
            .await?
            .into_inner()
            .tasks
            .into_iter()
            .next()
            .context("activity task should be available after brokered signal")?;

        worker
            .complete_activity_task(Request::new(
                fabrik_worker_protocol::activity_worker::CompleteActivityTaskRequest {
                    tenant_id: task.tenant_id.clone(),
                    instance_id: task.instance_id.clone(),
                    run_id: task.run_id.clone(),
                    activity_id: task.activity_id.clone(),
                    attempt: task.attempt,
                    worker_id: "worker-a".to_owned(),
                    worker_build_id: "build-a".to_owned(),
                    lease_epoch: task.lease_epoch,
                    owner_epoch: task.owner_epoch,
                    output_json: "\"hello fiona\"".to_owned(),
                },
            ))
            .await?;

        let history_deadline = Instant::now() + StdDuration::from_secs(10);
        let history = loop {
            let history = read_workflow_history(
                &redpanda.broker,
                "unified-runtime-history-test",
                &WorkflowHistoryFilter::new(
                    "tenant",
                    "instance-signal-history",
                    "run-signal-history-1",
                ),
                StdDuration::from_millis(500),
                StdDuration::from_secs(2),
            )
            .await?;
            let event_types =
                history.iter().map(|event| event.event_type.as_str()).collect::<Vec<_>>();
            if event_types.contains(&"SignalReceived")
                && event_types.contains(&"ActivityTaskCompleted")
            {
                break history;
            }
            if Instant::now() >= history_deadline {
                anyhow::bail!(
                    "broker history did not contain hot runtime signal/completion events"
                );
            }
            sleep(StdDuration::from_millis(100)).await;
        };

        let trace = replay_compiled_history_trace(&history, &artifact)?;
        assert_eq!(trace.final_state.status, WorkflowStatus::Completed);
        assert_eq!(trace.final_state.output, Some(Value::String("hello fiona".to_owned())));
        assert!(history.iter().any(|event| event.event_type == "SignalReceived"));
        assert!(history.iter().any(|event| event.event_type == "ActivityTaskCompleted"));

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[test]
    fn schedule_activities_inherits_workflow_queue_when_step_uses_default() -> Result<()> {
        let artifact = CompiledWorkflowArtifact::new(
            "queue-inheritance".to_owned(),
            1,
            "unified-runtime-test",
            ArtifactEntrypoint { module: "workflow.ts".to_owned(), export: "workflow".to_owned() },
            CompiledWorkflow {
                initial_state: "start".to_owned(),
                states: BTreeMap::from([(
                    "start".to_owned(),
                    CompiledStateNode::Step {
                        handler: "greet".to_owned(),
                        input: Expression::Literal { value: Value::String("fiona".to_owned()) },
                        next: Some("done".to_owned()),
                        task_queue: None,
                        retry: None,
                        output_var: None,
                        on_error: None,
                        config: None,
                        schedule_to_start_timeout_ms: None,
                        schedule_to_close_timeout_ms: None,
                        start_to_close_timeout_ms: Some(30_000),
                        heartbeat_timeout_ms: None,
                    },
                )]),
                params: Vec::new(),
                non_cancellable_states: BTreeSet::new(),
            },
        );
        let instance = WorkflowInstanceState {
            tenant_id: "tenant".to_owned(),
            definition_id: "definition".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact".to_owned()),
            instance_id: "instance".to_owned(),
            run_id: "run".to_owned(),
            workflow_task_queue: "orders".to_owned(),
            sticky_workflow_build_id: None,
            sticky_workflow_poller_id: None,
            current_state: Some("start".to_owned()),
            context: None,
            artifact_execution: Some(ArtifactExecutionState::default()),
            status: WorkflowStatus::Running,
            input: Some(Value::Null),
            persisted_input_handle: None,
            memo: None,
            search_attributes: None,
            output: None,
            event_count: 1,
            last_event_id: Uuid::now_v7(),
            last_event_type: "WorkflowTriggered".to_owned(),
            updated_at: Utc::now(),
        };
        let plan = CompiledExecutionPlan {
            workflow_version: 1,
            final_state: "start".to_owned(),
            emissions: vec![fabrik_workflow::ExecutionEmission {
                event: WorkflowEvent::ActivityTaskScheduled {
                    activity_id: "start".to_owned(),
                    activity_type: "greet".to_owned(),
                    task_queue: "default".to_owned(),
                    attempt: 1,
                    input: Value::String("fiona".to_owned()),
                    config: None,
                    state: Some("start".to_owned()),
                    schedule_to_start_timeout_ms: None,
                    schedule_to_close_timeout_ms: None,
                    start_to_close_timeout_ms: Some(30_000),
                    heartbeat_timeout_ms: None,
                },
                state: Some("start".to_owned()),
            }],
            execution_state: ArtifactExecutionState::default(),
            context: Some(Value::String("fiona".to_owned())),
            output: None,
        };

        let scheduled = schedule_activities_from_plan(&artifact, &instance, &plan, Utc::now())?;
        assert_eq!(scheduled.len(), 1);
        assert_eq!(scheduled[0].task_queue, "orders");
        Ok(())
    }

    #[tokio::test]
    async fn poll_activity_tasks_respects_activity_build_compatibility() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        store
            .register_task_queue_build(
                "tenant",
                TaskQueueKind::Activity,
                "payments",
                "build-a",
                &[],
                None,
            )
            .await?;
        store
            .register_task_queue_build(
                "tenant",
                TaskQueueKind::Activity,
                "payments",
                "build-b",
                &[],
                None,
            )
            .await?;
        store
            .upsert_compatibility_set(
                "tenant",
                TaskQueueKind::Activity,
                "payments",
                "stable",
                &["build-a".to_owned()],
                true,
            )
            .await?;

        let now = Utc::now();
        let mut inner = RuntimeInner::default();
        let queue_key =
            QueueKey { tenant_id: "tenant".to_owned(), task_queue: "payments".to_owned() };
        let queued = QueuedActivity {
            task_queue: "payments".to_owned(),
            ..queued_activity("activity-1", 1, now)
        };
        store.upsert_activities_scheduled_batch(&[activity_schedule_update(&queued)]).await?;
        inner.ready.entry(queue_key.clone()).or_default().push_back(queued);

        let state_dir =
            std::env::temp_dir().join(format!("unified-runtime-compatibility-{}", Uuid::now_v7()));
        let app_state = test_app_state(store.clone(), inner, state_dir.clone()).await;
        apply_ownership_record(&app_state, 1, "test-owner", 4, now + ChronoDuration::seconds(30));
        let worker = WorkerApi { state: app_state.clone() };

        let incompatible = worker
            .poll_activity_tasks(Request::new(PollActivityTasksRequest {
                tenant_id: "tenant".to_owned(),
                task_queue: "payments".to_owned(),
                worker_id: "worker-b".to_owned(),
                worker_build_id: "build-b".to_owned(),
                poll_timeout_ms: 10,
                max_tasks: 1,
                supports_cbor: false,
            }))
            .await?
            .into_inner();
        assert!(incompatible.tasks.is_empty());

        {
            let inner = app_state.inner.lock().expect("unified runtime lock poisoned");
            let queue = inner.ready.get(&queue_key).context("ready queue should still exist")?;
            assert_eq!(queue.len(), 1);
        }

        let compatible = worker
            .poll_activity_tasks(Request::new(PollActivityTasksRequest {
                tenant_id: "tenant".to_owned(),
                task_queue: "payments".to_owned(),
                worker_id: "worker-a".to_owned(),
                worker_build_id: "build-a".to_owned(),
                poll_timeout_ms: 10,
                max_tasks: 1,
                supports_cbor: false,
            }))
            .await?
            .into_inner();
        assert_eq!(compatible.tasks.len(), 1);
        assert_eq!(compatible.tasks[0].activity_id, "activity-1");
        assert_eq!(compatible.tasks[0].task_queue, "payments");

        fs::remove_dir_all(state_dir).ok();
        Ok(())
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

        let scheduled = schedule_activities_from_plan(&artifact, &instance, &plan, emitted_at)?;
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
        let payload_store = test_payload_store(&std::env::temp_dir()).await;
        let restored =
            restore_runtime_state_from_store(&store, &broker, &payload_store, &debug).await?;
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

    #[tokio::test]
    async fn store_restore_includes_nonterminal_instance_rows_without_snapshots() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let artifact = throughput_artifact_with_options(
            "default",
            Some(ThroughputBackend::StreamV2.as_str()),
            Some("all_settled"),
            16,
        );
        store.put_artifact("tenant-a", &artifact).await?;

        let identity = WorkflowIdentity::new(
            "tenant-a",
            &artifact.definition_id,
            artifact.definition_version,
            artifact.artifact_hash.clone(),
            "instance-instance-restore",
            "run-instance-restore",
            "unified-runtime-test",
        );
        let occurred_at = Utc::now();
        let input = json!({"items": [json!({"ok": true})]});
        let trigger = test_event(
            &identity,
            WorkflowEvent::WorkflowTriggered { input: input.clone() },
            occurred_at,
        );
        let plan = artifact.execute_trigger_with_turn(
            &input,
            ExecutionTurnContext { event_id: trigger.event_id, occurred_at },
        )?;
        let mut instance = WorkflowInstanceState::try_from(&trigger)?;
        apply_compiled_plan(&mut instance, &plan);
        store.upsert_instance(&instance).await?;

        let debug = Arc::new(StdMutex::new(UnifiedDebugState::default()));
        let payload_store = test_payload_store(&std::env::temp_dir()).await;
        let restored = restore_runtime_state_from_store(
            &store,
            &BrokerConfig::new("127.0.0.1:9092", "workflow-events", 1),
            &payload_store,
            &debug,
        )
        .await?;
        let runtime = restored
            .instances
            .get(&RunKey {
                tenant_id: identity.tenant_id.clone(),
                instance_id: identity.instance_id.clone(),
                run_id: identity.run_id.clone(),
            })
            .context("restored runtime state should contain workflow instance row")?;

        assert_eq!(runtime.instance.current_state.as_deref(), Some("join"));
        assert_eq!(runtime.instance.input.as_ref(), Some(&input));
        assert_eq!(runtime.instance.context, plan.context);
        assert!(!runtime.instance.status.is_terminal());
        assert!(restored.ready.values().all(VecDeque::is_empty));

        let debug_snapshot = debug.lock().expect("unified debug lock poisoned").clone();
        assert!(debug_snapshot.restored_from_snapshot);
        assert_eq!(debug_snapshot.restore_records_applied, 1);

        Ok(())
    }

    #[tokio::test]
    async fn store_restore_rehydrates_stream_v2_trigger_input_from_outbox_event() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let artifact = throughput_artifact_with_options(
            "default",
            Some(ThroughputBackend::StreamV2.as_str()),
            Some("all_settled"),
            16,
        );
        store.put_artifact("tenant-a", &artifact).await?;
        let state_dir = std::env::temp_dir()
            .join(format!("unified-runtime-restore-payload-{}", Uuid::now_v7()));
        let payload_store = test_payload_store(&state_dir).await;

        let identity = WorkflowIdentity::new(
            "tenant-a",
            &artifact.definition_id,
            artifact.definition_version,
            artifact.artifact_hash.clone(),
            "instance-input-handle-restore",
            "run-input-handle-restore",
            "unified-runtime-test",
        );
        let occurred_at = Utc::now();
        let input = json!({"items": [json!({"ok": true}), json!({"ok": false})]});
        let trigger = test_event(
            &identity,
            WorkflowEvent::WorkflowTriggered { input: input.clone() },
            occurred_at,
        );
        let plan = artifact.execute_trigger_with_turn(
            &input,
            ExecutionTurnContext { event_id: trigger.event_id, occurred_at },
        )?;
        let mut instance = WorkflowInstanceState::try_from(&trigger)?;
        apply_compiled_plan(&mut instance, &plan);
        store.put_workflow_event_outbox(&trigger, occurred_at).await?;

        let mut persisted = instance.clone();
        assert!(persisted.compact_trigger_bulk_wait_for_replayable_restore());
        store.upsert_persisted_instance(&persisted).await?;

        let restored = restore_runtime_state_from_store(
            &store,
            &BrokerConfig::new("127.0.0.1:9092", "workflow-events", 1),
            &payload_store,
            &Arc::new(StdMutex::new(UnifiedDebugState::default())),
        )
        .await?;
        let runtime = restored
            .instances
            .get(&RunKey {
                tenant_id: identity.tenant_id.clone(),
                instance_id: identity.instance_id.clone(),
                run_id: identity.run_id.clone(),
            })
            .context("restored runtime state should contain outbox-backed workflow")?;

        assert_eq!(runtime.instance.input.as_ref(), Some(&input));
        assert_eq!(
            runtime.instance.context,
            Some(json!([json!({"ok": true}), json!({"ok": false})]))
        );
        assert!(
            runtime
                .instance
                .artifact_execution
                .as_ref()
                .is_some_and(|execution| artifact.waits_on_bulk_activity("join", execution))
        );
        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn store_snapshot_restore_falls_back_to_full_history_when_snapshot_tail_is_inconsistent()
    -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let publisher = redpanda.connect_publisher().await?;
        let broker = redpanda.broker.clone();

        let artifact = signal_then_step_artifact();
        store.put_artifact("tenant-a", &artifact).await?;

        let identity = WorkflowIdentity::new(
            "tenant-a",
            &artifact.definition_id,
            artifact.definition_version,
            artifact.artifact_hash.clone(),
            "instance-inconsistent-snapshot",
            "run-inconsistent-snapshot",
            "unified-runtime-test",
        );
        let trigger_time = Utc::now();
        let trigger = test_event(
            &identity,
            WorkflowEvent::WorkflowTriggered { input: Value::Null },
            trigger_time,
        );
        publisher.publish(&trigger, &trigger.partition_key).await?;

        let trigger_plan = artifact.execute_trigger_with_turn(
            &Value::Null,
            ExecutionTurnContext { event_id: trigger.event_id, occurred_at: trigger.occurred_at },
        )?;
        let mut stale_snapshot = WorkflowInstanceState::try_from(&trigger)?;
        apply_compiled_plan(&mut stale_snapshot, &trigger_plan);

        let signal_time = trigger_time + ChronoDuration::milliseconds(1);
        let signal_received = test_event(
            &identity,
            WorkflowEvent::SignalReceived {
                signal_id: "sig-restore".to_owned(),
                signal_type: "approve".to_owned(),
                payload: Value::String("approved".to_owned()),
            },
            signal_time,
        );
        publisher.publish(&signal_received, &signal_received.partition_key).await?;

        stale_snapshot.event_count = 2;
        stale_snapshot.last_event_id = signal_received.event_id;
        stale_snapshot.last_event_type = signal_received.event_type.clone();
        stale_snapshot.updated_at = signal_received.occurred_at;
        store.upsert_instance(&stale_snapshot).await?;
        store.put_snapshot(&stale_snapshot).await?;

        let signal_plan = artifact.execute_after_signal_with_turn(
            "wait_approve",
            "approve",
            &Value::String("approved".to_owned()),
            execution_state_for_event(&stale_snapshot, Some(&signal_received)),
            ExecutionTurnContext {
                event_id: signal_received.event_id,
                occurred_at: signal_received.occurred_at,
            },
        )?;
        let scheduled = schedule_activities_from_plan(
            &artifact,
            &{
                let mut projected = stale_snapshot.clone();
                projected.apply_event(&signal_received);
                apply_compiled_plan(&mut projected, &signal_plan);
                projected
            },
            &signal_plan,
            signal_received.occurred_at,
        )?;

        let completion_time = signal_time + ChronoDuration::milliseconds(1);
        let completion = test_event(
            &identity,
            WorkflowEvent::ActivityTaskCompleted {
                activity_id: scheduled[0].activity_id.clone(),
                attempt: scheduled[0].attempt,
                output: Value::String("hello approved".to_owned()),
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
                payload: ActivityTerminalPayload::Completed {
                    output: Value::String("hello approved".to_owned()),
                },
                event_id: completion.event_id,
                event_type: completion.event_type.clone(),
                occurred_at: completion.occurred_at,
            }])
            .await?;

        let payload_store = test_payload_store(&std::env::temp_dir()).await;
        let restored = restore_runtime_state_from_store(
            &store,
            &broker,
            &payload_store,
            &Arc::new(StdMutex::new(UnifiedDebugState::default())),
        )
        .await?;
        let runtime = restored
            .instances
            .get(&RunKey {
                tenant_id: identity.tenant_id.clone(),
                instance_id: identity.instance_id.clone(),
                run_id: identity.run_id.clone(),
            })
            .context("restored runtime state should contain fallback-restored workflow")?;

        assert_eq!(runtime.instance.status, WorkflowStatus::Completed);
        assert_eq!(runtime.instance.last_event_id, completion.event_id);
        assert_eq!(runtime.instance.last_event_type, "ActivityTaskCompleted");
        assert_eq!(runtime.instance.output, Some(Value::String("hello approved".to_owned())));
        assert!(runtime.active_activities.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn store_snapshot_restore_preserves_alpha_queue_memo_and_search_attributes_after_handoff()
    -> Result<()> {
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
            "instance-alpha-metadata",
            "run-alpha-metadata",
            "unified-runtime-test",
        );
        let partition_id = publisher.partition_for_key(&identity.partition_key);
        let trigger_time = Utc::now();
        let mut trigger = test_event(
            &identity,
            WorkflowEvent::WorkflowTriggered { input: json!({"items": [json!({"ok": true})]}) },
            trigger_time,
        );
        trigger.metadata.insert("workflow_task_queue".to_owned(), "alpha-orders".to_owned());
        trigger
            .metadata
            .insert("memo_json".to_owned(), "{\"region\":\"eu\",\"priority\":1}".to_owned());
        trigger.metadata.insert(
            "search_attributes_json".to_owned(),
            "{\"CustomKeywordField\":[\"vip\",\"alpha\"],\"Region\":\"eu\"}".to_owned(),
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

        let scheduled = schedule_activities_from_plan(&artifact, &instance, &plan, emitted_at)?;
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
                "unified-runtime-alpha-metadata-test",
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
                "unified-owner-alpha-a",
                StdDuration::from_millis(200),
            )
            .await?
            .context("owner a should claim partition")?;
        assert_eq!(owner_a.owner_epoch, 1);

        sleep(StdDuration::from_millis(250)).await;

        let owner_b = store
            .claim_partition_ownership(
                partition_id,
                "unified-owner-alpha-b",
                StdDuration::from_secs(5),
            )
            .await?
            .context("owner b should claim expired partition")?;
        assert_eq!(owner_b.owner_epoch, 2);

        let debug = Arc::new(StdMutex::new(UnifiedDebugState::default()));
        let payload_store = test_payload_store(&std::env::temp_dir()).await;
        let restored =
            restore_runtime_state_from_store(&store, &broker, &payload_store, &debug).await?;
        let runtime = restored
            .instances
            .get(&RunKey {
                tenant_id: identity.tenant_id.clone(),
                instance_id: identity.instance_id.clone(),
                run_id: identity.run_id.clone(),
            })
            .context("restored runtime state should contain the alpha metadata workflow")?;

        assert_eq!(runtime.instance.workflow_task_queue, "alpha-orders");
        assert_eq!(runtime.instance.memo, Some(json!({"region": "eu", "priority": 1})));
        assert_eq!(
            runtime.instance.search_attributes,
            Some(json!({"CustomKeywordField": ["vip", "alpha"], "Region": "eu"}))
        );
        assert_eq!(runtime.instance.status, WorkflowStatus::Completed);
        assert_eq!(runtime.instance.last_event_id, completion.event_id);
        assert!(runtime.active_activities.is_empty());
        assert!(restored.ready.values().all(VecDeque::is_empty));
        assert!(
            debug.lock().expect("unified debug lock poisoned").restored_from_snapshot,
            "restore should still come from snapshot-backed handoff"
        );

        Ok(())
    }
}
