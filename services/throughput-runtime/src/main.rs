mod aggregation;
mod bridge;
mod local_state;
mod stream_jobs;

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs,
    io::Write,
    process::{Command, Stdio},
    sync::{
        Arc, Mutex as StdMutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use aggregation::AggregationCoordinator;
use anyhow::{Context, Result};
use axum::{
    Json,
    extract::{Path, Query, State as AxumState},
    http::StatusCode,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use fabrik_broker::{
    BrokerConfig, JsonTopicConfig, JsonTopicPublisher, WorkflowPublisher, build_json_consumer,
    build_json_consumer_from_offsets, build_workflow_consumer, decode_json_record,
    partition_for_key,
};
use fabrik_config::{
    GrpcServiceConfig, HttpServiceConfig, PostgresConfig, RedpandaConfig,
    ThroughputOwnershipConfig, ThroughputRuntimeConfig,
};
use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::{
    PartitionOwnershipRecord, StreamJobBridgeHandleRecord, TaskQueueKind,
    ThroughputProjectionBatchStateUpdate, ThroughputProjectionChunkStateUpdate,
    ThroughputProjectionEvent, ThroughputResultSegmentRecord, ThroughputTerminalRecord,
    WorkflowBulkBatchRecord, WorkflowBulkBatchStatus, WorkflowBulkChunkRecord,
    WorkflowBulkChunkStatus, WorkflowStore,
};
use fabrik_throughput::{
    ADMISSION_POLICY_VERSION, ActivityCapabilityRegistry, ActivityExecutionCapabilities,
    BENCHMARK_ECHO_ACTIVITY, CORE_ACCEPT_ACTIVITY, CORE_ECHO_ACTIVITY, CORE_NOOP_ACTIVITY,
    CollectResultsBatchManifest, CollectResultsChunkSegmentRef, PayloadHandle, PayloadStore,
    PayloadStoreConfig, PayloadStoreKind, StartThroughputRunCommand, StreamsChangelogEntry,
    StreamsChangelogPayload, StreamsProjectionEvent, StreamsViewDeleteRecord, StreamsViewRecord,
    THROUGHPUT_BRIDGE_PROTOCOL_VERSION, ThroughputBackend, ThroughputBatchIdentity,
    ThroughputBridgeOperationKind, ThroughputChangelogEntry, ThroughputChangelogPayload,
    ThroughputChunkReport, ThroughputChunkReportPayload, ThroughputCommand,
    ThroughputCommandEnvelope, WorkerActivityManifest, benchmark_echo_item_requires_output,
    bulk_reducer_class, bulk_reducer_name, bulk_reducer_settles,
    can_complete_payloadless_bulk_chunk, can_use_payloadless_bulk_transport, decode_cbor,
    effective_aggregation_group_count, encode_cbor, execute_benchmark_echo,
    group_id_for_chunk_index, load_activity_capability_registry_from_env,
    parse_benchmark_compact_input_meta_from_handle,
    parse_benchmark_compact_total_items_from_handle, planned_reduction_tree_depth,
    resolve_activity_capabilities, stream_v2_fast_lane_enabled, synthesize_benchmark_echo_items,
    throughput_bridge_request_id, throughput_execution_mode, throughput_partition_key,
    throughput_reducer_execution_path, throughput_reducer_version, throughput_routing_reason,
    throughput_terminal_callback_dedupe_key, throughput_terminal_callback_event_id,
};
use fabrik_worker_protocol::activity_worker::{
    Ack, BulkActivityTask, BulkActivityTaskResult, PollBulkActivityTaskRequest,
    PollBulkActivityTaskResponse, ReportBulkActivityTaskResultsRequest,
    activity_worker_api_server::{ActivityWorkerApi, ActivityWorkerApiServer},
};
use fabrik_workflow::execute_handler;
use futures_util::StreamExt;
use local_state::{
    LeasedChunkSnapshot, LocalThroughputCoordinatorPaths, LocalThroughputDebugSnapshot,
    LocalThroughputShardPaths, LocalThroughputState, PreparedValidatedReport,
    ProjectedBatchGroupSummary, ProjectedBatchTerminal, ProjectedGroupTerminal,
    ProjectedTerminalApply, ReportValidation,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{Notify, mpsc, oneshot};
use tonic::{Request, Response, Status, transport::Server};
use tracing::{error, info, warn};
use uuid::Uuid;

const BULK_EVENT_OUTBOX_BATCH_SIZE: usize = 128;
const BULK_EVENT_OUTBOX_LEASE_SECONDS: i64 = 30;
const BULK_EVENT_OUTBOX_RETRY_MS: i64 = 250;
const BRIDGE_EVENT_CONCURRENCY: usize = 64;
pub(crate) const STREAMS_RUNTIME_SERVICE_NAME: &str = "streams-runtime";
pub(crate) const STREAMS_RUNTIME_DEBUG_SERVICE_NAME: &str = "streams-runtime-debug";
pub(crate) const STREAMS_RUNTIME_DEBUG_ROUTE: &str = "/debug/streams-runtime";
pub(crate) const STREAMS_RUNTIME_DEBUG_BATCH_ROUTE: &str =
    "/debug/streams-runtime/batches/{tenant_id}/{instance_id}/{run_id}/{batch_id}";
pub(crate) const STREAMS_RUNTIME_DEBUG_CHUNKS_ROUTE: &str =
    "/debug/streams-runtime/batches/{tenant_id}/{instance_id}/{run_id}/{batch_id}/chunks";
pub(crate) const STREAMS_RUNTIME_DEBUG_STREAM_JOB_VIEW_ROUTE: &str = "/debug/streams-runtime/stream-jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}/views/{view_name}/keys/{logical_key}";
pub(crate) const STREAMS_RUNTIME_DEBUG_STREAM_JOB_VIEW_KEYS_ROUTE: &str = "/debug/streams-runtime/stream-jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}/views/{view_name}/keys";
pub(crate) const STREAMS_RUNTIME_DEBUG_STREAM_JOB_VIEW_ENTRIES_ROUTE: &str = "/debug/streams-runtime/stream-jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}/views/{view_name}/entries";
pub(crate) const STREAMS_RUNTIME_DEBUG_STREAM_JOB_VIEW_SCAN_ROUTE: &str = "/debug/streams-runtime/stream-jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}/views/{view_name}/scan";
pub(crate) const STREAMS_RUNTIME_DEBUG_STREAM_JOB_VIEW_RUNTIME_ROUTE: &str = "/debug/streams-runtime/stream-jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}/views/{view_name}/runtime";
pub(crate) const STREAMS_RUNTIME_DEBUG_STREAM_JOB_VIEW_PROJECTION_REBUILD_ROUTE: &str = "/debug/streams-runtime/stream-jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}/views/{view_name}/projections/rebuild";
pub(crate) const STREAMS_RUNTIME_DEBUG_STREAM_JOB_PROJECTION_REBUILD_ROUTE: &str = "/debug/streams-runtime/stream-jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}/projections/rebuild";
pub(crate) const STREAMS_RUNTIME_DEBUG_STREAM_JOB_RUNTIME_ROUTE: &str =
    "/debug/streams-runtime/stream-jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}/runtime";
const LEGACY_THROUGHPUT_DEBUG_ROUTE: &str = "/debug/throughput";
const LEGACY_THROUGHPUT_DEBUG_BATCH_ROUTE: &str =
    "/debug/throughput/batches/{tenant_id}/{instance_id}/{run_id}/{batch_id}";
const LEGACY_THROUGHPUT_DEBUG_CHUNKS_ROUTE: &str =
    "/debug/throughput/batches/{tenant_id}/{instance_id}/{run_id}/{batch_id}/chunks";
const LEGACY_THROUGHPUT_DEBUG_STREAM_JOB_RUNTIME_ROUTE: &str =
    "/debug/throughput/stream-jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}/runtime";
const LEGACY_THROUGHPUT_DEBUG_STREAM_JOB_VIEW_RUNTIME_ROUTE: &str = "/debug/throughput/stream-jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}/views/{view_name}/runtime";
const LEGACY_THROUGHPUT_DEBUG_STREAM_JOB_VIEW_PROJECTION_REBUILD_ROUTE: &str = "/debug/throughput/stream-jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}/views/{view_name}/projections/rebuild";
const LEGACY_THROUGHPUT_DEBUG_STREAM_JOB_PROJECTION_REBUILD_ROUTE: &str =
    "/debug/throughput/stream-jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}/projections/rebuild";
const LOCAL_BENCHMARK_FASTLANE_WORKER_ID: &str = "streams-runtime-local-fastlane";
const LOCAL_BENCHMARK_FASTLANE_WORKER_BUILD_ID: &str = "streams-runtime-local-fastlane";
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
            .or_else(|| {
                self.by_task_queue.get("").and_then(|activities| activities.get(activity_type))
            })
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
    aggregation: AggregationCoordinator,
    shard_workers: Arc<OnceLock<ShardWorkerPool>>,
    json_brokers: String,
    workflow_publisher: WorkflowPublisher,
    throughput_changelog_publisher: JsonTopicPublisher<ThroughputChangelogEntry>,
    streams_changelog_publisher: JsonTopicPublisher<StreamsChangelogEntry>,
    throughput_projection_publisher: JsonTopicPublisher<ThroughputProjectionEvent>,
    streams_projection_publisher: JsonTopicPublisher<StreamsProjectionEvent>,
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
    #[cfg(test)]
    fail_next_workflow_outbox_writes: Arc<AtomicUsize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishPlane {
    Throughput,
    Streams,
}

#[derive(Clone)]
struct WorkerApi {
    state: AppState,
    shard_workers: ShardWorkerPool,
}

struct ShardWorker {
    state: AppState,
    scheduling_state: LocalThroughputState,
    scheduling_partition_id: Option<i32>,
    shard_partitions: HashSet<i32>,
}

#[derive(Clone)]
struct ShardWorkerHandle {
    command_tx: mpsc::Sender<ShardWorkerCommand>,
}

#[derive(Clone)]
struct ShardWorkerPool {
    state: AppState,
    handles: Vec<ShardWorkerHandle>,
    handles_by_partition: Arc<HashMap<i32, ShardWorkerHandle>>,
    coordinator_storage: LocalThroughputCoordinatorPaths,
    local_state_by_partition: Arc<HashMap<i32, LocalThroughputState>>,
    storage_by_partition: Arc<HashMap<i32, LocalThroughputShardPaths>>,
    next_poll_index: Arc<AtomicUsize>,
}

enum ShardWorkerCommand {
    ApplyStreamJobPartitionWork {
        handle: StreamJobBridgeHandleRecord,
        work: stream_jobs::StreamJobPartitionWork,
        respond_to: oneshot::Sender<Result<StreamJobActivationResult>>,
    },
    PollTopicStreamJobSourcePartition {
        handle: StreamJobBridgeHandleRecord,
        request: stream_jobs::TopicStreamJobPollRequest,
        throughput_partitions: i32,
        respond_to: oneshot::Sender<Result<stream_jobs::TopicStreamJobPollResult>>,
    },
    PollBulkActivityTasks {
        request: PollBulkActivityTaskRequest,
        respond_to: oneshot::Sender<Result<PollBulkActivityTaskResponse, Status>>,
    },
    ApplyReportBatch {
        report_batch: Vec<ThroughputChunkReport>,
        capture_report_log: bool,
        respond_to: oneshot::Sender<Result<()>>,
    },
    RunLocalBenchmarkFastlaneOnce {
        batch_limit: Option<usize>,
        lease_ttl: chrono::Duration,
        max_chunks: usize,
        respond_to: oneshot::Sender<Result<usize>>,
    },
    RunExpiredChunkSweepOnce {
        now: chrono::DateTime<chrono::Utc>,
        limit: usize,
        respond_to: oneshot::Sender<Result<usize>>,
    },
}

impl ShardWorker {
    fn new(
        state: AppState,
        scheduling_state: LocalThroughputState,
        shard_partitions: HashSet<i32>,
    ) -> Self {
        let scheduling_partition_id = if shard_partitions.len() == 1 {
            shard_partitions.iter().copied().next()
        } else {
            None
        };
        Self { state, scheduling_state, scheduling_partition_id, shard_partitions }
    }

    fn owned_partitions(&self) -> HashSet<i32> {
        owned_throughput_partitions(&self.state)
            .into_iter()
            .filter(|partition_id| self.shard_partitions.contains(partition_id))
            .collect()
    }

    fn mirror_scheduling_changelog_records(
        &self,
        entries: &[ThroughputChangelogEntry],
    ) -> Result<()> {
        if self.scheduling_partition_id.is_none() || entries.is_empty() {
            return Ok(());
        }
        self.scheduling_state.mirror_changelog_records(entries)
    }

    fn apply_stream_job_partition_work(
        &self,
        handle: &StreamJobBridgeHandleRecord,
        work: &stream_jobs::StreamJobPartitionWork,
    ) -> Result<StreamJobActivationResult> {
        stream_jobs::apply_stream_job_partition_work_on_local_state(
            &self.scheduling_state,
            handle,
            work,
            self.state.runtime.owner_first_apply,
        )
    }

    async fn poll_topic_stream_job_source_partition(
        &self,
        handle: &StreamJobBridgeHandleRecord,
        request: &stream_jobs::TopicStreamJobPollRequest,
        throughput_partitions: i32,
    ) -> Result<stream_jobs::TopicStreamJobPollResult> {
        stream_jobs::poll_topic_stream_job_source_partition_on_local_state(
            Some(&self.state.store),
            &self.state,
            &self.scheduling_state,
            handle,
            request,
            throughput_partitions,
            Utc::now(),
        )
        .await
    }
}

impl ShardWorkerHandle {
    fn new(
        state: AppState,
        scheduling_state: LocalThroughputState,
        shard_partitions: HashSet<i32>,
    ) -> Self {
        let mailbox_capacity =
            state.runtime.report_apply_batch_size.max(state.runtime.poll_max_tasks).max(32);
        let (command_tx, command_rx) = mpsc::channel(mailbox_capacity);
        tokio::spawn(run_shard_worker_loop(
            ShardWorker::new(state, scheduling_state, shard_partitions),
            command_rx,
        ));
        Self { command_tx }
    }

    async fn poll_bulk_activity_tasks(
        &self,
        request: PollBulkActivityTaskRequest,
    ) -> Result<PollBulkActivityTaskResponse, Status> {
        let (respond_to, response_rx) = oneshot::channel();
        self.command_tx
            .send(ShardWorkerCommand::PollBulkActivityTasks { request, respond_to })
            .await
            .map_err(|_| Status::unavailable("shard worker stopped"))?;
        response_rx.await.map_err(|_| Status::unavailable("shard worker stopped"))?
    }

    async fn apply_stream_job_partition_work(
        &self,
        handle: StreamJobBridgeHandleRecord,
        work: stream_jobs::StreamJobPartitionWork,
    ) -> Result<StreamJobActivationResult> {
        let (respond_to, response_rx) = oneshot::channel();
        self.command_tx
            .send(ShardWorkerCommand::ApplyStreamJobPartitionWork { handle, work, respond_to })
            .await
            .map_err(|_| anyhow::anyhow!("shard worker stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("shard worker stopped"))?
    }

    async fn poll_topic_stream_job_source_partition(
        &self,
        handle: StreamJobBridgeHandleRecord,
        request: stream_jobs::TopicStreamJobPollRequest,
        throughput_partitions: i32,
    ) -> Result<stream_jobs::TopicStreamJobPollResult> {
        let (respond_to, response_rx) = oneshot::channel();
        self.command_tx
            .send(ShardWorkerCommand::PollTopicStreamJobSourcePartition {
                handle,
                request,
                throughput_partitions,
                respond_to,
            })
            .await
            .map_err(|_| anyhow::anyhow!("shard worker stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("shard worker stopped"))?
    }

    async fn apply_report_batch(
        &self,
        report_batch: Vec<ThroughputChunkReport>,
        capture_report_log: bool,
    ) -> Result<()> {
        let (respond_to, response_rx) = oneshot::channel();
        self.command_tx
            .send(ShardWorkerCommand::ApplyReportBatch {
                report_batch,
                capture_report_log,
                respond_to,
            })
            .await
            .map_err(|_| anyhow::anyhow!("shard worker stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("shard worker stopped"))?
    }

    async fn run_local_benchmark_fastlane_once(
        &self,
        batch_limit: Option<usize>,
        lease_ttl: chrono::Duration,
        max_chunks: usize,
    ) -> Result<usize> {
        let (respond_to, response_rx) = oneshot::channel();
        self.command_tx
            .send(ShardWorkerCommand::RunLocalBenchmarkFastlaneOnce {
                batch_limit,
                lease_ttl,
                max_chunks,
                respond_to,
            })
            .await
            .map_err(|_| anyhow::anyhow!("shard worker stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("shard worker stopped"))?
    }

    async fn run_expired_chunk_sweep_once(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        limit: usize,
    ) -> Result<usize> {
        let (respond_to, response_rx) = oneshot::channel();
        self.command_tx
            .send(ShardWorkerCommand::RunExpiredChunkSweepOnce { now, limit, respond_to })
            .await
            .map_err(|_| anyhow::anyhow!("shard worker stopped"))?;
        response_rx.await.map_err(|_| anyhow::anyhow!("shard worker stopped"))?
    }
}

impl ShardWorkerPool {
    fn new(state: AppState, managed_partitions: &[i32]) -> Self {
        let mut handles = Vec::with_capacity(managed_partitions.len().max(1));
        let mut handles_by_partition = HashMap::with_capacity(managed_partitions.len().max(1));
        let mut local_state_by_partition = HashMap::with_capacity(managed_partitions.len().max(1));
        let mut storage_by_partition = HashMap::with_capacity(managed_partitions.len().max(1));
        let coordinator_storage = LocalThroughputState::coordinator_paths(
            &state.runtime.local_state_dir,
            &state.runtime.checkpoint_dir,
        );
        for partition_id in managed_partitions.iter().copied() {
            let partition_state = LocalThroughputState::open_partition_shard(
                &state.runtime.local_state_dir,
                &state.runtime.checkpoint_dir,
                partition_id,
                state.runtime.checkpoint_retention,
            )
            .unwrap_or_else(|error| {
                panic!("failed to open shard-local throughput state for partition {partition_id}: {error}")
            });
            let storage = LocalThroughputState::partition_shard_paths(
                &state.runtime.local_state_dir,
                &state.runtime.checkpoint_dir,
                partition_id,
            );
            let handle = ShardWorkerHandle::new(
                state.clone(),
                partition_state.clone(),
                HashSet::from([partition_id]),
            );
            local_state_by_partition.insert(partition_id, partition_state);
            storage_by_partition.insert(partition_id, storage);
            handles_by_partition.insert(partition_id, handle.clone());
            handles.push(handle);
        }
        if handles.is_empty() {
            let partition_state = LocalThroughputState::open_partition_shard(
                &state.runtime.local_state_dir,
                &state.runtime.checkpoint_dir,
                0,
                state.runtime.checkpoint_retention,
            )
            .unwrap_or_else(|error| {
                panic!("failed to open fallback shard-local throughput state: {error}")
            });
            handles.push(ShardWorkerHandle::new(state.clone(), partition_state, HashSet::new()));
        }
        Self {
            state,
            handles,
            handles_by_partition: Arc::new(handles_by_partition),
            coordinator_storage,
            local_state_by_partition: Arc::new(local_state_by_partition),
            storage_by_partition: Arc::new(storage_by_partition),
            next_poll_index: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn poll_bulk_activity_tasks(
        &self,
        request: PollBulkActivityTaskRequest,
    ) -> Result<PollBulkActivityTaskResponse, Status> {
        if self.handles.is_empty() {
            return Ok(PollBulkActivityTaskResponse { tasks: Vec::new() });
        }
        let shard_count = self.handles.len();
        let start = self.next_poll_index.fetch_add(1, Ordering::Relaxed) % shard_count;
        let poll_started_at = tokio::time::Instant::now();
        let poll_timeout = Duration::from_millis(request.poll_timeout_ms.max(1));
        for offset in 0..shard_count {
            let elapsed = poll_started_at.elapsed();
            if elapsed >= poll_timeout {
                break;
            }
            let remaining = poll_timeout.saturating_sub(elapsed);
            let shards_left = shard_count.saturating_sub(offset);
            let slice_ms = if shards_left <= 1 {
                remaining.as_millis()
            } else {
                (remaining.as_millis() / u128::try_from(shards_left).unwrap_or(1)).max(1)
            };
            let mut shard_request = request.clone();
            shard_request.poll_timeout_ms = u64::try_from(slice_ms).unwrap_or(u64::MAX);
            let handle = &self.handles[(start + offset) % shard_count];
            let response = handle.poll_bulk_activity_tasks(shard_request).await?;
            if !response.tasks.is_empty() {
                return Ok(response);
            }
        }
        Ok(PollBulkActivityTaskResponse { tasks: Vec::new() })
    }

    async fn activate_stream_job(
        &self,
        handle: StreamJobBridgeHandleRecord,
        throughput_partitions: i32,
    ) -> Result<Option<StreamJobActivationResult>> {
        let occurred_at = Utc::now();
        let owner_epoch = stream_job_owner_epoch(&self.state, &handle.job_id).or(Some(1));
        if let Some(topic_plan) = stream_jobs::plan_topic_stream_job_owner_polls(
            &self.state.local_state,
            Some(&self.state.store),
            &self.state,
            &handle,
            throughput_partitions,
            owner_epoch.unwrap_or(1),
            occurred_at,
        )
        .await?
        {
            let mut projection_records = Vec::new();
            let mut changelog_records = Vec::with_capacity(
                topic_plan
                    .poll_requests
                    .len()
                    .saturating_add(topic_plan.post_apply_changelog_records.len() + 2),
            );
            if let Some(execution_planned) = topic_plan.execution_planned.clone() {
                let ChangelogRecordEntry::Streams(streams_entry) = &execution_planned.entry else {
                    anyhow::bail!("stream job execution plan must use a streams changelog entry");
                };
                self.state.local_state.mirror_streams_changelog_entry(&streams_entry)?;
                for local_state in self.local_state_by_partition.values() {
                    local_state.mirror_streams_changelog_entry(&streams_entry)?;
                }
                changelog_records.push(execution_planned);
            }
            if let Some(runtime_state) = &topic_plan.runtime_state_update {
                self.state.local_state.upsert_stream_job_runtime_state(runtime_state)?;
                for local_state in self.local_state_by_partition.values() {
                    local_state.upsert_stream_job_runtime_state(runtime_state)?;
                }
            }
            for record in &topic_plan.post_apply_changelog_records {
                let ChangelogRecordEntry::Streams(streams_entry) = &record.entry else {
                    anyhow::bail!("topic stream job owner poll records must use streams changelog");
                };
                self.state.local_state.mirror_streams_changelog_entry(streams_entry)?;
                for local_state in self.local_state_by_partition.values() {
                    local_state.mirror_streams_changelog_entry(streams_entry)?;
                }
            }
            for request in topic_plan.poll_requests {
                if self
                    .state
                    .store
                    .get_stream_job_callback_handle_by_handle_id(&handle.handle_id)
                    .await?
                    .is_some_and(|current| current.cancellation_requested_at.is_some())
                {
                    break;
                }
                let Some(source_worker) =
                    self.handles_by_partition.get(&request.source_owner_partition_id)
                else {
                    anyhow::bail!(
                        "no shard worker configured for topic source owner partition {}",
                        request.source_owner_partition_id
                    );
                };
                let polled = source_worker
                    .poll_topic_stream_job_source_partition(
                        handle.clone(),
                        request,
                        throughput_partitions,
                    )
                    .await?;
                for work in polled.partition_work {
                    let Some(worker) = self.handles_by_partition.get(&work.stream_partition_id)
                    else {
                        anyhow::bail!(
                            "no shard worker configured for stream job partition {}",
                            work.stream_partition_id
                        );
                    };
                    let applied =
                        worker.apply_stream_job_partition_work(handle.clone(), work).await?;
                    projection_records.extend(applied.projection_records);
                    changelog_records.extend(applied.changelog_records);
                }
                projection_records.extend(polled.projection_records);
                changelog_records.extend(polled.changelog_records);
            }
            projection_records.extend(topic_plan.post_apply_projection_records);
            changelog_records.extend(topic_plan.post_apply_changelog_records);
            if let Some(terminalized) = topic_plan.terminalized {
                if self
                    .state
                    .local_state
                    .load_stream_job_runtime_state(&handle.handle_id)?
                    .is_some_and(|state| {
                        state
                            .source_kind
                            .as_deref()
                            .is_some_and(|kind| kind == fabrik_throughput::STREAM_SOURCE_TOPIC)
                    })
                {
                    changelog_records.push(terminalized);
                }
            }
            return Ok(Some(StreamJobActivationResult { projection_records, changelog_records }));
        }
        let Some(plan) = stream_jobs::plan_stream_job_activation(
            &self.state.local_state,
            Some(&self.state.store),
            Some(&self.state),
            &handle,
            throughput_partitions,
            owner_epoch,
            occurred_at,
        )
        .await?
        else {
            return Ok(None);
        };

        let mut projection_records = Vec::new();
        let mut changelog_records = Vec::with_capacity(
            plan.partition_work.len().saturating_add(plan.post_apply_changelog_records.len() + 2),
        );
        if let Some(execution_planned) = plan.execution_planned.clone() {
            let ChangelogRecordEntry::Streams(streams_entry) = &execution_planned.entry else {
                anyhow::bail!("stream job execution plan must use a streams changelog entry");
            };
            self.state.local_state.mirror_streams_changelog_entry(&streams_entry)?;
            if let Some(runtime_state) = &plan.runtime_state_update {
                self.state.local_state.upsert_stream_job_runtime_state(runtime_state)?;
            }
            if !plan.partition_work.is_empty() {
                let manifest = plan
                    .partition_work
                    .iter()
                    .map(stream_jobs::local_dispatch_batch_from_work)
                    .collect::<Vec<_>>();
                self.state.local_state.replace_stream_job_dispatch_manifest(
                    &handle.handle_id,
                    manifest.clone(),
                    streams_entry.occurred_at,
                )?;
                for partition_id in
                    manifest.iter().map(|batch| batch.stream_partition_id).collect::<HashSet<_>>()
                {
                    if let Some(local_state) = self.local_state_for_partition(partition_id) {
                        local_state.mirror_streams_changelog_entry(&streams_entry)?;
                        local_state.replace_stream_job_dispatch_manifest(
                            &handle.handle_id,
                            manifest.clone(),
                            streams_entry.occurred_at,
                        )?;
                    }
                }
            }
            changelog_records.push(execution_planned);
        } else if let Some(runtime_state) = &plan.runtime_state_update {
            self.state.local_state.upsert_stream_job_runtime_state(runtime_state)?;
        }
        for work in plan.partition_work {
            if self
                .state
                .store
                .get_stream_job_callback_handle_by_handle_id(&handle.handle_id)
                .await?
                .is_some_and(|current| current.cancellation_requested_at.is_some())
            {
                self.state
                    .local_state
                    .mark_stream_job_dispatch_cancelled(&handle.handle_id, Utc::now())?;
                break;
            }
            let Some(worker) = self.handles_by_partition.get(&work.stream_partition_id) else {
                anyhow::bail!(
                    "no shard worker configured for stream job partition {}",
                    work.stream_partition_id
                );
            };
            let batch_id = work.batch_id.clone();
            let completes_dispatch = work.completes_dispatch;
            let occurred_at = work.occurred_at;
            let applied = worker.apply_stream_job_partition_work(handle.clone(), work).await?;
            self.state.local_state.mark_stream_job_dispatch_batch_applied(
                &handle.handle_id,
                &batch_id,
                completes_dispatch,
                occurred_at,
            )?;
            projection_records.extend(applied.projection_records);
            changelog_records.extend(applied.changelog_records);
        }
        if let Some(source_cursors) = plan.source_cursor_updates {
            self.state.local_state.replace_stream_job_source_cursors(
                &handle.handle_id,
                source_cursors,
                Utc::now(),
            )?;
        }
        projection_records.extend(plan.post_apply_projection_records);
        changelog_records.extend(plan.post_apply_changelog_records);
        if let Some(terminalized) = plan.terminalized {
            if self.state.local_state.load_stream_job_runtime_state(&handle.handle_id)?.is_some_and(
                |state| {
                    let bounded_completed = state.dispatch_completed_at.is_some()
                        && state.dispatch_cancelled_at.is_none();
                    let topic_cancelled = state
                        .source_kind
                        .as_deref()
                        .is_some_and(|kind| kind == fabrik_throughput::STREAM_SOURCE_TOPIC);
                    bounded_completed || topic_cancelled
                },
            ) {
                changelog_records.push(terminalized);
            }
        }
        Ok(Some(StreamJobActivationResult { projection_records, changelog_records }))
    }

    async fn apply_report_batch(
        &self,
        report_batch: Vec<ThroughputChunkReport>,
        capture_report_log: bool,
        throughput_partitions: i32,
    ) -> Result<()> {
        let mut batches_by_partition = HashMap::<i32, Vec<ThroughputChunkReport>>::new();
        for report in report_batch {
            let partition_id = throughput_partition_for_batch(
                &report.batch_id,
                report.group_id,
                throughput_partitions,
            );
            batches_by_partition.entry(partition_id).or_default().push(report);
        }
        let mut partition_ids = batches_by_partition.keys().copied().collect::<Vec<_>>();
        partition_ids.sort_unstable();
        for partition_id in partition_ids {
            let Some(handle) = self.handles_by_partition.get(&partition_id) else {
                anyhow::bail!("no shard worker configured for throughput partition {partition_id}");
            };
            if let Some(batch) = batches_by_partition.remove(&partition_id) {
                handle.apply_report_batch(batch, capture_report_log).await?;
            }
        }
        Ok(())
    }

    fn handles(&self) -> &[ShardWorkerHandle] {
        &self.handles
    }

    fn coordinator_storage(&self) -> &LocalThroughputCoordinatorPaths {
        &self.coordinator_storage
    }

    fn local_state_for_partition(&self, partition_id: i32) -> Option<&LocalThroughputState> {
        self.local_state_by_partition.get(&partition_id)
    }

    fn storage_paths(&self) -> impl Iterator<Item = &LocalThroughputShardPaths> {
        self.storage_by_partition.values()
    }

    fn sync_batch_chunks(
        &self,
        batch: &WorkflowBulkBatchRecord,
        chunks: &[WorkflowBulkChunkRecord],
        throughput_partitions: i32,
    ) -> Result<()> {
        let mut chunks_by_partition = HashMap::<i32, Vec<WorkflowBulkChunkRecord>>::new();
        for chunk in chunks {
            let partition_id = throughput_partition_for_batch(
                &chunk.batch_id,
                chunk.group_id,
                throughput_partitions,
            );
            chunks_by_partition.entry(partition_id).or_default().push(chunk.clone());
        }
        for (partition_id, partition_chunks) in chunks_by_partition {
            let Some(local_state) = self.local_state_for_partition(partition_id) else {
                anyhow::bail!("missing shard-local state for throughput partition {partition_id}");
            };
            local_state.upsert_batch_with_chunks(batch, &partition_chunks)?;
        }
        Ok(())
    }

    async fn run_expired_chunk_sweep_once(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        limit_per_shard: usize,
    ) -> Result<usize> {
        let mut requeued = 0usize;
        for handle in &self.handles {
            requeued = requeued
                .saturating_add(handle.run_expired_chunk_sweep_once(now, limit_per_shard).await?);
        }
        Ok(requeued)
    }
}

async fn run_shard_worker_loop(
    shard_worker: ShardWorker,
    mut command_rx: mpsc::Receiver<ShardWorkerCommand>,
) {
    while let Some(command) = command_rx.recv().await {
        match command {
            ShardWorkerCommand::ApplyStreamJobPartitionWork { handle, work, respond_to } => {
                let _ =
                    respond_to.send(shard_worker.apply_stream_job_partition_work(&handle, &work));
            }
            ShardWorkerCommand::PollTopicStreamJobSourcePartition {
                handle,
                request,
                throughput_partitions,
                respond_to,
            } => {
                let _ = respond_to.send(
                    shard_worker
                        .poll_topic_stream_job_source_partition(
                            &handle,
                            &request,
                            throughput_partitions,
                        )
                        .await,
                );
            }
            ShardWorkerCommand::PollBulkActivityTasks { request, respond_to } => {
                let _ = respond_to.send(shard_worker.poll_bulk_activity_tasks(request).await);
            }
            ShardWorkerCommand::ApplyReportBatch {
                report_batch,
                capture_report_log,
                respond_to,
            } => {
                let _ = respond_to
                    .send(shard_worker.apply_report_batch(&report_batch, capture_report_log).await);
            }
            ShardWorkerCommand::RunLocalBenchmarkFastlaneOnce {
                batch_limit,
                lease_ttl,
                max_chunks,
                respond_to,
            } => {
                let _ = respond_to.send(
                    shard_worker
                        .run_local_benchmark_fastlane_once(batch_limit, lease_ttl, max_chunks)
                        .await,
                );
            }
            ShardWorkerCommand::RunExpiredChunkSweepOnce { now, limit, respond_to } => {
                let _ =
                    respond_to.send(shard_worker.run_expired_chunk_sweep_once(now, limit).await);
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Default)]
struct ThroughputDebugState {
    throughput_commands_received: u64,
    streams_commands_received: u64,
    poll_requests: u64,
    poll_responses: u64,
    leased_tasks: u64,
    empty_polls: u64,
    lease_misses_with_ready_chunks: u64,
    last_lease_selection_debug: Option<serde_json::Value>,
    bridged_batches: u64,
    duplicate_bridge_events: u64,
    duplicate_stream_job_checkpoint_publications_ignored: u64,
    duplicate_stream_job_query_requests_ignored: u64,
    duplicate_stream_job_terminal_publications_ignored: u64,
    bridge_failures: u64,
    report_rpcs_received: u64,
    reports_received: u64,
    report_batches_applied: u64,
    report_batch_items_total: u64,
    reports_rejected: u64,
    local_fastlane_chunks_completed: u64,
    local_fastlane_batches_applied: u64,
    projection_events_published: u64,
    throughput_projection_events_published: u64,
    streams_projection_events_published: u64,
    projection_events_skipped: u64,
    projection_events_applied_directly: u64,
    changelog_entries_published: u64,
    throughput_changelog_entries_published: u64,
    streams_changelog_entries_published: u64,
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
    last_duplicate_stream_job_checkpoint_publication_at: Option<chrono::DateTime<chrono::Utc>>,
    last_duplicate_stream_job_checkpoint_publication: Option<String>,
    last_duplicate_stream_job_query_request_at: Option<chrono::DateTime<chrono::Utc>>,
    last_duplicate_stream_job_terminal_publication_at: Option<chrono::DateTime<chrono::Utc>>,
    last_throughput_command_at: Option<chrono::DateTime<chrono::Utc>>,
    last_streams_command_at: Option<chrono::DateTime<chrono::Utc>>,
    last_report_at: Option<chrono::DateTime<chrono::Utc>>,
    last_terminal_event_at: Option<chrono::DateTime<chrono::Utc>>,
    last_lease_miss_with_ready_chunks_at: Option<chrono::DateTime<chrono::Utc>>,
    last_report_rejection_reason: Option<String>,
    last_throttle_at: Option<chrono::DateTime<chrono::Utc>>,
    last_throttle_reason: Option<String>,
    last_apply_divergence: Option<String>,
    last_catchup_wait_partition: Option<i32>,
    last_duplicate_stream_job_query_request: Option<String>,
    last_duplicate_stream_job_terminal_publication: Option<String>,
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
    prefix: Option<String>,
    #[serde(alias = "logicalKeyPrefix", alias = "logical_key_prefix")]
    logical_key_prefix: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct StreamJobRuntimeStatsQuery {
    #[serde(alias = "sourcePartitionId")]
    source_partition_id: Option<i32>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct StreamJobViewScanQuery {
    consistency: Option<String>,
    prefix: Option<String>,
    #[serde(alias = "keyPrefix")]
    key_prefix: Option<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StrongStreamJobViewResponse {
    handle_id: String,
    job_id: String,
    view_name: String,
    logical_key: String,
    output: Value,
    checkpoint_sequence: i64,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StrongStreamJobViewKeyRecord {
    logical_key: String,
    checkpoint_sequence: i64,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StrongStreamJobViewKeysResponse {
    total: usize,
    keys: Vec<StrongStreamJobViewKeyRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StrongStreamJobViewEntryRecord {
    logical_key: String,
    output: Value,
    checkpoint_sequence: i64,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StrongStreamJobViewEntriesResponse {
    total: usize,
    entries: Vec<StrongStreamJobViewEntryRecord>,
}

#[derive(Debug, Clone)]
struct ChunkTerminalArtifacts {
    result_handle: Option<Value>,
    cancellation_reason: Option<String>,
    cancellation_metadata: Option<Value>,
}

#[derive(Debug, Clone)]
pub(crate) struct StreamJobActivationResult {
    pub projection_records: Vec<StreamProjectionRecord>,
    pub changelog_records: Vec<StreamChangelogRecord>,
}

#[derive(Debug, Clone)]
pub(crate) enum ProjectionRecordEvent {
    Throughput(ThroughputProjectionEvent),
    Streams(StreamsProjectionEvent),
}

impl ProjectionRecordEvent {
    fn plane(&self) -> PublishPlane {
        match self {
            Self::Throughput(_) => PublishPlane::Throughput,
            Self::Streams(_) => PublishPlane::Streams,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StreamProjectionRecord {
    pub key: String,
    pub event: ProjectionRecordEvent,
}

impl StreamProjectionRecord {
    fn throughput(key: String, event: ThroughputProjectionEvent) -> Self {
        Self { key, event: ProjectionRecordEvent::Throughput(event) }
    }

    fn streams(key: String, event: StreamsProjectionEvent) -> Self {
        Self { key, event: ProjectionRecordEvent::Streams(event) }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StreamJobTerminalResult {
    pub status: fabrik_throughput::StreamJobBridgeHandleStatus,
    pub output: Option<Value>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
enum ChangelogRecordEntry {
    Throughput(ThroughputChangelogEntry),
    Streams(StreamsChangelogEntry),
}

impl ChangelogRecordEntry {
    fn plane(&self) -> PublishPlane {
        match self {
            Self::Throughput(_) => PublishPlane::Throughput,
            Self::Streams(_) => PublishPlane::Streams,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StreamChangelogRecord {
    key: String,
    entry: ChangelogRecordEntry,
    local_mirror_applied: bool,
    partition_mirror_applied: bool,
}

impl StreamChangelogRecord {
    fn throughput(key: String, entry: ThroughputChangelogEntry) -> Self {
        Self {
            key,
            entry: ChangelogRecordEntry::Throughput(entry),
            local_mirror_applied: false,
            partition_mirror_applied: false,
        }
    }

    fn streams(key: String, entry: StreamsChangelogEntry) -> Self {
        Self {
            key,
            entry: ChangelogRecordEntry::Streams(entry),
            local_mirror_applied: false,
            partition_mirror_applied: false,
        }
    }

    pub(crate) fn occurred_at(&self) -> DateTime<Utc> {
        match &self.entry {
            ChangelogRecordEntry::Throughput(entry) => entry.occurred_at,
            ChangelogRecordEntry::Streams(entry) => entry.occurred_at,
        }
    }
}

#[derive(Debug, Clone)]
struct AcceptedReportApply {
    report: ThroughputChunkReport,
    identity: ThroughputBatchIdentity,
    projected: ProjectedTerminalApply,
}

#[derive(Debug, Clone, Default)]
struct PreparedReportBatchApply {
    accepted_reports: Vec<AcceptedReportApply>,
    result_segments: Vec<ThroughputResultSegmentRecord>,
}

#[derive(Debug, Clone, Default)]
struct PreparedLeaseBatch {
    leased_tasks: Vec<BulkActivityTask>,
    projection_records: Vec<StreamProjectionRecord>,
    changelog_records: Vec<StreamChangelogRecord>,
    skipped_projection_events: u64,
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
    let config = GrpcServiceConfig::from_env_aliases(
        &["STREAMS_RUNTIME", "THROUGHPUT_RUNTIME"],
        STREAMS_RUNTIME_SERVICE_NAME,
        50053,
    )?;
    let debug_config = HttpServiceConfig::from_env_aliases(
        &["STREAMS_DEBUG", "THROUGHPUT_DEBUG"],
        STREAMS_RUNTIME_DEBUG_SERVICE_NAME,
        3006,
    )?;
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
    let local_state = LocalThroughputState::open_coordinator(
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
    let streams_commands_config = JsonTopicConfig::new(
        redpanda.brokers.clone(),
        redpanda.streams_commands_topic.clone(),
        redpanda.throughput_partitions,
    );
    let changelog_config = JsonTopicConfig::new(
        redpanda.brokers.clone(),
        redpanda.throughput_changelog_topic.clone(),
        redpanda.throughput_partitions,
    );
    let streams_changelog_config = JsonTopicConfig::new(
        redpanda.brokers.clone(),
        redpanda.streams_changelog_topic.clone(),
        redpanda.throughput_partitions,
    );
    let throughput_projections_config = JsonTopicConfig::new(
        redpanda.brokers.clone(),
        redpanda.throughput_projections_topic.clone(),
        redpanda.throughput_partitions,
    );
    let streams_projections_config = JsonTopicConfig::new(
        redpanda.brokers.clone(),
        redpanda.streams_projections_topic.clone(),
        redpanda.throughput_partitions,
    );
    let workflow_publisher = WorkflowPublisher::new(&workflow_broker, "throughput-runtime").await?;
    let throughput_changelog_publisher =
        JsonTopicPublisher::new(&changelog_config, "throughput-runtime-changelog").await?;
    let streams_changelog_publisher =
        JsonTopicPublisher::new(&streams_changelog_config, "throughput-runtime-streams-changelog")
            .await?;
    let throughput_projection_publisher =
        JsonTopicPublisher::new(&throughput_projections_config, "throughput-runtime-projections")
            .await?;
    let streams_projection_publisher = JsonTopicPublisher::new(
        &streams_projections_config,
        "throughput-runtime-streams-projections",
    )
    .await?;

    let bulk_notify = Arc::new(Notify::new());
    let outbox_notify = Arc::new(Notify::new());
    let debug = Arc::new(StdMutex::new(ThroughputDebugState::default()));
    let shard_workers = Arc::new(OnceLock::new());
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
        aggregation: AggregationCoordinator::new(local_state.clone()),
        local_state,
        shard_workers: shard_workers.clone(),
        json_brokers: redpanda.brokers.clone(),
        workflow_publisher,
        throughput_changelog_publisher,
        streams_changelog_publisher,
        throughput_projection_publisher,
        streams_projection_publisher,
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
        #[cfg(test)]
        fail_next_workflow_outbox_writes: Arc::new(AtomicUsize::new(0)),
    };
    {
        let mut runtime_debug = debug.lock().expect("throughput debug lock poisoned");
        runtime_debug.changelog_consumer_enabled = !state.runtime.owner_first_apply;
    }
    let restored_from_checkpoint = restore_state_from_checkpoint(&state).await?;
    if restored_from_checkpoint {
        info!("streams-runtime restored local state from latest checkpoint");
    } else {
        seed_local_state_from_store(&state).await?;
    }
    if state.runtime.owner_first_apply {
        info!("streams-runtime skipping changelog restore in owner-first mode");
    } else {
        restore_local_state_from_changelog(
            &state,
            &changelog_config,
            PublishPlane::Throughput,
            true,
        )
        .await?;
        if streams_changelog_config.topic_name != changelog_config.topic_name {
            restore_local_state_from_changelog(
                &state,
                &streams_changelog_config,
                PublishPlane::Streams,
                true,
            )
            .await?;
        } else {
            info!(
                topic = %changelog_config.topic_name,
                "streams-runtime using shared topic for throughput and streams changelog restore"
            );
        }
    }
    let shard_workers = ShardWorkerPool::new(state.clone(), &managed_partitions);
    let _ = state.shard_workers.set(shard_workers.clone());
    let coordinator_storage = shard_workers.coordinator_storage();
    info!(
        db_path = %coordinator_storage.db_path.display(),
        checkpoint_dir = %coordinator_storage.checkpoint_dir.display(),
        "streams-runtime coordinator RocksDB layout"
    );
    for shard_storage in shard_workers.storage_paths() {
        info!(
            throughput_partition_id = shard_storage.throughput_partition_id,
            db_path = %shard_storage.db_path.display(),
            checkpoint_dir = %shard_storage.checkpoint_dir.display(),
            "streams-runtime planned shard-local RocksDB layout"
        );
    }
    seed_partition_shard_local_states_from_coordinator(&state, &shard_workers)?;
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
        replay_report_log_tail(&state, &shard_workers).await?;
    }

    let command_consumer = build_json_consumer(
        &commands_config,
        "throughput-runtime-throughput-commands",
        &commands_config.all_partition_ids(),
    )
    .await?;
    let consumer = build_workflow_consumer(
        &workflow_broker,
        "throughput-runtime",
        &workflow_broker.all_partition_ids(),
    )
    .await?;
    tokio::spawn(bridge::run_command_loop(
        state.clone(),
        command_consumer,
        bridge::CommandPlane::Throughput,
    ));
    if streams_commands_config.topic_name != commands_config.topic_name {
        let streams_command_consumer = build_json_consumer(
            &streams_commands_config,
            "throughput-runtime-streams-commands",
            &streams_commands_config.all_partition_ids(),
        )
        .await?;
        tokio::spawn(bridge::run_command_loop(
            state.clone(),
            streams_command_consumer,
            bridge::CommandPlane::Streams,
        ));
    } else {
        info!(
            topic = %commands_config.topic_name,
            "streams-runtime using shared topic for throughput and streams commands"
        );
    }
    tokio::spawn(bridge::run_bridge_loop(state.clone(), consumer));
    tokio::spawn(run_active_topic_stream_job_loop(state.clone()));
    if !state.runtime.owner_first_apply {
        tokio::spawn(run_changelog_consumer_loop(
            state.clone(),
            changelog_config.clone(),
            PublishPlane::Throughput,
        ));
        if streams_changelog_config.topic_name != changelog_config.topic_name {
            tokio::spawn(run_changelog_consumer_loop(
                state.clone(),
                streams_changelog_config.clone(),
                PublishPlane::Streams,
            ));
        }
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
    for shard_worker in shard_workers.handles().iter().cloned() {
        tokio::spawn(run_local_benchmark_fastlane_loop(
            shard_worker,
            state.runtime.clone(),
            state.bulk_notify.clone(),
        ));
    }
    let debug_app = default_router::<AppState>(ServiceInfo::new(
        debug_config.name,
        STREAMS_RUNTIME_DEBUG_SERVICE_NAME,
        env!("CARGO_PKG_VERSION"),
    ))
    .route(STREAMS_RUNTIME_DEBUG_ROUTE, get(get_debug_snapshot))
    .route(LEGACY_THROUGHPUT_DEBUG_ROUTE, get(get_debug_snapshot))
    .route(STREAMS_RUNTIME_DEBUG_BATCH_ROUTE, get(get_strong_batch_snapshot))
    .route(LEGACY_THROUGHPUT_DEBUG_BATCH_ROUTE, get(get_strong_batch_snapshot))
    .route(STREAMS_RUNTIME_DEBUG_CHUNKS_ROUTE, get(get_strong_chunk_snapshots))
    .route(LEGACY_THROUGHPUT_DEBUG_CHUNKS_ROUTE, get(get_strong_chunk_snapshots))
    .route(STREAMS_RUNTIME_DEBUG_STREAM_JOB_RUNTIME_ROUTE, get(get_strong_stream_job_runtime_stats))
    .route(
        STREAMS_RUNTIME_DEBUG_STREAM_JOB_VIEW_RUNTIME_ROUTE,
        get(get_strong_stream_job_view_runtime_stats),
    )
    .route(
        STREAMS_RUNTIME_DEBUG_STREAM_JOB_VIEW_PROJECTION_REBUILD_ROUTE,
        post(rebuild_stream_job_view_projections),
    )
    .route(
        STREAMS_RUNTIME_DEBUG_STREAM_JOB_PROJECTION_REBUILD_ROUTE,
        post(rebuild_stream_job_projections),
    )
    .route(
        LEGACY_THROUGHPUT_DEBUG_STREAM_JOB_RUNTIME_ROUTE,
        get(get_strong_stream_job_runtime_stats),
    )
    .route(
        LEGACY_THROUGHPUT_DEBUG_STREAM_JOB_VIEW_RUNTIME_ROUTE,
        get(get_strong_stream_job_view_runtime_stats),
    )
    .route(
        LEGACY_THROUGHPUT_DEBUG_STREAM_JOB_VIEW_PROJECTION_REBUILD_ROUTE,
        post(rebuild_stream_job_view_projections),
    )
    .route(
        LEGACY_THROUGHPUT_DEBUG_STREAM_JOB_PROJECTION_REBUILD_ROUTE,
        post(rebuild_stream_job_projections),
    )
    .route(STREAMS_RUNTIME_DEBUG_STREAM_JOB_VIEW_KEYS_ROUTE, get(get_strong_stream_job_view_keys))
    .route(
        STREAMS_RUNTIME_DEBUG_STREAM_JOB_VIEW_ENTRIES_ROUTE,
        get(get_strong_stream_job_view_entries),
    )
    .route(STREAMS_RUNTIME_DEBUG_STREAM_JOB_VIEW_SCAN_ROUTE, get(get_stream_job_view_scan))
    .route(STREAMS_RUNTIME_DEBUG_STREAM_JOB_VIEW_ROUTE, get(get_strong_stream_job_view))
    .with_state(state.clone());
    tokio::spawn(async move {
        if let Err(error) = serve(debug_app, debug_config.port).await {
            error!(error = %error, "streams-runtime debug server exited");
        }
    });

    let addr = format!("0.0.0.0:{}", config.port).parse()?;
    info!(%addr, "streams-runtime listening");
    Server::builder()
        .add_service(ActivityWorkerApiServer::new(WorkerApi {
            state: state.clone(),
            shard_workers,
        }))
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

async fn get_strong_stream_job_view(
    Path((tenant_id, instance_id, run_id, job_id, view_name, logical_key)): Path<(
        String,
        String,
        String,
        String,
        String,
        String,
    )>,
    AxumState(state): AxumState<AppState>,
) -> Result<Json<StrongStreamJobViewResponse>, (StatusCode, String)> {
    let handle = state
        .store
        .get_stream_job_bridge_handle(&tenant_id, &instance_id, &run_id, &job_id)
        .await
        .map_err(internal_http_error)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("stream job {job_id} not found")))?;
    let local_state = owner_local_state_for_stream_job(&state, &handle.job_id);
    let view = local_state
        .load_stream_job_view_state(&handle.handle_id, &view_name, &logical_key)
        .map_err(internal_http_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("stream job view {view_name} key {logical_key} not found"),
            )
        })?;
    Ok(Json(StrongStreamJobViewResponse {
        handle_id: view.handle_id,
        job_id: view.job_id,
        view_name: view.view_name,
        logical_key: view.logical_key,
        output: view.output,
        checkpoint_sequence: view.checkpoint_sequence,
        updated_at: view.updated_at,
    }))
}

async fn get_strong_stream_job_runtime_stats(
    Path((tenant_id, instance_id, run_id, job_id)): Path<(String, String, String, String)>,
    Query(query): Query<StreamJobRuntimeStatsQuery>,
    AxumState(state): AxumState<AppState>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let handle = state
        .store
        .get_stream_job_bridge_handle(&tenant_id, &instance_id, &run_id, &job_id)
        .await
        .map_err(internal_http_error)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("stream job {job_id} not found")))?;
    let requested_at = Utc::now();
    let output = stream_jobs::build_stream_job_query_output_on_local_state(
        owner_local_state_for_stream_job(&state, &handle.job_id),
        &handle,
        &fabrik_store::StreamJobQueryRecord {
            protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
            workflow_event_id: Uuid::now_v7(),
            tenant_id: tenant_id.clone(),
            instance_id: instance_id.clone(),
            run_id: run_id.clone(),
            job_id: job_id.clone(),
            handle_id: handle.handle_id.clone(),
            bridge_request_id: handle.bridge_request_id.clone(),
            query_id: format!("debug-runtime-{}", Uuid::now_v7()),
            query_name: stream_jobs::STREAM_JOB_RUNTIME_STATS_QUERY_NAME.to_owned(),
            query_args: query.source_partition_id.map(|source_partition_id| {
                json!({
                    "sourcePartitionId": source_partition_id
                })
            }),
            consistency: "strong".to_owned(),
            status: fabrik_throughput::StreamJobQueryStatus::Completed.as_str().to_owned(),
            workflow_owner_epoch: handle.workflow_owner_epoch,
            stream_owner_epoch: handle.stream_owner_epoch,
            output: None,
            error: None,
            requested_at,
            completed_at: None,
            accepted_at: None,
            cancelled_at: None,
            created_at: requested_at,
            updated_at: requested_at,
        },
    )
    .map_err(internal_http_error)?
    .ok_or_else(|| {
        (StatusCode::NOT_FOUND, format!("stream job runtime stats for {job_id} not found"))
    })?;
    Ok(Json(output))
}

async fn get_strong_stream_job_view_runtime_stats(
    Path((tenant_id, instance_id, run_id, job_id, view_name)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    AxumState(state): AxumState<AppState>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let handle = state
        .store
        .get_stream_job_bridge_handle(&tenant_id, &instance_id, &run_id, &job_id)
        .await
        .map_err(internal_http_error)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("stream job {job_id} not found")))?;
    let requested_at = Utc::now();
    let output = stream_jobs::build_stream_job_query_output(
        &state,
        &handle,
        &fabrik_store::StreamJobQueryRecord {
            protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
            workflow_event_id: Uuid::now_v7(),
            tenant_id: tenant_id.clone(),
            instance_id: instance_id.clone(),
            run_id: run_id.clone(),
            job_id: job_id.clone(),
            handle_id: handle.handle_id.clone(),
            bridge_request_id: handle.bridge_request_id.clone(),
            query_id: format!("debug-view-runtime-{}", Uuid::now_v7()),
            query_name: stream_jobs::STREAM_JOB_VIEW_RUNTIME_STATS_QUERY_NAME.to_owned(),
            query_args: Some(json!({
                "viewName": view_name
            })),
            consistency: "strong".to_owned(),
            status: fabrik_throughput::StreamJobQueryStatus::Completed.as_str().to_owned(),
            workflow_owner_epoch: handle.workflow_owner_epoch,
            stream_owner_epoch: handle.stream_owner_epoch,
            output: None,
            error: None,
            requested_at,
            completed_at: None,
            accepted_at: None,
            cancelled_at: None,
            created_at: requested_at,
            updated_at: requested_at,
        },
    )
    .await
    .map_err(internal_http_error)?
    .ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("stream job view runtime stats for {job_id}/{view_name} not found"),
        )
    })?;
    Ok(Json(output))
}

async fn rebuild_stream_job_projections(
    Path((tenant_id, instance_id, run_id, job_id)): Path<(String, String, String, String)>,
    AxumState(state): AxumState<AppState>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let handle = state
        .store
        .get_stream_job_bridge_handle(&tenant_id, &instance_id, &run_id, &job_id)
        .await
        .map_err(internal_http_error)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("stream job {job_id} not found")))?;
    let rebuilt =
        stream_jobs::rebuild_stream_job_eventual_projections(&state, &handle, None, Utc::now())
            .await
            .map_err(internal_http_error)?;
    Ok(Json(rebuilt))
}

async fn rebuild_stream_job_view_projections(
    Path((tenant_id, instance_id, run_id, job_id, view_name)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    AxumState(state): AxumState<AppState>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let handle = state
        .store
        .get_stream_job_bridge_handle(&tenant_id, &instance_id, &run_id, &job_id)
        .await
        .map_err(internal_http_error)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("stream job {job_id} not found")))?;
    let rebuilt = stream_jobs::rebuild_stream_job_eventual_projections(
        &state,
        &handle,
        Some(view_name.as_str()),
        Utc::now(),
    )
    .await
    .map_err(internal_http_error)?;
    Ok(Json(rebuilt))
}

async fn get_strong_stream_job_view_keys(
    Path((tenant_id, instance_id, run_id, job_id, view_name)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    Query(pagination): Query<ChunkPageQuery>,
    AxumState(state): AxumState<AppState>,
) -> Result<Json<StrongStreamJobViewKeysResponse>, (StatusCode, String)> {
    let handle = state
        .store
        .get_stream_job_bridge_handle(&tenant_id, &instance_id, &run_id, &job_id)
        .await
        .map_err(internal_http_error)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("stream job {job_id} not found")))?;
    let mut views = owner_local_state_for_stream_job(&state, &handle.job_id)
        .load_stream_job_views_for_view(&handle.handle_id, &view_name)
        .map_err(internal_http_error)?;
    let logical_key_prefix = pagination
        .logical_key_prefix
        .clone()
        .or_else(|| pagination.prefix.clone())
        .unwrap_or_default();
    if !logical_key_prefix.is_empty() {
        views.retain(|view| view.logical_key.starts_with(&logical_key_prefix));
    }
    views.sort_by(|left, right| left.logical_key.cmp(&right.logical_key));
    let offset = pagination.offset.unwrap_or(0);
    let limit = pagination.limit.unwrap_or(100);
    let total = views.len();
    let keys = views
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|view| StrongStreamJobViewKeyRecord {
            logical_key: view.logical_key,
            checkpoint_sequence: view.checkpoint_sequence,
            updated_at: view.updated_at,
        })
        .collect();
    Ok(Json(StrongStreamJobViewKeysResponse { total, keys }))
}

async fn get_strong_stream_job_view_entries(
    Path((tenant_id, instance_id, run_id, job_id, view_name)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    Query(pagination): Query<ChunkPageQuery>,
    AxumState(state): AxumState<AppState>,
) -> Result<Json<StrongStreamJobViewEntriesResponse>, (StatusCode, String)> {
    let handle = state
        .store
        .get_stream_job_bridge_handle(&tenant_id, &instance_id, &run_id, &job_id)
        .await
        .map_err(internal_http_error)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("stream job {job_id} not found")))?;
    let mut views = owner_local_state_for_stream_job(&state, &handle.job_id)
        .load_stream_job_views_for_view(&handle.handle_id, &view_name)
        .map_err(internal_http_error)?;
    let logical_key_prefix = pagination
        .logical_key_prefix
        .clone()
        .or_else(|| pagination.prefix.clone())
        .unwrap_or_default();
    if !logical_key_prefix.is_empty() {
        views.retain(|view| view.logical_key.starts_with(&logical_key_prefix));
    }
    views.sort_by(|left, right| left.logical_key.cmp(&right.logical_key));
    let offset = pagination.offset.unwrap_or(0);
    let limit = pagination.limit.unwrap_or(100);
    let total = views.len();
    let entries = views
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|view| StrongStreamJobViewEntryRecord {
            logical_key: view.logical_key,
            output: view.output,
            checkpoint_sequence: view.checkpoint_sequence,
            updated_at: view.updated_at,
        })
        .collect();
    Ok(Json(StrongStreamJobViewEntriesResponse { total, entries }))
}

async fn get_stream_job_view_scan(
    Path((tenant_id, instance_id, run_id, job_id, view_name)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    Query(query): Query<StreamJobViewScanQuery>,
    AxumState(state): AxumState<AppState>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let handle = state
        .store
        .get_stream_job_bridge_handle(&tenant_id, &instance_id, &run_id, &job_id)
        .await
        .map_err(internal_http_error)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("stream job {job_id} not found")))?;
    let requested_at = Utc::now();
    let mut query_args = serde_json::Map::new();
    if let Some(prefix) = query.prefix.or(query.key_prefix) {
        query_args.insert("prefix".to_owned(), Value::String(prefix));
    }
    if let Some(limit) = query.limit {
        query_args.insert("limit".to_owned(), Value::from(limit));
    }
    if let Some(offset) = query.offset {
        query_args.insert("offset".to_owned(), Value::from(offset));
    }
    let output = stream_jobs::build_stream_job_query_output(
        &state,
        &handle,
        &fabrik_store::StreamJobQueryRecord {
            protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
            workflow_event_id: Uuid::now_v7(),
            tenant_id: tenant_id.clone(),
            instance_id: instance_id.clone(),
            run_id: run_id.clone(),
            job_id: job_id.clone(),
            handle_id: handle.handle_id.clone(),
            bridge_request_id: handle.bridge_request_id.clone(),
            query_id: format!("debug-scan-{}", Uuid::now_v7()),
            query_name: view_name.clone(),
            query_args: (!query_args.is_empty()).then_some(Value::Object(query_args)),
            consistency: query.consistency.unwrap_or_else(|| "strong".to_owned()),
            status: fabrik_throughput::StreamJobQueryStatus::Completed.as_str().to_owned(),
            workflow_owner_epoch: handle.workflow_owner_epoch,
            stream_owner_epoch: handle.stream_owner_epoch,
            output: None,
            error: None,
            requested_at,
            completed_at: None,
            accepted_at: None,
            cancelled_at: None,
            created_at: requested_at,
            updated_at: requested_at,
        },
    )
    .await
    .map_err(internal_http_error)?
    .ok_or_else(|| (StatusCode::NOT_FOUND, format!("stream job view {view_name} not found")))?;
    Ok(Json(output))
}

async fn get_debug_snapshot(
    AxumState(state): AxumState<AppState>,
) -> Json<ThroughputDebugResponse> {
    let runtime = state.debug.lock().expect("throughput debug lock poisoned").clone();
    let local_state = state.local_state.debug_snapshot().expect("throughput local state snapshot");
    let ownership = throughput_ownership_debug_snapshot(&state);
    Json(ThroughputDebugResponse { runtime, local_state, ownership })
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

async fn restore_local_state_from_changelog(
    state: &AppState,
    config: &JsonTopicConfig,
    plane: PublishPlane,
    stop_on_idle: bool,
) -> Result<()> {
    let partitions = config.all_partition_ids();
    let offsets = state.local_state.next_start_offsets_for_plane(
        match plane {
            PublishPlane::Throughput => crate::local_state::LocalChangelogPlane::Throughput,
            PublishPlane::Streams => crate::local_state::LocalChangelogPlane::Streams,
        },
        &partitions,
    )?;
    let mut consumer = build_json_consumer_from_offsets(
        config,
        match plane {
            PublishPlane::Throughput => "throughput-runtime-changelog-restore",
            PublishPlane::Streams => "throughput-runtime-streams-changelog-restore",
        },
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
        let record = message.context(match plane {
            PublishPlane::Throughput => "failed to read throughput changelog message",
            PublishPlane::Streams => "failed to read streams changelog message",
        })?;
        state.local_state.record_observed_high_watermark_for_plane(
            match plane {
                PublishPlane::Throughput => crate::local_state::LocalChangelogPlane::Throughput,
                PublishPlane::Streams => crate::local_state::LocalChangelogPlane::Streams,
            },
            record.partition_id,
            record.high_watermark,
        );
        let apply_result = match plane {
            PublishPlane::Throughput => {
                let entry: ThroughputChangelogEntry = decode_json_record(&record.record)
                    .context("failed to decode throughput changelog entry")?;
                state.local_state.apply_changelog_entry(
                    record.partition_id,
                    record.record.offset,
                    &entry,
                )
            }
            PublishPlane::Streams => {
                let entry: StreamsChangelogEntry = decode_json_record(&record.record)
                    .context("failed to decode streams changelog entry")?;
                state.local_state.apply_streams_changelog_entry(
                    record.partition_id,
                    record.record.offset,
                    &entry,
                )
            }
        };
        if let Err(error) = apply_result {
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
        if run.command_published_at.is_none() {
            continue;
        }
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

async fn run_changelog_consumer_loop(
    state: AppState,
    config: JsonTopicConfig,
    plane: PublishPlane,
) {
    loop {
        if let Err(error) = restore_local_state_from_changelog(&state, &config, plane, false).await
        {
            state.local_state.record_changelog_apply_failure();
            error!(
                error = %error,
                plane = match plane {
                    PublishPlane::Throughput => "throughput",
                    PublishPlane::Streams => "streams",
                },
                "failed to apply changelog to local state"
            );
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

fn seed_partition_shard_local_states_from_coordinator(
    state: &AppState,
    shard_workers: &ShardWorkerPool,
) -> Result<()> {
    for shard_storage in shard_workers.storage_paths() {
        let checkpoint = state.local_state.snapshot_partition_checkpoint_value(
            shard_storage.throughput_partition_id,
            state.throughput_partitions,
        )?;
        let shard_state = shard_workers
            .local_state_for_partition(shard_storage.throughput_partition_id)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "missing shard-local state for throughput partition {}",
                    shard_storage.throughput_partition_id
                )
            })?;
        let restored = shard_state.restore_from_checkpoint_value_if_empty(checkpoint)?;
        info!(
            throughput_partition_id = shard_storage.throughput_partition_id,
            restored,
            db_path = %shard_storage.db_path.display(),
            checkpoint_dir = %shard_storage.checkpoint_dir.display(),
            "streams-runtime hydrated shard-local RocksDB from coordinator checkpoint"
        );
    }
    Ok(())
}

fn sync_batch_chunks_to_partition_shards(
    state: &AppState,
    batch: &WorkflowBulkBatchRecord,
    chunks: &[WorkflowBulkChunkRecord],
) -> Result<()> {
    let Some(shard_workers) = state.shard_workers.get() else {
        return Ok(());
    };
    shard_workers.sync_batch_chunks(batch, chunks, state.throughput_partitions)
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

async fn replay_report_log_tail(state: &AppState, shard_workers: &ShardWorkerPool) -> Result<()> {
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
            shard_workers.apply_report_batch(reports, false, state.throughput_partitions).await?;
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

pub(crate) fn throughput_partition_for_stream_job(job_id: &str, partition_count: i32) -> i32 {
    throughput_partition_for_batch(job_id, 0, partition_count)
}

pub(crate) fn throughput_partition_for_stream_key(logical_key: &str, partition_count: i32) -> i32 {
    partition_for_key(&throughput_partition_key(logical_key, 0), partition_count)
}

pub(crate) fn owner_local_state_for_stream_job<'a>(
    state: &'a AppState,
    job_id: &str,
) -> &'a LocalThroughputState {
    let partition_id = throughput_partition_for_stream_job(job_id, state.throughput_partitions);
    state
        .shard_workers
        .get()
        .and_then(|workers| workers.local_state_for_partition(partition_id))
        .unwrap_or(&state.local_state)
}

pub(crate) fn owner_local_state_for_stream_key<'a>(
    state: &'a AppState,
    logical_key: &str,
) -> &'a LocalThroughputState {
    let partition_id =
        throughput_partition_for_stream_key(logical_key, state.throughput_partitions);
    state
        .shard_workers
        .get()
        .and_then(|workers| workers.local_state_for_partition(partition_id))
        .unwrap_or(&state.local_state)
}

pub(crate) fn stream_job_owner_epoch(state: &AppState, job_id: &str) -> Option<u64> {
    let partition_id = throughput_partition_for_stream_job(job_id, state.throughput_partitions);
    throughput_partition_owner_epoch(state, partition_id)
}

pub(crate) fn throughput_partition_owner_epoch(state: &AppState, partition_id: i32) -> Option<u64> {
    let ownership = state.ownership.lock().expect("throughput ownership lock poisoned");
    ownership
        .leases
        .get(&partition_id)
        .filter(|lease| lease.active && lease.lease_expires_at > Utc::now())
        .map(|lease| lease.owner_epoch)
}

async fn repair_stale_terminal_projection_batches_in_store(
    store: &WorkflowStore,
    aggregation: &AggregationCoordinator,
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
        let Some(local_batch) = aggregation.batch_snapshot(&identity)? else {
            continue;
        };
        let Some(update) = aggregation.local_terminal_projection_batch_update(&local_batch)? else {
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
        &state.aggregation,
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
    if let Some(snapshot) = state.aggregation.batch_snapshot(&identity)? {
        batch.status = parse_bulk_batch_status(&snapshot.status)?;
        batch.succeeded_items = snapshot.succeeded_items;
        batch.failed_items = snapshot.failed_items;
        batch.cancelled_items = snapshot.cancelled_items;
        batch.error = snapshot.error;
        batch.updated_at = snapshot.updated_at;
        batch.terminal_at = snapshot.terminal_at;
    }
    batch.reducer_output =
        state.aggregation.reducer_output_after_report(&identity, batch.reducer.as_deref(), None)?;
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
                error!(error = %error, "streams-runtime failed to lease workflow outbox");
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
        if let Some(shard_workers) = state.shard_workers.get() {
            match shard_workers.run_expired_chunk_sweep_once(Utc::now(), 10_000).await {
                Ok(_) => continue,
                Err(error) => {
                    error!(error = %error, "streams-runtime shard-local expired chunk sweep failed");
                }
            }
        }
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
                error!(error = %error, "streams-runtime failed to list expired chunks from local state");
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

async fn run_local_benchmark_fastlane_loop(
    shard_worker: ShardWorkerHandle,
    runtime: ThroughputRuntimeConfig,
    bulk_notify: Arc<Notify>,
) {
    let idle_wait = Duration::from_millis(50);
    let batch_limit =
        (runtime.max_active_chunks_per_batch > 0).then_some(runtime.max_active_chunks_per_batch);
    let lease_ttl = chrono::Duration::seconds(runtime.lease_ttl_seconds as i64);
    let max_chunks = runtime
        .report_apply_batch_size
        .max(runtime.poll_max_tasks)
        .max(LOCAL_BENCHMARK_FASTLANE_BATCH_SIZE);
    loop {
        match shard_worker
            .run_local_benchmark_fastlane_once(batch_limit, lease_ttl, max_chunks)
            .await
        {
            Ok(0) => {
                let _ = tokio::time::timeout(idle_wait, bulk_notify.notified()).await;
            }
            Ok(_) => tokio::task::yield_now().await,
            Err(error) => {
                error!(error = %error, "streams-runtime local benchmark fastlane failed");
                let _ = tokio::time::timeout(idle_wait, bulk_notify.notified()).await;
            }
        }
    }
}

impl ShardWorker {
    async fn run_expired_chunk_sweep_once(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        limit: usize,
    ) -> Result<usize> {
        let owned = self.owned_partitions();
        if owned.is_empty() {
            return Ok(0);
        }
        let expired = self.scheduling_state.expired_started_chunks(
            now,
            limit,
            Some(&owned),
            self.state.throughput_partitions,
        )?;
        let mut requeued = 0usize;
        for chunk in expired {
            let projected = match self.scheduling_state.project_requeue(
                &chunk.identity,
                &chunk.chunk_id,
                now,
            )? {
                Some(projected) => projected,
                None => continue,
            };
            let entry = changelog_entry(
                throughput_partition_key(&chunk.identity.batch_id, projected.group_id),
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
            );
            self.state.local_state.mirror_changelog_entry(&entry)?;
            self.mirror_scheduling_changelog_records(std::slice::from_ref(&entry))?;
            if !self.state.runtime.owner_first_apply {
                publish_changelog_records(
                    &self.state,
                    vec![StreamChangelogRecord::throughput(entry.partition_key.clone(), entry)],
                )
                .await;
            }
            self.state.bulk_notify.notify_waiters();
            requeued = requeued.saturating_add(1);
        }
        Ok(requeued)
    }

    async fn run_local_benchmark_fastlane_once(
        &self,
        batch_limit: Option<usize>,
        lease_ttl: chrono::Duration,
        max_chunks: usize,
    ) -> Result<usize> {
        let owned = self.owned_partitions();
        if owned.is_empty() {
            return Ok(0);
        }
        let leased = self.scheduling_state.lease_ready_chunks_matching(
            LOCAL_BENCHMARK_FASTLANE_WORKER_ID,
            Utc::now(),
            lease_ttl,
            batch_limit,
            &HashSet::new(),
            Some(&owned),
            self.state.throughput_partitions,
            max_chunks,
            |chunk| is_local_fastlane_chunk(&self.state, chunk),
        )?;
        if leased.is_empty() {
            return Ok(0);
        }
        let mut reports = Vec::with_capacity(leased.len());
        for chunk in leased {
            reports.push(execute_local_fastlane_chunk(&self.state, chunk).await?);
        }
        {
            let mut debug = self.state.debug.lock().expect("throughput debug lock poisoned");
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
        self.apply_report_batch(&reports, true).await?;
        Ok(reports.len())
    }

    async fn poll_bulk_activity_tasks(
        &self,
        request: PollBulkActivityTaskRequest,
    ) -> Result<PollBulkActivityTaskResponse, Status> {
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
            let owned = self.owned_partitions();
            if owned.is_empty() {
                if !self.wait_for_bulk_work(deadline).await {
                    return Ok(Self::empty_poll_bulk_activity_task_response());
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
                    if !self.wait_for_bulk_work(deadline).await {
                        return Ok(Self::empty_poll_bulk_activity_task_response());
                    }
                    continue;
                }
                if control.is_draining {
                    record_throttle(&self.state, "queue_draining");
                    if !self.wait_for_bulk_work(deadline).await {
                        return Ok(Self::empty_poll_bulk_activity_task_response());
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
                .scheduling_state
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
                if !self.wait_for_bulk_work(deadline).await {
                    return Ok(Self::empty_poll_bulk_activity_task_response());
                }
                continue;
            }
            let queue_active = self
                .scheduling_state
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
                if !self.wait_for_bulk_work(deadline).await {
                    return Ok(Self::empty_poll_bulk_activity_task_response());
                }
                continue;
            }
            if let Some(prepared) = self
                .prepare_leased_task_batch(
                    &request,
                    &owned,
                    &paused_batch_ids,
                    batch_limit,
                    max_tasks,
                )
                .await?
            {
                return Ok(self.publish_leased_task_batch(prepared).await);
            }
            {
                let mut debug = self.state.debug.lock().expect("throughput debug lock poisoned");
                debug.empty_polls = debug.empty_polls.saturating_add(1);
            }
            if tokio::time::Instant::now() >= deadline {
                let lease_selection_debug = self
                    .scheduling_state
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
                return Ok(Self::empty_poll_bulk_activity_task_response());
            }
            if !self.wait_for_bulk_work(deadline).await {
                return Ok(Self::empty_poll_bulk_activity_task_response());
            }
        }
    }

    async fn prepare_leased_task_batch(
        &self,
        request: &PollBulkActivityTaskRequest,
        owned: &HashSet<i32>,
        paused_batch_ids: &HashSet<String>,
        batch_limit: Option<usize>,
        max_tasks: usize,
    ) -> Result<Option<PreparedLeaseBatch>, Status> {
        let now = Utc::now();
        let leased_local = self
            .scheduling_state
            .lease_ready_chunks(
                &request.tenant_id,
                &request.task_queue,
                &request.worker_id,
                now,
                chrono::Duration::seconds(self.state.runtime.lease_ttl_seconds as i64),
                batch_limit,
                paused_batch_ids,
                Some(owned),
                self.state.throughput_partitions,
                max_tasks,
            )
            .map_err(internal_status)?;
        if leased_local.is_empty() {
            return Ok(None);
        }

        let mut prepared = PreparedLeaseBatch {
            leased_tasks: Vec::with_capacity(leased_local.len()),
            projection_records: Vec::with_capacity(max_tasks * 2),
            changelog_records: Vec::with_capacity(max_tasks),
            skipped_projection_events: 0,
        };
        for leased_local in leased_local {
            let leased =
                leased_snapshot_to_projection_chunk(&leased_local, &request.worker_build_id)
                    .map_err(internal_status)?;
            let identity = batch_identity(&leased);
            if let Some(batch) = self
                .scheduling_state
                .project_batch_running(&identity, leased.updated_at)
                .map_err(internal_status)?
            {
                if self.state.runtime.publish_transient_projection_updates {
                    prepared.projection_records.push(running_batch_projection_record(&batch));
                } else {
                    prepared.skipped_projection_events =
                        prepared.skipped_projection_events.saturating_add(1);
                }
            }
            if self.state.runtime.publish_transient_projection_updates {
                prepared.projection_records.push(leased_chunk_projection_record(&leased));
            } else {
                prepared.skipped_projection_events =
                    prepared.skipped_projection_events.saturating_add(1);
            }
            if !self.state.runtime.owner_first_apply {
                prepared.changelog_records.push(changelog_record(
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
            prepared.leased_tasks.push(bulk_chunk_to_proto(
                &leased_local,
                request.supports_cbor,
                self.state.runtime.inline_chunk_input_threshold_bytes,
            ));
        }
        Ok(Some(prepared))
    }

    async fn publish_leased_task_batch(
        &self,
        prepared: PreparedLeaseBatch,
    ) -> PollBulkActivityTaskResponse {
        let PreparedLeaseBatch {
            leased_tasks,
            projection_records,
            changelog_records,
            skipped_projection_events,
        } = prepared;

        if let Err(error) = publish_projection_records(&self.state, projection_records).await {
            error!(error = %error, "failed to sync leased stream-v2 projection state");
        }
        let scheduling_entries = changelog_records
            .iter()
            .filter_map(|record| match &record.entry {
                ChangelogRecordEntry::Throughput(entry) => Some(entry.clone()),
                ChangelogRecordEntry::Streams(_) => None,
            })
            .collect::<Vec<_>>();
        if let Err(error) = self.state.local_state.mirror_changelog_records(&scheduling_entries) {
            self.state.local_state.record_changelog_apply_failure();
            error!(error = %error, "failed to mirror leased stream-v2 coordinator state");
        }
        if let Err(error) = self.mirror_scheduling_changelog_records(&scheduling_entries) {
            error!(error = %error, "failed to mirror leased stream-v2 scheduling state");
        }
        if !self.state.runtime.owner_first_apply {
            publish_changelog_records(&self.state, changelog_records).await;
        }
        {
            let mut debug = self.state.debug.lock().expect("throughput debug lock poisoned");
            debug.projection_events_skipped =
                debug.projection_events_skipped.saturating_add(skipped_projection_events);
            debug.leased_tasks = debug.leased_tasks.saturating_add(leased_tasks.len() as u64);
        }
        PollBulkActivityTaskResponse { tasks: leased_tasks }
    }

    async fn wait_for_bulk_work(&self, deadline: tokio::time::Instant) -> bool {
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        tokio::time::timeout(remaining, self.state.bulk_notify.notified()).await.is_ok()
    }

    fn empty_poll_bulk_activity_task_response() -> PollBulkActivityTaskResponse {
        PollBulkActivityTaskResponse { tasks: Vec::new() }
    }

    async fn apply_report_batch(
        &self,
        report_batch: &[ThroughputChunkReport],
        capture_report_log: bool,
    ) -> Result<()> {
        let prepared = self.prepare_report_batch(report_batch).await?;
        if !prepared.result_segments.is_empty() {
            self.state.store.upsert_throughput_result_segments(&prepared.result_segments).await?;
        }
        if capture_report_log && self.state.runtime.owner_first_apply {
            let captured = prepared
                .accepted_reports
                .iter()
                .map(|accepted| accepted.report.clone())
                .collect::<Vec<_>>();
            self.state.store.append_throughput_report_log_entries(&captured, Utc::now()).await?;
        }
        self.publish_applied_report_batch(prepared.accepted_reports).await
    }

    async fn prepare_report_batch(
        &self,
        report_batch: &[ThroughputChunkReport],
    ) -> Result<PreparedReportBatchApply> {
        let mut prepared = PreparedReportBatchApply {
            accepted_reports: Vec::with_capacity(report_batch.len()),
            result_segments: Vec::new(),
        };
        for report in report_batch {
            let throughput_partition_id = throughput_partition_for_batch(
                &report.batch_id,
                report.group_id,
                self.state.throughput_partitions,
            );
            if !owns_throughput_partition(&self.state, throughput_partition_id) {
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
            let validated_chunk = match self.scheduling_state.prepare_report_validation(report)? {
                PreparedValidatedReport::MissingBatch => {
                    anyhow::bail!("bulk batch {} is not owned by this runtime", report.batch_id);
                }
                PreparedValidatedReport::Rejected(validation) => {
                    let mut debug =
                        self.state.debug.lock().expect("throughput debug lock poisoned");
                    debug.reports_rejected = debug.reports_rejected.saturating_add(1);
                    debug.last_report_rejection_reason =
                        Some(report_validation_label(validation).to_owned());
                    continue;
                }
                PreparedValidatedReport::Accepted(validated_chunk) => validated_chunk,
            };
            let Some(projected_chunk) = self
                .scheduling_state
                .project_chunk_apply_after_validation(report, &validated_chunk)?
            else {
                let mut debug = self.state.debug.lock().expect("throughput debug lock poisoned");
                debug.apply_divergences = debug.apply_divergences.saturating_add(1);
                debug.last_apply_divergence = Some(format!(
                    "accepted report {} for batch {} chunk {} missing shard-local chunk projection state",
                    report.report_id, report.batch_id, report.chunk_id
                ));
                anyhow::bail!(
                    "accepted throughput report {} for batch {} chunk {} missing shard-local state",
                    report.report_id,
                    report.batch_id,
                    report.chunk_id
                );
            };
            let Some(projected_rollup) = self
                .state
                .aggregation
                .project_batch_rollup_after_chunk_apply(&identity, report, &projected_chunk)?
            else {
                let mut debug = self.state.debug.lock().expect("throughput debug lock poisoned");
                debug.apply_divergences = debug.apply_divergences.saturating_add(1);
                debug.last_apply_divergence = Some(format!(
                    "accepted report {} for batch {} chunk {} missing coordinator rollup state",
                    report.report_id, report.batch_id, report.chunk_id
                ));
                anyhow::bail!(
                    "accepted throughput report {} for batch {} chunk {} missing coordinator rollup state",
                    report.report_id,
                    report.batch_id,
                    report.chunk_id
                );
            };
            let projected = ProjectedTerminalApply::from_parts(projected_chunk, projected_rollup);
            let (report, segment) = materialize_collect_results_segment(
                &self.state,
                report,
                projected.batch_reducer.as_deref(),
                projected.chunk_item_count,
            )
            .await?;
            if let Some(segment) = segment {
                prepared.result_segments.push(segment);
            }
            prepared.accepted_reports.push(AcceptedReportApply { report, identity, projected });
        }
        Ok(prepared)
    }

    async fn publish_applied_report_batch(
        &self,
        accepted_reports: Vec<AcceptedReportApply>,
    ) -> Result<()> {
        let mut terminal_handoff_candidates = BTreeSet::new();
        let mut projection_records = Vec::with_capacity(accepted_reports.len() * 2);
        let mut skipped_projection_events = 0_u64;

        for accepted in accepted_reports {
            let report = &accepted.report;
            let identity = &accepted.identity;
            let projected = &accepted.projected;
            let terminal_artifacts = materialize_chunk_terminal_artifacts(
                &self.state,
                report,
                projected.batch_reducer.as_deref(),
            )
            .await?;
            let batch_reducer_output = reduce_stream_batch_output(
                &self.state,
                identity,
                projected.batch_reducer.as_deref(),
                Some(report),
            )?;
            let chunk_output = completed_chunk_output(report);
            projection_records.extend(projected_apply_projection_records(
                report,
                projected,
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
                record_group_terminal_published(&self.state, group_terminal);
                changelog_records.push(group_terminal_changelog_record(identity, group_terminal));
                for parent_group in &projected.parent_group_terminals {
                    record_group_terminal_published(&self.state, parent_group);
                    changelog_records.push(group_terminal_changelog_record(identity, parent_group));
                }
                if let Some(batch_terminal) = projected.grouped_batch_terminal.as_ref() {
                    projection_records.push(grouped_batch_terminal_projection_record(
                        identity,
                        batch_terminal,
                        batch_reducer_output.clone(),
                    ));
                    changelog_records
                        .push(grouped_batch_terminal_changelog_record(identity, batch_terminal));
                } else if let Some(summary) = projected.grouped_batch_summary.as_ref() {
                    if self.state.runtime.publish_transient_projection_updates {
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
                    .filter_map(|record| match &record.entry {
                        ChangelogRecordEntry::Throughput(entry) => Some(entry.clone()),
                        ChangelogRecordEntry::Streams(_) => None,
                    })
                    .collect::<Vec<_>>();
                self.state.local_state.mirror_changelog_records(&staged_entries)?;
            }
            let scheduling_entries = changelog_records
                .iter()
                .filter_map(|record| match &record.entry {
                    ChangelogRecordEntry::Throughput(entry) => Some(entry.clone()),
                    ChangelogRecordEntry::Streams(_) => None,
                })
                .collect::<Vec<_>>();
            if let Err(error) = self.mirror_scheduling_changelog_records(&scheduling_entries) {
                error!(error = %error, "failed to mirror applied stream-v2 scheduling state");
            }
            if !self.state.runtime.owner_first_apply {
                publish_changelog_records(&self.state, changelog_records).await;
            }
            if let Some(batch_terminal) = projected.grouped_batch_terminal.as_ref() {
                publish_grouped_batch_terminal_event(
                    &self.state,
                    report,
                    projected,
                    batch_terminal,
                )
                .await?;
                let mut debug = self.state.debug.lock().expect("throughput debug lock poisoned");
                debug.terminal_events_published = debug.terminal_events_published.saturating_add(1);
                debug.last_terminal_event_at = Some(Utc::now());
            }
            if projected.batch_terminal {
                publish_stream_terminal_event(&self.state, report, projected).await?;
                let mut debug = self.state.debug.lock().expect("throughput debug lock poisoned");
                debug.terminal_events_published = debug.terminal_events_published.saturating_add(1);
                debug.last_terminal_event_at = Some(Utc::now());
            }
            if projected.chunk_status == WorkflowBulkChunkStatus::Scheduled.as_str()
                || projected.batch_terminal_deferred
            {
                self.state.bulk_notify.notify_waiters();
            }
            terminal_handoff_candidates.insert((
                identity.tenant_id.clone(),
                identity.instance_id.clone(),
                identity.run_id.clone(),
                identity.batch_id.clone(),
            ));
        }

        publish_projection_records(&self.state, projection_records).await?;
        for (tenant_id, instance_id, run_id, batch_id) in terminal_handoff_candidates {
            if publish_projection_batch_terminal_handoff(
                &self.state,
                &tenant_id,
                &instance_id,
                &run_id,
                &batch_id,
            )
            .await?
            {
                let mut debug = self.state.debug.lock().expect("throughput debug lock poisoned");
                debug.terminal_events_published = debug.terminal_events_published.saturating_add(1);
                debug.last_terminal_event_at = Some(Utc::now());
            }
        }
        if skipped_projection_events > 0 {
            let mut debug = self.state.debug.lock().expect("throughput debug lock poisoned");
            debug.projection_events_skipped =
                debug.projection_events_skipped.saturating_add(skipped_projection_events);
        }
        Ok(())
    }
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
    let Some(bootstrap_path) = state.activity_executor_registry.lookup(task_queue, activity_type)
    else {
        anyhow::bail!(
            "activity {activity_type} has no local fastlane executor for task queue {task_queue}"
        );
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
        let stdin =
            child.stdin.as_mut().context("packaged activity bootstrap stdin unavailable")?;
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
                .aggregation
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
                    .aggregation
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

async fn run_active_topic_stream_job_loop(state: AppState) {
    let interval = Duration::from_millis(state.runtime.sweep_interval_ms.max(50));
    loop {
        if let Err(error) = bridge::reconcile_active_topic_stream_jobs(&state, 64).await {
            error!(error = %error, "failed to reconcile active topic-backed stream jobs");
        }
        tokio::time::sleep(interval).await;
    }
}

#[tonic::async_trait]
impl ActivityWorkerApi for WorkerApi {
    async fn poll_activity_task(
        &self,
        _request: Request<fabrik_worker_protocol::activity_worker::PollActivityTaskRequest>,
    ) -> Result<Response<fabrik_worker_protocol::activity_worker::PollActivityTaskResponse>, Status>
    {
        Err(Status::unimplemented("streams-runtime only serves bulk tasks"))
    }

    async fn poll_activity_tasks(
        &self,
        _request: Request<fabrik_worker_protocol::activity_worker::PollActivityTasksRequest>,
    ) -> Result<Response<fabrik_worker_protocol::activity_worker::PollActivityTasksResponse>, Status>
    {
        Err(Status::unimplemented("streams-runtime only serves bulk tasks"))
    }

    async fn poll_bulk_activity_task(
        &self,
        request: Request<PollBulkActivityTaskRequest>,
    ) -> Result<Response<PollBulkActivityTaskResponse>, Status> {
        Ok(Response::new(self.shard_workers.poll_bulk_activity_tasks(request.into_inner()).await?))
    }

    async fn complete_activity_task(
        &self,
        _request: Request<fabrik_worker_protocol::activity_worker::CompleteActivityTaskRequest>,
    ) -> Result<Response<Ack>, Status> {
        Err(Status::unimplemented("streams-runtime only serves bulk tasks"))
    }

    async fn fail_activity_task(
        &self,
        _request: Request<fabrik_worker_protocol::activity_worker::FailActivityTaskRequest>,
    ) -> Result<Response<Ack>, Status> {
        Err(Status::unimplemented("streams-runtime only serves bulk tasks"))
    }

    async fn record_activity_heartbeat(
        &self,
        _request: Request<fabrik_worker_protocol::activity_worker::RecordActivityHeartbeatRequest>,
    ) -> Result<
        Response<fabrik_worker_protocol::activity_worker::RecordActivityHeartbeatResponse>,
        Status,
    > {
        Err(Status::unimplemented("streams-runtime only serves bulk tasks"))
    }

    async fn report_activity_task_cancelled(
        &self,
        _request: Request<
            fabrik_worker_protocol::activity_worker::ReportActivityTaskCancelledRequest,
        >,
    ) -> Result<Response<Ack>, Status> {
        Err(Status::unimplemented("streams-runtime only serves bulk tasks"))
    }

    async fn report_activity_task_results(
        &self,
        _request: Request<
            fabrik_worker_protocol::activity_worker::ReportActivityTaskResultsRequest,
        >,
    ) -> Result<Response<Ack>, Status> {
        Err(Status::unimplemented("streams-runtime only serves bulk tasks"))
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
            self.shard_workers
                .apply_report_batch(report_batch.to_vec(), true, self.state.throughput_partitions)
                .await
                .map_err(internal_status)?;
        }
        Ok(Response::new(Ack {}))
    }
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
    if let Err(error) = sync_batch_chunks_to_partition_shards(state, &batch, &chunks) {
        error!(
            error = %error,
            batch_id = %batch_id,
            "failed to seed shard-local stream-v2 batch state"
        );
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
    if state.aggregation.batch_snapshot(&identity)?.is_some() {
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
    if let Err(error) = sync_batch_chunks_to_partition_shards(state, &batch, &chunks) {
        error!(
            error = %error,
            batch_id = %command.batch_id,
            "failed to seed native shard-local stream-v2 batch state"
        );
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
    state.aggregation.reducer_output_after_report(identity, reducer, pending_report)
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
    let dedupe_key = throughput_terminal_callback_dedupe_key(
        &report.tenant_id,
        &report.instance_id,
        &report.run_id,
        &report.batch_id,
        projected.batch_status.as_str(),
    );
    envelope.event_id = throughput_terminal_callback_event_id(&dedupe_key);
    envelope.occurred_at = terminal_at;
    envelope.dedupe_key = Some(dedupe_key);
    envelope.metadata.insert(
        "bridge_request_id".to_owned(),
        throughput_bridge_request_id(
            &report.tenant_id,
            &report.instance_id,
            &report.run_id,
            &report.batch_id,
        ),
    );
    envelope.metadata.insert(
        "bridge_terminal_dedupe_key".to_owned(),
        envelope.dedupe_key.clone().unwrap_or_default(),
    );
    envelope.metadata.insert(
        "bridge_protocol_version".to_owned(),
        THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
    );
    envelope.metadata.insert(
        "bridge_operation_kind".to_owned(),
        ThroughputBridgeOperationKind::BulkRun.as_str().to_owned(),
    );
    envelope.metadata.insert("bridge_owner_epoch".to_owned(), report.owner_epoch.to_string());
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
    let dedupe_key = throughput_terminal_callback_dedupe_key(
        &batch.tenant_id,
        &batch.instance_id,
        &batch.run_id,
        &batch.batch_id,
        batch.status.as_str(),
    );
    envelope.event_id = throughput_terminal_callback_event_id(&dedupe_key);
    envelope.occurred_at = terminal_at;
    envelope.dedupe_key = Some(dedupe_key);
    envelope.metadata.insert(
        "bridge_request_id".to_owned(),
        throughput_bridge_request_id(
            &batch.tenant_id,
            &batch.instance_id,
            &batch.run_id,
            &batch.batch_id,
        ),
    );
    envelope.metadata.insert(
        "bridge_terminal_dedupe_key".to_owned(),
        envelope.dedupe_key.clone().unwrap_or_default(),
    );
    envelope.metadata.insert(
        "bridge_protocol_version".to_owned(),
        THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
    );
    envelope.metadata.insert(
        "bridge_operation_kind".to_owned(),
        ThroughputBridgeOperationKind::BulkRun.as_str().to_owned(),
    );
    envelope.metadata.insert(
        "bridge_owner_epoch".to_owned(),
        resolve_terminal_bridge_owner_epoch(state, batch, pending_report).await?.to_string(),
    );
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

async fn resolve_terminal_bridge_owner_epoch(
    state: &AppState,
    batch: &WorkflowBulkBatchRecord,
    pending_report: Option<&ThroughputChunkReport>,
) -> Result<u64> {
    if let Some(report) = pending_report {
        return Ok(report.owner_epoch);
    }
    Ok(state
        .store
        .get_throughput_projection_batch_max_owner_epoch(
            &batch.tenant_id,
            &batch.instance_id,
            &batch.run_id,
            &batch.batch_id,
        )
        .await?
        .unwrap_or_default())
}

async fn finalize_grouped_stream_batch(
    state: &AppState,
    identity: &ThroughputBatchIdentity,
    projected: &ProjectedBatchTerminal,
) -> Result<()> {
    let Some(existing_batch) = state.aggregation.batch_snapshot(identity)? else {
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
        grouped_batch_terminal_projection_event(
            identity,
            projected,
            reduce_stream_batch_output(state, identity, existing_batch.reducer.as_deref(), None)?,
        ),
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
    let plane = changelog_plane_for_entry(&entry);
    let publisher = match plane {
        PublishPlane::Throughput => &state.throughput_changelog_publisher,
        PublishPlane::Streams => {
            let Some(streams_entry) = streams_changelog_entry_from_throughput(&entry) else {
                return;
            };
            if let Err(error) =
                state.streams_changelog_publisher.publish(&streams_entry, &key).await
            {
                error!(error = %error, "failed to publish streams changelog entry");
                return;
            }
            if let Err(error) = state.local_state.mirror_streams_changelog_entry(&streams_entry) {
                state.local_state.record_changelog_apply_failure();
                error!(error = %error, "failed to mirror streams changelog entry into local state");
                return;
            }
            let partition_id = partition_for_key(&key, state.throughput_partitions);
            if let Some(local_state) = state
                .shard_workers
                .get()
                .and_then(|workers| workers.local_state_for_partition(partition_id))
            {
                if let Err(error) = local_state.mirror_streams_changelog_entry(&streams_entry) {
                    local_state.record_changelog_apply_failure();
                    error!(
                        error = %error,
                        partition_id,
                        "failed to mirror streams changelog entry into shard-local state"
                    );
                    return;
                }
            }
            let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
            debug.changelog_entries_published = debug.changelog_entries_published.saturating_add(1);
            debug.streams_changelog_entries_published =
                debug.streams_changelog_entries_published.saturating_add(1);
            return;
        }
    };
    if let Err(error) = publisher.publish(&entry, &key).await {
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
    match plane {
        PublishPlane::Throughput => {
            debug.throughput_changelog_entries_published =
                debug.throughput_changelog_entries_published.saturating_add(1);
        }
        PublishPlane::Streams => {
            debug.streams_changelog_entries_published =
                debug.streams_changelog_entries_published.saturating_add(1);
        }
    }
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
    let plane = projection_plane_for_event(&payload);
    match plane {
        PublishPlane::Throughput => {
            state
                .throughput_projection_publisher
                .publish(&payload, &key)
                .await
                .context("failed to publish throughput projection event")?;
        }
        PublishPlane::Streams => {
            let Some(streams_event) = streams_projection_event_from_throughput(&payload) else {
                return Ok(());
            };
            state
                .streams_projection_publisher
                .publish(&streams_event, &key)
                .await
                .context("failed to publish streams projection event")?;
        }
    }
    let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
    debug.projection_events_published = debug.projection_events_published.saturating_add(1);
    match plane {
        PublishPlane::Throughput => {
            debug.throughput_projection_events_published =
                debug.throughput_projection_events_published.saturating_add(1);
        }
        PublishPlane::Streams => {
            debug.streams_projection_events_published =
                debug.streams_projection_events_published.saturating_add(1);
        }
    }
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
    StreamChangelogRecord::throughput(key, entry)
}

fn projection_plane_for_event(event: &ThroughputProjectionEvent) -> PublishPlane {
    match event {
        ThroughputProjectionEvent::UpsertStreamJobView { .. }
        | ThroughputProjectionEvent::DeleteStreamJobView { .. } => PublishPlane::Streams,
        _ => PublishPlane::Throughput,
    }
}

fn changelog_plane_for_entry(entry: &ThroughputChangelogEntry) -> PublishPlane {
    match entry.payload {
        ThroughputChangelogPayload::StreamJobExecutionPlanned { .. }
        | ThroughputChangelogPayload::StreamJobViewUpdated { .. }
        | ThroughputChangelogPayload::StreamJobViewEvicted { .. }
        | ThroughputChangelogPayload::StreamJobCheckpointReached { .. }
        | ThroughputChangelogPayload::StreamJobSourceLeaseAssigned { .. }
        | ThroughputChangelogPayload::StreamJobSourceProgressed { .. }
        | ThroughputChangelogPayload::StreamJobTerminalized { .. } => PublishPlane::Streams,
        _ => PublishPlane::Throughput,
    }
}

fn streams_projection_event_from_throughput(
    event: &ThroughputProjectionEvent,
) -> Option<StreamsProjectionEvent> {
    match event {
        ThroughputProjectionEvent::UpsertStreamJobView { view } => {
            Some(StreamsProjectionEvent::UpsertStreamJobView {
                view: StreamsViewRecord {
                    tenant_id: view.tenant_id.clone(),
                    instance_id: view.instance_id.clone(),
                    run_id: view.run_id.clone(),
                    job_id: view.job_id.clone(),
                    handle_id: view.handle_id.clone(),
                    view_name: view.view_name.clone(),
                    logical_key: view.logical_key.clone(),
                    output: view.output.clone(),
                    checkpoint_sequence: view.checkpoint_sequence,
                    updated_at: view.updated_at,
                },
            })
        }
        ThroughputProjectionEvent::DeleteStreamJobView { view } => {
            Some(StreamsProjectionEvent::DeleteStreamJobView {
                view: StreamsViewDeleteRecord {
                    tenant_id: view.tenant_id.clone(),
                    instance_id: view.instance_id.clone(),
                    run_id: view.run_id.clone(),
                    job_id: view.job_id.clone(),
                    handle_id: view.handle_id.clone(),
                    view_name: view.view_name.clone(),
                    logical_key: view.logical_key.clone(),
                    checkpoint_sequence: view.checkpoint_sequence,
                    evicted_at: view.evicted_at,
                },
            })
        }
        _ => None,
    }
}

pub(crate) fn streams_changelog_entry_from_throughput(
    entry: &ThroughputChangelogEntry,
) -> Option<StreamsChangelogEntry> {
    match &entry.payload {
        ThroughputChangelogPayload::StreamJobExecutionPlanned {
            handle_id,
            job_id,
            job_name,
            view_name,
            checkpoint_name,
            checkpoint_sequence,
            input_item_count,
            materialized_key_count,
            active_partitions,
            owner_epoch,
            planned_at,
        } => Some(StreamsChangelogEntry {
            entry_id: entry.entry_id,
            occurred_at: entry.occurred_at,
            partition_key: entry.partition_key.clone(),
            payload: StreamsChangelogPayload::StreamJobExecutionPlanned {
                handle_id: handle_id.clone(),
                job_id: job_id.clone(),
                job_name: job_name.clone(),
                view_name: view_name.clone(),
                checkpoint_name: checkpoint_name.clone(),
                checkpoint_sequence: *checkpoint_sequence,
                input_item_count: *input_item_count,
                materialized_key_count: *materialized_key_count,
                active_partitions: active_partitions.clone(),
                owner_epoch: *owner_epoch,
                planned_at: *planned_at,
            },
        }),
        ThroughputChangelogPayload::StreamJobViewUpdated {
            handle_id,
            job_id,
            view_name,
            logical_key,
            output,
            checkpoint_sequence,
            updated_at,
        } => Some(StreamsChangelogEntry {
            entry_id: entry.entry_id,
            occurred_at: entry.occurred_at,
            partition_key: entry.partition_key.clone(),
            payload: StreamsChangelogPayload::StreamJobViewUpdated {
                handle_id: handle_id.clone(),
                job_id: job_id.clone(),
                view_name: view_name.clone(),
                logical_key: logical_key.clone(),
                output: output.clone(),
                checkpoint_sequence: *checkpoint_sequence,
                updated_at: *updated_at,
            },
        }),
        ThroughputChangelogPayload::StreamJobViewEvicted {
            handle_id,
            job_id,
            view_name,
            logical_key,
            checkpoint_sequence,
            window_end,
            retention_seconds,
            evicted_window_count,
            last_evicted_window_end,
            last_evicted_at,
            evicted_at,
        } => Some(StreamsChangelogEntry {
            entry_id: entry.entry_id,
            occurred_at: entry.occurred_at,
            partition_key: entry.partition_key.clone(),
            payload: StreamsChangelogPayload::StreamJobViewEvicted {
                handle_id: handle_id.clone(),
                job_id: job_id.clone(),
                view_name: view_name.clone(),
                logical_key: logical_key.clone(),
                checkpoint_sequence: *checkpoint_sequence,
                window_end: *window_end,
                retention_seconds: *retention_seconds,
                evicted_window_count: *evicted_window_count,
                last_evicted_window_end: *last_evicted_window_end,
                last_evicted_at: *last_evicted_at,
                evicted_at: *evicted_at,
            },
        }),
        ThroughputChangelogPayload::StreamJobCheckpointReached {
            handle_id,
            job_id,
            checkpoint_name,
            checkpoint_sequence,
            stream_partition_id,
            owner_epoch,
            reached_at,
        } => Some(StreamsChangelogEntry {
            entry_id: entry.entry_id,
            occurred_at: entry.occurred_at,
            partition_key: entry.partition_key.clone(),
            payload: StreamsChangelogPayload::StreamJobCheckpointReached {
                handle_id: handle_id.clone(),
                job_id: job_id.clone(),
                checkpoint_name: checkpoint_name.clone(),
                checkpoint_sequence: *checkpoint_sequence,
                stream_partition_id: *stream_partition_id,
                owner_epoch: *owner_epoch,
                reached_at: *reached_at,
            },
        }),
        ThroughputChangelogPayload::StreamJobSourceLeaseAssigned {
            handle_id,
            job_id,
            source_partition_id,
            owner_partition_id,
            owner_epoch,
            lease_token,
            assigned_at,
        } => Some(StreamsChangelogEntry {
            entry_id: entry.entry_id,
            occurred_at: entry.occurred_at,
            partition_key: entry.partition_key.clone(),
            payload: StreamsChangelogPayload::StreamJobSourceLeaseAssigned {
                handle_id: handle_id.clone(),
                job_id: job_id.clone(),
                source_partition_id: *source_partition_id,
                owner_partition_id: *owner_partition_id,
                owner_epoch: *owner_epoch,
                lease_token: lease_token.clone(),
                assigned_at: *assigned_at,
            },
        }),
        ThroughputChangelogPayload::StreamJobSourceProgressed {
            handle_id,
            job_id,
            source_partition_id,
            next_offset,
            checkpoint_sequence,
            checkpoint_target_offset,
            last_applied_offset,
            last_high_watermark,
            last_event_time_watermark,
            last_closed_window_end,
            pending_window_ends,
            dropped_late_event_count,
            last_dropped_late_offset,
            last_dropped_late_event_at,
            last_dropped_late_window_end,
            source_owner_partition_id,
            lease_token,
            owner_epoch,
            progressed_at,
            ..
        } => Some(StreamsChangelogEntry {
            entry_id: entry.entry_id,
            occurred_at: entry.occurred_at,
            partition_key: entry.partition_key.clone(),
            payload: StreamsChangelogPayload::StreamJobSourceProgressed {
                handle_id: handle_id.clone(),
                job_id: job_id.clone(),
                source_partition_id: *source_partition_id,
                next_offset: *next_offset,
                checkpoint_sequence: *checkpoint_sequence,
                checkpoint_target_offset: *checkpoint_target_offset,
                last_applied_offset: *last_applied_offset,
                last_high_watermark: *last_high_watermark,
                last_event_time_watermark: *last_event_time_watermark,
                last_closed_window_end: *last_closed_window_end,
                pending_window_ends: pending_window_ends.clone(),
                dropped_late_event_count: *dropped_late_event_count,
                last_dropped_late_offset: *last_dropped_late_offset,
                last_dropped_late_event_at: *last_dropped_late_event_at,
                last_dropped_late_window_end: *last_dropped_late_window_end,
                dropped_evicted_window_event_count: 0,
                last_dropped_evicted_window_offset: None,
                last_dropped_evicted_window_event_at: None,
                last_dropped_evicted_window_end: None,
                source_owner_partition_id: *source_owner_partition_id,
                lease_token: lease_token.clone(),
                owner_epoch: *owner_epoch,
                progressed_at: *progressed_at,
            },
        }),
        ThroughputChangelogPayload::StreamJobTerminalized {
            handle_id,
            job_id,
            owner_epoch,
            status,
            output,
            error,
            terminal_at,
        } => Some(StreamsChangelogEntry {
            entry_id: entry.entry_id,
            occurred_at: entry.occurred_at,
            partition_key: entry.partition_key.clone(),
            payload: StreamsChangelogPayload::StreamJobTerminalized {
                handle_id: handle_id.clone(),
                job_id: job_id.clone(),
                owner_epoch: *owner_epoch,
                status: status.clone(),
                output: output.clone(),
                error: error.clone(),
                terminal_at: *terminal_at,
            },
        }),
        _ => None,
    }
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
    StreamChangelogRecord::throughput(entry.partition_key.clone(), entry)
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
    StreamProjectionRecord::throughput(
        throughput_partition_key(&identity.batch_id, 0),
        grouped_batch_terminal_projection_event(identity, projected, reducer_output),
    )
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
    StreamChangelogRecord::throughput(entry.partition_key.clone(), entry)
}

async fn publish_changelog_records(state: &AppState, records: Vec<StreamChangelogRecord>) {
    if records.is_empty() {
        return;
    }

    for batch in records.chunks(state.runtime.changelog_publish_batch_size.max(1)) {
        let mut throughput_payloads = Vec::new();
        let mut streams_payloads = Vec::new();
        let mut throughput_count = 0u64;
        let mut streams_count = 0u64;

        for record in batch {
            match &record.entry {
                ChangelogRecordEntry::Throughput(entry) => {
                    throughput_payloads.push((record.key.clone(), entry.clone()));
                    throughput_count = throughput_count.saturating_add(1);
                }
                ChangelogRecordEntry::Streams(entry) => {
                    streams_payloads.push((
                        record.key.clone(),
                        entry.clone(),
                        record.local_mirror_applied,
                        record.partition_mirror_applied,
                    ));
                    streams_count = streams_count.saturating_add(1);
                }
            }
        }

        if !throughput_payloads.is_empty() {
            if let Err(error) =
                state.throughput_changelog_publisher.publish_all(throughput_payloads.clone()).await
            {
                error!(error = %error, "failed to publish throughput changelog entries");
                return;
            }
            for (_key, entry) in throughput_payloads {
                if let Err(error) = state.local_state.mirror_changelog_entry(&entry) {
                    state.local_state.record_changelog_apply_failure();
                    error!(error = %error, "failed to mirror throughput changelog entry into local state");
                    return;
                }
            }
        }

        if !streams_payloads.is_empty() {
            let publish_payloads = streams_payloads
                .iter()
                .map(|(key, entry, _, _)| (key.clone(), entry.clone()))
                .collect::<Vec<_>>();
            if let Err(error) =
                state.streams_changelog_publisher.publish_all(publish_payloads).await
            {
                error!(error = %error, "failed to publish streams changelog entries");
                return;
            }
            for (_key, entry, local_mirror_applied, partition_mirror_applied) in streams_payloads {
                if !local_mirror_applied {
                    if let Err(error) = state.local_state.mirror_streams_changelog_entry(&entry) {
                        state.local_state.record_changelog_apply_failure();
                        error!(error = %error, "failed to mirror streams changelog entry into local state");
                        return;
                    }
                }
                if partition_mirror_applied {
                    continue;
                }
                let partition_id =
                    partition_for_key(&entry.partition_key, state.throughput_partitions);
                if let Some(local_state) = state
                    .shard_workers
                    .get()
                    .and_then(|workers| workers.local_state_for_partition(partition_id))
                {
                    if let Err(error) = local_state.mirror_streams_changelog_entry(&entry) {
                        local_state.record_changelog_apply_failure();
                        error!(
                            error = %error,
                            partition_id,
                            "failed to mirror streams changelog entry into shard-local state"
                        );
                        return;
                    }
                }
            }
        }

        let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
        debug.changelog_entries_published = debug
            .changelog_entries_published
            .saturating_add(throughput_count.saturating_add(streams_count));
        debug.throughput_changelog_entries_published =
            debug.throughput_changelog_entries_published.saturating_add(throughput_count);
        debug.streams_changelog_entries_published =
            debug.streams_changelog_entries_published.saturating_add(streams_count);
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
        let mut throughput_payloads = Vec::new();
        let mut streams_payloads = Vec::new();
        for record in batch {
            match &record.event {
                ProjectionRecordEvent::Throughput(event) => {
                    throughput_payloads.push((record.key.clone(), event.clone()));
                }
                ProjectionRecordEvent::Streams(event) => {
                    streams_payloads.push((record.key.clone(), event.clone()));
                }
            }
        }
        let throughput_count = throughput_payloads.len() as u64;
        let streams_count = streams_payloads.len() as u64;
        if !throughput_payloads.is_empty() {
            state
                .throughput_projection_publisher
                .publish_all(throughput_payloads)
                .await
                .context("failed to publish throughput projection events")?;
        }
        if !streams_payloads.is_empty() {
            state
                .streams_projection_publisher
                .publish_all(streams_payloads)
                .await
                .context("failed to publish streams projection events")?;
        }
        let mut debug = state.debug.lock().expect("throughput debug lock poisoned");
        debug.projection_events_published =
            debug.projection_events_published.saturating_add(batch.len() as u64);
        debug.throughput_projection_events_published =
            debug.throughput_projection_events_published.saturating_add(throughput_count);
        debug.streams_projection_events_published =
            debug.streams_projection_events_published.saturating_add(streams_count);
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
        ThroughputProjectionEvent::UpsertStreamJobView { view } => {
            state.store.upsert_stream_job_view_query(view).await?
        }
        ThroughputProjectionEvent::DeleteStreamJobView { view } => {
            state.store.delete_stream_job_view_query(view).await?
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
    let mut stream_job_view_upserts = Vec::new();
    let mut stream_job_view_deletes = Vec::new();
    let mut chunk_state_updates = Vec::new();
    let mut nonterminal_batch_updates = Vec::new();
    let mut terminal_batch_updates = Vec::new();

    for event in events {
        match event {
            ThroughputProjectionEvent::UpsertBatch { batch } => batch_upserts.push(batch.clone()),
            ThroughputProjectionEvent::UpsertChunk { chunk } => chunk_upserts.push(chunk.clone()),
            ThroughputProjectionEvent::UpsertStreamJobView { view } => {
                stream_job_view_upserts.push(view.clone());
            }
            ThroughputProjectionEvent::DeleteStreamJobView { view } => {
                stream_job_view_deletes.push(view.clone());
            }
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
    for view in &stream_job_view_upserts {
        state.store.upsert_stream_job_view_query(view).await?;
    }
    for view in &stream_job_view_deletes {
        state.store.delete_stream_job_view_query(view).await?;
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
    let throughput_events = records
        .iter()
        .filter_map(|record| match &record.event {
            ProjectionRecordEvent::Throughput(event) => Some(event.clone()),
            ProjectionRecordEvent::Streams(_) => None,
        })
        .collect::<Vec<_>>();
    if !throughput_events.is_empty() {
        apply_projection_events_directly(state, &throughput_events).await?;
    }
    for record in records {
        match &record.event {
            ProjectionRecordEvent::Streams(StreamsProjectionEvent::UpsertStreamJobView {
                view,
            }) => {
                state
                    .store
                    .upsert_stream_job_view_query(&fabrik_store::StreamJobViewRecord {
                        tenant_id: view.tenant_id.clone(),
                        instance_id: view.instance_id.clone(),
                        run_id: view.run_id.clone(),
                        job_id: view.job_id.clone(),
                        handle_id: view.handle_id.clone(),
                        view_name: view.view_name.clone(),
                        logical_key: view.logical_key.clone(),
                        output: view.output.clone(),
                        checkpoint_sequence: view.checkpoint_sequence,
                        updated_at: view.updated_at,
                    })
                    .await?;
            }
            ProjectionRecordEvent::Streams(StreamsProjectionEvent::DeleteStreamJobView {
                view,
            }) => {
                state
                    .store
                    .delete_stream_job_view_query(&fabrik_store::StreamJobViewDeleteRecord {
                        tenant_id: view.tenant_id.clone(),
                        instance_id: view.instance_id.clone(),
                        run_id: view.run_id.clone(),
                        job_id: view.job_id.clone(),
                        handle_id: view.handle_id.clone(),
                        view_name: view.view_name.clone(),
                        logical_key: view.logical_key.clone(),
                        checkpoint_sequence: view.checkpoint_sequence,
                        evicted_at: view.evicted_at,
                    })
                    .await?;
            }
            _ => {}
        }
    }
    let stream_event_count = records
        .iter()
        .filter(|record| matches!(record.event, ProjectionRecordEvent::Streams(_)))
        .count();
    if stream_event_count > 0 {
        record_projection_events_applied_directly(state, stream_event_count);
    }
    Ok(())
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
            serde_json::to_string(&record.activity_capabilities)
                .expect("bulk activity capabilities serialize")
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
            encode_cbor(&record.activity_capabilities, "bulk activity capabilities")
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

pub(crate) fn collect_results_uses_batch_result_handle_only(reducer: Option<&str>) -> bool {
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
        records.push(StreamProjectionRecord::throughput(
            throughput_partition_key(&report.batch_id, report.group_id),
            event,
        ));
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
        net::TcpListener,
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

    struct TestRedpanda {
        container_name: String,
        owned_container: bool,
        workflow_broker: BrokerConfig,
        changelog_topic: JsonTopicConfig,
        streams_changelog_topic: JsonTopicConfig,
        projections_topic: JsonTopicConfig,
        streams_projections_topic: JsonTopicConfig,
    }

    impl TestRedpanda {
        fn start() -> Result<Option<Self>> {
            if !docker_available() {
                eprintln!(
                    "skipping throughput-runtime redpanda-backed tests because docker is unavailable"
                );
                return Ok(None);
            }

            let local_output = Command::new("docker")
                .args(["ps", "--format", "{{.Names}}"])
                .output()
                .context("failed to inspect running docker containers for local redpanda")?;
            let local_names = String::from_utf8_lossy(&local_output.stdout);
            if local_names.lines().any(|line| line.trim() == "fabrik-redpanda-1") {
                let brokers = "127.0.0.1:29092".to_owned();
                return Ok(Some(Self {
                    container_name: "fabrik-redpanda-1".to_owned(),
                    owned_container: false,
                    workflow_broker: BrokerConfig::new(
                        brokers.clone(),
                        format!("workflow-events-test-{}", Uuid::now_v7()),
                        1,
                    ),
                    changelog_topic: JsonTopicConfig::new(
                        brokers.clone(),
                        format!("throughput-changelog-test-{}", Uuid::now_v7()),
                        1,
                    ),
                    streams_changelog_topic: JsonTopicConfig::new(
                        brokers.clone(),
                        format!("streams-changelog-test-{}", Uuid::now_v7()),
                        1,
                    ),
                    projections_topic: JsonTopicConfig::new(
                        brokers.clone(),
                        format!("throughput-projections-test-{}", Uuid::now_v7()),
                        1,
                    ),
                    streams_projections_topic: JsonTopicConfig::new(
                        brokers,
                        format!("streams-projections-test-{}", Uuid::now_v7()),
                        1,
                    ),
                }));
            }

            let image = std::env::var("FABRIK_TEST_REDPANDA_IMAGE")
                .unwrap_or_else(|_| "docker.redpanda.com/redpandadata/redpanda:v25.1.2".to_owned());
            let mut last_error = None;
            let mut container_name = String::new();
            let mut kafka_port = 0_u16;
            let mut started = false;
            for _ in 0..5 {
                kafka_port =
                    choose_free_port().context("failed to allocate redpanda kafka host port")?;
                container_name = format!("throughput-runtime-rp-test-{}", Uuid::now_v7());
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

            let brokers = format!("127.0.0.1:{kafka_port}");
            Ok(Some(Self {
                container_name,
                owned_container: true,
                workflow_broker: BrokerConfig::new(
                    brokers.clone(),
                    format!("workflow-events-test-{}", Uuid::now_v7()),
                    1,
                ),
                changelog_topic: JsonTopicConfig::new(
                    brokers.clone(),
                    format!("throughput-changelog-test-{}", Uuid::now_v7()),
                    1,
                ),
                streams_changelog_topic: JsonTopicConfig::new(
                    brokers.clone(),
                    format!("streams-changelog-test-{}", Uuid::now_v7()),
                    1,
                ),
                projections_topic: JsonTopicConfig::new(
                    brokers.clone(),
                    format!("throughput-projections-test-{}", Uuid::now_v7()),
                    1,
                ),
                streams_projections_topic: JsonTopicConfig::new(
                    brokers,
                    format!("streams-projections-test-{}", Uuid::now_v7()),
                    1,
                ),
            }))
        }

        async fn connect_workflow_publisher(&self) -> Result<WorkflowPublisher> {
            let deadline = Instant::now() + StdDuration::from_secs(45);
            loop {
                match WorkflowPublisher::new(&self.workflow_broker, "throughput-runtime-test").await
                {
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

        async fn connect_json_publisher<T>(
            &self,
            config: &JsonTopicConfig,
            id: &str,
        ) -> Result<JsonTopicPublisher<T>>
        where
            T: serde::Serialize,
        {
            let deadline = Instant::now() + StdDuration::from_secs(45);
            loop {
                match JsonTopicPublisher::new(config, id).await {
                    Ok(publisher) => return Ok(publisher),
                    Err(error) if Instant::now() < deadline => {
                        let _ = error;
                        sleep(StdDuration::from_millis(500)).await;
                    }
                    Err(error) => {
                        let logs = docker_logs(&self.container_name).unwrap_or_default();
                        return Err(error).with_context(|| {
                            format!(
                                "redpanda test container {} did not become ready for topic {}; logs:\n{}",
                                self.container_name, config.topic_name, logs
                            )
                        });
                    }
                }
            }
        }
    }

    impl Drop for TestRedpanda {
        fn drop(&mut self) {
            if self.owned_container {
                let _ = cleanup_container(&self.container_name);
            }
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
        let listener = TcpListener::bind("127.0.0.1:0").context("failed to bind ephemeral port")?;
        let port = listener.local_addr().context("failed to read ephemeral socket address")?.port();
        drop(listener);
        Ok(port)
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

    fn test_runtime_config(state_dir: &std::path::Path) -> ThroughputRuntimeConfig {
        ThroughputRuntimeConfig {
            native_stream_v2_engine_enabled: true,
            lease_ttl_seconds: 30,
            sweep_interval_ms: 500,
            poll_max_tasks: 32,
            report_apply_batch_size: 32,
            changelog_publish_batch_size: 32,
            projection_publish_batch_size: 32,
            publish_transient_projection_updates: false,
            owner_first_apply: true,
            publish_projection_events: false,
            max_active_chunks_per_batch: 1024,
            max_active_chunks_per_tenant: 4096,
            max_active_chunks_per_task_queue: 4096,
            local_state_dir: state_dir.join("state").display().to_string(),
            checkpoint_dir: state_dir.join("checkpoints").display().to_string(),
            checkpoint_interval_seconds: 60,
            checkpoint_retention: 2,
            checkpoint_key_prefix: format!("throughput-test-checkpoints-{}", Uuid::now_v7()),
            terminal_state_retention_seconds: 3600,
            terminal_gc_interval_seconds: 300,
            restore_idle_timeout_ms: 5_000,
            payload_store: fabrik_config::ThroughputPayloadStoreConfig {
                kind: fabrik_config::ThroughputPayloadStoreKind::LocalFilesystem,
                local_dir: state_dir.join("payloads").display().to_string(),
                s3_bucket: None,
                s3_region: "us-east-1".to_owned(),
                s3_endpoint: None,
                s3_access_key_id: None,
                s3_secret_access_key: None,
                s3_force_path_style: false,
                s3_key_prefix: "throughput-test".to_owned(),
            },
            inline_chunk_input_threshold_bytes: 256 * 1024,
            inline_chunk_output_threshold_bytes: 256 * 1024,
            grouping_chunk_threshold: 1024,
            target_chunks_per_group: 64,
            max_aggregation_groups: 1024,
        }
    }

    fn empty_debug_state() -> ThroughputDebugState {
        ThroughputDebugState {
            throughput_commands_received: 0,
            streams_commands_received: 0,
            poll_requests: 0,
            poll_responses: 0,
            leased_tasks: 0,
            empty_polls: 0,
            lease_misses_with_ready_chunks: 0,
            last_lease_selection_debug: None,
            bridged_batches: 0,
            duplicate_bridge_events: 0,
            duplicate_stream_job_checkpoint_publications_ignored: 0,
            duplicate_stream_job_query_requests_ignored: 0,
            duplicate_stream_job_terminal_publications_ignored: 0,
            bridge_failures: 0,
            report_rpcs_received: 0,
            reports_received: 0,
            report_batches_applied: 0,
            report_batch_items_total: 0,
            reports_rejected: 0,
            local_fastlane_chunks_completed: 0,
            local_fastlane_batches_applied: 0,
            projection_events_published: 0,
            throughput_projection_events_published: 0,
            streams_projection_events_published: 0,
            projection_events_skipped: 0,
            projection_events_applied_directly: 0,
            changelog_entries_published: 0,
            throughput_changelog_entries_published: 0,
            streams_changelog_entries_published: 0,
            terminal_events_published: 0,
            leaf_group_terminals_published: 0,
            parent_group_terminals_published: 0,
            root_group_terminals_published: 0,
            tenant_throttle_events: 0,
            task_queue_throttle_events: 0,
            batch_throttle_events: 0,
            apply_divergences: 0,
            ownership_catchup_waits: 0,
            changelog_consumer_enabled: false,
            last_bridge_at: None,
            last_duplicate_stream_job_checkpoint_publication_at: None,
            last_duplicate_stream_job_checkpoint_publication: None,
            last_duplicate_stream_job_query_request_at: None,
            last_duplicate_stream_job_terminal_publication_at: None,
            last_throughput_command_at: None,
            last_streams_command_at: None,
            last_report_at: None,
            last_terminal_event_at: None,
            last_lease_miss_with_ready_chunks_at: None,
            last_report_rejection_reason: None,
            last_throttle_at: None,
            last_throttle_reason: None,
            last_apply_divergence: None,
            last_catchup_wait_partition: None,
            last_duplicate_stream_job_query_request: None,
            last_duplicate_stream_job_terminal_publication: None,
        }
    }

    fn empty_ownership_state() -> ThroughputOwnershipState {
        ThroughputOwnershipState {
            owner_id: "throughput-runtime-test-owner".to_owned(),
            partition_id_offset: 0,
            leases: HashMap::new(),
        }
    }

    fn keyed_rollup_topic_job_config(topic_name: &str) -> String {
        serde_json::json!({
            "name": stream_jobs::KEYED_ROLLUP_JOB_NAME,
            "runtime": "keyed_rollup",
            "source": {
                "kind": "topic",
                "name": topic_name,
            },
            "keyBy": "accountId",
            "operators": [
                {
                    "kind": "reduce",
                    "config": {
                        "reducer": "sum",
                        "valueField": "amount",
                        "outputField": "totalAmount"
                    }
                },
                {
                    "kind": "emit_checkpoint",
                    "name": stream_jobs::KEYED_ROLLUP_CHECKPOINT_NAME,
                    "config": {
                        "sequence": stream_jobs::KEYED_ROLLUP_CHECKPOINT_SEQUENCE
                    }
                }
            ],
            "checkpointPolicy": {
                "kind": "named_checkpoints",
                "checkpoints": [
                    {
                        "name": stream_jobs::KEYED_ROLLUP_CHECKPOINT_NAME,
                        "sequence": stream_jobs::KEYED_ROLLUP_CHECKPOINT_SEQUENCE,
                        "delivery": "workflow_awaitable"
                    }
                ]
            },
            "views": [
                {
                    "name": "accountTotals",
                    "consistency": "strong",
                    "queryMode": "by_key",
                    "keyField": "accountId"
                }
            ],
            "queries": [
                {
                    "name": "accountTotals",
                    "viewName": "accountTotals",
                    "consistency": "strong"
                }
            ]
        })
        .to_string()
    }

    fn aggregate_v2_topic_job_config(topic_name: &str) -> String {
        serde_json::json!({
            "name": "fraud-detector",
            "runtime": "aggregate_v2",
            "source": {
                "kind": "topic",
                "name": topic_name,
            },
            "keyBy": "accountId",
            "states": [
                {
                    "id": "risk-state",
                    "kind": "keyed",
                    "keyFields": ["accountId"],
                    "valueFields": ["avgRisk"]
                }
            ],
            "operators": [
                {
                    "kind": "aggregate",
                    "operatorId": "avg-risk",
                    "config": {
                        "reducer": "avg",
                        "valueField": "risk",
                        "outputField": "avgRisk"
                    },
                    "stateIds": ["risk-state"]
                },
                {
                    "kind": "materialize",
                    "operatorId": "materialize-risk",
                    "config": {
                        "view": "riskScores"
                    },
                    "stateIds": ["risk-state"]
                },
                {
                    "kind": "emit_checkpoint",
                    "name": "initial-risk-ready",
                    "config": {
                        "sequence": 1
                    }
                }
            ],
            "checkpointPolicy": {
                "kind": "named_checkpoints",
                "checkpoints": [
                    {
                        "name": "initial-risk-ready",
                        "sequence": 1,
                        "delivery": "workflow_awaitable"
                    }
                ]
            },
            "views": [
                {
                    "name": "riskScores",
                    "consistency": "strong",
                    "queryMode": "by_key",
                    "keyField": "accountId"
                }
            ],
            "queries": [
                {
                    "name": "riskScoresByKey",
                    "viewName": "riskScores",
                    "consistency": "strong"
                }
            ]
        })
        .to_string()
    }

    fn aggregate_v2_bounded_threshold_signal_job_config() -> String {
        serde_json::json!({
            "name": "fraud-threshold-signal",
            "runtime": "aggregate_v2",
            "source": {
                "kind": "bounded_input"
            },
            "keyBy": "accountId",
            "states": [
                {
                    "id": "risk-threshold-state",
                    "kind": "keyed",
                    "keyFields": ["accountId"],
                    "valueFields": ["riskExceeded"]
                }
            ],
            "operators": [
                {
                    "kind": "aggregate",
                    "operatorId": "risk-threshold",
                    "config": {
                        "reducer": "threshold",
                        "valueField": "risk",
                        "threshold": 0.97,
                        "comparison": "gte",
                        "outputField": "riskExceeded"
                    },
                    "stateIds": ["risk-threshold-state"]
                },
                {
                    "kind": "materialize",
                    "operatorId": "materialize-threshold",
                    "config": {
                        "view": "riskThresholds"
                    },
                    "stateIds": ["risk-threshold-state"]
                },
                {
                    "kind": "signal_workflow",
                    "operatorId": "notify-fraud",
                    "config": {
                        "view": "riskThresholds",
                        "signalType": "fraud.threshold.crossed",
                        "whenOutputField": "riskExceeded"
                    }
                }
            ],
            "views": [
                {
                    "name": "riskThresholds",
                    "consistency": "strong",
                    "queryMode": "by_key",
                    "keyField": "accountId"
                }
            ],
            "queries": [
                {
                    "name": "riskThresholdsByKey",
                    "viewName": "riskThresholds",
                    "consistency": "strong"
                }
            ]
        })
        .to_string()
    }

    fn aggregate_v2_topic_window_job_config_with_retention(
        topic_name: &str,
        allowed_lateness: Option<&str>,
        retention_seconds: Option<u64>,
    ) -> String {
        let mut window_config = serde_json::Map::from_iter([
            ("mode".to_owned(), serde_json::json!("tumbling")),
            ("size".to_owned(), serde_json::json!("1m")),
            ("timeField".to_owned(), serde_json::json!("eventTime")),
        ]);
        if let Some(allowed_lateness) = allowed_lateness {
            window_config.insert("allowedLateness".to_owned(), serde_json::json!(allowed_lateness));
        }
        serde_json::json!({
            "name": "fraud-detector",
            "runtime": "aggregate_v2",
            "source": {
                "kind": "topic",
                "name": topic_name,
            },
            "keyBy": "accountId",
            "states": [
                {
                    "id": "minute-window",
                    "kind": "window",
                    "keyFields": ["accountId", "windowStart"],
                    "valueFields": ["avgRisk"]
                },
                {
                    "id": "risk-state",
                    "kind": "keyed",
                    "keyFields": ["accountId", "windowStart"],
                    "valueFields": ["avgRisk"]
                }
            ],
            "operators": [
                {
                    "kind": "window",
                    "operatorId": "minute-window",
                    "config": window_config,
                    "stateIds": ["minute-window"]
                },
                {
                    "kind": "aggregate",
                    "operatorId": "avg-risk",
                    "config": {
                        "reducer": "avg",
                        "valueField": "risk",
                        "outputField": "avgRisk"
                    },
                    "stateIds": ["risk-state"]
                },
                {
                    "kind": "materialize",
                    "operatorId": "materialize-risk",
                    "config": {
                        "view": "riskScores"
                    },
                    "stateIds": ["risk-state"]
                },
                {
                    "kind": "emit_checkpoint",
                    "name": "initial-risk-ready",
                    "config": {
                        "sequence": 1
                    }
                }
            ],
            "checkpointPolicy": {
                "kind": "named_checkpoints",
                "checkpoints": [
                    {
                        "name": "initial-risk-ready",
                        "sequence": 1,
                        "delivery": "workflow_awaitable"
                    }
                ]
            },
            "views": [
                {
                    "name": "riskScores",
                    "consistency": "strong",
                    "queryMode": "by_key",
                    "keyField": "accountId",
                    "supportedConsistencies": ["strong", "eventual"],
                    "retentionSeconds": retention_seconds
                }
            ],
            "queries": [
                {
                    "name": "riskScoresByKey",
                    "viewName": "riskScores",
                    "consistency": "strong",
                    "argFields": ["accountId", "windowStart"]
                }
            ]
        })
        .to_string()
    }

    fn topic_stream_job_handle(
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        job_id: &str,
        topic_name: &str,
        status: fabrik_throughput::StreamJobBridgeHandleStatus,
        cancellation_requested_at: Option<DateTime<Utc>>,
        cancellation_reason: Option<&str>,
        occurred_at: DateTime<Utc>,
    ) -> StreamJobBridgeHandleRecord {
        StreamJobBridgeHandleRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
            tenant_id: tenant_id.to_owned(),
            instance_id: instance_id.to_owned(),
            run_id: run_id.to_owned(),
            stream_instance_id: instance_id.to_owned(),
            stream_run_id: run_id.to_owned(),
            job_id: job_id.to_owned(),
            handle_id: fabrik_throughput::stream_job_handle_id(
                tenant_id,
                instance_id,
                run_id,
                job_id,
            ),
            bridge_request_id: fabrik_throughput::stream_job_bridge_request_id(
                tenant_id,
                instance_id,
                run_id,
                job_id,
            ),
            origin_kind: fabrik_throughput::StreamJobOriginKind::Workflow.as_str().to_owned(),
            definition_id: "payments-rollup".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-a".to_owned()),
            job_name: stream_jobs::KEYED_ROLLUP_JOB_NAME.to_owned(),
            input_ref: serde_json::json!({
                "kind": "topic",
                "topic": topic_name,
                "startOffset": "earliest"
            })
            .to_string(),
            config_ref: Some(keyed_rollup_topic_job_config(topic_name)),
            checkpoint_policy: None,
            view_definitions: None,
            status: status.as_str().to_owned(),
            workflow_owner_epoch: Some(1),
            stream_owner_epoch: None,
            cancellation_requested_at,
            cancellation_reason: cancellation_reason.map(str::to_owned),
            terminal_event_id: None,
            terminal_at: None,
            workflow_accepted_at: None,
            terminal_output: None,
            terminal_error: None,
            created_at: occurred_at,
            updated_at: occurred_at,
        }
    }

    fn aggregate_v2_topic_stream_job_handle(
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        job_id: &str,
        topic_name: &str,
        status: fabrik_throughput::StreamJobBridgeHandleStatus,
        cancellation_requested_at: Option<DateTime<Utc>>,
        cancellation_reason: Option<&str>,
        occurred_at: DateTime<Utc>,
    ) -> StreamJobBridgeHandleRecord {
        StreamJobBridgeHandleRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
            tenant_id: tenant_id.to_owned(),
            instance_id: instance_id.to_owned(),
            run_id: run_id.to_owned(),
            stream_instance_id: instance_id.to_owned(),
            stream_run_id: run_id.to_owned(),
            job_id: job_id.to_owned(),
            handle_id: fabrik_throughput::stream_job_handle_id(
                tenant_id,
                instance_id,
                run_id,
                job_id,
            ),
            bridge_request_id: fabrik_throughput::stream_job_bridge_request_id(
                tenant_id,
                instance_id,
                run_id,
                job_id,
            ),
            origin_kind: fabrik_throughput::StreamJobOriginKind::Workflow.as_str().to_owned(),
            definition_id: "fraud-detector".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-a".to_owned()),
            job_name: "fraud-detector".to_owned(),
            input_ref: serde_json::json!({
                "kind": "topic",
                "topic": topic_name,
                "startOffset": "earliest"
            })
            .to_string(),
            config_ref: Some(aggregate_v2_topic_job_config(topic_name)),
            checkpoint_policy: None,
            view_definitions: None,
            status: status.as_str().to_owned(),
            workflow_owner_epoch: Some(1),
            stream_owner_epoch: None,
            cancellation_requested_at,
            cancellation_reason: cancellation_reason.map(str::to_owned),
            terminal_event_id: None,
            terminal_at: None,
            workflow_accepted_at: None,
            terminal_output: None,
            terminal_error: None,
            created_at: occurred_at,
            updated_at: occurred_at,
        }
    }

    fn aggregate_v2_topic_window_stream_job_handle(
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        job_id: &str,
        topic_name: &str,
        status: fabrik_throughput::StreamJobBridgeHandleStatus,
        occurred_at: DateTime<Utc>,
    ) -> StreamJobBridgeHandleRecord {
        aggregate_v2_topic_window_stream_job_handle_with_allowed_lateness(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            topic_name,
            status,
            None,
            occurred_at,
        )
    }

    fn aggregate_v2_topic_window_stream_job_handle_with_allowed_lateness(
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        job_id: &str,
        topic_name: &str,
        status: fabrik_throughput::StreamJobBridgeHandleStatus,
        allowed_lateness: Option<&str>,
        occurred_at: DateTime<Utc>,
    ) -> StreamJobBridgeHandleRecord {
        aggregate_v2_topic_window_stream_job_handle_with_policy(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            topic_name,
            status,
            allowed_lateness,
            None,
            occurred_at,
        )
    }

    fn aggregate_v2_topic_window_stream_job_handle_with_policy(
        tenant_id: &str,
        instance_id: &str,
        run_id: &str,
        job_id: &str,
        topic_name: &str,
        status: fabrik_throughput::StreamJobBridgeHandleStatus,
        allowed_lateness: Option<&str>,
        retention_seconds: Option<u64>,
        occurred_at: DateTime<Utc>,
    ) -> StreamJobBridgeHandleRecord {
        StreamJobBridgeHandleRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
            tenant_id: tenant_id.to_owned(),
            instance_id: instance_id.to_owned(),
            run_id: run_id.to_owned(),
            stream_instance_id: instance_id.to_owned(),
            stream_run_id: run_id.to_owned(),
            job_id: job_id.to_owned(),
            handle_id: fabrik_throughput::stream_job_handle_id(
                tenant_id,
                instance_id,
                run_id,
                job_id,
            ),
            bridge_request_id: fabrik_throughput::stream_job_bridge_request_id(
                tenant_id,
                instance_id,
                run_id,
                job_id,
            ),
            origin_kind: fabrik_throughput::StreamJobOriginKind::Workflow.as_str().to_owned(),
            definition_id: "fraud-detector".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-a".to_owned()),
            job_name: "fraud-detector".to_owned(),
            input_ref: serde_json::json!({
                "kind": "topic",
                "topic": topic_name,
                "startOffset": "earliest"
            })
            .to_string(),
            config_ref: Some(aggregate_v2_topic_window_job_config_with_retention(
                topic_name,
                allowed_lateness,
                retention_seconds,
            )),
            checkpoint_policy: None,
            view_definitions: None,
            status: status.as_str().to_owned(),
            workflow_owner_epoch: Some(1),
            stream_owner_epoch: None,
            cancellation_requested_at: None,
            cancellation_reason: None,
            terminal_event_id: None,
            terminal_at: None,
            workflow_accepted_at: None,
            terminal_output: None,
            terminal_error: None,
            created_at: occurred_at,
            updated_at: occurred_at,
        }
    }

    async fn test_app_state(
        store: WorkflowStore,
        redpanda: &TestRedpanda,
        state_dir: &std::path::Path,
    ) -> Result<AppState> {
        test_app_state_with_partitions(store, redpanda, state_dir, 1).await
    }

    async fn test_app_state_with_partitions(
        store: WorkflowStore,
        redpanda: &TestRedpanda,
        state_dir: &std::path::Path,
        throughput_partitions: i32,
    ) -> Result<AppState> {
        fs::create_dir_all(state_dir.join("state"))?;
        fs::create_dir_all(state_dir.join("checkpoints"))?;
        fs::create_dir_all(state_dir.join("payloads"))?;
        let runtime = test_runtime_config(state_dir);
        let local_state = LocalThroughputState::open(
            std::path::Path::new(&runtime.local_state_dir),
            std::path::Path::new(&runtime.checkpoint_dir),
            3,
        )?;
        let payload_store = PayloadStore::from_config(PayloadStoreConfig {
            default_store: PayloadStoreKind::LocalFilesystem,
            local_dir: runtime.payload_store.local_dir.clone(),
            s3_bucket: None,
            s3_region: runtime.payload_store.s3_region.clone(),
            s3_endpoint: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_force_path_style: false,
            s3_key_prefix: runtime.payload_store.s3_key_prefix.clone(),
        })
        .await?;
        Ok(AppState {
            store,
            aggregation: AggregationCoordinator::new(local_state.clone()),
            local_state,
            shard_workers: Arc::new(OnceLock::new()),
            json_brokers: redpanda.workflow_broker.brokers.clone(),
            workflow_publisher: redpanda.connect_workflow_publisher().await?,
            throughput_changelog_publisher: redpanda
                .connect_json_publisher(
                    &redpanda.changelog_topic,
                    "throughput-runtime-changelog-test",
                )
                .await?,
            streams_changelog_publisher: redpanda
                .connect_json_publisher(
                    &redpanda.streams_changelog_topic,
                    "throughput-runtime-streams-changelog-test",
                )
                .await?,
            throughput_projection_publisher: redpanda
                .connect_json_publisher(
                    &redpanda.projections_topic,
                    "throughput-runtime-projections-test",
                )
                .await?,
            streams_projection_publisher: redpanda
                .connect_json_publisher(
                    &redpanda.streams_projections_topic,
                    "throughput-runtime-streams-projections-test",
                )
                .await?,
            payload_store,
            bulk_notify: Arc::new(Notify::new()),
            outbox_notify: Arc::new(Notify::new()),
            runtime,
            activity_capability_registry: Arc::new(ActivityCapabilityRegistry::default()),
            activity_executor_registry: Arc::new(LocalActivityExecutorRegistry::default()),
            throughput_partitions,
            outbox_publisher_id: "throughput-runtime-outbox-test".to_owned(),
            debug: Arc::new(StdMutex::new(empty_debug_state())),
            ownership: Arc::new(StdMutex::new(empty_ownership_state())),
            #[cfg(test)]
            fail_next_workflow_outbox_writes: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn test_batch_identity() -> ThroughputBatchIdentity {
        ThroughputBatchIdentity {
            tenant_id: "tenant-a".to_owned(),
            instance_id: "wf-a".to_owned(),
            run_id: "run-a".to_owned(),
            batch_id: "batch-a".to_owned(),
        }
    }

    fn distinct_stream_job_keys(partition_count: i32) -> (String, i32, String, i32) {
        let first_key = "acct_partition_a".to_owned();
        let first_partition = throughput_partition_for_stream_key(&first_key, partition_count);
        let second_key = (1..64)
            .map(|index| format!("acct_partition_b_{index}"))
            .find(|candidate| {
                throughput_partition_for_stream_key(candidate, partition_count) != first_partition
            })
            .expect("should find a logical key on a different partition");
        let second_partition = throughput_partition_for_stream_key(&second_key, partition_count);
        (first_key, first_partition, second_key, second_partition)
    }

    fn distinct_topic_partition_keys(partition_count: i32) -> (String, i32, String, i32) {
        let first_key = "topic_partition_a".to_owned();
        let first_partition = partition_for_key(&first_key, partition_count);
        let second_key = (1..128)
            .map(|index| format!("topic_partition_b_{index}"))
            .find(|candidate| partition_for_key(candidate, partition_count) != first_partition)
            .expect("should find a topic key on a different partition");
        let second_partition = partition_for_key(&second_key, partition_count);
        (first_key, first_partition, second_key, second_partition)
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
        let ProjectionRecordEvent::Throughput(ThroughputProjectionEvent::UpdateChunkState {
            update,
        }) = &records[0].event
        else {
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

    #[tokio::test]
    async fn stream_job_schedule_persists_terminal_state_before_query_path() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-stream-job-schedule-terminal");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;

        let tenant_id = "tenant";
        let instance_id = "instance-stream-job-schedule-terminal";
        let run_id = "run-stream-job-schedule-terminal";
        let job_id = "job-stream-job-schedule-terminal";
        let handle_id =
            fabrik_throughput::stream_job_handle_id(tenant_id, instance_id, run_id, job_id);
        let occurred_at = Utc::now();

        store
            .upsert_stream_job_bridge_handle(&StreamJobBridgeHandleRecord {
                workflow_event_id: Uuid::now_v7(),
                protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
                tenant_id: tenant_id.to_owned(),
                instance_id: instance_id.to_owned(),
                run_id: run_id.to_owned(),
                stream_instance_id: instance_id.to_owned(),
                stream_run_id: run_id.to_owned(),
                job_id: job_id.to_owned(),
                handle_id: handle_id.clone(),
                bridge_request_id: fabrik_throughput::stream_job_bridge_request_id(
                    tenant_id,
                    instance_id,
                    run_id,
                    job_id,
                ),
                origin_kind: fabrik_throughput::StreamJobOriginKind::Workflow.as_str().to_owned(),
                definition_id: "demo".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                job_name: stream_jobs::KEYED_ROLLUP_JOB_NAME.to_owned(),
                input_ref: r#"{"kind":"bounded_items","items":[{"accountId":"acct_1","amount":2.0},{"accountId":"acct_1","amount":3.0},{"accountId":"acct_2","amount":11.0}]}"#.to_owned(),
                config_ref: None,
                checkpoint_policy: None,
                view_definitions: None,
                status: fabrik_throughput::StreamJobBridgeHandleStatus::Admitted.as_str().to_owned(),
                workflow_owner_epoch: Some(3),
                stream_owner_epoch: None,
                cancellation_requested_at: None,
                cancellation_reason: None,
                terminal_event_id: None,
                terminal_at: None,
                workflow_accepted_at: None,
                terminal_output: None,
                terminal_error: None,
                created_at: occurred_at,
                updated_at: occurred_at,
            })
            .await?;

        let identity = WorkflowIdentity::new(
            tenant_id,
            "demo",
            1,
            "artifact-a",
            instance_id,
            run_id,
            "throughput-runtime-test",
        );
        let mut event = EventEnvelope::new(
            WorkflowEvent::StreamJobScheduled {
                job_id: job_id.to_owned(),
                job_name: stream_jobs::KEYED_ROLLUP_JOB_NAME.to_owned(),
                input: serde_json::json!({
                    "kind": "bounded_items",
                    "items": [
                        { "accountId": "acct_1", "amount": 2.0 },
                        { "accountId": "acct_1", "amount": 3.0 },
                        { "accountId": "acct_2", "amount": 11.0 }
                    ]
                }),
                config: None,
                state: None,
            }
            .event_type(),
            identity,
            WorkflowEvent::StreamJobScheduled {
                job_id: job_id.to_owned(),
                job_name: stream_jobs::KEYED_ROLLUP_JOB_NAME.to_owned(),
                input: serde_json::json!({
                    "kind": "bounded_items",
                    "items": [
                        { "accountId": "acct_1", "amount": 2.0 },
                        { "accountId": "acct_1", "amount": 3.0 },
                        { "accountId": "acct_2", "amount": 11.0 }
                    ]
                }),
                config: None,
                state: None,
            },
        );
        event.occurred_at = occurred_at;

        bridge::handle_bridge_event(app_state.clone(), event).await?;

        let stored_handle = store
            .get_stream_job_bridge_handle(tenant_id, instance_id, run_id, job_id)
            .await?
            .context("stream job handle should exist after schedule")?;
        assert_eq!(
            stored_handle.parsed_status(),
            Some(fabrik_throughput::StreamJobBridgeHandleStatus::Completed)
        );
        assert_eq!(stored_handle.stream_owner_epoch, Some(1));
        assert!(stored_handle.terminal_at.is_some());
        assert_eq!(
            stored_handle.terminal_output.as_ref().and_then(|output| output.get("status")),
            Some(&serde_json::json!("completed"))
        );

        let stored_job = store
            .get_stream_job(tenant_id, instance_id, run_id, job_id)
            .await?
            .context("stream job record should exist after schedule")?;
        assert_eq!(stored_job.parsed_status(), Some(fabrik_throughput::StreamJobStatus::Completed));

        let checkpoints =
            store.list_stream_job_bridge_checkpoints_for_handle_page(&handle_id, 10, 0).await?;
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(
            checkpoints[0].parsed_status(),
            Some(fabrik_throughput::StreamJobCheckpointStatus::Reached)
        );

        let outbox = store
            .lease_workflow_event_outbox(
                "stream-job-schedule-terminal-test",
                chrono::Duration::seconds(30),
                10,
                occurred_at + chrono::Duration::seconds(1),
            )
            .await?;
        assert!(outbox.iter().any(|record| record.event_type == "StreamJobCheckpointReached"));
        assert!(!outbox.iter().any(|record| record.event_type == "StreamJobCompleted"));
        assert!(!outbox.iter().any(|record| record.event_type == "StreamJobQueryCompleted"));
        assert!(!outbox.iter().any(|record| record.event_type == "StreamJobQueryFailed"));

        let queries =
            store.list_stream_job_bridge_queries_for_handle_page(&handle_id, 10, 0).await?;
        assert!(queries.is_empty());

        let acct_1 = app_state
            .local_state
            .load_stream_job_view_state(&handle_id, "accountTotals", "acct_1")?
            .context("acct_1 projection should exist")?;
        assert_eq!(acct_1.output["totalAmount"], serde_json::json!(5.0));

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn stream_job_schedule_routes_partition_work_to_owner_shards() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-stream-job-owner-shards");
        let throughput_partitions = 8;
        let app_state = test_app_state_with_partitions(
            store.clone(),
            &redpanda,
            &state_dir,
            throughput_partitions,
        )
        .await?;
        let managed_partitions = (0..throughput_partitions).collect::<Vec<_>>();
        let shard_workers = ShardWorkerPool::new(app_state.clone(), &managed_partitions);
        let _ = app_state.shard_workers.set(shard_workers.clone());

        let (first_key, first_partition, second_key, second_partition) =
            distinct_stream_job_keys(throughput_partitions);
        assert_ne!(first_partition, second_partition);

        let tenant_id = "tenant";
        let instance_id = "instance-stream-job-owner-shards";
        let run_id = "run-stream-job-owner-shards";
        let job_id = "job-stream-job-owner-shards";
        let handle_id =
            fabrik_throughput::stream_job_handle_id(tenant_id, instance_id, run_id, job_id);
        let occurred_at = Utc::now();

        store
            .upsert_stream_job_bridge_handle(&StreamJobBridgeHandleRecord {
                workflow_event_id: Uuid::now_v7(),
                protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
                tenant_id: tenant_id.to_owned(),
                instance_id: instance_id.to_owned(),
                run_id: run_id.to_owned(),
                stream_instance_id: instance_id.to_owned(),
                stream_run_id: run_id.to_owned(),
                job_id: job_id.to_owned(),
                handle_id: handle_id.clone(),
                bridge_request_id: fabrik_throughput::stream_job_bridge_request_id(
                    tenant_id,
                    instance_id,
                    run_id,
                    job_id,
                ),
                origin_kind: fabrik_throughput::StreamJobOriginKind::Workflow.as_str().to_owned(),
                definition_id: "demo".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                job_name: stream_jobs::KEYED_ROLLUP_JOB_NAME.to_owned(),
                input_ref: serde_json::json!({
                    "kind": "bounded_items",
                    "items": [
                        { "accountId": first_key.clone(), "amount": 2.0 },
                        { "accountId": first_key.clone(), "amount": 3.0 },
                        { "accountId": second_key.clone(), "amount": 11.0 }
                    ]
                })
                .to_string(),
                config_ref: None,
                checkpoint_policy: None,
                view_definitions: None,
                status: fabrik_throughput::StreamJobBridgeHandleStatus::Admitted
                    .as_str()
                    .to_owned(),
                workflow_owner_epoch: Some(3),
                stream_owner_epoch: None,
                cancellation_requested_at: None,
                cancellation_reason: None,
                terminal_event_id: None,
                terminal_at: None,
                workflow_accepted_at: None,
                terminal_output: None,
                terminal_error: None,
                created_at: occurred_at,
                updated_at: occurred_at,
            })
            .await?;

        let identity = WorkflowIdentity::new(
            tenant_id,
            "demo",
            1,
            "artifact-a",
            instance_id,
            run_id,
            "throughput-runtime-test",
        );
        let mut event = EventEnvelope::new(
            WorkflowEvent::StreamJobScheduled {
                job_id: job_id.to_owned(),
                job_name: stream_jobs::KEYED_ROLLUP_JOB_NAME.to_owned(),
                input: serde_json::json!({
                    "kind": "bounded_items",
                    "items": [
                        { "accountId": first_key.clone(), "amount": 2.0 },
                        { "accountId": first_key.clone(), "amount": 3.0 },
                        { "accountId": second_key.clone(), "amount": 11.0 }
                    ]
                }),
                config: None,
                state: None,
            }
            .event_type(),
            identity,
            WorkflowEvent::StreamJobScheduled {
                job_id: job_id.to_owned(),
                job_name: stream_jobs::KEYED_ROLLUP_JOB_NAME.to_owned(),
                input: serde_json::json!({
                    "kind": "bounded_items",
                    "items": [
                        { "accountId": first_key.clone(), "amount": 2.0 },
                        { "accountId": first_key.clone(), "amount": 3.0 },
                        { "accountId": second_key.clone(), "amount": 11.0 }
                    ]
                }),
                config: None,
                state: None,
            },
        );
        event.occurred_at = occurred_at;
        bridge::handle_bridge_event(app_state.clone(), event).await?;

        let first_owner_state = shard_workers
            .local_state_for_partition(first_partition)
            .expect("first owner shard should exist");
        let second_owner_state = shard_workers
            .local_state_for_partition(second_partition)
            .expect("second owner shard should exist");

        let first_view = first_owner_state
            .load_stream_job_view_state(&handle_id, "accountTotals", &first_key)?
            .context("first key should materialize on its owner shard")?;
        assert_eq!(first_view.output["totalAmount"], serde_json::json!(5.0));
        assert!(
            first_owner_state
                .load_stream_job_view_state(&handle_id, "accountTotals", &second_key)?
                .is_none()
        );

        let second_view = second_owner_state
            .load_stream_job_view_state(&handle_id, "accountTotals", &second_key)?
            .context("second key should materialize on its owner shard")?;
        assert_eq!(second_view.output["totalAmount"], serde_json::json!(11.0));
        assert!(
            second_owner_state
                .load_stream_job_view_state(&handle_id, "accountTotals", &first_key)?
                .is_none()
        );

        let shard_checkpoint = first_owner_state
            .snapshot_partition_checkpoint_value(first_partition, throughput_partitions)?;
        let restored_db_path = temp_path("throughput-stream-job-owner-shards-restored-db");
        let restored_checkpoint_dir =
            temp_path("throughput-stream-job-owner-shards-restored-checkpoints");
        let restored = LocalThroughputState::open(&restored_db_path, &restored_checkpoint_dir, 3)?;
        assert!(restored.restore_from_checkpoint_value_if_empty(shard_checkpoint)?);
        let restored_view = restored
            .load_stream_job_view_state(&handle_id, "accountTotals", &first_key)?
            .context("restored owner shard should retain strong-read materialization")?;
        assert_eq!(restored_view.output["totalAmount"], serde_json::json!(5.0));

        fs::remove_dir_all(state_dir).ok();
        fs::remove_dir_all(restored_db_path).ok();
        fs::remove_dir_all(restored_checkpoint_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn standalone_stream_job_command_activates_without_workflow_outbox_events() -> Result<()>
    {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-standalone-stream-job-command");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;

        let tenant_id = "tenant";
        let instance_id = "stream-standalone-command";
        let run_id = "run-standalone-command";
        let job_id = "job-standalone-command";
        let handle_id =
            fabrik_throughput::stream_job_handle_id(tenant_id, instance_id, run_id, job_id);
        let occurred_at = Utc::now();

        store
            .upsert_stream_job_bridge_handle(&StreamJobBridgeHandleRecord {
                workflow_event_id: Uuid::now_v7(),
                protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
                tenant_id: tenant_id.to_owned(),
                instance_id: instance_id.to_owned(),
                run_id: run_id.to_owned(),
                stream_instance_id: instance_id.to_owned(),
                stream_run_id: run_id.to_owned(),
                job_id: job_id.to_owned(),
                handle_id: handle_id.clone(),
                bridge_request_id: fabrik_throughput::stream_job_bridge_request_id(
                    tenant_id,
                    instance_id,
                    run_id,
                    job_id,
                ),
                origin_kind: fabrik_throughput::StreamJobOriginKind::Standalone.as_str().to_owned(),
                definition_id: "payments-rollup".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                job_name: stream_jobs::KEYED_ROLLUP_JOB_NAME.to_owned(),
                input_ref: r#"{"kind":"bounded_items","items":[{"accountId":"acct_1","amount":2.0},{"accountId":"acct_1","amount":3.0},{"accountId":"acct_2","amount":11.0}]}"#.to_owned(),
                config_ref: None,
                checkpoint_policy: None,
                view_definitions: None,
                status: fabrik_throughput::StreamJobBridgeHandleStatus::Admitted.as_str().to_owned(),
                workflow_owner_epoch: None,
                stream_owner_epoch: None,
                cancellation_requested_at: None,
                cancellation_reason: None,
                terminal_event_id: None,
                terminal_at: None,
                workflow_accepted_at: None,
                terminal_output: None,
                terminal_error: None,
                created_at: occurred_at,
                updated_at: occurred_at,
            })
            .await?;

        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{handle_id}"),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle_id.clone(),
                    },
                ),
            },
        )
        .await?;

        let stored_handle = store
            .get_stream_job_bridge_handle(tenant_id, instance_id, run_id, job_id)
            .await?
            .context("standalone stream job handle should exist after command")?;
        assert_eq!(
            stored_handle.parsed_status(),
            Some(fabrik_throughput::StreamJobBridgeHandleStatus::Completed)
        );

        let stored_job = store
            .get_stream_job(tenant_id, instance_id, run_id, job_id)
            .await?
            .context("standalone stream job record should exist after command")?;
        assert_eq!(stored_job.parsed_status(), Some(fabrik_throughput::StreamJobStatus::Completed));

        let outbox = store
            .lease_workflow_event_outbox(
                "standalone-stream-job-command-test",
                chrono::Duration::seconds(30),
                10,
                occurred_at + chrono::Duration::seconds(1),
            )
            .await?;
        assert!(outbox.is_empty());

        let acct_1 = app_state
            .local_state
            .load_stream_job_view_state(&handle_id, "accountTotals", "acct_1")?
            .context("acct_1 projection should exist")?;
        assert_eq!(acct_1.output["totalAmount"], serde_json::json!(5.0));

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn workflow_topic_stream_job_reaches_checkpoint_and_answers_strong_query() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-topic-stream-job-checkpoint-query");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;
        let source_topic = JsonTopicConfig::new(
            redpanda.workflow_broker.brokers.clone(),
            format!("stream-source-test-{}", Uuid::now_v7()),
            1,
        );
        let source_publisher = redpanda
            .connect_json_publisher::<Value>(&source_topic, "throughput-topic-stream-source-test")
            .await?;
        source_publisher.publish(&json!({"accountId": "acct_1", "amount": 2.0}), "acct_1").await?;
        source_publisher.publish(&json!({"accountId": "acct_1", "amount": 3.0}), "acct_1").await?;

        let tenant_id = "tenant";
        let instance_id = "instance-stream-job-topic";
        let run_id = "run-stream-job-topic";
        let job_id = "job-stream-job-topic";
        let occurred_at = Utc::now();
        let handle = topic_stream_job_handle(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            &source_topic.topic_name,
            fabrik_throughput::StreamJobBridgeHandleStatus::Admitted,
            None,
            None,
            occurred_at,
        );
        let handle_id = handle.handle_id.clone();
        store.upsert_stream_job_bridge_handle(&handle).await?;
        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{handle_id}"),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle_id.clone(),
                    },
                ),
            },
        )
        .await?;

        let stored_job = store
            .get_stream_job(tenant_id, instance_id, run_id, job_id)
            .await?
            .context("topic stream job record should exist after schedule")?;
        assert_eq!(stored_job.parsed_status(), Some(fabrik_throughput::StreamJobStatus::Running));

        let checkpoints =
            store.list_stream_job_bridge_checkpoints_for_handle_page(&handle_id, 10, 0).await?;
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(
            checkpoints[0].parsed_status(),
            Some(fabrik_throughput::StreamJobCheckpointStatus::Reached)
        );
        assert_eq!(checkpoints[0].checkpoint_name, stream_jobs::KEYED_ROLLUP_CHECKPOINT_NAME);

        let checkpoint_outbox = store
            .lease_workflow_event_outbox(
                "topic-stream-job-checkpoint-outbox",
                chrono::Duration::seconds(30),
                20,
                occurred_at + chrono::Duration::seconds(1),
            )
            .await?;
        assert!(
            checkpoint_outbox
                .iter()
                .any(|record| record.event_type == "StreamJobCheckpointReached")
        );
        assert!(!checkpoint_outbox.iter().any(|record| record.event_type == "StreamJobCompleted"));

        let query_requested_at = occurred_at + chrono::Duration::seconds(2);
        store
            .upsert_stream_job_bridge_query(&fabrik_store::StreamJobQueryRecord {
                protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
                workflow_event_id: Uuid::now_v7(),
                tenant_id: tenant_id.to_owned(),
                instance_id: instance_id.to_owned(),
                run_id: run_id.to_owned(),
                job_id: job_id.to_owned(),
                handle_id: handle_id.clone(),
                bridge_request_id: handle.bridge_request_id.clone(),
                query_id: "query-a".to_owned(),
                query_name: "accountTotals".to_owned(),
                query_args: Some(json!({ "key": "acct_1" })),
                consistency: "strong".to_owned(),
                status: fabrik_throughput::StreamJobQueryStatus::Requested.as_str().to_owned(),
                workflow_owner_epoch: Some(1),
                stream_owner_epoch: None,
                output: None,
                error: None,
                requested_at: query_requested_at,
                completed_at: None,
                accepted_at: None,
                cancelled_at: None,
                created_at: query_requested_at,
                updated_at: query_requested_at,
            })
            .await?;
        let identity = WorkflowIdentity::new(
            tenant_id,
            "payments-rollup",
            1,
            "artifact-a",
            instance_id,
            run_id,
            "throughput-runtime-test",
        );
        let query_payload = WorkflowEvent::StreamJobQueryRequested {
            job_id: job_id.to_owned(),
            query_id: "query-a".to_owned(),
            query_name: "accountTotals".to_owned(),
            args: Some(json!({ "key": "acct_1" })),
            consistency: "strong".to_owned(),
            state: None,
        };
        let mut query_event =
            EventEnvelope::new(query_payload.event_type(), identity, query_payload);
        query_event.occurred_at = query_requested_at;
        bridge::handle_bridge_event(app_state.clone(), query_event).await?;

        let stored_query = store
            .get_stream_job_callback_query("query-a")
            .await?
            .context("topic stream job query should be completed")?;
        assert_eq!(
            stored_query.parsed_status(),
            Some(fabrik_throughput::StreamJobQueryStatus::Completed)
        );
        assert_eq!(
            stored_query
                .output
                .as_ref()
                .and_then(|value| value.get("totalAmount"))
                .and_then(Value::as_f64),
            Some(5.0)
        );
        assert_eq!(
            stored_query
                .output
                .as_ref()
                .and_then(|value| value.get("consistency"))
                .and_then(Value::as_str),
            Some("strong")
        );

        let acct_1 = app_state
            .local_state
            .load_stream_job_view_state(&handle_id, "accountTotals", "acct_1")?
            .context("topic-backed acct_1 projection should exist")?;
        assert_eq!(acct_1.output["totalAmount"], json!(5.0));
        let runtime_state = app_state
            .local_state
            .load_stream_job_runtime_state(&handle_id)?
            .context("topic-backed runtime state should exist")?;
        assert_eq!(runtime_state.source_kind.as_deref(), Some("topic"));
        assert_eq!(runtime_state.source_name.as_deref(), Some(source_topic.topic_name.as_str()));
        assert!(
            runtime_state
                .source_cursors
                .iter()
                .all(|cursor| cursor.checkpoint_reached_at.is_some())
        );
        assert_eq!(runtime_state.source_partition_leases.len(), 1);
        assert_eq!(runtime_state.source_partition_leases[0].source_partition_id, 0);
        assert_eq!(
            runtime_state.source_partition_leases[0].owner_epoch,
            runtime_state.stream_owner_epoch
        );
        assert_eq!(runtime_state.source_partition_leases[0].owner_partition_id, 0);
        assert!(!runtime_state.source_partition_leases[0].lease_token.is_empty());

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn workflow_topic_stream_job_restores_source_progress_without_replaying_old_records()
    -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-topic-stream-job-restore-source-progress");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;
        let source_topic = JsonTopicConfig::new(
            redpanda.workflow_broker.brokers.clone(),
            format!("stream-source-restore-test-{}", Uuid::now_v7()),
            1,
        );
        let source_publisher = redpanda
            .connect_json_publisher::<Value>(&source_topic, "throughput-topic-stream-restore")
            .await?;
        source_publisher.publish(&json!({"accountId": "acct_1", "amount": 2.0}), "acct_1").await?;
        source_publisher.publish(&json!({"accountId": "acct_1", "amount": 3.0}), "acct_1").await?;

        let tenant_id = "tenant";
        let instance_id = "instance-stream-job-topic-restore";
        let run_id = "run-stream-job-topic-restore";
        let job_id = "job-stream-job-topic-restore";
        let occurred_at = Utc::now();
        let handle = topic_stream_job_handle(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            &source_topic.topic_name,
            fabrik_throughput::StreamJobBridgeHandleStatus::Admitted,
            None,
            None,
            occurred_at,
        );
        let handle_id = handle.handle_id.clone();
        store.upsert_stream_job_bridge_handle(&handle).await?;

        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{handle_id}"),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle_id.clone(),
                    },
                ),
            },
        )
        .await?;

        let initial_runtime_state = app_state
            .local_state
            .load_stream_job_runtime_state(&handle_id)?
            .context("initial topic runtime state should exist")?;
        assert_eq!(initial_runtime_state.source_cursors.len(), 1);
        assert_eq!(initial_runtime_state.source_cursors[0].next_offset, 2);
        assert_eq!(initial_runtime_state.source_cursors[0].initial_checkpoint_target_offset, 2);
        assert_eq!(initial_runtime_state.source_cursors[0].last_applied_offset, Some(1));
        assert_eq!(initial_runtime_state.source_cursors[0].last_high_watermark, Some(2));
        let initial_checkpoint = store
            .list_stream_job_bridge_checkpoints_for_handle_page(&handle_id, 10, 0)
            .await?
            .into_iter()
            .next()
            .context("initial topic checkpoint should exist")?;
        let checkpoint_snapshot = app_state.local_state.snapshot_checkpoint_value()?;

        let restored_state_dir =
            temp_path("throughput-topic-stream-job-restore-source-progress-restored");
        let restored_app_state =
            test_app_state(store.clone(), &redpanda, &restored_state_dir).await?;
        assert!(
            restored_app_state
                .local_state
                .restore_from_checkpoint_value_if_empty(checkpoint_snapshot)?
        );

        source_publisher.publish(&json!({"accountId": "acct_1", "amount": 7.0}), "acct_1").await?;

        let restored_runtime_state = {
            let mut state = None;
            for _ in 0..20 {
                bridge::reconcile_active_topic_stream_jobs(&restored_app_state, 64).await?;
                if let Some(candidate) = restored_app_state
                    .local_state
                    .load_stream_job_runtime_state(&handle_id)?
                    .filter(|runtime| {
                        runtime.source_cursors.iter().any(|cursor| {
                            cursor.last_applied_offset == Some(2)
                                && cursor.initial_checkpoint_target_offset >= 3
                        })
                    })
                {
                    state = Some(candidate);
                    break;
                }
                sleep(StdDuration::from_millis(100)).await;
            }
            state
                .context("restored runtime state should advance from the persisted source cursor")?
        };

        let cursor = restored_runtime_state
            .source_cursors
            .iter()
            .find(|cursor| cursor.source_partition_id == 0)
            .context("restored topic source cursor should exist")?;
        assert_eq!(cursor.next_offset, 3);
        assert_eq!(cursor.initial_checkpoint_target_offset, 3);
        assert_eq!(cursor.last_applied_offset, Some(2));
        assert_eq!(cursor.last_high_watermark, Some(3));
        assert!(cursor.checkpoint_reached_at.is_some());

        let acct_1 = restored_app_state
            .local_state
            .load_stream_job_view_state(&handle_id, "accountTotals", "acct_1")?
            .context("restored topic-backed acct_1 projection should exist")?;
        assert_eq!(acct_1.output["totalAmount"], json!(12.0));

        let query = fabrik_store::StreamJobQueryRecord {
            protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
            workflow_event_id: Uuid::now_v7(),
            tenant_id: tenant_id.to_owned(),
            instance_id: instance_id.to_owned(),
            run_id: run_id.to_owned(),
            job_id: job_id.to_owned(),
            handle_id: handle_id.clone(),
            bridge_request_id: handle.bridge_request_id.clone(),
            query_id: "query-restore-a".to_owned(),
            query_name: "accountTotals".to_owned(),
            query_args: Some(json!({ "key": "acct_1" })),
            consistency: "strong".to_owned(),
            status: fabrik_throughput::StreamJobQueryStatus::Requested.as_str().to_owned(),
            workflow_owner_epoch: Some(1),
            stream_owner_epoch: None,
            output: None,
            error: None,
            requested_at: occurred_at + chrono::Duration::seconds(3),
            completed_at: None,
            accepted_at: None,
            cancelled_at: None,
            created_at: occurred_at + chrono::Duration::seconds(3),
            updated_at: occurred_at + chrono::Duration::seconds(3),
        };
        let strong_output = stream_jobs::build_stream_job_query_output_on_local_state(
            &restored_app_state.local_state,
            &handle,
            &query,
        )?
        .context("restored strong query should return output")?;
        assert_eq!(strong_output["totalAmount"], json!(12.0));
        assert_eq!(strong_output["consistency"], json!("strong"));

        let restored_checkpoint =
            store.list_stream_job_bridge_checkpoints_for_handle_page(&handle_id, 10, 0).await?;
        assert_eq!(restored_checkpoint.len(), 1);
        assert_eq!(
            restored_checkpoint[0].parsed_status(),
            Some(fabrik_throughput::StreamJobCheckpointStatus::Reached)
        );
        assert!(
            restored_checkpoint[0]
                .reached_at
                .zip(initial_checkpoint.reached_at)
                .is_some_and(|(updated, initial)| updated >= initial)
        );

        fs::remove_dir_all(state_dir).ok();
        fs::remove_dir_all(restored_state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn workflow_topic_stream_job_advances_recurring_checkpoint_across_source_partitions()
    -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-topic-stream-job-recurring-checkpoint");
        let app_state =
            test_app_state_with_partitions(store.clone(), &redpanda, &state_dir, 2).await?;
        let shard_workers = ShardWorkerPool::new(app_state.clone(), &[0, 1]);
        let _ = app_state.shard_workers.set(shard_workers);
        let source_topic = JsonTopicConfig::new(
            redpanda.workflow_broker.brokers.clone(),
            format!("stream-source-recurring-checkpoint-{}", Uuid::now_v7()),
            2,
        );
        let source_publisher = redpanda
            .connect_json_publisher::<Value>(&source_topic, "throughput-topic-stream-recurring")
            .await?;
        let (first_key, first_source_partition, second_key, second_source_partition) =
            distinct_topic_partition_keys(2);
        source_publisher
            .publish(&json!({"accountId": first_key, "amount": 2.0}), &first_key)
            .await?;
        source_publisher
            .publish(&json!({"accountId": second_key, "amount": 3.0}), &second_key)
            .await?;

        let tenant_id = "tenant";
        let instance_id = "instance-stream-job-topic-recurring";
        let run_id = "run-stream-job-topic-recurring";
        let job_id = "job-stream-job-topic-recurring";
        let occurred_at = Utc::now();
        let handle = topic_stream_job_handle(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            &source_topic.topic_name,
            fabrik_throughput::StreamJobBridgeHandleStatus::Admitted,
            None,
            None,
            occurred_at,
        );
        let handle_id = handle.handle_id.clone();
        store.upsert_stream_job_bridge_handle(&handle).await?;

        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{handle_id}"),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle_id.clone(),
                    },
                ),
            },
        )
        .await?;

        let initial_runtime_state = app_state
            .local_state
            .load_stream_job_runtime_state(&handle_id)?
            .context("initial topic runtime state should exist")?;
        assert_eq!(initial_runtime_state.checkpoint_sequence, 1);
        assert_eq!(initial_runtime_state.source_partition_leases.len(), 2);
        for lease in &initial_runtime_state.source_partition_leases {
            assert_eq!(
                lease.owner_partition_id,
                stream_jobs::source_owner_partition_for_source_partition(
                    lease.source_partition_id,
                    2
                )
            );
            assert!(!lease.lease_token.is_empty());
        }

        let initial_checkpoints =
            store.list_stream_job_bridge_checkpoints_for_handle_page(&handle_id, 10, 0).await?;
        assert_eq!(initial_checkpoints.len(), 1);
        assert_eq!(initial_checkpoints[0].checkpoint_sequence, Some(1));

        source_publisher
            .publish(&json!({"accountId": first_key, "amount": 5.0}), &first_key)
            .await?;
        source_publisher
            .publish(&json!({"accountId": second_key, "amount": 7.0}), &second_key)
            .await?;

        let recurring_checkpoints = {
            let mut checkpoints = Vec::new();
            for _ in 0..30 {
                bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;
                let current = store
                    .list_stream_job_bridge_checkpoints_for_handle_page(&handle_id, 10, 0)
                    .await?;
                checkpoints = current.clone();
                if current.len() >= 2
                    && current.iter().any(|checkpoint| checkpoint.checkpoint_sequence == Some(2))
                {
                    break;
                }
                sleep(StdDuration::from_millis(100)).await;
            }
            checkpoints
        };
        assert!(
            recurring_checkpoints
                .iter()
                .any(|checkpoint| checkpoint.checkpoint_sequence == Some(2)),
            "runtime sequence: {:?}, checkpoint sequences: {:?}",
            app_state
                .local_state
                .load_stream_job_runtime_state(&handle_id)?
                .map(|state| state.checkpoint_sequence),
            recurring_checkpoints
                .iter()
                .map(|checkpoint| checkpoint.checkpoint_sequence)
                .collect::<Vec<_>>()
        );

        let refreshed_runtime_state = app_state
            .local_state
            .load_stream_job_runtime_state(&handle_id)?
            .context("refreshed topic runtime state should exist")?;
        assert_eq!(refreshed_runtime_state.checkpoint_sequence, 2);
        assert!(
            refreshed_runtime_state
                .source_cursors
                .iter()
                .all(|cursor| cursor.checkpoint_reached_at.is_some())
        );
        assert!(
            refreshed_runtime_state
                .source_cursors
                .iter()
                .find(|cursor| cursor.source_partition_id == first_source_partition)
                .is_some_and(|cursor| cursor.last_applied_offset.is_some())
        );
        assert!(
            refreshed_runtime_state
                .source_cursors
                .iter()
                .find(|cursor| cursor.source_partition_id == second_source_partition)
                .is_some_and(|cursor| cursor.last_applied_offset.is_some())
        );

        let first_total = app_state
            .local_state
            .load_stream_job_view_state(&handle_id, "accountTotals", &first_key)?
            .context("first topic partition view should exist")?;
        let second_total = app_state
            .local_state
            .load_stream_job_view_state(&handle_id, "accountTotals", &second_key)?
            .context("second topic partition view should exist")?;
        assert_eq!(first_total.output["totalAmount"], json!(7.0));
        assert_eq!(second_total.output["totalAmount"], json!(10.0));

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn workflow_topic_aggregate_v2_stream_job_answers_strong_average_query() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-topic-aggregate-v2-stream-job");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;
        let source_topic = JsonTopicConfig::new(
            redpanda.workflow_broker.brokers.clone(),
            format!("stream-aggregate-v2-source-test-{}", Uuid::now_v7()),
            1,
        );
        let source_publisher = redpanda
            .connect_json_publisher::<Value>(&source_topic, "throughput-topic-aggregate-v2-source")
            .await?;
        source_publisher.publish(&json!({"accountId": "acct_1", "risk": 0.9}), "acct_1").await?;
        source_publisher.publish(&json!({"accountId": "acct_1", "risk": 0.3}), "acct_1").await?;

        let tenant_id = "tenant";
        let instance_id = "instance-stream-job-topic-aggregate-v2";
        let run_id = "run-stream-job-topic-aggregate-v2";
        let job_id = "job-stream-job-topic-aggregate-v2";
        let occurred_at = Utc::now();
        let handle = aggregate_v2_topic_stream_job_handle(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            &source_topic.topic_name,
            fabrik_throughput::StreamJobBridgeHandleStatus::Admitted,
            None,
            None,
            occurred_at,
        );
        let handle_id = handle.handle_id.clone();
        store.upsert_stream_job_bridge_handle(&handle).await?;

        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{handle_id}"),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle_id.clone(),
                    },
                ),
            },
        )
        .await?;

        let stored_job = store
            .get_stream_job(tenant_id, instance_id, run_id, job_id)
            .await?
            .context("aggregate_v2 topic stream job record should exist after schedule")?;
        assert_eq!(stored_job.parsed_status(), Some(fabrik_throughput::StreamJobStatus::Running));

        let checkpoints =
            store.list_stream_job_bridge_checkpoints_for_handle_page(&handle_id, 10, 0).await?;
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(
            checkpoints[0].parsed_status(),
            Some(fabrik_throughput::StreamJobCheckpointStatus::Reached)
        );
        assert_eq!(checkpoints[0].checkpoint_name, "initial-risk-ready");

        let query_requested_at = occurred_at + chrono::Duration::seconds(2);
        store
            .upsert_stream_job_bridge_query(&fabrik_store::StreamJobQueryRecord {
                protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
                workflow_event_id: Uuid::now_v7(),
                tenant_id: tenant_id.to_owned(),
                instance_id: instance_id.to_owned(),
                run_id: run_id.to_owned(),
                job_id: job_id.to_owned(),
                handle_id: handle_id.clone(),
                bridge_request_id: handle.bridge_request_id.clone(),
                query_id: "query-risk-a".to_owned(),
                query_name: "riskScoresByKey".to_owned(),
                query_args: Some(json!({ "key": "acct_1" })),
                consistency: "strong".to_owned(),
                status: fabrik_throughput::StreamJobQueryStatus::Requested.as_str().to_owned(),
                workflow_owner_epoch: Some(1),
                stream_owner_epoch: None,
                output: None,
                error: None,
                requested_at: query_requested_at,
                completed_at: None,
                accepted_at: None,
                cancelled_at: None,
                created_at: query_requested_at,
                updated_at: query_requested_at,
            })
            .await?;
        let identity = WorkflowIdentity::new(
            tenant_id,
            "fraud-detector",
            1,
            "artifact-a",
            instance_id,
            run_id,
            "throughput-runtime-test",
        );
        let query_payload = WorkflowEvent::StreamJobQueryRequested {
            job_id: job_id.to_owned(),
            query_id: "query-risk-a".to_owned(),
            query_name: "riskScoresByKey".to_owned(),
            args: Some(json!({ "key": "acct_1" })),
            consistency: "strong".to_owned(),
            state: None,
        };
        let mut query_event =
            EventEnvelope::new(query_payload.event_type(), identity, query_payload);
        query_event.occurred_at = query_requested_at;
        bridge::handle_bridge_event(app_state.clone(), query_event).await?;

        let stored_query = store
            .get_stream_job_callback_query("query-risk-a")
            .await?
            .context("aggregate_v2 topic stream job query should be completed")?;
        assert_eq!(
            stored_query.parsed_status(),
            Some(fabrik_throughput::StreamJobQueryStatus::Completed)
        );
        assert_eq!(
            stored_query
                .output
                .as_ref()
                .and_then(|value| value.get("avgRisk"))
                .and_then(Value::as_f64),
            Some(0.6)
        );
        assert_eq!(
            stored_query
                .output
                .as_ref()
                .and_then(|value| value.get("consistency"))
                .and_then(Value::as_str),
            Some("strong")
        );
        assert!(stored_query.output.as_ref().and_then(|value| value.get("_stream_sum")).is_none());
        assert!(
            stored_query.output.as_ref().and_then(|value| value.get("_stream_count")).is_none()
        );

        let raw_view = app_state
            .local_state
            .load_stream_job_view_state(&handle_id, "riskScores", "acct_1")?
            .context("aggregate_v2 topic-backed acct_1 projection should exist")?;
        assert_eq!(raw_view.output["avgRisk"], json!(0.6));
        assert_eq!(raw_view.output["_stream_sum"], json!(1.2));
        assert_eq!(raw_view.output["_stream_count"], json!(2));

        let runtime_state = app_state
            .local_state
            .load_stream_job_runtime_state(&handle_id)?
            .context("aggregate_v2 topic-backed runtime state should exist")?;
        assert_eq!(runtime_state.source_kind.as_deref(), Some("topic"));
        assert_eq!(runtime_state.source_name.as_deref(), Some(source_topic.topic_name.as_str()));
        assert!(
            runtime_state
                .source_cursors
                .iter()
                .all(|cursor| cursor.checkpoint_reached_at.is_some())
        );

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn workflow_topic_windowed_aggregate_v2_stream_job_answers_window_query() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-topic-windowed-aggregate-v2-stream-job");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;
        let source_topic = JsonTopicConfig::new(
            redpanda.workflow_broker.brokers.clone(),
            format!("stream-windowed-aggregate-v2-source-test-{}", Uuid::now_v7()),
            1,
        );
        let source_publisher = redpanda
            .connect_json_publisher::<Value>(
                &source_topic,
                "throughput-topic-windowed-aggregate-v2-source",
            )
            .await?;
        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.9, "eventTime": "2026-03-15T10:00:05Z"}),
                "acct_1",
            )
            .await?;
        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.3, "eventTime": "2026-03-15T10:00:20Z"}),
                "acct_1",
            )
            .await?;
        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.6, "eventTime": "2026-03-15T10:01:05Z"}),
                "acct_1",
            )
            .await?;

        let tenant_id = "tenant";
        let instance_id = "instance-stream-job-topic-windowed-aggregate-v2";
        let run_id = "run-stream-job-topic-windowed-aggregate-v2";
        let job_id = "job-stream-job-topic-windowed-aggregate-v2";
        let occurred_at = Utc::now();
        let handle = aggregate_v2_topic_window_stream_job_handle(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            &source_topic.topic_name,
            fabrik_throughput::StreamJobBridgeHandleStatus::Admitted,
            occurred_at,
        );
        let handle_id = handle.handle_id.clone();
        store.upsert_stream_job_bridge_handle(&handle).await?;
        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{handle_id}"),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle_id.clone(),
                    },
                ),
            },
        )
        .await?;

        let checkpoints =
            store.list_stream_job_bridge_checkpoints_for_handle_page(&handle_id, 10, 0).await?;
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(
            checkpoints[0].parsed_status(),
            Some(fabrik_throughput::StreamJobCheckpointStatus::Reached)
        );

        let query_requested_at = occurred_at + chrono::Duration::seconds(2);
        store
            .upsert_stream_job_bridge_query(&fabrik_store::StreamJobQueryRecord {
                protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
                workflow_event_id: Uuid::now_v7(),
                tenant_id: tenant_id.to_owned(),
                instance_id: instance_id.to_owned(),
                run_id: run_id.to_owned(),
                job_id: job_id.to_owned(),
                handle_id: handle_id.clone(),
                bridge_request_id: handle.bridge_request_id.clone(),
                query_id: "query-window-a".to_owned(),
                query_name: "riskScoresByKey".to_owned(),
                query_args: Some(json!({
                    "key": "acct_1",
                    "windowStart": "2026-03-15T10:00:00+00:00"
                })),
                consistency: "strong".to_owned(),
                status: fabrik_throughput::StreamJobQueryStatus::Requested.as_str().to_owned(),
                workflow_owner_epoch: Some(1),
                stream_owner_epoch: None,
                output: None,
                error: None,
                requested_at: query_requested_at,
                completed_at: None,
                accepted_at: None,
                cancelled_at: None,
                created_at: query_requested_at,
                updated_at: query_requested_at,
            })
            .await?;
        let identity = WorkflowIdentity::new(
            tenant_id,
            "fraud-detector",
            1,
            "artifact-a",
            instance_id,
            run_id,
            "throughput-runtime-test",
        );
        let query_payload = WorkflowEvent::StreamJobQueryRequested {
            job_id: job_id.to_owned(),
            query_id: "query-window-a".to_owned(),
            query_name: "riskScoresByKey".to_owned(),
            args: Some(json!({
                "key": "acct_1",
                "windowStart": "2026-03-15T10:00:00+00:00"
            })),
            consistency: "strong".to_owned(),
            state: None,
        };
        let mut query_event =
            EventEnvelope::new(query_payload.event_type(), identity, query_payload);
        query_event.occurred_at = query_requested_at;
        bridge::handle_bridge_event(app_state.clone(), query_event).await?;

        let stored_query = store
            .get_stream_job_callback_query("query-window-a")
            .await?
            .context("windowed aggregate_v2 topic query should be completed")?;
        assert_eq!(
            stored_query.parsed_status(),
            Some(fabrik_throughput::StreamJobQueryStatus::Completed)
        );
        assert_eq!(
            stored_query
                .output
                .as_ref()
                .and_then(|value| value.get("avgRisk"))
                .and_then(Value::as_f64),
            Some(0.6)
        );
        assert_eq!(
            stored_query
                .output
                .as_ref()
                .and_then(|value| value.get("windowStart"))
                .and_then(Value::as_str),
            Some("2026-03-15T10:00:00+00:00")
        );
        assert_eq!(
            stored_query
                .output
                .as_ref()
                .and_then(|value| value.get("windowEnd"))
                .and_then(Value::as_str),
            Some("2026-03-15T10:01:00+00:00")
        );

        let runtime_state = app_state
            .local_state
            .load_stream_job_runtime_state(&handle_id)?
            .context("windowed aggregate_v2 topic runtime state should exist")?;
        assert_eq!(runtime_state.source_kind.as_deref(), Some("topic"));
        assert!(
            runtime_state
                .source_cursors
                .iter()
                .all(|cursor| cursor.checkpoint_reached_at.is_some())
        );
        assert_eq!(runtime_state.source_partition_leases.len(), 1);
        assert_eq!(runtime_state.source_partition_leases[0].source_partition_id, 0);
        assert_eq!(
            runtime_state.source_partition_leases[0].owner_epoch,
            runtime_state.stream_owner_epoch
        );
        assert_eq!(runtime_state.source_partition_leases[0].owner_partition_id, 0);
        assert!(!runtime_state.source_partition_leases[0].lease_token.is_empty());

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn workflow_topic_windowed_aggregate_v2_stream_job_waits_for_closed_window_before_checkpoint()
    -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-topic-windowed-aggregate-v2-empty-checkpoint");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;
        let source_topic = JsonTopicConfig::new(
            redpanda.workflow_broker.brokers.clone(),
            format!("stream-windowed-aggregate-v2-empty-{}", Uuid::now_v7()),
            1,
        );

        let tenant_id = "tenant";
        let instance_id = "instance-stream-job-topic-windowed-aggregate-v2-empty";
        let run_id = "run-stream-job-topic-windowed-aggregate-v2-empty";
        let job_id = "job-stream-job-topic-windowed-aggregate-v2-empty";
        let occurred_at = Utc::now();
        let handle = aggregate_v2_topic_window_stream_job_handle(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            &source_topic.topic_name,
            fabrik_throughput::StreamJobBridgeHandleStatus::Admitted,
            occurred_at,
        );
        let handle_id = handle.handle_id.clone();
        store.upsert_stream_job_bridge_handle(&handle).await?;

        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{handle_id}"),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle_id.clone(),
                    },
                ),
            },
        )
        .await?;

        let initial_checkpoints =
            store.list_stream_job_bridge_checkpoints_for_handle_page(&handle_id, 10, 0).await?;
        assert!(initial_checkpoints.is_empty());
        let initial_runtime_state = app_state
            .local_state
            .load_stream_job_runtime_state(&handle_id)?
            .context("windowed aggregate_v2 runtime state should exist after empty schedule")?;
        assert!(
            initial_runtime_state
                .source_cursors
                .iter()
                .all(|cursor| cursor.checkpoint_reached_at.is_none())
        );
        assert_eq!(initial_runtime_state.source_partition_leases.len(), 1);
        assert_eq!(initial_runtime_state.source_partition_leases[0].source_partition_id, 0);
        assert_eq!(
            initial_runtime_state.source_partition_leases[0].owner_epoch,
            initial_runtime_state.stream_owner_epoch
        );
        assert_eq!(initial_runtime_state.source_partition_leases[0].owner_partition_id, 0);
        assert!(!initial_runtime_state.source_partition_leases[0].lease_token.is_empty());

        let source_publisher = redpanda
            .connect_json_publisher::<Value>(
                &source_topic,
                "throughput-topic-windowed-aggregate-v2-empty-source",
            )
            .await?;
        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.9, "eventTime": "2026-03-15T10:00:05Z"}),
                "acct_1",
            )
            .await?;

        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        let checkpoints =
            store.list_stream_job_bridge_checkpoints_for_handle_page(&handle_id, 10, 0).await?;
        assert!(checkpoints.is_empty());
        let open_window_runtime_state =
            app_state.local_state.load_stream_job_runtime_state(&handle_id)?.context(
                "windowed aggregate_v2 runtime state should exist after open-window reconcile",
            )?;
        assert!(
            open_window_runtime_state
                .source_cursors
                .iter()
                .all(|cursor| cursor.checkpoint_reached_at.is_none())
        );
        let open_cursor = open_window_runtime_state
            .source_cursors
            .iter()
            .find(|cursor| cursor.source_partition_id == 0)
            .context(
                "windowed aggregate_v2 source cursor should exist after open-window reconcile",
            )?;
        assert_eq!(
            open_cursor.last_event_time_watermark.map(|value| value.to_rfc3339()),
            Some("2026-03-15T10:00:05+00:00".to_owned())
        );
        assert_eq!(
            open_cursor
                .pending_window_ends
                .iter()
                .map(|value| value.to_rfc3339())
                .collect::<Vec<_>>(),
            vec!["2026-03-15T10:01:00+00:00".to_owned()]
        );
        assert!(open_cursor.last_closed_window_end.is_none());

        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.7, "eventTime": "2026-03-15T10:01:05Z"}),
                "acct_1",
            )
            .await?;

        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        let checkpoints =
            store.list_stream_job_bridge_checkpoints_for_handle_page(&handle_id, 10, 0).await?;
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(
            checkpoints[0].parsed_status(),
            Some(fabrik_throughput::StreamJobCheckpointStatus::Reached)
        );
        let runtime_state = app_state
            .local_state
            .load_stream_job_runtime_state(&handle_id)?
            .context("windowed aggregate_v2 runtime state should exist after reconcile")?;
        assert!(
            runtime_state
                .source_cursors
                .iter()
                .all(|cursor| cursor.checkpoint_reached_at.is_some())
        );
        let closed_cursor = runtime_state
            .source_cursors
            .iter()
            .find(|cursor| cursor.source_partition_id == 0)
            .context(
                "windowed aggregate_v2 source cursor should exist after closed-window reconcile",
            )?;
        assert_eq!(
            closed_cursor.last_closed_window_end.map(|value| value.to_rfc3339()),
            Some("2026-03-15T10:01:00+00:00".to_owned())
        );
        assert_eq!(
            closed_cursor
                .pending_window_ends
                .iter()
                .map(|value| value.to_rfc3339())
                .collect::<Vec<_>>(),
            vec!["2026-03-15T10:02:00+00:00".to_owned()]
        );
        assert_eq!(runtime_state.source_partition_leases.len(), 1);
        assert_eq!(runtime_state.source_partition_leases[0].source_partition_id, 0);
        assert_eq!(
            runtime_state.source_partition_leases[0].owner_epoch,
            runtime_state.stream_owner_epoch
        );
        assert_eq!(runtime_state.source_partition_leases[0].owner_partition_id, 0);
        assert!(!runtime_state.source_partition_leases[0].lease_token.is_empty());

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn workflow_topic_windowed_aggregate_v2_stream_job_closes_idle_window_on_owner_timer()
    -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-topic-windowed-aggregate-v2-idle-timer");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;
        let source_topic = JsonTopicConfig::new(
            redpanda.workflow_broker.brokers.clone(),
            format!("stream-windowed-aggregate-v2-idle-{}", Uuid::now_v7()),
            1,
        );
        let source_publisher = redpanda
            .connect_json_publisher::<Value>(
                &source_topic,
                "throughput-topic-windowed-aggregate-v2-idle-source",
            )
            .await?;

        let tenant_id = "tenant";
        let instance_id = "instance-stream-job-topic-windowed-aggregate-v2-idle";
        let run_id = "run-stream-job-topic-windowed-aggregate-v2-idle";
        let job_id = "job-stream-job-topic-windowed-aggregate-v2-idle";
        let occurred_at = Utc::now();
        let handle = aggregate_v2_topic_window_stream_job_handle(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            &source_topic.topic_name,
            fabrik_throughput::StreamJobBridgeHandleStatus::Admitted,
            occurred_at,
        );
        let handle_id = handle.handle_id.clone();
        store.upsert_stream_job_bridge_handle(&handle).await?;
        let idle_event_time = Utc::now() - chrono::Duration::minutes(2);
        let idle_window_start =
            DateTime::<Utc>::from_timestamp(idle_event_time.timestamp().div_euclid(60) * 60, 0)
                .context("idle timer window start should be representable")?;
        let idle_window_end = idle_window_start + chrono::Duration::minutes(1);

        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{handle_id}"),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle_id.clone(),
                    },
                ),
            },
        )
        .await?;

        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.9, "eventTime": idle_event_time.to_rfc3339()}),
                "acct_1",
            )
            .await?;

        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        let initial_checkpoints =
            store.list_stream_job_bridge_checkpoints_for_handle_page(&handle_id, 10, 0).await?;
        assert!(initial_checkpoints.is_empty());
        let open_runtime_state =
            app_state.local_state.load_stream_job_runtime_state(&handle_id)?.context(
                "windowed aggregate_v2 idle-timer runtime state should exist after first reconcile",
            )?;
        let open_cursor = open_runtime_state
            .source_cursors
            .iter()
            .find(|cursor| cursor.source_partition_id == 0)
            .context(
                "windowed aggregate_v2 idle-timer source cursor should exist after first reconcile",
            )?;
        assert!(open_cursor.checkpoint_reached_at.is_none());
        assert!(open_cursor.last_closed_window_end.is_none());
        assert_eq!(
            open_cursor
                .pending_window_ends
                .iter()
                .map(|value| value.to_rfc3339())
                .collect::<Vec<_>>(),
            vec![idle_window_end.to_rfc3339()]
        );

        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        let checkpoints =
            store.list_stream_job_bridge_checkpoints_for_handle_page(&handle_id, 10, 0).await?;
        assert_eq!(checkpoints.len(), 1);
        let closed_runtime_state =
            app_state.local_state.load_stream_job_runtime_state(&handle_id)?.context(
                "windowed aggregate_v2 idle-timer runtime state should exist after timer reconcile",
            )?;
        let closed_cursor = closed_runtime_state
            .source_cursors
            .iter()
            .find(|cursor| cursor.source_partition_id == 0)
            .context(
                "windowed aggregate_v2 idle-timer source cursor should exist after timer reconcile",
            )?;
        assert!(closed_cursor.checkpoint_reached_at.is_some());
        assert_eq!(
            closed_cursor.last_closed_window_end.map(|value| value.to_rfc3339()),
            Some(idle_window_end.to_rfc3339())
        );
        assert!(closed_cursor.pending_window_ends.is_empty());

        let window_key =
            stream_jobs::windowed_logical_key("acct_1", &idle_window_start.to_rfc3339());
        let raw_view = app_state
            .local_state
            .load_stream_job_view_state(&handle_id, "riskScores", &window_key)?
            .context("idle-timer windowed aggregate_v2 view should exist")?;
        assert_eq!(raw_view.output["avgRisk"], json!(0.9));
        assert_eq!(raw_view.output["windowStart"], json!(idle_window_start.to_rfc3339()));
        assert_eq!(raw_view.output["windowEnd"], json!(idle_window_end.to_rfc3339()));

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn workflow_topic_windowed_aggregate_v2_stream_job_respects_allowed_lateness_before_checkpoint()
    -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-topic-windowed-aggregate-v2-allowed-lateness");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;
        let source_topic = JsonTopicConfig::new(
            redpanda.workflow_broker.brokers.clone(),
            format!("stream-windowed-aggregate-v2-late-{}", Uuid::now_v7()),
            1,
        );
        let tenant_id = "tenant";
        let instance_id = "instance-stream-job-topic-windowed-aggregate-v2-late";
        let run_id = "run-stream-job-topic-windowed-aggregate-v2-late";
        let job_id = "job-stream-job-topic-windowed-aggregate-v2-late";
        let occurred_at = Utc::now();
        let handle = aggregate_v2_topic_window_stream_job_handle_with_allowed_lateness(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            &source_topic.topic_name,
            fabrik_throughput::StreamJobBridgeHandleStatus::Admitted,
            Some("30s"),
            occurred_at,
        );
        let handle_id = handle.handle_id.clone();
        store.upsert_stream_job_bridge_handle(&handle).await?;
        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{handle_id}"),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle_id.clone(),
                    },
                ),
            },
        )
        .await?;

        let source_publisher = redpanda
            .connect_json_publisher::<Value>(
                &source_topic,
                "throughput-topic-windowed-aggregate-v2-late-source",
            )
            .await?;
        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.9, "eventTime": "2026-03-15T10:00:55Z"}),
                "acct_1",
            )
            .await?;
        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.8, "eventTime": "2026-03-15T10:01:05Z"}),
                "acct_1",
            )
            .await?;

        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        let checkpoints =
            store.list_stream_job_bridge_checkpoints_for_handle_page(&handle_id, 10, 0).await?;
        assert!(checkpoints.is_empty());
        let delayed_runtime_state = app_state
            .local_state
            .load_stream_job_runtime_state(&handle_id)?
            .context("windowed aggregate_v2 runtime state should exist after delayed reconcile")?;
        let delayed_cursor = delayed_runtime_state
            .source_cursors
            .iter()
            .find(|cursor| cursor.source_partition_id == 0)
            .context("windowed aggregate_v2 delayed source cursor should exist")?;
        assert!(
            delayed_cursor
                .last_event_time_watermark
                .is_some_and(|value| value.to_rfc3339() == "2026-03-15T10:01:05+00:00")
        );
        assert!(delayed_cursor.last_closed_window_end.is_none());

        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.7, "eventTime": "2026-03-15T10:01:35Z"}),
                "acct_1",
            )
            .await?;

        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        let checkpoints =
            store.list_stream_job_bridge_checkpoints_for_handle_page(&handle_id, 10, 0).await?;
        assert_eq!(checkpoints.len(), 1);
        let late_runtime_state =
            app_state.local_state.load_stream_job_runtime_state(&handle_id)?.context(
                "windowed aggregate_v2 runtime state should exist after lateness reconcile",
            )?;
        let late_cursor = late_runtime_state
            .source_cursors
            .iter()
            .find(|cursor| cursor.source_partition_id == 0)
            .context("windowed aggregate_v2 late source cursor should exist")?;
        assert!(
            late_cursor
                .last_closed_window_end
                .is_some_and(|value| value.to_rfc3339() == "2026-03-15T10:01:00+00:00")
        );

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn workflow_topic_windowed_aggregate_v2_stream_job_drops_late_events_after_window_closes()
    -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-topic-windowed-aggregate-v2-drop-late");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;
        let source_topic = JsonTopicConfig::new(
            redpanda.workflow_broker.brokers.clone(),
            format!("stream-windowed-aggregate-v2-drop-late-{}", Uuid::now_v7()),
            1,
        );
        let tenant_id = "tenant";
        let instance_id = "instance-stream-job-topic-windowed-aggregate-v2-drop-late";
        let run_id = "run-stream-job-topic-windowed-aggregate-v2-drop-late";
        let job_id = "job-stream-job-topic-windowed-aggregate-v2-drop-late";
        let occurred_at = Utc::now();
        let handle = aggregate_v2_topic_window_stream_job_handle_with_allowed_lateness(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            &source_topic.topic_name,
            fabrik_throughput::StreamJobBridgeHandleStatus::Admitted,
            Some("30s"),
            occurred_at,
        );
        let handle_id = handle.handle_id.clone();
        store.upsert_stream_job_bridge_handle(&handle).await?;
        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{handle_id}"),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle_id.clone(),
                    },
                ),
            },
        )
        .await?;

        let source_publisher = redpanda
            .connect_json_publisher::<Value>(
                &source_topic,
                "throughput-topic-windowed-aggregate-v2-drop-late-source",
            )
            .await?;
        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.9, "eventTime": "2026-03-15T10:00:55Z"}),
                "acct_1",
            )
            .await?;
        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.8, "eventTime": "2026-03-15T10:01:35Z"}),
                "acct_1",
            )
            .await?;
        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.1, "eventTime": "2026-03-15T10:00:20Z"}),
                "acct_1",
            )
            .await?;
        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        let query_requested_at = occurred_at + chrono::Duration::seconds(3);
        store
            .upsert_stream_job_bridge_query(&fabrik_store::StreamJobQueryRecord {
                protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
                workflow_event_id: Uuid::now_v7(),
                tenant_id: tenant_id.to_owned(),
                instance_id: instance_id.to_owned(),
                run_id: run_id.to_owned(),
                job_id: job_id.to_owned(),
                handle_id: handle_id.clone(),
                bridge_request_id: handle.bridge_request_id.clone(),
                query_id: "query-late-window-a".to_owned(),
                query_name: "riskScoresByKey".to_owned(),
                query_args: Some(json!({
                    "key": "acct_1",
                    "windowStart": "2026-03-15T10:00:00+00:00"
                })),
                consistency: "strong".to_owned(),
                status: fabrik_throughput::StreamJobQueryStatus::Requested.as_str().to_owned(),
                workflow_owner_epoch: Some(1),
                stream_owner_epoch: None,
                output: None,
                error: None,
                requested_at: query_requested_at,
                completed_at: None,
                accepted_at: None,
                cancelled_at: None,
                created_at: query_requested_at,
                updated_at: query_requested_at,
            })
            .await?;
        let identity = WorkflowIdentity::new(
            tenant_id,
            "fraud-detector",
            1,
            "artifact-a",
            instance_id,
            run_id,
            "throughput-runtime-test",
        );
        let query_payload = WorkflowEvent::StreamJobQueryRequested {
            job_id: job_id.to_owned(),
            query_id: "query-late-window-a".to_owned(),
            query_name: "riskScoresByKey".to_owned(),
            args: Some(json!({
                "key": "acct_1",
                "windowStart": "2026-03-15T10:00:00+00:00"
            })),
            consistency: "strong".to_owned(),
            state: None,
        };
        let mut query_event =
            EventEnvelope::new(query_payload.event_type(), identity, query_payload);
        query_event.occurred_at = query_requested_at;
        bridge::handle_bridge_event(app_state.clone(), query_event).await?;

        let stored_query = store
            .get_stream_job_callback_query("query-late-window-a")
            .await?
            .context("late-window query should be completed")?;
        assert_eq!(
            stored_query.parsed_status(),
            Some(fabrik_throughput::StreamJobQueryStatus::Completed)
        );
        assert_eq!(
            stored_query
                .output
                .as_ref()
                .and_then(|value| value.get("avgRisk"))
                .and_then(Value::as_f64),
            Some(0.9)
        );

        let runtime_query_requested_at = occurred_at + chrono::Duration::seconds(4);
        store
            .upsert_stream_job_bridge_query(&fabrik_store::StreamJobQueryRecord {
                protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
                workflow_event_id: Uuid::now_v7(),
                tenant_id: tenant_id.to_owned(),
                instance_id: instance_id.to_owned(),
                run_id: run_id.to_owned(),
                job_id: job_id.to_owned(),
                handle_id: handle_id.clone(),
                bridge_request_id: handle.bridge_request_id.clone(),
                query_id: "query-late-runtime-a".to_owned(),
                query_name: stream_jobs::STREAM_JOB_RUNTIME_STATS_QUERY_NAME.to_owned(),
                query_args: Some(json!({ "sourcePartitionId": 0 })),
                consistency: "strong".to_owned(),
                status: fabrik_throughput::StreamJobQueryStatus::Requested.as_str().to_owned(),
                workflow_owner_epoch: Some(1),
                stream_owner_epoch: None,
                output: None,
                error: None,
                requested_at: runtime_query_requested_at,
                completed_at: None,
                accepted_at: None,
                cancelled_at: None,
                created_at: runtime_query_requested_at,
                updated_at: runtime_query_requested_at,
            })
            .await?;
        let runtime_query_payload = WorkflowEvent::StreamJobQueryRequested {
            job_id: job_id.to_owned(),
            query_id: "query-late-runtime-a".to_owned(),
            query_name: stream_jobs::STREAM_JOB_RUNTIME_STATS_QUERY_NAME.to_owned(),
            args: Some(json!({ "sourcePartitionId": 0 })),
            consistency: "strong".to_owned(),
            state: None,
        };
        let runtime_identity = WorkflowIdentity::new(
            tenant_id,
            "fraud-detector",
            1,
            "artifact-a",
            instance_id,
            run_id,
            "throughput-runtime-test",
        );
        let mut runtime_query_event = EventEnvelope::new(
            runtime_query_payload.event_type(),
            runtime_identity,
            runtime_query_payload,
        );
        runtime_query_event.occurred_at = runtime_query_requested_at;
        bridge::handle_bridge_event(app_state.clone(), runtime_query_event).await?;

        let runtime_query = store
            .get_stream_job_callback_query("query-late-runtime-a")
            .await?
            .context("late-runtime query should be completed")?;
        assert_eq!(
            runtime_query.parsed_status(),
            Some(fabrik_throughput::StreamJobQueryStatus::Completed)
        );
        assert_eq!(
            runtime_query
                .output
                .as_ref()
                .and_then(|value| value.get("sourceCursors"))
                .and_then(Value::as_array)
                .and_then(|cursors| cursors.first())
                .and_then(|cursor| cursor.get("droppedLateEventCount"))
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            runtime_query
                .output
                .as_ref()
                .and_then(|value| value.get("sourceCursors"))
                .and_then(Value::as_array)
                .and_then(|cursors| cursors.first())
                .and_then(|cursor| cursor.get("lastDroppedLateEventAt"))
                .and_then(Value::as_str),
            Some("2026-03-15T10:00:20+00:00")
        );
        assert_eq!(
            runtime_query
                .output
                .as_ref()
                .and_then(|value| value.get("windowPolicy"))
                .and_then(|value| value.get("allowedLateness"))
                .and_then(Value::as_str),
            Some("30s")
        );
        assert_eq!(
            runtime_query
                .output
                .as_ref()
                .and_then(|value| value.get("windowPolicy"))
                .and_then(|value| value.get("lateEventPolicy"))
                .and_then(Value::as_str),
            Some("drop_after_closed_window")
        );
        assert_eq!(
            runtime_query
                .output
                .as_ref()
                .and_then(|value| value.get("windowPolicy"))
                .and_then(|value| value.get("retentionEvictionEnabled"))
                .and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            runtime_query
                .output
                .as_ref()
                .and_then(|value| value.get("materializedViews"))
                .and_then(Value::as_array)
                .and_then(|views| views.first())
                .and_then(|view| view.get("lateEventPolicy"))
                .and_then(Value::as_str),
            Some("drop_after_closed_window")
        );
        assert_eq!(
            runtime_query
                .output
                .as_ref()
                .and_then(|value| value.get("materializedViews"))
                .and_then(Value::as_array)
                .and_then(|views| views.first())
                .and_then(|view| view.get("windowPolicy"))
                .and_then(|policy| policy.get("allowedLateness"))
                .and_then(Value::as_str),
            Some("30s")
        );
        assert_eq!(
            runtime_query
                .output
                .as_ref()
                .and_then(|value| value.get("materializedViews"))
                .and_then(Value::as_array)
                .and_then(|views| views.first())
                .and_then(|view| view.get("retentionEvictionEnabled"))
                .and_then(Value::as_bool),
            Some(false)
        );

        let checkpoints =
            store.list_stream_job_bridge_checkpoints_for_handle_page(&handle_id, 10, 0).await?;
        assert_eq!(checkpoints.len(), 1);
        let runtime_state = app_state
            .local_state
            .load_stream_job_runtime_state(&handle_id)?
            .context("late-drop aggregate_v2 runtime state should exist")?;
        let cursor = runtime_state
            .source_cursors
            .iter()
            .find(|cursor| cursor.source_partition_id == 0)
            .context("late-drop aggregate_v2 source cursor should exist")?;
        assert_eq!(cursor.dropped_late_event_count, 1);
        assert_eq!(cursor.last_dropped_late_offset, Some(2));
        assert_eq!(
            cursor.last_dropped_late_event_at.map(|value| value.to_rfc3339()),
            Some("2026-03-15T10:00:20+00:00".to_owned())
        );
        assert_eq!(
            cursor.last_dropped_late_window_end.map(|value| value.to_rfc3339()),
            Some("2026-03-15T10:01:00+00:00".to_owned())
        );

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn workflow_topic_windowed_aggregate_v2_stream_job_evicts_retained_window_and_restores_without_it()
    -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-topic-windowed-aggregate-v2-retention-eviction");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;
        let source_topic = JsonTopicConfig::new(
            redpanda.workflow_broker.brokers.clone(),
            format!("stream-windowed-aggregate-v2-retention-{}", Uuid::now_v7()),
            1,
        );
        let tenant_id = "tenant";
        let instance_id = "instance-stream-job-topic-windowed-aggregate-v2-retention";
        let run_id = "run-stream-job-topic-windowed-aggregate-v2-retention";
        let job_id = "job-stream-job-topic-windowed-aggregate-v2-retention";
        let occurred_at = Utc::now();
        let handle = aggregate_v2_topic_window_stream_job_handle_with_policy(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            &source_topic.topic_name,
            fabrik_throughput::StreamJobBridgeHandleStatus::Admitted,
            None,
            Some(30),
            occurred_at,
        );
        let handle_id = handle.handle_id.clone();
        store.upsert_stream_job_bridge_handle(&handle).await?;
        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{handle_id}"),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle_id.clone(),
                    },
                ),
            },
        )
        .await?;

        let source_publisher = redpanda
            .connect_json_publisher::<Value>(
                &source_topic,
                "throughput-topic-windowed-aggregate-v2-retention-source",
            )
            .await?;
        let first_event_time = Utc::now() - chrono::Duration::minutes(4);
        let first_window_start =
            DateTime::<Utc>::from_timestamp(first_event_time.timestamp().div_euclid(60) * 60, 0)
                .context("retention test first window start should be representable")?;
        let first_window_end = first_window_start + chrono::Duration::minutes(1);
        let second_event_time = first_window_end + chrono::Duration::seconds(35);
        source_publisher
            .publish(
                &json!({
                    "accountId": "acct_1",
                    "risk": 0.9,
                    "eventTime": first_event_time.to_rfc3339()
                }),
                "acct_1",
            )
            .await?;
        source_publisher
            .publish(
                &json!({
                    "accountId": "acct_1",
                    "risk": 0.8,
                    "eventTime": second_event_time.to_rfc3339()
                }),
                "acct_1",
            )
            .await?;

        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;
        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        let query_requested_at = occurred_at + chrono::Duration::seconds(1);
        let evicted_query = fabrik_store::StreamJobQueryRecord {
            protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
            workflow_event_id: Uuid::now_v7(),
            tenant_id: tenant_id.to_owned(),
            instance_id: instance_id.to_owned(),
            run_id: run_id.to_owned(),
            job_id: job_id.to_owned(),
            handle_id: handle_id.clone(),
            bridge_request_id: handle.bridge_request_id.clone(),
            query_id: "query-evicted-window".to_owned(),
            query_name: "riskScoresByKey".to_owned(),
            query_args: Some(json!({
                "key": "acct_1",
                "windowStart": first_window_start.to_rfc3339()
            })),
            consistency: "strong".to_owned(),
            status: fabrik_throughput::StreamJobQueryStatus::Requested.as_str().to_owned(),
            workflow_owner_epoch: Some(1),
            stream_owner_epoch: None,
            output: None,
            error: None,
            requested_at: query_requested_at,
            completed_at: None,
            accepted_at: None,
            cancelled_at: None,
            created_at: query_requested_at,
            updated_at: query_requested_at,
        };
        let runtime_state = app_state
            .local_state
            .load_stream_job_runtime_state(&handle_id)?
            .context("retention runtime state should exist after eviction reconcile")?;
        assert_eq!(runtime_state.evicted_window_count, 1);
        assert_eq!(
            runtime_state.last_evicted_window_end.map(|value| value.to_rfc3339()),
            Some(first_window_end.to_rfc3339())
        );
        assert!(runtime_state.last_evicted_at.is_some());
        assert_eq!(
            runtime_state.view_runtime_stats("riskScores").map(|stats| stats.evicted_window_count),
            Some(1)
        );
        assert_eq!(
            runtime_state
                .view_runtime_stats("riskScores")
                .and_then(|stats| stats.last_evicted_window_end)
                .map(|value| value.to_rfc3339()),
            Some(first_window_end.to_rfc3339())
        );
        let window_key =
            stream_jobs::windowed_logical_key("acct_1", &first_window_start.to_rfc3339());
        assert!(
            app_state
                .local_state
                .load_stream_job_view_state(&handle_id, "riskScores", &window_key)?
                .is_none()
        );
        let query_error = stream_jobs::build_stream_job_query_output_on_local_state(
            &app_state.local_state,
            &handle,
            &evicted_query,
        )
        .expect_err("evicted window should no longer be strongly queryable");
        assert!(query_error.to_string().contains("not materialized"));

        source_publisher
            .publish(
                &json!({
                    "accountId": "acct_1",
                    "risk": 0.1,
                    "eventTime": (first_window_start + chrono::Duration::seconds(20)).to_rfc3339()
                }),
                "acct_1",
            )
            .await?;
        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        let post_eviction_runtime_state = app_state
            .local_state
            .load_stream_job_runtime_state(&handle_id)?
            .context("post-eviction runtime state should still exist")?;
        let post_eviction_cursor = post_eviction_runtime_state
            .source_cursors
            .iter()
            .find(|cursor| cursor.source_partition_id == 0)
            .context("post-eviction source cursor should exist")?;
        assert_eq!(post_eviction_cursor.dropped_late_event_count, 0);
        assert_eq!(post_eviction_cursor.dropped_evicted_window_event_count, 1);
        assert_eq!(post_eviction_cursor.last_dropped_evicted_window_offset, Some(2));
        assert_eq!(
            post_eviction_cursor.last_dropped_evicted_window_end.map(|value| value.to_rfc3339()),
            Some(first_window_end.to_rfc3339())
        );

        let checkpoint_snapshot = app_state.local_state.snapshot_checkpoint_value()?;
        let restored_state_dir =
            temp_path("throughput-topic-windowed-aggregate-v2-retention-eviction-restored");
        let restored_app_state =
            test_app_state(store.clone(), &redpanda, &restored_state_dir).await?;
        assert!(
            restored_app_state
                .local_state
                .restore_from_checkpoint_value_if_empty(checkpoint_snapshot)?
        );

        let restored_runtime_state = restored_app_state
            .local_state
            .load_stream_job_runtime_state(&handle_id)?
            .context("restored retention runtime state should exist")?;
        assert_eq!(restored_runtime_state.evicted_window_count, 1);
        assert_eq!(
            restored_runtime_state.last_evicted_window_end.map(|value| value.to_rfc3339()),
            Some(first_window_end.to_rfc3339())
        );
        assert_eq!(
            restored_runtime_state
                .view_runtime_stats("riskScores")
                .map(|stats| stats.evicted_window_count),
            Some(1)
        );
        assert_eq!(
            restored_runtime_state
                .view_runtime_stats("riskScores")
                .and_then(|stats| stats.last_evicted_window_end)
                .map(|value| value.to_rfc3339()),
            Some(first_window_end.to_rfc3339())
        );
        let restored_cursor = restored_runtime_state
            .source_cursors
            .iter()
            .find(|cursor| cursor.source_partition_id == 0)
            .context("restored retention source cursor should exist")?;
        assert_eq!(restored_cursor.dropped_evicted_window_event_count, 1);
        assert_eq!(restored_cursor.last_dropped_evicted_window_offset, Some(2));
        assert!(
            restored_app_state
                .local_state
                .load_stream_job_view_state(&handle_id, "riskScores", &window_key)?
                .is_none()
        );
        let restored_query_error = stream_jobs::build_stream_job_query_output_on_local_state(
            &restored_app_state.local_state,
            &handle,
            &evicted_query,
        )
        .expect_err("restored checkpoint should not resurrect evicted windows");
        assert!(restored_query_error.to_string().contains("not materialized"));

        fs::remove_dir_all(state_dir).ok();
        fs::remove_dir_all(restored_state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn debug_runtime_stats_endpoint_returns_late_drop_diagnostics() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-debug-runtime-stats-late-drop");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;
        let source_topic = JsonTopicConfig::new(
            redpanda.workflow_broker.brokers.clone(),
            format!("stream-debug-runtime-stats-late-drop-{}", Uuid::now_v7()),
            1,
        );
        let tenant_id = "tenant";
        let instance_id = "instance-debug-runtime-stats-late-drop";
        let run_id = "run-debug-runtime-stats-late-drop";
        let job_id = "job-debug-runtime-stats-late-drop";
        let occurred_at = Utc::now();
        let handle = aggregate_v2_topic_window_stream_job_handle_with_allowed_lateness(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            &source_topic.topic_name,
            fabrik_throughput::StreamJobBridgeHandleStatus::Admitted,
            Some("30s"),
            occurred_at,
        );
        store.upsert_stream_job_bridge_handle(&handle).await?;

        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{}", handle.handle_id),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle.handle_id.clone(),
                    },
                ),
            },
        )
        .await?;

        let source_publisher = redpanda
            .connect_json_publisher::<Value>(
                &source_topic,
                "throughput-debug-runtime-stats-late-drop-source",
            )
            .await?;
        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.9, "eventTime": "2026-03-15T10:00:55Z"}),
                "acct_1",
            )
            .await?;
        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.8, "eventTime": "2026-03-15T10:01:35Z"}),
                "acct_1",
            )
            .await?;
        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.1, "eventTime": "2026-03-15T10:00:20Z"}),
                "acct_1",
            )
            .await?;
        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        let Json(output) = get_strong_stream_job_runtime_stats(
            Path((
                tenant_id.to_owned(),
                instance_id.to_owned(),
                run_id.to_owned(),
                job_id.to_owned(),
            )),
            Query(StreamJobRuntimeStatsQuery { source_partition_id: Some(0) }),
            AxumState(app_state.clone()),
        )
        .await
        .map_err(|(_, message)| anyhow::anyhow!(message))?;

        assert_eq!(output["queryName"], stream_jobs::STREAM_JOB_RUNTIME_STATS_QUERY_NAME);
        assert_eq!(output["sourcePartitionId"], 0);
        assert_eq!(output["sourceCursors"][0]["droppedLateEventCount"], 1);
        assert_eq!(output["sourceCursors"][0]["lastDroppedLateOffset"], 2);
        assert_eq!(
            output["sourceCursors"][0]["lastDroppedLateEventAt"],
            "2026-03-15T10:00:20+00:00"
        );
        assert_eq!(
            output["sourceCursors"][0]["lastDroppedLateWindowEnd"],
            "2026-03-15T10:01:00+00:00"
        );

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn debug_view_runtime_stats_endpoint_returns_view_policy_and_counts() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-debug-view-runtime-stats");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;
        let source_topic = JsonTopicConfig::new(
            redpanda.workflow_broker.brokers.clone(),
            format!("stream-debug-view-runtime-stats-{}", Uuid::now_v7()),
            1,
        );
        let tenant_id = "tenant";
        let instance_id = "instance-debug-view-runtime-stats";
        let run_id = "run-debug-view-runtime-stats";
        let job_id = "job-debug-view-runtime-stats";
        let occurred_at = Utc::now();
        let handle = aggregate_v2_topic_window_stream_job_handle_with_policy(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            &source_topic.topic_name,
            fabrik_throughput::StreamJobBridgeHandleStatus::Admitted,
            Some("30s"),
            Some(120),
            occurred_at,
        );
        store.upsert_stream_job_bridge_handle(&handle).await?;

        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{}", handle.handle_id),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle.handle_id.clone(),
                    },
                ),
            },
        )
        .await?;

        let source_publisher = redpanda
            .connect_json_publisher::<Value>(
                &source_topic,
                "throughput-debug-view-runtime-stats-source",
            )
            .await?;
        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.9, "eventTime": "2026-03-15T10:00:55Z"}),
                "acct_1",
            )
            .await?;
        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.8, "eventTime": "2026-03-15T10:01:35Z"}),
                "acct_1",
            )
            .await?;
        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        let Json(output) = get_strong_stream_job_view_runtime_stats(
            Path((
                tenant_id.to_owned(),
                instance_id.to_owned(),
                run_id.to_owned(),
                job_id.to_owned(),
                "riskScores".to_owned(),
            )),
            AxumState(app_state.clone()),
        )
        .await
        .map_err(|(_, message)| anyhow::anyhow!(message))?;

        assert_eq!(output["queryName"], stream_jobs::STREAM_JOB_VIEW_RUNTIME_STATS_QUERY_NAME);
        assert_eq!(output["viewName"], "riskScores");
        assert_eq!(output["storedKeyCount"], 2);
        assert_eq!(output["activeKeyCount"], 2);
        assert_eq!(output["policy"]["operatorId"], "materialize-risk");
        assert_eq!(output["policy"]["retentionSeconds"], 120);
        assert_eq!(output["policy"]["lateEventPolicy"], "drop_after_closed_window");
        assert_eq!(output["policy"]["windowPolicy"]["allowedLateness"], "30s");
        assert_eq!(output["policy"]["windowPolicy"]["retentionSeconds"], 120);
        assert_eq!(output["freshness"]["checkpointSequenceLag"], 0);
        assert_eq!(output["freshness"]["latestEventTimeWatermark"], "2026-03-15T10:01:35+00:00");
        assert_eq!(output["freshness"]["latestClosedWindowEnd"], "2026-03-15T10:01:00+00:00");
        assert_eq!(output["freshness"]["latestMaterializedWindowEnd"], "2026-03-15T10:02:00+00:00");
        assert_eq!(output["freshness"]["closedWindowLagSeconds"], -60);
        assert_eq!(output["historicalEvictedWindowCount"], 0);
        assert_eq!(output["historicalLastEvictedWindowEnd"], Value::Null);
        assert_eq!(output["historicalLastEvictedAt"], Value::Null);
        assert_eq!(output["projectionStats"]["supported"], true);
        assert_eq!(output["projectionStats"]["rebuildSupported"], true);
        assert_eq!(output["projectionStats"]["summary"]["latestProjectedCheckpointSequence"], 1);
        assert_eq!(output["projectionStats"]["freshness"]["ownerCheckpointSequence"], 1);
        assert_eq!(output["projectionStats"]["freshness"]["checkpointSequenceLag"], 0);
        assert_eq!(output["consistency"], "strong");
        assert_eq!(output["consistencySource"], "stream_owner_local_state");

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn debug_view_scan_endpoint_returns_projection_stats_for_strong_scan() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-debug-view-scan-projection-stats");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;
        let source_topic = JsonTopicConfig::new(
            redpanda.workflow_broker.brokers.clone(),
            format!("stream-topic-debug-view-scan-{}", Uuid::now_v7()),
            1,
        );
        let tenant_id = "tenant";
        let instance_id = "instance-stream-topic-debug-view-scan";
        let run_id = "run-stream-topic-debug-view-scan";
        let job_id = "job-stream-topic-debug-view-scan";
        let occurred_at = Utc::now();
        let mut handle = aggregate_v2_topic_window_stream_job_handle_with_policy(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            &source_topic.topic_name,
            fabrik_throughput::StreamJobBridgeHandleStatus::Admitted,
            Some("30s"),
            Some(120),
            occurred_at,
        );
        if let Some(config_ref) = handle.config_ref.as_mut() {
            let mut config: Value = serde_json::from_str(config_ref)?;
            config["views"][0]["queryMode"] = json!("prefix_scan");
            *config_ref = config.to_string();
        }
        store.upsert_stream_job_bridge_handle(&handle).await?;
        let owner_local_state = owner_local_state_for_stream_job(&app_state, &handle.job_id);
        owner_local_state.upsert_stream_job_runtime_state(
            &crate::local_state::LocalStreamJobRuntimeState {
                handle_id: handle.handle_id.clone(),
                job_id: handle.job_id.clone(),
                job_name: handle.job_name.clone(),
                view_name: "riskScores".to_owned(),
                checkpoint_name: "initial-risk-ready".to_owned(),
                checkpoint_sequence: 4,
                input_item_count: 0,
                materialized_key_count: 2,
                active_partitions: vec![0],
                source_kind: Some(fabrik_throughput::STREAM_SOURCE_TOPIC.to_owned()),
                source_name: Some(source_topic.topic_name.clone()),
                source_cursors: vec![crate::local_state::LocalStreamJobSourceCursorState {
                    source_partition_id: 0,
                    next_offset: 3,
                    initial_checkpoint_target_offset: 3,
                    last_applied_offset: Some(2),
                    last_high_watermark: Some(3),
                    last_event_time_watermark: Some(
                        DateTime::parse_from_rfc3339("2026-03-15T10:01:35Z")
                            .expect("event watermark should parse")
                            .with_timezone(&Utc),
                    ),
                    last_closed_window_end: Some(
                        DateTime::parse_from_rfc3339("2026-03-15T10:01:00Z")
                            .expect("closed window should parse")
                            .with_timezone(&Utc),
                    ),
                    pending_window_ends: Vec::new(),
                    dropped_late_event_count: 0,
                    last_dropped_late_offset: None,
                    last_dropped_late_event_at: None,
                    last_dropped_late_window_end: None,
                    dropped_evicted_window_event_count: 0,
                    last_dropped_evicted_window_offset: None,
                    last_dropped_evicted_window_event_at: None,
                    last_dropped_evicted_window_end: None,
                    checkpoint_reached_at: Some(occurred_at),
                    updated_at: occurred_at,
                }],
                source_partition_leases: vec![crate::local_state::LocalStreamJobSourceLeaseState {
                    source_partition_id: 0,
                    owner_partition_id: 0,
                    owner_epoch: 7,
                    lease_token: "lease-0".to_owned(),
                    updated_at: occurred_at,
                }],
                dispatch_batches: Vec::new(),
                applied_dispatch_batch_ids: Vec::new(),
                dispatch_completed_at: None,
                dispatch_cancelled_at: None,
                stream_owner_epoch: 7,
                planned_at: occurred_at,
                latest_checkpoint_at: Some(occurred_at),
                evicted_window_count: 0,
                last_evicted_window_end: None,
                last_evicted_at: None,
                view_runtime_stats: Vec::new(),
                checkpoint_partitions: Vec::new(),
                terminal_status: None,
                terminal_output: None,
                terminal_error: None,
                terminal_at: None,
                updated_at: occurred_at,
            },
        )?;
        let first_key = stream_jobs::windowed_logical_key("acct_1", "2026-03-15T10:00:00+00:00");
        let second_key = stream_jobs::windowed_logical_key("acct_1", "2026-03-15T10:01:00+00:00");
        for (logical_key, window_start, avg_risk) in [
            (first_key.as_str(), "2026-03-15T10:00:00+00:00", 0.9),
            (second_key.as_str(), "2026-03-15T10:01:00+00:00", 0.8),
        ] {
            owner_local_state.upsert_stream_job_view_value(
                &handle.handle_id,
                &handle.job_id,
                "riskScores",
                logical_key,
                json!({
                    "accountId": "acct_1",
                    "avgRisk": avg_risk,
                    "windowStart": window_start,
                    "windowEnd": if window_start == "2026-03-15T10:00:00+00:00" {
                        "2026-03-15T10:01:00+00:00"
                    } else {
                        "2026-03-15T10:02:00+00:00"
                    },
                    "asOfCheckpoint": 4
                }),
                4,
                occurred_at,
            )?;
            store
                .upsert_stream_job_view_query(&fabrik_store::StreamJobViewRecord {
                    tenant_id: tenant_id.to_owned(),
                    instance_id: instance_id.to_owned(),
                    run_id: run_id.to_owned(),
                    job_id: job_id.to_owned(),
                    handle_id: handle.handle_id.clone(),
                    view_name: "riskScores".to_owned(),
                    logical_key: logical_key.to_owned(),
                    output: json!({
                        "accountId": "acct_1",
                        "avgRisk": avg_risk,
                        "windowStart": window_start,
                        "windowEnd": if window_start == "2026-03-15T10:00:00+00:00" {
                            "2026-03-15T10:01:00+00:00"
                        } else {
                            "2026-03-15T10:02:00+00:00"
                        },
                        "asOfCheckpoint": 3
                    }),
                    checkpoint_sequence: 3,
                    updated_at: occurred_at,
                })
                .await?;
        }

        let Json(output) = get_stream_job_view_scan(
            Path((
                tenant_id.to_owned(),
                instance_id.to_owned(),
                run_id.to_owned(),
                job_id.to_owned(),
                "riskScores".to_owned(),
            )),
            Query(StreamJobViewScanQuery {
                consistency: Some("strong".to_owned()),
                prefix: Some("acct_1".to_owned()),
                key_prefix: None,
                limit: Some(10),
                offset: Some(0),
            }),
            AxumState(app_state.clone()),
        )
        .await
        .map_err(|(_, message)| anyhow::anyhow!(message))?;

        assert_eq!(output["consistency"], "strong");
        assert_eq!(output["consistencySource"], "stream_owner_local_state");
        assert_eq!(output["total"], 2);
        assert_eq!(output["items"].as_array().map(Vec::len), Some(2));
        assert_eq!(output["runtimeStats"]["streamOwnerEpoch"], 7);
        assert_eq!(output["projectionStats"]["supported"], true);
        assert_eq!(output["projectionStats"]["rebuildSupported"], true);
        assert_eq!(output["projectionStats"]["summary"]["keyCount"], 2);
        assert_eq!(output["projectionStats"]["summary"]["latestProjectedCheckpointSequence"], 3);
        assert_eq!(output["projectionStats"]["freshness"]["ownerCheckpointSequence"], 4);
        assert_eq!(output["projectionStats"]["freshness"]["checkpointSequenceLag"], 1);

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn workflow_topic_windowed_eventual_query_reports_projection_freshness() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-topic-windowed-eventual-query-freshness");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;
        let source_topic = JsonTopicConfig::new(
            redpanda.workflow_broker.brokers.clone(),
            format!("stream-topic-eventual-query-{}", Uuid::now_v7()),
            1,
        );
        let tenant_id = "tenant";
        let instance_id = "instance-stream-topic-eventual-query";
        let run_id = "run-stream-topic-eventual-query";
        let job_id = "job-stream-topic-eventual-query";
        let occurred_at = Utc::now();
        let handle = aggregate_v2_topic_window_stream_job_handle_with_policy(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            &source_topic.topic_name,
            fabrik_throughput::StreamJobBridgeHandleStatus::Admitted,
            Some("30s"),
            Some(120),
            occurred_at,
        );
        store.upsert_stream_job_bridge_handle(&handle).await?;
        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{}", handle.handle_id),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle.handle_id.clone(),
                    },
                ),
            },
        )
        .await?;

        let source_publisher = redpanda
            .connect_json_publisher::<Value>(
                &source_topic,
                "throughput-topic-eventual-query-source",
            )
            .await?;
        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.9, "eventTime": "2026-03-15T10:00:55Z"}),
                "acct_1",
            )
            .await?;
        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.8, "eventTime": "2026-03-15T10:01:35Z"}),
                "acct_1",
            )
            .await?;
        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        let requested_at = Utc::now();
        let output = stream_jobs::build_stream_job_query_output(
            &app_state,
            &handle,
            &fabrik_store::StreamJobQueryRecord {
                protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
                workflow_event_id: Uuid::now_v7(),
                tenant_id: tenant_id.to_owned(),
                instance_id: instance_id.to_owned(),
                run_id: run_id.to_owned(),
                job_id: job_id.to_owned(),
                handle_id: handle.handle_id.clone(),
                bridge_request_id: handle.bridge_request_id.clone(),
                query_id: format!("eventual-query-{}", Uuid::now_v7()),
                query_name: "riskScoresByKey".to_owned(),
                query_args: Some(json!({
                    "key": "acct_1",
                    "windowStart": "2026-03-15T10:00:00+00:00"
                })),
                consistency: fabrik_throughput::StreamJobQueryConsistency::Eventual
                    .as_str()
                    .to_owned(),
                status: fabrik_throughput::StreamJobQueryStatus::Completed.as_str().to_owned(),
                workflow_owner_epoch: handle.workflow_owner_epoch,
                stream_owner_epoch: handle.stream_owner_epoch,
                output: None,
                error: None,
                requested_at,
                completed_at: None,
                accepted_at: None,
                cancelled_at: None,
                created_at: requested_at,
                updated_at: requested_at,
            },
        )
        .await?
        .context("eventual query output should exist")?;

        assert_eq!(output["consistency"], "eventual");
        assert_eq!(output["consistencySource"], "stream_projection_query");
        assert_eq!(output["avgRisk"], 0.9);
        assert_eq!(output["projectionStats"]["summary"]["keyCount"], 2);
        assert_eq!(output["projectionStats"]["summary"]["latestProjectedCheckpointSequence"], 1);
        assert_eq!(output["projectionStats"]["freshness"]["ownerCheckpointSequence"], 1);
        assert_eq!(output["projectionStats"]["freshness"]["checkpointSequenceLag"], 0);
        assert_eq!(
            output["projectionStats"]["summary"]["latestDeletedCheckpointSequence"],
            Value::Null
        );

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn debug_projection_rebuild_restores_missing_rows_and_deletes_stale_rows() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-debug-projection-rebuild");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;
        let source_topic = JsonTopicConfig::new(
            redpanda.workflow_broker.brokers.clone(),
            format!("stream-debug-projection-rebuild-{}", Uuid::now_v7()),
            1,
        );
        let tenant_id = "tenant";
        let instance_id = "instance-debug-projection-rebuild";
        let run_id = "run-debug-projection-rebuild";
        let job_id = "job-debug-projection-rebuild";
        let occurred_at = Utc::now();
        let handle = aggregate_v2_topic_window_stream_job_handle_with_policy(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            &source_topic.topic_name,
            fabrik_throughput::StreamJobBridgeHandleStatus::Admitted,
            Some("30s"),
            Some(120),
            occurred_at,
        );
        store.upsert_stream_job_bridge_handle(&handle).await?;
        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{}", handle.handle_id),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle.handle_id.clone(),
                    },
                ),
            },
        )
        .await?;
        let source_publisher = redpanda
            .connect_json_publisher::<Value>(
                &source_topic,
                "throughput-debug-projection-rebuild-source",
            )
            .await?;
        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.9, "eventTime": "2026-03-15T10:00:55Z"}),
                "acct_1",
            )
            .await?;
        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        let logical_key = stream_jobs::windowed_logical_key("acct_1", "2026-03-15T10:00:00+00:00");
        store
            .purge_stream_job_view_query_row_unchecked(
                tenant_id,
                instance_id,
                run_id,
                job_id,
                "riskScores",
                &logical_key,
            )
            .await?;
        store
            .upsert_stream_job_view_query(&fabrik_store::StreamJobViewRecord {
                tenant_id: tenant_id.to_owned(),
                instance_id: instance_id.to_owned(),
                run_id: run_id.to_owned(),
                job_id: job_id.to_owned(),
                handle_id: handle.handle_id.clone(),
                view_name: "riskScores".to_owned(),
                logical_key: "acct_ghost".to_owned(),
                output: json!({"accountId": "acct_ghost", "avgRisk": 0.1}),
                checkpoint_sequence: 0,
                updated_at: occurred_at,
            })
            .await?;

        let Json(rebuilt) = rebuild_stream_job_projections(
            Path((
                tenant_id.to_owned(),
                instance_id.to_owned(),
                run_id.to_owned(),
                job_id.to_owned(),
            )),
            AxumState(app_state.clone()),
        )
        .await
        .map_err(|(_, message)| anyhow::anyhow!(message))?;

        assert_eq!(rebuilt["views"][0]["viewName"], "riskScores");
        assert_eq!(rebuilt["views"][0]["upsertedCount"], 1);
        assert_eq!(rebuilt["views"][0]["deletedCount"], 1);
        assert!(
            store
                .get_stream_job_view_query(
                    tenant_id,
                    instance_id,
                    run_id,
                    job_id,
                    "riskScores",
                    &logical_key,
                )
                .await?
                .is_some()
        );
        assert!(
            store
                .get_stream_job_view_query(
                    tenant_id,
                    instance_id,
                    run_id,
                    job_id,
                    "riskScores",
                    "acct_ghost",
                )
                .await?
                .is_none()
        );

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn debug_view_projection_rebuild_restores_missing_rows_for_one_view() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-debug-view-projection-rebuild");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;
        let source_topic = JsonTopicConfig::new(
            redpanda.workflow_broker.brokers.clone(),
            format!("stream-debug-view-projection-rebuild-{}", Uuid::now_v7()),
            1,
        );
        let tenant_id = "tenant";
        let instance_id = "instance-debug-view-projection-rebuild";
        let run_id = "run-debug-view-projection-rebuild";
        let job_id = "job-debug-view-projection-rebuild";
        let occurred_at = Utc::now();
        let handle = aggregate_v2_topic_window_stream_job_handle_with_policy(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            &source_topic.topic_name,
            fabrik_throughput::StreamJobBridgeHandleStatus::Admitted,
            Some("30s"),
            Some(120),
            occurred_at,
        );
        store.upsert_stream_job_bridge_handle(&handle).await?;
        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{}", handle.handle_id),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle.handle_id.clone(),
                    },
                ),
            },
        )
        .await?;

        let source_publisher = redpanda
            .connect_json_publisher::<Value>(
                &source_topic,
                "throughput-debug-view-projection-rebuild-source",
            )
            .await?;
        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.9, "eventTime": "2026-03-15T10:00:55Z"}),
                "acct_1",
            )
            .await?;
        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.8, "eventTime": "2026-03-15T10:01:35Z"}),
                "acct_1",
            )
            .await?;
        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        let logical_key = stream_jobs::windowed_logical_key("acct_1", "2026-03-15T10:00:00+00:00");
        store
            .purge_stream_job_view_query_row_unchecked(
                tenant_id,
                instance_id,
                run_id,
                job_id,
                "riskScores",
                &logical_key,
            )
            .await?;

        let Json(rebuilt) = rebuild_stream_job_view_projections(
            Path((
                tenant_id.to_owned(),
                instance_id.to_owned(),
                run_id.to_owned(),
                job_id.to_owned(),
                "riskScores".to_owned(),
            )),
            AxumState(app_state.clone()),
        )
        .await
        .map_err(|(_, message)| anyhow::anyhow!(message))?;

        assert_eq!(rebuilt["viewName"], "riskScores");
        assert_eq!(rebuilt["views"].as_array().map(Vec::len), Some(1));
        assert_eq!(rebuilt["views"][0]["viewName"], "riskScores");
        assert_eq!(rebuilt["views"][0]["upsertedCount"], 2);
        assert_eq!(rebuilt["views"][0]["deletedCount"], 0);
        assert!(
            store
                .get_stream_job_view_query(
                    tenant_id,
                    instance_id,
                    run_id,
                    job_id,
                    "riskScores",
                    &logical_key,
                )
                .await?
                .is_some()
        );

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn workflow_topic_windowed_aggregate_v2_stream_job_restores_pending_window_and_closes_once()
    -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-topic-windowed-aggregate-v2-restore-pending-window");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;
        let source_topic = JsonTopicConfig::new(
            redpanda.workflow_broker.brokers.clone(),
            format!("stream-windowed-aggregate-v2-restore-pending-{}", Uuid::now_v7()),
            1,
        );
        let source_publisher = redpanda
            .connect_json_publisher::<Value>(
                &source_topic,
                "throughput-topic-windowed-aggregate-v2-restore-pending-source",
            )
            .await?;

        let tenant_id = "tenant";
        let instance_id = "instance-stream-job-topic-windowed-aggregate-v2-restore-pending";
        let run_id = "run-stream-job-topic-windowed-aggregate-v2-restore-pending";
        let job_id = "job-stream-job-topic-windowed-aggregate-v2-restore-pending";
        let occurred_at = Utc::now();
        let handle = aggregate_v2_topic_window_stream_job_handle(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            &source_topic.topic_name,
            fabrik_throughput::StreamJobBridgeHandleStatus::Admitted,
            occurred_at,
        );
        let handle_id = handle.handle_id.clone();
        store.upsert_stream_job_bridge_handle(&handle).await?;
        let restore_event_time = Utc::now() - chrono::Duration::minutes(2);
        let restore_window_start =
            DateTime::<Utc>::from_timestamp(restore_event_time.timestamp().div_euclid(60) * 60, 0)
                .context("restore timer window start should be representable")?;
        let restore_window_end = restore_window_start + chrono::Duration::minutes(1);

        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{handle_id}"),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle_id.clone(),
                    },
                ),
            },
        )
        .await?;

        source_publisher
            .publish(
                &json!({"accountId": "acct_1", "risk": 0.9, "eventTime": restore_event_time.to_rfc3339()}),
                "acct_1",
            )
            .await?;
        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        let open_runtime_state =
            app_state.local_state.load_stream_job_runtime_state(&handle_id)?.context(
                "windowed aggregate_v2 restore runtime state should exist after first reconcile",
            )?;
        let open_cursor = open_runtime_state
            .source_cursors
            .iter()
            .find(|cursor| cursor.source_partition_id == 0)
            .context(
                "windowed aggregate_v2 restore source cursor should exist after first reconcile",
            )?;
        assert!(open_cursor.checkpoint_reached_at.is_none());
        assert!(open_cursor.last_closed_window_end.is_none());
        assert_eq!(open_cursor.pending_window_ends.len(), 1);
        let checkpoint_snapshot = app_state.local_state.snapshot_checkpoint_value()?;

        let restored_state_dir =
            temp_path("throughput-topic-windowed-aggregate-v2-restore-pending-restored");
        let restored_app_state =
            test_app_state(store.clone(), &redpanda, &restored_state_dir).await?;
        assert!(
            restored_app_state
                .local_state
                .restore_from_checkpoint_value_if_empty(checkpoint_snapshot)?
        );

        bridge::reconcile_active_topic_stream_jobs(&restored_app_state, 64).await?;
        bridge::reconcile_active_topic_stream_jobs(&restored_app_state, 64).await?;

        let checkpoints =
            store.list_stream_job_bridge_checkpoints_for_handle_page(&handle_id, 10, 0).await?;
        assert_eq!(checkpoints.len(), 1);
        let restored_runtime_state =
            restored_app_state.local_state.load_stream_job_runtime_state(&handle_id)?.context(
                "windowed aggregate_v2 restored runtime state should exist after timer reconcile",
            )?;
        let restored_cursor = restored_runtime_state
            .source_cursors
            .iter()
            .find(|cursor| cursor.source_partition_id == 0)
            .context(
                "windowed aggregate_v2 restored source cursor should exist after timer reconcile",
            )?;
        assert!(restored_cursor.checkpoint_reached_at.is_some());
        assert_eq!(
            restored_cursor.last_closed_window_end.map(|value| value.to_rfc3339()),
            Some(restore_window_end.to_rfc3339())
        );
        assert!(restored_cursor.pending_window_ends.is_empty());

        let window_key =
            stream_jobs::windowed_logical_key("acct_1", &restore_window_start.to_rfc3339());
        let restored_view = restored_app_state
            .local_state
            .load_stream_job_view_state(&handle_id, "riskScores", &window_key)?
            .context("restored pending-window aggregate_v2 view should exist")?;
        assert_eq!(restored_view.output["avgRisk"], json!(0.9));

        fs::remove_dir_all(state_dir).ok();
        fs::remove_dir_all(restored_state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn topic_stream_job_cancel_command_drains_without_consuming_post_cancel_records()
    -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-topic-stream-job-cancel");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;
        let source_topic = JsonTopicConfig::new(
            redpanda.workflow_broker.brokers.clone(),
            format!("stream-source-cancel-test-{}", Uuid::now_v7()),
            1,
        );
        let source_publisher = redpanda
            .connect_json_publisher::<Value>(&source_topic, "throughput-topic-stream-cancel-source")
            .await?;
        source_publisher.publish(&json!({"accountId": "acct_1", "amount": 2.0}), "acct_1").await?;
        source_publisher.publish(&json!({"accountId": "acct_1", "amount": 3.0}), "acct_1").await?;

        let tenant_id = "tenant";
        let instance_id = "instance-stream-job-topic-cancel";
        let run_id = "run-stream-job-topic-cancel";
        let job_id = "job-stream-job-topic-cancel";
        let occurred_at = Utc::now();
        let initial_handle = topic_stream_job_handle(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            &source_topic.topic_name,
            fabrik_throughput::StreamJobBridgeHandleStatus::Admitted,
            None,
            None,
            occurred_at,
        );
        let handle_id = initial_handle.handle_id.clone();
        store.upsert_stream_job_bridge_handle(&initial_handle).await?;

        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{handle_id}"),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle_id.clone(),
                    },
                ),
            },
        )
        .await?;

        let cancel_requested_at = occurred_at + chrono::Duration::seconds(2);
        let mut cancelling_handle = store
            .get_stream_job_bridge_handle(tenant_id, instance_id, run_id, job_id)
            .await?
            .context("topic stream job handle should exist before cancel")?;
        cancelling_handle.status =
            fabrik_throughput::StreamJobBridgeHandleStatus::CancellationRequested
                .as_str()
                .to_owned();
        cancelling_handle.cancellation_requested_at = Some(cancel_requested_at);
        cancelling_handle.cancellation_reason = Some("user requested cancel".to_owned());
        cancelling_handle.updated_at = cancel_requested_at;
        store.upsert_stream_job_bridge_handle(&cancelling_handle).await?;

        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at: cancel_requested_at,
                dedupe_key: format!("stream-job-cancel:{handle_id}"),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::CancelStreamJob(
                    fabrik_throughput::CancelStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle_id.clone(),
                        reason: Some("user requested cancel".to_owned()),
                    },
                ),
            },
        )
        .await?;

        let draining_job = store
            .get_stream_job(tenant_id, instance_id, run_id, job_id)
            .await?
            .context("topic stream job record should exist after cancel request")?;
        assert_eq!(
            draining_job.parsed_status(),
            Some(fabrik_throughput::StreamJobStatus::Draining)
        );

        source_publisher.publish(&json!({"accountId": "acct_1", "amount": 7.0}), "acct_1").await?;
        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        let finalized_handle = store
            .get_stream_job_bridge_handle(tenant_id, instance_id, run_id, job_id)
            .await?
            .context("topic stream job handle should exist after drain")?;
        assert_eq!(
            finalized_handle.parsed_status(),
            Some(fabrik_throughput::StreamJobBridgeHandleStatus::Cancelled)
        );
        assert_eq!(finalized_handle.terminal_error.as_deref(), Some("user requested cancel"));

        let acct_1 = app_state
            .local_state
            .load_stream_job_view_state(&handle_id, "accountTotals", "acct_1")?
            .context("cancelled topic-backed acct_1 projection should exist")?;
        assert_eq!(acct_1.output["totalAmount"], json!(5.0));

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn stream_job_query_request_emits_deferred_terminal_callback() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-stream-job-query-terminal");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;

        let tenant_id = "tenant";
        let instance_id = "instance-stream-job-query-terminal";
        let run_id = "run-stream-job-query-terminal";
        let job_id = "job-stream-job-query-terminal";
        let handle_id =
            fabrik_throughput::stream_job_handle_id(tenant_id, instance_id, run_id, job_id);
        let occurred_at = Utc::now();

        store
            .upsert_stream_job_bridge_handle(&StreamJobBridgeHandleRecord {
                workflow_event_id: Uuid::now_v7(),
                protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
                tenant_id: tenant_id.to_owned(),
                instance_id: instance_id.to_owned(),
                run_id: run_id.to_owned(),
                stream_instance_id: instance_id.to_owned(),
                stream_run_id: run_id.to_owned(),
                job_id: job_id.to_owned(),
                handle_id: handle_id.clone(),
                bridge_request_id: fabrik_throughput::stream_job_bridge_request_id(
                    tenant_id,
                    instance_id,
                    run_id,
                    job_id,
                ),
                origin_kind: fabrik_throughput::StreamJobOriginKind::Workflow.as_str().to_owned(),
                definition_id: "payments-rollup".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                job_name: stream_jobs::KEYED_ROLLUP_JOB_NAME.to_owned(),
                input_ref: serde_json::json!({
                    "kind": "bounded_items",
                    "items": [
                        { "accountId": "acct_1", "amount": 2.0 },
                        { "accountId": "acct_1", "amount": 3.0 }
                    ]
                })
                .to_string(),
                config_ref: None,
                checkpoint_policy: None,
                view_definitions: None,
                status: fabrik_throughput::StreamJobBridgeHandleStatus::Admitted
                    .as_str()
                    .to_owned(),
                workflow_owner_epoch: None,
                stream_owner_epoch: None,
                cancellation_requested_at: None,
                cancellation_reason: None,
                terminal_event_id: None,
                terminal_at: None,
                workflow_accepted_at: None,
                terminal_output: None,
                terminal_error: None,
                created_at: occurred_at,
                updated_at: occurred_at,
            })
            .await?;

        let identity = WorkflowIdentity::new(
            tenant_id,
            "payments-rollup",
            1,
            "artifact-a",
            instance_id,
            run_id,
            "throughput-runtime-test",
        );
        let schedule_payload = WorkflowEvent::StreamJobScheduled {
            job_id: job_id.to_owned(),
            job_name: stream_jobs::KEYED_ROLLUP_JOB_NAME.to_owned(),
            input: serde_json::json!({
                "kind": "bounded_items",
                "items": [
                    { "accountId": "acct_1", "amount": 2.0 },
                    { "accountId": "acct_1", "amount": 3.0 }
                ]
            }),
            config: None,
            state: None,
        };
        let mut schedule_event =
            EventEnvelope::new(schedule_payload.event_type(), identity.clone(), schedule_payload);
        schedule_event.occurred_at = occurred_at;
        bridge::handle_bridge_event(app_state.clone(), schedule_event).await?;

        let query_payload = WorkflowEvent::StreamJobQueryRequested {
            job_id: job_id.to_owned(),
            query_id: "query-a".to_owned(),
            query_name: "accountTotals".to_owned(),
            args: Some(serde_json::json!({ "key": "acct_1" })),
            consistency: "strong".to_owned(),
            state: None,
        };
        let mut query_request =
            EventEnvelope::new(query_payload.event_type(), identity.clone(), query_payload);
        query_request.occurred_at = occurred_at + chrono::Duration::milliseconds(1);
        store
            .upsert_stream_job_bridge_query(&fabrik_store::StreamJobQueryRecord {
                protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
                workflow_event_id: Uuid::now_v7(),
                tenant_id: tenant_id.to_owned(),
                instance_id: instance_id.to_owned(),
                run_id: run_id.to_owned(),
                job_id: job_id.to_owned(),
                handle_id: handle_id.clone(),
                bridge_request_id: fabrik_throughput::stream_job_bridge_request_id(
                    tenant_id,
                    instance_id,
                    run_id,
                    job_id,
                ),
                query_id: "query-a".to_owned(),
                query_name: "accountTotals".to_owned(),
                query_args: Some(serde_json::json!({ "key": "acct_1" })),
                consistency: "strong".to_owned(),
                status: fabrik_throughput::StreamJobQueryStatus::Requested.as_str().to_owned(),
                workflow_owner_epoch: None,
                stream_owner_epoch: None,
                output: None,
                error: None,
                requested_at: query_request.occurred_at,
                completed_at: None,
                accepted_at: None,
                cancelled_at: None,
                created_at: query_request.occurred_at,
                updated_at: query_request.occurred_at,
            })
            .await?;
        bridge::handle_bridge_event(app_state.clone(), query_request).await?;

        let duplicate_query_payload = WorkflowEvent::StreamJobQueryRequested {
            job_id: job_id.to_owned(),
            query_id: "query-a".to_owned(),
            query_name: "accountTotals".to_owned(),
            args: Some(serde_json::json!({ "key": "acct_1" })),
            consistency: "strong".to_owned(),
            state: None,
        };
        let mut duplicate_query_request = EventEnvelope::new(
            duplicate_query_payload.event_type(),
            identity,
            duplicate_query_payload,
        );
        duplicate_query_request.occurred_at = occurred_at + chrono::Duration::milliseconds(2);
        bridge::handle_bridge_event(app_state.clone(), duplicate_query_request).await?;

        let outbox = store
            .lease_workflow_event_outbox(
                "stream-job-query-terminal-test",
                chrono::Duration::seconds(30),
                20,
                occurred_at + chrono::Duration::seconds(1),
            )
            .await?;
        assert_eq!(
            outbox.iter().filter(|record| record.event_type == "StreamJobQueryCompleted").count(),
            1
        );
        assert_eq!(
            outbox.iter().filter(|record| record.event_type == "StreamJobCompleted").count(),
            1
        );

        let debug = app_state.debug.lock().expect("throughput debug lock poisoned").clone();
        assert_eq!(debug.duplicate_stream_job_query_requests_ignored, 1);
        assert_eq!(debug.duplicate_bridge_events, 1);
        assert_eq!(
            debug.last_duplicate_stream_job_query_request.as_deref(),
            Some(
                "tenant:instance-stream-job-query-terminal:run-stream-job-query-terminal:job-stream-job-query-terminal:query-a:completed"
            )
        );
        assert_eq!(
            debug.last_duplicate_stream_job_query_request_at,
            Some(occurred_at + chrono::Duration::milliseconds(2))
        );

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn stream_job_query_outbox_publication_replays_after_reconcile() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-stream-job-query-reconcile");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;

        let tenant_id = "tenant";
        let instance_id = "instance-stream-job-query-reconcile";
        let run_id = "run-stream-job-query-reconcile";
        let job_id = "job-stream-job-query-reconcile";
        let handle_id =
            fabrik_throughput::stream_job_handle_id(tenant_id, instance_id, run_id, job_id);
        let occurred_at = Utc::now();

        store
            .upsert_stream_job_bridge_handle(&StreamJobBridgeHandleRecord {
                workflow_event_id: Uuid::now_v7(),
                protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
                tenant_id: tenant_id.to_owned(),
                instance_id: instance_id.to_owned(),
                run_id: run_id.to_owned(),
                stream_instance_id: instance_id.to_owned(),
                stream_run_id: run_id.to_owned(),
                job_id: job_id.to_owned(),
                handle_id: handle_id.clone(),
                bridge_request_id: fabrik_throughput::stream_job_bridge_request_id(
                    tenant_id,
                    instance_id,
                    run_id,
                    job_id,
                ),
                origin_kind: fabrik_throughput::StreamJobOriginKind::Workflow.as_str().to_owned(),
                definition_id: "payments-rollup".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                job_name: stream_jobs::KEYED_ROLLUP_JOB_NAME.to_owned(),
                input_ref: serde_json::json!({
                    "kind": "bounded_items",
                    "items": [
                        { "accountId": "acct_1", "amount": 2.0 },
                        { "accountId": "acct_1", "amount": 3.0 }
                    ]
                })
                .to_string(),
                config_ref: None,
                checkpoint_policy: None,
                view_definitions: None,
                status: fabrik_throughput::StreamJobBridgeHandleStatus::Admitted
                    .as_str()
                    .to_owned(),
                workflow_owner_epoch: None,
                stream_owner_epoch: None,
                cancellation_requested_at: None,
                cancellation_reason: None,
                terminal_event_id: None,
                terminal_at: None,
                workflow_accepted_at: None,
                terminal_output: None,
                terminal_error: None,
                created_at: occurred_at,
                updated_at: occurred_at,
            })
            .await?;

        let identity = WorkflowIdentity::new(
            tenant_id,
            "payments-rollup",
            1,
            "artifact-a",
            instance_id,
            run_id,
            "throughput-runtime-test",
        );
        let schedule_payload = WorkflowEvent::StreamJobScheduled {
            job_id: job_id.to_owned(),
            job_name: stream_jobs::KEYED_ROLLUP_JOB_NAME.to_owned(),
            input: serde_json::json!({
                "kind": "bounded_items",
                "items": [
                    { "accountId": "acct_1", "amount": 2.0 },
                    { "accountId": "acct_1", "amount": 3.0 }
                ]
            }),
            config: None,
            state: None,
        };
        let mut schedule_event =
            EventEnvelope::new(schedule_payload.event_type(), identity.clone(), schedule_payload);
        schedule_event.occurred_at = occurred_at;
        bridge::handle_bridge_event(app_state.clone(), schedule_event).await?;

        let query_payload = WorkflowEvent::StreamJobQueryRequested {
            job_id: job_id.to_owned(),
            query_id: "query-a".to_owned(),
            query_name: "accountTotals".to_owned(),
            args: Some(serde_json::json!({ "key": "acct_1" })),
            consistency: "strong".to_owned(),
            state: None,
        };
        let mut query_request =
            EventEnvelope::new(query_payload.event_type(), identity, query_payload);
        query_request.occurred_at = occurred_at + chrono::Duration::milliseconds(1);
        store
            .upsert_stream_job_bridge_query(&fabrik_store::StreamJobQueryRecord {
                protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
                workflow_event_id: Uuid::now_v7(),
                tenant_id: tenant_id.to_owned(),
                instance_id: instance_id.to_owned(),
                run_id: run_id.to_owned(),
                job_id: job_id.to_owned(),
                handle_id: handle_id.clone(),
                bridge_request_id: fabrik_throughput::stream_job_bridge_request_id(
                    tenant_id,
                    instance_id,
                    run_id,
                    job_id,
                ),
                query_id: "query-a".to_owned(),
                query_name: "accountTotals".to_owned(),
                query_args: Some(serde_json::json!({ "key": "acct_1" })),
                consistency: "strong".to_owned(),
                status: fabrik_throughput::StreamJobQueryStatus::Requested.as_str().to_owned(),
                workflow_owner_epoch: None,
                stream_owner_epoch: None,
                output: None,
                error: None,
                requested_at: query_request.occurred_at,
                completed_at: None,
                accepted_at: None,
                cancelled_at: None,
                created_at: query_request.occurred_at,
                updated_at: query_request.occurred_at,
            })
            .await?;

        app_state.fail_next_workflow_outbox_writes.store(1, Ordering::SeqCst);
        let error = bridge::handle_bridge_event(app_state.clone(), query_request)
            .await
            .expect_err("query outbox failpoint should force query callback failure");
        assert!(error.to_string().contains("test failpoint: workflow event outbox enqueue failed"));

        let failed_query = store
            .get_stream_job_bridge_query("query-a")
            .await?
            .context("query should exist after failed callback publication")?;
        assert_eq!(
            failed_query.parsed_status(),
            Some(fabrik_throughput::StreamJobQueryStatus::Completed)
        );
        assert!(failed_query.accepted_at.is_none());
        assert!(failed_query.output.is_some());

        let initial_outbox = store
            .lease_workflow_event_outbox(
                "stream-job-query-reconcile-initial",
                chrono::Duration::seconds(30),
                20,
                occurred_at + chrono::Duration::seconds(1),
            )
            .await?;
        assert!(
            !initial_outbox.iter().any(|record| record.event_type == "StreamJobQueryCompleted")
        );
        assert!(!initial_outbox.iter().any(|record| record.event_type == "StreamJobQueryFailed"));
        assert!(!initial_outbox.iter().any(|record| record.event_type == "StreamJobCompleted"));

        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;
        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        let outbox = store
            .lease_workflow_event_outbox(
                "stream-job-query-reconcile-outbox",
                chrono::Duration::seconds(30),
                20,
                occurred_at + chrono::Duration::seconds(2),
            )
            .await?;
        assert_eq!(
            outbox.iter().filter(|record| record.event_type == "StreamJobQueryCompleted").count(),
            1
        );
        assert_eq!(
            outbox.iter().filter(|record| record.event_type == "StreamJobCompleted").count(),
            1
        );
        let query_outbox = outbox
            .iter()
            .find(|record| record.event_type == "StreamJobQueryCompleted")
            .context("query callback should be enqueued after reconcile")?;
        match &query_outbox.event.payload {
            WorkflowEvent::StreamJobQueryCompleted { query_id, query_name, output, .. } => {
                assert_eq!(query_id, "query-a");
                assert_eq!(query_name, "accountTotals");
                assert_eq!(output["accountId"], serde_json::json!("acct_1"));
                assert_eq!(output["totalAmount"], serde_json::json!(5.0));
            }
            other => panic!("unexpected query outbox payload after reconcile: {other:?}"),
        }

        let repaired_query = store
            .get_stream_job_bridge_query("query-a")
            .await?
            .context("query should still exist after reconcile repair")?;
        assert!(repaired_query.accepted_at.is_none());

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn stream_job_signal_outbox_publication_replays_after_reconcile() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-stream-job-signal-reconcile");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;

        let tenant_id = "tenant";
        let instance_id = "instance-stream-job-signal-reconcile";
        let run_id = "run-stream-job-signal-reconcile";
        let job_id = "job-stream-job-signal-reconcile";
        let handle_id =
            fabrik_throughput::stream_job_handle_id(tenant_id, instance_id, run_id, job_id);
        let occurred_at = Utc::now();

        store
            .upsert_stream_job_bridge_handle(&StreamJobBridgeHandleRecord {
                workflow_event_id: Uuid::now_v7(),
                protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
                tenant_id: tenant_id.to_owned(),
                instance_id: instance_id.to_owned(),
                run_id: run_id.to_owned(),
                stream_instance_id: instance_id.to_owned(),
                stream_run_id: run_id.to_owned(),
                job_id: job_id.to_owned(),
                handle_id: handle_id.clone(),
                bridge_request_id: fabrik_throughput::stream_job_bridge_request_id(
                    tenant_id,
                    instance_id,
                    run_id,
                    job_id,
                ),
                origin_kind: fabrik_throughput::StreamJobOriginKind::Workflow.as_str().to_owned(),
                definition_id: "fraud-threshold-signal".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-threshold-signal".to_owned()),
                job_name: "fraud-threshold-signal".to_owned(),
                input_ref: serde_json::json!({
                    "kind": "bounded_items",
                    "items": [
                        { "accountId": "acct_1", "risk": 0.96 },
                        { "accountId": "acct_1", "risk": 0.99 },
                        { "accountId": "acct_2", "risk": 0.72 }
                    ]
                })
                .to_string(),
                config_ref: Some(aggregate_v2_bounded_threshold_signal_job_config()),
                checkpoint_policy: None,
                view_definitions: None,
                status: fabrik_throughput::StreamJobBridgeHandleStatus::Admitted
                    .as_str()
                    .to_owned(),
                workflow_owner_epoch: None,
                stream_owner_epoch: None,
                cancellation_requested_at: None,
                cancellation_reason: None,
                terminal_event_id: None,
                terminal_at: None,
                workflow_accepted_at: None,
                terminal_output: None,
                terminal_error: None,
                created_at: occurred_at,
                updated_at: occurred_at,
            })
            .await?;

        app_state.fail_next_workflow_outbox_writes.store(1, Ordering::SeqCst);
        let error = bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{handle_id}"),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle_id.clone(),
                    },
                ),
            },
        )
        .await
        .expect_err("signal outbox failpoint should force schedule failure");
        assert!(error.to_string().contains("test failpoint: workflow event outbox enqueue failed"));

        let pending_signal = app_state
            .local_state
            .load_stream_job_workflow_signal_state(&handle_id, "notify-fraud", "acct_1")?
            .context("signal state should be durable before callback publication")?;
        assert!(pending_signal.published_at.is_none());

        let failed_handle = store
            .get_stream_job_bridge_handle(tenant_id, instance_id, run_id, job_id)
            .await?
            .context("stream job handle should still exist after failed signal publish")?;
        assert_eq!(
            failed_handle.parsed_status(),
            Some(fabrik_throughput::StreamJobBridgeHandleStatus::Admitted)
        );
        assert!(failed_handle.terminal_at.is_none());

        let initial_outbox = store
            .lease_workflow_event_outbox(
                "stream-job-signal-reconcile-initial",
                chrono::Duration::seconds(30),
                10,
                occurred_at + chrono::Duration::seconds(1),
            )
            .await?;
        assert!(initial_outbox.is_empty());

        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;
        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        let outbox = store
            .lease_workflow_event_outbox(
                "stream-job-signal-reconcile-outbox",
                chrono::Duration::seconds(30),
                20,
                occurred_at + chrono::Duration::seconds(2),
            )
            .await?;
        assert_eq!(outbox.iter().filter(|record| record.event_type == "SignalQueued").count(), 1);
        assert!(!outbox.iter().any(|record| record.event_type == "StreamJobCompleted"));
        let signal_outbox = outbox
            .iter()
            .find(|record| record.event_type == "SignalQueued")
            .context("signal callback should be enqueued after reconcile")?;
        match &signal_outbox.event.payload {
            WorkflowEvent::SignalQueued { signal_type, payload, .. } => {
                assert_eq!(signal_type, "fraud.threshold.crossed");
                assert_eq!(payload["jobId"], serde_json::json!(job_id));
                assert_eq!(payload["logicalKey"], serde_json::json!("acct_1"));
                assert_eq!(payload["output"]["riskExceeded"], serde_json::json!(true));
            }
            other => panic!("unexpected outbox payload after reconcile: {other:?}"),
        }

        let published_signal = app_state
            .local_state
            .load_stream_job_workflow_signal_state(&handle_id, "notify-fraud", "acct_1")?
            .context("signal state should still exist after publish")?;
        assert!(published_signal.published_at.is_some());

        let completed_handle = store
            .get_stream_job_bridge_handle(tenant_id, instance_id, run_id, job_id)
            .await?
            .context("stream job handle should exist after reconcile replay")?;
        assert_eq!(
            completed_handle.parsed_status(),
            Some(fabrik_throughput::StreamJobBridgeHandleStatus::Completed)
        );
        assert!(completed_handle.terminal_at.is_some());

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn stream_job_checkpoint_outbox_publication_replays_after_reconcile() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-stream-job-checkpoint-reconcile");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;

        let tenant_id = "tenant";
        let instance_id = "instance-stream-job-checkpoint-reconcile";
        let run_id = "run-stream-job-checkpoint-reconcile";
        let job_id = "job-stream-job-checkpoint-reconcile";
        let handle_id =
            fabrik_throughput::stream_job_handle_id(tenant_id, instance_id, run_id, job_id);
        let occurred_at = Utc::now();

        store
            .upsert_stream_job_bridge_handle(&StreamJobBridgeHandleRecord {
                workflow_event_id: Uuid::now_v7(),
                protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
                tenant_id: tenant_id.to_owned(),
                instance_id: instance_id.to_owned(),
                run_id: run_id.to_owned(),
                stream_instance_id: instance_id.to_owned(),
                stream_run_id: run_id.to_owned(),
                job_id: job_id.to_owned(),
                handle_id: handle_id.clone(),
                bridge_request_id: fabrik_throughput::stream_job_bridge_request_id(
                    tenant_id,
                    instance_id,
                    run_id,
                    job_id,
                ),
                origin_kind: fabrik_throughput::StreamJobOriginKind::Workflow.as_str().to_owned(),
                definition_id: "payments-rollup".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                job_name: stream_jobs::KEYED_ROLLUP_JOB_NAME.to_owned(),
                input_ref: serde_json::json!({
                    "kind": "bounded_items",
                    "items": [
                        { "accountId": "acct_1", "amount": 2.0 },
                        { "accountId": "acct_1", "amount": 3.0 },
                        { "accountId": "acct_2", "amount": 11.0 }
                    ]
                })
                .to_string(),
                config_ref: None,
                checkpoint_policy: None,
                view_definitions: None,
                status: fabrik_throughput::StreamJobBridgeHandleStatus::Admitted
                    .as_str()
                    .to_owned(),
                workflow_owner_epoch: Some(3),
                stream_owner_epoch: None,
                cancellation_requested_at: None,
                cancellation_reason: None,
                terminal_event_id: None,
                terminal_at: None,
                workflow_accepted_at: None,
                terminal_output: None,
                terminal_error: None,
                created_at: occurred_at,
                updated_at: occurred_at,
            })
            .await?;

        app_state.fail_next_workflow_outbox_writes.store(1, Ordering::SeqCst);
        let error = bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-submit:{handle_id}"),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::ScheduleStreamJob(
                    fabrik_throughput::ScheduleStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle_id.clone(),
                    },
                ),
            },
        )
        .await
        .expect_err("checkpoint outbox failpoint should force schedule failure");
        assert!(error.to_string().contains("test failpoint: workflow event outbox enqueue failed"));

        let checkpoints =
            store.list_stream_job_bridge_checkpoints_for_handle_page(&handle_id, 10, 0).await?;
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(
            checkpoints[0].parsed_status(),
            Some(fabrik_throughput::StreamJobCheckpointStatus::Reached)
        );
        assert!(checkpoints[0].accepted_at.is_none());
        assert!(checkpoints[0].cancelled_at.is_none());

        let failed_handle = store
            .get_stream_job_bridge_handle(tenant_id, instance_id, run_id, job_id)
            .await?
            .context("stream job handle should still exist after failed checkpoint publish")?;
        assert_eq!(
            failed_handle.parsed_status(),
            Some(fabrik_throughput::StreamJobBridgeHandleStatus::Completed)
        );
        assert!(failed_handle.terminal_at.is_some());
        assert!(failed_handle.workflow_accepted_at.is_none());

        let pending_repairs = store.list_pending_stream_job_checkpoint_repairs(10).await?;
        assert_eq!(pending_repairs.len(), 1);
        assert_eq!(pending_repairs[0].await_request_id, checkpoints[0].await_request_id);

        let initial_outbox = store
            .lease_workflow_event_outbox(
                "stream-job-checkpoint-reconcile-initial",
                chrono::Duration::seconds(30),
                10,
                occurred_at + chrono::Duration::seconds(1),
            )
            .await?;
        assert!(initial_outbox.is_empty());

        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;
        bridge::reconcile_active_topic_stream_jobs(&app_state, 64).await?;

        let outbox = store
            .lease_workflow_event_outbox(
                "stream-job-checkpoint-reconcile-outbox",
                chrono::Duration::seconds(30),
                20,
                occurred_at + chrono::Duration::seconds(2),
            )
            .await?;
        assert_eq!(
            outbox
                .iter()
                .filter(|record| record.event_type == "StreamJobCheckpointReached")
                .count(),
            1
        );
        assert!(!outbox.iter().any(|record| record.event_type == "StreamJobCompleted"));
        let checkpoint_outbox = outbox
            .iter()
            .find(|record| record.event_type == "StreamJobCheckpointReached")
            .context("checkpoint callback should be enqueued after reconcile")?;
        match &checkpoint_outbox.event.payload {
            WorkflowEvent::StreamJobCheckpointReached {
                checkpoint_name,
                checkpoint_sequence,
                output,
                ..
            } => {
                assert_eq!(checkpoint_name, stream_jobs::KEYED_ROLLUP_CHECKPOINT_NAME);
                assert_eq!(*checkpoint_sequence, stream_jobs::KEYED_ROLLUP_CHECKPOINT_SEQUENCE);
                assert_eq!(
                    output.as_ref().and_then(|value| value.get("checkpoint")),
                    Some(&serde_json::json!(stream_jobs::KEYED_ROLLUP_CHECKPOINT_NAME))
                );
            }
            other => panic!("unexpected checkpoint outbox payload after reconcile: {other:?}"),
        }

        let repaired_checkpoint = store
            .get_stream_job_bridge_checkpoint(&checkpoints[0].await_request_id)
            .await?
            .context("checkpoint should still exist after reconcile repair")?;
        assert!(repaired_checkpoint.accepted_at.is_none());
        assert!(repaired_checkpoint.cancelled_at.is_none());

        fs::remove_dir_all(state_dir).ok();
        Ok(())
    }

    #[tokio::test]
    async fn standalone_stream_job_cancel_command_cancels_without_workflow_outbox_events()
    -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state_dir = temp_path("throughput-standalone-stream-job-cancel-command");
        let app_state = test_app_state(store.clone(), &redpanda, &state_dir).await?;

        let tenant_id = "tenant";
        let instance_id = "stream-standalone-cancel-command";
        let run_id = "run-standalone-cancel-command";
        let job_id = "job-standalone-cancel-command";
        let handle_id =
            fabrik_throughput::stream_job_handle_id(tenant_id, instance_id, run_id, job_id);
        let occurred_at = Utc::now();

        store
            .upsert_stream_job_bridge_handle(&StreamJobBridgeHandleRecord {
                workflow_event_id: Uuid::now_v7(),
                protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: ThroughputBridgeOperationKind::StreamJob.as_str().to_owned(),
                tenant_id: tenant_id.to_owned(),
                instance_id: instance_id.to_owned(),
                run_id: run_id.to_owned(),
                stream_instance_id: instance_id.to_owned(),
                stream_run_id: run_id.to_owned(),
                job_id: job_id.to_owned(),
                handle_id: handle_id.clone(),
                bridge_request_id: fabrik_throughput::stream_job_bridge_request_id(
                    tenant_id,
                    instance_id,
                    run_id,
                    job_id,
                ),
                origin_kind: fabrik_throughput::StreamJobOriginKind::Standalone.as_str().to_owned(),
                definition_id: "payments-rollup".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                job_name: stream_jobs::KEYED_ROLLUP_JOB_NAME.to_owned(),
                input_ref:
                    r#"{"kind":"bounded_items","items":[{"accountId":"acct_1","amount":2.0}]}"#
                        .to_owned(),
                config_ref: None,
                checkpoint_policy: None,
                view_definitions: None,
                status: fabrik_throughput::StreamJobBridgeHandleStatus::CancellationRequested
                    .as_str()
                    .to_owned(),
                workflow_owner_epoch: None,
                stream_owner_epoch: Some(1),
                cancellation_requested_at: Some(occurred_at),
                cancellation_reason: Some("user requested cancel".to_owned()),
                terminal_event_id: None,
                terminal_at: None,
                workflow_accepted_at: None,
                terminal_output: None,
                terminal_error: None,
                created_at: occurred_at,
                updated_at: occurred_at,
            })
            .await?;

        bridge::handle_throughput_command(
            app_state.clone(),
            ThroughputCommandEnvelope {
                command_id: Uuid::now_v7(),
                occurred_at,
                dedupe_key: format!("stream-job-cancel:{handle_id}"),
                partition_key: throughput_partition_key(job_id, 0),
                payload: ThroughputCommand::CancelStreamJob(
                    fabrik_throughput::CancelStreamJobCommand {
                        tenant_id: tenant_id.to_owned(),
                        stream_instance_id: instance_id.to_owned(),
                        stream_run_id: run_id.to_owned(),
                        job_id: job_id.to_owned(),
                        handle_id: handle_id.clone(),
                        reason: Some("user requested cancel".to_owned()),
                    },
                ),
            },
        )
        .await?;

        let stored_handle = store
            .get_stream_job_bridge_handle(tenant_id, instance_id, run_id, job_id)
            .await?
            .context("standalone stream job handle should exist after cancel command")?;
        assert_eq!(
            stored_handle.parsed_status(),
            Some(fabrik_throughput::StreamJobBridgeHandleStatus::Cancelled)
        );
        assert_eq!(stored_handle.terminal_error.as_deref(), Some("user requested cancel"));

        let stored_job = store
            .get_stream_job(tenant_id, instance_id, run_id, job_id)
            .await?
            .context("standalone stream job record should exist after cancel command")?;
        assert_eq!(stored_job.parsed_status(), Some(fabrik_throughput::StreamJobStatus::Cancelled));
        assert_eq!(stored_job.terminal_error.as_deref(), Some("user requested cancel"));

        let outbox = store
            .lease_workflow_event_outbox(
                "standalone-stream-job-cancel-command-test",
                chrono::Duration::seconds(30),
                10,
                occurred_at + chrono::Duration::seconds(1),
            )
            .await?;
        assert!(outbox.is_empty());

        fs::remove_dir_all(state_dir).ok();
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
        let dedupe_key = throughput_terminal_callback_dedupe_key(
            "tenant-a",
            "wf-a",
            "run-a",
            "batch-a",
            "completed",
        );
        assert_eq!(dedupe_key, "throughput-terminal:tenant-a:wf-a:run-a:batch-a:completed");
        assert_eq!(
            throughput_terminal_callback_event_id(&dedupe_key),
            throughput_terminal_callback_event_id(&dedupe_key)
        );
    }

    #[test]
    fn stream_batch_terminal_identity_changes_when_projected_terminal_changes() {
        let completed = throughput_terminal_callback_dedupe_key(
            "tenant-a",
            "wf-a",
            "run-a",
            "batch-a",
            "completed",
        );
        let failed = throughput_terminal_callback_dedupe_key(
            "tenant-a", "wf-a", "run-a", "batch-a", "failed",
        );

        assert_ne!(completed, failed);
        assert_ne!(
            throughput_terminal_callback_event_id(&completed),
            throughput_terminal_callback_event_id(&failed)
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
        let aggregation = AggregationCoordinator::new(local_state.clone());
        let repaired =
            repair_stale_terminal_projection_batches_in_store(&store, &aggregation, &owned, 8)
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
    StreamProjectionRecord::throughput(
        throughput_partition_key(&chunk.batch_id, chunk.group_id),
        ThroughputProjectionEvent::UpdateChunkState { update: chunk_update },
    )
}

fn running_batch_projection_record(
    batch: &local_state::LocalBatchSnapshot,
) -> StreamProjectionRecord {
    StreamProjectionRecord::throughput(
        throughput_partition_key(&batch.identity.batch_id, 0),
        ThroughputProjectionEvent::UpdateBatchState {
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
    )
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
    StreamProjectionRecord::throughput(
        throughput_partition_key(&identity.batch_id, 0),
        grouped_batch_summary_projection_event(identity, summary, reducer_output),
    )
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
                .throughput_projection_publisher
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
            if let Ok(Some(group_terminal)) = state.aggregation.project_group_terminal(
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
