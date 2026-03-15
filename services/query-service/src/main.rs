use anyhow::{Context, Result};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use fabrik_broker::{
    BrokerConfig, WorkflowHistoryFilter, WorkflowTopicTopology, describe_workflow_topic,
    partition_for_key, read_workflow_history,
};
use fabrik_config::{HttpServiceConfig, PostgresConfig, QueryRuntimeConfig, RedpandaConfig};
use fabrik_events::{EventEnvelope, WorkflowEvent};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::{
    QueryRetentionCutoffs, QueryRetentionPruneResult, StreamArtifactSummary,
    StreamJobBridgeHandleRecord, StreamJobBridgeLedgerRecord, StreamJobCheckpointRecord,
    StreamJobQueryRecord, StreamJobRecord, StreamJobViewProjectionSummaryRecord, TaskQueueKind,
    ThroughputBridgeLedgerRecord, WorkflowActivityRecord, WorkflowActivityStatus,
    WorkflowArtifactSummary, WorkflowBulkBatchRecord, WorkflowBulkBatchRuntimeControlRecord,
    WorkflowBulkBatchStatus, WorkflowBulkChunkRecord, WorkflowChildRecord,
    WorkflowDefinitionSummary, WorkflowInstanceFilters, WorkflowRunFilters, WorkflowRunRecord,
    WorkflowRunSummaryRecord, WorkflowSignalRecord, WorkflowSignalStatus, WorkflowStateSnapshot,
    WorkflowStore, WorkflowUpdateRecord, WorkflowUpdateStatus,
};
use fabrik_throughput::{
    CollectResultsBatchManifest, CompiledStreamJobArtifact, PayloadHandle, PayloadStore,
    PayloadStoreConfig, PayloadStoreKind, ThroughputBackend, throughput_partition_key,
};
use fabrik_workflow::{
    CompiledStateNode, CompiledWorkflowArtifact, ReplayDivergence, ReplayDivergenceKind,
    ReplayFieldMismatch, ReplaySource, ReplayTransitionTraceEntry, SourceLocation,
    WorkflowDefinition, WorkflowInstanceState, artifact_hash, execution_state_for_event,
    first_transition_divergence, projection_mismatches, replay_compiled_history,
    replay_compiled_history_trace, replay_compiled_history_trace_from_snapshot,
    replay_history_trace, replay_history_trace_from_snapshot, same_projection,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, to_value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tracing::info;
use uuid::Uuid;

const HISTORY_IDLE_TIMEOUT_MS: u64 = 1_000;
const HISTORY_MAX_SCAN_MS: u64 = 10_000;

#[derive(Clone)]
struct AppState {
    store: WorkflowStore,
    broker: BrokerConfig,
    query: QueryRuntimeConfig,
    payload_store: PayloadStore,
    client: Client,
    throughput_partitions: i32,
    retention: Arc<Mutex<RetentionDebugState>>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    message: String,
}

#[derive(Debug, Serialize)]
struct WorkflowHistoryResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    definition_id: String,
    definition_version: u32,
    artifact_hash: String,
    previous_run_id: Option<String>,
    next_run_id: Option<String>,
    continue_reason: Option<String>,
    event_count: usize,
    page: PageInfo,
    activity_attempt_count: usize,
    activity_attempts: Vec<WorkflowActivityRecord>,
    events: Vec<EventEnvelope<WorkflowEvent>>,
}

#[derive(Debug, Serialize)]
struct WorkflowReplayResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    definition_id: String,
    definition_version: u32,
    artifact_hash: String,
    previous_run_id: Option<String>,
    next_run_id: Option<String>,
    continue_reason: Option<String>,
    event_count: usize,
    last_event_type: String,
    activity_attempt_count: usize,
    activity_attempts: Vec<WorkflowActivityRecord>,
    projection_matches_store: Option<bool>,
    replay_source: ReplaySource,
    snapshot: Option<ReplaySnapshotSummary>,
    divergence_count: usize,
    divergences: Vec<ReplayDivergence>,
    transition_count: usize,
    transition_trace: Vec<ReplayTransitionTraceEntry>,
    replayed_state: WorkflowInstanceState,
}

#[derive(Debug, Serialize)]
struct ReplaySnapshotSummary {
    run_id: String,
    snapshot_schema_version: u32,
    event_count: i64,
    last_event_id: Uuid,
    last_event_type: String,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct WorkflowActivitiesResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    page: PageInfo,
    activity_count: usize,
    activities: Vec<WorkflowActivityRecord>,
}

#[derive(Debug, Serialize)]
struct WorkflowBulkBatchesResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    consistency: &'static str,
    authoritative_source: &'static str,
    projection_lag_ms: Option<i64>,
    watch_cursor: String,
    page: PageInfo,
    batch_count: usize,
    batches: Vec<WorkflowBulkBatchRecord>,
    bridge_statuses: Vec<WorkflowBulkBridgeStatus>,
}

#[derive(Debug, Serialize)]
struct WorkflowBulkBatchResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    consistency: &'static str,
    authoritative_source: &'static str,
    projection_lag_ms: Option<i64>,
    watch_cursor: String,
    runtime_control: Option<WorkflowBulkBatchRuntimeControlRecord>,
    batch: WorkflowBulkBatchRecord,
    bridge_status: Option<WorkflowBulkBridgeStatus>,
}

#[derive(Debug, Serialize)]
struct WorkflowBulkChunksResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    batch_id: String,
    consistency: &'static str,
    authoritative_source: &'static str,
    projection_lag_ms: Option<i64>,
    watch_cursor: String,
    page: PageInfo,
    chunk_count: usize,
    chunks: Vec<WorkflowBulkChunkRecord>,
    bridge_status: Option<WorkflowBulkBridgeStatus>,
}

#[derive(Debug, Serialize)]
struct WorkflowBulkResultsResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    batch_id: String,
    consistency: &'static str,
    authoritative_source: &'static str,
    projection_lag_ms: Option<i64>,
    watch_cursor: String,
    page: PageInfo,
    chunk_count: usize,
    chunks: Vec<WorkflowBulkChunkRecord>,
    bridge_status: Option<WorkflowBulkBridgeStatus>,
}

#[derive(Debug, Serialize)]
struct WorkflowBulkBridgeStatus {
    batch_id: String,
    protocol_version: String,
    operation_kind: String,
    source_workflow_event_id: Option<Uuid>,
    bridge_request_id: String,
    submission_status: Option<String>,
    submission_consistency: &'static str,
    command_id: Option<Uuid>,
    command_partition_key: Option<String>,
    stream_status: String,
    stream_consistency: &'static str,
    stream_terminal_at: Option<DateTime<Utc>>,
    cancellation_requested_at: Option<DateTime<Utc>>,
    cancellation_reason: Option<String>,
    cancel_command_published_at: Option<DateTime<Utc>>,
    cancelled_at: Option<DateTime<Utc>>,
    workflow_status: Option<String>,
    workflow_consistency: &'static str,
    workflow_terminal_event_id: Option<Uuid>,
    workflow_owner_epoch: Option<u64>,
    workflow_accepted_at: Option<DateTime<Utc>>,
    next_repair: Option<String>,
}

#[derive(Debug, Serialize)]
struct WorkflowBulkBridgeRepairRecord {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    batch_id: String,
    protocol_version: String,
    operation_kind: String,
    source_workflow_event_id: Option<Uuid>,
    bridge_request_id: String,
    next_repair: String,
    submission_status: Option<String>,
    command_id: Option<Uuid>,
    command_partition_key: Option<String>,
    command_published_at: Option<DateTime<Utc>>,
    cancellation_requested_at: Option<DateTime<Utc>>,
    cancellation_reason: Option<String>,
    cancel_command_published_at: Option<DateTime<Utc>>,
    cancelled_at: Option<DateTime<Utc>>,
    stream_status: String,
    stream_terminal_at: Option<DateTime<Utc>>,
    workflow_status: Option<String>,
    workflow_terminal_event_id: Option<Uuid>,
    workflow_owner_epoch: Option<u64>,
    workflow_accepted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
struct WorkflowBulkBridgeRepairsResponse {
    throughput_backend: String,
    page: PageInfo,
    repair_count: usize,
    repairs: Vec<WorkflowBulkBridgeRepairRecord>,
}

#[derive(Debug, Serialize)]
struct StreamJobBridgeHandleSummary {
    protocol_version: String,
    operation_kind: String,
    source_workflow_event_id: Uuid,
    bridge_request_id: String,
    handle_id: String,
    job_id: String,
    definition_id: String,
    definition_version: Option<u32>,
    artifact_hash: Option<String>,
    job_name: String,
    input_ref: String,
    config_ref: Option<String>,
    checkpoint_policy: Option<Value>,
    view_definitions: Option<Value>,
    status: String,
    workflow_owner_epoch: Option<u64>,
    stream_owner_epoch: Option<u64>,
    cancellation_requested_at: Option<DateTime<Utc>>,
    cancellation_reason: Option<String>,
    terminal_event_id: Option<Uuid>,
    terminal_at: Option<DateTime<Utc>>,
    workflow_accepted_at: Option<DateTime<Utc>>,
    checkpoint_count: usize,
    query_count: usize,
    pending_repair_count: usize,
    pending_repairs: Vec<String>,
    next_repair: Option<String>,
    latest_query_id: Option<String>,
    latest_query_name: Option<String>,
    latest_query_status: Option<String>,
    latest_query_consistency: Option<String>,
    latest_query_requested_at: Option<DateTime<Utc>>,
    latest_query_completed_at: Option<DateTime<Utc>>,
    latest_query_accepted_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct StreamJobBridgeCheckpointView {
    protocol_version: String,
    operation_kind: String,
    source_workflow_event_id: Uuid,
    bridge_request_id: String,
    await_request_id: String,
    checkpoint_name: String,
    checkpoint_sequence: Option<i64>,
    status: String,
    workflow_owner_epoch: Option<u64>,
    stream_owner_epoch: Option<u64>,
    reached_at: Option<DateTime<Utc>>,
    output: Option<Value>,
    accepted_at: Option<DateTime<Utc>>,
    cancelled_at: Option<DateTime<Utc>>,
    next_repair: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct StreamJobBridgeQueryView {
    protocol_version: String,
    operation_kind: String,
    source_workflow_event_id: Uuid,
    bridge_request_id: String,
    query_id: String,
    query_name: String,
    query_args: Option<Value>,
    consistency: String,
    status: String,
    workflow_owner_epoch: Option<u64>,
    stream_owner_epoch: Option<u64>,
    output: Option<Value>,
    error: Option<String>,
    requested_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
    accepted_at: Option<DateTime<Utc>>,
    cancelled_at: Option<DateTime<Utc>>,
    next_repair: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct StreamJobBridgeHandlesResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    page: PageInfo,
    handle_count: usize,
    handles: Vec<StreamJobBridgeHandleSummary>,
}

#[derive(Debug, Serialize)]
struct StreamJobBridgeRepairsResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    page: PageInfo,
    repair_count: usize,
    handles: Vec<StreamJobBridgeHandleSummary>,
}

#[derive(Debug, Serialize)]
struct StreamJobBridgeHandleResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    job_id: String,
    handle: StreamJobBridgeHandleSummary,
    checkpoints: Vec<StreamJobBridgeCheckpointView>,
    queries: Vec<StreamJobBridgeQueryView>,
}

#[derive(Debug, Clone, Serialize)]
struct StreamJobSummary {
    protocol_version: String,
    operation_kind: String,
    origin_kind: String,
    stream_instance_id: String,
    stream_run_id: String,
    source_workflow_event_id: Uuid,
    bridge_request_id: String,
    handle_id: String,
    job_id: String,
    definition_id: String,
    definition_version: Option<u32>,
    artifact_hash: Option<String>,
    job_name: String,
    input_ref: String,
    config_ref: Option<String>,
    checkpoint_policy: Option<Value>,
    declared_checkpoints: Vec<StreamJobCheckpointSummary>,
    view_definitions: Option<Value>,
    stream_surface: StreamJobSurfaceSummary,
    bridge_surface: StreamJobBridgeSurfaceSummary,
    status: String,
    workflow_owner_epoch: Option<u64>,
    stream_owner_epoch: Option<u64>,
    starting_at: Option<DateTime<Utc>>,
    running_at: Option<DateTime<Utc>>,
    draining_at: Option<DateTime<Utc>>,
    latest_checkpoint_name: Option<String>,
    latest_checkpoint_sequence: Option<i64>,
    latest_checkpoint_at: Option<DateTime<Utc>>,
    latest_checkpoint_output: Option<Value>,
    cancellation_requested_at: Option<DateTime<Utc>>,
    cancellation_reason: Option<String>,
    workflow_accepted_at: Option<DateTime<Utc>>,
    terminal_event_id: Option<Uuid>,
    terminal_at: Option<DateTime<Utc>>,
    terminal_output: Option<Value>,
    terminal_error: Option<String>,
    checkpoint_count: usize,
    query_count: usize,
    latest_query_id: Option<String>,
    latest_query_name: Option<String>,
    latest_query_status: Option<String>,
    latest_query_consistency: Option<String>,
    latest_query_requested_at: Option<DateTime<Utc>>,
    latest_query_completed_at: Option<DateTime<Utc>>,
    latest_query_accepted_at: Option<DateTime<Utc>>,
    views: Vec<StreamJobViewSummary>,
    workflow_binding: Option<StreamJobWorkflowBinding>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct StreamJobWorkflowBinding {
    instance_id: String,
    run_id: String,
}

#[derive(Debug, Clone, Serialize)]
struct StreamJobBridgeSurfaceSummary {
    pending_repair_count: usize,
    pending_repairs: Vec<String>,
    next_repair: Option<String>,
    latest_query_id: Option<String>,
    latest_query_name: Option<String>,
    latest_query_status: Option<String>,
    latest_query_consistency: Option<String>,
    latest_query_requested_at: Option<DateTime<Utc>>,
    latest_query_completed_at: Option<DateTime<Utc>>,
    latest_query_accepted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
struct StreamJobCheckpointSummary {
    checkpoint_name: String,
    definition: Option<Value>,
    delivery: Option<String>,
    sequence: Option<i64>,
    reached: bool,
    reached_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
struct StreamJobSurfaceSummary {
    declared_checkpoint_count: usize,
    reached_checkpoint_count: usize,
    latest_checkpoint_name: Option<String>,
    latest_checkpoint_sequence: Option<i64>,
    latest_checkpoint_at: Option<DateTime<Utc>>,
    view_count: usize,
    projected_view_count: usize,
    total_projected_keys: usize,
    slowest_eventual_view_lag_ms: Option<i64>,
    checkpoints: Vec<StreamJobCheckpointSummary>,
    views: Vec<StreamJobViewSummary>,
}

#[derive(Debug, Serialize)]
struct StreamJobsResponse {
    tenant_id: String,
    instance_id: String,
    stream_instance_id: String,
    run_id: String,
    stream_run_id: String,
    page: PageInfo,
    job_count: usize,
    jobs: Vec<StreamJobSummary>,
}

#[derive(Debug, Serialize)]
struct TenantStreamJobsResponse {
    tenant_id: String,
    page: PageInfo,
    job_count: usize,
    jobs: Vec<StreamJobSummary>,
}

#[derive(Debug, Serialize)]
struct StreamJobResponse {
    tenant_id: String,
    instance_id: String,
    stream_instance_id: String,
    run_id: String,
    stream_run_id: String,
    job_id: String,
    job: StreamJobSummary,
    views: Vec<StreamJobViewSummary>,
    checkpoints: Vec<StreamJobBridgeCheckpointView>,
    queries: Vec<StreamJobBridgeQueryView>,
}

#[derive(Debug, Clone, Serialize)]
struct StreamJobViewSummary {
    view_name: String,
    definition: Option<Value>,
    projected_key_count: usize,
    latest_projected_checkpoint_sequence: Option<i64>,
    latest_projected_at: Option<DateTime<Utc>>,
    eventual_projection_lag_ms: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StrongStreamJobViewResponse {
    handle_id: String,
    job_id: String,
    view_name: String,
    logical_key: String,
    output: Value,
    checkpoint_sequence: i64,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct StrongStreamJobViewKeysResponse {
    total: usize,
    keys: Vec<StreamJobViewKeyRecord>,
}

#[derive(Debug, Deserialize)]
struct StrongStreamJobViewEntriesResponse {
    total: usize,
    entries: Vec<StreamJobViewEntryRecord>,
}

#[derive(Debug, Serialize)]
struct StreamJobViewResponse {
    handle_id: String,
    job_id: String,
    view_name: String,
    logical_key: String,
    consistency: String,
    source: String,
    projection_lag_ms: Option<i64>,
    watch_cursor: String,
    output: Value,
    checkpoint_sequence: i64,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StreamJobViewKeyRecord {
    logical_key: String,
    checkpoint_sequence: i64,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct StreamJobViewKeysResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    job_id: String,
    view_name: String,
    consistency: String,
    source: String,
    projection_lag_ms: Option<i64>,
    watch_cursor: String,
    page: PageInfo,
    key_count: usize,
    keys: Vec<StreamJobViewKeyRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StreamJobViewEntryRecord {
    logical_key: String,
    output: Value,
    checkpoint_sequence: i64,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct StreamJobViewEntriesResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    job_id: String,
    view_name: String,
    consistency: String,
    source: String,
    projection_lag_ms: Option<i64>,
    watch_cursor: String,
    page: PageInfo,
    entry_count: usize,
    entries: Vec<StreamJobViewEntryRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StreamJobViewScanItemRecord {
    logical_key: String,
    output: Value,
    checkpoint_sequence: i64,
    updated_at: DateTime<Utc>,
    window_expired_at: Option<DateTime<Utc>>,
    window_retention_seconds: Option<i64>,
}

#[derive(Debug, Serialize)]
struct StreamJobViewScanResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    job_id: String,
    view_name: String,
    consistency: String,
    source: String,
    projection_lag_ms: Option<i64>,
    watch_cursor: String,
    prefix: String,
    page: PageInfo,
    item_count: usize,
    items: Vec<StreamJobViewScanItemRecord>,
    runtime_stats: Option<Value>,
}

#[derive(Debug, Serialize)]
struct WorkflowSignalsResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    page: PageInfo,
    signal_count: usize,
    signals: Vec<WorkflowSignalRecord>,
}

#[derive(Debug, Serialize)]
struct WorkflowUpdatesResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    page: PageInfo,
    update_count: usize,
    updates: Vec<WorkflowUpdateRecord>,
}

#[derive(Debug, Serialize)]
struct WorkflowUpdateResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    update: WorkflowUpdateRecord,
}

#[derive(Debug, Serialize)]
struct WorkflowRunsResponse {
    tenant_id: String,
    instance_id: String,
    current_run_id: Option<String>,
    page: PageInfo,
    run_count: usize,
    runs: Vec<WorkflowRunRecord>,
}

#[derive(Debug, Serialize)]
struct WorkflowListItem {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    definition_id: String,
    definition_version: Option<u32>,
    artifact_hash: Option<String>,
    workflow_task_queue: String,
    memo: Option<Value>,
    search_attributes: Option<Value>,
    sticky_workflow_build_id: Option<String>,
    sticky_workflow_poller_id: Option<String>,
    routing_status: String,
    admission_mode: Option<String>,
    fast_path_rejection_reason: Option<String>,
    current_state: Option<String>,
    status: String,
    event_count: i64,
    last_event_id: Uuid,
    last_event_type: String,
    updated_at: DateTime<Utc>,
    consistency: &'static str,
    source: &'static str,
}

#[derive(Debug, Serialize)]
struct WorkflowListResponse {
    tenant_id: String,
    consistency: &'static str,
    authoritative_source: &'static str,
    page: PageInfo,
    workflow_count: usize,
    items: Vec<WorkflowListItem>,
}

#[derive(Debug, Serialize)]
struct RunListItem {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    definition_id: String,
    definition_version: Option<u32>,
    artifact_hash: Option<String>,
    workflow_task_queue: String,
    memo: Option<Value>,
    search_attributes: Option<Value>,
    sticky_workflow_build_id: Option<String>,
    sticky_workflow_poller_id: Option<String>,
    routing_status: String,
    admission_mode: String,
    fast_path_rejection_reason: Option<String>,
    sticky_updated_at: Option<DateTime<Utc>>,
    previous_run_id: Option<String>,
    next_run_id: Option<String>,
    continue_reason: Option<String>,
    started_at: DateTime<Utc>,
    closed_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
    last_transition_at: DateTime<Utc>,
    status: String,
    current_state: Option<String>,
    last_event_type: Option<String>,
    event_count: Option<i64>,
    consistency: &'static str,
    source: &'static str,
}

#[derive(Debug, Serialize)]
struct RunListResponse {
    tenant_id: String,
    consistency: &'static str,
    authoritative_source: &'static str,
    page: PageInfo,
    run_count: usize,
    items: Vec<RunListItem>,
}

#[derive(Debug, Serialize)]
struct WorkflowDefinitionSummariesResponse {
    tenant_id: String,
    items: Vec<WorkflowDefinitionSummary>,
}

#[derive(Debug, Serialize)]
struct WorkflowArtifactSummariesResponse {
    tenant_id: String,
    items: Vec<WorkflowArtifactSummary>,
}

#[derive(Debug, Serialize)]
struct StreamArtifactSummariesResponse {
    tenant_id: String,
    items: Vec<StreamArtifactSummary>,
}

#[derive(Debug, Serialize)]
struct OverviewTaskQueueSummary {
    queue_kind: String,
    task_queue: String,
    backlog: u64,
    poller_count: usize,
    registered_build_count: usize,
    default_set_id: Option<String>,
    throughput_backend: Option<String>,
    oldest_backlog_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
struct OverviewSummaryResponse {
    tenant_id: String,
    consistency: &'static str,
    authoritative_source: &'static str,
    total_workflows: u64,
    total_workflow_definitions: u64,
    total_workflow_artifacts: u64,
    counts_by_status: BTreeMap<String, u64>,
    total_task_queues: usize,
    total_backlog: u64,
    total_pollers: usize,
    total_registered_builds: usize,
    replay_divergence_count: u64,
    recent_failures: Vec<WorkflowListItem>,
    hottest_task_queues: Vec<OverviewTaskQueueSummary>,
}

#[derive(Debug, Serialize)]
struct WorkflowExecutionGraphNode {
    id: String,
    kind: String,
    label: String,
    status: String,
    subtitle: Option<String>,
}

#[derive(Debug, Serialize)]
struct WorkflowExecutionGraphEdge {
    id: String,
    source: String,
    target: String,
    label: String,
}

#[derive(Debug, Serialize)]
struct WorkflowExecutionGraphResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    consistency: &'static str,
    authoritative_source: &'static str,
    nodes: Vec<WorkflowExecutionGraphNode>,
    edges: Vec<WorkflowExecutionGraphEdge>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphSourceAnchor {
    file: String,
    line: u32,
    column: u32,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphNode {
    id: String,
    graph: String,
    module_id: String,
    state_id: Option<String>,
    kind: String,
    label: String,
    subtitle: Option<String>,
    source_anchor: Option<WorkflowGraphSourceAnchor>,
    next_ids: Vec<String>,
    raw: Value,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphEdge {
    id: String,
    source: String,
    target: String,
    label: String,
    kind: String,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphModule {
    id: String,
    graph: String,
    kind: String,
    label: String,
    subtitle: Option<String>,
    node_ids: Vec<String>,
    state_ids: Vec<String>,
    focus_node_id: String,
    collapsed_by_default: bool,
    source_anchor: Option<WorkflowGraphSourceAnchor>,
    raw: Value,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphOverlayStatus {
    id: String,
    status: String,
    summary: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphBlockedBy {
    kind: String,
    label: String,
    detail: Option<String>,
    node_id: Option<String>,
    module_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphTraceStep {
    id: String,
    occurred_at: DateTime<Utc>,
    lane: String,
    label: String,
    detail: Option<String>,
    event_type: String,
    node_id: Option<String>,
    module_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphActivitySummary {
    node_id: String,
    module_id: String,
    activity_type: String,
    total: usize,
    pending: usize,
    completed: usize,
    failed: usize,
    retrying: usize,
    worker_build_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphBulkSummary {
    node_id: String,
    module_id: String,
    batch_id: String,
    activity_type: String,
    status: String,
    total_items: u32,
    succeeded_items: u32,
    failed_items: u32,
    cancelled_items: u32,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphSignalSummary {
    signal_name: String,
    count: usize,
    latest_status: String,
    latest_seen_at: DateTime<Utc>,
    node_id: Option<String>,
    module_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphUpdateSummary {
    update_name: String,
    count: usize,
    latest_status: String,
    latest_seen_at: DateTime<Utc>,
    module_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphChildSummary {
    child_id: String,
    child_definition_id: String,
    child_workflow_id: String,
    child_run_id: Option<String>,
    status: String,
    node_id: Option<String>,
    module_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphOverlay {
    mode: String,
    run_id: Option<String>,
    current_node_id: Option<String>,
    current_module_id: Option<String>,
    blocked_by: Option<WorkflowGraphBlockedBy>,
    node_statuses: Vec<WorkflowGraphOverlayStatus>,
    module_statuses: Vec<WorkflowGraphOverlayStatus>,
    trace: Vec<WorkflowGraphTraceStep>,
    activity_summaries: Vec<WorkflowGraphActivitySummary>,
    bulk_summaries: Vec<WorkflowGraphBulkSummary>,
    signal_summaries: Vec<WorkflowGraphSignalSummary>,
    update_summaries: Vec<WorkflowGraphUpdateSummary>,
    child_summaries: Vec<WorkflowGraphChildSummary>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkflowGraphResponse {
    tenant_id: String,
    definition_id: String,
    definition_version: u32,
    artifact_hash: String,
    entrypoint_module: String,
    entrypoint_export: String,
    consistency: &'static str,
    authoritative_source: &'static str,
    source_files: Vec<String>,
    nodes: Vec<WorkflowGraphNode>,
    edges: Vec<WorkflowGraphEdge>,
    modules: Vec<WorkflowGraphModule>,
    module_edges: Vec<WorkflowGraphEdge>,
    overlay: WorkflowGraphOverlay,
}

#[derive(Debug, Deserialize, Default)]
struct StrongQueryRequest {
    #[serde(default)]
    args: Value,
}

#[derive(Debug, Serialize)]
struct StrongQueryResponse {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    query_name: String,
    consistency: String,
    source: String,
    result: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct InternalQueryRequest {
    #[serde(default)]
    args: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct InternalQueryResponse {
    result: Value,
    consistency: String,
    source: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct PaginationQuery {
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct StreamJobBridgeListQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    status: Option<String>,
    next_repair: Option<String>,
    latest_query_status: Option<String>,
    latest_query_name: Option<String>,
    latest_query_consistency: Option<String>,
    sort: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct StreamJobListQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    status: Option<String>,
    job_name: Option<String>,
    definition_id: Option<String>,
    origin_kind: Option<String>,
    instance_id: Option<String>,
    run_id: Option<String>,
    stream_instance_id: Option<String>,
    stream_run_id: Option<String>,
    sort: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct StreamJobViewQuery {
    consistency: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
    prefix: Option<String>,
    #[serde(alias = "logicalKeyPrefix", alias = "logical_key_prefix")]
    logical_key_prefix: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct WorkflowListQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    status: Option<String>,
    definition_id: Option<String>,
    task_queue: Option<String>,
    routing_status: Option<String>,
    updated_after: Option<DateTime<Utc>>,
    q: Option<String>,
    memo_key: Option<String>,
    memo_value: Option<String>,
    search_attribute_key: Option<String>,
    search_attribute_value: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RunListQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    status: Option<String>,
    definition_id: Option<String>,
    instance_id: Option<String>,
    run_id: Option<String>,
    task_queue: Option<String>,
    routing_status: Option<String>,
    q: Option<String>,
    memo_key: Option<String>,
    memo_value: Option<String>,
    search_attribute_key: Option<String>,
    search_attribute_value: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct SearchableListQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    q: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct BulkReadQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    consistency: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ConsistencyQuery {
    consistency: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadConsistency {
    Eventual,
    Strong,
}

impl ReadConsistency {
    fn as_str(self) -> &'static str {
        match self {
            Self::Eventual => "eventual",
            Self::Strong => "strong",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ResolvedPage {
    limit: usize,
    offset: usize,
}

#[derive(Debug, Clone, Serialize)]
struct PageInfo {
    limit: usize,
    offset: usize,
    returned: usize,
    total: usize,
    has_more: bool,
    next_offset: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
struct RetentionPolicyResponse {
    history_retention_days: Option<u64>,
    run_retention_days: Option<u64>,
    activity_retention_days: Option<u64>,
    signal_retention_days: Option<u64>,
    snapshot_retention_days: Option<u64>,
    retention_sweep_interval_seconds: u64,
}

#[derive(Debug, Clone, Serialize)]
struct RetentionDebugResponse {
    policy: RetentionPolicyResponse,
    last_sweep_at: Option<DateTime<Utc>>,
    last_result: Option<QueryRetentionPruneResult>,
}

#[derive(Debug, Default)]
struct RetentionDebugState {
    last_sweep_at: Option<DateTime<Utc>>,
    last_result: Option<QueryRetentionPruneResult>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = HttpServiceConfig::from_env("QUERY_SERVICE", "query-service", 3005)?;
    let postgres = PostgresConfig::from_env()?;
    let redpanda = RedpandaConfig::from_env()?;
    let query = QueryRuntimeConfig::from_env()?;
    init_tracing(&config.log_filter);
    info!(
        port = config.port,
        default_page_size = query.default_page_size,
        max_page_size = query.max_page_size,
        history_retention_days = ?query.history_retention_days,
        run_retention_days = ?query.run_retention_days,
        activity_retention_days = ?query.activity_retention_days,
        signal_retention_days = ?query.signal_retention_days,
        snapshot_retention_days = ?query.snapshot_retention_days,
        retention_sweep_interval_seconds = query.retention_sweep_interval_seconds,
        "starting query service"
    );

    let store = WorkflowStore::connect(&postgres.url).await?;
    store.init().await?;
    let payload_store = PayloadStore::from_config(build_payload_store_config(&query)).await?;
    let broker = BrokerConfig::new(
        redpanda.brokers,
        redpanda.workflow_events_topic,
        redpanda.workflow_events_partitions,
    );
    let retention = Arc::new(Mutex::new(RetentionDebugState::default()));
    tokio::spawn(run_retention_sweeper(store.clone(), query.clone(), retention.clone()));

    let app = default_router::<AppState>(ServiceInfo::new(
        config.name,
        "query",
        env!("CARGO_PKG_VERSION"),
    ))
    .route("/tenants", get(list_tenants))
    .route("/tenants/{tenant_id}/overview", get(get_overview_summary))
    .route("/tenants/{tenant_id}/workflows", get(list_workflows))
    .route("/tenants/{tenant_id}/runs", get(list_runs))
    .route(
        "/tenants/{tenant_id}/workflow-definitions/{definition_id}/latest",
        get(get_latest_workflow_definition),
    )
    .route(
        "/tenants/{tenant_id}/workflow-definitions/{definition_id}/graph",
        get(get_workflow_definition_graph),
    )
    .route(
        "/tenants/{tenant_id}/workflow-definitions",
        get(list_workflow_definition_summaries),
    )
    .route(
        "/tenants/{tenant_id}/workflow-definitions/{definition_id}/versions/{version}",
        get(get_workflow_definition_version),
    )
    .route(
        "/tenants/{tenant_id}/workflow-artifacts/{definition_id}/latest",
        get(get_latest_workflow_artifact),
    )
    .route(
        "/tenants/{tenant_id}/workflow-artifacts",
        get(list_workflow_artifact_summaries),
    )
    .route(
        "/tenants/{tenant_id}/workflow-artifacts/{definition_id}/versions/{version}",
        get(get_workflow_artifact_version),
    )
    .route(
        "/tenants/{tenant_id}/stream-artifacts/{definition_id}/latest",
        get(get_latest_stream_artifact),
    )
    .route(
        "/tenants/{tenant_id}/stream-artifacts",
        get(list_stream_artifact_summaries),
    )
    .route(
        "/tenants/{tenant_id}/stream-artifacts/{definition_id}/versions/{version}",
        get(get_stream_artifact_version),
    )
    .route(
        "/tenants/{tenant_id}/streams/jobs",
        get(get_tenant_stream_jobs),
    )
    .route(
        "/tenants/{tenant_id}/stream-jobs",
        get(get_tenant_stream_jobs),
    )
    .route(
        "/tenants/{tenant_id}/streams/jobs/{instance_id}/{run_id}",
        get(get_stream_jobs),
    )
    .route(
        "/tenants/{tenant_id}/stream-jobs/{instance_id}/{run_id}",
        get(get_stream_jobs),
    )
    .route(
        "/tenants/{tenant_id}/streams/jobs/{instance_id}/{run_id}/{job_id}",
        get(get_stream_job),
    )
    .route(
        "/tenants/{tenant_id}/stream-jobs/{instance_id}/{run_id}/{job_id}",
        get(get_stream_job),
    )
    .route(
        "/tenants/{tenant_id}/streams/jobs/{instance_id}/{run_id}/{job_id}/views/{view_name}/keys/{logical_key}",
        get(get_stream_job_view),
    )
    .route(
        "/tenants/{tenant_id}/stream-jobs/{instance_id}/{run_id}/{job_id}/views/{view_name}/keys/{logical_key}",
        get(get_stream_job_view),
    )
    .route(
        "/tenants/{tenant_id}/streams/jobs/{instance_id}/{run_id}/{job_id}/views/{view_name}/keys",
        get(get_stream_job_view_keys),
    )
    .route(
        "/tenants/{tenant_id}/stream-jobs/{instance_id}/{run_id}/{job_id}/views/{view_name}/keys",
        get(get_stream_job_view_keys),
    )
    .route(
        "/tenants/{tenant_id}/streams/jobs/{instance_id}/{run_id}/{job_id}/views/{view_name}/scan",
        get(get_stream_job_view_scan),
    )
    .route(
        "/tenants/{tenant_id}/stream-jobs/{instance_id}/{run_id}/{job_id}/views/{view_name}/scan",
        get(get_stream_job_view_scan),
    )
    .route(
        "/tenants/{tenant_id}/streams/jobs/{instance_id}/{run_id}/{job_id}/views/{view_name}/entries",
        get(get_stream_job_view_entries),
    )
    .route(
        "/tenants/{tenant_id}/stream-jobs/{instance_id}/{run_id}/{job_id}/views/{view_name}/entries",
        get(get_stream_job_view_entries),
    )
    .route("/debug/broker", get(get_broker_debug))
    .route(
        "/debug/streams-callbacks/repairs/{throughput_backend}",
        get(get_pending_stream_bridge_repairs),
    )
    .route(
        "/debug/streams-callbacks/jobs/{tenant_id}/{instance_id}/{run_id}",
        get(get_stream_job_callback_handles),
    )
    .route(
        "/debug/streams-callbacks/jobs/{tenant_id}/{instance_id}/{run_id}/repairs",
        get(get_stream_job_callback_repairs),
    )
    .route(
        "/debug/streams-callbacks/jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}",
        get(get_stream_job_callback_handle),
    )
    .route(
        "/debug/streams-callbacks/jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}/views/{view_name}/keys/{logical_key}",
        get(get_strong_stream_job_view),
    )
    .route(
        "/debug/streams-bridge/repairs/{throughput_backend}",
        get(get_pending_stream_bridge_repairs),
    )
    .route(
        "/debug/streams-bridge/jobs/{tenant_id}/{instance_id}/{run_id}",
        get(get_stream_job_callback_handles),
    )
    .route(
        "/debug/streams-bridge/jobs/{tenant_id}/{instance_id}/{run_id}/repairs",
        get(get_stream_job_callback_repairs),
    )
    .route(
        "/debug/streams-bridge/jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}",
        get(get_stream_job_callback_handle),
    )
    .route(
        "/debug/streams-bridge/jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}/views/{view_name}/keys/{logical_key}",
        get(get_strong_stream_job_view),
    )
    .route("/debug/retention", get(get_retention_debug))
    .route("/tenants/{tenant_id}/workflows/{instance_id}", get(get_workflow_instance))
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/queries/{query_name}",
        post(execute_strong_query),
    )
    .route("/tenants/{tenant_id}/workflows/{instance_id}/runs", get(get_workflow_runs))
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/snapshot",
        get(get_latest_workflow_snapshot),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/activities",
        get(get_current_workflow_activities),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/signals",
        get(get_current_workflow_signals),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/updates",
        get(get_current_workflow_updates),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/activities",
        get(get_workflow_activities_for_run),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/signals",
        get(get_workflow_signals_for_run),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/updates",
        get(get_workflow_updates_for_run),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/updates/{update_id}",
        get(get_current_workflow_update),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/updates/{update_id}",
        get(get_workflow_update_for_run),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/bulk-batches",
        get(get_workflow_bulk_batches_for_run),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/bulk-batches/{batch_id}",
        get(get_workflow_bulk_batch_for_run),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/bulk-batches/{batch_id}/chunks",
        get(get_workflow_bulk_chunks_for_run),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/bulk-batches/{batch_id}/results",
        get(get_workflow_bulk_results_for_run),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/history",
        get(get_current_workflow_history),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/history",
        get(get_workflow_history_for_run),
    )
    .route("/tenants/{tenant_id}/workflows/{instance_id}/replay", get(get_current_workflow_replay))
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/replay",
        get(get_workflow_replay_for_run),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/runs/{run_id}/graph",
        get(get_workflow_run_graph),
    )
    .route(
        "/tenants/{tenant_id}/workflows/{instance_id}/execution-graph",
        get(get_workflow_execution_graph),
    )
    .with_state(AppState {
        store,
        broker,
        query,
        payload_store,
        client: Client::new(),
        throughput_partitions: redpanda.throughput_partitions,
        retention,
    });

    serve(app, config.port).await
}

async fn get_broker_debug(
    State(state): State<AppState>,
) -> Result<Json<WorkflowTopicTopology>, (StatusCode, Json<ErrorResponse>)> {
    let topology = describe_workflow_topic(&state.broker, "query-service-debug")
        .await
        .map_err(internal_error)?;
    Ok(Json(topology))
}

async fn get_retention_debug(
    State(state): State<AppState>,
) -> Result<Json<RetentionDebugResponse>, (StatusCode, Json<ErrorResponse>)> {
    let snapshot = state.retention.lock().map_err(|_| {
        internal_error(anyhow::anyhow!("query retention debug state lock poisoned"))
    })?;
    Ok(Json(RetentionDebugResponse {
        policy: retention_policy_response(&state.query),
        last_sweep_at: snapshot.last_sweep_at,
        last_result: snapshot.last_result.clone(),
    }))
}

async fn get_pending_stream_bridge_repairs(
    Path(throughput_backend): Path<String>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowBulkBridgeRepairsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_pending_stream_bridge_repairs(&state, &throughput_backend, pagination)
        .await
        .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_stream_jobs(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    Query(query): Query<StreamJobListQuery>,
    State(state): State<AppState>,
) -> Result<Json<StreamJobsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_stream_jobs(&state, &tenant_id, &instance_id, &run_id, query)
        .await
        .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_tenant_stream_jobs(
    Path(tenant_id): Path<String>,
    Query(query): Query<StreamJobListQuery>,
    State(state): State<AppState>,
) -> Result<Json<TenantStreamJobsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response =
        load_tenant_stream_jobs(&state, &tenant_id, query).await.map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_stream_job(
    Path((tenant_id, instance_id, run_id, job_id)): Path<(String, String, String, String)>,
    State(state): State<AppState>,
) -> Result<Json<StreamJobResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_stream_job(&state, &tenant_id, &instance_id, &run_id, &job_id)
        .await
        .map_err(|error| {
            if error.to_string().contains("not found") {
                not_found(error.to_string())
            } else {
                internal_error(error)
            }
        })?;
    Ok(Json(response))
}

async fn get_stream_job_view(
    Path((tenant_id, instance_id, run_id, job_id, view_name, logical_key)): Path<(
        String,
        String,
        String,
        String,
        String,
        String,
    )>,
    Query(query): Query<StreamJobViewQuery>,
    State(state): State<AppState>,
) -> Result<Json<StreamJobViewResponse>, (StatusCode, Json<ErrorResponse>)> {
    let job = resolve_stream_job(&state, &tenant_id, &instance_id, &run_id, &job_id)
        .await
        .map_err(|error| {
            if error.to_string().contains("not found") {
                not_found(error.to_string())
            } else {
                internal_error(error)
            }
        })?;
    let consistency =
        parse_read_consistency(Some(query.consistency.as_deref().unwrap_or("strong")))
            .map_err(invalid_request)?;
    let response = match consistency {
        ReadConsistency::Strong => {
            let response = fetch_strong_stream_job_view(
                &state,
                &tenant_id,
                &job.instance_id,
                &job.run_id,
                &job_id,
                &view_name,
                &logical_key,
            )
            .await
            .map_err(|error| {
                if error.to_string().contains("not found") {
                    not_found(error.to_string())
                } else {
                    internal_error(error)
                }
            })?;
            StreamJobViewResponse {
                handle_id: response.handle_id,
                job_id: response.job_id,
                view_name: response.view_name,
                logical_key: response.logical_key,
                consistency: "strong".to_owned(),
                source: "stream-owner".to_owned(),
                projection_lag_ms: None,
                watch_cursor: response.updated_at.to_rfc3339(),
                output: response.output,
                checkpoint_sequence: response.checkpoint_sequence,
                updated_at: response.updated_at,
            }
        }
        ReadConsistency::Eventual => {
            let response = load_eventual_stream_job_view(
                &state,
                &tenant_id,
                &job.instance_id,
                &job.run_id,
                &job_id,
                &view_name,
                &logical_key,
            )
            .await
            .map_err(|error| {
                if error.to_string().contains("not found") {
                    not_found(error.to_string())
                } else {
                    internal_error(error)
                }
            })?;
            StreamJobViewResponse {
                handle_id: response.handle_id,
                job_id: response.job_id,
                view_name: response.view_name,
                logical_key: response.logical_key,
                consistency: "eventual".to_owned(),
                source: "stream-view-query".to_owned(),
                projection_lag_ms: projection_lag_ms_from_times([Some(response.updated_at)]),
                watch_cursor: response.updated_at.to_rfc3339(),
                output: response.output,
                checkpoint_sequence: response.checkpoint_sequence,
                updated_at: response.updated_at,
            }
        }
    };
    Ok(Json(response))
}

async fn get_stream_job_view_keys(
    Path((tenant_id, instance_id, run_id, job_id, view_name)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    Query(query): Query<StreamJobViewQuery>,
    State(state): State<AppState>,
) -> Result<Json<StreamJobViewKeysResponse>, (StatusCode, Json<ErrorResponse>)> {
    let job = resolve_stream_job(&state, &tenant_id, &instance_id, &run_id, &job_id)
        .await
        .map_err(|error| {
            if error.to_string().contains("not found") {
                not_found(error.to_string())
            } else {
                internal_error(error)
            }
        })?;
    let consistency =
        parse_read_consistency(Some(query.consistency.as_deref().unwrap_or("strong")))
            .map_err(invalid_request)?;
    let page =
        resolve_page(&state.query, &PaginationQuery { limit: query.limit, offset: query.offset });
    let logical_key_prefix = query
        .logical_key_prefix
        .clone()
        .or_else(|| query.prefix.clone())
        .filter(|value| !value.is_empty());
    let response = match consistency {
        ReadConsistency::Strong => {
            let response = fetch_strong_stream_job_view_keys(
                &state,
                &tenant_id,
                &job.instance_id,
                &job.run_id,
                &job_id,
                &view_name,
                logical_key_prefix.as_deref(),
                page.limit,
                page.offset,
            )
            .await
            .map_err(|error| {
                if error.to_string().contains("not found") {
                    not_found(error.to_string())
                } else {
                    internal_error(error)
                }
            })?;
            StreamJobViewKeysResponse {
                tenant_id,
                instance_id,
                run_id,
                job_id,
                view_name,
                consistency: "strong".to_owned(),
                source: "stream-owner".to_owned(),
                projection_lag_ms: None,
                watch_cursor: response
                    .keys
                    .iter()
                    .map(|key| key.updated_at)
                    .max()
                    .unwrap_or_else(Utc::now)
                    .to_rfc3339(),
                page: build_page_info(&page, response.total, response.keys.len()),
                key_count: response.total,
                keys: response.keys,
            }
        }
        ReadConsistency::Eventual => {
            let (total, keys) = load_eventual_stream_job_view_keys(
                &state,
                &tenant_id,
                &job.instance_id,
                &job.run_id,
                &job_id,
                &view_name,
                logical_key_prefix.as_deref(),
                page.limit,
                page.offset,
            )
            .await
            .map_err(|error| {
                if error.to_string().contains("not found") {
                    not_found(error.to_string())
                } else {
                    internal_error(error)
                }
            })?;
            StreamJobViewKeysResponse {
                tenant_id,
                instance_id,
                run_id,
                job_id,
                view_name,
                consistency: "eventual".to_owned(),
                source: "stream-view-query".to_owned(),
                projection_lag_ms: projection_lag_ms_from_times(
                    keys.iter().map(|key| Some(key.updated_at)),
                ),
                watch_cursor: keys
                    .iter()
                    .map(|key| key.updated_at)
                    .max()
                    .unwrap_or_else(Utc::now)
                    .to_rfc3339(),
                page: build_page_info(&page, total, keys.len()),
                key_count: total,
                keys,
            }
        }
    };
    Ok(Json(response))
}

async fn get_stream_job_view_scan(
    Path((tenant_id, instance_id, run_id, job_id, view_name)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    Query(query): Query<StreamJobViewQuery>,
    State(state): State<AppState>,
) -> Result<Json<StreamJobViewScanResponse>, (StatusCode, Json<ErrorResponse>)> {
    let job = resolve_stream_job(&state, &tenant_id, &instance_id, &run_id, &job_id)
        .await
        .map_err(|error| {
            if error.to_string().contains("not found") {
                not_found(error.to_string())
            } else {
                internal_error(error)
            }
        })?;
    let consistency =
        parse_read_consistency(Some(query.consistency.as_deref().unwrap_or("strong")))
            .map_err(invalid_request)?;
    let page =
        resolve_page(&state.query, &PaginationQuery { limit: query.limit, offset: query.offset });
    let logical_key_prefix =
        query.logical_key_prefix.clone().or_else(|| query.prefix.clone()).unwrap_or_default();
    let response = fetch_stream_job_view_scan(
        &state,
        &tenant_id,
        &job.instance_id,
        &job.run_id,
        &job_id,
        &view_name,
        match consistency {
            ReadConsistency::Strong => "strong",
            ReadConsistency::Eventual => "eventual",
        },
        &logical_key_prefix,
        page.limit,
        page.offset,
    )
    .await
    .map_err(|error| {
        if error.to_string().contains("not found") {
            not_found(error.to_string())
        } else {
            internal_error(error)
        }
    })?;
    let consistency = response
        .get("consistency")
        .and_then(Value::as_str)
        .unwrap_or(match consistency {
            ReadConsistency::Strong => "strong",
            ReadConsistency::Eventual => "eventual",
        })
        .to_owned();
    let source = stream_query_source_label(
        response.get("consistencySource").and_then(Value::as_str).unwrap_or_default(),
    );
    let total = response.get("total").and_then(Value::as_u64).unwrap_or(0) as usize;
    let items = response
        .get("items")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(stream_job_view_scan_item_from_value)
        .collect::<Result<Vec<_>>>()
        .map_err(internal_error)?;
    let watch_cursor =
        items.iter().map(|item| item.updated_at).max().unwrap_or_else(Utc::now).to_rfc3339();
    let projection_lag_ms = (consistency == "eventual")
        .then(|| projection_lag_ms_from_times(items.iter().map(|item| Some(item.updated_at))))
        .flatten();
    Ok(Json(StreamJobViewScanResponse {
        tenant_id,
        instance_id,
        run_id,
        job_id,
        view_name,
        consistency,
        source,
        projection_lag_ms,
        watch_cursor,
        prefix: response
            .get("prefix")
            .and_then(Value::as_str)
            .unwrap_or(logical_key_prefix.as_str())
            .to_owned(),
        page: build_page_info(&page, total, items.len()),
        item_count: total,
        items,
        runtime_stats: response.get("runtimeStats").cloned(),
    }))
}

async fn get_stream_job_view_entries(
    Path((tenant_id, instance_id, run_id, job_id, view_name)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    Query(query): Query<StreamJobViewQuery>,
    State(state): State<AppState>,
) -> Result<Json<StreamJobViewEntriesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let job = resolve_stream_job(&state, &tenant_id, &instance_id, &run_id, &job_id)
        .await
        .map_err(|error| {
            if error.to_string().contains("not found") {
                not_found(error.to_string())
            } else {
                internal_error(error)
            }
        })?;
    let consistency =
        parse_read_consistency(Some(query.consistency.as_deref().unwrap_or("strong")))
            .map_err(invalid_request)?;
    let page =
        resolve_page(&state.query, &PaginationQuery { limit: query.limit, offset: query.offset });
    let logical_key_prefix = query
        .logical_key_prefix
        .clone()
        .or_else(|| query.prefix.clone())
        .filter(|value| !value.is_empty());
    let response = match consistency {
        ReadConsistency::Strong => {
            let response = fetch_strong_stream_job_view_entries(
                &state,
                &tenant_id,
                &job.instance_id,
                &job.run_id,
                &job_id,
                &view_name,
                logical_key_prefix.as_deref(),
                page.limit,
                page.offset,
            )
            .await
            .map_err(|error| {
                if error.to_string().contains("not found") {
                    not_found(error.to_string())
                } else {
                    internal_error(error)
                }
            })?;
            StreamJobViewEntriesResponse {
                tenant_id,
                instance_id,
                run_id,
                job_id,
                view_name,
                consistency: "strong".to_owned(),
                source: "stream-owner".to_owned(),
                projection_lag_ms: None,
                watch_cursor: response
                    .entries
                    .iter()
                    .map(|entry| entry.updated_at)
                    .max()
                    .unwrap_or_else(Utc::now)
                    .to_rfc3339(),
                page: build_page_info(&page, response.total, response.entries.len()),
                entry_count: response.total,
                entries: response.entries,
            }
        }
        ReadConsistency::Eventual => {
            let (total, entries) = load_eventual_stream_job_view_entries(
                &state,
                &tenant_id,
                &job.instance_id,
                &job.run_id,
                &job_id,
                &view_name,
                logical_key_prefix.as_deref(),
                page.limit,
                page.offset,
            )
            .await
            .map_err(|error| {
                if error.to_string().contains("not found") {
                    not_found(error.to_string())
                } else {
                    internal_error(error)
                }
            })?;
            StreamJobViewEntriesResponse {
                tenant_id,
                instance_id,
                run_id,
                job_id,
                view_name,
                consistency: "eventual".to_owned(),
                source: "stream-view-query".to_owned(),
                projection_lag_ms: projection_lag_ms_from_times(
                    entries.iter().map(|entry| Some(entry.updated_at)),
                ),
                watch_cursor: entries
                    .iter()
                    .map(|entry| entry.updated_at)
                    .max()
                    .unwrap_or_else(Utc::now)
                    .to_rfc3339(),
                page: build_page_info(&page, total, entries.len()),
                entry_count: total,
                entries,
            }
        }
    };
    Ok(Json(response))
}

async fn get_stream_job_callback_handles(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    Query(query): Query<StreamJobBridgeListQuery>,
    State(state): State<AppState>,
) -> Result<Json<StreamJobBridgeHandlesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response =
        load_stream_job_callback_handles(&state, &tenant_id, &instance_id, &run_id, query)
            .await
            .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_stream_job_callback_repairs(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    Query(query): Query<StreamJobBridgeListQuery>,
    State(state): State<AppState>,
) -> Result<Json<StreamJobBridgeRepairsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response =
        load_stream_job_callback_repairs(&state, &tenant_id, &instance_id, &run_id, query)
            .await
            .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_stream_job_callback_handle(
    Path((tenant_id, instance_id, run_id, job_id)): Path<(String, String, String, String)>,
    State(state): State<AppState>,
) -> Result<Json<StreamJobBridgeHandleResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response =
        load_stream_job_callback_handle(&state, &tenant_id, &instance_id, &run_id, &job_id)
            .await
            .map_err(|error| {
            if error.to_string().contains("not found") {
                not_found(error.to_string())
            } else {
                internal_error(error)
            }
        })?;
    Ok(Json(response))
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
    State(state): State<AppState>,
) -> Result<Json<StrongStreamJobViewResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = fetch_strong_stream_job_view(
        &state,
        &tenant_id,
        &instance_id,
        &run_id,
        &job_id,
        &view_name,
        &logical_key,
    )
    .await
    .map_err(|error| {
        if error.to_string().contains("not found") {
            not_found(error.to_string())
        } else {
            internal_error(error)
        }
    })?;
    Ok(Json(response))
}

async fn list_tenants(
    State(state): State<AppState>,
) -> Result<Json<Value>, (StatusCode, Json<ErrorResponse>)> {
    let tenants = state.store.list_tenants().await.map_err(internal_error)?;
    Ok(Json(json!({ "tenants": tenants })))
}

async fn get_overview_summary(
    Path(tenant_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<OverviewSummaryResponse>, (StatusCode, Json<ErrorResponse>)> {
    let total_workflow_definitions = state
        .store
        .count_workflow_definition_summaries(&tenant_id, None)
        .await
        .map_err(internal_error)?;
    let total_workflow_artifacts = state
        .store
        .count_workflow_artifact_summaries(&tenant_id, None)
        .await
        .map_err(internal_error)?;
    let total_workflows = state
        .store
        .count_instances_with_filters(&tenant_id, &WorkflowInstanceFilters::default())
        .await
        .map_err(internal_error)?;

    let mut counts_by_status = BTreeMap::new();
    for status in ["triggered", "running", "completed", "failed", "cancelled", "terminated"] {
        counts_by_status.insert(
            status.to_owned(),
            state
                .store
                .count_instances_with_filters(
                    &tenant_id,
                    &WorkflowInstanceFilters {
                        status: Some(status.to_owned()),
                        ..WorkflowInstanceFilters::default()
                    },
                )
                .await
                .map_err(internal_error)?,
        );
    }

    let recent_failure_states = state
        .store
        .list_instances_page_with_filters(
            &tenant_id,
            &WorkflowInstanceFilters {
                status: Some("failed".to_owned()),
                ..WorkflowInstanceFilters::default()
            },
            6,
            0,
        )
        .await
        .map_err(internal_error)?;
    let mut recent_failures = Vec::with_capacity(recent_failure_states.len());
    for workflow in recent_failure_states {
        recent_failures.push(
            workflow_list_item_from_state(&state.store, workflow).await.map_err(internal_error)?,
        );
    }

    let queue_refs =
        state.store.list_task_queue_refs_for_tenant(&tenant_id).await.map_err(internal_error)?;
    let mut task_queues = Vec::with_capacity(queue_refs.len());
    let mut total_backlog = 0_u64;
    let mut total_pollers = 0_usize;
    let mut total_registered_builds = 0_usize;
    for queue_ref in queue_refs {
        let inspection = state
            .store
            .inspect_task_queue(
                &tenant_id,
                queue_ref.queue_kind.clone(),
                &queue_ref.task_queue,
                Utc::now(),
            )
            .await
            .map_err(internal_error)?;
        total_backlog += inspection.backlog;
        total_pollers += inspection.pollers.len();
        total_registered_builds += inspection.registered_builds.len();
        task_queues.push(OverviewTaskQueueSummary {
            queue_kind: queue_kind_label(&inspection.queue_kind).to_owned(),
            task_queue: inspection.task_queue,
            backlog: inspection.backlog,
            poller_count: inspection.pollers.len(),
            registered_build_count: inspection.registered_builds.len(),
            default_set_id: inspection.default_set_id,
            throughput_backend: inspection.throughput_policy.map(|policy| policy.backend),
            oldest_backlog_at: inspection.oldest_backlog_at,
        });
    }
    task_queues.sort_by(|left, right| {
        right
            .backlog
            .cmp(&left.backlog)
            .then_with(|| left.queue_kind.cmp(&right.queue_kind))
            .then_with(|| left.task_queue.cmp(&right.task_queue))
    });

    Ok(Json(OverviewSummaryResponse {
        tenant_id,
        consistency: "eventual",
        authoritative_source: "projection",
        total_workflows,
        total_workflow_definitions,
        total_workflow_artifacts,
        counts_by_status,
        total_task_queues: task_queues.len(),
        total_backlog,
        total_pollers,
        total_registered_builds,
        replay_divergence_count: 0,
        recent_failures,
        hottest_task_queues: task_queues.into_iter().take(6).collect(),
    }))
}

async fn list_workflows(
    Path(tenant_id): Path<String>,
    Query(query): Query<WorkflowListQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowListResponse>, (StatusCode, Json<ErrorResponse>)> {
    let page =
        resolve_page(&state.query, &PaginationQuery { limit: query.limit, offset: query.offset });
    let filters = WorkflowInstanceFilters {
        status: query.status.and_then(non_empty),
        definition_id: query.definition_id.and_then(non_empty),
        task_queue: query.task_queue.and_then(non_empty),
        updated_after: query.updated_after,
        query: query.q.and_then(non_empty),
        memo_key: query.memo_key.and_then(non_empty),
        memo_value: query.memo_value.and_then(non_empty),
        search_attribute_key: query.search_attribute_key.and_then(non_empty),
        search_attribute_value: query.search_attribute_value.and_then(non_empty),
    };
    let total = state
        .store
        .count_instances_with_filters(&tenant_id, &filters)
        .await
        .map_err(internal_error)? as usize;
    if let Some(routing_status) = query.routing_status.and_then(non_empty) {
        let (items, filtered_total) = filtered_workflow_list_items(
            &state.store,
            &tenant_id,
            &filters,
            &page,
            &routing_status,
        )
        .await
        .map_err(internal_error)?;
        return Ok(Json(WorkflowListResponse {
            tenant_id,
            consistency: "eventual",
            authoritative_source: "projection",
            page: build_page_info(&page, filtered_total, items.len()),
            workflow_count: filtered_total,
            items,
        }));
    }
    let workflows = state
        .store
        .list_instances_page_with_filters(
            &tenant_id,
            &filters,
            i64::try_from(page.limit).map_err(internal_error_from_display)?,
            i64::try_from(page.offset).map_err(internal_error_from_display)?,
        )
        .await
        .map_err(internal_error)?;
    let mut items = Vec::with_capacity(workflows.len());
    for workflow in workflows {
        items.push(
            workflow_list_item_from_state(&state.store, workflow).await.map_err(internal_error)?,
        );
    }

    Ok(Json(WorkflowListResponse {
        tenant_id,
        consistency: "eventual",
        authoritative_source: "projection",
        page: build_page_info(&page, total, items.len()),
        workflow_count: total,
        items,
    }))
}

async fn list_runs(
    Path(tenant_id): Path<String>,
    Query(query): Query<RunListQuery>,
    State(state): State<AppState>,
) -> Result<Json<RunListResponse>, (StatusCode, Json<ErrorResponse>)> {
    let page =
        resolve_page(&state.query, &PaginationQuery { limit: query.limit, offset: query.offset });
    let filters = WorkflowRunFilters {
        status: query.status.and_then(non_empty),
        definition_id: query.definition_id.and_then(non_empty),
        instance_id: query.instance_id.and_then(non_empty),
        run_id: query.run_id.and_then(non_empty),
        task_queue: query.task_queue.and_then(non_empty),
        query: query.q.and_then(non_empty),
        memo_key: query.memo_key.and_then(non_empty),
        memo_value: query.memo_value.and_then(non_empty),
        search_attribute_key: query.search_attribute_key.and_then(non_empty),
        search_attribute_value: query.search_attribute_value.and_then(non_empty),
    };
    let total =
        state.store.count_runs_with_filters(&tenant_id, &filters).await.map_err(internal_error)?
            as usize;
    if let Some(routing_status) = query.routing_status.and_then(non_empty) {
        let (items, filtered_total) =
            filtered_run_list_items(&state.store, &tenant_id, &filters, &page, &routing_status)
                .await
                .map_err(internal_error)?;
        return Ok(Json(RunListResponse {
            tenant_id,
            consistency: "eventual",
            authoritative_source: "projection",
            page: build_page_info(&page, filtered_total, items.len()),
            run_count: filtered_total,
            items,
        }));
    }
    let runs = state
        .store
        .list_runs_page_with_filters(
            &tenant_id,
            &filters,
            i64::try_from(page.limit).map_err(internal_error_from_display)?,
            i64::try_from(page.offset).map_err(internal_error_from_display)?,
        )
        .await
        .map_err(internal_error)?;
    let mut items = Vec::with_capacity(runs.len());
    for run in runs {
        items.push(run_list_item_from_record(&state.store, run).await.map_err(internal_error)?);
    }

    Ok(Json(RunListResponse {
        tenant_id,
        consistency: "eventual",
        authoritative_source: "projection",
        page: build_page_info(&page, total, items.len()),
        run_count: total,
        items,
    }))
}

async fn list_workflow_definition_summaries(
    Path(tenant_id): Path<String>,
    Query(query): Query<SearchableListQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowDefinitionSummariesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let page =
        resolve_page(&state.query, &PaginationQuery { limit: query.limit, offset: query.offset });
    let items = state
        .store
        .list_workflow_definition_summaries(
            &tenant_id,
            query.q.as_deref().and_then(non_empty_ref),
            i64::try_from(page.limit).map_err(internal_error_from_display)?,
            i64::try_from(page.offset).map_err(internal_error_from_display)?,
        )
        .await
        .map_err(internal_error)?;
    Ok(Json(WorkflowDefinitionSummariesResponse { tenant_id, items }))
}

async fn list_workflow_artifact_summaries(
    Path(tenant_id): Path<String>,
    Query(query): Query<SearchableListQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowArtifactSummariesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let page =
        resolve_page(&state.query, &PaginationQuery { limit: query.limit, offset: query.offset });
    let items = state
        .store
        .list_workflow_artifact_summaries(
            &tenant_id,
            query.q.as_deref().and_then(non_empty_ref),
            i64::try_from(page.limit).map_err(internal_error_from_display)?,
            i64::try_from(page.offset).map_err(internal_error_from_display)?,
        )
        .await
        .map_err(internal_error)?;
    Ok(Json(WorkflowArtifactSummariesResponse { tenant_id, items }))
}

async fn list_stream_artifact_summaries(
    Path(tenant_id): Path<String>,
    Query(query): Query<SearchableListQuery>,
    State(state): State<AppState>,
) -> Result<Json<StreamArtifactSummariesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let page =
        resolve_page(&state.query, &PaginationQuery { limit: query.limit, offset: query.offset });
    let items = state
        .store
        .list_stream_artifact_summaries(
            &tenant_id,
            query.q.as_deref().and_then(non_empty_ref),
            i64::try_from(page.limit).map_err(internal_error_from_display)?,
            i64::try_from(page.offset).map_err(internal_error_from_display)?,
        )
        .await
        .map_err(internal_error)?;
    Ok(Json(StreamArtifactSummariesResponse { tenant_id, items }))
}

async fn get_workflow_instance(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowInstanceState>, (StatusCode, Json<ErrorResponse>)> {
    match state.store.get_instance(&tenant_id, &instance_id).await.map_err(internal_error)? {
        Some(instance) => Ok(Json(instance)),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                message: format!(
                    "workflow instance {instance_id} not found for tenant {tenant_id}"
                ),
            }),
        )),
    }
}

async fn get_current_workflow_history(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowHistoryResponse>, (StatusCode, Json<ErrorResponse>)> {
    let instance = state
        .store
        .get_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    message: format!(
                        "workflow instance {instance_id} not found for tenant {tenant_id}"
                    ),
                }),
            )
        })?;
    let response =
        load_workflow_history(&state, &tenant_id, &instance_id, &instance.run_id, pagination)
            .await
            .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_runs(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowRunsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let current_instance =
        state.store.get_instance(&tenant_id, &instance_id).await.map_err(internal_error)?;
    let page = resolve_page(&state.query, &pagination);
    let total = state
        .store
        .count_runs_for_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)? as usize;
    let runs = state
        .store
        .list_runs_for_instance_page(
            &tenant_id,
            &instance_id,
            i64::try_from(page.limit).map_err(internal_error_from_display)?,
            i64::try_from(page.offset).map_err(internal_error_from_display)?,
        )
        .await
        .map_err(internal_error)?;
    Ok(Json(WorkflowRunsResponse {
        tenant_id,
        instance_id,
        current_run_id: current_instance.map(|instance| instance.run_id),
        page: build_page_info(&page, total, runs.len()),
        run_count: total,
        runs,
    }))
}

async fn execute_strong_query(
    Path((tenant_id, instance_id, query_name)): Path<(String, String, String)>,
    State(state): State<AppState>,
    Json(request): Json<StrongQueryRequest>,
) -> Result<Json<StrongQueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    let instance = state
        .store
        .get_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| not_found(format!("workflow instance {instance_id} not found")))?;
    let version = instance.definition_version.ok_or_else(|| {
        internal_error(anyhow::anyhow!(
            "workflow instance {instance_id} is missing definition_version"
        ))
    })?;
    let artifact = state
        .store
        .get_artifact_version(&tenant_id, &instance.definition_id, version)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            internal_error(anyhow::anyhow!(
                "workflow artifact {} version {} not found",
                instance.definition_id,
                version
            ))
        })?;

    if !instance.status.is_terminal() && instance.artifact_execution.is_some() {
        if let Some(owner_response) =
            try_owner_strong_query(&state, &tenant_id, &instance_id, &query_name, &request.args)
                .await
        {
            return Ok(Json(StrongQueryResponse {
                tenant_id,
                instance_id,
                run_id: instance.run_id,
                query_name,
                consistency: owner_response.consistency,
                source: owner_response.source,
                result: owner_response.result,
            }));
        }
    }

    let (query_state, source) =
        if !instance.status.is_terminal() && instance.artifact_execution.is_some() {
            (instance.clone(), "hot_owner_fallback")
        } else {
            let history = read_workflow_history(
                &state.broker,
                "query-service-strong-query",
                &WorkflowHistoryFilter::new(&tenant_id, &instance_id, &instance.run_id),
                Duration::from_millis(HISTORY_IDLE_TIMEOUT_MS),
                Duration::from_millis(HISTORY_MAX_SCAN_MS),
            )
            .await
            .map_err(internal_error)?;
            (replay_compiled_history(&history, &artifact).map_err(internal_error)?, "replay")
        };

    let result = artifact
        .evaluate_query(&query_name, &request.args, execution_state_for_event(&query_state, None))
        .map_err(|error| {
            (StatusCode::BAD_REQUEST, Json(ErrorResponse { message: error.to_string() }))
        })?;

    Ok(Json(StrongQueryResponse {
        tenant_id,
        instance_id,
        run_id: query_state.run_id,
        query_name,
        consistency: "strong".to_owned(),
        source: source.to_owned(),
        result,
    }))
}

async fn try_owner_strong_query(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    query_name: &str,
    args: &Value,
) -> Option<InternalQueryResponse> {
    for base_url in owner_query_endpoints(&state.query) {
        if let Some(payload) = try_owner_strong_query_endpoint(
            &state.client,
            &base_url,
            tenant_id,
            instance_id,
            query_name,
            args,
        )
        .await
        {
            return Some(payload);
        }
    }
    None
}

async fn try_owner_strong_query_endpoint(
    client: &Client,
    base_url: &str,
    tenant_id: &str,
    instance_id: &str,
    query_name: &str,
    args: &Value,
) -> Option<InternalQueryResponse> {
    let url = format!(
        "{}/internal/workflows/{}/{}/queries/{}",
        base_url.trim_end_matches('/'),
        tenant_id,
        instance_id,
        query_name,
    );
    let response =
        match client.post(&url).json(&InternalQueryRequest { args: args.clone() }).send().await {
            Ok(response) => response,
            Err(_) => return None,
        };
    if !response.status().is_success() {
        return None;
    }
    response.json::<InternalQueryResponse>().await.ok()
}

fn owner_query_endpoints(config: &QueryRuntimeConfig) -> Vec<String> {
    let mut endpoints = Vec::new();
    if let Some(url) = config.strong_query_unified_url.as_ref() {
        endpoints.push(url.clone());
    }
    endpoints
}

async fn get_latest_workflow_snapshot(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowStateSnapshot>, (StatusCode, Json<ErrorResponse>)> {
    match state.store.get_latest_snapshot(&tenant_id, &instance_id).await.map_err(internal_error)? {
        Some(snapshot) => Ok(Json(snapshot)),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                message: format!(
                    "workflow snapshot for instance {instance_id} not found for tenant {tenant_id}"
                ),
            }),
        )),
    }
}

async fn get_current_workflow_activities(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowActivitiesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let instance = state
        .store
        .get_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    message: format!(
                        "workflow instance {instance_id} not found for tenant {tenant_id}"
                    ),
                }),
            )
        })?;
    let response =
        load_workflow_activities(&state, &tenant_id, &instance_id, &instance.run_id, pagination)
            .await
            .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_current_workflow_signals(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowSignalsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let instance = state
        .store
        .get_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    message: format!(
                        "workflow instance {instance_id} not found for tenant {tenant_id}"
                    ),
                }),
            )
        })?;
    let response =
        load_workflow_signals(&state, &tenant_id, &instance_id, &instance.run_id, pagination)
            .await
            .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_current_workflow_updates(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowUpdatesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let instance = state
        .store
        .get_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    message: format!(
                        "workflow instance {instance_id} not found for tenant {tenant_id}"
                    ),
                }),
            )
        })?;
    let response =
        load_workflow_updates(&state, &tenant_id, &instance_id, &instance.run_id, pagination)
            .await
            .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_activities_for_run(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowActivitiesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_activities(&state, &tenant_id, &instance_id, &run_id, pagination)
        .await
        .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_bulk_batches_for_run(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    Query(query): Query<BulkReadQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowBulkBatchesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_bulk_batches(
        &state,
        &tenant_id,
        &instance_id,
        &run_id,
        PaginationQuery { limit: query.limit, offset: query.offset },
        parse_read_consistency(query.consistency.as_deref()).map_err(invalid_request)?,
    )
    .await
    .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_bulk_batch_for_run(
    Path((tenant_id, instance_id, run_id, batch_id)): Path<(String, String, String, String)>,
    Query(query): Query<ConsistencyQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowBulkBatchResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_bulk_batch(
        &state,
        &tenant_id,
        &instance_id,
        &run_id,
        &batch_id,
        parse_read_consistency(query.consistency.as_deref()).map_err(invalid_request)?,
    )
    .await
    .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_bulk_chunks_for_run(
    Path((tenant_id, instance_id, run_id, batch_id)): Path<(String, String, String, String)>,
    Query(query): Query<BulkReadQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowBulkChunksResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_bulk_chunks(
        &state,
        &tenant_id,
        &instance_id,
        &run_id,
        &batch_id,
        PaginationQuery { limit: query.limit, offset: query.offset },
        parse_read_consistency(query.consistency.as_deref()).map_err(invalid_request)?,
    )
    .await
    .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_bulk_results_for_run(
    Path((tenant_id, instance_id, run_id, batch_id)): Path<(String, String, String, String)>,
    Query(query): Query<BulkReadQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowBulkResultsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_bulk_results(
        &state,
        &tenant_id,
        &instance_id,
        &run_id,
        &batch_id,
        PaginationQuery { limit: query.limit, offset: query.offset },
        parse_read_consistency(query.consistency.as_deref()).map_err(invalid_request)?,
    )
    .await
    .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_signals_for_run(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowSignalsResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_signals(&state, &tenant_id, &instance_id, &run_id, pagination)
        .await
        .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_updates_for_run(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowUpdatesResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_updates(&state, &tenant_id, &instance_id, &run_id, pagination)
        .await
        .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_current_workflow_update(
    Path((tenant_id, instance_id, update_id)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowUpdateResponse>, (StatusCode, Json<ErrorResponse>)> {
    let instance = state
        .store
        .get_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    message: format!(
                        "workflow instance {instance_id} not found for tenant {tenant_id}"
                    ),
                }),
            )
        })?;
    let response =
        load_workflow_update(&state, &tenant_id, &instance_id, &instance.run_id, &update_id)
            .await?;
    Ok(Json(response))
}

async fn get_workflow_update_for_run(
    Path((tenant_id, instance_id, run_id, update_id)): Path<(String, String, String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowUpdateResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response =
        load_workflow_update(&state, &tenant_id, &instance_id, &run_id, &update_id).await?;
    Ok(Json(response))
}

async fn get_workflow_history_for_run(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    Query(pagination): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowHistoryResponse>, (StatusCode, Json<ErrorResponse>)> {
    let response = load_workflow_history(&state, &tenant_id, &instance_id, &run_id, pagination)
        .await
        .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_current_workflow_replay(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowReplayResponse>, (StatusCode, Json<ErrorResponse>)> {
    let current_instance = state
        .store
        .get_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    message: format!(
                        "workflow instance {instance_id} not found for tenant {tenant_id}"
                    ),
                }),
            )
        })?;
    let response = replay_workflow_run(&state, &current_instance, &current_instance.run_id)
        .await
        .map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_replay_for_run(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowReplayResponse>, (StatusCode, Json<ErrorResponse>)> {
    let current_instance = state
        .store
        .get_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    message: format!(
                        "workflow instance {instance_id} not found for tenant {tenant_id}"
                    ),
                }),
            )
        })?;
    let response =
        replay_workflow_run(&state, &current_instance, &run_id).await.map_err(internal_error)?;
    Ok(Json(response))
}

async fn get_workflow_definition_graph(
    Path((tenant_id, definition_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowGraphResponse>, (StatusCode, Json<ErrorResponse>)> {
    let artifact = state
        .store
        .get_latest_artifact(&tenant_id, &definition_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            not_found(format!("workflow artifact {definition_id} not found for tenant {tenant_id}"))
        })?;
    Ok(Json(build_workflow_graph_response(tenant_id, &artifact, build_artifact_overlay())))
}

async fn get_workflow_run_graph(
    Path((tenant_id, instance_id, run_id)): Path<(String, String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowGraphResponse>, (StatusCode, Json<ErrorResponse>)> {
    let run = state
        .store
        .get_run_record(&tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| not_found(format!("workflow run {run_id} not found")))?;
    let artifact = load_artifact_for_run(&state, &tenant_id, &run)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
        not_found(format!(
            "workflow artifact {} version {:?} not found for tenant {}",
            run.definition_id, run.definition_version, tenant_id
        ))
    })?;
    let current_instance =
        state.store.get_instance(&tenant_id, &instance_id).await.map_err(internal_error)?;
    let history =
        read_history(&state, &tenant_id, &instance_id, &run_id).await.map_err(internal_error)?;
    let activities = state
        .store
        .list_activities_for_run(&tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?;
    let signals = state
        .store
        .list_signals_for_run(&tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?;
    let updates = state
        .store
        .list_updates_for_run(&tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?;
    let children = state
        .store
        .list_children_for_run(&tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?;
    let bulk_batches = state
        .store
        .list_bulk_batches_for_run_page(&tenant_id, &instance_id, &run_id, i64::MAX, 0)
        .await
        .map_err(internal_error)?;
    let overlay = build_run_overlay(
        &artifact,
        &run_id,
        current_instance
            .as_ref()
            .filter(|instance| instance.run_id == run_id)
            .and_then(|instance| instance.current_state.clone()),
        &history,
        &activities,
        &signals,
        &updates,
        &children,
        &bulk_batches,
    );
    Ok(Json(build_workflow_graph_response(tenant_id, &artifact, overlay)))
}

async fn get_workflow_execution_graph(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowExecutionGraphResponse>, (StatusCode, Json<ErrorResponse>)> {
    let instance = state
        .store
        .get_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| not_found(format!("workflow instance {instance_id} not found")))?;
    let run_id = instance.run_id.clone();
    let run = state
        .store
        .get_run_record(&tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| not_found(format!("workflow run {run_id} not found")))?;
    let activities = state
        .store
        .list_activities_for_run(&tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?;
    let signals = state
        .store
        .list_signals_for_run(&tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?;
    let children = state
        .store
        .list_children_for_run(&tenant_id, &instance_id, &run_id)
        .await
        .map_err(internal_error)?;
    let bulk_batches = state
        .store
        .list_bulk_batches_for_run_page(&tenant_id, &instance_id, &run_id, i64::MAX, 0)
        .await
        .map_err(internal_error)?;
    let runs = state
        .store
        .list_runs_for_instance(&tenant_id, &instance_id)
        .await
        .map_err(internal_error)?;

    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    let workflow_node_id = format!("workflow:{instance_id}:{run_id}");
    nodes.push(WorkflowExecutionGraphNode {
        id: workflow_node_id.clone(),
        kind: "workflow".to_owned(),
        label: instance.definition_id.clone(),
        status: instance.status.as_str().to_owned(),
        subtitle: instance.current_state.clone(),
    });

    let queue_node_id = format!("queue:workflow:{}", run.workflow_task_queue);
    nodes.push(WorkflowExecutionGraphNode {
        id: queue_node_id.clone(),
        kind: "workflow_queue".to_owned(),
        label: run.workflow_task_queue.clone(),
        status: "active".to_owned(),
        subtitle: Some("workflow task queue".to_owned()),
    });
    edges.push(WorkflowExecutionGraphEdge {
        id: format!("{queue_node_id}->{workflow_node_id}"),
        source: queue_node_id.clone(),
        target: workflow_node_id.clone(),
        label: "dispatches".to_owned(),
    });

    if let Some(build_id) = instance.sticky_workflow_build_id.clone() {
        let build_node_id = format!("build:{build_id}");
        nodes.push(WorkflowExecutionGraphNode {
            id: build_node_id.clone(),
            kind: "workflow_build".to_owned(),
            label: build_id,
            status: "sticky".to_owned(),
            subtitle: Some("sticky build".to_owned()),
        });
        edges.push(WorkflowExecutionGraphEdge {
            id: format!("{build_node_id}->{workflow_node_id}"),
            source: build_node_id.clone(),
            target: workflow_node_id.clone(),
            label: "routes".to_owned(),
        });
        if let Some(poller_id) = instance.sticky_workflow_poller_id.clone() {
            let poller_node_id = format!("poller:{poller_id}");
            nodes.push(WorkflowExecutionGraphNode {
                id: poller_node_id.clone(),
                kind: "workflow_poller".to_owned(),
                label: poller_id,
                status: "active".to_owned(),
                subtitle: Some("sticky poller".to_owned()),
            });
            edges.push(WorkflowExecutionGraphEdge {
                id: format!("{poller_node_id}->{build_node_id}"),
                source: poller_node_id,
                target: build_node_id,
                label: "advertises".to_owned(),
            });
        }
    }

    for activity in activities {
        let activity_node_id = format!("activity:{}:{}", activity.activity_id, activity.attempt);
        nodes.push(activity_graph_node(&activity_node_id, &activity));
        edges.push(WorkflowExecutionGraphEdge {
            id: format!("{workflow_node_id}->{activity_node_id}"),
            source: workflow_node_id.clone(),
            target: activity_node_id.clone(),
            label: activity.activity_type.clone(),
        });
        let queue_id = format!("queue:activity:{}", activity.task_queue);
        nodes.push(WorkflowExecutionGraphNode {
            id: queue_id.clone(),
            kind: "activity_queue".to_owned(),
            label: activity.task_queue.clone(),
            status: "active".to_owned(),
            subtitle: Some("activity task queue".to_owned()),
        });
        edges.push(WorkflowExecutionGraphEdge {
            id: format!("{activity_node_id}->{queue_id}"),
            source: activity_node_id,
            target: queue_id,
            label: "queued_on".to_owned(),
        });
    }

    for signal in signals {
        let signal_node_id = format!("signal:{}", signal.signal_id);
        nodes.push(WorkflowExecutionGraphNode {
            id: signal_node_id.clone(),
            kind: "signal".to_owned(),
            label: signal.signal_type.clone(),
            status: signal_status_label(&signal),
            subtitle: Some(signal.signal_id.clone()),
        });
        edges.push(WorkflowExecutionGraphEdge {
            id: format!("{signal_node_id}->{workflow_node_id}"),
            source: signal_node_id,
            target: workflow_node_id.clone(),
            label: "delivers".to_owned(),
        });
    }

    for child in children {
        let child_node_id = format!("child:{}", child.child_id);
        nodes.push(child_graph_node(&child_node_id, &child));
        edges.push(WorkflowExecutionGraphEdge {
            id: format!("{workflow_node_id}->{child_node_id}"),
            source: workflow_node_id.clone(),
            target: child_node_id,
            label: "starts".to_owned(),
        });
    }

    for batch in bulk_batches {
        let batch_node_id = format!("bulk:{}", batch.batch_id);
        nodes.push(WorkflowExecutionGraphNode {
            id: batch_node_id.clone(),
            kind: "bulk_batch".to_owned(),
            label: batch.activity_type.clone(),
            status: batch.status.as_str().to_owned(),
            subtitle: Some(format!("{} items / {} chunks", batch.total_items, batch.chunk_count)),
        });
        edges.push(WorkflowExecutionGraphEdge {
            id: format!("{workflow_node_id}->{batch_node_id}"),
            source: workflow_node_id.clone(),
            target: batch_node_id,
            label: "bulk fan-out".to_owned(),
        });
    }

    for run_record in runs {
        let run_node_id = format!("run:{}", run_record.run_id);
        nodes.push(WorkflowExecutionGraphNode {
            id: run_node_id.clone(),
            kind: "run".to_owned(),
            label: run_record.run_id.clone(),
            status: if run_record.closed_at.is_some() { "closed" } else { "open" }.to_owned(),
            subtitle: run_record.continue_reason.clone(),
        });
        if run_record.run_id == run_id {
            edges.push(WorkflowExecutionGraphEdge {
                id: format!("{run_node_id}->{workflow_node_id}"),
                source: run_node_id.clone(),
                target: workflow_node_id.clone(),
                label: "active".to_owned(),
            });
        }
        if let Some(previous_run_id) = run_record.previous_run_id.clone() {
            edges.push(WorkflowExecutionGraphEdge {
                id: format!("run:{previous_run_id}->{run_node_id}"),
                source: format!("run:{previous_run_id}"),
                target: run_node_id.clone(),
                label: "continue-as-new".to_owned(),
            });
        }
    }

    dedupe_graph_nodes(&mut nodes);
    dedupe_graph_edges(&mut edges);

    Ok(Json(WorkflowExecutionGraphResponse {
        tenant_id,
        instance_id,
        run_id,
        consistency: "eventual",
        authoritative_source: "projection",
        nodes,
        edges,
    }))
}

async fn load_artifact_for_run(
    state: &AppState,
    tenant_id: &str,
    run: &WorkflowRunRecord,
) -> Result<Option<CompiledWorkflowArtifact>> {
    if let Some(version) = run.definition_version {
        if let Some(artifact) =
            state.store.get_artifact_version(tenant_id, &run.definition_id, version).await?
        {
            return Ok(Some(artifact));
        }
    }
    state.store.get_latest_artifact(tenant_id, &run.definition_id).await
}

fn build_workflow_graph_response(
    tenant_id: String,
    artifact: &CompiledWorkflowArtifact,
    overlay: WorkflowGraphOverlay,
) -> WorkflowGraphResponse {
    let projection = normalize_artifact_graph(artifact);
    WorkflowGraphResponse {
        tenant_id,
        definition_id: artifact.definition_id.clone(),
        definition_version: artifact.definition_version,
        artifact_hash: artifact.artifact_hash.clone(),
        entrypoint_module: artifact.entrypoint.module.clone(),
        entrypoint_export: artifact.entrypoint.export.clone(),
        consistency: "eventual",
        authoritative_source: "artifact+projection",
        source_files: artifact.source_files.clone(),
        nodes: projection.nodes,
        edges: projection.edges,
        modules: projection.modules,
        module_edges: projection.module_edges,
        overlay,
    }
}

struct WorkflowGraphProjection {
    nodes: Vec<WorkflowGraphNode>,
    edges: Vec<WorkflowGraphEdge>,
    modules: Vec<WorkflowGraphModule>,
    module_edges: Vec<WorkflowGraphEdge>,
}

fn normalize_artifact_graph(artifact: &CompiledWorkflowArtifact) -> WorkflowGraphProjection {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    let mut modules = Vec::new();

    let entry_node_id = workflow_entry_node_id();
    let entry_module_id = workflow_entry_module_id();
    nodes.push(WorkflowGraphNode {
        id: entry_node_id.clone(),
        graph: "workflow".to_owned(),
        module_id: entry_module_id.clone(),
        state_id: None,
        kind: "entry".to_owned(),
        label: "Entry".to_owned(),
        subtitle: Some(format!("{}::{}", artifact.entrypoint.module, artifact.entrypoint.export)),
        source_anchor: None,
        next_ids: vec![workflow_state_node_id(&artifact.workflow.initial_state)],
        raw: json!({
            "type": "entry",
            "module": artifact.entrypoint.module,
            "export": artifact.entrypoint.export,
            "initial_state": artifact.workflow.initial_state,
        }),
    });
    modules.push(WorkflowGraphModule {
        id: entry_module_id.clone(),
        graph: "workflow".to_owned(),
        kind: "entry".to_owned(),
        label: "Entry".to_owned(),
        subtitle: Some(format!("{}::{}", artifact.entrypoint.module, artifact.entrypoint.export)),
        node_ids: vec![entry_node_id.clone()],
        state_ids: Vec::new(),
        focus_node_id: entry_node_id.clone(),
        collapsed_by_default: false,
        source_anchor: None,
        raw: json!({
            "entrypoint": artifact.entrypoint,
            "initial_state": artifact.workflow.initial_state,
        }),
    });
    edges.push(WorkflowGraphEdge {
        id: format!(
            "{entry_node_id}->{}",
            workflow_state_node_id(&artifact.workflow.initial_state)
        ),
        source: entry_node_id,
        target: workflow_state_node_id(&artifact.workflow.initial_state),
        label: "starts".to_owned(),
        kind: "entry".to_owned(),
    });

    for (state_id, state) in &artifact.workflow.states {
        let node_id = workflow_state_node_id(state_id);
        let module_id = workflow_module_id(state_id);
        let next_ids = state_transition_targets(state)
            .into_iter()
            .map(|(_, target)| workflow_state_node_id(&target))
            .collect::<Vec<_>>();
        let raw = to_value(state).unwrap_or(Value::Null);
        let subtitle = compiled_state_subtitle(state);
        let source_anchor = resolve_source_anchor(
            &artifact.source_map,
            &[state_id.clone(), format!("workflow:{state_id}"), format!("state:{state_id}")],
        );
        nodes.push(WorkflowGraphNode {
            id: node_id.clone(),
            graph: "workflow".to_owned(),
            module_id: module_id.clone(),
            state_id: Some(state_id.clone()),
            kind: compiled_state_kind(state).to_owned(),
            label: state_id.clone(),
            subtitle: subtitle.clone(),
            source_anchor: source_anchor.clone(),
            next_ids,
            raw: raw.clone(),
        });
        modules.push(WorkflowGraphModule {
            id: module_id.clone(),
            graph: "workflow".to_owned(),
            kind: semantic_module_kind("workflow", state).to_owned(),
            label: state_id.clone(),
            subtitle,
            node_ids: vec![node_id.clone()],
            state_ids: vec![state_id.clone()],
            focus_node_id: node_id.clone(),
            collapsed_by_default: module_collapsed_by_default(state),
            source_anchor,
            raw,
        });
        for (label, target) in state_transition_targets(state) {
            edges.push(WorkflowGraphEdge {
                id: format!("{node_id}->{}", workflow_state_node_id(&target)),
                source: node_id.clone(),
                target: workflow_state_node_id(&target),
                label,
                kind: "transition".to_owned(),
            });
        }
    }

    if !artifact.signals.is_empty() || !artifact.updates.is_empty() || !artifact.queries.is_empty()
    {
        let handlers_node_id = handlers_hub_node_id();
        let handlers_module_id = handlers_hub_module_id();
        nodes.push(WorkflowGraphNode {
            id: handlers_node_id.clone(),
            graph: "system".to_owned(),
            module_id: handlers_module_id.clone(),
            state_id: None,
            kind: "handlers".to_owned(),
            label: "Handlers".to_owned(),
            subtitle: Some("signals, updates, queries".to_owned()),
            source_anchor: None,
            next_ids: Vec::new(),
            raw: json!({
                "signals": artifact.signals.keys().collect::<Vec<_>>(),
                "updates": artifact.updates.keys().collect::<Vec<_>>(),
                "queries": artifact.queries.keys().collect::<Vec<_>>(),
            }),
        });
        modules.push(WorkflowGraphModule {
            id: handlers_module_id.clone(),
            graph: "system".to_owned(),
            kind: "handlers".to_owned(),
            label: "Handlers".to_owned(),
            subtitle: Some("signals, updates, queries".to_owned()),
            node_ids: vec![handlers_node_id.clone()],
            state_ids: Vec::new(),
            focus_node_id: handlers_node_id.clone(),
            collapsed_by_default: false,
            source_anchor: None,
            raw: json!({
                "signals": artifact.signals.keys().collect::<Vec<_>>(),
                "updates": artifact.updates.keys().collect::<Vec<_>>(),
                "queries": artifact.queries.keys().collect::<Vec<_>>(),
            }),
        });
        edges.push(WorkflowGraphEdge {
            id: format!("{}->{}", workflow_entry_node_id(), handlers_node_id.clone()),
            source: workflow_entry_node_id(),
            target: handlers_node_id.clone(),
            label: "messages".to_owned(),
            kind: "system".to_owned(),
        });

        for (signal_name, handler) in &artifact.signals {
            let module_id = signal_handler_module_id(signal_name);
            let mut node_ids = Vec::new();
            let mut state_ids = Vec::new();
            for (state_id, state) in &handler.states {
                let node_id = signal_handler_node_id(signal_name, state_id);
                let source_anchor = resolve_source_anchor(
                    &artifact.source_map,
                    &[
                        format!("signal:{signal_name}:{state_id}"),
                        state_id.clone(),
                        format!("state:{state_id}"),
                    ],
                );
                let raw = to_value(state).unwrap_or(Value::Null);
                nodes.push(WorkflowGraphNode {
                    id: node_id.clone(),
                    graph: "signal_handler".to_owned(),
                    module_id: module_id.clone(),
                    state_id: Some(state_id.clone()),
                    kind: compiled_state_kind(state).to_owned(),
                    label: state_id.clone(),
                    subtitle: compiled_state_subtitle(state),
                    source_anchor,
                    next_ids: state_transition_targets(state)
                        .into_iter()
                        .map(|(_, target)| signal_handler_node_id(signal_name, &target))
                        .collect(),
                    raw,
                });
                node_ids.push(node_id.clone());
                state_ids.push(state_id.clone());
                for (label, target) in state_transition_targets(state) {
                    edges.push(WorkflowGraphEdge {
                        id: format!("{node_id}->{}", signal_handler_node_id(signal_name, &target)),
                        source: node_id.clone(),
                        target: signal_handler_node_id(signal_name, &target),
                        label,
                        kind: "handler_transition".to_owned(),
                    });
                }
            }
            let entry_node_id = signal_handler_node_id(signal_name, &handler.initial_state);
            modules.push(WorkflowGraphModule {
                id: module_id.clone(),
                graph: "signal_handler".to_owned(),
                kind: "signal_handler".to_owned(),
                label: signal_name.clone(),
                subtitle: Some("signal handler".to_owned()),
                node_ids,
                state_ids,
                focus_node_id: entry_node_id.clone(),
                collapsed_by_default: true,
                source_anchor: resolve_source_anchor(
                    &artifact.source_map,
                    &[format!("signal:{signal_name}"), signal_name.clone()],
                ),
                raw: json!({
                    "name": signal_name,
                    "arg_name": handler.arg_name,
                    "initial_state": handler.initial_state,
                }),
            });
            edges.push(WorkflowGraphEdge {
                id: format!("{handlers_node_id}->{entry_node_id}"),
                source: handlers_node_id.clone(),
                target: entry_node_id,
                label: format!("signal {signal_name}"),
                kind: "handler".to_owned(),
            });
        }

        for (update_name, handler) in &artifact.updates {
            let module_id = update_handler_module_id(update_name);
            let mut node_ids = Vec::new();
            let mut state_ids = Vec::new();
            for (state_id, state) in &handler.states {
                let node_id = update_handler_node_id(update_name, state_id);
                let source_anchor = resolve_source_anchor(
                    &artifact.source_map,
                    &[
                        format!("update:{update_name}:{state_id}"),
                        state_id.clone(),
                        format!("state:{state_id}"),
                    ],
                );
                let raw = to_value(state).unwrap_or(Value::Null);
                nodes.push(WorkflowGraphNode {
                    id: node_id.clone(),
                    graph: "update_handler".to_owned(),
                    module_id: module_id.clone(),
                    state_id: Some(state_id.clone()),
                    kind: compiled_state_kind(state).to_owned(),
                    label: state_id.clone(),
                    subtitle: compiled_state_subtitle(state),
                    source_anchor,
                    next_ids: state_transition_targets(state)
                        .into_iter()
                        .map(|(_, target)| update_handler_node_id(update_name, &target))
                        .collect(),
                    raw,
                });
                node_ids.push(node_id.clone());
                state_ids.push(state_id.clone());
                for (label, target) in state_transition_targets(state) {
                    edges.push(WorkflowGraphEdge {
                        id: format!("{node_id}->{}", update_handler_node_id(update_name, &target)),
                        source: node_id.clone(),
                        target: update_handler_node_id(update_name, &target),
                        label,
                        kind: "handler_transition".to_owned(),
                    });
                }
            }
            let entry_node_id = update_handler_node_id(update_name, &handler.initial_state);
            modules.push(WorkflowGraphModule {
                id: module_id.clone(),
                graph: "update_handler".to_owned(),
                kind: "update_handler".to_owned(),
                label: update_name.clone(),
                subtitle: Some("update handler".to_owned()),
                node_ids,
                state_ids,
                focus_node_id: entry_node_id.clone(),
                collapsed_by_default: true,
                source_anchor: resolve_source_anchor(
                    &artifact.source_map,
                    &[format!("update:{update_name}"), update_name.clone()],
                ),
                raw: json!({
                    "name": update_name,
                    "arg_name": handler.arg_name,
                    "initial_state": handler.initial_state,
                }),
            });
            edges.push(WorkflowGraphEdge {
                id: format!("{handlers_node_id}->{entry_node_id}"),
                source: handlers_node_id.clone(),
                target: entry_node_id,
                label: format!("update {update_name}"),
                kind: "handler".to_owned(),
            });
        }

        for (query_name, handler) in &artifact.queries {
            let node_id = query_node_id(query_name);
            let module_id = query_module_id(query_name);
            let source_anchor = resolve_source_anchor(
                &artifact.source_map,
                &[format!("query:{query_name}"), query_name.clone()],
            );
            let raw = to_value(handler).unwrap_or(Value::Null);
            nodes.push(WorkflowGraphNode {
                id: node_id.clone(),
                graph: "query_handler".to_owned(),
                module_id: module_id.clone(),
                state_id: None,
                kind: "query".to_owned(),
                label: query_name.clone(),
                subtitle: Some("query".to_owned()),
                source_anchor: source_anchor.clone(),
                next_ids: Vec::new(),
                raw: raw.clone(),
            });
            modules.push(WorkflowGraphModule {
                id: module_id,
                graph: "query_handler".to_owned(),
                kind: "query_handler".to_owned(),
                label: query_name.clone(),
                subtitle: Some("query".to_owned()),
                node_ids: vec![node_id.clone()],
                state_ids: Vec::new(),
                focus_node_id: node_id.clone(),
                collapsed_by_default: true,
                source_anchor,
                raw,
            });
            edges.push(WorkflowGraphEdge {
                id: format!("{handlers_node_id}->{node_id}"),
                source: handlers_node_id.clone(),
                target: node_id,
                label: format!("query {query_name}"),
                kind: "handler".to_owned(),
            });
        }
    }

    let module_lookup = nodes
        .iter()
        .map(|node| (node.id.clone(), node.module_id.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut seen_module_edges = BTreeSet::new();
    let mut module_edges = Vec::new();
    for edge in &edges {
        let Some(source_module) = module_lookup.get(&edge.source) else {
            continue;
        };
        let Some(target_module) = module_lookup.get(&edge.target) else {
            continue;
        };
        if source_module == target_module {
            continue;
        }
        let dedupe_key = format!("{source_module}->{target_module}:{}", edge.label);
        if seen_module_edges.insert(dedupe_key) {
            module_edges.push(WorkflowGraphEdge {
                id: format!("{source_module}->{target_module}:{}", edge.label),
                source: source_module.clone(),
                target: target_module.clone(),
                label: edge.label.clone(),
                kind: edge.kind.clone(),
            });
        }
    }

    WorkflowGraphProjection { nodes, edges, modules, module_edges }
}

fn build_artifact_overlay() -> WorkflowGraphOverlay {
    WorkflowGraphOverlay {
        mode: "artifact".to_owned(),
        run_id: None,
        current_node_id: None,
        current_module_id: None,
        blocked_by: None,
        node_statuses: Vec::new(),
        module_statuses: Vec::new(),
        trace: Vec::new(),
        activity_summaries: Vec::new(),
        bulk_summaries: Vec::new(),
        signal_summaries: Vec::new(),
        update_summaries: Vec::new(),
        child_summaries: Vec::new(),
    }
}

fn build_run_overlay(
    artifact: &CompiledWorkflowArtifact,
    run_id: &str,
    current_state: Option<String>,
    history: &[EventEnvelope<WorkflowEvent>],
    activities: &[WorkflowActivityRecord],
    signals: &[WorkflowSignalRecord],
    updates: &[WorkflowUpdateRecord],
    children: &[WorkflowChildRecord],
    bulk_batches: &[WorkflowBulkBatchRecord],
) -> WorkflowGraphOverlay {
    let current_node_id = current_state.as_deref().map(workflow_state_node_id);
    let current_module_id = current_state.as_deref().map(workflow_module_id);
    let state_to_module = artifact
        .workflow
        .states
        .keys()
        .map(|state_id| (state_id.clone(), workflow_module_id(state_id)))
        .collect::<BTreeMap<_, _>>();
    let trace = history
        .iter()
        .enumerate()
        .map(|(index, event)| {
            let state_id = graph_state_for_event(event);
            let node_id = state_id.as_deref().map(workflow_state_node_id);
            let module_id =
                state_id.as_deref().and_then(|state_id| state_to_module.get(state_id).cloned());
            WorkflowGraphTraceStep {
                id: format!("trace-{index}"),
                occurred_at: event.occurred_at,
                lane: timeline_lane_for_event(&event.event_type).to_owned(),
                label: event.event_type.clone(),
                detail: event_detail(event),
                event_type: event.event_type.clone(),
                node_id,
                module_id,
            }
        })
        .collect::<Vec<_>>();

    let mut node_summaries = BTreeMap::<String, (String, String)>::new();
    for state_id in history.iter().filter_map(graph_state_for_event) {
        let node_id = workflow_state_node_id(&state_id);
        node_summaries
            .entry(node_id)
            .or_insert_with(|| ("executed".to_owned(), "Visited in execution trace".to_owned()));
    }
    if let Some(current_state_id) = current_state.as_deref() {
        node_summaries.insert(
            workflow_state_node_id(current_state_id),
            ("current".to_owned(), "Current wait point".to_owned()),
        );
    }

    let activity_summaries = summarize_activities_by_state(activities);
    for summary in &activity_summaries {
        let status = if summary.failed > 0 {
            "failed"
        } else if summary.pending > 0 || summary.retrying > 0 {
            "pending"
        } else {
            "executed"
        };
        let detail = if summary.failed > 0 {
            format!("{} failed activity attempts", summary.failed)
        } else if summary.pending > 0 || summary.retrying > 0 {
            format!("{} pending, {} retrying", summary.pending, summary.retrying)
        } else {
            format!("{} activity attempts completed", summary.completed)
        };
        merge_status(&mut node_summaries, &summary.node_id, status, &detail);
    }

    let bulk_summaries = summarize_bulk_batches_by_state(bulk_batches);
    for summary in &bulk_summaries {
        let status = if summary.failed_items > 0 || summary.status == "failed" {
            "failed"
        } else if summary.status == "scheduled" || summary.status == "running" {
            "pending"
        } else {
            "executed"
        };
        let detail = format!(
            "{} items, {} failed, {} cancelled",
            summary.total_items, summary.failed_items, summary.cancelled_items
        );
        merge_status(&mut node_summaries, &summary.node_id, status, &detail);
    }

    let child_summaries = summarize_children(artifact, children);
    for summary in &child_summaries {
        if let Some(node_id) = summary.node_id.as_deref() {
            let status = if matches!(summary.status.as_str(), "failed" | "cancelled" | "terminated")
            {
                "failed"
            } else if summary.status == "running" || summary.status == "started" {
                "pending"
            } else {
                "executed"
            };
            merge_status(
                &mut node_summaries,
                node_id,
                status,
                &format!("child workflow {}", summary.status),
            );
        }
    }

    let signal_summaries = summarize_signals(artifact, signals);
    let update_summaries = summarize_updates(updates);

    let mut module_summaries = BTreeMap::<String, (String, String)>::new();
    for (node_id, (status, summary)) in &node_summaries {
        if let Some(module_id) = node_id_to_module_id(node_id) {
            merge_status(&mut module_summaries, &module_id, status, summary);
        }
    }
    for summary in &signal_summaries {
        if let Some(module_id) = summary.module_id.as_deref() {
            merge_status(
                &mut module_summaries,
                module_id,
                "pending",
                &format!("{} signals", summary.count),
            );
        }
    }
    for summary in &update_summaries {
        if let Some(module_id) = summary.module_id.as_deref() {
            let status = if summary.latest_status == "rejected" { "failed" } else { "pending" };
            merge_status(
                &mut module_summaries,
                module_id,
                status,
                &format!("{} updates", summary.count),
            );
        }
    }

    let blocked_by = current_state
        .as_deref()
        .and_then(|state_id| artifact.workflow.states.get(state_id).map(|state| (state_id, state)))
        .and_then(|(state_id, state)| {
            blocked_by_for_state(state_id, state, signals, bulk_batches, activities)
        });

    WorkflowGraphOverlay {
        mode: "run".to_owned(),
        run_id: Some(run_id.to_owned()),
        current_node_id,
        current_module_id,
        blocked_by,
        node_statuses: node_summaries
            .into_iter()
            .map(|(id, (status, summary))| WorkflowGraphOverlayStatus {
                id,
                status,
                summary: Some(summary),
            })
            .collect(),
        module_statuses: module_summaries
            .into_iter()
            .map(|(id, (status, summary))| WorkflowGraphOverlayStatus {
                id,
                status,
                summary: Some(summary),
            })
            .collect(),
        trace,
        activity_summaries,
        bulk_summaries,
        signal_summaries,
        update_summaries,
        child_summaries,
    }
}

fn resolve_page(config: &QueryRuntimeConfig, pagination: &PaginationQuery) -> ResolvedPage {
    let limit = pagination.limit.unwrap_or(config.default_page_size).min(config.max_page_size);
    ResolvedPage { limit: limit.max(1), offset: pagination.offset.unwrap_or(0) }
}

fn workflow_entry_node_id() -> String {
    "workflow:entry".to_owned()
}

fn workflow_entry_module_id() -> String {
    "module:workflow:entry".to_owned()
}

fn handlers_hub_node_id() -> String {
    "system:handlers".to_owned()
}

fn handlers_hub_module_id() -> String {
    "module:system:handlers".to_owned()
}

fn workflow_state_node_id(state_id: &str) -> String {
    format!("workflow:{state_id}")
}

fn workflow_module_id(state_id: &str) -> String {
    format!("module:workflow:{state_id}")
}

fn signal_handler_node_id(signal_name: &str, state_id: &str) -> String {
    format!("signal:{signal_name}:{state_id}")
}

fn signal_handler_module_id(signal_name: &str) -> String {
    format!("module:signal:{signal_name}")
}

fn update_handler_node_id(update_name: &str, state_id: &str) -> String {
    format!("update:{update_name}:{state_id}")
}

fn update_handler_module_id(update_name: &str) -> String {
    format!("module:update:{update_name}")
}

fn query_node_id(query_name: &str) -> String {
    format!("query:{query_name}")
}

fn query_module_id(query_name: &str) -> String {
    format!("module:query:{query_name}")
}

fn node_id_to_module_id(node_id: &str) -> Option<String> {
    if node_id == workflow_entry_node_id() {
        return Some(workflow_entry_module_id());
    }
    if node_id == handlers_hub_node_id() {
        return Some(handlers_hub_module_id());
    }
    node_id.strip_prefix("workflow:").map(workflow_module_id).or_else(|| {
        node_id.split_once(':').and_then(|(prefix, rest)| match prefix {
            "signal" => rest.split_once(':').map(|(name, _)| signal_handler_module_id(name)),
            "update" => rest.split_once(':').map(|(name, _)| update_handler_module_id(name)),
            "query" => Some(query_module_id(rest)),
            _ => None,
        })
    })
}

fn resolve_source_anchor(
    source_map: &BTreeMap<String, SourceLocation>,
    keys: &[String],
) -> Option<WorkflowGraphSourceAnchor> {
    keys.iter().find_map(|key| source_map.get(key)).map(source_anchor)
}

fn source_anchor(location: &SourceLocation) -> WorkflowGraphSourceAnchor {
    WorkflowGraphSourceAnchor {
        file: location.file.clone(),
        line: location.line,
        column: location.column,
    }
}

fn compiled_state_kind(state: &CompiledStateNode) -> &'static str {
    match state {
        CompiledStateNode::Assign { .. } => "assign",
        CompiledStateNode::RegisterDynamicSignalHandler { .. } => "register_dynamic_signal_handler",
        CompiledStateNode::Choice { .. } => "choice",
        CompiledStateNode::Step { .. } => "step",
        CompiledStateNode::DynamicStep { .. } => "dynamic_step",
        CompiledStateNode::CallAsyncHelper { .. } => "call_async_helper",
        CompiledStateNode::ReturnAsyncHelper { .. } => "return_async_helper",
        CompiledStateNode::RaiseAsyncHelper { .. } => "raise_async_helper",
        CompiledStateNode::StartStepHandle { .. } => "start_step_handle",
        CompiledStateNode::FanOut { .. } => "fan_out",
        CompiledStateNode::StartBulkActivity { .. } => "start_bulk_activity",
        CompiledStateNode::DynamicStartBulkActivity { .. } => "dynamic_start_bulk_activity",
        CompiledStateNode::WaitForBulkActivity { .. } => "wait_for_bulk_activity",
        CompiledStateNode::StartStreamJob { .. } => "start_stream_job",
        CompiledStateNode::WaitForStreamCheckpoint { .. } => "wait_for_stream_checkpoint",
        CompiledStateNode::QueryStreamJob { .. } => "query_stream_job",
        CompiledStateNode::CancelStreamJob { .. } => "cancel_stream_job",
        CompiledStateNode::AwaitStreamJobTerminal { .. } => "await_stream_job_terminal",
        CompiledStateNode::WaitForAllActivities { .. } => "wait_for_all_activities",
        CompiledStateNode::StartChild { .. } => "start_child",
        CompiledStateNode::SignalChild { .. } => "signal_child",
        CompiledStateNode::CancelChild { .. } => "cancel_child",
        CompiledStateNode::SignalExternal { .. } => "signal_external",
        CompiledStateNode::CancelExternal { .. } => "cancel_external",
        CompiledStateNode::WaitForChild { .. } => "wait_for_child",
        CompiledStateNode::WaitForEvent { .. } => "wait_for_event",
        CompiledStateNode::WaitForCondition { .. } => "wait_for_condition",
        CompiledStateNode::StartTimerHandle { .. } => "start_timer_handle",
        CompiledStateNode::WaitForTimer { .. } => "wait_for_timer",
        CompiledStateNode::Succeed { .. } => "succeed",
        CompiledStateNode::Fail { .. } => "fail",
        CompiledStateNode::ContinueAsNew { .. } => "continue_as_new",
    }
}

fn semantic_module_kind(_graph: &str, state: &CompiledStateNode) -> &'static str {
    match state {
        CompiledStateNode::Assign { .. }
        | CompiledStateNode::RegisterDynamicSignalHandler { .. }
        | CompiledStateNode::Choice { .. } => "assign_decision",
        CompiledStateNode::Step { .. }
        | CompiledStateNode::DynamicStep { .. }
        | CompiledStateNode::CallAsyncHelper { .. }
        | CompiledStateNode::ReturnAsyncHelper { .. }
        | CompiledStateNode::RaiseAsyncHelper { .. }
        | CompiledStateNode::StartStepHandle { .. } => "activity_step",
        CompiledStateNode::FanOut { .. } | CompiledStateNode::WaitForAllActivities { .. } => {
            "fan_out"
        }
        CompiledStateNode::StartBulkActivity { .. }
        | CompiledStateNode::DynamicStartBulkActivity { .. }
        | CompiledStateNode::WaitForBulkActivity { .. } => "bulk_activity",
        CompiledStateNode::StartStreamJob { .. }
        | CompiledStateNode::WaitForStreamCheckpoint { .. }
        | CompiledStateNode::QueryStreamJob { .. }
        | CompiledStateNode::CancelStreamJob { .. }
        | CompiledStateNode::AwaitStreamJobTerminal { .. } => "stream_job",
        CompiledStateNode::StartChild { .. }
        | CompiledStateNode::SignalChild { .. }
        | CompiledStateNode::CancelChild { .. }
        | CompiledStateNode::SignalExternal { .. }
        | CompiledStateNode::CancelExternal { .. }
        | CompiledStateNode::WaitForChild { .. } => "child_workflow",
        CompiledStateNode::WaitForEvent { .. } | CompiledStateNode::WaitForCondition { .. } => {
            "wait"
        }
        CompiledStateNode::StartTimerHandle { .. } | CompiledStateNode::WaitForTimer { .. } => {
            "timer"
        }
        CompiledStateNode::Succeed { .. }
        | CompiledStateNode::Fail { .. }
        | CompiledStateNode::ContinueAsNew { .. } => "terminal",
    }
}

fn module_collapsed_by_default(state: &CompiledStateNode) -> bool {
    matches!(
        state,
        CompiledStateNode::FanOut { .. }
            | CompiledStateNode::StartBulkActivity { .. }
            | CompiledStateNode::DynamicStartBulkActivity { .. }
            | CompiledStateNode::WaitForBulkActivity { .. }
            | CompiledStateNode::StartStreamJob { .. }
            | CompiledStateNode::WaitForStreamCheckpoint { .. }
            | CompiledStateNode::QueryStreamJob { .. }
            | CompiledStateNode::CancelStreamJob { .. }
            | CompiledStateNode::AwaitStreamJobTerminal { .. }
            | CompiledStateNode::StartChild { .. }
            | CompiledStateNode::WaitForChild { .. }
    )
}

fn compiled_state_subtitle(state: &CompiledStateNode) -> Option<String> {
    match state {
        CompiledStateNode::Assign { actions, .. } => Some(format!("{} assignments", actions.len())),
        CompiledStateNode::RegisterDynamicSignalHandler { .. } => {
            Some("register dynamic signal handler".to_owned())
        }
        CompiledStateNode::Choice { .. } => Some("branch".to_owned()),
        CompiledStateNode::Step { handler, task_queue, .. } => Some(
            task_queue
                .as_ref()
                .map(|_| format!("step · {handler}"))
                .unwrap_or_else(|| handler.clone()),
        ),
        CompiledStateNode::DynamicStep { .. } => Some("dynamic step".to_owned()),
        CompiledStateNode::CallAsyncHelper { helper_name, .. } => {
            Some(format!("call async helper · {helper_name}"))
        }
        CompiledStateNode::ReturnAsyncHelper { .. } => Some("return async helper".to_owned()),
        CompiledStateNode::RaiseAsyncHelper { .. } => Some("raise async helper".to_owned()),
        CompiledStateNode::StartStepHandle { handle_var, .. } => {
            Some(format!("start step handle · {handle_var}"))
        }
        CompiledStateNode::FanOut { activity_type, .. } => {
            Some(format!("fan-out · {activity_type}"))
        }
        CompiledStateNode::StartBulkActivity { activity_type, .. } => {
            Some(format!("bulk · {activity_type}"))
        }
        CompiledStateNode::DynamicStartBulkActivity { .. } => Some("bulk · dynamic".to_owned()),
        CompiledStateNode::WaitForBulkActivity { bulk_ref_var, .. } => {
            Some(format!("await bulk {bulk_ref_var}"))
        }
        CompiledStateNode::StartStreamJob { job_name, .. } => {
            Some(format!("stream job · {job_name}"))
        }
        CompiledStateNode::WaitForStreamCheckpoint { checkpoint_name, .. } => {
            Some(format!("await checkpoint · {checkpoint_name}"))
        }
        CompiledStateNode::QueryStreamJob { query_name, .. } => {
            Some(format!("stream query · {query_name}"))
        }
        CompiledStateNode::CancelStreamJob { .. } => Some("cancel stream job".to_owned()),
        CompiledStateNode::AwaitStreamJobTerminal { stream_job_ref_var, .. } => {
            Some(format!("await stream terminal {stream_job_ref_var}"))
        }
        CompiledStateNode::WaitForAllActivities { fanout_ref_var, .. } => {
            Some(format!("await fan-out {fanout_ref_var}"))
        }
        CompiledStateNode::StartChild { child_definition_id, .. } => {
            Some(format!("child · {child_definition_id}"))
        }
        CompiledStateNode::SignalChild { signal_name, .. } => {
            Some(format!("signal child · {signal_name}"))
        }
        CompiledStateNode::CancelChild { .. } => Some("cancel child".to_owned()),
        CompiledStateNode::SignalExternal { signal_name, .. } => {
            Some(format!("signal external · {signal_name}"))
        }
        CompiledStateNode::CancelExternal { .. } => Some("cancel external".to_owned()),
        CompiledStateNode::WaitForChild { child_ref_var, .. } => {
            Some(format!("await child {child_ref_var}"))
        }
        CompiledStateNode::WaitForEvent { event_type, .. } => {
            Some(format!("wait signal · {event_type}"))
        }
        CompiledStateNode::WaitForCondition { .. } => Some("wait condition".to_owned()),
        CompiledStateNode::StartTimerHandle { timer_ref, .. } => {
            Some(format!("start timer · {timer_ref}"))
        }
        CompiledStateNode::WaitForTimer { timer_ref, .. } => Some(format!("timer · {timer_ref}")),
        CompiledStateNode::Succeed { .. } => Some("success".to_owned()),
        CompiledStateNode::Fail { .. } => Some("failure".to_owned()),
        CompiledStateNode::ContinueAsNew { .. } => Some("continue as new".to_owned()),
    }
}

fn state_transition_targets(state: &CompiledStateNode) -> Vec<(String, String)> {
    match state {
        CompiledStateNode::Assign { next, .. }
        | CompiledStateNode::RegisterDynamicSignalHandler { next, .. } => {
            vec![("next".to_owned(), next.clone())]
        }
        CompiledStateNode::Choice { then_next, else_next, .. } => {
            vec![("then".to_owned(), then_next.clone()), ("else".to_owned(), else_next.clone())]
        }
        CompiledStateNode::Step { next, on_error, .. } => {
            let mut transitions = next
                .iter()
                .map(|target| ("success".to_owned(), target.clone()))
                .collect::<Vec<_>>();
            if let Some(on_error) = on_error {
                transitions.push(("error".to_owned(), on_error.next.clone()));
            }
            transitions
        }
        CompiledStateNode::DynamicStep { next, on_error, .. } => {
            let mut transitions = next
                .iter()
                .map(|target| ("success".to_owned(), target.clone()))
                .collect::<Vec<_>>();
            if let Some(on_error) = on_error {
                transitions.push(("error".to_owned(), on_error.next.clone()));
            }
            transitions
        }
        CompiledStateNode::CallAsyncHelper { next, on_error, .. } => {
            let mut transitions = vec![("return".to_owned(), next.clone())];
            if let Some(on_error) = on_error {
                transitions.push(("error".to_owned(), on_error.next.clone()));
            }
            transitions
        }
        CompiledStateNode::ReturnAsyncHelper { .. }
        | CompiledStateNode::RaiseAsyncHelper { .. } => Vec::new(),
        CompiledStateNode::StartStepHandle { next, .. } => vec![("next".to_owned(), next.clone())],
        CompiledStateNode::FanOut { next, .. } => vec![("await".to_owned(), next.clone())],
        CompiledStateNode::StartBulkActivity { next, .. }
        | CompiledStateNode::DynamicStartBulkActivity { next, .. } => {
            vec![("await".to_owned(), next.clone())]
        }
        CompiledStateNode::StartStreamJob { next, .. } => vec![("await".to_owned(), next.clone())],
        CompiledStateNode::WaitForStreamCheckpoint { next, on_error, .. }
        | CompiledStateNode::QueryStreamJob { next, on_error, .. }
        | CompiledStateNode::AwaitStreamJobTerminal { next, on_error, .. } => {
            let mut transitions = vec![("success".to_owned(), next.clone())];
            if let Some(on_error) = on_error {
                transitions.push(("error".to_owned(), on_error.next.clone()));
            }
            transitions
        }
        CompiledStateNode::CancelStreamJob { next, .. } => vec![("next".to_owned(), next.clone())],
        CompiledStateNode::WaitForBulkActivity { next, on_error, .. }
        | CompiledStateNode::WaitForAllActivities { next, on_error, .. }
        | CompiledStateNode::WaitForChild { next, on_error, .. } => {
            let mut transitions = vec![("success".to_owned(), next.clone())];
            if let Some(on_error) = on_error {
                transitions.push(("error".to_owned(), on_error.next.clone()));
            }
            transitions
        }
        CompiledStateNode::StartChild { next, .. }
        | CompiledStateNode::SignalChild { next, .. }
        | CompiledStateNode::CancelChild { next, .. }
        | CompiledStateNode::SignalExternal { next, .. }
        | CompiledStateNode::CancelExternal { next, .. }
        | CompiledStateNode::StartTimerHandle { next, .. }
        | CompiledStateNode::WaitForEvent { next, .. }
        | CompiledStateNode::WaitForTimer { next, .. } => vec![("next".to_owned(), next.clone())],
        CompiledStateNode::WaitForCondition { next, timeout_next, .. } => {
            let mut transitions = vec![("condition".to_owned(), next.clone())];
            if let Some(timeout_next) = timeout_next {
                transitions.push(("timeout".to_owned(), timeout_next.clone()));
            }
            transitions
        }
        CompiledStateNode::Succeed { .. }
        | CompiledStateNode::Fail { .. }
        | CompiledStateNode::ContinueAsNew { .. } => Vec::new(),
    }
}

fn timeline_lane_for_event(event_type: &str) -> &'static str {
    let lower = event_type.to_lowercase();
    if lower.contains("activity") {
        "activities"
    } else if lower.contains("signal") || lower.contains("update") || lower.contains("query") {
        "messages"
    } else if lower.contains("timer") {
        "timers"
    } else if lower.contains("child") {
        "children"
    } else if lower.contains("workflow") {
        "lifecycle"
    } else {
        "other"
    }
}

fn event_detail(event: &EventEnvelope<WorkflowEvent>) -> Option<String> {
    match &event.payload {
        WorkflowEvent::ActivityTaskScheduled { activity_type, attempt, .. } => {
            Some(format!("{activity_type} attempt {attempt}"))
        }
        WorkflowEvent::ActivityTaskFailed { error, .. } => Some(error.clone()),
        WorkflowEvent::BulkActivityBatchScheduled { activity_type, chunk_size, .. } => {
            Some(format!("{activity_type} chunks of {chunk_size}"))
        }
        WorkflowEvent::SignalQueued { signal_type, .. }
        | WorkflowEvent::SignalReceived { signal_type, .. } => Some(signal_type.clone()),
        WorkflowEvent::WorkflowUpdateRequested { update_name, .. }
        | WorkflowEvent::WorkflowUpdateAccepted { update_name, .. }
        | WorkflowEvent::WorkflowUpdateCompleted { update_name, .. }
        | WorkflowEvent::WorkflowUpdateRejected { update_name, .. } => Some(update_name.clone()),
        WorkflowEvent::ChildWorkflowStartRequested { child_definition_id, .. } => {
            Some(child_definition_id.clone())
        }
        WorkflowEvent::ChildWorkflowFailed { error, .. } => Some(error.clone()),
        WorkflowEvent::ChildWorkflowCancelled { reason, .. }
        | WorkflowEvent::ChildWorkflowTerminated { reason, .. } => Some(reason.clone()),
        WorkflowEvent::TimerScheduled { timer_id, .. } | WorkflowEvent::TimerFired { timer_id } => {
            Some(timer_id.clone())
        }
        WorkflowEvent::WorkflowFailed { reason } => Some(reason.clone()),
        _ => None,
    }
}

fn graph_state_for_event(event: &EventEnvelope<WorkflowEvent>) -> Option<String> {
    event.metadata.get("state").cloned().or_else(|| match &event.payload {
        WorkflowEvent::ActivityTaskScheduled { state, .. } => state.clone(),
        WorkflowEvent::BulkActivityBatchScheduled { state, .. } => state.clone(),
        _ => None,
    })
}

fn summarize_activities_by_state(
    activities: &[WorkflowActivityRecord],
) -> Vec<WorkflowGraphActivitySummary> {
    let mut grouped = BTreeMap::<String, WorkflowGraphActivitySummary>::new();
    for activity in activities {
        let Some(state_id) = activity.state.as_deref() else {
            continue;
        };
        let key = workflow_state_node_id(state_id);
        let entry = grouped.entry(key.clone()).or_insert_with(|| WorkflowGraphActivitySummary {
            node_id: key.clone(),
            module_id: workflow_module_id(state_id),
            activity_type: activity.activity_type.clone(),
            total: 0,
            pending: 0,
            completed: 0,
            failed: 0,
            retrying: 0,
            worker_build_ids: Vec::new(),
        });
        entry.total += 1;
        match workflow_activity_status_label(&activity.status) {
            "scheduled" | "started" => entry.pending += 1,
            "completed" => entry.completed += 1,
            "failed" | "timed_out" | "cancelled" => entry.failed += 1,
            _ => {}
        }
        if activity.attempt > 1 {
            entry.retrying += 1;
        }
        if let Some(build_id) = activity.worker_build_id.clone() {
            if !entry.worker_build_ids.iter().any(|existing| existing == &build_id) {
                entry.worker_build_ids.push(build_id);
            }
        }
    }
    grouped.into_values().collect()
}

fn summarize_bulk_batches_by_state(
    bulk_batches: &[WorkflowBulkBatchRecord],
) -> Vec<WorkflowGraphBulkSummary> {
    bulk_batches
        .iter()
        .filter_map(|batch| {
            batch.state.as_deref().map(|state_id| WorkflowGraphBulkSummary {
                node_id: workflow_state_node_id(state_id),
                module_id: workflow_module_id(state_id),
                batch_id: batch.batch_id.clone(),
                activity_type: batch.activity_type.clone(),
                status: workflow_bulk_status_label(&batch.status).to_owned(),
                total_items: batch.total_items,
                succeeded_items: batch.succeeded_items,
                failed_items: batch.failed_items,
                cancelled_items: batch.cancelled_items,
            })
        })
        .collect()
}

fn summarize_signals(
    artifact: &CompiledWorkflowArtifact,
    signals: &[WorkflowSignalRecord],
) -> Vec<WorkflowGraphSignalSummary> {
    let wait_states = artifact
        .workflow
        .states
        .iter()
        .filter_map(|(state_id, state)| match state {
            CompiledStateNode::WaitForEvent { event_type, .. } => {
                Some((event_type.clone(), state_id.clone()))
            }
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let mut grouped = BTreeMap::<String, WorkflowGraphSignalSummary>::new();
    for signal in signals {
        let entry = grouped.entry(signal.signal_type.clone()).or_insert_with(|| {
            let state_id = wait_states.get(&signal.signal_type).cloned();
            WorkflowGraphSignalSummary {
                signal_name: signal.signal_type.clone(),
                count: 0,
                latest_status: workflow_signal_status_label(&signal.status).to_owned(),
                latest_seen_at: signal.updated_at,
                node_id: state_id.as_deref().map(workflow_state_node_id),
                module_id: state_id.as_deref().map(workflow_module_id),
            }
        });
        entry.count += 1;
        if signal.updated_at >= entry.latest_seen_at {
            entry.latest_seen_at = signal.updated_at;
            entry.latest_status = workflow_signal_status_label(&signal.status).to_owned();
        }
    }
    grouped.into_values().collect()
}

fn summarize_updates(updates: &[WorkflowUpdateRecord]) -> Vec<WorkflowGraphUpdateSummary> {
    let mut grouped = BTreeMap::<String, WorkflowGraphUpdateSummary>::new();
    for update in updates {
        let entry = grouped.entry(update.update_name.clone()).or_insert_with(|| {
            WorkflowGraphUpdateSummary {
                update_name: update.update_name.clone(),
                count: 0,
                latest_status: workflow_update_status_label(&update.status).to_owned(),
                latest_seen_at: update.updated_at,
                module_id: Some(update_handler_module_id(&update.update_name)),
            }
        });
        entry.count += 1;
        if update.updated_at >= entry.latest_seen_at {
            entry.latest_seen_at = update.updated_at;
            entry.latest_status = workflow_update_status_label(&update.status).to_owned();
        }
    }
    grouped.into_values().collect()
}

fn summarize_children(
    artifact: &CompiledWorkflowArtifact,
    children: &[WorkflowChildRecord],
) -> Vec<WorkflowGraphChildSummary> {
    let child_state_lookup = artifact
        .workflow
        .states
        .iter()
        .filter_map(|(state_id, state)| match state {
            CompiledStateNode::StartChild { child_definition_id, .. } => {
                Some((child_definition_id.clone(), state_id.clone()))
            }
            _ => None,
        })
        .fold(BTreeMap::<String, Vec<String>>::new(), |mut acc, (definition_id, state_id)| {
            acc.entry(definition_id).or_default().push(state_id);
            acc
        });

    children
        .iter()
        .map(|child| {
            let state_id = child_state_lookup
                .get(&child.child_definition_id)
                .and_then(|matches| (matches.len() == 1).then(|| matches[0].clone()));
            WorkflowGraphChildSummary {
                child_id: child.child_id.clone(),
                child_definition_id: child.child_definition_id.clone(),
                child_workflow_id: child.child_workflow_id.clone(),
                child_run_id: child.child_run_id.clone(),
                status: child.status.clone(),
                node_id: state_id.as_deref().map(workflow_state_node_id),
                module_id: state_id.as_deref().map(workflow_module_id),
            }
        })
        .collect()
}

fn blocked_by_for_state(
    state_id: &str,
    state: &CompiledStateNode,
    signals: &[WorkflowSignalRecord],
    bulk_batches: &[WorkflowBulkBatchRecord],
    activities: &[WorkflowActivityRecord],
) -> Option<WorkflowGraphBlockedBy> {
    let node_id = Some(workflow_state_node_id(state_id));
    let module_id = Some(workflow_module_id(state_id));
    match state {
        CompiledStateNode::WaitForEvent { event_type, .. } => Some(WorkflowGraphBlockedBy {
            kind: "signal".to_owned(),
            label: event_type.clone(),
            detail: Some(format!(
                "{} queued/consumed signals",
                signals.iter().filter(|signal| signal.signal_type == *event_type).count()
            )),
            node_id,
            module_id,
        }),
        CompiledStateNode::WaitForTimer { timer_ref, .. } => Some(WorkflowGraphBlockedBy {
            kind: "timer".to_owned(),
            label: timer_ref.clone(),
            detail: Some("waiting for timer fire".to_owned()),
            node_id,
            module_id,
        }),
        CompiledStateNode::WaitForCondition { timeout_ref, .. } => Some(WorkflowGraphBlockedBy {
            kind: "condition".to_owned(),
            label: "condition".to_owned(),
            detail: timeout_ref.as_ref().map(|timeout_ref| format!("timeout {timeout_ref} armed")),
            node_id,
            module_id,
        }),
        CompiledStateNode::WaitForBulkActivity { bulk_ref_var, .. } => {
            let pending = bulk_batches
                .iter()
                .find(|batch| batch.state.as_deref() == Some(state_id))
                .map(|batch| format!("{} items still aggregating", batch.total_items));
            Some(WorkflowGraphBlockedBy {
                kind: "bulk".to_owned(),
                label: bulk_ref_var.clone(),
                detail: pending,
                node_id,
                module_id,
            })
        }
        CompiledStateNode::WaitForAllActivities { fanout_ref_var, .. } => {
            Some(WorkflowGraphBlockedBy {
                kind: "fan_out".to_owned(),
                label: fanout_ref_var.clone(),
                detail: Some(format!(
                    "{} active attempts",
                    activities
                        .iter()
                        .filter(|activity| {
                            activity.state.as_deref() == Some(state_id)
                                && matches!(
                                    workflow_activity_status_label(&activity.status),
                                    "scheduled" | "started"
                                )
                        })
                        .count()
                )),
                node_id,
                module_id,
            })
        }
        CompiledStateNode::Step { handler, .. } => Some(WorkflowGraphBlockedBy {
            kind: "activity".to_owned(),
            label: handler.clone(),
            detail: Some(format!(
                "{} active attempts",
                activities
                    .iter()
                    .filter(|activity| {
                        activity.state.as_deref() == Some(state_id)
                            && matches!(
                                workflow_activity_status_label(&activity.status),
                                "scheduled" | "started"
                            )
                    })
                    .count()
            )),
            node_id,
            module_id,
        }),
        CompiledStateNode::StartChild { child_definition_id, .. }
        | CompiledStateNode::WaitForChild { child_ref_var: child_definition_id, .. } => {
            Some(WorkflowGraphBlockedBy {
                kind: "child".to_owned(),
                label: child_definition_id.clone(),
                detail: Some("waiting on child workflow".to_owned()),
                node_id,
                module_id,
            })
        }
        _ => None,
    }
}

fn merge_status(
    target: &mut BTreeMap<String, (String, String)>,
    id: &str,
    next_status: &str,
    next_summary: &str,
) {
    match target.get(id) {
        Some((current_status, _)) if status_rank(current_status) >= status_rank(next_status) => {}
        _ => {
            target.insert(id.to_owned(), (next_status.to_owned(), next_summary.to_owned()));
        }
    }
}

fn status_rank(status: &str) -> u8 {
    match status {
        "current" => 4,
        "failed" => 3,
        "pending" => 2,
        "executed" => 1,
        _ => 0,
    }
}

fn workflow_activity_status_label(status: &WorkflowActivityStatus) -> &'static str {
    match status {
        WorkflowActivityStatus::Scheduled => "scheduled",
        WorkflowActivityStatus::Started => "started",
        WorkflowActivityStatus::Completed => "completed",
        WorkflowActivityStatus::Failed => "failed",
        WorkflowActivityStatus::TimedOut => "timed_out",
        WorkflowActivityStatus::Cancelled => "cancelled",
    }
}

fn workflow_bulk_status_label(status: &WorkflowBulkBatchStatus) -> &'static str {
    match status {
        WorkflowBulkBatchStatus::Scheduled => "scheduled",
        WorkflowBulkBatchStatus::Running => "running",
        WorkflowBulkBatchStatus::Completed => "completed",
        WorkflowBulkBatchStatus::Failed => "failed",
        WorkflowBulkBatchStatus::Cancelled => "cancelled",
    }
}

fn workflow_signal_status_label(status: &WorkflowSignalStatus) -> &'static str {
    match status {
        WorkflowSignalStatus::Queued => "queued",
        WorkflowSignalStatus::Dispatching => "dispatching",
        WorkflowSignalStatus::Consumed => "consumed",
    }
}

fn workflow_update_status_label(status: &WorkflowUpdateStatus) -> &'static str {
    match status {
        WorkflowUpdateStatus::Requested => "requested",
        WorkflowUpdateStatus::Accepted => "accepted",
        WorkflowUpdateStatus::Completed => "completed",
        WorkflowUpdateStatus::Rejected => "rejected",
    }
}

fn non_empty(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() { None } else { Some(trimmed.to_owned()) }
}

fn non_empty_ref(value: &str) -> Option<&str> {
    if value.trim().is_empty() { None } else { Some(value) }
}

fn build_page_info(page: &ResolvedPage, total: usize, returned: usize) -> PageInfo {
    let limit = page.limit;
    let offset = page.offset;
    let next_offset = (offset + returned < total).then_some(offset + returned);
    PageInfo { limit, offset, returned, total, has_more: next_offset.is_some(), next_offset }
}

async fn compute_routing_status(
    store: &WorkflowStore,
    tenant_id: &str,
    workflow_task_queue: &str,
    sticky_workflow_build_id: Option<&str>,
    artifact_hash: Option<&str>,
) -> Result<String> {
    let sticky_build_compatible_with_queue = if let Some(build_id) = sticky_workflow_build_id {
        Some(
            store
                .is_build_compatible_with_queue(
                    tenant_id,
                    TaskQueueKind::Workflow,
                    workflow_task_queue,
                    build_id,
                )
                .await?,
        )
    } else {
        None
    };
    let sticky_build_supports_pinned_artifact = if let Some(build_id) = sticky_workflow_build_id {
        Some(
            store
                .workflow_build_supports_artifact(
                    tenant_id,
                    workflow_task_queue,
                    build_id,
                    artifact_hash,
                )
                .await?,
        )
    } else {
        None
    };
    let status = match (sticky_build_compatible_with_queue, sticky_build_supports_pinned_artifact) {
        (Some(false), _) => "sticky_incompatible_fallback_required",
        (_, Some(false)) => "sticky_artifact_mismatch_fallback_required",
        (Some(true), Some(true)) | (Some(true), None) | (None, Some(true)) => "sticky_active",
        (None, None) => "queue_default_active",
    };
    Ok(status.to_owned())
}

async fn filtered_workflow_list_items(
    store: &WorkflowStore,
    tenant_id: &str,
    filters: &WorkflowInstanceFilters,
    page: &ResolvedPage,
    routing_status: &str,
) -> Result<(Vec<WorkflowListItem>, usize)> {
    const BATCH_SIZE: i64 = 200;
    let mut scan_offset = 0_i64;
    let mut matched = 0_usize;
    let mut items = Vec::new();

    loop {
        let batch = store
            .list_instances_page_with_filters(tenant_id, filters, BATCH_SIZE, scan_offset)
            .await?;
        if batch.is_empty() {
            break;
        }
        let batch_len = batch.len();
        for workflow in batch {
            let item = workflow_list_item_from_state(store, workflow).await?;
            if item.routing_status != routing_status {
                continue;
            }
            if matched >= page.offset && items.len() < page.limit {
                items.push(item);
            }
            matched = matched.saturating_add(1);
        }
        if batch_len < BATCH_SIZE as usize {
            break;
        }
        scan_offset += BATCH_SIZE;
    }

    Ok((items, matched))
}

async fn filtered_run_list_items(
    store: &WorkflowStore,
    tenant_id: &str,
    filters: &WorkflowRunFilters,
    page: &ResolvedPage,
    routing_status: &str,
) -> Result<(Vec<RunListItem>, usize)> {
    const BATCH_SIZE: i64 = 200;
    let mut scan_offset = 0_i64;
    let mut matched = 0_usize;
    let mut items = Vec::new();

    loop {
        let batch =
            store.list_runs_page_with_filters(tenant_id, filters, BATCH_SIZE, scan_offset).await?;
        if batch.is_empty() {
            break;
        }
        let batch_len = batch.len();
        for run in batch {
            let item = run_list_item_from_record(store, run).await?;
            if item.routing_status != routing_status {
                continue;
            }
            if matched >= page.offset && items.len() < page.limit {
                items.push(item);
            }
            matched = matched.saturating_add(1);
        }
        if batch_len < BATCH_SIZE as usize {
            break;
        }
        scan_offset += BATCH_SIZE;
    }

    Ok((items, matched))
}

async fn workflow_list_item_from_state(
    store: &WorkflowStore,
    state: WorkflowInstanceState,
) -> Result<WorkflowListItem> {
    let run_metadata =
        store.get_run_record(&state.tenant_id, &state.instance_id, &state.run_id).await?;
    let routing_status = compute_routing_status(
        store,
        &state.tenant_id,
        &state.workflow_task_queue,
        state.sticky_workflow_build_id.as_deref(),
        state.artifact_hash.as_deref(),
    )
    .await?;
    Ok(WorkflowListItem {
        tenant_id: state.tenant_id,
        instance_id: state.instance_id,
        run_id: state.run_id,
        definition_id: state.definition_id,
        definition_version: state.definition_version,
        artifact_hash: state.artifact_hash,
        workflow_task_queue: state.workflow_task_queue,
        memo: state.memo,
        search_attributes: state.search_attributes,
        sticky_workflow_build_id: state.sticky_workflow_build_id,
        sticky_workflow_poller_id: state.sticky_workflow_poller_id,
        routing_status,
        admission_mode: run_metadata.as_ref().map(|run| run.admission_mode.clone()),
        fast_path_rejection_reason: run_metadata
            .as_ref()
            .and_then(|run| run.fast_path_rejection_reason.clone()),
        current_state: state.current_state,
        status: state.status.as_str().to_owned(),
        event_count: state.event_count,
        last_event_id: state.last_event_id,
        last_event_type: state.last_event_type,
        updated_at: state.updated_at,
        consistency: "eventual",
        source: "projection",
    })
}

async fn run_list_item_from_record(
    store: &WorkflowStore,
    record: WorkflowRunSummaryRecord,
) -> Result<RunListItem> {
    let routing_status = compute_routing_status(
        store,
        &record.tenant_id,
        &record.workflow_task_queue,
        record.sticky_workflow_build_id.as_deref(),
        record.artifact_hash.as_deref(),
    )
    .await?;
    Ok(RunListItem {
        tenant_id: record.tenant_id,
        instance_id: record.instance_id,
        run_id: record.run_id,
        definition_id: record.definition_id,
        definition_version: record.definition_version,
        artifact_hash: record.artifact_hash,
        workflow_task_queue: record.workflow_task_queue,
        memo: record.memo,
        search_attributes: record.search_attributes,
        sticky_workflow_build_id: record.sticky_workflow_build_id,
        sticky_workflow_poller_id: record.sticky_workflow_poller_id,
        routing_status,
        admission_mode: record.admission_mode,
        fast_path_rejection_reason: record.fast_path_rejection_reason,
        sticky_updated_at: record.sticky_updated_at,
        previous_run_id: record.previous_run_id,
        next_run_id: record.next_run_id,
        continue_reason: record.continue_reason,
        started_at: record.started_at,
        closed_at: record.closed_at,
        updated_at: record.updated_at,
        last_transition_at: record.last_transition_at,
        status: record.status,
        current_state: record.current_state,
        last_event_type: record.last_event_type,
        event_count: record.event_count,
        consistency: "eventual",
        source: "projection",
    })
}

fn activity_graph_node(
    node_id: &str,
    activity: &WorkflowActivityRecord,
) -> WorkflowExecutionGraphNode {
    WorkflowExecutionGraphNode {
        id: node_id.to_owned(),
        kind: "activity".to_owned(),
        label: activity.activity_type.clone(),
        status: match activity.status {
            fabrik_store::WorkflowActivityStatus::Scheduled => "scheduled",
            fabrik_store::WorkflowActivityStatus::Started => "started",
            fabrik_store::WorkflowActivityStatus::Completed => "completed",
            fabrik_store::WorkflowActivityStatus::Failed => "failed",
            fabrik_store::WorkflowActivityStatus::TimedOut => "timed_out",
            fabrik_store::WorkflowActivityStatus::Cancelled => "cancelled",
        }
        .to_owned(),
        subtitle: Some(format!("{} attempt {}", activity.activity_id, activity.attempt)),
    }
}

fn child_graph_node(node_id: &str, child: &WorkflowChildRecord) -> WorkflowExecutionGraphNode {
    WorkflowExecutionGraphNode {
        id: node_id.to_owned(),
        kind: "child_workflow".to_owned(),
        label: child.child_definition_id.clone(),
        status: child.status.clone(),
        subtitle: Some(child.child_workflow_id.clone()),
    }
}

fn queue_kind_label(queue_kind: &TaskQueueKind) -> &'static str {
    match queue_kind {
        TaskQueueKind::Workflow => "workflow",
        TaskQueueKind::Activity => "activity",
    }
}

fn signal_status_label(signal: &WorkflowSignalRecord) -> String {
    match signal.status {
        fabrik_store::WorkflowSignalStatus::Queued => "queued",
        fabrik_store::WorkflowSignalStatus::Dispatching => "dispatching",
        fabrik_store::WorkflowSignalStatus::Consumed => "consumed",
    }
    .to_owned()
}

fn dedupe_graph_nodes(nodes: &mut Vec<WorkflowExecutionGraphNode>) {
    let mut seen = std::collections::HashSet::new();
    nodes.retain(|node| seen.insert(node.id.clone()));
}

fn dedupe_graph_edges(edges: &mut Vec<WorkflowExecutionGraphEdge>) {
    let mut seen = std::collections::HashSet::new();
    edges.retain(|edge| seen.insert(edge.id.clone()));
}

fn projection_lag_ms_from_times(
    times: impl IntoIterator<Item = Option<DateTime<Utc>>>,
) -> Option<i64> {
    let now = Utc::now();
    times
        .into_iter()
        .flatten()
        .min()
        .map(|updated_at| now.signed_duration_since(updated_at).num_milliseconds().max(0))
}

fn stream_query_source_label(consistency_source: &str) -> String {
    match consistency_source {
        "stream_owner_local_state" => "stream-owner",
        "stream_projection_query" => "stream-view-query",
        other if !other.is_empty() => other,
        _ => "stream-owner",
    }
    .to_owned()
}

fn stream_job_view_scan_item_from_value(value: Value) -> Result<StreamJobViewScanItemRecord> {
    let object = value
        .as_object()
        .with_context(|| format!("stream job scan item must be an object: {value}"))?;
    let logical_key = object
        .get("logicalKey")
        .and_then(Value::as_str)
        .context("stream job scan item missing logicalKey")?
        .to_owned();
    let output = object.get("output").cloned().context("stream job scan item missing output")?;
    let checkpoint_sequence = object
        .get("checkpointSequence")
        .and_then(Value::as_i64)
        .context("stream job scan item missing checkpointSequence")?;
    let updated_at = parse_rfc3339_value(
        object
            .get("updatedAt")
            .and_then(Value::as_str)
            .context("stream job scan item missing updatedAt")?,
    )?;
    let window_expired_at = object
        .get("windowExpiredAt")
        .and_then(Value::as_str)
        .map(parse_rfc3339_value)
        .transpose()?;
    let window_retention_seconds = object.get("windowRetentionSeconds").and_then(Value::as_i64);
    Ok(StreamJobViewScanItemRecord {
        logical_key,
        output,
        checkpoint_sequence,
        updated_at,
        window_expired_at,
        window_retention_seconds,
    })
}

fn parse_rfc3339_value(value: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("invalid RFC3339 timestamp {value}"))?
        .with_timezone(&Utc))
}

fn stream_job_view_summaries(
    view_definitions: Option<&Value>,
    projection_summaries: Vec<StreamJobViewProjectionSummaryRecord>,
) -> Vec<StreamJobViewSummary> {
    let mut views = BTreeMap::new();
    for summary in projection_summaries {
        views.insert(
            summary.view_name.clone(),
            StreamJobViewSummary {
                view_name: summary.view_name,
                definition: None,
                projected_key_count: summary.key_count as usize,
                latest_projected_checkpoint_sequence: summary.latest_checkpoint_sequence,
                latest_projected_at: summary.latest_updated_at,
                eventual_projection_lag_ms: projection_lag_ms_from_times([
                    summary.latest_updated_at
                ]),
            },
        );
    }
    for (view_name, definition) in declared_stream_job_views(view_definitions) {
        views
            .entry(view_name.clone())
            .and_modify(|view| view.definition = Some(definition.clone()))
            .or_insert(StreamJobViewSummary {
                view_name,
                definition: Some(definition),
                projected_key_count: 0,
                latest_projected_checkpoint_sequence: None,
                latest_projected_at: None,
                eventual_projection_lag_ms: None,
            });
    }
    views.into_values().collect()
}

fn stream_job_checkpoint_summaries(
    checkpoint_policy: Option<&Value>,
    latest_checkpoint_name: Option<&str>,
    latest_checkpoint_sequence: Option<i64>,
    latest_checkpoint_at: Option<DateTime<Utc>>,
) -> Vec<StreamJobCheckpointSummary> {
    let Some(checkpoint_policy) = checkpoint_policy else {
        return Vec::new();
    };
    let checkpoints = match checkpoint_policy {
        Value::Array(entries) => Some(entries),
        Value::Object(object) => object.get("checkpoints").and_then(Value::as_array),
        _ => None,
    };
    let mut declared = BTreeMap::new();
    for checkpoint in checkpoints.into_iter().flatten() {
        let Some(checkpoint_name) = checkpoint.get("name").and_then(Value::as_str) else {
            continue;
        };
        let sequence = checkpoint.get("sequence").and_then(Value::as_i64);
        let reached = latest_checkpoint_name == Some(checkpoint_name)
            || sequence.zip(latest_checkpoint_sequence).is_some_and(|(left, right)| left <= right);
        declared.entry(checkpoint_name.to_owned()).or_insert(StreamJobCheckpointSummary {
            checkpoint_name: checkpoint_name.to_owned(),
            definition: Some(checkpoint.clone()),
            delivery: checkpoint.get("delivery").and_then(Value::as_str).map(ToOwned::to_owned),
            sequence,
            reached,
            reached_at: (latest_checkpoint_name == Some(checkpoint_name))
                .then_some(latest_checkpoint_at)
                .flatten(),
        });
    }
    declared.into_values().collect()
}

fn stream_job_surface_summary(
    declared_checkpoints: &[StreamJobCheckpointSummary],
    views: &[StreamJobViewSummary],
    latest_checkpoint_name: Option<&String>,
    latest_checkpoint_sequence: Option<i64>,
    latest_checkpoint_at: Option<DateTime<Utc>>,
) -> StreamJobSurfaceSummary {
    StreamJobSurfaceSummary {
        declared_checkpoint_count: declared_checkpoints.len(),
        reached_checkpoint_count: declared_checkpoints
            .iter()
            .filter(|checkpoint| checkpoint.reached)
            .count(),
        latest_checkpoint_name: latest_checkpoint_name.cloned(),
        latest_checkpoint_sequence,
        latest_checkpoint_at,
        view_count: views.len(),
        projected_view_count: views
            .iter()
            .filter(|view| view.latest_projected_at.is_some())
            .count(),
        total_projected_keys: views.iter().map(|view| view.projected_key_count).sum(),
        slowest_eventual_view_lag_ms: views
            .iter()
            .filter_map(|view| view.eventual_projection_lag_ms)
            .max(),
        checkpoints: declared_checkpoints.to_vec(),
        views: views.to_vec(),
    }
}

fn declared_stream_job_views(view_definitions: Option<&Value>) -> Vec<(String, Value)> {
    let Some(view_definitions) = view_definitions else {
        return Vec::new();
    };
    let definitions = match view_definitions {
        Value::Array(entries) => Some(entries),
        Value::Object(object) => object.get("views").and_then(Value::as_array),
        _ => None,
    };
    let mut declared = BTreeMap::new();
    for definition in definitions.into_iter().flatten() {
        let Some(view_name) = definition.get("name").and_then(Value::as_str) else {
            continue;
        };
        declared.entry(view_name.to_owned()).or_insert_with(|| definition.clone());
    }
    declared.into_iter().collect()
}

fn authoritative_bulk_source(throughput_backend: &str) -> &'static str {
    if throughput_backend == ThroughputBackend::StreamV2.as_str() {
        "bridge-owner-routed"
    } else {
        "workflow-store"
    }
}

fn projected_bulk_source(throughput_backend: &str) -> &'static str {
    if throughput_backend == ThroughputBackend::StreamV2.as_str() {
        "stream-projection"
    } else {
        "workflow-store"
    }
}

fn bridge_status_from_ledger(
    bridge_state: ThroughputBridgeLedgerRecord,
) -> WorkflowBulkBridgeStatus {
    let submission_status = bridge_state
        .parsed_submission_status()
        .map(|status| status.as_str().to_owned())
        .or(bridge_state.submission_status.clone());
    let stream_status = bridge_state
        .parsed_stream_status()
        .map(|status| status.as_str().to_owned())
        .unwrap_or_else(|| {
            bridge_state.stream_status.clone().unwrap_or_else(|| "missing".to_owned())
        });
    let workflow_status = bridge_state
        .parsed_workflow_status()
        .map(|status| status.as_str().to_owned())
        .or(bridge_state.workflow_status.clone());
    let next_repair = bridge_state.next_repair().map(|repair| repair.as_str().to_owned());
    WorkflowBulkBridgeStatus {
        batch_id: bridge_state.batch_id,
        protocol_version: bridge_state.protocol_version,
        operation_kind: bridge_state.operation_kind,
        source_workflow_event_id: bridge_state.workflow_event_id,
        bridge_request_id: bridge_state.bridge_request_id,
        submission_status,
        submission_consistency: "bridge-store",
        command_id: bridge_state.command_id,
        command_partition_key: bridge_state.command_partition_key,
        stream_status,
        stream_consistency: "stream-runtime",
        stream_terminal_at: bridge_state.stream_terminal_at,
        cancellation_requested_at: bridge_state.cancellation_requested_at,
        cancellation_reason: bridge_state.cancellation_reason,
        cancel_command_published_at: bridge_state.cancel_command_published_at,
        cancelled_at: bridge_state.cancelled_at,
        workflow_status,
        workflow_consistency: "workflow-history",
        workflow_terminal_event_id: bridge_state.workflow_terminal_event_id,
        workflow_owner_epoch: bridge_state.workflow_owner_epoch,
        workflow_accepted_at: bridge_state.workflow_accepted_at,
        next_repair,
    }
}

fn bridge_repair_from_ledger(
    bridge_state: ThroughputBridgeLedgerRecord,
) -> Option<WorkflowBulkBridgeRepairRecord> {
    let next_repair = bridge_state.next_repair()?.as_str().to_owned();
    let submission_status = bridge_state
        .parsed_submission_status()
        .map(|status| status.as_str().to_owned())
        .or(bridge_state.submission_status.clone());
    let stream_status = bridge_state
        .parsed_stream_status()
        .map(|status| status.as_str().to_owned())
        .unwrap_or_else(|| {
            bridge_state.stream_status.clone().unwrap_or_else(|| "missing".to_owned())
        });
    let workflow_status = bridge_state
        .parsed_workflow_status()
        .map(|status| status.as_str().to_owned())
        .or(bridge_state.workflow_status.clone());
    Some(WorkflowBulkBridgeRepairRecord {
        tenant_id: bridge_state.tenant_id,
        instance_id: bridge_state.instance_id,
        run_id: bridge_state.run_id,
        batch_id: bridge_state.batch_id,
        protocol_version: bridge_state.protocol_version,
        operation_kind: bridge_state.operation_kind,
        source_workflow_event_id: bridge_state.workflow_event_id,
        bridge_request_id: bridge_state.bridge_request_id,
        next_repair,
        submission_status,
        command_id: bridge_state.command_id,
        command_partition_key: bridge_state.command_partition_key,
        command_published_at: bridge_state.command_published_at,
        cancellation_requested_at: bridge_state.cancellation_requested_at,
        cancellation_reason: bridge_state.cancellation_reason,
        cancel_command_published_at: bridge_state.cancel_command_published_at,
        cancelled_at: bridge_state.cancelled_at,
        stream_status,
        stream_terminal_at: bridge_state.stream_terminal_at,
        workflow_status,
        workflow_terminal_event_id: bridge_state.workflow_terminal_event_id,
        workflow_owner_epoch: bridge_state.workflow_owner_epoch,
        workflow_accepted_at: bridge_state.workflow_accepted_at,
    })
}

async fn load_bulk_bridge_status(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
    throughput_backend: &str,
) -> Result<Option<WorkflowBulkBridgeStatus>> {
    if throughput_backend != ThroughputBackend::StreamV2.as_str() {
        return Ok(None);
    }
    let Some(bridge_state) = state
        .store
        .load_throughput_bridge_ledger_for_batch(tenant_id, instance_id, run_id, batch_id)
        .await?
    else {
        return Ok(None);
    };
    Ok(Some(bridge_status_from_ledger(bridge_state)))
}

async fn load_pending_stream_bridge_repairs(
    state: &AppState,
    throughput_backend: &str,
    pagination: PaginationQuery,
) -> Result<WorkflowBulkBridgeRepairsResponse> {
    let page = resolve_page(&state.query, &pagination);
    let limit = page.limit.saturating_add(page.offset);
    let pending = state
        .store
        .list_pending_throughput_bridge_repairs_for_backend(throughput_backend, limit.max(1))
        .await?;
    let repair_count = pending.len();
    let repairs = pending
        .into_iter()
        .skip(page.offset)
        .take(page.limit)
        .filter_map(bridge_repair_from_ledger)
        .collect::<Vec<_>>();
    Ok(WorkflowBulkBridgeRepairsResponse {
        throughput_backend: throughput_backend.to_owned(),
        page: build_page_info(&page, repair_count, repairs.len()),
        repair_count,
        repairs,
    })
}

fn stream_job_summary(
    job: StreamJobRecord,
    bridge_surface: StreamJobBridgeSurfaceSummary,
    checkpoint_count: usize,
    query_count: usize,
    views: Vec<StreamJobViewSummary>,
    latest_query: (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<DateTime<Utc>>,
        Option<DateTime<Utc>>,
        Option<DateTime<Utc>>,
    ),
) -> StreamJobSummary {
    let origin_kind =
        job.parsed_origin_kind().unwrap_or(fabrik_throughput::StreamJobOriginKind::Workflow);
    let checkpoint_policy = job.checkpoint_policy.clone();
    let declared_checkpoints = stream_job_checkpoint_summaries(
        checkpoint_policy.as_ref(),
        job.latest_checkpoint_name.as_deref(),
        job.latest_checkpoint_sequence,
        job.latest_checkpoint_at,
    );
    let stream_surface = stream_job_surface_summary(
        &declared_checkpoints,
        &views,
        job.latest_checkpoint_name.as_ref(),
        job.latest_checkpoint_sequence,
        job.latest_checkpoint_at,
    );
    let status = job
        .parsed_status()
        .map(|status| status.as_str().to_owned())
        .unwrap_or_else(|| job.status.clone());
    StreamJobSummary {
        protocol_version: job.protocol_version,
        operation_kind: job.operation_kind,
        origin_kind: origin_kind.as_str().to_owned(),
        stream_instance_id: job.stream_instance_id.clone(),
        stream_run_id: job.stream_run_id.clone(),
        source_workflow_event_id: job.workflow_event_id,
        bridge_request_id: job.bridge_request_id,
        handle_id: job.handle_id,
        job_id: job.job_id,
        definition_id: job.definition_id,
        definition_version: job.definition_version,
        artifact_hash: job.artifact_hash,
        job_name: job.job_name,
        input_ref: job.input_ref,
        config_ref: job.config_ref,
        checkpoint_policy,
        declared_checkpoints,
        view_definitions: job.view_definitions,
        stream_surface,
        bridge_surface,
        status,
        workflow_owner_epoch: job.workflow_owner_epoch,
        stream_owner_epoch: job.stream_owner_epoch,
        starting_at: job.starting_at,
        running_at: job.running_at,
        draining_at: job.draining_at,
        latest_checkpoint_name: job.latest_checkpoint_name,
        latest_checkpoint_sequence: job.latest_checkpoint_sequence,
        latest_checkpoint_at: job.latest_checkpoint_at,
        latest_checkpoint_output: job.latest_checkpoint_output,
        cancellation_requested_at: job.cancellation_requested_at,
        cancellation_reason: job.cancellation_reason,
        workflow_accepted_at: job.workflow_accepted_at,
        terminal_event_id: job.terminal_event_id,
        terminal_at: job.terminal_at,
        terminal_output: job.terminal_output,
        terminal_error: job.terminal_error,
        checkpoint_count,
        query_count,
        latest_query_id: latest_query.0,
        latest_query_name: latest_query.1,
        latest_query_status: latest_query.2,
        latest_query_consistency: latest_query.3,
        latest_query_requested_at: latest_query.4,
        latest_query_completed_at: latest_query.5,
        latest_query_accepted_at: latest_query.6,
        views,
        workflow_binding: matches!(origin_kind, fabrik_throughput::StreamJobOriginKind::Workflow)
            .then_some(StreamJobWorkflowBinding {
                instance_id: job.instance_id.clone(),
                run_id: job.run_id.clone(),
            }),
        created_at: job.created_at,
        updated_at: job.updated_at,
    }
}

fn stream_job_matches_filters(job: &StreamJobRecord, filters: &StreamJobListQuery) -> bool {
    if let Some(status) = filters.status.clone().and_then(non_empty) {
        let job_status =
            job.parsed_status().map(|value| value.as_str()).unwrap_or(job.status.as_str());
        if job_status != status {
            return false;
        }
    }
    if let Some(job_name) = filters.job_name.clone().and_then(non_empty)
        && job.job_name != job_name
    {
        return false;
    }
    if let Some(definition_id) = filters.definition_id.clone().and_then(non_empty)
        && job.definition_id != definition_id
    {
        return false;
    }
    if let Some(origin_kind) = filters.origin_kind.clone().and_then(non_empty) {
        let actual_origin = job
            .parsed_origin_kind()
            .map(|value| value.as_str())
            .unwrap_or(job.origin_kind.as_str());
        if actual_origin != origin_kind {
            return false;
        }
    }
    if let Some(instance_id) = filters.instance_id.clone().and_then(non_empty)
        && job.instance_id != instance_id
    {
        return false;
    }
    if let Some(run_id) = filters.run_id.clone().and_then(non_empty)
        && job.run_id != run_id
    {
        return false;
    }
    if let Some(stream_instance_id) = filters.stream_instance_id.clone().and_then(non_empty)
        && job.stream_instance_id != stream_instance_id
    {
        return false;
    }
    if let Some(stream_run_id) = filters.stream_run_id.clone().and_then(non_empty)
        && job.stream_run_id != stream_run_id
    {
        return false;
    }
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamJobSort {
    CreatedAtAsc,
    CreatedAtDesc,
    UpdatedAtDesc,
    LatestCheckpointDesc,
}

impl StreamJobSort {
    fn parse(raw: Option<&str>) -> Result<Self> {
        match raw.unwrap_or("created_at_asc") {
            "created_at_asc" => Ok(Self::CreatedAtAsc),
            "created_at_desc" => Ok(Self::CreatedAtDesc),
            "updated_at_desc" => Ok(Self::UpdatedAtDesc),
            "latest_checkpoint_desc" => Ok(Self::LatestCheckpointDesc),
            other => anyhow::bail!("unsupported stream job sort {other}"),
        }
    }
}

fn stream_job_sort_key(
    summary: &StreamJobSummary,
    sort: StreamJobSort,
) -> (Option<DateTime<Utc>>, DateTime<Utc>, &str) {
    match sort {
        StreamJobSort::CreatedAtAsc | StreamJobSort::CreatedAtDesc => {
            (None, summary.created_at, summary.job_id.as_str())
        }
        StreamJobSort::UpdatedAtDesc => (None, summary.updated_at, summary.job_id.as_str()),
        StreamJobSort::LatestCheckpointDesc => {
            (summary.latest_checkpoint_at, summary.updated_at, summary.job_id.as_str())
        }
    }
}

async fn resolve_stream_job(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    job_id: &str,
) -> Result<StreamJobRecord> {
    if let Some(job) = state.store.get_stream_job(tenant_id, instance_id, run_id, job_id).await? {
        return Ok(job);
    }
    state
        .store
        .get_stream_job_by_stream_identity(tenant_id, instance_id, run_id, job_id)
        .await?
        .with_context(|| {
            format!(
                "stream job not found for tenant={tenant_id}, instance_id={instance_id}, run_id={run_id}, job_id={job_id}"
            )
        })
}

async fn list_stream_jobs_for_requested_identity(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<StreamJobRecord>> {
    let workflow_jobs = state
        .store
        .list_stream_jobs_for_run_page(tenant_id, instance_id, run_id, limit, offset)
        .await?;
    if !workflow_jobs.is_empty() {
        return Ok(workflow_jobs);
    }
    state
        .store
        .list_stream_jobs_for_stream_run_page(tenant_id, instance_id, run_id, limit, offset)
        .await
}

async fn summarize_stream_job_batch(
    state: &AppState,
    tenant_id: &str,
    batch: Vec<StreamJobRecord>,
    query: &StreamJobListQuery,
) -> Result<Vec<StreamJobSummary>> {
    let mut jobs_by_identity = BTreeMap::<(String, String), Vec<String>>::new();
    let handle_ids = batch.iter().map(|job| job.handle_id.clone()).collect::<Vec<_>>();
    for job in &batch {
        jobs_by_identity
            .entry((job.instance_id.clone(), job.run_id.clone()))
            .or_default()
            .push(job.job_id.clone());
    }

    let mut view_summaries_by_job =
        BTreeMap::<String, Vec<StreamJobViewProjectionSummaryRecord>>::new();
    for ((instance_id, run_id), job_ids) in jobs_by_identity {
        let summaries = state
            .store
            .list_stream_job_view_projection_summaries_for_jobs(
                tenant_id,
                &instance_id,
                &run_id,
                &job_ids,
            )
            .await?;
        for summary in summaries {
            view_summaries_by_job.entry(summary.job_id.clone()).or_default().push(summary);
        }
    }

    let mut ledgers_by_handle = state
        .store
        .list_stream_job_callback_ledgers_for_handle_ids(tenant_id, &handle_ids)
        .await?
        .into_iter()
        .map(|ledger| (ledger.handle_id.clone(), ledger))
        .collect::<BTreeMap<_, _>>();

    let mut summaries = Vec::new();
    for job in batch {
        if !stream_job_matches_filters(&job, query) {
            continue;
        }
        let ledger = ledgers_by_handle.remove(&job.handle_id);
        let view_definitions = job.view_definitions.clone();
        let view_summaries = stream_job_view_summaries(
            view_definitions.as_ref(),
            view_summaries_by_job.remove(&job.job_id).unwrap_or_default(),
        );
        let bridge_surface = stream_job_bridge_surface_summary(ledger.as_ref());
        let latest_query = latest_stream_job_query_summary_from_ledger(ledger.as_ref());
        let checkpoint_count =
            ledger.as_ref().map(|ledger| ledger.checkpoint_count as usize).unwrap_or_default();
        let query_count =
            ledger.as_ref().map(|ledger| ledger.query_count as usize).unwrap_or_default();
        summaries.push(stream_job_summary(
            job,
            bridge_surface,
            checkpoint_count,
            query_count,
            view_summaries,
            latest_query,
        ));
    }
    Ok(summaries)
}

async fn load_stream_jobs(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    query: StreamJobListQuery,
) -> Result<StreamJobsResponse> {
    const BATCH_SIZE: i64 = 256;
    let sort = StreamJobSort::parse(query.sort.as_deref())?;
    let page =
        resolve_page(&state.query, &PaginationQuery { limit: query.limit, offset: query.offset });
    let mut scan_offset = 0_i64;
    let mut summaries = Vec::new();
    let mut resolved_workflow_instance_id = instance_id.to_owned();
    let mut resolved_workflow_run_id = run_id.to_owned();
    let mut resolved_stream_instance_id = instance_id.to_owned();
    let mut resolved_stream_run_id = run_id.to_owned();
    loop {
        let batch = list_stream_jobs_for_requested_identity(
            state,
            tenant_id,
            instance_id,
            run_id,
            BATCH_SIZE,
            scan_offset,
        )
        .await?;
        if batch.is_empty() {
            break;
        }
        let batch_len = batch.len();
        if let Some(first_job) = batch.first() {
            resolved_workflow_instance_id = first_job.instance_id.clone();
            resolved_workflow_run_id = first_job.run_id.clone();
            resolved_stream_instance_id = first_job.stream_instance_id.clone();
            resolved_stream_run_id = first_job.stream_run_id.clone();
        }
        summaries.extend(summarize_stream_job_batch(state, tenant_id, batch, &query).await?);
        if batch_len < BATCH_SIZE as usize {
            break;
        }
        scan_offset += BATCH_SIZE;
    }
    match sort {
        StreamJobSort::CreatedAtAsc => summaries.sort_by(|left, right| {
            stream_job_sort_key(left, sort).cmp(&stream_job_sort_key(right, sort))
        }),
        StreamJobSort::CreatedAtDesc
        | StreamJobSort::UpdatedAtDesc
        | StreamJobSort::LatestCheckpointDesc => summaries.sort_by(|left, right| {
            stream_job_sort_key(right, sort).cmp(&stream_job_sort_key(left, sort))
        }),
    }
    let job_count = summaries.len();
    let jobs = summaries.into_iter().skip(page.offset).take(page.limit).collect::<Vec<_>>();
    Ok(StreamJobsResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: resolved_workflow_instance_id,
        stream_instance_id: resolved_stream_instance_id,
        run_id: resolved_workflow_run_id,
        stream_run_id: resolved_stream_run_id,
        page: build_page_info(&page, job_count, jobs.len()),
        job_count,
        jobs,
    })
}

async fn load_tenant_stream_jobs(
    state: &AppState,
    tenant_id: &str,
    query: StreamJobListQuery,
) -> Result<TenantStreamJobsResponse> {
    const BATCH_SIZE: i64 = 256;
    let sort = StreamJobSort::parse(query.sort.as_deref())?;
    let page =
        resolve_page(&state.query, &PaginationQuery { limit: query.limit, offset: query.offset });
    let mut scan_offset = 0_i64;
    let mut summaries = Vec::new();
    loop {
        let batch = state
            .store
            .list_stream_jobs_for_tenant_page(tenant_id, BATCH_SIZE, scan_offset)
            .await?;
        if batch.is_empty() {
            break;
        }
        let batch_len = batch.len();
        summaries.extend(summarize_stream_job_batch(state, tenant_id, batch, &query).await?);
        if batch_len < BATCH_SIZE as usize {
            break;
        }
        scan_offset += BATCH_SIZE;
    }
    match sort {
        StreamJobSort::CreatedAtAsc => summaries.sort_by(|left, right| {
            stream_job_sort_key(left, sort).cmp(&stream_job_sort_key(right, sort))
        }),
        StreamJobSort::CreatedAtDesc
        | StreamJobSort::UpdatedAtDesc
        | StreamJobSort::LatestCheckpointDesc => summaries.sort_by(|left, right| {
            stream_job_sort_key(right, sort).cmp(&stream_job_sort_key(left, sort))
        }),
    }
    let job_count = summaries.len();
    let jobs = summaries.into_iter().skip(page.offset).take(page.limit).collect::<Vec<_>>();
    Ok(TenantStreamJobsResponse {
        tenant_id: tenant_id.to_owned(),
        page: build_page_info(&page, job_count, jobs.len()),
        job_count,
        jobs,
    })
}

async fn load_stream_job(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    job_id: &str,
) -> Result<StreamJobResponse> {
    let job = resolve_stream_job(state, tenant_id, instance_id, run_id, job_id).await?;
    let workflow_instance_id = job.instance_id.clone();
    let workflow_run_id = job.run_id.clone();
    let stream_instance_id = job.stream_instance_id.clone();
    let stream_run_id = job.stream_run_id.clone();
    let view_definitions = job.view_definitions.clone();
    let checkpoints = state
        .store
        .list_stream_job_callback_checkpoints_for_handle_page(&job.handle_id, 10_000, 0)
        .await?;
    let queries = state
        .store
        .list_stream_job_callback_queries_for_handle_page(&job.handle_id, 10_000, 0)
        .await?;
    let bridge_ledger =
        state.store.load_stream_job_callback_ledger_for_handle(&job.handle_id).await?;
    let handle_stream_owner_epoch = job.stream_owner_epoch;
    let view_projection_summaries = state
        .store
        .list_stream_job_view_projection_summaries(tenant_id, &job.instance_id, &job.run_id, job_id)
        .await?;
    let views = stream_job_view_summaries(view_definitions.as_ref(), view_projection_summaries);
    let summary = stream_job_summary(
        job,
        stream_job_bridge_surface_summary(bridge_ledger.as_ref()),
        checkpoints.len(),
        queries.len(),
        views.clone(),
        latest_stream_job_query_summary_from_ledger(bridge_ledger.as_ref()),
    );
    Ok(StreamJobResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: workflow_instance_id,
        stream_instance_id,
        run_id: workflow_run_id,
        stream_run_id,
        job_id: job_id.to_owned(),
        job: summary,
        views,
        checkpoints: checkpoints
            .into_iter()
            .map(|checkpoint| stream_job_checkpoint_view(checkpoint, handle_stream_owner_epoch))
            .collect(),
        queries: queries
            .into_iter()
            .map(|query| stream_job_query_view(query, handle_stream_owner_epoch))
            .collect(),
    })
}

fn stream_job_checkpoint_view(
    checkpoint: StreamJobCheckpointRecord,
    handle_stream_owner_epoch: Option<u64>,
) -> StreamJobBridgeCheckpointView {
    let status = checkpoint
        .parsed_status()
        .map(|status| status.as_str().to_owned())
        .unwrap_or_else(|| checkpoint.status.clone());
    let next_repair = checkpoint
        .next_repair_for_handle_epoch(handle_stream_owner_epoch)
        .map(|repair| repair.as_str().to_owned());
    StreamJobBridgeCheckpointView {
        protocol_version: checkpoint.protocol_version,
        operation_kind: checkpoint.operation_kind,
        source_workflow_event_id: checkpoint.workflow_event_id,
        bridge_request_id: checkpoint.bridge_request_id,
        await_request_id: checkpoint.await_request_id,
        checkpoint_name: checkpoint.checkpoint_name,
        checkpoint_sequence: checkpoint.checkpoint_sequence,
        status,
        workflow_owner_epoch: checkpoint.workflow_owner_epoch,
        stream_owner_epoch: checkpoint.stream_owner_epoch,
        reached_at: checkpoint.reached_at,
        output: checkpoint.output,
        accepted_at: checkpoint.accepted_at,
        cancelled_at: checkpoint.cancelled_at,
        next_repair,
        created_at: checkpoint.created_at,
        updated_at: checkpoint.updated_at,
    }
}

fn stream_job_query_view(
    query: StreamJobQueryRecord,
    handle_stream_owner_epoch: Option<u64>,
) -> StreamJobBridgeQueryView {
    let consistency = query
        .parsed_consistency()
        .map(|consistency| consistency.as_str().to_owned())
        .unwrap_or_else(|| query.consistency.clone());
    let status = query
        .parsed_status()
        .map(|status| status.as_str().to_owned())
        .unwrap_or_else(|| query.status.clone());
    let next_repair = query
        .next_repair_for_handle_epoch(handle_stream_owner_epoch)
        .map(|repair| repair.as_str().to_owned());
    StreamJobBridgeQueryView {
        protocol_version: query.protocol_version,
        operation_kind: query.operation_kind,
        source_workflow_event_id: query.workflow_event_id,
        bridge_request_id: query.bridge_request_id,
        query_id: query.query_id,
        query_name: query.query_name,
        query_args: query.query_args,
        consistency,
        status,
        workflow_owner_epoch: query.workflow_owner_epoch,
        stream_owner_epoch: query.stream_owner_epoch,
        output: query.output,
        error: query.error,
        requested_at: query.requested_at,
        completed_at: query.completed_at,
        accepted_at: query.accepted_at,
        cancelled_at: query.cancelled_at,
        next_repair,
        created_at: query.created_at,
        updated_at: query.updated_at,
    }
}

fn stream_job_pending_repairs(
    handle: &StreamJobBridgeHandleRecord,
    checkpoints: &[StreamJobCheckpointRecord],
    queries: &[StreamJobQueryRecord],
) -> Vec<String> {
    let mut repairs = BTreeSet::new();
    if let Some(repair) = handle.next_repair() {
        repairs.insert(repair.as_str().to_owned());
    }
    for checkpoint in checkpoints {
        if let Some(repair) = checkpoint.next_repair_for_handle_epoch(handle.stream_owner_epoch) {
            repairs.insert(repair.as_str().to_owned());
        }
    }
    for query in queries {
        if let Some(repair) = query.next_repair_for_handle_epoch(handle.stream_owner_epoch) {
            repairs.insert(repair.as_str().to_owned());
        }
    }
    repairs.into_iter().collect()
}

fn stream_job_pending_repairs_from_ledger(ledger: &StreamJobBridgeLedgerRecord) -> Vec<String> {
    ledger.pending_repairs().into_iter().map(|repair| repair.as_str().to_owned()).collect()
}

fn latest_stream_job_query_summary(
    queries: &[StreamJobQueryRecord],
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
) {
    let Some(query) = queries.iter().max_by_key(|query| {
        (
            query.accepted_at.or(query.completed_at).unwrap_or(query.requested_at),
            query.query_id.clone(),
        )
    }) else {
        return (None, None, None, None, None, None, None);
    };
    let status = query
        .parsed_status()
        .map(|status| status.as_str().to_owned())
        .unwrap_or_else(|| query.status.clone());
    let consistency = query
        .parsed_consistency()
        .map(|consistency| consistency.as_str().to_owned())
        .unwrap_or_else(|| query.consistency.clone());
    (
        Some(query.query_id.clone()),
        Some(query.query_name.clone()),
        Some(status),
        Some(consistency),
        Some(query.requested_at),
        query.completed_at,
        query.accepted_at,
    )
}

fn latest_stream_job_query_summary_from_ledger(
    ledger: Option<&StreamJobBridgeLedgerRecord>,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
) {
    let Some(ledger) = ledger else {
        return (None, None, None, None, None, None, None);
    };
    let latest_query_status = ledger
        .parsed_latest_query_status()
        .map(|status| status.as_str().to_owned())
        .or(ledger.latest_query_status.clone());
    let latest_query_consistency = ledger
        .parsed_latest_query_consistency()
        .map(|consistency| consistency.as_str().to_owned())
        .or(ledger.latest_query_consistency.clone());
    (
        ledger.latest_query_id.clone(),
        ledger.latest_query_name.clone(),
        latest_query_status,
        latest_query_consistency,
        ledger.latest_query_requested_at,
        ledger.latest_query_completed_at,
        ledger.latest_query_accepted_at,
    )
}

fn stream_job_bridge_surface_summary(
    ledger: Option<&StreamJobBridgeLedgerRecord>,
) -> StreamJobBridgeSurfaceSummary {
    let pending_repairs = ledger.map(stream_job_pending_repairs_from_ledger).unwrap_or_default();
    let latest_query = latest_stream_job_query_summary_from_ledger(ledger);
    StreamJobBridgeSurfaceSummary {
        pending_repair_count: pending_repairs.len(),
        next_repair: pending_repairs.first().cloned(),
        pending_repairs,
        latest_query_id: latest_query.0,
        latest_query_name: latest_query.1,
        latest_query_status: latest_query.2,
        latest_query_consistency: latest_query.3,
        latest_query_requested_at: latest_query.4,
        latest_query_completed_at: latest_query.5,
        latest_query_accepted_at: latest_query.6,
    }
}

fn stream_job_ledger_matches_filters(
    ledger: &StreamJobBridgeLedgerRecord,
    pending_repairs: &[String],
    filters: &StreamJobBridgeListQuery,
) -> bool {
    if let Some(status) = filters.status.clone().and_then(non_empty) {
        let handle_status =
            ledger.parsed_status().map(|status| status.as_str()).unwrap_or(ledger.status.as_str());
        if handle_status != status {
            return false;
        }
    }
    if let Some(next_repair) = filters.next_repair.clone().and_then(non_empty)
        && !pending_repairs.iter().any(|repair| repair == &next_repair)
    {
        return false;
    }
    if let Some(latest_query_status) = filters.latest_query_status.clone().and_then(non_empty)
        && ledger
            .parsed_latest_query_status()
            .map(|status| status.as_str())
            .or(ledger.latest_query_status.as_deref())
            != Some(latest_query_status.as_str())
    {
        return false;
    }
    if let Some(latest_query_name) = filters.latest_query_name.clone().and_then(non_empty)
        && ledger.latest_query_name.as_deref() != Some(latest_query_name.as_str())
    {
        return false;
    }
    if let Some(latest_query_consistency) =
        filters.latest_query_consistency.clone().and_then(non_empty)
        && ledger
            .parsed_latest_query_consistency()
            .map(|consistency| consistency.as_str())
            .or(ledger.latest_query_consistency.as_deref())
            != Some(latest_query_consistency.as_str())
    {
        return false;
    }
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamJobBridgeSort {
    CreatedAtAsc,
    CreatedAtDesc,
    LatestQueryActivityDesc,
    LatestQueryActivityAsc,
}

impl StreamJobBridgeSort {
    fn parse(raw: Option<&str>) -> Result<Self> {
        match raw.unwrap_or("created_at_asc") {
            "created_at_asc" => Ok(Self::CreatedAtAsc),
            "created_at_desc" => Ok(Self::CreatedAtDesc),
            "latest_query_activity_desc" => Ok(Self::LatestQueryActivityDesc),
            "latest_query_activity_asc" => Ok(Self::LatestQueryActivityAsc),
            other => anyhow::bail!("unsupported stream job sort {other}"),
        }
    }
}

fn stream_job_summary_sort_key(
    summary: &StreamJobBridgeHandleSummary,
    sort: StreamJobBridgeSort,
) -> (Option<DateTime<Utc>>, DateTime<Utc>, &str) {
    let latest_query_activity = summary
        .latest_query_accepted_at
        .or(summary.latest_query_completed_at)
        .or(summary.latest_query_requested_at);
    match sort {
        StreamJobBridgeSort::CreatedAtAsc | StreamJobBridgeSort::CreatedAtDesc => {
            (None, summary.created_at, summary.handle_id.as_str())
        }
        StreamJobBridgeSort::LatestQueryActivityDesc
        | StreamJobBridgeSort::LatestQueryActivityAsc => {
            (latest_query_activity, summary.created_at, summary.handle_id.as_str())
        }
    }
}

fn stream_job_handle_summary(
    handle: StreamJobBridgeHandleRecord,
    pending_repairs: Vec<String>,
    checkpoint_count: usize,
    query_count: usize,
    latest_query: (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<DateTime<Utc>>,
        Option<DateTime<Utc>>,
        Option<DateTime<Utc>>,
    ),
) -> StreamJobBridgeHandleSummary {
    let status = handle
        .parsed_status()
        .map(|status| status.as_str().to_owned())
        .unwrap_or_else(|| handle.status.clone());
    StreamJobBridgeHandleSummary {
        protocol_version: handle.protocol_version,
        operation_kind: handle.operation_kind,
        source_workflow_event_id: handle.workflow_event_id,
        bridge_request_id: handle.bridge_request_id,
        handle_id: handle.handle_id,
        job_id: handle.job_id,
        definition_id: handle.definition_id,
        definition_version: handle.definition_version,
        artifact_hash: handle.artifact_hash,
        job_name: handle.job_name,
        input_ref: handle.input_ref,
        config_ref: handle.config_ref,
        checkpoint_policy: handle.checkpoint_policy,
        view_definitions: handle.view_definitions,
        status,
        workflow_owner_epoch: handle.workflow_owner_epoch,
        stream_owner_epoch: handle.stream_owner_epoch,
        cancellation_requested_at: handle.cancellation_requested_at,
        cancellation_reason: handle.cancellation_reason,
        terminal_event_id: handle.terminal_event_id,
        terminal_at: handle.terminal_at,
        workflow_accepted_at: handle.workflow_accepted_at,
        checkpoint_count,
        query_count,
        pending_repair_count: pending_repairs.len(),
        next_repair: pending_repairs.first().cloned(),
        pending_repairs,
        latest_query_id: latest_query.0,
        latest_query_name: latest_query.1,
        latest_query_status: latest_query.2,
        latest_query_consistency: latest_query.3,
        latest_query_requested_at: latest_query.4,
        latest_query_completed_at: latest_query.5,
        latest_query_accepted_at: latest_query.6,
        created_at: handle.created_at,
        updated_at: handle.updated_at,
    }
}

fn stream_job_handle_summary_from_ledger(
    ledger: StreamJobBridgeLedgerRecord,
) -> StreamJobBridgeHandleSummary {
    let pending_repairs = stream_job_pending_repairs_from_ledger(&ledger);
    let status = ledger
        .parsed_status()
        .map(|status| status.as_str().to_owned())
        .unwrap_or_else(|| ledger.status.clone());
    let latest_query_status = ledger
        .parsed_latest_query_status()
        .map(|status| status.as_str().to_owned())
        .or(ledger.latest_query_status.clone());
    let latest_query_consistency = ledger
        .parsed_latest_query_consistency()
        .map(|consistency| consistency.as_str().to_owned())
        .or(ledger.latest_query_consistency.clone());
    StreamJobBridgeHandleSummary {
        protocol_version: ledger.protocol_version,
        operation_kind: ledger.operation_kind,
        source_workflow_event_id: ledger.workflow_event_id,
        bridge_request_id: ledger.bridge_request_id,
        handle_id: ledger.handle_id,
        job_id: ledger.job_id,
        definition_id: ledger.definition_id,
        definition_version: ledger.definition_version,
        artifact_hash: ledger.artifact_hash,
        job_name: ledger.job_name,
        input_ref: ledger.input_ref,
        config_ref: ledger.config_ref,
        checkpoint_policy: None,
        view_definitions: None,
        status,
        workflow_owner_epoch: ledger.workflow_owner_epoch,
        stream_owner_epoch: ledger.stream_owner_epoch,
        cancellation_requested_at: ledger.cancellation_requested_at,
        cancellation_reason: ledger.cancellation_reason,
        terminal_event_id: ledger.terminal_event_id,
        terminal_at: ledger.terminal_at,
        workflow_accepted_at: ledger.workflow_accepted_at,
        checkpoint_count: ledger.checkpoint_count as usize,
        query_count: ledger.query_count as usize,
        pending_repair_count: pending_repairs.len(),
        next_repair: pending_repairs.first().cloned(),
        pending_repairs,
        latest_query_id: ledger.latest_query_id,
        latest_query_name: ledger.latest_query_name,
        latest_query_status,
        latest_query_consistency,
        latest_query_requested_at: ledger.latest_query_requested_at,
        latest_query_completed_at: ledger.latest_query_completed_at,
        latest_query_accepted_at: ledger.latest_query_accepted_at,
        created_at: ledger.created_at,
        updated_at: ledger.updated_at,
    }
}

async fn load_stream_job_callback_handles(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    query: StreamJobBridgeListQuery,
) -> Result<StreamJobBridgeHandlesResponse> {
    let (page, handle_count, handles) = load_stream_job_callback_handle_summaries(
        state,
        tenant_id,
        instance_id,
        run_id,
        query,
        false,
    )
    .await?;
    Ok(StreamJobBridgeHandlesResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        page,
        handle_count,
        handles,
    })
}

async fn load_stream_job_callback_repairs(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    query: StreamJobBridgeListQuery,
) -> Result<StreamJobBridgeRepairsResponse> {
    let (page, repair_count, handles) = load_stream_job_callback_handle_summaries(
        state,
        tenant_id,
        instance_id,
        run_id,
        query,
        true,
    )
    .await?;
    Ok(StreamJobBridgeRepairsResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        page,
        repair_count,
        handles,
    })
}

async fn load_stream_job_callback_handle_summaries(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    query: StreamJobBridgeListQuery,
    repairs_only: bool,
) -> Result<(PageInfo, usize, Vec<StreamJobBridgeHandleSummary>)> {
    const BATCH_SIZE: i64 = 200;
    let sort = StreamJobBridgeSort::parse(query.sort.as_deref())?;
    let page =
        resolve_page(&state.query, &PaginationQuery { limit: query.limit, offset: query.offset });
    let mut scan_offset = 0_i64;
    let mut filtered_summaries = Vec::new();
    loop {
        let batch = state
            .store
            .list_stream_job_callback_ledgers_for_run_page(
                tenant_id,
                instance_id,
                run_id,
                BATCH_SIZE,
                scan_offset,
            )
            .await?;
        if batch.is_empty() {
            break;
        }
        let batch_len = batch.len();
        for ledger in batch {
            let pending_repairs = stream_job_pending_repairs_from_ledger(&ledger);
            if repairs_only && pending_repairs.is_empty() {
                continue;
            }
            if !stream_job_ledger_matches_filters(&ledger, &pending_repairs, &query) {
                continue;
            }
            let _ = pending_repairs;
            filtered_summaries.push(stream_job_handle_summary_from_ledger(ledger));
        }
        if batch_len < BATCH_SIZE as usize {
            break;
        }
        scan_offset += BATCH_SIZE;
    }
    match sort {
        StreamJobBridgeSort::CreatedAtAsc => filtered_summaries.sort_by(|left, right| {
            stream_job_summary_sort_key(left, sort).cmp(&stream_job_summary_sort_key(right, sort))
        }),
        StreamJobBridgeSort::CreatedAtDesc => filtered_summaries.sort_by(|left, right| {
            stream_job_summary_sort_key(right, sort).cmp(&stream_job_summary_sort_key(left, sort))
        }),
        StreamJobBridgeSort::LatestQueryActivityDesc => {
            filtered_summaries.sort_by(|left, right| {
                stream_job_summary_sort_key(right, sort)
                    .cmp(&stream_job_summary_sort_key(left, sort))
            })
        }
        StreamJobBridgeSort::LatestQueryActivityAsc => filtered_summaries.sort_by(|left, right| {
            stream_job_summary_sort_key(left, sort).cmp(&stream_job_summary_sort_key(right, sort))
        }),
    }
    let handle_count = filtered_summaries.len();
    let summaries =
        filtered_summaries.into_iter().skip(page.offset).take(page.limit).collect::<Vec<_>>();
    Ok((build_page_info(&page, handle_count, summaries.len()), handle_count, summaries))
}

async fn load_stream_job_callback_handle(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    job_id: &str,
) -> Result<StreamJobBridgeHandleResponse> {
    let handle = state
        .store
        .get_stream_job_callback_handle(tenant_id, instance_id, run_id, job_id)
        .await?
        .with_context(|| {
            format!(
                "stream job bridge handle not found for tenant={tenant_id}, instance_id={instance_id}, run_id={run_id}, job_id={job_id}"
            )
        })?;
    let checkpoints = state
        .store
        .list_stream_job_callback_checkpoints_for_handle_page(&handle.handle_id, 10_000, 0)
        .await?;
    let queries = state
        .store
        .list_stream_job_callback_queries_for_handle_page(&handle.handle_id, 10_000, 0)
        .await?;
    let ledger = state.store.load_stream_job_bridge_ledger_for_handle(&handle.handle_id).await?;
    Ok(StreamJobBridgeHandleResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        job_id: job_id.to_owned(),
        handle: ledger.map(stream_job_handle_summary_from_ledger).unwrap_or_else(|| {
            let checkpoint_count = checkpoints.len();
            let query_count = queries.len();
            let latest_query = latest_stream_job_query_summary(&queries);
            let pending_repairs = stream_job_pending_repairs(&handle, &checkpoints, &queries);
            stream_job_handle_summary(
                handle.clone(),
                pending_repairs,
                checkpoint_count,
                query_count,
                latest_query,
            )
        }),
        checkpoints: checkpoints
            .into_iter()
            .map(|checkpoint| stream_job_checkpoint_view(checkpoint, handle.stream_owner_epoch))
            .collect(),
        queries: queries
            .into_iter()
            .map(|query| stream_job_query_view(query, handle.stream_owner_epoch))
            .collect(),
    })
}

fn parse_read_consistency(raw: Option<&str>) -> Result<ReadConsistency> {
    match raw.unwrap_or("eventual") {
        "eventual" => Ok(ReadConsistency::Eventual),
        "strong" => Ok(ReadConsistency::Strong),
        other => anyhow::bail!("unsupported consistency {other}"),
    }
}

fn retention_policy_response(config: &QueryRuntimeConfig) -> RetentionPolicyResponse {
    RetentionPolicyResponse {
        history_retention_days: config.history_retention_days,
        run_retention_days: config.run_retention_days,
        activity_retention_days: config.activity_retention_days,
        signal_retention_days: config.signal_retention_days,
        snapshot_retention_days: config.snapshot_retention_days,
        retention_sweep_interval_seconds: config.retention_sweep_interval_seconds,
    }
}

fn internal_error(error: anyhow::Error) -> (StatusCode, Json<ErrorResponse>) {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { message: error.to_string() }))
}

fn not_found(message: impl Into<String>) -> (StatusCode, Json<ErrorResponse>) {
    (StatusCode::NOT_FOUND, Json(ErrorResponse { message: message.into() }))
}

fn invalid_request(error: anyhow::Error) -> (StatusCode, Json<ErrorResponse>) {
    (StatusCode::BAD_REQUEST, Json(ErrorResponse { message: error.to_string() }))
}

fn internal_error_from_display(error: impl std::fmt::Display) -> (StatusCode, Json<ErrorResponse>) {
    internal_error(anyhow::anyhow!("{error}"))
}

async fn load_workflow_history(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    pagination: PaginationQuery,
) -> Result<WorkflowHistoryResponse> {
    let history = read_history(state, tenant_id, instance_id, run_id).await?;
    let first_event = history.first().ok_or_else(|| {
        anyhow::anyhow!(
            "no workflow history found for tenant={tenant_id}, instance_id={instance_id}, run_id={run_id}"
        )
    })?;
    let page = resolve_page(&state.query, &pagination);
    let total = history.len();
    let events = history.iter().skip(page.offset).take(page.limit).cloned().collect::<Vec<_>>();
    let activity_attempts = state
        .store
        .list_activities_for_run_page(
            tenant_id,
            instance_id,
            run_id,
            i64::try_from(page.limit).context("history page limit exceeds i64")?,
            i64::try_from(page.offset).context("history page offset exceeds i64")?,
        )
        .await?;
    let run = state.store.get_run_record(tenant_id, instance_id, run_id).await?;

    Ok(WorkflowHistoryResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        definition_id: first_event.definition_id.clone(),
        definition_version: first_event.definition_version,
        artifact_hash: first_event.artifact_hash.clone(),
        previous_run_id: run.as_ref().and_then(|run| run.previous_run_id.clone()),
        next_run_id: run.as_ref().and_then(|run| run.next_run_id.clone()),
        continue_reason: run.and_then(|run| run.continue_reason),
        event_count: total,
        page: build_page_info(&page, total, events.len()),
        activity_attempt_count: state
            .store
            .count_activities_for_run(tenant_id, instance_id, run_id)
            .await? as usize,
        activity_attempts,
        events,
    })
}

async fn load_workflow_activities(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    pagination: PaginationQuery,
) -> Result<WorkflowActivitiesResponse> {
    let page = resolve_page(&state.query, &pagination);
    let total =
        state.store.count_activities_for_run(tenant_id, instance_id, run_id).await? as usize;
    let activities = state
        .store
        .list_activities_for_run_page(
            tenant_id,
            instance_id,
            run_id,
            i64::try_from(page.limit).context("activities page limit exceeds i64")?,
            i64::try_from(page.offset).context("activities page offset exceeds i64")?,
        )
        .await?;
    Ok(WorkflowActivitiesResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        page: build_page_info(&page, total, activities.len()),
        activity_count: total,
        activities,
    })
}

async fn load_workflow_bulk_batches(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    pagination: PaginationQuery,
    consistency: ReadConsistency,
) -> Result<WorkflowBulkBatchesResponse> {
    let page = resolve_page(&state.query, &pagination);
    let (total, batches) = match consistency {
        ReadConsistency::Eventual => (
            state
                .store
                .count_bulk_batches_for_run_query_view(tenant_id, instance_id, run_id)
                .await? as usize,
            state
                .store
                .list_bulk_batches_for_run_page_query_view(
                    tenant_id,
                    instance_id,
                    run_id,
                    i64::try_from(page.limit).context("bulk batch page limit exceeds i64")?,
                    i64::try_from(page.offset).context("bulk batch page offset exceeds i64")?,
                )
                .await?,
        ),
        ReadConsistency::Strong => {
            let limit = i64::try_from(page.limit).context("bulk batch page limit exceeds i64")?;
            let offset =
                i64::try_from(page.offset).context("bulk batch page offset exceeds i64")?;
            let durable_total =
                state.store.count_bulk_batches_for_run(tenant_id, instance_id, run_id).await?
                    as usize;
            let mut batches = state
                .store
                .list_bulk_batches_for_run_page(tenant_id, instance_id, run_id, limit, offset)
                .await?;
            let (total, discovery_batches) = if batches.is_empty() {
                (
                    state
                        .store
                        .count_bulk_batches_for_run_query_view(tenant_id, instance_id, run_id)
                        .await? as usize,
                    state
                        .store
                        .list_bulk_batches_for_run_page_query_view(
                            tenant_id,
                            instance_id,
                            run_id,
                            limit,
                            offset,
                        )
                        .await?,
                )
            } else {
                (durable_total, batches.clone())
            };
            if batches.is_empty() {
                batches = discovery_batches;
            }
            for batch in &mut batches {
                if batch.throughput_backend == ThroughputBackend::StreamV2.as_str() {
                    *batch = fetch_strong_stream_batch(
                        state,
                        tenant_id,
                        instance_id,
                        run_id,
                        &batch.batch_id,
                    )
                    .await?;
                }
            }
            (total, batches)
        }
    };
    let authoritative_source = if consistency == ReadConsistency::Strong {
        batches
            .iter()
            .find(|batch| batch.throughput_backend == ThroughputBackend::StreamV2.as_str())
            .map(|batch| authoritative_bulk_source(&batch.throughput_backend))
            .unwrap_or("workflow-store")
    } else {
        batches
            .iter()
            .find(|batch| batch.throughput_backend == ThroughputBackend::StreamV2.as_str())
            .map(|batch| projected_bulk_source(&batch.throughput_backend))
            .unwrap_or("workflow-store")
    };
    let mut bridge_statuses = Vec::new();
    for batch in &batches {
        if let Some(bridge_status) = load_bulk_bridge_status(
            state,
            tenant_id,
            instance_id,
            run_id,
            &batch.batch_id,
            &batch.throughput_backend,
        )
        .await?
        {
            bridge_statuses.push(bridge_status);
        }
    }
    Ok(WorkflowBulkBatchesResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        consistency: consistency.as_str(),
        authoritative_source,
        projection_lag_ms: (consistency == ReadConsistency::Eventual)
            .then(|| {
                projection_lag_ms_from_times(batches.iter().map(|batch| Some(batch.updated_at)))
            })
            .flatten(),
        watch_cursor: batches
            .iter()
            .map(|batch| batch.updated_at)
            .max()
            .unwrap_or_else(Utc::now)
            .to_rfc3339(),
        page: build_page_info(&page, total, batches.len()),
        batch_count: total,
        batches,
        bridge_statuses,
    })
}

async fn load_workflow_bulk_batch(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
    consistency: ReadConsistency,
) -> Result<WorkflowBulkBatchResponse> {
    let batch = match consistency {
        ReadConsistency::Eventual => {
            state.store.get_bulk_batch_query_view(tenant_id, instance_id, run_id, batch_id).await?
        }
        ReadConsistency::Strong => {
            let batch =
                load_bulk_batch_for_strong_read(state, tenant_id, instance_id, run_id, batch_id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("bulk batch {batch_id} not found"))?;
            if batch.throughput_backend == ThroughputBackend::StreamV2.as_str() {
                Some(
                    fetch_strong_stream_batch(state, tenant_id, instance_id, run_id, batch_id)
                        .await?,
                )
            } else {
                Some(batch)
            }
        }
    }
    .ok_or_else(|| anyhow::anyhow!("bulk batch {batch_id} not found"))?;
    let runtime_control = state
        .store
        .get_bulk_batch_runtime_control(tenant_id, instance_id, run_id, batch_id)
        .await?;
    let bridge_status = load_bulk_bridge_status(
        state,
        tenant_id,
        instance_id,
        run_id,
        batch_id,
        &batch.throughput_backend,
    )
    .await?;
    Ok(WorkflowBulkBatchResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        consistency: consistency.as_str(),
        authoritative_source: if consistency == ReadConsistency::Strong {
            authoritative_bulk_source(&batch.throughput_backend)
        } else {
            projected_bulk_source(&batch.throughput_backend)
        },
        projection_lag_ms: (consistency == ReadConsistency::Eventual)
            .then(|| projection_lag_ms_from_times([Some(batch.updated_at)]))
            .flatten(),
        watch_cursor: batch.updated_at.to_rfc3339(),
        runtime_control,
        batch,
        bridge_status,
    })
}

async fn load_workflow_bulk_chunks(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
    pagination: PaginationQuery,
    consistency: ReadConsistency,
) -> Result<WorkflowBulkChunksResponse> {
    let batch = match consistency {
        ReadConsistency::Eventual => {
            state.store.get_bulk_batch_query_view(tenant_id, instance_id, run_id, batch_id).await?
        }
        ReadConsistency::Strong => {
            let batch =
                load_bulk_batch_for_strong_read(state, tenant_id, instance_id, run_id, batch_id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("bulk batch {batch_id} not found"))?;
            if batch.throughput_backend == ThroughputBackend::StreamV2.as_str() {
                Some(
                    fetch_strong_stream_batch(state, tenant_id, instance_id, run_id, batch_id)
                        .await?,
                )
            } else {
                Some(batch)
            }
        }
    }
    .ok_or_else(|| anyhow::anyhow!("bulk batch {batch_id} not found"))?;
    let page = resolve_page(&state.query, &pagination);
    let total = match consistency {
        ReadConsistency::Eventual => {
            state
                .store
                .count_bulk_chunks_for_batch_query_view(tenant_id, instance_id, run_id, batch_id)
                .await? as usize
        }
        ReadConsistency::Strong => {
            if batch.throughput_backend == ThroughputBackend::StreamV2.as_str() {
                state
                    .store
                    .count_bulk_chunks_for_batch_query_view(
                        tenant_id,
                        instance_id,
                        run_id,
                        batch_id,
                    )
                    .await? as usize
            } else {
                state
                    .store
                    .count_bulk_chunks_for_batch(tenant_id, instance_id, run_id, batch_id)
                    .await? as usize
            }
        }
    };
    let chunks = match consistency {
        ReadConsistency::Eventual => {
            state
                .store
                .list_bulk_chunks_for_batch_page_query_view(
                    tenant_id,
                    instance_id,
                    run_id,
                    batch_id,
                    i64::try_from(page.limit).context("bulk chunk page limit exceeds i64")?,
                    i64::try_from(page.offset).context("bulk chunk page offset exceeds i64")?,
                )
                .await?
        }
        ReadConsistency::Strong => {
            if batch.throughput_backend == ThroughputBackend::StreamV2.as_str() {
                fetch_strong_stream_chunks(
                    state,
                    tenant_id,
                    instance_id,
                    run_id,
                    batch_id,
                    page.limit,
                    page.offset,
                )
                .await?
            } else {
                state
                    .store
                    .list_bulk_chunks_for_batch_page(
                        tenant_id,
                        instance_id,
                        run_id,
                        batch_id,
                        i64::try_from(page.limit).context("bulk chunk page limit exceeds i64")?,
                        i64::try_from(page.offset).context("bulk chunk page offset exceeds i64")?,
                    )
                    .await?
            }
        }
    };
    let mut resolved_chunks = Vec::new();
    for chunk in chunks {
        resolved_chunks.push(resolve_chunk_payloads(state, chunk, true, false).await?);
    }
    let bridge_status = load_bulk_bridge_status(
        state,
        tenant_id,
        instance_id,
        run_id,
        batch_id,
        &batch.throughput_backend,
    )
    .await?;
    Ok(WorkflowBulkChunksResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        batch_id: batch_id.to_owned(),
        consistency: consistency.as_str(),
        authoritative_source: if consistency == ReadConsistency::Strong {
            authoritative_bulk_source(&batch.throughput_backend)
        } else {
            projected_bulk_source(&batch.throughput_backend)
        },
        projection_lag_ms: (consistency == ReadConsistency::Eventual)
            .then(|| {
                projection_lag_ms_from_times(
                    resolved_chunks.iter().map(|chunk| Some(chunk.updated_at)),
                )
            })
            .flatten(),
        watch_cursor: resolved_chunks
            .iter()
            .map(|chunk| chunk.updated_at)
            .max()
            .unwrap_or(batch.updated_at)
            .to_rfc3339(),
        page: build_page_info(&page, total, resolved_chunks.len()),
        chunk_count: total,
        chunks: resolved_chunks,
        bridge_status,
    })
}

async fn load_workflow_bulk_results(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
    pagination: PaginationQuery,
    consistency: ReadConsistency,
) -> Result<WorkflowBulkResultsResponse> {
    let batch =
        load_workflow_bulk_batch(state, tenant_id, instance_id, run_id, batch_id, consistency)
            .await?
            .batch;
    let page = resolve_page(&state.query, &pagination);
    let total = match consistency {
        ReadConsistency::Eventual => {
            state
                .store
                .count_bulk_chunks_for_batch_query_view(tenant_id, instance_id, run_id, batch_id)
                .await? as usize
        }
        ReadConsistency::Strong => {
            if batch.throughput_backend == ThroughputBackend::StreamV2.as_str() {
                state
                    .store
                    .count_bulk_chunks_for_batch_query_view(
                        tenant_id,
                        instance_id,
                        run_id,
                        batch_id,
                    )
                    .await? as usize
            } else {
                state
                    .store
                    .count_bulk_chunks_for_batch(tenant_id, instance_id, run_id, batch_id)
                    .await? as usize
            }
        }
    };
    let chunks = match consistency {
        ReadConsistency::Eventual => {
            state
                .store
                .list_bulk_chunks_for_batch_page_query_view(
                    tenant_id,
                    instance_id,
                    run_id,
                    batch_id,
                    i64::try_from(page.limit).context("bulk result page limit exceeds i64")?,
                    i64::try_from(page.offset).context("bulk result page offset exceeds i64")?,
                )
                .await?
        }
        ReadConsistency::Strong => {
            if batch.throughput_backend == ThroughputBackend::StreamV2.as_str() {
                fetch_strong_stream_chunks(
                    state,
                    tenant_id,
                    instance_id,
                    run_id,
                    batch_id,
                    page.limit,
                    page.offset,
                )
                .await?
            } else {
                state
                    .store
                    .list_bulk_chunks_for_batch_page(
                        tenant_id,
                        instance_id,
                        run_id,
                        batch_id,
                        i64::try_from(page.limit).context("bulk result page limit exceeds i64")?,
                        i64::try_from(page.offset)
                            .context("bulk result page offset exceeds i64")?,
                    )
                    .await?
            }
        }
    };
    let resolved_chunks = if batch.throughput_backend == ThroughputBackend::StreamV2.as_str()
        && consistency == ReadConsistency::Eventual
        && batch.reducer.as_deref() == Some("collect_results")
        && batch.status == WorkflowBulkBatchStatus::Completed
    {
        resolve_collect_results_page_from_batch_payload(state, &batch, chunks).await?
    } else {
        let mut resolved_chunks = Vec::new();
        for chunk in chunks {
            resolved_chunks.push(resolve_chunk_payloads(state, chunk, false, true).await?);
        }
        resolved_chunks
    };
    let chunks =
        resolved_chunks.into_iter().filter(|chunk| chunk.output.is_some()).collect::<Vec<_>>();
    let bridge_status = load_bulk_bridge_status(
        state,
        tenant_id,
        instance_id,
        run_id,
        batch_id,
        &batch.throughput_backend,
    )
    .await?;
    Ok(WorkflowBulkResultsResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        batch_id: batch_id.to_owned(),
        consistency: consistency.as_str(),
        authoritative_source: if consistency == ReadConsistency::Strong {
            authoritative_bulk_source(&batch.throughput_backend)
        } else {
            projected_bulk_source(&batch.throughput_backend)
        },
        projection_lag_ms: (consistency == ReadConsistency::Eventual)
            .then(|| {
                projection_lag_ms_from_times(chunks.iter().map(|chunk| Some(chunk.updated_at)))
            })
            .flatten(),
        watch_cursor: chunks
            .iter()
            .map(|chunk| chunk.updated_at)
            .max()
            .unwrap_or(batch.updated_at)
            .to_rfc3339(),
        page: build_page_info(&page, total, chunks.len()),
        chunk_count: total,
        chunks,
        bridge_status,
    })
}

async fn resolve_collect_results_page_from_batch_payload(
    state: &AppState,
    batch: &WorkflowBulkBatchRecord,
    chunks: Vec<WorkflowBulkChunkRecord>,
) -> Result<Vec<WorkflowBulkChunkRecord>> {
    let handle = serde_json::from_value::<PayloadHandle>(batch.result_handle.clone())
        .context("failed to decode collect_results batch result handle")?;
    let value = state
        .payload_store
        .read_value(&handle)
        .await
        .context("failed to read collect_results batch result payload")?;
    if let Ok(manifest) = serde_json::from_value::<CollectResultsBatchManifest>(value.clone()) {
        let handles = manifest
            .chunks
            .into_iter()
            .map(|chunk| (chunk.chunk_id, chunk.result_handle))
            .collect::<BTreeMap<_, _>>();
        let mut resolved = Vec::with_capacity(chunks.len());
        for mut chunk in chunks {
            if let Some(handle) = handles.get(&chunk.chunk_id) {
                let value = state
                    .payload_store
                    .read_value(handle)
                    .await
                    .context("failed to read collect_results segment payload")?;
                chunk.output = Some(match value {
                    Value::Array(items) => items,
                    _ => Vec::new(),
                });
            }
            resolved.push(chunk);
        }
        return Ok(resolved);
    }
    let items = match value {
        Value::Array(items) => items,
        _ => Vec::new(),
    };
    let chunk_size = batch.chunk_size.max(1);
    let mut resolved = Vec::with_capacity(chunks.len());
    for mut chunk in chunks {
        chunk.output = Some(slice_collect_results_chunk_output(
            &items,
            chunk.chunk_index,
            chunk_size,
            chunk.item_count,
        )?);
        resolved.push(chunk);
    }
    Ok(resolved)
}

fn slice_collect_results_chunk_output(
    items: &[Value],
    chunk_index: u32,
    chunk_size: u32,
    item_count: u32,
) -> Result<Vec<Value>> {
    let chunk_size = usize::try_from(chunk_size.max(1)).context("chunk size exceeds usize")?;
    let start = usize::try_from(chunk_index)
        .context("chunk index exceeds usize")?
        .saturating_mul(chunk_size);
    let len = usize::try_from(item_count).context("chunk item count exceeds usize")?;
    let end = start.saturating_add(len).min(items.len());
    if start > items.len() {
        return Ok(Vec::new());
    }
    Ok(items[start..end].to_vec())
}

async fn resolve_chunk_payloads(
    state: &AppState,
    mut chunk: WorkflowBulkChunkRecord,
    resolve_input: bool,
    resolve_output: bool,
) -> Result<WorkflowBulkChunkRecord> {
    if resolve_input && chunk.items.is_empty() && !chunk.input_handle.is_null() {
        if let Ok(handle) = serde_json::from_value::<PayloadHandle>(chunk.input_handle.clone()) {
            chunk.items = state.payload_store.read_items(&handle).await?;
        }
    }
    if resolve_output && chunk.output.is_none() && !chunk.result_handle.is_null() {
        if let Ok(handle) = serde_json::from_value::<PayloadHandle>(chunk.result_handle.clone()) {
            let value = state.payload_store.read_value(&handle).await?;
            if let Value::Array(items) = value {
                chunk.output = Some(items);
            }
        }
    }
    Ok(chunk)
}

async fn fetch_strong_stream_batch(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
) -> Result<WorkflowBulkBatchRecord> {
    let endpoint = throughput_owner_query_endpoint(state, batch_id).await?;
    let response = state
        .client
        .get(format!(
            "{endpoint}/debug/streams-runtime/batches/{tenant_id}/{instance_id}/{run_id}/{batch_id}"
        ))
        .send()
        .await
        .context("failed to fetch strong throughput batch")?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        anyhow::bail!("bulk batch {batch_id} not found");
    }
    let response = response.error_for_status().context("strong throughput batch request failed")?;
    Ok(response
        .json::<StrongThroughputBatchResponse>()
        .await
        .context("failed to decode strong throughput batch response")?
        .batch)
}

async fn fetch_strong_stream_job_view(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    job_id: &str,
    view_name: &str,
    logical_key: &str,
) -> Result<StrongStreamJobViewResponse> {
    let endpoint = stream_job_owner_query_endpoint(state, job_id).await?;
    let response = state
        .client
        .get(format!(
            "{endpoint}/debug/streams-runtime/stream-jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}/views/{view_name}/keys/{logical_key}"
        ))
        .send()
        .await
        .context("failed to fetch strong stream job view")?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        anyhow::bail!("stream job {job_id} view {view_name} key {logical_key} not found");
    }
    response
        .error_for_status()
        .context("strong stream job view request failed")?
        .json::<StrongStreamJobViewResponse>()
        .await
        .context("failed to decode strong stream job view response")
}

async fn fetch_strong_stream_job_view_keys(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    job_id: &str,
    view_name: &str,
    logical_key_prefix: Option<&str>,
    limit: usize,
    offset: usize,
) -> Result<StrongStreamJobViewKeysResponse> {
    let endpoint = stream_job_owner_query_endpoint(state, job_id).await?;
    let mut query =
        vec![("limit".to_owned(), limit.to_string()), ("offset".to_owned(), offset.to_string())];
    if let Some(prefix) = logical_key_prefix {
        query.push(("prefix".to_owned(), prefix.to_owned()));
    }
    let response = state
        .client
        .get(format!(
            "{endpoint}/debug/streams-runtime/stream-jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}/views/{view_name}/keys"
        ))
        .query(&query)
        .send()
        .await
        .context("failed to fetch strong stream job view keys")?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        anyhow::bail!("stream job {job_id} view {view_name} not found");
    }
    response
        .error_for_status()
        .context("strong stream job view keys request failed")?
        .json::<StrongStreamJobViewKeysResponse>()
        .await
        .context("failed to decode strong stream job view keys response")
}

async fn fetch_strong_stream_job_view_entries(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    job_id: &str,
    view_name: &str,
    logical_key_prefix: Option<&str>,
    limit: usize,
    offset: usize,
) -> Result<StrongStreamJobViewEntriesResponse> {
    let endpoint = stream_job_owner_query_endpoint(state, job_id).await?;
    let mut query =
        vec![("limit".to_owned(), limit.to_string()), ("offset".to_owned(), offset.to_string())];
    if let Some(prefix) = logical_key_prefix {
        query.push(("prefix".to_owned(), prefix.to_owned()));
    }
    let response = state
        .client
        .get(format!(
            "{endpoint}/debug/streams-runtime/stream-jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}/views/{view_name}/entries"
        ))
        .query(&query)
        .send()
        .await
        .context("failed to fetch strong stream job view entries")?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        anyhow::bail!("stream job {job_id} view {view_name} not found");
    }
    response
        .error_for_status()
        .context("strong stream job view entries request failed")?
        .json::<StrongStreamJobViewEntriesResponse>()
        .await
        .context("failed to decode strong stream job view entries response")
}

async fn fetch_stream_job_view_scan(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    job_id: &str,
    view_name: &str,
    consistency: &str,
    logical_key_prefix: &str,
    limit: usize,
    offset: usize,
) -> Result<Value> {
    let endpoint = stream_job_owner_query_endpoint(state, job_id).await?;
    let response = state
        .client
        .get(format!(
            "{endpoint}/debug/streams-runtime/stream-jobs/{tenant_id}/{instance_id}/{run_id}/{job_id}/views/{view_name}/scan"
        ))
        .query(&[
            ("consistency", consistency),
            ("prefix", logical_key_prefix),
            ("limit", &limit.to_string()),
            ("offset", &offset.to_string()),
        ])
        .send()
        .await
        .context("failed to fetch stream job view scan")?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        anyhow::bail!("stream job {job_id} view {view_name} not found");
    }
    response
        .error_for_status()
        .context("stream job view scan request failed")?
        .json::<Value>()
        .await
        .context("failed to decode stream job view scan response")
}

async fn load_eventual_stream_job_view(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    job_id: &str,
    view_name: &str,
    logical_key: &str,
) -> Result<StrongStreamJobViewResponse> {
    let view = state
        .store
        .get_stream_job_view_query(tenant_id, instance_id, run_id, job_id, view_name, logical_key)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("stream job {job_id} view {view_name} key {logical_key} not found")
        })?;
    Ok(StrongStreamJobViewResponse {
        handle_id: view.handle_id,
        job_id: view.job_id,
        view_name: view.view_name,
        logical_key: view.logical_key,
        output: view.output,
        checkpoint_sequence: view.checkpoint_sequence,
        updated_at: view.updated_at,
    })
}

async fn load_eventual_stream_job_view_keys(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    job_id: &str,
    view_name: &str,
    logical_key_prefix: Option<&str>,
    limit: usize,
    offset: usize,
) -> Result<(usize, Vec<StreamJobViewKeyRecord>)> {
    let total = state
        .store
        .count_stream_job_view_query_keys(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            view_name,
            logical_key_prefix,
        )
        .await? as usize;
    let views = state
        .store
        .list_stream_job_view_query_page(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            view_name,
            logical_key_prefix,
            i64::try_from(limit).context("stream job view page limit exceeds i64")?,
            i64::try_from(offset).context("stream job view page offset exceeds i64")?,
        )
        .await?;
    let keys = views
        .into_iter()
        .map(|view| StreamJobViewKeyRecord {
            logical_key: view.logical_key,
            checkpoint_sequence: view.checkpoint_sequence,
            updated_at: view.updated_at,
        })
        .collect();
    Ok((total, keys))
}

async fn load_eventual_stream_job_view_entries(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    job_id: &str,
    view_name: &str,
    logical_key_prefix: Option<&str>,
    limit: usize,
    offset: usize,
) -> Result<(usize, Vec<StreamJobViewEntryRecord>)> {
    let total = state
        .store
        .count_stream_job_view_query_keys(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            view_name,
            logical_key_prefix,
        )
        .await? as usize;
    let views = state
        .store
        .list_stream_job_view_query_page(
            tenant_id,
            instance_id,
            run_id,
            job_id,
            view_name,
            logical_key_prefix,
            i64::try_from(limit).context("stream job view page limit exceeds i64")?,
            i64::try_from(offset).context("stream job view page offset exceeds i64")?,
        )
        .await?;
    let entries = views
        .into_iter()
        .map(|view| StreamJobViewEntryRecord {
            logical_key: view.logical_key,
            output: view.output,
            checkpoint_sequence: view.checkpoint_sequence,
            updated_at: view.updated_at,
        })
        .collect();
    Ok((total, entries))
}

async fn load_bulk_batch_for_strong_read(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
) -> Result<Option<WorkflowBulkBatchRecord>> {
    if let Some(batch) =
        state.store.get_bulk_batch(tenant_id, instance_id, run_id, batch_id).await?
    {
        return Ok(Some(batch));
    }
    state.store.get_bulk_batch_query_view(tenant_id, instance_id, run_id, batch_id).await
}

async fn fetch_strong_stream_chunks(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
    limit: usize,
    offset: usize,
) -> Result<Vec<WorkflowBulkChunkRecord>> {
    let endpoint = throughput_owner_query_endpoint(state, batch_id).await?;
    let response = state
        .client
        .get(format!(
            "{endpoint}/debug/streams-runtime/batches/{tenant_id}/{instance_id}/{run_id}/{batch_id}/chunks"
        ))
        .query(&[("limit", limit), ("offset", offset)])
        .send()
        .await
        .context("failed to fetch strong throughput chunks")?;
    let response =
        response.error_for_status().context("strong throughput chunks request failed")?;
    Ok(response
        .json::<StrongThroughputChunksResponse>()
        .await
        .context("failed to decode strong throughput chunks response")?
        .chunks)
}

async fn throughput_owner_query_endpoint(state: &AppState, batch_id: &str) -> Result<String> {
    let partition_id = partition_for_key(
        &throughput_partition_key(batch_id, 0),
        state.throughput_partitions.max(1),
    );
    let member = state
        .store
        .get_active_throughput_member_for_partition(partition_id, Utc::now())
        .await?
        .ok_or_else(|| anyhow::anyhow!("no active throughput owner for batch {batch_id}"))?;
    Ok(member.query_endpoint.trim_end_matches('/').to_owned())
}

async fn stream_job_owner_query_endpoint(state: &AppState, job_id: &str) -> Result<String> {
    let partition_id =
        partition_for_key(&throughput_partition_key(job_id, 0), state.throughput_partitions.max(1));
    let member = state
        .store
        .get_active_throughput_member_for_partition(partition_id, Utc::now())
        .await?
        .ok_or_else(|| anyhow::anyhow!("no active throughput owner for stream job {job_id}"))?;
    Ok(member.query_endpoint.trim_end_matches('/').to_owned())
}

#[derive(Debug, Deserialize)]
struct StrongThroughputBatchResponse {
    batch: WorkflowBulkBatchRecord,
}

#[derive(Debug, Deserialize)]
struct StrongThroughputChunksResponse {
    chunks: Vec<WorkflowBulkChunkRecord>,
}

fn build_payload_store_config(query: &QueryRuntimeConfig) -> PayloadStoreConfig {
    PayloadStoreConfig {
        default_store: match query.throughput_payload_store.kind {
            fabrik_config::ThroughputPayloadStoreKind::LocalFilesystem => {
                PayloadStoreKind::LocalFilesystem
            }
            fabrik_config::ThroughputPayloadStoreKind::S3 => PayloadStoreKind::S3,
        },
        local_dir: query.throughput_payload_store.local_dir.clone(),
        s3_bucket: query.throughput_payload_store.s3_bucket.clone(),
        s3_region: query.throughput_payload_store.s3_region.clone(),
        s3_endpoint: query.throughput_payload_store.s3_endpoint.clone(),
        s3_access_key_id: query.throughput_payload_store.s3_access_key_id.clone(),
        s3_secret_access_key: query.throughput_payload_store.s3_secret_access_key.clone(),
        s3_force_path_style: query.throughput_payload_store.s3_force_path_style,
        s3_key_prefix: query.throughput_payload_store.s3_key_prefix.clone(),
    }
}

async fn load_workflow_signals(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    pagination: PaginationQuery,
) -> Result<WorkflowSignalsResponse> {
    let page = resolve_page(&state.query, &pagination);
    let total = state.store.count_signals_for_run(tenant_id, instance_id, run_id).await? as usize;
    let signals = state
        .store
        .list_signals_for_run_page(
            tenant_id,
            instance_id,
            run_id,
            i64::try_from(page.limit).context("signals page limit exceeds i64")?,
            i64::try_from(page.offset).context("signals page offset exceeds i64")?,
        )
        .await?;
    Ok(WorkflowSignalsResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        page: build_page_info(&page, total, signals.len()),
        signal_count: total,
        signals,
    })
}

async fn load_workflow_updates(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    pagination: PaginationQuery,
) -> Result<WorkflowUpdatesResponse> {
    let page = resolve_page(&state.query, &pagination);
    let total = state.store.count_updates_for_run(tenant_id, instance_id, run_id).await? as usize;
    let updates = state
        .store
        .list_updates_for_run_page(
            tenant_id,
            instance_id,
            run_id,
            i64::try_from(page.limit).context("updates page limit exceeds i64")?,
            i64::try_from(page.offset).context("updates page offset exceeds i64")?,
        )
        .await?;
    Ok(WorkflowUpdatesResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        page: build_page_info(&page, total, updates.len()),
        update_count: total,
        updates,
    })
}

async fn load_workflow_update(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    update_id: &str,
) -> Result<WorkflowUpdateResponse, (StatusCode, Json<ErrorResponse>)> {
    let update = state
        .store
        .get_update(tenant_id, instance_id, run_id, update_id)
        .await
        .map_err(internal_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    message: format!(
                        "workflow update {update_id} not found for instance {instance_id} run {run_id}"
                    ),
                }),
            )
        })?;
    Ok(WorkflowUpdateResponse {
        tenant_id: tenant_id.to_owned(),
        instance_id: instance_id.to_owned(),
        run_id: run_id.to_owned(),
        update,
    })
}

async fn replay_workflow_run(
    state: &AppState,
    current_instance: &WorkflowInstanceState,
    run_id: &str,
) -> Result<WorkflowReplayResponse> {
    let history =
        read_history(state, &current_instance.tenant_id, &current_instance.instance_id, run_id)
            .await?;
    if history.is_empty() {
        anyhow::bail!(
            "no workflow history found for tenant={}, instance_id={}, run_id={run_id}",
            current_instance.tenant_id,
            current_instance.instance_id
        );
    }
    let first_event = history
        .first()
        .ok_or_else(|| anyhow::anyhow!("workflow history unexpectedly empty after read"))?;
    let run = state
        .store
        .get_run_record(&current_instance.tenant_id, &current_instance.instance_id, run_id)
        .await?;

    let pinned_artifact = {
        let version = first_event.definition_version;
        state
            .store
            .get_artifact_version(&current_instance.tenant_id, &first_event.definition_id, version)
            .await?
    };

    let full_trace = if let Some(artifact) = pinned_artifact.as_ref() {
        replay_compiled_history_trace(&history, artifact)?
    } else {
        replay_history_trace(&history)?
    };
    let definition_version = full_trace
        .final_state
        .definition_version
        .ok_or_else(|| anyhow::anyhow!("replayed state is missing definition_version"))?;
    let pinned_artifact_hash = full_trace
        .final_state
        .artifact_hash
        .clone()
        .ok_or_else(|| anyhow::anyhow!("replayed state is missing artifact_hash"))?;
    if let Some(artifact) = state
        .store
        .get_artifact_version(
            &current_instance.tenant_id,
            &full_trace.final_state.definition_id,
            definition_version,
        )
        .await?
    {
        if artifact.artifact_hash != pinned_artifact_hash {
            anyhow::bail!(
                "artifact hash mismatch for replayed run: history={}, artifact={}",
                pinned_artifact_hash,
                artifact.artifact_hash
            );
        }
    } else {
        let definition = state
            .store
            .get_definition_version(
                &current_instance.tenant_id,
                &full_trace.final_state.definition_id,
                definition_version,
            )
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "workflow definition {} version {} not found for tenant {}",
                    full_trace.final_state.definition_id,
                    definition_version,
                    current_instance.tenant_id
                )
            })?;
        let computed_artifact_hash = artifact_hash(&definition);
        if computed_artifact_hash != pinned_artifact_hash {
            anyhow::bail!(
                "artifact hash mismatch for replayed run: history={}, definition={}",
                pinned_artifact_hash,
                computed_artifact_hash
            );
        }
    }

    let snapshot = state
        .store
        .get_snapshot_for_run(&current_instance.tenant_id, &current_instance.instance_id, run_id)
        .await?;
    let snapshot_summary = snapshot.as_ref().map(snapshot_summary);
    let mut divergences = Vec::new();

    let active_trace = if let Some(snapshot) = snapshot {
        match history.iter().position(|event| event.event_id == snapshot.last_event_id) {
            Some(index) => {
                let tail = &history[index + 1..];
                let trace = if let Some(artifact) = pinned_artifact.as_ref() {
                    replay_compiled_history_trace_from_snapshot(
                        tail,
                        &snapshot.state,
                        artifact,
                        snapshot.event_count,
                        snapshot.last_event_id,
                        &snapshot.last_event_type,
                    )?
                } else {
                    replay_history_trace_from_snapshot(
                        tail,
                        &snapshot.state,
                        snapshot.event_count,
                        snapshot.last_event_id,
                        &snapshot.last_event_type,
                    )?
                };
                let expected_tail =
                    full_trace.transitions.iter().skip(index + 1).cloned().collect::<Vec<_>>();
                if let Some(divergence) =
                    first_transition_divergence(&expected_tail, &trace.transitions)
                {
                    divergences.push(divergence);
                } else if !same_projection(&full_trace.final_state, &trace.final_state) {
                    divergences.push(ReplayDivergence {
                        kind: ReplayDivergenceKind::SnapshotMismatch,
                        event_id: history.last().map(|event| event.event_id),
                        event_type: history.last().map(|event| event.event_type.clone()),
                        message: "snapshot-backed replay produced a different final state"
                            .to_owned(),
                        fields: projection_mismatches(&full_trace.final_state, &trace.final_state),
                    });
                }
                trace
            }
            None => {
                divergences.push(ReplayDivergence {
                    kind: ReplayDivergenceKind::SnapshotMismatch,
                    event_id: Some(snapshot.last_event_id),
                    event_type: Some(snapshot.last_event_type.clone()),
                    message: "snapshot boundary event was not found in broker history".to_owned(),
                    fields: vec![ReplayFieldMismatch {
                        field: "snapshot_last_event_id".to_owned(),
                        expected: to_value(snapshot.last_event_id)
                            .expect("snapshot event id serializes"),
                        actual: Value::Null,
                    }],
                });
                full_trace.clone()
            }
        }
    } else {
        full_trace.clone()
    };

    let last_event_type = history
        .last()
        .map(|event| event.event_type.clone())
        .ok_or_else(|| anyhow::anyhow!("workflow history unexpectedly empty after replay"))?;
    let activity_attempts = state
        .store
        .list_activities_for_run(&current_instance.tenant_id, &current_instance.instance_id, run_id)
        .await?;
    let comparison_state = {
        let mut state = active_trace.final_state.clone();
        state.compact_terminal_for_persistence();
        state
    };
    let mut projection_matches_store = (current_instance.run_id == comparison_state.run_id)
        .then(|| same_projection(current_instance, &comparison_state));
    if matches!(projection_matches_store, Some(false)) {
        divergences.push(ReplayDivergence {
            kind: ReplayDivergenceKind::ProjectionMismatch,
            event_id: Some(active_trace.final_state.last_event_id),
            event_type: Some(active_trace.final_state.last_event_type.clone()),
            message: "replayed final state does not match the stored workflow projection"
                .to_owned(),
            fields: projection_mismatches(current_instance, &comparison_state),
        });
    }
    if projection_matches_store.is_some() && !divergences.is_empty() {
        projection_matches_store = Some(false);
    }

    Ok(WorkflowReplayResponse {
        tenant_id: current_instance.tenant_id.clone(),
        instance_id: current_instance.instance_id.clone(),
        run_id: run_id.to_owned(),
        definition_id: active_trace.final_state.definition_id.clone(),
        definition_version,
        artifact_hash: pinned_artifact_hash,
        previous_run_id: run.as_ref().and_then(|run| run.previous_run_id.clone()),
        next_run_id: run.as_ref().and_then(|run| run.next_run_id.clone()),
        continue_reason: run.and_then(|run| run.continue_reason),
        event_count: history.len(),
        last_event_type,
        activity_attempt_count: activity_attempts.len(),
        activity_attempts,
        projection_matches_store,
        replay_source: active_trace.source.clone(),
        snapshot: snapshot_summary,
        divergence_count: divergences.len(),
        divergences,
        transition_count: active_trace.transitions.len(),
        transition_trace: active_trace.transitions,
        replayed_state: active_trace.final_state,
    })
}

fn snapshot_summary(snapshot: &WorkflowStateSnapshot) -> ReplaySnapshotSummary {
    ReplaySnapshotSummary {
        run_id: snapshot.run_id.clone(),
        snapshot_schema_version: snapshot.snapshot_schema_version,
        event_count: snapshot.event_count,
        last_event_id: snapshot.last_event_id,
        last_event_type: snapshot.last_event_type.clone(),
        updated_at: snapshot.updated_at,
    }
}

async fn read_history(
    state: &AppState,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
) -> Result<Vec<EventEnvelope<WorkflowEvent>>> {
    let history = read_workflow_history(
        &state.broker,
        "query-service",
        &WorkflowHistoryFilter::new(tenant_id, instance_id, run_id),
        Duration::from_millis(HISTORY_IDLE_TIMEOUT_MS),
        Duration::from_millis(HISTORY_MAX_SCAN_MS),
    )
    .await?;
    if !history.is_empty() {
        return Ok(history);
    }
    if let Some(fast_start) =
        state.store.get_workflow_fast_start(tenant_id, instance_id, run_id).await?
    {
        return build_fast_start_history(&fast_start);
    }
    Ok(history)
}

fn build_fast_start_history(
    fast_start: &fabrik_store::WorkflowFastStartRecord,
) -> Result<Vec<EventEnvelope<WorkflowEvent>>> {
    let identity = fabrik_events::WorkflowIdentity::new(
        fast_start.tenant_id.clone(),
        fast_start.definition_id.clone(),
        fast_start.definition_version,
        fast_start.artifact_hash.clone(),
        fast_start.instance_id.clone(),
        fast_start.run_id.clone(),
        "tiny-workflow-engine",
    );
    let mut trigger = EventEnvelope::new(
        WorkflowEvent::WorkflowTriggered { input: fast_start.input.clone() }.event_type(),
        identity.clone(),
        WorkflowEvent::WorkflowTriggered { input: fast_start.input.clone() },
    );
    trigger.event_id = fast_start.trigger_event_id;
    trigger.occurred_at = fast_start.accepted_at;
    trigger
        .metadata
        .insert("workflow_task_queue".to_owned(), fast_start.workflow_task_queue.clone());
    if let Some(memo) = fast_start.memo.as_ref() {
        trigger.metadata.insert(
            "memo_json".to_owned(),
            serde_json::to_string(memo).context("failed to encode fast-start memo")?,
        );
    }
    if let Some(search_attributes) = fast_start.search_attributes.as_ref() {
        trigger.metadata.insert(
            "search_attributes_json".to_owned(),
            serde_json::to_string(search_attributes)
                .context("failed to encode fast-start search attributes")?,
        );
    }
    let mut history = vec![trigger];
    if let (Some(event_id), Some(event_type), Some(payload), Some(completed_at)) = (
        fast_start.terminal_event_id,
        fast_start.terminal_event_type.as_deref(),
        fast_start.terminal_payload.as_ref(),
        fast_start.completed_at,
    ) {
        let terminal_payload = match event_type {
            "WorkflowCompleted" => WorkflowEvent::WorkflowCompleted {
                output: payload.get("output").cloned().unwrap_or(Value::Null),
            },
            "WorkflowFailed" => WorkflowEvent::WorkflowFailed {
                reason: payload
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or("workflow failed")
                    .to_owned(),
            },
            other => anyhow::bail!("unsupported fast-start terminal event type {other}"),
        };
        let mut terminal =
            EventEnvelope::new(terminal_payload.event_type(), identity, terminal_payload);
        terminal.event_id = event_id;
        terminal.occurred_at = completed_at;
        history.push(terminal);
    }
    Ok(history)
}

async fn run_retention_sweeper(
    store: WorkflowStore,
    config: QueryRuntimeConfig,
    debug: Arc<Mutex<RetentionDebugState>>,
) {
    loop {
        let now = Utc::now();
        let cutoffs = QueryRetentionCutoffs {
            run_closed_before: config
                .run_retention_days
                .and_then(|days| chrono::Duration::try_days(days as i64))
                .map(|duration| now - duration),
            activity_run_closed_before: config
                .activity_retention_days
                .and_then(|days| chrono::Duration::try_days(days as i64))
                .map(|duration| now - duration),
            signal_run_closed_before: config
                .signal_retention_days
                .and_then(|days| chrono::Duration::try_days(days as i64))
                .map(|duration| now - duration),
            snapshot_run_closed_before: config
                .snapshot_retention_days
                .and_then(|days| chrono::Duration::try_days(days as i64))
                .map(|duration| now - duration),
        };

        if cutoffs != QueryRetentionCutoffs::default() {
            match store.prune_query_retention(&cutoffs).await {
                Ok(result) => {
                    if let Ok(mut state) = debug.lock() {
                        state.last_sweep_at = Some(now);
                        state.last_result = Some(result);
                    }
                }
                Err(error) => {
                    tracing::error!(error = %error, "failed to sweep query retention policy");
                }
            }
        } else if let Ok(mut state) = debug.lock() {
            state.last_sweep_at = Some(now);
            state.last_result = Some(QueryRetentionPruneResult::default());
        }

        tokio::time::sleep(Duration::from_secs(config.retention_sweep_interval_seconds)).await;
    }
}

async fn get_latest_workflow_definition(
    Path((tenant_id, definition_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowDefinition>, (StatusCode, Json<ErrorResponse>)> {
    match state
        .store
        .get_latest_definition(&tenant_id, &definition_id)
        .await
        .map_err(internal_error)?
    {
        Some(definition) => Ok(Json(definition)),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                message: format!(
                    "workflow definition {definition_id} not found for tenant {tenant_id}"
                ),
            }),
        )),
    }
}

async fn get_workflow_definition_version(
    Path((tenant_id, definition_id, version)): Path<(String, String, u32)>,
    State(state): State<AppState>,
) -> Result<Json<WorkflowDefinition>, (StatusCode, Json<ErrorResponse>)> {
    match state
        .store
        .get_definition_version(&tenant_id, &definition_id, version)
        .await
        .map_err(internal_error)?
    {
        Some(definition) => Ok(Json(definition)),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                message: format!(
                    "workflow definition {definition_id} version {version} not found for tenant {tenant_id}"
                ),
            }),
        )),
    }
}

async fn get_latest_workflow_artifact(
    Path((tenant_id, definition_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<CompiledWorkflowArtifact>, (StatusCode, Json<ErrorResponse>)> {
    match state
        .store
        .get_latest_artifact(&tenant_id, &definition_id)
        .await
        .map_err(internal_error)?
    {
        Some(artifact) => Ok(Json(artifact)),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                message: format!(
                    "workflow artifact {definition_id} not found for tenant {tenant_id}"
                ),
            }),
        )),
    }
}

async fn get_workflow_artifact_version(
    Path((tenant_id, definition_id, version)): Path<(String, String, u32)>,
    State(state): State<AppState>,
) -> Result<Json<CompiledWorkflowArtifact>, (StatusCode, Json<ErrorResponse>)> {
    match state
        .store
        .get_artifact_version(&tenant_id, &definition_id, version)
        .await
        .map_err(internal_error)?
    {
        Some(artifact) => Ok(Json(artifact)),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                message: format!(
                    "workflow artifact {definition_id} version {version} not found for tenant {tenant_id}"
                ),
            }),
        )),
    }
}

async fn get_latest_stream_artifact(
    Path((tenant_id, definition_id)): Path<(String, String)>,
    State(state): State<AppState>,
) -> Result<Json<CompiledStreamJobArtifact>, (StatusCode, Json<ErrorResponse>)> {
    match state
        .store
        .get_latest_stream_artifact(&tenant_id, &definition_id)
        .await
        .map_err(internal_error)?
    {
        Some(artifact) => Ok(Json(artifact)),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                message: format!(
                    "stream artifact {definition_id} not found for tenant {tenant_id}"
                ),
            }),
        )),
    }
}

async fn get_stream_artifact_version(
    Path((tenant_id, definition_id, version)): Path<(String, String, u32)>,
    State(state): State<AppState>,
) -> Result<Json<CompiledStreamJobArtifact>, (StatusCode, Json<ErrorResponse>)> {
    match state
        .store
        .get_stream_artifact_version(&tenant_id, &definition_id, version)
        .await
        .map_err(internal_error)?
    {
        Some(artifact) => Ok(Json(artifact)),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                message: format!(
                    "stream artifact {definition_id} version {version} not found for tenant {tenant_id}"
                ),
            }),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use fabrik_config::{ThroughputPayloadStoreConfig, ThroughputPayloadStoreKind};
    use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
    use fabrik_store::StreamJobViewRecord;
    use fabrik_workflow::{
        ArtifactEntrypoint, CompiledQueryHandler, CompiledUpdateHandler, CompiledWorkflow,
        Expression, SourceLocation, WorkflowStatus,
    };
    use serde_json::json;
    use std::{
        collections::{BTreeMap, BTreeSet},
        net::SocketAddr,
        process::Command,
        sync::{Arc, Mutex},
        time::{Duration as StdDuration, Instant},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        time::sleep,
    };
    use uuid::Uuid;

    #[test]
    fn collect_results_page_slices_outputs_by_chunk_index() {
        let items = vec![json!(1), json!(2), json!(3), json!(4), json!(5)];
        assert_eq!(
            slice_collect_results_chunk_output(&items, 0, 2, 2).unwrap(),
            vec![json!(1), json!(2)]
        );
        assert_eq!(
            slice_collect_results_chunk_output(&items, 1, 2, 2).unwrap(),
            vec![json!(3), json!(4)]
        );
        assert_eq!(slice_collect_results_chunk_output(&items, 2, 2, 1).unwrap(), vec![json!(5)]);
    }

    #[test]
    fn stream_job_checkpoint_summaries_parse_declared_checkpoints() {
        let now = Utc::now();
        let summaries = stream_job_checkpoint_summaries(
            Some(&json!({
                "kind": "named_checkpoints",
                "checkpoints": [
                    {
                        "name": "hourly-rollup-ready",
                        "delivery": "workflow_awaitable",
                        "sequence": 1
                    },
                    {
                        "name": "terminal-summary",
                        "delivery": "materialized",
                        "sequence": 2
                    }
                ]
            })),
            Some("hourly-rollup-ready"),
            Some(1),
            Some(now),
        );
        assert_eq!(summaries.len(), 2);
        assert_eq!(summaries[0].checkpoint_name, "hourly-rollup-ready");
        assert_eq!(summaries[0].delivery.as_deref(), Some("workflow_awaitable"));
        assert_eq!(summaries[0].sequence, Some(1));
        assert!(summaries[0].reached);
        assert_eq!(summaries[0].reached_at, Some(now));
        assert_eq!(summaries[1].checkpoint_name, "terminal-summary");
        assert_eq!(summaries[1].delivery.as_deref(), Some("materialized"));
        assert_eq!(summaries[1].sequence, Some(2));
        assert!(!summaries[1].reached);
        assert_eq!(summaries[1].reached_at, None);
    }

    #[test]
    fn stream_job_view_scan_item_parses_window_metadata() {
        let item = stream_job_view_scan_item_from_value(json!({
            "logicalKey": "acct_1",
            "output": { "totalAmount": 12 },
            "checkpointSequence": 9,
            "updatedAt": "2026-03-15T10:00:00Z",
            "windowExpiredAt": "2026-03-15T10:02:00Z",
            "windowRetentionSeconds": 120
        }))
        .expect("scan item should parse");
        assert_eq!(item.logical_key, "acct_1");
        assert_eq!(item.output["totalAmount"], 12);
        assert_eq!(item.checkpoint_sequence, 9);
        assert_eq!(item.updated_at.to_rfc3339(), "2026-03-15T10:00:00+00:00");
        assert_eq!(
            item.window_expired_at.map(|value| value.to_rfc3339()),
            Some("2026-03-15T10:02:00+00:00".to_owned())
        );
        assert_eq!(item.window_retention_seconds, Some(120));
    }

    #[test]
    fn stream_query_source_label_maps_runtime_sources() {
        assert_eq!(stream_query_source_label("stream_owner_local_state"), "stream-owner");
        assert_eq!(stream_query_source_label("stream_projection_query"), "stream-view-query");
    }

    #[test]
    fn stream_job_surface_summary_rolls_up_checkpoints_and_views() {
        let now = Utc::now();
        let checkpoints = vec![
            StreamJobCheckpointSummary {
                checkpoint_name: "hourly-rollup-ready".to_owned(),
                definition: None,
                delivery: Some("workflow_awaitable".to_owned()),
                sequence: Some(1),
                reached: true,
                reached_at: Some(now),
            },
            StreamJobCheckpointSummary {
                checkpoint_name: "terminal-summary".to_owned(),
                definition: None,
                delivery: Some("materialized".to_owned()),
                sequence: Some(2),
                reached: false,
                reached_at: None,
            },
        ];
        let views = vec![
            StreamJobViewSummary {
                view_name: "accountTotals".to_owned(),
                definition: None,
                projected_key_count: 3,
                latest_projected_checkpoint_sequence: Some(7),
                latest_projected_at: Some(now),
                eventual_projection_lag_ms: Some(25),
            },
            StreamJobViewSummary {
                view_name: "fraudScores".to_owned(),
                definition: None,
                projected_key_count: 0,
                latest_projected_checkpoint_sequence: None,
                latest_projected_at: None,
                eventual_projection_lag_ms: None,
            },
        ];
        let latest_checkpoint_name = "hourly-rollup-ready".to_owned();
        let surface = stream_job_surface_summary(
            &checkpoints,
            &views,
            Some(&latest_checkpoint_name),
            Some(7),
            Some(now),
        );
        assert_eq!(surface.declared_checkpoint_count, 2);
        assert_eq!(surface.reached_checkpoint_count, 1);
        assert_eq!(surface.latest_checkpoint_name.as_deref(), Some("hourly-rollup-ready"));
        assert_eq!(surface.latest_checkpoint_sequence, Some(7));
        assert_eq!(surface.latest_checkpoint_at, Some(now));
        assert_eq!(surface.view_count, 2);
        assert_eq!(surface.projected_view_count, 1);
        assert_eq!(surface.total_projected_keys, 3);
        assert_eq!(surface.slowest_eventual_view_lag_ms, Some(25));
        assert_eq!(surface.checkpoints.len(), 2);
        assert_eq!(surface.views.len(), 2);
    }

    struct TestPostgres {
        container_name: String,
        database_url: String,
    }

    impl TestPostgres {
        fn start() -> Result<Option<Self>> {
            if !docker_available() {
                eprintln!("skipping query-service integration tests because docker is unavailable");
                return Ok(None);
            }

            let container_name = format!("fabrik-query-test-pg-{}", Uuid::now_v7());
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

    async fn test_state(store: WorkflowStore) -> Result<AppState> {
        let temp_dir =
            std::env::temp_dir().join(format!("fabrik-query-payloads-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&temp_dir).context("failed to create test payload store dir")?;
        let query = QueryRuntimeConfig {
            default_page_size: 100,
            max_page_size: 500,
            throughput_payload_store: ThroughputPayloadStoreConfig {
                kind: ThroughputPayloadStoreKind::LocalFilesystem,
                local_dir: temp_dir.display().to_string(),
                s3_bucket: None,
                s3_region: "us-east-1".to_owned(),
                s3_endpoint: None,
                s3_access_key_id: None,
                s3_secret_access_key: None,
                s3_force_path_style: false,
                s3_key_prefix: String::new(),
            },
            strong_query_unified_url: None,
            history_retention_days: None,
            run_retention_days: None,
            activity_retention_days: None,
            signal_retention_days: None,
            snapshot_retention_days: None,
            retention_sweep_interval_seconds: 300,
        };
        let payload_store = PayloadStore::from_config(build_payload_store_config(&query)).await?;
        Ok(AppState {
            store,
            broker: BrokerConfig::new("127.0.0.1:9092", "workflow-events", 1),
            query,
            payload_store,
            client: Client::new(),
            throughput_partitions: 1,
            retention: Arc::new(Mutex::new(RetentionDebugState::default())),
        })
    }

    #[test]
    fn owner_query_endpoints_uses_unified_only_by_default() {
        let config = QueryRuntimeConfig {
            default_page_size: 100,
            max_page_size: 500,
            throughput_payload_store: fabrik_config::ThroughputPayloadStoreConfig {
                kind: fabrik_config::ThroughputPayloadStoreKind::LocalFilesystem,
                local_dir: "/tmp".to_owned(),
                s3_bucket: None,
                s3_region: "us-east-1".to_owned(),
                s3_endpoint: None,
                s3_access_key_id: None,
                s3_secret_access_key: None,
                s3_force_path_style: false,
                s3_key_prefix: String::new(),
            },
            strong_query_unified_url: Some("http://unified".to_owned()),
            history_retention_days: None,
            run_retention_days: None,
            activity_retention_days: None,
            signal_retention_days: None,
            snapshot_retention_days: None,
            retention_sweep_interval_seconds: 300,
        };

        assert_eq!(owner_query_endpoints(&config), vec!["http://unified".to_owned()]);
    }

    #[tokio::test]
    async fn owner_query_endpoint_round_trip_returns_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test listener");
        let addr: SocketAddr = listener.local_addr().expect("read test listener address");
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept test connection");
            let mut buf = vec![0_u8; 4096];
            let _ = socket.read(&mut buf).await.expect("read test request");
            let body = serde_json::to_string(&InternalQueryResponse {
                result: json!({"ok": true}),
                consistency: "strong".to_owned(),
                source: "hot_owner".to_owned(),
            })
            .expect("serialize test response");
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body,
            );
            socket.write_all(response.as_bytes()).await.expect("write test response");
        });

        let response = try_owner_strong_query_endpoint(
            &Client::new(),
            &format!("http://{addr}"),
            "tenant",
            "instance",
            "summary",
            &json!({"ok": true}),
        )
        .await
        .expect("owner query should succeed");

        assert_eq!(response.result, json!({"ok": true}));
        assert_eq!(response.consistency, "strong");
        assert_eq!(response.source, "hot_owner");
    }

    #[test]
    fn bulk_sources_distinguish_bridge_reads_from_projections() {
        assert_eq!(
            authoritative_bulk_source(ThroughputBackend::StreamV2.as_str()),
            "bridge-owner-routed"
        );
        assert_eq!(
            projected_bulk_source(ThroughputBackend::StreamV2.as_str()),
            "stream-projection"
        );
        assert_eq!(authoritative_bulk_source("legacy"), "workflow-store");
        assert_eq!(projected_bulk_source("legacy"), "workflow-store");
    }

    #[tokio::test]
    async fn load_bulk_bridge_status_returns_stream_and_workflow_terminal_state() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state = test_state(store.clone()).await?;
        let now = Utc::now();
        let workflow_event_id = Uuid::now_v7();
        let command_id = Uuid::now_v7();
        let command = fabrik_throughput::ThroughputCommandEnvelope {
            command_id,
            occurred_at: now,
            dedupe_key: "throughput-start:test-query-bridge".to_owned(),
            partition_key: "batch-a:0".to_owned(),
            payload: fabrik_throughput::ThroughputCommand::StartThroughputRun(
                fabrik_throughput::StartThroughputRunCommand {
                    dedupe_key: "throughput-start:test-query-bridge".to_owned(),
                    bridge_request_id: fabrik_throughput::throughput_bridge_request_id(
                        "tenant-a",
                        "instance-a",
                        "run-a",
                        "batch-a",
                    ),
                    bridge_protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION
                        .to_owned(),
                    bridge_operation_kind:
                        fabrik_throughput::ThroughputBridgeOperationKind::BulkRun
                            .as_str()
                            .to_owned(),
                    tenant_id: "tenant-a".to_owned(),
                    definition_id: "demo".to_owned(),
                    definition_version: Some(1),
                    artifact_hash: Some("artifact-a".to_owned()),
                    instance_id: "instance-a".to_owned(),
                    run_id: "run-a".to_owned(),
                    batch_id: "batch-a".to_owned(),
                    activity_type: "benchmark.echo".to_owned(),
                    activity_capabilities: None,
                    task_queue: "bulk".to_owned(),
                    state: Some("join".to_owned()),
                    chunk_size: 2,
                    max_attempts: 1,
                    retry_delay_ms: 0,
                    total_items: 2,
                    aggregation_group_count: 1,
                    execution_policy: Some("parallel".to_owned()),
                    reducer: Some("count".to_owned()),
                    throughput_backend: ThroughputBackend::StreamV2.as_str().to_owned(),
                    throughput_backend_version: ThroughputBackend::StreamV2
                        .default_version()
                        .to_owned(),
                    routing_reason: "stream_v2_selected".to_owned(),
                    admission_policy_version: fabrik_throughput::ADMISSION_POLICY_VERSION
                        .to_owned(),
                    input_handle: fabrik_throughput::PayloadHandle::Inline {
                        key: "bulk-input:batch-a".to_owned(),
                    },
                    result_handle: fabrik_throughput::PayloadHandle::Inline {
                        key: "bulk-result:batch-a".to_owned(),
                    },
                },
            ),
        };
        store
            .upsert_throughput_bridge_submission(
                workflow_event_id,
                fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION,
                fabrik_throughput::ThroughputBridgeOperationKind::BulkRun,
                "tenant-a",
                "instance-a",
                "run-a",
                "batch-a",
                ThroughputBackend::StreamV2.as_str(),
                &fabrik_throughput::throughput_bridge_request_id(
                    "tenant-a",
                    "instance-a",
                    "run-a",
                    "batch-a",
                ),
                &command.dedupe_key,
                Some(command.command_id),
                Some(&command.partition_key),
                now,
            )
            .await?;
        store.mark_throughput_bridge_command_published(workflow_event_id, now).await?;
        store
            .upsert_throughput_run(&fabrik_store::ThroughputRunRecord {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-a".to_owned(),
                run_id: "run-a".to_owned(),
                definition_id: "demo".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                batch_id: "batch-a".to_owned(),
                bridge_protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION
                    .to_owned(),
                bridge_operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::BulkRun
                    .as_str()
                    .to_owned(),
                bridge_request_id: fabrik_throughput::throughput_bridge_request_id(
                    "tenant-a",
                    "instance-a",
                    "run-a",
                    "batch-a",
                ),
                throughput_backend: ThroughputBackend::StreamV2.as_str().to_owned(),
                execution_path: "native_stream_v2".to_owned(),
                status: "completed".to_owned(),
                command_dedupe_key: command.dedupe_key.clone(),
                command,
                command_published_at: Some(now),
                started_at: Some(now),
                terminal_at: Some(now),
                bridge_terminal_status: Some("completed".to_owned()),
                bridge_terminal_event_id: Some(Uuid::now_v7()),
                bridge_terminal_owner_epoch: Some(9),
                bridge_terminal_accepted_at: Some(now + chrono::Duration::seconds(1)),
                created_at: now,
                updated_at: now,
            })
            .await?;

        let status = load_bulk_bridge_status(
            &state,
            "tenant-a",
            "instance-a",
            "run-a",
            "batch-a",
            ThroughputBackend::StreamV2.as_str(),
        )
        .await?
        .context("bridge status should exist")?;
        assert_eq!(status.batch_id, "batch-a");
        assert_eq!(status.protocol_version, fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION);
        assert_eq!(
            status.operation_kind,
            fabrik_throughput::ThroughputBridgeOperationKind::BulkRun.as_str()
        );
        assert_eq!(status.source_workflow_event_id, Some(workflow_event_id));
        assert_eq!(status.bridge_request_id, "throughput-bridge:tenant-a:instance-a:run-a:batch-a");
        assert_eq!(status.submission_status.as_deref(), Some("published"));
        assert_eq!(status.submission_consistency, "bridge-store");
        assert_eq!(status.stream_consistency, "stream-runtime");
        assert_eq!(status.workflow_consistency, "workflow-history");
        assert_eq!(status.command_id, Some(command_id));
        assert_eq!(status.command_partition_key.as_deref(), Some("batch-a:0"));
        assert_eq!(status.stream_status, "completed");
        assert!(status.stream_terminal_at.is_some());
        assert_eq!(status.workflow_status.as_deref(), Some("completed"));
        assert_eq!(status.workflow_owner_epoch, Some(9));
        assert!(status.workflow_terminal_event_id.is_some());
        assert!(status.workflow_accepted_at.is_some());
        assert_eq!(status.next_repair, None);

        assert!(
            load_bulk_bridge_status(
                &state,
                "tenant-a",
                "instance-a",
                "run-a",
                "batch-a",
                "legacy",
            )
            .await?
            .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn load_pending_stream_bridge_repairs_returns_pending_start_repairs() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state = test_state(store.clone()).await?;
        let now = Utc::now();
        let workflow_event_id = Uuid::now_v7();
        let command_id = Uuid::now_v7();
        let command = fabrik_throughput::ThroughputCommandEnvelope {
            command_id,
            occurred_at: now,
            dedupe_key: "throughput-start:test-query-repair".to_owned(),
            partition_key: "batch-repair:0".to_owned(),
            payload: fabrik_throughput::ThroughputCommand::StartThroughputRun(
                fabrik_throughput::StartThroughputRunCommand {
                    dedupe_key: "throughput-start:test-query-repair".to_owned(),
                    bridge_request_id: fabrik_throughput::throughput_bridge_request_id(
                        "tenant-a",
                        "instance-a",
                        "run-a",
                        "batch-repair",
                    ),
                    bridge_protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION
                        .to_owned(),
                    bridge_operation_kind:
                        fabrik_throughput::ThroughputBridgeOperationKind::BulkRun
                            .as_str()
                            .to_owned(),
                    tenant_id: "tenant-a".to_owned(),
                    definition_id: "demo".to_owned(),
                    definition_version: Some(1),
                    artifact_hash: Some("artifact-a".to_owned()),
                    instance_id: "instance-a".to_owned(),
                    run_id: "run-a".to_owned(),
                    batch_id: "batch-repair".to_owned(),
                    activity_type: "benchmark.echo".to_owned(),
                    activity_capabilities: None,
                    task_queue: "bulk".to_owned(),
                    state: Some("join".to_owned()),
                    chunk_size: 2,
                    max_attempts: 1,
                    retry_delay_ms: 0,
                    total_items: 2,
                    aggregation_group_count: 1,
                    execution_policy: Some("parallel".to_owned()),
                    reducer: Some("count".to_owned()),
                    throughput_backend: ThroughputBackend::StreamV2.as_str().to_owned(),
                    throughput_backend_version: ThroughputBackend::StreamV2
                        .default_version()
                        .to_owned(),
                    routing_reason: "stream_v2_selected".to_owned(),
                    admission_policy_version: fabrik_throughput::ADMISSION_POLICY_VERSION
                        .to_owned(),
                    input_handle: fabrik_throughput::PayloadHandle::Inline {
                        key: "bulk-input:batch-repair".to_owned(),
                    },
                    result_handle: fabrik_throughput::PayloadHandle::Inline {
                        key: "bulk-result:batch-repair".to_owned(),
                    },
                },
            ),
        };
        store
            .upsert_throughput_bridge_submission(
                workflow_event_id,
                fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION,
                fabrik_throughput::ThroughputBridgeOperationKind::BulkRun,
                "tenant-a",
                "instance-a",
                "run-a",
                "batch-repair",
                ThroughputBackend::StreamV2.as_str(),
                &fabrik_throughput::throughput_bridge_request_id(
                    "tenant-a",
                    "instance-a",
                    "run-a",
                    "batch-repair",
                ),
                &command.dedupe_key,
                Some(command_id),
                Some(&command.partition_key),
                now,
            )
            .await?;
        store
            .upsert_throughput_run(&fabrik_store::ThroughputRunRecord {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-a".to_owned(),
                run_id: "run-a".to_owned(),
                definition_id: "demo".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                batch_id: "batch-repair".to_owned(),
                bridge_protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION
                    .to_owned(),
                bridge_operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::BulkRun
                    .as_str()
                    .to_owned(),
                bridge_request_id: fabrik_throughput::throughput_bridge_request_id(
                    "tenant-a",
                    "instance-a",
                    "run-a",
                    "batch-repair",
                ),
                throughput_backend: ThroughputBackend::StreamV2.as_str().to_owned(),
                execution_path: "native_stream_v2".to_owned(),
                status: "scheduled".to_owned(),
                command_dedupe_key: command.dedupe_key.clone(),
                command,
                command_published_at: None,
                started_at: None,
                terminal_at: None,
                bridge_terminal_status: None,
                bridge_terminal_event_id: None,
                bridge_terminal_owner_epoch: None,
                bridge_terminal_accepted_at: None,
                created_at: now,
                updated_at: now,
            })
            .await?;

        let repairs = load_pending_stream_bridge_repairs(
            &state,
            ThroughputBackend::StreamV2.as_str(),
            PaginationQuery { limit: Some(10), offset: Some(0) },
        )
        .await?;

        assert_eq!(repairs.throughput_backend, ThroughputBackend::StreamV2.as_str());
        assert_eq!(repairs.repair_count, 1);
        assert_eq!(repairs.repairs.len(), 1);
        let repair = &repairs.repairs[0];
        assert_eq!(repair.batch_id, "batch-repair");
        assert_eq!(repair.next_repair, "publish_start");
        assert_eq!(repair.protocol_version, fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION);
        assert_eq!(
            repair.operation_kind,
            fabrik_throughput::ThroughputBridgeOperationKind::BulkRun.as_str()
        );
        assert_eq!(repair.source_workflow_event_id, Some(workflow_event_id));
        assert_eq!(repair.command_id, Some(command_id));
        assert_eq!(repair.command_partition_key.as_deref(), Some("batch-repair:0"));
        assert_eq!(repair.stream_status, "scheduled");
        assert_eq!(repair.submission_status.as_deref(), Some("admitted"));

        Ok(())
    }

    #[tokio::test]
    async fn load_stream_job_bridge_handle_returns_handle_and_checkpoints() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state = test_state(store.clone()).await?;
        let now = Utc::now();
        let handle = StreamJobBridgeHandleRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            tenant_id: "tenant-a".to_owned(),
            instance_id: "instance-a".to_owned(),
            run_id: "run-a".to_owned(),
            stream_instance_id: "instance-a".to_owned(),
            stream_run_id: "run-a".to_owned(),
            job_id: "job-a".to_owned(),
            handle_id: "stream-job-handle:tenant-a:instance-a:run-a:job-a".to_owned(),
            bridge_request_id: "stream-job-bridge:tenant-a:instance-a:run-a:job-a".to_owned(),
            origin_kind: fabrik_throughput::StreamJobOriginKind::Workflow.as_str().to_owned(),
            definition_id: "demo".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-a".to_owned()),
            job_name: "fraud-detector".to_owned(),
            input_ref: "topic:payments".to_owned(),
            config_ref: Some("config:fraud-detector:v1".to_owned()),
            checkpoint_policy: None,
            view_definitions: None,
            status: fabrik_throughput::StreamJobBridgeHandleStatus::Running.as_str().to_owned(),
            workflow_owner_epoch: Some(2),
            stream_owner_epoch: Some(7),
            cancellation_requested_at: None,
            cancellation_reason: None,
            terminal_event_id: None,
            terminal_at: None,
            workflow_accepted_at: None,
            terminal_output: None,
            terminal_error: None,
            created_at: now,
            updated_at: now,
        };
        store.upsert_stream_job_bridge_handle(&handle).await?;
        let later_handle = StreamJobBridgeHandleRecord {
            workflow_event_id: Uuid::now_v7(),
            protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                .as_str()
                .to_owned(),
            tenant_id: "tenant-a".to_owned(),
            instance_id: "instance-a".to_owned(),
            run_id: "run-a".to_owned(),
            stream_instance_id: "instance-a".to_owned(),
            stream_run_id: "run-a".to_owned(),
            job_id: "job-b".to_owned(),
            handle_id: "stream-job-handle:tenant-a:instance-a:run-a:job-b".to_owned(),
            bridge_request_id: "stream-job-bridge:tenant-a:instance-a:run-a:job-b".to_owned(),
            origin_kind: fabrik_throughput::StreamJobOriginKind::Workflow.as_str().to_owned(),
            definition_id: "demo".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact-a".to_owned()),
            job_name: "fraud-detector".to_owned(),
            input_ref: "topic:payments".to_owned(),
            config_ref: Some("config:fraud-detector:v1".to_owned()),
            checkpoint_policy: None,
            view_definitions: None,
            status: fabrik_throughput::StreamJobBridgeHandleStatus::Running.as_str().to_owned(),
            workflow_owner_epoch: Some(2),
            stream_owner_epoch: Some(8),
            cancellation_requested_at: None,
            cancellation_reason: None,
            terminal_event_id: None,
            terminal_at: None,
            workflow_accepted_at: None,
            terminal_output: None,
            terminal_error: None,
            created_at: now + chrono::Duration::seconds(5),
            updated_at: now + chrono::Duration::seconds(5),
        };
        store.upsert_stream_job_bridge_handle(&later_handle).await?;
        store
            .upsert_stream_job_bridge_checkpoint(&StreamJobCheckpointRecord {
                workflow_event_id: Uuid::now_v7(),
                protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                    .as_str()
                    .to_owned(),
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-a".to_owned(),
                run_id: "run-a".to_owned(),
                job_id: "job-a".to_owned(),
                handle_id: handle.handle_id.clone(),
                bridge_request_id: handle.bridge_request_id.clone(),
                await_request_id: "stream-job-await:job-a:hourly-rollup-ready:event-a".to_owned(),
                checkpoint_name: "hourly-rollup-ready".to_owned(),
                checkpoint_sequence: Some(7),
                status: fabrik_throughput::StreamJobCheckpointStatus::Awaiting.as_str().to_owned(),
                workflow_owner_epoch: Some(2),
                stream_owner_epoch: Some(7),
                reached_at: None,
                output: None,
                accepted_at: None,
                cancelled_at: None,
                created_at: now,
                updated_at: now,
            })
            .await?;
        store
            .upsert_stream_job_bridge_query(&StreamJobQueryRecord {
                workflow_event_id: Uuid::now_v7(),
                protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                    .as_str()
                    .to_owned(),
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-a".to_owned(),
                run_id: "run-a".to_owned(),
                job_id: "job-a".to_owned(),
                handle_id: handle.handle_id.clone(),
                bridge_request_id: handle.bridge_request_id.clone(),
                query_id: "stream-job-query:job-a:currentStats:event-a".to_owned(),
                query_name: "currentStats".to_owned(),
                query_args: Some(json!({"window": "1h"})),
                consistency: "strong".to_owned(),
                status: fabrik_throughput::StreamJobQueryStatus::Completed.as_str().to_owned(),
                workflow_owner_epoch: Some(2),
                stream_owner_epoch: Some(7),
                output: Some(json!({"anomalies": 2})),
                error: None,
                requested_at: now,
                completed_at: Some(now),
                accepted_at: None,
                cancelled_at: None,
                created_at: now,
                updated_at: now,
            })
            .await?;
        store
            .upsert_stream_job_bridge_query(&StreamJobQueryRecord {
                workflow_event_id: Uuid::now_v7(),
                protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                    .as_str()
                    .to_owned(),
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-a".to_owned(),
                run_id: "run-a".to_owned(),
                job_id: "job-b".to_owned(),
                handle_id: later_handle.handle_id.clone(),
                bridge_request_id: later_handle.bridge_request_id.clone(),
                query_id: "stream-job-query:job-b:currentStats:event-b".to_owned(),
                query_name: "currentStats".to_owned(),
                query_args: Some(json!({"window": "5m"})),
                consistency: "strong".to_owned(),
                status: fabrik_throughput::StreamJobQueryStatus::Accepted.as_str().to_owned(),
                workflow_owner_epoch: Some(2),
                stream_owner_epoch: Some(8),
                output: Some(json!({"anomalies": 5})),
                error: None,
                requested_at: now + chrono::Duration::seconds(10),
                completed_at: Some(now + chrono::Duration::seconds(11)),
                accepted_at: Some(now + chrono::Duration::seconds(12)),
                cancelled_at: None,
                created_at: now + chrono::Duration::seconds(10),
                updated_at: now + chrono::Duration::seconds(12),
            })
            .await?;

        let response =
            load_stream_job_callback_handle(&state, "tenant-a", "instance-a", "run-a", "job-a")
                .await?;
        assert_eq!(response.handle.job_id, "job-a");
        assert_eq!(response.handle.status, "running");
        assert_eq!(response.handle.checkpoint_count, 1);
        assert_eq!(response.handle.query_count, 1);
        assert_eq!(response.handle.pending_repair_count, 1);
        assert_eq!(response.handle.next_repair.as_deref(), Some("accept_stream_query"));
        assert_eq!(response.handle.pending_repairs, vec!["accept_stream_query".to_owned()]);
        assert_eq!(response.handle.latest_query_name.as_deref(), Some("currentStats"));
        assert_eq!(response.handle.latest_query_status.as_deref(), Some("completed"));
        assert_eq!(response.handle.latest_query_consistency.as_deref(), Some("strong"));
        assert_eq!(response.checkpoints.len(), 1);
        assert_eq!(response.checkpoints[0].checkpoint_name, "hourly-rollup-ready");
        assert_eq!(response.checkpoints[0].status, "awaiting");
        assert_eq!(response.checkpoints[0].next_repair, None);
        assert_eq!(response.queries.len(), 1);
        assert_eq!(response.queries[0].query_name, "currentStats");
        assert_eq!(response.queries[0].status, "completed");
        assert_eq!(response.queries[0].next_repair.as_deref(), Some("accept_stream_query"));

        let list = load_stream_job_callback_handles(
            &state,
            "tenant-a",
            "instance-a",
            "run-a",
            StreamJobBridgeListQuery {
                limit: Some(10),
                offset: Some(0),
                ..StreamJobBridgeListQuery::default()
            },
        )
        .await?;
        assert_eq!(list.handle_count, 2);
        assert_eq!(list.handles[0].handle_id, handle.handle_id);
        assert_eq!(list.handles[0].query_count, 1);
        assert_eq!(list.handles[0].pending_repair_count, 1);
        assert_eq!(list.handles[0].next_repair.as_deref(), Some("accept_stream_query"));
        assert_eq!(list.handles[0].latest_query_name.as_deref(), Some("currentStats"));
        assert_eq!(list.handles[0].latest_query_status.as_deref(), Some("completed"));
        assert_eq!(list.handles[1].handle_id, later_handle.handle_id);
        assert_eq!(list.handles[1].pending_repair_count, 0);

        let filtered = load_stream_job_callback_handles(
            &state,
            "tenant-a",
            "instance-a",
            "run-a",
            StreamJobBridgeListQuery {
                limit: Some(10),
                offset: Some(0),
                latest_query_status: Some("completed".to_owned()),
                latest_query_name: Some("currentStats".to_owned()),
                ..StreamJobBridgeListQuery::default()
            },
        )
        .await?;
        assert_eq!(filtered.handle_count, 1);
        assert_eq!(filtered.handles.len(), 1);
        assert_eq!(filtered.handles[0].handle_id, handle.handle_id);

        let repair_filtered = load_stream_job_callback_handles(
            &state,
            "tenant-a",
            "instance-a",
            "run-a",
            StreamJobBridgeListQuery {
                limit: Some(10),
                offset: Some(0),
                next_repair: Some("accept_stream_query".to_owned()),
                ..StreamJobBridgeListQuery::default()
            },
        )
        .await?;
        assert_eq!(repair_filtered.handle_count, 1);
        assert_eq!(repair_filtered.handles.len(), 1);
        assert_eq!(repair_filtered.handles[0].handle_id, handle.handle_id);

        let filtered_out = load_stream_job_callback_handles(
            &state,
            "tenant-a",
            "instance-a",
            "run-a",
            StreamJobBridgeListQuery {
                limit: Some(10),
                offset: Some(0),
                latest_query_status: Some("failed".to_owned()),
                ..StreamJobBridgeListQuery::default()
            },
        )
        .await?;
        assert_eq!(filtered_out.handle_count, 0);
        assert!(filtered_out.handles.is_empty());

        let repair_filtered_out = load_stream_job_callback_handles(
            &state,
            "tenant-a",
            "instance-a",
            "run-a",
            StreamJobBridgeListQuery {
                limit: Some(10),
                offset: Some(0),
                next_repair: Some("accept_stream_terminal".to_owned()),
                ..StreamJobBridgeListQuery::default()
            },
        )
        .await?;
        assert_eq!(repair_filtered_out.handle_count, 0);
        assert!(repair_filtered_out.handles.is_empty());

        let repairs = load_stream_job_callback_repairs(
            &state,
            "tenant-a",
            "instance-a",
            "run-a",
            StreamJobBridgeListQuery {
                limit: Some(10),
                offset: Some(0),
                ..StreamJobBridgeListQuery::default()
            },
        )
        .await?;
        assert_eq!(repairs.repair_count, 1);
        assert_eq!(repairs.handles.len(), 1);
        assert_eq!(repairs.handles[0].handle_id, handle.handle_id);

        let query_repairs = load_stream_job_callback_repairs(
            &state,
            "tenant-a",
            "instance-a",
            "run-a",
            StreamJobBridgeListQuery {
                limit: Some(10),
                offset: Some(0),
                next_repair: Some("accept_stream_query".to_owned()),
                ..StreamJobBridgeListQuery::default()
            },
        )
        .await?;
        assert_eq!(query_repairs.repair_count, 1);
        assert_eq!(query_repairs.handles.len(), 1);
        assert_eq!(query_repairs.handles[0].handle_id, handle.handle_id);

        let sorted = load_stream_job_callback_handles(
            &state,
            "tenant-a",
            "instance-a",
            "run-a",
            StreamJobBridgeListQuery {
                limit: Some(10),
                offset: Some(0),
                sort: Some("latest_query_activity_desc".to_owned()),
                ..StreamJobBridgeListQuery::default()
            },
        )
        .await?;
        assert_eq!(sorted.handle_count, 2);
        assert_eq!(sorted.handles[0].handle_id, later_handle.handle_id);
        assert_eq!(sorted.handles[1].handle_id, handle.handle_id);

        Ok(())
    }

    #[tokio::test]
    async fn load_tenant_stream_jobs_includes_workflow_and_standalone_jobs() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state = test_state(store.clone()).await?;
        let now = Utc::now();

        store
            .upsert_stream_job(&StreamJobRecord {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "workflow-instance".to_owned(),
                run_id: "workflow-run".to_owned(),
                stream_instance_id: "stream-workflow-instance".to_owned(),
                stream_run_id: "stream-workflow-run".to_owned(),
                job_id: "job-workflow".to_owned(),
                handle_id: "handle-workflow".to_owned(),
                protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                    .as_str()
                    .to_owned(),
                workflow_event_id: Uuid::now_v7(),
                bridge_request_id: "bridge-workflow".to_owned(),
                origin_kind: fabrik_throughput::StreamJobOriginKind::Workflow.as_str().to_owned(),
                definition_id: "fraud-detector".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                job_name: "fraud-detector".to_owned(),
                input_ref: "topic:payments".to_owned(),
                config_ref: Some("config:workflow".to_owned()),
                checkpoint_policy: None,
                view_definitions: None,
                status: fabrik_throughput::StreamJobStatus::Running.as_str().to_owned(),
                workflow_owner_epoch: Some(2),
                stream_owner_epoch: Some(3),
                starting_at: Some(now),
                running_at: Some(now + chrono::Duration::seconds(1)),
                draining_at: None,
                latest_checkpoint_name: Some("hourly-rollup-ready".to_owned()),
                latest_checkpoint_sequence: Some(5),
                latest_checkpoint_at: Some(now + chrono::Duration::seconds(2)),
                latest_checkpoint_output: Some(json!({"ready": true})),
                cancellation_requested_at: None,
                cancellation_reason: None,
                workflow_accepted_at: Some(now + chrono::Duration::seconds(2)),
                terminal_event_id: None,
                terminal_at: None,
                terminal_output: None,
                terminal_error: None,
                created_at: now,
                updated_at: now + chrono::Duration::seconds(2),
            })
            .await?;

        store
            .upsert_stream_job_bridge_handle(&StreamJobBridgeHandleRecord {
                workflow_event_id: Uuid::now_v7(),
                protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                    .as_str()
                    .to_owned(),
                tenant_id: "tenant-a".to_owned(),
                instance_id: "workflow-instance".to_owned(),
                run_id: "workflow-run".to_owned(),
                stream_instance_id: "stream-workflow-instance".to_owned(),
                stream_run_id: "stream-workflow-run".to_owned(),
                job_id: "job-workflow".to_owned(),
                handle_id: "handle-workflow".to_owned(),
                bridge_request_id: "bridge-workflow".to_owned(),
                origin_kind: fabrik_throughput::StreamJobOriginKind::Workflow.as_str().to_owned(),
                definition_id: "fraud-detector".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                job_name: "fraud-detector".to_owned(),
                input_ref: "topic:payments".to_owned(),
                config_ref: Some("config:workflow".to_owned()),
                checkpoint_policy: None,
                view_definitions: None,
                status: fabrik_throughput::StreamJobBridgeHandleStatus::Running.as_str().to_owned(),
                workflow_owner_epoch: Some(2),
                stream_owner_epoch: Some(3),
                cancellation_requested_at: None,
                cancellation_reason: None,
                terminal_event_id: None,
                terminal_at: None,
                workflow_accepted_at: Some(now + chrono::Duration::seconds(2)),
                terminal_output: None,
                terminal_error: None,
                created_at: now,
                updated_at: now + chrono::Duration::seconds(2),
            })
            .await?;

        store
            .upsert_stream_job_bridge_query(&StreamJobQueryRecord {
                workflow_event_id: Uuid::now_v7(),
                protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                    .as_str()
                    .to_owned(),
                tenant_id: "tenant-a".to_owned(),
                instance_id: "workflow-instance".to_owned(),
                run_id: "workflow-run".to_owned(),
                job_id: "job-workflow".to_owned(),
                handle_id: "handle-workflow".to_owned(),
                bridge_request_id: "bridge-workflow".to_owned(),
                query_id: "query-workflow".to_owned(),
                query_name: "currentStats".to_owned(),
                query_args: Some(json!({"window": "1h"})),
                consistency: fabrik_throughput::StreamJobQueryConsistency::Strong
                    .as_str()
                    .to_owned(),
                status: fabrik_throughput::StreamJobQueryStatus::Completed.as_str().to_owned(),
                workflow_owner_epoch: Some(2),
                stream_owner_epoch: Some(3),
                output: Some(json!({"anomalies": 2})),
                error: None,
                requested_at: now + chrono::Duration::seconds(2),
                completed_at: Some(now + chrono::Duration::seconds(3)),
                accepted_at: None,
                cancelled_at: None,
                created_at: now + chrono::Duration::seconds(2),
                updated_at: now + chrono::Duration::seconds(3),
            })
            .await?;

        store
            .upsert_stream_job(&StreamJobRecord {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "stream-standalone".to_owned(),
                run_id: "run-standalone".to_owned(),
                stream_instance_id: "stream-standalone".to_owned(),
                stream_run_id: "run-standalone".to_owned(),
                job_id: "job-standalone".to_owned(),
                handle_id: "handle-standalone".to_owned(),
                protocol_version: fabrik_throughput::THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
                operation_kind: fabrik_throughput::ThroughputBridgeOperationKind::StreamJob
                    .as_str()
                    .to_owned(),
                workflow_event_id: Uuid::now_v7(),
                bridge_request_id: "bridge-standalone".to_owned(),
                origin_kind: fabrik_throughput::StreamJobOriginKind::Standalone.as_str().to_owned(),
                definition_id: "leaderboard".to_owned(),
                definition_version: Some(3),
                artifact_hash: Some("artifact-b".to_owned()),
                job_name: "leaderboard".to_owned(),
                input_ref: "topic:scores".to_owned(),
                config_ref: Some("config:standalone".to_owned()),
                checkpoint_policy: None,
                view_definitions: None,
                status: fabrik_throughput::StreamJobStatus::Created.as_str().to_owned(),
                workflow_owner_epoch: None,
                stream_owner_epoch: Some(1),
                starting_at: None,
                running_at: None,
                draining_at: None,
                latest_checkpoint_name: None,
                latest_checkpoint_sequence: None,
                latest_checkpoint_at: None,
                latest_checkpoint_output: None,
                cancellation_requested_at: None,
                cancellation_reason: None,
                workflow_accepted_at: None,
                terminal_event_id: None,
                terminal_at: None,
                terminal_output: None,
                terminal_error: None,
                created_at: now + chrono::Duration::seconds(5),
                updated_at: now + chrono::Duration::seconds(5),
            })
            .await?;

        let response = load_tenant_stream_jobs(
            &state,
            "tenant-a",
            StreamJobListQuery {
                limit: Some(10),
                offset: Some(0),
                sort: Some("created_at_asc".to_owned()),
                ..StreamJobListQuery::default()
            },
        )
        .await?;
        assert_eq!(response.job_count, 2);
        assert_eq!(response.jobs.len(), 2);
        assert_eq!(response.jobs[0].job_id, "job-workflow");
        assert_eq!(
            response.jobs[0].workflow_binding.as_ref().map(|binding| binding.instance_id.as_str()),
            Some("workflow-instance")
        );
        assert_eq!(response.jobs[0].bridge_surface.pending_repair_count, 1);
        assert_eq!(
            response.jobs[0].bridge_surface.next_repair.as_deref(),
            Some("accept_stream_query")
        );
        assert_eq!(
            response.jobs[0].bridge_surface.latest_query_name.as_deref(),
            Some("currentStats")
        );
        assert_eq!(response.jobs[1].job_id, "job-standalone");
        assert!(response.jobs[1].workflow_binding.is_none());
        assert_eq!(response.jobs[1].stream_instance_id, "stream-standalone");

        let standalone_only = load_tenant_stream_jobs(
            &state,
            "tenant-a",
            StreamJobListQuery {
                limit: Some(10),
                offset: Some(0),
                origin_kind: Some("standalone".to_owned()),
                ..StreamJobListQuery::default()
            },
        )
        .await?;
        assert_eq!(standalone_only.job_count, 1);
        assert_eq!(standalone_only.jobs[0].job_id, "job-standalone");

        let filtered = load_tenant_stream_jobs(
            &state,
            "tenant-a",
            StreamJobListQuery {
                limit: Some(10),
                offset: Some(0),
                stream_instance_id: Some("stream-workflow-instance".to_owned()),
                ..StreamJobListQuery::default()
            },
        )
        .await?;
        assert_eq!(filtered.job_count, 1);
        assert_eq!(filtered.jobs[0].job_id, "job-workflow");

        Ok(())
    }

    #[tokio::test]
    async fn eventual_stream_job_view_browse_filters_by_prefix() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state = test_state(store.clone()).await?;
        let now = Utc::now();

        for (logical_key, value) in [
            ("acct_1", json!({"accountId": "acct_1", "totalAmount": 10})),
            ("acct_2", json!({"accountId": "acct_2", "totalAmount": 20})),
            ("cust_1", json!({"accountId": "cust_1", "totalAmount": 30})),
        ] {
            store
                .upsert_stream_job_view_query(&StreamJobViewRecord {
                    tenant_id: "tenant-a".to_owned(),
                    instance_id: "instance-a".to_owned(),
                    run_id: "run-a".to_owned(),
                    job_id: "job-a".to_owned(),
                    handle_id: "handle-a".to_owned(),
                    view_name: "accountTotals".to_owned(),
                    logical_key: logical_key.to_owned(),
                    output: value,
                    checkpoint_sequence: 7,
                    updated_at: now,
                })
                .await?;
        }

        let (key_total, keys) = load_eventual_stream_job_view_keys(
            &state,
            "tenant-a",
            "instance-a",
            "run-a",
            "job-a",
            "accountTotals",
            Some("acct_"),
            10,
            0,
        )
        .await?;
        assert_eq!(key_total, 2);
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].logical_key, "acct_1");
        assert_eq!(keys[1].logical_key, "acct_2");

        let (entry_total, entries) = load_eventual_stream_job_view_entries(
            &state,
            "tenant-a",
            "instance-a",
            "run-a",
            "job-a",
            "accountTotals",
            Some("acct_"),
            10,
            0,
        )
        .await?;
        assert_eq!(entry_total, 2);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].logical_key, "acct_1");
        assert_eq!(entries[0].output["totalAmount"], 10);
        assert_eq!(entries[1].logical_key, "acct_2");
        assert_eq!(entries[1].output["totalAmount"], 20);

        Ok(())
    }

    fn graph_test_artifact() -> CompiledWorkflowArtifact {
        let mut artifact = CompiledWorkflowArtifact::new(
            "payments",
            3,
            "tsc-0.1",
            ArtifactEntrypoint {
                module: "workflows/payments.ts".to_owned(),
                export: "workflow".to_owned(),
            },
            CompiledWorkflow {
                initial_state: "charge-card".to_owned(),
                states: BTreeMap::from([
                    (
                        "charge-card".to_owned(),
                        CompiledStateNode::Step {
                            handler: "payments.charge".to_owned(),
                            input: Expression::Literal { value: json!({"charge": true}) },
                            next: Some("wait-payment".to_owned()),
                            task_queue: None,
                            retry: None,
                            config: None,
                            schedule_to_start_timeout_ms: None,
                            schedule_to_close_timeout_ms: None,
                            start_to_close_timeout_ms: None,
                            heartbeat_timeout_ms: None,
                            output_var: None,
                            on_error: Some(fabrik_workflow::ErrorTransition {
                                next: "failed".to_owned(),
                                error_var: Some("error".to_owned()),
                            }),
                        },
                    ),
                    (
                        "wait-payment".to_owned(),
                        CompiledStateNode::WaitForEvent {
                            event_type: "payment-approved".to_owned(),
                            next: "bulk-refund".to_owned(),
                            output_var: Some("signal".to_owned()),
                        },
                    ),
                    (
                        "bulk-refund".to_owned(),
                        CompiledStateNode::StartBulkActivity {
                            activity_type: "payments.refund".to_owned(),
                            items: Expression::Literal { value: json!([1, 2, 3]) },
                            next: "await-refund".to_owned(),
                            handle_var: "refund-batch".to_owned(),
                            activity_capabilities: None,
                            task_queue: None,
                            execution_policy: None,
                            reducer: Some("count".to_owned()),
                            throughput_backend: Some("stream-v2".to_owned()),
                            chunk_size: Some(32),
                            retry: None,
                        },
                    ),
                    (
                        "await-refund".to_owned(),
                        CompiledStateNode::WaitForBulkActivity {
                            bulk_ref_var: "refund-batch".to_owned(),
                            next: "spawn-child".to_owned(),
                            output_var: Some("refund-summary".to_owned()),
                            on_error: None,
                        },
                    ),
                    (
                        "spawn-child".to_owned(),
                        CompiledStateNode::StartChild {
                            child_definition_id: "settlement-child".to_owned(),
                            input: Expression::Literal { value: json!({"orderId": 7}) },
                            next: "await-child".to_owned(),
                            handle_var: Some("child".to_owned()),
                            workflow_id: None,
                            task_queue: None,
                            parent_close_policy: fabrik_workflow::ParentClosePolicy::Terminate,
                        },
                    ),
                    (
                        "await-child".to_owned(),
                        CompiledStateNode::WaitForChild {
                            child_ref_var: "child".to_owned(),
                            next: "done".to_owned(),
                            output_var: Some("child-output".to_owned()),
                            on_error: None,
                        },
                    ),
                    ("done".to_owned(), CompiledStateNode::Succeed { output: None }),
                    (
                        "failed".to_owned(),
                        CompiledStateNode::Fail {
                            reason: Some(Expression::Literal { value: json!("failed") }),
                        },
                    ),
                ]),
                params: Vec::new(),
                async_helpers: BTreeMap::new(),
                non_cancellable_states: BTreeSet::new(),
            },
        );
        artifact.source_files = vec!["workflows/payments.ts".to_owned()];
        artifact.source_map = BTreeMap::from([
            (
                "charge-card".to_owned(),
                SourceLocation { file: "workflows/payments.ts".to_owned(), line: 12, column: 2 },
            ),
            (
                "wait-payment".to_owned(),
                SourceLocation { file: "workflows/payments.ts".to_owned(), line: 18, column: 2 },
            ),
            (
                "signal:payment-approved".to_owned(),
                SourceLocation { file: "workflows/payments.ts".to_owned(), line: 22, column: 4 },
            ),
        ]);
        artifact.queries = BTreeMap::from([(
            "status".to_owned(),
            CompiledQueryHandler {
                arg_name: None,
                expr: Expression::Literal { value: json!("ok") },
            },
        )]);
        artifact.updates = BTreeMap::from([(
            "reprice".to_owned(),
            CompiledUpdateHandler {
                arg_name: None,
                initial_state: "apply".to_owned(),
                states: BTreeMap::from([(
                    "apply".to_owned(),
                    CompiledStateNode::Succeed { output: None },
                )]),
            },
        )]);
        artifact.artifact_hash = artifact.hash();
        artifact
    }

    #[test]
    fn normalize_artifact_graph_builds_semantic_modules_and_source_anchors() {
        let artifact = graph_test_artifact();
        let projection = normalize_artifact_graph(&artifact);

        assert!(projection.modules.iter().any(|module| module.kind == "entry"));
        assert!(
            projection
                .modules
                .iter()
                .any(|module| module.kind == "fan_out" || module.kind == "bulk_activity")
        );
        assert!(projection.modules.iter().any(|module| module.kind == "child_workflow"));
        assert!(projection.modules.iter().any(|module| module.kind == "update_handler"));
        assert!(projection.modules.iter().any(|module| module.kind == "query_handler"));
        let wait_module = projection
            .modules
            .iter()
            .find(|module| module.id == workflow_module_id("wait-payment"))
            .expect("wait module");
        assert_eq!(
            wait_module.source_anchor.as_ref().map(|anchor| anchor.file.as_str()),
            Some("workflows/payments.ts")
        );
        assert!(
            projection
                .module_edges
                .iter()
                .any(|edge| edge.source == workflow_module_id("charge-card")
                    && edge.target == workflow_module_id("wait-payment"))
        );
    }

    #[test]
    fn build_run_overlay_marks_current_and_failed_nodes() {
        let artifact = graph_test_artifact();
        let identity = WorkflowIdentity::new(
            "tenant-a",
            "payments",
            artifact.definition_version,
            artifact.artifact_hash.clone(),
            "instance-1",
            "run-9",
            "test",
        );

        let mut started =
            EventEnvelope::new("WorkflowStarted", identity.clone(), WorkflowEvent::WorkflowStarted);
        started.metadata.insert("state".to_owned(), "charge-card".to_owned());

        let scheduled = EventEnvelope::new(
            "ActivityTaskScheduled",
            identity.clone(),
            WorkflowEvent::ActivityTaskScheduled {
                activity_id: "charge-card".to_owned(),
                activity_type: "payments.charge".to_owned(),
                activity_capabilities: None,
                task_queue: "payments".to_owned(),
                attempt: 2,
                input: json!({"orderId": 7}),
                config: None,
                state: Some("charge-card".to_owned()),
                schedule_to_start_timeout_ms: None,
                schedule_to_close_timeout_ms: None,
                start_to_close_timeout_ms: None,
                heartbeat_timeout_ms: None,
            },
        );

        let mut waiting = EventEnvelope::new(
            "SignalQueued",
            identity,
            WorkflowEvent::SignalQueued {
                signal_id: "sig-1".to_owned(),
                signal_type: "payment-approved".to_owned(),
                payload: json!({"ok": true}),
            },
        );
        waiting.metadata.insert("state".to_owned(), "wait-payment".to_owned());

        let now = Utc::now();
        let overlay = build_run_overlay(
            &artifact,
            "run-9",
            Some("wait-payment".to_owned()),
            &[started, scheduled, waiting],
            &[WorkflowActivityRecord {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-1".to_owned(),
                run_id: "run-9".to_owned(),
                definition_id: "payments".to_owned(),
                definition_version: Some(3),
                artifact_hash: Some(artifact.artifact_hash.clone()),
                activity_id: "charge-card".to_owned(),
                attempt: 2,
                activity_type: "payments.charge".to_owned(),
                activity_capabilities: None,
                task_queue: "payments".to_owned(),
                state: Some("charge-card".to_owned()),
                status: WorkflowActivityStatus::Failed,
                input: json!({"orderId": 7}),
                config: None,
                output: None,
                error: Some("card declined".to_owned()),
                cancellation_requested: false,
                cancellation_reason: None,
                cancellation_metadata: None,
                worker_id: Some("worker-a".to_owned()),
                worker_build_id: Some("build-a".to_owned()),
                scheduled_at: now,
                started_at: Some(now),
                last_heartbeat_at: None,
                lease_expires_at: None,
                completed_at: Some(now),
                schedule_to_start_timeout_ms: None,
                schedule_to_close_timeout_ms: None,
                start_to_close_timeout_ms: None,
                heartbeat_timeout_ms: None,
                last_event_id: Uuid::now_v7(),
                last_event_type: "ActivityTaskFailed".to_owned(),
                updated_at: now,
            }],
            &[WorkflowSignalRecord {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-1".to_owned(),
                run_id: "run-9".to_owned(),
                signal_id: "sig-1".to_owned(),
                signal_type: "payment-approved".to_owned(),
                dedupe_key: None,
                payload: json!({"ok": true}),
                status: WorkflowSignalStatus::Queued,
                source_event_id: Uuid::now_v7(),
                dispatch_event_id: None,
                consumed_event_id: None,
                enqueued_at: now,
                consumed_at: None,
                updated_at: now,
            }],
            &[],
            &[],
            &[],
        );

        assert_eq!(overlay.current_node_id.as_deref(), Some("workflow:wait-payment"));
        assert!(
            overlay
                .node_statuses
                .iter()
                .any(|status| status.id == "workflow:charge-card" && status.status == "failed")
        );
        assert!(
            overlay
                .node_statuses
                .iter()
                .any(|status| status.id == "workflow:wait-payment" && status.status == "current")
        );
        assert!(
            overlay
                .signal_summaries
                .iter()
                .any(|summary| summary.signal_name == "payment-approved"
                    && summary.node_id.as_deref() == Some("workflow:wait-payment"))
        );
        assert_eq!(
            overlay.blocked_by.as_ref().map(|blocked| blocked.kind.as_str()),
            Some("signal")
        );
    }

    #[tokio::test]
    async fn workflow_definition_graph_endpoint_returns_normalized_graph() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state = test_state(store.clone()).await?;
        let artifact = graph_test_artifact();
        store.put_artifact("tenant-a", &artifact).await?;

        let Json(response) = get_workflow_definition_graph(
            Path(("tenant-a".to_owned(), "payments".to_owned())),
            State(state),
        )
        .await
        .expect("graph response");

        assert_eq!(response.definition_id, "payments");
        assert!(response.nodes.iter().any(|node| node.id == "workflow:charge-card"));
        assert!(response.modules.iter().any(|module| module.kind == "update_handler"));
        Ok(())
    }

    #[tokio::test]
    async fn list_runs_uses_current_projection_and_snapshots_for_filters() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state = test_state(store.clone()).await?;

        let historical_updated_at = Utc::now() - chrono::Duration::minutes(10);
        store
            .put_run_start(
                "tenant-a",
                "instance-1",
                "run-1",
                "payments",
                Some(1),
                Some("artifact-a"),
                "payments",
                None,
                None,
                Uuid::now_v7(),
                historical_updated_at - chrono::Duration::minutes(2),
                None,
                None,
            )
            .await?;
        store.close_run("tenant-a", "instance-1", "run-1", historical_updated_at).await?;
        store
            .put_snapshot(&WorkflowInstanceState {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-1".to_owned(),
                run_id: "run-1".to_owned(),
                definition_id: "payments".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                workflow_task_queue: "payments".to_owned(),
                sticky_workflow_build_id: Some("build-a".to_owned()),
                sticky_workflow_poller_id: Some("poller-a".to_owned()),
                current_state: Some("settled".to_owned()),
                context: None,
                artifact_execution: None,
                status: WorkflowStatus::Completed,
                input: Some(json!({"orderId": 1})),
                persisted_input_handle: None,
                memo: None,
                search_attributes: None,
                output: Some(json!({"ok": true})),
                event_count: 8,
                last_event_id: Uuid::now_v7(),
                last_event_type: "WorkflowCompleted".to_owned(),
                updated_at: historical_updated_at,
            })
            .await?;
        store
            .put_run_start(
                "tenant-a",
                "instance-1",
                "run-2",
                "payments",
                Some(2),
                Some("artifact-b"),
                "payments",
                None,
                None,
                Uuid::now_v7(),
                Utc::now() - chrono::Duration::minutes(1),
                Some("run-1"),
                Some("run-1"),
            )
            .await?;
        store
            .record_run_continuation(
                "tenant-a",
                "instance-1",
                "run-1",
                "run-2",
                "continue-as-new",
                Uuid::now_v7(),
                Uuid::now_v7(),
                Utc::now() - chrono::Duration::minutes(1),
            )
            .await?;
        store
            .upsert_instance(&WorkflowInstanceState {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-1".to_owned(),
                run_id: "run-2".to_owned(),
                definition_id: "payments".to_owned(),
                definition_version: Some(2),
                artifact_hash: Some("artifact-b".to_owned()),
                workflow_task_queue: "payments".to_owned(),
                sticky_workflow_build_id: Some("build-b".to_owned()),
                sticky_workflow_poller_id: Some("poller-b".to_owned()),
                current_state: Some("charge-card".to_owned()),
                context: None,
                artifact_execution: None,
                status: WorkflowStatus::Running,
                input: Some(json!({"orderId": 2})),
                persisted_input_handle: None,
                memo: None,
                search_attributes: None,
                output: None,
                event_count: 12,
                last_event_id: Uuid::now_v7(),
                last_event_type: "ActivityScheduled".to_owned(),
                updated_at: Utc::now(),
            })
            .await?;

        let Json(response) = list_runs(
            Path("tenant-a".to_owned()),
            Query(RunListQuery { status: Some("completed".to_owned()), ..RunListQuery::default() }),
            State(state),
        )
        .await
        .expect("list runs response");

        assert_eq!(response.run_count, 1);
        assert_eq!(response.items[0].run_id, "run-1");
        assert_eq!(response.items[0].status, "completed");
        assert_eq!(response.items[0].current_state.as_deref(), Some("settled"));
        assert_eq!(response.items[0].last_event_type.as_deref(), Some("WorkflowCompleted"));
        Ok(())
    }

    #[tokio::test]
    async fn list_runs_includes_routing_status_when_sticky_build_is_incompatible() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state = test_state(store.clone()).await?;

        store
            .register_task_queue_build(
                "tenant-a",
                TaskQueueKind::Workflow,
                "payments",
                "build-a",
                &["artifact-a".to_owned()],
                None,
            )
            .await?;
        store
            .register_task_queue_build(
                "tenant-a",
                TaskQueueKind::Workflow,
                "payments",
                "build-b",
                &["artifact-a".to_owned()],
                None,
            )
            .await?;
        store
            .upsert_compatibility_set(
                "tenant-a",
                TaskQueueKind::Workflow,
                "payments",
                "stable",
                &["build-a".to_owned()],
                true,
            )
            .await?;
        store
            .put_run_start(
                "tenant-a",
                "instance-1",
                "run-1",
                "payments",
                Some(1),
                Some("artifact-a"),
                "payments",
                None,
                None,
                Uuid::now_v7(),
                Utc::now(),
                None,
                None,
            )
            .await?;
        store
            .upsert_instance(&WorkflowInstanceState {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-1".to_owned(),
                run_id: "run-1".to_owned(),
                definition_id: "payments".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                workflow_task_queue: "payments".to_owned(),
                sticky_workflow_build_id: Some("build-b".to_owned()),
                sticky_workflow_poller_id: Some("poller-b".to_owned()),
                current_state: Some("charge-card".to_owned()),
                context: None,
                artifact_execution: None,
                status: WorkflowStatus::Running,
                input: Some(json!({"orderId": 2})),
                persisted_input_handle: None,
                memo: None,
                search_attributes: None,
                output: None,
                event_count: 12,
                last_event_id: Uuid::now_v7(),
                last_event_type: "ActivityScheduled".to_owned(),
                updated_at: Utc::now(),
            })
            .await?;
        store
            .update_run_workflow_sticky(
                "tenant-a",
                "instance-1",
                "run-1",
                "payments",
                "build-b",
                "poller-b",
                Utc::now(),
            )
            .await?;

        let Json(response) =
            list_runs(Path("tenant-a".to_owned()), Query(RunListQuery::default()), State(state))
                .await
                .expect("list runs response");

        assert_eq!(response.run_count, 1);
        assert_eq!(response.items[0].routing_status, "sticky_incompatible_fallback_required");
        Ok(())
    }

    #[tokio::test]
    async fn list_runs_can_filter_by_routing_status() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state = test_state(store.clone()).await?;

        store
            .register_task_queue_build(
                "tenant-a",
                TaskQueueKind::Workflow,
                "payments",
                "build-a",
                &["artifact-a".to_owned()],
                None,
            )
            .await?;
        store
            .register_task_queue_build(
                "tenant-a",
                TaskQueueKind::Workflow,
                "payments",
                "build-b",
                &["artifact-a".to_owned()],
                None,
            )
            .await?;
        store
            .upsert_compatibility_set(
                "tenant-a",
                TaskQueueKind::Workflow,
                "payments",
                "stable",
                &["build-a".to_owned()],
                true,
            )
            .await?;

        for (instance_id, run_id, sticky_build_id) in
            [("instance-a", "run-a", Some("build-a")), ("instance-b", "run-b", Some("build-b"))]
        {
            store
                .put_run_start(
                    "tenant-a",
                    instance_id,
                    run_id,
                    "payments",
                    Some(1),
                    Some("artifact-a"),
                    "payments",
                    None,
                    None,
                    Uuid::now_v7(),
                    Utc::now(),
                    None,
                    None,
                )
                .await?;
            store
                .upsert_instance(&WorkflowInstanceState {
                    tenant_id: "tenant-a".to_owned(),
                    instance_id: instance_id.to_owned(),
                    run_id: run_id.to_owned(),
                    definition_id: "payments".to_owned(),
                    definition_version: Some(1),
                    artifact_hash: Some("artifact-a".to_owned()),
                    workflow_task_queue: "payments".to_owned(),
                    sticky_workflow_build_id: sticky_build_id.map(str::to_owned),
                    sticky_workflow_poller_id: Some(format!("poller-{run_id}")),
                    current_state: Some("charge-card".to_owned()),
                    context: None,
                    artifact_execution: None,
                    status: WorkflowStatus::Running,
                    input: Some(json!({"instance": instance_id})),
                    persisted_input_handle: None,
                    memo: None,
                    search_attributes: None,
                    output: None,
                    event_count: 4,
                    last_event_id: Uuid::now_v7(),
                    last_event_type: "ActivityScheduled".to_owned(),
                    updated_at: Utc::now(),
                })
                .await?;
            if let Some(build_id) = sticky_build_id {
                store
                    .update_run_workflow_sticky(
                        "tenant-a",
                        instance_id,
                        run_id,
                        "payments",
                        build_id,
                        &format!("poller-{run_id}"),
                        Utc::now(),
                    )
                    .await?;
            }
        }

        let Json(response) = list_runs(
            Path("tenant-a".to_owned()),
            Query(RunListQuery {
                routing_status: Some("sticky_incompatible_fallback_required".to_owned()),
                ..RunListQuery::default()
            }),
            State(state),
        )
        .await
        .expect("list runs response");

        assert_eq!(response.run_count, 1);
        assert_eq!(response.items[0].instance_id, "instance-b");
        assert_eq!(response.items[0].routing_status, "sticky_incompatible_fallback_required");
        Ok(())
    }

    #[tokio::test]
    async fn list_workflows_can_filter_by_routing_status() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state = test_state(store.clone()).await?;

        store
            .register_task_queue_build(
                "tenant-a",
                TaskQueueKind::Workflow,
                "payments",
                "build-a",
                &["artifact-a".to_owned()],
                None,
            )
            .await?;
        store
            .upsert_compatibility_set(
                "tenant-a",
                TaskQueueKind::Workflow,
                "payments",
                "stable",
                &["build-a".to_owned()],
                true,
            )
            .await?;

        store
            .upsert_instance(&WorkflowInstanceState {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-a".to_owned(),
                run_id: "run-a".to_owned(),
                definition_id: "payments".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                workflow_task_queue: "payments".to_owned(),
                sticky_workflow_build_id: Some("build-a".to_owned()),
                sticky_workflow_poller_id: Some("poller-a".to_owned()),
                current_state: Some("charge-card".to_owned()),
                context: None,
                artifact_execution: None,
                status: WorkflowStatus::Running,
                input: Some(json!({"instance": "a"})),
                persisted_input_handle: None,
                memo: None,
                search_attributes: None,
                output: None,
                event_count: 4,
                last_event_id: Uuid::now_v7(),
                last_event_type: "ActivityScheduled".to_owned(),
                updated_at: Utc::now(),
            })
            .await?;
        store
            .upsert_instance(&WorkflowInstanceState {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-b".to_owned(),
                run_id: "run-b".to_owned(),
                definition_id: "payments".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                workflow_task_queue: "payments".to_owned(),
                sticky_workflow_build_id: Some("build-z".to_owned()),
                sticky_workflow_poller_id: Some("poller-b".to_owned()),
                current_state: Some("charge-card".to_owned()),
                context: None,
                artifact_execution: None,
                status: WorkflowStatus::Running,
                input: Some(json!({"instance": "b"})),
                persisted_input_handle: None,
                memo: None,
                search_attributes: None,
                output: None,
                event_count: 4,
                last_event_id: Uuid::now_v7(),
                last_event_type: "ActivityScheduled".to_owned(),
                updated_at: Utc::now(),
            })
            .await?;

        let Json(response) = list_workflows(
            Path("tenant-a".to_owned()),
            Query(WorkflowListQuery {
                routing_status: Some("sticky_incompatible_fallback_required".to_owned()),
                ..WorkflowListQuery::default()
            }),
            State(state),
        )
        .await
        .expect("list workflows response");

        assert_eq!(response.workflow_count, 1);
        assert_eq!(response.items[0].instance_id, "instance-b");
        assert_eq!(response.items[0].routing_status, "sticky_incompatible_fallback_required");
        Ok(())
    }

    #[tokio::test]
    async fn list_endpoints_support_alpha_memo_and_search_attribute_filters() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let state = test_state(store.clone()).await?;

        let memo = json!({"region": "eu", "priority": 1});
        let search_attributes = json!({"CustomKeywordField": ["vip", "alpha"], "Region": "eu"});
        let now = Utc::now();

        store
            .put_run_start(
                "tenant-a",
                "instance-1",
                "run-1",
                "payments",
                Some(1),
                Some("artifact-a"),
                "payments",
                Some(&memo),
                Some(&search_attributes),
                Uuid::now_v7(),
                now,
                None,
                None,
            )
            .await?;
        store
            .upsert_instance(&WorkflowInstanceState {
                tenant_id: "tenant-a".to_owned(),
                instance_id: "instance-1".to_owned(),
                run_id: "run-1".to_owned(),
                definition_id: "payments".to_owned(),
                definition_version: Some(1),
                artifact_hash: Some("artifact-a".to_owned()),
                workflow_task_queue: "payments".to_owned(),
                sticky_workflow_build_id: None,
                sticky_workflow_poller_id: None,
                current_state: Some("charge-card".to_owned()),
                context: None,
                artifact_execution: None,
                status: WorkflowStatus::Running,
                input: Some(json!({"orderId": 1})),
                persisted_input_handle: None,
                memo: Some(memo.clone()),
                search_attributes: Some(search_attributes.clone()),
                output: None,
                event_count: 3,
                last_event_id: Uuid::now_v7(),
                last_event_type: "ActivityScheduled".to_owned(),
                updated_at: now,
            })
            .await?;

        let Json(workflows) = list_workflows(
            Path("tenant-a".to_owned()),
            Query(WorkflowListQuery {
                memo_key: Some("region".to_owned()),
                memo_value: Some("eu".to_owned()),
                search_attribute_key: Some("CustomKeywordField".to_owned()),
                search_attribute_value: Some("vip".to_owned()),
                ..WorkflowListQuery::default()
            }),
            State(state.clone()),
        )
        .await
        .expect("workflow filter response");
        assert_eq!(workflows.workflow_count, 1);
        assert_eq!(workflows.items[0].memo.as_ref(), Some(&memo));
        assert_eq!(workflows.items[0].search_attributes.as_ref(), Some(&search_attributes));

        let Json(runs) = list_runs(
            Path("tenant-a".to_owned()),
            Query(RunListQuery {
                memo_key: Some("region".to_owned()),
                memo_value: Some("eu".to_owned()),
                search_attribute_key: Some("Region".to_owned()),
                search_attribute_value: Some("eu".to_owned()),
                ..RunListQuery::default()
            }),
            State(state),
        )
        .await
        .expect("run filter response");
        assert_eq!(runs.run_count, 1);
        assert_eq!(runs.items[0].memo.as_ref(), Some(&memo));
        assert_eq!(runs.items[0].search_attributes.as_ref(), Some(&search_attributes));
        Ok(())
    }
}
