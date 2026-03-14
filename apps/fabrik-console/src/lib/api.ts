export type PageInfo = {
  limit: number;
  offset: number;
  returned: number;
  total: number;
  has_more: boolean;
  next_offset: number | null;
};

export type WorkflowListItem = {
  tenant_id: string;
  instance_id: string;
  run_id: string;
  definition_id: string;
  definition_version: number | null;
  artifact_hash: string | null;
  workflow_task_queue: string;
  memo: unknown;
  search_attributes: unknown;
  sticky_workflow_build_id: string | null;
  sticky_workflow_poller_id: string | null;
  routing_status: string;
  current_state: string | null;
  status: string;
  event_count: number;
  last_event_id: string;
  last_event_type: string;
  updated_at: string;
  consistency: string;
  source: string;
};

export type WorkflowListResponse = {
  tenant_id: string;
  consistency: string;
  authoritative_source: string;
  page: PageInfo;
  workflow_count: number;
  items: WorkflowListItem[];
};

export type OverviewTaskQueueSummary = {
  queue_kind: string;
  task_queue: string;
  backlog: number;
  poller_count: number;
  registered_build_count: number;
  default_set_id: string | null;
  throughput_backend: string | null;
  oldest_backlog_at: string | null;
};

export type OverviewSummary = {
  tenant_id: string;
  consistency: string;
  authoritative_source: string;
  total_workflows: number;
  total_workflow_definitions: number;
  total_workflow_artifacts: number;
  counts_by_status: Record<string, number>;
  total_task_queues: number;
  total_backlog: number;
  total_pollers: number;
  total_registered_builds: number;
  replay_divergence_count: number;
  recent_failures: WorkflowListItem[];
  hottest_task_queues: OverviewTaskQueueSummary[];
};

export type WorkflowInstance = {
  tenant_id: string;
  instance_id: string;
  run_id: string;
  definition_id: string;
  definition_version: number | null;
  artifact_hash: string | null;
  workflow_task_queue: string;
  sticky_workflow_build_id: string | null;
  sticky_workflow_poller_id: string | null;
  current_state: string | null;
  context: unknown;
  status: string;
  input: unknown;
  memo: unknown;
  search_attributes: unknown;
  output: unknown;
  event_count: number;
  last_event_id: string;
  last_event_type: string;
  updated_at: string;
};

export type WorkflowHistoryEvent = {
  event_id: string;
  event_type: string;
  occurred_at: string;
  metadata: Record<string, string>;
  payload: unknown;
};

export type WorkflowHistoryResponse = {
  tenant_id: string;
  instance_id: string;
  run_id: string;
  definition_id: string;
  definition_version: number;
  artifact_hash: string;
  previous_run_id: string | null;
  next_run_id: string | null;
  continue_reason: string | null;
  event_count: number;
  page: PageInfo;
  activity_attempt_count: number;
  activity_attempts: WorkflowActivity[];
  events: WorkflowHistoryEvent[];
};

export type WorkflowActivity = {
  activity_id: string;
  attempt: number;
  activity_type: string;
  task_queue: string;
  state: string | null;
  status: string;
  worker_id: string | null;
  worker_build_id: string | null;
  scheduled_at: string;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
};

export type WorkflowActivitiesResponse = {
  tenant_id: string;
  instance_id: string;
  run_id: string;
  page: PageInfo;
  activity_count: number;
  activities: WorkflowActivity[];
};

export type WorkflowBulkBatch = {
  tenant_id: string;
  instance_id: string;
  run_id: string;
  definition_id: string;
  definition_version: number | null;
  artifact_hash: string | null;
  batch_id: string;
  activity_type: string;
  task_queue: string;
  state: string | null;
  input_handle: unknown;
  result_handle: unknown;
  throughput_backend: string;
  throughput_backend_version: string;
  execution_mode: string;
  selected_backend: string;
  routing_reason: string;
  admission_policy_version: string;
  execution_policy: string | null;
  reducer: string | null;
  reducer_kind: string;
  reducer_class: string;
  reducer_version: string;
  reducer_execution_path: string;
  aggregation_tree_depth: number;
  fast_lane_enabled: boolean;
  aggregation_group_count: number;
  status: string;
  total_items: number;
  chunk_size: number;
  chunk_count: number;
  succeeded_items: number;
  failed_items: number;
  cancelled_items: number;
  max_attempts: number;
  retry_delay_ms: number;
  error: string | null;
  reducer_output: unknown | null;
  scheduled_at: string;
  terminal_at: string | null;
  updated_at: string;
};

export type WorkflowBulkBatchRuntimeControl = {
  tenant_id: string;
  instance_id: string;
  run_id: string;
  batch_id: string;
  is_paused: boolean;
  reason: string | null;
  created_at: string;
  updated_at: string;
};

export type WorkflowBulkBatchesResponse = {
  tenant_id: string;
  instance_id: string;
  run_id: string;
  consistency: string;
  authoritative_source: string;
  projection_lag_ms: number | null;
  watch_cursor: string;
  page: PageInfo;
  batch_count: number;
  batches: WorkflowBulkBatch[];
};

export type WorkflowBulkBatchResponse = {
  tenant_id: string;
  instance_id: string;
  run_id: string;
  consistency: string;
  authoritative_source: string;
  projection_lag_ms: number | null;
  watch_cursor: string;
  runtime_control: WorkflowBulkBatchRuntimeControl | null;
  batch: WorkflowBulkBatch;
};

export type WorkflowSignal = {
  signal_id: string;
  signal_type: string;
  status: string;
  enqueued_at: string;
  consumed_at: string | null;
};

export type WorkflowSignalsResponse = {
  tenant_id: string;
  instance_id: string;
  run_id: string;
  page: PageInfo;
  signal_count: number;
  signals: WorkflowSignal[];
};

export type WorkflowRun = {
  run_id: string;
  previous_run_id: string | null;
  next_run_id: string | null;
  continue_reason: string | null;
  workflow_task_queue: string;
  memo: unknown;
  search_attributes: unknown;
  sticky_workflow_build_id: string | null;
  sticky_workflow_poller_id: string | null;
  started_at: string;
  closed_at: string | null;
};

export type WorkflowRunsResponse = {
  tenant_id: string;
  instance_id: string;
  current_run_id: string | null;
  page: PageInfo;
  run_count: number;
  runs: WorkflowRun[];
};

export type RunListItem = {
  tenant_id: string;
  instance_id: string;
  run_id: string;
  definition_id: string;
  definition_version: number | null;
  artifact_hash: string | null;
  workflow_task_queue: string;
  memo: unknown;
  search_attributes: unknown;
  sticky_workflow_build_id: string | null;
  sticky_workflow_poller_id: string | null;
  routing_status: string;
  sticky_updated_at: string | null;
  previous_run_id: string | null;
  next_run_id: string | null;
  continue_reason: string | null;
  started_at: string;
  closed_at: string | null;
  updated_at: string;
  last_transition_at: string;
  status: string;
  current_state: string | null;
  last_event_type: string | null;
  event_count: number | null;
  consistency: string;
  source: string;
};

export type RunListResponse = {
  tenant_id: string;
  consistency: string;
  authoritative_source: string;
  page: PageInfo;
  run_count: number;
  items: RunListItem[];
};

export type WorkflowReplayResponse = {
  tenant_id: string;
  instance_id: string;
  run_id: string;
  definition_id: string;
  definition_version: number;
  artifact_hash: string;
  divergence_count: number;
  divergences: Array<{ kind: string; path?: string | null; message: string }>;
  transition_count: number;
  replay_source: unknown;
  projection_matches_store: boolean | null;
  snapshot?: {
    run_id: string;
    snapshot_schema_version: number;
    event_count: number;
    last_event_id: string;
    last_event_type: string;
    updated_at: string;
  } | null;
  replayed_state?: {
    workflow_task_queue?: string;
    memo?: unknown;
    search_attributes?: unknown;
    status?: string;
    current_state?: string | null;
  };
};

export type WorkflowRoutingResponse = {
  tenant_id: string;
  instance_id: string;
  run_id: string;
  definition_id: string;
  definition_version: number | null;
  artifact_hash: string | null;
  workflow_task_queue: string;
  routing_status: string;
  default_compatibility_set_id: string | null;
  compatible_build_ids: string[];
  registered_build_ids: string[];
  sticky_workflow_build_id: string | null;
  sticky_workflow_poller_id: string | null;
  sticky_updated_at: string | null;
  sticky_build_compatible_with_queue: boolean | null;
  sticky_build_supports_pinned_artifact: boolean | null;
};

export type ExecutionGraphResponse = {
  tenant_id: string;
  instance_id: string;
  run_id: string;
  consistency: string;
  authoritative_source: string;
  nodes: Array<{ id: string; kind: string; label: string; status: string; subtitle: string | null }>;
  edges: Array<{ id: string; source: string; target: string; label: string }>;
};

export type TaskQueueSummary = {
  tenant_id: string;
  queue_kind: string;
  task_queue: string;
  backlog: number;
  oldest_backlog_at: string | null;
  poller_count: number;
  registered_build_count: number;
  default_set_id: string | null;
  throughput_backend: string | null;
  effective_throughput_backend: string | null;
  is_paused: boolean;
  is_draining: boolean;
  stream_v2_capacity_state: string | null;
  active_stream_v2_chunks: number | null;
  sticky_hit_rate: number | null;
  consistency: string;
  source: string;
};

export type TaskQueueListResponse = {
  tenant_id: string;
  consistency: string;
  authoritative_source: string;
  queue_count: number;
  items: TaskQueueSummary[];
};

export type TaskQueueInspection = {
  tenant_id: string;
  queue_kind: string;
  task_queue: string;
  backlog: number;
  oldest_backlog_at: string | null;
  sticky_effectiveness: { sticky_hit_rate: number; sticky_fallback_rate: number } | null;
  resume_coalescing: { resume_rows_per_task_row: number } | null;
  activity_completion_metrics: {
    completed: number;
    failed: number;
    cancelled: number;
    timed_out: number;
    avg_schedule_to_start_latency_ms: number;
    avg_start_to_close_latency_ms: number;
  } | null;
  default_set_id: string | null;
  throughput_policy: { backend: string } | null;
  admission: {
    configured_default_backend: string;
    configured_task_queue_backend: string | null;
    persisted_task_queue_backend: string | null;
    effective_preferred_backend: string;
    stream_v2_active_chunks: {
      tenant: number;
      task_queue: number;
    };
    stream_v2_capacity: {
      tenant_limit: number;
      task_queue_limit: number;
      tenant_utilization: number | null;
      task_queue_utilization: number | null;
      state: string;
    };
    nonterminal_backend_counts: Record<string, number>;
    nonterminal_routing_reason_counts: Record<string, number>;
    nonterminal_admission_policy_versions: Record<string, number>;
  } | null;
  runtime_control: {
    tenant_id: string;
    queue_kind: string;
    task_queue: string;
    is_paused: boolean;
    is_draining: boolean;
    reason: string | null;
    created_at: string;
    updated_at: string;
  } | null;
  compatible_build_ids: string[];
  registered_builds: Array<{ build_id: string; artifact_hashes: string[]; updated_at: string }>;
  compatibility_sets: Array<{ set_id: string; build_ids: string[]; is_default: boolean; updated_at: string }>;
  pollers: Array<{ poller_id: string; build_id: string; partition_id: number | null; last_seen_at: string; expires_at: string }>;
  watch_cursor?: string;
};

export type TopicAdapter = {
  tenant_id: string;
  adapter_id: string;
  adapter_kind: string;
  brokers: string;
  topic_name: string;
  topic_partitions: number;
  action: string;
  definition_id: string | null;
  signal_type: string | null;
  workflow_task_queue: string | null;
  workflow_instance_id_json_pointer: string | null;
  payload_json_pointer: string | null;
  payload_template_json: unknown | null;
  memo_json_pointer: string | null;
  memo_template_json: unknown | null;
  search_attributes_json_pointer: string | null;
  search_attributes_template_json: unknown | null;
  request_id_json_pointer: string | null;
  dedupe_key_json_pointer: string | null;
  dead_letter_policy: string;
  is_paused: boolean;
  processed_count: number;
  failed_count: number;
  ownership_handoff_count: number;
  last_processed_at: string | null;
  last_handoff_at: string | null;
  last_takeover_latency_ms: number | null;
  last_error: string | null;
  last_error_at: string | null;
  created_at: string;
  updated_at: string;
};

export type TopicAdapterOffset = {
  tenant_id: string;
  adapter_id: string;
  partition_id: number;
  log_offset: number;
  updated_at: string;
};

export type TopicAdapterDeadLetter = {
  tenant_id: string;
  adapter_id: string;
  partition_id: number;
  log_offset: number;
  record_key: string | null;
  payload: unknown;
  error: string;
  occurred_at: string;
  updated_at: string;
};

export type TopicAdapterOwnership = {
  tenant_id: string;
  adapter_id: string;
  owner_id: string;
  owner_epoch: number;
  lease_expires_at: string;
  acquired_at: string;
  last_transition_at: string;
  updated_at: string;
};

export type TopicAdapterLag = {
  available: boolean;
  error?: string | null;
  total_lag_records: number | null;
  partitions: Array<{
    partition_id: number;
    committed_offset: number | null;
    latest_offset: number;
    lag_records: number;
  }>;
};

export type TopicAdapterDetailResponse = {
  adapter: TopicAdapter;
  offsets: TopicAdapterOffset[];
  recent_dead_letters: TopicAdapterDeadLetter[];
  ownership: TopicAdapterOwnership | null;
  lag: TopicAdapterLag;
  watch_cursor?: string;
};

export type TopicAdapterDeleteResponse = {
  tenant_id: string;
  adapter_id: string;
  deleted: boolean;
  forced: boolean;
};

export type TopicAdapterPreviewResponse = {
  ok: boolean;
  dispatch: unknown | null;
  diagnostics: Array<{
    field: string;
    mode: string;
    detail: string;
  }>;
  error: {
    field: string;
    detail: string;
  } | null;
};

export type TopicAdapterUpsertRequest = {
  adapter_kind: string;
  brokers: string;
  topic_name: string;
  topic_partitions: number;
  action: string;
  definition_id?: string | null;
  signal_type?: string | null;
  workflow_task_queue?: string | null;
  workflow_instance_id_json_pointer?: string | null;
  payload_json_pointer?: string | null;
  payload_template_json?: unknown | null;
  memo_json_pointer?: string | null;
  memo_template_json?: unknown | null;
  search_attributes_json_pointer?: string | null;
  search_attributes_template_json?: unknown | null;
  request_id_json_pointer?: string | null;
  dedupe_key_json_pointer?: string | null;
  dead_letter_policy?: string;
  is_paused?: boolean;
};

export type WatchEvent<T = unknown> = {
  event_type: string;
  occurred_at: string;
  consistency: string;
  authoritative_source: string;
  projection_lag_ms: number | null;
  owner_id: string | null;
  partition_id: number | null;
  cursor: string;
  body: T;
};

export type SearchResult = {
  kind: string;
  id: string;
  title: string;
  subtitle: string;
  href: string;
  tenant_id: string;
};

export type DefinitionSummary = {
  tenant_id: string;
  workflow_id: string;
  latest_version: number;
  active_version: number | null;
  version_count: number;
  updated_at: string;
};

export type ArtifactSummary = DefinitionSummary;

export type WorkflowDefinition = {
  id: string;
  version: number;
  initial_state: string;
  states: Record<string, unknown>;
};

export type CompiledWorkflowArtifact = {
  definition_id: string;
  definition_version: number;
  compiler_version: string;
  source_language: string;
  entrypoint: { module: string; export: string };
  source_files: string[];
  source_map: Record<string, { file: string; line: number; column: number }>;
  queries: Record<string, unknown>;
  signals: Record<string, unknown>;
  updates: Record<string, unknown>;
  workflow: { initial_state: string; states: Record<string, unknown> };
  artifact_hash: string;
};

export type ArtifactValidationFailure = {
  instance_id: string;
  run_id: string;
  message: string;
  divergence: { kind: string; message: string; fields?: Array<{ field: string }> } | null;
};

export type ValidateWorkflowArtifactResponse = {
  tenant_id: string;
  definition_id: string;
  version: number;
  artifact_hash: string;
  compatible: boolean;
  status: string;
  validation: {
    enabled: boolean;
    validated_run_count: number;
    skipped_run_count: number;
    failed_run_count: number;
    failures: ArtifactValidationFailure[];
  };
};

export type TrustSummaryReport = {
  layer_id: string;
  title: string;
  purpose: string;
  case_count: number;
  passed_count: number;
  failed_count: number;
  report_path: string;
  public_report_path: string;
  failed_cases: Array<{
    id: string;
    title: string;
    summary: string;
    href: string;
  }>;
};

export type TrustSummary = {
  schema_version: number;
  milestone_scope: string;
  title: string;
  goal: string;
  generated_at: string;
  status: string;
  trusted_confidence_floor: string;
  upgrade_confidence_floor: string;
  reports: TrustSummaryReport[];
  confidence_bands: Array<{
    confidence_class: string;
    count: number;
    features: string[];
  }>;
  confidence_deltas?: Array<{
    feature: string;
    label: string;
    declared_confidence_class: string;
    derived_confidence_class: string;
    confidence_status: string;
  }>;
  features?: Array<{
    feature: string;
    label: string;
    milestone_status: string;
    confidence_class: string;
    declared_confidence_class?: string;
    confidence_status?: string;
    evidence?: {
      support_layer_status?: string;
      semantic_layer_status?: string;
      trust_layer_status?: string;
      upgrade_layer_status?: string;
    };
  }>;
  headline_trusted_features: string[];
  blocked_features: string[];
  promotion_requirements: string[];
  support_matrix_public_path?: string;
};

export type ConformanceCaseResult = {
  id: string;
  title: string;
  kind: string;
  status: string;
  duration_ms: number;
  summary?: string;
  error?: string;
  observed?: Record<string, unknown>;
  evidence?: {
    stdout_excerpt?: string | null;
    stderr_excerpt?: string | null;
    combined_excerpt?: string | null;
    source_files?: string[];
    state_count?: number;
    finding_count?: number;
    finding_features?: string[];
  };
};

export type ConformanceReport = {
  schema_version: number;
  layer_id: string;
  title: string;
  purpose: string;
  case_count: number;
  passed_count: number;
  failed_count: number;
  results: ConformanceCaseResult[];
};

export type WorkflowGraphSourceAnchor = {
  file: string;
  line: number;
  column: number;
};

export type WorkflowGraphNode = {
  id: string;
  graph: string;
  module_id: string;
  state_id: string | null;
  kind: string;
  label: string;
  subtitle: string | null;
  source_anchor: WorkflowGraphSourceAnchor | null;
  next_ids: string[];
  raw: unknown;
};

export type WorkflowGraphEdge = {
  id: string;
  source: string;
  target: string;
  label: string;
  kind: string;
};

export type WorkflowGraphModule = {
  id: string;
  graph: string;
  kind: string;
  label: string;
  subtitle: string | null;
  node_ids: string[];
  state_ids: string[];
  focus_node_id: string;
  collapsed_by_default: boolean;
  source_anchor: WorkflowGraphSourceAnchor | null;
  raw: unknown;
};

export type WorkflowGraphOverlayStatus = {
  id: string;
  status: string;
  summary: string | null;
};

export type WorkflowGraphBlockedBy = {
  kind: string;
  label: string;
  detail: string | null;
  node_id: string | null;
  module_id: string | null;
};

export type WorkflowGraphTraceStep = {
  id: string;
  occurred_at: string;
  lane: string;
  label: string;
  detail: string | null;
  event_type: string;
  node_id: string | null;
  module_id: string | null;
};

export type WorkflowGraphActivitySummary = {
  node_id: string;
  module_id: string;
  activity_type: string;
  total: number;
  pending: number;
  completed: number;
  failed: number;
  retrying: number;
  worker_build_ids: string[];
};

export type WorkflowGraphBulkSummary = {
  node_id: string;
  module_id: string;
  batch_id: string;
  activity_type: string;
  status: string;
  total_items: number;
  succeeded_items: number;
  failed_items: number;
  cancelled_items: number;
};

export type WorkflowGraphSignalSummary = {
  signal_name: string;
  count: number;
  latest_status: string;
  latest_seen_at: string;
  node_id: string | null;
  module_id: string | null;
};

export type WorkflowGraphUpdateSummary = {
  update_name: string;
  count: number;
  latest_status: string;
  latest_seen_at: string;
  module_id: string | null;
};

export type WorkflowGraphChildSummary = {
  child_id: string;
  child_definition_id: string;
  child_workflow_id: string;
  child_run_id: string | null;
  status: string;
  node_id: string | null;
  module_id: string | null;
};

export type WorkflowGraphOverlay = {
  mode: string;
  run_id: string | null;
  current_node_id: string | null;
  current_module_id: string | null;
  blocked_by: WorkflowGraphBlockedBy | null;
  node_statuses: WorkflowGraphOverlayStatus[];
  module_statuses: WorkflowGraphOverlayStatus[];
  trace: WorkflowGraphTraceStep[];
  activity_summaries: WorkflowGraphActivitySummary[];
  bulk_summaries: WorkflowGraphBulkSummary[];
  signal_summaries: WorkflowGraphSignalSummary[];
  update_summaries: WorkflowGraphUpdateSummary[];
  child_summaries: WorkflowGraphChildSummary[];
};

export type WorkflowGraphResponse = {
  tenant_id: string;
  definition_id: string;
  definition_version: number;
  artifact_hash: string;
  entrypoint_module: string;
  entrypoint_export: string;
  consistency: string;
  authoritative_source: string;
  source_files: string[];
  nodes: WorkflowGraphNode[];
  edges: WorkflowGraphEdge[];
  modules: WorkflowGraphModule[];
  module_edges: WorkflowGraphEdge[];
  overlay: WorkflowGraphOverlay;
};

type SearchResponse = {
  tenant_id: string;
  items: SearchResult[];
};

type TenantListResponse = {
  tenants: string[];
};

type DefinitionSummariesResponse = {
  tenant_id: string;
  items: DefinitionSummary[];
};

type ArtifactSummariesResponse = {
  tenant_id: string;
  items: ArtifactSummary[];
};

function resolveDefaultApiBase(): string {
  const configuredBase = (import.meta.env.VITE_API_BASE_URL as string | undefined)?.trim();
  if (configuredBase) {
    return configuredBase;
  }
  return "";
}

const DEFAULT_API_BASE = resolveDefaultApiBase();

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${DEFAULT_API_BASE}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {})
    }
  });
  if (!response.ok) {
    throw new Error(await response.text());
  }
  return (await response.json()) as T;
}

export const api = {
  listTenants: () => request<TenantListResponse>("/admin/tenants"),
  getOverview: (tenantId: string) => request<OverviewSummary>(`/tenants/${tenantId}/overview`),
  listWorkflows: (tenantId: string, params = new URLSearchParams()) =>
    request<WorkflowListResponse>(`/tenants/${tenantId}/workflows${params.toString() ? `?${params.toString()}` : ""}`),
  listRuns: (tenantId: string, params = new URLSearchParams()) =>
    request<RunListResponse>(`/tenants/${tenantId}/runs${params.toString() ? `?${params.toString()}` : ""}`),
  getWorkflow: (tenantId: string, instanceId: string) => request<WorkflowInstance>(`/tenants/${tenantId}/workflows/${instanceId}`),
  getWorkflowRuns: (tenantId: string, instanceId: string, params = new URLSearchParams()) =>
    request<WorkflowRunsResponse>(
      `/tenants/${tenantId}/workflows/${instanceId}/runs${params.toString() ? `?${params.toString()}` : ""}`
    ),
  getWorkflowHistory: (tenantId: string, instanceId: string, runId?: string, offset = 0) =>
    request<WorkflowHistoryResponse>(
      runId
        ? `/tenants/${tenantId}/workflows/${instanceId}/runs/${runId}/history?offset=${offset}`
        : `/tenants/${tenantId}/workflows/${instanceId}/history?offset=${offset}`
    ),
  getWorkflowActivities: (tenantId: string, instanceId: string, runId?: string) =>
    request<WorkflowActivitiesResponse>(
      runId
        ? `/tenants/${tenantId}/workflows/${instanceId}/runs/${runId}/activities`
        : `/tenants/${tenantId}/workflows/${instanceId}/activities`
    ),
  getWorkflowBulkBatches: (tenantId: string, instanceId: string, runId: string, consistency = "eventual") =>
    request<WorkflowBulkBatchesResponse>(
      `/tenants/${tenantId}/workflows/${instanceId}/runs/${runId}/bulk-batches?consistency=${consistency}`
    ),
  getWorkflowBulkBatch: (
    tenantId: string,
    instanceId: string,
    runId: string,
    batchId: string,
    consistency = "eventual"
  ) =>
    request<WorkflowBulkBatchResponse>(
      `/tenants/${tenantId}/workflows/${instanceId}/runs/${runId}/bulk-batches/${batchId}?consistency=${consistency}`
    ),
  getWorkflowSignals: (tenantId: string, instanceId: string, runId?: string) =>
    request<WorkflowSignalsResponse>(
      runId
        ? `/tenants/${tenantId}/workflows/${instanceId}/runs/${runId}/signals`
        : `/tenants/${tenantId}/workflows/${instanceId}/signals`
    ),
  getWorkflowReplay: (tenantId: string, instanceId: string, runId?: string) =>
    request<WorkflowReplayResponse>(
      runId
        ? `/tenants/${tenantId}/workflows/${instanceId}/runs/${runId}/replay`
        : `/tenants/${tenantId}/workflows/${instanceId}/replay`
    ),
  getWorkflowRouting: (tenantId: string, instanceId: string) =>
    request<WorkflowRoutingResponse>(`/tenants/${tenantId}/workflows/${instanceId}/routing`),
  getExecutionGraph: (tenantId: string, instanceId: string) =>
    request<ExecutionGraphResponse>(`/tenants/${tenantId}/workflows/${instanceId}/execution-graph`),
  listTaskQueues: (tenantId: string) => request<TaskQueueListResponse>(`/admin/tenants/${tenantId}/task-queues`),
  getTaskQueue: (tenantId: string, queueKind: string, taskQueue: string) =>
    request<TaskQueueInspection>(`/admin/tenants/${tenantId}/task-queues/${queueKind}/${taskQueue}`),
  listTopicAdapters: (tenantId: string) => request<TopicAdapter[]>(`/admin/tenants/${tenantId}/topic-adapters`),
  getTopicAdapter: (tenantId: string, adapterId: string) =>
    request<TopicAdapterDetailResponse>(`/admin/tenants/${tenantId}/topic-adapters/${adapterId}`),
  setTopicAdapter: (tenantId: string, adapterId: string, payload: TopicAdapterUpsertRequest) =>
    request<TopicAdapter>(`/admin/tenants/${tenantId}/topic-adapters/${adapterId}`, {
      method: "PUT",
      body: JSON.stringify(payload)
    }),
  deleteTopicAdapter: (tenantId: string, adapterId: string, force = false) =>
    request<TopicAdapterDeleteResponse>(
      `/admin/tenants/${tenantId}/topic-adapters/${adapterId}${force ? "?force=true" : ""}`,
      {
        method: "DELETE"
      }
    ),
  pauseTopicAdapter: (tenantId: string, adapterId: string) =>
    request<TopicAdapter>(`/admin/tenants/${tenantId}/topic-adapters/${adapterId}/pause`, {
      method: "POST",
      body: JSON.stringify({})
    }),
  resumeTopicAdapter: (tenantId: string, adapterId: string) =>
    request<TopicAdapter>(`/admin/tenants/${tenantId}/topic-adapters/${adapterId}/resume`, {
      method: "POST",
      body: JSON.stringify({})
    }),
  listTopicAdapterDeadLetters: (tenantId: string, adapterId: string, limit = 100, offset = 0) =>
    request<{ tenant_id: string; adapter_id: string; items: TopicAdapterDeadLetter[]; limit: number; offset: number }>(
      `/admin/tenants/${tenantId}/topic-adapters/${adapterId}/dead-letters?limit=${limit}&offset=${offset}`
    ),
  previewTopicAdapter: (
    tenantId: string,
    adapterId: string,
    payload: { payload: unknown; partition_id?: number; log_offset?: number }
  ) =>
    request<TopicAdapterPreviewResponse>(`/admin/tenants/${tenantId}/topic-adapters/${adapterId}/preview`, {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  previewTopicAdapterDraft: (
    tenantId: string,
    payload: {
      adapter_id?: string;
      adapter: TopicAdapterUpsertRequest;
      payload: unknown;
      partition_id?: number;
      log_offset?: number;
    }
  ) =>
    request<TopicAdapterPreviewResponse>(`/admin/tenants/${tenantId}/topic-adapters/preview-draft`, {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  topicAdapterWatchPath: (tenantId: string, adapterId: string) =>
    `${DEFAULT_API_BASE}/admin/tenants/${tenantId}/topic-adapters/${adapterId}/watch`,
  getTrustSummary: () => request<TrustSummary>("/conformance-reports/trust-summary.json"),
  getConformanceReport: (publicPath: string) => request<ConformanceReport>(publicPath),
  search: (tenantId: string, q: string, limit = 20) =>
    request<SearchResponse>(`/search?tenant_id=${tenantId}&q=${encodeURIComponent(q)}&limit=${limit}`),
  listDefinitionSummaries: (tenantId: string, q = "") =>
    request<DefinitionSummariesResponse>(`/tenants/${tenantId}/workflow-definitions${q ? `?q=${encodeURIComponent(q)}` : ""}`),
  listArtifactSummaries: (tenantId: string, q = "") =>
    request<ArtifactSummariesResponse>(`/tenants/${tenantId}/workflow-artifacts${q ? `?q=${encodeURIComponent(q)}` : ""}`),
  getLatestDefinition: (tenantId: string, definitionId: string) =>
    request<WorkflowDefinition>(`/tenants/${tenantId}/workflow-definitions/${definitionId}/latest`),
  getDefinitionGraph: (tenantId: string, definitionId: string) =>
    request<WorkflowGraphResponse>(`/tenants/${tenantId}/workflow-definitions/${definitionId}/graph`),
  getLatestArtifact: (tenantId: string, definitionId: string) =>
    request<CompiledWorkflowArtifact>(`/tenants/${tenantId}/workflow-artifacts/${definitionId}/latest`),
  validateWorkflowArtifact: (tenantId: string, artifact: CompiledWorkflowArtifact, validationRunLimit = 10) =>
    request<ValidateWorkflowArtifactResponse>(
      `/tenants/${tenantId}/workflow-artifacts/validate?validation_run_limit=${validationRunLimit}`,
      {
        method: "POST",
        body: JSON.stringify(artifact)
      }
    ),
  getRunGraph: (tenantId: string, instanceId: string, runId: string) =>
    request<WorkflowGraphResponse>(`/tenants/${tenantId}/workflows/${instanceId}/runs/${runId}/graph`),
  signalWorkflow: (tenantId: string, instanceId: string, signalType: string, payload: unknown) =>
    request(`/tenants/${tenantId}/workflows/${instanceId}/signals/${signalType}`, {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  terminateWorkflow: (tenantId: string, instanceId: string, payload: unknown) =>
    request(`/tenants/${tenantId}/workflows/${instanceId}/terminate`, {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  cancelActivity: (tenantId: string, instanceId: string, activityId: string, payload: unknown) =>
    request(`/tenants/${tenantId}/workflows/${instanceId}/activities/${activityId}/cancel`, {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  registerBuild: (tenantId: string, queueKind: string, taskQueue: string, payload: unknown) =>
    request(`/admin/tenants/${tenantId}/task-queues/${queueKind}/${taskQueue}/builds`, {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  setDefaultSet: (tenantId: string, queueKind: string, taskQueue: string, setId: string) =>
    request(`/admin/tenants/${tenantId}/task-queues/${queueKind}/${taskQueue}/default-set`, {
      method: "POST",
      body: JSON.stringify({ set_id: setId })
    }),
  setThroughputPolicy: (tenantId: string, queueKind: string, taskQueue: string, backend: string) =>
    request(`/admin/tenants/${tenantId}/task-queues/${queueKind}/${taskQueue}/throughput-policy`, {
      method: "PUT",
      body: JSON.stringify({ backend })
    }),
  setTaskQueueRuntimeControl: (
    tenantId: string,
    queueKind: string,
    taskQueue: string,
    payload: { is_paused: boolean; is_draining: boolean; reason?: string | null }
  ) =>
    request(`/admin/tenants/${tenantId}/task-queues/${queueKind}/${taskQueue}/runtime-control`, {
      method: "PUT",
      body: JSON.stringify(payload)
    }),
  setBulkBatchRuntimeControl: (
    tenantId: string,
    instanceId: string,
    runId: string,
    batchId: string,
    payload: { is_paused: boolean; reason?: string | null }
  ) =>
    request(
      `/admin/tenants/${tenantId}/workflows/${instanceId}/runs/${runId}/bulk-batches/${batchId}/runtime-control`,
      {
        method: "PUT",
        body: JSON.stringify(payload)
      }
    ),
  bulkBatchWatchPath: (tenantId: string, instanceId: string, runId: string, batchId: string) =>
    `${DEFAULT_API_BASE}/tenants/${tenantId}/workflows/${instanceId}/runs/${runId}/bulk-batches/${batchId}/watch`,
  workflowRunWatchPath: (tenantId: string, instanceId: string, runId: string) =>
    `${DEFAULT_API_BASE}/tenants/${tenantId}/workflows/${instanceId}/runs/${runId}/watch`,
  taskQueueWatchPath: (tenantId: string, queueKind: string, taskQueue: string) =>
    `${DEFAULT_API_BASE}/admin/tenants/${tenantId}/task-queues/${queueKind}/${taskQueue}/watch`
};
