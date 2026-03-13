import { useQuery, useQueryClient } from "@tanstack/react-query";
import { FormEvent, useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { toast } from "sonner";

import { WorkflowGraphExplorer } from "../components/graph/workflow-graph-explorer";
import { Badge, ConsistencyBadge, Panel } from "../components/ui";
import { api } from "../lib/api";
import { formatDate, formatDuration, formatInlineValue, formatNumber } from "../lib/format";
import { useTenant } from "../lib/tenant-context";

const TABS = ["summary", "graph", "timeline", "activities", "raw-history"] as const;
type Tab = (typeof TABS)[number];

type TimelineLane = "lifecycle" | "activities" | "messages" | "timers" | "infra" | "other";

type LiveBatchRoutingSignal = {
  batch_id: string;
  task_queue: string | null;
  selected_backend: string | null;
  routing_reason: string | null;
  admission_policy_version: string | null;
  status: string | null;
};

type LiveBatchCapacitySignal = {
  batch_id: string;
  task_queue: string | null;
  selected_backend: string | null;
  routing_reason: string | null;
  admission: {
    stream_v2_capacity?: {
      state?: string | null;
      tenant_utilization?: number | null;
      task_queue_utilization?: number | null;
    };
  } | null;
};

function parseWatchBody<T>(event: MessageEvent<string>): T | null {
  try {
    const parsed = JSON.parse(event.data) as { body?: T };
    return parsed.body ?? null;
  } catch {
    return null;
  }
}

function metadataPreview(value: unknown) {
  if (value == null || typeof value !== "object" || Array.isArray(value)) return "-";
  const entries = Object.entries(value as Record<string, unknown>);
  if (entries.length === 0) return "-";
  return entries
    .slice(0, 4)
    .map(([key, entryValue]) =>
      `${key}=${
        Array.isArray(entryValue)
          ? entryValue.join("|")
          : typeof entryValue === "object"
            ? JSON.stringify(entryValue)
            : String(entryValue)
      }`
    )
    .join(" · ");
}

function eventLane(eventType: string): TimelineLane {
  const lower = eventType.toLowerCase();
  if (lower.includes("activity")) return "activities";
  if (lower.includes("signal") || lower.includes("query") || lower.includes("update")) return "messages";
  if (lower.includes("timer")) return "timers";
  if (lower.includes("ownership") || lower.includes("snapshot") || lower.includes("replay")) return "infra";
  if (lower.includes("workflow")) return "lifecycle";
  return "other";
}

export function WorkflowDetailPage() {
  const { instanceId = "", runId } = useParams();
  const { tenantId } = useTenant();
  const client = useQueryClient();
  const [activeTab, setActiveTab] = useState<Tab>("summary");
  const [signalType, setSignalType] = useState("operator.ping");
  const [signalPayload, setSignalPayload] = useState("{\"ok\":true}");
  const [terminateReason, setTerminateReason] = useState("terminated by operator");
  const [laneFilter, setLaneFilter] = useState<"all" | TimelineLane>("all");
  const [windowPreset, setWindowPreset] = useState<"all" | "recent-50" | "recent-20">("all");
  const [selectedBatchId, setSelectedBatchId] = useState("");
  const [batchControlPending, setBatchControlPending] = useState(false);
  const [liveBatchRouting, setLiveBatchRouting] = useState<LiveBatchRoutingSignal | null>(null);
  const [liveBatchCapacity, setLiveBatchCapacity] = useState<LiveBatchCapacitySignal | null>(null);

  const workflowQuery = useQuery({
    queryKey: ["workflow", tenantId, instanceId],
    enabled: tenantId !== "" && instanceId !== "",
    queryFn: () => api.getWorkflow(tenantId, instanceId)
  });
  const runListQuery = useQuery({
    queryKey: ["run-detail-runs", tenantId, instanceId],
    enabled: tenantId !== "" && instanceId !== "",
    queryFn: () => {
      const params = new URLSearchParams({ instance_id: instanceId, limit: "200" });
      return api.listRuns(tenantId, params);
    }
  });

  const resolvedRunId = runId ?? workflowQuery.data?.run_id ?? "";
  const historyQuery = useQuery({
    queryKey: ["run-history", tenantId, instanceId, resolvedRunId],
    enabled: tenantId !== "" && instanceId !== "" && resolvedRunId !== "",
    queryFn: () => api.getWorkflowHistory(tenantId, instanceId, resolvedRunId)
  });
  const activitiesQuery = useQuery({
    queryKey: ["run-activities", tenantId, instanceId, resolvedRunId],
    enabled: tenantId !== "" && instanceId !== "" && resolvedRunId !== "",
    queryFn: () => api.getWorkflowActivities(tenantId, instanceId, resolvedRunId)
  });
  const bulkBatchesQuery = useQuery({
    queryKey: ["run-bulk-batches", tenantId, instanceId, resolvedRunId],
    enabled: tenantId !== "" && instanceId !== "" && resolvedRunId !== "",
    queryFn: () => api.getWorkflowBulkBatches(tenantId, instanceId, resolvedRunId)
  });
  const bulkBatchDetailQuery = useQuery({
    queryKey: ["run-bulk-batch", tenantId, instanceId, resolvedRunId, selectedBatchId],
    enabled: tenantId !== "" && instanceId !== "" && resolvedRunId !== "" && selectedBatchId !== "",
    queryFn: () => api.getWorkflowBulkBatch(tenantId, instanceId, resolvedRunId, selectedBatchId, "strong")
  });
  const graphQuery = useQuery({
    queryKey: ["run-graph", tenantId, instanceId, resolvedRunId],
    enabled: tenantId !== "" && instanceId !== "" && resolvedRunId !== "",
    queryFn: () => api.getRunGraph(tenantId, instanceId, resolvedRunId)
  });
  const routingQuery = useQuery({
    queryKey: ["workflow-routing", tenantId, instanceId],
    enabled: tenantId !== "" && instanceId !== "",
    queryFn: () => api.getWorkflowRouting(tenantId, instanceId)
  });
  const replayQuery = useQuery({
    queryKey: ["workflow-replay", tenantId, instanceId, resolvedRunId],
    enabled: tenantId !== "" && instanceId !== "" && resolvedRunId !== "",
    queryFn: () => api.getWorkflowReplay(tenantId, instanceId, resolvedRunId)
  });

  const runs = useMemo(
    () => [...(runListQuery.data?.items ?? [])].sort((left, right) => left.started_at.localeCompare(right.started_at)),
    [runListQuery.data?.items]
  );
  const selectedRun = useMemo(() => {
    if (resolvedRunId) {
      return runs.find((item) => item.run_id === resolvedRunId) ?? null;
    }
    return runs.find((item) => item.run_id === workflowQuery.data?.run_id) ?? null;
  }, [resolvedRunId, runs, workflowQuery.data?.run_id]);
  const isCurrentRun = selectedRun?.run_id != null && selectedRun.run_id === workflowQuery.data?.run_id;
  const canMutate = Boolean(isCurrentRun && workflowQuery.data && !["completed", "failed", "cancelled", "terminated"].includes(workflowQuery.data.status));

  const activeActivityIds = useMemo(
    () => (activitiesQuery.data?.activities ?? []).filter((activity) => ["scheduled", "started"].includes(activity.status)).map((activity) => activity.activity_id),
    [activitiesQuery.data?.activities]
  );

  useEffect(() => {
    const firstBatchId = bulkBatchesQuery.data?.batches?.[0]?.batch_id ?? "";
    if (selectedBatchId === "" && firstBatchId !== "") {
      setSelectedBatchId(firstBatchId);
    }
    if (
      selectedBatchId !== "" &&
      bulkBatchesQuery.data?.batches &&
      !bulkBatchesQuery.data.batches.some((batch) => batch.batch_id === selectedBatchId)
    ) {
      setSelectedBatchId(firstBatchId);
    }
  }, [bulkBatchesQuery.data?.batches, selectedBatchId]);

  useEffect(() => {
    if (tenantId === "" || instanceId === "" || resolvedRunId === "") return;
    if (typeof EventSource === "undefined") return;
    const source = new EventSource(api.workflowRunWatchPath(tenantId, instanceId, resolvedRunId));
    const refresh = () => {
      void client.invalidateQueries({ queryKey: ["workflow", tenantId, instanceId] });
      void client.invalidateQueries({ queryKey: ["run-detail-runs", tenantId, instanceId] });
    };
    const refreshBatches = () => {
      void client.invalidateQueries({ queryKey: ["run-bulk-batches", tenantId, instanceId, resolvedRunId] });
      if (selectedBatchId !== "") {
        void client.invalidateQueries({
          queryKey: ["run-bulk-batch", tenantId, instanceId, resolvedRunId, selectedBatchId]
        });
      }
    };
    source.addEventListener("workflow_state_changed", refresh);
    source.addEventListener("throughput_batches_changed", refreshBatches);
    return () => source.close();
  }, [client, instanceId, resolvedRunId, selectedBatchId, tenantId]);

  useEffect(() => {
    if (tenantId === "" || instanceId === "" || resolvedRunId === "" || selectedBatchId === "") return;
    if (typeof EventSource === "undefined") return;
    setLiveBatchRouting(null);
    setLiveBatchCapacity(null);
    const source = new EventSource(api.bulkBatchWatchPath(tenantId, instanceId, resolvedRunId, selectedBatchId));
    const refresh = () => {
      void client.invalidateQueries({ queryKey: ["run-bulk-batches", tenantId, instanceId, resolvedRunId] });
      void client.invalidateQueries({
        queryKey: ["run-bulk-batch", tenantId, instanceId, resolvedRunId, selectedBatchId]
      });
    };
    source.addEventListener("batch_progress", refresh);
    source.addEventListener("batch_terminal", refresh);
    source.addEventListener("projection_lag", refresh);
    source.addEventListener("routing_changed", (event) => {
      const body = parseWatchBody<LiveBatchRoutingSignal>(event as MessageEvent<string>);
      if (body) setLiveBatchRouting(body);
      refresh();
    });
    source.addEventListener("capacity_pressure", (event) => {
      const body = parseWatchBody<LiveBatchCapacitySignal>(event as MessageEvent<string>);
      if (body) setLiveBatchCapacity(body);
    });
    return () => source.close();
  }, [client, instanceId, resolvedRunId, selectedBatchId, tenantId]);

  const timelineItems = useMemo(() => {
    const historyItems = (historyQuery.data?.events ?? []).map((event) => ({
      id: event.event_id,
      occurred_at: event.occurred_at,
      lane: eventLane(event.event_type),
      kind: "history",
      label: event.event_type,
      detail: Object.keys(event.metadata ?? {}).length > 0 ? JSON.stringify(event.metadata) : null
    }));
    const activityItems = (activitiesQuery.data?.activities ?? []).flatMap((activity) => [
      {
        id: `${activity.activity_id}:${activity.attempt}:scheduled`,
        occurred_at: activity.scheduled_at,
        lane: "activities" as TimelineLane,
        kind: "activity",
        label: `${activity.activity_type} scheduled`,
        detail: `${activity.activity_id} attempt ${activity.attempt}`
      },
      ...(activity.started_at
        ? [
            {
              id: `${activity.activity_id}:${activity.attempt}:started`,
              occurred_at: activity.started_at,
              lane: "activities" as TimelineLane,
              kind: "activity",
              label: `${activity.activity_type} started`,
              detail: activity.worker_build_id ?? activity.worker_id ?? null
            }
          ]
        : []),
      ...(activity.completed_at
        ? [
            {
              id: `${activity.activity_id}:${activity.attempt}:completed`,
              occurred_at: activity.completed_at,
              lane: "activities" as TimelineLane,
              kind: "activity",
              label: `${activity.activity_type} ${activity.status}`,
              detail: activity.error ?? null
            }
          ]
        : [])
    ]);
    const merged = [...historyItems, ...activityItems]
      .filter((item) => item.occurred_at)
      .sort((left, right) => left.occurred_at.localeCompare(right.occurred_at));
    const laneFiltered = laneFilter === "all" ? merged : merged.filter((item) => item.lane === laneFilter);
    if (windowPreset === "recent-20") return laneFiltered.slice(-20);
    if (windowPreset === "recent-50") return laneFiltered.slice(-50);
    return laneFiltered;
  }, [activitiesQuery.data?.activities, historyQuery.data?.events, laneFilter, windowPreset]);

  const activityFailures = (activitiesQuery.data?.activities ?? []).filter((activity) =>
    ["failed", "cancelled", "timed_out"].includes(activity.status)
  );
  const routing = routingQuery.data;
  const replay = replayQuery.data;
  const expectedQueue = selectedRun?.workflow_task_queue ?? workflowQuery.data?.workflow_task_queue ?? null;
  const expectedDefinitionVersion = selectedRun?.definition_version ?? workflowQuery.data?.definition_version ?? null;
  const expectedArtifactHash = selectedRun?.artifact_hash ?? workflowQuery.data?.artifact_hash ?? null;
  const replayQueue = replay?.replayed_state?.workflow_task_queue ?? null;
  const replayQueuePreserved =
    expectedQueue != null && replayQueue != null ? replayQueue === expectedQueue : null;
  const replayVersionMatchesPinned =
    expectedDefinitionVersion != null ? replay?.definition_version === expectedDefinitionVersion : null;
  const replayArtifactMatchesPinned =
    expectedArtifactHash != null && replay?.artifact_hash != null ? replay.artifact_hash === expectedArtifactHash : null;
  const routingQueueHref =
    routing?.workflow_task_queue != null
      ? `/task-queues?queue_kind=workflow&task_queue=${encodeURIComponent(routing.workflow_task_queue)}`
      : null;
  const relatedRunsSummary =
    runs.length > 0
      ? runs
          .map((item) => `${item.run_id} · v${item.definition_version ?? "-"} · ${item.artifact_hash ?? "-"}`)
          .join(" | ")
      : "-";
  const selectedBatch = bulkBatchDetailQuery.data?.batch ?? null;

  async function onSignal(event: FormEvent) {
    event.preventDefault();
    try {
      await api.signalWorkflow(tenantId, instanceId, signalType, { payload: JSON.parse(signalPayload) });
      toast.success("Signal accepted");
      void client.invalidateQueries({ queryKey: ["run-history", tenantId, instanceId, resolvedRunId] });
    } catch (error) {
      toast.error(String(error));
    }
  }

  async function onTerminate(event: FormEvent) {
    event.preventDefault();
    try {
      await api.terminateWorkflow(tenantId, instanceId, { reason: terminateReason });
      toast.success("Terminate accepted");
      void client.invalidateQueries({ queryKey: ["workflow", tenantId, instanceId] });
    } catch (error) {
      toast.error(String(error));
    }
  }

  async function cancelActivity(activityId: string) {
    try {
      await api.cancelActivity(tenantId, instanceId, activityId, { reason: "cancelled from console" });
      toast.success(`Cancellation requested for ${activityId}`);
      void client.invalidateQueries({ queryKey: ["run-activities", tenantId, instanceId, resolvedRunId] });
    } catch (error) {
      toast.error(String(error));
    }
  }

  async function toggleBatchPause() {
    if (tenantId === "" || instanceId === "" || resolvedRunId === "" || selectedBatchId === "" || !selectedBatch) {
      return;
    }
    setBatchControlPending(true);
    try {
      await api.setBulkBatchRuntimeControl(tenantId, instanceId, resolvedRunId, selectedBatchId, {
        is_paused: !bulkBatchDetailQuery.data?.runtime_control?.is_paused,
        reason:
          bulkBatchDetailQuery.data?.runtime_control?.is_paused
            ? "batch resumed by operator"
            : "batch paused by operator"
      });
      toast.success("Batch control updated");
      await client.invalidateQueries({ queryKey: ["run-bulk-batches", tenantId, instanceId, resolvedRunId] });
      await client.invalidateQueries({
        queryKey: ["run-bulk-batch", tenantId, instanceId, resolvedRunId, selectedBatchId]
      });
    } catch (error) {
      toast.error(String(error));
    } finally {
      setBatchControlPending(false);
    }
  }

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <div className="eyebrow">Run detail</div>
          <h1>{selectedRun?.definition_id ?? workflowQuery.data?.definition_id ?? instanceId}</h1>
          <p>
            {instanceId} {resolvedRunId ? `· ${resolvedRunId}` : ""}
          </p>
        </div>
        <div className="row">
          <ConsistencyBadge consistency={selectedRun?.consistency ?? "eventual"} source={selectedRun?.source ?? "projection"} />
          <Badge value={selectedRun?.status ?? workflowQuery.data?.status} />
        </div>
      </header>

      <div className="split">
        <Panel>
          <div className="tabs">
            {TABS.map((tab) => (
              <button key={tab} className={`button ghost ${tab === activeTab ? "active" : ""}`} onClick={() => setActiveTab(tab)}>
                {tab}
              </button>
            ))}
          </div>

          {activeTab === "summary" ? (
            <div className="stack detail-stack">
              <div className="grid two">
                <Panel className="nested-panel">
                  <h3>Execution</h3>
                  <div className="stack">
                    <div>Status {selectedRun?.status ?? "-"}</div>
                    <div>Current state {selectedRun?.current_state ?? "-"}</div>
                    <div>Task queue {selectedRun?.workflow_task_queue ?? "-"}</div>
                    <div>Duration {formatDuration(selectedRun?.started_at, selectedRun?.closed_at)}</div>
                    <div>Last transition {formatDate(selectedRun?.last_transition_at)}</div>
                  </div>
                </Panel>
                <Panel className="nested-panel">
                  <h3>Related routing</h3>
                  <div className="stack">
                    <div>Definition version {formatInlineValue(routing?.definition_version ?? selectedRun?.definition_version)}</div>
                    <div>Artifact hash {routing?.artifact_hash ?? selectedRun?.artifact_hash ?? "-"}</div>
                    <div>Routing status {routing?.routing_status ?? "-"}</div>
                    <div>Default set {routing?.default_compatibility_set_id ?? "-"}</div>
                    <div>Sticky build {routing?.sticky_workflow_build_id ?? selectedRun?.sticky_workflow_build_id ?? "-"}</div>
                    <div>Sticky poller {routing?.sticky_workflow_poller_id ?? selectedRun?.sticky_workflow_poller_id ?? "-"}</div>
                    <div>
                      Sticky queue compatibility {formatInlineValue(routing?.sticky_build_compatible_with_queue)}
                    </div>
                    <div>
                      Sticky artifact support {formatInlineValue(routing?.sticky_build_supports_pinned_artifact)}
                    </div>
                    <div>Compatible builds {(routing?.compatible_build_ids ?? []).join(", ") || "-"}</div>
                    <div>Registered builds {(routing?.registered_build_ids ?? []).join(", ") || "-"}</div>
                    {routingQueueHref ? (
                      <Link className="button ghost" to={routingQueueHref}>
                        Inspect workflow queue
                      </Link>
                    ) : null}
                  </div>
                </Panel>
              </div>

              <div className="grid two">
                <Panel className="nested-panel">
                  <h3>Pinned artifact evidence</h3>
                  <div className="stack">
                    <div>Pinned definition version {formatInlineValue(expectedDefinitionVersion)}</div>
                    <div>Pinned artifact hash {formatInlineValue(expectedArtifactHash)}</div>
                    <div>Replay definition version {formatInlineValue(replay?.definition_version)}</div>
                    <div>Replay artifact hash {formatInlineValue(replay?.artifact_hash)}</div>
                    <div>Replay kept pinned version {formatInlineValue(replayVersionMatchesPinned)}</div>
                    <div>Replay kept pinned artifact {formatInlineValue(replayArtifactMatchesPinned)}</div>
                    <div>Run lineage {relatedRunsSummary}</div>
                  </div>
                </Panel>
                <Panel className="nested-panel">
                  <h3>Alpha metadata evidence</h3>
                  <div className="stack">
                    <div>Memo {metadataPreview(selectedRun?.memo ?? workflowQuery.data?.memo)}</div>
                    <div>
                      Search attributes{" "}
                      {metadataPreview(selectedRun?.search_attributes ?? workflowQuery.data?.search_attributes)}
                    </div>
                    <div className="muted">
                      Supported slice: static start-time values plus exact-match visibility filters.
                    </div>
                  </div>
                </Panel>
                <Panel className="nested-panel">
                  <h3>Replay and failover</h3>
                  <div className="stack">
                    <div>Replay source {formatInlineValue(replay?.replay_source)}</div>
                    <div>Divergences {formatNumber(replay?.divergence_count)}</div>
                    <div>Projection matches store {formatInlineValue(replay?.projection_matches_store)}</div>
                    <div>Snapshot boundary {formatInlineValue(replay?.snapshot?.last_event_type)}</div>
                    <div>Snapshot updated {formatDate(replay?.snapshot?.updated_at)}</div>
                    <div>Expected queue {formatInlineValue(expectedQueue)}</div>
                    <div>
                      Replayed queue {formatInlineValue(replay?.replayed_state?.workflow_task_queue)}
                    </div>
                    <div>Queue preserved across replay/handoff {formatInlineValue(replayQueuePreserved)}</div>
                    <div>Replayed memo {metadataPreview(replay?.replayed_state?.memo)}</div>
                    <div>
                      Replayed search {metadataPreview(replay?.replayed_state?.search_attributes)}
                    </div>
                    <Link
                      className="button ghost"
                      to={`/replay?instance=${encodeURIComponent(instanceId)}&run=${encodeURIComponent(resolvedRunId)}`}
                    >
                      Open replay workbench
                    </Link>
                  </div>
                </Panel>
              </div>

              <Panel className="nested-panel">
                <div className="row space-between">
                  <h3>Run lineage</h3>
                  <Link className="button ghost" to={`/runs?instance_id=${encodeURIComponent(instanceId)}`}>
                    View all instance runs
                  </Link>
                </div>
                <div className="stack">
                  {runs.map((item) => (
                    <Link key={item.run_id} className={`subtle-block ${item.run_id === resolvedRunId ? "current" : ""}`} to={`/runs/${item.instance_id}/${item.run_id}`}>
                      <div className="row space-between">
                        <strong>{item.run_id}</strong>
                        <Badge value={item.status} />
                      </div>
                      <div className="muted">
                        {formatDate(item.started_at)} · {item.continue_reason ?? "initial run"}
                      </div>
                    </Link>
                  ))}
                </div>
              </Panel>

              <Panel className="nested-panel">
                <div className="row space-between">
                  <h3>Throughput batches</h3>
                  <ConsistencyBadge
                    consistency={bulkBatchesQuery.data?.consistency ?? bulkBatchDetailQuery.data?.consistency}
                    source={bulkBatchesQuery.data?.authoritative_source ?? bulkBatchDetailQuery.data?.authoritative_source}
                  />
                </div>
                {bulkBatchesQuery.data && bulkBatchesQuery.data.batches.length > 0 ? (
                  <div className="split">
                    <div className="stack">
                      {bulkBatchesQuery.data.batches.map((batch) => (
                        <button
                          key={batch.batch_id}
                          className={`subtle-block ${batch.batch_id === selectedBatchId ? "current" : ""}`}
                          onClick={() => setSelectedBatchId(batch.batch_id)}
                          style={{ textAlign: "left" }}
                        >
                          <div className="row space-between">
                            <strong>{batch.activity_type}</strong>
                            <Badge value={batch.status} />
                          </div>
                          <div className="muted">
                            {batch.batch_id} · {formatNumber(batch.total_items)} items · {batch.selected_backend}
                          </div>
                        </button>
                      ))}
                    </div>

                    <div className="stack">
                      {selectedBatch ? (
                        <Panel className="nested-panel">
                          <div className="row space-between">
                            <h3>{selectedBatch.batch_id}</h3>
                            <div className="row">
                              {bulkBatchDetailQuery.data?.runtime_control?.is_paused ? <Badge value="paused" /> : null}
                              <Badge value={selectedBatch.status} />
                            </div>
                          </div>
                          <div className="stack">
                            <div>Backend {selectedBatch.selected_backend}</div>
                            <div>Execution mode {selectedBatch.execution_mode}</div>
                            <div>Routing reason {selectedBatch.routing_reason}</div>
                            <div>Admission policy {selectedBatch.admission_policy_version}</div>
                            <div>
                              Live routing signal{" "}
                              {liveBatchRouting
                                ? `${liveBatchRouting.selected_backend ?? "-"} · ${liveBatchRouting.routing_reason ?? "-"}`
                                : "-"}
                            </div>
                            <div>
                              Live capacity signal{" "}
                              {liveBatchCapacity?.admission?.stream_v2_capacity?.state ?? "-"}
                            </div>
                            <div>
                              Live queue utilization{" "}
                              {liveBatchCapacity?.admission?.stream_v2_capacity?.task_queue_utilization != null
                                ? `${((liveBatchCapacity.admission.stream_v2_capacity.task_queue_utilization ?? 0) * 100).toFixed(1)}%`
                                : "-"}
                            </div>
                            <div>Reducer {selectedBatch.reducer_kind}</div>
                            <div>Reducer path {selectedBatch.reducer_execution_path}</div>
                            <div>Projection lag {formatInlineValue(bulkBatchDetailQuery.data?.projection_lag_ms)}</div>
                            <div>Fast lane {formatInlineValue(selectedBatch.fast_lane_enabled)}</div>
                            <div>Tree depth {formatInlineValue(selectedBatch.aggregation_tree_depth)}</div>
                            <div>Group count {formatInlineValue(selectedBatch.aggregation_group_count)}</div>
                            <div>
                              Progress {formatNumber(selectedBatch.succeeded_items)} succeeded · {formatNumber(selectedBatch.failed_items)} failed
                              {" "}· {formatNumber(selectedBatch.cancelled_items)} cancelled
                            </div>
                            <div>Control reason {bulkBatchDetailQuery.data?.runtime_control?.reason ?? "-"}</div>
                            <button className="button ghost" disabled={batchControlPending} onClick={() => void toggleBatchPause()}>
                              {bulkBatchDetailQuery.data?.runtime_control?.is_paused ? "Resume batch" : "Pause batch"}
                            </button>
                          </div>
                        </Panel>
                      ) : (
                        <div className="empty">Select a throughput batch to inspect live routing and progress.</div>
                      )}
                    </div>
                  </div>
                ) : (
                  <div className="empty">No throughput batches recorded for this run.</div>
                )}
              </Panel>

              <div className="grid two">
                <Panel className="nested-panel">
                  <h3>Failure and retry signal</h3>
                  <div className="stack">
                    <div>Activity failures {formatNumber(activityFailures.length)}</div>
                    <div>Active activities {formatNumber(activeActivityIds.length)}</div>
                    <div>History events {formatNumber(historyQuery.data?.event_count)}</div>
                    <div>Last event {selectedRun?.last_event_type ?? "-"}</div>
                  </div>
                </Panel>
                <Panel className="nested-panel">
                  <h3>Projection details</h3>
                  <div className="stack">
                    <div>Event count {formatNumber(selectedRun?.event_count)}</div>
                    <div>Started {formatDate(selectedRun?.started_at)}</div>
                    <div>Closed {formatDate(selectedRun?.closed_at)}</div>
                    <div>Continue reason {selectedRun?.continue_reason ?? "-"}</div>
                  </div>
                </Panel>
              </div>
            </div>
          ) : null}

          {activeTab === "graph" ? (
            <WorkflowGraphExplorer
              graph={graphQuery.data}
              onOpenActivities={() => setActiveTab("activities")}
              onOpenHistory={() => setActiveTab("raw-history")}
            />
          ) : null}

          {activeTab === "timeline" ? (
            <div className="stack">
              <div className="row space-between">
                <div className="row">
                  {(["all", "lifecycle", "activities", "messages", "timers", "infra", "other"] as const).map((lane) => (
                    <button key={lane} className={`button ghost ${laneFilter === lane ? "active" : ""}`} onClick={() => setLaneFilter(lane)}>
                      {lane}
                    </button>
                  ))}
                </div>
                <div className="row">
                  {(["all", "recent-50", "recent-20"] as const).map((preset) => (
                    <button key={preset} className={`button ghost ${windowPreset === preset ? "active" : ""}`} onClick={() => setWindowPreset(preset)}>
                      {preset}
                    </button>
                  ))}
                </div>
              </div>
              <div className="timeline">
                {timelineItems.map((item) => (
                  <div key={item.id} className="timeline-item">
                    <div className="timeline-time">{formatDate(item.occurred_at)}</div>
                    <div className={`timeline-lane ${item.lane}`}>{item.lane}</div>
                    <div>
                      <strong>{item.label}</strong>
                      {item.detail ? <div className="muted">{item.detail}</div> : null}
                    </div>
                  </div>
                ))}
                {timelineItems.length === 0 ? <div className="empty">No timeline events matched the current lane and window filters.</div> : null}
              </div>
            </div>
          ) : null}

          {activeTab === "activities" ? (
            <table className="table">
              <thead>
                <tr>
                  <th>Activity</th>
                  <th>Status</th>
                  <th>Worker</th>
                  <th>Schedule-to-start</th>
                  <th>Start-to-close</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {(activitiesQuery.data?.activities ?? []).map((activity) => (
                  <tr key={`${activity.activity_id}:${activity.attempt}`}>
                    <td>
                      {activity.activity_type}
                      <div className="muted">
                        {activity.activity_id} attempt {activity.attempt}
                      </div>
                    </td>
                    <td>
                      <Badge value={activity.status} />
                    </td>
                    <td>{activity.worker_build_id ?? activity.worker_id ?? "-"}</td>
                    <td>{formatDuration(activity.scheduled_at, activity.started_at)}</td>
                    <td>{formatDuration(activity.started_at, activity.completed_at)}</td>
                    <td>
                      {canMutate && activeActivityIds.includes(activity.activity_id) ? (
                        <button className="button danger" onClick={() => cancelActivity(activity.activity_id)}>
                          Cancel
                        </button>
                      ) : null}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : null}

          {activeTab === "raw-history" ? (
            <table className="table">
              <thead>
                <tr>
                  <th>Event</th>
                  <th>Time</th>
                  <th>Metadata</th>
                  <th>Payload</th>
                </tr>
              </thead>
              <tbody>
                {(historyQuery.data?.events ?? []).map((event) => (
                  <tr key={event.event_id}>
                    <td>{event.event_type}</td>
                    <td>{formatDate(event.occurred_at)}</td>
                    <td>
                      <details>
                        <summary>View metadata</summary>
                        <pre className="code">{JSON.stringify(event.metadata, null, 2)}</pre>
                      </details>
                    </td>
                    <td>
                      <details>
                        <summary>View payload</summary>
                        <pre className="code">{JSON.stringify(event.payload, null, 2)}</pre>
                      </details>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : null}
        </Panel>

        <div className="stack">
          <Panel>
            <h3>Safe actions</h3>
            <div className="muted">Workflow actions are only enabled for the current non-terminal run.</div>
          </Panel>

          <Panel>
            <h3>Signal workflow</h3>
            <form className="stack" onSubmit={onSignal}>
              <input className="input" value={signalType} onChange={(event) => setSignalType(event.target.value)} disabled={!canMutate} />
              <textarea className="textarea" rows={6} value={signalPayload} onChange={(event) => setSignalPayload(event.target.value)} disabled={!canMutate} />
              <button className="button primary" type="submit" disabled={!canMutate}>
                Send signal
              </button>
            </form>
          </Panel>

          <Panel>
            <h3>Terminate workflow</h3>
            <form className="stack" onSubmit={onTerminate}>
              <textarea className="textarea" rows={4} value={terminateReason} onChange={(event) => setTerminateReason(event.target.value)} disabled={!canMutate} />
              <button className="button danger" type="submit" disabled={!canMutate}>
                Request terminate
              </button>
            </form>
          </Panel>
        </div>
      </div>
    </div>
  );
}
