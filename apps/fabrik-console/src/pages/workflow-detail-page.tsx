import { useQuery, useQueryClient } from "@tanstack/react-query";
import { FormEvent, useMemo, useState } from "react";
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
  const routingQueueHref =
    routing?.workflow_task_queue != null
      ? `/task-queues?queue_kind=workflow&task_queue=${encodeURIComponent(routing.workflow_task_queue)}`
      : null;

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
