import { useMemo } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";

import { Badge, ConsistencyBadge, Panel } from "../components/ui";
import { api, type StreamJobSummary, type StreamJobViewSummary } from "../lib/api";
import { formatDate, formatInlineValue, formatNumber } from "../lib/format";
import { useTenant } from "../lib/tenant-context";

const VIEW_ENTRY_PAGE_SIZE = 20;
const LAGGING_VIEW_THRESHOLD_MS = 60_000;

function labelFromSnakeCase(value: string | null | undefined) {
  if (!value) return "-";
  return value
    .split("_")
    .filter(Boolean)
    .map((segment) => segment[0]?.toUpperCase() + segment.slice(1))
    .join(" ");
}

function jsonPreview(value: unknown) {
  if (value == null) return "-";
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (Array.isArray(value)) {
    return value.slice(0, 3).map((entry) => formatInlineValue(entry)).join(" · ");
  }
  if (typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>).slice(0, 3);
    if (entries.length === 0) return "{}";
    return entries.map(([key, entry]) => `${key}:${formatInlineValue(entry)}`).join(" · ");
  }
  return formatInlineValue(value);
}

function formatLag(value: number | null | undefined) {
  if (value == null) return "-";
  if (value < 1_000) return `${value} ms`;
  if (value < 60_000) return `${(value / 1_000).toFixed(1)} s`;
  return `${(value / 60_000).toFixed(1)} m`;
}

function updateParams(
  searchParams: URLSearchParams,
  setSearchParams: ReturnType<typeof useSearchParams>[1],
  updates: Record<string, string | null | undefined>,
) {
  const next = new URLSearchParams(searchParams);
  for (const [key, value] of Object.entries(updates)) {
    if (!value) {
      next.delete(key);
    } else {
      next.set(key, value);
    }
  }
  setSearchParams(next);
}

function buildJobListParams(searchParams: URLSearchParams) {
  const params = new URLSearchParams();
  params.set("limit", "100");
  params.set("offset", "0");
  for (const key of [
    "job_name",
    "definition_id",
    "status",
    "origin_kind",
    "instance_id",
    "run_id",
    "stream_instance_id",
    "stream_run_id",
    "sort",
  ]) {
    const value = searchParams.get(key)?.trim();
    if (value) params.set(key, value);
  }
  if (!params.has("sort")) {
    params.set("sort", "updated_at_desc");
  }
  return params;
}

function findSelectedJob(requestedJobId: string | null, jobs: StreamJobSummary[]) {
  if (requestedJobId) {
    const exact = jobs.find((job) => job.job_id === requestedJobId);
    if (exact) return exact;
  }
  return jobs[0] ?? null;
}

function findSelectedView(requestedView: string | null, views: StreamJobViewSummary[]) {
  if (requestedView) {
    const exact = views.find((view) => view.view_name === requestedView);
    if (exact) return exact;
  }
  return views[0] ?? null;
}

function selectedIdentity(job: StreamJobSummary | null) {
  if (!job) return null;
  return {
    instanceId: job.workflow_binding?.instance_id ?? job.stream_instance_id,
    runId: job.workflow_binding?.run_id ?? job.stream_run_id,
  };
}

function identityLabel(job: StreamJobSummary) {
  if (job.workflow_binding) {
    return `${job.workflow_binding.instance_id} / ${job.workflow_binding.run_id}`;
  }
  return `${job.stream_instance_id} / ${job.stream_run_id}`;
}

function hasLaggingState(job: StreamJobSummary) {
  return (job.stream_surface.slowest_eventual_view_lag_ms ?? 0) >= LAGGING_VIEW_THRESHOLD_MS;
}

export function StreamJobsPage() {
  const { tenantId } = useTenant();
  const [searchParams, setSearchParams] = useSearchParams();
  const viewConsistency = searchParams.get("consistency") === "eventual" ? "eventual" : "strong";
  const viewPrefix = searchParams.get("prefix")?.trim() || null;
  const jobListParams = useMemo(() => buildJobListParams(searchParams), [searchParams]);

  const jobsQuery = useQuery({
    queryKey: ["tenant-stream-jobs", tenantId, jobListParams.toString()],
    enabled: tenantId !== "",
    refetchInterval: 5_000,
    queryFn: () => api.listTenantStreamJobs(tenantId, jobListParams),
  });

  const selectedJob = findSelectedJob(searchParams.get("job_id"), jobsQuery.data?.jobs ?? []);
  const selectedJobIdentity = selectedIdentity(selectedJob);

  const jobDetailQuery = useQuery({
    queryKey: ["stream-job-detail", tenantId, selectedJobIdentity?.instanceId, selectedJobIdentity?.runId, selectedJob?.job_id],
    enabled: tenantId !== "" && selectedJob != null && selectedJobIdentity != null,
    refetchInterval: 5_000,
    queryFn: () => api.getStreamJob(tenantId, selectedJobIdentity!.instanceId, selectedJobIdentity!.runId, selectedJob!.job_id),
  });

  const bridgeDetailQuery = useQuery({
    queryKey: [
      "stream-job-bridge-handle",
      tenantId,
      selectedJobIdentity?.instanceId,
      selectedJobIdentity?.runId,
      selectedJob?.job_id,
    ],
    enabled: tenantId !== "" && selectedJob != null && selectedJobIdentity != null,
    refetchInterval: 5_000,
    queryFn: () =>
      api.getStreamJobBridgeHandle(tenantId, selectedJobIdentity!.instanceId, selectedJobIdentity!.runId, selectedJob!.job_id),
  });

  const selectedJobDetail = jobDetailQuery.data?.job ?? selectedJob;
  const selectedJobDetailIdentity = selectedIdentity(selectedJobDetail);
  const selectedViews = jobDetailQuery.data?.views ?? selectedJobDetail?.views ?? [];
  const selectedView = findSelectedView(searchParams.get("view"), selectedViews);

  const viewEntriesQuery = useQuery({
    queryKey: [
      "stream-job-view-entries",
      tenantId,
      selectedJobDetailIdentity?.instanceId,
      selectedJobDetailIdentity?.runId,
      selectedJobDetail?.job_id,
      selectedView?.view_name,
      viewConsistency,
      viewPrefix,
    ],
    enabled:
      tenantId !== "" &&
      selectedJobDetail != null &&
      selectedJobDetailIdentity != null &&
      selectedView != null,
    refetchInterval: 5_000,
    queryFn: () =>
      api.getStreamJobViewEntries(
        tenantId,
        selectedJobDetailIdentity!.instanceId,
        selectedJobDetailIdentity!.runId,
        selectedJobDetail!.job_id,
        selectedView!.view_name,
        viewConsistency,
        viewPrefix,
        VIEW_ENTRY_PAGE_SIZE,
        0,
      ),
  });

  const selectedViewEntry =
    viewEntriesQuery.data?.entries.find((entry) => entry.logical_key === searchParams.get("key")) ??
    viewEntriesQuery.data?.entries[0] ??
    null;

  const viewValueQuery = useQuery({
    queryKey: [
      "stream-job-view-value",
      tenantId,
      selectedJobDetailIdentity?.instanceId,
      selectedJobDetailIdentity?.runId,
      selectedJobDetail?.job_id,
      selectedView?.view_name,
      selectedViewEntry?.logical_key,
      viewConsistency,
    ],
    enabled:
      tenantId !== "" &&
      selectedJobDetail != null &&
      selectedJobDetailIdentity != null &&
      selectedView != null &&
      selectedViewEntry != null,
    refetchInterval: 5_000,
    queryFn: () =>
      api.getStreamJobView(
        tenantId,
        selectedJobDetailIdentity!.instanceId,
        selectedJobDetailIdentity!.runId,
        selectedJobDetail!.job_id,
        selectedView!.view_name,
        selectedViewEntry!.logical_key,
        viewConsistency,
      ),
  });

  const workflowBoundCount = (jobsQuery.data?.jobs ?? []).filter((job) => job.workflow_binding != null).length;
  const standaloneCount = (jobsQuery.data?.jobs ?? []).filter((job) => job.workflow_binding == null).length;
  const totalProjectedKeys = (jobsQuery.data?.jobs ?? []).reduce(
    (sum, job) => sum + job.stream_surface.total_projected_keys,
    0,
  );
  const declaredCheckpointCount = (jobsQuery.data?.jobs ?? []).reduce(
    (sum, job) => sum + job.stream_surface.declared_checkpoint_count,
    0,
  );
  const reachedCheckpointCount = (jobsQuery.data?.jobs ?? []).reduce(
    (sum, job) => sum + job.stream_surface.reached_checkpoint_count,
    0,
  );
  const slowestLagMs = (jobsQuery.data?.jobs ?? []).reduce<number | null>((slowest, job) => {
    const candidate = job.stream_surface.slowest_eventual_view_lag_ms;
    if (candidate == null) return slowest;
    if (slowest == null) return candidate;
    return Math.max(slowest, candidate);
  }, null);
  const repairPendingCount = (jobsQuery.data?.jobs ?? []).filter((job) => job.bridge_surface.pending_repair_count > 0).length;
  const failedLatestQueryCount = (jobsQuery.data?.jobs ?? []).filter(
    (job) => job.bridge_surface.latest_query_status === "failed",
  ).length;
  const laggingStateCount = (jobsQuery.data?.jobs ?? []).filter(hasLaggingState).length;
  const runningCount = (jobsQuery.data?.jobs ?? []).filter((job) => ["starting", "running", "draining"].includes(job.status)).length;
  const terminalCount = (jobsQuery.data?.jobs ?? []).filter((job) => ["completed", "failed", "cancelled"].includes(job.status)).length;
  const pageConsistency = viewValueQuery.data?.consistency ?? (selectedJobDetail ? "mixed" : "eventual");
  const pageSource = viewValueQuery.data?.source ?? "tenant-index";

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <div className="eyebrow">Workflow / stream bridge</div>
          <h1>Stream Jobs</h1>
          <p>Browse stream jobs tenant-wide, including standalone jobs, then drill into bridge state and materialized views.</p>
        </div>
        <ConsistencyBadge consistency={pageConsistency} source={pageSource} />
      </header>

      <Panel>
        <div className="filters-grid">
          <input
            className="input"
            placeholder="Job name"
            value={searchParams.get("job_name") ?? ""}
            onChange={(event) =>
              updateParams(searchParams, setSearchParams, {
                job_name: event.target.value.trimStart() || null,
                job_id: null,
                view: null,
                key: null,
              })
            }
          />
          <input
            className="input"
            placeholder="Definition id"
            value={searchParams.get("definition_id") ?? ""}
            onChange={(event) =>
              updateParams(searchParams, setSearchParams, {
                definition_id: event.target.value.trimStart() || null,
                job_id: null,
                view: null,
                key: null,
              })
            }
          />
          <select
            className="select"
            value={searchParams.get("status") ?? ""}
            onChange={(event) =>
              updateParams(searchParams, setSearchParams, {
                status: event.target.value || null,
                job_id: null,
                view: null,
                key: null,
              })
            }
          >
            <option value="">All job statuses</option>
            <option value="created">created</option>
            <option value="starting">starting</option>
            <option value="running">running</option>
            <option value="draining">draining</option>
            <option value="completed">completed</option>
            <option value="failed">failed</option>
            <option value="cancelled">cancelled</option>
          </select>
          <select
            className="select"
            value={searchParams.get("origin_kind") ?? ""}
            onChange={(event) =>
              updateParams(searchParams, setSearchParams, {
                origin_kind: event.target.value || null,
                job_id: null,
                view: null,
                key: null,
              })
            }
          >
            <option value="">All origins</option>
            <option value="workflow">workflow</option>
            <option value="standalone">standalone</option>
          </select>
          <input
            className="input"
            placeholder="Workflow instance id"
            value={searchParams.get("instance_id") ?? ""}
            onChange={(event) =>
              updateParams(searchParams, setSearchParams, {
                instance_id: event.target.value.trimStart() || null,
                job_id: null,
                view: null,
                key: null,
              })
            }
          />
          <input
            className="input"
            placeholder="Workflow run id"
            value={searchParams.get("run_id") ?? ""}
            onChange={(event) =>
              updateParams(searchParams, setSearchParams, {
                run_id: event.target.value.trimStart() || null,
                job_id: null,
                view: null,
                key: null,
              })
            }
          />
          <input
            className="input"
            placeholder="Stream instance id"
            value={searchParams.get("stream_instance_id") ?? ""}
            onChange={(event) =>
              updateParams(searchParams, setSearchParams, {
                stream_instance_id: event.target.value.trimStart() || null,
                job_id: null,
                view: null,
                key: null,
              })
            }
          />
          <input
            className="input"
            placeholder="Stream run id"
            value={searchParams.get("stream_run_id") ?? ""}
            onChange={(event) =>
              updateParams(searchParams, setSearchParams, {
                stream_run_id: event.target.value.trimStart() || null,
                job_id: null,
                view: null,
                key: null,
              })
            }
          />
        </div>
      </Panel>

      <div className="grid metrics">
        <Panel>
          <div className="muted">Visible jobs</div>
          <div className="metric-value">{formatNumber(jobsQuery.data?.job_count)}</div>
          <div className="muted">
            running {formatNumber(runningCount)} · terminal {formatNumber(terminalCount)}
          </div>
        </Panel>
        <Panel>
          <div className="muted">Origin mix</div>
          <div className="metric-value">{formatNumber(workflowBoundCount)}</div>
          <div className="muted">workflow-bound</div>
          <div className="muted">standalone {formatNumber(standaloneCount)}</div>
        </Panel>
        <Panel>
          <div className="muted">Checkpoint surface</div>
          <div className="metric-value">
            {formatNumber(reachedCheckpointCount)} / {formatNumber(declaredCheckpointCount)}
          </div>
          <div className="muted">reached / declared</div>
        </Panel>
        <Panel>
          <div className="muted">Materialized keys</div>
          <div className="metric-value">{formatNumber(totalProjectedKeys)}</div>
          <div className="muted">slowest lag {formatLag(slowestLagMs)}</div>
        </Panel>
        <Panel>
          <div className="muted">Bridge health</div>
          <div className="metric-value">{formatNumber(repairPendingCount)}</div>
          <div className="muted">
            repair-pending · query failed {formatNumber(failedLatestQueryCount)}
          </div>
        </Panel>
        <Panel>
          <div className="muted">State health</div>
          <div className="metric-value">{formatNumber(laggingStateCount)}</div>
          <div className="muted">lagging views over 60s</div>
        </Panel>
      </div>

      <div className="split">
        <Panel>
          <div className="row space-between">
            <div>
              <h2>Tenant stream job index</h2>
              <div className="muted">Select any workflow-bound or standalone stream job for deeper inspection.</div>
            </div>
          </div>
          {jobsQuery.error ? <div className="empty">{String(jobsQuery.error)}</div> : null}
          <table className="table">
            <thead>
              <tr>
                <th>Job</th>
                <th>Origin</th>
                <th>Status</th>
                <th>Identity</th>
                <th>Surface</th>
                <th>Health</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {(jobsQuery.data?.jobs ?? []).map((job) => (
                <tr
                  key={`${job.job_id}:${job.stream_instance_id}:${job.stream_run_id}`}
                  className={selectedJobDetail?.job_id === job.job_id ? "row-selected" : ""}
                  onClick={() =>
                    updateParams(searchParams, setSearchParams, {
                      job_id: job.job_id,
                      view: null,
                      key: null,
                    })
                  }
                  style={{ cursor: "pointer" }}
                >
                  <td>
                    <strong>{job.job_name}</strong>
                    <div className="muted">{job.job_id}</div>
                    <div className="muted">{job.definition_id}</div>
                  </td>
                  <td>
                    <Badge value={job.origin_kind} />
                    <div className="muted">{job.workflow_binding ? "workflow bridge" : "standalone stream"}</div>
                  </td>
                  <td>
                    <Badge value={job.status} />
                    <div className="muted">{labelFromSnakeCase(job.operation_kind)}</div>
                  </td>
                  <td>
                    {identityLabel(job)}
                    <div className="muted">stream {job.stream_instance_id} / {job.stream_run_id}</div>
                  </td>
                  <td>
                    {formatNumber(job.stream_surface.reached_checkpoint_count)} /{" "}
                    {formatNumber(job.stream_surface.declared_checkpoint_count)} checkpoints
                    <div className="muted">
                      {formatNumber(job.stream_surface.view_count)} views · {formatNumber(job.stream_surface.total_projected_keys)} keys
                    </div>
                  </td>
                  <td>
                    <div className="row">
                      {job.bridge_surface.pending_repair_count > 0 ? <Badge value="repair-pending" /> : null}
                      {job.bridge_surface.latest_query_status === "failed" ? <Badge value="query-failed" /> : null}
                      {hasLaggingState(job) ? <Badge value="lagging" /> : null}
                    </div>
                    <div className="muted">
                      repair {job.bridge_surface.next_repair ?? "-"} · query {job.bridge_surface.latest_query_status ?? "-"}
                    </div>
                    <div className="muted">lag {formatLag(job.stream_surface.slowest_eventual_view_lag_ms)}</div>
                  </td>
                  <td>
                    {formatDate(job.updated_at)}
                    <div className="muted">
                      checkpoint {job.latest_checkpoint_name ?? "-"} · lag {formatLag(job.stream_surface.slowest_eventual_view_lag_ms)}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {(jobsQuery.data?.jobs.length ?? 0) === 0 ? <div className="empty">No stream jobs matched the current tenant filters.</div> : null}
        </Panel>

        <Panel>
          {selectedJobDetail ? (
            <div className="stack">
              <div className="row space-between">
                <div>
                  <h2>{selectedJobDetail.job_name}</h2>
                  <div className="muted">
                    {selectedJobDetail.job_id} · handle {selectedJobDetail.handle_id}
                  </div>
                </div>
                <div className="row">
                  <Badge value={selectedJobDetail.status} />
                  {(bridgeDetailQuery.data?.handle.pending_repair_count ?? selectedJobDetail.bridge_surface.pending_repair_count) ? (
                    <Badge value="repair-pending" />
                  ) : null}
                  {hasLaggingState(selectedJobDetail) ? <Badge value="lagging" /> : null}
                </div>
              </div>

              <div className="grid two">
                <Panel className="nested-panel">
                  <h3>Lifecycle</h3>
                  <div className="stack">
                    <div>Definition {selectedJobDetail.definition_id}</div>
                    <div>Version {formatInlineValue(selectedJobDetail.definition_version)}</div>
                    <div>Origin {labelFromSnakeCase(selectedJobDetail.origin_kind)}</div>
                    <div>Workflow identity {selectedJobDetail.workflow_binding ? identityLabel(selectedJobDetail) : "not workflow-bound"}</div>
                    <div>Stream identity {selectedJobDetail.stream_instance_id} / {selectedJobDetail.stream_run_id}</div>
                    <div>Input ref {selectedJobDetail.input_ref}</div>
                    <div>Started {formatDate(selectedJobDetail.starting_at ?? selectedJobDetail.created_at)}</div>
                    <div>Running {formatDate(selectedJobDetail.running_at)}</div>
                    <div>Terminal {formatDate(selectedJobDetail.terminal_at)}</div>
                    <div>Latest checkpoint {selectedJobDetail.latest_checkpoint_name ?? "-"}</div>
                    <div>Terminal output {jsonPreview(selectedJobDetail.terminal_output)}</div>
                    <div>Terminal error {selectedJobDetail.terminal_error ?? "-"}</div>
                  </div>
                </Panel>

                <Panel className="nested-panel">
                  <h3>Bridge</h3>
                  <div className="stack">
                    <div>Bridge request {selectedJobDetail.bridge_request_id}</div>
                    <div>Workflow accepted {formatDate(selectedJobDetail.workflow_accepted_at)}</div>
                    <div>Workflow owner epoch {formatInlineValue(selectedJobDetail.workflow_owner_epoch)}</div>
                    <div>Stream owner epoch {formatInlineValue(selectedJobDetail.stream_owner_epoch)}</div>
                    <div>
                      Pending repairs{" "}
                      {formatNumber(
                        bridgeDetailQuery.data?.handle.pending_repair_count ?? selectedJobDetail.bridge_surface.pending_repair_count,
                      )}
                    </div>
                    <div>Next repair {bridgeDetailQuery.data?.handle.next_repair ?? selectedJobDetail.bridge_surface.next_repair ?? "-"}</div>
                    <div>
                      Latest query{" "}
                      {bridgeDetailQuery.data?.handle.latest_query_name ??
                        selectedJobDetail.bridge_surface.latest_query_name ??
                        selectedJobDetail.latest_query_name ??
                        "-"}
                    </div>
                    <div>
                      Latest query status{" "}
                      {bridgeDetailQuery.data?.handle.latest_query_status ??
                        selectedJobDetail.bridge_surface.latest_query_status ??
                        selectedJobDetail.latest_query_status ??
                        "-"}
                    </div>
                    <div>Projected state lag {formatLag(selectedJobDetail.stream_surface.slowest_eventual_view_lag_ms)}</div>
                    <div>Cancellation reason {bridgeDetailQuery.data?.handle.cancellation_reason ?? selectedJobDetail.cancellation_reason ?? "-"}</div>
                    {selectedJobDetail.workflow_binding ? (
                      <Link
                        className="button ghost"
                        to={`/runs/${encodeURIComponent(selectedJobDetail.workflow_binding.instance_id)}/${encodeURIComponent(selectedJobDetail.workflow_binding.run_id)}`}
                      >
                        Open workflow run
                      </Link>
                    ) : null}
                  </div>
                </Panel>
              </div>

              <Panel className="nested-panel">
                <h3>Checkpoints</h3>
                <table className="table">
                  <thead>
                    <tr>
                      <th>Checkpoint</th>
                      <th>Status</th>
                      <th>Sequence</th>
                      <th>Reached</th>
                      <th>Accepted</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(bridgeDetailQuery.data?.checkpoints ?? jobDetailQuery.data?.checkpoints ?? []).map((checkpoint) => (
                      <tr key={`${checkpoint.await_request_id}:${checkpoint.checkpoint_name}`}>
                        <td>
                          <strong>{checkpoint.checkpoint_name}</strong>
                          <div className="muted">{checkpoint.await_request_id}</div>
                        </td>
                        <td>
                          <Badge value={checkpoint.status} />
                          <div className="muted">{checkpoint.next_repair ?? "-"}</div>
                        </td>
                        <td>{formatInlineValue(checkpoint.checkpoint_sequence)}</td>
                        <td>{formatDate(checkpoint.reached_at)}</td>
                        <td>{formatDate(checkpoint.accepted_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {(bridgeDetailQuery.data?.checkpoints.length ?? jobDetailQuery.data?.checkpoints.length ?? 0) === 0 ? (
                  <div className="empty">No checkpoint bridge records for this job.</div>
                ) : null}
              </Panel>

              <Panel className="nested-panel">
                <h3>Queries</h3>
                <table className="table">
                  <thead>
                    <tr>
                      <th>Query</th>
                      <th>Status</th>
                      <th>Consistency</th>
                      <th>Requested</th>
                      <th>Completed</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(bridgeDetailQuery.data?.queries ?? jobDetailQuery.data?.queries ?? []).map((query) => (
                      <tr key={query.query_id}>
                        <td>
                          <strong>{query.query_name}</strong>
                          <div className="muted">{query.query_id}</div>
                        </td>
                        <td>
                          <Badge value={query.status} />
                          <div className="muted">{query.next_repair ?? "-"}</div>
                        </td>
                        <td>{query.consistency}</td>
                        <td>{formatDate(query.requested_at)}</td>
                        <td>
                          {formatDate(query.completed_at)}
                          <div className="muted">{query.error ?? jsonPreview(query.output)}</div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {(bridgeDetailQuery.data?.queries.length ?? jobDetailQuery.data?.queries.length ?? 0) === 0 ? (
                  <div className="empty">No bridge queries recorded for this job.</div>
                ) : null}
              </Panel>
            </div>
          ) : (
            <div className="empty">Select a stream job from the tenant index to inspect its lifecycle and bridge state.</div>
          )}
        </Panel>
      </div>

      <Panel>
        {selectedJobDetail ? (
          <div className="stack">
            <div className="row space-between">
              <div>
                <h2>Materialized views</h2>
                <div className="muted">Strong owner reads and eventual projections are explicit on every view query.</div>
              </div>
              <div className="row">
                <select
                  className="select"
                  value={selectedView?.view_name ?? ""}
                  onChange={(event) =>
                    updateParams(searchParams, setSearchParams, {
                      view: event.target.value || null,
                      key: null,
                    })
                  }
                >
                  {selectedViews.map((view) => (
                    <option key={view.view_name} value={view.view_name}>
                      {view.view_name}
                    </option>
                  ))}
                </select>
                <select
                  className="select"
                  value={viewConsistency}
                  onChange={(event) =>
                    updateParams(searchParams, setSearchParams, {
                      consistency: event.target.value,
                      key: null,
                    })
                  }
                >
                  <option value="strong">strong</option>
                  <option value="eventual">eventual</option>
                </select>
              </div>
            </div>

            <div className="grid two">
              <Panel className="nested-panel">
                <h3>Declared views</h3>
                <div className="stack">
                  {selectedViews.map((view) => (
                    <button
                      key={view.view_name}
                      className={`subtle-block ${selectedView?.view_name === view.view_name ? "current" : ""}`.trim()}
                      onClick={() =>
                        updateParams(searchParams, setSearchParams, {
                          view: view.view_name,
                          key: null,
                        })
                      }
                      style={{ textAlign: "left" }}
                    >
                      <div className="row space-between">
                        <strong>{view.view_name}</strong>
                        <Badge value={selectedView?.view_name === view.view_name ? "selected" : "view"} />
                      </div>
                      <div className="muted">
                        {formatNumber(view.projected_key_count)} keys · checkpoint{" "}
                        {formatInlineValue(view.latest_projected_checkpoint_sequence)}
                      </div>
                      <div className="muted">
                        updated {formatDate(view.latest_projected_at)} · lag {formatLag(view.eventual_projection_lag_ms)}
                      </div>
                    </button>
                  ))}
                </div>
              </Panel>

              <Panel className="nested-panel">
                <h3>Selected key</h3>
                {viewValueQuery.data ? (
                  <div className="stack">
                    <ConsistencyBadge consistency={viewValueQuery.data.consistency} source={viewValueQuery.data.source} />
                    <div>Logical key {viewValueQuery.data.logical_key}</div>
                    <div>Checkpoint {formatNumber(viewValueQuery.data.checkpoint_sequence)}</div>
                    <div>Updated {formatDate(viewValueQuery.data.updated_at)}</div>
                    <pre className="code">{JSON.stringify(viewValueQuery.data.output, null, 2)}</pre>
                  </div>
                ) : (
                  <div className="empty">Choose a view key to inspect its current materialized value.</div>
                )}
              </Panel>
            </div>

            <Panel className="nested-panel">
              <div className="row space-between">
                <div>
                  <h3>View entries</h3>
                  <div className="muted">
                    {selectedView?.view_name ?? "No view selected"} · {formatNumber(viewEntriesQuery.data?.entry_count)} total keys
                  </div>
                </div>
                {viewEntriesQuery.data ? (
                  <ConsistencyBadge consistency={viewEntriesQuery.data.consistency} source={viewEntriesQuery.data.source} />
                ) : null}
              </div>
              <table className="table">
                <thead>
                  <tr>
                    <th>Logical key</th>
                    <th>Value preview</th>
                    <th>Checkpoint</th>
                    <th>Updated</th>
                  </tr>
                </thead>
                <tbody>
                  {(viewEntriesQuery.data?.entries ?? []).map((entry) => (
                    <tr
                      key={entry.logical_key}
                      className={selectedViewEntry?.logical_key === entry.logical_key ? "row-selected" : ""}
                      onClick={() => updateParams(searchParams, setSearchParams, { key: entry.logical_key })}
                      style={{ cursor: "pointer" }}
                    >
                      <td>
                        <strong>{entry.logical_key}</strong>
                      </td>
                      <td>{jsonPreview(entry.output)}</td>
                      <td>{formatNumber(entry.checkpoint_sequence)}</td>
                      <td>{formatDate(entry.updated_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {(viewEntriesQuery.data?.entries.length ?? 0) === 0 ? (
                <div className="empty">No entries were materialized for the selected view.</div>
              ) : null}
            </Panel>
          </div>
        ) : (
          <div className="empty">Select a stream job to inspect its materialized state.</div>
        )}
      </Panel>
    </div>
  );
}
