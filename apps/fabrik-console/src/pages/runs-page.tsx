import { useQuery } from "@tanstack/react-query";
import { Link, useSearchParams } from "react-router-dom";

import { Badge, ConsistencyBadge, Panel } from "../components/ui";
import { api } from "../lib/api";
import { formatDate, formatDuration, formatNumber } from "../lib/format";
import { useTenant } from "../lib/tenant-context";

const PAGE_SIZE = 25;

function metadataPreview(value: unknown) {
  if (value == null || typeof value !== "object" || Array.isArray(value)) return "-";
  const entries = Object.entries(value as Record<string, unknown>).slice(0, 2);
  if (entries.length === 0) return "-";
  return entries
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

function labelFromSnakeCase(value: string | null | undefined) {
  if (!value) return "-";
  return value
    .split("_")
    .filter(Boolean)
    .map((segment) => segment[0]?.toUpperCase() + segment.slice(1))
    .join(" ");
}

function workflowModeLabel(admissionMode: string | null | undefined) {
  if (!admissionMode) return "-";
  if (admissionMode === "inline_fast_start") {
    return "Inline Fast Start";
  }
  if (admissionMode === "durable_workflow") {
    return "Durable Workflow";
  }
  return labelFromSnakeCase(admissionMode);
}

function buildParams(searchParams: URLSearchParams) {
  const params = new URLSearchParams();
  for (const key of [
    "status",
    "routing_status",
    "definition_id",
    "instance_id",
    "run_id",
    "task_queue",
    "q",
    "memo_key",
    "memo_value",
    "search_attribute_key",
    "search_attribute_value",
  ]) {
    const value = searchParams.get(key)?.trim();
    if (value) params.set(key, value);
  }
  params.set("limit", String(PAGE_SIZE));
  params.set("offset", searchParams.get("offset") ?? "0");
  return params;
}

export function RunsPage() {
  const { tenantId } = useTenant();
  const [searchParams, setSearchParams] = useSearchParams();
  const params = buildParams(searchParams);
  const runsQuery = useQuery({
    queryKey: ["runs", tenantId, params.toString()],
    enabled: tenantId !== "",
    queryFn: () => api.listRuns(tenantId, params)
  });

  const offset = Number(params.get("offset") ?? "0");

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <div className="eyebrow">Investigation</div>
          <h1>Runs</h1>
          <p>Inspect active and historical runs across the current tenant with real query-backed filters.</p>
        </div>
        <ConsistencyBadge consistency={runsQuery.data?.consistency} source={runsQuery.data?.authoritative_source} />
      </header>

      <Panel>
        <div className="filters-grid">
          <input
            className="input"
            placeholder="Search workflow, run, current state"
            value={searchParams.get("q") ?? ""}
            onChange={(event) => {
              const next = new URLSearchParams(searchParams);
              const value = event.target.value.trimStart();
              value ? next.set("q", value) : next.delete("q");
              next.delete("offset");
              setSearchParams(next);
            }}
          />
          <input
            className="input"
            placeholder="Definition id"
            value={searchParams.get("definition_id") ?? ""}
            onChange={(event) => {
              const next = new URLSearchParams(searchParams);
              const value = event.target.value.trimStart();
              value ? next.set("definition_id", value) : next.delete("definition_id");
              next.delete("offset");
              setSearchParams(next);
            }}
          />
          <input
            className="input"
            placeholder="Workflow id"
            value={searchParams.get("instance_id") ?? ""}
            onChange={(event) => {
              const next = new URLSearchParams(searchParams);
              const value = event.target.value.trimStart();
              value ? next.set("instance_id", value) : next.delete("instance_id");
              next.delete("offset");
              setSearchParams(next);
            }}
          />
          <input
            className="input"
            placeholder="Run id"
            value={searchParams.get("run_id") ?? ""}
            onChange={(event) => {
              const next = new URLSearchParams(searchParams);
              const value = event.target.value.trimStart();
              value ? next.set("run_id", value) : next.delete("run_id");
              next.delete("offset");
              setSearchParams(next);
            }}
          />
          <input
            className="input"
            placeholder="Task queue"
            value={searchParams.get("task_queue") ?? ""}
            onChange={(event) => {
              const next = new URLSearchParams(searchParams);
              const value = event.target.value.trimStart();
              value ? next.set("task_queue", value) : next.delete("task_queue");
              next.delete("offset");
              setSearchParams(next);
            }}
          />
          <input
            className="input"
            placeholder="Memo key"
            value={searchParams.get("memo_key") ?? ""}
            onChange={(event) => {
              const next = new URLSearchParams(searchParams);
              const value = event.target.value.trimStart();
              value ? next.set("memo_key", value) : next.delete("memo_key");
              next.delete("offset");
              setSearchParams(next);
            }}
          />
          <input
            className="input"
            placeholder="Memo value"
            value={searchParams.get("memo_value") ?? ""}
            onChange={(event) => {
              const next = new URLSearchParams(searchParams);
              const value = event.target.value.trimStart();
              value ? next.set("memo_value", value) : next.delete("memo_value");
              next.delete("offset");
              setSearchParams(next);
            }}
          />
          <input
            className="input"
            placeholder="Search attribute key"
            value={searchParams.get("search_attribute_key") ?? ""}
            onChange={(event) => {
              const next = new URLSearchParams(searchParams);
              const value = event.target.value.trimStart();
              value ? next.set("search_attribute_key", value) : next.delete("search_attribute_key");
              next.delete("offset");
              setSearchParams(next);
            }}
          />
          <input
            className="input"
            placeholder="Search attribute value"
            value={searchParams.get("search_attribute_value") ?? ""}
            onChange={(event) => {
              const next = new URLSearchParams(searchParams);
              const value = event.target.value.trimStart();
              value ? next.set("search_attribute_value", value) : next.delete("search_attribute_value");
              next.delete("offset");
              setSearchParams(next);
            }}
          />
          <select
            className="select"
            value={searchParams.get("status") ?? ""}
            onChange={(event) => {
              const next = new URLSearchParams(searchParams);
              const value = event.target.value;
              value ? next.set("status", value) : next.delete("status");
              next.delete("offset");
              setSearchParams(next);
            }}
          >
            <option value="">All statuses</option>
            <option value="triggered">triggered</option>
            <option value="running">running</option>
            <option value="completed">completed</option>
            <option value="failed">failed</option>
            <option value="cancelled">cancelled</option>
            <option value="terminated">terminated</option>
            <option value="continued">continued</option>
            <option value="closed">closed</option>
          </select>
          <select
            className="select"
            value={searchParams.get("routing_status") ?? ""}
            onChange={(event) => {
              const next = new URLSearchParams(searchParams);
              const value = event.target.value;
              value ? next.set("routing_status", value) : next.delete("routing_status");
              next.delete("offset");
              setSearchParams(next);
            }}
          >
            <option value="">All routing states</option>
            <option value="sticky_active">sticky_active</option>
            <option value="queue_default_active">queue_default_active</option>
            <option value="sticky_incompatible_fallback_required">
              sticky_incompatible_fallback_required
            </option>
            <option value="sticky_artifact_mismatch_fallback_required">
              sticky_artifact_mismatch_fallback_required
            </option>
          </select>
        </div>
      </Panel>

      <Panel>
        <div className="row space-between table-toolbar">
          <div className="muted">Showing {formatNumber(runsQuery.data?.items.length)} of {formatNumber(runsQuery.data?.run_count)}</div>
          <div className="row">
            <button
              className="button ghost"
              disabled={offset === 0}
              onClick={() => {
                const next = new URLSearchParams(searchParams);
                next.set("offset", String(Math.max(0, offset - PAGE_SIZE)));
                setSearchParams(next);
              }}
            >
              Previous
            </button>
            <button
              className="button ghost"
              disabled={!runsQuery.data?.page.has_more}
              onClick={() => {
                const next = new URLSearchParams(searchParams);
                next.set("offset", String(offset + PAGE_SIZE));
                setSearchParams(next);
              }}
            >
              Next
            </button>
          </div>
        </div>
        <table className="table">
          <thead>
            <tr>
              <th>Workflow</th>
              <th>Status</th>
              <th>Routing</th>
              <th>Current step</th>
              <th>Queue</th>
              <th>Triage</th>
              <th>Started</th>
              <th>Last transition</th>
              <th>Duration</th>
              <th>Events</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {(runsQuery.data?.items ?? []).map((item) => (
              <tr key={`${item.instance_id}:${item.run_id}`}>
                <td>
                  <strong>{item.definition_id}</strong>
                  <div className="muted">{item.instance_id}</div>
                  <div className="muted">{item.run_id}</div>
                  <div className="muted">memo {metadataPreview(item.memo)}</div>
                  <div className="muted">search {metadataPreview(item.search_attributes)}</div>
                </td>
                <td>
                  <Badge value={item.status} />
                </td>
                <td>
                  <Badge value={item.routing_status} />
                  <div className="muted">mode {workflowModeLabel(item.admission_mode)}</div>
                  {item.fast_path_rejection_reason ? (
                    <div className="muted">fast-start fallback {labelFromSnakeCase(item.fast_path_rejection_reason)}</div>
                  ) : null}
                </td>
                <td>
                  {item.current_state ?? "-"}
                  <div className="muted">{item.last_event_type ?? "no event projection"}</div>
                </td>
                <td>{item.workflow_task_queue}</td>
                <td>
                  <div className="stack">
                    <Link
                      className="button ghost"
                      to={`/task-queues?queue_kind=workflow&task_queue=${encodeURIComponent(item.workflow_task_queue)}`}
                    >
                      Queue
                    </Link>
                    <Link
                      className="button ghost"
                      to={`/builds?queue_kind=workflow&task_queue=${encodeURIComponent(item.workflow_task_queue)}`}
                    >
                      Builds
                    </Link>
                  </div>
                </td>
                <td>{formatDate(item.started_at)}</td>
                <td>{formatDate(item.last_transition_at)}</td>
                <td>{formatDuration(item.started_at, item.closed_at)}</td>
                <td>{formatNumber(item.event_count)}</td>
                <td>
                  <Link className="button ghost" to={`/runs/${item.instance_id}/${item.run_id}`}>
                    Inspect
                  </Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {(runsQuery.data?.items ?? []).length === 0 ? <div className="empty">No runs matched the current filters.</div> : null}
      </Panel>
    </div>
  );
}
