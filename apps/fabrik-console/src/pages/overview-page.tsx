import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";

import { Badge, ConsistencyBadge, Panel } from "../components/ui";
import { api } from "../lib/api";
import { formatDate, formatNumber } from "../lib/format";
import { useTenant } from "../lib/tenant-context";

function labelFromSnakeCase(value: string | null | undefined) {
  if (!value) return "-";
  return value
    .split("_")
    .filter(Boolean)
    .map((segment) => segment[0]?.toUpperCase() + segment.slice(1))
    .join(" ");
}

const evidenceSurfaces = [
  {
    title: "Workflow definitions",
    href: "/workflows",
    summary: "Pinned artifacts, rollout validation, and graph-level causality."
  },
  {
    title: "Builds and routing",
    href: "/builds",
    summary: "Read-only compatibility inventory for rolling restart and mixed-build trust."
  },
  {
    title: "Conformance evidence",
    href: "/conformance",
    summary: "Layer-by-layer fixture results, replay evidence, and trust-case drill-down."
  },
  {
    title: "Replay workbench",
    href: "/replay",
    summary: "Run-scoped replay output, queue preservation, snapshot boundaries, and divergence details."
  },
  {
    title: "Task queues",
    href: "/task-queues",
    summary: "Queue health, pollers, and compatibility-set visibility."
  },
  {
    title: "Streaming ops",
    href: "/streaming",
    summary: "Unified ingress lag, queue pressure, routing mix, and live reducer progress."
  }
];

export function OverviewPage() {
  const { tenantId } = useTenant();
  const overviewQuery = useQuery({
    queryKey: ["overview", tenantId],
    enabled: tenantId !== "",
    queryFn: () => api.getOverview(tenantId)
  });
  const trustQuery = useQuery({
    queryKey: ["trust-summary"],
    queryFn: () => api.getTrustSummary()
  });

  const overview = overviewQuery.data;
  const trustSummary = trustQuery.data;

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <div className="eyebrow">Cluster landing</div>
          <h1>Home</h1>
          <p>Fast operator context for workflow health, queue pressure, replay signal, and recent failures.</p>
        </div>
        <ConsistencyBadge consistency={overview?.consistency} source={overview?.authoritative_source} />
      </header>

      <div className="grid metrics">
        <Panel>
          <div className="muted">Workflow health</div>
          <div className="metric-value">{formatNumber(overview?.total_workflows)}</div>
          <div className="muted">running {formatNumber(overview?.counts_by_status?.running)}</div>
          <div className="muted">failed {formatNumber(overview?.counts_by_status?.failed)}</div>
        </Panel>
        <Panel>
          <div className="muted">Queue pressure</div>
          <div className="metric-value">{formatNumber(overview?.total_backlog)}</div>
          <div className="muted">task queues {formatNumber(overview?.total_task_queues)}</div>
          <div className="muted">pollers {formatNumber(overview?.total_pollers)}</div>
        </Panel>
        <Panel>
          <div className="muted">Replay divergences</div>
          <div className="metric-value">{formatNumber(overview?.replay_divergence_count)}</div>
          <div className="muted">current projection count</div>
        </Panel>
        <Panel>
          <div className="muted">Definition inventory</div>
          <div className="metric-value">{formatNumber(overview?.total_workflow_definitions)}</div>
          <div className="muted">artifacts {formatNumber(overview?.total_workflow_artifacts)}</div>
          <div className="muted">registered builds {formatNumber(overview?.total_registered_builds)}</div>
        </Panel>
      </div>

      <div className="split">
        <Panel>
          <div className="row space-between">
            <div>
              <h2>Recent failures</h2>
              <div className="muted">Latest failed workflow projections in this tenant.</div>
            </div>
            <Link className="button ghost" to="/runs?status=failed">
              Open failed runs
            </Link>
          </div>
          <table className="table">
            <thead>
              <tr>
                <th>Workflow</th>
                <th>Status</th>
                <th>Routing</th>
                <th>Queue</th>
                <th>Updated</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {(overview?.recent_failures ?? []).map((item) => (
                <tr key={item.instance_id}>
                  <td>
                    <strong>{item.definition_id}</strong>
                    <div className="muted">{item.instance_id}</div>
                  </td>
                <td>{item.status}</td>
                <td>
                  <Badge value={item.routing_status} />
                  <div className="muted">engine {labelFromSnakeCase(item.execution_path)}</div>
                  {item.fast_path_rejection_reason ? (
                    <div className="muted">fallback {labelFromSnakeCase(item.fast_path_rejection_reason)}</div>
                  ) : null}
                </td>
                <td>{item.workflow_task_queue}</td>
                  <td>{formatDate(item.updated_at)}</td>
                  <td>
                    <Link className="button ghost" to={`/runs/${item.instance_id}/${item.run_id}`}>
                      Inspect
                    </Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {(overview?.recent_failures ?? []).length === 0 ? <div className="empty">No failed workflows in the current projection.</div> : null}
        </Panel>

        <div className="stack">
          <Panel>
            <div className="row space-between">
              <div>
                <h2>Hot task queues</h2>
                <div className="muted">Backlog and poller pressure from the queue inspection layer.</div>
              </div>
              <Link className="button ghost" to="/task-queues">
                View queues
              </Link>
            </div>
            <div className="stack">
              {(overview?.hottest_task_queues ?? []).map((queue) => (
                <Link
                  key={`${queue.queue_kind}:${queue.task_queue}`}
                  className="panel nested-panel"
                  to={`/task-queues?queue_kind=${queue.queue_kind}&task_queue=${queue.task_queue}`}
                >
                  <div className="row space-between">
                    <strong>{queue.task_queue}</strong>
                    <span className="muted">{queue.queue_kind}</span>
                  </div>
                  <div className="row">
                    <span>backlog {formatNumber(queue.backlog)}</span>
                    <span className="muted">pollers {formatNumber(queue.poller_count)}</span>
                  </div>
                  <div className="muted">oldest queued work {formatDate(queue.oldest_backlog_at)}</div>
                </Link>
              ))}
            </div>
          </Panel>

          <Panel>
            <h2>{trustSummary?.title ?? "Temporal TS subset trust"}</h2>
            <div className="stack">
              <div className="muted">
                {trustSummary?.goal ?? "No conformance trust snapshot has been generated yet."}
              </div>
              {(trustSummary?.confidence_bands ?? []).map((band) => (
                <div key={band.confidence_class} className="subtle-block">
                  <div className="row space-between">
                    <strong>{band.confidence_class}</strong>
                    <Badge value={band.confidence_class} />
                  </div>
                  <div className="muted">{formatNumber(band.count)} feature(s)</div>
                  <div className="muted">{band.features.join(", ")}</div>
                </div>
              ))}
              {trustSummary ? (
                <div className="muted">
                  Generated {formatDate(trustSummary.generated_at)} · status {trustSummary.status}
                </div>
              ) : null}
            </div>
          </Panel>

          <Panel>
            <h2>Conformance Program</h2>
            <div className="stack">
              {(trustSummary?.reports ?? []).map((layer) => (
                <Link
                  key={layer.layer_id}
                  className="subtle-block"
                  to={`/conformance?layer=${encodeURIComponent(layer.layer_id)}`}
                >
                  <div className="row space-between">
                    <strong>{layer.title}</strong>
                    <Badge value={layer.failed_count === 0 ? "passing" : "failing"} />
                  </div>
                  <div className="muted">{layer.purpose}</div>
                  <div className="muted">
                    Cases {formatNumber(layer.case_count)} · passed {formatNumber(layer.passed_count)} · failed {formatNumber(layer.failed_count)}
                  </div>
                </Link>
              ))}
              {trustSummary == null ? <div className="muted">No conformance report snapshot loaded.</div> : null}
            </div>
          </Panel>

          <Panel>
            <h2>Evidence Surfaces</h2>
            <div className="stack">
              {evidenceSurfaces.map((surface) => (
                <Link key={surface.href} className="panel nested-panel" to={surface.href}>
                  <div className="row space-between">
                    <strong>{surface.title}</strong>
                    <span className="muted">Read-only</span>
                  </div>
                  <div className="muted">{surface.summary}</div>
                </Link>
              ))}
            </div>
          </Panel>
        </div>
      </div>
    </div>
  );
}
