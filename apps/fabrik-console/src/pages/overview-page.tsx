import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";

import { Badge, ConsistencyBadge, Panel } from "../components/ui";
import { api } from "../lib/api";
import { formatDate, formatNumber } from "../lib/format";
import { useTenant } from "../lib/tenant-context";

export function OverviewPage() {
  const { tenantId } = useTenant();
  const overviewQuery = useQuery({
    queryKey: ["overview", tenantId],
    enabled: tenantId !== "",
    queryFn: () => api.getOverview(tenantId)
  });

  const overview = overviewQuery.data;

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
            <h2>Unavailable in milestone 1</h2>
            <div className="stack">
              <div className="muted">Runtime overview, incidents, deploy impact, and migration surfaces are intentionally deferred until they have real backing data.</div>
            </div>
          </Panel>
        </div>
      </div>
    </div>
  );
}
