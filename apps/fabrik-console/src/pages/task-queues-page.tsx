import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";

import { Badge, ConsistencyBadge, Panel } from "../components/ui";
import { api } from "../lib/api";
import { formatDate, formatNumber } from "../lib/format";
import { useTenant } from "../lib/tenant-context";

export function TaskQueuesPage() {
  const { tenantId } = useTenant();
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedKind = searchParams.get("queue_kind") ?? "";
  const selectedQueue = searchParams.get("task_queue") ?? "";

  const queuesQuery = useQuery({
    queryKey: ["task-queues", tenantId],
    enabled: tenantId !== "",
    queryFn: () => api.listTaskQueues(tenantId)
  });
  const inspectionQuery = useQuery({
    queryKey: ["task-queue-inspection", tenantId, selectedKind, selectedQueue],
    enabled: tenantId !== "" && selectedKind !== "" && selectedQueue !== "",
    queryFn: () => api.getTaskQueue(tenantId, selectedKind, selectedQueue)
  });

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <div className="eyebrow">Queue health</div>
          <h1>Task Queues</h1>
          <p>Read-only queue inventory, backlog inspection, pollers, and compatibility visibility.</p>
        </div>
        <ConsistencyBadge consistency={queuesQuery.data?.consistency} source={queuesQuery.data?.authoritative_source} />
      </header>

      <div className="split">
        <Panel>
          <table className="table">
            <thead>
              <tr>
                <th>Queue</th>
                <th>Kind</th>
                <th>Backlog</th>
                <th>Pollers</th>
                <th>Builds</th>
              </tr>
            </thead>
            <tbody>
              {(queuesQuery.data?.items ?? []).map((item) => (
                <tr
                  key={`${item.queue_kind}:${item.task_queue}`}
                  className={selectedKind === item.queue_kind && selectedQueue === item.task_queue ? "row-selected" : ""}
                  onClick={() => setSearchParams({ queue_kind: item.queue_kind, task_queue: item.task_queue })}
                  style={{ cursor: "pointer" }}
                >
                  <td>
                    <strong>{item.task_queue}</strong>
                    <div className="muted">{item.oldest_backlog_at ? `oldest ${formatDate(item.oldest_backlog_at)}` : "no backlog age"}</div>
                  </td>
                  <td>
                    <Badge value={item.queue_kind} />
                  </td>
                  <td>{formatNumber(item.backlog)}</td>
                  <td>{formatNumber(item.poller_count)}</td>
                  <td>{formatNumber(item.registered_build_count)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Panel>

        <Panel>
          {inspectionQuery.data ? (
            <div className="stack">
              <div className="row space-between">
                <h2>{inspectionQuery.data.task_queue}</h2>
                <Badge value={inspectionQuery.data.queue_kind} />
              </div>

              <div className="grid two">
                <Panel className="nested-panel">
                  <h3>Overview</h3>
                  <div className="stack">
                    <div>Backlog {formatNumber(inspectionQuery.data.backlog)}</div>
                    <div>Oldest queued work {formatDate(inspectionQuery.data.oldest_backlog_at)}</div>
                    <div>Default compatibility set {inspectionQuery.data.default_set_id ?? "-"}</div>
                    <div>Throughput backend {inspectionQuery.data.throughput_policy?.backend ?? "-"}</div>
                  </div>
                </Panel>
                <Panel className="nested-panel">
                  <h3>Polling health</h3>
                  <div className="stack">
                    <div>Sticky hit rate {inspectionQuery.data.sticky_effectiveness?.sticky_hit_rate?.toFixed(2) ?? "-"}</div>
                    <div>Sticky fallback rate {inspectionQuery.data.sticky_effectiveness?.sticky_fallback_rate?.toFixed(2) ?? "-"}</div>
                    <div>Schedule-to-start avg {inspectionQuery.data.activity_completion_metrics?.avg_schedule_to_start_latency_ms?.toFixed(0) ?? "-"} ms</div>
                    <div>Start-to-close avg {inspectionQuery.data.activity_completion_metrics?.avg_start_to_close_latency_ms?.toFixed(0) ?? "-"} ms</div>
                  </div>
                </Panel>
              </div>

              <Panel className="nested-panel">
                <h3>Compatibility sets</h3>
                <div className="stack">
                  {(inspectionQuery.data.compatibility_sets ?? []).map((set) => (
                    <div key={set.set_id} className="subtle-block">
                      <div className="row space-between">
                        <strong>{set.set_id}</strong>
                        <Badge value={set.is_default ? "active" : "scheduled"} />
                      </div>
                      <div className="muted">{set.build_ids.join(", ") || "No builds"}</div>
                    </div>
                  ))}
                </div>
              </Panel>

              <Panel className="nested-panel">
                <h3>Pollers</h3>
                <table className="table">
                  <thead>
                    <tr>
                      <th>Poller</th>
                      <th>Build</th>
                      <th>Partition</th>
                      <th>Last seen</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(inspectionQuery.data.pollers ?? []).map((poller) => (
                      <tr key={poller.poller_id}>
                        <td>{poller.poller_id}</td>
                        <td>{poller.build_id}</td>
                        <td>{poller.partition_id ?? "-"}</td>
                        <td>{formatDate(poller.last_seen_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </Panel>
            </div>
          ) : (
            <div className="empty">Select a queue to inspect its health and poller state.</div>
          )}
        </Panel>
      </div>
    </div>
  );
}
