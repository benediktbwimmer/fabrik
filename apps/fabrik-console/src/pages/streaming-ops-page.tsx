import { useQueries, useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";

import { Badge, ConsistencyBadge, Panel } from "../components/ui";
import { api, type RunListItem, type TopicAdapterDetailResponse, type WorkflowBulkBatch } from "../lib/api";
import { formatDate, formatInlineValue, formatNumber } from "../lib/format";
import { useTenant } from "../lib/tenant-context";

const ACTIVE_BATCH_STATUSES = new Set(["scheduled", "running"]);

type ActiveBatchRow = {
  run: RunListItem;
  batch: WorkflowBulkBatch;
};

function labelFromSnakeCase(value: string | null | undefined) {
  if (!value) return "-";
  return value
    .split("_")
    .filter(Boolean)
    .map((segment) => segment[0]?.toUpperCase() + segment.slice(1))
    .join(" ");
}

function reducerOutputPreview(value: unknown) {
  if (value == null) return "-";
  if (typeof value === "number" || typeof value === "boolean" || typeof value === "string") {
    return String(value);
  }
  if (Array.isArray(value)) {
    return value.slice(0, 3).map((item) => formatInlineValue(item)).join(", ");
  }
  if (typeof value === "object") {
    const record = value as Record<string, unknown>;
    if (Array.isArray(record.sample) && typeof record.total === "number") {
      const sample = record.sample.slice(0, 2).map((item) => formatInlineValue(item)).join(", ");
      return `sample(${sample || "-"}) · total ${formatNumber(record.total)}`;
    }
    const numericEntries = Object.entries(record).filter(([, entryValue]) => typeof entryValue === "number");
    if (numericEntries.length > 0) {
      return numericEntries
        .sort((left, right) => Number(right[1]) - Number(left[1]))
        .slice(0, 3)
        .map(([key, count]) => `${key}:${formatNumber(Number(count))}`)
        .join(" · ");
    }
  }
  return formatInlineValue(value);
}

function queuePressureScore(queue: {
  backlog: number;
  is_paused: boolean;
  is_draining: boolean;
  stream_v2_capacity_state: string | null;
}) {
  return (
    queue.backlog +
    (queue.is_paused ? 25_000 : 0) +
    (queue.is_draining ? 10_000 : 0) +
    (queue.stream_v2_capacity_state && queue.stream_v2_capacity_state !== "available" ? 5_000 : 0)
  );
}

export function StreamingOpsPage() {
  const { tenantId } = useTenant();

  const queuesQuery = useQuery({
    queryKey: ["task-queues", tenantId, "streaming-ops"],
    enabled: tenantId !== "",
    refetchInterval: 5_000,
    queryFn: () => api.listTaskQueues(tenantId)
  });

  const adaptersQuery = useQuery({
    queryKey: ["topic-adapters", tenantId, "streaming-ops"],
    enabled: tenantId !== "",
    refetchInterval: 5_000,
    queryFn: () => api.listTopicAdapters(tenantId)
  });

  const activeRunsQuery = useQuery({
    queryKey: ["runs", tenantId, "streaming-ops-active"],
    enabled: tenantId !== "",
    refetchInterval: 5_000,
    queryFn: () => {
      const params = new URLSearchParams({
        status: "running",
        limit: "12",
        offset: "0"
      });
      return api.listRuns(tenantId, params);
    }
  });

  const activeRuns = activeRunsQuery.data?.items ?? [];
  const adapterIds = (adaptersQuery.data ?? [])
    .slice()
    .sort((left, right) => Number(right.failed_count > left.failed_count) || right.updated_at.localeCompare(left.updated_at))
    .slice(0, 6)
    .map((adapter) => adapter.adapter_id);

  const adapterDetailQueries = useQueries({
    queries: adapterIds.map((adapterId) => ({
      queryKey: ["topic-adapter", tenantId, adapterId, "streaming-ops"],
      enabled: tenantId !== "",
      refetchInterval: 5_000,
      queryFn: () => api.getTopicAdapter(tenantId, adapterId)
    }))
  });

  const batchQueries = useQueries({
    queries: activeRuns.slice(0, 8).map((run) => ({
      queryKey: ["run-bulk-batches", tenantId, run.instance_id, run.run_id, "streaming-ops"],
      enabled: tenantId !== "",
      refetchInterval: 4_000,
      queryFn: () => api.getWorkflowBulkBatches(tenantId, run.instance_id, run.run_id)
    }))
  });

  const adapterDetails = adapterDetailQueries
    .map((query, index) => ({ adapterId: adapterIds[index], detail: query.data }))
    .filter((entry): entry is { adapterId: string; detail: TopicAdapterDetailResponse } => entry.detail != null);

  const queueItems = [...(queuesQuery.data?.items ?? [])].sort((left, right) => queuePressureScore(right) - queuePressureScore(left));
  const hotQueues = queueItems.slice(0, 6);

  const activeBatches = batchQueries.flatMap((query, index) => {
    const run = activeRuns[index];
    if (!run || !query.data) return [];
    return query.data.batches
      .filter((batch) => ACTIVE_BATCH_STATUSES.has(batch.status))
      .map((batch): ActiveBatchRow => ({ run, batch }));
  });

  activeBatches.sort((left, right) => right.batch.updated_at.localeCompare(left.batch.updated_at));

  const streamV2BatchCount = activeBatches.filter((entry) => entry.batch.selected_backend === "stream-v2").length;
  const fastLaneBatchCount = activeBatches.filter((entry) => entry.batch.fast_lane_enabled).length;
  const totalAdapterLag = adapterDetails.reduce((sum, entry) => {
    if (!entry.detail.lag.available || entry.detail.lag.total_lag_records == null) return sum;
    return sum + entry.detail.lag.total_lag_records;
  }, 0);
  const unresolvedDeadLetters = adapterDetails.reduce(
    (sum, entry) => sum + entry.detail.recent_dead_letters.filter((deadLetter) => deadLetter.resolved_at == null).length,
    0
  );
  const constrainedQueues = queueItems.filter(
    (queue) => queue.is_paused || queue.is_draining || (queue.stream_v2_capacity_state && queue.stream_v2_capacity_state !== "available")
  ).length;

  const routingMix = activeBatches.reduce<Record<string, number>>((counts, entry) => {
    const key = entry.batch.selected_backend || "unknown";
    counts[key] = (counts[key] ?? 0) + 1;
    return counts;
  }, {});

  const reducerMix = activeBatches.reduce<Record<string, number>>((counts, entry) => {
    const key = entry.batch.reducer_kind || entry.batch.reducer || "unknown";
    counts[key] = (counts[key] ?? 0) + 1;
    return counts;
  }, {});

  const consistency = queuesQuery.data?.consistency ?? activeRunsQuery.data?.consistency ?? "eventual";
  const source = queuesQuery.data?.authoritative_source ?? activeRunsQuery.data?.authoritative_source ?? "projection";

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <div className="eyebrow">Streaming control plane</div>
          <h1>Streaming Ops</h1>
          <p>Unified ingress, queue, and fan-out/fan-in visibility for the current tenant.</p>
        </div>
        <ConsistencyBadge consistency={consistency} source={source} />
      </header>

      <div className="grid metrics">
        <Panel>
          <div className="muted">Ingress adapters</div>
          <div className="metric-value">{formatNumber(adaptersQuery.data?.length)}</div>
          <div className="muted">lag {formatNumber(totalAdapterLag)}</div>
          <div className="muted">open dead letters {formatNumber(unresolvedDeadLetters)}</div>
        </Panel>
        <Panel>
          <div className="muted">Queue pressure</div>
          <div className="metric-value">{formatNumber(queueItems.reduce((sum, queue) => sum + queue.backlog, 0))}</div>
          <div className="muted">queues {formatNumber(queueItems.length)}</div>
          <div className="muted">constrained {formatNumber(constrainedQueues)}</div>
        </Panel>
        <Panel>
          <div className="muted">Active runs</div>
          <div className="metric-value">{formatNumber(activeRuns.length)}</div>
          <div className="muted">stream batches {formatNumber(activeBatches.length)}</div>
          <div className="muted">stream-v2 {formatNumber(streamV2BatchCount)}</div>
        </Panel>
        <Panel>
          <div className="muted">Reducer path</div>
          <div className="metric-value">{formatNumber(fastLaneBatchCount)}</div>
          <div className="muted">fast-lane batches</div>
          <div className="muted">reducers {Object.keys(reducerMix).length}</div>
        </Panel>
      </div>

      <div className="split">
        <div className="stack">
          <Panel>
            <div className="row space-between">
              <div>
                <h2>Ingress Health</h2>
                <div className="muted">Top adapters by operator relevance: lag, failures, and handoffs.</div>
              </div>
              <Link className="button ghost" to="/topic-adapters">
                Open adapters
              </Link>
            </div>
            <table className="table">
              <thead>
                <tr>
                  <th>Adapter</th>
                  <th>Action</th>
                  <th>Lag</th>
                  <th>DLQ</th>
                  <th>Owner</th>
                  <th>Last activity</th>
                </tr>
              </thead>
              <tbody>
                {adapterDetails.map(({ adapterId, detail }) => {
                  const unresolved = detail.recent_dead_letters.filter((entry) => entry.resolved_at == null).length;
                  return (
                    <tr key={adapterId}>
                      <td>
                        <Link to={`/topic-adapters?adapter_id=${encodeURIComponent(adapterId)}`}>
                          <strong>{adapterId}</strong>
                        </Link>
                        <div className="muted">{detail.adapter.topic_name}</div>
                      </td>
                      <td>
                        <Badge value={detail.adapter.action} />
                        <div className="muted">{detail.adapter.workflow_task_queue ?? detail.adapter.signal_type ?? "-"}</div>
                      </td>
                      <td>
                        {detail.lag.available ? formatNumber(detail.lag.total_lag_records ?? 0) : "-"}
                        <div className="muted">{detail.lag.available ? `${detail.lag.partitions.length} partitions` : detail.lag.error ?? "lag unavailable"}</div>
                      </td>
                      <td>
                        {formatNumber(unresolved)}
                        <div className="muted">
                          failed {formatNumber(detail.adapter.failed_count)} · replayed{" "}
                          {formatNumber(detail.recent_dead_letters.reduce((sum, entry) => sum + entry.replay_count, 0))}
                        </div>
                      </td>
                      <td>
                        {detail.ownership?.owner_id ?? "-"}
                        <div className="muted">
                          epoch {detail.ownership?.owner_epoch ?? "-"} · handoffs {formatNumber(detail.adapter.ownership_handoff_count)}
                        </div>
                      </td>
                      <td>
                        {formatDate(detail.adapter.last_processed_at ?? detail.adapter.updated_at)}
                        {detail.adapter.last_error ? <div className="muted">{detail.adapter.last_error}</div> : null}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {adapterDetails.length === 0 ? <div className="empty">No topic adapters are configured for this tenant.</div> : null}
          </Panel>

          <Panel>
            <div className="row space-between">
              <div>
                <h2>Queue Pressure</h2>
                <div className="muted">Backlog, backend choice, and stream-v2 pressure across task queues.</div>
              </div>
              <Link className="button ghost" to="/task-queues">
                Open queues
              </Link>
            </div>
            <table className="table">
              <thead>
                <tr>
                  <th>Queue</th>
                  <th>Backlog</th>
                  <th>Backend</th>
                  <th>Capacity</th>
                  <th>Pollers</th>
                  <th>Flags</th>
                </tr>
              </thead>
              <tbody>
                {hotQueues.map((queue) => (
                  <tr key={`${queue.queue_kind}:${queue.task_queue}`}>
                    <td>
                      <Link
                        to={`/task-queues?queue_kind=${encodeURIComponent(queue.queue_kind)}&task_queue=${encodeURIComponent(queue.task_queue)}`}
                      >
                        <strong>{queue.task_queue}</strong>
                      </Link>
                      <div className="muted">{queue.queue_kind}</div>
                    </td>
                    <td>
                      {formatNumber(queue.backlog)}
                      <div className="muted">oldest {formatDate(queue.oldest_backlog_at)}</div>
                    </td>
                    <td>
                      <Badge value={queue.effective_throughput_backend ?? queue.throughput_backend ?? "unknown"} />
                      <div className="muted">preferred {queue.throughput_backend ?? "-"}</div>
                    </td>
                    <td>
                      {queue.stream_v2_capacity_state ?? "-"}
                      <div className="muted">active chunks {formatNumber(queue.active_stream_v2_chunks ?? 0)}</div>
                    </td>
                    <td>
                      {formatNumber(queue.poller_count)}
                      <div className="muted">builds {formatNumber(queue.registered_build_count)}</div>
                    </td>
                    <td>
                      <div className="row">
                        {queue.is_paused ? <Badge value="paused" /> : null}
                        {!queue.is_paused && queue.is_draining ? <Badge value="draining" /> : null}
                        {!queue.is_paused && !queue.is_draining && queue.backlog > 0 ? <Badge value="queued" /> : null}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {hotQueues.length === 0 ? <div className="empty">No task queues found for this tenant.</div> : null}
          </Panel>
        </div>

        <div className="stack">
          <Panel>
            <div className="row space-between">
              <div>
                <h2>Active Streaming Batches</h2>
                <div className="muted">Live reducer progress from recent running workflows.</div>
              </div>
              <Link className="button ghost" to="/runs?status=running">
                Open runs
              </Link>
            </div>
            <table className="table">
              <thead>
                <tr>
                  <th>Workflow</th>
                  <th>Batch</th>
                  <th>Progress</th>
                  <th>Backend</th>
                  <th>Reducer</th>
                  <th>Updated</th>
                </tr>
              </thead>
              <tbody>
                {activeBatches.slice(0, 12).map(({ run, batch }) => {
                  const settled = batch.succeeded_items + batch.failed_items + batch.cancelled_items;
                  const progress = batch.total_items > 0 ? (settled / batch.total_items) * 100 : 0;
                  return (
                    <tr key={`${run.instance_id}:${run.run_id}:${batch.batch_id}`}>
                      <td>
                        <Link to={`/runs/${run.instance_id}/${run.run_id}`}>
                          <strong>{run.definition_id}</strong>
                        </Link>
                        <div className="muted">{run.instance_id}</div>
                      </td>
                      <td>
                        <strong>{batch.batch_id}</strong>
                        <div className="muted">{batch.activity_type}</div>
                      </td>
                      <td>
                        {progress.toFixed(1)}%
                        <div className="muted">
                          {formatNumber(settled)} / {formatNumber(batch.total_items)}
                        </div>
                      </td>
                      <td>
                        <Badge value={batch.selected_backend} />
                        <div className="muted">{labelFromSnakeCase(batch.routing_reason)}</div>
                      </td>
                      <td>
                        <div>{labelFromSnakeCase(batch.reducer_kind)}</div>
                        <div className="muted">{reducerOutputPreview(batch.reducer_output)}</div>
                      </td>
                      <td>{formatDate(batch.updated_at)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {activeBatches.length === 0 ? <div className="empty">No active throughput batches were found in the recent running set.</div> : null}
          </Panel>

          <Panel>
            <h2>Routing Mix</h2>
            <div className="grid two">
              <div className="stack">
                {Object.entries(routingMix)
                  .sort((left, right) => right[1] - left[1])
                  .map(([backend, count]) => (
                    <div key={backend} className="subtle-block">
                      <div className="row space-between">
                        <strong>{backend}</strong>
                        <Badge value={backend} />
                      </div>
                      <div className="muted">{formatNumber(count)} active batch(es)</div>
                    </div>
                  ))}
                {Object.keys(routingMix).length === 0 ? <div className="empty">No routing mix available yet.</div> : null}
              </div>
              <div className="stack">
                {Object.entries(reducerMix)
                  .sort((left, right) => right[1] - left[1])
                  .map(([reducer, count]) => (
                    <div key={reducer} className="subtle-block">
                      <div className="row space-between">
                        <strong>{labelFromSnakeCase(reducer)}</strong>
                        <span className="muted">{formatNumber(count)}</span>
                      </div>
                      <div className="muted">active reducer path</div>
                    </div>
                  ))}
                {Object.keys(reducerMix).length === 0 ? <div className="empty">No reducer activity available yet.</div> : null}
              </div>
            </div>
          </Panel>
        </div>
      </div>
    </div>
  );
}
