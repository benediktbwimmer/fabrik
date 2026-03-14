import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { toast } from "sonner";

import { Badge, Panel } from "../components/ui";
import {
  api,
  type TopicAdapter,
  type TopicAdapterDetailResponse,
  type TopicAdapterPreviewResponse,
  type WatchEvent
} from "../lib/api";
import { formatDate, formatNumber } from "../lib/format";
import { useTenant } from "../lib/tenant-context";

type OwnershipTransition = {
  change_kind: string;
  previous_owner_id: string | null;
  previous_owner_epoch: number | null;
  ownership: TopicAdapterDetailResponse["ownership"];
};

export function TopicAdaptersPage() {
  const { tenantId } = useTenant();
  const client = useQueryClient();
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedAdapterId = searchParams.get("adapter_id") ?? "";
  const [controlPending, setControlPending] = useState(false);
  const [latestOwnershipTransition, setLatestOwnershipTransition] = useState<OwnershipTransition | null>(null);
  const [liveProcessedRate, setLiveProcessedRate] = useState<number | null>(null);
  const [previewInput, setPreviewInput] = useState('{\n  "instance_id": "order-42",\n  "payload": {\n    "amount": 125,\n    "currency": "EUR"\n  },\n  "request_id": "req-42"\n}');
  const [previewPending, setPreviewPending] = useState(false);
  const [previewResult, setPreviewResult] = useState<TopicAdapterPreviewResponse | null>(null);

  const adaptersQuery = useQuery({
    queryKey: ["topic-adapters", tenantId],
    enabled: tenantId !== "",
    queryFn: () => api.listTopicAdapters(tenantId)
  });
  const detailQuery = useQuery({
    queryKey: ["topic-adapter", tenantId, selectedAdapterId],
    enabled: tenantId !== "" && selectedAdapterId !== "",
    queryFn: () => api.getTopicAdapter(tenantId, selectedAdapterId)
  });

  useEffect(() => {
    if (tenantId === "" || selectedAdapterId === "") return;
    if (typeof EventSource === "undefined") return;
    setLatestOwnershipTransition(null);
    setLiveProcessedRate(null);
    let previousSample: { processedCount: number; observedAt: number } | null = null;
    const source = new EventSource(api.topicAdapterWatchPath(tenantId, selectedAdapterId));

    const applyDetail = (next: TopicAdapterDetailResponse) => {
      client.setQueryData(["topic-adapter", tenantId, selectedAdapterId], next);
      client.setQueryData<TopicAdapter[] | undefined>(["topic-adapters", tenantId], (current) =>
        (current ?? []).map((adapter) =>
          adapter.adapter_id === next.adapter.adapter_id ? next.adapter : adapter
        )
      );
    };

    source.addEventListener("adapter_state_changed", (event) => {
      const envelope = JSON.parse((event as MessageEvent).data) as WatchEvent<TopicAdapterDetailResponse>;
      const observedAt = Date.parse(envelope.occurred_at);
      if (!Number.isNaN(observedAt)) {
        if (previousSample) {
          const deltaCount = envelope.body.adapter.processed_count - previousSample.processedCount;
          const deltaSeconds = (observedAt - previousSample.observedAt) / 1000;
          if (deltaCount >= 0 && deltaSeconds > 0) {
            setLiveProcessedRate(deltaCount / deltaSeconds);
          }
        }
        previousSample = {
          processedCount: envelope.body.adapter.processed_count,
          observedAt
        };
      }
      applyDetail(envelope.body);
    });
    source.addEventListener("adapter_lag", (event) => {
      const envelope = JSON.parse((event as MessageEvent).data) as WatchEvent<{ lag: TopicAdapterDetailResponse["lag"] }>;
      client.setQueryData<TopicAdapterDetailResponse | undefined>(
        ["topic-adapter", tenantId, selectedAdapterId],
        (current) => (current ? { ...current, lag: envelope.body.lag } : current)
      );
    });
    source.addEventListener("adapter_ownership_changed", (event) => {
      const envelope = JSON.parse((event as MessageEvent).data) as WatchEvent<OwnershipTransition>;
      setLatestOwnershipTransition(envelope.body);
      client.setQueryData<TopicAdapterDetailResponse | undefined>(
        ["topic-adapter", tenantId, selectedAdapterId],
        (current) => (current ? { ...current, ownership: envelope.body.ownership } : current)
      );
    });
    source.addEventListener("adapter_dead_letter", () => {
      void client.invalidateQueries({ queryKey: ["topic-adapter", tenantId, selectedAdapterId] });
      void client.invalidateQueries({ queryKey: ["topic-adapters", tenantId] });
    });
    source.addEventListener("adapter_runtime_error", (event) => {
      const envelope = JSON.parse((event as MessageEvent).data) as WatchEvent<{ error: string }>;
      toast.error(envelope.body.error);
    });

    return () => source.close();
  }, [client, selectedAdapterId, tenantId]);

  async function setPaused(isPaused: boolean) {
    if (tenantId === "" || selectedAdapterId === "") return;
    setControlPending(true);
    try {
      if (isPaused) {
        await api.pauseTopicAdapter(tenantId, selectedAdapterId);
      } else {
        await api.resumeTopicAdapter(tenantId, selectedAdapterId);
      }
      toast.success(`Adapter ${isPaused ? "paused" : "resumed"}`);
      await client.invalidateQueries({ queryKey: ["topic-adapters", tenantId] });
      await client.invalidateQueries({ queryKey: ["topic-adapter", tenantId, selectedAdapterId] });
    } catch (error) {
      toast.error(String(error));
    } finally {
      setControlPending(false);
    }
  }

  async function runPreview() {
    if (tenantId === "" || selectedAdapterId === "") return;
    setPreviewPending(true);
    try {
      const payload = JSON.parse(previewInput) as unknown;
      const result = await api.previewTopicAdapter(tenantId, selectedAdapterId, { payload });
      setPreviewResult(result);
      if (!result.ok) {
        toast.error(result.error?.detail ?? "Preview failed");
      }
    } catch (error) {
      const message = error instanceof SyntaxError ? error.message : String(error);
      setPreviewResult(null);
      toast.error(message);
    } finally {
      setPreviewPending(false);
    }
  }

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <div className="eyebrow">Streaming ingress</div>
          <h1>Topic Adapters</h1>
          <p>Kafka and Redpanda topic consumers that start workflows or queue signals with durable offsets and stored dead letters.</p>
        </div>
      </header>

      <div className="split">
        <Panel>
          <table className="table">
            <thead>
              <tr>
                <th>Adapter</th>
                <th>Action</th>
                <th>Processed</th>
                <th>Failed</th>
              </tr>
            </thead>
            <tbody>
              {(adaptersQuery.data ?? []).map((adapter) => (
                <tr
                  key={adapter.adapter_id}
                  className={selectedAdapterId === adapter.adapter_id ? "row-selected" : ""}
                  onClick={() => setSearchParams({ adapter_id: adapter.adapter_id })}
                  style={{ cursor: "pointer" }}
                >
                  <td>
                    <strong>{adapter.adapter_id}</strong>
                    <div className="muted">
                      {adapter.adapter_kind} / {adapter.topic_name} ({formatNumber(adapter.topic_partitions)} partitions)
                    </div>
                    <div className="muted">{adapter.workflow_task_queue ? `queue ${adapter.workflow_task_queue}` : "queue inferred by workflow"}</div>
                    {adapter.is_paused ? <div className="muted">paused</div> : null}
                  </td>
                  <td>
                    <Badge value={adapter.action} />
                  </td>
                  <td>{formatNumber(adapter.processed_count)}</td>
                  <td>{formatNumber(adapter.failed_count)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Panel>

        <Panel>
          {detailQuery.data ? (
            <div className="stack">
              <div className="row space-between">
                <h2>{detailQuery.data.adapter.adapter_id}</h2>
                <div className="row">
                  {detailQuery.data.adapter.is_paused ? <Badge value="paused" /> : <Badge value="active" />}
                  <Badge value={detailQuery.data.adapter.action} />
                </div>
              </div>

              <Panel className="nested-panel">
                <h3>Config</h3>
                <div className="grid two">
                  <div className="stack">
                    <div>Kind {detailQuery.data.adapter.adapter_kind}</div>
                    <div>Topic {detailQuery.data.adapter.topic_name}</div>
                    <div>Partitions {formatNumber(detailQuery.data.adapter.topic_partitions)}</div>
                    <div>Brokers {detailQuery.data.adapter.brokers}</div>
                    <div>Queue {detailQuery.data.adapter.workflow_task_queue ?? "-"}</div>
                  </div>
                  <div className="stack">
                    <div>Definition {detailQuery.data.adapter.definition_id ?? "-"}</div>
                    <div>Signal type {detailQuery.data.adapter.signal_type ?? "-"}</div>
                    <div>Instance pointer {detailQuery.data.adapter.workflow_instance_id_json_pointer ?? "-"}</div>
                    <div>Payload pointer {detailQuery.data.adapter.payload_json_pointer ?? "<whole message>"}</div>
                    <div>Payload template {detailQuery.data.adapter.payload_template_json ? "configured" : "-"}</div>
                    <div>Request id pointer {detailQuery.data.adapter.request_id_json_pointer ?? "<derived from offset>"}</div>
                  </div>
                </div>
                <div className="grid two">
                  <div className="stack">
                    <div>Memo pointer {detailQuery.data.adapter.memo_json_pointer ?? "-"}</div>
                    <div>Memo template {detailQuery.data.adapter.memo_template_json ? "configured" : "-"}</div>
                  </div>
                  <div className="stack">
                    <div>Search pointer {detailQuery.data.adapter.search_attributes_json_pointer ?? "-"}</div>
                    <div>Search template {detailQuery.data.adapter.search_attributes_template_json ? "configured" : "-"}</div>
                  </div>
                </div>
                {detailQuery.data.adapter.payload_template_json ? (
                  <pre>{JSON.stringify(detailQuery.data.adapter.payload_template_json, null, 2)}</pre>
                ) : null}
                {detailQuery.data.adapter.memo_template_json ? (
                  <pre>{JSON.stringify(detailQuery.data.adapter.memo_template_json, null, 2)}</pre>
                ) : null}
                {detailQuery.data.adapter.search_attributes_template_json ? (
                  <pre>{JSON.stringify(detailQuery.data.adapter.search_attributes_template_json, null, 2)}</pre>
                ) : null}
                <div className="row">
                  <button
                    className="button ghost"
                    disabled={controlPending}
                    onClick={() => void setPaused(!detailQuery.data.adapter.is_paused)}
                  >
                    {detailQuery.data.adapter.is_paused ? "Resume adapter" : "Pause adapter"}
                  </button>
                </div>
              </Panel>

              <Panel className="nested-panel">
                <h3>Preview</h3>
                <div className="stack">
                  <div className="muted">Paste a sample topic payload to preview the workflow start or signal request before enabling the adapter.</div>
                  <textarea
                    value={previewInput}
                    onChange={(event) => setPreviewInput(event.target.value)}
                    rows={12}
                    style={{ width: "100%", fontFamily: "monospace" }}
                  />
                  <div className="row">
                    <button className="button ghost" disabled={previewPending} onClick={() => void runPreview()}>
                      {previewPending ? "Running preview..." : "Run preview"}
                    </button>
                  </div>
                  {previewResult?.error ? (
                    <div className="muted">
                      Error in {previewResult.error.field}: {previewResult.error.detail}
                    </div>
                  ) : null}
                  {previewResult?.diagnostics.length ? (
                    <div className="stack">
                      {previewResult.diagnostics.map((diagnostic) => (
                        <div key={`${diagnostic.field}:${diagnostic.mode}:${diagnostic.detail}`} className="muted">
                          {diagnostic.field}: {diagnostic.mode} ({diagnostic.detail})
                        </div>
                      ))}
                    </div>
                  ) : null}
                  {previewResult?.dispatch ? <pre>{JSON.stringify(previewResult.dispatch, null, 2)}</pre> : null}
                </div>
              </Panel>

              <Panel className="nested-panel">
                <h3>State</h3>
                <div className="grid two">
                  <div className="stack">
                    <div>Processed {formatNumber(detailQuery.data.adapter.processed_count)}</div>
                    <div>Failed {formatNumber(detailQuery.data.adapter.failed_count)}</div>
                    <div>
                      Processed/sec {liveProcessedRate == null ? "-" : formatNumber(liveProcessedRate)}
                    </div>
                    <div>
                      Lag {detailQuery.data.lag.available ? formatNumber(detailQuery.data.lag.total_lag_records) : "-"}
                    </div>
                    <div>Owner {detailQuery.data.ownership?.owner_id ?? "-"}</div>
                    <div>Owner epoch {detailQuery.data.ownership ? formatNumber(detailQuery.data.ownership.owner_epoch) : "-"}</div>
                    <div>Last processed {formatDate(detailQuery.data.adapter.last_processed_at)}</div>
                    <div>Last error at {formatDate(detailQuery.data.adapter.last_error_at)}</div>
                  </div>
                  <div className="stack">
                    <div>Handoffs {formatNumber(detailQuery.data.adapter.ownership_handoff_count)}</div>
                    <div>Last handoff {formatDate(detailQuery.data.adapter.last_handoff_at)}</div>
                    <div>
                      Last takeover latency {detailQuery.data.adapter.last_takeover_latency_ms == null ? "-" : `${formatNumber(detailQuery.data.adapter.last_takeover_latency_ms)} ms`}
                    </div>
                    <div>Dead-letter policy {detailQuery.data.adapter.dead_letter_policy}</div>
                    <div>Updated {formatDate(detailQuery.data.adapter.updated_at)}</div>
                    <div>Created {formatDate(detailQuery.data.adapter.created_at)}</div>
                    <div>Lease expires {formatDate(detailQuery.data.ownership?.lease_expires_at ?? null)}</div>
                    <div>Owner acquired {formatDate(detailQuery.data.ownership?.acquired_at ?? null)}</div>
                    <div>Last error {detailQuery.data.adapter.last_error ?? "-"}</div>
                    <div>Lag error {detailQuery.data.lag.error ?? "-"}</div>
                  </div>
                </div>
              </Panel>

              <Panel className="nested-panel">
                <h3>Ownership</h3>
                {latestOwnershipTransition ? (
                  <div className="grid two">
                    <div className="stack">
                      <div>Latest change {latestOwnershipTransition.change_kind}</div>
                      <div>Previous owner {latestOwnershipTransition.previous_owner_id ?? "-"}</div>
                      <div>
                        Previous epoch {latestOwnershipTransition.previous_owner_epoch == null ? "-" : formatNumber(latestOwnershipTransition.previous_owner_epoch)}
                      </div>
                    </div>
                    <div className="stack">
                      <div>Current owner {latestOwnershipTransition.ownership?.owner_id ?? "-"}</div>
                      <div>
                        Current epoch {latestOwnershipTransition.ownership ? formatNumber(latestOwnershipTransition.ownership.owner_epoch) : "-"}
                      </div>
                      <div>
                        Transition at {formatDate(latestOwnershipTransition.ownership?.last_transition_at ?? null)}
                      </div>
                    </div>
                  </div>
                ) : (
                  <div className="muted">No live ownership transition seen for this session.</div>
                )}
              </Panel>

              <Panel className="nested-panel">
                <h3>Offsets</h3>
                <table className="table">
                  <thead>
                    <tr>
                      <th>Partition</th>
                      <th>Lag</th>
                      <th>Offset</th>
                      <th>Latest</th>
                      <th>Updated</th>
                    </tr>
                  </thead>
                  <tbody>
                    {detailQuery.data.lag.partitions.map((partition) => {
                      const offset = detailQuery.data.offsets.find(
                        (candidate) => candidate.partition_id === partition.partition_id
                      );
                      return (
                      <tr key={partition.partition_id}>
                        <td>{partition.partition_id}</td>
                        <td>{formatNumber(partition.lag_records)}</td>
                        <td>{formatNumber(offset?.log_offset ?? partition.committed_offset ?? -1)}</td>
                        <td>{formatNumber(partition.latest_offset)}</td>
                        <td>{formatDate(offset?.updated_at ?? null)}</td>
                      </tr>
                      );
                    })}
                  </tbody>
                </table>
              </Panel>

              <Panel className="nested-panel">
                <h3>Recent Dead Letters</h3>
                <table className="table">
                  <thead>
                    <tr>
                      <th>Partition</th>
                      <th>Offset</th>
                      <th>Error</th>
                      <th>When</th>
                    </tr>
                  </thead>
                  <tbody>
                    {detailQuery.data.recent_dead_letters.map((entry) => (
                      <tr key={`${entry.partition_id}:${entry.log_offset}`}>
                        <td>{entry.partition_id}</td>
                        <td>{formatNumber(entry.log_offset)}</td>
                        <td>{entry.error}</td>
                        <td>{formatDate(entry.occurred_at)}</td>
                      </tr>
                    ))}
                    {detailQuery.data.recent_dead_letters.length === 0 ? (
                      <tr>
                        <td colSpan={4} className="muted">
                          No dead letters
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </Panel>
            </div>
          ) : (
            <div className="muted">Select an adapter to inspect offsets and dead letters.</div>
          )}
        </Panel>
      </div>
    </div>
  );
}
