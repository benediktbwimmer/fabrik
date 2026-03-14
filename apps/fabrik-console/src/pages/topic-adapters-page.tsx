import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { toast } from "sonner";

import { Badge, Panel } from "../components/ui";
import {
  api,
  type TopicAdapter,
  type TopicAdapterDetailResponse,
  type TopicAdapterPreviewResponse,
  type TopicAdapterUpsertRequest,
  type WatchEvent
} from "../lib/api";
import { formatDate, formatNumber } from "../lib/format";
import { useTenant } from "../lib/tenant-context";

const NEW_ADAPTER_KEY = "__new__";

type OwnershipTransition = {
  change_kind: string;
  previous_owner_id: string | null;
  previous_owner_epoch: number | null;
  ownership: TopicAdapterDetailResponse["ownership"];
};

type PayloadMode = "whole" | "pointer" | "template";
type OptionalMappingMode = "none" | "pointer" | "template";
type PointerMode = "derived" | "pointer";

type AdapterDraft = {
  adapterId: string;
  adapterKind: string;
  brokers: string;
  topicName: string;
  topicPartitions: string;
  action: string;
  definitionId: string;
  signalType: string;
  workflowTaskQueue: string;
  workflowInstanceIdPointer: string;
  payloadMode: PayloadMode;
  payloadPointer: string;
  payloadTemplate: string;
  memoMode: OptionalMappingMode;
  memoPointer: string;
  memoTemplate: string;
  searchMode: OptionalMappingMode;
  searchPointer: string;
  searchTemplate: string;
  requestIdMode: PointerMode;
  requestIdPointer: string;
  dedupeKeyMode: PointerMode;
  dedupeKeyPointer: string;
  deadLetterPolicy: string;
  isPaused: boolean;
};

export function TopicAdaptersPage() {
  const { tenantId } = useTenant();
  const client = useQueryClient();
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedAdapterId = searchParams.get("adapter_id") ?? "";
  const isCreating = selectedAdapterId === NEW_ADAPTER_KEY;
  const watchedAdapterId = selectedAdapterId !== "" && !isCreating ? selectedAdapterId : "";
  const [controlPending, setControlPending] = useState(false);
  const [savePending, setSavePending] = useState(false);
  const [previewPending, setPreviewPending] = useState(false);
  const [latestOwnershipTransition, setLatestOwnershipTransition] = useState<OwnershipTransition | null>(null);
  const [liveProcessedRate, setLiveProcessedRate] = useState<number | null>(null);
  const [draft, setDraft] = useState<AdapterDraft>(createEmptyDraft());
  const [formError, setFormError] = useState<string | null>(null);
  const [previewInput, setPreviewInput] = useState(
    '{\n  "instance_id": "order-42",\n  "payload": {\n    "amount": 125,\n    "currency": "EUR"\n  },\n  "request_id": "req-42"\n}'
  );
  const [previewResult, setPreviewResult] = useState<TopicAdapterPreviewResponse | null>(null);

  const adaptersQuery = useQuery({
    queryKey: ["topic-adapters", tenantId],
    enabled: tenantId !== "",
    queryFn: () => api.listTopicAdapters(tenantId)
  });
  const detailQuery = useQuery({
    queryKey: ["topic-adapter", tenantId, watchedAdapterId],
    enabled: tenantId !== "" && watchedAdapterId !== "",
    queryFn: () => api.getTopicAdapter(tenantId, watchedAdapterId)
  });

  const selectedListAdapter = useMemo(
    () => (adaptersQuery.data ?? []).find((adapter) => adapter.adapter_id === watchedAdapterId) ?? null,
    [adaptersQuery.data, watchedAdapterId]
  );
  const selectedAdapter = detailQuery.data?.adapter ?? selectedListAdapter;
  const sourceDraft = useMemo(
    () => (isCreating ? createEmptyDraft() : selectedAdapter ? draftFromAdapter(selectedAdapter) : null),
    [isCreating, selectedAdapter]
  );
  const draftDirty = sourceDraft ? JSON.stringify(draft) !== JSON.stringify(sourceDraft) : false;

  useEffect(() => {
    if (isCreating) {
      setDraft(createEmptyDraft());
      setFormError(null);
      setPreviewResult(null);
      return;
    }
    if (selectedAdapterId === "" || !selectedAdapter) {
      return;
    }
    setDraft(draftFromAdapter(selectedAdapter));
    setFormError(null);
    setPreviewResult(null);
  }, [isCreating, selectedAdapter, selectedAdapterId]);

  useEffect(() => {
    if (tenantId === "" || watchedAdapterId === "") return;
    if (typeof EventSource === "undefined") return;
    setLatestOwnershipTransition(null);
    setLiveProcessedRate(null);
    let previousSample: { processedCount: number; observedAt: number } | null = null;
    const source = new EventSource(api.topicAdapterWatchPath(tenantId, watchedAdapterId));

    const applyDetail = (next: TopicAdapterDetailResponse) => {
      client.setQueryData(["topic-adapter", tenantId, watchedAdapterId], next);
      client.setQueryData<TopicAdapter[] | undefined>(["topic-adapters", tenantId], (current) =>
        upsertAdapterSummary(current, next.adapter)
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
        ["topic-adapter", tenantId, watchedAdapterId],
        (current) => (current ? { ...current, lag: envelope.body.lag } : current)
      );
    });
    source.addEventListener("adapter_ownership_changed", (event) => {
      const envelope = JSON.parse((event as MessageEvent).data) as WatchEvent<OwnershipTransition>;
      setLatestOwnershipTransition(envelope.body);
      client.setQueryData<TopicAdapterDetailResponse | undefined>(
        ["topic-adapter", tenantId, watchedAdapterId],
        (current) => (current ? { ...current, ownership: envelope.body.ownership } : current)
      );
    });
    source.addEventListener("adapter_dead_letter", () => {
      void client.invalidateQueries({ queryKey: ["topic-adapter", tenantId, watchedAdapterId] });
      void client.invalidateQueries({ queryKey: ["topic-adapters", tenantId] });
    });
    source.addEventListener("adapter_runtime_error", (event) => {
      const envelope = JSON.parse((event as MessageEvent).data) as WatchEvent<{ error: string }>;
      toast.error(envelope.body.error);
    });

    return () => source.close();
  }, [client, tenantId, watchedAdapterId]);

  function updateDraft(patch: Partial<AdapterDraft>) {
    setDraft((current) => ({ ...current, ...patch }));
    setFormError(null);
    setPreviewResult(null);
  }

  function selectAdapter(adapterId: string) {
    setSearchParams({ adapter_id: adapterId });
  }

  async function saveDraft() {
    if (tenantId === "") return;
    setSavePending(true);
    try {
      const adapterId = normalizeRequired(draft.adapterId, "adapter id");
      const payload = buildTopicAdapterPayload(draft);
      const saved = await api.setTopicAdapter(tenantId, adapterId, payload);
      const nextDraft = draftFromAdapter(saved);
      setDraft(nextDraft);
      setFormError(null);
      setPreviewResult(null);
      client.setQueryData<TopicAdapter[] | undefined>(["topic-adapters", tenantId], (current) =>
        upsertAdapterSummary(current, saved)
      );
      void client.invalidateQueries({ queryKey: ["topic-adapter", tenantId, adapterId] });
      setSearchParams({ adapter_id: adapterId });
      toast.success(isCreating ? "Adapter created" : "Adapter updated");
    } catch (error) {
      const message = extractErrorMessage(error);
      setFormError(message);
      toast.error(message);
    } finally {
      setSavePending(false);
    }
  }

  async function setPaused(isPaused: boolean) {
    if (tenantId === "" || watchedAdapterId === "") return;
    setControlPending(true);
    try {
      if (isPaused) {
        await api.pauseTopicAdapter(tenantId, watchedAdapterId);
      } else {
        await api.resumeTopicAdapter(tenantId, watchedAdapterId);
      }
      toast.success(`Adapter ${isPaused ? "paused" : "resumed"}`);
      await client.invalidateQueries({ queryKey: ["topic-adapters", tenantId] });
      await client.invalidateQueries({ queryKey: ["topic-adapter", tenantId, watchedAdapterId] });
    } catch (error) {
      toast.error(extractErrorMessage(error));
    } finally {
      setControlPending(false);
    }
  }

  async function runPreview() {
    if (tenantId === "") return;
    setPreviewPending(true);
    try {
      const adapterId = draft.adapterId.trim();
      const adapterPayload = buildTopicAdapterPayload(draft);
      const payload = JSON.parse(previewInput) as unknown;
      const result = await api.previewTopicAdapterDraft(tenantId, {
        adapter_id: adapterId === "" ? undefined : adapterId,
        adapter: adapterPayload,
        payload
      });
      setPreviewResult(result);
      setFormError(null);
      if (!result.ok && result.error) {
        toast.error(`Preview failed: ${result.error.detail}`);
      }
    } catch (error) {
      const message = error instanceof SyntaxError ? error.message : extractErrorMessage(error);
      setPreviewResult(null);
      toast.error(message);
    } finally {
      setPreviewPending(false);
    }
  }

  function resetDraft() {
    setDraft(sourceDraft ?? createEmptyDraft());
    setFormError(null);
    setPreviewResult(null);
  }

  const canShowDetail = !isCreating && detailQuery.data;
  const actionIsSignal = draft.action === "signal_workflow";

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <div className="eyebrow">Streaming ingress</div>
          <h1>Topic Adapters</h1>
          <p>Author Kafka and Redpanda topic consumers that start workflows or queue signals with durable offsets, previewable mappings, and live operator state.</p>
        </div>
        <button className="button primary" onClick={() => selectAdapter(NEW_ADAPTER_KEY)}>
          New adapter
        </button>
      </header>

      <div className="split">
        <Panel>
          <div className="table-toolbar row space-between">
            <div className="muted">
              {(adaptersQuery.data ?? []).length === 0 ? "No adapters yet." : `${formatNumber((adaptersQuery.data ?? []).length)} adapters`}
            </div>
            <button className="button ghost" onClick={() => selectAdapter(NEW_ADAPTER_KEY)}>
              Create draft
            </button>
          </div>
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
              <tr
                className={isCreating ? "row-selected" : ""}
                onClick={() => selectAdapter(NEW_ADAPTER_KEY)}
                style={{ cursor: "pointer" }}
              >
                <td>
                  <strong>New adapter draft</strong>
                  <div className="muted">Create an unsaved adapter and preview mappings before writing it.</div>
                </td>
                <td>
                  <Badge value="draft" />
                </td>
                <td>-</td>
                <td>-</td>
              </tr>
              {(adaptersQuery.data ?? []).map((adapter) => (
                <tr
                  key={adapter.adapter_id}
                  className={watchedAdapterId === adapter.adapter_id ? "row-selected" : ""}
                  onClick={() => selectAdapter(adapter.adapter_id)}
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
          <div className="stack">
            <div className="row space-between">
              <h2>{isCreating ? "New adapter" : draft.adapterId || "Topic adapter"}</h2>
              <div className="row">
                {draftDirty ? <Badge value="unsaved" /> : null}
                {isCreating ? <Badge value="draft" /> : detailQuery.data?.adapter.is_paused ? <Badge value="paused" /> : <Badge value="active" />}
                <Badge value={draft.action} />
              </div>
            </div>

            <Panel className="nested-panel">
              <div className="row space-between">
                <h3>Editor</h3>
                <div className="row">
                  <button className="button ghost" disabled={savePending} onClick={resetDraft}>
                    Reset
                  </button>
                  <button className="button primary" disabled={savePending} onClick={() => void saveDraft()}>
                    {savePending ? "Saving..." : isCreating ? "Create adapter" : "Save changes"}
                  </button>
                </div>
              </div>

              <div className="editor-grid">
                <Field label="Adapter id" help="Stable control-plane id used for ownership, offsets, and preview request ids.">
                  <input
                    className="input"
                    value={draft.adapterId}
                    onChange={(event) => updateDraft({ adapterId: event.target.value })}
                    placeholder="orders"
                  />
                </Field>
                <Field label="Action" help="Choose whether the adapter starts workflows or signals existing ones.">
                  <select
                    className="select"
                    value={draft.action}
                    onChange={(event) => updateDraft({ action: event.target.value })}
                  >
                    <option value="start_workflow">start_workflow</option>
                    <option value="signal_workflow">signal_workflow</option>
                  </select>
                </Field>
                <Field label="Adapter kind">
                  <select
                    className="select"
                    value={draft.adapterKind}
                    onChange={(event) => updateDraft({ adapterKind: event.target.value })}
                  >
                    <option value="redpanda">redpanda</option>
                  </select>
                </Field>
                <Field label="Dead-letter policy">
                  <select
                    className="select"
                    value={draft.deadLetterPolicy}
                    onChange={(event) => updateDraft({ deadLetterPolicy: event.target.value })}
                  >
                    <option value="store">store</option>
                  </select>
                </Field>
                <Field label="Brokers" help="Comma-separated broker addresses.">
                  <input
                    className="input"
                    value={draft.brokers}
                    onChange={(event) => updateDraft({ brokers: event.target.value })}
                    placeholder="127.0.0.1:9092"
                  />
                </Field>
                <Field label="Topic name">
                  <input
                    className="input"
                    value={draft.topicName}
                    onChange={(event) => updateDraft({ topicName: event.target.value })}
                    placeholder="orders.events"
                  />
                </Field>
                <Field label="Topic partitions">
                  <input
                    className="input"
                    value={draft.topicPartitions}
                    onChange={(event) => updateDraft({ topicPartitions: event.target.value })}
                    inputMode="numeric"
                  />
                </Field>
                {actionIsSignal ? (
                  <Field label="Signal type" help="Signal queued for the target workflow instance.">
                    <input
                      className="input"
                      value={draft.signalType}
                      onChange={(event) => updateDraft({ signalType: event.target.value })}
                      placeholder="external.approved"
                    />
                  </Field>
                ) : (
                  <Field label="Definition id" help="Workflow definition started for each consumed topic record.">
                    <input
                      className="input"
                      value={draft.definitionId}
                      onChange={(event) => updateDraft({ definitionId: event.target.value })}
                      placeholder="order-workflow"
                    />
                  </Field>
                )}
                <Field
                  label="Workflow task queue"
                  help={actionIsSignal ? "Signals ignore task queue routing." : "Optional explicit queue for started workflows."}
                >
                  <input
                    className="input"
                    value={draft.workflowTaskQueue}
                    onChange={(event) => updateDraft({ workflowTaskQueue: event.target.value })}
                    placeholder={actionIsSignal ? "not used for signals" : "orders"}
                    disabled={actionIsSignal}
                  />
                </Field>
                <Field
                  label="Workflow instance pointer"
                  help={actionIsSignal ? "Required JSON pointer to the target workflow instance id." : "Optional JSON pointer for explicit workflow instance ids."}
                >
                  <input
                    className="input"
                    value={draft.workflowInstanceIdPointer}
                    onChange={(event) => updateDraft({ workflowInstanceIdPointer: event.target.value })}
                    placeholder="/instance_id"
                  />
                </Field>
                <Field label="Request id mode" help="Use a payload field or derive from topic partition and offset.">
                  <select
                    className="select"
                    value={draft.requestIdMode}
                    onChange={(event) => updateDraft({ requestIdMode: event.target.value as PointerMode })}
                  >
                    <option value="derived">derived from partition + offset</option>
                    <option value="pointer">json pointer</option>
                  </select>
                </Field>
                {draft.requestIdMode === "pointer" ? (
                  <Field label="Request id pointer">
                    <input
                      className="input"
                      value={draft.requestIdPointer}
                      onChange={(event) => updateDraft({ requestIdPointer: event.target.value })}
                      placeholder="/request_id"
                    />
                  </Field>
                ) : null}
                {actionIsSignal ? (
                  <>
                    <Field label="Dedupe key mode" help="Queue-time dedupe key for signals.">
                      <select
                        className="select"
                        value={draft.dedupeKeyMode}
                        onChange={(event) => updateDraft({ dedupeKeyMode: event.target.value as PointerMode })}
                      >
                        <option value="derived">default to request id</option>
                        <option value="pointer">json pointer</option>
                      </select>
                    </Field>
                    {draft.dedupeKeyMode === "pointer" ? (
                      <Field label="Dedupe key pointer">
                        <input
                          className="input"
                          value={draft.dedupeKeyPointer}
                          onChange={(event) => updateDraft({ dedupeKeyPointer: event.target.value })}
                          placeholder="/dedupe_key"
                        />
                      </Field>
                    ) : null}
                  </>
                ) : null}
              </div>

              <div className="editor-section">
                <h3>{actionIsSignal ? "Signal payload mapping" : "Workflow input mapping"}</h3>
                <Field label="Mapping mode">
                  <select
                    className="select"
                    value={draft.payloadMode}
                    onChange={(event) => updateDraft({ payloadMode: event.target.value as PayloadMode })}
                  >
                    <option value="whole">whole topic payload</option>
                    <option value="pointer">json pointer</option>
                    <option value="template">template</option>
                  </select>
                </Field>
                {draft.payloadMode === "pointer" ? (
                  <Field label="Payload pointer">
                    <input
                      className="input"
                      value={draft.payloadPointer}
                      onChange={(event) => updateDraft({ payloadPointer: event.target.value })}
                      placeholder="/payload"
                    />
                  </Field>
                ) : null}
                {draft.payloadMode === "template" ? (
                  <Field label="Payload template" help='Deterministic JSON shaping with "$from" and optional "$optional".'>
                    <textarea
                      className="textarea code-input"
                      value={draft.payloadTemplate}
                      onChange={(event) => updateDraft({ payloadTemplate: event.target.value })}
                      rows={10}
                    />
                  </Field>
                ) : null}
              </div>

              {!actionIsSignal ? (
                <div className="grid two">
                  <div className="editor-section">
                    <h3>Memo mapping</h3>
                    <Field label="Mapping mode">
                      <select
                        className="select"
                        value={draft.memoMode}
                        onChange={(event) => updateDraft({ memoMode: event.target.value as OptionalMappingMode })}
                      >
                        <option value="none">none</option>
                        <option value="pointer">json pointer</option>
                        <option value="template">template</option>
                      </select>
                    </Field>
                    {draft.memoMode === "pointer" ? (
                      <Field label="Memo pointer">
                        <input
                          className="input"
                          value={draft.memoPointer}
                          onChange={(event) => updateDraft({ memoPointer: event.target.value })}
                          placeholder="/memo"
                        />
                      </Field>
                    ) : null}
                    {draft.memoMode === "template" ? (
                      <Field label="Memo template">
                        <textarea
                          className="textarea code-input"
                          value={draft.memoTemplate}
                          onChange={(event) => updateDraft({ memoTemplate: event.target.value })}
                          rows={8}
                        />
                      </Field>
                    ) : null}
                  </div>

                  <div className="editor-section">
                    <h3>Search attributes mapping</h3>
                    <Field label="Mapping mode">
                      <select
                        className="select"
                        value={draft.searchMode}
                        onChange={(event) => updateDraft({ searchMode: event.target.value as OptionalMappingMode })}
                      >
                        <option value="none">none</option>
                        <option value="pointer">json pointer</option>
                        <option value="template">template</option>
                      </select>
                    </Field>
                    {draft.searchMode === "pointer" ? (
                      <Field label="Search pointer">
                        <input
                          className="input"
                          value={draft.searchPointer}
                          onChange={(event) => updateDraft({ searchPointer: event.target.value })}
                          placeholder="/search"
                        />
                      </Field>
                    ) : null}
                    {draft.searchMode === "template" ? (
                      <Field label="Search template">
                        <textarea
                          className="textarea code-input"
                          value={draft.searchTemplate}
                          onChange={(event) => updateDraft({ searchTemplate: event.target.value })}
                          rows={8}
                        />
                      </Field>
                    ) : null}
                  </div>
                </div>
              ) : null}

              {formError ? <div className="muted">Validation: {formError}</div> : null}
            </Panel>

            <Panel className="nested-panel">
              <div className="row space-between">
                <h3>Preview draft</h3>
                <button className="button ghost" disabled={previewPending} onClick={() => void runPreview()}>
                  {previewPending ? "Running preview..." : "Run preview"}
                </button>
              </div>
              <div className="muted">
                Preview uses the current draft, including unsaved mapping changes, before anything is written to the control plane.
              </div>
              <textarea
                className="textarea code-input"
                value={previewInput}
                onChange={(event) => setPreviewInput(event.target.value)}
                rows={12}
              />
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
              {previewResult?.dispatch ? <pre className="code">{JSON.stringify(previewResult.dispatch, null, 2)}</pre> : null}
            </Panel>

            {canShowDetail ? (
              <>
                <Panel className="nested-panel">
                  <div className="row space-between">
                    <h3>Live state</h3>
                    <button
                      className="button ghost"
                      disabled={controlPending}
                      onClick={() => void setPaused(!detailQuery.data.adapter.is_paused)}
                    >
                      {detailQuery.data.adapter.is_paused ? "Resume adapter" : "Pause adapter"}
                    </button>
                  </div>
                  <div className="grid two">
                    <div className="stack">
                      <div>Processed {formatNumber(detailQuery.data.adapter.processed_count)}</div>
                      <div>Failed {formatNumber(detailQuery.data.adapter.failed_count)}</div>
                      <div>Processed/sec {liveProcessedRate == null ? "-" : formatNumber(liveProcessedRate)}</div>
                      <div>Lag {detailQuery.data.lag.available ? formatNumber(detailQuery.data.lag.total_lag_records) : "-"}</div>
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
                        <div>Transition at {formatDate(latestOwnershipTransition.ownership?.last_transition_at ?? null)}</div>
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
                  <h3>Recent dead letters</h3>
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
              </>
            ) : (
              <Panel className="nested-panel">
                <div className="muted">
                  {isCreating
                    ? "Save the adapter to unlock live lag, ownership, and dead-letter state."
                    : "Select an adapter to inspect live state."}
                </div>
              </Panel>
            )}
          </div>
        </Panel>
      </div>
    </div>
  );
}

function Field({
  label,
  help,
  children
}: {
  label: string;
  help?: string;
  children: React.ReactNode;
}) {
  return (
    <label className="stack">
      <span className="field-label">{label}</span>
      {help ? <span className="field-help">{help}</span> : null}
      {children}
    </label>
  );
}

function createEmptyDraft(): AdapterDraft {
  return {
    adapterId: "",
    adapterKind: "redpanda",
    brokers: "127.0.0.1:9092",
    topicName: "",
    topicPartitions: "1",
    action: "start_workflow",
    definitionId: "",
    signalType: "",
    workflowTaskQueue: "",
    workflowInstanceIdPointer: "",
    payloadMode: "whole",
    payloadPointer: "",
    payloadTemplate: '{\n  "payload": {\n    "$from": "/payload"\n  }\n}',
    memoMode: "none",
    memoPointer: "",
    memoTemplate: '{\n  "source": {\n    "$from": "/source",\n    "$optional": true\n  }\n}',
    searchMode: "none",
    searchPointer: "",
    searchTemplate: '{\n  "customer_id": {\n    "$from": "/customer/id",\n    "$optional": true\n  }\n}',
    requestIdMode: "derived",
    requestIdPointer: "",
    dedupeKeyMode: "derived",
    dedupeKeyPointer: "",
    deadLetterPolicy: "store",
    isPaused: false
  };
}

function draftFromAdapter(adapter: TopicAdapter): AdapterDraft {
  return {
    adapterId: adapter.adapter_id,
    adapterKind: adapter.adapter_kind,
    brokers: adapter.brokers,
    topicName: adapter.topic_name,
    topicPartitions: String(adapter.topic_partitions),
    action: adapter.action,
    definitionId: adapter.definition_id ?? "",
    signalType: adapter.signal_type ?? "",
    workflowTaskQueue: adapter.workflow_task_queue ?? "",
    workflowInstanceIdPointer: adapter.workflow_instance_id_json_pointer ?? "",
    payloadMode: adapter.payload_template_json
      ? "template"
      : adapter.payload_json_pointer
        ? "pointer"
        : "whole",
    payloadPointer: adapter.payload_json_pointer ?? "",
    payloadTemplate: formatJsonForEditor(adapter.payload_template_json),
    memoMode: adapter.memo_template_json ? "template" : adapter.memo_json_pointer ? "pointer" : "none",
    memoPointer: adapter.memo_json_pointer ?? "",
    memoTemplate: formatJsonForEditor(adapter.memo_template_json),
    searchMode: adapter.search_attributes_template_json
      ? "template"
      : adapter.search_attributes_json_pointer
        ? "pointer"
        : "none",
    searchPointer: adapter.search_attributes_json_pointer ?? "",
    searchTemplate: formatJsonForEditor(adapter.search_attributes_template_json),
    requestIdMode: adapter.request_id_json_pointer ? "pointer" : "derived",
    requestIdPointer: adapter.request_id_json_pointer ?? "",
    dedupeKeyMode: adapter.dedupe_key_json_pointer ? "pointer" : "derived",
    dedupeKeyPointer: adapter.dedupe_key_json_pointer ?? "",
    deadLetterPolicy: adapter.dead_letter_policy,
    isPaused: adapter.is_paused
  };
}

function buildTopicAdapterPayload(draft: AdapterDraft): TopicAdapterUpsertRequest {
  const topicPartitions = Number.parseInt(draft.topicPartitions.trim(), 10);
  if (!Number.isFinite(topicPartitions)) {
    throw new Error("topic partitions must be a number");
  }

  const actionIsSignal = draft.action === "signal_workflow";
  const payloadTemplate =
    draft.payloadMode === "template" ? parseJsonField(draft.payloadTemplate, "payload template") : null;
  const memoTemplate = draft.memoMode === "template" ? parseJsonField(draft.memoTemplate, "memo template") : null;
  const searchTemplate =
    draft.searchMode === "template"
      ? parseJsonField(draft.searchTemplate, "search attributes template")
      : null;

  return {
    adapter_kind: draft.adapterKind,
    brokers: normalizeRequired(draft.brokers, "brokers"),
    topic_name: normalizeRequired(draft.topicName, "topic name"),
    topic_partitions: topicPartitions,
    action: draft.action,
    definition_id: actionIsSignal ? null : normalizeOptional(draft.definitionId),
    signal_type: actionIsSignal ? normalizeOptional(draft.signalType) : null,
    workflow_task_queue: actionIsSignal ? null : normalizeOptional(draft.workflowTaskQueue),
    workflow_instance_id_json_pointer: normalizeOptional(draft.workflowInstanceIdPointer),
    payload_json_pointer:
      draft.payloadMode === "pointer" ? normalizeOptional(draft.payloadPointer) : null,
    payload_template_json: draft.payloadMode === "template" ? payloadTemplate : null,
    memo_json_pointer:
      !actionIsSignal && draft.memoMode === "pointer" ? normalizeOptional(draft.memoPointer) : null,
    memo_template_json: !actionIsSignal && draft.memoMode === "template" ? memoTemplate : null,
    search_attributes_json_pointer:
      !actionIsSignal && draft.searchMode === "pointer"
        ? normalizeOptional(draft.searchPointer)
        : null,
    search_attributes_template_json:
      !actionIsSignal && draft.searchMode === "template" ? searchTemplate : null,
    request_id_json_pointer:
      draft.requestIdMode === "pointer" ? normalizeOptional(draft.requestIdPointer) : null,
    dedupe_key_json_pointer:
      actionIsSignal && draft.dedupeKeyMode === "pointer"
        ? normalizeOptional(draft.dedupeKeyPointer)
        : null,
    dead_letter_policy: draft.deadLetterPolicy,
    is_paused: draft.isPaused
  };
}

function normalizeOptional(value: string): string | null {
  const trimmed = value.trim();
  return trimmed === "" ? null : trimmed;
}

function normalizeRequired(value: string, label: string): string {
  const trimmed = value.trim();
  if (trimmed === "") {
    throw new Error(`${label} is required`);
  }
  return trimmed;
}

function parseJsonField(source: string, label: string): unknown {
  const trimmed = source.trim();
  if (trimmed === "") {
    throw new Error(`${label} must be valid JSON`);
  }
  try {
    return JSON.parse(trimmed) as unknown;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`${label}: ${message}`);
  }
}

function formatJsonForEditor(value: unknown): string {
  return value == null ? "" : JSON.stringify(value, null, 2);
}

function extractErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

function upsertAdapterSummary(current: TopicAdapter[] | undefined, next: TopicAdapter): TopicAdapter[] {
  const items = current ?? [];
  const index = items.findIndex((adapter) => adapter.adapter_id === next.adapter_id);
  if (index === -1) {
    return [next, ...items];
  }
  return items.map((adapter) => (adapter.adapter_id === next.adapter_id ? next : adapter));
}
