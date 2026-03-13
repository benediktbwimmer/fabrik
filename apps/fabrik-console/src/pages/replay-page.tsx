import { FormEvent, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";

import { api } from "../lib/api";
import { formatInlineValue } from "../lib/format";
import { useTenant } from "../lib/tenant-context";
import { Badge, Panel } from "../components/ui";

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

export function ReplayPage() {
  const { tenantId } = useTenant();
  const [searchParams, setSearchParams] = useSearchParams();
  const [instanceId, setInstanceId] = useState(searchParams.get("instance") ?? "");
  const [runId, setRunId] = useState(searchParams.get("run") ?? "");
  const replayQuery = useQuery({
    queryKey: ["replay-page", tenantId, searchParams.get("instance"), searchParams.get("run")],
    enabled: tenantId !== "" && Boolean(searchParams.get("instance")),
    queryFn: () => api.getWorkflowReplay(tenantId, searchParams.get("instance")!, searchParams.get("run") ?? undefined)
  });
  const workflowQuery = useQuery({
    queryKey: ["replay-page-workflow", tenantId, searchParams.get("instance")],
    enabled: tenantId !== "" && Boolean(searchParams.get("instance")),
    queryFn: () => api.getWorkflow(tenantId, searchParams.get("instance")!)
  });
  const routingQuery = useQuery({
    queryKey: ["replay-page-routing", tenantId, searchParams.get("instance")],
    enabled: tenantId !== "" && Boolean(searchParams.get("instance")),
    queryFn: () => api.getWorkflowRouting(tenantId, searchParams.get("instance")!)
  });
  const workflowsQuery = useQuery({
    queryKey: ["replay-page-workflows", tenantId],
    enabled: tenantId !== "",
    queryFn: () => api.listWorkflows(tenantId)
  });
  const expectedQueue = workflowQuery.data?.workflow_task_queue ?? null;
  const expectedDefinitionVersion = workflowQuery.data?.definition_version ?? null;
  const expectedArtifactHash = workflowQuery.data?.artifact_hash ?? null;
  const replayQueue = replayQuery.data?.replayed_state?.workflow_task_queue ?? null;
  const replayQueuePreserved =
    expectedQueue != null && replayQueue != null ? expectedQueue === replayQueue : null;
  const replayVersionMatchesPinned =
    expectedDefinitionVersion != null ? replayQuery.data?.definition_version === expectedDefinitionVersion : null;
  const replayArtifactMatchesPinned =
    expectedArtifactHash != null && replayQuery.data?.artifact_hash != null
      ? replayQuery.data.artifact_hash === expectedArtifactHash
      : null;

  function onSubmit(event: FormEvent) {
    event.preventDefault();
    if (instanceId.trim() !== "") {
      const next = new URLSearchParams();
      next.set("instance", instanceId.trim());
      if (runId.trim() !== "") next.set("run", runId.trim());
      setSearchParams(next);
    }
  }

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <div className="eyebrow">Replay workbench</div>
          <h1>Replay</h1>
          <p>Inspect divergence counts and replay source for any workflow instance in the current tenant.</p>
        </div>
      </header>

      <Panel>
        <form className="row" onSubmit={onSubmit}>
          <input className="input" placeholder="workflow instance id" value={instanceId} onChange={(event) => setInstanceId(event.target.value)} />
          <input className="input" placeholder="run id (optional)" value={runId} onChange={(event) => setRunId(event.target.value)} />
          <button className="button primary" type="submit">
            Load replay
          </button>
        </form>
      </Panel>

      <Panel>
        <h2>Recent workflows</h2>
        <div className="stack">
          {(workflowsQuery.data?.items ?? []).slice(0, 5).map((workflow) => (
            <button
              key={workflow.instance_id}
              className="button ghost"
              onClick={() => {
                setInstanceId(workflow.instance_id);
                setRunId(workflow.run_id);
                setSearchParams({ instance: workflow.instance_id, run: workflow.run_id });
              }}
            >
              {workflow.definition_id}
              <span className="muted">{workflow.instance_id}</span>
            </button>
          ))}
          {(workflowsQuery.data?.items ?? []).length === 0 ? <div className="empty">No workflows found for this tenant.</div> : null}
        </div>
      </Panel>

      <Panel>
        {replayQuery.data ? (
          <div className="stack">
            <div className="row">
              <Badge value={replayQuery.data.divergence_count > 0 ? "failed" : "completed"} />
              <span className="muted">Source {formatInlineValue(replayQuery.data.replay_source)}</span>
            </div>
            <div className="muted">
              Projection matches store {formatInlineValue(replayQuery.data.projection_matches_store)} · snapshot{" "}
              {formatInlineValue(replayQuery.data.snapshot?.last_event_type)}
            </div>
            <strong>Divergence count {replayQuery.data.divergence_count}</strong>
            <div className="muted">Pinned definition version {formatInlineValue(expectedDefinitionVersion)}</div>
            <div className="muted">Replay definition version {formatInlineValue(replayQuery.data.definition_version)}</div>
            <div className="muted">Pinned artifact hash {formatInlineValue(expectedArtifactHash)}</div>
            <div className="muted">Replay artifact hash {formatInlineValue(replayQuery.data.artifact_hash)}</div>
            <div className="muted">Replay kept pinned version {formatInlineValue(replayVersionMatchesPinned)}</div>
            <div className="muted">Replay kept pinned artifact {formatInlineValue(replayArtifactMatchesPinned)}</div>
            <div className="muted">Expected queue {formatInlineValue(expectedQueue)}</div>
            <div className="muted">
              Replayed queue {formatInlineValue(replayQuery.data.replayed_state?.workflow_task_queue)}
            </div>
            <div className="muted">Queue preserved across replay/handoff {formatInlineValue(replayQueuePreserved)}</div>
            <div className="muted">Routing status {formatInlineValue(routingQuery.data?.routing_status)}</div>
            <div className="muted">Default set {formatInlineValue(routingQuery.data?.default_compatibility_set_id)}</div>
            <div className="muted">Sticky build {formatInlineValue(routingQuery.data?.sticky_workflow_build_id)}</div>
            <div className="muted">Replayed memo {metadataPreview(replayQuery.data.replayed_state?.memo)}</div>
            <div className="muted">
              Replayed search {metadataPreview(replayQuery.data.replayed_state?.search_attributes)}
            </div>
            <pre className="code">{JSON.stringify(replayQuery.data, null, 2)}</pre>
          </div>
        ) : (
          <div className="empty">Choose a workflow instance to inspect replay output.</div>
        )}
      </Panel>
    </div>
  );
}
