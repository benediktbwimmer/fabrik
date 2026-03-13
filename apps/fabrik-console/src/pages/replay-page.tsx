import { FormEvent, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";

import { api } from "../lib/api";
import { formatInlineValue } from "../lib/format";
import { useTenant } from "../lib/tenant-context";
import { Badge, Panel } from "../components/ui";

export function ReplayPage() {
  const { tenantId } = useTenant();
  const [searchParams, setSearchParams] = useSearchParams();
  const [instanceId, setInstanceId] = useState(searchParams.get("instance") ?? "");
  const replayQuery = useQuery({
    queryKey: ["replay-page", tenantId, searchParams.get("instance")],
    enabled: tenantId !== "" && Boolean(searchParams.get("instance")),
    queryFn: () => api.getWorkflowReplay(tenantId, searchParams.get("instance")!)
  });
  const workflowsQuery = useQuery({
    queryKey: ["replay-page-workflows", tenantId],
    enabled: tenantId !== "",
    queryFn: () => api.listWorkflows(tenantId)
  });

  function onSubmit(event: FormEvent) {
    event.preventDefault();
    if (instanceId.trim() !== "") {
      setSearchParams({ instance: instanceId.trim() });
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
                setSearchParams({ instance: workflow.instance_id });
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
            <strong>Divergence count {replayQuery.data.divergence_count}</strong>
            <pre className="code">{JSON.stringify(replayQuery.data, null, 2)}</pre>
          </div>
        ) : (
          <div className="empty">Choose a workflow instance to inspect replay output.</div>
        )}
      </Panel>
    </div>
  );
}
