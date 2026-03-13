import { useQuery } from "@tanstack/react-query";
import { useMemo } from "react";
import { Link, useSearchParams } from "react-router-dom";

import { Panel } from "../components/ui";
import { api } from "../lib/api";
import { formatDate, formatNumber } from "../lib/format";
import { useTenant } from "../lib/tenant-context";

export function WorkflowsPage() {
  const { tenantId } = useTenant();
  const [searchParams, setSearchParams] = useSearchParams();
  const query = searchParams.get("q") ?? "";
  const needle = query;

  const definitionsQuery = useQuery({
    queryKey: ["workflow-definitions", tenantId, needle],
    enabled: tenantId !== "",
    queryFn: () => api.listDefinitionSummaries(tenantId, needle)
  });
  const artifactsQuery = useQuery({
    queryKey: ["workflow-artifacts", tenantId, needle],
    enabled: tenantId !== "",
    queryFn: () => api.listArtifactSummaries(tenantId, needle)
  });

  const rows = useMemo(() => {
    const artifactsById = new Map((artifactsQuery.data?.items ?? []).map((item) => [item.workflow_id, item]));
    return (definitionsQuery.data?.items ?? []).map((definition) => ({
      definition,
      artifact: artifactsById.get(definition.workflow_id) ?? null
    }));
  }, [artifactsQuery.data?.items, definitionsQuery.data?.items]);

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <div className="eyebrow">Catalog</div>
          <h1>Workflows</h1>
          <p>Browse registered workflow definitions and jump directly into operational runs.</p>
        </div>
      </header>

      <Panel>
        <div className="grid two">
          <input
            className="input"
            placeholder="Search workflow definition"
            value={needle}
            onChange={(event) => {
              const value = event.target.value.trimStart();
              setSearchParams(value ? { q: value } : {});
            }}
          />
          <div className="row cluster-summary">
            <div className="muted">Definitions</div>
            <strong>{formatNumber(definitionsQuery.data?.items.length)}</strong>
            <div className="muted">Artifacts</div>
            <strong>{formatNumber(artifactsQuery.data?.items.length)}</strong>
          </div>
        </div>
      </Panel>

      <Panel>
        <table className="table">
          <thead>
            <tr>
              <th>Workflow</th>
              <th>Definition</th>
              <th>Artifact</th>
              <th>Versions</th>
              <th>Updated</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {rows.map(({ definition, artifact }) => (
              <tr key={definition.workflow_id}>
                <td>
                  <strong>{definition.workflow_id}</strong>
                </td>
                <td>
                  v{definition.latest_version}
                  <div className="muted">active {definition.active_version ?? "-"}</div>
                </td>
                <td>
                  {artifact ? `v${artifact.latest_version}` : "-"}
                  <div className="muted">active {artifact?.active_version ?? "-"}</div>
                </td>
                <td>
                  {formatNumber(definition.version_count)} definitions
                  <div className="muted">{formatNumber(artifact?.version_count)} artifacts</div>
                </td>
                <td>{formatDate(artifact?.updated_at ?? definition.updated_at)}</td>
                <td>
                  <Link className="button ghost" to={`/runs?definition_id=${encodeURIComponent(definition.workflow_id)}`}>
                    View runs
                  </Link>
                  <Link className="button ghost" to={`/workflows/${encodeURIComponent(definition.workflow_id)}`}>
                    Open graph
                  </Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {rows.length === 0 ? <div className="empty">No workflow definitions matched the current filter.</div> : null}
      </Panel>
    </div>
  );
}
