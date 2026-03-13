import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";

import { api } from "../lib/api";
import { formatDate } from "../lib/format";
import { useTenant } from "../lib/tenant-context";
import { Panel } from "../components/ui";

export function DefinitionsPage() {
  const { tenantId } = useTenant();
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedDefinition = searchParams.get("definition") ?? searchParams.get("artifact") ?? "";
  const definitionsQuery = useQuery({
    queryKey: ["definitions", tenantId],
    enabled: tenantId !== "",
    queryFn: () => api.listDefinitionSummaries(tenantId)
  });
  const artifactsQuery = useQuery({
    queryKey: ["artifacts", tenantId],
    enabled: tenantId !== "",
    queryFn: () => api.listArtifactSummaries(tenantId)
  });
  const definitionDetailQuery = useQuery({
    queryKey: ["definition-detail", tenantId, selectedDefinition],
    enabled: tenantId !== "" && selectedDefinition !== "",
    queryFn: async () => {
      const [definition, artifact] = await Promise.allSettled([
        api.getLatestDefinition(tenantId, selectedDefinition),
        api.getLatestArtifact(tenantId, selectedDefinition)
      ]);
      return {
        definition: definition.status === "fulfilled" ? definition.value : null,
        artifact: artifact.status === "fulfilled" ? artifact.value : null
      };
    }
  });
  const definitionRows =
    (definitionsQuery.data?.items ?? []).length > 0 ? definitionsQuery.data?.items ?? [] : artifactsQuery.data?.items ?? [];

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <div className="eyebrow">Registry</div>
          <h1>Definitions & Artifacts</h1>
          <p>Browse pinned workflow definitions and compiled artifact versions for the current tenant.</p>
        </div>
      </header>

      <div className="split">
        <Panel>
          <h2>Definitions</h2>
          <table className="table">
            <thead>
              <tr>
                <th>Workflow</th>
                <th>Latest</th>
                <th>Active</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {definitionRows.map((item) => (
                <tr key={item.workflow_id} onClick={() => setSearchParams({ definition: item.workflow_id })} style={{ cursor: "pointer" }}>
                  <td>{item.workflow_id}</td>
                  <td>{item.latest_version}</td>
                  <td>{item.active_version ?? "-"}</td>
                  <td>{formatDate(item.updated_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <h2>Artifacts</h2>
          <table className="table">
            <thead>
              <tr>
                <th>Workflow</th>
                <th>Latest</th>
                <th>Active</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {(artifactsQuery.data?.items ?? []).map((item) => (
                <tr key={item.workflow_id} onClick={() => setSearchParams({ artifact: item.workflow_id })} style={{ cursor: "pointer" }}>
                  <td>{item.workflow_id}</td>
                  <td>{item.latest_version}</td>
                  <td>{item.active_version ?? "-"}</td>
                  <td>{formatDate(item.updated_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Panel>

        <Panel>
          <h2>{selectedDefinition || "Select a definition"}</h2>
          {definitionDetailQuery.data ? (
            <div className="stack">
              {definitionDetailQuery.data.definition ? <pre className="code">{JSON.stringify(definitionDetailQuery.data.definition, null, 2)}</pre> : null}
              {definitionDetailQuery.data.artifact ? <pre className="code">{JSON.stringify(definitionDetailQuery.data.artifact, null, 2)}</pre> : null}
              {!definitionDetailQuery.data.definition && !definitionDetailQuery.data.artifact ? (
                <div className="empty">No definition or artifact metadata available for this workflow.</div>
              ) : null}
            </div>
          ) : (
            <div className="empty">Pick a definition or artifact to inspect latest metadata.</div>
          )}
        </Panel>
      </div>
    </div>
  );
}
