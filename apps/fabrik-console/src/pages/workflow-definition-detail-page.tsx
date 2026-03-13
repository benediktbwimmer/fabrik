import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useParams } from "react-router-dom";

import { WorkflowGraphExplorer } from "../components/graph/workflow-graph-explorer";
import { Badge, Panel } from "../components/ui";
import { api } from "../lib/api";
import { formatDate, formatInlineValue, formatNumber } from "../lib/format";
import { useTenant } from "../lib/tenant-context";

const TABS = ["overview", "graph"] as const;
type Tab = (typeof TABS)[number];

export function WorkflowDefinitionDetailPage() {
  const { definitionId = "" } = useParams();
  const { tenantId } = useTenant();
  const [activeTab, setActiveTab] = useState<Tab>("graph");

  const definitionQuery = useQuery({
    queryKey: ["workflow-definition", tenantId, definitionId],
    enabled: tenantId !== "" && definitionId !== "",
    queryFn: () => api.getLatestDefinition(tenantId, definitionId)
  });
  const artifactQuery = useQuery({
    queryKey: ["workflow-artifact", tenantId, definitionId],
    enabled: tenantId !== "" && definitionId !== "",
    queryFn: () => api.getLatestArtifact(tenantId, definitionId)
  });
  const graphQuery = useQuery({
    queryKey: ["workflow-definition-graph", tenantId, definitionId],
    enabled: tenantId !== "" && definitionId !== "",
    queryFn: () => api.getDefinitionGraph(tenantId, definitionId)
  });
  const runsQuery = useQuery({
    queryKey: ["workflow-definition-runs", tenantId, definitionId],
    enabled: tenantId !== "" && definitionId !== "",
    queryFn: () => {
      const params = new URLSearchParams({ definition_id: definitionId, limit: "8" });
      return api.listRuns(tenantId, params);
    }
  });

  const artifact = artifactQuery.data;
  const definition = definitionQuery.data;

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <div className="eyebrow">Workflow definition</div>
          <h1>{definitionId}</h1>
          <p>Compiled TypeScript artifact, source anchors, and graph exploration for this workflow definition.</p>
        </div>
        <div className="row">
          {artifact ? <Badge value={`v${artifact.definition_version}`} /> : null}
          {artifact ? <Badge value={artifact.source_language} /> : null}
        </div>
      </header>

      <Panel>
        <div className="tabs">
          {TABS.map((tab) => (
            <button key={tab} className={`button ghost ${tab === activeTab ? "active" : ""}`} onClick={() => setActiveTab(tab)}>
              {tab}
            </button>
          ))}
        </div>
      </Panel>

      {activeTab === "overview" ? (
        <div className="grid two">
          <Panel>
            <h3>Artifact</h3>
            <div className="stack">
              <div>Definition version {formatInlineValue(artifact?.definition_version)}</div>
              <div>Compiler {artifact?.compiler_version ?? "-"}</div>
              <div>Entrypoint {artifact ? `${artifact.entrypoint.module}::${artifact.entrypoint.export}` : "-"}</div>
              <div>Artifact hash {artifact?.artifact_hash ?? "-"}</div>
              <div>Initial state {definition?.initial_state ?? artifact?.workflow.initial_state ?? "-"}</div>
            </div>
          </Panel>

          <Panel>
            <h3>Graph inventory</h3>
            <div className="stack">
              <div>States {formatNumber(Object.keys(artifact?.workflow.states ?? {}).length)}</div>
              <div>Signals {formatNumber(Object.keys(artifact?.signals ?? {}).length)}</div>
              <div>Updates {formatNumber(Object.keys(artifact?.updates ?? {}).length)}</div>
              <div>Queries {formatNumber(Object.keys(artifact?.queries ?? {}).length)}</div>
              <div>Source files {formatNumber(artifact?.source_files.length)}</div>
            </div>
          </Panel>

          <Panel>
            <h3>Source files</h3>
            <div className="stack">
              {(artifact?.source_files ?? []).map((file) => (
                <div key={file} className="subtle-block">
                  <strong>{file}</strong>
                </div>
              ))}
              {(artifact?.source_files.length ?? 0) === 0 ? <div className="muted">No source files were embedded in the current artifact.</div> : null}
            </div>
          </Panel>

          <Panel>
            <div className="row space-between">
              <h3>Recent runs</h3>
              <Link className="button ghost" to={`/runs?definition_id=${encodeURIComponent(definitionId)}`}>
                View all runs
              </Link>
            </div>
            <div className="stack">
              {(runsQuery.data?.items ?? []).map((run) => (
                <Link key={`${run.instance_id}:${run.run_id}`} className="subtle-block" to={`/runs/${run.instance_id}/${run.run_id}`}>
                  <div className="row space-between">
                    <strong>{run.instance_id}</strong>
                    <Badge value={run.status} />
                  </div>
                  <div className="muted">
                    {run.run_id} · {formatDate(run.last_transition_at)}
                  </div>
                </Link>
              ))}
              {(runsQuery.data?.items.length ?? 0) === 0 ? <div className="muted">No recent runs were found for this definition.</div> : null}
            </div>
          </Panel>
        </div>
      ) : null}

      {activeTab === "graph" ? <WorkflowGraphExplorer graph={graphQuery.data} /> : null}
    </div>
  );
}
