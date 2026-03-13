import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { toast } from "sonner";

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
  const [validation, setValidation] = useState<Awaited<
    ReturnType<typeof api.validateWorkflowArtifact>
  > | null>(null);
  const [validating, setValidating] = useState(false);

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

  async function onValidateRollout() {
    if (!artifact) return;
    setValidating(true);
    try {
      const response = await api.validateWorkflowArtifact(tenantId, artifact);
      setValidation(response);
      if (response.compatible) {
        toast.success("Artifact validated against recent runs");
      } else {
        toast.error("Artifact is incompatible with recent runs");
      }
    } catch (error) {
      toast.error(String(error));
    } finally {
      setValidating(false);
    }
  }

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
            <div className="row space-between">
              <h3>Rollout validation</h3>
              <button className="button primary" type="button" disabled={!artifact || validating} onClick={onValidateRollout}>
                {validating ? "Validating..." : "Validate rollout"}
              </button>
            </div>
            <div className="stack">
              <div className="muted">Replay the latest artifact against recent captured histories before promoting it.</div>
              {validation ? (
                <>
                  <div className="row">
                    <Badge value={validation.status} />
                    <div className="muted">Validated runs {formatNumber(validation.validation.validated_run_count)}</div>
                    <div className="muted">Skipped {formatNumber(validation.validation.skipped_run_count)}</div>
                    <div className="muted">Failures {formatNumber(validation.validation.failed_run_count)}</div>
                  </div>
                  {validation.validation.failures.length > 0 ? (
                    <div className="stack">
                      {validation.validation.failures.map((failure) => (
                        <div key={`${failure.instance_id}:${failure.run_id}`} className="subtle-block">
                          <strong>
                            {failure.instance_id} / {failure.run_id}
                          </strong>
                          <div className="muted">{failure.message}</div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="muted">No replay divergences were found in the sampled runs.</div>
                  )}
                </>
              ) : (
                <div className="muted">No dry-run validation has been executed for the current artifact.</div>
              )}
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
