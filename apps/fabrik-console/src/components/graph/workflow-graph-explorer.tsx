import ELK from "elkjs/lib/elk.bundled.js";
import { useEffect, useMemo, useState } from "react";
import ReactFlow, { Background, Controls, MiniMap, type Edge, type Node } from "reactflow";
import "reactflow/dist/style.css";

import {
  type WorkflowGraphEdge,
  type WorkflowGraphModule,
  type WorkflowGraphNode,
  type WorkflowGraphResponse
} from "../../lib/api";
import { formatDate } from "../../lib/format";
import { Badge, ConsistencyBadge, Panel } from "../ui";

type Props = {
  graph?: WorkflowGraphResponse;
  onOpenActivities?: () => void;
  onOpenHistory?: () => void;
};

type ViewMode = "semantic" | "canvas" | "timeline";

const elk = new ELK();

export function WorkflowGraphExplorer({ graph, onOpenActivities, onOpenHistory }: Props) {
  const [viewMode, setViewMode] = useState<ViewMode>("semantic");
  const [selectedModuleId, setSelectedModuleId] = useState<string | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);

  useEffect(() => {
    if (!graph) {
      setSelectedModuleId(null);
      setSelectedNodeId(null);
      return;
    }
    const defaultModuleId = graph.overlay.current_module_id ?? graph.modules[0]?.id ?? null;
    const defaultNodeId =
      graph.overlay.current_node_id ??
      graph.modules.find((module) => module.id === defaultModuleId)?.focus_node_id ??
      graph.nodes[0]?.id ??
      null;
    setSelectedModuleId(defaultModuleId);
    setSelectedNodeId(defaultNodeId);
  }, [graph]);

  const moduleStatusMap = useMemo(
    () => new Map((graph?.overlay.module_statuses ?? []).map((status) => [status.id, status])),
    [graph?.overlay.module_statuses]
  );
  const nodeStatusMap = useMemo(
    () => new Map((graph?.overlay.node_statuses ?? []).map((status) => [status.id, status])),
    [graph?.overlay.node_statuses]
  );

  const selectedModule = useMemo(
    () => graph?.modules.find((module) => module.id === selectedModuleId) ?? null,
    [graph?.modules, selectedModuleId]
  );
  const selectedNode = useMemo(
    () => graph?.nodes.find((node) => node.id === selectedNodeId) ?? null,
    [graph?.nodes, selectedNodeId]
  );

  const semanticNodes = useMemo(
    () =>
      (graph?.modules ?? []).map((module) => ({
        id: module.id,
        kind: module.kind,
        label: module.label,
        subtitle: module.subtitle,
        status: moduleStatusMap.get(module.id)?.status ?? "idle",
        summary: moduleStatusMap.get(module.id)?.summary ?? null
      })),
    [graph?.modules, moduleStatusMap]
  );

  const canvasNodes = useMemo(
    () =>
      (graph?.nodes ?? []).map((node) => ({
        id: node.id,
        kind: node.kind,
        label: node.label,
        subtitle: node.subtitle,
        status: nodeStatusMap.get(node.id)?.status ?? "idle",
        summary: nodeStatusMap.get(node.id)?.summary ?? null
      })),
    [graph?.nodes, nodeStatusMap]
  );

  const relevantTrace = useMemo(() => {
    if (!graph) return [];
    return graph.overlay.trace.filter((step) => {
      if (selectedNodeId && step.node_id === selectedNodeId) return true;
      if (selectedModuleId && step.module_id === selectedModuleId) return true;
      return false;
    });
  }, [graph, selectedModuleId, selectedNodeId]);

  const whyHereSteps = useMemo(() => {
    if (!graph || graph.overlay.trace.length === 0) return [];
    let lastMatchIndex = -1;
    for (let index = graph.overlay.trace.length - 1; index >= 0; index -= 1) {
      const step = graph.overlay.trace[index];
      if ((selectedNodeId && step.node_id === selectedNodeId) || (selectedModuleId && step.module_id === selectedModuleId)) {
        lastMatchIndex = index;
        break;
      }
    }
    if (lastMatchIndex < 0) return [];
    return graph.overlay.trace.slice(Math.max(lastMatchIndex - 3, 0), lastMatchIndex + 1);
  }, [graph, selectedModuleId, selectedNodeId]);

  const nextTransitions = useMemo(() => {
    if (!graph) return [];
    if (viewMode === "semantic") {
      return graph.module_edges.filter((edge) => edge.source === selectedModuleId);
    }
    return graph.edges.filter((edge) => edge.source === selectedNodeId);
  }, [graph, selectedModuleId, selectedNodeId, viewMode]);

  const activityEvidence = useMemo(
    () =>
      (graph?.overlay.activity_summaries ?? []).filter((summary) => {
        if (selectedNodeId && summary.node_id === selectedNodeId) return true;
        if (selectedModuleId && summary.module_id === selectedModuleId) return true;
        return false;
      }),
    [graph?.overlay.activity_summaries, selectedModuleId, selectedNodeId]
  );
  const bulkEvidence = useMemo(
    () =>
      (graph?.overlay.bulk_summaries ?? []).filter((summary) => {
        if (selectedNodeId && summary.node_id === selectedNodeId) return true;
        if (selectedModuleId && summary.module_id === selectedModuleId) return true;
        return false;
      }),
    [graph?.overlay.bulk_summaries, selectedModuleId, selectedNodeId]
  );
  const signalEvidence = useMemo(
    () =>
      (graph?.overlay.signal_summaries ?? []).filter((summary) => {
        if (selectedNodeId && summary.node_id === selectedNodeId) return true;
        if (selectedModuleId && summary.module_id === selectedModuleId) return true;
        return selectedModule?.kind === "wait" && selectedModule.label === summary.signal_name;
      }),
    [graph?.overlay.signal_summaries, selectedModule, selectedModuleId, selectedNodeId]
  );
  const updateEvidence = useMemo(
    () =>
      (graph?.overlay.update_summaries ?? []).filter((summary) => {
        if (selectedModuleId && summary.module_id === selectedModuleId) return true;
        return false;
      }),
    [graph?.overlay.update_summaries, selectedModuleId]
  );
  const childEvidence = useMemo(
    () =>
      (graph?.overlay.child_summaries ?? []).filter((summary) => {
        if (selectedNodeId && summary.node_id === selectedNodeId) return true;
        if (selectedModuleId && summary.module_id === selectedModuleId) return true;
        return false;
      }),
    [graph?.overlay.child_summaries, selectedModuleId, selectedNodeId]
  );

  if (!graph) {
    return <div className="empty">No graph data available.</div>;
  }

  const selectedModuleStatus = selectedModule ? moduleStatusMap.get(selectedModule.id) : null;
  const selectedNodeStatus = selectedNode ? nodeStatusMap.get(selectedNode.id) : null;
  const sourceAnchor = selectedNode?.source_anchor ?? selectedModule?.source_anchor ?? null;

  return (
    <div className="graph-workbench">
      <Panel>
        <div className="row space-between">
          <div>
            <h3>Graph Explorer</h3>
            <div className="muted">
              {graph.definition_id} v{graph.definition_version} · {graph.overlay.mode === "run" ? "run overlay" : "artifact"}
            </div>
          </div>
          <div className="row">
            <ConsistencyBadge consistency={graph.consistency} source={graph.authoritative_source} />
            {graph.overlay.run_id ? <Badge value="run overlay" /> : <Badge value="artifact" />}
          </div>
        </div>

        <div className="tabs" style={{ marginTop: 12 }}>
          {(["semantic", "canvas", "timeline"] as const).map((mode) => (
            <button key={mode} className={`button ghost ${viewMode === mode ? "active" : ""}`} onClick={() => setViewMode(mode)}>
              {mode === "semantic" ? "Semantic Map" : mode === "canvas" ? "Canvas" : "Timeline"}
            </button>
          ))}
        </div>
      </Panel>

      <div className="graph-workbench-body">
        <Panel className="graph-main-panel">
          {viewMode === "semantic" ? (
            <SelectableGraphCanvas
              graphId="semantic"
              items={semanticNodes}
              edges={graph.module_edges}
              selectedId={selectedModuleId}
              onSelect={(id) => {
                setSelectedModuleId(id);
                const module = graph.modules.find((candidate) => candidate.id === id);
                if (module) {
                  setSelectedNodeId(module.focus_node_id);
                }
              }}
            />
          ) : null}

          {viewMode === "canvas" ? (
            <SelectableGraphCanvas
              graphId="canvas"
              items={canvasNodes}
              edges={graph.edges}
              selectedId={selectedNodeId}
              onSelect={(id) => {
                setSelectedNodeId(id);
                const node = graph.nodes.find((candidate) => candidate.id === id);
                if (node) {
                  setSelectedModuleId(node.module_id);
                }
              }}
            />
          ) : null}

          {viewMode === "timeline" ? (
            <div className="timeline">
              {graph.overlay.trace.map((step) => (
                <button
                  key={step.id}
                  className={`timeline-item graph-timeline-item ${
                    (selectedNodeId && step.node_id === selectedNodeId) || (selectedModuleId && step.module_id === selectedModuleId)
                      ? "active"
                      : ""
                  }`}
                  onClick={() => {
                    if (step.module_id) setSelectedModuleId(step.module_id);
                    if (step.node_id) setSelectedNodeId(step.node_id);
                  }}
                >
                  <div className="timeline-time">{formatDate(step.occurred_at)}</div>
                  <div className={`timeline-lane ${step.lane}`}>{step.lane}</div>
                  <div>
                    <strong>{step.label}</strong>
                    {step.detail ? <div className="muted">{step.detail}</div> : null}
                  </div>
                </button>
              ))}
              {graph.overlay.trace.length === 0 ? <div className="empty">No execution trace is available for this graph view.</div> : null}
            </div>
          ) : null}
        </Panel>

        <div className="stack">
          <Panel>
            <div className="row space-between">
              <h3>Summary</h3>
              <div className="row">
                {selectedNodeStatus ? <Badge value={selectedNodeStatus.status} /> : null}
                {selectedModuleStatus ? <Badge value={selectedModuleStatus.status} /> : null}
              </div>
            </div>
            <div className="stack">
              <div>
                <strong>{selectedNode?.label ?? selectedModule?.label ?? "Nothing selected"}</strong>
                <div className="muted">
                  {selectedNode?.kind ?? selectedModule?.kind ?? "Select a graph element to inspect it."}
                </div>
              </div>
              {selectedNode?.subtitle || selectedModule?.subtitle ? (
                <div>{selectedNode?.subtitle ?? selectedModule?.subtitle}</div>
              ) : null}
              {graph.overlay.blocked_by &&
              ((selectedNode && graph.overlay.blocked_by.node_id === selectedNode.id) ||
                (selectedModule && graph.overlay.blocked_by.module_id === selectedModule.id)) ? (
                <div className="subtle-block">
                  <strong>Blocked on {graph.overlay.blocked_by.kind}</strong>
                  <div>{graph.overlay.blocked_by.label}</div>
                  {graph.overlay.blocked_by.detail ? <div className="muted">{graph.overlay.blocked_by.detail}</div> : null}
                </div>
              ) : null}
            </div>
          </Panel>

          <Panel>
            <div className="row space-between">
              <h3>Why am I here?</h3>
              {selectedModule && viewMode !== "canvas" ? (
                <button className="button ghost" onClick={() => setViewMode("canvas")}>
                  Open raw canvas
                </button>
              ) : null}
            </div>
            <div className="stack">
              {whyHereSteps.map((step) => (
                <div key={step.id} className="subtle-block">
                  <strong>{step.label}</strong>
                  <div className="muted">{formatDate(step.occurred_at)}</div>
                  {step.detail ? <div>{step.detail}</div> : null}
                </div>
              ))}
              {whyHereSteps.length === 0 ? <div className="muted">No causal trace is available for the current selection.</div> : null}
            </div>
          </Panel>

          <Panel>
            <h3>What can happen next?</h3>
            <div className="stack">
              {nextTransitions.map((edge) => (
                <button
                  key={edge.id}
                  className="subtle-block left-align"
                  onClick={() => {
                    if (viewMode === "semantic") {
                      setSelectedModuleId(edge.target);
                      const module = graph.modules.find((candidate) => candidate.id === edge.target);
                      if (module) setSelectedNodeId(module.focus_node_id);
                    } else {
                      setSelectedNodeId(edge.target);
                      const node = graph.nodes.find((candidate) => candidate.id === edge.target);
                      if (node) setSelectedModuleId(node.module_id);
                    }
                  }}
                >
                  <strong>{edge.label}</strong>
                  <div className="muted">{edge.target}</div>
                </button>
              ))}
              {nextTransitions.length === 0 ? <div className="muted">This element does not currently expose any outgoing transitions.</div> : null}
            </div>
          </Panel>

          <Panel>
            <h3>Runtime Evidence</h3>
            <div className="stack">
              {activityEvidence.map((summary) => (
                <div key={summary.node_id} className="subtle-block">
                  <strong>{summary.activity_type}</strong>
                  <div className="muted">
                    {summary.total} attempts · {summary.pending} pending · {summary.failed} failed · {summary.retrying} retrying
                  </div>
                  {summary.worker_build_ids.length > 0 ? <div>Builds {summary.worker_build_ids.join(", ")}</div> : null}
                </div>
              ))}
              {bulkEvidence.map((summary) => (
                <div key={summary.batch_id} className="subtle-block">
                  <strong>{summary.activity_type}</strong>
                  <div className="muted">
                    {summary.total_items} items · {summary.failed_items} failed · {summary.cancelled_items} cancelled
                  </div>
                </div>
              ))}
              {signalEvidence.map((summary) => (
                <div key={summary.signal_name} className="subtle-block">
                  <strong>{summary.signal_name}</strong>
                  <div className="muted">
                    {summary.count} seen · latest {summary.latest_status} at {formatDate(summary.latest_seen_at)}
                  </div>
                </div>
              ))}
              {updateEvidence.map((summary) => (
                <div key={summary.update_name} className="subtle-block">
                  <strong>{summary.update_name}</strong>
                  <div className="muted">
                    {summary.count} updates · latest {summary.latest_status} at {formatDate(summary.latest_seen_at)}
                  </div>
                </div>
              ))}
              {childEvidence.map((summary) => (
                <div key={summary.child_id} className="subtle-block">
                  <strong>{summary.child_definition_id}</strong>
                  <div className="muted">
                    {summary.child_workflow_id} · {summary.status}
                  </div>
                  {summary.child_run_id ? <div>Run {summary.child_run_id}</div> : null}
                </div>
              ))}
              {activityEvidence.length === 0 &&
              bulkEvidence.length === 0 &&
              signalEvidence.length === 0 &&
              updateEvidence.length === 0 &&
              childEvidence.length === 0 ? <div className="muted">No runtime evidence is attached to the current selection.</div> : null}
            </div>
          </Panel>

          <Panel>
            <h3>Source</h3>
            <div className="stack">
              <div>Entrypoint {graph.entrypoint_module}::{graph.entrypoint_export}</div>
              {sourceAnchor ? (
                <div className="subtle-block">
                  <strong>{sourceAnchor.file}</strong>
                  <div className="muted">
                    line {sourceAnchor.line}, column {sourceAnchor.column}
                  </div>
                </div>
              ) : (
                <div className="muted">No source map anchor is available for the current selection.</div>
              )}
            </div>
          </Panel>

          <Panel>
            <div className="row space-between">
              <h3>IR</h3>
              <div className="row">
                {onOpenActivities && graph.overlay.mode === "run" ? (
                  <button className="button ghost" onClick={onOpenActivities}>
                    Open activities
                  </button>
                ) : null}
                {onOpenHistory && graph.overlay.mode === "run" ? (
                  <button className="button ghost" onClick={onOpenHistory}>
                    Open raw history
                  </button>
                ) : null}
              </div>
            </div>
            <details open={Boolean(selectedNode)}>
              <summary>Selected node JSON</summary>
              <pre className="code">{JSON.stringify(selectedNode?.raw ?? {}, null, 2)}</pre>
            </details>
            <details>
              <summary>Selected module JSON</summary>
              <pre className="code">{JSON.stringify(selectedModule?.raw ?? {}, null, 2)}</pre>
            </details>
            <details>
              <summary>Matching trace entries</summary>
              <pre className="code">{JSON.stringify(relevantTrace, null, 2)}</pre>
            </details>
          </Panel>
        </div>
      </div>
    </div>
  );
}

function SelectableGraphCanvas({
  graphId,
  items,
  edges,
  selectedId,
  onSelect
}: {
  graphId: string;
  items: Array<{ id: string; kind: string; label: string; subtitle: string | null; status: string; summary: string | null }>;
  edges: WorkflowGraphEdge[];
  selectedId: string | null;
  onSelect: (id: string) => void;
}) {
  const [layouted, setLayouted] = useState<{ nodes: Node[]; edges: Edge[] }>({ nodes: [], edges: [] });

  useEffect(() => {
    if (items.length === 0) {
      setLayouted({ nodes: [], edges: [] });
      return;
    }
    const run = async () => {
      const result = await elk.layout({
        id: graphId,
        layoutOptions: {
          "elk.algorithm": "layered",
          "elk.direction": "RIGHT",
          "elk.spacing.nodeNode": "30",
          "elk.layered.spacing.nodeNodeBetweenLayers": "110"
        },
        children: items.map((item) => ({
          id: item.id,
          width: 240,
          height: 110
        })),
        edges: edges.map((edge) => ({
          id: edge.id,
          sources: [edge.source],
          targets: [edge.target]
        }))
      });
      setLayouted({
        nodes: items.map((item) => {
          const elkNode = result.children?.find((entry) => entry.id === item.id);
          const isSelected = item.id === selectedId;
          return {
            id: item.id,
            position: { x: elkNode?.x ?? 0, y: elkNode?.y ?? 0 },
            data: {
              label: (
                <div className="graph-card">
                  <div className="row space-between">
                    <strong>{item.label}</strong>
                    <Badge value={item.status} />
                  </div>
                  <div className="muted">{item.kind.replaceAll("_", " ")}</div>
                  {item.subtitle ? <div>{item.subtitle}</div> : null}
                  {item.summary ? <div className="muted">{item.summary}</div> : null}
                </div>
              )
            },
            style: {
              width: 240,
              borderRadius: 16,
              border: isSelected ? "1px solid rgba(59, 182, 199, 0.8)" : "1px solid rgba(127,170,193,.22)",
              background: isSelected ? "rgba(11, 31, 39, 0.98)" : "rgba(9, 21, 28, 0.96)",
              color: "#e6f0f2",
              padding: 12,
              boxShadow: isSelected ? "0 0 0 2px rgba(59, 182, 199, 0.18)" : "none"
            }
          };
        }),
        edges: edges.map((edge) => ({
          id: edge.id,
          source: edge.source,
          target: edge.target,
          type: "smoothstep",
          label: edge.label,
          style: {
            stroke: "rgba(127,170,193,.44)"
          },
          labelStyle: {
            fill: "#99afb9",
            fontSize: 12
          }
        }))
      });
    };
    void run();
  }, [edges, graphId, items, selectedId]);

  if (layouted.nodes.length === 0) {
    return <div className="empty">No graph data is available for this view.</div>;
  }

  return (
    <div className="graph graph-surface" data-testid={`graph-${graphId}`}>
      <ReactFlow
        fitView
        nodes={layouted.nodes}
        edges={layouted.edges}
        onNodeClick={(_, node) => onSelect(node.id)}
      >
        <MiniMap />
        <Controls />
        <Background />
      </ReactFlow>
    </div>
  );
}
