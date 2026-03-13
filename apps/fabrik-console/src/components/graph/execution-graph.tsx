import ELK from "elkjs/lib/elk.bundled.js";
import { useEffect, useMemo, useState } from "react";
import ReactFlow, { Background, Controls, MiniMap, type Edge, type Node } from "reactflow";
import "reactflow/dist/style.css";

import { ExecutionGraphResponse } from "../../lib/api";

type Props = {
  graph?: ExecutionGraphResponse;
};

const elk = new ELK();

export function ExecutionGraph({ graph }: Props) {
  const [layouted, setLayouted] = useState<{ nodes: Node[]; edges: Edge[] }>({ nodes: [], edges: [] });

  useEffect(() => {
    if (!graph || graph.nodes.length === 0) {
      setLayouted({ nodes: [], edges: [] });
      return;
    }
    const run = async () => {
      const result = await elk.layout({
        id: "root",
        layoutOptions: {
          "elk.algorithm": "layered",
          "elk.direction": "RIGHT",
          "elk.spacing.nodeNode": "36",
          "elk.layered.spacing.nodeNodeBetweenLayers": "120"
        },
        children: graph.nodes.map((node) => ({
          id: node.id,
          width: 200,
          height: 72
        })),
        edges: graph.edges.map((edge) => ({
          id: edge.id,
          sources: [edge.source],
          targets: [edge.target]
        }))
      });
      setLayouted({
        nodes: graph.nodes.map((node) => {
          const elkNode = result.children?.find((entry) => entry.id === node.id);
          return {
            id: node.id,
            position: { x: elkNode?.x ?? 0, y: elkNode?.y ?? 0 },
            data: {
              label: (
                <div className="stack" style={{ gap: 4 }}>
                  <strong>{node.label}</strong>
                  <span className="muted">{node.kind.replaceAll("_", " ")}</span>
                  {node.subtitle ? <span className="muted">{node.subtitle}</span> : null}
                </div>
              )
            },
            style: {
              width: 200,
              borderRadius: 12,
              border: "1px solid rgba(127,170,193,.22)",
              background: "rgba(9,21,28,.96)",
              color: "#e6f0f2",
              padding: 12
            }
          };
        }),
        edges: graph.edges.map((edge) => ({
          id: edge.id,
          source: edge.source,
          target: edge.target,
          type: "smoothstep",
          label: edge.label
        }))
      });
    };
    void run();
  }, [graph]);

  const hasGraph = useMemo(() => layouted.nodes.length > 0, [layouted.nodes.length]);
  if (!hasGraph) {
    return <div className="empty">No execution graph available.</div>;
  }

  return (
    <div className="graph" data-testid="execution-graph">
      <ReactFlow fitView nodes={layouted.nodes} edges={layouted.edges}>
        <MiniMap />
        <Controls />
        <Background />
      </ReactFlow>
    </div>
  );
}
