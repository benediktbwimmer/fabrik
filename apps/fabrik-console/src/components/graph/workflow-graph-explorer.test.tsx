import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { WorkflowGraphExplorer } from "./workflow-graph-explorer";

const graph = {
  tenant_id: "tenant-a",
  definition_id: "payments",
  definition_version: 2,
  artifact_hash: "artifact-b",
  entrypoint_module: "workflows/payments.ts",
  entrypoint_export: "workflow",
  consistency: "eventual",
  authoritative_source: "artifact+projection",
  source_files: ["workflows/payments.ts"],
  nodes: [
    {
      id: "workflow:charge-card",
      graph: "workflow",
      module_id: "module:workflow:charge-card",
      state_id: "charge-card",
      kind: "step",
      label: "charge-card",
      subtitle: "payments.charge",
      source_anchor: { file: "workflows/payments.ts", line: 12, column: 2 },
      next_ids: ["workflow:wait-payment"],
      raw: { type: "step" }
    },
    {
      id: "workflow:wait-payment",
      graph: "workflow",
      module_id: "module:workflow:wait-payment",
      state_id: "wait-payment",
      kind: "wait_for_event",
      label: "wait-payment",
      subtitle: "wait signal · payment-approved",
      source_anchor: { file: "workflows/payments.ts", line: 18, column: 2 },
      next_ids: [],
      raw: { type: "wait_for_event" }
    }
  ],
  edges: [{ id: "edge-1", source: "workflow:charge-card", target: "workflow:wait-payment", label: "success", kind: "transition" }],
  modules: [
    {
      id: "module:workflow:charge-card",
      graph: "workflow",
      kind: "activity_step",
      label: "charge-card",
      subtitle: "payments.charge",
      node_ids: ["workflow:charge-card"],
      state_ids: ["charge-card"],
      focus_node_id: "workflow:charge-card",
      collapsed_by_default: false,
      source_anchor: { file: "workflows/payments.ts", line: 12, column: 2 },
      raw: { type: "step" }
    },
    {
      id: "module:workflow:wait-payment",
      graph: "workflow",
      kind: "wait",
      label: "wait-payment",
      subtitle: "wait signal · payment-approved",
      node_ids: ["workflow:wait-payment"],
      state_ids: ["wait-payment"],
      focus_node_id: "workflow:wait-payment",
      collapsed_by_default: false,
      source_anchor: { file: "workflows/payments.ts", line: 18, column: 2 },
      raw: { type: "wait_for_event" }
    }
  ],
  module_edges: [
    {
      id: "module-edge-1",
      source: "module:workflow:charge-card",
      target: "module:workflow:wait-payment",
      label: "success",
      kind: "transition"
    }
  ],
  overlay: {
    mode: "run",
    run_id: "run-2",
    current_node_id: "workflow:wait-payment",
    current_module_id: "module:workflow:wait-payment",
    blocked_by: {
      kind: "signal",
      label: "payment-approved",
      detail: "1 queued/consumed signals",
      node_id: "workflow:wait-payment",
      module_id: "module:workflow:wait-payment"
    },
    node_statuses: [
      { id: "workflow:charge-card", status: "failed", summary: "1 failed activity attempts" },
      { id: "workflow:wait-payment", status: "current", summary: "Current wait point" }
    ],
    module_statuses: [
      { id: "module:workflow:charge-card", status: "failed", summary: "1 failed activity attempts" },
      { id: "module:workflow:wait-payment", status: "current", summary: "Current wait point" }
    ],
    trace: [
      {
        id: "trace-1",
        occurred_at: "2026-03-13T09:01:00Z",
        lane: "activities",
        label: "ActivityTaskScheduled",
        detail: "payments.charge attempt 2",
        event_type: "ActivityTaskScheduled",
        node_id: "workflow:charge-card",
        module_id: "module:workflow:charge-card"
      },
      {
        id: "trace-2",
        occurred_at: "2026-03-13T09:03:00Z",
        lane: "messages",
        label: "SignalQueued",
        detail: "payment-approved",
        event_type: "SignalQueued",
        node_id: "workflow:wait-payment",
        module_id: "module:workflow:wait-payment"
      }
    ],
    activity_summaries: [
      {
        node_id: "workflow:charge-card",
        module_id: "module:workflow:charge-card",
        activity_type: "payments.charge",
        total: 1,
        pending: 0,
        completed: 0,
        failed: 1,
        retrying: 1,
        worker_build_ids: ["build-b"]
      }
    ],
    bulk_summaries: [],
    signal_summaries: [
      {
        signal_name: "payment-approved",
        count: 1,
        latest_status: "queued",
        latest_seen_at: "2026-03-13T09:03:00Z",
        node_id: "workflow:wait-payment",
        module_id: "module:workflow:wait-payment"
      }
    ],
    update_summaries: [],
    child_summaries: []
  }
};

test("switches graph explorer views and preserves runtime inspector context", async () => {
  render(<WorkflowGraphExplorer graph={graph} />);

  expect(screen.getByText("Blocked on signal")).toBeInTheDocument();
  expect(screen.getAllByText("payment-approved").length).toBeGreaterThan(0);

  await userEvent.click(screen.getByRole("button", { name: "Timeline" }));
  expect(screen.getAllByText("SignalQueued").length).toBeGreaterThan(0);

  await userEvent.click(screen.getByRole("button", { name: "Canvas" }));
  expect(screen.getByText("Selected node JSON")).toBeInTheDocument();
});
