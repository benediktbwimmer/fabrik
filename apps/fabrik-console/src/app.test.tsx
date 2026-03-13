import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { vi } from "vitest";

import { App } from "./app";
import { AppProviders } from "./providers";

vi.stubGlobal(
  "fetch",
  vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("/conformance-reports/trust-summary.json")) {
      return new Response(
        JSON.stringify({
          schema_version: 1,
          milestone_scope: "temporal_ts_subset_trust",
          title: "Temporal TS subset trust",
          goal: "Prove the frozen Temporal TypeScript subset is deterministic, replay-safe, and operationally trustworthy in the lab.",
          generated_at: "2026-03-13T12:00:00Z",
        status: "passing",
        trusted_confidence_floor: "supported_failover_validated",
        upgrade_confidence_floor: "supported_upgrade_validated",
        reports: [],
        confidence_bands: [],
        headline_trusted_features: [],
        blocked_features: [],
        promotion_requirements: []
        }),
        { status: 200 }
      );
    }
    if (url.includes("/admin/tenants")) {
      return new Response(JSON.stringify({ tenants: ["tenant-a"] }), { status: 200 });
    }
    if (url.includes("/tenants/tenant-a/overview")) {
      return new Response(
        JSON.stringify({
          tenant_id: "tenant-a",
          consistency: "eventual",
          authoritative_source: "projection",
          total_workflows: 2,
          total_workflow_definitions: 1,
          total_workflow_artifacts: 1,
          counts_by_status: { running: 1, failed: 1 },
          total_task_queues: 1,
          total_backlog: 3,
          total_pollers: 1,
          total_registered_builds: 1,
          replay_divergence_count: 0,
          recent_failures: [],
          hottest_task_queues: []
        }),
        { status: 200 }
      );
    }
    return new Response(JSON.stringify({ items: [] }), { status: 200 });
  })
);

test("renders overview shell", async () => {
  render(
    <MemoryRouter initialEntries={["/"]} future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      <AppProviders>
        <App />
      </AppProviders>
    </MemoryRouter>
  );

  await waitFor(() => expect(screen.getByRole("heading", { name: "Home" })).toBeInTheDocument());
  expect(screen.getByText("Runs")).toBeInTheDocument();
  expect(screen.getByText("Conformance")).toBeInTheDocument();
});
