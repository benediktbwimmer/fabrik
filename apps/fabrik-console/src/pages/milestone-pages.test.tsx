import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, vi } from "vitest";

import { App } from "../app";
import { AppProviders } from "../providers";

const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
  const url = String(input);
  if (url.includes("/tenants/tenant-a/runs") && url.includes("instance_id=instance-1")) {
    return new Response(
      JSON.stringify({
        tenant_id: "tenant-a",
        consistency: "eventual",
        authoritative_source: "projection",
        page: { limit: 200, offset: 0, returned: 2, total: 2, has_more: false, next_offset: null },
        run_count: 2,
        items: [
          {
            tenant_id: "tenant-a",
            instance_id: "instance-1",
            run_id: "run-2",
            definition_id: "payments",
            definition_version: 2,
            artifact_hash: "artifact-b",
            workflow_task_queue: "payments",
            sticky_workflow_build_id: "build-b",
            sticky_workflow_poller_id: "poller-b",
            sticky_updated_at: "2026-03-13T09:04:00Z",
            previous_run_id: "run-1",
            next_run_id: null,
            continue_reason: null,
            started_at: "2026-03-13T09:00:00Z",
            closed_at: null,
            updated_at: "2026-03-13T09:05:00Z",
            last_transition_at: "2026-03-13T09:05:00Z",
            status: "running",
            current_state: "charge-card",
            last_event_type: "ActivityScheduled",
            event_count: 12,
            consistency: "eventual",
            source: "projection"
          },
          {
            tenant_id: "tenant-a",
            instance_id: "instance-1",
            run_id: "run-1",
            definition_id: "payments",
            definition_version: 1,
            artifact_hash: "artifact-a",
            workflow_task_queue: "payments",
            sticky_workflow_build_id: "build-a",
            sticky_workflow_poller_id: "poller-a",
            sticky_updated_at: "2026-03-13T08:10:00Z",
            previous_run_id: null,
            next_run_id: "run-2",
            continue_reason: "continue-as-new",
            started_at: "2026-03-13T08:00:00Z",
            closed_at: "2026-03-13T08:10:00Z",
            updated_at: "2026-03-13T08:10:00Z",
            last_transition_at: "2026-03-13T08:10:00Z",
            status: "continued",
            current_state: "handoff",
            last_event_type: "WorkflowContinuedAsNew",
            event_count: 8,
            consistency: "eventual",
            source: "projection"
          }
        ]
      }),
      { status: 200 }
    );
  }
  if (url.includes("/tenants/tenant-a/runs")) {
    return new Response(
      JSON.stringify({
        tenant_id: "tenant-a",
        consistency: "eventual",
        authoritative_source: "projection",
        page: { limit: 25, offset: 0, returned: 1, total: 1, has_more: false, next_offset: null },
        run_count: 1,
        items: [
          {
            tenant_id: "tenant-a",
            instance_id: "instance-1",
            run_id: "run-2",
            definition_id: "payments",
            definition_version: 2,
            artifact_hash: "artifact-b",
            workflow_task_queue: "payments",
            sticky_workflow_build_id: "build-b",
            sticky_workflow_poller_id: "poller-b",
            sticky_updated_at: "2026-03-13T09:04:00Z",
            previous_run_id: "run-1",
            next_run_id: null,
            continue_reason: null,
            started_at: "2026-03-13T09:00:00Z",
            closed_at: null,
            updated_at: "2026-03-13T09:05:00Z",
            last_transition_at: "2026-03-13T09:05:00Z",
            status: "running",
            current_state: "charge-card",
            last_event_type: "ActivityScheduled",
            event_count: 12,
            consistency: "eventual",
            source: "projection"
          }
        ]
      }),
      { status: 200 }
    );
  }
  if (url.includes("/tenants/tenant-a/workflows/instance-1/history") || url.includes("/runs/run-1/history")) {
    return new Response(
      JSON.stringify({
        tenant_id: "tenant-a",
        instance_id: "instance-1",
        run_id: "run-1",
        definition_id: "payments",
        definition_version: 1,
        artifact_hash: "artifact-a",
        previous_run_id: null,
        next_run_id: "run-2",
        continue_reason: "continue-as-new",
        event_count: 2,
        page: { limit: 100, offset: 0, returned: 2, total: 2, has_more: false, next_offset: null },
        activity_attempt_count: 0,
        activity_attempts: [],
        events: [
          { event_id: "evt-1", event_type: "WorkflowStarted", occurred_at: "2026-03-13T08:00:00Z", metadata: {}, payload: { input: true } },
          { event_id: "evt-2", event_type: "WorkflowContinuedAsNew", occurred_at: "2026-03-13T08:10:00Z", metadata: {}, payload: { ok: true } }
        ]
      }),
      { status: 200 }
    );
  }
  if (url.includes("/tenants/tenant-a/workflows/instance-1/activities") || url.includes("/runs/run-1/activities")) {
    return new Response(
      JSON.stringify({
        tenant_id: "tenant-a",
        instance_id: "instance-1",
        run_id: "run-1",
        page: { limit: 100, offset: 0, returned: 1, total: 1, has_more: false, next_offset: null },
        activity_count: 1,
        activities: [
          {
            activity_id: "activity-1",
            attempt: 1,
            activity_type: "charge-card",
            task_queue: "payments",
            state: null,
            status: "completed",
            worker_id: "worker-a",
            worker_build_id: "build-a",
            scheduled_at: "2026-03-13T08:01:00Z",
            started_at: "2026-03-13T08:02:00Z",
            completed_at: "2026-03-13T08:03:00Z",
            error: null
          }
        ]
      }),
      { status: 200 }
    );
  }
  if (url.includes("/tenants/tenant-a/workflows/instance-1") && !url.includes("/history") && !url.includes("/activities")) {
    return new Response(
      JSON.stringify({
        tenant_id: "tenant-a",
        instance_id: "instance-1",
        run_id: "run-2",
        definition_id: "payments",
        definition_version: 2,
        artifact_hash: "artifact-b",
        workflow_task_queue: "payments",
        sticky_workflow_build_id: "build-b",
        sticky_workflow_poller_id: "poller-b",
        current_state: "charge-card",
        context: {},
        status: "running",
        input: {},
        output: null,
        event_count: 12,
        last_event_id: "evt-9",
        last_event_type: "ActivityScheduled",
        updated_at: "2026-03-13T09:05:00Z"
      }),
      { status: 200 }
    );
  }
  if (url.includes("/admin/tenants/tenant-a/task-queues/workflow/payments")) {
    return new Response(
      JSON.stringify({
        tenant_id: "tenant-a",
        queue_kind: "workflow",
        task_queue: "payments",
        backlog: 4,
        oldest_backlog_at: "2026-03-13T09:01:00Z",
        sticky_effectiveness: { sticky_hit_rate: 0.75, sticky_fallback_rate: 0.25 },
        resume_coalescing: null,
        activity_completion_metrics: {
          completed: 5,
          failed: 0,
          cancelled: 0,
          timed_out: 0,
          avg_schedule_to_start_latency_ms: 45,
          avg_start_to_close_latency_ms: 420
        },
        default_set_id: "stable",
        throughput_policy: { backend: "pg-v1" },
        compatible_build_ids: ["build-a"],
        registered_builds: [{ build_id: "build-a", artifact_hashes: ["artifact-a"], updated_at: "2026-03-13T09:00:00Z" }],
        compatibility_sets: [{ set_id: "stable", build_ids: ["build-a"], is_default: true, updated_at: "2026-03-13T09:00:00Z" }],
        pollers: [{ poller_id: "poller-a", build_id: "build-a", partition_id: 2, last_seen_at: "2026-03-13T09:04:00Z", expires_at: "2026-03-13T09:05:00Z" }]
      }),
      { status: 200 }
    );
  }
  if (url.includes("/admin/tenants/tenant-a/task-queues")) {
    return new Response(
      JSON.stringify({
        tenant_id: "tenant-a",
        consistency: "eventual",
        authoritative_source: "projection",
        queue_count: 1,
        items: [
          {
            tenant_id: "tenant-a",
            queue_kind: "workflow",
            task_queue: "payments",
            backlog: 4,
            oldest_backlog_at: "2026-03-13T09:01:00Z",
            poller_count: 1,
            registered_build_count: 1,
            default_set_id: "stable",
            throughput_backend: "pg-v1",
            sticky_hit_rate: 0.75,
            consistency: "eventual",
            source: "projection"
          }
        ]
      }),
      { status: 200 }
    );
  }
  if (url.includes("/tenants/tenant-a/overview")) {
    return new Response(
      JSON.stringify({
        tenant_id: "tenant-a",
        consistency: "eventual",
        authoritative_source: "projection",
        total_workflows: 1,
        total_workflow_definitions: 1,
        total_workflow_artifacts: 1,
        counts_by_status: { running: 1, failed: 0 },
        total_task_queues: 1,
        total_backlog: 4,
        total_pollers: 1,
        total_registered_builds: 1,
        replay_divergence_count: 0,
        recent_failures: [],
        hottest_task_queues: []
      }),
      { status: 200 }
    );
  }
  if (url.includes("/workflow-definitions") || url.includes("/workflow-artifacts") || url.includes("/search")) {
    return new Response(JSON.stringify({ tenant_id: "tenant-a", items: [] }), { status: 200 });
  }
  if (url.endsWith("/admin/tenants")) {
    return new Response(JSON.stringify({ tenants: ["tenant-a"] }), { status: 200 });
  }
  return new Response(JSON.stringify({}), { status: 200 });
});

vi.stubGlobal("fetch", fetchMock);

beforeEach(() => {
  fetchMock.mockClear();
});

function renderApp(path: string) {
  render(
    <MemoryRouter initialEntries={[path]} future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      <AppProviders>
        <App />
      </AppProviders>
    </MemoryRouter>
  );
}

test("renders runs index rows", async () => {
  renderApp("/runs");

  await waitFor(() => expect(screen.getByRole("link", { name: "Inspect" })).toBeInTheDocument());
  expect(screen.getAllByText("payments").length).toBeGreaterThan(0);
  expect(screen.getByRole("link", { name: "Inspect" })).toHaveAttribute("href", "/runs/instance-1/run-2");
});

test("disables safe actions for a historical run detail", async () => {
  renderApp("/runs/instance-1/run-1");

  await waitFor(() => expect(screen.getByRole("heading", { name: "payments" })).toBeInTheDocument());
  expect(screen.getByRole("button", { name: "Send signal" })).toBeDisabled();
  await userEvent.click(screen.getByRole("button", { name: "raw-history" }));
  expect(screen.getAllByText("View payload").length).toBeGreaterThan(0);
});

test("keeps task queue inspection read-only", async () => {
  renderApp("/task-queues?queue_kind=workflow&task_queue=payments");

  await waitFor(() => expect(screen.queryByText("Select a queue to inspect its health and poller state.")).not.toBeInTheDocument());
  expect(screen.getAllByText("payments").length).toBeGreaterThan(0);
  expect(screen.queryByText("Promote default")).not.toBeInTheDocument();
  expect(screen.queryByText("stream-v2")).not.toBeInTheDocument();
});
