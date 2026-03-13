import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, vi } from "vitest";

import { App } from "../app";
import { AppProviders } from "../providers";

const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
  const url = String(input);
  if (url.includes("/conformance-reports/trust-summary.json")) {
    return new Response(
      JSON.stringify({
        schema_version: 1,
        milestone_scope: "temporal_ts_subset_trust",
        title: "Temporal TS subset trust",
        goal: "Prove the frozen Temporal TypeScript subset is deterministic, replay-safe, and operationally trustworthy in the lab.",
        generated_at: "2026-03-13T12:00:00Z",
        status: "failing",
        trusted_confidence_floor: "supported_failover_validated",
        upgrade_confidence_floor: "supported_upgrade_validated",
        reports: [
          {
            layer_id: "layer_a_support",
            title: "Layer A: support fixtures",
            purpose: "Analyzer, compiler, import, and packaging correctness for the frozen Temporal TS subset.",
            case_count: 7,
            passed_count: 7,
            failed_count: 0,
            report_path: "target/conformance-reports/layer-a-support.json",
            public_report_path: "/conformance-reports/layer-a-support.json",
            failed_cases: []
          },
          {
            layer_id: "layer_b_semantic",
            title: "Layer B: semantic parity fixtures",
            purpose: "Compiled semantic artifact cases for the supported Temporal TS subset.",
            case_count: 9,
            passed_count: 9,
            failed_count: 0,
            report_path: "target/conformance-reports/layer-b-semantic.json",
            public_report_path: "/conformance-reports/layer-b-semantic.json",
            failed_cases: []
          },
          {
            layer_id: "layer_c_trust",
            title: "Layer C: trust and failure fixtures",
            purpose: "Restart, failover, replay, upgrade, and routing trust checks for the frozen Temporal TS subset.",
            case_count: 8,
            passed_count: 7,
            failed_count: 1,
            report_path: "target/conformance-reports/layer-c-trust.json",
            public_report_path: "/conformance-reports/layer-c-trust.json",
            failed_cases: [
              {
                id: "store-reconciles-stale-resume-backlog",
                title: "Store reconciles stale resume backlog",
                summary: "duplicate delivery reconciliation regressed during the trust drill",
                href: "/conformance?layer=layer_c_trust&case=store-reconciles-stale-resume-backlog"
              }
            ]
          }
        ],
        confidence_bands: [
          {
            confidence_class: "supported_failover_validated",
            count: 6,
            features: ["Proxy activities", "Signals, queries, and updates"]
          },
          {
            confidence_class: "supported_upgrade_validated",
            count: 2,
            features: ["Version markers and workflow evolution", "Worker builds and routing"]
          }
        ],
        headline_trusted_features: ["Version markers and workflow evolution", "Worker builds and routing"],
        blocked_features: ["Payload and data converter usage"],
        promotion_requirements: ["analyzer support exists", "support fixtures exist"]
      }),
      { status: 200 }
    );
  }
  if (url.includes("/conformance-reports/layer-a-support.json")) {
    return new Response(
      JSON.stringify({
        schema_version: 1,
        layer_id: "layer_a_support",
        title: "Layer A: support fixtures",
        purpose: "Analyzer, compiler, import, and packaging correctness for the frozen Temporal TS subset.",
        case_count: 2,
        passed_count: 2,
        failed_count: 0,
        results: [
          {
            id: "proxy-activities-supported",
            title: "Proxy activities supported",
            kind: "analyzer",
            status: "passed",
            duration_ms: 12,
            summary: "workflows=1 workers=1 hard_blocks=0",
            observed: { workflow_count: 1 },
            evidence: { finding_count: 0, finding_features: [] }
          },
          {
            id: "payload-converter-blocked",
            title: "Payload converter blocked",
            kind: "analyzer",
            status: "passed",
            duration_ms: 16,
            summary: "workflows=1 workers=0 hard_blocks=1",
            observed: { hard_block_count: 1 },
            evidence: { finding_count: 1, finding_features: ["payload_converter"] }
          }
        ]
      }),
      { status: 200 }
    );
  }
  if (url.includes("/conformance-reports/layer-b-semantic.json")) {
    return new Response(
      JSON.stringify({
        schema_version: 1,
        layer_id: "layer_b_semantic",
        title: "Layer B: semantic parity fixtures",
        purpose: "Compiled semantic artifact cases for the supported Temporal TS subset.",
        case_count: 2,
        passed_count: 2,
        failed_count: 0,
        results: [
          {
            id: "signals-queries-updates",
            title: "Signals, queries, and updates",
            kind: "compiler",
            status: "passed",
            duration_ms: 22,
            summary: "definition=payments source_files=1",
            observed: { definition_id: "payments" },
            evidence: { source_files: ["workflows/payments.ts"], state_count: 4 }
          },
          {
            id: "continue-as-new",
            title: "Continue-as-new",
            kind: "compiler",
            status: "passed",
            duration_ms: 19,
            summary: "definition=handoff source_files=1",
            observed: { definition_id: "handoff" },
            evidence: { source_files: ["workflows/handoff.ts"], state_count: 3 }
          }
        ]
      }),
      { status: 200 }
    );
  }
  if (url.includes("/conformance-reports/layer-c-trust.json")) {
    return new Response(
      JSON.stringify({
        schema_version: 1,
        layer_id: "layer_c_trust",
        title: "Layer C: trust and failure fixtures",
        purpose: "Restart, failover, replay, upgrade, and routing trust checks for the frozen Temporal TS subset.",
        case_count: 2,
        passed_count: 1,
        failed_count: 1,
        results: [
          {
            id: "workflow-compatibility-accepts-version-guarded-change",
            title: "Workflow compatibility accepts version-guarded change",
            kind: "command",
            status: "passed",
            duration_ms: 242,
            summary: "test result: ok. 1 passed; 0 failed",
            observed: {
              command: "cargo test -p fabrik-workflow compiled_artifact_compatibility_accepts_version_marker_guarded_change -- --exact"
            },
            evidence: {
              combined_excerpt: "test result: ok. 1 passed; 0 failed"
            }
          },
          {
            id: "store-reconciles-stale-resume-backlog",
            title: "Store reconciles stale resume backlog",
            kind: "command",
            status: "failed",
            duration_ms: 8718,
            summary: "test result: FAILED. 0 passed; 1 failed",
            error: "Command failed: cargo test -p fabrik-store complete_workflow_task_reconciles_stale_resume_backlog -- --exact",
            observed: {
              command: "cargo test -p fabrik-store complete_workflow_task_reconciles_stale_resume_backlog -- --exact"
            },
            evidence: {
              combined_excerpt: "running 1 test\nstore reconciliation backlog assertion failed\ntest result: FAILED. 0 passed; 1 failed"
            }
          }
        ]
      }),
      { status: 200 }
    );
  }
  if (url.includes("/tenants/tenant-a/workflow-definitions/payments/graph")) {
    return new Response(
      JSON.stringify({
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
            id: "workflow:entry",
            graph: "workflow",
            module_id: "module:workflow:entry",
            state_id: null,
            kind: "entry",
            label: "Entry",
            subtitle: "workflows/payments.ts::workflow",
            source_anchor: null,
            next_ids: ["workflow:charge-card"],
            raw: {}
          },
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
            raw: {}
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
            raw: {}
          },
          {
            id: "signal:payment-approved:accepted",
            graph: "signal_handler",
            module_id: "module:signal:payment-approved",
            state_id: "accepted",
            kind: "succeed",
            label: "accepted",
            subtitle: "success",
            source_anchor: null,
            next_ids: [],
            raw: {}
          }
        ],
        edges: [
          { id: "workflow:entry->workflow:charge-card", source: "workflow:entry", target: "workflow:charge-card", label: "starts", kind: "entry" },
          { id: "workflow:charge-card->workflow:wait-payment", source: "workflow:charge-card", target: "workflow:wait-payment", label: "success", kind: "transition" },
          { id: "system:handlers->signal:payment-approved:accepted", source: "system:handlers", target: "signal:payment-approved:accepted", label: "signal payment-approved", kind: "handler" }
        ],
        modules: [
          {
            id: "module:workflow:entry",
            graph: "workflow",
            kind: "entry",
            label: "Entry",
            subtitle: "workflows/payments.ts::workflow",
            node_ids: ["workflow:entry"],
            state_ids: [],
            focus_node_id: "workflow:entry",
            collapsed_by_default: false,
            source_anchor: null,
            raw: {}
          },
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
            raw: {}
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
            raw: {}
          },
          {
            id: "module:signal:payment-approved",
            graph: "signal_handler",
            kind: "signal_handler",
            label: "payment-approved",
            subtitle: "signal handler",
            node_ids: ["signal:payment-approved:accepted"],
            state_ids: ["accepted"],
            focus_node_id: "signal:payment-approved:accepted",
            collapsed_by_default: true,
            source_anchor: { file: "workflows/payments.ts", line: 22, column: 4 },
            raw: {}
          }
        ],
        module_edges: [
          { id: "module:workflow:entry->module:workflow:charge-card", source: "module:workflow:entry", target: "module:workflow:charge-card", label: "starts", kind: "entry" },
          { id: "module:workflow:charge-card->module:workflow:wait-payment", source: "module:workflow:charge-card", target: "module:workflow:wait-payment", label: "success", kind: "transition" }
        ],
        overlay: {
          mode: "artifact",
          run_id: null,
          current_node_id: null,
          current_module_id: null,
          blocked_by: null,
          node_statuses: [],
          module_statuses: [],
          trace: [],
          activity_summaries: [],
          bulk_summaries: [],
          signal_summaries: [],
          update_summaries: [],
          child_summaries: []
        }
      }),
      { status: 200 }
    );
  }
  if (url.includes("/tenants/tenant-a/workflows/instance-1/runs/") && url.includes("/graph")) {
    return new Response(
      JSON.stringify({
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
            raw: {}
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
            raw: {}
          }
        ],
        edges: [{ id: "workflow:charge-card->workflow:wait-payment", source: "workflow:charge-card", target: "workflow:wait-payment", label: "success", kind: "transition" }],
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
            raw: {}
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
            raw: {}
          }
        ],
        module_edges: [{ id: "module:workflow:charge-card->module:workflow:wait-payment", source: "module:workflow:charge-card", target: "module:workflow:wait-payment", label: "success", kind: "transition" }],
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
      }),
      { status: 200 }
    );
  }
  if (url.includes("/tenants/tenant-a/workflow-definitions/payments/latest")) {
    return new Response(
      JSON.stringify({
        id: "payments",
        version: 2,
        initial_state: "charge-card",
        states: {
          "charge-card": { type: "step" },
          "wait-payment": { type: "wait_for_event" }
        }
      }),
      { status: 200 }
    );
  }
  if (url.includes("/tenants/tenant-a/workflow-artifacts/payments/latest")) {
    return new Response(
      JSON.stringify({
        definition_id: "payments",
        definition_version: 2,
        compiler_version: "tsc-0.1",
        source_language: "typescript",
        entrypoint: { module: "workflows/payments.ts", export: "workflow" },
        source_files: ["workflows/payments.ts"],
        source_map: {},
        queries: { status: {} },
        signals: { "payment-approved": {} },
        updates: { reprice: {} },
        workflow: {
          initial_state: "charge-card",
          states: {
            "charge-card": { type: "step" },
            "wait-payment": { type: "wait_for_event" }
          }
        },
        artifact_hash: "artifact-b"
      }),
      { status: 200 }
    );
  }
  if (url.includes("/tenants/tenant-a/workflow-artifacts/validate")) {
    return new Response(
      JSON.stringify({
        tenant_id: "tenant-a",
        definition_id: "payments",
        version: 2,
        artifact_hash: "artifact-b",
        compatible: false,
        status: "incompatible",
        validation: {
          enabled: true,
          validated_run_count: 2,
          skipped_run_count: 1,
          failed_run_count: 1,
          failures: [
            {
              instance_id: "instance-legacy",
              run_id: "run-17",
              message: "candidate artifact replay diverged after event evt-17 (WorkflowCompleted)",
              divergence: {
                kind: "projection_mismatch",
                message: "candidate artifact replay diverged after event evt-17 (WorkflowCompleted)",
                fields: [{ field: "output" }]
              }
            }
          ]
        }
      }),
      { status: 200 }
    );
  }
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
            routing_status: "sticky_active",
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
            routing_status: "queue_default_active",
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
            routing_status: "sticky_active",
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
  if (url.includes("/tenants/tenant-a/workflows/instance-1/runs/run-1/replay")) {
    return new Response(
      JSON.stringify({
        tenant_id: "tenant-a",
        instance_id: "instance-1",
        run_id: "run-1",
        definition_id: "payments",
        definition_version: 1,
        artifact_hash: "artifact-a",
        divergence_count: 0,
        divergences: [],
        transition_count: 8,
        replay_source: "snapshot_tail",
        projection_matches_store: true,
        snapshot: {
          run_id: "run-1",
          snapshot_schema_version: 1,
          event_count: 8,
          last_event_id: "evt-2",
          last_event_type: "WorkflowContinuedAsNew",
          updated_at: "2026-03-13T08:10:00Z"
        },
        replayed_state: {
          workflow_task_queue: "payments",
          memo: { region: "eu" },
          search_attributes: { Region: "eu" },
          status: "continued",
          current_state: "handoff"
        }
      }),
      { status: 200 }
    );
  }
  if (url.includes("/tenants/tenant-a/workflows/instance-1/runs/run-2/replay")) {
    return new Response(
      JSON.stringify({
        tenant_id: "tenant-a",
        instance_id: "instance-1",
        run_id: "run-2",
        definition_id: "payments",
        definition_version: 2,
        artifact_hash: "artifact-b",
        divergence_count: 0,
        divergences: [],
        transition_count: 12,
        replay_source: "full_history",
        projection_matches_store: true,
        snapshot: {
          run_id: "run-2",
          snapshot_schema_version: 1,
          event_count: 12,
          last_event_id: "evt-9",
          last_event_type: "ActivityScheduled",
          updated_at: "2026-03-13T09:05:00Z"
        },
        replayed_state: {
          workflow_task_queue: "payments",
          memo: { region: "us" },
          search_attributes: { Region: "us" },
          status: "running",
          current_state: "charge-card"
        }
      }),
      { status: 200 }
    );
  }
  if (url.includes("/tenants/tenant-a/workflows/instance-1/routing")) {
    return new Response(
      JSON.stringify({
        tenant_id: "tenant-a",
        instance_id: "instance-1",
        run_id: "run-2",
        definition_id: "payments",
        definition_version: 2,
        artifact_hash: "artifact-b",
        workflow_task_queue: "payments",
        routing_status: "sticky_active",
        default_compatibility_set_id: "stable",
        compatible_build_ids: ["build-b"],
        registered_build_ids: ["build-a", "build-b"],
        sticky_workflow_build_id: "build-b",
        sticky_workflow_poller_id: "poller-b",
        sticky_updated_at: "2026-03-13T09:04:00Z",
        sticky_build_compatible_with_queue: true,
        sticky_build_supports_pinned_artifact: true
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
        recent_failures: [
          {
            tenant_id: "tenant-a",
            instance_id: "instance-failed",
            run_id: "run-failed",
            definition_id: "payments",
            definition_version: 2,
            artifact_hash: "artifact-b",
            workflow_task_queue: "payments",
            sticky_workflow_build_id: "build-b",
            sticky_workflow_poller_id: "poller-b",
            routing_status: "sticky_incompatible_fallback_required",
            current_state: "charge-card",
            status: "failed",
            event_count: 9,
            last_event_id: "evt-failed",
            last_event_type: "WorkflowFailed",
            updated_at: "2026-03-13T09:06:00Z",
            consistency: "eventual",
            source: "projection"
          }
        ],
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
  expect(screen.getAllByText("sticky_active").length).toBeGreaterThan(0);
  expect(screen.getByRole("link", { name: "Inspect" })).toHaveAttribute("href", "/runs/instance-1/run-2");
});

test("supports routing-status run filtering in the runs view", async () => {
  renderApp("/runs?routing_status=sticky_active");

  await waitFor(() => expect(screen.getByRole("link", { name: "Inspect" })).toBeInTheDocument());
  expect(screen.getByDisplayValue("sticky_active")).toBeInTheDocument();
  expect(screen.getAllByText("sticky_active").length).toBeGreaterThan(0);
  expect(screen.getByRole("link", { name: "Queue" })).toHaveAttribute(
    "href",
    "/task-queues?queue_kind=workflow&task_queue=payments"
  );
  expect(screen.getByRole("link", { name: "Builds" })).toHaveAttribute(
    "href",
    "/builds?queue_kind=workflow&task_queue=payments"
  );
});

test("shows routing status on overview recent failures", async () => {
  renderApp("/");

  await waitFor(() => expect(screen.getByText("sticky_incompatible_fallback_required")).toBeInTheDocument());
  await waitFor(() => expect(screen.getByText("Layer A: support fixtures")).toBeInTheDocument());
  expect(screen.getByText("sticky_incompatible_fallback_required")).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Inspect" })).toHaveAttribute("href", "/runs/instance-failed/run-failed");
  expect(screen.getByText("Temporal TS subset trust")).toBeInTheDocument();
  expect(screen.getByText("Layer A: support fixtures")).toBeInTheDocument();
  expect(screen.getByRole("link", { name: /Workflow definitions/ })).toHaveAttribute("href", "/workflows");
});

test("disables safe actions for a historical run detail", async () => {
  renderApp("/runs/instance-1/run-1");

  await waitFor(() => expect(screen.getByRole("heading", { name: "payments" })).toBeInTheDocument());
  expect(screen.getByRole("button", { name: "Send signal" })).toBeDisabled();
  expect(screen.getByText("Routing status sticky_active")).toBeInTheDocument();
  expect(screen.getByText("Default set stable")).toBeInTheDocument();
  expect(screen.getByText("Compatible builds build-b")).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Inspect workflow queue" })).toHaveAttribute(
    "href",
    "/task-queues?queue_kind=workflow&task_queue=payments"
  );
  expect(screen.getByText("Pinned definition version 1")).toBeInTheDocument();
  expect(screen.getByText("Replay kept pinned artifact true")).toBeInTheDocument();
  expect(screen.getByText(/run-1 · v1 · artifact-a/)).toBeInTheDocument();
  await userEvent.click(screen.getByRole("button", { name: "raw-history" }));
  expect(screen.getAllByText("View payload").length).toBeGreaterThan(0);
});

test("shows pinned artifact and routing evidence in the replay workbench", async () => {
  renderApp("/replay?instance=instance-1&run=run-2");

  await waitFor(() => expect(screen.getByRole("heading", { name: "Replay" })).toBeInTheDocument());
  await waitFor(() => expect(screen.getByText("Replay kept pinned artifact true")).toBeInTheDocument());
  expect(screen.getByText("Replay kept pinned version true")).toBeInTheDocument();
  expect(screen.getByText("Routing status sticky_active")).toBeInTheDocument();
  expect(screen.getByText("Default set stable")).toBeInTheDocument();
  expect(screen.getByText("Sticky build build-b")).toBeInTheDocument();
});

test("keeps task queue inspection read-only", async () => {
  renderApp("/task-queues?queue_kind=workflow&task_queue=payments");

  await waitFor(() => expect(screen.queryByText("Select a queue to inspect its health and poller state.")).not.toBeInTheDocument());
  expect(screen.getAllByText("payments").length).toBeGreaterThan(0);
  expect(screen.queryByText("Promote default")).not.toBeInTheDocument();
  expect(screen.queryByText("stream-v2")).not.toBeInTheDocument();
});

test("keeps build routing surfaces read-only", async () => {
  renderApp("/builds?queue_kind=workflow&task_queue=payments");

  await waitFor(() => expect(screen.getByRole("heading", { name: "Builds & Routing" })).toBeInTheDocument());
  await waitFor(() => expect(screen.getByText("Layer C: trust and failure fixtures")).toBeInTheDocument());
  expect(screen.getByText("Trust evidence")).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "Register build" })).not.toBeInTheDocument();
  expect(screen.getByText("Upgrade-sensitive features")).toBeInTheDocument();
  expect(screen.getByText(/Version markers and workflow evolution/)).toBeInTheDocument();
  expect(screen.getByRole("link", { name: /Store reconciles stale resume backlog/ })).toHaveAttribute(
    "href",
    "/conformance?layer=layer_c_trust&case=store-reconciles-stale-resume-backlog"
  );
});

test("renders conformance drill-down evidence", async () => {
  renderApp("/conformance?layer=layer_c_trust&case=store-reconciles-stale-resume-backlog");

  await waitFor(() => expect(screen.getByRole("heading", { name: "Conformance" })).toBeInTheDocument());
  await waitFor(() => expect(screen.getByRole("heading", { name: "Store reconciles stale resume backlog" })).toBeInTheDocument());
  expect(screen.getByRole("link", { name: /Layer C: trust and failure fixtures/ })).toHaveAttribute(
    "href",
    "/conformance?layer=layer_c_trust"
  );
  expect(screen.getAllByText("test result: FAILED. 0 passed; 1 failed").length).toBeGreaterThan(0);
  expect(screen.getByText(/running 1 test/)).toBeInTheDocument();
  expect(screen.getAllByText(/complete_workflow_task_reconciles_stale_resume_backlog/).length).toBeGreaterThan(0);
  expect(screen.getByRole("link", { name: "Raw JSON" })).toHaveAttribute("href", "/conformance-reports/layer-c-trust.json");
});

test("renders workflow definition graph explorer", async () => {
  renderApp("/workflows/payments");

  await waitFor(() => expect(screen.getByText("Graph Explorer")).toBeInTheDocument());
  expect(screen.getByRole("button", { name: "Semantic Map" })).toBeInTheDocument();
  expect(screen.getByText("Entry")).toBeInTheDocument();
});

test("runs dry-run rollout validation from workflow definition detail", async () => {
  renderApp("/workflows/payments");

  await waitFor(() => expect(screen.getByRole("button", { name: "overview" })).toBeInTheDocument());
  await userEvent.click(screen.getByRole("button", { name: "overview" }));
  await userEvent.click(screen.getByRole("button", { name: "Validate rollout" }));

  await waitFor(() => expect(screen.getByText("incompatible")).toBeInTheDocument());
  expect(screen.getByText("Validated runs 2")).toBeInTheDocument();
  expect(screen.getByText("Skipped 1")).toBeInTheDocument();
  expect(screen.getByText("Failures 1")).toBeInTheDocument();
  expect(screen.getByText("instance-legacy / run-17")).toBeInTheDocument();
});
