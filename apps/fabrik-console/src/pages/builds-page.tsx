import { useQuery } from "@tanstack/react-query";
import { Link, useSearchParams } from "react-router-dom";

import { api } from "../lib/api";
import { useTenant } from "../lib/tenant-context";
import { Badge, Panel } from "../components/ui";

export function BuildsPage() {
  const { tenantId } = useTenant();
  const [searchParams] = useSearchParams();
  const queueKind = searchParams.get("queue_kind") ?? "workflow";
  const taskQueue = searchParams.get("task_queue") ?? "";
  const queuesQuery = useQuery({
    queryKey: ["task-queues-builds", tenantId],
    enabled: tenantId !== "",
    queryFn: () => api.listTaskQueues(tenantId)
  });
  const trustQuery = useQuery({
    queryKey: ["trust-summary"],
    queryFn: () => api.getTrustSummary()
  });
  const selectedQueue = (queuesQuery.data?.items ?? []).find(
    (item) => item.queue_kind === queueKind && item.task_queue === taskQueue
  );
  const trustSummary = trustQuery.data;
  const layerC = trustSummary?.reports.find((report) => report.layer_id === "layer_c_trust");

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <div className="eyebrow">Rollout safety</div>
          <h1>Builds & Routing</h1>
          <p>Read-only build and routing evidence for pinned replay, mixed-build coexistence, and queue compatibility.</p>
        </div>
      </header>

      <div className="split">
        <Panel>
          <table className="table">
            <thead>
              <tr>
                <th>Task queue</th>
                <th>Kind</th>
                <th>Registered builds</th>
                <th>Default set</th>
              </tr>
            </thead>
            <tbody>
              {(queuesQuery.data?.items ?? []).map((item) => (
                <tr key={`${item.queue_kind}:${item.task_queue}`}>
                  <td>{item.task_queue}</td>
                  <td>{item.queue_kind}</td>
                  <td>{item.registered_build_count}</td>
                  <td>{item.default_set_id ?? "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Panel>

        <Panel>
          <h2>Trust evidence</h2>
          <div className="stack">
            <div className="muted">
              This milestone keeps routing surfaces read-only. Queue selection is used to inspect compatibility inventory, not to mutate rollout state from the console.
            </div>
            <div className="subtle-block">
              <div className="row space-between">
                <strong>Selected queue</strong>
                <Badge value={queueKind} />
              </div>
              <div className="muted">{taskQueue || "Select a queue from the table or task-queue page."}</div>
              <div className="muted">
                Registered builds {selectedQueue?.registered_build_count ?? 0} · default set {selectedQueue?.default_set_id ?? "-"}
              </div>
            </div>
            {layerC ? (
              <div className="subtle-block">
                <div className="row space-between">
                  <strong>{layerC.title}</strong>
                  <Badge value={layerC.failed_count === 0 ? "passing" : "failing"} />
                </div>
                <div className="muted">{layerC.purpose}</div>
                <div className="muted">
                  Cases {layerC.case_count} · passed {layerC.passed_count} · failed {layerC.failed_count}
                </div>
                <div className="row">
                  <Link className="button ghost" to={`/conformance?layer=${encodeURIComponent(layerC.layer_id)}`}>
                    Open layer report
                  </Link>
                  <a className="button ghost" href={layerC.public_report_path}>
                    Raw JSON
                  </a>
                </div>
                {layerC.failed_cases.length > 0 ? (
                  <div className="stack" style={{ marginTop: 12 }}>
                    {layerC.failed_cases.map((testCase) => (
                      <Link key={testCase.id} className="panel nested-panel" to={testCase.href}>
                        <strong>{testCase.title}</strong>
                        <div className="muted">{testCase.summary}</div>
                      </Link>
                    ))}
                  </div>
                ) : (
                  <div className="muted" style={{ marginTop: 12 }}>
                    No failed trust cases.
                  </div>
                )}
              </div>
            ) : (
              <div className="subtle-block">
                <strong>Layer C</strong>
                <div className="muted">No trust and failure report snapshot loaded.</div>
              </div>
            )}
            <div className="subtle-block">
              <strong>Upgrade-sensitive features</strong>
              <div className="muted">
                {(trustSummary?.headline_trusted_features ?? []).join(", ") || "No trusted feature snapshot loaded."}
              </div>
            </div>
            {trustSummary ? (
              <div className="subtle-block">
                <strong>Promotion rule</strong>
                <div className="muted">{trustSummary.promotion_requirements.join("; ")}</div>
              </div>
            ) : null}
          </div>
        </Panel>
      </div>
    </div>
  );
}
