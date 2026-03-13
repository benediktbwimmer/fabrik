import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { FormEvent, useState } from "react";
import { toast } from "sonner";

import { api } from "../lib/api";
import { useTenant } from "../lib/tenant-context";
import { Panel } from "../components/ui";

export function BuildsPage() {
  const { tenantId } = useTenant();
  const [searchParams] = useSearchParams();
  const queueKind = searchParams.get("queue_kind") ?? "workflow";
  const taskQueue = searchParams.get("task_queue") ?? "";
  const [buildId, setBuildId] = useState("");
  const queuesQuery = useQuery({
    queryKey: ["task-queues-builds", tenantId],
    enabled: tenantId !== "",
    queryFn: () => api.listTaskQueues(tenantId)
  });

  async function onRegister(event: FormEvent) {
    event.preventDefault();
    if (!taskQueue || !buildId) return;
    try {
      await api.registerBuild(tenantId, queueKind, taskQueue, { build_id: buildId, artifact_hashes: [], promote_default: false });
      toast.success("Build registered");
      setBuildId("");
    } catch (error) {
      toast.error(String(error));
    }
  }

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <div className="eyebrow">Rollout safety</div>
          <h1>Builds & Routing</h1>
          <p>Use task-queue routing as the control plane for workflow and activity build rollout.</p>
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
          <h2>Register build</h2>
          <p className="muted">Use the selected task queue from global search or the task-queue page query string.</p>
          <form className="stack" onSubmit={onRegister}>
            <input className="input" value={queueKind} readOnly />
            <input className="input" value={taskQueue} readOnly />
            <input className="input" placeholder="build id" value={buildId} onChange={(event) => setBuildId(event.target.value)} />
            <button className="button primary" type="submit" disabled={!taskQueue}>
              Register build
            </button>
          </form>
        </Panel>
      </div>
    </div>
  );
}
