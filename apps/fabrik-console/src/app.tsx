import { Navigate, Route, Routes, useParams } from "react-router-dom";

import { AppShell } from "./components/app-shell";
import { OverviewPage } from "./pages/overview-page";
import { RunsPage } from "./pages/runs-page";
import { TaskQueuesPage } from "./pages/task-queues-page";
import { WorkflowDetailPage } from "./pages/workflow-detail-page";
import { WorkflowsPage } from "./pages/workflows-page";

function LegacyWorkflowRedirect() {
  const { instanceId = "" } = useParams();
  return <Navigate to={`/runs/${instanceId}`} replace />;
}

export function App() {
  return (
    <Routes>
      <Route path="/" element={<AppShell />}>
        <Route index element={<OverviewPage />} />
        <Route path="workflows" element={<WorkflowsPage />} />
        <Route path="workflows/:instanceId" element={<LegacyWorkflowRedirect />} />
        <Route path="runs" element={<RunsPage />} />
        <Route path="runs/:instanceId" element={<WorkflowDetailPage />} />
        <Route path="runs/:instanceId/:runId" element={<WorkflowDetailPage />} />
        <Route path="task-queues" element={<TaskQueuesPage />} />
      </Route>
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
