import { Navigate, Route, Routes } from "react-router-dom";

import { AppShell } from "./components/app-shell";
import { BuildsPage } from "./pages/builds-page";
import { ConformancePage } from "./pages/conformance-page";
import { OverviewPage } from "./pages/overview-page";
import { ReplayPage } from "./pages/replay-page";
import { RunsPage } from "./pages/runs-page";
import { StreamingOpsPage } from "./pages/streaming-ops-page";
import { TaskQueuesPage } from "./pages/task-queues-page";
import { TopicAdaptersPage } from "./pages/topic-adapters-page";
import { WorkflowDefinitionDetailPage } from "./pages/workflow-definition-detail-page";
import { WorkflowDetailPage } from "./pages/workflow-detail-page";
import { WorkflowsPage } from "./pages/workflows-page";

export function App() {
  return (
    <Routes>
      <Route path="/" element={<AppShell />}>
        <Route index element={<OverviewPage />} />
        <Route path="workflows" element={<WorkflowsPage />} />
        <Route path="workflows/:definitionId" element={<WorkflowDefinitionDetailPage />} />
        <Route path="runs" element={<RunsPage />} />
        <Route path="runs/:instanceId" element={<WorkflowDetailPage />} />
        <Route path="runs/:instanceId/:runId" element={<WorkflowDetailPage />} />
        <Route path="replay" element={<ReplayPage />} />
        <Route path="builds" element={<BuildsPage />} />
        <Route path="conformance" element={<ConformancePage />} />
        <Route path="streaming" element={<StreamingOpsPage />} />
        <Route path="task-queues" element={<TaskQueuesPage />} />
        <Route path="topic-adapters" element={<TopicAdaptersPage />} />
      </Route>
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
