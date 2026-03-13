import { useQuery } from "@tanstack/react-query";
import { Link, useSearchParams } from "react-router-dom";

import { Badge, Panel } from "../components/ui";
import { api } from "../lib/api";
import { formatInlineValue, formatNumber } from "../lib/format";

function buildLayerParams(current: URLSearchParams, layerId: string, caseId?: string) {
  const next = new URLSearchParams(current);
  next.set("layer", layerId);
  if (caseId) {
    next.set("case", caseId);
  } else {
    next.delete("case");
  }
  return `?${next.toString()}`;
}

export function ConformancePage() {
  const [searchParams] = useSearchParams();
  const trustQuery = useQuery({
    queryKey: ["trust-summary"],
    queryFn: () => api.getTrustSummary(),
  });

  const selectedLayerId = searchParams.get("layer") ?? trustQuery.data?.reports[0]?.layer_id ?? null;
  const selectedLayer = trustQuery.data?.reports.find((report) => report.layer_id === selectedLayerId) ?? null;
  const reportQuery = useQuery({
    queryKey: ["conformance-report", selectedLayer?.public_report_path],
    enabled: Boolean(selectedLayer?.public_report_path),
    queryFn: () => api.getConformanceReport(selectedLayer!.public_report_path),
  });
  const selectedCaseId = searchParams.get("case");
  const selectedCase =
    reportQuery.data?.results.find((result) => result.id === selectedCaseId) ??
    reportQuery.data?.results.find((result) => result.status === "failed") ??
    reportQuery.data?.results[0] ??
    null;

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <div className="eyebrow">Milestone evidence</div>
          <h1>Conformance</h1>
          <p>Read-only fixture results for support coverage, runtime semantics, and restart/failover trust.</p>
        </div>
        <Badge value={trustQuery.data?.status ?? "loading"} />
      </header>

      <div className="split">
        <Panel>
          <h2>Layers</h2>
          <div className="stack">
            {(trustQuery.data?.reports ?? []).map((layer) => (
              <Link
                key={layer.layer_id}
                className={`panel nested-panel ${selectedLayer?.layer_id === layer.layer_id ? "row-selected" : ""}`.trim()}
                to={buildLayerParams(searchParams, layer.layer_id)}
              >
                <div className="row space-between">
                  <strong>{layer.title}</strong>
                  <Badge value={layer.failed_count === 0 ? "passing" : "failing"} />
                </div>
                <div className="muted">{layer.purpose}</div>
                <div className="muted">
                  Cases {formatNumber(layer.case_count)} · passed {formatNumber(layer.passed_count)} · failed {formatNumber(layer.failed_count)}
                </div>
              </Link>
            ))}
            {trustQuery.data == null ? <div className="empty">No trust summary loaded.</div> : null}
          </div>
        </Panel>

        <div className="stack">
          <Panel>
            <div className="row space-between">
              <div>
                <h2>{selectedLayer?.title ?? "Layer detail"}</h2>
                <div className="muted">{selectedLayer?.purpose ?? "Choose a conformance layer to inspect its evidence."}</div>
              </div>
              {selectedLayer ? (
                <a className="button ghost" href={selectedLayer.public_report_path}>
                  Raw JSON
                </a>
              ) : null}
            </div>
            {reportQuery.data ? (
              <table className="table">
                <thead>
                  <tr>
                    <th>Case</th>
                    <th>Status</th>
                    <th>Kind</th>
                    <th>Duration</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {reportQuery.data.results.map((result) => (
                    <tr key={result.id} className={selectedCase?.id === result.id ? "row-selected" : ""}>
                      <td>
                        <strong>{result.title}</strong>
                        <div className="muted">{result.summary ?? result.id}</div>
                      </td>
                      <td>
                        <Badge value={result.status} />
                      </td>
                      <td>{result.kind}</td>
                      <td>{formatNumber(result.duration_ms)} ms</td>
                      <td>
                        <Link className="button ghost" to={buildLayerParams(searchParams, reportQuery.data.layer_id, result.id)}>
                          Inspect
                        </Link>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <div className="empty">No conformance report loaded for the selected layer.</div>
            )}
          </Panel>

          <Panel>
            {selectedCase ? (
              <div className="stack">
                <div className="row space-between">
                  <div>
                    <h2>{selectedCase.title}</h2>
                    <div className="muted">{selectedCase.id}</div>
                  </div>
                  <Badge value={selectedCase.status} />
                </div>
                <div className="muted">{selectedCase.summary ?? "No summary recorded."}</div>
                <div className="grid two">
                  <Panel className="nested-panel">
                    <h3>Observed</h3>
                    <pre className="code">{JSON.stringify(selectedCase.observed ?? {}, null, 2)}</pre>
                  </Panel>
                  <Panel className="nested-panel">
                    <h3>Evidence</h3>
                    <div className="stack">
                      {selectedCase.evidence?.combined_excerpt ? <pre className="code">{selectedCase.evidence.combined_excerpt}</pre> : null}
                      {selectedCase.evidence?.stdout_excerpt ? <pre className="code">{selectedCase.evidence.stdout_excerpt}</pre> : null}
                      {selectedCase.evidence?.stderr_excerpt ? <pre className="code">{selectedCase.evidence.stderr_excerpt}</pre> : null}
                      {selectedCase.evidence?.source_files?.length ? (
                        <div className="muted">Source files {selectedCase.evidence.source_files.join(", ")}</div>
                      ) : null}
                      {selectedCase.evidence?.state_count != null ? (
                        <div className="muted">State count {formatNumber(selectedCase.evidence.state_count)}</div>
                      ) : null}
                      {selectedCase.evidence?.finding_count != null ? (
                        <div className="muted">Finding count {formatNumber(selectedCase.evidence.finding_count)}</div>
                      ) : null}
                      {selectedCase.evidence?.finding_features?.length ? (
                        <div className="muted">Finding features {selectedCase.evidence.finding_features.join(", ")}</div>
                      ) : null}
                    </div>
                  </Panel>
                </div>
                {selectedCase.error ? (
                  <Panel className="nested-panel">
                    <h3>Error</h3>
                    <pre className="code">{selectedCase.error}</pre>
                  </Panel>
                ) : null}
              </div>
            ) : (
              <div className="empty">Select a case to inspect observed output and evidence excerpts.</div>
            )}
          </Panel>

          <Panel>
            <h2>Milestone policy</h2>
            <div className="stack">
              <div className="muted">Trusted feature floor {trustQuery.data?.trusted_confidence_floor ?? "-"}</div>
              <div className="muted">Upgrade feature floor {trustQuery.data?.upgrade_confidence_floor ?? "-"}</div>
              {trustQuery.data?.support_matrix_public_path ? (
                <a className="button ghost" href={trustQuery.data.support_matrix_public_path}>
                  Derived support matrix
                </a>
              ) : null}
              <div className="muted">
                Promotion rule {(trustQuery.data?.promotion_requirements ?? []).map((item) => formatInlineValue(item)).join("; ") || "-"}
              </div>
            </div>
          </Panel>

          <Panel>
            <h2>Confidence derivation</h2>
            <div className="stack">
              {(trustQuery.data?.confidence_deltas ?? []).length > 0 ? (
                (trustQuery.data?.confidence_deltas ?? []).map((delta) => (
                  <div key={delta.feature} className="subtle-block">
                    <div className="row space-between">
                      <strong>{delta.label}</strong>
                      <Badge value={delta.confidence_status} />
                    </div>
                    <div className="muted">
                      declared {delta.declared_confidence_class} → derived {delta.derived_confidence_class}
                    </div>
                  </div>
                ))
              ) : (
                <div className="muted">Derived confidence matches the declared support matrix for the current evidence set.</div>
              )}
            </div>
          </Panel>
        </div>
      </div>
    </div>
  );
}
