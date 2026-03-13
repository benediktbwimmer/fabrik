import fs from "node:fs/promises";
import path from "node:path";

const root = "/Users/bene/code/fabrik";
const reportsDir = path.join(root, "target", "conformance-reports");
const publicReportsDir = path.join(root, "apps", "fabrik-console", "public", "conformance-reports");
const outputPath = path.join(publicReportsDir, "trust-summary.json");
const supportMatrixPath = path.join(root, "sdk", "typescript-compiler", "temporal-ts-subset-support-matrix.json");
const derivedSupportMatrixReportPath = path.join(reportsDir, "support-matrix-derived.json");
const derivedSupportMatrixOutputPath = path.join(publicReportsDir, "support-matrix-derived.json");
const reportFiles = [
  "layer-a-support.json",
  "layer-b-semantic.json",
  "layer-c-trust.json",
];

function summarizeFeatures(features) {
  const grouped = new Map();
  for (const feature of features) {
    const key = feature.confidence_class;
    const current = grouped.get(key) ?? [];
    current.push(feature.label);
    grouped.set(key, current);
  }
  return [...grouped.entries()].map(([confidence_class, labels]) => ({
    confidence_class,
    count: labels.length,
    features: labels.sort(),
  }));
}

async function readJson(filePath) {
  return JSON.parse(await fs.readFile(filePath, "utf8"));
}

function layerStatus(report) {
  if (!report) return "missing";
  return report.failed_count === 0 ? "passing" : "failing";
}

const confidenceRank = new Map([
  ["blocked", 0],
  ["supported", 1],
  ["supported_replay_validated", 2],
  ["supported_failover_validated", 3],
  ["supported_upgrade_validated", 4],
]);

function compareConfidence(left, right) {
  return (confidenceRank.get(left) ?? -1) - (confidenceRank.get(right) ?? -1);
}

function deriveConfidenceClass(feature, reportsByLayer) {
  if (feature.milestone_status === "blocked" || feature.support_level === "blocked") {
    return {
      confidence_class: "blocked",
      evidence: {
        support_layer_status: layerStatus(reportsByLayer.layer_a_support),
        semantic_layer_status: layerStatus(reportsByLayer.layer_b_semantic),
        trust_layer_status: layerStatus(reportsByLayer.layer_c_trust),
        upgrade_layer_status: layerStatus(reportsByLayer.layer_c_trust),
      },
    };
  }

  const supportLayerPassing = feature.support_fixtures && reportsByLayer.layer_a_support?.failed_count === 0;
  const semanticLayerPassing = feature.semantic_fixtures && reportsByLayer.layer_b_semantic?.failed_count === 0;
  const trustLayerPassing = feature.trust_fixtures && reportsByLayer.layer_c_trust?.failed_count === 0;
  const upgradeLayerPassing = feature.upgrade_fixtures && reportsByLayer.layer_c_trust?.failed_count === 0;

  let confidenceClass = "supported";
  if (feature.upgrade_fixtures && upgradeLayerPassing) {
    confidenceClass = "supported_upgrade_validated";
  } else if (feature.trust_fixtures && trustLayerPassing) {
    confidenceClass = "supported_failover_validated";
  } else if (feature.replay_validation && semanticLayerPassing) {
    confidenceClass = "supported_replay_validated";
  } else if (!supportLayerPassing && !semanticLayerPassing && !trustLayerPassing && !upgradeLayerPassing) {
    confidenceClass = "supported";
  }

  return {
    confidence_class: confidenceClass,
    evidence: {
      support_layer_status: supportLayerPassing ? "passing" : layerStatus(reportsByLayer.layer_a_support),
      semantic_layer_status: semanticLayerPassing ? "passing" : layerStatus(reportsByLayer.layer_b_semantic),
      trust_layer_status: trustLayerPassing ? "passing" : layerStatus(reportsByLayer.layer_c_trust),
      upgrade_layer_status: upgradeLayerPassing ? "passing" : layerStatus(reportsByLayer.layer_c_trust),
    },
  };
}

async function main() {
  const supportMatrixDocument = await readJson(supportMatrixPath);
  await fs.mkdir(reportsDir, { recursive: true });
  await fs.mkdir(publicReportsDir, { recursive: true });
  const reports = await Promise.all(
    reportFiles.map(async (fileName) => {
      const reportPath = path.join(reportsDir, fileName);
      const report = await readJson(reportPath);
      await fs.writeFile(
        path.join(publicReportsDir, fileName),
        JSON.stringify(report, null, 2),
      );
      const failedCases = (report.results ?? [])
        .filter((result) => result.status === "failed")
        .map((result) => ({
          id: result.id,
          title: result.title ?? result.id,
          summary: result.summary ?? result.error ?? "failed",
          href: `/conformance?layer=${encodeURIComponent(report.layer_id)}&case=${encodeURIComponent(result.id)}`,
        }));
      return {
        layer_id: report.layer_id,
        title: report.title,
        purpose: report.purpose,
        case_count: report.case_count,
        passed_count: report.passed_count,
        failed_count: report.failed_count,
        report_path: `target/conformance-reports/${fileName}`,
        public_report_path: `/conformance-reports/${fileName}`,
        failed_cases: failedCases,
      };
    }),
  );
  const reportsByLayer = Object.fromEntries(reports.map((report) => [report.layer_id, report]));
  const failed_count = reports.reduce((sum, report) => sum + report.failed_count, 0);
  const features = supportMatrixDocument.features.map((feature) => {
    const derived = deriveConfidenceClass(feature, reportsByLayer);
    let confidenceStatus = "declared_confirmed";
    if (compareConfidence(derived.confidence_class, feature.confidence_class) > 0) {
      confidenceStatus = "evidence_promoted";
    } else if (compareConfidence(derived.confidence_class, feature.confidence_class) < 0) {
      confidenceStatus = "evidence_demoted";
    }
    return {
      ...feature,
      declared_confidence_class: feature.confidence_class,
      confidence_class: derived.confidence_class,
      confidence_status: confidenceStatus,
      evidence: derived.evidence,
    };
  });
  const confidence_bands = summarizeFeatures(features);
  const headline_trusted_features = features
    .filter((feature) =>
      ["supported_failover_validated", "supported_upgrade_validated"].includes(feature.confidence_class),
    )
    .map((feature) => feature.label)
    .sort();
  const blocked_features = features
    .filter((feature) => feature.milestone_status === "blocked")
    .map((feature) => feature.label)
    .sort();
  const confidence_deltas = features
    .filter((feature) => feature.confidence_class !== feature.declared_confidence_class)
    .map((feature) => ({
      feature: feature.feature,
      label: feature.label,
      declared_confidence_class: feature.declared_confidence_class,
      derived_confidence_class: feature.confidence_class,
      confidence_status: feature.confidence_status,
    }))
    .sort((left, right) => left.label.localeCompare(right.label));

  const derivedSupportMatrix = {
    schema_version: 1,
    milestone_scope: supportMatrixDocument.milestone_scope,
    generated_at: new Date().toISOString(),
    reports: reports.map((report) => ({
      layer_id: report.layer_id,
      status: report.failed_count === 0 ? "passing" : "failing",
      case_count: report.case_count,
      failed_count: report.failed_count,
    })),
    features,
    confidence_deltas,
  };

  const summary = {
    schema_version: 1,
    milestone_scope: supportMatrixDocument.milestone_scope,
    title: "Temporal TS subset trust",
    goal: supportMatrixDocument.goal,
    generated_at: new Date().toISOString(),
    status: failed_count === 0 ? "passing" : "failing",
    trusted_confidence_floor: supportMatrixDocument.trusted_confidence_floor,
    upgrade_confidence_floor: supportMatrixDocument.upgrade_confidence_floor,
    reports,
    confidence_bands,
    headline_trusted_features,
    blocked_features,
    promotion_requirements: supportMatrixDocument.promotion_requirements,
    confidence_deltas,
    features,
    support_matrix_public_path: "/conformance-reports/support-matrix-derived.json",
  };

  await fs.writeFile(derivedSupportMatrixReportPath, JSON.stringify(derivedSupportMatrix, null, 2));
  await fs.writeFile(derivedSupportMatrixOutputPath, JSON.stringify(derivedSupportMatrix, null, 2));
  await fs.writeFile(outputPath, JSON.stringify(summary, null, 2));
  process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
