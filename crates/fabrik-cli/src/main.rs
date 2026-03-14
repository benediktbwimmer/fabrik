use std::{
    collections::BTreeSet,
    env,
    ffi::OsString,
    fs::{self, File},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use anyhow::{Context, Result, bail};
use fabrik_workflow::CompiledWorkflowArtifact;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::{Duration, sleep};

const MIGRATION_REPORT_SCHEMA_VERSION: u32 = 1;
const REPO_ROOT: &str = "/Users/bene/code/fabrik";

#[derive(Debug, Clone)]
struct CliArgs {
    project_root: PathBuf,
    deploy: bool,
    analyze_only: bool,
    output_dir: Option<PathBuf>,
    validation_run_limit: usize,
    api_url: Option<String>,
    tenant_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MigrationStatus {
    CompatibleDeployed,
    CompatibleReadyNotDeployed,
    IncompatibleBlocked,
    AnalysisFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum AlphaQualificationVerdict {
    Qualified,
    QualifiedWithCaveats,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum QualificationBlockerCategory {
    UnsupportedApi,
    UnsupportedPackagingBootstrap,
    UnsupportedVisibilitySearchUsage,
    ReplayTrustBlocker,
    OperationalBlocker,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QualificationBlockerGroup {
    category: QualificationBlockerCategory,
    count: usize,
    reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AlphaQualificationReport {
    verdict: AlphaQualificationVerdict,
    summary: String,
    blocker_categories: Vec<QualificationBlockerGroup>,
    caveats: Vec<String>,
    next_steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum ConfidenceClass {
    SourceCompatible,
    DeployCompatible,
    ReplayValidated,
    ProductionReady,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FindingSeverity {
    HardBlock,
    Warning,
    Info,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SupportLevel {
    Native,
    Adapter,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnalyzerOutput {
    schema_version: u32,
    project_root: String,
    support_matrix_meta: Value,
    support_matrix: Vec<SupportMatrixEntry>,
    files: Vec<AnalyzedFile>,
    workflows: Vec<DiscoveredWorkflow>,
    workers: Vec<DiscoveredWorker>,
    findings: Vec<AnalyzerFinding>,
    summary: AnalyzerSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SupportMatrixEntry {
    feature: String,
    label: String,
    milestone_status: String,
    support_level: SupportLevel,
    confidence_class: String,
    replay_validation: bool,
    deploy_validation: bool,
    support_fixtures: bool,
    semantic_fixtures: bool,
    trust_fixtures: bool,
    upgrade_fixtures: bool,
    semantic_caveats: Vec<String>,
    blocking_severity: FindingSeverity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnalyzedFile {
    path: String,
    temporal_imports: Vec<String>,
    exported_workflows: Vec<String>,
    uses: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiscoveredWorkflow {
    file: String,
    export_name: String,
    definition_id_suggestion: String,
    uses: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiscoveredWorker {
    file: String,
    worker_index: usize,
    task_queue: Option<String>,
    build_id: Option<String>,
    workflows_path: Option<String>,
    activities_reference: Option<String>,
    activity_module: Option<String>,
    activity_factory_export: Option<String>,
    activity_factory_args_js: Vec<String>,
    activity_runtime_helper: bool,
    data_converter_mode: Option<String>,
    payload_converter_module: Option<String>,
    bootstrap_pattern: String,
    uses: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnalyzerFinding {
    code: String,
    severity: FindingSeverity,
    feature: String,
    file: String,
    line: Option<u32>,
    column: Option<u32>,
    symbol: Option<String>,
    message: String,
    remediation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnalyzerSummary {
    workflow_count: usize,
    worker_count: usize,
    hard_block_count: usize,
    warning_count: usize,
    info_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompiledArtifactRecord {
    definition_id: String,
    definition_version: u32,
    artifact_hash: String,
    entry: ArtifactEntrypoint,
    artifact_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArtifactEntrypoint {
    module: String,
    export: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkflowMigrationRecord {
    file: String,
    export_name: String,
    definition_id: String,
    definition_version: u32,
    confidence_class: Option<ConfidenceClass>,
    status: String,
    artifact_path: Option<String>,
    artifact_hash: Option<String>,
    compile_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkerPackageRecord {
    worker_file: String,
    worker_index: usize,
    task_queue: Option<String>,
    build_id: String,
    package_dir: String,
    dist_dir: String,
    manifest_path: String,
    bootstrap_path: String,
    resolved_activity_module_path: Option<String>,
    resolved_activity_runtime_helper_path: Option<String>,
    data_converter_mode: Option<String>,
    resolved_payload_converter_module_path: Option<String>,
    log_path: String,
    pid_path: String,
    package_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GateResult {
    status: String,
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArtifactValidationResponse {
    compatible: bool,
    status: String,
    validation: ArtifactValidationSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArtifactValidationSummary {
    enabled: bool,
    validated_run_count: usize,
    skipped_run_count: usize,
    failed_run_count: usize,
    failures: Vec<ArtifactValidationFailure>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArtifactValidationFailure {
    instance_id: String,
    run_id: String,
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArtifactValidationRecord {
    definition_id: String,
    artifact_hash: String,
    compatible: bool,
    status: String,
    validation: ArtifactValidationSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeployArtifactRecord {
    definition_id: String,
    artifact_hash: String,
    status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskQueueInspection {
    task_queue: String,
    pollers: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeployWorkerRecord {
    task_queue: String,
    build_id: String,
    registered: bool,
    poller_count: usize,
    pid: Option<u32>,
    log_path: Option<String>,
    status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ValidationSummary {
    source_compatibility_preflight: GateResult,
    workflow_compile_validation: GateResult,
    worker_packaging_validation: GateResult,
    payload_data_converter_validation: GateResult,
    visibility_search_validation: GateResult,
    replay_rollout_validation: GateResult,
    deploy_validation: GateResult,
    replay_artifacts: Vec<ArtifactValidationRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeploymentSummary {
    requested: bool,
    api_url: Option<String>,
    tenant_id: Option<String>,
    published_artifacts: Vec<DeployArtifactRecord>,
    workers: Vec<DeployWorkerRecord>,
    status: String,
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MigrationReport {
    schema_version: u32,
    milestone_scope: String,
    source_path: String,
    repo_fingerprint: String,
    sdk_target: String,
    status: MigrationStatus,
    alpha_qualification: AlphaQualificationReport,
    discovered: Value,
    support_matrix: Vec<SupportMatrixEntry>,
    findings: Vec<AnalyzerFinding>,
    generated_artifacts: Vec<Value>,
    compiled_workflows: Vec<WorkflowMigrationRecord>,
    worker_packages: Vec<WorkerPackageRecord>,
    validation: ValidationSummary,
    deployment: DeploymentSummary,
    trust: Value,
}

#[tokio::main]
async fn main() {
    match try_main().await {
        Ok(status) => {
            std::process::exit(status);
        }
        Err(error) => {
            eprintln!("{error:#}");
            std::process::exit(1);
        }
    }
}

async fn try_main() -> Result<i32> {
    let args = parse_args(env::args().skip(1).collect())?;
    let output_dir =
        args.output_dir.clone().unwrap_or_else(|| args.project_root.join(".fabrik-migration"));
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create output directory {}", output_dir.display()))?;
    let workspace_dir = output_dir.join("workspace");
    let artifacts_dir = output_dir.join("artifacts");
    let workers_dir = output_dir.join("workers");
    fs::create_dir_all(&workspace_dir)?;
    fs::create_dir_all(&artifacts_dir)?;
    fs::create_dir_all(&workers_dir)?;

    let analyzer = run_analyzer(&args.project_root)?;
    let fingerprint = repo_fingerprint(&args.project_root, &analyzer.files)?;
    let equivalence_contract =
        load_repo_json("sdk/typescript-compiler/temporal-ts-equivalence-contract.json")?;
    let conformance_manifest =
        load_repo_json("sdk/typescript-compiler/temporal-ts-conformance-manifest.json")?;
    let support_summary = build_support_summary(&analyzer.support_matrix);
    let trust_contracts = json!([
        {
            "name": "Temporal TypeScript equivalence contract",
            "path": format!("{REPO_ROOT}/docs/temporal-typescript-equivalence-contract.md"),
            "summary": "Defines the required and non-required axes for same behavior and same replay outcome."
        },
        {
            "name": "Durability and replay contract",
            "path": format!("{REPO_ROOT}/docs/durability-and-replay-contract.md"),
            "summary": "Defines the authoritative recovery model, replay inputs, and backup/restore expectations."
        },
        {
            "name": "Ownership and fencing contract",
            "path": format!("{REPO_ROOT}/docs/ownership-and-fencing-contract.md"),
            "summary": "Defines owner transition, duplicate delivery, stale delivery, and mixed-build fencing rules."
        }
    ]);

    write_json(
        &workspace_dir.join("support-matrix.json"),
        &json!({
            "support_matrix_meta": &analyzer.support_matrix_meta,
            "support_matrix": &analyzer.support_matrix,
        }),
    )?;
    write_json(
        &workspace_dir.join("analysis.json"),
        &serde_json::to_value(&analyzer).expect("analysis serializes"),
    )?;
    write_json(&workspace_dir.join("equivalence-contract.json"), &equivalence_contract)?;
    write_json(&workspace_dir.join("conformance-manifest.json"), &conformance_manifest)?;
    write_json(&workspace_dir.join("trust-summary.json"), &support_summary)?;

    let hard_blocks = analyzer
        .findings
        .iter()
        .filter(|finding| matches!(finding.severity, FindingSeverity::HardBlock))
        .count();
    let payload_blocks = analyzer.findings.iter().any(|finding| {
        matches!(finding.severity, FindingSeverity::HardBlock)
            && finding.feature == "payload_data_converter_usage"
    });
    let visibility_blocks = analyzer.findings.iter().any(|finding| {
        matches!(finding.severity, FindingSeverity::HardBlock)
            && finding.feature == "visibility_search_usage"
    });

    let mut generated_artifacts = vec![
        json!({ "kind": "analysis", "path": workspace_dir.join("analysis.json").display().to_string() }),
        json!({ "kind": "support_matrix", "path": workspace_dir.join("support-matrix.json").display().to_string() }),
        json!({ "kind": "equivalence_contract", "path": workspace_dir.join("equivalence-contract.json").display().to_string() }),
        json!({ "kind": "conformance_manifest", "path": workspace_dir.join("conformance-manifest.json").display().to_string() }),
        json!({ "kind": "trust_summary", "path": workspace_dir.join("trust-summary.json").display().to_string() }),
    ];

    let mut workflow_records = Vec::new();
    let mut compiled_artifacts = Vec::new();
    let client = Client::new();

    if hard_blocks == 0 && !args.analyze_only {
        for workflow in &analyzer.workflows {
            let definition_version = determine_definition_version(
                &client,
                args.api_url.as_deref(),
                args.tenant_id.as_deref(),
                &workflow.definition_id_suggestion,
                args.deploy,
            )
            .await
            .unwrap_or(1);
            match compile_workflow(&args.project_root, &artifacts_dir, workflow, definition_version)
            {
                Ok(record) => {
                    generated_artifacts.push(json!({
                        "kind": "compiled_workflow_artifact",
                        "definition_id": record.definition_id,
                        "path": record.artifact_path,
                    }));
                    workflow_records.push(WorkflowMigrationRecord {
                        file: workflow.file.clone(),
                        export_name: workflow.export_name.clone(),
                        definition_id: record.definition_id.clone(),
                        definition_version: record.definition_version,
                        confidence_class: Some(ConfidenceClass::SourceCompatible),
                        status: "compiled".to_owned(),
                        artifact_path: Some(record.artifact_path.clone()),
                        artifact_hash: Some(record.artifact_hash.clone()),
                        compile_error: None,
                    });
                    compiled_artifacts.push(record);
                }
                Err(error) => {
                    workflow_records.push(WorkflowMigrationRecord {
                        file: workflow.file.clone(),
                        export_name: workflow.export_name.clone(),
                        definition_id: workflow.definition_id_suggestion.clone(),
                        definition_version,
                        confidence_class: None,
                        status: "compile_failed".to_owned(),
                        artifact_path: None,
                        artifact_hash: None,
                        compile_error: Some(error.to_string()),
                    });
                }
            }
        }
    } else {
        workflow_records.extend(analyzer.workflows.iter().map(|workflow| {
            WorkflowMigrationRecord {
                file: workflow.file.clone(),
                export_name: workflow.export_name.clone(),
                definition_id: workflow.definition_id_suggestion.clone(),
                definition_version: 1,
                confidence_class: None,
                status: if args.analyze_only {
                    "analysis_only".to_owned()
                } else {
                    "blocked".to_owned()
                },
                artifact_path: None,
                artifact_hash: None,
                compile_error: None,
            }
        }));
    }

    let package_manifest =
        write_deploy_manifest(&workspace_dir, &compiled_artifacts, &analyzer.workers)?;
    generated_artifacts.push(json!({
        "kind": "deploy_manifest",
        "path": package_manifest.display().to_string(),
    }));

    let worker_packages =
        build_worker_packages(&args.project_root, &workers_dir, &analyzer.workers)?;
    generated_artifacts.extend(worker_packages.iter().flat_map(|worker| {
        [
            json!({ "kind": "worker_package", "path": worker.package_dir }),
            json!({ "kind": "worker_manifest", "path": worker.manifest_path }),
            json!({ "kind": "worker_bootstrap", "path": worker.bootstrap_path }),
        ]
    }));

    let compile_failed = workflow_records.iter().any(|record| record.status == "compile_failed");
    let packaging_failed = worker_packages.iter().any(|record| record.package_status != "packaged");

    let replay_artifacts = if hard_blocks == 0
        && !compile_failed
        && args.api_url.is_some()
        && args.tenant_id.is_some()
        && !args.analyze_only
    {
        validate_artifacts(
            &client,
            args.api_url.as_deref().expect("api url gated"),
            args.tenant_id.as_deref().expect("tenant gated"),
            &compiled_artifacts,
            args.validation_run_limit,
        )
        .await?
    } else {
        Vec::new()
    };

    let replay_blocked = replay_artifacts.iter().any(|record| !record.compatible);
    let replay_validated = !replay_artifacts.is_empty() && !replay_blocked;

    let mut deployment = DeploymentSummary {
        requested: args.deploy,
        api_url: args.api_url.clone(),
        tenant_id: args.tenant_id.clone(),
        published_artifacts: Vec::new(),
        workers: Vec::new(),
        status: "not_requested".to_owned(),
        message: "deployment was not requested".to_owned(),
    };

    let deploy_requested_but_unconfigured =
        args.deploy && (args.api_url.is_none() || args.tenant_id.is_none());
    if deploy_requested_but_unconfigured {
        deployment.status = "failed".to_owned();
        deployment.message =
            "--deploy requires both --api-url (or FABRIK_API_URL) and --tenant (or FABRIK_TENANT_ID)"
                .to_owned();
    } else if args.deploy
        && hard_blocks == 0
        && !compile_failed
        && !packaging_failed
        && !replay_blocked
    {
        deployment = deploy_bundle(
            &client,
            args.api_url.as_deref().expect("deploy gated"),
            args.tenant_id.as_deref().expect("deploy gated"),
            &compiled_artifacts,
            &worker_packages,
        )
        .await?;
    }

    for record in &mut workflow_records {
        if record.status != "compiled" {
            continue;
        }
        record.confidence_class = Some(ConfidenceClass::SourceCompatible);
        if !packaging_failed && !payload_blocks && !visibility_blocks {
            record.confidence_class = Some(ConfidenceClass::DeployCompatible);
        }
        if replay_validated {
            record.confidence_class = Some(ConfidenceClass::ReplayValidated);
        }
        if deployment.status == "deployed" {
            record.confidence_class = Some(ConfidenceClass::ProductionReady);
        }
    }

    let status = if deploy_requested_but_unconfigured {
        MigrationStatus::AnalysisFailed
    } else if hard_blocks > 0 || compile_failed || packaging_failed || replay_blocked {
        MigrationStatus::IncompatibleBlocked
    } else if deployment.status == "deployed" {
        MigrationStatus::CompatibleDeployed
    } else {
        MigrationStatus::CompatibleReadyNotDeployed
    };

    let validation = ValidationSummary {
        source_compatibility_preflight: GateResult {
            status: if hard_blocks > 0 { "blocked".to_owned() } else { "passed".to_owned() },
            message: format!(
                "discovered {} workflows, {} workers, {} hard blocks",
                analyzer.summary.workflow_count, analyzer.summary.worker_count, hard_blocks
            ),
        },
        workflow_compile_validation: GateResult {
            status: if args.analyze_only {
                "skipped".to_owned()
            } else if compile_failed {
                "blocked".to_owned()
            } else {
                "passed".to_owned()
            },
            message: format!(
                "{} compiled artifact(s), {} compile failure(s)",
                compiled_artifacts.len(),
                workflow_records.iter().filter(|record| record.status == "compile_failed").count()
            ),
        },
        worker_packaging_validation: GateResult {
            status: if packaging_failed { "blocked".to_owned() } else { "passed".to_owned() },
            message: format!("{} worker package(s) prepared", worker_packages.len()),
        },
        payload_data_converter_validation: GateResult {
            status: if payload_blocks { "blocked".to_owned() } else { "passed".to_owned() },
            message: if payload_blocks {
                "payload/data converter usage exceeded the supported static adapter subset"
                    .to_owned()
            } else {
                "no unsupported payload/data converter blockers detected".to_owned()
            },
        },
        visibility_search_validation: GateResult {
            status: if visibility_blocks { "blocked".to_owned() } else { "passed".to_owned() },
            message: if visibility_blocks {
                "visibility/search dependent constructs were detected and currently block migration"
                    .to_owned()
            } else {
                "no blocking visibility/search usage detected".to_owned()
            },
        },
        replay_rollout_validation: GateResult {
            status: if args.analyze_only || hard_blocks > 0 || compile_failed {
                "skipped".to_owned()
            } else if replay_artifacts.is_empty() {
                "warning".to_owned()
            } else if replay_blocked {
                "blocked".to_owned()
            } else {
                "passed".to_owned()
            },
            message: if replay_artifacts.is_empty() {
                "no Fabrik-side histories were available for replay validation".to_owned()
            } else {
                format!("validated {} artifact candidate(s)", replay_artifacts.len())
            },
        },
        deploy_validation: GateResult {
            status: match deployment.status.as_str() {
                "deployed" => "passed".to_owned(),
                "failed" => "blocked".to_owned(),
                _ if args.deploy => "warning".to_owned(),
                _ => "skipped".to_owned(),
            },
            message: deployment.message.clone(),
        },
        replay_artifacts,
    };

    let alpha_qualification = build_alpha_qualification(
        &analyzer.support_matrix,
        &analyzer.findings,
        &workflow_records,
        &worker_packages,
        &validation,
        &deployment,
        args.deploy,
    );
    write_json(
        &workspace_dir.join("alpha-qualification.json"),
        &serde_json::to_value(&alpha_qualification).expect("alpha qualification serializes"),
    )?;
    generated_artifacts.push(json!({
        "kind": "alpha_qualification",
        "path": workspace_dir.join("alpha-qualification.json").display().to_string(),
    }));

    let report = MigrationReport {
        schema_version: MIGRATION_REPORT_SCHEMA_VERSION,
        milestone_scope: analyzer
            .support_matrix_meta
            .get("milestone_scope")
            .and_then(Value::as_str)
            .unwrap_or("temporal_ts_subset_trust")
            .to_owned(),
        source_path: args.project_root.display().to_string(),
        repo_fingerprint: fingerprint,
        sdk_target: "temporal_typescript".to_owned(),
        status,
        alpha_qualification,
        discovered: json!({
            "files": analyzer.files,
            "workflows": analyzer.workflows,
            "workers": analyzer.workers,
        }),
        support_matrix: analyzer.support_matrix,
        findings: analyzer.findings,
        generated_artifacts,
        compiled_workflows: workflow_records,
        worker_packages,
        validation,
        deployment,
        trust: json!({
            "goal": analyzer.support_matrix_meta.get("goal").cloned().unwrap_or(Value::Null),
            "trusted_confidence_floor": analyzer.support_matrix_meta.get("trusted_confidence_floor").cloned().unwrap_or(Value::Null),
            "upgrade_confidence_floor": analyzer.support_matrix_meta.get("upgrade_confidence_floor").cloned().unwrap_or(Value::Null),
            "promotion_requirements": analyzer.support_matrix_meta.get("promotion_requirements").cloned().unwrap_or(Value::Array(Vec::new())),
            "support_summary": support_summary,
            "equivalence_contract": equivalence_contract,
            "conformance_manifest": conformance_manifest,
            "contract_documents": trust_contracts,
        }),
    };

    write_json(
        &output_dir.join("migration-report.json"),
        &serde_json::to_value(&report).expect("report serializes"),
    )?;
    fs::write(output_dir.join("migration-report.md"), render_markdown_report(&report))
        .context("failed to write migration markdown report")?;

    Ok(match report.status {
        MigrationStatus::CompatibleDeployed | MigrationStatus::CompatibleReadyNotDeployed => 0,
        MigrationStatus::IncompatibleBlocked | MigrationStatus::AnalysisFailed => 2,
    })
}

fn parse_args(args: Vec<String>) -> Result<CliArgs> {
    if args.len() < 4 || args[0] != "migrate" || args[1] != "temporal" {
        bail!(
            "usage: fabrik migrate temporal <local_repo_path> [--deploy] [--analyze-only] [--output-dir <dir>] [--validation-run-limit <n>] [--api-url <url>] [--tenant <tenant_id>]"
        );
    }

    let project_root = PathBuf::from(&args[2]);
    let mut deploy = false;
    let mut analyze_only = false;
    let mut output_dir = None;
    let mut validation_run_limit = 10_usize;
    let mut api_url = env::var("FABRIK_API_URL").ok();
    let mut tenant_id = env::var("FABRIK_TENANT_ID").ok();

    let mut index = 3;
    while index < args.len() {
        match args[index].as_str() {
            "--deploy" => {
                deploy = true;
                index += 1;
            }
            "--analyze-only" => {
                analyze_only = true;
                index += 1;
            }
            "--output-dir" => {
                let value = args.get(index + 1).context("--output-dir requires a value")?;
                output_dir = Some(PathBuf::from(value));
                index += 2;
            }
            "--validation-run-limit" => {
                let value = args
                    .get(index + 1)
                    .context("--validation-run-limit requires a value")?
                    .parse::<usize>()
                    .context("--validation-run-limit must be an integer")?;
                validation_run_limit = value.max(1);
                index += 2;
            }
            "--api-url" => {
                api_url = Some(args.get(index + 1).context("--api-url requires a value")?.clone());
                index += 2;
            }
            "--tenant" => {
                tenant_id = Some(args.get(index + 1).context("--tenant requires a value")?.clone());
                index += 2;
            }
            flag => bail!("unknown flag {flag}"),
        }
    }

    Ok(CliArgs {
        project_root: fs::canonicalize(&project_root)
            .with_context(|| format!("failed to resolve {}", project_root.display()))?,
        deploy,
        analyze_only,
        output_dir,
        validation_run_limit,
        api_url,
        tenant_id,
    })
}

fn run_analyzer(project_root: &Path) -> Result<AnalyzerOutput> {
    let output = Command::new("node")
        .arg("sdk/typescript-compiler/migration-analyzer.mjs")
        .arg("--project")
        .arg(project_root)
        .current_dir(REPO_ROOT)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("failed to spawn Temporal TypeScript analyzer")?;
    if !output.status.success() {
        bail!("temporal analyzer failed: {}", String::from_utf8_lossy(&output.stderr).trim());
    }
    serde_json::from_slice(&output.stdout).context("failed to decode analyzer output")
}

fn load_repo_json(relative_path: &str) -> Result<Value> {
    let path = Path::new(REPO_ROOT).join(relative_path);
    let payload =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&payload).with_context(|| format!("failed to decode {}", path.display()))
}

fn build_support_summary(support_matrix: &[SupportMatrixEntry]) -> Value {
    let mut by_confidence = BTreeSet::new();
    let mut confidence_counts = serde_json::Map::new();
    let mut supported_feature_count = 0_u64;
    let mut blocked_feature_count = 0_u64;
    let mut headline_trusted_feature_count = 0_u64;
    let mut upgrade_validated_feature_count = 0_u64;

    for entry in support_matrix {
        by_confidence.insert(entry.confidence_class.clone());
        if entry.milestone_status == "supported" {
            supported_feature_count += 1;
        } else if entry.milestone_status == "blocked" {
            blocked_feature_count += 1;
        }
        if matches!(
            entry.confidence_class.as_str(),
            "supported_failover_validated" | "supported_upgrade_validated"
        ) {
            headline_trusted_feature_count += 1;
        }
        if entry.confidence_class == "supported_upgrade_validated" {
            upgrade_validated_feature_count += 1;
        }
    }

    for confidence in by_confidence {
        let count =
            support_matrix.iter().filter(|entry| entry.confidence_class == confidence).count();
        confidence_counts.insert(confidence, json!(count));
    }

    json!({
        "supported_feature_count": supported_feature_count,
        "blocked_feature_count": blocked_feature_count,
        "headline_trusted_feature_count": headline_trusted_feature_count,
        "upgrade_validated_feature_count": upgrade_validated_feature_count,
        "by_confidence": Value::Object(confidence_counts),
    })
}

fn build_alpha_qualification(
    support_matrix: &[SupportMatrixEntry],
    findings: &[AnalyzerFinding],
    workflow_records: &[WorkflowMigrationRecord],
    worker_packages: &[WorkerPackageRecord],
    validation: &ValidationSummary,
    deployment: &DeploymentSummary,
    deploy_requested: bool,
) -> AlphaQualificationReport {
    use std::collections::{BTreeMap, BTreeSet};

    let mut grouped = BTreeMap::<QualificationBlockerCategory, BTreeSet<String>>::new();
    let label_by_feature = support_matrix
        .iter()
        .map(|entry| (entry.feature.as_str(), entry.label.as_str()))
        .collect::<BTreeMap<_, _>>();

    for finding in
        findings.iter().filter(|finding| matches!(finding.severity, FindingSeverity::HardBlock))
    {
        let category = qualification_category_for_feature(&finding.feature);
        let label = label_by_feature
            .get(finding.feature.as_str())
            .copied()
            .unwrap_or(finding.feature.as_str());
        grouped.entry(category).or_default().insert(format!("{label}: {}", finding.message));
    }

    if workflow_records.iter().any(|record| record.status == "compile_failed") {
        grouped.entry(QualificationBlockerCategory::UnsupportedApi).or_default().insert(
            "One or more workflows failed to compile into the currently supported Fabrik subset."
                .to_owned(),
        );
    }

    if worker_packages.iter().any(|record| record.package_status != "packaged") {
        grouped
            .entry(QualificationBlockerCategory::UnsupportedPackagingBootstrap)
            .or_default()
            .insert(
                "One or more worker packages could not be prepared from the discovered bootstrap shape."
                    .to_owned(),
            );
    }

    if validation.replay_rollout_validation.status == "blocked" {
        grouped
            .entry(QualificationBlockerCategory::ReplayTrustBlocker)
            .or_default()
            .insert(validation.replay_rollout_validation.message.clone());
    }

    if deployment.status == "failed" {
        grouped
            .entry(QualificationBlockerCategory::OperationalBlocker)
            .or_default()
            .insert(deployment.message.clone());
    }

    let blocker_categories = grouped
        .into_iter()
        .map(|(category, reasons)| QualificationBlockerGroup {
            category,
            count: reasons.len(),
            reasons: reasons.into_iter().collect(),
        })
        .collect::<Vec<_>>();

    let warning_count = findings
        .iter()
        .filter(|finding| matches!(finding.severity, FindingSeverity::Warning))
        .count();
    let info_count =
        findings.iter().filter(|finding| matches!(finding.severity, FindingSeverity::Info)).count();

    let mut caveats = Vec::new();
    if validation.replay_rollout_validation.status != "passed" {
        caveats.push(validation.replay_rollout_validation.message.clone());
    }
    if !deploy_requested && deployment.status == "not_requested" {
        caveats.push("Deployment was not requested, so poller registration and runtime health remain unproven.".to_owned());
    }
    if warning_count > 0 {
        caveats.push(format!("{warning_count} warning finding(s) remain and should be reviewed before design-partner rollout."));
    }
    if info_count > 0 {
        caveats.push(format!(
            "{info_count} informational finding(s) were emitted for operator review."
        ));
    }

    let verdict = if !blocker_categories.is_empty() {
        AlphaQualificationVerdict::Blocked
    } else if !caveats.is_empty() {
        AlphaQualificationVerdict::QualifiedWithCaveats
    } else {
        AlphaQualificationVerdict::Qualified
    };

    let summary = match verdict {
        AlphaQualificationVerdict::Qualified => {
            "This repo fits the currently supported alpha subset and passed the available migration gates."
                .to_owned()
        }
        AlphaQualificationVerdict::QualifiedWithCaveats => {
            "This repo fits the current alpha subset, but additional trust or deployment evidence is still required."
                .to_owned()
        }
        AlphaQualificationVerdict::Blocked => {
            "This repo is blocked for the current alpha subset until the grouped blockers below are addressed."
                .to_owned()
        }
    };

    let mut next_steps = Vec::new();
    match verdict {
        AlphaQualificationVerdict::Qualified => {
            next_steps.push(
                "Run package/deploy validation against the target Fabrik environment and capture operator evidence."
                    .to_owned(),
            );
            next_steps.push(
                "Record this repo as a candidate primary or shadow alpha workload and keep the generated artifacts with the engagement."
                    .to_owned(),
            );
        }
        AlphaQualificationVerdict::QualifiedWithCaveats => {
            if validation.replay_rollout_validation.status != "passed" {
                next_steps.push(
                    "Capture Fabrik-side histories for the migrated workflow and rerun replay/rollout validation."
                        .to_owned(),
                );
            }
            if !deploy_requested {
                next_steps.push(
                    "Run the migration with --deploy (or equivalent internal deployment flow) to prove poller presence and runtime health."
                        .to_owned(),
                );
            }
            next_steps.push(
                "Review warning findings and decide whether each one is acceptable for the design-partner alpha boundary."
                    .to_owned(),
            );
        }
        AlphaQualificationVerdict::Blocked => {
            for group in &blocker_categories {
                next_steps.push(match group.category {
                    QualificationBlockerCategory::UnsupportedApi => {
                        "Reduce unsupported workflow/runtime API usage or extend the supported compiler and migration subset before re-running qualification."
                            .to_owned()
                    }
                    QualificationBlockerCategory::UnsupportedPackagingBootstrap => {
                        "Rewrite worker bootstrap and packaging inputs to the supported static shape before re-running qualification."
                            .to_owned()
                    }
                    QualificationBlockerCategory::UnsupportedVisibilitySearchUsage => {
                        "Remove or defer search/memo-dependent behavior until the alpha visibility slice is implemented."
                            .to_owned()
                    }
                    QualificationBlockerCategory::ReplayTrustBlocker => {
                        "Resolve replay divergence or rollout incompatibility before promoting this repo into the trusted alpha subset."
                            .to_owned()
                    }
                    QualificationBlockerCategory::OperationalBlocker => {
                        "Fix deployment or operator-surface issues before treating this repo as alpha-ready."
                            .to_owned()
                    }
                });
            }
            next_steps.sort();
            next_steps.dedup();
        }
    }

    AlphaQualificationReport { verdict, summary, blocker_categories, caveats, next_steps }
}

fn qualification_category_for_feature(feature: &str) -> QualificationBlockerCategory {
    match feature {
        "visibility_search_usage" => QualificationBlockerCategory::UnsupportedVisibilitySearchUsage,
        "worker_bootstrap_patterns" => QualificationBlockerCategory::UnsupportedPackagingBootstrap,
        "unsupported_temporal_api" | "payload_data_converter_usage" | "interceptors_middleware" => {
            QualificationBlockerCategory::UnsupportedApi
        }
        _ => QualificationBlockerCategory::UnsupportedApi,
    }
}

fn repo_fingerprint(project_root: &Path, files: &[AnalyzedFile]) -> Result<String> {
    let mut digest = Sha256::new();
    let mut unique_paths = BTreeSet::new();
    for file in files {
        unique_paths.insert(project_root.join(&file.path));
    }
    let package_json = project_root.join("package.json");
    if package_json.exists() {
        unique_paths.insert(package_json);
    }
    let tsconfig = project_root.join("tsconfig.json");
    if tsconfig.exists() {
        unique_paths.insert(tsconfig);
    }
    for file in unique_paths {
        digest.update(file.strip_prefix(project_root).unwrap_or(&file).display().to_string());
        let bytes = fs::read(&file)
            .with_context(|| format!("failed to read fingerprint input {}", file.display()))?;
        digest.update(bytes);
    }
    Ok(format!("{:x}", digest.finalize()))
}

async fn determine_definition_version(
    client: &Client,
    api_url: Option<&str>,
    tenant_id: Option<&str>,
    definition_id: &str,
    deploy: bool,
) -> Result<u32> {
    if !deploy {
        return Ok(1);
    }
    let Some(api_url) = api_url else {
        return Ok(1);
    };
    let Some(tenant_id) = tenant_id else {
        return Ok(1);
    };
    let url = format!("{api_url}/tenants/{tenant_id}/workflow-artifacts/{definition_id}/latest");
    let response = client.get(&url).send().await?;
    if response.status() == StatusCode::NOT_FOUND {
        return Ok(1);
    }
    if !response.status().is_success() {
        return Ok(1);
    }
    let payload: Value = response.json().await?;
    Ok(payload
        .get("definition_version")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or(0)
        .saturating_add(1)
        .max(1))
}

fn compile_workflow(
    project_root: &Path,
    artifacts_dir: &Path,
    workflow: &DiscoveredWorkflow,
    definition_version: u32,
) -> Result<CompiledArtifactRecord> {
    let artifact_path = artifacts_dir
        .join(format!("{}-v{definition_version}.json", workflow.definition_id_suggestion));
    let output = Command::new("node")
        .arg("sdk/typescript-compiler/compiler.mjs")
        .arg("--entry")
        .arg(project_root.join(&workflow.file))
        .arg("--export")
        .arg(&workflow.export_name)
        .arg("--definition-id")
        .arg(&workflow.definition_id_suggestion)
        .arg("--version")
        .arg(definition_version.to_string())
        .arg("--out")
        .arg(&artifact_path)
        .current_dir("/Users/bene/code/fabrik")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| {
            format!("failed to spawn compiler for {}:{}", workflow.file, workflow.export_name)
        })?;
    if !output.status.success() {
        bail!("{}", String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    let mut artifact: CompiledWorkflowArtifact = serde_json::from_slice(
        &fs::read(&artifact_path)
            .with_context(|| format!("failed to read {}", artifact_path.display()))?,
    )
    .context("failed to decode compiled artifact")?;
    let artifact_hash = artifact.hash();
    artifact.artifact_hash = artifact_hash.clone();
    write_json(&artifact_path, &serde_json::to_value(&artifact)?)?;
    Ok(CompiledArtifactRecord {
        definition_id: workflow.definition_id_suggestion.clone(),
        definition_version,
        artifact_hash,
        entry: ArtifactEntrypoint {
            module: workflow.file.clone(),
            export: workflow.export_name.clone(),
        },
        artifact_path: artifact_path.display().to_string(),
    })
}

fn build_worker_packages(
    project_root: &Path,
    workers_dir: &Path,
    workers: &[DiscoveredWorker],
) -> Result<Vec<WorkerPackageRecord>> {
    let mut packages = Vec::new();
    let package_workers = preferred_packaged_workers(workers);
    for worker in package_workers {
        let package_name = format!(
            "{}-{}",
            sanitize_segment(
                Path::new(&worker.file)
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .unwrap_or("worker"),
            ),
            worker.worker_index
        );
        let package_dir = workers_dir.join(&package_name);
        fs::create_dir_all(&package_dir)?;
        let dist_dir = package_dir.join("dist");
        let build_id =
            worker.build_id.clone().unwrap_or_else(|| format!("migrated-{}", package_name));
        let bootstrap_path = package_dir.join("bootstrap.mjs");
        let manifest_path = package_dir.join("worker-package.json");
        let log_path = package_dir.join("worker.log");
        let pid_path = package_dir.join("worker.pid");
        let package_status = "packaged".to_owned();
        let requires_transpile =
            worker.activity_module.is_some() || worker.payload_converter_module.is_some();
        if requires_transpile {
            transpile_project_into(project_root, &dist_dir)?;
        }
        let resolved_activity_module_path =
            if let Some(activity_module) = worker.activity_module.as_deref() {
                let resolved_source = resolve_module_specifier(
                    project_root,
                    &project_root.join(&worker.file),
                    activity_module,
                )?;
                Some(
                    transpiled_output_path(project_root, &dist_dir, &resolved_source)?
                        .display()
                        .to_string(),
                )
            } else {
                None
            };
        let resolved_payload_converter_module_path =
            if let Some(payload_converter_module) = worker.payload_converter_module.as_deref() {
                let resolved_source = resolve_module_specifier(
                    project_root,
                    &project_root.join(&worker.file),
                    payload_converter_module,
                )?;
                Some(
                    transpiled_output_path(project_root, &dist_dir, &resolved_source)?
                        .display()
                        .to_string(),
                )
            } else {
                None
            };
        let resolved_activity_runtime_helper_path = if worker.activity_runtime_helper {
            Some(generate_activity_runtime_helper(
                project_root,
                &dist_dir,
                worker,
            )?)
        } else {
            None
        };
        fs::write(
            &bootstrap_path,
            render_worker_bootstrap(
                worker,
                &build_id,
                resolved_activity_module_path.as_deref(),
                resolved_activity_runtime_helper_path.as_deref(),
                resolved_payload_converter_module_path.as_deref(),
            ),
        )
        .with_context(|| format!("failed to write {}", bootstrap_path.display()))?;
        write_json(
            &manifest_path,
            &json!({
                "schema_version": 1,
                "source_worker": worker.file,
                "worker_index": worker.worker_index,
                "task_queue": worker.task_queue,
                "build_id": build_id,
                "bootstrap_pattern": worker.bootstrap_pattern,
                "workflows_path": worker.workflows_path,
                "activities_reference": worker.activities_reference,
                "activity_module": worker.activity_module,
                "activity_factory_export": worker.activity_factory_export,
                "activity_factory_args_js": worker.activity_factory_args_js,
                "activity_runtime_helper": worker.activity_runtime_helper,
                "data_converter_mode": worker.data_converter_mode,
                "payload_converter_module": worker.payload_converter_module,
                "project_root": project_root.display().to_string(),
                "dist_dir": dist_dir.display().to_string(),
                "resolved_activity_module_path": resolved_activity_module_path,
                "resolved_activity_runtime_helper_path": resolved_activity_runtime_helper_path,
                "resolved_payload_converter_module_path": resolved_payload_converter_module_path,
                "bootstrap_path": bootstrap_path.display().to_string(),
                "log_path": log_path.display().to_string(),
                "pid_path": pid_path.display().to_string()
            }),
        )?;
        packages.push(WorkerPackageRecord {
            worker_file: worker.file.clone(),
            worker_index: worker.worker_index,
            task_queue: worker.task_queue.clone(),
            build_id,
            package_dir: package_dir.display().to_string(),
            dist_dir: dist_dir.display().to_string(),
            manifest_path: manifest_path.display().to_string(),
            bootstrap_path: bootstrap_path.display().to_string(),
            resolved_activity_module_path,
            resolved_activity_runtime_helper_path,
            data_converter_mode: worker.data_converter_mode.clone(),
            resolved_payload_converter_module_path,
            log_path: log_path.display().to_string(),
            pid_path: pid_path.display().to_string(),
            package_status,
        });
    }
    Ok(packages)
}

fn preferred_packaged_workers(workers: &[DiscoveredWorker]) -> Vec<&DiscoveredWorker> {
    let has_non_test = workers.iter().any(|worker| !is_test_worker_source(&worker.file));
    workers
        .iter()
        .filter(|worker| !has_non_test || !is_test_worker_source(&worker.file))
        .collect()
}

fn is_test_worker_source(file: &str) -> bool {
    let path = Path::new(file);
    if path.components().any(|component| {
        matches!(
            component.as_os_str().to_str(),
            Some("test") | Some("tests") | Some("__tests__")
        )
    }) {
        return true;
    }
    path.file_name().and_then(|value| value.to_str()).is_some_and(|name| {
        name.contains(".test.") || name.contains(".spec.")
    })
}

fn transpile_project_into(project_root: &Path, dist_dir: &Path) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to read current directory")?;
    let project_root_abs =
        if project_root.is_absolute() { project_root.to_path_buf() } else { cwd.join(project_root) };
    let dist_dir_abs =
        if dist_dir.is_absolute() { dist_dir.to_path_buf() } else { cwd.join(dist_dir) };
    if dist_dir.exists() && dist_dir.read_dir()?.next().is_some() {
        return Ok(());
    }
    fs::create_dir_all(dist_dir)?;
    let tsc = Path::new("/Users/bene/code/fabrik/node_modules/.bin/tsc");
    if !tsc.exists() {
        bail!("missing TypeScript compiler at {}", tsc.display());
    }
    let mut command = Command::new(tsc);
    let tsconfig = project_root_abs.join("tsconfig.json");
    if tsconfig.exists() {
        command.arg("-p").arg(&tsconfig);
    } else {
        let inputs = collect_transpile_inputs(&project_root_abs)?;
        if inputs.is_empty() {
            bail!("no source files found to transpile in {}", project_root.display());
        }
        command.args(inputs);
        command.arg("--target").arg("es2022");
        command.arg("--rootDir").arg(&project_root_abs);
        command.arg("--esModuleInterop");
        command.arg("--resolveJsonModule");
    }
    let output = command
        .arg("--outDir")
        .arg(&dist_dir_abs)
        .arg("--module")
        .arg("es2022")
        .arg("--moduleResolution")
        .arg("bundler")
        .arg("--allowJs")
        .arg("--checkJs")
        .arg("false")
        .arg("--skipLibCheck")
        .arg("--noEmitOnError")
        .arg("false")
        .arg("--pretty")
        .arg("false")
        .current_dir(&project_root_abs)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("failed to run tsc for {}", project_root.display()))?;
    if !output.status.success() {
        if dist_dir.read_dir()?.next().is_some() {
            return Ok(());
        }
        let relaxed_transpiler = Path::new("/Users/bene/code/fabrik/scripts/transpile-relaxed.mjs");
        if relaxed_transpiler.exists() {
            let relaxed = Command::new("node")
                .arg(relaxed_transpiler)
                .arg(&project_root_abs)
                .arg(&dist_dir_abs)
                .current_dir(&project_root_abs)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .with_context(|| {
                    format!("failed to run relaxed transpiler for {}", project_root.display())
                })?;
            if relaxed.status.success() && dist_dir.read_dir()?.next().is_some() {
                return Ok(());
            }
        }
        bail!(
            "TypeScript transpile failed for {}: {}{}{}",
            project_root.display(),
            String::from_utf8_lossy(&output.stderr).trim(),
            if output.stdout.is_empty() { "" } else { " " },
            String::from_utf8_lossy(&output.stdout).trim()
        );
    }
    Ok(())
}

fn generate_activity_runtime_helper(
    project_root: &Path,
    dist_dir: &Path,
    worker: &DiscoveredWorker,
) -> Result<String> {
    let generator = Path::new(REPO_ROOT).join("scripts/generate-worker-runtime-helper.mjs");
    if !generator.exists() {
        bail!("missing worker runtime helper generator at {}", generator.display());
    }
    let worker_js = transpiled_output_path(project_root, dist_dir, &project_root.join(&worker.file))?;
    let helper_path = worker_js.with_file_name(format!(
        "{}.fabrik-runtime-helper.mjs",
        worker_js
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("worker")
    ));
    let output = Command::new("node")
        .arg(&generator)
        .arg("--worker-js")
        .arg(&worker_js)
        .arg("--worker-index")
        .arg(worker.worker_index.to_string())
        .arg("--output")
        .arg(&helper_path)
        .current_dir(REPO_ROOT)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("failed to run {} for {}", generator.display(), worker.file))?;
    if !output.status.success() {
        bail!(
            "worker runtime helper generation failed for {}: {}",
            worker.file,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(helper_path.display().to_string())
}

fn collect_transpile_inputs(project_root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut stack = vec![project_root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in
            fs::read_dir(&dir).with_context(|| format!("failed to read {}", dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
                    continue;
                };
                if [".git", ".next", ".turbo", "build", "coverage", "dist", "node_modules"]
                    .contains(&name)
                {
                    continue;
                }
                stack.push(path);
                continue;
            }
            let Some(extension) = path.extension().and_then(|value| value.to_str()) else {
                continue;
            };
            if !["ts", "mts", "cts", "js", "mjs", "cjs", "tsx", "jsx"].contains(&extension) {
                continue;
            }
            if path
                .file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|name| name.ends_with(".d.ts"))
            {
                continue;
            }
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn resolve_module_specifier(
    project_root: &Path,
    from_file: &Path,
    specifier: &str,
) -> Result<PathBuf> {
    if specifier.starts_with('.') {
        let bases = [
            from_file.parent().unwrap_or_else(|| Path::new("")).join(specifier),
            project_root.join(specifier),
        ];
        for base in bases {
            for candidate in module_resolution_candidates(&base) {
                if candidate.exists() {
                    return Ok(candidate);
                }
            }
        }
    }
    if let Some(resolved) =
        resolve_module_specifier_with_typescript(project_root, from_file, specifier)?
    {
        return Ok(resolved);
    }
    bail!("failed to resolve module specifier {specifier} from {}", from_file.display())
}

fn resolve_module_specifier_with_typescript(
    project_root: &Path,
    from_file: &Path,
    specifier: &str,
) -> Result<Option<PathBuf>> {
    let resolver = Path::new(REPO_ROOT).join("scripts/resolve-module-specifier.mjs");
    if !resolver.exists() {
        return Ok(None);
    }
    let output = Command::new("node")
        .arg(&resolver)
        .arg(project_root)
        .arg(from_file)
        .arg(specifier)
        .current_dir(REPO_ROOT)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("failed to run {} for {specifier}", resolver.display()))?;
    if !output.status.success() {
        bail!(
            "module resolution helper failed for {specifier}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let payload: Value = serde_json::from_slice(&output.stdout).with_context(|| {
        format!("failed to decode module resolution helper output for {specifier}")
    })?;
    let resolved = payload
        .get("resolved")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from);
    Ok(resolved.filter(|path| path.exists()))
}

fn module_resolution_candidates(base: &Path) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    if base.extension().is_some() {
        candidates.push(base.to_path_buf());
    } else {
        for extension in ["ts", "mts", "cts", "js", "mjs", "cjs"] {
            candidates.push(base.with_extension(extension));
        }
        for index_name in
            ["index.ts", "index.mts", "index.cts", "index.js", "index.mjs", "index.cjs"]
        {
            candidates.push(base.join(index_name));
        }
    }
    candidates
}

fn transpiled_output_path(
    project_root: &Path,
    dist_dir: &Path,
    source_path: &Path,
) -> Result<PathBuf> {
    let relative = source_path.strip_prefix(project_root).with_context(|| {
        format!("{} is not inside {}", source_path.display(), project_root.display())
    })?;
    let mut output = dist_dir.join(relative);
    output.set_extension(match source_path.extension().and_then(|value| value.to_str()) {
        Some("mts") => "mjs",
        Some("cts") => "cjs",
        _ => "js",
    });
    if output.exists() {
        return Ok(output);
    }
    let file_name = output
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| {
            anyhow::anyhow!("unable to determine transpiled file name for {}", output.display())
        })?
        .to_owned();
    if let Some(fallback) = find_file_by_name(dist_dir, &file_name)? {
        return Ok(fallback);
    }
    Ok(output)
}

fn find_file_by_name(root: &Path, file_name: &str) -> Result<Option<PathBuf>> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)
            .with_context(|| format!("failed to read directory {}", dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.file_name().and_then(|value| value.to_str()) == Some(file_name) {
                return Ok(Some(path));
            }
        }
    }
    Ok(None)
}

fn write_deploy_manifest(
    workspace_dir: &Path,
    artifacts: &[CompiledArtifactRecord],
    workers: &[DiscoveredWorker],
) -> Result<PathBuf> {
    let path = workspace_dir.join("deploy-manifest.json");
    let workflows = artifacts
        .iter()
        .map(|artifact| {
            json!({
                "definition_id": artifact.definition_id,
                "definition_version": artifact.definition_version,
                "artifact_hash": artifact.artifact_hash,
                "artifact_path": artifact.artifact_path,
                "entry": artifact.entry,
            })
        })
        .collect::<Vec<_>>();
    let workers = workers
        .iter()
        .map(|worker| {
            json!({
                "worker_file": worker.file,
                "task_queue": worker.task_queue,
                "build_id": worker.build_id,
                "workflows_path": worker.workflows_path,
                "activities_reference": worker.activities_reference,
                "activity_module": worker.activity_module,
                "data_converter_mode": worker.data_converter_mode,
                "payload_converter_module": worker.payload_converter_module,
                "bootstrap_pattern": worker.bootstrap_pattern,
            })
        })
        .collect::<Vec<_>>();
    write_json(
        &path,
        &json!({
            "schema_version": 1,
            "workflows": workflows,
            "workers": workers,
        }),
    )?;
    Ok(path)
}

async fn validate_artifacts(
    client: &Client,
    api_url: &str,
    tenant_id: &str,
    artifacts: &[CompiledArtifactRecord],
    validation_run_limit: usize,
) -> Result<Vec<ArtifactValidationRecord>> {
    let mut records = Vec::new();
    for artifact in artifacts {
        let payload = fs::read_to_string(&artifact.artifact_path)
            .with_context(|| format!("failed to read {}", artifact.artifact_path))?;
        let url = format!(
            "{api_url}/tenants/{tenant_id}/workflow-artifacts/validate?validation_run_limit={validation_run_limit}"
        );
        let response = client
            .post(&url)
            .header("content-type", "application/json")
            .body(payload)
            .send()
            .await
            .with_context(|| format!("failed to validate artifact {}", artifact.definition_id))?;
        if !response.status().is_success() {
            bail!(
                "artifact validation failed for {}: {}",
                artifact.definition_id,
                response.text().await.unwrap_or_default()
            );
        }
        let validation: ArtifactValidationResponse = response.json().await?;
        records.push(ArtifactValidationRecord {
            definition_id: artifact.definition_id.clone(),
            artifact_hash: artifact.artifact_hash.clone(),
            compatible: validation.compatible,
            status: validation.status,
            validation: validation.validation,
        });
    }
    Ok(records)
}

async fn deploy_bundle(
    client: &Client,
    api_url: &str,
    tenant_id: &str,
    artifacts: &[CompiledArtifactRecord],
    workers: &[WorkerPackageRecord],
) -> Result<DeploymentSummary> {
    let mut published_artifacts = Vec::new();
    for artifact in artifacts {
        let payload = fs::read_to_string(&artifact.artifact_path)
            .with_context(|| format!("failed to read {}", artifact.artifact_path))?;
        let url = format!("{api_url}/tenants/{tenant_id}/workflow-artifacts");
        let response = client
            .post(&url)
            .header("content-type", "application/json")
            .body(payload)
            .send()
            .await
            .with_context(|| format!("failed to publish artifact {}", artifact.definition_id))?;
        if !response.status().is_success() {
            return Ok(DeploymentSummary {
                requested: true,
                api_url: Some(api_url.to_owned()),
                tenant_id: Some(tenant_id.to_owned()),
                published_artifacts,
                workers: Vec::new(),
                status: "failed".to_owned(),
                message: format!(
                    "artifact publish failed for {}: {}",
                    artifact.definition_id,
                    response.text().await.unwrap_or_default()
                ),
            });
        }
        published_artifacts.push(DeployArtifactRecord {
            definition_id: artifact.definition_id.clone(),
            artifact_hash: artifact.artifact_hash.clone(),
            status: "stored".to_owned(),
        });
    }

    let artifact_hashes =
        artifacts.iter().map(|artifact| artifact.artifact_hash.clone()).collect::<Vec<_>>();
    let workflow_build_id = env::var("FABRIK_WORKFLOW_BUILD_ID")
        .or_else(|_| env::var("WORKFLOW_WORKER_BUILD_ID"))
        .unwrap_or_else(|_| "dev-workflow-build".to_owned());
    let mut workflow_task_queues = BTreeSet::new();
    for worker in workers {
        if let Some(task_queue) = worker.task_queue.as_deref() {
            workflow_task_queues.insert(task_queue.to_owned());
        }
    }
    for task_queue in workflow_task_queues {
        let register_url =
            format!("{api_url}/admin/tenants/{tenant_id}/task-queues/workflow/{task_queue}/builds");
        let response = client
            .post(&register_url)
            .json(&json!({
                "build_id": workflow_build_id,
                "artifact_hashes": artifact_hashes,
                "promote_default": true
            }))
            .send()
            .await
            .with_context(|| {
                format!(
                    "failed to register workflow build {} for {}",
                    workflow_build_id, task_queue
                )
            })?;
        if !response.status().is_success() {
            return Ok(DeploymentSummary {
                requested: true,
                api_url: Some(api_url.to_owned()),
                tenant_id: Some(tenant_id.to_owned()),
                published_artifacts,
                workers: Vec::new(),
                status: "failed".to_owned(),
                message: format!(
                    "workflow build registration failed for {} on {}: {}",
                    workflow_build_id,
                    task_queue,
                    response.text().await.unwrap_or_default()
                ),
            });
        }
    }
    let mut deployed_workers = Vec::new();
    let matching_endpoint = env::var("FABRIK_UNIFIED_RUNTIME_ENDPOINT")
        .or_else(|_| env::var("FABRIK_MATCHING_ENDPOINT"))
        .unwrap_or_else(|_| "http://127.0.0.1:50054".to_owned());
    let poller_wait_ms = env::var("FABRIK_ACTIVITY_POLLER_WAIT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(10_000);
    for worker in workers {
        let Some(task_queue) = worker.task_queue.as_deref() else {
            continue;
        };
        let register_url =
            format!("{api_url}/admin/tenants/{tenant_id}/task-queues/activity/{task_queue}/builds");
        let response = client
            .post(&register_url)
            .json(&json!({
                "build_id": worker.build_id,
                "artifact_hashes": artifact_hashes,
                "promote_default": true
            }))
            .send()
            .await
            .with_context(|| format!("failed to register build {}", worker.build_id))?;
        if !response.status().is_success() {
            return Ok(DeploymentSummary {
                requested: true,
                api_url: Some(api_url.to_owned()),
                tenant_id: Some(tenant_id.to_owned()),
                published_artifacts,
                workers: deployed_workers,
                status: "failed".to_owned(),
                message: format!(
                    "build registration failed for {}: {}",
                    worker.build_id,
                    response.text().await.unwrap_or_default()
                ),
            });
        }
        let managed_process = match spawn_managed_activity_worker(
            tenant_id,
            &matching_endpoint,
            task_queue,
            worker,
        ) {
            Ok(process) => process,
            Err(error) => {
                return Ok(DeploymentSummary {
                    requested: true,
                    api_url: Some(api_url.to_owned()),
                    tenant_id: Some(tenant_id.to_owned()),
                    published_artifacts,
                    workers: deployed_workers,
                    status: "failed".to_owned(),
                    message: format!(
                        "failed to launch managed worker {} for {}: {error}",
                        worker.build_id, task_queue
                    ),
                });
            }
        };
        fs::write(&worker.pid_path, managed_process.pid.to_string())
            .with_context(|| format!("failed to write {}", worker.pid_path))?;
        let poller_count =
            match wait_for_poller_count(client, api_url, tenant_id, task_queue, poller_wait_ms)
                .await
            {
                Ok(count) => count,
                Err(error) => {
                    return Ok(DeploymentSummary {
                        requested: true,
                        api_url: Some(api_url.to_owned()),
                        tenant_id: Some(tenant_id.to_owned()),
                        published_artifacts,
                        workers: deployed_workers,
                        status: "failed".to_owned(),
                        message: format!(
                            "failed while waiting for pollers on {} / {}: {error}",
                            task_queue, worker.build_id
                        ),
                    });
                }
            };
        deployed_workers.push(DeployWorkerRecord {
            task_queue: task_queue.to_owned(),
            build_id: worker.build_id.clone(),
            registered: true,
            poller_count,
            pid: Some(managed_process.pid),
            log_path: Some(worker.log_path.clone()),
            status: if poller_count > 0 {
                "ready".to_owned()
            } else {
                "waiting_for_pollers".to_owned()
            },
        });
    }

    let all_workers_ready = deployed_workers.iter().all(|worker| worker.poller_count > 0);
    Ok(DeploymentSummary {
        requested: true,
        api_url: Some(api_url.to_owned()),
        tenant_id: Some(tenant_id.to_owned()),
        published_artifacts,
        workers: deployed_workers,
        status: if all_workers_ready { "deployed".to_owned() } else { "failed".to_owned() },
        message: if all_workers_ready {
            "artifacts published, builds registered, and activity pollers detected".to_owned()
        } else {
            "artifacts published and builds registered, but no activity pollers were detected for every required task queue".to_owned()
        },
    })
}

struct ManagedWorkerProcess {
    pid: u32,
}

struct ManagedWorkerCommand {
    program: OsString,
    args: Vec<OsString>,
    current_dir: Option<PathBuf>,
}

fn resolve_managed_activity_worker_command() -> Result<ManagedWorkerCommand> {
    if let Ok(bin) = env::var("FABRIK_ACTIVITY_WORKER_BIN") {
        return Ok(ManagedWorkerCommand {
            program: OsString::from(bin),
            args: Vec::new(),
            current_dir: None,
        });
    }

    if let Ok(current_exe) = env::current_exe() {
        if let Some(bin_dir) = current_exe.parent() {
            let sibling = bin_dir.join("activity-worker-service");
            if sibling.is_file() {
                return Ok(ManagedWorkerCommand {
                    program: sibling.into_os_string(),
                    args: Vec::new(),
                    current_dir: None,
                });
            }
        }
    }

    Ok(ManagedWorkerCommand {
        program: OsString::from("cargo"),
        args: vec![
            OsString::from("run"),
            OsString::from("-p"),
            OsString::from("activity-worker-service"),
            OsString::from("--quiet"),
        ],
        current_dir: Some(PathBuf::from(REPO_ROOT)),
    })
}

fn spawn_managed_activity_worker(
    tenant_id: &str,
    matching_endpoint: &str,
    task_queue: &str,
    worker: &WorkerPackageRecord,
) -> Result<ManagedWorkerProcess> {
    let stdout = File::create(&worker.log_path)
        .with_context(|| format!("failed to open {}", worker.log_path))?;
    let stderr =
        stdout.try_clone().with_context(|| format!("failed to clone {}", worker.log_path))?;
    let resolved = resolve_managed_activity_worker_command()?;
    let mut command = Command::new(&resolved.program);
    command.args(&resolved.args);
    if let Some(current_dir) = &resolved.current_dir {
        command.current_dir(current_dir);
    }
    #[cfg(unix)]
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command
        .env("ACTIVITY_WORKER_SERVICE_PORT", "0")
        .env("MATCHING_SERVICE_ENDPOINT", matching_endpoint)
        .env("ACTIVITY_TASK_QUEUE", task_queue)
        .env("ACTIVITY_WORKER_TENANT_ID", tenant_id)
        .env("ACTIVITY_WORKER_BUILD_ID", &worker.build_id)
        .env("ACTIVITY_ENABLE_BULK_LANES", "false")
        .env("ACTIVITY_WORKER_CONCURRENCY", "1")
        .env("ACTIVITY_RESULT_FLUSHER_CONCURRENCY", "1")
        .env("ACTIVITY_NODE_BOOTSTRAP", &worker.bootstrap_path)
        .env("ACTIVITY_NODE_EXECUTABLE", "node")
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .with_context(|| format!("failed to spawn managed activity worker {}", worker.build_id))?;
    Ok(ManagedWorkerProcess { pid: child.id() })
}

async fn wait_for_poller_count(
    client: &Client,
    api_url: &str,
    tenant_id: &str,
    task_queue: &str,
    wait_ms: u64,
) -> Result<usize> {
    let inspection_url =
        format!("{api_url}/admin/tenants/{tenant_id}/task-queues/activity/{task_queue}");
    let deadline = std::time::Instant::now() + Duration::from_millis(wait_ms);
    loop {
        let inspection = client
            .get(&inspection_url)
            .send()
            .await
            .with_context(|| format!("failed to inspect task queue {task_queue}"))?;
        if !inspection.status().is_success() {
            bail!(
                "task queue inspection failed for {}: {}",
                task_queue,
                inspection.text().await.unwrap_or_default()
            );
        }
        let inspection: TaskQueueInspection = inspection.json().await?;
        let poller_count = inspection.pollers.len();
        if poller_count > 0 || std::time::Instant::now() >= deadline {
            return Ok(poller_count);
        }
        sleep(Duration::from_millis(250)).await;
    }
}

fn render_worker_bootstrap(
    worker: &DiscoveredWorker,
    build_id: &str,
    resolved_activity_module_path: Option<&str>,
    resolved_activity_runtime_helper_path: Option<&str>,
    resolved_payload_converter_module_path: Option<&str>,
) -> String {
    let source_worker = serde_json::to_string(&worker.file).expect("worker file serializes");
    let task_queue = serde_json::to_string(&worker.task_queue).expect("task queue serializes");
    let build_id = serde_json::to_string(build_id).expect("build id serializes");
    let workflows_path =
        serde_json::to_string(&worker.workflows_path).expect("workflows path serializes");
    let activities_reference = serde_json::to_string(&worker.activities_reference)
        .expect("activities reference serializes");
    let activity_module =
        serde_json::to_string(&worker.activity_module).expect("activity module serializes");
    let activity_factory_export = serde_json::to_string(&worker.activity_factory_export)
        .expect("activity factory export serializes");
    let activity_factory_args_js = serde_json::to_string(&worker.activity_factory_args_js)
        .expect("activity factory args serialize");
    let activity_runtime_helper = serde_json::to_string(&worker.activity_runtime_helper)
        .expect("activity runtime helper serializes");
    let resolved_activity_module_path = serde_json::to_string(&resolved_activity_module_path)
        .expect("resolved activity module path serializes");
    let resolved_activity_runtime_helper_path =
        serde_json::to_string(&resolved_activity_runtime_helper_path)
            .expect("resolved activity runtime helper path serializes");
    let resolved_payload_converter_module_path =
        serde_json::to_string(&resolved_payload_converter_module_path)
            .expect("resolved payload converter module path serializes");
    let data_converter_mode =
        serde_json::to_string(&worker.data_converter_mode).expect("data converter mode serializes");
    let bootstrap_pattern =
        serde_json::to_string(&worker.bootstrap_pattern).expect("bootstrap pattern serializes");
    format!(
        "import {{ pathToFileURL }} from 'node:url';\n\
         export const fabrikMigratedWorker = {{\n  sourceWorker: {source_worker},\n  taskQueue: {task_queue},\n  buildId: {build_id},\n  workflowsPath: {workflows_path},\n  activitiesReference: {activities_reference},\n  activityModule: {activity_module},\n  activityFactoryExport: {activity_factory_export},\n  activityFactoryArgsJs: {activity_factory_args_js},\n  activityRuntimeHelper: {activity_runtime_helper},\n  resolvedActivityModulePath: {resolved_activity_module_path},\n  resolvedActivityRuntimeHelperPath: {resolved_activity_runtime_helper_path},\n  dataConverterMode: {data_converter_mode},\n  resolvedPayloadConverterModulePath: {resolved_payload_converter_module_path},\n  bootstrapPattern: {bootstrap_pattern}\n}};\n\
         async function loadPayloadConverterModule() {{\n\
           if (typeof fabrikMigratedWorker.dataConverterMode !== 'string' || !fabrikMigratedWorker.dataConverterMode.startsWith('path_')) {{\n\
             return null;\n\
           }}\n\
           if (!fabrikMigratedWorker.resolvedPayloadConverterModulePath) {{\n\
             throw new Error('worker package is missing a resolved payload converter module path');\n\
           }}\n\
           const converterMod = await import(pathToFileURL(fabrikMigratedWorker.resolvedPayloadConverterModulePath).href);\n\
           if (!('payloadConverter' in converterMod)) {{\n\
             throw new Error(`payload converter module ${{fabrikMigratedWorker.resolvedPayloadConverterModulePath}} does not export payloadConverter`);\n\
           }}\n\
           return converterMod.payloadConverter;\n\
         }}\n\
         async function invoke(request) {{\n\
           await loadPayloadConverterModule();\n\
           if (!fabrikMigratedWorker.resolvedActivityModulePath) {{\n\
             throw new Error(`worker ${{fabrikMigratedWorker.sourceWorker}} does not export activities for Fabrik dispatch`);\n\
           }}\n\
           let activityContainer;\n\
           if (fabrikMigratedWorker.activityRuntimeHelper) {{\n\
             if (!fabrikMigratedWorker.resolvedActivityRuntimeHelperPath) {{\n\
               throw new Error(`worker ${{fabrikMigratedWorker.sourceWorker}} is missing its activity runtime helper`);\n\
             }}\n\
             const helper = await import(pathToFileURL(fabrikMigratedWorker.resolvedActivityRuntimeHelperPath).href);\n\
             const runtime = await helper.createFabrikWorkerRuntime();\n\
             activityContainer = runtime?.activities;\n\
           }} else {{\n\
             const mod = await import(pathToFileURL(fabrikMigratedWorker.resolvedActivityModulePath).href);\n\
             activityContainer = fabrikMigratedWorker.activityFactoryExport\n\
               ? mod[fabrikMigratedWorker.activityFactoryExport](...fabrikMigratedWorker.activityFactoryArgsJs.map((arg) => (0, eval)(arg)))\n\
               : mod;\n\
           }}\n\
           const fn = activityContainer?.[request.activity_type];\n\
           if (typeof fn !== 'function') {{\n\
             throw new Error(`activity ${{request.activity_type}} is not exported by ${{fabrikMigratedWorker.resolvedActivityModulePath}}`);\n\
           }}\n\
           const args = Array.isArray(request.input) ? request.input : request.input == null ? [] : [request.input];\n\
           return await fn(...args);\n\
         }}\n\
         async function main() {{\n\
           let data = '';\n\
           process.stdin.setEncoding('utf8');\n\
           for await (const chunk of process.stdin) {{ data += chunk; }}\n\
           const request = JSON.parse(data || '{{}}');\n\
           try {{\n\
             const output = await invoke(request);\n\
             process.stdout.write(JSON.stringify({{ ok: true, output }}));\n\
           }} catch (error) {{\n\
             process.stdout.write(JSON.stringify({{ ok: false, error: String(error?.message ?? error) }}));\n\
             process.exitCode = 1;\n\
           }}\n\
         }}\n\
         await main();\n",
        source_worker = source_worker,
        task_queue = task_queue,
        build_id = build_id,
        workflows_path = workflows_path,
        activities_reference = activities_reference,
        activity_module = activity_module,
        activity_factory_export = activity_factory_export,
        activity_factory_args_js = activity_factory_args_js,
        activity_runtime_helper = activity_runtime_helper,
        resolved_activity_module_path = resolved_activity_module_path,
        resolved_activity_runtime_helper_path = resolved_activity_runtime_helper_path,
        data_converter_mode = data_converter_mode,
        resolved_payload_converter_module_path = resolved_payload_converter_module_path,
        bootstrap_pattern = bootstrap_pattern,
    )
}

fn render_markdown_report(report: &MigrationReport) -> String {
    let mut lines = Vec::new();
    lines.push("# Temporal TypeScript Migration Report".to_owned());
    lines.push(String::new());
    lines.push(format!("- Status: `{:?}`", report.status));
    lines.push(format!("- Alpha qualification: `{}`", report.alpha_qualification.verdict.as_str()));
    lines.push(format!("- Milestone scope: `{}`", report.milestone_scope));
    lines.push(format!("- Source path: `{}`", report.source_path));
    lines.push(format!("- Repo fingerprint: `{}`", report.repo_fingerprint));
    lines.push(format!("- SDK target: `{}`", report.sdk_target));
    lines.push(String::new());
    lines.push("## Summary".to_owned());
    lines.push(String::new());
    lines.push(format!(
        "- Workflows discovered: `{}`",
        report.discovered.get("workflows").and_then(Value::as_array).map_or(0, Vec::len)
    ));
    lines.push(format!(
        "- Worker bootstraps discovered: `{}`",
        report.discovered.get("workers").and_then(Value::as_array).map_or(0, Vec::len)
    ));
    lines.push(format!("- Findings: `{}`", report.findings.len()));
    lines.push(format!(
        "- Qualification blockers: `{}`",
        report.alpha_qualification.blocker_categories.len()
    ));
    lines.push(format!(
        "- Supported features in frozen subset: `{}`",
        report
            .trust
            .get("support_summary")
            .and_then(|value| value.get("supported_feature_count"))
            .and_then(Value::as_u64)
            .unwrap_or(0)
    ));
    lines.push(format!(
        "- Headline trusted features: `{}`",
        report
            .trust
            .get("support_summary")
            .and_then(|value| value.get("headline_trusted_feature_count"))
            .and_then(Value::as_u64)
            .unwrap_or(0)
    ));
    lines.push(format!(
        "- Upgrade-validated features: `{}`",
        report
            .trust
            .get("support_summary")
            .and_then(|value| value.get("upgrade_validated_feature_count"))
            .and_then(Value::as_u64)
            .unwrap_or(0)
    ));
    lines.push(String::new());
    lines.push("## Alpha Qualification".to_owned());
    lines.push(String::new());
    lines.push(format!(
        "- Verdict: `{}` {}",
        report.alpha_qualification.verdict.as_str(),
        report.alpha_qualification.summary
    ));
    if !report.alpha_qualification.blocker_categories.is_empty() {
        lines.push("- Blocker categories:".to_owned());
        for group in &report.alpha_qualification.blocker_categories {
            lines.push(format!("- `{}` count=`{}`", group.category.as_str(), group.count));
            for reason in &group.reasons {
                lines.push(format!("  - {}", reason));
            }
        }
    }
    if !report.alpha_qualification.caveats.is_empty() {
        lines.push("- Caveats:".to_owned());
        for caveat in &report.alpha_qualification.caveats {
            lines.push(format!("  - {}", caveat));
        }
    }
    if !report.alpha_qualification.next_steps.is_empty() {
        lines.push("- Next steps:".to_owned());
        for step in &report.alpha_qualification.next_steps {
            lines.push(format!("  - {}", step));
        }
    }
    lines.push(String::new());
    lines.push("## Trust Contracts".to_owned());
    lines.push(String::new());
    if let Some(goal) = report.trust.get("goal").and_then(Value::as_str) {
        lines.push(format!("- Goal: {}", goal));
    }
    if let Some(items) = report.trust.get("contract_documents").and_then(Value::as_array) {
        for item in items {
            lines.push(format!(
                "- {}: `{}` {}",
                item.get("name").and_then(Value::as_str).unwrap_or("Contract"),
                item.get("path").and_then(Value::as_str).unwrap_or("-"),
                item.get("summary").and_then(Value::as_str).unwrap_or("")
            ));
        }
    }
    if let Some(requirements) = report.trust.get("promotion_requirements").and_then(Value::as_array)
    {
        lines.push("- Promotion rule:".to_owned());
        for requirement in requirements {
            lines.push(format!("- {}", requirement.as_str().unwrap_or("unknown")));
        }
    }
    lines.push(String::new());
    lines.push("## Conformance Program".to_owned());
    lines.push(String::new());
    if let Some(layers) = report
        .trust
        .get("conformance_manifest")
        .and_then(|value| value.get("layers"))
        .and_then(Value::as_array)
    {
        for layer in layers {
            lines.push(format!(
                "- {}: {}",
                layer.get("name").and_then(Value::as_str).unwrap_or("Layer"),
                layer.get("purpose").and_then(Value::as_str).unwrap_or("")
            ));
        }
    }
    lines.push(String::new());
    lines.push("## Validation".to_owned());
    lines.push(String::new());
    for (label, gate) in [
        ("Source compatibility preflight", &report.validation.source_compatibility_preflight),
        ("Workflow compile validation", &report.validation.workflow_compile_validation),
        ("Worker packaging validation", &report.validation.worker_packaging_validation),
        ("Payload/data converter validation", &report.validation.payload_data_converter_validation),
        ("Visibility/search validation", &report.validation.visibility_search_validation),
        ("Replay/rollout validation", &report.validation.replay_rollout_validation),
        ("Deploy validation", &report.validation.deploy_validation),
    ] {
        lines.push(format!("- {}: `{}` {}", label, gate.status, gate.message));
    }
    lines.push(String::new());
    lines.push("## Support Matrix".to_owned());
    lines.push(String::new());
    for feature in &report.support_matrix {
        lines.push(format!(
            "- `{}` status=`{}` confidence=`{}` support=`{:?}`",
            feature.label,
            feature.milestone_status,
            feature.confidence_class,
            feature.support_level
        ));
    }
    lines.push(String::new());
    lines.push("## Workflows".to_owned());
    lines.push(String::new());
    for workflow in &report.compiled_workflows {
        lines.push(format!(
            "- `{}`:`{}` -> `{}` v{} status=`{}` confidence=`{}`",
            workflow.file,
            workflow.export_name,
            workflow.definition_id,
            workflow.definition_version,
            workflow.status,
            workflow
                .confidence_class
                .as_ref()
                .map(|value| format!("{value:?}"))
                .unwrap_or_else(|| "none".to_owned())
        ));
        if let Some(error) = workflow.compile_error.as_deref() {
            lines.push(format!("  compile_error: {}", error));
        }
    }
    lines.push(String::new());
    lines.push("## Worker Packages".to_owned());
    lines.push(String::new());
    for worker in &report.worker_packages {
        lines.push(format!(
            "- `{}` queue=`{}` build=`{}` status=`{}`",
            worker.worker_file,
            worker.task_queue.as_deref().unwrap_or("-"),
            worker.build_id,
            worker.package_status
        ));
    }
    if !report.findings.is_empty() {
        lines.push(String::new());
        lines.push("## Findings".to_owned());
        lines.push(String::new());
        for finding in &report.findings {
            lines.push(format!(
                "- `{}` `{}` {}:{}:{} {}",
                finding.severity_string(),
                finding.code,
                finding.file,
                finding.line.unwrap_or(0),
                finding.column.unwrap_or(0),
                finding.message
            ));
        }
    }
    lines.push(String::new());
    lines.join("\n")
}

impl AnalyzerFinding {
    fn severity_string(&self) -> &'static str {
        match self.severity {
            FindingSeverity::HardBlock => "hard_block",
            FindingSeverity::Warning => "warning",
            FindingSeverity::Info => "info",
        }
    }
}

impl AlphaQualificationVerdict {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Qualified => "qualified",
            Self::QualifiedWithCaveats => "qualified_with_caveats",
            Self::Blocked => "blocked",
        }
    }
}

impl QualificationBlockerCategory {
    fn as_str(&self) -> &'static str {
        match self {
            Self::UnsupportedApi => "unsupported_api",
            Self::UnsupportedPackagingBootstrap => "unsupported_packaging_bootstrap",
            Self::UnsupportedVisibilitySearchUsage => "unsupported_visibility_search_usage",
            Self::ReplayTrustBlocker => "replay_trust_blocker",
            Self::OperationalBlocker => "operational_blocker",
        }
    }
}

fn sanitize_segment(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect::<String>()
        .trim_matches('-')
        .to_owned()
}

fn write_json(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_string_pretty(value)?)
        .with_context(|| format!("failed to write {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_temporal_migrate_args() -> Result<()> {
        let args = parse_args(vec![
            "migrate".to_owned(),
            "temporal".to_owned(),
            "/Users/bene/code/fabrik".to_owned(),
            "--analyze-only".to_owned(),
        ])?;
        assert!(args.analyze_only);
        assert!(!args.deploy);
        Ok(())
    }

    #[test]
    fn render_report_contains_status() {
        let report = MigrationReport {
            schema_version: 1,
            milestone_scope: "temporal_ts_subset_trust".to_owned(),
            source_path: "/tmp/app".to_owned(),
            repo_fingerprint: "abc".to_owned(),
            sdk_target: "temporal_typescript".to_owned(),
            status: MigrationStatus::CompatibleReadyNotDeployed,
            alpha_qualification: AlphaQualificationReport {
                verdict: AlphaQualificationVerdict::QualifiedWithCaveats,
                summary: "summary".to_owned(),
                blocker_categories: Vec::new(),
                caveats: vec!["caveat".to_owned()],
                next_steps: vec!["step".to_owned()],
            },
            discovered: json!({ "workflows": [], "workers": [] }),
            support_matrix: Vec::new(),
            findings: Vec::new(),
            generated_artifacts: Vec::new(),
            compiled_workflows: Vec::new(),
            worker_packages: Vec::new(),
            validation: ValidationSummary {
                source_compatibility_preflight: GateResult {
                    status: "passed".to_owned(),
                    message: "ok".to_owned(),
                },
                workflow_compile_validation: GateResult {
                    status: "passed".to_owned(),
                    message: "ok".to_owned(),
                },
                worker_packaging_validation: GateResult {
                    status: "passed".to_owned(),
                    message: "ok".to_owned(),
                },
                payload_data_converter_validation: GateResult {
                    status: "passed".to_owned(),
                    message: "ok".to_owned(),
                },
                visibility_search_validation: GateResult {
                    status: "passed".to_owned(),
                    message: "ok".to_owned(),
                },
                replay_rollout_validation: GateResult {
                    status: "warning".to_owned(),
                    message: "none".to_owned(),
                },
                deploy_validation: GateResult {
                    status: "skipped".to_owned(),
                    message: "none".to_owned(),
                },
                replay_artifacts: Vec::new(),
            },
            deployment: DeploymentSummary {
                requested: false,
                api_url: None,
                tenant_id: None,
                published_artifacts: Vec::new(),
                workers: Vec::new(),
                status: "not_requested".to_owned(),
                message: "none".to_owned(),
            },
            trust: json!({
                "goal": "goal",
                "support_summary": {
                    "supported_feature_count": 1,
                    "headline_trusted_feature_count": 1,
                    "upgrade_validated_feature_count": 0
                },
                "promotion_requirements": ["analyzer support exists"],
                "conformance_manifest": {
                    "layers": [
                        {
                            "name": "Layer A: support fixtures",
                            "purpose": "support"
                        }
                    ]
                },
                "contract_documents": [
                    {
                        "name": "Durability and replay contract",
                        "path": "/tmp/contract.md",
                        "summary": "summary"
                    }
                ]
            }),
        };
        let markdown = render_markdown_report(&report);
        assert!(markdown.contains("Temporal TypeScript Migration Report"));
        assert!(markdown.contains("CompatibleReadyNotDeployed"));
        assert!(markdown.contains("qualified_with_caveats"));
        assert!(markdown.contains("temporal_ts_subset_trust"));
    }
}
