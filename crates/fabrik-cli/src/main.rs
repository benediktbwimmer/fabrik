use std::{
    collections::BTreeSet,
    env,
    fs::{self, File},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{Context, Result, bail};
use fabrik_workflow::CompiledWorkflowArtifact;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::{Duration, sleep};

const MIGRATION_REPORT_SCHEMA_VERSION: u32 = 1;

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
    support_level: SupportLevel,
    replay_validation: bool,
    deploy_validation: bool,
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
    task_queue: Option<String>,
    build_id: Option<String>,
    workflows_path: Option<String>,
    activities_reference: Option<String>,
    activity_module: Option<String>,
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
    task_queue: Option<String>,
    build_id: String,
    package_dir: String,
    dist_dir: String,
    manifest_path: String,
    bootstrap_path: String,
    resolved_activity_module_path: Option<String>,
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
    source_path: String,
    repo_fingerprint: String,
    sdk_target: String,
    status: MigrationStatus,
    discovered: Value,
    support_matrix: Vec<SupportMatrixEntry>,
    findings: Vec<AnalyzerFinding>,
    generated_artifacts: Vec<Value>,
    compiled_workflows: Vec<WorkflowMigrationRecord>,
    worker_packages: Vec<WorkerPackageRecord>,
    validation: ValidationSummary,
    deployment: DeploymentSummary,
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

    write_json(
        &workspace_dir.join("support-matrix.json"),
        &json!({ "support_matrix": analyzer.support_matrix }),
    )?;
    write_json(
        &workspace_dir.join("analysis.json"),
        &serde_json::to_value(&analyzer).expect("analysis serializes"),
    )?;

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
                "custom payload/data converter usage must be removed or adapter-backed before migration"
                    .to_owned()
            } else {
                "no payload/data converter blockers detected".to_owned()
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

    let report = MigrationReport {
        schema_version: MIGRATION_REPORT_SCHEMA_VERSION,
        source_path: args.project_root.display().to_string(),
        repo_fingerprint: fingerprint,
        sdk_target: "temporal_typescript".to_owned(),
        status,
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
        .current_dir("/Users/bene/code/fabrik")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("failed to spawn Temporal TypeScript analyzer")?;
    if !output.status.success() {
        bail!("temporal analyzer failed: {}", String::from_utf8_lossy(&output.stderr).trim());
    }
    serde_json::from_slice(&output.stdout).context("failed to decode analyzer output")
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
    for worker in workers {
        let package_name = sanitize_segment(
            Path::new(&worker.file)
                .file_stem()
                .and_then(|value| value.to_str())
                .unwrap_or("worker"),
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
        let mut package_status = "packaged".to_owned();
        let resolved_activity_module_path =
            if let Some(activity_module) = worker.activity_module.as_deref() {
                transpile_project_into(project_root, &dist_dir)?;
                let resolved_source =
                    resolve_module_specifier(&project_root.join(&worker.file), activity_module)?;
                Some(
                    transpiled_output_path(project_root, &dist_dir, &resolved_source)?
                        .display()
                        .to_string(),
                )
            } else {
                package_status = "blocked".to_owned();
                None
            };
        fs::write(
            &bootstrap_path,
            render_worker_bootstrap(worker, &build_id, resolved_activity_module_path.as_deref()),
        )
        .with_context(|| format!("failed to write {}", bootstrap_path.display()))?;
        write_json(
            &manifest_path,
            &json!({
                "schema_version": 1,
                "source_worker": worker.file,
                "task_queue": worker.task_queue,
                "build_id": build_id,
                "bootstrap_pattern": worker.bootstrap_pattern,
                "workflows_path": worker.workflows_path,
                "activities_reference": worker.activities_reference,
                "activity_module": worker.activity_module,
                "project_root": project_root.display().to_string(),
                "dist_dir": dist_dir.display().to_string(),
                "resolved_activity_module_path": resolved_activity_module_path,
                "bootstrap_path": bootstrap_path.display().to_string(),
                "log_path": log_path.display().to_string(),
                "pid_path": pid_path.display().to_string()
            }),
        )?;
        packages.push(WorkerPackageRecord {
            worker_file: worker.file.clone(),
            task_queue: worker.task_queue.clone(),
            build_id,
            package_dir: package_dir.display().to_string(),
            dist_dir: dist_dir.display().to_string(),
            manifest_path: manifest_path.display().to_string(),
            bootstrap_path: bootstrap_path.display().to_string(),
            resolved_activity_module_path,
            log_path: log_path.display().to_string(),
            pid_path: pid_path.display().to_string(),
            package_status,
        });
    }
    Ok(packages)
}

fn transpile_project_into(project_root: &Path, dist_dir: &Path) -> Result<()> {
    if dist_dir.exists() && dist_dir.read_dir()?.next().is_some() {
        return Ok(());
    }
    fs::create_dir_all(dist_dir)?;
    let tsc = Path::new("/Users/bene/code/fabrik/node_modules/.bin/tsc");
    if !tsc.exists() {
        bail!("missing TypeScript compiler at {}", tsc.display());
    }
    let output = Command::new(tsc)
        .arg("-p")
        .arg(project_root.join("tsconfig.json"))
        .arg("--outDir")
        .arg(dist_dir)
        .arg("--module")
        .arg("es2022")
        .arg("--moduleResolution")
        .arg("bundler")
        .arg("--skipLibCheck")
        .current_dir(project_root)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("failed to run tsc for {}", project_root.display()))?;
    if !output.status.success() {
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

fn resolve_module_specifier(from_file: &Path, specifier: &str) -> Result<PathBuf> {
    if specifier.starts_with('.') {
        let base = from_file.parent().unwrap_or_else(|| Path::new("")).join(specifier);
        for candidate in module_resolution_candidates(&base) {
            if candidate.exists() {
                return Ok(candidate);
            }
        }
    }
    bail!("failed to resolve module specifier {specifier} from {}", from_file.display())
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
    Ok(output)
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
                format!("failed to register workflow build {} for {}", workflow_build_id, task_queue)
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
    let matching_endpoint = env::var("FABRIK_MATCHING_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:50051".to_owned());
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
    let mut command = if let Ok(bin) = env::var("FABRIK_ACTIVITY_WORKER_BIN") {
        Command::new(bin)
    } else {
        let mut command = Command::new("cargo");
        command
            .arg("run")
            .arg("-p")
            .arg("activity-worker-service")
            .arg("--quiet")
            .current_dir("/Users/bene/code/fabrik");
        command
    };
    let child = command
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
) -> String {
    let module_path = resolved_activity_module_path.unwrap_or("");
    format!(
        "import {{ pathToFileURL }} from 'node:url';\n\
         export const fabrikMigratedWorker = {{\n  sourceWorker: {source_worker:?},\n  taskQueue: {task_queue:?},\n  buildId: {build_id:?},\n  workflowsPath: {workflows_path:?},\n  activitiesReference: {activities_reference:?},\n  activityModule: {activity_module:?},\n  resolvedActivityModulePath: {resolved_activity_module_path:?},\n  bootstrapPattern: {bootstrap_pattern:?}\n}};\n\
         async function invoke(request) {{\n\
           if (!fabrikMigratedWorker.resolvedActivityModulePath) {{\n\
             throw new Error('worker package is missing a resolved activity module path');\n\
           }}\n\
           const mod = await import(pathToFileURL(fabrikMigratedWorker.resolvedActivityModulePath).href);\n\
           const fn = mod[request.activity_type];\n\
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
         if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {{\n\
           await main();\n\
         }}\n",
        source_worker = worker.file,
        task_queue = worker.task_queue,
        build_id = build_id,
        workflows_path = worker.workflows_path,
        activities_reference = worker.activities_reference,
        activity_module = worker.activity_module,
        resolved_activity_module_path = module_path,
        bootstrap_pattern = worker.bootstrap_pattern,
    )
}

fn render_markdown_report(report: &MigrationReport) -> String {
    let mut lines = Vec::new();
    lines.push("# Temporal TypeScript Migration Report".to_owned());
    lines.push(String::new());
    lines.push(format!("- Status: `{:?}`", report.status));
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
            source_path: "/tmp/app".to_owned(),
            repo_fingerprint: "abc".to_owned(),
            sdk_target: "temporal_typescript".to_owned(),
            status: MigrationStatus::CompatibleReadyNotDeployed,
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
        };
        let markdown = render_markdown_report(&report);
        assert!(markdown.contains("Temporal TypeScript Migration Report"));
        assert!(markdown.contains("CompatibleReadyNotDeployed"));
    }
}
