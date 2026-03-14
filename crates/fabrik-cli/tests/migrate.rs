use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("test-fixtures").join(name)
}

fn temp_output_dir(label: &str) -> PathBuf {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_nanos();
    std::env::temp_dir().join(format!("fabrik-cli-{label}-{nonce}"))
}

fn run_cli(
    project_root: &Path,
    output_dir: &Path,
    extra_args: &[&str],
) -> (std::process::ExitStatus, Value) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_fabrik"));
    command.args(["migrate", "temporal"]);
    command.arg(project_root);
    command.args(["--output-dir", output_dir.to_str().expect("utf8")]);
    command.args(extra_args);
    command.current_dir("/Users/bene/code/fabrik");
    let output = command.output().expect("cli runs");
    let report =
        fs::read_to_string(output_dir.join("migration-report.json")).expect("report exists");
    let payload: Value = serde_json::from_str(&report).expect("report json");
    (output.status, payload)
}

fn run_cli_relative(
    project_root: &Path,
    output_dir: &Path,
    extra_args: &[&str],
) -> (std::process::ExitStatus, Value) {
    let cwd = Path::new("/Users/bene/code/fabrik");
    let relative_root =
        project_root.strip_prefix(cwd).expect("fixture path should be under repo root");
    let mut command = Command::new(env!("CARGO_BIN_EXE_fabrik"));
    command.args(["migrate", "temporal"]);
    command.arg(relative_root);
    command.args(["--output-dir", output_dir.to_str().expect("utf8")]);
    command.args(extra_args);
    command.current_dir(cwd);
    let output = command.output().expect("cli runs");
    let report =
        fs::read_to_string(output_dir.join("migration-report.json")).expect("report exists");
    let payload: Value = serde_json::from_str(&report).expect("report json");
    (output.status, payload)
}

#[test]
fn supported_project_compiles_and_packages() {
    let output_dir = temp_output_dir("supported");
    let (status, report) = run_cli(&fixture("temporal-supported"), &output_dir, &[]);
    assert!(status.success(), "stderr: {:?}", report);
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["milestone_scope"], "temporal_ts_subset_trust");
    assert_eq!(report["validation"]["source_compatibility_preflight"]["status"], "passed");
    assert_eq!(report["validation"]["workflow_compile_validation"]["status"], "passed");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert!(report["worker_packages"][0]["resolved_activity_module_path"].is_string());
    assert_eq!(report["trust"]["support_summary"]["headline_trusted_feature_count"], 11);
    let generated = report["generated_artifacts"].as_array().expect("generated artifacts");
    assert!(generated.iter().any(|artifact| artifact["kind"] == "alpha_qualification"));
    assert!(generated.iter().any(|artifact| artifact["kind"] == "equivalence_contract"));
    assert!(generated.iter().any(|artifact| artifact["kind"] == "conformance_manifest"));
    assert!(generated.iter().any(|artifact| artifact["kind"] == "trust_summary"));
}

#[test]
fn analyze_only_supported_project_stops_before_compile() {
    let output_dir = temp_output_dir("supported-analyze");
    let (status, report) =
        run_cli(&fixture("temporal-supported"), &output_dir, &["--analyze-only"]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["validation"]["workflow_compile_validation"]["status"], "skipped");
}

#[test]
fn default_compatible_payload_converter_usage_is_qualified_for_alpha() {
    let output_dir = temp_output_dir("payload-qualified");
    let (status, report) = run_cli(&fixture("temporal-payload-qualified"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["validation"]["payload_data_converter_validation"]["status"], "passed");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert_eq!(report["worker_packages"][0]["data_converter_mode"], "default_temporal");
}

#[test]
fn static_worker_bootstrap_patterns_from_real_temporal_samples_are_qualified() {
    let output_dir = temp_output_dir("bootstrap-qualified");
    let (status, report) = run_cli(&fixture("temporal-bootstrap-qualified"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert_eq!(report["worker_packages"][0]["task_queue"], "bootstrap-qualified");
    assert_eq!(report["discovered"]["workers"][0]["bootstrap_pattern"], "worker_create_static");
}

#[test]
fn esm_workflows_path_bootstrap_pattern_is_qualified() {
    let output_dir = temp_output_dir("bootstrap-esm-qualified");
    let (status, report) = run_cli(&fixture("temporal-bootstrap-esm-qualified"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert_eq!(report["worker_packages"][0]["task_queue"], "bootstrap-esm-qualified");
    assert_eq!(report["discovered"]["workers"][0]["workflows_path"], "./workflows.ts");
}

#[test]
fn self_file_workflows_path_and_workflow_context_guard_qualify() {
    let output_dir = temp_output_dir("bootstrap-self-file-qualified");
    let (status, report) =
        run_cli(&fixture("temporal-bootstrap-self-file-qualified"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert_eq!(report["worker_packages"][0]["task_queue"], "bootstrap-self-file");
}

#[test]
fn activity_factory_registration_with_static_bootstrap_args_is_packaged() {
    let output_dir = temp_output_dir("activity-factory-qualified");
    let (status, report) =
        run_cli(&fixture("temporal-activity-factory-qualified"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert_eq!(report["worker_packages"][0]["task_queue"], "activity-factory-qualified");
    assert_eq!(report["discovered"]["workers"][0]["activity_factory_export"], "createActivities");
    let factory_arg = report["discovered"]["workers"][0]["activity_factory_args_js"][0]
        .as_str()
        .expect("factory arg");
    assert!(factory_arg.contains("async get(_key)"));
    assert!(factory_arg.contains("\"Temporal\""));
}

#[test]
fn test_only_worker_bootstrap_findings_do_not_block_production_worker() {
    let output_dir = temp_output_dir("test-worker-qualified");
    let (status, report) = run_cli(&fixture("temporal-test-worker-qualified"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert_eq!(report["worker_packages"][0]["task_queue"], "test-worker-qualified");
    assert!(
        report["findings"]
            .as_array()
            .expect("findings")
            .iter()
            .all(|finding| !(finding["file"] == "src/workflows.test.ts"
                && finding["severity"] == "hard_block"))
    );
}

#[test]
fn dynamic_worker_task_queue_is_packaged_with_caveats() {
    let output_dir = temp_output_dir("bootstrap-dynamic-taskqueue-qualified");
    let (status, report) =
        run_cli(&fixture("temporal-bootstrap-dynamic-taskqueue-qualified"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["validation"]["source_compatibility_preflight"]["status"], "passed");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert!(report["worker_packages"][0]["task_queue"].is_null());
}

#[test]
fn default_compatible_payload_converter_path_usage_is_qualified_for_alpha() {
    let output_dir = temp_output_dir("payload-path-qualified");
    let (status, report) = run_cli(&fixture("temporal-payload-path-qualified"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["validation"]["payload_data_converter_validation"]["status"], "passed");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert_eq!(report["worker_packages"][0]["data_converter_mode"], "path_default_temporal");
    assert!(report["worker_packages"][0]["resolved_payload_converter_module_path"].is_string());
}

#[test]
fn mixed_build_payload_converter_path_upgrade_fixture_is_qualified_for_alpha() {
    let output_dir = temp_output_dir("payload-path-qualified-v2");
    let (status, report) =
        run_cli(&fixture("temporal-payload-path-qualified-v2"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    assert_eq!(report["compiled_workflows"][0]["definition_version"], 1);
    assert_eq!(report["worker_packages"][0]["data_converter_mode"], "path_default_temporal");
}

#[test]
fn static_data_converter_factory_usage_is_qualified_for_alpha() {
    let output_dir = temp_output_dir("data-converter-factory");
    let (status, report) =
        run_cli(&fixture("temporal-data-converter-factory-qualified"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["validation"]["payload_data_converter_validation"]["status"], "passed");
    assert_eq!(
        report["worker_packages"][0]["data_converter_mode"],
        "static_data_converter_factory"
    );
}

#[test]
fn interceptor_workflow_modules_do_not_register_helper_exports_as_workflows() {
    let output_dir = temp_output_dir("interceptor-pressure");
    let (status, report) = run_cli(&fixture("temporal-interceptor-pressure"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["compiled_workflows"].as_array().expect("compiled workflows").len(), 1);
    assert_eq!(
        report["compiled_workflows"][0]["definition_id"],
        "src-workflows-interceptorpressureworkflow"
    );
    assert_eq!(
        report["discovered"]["files"]
            .as_array()
            .expect("files")
            .iter()
            .find(|file| file["path"] == "src/interceptors.ts")
            .expect("interceptor file")["exported_workflows"]
            .as_array()
            .expect("exported workflows")
            .len(),
        0
    );
}

#[test]
fn static_payload_converter_module_usage_is_qualified_for_alpha() {
    let output_dir = temp_output_dir("payload");
    let (status, report) = run_cli(&fixture("temporal-payload-blocked"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["validation"]["payload_data_converter_validation"]["status"], "passed");
    assert_eq!(
        report["worker_packages"][0]["data_converter_mode"],
        "path_static_payload_converter"
    );
    assert!(report["worker_packages"][0]["resolved_payload_converter_module_path"].is_string());
}

#[test]
fn static_visibility_usage_is_qualified_for_alpha() {
    let output_dir = temp_output_dir("visibility");
    let (status, report) = run_cli(&fixture("temporal-visibility-blocked"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["validation"]["visibility_search_validation"]["status"], "passed");
    assert!(
        report["findings"]
            .as_array()
            .expect("findings")
            .iter()
            .all(|finding| finding["feature"] != "visibility_search_usage")
    );
}

#[test]
fn shadow_project_with_static_evaluable_visibility_qualifies() {
    let output_dir = temp_output_dir("shadow-qualified");
    let (status, report) = run_cli(&fixture("temporal-shadow-qualified"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["validation"]["visibility_search_validation"]["status"], "passed");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
}

#[test]
fn supported_temporal_workflow_api_slice_qualifies_and_packages() {
    let output_dir = temp_output_dir("supported-api-qualified");
    let (status, report) = run_cli(&fixture("temporal-supported-api-qualified"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert_eq!(report["worker_packages"][0]["task_queue"], "supported-api");
}

#[test]
fn temporal_activity_failure_type_checks_qualify_and_package() {
    let output_dir = temp_output_dir("activity-failure-qualified");
    let (status, report) =
        run_cli(&fixture("temporal-activity-failure-qualified"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert_eq!(report["worker_packages"][0]["task_queue"], "activity-failure-qualified");
}

#[test]
fn temporal_patching_api_slice_qualifies_and_packages() {
    let output_dir = temp_output_dir("patching-qualified");
    let (status, report) = run_cli(&fixture("temporal-patching-qualified"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert_eq!(report["worker_packages"][0]["task_queue"], "patching-qualified");
}

#[test]
fn temporal_worker_versioning_annotations_qualify_and_package() {
    let output_dir = temp_output_dir("worker-versioning-qualified");
    let (status, report) =
        run_cli(&fixture("temporal-worker-versioning-qualified"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert_eq!(report["worker_packages"][0]["task_queue"], "worker-versioning-qualified");
}

#[test]
fn temporal_wrapped_worker_versioning_workflow_qualifies_and_packages() {
    let output_dir = temp_output_dir("versioning-upgrade-pressure");
    let (status, report) =
        run_cli(&fixture("temporal-versioning-upgrade-pressure"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    assert_eq!(report["compiled_workflows"][0]["export_name"], "versioningPressureWorkflow");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert_eq!(report["worker_packages"][0]["task_queue"], "versioning-pressure");
}

#[test]
fn temporal_search_attributes_workflow_slice_qualifies_and_packages() {
    let output_dir = temp_output_dir("search-attributes-qualified");
    let (status, report) =
        run_cli(&fixture("temporal-search-attributes-qualified"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["validation"]["visibility_search_validation"]["status"], "passed");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert_eq!(report["worker_packages"][0]["task_queue"], "search-attributes-qualified");
}

#[test]
fn workflow_local_map_state_qualifies_and_packages() {
    let output_dir = temp_output_dir("map-state-qualified");
    let (status, report) = run_cli(&fixture("temporal-map-state-qualified"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert_eq!(report["worker_packages"][0]["task_queue"], "map-state-qualified");
}

#[test]
fn workflow_only_workers_package_without_activity_modules() {
    let output_dir = temp_output_dir("workflow-only-qualified");
    let (status, report) = run_cli(&fixture("temporal-workflow-only-qualified"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert!(report["worker_packages"][0]["resolved_activity_module_path"].is_null());
    assert_eq!(report["worker_packages"][0]["task_queue"], "workflow-only");
}

#[test]
fn workspace_and_tsconfig_path_activity_modules_package_cleanly() {
    let output_dir = temp_output_dir("workspace-qualified");
    let (status, report) = run_cli(&fixture("temporal-workspace-qualified"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert_eq!(report["worker_packages"][0]["task_queue"], "workspace-qualified");
    assert!(report["worker_packages"][0]["resolved_activity_module_path"].is_string());
}

#[test]
fn relative_project_root_uses_relaxed_transpile_fallback_for_worker_packaging() {
    let output_dir = temp_output_dir("relative-relaxed-transpile");
    let (status, report) =
        run_cli_relative(&fixture("temporal-relaxed-transpile-qualified"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    let worker_packages = report["worker_packages"].as_array().expect("worker packages");
    assert_eq!(worker_packages.len(), 1);
    assert_eq!(worker_packages[0]["worker_file"], "src/worker.ts");
    assert_eq!(worker_packages[0]["package_status"], "packaged");
    assert!(worker_packages[0]["resolved_activity_module_path"].is_string());
}

#[test]
fn monorepo_multiworker_packages_do_not_collide_on_shared_worker_filenames() {
    let output_dir = temp_output_dir("monorepo-multiworker");
    let (status, report) =
        run_cli(&fixture("temporal-monorepo-multiworker-pressure"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    let worker_packages = report["worker_packages"].as_array().expect("worker packages");
    assert_eq!(worker_packages.len(), 2);
    let package_dirs = worker_packages
        .iter()
        .map(|worker| worker["package_dir"].as_str().expect("package dir"))
        .collect::<Vec<_>>();
    assert_ne!(package_dirs[0], package_dirs[1]);
    assert!(package_dirs.iter().any(|dir| dir.contains("apps-orders-worker-src-worker-1")));
    assert!(package_dirs.iter().any(|dir| dir.contains("apps-reports-worker-src-worker-1")));
}

#[test]
fn async_external_pressure_fixture_qualifies_and_packages_both_workflows() {
    let output_dir = temp_output_dir("async-external-pressure");
    let (status, report) =
        run_cli(&fixture("temporal-async-external-pressure"), &output_dir, &[]);
    assert!(status.success(), "report: {report:?}");
    assert_eq!(report["status"], "compatible_ready_not_deployed");
    assert_eq!(report["alpha_qualification"]["verdict"], "qualified_with_caveats");
    let compiled_workflows = report["compiled_workflows"].as_array().expect("compiled workflows");
    assert_eq!(compiled_workflows.len(), 2);
    let definition_ids = compiled_workflows
        .iter()
        .map(|workflow| workflow["definition_id"].as_str().expect("definition id"))
        .collect::<Vec<_>>();
    assert!(definition_ids.iter().any(|id| id.contains("asyncsignaltargetworkflow")));
    assert!(definition_ids.iter().any(|id| id.contains("externalcontrollerworkflow")));
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert_eq!(report["worker_packages"][0]["task_queue"], "async-external-pressure");
}

#[test]
fn dynamic_worker_bootstrap_blocks_migration() {
    let output_dir = temp_output_dir("dynamic-bootstrap");
    let (status, report) =
        run_cli(&fixture("temporal-dynamic-bootstrap-blocked"), &output_dir, &[]);
    assert_eq!(status.code(), Some(2));
    assert_eq!(report["status"], "incompatible_blocked");
    assert_eq!(report["validation"]["source_compatibility_preflight"]["status"], "blocked");
    assert!(
        report["alpha_qualification"]["blocker_categories"]
            .as_array()
            .expect("blocker categories")
            .iter()
            .any(|group| group["category"] == "unsupported_packaging_bootstrap")
    );
}

#[test]
fn unsupported_temporal_api_blocks_migration() {
    let output_dir = temp_output_dir("unsupported-api");
    let (status, report) = run_cli(&fixture("temporal-unsupported-api-blocked"), &output_dir, &[]);
    assert_eq!(status.code(), Some(2));
    assert_eq!(report["status"], "incompatible_blocked");
    let findings = report["findings"].as_array().expect("findings array");
    assert!(findings.iter().any(|finding| finding["feature"] == "unsupported_temporal_api"));
    assert!(
        report["alpha_qualification"]["blocker_categories"]
            .as_array()
            .expect("blocker categories")
            .iter()
            .any(|group| group["category"] == "unsupported_api")
    );
}
