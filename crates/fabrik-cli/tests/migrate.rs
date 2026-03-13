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
fn custom_payload_converter_usage_blocks_migration() {
    let output_dir = temp_output_dir("payload");
    let (status, report) = run_cli(&fixture("temporal-payload-blocked"), &output_dir, &[]);
    assert_eq!(status.code(), Some(2));
    assert_eq!(report["status"], "incompatible_blocked");
    assert_eq!(report["alpha_qualification"]["verdict"], "blocked");
    assert_eq!(report["validation"]["payload_data_converter_validation"]["status"], "blocked");
    assert!(
        report["findings"]
            .as_array()
            .expect("findings")
            .iter()
            .any(|finding| finding["feature"] == "payload_data_converter_usage")
    );
    assert!(
        report["alpha_qualification"]["blocker_categories"]
            .as_array()
            .expect("blocker categories")
            .iter()
            .any(|group| group["category"] == "unsupported_api")
    );
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
