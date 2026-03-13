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
    assert_eq!(report["validation"]["source_compatibility_preflight"]["status"], "passed");
    assert_eq!(report["validation"]["workflow_compile_validation"]["status"], "passed");
    assert_eq!(report["compiled_workflows"][0]["status"], "compiled");
    assert_eq!(report["worker_packages"][0]["package_status"], "packaged");
    assert!(report["worker_packages"][0]["resolved_activity_module_path"].is_string());
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
fn payload_converter_usage_blocks_migration() {
    let output_dir = temp_output_dir("payload");
    let (status, report) = run_cli(&fixture("temporal-payload-blocked"), &output_dir, &[]);
    assert_eq!(status.code(), Some(2));
    assert_eq!(report["status"], "incompatible_blocked");
    assert_eq!(report["validation"]["payload_data_converter_validation"]["status"], "blocked");
}

#[test]
fn visibility_usage_blocks_migration() {
    let output_dir = temp_output_dir("visibility");
    let (status, report) = run_cli(&fixture("temporal-visibility-blocked"), &output_dir, &[]);
    assert_eq!(status.code(), Some(2));
    assert_eq!(report["status"], "incompatible_blocked");
    assert_eq!(report["validation"]["visibility_search_validation"]["status"], "blocked");
}

#[test]
fn dynamic_worker_bootstrap_blocks_migration() {
    let output_dir = temp_output_dir("dynamic-bootstrap");
    let (status, report) =
        run_cli(&fixture("temporal-dynamic-bootstrap-blocked"), &output_dir, &[]);
    assert_eq!(status.code(), Some(2));
    assert_eq!(report["status"], "incompatible_blocked");
    assert_eq!(report["validation"]["source_compatibility_preflight"]["status"], "blocked");
}

#[test]
fn unsupported_temporal_api_blocks_migration() {
    let output_dir = temp_output_dir("unsupported-api");
    let (status, report) = run_cli(&fixture("temporal-unsupported-api-blocked"), &output_dir, &[]);
    assert_eq!(status.code(), Some(2));
    assert_eq!(report["status"], "incompatible_blocked");
    let findings = report["findings"].as_array().expect("findings array");
    assert!(findings.iter().any(|finding| finding["feature"] == "unsupported_temporal_api"));
}
