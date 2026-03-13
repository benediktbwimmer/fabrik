#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

SOURCE_REPORT="${1:-target/shadow-qualification/temporal-payload-blocked/migration-report.json}"
OUTPUT_DIR="${2:-target/payload-converter-scope}"

mkdir -p "$OUTPUT_DIR"

python3 - "$SOURCE_REPORT" "$OUTPUT_DIR" <<'PY'
import json
import pathlib
import sys

report_path = pathlib.Path(sys.argv[1])
output_dir = pathlib.Path(sys.argv[2])
report = json.loads(report_path.read_text(encoding="utf-8"))
worker = (report.get("discovered", {}).get("workers") or [{}])[0]
findings = report.get("findings") or []

scope = {
    "schema_version": 1,
    "source_report": str(report_path.resolve()),
    "repo": report.get("source_path"),
    "status": report.get("status"),
    "verdict": report.get("alpha_qualification", {}).get("verdict"),
    "scope_name": "payload_converter_path_default_compatible",
    "exact_pattern": {
        "task_queue": worker.get("task_queue"),
        "bootstrap_pattern": worker.get("bootstrap_pattern"),
        "data_converter_mode": worker.get("data_converter_mode"),
        "blocked_symbols": [finding.get("symbol") for finding in findings if finding.get("feature") == "payload_data_converter_usage"],
    },
    "in_scope": [
        "static Worker.create({ ... })",
        "static payloadConverterPath string literal",
        "resolved module exports payloadConverter",
        "default-compatible payload converter only",
    ],
    "out_of_scope": [
        "payload codecs",
        "codec server",
        "failure converter overrides",
        "client-side dataConverter injection",
        "arbitrary custom converter logic",
    ],
}

markdown = [
    "# Payload Converter Project Scope",
    "",
    f"- Repo: `{scope['repo']}`",
    f"- Source report: `{scope['source_report']}`",
    f"- Current status: `{scope['status']}`",
    f"- Current verdict: `{scope['verdict']}`",
    f"- Scope name: `{scope['scope_name']}`",
    "",
    "## Exact Pattern",
    "",
    f"- Task queue: `{scope['exact_pattern']['task_queue']}`",
    f"- Bootstrap pattern: `{scope['exact_pattern']['bootstrap_pattern']}`",
    f"- Current converter mode: `{scope['exact_pattern']['data_converter_mode']}`",
    f"- Blocked symbols: `{', '.join([symbol for symbol in scope['exact_pattern']['blocked_symbols'] if symbol])}`",
    "",
    "## In Scope",
    "",
]
markdown.extend([f"- {item}" for item in scope["in_scope"]])
markdown.extend([
    "",
    "## Out Of Scope",
    "",
])
markdown.extend([f"- {item}" for item in scope["out_of_scope"]])
markdown.append("")

(output_dir / "payload-converter-scope.json").write_text(json.dumps(scope, indent=2) + "\n", encoding="utf-8")
(output_dir / "payload-converter-scope.md").write_text("\n".join(markdown), encoding="utf-8")
PY
