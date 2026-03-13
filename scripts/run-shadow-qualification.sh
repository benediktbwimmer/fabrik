#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

OUTPUT_ROOT="target/shadow-qualification"
FIXTURES=(
  "temporal-shadow-qualified"
  "temporal-visibility-blocked"
  "temporal-payload-qualified"
  "temporal-payload-path-qualified"
  "temporal-payload-blocked"
  "temporal-dynamic-bootstrap-blocked"
  "temporal-unsupported-api-blocked"
)

while (($#)); do
  case "$1" in
    --output-dir)
      OUTPUT_ROOT="$2"
      shift 2
      ;;
    --fixture)
      FIXTURES=("$2")
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

mkdir -p "$OUTPUT_ROOT"

for fixture in "${FIXTURES[@]}"; do
  set +e
  cargo run -p fabrik-cli -- migrate temporal \
    "crates/fabrik-cli/test-fixtures/${fixture}" \
    --output-dir "${OUTPUT_ROOT}/${fixture}"
  status=$?
  set -e
  if [[ "$status" != "0" && "$status" != "2" ]]; then
    echo "shadow qualification failed unexpectedly for ${fixture} (exit ${status})" >&2
    exit "$status"
  fi
done

python3 - "$OUTPUT_ROOT" "${FIXTURES[@]}" <<'PY'
import json
import pathlib
import sys

output_root = pathlib.Path(sys.argv[1])
fixtures = sys.argv[2:]

def decision_for(fixture, report):
    if fixture == "temporal-shadow-qualified":
        return {
            "decision": "shadow_target",
            "reason": "deployable shadow repo exercising static-evaluable memo/search attribute shapes inside the alpha subset",
        }
    verdict = report["alpha_qualification"]["verdict"]
    if fixture == "temporal-visibility-blocked" and verdict != "blocked":
        return {
            "decision": "fixed_now",
            "reason": "static-evaluable memo/search attribute shapes now qualify for the alpha slice",
        }
    if fixture == "temporal-payload-qualified" and verdict != "blocked":
        return {
            "decision": "fixed_now",
            "reason": "default-compatible dataConverter declarations now qualify for the alpha payload adapter slice",
        }
    if fixture == "temporal-payload-path-qualified" and verdict != "blocked":
        return {
            "decision": "fixed_now",
            "reason": "default-compatible payloadConverterPath modules now qualify for the alpha payload adapter slice",
        }
    return {
        "decision": "explicit_alpha_limitation",
        "reason": report["alpha_qualification"]["summary"],
    }

summary = {
    "schema_version": 1,
    "generated_reports": [],
}
lines = [
    "# Shadow Qualification Summary",
    "",
    "This summary records the current workspace shadow qualification results for the Temporal TS alpha boundary.",
    "",
]

for fixture in fixtures:
    report_path = output_root / fixture / "migration-report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    decision = decision_for(fixture, report)
    summary["generated_reports"].append(
        {
            "fixture": fixture,
            "status": report["status"],
            "verdict": report["alpha_qualification"]["verdict"],
            "blocker_categories": [
                group["category"]
                for group in report["alpha_qualification"].get("blocker_categories", [])
            ],
            "decision": decision["decision"],
            "reason": decision["reason"],
            "report_path": str(report_path.resolve()),
        }
    )
    lines.extend(
        [
            f"## {fixture}",
            "",
            f"- Status: `{report['status']}`",
            f"- Verdict: `{report['alpha_qualification']['verdict']}`",
            f"- Decision: `{decision['decision']}`",
            f"- Reason: {decision['reason']}",
            f"- Report: `{report_path.resolve()}`",
            "",
        ]
    )
    blocker_categories = report["alpha_qualification"].get("blocker_categories", [])
    if blocker_categories:
        lines.append("- Blocker categories:")
        for group in blocker_categories:
            lines.append(f"  - `{group['category']}` x{group['count']}")
        lines.append("")

(output_root / "summary.json").write_text(
    json.dumps(summary, indent=2) + "\n",
    encoding="utf-8",
)
(output_root / "summary.md").write_text("\n".join(lines), encoding="utf-8")
PY
