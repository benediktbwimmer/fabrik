#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

OUTPUT_ROOT="target/external-repo-qualification"
DEPLOY="off"
REPOS=()

usage() {
  cat >&2 <<'EOF'
usage: scripts/run-external-repo-qualification.sh [options] <repo> [<repo> ...]

options:
  --output-dir <path>
  --deploy
EOF
  exit 1
}

while (($#)); do
  case "$1" in
    --output-dir)
      OUTPUT_ROOT="$2"
      shift 2
      ;;
    --deploy)
      DEPLOY="on"
      shift
      ;;
    --help|-h)
      usage
      ;;
    *)
      REPOS+=("$1")
      shift
      ;;
  esac
done

if [[ ${#REPOS[@]} -eq 0 ]]; then
  usage
fi

mkdir -p "$OUTPUT_ROOT"

for repo in "${REPOS[@]}"; do
  if [[ ! -d "$repo" ]]; then
    echo "repo not found: $repo" >&2
    exit 1
  fi
done

for repo in "${REPOS[@]}"; do
  repo_name="$(basename "$repo")"
  output_dir="${OUTPUT_ROOT}/${repo_name}"
  mkdir -p "$output_dir"
  set +e
  if [[ "$DEPLOY" == "on" ]]; then
    cargo run -q -p fabrik-cli -- migrate temporal "$repo" --output-dir "$output_dir" --deploy
  else
    cargo run -q -p fabrik-cli -- migrate temporal "$repo" --output-dir "$output_dir"
  fi
  status=$?
  set -e
  if [[ "$status" != "0" && "$status" != "2" ]]; then
    echo "qualification failed unexpectedly for ${repo} (exit ${status})" >&2
    exit "$status"
  fi
done

python3 - "$OUTPUT_ROOT" "${REPOS[@]}" <<'PY'
import json
import pathlib
import sys

output_root = pathlib.Path(sys.argv[1]).resolve()
repos = [pathlib.Path(arg).resolve() for arg in sys.argv[2:]]

summary = {
    "schema_version": 1,
    "repos": [],
}

lines = [
    "# External Repo Qualification Summary",
    "",
    "This summary records the current qualification state for external Temporal TypeScript repos.",
    "",
]

for repo in repos:
    repo_name = repo.name
    report_path = output_root / repo_name / "migration-report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    blocker_categories = report["alpha_qualification"].get("blocker_categories", [])
    summary["repos"].append(
        {
            "repo_name": repo_name,
            "repo_path": str(repo),
            "status": report["status"],
            "verdict": report["alpha_qualification"]["verdict"],
            "summary": report["alpha_qualification"]["summary"],
            "blocker_categories": blocker_categories,
            "report_path": str(report_path),
        }
    )
    lines.extend(
        [
            f"## {repo_name}",
            "",
            f"- Repo: `{repo}`",
            f"- Status: `{report['status']}`",
            f"- Verdict: `{report['alpha_qualification']['verdict']}`",
            f"- Summary: {report['alpha_qualification']['summary']}",
            f"- Report: `{report_path}`",
            "",
        ]
    )
    if blocker_categories:
        lines.append("- Blocker categories:")
        for group in blocker_categories:
            lines.append(f"  - `{group['category']}` x{group['count']}")
        lines.append("")

(output_root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
(output_root / "summary.md").write_text("\n".join(lines), encoding="utf-8")
PY
