#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
  cat >&2 <<'EOF'
usage: scripts/run-temporal-ts-blocker-census.sh [options]

options:
  --repo-root <path>
  --output-dir <path>
  --sample <name>
  --limit <n>
  --bootstrap <auto|off>
EOF
  exit 1
}

REPO_ROOT="target/external/temporal-samples-typescript"
OUTPUT_ROOT="target/blocker-census/temporal-samples-typescript"
LIMIT=""
BOOTSTRAP_MODE="auto"
SAMPLES=()

while (($#)); do
  case "$1" in
    --repo-root)
      REPO_ROOT="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_ROOT="$2"
      shift 2
      ;;
    --sample)
      SAMPLES+=("$2")
      shift 2
      ;;
    --limit)
      LIMIT="$2"
      shift 2
      ;;
    --bootstrap)
      BOOTSTRAP_MODE="$2"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      ;;
  esac
done

if [[ "$BOOTSTRAP_MODE" != "auto" && "$BOOTSTRAP_MODE" != "off" ]]; then
  echo "unsupported bootstrap mode: $BOOTSTRAP_MODE" >&2
  usage
fi

if [[ ! -d "$REPO_ROOT" ]]; then
  echo "repo root not found: $REPO_ROOT" >&2
  exit 1
fi

mkdir -p "$OUTPUT_ROOT"

if [[ ${#SAMPLES[@]} -eq 0 ]]; then
  while IFS= read -r sample; do
    SAMPLES+=("$sample")
  done < <(
    find "$REPO_ROOT" -mindepth 1 -maxdepth 1 -type d \
      ! -name '.*' \
      -exec test -f '{}/package.json' ';' \
      -print \
      | sort \
      | sed "s#^${REPO_ROOT}/##"
  )
fi

if [[ -n "$LIMIT" ]]; then
  SAMPLES=("${SAMPLES[@]:0:$LIMIT}")
fi

if [[ ${#SAMPLES[@]} -eq 0 ]]; then
  echo "no sample directories selected" >&2
  exit 1
fi

FABRIK_BIN="${FABRIK_BIN:-target/debug/fabrik}"
if [[ ! -x "$FABRIK_BIN" ]]; then
  cargo build -p fabrik-cli >/dev/null
fi

repo_head="$(git -C "$REPO_ROOT" rev-parse HEAD)"
repo_remote="$(git -C "$REPO_ROOT" remote get-url origin)"

collect_package_specs() {
  python3 - "$REPO_ROOT" "$@" <<'PY'
import json
import pathlib
import sys

repo_root = pathlib.Path(sys.argv[1]).resolve()
sample_names = sys.argv[2:]
package_specs = {}
package_files = [repo_root / sample / "package.json" for sample in sample_names]

def include_dev_dependency(name: str) -> bool:
    return (
        name == "typescript"
        or name in {"ts-node", "tsx"}
        or name.startswith("@types/")
        or name.startswith("@tsconfig/")
    )

for package_file in package_files:
    if not package_file.exists():
        continue
    package = json.loads(package_file.read_text(encoding="utf-8"))
    for group_name in ("dependencies", "devDependencies", "peerDependencies"):
        for name, version in package.get(group_name, {}).items():
            if not isinstance(version, str):
                continue
            if version == "*":
                continue
            if version.startswith(("file:", "link:", "workspace:", ".")):
                continue
            if group_name != "dependencies" and not include_dev_dependency(name):
                continue
            package_specs[name] = version

for name in sorted(package_specs):
    print(f"{name}@{package_specs[name]}")
PY
}

bootstrap_dependencies() {
  local scope_name="$1"
  shift
  local specs=()
  while IFS= read -r spec; do
    [[ -n "$spec" ]] && specs+=("$spec")
  done < <(collect_package_specs "$@")

  if [[ ${#specs[@]} -eq 0 ]]; then
    return 0
  fi

  local bootstrap_dir="${OUTPUT_ROOT}/_bootstrap/${scope_name}"
  mkdir -p "$bootstrap_dir"
  local bootstrap_log="${bootstrap_dir}/npm-install.log"
  echo "Bootstrapping shared dependencies for ${scope_name} (${#specs[@]} packages)" >&2
  npm install \
    --prefix "$REPO_ROOT" \
    --no-save \
    --no-package-lock \
    --ignore-scripts \
    --legacy-peer-deps \
    "${specs[@]}" >"$bootstrap_log" 2>&1
}

write_synthetic_report() {
  local sample="$1"
  local output_dir="$2"
  local exit_status="$3"
  local stderr_log="$4"
  local stdout_log="$5"

  python3 - "$sample" "$output_dir" "$exit_status" "$stderr_log" "$stdout_log" <<'PY'
import json
import pathlib
import sys

sample = sys.argv[1]
output_dir = pathlib.Path(sys.argv[2]).resolve()
exit_status = int(sys.argv[3])
stderr_log = pathlib.Path(sys.argv[4]).resolve()
stdout_log = pathlib.Path(sys.argv[5]).resolve()
stderr_text = stderr_log.read_text(encoding="utf-8", errors="replace")

category = "source_environment_bootstrap"
feature = "source_environment_bootstrap"
summary = "The source repo could not be analyzed end to end because its local dependency/TypeScript environment was incomplete for this sample."
next_steps = [
    "Bootstrap the sample's declared dependencies into the source repo and rerun the census.",
    "If the sample depends on unsupported tooling or packaging conventions, classify that explicitly before widening Fabrik support.",
]

if "Cannot find module" not in stderr_text and "error TS6053" not in stderr_text:
    category = "unexpected_tool_failure"
    feature = "unexpected_tool_failure"
    summary = "The migration analyzer failed before it could emit a normal qualification report for this sample."
    next_steps = [
        "Inspect the captured stdout/stderr logs and reproduce the tool failure locally.",
        "If this reflects a real sample packaging pattern, convert it into an explicit blocker category before widening support.",
    ]

report = {
    "schema_version": 1,
    "source_path": str(output_dir.parent / sample),
    "status": "source_environment_blocked",
    "findings": [
        {
            "feature": feature,
            "severity": "hard_block",
            "message": summary,
        }
    ],
    "alpha_qualification": {
        "verdict": "blocked",
        "summary": summary,
        "blocker_categories": [
            {
                "category": category,
                "count": 1,
                "reasons": [summary],
            }
        ],
        "caveats": [
            "This is a synthetic blocker-census report emitted because the migration CLI did not complete normally.",
            f"fabrik migrate temporal exited with status {exit_status}",
        ],
        "next_steps": next_steps,
    },
    "validation": {
        "stdout_log": str(stdout_log),
        "stderr_log": str(stderr_log),
    },
}

(output_dir / "migration-report.json").write_text(
    json.dumps(report, indent=2) + "\n", encoding="utf-8"
)
PY
}

run_sample_migration() {
  local sample="$1"
  local output_dir="$2"
  local project_root="${REPO_ROOT}/${sample}"
  local stdout_log="${output_dir}/migrate.stdout.log"
  local stderr_log="${output_dir}/migrate.stderr.log"
  local status

  set +e
  "$FABRIK_BIN" migrate temporal "$project_root" --output-dir "$output_dir" \
    >"$stdout_log" 2>"$stderr_log"
  status=$?
  set -e

  if [[ "$status" == "0" || "$status" == "2" ]]; then
    return 0
  fi

  if [[ "$BOOTSTRAP_MODE" == "auto" ]]; then
    bootstrap_dependencies "sample-${sample}" "$sample" || true
    set +e
    "$FABRIK_BIN" migrate temporal "$project_root" --output-dir "$output_dir" \
      >"$stdout_log" 2>"$stderr_log"
    status=$?
    set -e
    if [[ "$status" == "0" || "$status" == "2" ]]; then
      return 0
    fi
  fi

  write_synthetic_report "$sample" "$output_dir" "$status" "$stderr_log" "$stdout_log"
}

if [[ "$BOOTSTRAP_MODE" == "auto" ]]; then
  bootstrap_dependencies "selected-samples" "${SAMPLES[@]}" || true
fi

for sample in "${SAMPLES[@]}"; do
  project_root="${REPO_ROOT}/${sample}"
  output_dir="${OUTPUT_ROOT}/${sample}"
  mkdir -p "$output_dir"
  run_sample_migration "$sample" "$output_dir"
done

python3 - "$OUTPUT_ROOT" "$REPO_ROOT" "$repo_remote" "$repo_head" "${SAMPLES[@]}" <<'PY'
import collections
import json
import pathlib
import sys

output_root = pathlib.Path(sys.argv[1])
repo_root = pathlib.Path(sys.argv[2]).resolve()
repo_remote = sys.argv[3]
repo_head = sys.argv[4]
samples = sys.argv[5:]

status_counts = collections.Counter()
verdict_counts = collections.Counter()
blocker_category_counts = collections.Counter()
blocker_feature_counts = collections.Counter()
samples_by_category = collections.defaultdict(list)
samples_by_feature = collections.defaultdict(list)
records = []

for sample in samples:
    report_path = output_root / sample / "migration-report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    status = report["status"]
    verdict = report["alpha_qualification"]["verdict"]
    status_counts[status] += 1
    verdict_counts[verdict] += 1

    finding_features = [
        finding["feature"]
        for finding in report.get("findings", [])
        if finding.get("severity") == "hard_block"
    ]
    for feature in finding_features:
        blocker_feature_counts[feature] += 1
        samples_by_feature[feature].append(sample)

    blocker_categories = [
        {
            "category": group["category"],
            "count": group["count"],
            "reasons": group.get("reasons", []),
        }
        for group in report["alpha_qualification"].get("blocker_categories", [])
    ]
    for group in blocker_categories:
        blocker_category_counts[group["category"]] += group["count"]
        samples_by_category[group["category"]].append(sample)

    records.append(
        {
            "sample": sample,
            "project_root": str((repo_root / sample).resolve()),
            "status": status,
            "verdict": verdict,
            "hard_block_features": finding_features,
            "blocker_categories": blocker_categories,
            "report_path": str(report_path.resolve()),
        }
    )

records.sort(key=lambda item: (item["verdict"], item["status"], item["sample"]))

summary = {
    "schema_version": 1,
    "source": {
        "repo_root": str(repo_root),
        "remote": repo_remote,
        "head": repo_head,
        "sample_count": len(samples),
    },
    "counts": {
        "by_status": dict(status_counts),
        "by_verdict": dict(verdict_counts),
        "by_blocker_category": dict(blocker_category_counts),
        "by_hard_block_feature": dict(blocker_feature_counts),
    },
    "top_blocker_categories": [
        {
            "category": category,
            "count": count,
            "samples": sorted(set(samples_by_category[category])),
        }
        for category, count in blocker_category_counts.most_common()
    ],
    "top_hard_block_features": [
        {
            "feature": feature,
            "count": count,
            "samples": sorted(set(samples_by_feature[feature])),
        }
        for feature, count in blocker_feature_counts.most_common()
    ],
    "samples": records,
}

lines = [
    "# Temporal TS Blocker Census",
    "",
    f"- Source repo: `{repo_remote}`",
    f"- Source HEAD: `{repo_head}`",
    f"- Local snapshot: `{repo_root}`",
    f"- Samples analyzed: `{len(samples)}`",
    "",
    "## Verdict Counts",
    "",
]

for verdict, count in verdict_counts.most_common():
    lines.append(f"- `{verdict}`: `{count}`")

lines.extend(["", "## Top Blocker Categories", ""])
if blocker_category_counts:
    for category, count in blocker_category_counts.most_common():
        sample_list = ", ".join(sorted(set(samples_by_category[category]))[:8])
        lines.append(f"- `{category}`: `{count}` occurrences")
        lines.append(f"  samples: {sample_list}")
else:
    lines.append("- none")

lines.extend(["", "## Top Hard-Block Features", ""])
if blocker_feature_counts:
    for feature, count in blocker_feature_counts.most_common():
        sample_list = ", ".join(sorted(set(samples_by_feature[feature]))[:8])
        lines.append(f"- `{feature}`: `{count}` occurrences")
        lines.append(f"  samples: {sample_list}")
else:
    lines.append("- none")

lines.extend(["", "## Sample Results", ""])
for record in records:
    lines.append(f"### {record['sample']}")
    lines.append("")
    lines.append(f"- Status: `{record['status']}`")
    lines.append(f"- Verdict: `{record['verdict']}`")
    lines.append(f"- Report: `{record['report_path']}`")
    if record["blocker_categories"]:
        lines.append("- Blocker categories:")
        for group in record["blocker_categories"]:
            lines.append(f"  - `{group['category']}` x{group['count']}")
    elif record["hard_block_features"]:
        lines.append(f"- Hard-block features: {', '.join(record['hard_block_features'])}")
    else:
        lines.append("- No hard blockers")
    lines.append("")

(output_root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
(output_root / "summary.md").write_text("\n".join(lines), encoding="utf-8")
PY
