# Temporal TypeScript Migration Policy

`fabrik migrate temporal <local_repo_path>` is a strict, CLI-first migration path for existing Temporal TypeScript repos.

The migration contract is split across three layers:

- source compatibility: Temporal TS workflow and worker source can be analyzed and compiled without editing the user repo
- deployment compatibility: Fabrik can package artifacts and worker metadata with equivalent queue/build assumptions
- semantic compatibility: replay-critical workflow behavior remains within Fabrik's supported compiled subset

## Default Behavior

- local repo import only
- generated adapters and wrappers are allowed
- user source is never edited
- deploy is opt-in with `--deploy`
- unsupported constructs block early instead of degrading silently

## Support Matrix

The current source-of-truth support matrix is emitted by `sdk/typescript-compiler/migration-analyzer.mjs` and embedded into every `migration-report.json`.

Current headline policy:

- `proxyActivities`, signals, queries, updates, conditions, child workflows, cancellation scopes, continue-as-new, and workflow evolution markers are in scope for migration
- worker bootstraps are adapter-backed only when they are static `Worker.create({ ... })` calls
- payload/data converter usage is blocked by default
- search attributes and memo dependent apps are blocked by default
- interceptors and middleware are blocked by default
- unsupported Temporal APIs are blocked by default

## Confidence Classes

Each workflow receives the highest confidence class it has actually earned:

- `source_compatible`
- `deploy_compatible`
- `replay_validated`
- `production_ready`

`production_ready` requires compile success, package success, replay validation, and successful deploy validation.

## Generated Outputs

Every migration run writes:

- `migration-report.json`
- `migration-report.md`
- `workspace/analysis.json`
- `workspace/support-matrix.json`
- `workspace/deploy-manifest.json`
- compiled workflow artifacts under `artifacts/`
- executable worker packages under `workers/`

These outputs are deterministic and inspectable. They are the contract another tool or operator can use to review the migration before rollout.

## Managed Worker Launch

When `--deploy` is used, the migration CLI now:

- publishes compiled workflow artifacts through the normal API
- registers activity worker builds on the target Fabrik activity task queues
- launches managed `activity-worker-service` pollers with the generated Node bootstrap bundle
- waits for queue poller presence before returning `compatible_deployed`

Relevant environment variables:

- `FABRIK_API_URL`
- `FABRIK_TENANT_ID`
- `FABRIK_MATCHING_ENDPOINT`
- `FABRIK_ACTIVITY_WORKER_BIN` to override the worker executable path
- `FABRIK_ACTIVITY_POLLER_WAIT_MS` to tune poller wait time
