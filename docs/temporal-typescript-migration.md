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

The current source-of-truth support matrix lives in `sdk/typescript-compiler/temporal-ts-subset-support-matrix.json`, is emitted by `sdk/typescript-compiler/migration-analyzer.mjs`, and is embedded into every `migration-report.json`.

For the design-partner alpha flow, migration also emits an explicit qualification verdict that classifies the repo as:

- `qualified`
- `qualified_with_caveats`
- `blocked`

The verdict groups blockers into the operator-facing categories used by the alpha process:

- unsupported API
- unsupported packaging/bootstrap
- unsupported visibility/search usage
- replay/trust blocker
- operational blocker

Current headline policy:

- `proxyActivities`, signals, queries, updates, conditions, child workflows, cancellation scopes, continue-as-new, and workflow evolution markers are in scope for migration
- worker bootstraps are adapter-backed only when they are static `Worker.create({ ... })` calls
- payload/data converter usage is supported only for static default-compatible `dataConverter` declarations; custom codecs and path-based converters remain blocked
- search attributes and memo are supported only for the alpha slice of static start-time values plus exact-match visibility filters
- interceptors and middleware are blocked by default
- unsupported Temporal APIs are blocked by default

Feature entries now also carry milestone-facing trust metadata:

- milestone status: `supported` or `blocked`
- feature confidence class
- support / semantic / trust / upgrade fixture coverage
- promotion requirements for moving a blocked feature into the trusted subset

## Confidence Classes

There are two confidence systems in the migration output.

Feature confidence classes in the support matrix:

- `supported`
- `supported_replay_validated`
- `supported_failover_validated`
- `supported_upgrade_validated`
- `blocked`

Workflow migration confidence classes in `compiled_workflows`:

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
- `workspace/alpha-qualification.json`
- `workspace/support-matrix.json`
- `workspace/equivalence-contract.json`
- `workspace/conformance-manifest.json`
- `workspace/trust-summary.json`
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

## Alpha Operator Docs

The design-partner alpha process is documented in:

- `docs/alpha-support-matrix.md`
- `docs/alpha-known-limitations.md`
- `docs/alpha-qualification-guide.md`
- `docs/operator-runbook-alpha.md`
