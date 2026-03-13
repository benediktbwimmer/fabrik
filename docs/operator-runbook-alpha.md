# Operator Runbook Alpha

This runbook covers the minimum operator workflow for the Temporal TypeScript design-partner alpha.

## Inputs

- migrated repo output directory
- Fabrik API URL
- tenant ID
- matching or unified runtime endpoint

## Preflight

1. Confirm `migration-report.json` status is not `incompatible_blocked`.
2. Confirm `workspace/alpha-qualification.json` is either `qualified` or `qualified_with_caveats`.
3. Review any caveats before deployment.
4. Check `workspace/deploy-manifest.json` and generated worker packages.

## Deploy

Run the migration flow with deployment enabled:

```bash
cargo run -p fabrik-cli -- migrate temporal <repo> --deploy --api-url <url> --tenant <tenant>
```

For the local primary alpha drill against the supported Temporal TS fixture, use:

```bash
scripts/run-alpha-primary-drill.sh
```

Expected deployment evidence:

- workflow artifacts published
- activity worker builds registered
- managed worker pollers detected on required task queues

## Inspect

After deployment, confirm:

- task queue pollers are present
- workflow artifacts are stored at the expected version
- migrated workflow starts successfully
- workflow list and run inspection surfaces are usable

Use these surfaces:

- workflow list
- run list
- workflow routing
- replay endpoint
- task queue inspection

## Replay and Recovery

For a candidate primary alpha workflow:

1. start the workflow
2. capture the generated migration artifacts
3. run replay validation against the pinned artifact
4. restart or fail over the owning runtime
5. confirm the workflow resumes from authoritative state
6. confirm no stale completion mutates the workflow after recovery

## Rollback Drill

For the primary alpha repo:

1. deploy the candidate artifact and worker build
2. run the representative workflow path
3. induce restart or failover
4. verify health and replay consistency
5. roll back the build or artifact candidate
6. confirm pinned runs and queue routing remain sane

## Escalate as Blocked When

- pollers never appear on a required queue
- replay validation reports divergence
- rollout validation rejects the candidate artifact
- migration output changes from `qualified_with_caveats` to `blocked`
- the workflow depends on memo/search behavior that remains out of scope
