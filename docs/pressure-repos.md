# Pressure Repos

These repos are internal engineering pressure fixtures, not customer evidence.

They exist to exercise the remaining trust and packaging surfaces once the official Temporal TypeScript samples stop being the main source of pressure.

## Current Set

- `crates/fabrik-cli/test-fixtures/temporal-interceptor-pressure`
  - purpose: static `interceptors.workflowModules` bridge with a real worker bootstrap, workflow-owned query state, and activity-backed publish path
  - passing drill bundle: `target/alpha-drills/temporal-interceptor-pressure/interceptor-pressure-drill-report.json`
- `crates/fabrik-cli/test-fixtures/temporal-sinks-pressure`
  - purpose: sink bridge behavior with worker sink injection and workflow-side `proxySinks()`
  - passing drill bundle: `target/alpha-drills/temporal-sinks-pressure/sinks-pressure-drill-report.json`
- `crates/fabrik-cli/test-fixtures/temporal-converter-trust-pressure`
  - purpose: static `dataConverter` factory plus static `payloadConverterPath` packaging and replay pressure
  - passing drill bundle: `target/alpha-drills/temporal-converter-trust-pressure/converter-pressure-drill-report.json`
  - caveat: pre-normalize query assertions currently validate only workflow phase, not copied input-field reflection
- `crates/fabrik-cli/test-fixtures/temporal-monorepo-multiworker-pressure`
  - purpose: multi-worker monorepo packaging, workspace resolution, and tsconfig-path handling
- `crates/fabrik-cli/test-fixtures/temporal-versioning-upgrade-pressure`
  - purpose: versioning behavior, patch markers, and mixed-build drill preparation
  - passing drill bundle: `target/alpha-drills/temporal-versioning-upgrade-pressure/versioning-pressure-mixed-build-drill-report.json`

## How To Run

Qualify the full set with:

```bash
scripts/run-pressure-repo-qualification.sh
```

This writes results to:

- `target/pressure-repo-qualification/summary.json`
- `target/pressure-repo-qualification/summary.md`

Use these repos for engineering coverage and trust-hardening work. Do not present them as external design-partner proof.
