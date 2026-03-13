# Alpha Shadow Qualification

This document records the current shadow-qualification stance for the workspace Temporal TypeScript fixtures.

## Current Decision

- `temporal-shadow-qualified`: treat as the `shadow_target`
  This is the deployable shadow repo for the current alpha. It exercises static-evaluable memo/search-attribute usage inside the supported subset.
- `temporal-visibility-blocked`: treat as `fixed_now`
  Static-evaluable start-time `memo` and `searchAttributes` shapes now qualify for the alpha slice, so this fixture is no longer a blocker.
- `temporal-payload-qualified`: treat as `fixed_now`
  Static default-compatible `dataConverter` declarations now qualify for the alpha payload adapter slice.
- `temporal-payload-path-qualified`: treat as `fixed_now`
  Static default-compatible `payloadConverterPath` modules now qualify for the alpha payload adapter slice.
- `temporal-payload-blocked`: treat as `explicit_alpha_limitation`
  Custom payload/data converter behavior remains out of scope for the current alpha.
- `temporal-dynamic-bootstrap-blocked`: treat as `explicit_alpha_limitation`
  Dynamic `Worker.create(...)` bootstrap shapes remain out of scope for the current alpha.
- `temporal-unsupported-api-blocked`: treat as `explicit_alpha_limitation`
  Unsupported Temporal workflow APIs remain blocked until they are explicitly implemented and trusted.

## How To Refresh

Run:

```bash
scripts/run-shadow-qualification.sh
```

For the deployable shadow workflow path, run:

```bash
scripts/run-alpha-shadow-drill.sh --skip-stack
```

The script writes per-fixture reports plus:

- `target/shadow-qualification/summary.json`
- `target/shadow-qualification/summary.md`

## Why This Exists

The primary alpha drill proves one representative workflow path can migrate, run, replay, restart, and roll back cleanly.
The shadow qualification path exists to keep the alpha boundary honest:

- fix conservative blockers that are already inside the intended subset
- freeze the remaining blocked categories as explicit alpha limitations instead of silently widening scope
