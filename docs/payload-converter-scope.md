# Payload Converter Scope

This document defined the scoped payload/data-converter project for the Temporal TypeScript alpha path.

Status:

- static default-compatible `payloadConverterPath` support is now implemented
- the path-based adapter slice now has deploy/restart/rollback evidence via [`scripts/run-alpha-mixed-build-drill.sh`](/Users/bene/code/fabrik/scripts/run-alpha-mixed-build-drill.sh)
- broader custom converter parity remains out of scope

## Named Blocker Repo

- Repo: [`crates/fabrik-cli/test-fixtures/temporal-payload-blocked`](/Users/bene/code/fabrik/crates/fabrik-cli/test-fixtures/temporal-payload-blocked)
- Current qualification: [`target/shadow-qualification/temporal-payload-blocked/migration-report.json`](/Users/bene/code/fabrik/target/shadow-qualification/temporal-payload-blocked/migration-report.json)

## Exact Blocked Pattern

The current blocker is not “all custom converters.”
It is one narrow worker-side pattern:

- static `Worker.create({ ... })`
- `dataConverter: { payloadConverterPath: "./src/custom-payload-converter.ts" }`
- the target module exports `payloadConverter`
- the target module currently constructs `new DefaultPayloadConverter()`
- no payload codecs
- no codec server
- no failure converter override
- no client-side `WorkflowClient({ dataConverter })`
- no signal/update/query-side custom converter wiring

That means the next project should be:

- support static worker `payloadConverterPath` only when the referenced module is provably default-compatible
- this exact path-based slice is now the next supported adapter target to keep narrow

It should not be:

- general custom converter parity
- codec parity
- encrypted/compressed payload parity
- broad client/worker/workflow converter parity

## Why This Is Worth Doing

- It unblocks a real named blocked repo.
- It stays close to the existing alpha-safe slice, because the converter behavior remains equivalent to Temporal default conversion.
- It removes a migration footgun where repos are blocked only because the converter is expressed through a module path instead of an inline object.

## Proposed Supported Slice

The scoped project should support only this shape:

- `dataConverter.payloadConverterPath` is a static string literal
- the referenced module resolves inside the repo
- the module exports `payloadConverter`
- the exported converter is one of:
  - `defaultPayloadConverter`
  - `new DefaultPayloadConverter()`
  - an identifier bound to one of those static expressions
- the worker does not also set:
  - `payloadCodec`
  - `payloadCodecs`
  - `codecServer`
  - `failureConverterPath`
  - non-default `dataConverter` fields outside the supported slice

## Explicitly Out Of Scope

- `PayloadCodec` support
- encryption/compression codecs
- protobuf-backed converter variants
- arbitrary `CompositePayloadConverter(...)`
- custom `PayloadConverterWithEncoding`
- client-side data converter injection
- signal/update/query payload conversion parity
- failure converter parity
- visibility payload re-encoding

## Required Implementation Work

1. Analyzer
- Accept static `payloadConverterPath` only for the supported module shape.
- Keep emitting a hard block for anything outside that shape.

2. Packaging
- Resolve and transpile the referenced converter module into the worker package.
- Record converter packaging metadata in the worker manifest.

3. Managed worker bootstrap
- Load the supported converter module in the generated bootstrap.
- Use it only as an adapter for managed activity execution.
- Fail clearly if the module does not export the expected binding.

4. Trust
- Add a support fixture for path-based default-compatible converter modules.
- Add at least one deploy/restart/rollback drill using that fixture.
- Do not promote the feature above `supported` until replay/failover evidence exists for the exact slice.

## Acceptance Criteria

The scoped project is done when all of the following are true:

- `temporal-payload-blocked` can be re-expressed or replaced by a qualifying fixture that uses `payloadConverterPath`
- analyzer verdict becomes `qualified_with_caveats` or better for that supported path-based shape
- migration packaging succeeds without manual repo edits
- one live alpha drill passes for the path-based shape
- custom codecs and broader custom converter shapes still remain blocked

## Stop Conditions

Stop and rescope if any of these become necessary:

- client-side converter injection
- non-default wire encodings
- signal/update/query converter parity
- failure converter compatibility
- workflow-history payload re-encoding beyond the current JSON/CBOR path

If one of those appears, this is no longer a narrow adapter project. It becomes a new runtime payload-compatibility track.
