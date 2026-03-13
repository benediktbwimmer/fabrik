# Temporal TypeScript Equivalence Contract

This document freezes what “same behavior” means for the Temporal TS subset trust milestone.

## Required Equivalence Axes

For every supported subset feature, Fabrik must preserve:

- terminal outcome class
- return value shape
- failure class equivalence
- failure propagation location
- retry behavior
- handler ordering and visibility rules
- history-level replay outcome
- upgrade and versioning compatibility outcome

## Explicit Non-Requirements

Parity does not require:

- exact error string text
- exact stack trace text
- exact internal event IDs
- incidental internal timing differences that do not change workflow-visible semantics

## Replay Equivalence

Replay equivalence means:

- the same captured history reaches the same semantic outcome class
- the same workflow-visible state is reconstructed
- incompatible candidate artifacts fail deterministically with explicit diagnostics

## Failure Equivalence

Failure equivalence means:

- Fabrik may emit different internal diagnostics
- Fabrik may not change the semantic failure class or where the workflow observes that failure
