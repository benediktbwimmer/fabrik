# Security and Tenancy

## Purpose

This document freezes the security and tenant-isolation contract.

## Tenant Scope

Every API, event, snapshot, and query is tenant-scoped.

## Artifact Trust Model

- workflow definitions and artifacts are publishable only by authorized principals
- artifact metadata is auditable
- running executions are pinned to trusted published artifacts

## Secret Model

- secrets are not stored inside workflow definitions
- connectors resolve secrets through tenant-scoped secret references
- secret access is capability-controlled

## Wasm Capability Model

- Wasm modules are untrusted by default
- host capabilities are explicitly granted
- resource limits are enforced per tenant and per execution

## Access Control

The platform must define who can:

- publish definitions
- start workflows
- signal workflows
- inspect payloads
- replay histories
- deploy new artifacts
