# Security and Tenancy

## Purpose

This document freezes the security and tenant-isolation contract.

## Tenant Scope

Every API, event, task, snapshot, visibility record, and query is tenant-scoped.

## Artifact Trust Model

- workflow definitions and artifacts are publishable only by authorized principals
- artifact metadata is auditable
- running executions are pinned to trusted published artifacts

## Worker Trust Model

- activity workers must authenticate before polling task queues or completing tasks
- worker identities and build identifiers must be auditable
- hosted worker models and bring-your-own-worker models must have explicit trust boundaries

## Secret Model

- secrets are not stored inside workflow definitions or workflow history
- activities and connectors resolve secrets through tenant-scoped references or runtime injection
- secret access is capability-controlled and auditable

## Isolation Model

- one tenant's workflow or activity load must not collapse shared control planes
- rate limits and quotas must be enforceable per tenant and per task queue
- worker-facing credentials must be revocable without rewriting workflow history

## Access Control

The platform must define who can:

- publish workflow definitions
- start workflows
- signal workflows
- query workflows
- submit updates
- cancel or terminate workflows
- inspect payloads
- replay histories
- register workers
- deploy new worker builds
