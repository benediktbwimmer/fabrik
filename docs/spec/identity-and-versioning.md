# Identity and Versioning

## Purpose

This document freezes workflow identity and execution versioning semantics.

## Canonical Identities

- `definition_id`: stable identity of a workflow type
- `definition_version`: published version within that type
- `artifact_hash`: exact compiled workflow artifact
- `instance_id`: stable logical workflow identity
- `run_id`: one concrete execution epoch
- `workflow_task_queue`: queue used for workflow task dispatch
- `activity_type`: stable identity of an activity implementation contract
- `activity_task_queue`: queue used for activity task dispatch
- `worker_build_id`: identity of one worker build

## Workflow Rules

- a new workflow start creates a new `instance_id` and an initial `run_id`
- `ContinueAsNew` preserves `instance_id` and creates a fresh `run_id`
- every event within a run must carry the same pinned `definition_version` and `artifact_hash`
- replay resolves workflow semantics from history, never from "latest"

## Worker Versioning Rules

- activity task routing must respect compatible worker build sets
- workflow runs keep their pinned workflow artifact semantics for the lifetime of the run
- worker versioning and workflow artifact versioning are independent but related rollout surfaces
- a deployment control plane must be able to answer why a task was routed to a particular worker build

## Migration Rule

- new runs may start on newer workflow artifacts
- existing runs keep prior workflow semantics unless an explicit replay-safe migration rule exists
- incompatible changes require either version markers, new task queues, or a documented rollout strategy
- activity contract changes must be safe for in-flight retries and outstanding tasks

## Visibility Rule

Operators must be able to inspect:

- the workflow artifact pinned to a run
- the task queues associated with a workflow and its activities
- the worker build identifiers that handled activity tasks
