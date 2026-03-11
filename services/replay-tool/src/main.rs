use std::{env, time::Duration};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use fabrik_broker::{BrokerConfig, WorkflowHistoryFilter, read_workflow_history};
use fabrik_config::{PostgresConfig, RedpandaConfig};
use fabrik_store::{WorkflowStateSnapshot, WorkflowStore};
use fabrik_workflow::{
    ReplayDivergence, ReplayDivergenceKind, ReplayFieldMismatch, ReplaySource,
    ReplayTransitionTraceEntry, WorkflowInstanceState, artifact_hash, first_transition_divergence,
    projection_mismatches, replay_compiled_history_trace,
    replay_compiled_history_trace_from_snapshot, replay_history_trace,
    replay_history_trace_from_snapshot, same_projection,
};
use serde::Serialize;
use uuid::Uuid;

const DEFAULT_IDLE_TIMEOUT_MS: u64 = 1_000;
const DEFAULT_MAX_SCAN_MS: u64 = 10_000;

#[derive(Debug)]
struct ReplayArgs {
    tenant_id: String,
    instance_id: String,
    run_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct ReplaySummary {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    definition_id: String,
    definition_version: u32,
    artifact_hash: String,
    event_count: usize,
    last_event_type: String,
    projection_matches_store: Option<bool>,
    replay_source: ReplaySource,
    snapshot: Option<ReplaySnapshotSummary>,
    divergence_count: usize,
    divergences: Vec<ReplayDivergence>,
    transition_count: usize,
    transition_trace: Vec<ReplayTransitionTraceEntry>,
    replayed_state: WorkflowInstanceState,
}

#[derive(Debug, Serialize)]
struct ReplaySnapshotSummary {
    run_id: String,
    snapshot_schema_version: u32,
    event_count: i64,
    last_event_id: Uuid,
    last_event_type: String,
    updated_at: DateTime<Utc>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let redpanda = RedpandaConfig::from_env()?;
    let postgres = PostgresConfig::from_env()?;
    let store = WorkflowStore::connect(&postgres.url).await?;
    store.init().await?;

    let current_instance =
        store.get_instance(&args.tenant_id, &args.instance_id).await?.ok_or_else(|| {
            anyhow::anyhow!(
                "workflow instance {} not found for tenant {}",
                args.instance_id,
                args.tenant_id
            )
        })?;
    let run_id = args.run_id.unwrap_or_else(|| current_instance.run_id.clone());

    let broker = BrokerConfig::new(
        redpanda.brokers,
        redpanda.workflow_events_topic,
        redpanda.workflow_events_partitions,
    );
    let history = read_workflow_history(
        &broker,
        "replay-tool",
        &WorkflowHistoryFilter::new(&args.tenant_id, &args.instance_id, &run_id),
        Duration::from_millis(DEFAULT_IDLE_TIMEOUT_MS),
        Duration::from_millis(DEFAULT_MAX_SCAN_MS),
    )
    .await?;

    if history.is_empty() {
        bail!(
            "no workflow history found for tenant={}, instance_id={}, run_id={run_id}",
            args.tenant_id,
            args.instance_id
        );
    }
    let first_event = history.first().context("workflow history unexpectedly empty after read")?;

    let pinned_artifact = store
        .get_artifact_version(
            &args.tenant_id,
            &first_event.definition_id,
            first_event.definition_version,
        )
        .await?;

    let full_trace = if let Some(artifact) = pinned_artifact.as_ref() {
        replay_compiled_history_trace(&history, artifact)?
    } else {
        replay_history_trace(&history)?
    };
    let definition_version = full_trace
        .final_state
        .definition_version
        .context("replayed state is missing definition_version")?;
    let pinned_artifact_hash = full_trace
        .final_state
        .artifact_hash
        .clone()
        .context("replayed state is missing artifact_hash")?;
    if let Some(artifact) = store
        .get_artifact_version(
            &args.tenant_id,
            &full_trace.final_state.definition_id,
            definition_version,
        )
        .await?
    {
        if artifact.artifact_hash != pinned_artifact_hash {
            bail!(
                "artifact hash mismatch for replayed run: history={}, artifact={}",
                pinned_artifact_hash,
                artifact.artifact_hash
            );
        }
    } else {
        let definition = store
            .get_definition_version(
                &args.tenant_id,
                &full_trace.final_state.definition_id,
                definition_version,
            )
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "workflow definition {} version {} not found for tenant {}",
                    full_trace.final_state.definition_id,
                    definition_version,
                    args.tenant_id
                )
            })?;
        let computed_artifact_hash = artifact_hash(&definition);
        if computed_artifact_hash != pinned_artifact_hash {
            bail!(
                "artifact hash mismatch for replayed run: history={}, definition={}",
                pinned_artifact_hash,
                computed_artifact_hash
            );
        }
    }

    let snapshot = store.get_snapshot_for_run(&args.tenant_id, &args.instance_id, &run_id).await?;
    let snapshot_summary = snapshot.as_ref().map(snapshot_summary);
    let mut divergences = Vec::new();
    let active_trace = if let Some(snapshot) = snapshot {
        match history.iter().position(|event| event.event_id == snapshot.last_event_id) {
            Some(index) => {
                let tail = &history[index + 1..];
                let trace = if let Some(artifact) = pinned_artifact.as_ref() {
                    replay_compiled_history_trace_from_snapshot(
                        tail,
                        &snapshot.state,
                        artifact,
                        snapshot.event_count,
                        snapshot.last_event_id,
                        &snapshot.last_event_type,
                    )?
                } else {
                    replay_history_trace_from_snapshot(
                        tail,
                        &snapshot.state,
                        snapshot.event_count,
                        snapshot.last_event_id,
                        &snapshot.last_event_type,
                    )?
                };
                let expected_tail =
                    full_trace.transitions.iter().skip(index + 1).cloned().collect::<Vec<_>>();
                if let Some(divergence) =
                    first_transition_divergence(&expected_tail, &trace.transitions)
                {
                    divergences.push(divergence);
                } else if !same_projection(&full_trace.final_state, &trace.final_state) {
                    divergences.push(ReplayDivergence {
                        kind: ReplayDivergenceKind::SnapshotMismatch,
                        event_id: history.last().map(|event| event.event_id),
                        event_type: history.last().map(|event| event.event_type.clone()),
                        message: "snapshot-backed replay produced a different final state"
                            .to_owned(),
                        fields: projection_mismatches(&full_trace.final_state, &trace.final_state),
                    });
                }
                trace
            }
            None => {
                divergences.push(ReplayDivergence {
                    kind: ReplayDivergenceKind::SnapshotMismatch,
                    event_id: Some(snapshot.last_event_id),
                    event_type: Some(snapshot.last_event_type.clone()),
                    message: "snapshot boundary event was not found in broker history".to_owned(),
                    fields: vec![ReplayFieldMismatch {
                        field: "snapshot_last_event_id".to_owned(),
                        expected: serde_json::to_value(snapshot.last_event_id)
                            .expect("snapshot event id serializes"),
                        actual: serde_json::Value::Null,
                    }],
                });
                full_trace.clone()
            }
        }
    } else {
        full_trace.clone()
    };

    let mut projection_matches_store = (current_instance.run_id == active_trace.final_state.run_id)
        .then(|| same_projection(&current_instance, &active_trace.final_state));
    if matches!(projection_matches_store, Some(false)) {
        divergences.push(ReplayDivergence {
            kind: ReplayDivergenceKind::ProjectionMismatch,
            event_id: Some(active_trace.final_state.last_event_id),
            event_type: Some(active_trace.final_state.last_event_type.clone()),
            message: "replayed final state does not match the stored workflow projection"
                .to_owned(),
            fields: projection_mismatches(&current_instance, &active_trace.final_state),
        });
    }
    if projection_matches_store.is_some() && !divergences.is_empty() {
        projection_matches_store = Some(false);
    }
    let last_event_type = history
        .last()
        .map(|event| event.event_type.clone())
        .context("workflow history unexpectedly empty after replay")?;

    let summary = ReplaySummary {
        tenant_id: args.tenant_id,
        instance_id: args.instance_id,
        run_id,
        definition_id: active_trace.final_state.definition_id.clone(),
        definition_version,
        artifact_hash: pinned_artifact_hash,
        event_count: history.len(),
        last_event_type,
        projection_matches_store,
        replay_source: active_trace.source.clone(),
        snapshot: snapshot_summary,
        divergence_count: divergences.len(),
        divergences,
        transition_count: active_trace.transitions.len(),
        transition_trace: active_trace.transitions,
        replayed_state: active_trace.final_state,
    };

    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

fn snapshot_summary(snapshot: &WorkflowStateSnapshot) -> ReplaySnapshotSummary {
    ReplaySnapshotSummary {
        run_id: snapshot.run_id.clone(),
        snapshot_schema_version: snapshot.snapshot_schema_version,
        event_count: snapshot.event_count,
        last_event_id: snapshot.last_event_id,
        last_event_type: snapshot.last_event_type.clone(),
        updated_at: snapshot.updated_at,
    }
}

fn parse_args() -> Result<ReplayArgs> {
    let mut args = env::args().skip(1);
    let tenant_id = args
        .next()
        .context("usage: cargo run -p replay-tool -- <tenant_id> <instance_id> [run_id]")?;
    let instance_id = args
        .next()
        .context("usage: cargo run -p replay-tool -- <tenant_id> <instance_id> [run_id]")?;
    let run_id = args.next();

    if args.next().is_some() {
        bail!("usage: cargo run -p replay-tool -- <tenant_id> <instance_id> [run_id]");
    }

    Ok(ReplayArgs { tenant_id, instance_id, run_id })
}
