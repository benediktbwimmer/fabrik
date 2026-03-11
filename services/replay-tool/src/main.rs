use std::{env, time::Duration};

use anyhow::{Context, Result, bail};
use fabrik_broker::{BrokerConfig, WorkflowHistoryFilter, read_workflow_history};
use fabrik_config::{PostgresConfig, RedpandaConfig};
use fabrik_store::WorkflowStore;
use fabrik_workflow::{
    WorkflowInstanceState, artifact_hash, replay_compiled_history, replay_history, same_projection,
};
use serde::Serialize;

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
    replayed_state: WorkflowInstanceState,
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

    let broker = BrokerConfig::new(redpanda.brokers, redpanda.workflow_events_topic);
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

    let pinned_artifact = if let Some(version) = current_instance.definition_version {
        store
            .get_artifact_version(&args.tenant_id, &current_instance.definition_id, version)
            .await?
    } else {
        None
    };

    let replayed_state = if let Some(artifact) = pinned_artifact {
        replay_compiled_history(&history, &artifact)?
    } else {
        replay_history(&history)?
    };
    let definition_version = replayed_state
        .definition_version
        .context("replayed state is missing definition_version")?;
    let pinned_artifact_hash =
        replayed_state.artifact_hash.clone().context("replayed state is missing artifact_hash")?;
    if let Some(artifact) = store
        .get_artifact_version(&args.tenant_id, &replayed_state.definition_id, definition_version)
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
                &replayed_state.definition_id,
                definition_version,
            )
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "workflow definition {} version {} not found for tenant {}",
                    replayed_state.definition_id,
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

    let projection_matches_store = (current_instance.run_id == replayed_state.run_id)
        .then(|| same_projection(&current_instance, &replayed_state));
    let last_event_type = history
        .last()
        .map(|event| event.event_type.clone())
        .context("workflow history unexpectedly empty after replay")?;

    let summary = ReplaySummary {
        tenant_id: args.tenant_id,
        instance_id: args.instance_id,
        run_id,
        definition_id: replayed_state.definition_id.clone(),
        definition_version,
        artifact_hash: pinned_artifact_hash,
        event_count: history.len(),
        last_event_type,
        projection_matches_store,
        replayed_state,
    };

    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
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
