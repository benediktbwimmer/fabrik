use anyhow::Result;
use chrono::Utc;
use fabrik_broker::{BrokerConfig, WorkflowPublisher};
use fabrik_config::{HttpServiceConfig, PostgresConfig, RedpandaConfig};
use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::{ScheduledTimer, WorkflowStore};
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    let config = HttpServiceConfig::from_env("TIMER_SERVICE", "timer-service", 3003)?;
    let redpanda = RedpandaConfig::from_env()?;
    let postgres = PostgresConfig::from_env()?;
    init_tracing(&config.log_filter);
    info!(port = config.port, "starting timer service");

    let broker = BrokerConfig::new(redpanda.brokers, redpanda.workflow_events_topic);
    let publisher = WorkflowPublisher::new(&broker, "timer-service").await?;
    let store = WorkflowStore::connect(&postgres.url).await?;
    store.init().await?;

    tokio::spawn(run_timer_loop(store.clone(), publisher));

    let app =
        default_router::<()>(ServiceInfo::new(config.name, "timer", env!("CARGO_PKG_VERSION")));

    serve(app, config.port).await
}

async fn run_timer_loop(store: WorkflowStore, publisher: WorkflowPublisher) {
    let interval = std::time::Duration::from_millis(250);

    loop {
        match store.claim_due_timers(Utc::now(), 100).await {
            Ok(timers) => {
                for timer in timers {
                    if let Err(error) = dispatch_timer(&store, &publisher, timer).await {
                        error!(error = %error, "failed to dispatch timer");
                    }
                }
            }
            Err(error) => error!(error = %error, "failed to load due timers"),
        }

        tokio::time::sleep(interval).await;
    }
}

async fn dispatch_timer(
    store: &WorkflowStore,
    publisher: &WorkflowPublisher,
    timer: ScheduledTimer,
) -> Result<()> {
    if !store.mark_timer_dispatched(&timer.tenant_id, &timer.instance_id, &timer.timer_id).await? {
        return Ok(());
    }

    let definition_version = timer
        .definition_version
        .ok_or_else(|| anyhow::anyhow!("timer {} missing definition_version", timer.timer_id))?;
    let artifact_hash = timer
        .artifact_hash
        .clone()
        .ok_or_else(|| anyhow::anyhow!("timer {} missing artifact_hash", timer.timer_id))?;

    let mut envelope = EventEnvelope::new(
        WorkflowEvent::TimerFired { timer_id: timer.timer_id.clone() }.event_type(),
        WorkflowIdentity::new(
            timer.tenant_id.clone(),
            timer.definition_id.clone(),
            definition_version,
            artifact_hash,
            timer.instance_id.clone(),
            timer.run_id.clone(),
            "timer-service",
        ),
        WorkflowEvent::TimerFired { timer_id: timer.timer_id.clone() },
    );
    envelope.causation_id = Some(timer.scheduled_event_id);
    envelope.correlation_id = timer.correlation_id;
    if let Some(state) = timer.state.clone() {
        envelope.metadata.insert("state".to_owned(), state);
    }
    envelope.metadata.insert("fire_at".to_owned(), timer.fire_at.to_rfc3339());
    envelope.dedupe_key = Some(format!(
        "{}:{}:{}:{}",
        timer.tenant_id, timer.instance_id, timer.run_id, timer.timer_id
    ));

    if let Err(error) = publisher.publish(&envelope, &envelope.partition_key).await {
        store.reset_timer_dispatch(&timer.tenant_id, &timer.instance_id, &timer.timer_id).await?;
        return Err(error);
    }

    Ok(())
}
