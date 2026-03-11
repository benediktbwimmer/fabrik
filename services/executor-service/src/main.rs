use anyhow::Result;
use fabrik_broker::{
    BrokerConfig, WorkflowPublisher, build_workflow_consumer, decode_workflow_event,
};
use fabrik_config::{HttpServiceConfig, PostgresConfig, RedpandaConfig};
use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::WorkflowStore;
use fabrik_workflow::{
    ArtifactExecutionState, CompiledExecutionPlan, CompiledWorkflowArtifact, ExecutionEmission,
    WorkflowInstanceState,
};
use futures_util::StreamExt;
use serde_json::Value;
use tracing::{error, info, warn};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    let config = HttpServiceConfig::from_env("EXECUTOR_SERVICE", "executor-service", 3002)?;
    let redpanda = RedpandaConfig::from_env()?;
    let postgres = PostgresConfig::from_env()?;
    init_tracing(&config.log_filter);
    info!(port = config.port, "starting executor service");

    let broker = BrokerConfig::new(redpanda.brokers, redpanda.workflow_events_topic);
    let publisher = WorkflowPublisher::new(&broker, "executor-service").await?;
    let consumer = build_workflow_consumer(&broker, "executor-service").await?;
    let store = WorkflowStore::connect(&postgres.url).await?;
    store.init().await?;

    tokio::spawn(run_executor_loop(consumer, publisher, store.clone()));

    let app =
        default_router::<()>(ServiceInfo::new(config.name, "executor", env!("CARGO_PKG_VERSION")));

    serve(app, config.port).await
}

async fn run_executor_loop(
    consumer: rskafka::client::consumer::StreamConsumer,
    publisher: WorkflowPublisher,
    store: WorkflowStore,
) {
    let mut stream = consumer;

    while let Some(message) = stream.next().await {
        match message {
            Ok((record, _high_watermark)) => match decode_workflow_event(&record) {
                Ok(event) => {
                    if let Err(error) = process_event(&store, &publisher, event).await {
                        error!(error = %error, "failed to process workflow event");
                    }
                }
                Err(error) => warn!(error = %error, "skipping invalid workflow event"),
            },
            Err(error) => error!(error = %error, "executor consumer error"),
        }
    }
}

async fn process_event(
    store: &WorkflowStore,
    publisher: &WorkflowPublisher,
    event: EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    if !store.mark_event_processed(&event).await? {
        return Ok(());
    }

    let mut state = match store.get_instance(&event.tenant_id, &event.instance_id).await? {
        Some(state) if state.run_id != event.run_id => {
            if matches!(event.payload, WorkflowEvent::WorkflowContinuedAsNew { .. }) {
                let deleted = store
                    .delete_timers_for_run(&event.tenant_id, &event.instance_id, &event.run_id)
                    .await?;
                info!(
                    workflow_instance_id = %event.instance_id,
                    stale_run_id = %event.run_id,
                    active_run_id = %state.run_id,
                    deleted_timers = deleted,
                    "processed stale continue-as-new marker for timer cleanup"
                );
                return Ok(());
            }
            if is_run_initialization_event(&event.payload) {
                WorkflowInstanceState::try_from(&event)?
            } else {
                warn!(
                    event_type = %event.event_type,
                    workflow_instance_id = %event.instance_id,
                    stale_run_id = %event.run_id,
                    active_run_id = %state.run_id,
                    "dropping stale event for non-active workflow run"
                );
                return Ok(());
            }
        }
        Some(mut state) => {
            state.apply_event(&event);
            state
        }
        None => match WorkflowInstanceState::try_from(&event) {
            Ok(state) => state,
            Err(_) => {
                warn!(
                    event_type = %event.event_type,
                    workflow_instance_id = %event.instance_id,
                    "dropping non-initial event without existing workflow state"
                );
                return Ok(());
            }
        },
    };

    store.upsert_instance(&state).await?;

    match &event.payload {
        WorkflowEvent::WorkflowTriggered { .. } => {
            if let Some(artifact) = load_pinned_artifact(store, &event, &state).await? {
                publish_artifact_pinned(publisher, &event).await?;
                let input = state.context.clone().unwrap_or(Value::Null);
                let plan = artifact.execute_trigger(&input)?;
                apply_compiled_plan(&mut state, &plan);
                store.upsert_instance(&state).await?;
                publish_compiled_plan(publisher, &event, &artifact, plan).await?;
                return Ok(());
            }

            let definition = match store
                .get_definition_version(
                    &event.tenant_id,
                    &event.definition_id,
                    event.definition_version,
                )
                .await?
            {
                Some(definition) => definition,
                None => {
                    publish_failure(
                        publisher,
                        &event,
                        Some(event.definition_version),
                        format!(
                            "workflow definition {} version {} not found for tenant {}",
                            event.definition_id, event.definition_version, event.tenant_id
                        ),
                        None,
                    )
                    .await?;
                    return Ok(());
                }
            };

            publish_artifact_pinned(publisher, &event).await?;

            let input = state.context.clone().unwrap_or(Value::Null);
            let plan = match definition.execute_trigger(&input) {
                Ok(plan) => plan,
                Err(error) => {
                    publish_failure(
                        publisher,
                        &event,
                        Some(event.definition_version),
                        error.to_string(),
                        Some(definition.initial_state.clone()),
                    )
                    .await?;
                    return Ok(());
                }
            };

            publish_plan(publisher, &event, event.definition_version, plan).await?;
        }
        WorkflowEvent::TimerScheduled { timer_id, fire_at } => {
            store
                .upsert_timer(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    &state.definition_id,
                    state.definition_version,
                    state.artifact_hash.as_deref(),
                    timer_id,
                    state.current_state.as_deref(),
                    *fire_at,
                    event.event_id,
                    event.correlation_id,
                )
                .await?;
        }
        WorkflowEvent::TimerFired { timer_id } => {
            store.delete_timer(&event.tenant_id, &event.instance_id, timer_id).await?;

            if let Some(artifact) = load_pinned_artifact(store, &event, &state).await? {
                if let Some((step_id, attempt)) = parse_retry_timer_id(timer_id) {
                    let input = artifact
                        .step_details(
                            &step_id,
                            state
                                .artifact_execution
                                .as_ref()
                                .unwrap_or(&ArtifactExecutionState::default()),
                        )?
                        .2;
                    state.context = Some(input);
                    state.current_state = Some(step_id.clone());
                    store.upsert_instance(&state).await?;
                    let emission = ExecutionEmission {
                        event: WorkflowEvent::StepScheduled {
                            step_id: step_id.clone(),
                            attempt,
                            input: state.context.clone().unwrap_or(Value::Null),
                        },
                        state: Some(step_id.clone()),
                    };
                    publish_plan(
                        publisher,
                        &event,
                        artifact.definition_version,
                        fabrik_workflow::WorkflowExecutionPlan {
                            workflow_version: artifact.definition_version,
                            final_state: step_id.clone(),
                            emissions: vec![emission],
                        },
                    )
                    .await?;
                    return Ok(());
                }

                let wait_state = match state.current_state.clone() {
                    Some(wait_state) => wait_state,
                    None => {
                        warn!(
                            workflow_instance_id = %event.instance_id,
                            timer_id = %timer_id,
                            "timer fired without current_state"
                        );
                        return Ok(());
                    }
                };
                let plan = artifact.execute_after_timer(
                    &wait_state,
                    timer_id,
                    state.artifact_execution.clone().unwrap_or_default(),
                )?;
                apply_compiled_plan(&mut state, &plan);
                store.upsert_instance(&state).await?;
                publish_compiled_plan(publisher, &event, &artifact, plan).await?;
                return Ok(());
            }

            if let Some((step_id, attempt)) = parse_retry_timer_id(timer_id) {
                let definition = load_pinned_definition(store, &event, &state).await?;
                let input =
                    state.context.clone().or_else(|| state.input.clone()).unwrap_or(Value::Null);
                let emission = ExecutionEmission {
                    event: WorkflowEvent::StepScheduled {
                        step_id: step_id.clone(),
                        attempt,
                        input,
                    },
                    state: Some(step_id),
                };
                publish_plan(
                    publisher,
                    &event,
                    definition.version,
                    fabrik_workflow::WorkflowExecutionPlan {
                        workflow_version: definition.version,
                        final_state: state.current_state.clone().unwrap_or_default(),
                        emissions: vec![emission],
                    },
                )
                .await?;
                return Ok(());
            }

            let wait_state = match state.current_state.clone() {
                Some(wait_state) => wait_state,
                None => {
                    warn!(
                        workflow_instance_id = %event.instance_id,
                        timer_id = %timer_id,
                        "timer fired without current_state"
                    );
                    return Ok(());
                }
            };

            let definition = load_pinned_definition(store, &event, &state).await?;
            let input =
                state.context.clone().or_else(|| state.input.clone()).unwrap_or(Value::Null);

            let plan = match definition.execute_after_timer(&wait_state, timer_id, &input) {
                Ok(plan) => plan,
                Err(error) => {
                    publish_failure(
                        publisher,
                        &event,
                        Some(definition.version),
                        error.to_string(),
                        Some(wait_state),
                    )
                    .await?;
                    return Ok(());
                }
            };

            publish_plan(publisher, &event, definition.version, plan).await?;
        }
        WorkflowEvent::StepCompleted { step_id, attempt: _, output } => {
            if let Some(artifact) = load_pinned_artifact(store, &event, &state).await? {
                let step_state = match state.current_state.clone() {
                    Some(step_state) => step_state,
                    None => {
                        warn!(
                            workflow_instance_id = %event.instance_id,
                            step_id = %step_id,
                            "step completed without current_state"
                        );
                        return Ok(());
                    }
                };
                let plan = artifact.execute_after_step_completion(
                    &step_state,
                    step_id,
                    output,
                    state.artifact_execution.clone().unwrap_or_default(),
                )?;
                apply_compiled_plan(&mut state, &plan);
                store.upsert_instance(&state).await?;
                publish_compiled_plan(publisher, &event, &artifact, plan).await?;
                return Ok(());
            }

            let step_state = match state.current_state.clone() {
                Some(step_state) => step_state,
                None => {
                    warn!(
                        workflow_instance_id = %event.instance_id,
                        step_id = %step_id,
                        "step completed without current_state"
                    );
                    return Ok(());
                }
            };

            let definition = load_pinned_definition(store, &event, &state).await?;
            let plan = match definition.execute_after_step_completion(&step_state, step_id, output)
            {
                Ok(plan) => plan,
                Err(error) => {
                    publish_failure(
                        publisher,
                        &event,
                        Some(definition.version),
                        error.to_string(),
                        Some(step_state),
                    )
                    .await?;
                    return Ok(());
                }
            };

            publish_plan(publisher, &event, definition.version, plan).await?;
        }
        WorkflowEvent::StepFailed { step_id, attempt, error: step_error } => {
            if let Some(artifact) = load_pinned_artifact(store, &event, &state).await? {
                let step_state = match state.current_state.clone() {
                    Some(step_state) => step_state,
                    None => {
                        warn!(
                            workflow_instance_id = %event.instance_id,
                            step_id = %step_id,
                            "step failed without current_state"
                        );
                        return Ok(());
                    }
                };

                match artifact.step_retry(&step_state)? {
                    Some(retry_policy) if *attempt < retry_policy.max_attempts => {
                        let retry = chrono::Utc::now()
                            + fabrik_workflow::parse_timer_ref(&retry_policy.delay)?;
                        let retry_timer_id = build_retry_timer_id(step_id, attempt + 1);
                        let emission = ExecutionEmission {
                            event: WorkflowEvent::TimerScheduled {
                                timer_id: retry_timer_id,
                                fire_at: retry,
                            },
                            state: Some(step_state.clone()),
                        };
                        publish_plan(
                            publisher,
                            &event,
                            artifact.definition_version,
                            fabrik_workflow::WorkflowExecutionPlan {
                                workflow_version: artifact.definition_version,
                                final_state: step_state,
                                emissions: vec![emission],
                            },
                        )
                        .await?;
                    }
                    _ => {
                        let plan = artifact.execute_after_step_failure(
                            &step_state,
                            step_id,
                            step_error,
                            state.artifact_execution.clone().unwrap_or_default(),
                        )?;
                        apply_compiled_plan(&mut state, &plan);
                        store.upsert_instance(&state).await?;
                        publish_compiled_plan(publisher, &event, &artifact, plan).await?;
                    }
                }
                return Ok(());
            }

            let step_state = match state.current_state.clone() {
                Some(step_state) => step_state,
                None => {
                    warn!(
                        workflow_instance_id = %event.instance_id,
                        step_id = %step_id,
                        "step failed without current_state"
                    );
                    return Ok(());
                }
            };

            let definition = load_pinned_definition(store, &event, &state).await?;
            match definition.schedule_retry(&step_state, *attempt) {
                Ok(Some(retry)) => {
                    let retry_timer_id = build_retry_timer_id(step_id, retry.attempt);
                    let emission = ExecutionEmission {
                        event: WorkflowEvent::TimerScheduled {
                            timer_id: retry_timer_id,
                            fire_at: retry.fire_at,
                        },
                        state: Some(step_state),
                    };
                    publish_plan(
                        publisher,
                        &event,
                        definition.version,
                        fabrik_workflow::WorkflowExecutionPlan {
                            workflow_version: definition.version,
                            final_state: state.current_state.clone().unwrap_or_default(),
                            emissions: vec![emission],
                        },
                    )
                    .await?;
                }
                Ok(None) => {
                    publish_failure(
                        publisher,
                        &event,
                        Some(definition.version),
                        step_error.clone(),
                        Some(step_state),
                    )
                    .await?;
                }
                Err(error) => {
                    publish_failure(
                        publisher,
                        &event,
                        Some(definition.version),
                        error.to_string(),
                        Some(step_state),
                    )
                    .await?;
                }
            }
        }
        WorkflowEvent::SignalReceived { signal_type, payload } => {
            if let Some(artifact) = load_pinned_artifact(store, &event, &state).await? {
                let wait_state = match state.current_state.clone() {
                    Some(wait_state) => wait_state,
                    None => {
                        warn!(
                            workflow_instance_id = %event.instance_id,
                            signal_type = %signal_type,
                            "signal received without current_state"
                        );
                        return Ok(());
                    }
                };
                let plan = artifact.execute_after_signal(
                    &wait_state,
                    signal_type,
                    payload,
                    state.artifact_execution.clone().unwrap_or_default(),
                )?;
                apply_compiled_plan(&mut state, &plan);
                store.upsert_instance(&state).await?;
                publish_compiled_plan(publisher, &event, &artifact, plan).await?;
                return Ok(());
            }

            let wait_state = match state.current_state.clone() {
                Some(wait_state) => wait_state,
                None => {
                    warn!(
                        workflow_instance_id = %event.instance_id,
                        signal_type = %signal_type,
                        "signal received without current_state"
                    );
                    return Ok(());
                }
            };

            let definition = load_pinned_definition(store, &event, &state).await?;
            let plan = match definition.execute_after_signal(&wait_state, signal_type, payload) {
                Ok(plan) => plan,
                Err(error) => {
                    publish_failure(
                        publisher,
                        &event,
                        Some(definition.version),
                        error.to_string(),
                        Some(wait_state),
                    )
                    .await?;
                    return Ok(());
                }
            };

            publish_plan(publisher, &event, definition.version, plan).await?;
        }
        WorkflowEvent::WorkflowContinuedAsNew { .. } => {
            let deleted = store
                .delete_timers_for_run(&event.tenant_id, &event.instance_id, &event.run_id)
                .await?;
            info!(
                workflow_instance_id = %event.instance_id,
                run_id = %event.run_id,
                deleted_timers = deleted,
                "continued workflow as new run"
            );
        }
        WorkflowEvent::WorkflowArtifactPinned | WorkflowEvent::WorkflowStarted => {}
        _ => {}
    }

    Ok(())
}

async fn load_pinned_definition(
    store: &WorkflowStore,
    event: &EventEnvelope<WorkflowEvent>,
    state: &WorkflowInstanceState,
) -> Result<fabrik_workflow::WorkflowDefinition> {
    if let Some(version) = state.definition_version {
        if let Some(definition) =
            store.get_definition_version(&event.tenant_id, &state.definition_id, version).await?
        {
            return Ok(definition);
        }
    }

    store.get_latest_definition(&event.tenant_id, &state.definition_id).await?.ok_or_else(|| {
        anyhow::anyhow!(
            "workflow definition {} not found for tenant {}",
            state.definition_id,
            event.tenant_id
        )
    })
}

async fn load_pinned_artifact(
    store: &WorkflowStore,
    event: &EventEnvelope<WorkflowEvent>,
    state: &WorkflowInstanceState,
) -> Result<Option<CompiledWorkflowArtifact>> {
    if let Some(version) = state.definition_version {
        if let Some(artifact) =
            store.get_artifact_version(&event.tenant_id, &state.definition_id, version).await?
        {
            return Ok(Some(artifact));
        }
    }

    Ok(None)
}

async fn publish_plan(
    publisher: &WorkflowPublisher,
    event: &EventEnvelope<WorkflowEvent>,
    workflow_version: u32,
    plan: fabrik_workflow::WorkflowExecutionPlan,
) -> Result<()> {
    let key = event.partition_key.clone();
    let correlation_id = event.correlation_id.or(Some(event.event_id));
    let mut previous_causation_id = event.event_id;

    for emission in plan.emissions {
        let mut envelope = build_workflow_envelope(
            event,
            previous_causation_id,
            correlation_id,
            workflow_version,
            emission,
        );
        if matches!(envelope.payload, WorkflowEvent::WorkflowCompleted { .. }) {
            envelope.metadata.insert("final_state".to_owned(), plan.final_state.clone());
        }
        previous_causation_id = envelope.event_id;
        publisher.publish(&envelope, &key).await?;
    }

    Ok(())
}

async fn publish_failure(
    publisher: &WorkflowPublisher,
    event: &EventEnvelope<WorkflowEvent>,
    _workflow_version: Option<u32>,
    reason: String,
    state: Option<String>,
) -> Result<()> {
    let key = event.partition_key.clone();
    let mut failed = EventEnvelope::new(
        WorkflowEvent::WorkflowFailed { reason: reason.clone() }.event_type(),
        source_identity(event, "executor-service"),
        WorkflowEvent::WorkflowFailed { reason },
    );
    failed.causation_id = Some(event.event_id);
    failed.correlation_id = event.correlation_id.or(Some(event.event_id));
    if let Some(state) = state {
        failed.metadata.insert("state".to_owned(), state);
    }
    publisher.publish(&failed, &key).await?;
    Ok(())
}

async fn publish_compiled_plan(
    publisher: &WorkflowPublisher,
    event: &EventEnvelope<WorkflowEvent>,
    artifact: &CompiledWorkflowArtifact,
    plan: CompiledExecutionPlan,
) -> Result<()> {
    let key = event.partition_key.clone();
    let correlation_id = event.correlation_id.or(Some(event.event_id));
    let mut previous_causation_id = event.event_id;

    for emission in plan.emissions {
        let mut identity = source_identity(event, "executor-service");
        identity.definition_version = artifact.definition_version;
        identity.artifact_hash = artifact.artifact_hash.clone();
        let event_payload = emission.event.clone();
        if let WorkflowEvent::WorkflowContinuedAsNew { new_run_id, input } = &event_payload.clone()
        {
            let mut envelope =
                EventEnvelope::new(event_payload.event_type(), identity.clone(), event_payload);
            envelope.causation_id = Some(previous_causation_id);
            envelope.correlation_id = correlation_id;
            if let Some(state) = emission.state.clone() {
                envelope.metadata.insert("state".to_owned(), state);
            }
            publisher.publish(&envelope, &key).await?;

            let trigger_payload = WorkflowEvent::WorkflowTriggered { input: input.clone() };
            let mut trigger = EventEnvelope::new(
                trigger_payload.event_type(),
                WorkflowIdentity::new(
                    event.tenant_id.clone(),
                    event.definition_id.clone(),
                    artifact.definition_version,
                    artifact.artifact_hash.clone(),
                    event.instance_id.clone(),
                    new_run_id.clone(),
                    "executor-service",
                ),
                trigger_payload,
            );
            trigger.causation_id = Some(envelope.event_id);
            trigger.correlation_id = correlation_id;
            publisher.publish(&trigger, &trigger.partition_key).await?;
            previous_causation_id = trigger.event_id;
            continue;
        }

        let mut envelope = EventEnvelope::new(event_payload.event_type(), identity, event_payload);
        envelope.causation_id = Some(previous_causation_id);
        envelope.correlation_id = correlation_id;
        if let Some(state) = emission.state {
            envelope.metadata.insert("state".to_owned(), state);
        }
        previous_causation_id = envelope.event_id;
        publisher.publish(&envelope, &key).await?;
    }

    Ok(())
}

fn build_retry_timer_id(step_id: &str, attempt: u32) -> String {
    format!("retry:{step_id}:{attempt}")
}

fn parse_retry_timer_id(timer_id: &str) -> Option<(String, u32)> {
    let suffix = timer_id.strip_prefix("retry:")?;
    let (step_id, attempt) = suffix.rsplit_once(':')?;
    let attempt = attempt.parse::<u32>().ok()?;
    Some((step_id.to_owned(), attempt))
}

fn build_workflow_envelope(
    source: &EventEnvelope<WorkflowEvent>,
    causation_id: Uuid,
    correlation_id: Option<Uuid>,
    _workflow_version: u32,
    emission: ExecutionEmission,
) -> EventEnvelope<WorkflowEvent> {
    let mut envelope = EventEnvelope::new(
        emission.event.event_type(),
        source_identity(source, "executor-service"),
        emission.event,
    );
    envelope.causation_id = Some(causation_id);
    envelope.correlation_id = correlation_id;
    if let Some(state) = emission.state {
        envelope.metadata.insert("state".to_owned(), state);
    }
    envelope
}

async fn publish_artifact_pinned(
    publisher: &WorkflowPublisher,
    event: &EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    let mut envelope = EventEnvelope::new(
        WorkflowEvent::WorkflowArtifactPinned.event_type(),
        source_identity(event, "executor-service"),
        WorkflowEvent::WorkflowArtifactPinned,
    );
    envelope.causation_id = Some(event.event_id);
    envelope.correlation_id = event.correlation_id.or(Some(event.event_id));
    publisher.publish(&envelope, &envelope.partition_key).await?;
    Ok(())
}

fn source_identity(event: &EventEnvelope<WorkflowEvent>, producer: &str) -> WorkflowIdentity {
    WorkflowIdentity::new(
        event.tenant_id.clone(),
        event.definition_id.clone(),
        event.definition_version,
        event.artifact_hash.clone(),
        event.instance_id.clone(),
        event.run_id.clone(),
        producer.to_owned(),
    )
}

fn apply_compiled_plan(state: &mut WorkflowInstanceState, plan: &CompiledExecutionPlan) {
    state.current_state = Some(plan.final_state.clone());
    state.context = plan.context.clone();
    state.output = plan.output.clone();
    state.artifact_execution = Some(plan.execution_state.clone());
}

fn is_run_initialization_event(event: &WorkflowEvent) -> bool {
    matches!(
        event,
        WorkflowEvent::WorkflowTriggered { .. }
            | WorkflowEvent::WorkflowStarted
            | WorkflowEvent::WorkflowArtifactPinned
    )
}
