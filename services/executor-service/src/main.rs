use anyhow::Result;
use axum::{Json, extract::Path, extract::State as AxumState, http::StatusCode, routing::get};
use fabrik_broker::{
    BrokerConfig, WorkflowHistoryFilter, WorkflowPublisher, build_workflow_consumer,
    decode_workflow_event, read_workflow_history,
};
use fabrik_config::{
    ExecutorRuntimeConfig, HttpServiceConfig, OwnershipConfig, PostgresConfig, RedpandaConfig,
};
use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::{PartitionOwnershipRecord, WorkflowStore};
use fabrik_workflow::{
    CompiledExecutionPlan, CompiledWorkflowArtifact, ExecutionEmission, ExecutionTurnContext,
    WorkflowInstanceState,
};
use futures_util::StreamExt;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{error, info, warn};
use uuid::Uuid;

const OWNERSHIP_TRANSITION_HISTORY_LIMIT: usize = 16;

#[tokio::main]
async fn main() -> Result<()> {
    let config = HttpServiceConfig::from_env("EXECUTOR_SERVICE", "executor-service", 3002)?;
    let redpanda = RedpandaConfig::from_env()?;
    let postgres = PostgresConfig::from_env()?;
    let runtime = ExecutorRuntimeConfig::from_env()?;
    let ownership = OwnershipConfig::from_env()?;
    init_tracing(&config.log_filter);
    info!(
        port = config.port,
        cache_capacity = runtime.cache_capacity,
        snapshot_interval_events = runtime.snapshot_interval_events,
        continue_as_new_event_threshold = ?runtime.continue_as_new_event_threshold,
        continue_as_new_effect_attempt_threshold = ?runtime.continue_as_new_effect_attempt_threshold,
        continue_as_new_run_age_seconds = ?runtime.continue_as_new_run_age_seconds,
        partition_id = ownership.partition_id,
        lease_ttl_seconds = ownership.lease_ttl_seconds,
        renew_interval_seconds = ownership.renew_interval_seconds,
        "starting executor service"
    );

    let broker = BrokerConfig::new(redpanda.brokers, redpanda.workflow_events_topic);
    let publisher = WorkflowPublisher::new(&broker, "executor-service").await?;
    let consumer = build_workflow_consumer(&broker, "executor-service").await?;
    let store = WorkflowStore::connect(&postgres.url).await?;
    store.init().await?;
    let owner_id = format!("executor-service:{}", Uuid::now_v7());
    let lease_ttl = std::time::Duration::from_secs(ownership.lease_ttl_seconds);
    let initial_ownership =
        await_initial_partition_ownership(&store, ownership.partition_id, &owner_id, lease_ttl)
            .await?;
    let lease_state = Arc::new(Mutex::new(LeaseState::from_record(&initial_ownership)));
    let debug_state = Arc::new(Mutex::new(ExecutorDebugState::new(
        &initial_ownership,
        runtime.cache_capacity,
        runtime.snapshot_interval_events,
    )));
    tokio::spawn(run_ownership_renewal_loop(
        store.clone(),
        lease_state.clone(),
        debug_state.clone(),
        lease_ttl,
        std::time::Duration::from_secs(ownership.renew_interval_seconds),
    ));

    tokio::spawn(run_executor_loop(
        consumer,
        publisher,
        store.clone(),
        broker.clone(),
        runtime,
        debug_state.clone(),
        lease_state.clone(),
    ));

    let app = default_router::<ExecutorAppState>(ServiceInfo::new(
        config.name,
        "executor",
        env!("CARGO_PKG_VERSION"),
    ))
    .route("/debug/runtime", get(get_runtime_debug))
    .route("/debug/ownership", get(get_ownership_debug))
    .route("/debug/hot-state/{tenant_id}/{instance_id}", get(get_hot_state_debug))
    .with_state(ExecutorAppState { debug: debug_state });

    serve(app, config.port).await
}

#[derive(Clone)]
struct ExecutorAppState {
    debug: Arc<Mutex<ExecutorDebugState>>,
}

async fn run_executor_loop(
    consumer: rskafka::client::consumer::StreamConsumer,
    publisher: WorkflowPublisher,
    store: WorkflowStore,
    broker: BrokerConfig,
    runtime_config: ExecutorRuntimeConfig,
    debug_state: Arc<Mutex<ExecutorDebugState>>,
    lease_state: Arc<Mutex<LeaseState>>,
) {
    let mut stream = consumer;
    let mut runtime = ExecutorRuntime::new(
        runtime_config.cache_capacity,
        runtime_config.snapshot_interval_events,
        runtime_config.continue_as_new_event_threshold,
        runtime_config.continue_as_new_effect_attempt_threshold,
        runtime_config.continue_as_new_run_age_seconds,
    );

    while let Some(message) = stream.next().await {
        match message {
            Ok((record, _high_watermark)) => match decode_workflow_event(&record) {
                Ok(event) => {
                    if let Err(error) = process_event(
                        &store,
                        &broker,
                        &publisher,
                        &mut runtime,
                        &debug_state,
                        &lease_state,
                        event,
                    )
                    .await
                    {
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
    broker: &BrokerConfig,
    publisher: &WorkflowPublisher,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    event: EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    let lease_snapshot =
        ensure_active_partition_ownership(store, runtime, debug_state, lease_state).await?;
    if !store.mark_event_processed(&event).await? {
        return Ok(());
    }
    if should_drop_stale_timer_event(&event, lease_snapshot.owner_epoch) {
        warn!(
            event_type = %event.event_type,
            workflow_instance_id = %event.instance_id,
            run_id = %event.run_id,
            observed_owner_epoch = lease_snapshot.owner_epoch,
            timer_owner_epoch = ?event.metadata.get("owner_epoch"),
            "dropping timer event from stale ownership epoch"
        );
        return Ok(());
    }

    let mut record = match load_cached_or_snapshot_state(
        store,
        broker,
        runtime,
        debug_state,
        lease_snapshot.owner_epoch,
        &event,
    )
    .await?
    {
        Some(record) if record.state.run_id != event.run_id => {
            if matches!(event.payload, WorkflowEvent::WorkflowContinuedAsNew { .. }) {
                let deleted = store
                    .delete_timers_for_run(&event.tenant_id, &event.instance_id, &event.run_id)
                    .await?;
                info!(
                    workflow_instance_id = %event.instance_id,
                    stale_run_id = %event.run_id,
                    active_run_id = %record.state.run_id,
                    deleted_timers = deleted,
                    "processed stale continue-as-new marker for timer cleanup"
                );
                return Ok(());
            }
            if is_run_initialization_event(&event.payload) {
                HotStateRecord {
                    state: WorkflowInstanceState::try_from(&event)?,
                    last_snapshot_event_count: None,
                    restore_source: RestoreSource::Initialized,
                }
            } else {
                warn!(
                    event_type = %event.event_type,
                    workflow_instance_id = %event.instance_id,
                    stale_run_id = %event.run_id,
                    active_run_id = %record.state.run_id,
                    "dropping stale event for non-active workflow run"
                );
                return Ok(());
            }
        }
        Some(mut record) => {
            if should_ignore_effect_terminal_event(&record.state, &event.payload) {
                warn!(
                    event_type = %event.event_type,
                    workflow_instance_id = %event.instance_id,
                    run_id = %event.run_id,
                    current_state = ?record.state.current_state,
                    "dropping late effect terminal event for non-active effect state"
                );
                return Ok(());
            }
            record.state.apply_event(&event);
            record
        }
        None => match WorkflowInstanceState::try_from(&event) {
            Ok(state) => HotStateRecord {
                state,
                last_snapshot_event_count: None,
                restore_source: RestoreSource::Initialized,
            },
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

    let mut state = record.state.clone();
    persist_state(store, runtime, debug_state, lease_state, &mut record, &state).await?;

    match &event.payload {
        WorkflowEvent::WorkflowTriggered { .. } => {
            if let Some(artifact) = load_pinned_artifact(store, &event, &state).await? {
                publish_artifact_pinned(
                    store,
                    runtime,
                    debug_state,
                    lease_state,
                    publisher,
                    &event,
                )
                .await?;
                let input = state.context.clone().unwrap_or(Value::Null);
                let plan = artifact.execute_trigger_with_turn(
                    &input,
                    ExecutionTurnContext {
                        event_id: event.event_id,
                        occurred_at: event.occurred_at,
                    },
                )?;
                apply_compiled_plan(&mut state, &plan);
                persist_state(store, runtime, debug_state, lease_state, &mut record, &state)
                    .await?;
                publish_compiled_plan(
                    store,
                    runtime,
                    debug_state,
                    lease_state,
                    publisher,
                    &event,
                    &artifact,
                    plan,
                    state.context.clone().or_else(|| state.input.clone()),
                )
                .await?;
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
                        store,
                        runtime,
                        debug_state,
                        lease_state,
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

            publish_artifact_pinned(store, runtime, debug_state, lease_state, publisher, &event)
                .await?;

            let input = state.context.clone().unwrap_or(Value::Null);
            let plan = match definition.execute_trigger(&input) {
                Ok(plan) => plan,
                Err(error) => {
                    publish_failure(
                        store,
                        runtime,
                        debug_state,
                        lease_state,
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

            publish_plan(
                store,
                runtime,
                debug_state,
                lease_state,
                publisher,
                &event,
                event.definition_version,
                plan,
                state.context.clone().or_else(|| state.input.clone()),
            )
            .await?;
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
                if let Some((effect_id, attempt)) = parse_effect_timeout_timer_id(timer_id) {
                    if state.current_state.as_deref() != Some(effect_id.as_str()) {
                        return Ok(());
                    }
                    let emission = ExecutionEmission {
                        event: WorkflowEvent::EffectTimedOut { effect_id, attempt },
                        state: state.current_state.clone(),
                    };
                    publish_plan(
                        store,
                        runtime,
                        debug_state,
                        lease_state,
                        publisher,
                        &event,
                        artifact.definition_version,
                        fabrik_workflow::WorkflowExecutionPlan {
                            workflow_version: artifact.definition_version,
                            final_state: state.current_state.clone().unwrap_or_default(),
                            emissions: vec![emission],
                        },
                        state.context.clone().or_else(|| state.input.clone()),
                    )
                    .await?;
                    return Ok(());
                }
                if let Some((target_kind, target_id, attempt)) = parse_retry_timer_id(timer_id) {
                    let mut execution_state = state.artifact_execution.clone().unwrap_or_default();
                    execution_state.turn_context = Some(ExecutionTurnContext {
                        event_id: event.event_id,
                        occurred_at: event.occurred_at,
                    });
                    match target_kind {
                        RetryTargetKind::Step => {
                            let input = artifact.step_details(&target_id, &execution_state)?.2;
                            state.context = Some(input);
                            state.current_state = Some(target_id.clone());
                            persist_state(
                                store,
                                runtime,
                                debug_state,
                                lease_state,
                                &mut record,
                                &state,
                            )
                            .await?;
                            let emission = ExecutionEmission {
                                event: WorkflowEvent::StepScheduled {
                                    step_id: target_id.clone(),
                                    attempt,
                                    input: state.context.clone().unwrap_or(Value::Null),
                                },
                                state: Some(target_id.clone()),
                            };
                            publish_plan(
                                store,
                                runtime,
                                debug_state,
                                lease_state,
                                publisher,
                                &event,
                                artifact.definition_version,
                                fabrik_workflow::WorkflowExecutionPlan {
                                    workflow_version: artifact.definition_version,
                                    final_state: target_id,
                                    emissions: vec![emission],
                                },
                                state.context.clone().or_else(|| state.input.clone()),
                            )
                            .await?;
                            return Ok(());
                        }
                        RetryTargetKind::Effect => {
                            let (connector, _, input) =
                                artifact.effect_details(&target_id, &execution_state)?;
                            state.context = Some(input);
                            state.current_state = Some(target_id.clone());
                            persist_state(
                                store,
                                runtime,
                                debug_state,
                                lease_state,
                                &mut record,
                                &state,
                            )
                            .await?;
                            let emission = ExecutionEmission {
                                event: WorkflowEvent::EffectRequested {
                                    effect_id: target_id.clone(),
                                    connector,
                                    attempt,
                                    input: state.context.clone().unwrap_or(Value::Null),
                                },
                                state: Some(target_id.clone()),
                            };
                            publish_plan(
                                store,
                                runtime,
                                debug_state,
                                lease_state,
                                publisher,
                                &event,
                                artifact.definition_version,
                                fabrik_workflow::WorkflowExecutionPlan {
                                    workflow_version: artifact.definition_version,
                                    final_state: target_id,
                                    emissions: vec![emission],
                                },
                                state.context.clone().or_else(|| state.input.clone()),
                            )
                            .await?;
                            return Ok(());
                        }
                    }
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
                let plan = artifact.execute_after_timer_with_turn(
                    &wait_state,
                    timer_id,
                    state.artifact_execution.clone().unwrap_or_default(),
                    ExecutionTurnContext {
                        event_id: event.event_id,
                        occurred_at: event.occurred_at,
                    },
                )?;
                apply_compiled_plan(&mut state, &plan);
                persist_state(store, runtime, debug_state, lease_state, &mut record, &state)
                    .await?;
                publish_compiled_plan(
                    store,
                    runtime,
                    debug_state,
                    lease_state,
                    publisher,
                    &event,
                    &artifact,
                    plan,
                    state.context.clone().or_else(|| state.input.clone()),
                )
                .await?;
                return Ok(());
            }

            if let Some((target_kind, target_id, attempt)) = parse_retry_timer_id(timer_id) {
                if !matches!(target_kind, RetryTargetKind::Step) {
                    return Ok(());
                }
                let definition = load_pinned_definition(store, &event, &state).await?;
                let input =
                    state.context.clone().or_else(|| state.input.clone()).unwrap_or(Value::Null);
                let emission = ExecutionEmission {
                    event: WorkflowEvent::StepScheduled {
                        step_id: target_id.clone(),
                        attempt,
                        input,
                    },
                    state: Some(target_id),
                };
                publish_plan(
                    store,
                    runtime,
                    debug_state,
                    lease_state,
                    publisher,
                    &event,
                    definition.version,
                    fabrik_workflow::WorkflowExecutionPlan {
                        workflow_version: definition.version,
                        final_state: state.current_state.clone().unwrap_or_default(),
                        emissions: vec![emission],
                    },
                    state.context.clone().or_else(|| state.input.clone()),
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
                        store,
                        runtime,
                        debug_state,
                        lease_state,
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

            publish_plan(
                store,
                runtime,
                debug_state,
                lease_state,
                publisher,
                &event,
                definition.version,
                plan,
                state.context.clone().or_else(|| state.input.clone()),
            )
            .await?;
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
                let plan = artifact.execute_after_step_completion_with_turn(
                    &step_state,
                    step_id,
                    output,
                    state.artifact_execution.clone().unwrap_or_default(),
                    ExecutionTurnContext {
                        event_id: event.event_id,
                        occurred_at: event.occurred_at,
                    },
                )?;
                apply_compiled_plan(&mut state, &plan);
                persist_state(store, runtime, debug_state, lease_state, &mut record, &state)
                    .await?;
                publish_compiled_plan(
                    store,
                    runtime,
                    debug_state,
                    lease_state,
                    publisher,
                    &event,
                    &artifact,
                    plan,
                    state.context.clone().or_else(|| state.input.clone()),
                )
                .await?;
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
                        store,
                        runtime,
                        debug_state,
                        lease_state,
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

            publish_plan(
                store,
                runtime,
                debug_state,
                lease_state,
                publisher,
                &event,
                definition.version,
                plan,
                state.context.clone().or_else(|| state.input.clone()),
            )
            .await?;
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
                        let retry_timer_id =
                            build_retry_timer_id(RetryTargetKind::Step, step_id, attempt + 1);
                        let emission = ExecutionEmission {
                            event: WorkflowEvent::TimerScheduled {
                                timer_id: retry_timer_id,
                                fire_at: retry,
                            },
                            state: Some(step_state.clone()),
                        };
                        publish_plan(
                            store,
                            runtime,
                            debug_state,
                            lease_state,
                            publisher,
                            &event,
                            artifact.definition_version,
                            fabrik_workflow::WorkflowExecutionPlan {
                                workflow_version: artifact.definition_version,
                                final_state: step_state,
                                emissions: vec![emission],
                            },
                            state.context.clone().or_else(|| state.input.clone()),
                        )
                        .await?;
                    }
                    _ => {
                        let plan = artifact.execute_after_step_failure_with_turn(
                            &step_state,
                            step_id,
                            step_error,
                            state.artifact_execution.clone().unwrap_or_default(),
                            ExecutionTurnContext {
                                event_id: event.event_id,
                                occurred_at: event.occurred_at,
                            },
                        )?;
                        apply_compiled_plan(&mut state, &plan);
                        persist_state(
                            store,
                            runtime,
                            debug_state,
                            lease_state,
                            &mut record,
                            &state,
                        )
                        .await?;
                        publish_compiled_plan(
                            store,
                            runtime,
                            debug_state,
                            lease_state,
                            publisher,
                            &event,
                            &artifact,
                            plan,
                            state.context.clone().or_else(|| state.input.clone()),
                        )
                        .await?;
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
                    let retry_timer_id =
                        build_retry_timer_id(RetryTargetKind::Step, step_id, retry.attempt);
                    let emission = ExecutionEmission {
                        event: WorkflowEvent::TimerScheduled {
                            timer_id: retry_timer_id,
                            fire_at: retry.fire_at,
                        },
                        state: Some(step_state),
                    };
                    publish_plan(
                        store,
                        runtime,
                        debug_state,
                        lease_state,
                        publisher,
                        &event,
                        definition.version,
                        fabrik_workflow::WorkflowExecutionPlan {
                            workflow_version: definition.version,
                            final_state: state.current_state.clone().unwrap_or_default(),
                            emissions: vec![emission],
                        },
                        state.context.clone().or_else(|| state.input.clone()),
                    )
                    .await?;
                }
                Ok(None) => {
                    publish_failure(
                        store,
                        runtime,
                        debug_state,
                        lease_state,
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
                        store,
                        runtime,
                        debug_state,
                        lease_state,
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
        WorkflowEvent::EffectRequested { effect_id, connector, attempt, input } => {
            let timeout_ref =
                if let Some(artifact) = load_pinned_artifact(store, &event, &state).await? {
                    artifact.effect_timeout(effect_id)?.map(str::to_owned)
                } else {
                    None
                };
            store
                .upsert_effect_requested(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    &state.definition_id,
                    state.definition_version,
                    state.artifact_hash.as_deref(),
                    effect_id,
                    *attempt,
                    connector,
                    state.current_state.as_deref(),
                    timeout_ref.as_deref(),
                    input,
                    event.event_id,
                    event.occurred_at,
                )
                .await?;

            if let Some(timeout_ref) = timeout_ref {
                let fire_at = chrono::Utc::now() + fabrik_workflow::parse_timer_ref(&timeout_ref)?;
                let emission = ExecutionEmission {
                    event: WorkflowEvent::TimerScheduled {
                        timer_id: build_effect_timeout_timer_id(effect_id, *attempt),
                        fire_at,
                    },
                    state: state.current_state.clone(),
                };
                publish_plan(
                    store,
                    runtime,
                    debug_state,
                    lease_state,
                    publisher,
                    &event,
                    state.definition_version.unwrap_or(event.definition_version),
                    fabrik_workflow::WorkflowExecutionPlan {
                        workflow_version: state
                            .definition_version
                            .unwrap_or(event.definition_version),
                        final_state: state.current_state.clone().unwrap_or_default(),
                        emissions: vec![emission],
                    },
                    state.context.clone().or_else(|| state.input.clone()),
                )
                .await?;
            }
        }
        WorkflowEvent::EffectCompleted { effect_id, attempt, output } => {
            let _ = store
                .delete_timer(
                    &event.tenant_id,
                    &event.instance_id,
                    &build_effect_timeout_timer_id(effect_id, *attempt),
                )
                .await;
            store
                .complete_effect(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    effect_id,
                    *attempt,
                    output,
                    event.event_id,
                    &event.event_type,
                    event.occurred_at,
                )
                .await?;
            if let Some(artifact) = load_pinned_artifact(store, &event, &state).await? {
                let effect_state = match state.current_state.clone() {
                    Some(effect_state) => effect_state,
                    None => {
                        warn!(
                            workflow_instance_id = %event.instance_id,
                            effect_id = %effect_id,
                            "effect completed without current_state"
                        );
                        return Ok(());
                    }
                };
                let plan = artifact.execute_after_effect_completion_with_turn(
                    &effect_state,
                    effect_id,
                    output,
                    state.artifact_execution.clone().unwrap_or_default(),
                    ExecutionTurnContext {
                        event_id: event.event_id,
                        occurred_at: event.occurred_at,
                    },
                )?;
                apply_compiled_plan(&mut state, &plan);
                persist_state(store, runtime, debug_state, lease_state, &mut record, &state)
                    .await?;
                publish_compiled_plan(
                    store,
                    runtime,
                    debug_state,
                    lease_state,
                    publisher,
                    &event,
                    &artifact,
                    plan,
                    state.context.clone().or_else(|| state.input.clone()),
                )
                .await?;
            }
        }
        WorkflowEvent::EffectFailed { effect_id, attempt, error: effect_error } => {
            let _ = store
                .delete_timer(
                    &event.tenant_id,
                    &event.instance_id,
                    &build_effect_timeout_timer_id(effect_id, *attempt),
                )
                .await;
            store
                .fail_effect(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    effect_id,
                    *attempt,
                    fabrik_store::WorkflowEffectStatus::Failed,
                    effect_error,
                    None,
                    event.event_id,
                    &event.event_type,
                    event.occurred_at,
                )
                .await?;
            if let Some(artifact) = load_pinned_artifact(store, &event, &state).await? {
                let effect_state = match state.current_state.clone() {
                    Some(effect_state) => effect_state,
                    None => {
                        warn!(
                            workflow_instance_id = %event.instance_id,
                            effect_id = %effect_id,
                            "effect failed without current_state"
                        );
                        return Ok(());
                    }
                };

                match artifact.effect_retry(&effect_state)? {
                    Some(retry_policy) if *attempt < retry_policy.max_attempts => {
                        let retry = chrono::Utc::now()
                            + fabrik_workflow::parse_timer_ref(&retry_policy.delay)?;
                        let retry_timer_id =
                            build_retry_timer_id(RetryTargetKind::Effect, effect_id, attempt + 1);
                        let emission = ExecutionEmission {
                            event: WorkflowEvent::TimerScheduled {
                                timer_id: retry_timer_id,
                                fire_at: retry,
                            },
                            state: Some(effect_state),
                        };
                        publish_plan(
                            store,
                            runtime,
                            debug_state,
                            lease_state,
                            publisher,
                            &event,
                            artifact.definition_version,
                            fabrik_workflow::WorkflowExecutionPlan {
                                workflow_version: artifact.definition_version,
                                final_state: state.current_state.clone().unwrap_or_default(),
                                emissions: vec![emission],
                            },
                            state.context.clone().or_else(|| state.input.clone()),
                        )
                        .await?;
                    }
                    _ => {
                        let plan = artifact.execute_after_effect_failure_with_turn(
                            &effect_state,
                            effect_id,
                            effect_error,
                            state.artifact_execution.clone().unwrap_or_default(),
                            ExecutionTurnContext {
                                event_id: event.event_id,
                                occurred_at: event.occurred_at,
                            },
                        )?;
                        apply_compiled_plan(&mut state, &plan);
                        persist_state(
                            store,
                            runtime,
                            debug_state,
                            lease_state,
                            &mut record,
                            &state,
                        )
                        .await?;
                        publish_compiled_plan(
                            store,
                            runtime,
                            debug_state,
                            lease_state,
                            publisher,
                            &event,
                            &artifact,
                            plan,
                            state.context.clone().or_else(|| state.input.clone()),
                        )
                        .await?;
                    }
                }
            }
        }
        WorkflowEvent::EffectTimedOut { effect_id, attempt } => {
            store
                .fail_effect(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    effect_id,
                    *attempt,
                    fabrik_store::WorkflowEffectStatus::TimedOut,
                    "effect timed out",
                    None,
                    event.event_id,
                    &event.event_type,
                    event.occurred_at,
                )
                .await?;
            if let Some(artifact) = load_pinned_artifact(store, &event, &state).await? {
                let effect_state = match state.current_state.clone() {
                    Some(effect_state) => effect_state,
                    None => {
                        warn!(
                            workflow_instance_id = %event.instance_id,
                            effect_id = %effect_id,
                            "effect timed out without current_state"
                        );
                        return Ok(());
                    }
                };

                match artifact.effect_retry(&effect_state)? {
                    Some(retry_policy) if *attempt < retry_policy.max_attempts => {
                        let retry = chrono::Utc::now()
                            + fabrik_workflow::parse_timer_ref(&retry_policy.delay)?;
                        let retry_timer_id =
                            build_retry_timer_id(RetryTargetKind::Effect, effect_id, attempt + 1);
                        let emission = ExecutionEmission {
                            event: WorkflowEvent::TimerScheduled {
                                timer_id: retry_timer_id,
                                fire_at: retry,
                            },
                            state: Some(effect_state),
                        };
                        publish_plan(
                            store,
                            runtime,
                            debug_state,
                            lease_state,
                            publisher,
                            &event,
                            artifact.definition_version,
                            fabrik_workflow::WorkflowExecutionPlan {
                                workflow_version: artifact.definition_version,
                                final_state: state.current_state.clone().unwrap_or_default(),
                                emissions: vec![emission],
                            },
                            state.context.clone().or_else(|| state.input.clone()),
                        )
                        .await?;
                    }
                    _ => {
                        let plan = artifact.execute_after_effect_failure_with_turn(
                            &effect_state,
                            effect_id,
                            "effect timed out",
                            state.artifact_execution.clone().unwrap_or_default(),
                            ExecutionTurnContext {
                                event_id: event.event_id,
                                occurred_at: event.occurred_at,
                            },
                        )?;
                        apply_compiled_plan(&mut state, &plan);
                        persist_state(
                            store,
                            runtime,
                            debug_state,
                            lease_state,
                            &mut record,
                            &state,
                        )
                        .await?;
                        publish_compiled_plan(
                            store,
                            runtime,
                            debug_state,
                            lease_state,
                            publisher,
                            &event,
                            &artifact,
                            plan,
                            state.context.clone().or_else(|| state.input.clone()),
                        )
                        .await?;
                    }
                }
            }
        }
        WorkflowEvent::EffectCancelled { effect_id, attempt, reason, metadata } => {
            let _ = store
                .delete_timer(
                    &event.tenant_id,
                    &event.instance_id,
                    &build_effect_timeout_timer_id(effect_id, *attempt),
                )
                .await;
            store
                .fail_effect(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    effect_id,
                    *attempt,
                    fabrik_store::WorkflowEffectStatus::Cancelled,
                    reason,
                    metadata.as_ref(),
                    event.event_id,
                    &event.event_type,
                    event.occurred_at,
                )
                .await?;
            if let Some(artifact) = load_pinned_artifact(store, &event, &state).await? {
                let effect_state = match state.current_state.clone() {
                    Some(effect_state) => effect_state,
                    None => {
                        warn!(
                            workflow_instance_id = %event.instance_id,
                            effect_id = %effect_id,
                            "effect cancelled without current_state"
                        );
                        return Ok(());
                    }
                };
                let plan = artifact.execute_after_effect_failure_with_turn(
                    &effect_state,
                    effect_id,
                    reason,
                    state.artifact_execution.clone().unwrap_or_default(),
                    ExecutionTurnContext {
                        event_id: event.event_id,
                        occurred_at: event.occurred_at,
                    },
                )?;
                apply_compiled_plan(&mut state, &plan);
                persist_state(store, runtime, debug_state, lease_state, &mut record, &state)
                    .await?;
                publish_compiled_plan(
                    store,
                    runtime,
                    debug_state,
                    lease_state,
                    publisher,
                    &event,
                    &artifact,
                    plan,
                    state.context.clone().or_else(|| state.input.clone()),
                )
                .await?;
            }
        }
        WorkflowEvent::SignalReceived { signal_type, payload, .. } => {
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
                let plan = artifact.execute_after_signal_with_turn(
                    &wait_state,
                    signal_type,
                    payload,
                    state.artifact_execution.clone().unwrap_or_default(),
                    ExecutionTurnContext {
                        event_id: event.event_id,
                        occurred_at: event.occurred_at,
                    },
                )?;
                apply_compiled_plan(&mut state, &plan);
                persist_state(store, runtime, debug_state, lease_state, &mut record, &state)
                    .await?;
                publish_compiled_plan(
                    store,
                    runtime,
                    debug_state,
                    lease_state,
                    publisher,
                    &event,
                    &artifact,
                    plan,
                    state.context.clone().or_else(|| state.input.clone()),
                )
                .await?;
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
                        store,
                        runtime,
                        debug_state,
                        lease_state,
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

            publish_plan(
                store,
                runtime,
                debug_state,
                lease_state,
                publisher,
                &event,
                definition.version,
                plan,
                state.context.clone().or_else(|| state.input.clone()),
            )
            .await?;
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
        WorkflowEvent::SignalQueued { .. } => {
            try_dispatch_buffered_signal(
                store,
                runtime,
                debug_state,
                lease_state,
                publisher,
                &state,
                &event,
            )
            .await?;
        }
        WorkflowEvent::WorkflowArtifactPinned
        | WorkflowEvent::WorkflowStarted
        | WorkflowEvent::MarkerRecorded { .. } => {}
        _ => {}
    }

    Ok(())
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct HotStateKey {
    tenant_id: String,
    instance_id: String,
}

impl HotStateKey {
    fn new(tenant_id: &str, instance_id: &str) -> Self {
        Self { tenant_id: tenant_id.to_owned(), instance_id: instance_id.to_owned() }
    }
}

#[derive(Clone)]
struct HotStateRecord {
    state: WorkflowInstanceState,
    last_snapshot_event_count: Option<i64>,
    restore_source: RestoreSource,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum RestoreSource {
    Initialized,
    Projection,
    SnapshotReplay,
    Cache,
}

#[derive(Clone)]
struct CachedStateEntry {
    record: HotStateRecord,
    access_epoch: u64,
}

struct ExecutorRuntime {
    cache_capacity: usize,
    snapshot_interval_events: u64,
    continue_as_new_event_threshold: Option<u64>,
    continue_as_new_effect_attempt_threshold: Option<u64>,
    continue_as_new_run_age_seconds: Option<u64>,
    access_epoch: u64,
    hits: u64,
    misses: u64,
    restores_from_projection: u64,
    restores_from_snapshot_replay: u64,
    restores_after_handoff: u64,
    cache: HashMap<HotStateKey, CachedStateEntry>,
}

#[derive(Debug, Clone, Serialize)]
struct ExecutorDebugState {
    ownership: PartitionOwnership,
    cache_capacity: usize,
    snapshot_interval_events: u64,
    cache_hits: u64,
    cache_misses: u64,
    restores_from_projection: u64,
    restores_from_snapshot_replay: u64,
    restores_after_handoff: u64,
    hot_instance_count: usize,
    hot_instances: Vec<HotInstanceDebug>,
    ownership_transitions: Vec<OwnershipTransitionDebug>,
}

#[derive(Debug, Clone, Serialize)]
struct PartitionOwnership {
    partition_id: i32,
    owner_id: String,
    status: &'static str,
    epoch: u64,
    acquired_at: chrono::DateTime<chrono::Utc>,
    last_transition_at: chrono::DateTime<chrono::Utc>,
    lease_expires_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct OwnershipTransitionDebug {
    status: &'static str,
    epoch: u64,
    transitioned_at: chrono::DateTime<chrono::Utc>,
    reason: String,
}

#[derive(Debug, Clone)]
struct LeaseState {
    partition_id: i32,
    owner_id: String,
    owner_epoch: u64,
    lease_expires_at: chrono::DateTime<chrono::Utc>,
    active: bool,
}

impl LeaseState {
    fn from_record(record: &PartitionOwnershipRecord) -> Self {
        Self {
            partition_id: record.partition_id,
            owner_id: record.owner_id.clone(),
            owner_epoch: record.owner_epoch,
            lease_expires_at: record.lease_expires_at,
            active: true,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct HotInstanceDebug {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    definition_id: String,
    event_count: i64,
    current_state: Option<String>,
    restore_source: RestoreSource,
    last_snapshot_event_count: Option<i64>,
}

impl ExecutorRuntime {
    fn new(
        cache_capacity: usize,
        snapshot_interval_events: u64,
        continue_as_new_event_threshold: Option<u64>,
        continue_as_new_effect_attempt_threshold: Option<u64>,
        continue_as_new_run_age_seconds: Option<u64>,
    ) -> Self {
        Self {
            cache_capacity,
            snapshot_interval_events,
            continue_as_new_event_threshold,
            continue_as_new_effect_attempt_threshold,
            continue_as_new_run_age_seconds,
            access_epoch: 0,
            hits: 0,
            misses: 0,
            restores_from_projection: 0,
            restores_from_snapshot_replay: 0,
            restores_after_handoff: 0,
            cache: HashMap::new(),
        }
    }

    fn get(&mut self, tenant_id: &str, instance_id: &str) -> Option<HotStateRecord> {
        if self.cache_capacity == 0 {
            self.misses += 1;
            return None;
        }

        let key = HotStateKey::new(tenant_id, instance_id);
        match self.cache.get_mut(&key) {
            Some(entry) => {
                self.hits += 1;
                self.access_epoch += 1;
                entry.access_epoch = self.access_epoch;
                entry.record.restore_source = RestoreSource::Cache;
                Some(entry.record.clone())
            }
            None => {
                self.misses += 1;
                None
            }
        }
    }

    fn put(&mut self, record: HotStateRecord) {
        if self.cache_capacity == 0 {
            return;
        }

        self.access_epoch += 1;
        let key = HotStateKey::new(&record.state.tenant_id, &record.state.instance_id);
        self.cache.insert(key, CachedStateEntry { record, access_epoch: self.access_epoch });

        if self.cache.len() > self.cache_capacity {
            self.evict_one();
        }
    }

    fn evict_one(&mut self) {
        if let Some(key) = self
            .cache
            .iter()
            .min_by_key(|(_, entry)| entry.access_epoch)
            .map(|(key, _)| key.clone())
        {
            self.cache.remove(&key);
        }
    }

    fn clear(&mut self) {
        self.cache.clear();
    }
}

impl ExecutorDebugState {
    fn new(
        ownership: &PartitionOwnershipRecord,
        cache_capacity: usize,
        snapshot_interval_events: u64,
    ) -> Self {
        Self {
            ownership: map_partition_ownership(ownership, "owned"),
            cache_capacity,
            snapshot_interval_events,
            cache_hits: 0,
            cache_misses: 0,
            restores_from_projection: 0,
            restores_from_snapshot_replay: 0,
            restores_after_handoff: 0,
            hot_instance_count: 0,
            hot_instances: Vec::new(),
            ownership_transitions: vec![OwnershipTransitionDebug {
                status: "acquired",
                epoch: ownership.owner_epoch,
                transitioned_at: ownership.last_transition_at,
                reason: "initial ownership claim".to_owned(),
            }],
        }
    }

    fn sync_from_runtime(&mut self, runtime: &ExecutorRuntime) {
        self.cache_hits = runtime.hits;
        self.cache_misses = runtime.misses;
        self.restores_from_projection = runtime.restores_from_projection;
        self.restores_from_snapshot_replay = runtime.restores_from_snapshot_replay;
        self.restores_after_handoff = runtime.restores_after_handoff;
        self.hot_instance_count = runtime.cache.len();
        self.hot_instances = runtime
            .cache
            .values()
            .map(|entry| HotInstanceDebug {
                tenant_id: entry.record.state.tenant_id.clone(),
                instance_id: entry.record.state.instance_id.clone(),
                run_id: entry.record.state.run_id.clone(),
                definition_id: entry.record.state.definition_id.clone(),
                event_count: entry.record.state.event_count,
                current_state: entry.record.state.current_state.clone(),
                restore_source: entry.record.restore_source,
                last_snapshot_event_count: entry.record.last_snapshot_event_count,
            })
            .collect();
        self.hot_instances.sort_by(|left, right| {
            left.tenant_id.cmp(&right.tenant_id).then(left.instance_id.cmp(&right.instance_id))
        });
    }

    fn update_ownership(
        &mut self,
        record: &PartitionOwnershipRecord,
        status: &'static str,
        reason: impl Into<String>,
    ) {
        self.ownership = map_partition_ownership(record, status);
        self.record_transition(status, record.owner_epoch, record.last_transition_at, reason);
    }

    fn mark_ownership_lost(
        &mut self,
        epoch: u64,
        lease_expires_at: chrono::DateTime<chrono::Utc>,
        reason: impl Into<String>,
    ) {
        let now = chrono::Utc::now();
        self.ownership.status = "lost";
        self.ownership.epoch = epoch;
        self.ownership.last_transition_at = now;
        self.ownership.lease_expires_at = lease_expires_at;
        self.record_transition("lost", epoch, now, reason);
    }

    fn record_transition(
        &mut self,
        status: &'static str,
        epoch: u64,
        transitioned_at: chrono::DateTime<chrono::Utc>,
        reason: impl Into<String>,
    ) {
        self.ownership_transitions.insert(
            0,
            OwnershipTransitionDebug { status, epoch, transitioned_at, reason: reason.into() },
        );
        if self.ownership_transitions.len() > OWNERSHIP_TRANSITION_HISTORY_LIMIT {
            self.ownership_transitions.truncate(OWNERSHIP_TRANSITION_HISTORY_LIMIT);
        }
    }
}

async fn load_cached_or_snapshot_state(
    store: &WorkflowStore,
    broker: &BrokerConfig,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    ownership_epoch: u64,
    event: &EventEnvelope<WorkflowEvent>,
) -> Result<Option<HotStateRecord>> {
    if let Some(mut record) = runtime.get(&event.tenant_id, &event.instance_id) {
        record.restore_source = RestoreSource::Cache;
        sync_debug_state(debug_state, runtime);
        return Ok(Some(record));
    }

    if let Some(snapshot) = store.get_latest_snapshot(&event.tenant_id, &event.instance_id).await? {
        let record = restore_from_snapshot_tail(broker, snapshot).await?;
        runtime.restores_from_snapshot_replay += 1;
        if ownership_epoch > 1 {
            runtime.restores_after_handoff += 1;
        }
        runtime.put(record.clone());
        sync_debug_state(debug_state, runtime);
        return Ok(Some(record));
    }

    let instance = store.get_instance(&event.tenant_id, &event.instance_id).await?;
    if let Some(state) = instance.clone() {
        runtime.restores_from_projection += 1;
        if ownership_epoch > 1 {
            runtime.restores_after_handoff += 1;
        }
        runtime.put(HotStateRecord {
            state,
            last_snapshot_event_count: None,
            restore_source: RestoreSource::Projection,
        });
        sync_debug_state(debug_state, runtime);
    }
    Ok(instance.map(|state| HotStateRecord {
        state,
        last_snapshot_event_count: None,
        restore_source: RestoreSource::Projection,
    }))
}

async fn persist_state(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    record: &mut HotStateRecord,
    state: &WorkflowInstanceState,
) -> Result<()> {
    ensure_active_partition_ownership(store, runtime, debug_state, lease_state).await?;
    store.upsert_instance(state).await?;
    let should_snapshot = record.last_snapshot_event_count.is_none()
        || (state.event_count - record.last_snapshot_event_count.unwrap_or_default())
            >= runtime.snapshot_interval_events as i64
        || matches!(
            state.status,
            fabrik_workflow::WorkflowStatus::Completed | fabrik_workflow::WorkflowStatus::Failed
        );
    if should_snapshot {
        store.put_snapshot(state).await?;
        record.last_snapshot_event_count = Some(state.event_count);
    }
    record.state = state.clone();
    runtime.put(record.clone());
    sync_debug_state(debug_state, runtime);
    Ok(())
}

async fn restore_from_snapshot_tail(
    broker: &BrokerConfig,
    snapshot: fabrik_store::WorkflowStateSnapshot,
) -> Result<HotStateRecord> {
    let history = read_workflow_history(
        broker,
        "executor-service-restore",
        &WorkflowHistoryFilter::new(&snapshot.tenant_id, &snapshot.instance_id, &snapshot.run_id),
        std::time::Duration::from_millis(500),
        std::time::Duration::from_secs(5),
    )
    .await?;
    let tail_start = history
        .iter()
        .position(|event| event.event_id == snapshot.last_event_id)
        .map(|index| index + 1)
        .unwrap_or(history.len());
    let mut state = snapshot.state.clone();
    for event in history.into_iter().skip(tail_start) {
        state.apply_event(&event);
    }
    Ok(HotStateRecord {
        state,
        last_snapshot_event_count: Some(snapshot.event_count),
        restore_source: RestoreSource::SnapshotReplay,
    })
}

fn sync_debug_state(debug_state: &Arc<Mutex<ExecutorDebugState>>, runtime: &ExecutorRuntime) {
    if let Ok(mut state) = debug_state.lock() {
        state.sync_from_runtime(runtime);
    }
}

fn map_partition_ownership(
    record: &PartitionOwnershipRecord,
    status: &'static str,
) -> PartitionOwnership {
    PartitionOwnership {
        partition_id: record.partition_id,
        owner_id: record.owner_id.clone(),
        status,
        epoch: record.owner_epoch,
        acquired_at: record.acquired_at,
        last_transition_at: record.last_transition_at,
        lease_expires_at: record.lease_expires_at,
    }
}

async fn await_initial_partition_ownership(
    store: &WorkflowStore,
    partition_id: i32,
    owner_id: &str,
    lease_ttl: std::time::Duration,
) -> Result<PartitionOwnershipRecord> {
    loop {
        if let Some(record) =
            store.claim_partition_ownership(partition_id, owner_id, lease_ttl).await?
        {
            return Ok(record);
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

async fn run_ownership_renewal_loop(
    store: WorkflowStore,
    lease_state: Arc<Mutex<LeaseState>>,
    debug_state: Arc<Mutex<ExecutorDebugState>>,
    lease_ttl: std::time::Duration,
    renew_interval: std::time::Duration,
) {
    loop {
        tokio::time::sleep(renew_interval).await;

        let snapshot = match lease_state.lock() {
            Ok(state) => state.clone(),
            Err(_) => {
                warn!("executor lease state lock poisoned");
                continue;
            }
        };

        if snapshot.active {
            match store
                .renew_partition_ownership(
                    snapshot.partition_id,
                    &snapshot.owner_id,
                    snapshot.owner_epoch,
                    lease_ttl,
                )
                .await
            {
                Ok(Some(record)) => {
                    if let Ok(mut state) = lease_state.lock() {
                        *state = LeaseState::from_record(&record);
                    }
                    if let Ok(mut debug) = debug_state.lock() {
                        debug.update_ownership(&record, "owned", "lease renewed");
                    }
                }
                Ok(None) => {
                    if let Ok(mut state) = lease_state.lock() {
                        state.active = false;
                    }
                    if let Ok(mut debug) = debug_state.lock() {
                        debug.mark_ownership_lost(
                            snapshot.owner_epoch,
                            snapshot.lease_expires_at,
                            "lease renewal lost",
                        );
                    }
                }
                Err(error) => warn!(error = %error, "failed to renew executor ownership lease"),
            }
            continue;
        }

        match store
            .claim_partition_ownership(snapshot.partition_id, &snapshot.owner_id, lease_ttl)
            .await
        {
            Ok(Some(record)) => {
                if let Ok(mut state) = lease_state.lock() {
                    *state = LeaseState::from_record(&record);
                }
                if let Ok(mut debug) = debug_state.lock() {
                    debug.update_ownership(&record, "owned", "ownership reacquired");
                }
            }
            Ok(None) => {}
            Err(error) => warn!(error = %error, "failed to reacquire executor ownership lease"),
        }
    }
}

async fn ensure_active_partition_ownership(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
) -> Result<LeaseState> {
    let snapshot = lease_state
        .lock()
        .map(|state| state.clone())
        .map_err(|_| anyhow::anyhow!("executor lease state lock poisoned"))?;

    if !snapshot.active {
        runtime.clear();
        sync_debug_state(debug_state, runtime);
        anyhow::bail!("executor does not currently hold partition ownership");
    }

    if !store
        .validate_partition_ownership(
            snapshot.partition_id,
            &snapshot.owner_id,
            snapshot.owner_epoch,
        )
        .await?
    {
        runtime.clear();
        sync_debug_state(debug_state, runtime);
        if let Ok(mut state) = lease_state.lock() {
            state.active = false;
        }
        if let Ok(mut debug) = debug_state.lock() {
            debug.mark_ownership_lost(
                snapshot.owner_epoch,
                snapshot.lease_expires_at,
                "ownership validation failed",
            );
        }
        anyhow::bail!("executor partition ownership is stale");
    }

    Ok(snapshot)
}

fn should_drop_stale_timer_event(
    event: &EventEnvelope<WorkflowEvent>,
    current_owner_epoch: u64,
) -> bool {
    if !matches!(event.payload, WorkflowEvent::TimerFired { .. }) {
        return false;
    }

    event
        .metadata
        .get("owner_epoch")
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|owner_epoch| owner_epoch != current_owner_epoch)
}

async fn get_runtime_debug(
    AxumState(state): AxumState<ExecutorAppState>,
) -> Result<Json<ExecutorDebugState>, (StatusCode, Json<ExecutorErrorResponse>)> {
    let snapshot = {
        let state = state
            .debug
            .lock()
            .map_err(|_| internal_executor_error("executor debug state lock poisoned"))?;
        state.clone()
    };
    Ok(Json(snapshot))
}

async fn get_ownership_debug(
    AxumState(state): AxumState<ExecutorAppState>,
) -> Result<Json<PartitionOwnership>, (StatusCode, Json<ExecutorErrorResponse>)> {
    let snapshot = state
        .debug
        .lock()
        .map(|state| state.ownership.clone())
        .map_err(|_| internal_executor_error("executor debug state lock poisoned"))?;
    Ok(Json(snapshot))
}

async fn get_hot_state_debug(
    Path((tenant_id, instance_id)): Path<(String, String)>,
    AxumState(state): AxumState<ExecutorAppState>,
) -> Result<Json<HotInstanceDebug>, (StatusCode, Json<ExecutorErrorResponse>)> {
    let snapshot = state
        .debug
        .lock()
        .map_err(|_| internal_executor_error("executor debug state lock poisoned"))?;
    let instance = snapshot
        .hot_instances
        .iter()
        .find(|entry| entry.tenant_id == tenant_id && entry.instance_id == instance_id)
        .cloned()
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ExecutorErrorResponse {
                    message: format!(
                        "hot state for workflow instance {instance_id} not present in executor cache"
                    ),
                }),
            )
        })?;
    Ok(Json(instance))
}

#[derive(Debug, Serialize)]
struct ExecutorErrorResponse {
    message: String,
}

fn internal_executor_error(
    message: impl Into<String>,
) -> (StatusCode, Json<ExecutorErrorResponse>) {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(ExecutorErrorResponse { message: message.into() }))
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
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &WorkflowPublisher,
    event: &EventEnvelope<WorkflowEvent>,
    workflow_version: u32,
    plan: fabrik_workflow::WorkflowExecutionPlan,
    continue_input: Option<Value>,
) -> Result<()> {
    ensure_active_partition_ownership(store, runtime, debug_state, lease_state).await?;
    let final_state = plan.final_state.clone();
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
            envelope.metadata.insert("final_state".to_owned(), final_state.clone());
        }
        previous_causation_id = envelope.event_id;
        publisher.publish(&envelope, &key).await?;
    }

    let continued = maybe_auto_continue_as_new_legacy(
        store,
        runtime,
        debug_state,
        lease_state,
        publisher,
        event,
        workflow_version,
        &final_state,
        continue_input,
    )
    .await?;
    if !continued {
        if let Some(current) = store.get_instance(&event.tenant_id, &event.instance_id).await? {
            try_dispatch_buffered_signal(
                store,
                runtime,
                debug_state,
                lease_state,
                publisher,
                &current,
                event,
            )
            .await?;
        }
    }

    Ok(())
}

async fn publish_failure(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &WorkflowPublisher,
    event: &EventEnvelope<WorkflowEvent>,
    _workflow_version: Option<u32>,
    reason: String,
    state: Option<String>,
) -> Result<()> {
    ensure_active_partition_ownership(store, runtime, debug_state, lease_state).await?;
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
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &WorkflowPublisher,
    event: &EventEnvelope<WorkflowEvent>,
    artifact: &CompiledWorkflowArtifact,
    plan: CompiledExecutionPlan,
    continue_input: Option<Value>,
) -> Result<()> {
    ensure_active_partition_ownership(store, runtime, debug_state, lease_state).await?;
    let final_state = plan.final_state.clone();
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

    let continued = maybe_auto_continue_as_new_compiled(
        store,
        runtime,
        debug_state,
        lease_state,
        publisher,
        event,
        artifact,
        &final_state,
        continue_input,
    )
    .await?;
    if !continued {
        if let Some(current) = store.get_instance(&event.tenant_id, &event.instance_id).await? {
            try_dispatch_buffered_signal(
                store,
                runtime,
                debug_state,
                lease_state,
                publisher,
                &current,
                event,
            )
            .await?;
        }
    }

    Ok(())
}

#[derive(Clone, Copy)]
enum RetryTargetKind {
    Step,
    Effect,
}

fn build_retry_timer_id(kind: RetryTargetKind, target_id: &str, attempt: u32) -> String {
    let kind = match kind {
        RetryTargetKind::Step => "step",
        RetryTargetKind::Effect => "effect",
    };
    format!("retry:{kind}:{target_id}:{attempt}")
}

fn parse_retry_timer_id(timer_id: &str) -> Option<(RetryTargetKind, String, u32)> {
    let suffix = timer_id.strip_prefix("retry:")?;
    let (kind_and_id, attempt) = suffix.rsplit_once(':')?;
    let (kind, target_id) = kind_and_id.split_once(':')?;
    let attempt = attempt.parse::<u32>().ok()?;
    let kind = match kind {
        "step" => RetryTargetKind::Step,
        "effect" => RetryTargetKind::Effect,
        _ => return None,
    };
    Some((kind, target_id.to_owned(), attempt))
}

fn build_effect_timeout_timer_id(effect_id: &str, attempt: u32) -> String {
    format!("effect-timeout:{effect_id}:{attempt}")
}

fn parse_effect_timeout_timer_id(timer_id: &str) -> Option<(String, u32)> {
    let suffix = timer_id.strip_prefix("effect-timeout:")?;
    let (effect_id, attempt) = suffix.rsplit_once(':')?;
    let attempt = attempt.parse::<u32>().ok()?;
    Some((effect_id.to_owned(), attempt))
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
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &WorkflowPublisher,
    event: &EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    ensure_active_partition_ownership(store, runtime, debug_state, lease_state).await?;
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

async fn try_dispatch_buffered_signal(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &WorkflowPublisher,
    state: &WorkflowInstanceState,
    event: &EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    let Some(wait_state) = state.current_state.as_deref() else {
        return Ok(());
    };
    let expected_signal_type =
        if let Some(artifact) = load_pinned_artifact(store, event, state).await? {
            artifact.expected_signal_type(wait_state)?.map(str::to_owned)
        } else {
            let definition = load_pinned_definition(store, event, state).await?;
            definition.expected_signal_type(wait_state)?.map(str::to_owned)
        };
    let Some(expected_signal_type) = expected_signal_type else {
        return Ok(());
    };

    let Some(oldest_signal) = store
        .get_oldest_pending_signal(&event.tenant_id, &event.instance_id, &event.run_id)
        .await?
    else {
        return Ok(());
    };
    if oldest_signal.signal_type != expected_signal_type {
        return Ok(());
    }

    let Some(signal) = store
        .claim_signal_for_dispatch(
            &event.tenant_id,
            &event.instance_id,
            &event.run_id,
            &oldest_signal.signal_id,
        )
        .await?
    else {
        return Ok(());
    };
    let dispatch_event_id = signal.dispatch_event_id.ok_or_else(|| {
        anyhow::anyhow!("workflow signal {} missing dispatch_event_id", signal.signal_id)
    })?;

    ensure_active_partition_ownership(store, runtime, debug_state, lease_state).await?;
    let payload = WorkflowEvent::SignalReceived {
        signal_id: signal.signal_id.clone(),
        signal_type: signal.signal_type.clone(),
        payload: signal.payload.clone(),
    };
    let mut envelope = EventEnvelope::new(
        payload.event_type(),
        WorkflowIdentity::new(
            event.tenant_id.clone(),
            event.definition_id.clone(),
            event.definition_version,
            event.artifact_hash.clone(),
            event.instance_id.clone(),
            event.run_id.clone(),
            "executor-service",
        ),
        payload,
    );
    envelope.event_id = dispatch_event_id;
    envelope.occurred_at = chrono::Utc::now();
    envelope.causation_id = Some(signal.source_event_id);
    envelope.correlation_id = event.correlation_id.or(Some(signal.source_event_id));
    envelope.dedupe_key = Some(format!("signal:{}", signal.signal_id));
    envelope.metadata.insert("mailbox_delivery".to_owned(), "true".to_owned());

    publisher.publish(&envelope, &envelope.partition_key).await?;
    store
        .mark_signal_consumed(
            &signal.tenant_id,
            &signal.instance_id,
            &signal.run_id,
            &signal.signal_id,
            dispatch_event_id,
            envelope.occurred_at,
        )
        .await?;

    Ok(())
}

async fn maybe_auto_continue_as_new_legacy(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &WorkflowPublisher,
    event: &EventEnvelope<WorkflowEvent>,
    workflow_version: u32,
    final_state: &str,
    continue_input: Option<Value>,
) -> Result<bool> {
    let Some(input) = continue_input else {
        return Ok(false);
    };
    let Some(definition) = store
        .get_definition_version(&event.tenant_id, &event.definition_id, workflow_version)
        .await?
    else {
        return Ok(false);
    };
    if !definition.is_wait_state(final_state)? {
        return Ok(false);
    }
    let Some(reason) =
        rollover_reason(store, runtime, &event.tenant_id, &event.instance_id, &event.run_id)
            .await?
    else {
        return Ok(false);
    };
    emit_continue_as_new(
        store,
        runtime,
        debug_state,
        lease_state,
        publisher,
        event,
        workflow_version,
        &event.artifact_hash,
        input,
        &reason,
    )
    .await?;
    Ok(true)
}

async fn maybe_auto_continue_as_new_compiled(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &WorkflowPublisher,
    event: &EventEnvelope<WorkflowEvent>,
    artifact: &CompiledWorkflowArtifact,
    final_state: &str,
    continue_input: Option<Value>,
) -> Result<bool> {
    let Some(input) = continue_input else {
        return Ok(false);
    };
    if !artifact.is_wait_state(final_state)? {
        return Ok(false);
    }
    let Some(reason) =
        rollover_reason(store, runtime, &event.tenant_id, &event.instance_id, &event.run_id)
            .await?
    else {
        return Ok(false);
    };
    emit_continue_as_new(
        store,
        runtime,
        debug_state,
        lease_state,
        publisher,
        event,
        artifact.definition_version,
        &artifact.artifact_hash,
        input,
        &reason,
    )
    .await?;
    Ok(true)
}

async fn rollover_reason(
    store: &WorkflowStore,
    runtime: &ExecutorRuntime,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
) -> Result<Option<String>> {
    let Some(run) = store.get_run_record(tenant_id, instance_id, run_id).await? else {
        return Ok(None);
    };
    if run.next_run_id.is_some() {
        return Ok(None);
    }

    if let Some(threshold) = runtime.continue_as_new_event_threshold {
        if let Some(instance) = store.get_instance(tenant_id, instance_id).await? {
            if instance.run_id == run_id && instance.event_count as u64 >= threshold {
                return Ok(Some(format!("event_count:{threshold}")));
            }
        }
    }

    if let Some(threshold) = runtime.continue_as_new_effect_attempt_threshold {
        let attempts = store.count_effect_attempts_for_run(tenant_id, instance_id, run_id).await?;
        if attempts >= threshold {
            return Ok(Some(format!("effect_attempts:{threshold}")));
        }
    }

    if let Some(threshold) = runtime.continue_as_new_run_age_seconds {
        let age = chrono::Utc::now() - run.started_at;
        if age.num_seconds().max(0) as u64 >= threshold {
            return Ok(Some(format!("run_age_seconds:{threshold}")));
        }
    }

    Ok(None)
}

async fn emit_continue_as_new(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &WorkflowPublisher,
    event: &EventEnvelope<WorkflowEvent>,
    definition_version: u32,
    artifact_hash: &str,
    input: Value,
    reason: &str,
) -> Result<()> {
    ensure_active_partition_ownership(store, runtime, debug_state, lease_state).await?;
    let new_run_id = format!("run-{}", Uuid::now_v7());
    let continue_payload = WorkflowEvent::WorkflowContinuedAsNew {
        new_run_id: new_run_id.clone(),
        input: input.clone(),
    };
    let mut continued = EventEnvelope::new(
        continue_payload.event_type(),
        WorkflowIdentity::new(
            event.tenant_id.clone(),
            event.definition_id.clone(),
            definition_version,
            artifact_hash.to_owned(),
            event.instance_id.clone(),
            event.run_id.clone(),
            "executor-service",
        ),
        continue_payload,
    );
    continued.causation_id = Some(event.event_id);
    continued.correlation_id = event.correlation_id.or(Some(event.event_id));
    continued.metadata.insert("continue_reason".to_owned(), reason.to_owned());
    publisher.publish(&continued, &continued.partition_key).await?;

    let trigger_payload = WorkflowEvent::WorkflowTriggered { input };
    let mut trigger = EventEnvelope::new(
        trigger_payload.event_type(),
        WorkflowIdentity::new(
            event.tenant_id.clone(),
            event.definition_id.clone(),
            definition_version,
            artifact_hash.to_owned(),
            event.instance_id.clone(),
            new_run_id.clone(),
            "executor-service",
        ),
        trigger_payload,
    );
    trigger.causation_id = Some(continued.event_id);
    trigger.correlation_id = continued.correlation_id;
    trigger.metadata.insert("continue_reason".to_owned(), reason.to_owned());
    publisher.publish(&trigger, &trigger.partition_key).await?;

    store
        .put_run_start(
            &trigger.tenant_id,
            &trigger.instance_id,
            &trigger.run_id,
            &trigger.definition_id,
            Some(trigger.definition_version),
            Some(&trigger.artifact_hash),
            trigger.event_id,
            trigger.occurred_at,
            Some(&event.run_id),
            Some(&event.run_id),
        )
        .await?;
    store
        .record_run_continuation(
            &continued.tenant_id,
            &continued.instance_id,
            &event.run_id,
            &new_run_id,
            reason,
            continued.event_id,
            trigger.event_id,
            trigger.occurred_at,
        )
        .await?;

    Ok(())
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
            | WorkflowEvent::MarkerRecorded { .. }
    )
}

fn should_ignore_effect_terminal_event(
    state: &WorkflowInstanceState,
    event: &WorkflowEvent,
) -> bool {
    let effect_id = match event {
        WorkflowEvent::EffectCompleted { effect_id, .. }
        | WorkflowEvent::EffectFailed { effect_id, .. }
        | WorkflowEvent::EffectTimedOut { effect_id, .. }
        | WorkflowEvent::EffectCancelled { effect_id, .. } => effect_id,
        _ => return false,
    };

    state.current_state.as_deref() != Some(effect_id.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use fabrik_workflow::WorkflowStatus;

    fn demo_state(instance_id: &str) -> WorkflowInstanceState {
        WorkflowInstanceState {
            tenant_id: "tenant-a".to_owned(),
            instance_id: instance_id.to_owned(),
            run_id: format!("run-{instance_id}"),
            definition_id: "demo".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact".to_owned()),
            current_state: Some("wait".to_owned()),
            context: Some(Value::Null),
            artifact_execution: None,
            status: WorkflowStatus::Running,
            input: Some(Value::Null),
            output: None,
            event_count: 1,
            last_event_id: Uuid::now_v7(),
            last_event_type: "WorkflowStarted".to_owned(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn hot_state_cache_returns_cached_entries() {
        let mut runtime = ExecutorRuntime::new(2, 10, None, None, None);
        let state = demo_state("instance-1");
        runtime.put(HotStateRecord {
            state: state.clone(),
            last_snapshot_event_count: Some(1),
            restore_source: RestoreSource::Initialized,
        });

        let cached =
            runtime.get("tenant-a", "instance-1").expect("cached state should exist").state;
        assert_eq!(cached.run_id, state.run_id);
        assert_eq!(runtime.hits, 1);
        assert_eq!(runtime.misses, 0);
    }

    #[test]
    fn hot_state_cache_evicts_least_recent_entry() {
        let mut runtime = ExecutorRuntime::new(2, 10, None, None, None);
        runtime.put(HotStateRecord {
            state: demo_state("instance-1"),
            last_snapshot_event_count: Some(1),
            restore_source: RestoreSource::Initialized,
        });
        runtime.put(HotStateRecord {
            state: demo_state("instance-2"),
            last_snapshot_event_count: Some(1),
            restore_source: RestoreSource::Initialized,
        });
        let _ = runtime.get("tenant-a", "instance-1");
        runtime.put(HotStateRecord {
            state: demo_state("instance-3"),
            last_snapshot_event_count: Some(1),
            restore_source: RestoreSource::Initialized,
        });

        assert!(runtime.get("tenant-a", "instance-1").is_some());
        assert!(runtime.get("tenant-a", "instance-3").is_some());
        assert!(runtime.get("tenant-a", "instance-2").is_none());
    }
}
