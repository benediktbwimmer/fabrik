use anyhow::Result;
use axum::{
    Json,
    extract::Path,
    extract::State as AxumState,
    http::StatusCode,
    routing::{get, post},
};
use fabrik_broker::{
    BrokerConfig, WorkflowHistoryFilter, WorkflowPublisher, WorkflowTopicTopology,
    build_workflow_partition_consumer, decode_workflow_event, describe_workflow_topic,
    read_workflow_history,
};
use fabrik_config::{
    ExecutorRuntimeConfig, HttpServiceConfig, OwnershipConfig, PostgresConfig, RedpandaConfig,
};
use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::{PartitionOwnershipRecord, WorkflowStore};
use fabrik_workflow::{
    CompiledExecutionPlan, CompiledWorkflowArtifact, ExecutionEmission, ExecutionTurnContext,
    WorkflowInstanceState, replay_compiled_history_trace_from_snapshot,
    replay_history_trace_from_snapshot,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
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
        continue_as_new_activity_attempt_threshold = ?runtime.continue_as_new_activity_attempt_threshold,
        continue_as_new_run_age_seconds = ?runtime.continue_as_new_run_age_seconds,
        static_partition_ids = ?ownership.static_partition_ids,
        executor_capacity = ownership.executor_capacity,
        lease_ttl_seconds = ownership.lease_ttl_seconds,
        renew_interval_seconds = ownership.renew_interval_seconds,
        member_heartbeat_ttl_seconds = ownership.member_heartbeat_ttl_seconds,
        assignment_poll_interval_seconds = ownership.assignment_poll_interval_seconds,
        rebalance_interval_seconds = ownership.rebalance_interval_seconds,
        "starting executor service"
    );

    let broker = BrokerConfig::new(
        redpanda.brokers,
        redpanda.workflow_events_topic,
        redpanda.workflow_events_partitions,
    );
    let publisher = WorkflowPublisher::new(&broker, "executor-service").await?;
    let store = WorkflowStore::connect(&postgres.url).await?;
    store.init().await?;
    let owner_id = format!("executor-service:{}", Uuid::now_v7());
    let lease_ttl = std::time::Duration::from_secs(ownership.lease_ttl_seconds);
    let initial_partitions = ownership.static_partition_ids.clone().unwrap_or_default();
    let initial_ownerships = if initial_partitions.is_empty() {
        Vec::new()
    } else {
        let mut ownerships = Vec::new();
        for partition_id in &initial_partitions {
            ownerships.push(
                await_initial_partition_ownership(&store, *partition_id, &owner_id, lease_ttl)
                    .await?,
            );
        }
        ownerships
    };
    let debug_state = Arc::new(Mutex::new(ExecutorDebugState::new(
        &initial_ownerships,
        runtime.cache_capacity,
        runtime.snapshot_interval_events,
    )));
    let workers = Arc::new(Mutex::new(HashMap::new()));
    if initial_partitions.is_empty() {
        tokio::spawn(run_assignment_supervisor(
            store.clone(),
            broker.clone(),
            publisher.clone(),
            runtime.clone(),
            ownership.clone(),
            debug_state.clone(),
            workers.clone(),
            owner_id.clone(),
            lease_ttl,
        ));
    } else {
        for record in initial_ownerships {
            spawn_partition_worker(
                &store,
                &broker,
                &publisher,
                &runtime,
                &debug_state,
                &workers,
                owner_id.clone(),
                record,
                std::time::Duration::from_secs(ownership.renew_interval_seconds),
                lease_ttl,
            )
            .await?;
        }
    }

    let app = default_router::<ExecutorAppState>(ServiceInfo::new(
        config.name,
        "executor",
        env!("CARGO_PKG_VERSION"),
    ))
    .route("/debug/runtime", get(get_runtime_debug))
    .route("/debug/ownership", get(get_ownership_debug))
    .route("/debug/broker", get(get_broker_debug))
    .route("/debug/hot-state/{tenant_id}/{instance_id}", get(get_hot_state_debug))
    .route(
        "/internal/workflows/{tenant_id}/{instance_id}/queries/{query_name}",
        post(execute_internal_query),
    )
    .with_state(ExecutorAppState { debug: debug_state, broker, store });

    serve(app, config.port).await
}

#[derive(Clone)]
struct ExecutorAppState {
    debug: Arc<Mutex<ExecutorDebugState>>,
    broker: BrokerConfig,
    store: WorkflowStore,
}

struct PartitionWorkerHandle {
    renewal_task: JoinHandle<()>,
    consumer_task: JoinHandle<()>,
    lease_state: Arc<Mutex<LeaseState>>,
}

async fn run_assignment_supervisor(
    store: WorkflowStore,
    broker: BrokerConfig,
    publisher: WorkflowPublisher,
    runtime_config: ExecutorRuntimeConfig,
    ownership_config: OwnershipConfig,
    debug_state: Arc<Mutex<ExecutorDebugState>>,
    workers: Arc<Mutex<HashMap<i32, PartitionWorkerHandle>>>,
    owner_id: String,
    lease_ttl: std::time::Duration,
) {
    let heartbeat_ttl =
        std::time::Duration::from_secs(ownership_config.member_heartbeat_ttl_seconds);
    let poll_interval =
        std::time::Duration::from_secs(ownership_config.assignment_poll_interval_seconds);
    let rebalance_interval =
        std::time::Duration::from_secs(ownership_config.rebalance_interval_seconds);
    let renew_interval = std::time::Duration::from_secs(ownership_config.renew_interval_seconds);
    let mut last_rebalance_at = std::time::Instant::now() - rebalance_interval;
    let query_endpoint = "http://127.0.0.1:3002".to_owned();

    loop {
        if let Err(error) = store
            .heartbeat_executor_member(
                &owner_id,
                &query_endpoint,
                ownership_config.executor_capacity,
                heartbeat_ttl,
            )
            .await
        {
            error!(error = %error, "failed to heartbeat executor membership");
        }

        if last_rebalance_at.elapsed() >= rebalance_interval {
            match store.list_active_executor_members(chrono::Utc::now()).await {
                Ok(members) => {
                    if let Err(error) = store
                        .reconcile_partition_assignments(
                            broker.workflow_events_partitions,
                            &members,
                        )
                        .await
                    {
                        error!(error = %error, "failed to reconcile partition assignments");
                    }
                }
                Err(error) => error!(error = %error, "failed to load active executor members"),
            }
            last_rebalance_at = std::time::Instant::now();
        }

        match store.list_assignments_for_executor(&owner_id).await {
            Ok(assignments) => {
                let desired = assignments
                    .into_iter()
                    .map(|assignment| assignment.partition_id)
                    .collect::<Vec<_>>();
                if let Err(error) = reconcile_partition_workers(
                    &store,
                    &broker,
                    &publisher,
                    &runtime_config,
                    &debug_state,
                    &workers,
                    &owner_id,
                    desired,
                    renew_interval,
                    lease_ttl,
                )
                .await
                {
                    error!(error = %error, "failed to reconcile local partition workers");
                }
            }
            Err(error) => error!(error = %error, "failed to load local partition assignments"),
        }

        tokio::time::sleep(poll_interval).await;
    }
}

async fn reconcile_partition_workers(
    store: &WorkflowStore,
    broker: &BrokerConfig,
    publisher: &WorkflowPublisher,
    runtime_config: &ExecutorRuntimeConfig,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    workers: &Arc<Mutex<HashMap<i32, PartitionWorkerHandle>>>,
    owner_id: &str,
    desired_partitions: Vec<i32>,
    renew_interval: std::time::Duration,
    lease_ttl: std::time::Duration,
) -> Result<()> {
    let active_partitions = workers
        .lock()
        .map(|state| state.keys().copied().collect::<Vec<_>>())
        .map_err(|_| anyhow::anyhow!("partition worker map lock poisoned"))?;

    for partition_id in active_partitions
        .iter()
        .copied()
        .filter(|partition_id| !desired_partitions.contains(partition_id))
        .collect::<Vec<_>>()
    {
        stop_partition_worker(workers, debug_state, partition_id)?;
    }

    for partition_id in desired_partitions {
        let already_running = workers
            .lock()
            .map(|state| state.contains_key(&partition_id))
            .map_err(|_| anyhow::anyhow!("partition worker map lock poisoned"))?;
        if already_running {
            continue;
        }
        let record =
            await_initial_partition_ownership(store, partition_id, owner_id, lease_ttl).await?;
        spawn_partition_worker(
            store,
            broker,
            publisher,
            runtime_config,
            debug_state,
            workers,
            owner_id.to_owned(),
            record,
            renew_interval,
            lease_ttl,
        )
        .await?;
    }

    Ok(())
}

async fn spawn_partition_worker(
    store: &WorkflowStore,
    broker: &BrokerConfig,
    publisher: &WorkflowPublisher,
    runtime_config: &ExecutorRuntimeConfig,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    workers: &Arc<Mutex<HashMap<i32, PartitionWorkerHandle>>>,
    owner_id: String,
    initial_ownership: PartitionOwnershipRecord,
    renew_interval: std::time::Duration,
    lease_ttl: std::time::Duration,
) -> Result<()> {
    let partition_id = initial_ownership.partition_id;
    let lease_state = Arc::new(Mutex::new(LeaseState::from_record(&initial_ownership)));
    if let Ok(mut debug) = debug_state.lock() {
        debug.update_ownership(&initial_ownership, "owned", "partition worker started");
    }

    let renewal_task = tokio::spawn(run_ownership_renewal_loop(
        store.clone(),
        lease_state.clone(),
        debug_state.clone(),
        lease_ttl,
        renew_interval,
    ));
    let consumer =
        build_workflow_partition_consumer(broker, "executor-service", partition_id).await?;
    let consumer_task = tokio::spawn(run_executor_loop(
        consumer,
        publisher.clone(),
        store.clone(),
        broker.clone(),
        runtime_config.clone(),
        debug_state.clone(),
        lease_state.clone(),
    ));

    workers
        .lock()
        .map_err(|_| anyhow::anyhow!("partition worker map lock poisoned"))?
        .insert(partition_id, PartitionWorkerHandle { renewal_task, consumer_task, lease_state });
    let _ = owner_id;
    Ok(())
}

fn stop_partition_worker(
    workers: &Arc<Mutex<HashMap<i32, PartitionWorkerHandle>>>,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    partition_id: i32,
) -> Result<()> {
    let handle = workers
        .lock()
        .map_err(|_| anyhow::anyhow!("partition worker map lock poisoned"))?
        .remove(&partition_id);
    if let Some(handle) = handle {
        let lease_snapshot = handle.lease_state.lock().ok().map(|state| state.clone());
        handle.consumer_task.abort();
        handle.renewal_task.abort();
        if let Some(lease_snapshot) = lease_snapshot {
            if let Ok(mut debug) = debug_state.lock() {
                debug.mark_ownership_lost(
                    partition_id,
                    lease_snapshot.owner_epoch,
                    lease_snapshot.lease_expires_at,
                    "partition assignment removed",
                );
                let partition = debug.partition_mut(partition_id);
                partition.hot_instance_count = 0;
                partition.hot_instances.clear();
                partition.cache_hits = 0;
                partition.cache_misses = 0;
                partition.restores_from_projection = 0;
                partition.restores_from_snapshot_replay = 0;
                partition.restores_after_handoff = 0;
                debug.recompute_totals();
            }
        }
    }
    Ok(())
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
        runtime_config.continue_as_new_activity_attempt_threshold,
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
    if !store.mark_event_processed(&event).await? {
        return Ok(());
    }

    let mut record = match load_cached_or_snapshot_state(
        store,
        broker,
        runtime,
        debug_state,
        lease_snapshot.partition_id,
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
    persist_state(
        store,
        runtime,
        debug_state,
        lease_state,
        lease_snapshot.partition_id,
        &mut record,
        &state,
    )
    .await?;

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
                persist_state(
                    store,
                    runtime,
                    debug_state,
                    lease_state,
                    lease_snapshot.partition_id,
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
                    lease_snapshot.partition_id,
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
                                lease_snapshot.partition_id,
                                &mut record,
                                &state,
                            )
                            .await?;
                            let (activity_type, config) = artifact.step_descriptor(&target_id)?;
                            let emission = ExecutionEmission {
                                event: WorkflowEvent::ActivityTaskScheduled {
                                    activity_id: target_id.clone(),
                                    activity_type,
                                    task_queue: "default".to_owned(),
                                    attempt,
                                    input: state.context.clone().unwrap_or(Value::Null),
                                    config: config.map(|config| {
                                        serde_json::to_value(config)
                                            .expect("activity config serializes")
                                    }),
                                    state: Some(target_id.clone()),
                                    schedule_to_start_timeout_ms: None,
                                    start_to_close_timeout_ms: None,
                                    heartbeat_timeout_ms: None,
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
                persist_state(
                    store,
                    runtime,
                    debug_state,
                    lease_state,
                    lease_snapshot.partition_id,
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
                return Ok(());
            }

            if let Some((target_kind, target_id, attempt)) = parse_retry_timer_id(timer_id) {
                let RetryTargetKind::Step = target_kind;
                let definition = load_pinned_definition(store, &event, &state).await?;
                let input =
                    state.context.clone().or_else(|| state.input.clone()).unwrap_or(Value::Null);
                let activity_type = definition.step_handler(&target_id)?.to_owned();
                let config = definition.step_config(&target_id)?.cloned().map(|config| {
                    serde_json::to_value(config).expect("activity config serializes")
                });
                let emission = ExecutionEmission {
                    event: WorkflowEvent::ActivityTaskScheduled {
                        activity_id: target_id.clone(),
                        activity_type,
                        task_queue: "default".to_owned(),
                        attempt,
                        input,
                        config,
                        state: Some(target_id.clone()),
                        schedule_to_start_timeout_ms: None,
                        start_to_close_timeout_ms: None,
                        heartbeat_timeout_ms: None,
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
        WorkflowEvent::ActivityTaskCompleted {
            activity_id: step_id, attempt: _, output, ..
        } => {
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
                persist_state(
                    store,
                    runtime,
                    debug_state,
                    lease_state,
                    lease_snapshot.partition_id,
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
        WorkflowEvent::ActivityTaskFailed {
            activity_id: step_id,
            attempt,
            error: step_error,
            ..
        }
        | WorkflowEvent::ActivityTaskCancelled {
            activity_id: step_id,
            attempt,
            reason: step_error,
            ..
        } => {
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
                            lease_snapshot.partition_id,
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
        WorkflowEvent::ActivityTaskTimedOut { activity_id, attempt, .. } => {
            let step_error = "activity timed out";
            if let Some(artifact) = load_pinned_artifact(store, &event, &state).await? {
                let step_state = match state.current_state.clone() {
                    Some(step_state) => step_state,
                    None => {
                        warn!(
                            workflow_instance_id = %event.instance_id,
                            activity_id = %activity_id,
                            "activity timed out without current_state"
                        );
                        return Ok(());
                    }
                };

                match artifact.step_retry(&step_state)? {
                    Some(retry_policy) if *attempt < retry_policy.max_attempts => {
                        let retry = chrono::Utc::now()
                            + fabrik_workflow::parse_timer_ref(&retry_policy.delay)?;
                        let retry_timer_id =
                            build_retry_timer_id(RetryTargetKind::Step, activity_id, attempt + 1);
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
                            activity_id,
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
                            lease_snapshot.partition_id,
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
                        activity_id = %activity_id,
                        "activity timed out without current_state"
                    );
                    return Ok(());
                }
            };

            let definition = load_pinned_definition(store, &event, &state).await?;
            match definition.schedule_retry(&step_state, *attempt) {
                Ok(Some(retry)) => {
                    let retry_timer_id =
                        build_retry_timer_id(RetryTargetKind::Step, activity_id, retry.attempt);
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
                        step_error.to_owned(),
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
        WorkflowEvent::SignalReceived { signal_id, signal_type, payload } => {
            store
                .mark_signal_consumed(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    signal_id,
                    event.event_id,
                    event.occurred_at,
                )
                .await?;
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
                persist_state(
                    store,
                    runtime,
                    debug_state,
                    lease_state,
                    lease_snapshot.partition_id,
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
        WorkflowEvent::ChildWorkflowCompleted { child_id, output, .. } => {
            store
                .complete_child(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    child_id,
                    "completed",
                    Some(output),
                    None,
                    event.event_id,
                    event.occurred_at,
                )
                .await?;
            let Some(artifact) = load_pinned_artifact(store, &event, &state).await? else {
                return Ok(());
            };
            let wait_state = match state.current_state.clone() {
                Some(wait_state) => wait_state,
                None => {
                    warn!(
                        workflow_instance_id = %event.instance_id,
                        child_id = %child_id,
                        "child completed without current_state"
                    );
                    return Ok(());
                }
            };
            let plan = artifact.execute_after_child_completion_with_turn(
                &wait_state,
                child_id,
                output,
                state.artifact_execution.clone().unwrap_or_default(),
                ExecutionTurnContext { event_id: event.event_id, occurred_at: event.occurred_at },
            )?;
            apply_compiled_plan(&mut state, &plan);
            persist_state(
                store,
                runtime,
                debug_state,
                lease_state,
                lease_snapshot.partition_id,
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
            return Ok(());
        }
        WorkflowEvent::ChildWorkflowFailed { child_id, error, .. }
        | WorkflowEvent::ChildWorkflowCancelled { child_id, reason: error, .. }
        | WorkflowEvent::ChildWorkflowTerminated { child_id, reason: error, .. } => {
            let status = match &event.payload {
                WorkflowEvent::ChildWorkflowFailed { .. } => "failed",
                WorkflowEvent::ChildWorkflowCancelled { .. } => "cancelled",
                WorkflowEvent::ChildWorkflowTerminated { .. } => "terminated",
                _ => unreachable!(),
            };
            store
                .complete_child(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    child_id,
                    status,
                    None,
                    Some(error),
                    event.event_id,
                    event.occurred_at,
                )
                .await?;
            let Some(artifact) = load_pinned_artifact(store, &event, &state).await? else {
                return Ok(());
            };
            let wait_state = match state.current_state.clone() {
                Some(wait_state) => wait_state,
                None => {
                    warn!(
                        workflow_instance_id = %event.instance_id,
                        child_id = %child_id,
                        "child failed without current_state"
                    );
                    return Ok(());
                }
            };
            let plan = artifact.execute_after_child_failure_with_turn(
                &wait_state,
                child_id,
                error,
                state.artifact_execution.clone().unwrap_or_default(),
                ExecutionTurnContext { event_id: event.event_id, occurred_at: event.occurred_at },
            )?;
            apply_compiled_plan(&mut state, &plan);
            persist_state(
                store,
                runtime,
                debug_state,
                lease_state,
                lease_snapshot.partition_id,
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
            return Ok(());
        }
        WorkflowEvent::WorkflowUpdateRequested { .. } => {}
        WorkflowEvent::WorkflowUpdateAccepted { update_id, update_name, payload } => {
            let Some(artifact) = load_pinned_artifact(store, &event, &state).await? else {
                publish_failure(
                    store,
                    runtime,
                    debug_state,
                    lease_state,
                    publisher,
                    &event,
                    state.definition_version,
                    format!("update {update_name} requires a compiled workflow artifact"),
                    state.current_state.clone(),
                )
                .await?;
                return Ok(());
            };
            let wait_state = state
                .current_state
                .clone()
                .unwrap_or_else(|| artifact.workflow.initial_state.clone());
            let plan = artifact.execute_update_with_turn(
                &wait_state,
                update_id,
                update_name,
                payload,
                state.artifact_execution.clone().unwrap_or_default(),
                ExecutionTurnContext { event_id: event.event_id, occurred_at: event.occurred_at },
            )?;
            apply_compiled_plan(&mut state, &plan);
            persist_state(
                store,
                runtime,
                debug_state,
                lease_state,
                lease_snapshot.partition_id,
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
            return Ok(());
        }
        WorkflowEvent::ChildWorkflowStartRequested {
            child_id,
            child_workflow_id,
            child_definition_id,
            input,
            task_queue: _,
            parent_close_policy,
        } => {
            store
                .upsert_child_start_requested(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    child_id,
                    child_workflow_id,
                    child_definition_id,
                    parent_close_policy,
                    input,
                    event.event_id,
                    event.occurred_at,
                )
                .await?;

            let (definition_version, artifact_hash) = if let Some(artifact) =
                store.get_latest_artifact(&event.tenant_id, child_definition_id).await?
            {
                (artifact.definition_version, artifact.artifact_hash)
            } else {
                let definition = store
                    .get_latest_definition(&event.tenant_id, child_definition_id)
                    .await?
                    .ok_or_else(|| {
                        anyhow::anyhow!("child workflow definition {child_definition_id} not found")
                    })?;
                (definition.version, fabrik_workflow::artifact_hash(&definition))
            };
            let child_run_id = format!("run-{}", Uuid::now_v7());
            store
                .put_run_start(
                    &event.tenant_id,
                    child_workflow_id,
                    &child_run_id,
                    child_definition_id,
                    Some(definition_version),
                    Some(&artifact_hash),
                    event.event_id,
                    event.occurred_at,
                    None,
                    Some(&event.run_id),
                )
                .await?;

            ensure_active_partition_ownership(store, runtime, debug_state, lease_state).await?;
            let mut child_trigger = EventEnvelope::new(
                WorkflowEvent::WorkflowTriggered { input: input.clone() }.event_type(),
                WorkflowIdentity::new(
                    event.tenant_id.clone(),
                    child_definition_id.clone(),
                    definition_version,
                    artifact_hash,
                    child_workflow_id.clone(),
                    child_run_id.clone(),
                    "executor-service",
                ),
                WorkflowEvent::WorkflowTriggered { input: input.clone() },
            );
            child_trigger.causation_id = Some(event.event_id);
            child_trigger.correlation_id = event.correlation_id.or(Some(event.event_id));
            child_trigger
                .metadata
                .insert("parent_instance_id".to_owned(), event.instance_id.clone());
            child_trigger.metadata.insert("parent_run_id".to_owned(), event.run_id.clone());
            child_trigger.metadata.insert("parent_child_id".to_owned(), child_id.clone());
            publisher.publish(&child_trigger, &child_trigger.partition_key).await?;

            let mut parent_started = EventEnvelope::new(
                WorkflowEvent::ChildWorkflowStarted {
                    child_id: child_id.clone(),
                    child_workflow_id: child_workflow_id.clone(),
                    child_run_id: child_run_id.clone(),
                }
                .event_type(),
                source_identity(&event, "executor-service"),
                WorkflowEvent::ChildWorkflowStarted {
                    child_id: child_id.clone(),
                    child_workflow_id: child_workflow_id.clone(),
                    child_run_id,
                },
            );
            parent_started.causation_id = Some(event.event_id);
            parent_started.correlation_id = event.correlation_id.or(Some(event.event_id));
            publisher.publish(&parent_started, &parent_started.partition_key).await?;
            return Ok(());
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
            try_dispatch_next_message(
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
        WorkflowEvent::WorkflowCancellationRequested { reason } => {
            ensure_active_partition_ownership(store, runtime, debug_state, lease_state).await?;
            let mut cancelled = EventEnvelope::new(
                WorkflowEvent::WorkflowCancelled { reason: reason.clone() }.event_type(),
                source_identity(&event, "executor-service"),
                WorkflowEvent::WorkflowCancelled { reason: reason.clone() },
            );
            cancelled.causation_id = Some(event.event_id);
            cancelled.correlation_id = event.correlation_id.or(Some(event.event_id));
            publisher.publish(&cancelled, &cancelled.partition_key).await?;
            return Ok(());
        }
        WorkflowEvent::WorkflowArtifactPinned
        | WorkflowEvent::WorkflowStarted
        | WorkflowEvent::MarkerRecorded { .. } => {}
        _ => {}
    }

    match &event.payload {
        WorkflowEvent::WorkflowUpdateCompleted { update_id, output, .. } => {
            store
                .complete_update(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    update_id,
                    Some(output),
                    None,
                    event.event_id,
                    event.occurred_at,
                )
                .await?;
        }
        WorkflowEvent::WorkflowUpdateRejected { update_id, error, .. } => {
            store
                .complete_update(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    update_id,
                    None,
                    Some(error),
                    event.event_id,
                    event.occurred_at,
                )
                .await?;
        }
        WorkflowEvent::ChildWorkflowStarted { child_id, child_run_id, .. } => {
            store
                .mark_child_started(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    child_id,
                    child_run_id,
                    event.event_id,
                    event.occurred_at,
                )
                .await?;
        }
        WorkflowEvent::ChildWorkflowCompleted { child_id, output, .. } => {
            store
                .complete_child(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    child_id,
                    "completed",
                    Some(output),
                    None,
                    event.event_id,
                    event.occurred_at,
                )
                .await?;
        }
        WorkflowEvent::ChildWorkflowFailed { child_id, error, .. } => {
            store
                .complete_child(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    child_id,
                    "failed",
                    None,
                    Some(error),
                    event.event_id,
                    event.occurred_at,
                )
                .await?;
        }
        WorkflowEvent::ChildWorkflowCancelled { child_id, reason, .. } => {
            store
                .complete_child(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    child_id,
                    "cancelled",
                    None,
                    Some(reason),
                    event.event_id,
                    event.occurred_at,
                )
                .await?;
        }
        WorkflowEvent::ChildWorkflowTerminated { child_id, reason, .. } => {
            store
                .complete_child(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    child_id,
                    "terminated",
                    None,
                    Some(reason),
                    event.event_id,
                    event.occurred_at,
                )
                .await?;
        }
        WorkflowEvent::WorkflowCompleted { .. }
        | WorkflowEvent::WorkflowFailed { .. }
        | WorkflowEvent::WorkflowCancelled { .. }
        | WorkflowEvent::WorkflowTerminated { .. } => {
            store
                .close_run(&event.tenant_id, &event.instance_id, &event.run_id, event.occurred_at)
                .await?;
            reject_outstanding_updates_for_closed_run(
                store,
                runtime,
                debug_state,
                lease_state,
                publisher,
                &event,
            )
            .await?;
            enforce_parent_close_policies(
                store,
                runtime,
                debug_state,
                lease_state,
                publisher,
                &event,
            )
            .await?;
        }
        _ => {}
    }

    if let Some(parent_child) = match &event.payload {
        WorkflowEvent::WorkflowCompleted { .. }
        | WorkflowEvent::WorkflowFailed { .. }
        | WorkflowEvent::WorkflowCancelled { .. }
        | WorkflowEvent::WorkflowTerminated { .. } => {
            store.find_parent_for_child_run(&event.tenant_id, &event.run_id).await?
        }
        _ => None,
    } {
        publish_child_terminal_reflection(
            store,
            runtime,
            debug_state,
            lease_state,
            publisher,
            &event,
            &parent_child,
        )
        .await?;
    }

    if !event_is_terminal(&event.payload) && !state.status.is_terminal() {
        try_dispatch_next_message(
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
    continue_as_new_activity_attempt_threshold: Option<u64>,
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
    cache_capacity: usize,
    snapshot_interval_events: u64,
    cache_hits: u64,
    cache_misses: u64,
    restores_from_projection: u64,
    restores_from_snapshot_replay: u64,
    restores_after_handoff: u64,
    hot_instance_count: usize,
    partitions: Vec<PartitionRuntimeDebug>,
    ownership_transitions: Vec<OwnershipTransitionDebug>,
}

#[derive(Debug, Clone, Serialize)]
struct PartitionRuntimeDebug {
    partition_id: i32,
    ownership: PartitionOwnership,
    cache_hits: u64,
    cache_misses: u64,
    restores_from_projection: u64,
    restores_from_snapshot_replay: u64,
    restores_after_handoff: u64,
    hot_instance_count: usize,
    hot_instances: Vec<HotInstanceDebug>,
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
    partition_id: i32,
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
    partition_id: i32,
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
        continue_as_new_activity_attempt_threshold: Option<u64>,
        continue_as_new_run_age_seconds: Option<u64>,
    ) -> Self {
        Self {
            cache_capacity,
            snapshot_interval_events,
            continue_as_new_event_threshold,
            continue_as_new_activity_attempt_threshold,
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
        ownerships: &[PartitionOwnershipRecord],
        cache_capacity: usize,
        snapshot_interval_events: u64,
    ) -> Self {
        Self {
            cache_capacity,
            snapshot_interval_events,
            cache_hits: 0,
            cache_misses: 0,
            restores_from_projection: 0,
            restores_from_snapshot_replay: 0,
            restores_after_handoff: 0,
            hot_instance_count: 0,
            partitions: ownerships
                .iter()
                .map(|ownership| PartitionRuntimeDebug {
                    partition_id: ownership.partition_id,
                    ownership: map_partition_ownership(ownership, "owned"),
                    cache_hits: 0,
                    cache_misses: 0,
                    restores_from_projection: 0,
                    restores_from_snapshot_replay: 0,
                    restores_after_handoff: 0,
                    hot_instance_count: 0,
                    hot_instances: Vec::new(),
                })
                .collect(),
            ownership_transitions: ownerships
                .iter()
                .map(|ownership| OwnershipTransitionDebug {
                    partition_id: ownership.partition_id,
                    status: "acquired",
                    epoch: ownership.owner_epoch,
                    transitioned_at: ownership.last_transition_at,
                    reason: "initial ownership claim".to_owned(),
                })
                .collect(),
        }
    }

    fn sync_from_runtime(&mut self, partition_id: i32, runtime: &ExecutorRuntime) {
        let partition = self.partition_mut(partition_id);
        partition.cache_hits = runtime.hits;
        partition.cache_misses = runtime.misses;
        partition.restores_from_projection = runtime.restores_from_projection;
        partition.restores_from_snapshot_replay = runtime.restores_from_snapshot_replay;
        partition.restores_after_handoff = runtime.restores_after_handoff;
        partition.hot_instance_count = runtime.cache.len();
        partition.hot_instances = runtime
            .cache
            .values()
            .map(|entry| HotInstanceDebug {
                partition_id,
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
        partition.hot_instances.sort_by(|left, right| {
            left.tenant_id.cmp(&right.tenant_id).then(left.instance_id.cmp(&right.instance_id))
        });
        self.recompute_totals();
    }

    fn update_ownership(
        &mut self,
        record: &PartitionOwnershipRecord,
        status: &'static str,
        reason: impl Into<String>,
    ) {
        self.partition_mut(record.partition_id).ownership = map_partition_ownership(record, status);
        self.record_transition(
            record.partition_id,
            status,
            record.owner_epoch,
            record.last_transition_at,
            reason,
        );
    }

    fn mark_ownership_lost(
        &mut self,
        partition_id: i32,
        epoch: u64,
        lease_expires_at: chrono::DateTime<chrono::Utc>,
        reason: impl Into<String>,
    ) {
        let now = chrono::Utc::now();
        let ownership = &mut self.partition_mut(partition_id).ownership;
        ownership.status = "lost";
        ownership.epoch = epoch;
        ownership.last_transition_at = now;
        ownership.lease_expires_at = lease_expires_at;
        self.record_transition(partition_id, "lost", epoch, now, reason);
    }

    fn record_transition(
        &mut self,
        partition_id: i32,
        status: &'static str,
        epoch: u64,
        transitioned_at: chrono::DateTime<chrono::Utc>,
        reason: impl Into<String>,
    ) {
        self.ownership_transitions.insert(
            0,
            OwnershipTransitionDebug {
                partition_id,
                status,
                epoch,
                transitioned_at,
                reason: reason.into(),
            },
        );
        if self.ownership_transitions.len() > OWNERSHIP_TRANSITION_HISTORY_LIMIT {
            self.ownership_transitions.truncate(OWNERSHIP_TRANSITION_HISTORY_LIMIT);
        }
    }

    fn partition_mut(&mut self, partition_id: i32) -> &mut PartitionRuntimeDebug {
        if let Some(index) =
            self.partitions.iter().position(|entry| entry.partition_id == partition_id)
        {
            return &mut self.partitions[index];
        }
        self.partitions.push(PartitionRuntimeDebug {
            partition_id,
            ownership: PartitionOwnership {
                partition_id,
                owner_id: String::new(),
                status: "unknown",
                epoch: 0,
                acquired_at: chrono::Utc::now(),
                last_transition_at: chrono::Utc::now(),
                lease_expires_at: chrono::Utc::now(),
            },
            cache_hits: 0,
            cache_misses: 0,
            restores_from_projection: 0,
            restores_from_snapshot_replay: 0,
            restores_after_handoff: 0,
            hot_instance_count: 0,
            hot_instances: Vec::new(),
        });
        self.partitions.last_mut().expect("partition debug inserted")
    }

    fn recompute_totals(&mut self) {
        self.partitions.sort_by_key(|entry| entry.partition_id);
        self.cache_hits = self.partitions.iter().map(|entry| entry.cache_hits).sum();
        self.cache_misses = self.partitions.iter().map(|entry| entry.cache_misses).sum();
        self.restores_from_projection =
            self.partitions.iter().map(|entry| entry.restores_from_projection).sum();
        self.restores_from_snapshot_replay =
            self.partitions.iter().map(|entry| entry.restores_from_snapshot_replay).sum();
        self.restores_after_handoff =
            self.partitions.iter().map(|entry| entry.restores_after_handoff).sum();
        self.hot_instance_count =
            self.partitions.iter().map(|entry| entry.hot_instance_count).sum();
    }
}

async fn load_cached_or_snapshot_state(
    store: &WorkflowStore,
    broker: &BrokerConfig,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    partition_id: i32,
    ownership_epoch: u64,
    event: &EventEnvelope<WorkflowEvent>,
) -> Result<Option<HotStateRecord>> {
    if let Some(mut record) = runtime.get(&event.tenant_id, &event.instance_id) {
        record.restore_source = RestoreSource::Cache;
        sync_debug_state(debug_state, partition_id, runtime);
        return Ok(Some(record));
    }

    if let Some(snapshot) =
        store.get_snapshot_for_run(&event.tenant_id, &event.instance_id, &event.run_id).await?
    {
        let record =
            restore_from_snapshot_tail(store, broker, snapshot, Some(event.event_id)).await?;
        runtime.restores_from_snapshot_replay += 1;
        if ownership_epoch > 1 {
            runtime.restores_after_handoff += 1;
        }
        runtime.put(record.clone());
        sync_debug_state(debug_state, partition_id, runtime);
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
        sync_debug_state(debug_state, partition_id, runtime);
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
    partition_id: i32,
    record: &mut HotStateRecord,
    state: &WorkflowInstanceState,
) -> Result<()> {
    ensure_active_partition_ownership(store, runtime, debug_state, lease_state).await?;
    store.upsert_instance(state).await?;
    let snapshot_safe = state.artifact_hash.is_none()
        || state.artifact_execution.is_some()
        || matches!(
            state.status,
            fabrik_workflow::WorkflowStatus::Completed | fabrik_workflow::WorkflowStatus::Failed
        );
    let should_snapshot = snapshot_safe
        && (record.last_snapshot_event_count.is_none()
            || (state.event_count - record.last_snapshot_event_count.unwrap_or_default())
                >= runtime.snapshot_interval_events as i64
            || matches!(
                state.status,
                fabrik_workflow::WorkflowStatus::Completed
                    | fabrik_workflow::WorkflowStatus::Failed
            ));
    if should_snapshot {
        store.put_snapshot(state).await?;
        record.last_snapshot_event_count = Some(state.event_count);
    }
    record.state = state.clone();
    runtime.put(record.clone());
    sync_debug_state(debug_state, partition_id, runtime);
    Ok(())
}

async fn restore_from_snapshot_tail(
    store: &WorkflowStore,
    broker: &BrokerConfig,
    snapshot: fabrik_store::WorkflowStateSnapshot,
    exclude_event_id: Option<Uuid>,
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
    let tail_events = history[tail_start..]
        .iter()
        .filter(|event| Some(event.event_id) != exclude_event_id)
        .cloned()
        .collect::<Vec<_>>();
    let tail = tail_events.as_slice();
    let state = if let Some(definition_version) = snapshot.state.definition_version {
        if let Some(artifact) = store
            .get_artifact_version(
                &snapshot.tenant_id,
                &snapshot.state.definition_id,
                definition_version,
            )
            .await?
        {
            if snapshot.state.artifact_execution.is_some()
                || matches!(
                    snapshot.state.status,
                    fabrik_workflow::WorkflowStatus::Completed
                        | fabrik_workflow::WorkflowStatus::Failed
                )
            {
                replay_compiled_history_trace_from_snapshot(
                    tail,
                    &snapshot.state,
                    &artifact,
                    snapshot.event_count,
                    snapshot.last_event_id,
                    &snapshot.last_event_type,
                )?
                .final_state
            } else {
                let replay_history = history
                    .iter()
                    .filter(|event| Some(event.event_id) != exclude_event_id)
                    .cloned()
                    .collect::<Vec<_>>();
                fabrik_workflow::replay_compiled_history_trace(&replay_history, &artifact)?
                    .final_state
            }
        } else {
            replay_history_trace_from_snapshot(
                tail,
                &snapshot.state,
                snapshot.event_count,
                snapshot.last_event_id,
                &snapshot.last_event_type,
            )?
            .final_state
        }
    } else {
        replay_history_trace_from_snapshot(
            tail,
            &snapshot.state,
            snapshot.event_count,
            snapshot.last_event_id,
            &snapshot.last_event_type,
        )?
        .final_state
    };
    Ok(HotStateRecord {
        state,
        last_snapshot_event_count: Some(snapshot.event_count),
        restore_source: RestoreSource::SnapshotReplay,
    })
}

fn sync_debug_state(
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    partition_id: i32,
    runtime: &ExecutorRuntime,
) {
    if let Ok(mut state) = debug_state.lock() {
        state.sync_from_runtime(partition_id, runtime);
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
                            snapshot.partition_id,
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
        sync_debug_state(debug_state, snapshot.partition_id, runtime);
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
        sync_debug_state(debug_state, snapshot.partition_id, runtime);
        if let Ok(mut state) = lease_state.lock() {
            state.active = false;
        }
        if let Ok(mut debug) = debug_state.lock() {
            debug.mark_ownership_lost(
                snapshot.partition_id,
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
) -> Result<Json<Vec<PartitionOwnership>>, (StatusCode, Json<ExecutorErrorResponse>)> {
    let snapshot = state
        .debug
        .lock()
        .map(|state| state.partitions.iter().map(|partition| partition.ownership.clone()).collect())
        .map_err(|_| internal_executor_error("executor debug state lock poisoned"))?;
    Ok(Json(snapshot))
}

async fn get_broker_debug(
    AxumState(state): AxumState<ExecutorAppState>,
) -> Result<Json<WorkflowTopicTopology>, (StatusCode, Json<ExecutorErrorResponse>)> {
    let topology = describe_workflow_topic(&state.broker, "executor-service-debug").await.map_err(
        |error| internal_executor_error(format!("failed to describe workflow topic: {error}")),
    )?;
    Ok(Json(topology))
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
        .partitions
        .iter()
        .flat_map(|partition| partition.hot_instances.iter())
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
            try_dispatch_next_message(
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
            try_dispatch_next_message(
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
}

fn build_retry_timer_id(kind: RetryTargetKind, target_id: &str, attempt: u32) -> String {
    let kind = match kind {
        RetryTargetKind::Step => "step",
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
        _ => return None,
    };
    Some((kind, target_id.to_owned(), attempt))
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

#[derive(Debug, Deserialize)]
struct InternalQueryRequest {
    #[serde(default)]
    args: Value,
}

#[derive(Debug, Serialize)]
struct InternalQueryResponse {
    result: Value,
    consistency: &'static str,
    source: &'static str,
}

async fn execute_internal_query(
    Path((tenant_id, instance_id, query_name)): Path<(String, String, String)>,
    AxumState(state): AxumState<ExecutorAppState>,
    Json(request): Json<InternalQueryRequest>,
) -> Result<Json<InternalQueryResponse>, (StatusCode, Json<ExecutorErrorResponse>)> {
    let instance = state.store.get_instance(&tenant_id, &instance_id).await.map_err(|error| {
        internal_executor_error(format!("failed to load workflow instance: {error}"))
    })?;
    let Some(instance) = instance else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ExecutorErrorResponse {
                message: format!("workflow instance {instance_id} not found"),
            }),
        ));
    };
    let Some(version) = instance.definition_version else {
        return Err(internal_executor_error(
            "workflow instance is missing a pinned artifact version",
        ));
    };
    let artifact = state
        .store
        .get_artifact_version(&tenant_id, &instance.definition_id, version)
        .await
        .map_err(|error| {
            internal_executor_error(format!("failed to load workflow artifact: {error}"))
        })?
        .ok_or_else(|| internal_executor_error("workflow artifact not found"))?;
    let result = artifact
        .evaluate_query(
            &query_name,
            &request.args,
            instance.artifact_execution.clone().unwrap_or_default(),
        )
        .map_err(|error| {
            (StatusCode::BAD_REQUEST, Json(ExecutorErrorResponse { message: error.to_string() }))
        })?;
    Ok(Json(InternalQueryResponse {
        result,
        consistency: "strong",
        source: if instance.status == fabrik_workflow::WorkflowStatus::Running {
            "hot_owner"
        } else {
            "replay"
        },
    }))
}

async fn try_dispatch_buffered_update(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &WorkflowPublisher,
    state: &WorkflowInstanceState,
    event: &EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    if state.status.is_terminal() {
        return Ok(());
    }
    if state
        .artifact_execution
        .as_ref()
        .and_then(|execution| execution.active_update.as_ref())
        .is_some()
    {
        return Ok(());
    }
    let Some(artifact) = load_pinned_artifact(store, event, state).await? else {
        return Ok(());
    };
    let oldest_signal = store
        .get_oldest_pending_signal(&event.tenant_id, &event.instance_id, &event.run_id)
        .await?;
    let Some(update) = store
        .get_oldest_requested_update(&event.tenant_id, &event.instance_id, &event.run_id)
        .await?
    else {
        return Ok(());
    };
    if choose_pending_inbound_kind(oldest_signal.as_ref(), Some(&update))
        != Some(PendingInboundKind::Update)
    {
        return Ok(());
    }
    if !artifact.has_update(&update.update_name) {
        store
            .complete_update(
                &update.tenant_id,
                &update.instance_id,
                &update.run_id,
                &update.update_id,
                None,
                Some(&format!("unknown update handler {}", update.update_name)),
                update.source_event_id,
                chrono::Utc::now(),
            )
            .await?;
        ensure_active_partition_ownership(store, runtime, debug_state, lease_state).await?;
        let payload = WorkflowEvent::WorkflowUpdateRejected {
            update_id: update.update_id.clone(),
            update_name: update.update_name.clone(),
            error: format!("unknown update handler {}", update.update_name),
        };
        let mut envelope = EventEnvelope::new(
            payload.event_type(),
            source_identity(event, "executor-service"),
            payload,
        );
        envelope.causation_id = Some(update.source_event_id);
        envelope.correlation_id = event.correlation_id.or(Some(update.source_event_id));
        envelope.dedupe_key = Some(format!("update-rejected:{}", update.update_id));
        publisher.publish(&envelope, &envelope.partition_key).await?;
        return Ok(());
    }
    let accepted_event_id = Uuid::now_v7();
    if !store
        .accept_update(
            &update.tenant_id,
            &update.instance_id,
            &update.run_id,
            &update.update_id,
            accepted_event_id,
            chrono::Utc::now(),
        )
        .await?
    {
        return Ok(());
    }

    ensure_active_partition_ownership(store, runtime, debug_state, lease_state).await?;
    let payload = WorkflowEvent::WorkflowUpdateAccepted {
        update_id: update.update_id.clone(),
        update_name: update.update_name.clone(),
        payload: update.payload.clone(),
    };
    let mut envelope = EventEnvelope::new(
        payload.event_type(),
        source_identity(event, "executor-service"),
        payload,
    );
    envelope.event_id = accepted_event_id;
    envelope.occurred_at = chrono::Utc::now();
    envelope.causation_id = Some(update.source_event_id);
    envelope.correlation_id = event.correlation_id.or(Some(update.source_event_id));
    envelope.dedupe_key = Some(format!("update-accepted:{}", update.update_id));
    publisher.publish(&envelope, &envelope.partition_key).await?;
    Ok(())
}

async fn publish_child_terminal_reflection(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &WorkflowPublisher,
    event: &EventEnvelope<WorkflowEvent>,
    parent_child: &fabrik_store::WorkflowChildRecord,
) -> Result<()> {
    ensure_active_partition_ownership(store, runtime, debug_state, lease_state).await?;
    let parent_instance = store.get_instance(&event.tenant_id, &parent_child.instance_id).await?;
    let Some(parent_instance) = parent_instance else {
        return Ok(());
    };
    let payload = match &event.payload {
        WorkflowEvent::WorkflowCompleted { output } => WorkflowEvent::ChildWorkflowCompleted {
            child_id: parent_child.child_id.clone(),
            child_run_id: event.run_id.clone(),
            output: output.clone(),
        },
        WorkflowEvent::WorkflowFailed { reason } => WorkflowEvent::ChildWorkflowFailed {
            child_id: parent_child.child_id.clone(),
            child_run_id: event.run_id.clone(),
            error: reason.clone(),
        },
        WorkflowEvent::WorkflowCancelled { reason } => WorkflowEvent::ChildWorkflowCancelled {
            child_id: parent_child.child_id.clone(),
            child_run_id: event.run_id.clone(),
            reason: reason.clone(),
        },
        WorkflowEvent::WorkflowTerminated { reason } => WorkflowEvent::ChildWorkflowTerminated {
            child_id: parent_child.child_id.clone(),
            child_run_id: event.run_id.clone(),
            reason: reason.clone(),
        },
        _ => return Ok(()),
    };
    let mut envelope = EventEnvelope::new(
        payload.event_type(),
        WorkflowIdentity::new(
            event.tenant_id.clone(),
            parent_instance.definition_id.clone(),
            parent_instance.definition_version.unwrap_or(event.definition_version),
            parent_instance.artifact_hash.clone().unwrap_or_else(|| event.artifact_hash.clone()),
            parent_child.instance_id.clone(),
            parent_child.run_id.clone(),
            "executor-service",
        ),
        payload,
    );
    envelope.causation_id = Some(event.event_id);
    envelope.correlation_id = event.correlation_id.or(Some(event.event_id));
    publisher.publish(&envelope, &envelope.partition_key).await?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingInboundKind {
    Signal,
    Update,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParentCloseAction {
    Terminate,
    RequestCancel,
    Abandon,
}

fn choose_pending_inbound_kind(
    signal: Option<&fabrik_store::WorkflowSignalRecord>,
    update: Option<&fabrik_store::WorkflowUpdateRecord>,
) -> Option<PendingInboundKind> {
    match (signal, update) {
        (Some(signal), Some(update)) => {
            if update.requested_at < signal.enqueued_at {
                Some(PendingInboundKind::Update)
            } else if signal.enqueued_at < update.requested_at {
                Some(PendingInboundKind::Signal)
            } else if update.update_id <= signal.signal_id {
                Some(PendingInboundKind::Update)
            } else {
                Some(PendingInboundKind::Signal)
            }
        }
        (Some(_), None) => Some(PendingInboundKind::Signal),
        (None, Some(_)) => Some(PendingInboundKind::Update),
        (None, None) => None,
    }
}

async fn try_dispatch_next_message(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &WorkflowPublisher,
    state: &WorkflowInstanceState,
    event: &EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    if state.status.is_terminal() {
        return Ok(());
    }
    if state
        .artifact_execution
        .as_ref()
        .and_then(|execution| execution.active_update.as_ref())
        .is_some()
    {
        return Ok(());
    }
    let oldest_signal = store
        .get_oldest_pending_signal(&event.tenant_id, &event.instance_id, &event.run_id)
        .await?;
    let oldest_update = store
        .get_oldest_requested_update(&event.tenant_id, &event.instance_id, &event.run_id)
        .await?;
    match choose_pending_inbound_kind(oldest_signal.as_ref(), oldest_update.as_ref()) {
        Some(PendingInboundKind::Signal) => {
            try_dispatch_buffered_signal(
                store,
                runtime,
                debug_state,
                lease_state,
                publisher,
                state,
                event,
            )
            .await
        }
        Some(PendingInboundKind::Update) => {
            try_dispatch_buffered_update(
                store,
                runtime,
                debug_state,
                lease_state,
                publisher,
                state,
                event,
            )
            .await
        }
        None => Ok(()),
    }
}

async fn enforce_parent_close_policies(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &WorkflowPublisher,
    event: &EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    let children = store
        .list_open_children_for_run(&event.tenant_id, &event.instance_id, &event.run_id)
        .await?;
    for child in children {
        match parse_parent_close_action(&child.parent_close_policy) {
            ParentCloseAction::Abandon => {}
            ParentCloseAction::RequestCancel => {
                if let Some(child_run_id) = &child.child_run_id {
                    emit_parent_close_child_event(
                        store,
                        runtime,
                        debug_state,
                        lease_state,
                        publisher,
                        event,
                        &child,
                        child_run_id,
                        WorkflowEvent::WorkflowCancellationRequested {
                            reason: format!(
                                "cancel requested by parent close policy for parent run {}",
                                event.run_id
                            ),
                        },
                    )
                    .await?;
                } else {
                    store
                        .complete_child(
                            &child.tenant_id,
                            &child.instance_id,
                            &child.run_id,
                            &child.child_id,
                            "cancelled",
                            None,
                            Some("cancel requested before child start"),
                            event.event_id,
                            event.occurred_at,
                        )
                        .await?;
                }
            }
            ParentCloseAction::Terminate => {
                if let Some(child_run_id) = &child.child_run_id {
                    emit_parent_close_child_event(
                        store,
                        runtime,
                        debug_state,
                        lease_state,
                        publisher,
                        event,
                        &child,
                        child_run_id,
                        WorkflowEvent::WorkflowTerminated {
                            reason: format!(
                                "terminated by parent close policy for parent run {}",
                                event.run_id
                            ),
                        },
                    )
                    .await?;
                } else {
                    store
                        .complete_child(
                            &child.tenant_id,
                            &child.instance_id,
                            &child.run_id,
                            &child.child_id,
                            "terminated",
                            None,
                            Some("terminated before child start"),
                            event.event_id,
                            event.occurred_at,
                        )
                        .await?;
                }
            }
        }
    }
    Ok(())
}

fn parse_parent_close_action(value: &str) -> ParentCloseAction {
    match value {
        "ABANDON" => ParentCloseAction::Abandon,
        "REQUEST_CANCEL" => ParentCloseAction::RequestCancel,
        _ => ParentCloseAction::Terminate,
    }
}

async fn emit_parent_close_child_event(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &WorkflowPublisher,
    parent_event: &EventEnvelope<WorkflowEvent>,
    child: &fabrik_store::WorkflowChildRecord,
    child_run_id: &str,
    payload: WorkflowEvent,
) -> Result<()> {
    ensure_active_partition_ownership(store, runtime, debug_state, lease_state).await?;
    let definition =
        store.get_latest_artifact(&child.tenant_id, &child.child_definition_id).await?;
    let (definition_version, artifact_hash) = if let Some(artifact) = definition {
        (artifact.definition_version, artifact.artifact_hash)
    } else {
        let definition = store
            .get_latest_definition(&child.tenant_id, &child.child_definition_id)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("child definition {} not found", child.child_definition_id)
            })?;
        (definition.version, fabrik_workflow::artifact_hash(&definition))
    };
    let mut envelope = EventEnvelope::new(
        payload.event_type(),
        WorkflowIdentity::new(
            child.tenant_id.clone(),
            child.child_definition_id.clone(),
            definition_version,
            artifact_hash,
            child.child_workflow_id.clone(),
            child_run_id.to_owned(),
            "executor-service",
        ),
        payload,
    );
    envelope.causation_id = Some(parent_event.event_id);
    envelope.correlation_id = parent_event.correlation_id.or(Some(parent_event.event_id));
    publisher.publish(&envelope, &envelope.partition_key).await?;
    Ok(())
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
    if state.status.is_terminal() {
        return Ok(());
    }
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
    let oldest_update = store
        .get_oldest_requested_update(&event.tenant_id, &event.instance_id, &event.run_id)
        .await?;
    if choose_pending_inbound_kind(Some(&oldest_signal), oldest_update.as_ref())
        != Some(PendingInboundKind::Signal)
    {
        return Ok(());
    }
    if oldest_signal.signal_type != expected_signal_type {
        return Ok(());
    }

    let Some((signal, newly_claimed)) = store
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
    if !newly_claimed {
        return Ok(());
    }
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

async fn reject_outstanding_updates_for_closed_run(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &WorkflowPublisher,
    event: &EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    let error = format!(
        "workflow run {} is already {}",
        event.run_id,
        terminal_status_label(&event.payload)
    );
    for update in
        store.list_updates_for_run(&event.tenant_id, &event.instance_id, &event.run_id).await?
    {
        if !matches!(
            update.status,
            fabrik_store::WorkflowUpdateStatus::Requested
                | fabrik_store::WorkflowUpdateStatus::Accepted
        ) {
            continue;
        }
        if !store
            .complete_update(
                &update.tenant_id,
                &update.instance_id,
                &update.run_id,
                &update.update_id,
                None,
                Some(&error),
                event.event_id,
                event.occurred_at,
            )
            .await?
        {
            continue;
        }
        ensure_active_partition_ownership(store, runtime, debug_state, lease_state).await?;
        let payload = WorkflowEvent::WorkflowUpdateRejected {
            update_id: update.update_id.clone(),
            update_name: update.update_name.clone(),
            error: error.clone(),
        };
        let mut envelope = EventEnvelope::new(
            payload.event_type(),
            source_identity(event, "executor-service"),
            payload,
        );
        envelope.causation_id = Some(event.event_id);
        envelope.correlation_id = event.correlation_id.or(Some(event.event_id));
        envelope.dedupe_key = Some(format!("update-rejected:{}", update.update_id));
        publisher.publish(&envelope, &envelope.partition_key).await?;
    }
    Ok(())
}

fn terminal_status_label(payload: &WorkflowEvent) -> &'static str {
    match payload {
        WorkflowEvent::WorkflowCompleted { .. } => "completed",
        WorkflowEvent::WorkflowFailed { .. } => "failed",
        WorkflowEvent::WorkflowCancelled { .. } => "cancelled",
        WorkflowEvent::WorkflowTerminated { .. } => "terminated",
        _ => "closed",
    }
}

fn event_is_terminal(payload: &WorkflowEvent) -> bool {
    matches!(
        payload,
        WorkflowEvent::WorkflowCompleted { .. }
            | WorkflowEvent::WorkflowFailed { .. }
            | WorkflowEvent::WorkflowCancelled { .. }
            | WorkflowEvent::WorkflowTerminated { .. }
    )
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

    if let Some(threshold) = runtime.continue_as_new_activity_attempt_threshold {
        let attempts =
            store.count_activity_attempts_for_run(tenant_id, instance_id, run_id).await?;
        if attempts >= threshold {
            return Ok(Some(format!("activity_attempts:{threshold}")));
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
    for emission in &plan.emissions {
        match emission.event {
            WorkflowEvent::WorkflowCompleted { .. } => {
                state.status = fabrik_workflow::WorkflowStatus::Completed;
            }
            WorkflowEvent::WorkflowFailed { .. } => {
                state.status = fabrik_workflow::WorkflowStatus::Failed;
            }
            WorkflowEvent::WorkflowCancelled { .. } => {
                state.status = fabrik_workflow::WorkflowStatus::Cancelled;
            }
            WorkflowEvent::WorkflowTerminated { .. } => {
                state.status = fabrik_workflow::WorkflowStatus::Terminated;
            }
            _ => {}
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use fabrik_store::{
        WorkflowSignalRecord, WorkflowSignalStatus, WorkflowUpdateRecord, WorkflowUpdateStatus,
    };
    use fabrik_workflow::WorkflowStatus;
    use serde_json::json;

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

    fn demo_signal(signal_id: &str, enqueued_at: chrono::DateTime<Utc>) -> WorkflowSignalRecord {
        WorkflowSignalRecord {
            tenant_id: "tenant-a".to_owned(),
            instance_id: "instance-1".to_owned(),
            run_id: "run-1".to_owned(),
            signal_id: signal_id.to_owned(),
            signal_type: "external.approved".to_owned(),
            dedupe_key: None,
            payload: json!({}),
            status: WorkflowSignalStatus::Queued,
            source_event_id: Uuid::now_v7(),
            dispatch_event_id: None,
            consumed_event_id: None,
            enqueued_at,
            consumed_at: None,
            updated_at: enqueued_at,
        }
    }

    fn demo_update(update_id: &str, requested_at: chrono::DateTime<Utc>) -> WorkflowUpdateRecord {
        WorkflowUpdateRecord {
            tenant_id: "tenant-a".to_owned(),
            instance_id: "instance-1".to_owned(),
            run_id: "run-1".to_owned(),
            update_id: update_id.to_owned(),
            update_name: "setValue".to_owned(),
            request_id: None,
            payload: json!({}),
            status: WorkflowUpdateStatus::Requested,
            output: None,
            error: None,
            source_event_id: Uuid::now_v7(),
            accepted_event_id: None,
            completed_event_id: None,
            requested_at,
            accepted_at: None,
            completed_at: None,
            updated_at: requested_at,
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

    #[test]
    fn pending_inbound_order_prefers_earliest_timestamp() {
        let now = Utc::now();
        let signal = demo_signal("sig-b", now + chrono::Duration::seconds(5));
        let update = demo_update("upd-a", now);

        assert_eq!(
            choose_pending_inbound_kind(Some(&signal), Some(&update)),
            Some(PendingInboundKind::Update)
        );
        assert_eq!(
            choose_pending_inbound_kind(
                Some(&demo_signal("sig-a", now)),
                Some(&demo_update("upd-b", now + chrono::Duration::seconds(5)))
            ),
            Some(PendingInboundKind::Signal)
        );
    }

    #[test]
    fn pending_inbound_order_uses_stable_id_tiebreaker() {
        let now = Utc::now();
        let signal = demo_signal("sig-b", now);
        let update = demo_update("sig-a", now);

        assert_eq!(
            choose_pending_inbound_kind(Some(&signal), Some(&update)),
            Some(PendingInboundKind::Update)
        );
    }

    #[test]
    fn parent_close_action_parser_matches_supported_variants() {
        assert_eq!(parse_parent_close_action("ABANDON"), ParentCloseAction::Abandon);
        assert_eq!(parse_parent_close_action("REQUEST_CANCEL"), ParentCloseAction::RequestCancel);
        assert_eq!(parse_parent_close_action("TERMINATE"), ParentCloseAction::Terminate);
        assert_eq!(parse_parent_close_action("unknown"), ParentCloseAction::Terminate);
    }

    #[test]
    fn apply_compiled_plan_marks_terminal_status_from_emissions() {
        let mut state = demo_state("instance-terminal");
        apply_compiled_plan(
            &mut state,
            &CompiledExecutionPlan {
                workflow_version: 1,
                final_state: "done".to_owned(),
                emissions: vec![ExecutionEmission {
                    event: WorkflowEvent::WorkflowCompleted { output: json!({"ok": true}) },
                    state: Some("done".to_owned()),
                }],
                execution_state: fabrik_workflow::ArtifactExecutionState::default(),
                context: Some(json!({"ok": true})),
                output: Some(json!({"ok": true})),
            },
        );

        assert_eq!(state.status, WorkflowStatus::Completed);
    }
}
