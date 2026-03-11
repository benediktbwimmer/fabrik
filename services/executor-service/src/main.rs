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
use fabrik_events::{
    EventEnvelope, WorkflowEvent, WorkflowIdentity, WorkflowTurnRouting, workflow_turn_routing,
};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::{ConsumedSignalRecord, PartitionOwnershipRecord, WorkflowStore};
use fabrik_worker_protocol::activity_worker::{
    CompleteWorkflowTaskRequest, FailWorkflowTaskRequest, PollWorkflowTaskRequest, WorkflowTask,
    workflow_worker_api_client::WorkflowWorkerApiClient,
};
use fabrik_workflow::{
    ArtifactEntrypoint, CompiledExecutionPlan, CompiledStateNode, CompiledWorkflow,
    CompiledWorkflowArtifact, ExecutionEmission, ExecutionTurnContext, Expression,
    WorkflowInstanceState, replay_compiled_history_trace_from_snapshot,
    replay_history_trace_from_snapshot,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::net::TcpListener;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration as StdDuration, Instant};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};
use uuid::Uuid;

const OWNERSHIP_TRANSITION_HISTORY_LIMIT: usize = 16;
const SIGNAL_DISPATCH_EVENT_NAMESPACE: Uuid =
    Uuid::from_u128(0x2f8a_ff40_8508_4b85_9082_55d2_eb0b_9f19);

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    if matches!(args.next().as_deref(), Some("benchmark-milestone")) {
        init_tracing("info");
        return run_milestone_benchmark(args.collect()).await;
    }

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
    let matching_endpoint = env::var("MATCHING_SERVICE_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:50051".to_owned());
    let workflow_worker_build_id =
        env::var("WORKFLOW_WORKER_BUILD_ID").unwrap_or_else(|_| "dev-workflow-build".to_owned());
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
            matching_endpoint.clone(),
            workflow_worker_build_id.clone(),
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
                matching_endpoint.clone(),
                workflow_worker_build_id.clone(),
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
    .route("/debug/hybrid-routing", get(get_hybrid_routing_debug))
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

struct MilestoneBenchmarkConfig {
    items_per_run: usize,
    iterations: usize,
}

impl MilestoneBenchmarkConfig {
    fn parse(args: Vec<String>) -> Result<Self> {
        let mut config = Self { items_per_run: 1000, iterations: 5 };
        let mut index = 0usize;
        while index < args.len() {
            match args[index].as_str() {
                "--items" => {
                    let value = args
                        .get(index + 1)
                        .ok_or_else(|| anyhow::anyhow!("--items requires a value"))?;
                    config.items_per_run = value.parse()?;
                    index += 2;
                }
                "--iterations" => {
                    let value = args
                        .get(index + 1)
                        .ok_or_else(|| anyhow::anyhow!("--iterations requires a value"))?;
                    config.iterations = value.parse()?;
                    index += 2;
                }
                unknown => {
                    return Err(anyhow::anyhow!(
                        "unknown benchmark argument {unknown}; supported flags: --items, --iterations"
                    ));
                }
            }
        }
        Ok(config)
    }
}

struct BenchmarkPostgres {
    container_name: String,
    database_url: String,
}

impl BenchmarkPostgres {
    fn start() -> Result<Self> {
        let container_name = format!("fabrik-executor-bench-pg-{}", Uuid::now_v7());
        let image = env::var("FABRIK_TEST_POSTGRES_IMAGE")
            .unwrap_or_else(|_| "postgres:16-alpine".to_owned());
        let output = Command::new("docker")
            .args([
                "run",
                "--detach",
                "--rm",
                "--name",
                &container_name,
                "--env",
                "POSTGRES_USER=fabrik",
                "--env",
                "POSTGRES_PASSWORD=fabrik",
                "--env",
                "POSTGRES_DB=fabrik_bench",
                "--publish-all",
                &image,
            ])
            .output()?;
        if !output.status.success() {
            anyhow::bail!(
                "failed to start postgres benchmark container: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }

        let host_port = wait_for_docker_port(&container_name, "5432/tcp")?;
        let database_url =
            format!("postgres://fabrik:fabrik@127.0.0.1:{host_port}/fabrik_bench?sslmode=disable");
        Ok(Self { container_name, database_url })
    }

    async fn connect_store(&self) -> Result<WorkflowStore> {
        let deadline = Instant::now() + StdDuration::from_secs(30);
        loop {
            match WorkflowStore::connect(&self.database_url).await {
                Ok(store) => {
                    store.init().await?;
                    return Ok(store);
                }
                Err(error) if Instant::now() < deadline => {
                    let _ = error;
                    tokio::time::sleep(StdDuration::from_millis(250)).await;
                }
                Err(error) => {
                    let logs = docker_logs(&self.container_name).unwrap_or_default();
                    return Err(anyhow::Error::from(error).context(format!(
                        "postgres benchmark container {} did not become ready; logs:\n{}",
                        self.container_name, logs
                    )));
                }
            }
        }
    }
}

impl Drop for BenchmarkPostgres {
    fn drop(&mut self) {
        let _ = cleanup_container(&self.container_name);
    }
}

struct BenchmarkRedpanda {
    container_name: String,
    broker: BrokerConfig,
}

impl BenchmarkRedpanda {
    fn start() -> Result<Self> {
        let kafka_port = choose_free_port()?;
        let container_name = format!("fabrik-executor-bench-rp-{}", Uuid::now_v7());
        let image = env::var("FABRIK_TEST_REDPANDA_IMAGE")
            .unwrap_or_else(|_| "docker.redpanda.com/redpandadata/redpanda:v25.1.2".to_owned());
        let topic = format!("workflow-events-bench-{}", Uuid::now_v7());
        let output = Command::new("docker")
            .args([
                "run",
                "--detach",
                "--rm",
                "--name",
                &container_name,
                "--publish",
                &format!("{kafka_port}:{kafka_port}"),
                &image,
                "redpanda",
                "start",
                "--overprovisioned",
                "--smp",
                "1",
                "--memory",
                "1G",
                "--reserve-memory",
                "0M",
                "--node-id",
                "0",
                "--check=false",
                "--kafka-addr",
                &format!("PLAINTEXT://0.0.0.0:9092,OUTSIDE://0.0.0.0:{kafka_port}"),
                "--advertise-kafka-addr",
                &format!("PLAINTEXT://127.0.0.1:9092,OUTSIDE://127.0.0.1:{kafka_port}"),
                "--rpc-addr",
                "0.0.0.0:33145",
                "--advertise-rpc-addr",
                "127.0.0.1:33145",
            ])
            .output()?;
        if !output.status.success() {
            anyhow::bail!(
                "failed to start redpanda benchmark container: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }

        Ok(Self {
            container_name,
            broker: BrokerConfig::new(format!("127.0.0.1:{kafka_port}"), topic, 1),
        })
    }

    async fn connect_publisher(&self) -> Result<WorkflowPublisher> {
        let deadline = Instant::now() + StdDuration::from_secs(45);
        loop {
            match WorkflowPublisher::new(&self.broker, "executor-benchmark").await {
                Ok(publisher) => return Ok(publisher),
                Err(error) if Instant::now() < deadline => {
                    let _ = error;
                    tokio::time::sleep(StdDuration::from_millis(500)).await;
                }
                Err(error) => {
                    let logs = docker_logs(&self.container_name).unwrap_or_default();
                    return Err(anyhow::Error::from(error).context(format!(
                        "redpanda benchmark container {} did not become ready; logs:\n{}",
                        self.container_name, logs
                    )));
                }
            }
        }
    }
}

impl Drop for BenchmarkRedpanda {
    fn drop(&mut self) {
        let _ = cleanup_container(&self.container_name);
    }
}

struct BenchmarkHarness {
    store: WorkflowStore,
    broker: BrokerConfig,
    publisher: WorkflowPublisher,
    debug_state: Arc<Mutex<ExecutorDebugState>>,
    lease_state: Arc<Mutex<LeaseState>>,
    partition_id: i32,
    task_queue: String,
    worker_id: String,
    worker_build_id: String,
}

struct ScenarioMeasurement {
    enqueue_duration: StdDuration,
    drain_duration: StdDuration,
    end_to_end_duration: StdDuration,
    task_rows: u64,
    tasks_executed: u64,
    restores: u64,
}

struct ScenarioSummary {
    name: &'static str,
    workload: &'static str,
    items_per_run: usize,
    iterations: usize,
    measurements: Vec<ScenarioMeasurement>,
}

#[derive(Debug)]
struct ConsumedSignal {
    signal_id: String,
    consumed_event_id: Uuid,
    consumed_at: chrono::DateTime<chrono::Utc>,
}

struct WorkflowPublishBuffer<'a> {
    publisher: &'a WorkflowPublisher,
    envelopes: Vec<EventEnvelope<WorkflowEvent>>,
}

impl<'a> WorkflowPublishBuffer<'a> {
    fn new(publisher: &'a WorkflowPublisher) -> Self {
        Self { publisher, envelopes: Vec::new() }
    }

    fn publish(&mut self, envelope: &EventEnvelope<WorkflowEvent>) {
        self.envelopes.push(envelope.clone());
    }

    async fn flush(&mut self) -> Result<()> {
        if self.envelopes.is_empty() {
            return Ok(());
        }
        let pending = std::mem::take(&mut self.envelopes);
        self.publisher.publish_all(&pending).await
    }
}

#[derive(Clone, PartialEq, Eq)]
struct WorkflowLookupKey {
    tenant_id: String,
    definition_id: String,
    definition_version: Option<u32>,
}

#[derive(Default)]
struct WorkflowLookupCache {
    key: Option<WorkflowLookupKey>,
    artifact: Option<Option<CompiledWorkflowArtifact>>,
    definition: Option<fabrik_workflow::WorkflowDefinition>,
}

async fn run_milestone_benchmark(args: Vec<String>) -> Result<()> {
    let config = MilestoneBenchmarkConfig::parse(args)?;
    info!(
        items_per_run = config.items_per_run,
        iterations = config.iterations,
        "starting milestone benchmark"
    );

    let postgres = BenchmarkPostgres::start()?;
    let redpanda = BenchmarkRedpanda::start()?;
    let store = postgres.connect_store().await?;
    let publisher = redpanda.connect_publisher().await?;
    let partition_id = 0;
    let ownership = store
        .claim_partition_ownership(partition_id, "executor-benchmark", StdDuration::from_secs(120))
        .await?
        .ok_or_else(|| anyhow::anyhow!("failed to claim benchmark partition ownership"))?;
    let debug_state =
        Arc::new(Mutex::new(ExecutorDebugState::new(&[ownership.clone()], 128, 10_000)));
    let lease_state = Arc::new(Mutex::new(LeaseState::from_record(&ownership)));
    let harness = BenchmarkHarness {
        store: store.clone(),
        broker: redpanda.broker.clone(),
        publisher,
        debug_state,
        lease_state,
        partition_id,
        task_queue: "bench".to_owned(),
        worker_id: "benchmark-worker".to_owned(),
        worker_build_id: "benchmark-build".to_owned(),
    };

    let signal_artifact = signal_benchmark_artifact();
    let activity_artifact = activity_benchmark_artifact();
    harness.store.put_artifact("tenant-bench", &signal_artifact).await?;
    harness.store.put_artifact("tenant-bench", &activity_artifact).await?;

    let signal_current = run_signal_benchmark(&harness, &signal_artifact, &config).await?;
    let activity_current =
        run_activity_resume_benchmark(&harness, &activity_artifact, &config).await?;
    print_scenario_summary(&signal_current);
    print_scenario_summary(&activity_current);

    Ok(())
}

async fn run_signal_benchmark(
    harness: &BenchmarkHarness,
    artifact: &CompiledWorkflowArtifact,
    config: &MilestoneBenchmarkConfig,
) -> Result<ScenarioSummary> {
    let mut measurements = Vec::with_capacity(config.iterations);
    for iteration in 0..config.iterations {
        let instance_id = format!("bench-signal-current-{iteration}");
        let run_id = format!("run-{instance_id}");
        seed_benchmark_run(harness, artifact, &instance_id, &run_id, "wait_signal").await?;

        let start = Instant::now();
        let enqueue_duration =
            enqueue_signal_burst(harness, artifact, &instance_id, &run_id, config.items_per_run)
                .await?;
        let task_rows = harness
            .store
            .count_workflow_tasks_for_run("tenant-bench", &instance_id, &run_id)
            .await?;
        let (drain_duration, tasks_executed, restores) =
            drain_benchmark_run(harness, config.items_per_run).await?;
        measurements.push(ScenarioMeasurement {
            enqueue_duration,
            drain_duration,
            end_to_end_duration: start.elapsed(),
            task_rows,
            tasks_executed,
            restores,
        });
    }

    Ok(ScenarioSummary {
        name: "signal_current",
        workload: "signals",
        items_per_run: config.items_per_run,
        iterations: config.iterations,
        measurements,
    })
}

async fn run_activity_resume_benchmark(
    harness: &BenchmarkHarness,
    artifact: &CompiledWorkflowArtifact,
    config: &MilestoneBenchmarkConfig,
) -> Result<ScenarioSummary> {
    let mut measurements = Vec::with_capacity(config.iterations);
    for iteration in 0..config.iterations {
        let instance_id = format!("bench-activity-current-{iteration}");
        let run_id = format!("run-{instance_id}");
        seed_benchmark_run(harness, artifact, &instance_id, &run_id, "step").await?;

        let start = Instant::now();
        let enqueue_duration = enqueue_activity_resume_burst(
            harness,
            artifact,
            &instance_id,
            &run_id,
            config.items_per_run,
        )
        .await?;
        let task_rows = harness
            .store
            .count_workflow_tasks_for_run("tenant-bench", &instance_id, &run_id)
            .await?;
        let (drain_duration, tasks_executed, restores) =
            drain_benchmark_run(harness, config.items_per_run).await?;
        measurements.push(ScenarioMeasurement {
            enqueue_duration,
            drain_duration,
            end_to_end_duration: start.elapsed(),
            task_rows,
            tasks_executed,
            restores,
        });
    }

    Ok(ScenarioSummary {
        name: "activity_resume_current",
        workload: "activity_resumes",
        items_per_run: config.items_per_run,
        iterations: config.iterations,
        measurements,
    })
}

async fn seed_benchmark_run(
    harness: &BenchmarkHarness,
    artifact: &CompiledWorkflowArtifact,
    instance_id: &str,
    run_id: &str,
    current_state: &str,
) -> Result<()> {
    let started_at = chrono::Utc::now();
    let trigger_event_id = Uuid::now_v7();
    harness
        .store
        .put_run_start(
            "tenant-bench",
            instance_id,
            run_id,
            &artifact.definition_id,
            Some(artifact.definition_version),
            Some(&artifact.artifact_hash),
            &harness.task_queue,
            trigger_event_id,
            started_at,
            None,
            None,
        )
        .await?;
    harness
        .store
        .upsert_instance(&WorkflowInstanceState {
            tenant_id: "tenant-bench".to_owned(),
            instance_id: instance_id.to_owned(),
            run_id: run_id.to_owned(),
            definition_id: artifact.definition_id.clone(),
            definition_version: Some(artifact.definition_version),
            artifact_hash: Some(artifact.artifact_hash.clone()),
            workflow_task_queue: harness.task_queue.clone(),
            sticky_workflow_build_id: None,
            sticky_workflow_poller_id: None,
            current_state: Some(current_state.to_owned()),
            context: Some(Value::Null),
            artifact_execution: Some(Default::default()),
            status: fabrik_workflow::WorkflowStatus::Running,
            input: Some(Value::Null),
            output: None,
            event_count: 1,
            last_event_id: trigger_event_id,
            last_event_type: "WorkflowStarted".to_owned(),
            updated_at: started_at,
        })
        .await?;
    Ok(())
}

async fn enqueue_signal_burst(
    harness: &BenchmarkHarness,
    artifact: &CompiledWorkflowArtifact,
    instance_id: &str,
    run_id: &str,
    items: usize,
) -> Result<StdDuration> {
    let start = Instant::now();
    for sequence in 0..items {
        let event = benchmark_signal_event(
            &artifact.definition_id,
            artifact.definition_version,
            &artifact.artifact_hash,
            instance_id,
            run_id,
            sequence,
            &harness.task_queue,
        );
        harness
            .store
            .queue_signal(
                "tenant-bench",
                instance_id,
                run_id,
                &format!("sig-{sequence}"),
                "external.approved",
                None,
                &json!({ "sequence": sequence }),
                event.event_id,
                event.occurred_at,
            )
            .await?;
        harness
            .store
            .enqueue_workflow_mailbox_message(
                harness.partition_id,
                &harness.task_queue,
                None,
                &event,
                fabrik_store::WorkflowMailboxKind::Signal,
                Some(&format!("sig-{sequence}")),
                Some("external.approved"),
                Some(&json!({ "sequence": sequence })),
            )
            .await?;
    }
    Ok(start.elapsed())
}

async fn enqueue_activity_resume_burst(
    harness: &BenchmarkHarness,
    artifact: &CompiledWorkflowArtifact,
    instance_id: &str,
    run_id: &str,
    items: usize,
) -> Result<StdDuration> {
    let start = Instant::now();
    for sequence in 0..items {
        let event = benchmark_activity_completed_event(
            &artifact.definition_id,
            artifact.definition_version,
            &artifact.artifact_hash,
            instance_id,
            run_id,
            sequence,
            &harness.task_queue,
        );
        harness
            .store
            .enqueue_workflow_resume(
                harness.partition_id,
                &harness.task_queue,
                None,
                &event,
                fabrik_store::WorkflowResumeKind::ActivityTerminal,
                &format!("step:{}", sequence + 1),
                Some("completed"),
            )
            .await?;
    }
    Ok(start.elapsed())
}

async fn drain_benchmark_run(
    harness: &BenchmarkHarness,
    items_per_run: usize,
) -> Result<(StdDuration, u64, u64)> {
    let start = Instant::now();
    let mut tasks_executed = 0u64;
    let mut restores = 0u64;

    loop {
        let Some(task_record) = harness
            .store
            .lease_next_workflow_task(
                harness.partition_id,
                &harness.worker_id,
                &harness.worker_build_id,
                chrono::Duration::seconds(30),
            )
            .await?
        else {
            break;
        };

        let mut runtime =
            ExecutorRuntime::new(64, 10_000, None, None, None, items_per_run, items_per_run);
        let task = WorkflowTask {
            task_id: task_record.task_id.to_string(),
            tenant_id: task_record.tenant_id.clone(),
            instance_id: task_record.instance_id.clone(),
            run_id: task_record.run_id.clone(),
            definition_id: task_record.definition_id.clone(),
            definition_version: task_record.definition_version.unwrap_or_default(),
            artifact_hash: task_record.artifact_hash.clone().unwrap_or_default(),
            partition_id: task_record.partition_id,
            task_queue: task_record.task_queue.clone(),
            preferred_build_id: task_record.preferred_build_id.clone().unwrap_or_default(),
            mailbox_consumed_seq: task_record.mailbox_consumed_seq,
            resume_consumed_seq: task_record.resume_consumed_seq,
            mailbox_backlog: task_record.mailbox_backlog,
            resume_backlog: task_record.resume_backlog,
            attempt_count: task_record.attempt_count,
            created_at_unix_ms: task_record.created_at.timestamp_millis(),
        };
        let outcome = process_workflow_task(
            &harness.store,
            &harness.broker,
            &harness.publisher,
            &mut runtime,
            &harness.debug_state,
            &harness.lease_state,
            &task,
            &harness.worker_id,
            &harness.worker_build_id,
        )
        .await?;
        if !matches!(outcome, TaskDrainOutcome::Drained) {
            anyhow::bail!("benchmark run blocked unexpectedly while draining task");
        }
        harness
            .store
            .complete_workflow_task(
                task_record.task_id,
                &harness.worker_id,
                &harness.worker_build_id,
                chrono::Utc::now(),
            )
            .await?;

        tasks_executed += 1;
        restores += runtime.restores_from_projection + runtime.restores_from_snapshot_replay;
    }

    Ok((start.elapsed(), tasks_executed, restores))
}

fn signal_benchmark_artifact() -> CompiledWorkflowArtifact {
    CompiledWorkflowArtifact::new(
        "bench-signal",
        1,
        "benchmark",
        ArtifactEntrypoint { module: "bench.ts".to_owned(), export: "signal".to_owned() },
        CompiledWorkflow {
            initial_state: "wait_signal".to_owned(),
            states: BTreeMap::from([(
                "wait_signal".to_owned(),
                CompiledStateNode::WaitForEvent {
                    event_type: "external.approved".to_owned(),
                    next: "wait_signal".to_owned(),
                    output_var: None,
                },
            )]),
        },
    )
}

fn activity_benchmark_artifact() -> CompiledWorkflowArtifact {
    CompiledWorkflowArtifact::new(
        "bench-activity",
        1,
        "benchmark",
        ArtifactEntrypoint { module: "bench.ts".to_owned(), export: "activity".to_owned() },
        CompiledWorkflow {
            initial_state: "step".to_owned(),
            states: BTreeMap::from([(
                "step".to_owned(),
                CompiledStateNode::Step {
                    handler: "core.echo".to_owned(),
                    input: Expression::Literal { value: Value::Null },
                    next: Some("step".to_owned()),
                    retry: None,
                    config: None,
                    output_var: None,
                    on_error: None,
                },
            )]),
        },
    )
}

fn benchmark_signal_event(
    definition_id: &str,
    definition_version: u32,
    artifact_hash: &str,
    instance_id: &str,
    run_id: &str,
    sequence: usize,
    task_queue: &str,
) -> EventEnvelope<WorkflowEvent> {
    let mut event = EventEnvelope::new(
        WorkflowEvent::SignalQueued {
            signal_id: format!("sig-{sequence}"),
            signal_type: "external.approved".to_owned(),
            payload: json!({ "sequence": sequence }),
        }
        .event_type(),
        WorkflowIdentity::new(
            "tenant-bench",
            definition_id,
            definition_version,
            artifact_hash,
            instance_id,
            run_id,
            "benchmark",
        ),
        WorkflowEvent::SignalQueued {
            signal_id: format!("sig-{sequence}"),
            signal_type: "external.approved".to_owned(),
            payload: json!({ "sequence": sequence }),
        },
    );
    event.metadata.insert("workflow_task_queue".to_owned(), task_queue.to_owned());
    event.occurred_at += chrono::Duration::milliseconds(sequence as i64);
    event
}

fn benchmark_activity_completed_event(
    definition_id: &str,
    definition_version: u32,
    artifact_hash: &str,
    instance_id: &str,
    run_id: &str,
    sequence: usize,
    task_queue: &str,
) -> EventEnvelope<WorkflowEvent> {
    let mut event = EventEnvelope::new(
        WorkflowEvent::ActivityTaskCompleted {
            activity_id: "step".to_owned(),
            attempt: 1,
            output: json!({ "sequence": sequence }),
            worker_id: "activity-bench-worker".to_owned(),
            worker_build_id: "activity-bench-build".to_owned(),
        }
        .event_type(),
        WorkflowIdentity::new(
            "tenant-bench",
            definition_id,
            definition_version,
            artifact_hash,
            instance_id,
            run_id,
            "benchmark",
        ),
        WorkflowEvent::ActivityTaskCompleted {
            activity_id: "step".to_owned(),
            attempt: 1,
            output: json!({ "sequence": sequence }),
            worker_id: "activity-bench-worker".to_owned(),
            worker_build_id: "activity-bench-build".to_owned(),
        },
    );
    event.metadata.insert("workflow_task_queue".to_owned(), task_queue.to_owned());
    event.occurred_at += chrono::Duration::milliseconds(sequence as i64);
    event
}

fn print_scenario_summary(summary: &ScenarioSummary) {
    let enqueue_samples = summary
        .measurements
        .iter()
        .map(|measurement| measurement.enqueue_duration)
        .collect::<Vec<_>>();
    let drain_samples = summary
        .measurements
        .iter()
        .map(|measurement| measurement.drain_duration)
        .collect::<Vec<_>>();
    let end_to_end_samples = summary
        .measurements
        .iter()
        .map(|measurement| measurement.end_to_end_duration)
        .collect::<Vec<_>>();
    let total_events = summary.items_per_run * summary.iterations;
    let total_tasks_executed =
        summary.measurements.iter().map(|measurement| measurement.tasks_executed).sum::<u64>();
    let total_restores =
        summary.measurements.iter().map(|measurement| measurement.restores).sum::<u64>();
    let avg_task_rows =
        summary.measurements.iter().map(|measurement| measurement.task_rows as f64).sum::<f64>()
            / summary.measurements.len() as f64;

    println!(
        "benchmark={} workload={} iterations={} items_per_run={} avg_task_rows={avg_task_rows:.2} total_tasks_executed={} total_restores={} enqueue_total_ms={:.2} enqueue_throughput_eps={:.2} enqueue_p50_ms={:.2} enqueue_p95_ms={:.2} drain_total_ms={:.2} drain_throughput_eps={:.2} drain_p50_ms={:.2} drain_p95_ms={:.2} e2e_total_ms={:.2} e2e_throughput_eps={:.2} e2e_p50_ms={:.2} e2e_p95_ms={:.2}",
        summary.name,
        summary.workload,
        summary.iterations,
        summary.items_per_run,
        total_tasks_executed,
        total_restores,
        total_ms(&enqueue_samples),
        throughput_events_per_second(total_events, &enqueue_samples),
        percentile_ms(&enqueue_samples, 50.0),
        percentile_ms(&enqueue_samples, 95.0),
        total_ms(&drain_samples),
        throughput_events_per_second(total_events, &drain_samples),
        percentile_ms(&drain_samples, 50.0),
        percentile_ms(&drain_samples, 95.0),
        total_ms(&end_to_end_samples),
        throughput_events_per_second(total_events, &end_to_end_samples),
        percentile_ms(&end_to_end_samples, 50.0),
        percentile_ms(&end_to_end_samples, 95.0),
    );
}

fn total_ms(samples: &[StdDuration]) -> f64 {
    samples.iter().map(|sample| sample.as_secs_f64() * 1000.0).sum()
}

fn throughput_events_per_second(total_events: usize, samples: &[StdDuration]) -> f64 {
    let total_seconds = samples.iter().map(StdDuration::as_secs_f64).sum::<f64>();
    if total_seconds == 0.0 { 0.0 } else { total_events as f64 / total_seconds }
}

fn percentile_ms(samples: &[StdDuration], percentile: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut values = samples.iter().map(|sample| sample.as_secs_f64() * 1000.0).collect::<Vec<_>>();
    values.sort_by(|left, right| left.total_cmp(right));
    let rank = ((percentile / 100.0) * (values.len().saturating_sub(1) as f64)).round() as usize;
    values[rank]
}

fn wait_for_docker_port(container_name: &str, container_port: &str) -> Result<u16> {
    let deadline = Instant::now() + StdDuration::from_secs(15);
    loop {
        let output = Command::new("docker")
            .args([
                "inspect",
                "--format",
                &format!(
                    "{{{{(index (index .NetworkSettings.Ports \"{container_port}\") 0).HostPort}}}}"
                ),
                container_name,
            ])
            .output()?;
        if output.status.success() {
            let host_port = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            if !host_port.is_empty() {
                return host_port.parse::<u16>().map_err(anyhow::Error::from);
            }
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for port {container_port} on {container_name}");
        }
        std::thread::sleep(StdDuration::from_millis(100));
    }
}

fn choose_free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn docker_logs(container_name: &str) -> Result<String> {
    let output = Command::new("docker").args(["logs", container_name]).output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    Ok(format!("{stdout}{stderr}"))
}

fn cleanup_container(container_name: &str) -> Result<()> {
    let output = Command::new("docker").args(["rm", "--force", container_name]).output()?;
    if output.status.success() {
        Ok(())
    } else {
        anyhow::bail!(
            "docker failed to remove container {container_name}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
    }
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
    poller_task: JoinHandle<()>,
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
    matching_endpoint: String,
    workflow_worker_build_id: String,
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
                    &matching_endpoint,
                    &workflow_worker_build_id,
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
    matching_endpoint: &str,
    workflow_worker_build_id: &str,
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
            matching_endpoint.to_owned(),
            workflow_worker_build_id.to_owned(),
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
    matching_endpoint: String,
    workflow_worker_build_id: String,
    initial_ownership: PartitionOwnershipRecord,
    renew_interval: std::time::Duration,
    lease_ttl: std::time::Duration,
) -> Result<()> {
    let partition_id = initial_ownership.partition_id;
    let lease_state = Arc::new(Mutex::new(LeaseState::from_record(&initial_ownership)));
    let runtime = Arc::new(tokio::sync::Mutex::new(ExecutorRuntime::new(
        runtime_config.cache_capacity,
        runtime_config.snapshot_interval_events,
        runtime_config.continue_as_new_event_threshold,
        runtime_config.continue_as_new_activity_attempt_threshold,
        runtime_config.continue_as_new_run_age_seconds,
        runtime_config.max_mailbox_items_per_turn,
        runtime_config.max_transitions_per_turn,
    )));
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
        partition_id,
        consumer,
        publisher.clone(),
        store.clone(),
        broker.clone(),
        runtime.clone(),
        debug_state.clone(),
        lease_state.clone(),
    ));
    let poller_task = tokio::spawn(run_workflow_poll_loop(
        matching_endpoint,
        partition_id,
        format!("{owner_id}:partition-{partition_id}"),
        workflow_worker_build_id,
        publisher.clone(),
        store.clone(),
        broker.clone(),
        runtime.clone(),
        debug_state.clone(),
        lease_state.clone(),
    ));

    workers.lock().map_err(|_| anyhow::anyhow!("partition worker map lock poisoned"))?.insert(
        partition_id,
        PartitionWorkerHandle { renewal_task, consumer_task, poller_task, lease_state },
    );
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
        handle.poller_task.abort();
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
                partition.workflow_turns_routed_via_matching = 0;
                partition.workflow_turns_processed_locally = 0;
                partition.sticky_build_hits = 0;
                partition.sticky_build_fallbacks = 0;
                partition.workflow_task_poll_hits = 0;
                partition.workflow_task_poll_empties = 0;
                partition.workflow_task_poll_failures = 0;
                partition.workflow_task_queue_latency = DurationStats::default();
                partition.workflow_task_execution_latency = DurationStats::default();
                debug.recompute_totals();
            }
        }
    }
    Ok(())
}

async fn run_executor_loop(
    partition_id: i32,
    consumer: rskafka::client::consumer::StreamConsumer,
    publisher: WorkflowPublisher,
    store: WorkflowStore,
    broker: BrokerConfig,
    runtime: Arc<tokio::sync::Mutex<ExecutorRuntime>>,
    debug_state: Arc<Mutex<ExecutorDebugState>>,
    lease_state: Arc<Mutex<LeaseState>>,
) {
    let mut stream = consumer;

    while let Some(message) = stream.next().await {
        match message {
            Ok((record, _high_watermark)) => match decode_workflow_event(&record) {
                Ok(event) => {
                    if matches!(
                        workflow_turn_routing(&event.payload),
                        WorkflowTurnRouting::MatchingPoller
                    ) {
                        continue;
                    }
                    if let Ok(mut state) = debug_state.lock() {
                        state.record_local_executor_event(partition_id);
                    }
                    let mut runtime = runtime.lock().await;
                    let mut persist_mode = PersistMode::Immediate;
                    let mut lookup_cache = WorkflowLookupCache::default();
                    let mut publish_buffer = WorkflowPublishBuffer::new(&publisher);
                    if let Err(error) = process_event(
                        &store,
                        &broker,
                        &mut publish_buffer,
                        &mut runtime,
                        &debug_state,
                        &lease_state,
                        event,
                        &mut persist_mode,
                        &mut lookup_cache,
                    )
                    .await
                    {
                        error!(error = %error, "failed to process workflow event");
                    } else if let Err(error) = publish_buffer.flush().await {
                        error!(error = %error, "failed to flush workflow event publishes");
                    }
                }
                Err(error) => warn!(error = %error, "skipping invalid workflow event"),
            },
            Err(error) => error!(error = %error, "executor consumer error"),
        }
    }
}

async fn run_workflow_poll_loop(
    matching_endpoint: String,
    partition_id: i32,
    worker_id: String,
    worker_build_id: String,
    publisher: WorkflowPublisher,
    store: WorkflowStore,
    broker: BrokerConfig,
    runtime: Arc<tokio::sync::Mutex<ExecutorRuntime>>,
    debug_state: Arc<Mutex<ExecutorDebugState>>,
    lease_state: Arc<Mutex<LeaseState>>,
) {
    loop {
        let mut client = match WorkflowWorkerApiClient::connect(matching_endpoint.clone()).await {
            Ok(client) => client,
            Err(error) => {
                error!(error = %error, partition_id, "failed to connect to matching-service workflow api");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }
        };

        loop {
            let poll_started_at = std::time::Instant::now();
            let response = client
                .poll_workflow_task(PollWorkflowTaskRequest {
                    partition_id,
                    worker_id: worker_id.clone(),
                    worker_build_id: worker_build_id.clone(),
                    poll_timeout_ms: 30_000,
                })
                .await;
            let Some(task) = (match response {
                Ok(response) => response.into_inner().task,
                Err(error) => {
                    if let Ok(mut state) = debug_state.lock() {
                        state.record_workflow_poll_failure(partition_id);
                    }
                    error!(error = %error, partition_id, "failed to poll workflow task");
                    break;
                }
            }) else {
                if let Ok(mut state) = debug_state.lock() {
                    state.record_workflow_poll_empty(partition_id);
                }
                continue;
            };
            if let Ok(mut state) = debug_state.lock() {
                state.record_workflow_poll_hit(partition_id, poll_started_at.elapsed());
                state.record_matching_routed_event(partition_id);
            }

            let task_id = task.task_id.clone();
            let task_uuid = Uuid::parse_str(&task_id).expect("workflow task id is a uuid");
            let execution_started_at = std::time::Instant::now();
            let queue_latency_ms = chrono::Utc::now()
                .timestamp_millis()
                .saturating_sub(task.created_at_unix_ms as i64)
                .max(0) as u64;
            let sticky_outcome =
                classify_sticky_dispatch(&task.preferred_build_id, &worker_build_id);
            let result = {
                let mut runtime = runtime.lock().await;
                process_workflow_task(
                    &store,
                    &broker,
                    &publisher,
                    &mut runtime,
                    &debug_state,
                    &lease_state,
                    &task,
                    &worker_id,
                    &worker_build_id,
                )
                .await
            };

            match result {
                Ok(outcome) => {
                    if let Ok(mut state) = debug_state.lock() {
                        state.record_workflow_task_execution(
                            partition_id,
                            queue_latency_ms,
                            execution_started_at.elapsed(),
                            sticky_outcome,
                        );
                    }
                    let _ = store
                        .update_run_workflow_sticky(
                            &task.tenant_id,
                            &task.instance_id,
                            &task.run_id,
                            &task.task_queue,
                            &worker_build_id,
                            &worker_id,
                            chrono::Utc::now(),
                        )
                        .await;
                    match outcome {
                        TaskDrainOutcome::Drained => {
                            if let Err(error) = client
                                .complete_workflow_task(CompleteWorkflowTaskRequest {
                                    task_id,
                                    worker_id: worker_id.clone(),
                                    worker_build_id: worker_build_id.clone(),
                                })
                                .await
                            {
                                error!(
                                    error = %error,
                                    partition_id,
                                    workflow_instance_id = %task.instance_id,
                                    "failed to ack workflow task completion"
                                );
                                break;
                            }
                        }
                        TaskDrainOutcome::Blocked => {
                            if let Err(error) = store
                                .park_workflow_task(
                                    task_uuid,
                                    &worker_id,
                                    &worker_build_id,
                                    chrono::Utc::now(),
                                )
                                .await
                            {
                                error!(
                                    error = %error,
                                    partition_id,
                                    workflow_instance_id = %task.instance_id,
                                    "failed to park blocked workflow task"
                                );
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    if let Ok(mut state) = debug_state.lock() {
                        state.record_workflow_task_execution(
                            partition_id,
                            queue_latency_ms,
                            execution_started_at.elapsed(),
                            sticky_outcome,
                        );
                    }
                    error!(
                        error = %error,
                        partition_id,
                        workflow_instance_id = %task.instance_id,
                        run_id = %task.run_id,
                        "failed to process leased workflow task"
                    );
                    if let Err(ack_error) = client
                        .fail_workflow_task(FailWorkflowTaskRequest {
                            task_id,
                            worker_id: worker_id.clone(),
                            worker_build_id: worker_build_id.clone(),
                            error: error.to_string(),
                        })
                        .await
                    {
                        error!(error = %ack_error, partition_id, "failed to nack workflow task");
                        break;
                    }
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

async fn process_workflow_task(
    store: &WorkflowStore,
    broker: &BrokerConfig,
    publisher: &WorkflowPublisher,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    task: &WorkflowTask,
    worker_id: &str,
    worker_build_id: &str,
) -> Result<TaskDrainOutcome> {
    let mut transitions = 0usize;
    let mut mailbox_items = 0usize;
    let mut persist_mode = PersistMode::Deferred { dirty: false };
    let mut lookup_cache = WorkflowLookupCache::default();
    let mut consumed_signals = Vec::new();
    let mut publish_buffer = WorkflowPublishBuffer::new(publisher);

    loop {
        if transitions >= runtime.max_transitions_per_turn
            || mailbox_items >= runtime.max_mailbox_items_per_turn
        {
            return finalize_task_drain(
                store,
                runtime,
                debug_state,
                lease_state,
                task,
                &persist_mode,
                &mut publish_buffer,
                TaskDrainOutcome::Drained,
            )
            .await;
        }

        let resume_batch_limit = runtime.max_transitions_per_turn.saturating_sub(transitions);
        if resume_batch_limit > 0 {
            let resume_batch = store
                .list_next_workflow_resume_items(
                    &task.tenant_id,
                    &task.instance_id,
                    &task.run_id,
                    resume_batch_limit.min(256),
                )
            .await?;
            if !resume_batch.is_empty() {
                let mut max_consumed_resume_seq = None;
                for resume in resume_batch {
                    if transitions >= runtime.max_transitions_per_turn {
                        break;
                    }
                    let mut event = resume.source_event.clone();
                    annotate_workflow_task_event(&mut event, worker_id, worker_build_id);
                    if let Err(error) = process_event(
                        store,
                        broker,
                        &mut publish_buffer,
                        runtime,
                        debug_state,
                        lease_state,
                        event,
                        &mut persist_mode,
                        &mut lookup_cache,
                    )
                    .await
                    {
                        flush_task_side_effects(
                            store,
                            runtime,
                            debug_state,
                            lease_state,
                            task,
                            &persist_mode,
                            &mut publish_buffer,
                        )
                        .await?;
                        return Err(error);
                    }
                    max_consumed_resume_seq = Some(resume.resume_seq);
                    transitions += 1;
                }
                if let Some(max_resume_seq) = max_consumed_resume_seq {
                    store
                        .mark_workflow_resume_items_consumed_through(
                            &task.tenant_id,
                            &task.instance_id,
                            &task.run_id,
                            max_resume_seq,
                            chrono::Utc::now(),
                        )
                        .await?;
                }
                continue;
            }
        }

        if mailbox_items >= runtime.max_mailbox_items_per_turn
            || transitions >= runtime.max_transitions_per_turn
        {
            return finalize_task_drain(
                store,
                runtime,
                debug_state,
                lease_state,
                task,
                &persist_mode,
                &mut publish_buffer,
                TaskDrainOutcome::Drained,
            )
            .await;
        }

        let mailbox_batch_limit = runtime.max_mailbox_items_per_turn.saturating_sub(mailbox_items);
        let mailbox_batch = store
            .list_next_workflow_mailbox_items(
                &task.tenant_id,
                &task.instance_id,
                &task.run_id,
                mailbox_batch_limit.min(256),
            )
            .await?;
        if mailbox_batch.is_empty() {
            return finalize_task_drain(
                store,
                runtime,
                debug_state,
                lease_state,
                task,
                &persist_mode,
                &mut publish_buffer,
                TaskDrainOutcome::Drained,
            )
            .await;
        }

        let mut max_consumed_mailbox_seq = None;
        for item in mailbox_batch {
            if mailbox_items >= runtime.max_mailbox_items_per_turn
                || transitions >= runtime.max_transitions_per_turn
            {
                break;
            }

            let dispatch_outcome = match dispatch_mailbox_item(
                store,
                broker,
                &mut publish_buffer,
                runtime,
                debug_state,
                lease_state,
                &item,
                worker_id,
                worker_build_id,
                &mut persist_mode,
                &mut lookup_cache,
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    flush_task_side_effects(
                        store,
                        runtime,
                        debug_state,
                        lease_state,
                        task,
                        &persist_mode,
                        &mut publish_buffer,
                    )
                    .await?;
                    return Err(error);
                }
            };

            match dispatch_outcome {
                MailboxDispatchOutcome::Processed { consumed_signal } => {
                    if let Some(consumed_signal) = consumed_signal {
                        consumed_signals.push(consumed_signal);
                    }
                    max_consumed_mailbox_seq = Some(item.accepted_seq);
                    mailbox_items += 1;
                    transitions += 1;
                }
                MailboxDispatchOutcome::ConsumedNoop => {
                    max_consumed_mailbox_seq = Some(item.accepted_seq);
                    mailbox_items += 1;
                }
                MailboxDispatchOutcome::Blocked => {
                    if !consumed_signals.is_empty() {
                        store
                            .mark_signals_consumed(
                                &task.tenant_id,
                                &task.instance_id,
                                &task.run_id,
                                &consumed_signals
                                    .iter()
                                    .map(|signal| ConsumedSignalRecord {
                                        signal_id: signal.signal_id.clone(),
                                        consumed_event_id: signal.consumed_event_id,
                                        consumed_at: signal.consumed_at,
                                    })
                                    .collect::<Vec<_>>(),
                            )
                            .await?;
                        consumed_signals.clear();
                    }
                    if let Some(max_mailbox_seq) = max_consumed_mailbox_seq {
                        store
                            .mark_workflow_mailbox_items_consumed_through(
                                &task.tenant_id,
                                &task.instance_id,
                                &task.run_id,
                                max_mailbox_seq,
                                chrono::Utc::now(),
                            )
                            .await?;
                    }
                    return finalize_task_drain(
                        store,
                        runtime,
                        debug_state,
                        lease_state,
                        task,
                        &persist_mode,
                        &mut publish_buffer,
                        TaskDrainOutcome::Blocked,
                    )
                    .await;
                }
            }
        }

        if !consumed_signals.is_empty() {
            store
                .mark_signals_consumed(
                    &task.tenant_id,
                    &task.instance_id,
                    &task.run_id,
                    &consumed_signals
                        .iter()
                        .map(|signal| ConsumedSignalRecord {
                            signal_id: signal.signal_id.clone(),
                            consumed_event_id: signal.consumed_event_id,
                            consumed_at: signal.consumed_at,
                        })
                        .collect::<Vec<_>>(),
                )
                .await?;
            consumed_signals.clear();
        }
        if let Some(max_mailbox_seq) = max_consumed_mailbox_seq {
            store
                .mark_workflow_mailbox_items_consumed_through(
                    &task.tenant_id,
                    &task.instance_id,
                    &task.run_id,
                    max_mailbox_seq,
                    chrono::Utc::now(),
                )
                .await?;
        }
    }
}

#[derive(Debug)]
enum MailboxDispatchOutcome {
    Processed { consumed_signal: Option<ConsumedSignal> },
    ConsumedNoop,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskDrainOutcome {
    Drained,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistMode {
    Immediate,
    Deferred { dirty: bool },
}

async fn finalize_task_drain(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    task: &WorkflowTask,
    persist_mode: &PersistMode,
    publish_buffer: &mut WorkflowPublishBuffer<'_>,
    outcome: TaskDrainOutcome,
) -> Result<TaskDrainOutcome> {
    flush_task_side_effects(
        store,
        runtime,
        debug_state,
        lease_state,
        task,
        persist_mode,
        publish_buffer,
    )
    .await?;
    Ok(outcome)
}

async fn flush_task_side_effects(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    task: &WorkflowTask,
    persist_mode: &PersistMode,
    publish_buffer: &mut WorkflowPublishBuffer<'_>,
) -> Result<()> {
    if let Err(error) = publish_buffer.flush().await {
        if let PersistMode::Deferred { dirty: true } = persist_mode {
            flush_deferred_state(store, runtime, debug_state, lease_state, task).await?;
        }
        return Err(error);
    }
    if let PersistMode::Deferred { dirty: true } = persist_mode {
        flush_deferred_state(store, runtime, debug_state, lease_state, task).await?;
    }
    Ok(())
}

async fn flush_deferred_state(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    task: &WorkflowTask,
) -> Result<()> {
    let Some(mut record) = runtime.peek(&task.tenant_id, &task.instance_id) else {
        return Ok(());
    };
    let state = record.state.clone();
    let lease_snapshot =
        ensure_active_partition_ownership(store, runtime, debug_state, lease_state).await?;
    persist_state_immediate(
        store,
        runtime,
        debug_state,
        lease_state,
        lease_snapshot.partition_id,
        &mut record,
        &state,
    )
    .await
}

fn annotate_workflow_task_event(
    event: &mut EventEnvelope<WorkflowEvent>,
    worker_id: &str,
    worker_build_id: &str,
) {
    event.metadata.insert("workflow_build_id".to_owned(), worker_build_id.to_owned());
    event.metadata.insert("workflow_poller_id".to_owned(), worker_id.to_owned());
}

fn signal_dispatch_event_id(source_event_id: Uuid) -> Uuid {
    Uuid::new_v5(
        &SIGNAL_DISPATCH_EVENT_NAMESPACE,
        format!("signal-dispatch:{source_event_id}").as_bytes(),
    )
}

async fn current_workflow_state(
    store: &WorkflowStore,
    runtime: &ExecutorRuntime,
    tenant_id: &str,
    instance_id: &str,
) -> Result<Option<WorkflowInstanceState>> {
    if let Some(record) = runtime.peek(tenant_id, instance_id) {
        return Ok(Some(record.state));
    }
    store.get_instance(tenant_id, instance_id).await
}

async fn dispatch_mailbox_item(
    store: &WorkflowStore,
    broker: &BrokerConfig,
    publisher: &mut WorkflowPublishBuffer<'_>,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    item: &fabrik_store::WorkflowMailboxRecord,
    worker_id: &str,
    worker_build_id: &str,
    persist_mode: &mut PersistMode,
    lookup_cache: &mut WorkflowLookupCache,
) -> Result<MailboxDispatchOutcome> {
    match item.kind {
        fabrik_store::WorkflowMailboxKind::Trigger
        | fabrik_store::WorkflowMailboxKind::CancelRequest => {
            let mut event = item.source_event.clone();
            annotate_workflow_task_event(&mut event, worker_id, worker_build_id);
            process_event(
                store,
                broker,
                publisher,
                runtime,
                debug_state,
                lease_state,
                event,
                persist_mode,
                lookup_cache,
            )
            .await?;
            Ok(MailboxDispatchOutcome::Processed { consumed_signal: None })
        }
        fabrik_store::WorkflowMailboxKind::Signal => {
            let Some(message_id) = item.message_id.as_deref() else {
                return Ok(MailboxDispatchOutcome::ConsumedNoop);
            };
            let Some(instance) =
                current_workflow_state(store, runtime, &item.tenant_id, &item.instance_id).await?
            else {
                return Ok(MailboxDispatchOutcome::ConsumedNoop);
            };
            let Some(wait_state) = instance.current_state.as_deref() else {
                return Ok(MailboxDispatchOutcome::Blocked);
            };
            let expected_signal_type = if let Some(artifact) =
                load_pinned_artifact(store, &item.source_event, &instance, lookup_cache).await?
            {
                artifact.expected_signal_type(wait_state)?.map(str::to_owned)
            } else {
                let definition =
                    load_pinned_definition(store, &item.source_event, &instance, lookup_cache)
                        .await?;
                definition.expected_signal_type(wait_state)?.map(str::to_owned)
            };
            if expected_signal_type.as_deref() != item.message_name.as_deref() {
                return Ok(MailboxDispatchOutcome::Blocked);
            }

            let payload = item.payload.clone().unwrap_or(Value::Null);
            let mut event = EventEnvelope::new(
                WorkflowEvent::SignalReceived {
                    signal_id: message_id.to_owned(),
                    signal_type: item.message_name.clone().unwrap_or_default(),
                    payload: payload.clone(),
                }
                .event_type(),
                WorkflowIdentity::new(
                    item.tenant_id.clone(),
                    item.source_event.definition_id.clone(),
                    item.source_event.definition_version,
                    item.source_event.artifact_hash.clone(),
                    item.instance_id.clone(),
                    item.run_id.clone(),
                    "executor-service",
                ),
                WorkflowEvent::SignalReceived {
                    signal_id: message_id.to_owned(),
                    signal_type: item.message_name.clone().unwrap_or_default(),
                    payload,
                },
            );
            event.event_id = signal_dispatch_event_id(item.source_event.event_id);
            event.occurred_at = chrono::Utc::now();
            event.causation_id = Some(item.source_event.event_id);
            event.correlation_id =
                item.source_event.correlation_id.or(Some(item.source_event.event_id));
            event.dedupe_key = Some(format!("signal:{message_id}"));
            annotate_workflow_task_event(&mut event, worker_id, worker_build_id);
            publisher.publish(&event);
            let consumed_event_id = event.event_id;
            let consumed_at = event.occurred_at;
            process_event(
                store,
                broker,
                publisher,
                runtime,
                debug_state,
                lease_state,
                event,
                persist_mode,
                lookup_cache,
            )
            .await?;
            Ok(MailboxDispatchOutcome::Processed {
                consumed_signal: Some(ConsumedSignal {
                    signal_id: message_id.to_owned(),
                    consumed_event_id,
                    consumed_at,
                }),
            })
        }
        fabrik_store::WorkflowMailboxKind::Update => {
            let Some(message_id) = item.message_id.as_deref() else {
                return Ok(MailboxDispatchOutcome::ConsumedNoop);
            };
            let Some(instance) =
                current_workflow_state(store, runtime, &item.tenant_id, &item.instance_id).await?
            else {
                return Ok(MailboxDispatchOutcome::ConsumedNoop);
            };
            if instance.status.is_terminal() {
                let error =
                    format!("workflow run {} is already {}", item.run_id, instance.status.as_str());
                if let Some(update) = store
                    .get_update(&item.tenant_id, &item.instance_id, &item.run_id, message_id)
                    .await?
                {
                    if store
                        .complete_update(
                            &update.tenant_id,
                            &update.instance_id,
                            &update.run_id,
                            &update.update_id,
                            None,
                            Some(&error),
                            item.source_event.event_id,
                            chrono::Utc::now(),
                        )
                        .await?
                    {
                        let mut event = EventEnvelope::new(
                            WorkflowEvent::WorkflowUpdateRejected {
                                update_id: update.update_id.clone(),
                                update_name: update.update_name.clone(),
                                error: error.clone(),
                            }
                            .event_type(),
                            source_identity(&item.source_event, "executor-service"),
                            WorkflowEvent::WorkflowUpdateRejected {
                                update_id: update.update_id.clone(),
                                update_name: update.update_name.clone(),
                                error,
                            },
                        );
                        event.causation_id = Some(item.source_event.event_id);
                        event.correlation_id =
                            item.source_event.correlation_id.or(Some(item.source_event.event_id));
                        event.dedupe_key = Some(format!("update-rejected:{}", update.update_id));
                        annotate_workflow_task_event(&mut event, worker_id, worker_build_id);
                        publisher.publish(&event);
                        process_event(
                            store,
                            broker,
                            publisher,
                            runtime,
                            debug_state,
                            lease_state,
                            event,
                            persist_mode,
                            lookup_cache,
                        )
                        .await?;
                        return Ok(MailboxDispatchOutcome::Processed { consumed_signal: None });
                    }
                }
                return Ok(MailboxDispatchOutcome::ConsumedNoop);
            }
            if instance
                .artifact_execution
                .as_ref()
                .and_then(|execution| execution.active_update.as_ref())
                .is_some()
            {
                return Ok(MailboxDispatchOutcome::Blocked);
            }
            let Some(artifact) =
                load_pinned_artifact(store, &item.source_event, &instance, lookup_cache).await?
            else {
                return Ok(MailboxDispatchOutcome::Blocked);
            };
            let Some(update) = store
                .get_update(&item.tenant_id, &item.instance_id, &item.run_id, message_id)
                .await?
            else {
                return Ok(MailboxDispatchOutcome::ConsumedNoop);
            };
            if !artifact.has_update(&update.update_name) {
                if !store
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
                    .await?
                {
                    return Ok(MailboxDispatchOutcome::ConsumedNoop);
                }
                let mut event = EventEnvelope::new(
                    WorkflowEvent::WorkflowUpdateRejected {
                        update_id: update.update_id.clone(),
                        update_name: update.update_name.clone(),
                        error: format!("unknown update handler {}", update.update_name),
                    }
                    .event_type(),
                    source_identity(&item.source_event, "executor-service"),
                    WorkflowEvent::WorkflowUpdateRejected {
                        update_id: update.update_id.clone(),
                        update_name: update.update_name.clone(),
                        error: format!("unknown update handler {}", update.update_name),
                    },
                );
                event.causation_id = Some(update.source_event_id);
                event.correlation_id =
                    item.source_event.correlation_id.or(Some(update.source_event_id));
                event.dedupe_key = Some(format!("update-rejected:{}", update.update_id));
                annotate_workflow_task_event(&mut event, worker_id, worker_build_id);
                publisher.publish(&event);
                process_event(
                    store,
                    broker,
                    publisher,
                    runtime,
                    debug_state,
                    lease_state,
                    event,
                    persist_mode,
                    lookup_cache,
                )
                .await?;
                return Ok(MailboxDispatchOutcome::Processed { consumed_signal: None });
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
                return Ok(MailboxDispatchOutcome::ConsumedNoop);
            }
            let mut event = EventEnvelope::new(
                WorkflowEvent::WorkflowUpdateAccepted {
                    update_id: update.update_id.clone(),
                    update_name: update.update_name.clone(),
                    payload: update.payload.clone(),
                }
                .event_type(),
                source_identity(&item.source_event, "executor-service"),
                WorkflowEvent::WorkflowUpdateAccepted {
                    update_id: update.update_id.clone(),
                    update_name: update.update_name.clone(),
                    payload: update.payload.clone(),
                },
            );
            event.event_id = accepted_event_id;
            event.occurred_at = chrono::Utc::now();
            event.causation_id = Some(update.source_event_id);
            event.correlation_id =
                item.source_event.correlation_id.or(Some(update.source_event_id));
            event.dedupe_key = Some(format!("update-accepted:{}", update.update_id));
            annotate_workflow_task_event(&mut event, worker_id, worker_build_id);
            publisher.publish(&event);
            process_event(
                store,
                broker,
                publisher,
                runtime,
                debug_state,
                lease_state,
                event,
                persist_mode,
                lookup_cache,
            )
            .await?;
            Ok(MailboxDispatchOutcome::Processed { consumed_signal: None })
        }
    }
}

async fn process_event(
    store: &WorkflowStore,
    broker: &BrokerConfig,
    publisher: &mut WorkflowPublishBuffer<'_>,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    event: EventEnvelope<WorkflowEvent>,
    persist_mode: &mut PersistMode,
    lookup_cache: &mut WorkflowLookupCache,
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
    persist_state_with_mode(
        store,
        runtime,
        debug_state,
        lease_state,
        lease_snapshot.partition_id,
        &mut record,
        &state,
        persist_mode,
    )
    .await?;

    match &event.payload {
        WorkflowEvent::WorkflowTriggered { .. } => {
            if let Some(artifact) =
                load_pinned_artifact(store, &event, &state, lookup_cache).await?
            {
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
                persist_state_with_mode(
                    store,
                    runtime,
                    debug_state,
                    lease_state,
                    lease_snapshot.partition_id,
                    &mut record,
                    &state,
                    persist_mode,
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

            if let Some(artifact) =
                load_pinned_artifact(store, &event, &state, lookup_cache).await?
            {
                if let Some((target_kind, target_id, attempt)) = parse_retry_timer_id(timer_id) {
                    let mut execution_state = state.artifact_execution.clone().unwrap_or_default();
                    execution_state.turn_context = Some(ExecutionTurnContext {
                        event_id: event.event_id,
                        occurred_at: event.occurred_at,
                    });
                    match target_kind {
                        RetryTargetKind::Step => {
                            let input = artifact.step_details(&target_id, &execution_state)?.2;
                            let reschedule_state = artifact
                                .reschedule_state_for_activity(&target_id, &execution_state)
                                .unwrap_or_else(|| target_id.clone());
                            state.context = Some(input);
                            state.current_state = Some(reschedule_state.clone());
                            persist_state_with_mode(
                                store,
                                runtime,
                                debug_state,
                                lease_state,
                                lease_snapshot.partition_id,
                                &mut record,
                                &state,
                                persist_mode,
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
                                    state: Some(reschedule_state.clone()),
                                    schedule_to_start_timeout_ms: None,
                                    start_to_close_timeout_ms: None,
                                    heartbeat_timeout_ms: None,
                                },
                                state: Some(reschedule_state.clone()),
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
                                    final_state: reschedule_state,
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
                persist_state_with_mode(
                    store,
                    runtime,
                    debug_state,
                    lease_state,
                    lease_snapshot.partition_id,
                    &mut record,
                    &state,
                    persist_mode,
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
                let definition =
                    load_pinned_definition(store, &event, &state, lookup_cache).await?;
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

            let definition = load_pinned_definition(store, &event, &state, lookup_cache).await?;
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
        WorkflowEvent::BulkActivityBatchCompleted {
            batch_id,
            total_items,
            succeeded_items,
            failed_items,
            cancelled_items,
            chunk_count,
        } => {
            let Some(artifact) = load_pinned_artifact(store, &event, &state, lookup_cache).await?
            else {
                return Ok(());
            };
            let wait_state = match state.current_state.clone() {
                Some(wait_state) => wait_state,
                None => {
                    warn!(
                        workflow_instance_id = %event.instance_id,
                        batch_id = %batch_id,
                        "bulk batch completed without current_state"
                    );
                    return Ok(());
                }
            };
            let summary = serde_json::json!({
                "batchId": batch_id,
                "status": "completed",
                "totalItems": total_items,
                "succeededItems": succeeded_items,
                "failedItems": failed_items,
                "cancelledItems": cancelled_items,
                "chunkCount": chunk_count,
                "resultHandle": { "batchId": batch_id },
            });
            let plan = artifact.execute_after_bulk_completion_with_turn(
                &wait_state,
                batch_id,
                &summary,
                state.artifact_execution.clone().unwrap_or_default(),
                ExecutionTurnContext {
                    event_id: event.event_id,
                    occurred_at: event.occurred_at,
                },
            )?;
            apply_compiled_plan(&mut state, &plan);
            persist_state_with_mode(
                store,
                runtime,
                debug_state,
                lease_state,
                lease_snapshot.partition_id,
                &mut record,
                &state,
                persist_mode,
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
        WorkflowEvent::BulkActivityBatchFailed {
            batch_id,
            total_items,
            succeeded_items,
            failed_items,
            cancelled_items,
            chunk_count,
            message,
        }
        | WorkflowEvent::BulkActivityBatchCancelled {
            batch_id,
            total_items,
            succeeded_items,
            failed_items,
            cancelled_items,
            chunk_count,
            message,
        } => {
            let Some(artifact) = load_pinned_artifact(store, &event, &state, lookup_cache).await?
            else {
                return Ok(());
            };
            let wait_state = match state.current_state.clone() {
                Some(wait_state) => wait_state,
                None => {
                    warn!(
                        workflow_instance_id = %event.instance_id,
                        batch_id = %batch_id,
                        "bulk batch failed without current_state"
                    );
                    return Ok(());
                }
            };
            let error = serde_json::json!({
                "batchId": batch_id,
                "status": if matches!(&event.payload, WorkflowEvent::BulkActivityBatchFailed { .. }) {
                    "failed"
                } else {
                    "cancelled"
                },
                "message": message,
                "totalItems": total_items,
                "succeededItems": succeeded_items,
                "failedItems": failed_items,
                "cancelledItems": cancelled_items,
                "chunkCount": chunk_count,
            });
            let plan = artifact.execute_after_bulk_failure_with_turn(
                &wait_state,
                batch_id,
                &error,
                state.artifact_execution.clone().unwrap_or_default(),
                ExecutionTurnContext {
                    event_id: event.event_id,
                    occurred_at: event.occurred_at,
                },
            )?;
            apply_compiled_plan(&mut state, &plan);
            persist_state_with_mode(
                store,
                runtime,
                debug_state,
                lease_state,
                lease_snapshot.partition_id,
                &mut record,
                &state,
                persist_mode,
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
        WorkflowEvent::ActivityTaskCompleted {
            activity_id: step_id, attempt: _, output, ..
        } => {
            if let Some(artifact) =
                load_pinned_artifact(store, &event, &state, lookup_cache).await?
            {
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
                persist_state_with_mode(
                    store,
                    runtime,
                    debug_state,
                    lease_state,
                    lease_snapshot.partition_id,
                    &mut record,
                    &state,
                    persist_mode,
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

            let definition = load_pinned_definition(store, &event, &state, lookup_cache).await?;
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
            if let Some(artifact) =
                load_pinned_artifact(store, &event, &state, lookup_cache).await?
            {
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

                match artifact.step_retry(
                    &step_state,
                    &state.artifact_execution.clone().unwrap_or_default(),
                )? {
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
                        persist_state_with_mode(
                            store,
                            runtime,
                            debug_state,
                            lease_state,
                            lease_snapshot.partition_id,
                            &mut record,
                            &state,
                            persist_mode,
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

            let definition = load_pinned_definition(store, &event, &state, lookup_cache).await?;
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
            if let Some(artifact) =
                load_pinned_artifact(store, &event, &state, lookup_cache).await?
            {
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

                match artifact.step_retry(
                    &step_state,
                    &state.artifact_execution.clone().unwrap_or_default(),
                )? {
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
                        persist_state_with_mode(
                            store,
                            runtime,
                            debug_state,
                            lease_state,
                            lease_snapshot.partition_id,
                            &mut record,
                            &state,
                            persist_mode,
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

            let definition = load_pinned_definition(store, &event, &state, lookup_cache).await?;
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
        WorkflowEvent::SignalReceived { signal_id: _, signal_type, payload } => {
            if let Some(artifact) =
                load_pinned_artifact(store, &event, &state, lookup_cache).await?
            {
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
                persist_state_with_mode(
                    store,
                    runtime,
                    debug_state,
                    lease_state,
                    lease_snapshot.partition_id,
                    &mut record,
                    &state,
                    persist_mode,
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

            let definition = load_pinned_definition(store, &event, &state, lookup_cache).await?;
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
            let Some(artifact) = load_pinned_artifact(store, &event, &state, lookup_cache).await?
            else {
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
            persist_state_with_mode(
                store,
                runtime,
                debug_state,
                lease_state,
                lease_snapshot.partition_id,
                &mut record,
                &state,
                persist_mode,
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
            let Some(artifact) = load_pinned_artifact(store, &event, &state, lookup_cache).await?
            else {
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
            persist_state_with_mode(
                store,
                runtime,
                debug_state,
                lease_state,
                lease_snapshot.partition_id,
                &mut record,
                &state,
                persist_mode,
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
            let Some(artifact) = load_pinned_artifact(store, &event, &state, lookup_cache).await?
            else {
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
            persist_state_with_mode(
                store,
                runtime,
                debug_state,
                lease_state,
                lease_snapshot.partition_id,
                &mut record,
                &state,
                persist_mode,
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
            task_queue,
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
            let workflow_task_queue =
                task_queue.clone().unwrap_or_else(|| state.workflow_task_queue.clone());
            store
                .put_run_start(
                    &event.tenant_id,
                    child_workflow_id,
                    &child_run_id,
                    child_definition_id,
                    Some(definition_version),
                    Some(&artifact_hash),
                    &workflow_task_queue,
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
            child_trigger.metadata.insert("workflow_task_queue".to_owned(), workflow_task_queue);
            publisher.publish(&child_trigger);

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
            publisher.publish(&parent_started);
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
        WorkflowEvent::SignalQueued { .. } => {}
        WorkflowEvent::WorkflowCancellationRequested { reason } => {
            ensure_active_partition_ownership(store, runtime, debug_state, lease_state).await?;
            let mut cancelled = EventEnvelope::new(
                WorkflowEvent::WorkflowCancelled { reason: reason.clone() }.event_type(),
                source_identity(&event, "executor-service"),
                WorkflowEvent::WorkflowCancelled { reason: reason.clone() },
            );
            cancelled.causation_id = Some(event.event_id);
            cancelled.correlation_id = event.correlation_id.or(Some(event.event_id));
            publisher.publish(&cancelled);
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

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
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
    max_mailbox_items_per_turn: usize,
    max_transitions_per_turn: usize,
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
    workflow_turns_routed_via_matching: u64,
    workflow_turns_processed_locally: u64,
    sticky_build_hits: u64,
    sticky_build_fallbacks: u64,
    workflow_task_poll_hits: u64,
    workflow_task_poll_empties: u64,
    workflow_task_poll_failures: u64,
    workflow_task_queue_latency: DurationStats,
    workflow_task_execution_latency: DurationStats,
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
    workflow_turns_routed_via_matching: u64,
    workflow_turns_processed_locally: u64,
    sticky_build_hits: u64,
    sticky_build_fallbacks: u64,
    workflow_task_poll_hits: u64,
    workflow_task_poll_empties: u64,
    workflow_task_poll_failures: u64,
    workflow_task_queue_latency: DurationStats,
    workflow_task_execution_latency: DurationStats,
    hot_instances: Vec<HotInstanceDebug>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct HybridRoutingDebugSummary {
    total_workflow_turns: u64,
    matching_routed_turns: u64,
    local_executor_turns: u64,
    matching_routed_ratio: f64,
    local_executor_ratio: f64,
    sticky_build_hits: u64,
    sticky_build_fallbacks: u64,
    sticky_hit_rate: f64,
    sticky_fallback_rate: f64,
    workflow_task_poll_hits: u64,
    workflow_task_poll_empties: u64,
    workflow_task_poll_failures: u64,
    poll_hit_rate: f64,
    poll_empty_rate: f64,
    poll_failure_rate: f64,
    avg_workflow_task_queue_latency_ms: f64,
    max_workflow_task_queue_latency_ms: u64,
    avg_workflow_task_execution_latency_ms: f64,
    max_workflow_task_execution_latency_ms: u64,
    restores_after_handoff: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Default)]
struct DurationStats {
    sample_count: u64,
    total_ms: u64,
    max_ms: u64,
}

impl DurationStats {
    fn record_ms(&mut self, value_ms: u64) {
        self.sample_count += 1;
        self.total_ms = self.total_ms.saturating_add(value_ms);
        self.max_ms = self.max_ms.max(value_ms);
    }

    fn combine(values: impl Iterator<Item = Self>) -> Self {
        values.fold(Self::default(), |mut combined, value| {
            combined.sample_count = combined.sample_count.saturating_add(value.sample_count);
            combined.total_ms = combined.total_ms.saturating_add(value.total_ms);
            combined.max_ms = combined.max_ms.max(value.max_ms);
            combined
        })
    }

    fn average_ms(&self) -> f64 {
        if self.sample_count == 0 { 0.0 } else { self.total_ms as f64 / self.sample_count as f64 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StickyDispatchOutcome {
    None,
    Hit,
    Fallback,
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
        max_mailbox_items_per_turn: usize,
        max_transitions_per_turn: usize,
    ) -> Self {
        Self {
            cache_capacity,
            snapshot_interval_events,
            continue_as_new_event_threshold,
            continue_as_new_activity_attempt_threshold,
            continue_as_new_run_age_seconds,
            max_mailbox_items_per_turn,
            max_transitions_per_turn,
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

    fn peek(&self, tenant_id: &str, instance_id: &str) -> Option<HotStateRecord> {
        let key = HotStateKey::new(tenant_id, instance_id);
        self.cache.get(&key).map(|entry| entry.record.clone())
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
            workflow_turns_routed_via_matching: 0,
            workflow_turns_processed_locally: 0,
            sticky_build_hits: 0,
            sticky_build_fallbacks: 0,
            workflow_task_poll_hits: 0,
            workflow_task_poll_empties: 0,
            workflow_task_poll_failures: 0,
            workflow_task_queue_latency: DurationStats::default(),
            workflow_task_execution_latency: DurationStats::default(),
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
                    workflow_turns_routed_via_matching: 0,
                    workflow_turns_processed_locally: 0,
                    sticky_build_hits: 0,
                    sticky_build_fallbacks: 0,
                    workflow_task_poll_hits: 0,
                    workflow_task_poll_empties: 0,
                    workflow_task_poll_failures: 0,
                    workflow_task_queue_latency: DurationStats::default(),
                    workflow_task_execution_latency: DurationStats::default(),
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

    fn record_matching_routed_event(&mut self, partition_id: i32) {
        self.partition_mut(partition_id).workflow_turns_routed_via_matching += 1;
        self.recompute_totals();
    }

    fn record_local_executor_event(&mut self, partition_id: i32) {
        self.partition_mut(partition_id).workflow_turns_processed_locally += 1;
        self.recompute_totals();
    }

    fn record_workflow_poll_hit(&mut self, partition_id: i32, _latency: std::time::Duration) {
        self.partition_mut(partition_id).workflow_task_poll_hits += 1;
        self.recompute_totals();
    }

    fn record_workflow_poll_empty(&mut self, partition_id: i32) {
        self.partition_mut(partition_id).workflow_task_poll_empties += 1;
        self.recompute_totals();
    }

    fn record_workflow_poll_failure(&mut self, partition_id: i32) {
        self.partition_mut(partition_id).workflow_task_poll_failures += 1;
        self.recompute_totals();
    }

    fn record_workflow_task_execution(
        &mut self,
        partition_id: i32,
        queue_latency_ms: u64,
        execution_latency: std::time::Duration,
        sticky_outcome: StickyDispatchOutcome,
    ) {
        let partition = self.partition_mut(partition_id);
        partition.workflow_task_queue_latency.record_ms(queue_latency_ms);
        partition
            .workflow_task_execution_latency
            .record_ms(execution_latency.as_millis().min(u128::from(u64::MAX)) as u64);
        match sticky_outcome {
            StickyDispatchOutcome::Hit => partition.sticky_build_hits += 1,
            StickyDispatchOutcome::Fallback => partition.sticky_build_fallbacks += 1,
            StickyDispatchOutcome::None => {}
        }
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
            workflow_turns_routed_via_matching: 0,
            workflow_turns_processed_locally: 0,
            sticky_build_hits: 0,
            sticky_build_fallbacks: 0,
            workflow_task_poll_hits: 0,
            workflow_task_poll_empties: 0,
            workflow_task_poll_failures: 0,
            workflow_task_queue_latency: DurationStats::default(),
            workflow_task_execution_latency: DurationStats::default(),
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
        self.workflow_turns_routed_via_matching =
            self.partitions.iter().map(|entry| entry.workflow_turns_routed_via_matching).sum();
        self.workflow_turns_processed_locally =
            self.partitions.iter().map(|entry| entry.workflow_turns_processed_locally).sum();
        self.sticky_build_hits = self.partitions.iter().map(|entry| entry.sticky_build_hits).sum();
        self.sticky_build_fallbacks =
            self.partitions.iter().map(|entry| entry.sticky_build_fallbacks).sum();
        self.workflow_task_poll_hits =
            self.partitions.iter().map(|entry| entry.workflow_task_poll_hits).sum();
        self.workflow_task_poll_empties =
            self.partitions.iter().map(|entry| entry.workflow_task_poll_empties).sum();
        self.workflow_task_poll_failures =
            self.partitions.iter().map(|entry| entry.workflow_task_poll_failures).sum();
        self.workflow_task_queue_latency = DurationStats::combine(
            self.partitions.iter().map(|entry| entry.workflow_task_queue_latency),
        );
        self.workflow_task_execution_latency = DurationStats::combine(
            self.partitions.iter().map(|entry| entry.workflow_task_execution_latency),
        );
    }

    fn hybrid_routing_summary(&self) -> HybridRoutingDebugSummary {
        let total_workflow_turns =
            self.workflow_turns_routed_via_matching + self.workflow_turns_processed_locally;
        let total_sticky_dispatches = self.sticky_build_hits + self.sticky_build_fallbacks;
        let total_polls = self.workflow_task_poll_hits
            + self.workflow_task_poll_empties
            + self.workflow_task_poll_failures;

        HybridRoutingDebugSummary {
            total_workflow_turns,
            matching_routed_turns: self.workflow_turns_routed_via_matching,
            local_executor_turns: self.workflow_turns_processed_locally,
            matching_routed_ratio: ratio(
                self.workflow_turns_routed_via_matching,
                total_workflow_turns,
            ),
            local_executor_ratio: ratio(
                self.workflow_turns_processed_locally,
                total_workflow_turns,
            ),
            sticky_build_hits: self.sticky_build_hits,
            sticky_build_fallbacks: self.sticky_build_fallbacks,
            sticky_hit_rate: ratio(self.sticky_build_hits, total_sticky_dispatches),
            sticky_fallback_rate: ratio(self.sticky_build_fallbacks, total_sticky_dispatches),
            workflow_task_poll_hits: self.workflow_task_poll_hits,
            workflow_task_poll_empties: self.workflow_task_poll_empties,
            workflow_task_poll_failures: self.workflow_task_poll_failures,
            poll_hit_rate: ratio(self.workflow_task_poll_hits, total_polls),
            poll_empty_rate: ratio(self.workflow_task_poll_empties, total_polls),
            poll_failure_rate: ratio(self.workflow_task_poll_failures, total_polls),
            avg_workflow_task_queue_latency_ms: self.workflow_task_queue_latency.average_ms(),
            max_workflow_task_queue_latency_ms: self.workflow_task_queue_latency.max_ms,
            avg_workflow_task_execution_latency_ms: self
                .workflow_task_execution_latency
                .average_ms(),
            max_workflow_task_execution_latency_ms: self.workflow_task_execution_latency.max_ms,
            restores_after_handoff: self.restores_after_handoff,
        }
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 { 0.0 } else { numerator as f64 / denominator as f64 }
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

async fn persist_state_with_mode(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    partition_id: i32,
    record: &mut HotStateRecord,
    state: &WorkflowInstanceState,
    persist_mode: &mut PersistMode,
) -> Result<()> {
    match persist_mode {
        PersistMode::Immediate => {
            persist_state_immediate(
                store,
                runtime,
                debug_state,
                lease_state,
                partition_id,
                record,
                state,
            )
            .await
        }
        PersistMode::Deferred { dirty } => {
            record.state = state.clone();
            runtime.put(record.clone());
            sync_debug_state(debug_state, partition_id, runtime);
            *dirty = true;
            Ok(())
        }
    }
}

async fn persist_state_immediate(
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

    if snapshot.lease_expires_at > chrono::Utc::now() {
        return Ok(snapshot);
    }

    let refreshed = store.get_partition_ownership(snapshot.partition_id).await?;
    let Some(refreshed) = refreshed else {
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
    };

    if refreshed.owner_id == snapshot.owner_id
        && refreshed.owner_epoch == snapshot.owner_epoch
        && refreshed.lease_expires_at > chrono::Utc::now()
    {
        let refreshed_state = LeaseState::from_record(&refreshed);
        if let Ok(mut state) = lease_state.lock() {
            *state = refreshed_state.clone();
        }
        return Ok(refreshed_state);
    }

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
    anyhow::bail!("executor partition ownership is stale")
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

async fn get_hybrid_routing_debug(
    AxumState(state): AxumState<ExecutorAppState>,
) -> Result<Json<HybridRoutingDebugSummary>, (StatusCode, Json<ExecutorErrorResponse>)> {
    let snapshot = {
        let state = state
            .debug
            .lock()
            .map_err(|_| internal_executor_error("executor debug state lock poisoned"))?;
        state.hybrid_routing_summary()
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
    lookup_cache: &mut WorkflowLookupCache,
) -> Result<fabrik_workflow::WorkflowDefinition> {
    let key = WorkflowLookupKey {
        tenant_id: event.tenant_id.clone(),
        definition_id: state.definition_id.clone(),
        definition_version: state.definition_version,
    };
    if lookup_cache.key.as_ref() != Some(&key) {
        lookup_cache.key = Some(key);
        lookup_cache.artifact = None;
        lookup_cache.definition = None;
    }
    if let Some(definition) = lookup_cache.definition.clone() {
        return Ok(definition);
    }
    if let Some(version) = state.definition_version {
        if let Some(definition) =
            store.get_definition_version(&event.tenant_id, &state.definition_id, version).await?
        {
            lookup_cache.definition = Some(definition.clone());
            return Ok(definition);
        }
    }

    let definition = store
        .get_latest_definition(&event.tenant_id, &state.definition_id)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "workflow definition {} not found for tenant {}",
                state.definition_id,
                event.tenant_id
            )
        })?;
    lookup_cache.definition = Some(definition.clone());
    Ok(definition)
}

async fn load_pinned_artifact(
    store: &WorkflowStore,
    event: &EventEnvelope<WorkflowEvent>,
    state: &WorkflowInstanceState,
    lookup_cache: &mut WorkflowLookupCache,
) -> Result<Option<CompiledWorkflowArtifact>> {
    let key = WorkflowLookupKey {
        tenant_id: event.tenant_id.clone(),
        definition_id: state.definition_id.clone(),
        definition_version: state.definition_version,
    };
    if lookup_cache.key.as_ref() != Some(&key) {
        lookup_cache.key = Some(key);
        lookup_cache.artifact = None;
        lookup_cache.definition = None;
    }
    if let Some(artifact) = lookup_cache.artifact.clone() {
        return Ok(artifact);
    }
    if let Some(version) = state.definition_version {
        if let Some(artifact) =
            store.get_artifact_version(&event.tenant_id, &state.definition_id, version).await?
        {
            lookup_cache.artifact = Some(Some(artifact.clone()));
            return Ok(Some(artifact));
        }
    }

    lookup_cache.artifact = Some(None);
    Ok(None)
}

async fn publish_plan(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &mut WorkflowPublishBuffer<'_>,
    event: &EventEnvelope<WorkflowEvent>,
    workflow_version: u32,
    plan: fabrik_workflow::WorkflowExecutionPlan,
    continue_input: Option<Value>,
) -> Result<()> {
    let final_state = plan.final_state.clone();
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
        publisher.publish(&envelope);
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
    let _ = continued;

    Ok(())
}

async fn publish_failure(
    _store: &WorkflowStore,
    _runtime: &mut ExecutorRuntime,
    _debug_state: &Arc<Mutex<ExecutorDebugState>>,
    _lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &mut WorkflowPublishBuffer<'_>,
    event: &EventEnvelope<WorkflowEvent>,
    _workflow_version: Option<u32>,
    reason: String,
    state: Option<String>,
) -> Result<()> {
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
    publisher.publish(&failed);
    Ok(())
}

async fn publish_compiled_plan(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &mut WorkflowPublishBuffer<'_>,
    event: &EventEnvelope<WorkflowEvent>,
    artifact: &CompiledWorkflowArtifact,
    plan: CompiledExecutionPlan,
    continue_input: Option<Value>,
) -> Result<()> {
    let final_state = plan.final_state.clone();
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
            publisher.publish(&envelope);

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
            publisher.publish(&trigger);
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
        publisher.publish(&envelope);
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
    let _ = continued;

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
    _store: &WorkflowStore,
    _runtime: &mut ExecutorRuntime,
    _debug_state: &Arc<Mutex<ExecutorDebugState>>,
    _lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &mut WorkflowPublishBuffer<'_>,
    event: &EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    let mut envelope = EventEnvelope::new(
        WorkflowEvent::WorkflowArtifactPinned.event_type(),
        source_identity(event, "executor-service"),
        WorkflowEvent::WorkflowArtifactPinned,
    );
    envelope.causation_id = Some(event.event_id);
    envelope.correlation_id = event.correlation_id.or(Some(event.event_id));
    publisher.publish(&envelope);
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

async fn publish_child_terminal_reflection(
    store: &WorkflowStore,
    _runtime: &mut ExecutorRuntime,
    _debug_state: &Arc<Mutex<ExecutorDebugState>>,
    _lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &mut WorkflowPublishBuffer<'_>,
    event: &EventEnvelope<WorkflowEvent>,
    parent_child: &fabrik_store::WorkflowChildRecord,
) -> Result<()> {
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
    publisher.publish(&envelope);
    Ok(())
}

#[cfg(test)]
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

#[cfg(test)]
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

async fn enforce_parent_close_policies(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &mut WorkflowPublishBuffer<'_>,
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
    _runtime: &mut ExecutorRuntime,
    _debug_state: &Arc<Mutex<ExecutorDebugState>>,
    _lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &mut WorkflowPublishBuffer<'_>,
    parent_event: &EventEnvelope<WorkflowEvent>,
    child: &fabrik_store::WorkflowChildRecord,
    child_run_id: &str,
    payload: WorkflowEvent,
) -> Result<()> {
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
    publisher.publish(&envelope);
    Ok(())
}

async fn maybe_auto_continue_as_new_legacy(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &mut WorkflowPublishBuffer<'_>,
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
    _runtime: &mut ExecutorRuntime,
    _debug_state: &Arc<Mutex<ExecutorDebugState>>,
    _lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &mut WorkflowPublishBuffer<'_>,
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
        publisher.publish(&envelope);
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

fn classify_sticky_dispatch(
    preferred_build_id: &str,
    actual_build_id: &str,
) -> StickyDispatchOutcome {
    if preferred_build_id.trim().is_empty() {
        StickyDispatchOutcome::None
    } else if preferred_build_id == actual_build_id {
        StickyDispatchOutcome::Hit
    } else {
        StickyDispatchOutcome::Fallback
    }
}

async fn maybe_auto_continue_as_new_compiled(
    store: &WorkflowStore,
    runtime: &mut ExecutorRuntime,
    debug_state: &Arc<Mutex<ExecutorDebugState>>,
    lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &mut WorkflowPublishBuffer<'_>,
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
    _runtime: &mut ExecutorRuntime,
    _debug_state: &Arc<Mutex<ExecutorDebugState>>,
    _lease_state: &Arc<Mutex<LeaseState>>,
    publisher: &mut WorkflowPublishBuffer<'_>,
    event: &EventEnvelope<WorkflowEvent>,
    definition_version: u32,
    artifact_hash: &str,
    input: Value,
    reason: &str,
) -> Result<()> {
    let workflow_task_queue = store
        .get_run_record(&event.tenant_id, &event.instance_id, &event.run_id)
        .await?
        .map(|run| run.workflow_task_queue)
        .unwrap_or_else(|| "default".to_owned());
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
    publisher.publish(&continued);

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
    trigger.metadata.insert("workflow_task_queue".to_owned(), workflow_task_queue.clone());
    publisher.publish(&trigger);

    store
        .put_run_start(
            &trigger.tenant_id,
            &trigger.instance_id,
            &trigger.run_id,
            &trigger.definition_id,
            Some(trigger.definition_version),
            Some(&trigger.artifact_hash),
            &workflow_task_queue,
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
    use anyhow::Context;
    use chrono::Utc;
    use fabrik_broker::{WorkflowHistoryFilter, read_workflow_history};
    use fabrik_events::WorkflowIdentity;
    use fabrik_store::{
        TaskQueueKind, WorkflowSignalRecord, WorkflowSignalStatus, WorkflowTaskRecord,
        WorkflowTaskStatus, WorkflowUpdateRecord, WorkflowUpdateStatus,
    };
    use fabrik_worker_protocol::activity_worker::{
        Ack, CompleteWorkflowTaskRequest, FailWorkflowTaskRequest, PollWorkflowTaskRequest,
        PollWorkflowTaskResponse, WorkflowTask,
        workflow_worker_api_server::{WorkflowWorkerApi, WorkflowWorkerApiServer},
    };
    use fabrik_workflow::WorkflowStatus;
    use serde_json::json;
    use std::{
        collections::HashSet,
        net::TcpListener,
        process::Command,
        sync::{Arc, Mutex},
        time::{Duration as StdDuration, Instant},
    };
    use tokio::{
        sync::{Notify, oneshot},
        time::sleep,
    };
    use tonic::{Request, Response, Status, transport::Server};

    fn demo_state(instance_id: &str) -> WorkflowInstanceState {
        WorkflowInstanceState {
            tenant_id: "tenant-a".to_owned(),
            instance_id: instance_id.to_owned(),
            run_id: format!("run-{instance_id}"),
            definition_id: "demo".to_owned(),
            definition_version: Some(1),
            artifact_hash: Some("artifact".to_owned()),
            workflow_task_queue: "default".to_owned(),
            sticky_workflow_build_id: None,
            sticky_workflow_poller_id: None,
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

    struct TestPostgres {
        container_name: String,
        database_url: String,
    }

    impl TestPostgres {
        fn start() -> Result<Option<Self>> {
            if !docker_available() {
                eprintln!("skipping executor integration test because docker is unavailable");
                return Ok(None);
            }

            let container_name = format!("fabrik-executor-test-pg-{}", Uuid::now_v7());
            let image = std::env::var("FABRIK_TEST_POSTGRES_IMAGE")
                .unwrap_or_else(|_| "postgres:16-alpine".to_owned());
            let output = Command::new("docker")
                .args([
                    "run",
                    "--detach",
                    "--rm",
                    "--name",
                    &container_name,
                    "--env",
                    "POSTGRES_USER=fabrik",
                    "--env",
                    "POSTGRES_PASSWORD=fabrik",
                    "--env",
                    "POSTGRES_DB=fabrik_test",
                    "--publish-all",
                    &image,
                ])
                .output()
                .with_context(|| format!("failed to start docker container {container_name}"))?;
            if !output.status.success() {
                anyhow::bail!(
                    "docker failed to start postgres test container: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }

            let host_port = match wait_for_docker_port(&container_name, "5432/tcp") {
                Ok(port) => port,
                Err(error) => {
                    let _ = cleanup_container(&container_name);
                    return Err(error);
                }
            };
            let database_url = format!(
                "postgres://fabrik:fabrik@127.0.0.1:{host_port}/fabrik_test?sslmode=disable"
            );
            Ok(Some(Self { container_name, database_url }))
        }

        async fn connect_store(&self) -> Result<WorkflowStore> {
            let deadline = Instant::now() + StdDuration::from_secs(30);
            loop {
                match WorkflowStore::connect(&self.database_url).await {
                    Ok(store) => {
                        store.init().await?;
                        return Ok(store);
                    }
                    Err(error) if Instant::now() < deadline => {
                        let _ = error;
                        sleep(StdDuration::from_millis(250)).await;
                    }
                    Err(error) => {
                        let logs = docker_logs(&self.container_name).unwrap_or_default();
                        return Err(error).with_context(|| {
                            format!(
                                "postgres test container {} did not become ready; logs:\n{}",
                                self.container_name, logs
                            )
                        });
                    }
                }
            }
        }
    }

    impl Drop for TestPostgres {
        fn drop(&mut self) {
            let _ = cleanup_container(&self.container_name);
        }
    }

    struct TestRedpanda {
        container_name: String,
        broker: BrokerConfig,
    }

    impl TestRedpanda {
        fn start() -> Result<Option<Self>> {
            if !docker_available() {
                eprintln!("skipping executor integration test because docker is unavailable");
                return Ok(None);
            }

            let kafka_port = choose_free_port().context("failed to allocate kafka host port")?;
            let container_name = format!("fabrik-executor-test-rp-{}", Uuid::now_v7());
            let image = std::env::var("FABRIK_TEST_REDPANDA_IMAGE")
                .unwrap_or_else(|_| "docker.redpanda.com/redpandadata/redpanda:v25.1.2".to_owned());
            let topic = format!("workflow-events-test-{}", Uuid::now_v7());
            let output = Command::new("docker")
                .args([
                    "run",
                    "--detach",
                    "--rm",
                    "--name",
                    &container_name,
                    "--publish",
                    &format!("{kafka_port}:{kafka_port}"),
                    &image,
                    "redpanda",
                    "start",
                    "--overprovisioned",
                    "--smp",
                    "1",
                    "--memory",
                    "1G",
                    "--reserve-memory",
                    "0M",
                    "--node-id",
                    "0",
                    "--check=false",
                    "--kafka-addr",
                    &format!("PLAINTEXT://0.0.0.0:9092,OUTSIDE://0.0.0.0:{kafka_port}"),
                    "--advertise-kafka-addr",
                    &format!("PLAINTEXT://127.0.0.1:9092,OUTSIDE://127.0.0.1:{kafka_port}"),
                    "--rpc-addr",
                    "0.0.0.0:33145",
                    "--advertise-rpc-addr",
                    "127.0.0.1:33145",
                ])
                .output()
                .with_context(|| format!("failed to start docker container {container_name}"))?;
            if !output.status.success() {
                anyhow::bail!(
                    "docker failed to start redpanda test container: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }

            Ok(Some(Self {
                container_name,
                broker: BrokerConfig::new(format!("127.0.0.1:{kafka_port}"), topic, 1),
            }))
        }

        async fn connect_publisher(&self) -> Result<WorkflowPublisher> {
            let deadline = Instant::now() + StdDuration::from_secs(45);
            loop {
                match WorkflowPublisher::new(&self.broker, "executor-service-test").await {
                    Ok(publisher) => return Ok(publisher),
                    Err(error) if Instant::now() < deadline => {
                        let _ = error;
                        sleep(StdDuration::from_millis(500)).await;
                    }
                    Err(error) => {
                        let logs = docker_logs(&self.container_name).unwrap_or_default();
                        return Err(error).with_context(|| {
                            format!(
                                "redpanda test container {} did not become ready; logs:\n{}",
                                self.container_name, logs
                            )
                        });
                    }
                }
            }
        }
    }

    impl Drop for TestRedpanda {
        fn drop(&mut self) {
            let _ = cleanup_container(&self.container_name);
        }
    }

    fn docker_available() -> bool {
        Command::new("docker")
            .args(["info", "--format", "{{.ServerVersion}}"])
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    fn wait_for_docker_port(container_name: &str, container_port: &str) -> Result<u16> {
        let deadline = Instant::now() + StdDuration::from_secs(15);
        loop {
            let output = Command::new("docker")
                .args([
                    "inspect",
                    "--format",
                    &format!(
                        "{{{{(index (index .NetworkSettings.Ports \"{container_port}\") 0).HostPort}}}}"
                    ),
                    container_name,
                ])
                .output()
                .with_context(|| format!("failed to inspect docker container {container_name}"))?;
            if output.status.success() {
                let host_port = String::from_utf8_lossy(&output.stdout).trim().to_owned();
                if !host_port.is_empty() {
                    return host_port
                        .parse::<u16>()
                        .with_context(|| format!("invalid mapped port {host_port}"));
                }
            }
            if Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for port {container_port} on {container_name}");
            }
            std::thread::sleep(StdDuration::from_millis(100));
        }
    }

    fn choose_free_port() -> Result<u16> {
        let listener = TcpListener::bind("127.0.0.1:0").context("failed to bind ephemeral port")?;
        let port = listener.local_addr().context("failed to read ephemeral socket address")?.port();
        drop(listener);
        Ok(port)
    }

    fn docker_logs(container_name: &str) -> Result<String> {
        let output = Command::new("docker")
            .args(["logs", container_name])
            .output()
            .with_context(|| format!("failed to read docker logs for {container_name}"))?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        Ok(format!("{stdout}{stderr}"))
    }

    fn cleanup_container(container_name: &str) -> Result<()> {
        let output = Command::new("docker")
            .args(["rm", "--force", container_name])
            .output()
            .with_context(|| format!("failed to remove docker container {container_name}"))?;
        if output.status.success() {
            Ok(())
        } else {
            anyhow::bail!(
                "docker failed to remove container {container_name}: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )
        }
    }

    fn test_event(
        identity: &WorkflowIdentity,
        payload: WorkflowEvent,
    ) -> EventEnvelope<WorkflowEvent> {
        let mut event = EventEnvelope::new(payload.event_type(), identity.clone(), payload);
        event.metadata.insert("workflow_task_queue".to_owned(), "default".to_owned());
        event
    }

    #[derive(Clone)]
    struct TestWorkflowApi {
        store: WorkflowStore,
        notify: Arc<Notify>,
        blocked_workers: Arc<Mutex<HashSet<String>>>,
        lease_ttl: chrono::Duration,
    }

    impl TestWorkflowApi {
        fn workflow_task_to_proto(record: &WorkflowTaskRecord) -> WorkflowTask {
            WorkflowTask {
                task_id: record.task_id.to_string(),
                tenant_id: record.tenant_id.clone(),
                instance_id: record.instance_id.clone(),
                run_id: record.run_id.clone(),
                definition_id: record.definition_id.clone(),
                definition_version: record.definition_version.unwrap_or_default(),
                artifact_hash: record.artifact_hash.clone().unwrap_or_default(),
                partition_id: record.partition_id,
                task_queue: record.task_queue.clone(),
                preferred_build_id: record.preferred_build_id.clone().unwrap_or_default(),
                mailbox_consumed_seq: record.mailbox_consumed_seq,
                resume_consumed_seq: record.resume_consumed_seq,
                mailbox_backlog: record.mailbox_backlog,
                resume_backlog: record.resume_backlog,
                attempt_count: record.attempt_count,
                created_at_unix_ms: record.created_at.timestamp_millis(),
            }
        }
    }

    #[tonic::async_trait]
    impl WorkflowWorkerApi for TestWorkflowApi {
        async fn poll_workflow_task(
            &self,
            request: Request<PollWorkflowTaskRequest>,
        ) -> Result<Response<PollWorkflowTaskResponse>, Status> {
            let request = request.into_inner();
            let deadline = tokio::time::Instant::now()
                + StdDuration::from_millis(request.poll_timeout_ms.max(1));

            loop {
                let blocked = self
                    .blocked_workers
                    .lock()
                    .expect("blocked workers lock")
                    .contains(&request.worker_id);
                if !blocked {
                    if let Some(record) = self
                        .store
                        .lease_next_workflow_task(
                            request.partition_id,
                            &request.worker_id,
                            &request.worker_build_id,
                            self.lease_ttl,
                        )
                        .await
                        .map_err(|error| Status::internal(error.to_string()))?
                    {
                        self.store
                            .upsert_queue_poller(
                                &record.tenant_id,
                                TaskQueueKind::Workflow,
                                &record.task_queue,
                                &request.worker_id,
                                &request.worker_build_id,
                                Some(request.partition_id),
                                None,
                                self.lease_ttl,
                            )
                            .await
                            .map_err(|error| Status::internal(error.to_string()))?;
                        return Ok(Response::new(PollWorkflowTaskResponse {
                            task: Some(Self::workflow_task_to_proto(&record)),
                        }));
                    }
                }

                let now = tokio::time::Instant::now();
                if now >= deadline {
                    return Ok(Response::new(PollWorkflowTaskResponse { task: None }));
                }
                let remaining = deadline.saturating_duration_since(now);
                if tokio::time::timeout(remaining, self.notify.notified()).await.is_err() {
                    return Ok(Response::new(PollWorkflowTaskResponse { task: None }));
                }
            }
        }

        async fn complete_workflow_task(
            &self,
            request: Request<CompleteWorkflowTaskRequest>,
        ) -> Result<Response<Ack>, Status> {
            let request = request.into_inner();
            let task_id = Uuid::parse_str(&request.task_id)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            self.store
                .complete_workflow_task(
                    task_id,
                    &request.worker_id,
                    &request.worker_build_id,
                    Utc::now(),
                )
                .await
                .map_err(|error| Status::internal(error.to_string()))?;
            Ok(Response::new(Ack {}))
        }

        async fn fail_workflow_task(
            &self,
            request: Request<FailWorkflowTaskRequest>,
        ) -> Result<Response<Ack>, Status> {
            let request = request.into_inner();
            let task_id = Uuid::parse_str(&request.task_id)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            if self
                .store
                .fail_workflow_task(
                    task_id,
                    &request.worker_id,
                    &request.worker_build_id,
                    &request.error,
                    Utc::now(),
                )
                .await
                .map_err(|error| Status::internal(error.to_string()))?
            {
                self.blocked_workers
                    .lock()
                    .expect("blocked workers lock")
                    .insert(request.worker_id);
                self.notify.notify_waiters();
            }
            Ok(Response::new(Ack {}))
        }
    }

    struct TestWorkflowApiServerHandle {
        endpoint: String,
        shutdown: Option<oneshot::Sender<()>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl TestWorkflowApiServerHandle {
        async fn start(store: WorkflowStore) -> Result<Self> {
            let port = choose_free_port().context("failed to allocate workflow api port")?;
            let addr = format!("127.0.0.1:{port}")
                .parse()
                .context("failed to parse workflow api address")?;
            let endpoint = format!("http://127.0.0.1:{port}");
            let notify = Arc::new(Notify::new());
            let blocked_workers = Arc::new(Mutex::new(HashSet::new()));
            let api = TestWorkflowApi {
                store,
                notify,
                blocked_workers,
                lease_ttl: chrono::Duration::seconds(30),
            };
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let task = tokio::spawn(async move {
                let _ = Server::builder()
                    .add_service(WorkflowWorkerApiServer::new(api))
                    .serve_with_shutdown(addr, async {
                        let _ = shutdown_rx.await;
                    })
                    .await;
            });

            Ok(Self { endpoint, shutdown: Some(shutdown_tx), task })
        }

        async fn stop(mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
            let _ = self.task.await;
        }
    }

    async fn wait_for_workflow_task<F>(
        store: &WorkflowStore,
        task_id: Uuid,
        predicate: F,
        timeout: StdDuration,
    ) -> Result<WorkflowTaskRecord>
    where
        F: Fn(&WorkflowTaskRecord) -> bool,
    {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(record) = store.get_workflow_task(task_id).await? {
                if predicate(&record) {
                    return Ok(record);
                }
            }
            if Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for workflow task {task_id}");
            }
            sleep(StdDuration::from_millis(50)).await;
        }
    }

    #[test]
    fn hot_state_cache_returns_cached_entries() {
        let mut runtime = ExecutorRuntime::new(2, 10, None, None, None, 64, 10_000);
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
        let mut runtime = ExecutorRuntime::new(2, 10, None, None, None, 64, 10_000);
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

    #[test]
    fn sticky_dispatch_classifies_hit_and_fallback() {
        assert_eq!(classify_sticky_dispatch("", "build-a"), StickyDispatchOutcome::None);
        assert_eq!(classify_sticky_dispatch("build-a", "build-a"), StickyDispatchOutcome::Hit);
        assert_eq!(classify_sticky_dispatch("build-a", "build-b"), StickyDispatchOutcome::Fallback);
    }

    #[test]
    fn debug_state_tracks_workflow_routing_metrics() {
        let ownership = PartitionOwnershipRecord {
            partition_id: 2,
            owner_id: "executor-1".to_owned(),
            owner_epoch: 1,
            lease_expires_at: Utc::now(),
            acquired_at: Utc::now(),
            last_transition_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let mut debug = ExecutorDebugState::new(&[ownership], 10, 5);
        debug.record_matching_routed_event(2);
        debug.record_local_executor_event(2);
        debug.record_workflow_poll_hit(2, std::time::Duration::from_millis(3));
        debug.record_workflow_poll_empty(2);
        debug.record_workflow_poll_failure(2);
        debug.record_workflow_task_execution(
            2,
            11,
            std::time::Duration::from_millis(7),
            StickyDispatchOutcome::Fallback,
        );

        assert_eq!(debug.workflow_turns_routed_via_matching, 1);
        assert_eq!(debug.workflow_turns_processed_locally, 1);
        assert_eq!(debug.workflow_task_poll_hits, 1);
        assert_eq!(debug.workflow_task_poll_empties, 1);
        assert_eq!(debug.workflow_task_poll_failures, 1);
        assert_eq!(debug.sticky_build_fallbacks, 1);
        assert_eq!(debug.workflow_task_queue_latency.sample_count, 1);
        assert_eq!(debug.workflow_task_queue_latency.total_ms, 11);
        assert_eq!(debug.workflow_task_execution_latency.max_ms, 7);
    }

    #[test]
    fn hybrid_routing_summary_derives_ratios_and_averages() {
        let ownership = PartitionOwnershipRecord {
            partition_id: 2,
            owner_id: "executor-1".to_owned(),
            owner_epoch: 1,
            lease_expires_at: Utc::now(),
            acquired_at: Utc::now(),
            last_transition_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let mut debug = ExecutorDebugState::new(&[ownership], 10, 5);
        debug.record_matching_routed_event(2);
        debug.record_matching_routed_event(2);
        debug.record_local_executor_event(2);
        debug.record_workflow_poll_hit(2, std::time::Duration::from_millis(3));
        debug.record_workflow_poll_empty(2);
        debug.record_workflow_poll_failure(2);
        debug.record_workflow_task_execution(
            2,
            9,
            std::time::Duration::from_millis(6),
            StickyDispatchOutcome::Hit,
        );
        debug.record_workflow_task_execution(
            2,
            15,
            std::time::Duration::from_millis(12),
            StickyDispatchOutcome::Fallback,
        );
        debug.restores_after_handoff = 4;

        assert_eq!(
            debug.hybrid_routing_summary(),
            HybridRoutingDebugSummary {
                total_workflow_turns: 3,
                matching_routed_turns: 2,
                local_executor_turns: 1,
                matching_routed_ratio: 2.0 / 3.0,
                local_executor_ratio: 1.0 / 3.0,
                sticky_build_hits: 1,
                sticky_build_fallbacks: 1,
                sticky_hit_rate: 0.5,
                sticky_fallback_rate: 0.5,
                workflow_task_poll_hits: 1,
                workflow_task_poll_empties: 1,
                workflow_task_poll_failures: 1,
                poll_hit_rate: 1.0 / 3.0,
                poll_empty_rate: 1.0 / 3.0,
                poll_failure_rate: 1.0 / 3.0,
                avg_workflow_task_queue_latency_ms: 12.0,
                max_workflow_task_queue_latency_ms: 15,
                avg_workflow_task_execution_latency_ms: 9.0,
                max_workflow_task_execution_latency_ms: 12,
                restores_after_handoff: 4,
            }
        );
    }

    #[tokio::test]
    async fn deferred_persist_keeps_intermediate_state_out_of_store_until_flush() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let owner = store
            .claim_partition_ownership(2, "executor-a", StdDuration::from_secs(30))
            .await?
            .context("partition owner should claim benchmark partition")?;
        let debug = Arc::new(Mutex::new(ExecutorDebugState::new(&[owner.clone()], 8, 100)));
        let lease = Arc::new(Mutex::new(LeaseState::from_record(&owner)));
        let mut runtime = ExecutorRuntime::new(8, 100, None, None, None, 64, 10_000);
        let mut record = HotStateRecord {
            state: demo_state("instance-deferred"),
            last_snapshot_event_count: None,
            restore_source: RestoreSource::Initialized,
        };
        store.upsert_instance(&record.state).await?;
        runtime.put(record.clone());
        sync_debug_state(&debug, owner.partition_id, &runtime);

        let mut persist_mode = PersistMode::Deferred { dirty: false };
        let mut staged_state = record.state.clone();
        staged_state.event_count = 2;
        staged_state.last_event_type = "SignalQueued".to_owned();
        persist_state_with_mode(
            &store,
            &mut runtime,
            &debug,
            &lease,
            owner.partition_id,
            &mut record,
            &staged_state,
            &mut persist_mode,
        )
        .await?;

        let cached_before_flush = runtime
            .peek(&record.state.tenant_id, &record.state.instance_id)
            .context("deferred state should remain cached")?;
        assert_eq!(cached_before_flush.state.event_count, 2);
        let persisted_before_flush = store
            .get_instance(&record.state.tenant_id, &record.state.instance_id)
            .await?
            .context("projected workflow state should exist")?;
        assert_eq!(persisted_before_flush.event_count, 1);

        staged_state.event_count = 3;
        staged_state.last_event_type = "MarkerRecorded".to_owned();
        persist_state_with_mode(
            &store,
            &mut runtime,
            &debug,
            &lease,
            owner.partition_id,
            &mut record,
            &staged_state,
            &mut persist_mode,
        )
        .await?;

        let task = WorkflowTask {
            task_id: Uuid::nil().to_string(),
            tenant_id: record.state.tenant_id.clone(),
            instance_id: record.state.instance_id.clone(),
            run_id: record.state.run_id.clone(),
            definition_id: record.state.definition_id.clone(),
            definition_version: record.state.definition_version.unwrap_or_default(),
            artifact_hash: record.state.artifact_hash.clone().unwrap_or_default(),
            partition_id: owner.partition_id,
            task_queue: record.state.workflow_task_queue.clone(),
            preferred_build_id: String::new(),
            mailbox_consumed_seq: 0,
            resume_consumed_seq: 0,
            mailbox_backlog: 0,
            resume_backlog: 0,
            attempt_count: 0,
            created_at_unix_ms: Utc::now().timestamp_millis(),
        };
        flush_deferred_state(&store, &mut runtime, &debug, &lease, &task).await?;

        let persisted_after_flush = store
            .get_instance(&record.state.tenant_id, &record.state.instance_id)
            .await?
            .context("projected workflow state should still exist")?;
        assert_eq!(persisted_after_flush.event_count, 3);
        assert_eq!(persisted_after_flush.last_event_type, "MarkerRecorded");

        Ok(())
    }

    #[tokio::test]
    async fn ownership_handoff_replays_broker_tail_from_snapshot() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let publisher = redpanda.connect_publisher().await?;
        let broker = redpanda.broker.clone();

        let identity = WorkflowIdentity::new(
            "tenant-a",
            "demo",
            1,
            "artifact-1",
            "instance-handoff",
            "run-handoff",
            "test",
        );
        let partition_id = publisher.partition_for_key(&identity.partition_key);

        let trigger =
            test_event(&identity, WorkflowEvent::WorkflowTriggered { input: json!({"ok": true}) });
        let started = test_event(&identity, WorkflowEvent::WorkflowStarted);
        let signal_queued = test_event(
            &identity,
            WorkflowEvent::SignalQueued {
                signal_id: "sig-1".to_owned(),
                signal_type: "external.approved".to_owned(),
                payload: json!({"approved": true}),
            },
        );
        let marker = test_event(
            &identity,
            WorkflowEvent::MarkerRecorded {
                marker_id: "handoff".to_owned(),
                value: json!({"owner": "executor-b"}),
            },
        );

        publisher.publish(&trigger, &trigger.partition_key).await?;
        publisher.publish(&started, &started.partition_key).await?;
        publisher.publish(&signal_queued, &signal_queued.partition_key).await?;

        let mut snapshot_state = WorkflowInstanceState::try_from(&trigger)?;
        snapshot_state.apply_event(&started);
        store
            .put_run_start(
                &trigger.tenant_id,
                &trigger.instance_id,
                &trigger.run_id,
                &trigger.definition_id,
                Some(trigger.definition_version),
                Some(&trigger.artifact_hash),
                "default",
                trigger.event_id,
                trigger.occurred_at,
                None,
                None,
            )
            .await?;
        store.upsert_instance(&snapshot_state).await?;
        store.put_snapshot(&snapshot_state).await?;

        let owner_a = store
            .claim_partition_ownership(partition_id, "executor-a", StdDuration::from_secs(2))
            .await?
            .context("owner a should claim partition")?;
        let debug_a = Arc::new(Mutex::new(ExecutorDebugState::new(&[owner_a.clone()], 8, 100)));
        let lease_a = Arc::new(Mutex::new(LeaseState::from_record(&owner_a)));
        let mut runtime_a = ExecutorRuntime::new(8, 100, None, None, None, 64, 10_000);
        let mut persist_mode_a = PersistMode::Immediate;
        let mut lookup_cache_a = WorkflowLookupCache::default();
        let mut publish_buffer_a = WorkflowPublishBuffer::new(&publisher);
        process_event(
            &store,
            &broker,
            &mut publish_buffer_a,
            &mut runtime_a,
            &debug_a,
            &lease_a,
            signal_queued.clone(),
            &mut persist_mode_a,
            &mut lookup_cache_a,
        )
        .await?;
        publish_buffer_a.flush().await?;
        let state_after_owner_a = store
            .get_instance(&identity.tenant_id, &identity.instance_id)
            .await?
            .context("state after owner a should exist")?;
        assert_eq!(state_after_owner_a.event_count, 3);
        assert_eq!(state_after_owner_a.last_event_type, "SignalQueued");
        let snapshot_after_owner_a = store
            .get_snapshot_for_run(&identity.tenant_id, &identity.instance_id, &identity.run_id)
            .await?
            .context("snapshot should still exist")?;
        assert_eq!(snapshot_after_owner_a.event_count, 2);

        sleep(StdDuration::from_millis(2_200)).await;

        let owner_b = store
            .claim_partition_ownership(partition_id, "executor-b", StdDuration::from_secs(5))
            .await?
            .context("owner b should claim expired partition")?;
        let debug_b = Arc::new(Mutex::new(ExecutorDebugState::new(&[owner_b.clone()], 8, 100)));
        let lease_b = Arc::new(Mutex::new(LeaseState::from_record(&owner_b)));
        let mut runtime_b = ExecutorRuntime::new(8, 100, None, None, None, 64, 10_000);
        let mut persist_mode_b = PersistMode::Immediate;
        let mut lookup_cache_b = WorkflowLookupCache::default();

        publisher.publish(&marker, &marker.partition_key).await?;
        let mut publish_buffer_b = WorkflowPublishBuffer::new(&publisher);
        process_event(
            &store,
            &broker,
            &mut publish_buffer_b,
            &mut runtime_b,
            &debug_b,
            &lease_b,
            marker.clone(),
            &mut persist_mode_b,
            &mut lookup_cache_b,
        )
        .await?;
        publish_buffer_b.flush().await?;

        assert_eq!(runtime_b.restores_from_snapshot_replay, 1);
        assert_eq!(runtime_b.restores_after_handoff, 1);
        let cache_entry = runtime_b
            .cache
            .get(&HotStateKey::new(&identity.tenant_id, &identity.instance_id))
            .context("handoff restore should populate executor cache")?;
        assert_eq!(cache_entry.record.restore_source, RestoreSource::SnapshotReplay);
        assert_eq!(cache_entry.record.state.event_count, 4);
        assert_eq!(cache_entry.record.state.last_event_type, "MarkerRecorded");

        let final_state = store
            .get_instance(&identity.tenant_id, &identity.instance_id)
            .await?
            .context("final workflow state should exist")?;
        assert_eq!(final_state.event_count, 4);
        assert_eq!(final_state.last_event_type, "MarkerRecorded");
        assert_eq!(final_state.status, WorkflowStatus::Running);

        let debug_snapshot = debug_b.lock().expect("executor debug state lock").clone();
        assert_eq!(debug_snapshot.restores_after_handoff, 1);

        Ok(())
    }

    #[tokio::test]
    async fn matching_grpc_and_executor_poll_loop_fail_over_to_second_worker() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let publisher = redpanda.connect_publisher().await?;
        let broker = redpanda.broker.clone();
        let workflow_api = TestWorkflowApiServerHandle::start(store.clone()).await?;

        let identity = WorkflowIdentity::new(
            "tenant-a",
            "demo",
            1,
            "artifact-1",
            "instance-poll-failover",
            "run-poll-failover",
            "test",
        );
        let partition_id = publisher.partition_for_key(&identity.partition_key);

        store
            .register_task_queue_build(
                &identity.tenant_id,
                TaskQueueKind::Workflow,
                "default",
                "build-a",
                &[identity.artifact_hash.clone()],
                None,
            )
            .await?;
        store
            .register_task_queue_build(
                &identity.tenant_id,
                TaskQueueKind::Workflow,
                "default",
                "build-b",
                &[identity.artifact_hash.clone()],
                None,
            )
            .await?;
        store
            .upsert_compatibility_set(
                &identity.tenant_id,
                TaskQueueKind::Workflow,
                "default",
                "stable",
                &["build-a".to_owned(), "build-b".to_owned()],
                true,
            )
            .await?;

        let trigger =
            test_event(&identity, WorkflowEvent::WorkflowTriggered { input: json!({"ok": true}) });
        let started = test_event(&identity, WorkflowEvent::WorkflowStarted);
        let cancel_requested = test_event(
            &identity,
            WorkflowEvent::WorkflowCancellationRequested {
                reason: "operator requested stop".to_owned(),
            },
        );
        publisher.publish(&cancel_requested, &cancel_requested.partition_key).await?;

        let mut state = WorkflowInstanceState::try_from(&trigger)?;
        state.apply_event(&started);
        store
            .put_run_start(
                &identity.tenant_id,
                &identity.instance_id,
                &identity.run_id,
                &identity.definition_id,
                Some(identity.definition_version),
                Some(&identity.artifact_hash),
                "default",
                trigger.event_id,
                trigger.occurred_at,
                None,
                None,
            )
            .await?;
        store.upsert_instance(&state).await?;
        let task = store
            .enqueue_workflow_task(partition_id, "default", Some("build-a"), &cancel_requested)
            .await?
            .context("workflow task should be created")?;

        let runtime_a = Arc::new(tokio::sync::Mutex::new(ExecutorRuntime::new(
            8, 100, None, None, None, 64, 10_000,
        )));
        let debug_a = Arc::new(Mutex::new(ExecutorDebugState::new(&[], 8, 100)));
        let lease_a = Arc::new(Mutex::new(LeaseState {
            partition_id,
            owner_id: "executor-a".to_owned(),
            owner_epoch: 1,
            lease_expires_at: Utc::now(),
            active: false,
        }));
        let poller_a = tokio::spawn(run_workflow_poll_loop(
            workflow_api.endpoint.clone(),
            partition_id,
            "worker-a".to_owned(),
            "build-a".to_owned(),
            publisher.clone(),
            store.clone(),
            broker.clone(),
            runtime_a.clone(),
            debug_a.clone(),
            lease_a.clone(),
        ));

        let failed_task = wait_for_workflow_task(
            &store,
            task.task_id,
            |record| record.status == WorkflowTaskStatus::Pending && record.attempt_count >= 1,
            StdDuration::from_secs(15),
        )
        .await?;
        assert_eq!(
            failed_task.last_error.as_deref(),
            Some("executor does not currently hold partition ownership")
        );
        poller_a.abort();
        let _ = poller_a.await;

        let owner_b = store
            .claim_partition_ownership(partition_id, "executor-b", StdDuration::from_secs(10))
            .await?
            .context("owner b should claim partition")?;
        let runtime_b = Arc::new(tokio::sync::Mutex::new(ExecutorRuntime::new(
            8, 100, None, None, None, 64, 10_000,
        )));
        let debug_b = Arc::new(Mutex::new(ExecutorDebugState::new(&[owner_b.clone()], 8, 100)));
        let lease_b = Arc::new(Mutex::new(LeaseState::from_record(&owner_b)));
        let poller_b = tokio::spawn(run_workflow_poll_loop(
            workflow_api.endpoint.clone(),
            partition_id,
            "worker-b".to_owned(),
            "build-b".to_owned(),
            publisher.clone(),
            store.clone(),
            broker.clone(),
            runtime_b.clone(),
            debug_b.clone(),
            lease_b.clone(),
        ));

        let completed_task = wait_for_workflow_task(
            &store,
            task.task_id,
            |record| record.status == WorkflowTaskStatus::Completed,
            StdDuration::from_secs(20),
        )
        .await?;
        assert_eq!(completed_task.lease_build_id.as_deref(), Some("build-b"));
        assert_eq!(completed_task.attempt_count, 1);

        let history = read_workflow_history(
            &broker,
            "executor-poll-failover-test",
            &WorkflowHistoryFilter::new(
                &identity.tenant_id,
                &identity.instance_id,
                &identity.run_id,
            ),
            StdDuration::from_millis(500),
            StdDuration::from_secs(10),
        )
        .await?;
        assert!(history.iter().any(|event| matches!(
            event.payload,
            WorkflowEvent::WorkflowCancellationRequested { .. }
        )));
        assert!(
            history
                .iter()
                .any(|event| matches!(event.payload, WorkflowEvent::WorkflowCancelled { .. }))
        );

        let run = store
            .get_run_record(&identity.tenant_id, &identity.instance_id, &identity.run_id)
            .await?
            .context("workflow run should exist")?;
        assert_eq!(run.sticky_workflow_build_id.as_deref(), Some("build-b"));
        assert_eq!(run.sticky_workflow_poller_id.as_deref(), Some("worker-b"));

        let debug_snapshot = debug_b.lock().expect("executor debug state lock").clone();
        assert_eq!(debug_snapshot.workflow_task_poll_hits, 1);
        assert_eq!(debug_snapshot.workflow_turns_routed_via_matching, 1);
        assert_eq!(debug_snapshot.sticky_build_fallbacks, 1);

        poller_b.abort();
        let _ = poller_b.await;
        workflow_api.stop().await;
        Ok(())
    }
}
