use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use fabrik_broker::{JsonTopicConfig, JsonTopicPublisher, load_json_topic_latest_offsets};
use fabrik_config::PostgresConfig;
use fabrik_store::{
    TopicAdapterAction, TopicAdapterDeadLetterPolicy, TopicAdapterKind, TopicAdapterOffsetRecord,
    TopicAdapterRecord, TopicAdapterUpsert, WorkflowStore,
};
use fabrik_throughput::{
    BENCHMARK_ECHO_ACTIVITY, BENCHMARK_FAST_COUNT_ACTIVITY, PG_V1_BACKEND, STREAM_V2_BACKEND,
    benchmark_compact_input_spec,
};
use fabrik_workflow::{
    ArtifactEntrypoint, Assignment, CompiledStateNode, CompiledWorkflow, CompiledWorkflowArtifact,
    ErrorTransition, Expression, RetryPolicy,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

const DEFAULT_POLL_INTERVAL_MS: u64 = 250;
const DEFAULT_TIMEOUT_SECS: u64 = 300;
const DEFAULT_STREAM_PROJECTION_TIMEOUT_SECS: u64 = 30;
const DEFAULT_ADAPTER_READY_TIMEOUT_SECS: u64 = 30;
const DEFAULT_ADAPTER_FAILOVER_OBSERVATION_TIMEOUT_SECS: u64 = 15;

#[derive(Debug, Clone)]
struct BenchmarkProfile {
    workflow_count: usize,
    activities_per_workflow: usize,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ExecutionMode {
    Durable,
    Throughput,
    Unified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchmarkIngressDriver {
    HttpTrigger,
    TopicAdapterStart,
    TopicAdapterSignal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchmarkWorkloadKind {
    Fanout,
    TimerGate,
    SignalGate,
    UpdateGate,
    ContinueAsNew,
    ChildWorkflow,
}

impl BenchmarkWorkloadKind {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "fanout" => Ok(Self::Fanout),
            "timer_gate" => Ok(Self::TimerGate),
            "signal_gate" => Ok(Self::SignalGate),
            "update_gate" => Ok(Self::UpdateGate),
            "continue_as_new" => Ok(Self::ContinueAsNew),
            "child_workflow" => Ok(Self::ChildWorkflow),
            other => bail!(
                "unknown --workload-kind {other}; expected fanout, timer_gate, signal_gate, update_gate, continue_as_new, or child_workflow"
            ),
        }
    }

    fn definition_prefix(self) -> &'static str {
        match self {
            Self::Fanout => "fanout",
            Self::TimerGate => "timer-gate",
            Self::SignalGate => "signal-gate",
            Self::UpdateGate => "update-gate",
            Self::ContinueAsNew => "continue-as-new",
            Self::ChildWorkflow => "child-workflow",
        }
    }
}

#[derive(Debug, Clone)]
struct Args {
    suite_name: Option<String>,
    scenario_tag: Option<String>,
    profile_name: String,
    profile: BenchmarkProfile,
    output_path: PathBuf,
    worker_count: usize,
    payload_size: usize,
    retry_rate: f64,
    cancel_rate: f64,
    retry_delay_ms: u64,
    tenant_id: String,
    task_queue: String,
    execution_mode: ExecutionMode,
    throughput_backend: Option<String>,
    ingress_driver: BenchmarkIngressDriver,
    workload_kind: BenchmarkWorkloadKind,
    activity_type: String,
    bulk_reducer: String,
    chunk_size: u32,
    timer_secs: u32,
    continue_rounds: u32,
    timeout: Duration,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    scenario: String,
    profile: String,
    started_at: DateTime<Utc>,
    execution_completed_at: DateTime<Utc>,
    completed_at: DateTime<Utc>,
    execution_duration_ms: u128,
    projection_convergence_duration_ms: u128,
    duration_ms: u128,
    workflow_count: usize,
    activities_per_workflow: usize,
    total_activities: usize,
    worker_count: usize,
    payload_size: usize,
    retry_rate: f64,
    cancel_rate: f64,
    retry_delay_ms: u64,
    definition_id: String,
    task_queue: String,
    execution_mode: ExecutionMode,
    throughput_backend: Option<String>,
    bulk_reducer: String,
    chunk_size: u32,
    instance_prefix: String,
    workflow_outcomes: WorkflowOutcomeMetrics,
    activity_metrics: ActivityMetrics,
    coalescing_metrics: CoalescingMetrics,
    backlog_metrics: BacklogMetrics,
    bulk_batch_rows: u64,
    bulk_chunk_rows: u64,
    projection_batch_rows: u64,
    projection_chunk_rows: u64,
    max_aggregation_group_count: u64,
    grouped_batch_rows: u64,
    executor_debug: Value,
    executor_debug_before: Option<Value>,
    executor_debug_after: Option<Value>,
    executor_debug_delta: Option<Value>,
    throughput_runtime_debug_before: Option<Value>,
    throughput_runtime_debug: Option<Value>,
    throughput_runtime_debug_delta: Option<Value>,
    throughput_projector_debug_before: Option<Value>,
    throughput_projector_debug: Option<Value>,
    throughput_projector_debug_delta: Option<Value>,
    batch_routing_metrics: BatchRoutingMetrics,
    failover_injection: Option<FailoverInjectionMetrics>,
    adapter_metrics: Option<AdapterMetrics>,
    control_plane_metrics: Option<ControlPlaneMetrics>,
    executor_debug_delta_metrics: Option<ExecutorDebugDeltaMetrics>,
}

#[derive(Debug, Serialize)]
struct AdapterMetrics {
    ingress_driver: String,
    adapter_id: String,
    topic_name: String,
    topic_partitions: i32,
    processed_count: u64,
    failed_count: u64,
    final_lag_records: i64,
    max_lag_records: i64,
    ownership_handoff_count: u64,
    last_takeover_latency_ms: Option<u64>,
    last_handoff_at: Option<DateTime<Utc>>,
    owner_id: Option<String>,
    owner_epoch: Option<u64>,
}

#[derive(Debug, Serialize, Default)]
struct BatchRoutingMetrics {
    backend_counts: BTreeMap<String, u64>,
    routing_reason_counts: BTreeMap<String, u64>,
    admission_policy_version_counts: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FailoverInjectionMetrics {
    status: String,
    delay_ms: u64,
    stop_requested_at_ms: Option<u64>,
    stop_completed_at_ms: Option<u64>,
    restart_started_at_ms: Option<u64>,
    restart_ready_at_ms: Option<u64>,
    downtime_ms: Option<u64>,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct BenchmarkSuiteReport {
    suite: String,
    profile: String,
    generated_at: DateTime<Utc>,
    scenarios: Vec<BenchmarkReport>,
}

#[derive(Debug, Serialize)]
struct WorkflowOutcomeMetrics {
    completed: u64,
    failed: u64,
    cancelled: u64,
    running: u64,
}

#[derive(Debug, Serialize)]
struct ActivityMetrics {
    completed: u64,
    failed: u64,
    cancelled: u64,
    timed_out: u64,
    avg_schedule_to_start_latency_ms: f64,
    max_schedule_to_start_latency_ms: u64,
    avg_start_to_close_latency_ms: f64,
    max_start_to_close_latency_ms: u64,
    throughput_activities_per_second: f64,
}

#[derive(Debug, Serialize)]
struct CoalescingMetrics {
    workflow_task_rows: u64,
    resume_rows: u64,
    resume_events_per_task_row: f64,
}

#[derive(Debug, Serialize)]
struct BacklogMetrics {
    final_workflow_backlog: u64,
    final_activity_backlog: u64,
    max_workflow_backlog: u64,
    max_activity_backlog: u64,
}

#[derive(Debug, Serialize)]
struct ControlPlaneMetrics {
    avg_tasks_per_bulk_poll_response: f64,
    avg_results_per_bulk_report_rpc: f64,
    changelog_entries_per_completed_chunk: f64,
    projection_events_per_completed_chunk: f64,
    report_batches_applied: u64,
    avg_report_batch_size: f64,
    projection_events_published: u64,
    projection_events_skipped: u64,
    projection_events_applied_directly: u64,
    changelog_entries_published: u64,
    manifest_writes: u64,
}

#[derive(Debug, Serialize)]
struct ExecutorDebugDeltaMetrics {
    polls_per_leased_task: f64,
    report_rpcs_per_completed_activity: f64,
    report_batches_per_completed_activity: f64,
    log_writes: u64,
    snapshot_writes: u64,
}

#[derive(Debug, sqlx::FromRow)]
struct WorkflowOutcomeRow {
    completed: i64,
    failed: i64,
    cancelled: i64,
    running: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct ActivityMetricRow {
    completed: i64,
    failed: i64,
    cancelled: i64,
    timed_out: i64,
    avg_schedule_to_start_latency_ms: Option<f64>,
    max_schedule_to_start_latency_ms: Option<f64>,
    avg_start_to_close_latency_ms: Option<f64>,
    max_start_to_close_latency_ms: Option<f64>,
}

#[derive(Debug, sqlx::FromRow)]
struct CoalescingRow {
    workflow_task_rows: i64,
    resume_rows: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct StreamProjectionConvergenceRow {
    projection_batch_rows: i64,
    terminal_batch_rows: i64,
    inferred_terminal_batches: i64,
    batch_accounted_items: i64,
    interrupted_batch_missing_items: i64,
    completed_items: i64,
    failed_items: i64,
    cancelled_items: i64,
    terminal_chunk_rows: i64,
    pending_chunks: i64,
}

fn stream_projection_accounted_items(row: &StreamProjectionConvergenceRow) -> i64 {
    let chunk_accounted_items = row.completed_items + row.failed_items + row.cancelled_items;
    chunk_accounted_items.max(row.batch_accounted_items + row.interrupted_batch_missing_items)
}

fn stream_projection_has_converged(
    row: &StreamProjectionConvergenceRow,
    expected_batches: u64,
    expected_items: u64,
) -> bool {
    row.projection_batch_rows as u64 >= expected_batches
        && row.terminal_batch_rows as u64 >= expected_batches
        && stream_projection_accounted_items(row) as u64 >= expected_items
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    if let Some(suite_name) = args.suite_name.clone() {
        warn_suite_selection(&args, &suite_name);
        run_suite(args, suite_name).await?;
        return Ok(());
    }
    let report = run_benchmark(&args).await?;
    write_report(&args.output_path, &report)?;
    println!("{}", summary_text(&report));
    println!("report_path={}", args.output_path.display());
    Ok(())
}

fn warn_suite_selection(args: &Args, suite_name: &str) {
    if suite_name != "streaming" {
        return;
    }
    if args.execution_mode != ExecutionMode::Durable || args.throughput_backend.is_some() {
        eprintln!(
            "warning: --suite streaming ignores single-scenario selection and runs durable, throughput-pg-v1, and throughput-stream-v2 scenarios"
        );
    }
}

async fn run_benchmark(args: &Args) -> Result<BenchmarkReport> {
    let postgres = PostgresConfig::from_env()?;
    let pool = PgPool::connect(&postgres.url).await.context("failed to connect to postgres")?;
    let client = Client::new();
    let store = WorkflowStore::connect(&postgres.url)
        .await
        .context("failed to connect benchmark workflow store")?;

    let ingest_base =
        env::var("INGEST_SERVICE_URL").unwrap_or_else(|_| "http://127.0.0.1:3001".to_owned());
    let throughput_runtime_debug_base =
        env::var("THROUGHPUT_DEBUG_URL").unwrap_or_else(|_| "http://127.0.0.1:3006".to_owned());
    let throughput_projector_base =
        env::var("THROUGHPUT_PROJECTOR_URL").unwrap_or_else(|_| "http://127.0.0.1:3007".to_owned());
    let unified_runtime_debug_base = env::var("UNIFIED_RUNTIME_DEBUG_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:3008".to_owned());

    if args.ingress_driver != BenchmarkIngressDriver::HttpTrigger {
        return run_topic_adapter_benchmark(
            args,
            &pool,
            &store,
            &client,
            &ingest_base,
            &throughput_runtime_debug_base,
            &throughput_projector_base,
            &unified_runtime_debug_base,
        )
        .await;
    }

    let scenario_name = scenario_name(args);
    let definition_id = benchmark_definition_id(args, &scenario_name);
    let instance_prefix =
        format!("fanout-{}-{}-{}", args.profile_name, scenario_name, Uuid::now_v7());
    let started_at = Utc::now();
    let started = Instant::now();
    let executor_debug_url = format!("{unified_runtime_debug_base}/debug/unified");
    let executor_debug_before = Some(fetch_optional_debug(&client, &executor_debug_url).await);
    let throughput_runtime_debug_before = if args.execution_mode == ExecutionMode::Throughput
        && args.throughput_backend.as_deref() == Some(STREAM_V2_BACKEND)
    {
        Some(
            fetch_optional_debug(
                &client,
                &format!("{throughput_runtime_debug_base}/debug/throughput"),
            )
            .await,
        )
    } else {
        None
    };
    let throughput_projector_debug_before = if args.execution_mode == ExecutionMode::Throughput
        && args.throughput_backend.as_deref() == Some(STREAM_V2_BACKEND)
    {
        Some(
            fetch_optional_debug(
                &client,
                &format!("{throughput_projector_base}/debug/throughput-projector"),
            )
            .await,
        )
    } else {
        None
    };

    if args.execution_mode == ExecutionMode::Throughput {
        apply_task_queue_throughput_policy(
            &pool,
            &args.tenant_id,
            &args.task_queue,
            args.throughput_backend.as_deref(),
        )
        .await?;
    }

    for artifact in benchmark_artifacts(&definition_id, &args.task_queue, args) {
        publish_artifact(&client, &ingest_base, &args.tenant_id, &artifact).await?;
    }

    for workflow_index in 0..args.profile.workflow_count {
        let instance_id = format!("{instance_prefix}-{workflow_index:04}");
        let input = benchmark_input(
            args,
            args.profile.activities_per_workflow,
            args.payload_size,
            args.retry_rate,
            args.cancel_rate,
            &instance_id,
        );
        trigger_workflow(
            &client,
            &ingest_base,
            &definition_id,
            &args.tenant_id,
            &instance_id,
            &args.task_queue,
            input,
        )
        .await?;
    }

    match args.workload_kind {
        BenchmarkWorkloadKind::SignalGate => {
            for workflow_index in 0..args.profile.workflow_count {
                let instance_id = format!("{instance_prefix}-{workflow_index:04}");
                signal_workflow(&client, &ingest_base, &args.tenant_id, &instance_id).await?;
            }
        }
        BenchmarkWorkloadKind::UpdateGate => {
            for workflow_index in 0..args.profile.workflow_count {
                let instance_id = format!("{instance_prefix}-{workflow_index:04}");
                update_workflow(&client, &ingest_base, &args.tenant_id, &instance_id).await?;
            }
        }
        _ => {}
    }

    let mut max_workflow_backlog = 0_u64;
    let mut max_activity_backlog = 0_u64;
    loop {
        let outcomes =
            workflow_outcomes(&pool, &args.tenant_id, &instance_prefix, args.workload_kind).await?;
        let (workflow_backlog, activity_backlog) = backlog_snapshot(
            &pool,
            &args.tenant_id,
            &instance_prefix,
            args.workload_kind,
            args.execution_mode,
            args.throughput_backend.as_deref(),
        )
        .await?;
        max_workflow_backlog = max_workflow_backlog.max(workflow_backlog);
        max_activity_backlog = max_activity_backlog.max(activity_backlog);

        if outcomes.completed + outcomes.failed + outcomes.cancelled
            == args.profile.workflow_count as u64
        {
            break;
        }
        if started.elapsed() >= args.timeout {
            bail!(
                "timed out waiting for benchmark completion: completed={} failed={} cancelled={} expected={}",
                outcomes.completed,
                outcomes.failed,
                outcomes.cancelled,
                args.profile.workflow_count
            );
        }
        tokio::time::sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)).await;
    }

    let execution_completed_at = Utc::now();
    let execution_duration_ms = started.elapsed().as_millis();

    if args.execution_mode == ExecutionMode::Throughput
        && args.throughput_backend.as_deref() == Some(STREAM_V2_BACKEND)
    {
        wait_for_stream_projection_convergence(
            &pool,
            &args.tenant_id,
            &instance_prefix,
            args.workload_kind,
            args.profile.workflow_count as u64,
            (args.profile.workflow_count * args.profile.activities_per_workflow) as u64,
            Duration::from_secs(DEFAULT_STREAM_PROJECTION_TIMEOUT_SECS),
        )
        .await?;
    }

    let completed_at = Utc::now();
    let duration_ms = started.elapsed().as_millis();
    let projection_convergence_duration_ms = duration_ms.saturating_sub(execution_duration_ms);
    let workflow_outcomes =
        workflow_outcomes(&pool, &args.tenant_id, &instance_prefix, args.workload_kind).await?;
    let activity_metrics = activity_metrics(
        &pool,
        &args.tenant_id,
        &instance_prefix,
        args.workload_kind,
        duration_ms,
        args.profile.workflow_count * args.profile.activities_per_workflow,
        args.execution_mode,
        args.throughput_backend.as_deref(),
    )
    .await?;
    let coalescing_metrics =
        coalescing_metrics(&pool, &args.tenant_id, &instance_prefix, args.workload_kind).await?;
    let (_legacy_batch_rows, _legacy_chunk_rows) = (0_u64, 0_u64);
    let batch_routing_metrics =
        batch_routing_metrics(&pool, &args.tenant_id, &instance_prefix, args.workload_kind).await?;
    let (final_workflow_backlog, final_activity_backlog) = backlog_snapshot(
        &pool,
        &args.tenant_id,
        &instance_prefix,
        args.workload_kind,
        args.execution_mode,
        args.throughput_backend.as_deref(),
    )
    .await?;
    let executor_debug = client
        .get(&executor_debug_url)
        .send()
        .await
        .context("failed to fetch benchmark control-plane debug summary")?
        .error_for_status()
        .context("benchmark control-plane debug endpoint returned error")?
        .json::<Value>()
        .await
        .context("failed to decode benchmark control-plane debug summary")?;
    let executor_debug_after = Some(executor_debug.clone());
    let executor_debug_delta = executor_debug_before
        .as_ref()
        .zip(executor_debug_after.as_ref())
        .map(|(before, after)| json_numeric_delta(before, after));
    let throughput_runtime_debug_after = if args.execution_mode == ExecutionMode::Throughput
        && args.throughput_backend.as_deref() == Some(STREAM_V2_BACKEND)
    {
        Some(
            fetch_optional_debug(
                &client,
                &format!("{throughput_runtime_debug_base}/debug/throughput"),
            )
            .await,
        )
    } else {
        None
    };
    let throughput_runtime_debug_delta = throughput_runtime_debug_before
        .as_ref()
        .zip(throughput_runtime_debug_after.as_ref())
        .map(|(before, after)| json_numeric_delta(before, after));
    let throughput_projector_debug_after = if args.execution_mode == ExecutionMode::Throughput
        && args.throughput_backend.as_deref() == Some(STREAM_V2_BACKEND)
    {
        Some(
            fetch_optional_debug(
                &client,
                &format!("{throughput_projector_base}/debug/throughput-projector"),
            )
            .await,
        )
    } else {
        None
    };
    let throughput_projector_debug_delta = throughput_projector_debug_before
        .as_ref()
        .zip(throughput_projector_debug_after.as_ref())
        .map(|(before, after)| json_numeric_delta(before, after));
    let (
        bulk_batch_rows,
        bulk_chunk_rows,
        projection_batch_rows,
        projection_chunk_rows,
        max_aggregation_group_count,
        grouped_batch_rows,
    ) = bulk_metrics(&pool, &args.tenant_id, &instance_prefix, args.workload_kind).await?;
    let control_plane_metrics = control_plane_metrics(
        throughput_runtime_debug_delta.as_ref(),
        throughput_projector_debug_delta.as_ref(),
        &activity_metrics,
    );
    let executor_debug_delta_metrics =
        executor_debug_delta_metrics(executor_debug_delta.as_ref(), &activity_metrics);
    let failover_injection = load_failover_injection_metrics()?;

    Ok(BenchmarkReport {
        scenario: scenario_name,
        profile: args.profile_name.clone(),
        started_at,
        execution_completed_at,
        completed_at,
        execution_duration_ms,
        projection_convergence_duration_ms,
        duration_ms,
        workflow_count: args.profile.workflow_count,
        activities_per_workflow: args.profile.activities_per_workflow,
        total_activities: args.profile.workflow_count * args.profile.activities_per_workflow,
        worker_count: args.worker_count,
        payload_size: args.payload_size,
        retry_rate: args.retry_rate,
        cancel_rate: args.cancel_rate,
        retry_delay_ms: args.retry_delay_ms,
        definition_id,
        task_queue: args.task_queue.clone(),
        execution_mode: args.execution_mode,
        throughput_backend: args.throughput_backend.clone(),
        bulk_reducer: args.bulk_reducer.clone(),
        chunk_size: args.chunk_size,
        instance_prefix,
        workflow_outcomes,
        activity_metrics,
        coalescing_metrics,
        backlog_metrics: BacklogMetrics {
            final_workflow_backlog,
            final_activity_backlog,
            max_workflow_backlog,
            max_activity_backlog,
        },
        bulk_batch_rows,
        bulk_chunk_rows,
        projection_batch_rows,
        projection_chunk_rows,
        max_aggregation_group_count,
        grouped_batch_rows,
        executor_debug,
        executor_debug_before,
        executor_debug_after,
        executor_debug_delta,
        throughput_runtime_debug_before,
        throughput_runtime_debug: throughput_runtime_debug_after,
        throughput_runtime_debug_delta,
        throughput_projector_debug_before,
        throughput_projector_debug: throughput_projector_debug_after,
        throughput_projector_debug_delta,
        batch_routing_metrics,
        failover_injection,
        control_plane_metrics,
        executor_debug_delta_metrics,
        adapter_metrics: None,
    })
}

async fn run_topic_adapter_benchmark(
    args: &Args,
    pool: &PgPool,
    store: &WorkflowStore,
    client: &Client,
    ingest_base: &str,
    throughput_runtime_debug_base: &str,
    throughput_projector_base: &str,
    unified_runtime_debug_base: &str,
) -> Result<BenchmarkReport> {
    let redpanda_brokers = env::var("REDPANDA_BROKERS")
        .context("topic adapter benchmark requires REDPANDA_BROKERS in the environment")?;
    let scenario_name = scenario_name(args);
    let definition_id = benchmark_definition_id(args, &scenario_name);
    let instance_prefix =
        format!("fanout-{}-{}-{}", args.profile_name, scenario_name, Uuid::now_v7());

    if args.execution_mode == ExecutionMode::Throughput {
        apply_task_queue_throughput_policy(
            pool,
            &args.tenant_id,
            &args.task_queue,
            args.throughput_backend.as_deref(),
        )
        .await?;
    }

    for artifact in benchmark_artifacts(&definition_id, &args.task_queue, args) {
        publish_artifact(client, ingest_base, &args.tenant_id, &artifact).await?;
    }

    if args.ingress_driver == BenchmarkIngressDriver::TopicAdapterSignal {
        for workflow_index in 0..args.profile.workflow_count {
            let instance_id = format!("{instance_prefix}-{workflow_index:04}");
            let input = benchmark_input(
                args,
                args.profile.activities_per_workflow,
                args.payload_size,
                args.retry_rate,
                args.cancel_rate,
                &instance_id,
            );
            trigger_workflow(
                client,
                ingest_base,
                &definition_id,
                &args.tenant_id,
                &instance_id,
                &args.task_queue,
                input,
            )
            .await?;
        }
        wait_for_signal_gate_ready(
            pool,
            &args.tenant_id,
            &instance_prefix,
            args.profile.workflow_count as u64,
            Duration::from_secs(DEFAULT_ADAPTER_READY_TIMEOUT_SECS),
        )
        .await?;
    }

    let adapter = upsert_benchmark_topic_adapter(
        store,
        &redpanda_brokers,
        &args.tenant_id,
        &args.task_queue,
        &definition_id,
        &scenario_name,
        args,
    )
    .await?;
    let topic_config = JsonTopicConfig::new(
        adapter.brokers.clone(),
        adapter.topic_name.clone(),
        adapter.topic_partitions,
    );
    let topic_publisher = JsonTopicPublisher::<Value>::new(
        &topic_config,
        &format!("benchmark-runner-topic-{}", Uuid::now_v7()),
    )
    .await
    .context("failed to create topic adapter benchmark publisher")?;
    wait_for_topic_adapter_ready(
        store,
        &args.tenant_id,
        &adapter.adapter_id,
        Duration::from_secs(DEFAULT_ADAPTER_READY_TIMEOUT_SECS),
    )
    .await?;
    let initial_ownership = store
        .get_topic_adapter_ownership(&args.tenant_id, &adapter.adapter_id)
        .await
        .context("failed to load initial benchmark topic adapter ownership")?
        .context("benchmark topic adapter missing ownership after readiness")?;
    write_failover_arm_file_if_configured(&initial_ownership.owner_id)?;

    let started_at = Utc::now();
    let started = Instant::now();
    let executor_debug_url = format!("{unified_runtime_debug_base}/debug/unified");
    let executor_debug_before = Some(fetch_optional_debug(client, &executor_debug_url).await);
    let throughput_runtime_debug_before = if args.execution_mode == ExecutionMode::Throughput
        && args.throughput_backend.as_deref() == Some(STREAM_V2_BACKEND)
    {
        Some(
            fetch_optional_debug(
                client,
                &format!("{throughput_runtime_debug_base}/debug/throughput"),
            )
            .await,
        )
    } else {
        None
    };
    let throughput_projector_debug_before = if args.execution_mode == ExecutionMode::Throughput
        && args.throughput_backend.as_deref() == Some(STREAM_V2_BACKEND)
    {
        Some(
            fetch_optional_debug(
                client,
                &format!("{throughput_projector_base}/debug/throughput-projector"),
            )
            .await,
        )
    } else {
        None
    };

    publish_topic_adapter_inputs(
        &topic_publisher,
        args,
        &instance_prefix,
        &definition_id,
        started_at,
    )
    .await?;

    let mut max_workflow_backlog = 0_u64;
    let mut max_activity_backlog = 0_u64;
    let mut max_adapter_lag_records = 0_i64;
    loop {
        let outcomes =
            workflow_outcomes(pool, &args.tenant_id, &instance_prefix, args.workload_kind).await?;
        let (workflow_backlog, activity_backlog) = backlog_snapshot(
            pool,
            &args.tenant_id,
            &instance_prefix,
            args.workload_kind,
            args.execution_mode,
            args.throughput_backend.as_deref(),
        )
        .await?;
        max_workflow_backlog = max_workflow_backlog.max(workflow_backlog);
        max_activity_backlog = max_activity_backlog.max(activity_backlog);
        let adapter_lag_records =
            current_topic_adapter_lag_records(store, &adapter, &topic_config).await?;
        max_adapter_lag_records = max_adapter_lag_records.max(adapter_lag_records);

        if outcomes.completed + outcomes.failed + outcomes.cancelled
            == args.profile.workflow_count as u64
        {
            break;
        }
        if started.elapsed() >= args.timeout {
            bail!(
                "timed out waiting for topic adapter benchmark completion: completed={} failed={} cancelled={} expected={}",
                outcomes.completed,
                outcomes.failed,
                outcomes.cancelled,
                args.profile.workflow_count
            );
        }
        tokio::time::sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)).await;
    }

    let execution_completed_at = Utc::now();
    let execution_duration_ms = started.elapsed().as_millis();

    if args.execution_mode == ExecutionMode::Throughput
        && args.throughput_backend.as_deref() == Some(STREAM_V2_BACKEND)
    {
        wait_for_stream_projection_convergence(
            pool,
            &args.tenant_id,
            &instance_prefix,
            args.workload_kind,
            args.profile.workflow_count as u64,
            (args.profile.workflow_count * args.profile.activities_per_workflow) as u64,
            Duration::from_secs(DEFAULT_STREAM_PROJECTION_TIMEOUT_SECS),
        )
        .await?;
    }
    if topic_adapter_failover_expected(args) {
        wait_for_topic_adapter_failover_observed(
            store,
            &args.tenant_id,
            &adapter.adapter_id,
            &initial_ownership.owner_id,
            initial_ownership.owner_epoch,
            Duration::from_secs(DEFAULT_ADAPTER_FAILOVER_OBSERVATION_TIMEOUT_SECS),
        )
        .await?;
    }

    let completed_at = Utc::now();
    let duration_ms = started.elapsed().as_millis();
    let projection_convergence_duration_ms = duration_ms.saturating_sub(execution_duration_ms);
    let workflow_outcomes =
        workflow_outcomes(pool, &args.tenant_id, &instance_prefix, args.workload_kind).await?;
    let activity_metrics = activity_metrics(
        pool,
        &args.tenant_id,
        &instance_prefix,
        args.workload_kind,
        duration_ms,
        args.profile.workflow_count * args.profile.activities_per_workflow,
        args.execution_mode,
        args.throughput_backend.as_deref(),
    )
    .await?;
    let coalescing_metrics =
        coalescing_metrics(pool, &args.tenant_id, &instance_prefix, args.workload_kind).await?;
    let batch_routing_metrics =
        batch_routing_metrics(pool, &args.tenant_id, &instance_prefix, args.workload_kind).await?;
    let (final_workflow_backlog, final_activity_backlog) = backlog_snapshot(
        pool,
        &args.tenant_id,
        &instance_prefix,
        args.workload_kind,
        args.execution_mode,
        args.throughput_backend.as_deref(),
    )
    .await?;
    let executor_debug = client
        .get(&executor_debug_url)
        .send()
        .await
        .context("failed to fetch benchmark control-plane debug summary")?
        .error_for_status()
        .context("benchmark control-plane debug endpoint returned error")?
        .json::<Value>()
        .await
        .context("failed to decode benchmark control-plane debug summary")?;
    let executor_debug_after = Some(executor_debug.clone());
    let executor_debug_delta = executor_debug_before
        .as_ref()
        .zip(executor_debug_after.as_ref())
        .map(|(before, after)| json_numeric_delta(before, after));
    let throughput_runtime_debug_after = if args.execution_mode == ExecutionMode::Throughput
        && args.throughput_backend.as_deref() == Some(STREAM_V2_BACKEND)
    {
        Some(
            fetch_optional_debug(
                client,
                &format!("{throughput_runtime_debug_base}/debug/throughput"),
            )
            .await,
        )
    } else {
        None
    };
    let throughput_runtime_debug_delta = throughput_runtime_debug_before
        .as_ref()
        .zip(throughput_runtime_debug_after.as_ref())
        .map(|(before, after)| json_numeric_delta(before, after));
    let throughput_projector_debug_after = if args.execution_mode == ExecutionMode::Throughput
        && args.throughput_backend.as_deref() == Some(STREAM_V2_BACKEND)
    {
        Some(
            fetch_optional_debug(
                client,
                &format!("{throughput_projector_base}/debug/throughput-projector"),
            )
            .await,
        )
    } else {
        None
    };
    let throughput_projector_debug_delta = throughput_projector_debug_before
        .as_ref()
        .zip(throughput_projector_debug_after.as_ref())
        .map(|(before, after)| json_numeric_delta(before, after));
    let (
        bulk_batch_rows,
        bulk_chunk_rows,
        projection_batch_rows,
        projection_chunk_rows,
        max_aggregation_group_count,
        grouped_batch_rows,
    ) = bulk_metrics(pool, &args.tenant_id, &instance_prefix, args.workload_kind).await?;
    let control_plane_metrics = control_plane_metrics(
        throughput_runtime_debug_delta.as_ref(),
        throughput_projector_debug_delta.as_ref(),
        &activity_metrics,
    );
    let executor_debug_delta_metrics =
        executor_debug_delta_metrics(executor_debug_delta.as_ref(), &activity_metrics);
    let failover_injection = load_failover_injection_metrics()?;
    let adapter_metrics =
        Some(load_topic_adapter_metrics(store, &adapter, &topic_config, max_adapter_lag_records).await?);

    Ok(BenchmarkReport {
        scenario: scenario_name,
        profile: args.profile_name.clone(),
        started_at,
        execution_completed_at,
        completed_at,
        execution_duration_ms,
        projection_convergence_duration_ms,
        duration_ms,
        workflow_count: args.profile.workflow_count,
        activities_per_workflow: args.profile.activities_per_workflow,
        total_activities: args.profile.workflow_count * args.profile.activities_per_workflow,
        worker_count: args.worker_count,
        payload_size: args.payload_size,
        retry_rate: args.retry_rate,
        cancel_rate: args.cancel_rate,
        retry_delay_ms: args.retry_delay_ms,
        definition_id,
        task_queue: args.task_queue.clone(),
        execution_mode: args.execution_mode,
        throughput_backend: args.throughput_backend.clone(),
        bulk_reducer: args.bulk_reducer.clone(),
        chunk_size: args.chunk_size,
        instance_prefix,
        workflow_outcomes,
        activity_metrics,
        coalescing_metrics,
        backlog_metrics: BacklogMetrics {
            final_workflow_backlog,
            final_activity_backlog,
            max_workflow_backlog,
            max_activity_backlog,
        },
        bulk_batch_rows,
        bulk_chunk_rows,
        projection_batch_rows,
        projection_chunk_rows,
        max_aggregation_group_count,
        grouped_batch_rows,
        executor_debug,
        executor_debug_before,
        executor_debug_after,
        executor_debug_delta,
        throughput_runtime_debug_before,
        throughput_runtime_debug: throughput_runtime_debug_after,
        throughput_runtime_debug_delta,
        throughput_projector_debug_before,
        throughput_projector_debug: throughput_projector_debug_after,
        throughput_projector_debug_delta,
        batch_routing_metrics,
        failover_injection,
        adapter_metrics,
        control_plane_metrics,
        executor_debug_delta_metrics,
    })
}

async fn wait_for_signal_gate_ready(
    pool: &PgPool,
    tenant_id: &str,
    instance_prefix: &str,
    expected_count: u64,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let ready = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM workflow_instances
            WHERE tenant_id = $1
              AND workflow_instance_id LIKE $2
              AND state->>'current_state' = 'wait_signal'
            "#,
        )
        .bind(tenant_id)
        .bind(format!("{instance_prefix}%"))
        .fetch_one(pool)
        .await
        .context("failed to count signal-gate workflows waiting for signal")?;
        if ready as u64 >= expected_count {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for signal-gate workflows to reach wait_signal: ready={} expected={}",
                ready,
                expected_count
            );
        }
        tokio::time::sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)).await;
    }
}

async fn upsert_benchmark_topic_adapter(
    store: &WorkflowStore,
    brokers: &str,
    tenant_id: &str,
    task_queue: &str,
    definition_id: &str,
    scenario_name: &str,
    args: &Args,
) -> Result<TopicAdapterRecord> {
    let adapter_id = format!("adapter-{}-{}", scenario_name, Uuid::now_v7());
    let topic_name = format!("benchmark.{}.{}", tenant_id.replace('_', "-"), adapter_id);
    let action = match args.ingress_driver {
        BenchmarkIngressDriver::TopicAdapterStart => TopicAdapterAction::StartWorkflow,
        BenchmarkIngressDriver::TopicAdapterSignal => TopicAdapterAction::SignalWorkflow,
        BenchmarkIngressDriver::HttpTrigger => bail!("http trigger benchmark cannot upsert topic adapter"),
    };
    store
        .upsert_topic_adapter(&TopicAdapterUpsert {
            tenant_id: tenant_id.to_owned(),
            adapter_id,
            adapter_kind: TopicAdapterKind::Redpanda,
            brokers: brokers.to_owned(),
            topic_name,
            topic_partitions: 1,
            action,
            definition_id: (args.ingress_driver == BenchmarkIngressDriver::TopicAdapterStart)
                .then(|| definition_id.to_owned()),
            signal_type: (args.ingress_driver == BenchmarkIngressDriver::TopicAdapterSignal)
                .then(|| "approve".to_owned()),
            workflow_task_queue: Some(task_queue.to_owned()),
            workflow_instance_id_json_pointer: Some("/instance_id".to_owned()),
            payload_json_pointer: Some("/payload".to_owned()),
            payload_template_json: None,
            memo_json_pointer: None,
            memo_template_json: None,
            search_attributes_json_pointer: None,
            search_attributes_template_json: None,
            request_id_json_pointer: Some("/request_id".to_owned()),
            dedupe_key_json_pointer: None,
            dead_letter_policy: TopicAdapterDeadLetterPolicy::Store,
            is_paused: false,
        })
        .await
        .context("failed to upsert benchmark topic adapter")
}

async fn wait_for_topic_adapter_ready(
    store: &WorkflowStore,
    tenant_id: &str,
    adapter_id: &str,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(ownership) = store
            .get_topic_adapter_ownership(tenant_id, adapter_id)
            .await
            .context("failed to load topic adapter ownership during readiness check")?
        {
            if ownership.is_active_at(Utc::now()) {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for topic adapter {tenant_id}/{adapter_id} to become owned");
        }
        tokio::time::sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)).await;
    }
}

async fn publish_topic_adapter_inputs(
    publisher: &JsonTopicPublisher<Value>,
    args: &Args,
    instance_prefix: &str,
    definition_id: &str,
    started_at: DateTime<Utc>,
) -> Result<()> {
    for workflow_index in 0..args.profile.workflow_count {
        let instance_id = format!("{instance_prefix}-{workflow_index:04}");
        let payload = match args.ingress_driver {
            BenchmarkIngressDriver::TopicAdapterStart => json!({
                "instance_id": instance_id,
                "request_id": format!("adapter-start:{definition_id}:{workflow_index}"),
                "payload": benchmark_input(
                    args,
                    args.profile.activities_per_workflow,
                    args.payload_size,
                    args.retry_rate,
                    args.cancel_rate,
                    &instance_id,
                ),
                "published_at": started_at,
            }),
            BenchmarkIngressDriver::TopicAdapterSignal => json!({
                "instance_id": instance_id,
                "request_id": format!("adapter-signal:{definition_id}:{workflow_index}"),
                "payload": { "approved": true },
                "published_at": started_at,
            }),
            BenchmarkIngressDriver::HttpTrigger => bail!("http trigger benchmark cannot publish topic adapter input"),
        };
        publisher
            .publish(&payload, &instance_id)
            .await
            .with_context(|| format!("failed to publish benchmark adapter input for {instance_id}"))?;
    }
    Ok(())
}

async fn current_topic_adapter_lag_records(
    store: &WorkflowStore,
    adapter: &TopicAdapterRecord,
    topic_config: &JsonTopicConfig,
) -> Result<i64> {
    let offsets = store
        .load_topic_adapter_offsets(&adapter.tenant_id, &adapter.adapter_id)
        .await
        .context("failed to load topic adapter offsets for lag check")?;
    let latest_offsets = load_json_topic_latest_offsets(
        topic_config,
        &format!("benchmark-runner-adapter-lag-{}", adapter.adapter_id),
    )
    .await
    .context("failed to load latest topic adapter offsets")?;
    Ok(topic_adapter_total_lag_records(&offsets, &latest_offsets))
}

fn topic_adapter_total_lag_records(
    offsets: &[TopicAdapterOffsetRecord],
    latest_offsets: &[fabrik_broker::TopicPartitionOffset],
) -> i64 {
    let committed_by_partition =
        offsets.iter().map(|offset| (offset.partition_id, offset.log_offset)).collect::<BTreeMap<_, _>>();
    latest_offsets
        .iter()
        .map(|offset| match committed_by_partition.get(&offset.partition_id).copied() {
            Some(committed) => (offset.latest_offset - committed).max(0),
            None if offset.latest_offset >= 0 => offset.latest_offset.saturating_add(1),
            None => 0,
        })
        .sum()
}

async fn load_topic_adapter_metrics(
    store: &WorkflowStore,
    adapter: &TopicAdapterRecord,
    topic_config: &JsonTopicConfig,
    max_lag_records: i64,
) -> Result<AdapterMetrics> {
    let adapter = store
        .get_topic_adapter(&adapter.tenant_id, &adapter.adapter_id)
        .await
        .context("failed to load final benchmark topic adapter state")?
        .context("benchmark topic adapter missing at completion")?;
    let ownership = store
        .get_topic_adapter_ownership(&adapter.tenant_id, &adapter.adapter_id)
        .await
        .context("failed to load final benchmark topic adapter ownership")?;
    let offsets = store
        .load_topic_adapter_offsets(&adapter.tenant_id, &adapter.adapter_id)
        .await
        .context("failed to load final benchmark topic adapter offsets")?;
    let latest_offsets = load_json_topic_latest_offsets(
        topic_config,
        &format!("benchmark-runner-adapter-final-{}", adapter.adapter_id),
    )
    .await
    .context("failed to load final benchmark topic latest offsets")?;
    Ok(AdapterMetrics {
        ingress_driver: match adapter.action {
            TopicAdapterAction::StartWorkflow => "topic_adapter_start".to_owned(),
            TopicAdapterAction::SignalWorkflow => "topic_adapter_signal".to_owned(),
        },
        adapter_id: adapter.adapter_id.clone(),
        topic_name: adapter.topic_name.clone(),
        topic_partitions: adapter.topic_partitions,
        processed_count: adapter.processed_count,
        failed_count: adapter.failed_count,
        final_lag_records: topic_adapter_total_lag_records(&offsets, &latest_offsets),
        max_lag_records,
        ownership_handoff_count: adapter.ownership_handoff_count,
        last_takeover_latency_ms: adapter.last_takeover_latency_ms,
        last_handoff_at: adapter.last_handoff_at,
        owner_id: ownership.as_ref().map(|record| record.owner_id.clone()),
        owner_epoch: ownership.as_ref().map(|record| record.owner_epoch),
    })
}

fn write_failover_arm_file_if_configured(owner_id: &str) -> Result<()> {
    let path = match env::var("BENCHMARK_FAILOVER_ARM_PATH") {
        Ok(value) if !value.is_empty() => PathBuf::from(value),
        _ => return Ok(()),
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create benchmark failover arm directory {}",
                parent.display()
            )
        })?;
    }
    let payload = json!({
        "owner_id": owner_id,
    });
    fs::write(&path, serde_json::to_vec(&payload).context("failed to encode benchmark failover arm payload")?)
        .with_context(|| {
        format!(
            "failed to write benchmark failover arm file {}",
            path.display()
        )
    })?;
    Ok(())
}

fn topic_adapter_failover_expected(args: &Args) -> bool {
    args.scenario_tag
        .as_deref()
        .is_some_and(|tag| tag.contains("owner-crash"))
        && env::var("BENCHMARK_FAILOVER_INJECTION_PATH")
            .map(|value| !value.is_empty())
            .unwrap_or(false)
}

async fn wait_for_topic_adapter_failover_observed(
    store: &WorkflowStore,
    tenant_id: &str,
    adapter_id: &str,
    initial_owner_id: &str,
    initial_owner_epoch: u64,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let adapter = store
            .get_topic_adapter(tenant_id, adapter_id)
            .await
            .context("failed to load topic adapter while waiting for failover observation")?
            .context("topic adapter missing while waiting for failover observation")?;
        let ownership = store
            .get_topic_adapter_ownership(tenant_id, adapter_id)
            .await
            .context("failed to load topic adapter ownership while waiting for failover observation")?;
        if adapter.ownership_handoff_count > 0
            || ownership
                .as_ref()
                .is_some_and(|record| {
                    record.owner_epoch > initial_owner_epoch || record.owner_id != initial_owner_id
                })
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for topic adapter {tenant_id}/{adapter_id} to observe failover from owner {initial_owner_id} epoch {initial_owner_epoch}"
            );
        }
        tokio::time::sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)).await;
    }
}

fn parse_args() -> Result<Args> {
    let mut suite_name = None;
    let mut scenario_tag = None;
    let mut profile_name = "smoke".to_owned();
    let mut output_path = None;
    let mut worker_count = 1_usize;
    let mut payload_size = 128_usize;
    let mut retry_rate = 0.0_f64;
    let mut cancel_rate = 0.0_f64;
    let mut retry_delay_ms = 1_000_u64;
    let mut tenant_id = "benchmark".to_owned();
    let mut task_queue = "default".to_owned();
    let mut execution_mode = ExecutionMode::Durable;
    let mut throughput_backend = None;
    let ingress_driver = BenchmarkIngressDriver::HttpTrigger;
    let mut workload_kind = BenchmarkWorkloadKind::Fanout;
    let mut bulk_reducer = "collect_results".to_owned();
    let mut bulk_reducer_explicit = false;
    let mut chunk_size = 256_u32;
    let mut timer_secs = 1_u32;
    let mut continue_rounds = 1_u32;
    let mut timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECS);
    let mut workflow_count = None;
    let mut activities_per_workflow = None;

    let mut args = env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next().with_context(|| format!("missing value for argument {flag}"))?;
        match flag.as_str() {
            "--suite" => suite_name = Some(value),
            "--scenario-tag" => scenario_tag = Some(value),
            "--profile" => profile_name = value,
            "--output" => output_path = Some(PathBuf::from(value)),
            "--worker-count" => worker_count = value.parse().context("invalid --worker-count")?,
            "--payload-size" => payload_size = value.parse().context("invalid --payload-size")?,
            "--retry-rate" => retry_rate = value.parse().context("invalid --retry-rate")?,
            "--cancel-rate" => cancel_rate = value.parse().context("invalid --cancel-rate")?,
            "--retry-delay-ms" => {
                retry_delay_ms = value.parse().context("invalid --retry-delay-ms")?
            }
            "--tenant-id" => tenant_id = value,
            "--task-queue" => task_queue = value,
            "--execution-mode" => {
                execution_mode = match value.as_str() {
                    "durable" => ExecutionMode::Durable,
                    "throughput" => ExecutionMode::Throughput,
                    "unified" => ExecutionMode::Unified,
                    other => {
                        bail!(
                            "unknown --execution-mode {other}; expected durable, throughput, or unified"
                        )
                    }
                }
            }
            "--throughput-backend" => {
                throughput_backend = Some(match value.as_str() {
                    PG_V1_BACKEND => PG_V1_BACKEND.to_owned(),
                    STREAM_V2_BACKEND => STREAM_V2_BACKEND.to_owned(),
                    other => bail!(
                        "unknown --throughput-backend {other}; expected {PG_V1_BACKEND} or {STREAM_V2_BACKEND}"
                    ),
                })
            }
            "--workload-kind" => workload_kind = BenchmarkWorkloadKind::parse(&value)?,
            "--bulk-reducer" => {
                bulk_reducer_explicit = true;
                bulk_reducer = match value.as_str() {
                    "all_succeeded" | "all_settled" | "count" | "collect_results" | "sum"
                    | "min" | "max" | "avg" | "histogram" | "sample_errors" => value,
                    other => bail!(
                        "unknown --bulk-reducer {other}; expected all_succeeded, all_settled, count, collect_results, sum, min, max, avg, histogram, or sample_errors"
                    ),
                }
            }
            "--chunk-size" => chunk_size = value.parse().context("invalid --chunk-size")?,
            "--timer-secs" => timer_secs = value.parse().context("invalid --timer-secs")?,
            "--continue-rounds" => {
                continue_rounds = value.parse().context("invalid --continue-rounds")?
            }
            "--timeout-secs" => {
                timeout = Duration::from_secs(value.parse().context("invalid --timeout-secs")?)
            }
            "--workflow-count" => {
                workflow_count = Some(value.parse().context("invalid --workflow-count")?)
            }
            "--activities-per-workflow" => {
                activities_per_workflow =
                    Some(value.parse().context("invalid --activities-per-workflow")?)
            }
            other => bail!("unknown argument {other}"),
        }
    }

    bulk_reducer = normalize_bulk_reducer_for_execution_mode(
        execution_mode,
        bulk_reducer,
        bulk_reducer_explicit,
    )?;

    let default_profile = match profile_name.as_str() {
        "gate" => BenchmarkProfile { workflow_count: 6, activities_per_workflow: 64 },
        "smoke" => BenchmarkProfile { workflow_count: 10, activities_per_workflow: 100 },
        "target" => BenchmarkProfile { workflow_count: 100, activities_per_workflow: 1_000 },
        "stress" => BenchmarkProfile { workflow_count: 250, activities_per_workflow: 1_000 },
        other => bail!("unknown profile {other}; expected gate, smoke, target, or stress"),
    };

    let profile = BenchmarkProfile {
        workflow_count: workflow_count.unwrap_or(default_profile.workflow_count),
        activities_per_workflow: activities_per_workflow
            .unwrap_or(default_profile.activities_per_workflow),
    };
    let output_path = output_path.unwrap_or_else(|| {
        PathBuf::from(format!("target/benchmark-reports/{}.json", profile_name))
    });

    Ok(Args {
        suite_name,
        scenario_tag,
        profile_name,
        profile,
        output_path,
        worker_count,
        payload_size,
        retry_rate,
        cancel_rate,
        retry_delay_ms,
        tenant_id,
        task_queue,
        execution_mode,
        throughput_backend,
        ingress_driver,
        workload_kind,
        activity_type: BENCHMARK_ECHO_ACTIVITY.to_owned(),
        bulk_reducer,
        chunk_size,
        timer_secs,
        continue_rounds,
        timeout,
    })
}

fn normalize_bulk_reducer_for_execution_mode(
    execution_mode: ExecutionMode,
    bulk_reducer: String,
    bulk_reducer_explicit: bool,
) -> Result<String> {
    if execution_mode != ExecutionMode::Unified {
        return Ok(bulk_reducer);
    }
    if !bulk_reducer_explicit && bulk_reducer == "collect_results" {
        return Ok("all_settled".to_owned());
    }
    if matches!(bulk_reducer.as_str(), "all_settled" | "count") {
        return Ok(bulk_reducer);
    }
    bail!(
        "unified benchmarks require --bulk-reducer all_settled or count; {bulk_reducer} is unsupported"
    )
}

fn benchmark_artifacts(
    definition_id: &str,
    task_queue: &str,
    args: &Args,
) -> Vec<CompiledWorkflowArtifact> {
    let mut artifacts = vec![benchmark_artifact(definition_id, task_queue, args)];
    if args.workload_kind == BenchmarkWorkloadKind::ChildWorkflow {
        artifacts.push(benchmark_child_artifact(&child_definition_id(definition_id)));
    }
    artifacts
}

fn benchmark_artifact(
    definition_id: &str,
    task_queue: &str,
    args: &Args,
) -> CompiledWorkflowArtifact {
    let mut states = benchmark_terminal_states();
    let initial_state = match args.workload_kind {
        BenchmarkWorkloadKind::Fanout => {
            insert_dispatch_graph(&mut states, task_queue, args);
            "dispatch".to_owned()
        }
        BenchmarkWorkloadKind::TimerGate => {
            states.insert(
                "wait_timer".to_owned(),
                CompiledStateNode::WaitForTimer {
                    timer_ref: format!("{}s", args.timer_secs.max(1)),
                    timer_expr: None,
                    next: "dispatch".to_owned(),
                },
            );
            insert_dispatch_graph(&mut states, task_queue, args);
            "wait_timer".to_owned()
        }
        BenchmarkWorkloadKind::SignalGate => {
            states.insert(
                "init".to_owned(),
                CompiledStateNode::Assign {
                    actions: vec![Assignment {
                        target: "signal_ready".to_owned(),
                        expr: Expression::Literal { value: Value::Bool(false) },
                    }],
                    next: "wait_signal".to_owned(),
                },
            );
            states.insert(
                "wait_signal".to_owned(),
                CompiledStateNode::WaitForCondition {
                    condition: Expression::Identifier { name: "signal_ready".to_owned() },
                    next: "dispatch".to_owned(),
                    timeout_ref: None,
                    timeout_next: None,
                },
            );
            insert_dispatch_graph(&mut states, task_queue, args);
            let mut artifact = CompiledWorkflowArtifact::new(
                definition_id,
                1,
                "benchmark-runner",
                ArtifactEntrypoint {
                    module: "benchmark.ts".to_owned(),
                    export: "workflow".to_owned(),
                },
                CompiledWorkflow {
                    initial_state: "init".to_owned(),
                    states,
                    params: Vec::new(),
                    non_cancellable_states: Default::default(),
                },
            );
            artifact.signals.insert(
                "approve".to_owned(),
                fabrik_workflow::CompiledSignalHandler {
                    arg_name: Some("payload".to_owned()),
                    initial_state: "mark_ready".to_owned(),
                    states: BTreeMap::from([
                        (
                            "mark_ready".to_owned(),
                            CompiledStateNode::Assign {
                                actions: vec![Assignment {
                                    target: "signal_ready".to_owned(),
                                    expr: Expression::Literal { value: Value::Bool(true) },
                                }],
                                next: "finish_signal".to_owned(),
                            },
                        ),
                        (
                            "finish_signal".to_owned(),
                            CompiledStateNode::Succeed {
                                output: Some(Expression::Identifier { name: "payload".to_owned() }),
                            },
                        ),
                    ]),
                },
            );
            artifact.artifact_hash = artifact.hash();
            return artifact;
        }
        BenchmarkWorkloadKind::UpdateGate => {
            states.insert(
                "init".to_owned(),
                CompiledStateNode::Assign {
                    actions: vec![Assignment {
                        target: "update_ready".to_owned(),
                        expr: Expression::Literal { value: Value::Bool(false) },
                    }],
                    next: "wait_update".to_owned(),
                },
            );
            states.insert(
                "wait_update".to_owned(),
                CompiledStateNode::WaitForCondition {
                    condition: Expression::Identifier { name: "update_ready".to_owned() },
                    next: "dispatch".to_owned(),
                    timeout_ref: None,
                    timeout_next: None,
                },
            );
            insert_dispatch_graph(&mut states, task_queue, args);
            let mut artifact = CompiledWorkflowArtifact::new(
                definition_id,
                1,
                "benchmark-runner",
                ArtifactEntrypoint {
                    module: "benchmark.ts".to_owned(),
                    export: "workflow".to_owned(),
                },
                CompiledWorkflow {
                    initial_state: "init".to_owned(),
                    states,
                    params: Vec::new(),
                    non_cancellable_states: Default::default(),
                },
            );
            artifact.updates.insert(
                "setValue".to_owned(),
                fabrik_workflow::CompiledUpdateHandler {
                    arg_name: Some("payload".to_owned()),
                    initial_state: "mark_ready".to_owned(),
                    states: BTreeMap::from([
                        (
                            "mark_ready".to_owned(),
                            CompiledStateNode::Assign {
                                actions: vec![Assignment {
                                    target: "update_ready".to_owned(),
                                    expr: Expression::Literal { value: Value::Bool(true) },
                                }],
                                next: "finish_update".to_owned(),
                            },
                        ),
                        (
                            "finish_update".to_owned(),
                            CompiledStateNode::Succeed {
                                output: Some(Expression::Identifier { name: "payload".to_owned() }),
                            },
                        ),
                    ]),
                },
            );
            artifact.artifact_hash = artifact.hash();
            return artifact;
        }
        BenchmarkWorkloadKind::ContinueAsNew => {
            states.insert(
                "decide".to_owned(),
                CompiledStateNode::Choice {
                    condition: Expression::Binary {
                        op: fabrik_workflow::BinaryOp::GreaterThan,
                        left: Box::new(Expression::Member {
                            object: Box::new(Expression::Identifier { name: "input".to_owned() }),
                            property: "remaining".to_owned(),
                        }),
                        right: Box::new(Expression::Literal { value: json!(0) }),
                    },
                    then_next: "roll".to_owned(),
                    else_next: "dispatch".to_owned(),
                },
            );
            states.insert(
                "roll".to_owned(),
                CompiledStateNode::ContinueAsNew {
                    input: Some(Expression::Object {
                        fields: BTreeMap::from([
                            (
                                "remaining".to_owned(),
                                Expression::Binary {
                                    op: fabrik_workflow::BinaryOp::Subtract,
                                    left: Box::new(Expression::Member {
                                        object: Box::new(Expression::Identifier {
                                            name: "input".to_owned(),
                                        }),
                                        property: "remaining".to_owned(),
                                    }),
                                    right: Box::new(Expression::Literal { value: json!(1) }),
                                },
                            ),
                            (
                                "items".to_owned(),
                                Expression::Member {
                                    object: Box::new(Expression::Identifier {
                                        name: "input".to_owned(),
                                    }),
                                    property: "items".to_owned(),
                                },
                            ),
                        ]),
                    }),
                },
            );
            insert_dispatch_graph(&mut states, task_queue, args);
            "decide".to_owned()
        }
        BenchmarkWorkloadKind::ChildWorkflow => {
            states.insert(
                "start_child".to_owned(),
                CompiledStateNode::StartChild {
                    child_definition_id: child_definition_id(definition_id),
                    input: Expression::Literal { value: json!({ "ok": true }) },
                    next: "await_child".to_owned(),
                    handle_var: Some("child".to_owned()),
                    workflow_id: Some(Expression::Member {
                        object: Box::new(Expression::Identifier { name: "input".to_owned() }),
                        property: "child_instance_id".to_owned(),
                    }),
                    task_queue: None,
                    parent_close_policy: fabrik_workflow::ParentClosePolicy::Terminate,
                },
            );
            states.insert(
                "await_child".to_owned(),
                CompiledStateNode::WaitForChild {
                    child_ref_var: "child".to_owned(),
                    next: "dispatch".to_owned(),
                    output_var: Some("child_result".to_owned()),
                    on_error: Some(ErrorTransition {
                        next: "fail".to_owned(),
                        error_var: Some("error".to_owned()),
                    }),
                },
            );
            insert_dispatch_graph(&mut states, task_queue, args);
            "start_child".to_owned()
        }
    };

    CompiledWorkflowArtifact::new(
        definition_id,
        1,
        "benchmark-runner",
        ArtifactEntrypoint { module: "benchmark.ts".to_owned(), export: "workflow".to_owned() },
        CompiledWorkflow {
            initial_state,
            states,
            params: Vec::new(),
            non_cancellable_states: Default::default(),
        },
    )
}

fn benchmark_terminal_states() -> BTreeMap<String, CompiledStateNode> {
    BTreeMap::from([
        (
            "done".to_owned(),
            CompiledStateNode::Succeed {
                output: Some(Expression::Identifier { name: "results".to_owned() }),
            },
        ),
        (
            "fail".to_owned(),
            CompiledStateNode::Fail {
                reason: Some(Expression::Identifier { name: "error".to_owned() }),
            },
        ),
    ])
}

fn insert_dispatch_graph(
    states: &mut BTreeMap<String, CompiledStateNode>,
    task_queue: &str,
    args: &Args,
) {
    let enable_retry = args.retry_rate > 0.0;
    let activity_type = args.activity_type.clone();
    match args.execution_mode {
        ExecutionMode::Durable | ExecutionMode::Unified => {
            states.insert(
                "dispatch".to_owned(),
                CompiledStateNode::FanOut {
                    activity_type: activity_type.clone(),
                    items: Expression::Member {
                        object: Box::new(Expression::Identifier { name: "input".to_owned() }),
                        property: "items".to_owned(),
                    },
                    next: "join".to_owned(),
                    handle_var: "fanout".to_owned(),
                    task_queue: Some(Expression::Literal {
                        value: Value::String(task_queue.to_owned()),
                    }),
                    reducer: Some(args.bulk_reducer.clone()),
                    retry: enable_retry.then_some(RetryPolicy {
                        max_attempts: 2,
                        delay: format!("{}ms", args.retry_delay_ms),
                        maximum_interval: None,
                        backoff_coefficient_millis: None,
                        non_retryable_error_types: Vec::new(),
                    }),
                    config: None,
                    schedule_to_start_timeout_ms: None,
                    schedule_to_close_timeout_ms: None,
                    start_to_close_timeout_ms: None,
                    heartbeat_timeout_ms: None,
                },
            );
            states.insert(
                "join".to_owned(),
                CompiledStateNode::WaitForAllActivities {
                    fanout_ref_var: "fanout".to_owned(),
                    next: "done".to_owned(),
                    output_var: Some("results".to_owned()),
                    on_error: Some(ErrorTransition {
                        next: "fail".to_owned(),
                        error_var: Some("error".to_owned()),
                    }),
                },
            );
        }
        ExecutionMode::Throughput => {
            states.insert(
                "dispatch".to_owned(),
                CompiledStateNode::StartBulkActivity {
                    activity_type,
                    items: Expression::Member {
                        object: Box::new(Expression::Identifier { name: "input".to_owned() }),
                        property: "items".to_owned(),
                    },
                    next: "join".to_owned(),
                    handle_var: "fanout".to_owned(),
                    task_queue: Some(Expression::Literal {
                        value: Value::String(task_queue.to_owned()),
                    }),
                    execution_policy: (args.throughput_backend.as_deref()
                        == Some(STREAM_V2_BACKEND))
                    .then(|| "eager".to_owned()),
                    reducer: Some(args.bulk_reducer.clone()),
                    throughput_backend: args.throughput_backend.clone(),
                    chunk_size: Some(args.chunk_size),
                    retry: enable_retry.then_some(RetryPolicy {
                        max_attempts: 2,
                        delay: format!("{}ms", args.retry_delay_ms),
                        maximum_interval: None,
                        backoff_coefficient_millis: None,
                        non_retryable_error_types: Vec::new(),
                    }),
                },
            );
            states.insert(
                "join".to_owned(),
                CompiledStateNode::WaitForBulkActivity {
                    bulk_ref_var: "fanout".to_owned(),
                    next: "done".to_owned(),
                    output_var: Some("results".to_owned()),
                    on_error: Some(ErrorTransition {
                        next: "fail".to_owned(),
                        error_var: Some("error".to_owned()),
                    }),
                },
            );
        }
    }
}

fn benchmark_child_artifact(definition_id: &str) -> CompiledWorkflowArtifact {
    CompiledWorkflowArtifact::new(
        definition_id,
        1,
        "benchmark-runner",
        ArtifactEntrypoint {
            module: "benchmark-child.ts".to_owned(),
            export: "workflow".to_owned(),
        },
        CompiledWorkflow {
            initial_state: "done".to_owned(),
            states: BTreeMap::from([(
                "done".to_owned(),
                CompiledStateNode::Succeed {
                    output: Some(Expression::Literal { value: json!({ "ok": true }) }),
                },
            )]),
            params: Vec::new(),
            non_cancellable_states: Default::default(),
        },
    )
}

fn child_definition_id(parent_definition_id: &str) -> String {
    format!("{parent_definition_id}-child")
}

fn benchmark_input(
    args: &Args,
    activities_per_workflow: usize,
    payload_size: usize,
    retry_rate: f64,
    cancel_rate: f64,
    instance_id: &str,
) -> Value {
    let retry_count = ((activities_per_workflow as f64) * retry_rate).round() as usize;
    let cancel_count = ((activities_per_workflow as f64) * cancel_rate).round() as usize;
    let items_value = if should_use_compact_benchmark_input(args, retry_count, cancel_count) {
        benchmark_compact_input_spec(activities_per_workflow as u32)
    } else if args.activity_type == BENCHMARK_FAST_COUNT_ACTIVITY {
        Value::Array((0..activities_per_workflow).map(|_| Value::from(0)).collect())
    } else {
        let payload = "x".repeat(payload_size);
        Value::Array(
            (0..activities_per_workflow)
                .map(|index| {
                    let mut item = serde_json::Map::from_iter([
                        ("index".to_owned(), json!(index)),
                        ("payload".to_owned(), Value::String(payload.clone())),
                        (
                            "fail_until_attempt".to_owned(),
                            json!(if index < retry_count { 1 } else { 0 }),
                        ),
                        (
                            "cancel".to_owned(),
                            json!(index >= retry_count && index < retry_count + cancel_count),
                        ),
                    ]);
                    if let Some(reducer_value) = benchmark_reducer_value(&args.bulk_reducer, index)
                    {
                        item.insert("reducer_value".to_owned(), reducer_value);
                    }
                    Value::Object(item)
                })
                .collect(),
        )
    };
    let mut object = serde_json::Map::from_iter([("items".to_owned(), items_value)]);
    if args.workload_kind == BenchmarkWorkloadKind::ContinueAsNew {
        object.insert("remaining".to_owned(), json!(args.continue_rounds));
    }
    if args.workload_kind == BenchmarkWorkloadKind::ChildWorkflow {
        object
            .insert("child_instance_id".to_owned(), Value::String(format!("{instance_id}-child")));
    }
    Value::Object(object)
}

fn should_use_compact_benchmark_input(
    args: &Args,
    retry_count: usize,
    cancel_count: usize,
) -> bool {
    args.execution_mode == ExecutionMode::Throughput
        && args.throughput_backend.as_deref() == Some(STREAM_V2_BACKEND)
        && matches!(args.bulk_reducer.as_str(), "count" | "all_settled")
        && retry_count == 0
        && cancel_count == 0
        && matches!(
            args.activity_type.as_str(),
            BENCHMARK_ECHO_ACTIVITY | BENCHMARK_FAST_COUNT_ACTIVITY
        )
}

async fn apply_task_queue_throughput_policy(
    pool: &PgPool,
    tenant_id: &str,
    task_queue: &str,
    backend: Option<&str>,
) -> Result<()> {
    if backend.is_none() {
        sqlx::query(
            r#"
            DELETE FROM task_queue_throughput_policies
            WHERE tenant_id = $1
              AND queue_kind = 'activity'
              AND task_queue = $2
            "#,
        )
        .bind(tenant_id)
        .bind(task_queue)
        .execute(pool)
        .await
        .with_context(|| {
            format!(
                "failed to clear throughput policy for tenant {tenant_id} task queue {task_queue}"
            )
        })?;
        return Ok(());
    }
    let backend = backend.expect("backend presence already checked");
    sqlx::query(
        r#"
        INSERT INTO task_queue_throughput_policies (
            tenant_id,
            queue_kind,
            task_queue,
            backend,
            created_at,
            updated_at
        )
        VALUES ($1, 'activity', $2, $3, NOW(), NOW())
        ON CONFLICT (tenant_id, queue_kind, task_queue)
        DO UPDATE SET
            backend = EXCLUDED.backend,
            updated_at = EXCLUDED.updated_at
        "#,
    )
        .bind(tenant_id)
        .bind(task_queue)
        .bind(backend)
        .execute(pool)
        .await
        .with_context(|| {
            format!(
                "failed to apply throughput policy {backend} for tenant {tenant_id} task queue {task_queue}"
            )
        })?;
    Ok(())
}

async fn publish_artifact(
    client: &Client,
    ingest_base: &str,
    tenant_id: &str,
    artifact: &CompiledWorkflowArtifact,
) -> Result<()> {
    client
        .post(format!("{ingest_base}/tenants/{tenant_id}/workflow-artifacts"))
        .json(artifact)
        .send()
        .await
        .context("failed to publish benchmark artifact")?
        .error_for_status()
        .context("benchmark artifact publish returned error")?;
    Ok(())
}

async fn run_suite(args: Args, suite_name: String) -> Result<()> {
    let mut scenarios = benchmark_suite_scenarios(&args, &suite_name)?;
    if let Some(filter_tag) = args.scenario_tag.as_deref() {
        scenarios.retain(|scenario| scenario.scenario_tag.as_deref() == Some(filter_tag));
        if scenarios.is_empty() {
            bail!("suite {suite_name} does not contain scenario tag {filter_tag}");
        }
    }
    let mut reports = Vec::new();
    for scenario in scenarios {
        let report = run_benchmark(&scenario).await?;
        let output_path = scenario_output_path(&args.output_path, &report.scenario);
        write_report(&output_path, &report)?;
        println!("{}", summary_text(&report));
        println!("scenario_report_path={}", output_path.display());
        reports.push(report);
    }

    let suite_report = BenchmarkSuiteReport {
        suite: suite_name.clone(),
        profile: args.profile_name.clone(),
        generated_at: Utc::now(),
        scenarios: reports,
    };
    write_suite_report(&args.output_path, &suite_report)?;
    println!("suite_report_path={}", args.output_path.display());
    Ok(())
}

fn benchmark_suite_scenarios(args: &Args, suite_name: &str) -> Result<Vec<Args>> {
    match suite_name {
        "streaming" => {
            let mut durable = args.clone();
            durable.suite_name = None;
            durable.execution_mode = ExecutionMode::Durable;
            durable.throughput_backend = None;

            let mut throughput_pg = args.clone();
            throughput_pg.suite_name = None;
            throughput_pg.execution_mode = ExecutionMode::Throughput;
            throughput_pg.throughput_backend = Some(PG_V1_BACKEND.to_owned());

            let mut throughput_stream = args.clone();
            throughput_stream.suite_name = None;
            throughput_stream.execution_mode = ExecutionMode::Throughput;
            throughput_stream.throughput_backend = Some(STREAM_V2_BACKEND.to_owned());

            Ok(vec![durable, throughput_pg, throughput_stream])
        }
        "stream-v2-robustness" => {
            let mut throughput_stream = args.clone();
            throughput_stream.suite_name = None;
            throughput_stream.execution_mode = ExecutionMode::Throughput;
            throughput_stream.throughput_backend = Some(STREAM_V2_BACKEND.to_owned());
            throughput_stream.bulk_reducer = "all_settled".to_owned();
            Ok(vec![throughput_stream])
        }
        "stream-v2-fast-lane" => {
            let mut count = args.clone();
            count.suite_name = None;
            count.execution_mode = ExecutionMode::Throughput;
            count.throughput_backend = Some(STREAM_V2_BACKEND.to_owned());
            count.bulk_reducer = "count".to_owned();
            count.chunk_size = count.chunk_size.min(64);
            count.profile.activities_per_workflow =
                count.profile.activities_per_workflow.max(2_000);

            let mut settled = count.clone();
            settled.bulk_reducer = "all_settled".to_owned();
            settled.retry_rate = settled.retry_rate.max(0.01);
            settled.cancel_rate = settled.cancel_rate.max(0.01);

            Ok(vec![count, settled])
        }
        "stream-v2-fast-ceiling" => {
            let mut ceiling = args.clone();
            ceiling.suite_name = None;
            ceiling.execution_mode = ExecutionMode::Throughput;
            ceiling.throughput_backend = Some(STREAM_V2_BACKEND.to_owned());
            ceiling.activity_type = BENCHMARK_FAST_COUNT_ACTIVITY.to_owned();
            ceiling.bulk_reducer = "count".to_owned();
            ceiling.payload_size = 0;
            ceiling.retry_rate = 0.0;
            ceiling.cancel_rate = 0.0;
            ceiling.chunk_size = ceiling.chunk_size.max(256);
            ceiling.profile.activities_per_workflow =
                ceiling.profile.activities_per_workflow.max(4_096);
            ceiling.scenario_tag = Some("fast-ceiling".to_owned());
            Ok(vec![ceiling])
        }
        "stream-v2-failover" => {
            let mut failover = args.clone();
            failover.suite_name = None;
            failover.execution_mode = ExecutionMode::Throughput;
            failover.throughput_backend = Some(STREAM_V2_BACKEND.to_owned());
            failover.bulk_reducer = "all_settled".to_owned();
            failover.chunk_size = failover.chunk_size.min(64);
            failover.profile.workflow_count = failover.profile.workflow_count.max(12);
            failover.profile.activities_per_workflow =
                failover.profile.activities_per_workflow.max(4_096);
            failover.scenario_tag = Some("owner-restart".to_owned());

            let mut failover_retry_cancel = failover.clone();
            failover_retry_cancel.scenario_tag = Some("owner-restart-retry-cancel".to_owned());
            failover_retry_cancel.retry_rate = failover_retry_cancel.retry_rate.max(0.01);
            failover_retry_cancel.cancel_rate = failover_retry_cancel.cancel_rate.max(0.01);

            Ok(vec![failover, failover_retry_cancel])
        }
        "streaming-admission" => {
            let mut auto_small = args.clone();
            auto_small.suite_name = None;
            auto_small.execution_mode = ExecutionMode::Throughput;
            auto_small.throughput_backend = None;
            auto_small.bulk_reducer = "all_settled".to_owned();
            auto_small.chunk_size = 64;
            auto_small.profile.activities_per_workflow =
                auto_small.profile.activities_per_workflow.min(64);
            auto_small.profile.workflow_count = auto_small.profile.workflow_count.min(16);
            auto_small.scenario_tag = Some("auto-small".to_owned());

            let mut auto_large = args.clone();
            auto_large.suite_name = None;
            auto_large.execution_mode = ExecutionMode::Throughput;
            auto_large.throughput_backend = None;
            auto_large.bulk_reducer = "all_settled".to_owned();
            auto_large.chunk_size = 64;
            auto_large.profile.activities_per_workflow =
                auto_large.profile.activities_per_workflow.max(1_024);
            auto_large.scenario_tag = Some("auto-large".to_owned());

            let mut auto_materialized = args.clone();
            auto_materialized.suite_name = None;
            auto_materialized.execution_mode = ExecutionMode::Throughput;
            auto_materialized.throughput_backend = None;
            auto_materialized.bulk_reducer = "collect_results".to_owned();
            auto_materialized.chunk_size = 64;
            auto_materialized.profile.activities_per_workflow =
                auto_materialized.profile.activities_per_workflow.max(1_024);
            auto_materialized.scenario_tag = Some("auto-materialized".to_owned());

            Ok(vec![auto_small, auto_large, auto_materialized])
        }
        "streaming-reducers" => {
            let reducers = ["sum", "min", "max", "avg", "histogram"];
            let mut scenarios = Vec::with_capacity((reducers.len() * 2) + 4);
            for reducer in reducers {
                let mut pg = args.clone();
                pg.suite_name = None;
                pg.execution_mode = ExecutionMode::Throughput;
                pg.throughput_backend = Some(PG_V1_BACKEND.to_owned());
                pg.bulk_reducer = reducer.to_owned();
                pg.chunk_size = pg.chunk_size.min(64);
                pg.profile.activities_per_workflow = pg.profile.activities_per_workflow.max(1_024);
                pg.scenario_tag = Some(format!("{reducer}-pg-v1"));
                scenarios.push(pg);

                let mut stream = args.clone();
                stream.suite_name = None;
                stream.execution_mode = ExecutionMode::Throughput;
                stream.throughput_backend = Some(STREAM_V2_BACKEND.to_owned());
                stream.bulk_reducer = reducer.to_owned();
                stream.chunk_size = stream.chunk_size.min(64);
                stream.profile.activities_per_workflow =
                    stream.profile.activities_per_workflow.max(1_024);
                stream.scenario_tag = Some(format!("{reducer}-stream-v2"));
                scenarios.push(stream);
            }

            for backend in [PG_V1_BACKEND, STREAM_V2_BACKEND] {
                let backend_tag = if backend == PG_V1_BACKEND { "pg-v1" } else { "stream-v2" };

                let mut cancel_only = args.clone();
                cancel_only.suite_name = None;
                cancel_only.execution_mode = ExecutionMode::Throughput;
                cancel_only.throughput_backend = Some(backend.to_owned());
                cancel_only.bulk_reducer = "sample_errors".to_owned();
                cancel_only.chunk_size = cancel_only.chunk_size.min(64);
                cancel_only.profile.activities_per_workflow =
                    cancel_only.profile.activities_per_workflow.max(1_024);
                cancel_only.retry_rate = 0.0;
                cancel_only.cancel_rate = cancel_only.cancel_rate.max(0.01);
                cancel_only.scenario_tag = Some(format!("sample-errors-cancel-only-{backend_tag}"));
                scenarios.push(cancel_only);

                let mut retry_cancel = args.clone();
                retry_cancel.suite_name = None;
                retry_cancel.execution_mode = ExecutionMode::Throughput;
                retry_cancel.throughput_backend = Some(backend.to_owned());
                retry_cancel.bulk_reducer = "sample_errors".to_owned();
                retry_cancel.chunk_size = retry_cancel.chunk_size.min(64);
                retry_cancel.profile.activities_per_workflow =
                    retry_cancel.profile.activities_per_workflow.max(1_024);
                retry_cancel.retry_rate = retry_cancel.retry_rate.max(0.01);
                retry_cancel.cancel_rate = retry_cancel.cancel_rate.max(0.01);
                retry_cancel.retry_delay_ms = retry_cancel.retry_delay_ms.min(25);
                retry_cancel.scenario_tag =
                    Some(format!("sample-errors-retry-cancel-{backend_tag}"));
                scenarios.push(retry_cancel);
            }
            Ok(scenarios)
        }
        "topic-adapters" => {
            let mut start = args.clone();
            start.suite_name = None;
            start.execution_mode = ExecutionMode::Throughput;
            start.throughput_backend = Some(STREAM_V2_BACKEND.to_owned());
            start.ingress_driver = BenchmarkIngressDriver::TopicAdapterStart;
            start.workload_kind = BenchmarkWorkloadKind::Fanout;
            start.bulk_reducer = "all_settled".to_owned();
            start.chunk_size = start.chunk_size.min(64);
            start.profile.activities_per_workflow = start.profile.activities_per_workflow.max(512);
            start.scenario_tag = Some("start-workflow".to_owned());

            let mut signal = args.clone();
            signal.suite_name = None;
            signal.execution_mode = ExecutionMode::Throughput;
            signal.throughput_backend = Some(STREAM_V2_BACKEND.to_owned());
            signal.ingress_driver = BenchmarkIngressDriver::TopicAdapterSignal;
            signal.workload_kind = BenchmarkWorkloadKind::SignalGate;
            signal.bulk_reducer = "all_settled".to_owned();
            signal.chunk_size = signal.chunk_size.min(64);
            signal.profile.activities_per_workflow =
                signal.profile.activities_per_workflow.max(512);
            signal.scenario_tag = Some("signal-workflow".to_owned());

            Ok(vec![start, signal])
        }
        "topic-adapters-failover" => {
            let mut failover = args.clone();
            failover.suite_name = None;
            failover.execution_mode = ExecutionMode::Throughput;
            failover.throughput_backend = Some(STREAM_V2_BACKEND.to_owned());
            failover.ingress_driver = BenchmarkIngressDriver::TopicAdapterStart;
            failover.workload_kind = BenchmarkWorkloadKind::Fanout;
            failover.bulk_reducer = "all_settled".to_owned();
            failover.chunk_size = failover.chunk_size.min(64);
            failover.profile.workflow_count = failover.profile.workflow_count.max(12);
            failover.profile.activities_per_workflow =
                failover.profile.activities_per_workflow.max(4_096);
            failover.scenario_tag = Some("owner-crash".to_owned());

            let mut lag_under_load = failover.clone();
            lag_under_load.profile.workflow_count = 256;
            lag_under_load.profile.activities_per_workflow = 512;
            lag_under_load.scenario_tag = Some("lag-under-load-owner-crash".to_owned());

            Ok(vec![failover, lag_under_load])
        }
        other => bail!(
            "unknown suite {other}; expected streaming, stream-v2-robustness, stream-v2-fast-lane, stream-v2-fast-ceiling, stream-v2-failover, streaming-admission, streaming-reducers, topic-adapters, or topic-adapters-failover"
        ),
    }
}

fn load_failover_injection_metrics() -> Result<Option<FailoverInjectionMetrics>> {
    let path = match env::var("BENCHMARK_FAILOVER_INJECTION_PATH") {
        Ok(value) if !value.is_empty() => PathBuf::from(value),
        _ => return Ok(None),
    };
    let payload = fs::read(&path)
        .with_context(|| format!("failed to read failover injection report {}", path.display()))?;
    let metrics = serde_json::from_slice(&payload).with_context(|| {
        format!("failed to decode failover injection report {}", path.display())
    })?;
    Ok(Some(metrics))
}

fn benchmark_reducer_value(reducer: &str, index: usize) -> Option<Value> {
    match reducer {
        "sum" | "min" | "max" | "avg" => Some(json!(index + 1)),
        "histogram" => Some(json!(match index % 4 {
            0 => "alpha",
            1 => "beta",
            2 => "gamma",
            _ => "delta",
        })),
        _ => None,
    }
}

fn scenario_name(args: &Args) -> String {
    let mut scenario = match args.execution_mode {
        ExecutionMode::Durable => "durable".to_owned(),
        ExecutionMode::Throughput => {
            format!("throughput-{}", args.throughput_backend.as_deref().unwrap_or("auto"))
        }
        ExecutionMode::Unified => "unified-experiment".to_owned(),
    };
    match args.ingress_driver {
        BenchmarkIngressDriver::HttpTrigger => {}
        BenchmarkIngressDriver::TopicAdapterStart => scenario.push_str("-adapter-start"),
        BenchmarkIngressDriver::TopicAdapterSignal => scenario.push_str("-adapter-signal"),
    }
    if let Some(tag) = args.scenario_tag.as_deref() {
        scenario.push('-');
        scenario.push_str(tag);
    }
    if args.bulk_reducer != "collect_results" {
        scenario.push('-');
        scenario.push_str(&args.bulk_reducer.replace('_', "-"));
    }
    if let Some(suffix) = scenario_rate_suffix("retry", args.retry_rate) {
        scenario.push('-');
        scenario.push_str(&suffix);
    }
    if let Some(suffix) = scenario_rate_suffix("cancel", args.cancel_rate) {
        scenario.push('-');
        scenario.push_str(&suffix);
    }
    scenario
}

fn benchmark_definition_id(args: &Args, scenario_name: &str) -> String {
    format!(
        "{}-benchmark-{}-{}",
        args.workload_kind.definition_prefix(),
        args.profile_name,
        scenario_name
    )
}

fn scenario_rate_suffix(label: &str, rate: f64) -> Option<String> {
    if rate <= 0.0 {
        return None;
    }
    let basis_points = (rate * 10_000.0).round() as u64;
    Some(format!("{label}-{basis_points}bp"))
}

fn scenario_output_path(base: &Path, scenario: &str) -> PathBuf {
    let stem = base.file_stem().and_then(|value| value.to_str()).unwrap_or("benchmark");
    let extension = base.extension().and_then(|value| value.to_str()).unwrap_or("json");
    let file_name = format!("{stem}-{scenario}.{extension}");
    base.parent().unwrap_or_else(|| Path::new(".")).join(file_name)
}

fn write_suite_report(output_path: &Path, report: &BenchmarkSuiteReport) -> Result<()> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(
        output_path,
        serde_json::to_vec_pretty(report).context("failed to serialize benchmark suite report")?,
    )
    .with_context(|| format!("failed to write {}", output_path.display()))?;
    Ok(())
}

async fn fetch_optional_debug(client: &Client, url: &str) -> Value {
    match client.get(url).send().await {
        Ok(response) => match response.error_for_status() {
            Ok(ok) => ok.json::<Value>().await.unwrap_or_else(
                |error| json!({ "error": format!("failed to decode debug payload: {error}") }),
            ),
            Err(error) => json!({ "error": error.to_string() }),
        },
        Err(error) => json!({ "error": error.to_string() }),
    }
}

async fn trigger_workflow(
    client: &Client,
    ingest_base: &str,
    definition_id: &str,
    tenant_id: &str,
    instance_id: &str,
    task_queue: &str,
    input: Value,
) -> Result<()> {
    client
        .post(format!("{ingest_base}/workflows/{definition_id}/trigger"))
        .json(&json!({
            "tenant_id": tenant_id,
            "instance_id": instance_id,
            "workflow_task_queue": task_queue,
            "input": input,
            "request_id": instance_id,
        }))
        .send()
        .await
        .with_context(|| format!("failed to trigger benchmark workflow {instance_id}"))?
        .error_for_status()
        .with_context(|| format!("benchmark workflow trigger failed for {instance_id}"))?;
    Ok(())
}

async fn signal_workflow(
    client: &Client,
    ingest_base: &str,
    tenant_id: &str,
    instance_id: &str,
) -> Result<()> {
    let url = format!("{ingest_base}/tenants/{tenant_id}/workflows/{instance_id}/signals/approve");
    retry_workflow_mutation(
        client,
        &url,
        json!({
            "payload": { "approved": true },
            "request_id": format!("signal:{instance_id}"),
        }),
    )
    .await
    .with_context(|| format!("benchmark workflow signal failed for {instance_id}"))
}

async fn update_workflow(
    client: &Client,
    ingest_base: &str,
    tenant_id: &str,
    instance_id: &str,
) -> Result<()> {
    let url = format!("{ingest_base}/tenants/{tenant_id}/workflows/{instance_id}/updates/setValue");
    retry_workflow_mutation(
        client,
        &url,
        json!({
            "payload": 1,
            "request_id": format!("update:{instance_id}"),
            "wait_for": "completed",
            "timeout_ms": 5_000,
        }),
    )
    .await
    .with_context(|| format!("benchmark workflow update failed for {instance_id}"))
}

async fn retry_workflow_mutation(client: &Client, url: &str, body: Value) -> Result<()> {
    let mut last_status = None;
    let mut last_error = None;
    for _attempt in 0..100 {
        match client.post(url).json(&body).send().await {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(response)
                if response.status() == reqwest::StatusCode::NOT_FOUND
                    || response.status() == reqwest::StatusCode::CONFLICT =>
            {
                last_status = Some(response.status());
            }
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                bail!("request returned {status}: {body}");
            }
            Err(error) => {
                last_error = Some(error);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    if let Some(error) = last_error {
        return Err(error).context("mutation request did not succeed before retry deadline");
    }
    if let Some(status) = last_status {
        bail!("mutation request remained at retryable status {status}");
    }
    bail!("mutation request exhausted retries without a response")
}

async fn workflow_outcomes(
    pool: &PgPool,
    tenant_id: &str,
    instance_prefix: &str,
    workload_kind: BenchmarkWorkloadKind,
) -> Result<WorkflowOutcomeMetrics> {
    let excluded_child_pattern = instance_exclusion_pattern(workload_kind, instance_prefix);
    let row = sqlx::query_as::<_, WorkflowOutcomeRow>(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE state->>'status' = 'completed') AS completed,
            COUNT(*) FILTER (WHERE state->>'status' = 'failed') AS failed,
            COUNT(*) FILTER (WHERE state->>'status' = 'cancelled') AS cancelled,
            COUNT(*) FILTER (WHERE state->>'status' NOT IN ('completed', 'failed', 'cancelled', 'terminated')) AS running
        FROM workflow_instances
        WHERE tenant_id = $1
          AND workflow_instance_id LIKE $2
          AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)
        "#,
    )
    .bind(tenant_id)
    .bind(format!("{instance_prefix}%"))
    .bind(excluded_child_pattern)
    .fetch_one(pool)
    .await
    .context("failed to query workflow outcomes")?;

    Ok(WorkflowOutcomeMetrics {
        completed: row.completed as u64,
        failed: row.failed as u64,
        cancelled: row.cancelled as u64,
        running: row.running as u64,
    })
}

async fn wait_for_stream_projection_convergence(
    pool: &PgPool,
    tenant_id: &str,
    instance_prefix: &str,
    workload_kind: BenchmarkWorkloadKind,
    expected_batches: u64,
    expected_items: u64,
    timeout: Duration,
) -> Result<()> {
    let started = Instant::now();
    let excluded_child_pattern = instance_exclusion_pattern(workload_kind, instance_prefix);
    loop {
        let row = sqlx::query_as::<_, StreamProjectionConvergenceRow>(
            r#"
            SELECT
                (SELECT COUNT(*)::bigint
                 FROM throughput_projection_batches
                 WHERE tenant_id = $1
                   AND workflow_instance_id LIKE $2
                   AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)) AS projection_batch_rows,
                (SELECT COUNT(*) FILTER (WHERE status IN ('completed', 'failed', 'cancelled'))::bigint
                 FROM throughput_projection_batches
                 WHERE tenant_id = $1
                   AND workflow_instance_id LIKE $2
                   AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)) AS terminal_batch_rows,
                (SELECT COUNT(*)::bigint
                 FROM (
                     SELECT batch_id
                     FROM throughput_projection_chunks
                     WHERE tenant_id = $1
                       AND workflow_instance_id LIKE $2
                       AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)
                     GROUP BY batch_id
                     HAVING BOOL_AND(status IN ('completed', 'failed', 'cancelled'))
                 ) AS chunk_terminal_batches) AS inferred_terminal_batches,
                (SELECT COALESCE(SUM(succeeded_items + failed_items + cancelled_items), 0)::bigint
                 FROM throughput_projection_batches
                 WHERE tenant_id = $1
                   AND workflow_instance_id LIKE $2
                   AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)
                   AND status IN ('completed', 'failed', 'cancelled')) AS batch_accounted_items,
                (SELECT COALESCE(
                    SUM(
                        GREATEST(
                            total_items - succeeded_items - failed_items - cancelled_items,
                            0
                        )
                    ),
                    0
                )::bigint
                 FROM throughput_projection_batches
                 WHERE tenant_id = $1
                   AND workflow_instance_id LIKE $2
                   AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)
                   AND status IN ('failed', 'cancelled')) AS interrupted_batch_missing_items,
                (SELECT COALESCE(SUM(item_count), 0)::bigint
                 FROM throughput_projection_chunks
                 WHERE tenant_id = $1
                   AND workflow_instance_id LIKE $2
                   AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)
                   AND status = 'completed') AS completed_items,
                (SELECT COALESCE(SUM(item_count), 0)::bigint
                 FROM throughput_projection_chunks
                 WHERE tenant_id = $1
                   AND workflow_instance_id LIKE $2
                   AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)
                   AND status = 'failed') AS failed_items,
                (SELECT COALESCE(SUM(item_count), 0)::bigint
                 FROM throughput_projection_chunks
                 WHERE tenant_id = $1
                   AND workflow_instance_id LIKE $2
                   AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)
                   AND status = 'cancelled') AS cancelled_items,
                (SELECT COUNT(*) FILTER (WHERE status IN ('completed', 'failed', 'cancelled'))::bigint
                 FROM throughput_projection_chunks
                 WHERE tenant_id = $1
                   AND workflow_instance_id LIKE $2
                   AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)) AS terminal_chunk_rows,
                (
                    SELECT COUNT(*)::bigint
                    FROM throughput_projection_chunks
                    WHERE tenant_id = $1
                      AND workflow_instance_id LIKE $2
                      AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)
                      AND status = 'scheduled'
                ) AS pending_chunks
            "#,
        )
        .bind(tenant_id)
        .bind(format!("{instance_prefix}%"))
        .bind(excluded_child_pattern.clone())
        .fetch_one(pool)
        .await
        .context("failed to query stream-v2 projection convergence")?;

        let chunk_accounted_items = row.completed_items + row.failed_items + row.cancelled_items;
        if stream_projection_has_converged(&row, expected_batches, expected_items) {
            return Ok(());
        }

        if started.elapsed() >= timeout {
            bail!(
                "timed out waiting for stream-v2 projection convergence: batches={}/{} terminal_batches={}/{} inferred_terminal_batches={} batch_items={}/{} chunk_items={}/{} terminal_chunk_rows={} pending_chunks={}",
                row.projection_batch_rows,
                expected_batches,
                row.terminal_batch_rows,
                expected_batches,
                row.inferred_terminal_batches,
                row.batch_accounted_items + row.interrupted_batch_missing_items,
                expected_items,
                chunk_accounted_items,
                expected_items,
                row.terminal_chunk_rows,
                row.pending_chunks
            );
        }

        tokio::time::sleep(Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)).await;
    }
}

async fn activity_metrics(
    pool: &PgPool,
    tenant_id: &str,
    instance_prefix: &str,
    workload_kind: BenchmarkWorkloadKind,
    duration_ms: u128,
    total_activities: usize,
    execution_mode: ExecutionMode,
    throughput_backend: Option<&str>,
) -> Result<ActivityMetrics> {
    let excluded_child_pattern = instance_exclusion_pattern(workload_kind, instance_prefix);
    if execution_mode == ExecutionMode::Throughput {
        let row = if throughput_backend == Some(STREAM_V2_BACKEND) {
            sqlx::query_as::<_, (i64, i64, i64)>(
                r#"
                SELECT
                    COALESCE(SUM(item_count) FILTER (WHERE status = 'completed'), 0) AS completed,
                    COALESCE(SUM(item_count) FILTER (WHERE status = 'failed'), 0) AS failed,
                    COALESCE(SUM(item_count) FILTER (WHERE status = 'cancelled'), 0) AS cancelled
                FROM throughput_projection_chunks
                WHERE tenant_id = $1
                  AND workflow_instance_id LIKE $2
                  AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)
                "#,
            )
            .bind(tenant_id)
            .bind(format!("{instance_prefix}%"))
            .bind(excluded_child_pattern.clone())
            .fetch_one(pool)
            .await
            .context("failed to query stream-v2 activity metrics")?
        } else {
            sqlx::query_as::<_, (i64, i64, i64)>(
                r#"
                SELECT
                    COALESCE(SUM(succeeded_items), 0) AS completed,
                    COALESCE(SUM(failed_items), 0) AS failed,
                    COALESCE(SUM(cancelled_items), 0) AS cancelled
                FROM workflow_bulk_batches
                WHERE tenant_id = $1
                  AND workflow_instance_id LIKE $2
                  AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)
                "#,
            )
            .bind(tenant_id)
            .bind(format!("{instance_prefix}%"))
            .bind(excluded_child_pattern.clone())
            .fetch_one(pool)
            .await
            .context("failed to query pg-v1 bulk activity metrics")?
        };
        let throughput = if duration_ms == 0 {
            0.0
        } else {
            total_activities as f64 / (duration_ms as f64 / 1_000.0)
        };
        return Ok(ActivityMetrics {
            completed: row.0 as u64,
            failed: row.1 as u64,
            cancelled: row.2 as u64,
            timed_out: 0,
            avg_schedule_to_start_latency_ms: 0.0,
            max_schedule_to_start_latency_ms: 0,
            avg_start_to_close_latency_ms: 0.0,
            max_start_to_close_latency_ms: 0,
            throughput_activities_per_second: throughput,
        });
    }

    let row = sqlx::query_as::<_, ActivityMetricRow>(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE status = 'completed') AS completed,
            COUNT(*) FILTER (WHERE status = 'failed') AS failed,
            COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled,
            COUNT(*) FILTER (WHERE status = 'timed_out') AS timed_out,
            AVG((EXTRACT(EPOCH FROM (started_at - scheduled_at)) * 1000)::double precision)
                FILTER (WHERE started_at IS NOT NULL) AS avg_schedule_to_start_latency_ms,
            MAX((EXTRACT(EPOCH FROM (started_at - scheduled_at)) * 1000)::double precision)
                FILTER (WHERE started_at IS NOT NULL) AS max_schedule_to_start_latency_ms,
            AVG((EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000)::double precision)
                FILTER (WHERE completed_at IS NOT NULL AND started_at IS NOT NULL) AS avg_start_to_close_latency_ms,
            MAX((EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000)::double precision)
                FILTER (WHERE completed_at IS NOT NULL AND started_at IS NOT NULL) AS max_start_to_close_latency_ms
        FROM workflow_activities
        WHERE tenant_id = $1
          AND workflow_instance_id LIKE $2
          AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)
        "#,
    )
    .bind(tenant_id)
    .bind(format!("{instance_prefix}%"))
    .bind(excluded_child_pattern)
    .fetch_one(pool)
    .await
    .context("failed to query activity metrics")?;

    let throughput = if duration_ms == 0 {
        0.0
    } else {
        total_activities as f64 / (duration_ms as f64 / 1_000.0)
    };
    Ok(ActivityMetrics {
        completed: row.completed as u64,
        failed: row.failed as u64,
        cancelled: row.cancelled as u64,
        timed_out: row.timed_out as u64,
        avg_schedule_to_start_latency_ms: row.avg_schedule_to_start_latency_ms.unwrap_or(0.0),
        max_schedule_to_start_latency_ms: row.max_schedule_to_start_latency_ms.unwrap_or(0.0)
            as u64,
        avg_start_to_close_latency_ms: row.avg_start_to_close_latency_ms.unwrap_or(0.0),
        max_start_to_close_latency_ms: row.max_start_to_close_latency_ms.unwrap_or(0.0) as u64,
        throughput_activities_per_second: throughput,
    })
}

async fn bulk_metrics(
    pool: &PgPool,
    tenant_id: &str,
    instance_prefix: &str,
    workload_kind: BenchmarkWorkloadKind,
) -> Result<(u64, u64, u64, u64, u64, u64)> {
    let excluded_child_pattern = instance_exclusion_pattern(workload_kind, instance_prefix);
    let row = sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64)>(
        r#"
        SELECT
            (SELECT COUNT(*)
             FROM workflow_bulk_batches
             WHERE tenant_id = $1
               AND workflow_instance_id LIKE $2
               AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)) AS batch_rows,
            (SELECT COUNT(*)
             FROM workflow_bulk_chunks
             WHERE tenant_id = $1
               AND workflow_instance_id LIKE $2
               AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)) AS chunk_rows,
            (SELECT COUNT(*)
             FROM throughput_projection_batches
             WHERE tenant_id = $1
               AND workflow_instance_id LIKE $2
               AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)) AS projection_batch_rows,
            (SELECT COUNT(*)
             FROM throughput_projection_chunks
             WHERE tenant_id = $1
               AND workflow_instance_id LIKE $2
               AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)) AS projection_chunk_rows,
            (SELECT COALESCE(MAX(aggregation_group_count), 0)
             FROM throughput_projection_batches
             WHERE tenant_id = $1
               AND workflow_instance_id LIKE $2
               AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3))::bigint AS max_group_count,
            (SELECT COUNT(*)
             FROM throughput_projection_batches
             WHERE tenant_id = $1
               AND workflow_instance_id LIKE $2
               AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)
               AND aggregation_group_count > 1)
             AS grouped_batch_rows
        "#,
    )
    .bind(tenant_id)
    .bind(format!("{instance_prefix}%"))
    .bind(excluded_child_pattern)
    .fetch_one(pool)
    .await
    .context("failed to query bulk benchmark metrics")?;
    Ok((row.0 as u64, row.1 as u64, row.2 as u64, row.3 as u64, row.4 as u64, row.5 as u64))
}

async fn batch_routing_metrics(
    pool: &PgPool,
    tenant_id: &str,
    instance_prefix: &str,
    workload_kind: BenchmarkWorkloadKind,
) -> Result<BatchRoutingMetrics> {
    let excluded_child_pattern = instance_exclusion_pattern(workload_kind, instance_prefix);
    let rows = sqlx::query_as::<_, (String, String, String, i64)>(
        r#"
        SELECT
            selected_backend,
            routing_reason,
            admission_policy_version,
            SUM(batch_count)::bigint AS batch_count
        FROM (
            SELECT
                throughput_backend AS selected_backend,
                routing_reason,
                admission_policy_version,
                COUNT(*)::bigint AS batch_count
            FROM workflow_bulk_batches
            WHERE tenant_id = $1
              AND workflow_instance_id LIKE $2
              AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)
            GROUP BY throughput_backend, routing_reason, admission_policy_version
            UNION ALL
            SELECT
                throughput_backend AS selected_backend,
                routing_reason,
                admission_policy_version,
                COUNT(*)::bigint AS batch_count
            FROM throughput_projection_batches
            WHERE tenant_id = $1
              AND workflow_instance_id LIKE $2
              AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)
            GROUP BY throughput_backend, routing_reason, admission_policy_version
        ) AS routing
        GROUP BY selected_backend, routing_reason, admission_policy_version
        ORDER BY batch_count DESC, selected_backend ASC, routing_reason ASC
        "#,
    )
    .bind(tenant_id)
    .bind(format!("{instance_prefix}%"))
    .bind(excluded_child_pattern)
    .fetch_all(pool)
    .await
    .context("failed to query batch routing metrics")?;

    let mut metrics = BatchRoutingMetrics::default();
    for (backend, reason, version, count) in rows {
        let count = count as u64;
        *metrics.backend_counts.entry(backend).or_default() += count;
        *metrics.routing_reason_counts.entry(reason).or_default() += count;
        *metrics.admission_policy_version_counts.entry(version).or_default() += count;
    }
    Ok(metrics)
}

async fn coalescing_metrics(
    pool: &PgPool,
    tenant_id: &str,
    instance_prefix: &str,
    workload_kind: BenchmarkWorkloadKind,
) -> Result<CoalescingMetrics> {
    let excluded_child_pattern = instance_exclusion_pattern(workload_kind, instance_prefix);
    let row = sqlx::query_as::<_, CoalescingRow>(
        r#"
        SELECT
            (SELECT COUNT(*)
             FROM workflow_tasks
             WHERE tenant_id = $1
               AND workflow_instance_id LIKE $2
               AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)) AS workflow_task_rows,
            (SELECT COUNT(*)
             FROM workflow_resumes
             WHERE tenant_id = $1
               AND workflow_instance_id LIKE $2
               AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)) AS resume_rows
        "#,
    )
    .bind(tenant_id)
    .bind(format!("{instance_prefix}%"))
    .bind(excluded_child_pattern)
    .fetch_one(pool)
    .await
    .context("failed to query coalescing metrics")?;

    let workflow_task_rows = row.workflow_task_rows as u64;
    let resume_rows = row.resume_rows as u64;
    Ok(CoalescingMetrics {
        workflow_task_rows,
        resume_rows,
        resume_events_per_task_row: if workflow_task_rows == 0 {
            0.0
        } else {
            resume_rows as f64 / workflow_task_rows as f64
        },
    })
}

async fn backlog_snapshot(
    pool: &PgPool,
    tenant_id: &str,
    instance_prefix: &str,
    workload_kind: BenchmarkWorkloadKind,
    execution_mode: ExecutionMode,
    throughput_backend: Option<&str>,
) -> Result<(u64, u64)> {
    let excluded_child_pattern = instance_exclusion_pattern(workload_kind, instance_prefix);
    let workflow_backlog = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM workflow_tasks
        WHERE tenant_id = $1
          AND workflow_instance_id LIKE $2
          AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)
          AND status = 'pending'
          AND (mailbox_backlog > 0 OR resume_backlog > 0)
        "#,
    )
    .bind(tenant_id)
    .bind(format!("{instance_prefix}%"))
    .bind(excluded_child_pattern.clone())
    .fetch_one(pool)
    .await
    .context("failed to query workflow backlog")?;
    let activity_backlog = if execution_mode == ExecutionMode::Throughput
        && throughput_backend == Some(STREAM_V2_BACKEND)
    {
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT
                (SELECT COUNT(*)
                 FROM workflow_activities
                 WHERE tenant_id = $1
                   AND workflow_instance_id LIKE $2
                   AND status = 'scheduled')
                +
                (SELECT COUNT(*)
                 FROM throughput_projection_chunks chunks
                 JOIN throughput_projection_batches batches
                   ON batches.tenant_id = chunks.tenant_id
                  AND batches.workflow_instance_id = chunks.workflow_instance_id
                  AND batches.run_id = chunks.run_id
                 AND batches.batch_id = chunks.batch_id
                 WHERE chunks.tenant_id = $1
                   AND chunks.workflow_instance_id LIKE $2
                   AND ($3::text IS NULL OR chunks.workflow_instance_id NOT LIKE $3)
                   AND chunks.status = 'scheduled'
                   AND batches.status IN ('scheduled', 'running'))
            "#,
        )
        .bind(tenant_id)
        .bind(format!("{instance_prefix}%"))
        .bind(excluded_child_pattern.clone())
        .fetch_one(pool)
        .await
        .context("failed to query stream-v2 activity backlog")?
    } else {
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT
                (SELECT COUNT(*)
                 FROM workflow_activities
                 WHERE tenant_id = $1
                   AND workflow_instance_id LIKE $2
                   AND status = 'scheduled')
                +
                (SELECT COUNT(*)
                 FROM workflow_bulk_chunks chunks
                 JOIN workflow_bulk_batches batches
                   ON batches.tenant_id = chunks.tenant_id
                  AND batches.workflow_instance_id = chunks.workflow_instance_id
                  AND batches.run_id = chunks.run_id
                 AND batches.batch_id = chunks.batch_id
                 WHERE chunks.tenant_id = $1
                   AND chunks.workflow_instance_id LIKE $2
                   AND ($3::text IS NULL OR chunks.workflow_instance_id NOT LIKE $3)
                   AND chunks.status = 'scheduled'
                   AND batches.status IN ('scheduled', 'running'))
            "#,
        )
        .bind(tenant_id)
        .bind(format!("{instance_prefix}%"))
        .bind(excluded_child_pattern)
        .fetch_one(pool)
        .await
        .context("failed to query activity backlog")?
    };
    Ok((workflow_backlog as u64, activity_backlog as u64))
}

fn instance_exclusion_pattern(
    workload_kind: BenchmarkWorkloadKind,
    instance_prefix: &str,
) -> Option<String> {
    (workload_kind == BenchmarkWorkloadKind::ChildWorkflow)
        .then(|| format!("{instance_prefix}%-child"))
}

fn control_plane_metrics(
    runtime_debug_delta: Option<&Value>,
    projector_debug_delta: Option<&Value>,
    activity_metrics: &ActivityMetrics,
) -> Option<ControlPlaneMetrics> {
    let runtime = runtime_debug_delta?.get("runtime")?;
    let completed_chunks =
        activity_metrics.completed + activity_metrics.failed + activity_metrics.cancelled;
    let poll_requests = json_u64(runtime, "poll_requests");
    let leased_tasks = json_u64(runtime, "leased_tasks");
    let report_rpcs_received = json_u64(runtime, "report_rpcs_received");
    let reports_received = json_u64(runtime, "reports_received");
    let report_batches_applied = json_u64(runtime, "report_batches_applied");
    let projection_events_published = json_u64(runtime, "projection_events_published");
    let projection_events_skipped = json_u64(runtime, "projection_events_skipped");
    let projection_events_applied_directly =
        json_u64(runtime, "projection_events_applied_directly");
    let changelog_entries_published = json_u64(runtime, "changelog_entries_published");
    let manifest_writes = projector_debug_delta
        .and_then(|value| value.get("manifest_writes"))
        .and_then(Value::as_u64)
        .unwrap_or_default();

    Some(ControlPlaneMetrics {
        avg_tasks_per_bulk_poll_response: ratio(leased_tasks, poll_requests),
        avg_results_per_bulk_report_rpc: ratio(reports_received, report_rpcs_received),
        changelog_entries_per_completed_chunk: ratio(changelog_entries_published, completed_chunks),
        projection_events_per_completed_chunk: ratio(projection_events_published, completed_chunks),
        report_batches_applied,
        avg_report_batch_size: ratio(reports_received, report_batches_applied),
        projection_events_published,
        projection_events_skipped,
        projection_events_applied_directly,
        changelog_entries_published,
        manifest_writes,
    })
}

fn executor_debug_delta_metrics(
    executor_debug_delta: Option<&Value>,
    activity_metrics: &ActivityMetrics,
) -> Option<ExecutorDebugDeltaMetrics> {
    let debug = executor_debug_delta?;
    let runtime = debug.get("runtime")?;
    let completed_activities =
        activity_metrics.completed + activity_metrics.failed + activity_metrics.cancelled;
    let leased_tasks = json_u64(runtime, "leased_tasks");
    let poll_requests = json_u64(runtime, "poll_requests");
    let report_rpcs_received = json_u64(runtime, "report_rpcs_received");
    let report_batches_applied = json_u64(runtime, "report_batches_applied");

    Some(ExecutorDebugDeltaMetrics {
        polls_per_leased_task: ratio(poll_requests, leased_tasks),
        report_rpcs_per_completed_activity: ratio(report_rpcs_received, completed_activities),
        report_batches_per_completed_activity: ratio(report_batches_applied, completed_activities),
        log_writes: json_u64(runtime, "log_writes"),
        snapshot_writes: json_u64(runtime, "snapshot_writes"),
    })
}

fn json_numeric_delta(before: &Value, after: &Value) -> Value {
    match (before, after) {
        (Value::Object(before), Value::Object(after)) => {
            let mut merged = serde_json::Map::new();
            for key in before.keys().chain(after.keys()) {
                if merged.contains_key(key) {
                    continue;
                }
                let before_value = before.get(key).unwrap_or(&Value::Null);
                let after_value = after.get(key).unwrap_or(&Value::Null);
                let delta = json_numeric_delta(before_value, after_value);
                if !delta.is_null() {
                    merged.insert(key.clone(), delta);
                }
            }
            Value::Object(merged)
        }
        _ => {
            let before = before.as_i64().or_else(|| before.as_u64().map(|value| value as i64));
            let after = after.as_i64().or_else(|| after.as_u64().map(|value| value as i64));
            match (before, after) {
                (Some(before), Some(after)) => Value::from(after - before),
                _ => Value::Null,
            }
        }
    }
}

fn json_u64(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or_default()
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 { 0.0 } else { numerator as f64 / denominator as f64 }
}

fn write_report(output_path: &Path, report: &BenchmarkReport) -> Result<()> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(
        output_path,
        serde_json::to_vec_pretty(report).context("failed to serialize benchmark report")?,
    )
    .with_context(|| format!("failed to write {}", output_path.display()))?;

    let summary_path = output_path.with_extension("txt");
    fs::write(&summary_path, summary_text(report))
        .with_context(|| format!("failed to write {}", summary_path.display()))?;
    Ok(())
}

fn summary_text(report: &BenchmarkReport) -> String {
    let control_plane = report.control_plane_metrics.as_ref().map(|metrics| {
        format!(
            "avg_tasks_per_bulk_poll_response={:.2}\navg_results_per_bulk_report_rpc={:.2}\nchangelog_entries_per_completed_chunk={:.2}\nprojection_events_per_completed_chunk={:.2}\nreport_batches_applied={}\navg_report_batch_size={:.2}\nprojection_events_published={}\nprojection_events_skipped={}\nprojection_events_applied_directly={}\nchangelog_entries_published={}\nmanifest_writes={}\n",
            metrics.avg_tasks_per_bulk_poll_response,
            metrics.avg_results_per_bulk_report_rpc,
            metrics.changelog_entries_per_completed_chunk,
            metrics.projection_events_per_completed_chunk,
            metrics.report_batches_applied,
            metrics.avg_report_batch_size,
            metrics.projection_events_published,
            metrics.projection_events_skipped,
            metrics.projection_events_applied_directly,
            metrics.changelog_entries_published,
            metrics.manifest_writes,
        )
    }).unwrap_or_default();
    let executor_debug_delta = report.executor_debug_delta_metrics.as_ref().map(|metrics| {
        format!(
            "polls_per_leased_task={:.4}\nreport_rpcs_per_completed_activity={:.4}\nreport_batches_per_completed_activity={:.4}\nlog_writes={}\nsnapshot_writes={}\n",
            metrics.polls_per_leased_task,
            metrics.report_rpcs_per_completed_activity,
            metrics.report_batches_per_completed_activity,
            metrics.log_writes,
            metrics.snapshot_writes,
        )
    }).unwrap_or_default();
    let backend_counts = format_counts(&report.batch_routing_metrics.backend_counts);
    let routing_reason_counts = format_counts(&report.batch_routing_metrics.routing_reason_counts);
    let admission_policy_versions =
        format_counts(&report.batch_routing_metrics.admission_policy_version_counts);
    let failover = report.failover_injection.as_ref().map(|metrics| {
        let downtime_ms = metrics
            .downtime_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_owned());
        let stop_requested_at_ms = metrics
            .stop_requested_at_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_owned());
        let restart_ready_at_ms = metrics
            .restart_ready_at_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_owned());
        let error = metrics.error.clone().unwrap_or_else(|| "none".to_owned());
        format!(
            "failover_status={}\nfailover_delay_ms={}\nfailover_stop_requested_at_ms={}\nfailover_restart_ready_at_ms={}\nfailover_downtime_ms={}\nfailover_error={}\n",
            metrics.status,
            metrics.delay_ms,
            stop_requested_at_ms,
            restart_ready_at_ms,
            downtime_ms,
            error,
        )
    }).unwrap_or_default();
    let adapter = report.adapter_metrics.as_ref().map(|metrics| {
        let last_takeover_latency_ms = metrics
            .last_takeover_latency_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_owned());
        let last_handoff_at = metrics
            .last_handoff_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "n/a".to_owned());
        let owner_id = metrics.owner_id.clone().unwrap_or_else(|| "n/a".to_owned());
        let owner_epoch = metrics
            .owner_epoch
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_owned());
        format!(
            "adapter_ingress_driver={}\nadapter_id={}\nadapter_topic={}\nadapter_topic_partitions={}\nadapter_processed_count={}\nadapter_failed_count={}\nadapter_final_lag_records={}\nadapter_max_lag_records={}\nadapter_handoff_count={}\nadapter_last_takeover_latency_ms={}\nadapter_last_handoff_at={}\nadapter_owner_id={}\nadapter_owner_epoch={}\n",
            metrics.ingress_driver,
            metrics.adapter_id,
            metrics.topic_name,
            metrics.topic_partitions,
            metrics.processed_count,
            metrics.failed_count,
            metrics.final_lag_records,
            metrics.max_lag_records,
            metrics.ownership_handoff_count,
            last_takeover_latency_ms,
            last_handoff_at,
            owner_id,
            owner_epoch,
        )
    }).unwrap_or_default();
    format!(
        "scenario={scenario}\nprofile={profile}\nexecution_mode={execution_mode}\nthroughput_backend={throughput_backend}\nbulk_reducer={bulk_reducer}\nretry_rate={retry_rate}\ncancel_rate={cancel_rate}\nretry_delay_ms={retry_delay_ms}\nchunk_size={chunk_size}\nworkflows={workflows}\nactivities_per_workflow={activities_per_workflow}\ntotal_activities={total_activities}\nexecution_duration_ms={execution_duration_ms}\nprojection_convergence_duration_ms={projection_convergence_duration_ms}\nduration_ms={duration_ms}\nactivity_throughput_per_second={throughput:.2}\ncompleted_workflows={completed_workflows}\nfailed_workflows={failed_workflows}\nworkflow_task_rows={workflow_task_rows}\nresume_rows={resume_rows}\nresume_events_per_task_row={resume_ratio:.2}\nbulk_batch_rows={bulk_batch_rows}\nbulk_chunk_rows={bulk_chunk_rows}\nprojection_batch_rows={projection_batch_rows}\nprojection_chunk_rows={projection_chunk_rows}\nmax_aggregation_group_count={max_aggregation_group_count}\ngrouped_batch_rows={grouped_batch_rows}\nadmission_backend_counts={backend_counts}\nadmission_routing_reason_counts={routing_reason_counts}\nadmission_policy_versions={admission_policy_versions}\nmax_workflow_backlog={max_workflow_backlog}\nmax_activity_backlog={max_activity_backlog}\nfinal_workflow_backlog={final_workflow_backlog}\nfinal_activity_backlog={final_activity_backlog}\navg_activity_schedule_to_start_ms={avg_schedule:.2}\navg_activity_start_to_close_ms={avg_close:.2}\n{failover}{adapter}{control_plane}{executor_debug_delta}",
        scenario = report.scenario,
        profile = report.profile,
        execution_mode = match report.execution_mode {
            ExecutionMode::Durable => "durable",
            ExecutionMode::Throughput => "throughput",
            ExecutionMode::Unified => "unified",
        },
        throughput_backend = report.throughput_backend.as_deref().unwrap_or("n/a"),
        bulk_reducer = report.bulk_reducer,
        retry_rate = report.retry_rate,
        cancel_rate = report.cancel_rate,
        retry_delay_ms = report.retry_delay_ms,
        chunk_size = report.chunk_size,
        workflows = report.workflow_count,
        activities_per_workflow = report.activities_per_workflow,
        total_activities = report.total_activities,
        execution_duration_ms = report.execution_duration_ms,
        projection_convergence_duration_ms = report.projection_convergence_duration_ms,
        duration_ms = report.duration_ms,
        throughput = report.activity_metrics.throughput_activities_per_second,
        completed_workflows = report.workflow_outcomes.completed,
        failed_workflows = report.workflow_outcomes.failed,
        workflow_task_rows = report.coalescing_metrics.workflow_task_rows,
        resume_rows = report.coalescing_metrics.resume_rows,
        resume_ratio = report.coalescing_metrics.resume_events_per_task_row,
        bulk_batch_rows = report.bulk_batch_rows,
        bulk_chunk_rows = report.bulk_chunk_rows,
        projection_batch_rows = report.projection_batch_rows,
        projection_chunk_rows = report.projection_chunk_rows,
        max_aggregation_group_count = report.max_aggregation_group_count,
        grouped_batch_rows = report.grouped_batch_rows,
        backend_counts = backend_counts,
        routing_reason_counts = routing_reason_counts,
        admission_policy_versions = admission_policy_versions,
        max_workflow_backlog = report.backlog_metrics.max_workflow_backlog,
        max_activity_backlog = report.backlog_metrics.max_activity_backlog,
        final_workflow_backlog = report.backlog_metrics.final_workflow_backlog,
        final_activity_backlog = report.backlog_metrics.final_activity_backlog,
        avg_schedule = report.activity_metrics.avg_schedule_to_start_latency_ms,
        avg_close = report.activity_metrics.avg_start_to_close_latency_ms,
        failover = failover,
        adapter = adapter,
        control_plane = control_plane,
        executor_debug_delta = executor_debug_delta,
    )
}

fn format_counts(counts: &BTreeMap<String, u64>) -> String {
    if counts.is_empty() {
        return "none".to_owned();
    }
    counts.iter().map(|(key, value)| format!("{key}:{value}")).collect::<Vec<_>>().join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn demo_args() -> Args {
        Args {
            suite_name: None,
            scenario_tag: None,
            profile_name: "target".to_owned(),
            profile: BenchmarkProfile { workflow_count: 100, activities_per_workflow: 1_000 },
            output_path: PathBuf::from("target/benchmark-reports/demo.json"),
            worker_count: 8,
            payload_size: 128,
            retry_rate: 0.0,
            cancel_rate: 0.0,
            retry_delay_ms: 1_000,
            tenant_id: "benchmark".to_owned(),
            task_queue: "default".to_owned(),
            execution_mode: ExecutionMode::Throughput,
            throughput_backend: Some(STREAM_V2_BACKEND.to_owned()),
            ingress_driver: BenchmarkIngressDriver::HttpTrigger,
            workload_kind: BenchmarkWorkloadKind::Fanout,
            activity_type: BENCHMARK_ECHO_ACTIVITY.to_owned(),
            bulk_reducer: "collect_results".to_owned(),
            chunk_size: 100,
            timer_secs: 1,
            continue_rounds: 1,
            timeout: Duration::from_secs(30),
        }
    }

    #[test]
    fn scenario_name_includes_reducer_and_rates_for_robustness_variants() {
        let mut args = demo_args();
        args.bulk_reducer = "all_settled".to_owned();
        args.retry_rate = 0.01;
        args.cancel_rate = 0.01;

        assert_eq!(
            scenario_name(&args),
            "throughput-stream-v2-all-settled-retry-100bp-cancel-100bp"
        );
    }

    #[test]
    fn benchmark_definition_id_distinguishes_suite_variants() {
        let durable = Args {
            execution_mode: ExecutionMode::Durable,
            throughput_backend: None,
            ..demo_args()
        };
        let stream = demo_args();

        let durable_id = benchmark_definition_id(&durable, &scenario_name(&durable));
        let stream_id = benchmark_definition_id(&stream, &scenario_name(&stream));

        assert_ne!(durable_id, stream_id);
        assert_eq!(durable_id, "fanout-benchmark-target-durable");
        assert_eq!(stream_id, "fanout-benchmark-target-throughput-stream-v2");
    }

    #[test]
    fn durable_reducer_variant_is_reflected_in_scenario_and_artifact() {
        let mut durable = Args {
            execution_mode: ExecutionMode::Durable,
            throughput_backend: None,
            ..demo_args()
        };
        durable.bulk_reducer = "all_settled".to_owned();

        assert_eq!(scenario_name(&durable), "durable-all-settled");

        let artifact =
            benchmark_artifact("fanout-benchmark-target-durable-all-settled", "default", &durable);
        match artifact.workflow.states.get("dispatch") {
            Some(CompiledStateNode::FanOut { reducer, .. }) => {
                assert_eq!(reducer.as_deref(), Some("all_settled"));
            }
            other => panic!("expected fanout dispatch state, got {other:?}"),
        }
    }

    #[test]
    fn signal_gate_artifact_waits_for_signal_before_dispatch() {
        let mut args = Args {
            execution_mode: ExecutionMode::Durable,
            throughput_backend: None,
            ..demo_args()
        };
        args.workload_kind = BenchmarkWorkloadKind::SignalGate;

        let artifact = benchmark_artifact("signal-gate-benchmark", "default", &args);
        assert!(matches!(
            artifact.workflow.states.get("wait_signal"),
            Some(CompiledStateNode::WaitForCondition { .. })
        ));
        assert!(artifact.signals.contains_key("approve"));
    }

    #[test]
    fn throughput_artifact_uses_backend_specific_execution_policy() {
        let mut pg_args = demo_args();
        pg_args.execution_mode = ExecutionMode::Throughput;
        pg_args.throughput_backend = Some(PG_V1_BACKEND.to_owned());
        let pg_artifact =
            benchmark_artifact("fanout-benchmark-target-throughput-pg-v1", "default", &pg_args);

        let mut stream_args = demo_args();
        stream_args.execution_mode = ExecutionMode::Throughput;
        stream_args.throughput_backend = Some(STREAM_V2_BACKEND.to_owned());
        let stream_artifact = benchmark_artifact(
            "fanout-benchmark-target-throughput-stream-v2",
            "default",
            &stream_args,
        );

        let pg_dispatch = pg_artifact.workflow.states.get("dispatch").expect("pg dispatch state");
        let stream_dispatch =
            stream_artifact.workflow.states.get("dispatch").expect("stream dispatch state");

        match pg_dispatch {
            CompiledStateNode::StartBulkActivity {
                execution_policy, throughput_backend, ..
            } => {
                assert_eq!(execution_policy.as_deref(), None);
                assert_eq!(throughput_backend.as_deref(), Some(PG_V1_BACKEND));
            }
            other => panic!("expected bulk dispatch state, got {other:?}"),
        }

        match stream_dispatch {
            CompiledStateNode::StartBulkActivity {
                execution_policy, throughput_backend, ..
            } => {
                assert_eq!(execution_policy.as_deref(), Some("eager"));
                assert_eq!(throughput_backend.as_deref(), Some(STREAM_V2_BACKEND));
            }
            other => panic!("expected bulk dispatch state, got {other:?}"),
        }
    }

    #[test]
    fn stream_v2_robustness_suite_uses_all_settled_variant() {
        let scenarios =
            benchmark_suite_scenarios(&demo_args(), "stream-v2-robustness").expect("suite");
        assert_eq!(scenarios.len(), 1);
        assert_eq!(scenarios[0].execution_mode, ExecutionMode::Throughput);
        assert_eq!(scenarios[0].throughput_backend.as_deref(), Some(STREAM_V2_BACKEND));
        assert_eq!(scenarios[0].bulk_reducer, "all_settled");
    }

    #[test]
    fn stream_v2_fast_lane_suite_emits_mergeable_variants() {
        let scenarios =
            benchmark_suite_scenarios(&demo_args(), "stream-v2-fast-lane").expect("suite");
        assert_eq!(scenarios.len(), 2);
        assert_eq!(scenarios[0].bulk_reducer, "count");
        assert_eq!(scenarios[1].bulk_reducer, "all_settled");
        assert_eq!(scenarios[0].throughput_backend.as_deref(), Some(STREAM_V2_BACKEND));
        assert!(scenarios[0].profile.activities_per_workflow >= 2_000);
        assert!(scenarios[1].retry_rate >= 0.01);
        assert!(scenarios[1].cancel_rate >= 0.01);
    }

    #[test]
    fn stream_v2_fast_ceiling_suite_uses_native_fast_count_activity() {
        let scenarios =
            benchmark_suite_scenarios(&demo_args(), "stream-v2-fast-ceiling").expect("suite");
        assert_eq!(scenarios.len(), 1);
        assert_eq!(scenarios[0].throughput_backend.as_deref(), Some(STREAM_V2_BACKEND));
        assert_eq!(scenarios[0].activity_type, BENCHMARK_FAST_COUNT_ACTIVITY);
        assert_eq!(scenarios[0].bulk_reducer, "count");
        assert_eq!(scenarios[0].payload_size, 0);
        assert_eq!(scenarios[0].scenario_tag.as_deref(), Some("fast-ceiling"));
        assert!(scenarios[0].profile.activities_per_workflow >= 4_096);
    }

    #[test]
    fn benchmark_input_compacts_non_materializing_stream_v2_reducers() {
        let mut args = demo_args();
        args.bulk_reducer = "all_settled".to_owned();

        let input = benchmark_input(&args, 4_096, 128, 0.0, 0.0, "instance-a");
        assert_eq!(input.get("items"), Some(&benchmark_compact_input_spec(4_096)));
    }

    #[test]
    fn stream_v2_failover_suite_emits_owner_restart_variant() {
        let scenarios =
            benchmark_suite_scenarios(&demo_args(), "stream-v2-failover").expect("suite");
        assert_eq!(scenarios.len(), 2);
        assert_eq!(scenarios[0].throughput_backend.as_deref(), Some(STREAM_V2_BACKEND));
        assert_eq!(scenarios[0].bulk_reducer, "all_settled");
        assert_eq!(scenarios[0].scenario_tag.as_deref(), Some("owner-restart"));
        assert!(scenarios[0].profile.workflow_count >= 12);
        assert!(scenarios[0].profile.activities_per_workflow >= 4_096);
        assert_eq!(scenarios[1].scenario_tag.as_deref(), Some("owner-restart-retry-cancel"));
        assert!(scenarios[1].retry_rate >= 0.01);
        assert!(scenarios[1].cancel_rate >= 0.01);
    }

    #[test]
    fn streaming_admission_suite_emits_auto_routing_variants() {
        let scenarios =
            benchmark_suite_scenarios(&demo_args(), "streaming-admission").expect("suite");
        assert_eq!(scenarios.len(), 3);
        assert!(scenarios.iter().all(|scenario| scenario.throughput_backend.is_none()));
        assert_eq!(scenarios[0].scenario_tag.as_deref(), Some("auto-small"));
        assert_eq!(scenarios[1].scenario_tag.as_deref(), Some("auto-large"));
        assert_eq!(scenarios[2].scenario_tag.as_deref(), Some("auto-materialized"));
        assert_eq!(scenario_name(&scenarios[1]), "throughput-auto-auto-large-all-settled");
    }

    #[test]
    fn streaming_reducers_suite_emits_backend_pairs_for_numeric_reducers() {
        let scenarios =
            benchmark_suite_scenarios(&demo_args(), "streaming-reducers").expect("suite");
        assert_eq!(scenarios.len(), 14);
        assert_eq!(scenarios[0].bulk_reducer, "sum");
        assert_eq!(scenarios[0].throughput_backend.as_deref(), Some(PG_V1_BACKEND));
        assert_eq!(scenarios[1].bulk_reducer, "sum");
        assert_eq!(scenarios[1].throughput_backend.as_deref(), Some(STREAM_V2_BACKEND));
        assert_eq!(scenarios[8].bulk_reducer, "histogram");
        assert_eq!(scenarios[10].bulk_reducer, "sample_errors");
        assert_eq!(scenarios[10].scenario_tag.as_deref(), Some("sample-errors-cancel-only-pg-v1"));
        assert_eq!(scenarios[10].retry_rate, 0.0);
        assert!(scenarios[10].cancel_rate >= 0.01);
        assert_eq!(scenarios[11].scenario_tag.as_deref(), Some("sample-errors-retry-cancel-pg-v1"));
        assert!(scenarios[11].retry_rate >= 0.01);
        assert!(scenarios[11].cancel_rate >= 0.01);
        assert!(scenarios[11].retry_delay_ms <= 25);
        assert_eq!(
            scenarios[12].scenario_tag.as_deref(),
            Some("sample-errors-cancel-only-stream-v2")
        );
        assert_eq!(scenarios[12].retry_rate, 0.0);
        assert!(scenarios[12].cancel_rate >= 0.01);
        assert_eq!(
            scenarios[13].scenario_tag.as_deref(),
            Some("sample-errors-retry-cancel-stream-v2")
        );
        assert!(scenarios[13].retry_rate >= 0.01);
        assert!(scenarios[13].cancel_rate >= 0.01);
        assert!(scenarios[13].retry_delay_ms <= 25);
        assert!(scenarios.iter().all(|scenario| scenario.profile.activities_per_workflow >= 1_024));
    }

    #[test]
    fn topic_adapters_suite_emits_start_and_signal_variants() {
        let scenarios = benchmark_suite_scenarios(&demo_args(), "topic-adapters").expect("suite");
        assert_eq!(scenarios.len(), 2);
        assert_eq!(scenarios[0].ingress_driver, BenchmarkIngressDriver::TopicAdapterStart);
        assert_eq!(scenarios[0].workload_kind, BenchmarkWorkloadKind::Fanout);
        assert_eq!(scenarios[0].scenario_tag.as_deref(), Some("start-workflow"));
        assert_eq!(scenarios[1].ingress_driver, BenchmarkIngressDriver::TopicAdapterSignal);
        assert_eq!(scenarios[1].workload_kind, BenchmarkWorkloadKind::SignalGate);
        assert_eq!(scenarios[1].scenario_tag.as_deref(), Some("signal-workflow"));
        assert_eq!(
            scenario_name(&scenarios[0]),
            "throughput-stream-v2-adapter-start-start-workflow-all-settled"
        );
    }

    #[test]
    fn topic_adapters_failover_suite_emits_owner_crash_variants() {
        let scenarios =
            benchmark_suite_scenarios(&demo_args(), "topic-adapters-failover").expect("suite");
        assert_eq!(scenarios.len(), 2);
        assert_eq!(scenarios[0].ingress_driver, BenchmarkIngressDriver::TopicAdapterStart);
        assert_eq!(scenarios[0].scenario_tag.as_deref(), Some("owner-crash"));
        assert!(scenarios[0].profile.workflow_count >= 12);
        assert!(scenarios[0].profile.activities_per_workflow >= 4_096);
        assert_eq!(
            scenarios[1].scenario_tag.as_deref(),
            Some("lag-under-load-owner-crash")
        );
        assert!(scenarios[1].profile.workflow_count >= 256);
        assert!(scenarios[1].profile.activities_per_workflow >= 512);
    }

    #[test]
    fn numeric_reducer_inputs_include_numeric_reducer_value() {
        let mut args = demo_args();
        args.bulk_reducer = "sum".to_owned();

        let input = benchmark_input(&args, 3, 8, 0.0, 0.0, "instance-1");
        let items = input.get("items").and_then(Value::as_array).expect("items array");

        assert_eq!(items[0].get("reducer_value"), Some(&json!(1)));
        assert_eq!(items[2].get("reducer_value"), Some(&json!(3)));
    }

    #[test]
    fn histogram_reducer_inputs_include_bucket_values() {
        let mut args = demo_args();
        args.bulk_reducer = "histogram".to_owned();

        let input = benchmark_input(&args, 4, 8, 0.0, 0.0, "instance-1");
        let items = input.get("items").and_then(Value::as_array).expect("items array");

        assert_eq!(items[0].get("reducer_value"), Some(&json!("alpha")));
        assert_eq!(items[1].get("reducer_value"), Some(&json!("beta")));
        assert_eq!(items[3].get("reducer_value"), Some(&json!("delta")));
    }

    #[test]
    fn non_numeric_reducer_inputs_preserve_object_outputs() {
        let args = demo_args();

        let input = benchmark_input(&args, 2, 8, 0.0, 0.0, "instance-1");
        let items = input.get("items").and_then(Value::as_array).expect("items array");

        assert_eq!(items[0].get("reducer_value"), None);
    }

    #[test]
    fn format_counts_renders_compact_summary() {
        let counts = BTreeMap::from([("pg-v1".to_owned(), 2_u64), ("stream-v2".to_owned(), 5_u64)]);
        assert_eq!(format_counts(&counts), "pg-v1:2,stream-v2:5");
        assert_eq!(format_counts(&BTreeMap::new()), "none");
    }

    #[test]
    fn stream_projection_convergence_requires_terminal_batch_rows() {
        let row = StreamProjectionConvergenceRow {
            projection_batch_rows: 2,
            terminal_batch_rows: 1,
            inferred_terminal_batches: 2,
            batch_accounted_items: 128,
            interrupted_batch_missing_items: 0,
            completed_items: 128,
            failed_items: 0,
            cancelled_items: 0,
            terminal_chunk_rows: 16,
            pending_chunks: 0,
        };

        assert!(!stream_projection_has_converged(&row, 2, 128));
        assert_eq!(stream_projection_accounted_items(&row), 128);
    }

    #[test]
    fn summary_text_renders_failover_metrics_when_present() {
        let report = BenchmarkReport {
            scenario: "throughput-stream-v2-owner-restart-all-settled".to_owned(),
            profile: "gate".to_owned(),
            started_at: Utc::now(),
            execution_completed_at: Utc::now(),
            completed_at: Utc::now(),
            execution_duration_ms: 100,
            projection_convergence_duration_ms: 25,
            duration_ms: 125,
            workflow_count: 12,
            activities_per_workflow: 4_096,
            total_activities: 49_152,
            worker_count: 8,
            payload_size: 128,
            retry_rate: 0.0,
            cancel_rate: 0.0,
            retry_delay_ms: 1_000,
            definition_id: "bench".to_owned(),
            task_queue: "default".to_owned(),
            execution_mode: ExecutionMode::Throughput,
            throughput_backend: Some(STREAM_V2_BACKEND.to_owned()),
            bulk_reducer: "all_settled".to_owned(),
            chunk_size: 64,
            instance_prefix: "instance".to_owned(),
            workflow_outcomes: WorkflowOutcomeMetrics {
                completed: 12,
                failed: 0,
                cancelled: 0,
                running: 0,
            },
            activity_metrics: ActivityMetrics {
                completed: 49_152,
                failed: 0,
                cancelled: 0,
                timed_out: 0,
                avg_schedule_to_start_latency_ms: 1.0,
                max_schedule_to_start_latency_ms: 2,
                avg_start_to_close_latency_ms: 3.0,
                max_start_to_close_latency_ms: 4,
                throughput_activities_per_second: 10_000.0,
            },
            coalescing_metrics: CoalescingMetrics {
                workflow_task_rows: 12,
                resume_rows: 12,
                resume_events_per_task_row: 1.0,
            },
            backlog_metrics: BacklogMetrics {
                final_workflow_backlog: 0,
                final_activity_backlog: 0,
                max_workflow_backlog: 5,
                max_activity_backlog: 10,
            },
            bulk_batch_rows: 12,
            bulk_chunk_rows: 768,
            projection_batch_rows: 12,
            projection_chunk_rows: 768,
            max_aggregation_group_count: 4,
            grouped_batch_rows: 12,
            executor_debug: json!({}),
            executor_debug_before: None,
            executor_debug_after: None,
            executor_debug_delta: None,
            throughput_runtime_debug_before: None,
            throughput_runtime_debug: None,
            throughput_runtime_debug_delta: None,
            throughput_projector_debug_before: None,
            throughput_projector_debug: None,
            throughput_projector_debug_delta: None,
            batch_routing_metrics: BatchRoutingMetrics::default(),
            failover_injection: Some(FailoverInjectionMetrics {
                status: "completed".to_owned(),
                delay_ms: 1_000,
                stop_requested_at_ms: Some(10),
                stop_completed_at_ms: Some(20),
                restart_started_at_ms: Some(20),
                restart_ready_at_ms: Some(180),
                downtime_ms: Some(160),
                error: None,
            }),
            adapter_metrics: None,
            control_plane_metrics: None,
            executor_debug_delta_metrics: None,
        };

        let summary = summary_text(&report);
        assert!(summary.contains("failover_status=completed"));
        assert!(summary.contains("failover_downtime_ms=160"));
    }

    #[test]
    fn child_workflow_scope_excludes_child_instances_from_parent_metrics() {
        assert_eq!(
            instance_exclusion_pattern(
                BenchmarkWorkloadKind::ChildWorkflow,
                "fanout-smoke-unified-1234"
            ),
            Some("fanout-smoke-unified-1234%-child".to_owned())
        );
        assert_eq!(
            instance_exclusion_pattern(BenchmarkWorkloadKind::Fanout, "fanout-smoke-unified-1234"),
            None
        );
    }

    #[test]
    fn json_numeric_delta_recurses_over_nested_numeric_objects() {
        let before = json!({
            "runtime": {
                "poll_requests": 10,
                "leased_tasks": 5,
                "label": "ignored"
            }
        });
        let after = json!({
            "runtime": {
                "poll_requests": 16,
                "leased_tasks": 9,
                "label": "ignored"
            }
        });

        let delta = json_numeric_delta(&before, &after);
        assert_eq!(delta["runtime"]["poll_requests"], json!(6));
        assert_eq!(delta["runtime"]["leased_tasks"], json!(4));
        assert!(delta["runtime"].get("label").is_none());
    }

    #[test]
    fn unified_executor_debug_delta_metrics_are_derived_from_delta_snapshot() {
        let delta = json!({
            "runtime": {
                "poll_requests": 12,
                "leased_tasks": 24,
                "report_rpcs_received": 8,
                "report_batches_applied": 4,
                "log_writes": 3,
                "snapshot_writes": 1
            }
        });
        let activity_metrics = ActivityMetrics {
            completed: 20,
            failed: 2,
            cancelled: 2,
            timed_out: 0,
            avg_schedule_to_start_latency_ms: 0.0,
            max_schedule_to_start_latency_ms: 0,
            avg_start_to_close_latency_ms: 0.0,
            max_start_to_close_latency_ms: 0,
            throughput_activities_per_second: 0.0,
        };

        let metrics =
            executor_debug_delta_metrics(Some(&delta), &activity_metrics).expect("delta metrics");

        assert_eq!(metrics.polls_per_leased_task, 0.5);
        assert_eq!(metrics.report_rpcs_per_completed_activity, 8.0 / 24.0);
        assert_eq!(metrics.report_batches_per_completed_activity, 4.0 / 24.0);
        assert_eq!(metrics.log_writes, 3);
        assert_eq!(metrics.snapshot_writes, 1);
    }

    #[test]
    fn throughput_control_plane_metrics_are_derived_from_delta_snapshot() {
        let runtime_delta = json!({
            "runtime": {
                "poll_requests": 4,
                "leased_tasks": 20,
                "report_rpcs_received": 5,
                "reports_received": 15,
                "report_batches_applied": 3,
                "projection_events_published": 2,
                "projection_events_skipped": 8,
                "projection_events_applied_directly": 10,
                "changelog_entries_published": 4
            }
        });
        let projector_delta = json!({
            "manifest_writes": 6
        });
        let activity_metrics = ActivityMetrics {
            completed: 12,
            failed: 2,
            cancelled: 1,
            timed_out: 0,
            avg_schedule_to_start_latency_ms: 0.0,
            max_schedule_to_start_latency_ms: 0,
            avg_start_to_close_latency_ms: 0.0,
            max_start_to_close_latency_ms: 0,
            throughput_activities_per_second: 0.0,
        };

        let metrics =
            control_plane_metrics(Some(&runtime_delta), Some(&projector_delta), &activity_metrics)
                .expect("control plane metrics");

        assert_eq!(metrics.avg_tasks_per_bulk_poll_response, 5.0);
        assert_eq!(metrics.avg_results_per_bulk_report_rpc, 3.0);
        assert_eq!(metrics.report_batches_applied, 3);
        assert_eq!(metrics.avg_report_batch_size, 5.0);
        assert_eq!(metrics.changelog_entries_per_completed_chunk, 4.0 / 15.0);
        assert_eq!(metrics.projection_events_per_completed_chunk, 2.0 / 15.0);
        assert_eq!(metrics.manifest_writes, 6);
    }

    #[test]
    fn unified_defaults_collect_results_to_all_settled() {
        let reducer = normalize_bulk_reducer_for_execution_mode(
            ExecutionMode::Unified,
            "collect_results".to_owned(),
            false,
        )
        .expect("normalized reducer");

        assert_eq!(reducer, "all_settled");
    }

    #[test]
    fn unified_rejects_unsupported_reducers() {
        let error = normalize_bulk_reducer_for_execution_mode(
            ExecutionMode::Unified,
            "collect_results".to_owned(),
            true,
        )
        .expect_err("unsupported reducer should fail");

        assert!(error.to_string().contains("all_settled or count"));
    }
}
