use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use fabrik_config::PostgresConfig;
use fabrik_throughput::{PG_V1_BACKEND, STREAM_V2_BACKEND};
use fabrik_workflow::{
    ArtifactEntrypoint, Assignment, CompiledStateNode, CompiledWorkflow, CompiledWorkflowArtifact,
    ErrorTransition, Expression, RetryPolicy,
};
use reqwest::Client;
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

const DEFAULT_POLL_INTERVAL_MS: u64 = 250;
const DEFAULT_TIMEOUT_SECS: u64 = 300;
const DEFAULT_STREAM_PROJECTION_TIMEOUT_SECS: u64 = 30;

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
    tenant_id: String,
    task_queue: String,
    execution_mode: ExecutionMode,
    throughput_backend: Option<String>,
    workload_kind: BenchmarkWorkloadKind,
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
    throughput_runtime_debug: Option<Value>,
    throughput_projector_debug: Option<Value>,
    batch_routing_metrics: BatchRoutingMetrics,
    control_plane_metrics: Option<ControlPlaneMetrics>,
    executor_debug_delta_metrics: Option<ExecutorDebugDeltaMetrics>,
}

#[derive(Debug, Serialize, Default)]
struct BatchRoutingMetrics {
    backend_counts: BTreeMap<String, u64>,
    routing_reason_counts: BTreeMap<String, u64>,
    admission_policy_version_counts: BTreeMap<String, u64>,
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
    batch_accounted_items: i64,
    completed_items: i64,
    failed_items: i64,
    cancelled_items: i64,
    terminal_chunk_rows: i64,
    pending_chunks: i64,
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

    let ingest_base =
        env::var("INGEST_SERVICE_URL").unwrap_or_else(|_| "http://127.0.0.1:3001".to_owned());
    let throughput_runtime_debug_base =
        env::var("THROUGHPUT_DEBUG_URL").unwrap_or_else(|_| "http://127.0.0.1:3006".to_owned());
    let throughput_projector_base =
        env::var("THROUGHPUT_PROJECTOR_URL").unwrap_or_else(|_| "http://127.0.0.1:3007".to_owned());
    let unified_runtime_debug_base = env::var("UNIFIED_RUNTIME_DEBUG_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:3008".to_owned());

    let scenario_name = scenario_name(args);
    let definition_id = benchmark_definition_id(args, &scenario_name);
    let instance_prefix =
        format!("fanout-{}-{}-{}", args.profile_name, scenario_name, Uuid::now_v7());
    let started_at = Utc::now();
    let started = Instant::now();
    let executor_debug_url = format!("{unified_runtime_debug_base}/debug/unified");
    let executor_debug_before = Some(fetch_optional_debug(&client, &executor_debug_url).await);

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
    let throughput_runtime_debug = if args.execution_mode == ExecutionMode::Throughput
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
    let throughput_projector_debug = if args.execution_mode == ExecutionMode::Throughput
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
    let (
        bulk_batch_rows,
        bulk_chunk_rows,
        projection_batch_rows,
        projection_chunk_rows,
        max_aggregation_group_count,
        grouped_batch_rows,
    ) = bulk_metrics(&pool, &args.tenant_id, &instance_prefix, args.workload_kind).await?;
    let control_plane_metrics = control_plane_metrics(
        throughput_runtime_debug.as_ref(),
        throughput_projector_debug.as_ref(),
        &activity_metrics,
    );
    let executor_debug_delta_metrics =
        executor_debug_delta_metrics(executor_debug_delta.as_ref(), &activity_metrics);

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
        throughput_runtime_debug,
        throughput_projector_debug,
        batch_routing_metrics,
        control_plane_metrics,
        executor_debug_delta_metrics,
    })
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
    let mut tenant_id = "benchmark".to_owned();
    let mut task_queue = "default".to_owned();
    let mut execution_mode = ExecutionMode::Durable;
    let mut throughput_backend = None;
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
                    "all_succeeded" | "all_settled" | "count" | "collect_results" => value,
                    other => bail!(
                        "unknown --bulk-reducer {other}; expected all_succeeded, all_settled, count, or collect_results"
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
        "smoke" => BenchmarkProfile { workflow_count: 10, activities_per_workflow: 100 },
        "target" => BenchmarkProfile { workflow_count: 100, activities_per_workflow: 1_000 },
        "stress" => BenchmarkProfile { workflow_count: 250, activities_per_workflow: 1_000 },
        other => bail!("unknown profile {other}; expected smoke, target, or stress"),
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
        tenant_id,
        task_queue,
        execution_mode,
        throughput_backend,
        workload_kind,
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
    match args.execution_mode {
        ExecutionMode::Durable | ExecutionMode::Unified => {
            states.insert(
                "dispatch".to_owned(),
                CompiledStateNode::FanOut {
                    activity_type: "benchmark.echo".to_owned(),
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
                        delay: "1s".to_owned(),
                        non_retryable_error_types: Vec::new(),
                    }),
                    config: None,
                    schedule_to_start_timeout_ms: None,
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
                    activity_type: "benchmark.echo".to_owned(),
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
                        delay: "1s".to_owned(),
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
    let payload = "x".repeat(payload_size);
    let mut object = serde_json::Map::from_iter([(
        "items".to_owned(),
        Value::Array(
            (0..activities_per_workflow)
                .map(|index| {
                    json!({
                        "index": index,
                        "payload": payload,
                        "fail_until_attempt": if index < retry_count { 1 } else { 0 },
                        "cancel": index >= retry_count && index < retry_count + cancel_count,
                    })
                })
                .collect(),
        ),
    )]);
    if args.workload_kind == BenchmarkWorkloadKind::ContinueAsNew {
        object.insert("remaining".to_owned(), json!(args.continue_rounds));
    }
    if args.workload_kind == BenchmarkWorkloadKind::ChildWorkflow {
        object
            .insert("child_instance_id".to_owned(), Value::String(format!("{instance_id}-child")));
    }
    Value::Object(object)
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
    let scenarios = benchmark_suite_scenarios(&args, &suite_name)?;
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
        other => bail!(
            "unknown suite {other}; expected streaming, stream-v2-robustness, stream-v2-fast-lane, or streaming-admission"
        ),
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
                (SELECT COALESCE(SUM(succeeded_items + failed_items + cancelled_items), 0)::bigint
                 FROM throughput_projection_batches
                 WHERE tenant_id = $1
                   AND workflow_instance_id LIKE $2
                   AND ($3::text IS NULL OR workflow_instance_id NOT LIKE $3)
                   AND status IN ('completed', 'failed', 'cancelled')) AS batch_accounted_items,
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
        let accounted_items = chunk_accounted_items.max(row.batch_accounted_items);
        if row.projection_batch_rows as u64 >= expected_batches
            && row.terminal_batch_rows as u64 >= expected_batches
            && accounted_items as u64 >= expected_items
        {
            return Ok(());
        }

        if started.elapsed() >= timeout {
            bail!(
                "timed out waiting for stream-v2 projection convergence: batches={}/{} terminal_batches={}/{} batch_items={}/{} chunk_items={}/{} pending_chunks={}",
                row.projection_batch_rows,
                expected_batches,
                row.terminal_batch_rows,
                expected_batches,
                row.batch_accounted_items,
                expected_items,
                chunk_accounted_items,
                expected_items,
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
    runtime_debug: Option<&Value>,
    projector_debug: Option<&Value>,
    activity_metrics: &ActivityMetrics,
) -> Option<ControlPlaneMetrics> {
    let runtime = runtime_debug?.get("runtime")?;
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
    let manifest_writes = projector_debug
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
    format!(
        "scenario={scenario}\nprofile={profile}\nexecution_mode={execution_mode}\nthroughput_backend={throughput_backend}\nbulk_reducer={bulk_reducer}\nretry_rate={retry_rate}\ncancel_rate={cancel_rate}\nchunk_size={chunk_size}\nworkflows={workflows}\nactivities_per_workflow={activities_per_workflow}\ntotal_activities={total_activities}\nexecution_duration_ms={execution_duration_ms}\nprojection_convergence_duration_ms={projection_convergence_duration_ms}\nduration_ms={duration_ms}\nactivity_throughput_per_second={throughput:.2}\ncompleted_workflows={completed_workflows}\nfailed_workflows={failed_workflows}\nworkflow_task_rows={workflow_task_rows}\nresume_rows={resume_rows}\nresume_events_per_task_row={resume_ratio:.2}\nbulk_batch_rows={bulk_batch_rows}\nbulk_chunk_rows={bulk_chunk_rows}\nprojection_batch_rows={projection_batch_rows}\nprojection_chunk_rows={projection_chunk_rows}\nmax_aggregation_group_count={max_aggregation_group_count}\ngrouped_batch_rows={grouped_batch_rows}\nadmission_backend_counts={backend_counts}\nadmission_routing_reason_counts={routing_reason_counts}\nadmission_policy_versions={admission_policy_versions}\nmax_workflow_backlog={max_workflow_backlog}\nmax_activity_backlog={max_activity_backlog}\nfinal_workflow_backlog={final_workflow_backlog}\nfinal_activity_backlog={final_activity_backlog}\navg_activity_schedule_to_start_ms={avg_schedule:.2}\navg_activity_start_to_close_ms={avg_close:.2}\n{control_plane}{executor_debug_delta}",
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
            tenant_id: "benchmark".to_owned(),
            task_queue: "default".to_owned(),
            execution_mode: ExecutionMode::Throughput,
            throughput_backend: Some(STREAM_V2_BACKEND.to_owned()),
            workload_kind: BenchmarkWorkloadKind::Fanout,
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
    fn format_counts_renders_compact_summary() {
        let counts = BTreeMap::from([("pg-v1".to_owned(), 2_u64), ("stream-v2".to_owned(), 5_u64)]);
        assert_eq!(format_counts(&counts), "pg-v1:2,stream-v2:5");
        assert_eq!(format_counts(&BTreeMap::new()), "none");
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
