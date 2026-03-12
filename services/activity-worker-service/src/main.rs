use std::{env, sync::Arc, time::Duration};

use anyhow::Result;
use fabrik_config::{GrpcServiceConfig, ThroughputPayloadStoreConfig, ThroughputPayloadStoreKind};
use fabrik_throughput::{
    PayloadHandle, PayloadStore, PayloadStoreConfig, PayloadStoreKind, decode_cbor, encode_cbor,
};
use fabrik_worker_protocol::activity_worker::{
    ActivityTaskCancelledResult, ActivityTaskCompletedResult, ActivityTaskFailedResult,
    ActivityTaskResult, BulkActivityTaskCancelledResult, BulkActivityTaskCompletedResult,
    BulkActivityTaskFailedResult, BulkActivityTaskResult, PollActivityTaskRequest,
    PollBulkActivityTaskRequest, ReportActivityTaskResultsRequest,
    ReportBulkActivityTaskResultsRequest, activity_task_result,
    activity_worker_api_client::ActivityWorkerApiClient,
};
use fabrik_workflow::{StepConfig, execute_handler};
use reqwest::{Client, Method, Response};
use serde_json::Value;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinSet;
use tracing::{error, info};
use uuid::Uuid;

const RESULT_BATCH_MAX_ITEMS: usize = 256;
const RESULT_BATCH_MAX_BYTES: usize = 1_048_576;
const RESULT_BATCH_FLUSH_INTERVAL_MS: u64 = 5;
const DEFAULT_ACTIVITY_WORKER_CONCURRENCY: usize = 8;
const DEFAULT_RESULT_FLUSHER_CONCURRENCY: usize = 1;
const DEFAULT_BULK_POLL_MAX_TASKS: u32 = 8;
const MAX_BULK_CHUNK_INPUT_BYTES: usize = 1024 * 1024;

#[tokio::main]
async fn main() -> Result<()> {
    let config =
        GrpcServiceConfig::from_env("ACTIVITY_WORKER_SERVICE", "activity-worker-service", 50052)?;
    fabrik_service::init_tracing(&config.log_filter);

    let endpoint = env::var("MATCHING_SERVICE_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:50051".to_owned());
    let bulk_endpoint = env::var("BULK_ACTIVITY_ENDPOINT").unwrap_or_else(|_| endpoint.clone());
    let task_queue = env::var("ACTIVITY_TASK_QUEUE").unwrap_or_else(|_| "default".to_owned());
    let tenant_id = env::var("ACTIVITY_WORKER_TENANT_ID").unwrap_or_default();
    let worker_id = env::var("ACTIVITY_WORKER_ID")
        .unwrap_or_else(|_| format!("activity-worker:{}", Uuid::now_v7()));
    let worker_build_id =
        env::var("ACTIVITY_WORKER_BUILD_ID").unwrap_or_else(|_| "dev-build".to_owned());
    let concurrency = env::var("ACTIVITY_WORKER_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_ACTIVITY_WORKER_CONCURRENCY);
    let result_flusher_concurrency = env::var("ACTIVITY_RESULT_FLUSHER_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_RESULT_FLUSHER_CONCURRENCY)
        .min(concurrency);
    let bulk_poll_max_tasks = env::var("ACTIVITY_BULK_POLL_MAX_TASKS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_BULK_POLL_MAX_TASKS);
    let client = Arc::new(Client::new());
    let payload_store = Arc::new(PayloadStore::from_config(build_payload_store_config()).await?);

    info!(
        matching_endpoint = %endpoint,
        bulk_endpoint = %bulk_endpoint,
        task_queue = %task_queue,
        tenant_id = %tenant_id,
        worker_id = %worker_id,
        worker_build_id = %worker_build_id,
        concurrency,
        result_flusher_concurrency,
        bulk_poll_max_tasks,
        port = config.port,
        "activity-worker-service starting"
    );

    let mut workers = JoinSet::new();
    let mut result_txs = Vec::with_capacity(result_flusher_concurrency);
    let mut bulk_result_txs = Vec::with_capacity(result_flusher_concurrency);
    for flusher_index in 0..result_flusher_concurrency {
        let (result_tx, result_rx) = unbounded_channel();
        workers.spawn(run_result_flusher(
            endpoint.clone(),
            format!("result-flusher-{flusher_index}"),
            result_rx,
        ));
        result_txs.push(result_tx);
        let (bulk_result_tx, bulk_result_rx) = unbounded_channel();
        workers.spawn(run_bulk_result_flusher(
            bulk_endpoint.clone(),
            format!("bulk-result-flusher-{flusher_index}"),
            payload_store.clone(),
            bulk_result_rx,
        ));
        bulk_result_txs.push(bulk_result_tx);
    }

    for lane in 0..concurrency {
        workers.spawn(run_activity_lane(
            endpoint.clone(),
            tenant_id.clone(),
            task_queue.clone(),
            lane_worker_id(&worker_id, lane, concurrency),
            worker_build_id.clone(),
            client.clone(),
            result_txs[lane % result_txs.len()].clone(),
        ));
        workers.spawn(run_bulk_activity_lane(
            bulk_endpoint.clone(),
            tenant_id.clone(),
            task_queue.clone(),
            lane_worker_id(&worker_id, lane, concurrency),
            worker_build_id.clone(),
            client.clone(),
            payload_store.clone(),
            bulk_poll_max_tasks,
            bulk_result_txs[lane % bulk_result_txs.len()].clone(),
        ));
    }
    drop(result_txs);
    drop(bulk_result_txs);

    while let Some(result) = workers.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(error) => return Err(anyhow::anyhow!("activity worker task failed: {error}")),
        }
    }

    Ok(())
}

fn should_flush_results(
    pending_results: &[ActivityTaskResult],
    pending_result_bytes: usize,
    first_pending_at: Option<std::time::Instant>,
) -> bool {
    !pending_results.is_empty()
        && (pending_results.len() >= RESULT_BATCH_MAX_ITEMS
            || pending_result_bytes >= RESULT_BATCH_MAX_BYTES
            || first_pending_at.is_some_and(|started| {
                started.elapsed() >= Duration::from_millis(RESULT_BATCH_FLUSH_INTERVAL_MS)
            }))
}

fn estimate_result_size(result: &ActivityTaskResult) -> usize {
    let mut size = result.tenant_id.len()
        + result.instance_id.len()
        + result.run_id.len()
        + result.activity_id.len()
        + result.worker_id.len()
        + result.worker_build_id.len()
        + std::mem::size_of::<u32>();
    size += match result.result.as_ref() {
        Some(activity_task_result::Result::Completed(completed)) => completed.output_json.len(),
        Some(activity_task_result::Result::Failed(failed)) => failed.error.len(),
        Some(activity_task_result::Result::Cancelled(cancelled)) => {
            cancelled.reason.len() + cancelled.metadata_json.len()
        }
        None => 0,
    };
    size
}

fn estimate_bulk_result_size(result: &BulkActivityTaskResult) -> usize {
    let mut size = result.tenant_id.len()
        + result.instance_id.len()
        + result.run_id.len()
        + result.batch_id.len()
        + result.chunk_id.len()
        + result.worker_id.len()
        + result.worker_build_id.len()
        + std::mem::size_of::<u32>() * 2;
    size += match result.result.as_ref() {
        Some(
            fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Completed(
                completed,
            ),
        ) => {
            if completed.result_handle_json.is_empty()
                && completed.result_handle_cbor.is_empty()
                && (!completed.output_json.is_empty() || !completed.output_cbor.is_empty())
            {
                256
            } else {
                completed.output_json.len()
                    + completed.result_handle_json.len()
                    + completed.output_cbor.len()
                    + completed.result_handle_cbor.len()
            }
        }
        Some(
            fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Failed(
                failed,
            ),
        ) => failed.error.len(),
        Some(
            fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Cancelled(
                cancelled,
            ),
        ) => cancelled.reason.len() + cancelled.metadata_json.len(),
        None => 0,
    };
    size
}

async fn flush_results(
    worker: &mut ActivityWorkerApiClient<tonic::transport::Channel>,
    pending_results: &mut Vec<ActivityTaskResult>,
    pending_result_bytes: &mut usize,
    first_pending_at: &mut Option<std::time::Instant>,
) -> Result<()> {
    if pending_results.is_empty() {
        return Ok(());
    }
    worker
        .report_activity_task_results(ReportActivityTaskResultsRequest {
            results: std::mem::take(pending_results),
        })
        .await?;
    *pending_result_bytes = 0;
    *first_pending_at = None;
    Ok(())
}

async fn flush_bulk_results(
    worker: &mut ActivityWorkerApiClient<tonic::transport::Channel>,
    payload_store: &PayloadStore,
    flusher_id: &str,
    pending_results: &mut Vec<BulkActivityTaskResult>,
    pending_result_bytes: &mut usize,
    first_pending_at: &mut Option<std::time::Instant>,
) -> Result<()> {
    if pending_results.is_empty() {
        return Ok(());
    }
    externalize_bulk_result_outputs(payload_store, flusher_id, pending_results).await?;
    worker
        .report_bulk_activity_task_results(ReportBulkActivityTaskResultsRequest {
            results: std::mem::take(pending_results),
        })
        .await?;
    *pending_result_bytes = 0;
    *first_pending_at = None;
    Ok(())
}

async fn run_result_flusher(
    endpoint: String,
    flusher_id: String,
    mut result_rx: UnboundedReceiver<ActivityTaskResult>,
) -> Result<()> {
    let mut worker = connect_activity_worker_with_retry(&endpoint, &flusher_id).await;
    let mut pending_results = Vec::<ActivityTaskResult>::new();
    let mut pending_result_bytes = 0usize;
    let mut first_pending_at = None;

    loop {
        if pending_results.is_empty() {
            let Some(result) = result_rx.recv().await else {
                return Ok(());
            };
            pending_result_bytes =
                pending_result_bytes.saturating_add(estimate_result_size(&result));
            pending_results.push(result);
            first_pending_at.get_or_insert_with(std::time::Instant::now);
        }

        if should_flush_results(&pending_results, pending_result_bytes, first_pending_at) {
            flush_results(
                &mut worker,
                &mut pending_results,
                &mut pending_result_bytes,
                &mut first_pending_at,
            )
            .await?;
            continue;
        }

        let wait = first_pending_at
            .map(|started| {
                Duration::from_millis(RESULT_BATCH_FLUSH_INTERVAL_MS)
                    .saturating_sub(started.elapsed())
            })
            .unwrap_or_else(|| Duration::from_millis(RESULT_BATCH_FLUSH_INTERVAL_MS));

        match tokio::time::timeout(wait, result_rx.recv()).await {
            Ok(Some(result)) => {
                pending_result_bytes =
                    pending_result_bytes.saturating_add(estimate_result_size(&result));
                pending_results.push(result);
            }
            Ok(None) => {
                flush_results(
                    &mut worker,
                    &mut pending_results,
                    &mut pending_result_bytes,
                    &mut first_pending_at,
                )
                .await?;
                return Ok(());
            }
            Err(_) => {
                flush_results(
                    &mut worker,
                    &mut pending_results,
                    &mut pending_result_bytes,
                    &mut first_pending_at,
                )
                .await?;
            }
        }
    }
}

async fn run_bulk_result_flusher(
    endpoint: String,
    flusher_id: String,
    payload_store: Arc<PayloadStore>,
    mut result_rx: UnboundedReceiver<BulkActivityTaskResult>,
) -> Result<()> {
    let mut worker = connect_activity_worker_with_retry(&endpoint, &flusher_id).await;
    let mut pending_results = Vec::<BulkActivityTaskResult>::new();
    let mut pending_result_bytes = 0usize;
    let mut first_pending_at = None;

    loop {
        if pending_results.is_empty() {
            let Some(result) = result_rx.recv().await else {
                return Ok(());
            };
            pending_result_bytes =
                pending_result_bytes.saturating_add(estimate_bulk_result_size(&result));
            pending_results.push(result);
            first_pending_at.get_or_insert_with(std::time::Instant::now);
        }

        if !pending_results.is_empty()
            && (pending_results.len() >= RESULT_BATCH_MAX_ITEMS
                || pending_result_bytes >= RESULT_BATCH_MAX_BYTES
                || first_pending_at.is_some_and(|started| {
                    started.elapsed() >= Duration::from_millis(RESULT_BATCH_FLUSH_INTERVAL_MS)
                }))
        {
            flush_bulk_results(
                &mut worker,
                payload_store.as_ref(),
                &flusher_id,
                &mut pending_results,
                &mut pending_result_bytes,
                &mut first_pending_at,
            )
            .await?;
            continue;
        }

        let wait = first_pending_at
            .map(|started| {
                Duration::from_millis(RESULT_BATCH_FLUSH_INTERVAL_MS)
                    .saturating_sub(started.elapsed())
            })
            .unwrap_or_else(|| Duration::from_millis(RESULT_BATCH_FLUSH_INTERVAL_MS));

        match tokio::time::timeout(wait, result_rx.recv()).await {
            Ok(Some(result)) => {
                pending_result_bytes =
                    pending_result_bytes.saturating_add(estimate_bulk_result_size(&result));
                pending_results.push(result);
            }
            Ok(None) => {
                flush_bulk_results(
                    &mut worker,
                    payload_store.as_ref(),
                    &flusher_id,
                    &mut pending_results,
                    &mut pending_result_bytes,
                    &mut first_pending_at,
                )
                .await?;
                return Ok(());
            }
            Err(_) => {
                flush_bulk_results(
                    &mut worker,
                    payload_store.as_ref(),
                    &flusher_id,
                    &mut pending_results,
                    &mut pending_result_bytes,
                    &mut first_pending_at,
                )
                .await?;
            }
        }
    }
}

async fn run_activity_lane(
    endpoint: String,
    tenant_id: String,
    task_queue: String,
    worker_id: String,
    worker_build_id: String,
    client: Arc<Client>,
    result_tx: UnboundedSender<ActivityTaskResult>,
) -> Result<()> {
    let mut worker = connect_activity_worker_with_retry(&endpoint, &worker_id).await;

    loop {
        let response = worker
            .poll_activity_task(PollActivityTaskRequest {
                tenant_id: tenant_id.clone(),
                task_queue: task_queue.clone(),
                worker_id: worker_id.clone(),
                worker_build_id: worker_build_id.clone(),
                poll_timeout_ms: 30_000,
            })
            .await;

        let Some(task) = (match response {
            Ok(response) => response.into_inner().task,
            Err(error) => {
                error!(error = %error, worker_id = %worker_id, "failed to poll matching-service");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        }) else {
            continue;
        };

        let activity_result = execute_activity_task(
            client.as_ref(),
            task,
            worker_id.clone(),
            worker_build_id.clone(),
        )
        .await?;
        if result_tx.send(activity_result).is_err() {
            anyhow::bail!("activity result flusher channel closed");
        }
    }
}

async fn run_bulk_activity_lane(
    endpoint: String,
    tenant_id: String,
    task_queue: String,
    worker_id: String,
    worker_build_id: String,
    client: Arc<Client>,
    payload_store: Arc<PayloadStore>,
    bulk_poll_max_tasks: u32,
    result_tx: UnboundedSender<BulkActivityTaskResult>,
) -> Result<()> {
    let mut worker = connect_activity_worker_with_retry(&endpoint, &worker_id).await;

    loop {
        let response = worker
            .poll_bulk_activity_task(PollBulkActivityTaskRequest {
                tenant_id: tenant_id.clone(),
                task_queue: task_queue.clone(),
                worker_id: worker_id.clone(),
                worker_build_id: worker_build_id.clone(),
                poll_timeout_ms: 30_000,
                max_tasks: bulk_poll_max_tasks,
                supports_cbor: true,
            })
            .await;

        let tasks = match response {
            Ok(response) => response.into_inner().tasks,
            Err(error) => {
                error!(error = %error, worker_id = %worker_id, "failed to poll matching-service for bulk task");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        if tasks.is_empty() {
            continue;
        }

        for task in tasks {
            let activity_result = execute_bulk_activity_task(
                client.as_ref(),
                payload_store.as_ref(),
                task,
                worker_id.clone(),
                worker_build_id.clone(),
            )
            .await?;
            if result_tx.send(activity_result).is_err() {
                anyhow::bail!("bulk activity result flusher channel closed");
            }
        }
    }
}

fn encode_bulk_completed_result(outputs: &[Value]) -> Result<BulkActivityTaskCompletedResult> {
    Ok(BulkActivityTaskCompletedResult {
        output_json: String::new(),
        result_handle_json: String::new(),
        output_cbor: encode_cbor(&Value::Array(outputs.to_vec()), "bulk result output")?,
        result_handle_cbor: Vec::new(),
    })
}

async fn connect_activity_worker(
    endpoint: &str,
) -> Result<ActivityWorkerApiClient<tonic::transport::Channel>> {
    Ok(ActivityWorkerApiClient::connect(endpoint.to_owned()).await?)
}

async fn connect_activity_worker_with_retry(
    endpoint: &str,
    worker_id: &str,
) -> ActivityWorkerApiClient<tonic::transport::Channel> {
    loop {
        match connect_activity_worker(endpoint).await {
            Ok(worker) => return worker,
            Err(error) => {
                error!(
                    error = %error,
                    worker_id = %worker_id,
                    "failed to connect to matching-service"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

async fn execute_activity_task(
    client: &Client,
    task: fabrik_worker_protocol::activity_worker::ActivityTask,
    worker_id: String,
    worker_build_id: String,
) -> Result<ActivityTaskResult> {
    let result = execute_activity(
        client,
        &task.activity_type,
        task.attempt,
        parse_optional_json(&task.config_json)?,
        &parse_required_json(&task.input_json)?,
    )
    .await;

    Ok(match result {
        Ok(output) => ActivityTaskResult {
            tenant_id: task.tenant_id,
            instance_id: task.instance_id,
            run_id: task.run_id,
            activity_id: task.activity_id,
            attempt: task.attempt,
            worker_id,
            worker_build_id,
            result: Some(activity_task_result::Result::Completed(ActivityTaskCompletedResult {
                output_json: serde_json::to_string(&output)?,
            })),
        },
        Err(error) if error.to_string() == "activity cancelled" => ActivityTaskResult {
            tenant_id: task.tenant_id,
            instance_id: task.instance_id,
            run_id: task.run_id,
            activity_id: task.activity_id,
            attempt: task.attempt,
            worker_id,
            worker_build_id,
            result: Some(activity_task_result::Result::Cancelled(ActivityTaskCancelledResult {
                reason: "activity cancelled".to_owned(),
                metadata_json: String::new(),
            })),
        },
        Err(error) => ActivityTaskResult {
            tenant_id: task.tenant_id,
            instance_id: task.instance_id,
            run_id: task.run_id,
            activity_id: task.activity_id,
            attempt: task.attempt,
            worker_id,
            worker_build_id,
            result: Some(activity_task_result::Result::Failed(ActivityTaskFailedResult {
                error: error.to_string(),
            })),
        },
    })
}

async fn execute_bulk_activity_task(
    client: &Client,
    payload_store: &PayloadStore,
    task: fabrik_worker_protocol::activity_worker::BulkActivityTask,
    worker_id: String,
    worker_build_id: String,
) -> Result<BulkActivityTaskResult> {
    let input_handle = task_input_handle(&task)?;
    let items = resolve_bulk_task_items(payload_store, &task, input_handle.as_ref()).await?;
    if task.activity_type == "benchmark.echo" {
        return execute_benchmark_echo_bulk_task(task, worker_id, worker_build_id, items);
    }
    let mut outputs = Vec::with_capacity(items.len());
    for item in items {
        match execute_activity(client, &task.activity_type, task.attempt, None, &item).await {
            Ok(output) => outputs.push(output),
            Err(error) if error.to_string() == "activity cancelled" => {
                return Ok(BulkActivityTaskResult {
                    tenant_id: task.tenant_id,
                    instance_id: task.instance_id,
                    run_id: task.run_id,
                    batch_id: task.batch_id,
                    chunk_id: task.chunk_id,
                    chunk_index: task.chunk_index,
                    group_id: task.group_id,
                    attempt: task.attempt,
                    worker_id,
                    worker_build_id,
                    lease_token: task.lease_token,
                    lease_epoch: task.lease_epoch,
                    owner_epoch: task.owner_epoch,
                    report_id: Uuid::now_v7().to_string(),
                    result: Some(
                        fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Cancelled(
                            BulkActivityTaskCancelledResult {
                                reason: error.to_string(),
                                metadata_json: String::new(),
                                metadata_cbor: Vec::new(),
                            },
                        ),
                    ),
                });
            }
            Err(error) => {
                return Ok(BulkActivityTaskResult {
                    tenant_id: task.tenant_id,
                    instance_id: task.instance_id,
                    run_id: task.run_id,
                    batch_id: task.batch_id,
                    chunk_id: task.chunk_id,
                    chunk_index: task.chunk_index,
                    group_id: task.group_id,
                    attempt: task.attempt,
                    worker_id,
                    worker_build_id,
                    lease_token: task.lease_token,
                    lease_epoch: task.lease_epoch,
                    owner_epoch: task.owner_epoch,
                    report_id: Uuid::now_v7().to_string(),
                    result: Some(
                        fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Failed(
                            BulkActivityTaskFailedResult {
                                error: error.to_string(),
                            },
                        ),
                    ),
                });
            }
        }
    }

    Ok(BulkActivityTaskResult {
        tenant_id: task.tenant_id,
        instance_id: task.instance_id,
        run_id: task.run_id,
        batch_id: task.batch_id,
        chunk_id: task.chunk_id,
        chunk_index: task.chunk_index,
        group_id: task.group_id,
        attempt: task.attempt,
        worker_id,
        worker_build_id,
        lease_token: task.lease_token,
        lease_epoch: task.lease_epoch,
        owner_epoch: task.owner_epoch,
        report_id: Uuid::now_v7().to_string(),
        result: Some(
            fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Completed(
                encode_bulk_completed_result(&outputs)?,
            ),
        ),
    })
}

fn execute_benchmark_echo_bulk_task(
    task: fabrik_worker_protocol::activity_worker::BulkActivityTask,
    worker_id: String,
    worker_build_id: String,
    items: Vec<Value>,
) -> Result<BulkActivityTaskResult> {
    let mut outputs = Vec::with_capacity(items.len());
    for item in items {
        match execute_benchmark_echo(task.attempt, &item) {
            Ok(output) => outputs.push(output),
            Err(error) if error.to_string() == "activity cancelled" => {
                return Ok(BulkActivityTaskResult {
                    tenant_id: task.tenant_id,
                    instance_id: task.instance_id,
                    run_id: task.run_id,
                    batch_id: task.batch_id,
                    chunk_id: task.chunk_id,
                    chunk_index: task.chunk_index,
                    group_id: task.group_id,
                    attempt: task.attempt,
                    worker_id,
                    worker_build_id,
                    lease_token: task.lease_token,
                    lease_epoch: task.lease_epoch,
                    owner_epoch: task.owner_epoch,
                    report_id: Uuid::now_v7().to_string(),
                    result: Some(
                        fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Cancelled(
                            BulkActivityTaskCancelledResult {
                                reason: error.to_string(),
                                metadata_json: String::new(),
                                metadata_cbor: Vec::new(),
                            },
                        ),
                    ),
                });
            }
            Err(error) => {
                return Ok(BulkActivityTaskResult {
                    tenant_id: task.tenant_id,
                    instance_id: task.instance_id,
                    run_id: task.run_id,
                    batch_id: task.batch_id,
                    chunk_id: task.chunk_id,
                    chunk_index: task.chunk_index,
                    group_id: task.group_id,
                    attempt: task.attempt,
                    worker_id,
                    worker_build_id,
                    lease_token: task.lease_token,
                    lease_epoch: task.lease_epoch,
                    owner_epoch: task.owner_epoch,
                    report_id: Uuid::now_v7().to_string(),
                    result: Some(
                        fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Failed(
                            BulkActivityTaskFailedResult {
                                error: error.to_string(),
                            },
                        ),
                    ),
                });
            }
        }
    }

    Ok(BulkActivityTaskResult {
        tenant_id: task.tenant_id,
        instance_id: task.instance_id,
        run_id: task.run_id,
        batch_id: task.batch_id,
        chunk_id: task.chunk_id,
        chunk_index: task.chunk_index,
        group_id: task.group_id,
        attempt: task.attempt,
        worker_id,
        worker_build_id,
        lease_token: task.lease_token,
        lease_epoch: task.lease_epoch,
        owner_epoch: task.owner_epoch,
        report_id: Uuid::now_v7().to_string(),
        result: Some(
            fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Completed(
                encode_bulk_completed_result(&outputs)?,
            ),
        ),
    })
}

async fn externalize_bulk_result_outputs(
    payload_store: &PayloadStore,
    flusher_id: &str,
    pending_results: &mut [BulkActivityTaskResult],
) -> Result<()> {
    let mut flattened_outputs = Vec::new();
    let mut completions = Vec::new();

    for (index, result) in pending_results.iter_mut().enumerate() {
        let Some(
            fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Completed(
                completed,
            ),
        ) = result.result.as_mut()
        else {
            continue;
        };
        if (!completed.output_json.trim().is_empty() || !completed.output_cbor.is_empty())
            && (!completed.result_handle_json.trim().is_empty()
                || !completed.result_handle_cbor.is_empty())
        {
            continue;
        }
        if completed.output_json.trim().is_empty() && completed.output_cbor.is_empty() {
            continue;
        }

        let output = if !completed.output_cbor.is_empty() {
            match decode_cbor::<Value>(&completed.output_cbor, "bulk result output")? {
                Value::Array(items) => items,
                other => {
                    anyhow::bail!("bulk result output CBOR must decode to an array, got {other}")
                }
            }
        } else {
            serde_json::from_str::<Vec<Value>>(&completed.output_json)?
        };
        let start = flattened_outputs.len();
        let len = output.len();
        flattened_outputs.extend(output);
        completions.push((index, start, len));
    }

    if completions.is_empty() {
        return Ok(());
    }

    let handle = payload_store
        .write_value(
            &format!("bulk-result-flushes/{flusher_id}/{}", Uuid::now_v7()),
            &Value::Array(flattened_outputs),
        )
        .await?;
    let (key, store) = match handle {
        PayloadHandle::Manifest { key, store } => (key, store),
        PayloadHandle::ManifestSlice { key, store, .. } => (key, store),
        PayloadHandle::Inline { .. } => {
            anyhow::bail!("bulk result flush handle must be manifest-backed")
        }
    };

    for (index, start, len) in completions {
        let Some(
            fabrik_worker_protocol::activity_worker::bulk_activity_task_result::Result::Completed(
                completed,
            ),
        ) = pending_results[index].result.as_mut()
        else {
            continue;
        };
        let handle =
            PayloadHandle::ManifestSlice { key: key.clone(), store: store.clone(), start, len };
        completed.result_handle_json.clear();
        completed.result_handle_cbor = encode_cbor(&handle, "bulk result handle")?;
        completed.output_json.clear();
        completed.output_cbor.clear();
    }

    Ok(())
}

fn task_input_handle(
    task: &fabrik_worker_protocol::activity_worker::BulkActivityTask,
) -> Result<Option<PayloadHandle>> {
    if !task.input_handle_cbor.is_empty() {
        return Ok(Some(decode_cbor(&task.input_handle_cbor, "bulk task input handle")?));
    }
    if task.input_handle_json.trim().is_empty() {
        return Ok(None);
    }
    let value = serde_json::from_str::<Value>(&task.input_handle_json)?;
    if value.is_null() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_value(value)?))
}

async fn resolve_bulk_task_items(
    payload_store: &PayloadStore,
    task: &fabrik_worker_protocol::activity_worker::BulkActivityTask,
    input_handle: Option<&PayloadHandle>,
) -> Result<Vec<Value>> {
    if let Some(handle) = input_handle {
        return payload_store.read_items(handle).await;
    }
    if !task.items_cbor.is_empty() {
        if task.items_cbor.len() > MAX_BULK_CHUNK_INPUT_BYTES {
            anyhow::bail!("bulk chunk input exceeded {} bytes", MAX_BULK_CHUNK_INPUT_BYTES);
        }
        return match decode_cbor::<Value>(&task.items_cbor, "bulk chunk items")? {
            Value::Array(items) => Ok(items),
            other => anyhow::bail!("bulk chunk items CBOR must decode to an array, got {other}"),
        };
    }
    if task.items_json.len() > MAX_BULK_CHUNK_INPUT_BYTES {
        anyhow::bail!("bulk chunk input exceeded {} bytes", MAX_BULK_CHUNK_INPUT_BYTES);
    }
    Ok(serde_json::from_str::<Vec<Value>>(&task.items_json)?)
}

fn lane_worker_id(base_worker_id: &str, lane: usize, concurrency: usize) -> String {
    if concurrency <= 1 { base_worker_id.to_owned() } else { format!("{base_worker_id}:{lane}") }
}

async fn execute_activity(
    client: &Client,
    activity_type: &str,
    attempt: u32,
    config: Option<Value>,
    input: &Value,
) -> Result<Value> {
    if activity_type == "benchmark.echo" {
        return execute_benchmark_echo(attempt, input);
    }
    if activity_type == "http.request" {
        return execute_http_request(client, config.as_ref(), input, "activity-worker").await;
    }
    execute_handler(activity_type, input).map_err(anyhow::Error::from)
}

fn build_payload_store_config() -> PayloadStoreConfig {
    let config =
        ThroughputPayloadStoreConfig::from_env().expect("throughput payload store config loads");
    PayloadStoreConfig {
        default_store: match config.kind {
            ThroughputPayloadStoreKind::LocalFilesystem => PayloadStoreKind::LocalFilesystem,
            ThroughputPayloadStoreKind::S3 => PayloadStoreKind::S3,
        },
        local_dir: config.local_dir,
        s3_bucket: config.s3_bucket,
        s3_region: config.s3_region,
        s3_endpoint: config.s3_endpoint,
        s3_access_key_id: config.s3_access_key_id,
        s3_secret_access_key: config.s3_secret_access_key,
        s3_force_path_style: config.s3_force_path_style,
        s3_key_prefix: config.s3_key_prefix,
    }
}

fn execute_benchmark_echo(attempt: u32, input: &Value) -> Result<Value> {
    let fail_until_attempt =
        input.get("fail_until_attempt").and_then(Value::as_u64).unwrap_or_default() as u32;
    if input.get("cancel").and_then(Value::as_bool).unwrap_or(false) {
        anyhow::bail!("activity cancelled");
    }
    if attempt <= fail_until_attempt {
        anyhow::bail!("benchmark configured failure on attempt {attempt}");
    }
    Ok(input.clone())
}

async fn execute_http_request(
    client: &Client,
    config: Option<&Value>,
    input: &Value,
    idempotency_key: &str,
) -> Result<Value> {
    let Some(config) = config else {
        anyhow::bail!("http.request missing config");
    };
    let step_config: StepConfig = serde_json::from_value(config.clone())?;
    let StepConfig::HttpRequest(config) = step_config;

    let method = Method::from_bytes(config.method.as_bytes())?;
    let mut request =
        client.request(method, &config.url).header("Idempotency-Key", idempotency_key);
    for (name, value) in &config.headers {
        request = request.header(name, value);
    }
    if let Some(body) =
        config.body.clone().or_else(|| config.body_from_input.then(|| input.clone()))
    {
        request = request.json(&body);
    }

    let response = request.send().await?;
    let status = response.status();
    let headers = response_headers(&response);
    let body = response_body(response).await?;

    if !status.is_success() {
        anyhow::bail!(
            "http.request returned {} for {} {} with body {}",
            status.as_u16(),
            config.method,
            config.url,
            body
        );
    }

    Ok(serde_json::json!({
        "connector": "http.request",
        "method": config.method,
        "url": config.url,
        "status_code": status.as_u16(),
        "headers": headers,
        "body": body,
        "idempotency_key": idempotency_key,
    }))
}

fn parse_required_json(raw: &str) -> Result<Value> {
    serde_json::from_str(raw).map_err(anyhow::Error::from)
}

fn parse_optional_json(raw: &str) -> Result<Option<Value>> {
    if raw.trim().is_empty() { Ok(None) } else { Ok(Some(serde_json::from_str(raw)?)) }
}

fn response_headers(response: &Response) -> serde_json::Map<String, Value> {
    response
        .headers()
        .iter()
        .map(|(name, value)| {
            (name.as_str().to_owned(), Value::String(value.to_str().unwrap_or_default().to_owned()))
        })
        .collect()
}

async fn response_body(response: Response) -> Result<Value> {
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let bytes = response.bytes().await?;

    if bytes.is_empty() {
        return Ok(Value::Null);
    }
    if content_type.contains("application/json") {
        return Ok(serde_json::from_slice(&bytes)?);
    }

    Ok(Value::String(String::from_utf8_lossy(&bytes).into_owned()))
}
