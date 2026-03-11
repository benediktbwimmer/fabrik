use std::{env, sync::Arc, time::Duration};

use anyhow::Result;
use fabrik_config::GrpcServiceConfig;
use fabrik_worker_protocol::activity_worker::{
    ActivityTaskCancelledResult, ActivityTaskCompletedResult, ActivityTaskFailedResult,
    ActivityTaskResult, PollActivityTaskRequest, ReportActivityTaskResultsRequest,
    activity_task_result, activity_worker_api_client::ActivityWorkerApiClient,
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

#[tokio::main]
async fn main() -> Result<()> {
    let config =
        GrpcServiceConfig::from_env("ACTIVITY_WORKER_SERVICE", "activity-worker-service", 50052)?;
    fabrik_service::init_tracing(&config.log_filter);

    let endpoint = env::var("MATCHING_SERVICE_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:50051".to_owned());
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
    let client = Arc::new(Client::new());

    info!(
        matching_endpoint = %endpoint,
        task_queue = %task_queue,
        tenant_id = %tenant_id,
        worker_id = %worker_id,
        worker_build_id = %worker_build_id,
        concurrency,
        result_flusher_concurrency,
        port = config.port,
        "activity-worker-service starting"
    );

    let mut workers = JoinSet::new();
    let mut result_txs = Vec::with_capacity(result_flusher_concurrency);
    for flusher_index in 0..result_flusher_concurrency {
        let (result_tx, result_rx) = unbounded_channel();
        workers.spawn(run_result_flusher(
            endpoint.clone(),
            format!("result-flusher-{flusher_index}"),
            result_rx,
        ));
        result_txs.push(result_tx);
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
    }
    drop(result_txs);

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
