use std::{env, time::Duration};

use anyhow::Result;
use fabrik_config::GrpcServiceConfig;
use fabrik_worker_protocol::activity_worker::{
    CompleteActivityTaskRequest, FailActivityTaskRequest, PollActivityTaskRequest,
    ReportActivityTaskCancelledRequest, activity_worker_api_client::ActivityWorkerApiClient,
};
use fabrik_workflow::{StepConfig, execute_handler};
use reqwest::{Client, Method, Response};
use serde_json::Value;
use tracing::{error, info};
use uuid::Uuid;

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
    let client = Client::new();

    info!(
        matching_endpoint = %endpoint,
        task_queue = %task_queue,
        tenant_id = %tenant_id,
        worker_id = %worker_id,
        worker_build_id = %worker_build_id,
        port = config.port,
        "activity-worker-service starting"
    );

    let mut worker = ActivityWorkerApiClient::connect(endpoint).await?;

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
                error!(error = %error, "failed to poll matching-service");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        }) else {
            continue;
        };

        let result = execute_activity(
            &client,
            &task.activity_type,
            parse_optional_json(&task.config_json)?,
            &parse_required_json(&task.input_json)?,
        )
        .await;

        match result {
            Ok(output) => {
                worker
                    .complete_activity_task(CompleteActivityTaskRequest {
                        tenant_id: task.tenant_id,
                        instance_id: task.instance_id,
                        run_id: task.run_id,
                        activity_id: task.activity_id,
                        attempt: task.attempt,
                        worker_id: worker_id.clone(),
                        worker_build_id: worker_build_id.clone(),
                        output_json: serde_json::to_string(&output)?,
                    })
                    .await?;
            }
            Err(error) if error.to_string() == "activity cancelled" => {
                worker
                    .report_activity_task_cancelled(ReportActivityTaskCancelledRequest {
                        tenant_id: task.tenant_id,
                        instance_id: task.instance_id,
                        run_id: task.run_id,
                        activity_id: task.activity_id,
                        attempt: task.attempt,
                        worker_id: worker_id.clone(),
                        worker_build_id: worker_build_id.clone(),
                        reason: "activity cancelled".to_owned(),
                        metadata_json: String::new(),
                    })
                    .await?;
            }
            Err(error) => {
                worker
                    .fail_activity_task(FailActivityTaskRequest {
                        tenant_id: task.tenant_id,
                        instance_id: task.instance_id,
                        run_id: task.run_id,
                        activity_id: task.activity_id,
                        attempt: task.attempt,
                        worker_id: worker_id.clone(),
                        worker_build_id: worker_build_id.clone(),
                        error: error.to_string(),
                    })
                    .await?;
            }
        }
    }
}

async fn execute_activity(
    client: &Client,
    activity_type: &str,
    config: Option<Value>,
    input: &Value,
) -> Result<Value> {
    if activity_type == "http.request" {
        return execute_http_request(client, config.as_ref(), input, "activity-worker").await;
    }
    execute_handler(activity_type, input).map_err(anyhow::Error::from)
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
