use anyhow::Result;
use fabrik_broker::{
    BrokerConfig, WorkflowPublisher, build_workflow_consumer, decode_workflow_event,
};
use fabrik_config::{HttpServiceConfig, PostgresConfig, RedpandaConfig};
use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
use fabrik_service::{ServiceInfo, default_router, init_tracing, serve};
use fabrik_store::WorkflowStore;
use fabrik_workflow::{CompiledWorkflowArtifact, StepConfig, WorkflowDefinition, execute_handler};
use futures_util::StreamExt;
use reqwest::{Client, Method, Response};
use serde_json::Value;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    let config = HttpServiceConfig::from_env("CONNECTOR_SERVICE", "connector-service", 3004)?;
    let redpanda = RedpandaConfig::from_env()?;
    let postgres = PostgresConfig::from_env()?;
    init_tracing(&config.log_filter);
    info!(port = config.port, "starting connector service");

    let broker = BrokerConfig::new(redpanda.brokers, redpanda.workflow_events_topic);
    let publisher = WorkflowPublisher::new(&broker, "connector-service").await?;
    let consumer = build_workflow_consumer(&broker, "connector-service").await?;
    let store = WorkflowStore::connect(&postgres.url).await?;
    let client = Client::new();
    store.init().await?;

    tokio::spawn(run_connector_loop(consumer, publisher, store.clone(), client));

    let app =
        default_router::<()>(ServiceInfo::new(config.name, "connector", env!("CARGO_PKG_VERSION")));

    serve(app, config.port).await
}

async fn run_connector_loop(
    consumer: rskafka::client::consumer::StreamConsumer,
    publisher: WorkflowPublisher,
    store: WorkflowStore,
    client: Client,
) {
    let mut stream = consumer;

    while let Some(message) = stream.next().await {
        match message {
            Ok((record, _high_watermark)) => match decode_workflow_event(&record) {
                Ok(event) => {
                    if let Err(error) = process_event(&store, &publisher, &client, event).await {
                        error!(error = %error, "failed to process connector event");
                    }
                }
                Err(error) => warn!(error = %error, "skipping invalid workflow event"),
            },
            Err(error) => error!(error = %error, "connector consumer error"),
        }
    }
}

async fn process_event(
    store: &WorkflowStore,
    publisher: &WorkflowPublisher,
    client: &Client,
    event: EventEnvelope<WorkflowEvent>,
) -> Result<()> {
    let target = match &event.payload {
        WorkflowEvent::StepScheduled { step_id, attempt, input } => ConnectorTarget {
            id: step_id.clone(),
            attempt: *attempt,
            input: input.clone(),
            kind: ConnectorTargetKind::Step,
        },
        WorkflowEvent::EffectRequested { effect_id, attempt, input, .. } => ConnectorTarget {
            id: effect_id.clone(),
            attempt: *attempt,
            input: input.clone(),
            kind: ConnectorTargetKind::Effect,
        },
        _ => {
            return Ok(());
        }
    };

    if !store.mark_connector_event_processed(&event).await? {
        return Ok(());
    }

    let instance = match store.get_instance(&event.tenant_id, &event.instance_id).await? {
        Some(instance) => instance,
        None => {
            warn!(
                workflow_instance_id = %event.instance_id,
                target_id = %target.id,
                "dropping connector execution without workflow state"
            );
            return Ok(());
        }
    };

    let (handler, config) = if let Some(artifact) =
        load_pinned_artifact(store, &event, &instance.definition_id, instance.definition_version)
            .await?
    {
        let descriptor = match target.kind {
            ConnectorTargetKind::Step => artifact.step_descriptor(&target.id),
            ConnectorTargetKind::Effect => artifact.effect_descriptor(&target.id),
        };
        match descriptor {
            Ok((handler, config)) => (handler, config),
            Err(error) => {
                publish_connector_failure(publisher, &event, &target, error.to_string()).await?;
                return Ok(());
            }
        }
    } else {
        let definition = load_pinned_definition(
            store,
            &event,
            &instance.definition_id,
            instance.definition_version,
        )
        .await?;
        let handler = match definition.step_handler(&target.id) {
            Ok(handler) => handler.to_owned(),
            Err(error) => {
                publish_connector_failure(publisher, &event, &target, error.to_string()).await?;
                return Ok(());
            }
        };

        let config = match definition.step_config(&target.id) {
            Ok(config) => config.cloned(),
            Err(error) => {
                publish_connector_failure(publisher, &event, &target, error.to_string()).await?;
                return Ok(());
            }
        };
        (handler, config)
    };
    let idempotency_key = build_idempotency_key(&event, &target.id, target.attempt);

    match execute_step(client, &handler, config.as_ref(), &target.input, &idempotency_key).await {
        Ok(output) => {
            let payload = match target.kind {
                ConnectorTargetKind::Step => WorkflowEvent::StepCompleted {
                    step_id: target.id.clone(),
                    attempt: target.attempt,
                    output,
                },
                ConnectorTargetKind::Effect => WorkflowEvent::EffectCompleted {
                    effect_id: target.id.clone(),
                    attempt: target.attempt,
                    output,
                },
            };
            let mut envelope = EventEnvelope::new(
                payload.event_type(),
                source_identity(&event, "connector-service"),
                payload,
            );
            envelope.causation_id = Some(event.event_id);
            envelope.correlation_id = event.correlation_id.or(Some(event.event_id));
            envelope.metadata.insert("state".to_owned(), target.id.clone());
            envelope.metadata.insert("attempt".to_owned(), target.attempt.to_string());
            publisher.publish(&envelope, &envelope.partition_key).await?;
        }
        Err(error) => {
            publish_connector_failure(publisher, &event, &target, error.to_string()).await?;
        }
    }

    Ok(())
}

#[derive(Clone)]
struct ConnectorTarget {
    id: String,
    attempt: u32,
    input: Value,
    kind: ConnectorTargetKind,
}

#[derive(Clone, Copy)]
enum ConnectorTargetKind {
    Step,
    Effect,
}

async fn execute_step(
    client: &Client,
    handler: &str,
    config: Option<&StepConfig>,
    input: &Value,
    idempotency_key: &str,
) -> Result<Value> {
    match handler {
        "http.request" => execute_http_request(client, config, input, idempotency_key).await,
        _ => execute_handler(handler, input).map_err(anyhow::Error::from),
    }
}

async fn execute_http_request(
    client: &Client,
    config: Option<&StepConfig>,
    input: &Value,
    idempotency_key: &str,
) -> Result<Value> {
    let Some(StepConfig::HttpRequest(config)) = config else {
        anyhow::bail!("http.request missing http_request config");
    };

    let method = Method::from_bytes(config.method.as_bytes())?;
    let mut request =
        client.request(method.clone(), &config.url).header("Idempotency-Key", idempotency_key);

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

fn build_idempotency_key(
    event: &EventEnvelope<WorkflowEvent>,
    step_id: &str,
    attempt: u32,
) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}",
        event.tenant_id,
        event.instance_id,
        event.run_id,
        step_id,
        attempt,
        event.correlation_id.unwrap_or(event.event_id)
    )
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

async fn publish_connector_failure(
    publisher: &WorkflowPublisher,
    source: &EventEnvelope<WorkflowEvent>,
    target: &ConnectorTarget,
    error: String,
) -> Result<()> {
    let payload = match target.kind {
        ConnectorTargetKind::Step => {
            WorkflowEvent::StepFailed { step_id: target.id.clone(), attempt: target.attempt, error }
        }
        ConnectorTargetKind::Effect => WorkflowEvent::EffectFailed {
            effect_id: target.id.clone(),
            attempt: target.attempt,
            error,
        },
    };
    let mut envelope = EventEnvelope::new(
        payload.event_type(),
        source_identity(source, "connector-service"),
        payload,
    );
    envelope.causation_id = Some(source.event_id);
    envelope.correlation_id = source.correlation_id.or(Some(source.event_id));
    envelope.metadata.insert("state".to_owned(), target.id.clone());
    envelope.metadata.insert("attempt".to_owned(), target.attempt.to_string());
    publisher.publish(&envelope, &envelope.partition_key).await?;
    Ok(())
}

async fn load_pinned_definition(
    store: &WorkflowStore,
    event: &EventEnvelope<WorkflowEvent>,
    definition_id: &str,
    definition_version: Option<u32>,
) -> Result<WorkflowDefinition> {
    if let Some(version) = definition_version.or(Some(event.definition_version)) {
        if let Some(definition) =
            store.get_definition_version(&event.tenant_id, definition_id, version).await?
        {
            return Ok(definition);
        }
    }

    store.get_latest_definition(&event.tenant_id, definition_id).await?.ok_or_else(|| {
        anyhow::anyhow!(
            "workflow definition {} not found for tenant {}",
            definition_id,
            event.tenant_id
        )
    })
}

async fn load_pinned_artifact(
    store: &WorkflowStore,
    event: &EventEnvelope<WorkflowEvent>,
    definition_id: &str,
    definition_version: Option<u32>,
) -> Result<Option<CompiledWorkflowArtifact>> {
    if let Some(version) = definition_version.or(Some(event.definition_version)) {
        return store.get_artifact_version(&event.tenant_id, definition_id, version).await;
    }

    Ok(None)
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
