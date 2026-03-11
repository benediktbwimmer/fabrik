use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use chrono::Utc;
use fabrik_events::{EventEnvelope, WorkflowEvent};
use futures_util::StreamExt;
use rskafka::{
    client::{
        Client, ClientBuilder,
        consumer::{StartOffset, StreamConsumer, StreamConsumerBuilder},
        partition::UnknownTopicHandling,
        producer::{BatchProducer, BatchProducerBuilder, aggregator::RecordAggregator},
    },
    record::{Record, RecordAndOffset},
};
use tokio::time::{Instant, timeout};

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub brokers: String,
    pub workflow_events_topic: String,
}

impl BrokerConfig {
    pub fn new(brokers: impl Into<String>, workflow_events_topic: impl Into<String>) -> Self {
        Self { brokers: brokers.into(), workflow_events_topic: workflow_events_topic.into() }
    }
}

#[derive(Clone)]
pub struct WorkflowPublisher {
    producer: Arc<BatchProducer<RecordAggregator>>,
}

impl WorkflowPublisher {
    pub async fn new(config: &BrokerConfig, client_id: &str) -> Result<Self> {
        let client = ClientBuilder::new(vec![config.brokers.clone()])
            .client_id(client_id)
            .build()
            .await
            .context("failed to create kafka client")?;
        ensure_topic(&client, &config.workflow_events_topic).await?;

        let partition_client = Arc::new(
            client
                .partition_client(
                    config.workflow_events_topic.clone(),
                    0,
                    UnknownTopicHandling::Retry,
                )
                .await
                .context("failed to create producer partition client")?,
        );

        let producer = BatchProducerBuilder::new(partition_client)
            .with_linger(Duration::from_millis(5))
            .build(RecordAggregator::new(1024 * 1024));

        Ok(Self { producer: Arc::new(producer) })
    }

    pub async fn publish(&self, envelope: &EventEnvelope<WorkflowEvent>, key: &str) -> Result<()> {
        let payload = serde_json::to_vec(envelope).context("failed to serialize event envelope")?;
        self.producer
            .produce(Record {
                key: Some(key.as_bytes().to_vec()),
                value: Some(payload),
                headers: BTreeMap::new(),
                timestamp: Utc::now(),
            })
            .await
            .context("failed to publish event")?;
        Ok(())
    }
}

pub async fn build_workflow_consumer(
    config: &BrokerConfig,
    client_id: &str,
) -> Result<StreamConsumer> {
    let client = ClientBuilder::new(vec![config.brokers.clone()])
        .client_id(client_id)
        .build()
        .await
        .context("failed to create kafka client")?;
    ensure_topic(&client, &config.workflow_events_topic).await?;

    let partition_client = Arc::new(
        client
            .partition_client(config.workflow_events_topic.clone(), 0, UnknownTopicHandling::Retry)
            .await
            .context("failed to create consumer partition client")?,
    );

    Ok(StreamConsumerBuilder::new(partition_client, StartOffset::Earliest)
        .with_max_wait_ms(250)
        .build())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowHistoryFilter {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
}

impl WorkflowHistoryFilter {
    pub fn new(
        tenant_id: impl Into<String>,
        instance_id: impl Into<String>,
        run_id: impl Into<String>,
    ) -> Self {
        Self { tenant_id: tenant_id.into(), instance_id: instance_id.into(), run_id: run_id.into() }
    }

    fn matches(&self, event: &EventEnvelope<WorkflowEvent>) -> bool {
        event.tenant_id == self.tenant_id
            && event.instance_id == self.instance_id
            && event.run_id == self.run_id
    }
}

pub async fn read_workflow_history(
    config: &BrokerConfig,
    client_id: &str,
    filter: &WorkflowHistoryFilter,
    idle_timeout: Duration,
    max_scan_duration: Duration,
) -> Result<Vec<EventEnvelope<WorkflowEvent>>> {
    let mut consumer = build_workflow_consumer(config, client_id).await?;
    let scan_started_at = Instant::now();
    let mut last_match_at: Option<Instant> = None;
    let mut events = Vec::new();

    loop {
        if scan_started_at.elapsed() >= max_scan_duration {
            break;
        }
        if let Some(last_match_at) = last_match_at {
            if last_match_at.elapsed() >= idle_timeout {
                break;
            }
        }

        match timeout(Duration::from_millis(250), consumer.next()).await {
            Ok(Some(Ok((record, _high_watermark)))) => {
                if let Ok(event) = decode_workflow_event(&record) {
                    if filter.matches(&event) {
                        last_match_at = Some(Instant::now());
                        events.push(event);
                    }
                }
            }
            Ok(Some(Err(error))) => {
                return Err(error).context("failed to read workflow history");
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    Ok(events)
}

pub fn decode_workflow_event(record: &RecordAndOffset) -> Result<EventEnvelope<WorkflowEvent>> {
    let payload = record.record.value.as_deref().context("message payload missing")?;
    serde_json::from_slice(payload).context("failed to deserialize workflow event")
}

async fn ensure_topic(client: &Client, topic_name: &str) -> Result<()> {
    let topics = client.list_topics().await.context("failed to list kafka topics")?;

    if topics.iter().any(|topic| topic.name == topic_name) {
        return Ok(());
    }

    client
        .controller_client()
        .context("failed to create kafka controller client")?
        .create_topic(topic_name.to_owned(), 1, 1, 5_000)
        .await
        .context("failed to create workflow topic")?;

    Ok(())
}
