use std::{
    collections::{BTreeMap, HashMap},
    hash::{Hash, Hasher},
    marker::PhantomData,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::Utc;
use fabrik_events::{EventEnvelope, WorkflowEvent, workflow_partition_key};
use futures_util::{
    StreamExt,
    future::try_join_all,
    stream::{BoxStream, FuturesOrdered},
};
use rskafka::{
    client::{
        Client, ClientBuilder,
        consumer::{StartOffset, StreamConsumer, StreamConsumerBuilder},
        partition::OffsetAt,
        partition::UnknownTopicHandling,
        producer::{BatchProducer, BatchProducerBuilder, aggregator::RecordAggregator},
    },
    record::{Record, RecordAndOffset},
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::time::{Instant, sleep, timeout};

const MAX_WORKFLOW_EVENT_RECORD_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub brokers: String,
    pub workflow_events_topic: String,
    pub workflow_events_partitions: i32,
}

impl BrokerConfig {
    pub fn new(
        brokers: impl Into<String>,
        workflow_events_topic: impl Into<String>,
        workflow_events_partitions: i32,
    ) -> Self {
        Self {
            brokers: brokers.into(),
            workflow_events_topic: workflow_events_topic.into(),
            workflow_events_partitions,
        }
    }

    pub fn all_partition_ids(&self) -> Vec<i32> {
        (0..self.workflow_events_partitions).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkflowTopicTopology {
    pub brokers: String,
    pub topic_name: String,
    pub configured_partitions: i32,
    pub configured_partition_ids: Vec<i32>,
    pub actual_partitions: Option<i32>,
    pub actual_partition_ids: Vec<i32>,
    pub healthy: bool,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonTopicConfig {
    pub brokers: String,
    pub topic_name: String,
    pub partitions: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TopicPartitionOffset {
    pub partition_id: i32,
    pub latest_offset: i64,
}

impl JsonTopicConfig {
    pub fn new(brokers: impl Into<String>, topic_name: impl Into<String>, partitions: i32) -> Self {
        Self { brokers: brokers.into(), topic_name: topic_name.into(), partitions }
    }

    pub fn all_partition_ids(&self) -> Vec<i32> {
        (0..self.partitions).collect()
    }
}

#[derive(Clone)]
pub struct WorkflowPublisher {
    producers: Arc<HashMap<i32, Arc<BatchProducer<RecordAggregator>>>>,
    partition_count: i32,
}

#[derive(Debug)]
pub struct ConsumedWorkflowRecord {
    pub partition_id: i32,
    pub record: RecordAndOffset,
    pub high_watermark: i64,
}

pub type WorkflowConsumerStream = BoxStream<'static, Result<ConsumedWorkflowRecord>>;

#[derive(Clone)]
pub struct JsonTopicPublisher<T> {
    producers: Arc<HashMap<i32, Arc<BatchProducer<RecordAggregator>>>>,
    partition_count: i32,
    marker: PhantomData<T>,
}

#[derive(Debug)]
pub struct ConsumedJsonRecord {
    pub partition_id: i32,
    pub record: RecordAndOffset,
    pub high_watermark: i64,
}

pub type JsonConsumerStream = BoxStream<'static, Result<ConsumedJsonRecord>>;

impl WorkflowPublisher {
    pub async fn new(config: &BrokerConfig, client_id: &str) -> Result<Self> {
        let client = ClientBuilder::new(vec![config.brokers.clone()])
            .client_id(client_id)
            .build()
            .await
            .context("failed to create kafka client")?;
        ensure_topic(&client, &config.workflow_events_topic, config.workflow_events_partitions)
            .await?;

        let mut producers = HashMap::new();
        for partition_id in 0..config.workflow_events_partitions {
            let partition_client = Arc::new(
                client
                    .partition_client(
                        config.workflow_events_topic.clone(),
                        partition_id,
                        UnknownTopicHandling::Retry,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "failed to create producer partition client for partition {partition_id}"
                        )
                    })?,
            );

            let producer = BatchProducerBuilder::new(partition_client)
                .with_linger(Duration::ZERO)
                .build(RecordAggregator::new(MAX_WORKFLOW_EVENT_RECORD_BYTES));
            producers.insert(partition_id, Arc::new(producer));
        }

        Ok(Self {
            producers: Arc::new(producers),
            partition_count: config.workflow_events_partitions,
        })
    }

    pub async fn publish(&self, envelope: &EventEnvelope<WorkflowEvent>, key: &str) -> Result<()> {
        let payload = serde_json::to_vec(envelope).context("failed to serialize event envelope")?;
        let partition_id = partition_for_key(key, self.partition_count);
        let producer = self
            .producers
            .get(&partition_id)
            .with_context(|| format!("missing producer for workflow partition {partition_id}"))?;
        producer
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

    pub async fn publish_all(&self, envelopes: &[EventEnvelope<WorkflowEvent>]) -> Result<()> {
        if envelopes.is_empty() {
            return Ok(());
        }

        let mut records_by_partition = HashMap::<i32, Vec<Record>>::new();
        for envelope in envelopes {
            let payload =
                serde_json::to_vec(envelope).context("failed to serialize event envelope")?;
            let partition_id = partition_for_key(&envelope.partition_key, self.partition_count);
            records_by_partition.entry(partition_id).or_default().push(Record {
                key: Some(envelope.partition_key.as_bytes().to_vec()),
                value: Some(payload),
                headers: BTreeMap::new(),
                timestamp: Utc::now(),
            });
        }

        let mut flushes = Vec::new();
        for (partition_id, records) in records_by_partition {
            let producer = self
                .producers
                .get(&partition_id)
                .with_context(|| format!("missing producer for workflow partition {partition_id}"))?
                .clone();
            flushes.push(async move {
                let mut pending = FuturesOrdered::new();
                for record in records {
                    let producer = producer.clone();
                    pending.push_back(async move {
                        producer
                            .produce(record)
                            .await
                            .map(|_| ())
                            .context("failed to publish event")
                    });
                }
                while let Some(result) = pending.next().await {
                    result?;
                }
                Ok::<(), anyhow::Error>(())
            });
        }

        try_join_all(flushes).await?;
        Ok(())
    }

    pub fn partition_for_key(&self, key: &str) -> i32 {
        partition_for_key(key, self.partition_count)
    }
}

impl<T> JsonTopicPublisher<T>
where
    T: Serialize,
{
    pub async fn new(config: &JsonTopicConfig, client_id: &str) -> Result<Self> {
        let client = ClientBuilder::new(vec![config.brokers.clone()])
            .client_id(client_id)
            .build()
            .await
            .context("failed to create kafka client")?;
        ensure_topic(&client, &config.topic_name, config.partitions).await?;

        let mut producers = HashMap::new();
        for partition_id in 0..config.partitions {
            let partition_client = Arc::new(
                client
                    .partition_client(
                        config.topic_name.clone(),
                        partition_id,
                        UnknownTopicHandling::Retry,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "failed to create producer partition client for topic {} partition {partition_id}",
                            config.topic_name
                        )
                    })?,
            );
            let producer = BatchProducerBuilder::new(partition_client)
                .with_linger(Duration::ZERO)
                .build(RecordAggregator::new(MAX_WORKFLOW_EVENT_RECORD_BYTES));
            producers.insert(partition_id, Arc::new(producer));
        }

        Ok(Self {
            producers: Arc::new(producers),
            partition_count: config.partitions,
            marker: PhantomData,
        })
    }

    pub async fn publish(&self, payload: &T, key: &str) -> Result<()> {
        let bytes =
            serde_json::to_vec(payload).context("failed to serialize json topic payload")?;
        let partition_id = partition_for_key(key, self.partition_count);
        let producer = self
            .producers
            .get(&partition_id)
            .with_context(|| format!("missing producer for topic partition {partition_id}"))?;
        producer
            .produce(Record {
                key: Some(key.as_bytes().to_vec()),
                value: Some(bytes),
                headers: BTreeMap::new(),
                timestamp: Utc::now(),
            })
            .await
            .context("failed to publish json topic payload")?;
        Ok(())
    }

    pub async fn publish_all(&self, payloads: Vec<(String, T)>) -> Result<()> {
        if payloads.is_empty() {
            return Ok(());
        }

        let mut records_by_partition = HashMap::<i32, Vec<Record>>::new();
        for (key, payload) in payloads {
            let bytes =
                serde_json::to_vec(&payload).context("failed to serialize json topic payload")?;
            let partition_id = partition_for_key(&key, self.partition_count);
            records_by_partition.entry(partition_id).or_default().push(Record {
                key: Some(key.into_bytes()),
                value: Some(bytes),
                headers: BTreeMap::new(),
                timestamp: Utc::now(),
            });
        }

        let mut flushes = Vec::new();
        for (partition_id, records) in records_by_partition {
            let producer = self
                .producers
                .get(&partition_id)
                .with_context(|| format!("missing producer for topic partition {partition_id}"))?
                .clone();
            flushes.push(async move {
                let mut pending = FuturesOrdered::new();
                for record in records {
                    let producer = producer.clone();
                    pending.push_back(async move {
                        producer
                            .produce(record)
                            .await
                            .map(|_| ())
                            .context("failed to publish json topic payload")
                    });
                }
                while let Some(result) = pending.next().await {
                    result?;
                }
                Ok::<(), anyhow::Error>(())
            });
        }

        try_join_all(flushes).await?;
        Ok(())
    }
}

pub async fn build_workflow_partition_consumer(
    config: &BrokerConfig,
    client_id: &str,
    partition_id: i32,
) -> Result<StreamConsumer> {
    let client = ClientBuilder::new(vec![config.brokers.clone()])
        .client_id(format!("{client_id}-partition-{partition_id}"))
        .build()
        .await
        .context("failed to create kafka client")?;
    ensure_topic(&client, &config.workflow_events_topic, config.workflow_events_partitions).await?;

    let partition_client = Arc::new(
        client
            .partition_client(
                config.workflow_events_topic.clone(),
                partition_id,
                UnknownTopicHandling::Retry,
            )
            .await
            .with_context(|| {
                format!("failed to create consumer partition client for partition {partition_id}")
            })?,
    );

    Ok(StreamConsumerBuilder::new(partition_client, StartOffset::Earliest)
        .with_max_wait_ms(250)
        .build())
}

pub async fn build_workflow_consumer(
    config: &BrokerConfig,
    client_id: &str,
    partitions: &[i32],
) -> Result<WorkflowConsumerStream> {
    if partitions.is_empty() {
        anyhow::bail!("workflow consumer requires at least one partition");
    }

    let mut streams = futures_util::stream::SelectAll::new();
    for partition_id in partitions {
        let consumer = build_workflow_partition_consumer(config, client_id, *partition_id).await?;
        let partition_id = *partition_id;
        streams.push(
            consumer
                .map(move |result| {
                    result
                        .map(|(record, high_watermark)| ConsumedWorkflowRecord {
                            partition_id,
                            record,
                            high_watermark,
                        })
                        .map_err(anyhow::Error::from)
                })
                .boxed(),
        );
    }
    Ok(streams.boxed())
}

pub async fn build_json_partition_consumer(
    config: &JsonTopicConfig,
    client_id: &str,
    partition_id: i32,
) -> Result<StreamConsumer> {
    build_json_partition_consumer_from_offset(
        config,
        client_id,
        partition_id,
        StartOffset::Earliest,
    )
    .await
}

pub async fn build_json_partition_consumer_from_offset(
    config: &JsonTopicConfig,
    client_id: &str,
    partition_id: i32,
    start_offset: StartOffset,
) -> Result<StreamConsumer> {
    let client = ClientBuilder::new(vec![config.brokers.clone()])
        .client_id(format!("{client_id}-partition-{partition_id}"))
        .build()
        .await
        .context("failed to create kafka client")?;
    ensure_topic(&client, &config.topic_name, config.partitions).await?;

    let partition_client = Arc::new(
        client
            .partition_client(config.topic_name.clone(), partition_id, UnknownTopicHandling::Retry)
            .await
            .with_context(|| {
                format!("failed to create consumer partition client for partition {partition_id}")
            })?,
    );

    Ok(StreamConsumerBuilder::new(partition_client, start_offset).with_max_wait_ms(250).build())
}

pub async fn build_json_consumer(
    config: &JsonTopicConfig,
    client_id: &str,
    partitions: &[i32],
) -> Result<JsonConsumerStream> {
    build_json_consumer_from_offsets(config, client_id, &HashMap::new(), partitions).await
}

pub async fn build_json_consumer_from_offsets(
    config: &JsonTopicConfig,
    client_id: &str,
    start_offsets: &HashMap<i32, i64>,
    partitions: &[i32],
) -> Result<JsonConsumerStream> {
    if partitions.is_empty() {
        anyhow::bail!("json consumer requires at least one partition");
    }

    let mut streams = futures_util::stream::SelectAll::new();
    for partition_id in partitions {
        let start_offset = start_offsets
            .get(partition_id)
            .copied()
            .map(StartOffset::At)
            .unwrap_or(StartOffset::Earliest);
        let consumer = build_json_partition_consumer_from_offset(
            config,
            client_id,
            *partition_id,
            start_offset,
        )
        .await?;
        let partition_id = *partition_id;
        streams.push(
            consumer
                .map(move |result| {
                    result
                        .map(|(record, high_watermark)| ConsumedJsonRecord {
                            partition_id,
                            record,
                            high_watermark,
                        })
                        .map_err(anyhow::Error::from)
                })
                .boxed(),
        );
    }
    Ok(streams.boxed())
}

pub fn decode_json_record<T>(record: &RecordAndOffset) -> Result<T>
where
    T: DeserializeOwned,
{
    let payload = record.record.value.as_deref().context("message payload missing")?;
    serde_json::from_slice(payload).context("failed to deserialize topic payload")
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
    let partition_id = partition_for_instance(
        &filter.tenant_id,
        &filter.instance_id,
        config.workflow_events_partitions,
    );
    let mut consumer = build_workflow_partition_consumer(config, client_id, partition_id).await?;
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

pub async fn describe_workflow_topic(
    config: &BrokerConfig,
    client_id: &str,
) -> Result<WorkflowTopicTopology> {
    let client = ClientBuilder::new(vec![config.brokers.clone()])
        .client_id(client_id)
        .build()
        .await
        .context("failed to create kafka client")?;
    describe_workflow_topic_with_client(config, &client).await
}

pub async fn load_json_topic_latest_offsets(
    config: &JsonTopicConfig,
    client_id: &str,
) -> Result<Vec<TopicPartitionOffset>> {
    let client = ClientBuilder::new(vec![config.brokers.clone()])
        .client_id(client_id)
        .build()
        .await
        .context("failed to create kafka client")?;
    ensure_topic(&client, &config.topic_name, config.partitions).await?;

    let mut offsets = Vec::with_capacity(config.partitions as usize);
    for partition_id in 0..config.partitions {
        let partition_client = client
            .partition_client(config.topic_name.clone(), partition_id, UnknownTopicHandling::Retry)
            .await
            .with_context(|| {
                format!(
                    "failed to create partition client for topic {} partition {partition_id}",
                    config.topic_name
                )
            })?;
        let latest_offset = partition_client
            .get_offset(OffsetAt::Latest)
            .await
            .with_context(|| {
                format!(
                    "failed to load latest offset for topic {} partition {partition_id}",
                    config.topic_name
                )
            })?;
        offsets.push(TopicPartitionOffset { partition_id, latest_offset });
    }

    Ok(offsets)
}

pub fn partition_for_key(key: &str, partition_count: i32) -> i32 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() % partition_count as u64) as i32
}

async fn describe_workflow_topic_with_client(
    config: &BrokerConfig,
    client: &Client,
) -> Result<WorkflowTopicTopology> {
    let actual_partitions =
        load_topic_partition_count(client, &config.workflow_events_topic).await?;
    Ok(build_workflow_topic_topology(config, actual_partitions))
}

fn build_workflow_topic_topology(
    config: &BrokerConfig,
    actual_partitions: Option<i32>,
) -> WorkflowTopicTopology {
    let configured_partition_ids = config.all_partition_ids();
    let actual_partition_ids = match actual_partitions {
        Some(partitions) if partitions > 0 => (0..partitions).collect(),
        _ => Vec::new(),
    };
    let (healthy, status) = match actual_partitions {
        Some(actual) if actual < config.workflow_events_partitions => (
            false,
            format!(
                "partition_mismatch: topic has {actual} partitions but configuration requires {}",
                config.workflow_events_partitions
            ),
        ),
        Some(actual) if actual == config.workflow_events_partitions => (true, "ok".to_owned()),
        Some(actual) => (
            true,
            format!(
                "topic has {actual} partitions; configuration uses first {}",
                config.workflow_events_partitions
            ),
        ),
        None => (false, "topic_missing".to_owned()),
    };

    WorkflowTopicTopology {
        brokers: config.brokers.clone(),
        topic_name: config.workflow_events_topic.clone(),
        configured_partitions: config.workflow_events_partitions,
        configured_partition_ids,
        actual_partitions,
        actual_partition_ids,
        healthy,
        status,
    }
}

pub fn partition_for_instance(tenant_id: &str, instance_id: &str, partition_count: i32) -> i32 {
    partition_for_key(&workflow_partition_key(tenant_id, instance_id), partition_count)
}

pub fn decode_workflow_event(record: &RecordAndOffset) -> Result<EventEnvelope<WorkflowEvent>> {
    let payload = record.record.value.as_deref().context("message payload missing")?;
    serde_json::from_slice(payload).context("failed to deserialize workflow event")
}

pub fn decode_consumed_workflow_event(
    record: &ConsumedWorkflowRecord,
) -> Result<EventEnvelope<WorkflowEvent>> {
    decode_workflow_event(&record.record)
}

async fn ensure_topic(client: &Client, topic_name: &str, partitions: i32) -> Result<()> {
    if let Some(existing) = load_topic_partition_count(client, topic_name).await? {
        ensure_topic_partition_count(topic_name, existing, partitions)?;
        return Ok(());
    }

    let create_result = client
        .controller_client()
        .context("failed to create kafka controller client")?
        .create_topic(topic_name.to_owned(), partitions, 1, 5_000)
        .await;

    if let Err(error) = create_result {
        let message = error.to_string();
        if message.contains("TopicAlreadyExists") {
            let existing =
                load_topic_partition_count(client, topic_name).await?.unwrap_or_default();
            ensure_topic_partition_count(topic_name, existing, partitions)?;
            return Ok(());
        }
        return Err(error).context("failed to create workflow topic");
    }

    wait_for_topic_partitions(client, topic_name, partitions).await?;
    Ok(())
}

async fn load_topic_partition_count(client: &Client, topic_name: &str) -> Result<Option<i32>> {
    let topics = client.list_topics().await.context("failed to list kafka topics")?;
    Ok(topics
        .into_iter()
        .find(|topic| topic.name == topic_name)
        .map(|topic| topic.partitions.len() as i32))
}

fn ensure_topic_partition_count(topic_name: &str, actual: i32, expected: i32) -> Result<()> {
    if actual < expected {
        anyhow::bail!(
            "workflow topic {topic_name} has {actual} partitions but configuration requires {expected}"
        );
    }
    Ok(())
}

async fn wait_for_topic_partitions(
    client: &Client,
    topic_name: &str,
    partitions: i32,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(actual) = load_topic_partition_count(client, topic_name).await? {
            ensure_topic_partition_count(topic_name, actual, partitions)?;
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("workflow topic {topic_name} did not appear in metadata before timeout");
        }
        sleep(Duration::from_millis(200)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::{BrokerConfig, build_workflow_topic_topology, ensure_topic_partition_count};

    #[test]
    fn rejects_topic_partition_shortfall() {
        let error = ensure_topic_partition_count("workflow-events", 2, 4).unwrap_err();
        assert_eq!(
            error.to_string(),
            "workflow topic workflow-events has 2 partitions but configuration requires 4"
        );
    }

    #[test]
    fn topology_marks_missing_topic_unhealthy() {
        let config = BrokerConfig::new("localhost:29092", "workflow-events", 4);
        let topology = build_workflow_topic_topology(&config, None);
        assert!(!topology.healthy);
        assert_eq!(topology.status, "topic_missing");
        assert_eq!(topology.configured_partition_ids, vec![0, 1, 2, 3]);
        assert!(topology.actual_partition_ids.is_empty());
    }

    #[test]
    fn topology_marks_partition_shortfall_unhealthy() {
        let config = BrokerConfig::new("localhost:29092", "workflow-events", 4);
        let topology = build_workflow_topic_topology(&config, Some(2));
        assert!(!topology.healthy);
        assert_eq!(
            topology.status,
            "partition_mismatch: topic has 2 partitions but configuration requires 4"
        );
        assert_eq!(topology.actual_partition_ids, vec![0, 1]);
    }

    #[test]
    fn topology_marks_exact_match_healthy() {
        let config = BrokerConfig::new("localhost:29092", "workflow-events", 4);
        let topology = build_workflow_topic_topology(&config, Some(4));
        assert!(topology.healthy);
        assert_eq!(topology.status, "ok");
        assert_eq!(topology.actual_partition_ids, vec![0, 1, 2, 3]);
    }
}
