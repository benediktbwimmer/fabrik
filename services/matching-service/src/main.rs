use std::{
    collections::{HashMap, VecDeque},
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use chrono::{DateTime, Utc};
use fabrik_broker::{
    BrokerConfig, WorkflowPublisher, build_workflow_consumer, decode_consumed_workflow_event,
};
use fabrik_config::{GrpcServiceConfig, MatchingRuntimeConfig, PostgresConfig, RedpandaConfig};
use fabrik_events::{EventEnvelope, WorkflowEvent, WorkflowIdentity};
use fabrik_store::{WorkflowActivityRecord, WorkflowActivityStatus, WorkflowStore};
use fabrik_worker_protocol::activity_worker::{
    Ack, ActivityTask, CompleteActivityTaskRequest, FailActivityTaskRequest,
    PollActivityTaskRequest, PollActivityTaskResponse, RecordActivityHeartbeatRequest,
    RecordActivityHeartbeatResponse, ReportActivityTaskCancelledRequest,
    activity_worker_api_server::{ActivityWorkerApi, ActivityWorkerApiServer},
};
use futures_util::StreamExt;
use serde_json::Value;
use tokio::sync::{Mutex, Notify};
use tonic::{Request, Response, Status, transport::Server};
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    store: WorkflowStore,
    publisher: WorkflowPublisher,
    queues: Arc<QueueIndex>,
    runtime: MatchingRuntimeConfig,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ActivityKey {
    tenant_id: String,
    instance_id: String,
    run_id: String,
    activity_id: String,
    attempt: u32,
}

struct QueueIndex {
    queues: Mutex<HashMap<String, VecDeque<ActivityKey>>>,
    notify: Notify,
}

impl QueueIndex {
    fn new() -> Self {
        Self { queues: Mutex::new(HashMap::new()), notify: Notify::new() }
    }

    async fn enqueue(&self, queue_key: &str, key: ActivityKey) {
        let mut queues = self.queues.lock().await;
        queues.entry(queue_key.to_owned()).or_default().push_back(key);
        drop(queues);
        self.notify.notify_waiters();
    }

    async fn pop(&self, queue_key: &str) -> Option<ActivityKey> {
        let mut queues = self.queues.lock().await;
        queues.get_mut(queue_key).and_then(VecDeque::pop_front)
    }
}

#[derive(Clone)]
struct ActivityApi {
    state: AppState,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = GrpcServiceConfig::from_env("MATCHING_SERVICE", "matching-service", 50051)?;
    let redpanda = RedpandaConfig::from_env()?;
    let postgres = PostgresConfig::from_env()?;
    let runtime = MatchingRuntimeConfig::from_env()?;
    fabrik_service::init_tracing(&config.log_filter);

    let broker = BrokerConfig::new(
        redpanda.brokers,
        redpanda.workflow_events_topic,
        redpanda.workflow_events_partitions,
    );
    let store = WorkflowStore::connect(&postgres.url).await?;
    store.init().await?;
    let publisher = WorkflowPublisher::new(&broker, "matching-service").await?;
    let queues = Arc::new(QueueIndex::new());
    rebuild_pending_queues(&store, &queues, runtime.max_rebuild_tasks).await?;

    let state = AppState {
        store: store.clone(),
        publisher: publisher.clone(),
        queues: queues.clone(),
        runtime: runtime.clone(),
    };
    let consumer =
        build_workflow_consumer(&broker, "matching-service", &broker.all_partition_ids()).await?;
    tokio::spawn(run_event_loop(state.clone(), consumer));
    tokio::spawn(run_timeout_loop(state.clone()));

    let addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, config.port));
    info!(%addr, "matching-service listening");
    Server::builder()
        .add_service(ActivityWorkerApiServer::new(ActivityApi { state }))
        .serve(addr)
        .await?;
    Ok(())
}

async fn rebuild_pending_queues(
    store: &WorkflowStore,
    queues: &Arc<QueueIndex>,
    limit: i64,
) -> Result<()> {
    for record in store.list_runnable_activities(limit).await? {
        queues
            .enqueue(&queue_key(&record.tenant_id, &record.task_queue), key_from_record(&record))
            .await;
    }
    Ok(())
}

async fn run_event_loop(state: AppState, consumer: fabrik_broker::WorkflowConsumerStream) {
    let mut stream = consumer;
    while let Some(message) = stream.next().await {
        match message {
            Ok(record) => match decode_consumed_workflow_event(&record) {
                Ok(event) => {
                    if let Err(error) = process_event(&state, event).await {
                        error!(error = %error, "matching-service failed to process event");
                    }
                }
                Err(error) => warn!(error = %error, "matching-service skipping invalid event"),
            },
            Err(error) => error!(error = %error, "matching-service consumer error"),
        }
    }
}

async fn process_event(state: &AppState, event: EventEnvelope<WorkflowEvent>) -> Result<()> {
    match &event.payload {
        WorkflowEvent::ActivityTaskScheduled {
            activity_id,
            activity_type,
            task_queue,
            attempt,
            input,
            config,
            state: workflow_state,
            schedule_to_start_timeout_ms,
            start_to_close_timeout_ms,
            heartbeat_timeout_ms,
        } => {
            state
                .store
                .upsert_activity_scheduled(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    &event.definition_id,
                    Some(event.definition_version),
                    Some(&event.artifact_hash),
                    activity_id,
                    *attempt,
                    activity_type,
                    task_queue,
                    workflow_state.as_deref(),
                    input,
                    config.as_ref(),
                    *schedule_to_start_timeout_ms,
                    *start_to_close_timeout_ms,
                    *heartbeat_timeout_ms,
                    event.event_id,
                    &event.event_type,
                    event.occurred_at,
                )
                .await?;
            state
                .queues
                .enqueue(
                    &queue_key(&event.tenant_id, task_queue),
                    ActivityKey {
                        tenant_id: event.tenant_id.clone(),
                        instance_id: event.instance_id.clone(),
                        run_id: event.run_id.clone(),
                        activity_id: activity_id.clone(),
                        attempt: *attempt,
                    },
                )
                .await;
        }
        WorkflowEvent::ActivityTaskCancellationRequested {
            activity_id,
            attempt,
            reason,
            metadata,
        } => {
            if state
                .store
                .request_activity_cancellation(
                    &event.tenant_id,
                    &event.instance_id,
                    &event.run_id,
                    activity_id,
                    *attempt,
                    reason,
                    metadata.as_ref(),
                    event.event_id,
                    &event.event_type,
                    event.occurred_at,
                )
                .await?
            {
                if let Some(record) = state
                    .store
                    .get_activity_attempt(
                        &event.tenant_id,
                        &event.instance_id,
                        &event.run_id,
                        activity_id,
                        *attempt,
                    )
                    .await?
                {
                    if record.status == WorkflowActivityStatus::Scheduled {
                        publish_cancelled_event(
                            state,
                            &record,
                            "matching-service",
                            "system",
                            reason,
                            metadata.as_ref(),
                        )
                        .await?;
                    }
                }
            }
        }
        _ => {}
    }

    Ok(())
}

async fn run_timeout_loop(state: AppState) {
    loop {
        if let Err(error) = sweep_activities(&state).await {
            error!(error = %error, "matching-service activity sweep failed");
        }
        tokio::time::sleep(Duration::from_millis(state.runtime.sweep_interval_ms)).await;
    }
}

async fn sweep_activities(state: &AppState) -> Result<()> {
    let now = Utc::now();
    for record in state.store.list_runnable_activities(state.runtime.max_rebuild_tasks).await? {
        if let Some(timeout_ms) = record.schedule_to_start_timeout_ms {
            let deadline = record.scheduled_at + chrono::Duration::milliseconds(timeout_ms as i64);
            if deadline <= now {
                publish_timed_out_event(state, &record).await?;
            }
        }
    }
    for record in state.store.list_started_activities(state.runtime.max_rebuild_tasks).await? {
        if let Some(started_at) = record.started_at {
            if let Some(timeout_ms) = record.start_to_close_timeout_ms {
                let deadline = started_at + chrono::Duration::milliseconds(timeout_ms as i64);
                if deadline <= now {
                    publish_timed_out_event(state, &record).await?;
                    continue;
                }
            }
        }
        let heartbeat_base = record.last_heartbeat_at.or(record.started_at);
        if let (Some(base), Some(timeout_ms)) = (heartbeat_base, record.heartbeat_timeout_ms) {
            let deadline = base + chrono::Duration::milliseconds(timeout_ms as i64);
            if deadline <= now {
                publish_timed_out_event(state, &record).await?;
                continue;
            }
        }
        if record.lease_expires_at.is_some_and(|expires_at| expires_at <= now) {
            if state
                .store
                .requeue_activity(
                    &record.tenant_id,
                    &record.instance_id,
                    &record.run_id,
                    &record.activity_id,
                    record.attempt,
                    now,
                )
                .await?
            {
                state
                    .queues
                    .enqueue(
                        &queue_key(&record.tenant_id, &record.task_queue),
                        key_from_record(&record),
                    )
                    .await;
            }
        }
    }

    Ok(())
}

#[tonic::async_trait]
impl ActivityWorkerApi for ActivityApi {
    async fn poll_activity_task(
        &self,
        request: Request<PollActivityTaskRequest>,
    ) -> Result<Response<PollActivityTaskResponse>, Status> {
        let request = request.into_inner();
        if request.tenant_id.trim().is_empty() {
            return Err(Status::invalid_argument("tenant_id is required"));
        }
        if request.task_queue.trim().is_empty() {
            return Err(Status::invalid_argument("task_queue is required"));
        }
        if request.worker_id.trim().is_empty() {
            return Err(Status::invalid_argument("worker_id is required"));
        }
        if request.worker_build_id.trim().is_empty() {
            return Err(Status::invalid_argument("worker_build_id is required"));
        }
        let timeout_ms =
            if request.poll_timeout_ms == 0 { 30_000 } else { request.poll_timeout_ms };
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);

        loop {
            if let Some(record) = lease_next_activity(
                &self.state,
                &request.tenant_id,
                &request.task_queue,
                &request.worker_id,
                &request.worker_build_id,
            )
            .await
            .map_err(internal_status)?
            {
                return Ok(Response::new(PollActivityTaskResponse {
                    task: Some(record_to_proto(&record)),
                }));
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(Response::new(PollActivityTaskResponse { task: None }));
            }
            let remaining = deadline.saturating_duration_since(now);
            if tokio::time::timeout(remaining, self.state.queues.notify.notified()).await.is_err() {
                return Ok(Response::new(PollActivityTaskResponse { task: None }));
            }
        }
    }

    async fn complete_activity_task(
        &self,
        request: Request<CompleteActivityTaskRequest>,
    ) -> Result<Response<Ack>, Status> {
        let request = request.into_inner();
        let output: Value = serde_json::from_str(&request.output_json)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let event_id = Uuid::now_v7();
        let occurred_at = Utc::now();
        let updated = self
            .state
            .store
            .complete_activity(
                &request.tenant_id,
                &request.instance_id,
                &request.run_id,
                &request.activity_id,
                request.attempt,
                &output,
                &request.worker_id,
                &request.worker_build_id,
                event_id,
                "ActivityTaskCompleted",
                occurred_at,
            )
            .await
            .map_err(internal_status)?;
        if updated {
            let record = self
                .state
                .store
                .get_activity_attempt(
                    &request.tenant_id,
                    &request.instance_id,
                    &request.run_id,
                    &request.activity_id,
                    request.attempt,
                )
                .await
                .map_err(internal_status)?
                .ok_or_else(|| Status::not_found("activity attempt not found after completion"))?;
            publish_activity_event(
                &self.state.publisher,
                &record,
                WorkflowEvent::ActivityTaskCompleted {
                    activity_id: request.activity_id,
                    attempt: request.attempt,
                    output,
                    worker_id: request.worker_id,
                    worker_build_id: request.worker_build_id,
                },
                event_id,
                occurred_at,
            )
            .await
            .map_err(internal_status)?;
        }
        Ok(Response::new(Ack {}))
    }

    async fn fail_activity_task(
        &self,
        request: Request<FailActivityTaskRequest>,
    ) -> Result<Response<Ack>, Status> {
        let request = request.into_inner();
        let event_id = Uuid::now_v7();
        let occurred_at = Utc::now();
        let updated = self
            .state
            .store
            .fail_activity(
                &request.tenant_id,
                &request.instance_id,
                &request.run_id,
                &request.activity_id,
                request.attempt,
                WorkflowActivityStatus::Failed,
                &request.error,
                None,
                &request.worker_id,
                &request.worker_build_id,
                event_id,
                "ActivityTaskFailed",
                occurred_at,
            )
            .await
            .map_err(internal_status)?;
        if updated {
            let record = self
                .state
                .store
                .get_activity_attempt(
                    &request.tenant_id,
                    &request.instance_id,
                    &request.run_id,
                    &request.activity_id,
                    request.attempt,
                )
                .await
                .map_err(internal_status)?
                .ok_or_else(|| Status::not_found("activity attempt not found after failure"))?;
            publish_activity_event(
                &self.state.publisher,
                &record,
                WorkflowEvent::ActivityTaskFailed {
                    activity_id: request.activity_id,
                    attempt: request.attempt,
                    error: request.error,
                    worker_id: request.worker_id,
                    worker_build_id: request.worker_build_id,
                },
                event_id,
                occurred_at,
            )
            .await
            .map_err(internal_status)?;
        }
        Ok(Response::new(Ack {}))
    }

    async fn record_activity_heartbeat(
        &self,
        request: Request<RecordActivityHeartbeatRequest>,
    ) -> Result<Response<RecordActivityHeartbeatResponse>, Status> {
        let request = request.into_inner();
        let details = if request.details_json.trim().is_empty() {
            None
        } else {
            Some(
                serde_json::from_str::<Value>(&request.details_json)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?,
            )
        };
        let event_id = Uuid::now_v7();
        let occurred_at = Utc::now();
        let lease_expires_at =
            occurred_at + chrono::Duration::seconds(self.state.runtime.lease_ttl_seconds as i64);
        let previous_record = self
            .state
            .store
            .get_activity_attempt(
                &request.tenant_id,
                &request.instance_id,
                &request.run_id,
                &request.activity_id,
                request.attempt,
            )
            .await
            .map_err(internal_status)?;
        let result = self
            .state
            .store
            .record_activity_heartbeat(
                &request.tenant_id,
                &request.instance_id,
                &request.run_id,
                &request.activity_id,
                request.attempt,
                &request.worker_id,
                &request.worker_build_id,
                lease_expires_at,
                event_id,
                "ActivityTaskHeartbeatRecorded",
                occurred_at,
            )
            .await
            .map_err(internal_status)?;
        let publish_heartbeat = result.updated
            && previous_record
                .as_ref()
                .and_then(|record| record.last_heartbeat_at.or(record.started_at))
                .map(|last_progress| {
                    occurred_at.signed_duration_since(last_progress).num_milliseconds()
                        >= self.state.runtime.heartbeat_publish_interval_ms as i64
                })
                .unwrap_or(true);
        if publish_heartbeat {
            let record = self
                .state
                .store
                .get_activity_attempt(
                    &request.tenant_id,
                    &request.instance_id,
                    &request.run_id,
                    &request.activity_id,
                    request.attempt,
                )
                .await
                .map_err(internal_status)?
                .ok_or_else(|| Status::not_found("activity attempt not found after heartbeat"))?;
            publish_activity_event(
                &self.state.publisher,
                &record,
                WorkflowEvent::ActivityTaskHeartbeatRecorded {
                    activity_id: request.activity_id,
                    attempt: request.attempt,
                    worker_id: request.worker_id,
                    worker_build_id: request.worker_build_id,
                    details,
                },
                event_id,
                occurred_at,
            )
            .await
            .map_err(internal_status)?;
        }
        Ok(Response::new(RecordActivityHeartbeatResponse {
            cancellation_requested: result.cancellation_requested,
        }))
    }

    async fn report_activity_task_cancelled(
        &self,
        request: Request<ReportActivityTaskCancelledRequest>,
    ) -> Result<Response<Ack>, Status> {
        let request = request.into_inner();
        let metadata = if request.metadata_json.trim().is_empty() {
            None
        } else {
            Some(
                serde_json::from_str::<Value>(&request.metadata_json)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?,
            )
        };
        let event_id = Uuid::now_v7();
        let occurred_at = Utc::now();
        let updated = self
            .state
            .store
            .fail_activity(
                &request.tenant_id,
                &request.instance_id,
                &request.run_id,
                &request.activity_id,
                request.attempt,
                WorkflowActivityStatus::Cancelled,
                &request.reason,
                metadata.as_ref(),
                &request.worker_id,
                &request.worker_build_id,
                event_id,
                "ActivityTaskCancelled",
                occurred_at,
            )
            .await
            .map_err(internal_status)?;
        if updated {
            let record = self
                .state
                .store
                .get_activity_attempt(
                    &request.tenant_id,
                    &request.instance_id,
                    &request.run_id,
                    &request.activity_id,
                    request.attempt,
                )
                .await
                .map_err(internal_status)?
                .ok_or_else(|| Status::not_found("activity attempt not found after cancel"))?;
            publish_activity_event(
                &self.state.publisher,
                &record,
                WorkflowEvent::ActivityTaskCancelled {
                    activity_id: request.activity_id,
                    attempt: request.attempt,
                    reason: request.reason,
                    worker_id: request.worker_id,
                    worker_build_id: request.worker_build_id,
                    metadata,
                },
                event_id,
                occurred_at,
            )
            .await
            .map_err(internal_status)?;
        }
        Ok(Response::new(Ack {}))
    }
}

async fn lease_next_activity(
    state: &AppState,
    tenant_id: &str,
    task_queue: &str,
    worker_id: &str,
    worker_build_id: &str,
) -> Result<Option<WorkflowActivityRecord>> {
    let queue_key = queue_key(tenant_id, task_queue);
    while let Some(key) = state.queues.pop(&queue_key).await {
        let Some(record) = state
            .store
            .get_activity_attempt(
                &key.tenant_id,
                &key.instance_id,
                &key.run_id,
                &key.activity_id,
                key.attempt,
            )
            .await?
        else {
            continue;
        };

        if record.status != WorkflowActivityStatus::Scheduled
            || record.tenant_id != tenant_id
            || record.task_queue != task_queue
        {
            continue;
        }
        if record.cancellation_requested {
            publish_cancelled_event(
                state,
                &record,
                "matching-service",
                "system",
                "cancelled before dispatch",
                None,
            )
            .await?;
            continue;
        }

        let occurred_at = Utc::now();
        let event_id = Uuid::now_v7();
        let lease_expires_at =
            occurred_at + chrono::Duration::seconds(state.runtime.lease_ttl_seconds as i64);
        if !state
            .store
            .mark_activity_started(
                &record.tenant_id,
                &record.instance_id,
                &record.run_id,
                &record.activity_id,
                record.attempt,
                worker_id,
                worker_build_id,
                lease_expires_at,
                event_id,
                "ActivityTaskStarted",
                occurred_at,
            )
            .await?
        {
            continue;
        }

        let mut leased = record.clone();
        leased.status = WorkflowActivityStatus::Started;
        leased.worker_id = Some(worker_id.to_owned());
        leased.worker_build_id = Some(worker_build_id.to_owned());
        leased.started_at = Some(occurred_at);
        leased.last_heartbeat_at = Some(occurred_at);
        leased.lease_expires_at = Some(lease_expires_at);
        publish_activity_event(
            &state.publisher,
            &leased,
            WorkflowEvent::ActivityTaskStarted {
                activity_id: leased.activity_id.clone(),
                attempt: leased.attempt,
                worker_id: worker_id.to_owned(),
                worker_build_id: worker_build_id.to_owned(),
            },
            event_id,
            occurred_at,
        )
        .await?;
        return Ok(Some(leased));
    }

    Ok(None)
}

async fn publish_timed_out_event(state: &AppState, record: &WorkflowActivityRecord) -> Result<()> {
    let worker_id = record.worker_id.clone().unwrap_or_else(|| "matching-service".to_owned());
    let worker_build_id = record.worker_build_id.clone().unwrap_or_else(|| "system".to_owned());
    let event_id = Uuid::now_v7();
    let occurred_at = Utc::now();
    let updated = state
        .store
        .fail_activity(
            &record.tenant_id,
            &record.instance_id,
            &record.run_id,
            &record.activity_id,
            record.attempt,
            WorkflowActivityStatus::TimedOut,
            "activity timed out",
            None,
            &worker_id,
            &worker_build_id,
            event_id,
            "ActivityTaskTimedOut",
            occurred_at,
        )
        .await?;
    if updated {
        publish_activity_event(
            &state.publisher,
            record,
            WorkflowEvent::ActivityTaskTimedOut {
                activity_id: record.activity_id.clone(),
                attempt: record.attempt,
                worker_id,
                worker_build_id,
            },
            event_id,
            occurred_at,
        )
        .await?;
    }
    Ok(())
}

async fn publish_cancelled_event(
    state: &AppState,
    record: &WorkflowActivityRecord,
    worker_id: &str,
    worker_build_id: &str,
    reason: &str,
    metadata: Option<&Value>,
) -> Result<()> {
    let event_id = Uuid::now_v7();
    let occurred_at = Utc::now();
    let updated = state
        .store
        .fail_activity(
            &record.tenant_id,
            &record.instance_id,
            &record.run_id,
            &record.activity_id,
            record.attempt,
            WorkflowActivityStatus::Cancelled,
            reason,
            metadata,
            worker_id,
            worker_build_id,
            event_id,
            "ActivityTaskCancelled",
            occurred_at,
        )
        .await?;
    if updated {
        publish_activity_event(
            &state.publisher,
            record,
            WorkflowEvent::ActivityTaskCancelled {
                activity_id: record.activity_id.clone(),
                attempt: record.attempt,
                reason: reason.to_owned(),
                worker_id: worker_id.to_owned(),
                worker_build_id: worker_build_id.to_owned(),
                metadata: metadata.cloned(),
            },
            event_id,
            occurred_at,
        )
        .await?;
    }
    Ok(())
}

async fn publish_activity_event(
    publisher: &WorkflowPublisher,
    record: &WorkflowActivityRecord,
    payload: WorkflowEvent,
    event_id: Uuid,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    let mut envelope = EventEnvelope::new(
        payload.event_type(),
        WorkflowIdentity::new(
            record.tenant_id.clone(),
            record.definition_id.clone(),
            record.definition_version.unwrap_or_default(),
            record.artifact_hash.clone().unwrap_or_default(),
            record.instance_id.clone(),
            record.run_id.clone(),
            "matching-service",
        ),
        payload,
    );
    envelope.event_id = event_id;
    envelope.occurred_at = occurred_at;
    envelope.metadata.insert(
        "state".to_owned(),
        record.state.clone().unwrap_or_else(|| record.activity_id.clone()),
    );
    envelope.metadata.insert("attempt".to_owned(), record.attempt.to_string());
    publisher.publish(&envelope, &envelope.partition_key).await?;
    Ok(())
}

fn record_to_proto(record: &WorkflowActivityRecord) -> ActivityTask {
    ActivityTask {
        tenant_id: record.tenant_id.clone(),
        definition_id: record.definition_id.clone(),
        definition_version: record.definition_version.unwrap_or_default(),
        artifact_hash: record.artifact_hash.clone().unwrap_or_default(),
        instance_id: record.instance_id.clone(),
        run_id: record.run_id.clone(),
        activity_id: record.activity_id.clone(),
        activity_type: record.activity_type.clone(),
        task_queue: record.task_queue.clone(),
        attempt: record.attempt,
        input_json: serde_json::to_string(&record.input).expect("activity input serializes"),
        config_json: record
            .config
            .as_ref()
            .map(|config| serde_json::to_string(config).expect("activity config serializes"))
            .unwrap_or_default(),
        state: record.state.clone().unwrap_or_default(),
        scheduled_at_unix_ms: record.scheduled_at.timestamp_millis(),
        lease_expires_at_unix_ms: record
            .lease_expires_at
            .map(|value| value.timestamp_millis())
            .unwrap_or_default(),
        start_to_close_timeout_ms: record.start_to_close_timeout_ms.unwrap_or_default(),
        heartbeat_timeout_ms: record.heartbeat_timeout_ms.unwrap_or_default(),
        cancellation_requested: record.cancellation_requested,
        schedule_to_start_timeout_ms: record.schedule_to_start_timeout_ms.unwrap_or_default(),
    }
}

fn key_from_record(record: &WorkflowActivityRecord) -> ActivityKey {
    ActivityKey {
        tenant_id: record.tenant_id.clone(),
        instance_id: record.instance_id.clone(),
        run_id: record.run_id.clone(),
        activity_id: record.activity_id.clone(),
        attempt: record.attempt,
    }
}

fn queue_key(tenant_id: &str, task_queue: &str) -> String {
    format!("{tenant_id}:{task_queue}")
}

fn internal_status(error: anyhow::Error) -> Status {
    Status::internal(error.to_string())
}
