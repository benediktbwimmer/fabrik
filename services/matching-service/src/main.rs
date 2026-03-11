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
use fabrik_events::{
    EventEnvelope, WorkflowEvent, WorkflowIdentity, WorkflowTurnRouting, workflow_turn_routing,
};
use fabrik_store::{
    TaskQueueKind, WorkflowActivityRecord, WorkflowActivityStatus, WorkflowStore, WorkflowTaskRecord,
};
use fabrik_worker_protocol::activity_worker::{
    Ack, ActivityTask, CompleteActivityTaskRequest, FailActivityTaskRequest,
    PollActivityTaskRequest, PollActivityTaskResponse, PollWorkflowTaskRequest,
    PollWorkflowTaskResponse, RecordActivityHeartbeatRequest, RecordActivityHeartbeatResponse,
    ReportActivityTaskCancelledRequest, CompleteWorkflowTaskRequest, FailWorkflowTaskRequest,
    WorkflowTask,
    activity_worker_api_server::{ActivityWorkerApi, ActivityWorkerApiServer},
    workflow_worker_api_server::{WorkflowWorkerApi, WorkflowWorkerApiServer},
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
    workflow_notify: Arc<Notify>,
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
    let workflow_notify = Arc::new(Notify::new());
    rebuild_pending_queues(&store, &queues, runtime.max_rebuild_tasks).await?;

    let state = AppState {
        store: store.clone(),
        publisher: publisher.clone(),
        queues: queues.clone(),
        workflow_notify: workflow_notify.clone(),
        runtime: runtime.clone(),
    };
    let consumer =
        build_workflow_consumer(&broker, "matching-service", &broker.all_partition_ids()).await?;
    tokio::spawn(run_event_loop(state.clone(), consumer));
    tokio::spawn(run_timeout_loop(state.clone()));

    let addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, config.port));
    info!(%addr, "matching-service listening");
    Server::builder()
        .add_service(ActivityWorkerApiServer::new(ActivityApi { state: state.clone() }))
        .add_service(WorkflowWorkerApiServer::new(ActivityApi { state: state.clone() }))
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
                    if let Err(error) = process_event(&state, record.partition_id, event).await {
                        error!(error = %error, "matching-service failed to process event");
                    }
                }
                Err(error) => warn!(error = %error, "matching-service skipping invalid event"),
            },
            Err(error) => error!(error = %error, "matching-service consumer error"),
        }
    }
}

async fn process_event(
    state: &AppState,
    partition_id: i32,
    event: EventEnvelope<WorkflowEvent>,
) -> Result<()> {
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

    if matches!(workflow_turn_routing(&event.payload), WorkflowTurnRouting::MatchingPoller) {
        let task_queue = resolve_workflow_task_queue(state, &event).await?;
        let preferred_build_id = state
            .store
            .get_run_record(&event.tenant_id, &event.instance_id, &event.run_id)
            .await?
            .and_then(|run| run.sticky_workflow_build_id);
        let inserted = state
            .store
            .enqueue_workflow_task(partition_id, &task_queue, preferred_build_id.as_deref(), &event)
            .await?;
        if inserted.is_some() {
            state.workflow_notify.notify_waiters();
        }
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
        if !self
            .state
            .store
            .is_build_compatible_with_queue(
                &request.tenant_id,
                TaskQueueKind::Activity,
                &request.task_queue,
                &request.worker_build_id,
            )
            .await
            .map_err(internal_status)?
        {
            return Err(Status::permission_denied("worker build is not compatible with task queue"));
        }
        self.state
            .store
            .upsert_queue_poller(
                &request.tenant_id,
                TaskQueueKind::Activity,
                &request.task_queue,
                &request.worker_id,
                &request.worker_build_id,
                None,
                None,
                chrono::Duration::seconds(self.state.runtime.lease_ttl_seconds as i64),
            )
            .await
            .map_err(internal_status)?;

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

#[tonic::async_trait]
impl WorkflowWorkerApi for ActivityApi {
    async fn poll_workflow_task(
        &self,
        request: Request<PollWorkflowTaskRequest>,
    ) -> Result<Response<PollWorkflowTaskResponse>, Status> {
        let request = request.into_inner();
        if request.worker_id.trim().is_empty() {
            return Err(Status::invalid_argument("worker_id is required"));
        }
        if request.worker_build_id.trim().is_empty() {
            return Err(Status::invalid_argument("worker_build_id is required"));
        }

        let timeout_ms = if request.poll_timeout_ms == 0 {
            30_000
        } else {
            request.poll_timeout_ms
        };
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
        heartbeat_workflow_poller_queues(
            &self.state,
            &request.worker_id,
            &request.worker_build_id,
            request.partition_id,
        )
        .await
        .map_err(internal_status)?;

        loop {
            if let Some(record) = self
                .state
                .store
                .lease_next_workflow_task(
                    request.partition_id,
                    &request.worker_id,
                    &request.worker_build_id,
                    chrono::Duration::seconds(self.state.runtime.lease_ttl_seconds as i64),
                )
                .await
                .map_err(internal_status)?
            {
                self.state
                    .store
                    .upsert_queue_poller(
                        &record.tenant_id,
                        TaskQueueKind::Workflow,
                        &record.task_queue,
                        &request.worker_id,
                        &request.worker_build_id,
                        Some(request.partition_id),
                        None,
                        chrono::Duration::seconds(self.state.runtime.lease_ttl_seconds as i64),
                    )
                    .await
                    .map_err(internal_status)?;
                return Ok(Response::new(PollWorkflowTaskResponse {
                    task: Some(workflow_task_to_proto(&record)),
                }));
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(Response::new(PollWorkflowTaskResponse { task: None }));
            }
            let remaining = deadline.saturating_duration_since(now);
            if tokio::time::timeout(remaining, self.state.workflow_notify.notified())
                .await
                .is_err()
            {
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
        self.state
            .store
            .complete_workflow_task(task_id, &request.worker_id, &request.worker_build_id, Utc::now())
            .await
            .map_err(internal_status)?;
        Ok(Response::new(Ack {}))
    }

    async fn fail_workflow_task(
        &self,
        request: Request<FailWorkflowTaskRequest>,
    ) -> Result<Response<Ack>, Status> {
        let request = request.into_inner();
        let task_id = Uuid::parse_str(&request.task_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let failed = self
            .state
            .store
            .fail_workflow_task(
                task_id,
                &request.worker_id,
                &request.worker_build_id,
                &request.error,
                Utc::now(),
            )
            .await
            .map_err(internal_status)?;
        if failed {
            self.state.workflow_notify.notify_waiters();
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

fn workflow_task_to_proto(record: &WorkflowTaskRecord) -> WorkflowTask {
    WorkflowTask {
        task_id: record.task_id.to_string(),
        tenant_id: record.tenant_id.clone(),
        definition_id: record.definition_id.clone(),
        definition_version: record.definition_version.unwrap_or_default(),
        artifact_hash: record.artifact_hash.clone().unwrap_or_default(),
        instance_id: record.instance_id.clone(),
        run_id: record.run_id.clone(),
        partition_id: record.partition_id,
        task_queue: record.task_queue.clone(),
        preferred_build_id: record.preferred_build_id.clone().unwrap_or_default(),
        source_event_id: record.source_event_id.to_string(),
        source_event_type: record.source_event_type.clone(),
        source_event_json: serde_json::to_string(&record.source_event)
            .expect("workflow task event serializes"),
        attempt_count: record.attempt_count,
        created_at_unix_ms: record.created_at.timestamp_millis(),
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

async fn heartbeat_workflow_poller_queues(
    state: &AppState,
    worker_id: &str,
    worker_build_id: &str,
    partition_id: i32,
) -> Result<()> {
    let ttl = chrono::Duration::seconds(state.runtime.lease_ttl_seconds as i64);
    for (tenant_id, task_queue) in state
        .store
        .list_queues_for_build(TaskQueueKind::Workflow, worker_build_id)
        .await?
    {
        state
            .store
            .upsert_queue_poller(
                &tenant_id,
                TaskQueueKind::Workflow,
                &task_queue,
                worker_id,
                worker_build_id,
                Some(partition_id),
                None,
                ttl,
            )
            .await?;
    }
    Ok(())
}

async fn resolve_workflow_task_queue(
    state: &AppState,
    event: &EventEnvelope<WorkflowEvent>,
) -> Result<String> {
    if let Some(task_queue) = event.metadata.get("workflow_task_queue") {
        return Ok(task_queue.clone());
    }
    Ok(state
        .store
        .get_run_record(&event.tenant_id, &event.instance_id, &event.run_id)
        .await?
        .map(|run| run.workflow_task_queue)
        .unwrap_or_else(|| "default".to_owned()))
}

fn internal_status(error: anyhow::Error) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use fabrik_broker::build_workflow_consumer;
    use fabrik_worker_protocol::activity_worker::activity_worker_api_server::ActivityWorkerApi;
    use serde_json::json;
    use std::{
        net::TcpListener,
        process::Command,
        time::{Duration as StdDuration, Instant},
    };
    use tokio::time::sleep;
    use tonic::Request;

    fn demo_event(payload: WorkflowEvent) -> EventEnvelope<WorkflowEvent> {
        EventEnvelope::new(
            payload.event_type(),
            WorkflowIdentity::new(
                "tenant-a".to_owned(),
                "demo".to_owned(),
                1,
                "artifact".to_owned(),
                "wf-1".to_owned(),
                "run-1".to_owned(),
                "test",
            ),
            payload,
        )
    }

    struct TestPostgres {
        container_name: String,
        database_url: String,
    }

    impl TestPostgres {
        fn start() -> Result<Option<Self>> {
            if !docker_available() {
                eprintln!("skipping matching-service integration tests because docker is unavailable");
                return Ok(None);
            }

            let container_name = format!("fabrik-matching-test-pg-{}", Uuid::now_v7());
            let image =
                std::env::var("FABRIK_TEST_POSTGRES_IMAGE").unwrap_or_else(|_| "postgres:16-alpine".to_owned());
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
            let database_url =
                format!("postgres://fabrik:fabrik@127.0.0.1:{host_port}/fabrik_test?sslmode=disable");
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
                eprintln!("skipping matching-service integration tests because docker is unavailable");
                return Ok(None);
            }

            let kafka_port = choose_free_port().context("failed to allocate kafka host port")?;
            let container_name = format!("fabrik-matching-test-rp-{}", Uuid::now_v7());
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
                match WorkflowPublisher::new(&self.broker, "matching-service-test").await {
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

    fn docker_logs(container_name: &str) -> Result<String> {
        let output = Command::new("docker")
            .args(["logs", container_name])
            .output()
            .with_context(|| format!("failed to read docker logs for {container_name}"))?;
        Ok(format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ))
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

    fn choose_free_port() -> Result<u16> {
        let listener =
            TcpListener::bind("127.0.0.1:0").context("failed to bind ephemeral port")?;
        let port = listener.local_addr().context("failed to read socket address")?.port();
        drop(listener);
        Ok(port)
    }

    async fn wait_for_workflow_task_count(
        store: &WorkflowStore,
        tenant_id: &str,
        task_queue: &str,
        expected_backlog: u64,
        timeout: StdDuration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let inspection = store
                .inspect_task_queue(tenant_id, TaskQueueKind::Workflow, task_queue, Utc::now())
                .await?;
            if inspection.backlog == expected_backlog {
                return Ok(());
            }
            if Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for workflow backlog {expected_backlog}");
            }
            sleep(StdDuration::from_millis(50)).await;
        }
    }

    fn test_runtime() -> MatchingRuntimeConfig {
        MatchingRuntimeConfig {
            lease_ttl_seconds: 30,
            heartbeat_publish_interval_ms: 30_000,
            sweep_interval_ms: 500,
            max_rebuild_tasks: 10_000,
        }
    }

    fn app_state(store: WorkflowStore, publisher: WorkflowPublisher) -> AppState {
        AppState {
            store,
            publisher,
            queues: Arc::new(QueueIndex::new()),
            workflow_notify: Arc::new(Notify::new()),
            runtime: test_runtime(),
        }
    }

    #[test]
    fn workflow_task_classification_matches_turn_boundaries() {
        assert!(matches!(
            workflow_turn_routing(&WorkflowEvent::WorkflowTriggered {
            input: json!({"ok": true}),
        }),
            WorkflowTurnRouting::MatchingPoller
        ));
        assert!(matches!(
            workflow_turn_routing(&WorkflowEvent::SignalQueued {
            signal_id: "sig-1".to_owned(),
            signal_type: "approved".to_owned(),
            payload: json!(true),
        }),
            WorkflowTurnRouting::MatchingPoller
        ));
        assert!(matches!(
            workflow_turn_routing(&WorkflowEvent::ActivityTaskCompleted {
            activity_id: "a1".to_owned(),
            attempt: 1,
            output: json!({"done": true}),
            worker_id: "worker-1".to_owned(),
            worker_build_id: "build-1".to_owned(),
        }),
            WorkflowTurnRouting::MatchingPoller
        ));
        assert!(matches!(
            workflow_turn_routing(&WorkflowEvent::WorkflowStarted),
            WorkflowTurnRouting::LocalExecutor
        ));
        assert!(matches!(
            workflow_turn_routing(&WorkflowEvent::ActivityTaskScheduled {
            activity_id: "a1".to_owned(),
            activity_type: "demo".to_owned(),
            task_queue: "default".to_owned(),
            attempt: 1,
            input: Value::Null,
            config: None,
            state: None,
            schedule_to_start_timeout_ms: None,
            start_to_close_timeout_ms: None,
            heartbeat_timeout_ms: None,
        }),
            WorkflowTurnRouting::LocalExecutor
        ));
    }

    #[test]
    fn workflow_task_proto_preserves_core_routing_fields() {
        let event = demo_event(WorkflowEvent::WorkflowTriggered { input: json!({"ok": true}) });
        let record = WorkflowTaskRecord {
            task_id: Uuid::now_v7(),
            tenant_id: event.tenant_id.clone(),
            instance_id: event.instance_id.clone(),
            run_id: event.run_id.clone(),
            definition_id: event.definition_id.clone(),
            definition_version: Some(event.definition_version),
            artifact_hash: Some(event.artifact_hash.clone()),
            partition_id: 2,
            task_queue: "payments".to_owned(),
            preferred_build_id: Some("build-a".to_owned()),
            status: fabrik_store::WorkflowTaskStatus::Pending,
            source_event_id: event.event_id,
            source_event_type: event.event_type.clone(),
            source_event: event.clone(),
            lease_poller_id: None,
            lease_build_id: None,
            lease_expires_at: None,
            attempt_count: 0,
            last_error: None,
            created_at: event.occurred_at,
            updated_at: event.occurred_at,
        };

        let proto = workflow_task_to_proto(&record);
        assert_eq!(proto.partition_id, 2);
        assert_eq!(proto.task_queue, "payments");
        assert_eq!(proto.preferred_build_id, "build-a");
        assert_eq!(proto.source_event_type, "WorkflowTriggered");
        assert!(proto.source_event_json.contains("WorkflowTriggered"));
    }

    #[tokio::test]
    async fn activity_poll_rejects_incompatible_builds() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let publisher = redpanda.connect_publisher().await?;
        let state = app_state(store.clone(), publisher);
        let api = ActivityApi { state: state.clone() };

        store
            .register_task_queue_build(
                "tenant-a",
                TaskQueueKind::Activity,
                "payments",
                "build-a",
                &[],
                None,
            )
            .await?;
        store
            .register_task_queue_build(
                "tenant-a",
                TaskQueueKind::Activity,
                "payments",
                "build-b",
                &[],
                None,
            )
            .await?;
        store
            .upsert_compatibility_set(
                "tenant-a",
                TaskQueueKind::Activity,
                "payments",
                "stable",
                &["build-a".to_owned()],
                true,
            )
            .await?;

        let status = api
            .poll_activity_task(Request::new(PollActivityTaskRequest {
                tenant_id: "tenant-a".to_owned(),
                task_queue: "payments".to_owned(),
                worker_id: "worker-b".to_owned(),
                worker_build_id: "build-b".to_owned(),
                poll_timeout_ms: 1,
            }))
            .await
            .expect_err("incompatible activity worker should be rejected");
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        Ok(())
    }

    #[tokio::test]
    async fn activity_default_set_promotion_changes_routing_for_new_tasks_only() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let publisher = redpanda.connect_publisher().await?;
        let state = app_state(store.clone(), publisher);
        let api = ActivityApi { state: state.clone() };

        store
            .register_task_queue_build(
                "tenant-a",
                TaskQueueKind::Activity,
                "payments",
                "build-a",
                &[],
                None,
            )
            .await?;
        store
            .register_task_queue_build(
                "tenant-a",
                TaskQueueKind::Activity,
                "payments",
                "build-b",
                &[],
                None,
            )
            .await?;
        store
            .upsert_compatibility_set(
                "tenant-a",
                TaskQueueKind::Activity,
                "payments",
                "stable",
                &["build-a".to_owned()],
                true,
            )
            .await?;
        store
            .upsert_compatibility_set(
                "tenant-a",
                TaskQueueKind::Activity,
                "payments",
                "canary",
                &["build-b".to_owned()],
                false,
            )
            .await?;

        process_event(
            &state,
            0,
            demo_event(WorkflowEvent::ActivityTaskScheduled {
                activity_id: "a1".to_owned(),
                activity_type: "charge".to_owned(),
                task_queue: "payments".to_owned(),
                attempt: 1,
                input: json!({"amount": 10}),
                config: None,
                state: Some("pay".to_owned()),
                schedule_to_start_timeout_ms: None,
                start_to_close_timeout_ms: None,
                heartbeat_timeout_ms: None,
            }),
        )
        .await?;

        let first = api
            .poll_activity_task(Request::new(PollActivityTaskRequest {
                tenant_id: "tenant-a".to_owned(),
                task_queue: "payments".to_owned(),
                worker_id: "worker-a".to_owned(),
                worker_build_id: "build-a".to_owned(),
                poll_timeout_ms: 1,
            }))
            .await?
            .into_inner()
            .task
            .context("build-a should receive first task")?;
        assert_eq!(first.activity_id, "a1");

        store
            .set_default_compatibility_set(
                "tenant-a",
                TaskQueueKind::Activity,
                "payments",
                "canary",
            )
            .await?;

        process_event(
            &state,
            0,
            demo_event(WorkflowEvent::ActivityTaskScheduled {
                activity_id: "a2".to_owned(),
                activity_type: "charge".to_owned(),
                task_queue: "payments".to_owned(),
                attempt: 1,
                input: json!({"amount": 20}),
                config: None,
                state: Some("pay".to_owned()),
                schedule_to_start_timeout_ms: None,
                start_to_close_timeout_ms: None,
                heartbeat_timeout_ms: None,
            }),
        )
        .await?;

        let build_a_denied = api
            .poll_activity_task(Request::new(PollActivityTaskRequest {
                tenant_id: "tenant-a".to_owned(),
                task_queue: "payments".to_owned(),
                worker_id: "worker-a".to_owned(),
                worker_build_id: "build-a".to_owned(),
                poll_timeout_ms: 1,
            }))
            .await
            .expect_err("promoted default set should reject old build");
        assert_eq!(build_a_denied.code(), tonic::Code::PermissionDenied);

        let second = api
            .poll_activity_task(Request::new(PollActivityTaskRequest {
                tenant_id: "tenant-a".to_owned(),
                task_queue: "payments".to_owned(),
                worker_id: "worker-b".to_owned(),
                worker_build_id: "build-b".to_owned(),
                poll_timeout_ms: 1,
            }))
            .await?
            .into_inner()
            .task
            .context("build-b should receive promoted task")?;
        assert_eq!(second.activity_id, "a2");

        let first_record = store
            .get_activity_attempt("tenant-a", "wf-1", "run-1", "a1", 1)
            .await?
            .context("first activity attempt should exist")?;
        assert_eq!(first_record.status, WorkflowActivityStatus::Started);
        assert_eq!(first_record.worker_build_id.as_deref(), Some("build-a"));
        Ok(())
    }

    #[tokio::test]
    async fn broker_event_loop_materializes_workflow_tasks() -> Result<()> {
        let Some(postgres) = TestPostgres::start()? else {
            return Ok(());
        };
        let Some(redpanda) = TestRedpanda::start()? else {
            return Ok(());
        };
        let store = postgres.connect_store().await?;
        let publisher = redpanda.connect_publisher().await?;
        let state = app_state(store.clone(), publisher.clone());
        let consumer =
            build_workflow_consumer(&redpanda.broker, "matching-service-test", &redpanda.broker.all_partition_ids())
                .await?;
        let event_loop = tokio::spawn(run_event_loop(state.clone(), consumer));

        let identity = WorkflowIdentity::new(
            "tenant-a".to_owned(),
            "demo".to_owned(),
            1,
            "artifact-a".to_owned(),
            "wf-broker".to_owned(),
            "run-broker".to_owned(),
            "test",
        );
        let mut trigger = EventEnvelope::new(
            WorkflowEvent::WorkflowTriggered { input: json!({"ok": true}) }.event_type(),
            identity,
            WorkflowEvent::WorkflowTriggered { input: json!({"ok": true}) },
        );
        trigger
            .metadata
            .insert("workflow_task_queue".to_owned(), "payments".to_owned());

        publisher.publish(&trigger, &trigger.partition_key).await?;

        wait_for_workflow_task_count(&store, "tenant-a", "payments", 1, StdDuration::from_secs(10))
            .await?;

        let inspection = store
            .inspect_task_queue("tenant-a", TaskQueueKind::Workflow, "payments", Utc::now())
            .await?;
        assert_eq!(inspection.backlog, 1);

        event_loop.abort();
        let _ = event_loop.await;
        Ok(())
    }
}
