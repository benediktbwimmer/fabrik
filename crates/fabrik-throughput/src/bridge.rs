use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    ThroughputBatchIdentity, ThroughputCommand, ThroughputCommandEnvelope,
    throughput_bridge_request_id, throughput_partition_key,
};

pub const THROUGHPUT_BRIDGE_PROTOCOL_VERSION: &str = "1";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ThroughputBridgeOperationKind {
    BulkRun,
    StreamJob,
}

impl ThroughputBridgeOperationKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BulkRun => "bulk_run",
            Self::StreamJob => "stream_job",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "bulk_run" => Some(Self::BulkRun),
            "stream_job" => Some(Self::StreamJob),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ThroughputBridgeSubmissionStatus {
    Admitted,
    Published,
    CancellationRequested,
    CancelledBeforePublish,
    Cancelled,
}

impl ThroughputBridgeSubmissionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Published => "published",
            Self::CancellationRequested => "cancellation_requested",
            Self::CancelledBeforePublish => "cancelled_before_publish",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "admitted" => Some(Self::Admitted),
            "published" => Some(Self::Published),
            "cancellation_requested" => Some(Self::CancellationRequested),
            "cancelled_before_publish" => Some(Self::CancelledBeforePublish),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (Self::Admitted, Self::Published)
                    | (Self::Admitted, Self::CancelledBeforePublish)
                    | (Self::Published, Self::CancellationRequested)
                    | (Self::Published, Self::Cancelled)
                    | (Self::CancellationRequested, Self::Cancelled)
            )
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ThroughputRunStatus {
    Scheduled,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl ThroughputRunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "scheduled" => Some(Self::Scheduled),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    pub fn is_active_or_terminal(self) -> bool {
        matches!(self, Self::Running | Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ThroughputBridgeTerminalStatus {
    Completed,
    Failed,
    Cancelled,
}

impl ThroughputBridgeTerminalStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ThroughputBridgeRepairKind {
    PublishStart,
    PublishCancel,
    AcceptTerminal,
    AcceptStreamQuery,
    AcceptStreamCheckpoint,
    AcceptStreamTerminal,
}

impl ThroughputBridgeRepairKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PublishStart => "publish_start",
            Self::PublishCancel => "publish_cancel",
            Self::AcceptTerminal => "accept_terminal",
            Self::AcceptStreamQuery => "accept_stream_query",
            Self::AcceptStreamCheckpoint => "accept_stream_checkpoint",
            Self::AcceptStreamTerminal => "accept_stream_terminal",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum StreamJobBridgeHandleStatus {
    Admitted,
    Running,
    CancellationRequested,
    Completed,
    Failed,
    Cancelled,
}

impl StreamJobBridgeHandleStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Running => "running",
            Self::CancellationRequested => "cancellation_requested",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "admitted" => Some(Self::Admitted),
            "running" => Some(Self::Running),
            "cancellation_requested" => Some(Self::CancellationRequested),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum StreamJobStatus {
    Created,
    Starting,
    Running,
    Draining,
    Completed,
    Failed,
    Cancelled,
}

impl StreamJobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Draining => "draining",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "created" => Some(Self::Created),
            "starting" => Some(Self::Starting),
            "running" => Some(Self::Running),
            "draining" => Some(Self::Draining),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum StreamJobCheckpointStatus {
    Awaiting,
    Reached,
    Accepted,
    Cancelled,
}

impl StreamJobCheckpointStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Awaiting => "awaiting",
            Self::Reached => "reached",
            Self::Accepted => "accepted",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "awaiting" => Some(Self::Awaiting),
            "reached" => Some(Self::Reached),
            "accepted" => Some(Self::Accepted),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum StreamJobQueryStatus {
    Requested,
    Completed,
    Failed,
    Accepted,
    Cancelled,
}

impl StreamJobQueryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Accepted => "accepted",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "requested" => Some(Self::Requested),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "accepted" => Some(Self::Accepted),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum StreamJobQueryConsistency {
    Strong,
    Eventual,
}

impl StreamJobQueryConsistency {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Strong => "strong",
            Self::Eventual => "eventual",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "strong" => Some(Self::Strong),
            "eventual" => Some(Self::Eventual),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamJobBridgeIdentity {
    pub tenant_id: String,
    pub instance_id: String,
    pub run_id: String,
    pub job_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubmitStreamJobRequest {
    pub protocol_version: String,
    pub operation_kind: ThroughputBridgeOperationKind,
    pub bridge_request_id: String,
    pub handle_id: String,
    pub workflow_event_id: Uuid,
    pub workflow_owner_epoch: Option<u64>,
    pub stream_owner_epoch: Option<u64>,
    pub identity: StreamJobBridgeIdentity,
    pub definition_id: String,
    pub definition_version: Option<u32>,
    pub artifact_hash: Option<String>,
    pub job_name: String,
    pub input_ref: String,
    pub config_ref: Option<String>,
    pub checkpoint_policy: Option<Value>,
    pub view_definitions: Option<Value>,
    pub admitted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubmitStreamJobResponse {
    pub handle_id: String,
    pub status: StreamJobBridgeHandleStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AwaitStreamCheckpointRequest {
    pub protocol_version: String,
    pub operation_kind: ThroughputBridgeOperationKind,
    pub bridge_request_id: String,
    pub handle_id: String,
    pub await_request_id: String,
    pub checkpoint_name: String,
    pub workflow_event_id: Uuid,
    pub workflow_owner_epoch: Option<u64>,
    pub requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryStreamJobRequest {
    pub protocol_version: String,
    pub operation_kind: ThroughputBridgeOperationKind,
    pub bridge_request_id: String,
    pub handle_id: String,
    pub query_id: String,
    pub query_name: String,
    pub args: Option<Value>,
    pub consistency: StreamJobQueryConsistency,
    pub workflow_event_id: Uuid,
    pub workflow_owner_epoch: Option<u64>,
    pub requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CancelStreamJobRequest {
    pub protocol_version: String,
    pub operation_kind: ThroughputBridgeOperationKind,
    pub bridge_request_id: String,
    pub handle_id: String,
    pub workflow_event_id: Uuid,
    pub workflow_owner_epoch: Option<u64>,
    pub reason: Option<String>,
    pub requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AwaitStreamJobTerminalRequest {
    pub protocol_version: String,
    pub operation_kind: ThroughputBridgeOperationKind,
    pub bridge_request_id: String,
    pub handle_id: String,
    pub workflow_event_id: Uuid,
    pub workflow_owner_epoch: Option<u64>,
    pub requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamJobCheckpointCallback {
    pub protocol_version: String,
    pub operation_kind: ThroughputBridgeOperationKind,
    pub bridge_request_id: String,
    pub handle_id: String,
    pub checkpoint_name: String,
    pub checkpoint_sequence: i64,
    pub callback_idempotency_key: String,
    pub stream_owner_epoch: Option<u64>,
    pub reached_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamJobTerminalCallback {
    pub protocol_version: String,
    pub operation_kind: ThroughputBridgeOperationKind,
    pub bridge_request_id: String,
    pub handle_id: String,
    pub status: StreamJobBridgeHandleStatus,
    pub callback_idempotency_key: String,
    pub stream_owner_epoch: Option<u64>,
    pub terminal_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamJobQueryCallback {
    pub protocol_version: String,
    pub operation_kind: ThroughputBridgeOperationKind,
    pub bridge_request_id: String,
    pub handle_id: String,
    pub query_id: String,
    pub query_name: String,
    pub status: StreamJobQueryStatus,
    pub callback_idempotency_key: String,
    pub stream_owner_epoch: Option<u64>,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThroughputBridgeState {
    pub protocol_version: String,
    pub operation_kind: ThroughputBridgeOperationKind,
    pub workflow_event_id: Option<Uuid>,
    pub bridge_request_id: String,
    pub submission_status: Option<ThroughputBridgeSubmissionStatus>,
    pub command_id: Option<Uuid>,
    pub command_partition_key: Option<String>,
    pub command_published_at: Option<DateTime<Utc>>,
    pub cancellation_requested_at: Option<DateTime<Utc>>,
    pub cancellation_reason: Option<String>,
    pub cancel_command_published_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub stream_status: Option<ThroughputRunStatus>,
    pub stream_terminal_at: Option<DateTime<Utc>>,
    pub workflow_status: Option<ThroughputBridgeTerminalStatus>,
    pub workflow_terminal_event_id: Option<Uuid>,
    pub workflow_owner_epoch: Option<u64>,
    pub workflow_accepted_at: Option<DateTime<Utc>>,
}

impl ThroughputBridgeState {
    pub fn has_workflow_acceptance(&self) -> bool {
        self.workflow_accepted_at.is_some()
    }

    pub fn protocol_version(&self) -> &str {
        if self.protocol_version.trim().is_empty() {
            THROUGHPUT_BRIDGE_PROTOCOL_VERSION
        } else {
            self.protocol_version.as_str()
        }
    }

    pub fn needs_start_publication(&self) -> bool {
        matches!(self.submission_status, Some(ThroughputBridgeSubmissionStatus::Admitted))
            && self.command_published_at.is_none()
            && self.cancelled_at.is_none()
    }

    pub fn needs_cancel_publication(&self) -> bool {
        matches!(
            self.submission_status,
            Some(ThroughputBridgeSubmissionStatus::CancellationRequested)
        ) && self.cancellation_requested_at.is_some()
            && self.cancel_command_published_at.is_none()
            && self.cancelled_at.is_none()
    }

    pub fn needs_workflow_acceptance_repair(&self) -> bool {
        self.stream_terminal_at.is_some() && self.workflow_accepted_at.is_none()
    }

    pub fn next_repair(&self) -> Option<ThroughputBridgeRepairKind> {
        if self.needs_start_publication() {
            Some(ThroughputBridgeRepairKind::PublishStart)
        } else if self.needs_cancel_publication() {
            Some(ThroughputBridgeRepairKind::PublishCancel)
        } else if self.needs_workflow_acceptance_repair() {
            Some(ThroughputBridgeRepairKind::AcceptTerminal)
        } else {
            None
        }
    }

    pub fn accepts_submission_transition(&self, next: ThroughputBridgeSubmissionStatus) -> bool {
        self.submission_status.is_none_or(|current| current.can_transition_to(next))
    }
}

pub fn throughput_bridge_request_matches(
    actual: &str,
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    batch_id: &str,
) -> bool {
    actual.is_empty()
        || actual == throughput_bridge_request_id(tenant_id, instance_id, run_id, batch_id)
}

pub fn throughput_cancel_dedupe_key(workflow_event_id: Uuid) -> String {
    format!("throughput-cancel:{workflow_event_id}")
}

pub fn throughput_cancel_command_id(workflow_event_id: Uuid, batch_id: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("throughput-cancel:{workflow_event_id}:{batch_id}").as_bytes(),
    )
}

pub fn throughput_cancel_command_envelope(
    workflow_event_id: Uuid,
    identity: ThroughputBatchIdentity,
    reason: impl Into<String>,
    occurred_at: DateTime<Utc>,
) -> ThroughputCommandEnvelope {
    ThroughputCommandEnvelope {
        command_id: throughput_cancel_command_id(workflow_event_id, &identity.batch_id),
        occurred_at,
        dedupe_key: throughput_cancel_dedupe_key(workflow_event_id),
        partition_key: throughput_partition_key(&identity.batch_id, 0),
        payload: ThroughputCommand::CancelBatch { identity, reason: reason.into() },
    }
}

pub fn stream_job_bridge_request_id(
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    job_id: &str,
) -> String {
    format!("stream-job-bridge:{tenant_id}:{instance_id}:{run_id}:{job_id}")
}

pub fn stream_job_handle_id(
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    job_id: &str,
) -> String {
    format!("stream-job-handle:{tenant_id}:{instance_id}:{run_id}:{job_id}")
}

pub fn stream_job_checkpoint_await_request_id(
    handle_id: &str,
    checkpoint_name: &str,
    workflow_event_id: Uuid,
) -> String {
    format!("stream-job-await:{handle_id}:{checkpoint_name}:{workflow_event_id}")
}

pub fn stream_job_query_request_id(
    handle_id: &str,
    query_name: &str,
    workflow_event_id: Uuid,
) -> String {
    format!("stream-job-query:{handle_id}:{query_name}:{workflow_event_id}")
}

pub fn stream_job_checkpoint_callback_dedupe_key(
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    job_id: &str,
    checkpoint_name: &str,
    checkpoint_sequence: i64,
) -> String {
    format!(
        "stream-job-checkpoint:{tenant_id}:{instance_id}:{run_id}:{job_id}:{checkpoint_name}:{checkpoint_sequence}"
    )
}

pub fn stream_job_terminal_callback_dedupe_key(
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    job_id: &str,
    status: StreamJobBridgeHandleStatus,
) -> String {
    format!("stream-job-terminal:{tenant_id}:{instance_id}:{run_id}:{job_id}:{}", status.as_str())
}

pub fn stream_job_query_callback_dedupe_key(
    tenant_id: &str,
    instance_id: &str,
    run_id: &str,
    job_id: &str,
    query_id: &str,
    status: StreamJobQueryStatus,
) -> String {
    format!(
        "stream-job-query:{tenant_id}:{instance_id}:{run_id}:{job_id}:{query_id}:{}",
        status.as_str()
    )
}

pub fn stream_job_callback_event_id(dedupe_key: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, dedupe_key.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bridge_state_repairs_are_classified_from_shared_statuses() {
        let now = Utc::now();
        let admitted = ThroughputBridgeState {
            protocol_version: THROUGHPUT_BRIDGE_PROTOCOL_VERSION.to_owned(),
            operation_kind: ThroughputBridgeOperationKind::BulkRun,
            workflow_event_id: Some(Uuid::now_v7()),
            bridge_request_id: "throughput-bridge:tenant:instance:run:batch".to_owned(),
            submission_status: Some(ThroughputBridgeSubmissionStatus::Admitted),
            command_id: Some(Uuid::now_v7()),
            command_partition_key: Some("batch:0".to_owned()),
            command_published_at: None,
            cancellation_requested_at: None,
            cancellation_reason: None,
            cancel_command_published_at: None,
            cancelled_at: None,
            stream_status: Some(ThroughputRunStatus::Scheduled),
            stream_terminal_at: None,
            workflow_status: None,
            workflow_terminal_event_id: None,
            workflow_owner_epoch: None,
            workflow_accepted_at: None,
        };
        assert!(admitted.needs_start_publication());
        assert!(!admitted.needs_cancel_publication());
        assert!(!admitted.needs_workflow_acceptance_repair());
        assert_eq!(admitted.next_repair(), Some(ThroughputBridgeRepairKind::PublishStart));
        assert!(
            admitted.accepts_submission_transition(ThroughputBridgeSubmissionStatus::Published)
        );
        assert!(
            !admitted.accepts_submission_transition(ThroughputBridgeSubmissionStatus::Cancelled)
        );

        let cancel_requested = ThroughputBridgeState {
            submission_status: Some(ThroughputBridgeSubmissionStatus::CancellationRequested),
            command_published_at: Some(now),
            cancellation_requested_at: Some(now),
            cancel_command_published_at: None,
            cancelled_at: None,
            ..admitted.clone()
        };
        assert!(!cancel_requested.needs_start_publication());
        assert!(cancel_requested.needs_cancel_publication());
        assert_eq!(cancel_requested.next_repair(), Some(ThroughputBridgeRepairKind::PublishCancel));

        let terminal_pending = ThroughputBridgeState {
            stream_status: Some(ThroughputRunStatus::Completed),
            stream_terminal_at: Some(now),
            workflow_status: Some(ThroughputBridgeTerminalStatus::Completed),
            workflow_terminal_event_id: Some(Uuid::now_v7()),
            workflow_accepted_at: None,
            cancel_command_published_at: Some(now),
            cancelled_at: Some(now),
            ..cancel_requested
        };
        assert!(terminal_pending.needs_workflow_acceptance_repair());
        assert!(!terminal_pending.has_workflow_acceptance());
        assert_eq!(
            terminal_pending.next_repair(),
            Some(ThroughputBridgeRepairKind::AcceptTerminal)
        );

        let cancelled = ThroughputBridgeState {
            submission_status: Some(ThroughputBridgeSubmissionStatus::Cancelled),
            cancelled_at: Some(now),
            ..terminal_pending
        };
        assert!(!cancelled.needs_cancel_publication());
    }

    #[test]
    fn cancel_command_helpers_are_deterministic() {
        let workflow_event_id =
            Uuid::parse_str("11111111-1111-1111-1111-111111111111").expect("valid uuid");
        let identity = ThroughputBatchIdentity {
            tenant_id: "tenant-a".to_owned(),
            instance_id: "instance-a".to_owned(),
            run_id: "run-a".to_owned(),
            batch_id: "batch-a".to_owned(),
        };
        let command = throughput_cancel_command_envelope(
            workflow_event_id,
            identity.clone(),
            "workflow cancelled",
            Utc::now(),
        );
        assert_eq!(command.command_id, throughput_cancel_command_id(workflow_event_id, "batch-a"));
        assert_eq!(command.dedupe_key, throughput_cancel_dedupe_key(workflow_event_id));
        assert_eq!(command.partition_key, throughput_partition_key("batch-a", 0));
        match command.payload {
            ThroughputCommand::CancelBatch { identity: actual, reason } => {
                assert_eq!(actual, identity);
                assert_eq!(reason, "workflow cancelled");
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn submission_status_transitions_are_closed() {
        assert!(
            ThroughputBridgeSubmissionStatus::Admitted
                .can_transition_to(ThroughputBridgeSubmissionStatus::Published)
        );
        assert!(
            ThroughputBridgeSubmissionStatus::Published
                .can_transition_to(ThroughputBridgeSubmissionStatus::CancellationRequested)
        );
        assert!(
            ThroughputBridgeSubmissionStatus::CancellationRequested
                .can_transition_to(ThroughputBridgeSubmissionStatus::Cancelled)
        );
        assert!(
            !ThroughputBridgeSubmissionStatus::Admitted
                .can_transition_to(ThroughputBridgeSubmissionStatus::Cancelled)
        );
        assert!(
            !ThroughputBridgeSubmissionStatus::Cancelled
                .can_transition_to(ThroughputBridgeSubmissionStatus::Published)
        );
    }

    #[test]
    fn stream_job_bridge_helpers_are_deterministic() {
        let workflow_event_id =
            Uuid::parse_str("22222222-2222-2222-2222-222222222222").expect("valid uuid");
        let request_id = stream_job_bridge_request_id("tenant-a", "instance-a", "run-a", "job-a");
        assert_eq!(request_id, "stream-job-bridge:tenant-a:instance-a:run-a:job-a");

        let handle_id = stream_job_handle_id("tenant-a", "instance-a", "run-a", "job-a");
        assert_eq!(handle_id, "stream-job-handle:tenant-a:instance-a:run-a:job-a");

        let await_id = stream_job_checkpoint_await_request_id(
            &handle_id,
            "hourly-rollup-ready",
            workflow_event_id,
        );
        assert_eq!(
            await_id,
            format!("stream-job-await:{handle_id}:hourly-rollup-ready:{workflow_event_id}")
        );
        assert_eq!(
            StreamJobBridgeHandleStatus::parse("completed"),
            Some(StreamJobBridgeHandleStatus::Completed)
        );
        assert_eq!(
            StreamJobCheckpointStatus::parse("accepted"),
            Some(StreamJobCheckpointStatus::Accepted)
        );
        assert_eq!(StreamJobQueryStatus::parse("completed"), Some(StreamJobQueryStatus::Completed));
        assert_eq!(
            StreamJobQueryConsistency::parse("strong"),
            Some(StreamJobQueryConsistency::Strong)
        );
        let query_id = stream_job_query_request_id(&handle_id, "currentStats", workflow_event_id);
        assert_eq!(
            query_id,
            format!("stream-job-query:{handle_id}:currentStats:{workflow_event_id}")
        );

        let checkpoint_dedupe = stream_job_checkpoint_callback_dedupe_key(
            "tenant-a",
            "instance-a",
            "run-a",
            "job-a",
            "hourly-rollup-ready",
            7,
        );
        assert_eq!(
            checkpoint_dedupe,
            "stream-job-checkpoint:tenant-a:instance-a:run-a:job-a:hourly-rollup-ready:7"
        );
        let terminal_dedupe = stream_job_terminal_callback_dedupe_key(
            "tenant-a",
            "instance-a",
            "run-a",
            "job-a",
            StreamJobBridgeHandleStatus::Completed,
        );
        assert_eq!(
            terminal_dedupe,
            "stream-job-terminal:tenant-a:instance-a:run-a:job-a:completed"
        );
        let query_dedupe = stream_job_query_callback_dedupe_key(
            "tenant-a",
            "instance-a",
            "run-a",
            "job-a",
            &query_id,
            StreamJobQueryStatus::Completed,
        );
        assert_eq!(
            query_dedupe,
            format!("stream-job-query:tenant-a:instance-a:run-a:job-a:{query_id}:completed")
        );
        assert_eq!(
            stream_job_callback_event_id(&terminal_dedupe),
            stream_job_callback_event_id(&terminal_dedupe)
        );
        assert_eq!(
            stream_job_callback_event_id(&query_dedupe),
            stream_job_callback_event_id(&query_dedupe)
        );
    }
}
