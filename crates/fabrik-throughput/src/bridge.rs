use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    ThroughputBatchIdentity, ThroughputCommand, ThroughputCommandEnvelope,
    throughput_bridge_request_id, throughput_partition_key,
};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThroughputBridgeState {
    pub bridge_request_id: String,
    pub submission_status: Option<ThroughputBridgeSubmissionStatus>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bridge_state_repairs_are_classified_from_shared_statuses() {
        let now = Utc::now();
        let admitted = ThroughputBridgeState {
            bridge_request_id: "throughput-bridge:tenant:instance:run:batch".to_owned(),
            submission_status: Some(ThroughputBridgeSubmissionStatus::Admitted),
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

        let terminal_pending = ThroughputBridgeState {
            stream_status: Some(ThroughputRunStatus::Completed),
            stream_terminal_at: Some(now),
            workflow_status: Some(ThroughputBridgeTerminalStatus::Completed),
            workflow_terminal_event_id: Some(Uuid::now_v7()),
            workflow_accepted_at: None,
            ..cancel_requested
        };
        assert!(terminal_pending.needs_workflow_acceptance_repair());
        assert!(!terminal_pending.has_workflow_acceptance());

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
}
