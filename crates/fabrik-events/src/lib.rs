use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub const EVENT_SCHEMA_VERSION: u32 = 1;

pub fn workflow_partition_key(tenant_id: &str, instance_id: &str) -> String {
    format!("{tenant_id}:{instance_id}")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowIdentity {
    pub tenant_id: String,
    pub definition_id: String,
    pub definition_version: u32,
    pub artifact_hash: String,
    pub instance_id: String,
    pub run_id: String,
    pub partition_key: String,
    pub producer: String,
}

impl WorkflowIdentity {
    pub fn new(
        tenant_id: impl Into<String>,
        definition_id: impl Into<String>,
        definition_version: u32,
        artifact_hash: impl Into<String>,
        instance_id: impl Into<String>,
        run_id: impl Into<String>,
        producer: impl Into<String>,
    ) -> Self {
        let tenant_id = tenant_id.into();
        let definition_id = definition_id.into();
        let artifact_hash = artifact_hash.into();
        let instance_id = instance_id.into();
        let run_id = run_id.into();
        let partition_key = workflow_partition_key(&tenant_id, &instance_id);

        Self {
            tenant_id,
            definition_id,
            definition_version,
            artifact_hash,
            instance_id,
            run_id,
            partition_key,
            producer: producer.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventEnvelope<T> {
    pub schema_version: u32,
    pub event_id: Uuid,
    pub event_type: String,
    pub occurred_at: DateTime<Utc>,
    pub tenant_id: String,
    pub definition_id: String,
    pub definition_version: u32,
    pub artifact_hash: String,
    pub instance_id: String,
    pub run_id: String,
    pub partition_key: String,
    pub producer: String,
    pub causation_id: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
    pub dedupe_key: Option<String>,
    pub metadata: BTreeMap<String, String>,
    pub payload: T,
}

impl<T> EventEnvelope<T> {
    pub fn new(event_type: impl Into<String>, identity: WorkflowIdentity, payload: T) -> Self {
        Self {
            schema_version: EVENT_SCHEMA_VERSION,
            event_id: Uuid::now_v7(),
            event_type: event_type.into(),
            occurred_at: Utc::now(),
            tenant_id: identity.tenant_id,
            definition_id: identity.definition_id,
            definition_version: identity.definition_version,
            artifact_hash: identity.artifact_hash,
            instance_id: identity.instance_id,
            run_id: identity.run_id,
            partition_key: identity.partition_key,
            producer: identity.producer,
            causation_id: None,
            correlation_id: None,
            dedupe_key: None,
            metadata: BTreeMap::new(),
            payload,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkflowEvent {
    WorkflowTriggered {
        input: Value,
    },
    WorkflowStarted,
    WorkflowArtifactPinned,
    MarkerRecorded {
        marker_id: String,
        value: Value,
    },
    VersionMarkerRecorded {
        change_id: String,
        version: u32,
    },
    ActivityTaskScheduled {
        activity_id: String,
        activity_type: String,
        task_queue: String,
        attempt: u32,
        input: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        config: Option<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        state: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        schedule_to_start_timeout_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start_to_close_timeout_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        heartbeat_timeout_ms: Option<u64>,
    },
    ActivityTaskStarted {
        activity_id: String,
        attempt: u32,
        worker_id: String,
        worker_build_id: String,
    },
    ActivityTaskHeartbeatRecorded {
        activity_id: String,
        attempt: u32,
        worker_id: String,
        worker_build_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<Value>,
    },
    ActivityTaskCancellationRequested {
        activity_id: String,
        attempt: u32,
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<Value>,
    },
    ActivityTaskCompleted {
        activity_id: String,
        attempt: u32,
        output: Value,
        worker_id: String,
        worker_build_id: String,
    },
    ActivityTaskFailed {
        activity_id: String,
        attempt: u32,
        error: String,
        worker_id: String,
        worker_build_id: String,
    },
    ActivityTaskTimedOut {
        activity_id: String,
        attempt: u32,
        worker_id: String,
        worker_build_id: String,
    },
    ActivityTaskCancelled {
        activity_id: String,
        attempt: u32,
        reason: String,
        worker_id: String,
        worker_build_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<Value>,
    },
    BulkActivityBatchScheduled {
        batch_id: String,
        activity_type: String,
        task_queue: String,
        items: Vec<Value>,
        input_handle: Value,
        result_handle: Value,
        chunk_size: u32,
        max_attempts: u32,
        retry_delay_ms: u64,
        aggregation_group_count: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        execution_policy: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reducer: Option<String>,
        throughput_backend: String,
        throughput_backend_version: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        state: Option<String>,
    },
    BulkActivityBatchCompleted {
        batch_id: String,
        total_items: u32,
        succeeded_items: u32,
        failed_items: u32,
        cancelled_items: u32,
        chunk_count: u32,
    },
    BulkActivityBatchFailed {
        batch_id: String,
        total_items: u32,
        succeeded_items: u32,
        failed_items: u32,
        cancelled_items: u32,
        chunk_count: u32,
        message: String,
    },
    BulkActivityBatchCancelled {
        batch_id: String,
        total_items: u32,
        succeeded_items: u32,
        failed_items: u32,
        cancelled_items: u32,
        chunk_count: u32,
        message: String,
    },
    WorkflowContinuedAsNew {
        new_run_id: String,
        input: Value,
    },
    SignalQueued {
        signal_id: String,
        signal_type: String,
        payload: Value,
    },
    SignalReceived {
        signal_id: String,
        signal_type: String,
        payload: Value,
    },
    WorkflowUpdateRequested {
        update_id: String,
        update_name: String,
        payload: Value,
    },
    WorkflowUpdateAccepted {
        update_id: String,
        update_name: String,
        payload: Value,
    },
    WorkflowUpdateCompleted {
        update_id: String,
        update_name: String,
        output: Value,
    },
    WorkflowUpdateRejected {
        update_id: String,
        update_name: String,
        error: String,
    },
    ChildWorkflowStartRequested {
        child_id: String,
        child_workflow_id: String,
        child_definition_id: String,
        input: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_queue: Option<String>,
        parent_close_policy: String,
    },
    ChildWorkflowStarted {
        child_id: String,
        child_workflow_id: String,
        child_run_id: String,
    },
    ChildWorkflowSignalRequested {
        child_id: String,
        signal_name: String,
        payload: Value,
    },
    ChildWorkflowCancellationRequested {
        child_id: String,
        reason: String,
    },
    ExternalWorkflowSignalRequested {
        target_instance_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target_run_id: Option<String>,
        signal_name: String,
        payload: Value,
    },
    ExternalWorkflowCancellationRequested {
        target_instance_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target_run_id: Option<String>,
        reason: String,
    },
    ChildWorkflowCompleted {
        child_id: String,
        child_run_id: String,
        output: Value,
    },
    ChildWorkflowFailed {
        child_id: String,
        child_run_id: String,
        error: String,
    },
    ChildWorkflowCancelled {
        child_id: String,
        child_run_id: String,
        reason: String,
    },
    ChildWorkflowTerminated {
        child_id: String,
        child_run_id: String,
        reason: String,
    },
    TimerScheduled {
        timer_id: String,
        fire_at: DateTime<Utc>,
    },
    TimerFired {
        timer_id: String,
    },
    WorkflowCompleted {
        output: Value,
    },
    WorkflowFailed {
        reason: String,
    },
    WorkflowCancellationRequested {
        reason: String,
    },
    WorkflowCancelled {
        reason: String,
    },
    WorkflowTerminated {
        reason: String,
    },
}

impl WorkflowEvent {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::WorkflowTriggered { .. } => "WorkflowTriggered",
            Self::WorkflowStarted => "WorkflowStarted",
            Self::WorkflowArtifactPinned => "WorkflowArtifactPinned",
            Self::MarkerRecorded { .. } => "MarkerRecorded",
            Self::VersionMarkerRecorded { .. } => "VersionMarkerRecorded",
            Self::ActivityTaskScheduled { .. } => "ActivityTaskScheduled",
            Self::ActivityTaskStarted { .. } => "ActivityTaskStarted",
            Self::ActivityTaskHeartbeatRecorded { .. } => "ActivityTaskHeartbeatRecorded",
            Self::ActivityTaskCancellationRequested { .. } => "ActivityTaskCancellationRequested",
            Self::ActivityTaskCompleted { .. } => "ActivityTaskCompleted",
            Self::ActivityTaskFailed { .. } => "ActivityTaskFailed",
            Self::ActivityTaskTimedOut { .. } => "ActivityTaskTimedOut",
            Self::ActivityTaskCancelled { .. } => "ActivityTaskCancelled",
            Self::BulkActivityBatchScheduled { .. } => "BulkActivityBatchScheduled",
            Self::BulkActivityBatchCompleted { .. } => "BulkActivityBatchCompleted",
            Self::BulkActivityBatchFailed { .. } => "BulkActivityBatchFailed",
            Self::BulkActivityBatchCancelled { .. } => "BulkActivityBatchCancelled",
            Self::WorkflowContinuedAsNew { .. } => "WorkflowContinuedAsNew",
            Self::SignalQueued { .. } => "SignalQueued",
            Self::SignalReceived { .. } => "SignalReceived",
            Self::WorkflowUpdateRequested { .. } => "WorkflowUpdateRequested",
            Self::WorkflowUpdateAccepted { .. } => "WorkflowUpdateAccepted",
            Self::WorkflowUpdateCompleted { .. } => "WorkflowUpdateCompleted",
            Self::WorkflowUpdateRejected { .. } => "WorkflowUpdateRejected",
            Self::ChildWorkflowStartRequested { .. } => "ChildWorkflowStartRequested",
            Self::ChildWorkflowStarted { .. } => "ChildWorkflowStarted",
            Self::ChildWorkflowSignalRequested { .. } => "ChildWorkflowSignalRequested",
            Self::ChildWorkflowCancellationRequested { .. } => "ChildWorkflowCancellationRequested",
            Self::ExternalWorkflowSignalRequested { .. } => "ExternalWorkflowSignalRequested",
            Self::ExternalWorkflowCancellationRequested { .. } => {
                "ExternalWorkflowCancellationRequested"
            }
            Self::ChildWorkflowCompleted { .. } => "ChildWorkflowCompleted",
            Self::ChildWorkflowFailed { .. } => "ChildWorkflowFailed",
            Self::ChildWorkflowCancelled { .. } => "ChildWorkflowCancelled",
            Self::ChildWorkflowTerminated { .. } => "ChildWorkflowTerminated",
            Self::TimerScheduled { .. } => "TimerScheduled",
            Self::TimerFired { .. } => "TimerFired",
            Self::WorkflowCompleted { .. } => "WorkflowCompleted",
            Self::WorkflowFailed { .. } => "WorkflowFailed",
            Self::WorkflowCancellationRequested { .. } => "WorkflowCancellationRequested",
            Self::WorkflowCancelled { .. } => "WorkflowCancelled",
            Self::WorkflowTerminated { .. } => "WorkflowTerminated",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowTurnRouting {
    MatchingPoller,
    LocalExecutor,
}

pub fn workflow_turn_routing(payload: &WorkflowEvent) -> WorkflowTurnRouting {
    match payload {
        WorkflowEvent::WorkflowTriggered { .. }
        | WorkflowEvent::SignalQueued { .. }
        | WorkflowEvent::WorkflowUpdateRequested { .. }
        | WorkflowEvent::WorkflowCancellationRequested { .. }
        | WorkflowEvent::TimerFired { .. }
        | WorkflowEvent::ActivityTaskCompleted { .. }
        | WorkflowEvent::ActivityTaskFailed { .. }
        | WorkflowEvent::ActivityTaskTimedOut { .. }
        | WorkflowEvent::ActivityTaskCancelled { .. }
        | WorkflowEvent::BulkActivityBatchCompleted { .. }
        | WorkflowEvent::BulkActivityBatchFailed { .. }
        | WorkflowEvent::BulkActivityBatchCancelled { .. }
        | WorkflowEvent::ChildWorkflowSignalRequested { .. }
        | WorkflowEvent::ChildWorkflowCancellationRequested { .. }
        | WorkflowEvent::ExternalWorkflowSignalRequested { .. }
        | WorkflowEvent::ExternalWorkflowCancellationRequested { .. }
        | WorkflowEvent::ChildWorkflowCompleted { .. }
        | WorkflowEvent::ChildWorkflowFailed { .. }
        | WorkflowEvent::ChildWorkflowCancelled { .. }
        | WorkflowEvent::ChildWorkflowTerminated { .. } => WorkflowTurnRouting::MatchingPoller,
        _ => WorkflowTurnRouting::LocalExecutor,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EventEnvelope, WorkflowEvent, WorkflowIdentity, WorkflowTurnRouting, workflow_turn_routing,
    };
    use serde_json::json;

    #[test]
    fn serializes_workflow_events_inside_envelope() {
        let payload = WorkflowEvent::WorkflowStarted;
        let envelope = EventEnvelope::new(
            payload.event_type(),
            WorkflowIdentity::new(
                "tenant-a",
                "demo",
                1,
                "artifact-1",
                "instance-1",
                "run-1",
                "test",
            ),
            payload,
        );

        let json = serde_json::to_value(&envelope).unwrap();
        assert_eq!(json["event_type"], "WorkflowStarted");
        assert_eq!(json["payload"]["kind"], "workflow_started");
    }

    #[test]
    fn classifies_workflow_turn_routing_policy() {
        assert_eq!(
            workflow_turn_routing(&WorkflowEvent::WorkflowTriggered { input: json!({"ok": true}) }),
            WorkflowTurnRouting::MatchingPoller
        );
        assert_eq!(
            workflow_turn_routing(&WorkflowEvent::ActivityTaskCompleted {
                activity_id: "a1".to_owned(),
                attempt: 1,
                output: json!(true),
                worker_id: "worker".to_owned(),
                worker_build_id: "build".to_owned(),
            }),
            WorkflowTurnRouting::MatchingPoller
        );
        assert_eq!(
            workflow_turn_routing(&WorkflowEvent::WorkflowStarted),
            WorkflowTurnRouting::LocalExecutor
        );
        assert_eq!(
            workflow_turn_routing(&WorkflowEvent::ActivityTaskScheduled {
                activity_id: "a1".to_owned(),
                activity_type: "demo".to_owned(),
                task_queue: "default".to_owned(),
                attempt: 1,
                input: json!(null),
                config: None,
                state: None,
                schedule_to_start_timeout_ms: None,
                start_to_close_timeout_ms: None,
                heartbeat_timeout_ms: None,
            }),
            WorkflowTurnRouting::LocalExecutor
        );
    }
}
