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
    EffectRequested {
        effect_id: String,
        connector: String,
        attempt: u32,
        input: Value,
    },
    EffectCompleted {
        effect_id: String,
        attempt: u32,
        output: Value,
    },
    EffectFailed {
        effect_id: String,
        attempt: u32,
        error: String,
    },
    EffectTimedOut {
        effect_id: String,
        attempt: u32,
    },
    EffectCancelled {
        effect_id: String,
        attempt: u32,
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<Value>,
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
    StepScheduled {
        step_id: String,
        attempt: u32,
        input: Value,
    },
    StepCompleted {
        step_id: String,
        attempt: u32,
        output: Value,
    },
    StepFailed {
        step_id: String,
        attempt: u32,
        error: String,
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
}

impl WorkflowEvent {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::WorkflowTriggered { .. } => "WorkflowTriggered",
            Self::WorkflowStarted => "WorkflowStarted",
            Self::WorkflowArtifactPinned => "WorkflowArtifactPinned",
            Self::MarkerRecorded { .. } => "MarkerRecorded",
            Self::EffectRequested { .. } => "EffectRequested",
            Self::EffectCompleted { .. } => "EffectCompleted",
            Self::EffectFailed { .. } => "EffectFailed",
            Self::EffectTimedOut { .. } => "EffectTimedOut",
            Self::EffectCancelled { .. } => "EffectCancelled",
            Self::WorkflowContinuedAsNew { .. } => "WorkflowContinuedAsNew",
            Self::SignalQueued { .. } => "SignalQueued",
            Self::SignalReceived { .. } => "SignalReceived",
            Self::StepScheduled { .. } => "StepScheduled",
            Self::StepCompleted { .. } => "StepCompleted",
            Self::StepFailed { .. } => "StepFailed",
            Self::TimerScheduled { .. } => "TimerScheduled",
            Self::TimerFired { .. } => "TimerFired",
            Self::WorkflowCompleted { .. } => "WorkflowCompleted",
            Self::WorkflowFailed { .. } => "WorkflowFailed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{EventEnvelope, WorkflowEvent, WorkflowIdentity};

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
}
