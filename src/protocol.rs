use crate::model::{NodeHeartbeat, NodeRegistration, ObservedUnit, TaskId, TaskResult, TaskSpec};
use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u32 = 2;

pub fn capabilities() -> Vec<String> {
    [
        "submission_idempotency",
        "external_id",
        "timeouts",
        "task_dependencies",
        "task_reconciliation",
        "cgroup_limits",
        "node_capabilities",
        "retention_clean",
        "cancel_confirmation",
        "node_quarantine",
        "attempt_history",
        "fragmented_cpu_allocation",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Request {
    Hello {
        protocol_version: u32,
        #[serde(default)]
        auth_token: Option<String>,
    },
    Submit {
        #[serde(default)]
        request_id: Option<String>,
        spec: TaskSpec,
    },
    List {
        state: Option<String>,
    },
    Get {
        id: TaskId,
    },
    History {
        id: TaskId,
    },
    ConfirmStopped {
        id: TaskId,
        attempt: u32,
        reason: String,
        retry: bool,
    },
    GetExternal {
        external_id: String,
    },
    Cancel {
        id: TaskId,
    },
    Nodes,
    Clean {
        #[serde(default)]
        older_than_seconds: Option<u64>,
    },
    Register {
        node: NodeRegistration,
    },
    Reconcile {
        node: String,
        units: Vec<ObservedUnit>,
    },
    Heartbeat {
        heartbeat: NodeHeartbeat,
    },
    Poll {
        node: String,
    },
    Complete {
        result: TaskResult,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum Response {
    Hello {
        protocol_version: u32,
        capabilities: Vec<String>,
    },
    Ok,
    Task(crate::model::Task),
    Submitted {
        task: crate::model::Task,
        created: bool,
    },
    Tasks(Vec<crate::model::Task>),
    History(crate::model::TaskHistory),
    Nodes(Vec<crate::model::NodeInfo>),
    Assignment(Option<crate::model::TaskAssignment>),
    Heartbeat {
        cancel: Vec<TaskId>,
    },
    Reconciled {
        adopt: Vec<crate::model::Task>,
        stop: Vec<String>,
    },
    Message(String),
    Cleaned {
        removed: usize,
    },
    Error(String),
}
