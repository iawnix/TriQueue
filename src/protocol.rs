use crate::model::{NodeHeartbeat, NodeRegistration, ObservedUnit, TaskId, TaskResult, TaskSpec};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Request {
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
    GetExternal {
        external_id: String,
    },
    Cancel {
        id: TaskId,
    },
    Nodes,
    Clean,
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
    Ok,
    Task(crate::model::Task),
    Submitted {
        task: crate::model::Task,
        created: bool,
    },
    Tasks(Vec<crate::model::Task>),
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
