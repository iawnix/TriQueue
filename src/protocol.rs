use crate::model::{NodeHeartbeat, NodeRegistration, TaskId, TaskResult, TaskSpec};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Request {
    Submit { spec: TaskSpec },
    List { state: Option<String> },
    Get { id: TaskId },
    Cancel { id: TaskId },
    Nodes,
    Clean,
    Register { node: NodeRegistration },
    Heartbeat { heartbeat: NodeHeartbeat },
    Poll { node: String },
    Complete { result: TaskResult },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum Response {
    Ok,
    Task(crate::model::Task),
    Tasks(Vec<crate::model::Task>),
    Nodes(Vec<crate::model::NodeInfo>),
    Assignment(Option<crate::model::TaskAssignment>),
    Heartbeat { cancel: Vec<TaskId> },
    Message(String),
    Error(String),
}
