use serde::{Deserialize, Serialize};

pub type TaskId = i64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TaskState {
    Wait,
    Run,
    Done,
    Fail,
    Cancel,
    Lost,
    Blocked,
    #[serde(rename = "timed_out")]
    TimedOut,
}

impl TaskState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Wait => "wait",
            Self::Run => "run",
            Self::Done => "done",
            Self::Fail => "fail",
            Self::Cancel => "cancel",
            Self::Lost => "lost",
            Self::Blocked => "blocked",
            Self::TimedOut => "timed_out",
        }
    }
}

impl std::fmt::Display for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSpec {
    pub name: String,
    pub command: Vec<String>,
    #[serde(default)]
    pub external_id: Option<String>,
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
    pub requested_cpu: u32,
    pub requested_mem_bytes: u64,
    #[serde(default)]
    pub requested_gpus: u32,
    #[serde(default)]
    pub scratch_cwd: bool,
    pub requested_node: Option<String>,
    pub cwd: Option<String>,
    pub env: Vec<(String, String)>,
    #[serde(default)]
    pub output_path: Option<String>,
    #[serde(default)]
    pub error_path: Option<String>,
    #[serde(default)]
    pub depends_on: Vec<TaskId>,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
}

fn default_max_attempts() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub spec: TaskSpec,
    pub state: TaskState,
    pub node: Option<String>,
    pub assigned_cpus: Option<String>,
    #[serde(default)]
    pub assigned_gpus: Option<String>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub exit_code: Option<i32>,
    pub stdout_path: String,
    pub stderr_path: String,
    pub error: Option<String>,
    #[serde(default)]
    pub external_id: Option<String>,
    #[serde(default)]
    pub attempt: u32,
    #[serde(default)]
    pub lease_id: Option<String>,
    #[serde(default)]
    pub lease_expires_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub name: String,
    pub address: String,
    pub cpu_total: u32,
    pub memory_bytes: u64,
    #[serde(default)]
    pub gpu_total: u32,
    pub scratch: String,
    pub online: bool,
    pub last_seen: String,
    pub cpu_used: u32,
    pub memory_used: u64,
    #[serde(default)]
    pub gpu_used: u32,
    pub running_tasks: Vec<TaskId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunningTask {
    pub id: TaskId,
    pub cpu_count: u32,
    pub memory_bytes: u64,
    #[serde(default)]
    pub gpu_count: u32,
    #[serde(default)]
    pub lease_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegistration {
    pub name: String,
    pub address: String,
    pub cpu_total: u32,
    pub memory_bytes: u64,
    #[serde(default)]
    pub gpu_total: u32,
    pub scratch: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHeartbeat {
    pub name: String,
    pub cpu_used: u32,
    pub memory_used: u64,
    #[serde(default)]
    pub gpu_used: u32,
    pub running_tasks: Vec<RunningTask>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAssignment {
    pub task: Task,
    pub cpus: String,
    pub gpus: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    pub id: TaskId,
    pub state: TaskState,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
    #[serde(default)]
    pub attempt: u32,
    #[serde(default)]
    pub lease_id: Option<String>,
}
