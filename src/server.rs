use crate::model::{NodeHeartbeat, NodeInfo, NodeRegistration, Task, TaskId, TaskState};
use crate::protocol::{PROTOCOL_VERSION, Request, Response, capabilities};
use crate::store::{DependencyStatus, LEASE_SECONDS, Store};
use crate::{socket_path, tcp_addr};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixListener};

#[derive(Clone)]
pub struct Server {
    pub store: Arc<Store>,
    pub nodes: Arc<Mutex<HashMap<String, NodeInfo>>>,
}

impl Server {
    pub fn new(store: Store) -> Self {
        Self {
            store: Arc::new(store),
            nodes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn serve(self) -> Result<()> {
        let socket = socket_path();
        if let Some(parent) = socket.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let _ = tokio::fs::remove_file(&socket).await;
        let unix = UnixListener::bind(&socket)
            .with_context(|| format!("binding Unix socket {}", socket.display()))?;
        let tcp = TcpListener::bind(tcp_addr()).await?;
        tracing::info!(socket = %socket.display(), "tri-server ready");
        let auth_token = std::env::var("TRI_AUTH_TOKEN")
            .ok()
            .filter(|token| !token.trim().is_empty());
        if auth_token.is_none() {
            tracing::warn!(
                "TCP authentication is disabled; set TRI_AUTH_TOKEN for client authentication"
            );
        }

        let reaper = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
            loop {
                interval.tick().await;
                if let Err(error) = reaper.reap_expired() {
                    tracing::warn!(%error, "failed to reap expired task leases");
                }
            }
        });

        let unix_server = self.clone();
        tokio::spawn(async move {
            loop {
                match unix.accept().await {
                    Ok((stream, _)) => {
                        let server = unix_server.clone();
                        tokio::spawn(async move {
                            if let Err(error) = handle_stream(server, stream, false, None).await {
                                tracing::warn!(%error, "Unix request failed");
                            }
                        });
                    }
                    Err(error) => tracing::error!(%error, "Unix accept failed"),
                }
            }
        });

        loop {
            let (stream, peer) = tcp.accept().await?;
            let server = self.clone();
            let auth_token = auth_token.clone();
            tokio::spawn(async move {
                if let Err(error) = handle_stream(server, stream, true, auth_token).await {
                    tracing::warn!(%peer, %error, "TCP request failed");
                }
            });
        }
    }

    pub fn handle(&self, request: Request) -> Result<Response> {
        match request {
            Request::Hello {
                protocol_version, ..
            } => {
                anyhow::ensure!(
                    protocol_version == PROTOCOL_VERSION,
                    "protocol version mismatch: client={protocol_version}, server={PROTOCOL_VERSION}"
                );
                Ok(Response::Hello {
                    protocol_version: PROTOCOL_VERSION,
                    capabilities: capabilities(),
                })
            }
            Request::Submit { request_id, spec } => {
                let (task, created) = self
                    .store
                    .submit_with_receipt(request_id.as_deref(), spec)?;
                Ok(Response::Submitted { task, created })
            }
            Request::List { state } => Ok(Response::Tasks(self.store.list(state.as_deref())?)),
            Request::Get { id } => match self.store.get(id)? {
                Some(task) => Ok(Response::Task(task)),
                None => Ok(Response::Error(format!("task {id} does not exist"))),
            },
            Request::GetExternal { external_id } => match self.store.get_external(&external_id)? {
                Some(task) => Ok(Response::Task(task)),
                None => Ok(Response::Error(format!(
                    "external_id {external_id:?} does not exist"
                ))),
            },
            Request::Cancel { id } => {
                match self.store.cancel(id)? {
                    // A running process still owns its allocation until it reports
                    // completion.  The node will see the `cancel` state on its
                    // next heartbeat and terminate the process.
                    Some(task) => Ok(Response::Task(task)),
                    None => Ok(Response::Error(format!("task {id} does not exist"))),
                }
            }
            Request::Nodes => {
                let nodes = self.nodes.lock().expect("nodes mutex poisoned");
                let tasks = self.store.list(None)?;
                Ok(Response::Nodes(
                    nodes
                        .values()
                        .cloned()
                        .map(|mut node| {
                            let allocation = NodeAllocation::from_tasks(&tasks, &node.name);
                            node.cpu_used = allocation.cpus.len() as u32;
                            node.gpu_used = allocation.gpus.len() as u32;
                            node.memory_used = allocation.memory;
                            node.running_tasks = allocation.tasks;
                            node
                        })
                        .collect(),
                ))
            }
            Request::Clean { older_than_seconds } => Ok(Response::Cleaned {
                removed: self.store.clean(older_than_seconds)?,
            }),
            Request::Register { node } => {
                self.register(node);
                Ok(Response::Ok)
            }
            Request::Reconcile { node, units } => {
                let (adopt, stop) = self.reconcile(&node, &units)?;
                Ok(Response::Reconciled { adopt, stop })
            }
            Request::Heartbeat { heartbeat } => {
                let cancel = self.heartbeat(heartbeat)?;
                Ok(Response::Heartbeat { cancel })
            }
            Request::Poll { node } => Ok(Response::Assignment(self.poll(&node)?)),
            Request::Complete { result } => {
                self.complete(&result)?;
                Ok(Response::Ok)
            }
        }
    }

    fn register(&self, node: NodeRegistration) {
        let mut nodes = self.nodes.lock().expect("nodes mutex poisoned");
        if let Some(existing) = nodes.get_mut(&node.name) {
            existing.address = node.address;
            existing.cpu_total = node.cpu_total;
            existing.memory_bytes = node.memory_bytes;
            existing.gpu_total = node.gpu_total;
            existing.scratch = node.scratch;
            existing.capabilities = node.capabilities;
            existing.online = true;
            existing.last_seen = crate::now_string();
        } else {
            nodes.insert(
                node.name.clone(),
                NodeInfo {
                    name: node.name,
                    address: node.address,
                    cpu_total: node.cpu_total,
                    memory_bytes: node.memory_bytes,
                    gpu_total: node.gpu_total,
                    scratch: node.scratch,
                    online: true,
                    last_seen: crate::now_string(),
                    cpu_used: 0,
                    memory_used: 0,
                    gpu_used: 0,
                    running_tasks: Vec::new(),
                    capabilities: node.capabilities,
                },
            );
        }
    }

    fn heartbeat(&self, heartbeat: NodeHeartbeat) -> Result<Vec<TaskId>> {
        let mut nodes = self.nodes.lock().expect("nodes mutex poisoned");
        if let Some(node) = nodes.get_mut(&heartbeat.name) {
            node.online = true;
            node.last_seen = crate::now_string();
        }
        let mut cancelled = Vec::new();
        for running in &heartbeat.running_tasks {
            if let Some(lease_id) = &running.lease_id {
                self.store.renew_lease(running.id, lease_id)?;
            }
            if let Some(task) = self.store.get(running.id)?
                && task.state == TaskState::Cancel
            {
                cancelled.push(task.id);
            }
        }
        Ok(cancelled)
    }

    fn reconcile(
        &self,
        node_name: &str,
        units: &[crate::model::ObservedUnit],
    ) -> Result<(Vec<crate::model::Task>, Vec<String>)> {
        let mut adopt = Vec::new();
        let mut stop = Vec::new();
        for unit in units {
            let Some(task) = self.store.get(unit.task_id)? else {
                stop.push(unit.unit.clone());
                continue;
            };
            let matches_current_attempt = task.node.as_deref() == Some(node_name)
                && task.attempt == unit.attempt
                && task.lease_id.is_some()
                && matches!(task.state, TaskState::Run | TaskState::Cancel);
            if matches_current_attempt {
                adopt.push(task);
            } else {
                stop.push(unit.unit.clone());
            }
        }
        Ok((adopt, stop))
    }

    fn poll(&self, node_name: &str) -> Result<Option<crate::model::TaskAssignment>> {
        let mut nodes = self.nodes.lock().expect("nodes mutex poisoned");
        let Some(node) = nodes.get_mut(node_name) else {
            return Ok(None);
        };
        if !node.online {
            return Ok(None);
        }
        let tasks = self.store.list(Some("wait"))?;
        let allocation = NodeAllocation::from_tasks(&self.store.list(None)?, node_name);
        let occupied_cpus = &allocation.cpus;
        let occupied_gpus = &allocation.gpus;
        let occupied_memory = allocation.memory;
        for task in tasks {
            if task
                .spec
                .requested_node
                .as_deref()
                .is_some_and(|name| name != node_name)
            {
                continue;
            }
            match self.store.dependency_status(&task)? {
                DependencyStatus::Ready => {}
                DependencyStatus::Waiting => continue,
                DependencyStatus::Blocked(reason) => {
                    self.store.block(task.id, &reason)?;
                    continue;
                }
            }
            let available_cpu = node.cpu_total.saturating_sub(occupied_cpus.len() as u32);
            let available_mem = node.memory_bytes.saturating_sub(occupied_memory);
            let available_gpus = node.gpu_total.saturating_sub(occupied_gpus.len() as u32);
            if task.spec.requested_cpu > available_cpu
                || task.spec.requested_mem_bytes > available_mem
                || task.spec.requested_gpus > available_gpus
            {
                continue;
            }
            let Some(cpus) =
                first_free_range(node.cpu_total, task.spec.requested_cpu, occupied_cpus)
            else {
                continue;
            };
            let Some(gpus) =
                first_free_range(node.gpu_total, task.spec.requested_gpus, occupied_gpus)
            else {
                continue;
            };
            if let Some(task) = self.store.claim(task.id, node_name, &cpus, &gpus)? {
                return Ok(Some(crate::model::TaskAssignment { task, cpus, gpus }));
            }
        }
        Ok(None)
    }

    fn complete(&self, result: &crate::model::TaskResult) -> Result<()> {
        self.store.complete(result)?;
        Ok(())
    }

    fn reap_expired(&self) -> Result<()> {
        self.store.reap_expired(crate::now_seconds())?;
        let now = crate::now_seconds();
        let mut nodes = self.nodes.lock().expect("nodes mutex poisoned");
        for node in nodes.values_mut() {
            let last_seen = node.last_seen.parse::<u64>().unwrap_or(0);
            node.online = now.saturating_sub(last_seen) <= LEASE_SECONDS;
        }
        Ok(())
    }
}

/// One reservation snapshot powers both admission and node display.
#[derive(Default)]
struct NodeAllocation {
    cpus: std::collections::BTreeSet<u32>,
    gpus: std::collections::BTreeSet<u32>,
    memory: u64,
    tasks: Vec<TaskId>,
}

impl NodeAllocation {
    fn from_tasks(tasks: &[Task], node_name: &str) -> Self {
        let mut result = Self::default();
        for task in tasks
            .iter()
            .filter(|task| task.node.as_deref() == Some(node_name) && task.reserves_resources())
        {
            result
                .cpus
                .extend(parse_resource_set(task.assigned_cpus.as_deref()));
            result
                .gpus
                .extend(parse_resource_set(task.assigned_gpus.as_deref()));
            result.memory = result.memory.saturating_add(task.spec.requested_mem_bytes);
            result.tasks.push(task.id);
        }
        result
    }
}

fn parse_resource_set(value: Option<&str>) -> std::collections::BTreeSet<u32> {
    let mut result = std::collections::BTreeSet::new();
    let Some(value) = value else { return result };
    for part in value.split(',') {
        let Some((start, end)) = part.split_once('-') else {
            if let Ok(index) = part.parse::<u32>() {
                result.insert(index);
            }
            continue;
        };
        let (Ok(start), Ok(end)) = (start.parse::<u32>(), end.parse::<u32>()) else {
            continue;
        };
        result.extend(start..=end);
    }
    result
}

fn first_free_range(
    total: u32,
    requested: u32,
    occupied: &std::collections::BTreeSet<u32>,
) -> Option<String> {
    if requested == 0 {
        return Some(String::new());
    }
    let last_start = total.checked_sub(requested)?;
    for start in 0..=last_start {
        let end = start + requested - 1;
        if (start..=end).all(|index| !occupied.contains(&index)) {
            return Some(format!("{start}-{end}"));
        }
    }
    None
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use crate::model::{NodeRegistration, TaskResult, TaskSpec};
    use crate::protocol::{Request, Response};
    use std::path::PathBuf;

    fn spec(name: &str, cpu: u32) -> TaskSpec {
        TaskSpec {
            name: name.into(),
            command: vec!["true".into()],
            external_id: None,
            timeout_seconds: None,
            requested_cpu: cpu,
            requested_mem_bytes: 0,
            requested_gpus: 0,
            scratch_cwd: false,
            requested_node: Some("fed1".into()),
            cwd: None,
            env: Vec::new(),
            output_path: None,
            error_path: None,
            depends_on: Vec::new(),
            max_attempts: 1,
        }
    }

    #[test]
    fn reuses_the_actual_free_cpu_range() {
        let root = std::env::temp_dir().join(format!("tri-server-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = Store::open(&root.join("tri.db"), &root.join("data")).unwrap();
        let server = Server::new(store);
        server
            .handle(Request::Register {
                node: NodeRegistration {
                    name: "fed1".into(),
                    address: "local".into(),
                    cpu_total: 4,
                    memory_bytes: 4 << 30,
                    gpu_total: 0,
                    scratch: PathBuf::from("/tmp").to_string_lossy().into_owned(),
                    gpu_devices: Vec::new(),
                    capabilities: Vec::new(),
                },
            })
            .unwrap();
        let ids = ["a", "b", "c"]
            .into_iter()
            .map(|name| {
                match server
                    .handle(Request::Submit {
                        request_id: None,
                        spec: spec(name, 2),
                    })
                    .unwrap()
                {
                    Response::Submitted { task, .. } => task.id,
                    response => panic!("unexpected response: {response:?}"),
                }
            })
            .collect::<Vec<_>>();
        let first = match server
            .handle(Request::Poll {
                node: "fed1".into(),
            })
            .unwrap()
        {
            Response::Assignment(Some(assignment)) => assignment,
            response => panic!("unexpected response: {response:?}"),
        };
        let second = match server
            .handle(Request::Poll {
                node: "fed1".into(),
            })
            .unwrap()
        {
            Response::Assignment(Some(assignment)) => assignment,
            response => panic!("unexpected response: {response:?}"),
        };
        assert_eq!(first.cpus, "0-1");
        assert_eq!(second.cpus, "2-3");
        server
            .handle(Request::Complete {
                result: TaskResult {
                    id: ids[0],
                    state: TaskState::Done,
                    exit_code: Some(0),
                    error: None,
                    attempt: 1,
                    lease_id: first.task.lease_id.clone(),
                },
            })
            .unwrap();
        let third = match server
            .handle(Request::Poll {
                node: "fed1".into(),
            })
            .unwrap()
        {
            Response::Assignment(Some(assignment)) => assignment,
            response => panic!("unexpected response: {response:?}"),
        };
        assert_eq!(third.cpus, "0-1");
        let _ = std::fs::remove_dir_all(root);
    }

    fn test_server(name: &str) -> (Server, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!("tri-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let server = Server::new(Store::open(&root.join("tri.db"), &root.join("data")).unwrap());
        server
            .handle(Request::Register {
                node: NodeRegistration {
                    name: "fed1".into(),
                    address: "local".into(),
                    cpu_total: 24,
                    memory_bytes: 31 << 30,
                    gpu_total: 2,
                    scratch: root.to_string_lossy().into(),
                    gpu_devices: vec![],
                    capabilities: vec![],
                },
            })
            .unwrap();
        (server, root)
    }

    fn submit_resources(server: &Server, name: &str, cpu: u32, memory: u64, gpus: u32) -> Task {
        let mut spec = spec(name, cpu);
        spec.requested_mem_bytes = memory;
        spec.requested_gpus = gpus;
        match server
            .handle(Request::Submit {
                request_id: None,
                spec,
            })
            .unwrap()
        {
            Response::Submitted { task, .. } => task,
            other => panic!("unexpected response: {other:?}"),
        }
    }

    fn poll_assignment(server: &Server) -> Option<crate::model::TaskAssignment> {
        match server
            .handle(Request::Poll {
                node: "fed1".into(),
            })
            .unwrap()
        {
            Response::Assignment(task) => task,
            other => panic!("unexpected response: {other:?}"),
        }
    }

    fn node_view(server: &Server) -> NodeInfo {
        match server.handle(Request::Nodes).unwrap() {
            Response::Nodes(mut nodes) => nodes.remove(0),
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[test]
    fn default_memory_is_reserved_before_scheduling_another_task() {
        let (server, root) = test_server("default-memory-reservation");
        server
            .nodes
            .lock()
            .unwrap()
            .get_mut("fed1")
            .unwrap()
            .memory_bytes = crate::resources::DEFAULT_MEMORY_BYTES;
        let first = submit_resources(&server, "first", 1, 0, 0);
        submit_resources(&server, "second", 1, 0, 0);
        assert_eq!(
            first.spec.requested_mem_bytes,
            crate::resources::DEFAULT_MEMORY_BYTES
        );
        assert_eq!(poll_assignment(&server).unwrap().task.id, first.id);
        assert_eq!(
            node_view(&server).memory_used,
            crate::resources::DEFAULT_MEMORY_BYTES
        );
        assert!(
            poll_assignment(&server).is_none(),
            "default memory must prevent overcommit"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn cancel_confirmation_releases_cpu_memory_and_gpu_on_next_poll() {
        let (server, root) = test_server("cancel-release");
        let first = submit_resources(&server, "cancel", 12, 13 << 30, 1);
        let claimed = poll_assignment(&server).unwrap();
        server.handle(Request::Cancel { id: first.id }).unwrap();
        // Even an empty heartbeat must not erase an unacknowledged reservation.
        server
            .handle(Request::Heartbeat {
                heartbeat: NodeHeartbeat {
                    name: "fed1".into(),
                    cpu_used: 0,
                    memory_used: 0,
                    gpu_used: 0,
                    running_tasks: vec![],
                },
            })
            .unwrap();
        let node = node_view(&server);
        assert_eq!(
            (node.cpu_used, node.memory_used, node.gpu_used),
            (12, 13 << 30, 1)
        );
        assert_eq!(node.running_tasks, vec![first.id]);
        submit_resources(&server, "next-a", 12, 13 << 30, 1);
        submit_resources(&server, "next-b", 12, 13 << 30, 1);
        let second = poll_assignment(&server).unwrap();
        assert_eq!(second.cpus, "12-23");
        assert!(poll_assignment(&server).is_none());
        let mut result = TaskResult {
            id: first.id,
            state: TaskState::Cancel,
            exit_code: None,
            error: Some("cancel confirmed by node".into()),
            attempt: claimed.task.attempt,
            lease_id: Some("stale-lease".into()),
        };
        server
            .handle(Request::Complete {
                result: result.clone(),
            })
            .unwrap();
        assert!(poll_assignment(&server).is_none());
        result.lease_id = claimed.task.lease_id;
        server
            .handle(Request::Complete {
                result: result.clone(),
            })
            .unwrap();
        let node = node_view(&server);
        assert_eq!(
            (node.cpu_used, node.memory_used, node.gpu_used),
            (12, 13 << 30, 1)
        );
        assert_eq!(node.running_tasks, vec![second.task.id]);
        let third = poll_assignment(&server).unwrap();
        assert_eq!(third.cpus, "0-11");
        assert_eq!(third.gpus, "0-0");
        // Duplicate completion and stale telemetry cannot subtract a new allocation.
        server.handle(Request::Complete { result }).unwrap();
        server
            .handle(Request::Heartbeat {
                heartbeat: NodeHeartbeat {
                    name: "fed1".into(),
                    cpu_used: 0,
                    memory_used: 0,
                    gpu_used: 0,
                    running_tasks: vec![],
                },
            })
            .unwrap();
        let node = node_view(&server);
        assert_eq!(
            (node.cpu_used, node.memory_used, node.gpu_used),
            (24, 26 << 30, 2)
        );
        assert_eq!(node.running_tasks, vec![second.task.id, third.task.id]);
        let finished = server.store.get(first.id).unwrap().unwrap();
        assert!(finished.cancel_confirmed);
        assert_eq!(
            finished.completion_reason.as_deref(),
            Some("node_completion")
        );
        assert!(!finished.reserves_resources());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn each_resource_blocks_admission_until_cancel_is_confirmed() {
        // Independently make CPU, memory, and GPU the limiting resource.
        for (index, (cpu, memory, gpu)) in [(24, 0, 0), (1, 31 << 30, 0), (1, 0, 2)]
            .into_iter()
            .enumerate()
        {
            let (server, root) = test_server(&format!("cancel-limits-{index}"));
            let first = submit_resources(&server, "cancel", cpu, memory, gpu);
            let claimed = poll_assignment(&server).unwrap();
            server.handle(Request::Cancel { id: first.id }).unwrap();
            submit_resources(&server, "next", cpu, memory, gpu);
            assert!(poll_assignment(&server).is_none());
            server
                .handle(Request::Complete {
                    result: TaskResult {
                        id: first.id,
                        state: TaskState::Cancel,
                        exit_code: None,
                        error: None,
                        attempt: claimed.task.attempt,
                        lease_id: claimed.task.lease_id,
                    },
                })
                .unwrap();
            assert!(poll_assignment(&server).is_some());
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn queued_cancel_and_all_execution_terminal_states_release_capacity() {
        let (server, root) = test_server("terminal-capacity");
        let queued = submit_resources(&server, "queued", 24, 31 << 30, 2);
        let Response::Task(cancelled) = server.handle(Request::Cancel { id: queued.id }).unwrap()
        else {
            panic!()
        };
        assert!(cancelled.cancel_confirmed);
        assert_eq!(
            cancelled.completion_reason.as_deref(),
            Some("cancelled_before_start")
        );
        assert_eq!(node_view(&server).cpu_used, 0);
        for state in [TaskState::Done, TaskState::Fail, TaskState::TimedOut] {
            submit_resources(&server, state.as_str(), 24, 31 << 30, 2);
            let claimed = poll_assignment(&server).unwrap();
            assert_eq!(node_view(&server).cpu_used, 24);
            let mut result = TaskResult {
                id: claimed.task.id,
                state: TaskState::Run,
                exit_code: None,
                error: None,
                attempt: claimed.task.attempt,
                lease_id: claimed.task.lease_id,
            };
            assert!(
                server
                    .handle(Request::Complete {
                        result: result.clone()
                    })
                    .is_err()
            );
            assert_eq!(node_view(&server).cpu_used, 24);
            result.state = state;
            server.handle(Request::Complete { result }).unwrap();
            let node = node_view(&server);
            assert_eq!((node.cpu_used, node.memory_used, node.gpu_used), (0, 0, 0));
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn historical_cancel_does_not_reserve_resources_after_restart() {
        let (server, root) = test_server("legacy-cancel");
        let first = submit_resources(&server, "legacy", 24, 31 << 30, 2);
        poll_assignment(&server).unwrap();
        // Simulate the old schema and a closed cancellation without stop evidence.
        drop(server);
        let conn = rusqlite::Connection::open(root.join("tri.db")).unwrap();
        conn.execute("UPDATE tasks SET state='cancel', finished_at='1', lease_id=NULL, lease_expires_at=NULL WHERE id=?1", [first.id]).unwrap();
        conn.execute_batch("ALTER TABLE tasks DROP COLUMN cancel_confirmed; ALTER TABLE tasks DROP COLUMN completion_reason;").unwrap();
        drop(conn);
        let server = Server::new(Store::open(&root.join("tri.db"), &root.join("data")).unwrap());
        server.register(NodeRegistration {
            name: "fed1".into(),
            address: "local".into(),
            cpu_total: 24,
            memory_bytes: 31 << 30,
            gpu_total: 2,
            scratch: root.to_string_lossy().into(),
            gpu_devices: vec![],
            capabilities: vec![],
        });
        let old = server.store.get(first.id).unwrap().unwrap();
        assert!(!old.cancel_confirmed);
        assert!(old.completion_reason.is_none());
        assert_eq!(node_view(&server).cpu_used, 0);
        submit_resources(&server, "new-a", 12, 13 << 30, 1);
        submit_resources(&server, "new-b", 12, 13 << 30, 1);
        assert_eq!(poll_assignment(&server).unwrap().cpus, "0-11");
        assert_eq!(poll_assignment(&server).unwrap().cpus, "12-23");
        assert_eq!(node_view(&server).cpu_used, 24);
        std::fs::remove_dir_all(root).unwrap();
    }
}

async fn handle_stream<S>(
    server: Server,
    stream: S,
    require_auth: bool,
    expected_token: Option<String>,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let Some(first_request) = read_request(&mut reader).await? else {
        return Ok(());
    };
    let Request::Hello {
        protocol_version,
        auth_token,
    } = first_request
    else {
        write_response(
            &mut writer,
            &Response::Error("protocol hello required as the first request".into()),
        )
        .await?;
        return Ok(());
    };
    if protocol_version != PROTOCOL_VERSION {
        write_response(
            &mut writer,
            &Response::Error(format!(
                "protocol version mismatch: client={protocol_version}, server={PROTOCOL_VERSION}"
            )),
        )
        .await?;
        return Ok(());
    }
    if require_auth && expected_token.as_deref() != auth_token.as_deref() {
        write_response(
            &mut writer,
            &Response::Error("authentication failed".into()),
        )
        .await?;
        return Ok(());
    }
    let mut server_capabilities = capabilities();
    if expected_token.is_some() {
        server_capabilities.push("tcp_auth_token".into());
    }
    write_response(
        &mut writer,
        &Response::Hello {
            protocol_version: PROTOCOL_VERSION,
            capabilities: server_capabilities,
        },
    )
    .await?;
    while let Some(request) = read_request(&mut reader).await? {
        let response = match server.handle(request) {
            Ok(response) => response,
            Err(error) => Response::Error(error.to_string()),
        };
        write_response(&mut writer, &response).await?;
    }
    Ok(())
}

async fn read_request<R>(reader: &mut BufReader<R>) -> Result<Option<Request>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(None);
        }
        if line.trim().is_empty() {
            continue;
        }
        return Ok(Some(
            serde_json::from_str(line.trim()).context("invalid request")?,
        ));
    }
}

async fn write_response<W>(writer: &mut W, response: &Response) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    writer
        .write_all((serde_json::to_string(response)? + "\n").as_bytes())
        .await?;
    writer.flush().await?;
    Ok(())
}
