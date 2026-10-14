use crate::model::{NodeHeartbeat, NodeInfo, NodeRegistration, TaskId, TaskState};
use crate::protocol::{Request, Response};
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
                            if let Err(error) = handle_stream(server, stream).await {
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
            tokio::spawn(async move {
                if let Err(error) = handle_stream(server, stream).await {
                    tracing::warn!(%peer, %error, "TCP request failed");
                }
            });
        }
    }

    pub fn handle(&self, request: Request) -> Result<Response> {
        match request {
            Request::Submit { spec } => Ok(Response::Task(self.store.submit(spec)?)),
            Request::List { state } => Ok(Response::Tasks(self.store.list(state.as_deref())?)),
            Request::Get { id } => match self.store.get(id)? {
                Some(task) => Ok(Response::Task(task)),
                None => Ok(Response::Error(format!("task {id} does not exist"))),
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
                Ok(Response::Nodes(nodes.values().cloned().collect()))
            }
            Request::Clean => Ok(Response::Cleaned {
                removed: self.store.clean()?,
            }),
            Request::Register { node } => {
                self.register(node);
                Ok(Response::Ok)
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
                },
            );
        }
    }

    fn heartbeat(&self, heartbeat: NodeHeartbeat) -> Result<Vec<TaskId>> {
        let mut nodes = self.nodes.lock().expect("nodes mutex poisoned");
        if let Some(node) = nodes.get_mut(&heartbeat.name) {
            node.online = true;
            node.last_seen = crate::now_string();
            node.cpu_used = heartbeat.cpu_used;
            node.memory_used = heartbeat.memory_used;
            node.gpu_used = heartbeat.gpu_used;
            node.running_tasks = heartbeat.running_tasks.iter().map(|task| task.id).collect();
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

    fn poll(&self, node_name: &str) -> Result<Option<crate::model::TaskAssignment>> {
        let mut nodes = self.nodes.lock().expect("nodes mutex poisoned");
        let Some(node) = nodes.get_mut(node_name) else {
            return Ok(None);
        };
        if !node.online {
            return Ok(None);
        }
        let tasks = self.store.list(Some("wait"))?;
        let running = self.store.list(None)?;
        let mut occupied_cpus = std::collections::BTreeSet::new();
        let mut occupied_gpus = std::collections::BTreeSet::new();
        let mut occupied_memory = 0u64;
        for running_task in running.iter().filter(|task| {
            task.node.as_deref() == Some(node_name)
                && matches!(task.state, TaskState::Run | TaskState::Cancel)
        }) {
            occupied_cpus.extend(parse_resource_set(running_task.assigned_cpus.as_deref()));
            occupied_gpus.extend(parse_resource_set(running_task.assigned_gpus.as_deref()));
            occupied_memory = occupied_memory.saturating_add(running_task.spec.requested_mem_bytes);
        }
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
                first_free_range(node.cpu_total, task.spec.requested_cpu, &occupied_cpus)
            else {
                continue;
            };
            let Some(gpus) =
                first_free_range(node.gpu_total, task.spec.requested_gpus, &occupied_gpus)
            else {
                continue;
            };
            if let Some(task) = self.store.claim(task.id, node_name, &cpus, &gpus)? {
                occupied_cpus.extend(parse_resource_set(Some(&cpus)));
                occupied_gpus.extend(parse_resource_set(Some(&gpus)));
                node.cpu_used += task.spec.requested_cpu;
                node.memory_used += task.spec.requested_mem_bytes;
                node.gpu_used += task.spec.requested_gpus;
                node.running_tasks.push(task.id);
                return Ok(Some(crate::model::TaskAssignment { task, cpus, gpus }));
            }
        }
        Ok(None)
    }

    fn complete(&self, result: &crate::model::TaskResult) -> Result<()> {
        let task = self.store.get(result.id)?;
        self.store.complete(result)?;
        if let Some(task) = task
            && let Some(node_name) = task.node.as_deref()
            && matches!(task.state, TaskState::Run | TaskState::Cancel)
            && task.lease_id == result.lease_id
            && task.attempt == result.attempt
        {
            self.release_node_resources(node_name, &task);
        }
        Ok(())
    }

    fn release_node_resources(&self, node_name: &str, task: &crate::model::Task) {
        let mut nodes = self.nodes.lock().expect("nodes mutex poisoned");
        if let Some(node) = nodes.get_mut(node_name) {
            node.cpu_used = node.cpu_used.saturating_sub(task.spec.requested_cpu);
            node.memory_used = node
                .memory_used
                .saturating_sub(task.spec.requested_mem_bytes);
            node.gpu_used = node.gpu_used.saturating_sub(task.spec.requested_gpus);
            node.running_tasks.retain(|id| *id != task.id);
        }
    }

    fn reap_expired(&self) -> Result<()> {
        let expired = self.store.reap_expired(crate::now_seconds())?;
        for task in expired {
            if let Some(node_name) = task.node.as_deref() {
                self.release_node_resources(node_name, &task);
            }
        }
        let now = crate::now_seconds();
        let mut nodes = self.nodes.lock().expect("nodes mutex poisoned");
        for node in nodes.values_mut() {
            let last_seen = node.last_seen.parse::<u64>().unwrap_or(0);
            node.online = now.saturating_sub(last_seen) <= LEASE_SECONDS;
        }
        Ok(())
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
                    memory_bytes: 1024,
                    gpu_total: 0,
                    scratch: PathBuf::from("/tmp").to_string_lossy().into_owned(),
                },
            })
            .unwrap();
        let ids = ["a", "b", "c"]
            .into_iter()
            .map(|name| {
                match server
                    .handle(Request::Submit {
                        spec: spec(name, 2),
                    })
                    .unwrap()
                {
                    Response::Task(task) => task.id,
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
}

async fn handle_stream<S>(server: Server, stream: S) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    if line.trim().is_empty() {
        return Ok(());
    }
    let response = match serde_json::from_str::<Request>(line.trim()) {
        Ok(request) => match server.handle(request) {
            Ok(response) => response,
            Err(error) => Response::Error(error.to_string()),
        },
        Err(error) => Response::Error(format!("invalid request: {error}")),
    };
    writer
        .write_all((serde_json::to_string(&response)? + "\n").as_bytes())
        .await?;
    writer.flush().await?;
    Ok(())
}
