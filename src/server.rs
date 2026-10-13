use crate::model::{NodeHeartbeat, NodeInfo, NodeRegistration, TaskId, TaskState};
use crate::protocol::{Request, Response};
use crate::store::Store;
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
                let before = self.store.get(id)?;
                match self.store.cancel(id)? {
                    Some(task) => {
                        if before
                            .as_ref()
                            .is_some_and(|task| task.state == TaskState::Run)
                        {
                            self.release_resources(&task);
                        }
                        Ok(Response::Task(task))
                    }
                    None => Ok(Response::Error(format!("task {id} does not exist"))),
                }
            }
            Request::Nodes => {
                let nodes = self.nodes.lock().expect("nodes mutex poisoned");
                Ok(Response::Nodes(nodes.values().cloned().collect()))
            }
            Request::Clean => Ok(Response::Message(format!(
                "removed {} tasks",
                self.store.clean()?
            ))),
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
        for running in heartbeat.running_tasks {
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
        for task in tasks {
            if task
                .spec
                .requested_node
                .as_deref()
                .is_some_and(|name| name != node_name)
            {
                continue;
            }
            let available_cpu = node.cpu_total.saturating_sub(node.cpu_used);
            let available_mem = node.memory_bytes.saturating_sub(node.memory_used);
            let available_gpus = node.gpu_total.saturating_sub(node.gpu_used);
            if task.spec.requested_cpu > available_cpu
                || task.spec.requested_mem_bytes > available_mem
                || task.spec.requested_gpus > available_gpus
            {
                continue;
            }
            let start = node.cpu_used;
            let end = start + task.spec.requested_cpu.saturating_sub(1);
            let cpus = if task.spec.requested_cpu == 0 {
                String::new()
            } else {
                format!("{start}-{end}")
            };
            let gpus = if task.spec.requested_gpus == 0 {
                String::new()
            } else {
                let gpu_start = node.gpu_used;
                let gpu_end = gpu_start + task.spec.requested_gpus - 1;
                format!("{gpu_start}-{gpu_end}")
            };
            if let Some(task) = self.store.claim(task.id, node_name, &cpus, &gpus)? {
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
            && let Some(node_name) = task.node
        {
            let mut nodes = self.nodes.lock().expect("nodes mutex poisoned");
            if let Some(node) = nodes.get_mut(&node_name) {
                node.cpu_used = node.cpu_used.saturating_sub(task.spec.requested_cpu);
                node.memory_used = node
                    .memory_used
                    .saturating_sub(task.spec.requested_mem_bytes);
                node.gpu_used = node.gpu_used.saturating_sub(task.spec.requested_gpus);
                node.running_tasks.retain(|id| *id != task.id);
            }
        }
        Ok(())
    }

    fn release_resources(&self, task: &crate::model::Task) {
        if let Some(node_name) = &task.node {
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
