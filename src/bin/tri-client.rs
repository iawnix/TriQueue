use anyhow::{Context, Result};
use clap::Parser;
use std::collections::HashMap;
use std::process::Stdio;
use tokio::process::{Child, Command};
use tokio::time::{Duration, Instant, sleep};
use tri::client;
use tri::model::{NodeHeartbeat, NodeRegistration, RunningTask, TaskId, TaskResult, TaskState};
use tri::protocol::{Request, Response};
use tri::{init_logging, parse_bytes};

#[derive(Debug, Parser)]
#[command(name = "tri-client", about = "tri compute-node client")]
struct Args {
    #[arg(long, env = "TRI_NODE")]
    name: String,
    #[arg(long, env = "TRI_NODE_ADDRESS", default_value = "unknown")]
    address: String,
    #[arg(long, env = "TRI_NODE_CPUS", default_value_t = 1)]
    cpus: u32,
    #[arg(long, env = "TRI_NODE_MEMORY", default_value = "1G", value_parser = parse_bytes)]
    memory: u64,
    #[arg(long, env = "TRI_NODE_GPUS", default_value_t = 0)]
    gpus: u32,
    #[arg(long, env = "TRI_NODE_SCRATCH", default_value = "/tmp/tri-scratch")]
    scratch: String,
}

struct LocalTask {
    task_id: TaskId,
    cpu_count: u32,
    memory_bytes: u64,
    gpu_count: u32,
    scratch: std::path::PathBuf,
    pid: Option<u32>,
    child: Child,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let args = Args::parse();
    std::fs::create_dir_all(&args.scratch)?;
    let registration = NodeRegistration {
        name: args.name.clone(),
        address: args.address.clone(),
        cpu_total: args.cpus,
        memory_bytes: args.memory,
        gpu_total: args.gpus,
        scratch: args.scratch.clone(),
    };

    loop {
        match client::tcp(&Request::Register {
            node: registration.clone(),
        })
        .await
        {
            Ok(Response::Ok) => break,
            Ok(other) => tracing::warn!(?other, "unexpected registration response"),
            Err(error) => tracing::warn!(%error, "tri-server unavailable; retrying registration"),
        }
        sleep(Duration::from_secs(2)).await;
    }

    let mut running: HashMap<TaskId, LocalTask> = HashMap::new();
    let mut next_heartbeat = Instant::now();
    let mut next_registration = Instant::now() + Duration::from_secs(10);
    loop {
        let mut completed = Vec::new();
        for (id, local) in running.iter_mut() {
            if let Some(status) = local.child.try_wait()? {
                completed.push((*id, status.code()));
            }
        }
        for (id, code) in completed {
            if let Some(local) = running.remove(&id)
                && let Err(error) = std::fs::remove_dir_all(&local.scratch)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                tracing::warn!(task = id, %error, "failed to remove task scratch directory");
            }
            let state = if code == Some(0) {
                TaskState::Done
            } else {
                TaskState::Fail
            };
            let _ = client::tcp(&Request::Complete {
                result: TaskResult {
                    id,
                    state,
                    exit_code: code,
                    error: None,
                },
            })
            .await;
        }

        let now = Instant::now();
        if now >= next_registration {
            match client::tcp(&Request::Register {
                node: registration.clone(),
            })
            .await
            {
                Ok(Response::Ok) => {
                    next_registration = Instant::now() + Duration::from_secs(10);
                }
                Ok(other) => {
                    tracing::warn!(?other, "unexpected re-registration response");
                    next_registration = Instant::now() + Duration::from_secs(2);
                }
                Err(error) => {
                    tracing::warn!(%error, "tri-server unavailable; retrying registration");
                    next_registration = Instant::now() + Duration::from_secs(2);
                }
            }
        }

        if now >= next_heartbeat {
            let response = client::tcp(&Request::Heartbeat {
                heartbeat: NodeHeartbeat {
                    name: args.name.clone(),
                    cpu_used: running.values().map(|task| task.cpu_count).sum(),
                    memory_used: running.values().map(|task| task.memory_bytes).sum(),
                    gpu_used: running.values().map(|task| task.gpu_count).sum(),
                    running_tasks: running
                        .values()
                        .map(|task| RunningTask {
                            id: task.task_id,
                            cpu_count: task.cpu_count,
                            memory_bytes: task.memory_bytes,
                            gpu_count: task.gpu_count,
                        })
                        .collect(),
                },
            })
            .await;
            if let Ok(Response::Heartbeat { cancel }) = response {
                for id in cancel {
                    if let Some(task) = running.get_mut(&id) {
                        if let Some(pid) = task.pid {
                            terminate_group(pid);
                        } else {
                            let _ = task.child.kill().await;
                        }
                    }
                }
            }
            next_heartbeat = Instant::now() + Duration::from_secs(2);
        }

        if running.len() < args.cpus as usize
            && let Ok(Response::Assignment(Some(assignment))) = client::tcp(&Request::Poll {
                node: args.name.clone(),
            })
            .await
        {
            match spawn_task(
                &assignment.task,
                &assignment.cpus,
                &assignment.gpus,
                &args.scratch,
            )
            .await
            {
                Ok(Some(local)) => {
                    running.insert(assignment.task.id, local);
                }
                Ok(None) => {}
                Err(error) => {
                    let scratch =
                        std::path::Path::new(&args.scratch).join(assignment.task.id.to_string());
                    let _ = std::fs::remove_dir_all(scratch);
                    let _ = client::tcp(&Request::Complete {
                        result: TaskResult {
                            id: assignment.task.id,
                            state: TaskState::Fail,
                            exit_code: None,
                            error: Some(error.to_string()),
                        },
                    })
                    .await;
                }
            }
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn spawn_task(
    task: &tri::model::Task,
    cpus: &str,
    gpus: &str,
    scratch_root: &str,
) -> Result<Option<LocalTask>> {
    if task.spec.command.is_empty() {
        let _ = client::tcp(&Request::Complete {
            result: TaskResult {
                id: task.id,
                state: TaskState::Fail,
                exit_code: None,
                error: Some("empty command".into()),
            },
        })
        .await;
        return Ok(None);
    }
    let scratch = std::path::Path::new(scratch_root).join(task.id.to_string());
    std::fs::create_dir_all(&scratch)?;
    if let Some(parent) = std::path::Path::new(&task.stdout_path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    if task.stderr_path != task.stdout_path
        && let Some(parent) = std::path::Path::new(&task.stderr_path).parent()
    {
        std::fs::create_dir_all(parent)?;
    }
    let stdout = std::fs::File::create(&task.stdout_path)
        .with_context(|| format!("opening {}", task.stdout_path))?;
    let stderr = if task.stderr_path == task.stdout_path {
        stdout
            .try_clone()
            .with_context(|| format!("duplicating {}", task.stderr_path))?
    } else {
        std::fs::File::create(&task.stderr_path)
            .with_context(|| format!("opening {}", task.stderr_path))?
    };
    let mut command = if cpus.is_empty() {
        let mut command = Command::new(&task.spec.command[0]);
        command.args(&task.spec.command[1..]);
        command
    } else {
        let mut command = Command::new("taskset");
        command.arg("-c").arg(cpus).arg(&task.spec.command[0]);
        command.args(&task.spec.command[1..]);
        command
    };
    if let Some(cwd) = &task.spec.cwd {
        command.env("TRI_JOB_CWD", cwd);
    }
    if task.spec.scratch_cwd {
        command.current_dir(&scratch);
    } else if let Some(cwd) = &task.spec.cwd {
        command.current_dir(cwd);
    }
    command.env("TRI_TASK_ID", task.id.to_string());
    command.env("TRI_SCRATCH", &scratch);
    command.env("TMPDIR", &scratch);
    for (key, value) in &task.spec.env {
        command.env(key, value);
    }
    if !gpus.is_empty() {
        command.env("CUDA_VISIBLE_DEVICES", gpus);
        command.env("NVIDIA_VISIBLE_DEVICES", gpus);
    }
    command.stdout(Stdio::from(stdout));
    command.stderr(Stdio::from(stderr));
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command.spawn().with_context(|| {
        format!(
            "starting task {}: {}",
            task.id,
            tri::command_to_string(&task.spec.command)
        )
    })?;
    Ok(Some(LocalTask {
        task_id: task.id,
        cpu_count: task.spec.requested_cpu,
        memory_bytes: task.spec.requested_mem_bytes,
        gpu_count: task.spec.requested_gpus,
        scratch,
        pid: child.id(),
        child,
    }))
}

fn terminate_group(pid: u32) {
    unsafe {
        let _ = libc::kill(-(pid as i32), libc::SIGTERM);
    }
}
