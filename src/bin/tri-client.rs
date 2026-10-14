use anyhow::{Context, Result};
use clap::Parser;
use std::collections::HashMap;
use tokio::process::Command;
use tokio::time::{Duration, Instant, sleep};
use tri::client;
use tri::model::{
    NodeHeartbeat, NodeRegistration, ObservedUnit, RunningTask, TaskId, TaskResult, TaskState,
};
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
    #[arg(long, env = "TRI_NODE_GPU_DEVICES", value_delimiter = ',')]
    gpu_devices: Vec<String>,
    #[arg(long, env = "TRI_NODE_SCRATCH", default_value = "/tmp/tri-scratch")]
    scratch: String,
}

struct LocalTask {
    task_id: TaskId,
    unit: String,
    lease_id: Option<String>,
    attempt: u32,
    cpu_count: u32,
    memory_bytes: u64,
    gpu_count: u32,
    timeout_seconds: Option<u64>,
    scratch: std::path::PathBuf,
}

struct TaskCompletion {
    state: TaskState,
    exit_code: Option<i32>,
    error: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let args = Args::parse();
    std::fs::create_dir_all(&args.scratch)?;
    let gpu_devices = if args.gpu_devices.is_empty() {
        (0..args.gpus)
            .map(|index| format!("/dev/nvidia{index}"))
            .collect()
    } else {
        args.gpu_devices.clone()
    };
    anyhow::ensure!(
        gpu_devices.len() >= args.gpus as usize,
        "TRI_NODE_GPU_DEVICES must list at least TRI_NODE_GPUS device paths"
    );
    let registration = NodeRegistration {
        name: args.name.clone(),
        address: args.address.clone(),
        cpu_total: args.cpus,
        memory_bytes: args.memory,
        gpu_total: args.gpus,
        scratch: args.scratch.clone(),
        gpu_devices: gpu_devices.clone(),
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
    if let Err(error) = reconcile_units(&args.name, &args.scratch, &mut running).await {
        tracing::warn!(%error, "initial task reconciliation failed");
    }
    let mut next_heartbeat = Instant::now();
    let mut next_registration = Instant::now() + Duration::from_secs(10);
    loop {
        let mut completed = Vec::new();
        for (id, local) in &running {
            if let Some(result) = poll_task(&local.unit, local.timeout_seconds).await? {
                completed.push((*id, result));
            }
        }
        for (id, result) in completed {
            if let Some(local) = running.remove(&id) {
                if let Err(error) = std::fs::remove_dir_all(&local.scratch)
                    && error.kind() != std::io::ErrorKind::NotFound
                {
                    tracing::warn!(task = id, %error, "failed to remove task scratch directory");
                }
                cleanup_task_unit(&local.unit).await;
                let _ = client::tcp(&Request::Complete {
                    result: TaskResult {
                        id,
                        state: result.state,
                        exit_code: result.exit_code,
                        error: result.error,
                        attempt: local.attempt,
                        lease_id: local.lease_id,
                    },
                })
                .await;
            }
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
                    if let Err(error) =
                        reconcile_units(&args.name, &args.scratch, &mut running).await
                    {
                        tracing::warn!(%error, "task reconciliation failed");
                    }
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
                            lease_id: task.lease_id.clone(),
                        })
                        .collect(),
                },
            })
            .await;
            if let Ok(Response::Heartbeat { cancel }) = response {
                for id in cancel {
                    if let Some(unit) = running.get(&id).map(|task| task.unit.clone()) {
                        let _ = stop_task_unit(&unit).await;
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
                &gpu_devices,
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
                            attempt: assignment.task.attempt,
                            lease_id: assignment.task.lease_id.clone(),
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
    gpu_devices: &[String],
) -> Result<Option<LocalTask>> {
    if task.spec.command.is_empty() {
        let _ = client::tcp(&Request::Complete {
            result: TaskResult {
                id: task.id,
                state: TaskState::Fail,
                exit_code: None,
                error: Some("empty command".into()),
                attempt: task.attempt,
                lease_id: task.lease_id.clone(),
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
    let workdir = if task.spec.scratch_cwd {
        scratch.clone()
    } else if let Some(cwd) = &task.spec.cwd {
        std::path::PathBuf::from(cwd)
    } else {
        std::env::current_dir()?
    };
    let unit = format!("tri-task-{}-{}", task.id, task.attempt);
    let mut command = Command::new("systemd-run");
    command.args([
        "--user",
        "--quiet",
        "--no-block",
        "--remain-after-exit",
        "--service-type=exec",
        "--unit",
        &unit,
        "--working-directory",
    ]);
    command.arg(&workdir);
    command.args([
        "--property",
        &format!("StandardOutput=append:{}", task.stdout_path),
        "--property",
        &format!("StandardError=append:{}", task.stderr_path),
        "--property",
        "OOMPolicy=stop",
    ]);
    if task.spec.requested_mem_bytes > 0 {
        command.args([
            "--property",
            &format!("MemoryMax={}", task.spec.requested_mem_bytes),
            "--property",
            "MemorySwapMax=0",
        ]);
    }
    if let Some(timeout) = task.spec.timeout_seconds {
        command.args(["--property", &format!("RuntimeMaxSec={timeout}")]);
    }
    if !cpus.is_empty() {
        command.args(["--property", &format!("AllowedCPUs={cpus}")]);
    }
    if let Some(cwd) = &task.spec.cwd {
        command.args(["--setenv", &format!("TRI_JOB_CWD={cwd}")]);
    }
    command.args([
        "--setenv",
        &format!("TRI_TASK_ID={}", task.id),
        "--setenv",
        &format!("TRI_SCRATCH={}", scratch.display()),
        "--setenv",
        &format!("TMPDIR={}", scratch.display()),
    ]);
    for (key, value) in &task.spec.env {
        command.args(["--setenv", &format!("{key}={value}")]);
    }
    if !gpus.is_empty() {
        command.args(["--property", "DevicePolicy=closed"]);
        for device in standard_device_paths() {
            command.args(["--property", &format!("DeviceAllow={device} rwm")]);
        }
        for device in gpu_support_device_paths() {
            command.args(["--property", &format!("DeviceAllow={device} rwm")]);
        }
        for index in parse_resource_set(gpus) {
            let device = gpu_devices
                .get(index as usize)
                .with_context(|| format!("no device path configured for GPU {index}"))?;
            command.args(["--property", &format!("DeviceAllow={device} rwm")]);
        }
        command.args([
            "--setenv",
            &format!("CUDA_VISIBLE_DEVICES={gpus}"),
            "--setenv",
            &format!("NVIDIA_VISIBLE_DEVICES={gpus}"),
        ]);
    }
    if cpus.is_empty() {
        command.arg(&task.spec.command[0]);
        command.args(&task.spec.command[1..]);
    } else {
        command.args(["taskset", "-c", cpus, &task.spec.command[0]]);
        command.args(&task.spec.command[1..]);
    }
    let status = command
        .status()
        .await
        .with_context(|| format!("starting task service for {}", task.id))?;
    anyhow::ensure!(
        status.success(),
        "systemd-run could not start task {} (status {})",
        task.id,
        status
    );
    Ok(Some(LocalTask {
        task_id: task.id,
        unit,
        lease_id: task.lease_id.clone(),
        attempt: task.attempt,
        cpu_count: task.spec.requested_cpu,
        memory_bytes: task.spec.requested_mem_bytes,
        gpu_count: task.spec.requested_gpus,
        timeout_seconds: task.spec.timeout_seconds,
        scratch,
    }))
}

async fn reconcile_units(
    node: &str,
    scratch_root: &str,
    running: &mut HashMap<TaskId, LocalTask>,
) -> Result<()> {
    let (units, mut stop) = list_task_units().await?;
    let response = client::tcp(&Request::Reconcile {
        node: node.to_owned(),
        units,
    })
    .await?;
    let Response::Reconciled {
        adopt,
        stop: server_stop,
    } = response
    else {
        anyhow::bail!("unexpected task reconciliation response: {response:?}")
    };
    stop.extend(server_stop);
    for unit in stop {
        if let Err(error) = stop_task_unit(&unit).await {
            tracing::warn!(%error, %unit, "failed to stop orphan task unit");
        }
    }
    for task in adopt {
        if running.contains_key(&task.id) {
            continue;
        }
        let unit = format!("tri-task-{}-{}", task.id, task.attempt);
        running.insert(task.id, local_task_from_task(&task, unit, scratch_root)?);
    }
    Ok(())
}

async fn list_task_units() -> Result<(Vec<ObservedUnit>, Vec<String>)> {
    let output = Command::new("systemctl")
        .args([
            "--user",
            "list-units",
            "--all",
            "--plain",
            "--no-legend",
            "tri-task-*.service",
        ])
        .output()
        .await
        .context("listing tri task units")?;
    anyhow::ensure!(
        output.status.success(),
        "systemctl could not list task units: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    let mut units = Vec::new();
    let mut stop = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let Some(unit) = line.split_whitespace().next() else {
            continue;
        };
        let unit = unit.strip_suffix(".service").unwrap_or(unit);
        if let Some((task_id, attempt)) = parse_task_unit(unit) {
            units.push(ObservedUnit {
                unit: unit.to_owned(),
                task_id,
                attempt,
            });
        } else {
            stop.push(unit.to_owned());
        }
    }
    Ok((units, stop))
}

fn parse_task_unit(unit: &str) -> Option<(TaskId, u32)> {
    let suffix = unit.strip_prefix("tri-task-")?;
    let (id, attempt) = suffix.rsplit_once('-')?;
    Some((id.parse().ok()?, attempt.parse().ok()?))
}

fn local_task_from_task(
    task: &tri::model::Task,
    unit: String,
    scratch_root: &str,
) -> Result<LocalTask> {
    let scratch = std::path::Path::new(scratch_root).join(task.id.to_string());
    std::fs::create_dir_all(&scratch)?;
    Ok(LocalTask {
        task_id: task.id,
        unit,
        lease_id: task.lease_id.clone(),
        attempt: task.attempt,
        cpu_count: task.spec.requested_cpu,
        memory_bytes: task.spec.requested_mem_bytes,
        gpu_count: task.spec.requested_gpus,
        timeout_seconds: task.spec.timeout_seconds,
        scratch,
    })
}

fn parse_resource_set(value: &str) -> Vec<u32> {
    let mut result = Vec::new();
    for part in value.split(',') {
        if let Some((start, end)) = part.split_once('-') {
            if let (Ok(start), Ok(end)) = (start.parse::<u32>(), end.parse::<u32>()) {
                result.extend(start..=end);
            }
        } else if let Ok(index) = part.parse::<u32>() {
            result.push(index);
        }
    }
    result
}

fn standard_device_paths() -> [&'static str; 5] {
    [
        "/dev/null",
        "/dev/zero",
        "/dev/full",
        "/dev/random",
        "/dev/urandom",
    ]
}

fn gpu_support_device_paths() -> Vec<&'static str> {
    [
        "/dev/nvidiactl",
        "/dev/nvidia-uvm",
        "/dev/nvidia-uvm-tools",
        "/dev/nvidia-modeset",
        "/dev/nvidia-caps/*",
    ]
    .into_iter()
    .filter(|path| {
        if path.ends_with("/*") {
            std::path::Path::new(path.trim_end_matches("/*")).exists()
        } else {
            std::path::Path::new(path).exists()
        }
    })
    .collect()
}

async fn poll_task(unit: &str, timeout_seconds: Option<u64>) -> Result<Option<TaskCompletion>> {
    let unit = format!("{unit}.service");
    let output = Command::new("systemctl")
        .args([
            "--user",
            "show",
            &unit,
            "-p",
            "LoadState",
            "-p",
            "ActiveState",
            "-p",
            "SubState",
            "-p",
            "Result",
            "-p",
            "ExecMainCode",
            "-p",
            "ExecMainStatus",
        ])
        .output()
        .await
        .with_context(|| format!("checking task service {unit}"))?;
    anyhow::ensure!(
        output.status.success(),
        "systemctl could not inspect task service {unit}: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let fields = stdout
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect::<HashMap<_, _>>();
    if fields.get("LoadState") == Some(&"not-found") {
        anyhow::bail!("task service {unit} disappeared")
    }
    let active = fields.get("ActiveState").copied().unwrap_or_default();
    let sub = fields.get("SubState").copied().unwrap_or_default();
    if active == "activating" || active == "deactivating" || (active == "active" && sub != "exited")
    {
        return Ok(None);
    }
    let result = fields.get("Result").copied().unwrap_or_default();
    let status = fields
        .get("ExecMainStatus")
        .and_then(|value| value.parse::<i32>().ok());
    if result == "oom-kill" {
        return Ok(Some(TaskCompletion {
            state: TaskState::Fail,
            exit_code: None,
            error: Some("memory limit exceeded (MemoryMax)".into()),
        }));
    }
    if result == "timeout" {
        return Ok(Some(TaskCompletion {
            state: TaskState::TimedOut,
            exit_code: status,
            error: Some(format!(
                "task exceeded timeout of {} seconds",
                timeout_seconds.unwrap_or_default()
            )),
        }));
    }
    let success = result == "success" && status == Some(0);
    Ok(Some(TaskCompletion {
        state: if success {
            TaskState::Done
        } else {
            TaskState::Fail
        },
        exit_code: status,
        error: (!success).then(|| {
            if let Some(status) = status {
                format!("task exited with status {status}")
            } else {
                format!("task ended with systemd result {result}")
            }
        }),
    }))
}

async fn stop_task_unit(unit: &str) -> Result<()> {
    let unit = format!("{unit}.service");
    let status = Command::new("systemctl")
        .args(["--user", "stop", &unit])
        .status()
        .await
        .with_context(|| format!("stopping task service {unit}"))?;
    anyhow::ensure!(
        status.success(),
        "systemctl could not stop task service {unit}"
    );
    Ok(())
}

async fn cleanup_task_unit(unit: &str) {
    let _ = stop_task_unit(unit).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_task_unit_identity() {
        assert_eq!(parse_task_unit("tri-task-42-3"), Some((42, 3)));
        assert_eq!(parse_task_unit("tri-task-42-3.service"), None);
        assert_eq!(parse_task_unit("tri-task-invalid"), None);
    }

    #[test]
    fn expands_gpu_ranges() {
        assert_eq!(parse_resource_set("0-1,3"), vec![0, 1, 3]);
        assert!(parse_resource_set("").is_empty());
    }
}
