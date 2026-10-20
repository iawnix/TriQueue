use anyhow::{Context, Result};
use clap::Parser;
use std::collections::HashMap;
use std::sync::OnceLock;
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
    #[arg(long, env = "TRI_NODE", required_unless_present = "check")]
    name: Option<String>,
    /// Verify resource enforcement in a temporary unit, without joining a queue.
    #[arg(long)]
    check: bool,
    /// Run task units through the system manager as this non-root account (client requires root).
    #[arg(long, env = "TRI_TASK_USER")]
    task_user: Option<String>,
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

static TASK_USER: OnceLock<Option<String>> = OnceLock::new();

fn task_user() -> Option<&'static str> {
    TASK_USER.get().and_then(|user| user.as_deref())
}

fn systemctl_command() -> Command {
    let mut command = Command::new("systemctl");
    if task_user().is_none() {
        command.arg("--user");
    }
    command
}

fn create_directory(path: &std::path::Path) -> Result<()> {
    if let Some(user) = task_user() {
        let status = std::process::Command::new("runuser")
            .args(["-u", user, "--", "mkdir", "-p", "--"])
            .arg(path)
            .status()?;
        anyhow::ensure!(
            status.success(),
            "task account cannot create {}",
            path.display()
        );
    } else {
        std::fs::create_dir_all(path)?;
    }
    Ok(())
}

fn remove_scratch(path: &std::path::Path) -> Result<()> {
    if let Some(user) = task_user() {
        let status = std::process::Command::new("runuser")
            .args(["-u", user, "--", "rm", "-rf", "--"])
            .arg(path)
            .status()?;
        anyhow::ensure!(
            status.success(),
            "task account cannot remove {}",
            path.display()
        );
    } else if let Err(error) = std::fs::remove_dir_all(path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        return Err(error.into());
    }
    Ok(())
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
    completion: Option<TaskCompletion>,
    enforcement_failure: Option<String>,
}

struct TaskCompletion {
    state: TaskState,
    exit_code: Option<i32>,
    error: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let raw: Vec<String> = std::env::args().collect();
    if raw.get(1).is_some_and(|arg| arg == "__run-task") {
        anyhow::ensure!(
            raw.len() >= 5 && raw[3] == "--",
            "invalid internal task invocation"
        );
        tri::resources::exec_guarded(&raw[2], &raw[4..]);
    }
    init_logging();
    let args = Args::parse();
    if let Some(user) = &args.task_user {
        anyhow::ensure!(
            unsafe { libc::geteuid() } == 0,
            "--task-user requires a root-owned system service"
        );
        let output = std::process::Command::new("id")
            .args(["-u", "--", user])
            .output()?;
        anyhow::ensure!(
            output.status.success() && String::from_utf8_lossy(&output.stdout).trim() != "0",
            "--task-user must name an existing non-root account"
        );
    } else {
        anyhow::ensure!(
            unsafe { libc::geteuid() } != 0,
            "root clients must set --task-user to a non-root account"
        );
    }
    TASK_USER
        .set(args.task_user.clone())
        .expect("execution mode already initialized");
    if !args.check {
        create_directory(std::path::Path::new(&args.scratch))?;
    }
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
    if args.check {
        check_resource_enforcement(&gpu_devices)?;
        println!("CPU, memory, swap, and unallocated GPU access checks passed");
        return Ok(());
    }
    let node_name = args.name.context("node name is required")?;
    let registration = NodeRegistration {
        name: node_name.clone(),
        address: args.address.clone(),
        cpu_total: args.cpus,
        memory_bytes: args.memory,
        gpu_total: args.gpus,
        scratch: args.scratch.clone(),
        gpu_devices: gpu_devices.clone(),
        capabilities: node_capabilities(&gpu_devices),
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
    if let Err(error) = reconcile_units(&node_name, &args.scratch, &mut running).await {
        tracing::warn!(%error, "initial task reconciliation failed");
    }
    let mut next_heartbeat = Instant::now();
    let mut next_registration = Instant::now() + Duration::from_secs(10);
    loop {
        for local in running.values_mut() {
            if local.completion.is_none() {
                if let Some(reason) = &local.enforcement_failure {
                    match stop_task_unit(&local.unit).await {
                        Ok(()) => {
                            local.completion = Some(TaskCompletion {
                                state: TaskState::Fail,
                                exit_code: None,
                                error: Some(reason.clone()),
                            })
                        }
                        Err(error) => {
                            tracing::warn!(%error, unit = %local.unit, "unsafe task stop not confirmed; retrying")
                        }
                    }
                } else {
                    local.completion = poll_task(&local.unit, local.timeout_seconds).await?;
                }
            }
        }
        let mut acknowledged = Vec::new();
        for (id, local) in &running {
            let Some(result) = &local.completion else {
                continue;
            };
            let response = client::tcp(&Request::Complete {
                result: TaskResult {
                    id: *id,
                    state: result.state.clone(),
                    exit_code: result.exit_code,
                    error: result.error.clone(),
                    attempt: local.attempt,
                    lease_id: local.lease_id.clone(),
                },
            })
            .await;
            if matches!(response, Ok(Response::Ok)) {
                acknowledged.push(*id);
            } else {
                tracing::warn!(
                    task = id,
                    ?response,
                    "completion not acknowledged; retrying"
                );
            }
        }
        for id in acknowledged {
            if let Some(local) = running.remove(&id) {
                if let Err(error) = remove_scratch(&local.scratch) {
                    tracing::warn!(task = id, %error, "failed to remove task scratch directory");
                }
                cleanup_task_unit(&local.unit).await;
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
                        reconcile_units(&node_name, &args.scratch, &mut running).await
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
                    name: node_name.clone(),
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
                    if let Some(local) = running.get_mut(&id)
                        && local.completion.is_none()
                    {
                        match stop_task_unit(&local.unit).await {
                            Ok(()) => {
                                // systemctl stop waits for the stop job, including
                                // KillMode=control-group cleanup. The unit may be
                                // unloaded immediately afterwards: retain the ack.
                                local.completion = Some(TaskCompletion {
                                    state: TaskState::Cancel,
                                    exit_code: None,
                                    error: Some("cancel confirmed by node".into()),
                                });
                            }
                            Err(error) => {
                                tracing::warn!(task = id, %error, "cancel not confirmed; retrying")
                            }
                        }
                    }
                }
            }
            next_heartbeat = Instant::now() + Duration::from_secs(2);
        }

        if running.len() < args.cpus as usize
            && let Ok(Response::Assignment(Some(assignment))) = client::tcp(&Request::Poll {
                node: node_name.clone(),
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
                    let _ = remove_scratch(&scratch);
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

fn node_capabilities(gpu_devices: &[String]) -> Vec<String> {
    let mut capabilities = Vec::new();
    let cgroup_controllers = (if task_user().is_some() {
        Ok(std::path::PathBuf::from("/sys/fs/cgroup"))
    } else {
        tri::resources::user_manager_cgroup()
    })
    .and_then(|path| Ok(std::fs::read_to_string(path.join("cgroup.controllers"))?))
    .ok();
    if cgroup_controllers.is_some() {
        capabilities.push("cgroup_v2".to_owned());
    }
    if let Some(controllers) = cgroup_controllers {
        for controller in ["cpuset", "cpu", "memory"] {
            if controllers
                .split_whitespace()
                .any(|item| item == controller)
            {
                capabilities.push(format!("cgroup:{controller}"));
            }
        }
    }
    let mut manager = std::process::Command::new("systemctl");
    if task_user().is_none() {
        manager.arg("--user");
    }
    if manager
        .args(["show-environment"])
        .output()
        .is_ok_and(|output| output.status.success())
    {
        capabilities.push(
            if task_user().is_some() {
                "systemd_system"
            } else {
                "systemd_user"
            }
            .to_owned(),
        );
    }
    if capabilities.iter().any(|item| item == "cgroup:cpuset")
        && capabilities.iter().any(|item| item == "cgroup:memory")
    {
        match check_resource_enforcement(gpu_devices) {
            Ok(()) => capabilities.push("resource_enforcement".to_owned()),
            Err(error) => tracing::warn!(%error, "resource enforcement probe failed"),
        }
    }
    for device in gpu_devices {
        if std::path::Path::new(device).exists() {
            capabilities.push(format!("gpu_device:{device}"));
        }
    }
    capabilities
}

/// No tri-task prefix: this diagnostic must not be reconciled as queue work.
fn check_resource_enforcement(gpu_devices: &[String]) -> Result<()> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let cpus = status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
        .context("cannot read current CPU affinity")?;
    let cpu = tri::resources::parse_set(cpus.trim())?
        .into_iter()
        .next()
        .context("no usable CPU")?;
    let limits = tri::resources::Limits {
        cpus: cpu.to_string(),
        memory_bytes: 64 * 1024 * 1024,
        gpu_devices: gpu_devices.to_vec(),
        assigned_gpus: String::new(),
        environment: vec![],
        stdout_path: None,
        stderr_path: None,
    };
    let unit = format!(
        "tri-resource-check-{}-{}",
        std::process::id(),
        tri::now_seconds()
    );
    let mut probe = std::process::Command::new("systemd-run");
    if let Some(user) = task_user() {
        probe.args(["--property", &format!("User={user}")]);
    } else {
        probe.arg("--user");
    }
    let output = probe
        .args([
            "--quiet",
            "--wait",
            "--collect",
            "--unit",
            &unit,
            "--property",
            &format!("AllowedCPUs={cpu}"),
            "--property",
            "MemoryMax=64M",
            "--property",
            "MemorySwapMax=0",
            "--property",
            "RuntimeMaxSec=15",
            "--property",
            "DevicePolicy=closed",
            "--property",
            "StandardOutput=journal",
            "--property",
            "StandardError=journal",
        ])
        .arg(std::env::current_exe()?)
        .arg("__run-task")
        .arg(serde_json::to_string(&limits)?)
        .arg("--")
        .arg("/usr/bin/true")
        .output()
        .context("starting resource enforcement diagnostic")?;
    // Passing pipe descriptors from a service into PID 1 can be rejected by
    // SELinux. Use journal output, as normal task units do, and retrieve it on
    // failure so the guard's diagnostic is still visible to --check callers.
    let journal = if !output.status.success() {
        std::process::Command::new("journalctl")
            .args([
                "-u",
                &format!("{unit}.service"),
                "-n",
                "30",
                "--no-pager",
                "-o",
                "cat",
            ])
            .output()
            .map(|result| String::from_utf8_lossy(&result.stdout).into_owned())
            .unwrap_or_default()
    } else {
        String::new()
    };
    anyhow::ensure!(
        output.status.success(),
        "resource enforcement probe failed ({}, unit {unit}.service): {}\n{}\nSee: journalctl -u {unit}.service -n 30 --no-pager",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim(),
        journal.trim()
    );
    Ok(())
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
    create_directory(&scratch)?;
    if let Some(parent) = std::path::Path::new(&task.stdout_path).parent() {
        create_directory(parent)?;
    }
    if task.stderr_path != task.stdout_path
        && let Some(parent) = std::path::Path::new(&task.stderr_path).parent()
    {
        create_directory(parent)?;
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
    if let Some(user) = task_user() {
        command.args(["--property", &format!("User={user}")]);
    } else {
        command.arg("--user");
    }
    command.args([
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
        &if task_user().is_some() {
            "StandardOutput=journal".to_owned()
        } else {
            format!("StandardOutput=append:{}", task.stdout_path)
        },
        "--property",
        &if task_user().is_some() {
            "StandardError=journal".to_owned()
        } else {
            format!("StandardError=append:{}", task.stderr_path)
        },
        "--property",
        "OOMPolicy=stop",
        "--property",
        "KillMode=control-group",
        "--property",
        "NoNewPrivileges=yes",
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
    // Apply the device allowlist even to CPU-only jobs.
    command.args(["--property", "DevicePolicy=closed"]);
    for device in standard_device_paths() {
        command.args(["--property", &format!("DeviceAllow={device} rwm")]);
    }
    if !gpus.is_empty() {
        for device in tri::resources::gpu_support_devices() {
            command.args(["--property", &format!("DeviceAllow={device} rwm")]);
        }
        for index in parse_resource_set(gpus) {
            let device = gpu_devices
                .get(index as usize)
                .with_context(|| format!("no device path configured for GPU {index}"))?;
            command.args(["--property", &format!("DeviceAllow={device} rwm")]);
        }
    }
    let limits = tri::resources::Limits {
        cpus: cpus.to_owned(),
        memory_bytes: task.spec.requested_mem_bytes,
        gpu_devices: gpu_devices.to_vec(),
        assigned_gpus: gpus.to_owned(),
        environment: task.spec.env.clone(),
        stdout_path: task_user().map(|_| task.stdout_path.clone()),
        stderr_path: task_user().map(|_| task.stderr_path.clone()),
    };
    anyhow::ensure!(
        tri::resources::parse_set(cpus)?.len() == task.spec.requested_cpu as usize,
        "CPU assignment does not match reservation"
    );
    anyhow::ensure!(
        tri::resources::parse_set(gpus)?.len() == task.spec.requested_gpus as usize,
        "GPU assignment does not match reservation"
    );
    // Reuse this executable so installation does not require another helper.
    command.arg(std::env::current_exe()?);
    command
        .arg("__run-task")
        .arg(serde_json::to_string(&limits)?)
        .arg("--");
    command.args(&task.spec.command);
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
        completion: None,
        enforcement_failure: None,
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
        let mut local = local_task_from_task(&task, unit.clone(), scratch_root)?;
        local.completion = poll_task(&unit, task.spec.timeout_seconds).await?;
        if local.completion.is_none()
            && let Err(error) = verify_adopted_unit(&unit, &task).await
        {
            local.enforcement_failure = Some(format!(
                "resource enforcement failed during reconciliation: {error:#}"
            ));
        }
        running.insert(task.id, local);
    }
    Ok(())
}

async fn verify_adopted_unit(unit: &str, task: &tri::model::Task) -> Result<()> {
    let output = systemctl_command()
        .args([
            "show",
            &format!("{unit}.service"),
            "-p",
            "ControlGroup",
            "-p",
            "ExecStart",
        ])
        .output()
        .await?;
    anyhow::ensure!(output.status.success(), "cannot inspect adopted task");
    let text = String::from_utf8(output.stdout)?;
    let fields: HashMap<_, _> = text
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect();
    let executable = std::env::current_exe()?;
    let prefix = format!("argv[]={}", executable.display());
    anyhow::ensure!(
        fields
            .get("ExecStart")
            .is_some_and(|value| value.contains(&format!("{prefix} __run-task "))),
        "legacy task has no resource verification guard; drain tasks before upgrading"
    );
    let path = tri::resources::cgroup_path(
        std::path::Path::new("/sys/fs/cgroup"),
        fields
            .get("ControlGroup")
            .context("task cgroup is missing")?,
    )?;
    tri::resources::verify_cgroup(
        &path,
        &tri::resources::Limits {
            cpus: task
                .assigned_cpus
                .clone()
                .context("task has no CPU assignment")?,
            memory_bytes: task.spec.requested_mem_bytes,
            gpu_devices: vec![],
            assigned_gpus: task.assigned_gpus.clone().unwrap_or_default(),
            environment: vec![],
            stdout_path: None,
            stderr_path: None,
        },
    )
}

async fn list_task_units() -> Result<(Vec<ObservedUnit>, Vec<String>)> {
    let output = systemctl_command()
        .args([
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
    create_directory(&scratch)?;
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
        completion: None,
        enforcement_failure: None,
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

async fn poll_task(unit: &str, timeout_seconds: Option<u64>) -> Result<Option<TaskCompletion>> {
    let unit = format!("{unit}.service");
    let output = systemctl_command()
        .args([
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
    if status == Some(tri::resources::ENFORCEMENT_EXIT_CODE) && result == "exit-code" {
        return Ok(Some(TaskCompletion {
            state: TaskState::Fail,
            exit_code: status,
            error: Some("resource enforcement verification failed; see task stderr".into()),
        }));
    }
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
    anyhow::ensure!(
        unit.starts_with("tri-task-")
            && unit.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'),
        "refusing to stop a non-task unit"
    );
    let unit = format!("{unit}.service");
    let status = systemctl_command()
        .args(["stop", &unit])
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
    let _ = systemctl_command()
        .args(["reset-failed", &format!("{unit}.service")])
        .status()
        .await;
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
