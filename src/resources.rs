//! Verify kernel resource limits before a task's command is executed.
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::os::unix::fs::{FileTypeExt, OpenOptionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Component, Path, PathBuf};

pub const DEFAULT_MEMORY_BYTES: u64 = 1024 * 1024 * 1024;
pub const ENFORCEMENT_EXIT_CODE: i32 = 78;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Limits {
    pub cpus: String,
    pub memory_bytes: u64,
    pub gpu_devices: Vec<String>,
    pub assigned_gpus: String,
    #[serde(default)]
    pub environment: Vec<(String, String)>,
    #[serde(default)]
    pub stdout_path: Option<String>,
    #[serde(default)]
    pub stderr_path: Option<String>,
}

pub fn parse_set(value: &str) -> Result<BTreeSet<u32>> {
    let mut result = BTreeSet::new();
    if value.trim().is_empty() {
        return Ok(result);
    }
    for part in value.trim().split(',') {
        let (start, end) = match part.split_once('-') {
            Some((start, end)) => (start.parse::<u32>()?, end.parse::<u32>()?),
            None => {
                let n = part.parse::<u32>()?;
                (n, n)
            }
        };
        ensure!(
            start <= end && end < 1_048_576,
            "invalid resource range {part}"
        );
        result.extend(start..=end);
    }
    Ok(result)
}

pub fn cgroup_path(root: &Path, relative: &str) -> Result<PathBuf> {
    let relative = Path::new(relative.trim_start_matches('/'));
    ensure!(
        relative
            .components()
            .all(|p| matches!(p, Component::Normal(_))),
        "invalid cgroup path {}",
        relative.display()
    );
    Ok(root.join(relative))
}

pub fn current_cgroup() -> Result<PathBuf> {
    let membership = fs::read_to_string("/proc/self/cgroup")?;
    let path = membership
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .context("cgroup v2 membership is unavailable")?;
    cgroup_path(Path::new("/sys/fs/cgroup"), path)
}

/// Use the user manager's delegated subtree, not the host's root controllers.
pub fn user_manager_cgroup() -> Result<PathBuf> {
    let output = std::process::Command::new("systemctl")
        .args(["--user", "show", "--property=ControlGroup", "--value"])
        .output()
        .context("querying systemd user manager")?;
    ensure!(
        output.status.success(),
        "systemd user manager is unavailable"
    );
    let relative = String::from_utf8(output.stdout)?;
    ensure!(
        !relative.trim().is_empty() && relative.trim() != "/",
        "user manager returned no delegated cgroup"
    );
    cgroup_path(Path::new("/sys/fs/cgroup"), relative.trim())
}

pub fn verify_cgroup(path: &Path, limits: &Limits) -> Result<()> {
    let requested = parse_set(&limits.cpus)?;
    ensure!(!requested.is_empty(), "empty CPU assignment");
    let effective = fs::read_to_string(path.join("cpuset.cpus.effective"))
        .context("cpuset is not delegated to the task; configure the systemd user manager")?;
    ensure!(
        parse_set(effective.trim())? == requested,
        "CPU limit mismatch: assigned {}, effective {}",
        limits.cpus,
        effective.trim()
    );
    ensure!(
        limits.memory_bytes > 0,
        "task has no memory reservation; resubmit with --mem"
    );
    let memory = fs::read_to_string(path.join("memory.max"))
        .context("task memory controller is unavailable")?;
    ensure!(
        memory
            .trim()
            .parse::<u64>()
            .ok()
            .is_some_and(|n| n > 0 && n <= limits.memory_bytes),
        "memory limit mismatch: requested {}, effective {}",
        limits.memory_bytes,
        memory.trim()
    );
    let swap = fs::read_to_string(path.join("memory.swap.max"))
        .context("task swap controller is unavailable")?;
    ensure!(
        swap.trim() == "0",
        "task swap is not disabled: {}",
        swap.trim()
    );
    Ok(())
}

pub fn gpu_support_devices() -> Vec<String> {
    let mut paths: Vec<String> = [
        "/dev/nvidiactl",
        "/dev/nvidia-uvm",
        "/dev/nvidia-uvm-tools",
        "/dev/nvidia-modeset",
    ]
    .into_iter()
    .filter(|p| Path::new(p).exists())
    .map(str::to_owned)
    .collect();
    if let Ok(entries) = fs::read_dir("/dev/nvidia-caps") {
        paths.extend(
            entries
                .flatten()
                .map(|e| e.path().to_string_lossy().into_owned()),
        );
    }
    paths
}

fn gpu_endpoints(configured: &[String]) -> BTreeSet<PathBuf> {
    let mut paths: BTreeSet<PathBuf> = configured.iter().map(PathBuf::from).collect();
    for dir in ["/dev", "/dev/dri", "/dev/nvidia-caps"] {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                if dir != "/dev" || name.to_string_lossy().starts_with("nvidia") {
                    paths.insert(entry.path());
                }
            }
        }
    }
    paths
        .into_iter()
        .filter(|p| fs::metadata(p).is_ok_and(|m| m.file_type().is_char_device()))
        .collect()
}

/// Checking DevicePolicy's configured value is insufficient: user managers may
/// be unable to attach the device BPF program. Check real device opens instead.
pub fn verify_devices(limits: &Limits) -> Result<()> {
    let mut allowed = BTreeSet::new();
    for index in parse_set(&limits.assigned_gpus)? {
        let path = limits
            .gpu_devices
            .get(index as usize)
            .context("GPU assignment exceeds configured devices")?;
        OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY)
            .open(path)
            .with_context(|| format!("assigned GPU {path} is inaccessible"))?;
        allowed.insert(PathBuf::from(path));
    }
    if !allowed.is_empty() {
        allowed.extend(gpu_support_devices().iter().map(PathBuf::from));
    }
    let endpoints = gpu_endpoints(&limits.gpu_devices);
    let configured: BTreeSet<PathBuf> = limits.gpu_devices.iter().map(PathBuf::from).collect();
    for path in configured
        .intersection(&endpoints)
        .chain(endpoints.difference(&configured))
    {
        if !allowed.contains(path) {
            verify_denied(path)?;
        }
    }
    Ok(())
}

fn verify_denied(path: &Path) -> Result<()> {
    // Test both modes: denying writes alone does not prevent device access.
    for write in [false, true] {
        match OpenOptions::new()
            .read(true)
            .write(write)
            .custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY)
            .open(path)
        {
            Err(error) if matches!(error.raw_os_error(), Some(libc::EPERM | libc::EACCES)) => {}
            Err(error) => bail!(
                "cannot verify device isolation for {}: {error}",
                path.display()
            ),
            Ok(_) => bail!(
                "unallocated GPU device {} is accessible; systemd device enforcement is unavailable",
                path.display()
            ),
        }
    }
    Ok(())
}

pub fn verify_current(limits: &Limits) -> Result<()> {
    verify_cgroup(&current_cgroup()?, limits)?;
    verify_devices(limits)
}

fn redirect_logs(limits: &Limits) -> Result<()> {
    use std::os::fd::AsRawFd;
    for (path, fd) in [
        (&limits.stdout_path, libc::STDOUT_FILENO),
        (&limits.stderr_path, libc::STDERR_FILENO),
    ] {
        if let Some(path) = path {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .with_context(|| format!("task account cannot open log {path}"))?;
            ensure!(
                unsafe { libc::dup2(file.as_raw_fd(), fd) } >= 0,
                "redirecting task log failed"
            );
        }
    }
    Ok(())
}

/// Runs inside the task unit. No user code executes until all checks succeed.
pub fn exec_guarded(limits_json: &str, command: &[String]) -> ! {
    let verified = (|| -> Result<Limits> {
        let mut limits: Limits = serde_json::from_str(limits_json)?;
        ensure!(!command.is_empty(), "empty task command");
        redirect_logs(&limits)?;
        verify_current(&limits)?;
        // The scheduler uses ranges (0-1); CUDA expects individual IDs (0,1).
        limits.assigned_gpus = parse_set(&limits.assigned_gpus)?
            .into_iter()
            .map(|index| index.to_string())
            .collect::<Vec<_>>()
            .join(",");
        Ok(limits)
    })();
    let limits = match verified {
        Ok(limits) => limits,
        Err(error) => {
            eprintln!("TRI resource enforcement failed: {error:#}");
            std::process::exit(ENFORCEMENT_EXIT_CODE);
        }
    };
    let error = std::process::Command::new(&command[0])
        .args(&command[1..])
        .envs(limits.environment)
        .env("CUDA_VISIBLE_DEVICES", &limits.assigned_gpus)
        .env(
            "NVIDIA_VISIBLE_DEVICES",
            if limits.assigned_gpus.is_empty() {
                "void"
            } else {
                &limits.assigned_gpus
            },
        )
        .exec();
    eprintln!("TRI could not execute task: {error}");
    std::process::exit(126)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn limits() -> Limits {
        Limits {
            cpus: "0-1".into(),
            memory_bytes: 1024,
            gpu_devices: vec![],
            assigned_gpus: String::new(),
            environment: vec![],
            stdout_path: None,
            stderr_path: None,
        }
    }
    #[test]
    fn rejects_missing_or_ineffective_limits() {
        let root = std::env::temp_dir().join(format!("tri-resources-{}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        assert!(verify_cgroup(&root, &limits()).is_err());
        fs::write(root.join("cpuset.cpus.effective"), "0-23").unwrap();
        fs::write(root.join("memory.max"), "1024").unwrap();
        fs::write(root.join("memory.swap.max"), "0").unwrap();
        assert!(verify_cgroup(&root, &limits()).is_err());
        fs::write(root.join("cpuset.cpus.effective"), "0-1").unwrap();
        verify_cgroup(&root, &limits()).unwrap();
        for value in ["max", "2048", "0"] {
            fs::write(root.join("memory.max"), value).unwrap();
            assert!(verify_cgroup(&root, &limits()).is_err());
        }
        fs::write(root.join("memory.max"), "1024").unwrap();
        fs::write(root.join("memory.swap.max"), "max").unwrap();
        assert!(verify_cgroup(&root, &limits()).is_err());
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn accessible_unallocated_device_fails_verification() {
        assert!(verify_denied(Path::new("/dev/null")).is_err());
    }
    #[test]
    fn validates_cpu_sets_and_cgroup_paths() {
        assert_eq!(parse_set("0-1,3").unwrap(), BTreeSet::from([0, 1, 3]));
        for value in ["2-1", "a", "0-999999999", "1,,2"] {
            assert!(parse_set(value).is_err());
        }
        assert!(cgroup_path(Path::new("/sys/fs/cgroup"), "/../../tmp").is_err());
    }
}
