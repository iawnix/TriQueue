use crate::model::{NodeInfo, Task, TaskState};
use owo_colors::OwoColorize;

pub fn tasks(tasks: &[Task]) {
    if tasks.is_empty() {
        println!("No tasks.");
        return;
    }
    println!(
        "{:<5} {:<8} {:<7} {:>4}  {:>4}  {:>8}  {:>11}  NAME",
        "ID", "STATE", "NODE", "CPU", "GPU", "MEM", "TIME"
    );
    println!(
        "───── ──────── ─────── ────  ────  ────────  ───────────  ──────────────────────────────"
    );
    for task in tasks {
        let state = colored_state(&task.state);
        let state_pad = " ".repeat(8usize.saturating_sub(task.state.as_str().len()));
        println!(
            "{:<5} {}{} {:<7} {:>4}  {:>4}  {:>8}  {:>11}  {}",
            task.id,
            state,
            state_pad,
            task.node.as_deref().unwrap_or("--"),
            task.spec.requested_cpu,
            task.spec.requested_gpus,
            format_bytes(task.spec.requested_mem_bytes),
            crate::format_elapsed(task.started_at.as_deref(), task.finished_at.as_deref()),
            task.spec.name
        );
    }
}

pub fn task(task: &Task) {
    println!("{}", format!("Task {}", task.id).bold());
    println!("  state:   {}", colored_state(&task.state));
    if task.state == TaskState::Cancel {
        println!("  cancel confirmed: {}", task.cancel_confirmed);
    }
    if let Some(reason) = &task.completion_reason {
        println!("  completion reason: {reason}");
    }
    println!("  attempt: {}", task.attempt);
    if task.recovery_pending {
        println!("  recovery: execution uncertain; resources retained, node quarantined");
    }
    println!("  name:    {}", task.spec.name);
    println!("  node:    {}", task.node.as_deref().unwrap_or("--"));
    println!(
        "  cpu:     {}",
        task.assigned_cpus.as_deref().unwrap_or("--")
    );
    println!(
        "  gpu:     {}",
        task.assigned_gpus.as_deref().unwrap_or("--")
    );
    println!("  memory:  {}", format_bytes(task.spec.requested_mem_bytes));
    println!("  gpus:    {}", task.spec.requested_gpus);
    if let Some(timeout) = task.spec.timeout_seconds {
        println!("  timeout: {timeout}s");
    }
    if let Some(external_id) = &task.external_id {
        println!("  external: {external_id}");
    }
    println!(
        "  command: {}",
        crate::command_to_string(&task.spec.command)
    );
    println!("  stdout:  {}", task.stdout_path);
    println!("  stderr:  {}", task.stderr_path);
    if let Some(error) = &task.error {
        println!("  error:   {}", error.red());
    }
}

pub fn nodes(nodes: &[NodeInfo]) {
    if nodes.is_empty() {
        println!("No nodes.");
        return;
    }
    println!(
        "{:<8} {:<7} {:>9}  {:>9}  {:>15}  {:>5}  ADDRESS",
        "NODE", "STATE", "CPU", "GPU", "MEM", "TASKS"
    );
    println!("──────── ─────── ─────────  ─────────  ───────────────  ─────  ────────────────");
    let mut sorted = nodes.to_vec();
    sorted.sort_by(|left, right| left.name.cmp(&right.name));
    for node in sorted {
        let label = if node.quarantined {
            "hold"
        } else if node.online {
            "up"
        } else {
            "down"
        };
        let state = if node.quarantined {
            label.yellow().bold().to_string()
        } else if node.online {
            label.green().to_string()
        } else {
            label.red().bold().to_string()
        };
        let state_pad = " ".repeat(7usize.saturating_sub(label.len()));
        println!(
            "{:<8} {}{} {:>3}/{:<5}  {:>2}/{:<5}  {:>7}/{:<7}  {:>5}  {}",
            node.name,
            state,
            state_pad,
            node.cpu_used,
            node.cpu_total,
            node.gpu_used,
            node.gpu_total,
            format_bytes(node.memory_used),
            format_bytes(node.memory_bytes),
            node.running_tasks.len(),
            node.address
        );
    }
}

/// Show the scheduler queue and a compact count of every task state.
pub fn queue(items: &[Task]) {
    let mut counts = [0usize; 9];
    for task in items {
        let index = match &task.state {
            TaskState::Unknown => 8,
            TaskState::Wait => 0,
            TaskState::Run => 1,
            TaskState::Done => 2,
            TaskState::Fail => 3,
            TaskState::Cancel => 4,
            TaskState::Lost => 5,
            TaskState::Blocked => 6,
            TaskState::TimedOut => 7,
        };
        counts[index] += 1;
    }
    println!(
        "Queue  {} total   {} wait   {} run   {} done   {} fail   {} cancel   {} lost   {} blocked   {} timed_out   {} unknown",
        items.len(),
        counts[0],
        counts[1],
        counts[2],
        counts[3],
        counts[4],
        counts[5],
        counts[6],
        counts[7],
        counts[8]
    );
    let active: Vec<_> = items
        .iter()
        .filter(|task| task.state == TaskState::Wait || task.reserves_resources())
        .cloned()
        .collect();
    if active.is_empty() {
        println!("Queue is empty.");
    } else {
        tasks(&active);
    }
}

pub fn colored_state(state: &TaskState) -> String {
    match state {
        TaskState::Unknown => "unknown".magenta().bold().to_string(),
        TaskState::Wait => "wait".yellow().to_string(),
        TaskState::Run => "run".cyan().bold().to_string(),
        TaskState::Done => "done".green().to_string(),
        TaskState::Fail => "fail".red().bold().to_string(),
        TaskState::Cancel => "cancel".dimmed().to_string(),
        TaskState::Lost => "lost".magenta().bold().to_string(),
        TaskState::Blocked => "blocked".yellow().bold().to_string(),
        TaskState::TimedOut => "timed_out".red().bold().to_string(),
    }
}

pub fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1}G", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.1}M", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1}K", bytes as f64 / 1024.0)
    } else {
        format!("{bytes}B")
    }
}

pub fn task_json(task: &Task) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(task)?);
    Ok(())
}

pub fn tasks_json(tasks: &[Task]) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(tasks)?);
    Ok(())
}

pub fn submission_json(task: &Task, created: bool) -> anyhow::Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "task": task,
            "created": created,
            "deduplicated": !created,
        }))?
    );
    Ok(())
}

pub fn nodes_json(nodes: &[NodeInfo]) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(nodes)?);
    Ok(())
}

pub fn json_error(error: &anyhow::Error) -> ! {
    let message = error.to_string();
    let (code, retryable) = if message.starts_with("request_id_reused:") {
        ("request_id_reused", false)
    } else if message.starts_with("external_id_conflict:") {
        ("external_id_conflict", false)
    } else if message.contains("must be greater than zero") || message.contains("must not be empty")
    {
        ("invalid_request", false)
    } else {
        ("request_failed", true)
    };
    println!(
        "{}",
        serde_json::json!({
            "error": {
                "code": code,
                "message": message,
                "retryable": retryable
            }
        })
    );
    std::process::exit(1)
}
