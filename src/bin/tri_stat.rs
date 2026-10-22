use anyhow::{Result, bail};
use clap::Parser;
use tri::client;
use tri::output;
use tri::protocol::{Request, Response};

#[derive(Debug, Parser)]
#[command(name = "tri_stat", about = "show queue status or one tri task")]
struct Args {
    /// Show details for one task. Omit it to show the queue.
    id: Option<i64>,
    #[arg(long, conflicts_with = "id")]
    external_id: Option<String>,
    #[arg(long, help = "limit the queue view to one state")]
    state: Option<String>,
    /// Show durable execution attempts (use a numeric task ID).
    #[arg(long, requires = "id", conflicts_with_all = ["state", "confirm_stopped"])]
    history: bool,
    /// Show the task's event timeline.
    #[arg(long, requires = "id", conflicts_with_all = ["state", "confirm_stopped"])]
    events: bool,
    /// Assert that this unresolved execution has stopped and cannot start late.
    #[arg(long, requires_all = ["id", "attempt", "reason"], conflicts_with = "state")]
    confirm_stopped: bool,
    #[arg(long, requires = "confirm_stopped")]
    attempt: Option<u32>,
    /// Record how the old execution was verified stopped.
    #[arg(long, requires = "confirm_stopped")]
    reason: Option<String>,
    /// Requeue after confirmed stop, within the task's max_attempts limit.
    #[arg(long, requires = "confirm_stopped")]
    retry: bool,
    #[arg(long)]
    json: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let request = if args.confirm_stopped {
        Request::ConfirmStopped {
            id: args.id.unwrap(),
            attempt: args.attempt.unwrap(),
            reason: args.reason.unwrap(),
            retry: args.retry,
        }
    } else if args.history || args.events {
        Request::History {
            id: args.id.unwrap(),
        }
    } else {
        match (args.id, args.external_id) {
            (Some(id), None) => Request::Get { id },
            (None, Some(external_id)) => Request::GetExternal { external_id },
            (None, None) => Request::List { state: args.state },
            (Some(_), Some(_)) => bail!("provide either an id or --external-id, not both"),
        }
    };
    let response = match client::local(&request).await {
        Ok(response) => response,
        Err(error) if args.json => output::json_error(&error),
        Err(error) => return Err(error),
    };
    match response {
        Response::History(history) if args.json => {
            println!("{}", serde_json::to_string_pretty(&history)?)
        }
        Response::History(history) => {
            if args.history {
                for attempt in &history.attempts {
                    println!(
                        "Attempt {}  {}  node={}  cpus={}  gpus={}  start={}  end={}  exit={:?}",
                        attempt.attempt,
                        attempt.state,
                        attempt.node.as_deref().unwrap_or("--"),
                        attempt.assigned_cpus.as_deref().unwrap_or("--"),
                        attempt.assigned_gpus.as_deref().unwrap_or("--"),
                        attempt.started_at.as_deref().unwrap_or("--"),
                        attempt.finished_at.as_deref().unwrap_or("--"),
                        attempt.exit_code
                    );
                    if let Some(reason) = &attempt.error {
                        println!("  {reason}");
                    }
                }
                if history.attempts.is_empty() {
                    println!("No execution attempts recorded.");
                }
            }
            if args.events {
                for event in &history.events {
                    println!(
                        "{}  attempt={}  {}  {} -> {}  {}",
                        event.at,
                        event.attempt,
                        event.kind,
                        event.from_state.as_deref().unwrap_or("--"),
                        event.to_state,
                        event.detail.as_deref().unwrap_or("")
                    );
                }
            }
        }
        Response::Task(task) if args.json => output::task_json(&task)?,
        Response::Tasks(tasks) if args.json => output::tasks_json(&tasks)?,
        Response::Task(task) => output::task(&task),
        Response::Tasks(tasks) => output::queue(&tasks),
        other => anyhow::bail!("unexpected response: {other:?}"),
    }
    Ok(())
}
