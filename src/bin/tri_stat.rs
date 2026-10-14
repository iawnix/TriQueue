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
    #[arg(long)]
    json: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let request = match (args.id, args.external_id) {
        (Some(id), None) => Request::Get { id },
        (None, Some(external_id)) => Request::GetExternal { external_id },
        (None, None) => Request::List { state: args.state },
        (Some(_), Some(_)) => bail!("provide either an id or --external-id, not both"),
    };
    let response = match client::local(&request).await {
        Ok(response) => response,
        Err(error) if args.json => output::json_error(&error),
        Err(error) => return Err(error),
    };
    match response {
        Response::Task(task) if args.json => output::task_json(&task)?,
        Response::Tasks(tasks) if args.json => output::tasks_json(&tasks)?,
        Response::Task(task) => output::task(&task),
        Response::Tasks(tasks) => output::queue(&tasks),
        other => anyhow::bail!("unexpected response: {other:?}"),
    }
    Ok(())
}
