use anyhow::Result;
use clap::Parser;
use tri::client;
use tri::output;
use tri::protocol::{Request, Response};

#[derive(Debug, Parser)]
#[command(name = "tri_stat", about = "show queue status or one tri task")]
struct Args {
    /// Show details for one task. Omit it to show the queue.
    id: Option<i64>,
    #[arg(long, help = "limit the queue view to one state")]
    state: Option<String>,
    #[arg(long)]
    json: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let request = match args.id {
        Some(id) => Request::Get { id },
        None => Request::List { state: args.state },
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
