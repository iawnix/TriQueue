use anyhow::Result;
use clap::Parser;
use tri::client;
use tri::output;
use tri::protocol::{Request, Response};

#[derive(Debug, Parser)]
#[command(name = "tri_del", about = "cancel a tri task")]
struct Args {
    id: i64,
    #[arg(long)]
    json: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let response = match client::local(&Request::Cancel { id: args.id }).await {
        Ok(response) => response,
        Err(error) if args.json => output::json_error(&error),
        Err(error) => return Err(error),
    };
    match response {
        Response::Task(task) if args.json => output::task_json(&task)?,
        Response::Task(task) => output::task(&task),
        other => anyhow::bail!("unexpected response: {other:?}"),
    }
    Ok(())
}
