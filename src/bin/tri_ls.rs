use anyhow::Result;
use clap::Parser;
use tri::client;
use tri::output;
use tri::protocol::{Request, Response};

#[derive(Debug, Parser)]
#[command(name = "tri_ls", about = "list tri tasks")]
struct Args {
    #[arg(long)]
    state: Option<String>,
    #[arg(long)]
    json: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let response = match client::local(&Request::List { state: args.state }).await {
        Ok(response) => response,
        Err(error) if args.json => output::json_error(&error),
        Err(error) => return Err(error),
    };
    match response {
        Response::Tasks(tasks) if args.json => output::tasks_json(&tasks)?,
        Response::Tasks(tasks) => output::tasks(&tasks),
        other => anyhow::bail!("unexpected response: {other:?}"),
    }
    Ok(())
}
