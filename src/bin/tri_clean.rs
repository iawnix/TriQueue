use anyhow::Result;
use clap::Parser;
use tri::client;
use tri::protocol::{Request, Response};

#[derive(Debug, Parser)]
#[command(name = "tri_clean", about = "remove completed task records")]
struct Args {
    #[arg(long)]
    json: bool,
    /// Remove terminal tasks older than this many seconds (default: 30 days).
    #[arg(long, conflicts_with = "all")]
    older_than: Option<u64>,
    /// Remove all terminal tasks, including recent ones.
    #[arg(long)]
    all: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let older_than_seconds = if args.all {
        None
    } else {
        Some(args.older_than.unwrap_or(30 * 24 * 60 * 60))
    };
    let response = match client::local(&Request::Clean { older_than_seconds }).await {
        Ok(response) => response,
        Err(error) if args.json => tri::output::json_error(&error),
        Err(error) => return Err(error),
    };
    match response {
        Response::Cleaned { removed } if args.json => {
            println!("{}", serde_json::json!({"removed": removed}))
        }
        Response::Cleaned { removed } => println!("removed {removed} tasks"),
        Response::Message(message) => println!("{message}"),
        other => anyhow::bail!("unexpected response: {other:?}"),
    }
    Ok(())
}
