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
    match client::local(&Request::List { state: args.state }).await? {
        Response::Tasks(tasks) if args.json => {
            println!("{}", serde_json::to_string_pretty(&tasks)?)
        }
        Response::Tasks(tasks) => output::tasks(&tasks),
        other => anyhow::bail!("unexpected response: {other:?}"),
    }
    Ok(())
}
