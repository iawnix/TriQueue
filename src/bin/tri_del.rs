use anyhow::Result;
use clap::Parser;
use tri::client;
use tri::output;
use tri::protocol::{Request, Response};

#[derive(Debug, Parser)]
#[command(name = "tri_del", about = "cancel a tri task")]
struct Args {
    id: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    match client::local(&Request::Cancel { id: args.id }).await? {
        Response::Task(task) => output::task(&task),
        other => anyhow::bail!("unexpected response: {other:?}"),
    }
    Ok(())
}
