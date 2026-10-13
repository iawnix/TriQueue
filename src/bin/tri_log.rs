use anyhow::Result;
use clap::Parser;
use std::io::{Read, Seek, SeekFrom};
use tokio::time::{Duration, sleep};
use tri::client;
use tri::protocol::{Request, Response};

#[derive(Debug, Parser)]
#[command(name = "tri_log", about = "show a task's output")]
struct Args {
    id: i64,
    #[arg(long, default_value_t = false)]
    stderr: bool,
    #[arg(long, default_value_t = false)]
    follow: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let task = match client::local(&Request::Get { id: args.id }).await? {
        Response::Task(task) => task,
        other => anyhow::bail!("unexpected response: {other:?}"),
    };
    let path = if args.stderr {
        task.stderr_path
    } else {
        task.stdout_path
    };
    let mut offset = 0u64;
    loop {
        let mut file = std::fs::File::open(&path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        offset += buf.len() as u64;
        print!("{buf}");
        if !args.follow
            || matches!(
                task.state,
                tri::model::TaskState::Done
                    | tri::model::TaskState::Fail
                    | tri::model::TaskState::Cancel
                    | tri::model::TaskState::Lost
            )
        {
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }
    Ok(())
}
