use anyhow::Result;
use clap::Parser;
use std::io::{Read, Seek, SeekFrom, Write};
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
    let initial = match client::local(&Request::Get { id: args.id }).await? {
        Response::Task(task) => task,
        other => anyhow::bail!("unexpected response: {other:?}"),
    };
    let path = if args.stderr {
        initial.stderr_path
    } else {
        initial.stdout_path
    };
    let mut offset = 0u64;
    loop {
        match std::fs::File::open(&path) {
            Ok(mut file) => {
                file.seek(SeekFrom::Start(offset))?;
                let mut buf = String::new();
                file.read_to_string(&mut buf)?;
                offset += buf.len() as u64;
                print!("{buf}");
                std::io::stdout().flush()?;
            }
            Err(error) if args.follow && error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        if !args.follow {
            break;
        }
        let task = match client::local(&Request::Get { id: args.id }).await? {
            Response::Task(task) => task,
            other => anyhow::bail!("unexpected response: {other:?}"),
        };
        if matches!(
            task.state,
            tri::model::TaskState::Done
                | tri::model::TaskState::Fail
                | tri::model::TaskState::Cancel
                | tri::model::TaskState::Lost
                | tri::model::TaskState::Blocked
        ) {
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }
    Ok(())
}
