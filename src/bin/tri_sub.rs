use anyhow::{Context, Result, bail};
use clap::Parser;
use std::fs;
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use tri::client;
use tri::model::TaskSpec;
use tri::protocol::{Request, Response};
use tri::{command_to_string, parse_bytes, required_command};

#[derive(Debug, Parser)]
#[command(name = "tri_sub", about = "submit a tri task or job script")]
struct Args {
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    cpu: Option<u32>,
    #[arg(long, value_parser = parse_bytes, help = "memory reservation and limit (default: 1G)")]
    mem: Option<u64>,
    #[arg(long)]
    gpu: Option<u32>,
    #[arg(long, num_args = 0..=1, default_missing_value = "true", require_equals = true)]
    scratch_cwd: Option<bool>,
    #[arg(long)]
    node: Option<String>,
    #[arg(long, value_name = "ID")]
    external_id: Option<String>,
    #[arg(long, alias = "idempotency-key", value_name = "ID")]
    request_id: Option<String>,
    #[arg(long, value_name = "SECONDS", value_parser = parse_timeout)]
    timeout: Option<u64>,
    #[arg(long = "after", value_name = "ID")]
    after: Vec<i64>,
    /// Maximum executions, including explicit retries after a confirmed stop.
    #[arg(long, default_value_t = 1)]
    max_attempts: u32,
    #[arg(long)]
    json: bool,
    #[arg(
        long = "chdir",
        alias = "cwd",
        value_name = "DIR",
        help = "run the job from DIR (relative log paths use this directory)"
    )]
    chdir: Option<String>,
    #[arg(
        short = 'o',
        long,
        value_name = "PATH",
        help = "write standard output to PATH; %j is the task ID"
    )]
    output: Option<String>,
    #[arg(
        short = 'e',
        long,
        value_name = "PATH",
        help = "write standard error to PATH; %j is the task ID"
    )]
    error: Option<String>,
    #[arg(long, value_parser = parse_env)]
    env: Vec<(String, String)>,
    #[arg(required = true, trailing_var_arg = true)]
    command: Vec<String>,
}

fn parse_timeout(value: &str) -> Result<u64, String> {
    let timeout = value
        .parse::<u64>()
        .map_err(|_| "timeout must be a positive number of seconds".to_owned())?;
    if timeout == 0 {
        Err("timeout must be greater than zero".into())
    } else {
        Ok(timeout)
    }
}

fn parse_env(value: &str) -> Result<(String, String), String> {
    value
        .split_once('=')
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .ok_or_else(|| "environment values must use KEY=VALUE".into())
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => bail!("invalid boolean value {other:?}; use true or false"),
    }
}

#[derive(Default)]
struct ScriptMetadata {
    name: Option<String>,
    node: Option<String>,
    cpu: Option<u32>,
    mem: Option<u64>,
    gpu: Option<u32>,
    scratch_cwd: Option<bool>,
    output: Option<String>,
    error: Option<String>,
    timeout: Option<u64>,
}

fn parse_script_metadata(path: &Path) -> Result<ScriptMetadata> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("reading job script {}", path.display()))?;
    let mut metadata = ScriptMetadata::default();

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("#!") {
            continue;
        }
        if !trimmed.starts_with('#') {
            break;
        }

        let comment = trimmed.trim_start_matches('#').trim();
        let Some((key, value)) = comment.split_once(':').or_else(|| comment.split_once('=')) else {
            continue;
        };
        let key = key.trim().to_ascii_lowercase();
        let value = value.trim();
        match key.as_str() {
            "tri_name" => metadata.name = Some(value.to_owned()),
            "tri_node" => metadata.node = Some(value.to_owned()),
            "tri_cpu" => {
                let cpu = value
                    .parse::<u32>()
                    .with_context(|| format!("invalid tri_cpu value {value:?}"))?;
                if cpu == 0 {
                    bail!("tri_cpu must be greater than zero");
                }
                metadata.cpu = Some(cpu);
            }
            "tri_mem" => metadata.mem = Some(parse_bytes(value)?),
            "tri_gpu" | "tri_gpus" => {
                let gpu = value
                    .parse::<u32>()
                    .with_context(|| format!("invalid tri_gpu value {value:?}"))?;
                metadata.gpu = Some(gpu);
            }
            "tri_scratch" | "tri_scratch_cwd" => {
                metadata.scratch_cwd = Some(parse_bool(value)?);
            }
            "tri_output" | "tri_out" => metadata.output = Some(value.to_owned()),
            "tri_error" | "tri_err" => metadata.error = Some(value.to_owned()),
            "tri_timeout" => {
                metadata.timeout = Some(
                    parse_timeout(value)
                        .map_err(|error| anyhow::anyhow!("invalid tri_timeout: {error}"))?,
                )
            }
            _ => {}
        }
    }

    Ok(metadata)
}

fn script_path(command: &[String]) -> Result<Option<PathBuf>> {
    let Some(candidate) = command.first() else {
        return Ok(None);
    };
    let path = Path::new(candidate);
    if path.is_file() {
        // An explicit executable path is still a command. Do not read ELF
        // binaries as UTF-8 metadata or wrap them in bash.
        let mut file =
            fs::File::open(path).with_context(|| format!("opening command {}", path.display()))?;
        let mut magic = [0; 4];
        match file.read_exact(&mut magic) {
            Ok(()) if magic == *b"\x7fELF" => return Ok(None),
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {}
            Err(error) => return Err(error).context("reading command header"),
        }
        if path.is_absolute() {
            Ok(Some(path.to_path_buf()))
        } else {
            Ok(Some(std::env::current_dir()?.join(path)))
        }
    } else {
        Ok(None)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    required_command(&args.command)?;

    let script = script_path(&args.command)?;
    let (
        name,
        requested_cpu,
        requested_mem_bytes,
        requested_gpus,
        scratch_cwd,
        requested_node,
        cwd,
        output,
        error,
        timeout,
        command,
    ) = if let Some(script_path) = script {
        let metadata = parse_script_metadata(&script_path)?;
        let script_name = script_path
            .file_stem()
            .and_then(|name| name.to_str())
            .unwrap_or("script")
            .to_owned();
        let script_cwd = script_path
            .parent()
            .map(|path| path.to_string_lossy().into_owned());
        let mut command = vec![
            "bash".to_owned(),
            script_path.to_string_lossy().into_owned(),
        ];
        command.extend(args.command.iter().skip(1).cloned());
        (
            args.name.or(metadata.name).unwrap_or(script_name),
            args.cpu.or(metadata.cpu).unwrap_or(1),
            args.mem
                .or(metadata.mem)
                .unwrap_or(tri::resources::DEFAULT_MEMORY_BYTES),
            args.gpu.or(metadata.gpu).unwrap_or(0),
            args.scratch_cwd.or(metadata.scratch_cwd).unwrap_or(false),
            args.node.or(metadata.node),
            args.chdir.or(script_cwd),
            args.output.or(metadata.output),
            args.error.or(metadata.error),
            args.timeout.or(metadata.timeout),
            command,
        )
    } else {
        let submit_cwd = std::env::current_dir()
            .ok()
            .map(|path| path.to_string_lossy().into_owned());
        (
            args.name.unwrap_or_else(|| "task".to_owned()),
            args.cpu.unwrap_or(1),
            args.mem.unwrap_or(tri::resources::DEFAULT_MEMORY_BYTES),
            args.gpu.unwrap_or(0),
            args.scratch_cwd.unwrap_or(false),
            args.node,
            args.chdir.or(submit_cwd),
            args.output,
            args.error,
            args.timeout,
            args.command,
        )
    };

    eprintln!("submitting {}", command_to_string(&command));
    let response = match client::local(&Request::Submit {
        request_id: args.request_id,
        spec: TaskSpec {
            name,
            command,
            external_id: args.external_id,
            timeout_seconds: timeout,
            requested_cpu,
            requested_mem_bytes,
            requested_gpus,
            scratch_cwd,
            requested_node,
            cwd,
            env: args.env,
            output_path: output,
            error_path: error,
            depends_on: args.after,
            max_attempts: args.max_attempts,
        },
    })
    .await
    {
        Ok(response) => response,
        Err(error) if args.json => tri::output::json_error(&error),
        Err(error) => return Err(error),
    };
    match response {
        Response::Submitted { task, created } if args.json => {
            tri::output::submission_json(&task, created)?
        }
        Response::Submitted { task, .. } => println!("{}", task.id),
        other => anyhow::bail!("unexpected response: {other:?}"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executable_path_is_not_treated_as_a_job_script() {
        let executable = std::env::current_exe().unwrap();
        let command = vec![executable.to_string_lossy().into_owned(), "--help".into()];
        assert!(script_path(&command).unwrap().is_none());
        assert!(
            script_path(&["python3".into(), "-c".into(), "pass".into()])
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn shell_script_still_supplies_job_metadata() {
        let path = std::env::temp_dir().join(format!("tri-sub-script-{}.sh", std::process::id()));
        fs::write(
            &path,
            "#!/bin/bash\n# tri_cpu: 2\n# tri_mem: 64M\necho ok\n",
        )
        .unwrap();
        let detected = script_path(&[path.to_string_lossy().into_owned()])
            .unwrap()
            .unwrap();
        assert_eq!(detected, path);
        let metadata = parse_script_metadata(&detected).unwrap();
        assert_eq!(metadata.cpu, Some(2));
        assert_eq!(metadata.mem, Some(64 << 20));
        fs::remove_file(path).unwrap();
    }
}
