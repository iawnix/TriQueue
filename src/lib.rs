pub mod client;
pub mod model;
pub mod output;
pub mod protocol;
pub mod server;
pub mod store;
pub mod tui;

use anyhow::{Context, Result};
use std::path::PathBuf;

pub fn socket_path() -> PathBuf {
    std::env::var_os("TRI_SOCKET")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("XDG_RUNTIME_DIR")
                .map(PathBuf::from)
                .map(|runtime| runtime.join("tri/server.sock"))
        })
        .unwrap_or_else(|| PathBuf::from("/tmp/tri.sock"))
}

pub fn tcp_addr() -> String {
    std::env::var("TRI_SERVER_ADDR").unwrap_or_else(|_| "127.0.0.1:7400".into())
}

pub fn data_dir() -> PathBuf {
    std::env::var_os("TRI_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./data"))
}

pub fn db_path() -> PathBuf {
    std::env::var_os("TRI_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|| data_dir().join("tri.db"))
}

pub fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "tri=info".to_string()))
        .try_init();
}

pub fn required_command(command: &[String]) -> Result<()> {
    anyhow::ensure!(!command.is_empty(), "a command is required after --");
    Ok(())
}

pub fn command_to_string(command: &[String]) -> String {
    command
        .iter()
        .map(|s| shell_escape(s))
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_escape(value: &str) -> String {
    if value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"-._/:=@+".contains(&b))
    {
        value.to_owned()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

pub fn ensure_parent(path: &std::path::Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    Ok(())
}

pub fn now_string() -> String {
    now_seconds().to_string()
}

pub fn now_seconds() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

/// Format elapsed task runtime from Unix-second timestamps.
pub fn format_elapsed(start: Option<&str>, end: Option<&str>) -> String {
    let Some(start) = start.and_then(|value| value.parse::<u64>().ok()) else {
        return "--".into();
    };
    let end = end
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_else(|| {
            use std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_secs())
                .unwrap_or(start)
        });
    let seconds = end.saturating_sub(start);
    let days = seconds / 86_400;
    let hours = (seconds % 86_400) / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let seconds = seconds % 60;
    if days > 0 {
        format!("{days}-{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    }
}

pub fn parse_bytes(value: &str) -> Result<u64> {
    let value = value.trim();
    let split_at = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    let (number, suffix) = value.split_at(split_at);
    let number: u64 = number.parse().context("invalid byte count")?;
    let multiplier = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        "t" | "tb" | "tib" => 1024 * 1024 * 1024 * 1024,
        other => anyhow::bail!("unknown size suffix: {other}"),
    };
    Ok(number.saturating_mul(multiplier))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_human_sizes() {
        assert_eq!(parse_bytes("8G").unwrap(), 8 * 1024 * 1024 * 1024);
        assert_eq!(parse_bytes("512M").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_bytes("42").unwrap(), 42);
    }

    #[test]
    fn quotes_shell_arguments_when_needed() {
        assert_eq!(
            command_to_string(&["echo".into(), "hello world".into()]),
            "echo 'hello world'"
        );
        assert_eq!(
            command_to_string(&["python".into(), "script.py".into()]),
            "python script.py"
        );
    }

    #[test]
    fn formats_elapsed_runtime() {
        assert_eq!(format_elapsed(Some("100"), Some("3723")), "01:00:23");
        assert_eq!(format_elapsed(Some("0"), Some("90061")), "1-01:01:01");
        assert_eq!(format_elapsed(None, None), "--");
    }
}
