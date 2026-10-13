use crate::protocol::{Request, Response};
use crate::{socket_path, tcp_addr};
use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};

pub async fn local(request: &Request) -> Result<Response> {
    let socket = socket_path();
    if socket.exists() {
        let stream = UnixStream::connect(&socket)
            .await
            .with_context(|| format!("connecting to {}", socket.display()))?;
        exchange(stream, request).await
    } else {
        tcp(request).await
    }
}

pub async fn tcp(request: &Request) -> Result<Response> {
    let addr = tcp_addr();
    let stream = TcpStream::connect(&addr)
        .await
        .with_context(|| format!("connecting to {addr}"))?;
    exchange(stream, request).await
}

pub async fn exchange<S>(mut stream: S, request: &Request) -> Result<Response>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let message = serde_json::to_string(request)? + "\n";
    stream.write_all(message.as_bytes()).await?;
    stream.flush().await?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    anyhow::ensure!(
        !line.is_empty(),
        "server closed the connection without a response"
    );
    let response: Response = serde_json::from_str(line.trim())?;
    if let Response::Error(message) = &response {
        anyhow::bail!("{message}");
    }
    Ok(response)
}
