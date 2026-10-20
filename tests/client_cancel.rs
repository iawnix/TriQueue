//! Exercise the real compute client against an isolated in-process server.
//! Fake systemd commands make stop failure and unit disappearance deterministic.
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::process::{Child, Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use tri::{
    model::*,
    protocol::{Request, Response},
    server::Server,
    store::Store,
};

struct Harness {
    child: Child,
    stop: Arc<AtomicBool>,
    listener: Option<std::thread::JoinHandle<()>>,
    root: std::path::PathBuf,
}
impl Drop for Harness {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.listener.take() {
            let _ = thread.join();
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
fn eventually(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "client did not reach expected state"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}
fn submit(server: &Server, name: &str) -> Task {
    server
        .store
        .submit(TaskSpec {
            name: name.into(),
            command: vec!["sleep".into(), "100".into()],
            external_id: None,
            timeout_seconds: None,
            requested_cpu: 1,
            requested_mem_bytes: 1024,
            requested_gpus: 0,
            scratch_cwd: false,
            requested_node: Some("fixture".into()),
            cwd: None,
            env: vec![],
            output_path: None,
            error_path: None,
            depends_on: vec![],
            max_attempts: 1,
        })
        .unwrap()
}

#[test]
fn client_retries_stop_and_completion_before_next_job_uses_allocation() {
    let root = std::env::temp_dir().join(format!("tri-client-cancel-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    for (name, script) in [
        (
            "systemd-run",
            r#"#!/bin/sh
while [ "$#" -gt 0 ]; do
    if [ "$1" = '--unit' ]; then shift; touch "$TRI_FIXTURE/$1.started"; exit 0; fi
    shift
done
exit 1
"#,
        ),
        (
            "systemctl",
            r#"#!/bin/sh
case "$2" in
show-environment|list-units|reset-failed) exit 0;;
show)
    if [ -f "$TRI_FIXTURE/$3.stopped" ]; then
        echo LoadState=not-found
    else
        echo LoadState=loaded
        echo ActiveState=active
        echo SubState=running
    fi;;
stop)
    if [ ! -f "$TRI_FIXTURE/stop-failed" ]; then
        touch "$TRI_FIXTURE/stop-failed"
        exit 1
    fi
    touch "$TRI_FIXTURE/$3.stopped";;
*) exit 1;;
esac
"#,
        ),
    ] {
        let path = root.join(name);
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let server = Server::new(Store::open(&root.join("tri.db"), &root.join("data")).unwrap());
    let first = submit(&server, "cancel");
    let second = submit(&server, "next");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let completion_requests = Arc::new(AtomicUsize::new(0));
    let background = {
        let server = server.clone();
        let stop = stop.clone();
        let completion_requests = completion_requests.clone();
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(error) => panic!("accept: {error}"),
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                // The client sends a hello followed by one request per connection.
                for _ in 0..2 {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 {
                        break;
                    }
                    let request: Request = serde_json::from_str(&line).unwrap();
                    if matches!(request, Request::Complete { .. })
                        && completion_requests.fetch_add(1, Ordering::SeqCst) == 0
                    {
                        // Lose the first completion response before persisting it.
                        break;
                    }
                    let response = server.handle(request).unwrap();
                    if writeln!(stream, "{}", serde_json::to_string(&response).unwrap()).is_err() {
                        break;
                    }
                }
            }
        })
    };
    let child = Command::new(env!("CARGO_BIN_EXE_tri-client"))
        .args([
            "--name",
            "fixture",
            "--cpus",
            "1",
            "--memory",
            "1M",
            "--scratch",
        ])
        .arg(root.join("scratch"))
        .env("PATH", format!("{}:/usr/bin:/bin", root.display()))
        .env("TRI_FIXTURE", &root)
        .env("TRI_SERVER_ADDR", addr.to_string())
        .env_remove("TRI_AUTH_TOKEN")
        .env("TRI_NODE_GPUS", "0")
        .env_remove("TRI_NODE_GPU_DEVICES")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut harness = Harness {
        child,
        stop,
        listener: Some(background),
        root,
    };
    eventually(|| {
        harness
            .root
            .join(format!("tri-task-{}-1.started", first.id))
            .exists()
    });
    server.handle(Request::Cancel { id: first.id }).unwrap();
    eventually(|| harness.root.join("stop-failed").exists());
    let pending = server.store.get(first.id).unwrap().unwrap();
    assert!(!pending.cancel_confirmed);
    assert!(pending.reserves_resources());
    assert_eq!(
        server.store.get(second.id).unwrap().unwrap().state,
        TaskState::Wait
    );
    eventually(|| {
        server
            .store
            .get(first.id)
            .unwrap()
            .unwrap()
            .cancel_confirmed
    });
    eventually(|| {
        harness
            .root
            .join(format!("tri-task-{}-1.started", second.id))
            .exists()
    });
    assert!(completion_requests.load(Ordering::SeqCst) >= 2);
    let finished = server.store.get(first.id).unwrap().unwrap();
    assert_eq!(finished.state, TaskState::Cancel);
    assert!(finished.exit_code.is_none());
    assert_eq!(
        finished.completion_reason.as_deref(),
        Some("node_completion")
    );
    assert_eq!(
        server
            .store
            .get(second.id)
            .unwrap()
            .unwrap()
            .assigned_cpus
            .as_deref(),
        Some("0-0")
    );
    let Response::Nodes(nodes) = server.handle(Request::Nodes).unwrap() else {
        panic!()
    };
    assert_eq!(nodes[0].running_tasks, vec![second.id]);
    assert_eq!(nodes[0].cpu_used, 1);
    assert!(
        harness.child.try_wait().unwrap().is_none(),
        "client exited after unit disappearance"
    );
}
