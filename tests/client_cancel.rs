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
        for marker in ["block-start", "block-stop", "block-show"] {
            let _ = std::fs::remove_file(self.root.join(marker));
        }
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

fn fixture(
    name: &str,
    cpus: &str,
    block_start: bool,
) -> (Harness, Server, Task, Task, Arc<AtomicUsize>) {
    fixture_with_setup(name, cpus, block_start, |_, _, _| {})
}

fn fixture_with_setup(
    name: &str,
    cpus: &str,
    block_start: bool,
    setup: impl FnOnce(&std::path::Path, &Server, &Task),
) -> (Harness, Server, Task, Task, Arc<AtomicUsize>) {
    let root = std::env::temp_dir().join(format!("tri-client-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    if block_start {
        std::fs::write(root.join("block-start"), "").unwrap();
    }
    for (name, script) in [
        (
            "systemd-run",
            r#"#!/bin/sh
while [ "$#" -gt 0 ]; do
    if [ "$1" = '--unit' ]; then
        shift
        touch "$TRI_FIXTURE/$1.started"
        if [ -f "$TRI_FIXTURE/start-fail" ]; then
            rm "$TRI_FIXTURE/start-fail"
            exit 1
        fi
        while [ -f "$TRI_FIXTURE/block-start" ]; do sleep 0.05; done
        exit 0
    fi
    shift
done
exit 1
"#,
        ),
        (
            "systemctl",
            r#"#!/bin/sh
case "$2" in
show-environment|reset-failed) exit 0;;
list-units)
    if [ -f "$TRI_FIXTURE/adopt-unit" ]; then cat "$TRI_FIXTURE/adopt-unit"; fi
    exit 0;;
show)
    if [ -f "$TRI_FIXTURE/inspect-fail" ]; then
        touch "$TRI_FIXTURE/inspection-failed"
        exit 1
    fi
    if [ -f "$TRI_FIXTURE/block-show" ]; then
        touch "$TRI_FIXTURE/inspection-blocked"
        while [ -f "$TRI_FIXTURE/block-show" ]; do sleep 0.05; done
    fi
    if [ -f "$TRI_FIXTURE/$3.stopped" ]; then
        echo LoadState=not-found
        exit 4
    elif [ -f "$TRI_FIXTURE/$3.terminal" ]; then
        echo LoadState=loaded
        echo ActiveState=active
        echo SubState=exited
        echo Result=success
        echo ExecMainStatus=0
    else
        echo LoadState=loaded
        echo ActiveState=active
        echo SubState=running
    fi;;
stop)
    if [ -f "$TRI_FIXTURE/block-stop" ]; then
        touch "$TRI_FIXTURE/stop-blocked"
        while [ -f "$TRI_FIXTURE/block-stop" ]; do sleep 0.05; done
    fi
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
    setup(&root, &server, &first);
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
            cpus,
            "--memory",
            "1M",
            "--scratch",
        ])
        .arg(root.join("scratch"))
        .env("PATH", format!("{}:/usr/bin:/bin", root.display()))
        .env("TRI_FIXTURE", &root)
        .env("TRI_SERVER_ADDR", addr.to_string())
        .env_remove("TRI_AUTH_TOKEN")
        .env_remove("TRI_TASK_USER")
        .env("TRI_NODE_GPUS", "0")
        .env_remove("TRI_NODE_GPU_DEVICES")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let harness = Harness {
        child,
        stop,
        listener: Some(background),
        root,
    };
    (harness, server, first, second, completion_requests)
}

#[test]
fn client_retries_stop_and_completion_before_next_job_uses_allocation() {
    let (mut harness, server, first, second, completion_requests) = fixture("cancel", "1", false);
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

#[test]
fn missing_unit_reports_failure_without_stopping_other_tasks_or_client() {
    let (mut harness, server, first, second, completions) = fixture("missing", "2", false);
    eventually(|| {
        harness
            .root
            .join(format!("tri-task-{}-1.started", second.id))
            .exists()
    });
    std::fs::write(
        harness
            .root
            .join(format!("tri-task-{}-1.service.stopped", first.id)),
        "",
    )
    .unwrap();
    eventually(|| server.store.get(first.id).unwrap().unwrap().state == TaskState::Fail);
    let finished = server.store.get(first.id).unwrap().unwrap();
    assert!(
        finished
            .error
            .as_deref()
            .unwrap()
            .contains("disappeared; exit status unavailable")
    );
    assert!(finished.exit_code.is_none());
    assert!(!finished.reserves_resources());
    assert!(
        completions.load(Ordering::SeqCst) >= 2,
        "completion must survive a dropped response"
    );
    assert_eq!(
        server.store.get(second.id).unwrap().unwrap().state,
        TaskState::Run
    );
    let third = submit(&server, "after-missing");
    eventually(|| {
        harness
            .root
            .join(format!("tri-task-{}-1.started", third.id))
            .exists()
    });
    assert!(harness.child.try_wait().unwrap().is_none());
}

fn assert_lease_renews(server: &Server, id: TaskId) {
    let before = server
        .store
        .get(id)
        .unwrap()
        .unwrap()
        .lease_expires_at
        .unwrap();
    eventually(|| {
        let task = server.store.get(id).unwrap().unwrap();
        task.reserves_resources()
            && task
                .lease_expires_at
                .as_ref()
                .is_some_and(|after| after > &before)
    });
}

#[test]
fn heartbeats_continue_during_slow_start_inspection_and_stop() {
    let (mut harness, server, first, second, _) = fixture("slow", "1", true);
    eventually(|| {
        harness
            .root
            .join(format!("tri-task-{}-1.started", first.id))
            .exists()
    });
    // systemd-run has not returned yet, but this assigned attempt must renew.
    assert_lease_renews(&server, first.id);
    std::fs::write(harness.root.join("block-show"), "").unwrap();
    std::fs::remove_file(harness.root.join("block-start")).unwrap();
    eventually(|| harness.root.join("inspection-blocked").exists());
    assert_lease_renews(&server, first.id);
    std::fs::remove_file(harness.root.join("block-show")).unwrap();
    std::fs::write(harness.root.join("block-stop"), "").unwrap();
    server.handle(Request::Cancel { id: first.id }).unwrap();
    eventually(|| harness.root.join("stop-blocked").exists());
    assert_lease_renews(&server, first.id);
    assert!(
        !server
            .store
            .get(first.id)
            .unwrap()
            .unwrap()
            .cancel_confirmed
    );
    assert_eq!(
        server.store.get(second.id).unwrap().unwrap().state,
        TaskState::Wait
    );
    std::fs::remove_file(harness.root.join("block-stop")).unwrap();
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
    assert!(harness.child.try_wait().unwrap().is_none());
}

#[test]
fn inspection_errors_retain_allocation_and_recover() {
    let (mut harness, server, first, second, _) = fixture("inspect", "1", false);
    eventually(|| {
        harness
            .root
            .join(format!("tri-task-{}-1.started", first.id))
            .exists()
    });
    std::fs::write(harness.root.join("inspect-fail"), "").unwrap();
    eventually(|| harness.root.join("inspection-failed").exists());
    assert_lease_renews(&server, first.id);
    assert_eq!(
        server.store.get(first.id).unwrap().unwrap().state,
        TaskState::Run
    );
    assert_eq!(
        server.store.get(second.id).unwrap().unwrap().state,
        TaskState::Wait
    );
    std::fs::remove_file(harness.root.join("inspect-fail")).unwrap();
    std::fs::write(
        harness
            .root
            .join(format!("tri-task-{}-1.service.stopped", first.id)),
        "",
    )
    .unwrap();
    eventually(|| server.store.get(first.id).unwrap().unwrap().state == TaskState::Fail);
    eventually(|| {
        harness
            .root
            .join(format!("tri-task-{}-1.started", second.id))
            .exists()
    });
    assert!(harness.child.try_wait().unwrap().is_none());
}

#[test]
fn adopted_task_survives_inspection_error_then_reports_disappearance() {
    let (mut harness, server, first, second, _) =
        fixture_with_setup("adopt", "1", false, |root, server, task| {
            server.store.claim(task.id, "fixture", "0-0", "").unwrap();
            std::fs::write(
                root.join("adopt-unit"),
                format!("tri-task-{}-1.service loaded active running\n", task.id),
            )
            .unwrap();
            std::fs::write(root.join("inspect-fail"), "").unwrap();
        });
    eventually(|| harness.root.join("inspection-failed").exists());
    assert_lease_renews(&server, first.id);
    std::fs::write(
        harness
            .root
            .join(format!("tri-task-{}-1.service.stopped", first.id)),
        "",
    )
    .unwrap();
    std::fs::remove_file(harness.root.join("inspect-fail")).unwrap();
    eventually(|| server.store.get(first.id).unwrap().unwrap().state == TaskState::Fail);
    eventually(|| {
        harness
            .root
            .join(format!("tri-task-{}-1.started", second.id))
            .exists()
    });
    assert!(harness.child.try_wait().unwrap().is_none());
}

#[test]
fn startup_failure_waits_for_stop_and_retries_completion() {
    let (mut harness, server, first, second, completions) =
        fixture_with_setup("start-fail", "1", false, |root, _, _| {
            std::fs::write(root.join("start-fail"), "").unwrap();
        });
    eventually(|| server.store.get(first.id).unwrap().unwrap().state == TaskState::Fail);
    let finished = server.store.get(first.id).unwrap().unwrap();
    assert!(
        finished
            .error
            .as_deref()
            .unwrap()
            .contains("task startup failed")
    );
    assert!(
        harness
            .root
            .join(format!("tri-task-{}-1.service.stopped", first.id))
            .exists()
    );
    assert!(completions.load(Ordering::SeqCst) >= 2);
    eventually(|| {
        harness
            .root
            .join(format!("tri-task-{}-1.started", second.id))
            .exists()
    });
    assert!(harness.child.try_wait().unwrap().is_none());
}

#[test]
fn terminal_result_keeps_reservation_until_unit_stop_finishes() {
    let (harness, server, first, second, completions) = fixture("terminal-stop", "1", false);
    eventually(|| {
        harness
            .root
            .join(format!("tri-task-{}-1.started", first.id))
            .exists()
    });
    std::fs::write(harness.root.join("block-stop"), "").unwrap();
    std::fs::write(
        harness
            .root
            .join(format!("tri-task-{}-1.service.terminal", first.id)),
        "",
    )
    .unwrap();
    eventually(|| harness.root.join("stop-blocked").exists());
    assert!(
        server
            .store
            .get(first.id)
            .unwrap()
            .unwrap()
            .reserves_resources()
    );
    assert_eq!(
        server.store.get(second.id).unwrap().unwrap().state,
        TaskState::Wait
    );
    assert_eq!(completions.load(Ordering::SeqCst), 0);
    std::fs::remove_file(harness.root.join("block-stop")).unwrap();
    eventually(|| server.store.get(first.id).unwrap().unwrap().state == TaskState::Done);
    assert!(
        harness
            .root
            .join(format!("tri-task-{}-1.service.stopped", first.id))
            .exists()
    );
    eventually(|| server.store.get(second.id).unwrap().unwrap().state == TaskState::Run);
    let history = server.store.history(first.id).unwrap();
    assert_eq!(
        history
            .events
            .iter()
            .filter(|e| e.kind == "completed")
            .count(),
        1
    );
    assert_eq!(history.attempts[0].exit_code, Some(0));
}
