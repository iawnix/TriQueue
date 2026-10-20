//! Prove that failed enforcement prevents execution, including task-provided
//! environment hooks. These tests never create or alter a real queue task unit.
use std::process::Command;
use tri::resources::Limits;

#[test]
fn unchecked_task_cannot_execute_its_command() {
    let dir = std::env::temp_dir().join(format!("tri-guard-block-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let marker = dir.join("should-not-exist");
    let limits = Limits {
        // This is intentionally not the current cgroup's effective CPU set.
        cpus: "1048575".into(),
        memory_bytes: 64 * 1024 * 1024,
        gpu_devices: vec![],
        assigned_gpus: String::new(),
        environment: vec![("LD_PRELOAD".into(), "/does/not/exist.so".into())],
        stdout_path: Some(dir.join("out").to_string_lossy().into_owned()),
        stderr_path: Some(dir.join("err").to_string_lossy().into_owned()),
    };
    let output = Command::new(env!("CARGO_BIN_EXE_tri-client"))
        .arg("__run-task")
        .arg(serde_json::to_string(&limits).unwrap())
        .args(["--", "touch"])
        .arg(&marker)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(78));
    assert!(!marker.exists(), "user command ran without verified limits");
    let error = std::fs::read_to_string(dir.join("err")).unwrap();
    assert!(error.contains("TRI resource enforcement failed"), "{error}");
    assert!(
        !error.contains("ld.so"),
        "task environment affected the guard"
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn unallocated_device_access_cannot_be_claimed_as_isolated() {
    let limits = Limits {
        cpus: "0".into(),
        memory_bytes: 1024,
        // A character device guaranteed to be accessible on the test host.
        gpu_devices: vec!["/dev/null".into()],
        assigned_gpus: String::new(),
        environment: vec![],
        stdout_path: None,
        stderr_path: None,
    };
    let error = tri::resources::verify_devices(&limits).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unallocated GPU device /dev/null is accessible")
    );
}
