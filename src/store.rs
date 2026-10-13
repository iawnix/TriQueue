use crate::model::{Task, TaskId, TaskResult, TaskSpec, TaskState};
use crate::{ensure_parent, now_string};
use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub struct Store {
    conn: Mutex<Connection>,
    data_dir: PathBuf,
}

impl Store {
    pub fn open(db_path: &Path, data_dir: &Path) -> Result<Self> {
        ensure_parent(db_path)?;
        let conn = Connection::open(db_path)
            .with_context(|| format!("opening database {}", db_path.display()))?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS tasks (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               spec_json TEXT NOT NULL,
               state TEXT NOT NULL,
               node TEXT,
               assigned_cpus TEXT,
               created_at TEXT NOT NULL,
               started_at TEXT,
               finished_at TEXT,
               exit_code INTEGER,
               stdout_path TEXT NOT NULL,
               stderr_path TEXT NOT NULL,
               error TEXT,
               assigned_gpus TEXT
             );
             CREATE INDEX IF NOT EXISTS idx_tasks_state ON tasks(state, id);",
        )?;
        let has_assigned_gpus: i64 = conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('tasks') WHERE name='assigned_gpus'",
            [],
            |row| row.get(0),
        )?;
        if has_assigned_gpus == 0 {
            conn.execute("ALTER TABLE tasks ADD COLUMN assigned_gpus TEXT", [])?;
        }
        std::fs::create_dir_all(data_dir.join("tasks"))?;
        Ok(Self {
            conn: Mutex::new(conn),
            data_dir: data_dir.to_path_buf(),
        })
    }

    pub fn submit(&self, spec: TaskSpec) -> Result<Task> {
        let created_at = now_string();
        let conn = self.conn.lock().expect("store mutex poisoned");
        conn.execute(
            "INSERT INTO tasks (spec_json, state, created_at, stdout_path, stderr_path)
             VALUES (?1, 'wait', ?2, '', '')",
            params![serde_json::to_string(&spec)?, created_at],
        )?;
        let id = conn.last_insert_rowid();
        let stdout = resolve_log_path(
            spec.output_path.as_deref(),
            &spec,
            id,
            &format!("tri-{id}.out"),
            &self.data_dir,
        );
        let stderr = spec.error_path.as_deref().map_or_else(
            || stdout.clone(),
            |path| {
                resolve_log_path(
                    Some(path),
                    &spec,
                    id,
                    &format!("tri-{id}.err"),
                    &self.data_dir,
                )
            },
        );
        conn.execute(
            "UPDATE tasks SET stdout_path=?1, stderr_path=?2 WHERE id=?3",
            params![stdout.to_string_lossy(), stderr.to_string_lossy(), id],
        )?;
        drop(conn);
        self.get(id)?.context("new task disappeared")
    }

    pub fn list(&self, state: Option<&str>) -> Result<Vec<Task>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let mut stmt = if state.is_some() {
            conn.prepare("SELECT * FROM tasks WHERE state=?1 ORDER BY id")?
        } else {
            conn.prepare("SELECT * FROM tasks ORDER BY id")?
        };
        let rows = if let Some(state) = state {
            stmt.query_map(params![state], row_to_task)?
        } else {
            stmt.query_map([], row_to_task)?
        };
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn get(&self, id: TaskId) -> Result<Option<Task>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        Ok(conn
            .query_row("SELECT * FROM tasks WHERE id=?1", params![id], row_to_task)
            .optional()?)
    }

    pub fn claim(&self, id: TaskId, node: &str, cpus: &str, gpus: &str) -> Result<Option<Task>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let changed = conn.execute(
            "UPDATE tasks SET state='run', node=?1, assigned_cpus=?2, assigned_gpus=?3, started_at=?4
             WHERE id=?5 AND state='wait'",
            params![node, cpus, (!gpus.is_empty()).then_some(gpus), now_string(), id],
        )?;
        drop(conn);
        if changed == 0 { Ok(None) } else { self.get(id) }
    }

    pub fn cancel(&self, id: TaskId) -> Result<Option<Task>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        conn.execute(
            "UPDATE tasks SET state='cancel', finished_at=?1, error=CASE WHEN state='run' THEN 'cancel requested' ELSE error END
             WHERE id=?2 AND state IN ('wait','run')",
            params![now_string(), id],
        )?;
        drop(conn);
        self.get(id)
    }

    pub fn complete(&self, result: &TaskResult) -> Result<Option<Task>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        conn.execute(
            "UPDATE tasks SET state=?1, finished_at=?2, exit_code=?3, error=?4 WHERE id=?5 AND state='run'",
            params![result.state.as_str(), now_string(), result.exit_code, result.error, result.id],
        )?;
        drop(conn);
        self.get(result.id)
    }

    pub fn clean(&self) -> Result<usize> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        Ok(conn.execute(
            "DELETE FROM tasks WHERE state IN ('done','fail','cancel','lost')",
            [],
        )?)
    }
}

fn resolve_log_path(
    requested: Option<&str>,
    spec: &TaskSpec,
    id: TaskId,
    default_name: &str,
    data_dir: &Path,
) -> PathBuf {
    let raw = requested
        .unwrap_or(default_name)
        .replace("%j", &id.to_string());
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        path
    } else if let Some(cwd) = &spec.cwd {
        Path::new(cwd).join(path)
    } else {
        data_dir.join(path)
    }
}

fn row_to_task(row: &rusqlite::Row<'_>) -> rusqlite::Result<Task> {
    let spec_json: String = row.get(1)?;
    let state: String = row.get(2)?;
    Ok(Task {
        id: row.get(0)?,
        spec: serde_json::from_str(&spec_json).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(e))
        })?,
        state: match state.as_str() {
            "wait" => TaskState::Wait,
            "run" => TaskState::Run,
            "done" => TaskState::Done,
            "fail" => TaskState::Fail,
            "cancel" => TaskState::Cancel,
            "lost" => TaskState::Lost,
            _ => TaskState::Fail,
        },
        node: row.get(3)?,
        assigned_cpus: row.get(4)?,
        assigned_gpus: row.get(12)?,
        created_at: row.get(5)?,
        started_at: row.get(6)?,
        finished_at: row.get(7)?,
        exit_code: row.get(8)?,
        stdout_path: row.get(9)?,
        stderr_path: row.get(10)?,
        error: row.get(11)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_lifecycle_is_persisted() {
        let root = std::env::temp_dir().join(format!("tri-store-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = Store::open(&root.join("tri.db"), &root.join("data")).unwrap();
        let task = store
            .submit(TaskSpec {
                name: "test".into(),
                command: vec!["true".into()],
                requested_cpu: 2,
                requested_mem_bytes: 1024,
                requested_gpus: 0,
                scratch_cwd: false,
                requested_node: None,
                cwd: None,
                env: Vec::new(),
                output_path: None,
                error_path: None,
            })
            .unwrap();
        assert_eq!(task.state, TaskState::Wait);
        let claimed = store.claim(task.id, "worker", "0-1", "").unwrap().unwrap();
        assert_eq!(claimed.state, TaskState::Run);
        store
            .complete(&TaskResult {
                id: task.id,
                state: TaskState::Done,
                exit_code: Some(0),
                error: None,
            })
            .unwrap();
        assert_eq!(store.get(task.id).unwrap().unwrap().state, TaskState::Done);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn log_paths_follow_workdir_and_overrides() {
        let root = std::env::temp_dir().join(format!("tri-store-path-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let cwd = root.join("job");
        std::fs::create_dir_all(&cwd).unwrap();
        let store = Store::open(&root.join("tri.db"), &root.join("data")).unwrap();
        let task = store
            .submit(TaskSpec {
                name: "default".into(),
                command: vec!["true".into()],
                requested_cpu: 1,
                requested_mem_bytes: 0,
                requested_gpus: 0,
                scratch_cwd: false,
                requested_node: None,
                cwd: Some(cwd.to_string_lossy().into_owned()),
                env: Vec::new(),
                output_path: None,
                error_path: None,
            })
            .unwrap();
        assert_eq!(
            task.stdout_path,
            cwd.join(format!("tri-{}.out", task.id)).to_string_lossy()
        );
        assert_eq!(task.stdout_path, task.stderr_path);

        let custom = store
            .submit(TaskSpec {
                name: "custom".into(),
                command: vec!["true".into()],
                requested_cpu: 1,
                requested_mem_bytes: 0,
                requested_gpus: 0,
                scratch_cwd: false,
                requested_node: None,
                cwd: Some(cwd.to_string_lossy().into_owned()),
                env: Vec::new(),
                output_path: Some("logs/%j.out".into()),
                error_path: Some("errors/%j.err".into()),
            })
            .unwrap();
        assert_eq!(
            custom.stdout_path,
            cwd.join(format!("logs/{}.out", custom.id))
                .to_string_lossy()
        );
        assert_eq!(
            custom.stderr_path,
            cwd.join(format!("errors/{}.err", custom.id))
                .to_string_lossy()
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
