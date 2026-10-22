use crate::model::{Task, TaskEvent, TaskHistory, TaskId, TaskResult, TaskSpec, TaskState};
use crate::{ensure_parent, now_string};
use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub const LEASE_SECONDS: u64 = 60;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DependencyStatus {
    Ready,
    Waiting,
    Blocked(String),
}

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
               assigned_gpus TEXT,
               attempt INTEGER NOT NULL DEFAULT 0,
               lease_id TEXT,
               lease_expires_at TEXT,
               external_id TEXT,
               cancel_confirmed INTEGER NOT NULL DEFAULT 0,
               completion_reason TEXT
             );
             CREATE INDEX IF NOT EXISTS idx_tasks_state ON tasks(state, id);
             CREATE TABLE IF NOT EXISTS submission_receipts (
               request_id TEXT PRIMARY KEY,
               fingerprint TEXT NOT NULL,
               task_id INTEGER NOT NULL
             );",
        )?;
        let has_assigned_gpus: i64 = conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('tasks') WHERE name='assigned_gpus'",
            [],
            |row| row.get(0),
        )?;
        if has_assigned_gpus == 0 {
            conn.execute("ALTER TABLE tasks ADD COLUMN assigned_gpus TEXT", [])?;
        }
        for (name, definition) in [
            ("attempt", "INTEGER NOT NULL DEFAULT 0"),
            ("lease_id", "TEXT"),
            ("lease_expires_at", "TEXT"),
            ("external_id", "TEXT"),
            ("cancel_confirmed", "INTEGER NOT NULL DEFAULT 0"),
            ("completion_reason", "TEXT"),
            ("recovery_pending", "INTEGER NOT NULL DEFAULT 0"),
        ] {
            let exists: i64 = conn.query_row(
                "SELECT COUNT(*) FROM pragma_table_info('tasks') WHERE name=?1",
                params![name],
                |row| row.get(0),
            )?;
            if exists == 0 {
                conn.execute(
                    &format!("ALTER TABLE tasks ADD COLUMN {name} {definition}"),
                    [],
                )?;
            }
        }
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_external_id
             ON tasks(external_id) WHERE external_id IS NOT NULL",
            [],
        )?;
        install_history(&conn)?;
        std::fs::create_dir_all(data_dir.join("tasks"))?;
        Ok(Self {
            conn: Mutex::new(conn),
            data_dir: data_dir.to_path_buf(),
        })
    }

    pub fn submit(&self, spec: TaskSpec) -> Result<Task> {
        Ok(self.submit_with_receipt(None, spec)?.0)
    }

    pub fn submit_with_receipt(
        &self,
        request_id: Option<&str>,
        mut spec: TaskSpec,
    ) -> Result<(Task, bool)> {
        anyhow::ensure!(
            spec.requested_cpu > 0,
            "requested_cpu must be greater than zero"
        );
        if spec.requested_mem_bytes == 0 {
            spec.requested_mem_bytes = crate::resources::DEFAULT_MEMORY_BYTES;
        }
        anyhow::ensure!(
            spec.max_attempts > 0,
            "max_attempts must be greater than zero"
        );
        if let Some(request_id) = request_id {
            anyhow::ensure!(
                !request_id.trim().is_empty(),
                "request_id must not be empty"
            );
        }
        if let Some(external_id) = &spec.external_id {
            anyhow::ensure!(
                !external_id.trim().is_empty(),
                "external_id must not be empty"
            );
        }
        if let Some(timeout) = spec.timeout_seconds {
            anyhow::ensure!(timeout > 0, "timeout_seconds must be greater than zero");
        }
        let created_at = now_string();
        let fingerprint = serde_json::to_string(&spec)?;
        let mut conn = self.conn.lock().expect("store mutex poisoned");
        let tx = conn.transaction()?;

        if let Some(request_id) = request_id
            && let Some((stored_fingerprint, task_id)) = tx
                .query_row(
                    "SELECT fingerprint, task_id FROM submission_receipts WHERE request_id=?1",
                    params![request_id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, TaskId>(1)?)),
                )
                .optional()?
        {
            anyhow::ensure!(
                stored_fingerprint == fingerprint,
                "request_id_reused: request_id belongs to a different submission"
            );
            let task = tx
                .query_row(
                    "SELECT * FROM tasks WHERE id=?1",
                    params![task_id],
                    row_to_task,
                )
                .optional()?
                .context("submission receipt points to a missing task")?;
            tx.commit()?;
            return Ok((task, false));
        }

        if let Some(external_id) = &spec.external_id
            && let Some((task_id, stored_fingerprint)) = tx
                .query_row(
                    "SELECT id, spec_json FROM tasks WHERE external_id=?1",
                    params![external_id],
                    |row| Ok((row.get::<_, TaskId>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()?
        {
            anyhow::ensure!(
                stored_fingerprint == fingerprint,
                "external_id_conflict: external_id belongs to a different submission"
            );
            if let Some(request_id) = request_id {
                tx.execute(
                    "INSERT INTO submission_receipts (request_id, fingerprint, task_id)
                         VALUES (?1, ?2, ?3)",
                    params![request_id, fingerprint, task_id],
                )?;
            }
            let task = tx.query_row(
                "SELECT * FROM tasks WHERE id=?1",
                params![task_id],
                row_to_task,
            )?;
            tx.commit()?;
            return Ok((task, false));
        }

        for dependency in &spec.depends_on {
            let exists: Option<i64> = tx
                .query_row(
                    "SELECT id FROM tasks WHERE id=?1",
                    params![dependency],
                    |row| row.get(0),
                )
                .optional()?;
            anyhow::ensure!(
                exists.is_some(),
                "dependency task {dependency} does not exist"
            );
        }
        tx.execute(
            "INSERT INTO tasks (spec_json, state, created_at, stdout_path, stderr_path, external_id)
             VALUES (?1, 'wait', ?2, '', '', ?3)",
            params![fingerprint, created_at, spec.external_id],
        )?;
        let id = tx.last_insert_rowid();
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
        tx.execute(
            "UPDATE tasks SET stdout_path=?1, stderr_path=?2 WHERE id=?3",
            params![stdout.to_string_lossy(), stderr.to_string_lossy(), id],
        )?;
        if let Some(request_id) = request_id {
            tx.execute(
                "INSERT INTO submission_receipts (request_id, fingerprint, task_id)
                 VALUES (?1, ?2, ?3)",
                params![request_id, fingerprint, id],
            )?;
        }
        tx.commit()?;
        drop(conn);
        Ok((self.get(id)?.context("new task disappeared")?, true))
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

    pub fn get_external(&self, external_id: &str) -> Result<Option<Task>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        Ok(conn
            .query_row(
                "SELECT * FROM tasks WHERE external_id=?1",
                params![external_id],
                row_to_task,
            )
            .optional()?)
    }

    pub fn claim(&self, id: TaskId, node: &str, cpus: &str, gpus: &str) -> Result<Option<Task>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let current_attempt: u32 = conn.query_row(
            "SELECT attempt FROM tasks WHERE id=?1",
            params![id],
            |row| row.get(0),
        )?;
        let attempt = current_attempt.saturating_add(1);
        let lease_id = format!("tri-{id}-attempt-{attempt}");
        let lease_expires_at = (crate::now_seconds() + LEASE_SECONDS).to_string();
        let changed = conn.execute(
            "UPDATE tasks SET state='run', node=?1, assigned_cpus=?2, assigned_gpus=?3,
                    started_at=?4, attempt=?5, lease_id=?6, lease_expires_at=?7,
                    completion_reason=NULL, cancel_confirmed=0, recovery_pending=0,
                    finished_at=NULL, exit_code=NULL, error=NULL
             WHERE id=?8 AND state='wait'",
            params![
                node,
                cpus,
                (!gpus.is_empty()).then_some(gpus),
                now_string(),
                attempt,
                lease_id,
                lease_expires_at,
                id
            ],
        )?;
        drop(conn);
        if changed == 0 { Ok(None) } else { self.get(id) }
    }

    pub fn cancel(&self, id: TaskId) -> Result<Option<Task>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        conn.execute(
            "UPDATE tasks SET state='cancel',
                    finished_at=CASE WHEN state='wait' THEN ?1 ELSE finished_at END,
                    error=CASE WHEN state IN ('run','unknown') THEN 'cancel requested' ELSE error END,
                    cancel_confirmed=CASE WHEN state='wait' THEN 1 ELSE cancel_confirmed END,
                    completion_reason=CASE WHEN state='wait' THEN 'cancelled_before_start' ELSE NULL END
             WHERE id=?2 AND state IN ('wait','run','unknown')",
            params![now_string(), id],
        )?;
        drop(conn);
        self.get(id)
    }

    pub fn complete(&self, result: &TaskResult) -> Result<Option<Task>> {
        anyhow::ensure!(
            matches!(
                result.state,
                TaskState::Done | TaskState::Fail | TaskState::Cancel | TaskState::TimedOut
            ),
            "completion must report a terminal execution state"
        );
        let conn = self.conn.lock().expect("store mutex poisoned");
        conn.execute(
            "UPDATE tasks SET state=CASE WHEN state='cancel' THEN 'cancel' ELSE ?1 END,
                    finished_at=?2, exit_code=?3,
                    error=?4,
                    lease_id=NULL, lease_expires_at=NULL,
                    cancel_confirmed=CASE WHEN state='cancel' OR ?1='cancel' THEN 1 ELSE 0 END,
                    completion_reason='node_completion', recovery_pending=0
             WHERE id=?5 AND state IN ('run','unknown','cancel')
               AND lease_id=?6
               AND attempt=?7",
            params![
                result.state.as_str(),
                now_string(),
                result.exit_code,
                result.error,
                result.id,
                result.lease_id,
                result.attempt
            ],
        )?;
        drop(conn);
        self.get(result.id)
    }

    pub fn clean(&self, older_than_seconds: Option<u64>) -> Result<usize> {
        let mut conn = self.conn.lock().expect("store mutex poisoned");
        let tx = conn.transaction()?;
        let removed = if let Some(age) = older_than_seconds {
            let cutoff = crate::now_seconds().saturating_sub(age).to_string();
            tx.execute(
                "DELETE FROM tasks
                 WHERE state IN ('done','fail','cancel','lost','blocked','timed_out')
                   AND finished_at IS NOT NULL AND lease_id IS NULL AND CAST(finished_at AS INTEGER) < CAST(?1 AS INTEGER)",
                params![cutoff],
            )?
        } else {
            tx.execute(
                "DELETE FROM tasks WHERE state IN ('done','fail','cancel','lost','blocked','timed_out') AND finished_at IS NOT NULL AND lease_id IS NULL",
                [],
            )?
        };
        tx.execute(
            "DELETE FROM submission_receipts WHERE task_id NOT IN (SELECT id FROM tasks)",
            [],
        )?;
        tx.commit()?;
        Ok(removed)
    }

    pub fn dependency_status(&self, task: &Task) -> Result<DependencyStatus> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let mut waiting = false;
        for dependency in &task.spec.depends_on {
            let state: String = conn
                .query_row(
                    "SELECT state FROM tasks WHERE id=?1",
                    params![dependency],
                    |row| row.get(0),
                )
                .optional()?
                .unwrap_or_else(|| "missing".into());
            match state.as_str() {
                "done" => {}
                "wait" | "run" | "unknown" => waiting = true,
                "missing" => {
                    return Ok(DependencyStatus::Blocked(format!(
                        "dependency task {dependency} is missing"
                    )));
                }
                other => {
                    return Ok(DependencyStatus::Blocked(format!(
                        "dependency task {dependency} ended in {other}"
                    )));
                }
            }
        }
        Ok(if waiting {
            DependencyStatus::Waiting
        } else {
            DependencyStatus::Ready
        })
    }

    pub fn block(&self, id: TaskId, reason: &str) -> Result<Option<Task>> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        conn.execute(
            "UPDATE tasks SET state='blocked', finished_at=?1, error=?2 WHERE id=?3 AND state='wait'",
            params![now_string(), reason, id],
        )?;
        drop(conn);
        self.get(id)
    }

    pub fn renew_lease(&self, id: TaskId, lease_id: &str) -> Result<bool> {
        // Reap first so even a heartbeat arriving between sweeps records the gap.
        self.reap_expired(crate::now_seconds())?;
        let conn = self.conn.lock().expect("store mutex poisoned");
        let changed = conn.execute(
            "UPDATE tasks SET lease_expires_at=?1,
                    state=CASE WHEN state='unknown' THEN 'run' ELSE state END,
                    error=CASE WHEN recovery_pending=1 THEN NULL ELSE error END,
                    recovery_pending=0
             WHERE id=?2 AND state IN ('run','unknown','cancel') AND lease_id=?3
               AND finished_at IS NULL",
            params![
                (crate::now_seconds() + LEASE_SECONDS).to_string(),
                id,
                lease_id
            ],
        )?;
        Ok(changed != 0)
    }

    pub fn reap_expired(&self, now: u64) -> Result<Vec<Task>> {
        let mut conn = self.conn.lock().expect("store mutex poisoned");
        let tx = conn.transaction()?;
        let expired = {
            let mut stmt = tx.prepare(
                "SELECT * FROM tasks WHERE state IN ('run','cancel')
                 AND lease_expires_at IS NOT NULL AND CAST(lease_expires_at AS INTEGER) < ?1",
            )?;
            stmt.query_map(params![now], row_to_task)?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        tx.execute(
            "UPDATE tasks SET state=CASE WHEN state='cancel' THEN 'cancel' ELSE 'unknown' END,
                    recovery_pending=1, lease_expires_at=NULL,
                    error='node heartbeat expired; execution not confirmed stopped'
             WHERE state IN ('run','cancel') AND lease_expires_at IS NOT NULL
               AND CAST(lease_expires_at AS INTEGER) < ?1",
            params![now],
        )?;
        tx.commit()?;
        for task in &expired {
            tracing::warn!(task=task.id, attempt=task.attempt, node=?task.node,
                "task lease expired; allocation retained and node quarantined");
        }
        Ok(expired)
    }

    /// Operator assertion: the old execution has stopped and cannot start late.
    /// This must never be inferred from a missing heartbeat or unit listing.
    pub fn confirm_stopped(
        &self,
        id: TaskId,
        attempt: u32,
        reason: &str,
        retry: bool,
    ) -> Result<Task> {
        anyhow::ensure!(!reason.trim().is_empty(), "reason must not be empty");
        let mut conn = self.conn.lock().expect("store mutex poisoned");
        let tx = conn.transaction()?;
        let task = tx
            .query_row("SELECT * FROM tasks WHERE id=?1", [id], row_to_task)
            .optional()?
            .context("task does not exist")?;
        anyhow::ensure!(
            task.attempt == attempt && task.recovery_pending && task.reserves_resources(),
            "task is not an unresolved execution of attempt {attempt}"
        );
        anyhow::ensure!(
            !retry || (task.state != TaskState::Cancel && attempt < task.spec.max_attempts),
            "retry requires an uncancelled attempt below max_attempts"
        );
        tx.execute(
            "UPDATE tasks SET state=CASE WHEN state='cancel' THEN 'cancel' ELSE 'lost' END,
                    finished_at=?1, error=?2, completion_reason='operator_confirmed_stopped',
                    cancel_confirmed=CASE WHEN state='cancel' THEN 1 ELSE 0 END,
                    lease_id=NULL, lease_expires_at=NULL, recovery_pending=0 WHERE id=?3",
            params![now_string(), reason, id],
        )?;
        if retry {
            // The terminal attempt is archived by the trigger before requeuing.
            tx.execute(
                "UPDATE tasks SET state='wait', node=NULL, assigned_cpus=NULL,
                    assigned_gpus=NULL, started_at=NULL, finished_at=NULL,
                    exit_code=NULL, error=NULL, completion_reason=NULL WHERE id=?1",
                [id],
            )?;
        }
        let task = tx.query_row("SELECT * FROM tasks WHERE id=?1", [id], row_to_task)?;
        tx.commit()?;
        Ok(task)
    }

    pub fn history(&self, id: TaskId) -> Result<TaskHistory> {
        let conn = self.conn.lock().expect("store mutex poisoned");
        let exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM tasks WHERE id=?1)",
            [id],
            |r| r.get(0),
        )?;
        anyhow::ensure!(exists, "task {id} does not exist");
        let mut stmt =
            conn.prepare("SELECT snapshot FROM task_attempts WHERE task_id=?1 ORDER BY attempt")?;
        let snapshots = stmt
            .query_map([id], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let attempts = snapshots
            .iter()
            .map(|s| serde_json::from_str(s))
            .collect::<serde_json::Result<Vec<Task>>>()?;
        let mut stmt = conn.prepare("SELECT id, task_id, attempt, at, kind, from_state, to_state, detail FROM task_events WHERE task_id=?1 ORDER BY id")?;
        let events = stmt
            .query_map([id], |row| {
                Ok(TaskEvent {
                    id: row.get(0)?,
                    task_id: row.get(1)?,
                    attempt: row.get(2)?,
                    at: row.get(3)?,
                    kind: row.get(4)?,
                    from_state: row.get(5)?,
                    to_state: row.get(6)?,
                    detail: row.get(7)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(TaskHistory { attempts, events })
    }
}

/// Triggers keep audit writes in the same SQLite transaction as state changes,
/// including cancellation and migration. Heartbeat-only updates create no events.
fn install_history(conn: &Connection) -> Result<()> {
    fn snapshot(prefix: &str, old: Option<&str>) -> String {
        let fields = [
            "id",
            "state",
            "node",
            "assigned_cpus",
            "assigned_gpus",
            "created_at",
            "started_at",
            "finished_at",
            "exit_code",
            "stdout_path",
            "stderr_path",
            "error",
            "external_id",
            "attempt",
            "lease_id",
            "lease_expires_at",
            "completion_reason",
        ];
        let mut values = vec![format!("'spec',json({prefix}.spec_json)")];
        for field in fields {
            let value = if field == "lease_id" {
                old.map_or_else(|| format!("{prefix}.{field}"), |old|
                    format!("COALESCE({prefix}.{field},CASE WHEN {old}.attempt={prefix}.attempt THEN {old}.{field} END)"))
            } else {
                format!("{prefix}.{field}")
            };
            values.push(format!("'{field}',{value}"));
        }
        for field in ["cancel_confirmed", "recovery_pending"] {
            values.push(format!(
                "'{field}',json(CASE WHEN {prefix}.{field} THEN 'true' ELSE 'false' END)"
            ));
        }
        format!("json_object({})", values.join(","))
    }
    let initial = snapshot("t", None);
    let updated = snapshot("NEW", Some("OLD"));
    conn.execute_batch(&format!(
        "BEGIN IMMEDIATE;
         CREATE TABLE IF NOT EXISTS task_attempts (
            task_id INTEGER NOT NULL, attempt INTEGER NOT NULL, snapshot TEXT NOT NULL,
            PRIMARY KEY(task_id, attempt)
         );
         CREATE TABLE IF NOT EXISTS task_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, task_id INTEGER NOT NULL,
            attempt INTEGER NOT NULL, at TEXT NOT NULL, kind TEXT NOT NULL,
            from_state TEXT, to_state TEXT NOT NULL, detail TEXT
         );
         CREATE INDEX IF NOT EXISTS idx_task_events_task ON task_events(task_id, id);
         INSERT OR IGNORE INTO task_attempts SELECT t.id, t.attempt, {initial}
            FROM tasks t WHERE t.attempt > 0 AND t.state != 'wait';
         INSERT INTO task_events (task_id, attempt, at, kind, to_state, detail)
            SELECT id, attempt, CAST(strftime('%s','now') AS TEXT), 'baseline', state,
                'Imported current state; older execution history is unavailable'
            FROM tasks t WHERE NOT EXISTS (SELECT 1 FROM task_events e WHERE e.task_id=t.id);
         CREATE TRIGGER IF NOT EXISTS task_submitted AFTER INSERT ON tasks BEGIN
            INSERT INTO task_events (task_id, attempt, at, kind, to_state)
            VALUES (NEW.id, NEW.attempt, NEW.created_at, 'submitted', NEW.state);
         END;
         CREATE TRIGGER IF NOT EXISTS task_changed AFTER UPDATE ON tasks
         WHEN OLD.state IS NOT NEW.state OR OLD.attempt IS NOT NEW.attempt
           OR OLD.recovery_pending IS NOT NEW.recovery_pending
           OR OLD.finished_at IS NOT NEW.finished_at
           OR OLD.cancel_confirmed IS NOT NEW.cancel_confirmed
         BEGIN
            INSERT INTO task_attempts (task_id, attempt, snapshot)
                SELECT NEW.id, NEW.attempt, {updated}
                WHERE NEW.attempt > 0 AND NEW.state != 'wait'
                  AND (OLD.state != 'wait' OR NEW.attempt > OLD.attempt)
                ON CONFLICT(task_id, attempt) DO UPDATE SET snapshot=excluded.snapshot;
            INSERT INTO task_events (task_id, attempt, at, kind, from_state, to_state, detail)
            VALUES (NEW.id, NEW.attempt, CAST(strftime('%s','now') AS TEXT),
                CASE WHEN NEW.recovery_pending=1 AND OLD.recovery_pending=0 THEN 'lease_expired'
                     WHEN NEW.completion_reason='operator_confirmed_stopped' THEN 'confirmed_stopped'
                     WHEN NEW.finished_at IS NOT NULL THEN 'completed'
                     WHEN OLD.recovery_pending=1 AND NEW.recovery_pending=0 THEN 'recovered'
                     WHEN NEW.attempt > OLD.attempt THEN 'claimed'
                     WHEN NEW.state='wait' THEN 'requeued'
                     WHEN NEW.state='cancel' THEN 'cancel_requested'
                     ELSE 'state_changed' END,
                OLD.state, NEW.state, NEW.error);
         END;
         CREATE TRIGGER IF NOT EXISTS task_deleted AFTER DELETE ON tasks BEGIN
            DELETE FROM task_attempts WHERE task_id=OLD.id;
            DELETE FROM task_events WHERE task_id=OLD.id;
         END;
         COMMIT;"
    ))?;
    Ok(())
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
            "unknown" => TaskState::Unknown,
            "done" => TaskState::Done,
            "fail" => TaskState::Fail,
            "cancel" => TaskState::Cancel,
            "lost" => TaskState::Lost,
            "blocked" => TaskState::Blocked,
            "timed_out" => TaskState::TimedOut,
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
        attempt: row.get(13)?,
        lease_id: row.get(14)?,
        lease_expires_at: row.get(15)?,
        external_id: row.get(16)?,
        cancel_confirmed: row.get::<_, i64>("cancel_confirmed")? != 0,
        completion_reason: row.get("completion_reason")?,
        recovery_pending: row.get("recovery_pending")?,
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
                external_id: None,
                timeout_seconds: None,
                requested_cpu: 2,
                requested_mem_bytes: 1024,
                requested_gpus: 0,
                scratch_cwd: false,
                requested_node: None,
                cwd: None,
                env: Vec::new(),
                output_path: None,
                error_path: None,
                depends_on: Vec::new(),
                max_attempts: 1,
            })
            .unwrap();
        assert_eq!(task.state, TaskState::Wait);
        let claimed = store.claim(task.id, "fed2", "0-1", "").unwrap().unwrap();
        assert_eq!(claimed.state, TaskState::Run);
        store
            .complete(&TaskResult {
                id: task.id,
                state: TaskState::Done,
                exit_code: Some(0),
                error: None,
                attempt: 1,
                lease_id: claimed.lease_id.clone(),
            })
            .unwrap();
        assert_eq!(store.get(task.id).unwrap().unwrap().state, TaskState::Done);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn cancellation_keeps_a_running_task_allocated_until_completion() {
        let root = std::env::temp_dir().join(format!("tri-cancel-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = Store::open(&root.join("tri.db"), &root.join("data")).unwrap();
        let task = store
            .submit(TaskSpec {
                name: "cancel".into(),
                command: vec!["sleep".into(), "10".into()],
                external_id: None,
                timeout_seconds: None,
                requested_cpu: 1,
                requested_mem_bytes: 1,
                requested_gpus: 0,
                scratch_cwd: false,
                requested_node: None,
                cwd: None,
                env: Vec::new(),
                output_path: None,
                error_path: None,
                depends_on: Vec::new(),
                max_attempts: 1,
            })
            .unwrap();
        store.claim(task.id, "fed1", "0-0", "").unwrap();
        let cancelled = store.cancel(task.id).unwrap().unwrap();
        assert_eq!(cancelled.state, TaskState::Cancel);
        assert!(cancelled.finished_at.is_none());
        assert!(cancelled.reserves_resources());
        assert!(!cancelled.cancel_confirmed);
        assert!(
            store
                .renew_lease(task.id, cancelled.lease_id.as_deref().unwrap())
                .unwrap()
        );
        let finished = store
            .complete(&TaskResult {
                id: task.id,
                state: TaskState::Fail,
                exit_code: Some(143),
                error: Some("terminated".into()),
                attempt: 1,
                lease_id: cancelled.lease_id.clone(),
            })
            .unwrap()
            .unwrap();
        assert_eq!(finished.state, TaskState::Cancel);
        assert!(finished.finished_at.is_some());
        assert!(finished.cancel_confirmed);
        assert!(!finished.reserves_resources());
        assert_eq!(finished.error.as_deref(), Some("terminated"));
        assert_eq!(
            finished.completion_reason.as_deref(),
            Some("node_completion")
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn expired_cancel_is_not_reported_as_confirmed_stop() {
        let root =
            std::env::temp_dir().join(format!("tri-cancel-expiry-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = Store::open(&root.join("tri.db"), &root.join("data")).unwrap();
        let task = store
            .submit(TaskSpec {
                name: "cancel-expiry".into(),
                command: vec!["sleep".into(), "10".into()],
                external_id: None,
                timeout_seconds: None,
                requested_cpu: 1,
                requested_mem_bytes: 1,
                requested_gpus: 0,
                scratch_cwd: false,
                requested_node: None,
                cwd: None,
                env: Vec::new(),
                output_path: None,
                error_path: None,
                depends_on: Vec::new(),
                max_attempts: 1,
            })
            .unwrap();
        let claimed = store.claim(task.id, "fed1", "0-0", "").unwrap().unwrap();
        store.cancel(task.id).unwrap();
        store.reap_expired(crate::now_seconds() + 100).unwrap();
        let expired = store.get(task.id).unwrap().unwrap();
        assert_eq!(expired.state, TaskState::Cancel);
        assert!(!expired.cancel_confirmed);
        assert!(expired.finished_at.is_none());
        assert_eq!(expired.lease_id, claimed.lease_id);
        assert!(expired.recovery_pending);
        assert!(expired.reserves_resources());
        assert!(expired.completion_reason.is_none());
        assert_eq!(store.clean(None).unwrap(), 0);
        assert_eq!(store.clean(Some(0)).unwrap(), 0);
        // A delayed report for this exact cancelled attempt confirms stopping.
        let mut result = TaskResult {
            id: task.id,
            state: TaskState::Cancel,
            exit_code: None,
            error: None,
            attempt: claimed.attempt,
            lease_id: Some("wrong-lease".into()),
        };
        assert!(!store.complete(&result).unwrap().unwrap().cancel_confirmed);
        result.lease_id = claimed.lease_id;
        let confirmed = store.complete(&result).unwrap().unwrap();
        assert!(confirmed.cancel_confirmed);
        assert_eq!(
            confirmed.completion_reason.as_deref(),
            Some("node_completion")
        );
        drop(store);
        let reopened = Store::open(&root.join("tri.db"), &root.join("data")).unwrap();
        assert!(reopened.get(task.id).unwrap().unwrap().cancel_confirmed);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn dependencies_wait_for_success() {
        let root = std::env::temp_dir().join(format!("tri-dependency-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = Store::open(&root.join("tri.db"), &root.join("data")).unwrap();
        let first = store
            .submit(TaskSpec {
                name: "first".into(),
                command: vec!["true".into()],
                external_id: None,
                timeout_seconds: None,
                requested_cpu: 1,
                requested_mem_bytes: 0,
                requested_gpus: 0,
                scratch_cwd: false,
                requested_node: None,
                cwd: None,
                env: Vec::new(),
                output_path: None,
                error_path: None,
                depends_on: Vec::new(),
                max_attempts: 1,
            })
            .unwrap();
        let second = store
            .submit(TaskSpec {
                name: "second".into(),
                command: vec!["true".into()],
                external_id: None,
                timeout_seconds: None,
                requested_cpu: 1,
                requested_mem_bytes: 0,
                requested_gpus: 0,
                scratch_cwd: false,
                requested_node: None,
                cwd: None,
                env: Vec::new(),
                output_path: None,
                error_path: None,
                depends_on: vec![first.id],
                max_attempts: 1,
            })
            .unwrap();
        assert_eq!(
            store.dependency_status(&second).unwrap(),
            DependencyStatus::Waiting
        );
        let claimed = store.claim(first.id, "fed1", "0-0", "").unwrap().unwrap();
        store
            .complete(&TaskResult {
                id: first.id,
                state: TaskState::Done,
                exit_code: Some(0),
                error: None,
                attempt: claimed.attempt,
                lease_id: claimed.lease_id,
            })
            .unwrap();
        assert_eq!(
            store.dependency_status(&second).unwrap(),
            DependencyStatus::Ready
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn expired_lease_retains_allocation_until_confirmed_stopped() {
        let root = std::env::temp_dir().join(format!("tri-lease-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = Store::open(&root.join("tri.db"), &root.join("data")).unwrap();
        let lost = store
            .submit(TaskSpec {
                name: "lost".into(),
                command: vec!["true".into()],
                external_id: None,
                timeout_seconds: None,
                requested_cpu: 1,
                requested_mem_bytes: 0,
                requested_gpus: 0,
                scratch_cwd: false,
                requested_node: None,
                cwd: None,
                env: Vec::new(),
                output_path: None,
                error_path: None,
                depends_on: Vec::new(),
                max_attempts: 1,
            })
            .unwrap();
        store.claim(lost.id, "fed1", "0-0", "").unwrap();
        store.reap_expired(crate::now_seconds() + 100).unwrap();
        let unresolved = store.get(lost.id).unwrap().unwrap();
        assert_eq!(unresolved.state, TaskState::Unknown);
        assert!(unresolved.reserves_resources());
        assert!(store.claim(lost.id, "fed2", "0-0", "").unwrap().is_none());
        assert!(
            store
                .confirm_stopped(lost.id, 2, "node powered off", false)
                .is_err()
        );
        assert!(store.confirm_stopped(lost.id, 1, " ", false).is_err());
        assert!(
            store
                .confirm_stopped(lost.id, 1, "node powered off", true)
                .is_err()
        );
        let resolved = store
            .confirm_stopped(lost.id, 1, "node powered off", false)
            .unwrap();
        assert_eq!(resolved.state, TaskState::Lost);
        assert!(!resolved.reserves_resources());

        let retry = store
            .submit(TaskSpec {
                name: "retry".into(),
                command: vec!["true".into()],
                external_id: None,
                timeout_seconds: None,
                requested_cpu: 1,
                requested_mem_bytes: 0,
                requested_gpus: 0,
                scratch_cwd: false,
                requested_node: None,
                cwd: None,
                env: Vec::new(),
                output_path: None,
                error_path: None,
                depends_on: Vec::new(),
                max_attempts: 2,
            })
            .unwrap();
        store.claim(retry.id, "fed1", "0-0", "").unwrap();
        store.reap_expired(crate::now_seconds() + 100).unwrap();
        let retry = store.get(retry.id).unwrap().unwrap();
        assert_eq!(retry.state, TaskState::Unknown);
        assert!(retry.reserves_resources());
        let retry = store
            .confirm_stopped(retry.id, 1, "node powered off", true)
            .unwrap();
        assert_eq!(retry.state, TaskState::Wait);
        assert_eq!(retry.attempt, 1);
        // Cancelling a requeued task before its next claim must not rewrite
        // the previous terminal attempt with this queue-only cancellation.
        let before = serde_json::to_value(store.history(retry.id).unwrap().attempts).unwrap();
        store.cancel(retry.id).unwrap();
        let after = serde_json::to_value(store.history(retry.id).unwrap().attempts).unwrap();
        assert_eq!(before, after);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn matching_lease_can_recover_the_same_attempt() {
        let root = std::env::temp_dir().join(format!("tri-renew-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = Store::open(&root.join("tri.db"), &root.join("data")).unwrap();
        let task = store
            .submit(TaskSpec {
                name: "renew".into(),
                command: vec!["true".into()],
                external_id: None,
                timeout_seconds: None,
                requested_cpu: 1,
                requested_mem_bytes: 0,
                requested_gpus: 0,
                scratch_cwd: false,
                requested_node: None,
                cwd: None,
                env: Vec::new(),
                output_path: None,
                error_path: None,
                depends_on: Vec::new(),
                max_attempts: 1,
            })
            .unwrap();
        let claimed = store.claim(task.id, "fed1", "0-0", "").unwrap().unwrap();
        {
            let conn = store.conn.lock().unwrap();
            conn.execute(
                "UPDATE tasks SET lease_expires_at=?1 WHERE id=?2",
                params![crate::now_seconds().saturating_sub(1).to_string(), task.id],
            )
            .unwrap();
        }
        assert!(!store.renew_lease(task.id, "wrong-lease").unwrap());
        assert_eq!(
            store.get(task.id).unwrap().unwrap().state,
            TaskState::Unknown
        );
        assert!(
            store
                .renew_lease(task.id, claimed.lease_id.as_deref().unwrap())
                .unwrap()
        );
        let recovered = store.get(task.id).unwrap().unwrap();
        assert_eq!(recovered.state, TaskState::Run);
        assert_eq!(recovered.attempt, claimed.attempt);
        assert_eq!(recovered.lease_id, claimed.lease_id);
        assert!(!recovered.recovery_pending);
        assert!(
            store
                .confirm_stopped(task.id, 1, "obsolete observation", true)
                .is_err()
        );
        let count = store.history(task.id).unwrap().events.len();
        store
            .renew_lease(task.id, claimed.lease_id.as_deref().unwrap())
            .unwrap();
        assert_eq!(store.history(task.id).unwrap().events.len(), count);
        let history = store.history(task.id).unwrap();
        assert!(history.events.iter().any(|e| e.kind == "lease_expired"));
        assert_eq!(history.events.last().unwrap().kind, "recovered");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn stale_completion_does_not_finish_retried_attempt() {
        let root =
            std::env::temp_dir().join(format!("tri-stale-complete-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = Store::open(&root.join("tri.db"), &root.join("data")).unwrap();
        let task = store
            .submit(TaskSpec {
                name: "stale".into(),
                command: vec!["true".into()],
                external_id: None,
                timeout_seconds: None,
                requested_cpu: 1,
                requested_mem_bytes: 0,
                requested_gpus: 0,
                scratch_cwd: false,
                requested_node: None,
                cwd: None,
                env: Vec::new(),
                output_path: None,
                error_path: None,
                depends_on: Vec::new(),
                max_attempts: 2,
            })
            .unwrap();
        let first = store.claim(task.id, "fed1", "0-0", "").unwrap().unwrap();
        store.reap_expired(crate::now_seconds() + 100).unwrap();
        assert!(store.claim(task.id, "fed2", "0-0", "").unwrap().is_none());
        store
            .confirm_stopped(
                task.id,
                first.attempt,
                "old node fenced and powered off",
                true,
            )
            .unwrap();
        let second = store.claim(task.id, "fed2", "0-0", "").unwrap().unwrap();
        store
            .complete(&TaskResult {
                id: first.id,
                state: TaskState::Done,
                exit_code: Some(0),
                error: None,
                attempt: first.attempt,
                lease_id: first.lease_id.clone(),
            })
            .unwrap();
        let current = store.get(second.id).unwrap().unwrap();
        assert_eq!(current.state, TaskState::Run);
        assert_eq!(current.attempt, second.attempt);
        assert_eq!(current.lease_id, second.lease_id);
        let history = store.history(task.id).unwrap();
        assert_eq!(history.attempts.len(), 2);
        assert_eq!(history.attempts[0].state, TaskState::Lost);
        assert_eq!(history.attempts[0].node.as_deref(), Some("fed1"));
        assert_eq!(history.attempts[0].lease_id, first.lease_id);
        assert_eq!(
            history.attempts[0].error.as_deref(),
            Some("old node fenced and powered off")
        );
        assert_eq!(history.attempts[1].state, TaskState::Run);
        assert_eq!(
            history
                .events
                .iter()
                .map(|e| e.kind.as_str())
                .collect::<Vec<_>>(),
            [
                "submitted",
                "claimed",
                "lease_expired",
                "confirmed_stopped",
                "requeued",
                "claimed"
            ]
        );
        let serialized = serde_json::to_value(&history).unwrap();
        drop(store);
        let reopened = Store::open(&root.join("tri.db"), &root.join("data")).unwrap();
        assert_eq!(
            serde_json::to_value(reopened.history(task.id).unwrap()).unwrap(),
            serialized
        );
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
                external_id: None,
                timeout_seconds: None,
                requested_cpu: 1,
                requested_mem_bytes: 0,
                requested_gpus: 0,
                scratch_cwd: false,
                requested_node: None,
                cwd: Some(cwd.to_string_lossy().into_owned()),
                env: Vec::new(),
                output_path: None,
                error_path: None,
                depends_on: Vec::new(),
                max_attempts: 1,
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
                external_id: None,
                timeout_seconds: None,
                requested_cpu: 1,
                requested_mem_bytes: 0,
                requested_gpus: 0,
                scratch_cwd: false,
                requested_node: None,
                cwd: Some(cwd.to_string_lossy().into_owned()),
                env: Vec::new(),
                output_path: Some("logs/%j.out".into()),
                error_path: Some("errors/%j.err".into()),
                depends_on: Vec::new(),
                max_attempts: 1,
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

    #[test]
    fn submission_retries_are_idempotent_and_fingerprint_bound() {
        let root =
            std::env::temp_dir().join(format!("tri-idempotency-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = Store::open(&root.join("tri.db"), &root.join("data")).unwrap();
        let spec = TaskSpec {
            name: "idempotent".into(),
            command: vec!["true".into()],
            external_id: Some("workflow-step-a".into()),
            timeout_seconds: Some(60),
            requested_cpu: 1,
            requested_mem_bytes: 0,
            requested_gpus: 0,
            scratch_cwd: false,
            requested_node: None,
            cwd: None,
            env: Vec::new(),
            output_path: None,
            error_path: None,
            depends_on: Vec::new(),
            max_attempts: 1,
        };
        let (first, created) = store
            .submit_with_receipt(Some("request-1"), spec.clone())
            .unwrap();
        assert!(created);
        assert_eq!(
            first.spec.requested_mem_bytes,
            crate::resources::DEFAULT_MEMORY_BYTES
        );
        let (retry, created) = store
            .submit_with_receipt(Some("request-1"), spec.clone())
            .unwrap();
        assert!(!created);
        assert_eq!(retry.id, first.id);

        let (external_retry, created) = store
            .submit_with_receipt(Some("request-2"), spec.clone())
            .unwrap();
        assert!(!created);
        assert_eq!(external_retry.id, first.id);

        let mut changed = spec;
        changed.command = vec!["false".into()];
        let error = store
            .submit_with_receipt(Some("request-1"), changed)
            .unwrap_err()
            .to_string();
        assert!(error.contains("request_id_reused"));
        assert_eq!(store.list(None).unwrap().len(), 1);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn timeout_is_a_distinct_terminal_state() {
        let root = std::env::temp_dir().join(format!("tri-timeout-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = Store::open(&root.join("tri.db"), &root.join("data")).unwrap();
        let task = store
            .submit(TaskSpec {
                name: "timeout".into(),
                command: vec!["sleep".into(), "10".into()],
                external_id: None,
                timeout_seconds: Some(1),
                requested_cpu: 1,
                requested_mem_bytes: 0,
                requested_gpus: 0,
                scratch_cwd: false,
                requested_node: None,
                cwd: None,
                env: Vec::new(),
                output_path: None,
                error_path: None,
                depends_on: Vec::new(),
                max_attempts: 1,
            })
            .unwrap();
        let claimed = store.claim(task.id, "fed1", "0-0", "").unwrap().unwrap();
        let finished = store
            .complete(&TaskResult {
                id: task.id,
                state: TaskState::TimedOut,
                exit_code: None,
                error: Some("task exceeded RuntimeMaxSec".into()),
                attempt: claimed.attempt,
                lease_id: claimed.lease_id,
            })
            .unwrap()
            .unwrap();
        assert_eq!(finished.state, TaskState::TimedOut);
        assert_eq!(finished.spec.timeout_seconds, Some(1));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn clean_retains_recent_terminal_tasks_until_explicitly_requested() {
        let root = std::env::temp_dir().join(format!("tri-retention-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = Store::open(&root.join("tri.db"), &root.join("data")).unwrap();
        let task = store
            .submit(TaskSpec {
                name: "retention".into(),
                command: vec!["true".into()],
                external_id: None,
                timeout_seconds: None,
                requested_cpu: 1,
                requested_mem_bytes: 0,
                requested_gpus: 0,
                scratch_cwd: false,
                requested_node: None,
                cwd: None,
                env: Vec::new(),
                output_path: None,
                error_path: None,
                depends_on: Vec::new(),
                max_attempts: 1,
            })
            .unwrap();
        store.cancel(task.id).unwrap();
        assert_eq!(store.clean(Some(u64::MAX)).unwrap(), 0);
        assert_eq!(store.list(None).unwrap().len(), 1);
        assert_eq!(store.clean(None).unwrap(), 1);
        assert!(store.list(None).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(root);
    }
}
