//! Retry must not act on an ownership snapshot that another connection replaced.
use crate::{attach_honker_functions, bootstrap_honker_schema, honker_ops};
use rusqlite::Connection;
use rusqlite::functions::FunctionFlags;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

fn connect(path: &std::path::Path, clock: &Arc<AtomicI64>) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=0;")
        .unwrap();
    attach_honker_functions(&conn).unwrap();
    bootstrap_honker_schema(&conn).unwrap();
    let clock = clock.clone();
    // Control only time, not ownership or results. Avoid a subsecond deadline
    // test that fails if a loaded CI runner pauses before entering retry.
    conn.create_scalar_function("unixepoch", 0, FunctionFlags::SQLITE_UTF8, move |_| {
        Ok(clock.load(Ordering::SeqCst))
    })
    .unwrap();
    conn
}

fn call_retry(conn: &Connection, id: i64, sql: bool) -> rusqlite::Result<i64> {
    if sql {
        conn.query_row("SELECT honker_retry(?1, 'old', 0, 'retry')", [id], |r| {
            r.get(0)
        })
    } else {
        honker_ops::retry(conn, id, "old", 0, "retry")
    }
}

fn is_mutation(ctx: AuthContext<'_>, exhausted: bool) -> bool {
    match ctx.action {
        AuthAction::Delete { table_name } => exhausted && table_name == "_honker_live",
        AuthAction::Update { table_name, .. } => !exhausted && table_name == "_honker_live",
        _ => false,
    }
}

fn interleave(exhausted: bool, sql: bool, outer: bool) {
    let dir = std::env::temp_dir().join(format!(
        "honker-retry-owner-{}-{}",
        std::process::id(),
        NEXT_DB.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("jobs.db");
    let clock = Arc::new(AtomicI64::new(1000));
    let a = connect(&path, &clock);
    let b = connect(&path, &clock);
    let id = honker_ops::enqueue(
        &a,
        "q",
        "{}",
        None,
        None,
        0,
        if exhausted { 1 } else { 3 },
        None,
    )
    .unwrap();
    honker_ops::claim_batch(&a, "q", "old", 1, 5).unwrap();
    if outer {
        a.execute_batch("BEGIN").unwrap();
    }
    let fired = Arc::new(AtomicBool::new(false));
    let hit = fired.clone();
    a.authorizer(Some(move |ctx: AuthContext<'_>| {
        if is_mutation(ctx, exhausted) && !hit.swap(true, Ordering::SeqCst) {
            // SQLite calls this while preparing retry's write, AFTER its SELECT.
            // WAL lets B commit despite A's older read snapshot. A must then
            // reject that snapshot, never update B's row from its cached values.
            if exhausted {
                assert_eq!(honker_ops::cancel(&b, id).unwrap(), 1);
            } else {
                clock.store(1006, Ordering::SeqCst);
                let claimed = honker_ops::claim_batch(&b, "q", "new", 1, 300).unwrap();
                let jobs: serde_json::Value = serde_json::from_str(&claimed).unwrap();
                assert_eq!(jobs[0]["worker_id"], "new");
            }
        }
        Authorization::Allow
    }))
    .unwrap();
    let result = call_retry(&a, id, sql);
    a.authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
        .unwrap();
    assert!(
        fired.load(Ordering::SeqCst),
        "must exercise the read/write gap"
    );
    let err = result.expect_err("a replaced WAL snapshot must not be written");
    assert!(
        err.to_string().contains("locked"),
        "expected snapshot conflict, got {err}"
    );
    assert_eq!(
        a.is_autocommit(),
        !outer,
        "retry must preserve transaction ownership"
    );
    if outer {
        a.execute_batch("ROLLBACK").unwrap();
    }
    let live = honker_ops::get_job(&a, id).unwrap();
    if exhausted {
        assert_eq!(live, "", "cancelled job must remain absent");
    } else {
        let row: serde_json::Value = serde_json::from_str(&live).unwrap();
        assert_eq!(row["state"], "processing");
        assert_eq!(row["worker_id"], "new");
        assert_eq!(row["attempts"], 2);
        assert_eq!(row["claimed_at"], 1006);
        assert_eq!(row["claim_expires_at"], 1306);
    }
    let dead: i64 = a
        .query_row("SELECT count(*) FROM _honker_dead", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        dead, 0,
        "a cancelled/reclaimed row must not become a dead row"
    );
    drop(a);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn retry_cannot_overwrite_another_connections_reclaim() {
    for sql in [false, true] {
        for outer in [false, true] {
            interleave(false, sql, outer);
        }
    }
}

#[test]
fn final_retry_cannot_resurrect_another_connections_cancel() {
    for sql in [false, true] {
        for outer in [false, true] {
            interleave(true, sql, outer);
        }
    }
}

#[test]
fn retry_rechecks_lease_after_read_for_both_branches() {
    for exhausted in [false, true] {
        let clock = Arc::new(AtomicI64::new(1000));
        let a = connect(std::path::Path::new(":memory:"), &clock);
        let id = honker_ops::enqueue(
            &a,
            "q",
            "{}",
            None,
            None,
            0,
            if exhausted { 1 } else { 3 },
            None,
        )
        .unwrap();
        honker_ops::claim_batch(&a, "q", "old", 1, 5).unwrap();
        let before = honker_ops::get_job(&a, id).unwrap();
        let hit = Arc::new(AtomicBool::new(false));
        let fired = hit.clone();
        a.authorizer(Some(move |ctx: AuthContext<'_>| {
            if is_mutation(ctx, exhausted) {
                fired.store(true, Ordering::SeqCst);
                clock.store(1006, Ordering::SeqCst);
            }
            Authorization::Allow
        }))
        .unwrap();
        assert_eq!(call_retry(&a, id, true).unwrap(), 0);
        a.authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
            .unwrap();
        assert!(hit.load(Ordering::SeqCst));
        assert_eq!(honker_ops::get_job(&a, id).unwrap(), before);
        let dead: i64 = a
            .query_row("SELECT count(*) FROM _honker_dead", [], |r| r.get(0))
            .unwrap();
        assert_eq!(dead, 0);
        assert!(a.is_autocommit());
    }
}

#[test]
fn failed_final_retry_preserves_callers_write_and_the_job() {
    let a = connect(
        std::path::Path::new(":memory:"),
        &Arc::new(AtomicI64::new(1000)),
    );
    let id = honker_ops::enqueue(&a, "q", "{}", None, None, 0, 1, None).unwrap();
    honker_ops::claim_batch(&a, "q", "old", 1, 300).unwrap();
    let before = honker_ops::get_job(&a, id).unwrap();
    a.execute_batch("CREATE TABLE app(x); CREATE TRIGGER reject_dead BEFORE INSERT ON _honker_dead BEGIN SELECT RAISE(ABORT, 'reject dead'); END; BEGIN; INSERT INTO app VALUES (42);").unwrap();
    let err = call_retry(&a, id, true).unwrap_err();
    assert!(err.to_string().contains("reject dead"), "{err}");
    assert!(!a.is_autocommit());
    assert_eq!(honker_ops::get_job(&a, id).unwrap(), before);
    a.execute_batch("COMMIT").unwrap();
    assert_eq!(
        a.query_row("SELECT x FROM app", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        42
    );
    assert_eq!(
        a.query_row("SELECT count(*) FROM _honker_dead", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
}
