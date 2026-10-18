//! Runs the binary against a real node named by `CQL_MIGRATE_TEST_NODE` (CI starts ScyllaDB and Cassandra 5.0).
//! `cargo test --test integration -- --ignored`

use std::path::Path;
use std::process::{Command, Output};

fn node() -> String {
    std::env::var("CQL_MIGRATE_TEST_NODE").expect("set CQL_MIGRATE_TEST_NODE=host:port")
}

fn run(dir: &Path, _ks: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cql-migrate"))
        .args(["--node", &node(), "--single-node", "--migrations"])
        .arg(dir.join("migrations"))
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap()
}

fn out(o: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    )
}

fn setup(ks: &str) -> tempfile::TempDir {
    drop_all(ks); // leftovers of an earlier failed run
    let d = tempfile::tempdir().unwrap();
    std::fs::create_dir(d.path().join("migrations")).unwrap();
    std::fs::write(
        d.path().join("cql-migrate.toml"),
        format!("history-keyspace = \"hist_{ks}\"\nkeyspaces = [\"{ks}\"]\n"),
    )
    .unwrap();
    d
}

fn mig(d: &Path, name: &str, text: &str) {
    std::fs::write(d.join("migrations").join(name), text).unwrap();
}

/// Runs a statement through `cqlsh` inside the database container (`CQL_MIGRATE_CONTAINER`, default `db`).
fn sh(stmt: &str) -> bool {
    let c = std::env::var("CQL_MIGRATE_CONTAINER").unwrap_or_else(|_| "db".into());
    Command::new("docker")
        .args(["exec", &c, "cqlsh", "-e", stmt])
        .output()
        .is_ok_and(|o| o.status.success())
}

fn drop_all(ks: &str) {
    sh(&format!("DROP KEYSPACE IF EXISTS {ks}"));
    sh(&format!("DROP KEYSPACE IF EXISTS hist_{ks}"));
}

#[test]
#[ignore = "needs a database (CQL_MIGRATE_TEST_NODE)"]
fn lifecycle() {
    let ks = "mig_life";
    let d = setup(ks);
    let p = d.path();
    mig(
        p,
        "0001_keyspace.cql",
        &format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}};"
        ),
    );
    mig(
        p,
        "0002_users.cql",
        &format!("CREATE TABLE IF NOT EXISTS {ks}.users (id uuid PRIMARY KEY, name text);"),
    );

    let s = run(p, ks, &["status"]);
    assert!(out(&s).contains("pending"), "{}", out(&s));

    let dry = run(p, ks, &["up", "--dry-run"]);
    assert!(
        out(&dry).contains("CREATE TABLE IF NOT EXISTS"),
        "{}",
        out(&dry)
    );
    assert!(out(&run(p, ks, &["status"])).contains("2 pending"));

    let up = run(p, ks, &["up"]);
    assert!(up.status.success(), "{}", out(&up));
    assert!(out(&up).contains("applied 0002_users"));
    let again = run(p, ks, &["up"]);
    assert!(out(&again).contains("Nothing to do"), "{}", out(&again));

    mig(
        p,
        "0003_email.cql",
        &format!("ALTER TABLE {ks}.users ADD email text;"),
    );
    assert!(run(p, ks, &["up"]).status.success());
    let st = run(p, ks, &["status"]);
    assert!(
        st.status.success() && out(&st).contains("0 pending"),
        "{}",
        out(&st)
    );

    // editing an applied migration is refused, but comments/formatting do not count
    mig(
        p,
        "0002_users.cql",
        &format!(
            "-- users\ncreate table if not exists {ks}.users (\n  id uuid primary key, name text);"
        ),
    );
    assert!(run(p, ks, &["status"]).status.success());
    mig(
        p,
        "0002_users.cql",
        &format!(
            "CREATE TABLE IF NOT EXISTS {ks}.users (id uuid PRIMARY KEY, name text, extra int);"
        ),
    );
    let bad = run(p, ks, &["up"]);
    assert_eq!(bad.status.code(), Some(2));
    assert!(
        out(&bad).contains("was changed after it was applied"),
        "{}",
        out(&bad)
    );
    assert_eq!(run(p, ks, &["status"]).status.code(), Some(1));
    mig(
        p,
        "0002_users.cql",
        &format!("CREATE TABLE IF NOT EXISTS {ks}.users (id uuid PRIMARY KEY, name text);"),
    );

    // drift: the cluster matches, then somebody changes it by hand
    let ok = run(p, ks, &["drift"]);
    assert!(ok.status.success(), "{}", out(&ok));
    assert!(
        sh(&format!("ALTER TABLE {ks}.users ADD sneaky int")),
        "cqlsh in the database container failed"
    );
    let dr = run(p, ks, &["drift"]);
    assert_eq!(dr.status.code(), Some(1), "{}", out(&dr));
    assert!(out(&dr).contains("sneaky"), "{}", out(&dr));
    drop_all(ks);
}

#[test]
#[ignore = "needs a database (CQL_MIGRATE_TEST_NODE)"]
fn failure_is_recorded_and_resolvable() {
    let ks = "mig_fail";
    let d = setup(ks);
    let p = d.path();
    mig(
        p,
        "0001_keyspace.cql",
        &format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}};"
        ),
    );
    // valid for the offline replay, rejected by the server (unknown compaction option value)
    mig(
        p,
        "0002_bad.cql",
        &format!(
            "CREATE TABLE IF NOT EXISTS {ks}.t (id int PRIMARY KEY);\nCREATE TABLE {ks}.t (id int PRIMARY KEY);"
        ),
    );
    let up = run(p, ks, &["up", "--no-verify"]);
    assert_eq!(up.status.code(), Some(2), "{}", out(&up));
    assert!(
        out(&up).contains("failed at statement 2 of 2"),
        "{}",
        out(&up)
    );
    let st = run(p, ks, &["status"]);
    assert!(out(&st).contains("FAILED"), "{}", out(&st));
    let blocked = run(p, ks, &["up", "--no-verify"]);
    assert!(
        out(&blocked).contains("failed earlier"),
        "{}",
        out(&blocked)
    );
    // fix the migration (idempotent now), forget the failure, run again
    mig(
        p,
        "0002_bad.cql",
        &format!("CREATE TABLE IF NOT EXISTS {ks}.t (id int PRIMARY KEY);"),
    );
    assert!(run(p, ks, &["resolve", "2", "--retry"]).status.success());
    let ok = run(p, ks, &["up"]);
    assert!(ok.status.success(), "{}", out(&ok));
    drop_all(ks);
}

#[test]
#[ignore = "needs a database (CQL_MIGRATE_TEST_NODE)"]
fn lock_blocks_a_second_runner() {
    let ks = "mig_lock";
    let d = setup(ks);
    let p = d.path();
    mig(
        p,
        "0001_keyspace.cql",
        &format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}};"
        ),
    );
    mig(
        p,
        "0002_t.cql",
        &format!("CREATE TABLE IF NOT EXISTS {ks}.t (id int PRIMARY KEY);"),
    );
    assert!(run(p, ks, &["status"]).status.success());
    assert!(sh(&format!(
        "INSERT INTO hist_{ks}.lock (id, owner, since) VALUES ('migrate', 'someone-else', toTimestamp(now())) USING TTL 120"
    )));
    let blocked = run(p, ks, &["up"]);
    assert_eq!(blocked.status.code(), Some(2), "{}", out(&blocked));
    assert!(
        out(&blocked).contains("holds the lock"),
        "{}",
        out(&blocked)
    );
    assert!(out(&blocked).contains("someone-else"));
    drop_all(ks);
}
