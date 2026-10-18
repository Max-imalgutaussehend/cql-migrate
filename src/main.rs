use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use cql_migrate::config::{self, Config};
use cql_migrate::migration::{self, Migration};
use cql_migrate::model::{self, Change};
use cql_migrate::store::{Record, Store};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

/// Schema migrations for Cassandra and ScyllaDB.
#[derive(Parser)]
#[command(version, about, propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
    /// Config file (default: ./cql-migrate.toml if present).
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    /// Contact point, `host:port`.
    #[arg(long, global = true, env = "CQL_NODE")]
    node: Option<String>,
    /// Rewrite every address the cluster advertises to the contact point (Docker, tunnels).
    #[arg(long, global = true)]
    single_node: bool,
    /// Directory with the migration files.
    #[arg(long, global = true)]
    migrations: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create the next migration file.
    New { name: String },
    /// Show which migrations are applied, pending, changed or failed.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Apply pending migrations in order.
    Up {
        /// Stop after this version.
        #[arg(long)]
        to: Option<u32>,
        /// Print what would run, change nothing.
        #[arg(long)]
        dry_run: bool,
        /// Allow a pending migration with a lower version than an applied one.
        #[arg(long)]
        allow_out_of_order: bool,
        /// Skip the offline check of the migration chain.
        #[arg(long)]
        no_verify: bool,
    },
    /// Replay all migrations offline with cqlc-core and report schema errors (no database needed).
    Verify,
    /// Compare the schema the migrations produce with a desired schema (files or directories); exit 1 if they differ.
    Diff {
        /// Desired schema: `.cql` files or directories.
        #[arg(required = true)]
        schema: Vec<PathBuf>,
        /// Write the difference as the next migration with this name.
        #[arg(long)]
        write: Option<String>,
        /// Emit DROP statements instead of commented suggestions.
        #[arg(long)]
        allow_destructive: bool,
    },
    /// Compare the live cluster with the schema the migrations produce; exit 1 on drift.
    Drift,
    /// Fix up a failed or interrupted migration: record it as applied, or forget it so `up` retries it.
    Resolve {
        #[arg(value_name = "VERSION")]
        number: u32,
        #[arg(long, conflicts_with = "retry")]
        applied: bool,
        #[arg(long)]
        retry: bool,
    },
}

fn main() -> ExitCode {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    match rt.block_on(run()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("cql-migrate: {e:#}");
            ExitCode::from(2)
        }
    }
}

fn load_config(cli: &Cli) -> Result<Config> {
    let mut c = match &cli.config {
        Some(p) => config::load(p)?,
        None => config::discover()?,
    };
    if let Some(n) = &cli.node {
        c.node = n.clone();
    }
    if cli.single_node {
        c.single_node = true;
    }
    if let Some(m) = &cli.migrations {
        c.migrations = m.clone();
    }
    Ok(c)
}

async fn connect(cfg: &Config) -> Result<Store> {
    let user = match (std::env::var("CQL_USER"), std::env::var("CQL_PASSWORD")) {
        (Ok(u), Ok(p)) => Some((u, p)),
        _ => None,
    };
    let s = Store::connect(cfg, user).await?;
    s.init(&cfg.history_replication).await?;
    Ok(s)
}

#[derive(Debug, PartialEq, Clone, Copy)]
enum State {
    Pending,
    Applied,
    Changed,
    Failed,
    Running,
}

impl State {
    fn label(self) -> &'static str {
        match self {
            State::Pending => "pending",
            State::Applied => "applied",
            State::Changed => "CHANGED",
            State::Failed => "FAILED",
            State::Running => "RUNNING",
        }
    }
}

fn state_of(m: &Migration, r: Option<&Record>) -> State {
    match r {
        None => State::Pending,
        Some(r) if r.state == "failed" => State::Failed,
        Some(r) if r.state == "running" => State::Running,
        Some(r) if r.checksum != m.checksum => State::Changed,
        Some(_) => State::Applied,
    }
}

fn fmt_time(ms: Option<i64>) -> String {
    let Some(ms) = ms else { return "-".into() };
    let s = ms.div_euclid(1000);
    let (days, rem) = (s.div_euclid(86400), s.rem_euclid(86400));
    // civil-from-days (Howard Hinnant)
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

async fn run() -> Result<ExitCode> {
    let cli = Cli::parse();
    let cfg = load_config(&cli)?;
    match cli.cmd {
        Cmd::New { name } => {
            std::fs::create_dir_all(&cfg.migrations)?;
            let p = migration::next_file(&cfg.migrations, &name)?;
            std::fs::write(
                &p,
                format!(
                    "-- {name}\n-- Use IF NOT EXISTS / IF EXISTS so a half-applied migration can be retried.\n\n"
                ),
            )?;
            println!("created {}", p.display());
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Verify => {
            let ms = migration::load_dir(&cfg.migrations)?;
            let model = model::replay(&ms);
            if model.problems.is_empty() {
                println!(
                    "{} migration(s) replay cleanly: {} table(s), {} type(s).",
                    ms.len(),
                    model.schema.tables.len(),
                    model.schema.udts.len()
                );
                return Ok(ExitCode::SUCCESS);
            }
            for (f, m) in &model.problems {
                println!("{f}: {m}");
            }
            Ok(ExitCode::from(1))
        }
        Cmd::Diff {
            schema,
            write,
            allow_destructive,
        } => {
            let ms = migration::load_dir(&cfg.migrations).unwrap_or_default();
            let have = model::replay(&ms);
            let mut files = Vec::new();
            for p in &schema {
                collect_cql(p, &mut files)?;
            }
            if files.is_empty() {
                bail!("no .cql files found in the desired schema paths");
            }
            let want = model::schema_of_files(&files);
            for (f, m) in &want.problems {
                eprintln!("warning: {f}: {m}");
            }
            let changes = model::diff(&have.schema, &want.schema);
            if changes.is_empty() {
                println!("The migrations produce exactly the desired schema.");
                return Ok(ExitCode::SUCCESS);
            }
            for c in &changes {
                println!("{}", describe(c));
            }
            let sql = model::render(&changes, &want.schema, &files, allow_destructive);
            match write {
                Some(name) => {
                    std::fs::create_dir_all(&cfg.migrations)?;
                    let p = migration::next_file(&cfg.migrations, &name)?;
                    std::fs::write(
                        &p,
                        format!(
                            "-- generated by `cql-migrate diff`; review before applying\n\n{sql}"
                        ),
                    )?;
                    println!("\nwrote {}", p.display());
                }
                None => println!("\n{sql}"),
            }
            Ok(ExitCode::from(1))
        }
        Cmd::Status { json } => {
            let ms = migration::load_dir(&cfg.migrations)?;
            let store = connect(&cfg).await?;
            let recs = store.records().await?;
            let mut rows = Vec::new();
            let mut bad = false;
            for m in &ms {
                let st = state_of(m, recs.get(&m.version));
                bad |= st != State::Pending && st != State::Applied;
                let r = recs.get(&m.version);
                rows.push((
                    m.version,
                    m.name.clone(),
                    st.label().to_string(),
                    r.and_then(|r| r.applied_at_ms),
                    r.and_then(|r| r.execution_ms),
                    r.and_then(|r| r.error.clone()),
                ));
            }
            let have: BTreeSet<u32> = ms.iter().map(|m| m.version).collect();
            for (v, r) in &recs {
                if !have.contains(v) {
                    bad = true;
                    rows.push((
                        *v,
                        r.name.clone(),
                        "MISSING".into(),
                        r.applied_at_ms,
                        r.execution_ms,
                        None,
                    ));
                }
            }
            rows.sort_by_key(|r| r.0);
            if json {
                let v: Vec<_> = rows.iter().map(|r| serde_json::json!({"version": r.0, "name": r.1, "state": r.2, "applied_at_ms": r.3, "execution_ms": r.4, "error": r.5})).collect();
                println!("{}", serde_json::to_string_pretty(&v)?);
            } else {
                println!(
                    "{:<8} {:<32} {:<8} {:<20} TIME",
                    "VERSION", "NAME", "STATE", "APPLIED"
                );
                for r in &rows {
                    println!(
                        "{:<8} {:<32} {:<8} {:<20} {}",
                        format!("{:04}", r.0),
                        r.1,
                        r.2,
                        fmt_time(r.3),
                        r.4.map_or("-".into(), |m| format!("{m} ms"))
                    );
                    if let Some(e) = &r.5 {
                        println!("         error: {e}");
                    }
                }
                let pending = rows.iter().filter(|r| r.2 == "pending").count();
                println!(
                    "\n{} migration(s), {pending} pending{}",
                    rows.len(),
                    if bad {
                        ", PROBLEMS FOUND (CHANGED / FAILED / RUNNING / MISSING)"
                    } else {
                        ""
                    }
                );
            }
            Ok(if bad {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            })
        }
        Cmd::Up {
            to,
            dry_run,
            allow_out_of_order,
            no_verify,
        } => {
            let ms = migration::load_dir(&cfg.migrations)?;
            if !no_verify {
                let model = model::replay(&ms);
                if !model.problems.is_empty() {
                    for (f, m) in &model.problems {
                        eprintln!("{f}: {m}");
                    }
                    bail!(
                        "the migration chain does not replay cleanly (use --no-verify to skip this check)"
                    );
                }
            }
            let store = connect(&cfg).await?;
            let recs = store.records().await?;
            for m in &ms {
                match state_of(m, recs.get(&m.version)) {
                    State::Changed => bail!(
                        "migration {:04}_{} was changed after it was applied (checksum differs). Never edit applied migrations; add a new one.",
                        m.version,
                        m.name
                    ),
                    State::Failed => bail!(
                        "migration {:04}_{} failed earlier: {}. Fix it, then `cql-migrate resolve {} --retry` (or `--applied`).",
                        m.version,
                        m.name,
                        recs[&m.version].error.clone().unwrap_or_default(),
                        m.version
                    ),
                    State::Running => bail!(
                        "migration {:04}_{} is marked as running: a previous run was interrupted. Check the schema, then `cql-migrate resolve {} --retry` or `--applied`.",
                        m.version,
                        m.name,
                        m.version
                    ),
                    _ => {}
                }
            }
            let max_applied = recs
                .values()
                .filter(|r| r.state == "applied")
                .map(|r| r.version)
                .max()
                .unwrap_or(0);
            let pending: Vec<&Migration> = ms
                .iter()
                .filter(|m| !recs.contains_key(&m.version) && to.is_none_or(|t| m.version <= t))
                .collect();
            if let Some(m) = pending.iter().find(|m| m.version < max_applied)
                && !allow_out_of_order
            {
                bail!(
                    "migration {:04}_{} is older than the newest applied one ({max_applied:04}); use --allow-out-of-order to apply it anyway",
                    m.version,
                    m.name
                );
            }
            if pending.is_empty() {
                println!("Nothing to do: {} migration(s) applied.", recs.len());
                return Ok(ExitCode::SUCCESS);
            }
            if dry_run {
                for m in &pending {
                    println!(
                        "-- {:04}_{} ({} statement(s))",
                        m.version,
                        m.name,
                        m.statements.len()
                    );
                    for s in &m.statements {
                        println!("{s};");
                    }
                    println!();
                }
                return Ok(ExitCode::SUCCESS);
            }
            let owner = format!(
                "{}@{}",
                std::env::var("USER").unwrap_or_else(|_| "unknown".into()),
                std::process::id()
            );
            store.lock(&owner, cfg.lock_ttl).await?;
            let mut result = Ok(());
            for m in pending {
                let t = Instant::now();
                store.mark_running(m.version, &m.name, &m.checksum).await?;
                match store.apply(&m.statements).await {
                    Ok(()) => {
                        let ms = t.elapsed().as_millis() as i64;
                        store.mark_applied(m.version, ms).await?;
                        println!(
                            "applied {:04}_{} ({} statement(s), {ms} ms)",
                            m.version,
                            m.name,
                            m.statements.len()
                        );
                    }
                    Err((i, e)) => {
                        let msg = format!("statement {} of {}: {e}", i + 1, m.statements.len());
                        store.mark_failed(m.version, &msg).await?;
                        result = Err(anyhow::anyhow!(
                            "migration {:04}_{} failed at {msg}\nstatements before it were applied; DDL is not transactional. Make the statements idempotent, fix the problem, then `cql-migrate resolve {} --retry`",
                            m.version,
                            m.name,
                            m.version
                        ));
                        break;
                    }
                }
            }
            store.unlock().await.ok();
            result?;
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Drift => {
            let ms = migration::load_dir(&cfg.migrations)?;
            let want = model::replay(&ms);
            let model_cols = model::columns_of(&want.schema);
            let keyspaces: Vec<String> = if cfg.keyspaces.is_empty() {
                let set: BTreeSet<String> = model_cols
                    .keys()
                    .filter_map(|k| k.split_once('.').map(|(ks, _)| ks.to_string()))
                    .collect();
                set.into_iter().collect()
            } else {
                cfg.keyspaces.clone()
            };
            if keyspaces.is_empty() {
                bail!(
                    "the migrations define no keyspace-qualified tables; set `keyspaces` in cql-migrate.toml"
                );
            }
            let store = connect(&cfg).await?;
            let live = store.live_columns(&keyspaces).await?;
            let in_scope = |t: &str| {
                keyspaces
                    .iter()
                    .any(|k| t.starts_with(&format!("{}.", k.to_ascii_lowercase())))
            };
            let mut drift = Vec::new();
            for (t, cols) in &model_cols {
                if !in_scope(t) {
                    continue;
                }
                match live.get(t) {
                    None => drift.push(format!(
                        "table {t} is in the migrations but not in the cluster"
                    )),
                    Some(lc) => {
                        for (c, ty) in cols {
                            match lc.get(c) {
                                None => drift.push(format!(
                                    "{t}.{c} ({ty}) is in the migrations but not in the cluster"
                                )),
                                Some(l) if l != ty => drift.push(format!(
                                    "{t}.{c} is {l} in the cluster but {ty} in the migrations"
                                )),
                                _ => {}
                            }
                        }
                        for c in lc.keys() {
                            if !cols.contains_key(c) {
                                drift.push(format!(
                                    "{t}.{c} is in the cluster but not in the migrations"
                                ));
                            }
                        }
                    }
                }
            }
            for t in live.keys() {
                if !model_cols.contains_key(t) {
                    drift.push(format!(
                        "table {t} is in the cluster but not in the migrations"
                    ));
                }
            }
            if drift.is_empty() {
                println!(
                    "No drift: the cluster matches the migrations ({} table(s) in {}).",
                    live.len(),
                    keyspaces.join(", ")
                );
                Ok(ExitCode::SUCCESS)
            } else {
                for d in &drift {
                    println!("drift: {d}");
                }
                Ok(ExitCode::from(1))
            }
        }
        Cmd::Resolve {
            number: version,
            applied,
            retry,
        } => {
            if !applied && !retry {
                bail!("choose --applied (the schema change is in place) or --retry (run it again)");
            }
            let store = connect(&cfg).await?;
            let recs = store.records().await?;
            let r = recs
                .get(&version)
                .with_context(|| format!("version {version} is not in the history"))?;
            if r.state == "applied" {
                bail!("version {version} is already applied");
            }
            if retry {
                store.forget(version).await?;
                println!("forgot {version:04}; `cql-migrate up` will run it again.");
            } else {
                store.mark_applied(version, 0).await?;
                println!("recorded {version:04} as applied.");
            }
            Ok(ExitCode::SUCCESS)
        }
    }
}

fn describe(c: &Change) -> String {
    match c {
        Change::CreateTable { table } => format!("+ table {table}"),
        Change::DropTable { table } => format!("- table {table}"),
        Change::AddColumn { table, column, ty } => format!("+ {table}.{column} {ty}"),
        Change::DropColumn { table, column } => format!("- {table}.{column}"),
        Change::TypeChanged {
            table,
            column,
            from,
            to,
        } => format!("! {table}.{column}: {from} -> {to} (cannot be altered in place)"),
        Change::KeyChanged { table, .. } => {
            format!("! primary key of {table} changed (cannot be altered in place)")
        }
    }
}

fn collect_cql(p: &std::path::Path, out: &mut Vec<(String, String)>) -> Result<()> {
    if p.is_file() {
        out.push((
            p.display().to_string(),
            std::fs::read_to_string(p).with_context(|| format!("reading {}", p.display()))?,
        ));
    } else if p.is_dir() {
        let mut entries: Vec<_> = std::fs::read_dir(p)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect();
        entries.sort();
        for e in entries {
            if e.is_dir() || e.extension().is_some_and(|x| x == "cql") {
                collect_cql(&e, out)?;
            }
        }
    } else {
        bail!("{} does not exist", p.display());
    }
    Ok(())
}
