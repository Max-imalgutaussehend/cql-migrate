//! History and lock tables, statement execution and live schema reading.

use crate::config::Config;
use anyhow::{Context, Result, bail};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::TranslationError;
use scylla::policies::address_translator::{AddressTranslator, UntranslatedPeer};
use scylla::value::{CqlTimestamp, CqlValue, Row};
use std::collections::BTreeMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

struct SingleNode(SocketAddr);

#[async_trait::async_trait]
impl AddressTranslator for SingleNode {
    async fn translate_address(
        &self,
        _: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        Ok(self.0)
    }
}

pub struct Store {
    pub session: Session,
    ks: String,
}

#[derive(Clone, Debug)]
pub struct Record {
    pub version: u32,
    pub name: String,
    pub checksum: String,
    /// `running`, `applied` or `failed`.
    pub state: String,
    pub applied_at_ms: Option<i64>,
    pub execution_ms: Option<i64>,
    pub error: Option<String>,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

fn quote(s: &str) -> String {
    s.replace('\'', "''")
}

impl Store {
    pub async fn connect(cfg: &Config, user: Option<(String, String)>) -> Result<Store> {
        let addr = cfg
            .node
            .to_socket_addrs()
            .with_context(|| format!("resolving {}", cfg.node))?
            .next()
            .with_context(|| format!("{} did not resolve", cfg.node))?;
        let mut b = SessionBuilder::new().known_node_addr(addr);
        if cfg.single_node {
            b = b.address_translator(Arc::new(SingleNode(addr)));
        }
        if let Some((u, p)) = user {
            b = b.user(u, p);
        }
        let session = b
            .build()
            .await
            .with_context(|| format!("connecting to {}", cfg.node))?;
        Ok(Store {
            session,
            ks: cfg.history_keyspace.clone(),
        })
    }

    /// Creates the history keyspace and tables if they do not exist.
    pub async fn init(&self, replication: &str) -> Result<()> {
        let ks = &self.ks;
        let ddl = [
            format!("CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = {replication}"),
            format!(
                "CREATE TABLE IF NOT EXISTS {ks}.history (version int PRIMARY KEY, name text, checksum text, state text, applied_at timestamp, execution_ms bigint, error text)"
            ),
            format!(
                "CREATE TABLE IF NOT EXISTS {ks}.lock (id text PRIMARY KEY, owner text, since timestamp)"
            ),
        ];
        for d in ddl {
            self.session.query_unpaged(d, &[]).await?;
            self.session.await_schema_agreement().await?;
        }
        Ok(())
    }

    pub async fn records(&self) -> Result<BTreeMap<u32, Record>> {
        let res = self.session.query_unpaged(format!("SELECT version, name, checksum, state, applied_at, execution_ms, error FROM {}.history", self.ks), &[]).await?;
        let rows = res.into_rows_result()?;
        let mut out = BTreeMap::new();
        for r in rows.rows::<(
            i32,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<CqlTimestamp>,
            Option<i64>,
            Option<String>,
        )>()? {
            let (v, name, checksum, state, at, ms, error) = r?;
            out.insert(
                v as u32,
                Record {
                    version: v as u32,
                    name: name.unwrap_or_default(),
                    checksum: checksum.unwrap_or_default(),
                    state: state.unwrap_or_default(),
                    applied_at_ms: at.map(|t| t.0),
                    execution_ms: ms,
                    error,
                },
            );
        }
        Ok(out)
    }

    pub async fn mark_running(&self, version: u32, name: &str, checksum: &str) -> Result<()> {
        let q = format!(
            "INSERT INTO {}.history (version, name, checksum, state, applied_at) VALUES (?, ?, ?, 'running', ?)",
            self.ks
        );
        self.session
            .query_unpaged(q, (version as i32, name, checksum, CqlTimestamp(now_ms())))
            .await?;
        Ok(())
    }

    pub async fn mark_applied(&self, version: u32, ms: i64) -> Result<()> {
        let q = format!(
            "UPDATE {}.history SET state = 'applied', applied_at = ?, execution_ms = ?, error = null WHERE version = ?",
            self.ks
        );
        self.session
            .query_unpaged(q, (CqlTimestamp(now_ms()), ms, version as i32))
            .await?;
        Ok(())
    }

    pub async fn mark_failed(&self, version: u32, error: &str) -> Result<()> {
        let q = format!(
            "UPDATE {}.history SET state = 'failed', error = ? WHERE version = ?",
            self.ks
        );
        self.session
            .query_unpaged(q, (error, version as i32))
            .await?;
        Ok(())
    }

    pub async fn forget(&self, version: u32) -> Result<()> {
        self.session
            .query_unpaged(
                format!("DELETE FROM {}.history WHERE version = ?", self.ks),
                (version as i32,),
            )
            .await?;
        Ok(())
    }

    /// Takes the cluster-wide lock (a lightweight transaction with a TTL, so a crashed runner cannot block forever).
    pub async fn lock(&self, owner: &str, ttl: u32) -> Result<()> {
        let q = format!(
            "INSERT INTO {}.lock (id, owner, since) VALUES ('migrate', '{}', ?) IF NOT EXISTS USING TTL {ttl}",
            self.ks,
            quote(owner)
        );
        let res = self
            .session
            .query_unpaged(q, (CqlTimestamp(now_ms()),))
            .await?;
        let rows = res.into_rows_result()?;
        // an LWT answers `[applied]` plus, when it was not applied, the existing row (id, owner, since)
        let row = rows
            .first_row::<Row>()
            .context("the lock statement returned no `[applied]` column")?;
        if matches!(row.columns.first(), Some(Some(CqlValue::Boolean(true)))) {
            return Ok(());
        }
        let owner_idx = rows.column_specs().iter().position(|c| c.name() == "owner");
        let holder = owner_idx.and_then(|i| match row.columns.get(i) {
            Some(Some(CqlValue::Text(t))) => Some(t.clone()),
            _ => None,
        });
        bail!(
            "another migration run holds the lock ({}); it expires after {ttl} s",
            holder.unwrap_or_else(|| "unknown".into())
        )
    }

    pub async fn unlock(&self) -> Result<()> {
        self.session
            .query_unpaged(
                format!("DELETE FROM {}.lock WHERE id = 'migrate'", self.ks),
                &[],
            )
            .await?;
        Ok(())
    }

    /// Runs the statements of a migration; DDL waits for schema agreement. Returns the index of the failing statement.
    pub async fn apply(
        &self,
        statements: &[String],
    ) -> std::result::Result<(), (usize, anyhow::Error)> {
        for (i, s) in statements.iter().enumerate() {
            self.session
                .query_unpaged(s.as_str(), &[])
                .await
                .map_err(|e| (i, anyhow::Error::from(e)))?;
            let head = s
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_ascii_uppercase();
            if matches!(head.as_str(), "CREATE" | "ALTER" | "DROP") {
                self.session
                    .await_schema_agreement()
                    .await
                    .map_err(|e| (i, anyhow::Error::from(e)))?;
            }
        }
        Ok(())
    }

    /// `keyspace.table -> column -> type` for `keyspaces`, read from `system_schema`.
    pub async fn live_columns(
        &self,
        keyspaces: &[String],
    ) -> Result<BTreeMap<String, BTreeMap<String, String>>> {
        let res = self
            .session
            .query_unpaged(
                "SELECT keyspace_name, table_name, column_name, type FROM system_schema.columns",
                &[],
            )
            .await?;
        let rows = res.into_rows_result()?;
        let mut out: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        for r in rows.rows::<(String, String, String, String)>()? {
            let (ks, t, c, ty) = r?;
            if keyspaces.iter().any(|k| k.eq_ignore_ascii_case(&ks)) {
                out.entry(format!("{ks}.{t}").to_ascii_lowercase())
                    .or_default()
                    .insert(c.to_ascii_lowercase(), crate::model::norm_type(&ty));
            }
        }
        // a table without regular columns still has key columns, so every table appears above
        Ok(out)
    }
}
