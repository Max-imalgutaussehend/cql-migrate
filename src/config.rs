use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields, default)]
pub struct Config {
    /// `host:port` of a contact point.
    pub node: String,
    /// Rewrite every address the cluster advertises to `node` (Docker port mappings, SSH tunnels).
    pub single_node: bool,
    pub migrations: PathBuf,
    pub history_keyspace: String,
    /// CQL map literal used when the history keyspace has to be created.
    pub history_replication: String,
    /// Seconds a lock survives a crashed runner.
    pub lock_ttl: u32,
    /// Keyspaces `drift` compares (default: all keyspaces created by the migrations).
    pub keyspaces: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            node: "127.0.0.1:9042".into(),
            single_node: false,
            migrations: "migrations".into(),
            history_keyspace: "cql_migrate".into(),
            history_replication: "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
                .into(),
            lock_ttl: 600,
            keyspaces: vec![],
        }
    }
}

pub fn load(path: &Path) -> Result<Config> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

/// `cql-migrate.toml` in the current directory, if present.
pub fn discover() -> Result<Config> {
    let p = Path::new("cql-migrate.toml");
    if p.is_file() {
        load(p)
    } else {
        Ok(Config::default())
    }
}
