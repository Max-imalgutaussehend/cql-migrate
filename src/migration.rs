//! Migration files: `0001_create_users.cql`, ordered by the numeric prefix.

use anyhow::{Context, Result, bail};
use cqlc_core::lexer::{TokenKind as K, lex};
use cqlc_core::span::FileId;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct Migration {
    pub version: u32,
    pub name: String,
    pub path: PathBuf,
    /// Statements without the trailing `;`, in file order.
    pub statements: Vec<String>,
    /// SHA-256 of the normalised token stream: comments, whitespace and keyword case do not change it.
    pub checksum: String,
}

/// `0001_create_users.cql` -> (1, "create_users").
pub fn parse_file_name(name: &str) -> Option<(u32, String)> {
    let stem = name.strip_suffix(".cql")?;
    let (v, rest) = stem.split_once('_')?;
    if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) || rest.is_empty() {
        return None;
    }
    Some((v.parse().ok()?, rest.to_string()))
}

pub fn load_dir(dir: &Path) -> Result<Vec<Migration>> {
    let mut out: Vec<Migration> = Vec::new();
    let rd = std::fs::read_dir(dir)
        .with_context(|| format!("reading migrations directory {}", dir.display()))?;
    for e in rd {
        let p = e?.path();
        let Some(fname) = p.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !fname.ends_with(".cql") {
            continue;
        }
        let Some((version, name)) = parse_file_name(fname) else {
            bail!(
                "{}: migration files are named <version>_<name>.cql, e.g. 0001_create_users.cql",
                p.display()
            );
        };
        let text =
            std::fs::read_to_string(&p).with_context(|| format!("reading {}", p.display()))?;
        let (statements, checksum) = analyse(&text).with_context(|| p.display().to_string())?;
        if let Some(dup) = out.iter().find(|m| m.version == version) {
            bail!(
                "version {version} is used twice: {} and {}",
                dup.path.display(),
                p.display()
            );
        }
        out.push(Migration {
            version,
            name,
            path: p,
            statements,
            checksum,
        });
    }
    out.sort_by_key(|m| m.version);
    Ok(out)
}

/// Splits `text` into statements (a `BEGIN ... APPLY BATCH` block is one statement) and computes the checksum.
pub fn analyse(text: &str) -> Result<(Vec<String>, String)> {
    let (lexed, diags) = lex(FileId(0), text);
    if let Some(d) = diags.iter().find(|d| d.is_error()) {
        bail!("{}", d.message);
    }
    let toks: Vec<_> = lexed.tokens.iter().filter(|t| t.kind != K::Eof).collect();
    let mut statements = Vec::new();
    let mut hasher = Sha256::new();
    let mut start: Option<usize> = None;
    let mut in_batch = false;
    let mut prev_apply = false;
    let mut first_kw = true;
    for t in &toks {
        let r = t.span.range();
        let kw = if let K::Ident(i) = &t.kind {
            i.to_ascii_uppercase()
        } else {
            String::new()
        };
        if start.is_none() {
            start = Some(r.start);
            first_kw = true;
        }
        if first_kw && kw == "BEGIN" {
            in_batch = true;
        }
        first_kw = false;
        if in_batch && prev_apply && kw == "BATCH" {
            in_batch = false;
        }
        prev_apply = kw == "APPLY";
        // normalised token text
        match &t.kind {
            K::Ident(_) => hasher.update(kw.as_bytes()),
            _ => hasher.update(text[r.clone()].as_bytes()),
        }
        hasher.update([0x1f]);
        if t.kind == K::Semi {
            hasher.update([0x1e]);
            if !in_batch {
                let s = start.take().unwrap();
                let stmt = text[s..r.start].trim();
                if !stmt.is_empty() {
                    statements.push(stmt.to_string());
                }
            }
        }
    }
    if let Some(s) = start {
        let stmt = text[s..].trim().trim_end_matches(';').trim();
        if !stmt.is_empty() {
            statements.push(stmt.to_string());
        }
    }
    let checksum = hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    Ok((statements, checksum))
}

/// Next free version number and the file name for a new migration.
pub fn next_file(dir: &Path, name: &str) -> Result<PathBuf> {
    let existing = if dir.is_dir() { load_dir(dir)? } else { vec![] };
    let next = existing.last().map_or(1, |m| m.version + 1);
    let slug: String = name
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    let slug = slug.trim_matches('_').to_string();
    if slug.is_empty() {
        bail!("give the migration a name, e.g. `new create_users`");
    }
    Ok(dir.join(format!("{next:04}_{slug}.cql")))
}
